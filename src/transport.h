// Copyright 2019 Bytedance Inc. or its affiliates. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// =============================================================================

#ifndef PS_RDMA_TRANSPORT_H_
#define PS_RDMA_TRANSPORT_H_

#ifdef DMLC_USE_RDMA

#include "rdma_utils.h"

namespace ps {

class Transport {
 public:

   virtual void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) = 0;
   virtual void Recv(Message *msg) = 0;

   virtual void SendPullRequest(MessageBuffer *msg_buf) = 0;
   virtual void SendPushRequest(MessageBuffer *msg_buf) = 0;
   virtual void SendPushResponse(MessageBuffer *msg_buf)  = 0;
   virtual void SendPullResponse(MessageBuffer *msg_buf) = 0;

   virtual bool HasRemoteInfo(MessageBuffer *msg_buf, uint64_t key, bool is_push) = 0;
   virtual void StoreRemoteInfo(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) = 0;
   virtual void SendRendezvousBegin(MessageBuffer *msg_buf) = 0;

}; // class Transport


class RDMATransport : public Transport {
 public:
  explicit RDMATransport(Endpoint *endpoint, SimpleMempool *mempool) {
    endpoint_ = endpoint;
    mempool_ = mempool;
  };

  ~RDMATransport();

  void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) {
    // prepare memory
    for (auto& sa : msg_buf->data) {
      if (!sa.size()) continue;
      CHECK(sa.data());
      std::lock_guard<std::mutex> lock(mr_mu_);
      if (mem_mr_map_.find(sa.data()) == mem_mr_map_.end()) {
        struct ibv_mr *temp_mr;
        CHECK (temp_mr = ibv_reg_mr(pd_, sa.data(), sa.size(),
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
            << "Failed to register the memory region: " << strerror(errno)
            << ", sa.size()=" << sa.size();
        mem_mr_map_[sa.data()] = temp_mr;
      }
      auto it = mem_mr_map_.find(sa.data());
      MRPtr ptr(it->second, [](struct ibv_mr *mr) {});
      CHECK(ptr.get()) << strerror(errno);
      msg_buf->mrs.push_back(std::make_pair(std::move(ptr), sa.size()));
    }

    // prepare RDMA write sge list
    struct ibv_sge sge[1 + msg_buf->mrs.size()];
    sge[0].addr = reinterpret_cast<uint64_t>(msg_buf->inline_buf);
    sge[0].length = msg_buf->inline_len;
    sge[0].lkey = mempool_->LocalKey(msg_buf->inline_buf);

    size_t num_sge = 1;
    for (auto &pair : msg_buf->mrs) {
      size_t length = pair.second;
      CHECK(length);
      sge[num_sge].addr =
          reinterpret_cast<uint64_t>(pair.first->addr);
      sge[num_sge].length = length;
      sge[num_sge].lkey = pair.first->lkey;
      ++num_sge;
    }    

    WRContext *write_ctx = msg_buf->reserved_context;
    MessageBuffer **tmp =
        reinterpret_cast<MessageBuffer **>(write_ctx->buffer->addr);
    *tmp = msg_buf;  // write the addr of msg_buf into the mr buffer

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = reinterpret_cast<uint64_t>(write_ctx);
    wr.opcode = IBV_WR_RDMA_WRITE_WITH_IMM;
    wr.next = nullptr;
    wr.imm_data = idx;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = sge;
    wr.num_sge = num_sge;
    wr.wr.rdma.remote_addr = remote_addr;
    wr.wr.rdma.rkey = rkey;

    CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0)
        << "ibv_post_send failed.";
  }

  void SendPushResponse(MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(mu_);
    auto key = DecodeKey(msg_buf->data[0]);
    auto remote_addr = std::get<0>(push_addr_[key]);
    auto rkey = std::get<1>(push_addr_[key]);
    auto idx = std::get<2>(push_addr_[key]);
    RDMAWriteWithImm(msg_buf, remote_addr, rkey, idx);
  }

  void SendPullRequest(MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(mu_);
    auto key = DecodeKey(msg_buf->data[0]);
    auto remote_addr = std::get<0>(pull_addr_[key]);
    auto rkey = std::get<1>(pull_addr_[key]);
    auto idx = std::get<2>(pull_addr_[key]);
    RDMAWriteWithImm(msg_buf, remote_addr, rkey, idx);
  }

  bool HasRemoteInfo(MessageBuffer *msg_buf, uint64_t key, bool is_push) {
    std::lock_guard<std::mutex> lk(mu_);
    if ( is_push && (push_addr_.find(key) != push_addr_.end())) return true;
    if (!is_push && (pull_addr_.find(key) != pull_addr_.end())) return true;
    // no remote info, store the msg_buf address and push/pull flag for RendezvousReply
    msgbuf_cache_.emplace(reinterpret_cast<uint64_t>(msg_buf), is_push);
    return false;
  }

  void StoreRemoteInfo(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) {
    if (msg_buf->data.size() == 0) return; 
    auto key = DecodeKey(msg_buf->data[0]);
    auto buf = reinterpret_cast<uint64_t>(msg_buf);

    std::lock_guard<std::mutex> lk(mu_);
    auto is_push = msgbuf_cache_[buf];
    if (is_push) {
      push_addr_.emplace(key, std::make_tuple(remote_addr, rkey, idx));
    } else {
      pull_addr_.emplace(key, std::make_tuple(remote_addr, rkey, idx));
    }
    msgbuf_cache_.erase(buf);
  }

  void SendRendezvousBegin(MessageBuffer *msg_buf) {
    WRContext *context = nullptr, *reserved = nullptr;
    endpoint_->free_write_ctx.WaitAndPop(&reserved);
    endpoint_->free_start_ctx.WaitAndPop(&context);
    
    msg_buf->reserved_context = reserved;
    RendezvousStart *req =
        reinterpret_cast<RendezvousStart *>(context->buffer->addr);
    req->meta_len = msg_buf->inline_len;
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);
    req->data_num = msg_buf->data.size();
    for (size_t i = 0; i < req->data_num; ++i) {
      req->data_len[i] = msg->data[i].size();
    }
    
    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(req);
    sge.lkey = context->buffer->lkey;
    sge.length = sizeof(RendezvousStart);

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = reinterpret_cast<uint64_t>(context);
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.next = nullptr;
    wr.imm_data = kRendezvousStart;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0)
        << strerror(errno);
  }
  
  void SendPushRequest(MessageBuffer *msg_buf) override {
    std::lock_guard<std::mutex> lock(map_mu_);
    uint64_t key = DecodeKey(msg.data[0]);
    msg.meta.key = key;

    CHECK_EQ(msg.data.size(), 3) << msg.data.size();
    CHECK_NE(memory_mr_map_.find(msg.data[1].data()), memory_mr_map_.end());

    auto& vals = msg.data[1];
    msg.meta.addr = reinterpret_cast<uint64_t>(vals.data()); // vals address
    msg.meta.val_len = vals.size();
    msg.meta.option = memory_mr_map_[vals.data()]->rkey;
  }

  void SendPullResponse(MessageBuffer *msg_buf) override {
    std::lock_guard<std::mutex> lock(map_mu_);
    uint64_t key = msg.meta.key;
    auto recver = msg.meta.recver;

    CHECK_NE(key_meta_map_.find(key), key_meta_map_.end())
        << "key=" << key << " not inited in key_meta_map";
    CHECK_NE(key_meta_map_[key].find(recver), key_meta_map_[key].end())
        << "key=" << key << ", recver=" << recver << " not inited in key_meta_map[key]";

    msg.meta.val_len = std::get<0>(key_meta_map_[key][recver]);
    msg.meta.addr = std::get<1>(key_meta_map_[key][recver]);
    msg.meta.option = std::get<2>(key_meta_map_[key][recver]);

    // RDMA write
    auto raddr = std::get<1>(key_meta_map_[key][recver]);
    auto rkey = std::get<2>(key_meta_map_[key][recver]);

    auto temp_mr = memory_mr_map_.find(msg_buf->data[1].data());
    CHECK_NE(temp_mr, memory_mr_map_.end());

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(msg_buf->data[1].data());
    sge.length = msg_buf->data[1].size();
    sge.lkey = temp_mr->second->lkey;

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = reinterpret_cast<uint64_t>(raddr);
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.next = nullptr;
    // wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;

    CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
      << "ibv_post_send failed.";
  }

  void Recv(Message *msg) {
    
  }
 
 protected:
  Endpoint *endpoint_;
  SimpleMempool *mempool_;
  std::unordered_map<uint64_t, std::tuple<uint64_t, uint32_t, uint32_t> > push_addr_; // key, <remote_addr, rkey, idx>
  std::unordered_map<uint64_t, std::tuple<uint64_t, uint32_t, uint32_t> > pull_addr_; // key, <remote_addr, rkey, idx>
  std::unordered_map<uint64_t, bool> msgbuf_cache_; // msg_buf, is_push
  std::mutex mu_;

  std::unordered_map<char*, struct ibv_mr*> mem_mr_map_;
  std::mutex mr_mu_;
}; // class Transport


class IPCTransport : public RDMATransport {
 public:

  void SendPushRequest(Message &msg) override {
    // get from shared memory
  }

  void SendPullResponse(Message &msg) override {
    // std::lock_guard<std::mutex> lock(map_mu_);
    // auto key = msg.meta.key;
    // auto recver = msg.meta.recver;
    // auto len = std::get<0>(key_meta_map_[key][recver]);

    // // IPC
    // auto addr = (void*) msg_buf->data[1].data();
    // CHECK(addr);
    // void* shm_addr = GetSharedMemory(kShmPrefix, key);
    // // async copy
    // AsyncCopy m = {endpoint, msg_buf, shm_addr, addr, len, meta_len, false};
    // auto cnt = cpy_counter_.fetch_add(1);
    // async_copy_queue_[cnt % ipc_copy_nthreads_]->Push(m);
  }

}; // class IPCTransport


};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_


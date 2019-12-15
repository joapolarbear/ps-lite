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
   virtual void RecvPushRequest(Message *msg, BufferContext *buffer_ctx) = 0;
   virtual void RecvPullRequest(Message *msg, BufferContext *buffer_ctx) = 0;
   virtual void RecvPushResponse(Message *msg, BufferContext *buffer_ctx) = 0;
   virtual void RecvPullResponse(Message *msg, BufferContext *buffer_ctx) = 0;

   virtual void SendPullRequest(Message &msg, MessageBuffer *msg_buf) = 0;
   virtual void SendPushRequest(Message &msg, MessageBuffer *msg_buf) = 0;
   virtual void SendPushResponse(Message &msg, MessageBuffer *msg_buf)  = 0;
   virtual void SendPullResponse(Message &msg, MessageBuffer *msg_buf) = 0;
   virtual void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) = 0;
   virtual void SendRendezvousReply(RendezvousStart *req, AddressPool<BufferContext> &pool) = 0;

   virtual bool HasRemoteInfo(MessageBuffer *msg_buf, uint64_t key, bool is_push) = 0;
   virtual void StoreRemoteInfo(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) = 0;

}; // class Transport


class RDMATransport : public Transport {
 public:
  explicit RDMATransport(Endpoint *endpoint, SimpleMempool *mempool) {
    endpoint_ = endpoint;
    mempool_ = mempool;
    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server_ = (role=="server");
    if (is_server_) LOG(INFO) << "This is server";
    else LOG(INFO) << "This is " << ((role=="worker") ? "worker" : "scheduler");
  };

  ~RDMATransport() {
    for (auto& it : mem_mr_map_) ibv_dereg_mr(it.second);
  };

  void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) {
    // prepare memory
    for (auto& sa : msg_buf->data) {
      if (!sa.size()) continue;
      CHECK(sa.data());
      std::lock_guard<std::mutex> lock(mr_mu_);
      if (mem_mr_map_.find(sa.data()) == mem_mr_map_.end()) {
        struct ibv_mr *temp_mr;
        CHECK (temp_mr = ibv_reg_mr(mempool_->GetPD(), sa.data(), sa.size(),
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

  void SendPushResponse(Message &msg, MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lk(mu_);
    auto key = DecodeKey(msg_buf->data[0]);
    auto remote_addr = std::get<0>(push_addr_[key]);
    auto rkey = std::get<1>(push_addr_[key]);
    auto idx = std::get<2>(push_addr_[key]);
    RDMAWriteWithImm(msg_buf, remote_addr, rkey, idx);
  }

  void SendPullRequest(Message &msg, MessageBuffer *msg_buf) {
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

  void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) {
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
      req->data_len[i] = msg.data[i].size();
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

  void SendRendezvousReply(RendezvousStart *req, AddressPool<BufferContext> &pool) {

    BufferContext *buf_ctx = new BufferContext();
    uint64_t len = req->meta_len;
    buf_ctx->meta_len = req->meta_len;
    buf_ctx->data_num = req->data_num;
    for (size_t i = 0; i < req->data_num; ++i) {
      buf_ctx->data_len[i] = req->data_len[i];
      len += req->data_len[i];
    }
    char *buffer = mempool_->Alloc(is_server_ ? len : req->meta_len);
    CHECK(buffer) << len;
    buf_ctx->buffer = buffer;
    WRContext *reply_ctx = nullptr;
    endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);
    RendezvousReply *resp =
        reinterpret_cast<RendezvousReply *>(reply_ctx->buffer->addr);

    char* buffer = buf_ctx->buffer;
    resp->addr = reinterpret_cast<uint64_t>(buffer);
    resp->rkey = mempool_->RemoteKey(buffer);
    resp->origin_addr = req->origin_addr;
    resp->idx = pool.StoreAddress(buf_ctx);

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(resp);
    sge.length = sizeof(RendezvousReply);
    sge.lkey = reply_ctx->buffer->lkey;

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = reinterpret_cast<uint64_t>(reply_ctx);
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.next = nullptr;

    wr.imm_data = kRendezvousReply;

    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0)
        << "ibv_post_send failed.";
  }

  virtual void SendPushRequest(Message &msg, MessageBuffer *msg_buf) {
    std::lock_guard<std::mutex> lock(map_mu_);
    uint64_t key = DecodeKey(msg.data[0]);
    msg.meta.key = key;

    CHECK_EQ(msg.data.size(), 3) << msg.data.size();
    CHECK_NE(mem_mr_map_.find(msg.data[1].data()), mem_mr_map_.end());

    auto& vals = msg.data[1];
    msg.meta.addr = reinterpret_cast<uint64_t>(vals.data()); // vals address
    msg.meta.val_len = vals.size();
    msg.meta.option = mem_mr_map_[vals.data()]->rkey;
  }

  virtual void SendPullResponse(Message &msg, MessageBuffer *msg_buf) {
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

    auto temp_mr = mem_mr_map_.find(msg_buf->data[1].data());
    CHECK_NE(temp_mr, mem_mr_map_.end());

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

    CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0)
      << "ibv_post_send failed.";
  }

  virtual int RecvPushResponse(Message *msg, BufferContext *buffer_ctx) {
    return Recv(msg, buffer_ctx);
  }

  virtual int RecvPullRequest(Message *msg, BufferContext *buffer_ctx) {
    return Recv(msg, buffer_ctx);
  }

  virtual int RecvPullResponse(Message *msg, BufferContext *buffer_ctx) {
    
  }

  virtual int RecvPushRequest(Message *msg, BufferContext *buffer_ctx) {
    
  }

 private:
  int Recv(Message *msg, BufferContext *buffer_ctx) {
    uint64_t data_num = buffer_ctx->data_num;
    if (data_num == 0) {
      mempool_->Free(buffer_ctx->buffer);
      delete buffer_ctx;
      return 0;
    }    

    int total_data_len = 0;
    char *cur = buffer_ctx->buffer + buffer_ctx->meta_len; // offset

    Block *mem_block = new Block(mempool_.get(), buffer_ctx->buffer, data_num);
    for (size_t i = 0; i < data_num; i++) {
      uint32_t len = buffer_ctx->data_len[i];
      SArray<char> data;
      data.reset(cur, len, [mem_block](void *) {
        mem_block->Release();
      });  // Defer the deletion of block_ref
      msg->data.push_back(data);
      cur += len;
      total_data_len += len;
    }

    return total_data_len;
  }
 
 protected:
  Endpoint *endpoint_;
  SimpleMempool *mempool_;
  // role is server or worker
  bool is_server_;
  std::unordered_map<uint64_t, std::tuple<uint64_t, uint32_t, uint32_t> > push_addr_; // key, <remote_addr, rkey, idx>
  std::unordered_map<uint64_t, std::tuple<uint64_t, uint32_t, uint32_t> > pull_addr_; // key, <remote_addr, rkey, idx>
  std::unordered_map<uint64_t, bool> msgbuf_cache_; // msg_buf, is_push
  std::mutex mu_;

  std::unordered_map<char*, struct ibv_mr*> mem_mr_map_;
  std::mutex map_mu_;
  // macros for key_meta_map
  using MetaInfo = std::tuple<int, uint64_t, int>; // len, addr, rkey
  using SenderMeta = std::unordered_map<int, MetaInfo>; // sender as the key
  // (key, sender) --> MetaInfo
  std::unordered_map<ps::Key, SenderMeta> key_meta_map_;
  std::mutex mr_mu_;
}; // class Transport



class IPCTransport : public RDMATransport {
 public:

  explicit IPCTransport(Endpoint *endpoint, SimpleMempool *mempool) : RDMATransport(endpoint, mempool) {
    auto val = Environment::Get()->find("BYTEPS_IPC_COPY_NUM_THREADS");
    ipc_copy_nthreads_ = val ? atoi(val) : 4;
    LOG(INFO) << "IPC async copy nthreads set to " << ipc_copy_nthreads_;
    for (int i = 0; i < ipc_copy_nthreads_; ++i) {
      auto q = new ThreadsafeQueue<AsyncCopy>;
      async_copy_queue_.push_back(q);
    }
    for (int i = 0; i < ipc_copy_nthreads_; ++i) {
      auto t = new std::thread(&IPCTransport::AsyncCopyThread, this, i);
      ipc_copy_thread_list_.push_back(t);
    }
    val = Environment::Get()->find("BYTEPS_PARTITION_BYTES");
    byteps_partition_bytes_ = val ? atoi(val) : 4096000;

    val = Environment::Get()->find("BYTEPS_LOCAL_SIZE");
    auto byteps_local_size = val ? atoi(val) : 1;
    byteps_partition_bytes_ = AlignTo(byteps_partition_bytes_, (8 * byteps_local_size));
    CHECK(val) << "BYTEPS_LOCAL_SIZE not set";
    LOG(INFO) << "partition bytes set to " << byteps_partition_bytes_ << ", should be identical with byteps core";
  };

  ~IPCTransport() {
    for (size_t i = 0; i < ipc_copy_thread_list_.size(); ++i) {
      AsyncCopy m;
      m.shutdown = true;
      async_copy_queue_[i]->Push(m);
      ipc_copy_thread_list_[i]->join();
    }
  }

  void SendPushRequest(Message &msg, MessageBuffer *msg_buf) {
    // get from shared memory
  }

  void SendPullResponse(Message &msg, MessageBuffer *msg_buf) {
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

  void AsyncCopyThread(int i) {
    auto& q = async_copy_queue_[i];
    while (true) {
      AsyncCopy m;
      q->WaitAndPop(&m);
      if (m.shutdown) break;
      if (m.len == 0) continue;

      // TODO: use parallel copy
      CHECK(m.dst);
      CHECK(m.src);
      memcpy(m.dst, m.src, m.len);

      WRContext *context = nullptr, *reserved = nullptr;
      m.endpoint->free_write_ctx.WaitAndPop(&reserved);
      m.endpoint->free_start_ctx.WaitAndPop(&context);

      m.msg_buf->reserved_context = reserved;
      RendezvousStart *req =
          reinterpret_cast<RendezvousStart *>(context->buffer->addr);
      req->meta_len = m.meta_len;
      req->origin_addr = reinterpret_cast<uint64_t>(m.msg_buf);

      auto addr = reinterpret_cast<uint64_t>(req);
      req->data_num = 0;

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
  }

 private:

  void* GetSharedMemory(const std::string& prefix, uint64_t key) {
    std::lock_guard<std::mutex> lock(shm_mu_);
    auto worker_key = DecodeWorkerKey(key);
    auto seq_num = worker_key % (1 << 16);
    auto base_key = worker_key - seq_num;
    uint64_t offset = byteps_partition_bytes_ * seq_num;
    if (key_shm_addr_.find(base_key) != key_shm_addr_.end()) {
      return key_shm_addr_[base_key] + offset;
    }
    std::string shm_name(prefix);
    shm_name += std::to_string(base_key);
    int shm_fd = shm_open(shm_name.c_str(), O_RDWR, 0666);
    CHECK_GE(shm_fd, 0) << "shm_open failed for " << shm_name;

    struct stat sb;
    CHECK_EQ(0, fstat(shm_fd, &sb)) << strerror(errno);
    auto total_shm_size = sb.st_size;

    void* base_ptr = mmap(0, total_shm_size, PROT_READ | PROT_WRITE, MAP_SHARED, shm_fd, 0);
    CHECK_NE(base_ptr, (void*) -1) << strerror(errno);
    key_shm_addr_[base_key] = base_ptr;

    LOG(INFO) << "open Shared Memory: " << shm_name
              << ", offset=" << offset
              << ", (in bytes) size=" << total_shm_size;
    return key_shm_addr_[base_key] + offset;
  }

  int ipc_copy_nthreads_;
  std::vector<std::thread*> ipc_copy_thread_list_;
  std::vector<ThreadsafeQueue<AsyncCopy>*> async_copy_queue_;
  std::atomic<unsigned long long> cpy_counter_{0};

  int byteps_partition_bytes_ = 4096000;

  std::mutex shm_mu_;
  std::unordered_map<uint64_t, void *> key_shm_addr_;

}; // class IPCTransport


};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_


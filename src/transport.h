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

#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_

#ifdef DMLC_USE_RDMA

#include "rdma_utils.h"

namespace ps {


class Transport {
 public:
  explicit Transport(Endpoint *endpoint) {
    endpoint_ = endpoint;
  };

  ~Transport();

  void SendPushResponse(MessageBuffer *msg_buf) {
    
  }

  void SendPullRequest(MessageBuffer *msg_buf) {

  }

  void SendControlMessage(MessageBuffer *msg_buf) {
    if (no remote address) {
      Rendezvous and get address;
    }
    RDMAWriteWithImm(msg_buf);
  }

  void RDMAWriteWithImm(MessageBuffer *msg_buf) {
    
  }
  
  void Recv(Message *msg);
  void SendPushRequest(MessageBuffer *msg_buf);
  void SendPullResponse(MessageBuffer *msg_buf);

  Endpoint *endpoint_;
}; // class Transport



class RDMATransport : public Transport {
 public:
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

  // get remote address using SEND/RECV
  void GetRemoteAddr() { 

  }

  // register RDMA memory
  void RegisterMemory(Message &msg) {
    for (auto& sa : msg.data) {
      if (!sa.size()) continue;
      CHECK(sa.data());
      std::lock_guard<std::mutex> lock(map_mu_);
      if (memory_mr_map_.find(sa.data()) == memory_mr_map_.end()) {
        struct ibv_mr *temp_mr;
        CHECK (temp_mr = ibv_reg_mr(pd_, sa.data(), sa.size(),
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
            << "Failed to register the memory region: " << strerror(errno)
            << ", sa.size()=" << sa.size();
        memory_mr_map_[sa.data()] = temp_mr;
      }
    }
  }

  void Recv(Message *msg) override {

  }

 private:

}; // class RDMATransport


class IPCTransport : public Transport {
 public:

  void SendPushRequest(Message &msg) override {
    
  }

  void SendPullResponse(Message &msg) override {
    std::lock_guard<std::mutex> lock(map_mu_);
    auto key = msg.meta.key;
    auto recver = msg.meta.recver;
    auto len = std::get<0>(key_meta_map_[key][recver]);

    // IPC
    auto addr = (void*) msg_buf->data[1].data();
    CHECK(addr);
    void* shm_addr = GetSharedMemory(kShmPrefix, key);
    // async copy
    AsyncCopy m = {endpoint, msg_buf, shm_addr, addr, len, meta_len, false};
    auto cnt = cpy_counter_.fetch_add(1);
    async_copy_queue_[cnt % ipc_copy_nthreads_]->Push(m);
  }

  void Recv(Message *msg) override {

  } 
}; // class IPCTransport




};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_


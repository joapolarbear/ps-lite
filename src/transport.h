#ifndef PS_RDMA_VAN_H_
#define PS_RDMA_VAN_H_

#ifdef DMLC_USE_RDMA

#include <errno.h>
#include <fcntl.h>
#include <sys/shm.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <unistd.h>
#include <netdb.h>
#include <poll.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>

#include <rdma/rdma_cma.h>

#include <algorithm>
#include <map>
#include <queue>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <unordered_map>
#include <vector>

#include "ps/internal/threadsafe_queue.h"
#include "ps/internal/van.h"

namespace ps {

class Transport {
 public:
  explicit Transport(Endpoint *endpoint) {
    endpoint_ = endpoint;
  };

  ~Transport();

  void SendPushResponse(Message &msg) {

  }

  void SendPullRequest(Message &msg) {

  }

  void SendControlMessage(Message &msg) {
    PBMeta meta;
    PackMetaPB(msg.meta, &meta);
    MessageBuffer *msg_buf = new MessageBuffer();
    
    size_t meta_len = meta.ByteSize();
    size_t total_len = meta_len + msg.meta.data_size;
    CHECK(meta_len);

    msg_buf->inline_len = total_len;
    msg_buf->inline_buf = mempool_->Alloc(total_len);
    meta.SerializeToArray(msg_buf->inline_buf, meta_len);

    WRContext *context = nullptr, *reserved = nullptr;
    endpoint_->free_write_ctx.WaitAndPop(&reserved);
    endpoint_->free_start_ctx.WaitAndPop(&context);
    
    msg_buf->reserved_context = reserved;
    RendezvousStart *req =
        reinterpret_cast<RendezvousStart *>(context->buffer->addr);
    req->meta_len = meta_len;
    req->origin_addr = reinterpret_cast<uint64_t>(msg_buf);
    
    auto addr = reinterpret_cast<uint64_t>(req);

    // rendezvous message, not data message
    if (!is_server_ && is_local_[remote_id] && IsValidPushpull(msg)) { 
      // local IPC with shared memory
      req->data_num = 0;
    } else { 
      // normal RDMA
      req->data_num = msg.data.size();
      for (size_t i = 0; i < req->data_num; ++i) {
        req->data_len[i] = msg.data[i].size();
      }
    }
    SendRendezvousBegin(endpoint_, addr, context, kRendezvousStart);
  }
  
  void Recv(Message *msg);
  void SendPushRequest();
  void SendPullResponse();

  Endpoint* endpoint_;
}; // class Transport



class RDMATransport : public Transport {
 public:
  void SendPushRequest(Message &msg) override {
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

  void SendPullResponse(Message &msg) override {
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



} // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
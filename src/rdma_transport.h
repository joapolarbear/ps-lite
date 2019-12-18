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

class Transport;

struct Endpoint {
  enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };

  ConnectionStatus status;
  int node_id;
  std::condition_variable cv;
  std::mutex connect_mu;
  struct rdma_cm_id *cm_id;
  std::shared_ptr<Transport> trans;

  WRContext rx_ctx[kRxDepth];

  WRContext start_ctx[kStartDepth];
  WRContext reply_ctx[kReplyDepth];
  WRContext write_ctx[kWriteDepth];

  ThreadsafeQueue<WRContext *> free_start_ctx;
  ThreadsafeQueue<WRContext *> free_reply_ctx;
  ThreadsafeQueue<WRContext *> free_write_ctx;

  Endpoint() : status(IDLE), node_id(Node::kEmpty), cm_id(nullptr), rx_ctx() {}

  ~Endpoint() {
    for (int i = 0; i < kRxDepth; ++i) {
      if (!(rx_ctx[i].buffer)) {
        continue;
      }
      free(rx_ctx[i].buffer->addr);
      CHECK_EQ(ibv_dereg_mr(rx_ctx[i].buffer), 0);
    }

    for (int i = 0; i < kStartDepth; ++i) {
      if (start_ctx[i].buffer) {
        free(start_ctx[i].buffer->addr);
        CHECK_EQ(ibv_dereg_mr(start_ctx[i].buffer), 0);
      }
    }

    for (int i = 0; i < kReplyDepth; ++i) {
      if (reply_ctx[i].buffer) {
        free(reply_ctx[i].buffer->addr);
        CHECK_EQ(ibv_dereg_mr(reply_ctx[i].buffer), 0);
      }
    }

    for (int i = 0; i < kWriteDepth; ++i) {
      if (write_ctx[i].buffer) {
        free(write_ctx[i].buffer->addr);
        CHECK_EQ(ibv_dereg_mr(write_ctx[i].buffer), 0);
      }
    }

    rdma_destroy_qp(cm_id);
    CHECK_EQ(rdma_destroy_id(cm_id), 0) << strerror(errno);
  }

  void SetTransport(std::shared_ptr<Transport> t) { trans = t; }

  std::shared_ptr<Transport> GetTransport() { return trans; }

  void Disconnect() {
    std::unique_lock<std::mutex> lk(connect_mu);
    CHECK_EQ(rdma_disconnect(cm_id), 0) << strerror(errno);
    cv.wait(lk, [this] { return status == IDLE; });
    trans.reset();
  }

  void SetNodeID(int id) { node_id = id; }

  void InitSendContextHelper(struct ibv_pd *pd, WRContext *ctx,
                             ThreadsafeQueue<WRContext *> *queue, size_t num,
                             WRContextType type) {
    for (size_t i = 0; i < num; ++i) {
      void *buf;
      ib_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);
      struct ibv_mr *mr = ibv_reg_mr(pd, buf, kMempoolChunkSize, 0);
      CHECK(mr);

      ctx[i].type = type;
      ctx[i].buffer = mr;
      ctx[i].private_data = this;
      queue->Push(&ctx[i]);
    }
  }

  void Init(struct ibv_cq *cq, struct ibv_pd *pd) {
    struct ibv_qp_init_attr attr;
    memset(&attr, 0, sizeof(ibv_qp_init_attr));
    attr.send_cq = cq;
    attr.recv_cq = cq;
    attr.cap.max_send_wr = kStartDepth + kReplyDepth + kWriteDepth;
    attr.cap.max_recv_wr = kRxDepth;
    attr.cap.max_send_sge = kSGEntry;
    attr.cap.max_recv_sge = kSGEntry;
    attr.qp_type = IBV_QPT_RC;
    attr.sq_sig_all = 0;

    CHECK_EQ(rdma_create_qp(cm_id, pd, &attr), 0)
        << "Create RDMA queue pair failed";

    InitSendContextHelper(pd, start_ctx, &free_start_ctx, kStartDepth,
                          kRendezvousStartContext);
    InitSendContextHelper(pd, reply_ctx, &free_reply_ctx, kReplyDepth,
                          kRendezvousReplyContext);
    InitSendContextHelper(pd, write_ctx, &free_write_ctx, kWriteDepth,
                          kWriteContext);

    for (size_t i = 0; i < kRxDepth; ++i) {
      void *buf;
      ib_malloc((void**) &buf, kMempoolChunkSize);
      CHECK(buf);
      struct ibv_mr *mr =
          ibv_reg_mr(pd, buf, kMempoolChunkSize, IBV_ACCESS_LOCAL_WRITE);
      CHECK(mr);

      rx_ctx[i].type = kReceiveContext;
      rx_ctx[i].buffer = mr;
      rx_ctx[i].private_data = this;

      PostRecv(&rx_ctx[i]);
    }
  }

  void PostRecv(WRContext *ctx) {
    struct ibv_recv_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(ctx->buffer->addr);
    sge.length = kMempoolChunkSize;
    sge.lkey = ctx->buffer->lkey;

    wr.wr_id = reinterpret_cast<uint64_t>(ctx);
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;

    CHECK_EQ(ibv_post_recv(cm_id->qp, &wr, &bad_wr), 0)
        << "ibv_post_recv failed.";
  }
};

struct AsyncCopy {
  Endpoint* endpoint;
  MessageBuffer* msg_buf;
  void* dst;
  void* src;
  int len;
  uint64_t meta_len;
  bool shutdown;
};


class Transport {
 public:
   virtual void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) = 0;
   virtual int Recv(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
   virtual int RecvPushRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
   virtual int RecvPullRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
   virtual int RecvPushResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
   virtual int RecvPullResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) = 0;
  
   virtual void AddMeta(Message &msg) = 0;
   virtual void RegisterMemory(Message &msg) = 0;
   virtual void PrepareData(Message &msg, MessageBuffer *msg_buf) = 0;

   virtual void Send(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) = 0;
   virtual void SendPullRequest(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) = 0;
   virtual void SendPushRequest(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) = 0;
   virtual void SendPushResponse(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr)  = 0;
   virtual void SendPullResponse(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) = 0;
   virtual void SendRendezvousBegin(Message &msg, MessageBuffer *msg_buf) = 0;
   virtual void SendRendezvousReply(RendezvousStart *req, AddressPool<BufferContext> &pool) = 0;

}; // class Transport


class RDMATransport : public Transport {
 public:
  explicit RDMATransport(Endpoint *endpoint, SimpleMempool *mempool) {
    endpoint_ = CHECK_NOTNULL(endpoint);
    mempool_ = CHECK_NOTNULL(mempool);
    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server_ = (role=="server");
  };

  ~RDMATransport() {
    for (auto& it : mem_mr_map_) ibv_dereg_mr(it.second);
  };

  void RegisterMemory(Message &msg) {
    for (auto& sa : msg.data) {
      if (sa.size() == 0) continue;
      std::lock_guard<std::mutex> lock(map_mu_);
      if (mem_mr_map_.find(sa.data()) == mem_mr_map_.end()) {
        struct ibv_mr *temp_mr;
        CHECK (temp_mr = ibv_reg_mr(mempool_->GetPD(), sa.data(), sa.size(),
            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE))
            << "Failed to register the memory region: " << strerror(errno)
            << ", sa.size()=" << sa.size();
        mem_mr_map_[sa.data()] = temp_mr;
      }
    }
  }

  void PrepareData(Message &msg, MessageBuffer *msg_buf) {
    for (auto &sa : msg_buf->data) {
      if (sa.size() == 0) continue;
      std::lock_guard<std::mutex> lock(map_mu_);
      auto it = mem_mr_map_.find(sa.data());
      MRPtr ptr(it->second, [](struct ibv_mr *mr) {});
      CHECK(ptr.get()) << strerror(errno);
      msg_buf->mrs.push_back(std::make_pair(std::move(ptr), sa.size()));
    }
  }

  void RDMAWriteWithImm(MessageBuffer *msg_buf, uint64_t remote_addr, uint32_t rkey, uint32_t idx) {
    // prepare RDMA write sge list
    struct ibv_sge sge[1 + msg_buf->mrs.size()];
    sge[0].addr = reinterpret_cast<uint64_t>(msg_buf->inline_buf);
    sge[0].length = msg_buf->inline_len;
    sge[0].lkey = mempool_->LocalKey(msg_buf->inline_buf);

    size_t num_sge = 1;
    
    uint64_t data_len = 0;
    if (msg_buf->mrs.size() == 3) { 
      // push request, only write vals
      sge[1].addr = reinterpret_cast<uint64_t>(msg_buf->mrs[1].first->addr);
      sge[1].length = msg_buf->mrs[1].second;
      sge[1].lkey = msg_buf->mrs[1].first->lkey;
      ++num_sge;
      data_len += sge[1].length;
    } else {
      for (auto &pair : msg_buf->mrs) {
        size_t length = pair.second;      
        CHECK(length);
        sge[num_sge].addr =
            reinterpret_cast<uint64_t>(pair.first->addr);
        sge[num_sge].length = length;
        sge[num_sge].lkey = pair.first->lkey;
        ++num_sge;
        
        data_len += length;
      }
    }

    WRContext *write_ctx = msg_buf->reserved_context;
    CHECK(write_ctx);
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

  void SendRendezvousReply(RendezvousStart *req, AddressPool<BufferContext> &addrpool) {
    BufferContext *buf_ctx = new BufferContext();
    buf_ctx->meta_len = req->meta_len;
    buf_ctx->data_num = req->data_num;

    uint64_t len = req->meta_len;
    for (size_t i = 0; i < req->data_num; ++i) {
      buf_ctx->data_len[i] = req->data_len[i];
      len += req->data_len[i];
    }
    
    // worker only needs a buffer for receving meta
    char *buffer = mempool_->Alloc(is_server_ ? (kMaxMetaBound + len) : (kMaxMetaBound + req->meta_len));
    CHECK(buffer);
    buf_ctx->buffer = buffer;
    WRContext *reply_ctx = nullptr;
    endpoint_->free_reply_ctx.WaitAndPop(&reply_ctx);

    RendezvousReply *resp =
        reinterpret_cast<RendezvousReply *>(reply_ctx->buffer->addr);

    resp->addr = reinterpret_cast<uint64_t>(buffer);
    resp->rkey = mempool_->RemoteKey(buffer);
    resp->origin_addr = req->origin_addr;
    resp->idx = addrpool.StoreAddress(buf_ctx);

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

  void AddMeta(Message &msg) {
    if (msg.meta.request) {
      msg.meta.key = DecodeKey(msg.data[0]);
    }
    if (msg.meta.push && msg.meta.request) { 
      // push request
      CHECK_EQ(msg.data.size(), 3) << msg.data.size();

      std::lock_guard<std::mutex> lock(map_mu_);
      CHECK_NE(mem_mr_map_.find(msg.data[1].data()), mem_mr_map_.end());

      auto& vals = msg.data[1];
      msg.meta.addr = reinterpret_cast<uint64_t>(vals.data()); // vals address
      msg.meta.val_len = vals.size();
      msg.meta.option = mem_mr_map_[vals.data()]->rkey;
    }
  }

  void Send(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) {
    WRContext *reserved = nullptr;
    endpoint_->free_write_ctx.WaitAndPop(&reserved);
    msg_buf->reserved_context = reserved;
    auto key = msg.meta.key;

    auto raddr = std::get<0>(remote_addr);
    auto rkey = std::get<1>(remote_addr);
    auto idx = std::get<2>(remote_addr);

    RDMAWriteWithImm(msg_buf, raddr, rkey, idx);
  }

  void SendPushResponse(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) {
    Send(msg, msg_buf, remote_addr);
  }

  void SendPullRequest(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) {
    Send(msg, msg_buf, remote_addr);
  }

  virtual void SendPushRequest(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) {
    Send(msg, msg_buf, remote_addr);
  }

  virtual void SendPullResponse(Message &msg, MessageBuffer *msg_buf, RemoteAddress remote_addr) {
    std::lock_guard<std::mutex> lock(map_mu_);
    auto key = msg.meta.key;
    auto recver = msg.meta.recver;
    auto len = msg.meta.val_len;
    auto raddr = msg.meta.addr;
    auto rkey = msg.meta.option;
    auto temp_mr = mem_mr_map_.find(msg_buf->data[1].data());
    CHECK_NE(temp_mr, mem_mr_map_.end());

    struct ibv_sge sge;
    sge.addr = reinterpret_cast<uint64_t>(msg_buf->data[1].data());
    sge.length = msg_buf->data[1].size();
    sge.lkey = temp_mr->second->lkey;

    // this rdma-write will not trigger any signal both remotely and locally
    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));
    wr.wr_id = reinterpret_cast<uint64_t>(raddr);
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.next = nullptr;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    wr.wr.rdma.remote_addr = raddr;
    wr.wr.rdma.rkey = rkey;

    CHECK_EQ(ibv_post_send(endpoint_->cm_id->qp, &wr, &bad_wr), 0)
      << "ibv_post_send failed.";

    // after write keys/vals/lens (no imm), write the meta (with imm)
    // TODO: consolidate this into one RDMA_WRITE_WITH_IMM
    msg_buf->mrs.clear(); 
    Send(msg, msg_buf, remote_addr);
  }

  virtual int RecvPushResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    return Recv(msg, buffer_ctx, meta_len);
  }

  virtual int RecvPullRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    return Recv(msg, buffer_ctx, meta_len);
  }

  virtual int RecvPullResponse(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    int total_data_len = 0;
    std::lock_guard<std::mutex> lock(map_mu_);
    auto key = msg->meta.key;
    if (key_len_map_.find(key) == key_len_map_.end()) {
      // need a static address for keys/lens
      key_addr_map_[key] = (ps::Key) key;
      key_len_map_[key] = (int) msg->meta.val_len;
    }
    CHECK_NE(key_len_map_.find(key), key_len_map_.end()) << key;
    CHECK_NE(key_addr_map_.find(key), key_addr_map_.end()) << key;

    auto addr = msg->meta.addr;

    CHECK_NE(key_len_map_[key], 0) << msg->DebugString();

    SArray<char> keys;
    SArray<char> vals;
    SArray<char> lens;

    keys.reset(reinterpret_cast<char*>(&key_addr_map_[key]), sizeof(ps::Key), [](void *){});
    vals.reset(reinterpret_cast<char*>(addr), key_len_map_[key], [](void *){});
    lens.reset(reinterpret_cast<char*>(&key_len_map_[key]), sizeof(int), [](void *){});

    msg->data.push_back(keys);
    msg->data.push_back(vals);
    msg->data.push_back(lens);
    total_data_len += keys.size() + vals.size() + lens.size();

    return total_data_len;
  }

  virtual int RecvPushRequest(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    return Recv(msg, buffer_ctx, meta_len);
  }

 private:
  virtual int Recv(Message *msg, BufferContext *buffer_ctx, int meta_len) {
    uint64_t data_num = buffer_ctx->data_num;
    if (data_num == 0) {
      return 0;
    }

    char *cur = buffer_ctx->buffer + meta_len; // offset

    if (msg->meta.push && msg->meta.request) { // push request
      CHECK_EQ(data_num, 3);
      uint32_t len = buffer_ctx->data_len[1];
      
      SArray<char> keys;
      void *p = malloc(sizeof(Key));
      memcpy(p, &msg->meta.key, sizeof(Key));
      keys.reset((char *) p, sizeof(Key), [p](void *) { free(p); });

      SArray<char> vals;
      vals.reset(cur, len, [](void *) {});  // no need to delete

      SArray<char> lens;
      void *q = malloc(sizeof(int));
      memcpy(q, &len, sizeof(int));
      lens.reset((char *) q, sizeof(int), [q](void *) { free(q); });

      msg->data.push_back(keys);
      msg->data.push_back(vals);
      msg->data.push_back(lens);

      return sizeof(Key) + len + sizeof(int);
    }

    int total_data_len = 0;
    for (size_t i = 0; i < data_num; i++) {
      uint32_t len = buffer_ctx->data_len[i];
      SArray<char> data;
      data.reset(cur, len, [](void *) {});  // no need for delete
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

  // manage the following map
  std::mutex map_mu_;

  // (memory, ibv_mr) 
  std::unordered_map<char*, struct ibv_mr*> mem_mr_map_;

  // store the static address for keys and lens
  std::unordered_map<ps::Key, ps::Key> key_addr_map_;
  std::unordered_map<ps::Key, int> key_len_map_;

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


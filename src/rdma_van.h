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

#include "transport.h"

namespace ps {

class RDMAVan : public Van {
 public:
  RDMAVan() {
    CHECK_EQ(ibv_fork_init(), 0) << strerror(errno);
  }
  ~RDMAVan() {}

 protected:  
  void Start(int customer_id) override {
    start_mu_.lock();
    should_stop_ = false;

    auto val = Environment::Get()->find("DMLC_ROLE");
    std::string role(val);
    is_server_ = role=="server";
    if (is_server_) LOG(INFO) << "This is server";
    else LOG(INFO) << "This is " << ((role=="worker") ? "worker" : "scheduler");

    val = Environment::Get()->find("BYTEPS_ENABLE_IPC");
    disable_ipc_ = val ? !atoi(val) : true;
    if (disable_ipc_) LOG(INFO) << "Shared memory IPC has been disabled";

    val = Environment::Get()->find("BYTEPS_PARTITION_BYTES");
    byteps_partition_bytes_ = val ? atoi(val) : 4096000;

    val = Environment::Get()->find("BYTEPS_LOCAL_SIZE");
    auto byteps_local_size = val ? atoi(val) : 1;
    byteps_partition_bytes_ = AlignTo(byteps_partition_bytes_, (8 * byteps_local_size));
    if (!disable_ipc_) {
      CHECK(val) << "BYTEPS_LOCAL_SIZE not set";
      LOG(INFO) << "partition bytes set to " << byteps_partition_bytes_ << ", should be identical with byteps core";
    }

    val = Environment::Get()->find("BYTEPS_IPC_COPY_NUM_THREADS");
    ipc_copy_nthreads_ = val ? atoi(val) : 4;
    if (!disable_ipc_) {
      LOG(INFO) << "IPC async copy nthreads set to " << ipc_copy_nthreads_;
      for (int i = 0; i < ipc_copy_nthreads_; ++i) {
        auto q = new ThreadsafeQueue<AsyncCopy>;
        async_copy_queue_.push_back(q);
      }
      for (int i = 0; i < ipc_copy_nthreads_; ++i) {
        auto t = new std::thread(&RDMAVan::AsyncCopyThread, this, i);
        ipc_copy_thread_list_.push_back(t);
      }
    }
    
    if (event_channel_ == nullptr) {
      event_channel_ = rdma_create_event_channel();
      CHECK(event_channel_) << "Create RDMA event channel failed";

      cm_event_polling_thread_.reset(
          new std::thread(&RDMAVan::PollEvents, this));
    }

    start_mu_.unlock();
    Van::Start(customer_id);
  }

  void Stop() override {
    PS_VLOG(1) << my_node_.ShortDebugString() << " is stopping";
    Van::Stop();

    should_stop_ = true;
    CHECK(should_stop_);

    PS_VLOG(1) << "Stopping cq_polling_thread_.";
    cq_polling_thread_->join();
    cq_polling_thread_.reset();

    PS_VLOG(1) << "Stopping cm_event_polling_thread_.";
    cm_event_polling_thread_->join();
    cm_event_polling_thread_.reset();

    PS_VLOG(1) << "Clearing mempool.";
    mempool_.reset();

    for (auto& it : memory_mr_map_) ibv_dereg_mr(it.second);

    PS_VLOG(1) << "Clearing endpoints.";
    incoming_.clear();
    endpoints_.clear();

    PS_VLOG(1) << "Destroying cq and pd.";
    CHECK(!ibv_destroy_cq(cq_)) << "Failed to destroy CQ";
    CHECK(!ibv_destroy_comp_channel(comp_event_channel_))
        << "Failed to destroy channel";
    
    for (size_t i = 0; i < ipc_copy_thread_list_.size(); ++i) {
      AsyncCopy m;
      m.shutdown = true;
      async_copy_queue_[i]->Push(m);
      ipc_copy_thread_list_[i]->join();
    }

    // TODO: ibv_dealloc_pd sometimes complains resource busy, need to fix this
    // CHECK(!ibv_dealloc_pd(pd_)) << "Failed to deallocate PD: " <<
    // strerror(errno);

    PS_VLOG(1) << "Destroying listener.";
    rdma_destroy_id(listener_);
    rdma_destroy_event_channel(event_channel_);
  }

  int Bind(const Node &node, int max_retry) override {
    CHECK(rdma_create_id(event_channel_, &listener_, nullptr, RDMA_PS_TCP) == 0)
        << "Create RDMA connection identifier failed";
    
    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));    

    auto val = Environment::Get()->find("DMLC_NODE_HOST");
    if (val) {
      PS_VLOG(1) << "bind to DMLC_NODE_HOST: " << std::string(val);
      addr.sin_addr.s_addr = inet_addr(val);
    } 
    
    addr.sin_family = AF_INET;
    int port = node.port;
    unsigned seed = static_cast<unsigned>(time(NULL) + port);
    for (int i = 0; i < max_retry + 1; ++i) {
      addr.sin_port = htons(port);
      if (rdma_bind_addr(listener_,
                         reinterpret_cast<struct sockaddr *>(&addr)) == 0) {
        break;
      }
      if (i == max_retry) {
        port = -1;
      } else {
        port = 10000 + rand_r(&seed) % 40000;
      }
    }
    CHECK(rdma_listen(listener_, kRdmaListenBacklog) == 0)
        << "Listen RDMA connection failed: " << strerror(errno);
    return port;
  }

  void Connect(const Node &node) override {
    PS_VLOG(1) << "Connecting to Node " << node.id;
    CHECK_NE(node.id, node.kEmpty);
    CHECK_NE(node.port, node.kEmpty);
    CHECK(node.hostname.size());

    // worker doesn't need to connect to the other workers. same for server
    if ((node.role == my_node_.role) && (node.id != my_node_.id)) {
      return;
    }

    if (disable_ipc_) {
      is_local_[node.id] = false;
    } else {
      std::lock_guard<std::mutex> lock(local_mu_);
      is_local_[node.id] = (node.hostname == my_node_.hostname) ? true : false;
      LOG(INFO) << "IPC connected to " << node.id;
    }

    if (node.id != Node::kEmpty) {
      auto it = endpoints_.find(node.id);

      // if there is an endpoint with pending connection
      if (it != endpoints_.end()) {
        endpoints_.erase(it);
      }

      Endpoint *endpoint;
      endpoints_[node.id] = std::make_unique<Endpoint>();
      endpoint = endpoints_[node.id].get();

      endpoint->SetNodeID(node.id);

      Transport *t = is_local_[node.id] ? 
          std::make_unique<IPCTransport>(endpoint) : std::make_unique<RDMATransport>(endpoint);
      endpoint->SetTransport(t);

      struct addrinfo *remote_addr;
      CHECK_EQ(
          getaddrinfo(node.hostname.c_str(), std::to_string(node.port).c_str(),
                      nullptr, &remote_addr),
          0);

      while (endpoint->status != Endpoint::CONNECTED) {
        std::unique_lock<std::mutex> lk(endpoint->connect_mu);
        endpoint->status = Endpoint::CONNECTING;

        if (endpoint->cm_id != nullptr) {
          rdma_destroy_qp(endpoint->cm_id);
          CHECK_EQ(rdma_destroy_id(endpoint->cm_id), 0) << strerror(errno);
          endpoint->cm_id = nullptr;
        }

        CHECK_EQ(rdma_create_id(event_channel_, &endpoint->cm_id, nullptr,
                                RDMA_PS_TCP),
                 0)
            << "Create RDMA connection identifier failed";
        endpoint->cm_id->context = endpoint;

        int max_retry = kMaxResolveRetry;
        int port = kBasePort;
        unsigned seed = static_cast<unsigned>(time(NULL) + port);
        auto val = Environment::Get()->find("DMLC_NODE_HOST");
        if (val) {
          struct sockaddr_in addr;
          memset(&addr, 0, sizeof(addr)); 
          addr.sin_addr.s_addr = inet_addr(val);
          addr.sin_family = AF_INET;
          for (int i = 0; i < max_retry + 1; ++i) {
            addr.sin_port = htons(port);
            if (rdma_resolve_addr(endpoint->cm_id, 
                                  reinterpret_cast<struct sockaddr *>(&addr),
                                  remote_addr->ai_addr, kTimeoutms) == 0) {
              break;
            }
            if (i == max_retry) {
              port = -1;
            } else {
              port = 10000 + rand_r(&seed) % 40000;
            }
          }
        } else {
          CHECK_EQ(rdma_resolve_addr(endpoint->cm_id, nullptr,
                                     remote_addr->ai_addr, kTimeoutms),
                   0)
              << "Resolve RDMA address failed with errno: " << strerror(errno);
        }

        endpoint->cv.wait(lk, [endpoint] {
          return endpoint->status != Endpoint::CONNECTING;
        });

        if (endpoint->status == Endpoint::CONNECTED) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
      }

      freeaddrinfo(remote_addr);
    }
  }

  bool IsValidPushpull(const Message &msg) {
    if (!msg.meta.control.empty()) return false;
    if (msg.meta.simple_app) return false;
    return true;
  }

  uint64_t DecodeKey(SArray<char> keys) { // just a translation, the decoded key might not be readable when we have multiple servers
    ps::Key key = 0;
    uint64_t coef = 1;
    for (unsigned int i = 0; i < keys.size(); ++i) {
      key += coef * (uint8_t) keys.data()[i];
      coef *= 256; // 256=2^8 (uint8_t)
    }
    return key;
  }

  uint64_t DecodeWorkerKey(uint64_t key) {
    auto kr = ps::Postoffice::Get()->GetServerKeyRanges()[ps::Postoffice::Get()->my_rank()];
    return key - kr.begin();
  }

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

  void SendRendezvousBegin(Endpoint* endpoint, 
        uint64_t origin_addr, WRContext *context, MessageTypes msg_type) {
    struct ibv_sge sge;
    sge.addr = origin_addr;      
    sge.lkey = context->buffer->lkey;
    sge.length = sizeof(RendezvousStart);

    struct ibv_send_wr wr, *bad_wr = nullptr;
    memset(&wr, 0, sizeof(wr));

    wr.wr_id = reinterpret_cast<uint64_t>(context);
    wr.opcode = IBV_WR_SEND_WITH_IMM;
    wr.next = nullptr;

    wr.imm_data = msg_type;

    wr.send_flags = IBV_SEND_SIGNALED;
    wr.sg_list = &sge;
    wr.num_sge = 1;
    CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
        << strerror(errno);
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
      SendRendezvousBegin(m.endpoint, addr, context, kRendezvousStart);      
    }
  }

  int SendMsg(Message &msg) override {
    int remote_id = msg.meta.recver;
    CHECK_NE(remote_id, Meta::kEmpty);
    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    Endpoint *endpoint = endpoints_[remote_id].get();

    CHECK_NE(endpoints_.find(remote_id), endpoints_.end());
    Endpoint *endpoint = endpoints_[remote_id].get();
    MessageBuffer *msg_buf = new MessageBuffer();

    int meta_len = GetPackMetaLen(msg.meta);
    size_t data_len = msg.meta.data_size;
    size_t total_len = meta_len + data_len;
    CHECK(meta_len);

    msg_buf->inline_len = total_len;
    msg_buf->inline_buf = mempool_->Alloc(total_len);
    PackMeta(msg.meta, &(msg_buf->inline_buf), &meta_len);
    msg_buf->data = msg.data;

    auto trans = endpoint.GetTransport();
    if (!IsValidPushpull(msg)) { 
      trans->SendRendezvousBegin(msg_buf);
      return total_len;
    } else {
      auto is_push = msg.meta.push;
      auto key = DecodeKey(msg.data[0]);
      if (!trans->HasRemoteInfo(msg_buf, key, is_push)) {
        trans->SendRendezvousBegin(msg_buf);
        return total_len;
      }
    }
    
    // already know remote address, directly use RDMA-write 
    if (msg.meta.push && msg.meta.request) { 
      // worker, push request
      trans->SendPushRequest(msg_buf);
    } else if (msg.meta.push && !msg.meta.request) { 
      // server, push response
      trans->SendPushResponse(msg_buf);
    } else if (!msg.meta.push && msg.meta.request) { 
      // worker, pull request
      trans->SendPullRequest(msg_buf);
    } else if (!msg.meta.push && !msg.meta.request) { 
      // server, pull response
      trans->SendPullResponse(msg_buf);
    } else {
      CHECK(0) << "unexpected message type";
    }

    return total_len;
  }

  int RecvMsg(Message *msg) override {
    msg->data.clear();
    std::tuple<Endpoint *, BufferContext *> notification;
    recv_buffers_.WaitAndPop(&notification);

    Endpoint *endpoint = std::get<Endpoint *>(notification);
    BufferContext *buffer_ctx = std::get<BufferContext *>(notification);

    int total_len = 0;

    msg->meta.recver = my_node_.id;
    msg->meta.sender = endpoint->node_id;

    char *cur = buffer_ctx->buffer;

    UnpackMeta(cur, buffer_ctx->meta_len, &msg->meta);
    total_len += buffer_ctx->meta_len;
    uint64_t data_num = buffer_ctx->data_num;
    cur += buffer_ctx->meta_len;

    auto trans = endpoint->GetTransport();
    trans->Recv(msg);

    delete buffer_ctx;
    return total_len;
  }

 private:
  void InitContext(struct ibv_context *context) {
    context_ = context;
    CHECK(context_) << "ibv_context* empty";

    pd_ = ibv_alloc_pd(context_);
    CHECK(pd_) << "Failed to allocate protection domain";

    mempool_.reset(new SimpleMempool(pd_));

    comp_event_channel_ = ibv_create_comp_channel(context_);

    // TODO(clan): Replace the rough estimate here
    cq_ = ibv_create_cq(context_, kMaxConcurrentWorkRequest * 2, NULL,
                        comp_event_channel_, 0);

    CHECK(cq_) << "Failed to create completion queue";
    CHECK(!ibv_req_notify_cq(cq_, 0)) << "Failed to request CQ notification";
  }

  void ReleaseWorkRequestContext(WRContext *context, Endpoint *endpoint) {
    switch (context->type) {
      case kRendezvousStartContext:
        endpoint->free_start_ctx.Push(context);
        break;
      case kRendezvousReplyContext:
        endpoint->free_reply_ctx.Push(context);
        break;
      case kWriteContext:
        endpoint->free_write_ctx.Push(context);
        break;
      case kReceiveContext:
        endpoint->PostRecv(context);
        break;
      default:
        CHECK(0);
    }
  }
  
  void SendRendezvousReply(Endpoint* endpoint, BufferContext *buf_ctx, uint64_t origin_addr) {
    WRContext *reply_ctx = nullptr;
    endpoint->free_reply_ctx.WaitAndPop(&reply_ctx);
    RendezvousReply *resp =
        reinterpret_cast<RendezvousReply *>(reply_ctx->buffer->addr);

    char* buffer = buf_ctx->buffer;
    resp->addr = reinterpret_cast<uint64_t>(buffer);
    resp->rkey = mempool_->RemoteKey(buffer);
    resp->origin_addr = origin_addr;
    resp->idx = addr_pool_.StoreAddress(buf_ctx);

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

    CHECK_EQ(ibv_post_send(endpoint->cm_id->qp, &wr, &bad_wr), 0)
        << "ibv_post_send failed.";
  }

  void PollCQ() {
    // Pre-allocated work completions array used for polling
    struct ibv_wc wc[kMaxConcurrentWorkRequest];
    while (!should_stop_.load()) {
      int ne = ibv_poll_cq(cq_, kMaxConcurrentWorkRequest, wc);
      CHECK_GE(ne, 0);
      for (int i = 0; i < ne; ++i) {
        CHECK(wc[i].status == IBV_WC_SUCCESS)
            << "Failed status \n"
            << ibv_wc_status_str(wc[i].status) << " " << wc[i].status << " "
            << static_cast<uint64_t>(wc[i].wr_id) << " " << wc[i].vendor_err;

        WRContext *context = reinterpret_cast<WRContext *>(wc[i].wr_id);
        Endpoint *endpoint =
            reinterpret_cast<Endpoint *>(context->private_data);

        CHECK(endpoint);

        switch (wc[i].opcode) {
          case IBV_WC_SEND:
            // LOG(INFO) << "opcode: IBV_WC_SEND";
            ReleaseWorkRequestContext(context, endpoint);
            break;
          case IBV_WC_RDMA_WRITE: {
            // LOG(INFO) << "opcode: IBV_WC_RDMA_WRITE";
            // Note: This is not a struct ibv_mr*
            MessageBuffer *msg_buf =
                *reinterpret_cast<MessageBuffer **>(context->buffer->addr);
            mempool_->Free(msg_buf->inline_buf);
            delete msg_buf;
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          case IBV_WC_RECV_RDMA_WITH_IMM: {
            // LOG(INFO) << "opcode: IBV_WC_RECV_RDMA_WITH_IMM";
            uint32_t addr_idx = wc[i].imm_data;
            BufferContext *buf_ctx = addr_pool_.GetAddressAndRelease(addr_idx);
            recv_buffers_.Push(std::make_tuple(endpoint, buf_ctx));
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          case IBV_WC_RECV: {
            CHECK(wc[i].wc_flags & IBV_WC_WITH_IMM);
            uint32_t imm = wc[i].imm_data;
            struct ibv_mr *mr = context->buffer;

            if (imm == kRendezvousStart) {
              // LOG(INFO) << "opcode: IBV_WC_RECV kRendezvousStart";
              RendezvousStart *req =
                  reinterpret_cast<RendezvousStart *>(mr->addr);
                                  
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

              SendRendezvousReply(endpoint, buf_ctx, req->origin_addr);
            } else if (imm == kRendezvousReply) {
              auto trans = endpoint.GetTransport();
              // LOG(INFO) << "opcode: IBV_WC_RECV kRendezvousReply";
              RendezvousReply *resp =
                  reinterpret_cast<RendezvousReply *>(mr->addr);
              uint64_t remote_addr = resp->addr;
              uint64_t origin_addr = resp->origin_addr;
              uint32_t rkey = resp->rkey;
              uint32_t idx = resp->idx;

              MessageBuffer *msg_buf =
                  reinterpret_cast<MessageBuffer *>(origin_addr);

              // Before RDMA write, store the remote info so that 
              // subsequent write does not need repeated rendezvous 
              trans->StoreRemoteInfo(msg_buf, remote_addr, rkey, idx);
              trans->RDMAWriteWithImm(msg_buf, remote_addr, rkey, idx);
            } else {
              CHECK(0);
            }
            ReleaseWorkRequestContext(context, endpoint);
          } break;
          default:
            CHECK(0) << "Unexpected opcode: " << wc[i].opcode;
        }
      }
    }
  }

  void PollEvents() {
    int flags = fcntl(event_channel_->fd, F_GETFL);
    int rc = fcntl(event_channel_->fd, F_SETFL, flags | O_NONBLOCK);
    CHECK_GE(rc, 0);
    int error_flags = POLLERR | POLLHUP | POLLNVAL;

    while (!should_stop_.load()) {
      struct pollfd pfd = {
          .fd = event_channel_->fd, .events = POLLIN, .revents = 0};
      int ret = poll(&pfd, 1, 10);

      CHECK_GE(ret, 0) << strerror(errno);
      CHECK_EQ(pfd.revents & error_flags, 0);

      if (!(pfd.revents & POLLIN)) {
        continue;
      }

      struct rdma_cm_event *event;
      CHECK_EQ(rdma_get_cm_event(event_channel_, &event), 0);
      // TODO(clan): Reorder the list according to the event frequency
      switch (event->event) {
        case RDMA_CM_EVENT_CONNECT_REQUEST:
          OnConnectRequest(event);
          break;
        case RDMA_CM_EVENT_ADDR_RESOLVED:
          OnAddrResolved(event);
          break;
        case RDMA_CM_EVENT_ROUTE_RESOLVED:
          OnRouteResolved(event);
          break;
        case RDMA_CM_EVENT_ESTABLISHED:
          OnConnected(event);
          break;
        case RDMA_CM_EVENT_DISCONNECTED:
          OnDisconnected(event);
          break;
        case RDMA_CM_EVENT_REJECTED:
          OnRejected(event);
          break;
        default:
          CHECK(0) << "OnEvent: unknown event " << event->event << " ("
                   << rdma_event_str(event->event) << ")";
      }
      rdma_ack_cm_event(event);
    }
  }

  void OnRejected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

    auto it = endpoints_.find(endpoint->node_id);
    CHECK(it != endpoints_.end()) << "Connection not ready.";
    CHECK_EQ(endpoint->status, Endpoint::CONNECTING);
    CHECK_EQ(endpoint->cm_id, id);

    PS_VLOG(1) << "Connection rejected, retrying...";
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::REJECTED;
    }
    endpoint->cv.notify_all();
  }

  void OnConnectRequest(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_NOTNULL(id);

    CHECK_LE(sizeof(RequestContext), event->param.conn.private_data_len)
        << "RequestContext size mismatch. Actual: "
        << (size_t)event->param.conn.private_data_len
        << ", Expected: " << sizeof(RequestContext);
    CHECK_NOTNULL(event->param.conn.private_data);

    const RequestContext *remote_ctx = reinterpret_cast<const RequestContext *>(
        event->param.conn.private_data);

    const auto r = incoming_.emplace(std::make_unique<Endpoint>());
    Endpoint *endpoint = r.first->get();
    endpoint->SetNodeID(remote_ctx->node);
    endpoint->cm_id = id;
    id->context = endpoint;

    if (context_ == nullptr) {
      InitContext(id->verbs);
    }

    endpoint->Init(cq_, pd_);

    RequestContext ctx;
    ctx.node = static_cast<uint32_t>(my_node_.id);
    ctx.port = static_cast<uint16_t>(my_node_.port);
    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;
    cm_params.private_data = &ctx;
    cm_params.private_data_len = sizeof(RequestContext);

    CHECK_EQ(rdma_accept(id, &cm_params), 0)
        << "Accept RDMA connection failed: " << strerror(errno);
  }

  // Resolve a route after address is resolved
  void OnAddrResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK_EQ(rdma_resolve_route(id, kTimeoutms), 0)
        << "Resolve RDMA route failed";
  }

  // Make a connection after route is resolved
  void OnRouteResolved(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);

    if (context_ == nullptr) {
      InitContext(id->verbs);
    }

    endpoint->Init(cq_, pd_);

    RequestContext ctx;
    ctx.node = static_cast<uint32_t>(my_node_.id);
    ctx.port = static_cast<uint16_t>(my_node_.port);
    snprintf(ctx.hostname, kMaxHostnameLength, "%s", my_node_.hostname.c_str());

    struct rdma_conn_param cm_params;
    memset(&cm_params, 0, sizeof(cm_params));
    cm_params.retry_count = 7;
    cm_params.rnr_retry_count = 7;
    cm_params.private_data = &ctx;
    cm_params.private_data_len = sizeof(RequestContext);

    CHECK_EQ(rdma_connect(id, &cm_params), 0)
        << "RDMA connect failed" << strerror(errno);
  }

  void OnConnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    CHECK(id) << "rdma_cm_id not found.";
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    CHECK(endpoint) << "Endpoint not found.";

    if (cq_polling_thread_ == nullptr) {
      cq_polling_thread_.reset(new std::thread(&RDMAVan::PollCQ, this));
    }

    CHECK_EQ(endpoint->cm_id, id);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::CONNECTED;
    }
    endpoint->cv.notify_all();
    if (endpoint->node_id != my_node_.id) {
      PS_VLOG(1) << "OnConnected to Node " << endpoint->node_id;
    }
  }

  void OnDisconnected(struct rdma_cm_event *event) {
    struct rdma_cm_id *id = event->id;
    Endpoint *endpoint = reinterpret_cast<Endpoint *>(id->context);
    {
      std::lock_guard<std::mutex> lk(endpoint->connect_mu);
      endpoint->status = Endpoint::IDLE;
    }
    endpoint->cv.notify_all();
    LOG(INFO) << "OnDisconnected from Node " << endpoint->node_id;
  }

  int AlignTo(int input, int alignment) { return input / alignment * alignment; }

  AddressPool<BufferContext> addr_pool_;
  std::unique_ptr<SimpleMempool> mempool_;

  std::unique_ptr<RDMATransport> rdma_trans_;
  std::unique_ptr<IPCTransport> ipc_trans_;

  struct rdma_cm_id *listener_ = nullptr;
  std::atomic<bool> should_stop_;

  std::unordered_map<int, std::unique_ptr<Endpoint>> endpoints_;
  std::unordered_set<std::unique_ptr<Endpoint>> incoming_;

  struct rdma_event_channel *event_channel_ = nullptr;
  struct ibv_context *context_ = nullptr;

  std::unordered_map<char *, struct ibv_mr *> memory_mr_map_;

  // ibverbs protection domain
  struct ibv_pd *pd_ = nullptr;
  // Completion event channel, to wait for work completions
  struct ibv_comp_channel *comp_event_channel_ = nullptr;
  // Completion queue, to poll on work completions
  struct ibv_cq *cq_ = nullptr;
  // cq thread
  std::unique_ptr<std::thread> cq_polling_thread_;
  // event thread
  std::unique_ptr<std::thread> cm_event_polling_thread_;
  // Recv buffer queue
  ThreadsafeQueue<std::tuple<Endpoint *, BufferContext *>> recv_buffers_;

  // role is server or worker
  bool is_server_;
  // RDMA logging info
  bool enable_rdma_log_;

  std::mutex map_mu_;
  // macros for key_meta_map
  using MetaInfo = std::tuple<int, uint64_t, int>; // len, addr, rkey
  using SenderMeta = std::unordered_map<int, MetaInfo>; // sender as the key
  // (key, sender) --> MetaInfo
  std::unordered_map<ps::Key, SenderMeta> key_meta_map_;
  // a static address for the key
  std::unordered_map<ps::Key, ps::Key> key_addr_map_;
  // a static address for the length
  std::unordered_map<ps::Key, int> key_len_map_;

  // local IPC related
  bool disable_ipc_ = false;
  std::mutex local_mu_;
  std::unordered_map<int, bool> is_local_;

  std::mutex shm_mu_;
  std::unordered_map<uint64_t, void *> key_shm_addr_;

  int byteps_partition_bytes_ = 4096000;

  int ipc_copy_nthreads_;
  std::vector<std::thread*> ipc_copy_thread_list_;
  std::vector<ThreadsafeQueue<AsyncCopy>*> async_copy_queue_;
  std::atomic<unsigned long long> cpy_counter_{0};

};  // class RDMAVan

};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_

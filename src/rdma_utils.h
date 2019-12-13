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

#ifndef PS_RDMA_UTILS_H_
#define PS_RDMA_UTILS_H_

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


#define DIVUP(x, y) (((x)+(y)-1)/(y))
#define ROUNDUP(x, y) (DIVUP((x), (y))*(y))

static const int kStartDepth = 1024;
static const int kWriteDepth = kStartDepth * 2;

static const int kRxDepth = kStartDepth + kWriteDepth;
static const int kReplyDepth = kRxDepth;

static const int kSGEntry = 4;
static const int kTimeoutms = 1000;
static const int kRdmaListenBacklog = 128;
static const int kMaxConcurrentWorkRequest =
    kRxDepth + kStartDepth + kReplyDepth + kWriteDepth;
static const int kMaxHostnameLength = 16;
static const int kMaxDataFields = 4;
static const size_t kAlignment = 8;

static const int kMaxResolveRetry = 50000;
static const int kBasePort = 9010;

// should have the same prefix with BytePS shared memory
static const std::string kShmPrefix("BytePS_ShM_");

template <typename T>
static inline T align_floor(T v, T align) {
  return v - (v % align);
}

template <typename T>
static inline T align_ceil(T v, T align) {
  return align_floor(v + align - 1, align);
}

static inline void ib_malloc(void** ptr, size_t size) {
  size_t page_size = sysconf(_SC_PAGESIZE);
  void* p;
  int size_aligned = ROUNDUP(size, page_size);
  int ret = posix_memalign(&p, page_size, size_aligned);
  CHECK_EQ(ret, 0) << "posix_memalign error: " << strerror(ret);
  CHECK(p);
  memset(p, 0, size);
  *ptr = p;
}

class SimpleMempool {
 public:
  explicit SimpleMempool(struct ibv_pd *pd, size_t size = 0x10000000) {
    std::lock_guard<std::mutex> lk(mu_);
    pd_ = pd;
    struct ibv_mr *mr;
    
    // set init mempool size
    auto byteps_rdma_mempool_size = Environment::Get()->find("BYTEPS_RDMA_MEMPOOL_SIZE");
    size = byteps_rdma_mempool_size ? atoi(byteps_rdma_mempool_size) : size;
    auto byteps_rdma_mempool_num = Environment::Get()->find("BYTEPS_RDMA_MEMPOOL_NUM");
    size_t mempool_num = byteps_rdma_mempool_num ? atoi(byteps_rdma_mempool_num) : 1;
    PS_VLOG(1) << "RDMA initial mempool size set to " << size
               << ", mempool num set to " << mempool_num;
    
    for (size_t i = 0; i < mempool_num; ++i) {
      char *p;
      ib_malloc((void**) &p, size);
      total_allocated_size += size;
      CHECK(p);
      CHECK(mr = ibv_reg_mr(pd, p, size,
                            IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
      mr_list.emplace(p+size, mr); // this mr is associated with memory address range [p, p+size]
      free_list.emplace(size, p);
    }
  }

  ~SimpleMempool() {
    std::lock_guard<std::mutex> lk(mu_);
    for(auto it = mr_list.begin(); it != mr_list.end(); it++){
      CHECK_EQ(ibv_dereg_mr(it->second), 0);
      free(it->second->addr);
    }
  }

  char *Alloc(size_t size) {
    if (size == 0) {
      return nullptr;
    }

    std::lock_guard<std::mutex> lk(mu_);

    size_t proper_size = align_ceil(size, kAlignment);

    auto it = free_list.lower_bound(proper_size);

    if (it == free_list.end()) { // if there is no space left, need to allocate and register new memory
      size_t new_mem_size = total_allocated_size;
      while (proper_size > new_mem_size) {
        new_mem_size *= 2;
      }
      char *p;
      ib_malloc((void**) &p, new_mem_size);
      CHECK(p);
      struct ibv_mr *mr;
      CHECK(mr = ibv_reg_mr(pd_, p, new_mem_size, IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE));
      mr_list.emplace(p+new_mem_size, mr);
      free_list.emplace(new_mem_size, p);
      it = free_list.lower_bound(proper_size);
      PS_VLOG(1) << "Not enough memory in the pool, requested size " << proper_size << ", new allocated size " << new_mem_size;
      total_allocated_size += new_mem_size;
    }

    CHECK_NE(free_list.end(), it) << "Not enough memory";
    CHECK_GE(it->first, proper_size);

    char *addr = it->second;
    size_t space_left = it->first - proper_size;

    free_list.erase(it);
    CHECK_EQ(used_list.find(addr), used_list.end())
        << "Address is already allocated";

    used_list.emplace(addr, proper_size);

    if (space_left) {
      free_list.emplace(space_left, addr + proper_size);
    }

    return addr;
  }

  void Free(char *addr) {
    if (!addr) {
      return;
    }

    std::lock_guard<std::mutex> lk(mu_);

    auto it = used_list.find(addr);
    CHECK_NE(used_list.end(), it)
        << "Cannot find info about address: " << (uintptr_t)addr;

    size_t size = it->second;
    used_list.erase(it);
    free_list.emplace(size, addr);
  }

  uint32_t LocalKey(char *addr) {
    struct ibv_mr *mr = Addr2MR(addr);
    return mr->lkey;
  }
  uint32_t RemoteKey(char *addr) {
    struct ibv_mr *mr = Addr2MR(addr);
    return mr->rkey;
  }

 private:
  std::mutex mu_;
  std::multimap<size_t, char *> free_list;
  std::unordered_map<char *, size_t> used_list;
  struct ibv_pd *pd_;
  size_t total_allocated_size = 0;

  // first: `end` of this mr address (e.g., for mr with [addr, addr+size], point to `addr+size`)
  std::map<char *, struct ibv_mr*> mr_list;

  // convert the memory address to its associated RDMA memory region
  inline struct ibv_mr* Addr2MR(char *addr) {
    std::lock_guard<std::mutex> lk(mu_);
    auto it = mr_list.lower_bound(addr);
    CHECK_NE(it, mr_list.end()) << "cannot find the associated memory region";
    return it->second;
  }

};

class Block {
 public:
  explicit Block(SimpleMempool *pool, char *addr, int count)
      : pool(pool), addr(addr), counter(count) {}

  ~Block() {
    CHECK_EQ(counter, 0);
    pool->Free(addr);
  }

  void Release() {
    int v = counter.fetch_sub(1);
    if (v == 1) {
      delete this;
    }
  }

 private:
  SimpleMempool *pool;
  char *addr;
  std::atomic<int> counter;
};

enum MessageTypes : uint32_t {
  kRendezvousStart,
  kRendezvousReply,
};

struct RendezvousStart {
  uint64_t meta_len;
  uint64_t data_num;
  uint64_t data_len[kMaxDataFields];
  uint64_t origin_addr;
};

struct RendezvousReply {
  uint64_t addr;
  uint64_t origin_addr;
  uint32_t rkey;
  uint32_t idx;
};

enum WRContextType {
  kRendezvousStartContext,
  kRendezvousReplyContext,
  kWriteContext,
  kReceiveContext
};

struct WRContext {
  WRContextType type;
  struct ibv_mr *buffer;
  void *private_data;
};

struct BufferContext {
  char *buffer;
  size_t meta_len;
  size_t data_num;
  size_t data_len[kMaxDataFields];
};

typedef std::unique_ptr<struct ibv_mr, std::function<void(struct ibv_mr *)>>
    MRPtr;

struct MessageBuffer {
  size_t inline_len;
  char *inline_buf;
  WRContext *reserved_context;
  std::vector<SArray<char>> data;
  std::vector<std::pair<MRPtr, size_t>> mrs;
};

struct RequestContext {
  uint32_t node;
  uint16_t port;
  char hostname[kMaxHostnameLength];
};

static_assert(std::is_pod<RendezvousStart>::value,
              "RendezvousStart must be a POD type.");
static_assert(std::is_pod<RendezvousReply>::value,
              "RendezvousReply must be a POD type.");
static_assert(std::is_pod<RequestContext>::value,
              "RequestContext must be a POD type.");

static const size_t kMempoolChunkSize =
    std::max({sizeof(RendezvousStart), sizeof(RendezvousReply)});

template <typename T>
class AddressPool {
 public:
  AddressPool() {
    std::lock_guard<std::mutex> lk(mu_);
    // init the queue
    for (int i = 0; i < kMaxEntries; i++) {
      indices_.push(i);
      table_[i] = nullptr;
    }
  }

  T *GetAddressAndRelease(uint32_t index) {
    std::lock_guard<std::mutex> lk(mu_);
    T *ptr = table_[index];
    CHECK(ptr);
    indices_.push(index);
    table_[index] = nullptr;
    return ptr;
  }

  uint32_t StoreAddress(T *ptr) {
    std::lock_guard<std::mutex> lk(mu_);
    CHECK(ptr);
    CHECK(!indices_.empty())
      << "Address pool size is too small, "
      << "consider increasing kMaxEntries";
    uint32_t idx = indices_.front();
    indices_.pop();
    CHECK_EQ(table_[idx], nullptr) << idx;
    table_[idx] = ptr;
    return idx;
  }

 private:
  static const int kMaxEntries = 5120;

  std::mutex mu_;
  std::queue<uint32_t> indices_;
  T *table_[kMaxEntries];
};

struct Endpoint {
  enum ConnectionStatus { IDLE, CONNECTING, CONNECTED, REJECTED };

  ConnectionStatus status;
  int node_id;
  std::condition_variable cv;
  std::mutex connect_mu;
  struct rdma_cm_id *cm_id;
  std::unique_ptr<Transport> tran;

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

  void SetTransport(std::unique_ptr<Transport> t) { tran = t; }

  std::unique_ptr<Transport> GetTransport() { return tran; }

  void Disconnect() {
    std::unique_lock<std::mutex> lk(connect_mu);
    CHECK_EQ(rdma_disconnect(cm_id), 0) << strerror(errno);
    cv.wait(lk, [this] { return status == IDLE; });
    tran.reset();
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


};  // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_


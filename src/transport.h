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
  Transport();
  ~Transport();
  void SendPushResponse(Message &msg) {

  }

  void SendPullRequest(Message &msg) {

  }

  void SendControlMessage(Message &msg) {

  }
  
  void Recv(Message *msg);
  void SendPushRequest();
  void SendPullResponse();
}; // class Transport


class RDMATransport : public Transport {
 public:
  RDMATransport() {

  }

  ~RDMATransport();

  void SendPushRequest(Message &msg) override {

  }

  void SendPullResponse(Message &msg) override {

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
  IPCTransport() {

  }

  ~IPCTransport();

  void SendPushRequest(Message &msg) override {
    
  }

  void SendPullResponse(Message &msg) override {

  }

  void Recv(Message *msg) override {

  } 
}; // class IPCTransport



} // namespace ps

#endif  // DMLC_USE_RDMA
#endif  // PS_RDMA_VAN_H_
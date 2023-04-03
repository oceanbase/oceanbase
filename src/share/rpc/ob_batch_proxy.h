/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_RPC_OB_BATCH_PROXY_H_
#define OCEANBASE_RPC_OB_BATCH_PROXY_H_
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_proxy_macros.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace obrpc
{
enum {
  ELECTION_BATCH_REQ = 0,
  ELECTION_GROUP_BATCH_REQ = 1,
  CLOG_BATCH_REQ = 2,
  CLOG_BATCH_REQ_NODELAY = 3,
  TRX_BATCH_REQ_NODELAY = 4,
  SQL_BATCH_REQ_NODELAY1 = 5,
  SQL_BATCH_REQ_NODELAY2 = 6,
  CLOG_BATCH_REQ_NODELAY2 = 7,
  BATCH_REQ_TYPE_COUNT = 8
};

inline int64_t get_batch_delay_us(const int batch_type)
{
  int64_t delay[BATCH_REQ_TYPE_COUNT] = {2 * 1000, 2 * 1000, 1 * 1000, 0, 0, 0, 0, 0};
  return (batch_type >= 0 && batch_type < BATCH_REQ_TYPE_COUNT) ? delay[batch_type]: 0;
}

inline int64_t get_batch_buffer_size(const int batch_type)
{
  int64_t batch_buffer_size_k[BATCH_REQ_TYPE_COUNT] = {256, 256, 2048, 2048, 256, 256, 256, 2048};
  return batch_buffer_size_k[batch_type] * 1024;
}

inline bool is_hp_rpc(const int batch_type)
{
  static const bool hp_rpc_map[BATCH_REQ_TYPE_COUNT] = {true, true, false, false, false, false, false, false};
  return (batch_type >= 0 && batch_type < BATCH_REQ_TYPE_COUNT) ? hp_rpc_map[batch_type] : false;
}

inline int get_batch_thread_idx(const int batch_type)
{
  RLOCAL_INLINE(int, scount);
  int idx = batch_type;
  if (CLOG_BATCH_REQ_NODELAY == idx) {
    const int64_t total_clog_thread = min(common::get_cpu_num() / 48 + 1, 2);
    scount++;
    if (1 == scount % total_clog_thread) {
      idx = CLOG_BATCH_REQ_NODELAY2;
    }
  }
  return idx;
}

class ObIFill
{
public:
  ObIFill() {}
  virtual ~ObIFill() {}
  virtual int fill_buffer(char* buf, int64_t size, int64_t &filled_size) const = 0;
  virtual int64_t get_req_size() const = 0;
  virtual int64_t get_estimate_size() const { return 0; }
};

struct ObSimpleReqHeader
{
  void set(const uint32_t flag, const uint32_t batch_type, const uint32_t sub_type, const int32_t size) {
    type_ = ((flag << 24) & 0xff000000) | ((batch_type << 16) & 0xff0000) | sub_type;
    size_ = size;
  }
  uint32_t type_;
  int32_t size_;
};

class ObBatchPacket
{
  OB_UNIS_VERSION(1);
public:
  typedef ObSimpleReqHeader Req;
  ObBatchPacket(): size_(0), id_(0), src_(0), buf_(nullptr), src_addr_() {}
  ObBatchPacket(int32_t size, uint64_t src, char* buf)
      : size_(size), id_(gen_batch_packet_id()), src_(src), buf_(buf), src_addr_() {}
  ~ObBatchPacket() {}
  ObBatchPacket* set(int32_t size, const common::ObAddr &src, char* buf) { // for ipv6 support
    size_ = size;
    id_ = gen_batch_packet_id();
    buf_ = buf;
    // Compatible with version and previous
    if (src.using_ipv4()) {
      src_ = src.get_ipv4_server_id();
    }
    src_addr_ = src;
    return this;
  }
  uint32_t gen_batch_packet_id() { return 0; }
  Req* next(int64_t& pos) {
    Req* req = NULL;
    if (pos + sizeof(*req) >= size_) {
    } else {
      req = (Req*)(buf_ + pos);
      pos += sizeof(*req) + req->size_;
    }
    return req;
  }

  int32_t size_;
  uint32_t id_;
  uint64_t src_;
  char* buf_;
  common::ObAddr src_addr_; // for ipv6 support
  TO_STRING_KV(N_DATA_SIZE, size_,
               N_ID, id_,
               N_SERVER, src_,
               N_BUFFER, ((uint64_t)buf_),
               N_SERVER_ADDR, src_addr_);
};

class ObBatchRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObBatchRpcProxy);
  RPC_AP(PR1 post_packet, OB_BATCH, (ObBatchPacket));
  int post_batch(uint64_t tenant_id, const common::ObAddr& addr, const int64_t cluster_id, int batch_type, ObBatchPacket& pkt);
};
};
};


#endif /* OCEANBASE_RPC_OB_BATCH_PROXY_H_ */


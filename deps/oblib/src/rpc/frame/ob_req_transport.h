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

#ifndef _OCEABASE_RPC_FRAME_OB_REQ_TRANSPORT_H_
#define _OCEABASE_RPC_FRAME_OB_REQ_TRANSPORT_H_

#include "io/easy_io.h"
#include "lib/ob_errno.h"
#include "lib/net/ob_addr.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/stat/ob_diagnose_info.h"
#include "rpc/obrpc/ob_rpc_packet.h"

namespace oceanbase
{
namespace common
{
class ObDataBuffer;
} // end of namespace common

namespace rpc
{
class ObPacket;
namespace frame
{

using common::ObAddr;

enum {
  EASY_ADDR_FAMILY_RL_MOD_OFF    = 15,
  EASY_ADDR_FAMILY_RL_MOD_MASK   = (1 << EASY_ADDR_FAMILY_RL_MOD_OFF),
};

class SPAlloc {
public:
  SPAlloc() {}
  virtual ~SPAlloc() {}

  void *operator()(int64_t size) const
  {
    return alloc(size);
  }
  virtual void* alloc(int64_t size) const = 0;
};

class EasySPAlloc: public SPAlloc
{
public:
  explicit EasySPAlloc(easy_pool_t *pool)
      : pool_(pool)
  {}
  virtual ~EasySPAlloc() {}
  void *alloc(int64_t size) const
  {
    return easy_pool_alloc(pool_, static_cast<uint32_t>(size));
  }
private:
  easy_pool_t *pool_;
};

class RpcCbSPAlloc: public SPAlloc
{
public:
  explicit RpcCbSPAlloc() {}
  virtual ~RpcCbSPAlloc() {}
  void *alloc(int64_t size) const
  {
    return common::ob_malloc(size, common::ObModIds::OB_RPC);
  }
};

easy_addr_t to_ez_addr(const common::ObAddr &addr);

class ObReqTransport
{
public:
  // asynchronous callback class.
  //
  // Every asynchronous request will hold an object of this class that
  // been called after easy has detected the response packet.
  class AsyncCB
  {
  public:
    AsyncCB(int pcode)
        : low_level_cb_(NULL), dst_(), timeout_(0), tenant_id_(0),
          err_(0), pcode_(pcode), send_ts_(0), payload_(0)
    {}
    virtual ~AsyncCB() {}

    virtual AsyncCB *clone(const SPAlloc &alloc) const = 0;

    void record_stat(bool is_timeout);
    virtual int decode(void *pkt) = 0;
    virtual int process() = 0;
    virtual int get_rcode() = 0;
    virtual void reset_rcode() = 0;
    virtual void set_cloned(bool cloned) = 0;
    virtual bool get_cloned() = 0;

    // invoke when get a valid packet on protocol level, but can't decode it.
    virtual void on_invalid() { RPC_FRAME_LOG_RET(ERROR, common::OB_INVALID_ARGUMENT, "invalid packet"); }
    // invoke when can't get a valid or completed packet.
    virtual void on_timeout() { RPC_FRAME_LOG(DEBUG, "packet timeout"); }
    virtual int on_error(int err);
    void set_error(int err) { err_ = err; }
    int get_error() const { return err_; }

    void set_dst(const ObAddr &dst) { dst_ = dst; }
    void set_timeout(int64_t timeout) { timeout_ = timeout; }
    void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
    void set_send_ts(const int64_t send_ts) { send_ts_ = send_ts; }
    int64_t get_send_ts() { return send_ts_; }
    void set_payload(const int64_t payload) { payload_ = payload; }
    int64_t get_payload() { return payload_; }
    obrpc::ObRpcPacketCode get_pcode() const { return static_cast<obrpc::ObRpcPacketCode>(pcode_); }

    void* low_level_cb_;
  private:
    static const int64_t REQUEST_ITEM_COST_RT = 100 * 1000; // 100ms
  protected:
    ObAddr dst_;
    int64_t timeout_;
    uint64_t tenant_id_;
    int err_;
    int pcode_;
    int64_t send_ts_;
    int64_t payload_;
  };

  class Request {
    friend class ObReqTransport;

  public:
    Request() { reset(); }
    ~Request() { if (s_ && !async_) { destroy(); } }
    obrpc::ObRpcPacket *pkt() { return pkt_; }
    const obrpc::ObRpcPacket &const_pkt() const { return *pkt_; }
    char *buf() { return buf_; }
    int64_t buf_len() const { return buf_len_; }
    void set_cidx(int32_t cidx) { if (s_ != NULL) { s_->addr.cidx = cidx; } }
    void set_async() { async_ = true; }
    AsyncCB *cb() { return cb_; }
    int64_t timeout() const { return s_ ? static_cast<int64_t>(s_->timeout) : 0; }
    void destroy() { 
      if (s_) { 
        easy_session_destroy(s_); 
        s_ = NULL; 
      }
    }
    void reset()
    {
      s_ = NULL;
      pkt_ = NULL;
      buf_  = NULL;
      buf_len_ = 0;
      async_ = false;
      cb_ = NULL;
    }

    TO_STRING_KV("pkt", *pkt_);

  public:
    easy_session_t *s_;
    obrpc::ObRpcPacket *pkt_;
    AsyncCB *cb_;
  private:
    char *buf_;
    int64_t buf_len_;
    bool async_;
  };

  class Result {
    friend class ObReqTransport;

  public:
    Result() : pkt_(NULL) {}

    obrpc::ObRpcPacket *pkt() { return pkt_; }

  private:
    obrpc::ObRpcPacket *pkt_;
  };

public:
  ObReqTransport(easy_io_t *eio,
                 easy_io_handler_pt *handler);
  void set_sgid(int32_t sgid)
  {
    sgid_ = sgid;
  }
  void set_bucket_count(int32_t bucket_cnt)
  {
    bucket_count_ = bucket_cnt;
  }
  int ratelimit_enabled() const
  {
    return ratelimit_enabled_;
  }
  void set_ratelimit_enable(int ratelimit_enabled)
  {
    ratelimit_enabled_ = ratelimit_enabled;
  }
  void enable_use_ssl()
  {
    enable_use_ssl_ = true;
  }

  int create_session(
      easy_session_t *&session,
      const ObAddr &addr, int64_t size,
      int64_t timeout,
      const ObAddr &local_addr,
      bool do_ratelimit,
      int8_t is_bg_flow,
      const common::ObString &ssl_invited_nodes,
      const AsyncCB *cb = NULL,
      AsyncCB **newcb=NULL) const;

  int create_request(
      Request &req, const ObAddr &addr,
      int64_t size, int64_t timeout,
      const ObAddr &local_addr,
      bool do_ratelimit,
      int8_t is_bg_flow,
      const common::ObString &ssl_invited_nodes,
      const AsyncCB *cb = NULL) const;

  // synchronous interfaces
  int send(const Request &req, Result &r) const;

  // asynchronous interfaces
  int post(const Request &req) const;

private:
  int balance_assign(easy_session_t *s) const;
  ObPacket *send_session(easy_session_t *s) const;
  int post_session(easy_session_t *s) const;

private:
  static const int32_t OB_RPC_CONNECTION_COUNT_PER_THREAD = 1;
private:
  easy_io_t *eio_;
  easy_io_handler_pt *handler_;
  int32_t sgid_;
  int32_t bucket_count_;//Control the number of buckets of batch_rpc_eio
  int ratelimit_enabled_;
  bool enable_use_ssl_; // External client support enable ssl
}; // end of class ObReqTransport
} // end of namespace frame
} // end of namespace rpc
} // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_TRANSPORT_H_ */

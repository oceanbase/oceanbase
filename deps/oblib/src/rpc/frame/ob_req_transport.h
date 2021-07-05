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

namespace oceanbase {
namespace common {
class ObDataBuffer;
}  // end of namespace common

namespace rpc {
class ObPacket;
namespace frame {

using common::ObAddr;

class SPAlloc {
public:
  explicit SPAlloc(easy_pool_t* pool) : pool_(pool)
  {}
  void* operator()(uint32_t size) const
  {
    return easy_pool_alloc(pool_, size);
  }

private:
  easy_pool_t* pool_;
};

class ObReqTransport {
public:
  // asynchronous callback class.
  //
  // Every asynchronous request will hold an object of this class that
  // been called after easy has detected the response packet.
  class AsyncCB {
  public:
    AsyncCB() : dst_(), timeout_(0), tenant_id_(0), req_(NULL), send_ts_(0), payload_(0)
    {}
    virtual ~AsyncCB()
    {}

    virtual AsyncCB* clone(const SPAlloc& alloc) const = 0;

    virtual void do_first()
    {}
    virtual int decode(void* pkt) = 0;
    virtual int process() = 0;

    // invoke when get a valid packet on protocol level, but can't decode it.
    virtual void on_invalid()
    {
      RPC_FRAME_LOG(ERROR, "invalid packet");
    }
    // invoke when can't get a valid or completed packet.
    virtual void on_timeout()
    {
      RPC_FRAME_LOG(DEBUG, "packet timeout");
    }
    virtual int on_error(int err);
    int get_error() const;

    void set_dst(const ObAddr& dst)
    {
      dst_ = dst;
    }
    void set_timeout(int64_t timeout)
    {
      timeout_ = timeout;
    }
    void set_tenant_id(uint64_t tenant_id)
    {
      tenant_id_ = tenant_id;
    }
    void set_request(const easy_request_t* req)
    {
      req_ = req;
    }
    void set_send_ts(const int64_t send_ts)
    {
      send_ts_ = send_ts;
    }
    int64_t get_send_ts()
    {
      return send_ts_;
    }
    void set_payload(const int64_t payload)
    {
      payload_ = payload;
    }
    int64_t get_payload()
    {
      return payload_;
    }

  private:
    static const int64_t REQUEST_ITEM_COST_RT = 100 * 1000;  // 100ms
  protected:
    ObAddr dst_;
    int64_t timeout_;
    uint64_t tenant_id_;
    const easy_request_t* req_;
    int64_t send_ts_;
    int64_t payload_;
  };

  template <typename T>
  class Request {
    friend class ObReqTransport;

  public:
    Request()
    {
      reset();
    }
    ~Request()
    {
      if (s_ && !async_) {
        destroy();
      }
    }
    T* pkt()
    {
      return pkt_;
    }
    const T& const_pkt() const
    {
      return *pkt_;
    }
    char* buf()
    {
      return buf_;
    }
    int64_t buf_len() const
    {
      return buf_len_;
    }
    void set_cidx(int32_t cidx)
    {
      if (s_ != NULL) {
        s_->addr.cidx = cidx;
      }
    }
    void set_async()
    {
      async_ = true;
    }
    AsyncCB* cb()
    {
      return cb_;
    }
    int64_t timeout() const
    {
      return s_ ? static_cast<int64_t>(s_->timeout) : 0;
    }

    void destroy()
    {
      if (s_) {
        easy_session_destroy(s_);
        s_ = NULL;
      }
    }
    void reset()
    {
      s_ = NULL;
      pkt_ = NULL;
      buf_ = NULL;
      buf_len_ = 0;
      async_ = false;
      cb_ = NULL;
    }

    TO_STRING_KV("pkt", *pkt_);

  public:
    easy_session_t* s_;
    T* pkt_;
    AsyncCB* cb_;

  private:
    char* buf_;
    int64_t buf_len_;
    bool async_;
  };

  template <typename T>
  class Result {
    friend class ObReqTransport;

  public:
    Result() : pkt_(NULL)
    {}

    T* pkt()
    {
      return pkt_;
    }

  private:
    T* pkt_;
  };

public:
  ObReqTransport(easy_io_t* eio, easy_io_handler_pt* handler);
  void set_sgid(int32_t sgid)
  {
    sgid_ = sgid;
  }
  void set_bucket_count(int32_t bucket_cnt)
  {
    bucket_count_ = bucket_cnt;
  }
  template <typename T>
  int create_request(Request<T>& req, const ObAddr& addr, int64_t size, int64_t timeout, const ObAddr& local_addr,
      const common::ObString& ssl_invited_nodes, const AsyncCB* cb = NULL) const;

  // synchronous interfaces
  template <typename T>
  int send(const Request<T>& req, Result<T>& r) const;

  // asynchronous interfaces
  template <typename T>
  int post(const Request<T>& req) const;

  int create_session(easy_session_t*& session, const ObAddr& addr, int64_t size, int64_t timeout,
      const ObAddr& local_addr, const common::ObString& ssl_invited_nodes, const AsyncCB* cb = NULL,
      AsyncCB** newcb = NULL) const;

private:
  int balance_assign() const;
  ObPacket* send_session(easy_session_t* s) const;
  int post_session(easy_session_t* s) const;

  easy_addr_t to_ez_addr(const ObAddr& addr) const;

private:
  static const int32_t OB_RPC_CONNECTION_COUNT_PER_THREAD = 1;

private:
  easy_io_t* eio_;
  easy_io_handler_pt* handler_;
  int32_t sgid_;
  int32_t bucket_count_;  // Control the number of buckets of batch_rpc_eio
};                        // end of class ObReqTransport

template <typename T>
int ObReqTransport::create_request(Request<T>& req, const ObAddr& dst, int64_t size, int64_t timeout,
    const ObAddr& local_addr, const common::ObString& ssl_invited_nodes, const AsyncCB* cb) const
{
  int ret = common::OB_SUCCESS;
  easy_session_t* s = NULL;
  int64_t size2 = size + sizeof(T);

  if (timeout <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_FRAME_LOG(WARN, "invalid argument", K(timeout));
  } else if (!dst.is_valid()) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_FRAME_LOG(WARN, "invalid address", K(dst));
  } else if (OB_FAIL(create_session(s, dst, size2, timeout, local_addr, ssl_invited_nodes, cb, &req.cb_))) {
    RPC_FRAME_LOG(WARN, "create session fail", K(ret));
  } else if (NULL == s) {
    ret = common::OB_ERR_UNEXPECTED;
    RPC_FRAME_LOG(ERROR, "unexpected branch", K(ret));
  } else {
    req.pkt_ = new (s + 1) T();
    s->r.opacket = req.pkt_;
    req.s_ = s;

    req.buf_ = reinterpret_cast<char*>(req.pkt_ + 1);
    req.buf_len_ = size;
    req.pkt_->set_content(req.buf_, req.buf_len_);
  }
  return ret;
}

template <typename T>
int ObReqTransport::send(const Request<T>& req, Result<T>& r) const
{
  int easy_error = EASY_OK;
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(req.s_)) {
    ret = common::OB_INVALID_ARGUMENT;
    RPC_FRAME_LOG(ERROR, "invalid argument", K(req));
  } else {
    EVENT_INC(RPC_PACKET_OUT);
    EVENT_ADD(RPC_PACKET_OUT_BYTES,
        req.const_pkt().get_clen() + req.const_pkt().get_header_size() + common::OB_NET_HEADER_LENGTH);

    r.pkt_ = reinterpret_cast<T*>(send_session(req.s_));
    if (NULL == r.pkt_) {
      easy_error = req.s_->error;
      if (EASY_TIMEOUT == easy_error) {
        ret = common::OB_TIMEOUT;
        RPC_FRAME_LOG(WARN,
            "send packet fail due to session timeout. It may be caused by tenant queue "
            "being full or deadlock in RPC server side, or someting else. Please look into it "
            "in the log of server side",
            K(easy_error),
            K(req),
            K(ret));
      } else {
        ret = common::OB_RPC_SEND_ERROR;
        RPC_FRAME_LOG(WARN, "send packet fail", K(easy_error), K(req), K(ret));
      }
    } else {
      EVENT_INC(RPC_PACKET_IN);
      EVENT_ADD(RPC_PACKET_IN_BYTES, r.pkt()->get_clen() + r.pkt()->get_header_size() + common::OB_NET_HEADER_LENGTH);
    }
  }
  return ret;
}

template <typename T>
int ObReqTransport::post(const Request<T>& req) const
{
  EVENT_INC(RPC_PACKET_OUT);
  EVENT_ADD(RPC_PACKET_OUT_BYTES,
      req.const_pkt().get_clen() + req.const_pkt().get_header_size() + common::OB_NET_HEADER_LENGTH);
  return post_session(req.s_);
}

}  // end of namespace frame
}  // end of namespace rpc
}  // end of namespace oceanbase

#endif /* _OCEABASE_RPC_FRAME_OB_REQ_TRANSPORT_H_ */

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

#include <stdint.h>
#include <pthread.h>
#include "lib/net/ob_addr.h"
#include "lib/atomic/ob_atomic.h"
#ifndef OCEANBASE_COMMON_OB_TRACE_ID_H
#define OCEANBASE_COMMON_OB_TRACE_ID_H
namespace oceanbase {
namespace common {
#define TRACE_ID_FORMAT "Y%lX-%016lX"
struct ObCurTraceId {
  class Guard {
  public:
    explicit Guard(const ObAddr& addr);
    ~Guard();

  private:
    bool need_reset_;
  };
  class SeqGenerator {
  public:
    enum { BATCH = 1 << 20 };
    static uint64_t gen_seq()
    {
      static __thread uint64_t thread_seq = 0;
      if (0 == (thread_seq % BATCH)) {
        thread_seq = ATOMIC_FAA(&seq_generator_, BATCH);
      }
      return ++thread_seq;
    }
    static uint64_t seq_generator_;
  };
  class TraceId {
    OB_UNIS_VERSION(1);

  public:
    inline TraceId()
    {
      uval_[0] = 0;
      uval_[1] = 0;
    }
    inline bool is_invalid() const
    {
      return id_.seq_ == 0 ? true : false;
    }
    inline void init(const ObAddr& ip_port)
    {
      id_.seq_ = SeqGenerator::gen_seq();
      id_.ip_ = ip_port.get_ipv4();
      id_.is_user_request_ = 0;
      id_.reserved_ = 0;
      id_.port_ = static_cast<uint16_t>(ip_port.get_port());
    }
    inline int set(const uint64_t* uval)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(uval)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        uval_[0] = uval[0];
        uval_[1] = uval[1];
      }
      return ret;
    }
    inline void set(const TraceId& new_trace_id)
    {
      uval_[0] = new_trace_id.uval_[0];
      uval_[1] = new_trace_id.uval_[1];
    }
    inline const uint64_t* get() const
    {
      return uval_;
    }
    inline uint64_t get_seq() const
    {
      return id_.seq_;
    }
    inline const ObAddr get_addr() const
    {
      ObAddr addr(id_.ip_, id_.port_);
      return addr;
    }
    inline void reset()
    {
      uval_[0] = 0;
      uval_[1] = 0;
    }
    inline int get_server_addr(ObAddr& addr) const
    {
      int ret = OB_SUCCESS;
      if (is_invalid()) {
        ret = OB_ERR_UNEXPECTED;
      } else if (addr.set_ipv4_addr(id_.ip_, id_.port_)) {
      } else {
        ret = OB_ERR_UNEXPECTED;
      }
      return ret;
    }

    inline int64_t to_string(char* buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, TRACE_ID_FORMAT, uval_[0], uval_[1]);

      return pos;
    }
    inline bool equals(const TraceId& trace_id) const
    {
      return uval_[0] == trace_id.uval_[0] && uval_[1] == trace_id.uval_[1];
    }

    inline void mark_user_request()
    {
      id_.is_user_request_ = 1;
    }

    inline bool is_user_request() const
    {
      return 0 != id_.is_user_request_;
    }

  private:
    union {
      struct {
        uint32_t ip_ : 32;
        uint16_t port_ : 16;
        uint8_t is_user_request_ : 1;
        uint16_t reserved_ : 15;
        uint64_t seq_ : 64;
      } id_;
      uint64_t uval_[2];
    };
  };

  inline static void init(const ObAddr& ip_port)
  {
    TraceId* trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->init(ip_port);
    }
  }

  inline static void set(const uint64_t* uval)
  {
    TraceId* trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(uval);
    }
  }

  inline static void set(const uint64_t id, const uint64_t ipport = 0)
  {
    uint64_t uval[2] = {ipport, id};
    set(uval);
  }

  inline static void set(const TraceId& new_trace_id)
  {
    TraceId* trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(new_trace_id);
    }
  }

  inline static void reset()
  {
    TraceId* trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->reset();
    }
  }
  inline static const uint64_t* get()
  {
    TraceId* trace_id = get_trace_id();
    return trace_id->get();
  }
  inline static uint64_t get_seq()
  {
    TraceId* trace_id = get_trace_id();
    return trace_id->get_seq();
  }

  inline static const ObAddr get_addr()
  {
    TraceId* trace_id = get_trace_id();
    return trace_id->get_addr();
  }

  inline static TraceId* get_trace_id()
  {
    static RLOCAL(TraceId*, TRACE_ID);
    if (OB_UNLIKELY(TRACE_ID == nullptr)) {
      TRACE_ID = new (std::nothrow) TraceId();
    }
    return TRACE_ID;
  }

  inline static void mark_user_request()
  {
    get_trace_id()->mark_user_request();
  }

  inline static bool is_user_request()
  {
    return get_trace_id()->is_user_request();
  }
};

int32_t LogExtraHeaderCallback(
    char* buf, int32_t buf_size, int level, const char* file, int line, const char* function, pthread_t tid);

}  // namespace common
}  // namespace oceanbase

#endif

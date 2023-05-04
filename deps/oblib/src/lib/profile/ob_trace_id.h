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

#ifndef OCEANBASE_COMMON_OB_TRACE_ID_H
#define OCEANBASE_COMMON_OB_TRACE_ID_H
#include <stdint.h>
#include <pthread.h>
#include "lib/net/ob_addr.h"
#include "lib/atomic/ob_atomic.h"
namespace oceanbase
{
namespace common
{
#define TRACE_ID_FORMAT "Y%lX-%016lX"
#define TRACE_ID_FORMAT_V2 "Y%lX-%016lX-%lx-%lx"
#define TRACE_ID_FORMAT_PARAM(x) x[0], x[1], x[2], x[3]
struct ObCurTraceId
{
  class SeqGenerator
  {
  public:
    enum { BATCH = 1<<20 };
    OB_INLINE static uint64_t gen_seq() {
      RLOCAL_INLINE(uint64_t, thread_seq);
      if (0 == (thread_seq % BATCH)) {
        thread_seq = ATOMIC_FAA(&seq_generator_, BATCH);
      }
      return ++thread_seq;
    }
    static uint64_t seq_generator_;
  };
  class TraceId
  {
    OB_UNIS_VERSION(1);
  public:
    inline TraceId() { uval_[0] = 0; uval_[1] = 0; uval_[2] = 0; uval_[3] = 0; }

    inline bool is_valid() const { return id_.seq_ != 0; }
    inline bool is_invalid() const { return id_.seq_ == 0 ? true : false; }
    inline void init(const ObAddr &ip_port)
    {
      id_.seq_ = SeqGenerator::gen_seq();
      id_.is_user_request_ = 0;
      id_.is_ipv6_ = ip_port.using_ipv6();
      id_.reserved_ = 0;
      id_.port_ = static_cast<uint16_t>(ip_port.get_port());
      if (ip_port.using_ipv6()) {
        id_.ipv6_[0] = ip_port.get_ipv6_low();
        id_.ipv6_[1] = ip_port.get_ipv6_high();
      } else {
        id_.ip_ = ip_port.get_ipv4();
      }
    }
    void check_ipv6_valid() {
      if (id_.is_ipv6_ && id_.ipv6_[0] == 0) {
        fprintf(stderr, "ERROR trace id lost ipv6 addr: %s\n", lbt());
      }
    }
    inline int set(const uint64_t *uval)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(uval)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        uval_[0] = uval[0];
        uval_[1] = uval[1];
        uval_[2] = uval[2];
        uval_[3] = uval[3];
        check_ipv6_valid();
      }
      return ret;
    }
    inline void set(const TraceId &new_trace_id)
    {
      uval_[0] = new_trace_id.uval_[0];
      uval_[1] = new_trace_id.uval_[1];
      uval_[2] = new_trace_id.uval_[2];
      uval_[3] = new_trace_id.uval_[3];
      check_ipv6_valid();
    }
    inline const uint64_t* get() const { return uval_; }
    inline uint64_t get_seq() const { return id_.seq_; }
    inline const ObAddr get_addr() const
    {
      ObAddr addr(id_.ip_, id_.port_);
      if (id_.is_ipv6_) {
        addr.set_ipv6_addr(id_.ipv6_[1], id_.ipv6_[0], id_.port_);
      }
      return addr;
    }
    inline void get_uval(uint64_t (&uval)[4])
    {
      MEMCPY(&uval[0], &uval_[0], sizeof(uval_));
    }
    inline void reset() { uval_[0] = 0; uval_[1] = 0; uval_[2] = 0; uval_[3] = 0; }
    inline int get_server_addr(ObAddr &addr) const
    {
      int ret = OB_SUCCESS;
      if (is_invalid()) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        addr = get_addr();
      }
      return ret;
    }

    inline int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      common::databuff_printf(buf, buf_len, pos, TRACE_ID_FORMAT_V2, uval_[0], uval_[1], uval_[2], uval_[3]);

      return pos;
    }
    int parse_from_buf(char* buf) {
      int ret = OB_SUCCESS;
      if (4 != sscanf(buf, TRACE_ID_FORMAT_V2, &uval_[0], &uval_[1], &uval_[2], &uval_[3])) {
        ret = OB_INVALID_ARGUMENT;
      }
      return ret;
    }
    inline bool equals(const TraceId &trace_id) const
    {
      return uval_[0] == trace_id.uval_[0] && uval_[1] == trace_id.uval_[1]
          && uval_[2] == trace_id.uval_[2] && uval_[3] == trace_id.uval_[3];
    }

    inline void mark_user_request()
    {
      id_.is_user_request_ = 1;
    }

    inline bool is_user_request() const
    {
      return 0 != id_.is_user_request_;
    }

    inline int set(const char *buf)
    {
      int ret = OB_SUCCESS;
      if (OB_ISNULL(buf)) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        int32_t return_value = sscanf(buf, TRACE_ID_FORMAT, &uval_[0], &uval_[1]);
        if (0 != return_value && 2 != return_value) {
          ret = OB_ERR_UNEXPECTED;
        }
      }
      return ret;
    }

    inline int64_t hash() const
    {
      int64_t hash_value = 0;
      hash_value = common::murmurhash(&uval_[0], sizeof(uint64_t), hash_value);
      hash_value = common::murmurhash(&uval_[1], sizeof(uint64_t), hash_value);
      hash_value = common::murmurhash(&uval_[2], sizeof(uint64_t), hash_value);
      hash_value = common::murmurhash(&uval_[3], sizeof(uint64_t), hash_value);
      return hash_value;
    }

    inline int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

    inline bool operator == (const TraceId &other) const
    {
      return equals(other);
    }
  private:
    union
    {
      struct
      {
        uint32_t ip_: 32;
        uint16_t port_: 16;
        uint8_t is_user_request_: 1;
        uint8_t is_ipv6_:1;
        uint16_t reserved_: 14;
        uint64_t seq_: 64;
        uint64_t ipv6_[2];
      } id_;
      uint64_t uval_[4];
    };
  };

  inline static void init(const ObAddr &ip_port)
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->init(ip_port);
    }
  }

  inline static void set(const uint64_t *uval)
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(uval);
    }
  }

  inline static void set(const TraceId &new_trace_id)
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(new_trace_id);
    }
  }

  inline static void reset()
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->reset();
    }
  }
  static const char* get_trace_id_str()
  {
    return to_cstring(*get_trace_id());
  }
  inline static const uint64_t* get()
  {
    TraceId *trace_id = get_trace_id();
    return trace_id->get();
  }
  inline static uint64_t get_seq()
  {
    TraceId *trace_id = get_trace_id();
    return trace_id->get_seq();
  }

  inline static const ObAddr get_addr()
  {
    TraceId *trace_id = get_trace_id();
    return trace_id->get_addr();
  }

  inline static TraceId *get_trace_id()
  {
    #ifdef COMPILE_DLL_MODE
    return &trace_id_;
    #else
    static thread_local TraceId TRACE_ID;
    return &TRACE_ID;
    #endif
  }

  inline static void mark_user_request()
  {
    get_trace_id()->mark_user_request();
  }

  inline static bool is_user_request()
  {
    return get_trace_id()->is_user_request();
  }

  inline static void set(const char *buf)
  {
    TraceId *trace_id = get_trace_id();
    if (NULL != trace_id) {
      trace_id->set(buf);
    }
  }
#ifdef COMPILE_DLL_MODE
private:
  static TLOCAL(TraceId, trace_id_);
#endif
};

class ObTraceIdGuard final
{
public:
  explicit ObTraceIdGuard(const ObCurTraceId::TraceId &trace_id)
    : old_trace_id_(), is_old_trace_saved_(false)
  {
    if (NULL != ObCurTraceId::get_trace_id()) {
      old_trace_id_ = *ObCurTraceId::get_trace_id();
      is_old_trace_saved_ = true;
    }
    ObCurTraceId::set(trace_id);
  }
  ~ObTraceIdGuard()
  {
    if (is_old_trace_saved_) {
      ObCurTraceId::set(old_trace_id_);
    }
  }
private:
  ObCurTraceId::TraceId old_trace_id_;
  bool is_old_trace_saved_;
};

}// namespace common
}// namespace oceanbase


#endif

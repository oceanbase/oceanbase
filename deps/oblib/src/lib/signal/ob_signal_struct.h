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

#ifndef OCEANBASE_SIGNAL_STRUCT_H_
#define OCEANBASE_SIGNAL_STRUCT_H_

#include <stdio.h>
#include <stdint.h>
#include <time.h>
#include <signal.h>
#include "lib/string/ob_string.h"

namespace oceanbase
{
namespace common
{

extern void ob_signal_handler(int, siginfo_t *, void *);
typedef void (*signal_handler_t)(int, siginfo_t *, void *);
extern signal_handler_t &get_signal_handler();
extern bool g_redirect_handler;
extern const int MP_SIG; // MP means MULTI-PURPOSE
extern const int SIG_STACK_SIZE;
extern uint64_t g_rlimit_core;

class DTraceId
{
public:
  DTraceId()
    : v_(0) {}
  int64_t value() { return v_; }
  static DTraceId gen_trace_id();
private:
  int64_t v_;
};
extern DTraceId &get_tl_trace_id();
extern DTraceId set_tl_trace_id(DTraceId &id);
class DTraceIdGuard
{
public:
  DTraceIdGuard(DTraceId &id) : bak_(set_tl_trace_id(id)) {}
  ~DTraceIdGuard() { set_tl_trace_id(bak_); }
private:
  DTraceId bak_;
};

extern int install_ob_signal_handler();

enum ObSigRequestCode
{
  VERB_LEVEL_1, // bt only
  VERB_LEVEL_2, // bt and other(e.g. sql str)
  INVALID_LELVEL = 8
};

/*
  @param code: 诊断级别
  @param need_hang: 线程是否hang住，这里指二次接收信号的线程, 而非直接接收request的线程
 */
extern int send_request_and_wait(ObSigRequestCode code, int exclude_id);

class ObProcMaps
{
public:
  static ObProcMaps &get_instance()
  {
    static ObProcMaps the_one;
    return the_one;
  }
  void load_maps();
  const char *get_maps(int64_t &len) const
  {
    len = pos_;
    return buf_;
  }
private:
  ObProcMaps()
    : pos_(0) {}
private:
  char buf_[8192];
  int64_t pos_;
};

class ObSigHandler;
class ObSigProcessor;
struct ObSigHandlerCtx
{
  ObSigHandlerCtx()
    : fd_{-1, -1}, fd2_{-1, -1}, trace_id_(),
      handler_(nullptr), processor_(nullptr), lock_(0), req_id_(0)
  {}
  void atomic_set_req_id(int64_t req_id);
  void lock();
  void unlock();
  int fd_[2];
  int fd2_[2];
  DTraceId trace_id_;
  ObSigHandler *handler_;
  ObSigProcessor *processor_;
  int64_t lock_;
  int64_t req_id_;
};

extern ObSigHandlerCtx g_sig_handler_ctx_;

class ObSqlInfoGuard
{
public:
	ObSqlInfoGuard(const ObString &sql)
      : sql_(sql)
	{
    last_ = get_cur_guard();
    get_cur_guard() = this;
  }
	~ObSqlInfoGuard()
  {
    get_cur_guard() = last_;
  }
  static ObSqlInfoGuard *&get_cur_guard()
  {
    static thread_local ObSqlInfoGuard *cur_guard = NULL;
    return cur_guard;
  }
public:
  ObString sql_;
private:

  ObSqlInfoGuard *last_;
};

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SIGNAL_STRUCT_H_

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

namespace oceanbase {
namespace common {

extern void ob_signal_handler(int, siginfo_t*, void*);
typedef void (*signal_handler_t)(int, siginfo_t*, void*);
extern __thread signal_handler_t tl_handler;
extern const int MP_SIG;  // MP means MULTI-PURPOSE
extern const int SIG_STACK_SIZE;

class DTraceId {
public:
  DTraceId() : v_(0)
  {}
  int64_t value()
  {
    return v_;
  }
  static DTraceId gen_trace_id();

private:
  int64_t v_;
};
extern __thread char tl_trace_id_buf[sizeof(DTraceId)];
extern __thread DTraceId* tl_trace_id;
extern DTraceId& get_tl_trace_id();
extern DTraceId set_tl_trace_id(DTraceId& id);
class DTraceIdGuard {
public:
  DTraceIdGuard(DTraceId& id) : bak_(set_tl_trace_id(id))
  {}
  ~DTraceIdGuard()
  {
    set_tl_trace_id(bak_);
  }

private:
  DTraceId bak_;
};

extern int install_ob_signal_handler();

class ObISigHandler {
public:
  virtual void handle() = 0;
  virtual ~ObISigHandler() = 0;
};
inline ObISigHandler::~ObISigHandler()
{}

// Diagnostic level, the higher the level, the more detailed the output information
enum ObSigRequestCode {
  VERB_LEVEL_1,  // bt only
  VERB_LEVEL_2,  // bt and other(e.g. sql str)
  INVALID_LELVEL = 8
};

/*
  @param code: Diagnostic level
  @param need_hang: Whether the thread hangs, here refers to the thread that receives the signal twice, not the thread
  that directly receives the request
 */
extern int send_request_and_wait(ObSigRequestCode code, int exclude_id);

}  // namespace common
}  // namespace oceanbase

#endif  // OCEANBASE_SIGNAL_STRUCT_H_

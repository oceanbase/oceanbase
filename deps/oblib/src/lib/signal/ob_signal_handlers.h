/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SIGNAL_HANDLERS_H_
#define OCEANBASE_SIGNAL_HANDLERS_H_

#include <stdio.h>
#include <stdint.h>
#include <time.h>

namespace oceanbase
{
namespace common
{
class ObSigFaststack
{
public:
  ObSigFaststack(const ObSigFaststack &) = delete;
  ObSigFaststack& operator=(const ObSigFaststack &) = delete;
  static ObSigFaststack &get_instance();
  inline int64_t get_min_interval() const { return min_interval_; }
  inline void set_min_interval(int64_t interval) { min_interval_ = interval; }
private:
  ObSigFaststack();
  ~ObSigFaststack();
private:
  int64_t min_interval_;
};
extern int minicoredump(int sig, int64_t tid, pid_t& pid);
extern int faststack();
extern bool need_close_socket_fd_when_core_dump;
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SIGNAL_HANDLERS_H_

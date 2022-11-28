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

#ifndef OCEANBASE_SRC_PL_OB_PL_SYS_BREAKPOINT_H_
#define OCEANBASE_SRC_PL_OB_PL_SYS_BREAKPOINT_H_

#include "lib/lock/ob_spin_lock.h"
#include "ob_pl_breakpoint.h"

extern "C" {
  void __ob_register_jit_debug_code();
  void __ob_notify_pl_finished();
}

namespace oceanbase
{
namespace pl
{
namespace debugger
{
class ObPLSysBreakpointMgr
{
public:
  ObPLSysBreakpointMgr()
    : lock_(), ref_cnt_(0), register_jit_debug_code_(), notify_pl_finished_() {}

  static int build_instance();
  static ObPLSysBreakpointMgr* get_instance() { return instance_; }

  int set_breakpoint(int pid);
  int remove_breakpoint(int pid);

  int trigger_jit_breakpoint();
  int trigger_notify_breakpoint();

  bool is_jit_breakpoint(int64_t addr);
  bool is_notify_breakpoint(int64_t addr);

  bool is_sys_breakpoint(int64_t addr)
  {
    return is_jit_breakpoint(addr) || is_notify_breakpoint(addr);
  }
  common::ObSpinLock& get_lock() { return lock_; }
  ObPLBreakpoint* get_breakpoint(int64_t addr)
  {
    ObPLBreakpoint *bp = NULL;
    if (is_jit_breakpoint(addr)) {
      bp = &register_jit_debug_code_;
    } else if (is_notify_breakpoint(addr)) {
      bp = &notify_pl_finished_;
    }
    return bp;
  }

  TO_STRING_KV(K_(ref_cnt), K_(register_jit_debug_code), K_(notify_pl_finished));

private:

  int set_system_breakpoint(int pid, int64_t addr, ObPLBreakpoint &bp);

private:
  static ObPLSysBreakpointMgr *instance_;

private:
  common::ObSpinLock lock_;
  int ref_cnt_;
  ObPLBreakpoint register_jit_debug_code_;
  ObPLBreakpoint notify_pl_finished_;
};

} // end of debugger
} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_OB_PL_SYS_BREAKPOINT_H_
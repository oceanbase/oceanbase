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

#ifndef OCEANBASE_SRC_PL_OB_PL_DEBUGGEE_CTRL_H_
#define OCEANBASE_SRC_PL_OB_PL_DEBUGGEE_CTRL_H_

#include "objit/ob_llvm_helper.h"

namespace oceanbase
{
namespace jit
{
class ObDWARFHelper;
class ObLineAddress;
}
namespace pl
{
namespace debugger
{
class ObPLDebugger;
class ObPLSysBreakpointMgr;

class ObPLDebuggeeCtrl
{
public:
  ObPLDebuggeeCtrl(ObPLDebugger &debugger,
                   common::ObSpinLock &lock,
                   ObPLCommand &command,
                   common::ObLightyQueue &command_queue,
                   common::ObLightyQueue &result_queue,
                   ObPLBreakpointMgr &breakpoint_mgr)
    : debugger_(debugger),
      lock_(lock),
      command_(command),
      command_queue_(command_queue),
      result_queue_(result_queue),
      breakpoint_mgr_(breakpoint_mgr),
      sys_breakpoint_mgr_(ObPLSysBreakpointMgr::get_instance()) {}

public:
  static int debug_thread(void *data);

  int debug_start();
  int debug_stop();

  int attach_target_thread();
  void run();
  int detach_target_thread();

  int print_backtrace();
  int print_backtrace_v2();
  int set_breakpoint();
  int op_breakpoint();
  int show_breakpoints();
  int get_value();
  int get_values();
  int continue_execution_by_breakflag();

  int continue_execution_to_function_start();
  int find_address_by_function_line(const common::ObString&, int, int64_t&);

  int find_all_line_address(common::ObIArray<jit::ObLineAddress> &address);
  int find_function_from_pc(int64_t pc, bool &found);

  int single_step_instruction(); //single step without checking breakpoints
  int step_over_breakpoint();
  int handle_sigtrap(siginfo_t info);
  int wait_for_signal();
  uint64_t get_pc();
  int set_pc(uint64_t pc);

  int continue_execution();
  int step_over();
  int step_into();
  int step_any_return();
  int step_abort();
  int get_signal_info(siginfo_t &info);
  int set_deferred_breakpoints();

  bool is_jit_breakpoint();
  int notify();

  int update_runtime_info_by_start();
  int update_runtime_info_by_sigtrap();
  int handle_command();

  int get_dwarf_helpers(common::ObIArray<jit::ObDWARFHelper *> &dwarf_helpers);

  ObPLDebugger& get_debugger() { return debugger_; }

private:
  ObPLDebugger &debugger_;
  sql::ObSQLSessionInfo *sql_session_;

private:
  common::ObSpinLock &lock_;
  ObPLCommand &command_;
  common::ObLightyQueue &command_queue_;
  common::ObLightyQueue &result_queue_;

private:
  int debug_thread_pid_;
  int target_thread_pid_;
  int target_thread_group_id_;
  common::ObCond wait_for_traced_;

  common::ObArenaAllocator allocator_;

private:
  ObPLBreakpointMgr &breakpoint_mgr_;
  ObPLSysBreakpointMgr *sys_breakpoint_mgr_;
};

} // end of debugger
} // end of pl
} // end of oceanbase

#endif

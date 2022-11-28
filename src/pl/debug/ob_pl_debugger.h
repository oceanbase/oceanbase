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

#ifndef OCEANBASE_SRC_PL_OB_PL_DEBUGGER_H_
#define OCEANBASE_SRC_PL_OB_PL_DEBUGGER_H_

#include <unordered_map>
#include <cstdint>
#include <signal.h>
#include <fcntl.h>

#include "ob_pl_breakpoint.h"
#include "ob_pl_sys_breakpoint.h"
#include "ob_pl_command.h"
#include "lib/queue/ob_lighty_queue.h"
#include "ob_pl_debugger_ctrl.h"
#include "ob_pl_debuggee_ctrl.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}
namespace pl
{
class ObPLExecState;
namespace debugger
{

class ObPLCommand;

class ObPLDebugger
{
public:
  ObPLDebugger (uint32_t id) :

    runtime_info_(),

    lock_(),
    command_(),
    command_queue_(),
    result_queue_(),

    debugger_ctrl_(*this, lock_, command_, command_queue_, result_queue_),
    debuggee_ctrl_(*this, lock_, command_, command_queue_, result_queue_, breakpoint_mgr_),

    breakpoint_mgr_(),

    init_(false),
    id_(id),
    is_debugger_running_(false),

    is_force_debug_off_(false),

    allocator_(),
    sql_session_(NULL),

    timeout_(3600000000),
    timeout_timestamp_(common::ObTimeUtility::current_time() + timeout_),
    timeout_behaviour_(ObPLTimeoutBehaviour::continue_on_timeout) {}

  ~ObPLDebugger() { destroy(); }

public:
  enum ObPLNamespace
  {
    namespace_cursor = 0,
    namespace_pkgspec_or_toplevel = 1,
    namespace_pkg_body = 2,
    namespace_trigger = 3,
    namespace_none = 255
  };

  enum ObPLReasons
  {
    reason_none = 0,
    reason_interpreter_starting = 2,
    reason_breakpoint = 3,
    reason_enter = 6,
    reason_return = 7,
    reason_finish = 8,
    reason_line = 9,
    reason_interrupt = 10,
    reason_exception = 11,
    reason_exit = 15,
    reason_handler = 16,
    reason_timeout = 17,
    reason_instantiate = 20,
    reason_abort = 21,
    reason_knl_exit = 25
  };

  struct ProgramInfo
  {
    ProgramInfo() { reset(); }

    void reset()
    {
      namespace_ = namespace_none;
      name_.reset();
      owner_.reset();
      allocator_.reset();
    }

    int namespace_;
    common::ObString name_;
    common::ObString owner_;
    common::ObArenaAllocator allocator_;

    TO_STRING_KV(K_(namespace), K_(name), K_(owner));
  };

  struct RuntimeInfo
  {
    RuntimeInfo() { reset(); }

    void reset()
    {
      line_ = -1;
      is_terminated_ = true;
      breakpoint_ = -1;
      stack_depth_ = 0;
      reason_ = reason_none;
      program_info_.reset();
    }

    int line_;
    bool is_terminated_;
    int breakpoint_;
    int stack_depth_;
    int reason_;
    ProgramInfo program_info_;

    TO_STRING_KV(K_(line), K_(is_terminated), K_(breakpoint), K_(stack_depth), K_(reason), K_(program_info));
  };

public:

  uint32_t get_debugger_id() { return id_; }

  int initialize();
  int destroy();
  int debug_start();
  int debug_stop();
  int notify();

  int set_sql_session(sql::ObSQLSessionInfo *sql_session);
  sql::ObSQLSessionInfo* get_sql_session() { return sql_session_; }

  bool need_debug_off();

  bool is_debug_thread_running() { return is_debugger_running_; }
  void set_debug_thread_running(bool v) { is_debugger_running_ = v; }

  bool is_debug_on() { return init_; }

  ObPLDebuggerCtrl& get_debugger_ctrl() { return debugger_ctrl_; }
  ObPLDebuggeeCtrl& get_debuggee_ctrl() { return debuggee_ctrl_; }

  int get_runtime_info(common::ObObj &result);
  int ping();

  int set_timeout(int64_t timeout) { timeout_ = timeout * 1000000; return common::OB_SUCCESS; }
  int set_timeout_behaviour(int behaviour)
  {
    timeout_behaviour_ = (ObPLTimeoutBehaviour)behaviour;
    return common::OB_SUCCESS;
  }
  int get_timeout_behaviour(int &behaviour)
  {
    behaviour = (int)timeout_behaviour_;
    return common::OB_SUCCESS;
  }

  bool is_continue_on_timeout()
  {
    return ObPLTimeoutBehaviour::continue_on_timeout == timeout_behaviour_;
  }
  bool is_stop_on_timeout()
  {
    return ObPLTimeoutBehaviour::nodebug_on_timeout == timeout_behaviour_
      || ObPLTimeoutBehaviour::abort_on_timeout == timeout_behaviour_;
  }
  void set_force_debug_off() { is_force_debug_off_ = true; }

  bool is_already_timeout();

public:
  int print_backtrace();
  int get_value();
  int get_values();

  int get_cursor_value(const common::ObObj &obj, common::ObSqlString &append_string);
  int get_record_value(const common::ObObj &obj, common::ObSqlString &append_string);
  int get_collection_value(const common::ObObj &obj, common::ObSqlString &append_string);
  int get_user_value(const common::ObObj &obj, common::ObSqlString &append_string);
  int get_obj_value(const common::ObObj &obj, common::ObSqlString &append_string);

  int transform_runtime_info_to_extend(common::ObObj &result);

  int update_runtime_info_by_exit();
  int update_program_info();

  int get_exec_stack(common::ObIArray<pl::ObPLExecState *>*& stack);
  int get_top_function(pl::ObPLExecState*& pl_state);

  int get_stack_depth() { return runtime_info_.stack_depth_; }
  void set_reason_start() { runtime_info_.reason_ = ObPLReasons::reason_interpreter_starting; }

  RuntimeInfo& get_runtime_info() { return runtime_info_; }
  ObPLCommand& get_command() { return command_; }

private:
  RuntimeInfo runtime_info_;

private:
  common::ObSpinLock lock_;
  ObPLCommand command_;
  common::ObLightyQueue command_queue_;
  common::ObLightyQueue result_queue_;

  ObPLDebuggerCtrl debugger_ctrl_;
  ObPLDebuggeeCtrl debuggee_ctrl_;

  ObPLBreakpointMgr breakpoint_mgr_;

private:
  bool init_;
  int64_t id_; // id from debug_mgr.map_

  bool is_debugger_running_;
  bool is_force_debug_off_;

  common::ObArenaAllocator allocator_;
  sql::ObSQLSessionInfo *sql_session_;

private:
  int64_t timeout_;
  int64_t timeout_timestamp_;
  ObPLTimeoutBehaviour timeout_behaviour_;

public:
  TO_STRING_KV(K_(id), K(init_), K(runtime_info_), K(timeout_), K(timeout_timestamp_));
};

} // end of debugger
} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_OB_PL_DEBUGGER_H_

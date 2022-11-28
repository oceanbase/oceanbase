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

#ifndef OCEANBASE_SRC_PL_OB_PL_DEBUGGER_CTRL_H_
#define OCEANBASE_SRC_PL_OB_PL_DEBUGGER_CTRL_H_

#include "ob_pl_command.h"

namespace oceanbase
{
namespace pl
{
namespace debugger
{

class ObPLDebugger;
class ObPLDebuggerCtrl
{
public:
  ObPLDebuggerCtrl(ObPLDebugger &debugger,
                   common::ObSpinLock &lock,
                   ObPLCommand &command,
                   common::ObLightyQueue &command_queue,
                   common::ObLightyQueue &result_queue)
    : debugger_(debugger),
      lock_(lock),
      command_(command),
      command_queue_(command_queue),
      result_queue_(result_queue) {}

public:
  int print_backtrace(common::ObString &backtrace);
  int print_backtrace(common::ObObj &backtrace);
  int continue_by_flag(ObPLBreakFlag breakflag);
  int set_breakpoint(common::ObString func, int line, int& bp_num);
  int disable_breakpoint(int bp_num);
  int enable_breakpoint(int bp_num);
  int delete_breakpoint(int bp_num);
  int show_breakpoints(common::ObString &result);
  int get_value(common::ObString &name, int frame, common::ObString &result);
  int get_values(common::ObString &result);
  int ping();

private:
  int execute();

private:
  ObPLDebugger &debugger_;

private:
  common::ObSpinLock &lock_;
  ObPLCommand &command_;
  common::ObLightyQueue &command_queue_;
  common::ObLightyQueue &result_queue_;
};

} // end of debugger
} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_OB_PL_DEBUGGER_CTRL_H_

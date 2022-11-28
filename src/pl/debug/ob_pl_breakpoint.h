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

#ifndef OCEANBASE_SRC_PL_OB_PL_BREAKPOINT_H_
#define OCEANBASE_SRC_PL_OB_PL_BREAKPOINT_H_

#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace pl
{
namespace debugger
{

const common::ObString to_hex(std::intptr_t);

class ObPLBreakpoint
{
public:
  ObPLBreakpoint()
    : number_(-1),
      pid_(-1),
      addr_(-1),
      enabled_(false),
      saved_data_(0),
      module_(),
      line_(-1) {}

  ObPLBreakpoint(
      int number, pid_t pid, std::intptr_t addr, common::ObString &module, int line)
    : number_(number),
      pid_(pid),
      addr_(addr),
      enabled_(false),
      saved_data_(0),
      module_(module),
      line_(line) {}

  void set_pid(pid_t pid) { pid_ = pid; }
  void set_addr(std::intptr_t addr) { addr_ = addr; }
  void set_number(int number) { number_ = number; }

  void enable();
  void disable();

  bool is_enabled() const { return enabled_; }

  bool is_set_by_user() const { return number_ != -1; }

  void set_address(std::intptr_t address) { addr_ = address; }
  auto get_address() const -> std::intptr_t { return addr_; }

  int get_number() const { return number_; }

  int get_line() const { return line_; }

  const common::ObString& get_module() const { return module_; }

  TO_STRING_KV(K(number_),
               K(pid_),
               K(to_hex(addr_)),
               K(enabled_),
               K(to_hex(saved_data_)),
               K(module_),
               K(line_));

private:
  int number_;
  pid_t pid_;
  std::intptr_t addr_;
  bool enabled_; // already set sigtrap to address

#if defined(__aarch64__)
  uint64_t saved_data_;
#else
  uint8_t saved_data_; //data which used to be at the ObPLBreakpoint address
#endif

  common::ObString module_;
  int line_;
};

class ObPLBreakpointMgr
{
public:
  ObPLBreakpointMgr()
    : target_thread_pid_(0),
      next_breakpoint_number_(1),
      all_deferred_breakpoints_(),
      allocator_() {}

  int init();
  void destroy() { IGNORE_RETURN remove_all_breakpoints(); }

  void set_target_thread_pid(int pid) { target_thread_pid_ = pid; }

  int set_breakpoint(
    std::intptr_t address, int line = (-1),
    common::ObString func = common::ObString(), bool by_user = false);

  int remove_breakpoint(std::intptr_t address);

  int set_deferred_breakpoint(ObPLBreakpoint *breakpoint, std::intptr_t address);
  int save_deferred_breakpoint(common::ObString &func, int line);
  int update_deferred_breakpoint(common::ObIArray<ObPLBreakpoint*> &new_breakpoints);
  common::ObIArray<ObPLBreakpoint*>& get_deferred_breakpoints()
  {
    return all_deferred_breakpoints_;
  }

  bool has_breakpoint(std::intptr_t address);
  int get_breakpoint(std::intptr_t address, ObPLBreakpoint *&breakpoint);
  int get_breakpoint_by_number(int number, ObPLBreakpoint *&breakpoint);

  int show_breakpoints(common::ObSqlString &result);

  int remove_all_breakpoints();

  void print();

private:
  int target_thread_pid_;
  int next_breakpoint_number_;

  static const int64_t BREAKPOINT_MAP_SIZE = 1024;

  common::hash::ObHashMap<std::intptr_t, ObPLBreakpoint*> all_breakpoints_;
  common::ObSEArray<ObPLBreakpoint*, 4> all_deferred_breakpoints_;
  common::ObArenaAllocator allocator_;
};

} // end of debugger
} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_OB_PL_BREAKPOINT_H_

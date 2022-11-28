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

#ifndef OCEANBASE_SRC_PL_OB_PL_REGISTERS_H_
#define OCEANBASE_SRC_PL_OB_PL_REGISTERS_H_

#include <sys/user.h>
#include <algorithm>
#if defined(__aarch64__)
#include <linux/elf.h>
#endif

namespace oceanbase
{
namespace pl
{
namespace debugger
{

enum class reg
{
  rax, rbx, rcx, rdx,
  rdi, rsi, rbp, rsp,
  r8,  r9,  r10, r11,
  r12, r13, r14, r15,
  rip, rflags,    cs,
  orig_rax, fs_base,
  gs_base,
  fs, gs, ss, ds, es
};

static constexpr std::size_t n_registers = 27;

struct reg_descriptor
{
  reg r;
  int dwarf_r;
  std::string name;
};

//have a look in /usr/include/sys/user.h for how to lay this out
static const reg_descriptor g_register_descriptors[n_registers] = {
        { reg::r15, 15, "r15" },
        { reg::r14, 14, "r14" },
        { reg::r13, 13, "r13" },
        { reg::r12, 12, "r12" },
        { reg::rbp, 6, "rbp" },
        { reg::rbx, 3, "rbx" },
        { reg::r11, 11, "r11" },
        { reg::r10, 10, "r10" },
        { reg::r9, 9, "r9" },
        { reg::r8, 8, "r8" },
        { reg::rax, 0, "rax" },
        { reg::rcx, 2, "rcx" },
        { reg::rdx, 1, "rdx" },
        { reg::rsi, 4, "rsi" },
        { reg::rdi, 5, "rdi" },
        { reg::orig_rax, -1, "orig_rax" },
        { reg::rip, -1, "rip" },
        { reg::cs, 51, "cs" },
        { reg::rflags, 49, "eflags" },
        { reg::rsp, 7, "rsp" },
        { reg::ss, 52, "ss" },
        { reg::fs_base, 58, "fs_base" },
        { reg::gs_base, 59, "gs_base" },
        { reg::ds, 53, "ds" },
        { reg::es, 50, "es" },
        { reg::fs, 54, "fs" },
        { reg::gs, 55, "gs" },
};

#if defined(__aarch64__)
inline uint64_t get_register_value(pid_t pid, reg r)
{
  struct user_pt_regs regs;
  struct iovec io;
  uint64_t result = -1;
  io.iov_base = &regs;
  io.iov_len = sizeof(regs);
  ptrace(PTRACE_GETREGSET, pid, (void*)NT_PRSTATUS, &io);
  if (r == reg::rip) {
    result = regs.pc;
  } else if (r == reg::rbp) {
    result = regs.sp;
  }
  return result;
}

inline void set_register_value(pid_t pid, reg r, uint64_t value)
{
  struct user_pt_regs regs;
  struct iovec io;
  io.iov_base = &regs;
  io.iov_len = sizeof(regs);
  ptrace(PTRACE_GETREGSET, pid, (void*)NT_PRSTATUS, &io);
  if (r == reg::rip) {
    regs.pc = value;
  }
  ptrace(PTRACE_SETREGSET, pid, (void*)NT_PRSTATUS, &io);
}
#else
inline uint64_t get_register_value(pid_t pid, reg r)
{
  user_regs_struct regs;
  ptrace(PTRACE_GETREGS, pid, nullptr, &regs);
  int i = 0;
  for (; i < n_registers; ++i) {
    if (g_register_descriptors[i].r == r) {
      break;
    }
  }
  return *(reinterpret_cast<uint64_t*>(&regs) + i);
}

inline void set_register_value(pid_t pid, reg r, uint64_t value)
{
  user_regs_struct regs;
  ptrace(PTRACE_GETREGS, pid, nullptr, &regs);
  int i = 0;
  for (; i < n_registers; ++i) {
    if (g_register_descriptors[i].r == r) {
      break;
    }
  }
  *(reinterpret_cast<uint64_t*>(&regs) + i) = value;
  ptrace(PTRACE_SETREGS, pid, nullptr, &regs);
}
#endif

inline uint64_t get_register_value_from_dwarf_register (pid_t pid, unsigned regnum)
{
  int i = 0;
  for (; i < n_registers; ++i) {
    if (g_register_descriptors[i].dwarf_r == regnum) {
      break;
    }
  }
  if (i == n_registers) {
    throw std::out_of_range{"Unknown dwarf register"};
  }
  return get_register_value(pid, g_register_descriptors[i].r);
}

inline std::string get_register_name(reg r)
{
  std::string name;
  int i = 0;
  for (; i < n_registers; ++i) {
    if (g_register_descriptors[i].r == r) {
      name = g_register_descriptors[i].name;
      break;
    }
  }
  return name;
}

inline reg get_register_from_name(const std::string& name)
{
  int i = 0;
  for (; i < n_registers; ++i) {
    if (g_register_descriptors[i].name == name) {
      break;
    }
  }
  return g_register_descriptors[i].r;
}

} // end of debugger
} // end of pl
} // end of oceanbase

#endif // OCEANBASE_SRC_PL_OB_PL_REGISTERS_H_

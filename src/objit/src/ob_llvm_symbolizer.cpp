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

#ifndef ENABLE_SANITY
#else
#include "objit/ob_llvm_symbolizer.h"
#include "llvm/DebugInfo/Symbolize/Symbolize.h"
#include "llvm/Support/COM.h"
#include "llvm/Support/ManagedStatic.h"
#include <cstdio>
#include <cstring>
#include <string>
#include "lib/ob_define.h"

using namespace llvm;
using namespace symbolize;
using namespace oceanbase;

template<typename T>
static bool error(Expected<T> &ResOrErr)
{
  if (ResOrErr)
    return false;
  logAllUnhandledErrors(ResOrErr.takeError(), errs(),
                        "LLVMSymbolizer: error reading file: ");
  return true;
}

namespace oceanbase
{
namespace common
{

void print_symbol(void *addr, const char *func_name, const char *file_name, uint32_t line)
{
  fprintf(stderr, "%-14p %s %s:%u\n", addr, func_name, file_name, line);
}

int backtrace_symbolize(void **addrs, int32_t n_addr, SymbolizeCb cb)
{
  int ret = common::OB_SUCCESS;

  llvm::sys::InitializeCOMRAII COM(llvm::sys::COMThreadingMode::MultiThreaded);
  LLVMSymbolizer::Options Opts;
  Opts.PrintFunctions = DINameKind::LinkageName;
  Opts.UseSymbolTable = true;
  Opts.Demangle = true;
  Opts.RelativeAddresses = false;
  Opts.DefaultArch = "";
  LLVMSymbolizer Symbolizer(Opts);

  const int kMaxInputStringLength = 1024;
  char InputString[kMaxInputStringLength];

  int32_t idx = 0;
  while (idx < n_addr) {
    std::string ModuleName("/proc/self/exe");
    SectionedAddress ModuleOffset;
    ModuleOffset.Address = (uint64_t)addrs[idx];
    auto ResOrErr =
        Symbolizer.symbolizeCode(ModuleName, ModuleOffset);
    std::string func_name = error(ResOrErr) ? "?" : ResOrErr.get().FunctionName;
    std::string file_name = error(ResOrErr) ? "?" : ResOrErr.get().FileName;
    uint32_t line = error(ResOrErr) ? 0 : ResOrErr.get().Line;
    cb(addrs[idx], func_name.c_str(), file_name.c_str(), line);
    idx++;
  }

  return ret;
}

int backtrace_symbolize(void **addrs, int32_t n_addr)
{
  return backtrace_symbolize(addrs, n_addr, print_symbol);
}

} // end namespace common
} // end namespace oceanbase
#endif // ENABLE_SANITY

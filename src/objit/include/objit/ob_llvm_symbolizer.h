/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_COMMON_LLVM_SYMBOLIZE_H_
#define OCEANBASE_COMMON_LLVM_SYMBOLIZE_H_
#ifndef ENABLE_SANITY
#else
#include <stdint.h>
#include <stddef.h>
#include <functional>
namespace oceanbase
{
namespace common
{
extern void print_symbol(void *addr, const char *func_name, const char *file_name, uint32_t line);
using SymbolizeCb = std::function<decltype(print_symbol)>;
extern int backtrace_symbolize(void **addrs, int32_t n_addr, SymbolizeCb cb);
extern int backtrace_symbolize(void **addrs, int32_t n_addr);
} // end namespace common
} // end namespace oceanbase

#endif //ENABLE_SANITY
#endif //OCEANBASE_COMMON_LLVM_SYMBOLIZE_H_

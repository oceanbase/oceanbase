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

#if __ARM_PERF__
#pragma GCC system_header
extern "C" {
inline int __cxa_guard_acquire(char* guard)
{
  if (__atomic_load_n(guard, __ATOMIC_ACQUIRE))
    return 0;
  if (!__atomic_test_and_set(guard + 1, __ATOMIC_SEQ_CST))
    return 1;
  while (!__atomic_load_n(guard, __ATOMIC_ACQUIRE))
    ;
  return 0;
}
};
#endif

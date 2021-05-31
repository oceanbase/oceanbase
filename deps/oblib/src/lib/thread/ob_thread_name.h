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

#ifndef OCEANBASE_THREAD_OB_THREAD_NAME_H_
#define OCEANBASE_THREAD_OB_THREAD_NAME_H_
#include <stdint.h>
#include <stdio.h>
#include <sys/prctl.h>

namespace oceanbase {
namespace lib {
inline void set_thread_name(const char* name)
{
  prctl(PR_SET_NAME, name);
}

inline void set_thread_name(const char* type, uint64_t idx)
{
  char name[16];
  snprintf(name, sizeof(name), "%s%ld", type, idx);
  set_thread_name(name);
}
};  // end namespace lib
};  // end namespace oceanbase

#endif /* OCEANBASE_THREAD_OB_THREAD_NAME_H_ */

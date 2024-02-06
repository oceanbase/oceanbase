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

#include "lib/ob_define.h"

namespace oceanbase
{
namespace lib
{

inline void set_thread_name_inner(const char* name)
{
  prctl(PR_SET_NAME, name);
}

inline void set_thread_name(const char* type, uint64_t idx)
{
  char *name = ob_get_tname();
  uint64_t tenant_id = ob_get_tenant_id();
  char *ori_tname = ob_get_origin_thread_name();
  STRNCPY(ori_tname, type, oceanbase::OB_THREAD_NAME_BUF_LEN);
  if (tenant_id == 0) {
    snprintf(name, OB_THREAD_NAME_BUF_LEN, "%s%ld", type, idx);
  } else {
    snprintf(name, OB_THREAD_NAME_BUF_LEN, "T%ld_%s%ld", tenant_id, type, idx);
  }
  set_thread_name_inner(name);
}

inline void set_thread_name(const char* type)
{
  char *name = ob_get_tname();
  uint64_t tenant_id = ob_get_tenant_id();
  char *ori_tname = ob_get_origin_thread_name();
  STRNCPY(ori_tname, type, oceanbase::OB_THREAD_NAME_BUF_LEN);
  if (tenant_id == 0) {
    snprintf(name, OB_THREAD_NAME_BUF_LEN, "%s", type);
  } else {
    snprintf(name, OB_THREAD_NAME_BUF_LEN, "T%ld_%s", tenant_id, type);
  }
  set_thread_name_inner(name);
}

}; // end namespace lib
}; // end namespace oceanbase

#endif /* OCEANBASE_THREAD_OB_THREAD_NAME_H_ */


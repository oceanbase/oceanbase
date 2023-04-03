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

#ifndef OB_VERSION_H
#define OB_VERSION_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif
  const char *build_version();
  const char *build_date();
  const char *build_time();
  const char *build_flags();
  const char *build_branch();
  const char *build_info();
#ifdef __cplusplus
}
#endif

namespace oceanbase
{
namespace share
{
void get_package_and_svn(char *server_version, int64_t buf_len);
}
}

#endif /* OB_VERSION_H */

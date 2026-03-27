/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
int get_package_and_svn(char *server_version, int64_t buf_len);
}
}

#endif /* OB_VERSION_H */

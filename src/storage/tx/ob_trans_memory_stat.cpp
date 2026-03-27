/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "ob_trans_memory_stat.h"

namespace oceanbase
{
using namespace common;

namespace transaction
{
void ObTransMemoryStat::reset()
{
  addr_.reset();
  type_[0] = '\0';
  alloc_count_ = 0;
  release_count_ = 0;
}

int ObTransMemoryStat::init(const common::ObAddr &addr, const char *mod_type,
    const int64_t alloc_count, const int64_t release_count)
{
  int ret = OB_SUCCESS;

  if (!addr.is_valid() || OB_ISNULL(mod_type) || alloc_count < 0 || release_count < 0) {
    TRANS_LOG(WARN, "invalid argument", K(addr), KP(mod_type), K(alloc_count),
      K(release_count));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t len = strlen(mod_type);
    len = (OB_TRANS_MEMORY_MOD_TYPE_SIZE -1) > len ? len : OB_TRANS_MEMORY_MOD_TYPE_SIZE -1;
    strncpy(type_, mod_type, len);
    type_[len] = '\0';
    addr_ = addr;
    alloc_count_ = alloc_count;
    release_count_ = release_count;
  }

  return ret;
}

} // transaction
} // oceanbase

// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#ifndef OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
#define OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_
#include "stdint.h"

namespace oceanbase
{
namespace compaction
{
static const int64_t MACRO_STEP_SIZE = 0x1 << 25;
static const int64_t INVALID_TASK_IDX = -1;

} // namespace compaction
} // namespace oceanbase

#endif // OB_SHARE_STORAGE_SHARE_COMPACTION_SHARED_COMPACTION_UTIL_H_

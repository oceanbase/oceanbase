//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
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

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

#ifndef OCEANBASE_STORAGE_LS_OB_FREEZER_DEFINE
#define OCEANBASE_STORAGE_LS_OB_FREEZER_DEFINE

namespace oceanbase
{
namespace storage
{

// The ObFreezeSourceFlag is used to facilitate effective management of interfaces,
// enhance code reusability and modularity, and reduce potential conflicts and
// dependency issues(from chatgpt).
// Therefore, we will use it for each freeze interface in order to better
// identify and manage them. When you add a new freeze call, be sure to add the
// flag in the ObFreezeSourceFlag here.
// Additionally, please remember to maintain this information in the document at
//
// future learners and maintainers.
enum class ObFreezeSourceFlag : int64_t
{
  INVALID_SOURCE      = 0,
  CLOG_CHECKPOINT     = 1,
  USER_MINOR_FREEZE   = 2,
  FREEZE_TRIGGER      = 3,
  BACKUP              = 4,
  TRANSFER_NO_KILL_TX = 5,
  TRANSFER_BACKFILL   = 6,
  MAJOR_FREEZE        = 7,
  FAST_FREEZE         = 8,
  DIRECT_INC_START    = 9,
  DIRECT_INC_END      = 10,
  DIRECT_INC_FREEZE   = 11,
  GC_RETAIN_CTX       = 12, // deprecated
  TEST_MODE           = 13, // used for test only
  TABLET_SPLIT        = 14,
  MAX_SOURCE          = 15,
};

inline bool is_valid_freeze_source(const ObFreezeSourceFlag source)
{
  return source > ObFreezeSourceFlag::INVALID_SOURCE
    && source < ObFreezeSourceFlag::MAX_SOURCE;
}

} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_LS_OB_FREEZER_DEFINE

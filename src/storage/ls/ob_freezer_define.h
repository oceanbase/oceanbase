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

static const int64_t MAX_FREEZE_SOURCE_TYPE_COUNT = static_cast<int64_t>(ObFreezeSourceFlag::MAX_SOURCE);

inline bool is_valid_freeze_source(const ObFreezeSourceFlag source)
{
  return source > ObFreezeSourceFlag::INVALID_SOURCE
    && source < ObFreezeSourceFlag::MAX_SOURCE;
}

inline const char *obj_to_cstring(const ObFreezeSourceFlag type)
{
  const char *ret = "INVALID";
  switch (type) {
  case ObFreezeSourceFlag::INVALID_SOURCE:
    ret = "INVALID_SOURCE";
    break;
  case ObFreezeSourceFlag::CLOG_CHECKPOINT:
    ret = "CLOG_CHECKPOINT";
    break;
  case ObFreezeSourceFlag::USER_MINOR_FREEZE:
    ret = "USER_MINOR_FREEZE";
    break;
  case ObFreezeSourceFlag::FREEZE_TRIGGER:
    ret = "FREEZE_TRIGGER";
    break;
  case ObFreezeSourceFlag::BACKUP:
    ret = "BACKUP";
    break;
  case ObFreezeSourceFlag::TRANSFER_NO_KILL_TX:
    ret = "TRANSFER_NO_KILL_TX";
    break;
  case ObFreezeSourceFlag::TRANSFER_BACKFILL:
    ret = "TRANSFER_BACKFILL";
    break;
  case ObFreezeSourceFlag::MAJOR_FREEZE:
    ret = "MAJOR_FREEZE";
    break;
  case ObFreezeSourceFlag::FAST_FREEZE:
    ret = "FAST_FREEZE";
    break;
  case ObFreezeSourceFlag::DIRECT_INC_START:
    ret = "DIRECT_INC_START";
    break;
  case ObFreezeSourceFlag::DIRECT_INC_END:
    ret = "DIRECT_INC_END";
    break;
  case ObFreezeSourceFlag::DIRECT_INC_FREEZE:
    ret = "DIRECT_INC_FREEZE";
    break;
  case ObFreezeSourceFlag::GC_RETAIN_CTX:
    ret = "GC_RETAIN_CTX";
    break;
  case ObFreezeSourceFlag::TEST_MODE:
    ret = "TEST_MODE";
    break;
  case ObFreezeSourceFlag::TABLET_SPLIT:
    ret = "TABLET_SPLIT";
    break;
  case ObFreezeSourceFlag::MAX_SOURCE:
    ret = "MAX_SOURCE";
    break;
  default:
    break;
  }
  return ret;
}


} // storage
} // oceanbase

#endif // OCEANBASE_STORAGE_LS_OB_FREEZER_DEFINE

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

#include "storage/compaction/ob_compaction_util.h"
#include "share/ob_define.h"
namespace oceanbase
{
namespace storage
{

const char * ObMergeTypeStr[] = {
    "MINOR_MERGE",
    "HISTORY_MINOR_MERGE",
    "META_MAJOR_MERGE",
    "MINI_MERGE",
    "MAJOR_MERGE",
    "MEDIUM_MERGE",
    "DDL_KV_MERGE",
    "BACKFILL_TX_MERGE",
    "MDS_TABLE_MERGE",
    "EMPTY_MERGE_TYPE"
};

const char *merge_type_to_str(const ObMergeType &merge_type)
{
  STATIC_ASSERT(static_cast<int64_t>(MERGE_TYPE_MAX + 1) == ARRAYSIZEOF(ObMergeTypeStr), "merge type str len is mismatch");
  const char *str = "";
  if (merge_type > MERGE_TYPE_MAX || merge_type <= INVALID_MERGE_TYPE) {
    str = "invalid_merge_type";
  } else {
    str = ObMergeTypeStr[merge_type];
  }
  return str;
}

} // namespace storage
} // namespace oceanbase

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

#ifndef OB_STORAGE_COMPACTION_UTIL_H_
#define OB_STORAGE_COMPACTION_UTIL_H_

namespace oceanbase
{
namespace storage // temp solution
{
enum ObMergeType
{
  INVALID_MERGE_TYPE = -1,
  MINOR_MERGE = 0,  // minor merge, compaction several mini sstable into one larger mini sstable
  HISTORY_MINOR_MERGE = 1,
  META_MAJOR_MERGE = 2,
  MINI_MERGE = 3,  // mini merge, only flush memtable
  MAJOR_MERGE = 4,
  MEDIUM_MERGE = 5,
  DDL_KV_MERGE = 6,
  BACKFILL_TX_MERGE = 7,
  MDS_TABLE_MERGE = 8,
  MERGE_TYPE_MAX
};

const char *merge_type_to_str(const ObMergeType &merge_type);

inline bool is_major_merge(const ObMergeType &merge_type)
{
  return MAJOR_MERGE == merge_type;
}
inline bool is_medium_merge(const ObMergeType &merge_type)
{
  return MEDIUM_MERGE == merge_type;
}
inline bool is_major_merge_type(const ObMergeType &merge_type)
{
  return is_medium_merge(merge_type) || is_major_merge(merge_type);
}
inline bool is_mini_merge(const ObMergeType &merge_type)
{
  return MINI_MERGE == merge_type;
}
inline bool is_minor_merge(const ObMergeType &merge_type)
{
  return MINOR_MERGE == merge_type;
}
inline bool is_multi_version_merge(const ObMergeType &merge_type)
{
  return MINOR_MERGE == merge_type
      || MINI_MERGE == merge_type
      || HISTORY_MINOR_MERGE == merge_type
      || BACKFILL_TX_MERGE == merge_type;
}
inline bool is_history_minor_merge(const ObMergeType &merge_type)
{
  return HISTORY_MINOR_MERGE == merge_type;
}
inline bool is_minor_merge_type(const ObMergeType &merge_type)
{
  return is_minor_merge(merge_type) || is_history_minor_merge(merge_type);
}
inline bool is_meta_major_merge(const ObMergeType &merge_type)
{
  return META_MAJOR_MERGE == merge_type;
}

inline bool is_backfill_tx_merge(const ObMergeType &merge_type)
{
  return BACKFILL_TX_MERGE == merge_type;
}

inline bool is_mds_table_merge(const ObMergeType &merge_type)
{
  return MDS_TABLE_MERGE == merge_type;
}

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_UTIL_H_

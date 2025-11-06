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
#include "stdint.h"
namespace oceanbase
{
namespace compaction
{
enum ObMergeType : uint8_t
{
  INVALID_MERGE_TYPE = 0,
  MINOR_MERGE,  // minor merge, compaction several mini sstable into one larger mini sstable
  HISTORY_MINOR_MERGE,
  META_MAJOR_MERGE,
  MINI_MERGE,  // mini merge, only flush memtable
  MAJOR_MERGE,
  MEDIUM_MERGE,
  DDL_KV_MERGE, // only use for ddl dag
  BACKFILL_TX_MERGE,
  MDS_MINI_MERGE,
  MDS_MINOR_MERGE,
  BATCH_EXEC, // for ObBatchExecDag
  CONVERT_CO_MAJOR_MERGE, // convert row store major into columnar store cg sstables
  INC_MAJOR_MERGE,
  // add new merge type here
  // fix merge_type_to_str & ObPartitionMergePolicy::get_merge_tables
  MERGE_TYPE_MAX
};

const char *merge_type_to_str(const ObMergeType &merge_type);
inline bool is_valid_merge_type(const ObMergeType &merge_type)
{
  return merge_type > INVALID_MERGE_TYPE && merge_type < MERGE_TYPE_MAX;
}
inline bool is_major_merge(const ObMergeType &merge_type)
{
  return MAJOR_MERGE == merge_type;
}
inline bool is_medium_merge(const ObMergeType &merge_type)
{
  return MEDIUM_MERGE == merge_type;
}
inline bool is_inc_major_merge(const ObMergeType &merge_type)
{
  return INC_MAJOR_MERGE == merge_type;
}
inline bool is_convert_co_major_merge(const ObMergeType &merge_type)
{
  return CONVERT_CO_MAJOR_MERGE == merge_type;
}
inline bool is_major_merge_type(const ObMergeType &merge_type)
{
  return is_convert_co_major_merge(merge_type) || is_medium_merge(merge_type) || is_major_merge(merge_type);
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
      || BACKFILL_TX_MERGE == merge_type
      || MDS_MINOR_MERGE == merge_type;
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
inline bool is_major_or_meta_merge_type(const ObMergeType &merge_type)
{
  return is_major_merge_type(merge_type) || is_meta_major_merge(merge_type);
}
inline bool is_backfill_tx_merge(const ObMergeType &merge_type)
{
  return BACKFILL_TX_MERGE == merge_type;
}
inline bool need_collect_uncommit_tx_info(const ObMergeType &merge_type)
{
  return is_mini_merge(merge_type) || is_minor_merge_type(merge_type);
}
inline bool is_mds_mini_merge(const ObMergeType &merge_type)
{
  return MDS_MINI_MERGE == merge_type;
}
inline bool is_mds_minor_merge(const ObMergeType &merge_type)
{
  return MDS_MINOR_MERGE == merge_type;
}
inline bool is_mds_merge(const ObMergeType &merge_type)
{
  return is_mds_mini_merge(merge_type) || is_mds_minor_merge(merge_type);
}

enum ObMergeLevel : uint8_t
{
  MACRO_BLOCK_MERGE_LEVEL = 0,
  MICRO_BLOCK_MERGE_LEVEL = 1,
  MERGE_LEVEL_MAX
};

inline bool is_valid_merge_level(const ObMergeLevel &merge_level)
{
  return merge_level >= MACRO_BLOCK_MERGE_LEVEL && merge_level < MERGE_LEVEL_MAX;
}
const char *merge_level_to_str(const ObMergeLevel &merge_level);

enum ObExecMode : uint8_t {
  EXEC_MODE_LOCAL = 0,
  EXEC_MODE_CALC_CKM, // calc checksum, not output macro
  EXEC_MODE_OUTPUT,   // normal compaction, output macro to share_storage
  EXEC_MODE_VALIDATE,   // verify checksum and dump macro block
  EXEC_MODE_MAX
};

inline bool is_valid_exec_mode(const ObExecMode &exec_mode)
{
  return exec_mode >= EXEC_MODE_LOCAL && exec_mode < EXEC_MODE_MAX;
}
inline bool is_local_exec_mode(const ObExecMode &exec_mode)
{
  return EXEC_MODE_LOCAL == exec_mode;
}
inline bool is_output_exec_mode(const ObExecMode &exec_mode)
{
  return EXEC_MODE_OUTPUT == exec_mode;
}
inline bool is_calc_ckm_exec_mode(const ObExecMode &exec_mode)
{
  return EXEC_MODE_CALC_CKM == exec_mode;
}
inline bool is_validate_exec_mode(const ObExecMode &exec_mode)
{
  return EXEC_MODE_VALIDATE == exec_mode;
}
inline bool is_flush_macro_exec_mode(const ObExecMode &exec_mode)
{
  return is_local_exec_mode(exec_mode) || is_output_exec_mode(exec_mode);
}

const char *exec_mode_to_str(const ObExecMode &exec_mode);

enum ObGetMacroSeqStage : uint8_t
{
  BUILD_INDEX_TREE = 0,
  GET_NEW_ROOT_MACRO_SEQ = 1, // for next major
  MACRO_SEQ_TYPE_MAX
};
bool is_valid_get_macro_seq_stage(const ObGetMacroSeqStage stage);

const int64_t MAX_MERGE_THREAD = 64;
const int64_t DEFAULT_CG_MERGE_BATCH_SIZE = 10;
const int64_t ALL_CG_IN_ONE_BATCH_CNT = DEFAULT_CG_MERGE_BATCH_SIZE * 2;

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_UTIL_H_

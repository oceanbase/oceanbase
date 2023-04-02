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

#ifndef OB_SSTABLE_STRUCT_H_
#define OB_SSTABLE_STRUCT_H_

#include "blocksstable/ob_block_sstable_struct.h"
#include "ob_i_table.h"
#include "compaction/ob_i_compaction_filter.h"
#include "compaction/ob_compaction_util.h"

namespace oceanbase
{
namespace storage
{

const int64_t MAX_MERGE_THREAD = 64;
const int64_t MACRO_BLOCK_CNT_PER_THREAD = 128;

enum ObSSTableMergeInfoStatus
{
  MERGE_START = 0,
  MERGE_RUNNING = 1,
  MERGE_FINISH = 2,
  MERGE_STATUS_MAX
};

struct ObMultiVersionSSTableMergeInfo final
{
public:
  ObMultiVersionSSTableMergeInfo();
  ~ObMultiVersionSSTableMergeInfo() = default;
  int add(const ObMultiVersionSSTableMergeInfo &info);
  void reset();
  TO_STRING_KV(K_(delete_logic_row_count), K_(update_logic_row_count), K_(insert_logic_row_count),
      K_(empty_delete_logic_row_count));
public:
  uint64_t delete_logic_row_count_;
  uint64_t update_logic_row_count_;
  uint64_t insert_logic_row_count_;
  uint64_t empty_delete_logic_row_count_;
};


struct ObParalleMergeInfo
{
  ObParalleMergeInfo()
  : info_()
  {}
  ~ObParalleMergeInfo() {}
  int64_t to_paral_info_string(char *buf, const int64_t buf_len) const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  const char *get_para_info_str(const int64_t idx) const;
  void reset();

  enum PARA_INFO_TYPE
  {
    SCAN_UNITS = 0,
    MERGE_COST_TIME = 1,
    USE_OLD_BLK_CNT = 2,
    INC_ROW_CNT = 3,
    ARRAY_IDX_MAX = 4
  };

  static const char *para_info_type_str[];

  struct BasicInfo
  {
    BasicInfo()
     : min_value_(INT64_MAX),
       max_value_(INT64_MIN),
       sum_value_(0),
       count_(0)
    {}
    ~BasicInfo() {}
    void add(int64_t value)
    {
      min_value_ = MIN(value, min_value_);
      max_value_ = MAX(value, max_value_);
      sum_value_ += value;
      ++count_;
    }
    void reset()
    {
      min_value_ = 0;
      max_value_ = 0;
      sum_value_ = 0;
      count_ = 0;
    }
    TO_STRING_KV("min", min_value_, "max", max_value_, "sum", sum_value_, "count", count_);
    int64_t min_value_;
    int64_t max_value_;
    int64_t sum_value_;
    int64_t count_;
  };

public:
  BasicInfo info_[ARRAY_IDX_MAX];
};

struct ObSSTableMergeInfo final
{
public:
  ObSSTableMergeInfo();
  ~ObSSTableMergeInfo() = default;
  bool is_valid() const;
  int add(const ObSSTableMergeInfo &other);
  OB_INLINE bool is_major_merge_type() const { return storage::is_major_merge_type(merge_type_); }
  void dump_info(const char *msg);
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(compaction_scn),
              "merge_type", merge_type_to_str(merge_type_), "merge_cost_time", merge_finish_time_ - merge_start_time_,
               K_(merge_start_time), K_(merge_finish_time), K_(dag_id), K_(occupy_size), K_(new_flush_occupy_size), K_(original_size),
               K_(compressed_size), K_(macro_block_count), K_(multiplexed_macro_block_count),
               K_(new_micro_count_in_new_macro), K_(multiplexed_micro_count_in_new_macro),
               K_(total_row_count), K_(incremental_row_count), K_(new_flush_data_rate),
               K_(is_full_merge), K_(progressive_merge_round), K_(progressive_merge_num),
               K_(concurrent_cnt), K_(parallel_merge_info), K_(filter_statistics), K_(participant_table_str),
               K_(macro_id_list), K_(comment));
public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  int64_t compaction_scn_; // major_scn OR minor end_log_ts
  ObMergeType merge_type_;
  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  common::ObCurTraceId::TraceId dag_id_;
  int64_t occupy_size_; // including lob_macro
  int64_t new_flush_occupy_size_; 
  int64_t original_size_;
  int64_t compressed_size_;
  int64_t macro_block_count_;
  int64_t multiplexed_macro_block_count_;
  int64_t new_micro_count_in_new_macro_;
  int64_t multiplexed_micro_count_in_new_macro_;
  int64_t total_row_count_;
  int64_t incremental_row_count_;
  int64_t new_flush_data_rate_;
  bool is_full_merge_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_num_;
  int64_t concurrent_cnt_;
  int64_t macro_bloomfilter_count_;
  ObParalleMergeInfo parallel_merge_info_;
  compaction::ObICompactionFilter::ObFilterStatistics filter_statistics_;
  char participant_table_str_[common::OB_PART_TABLE_INFO_LENGTH];
  char macro_id_list_[common::OB_MACRO_ID_INFO_LENGTH];
  char comment_[common::OB_COMPACTION_EVENT_STR_LENGTH];
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_SSTABLE_STRUCT_H_

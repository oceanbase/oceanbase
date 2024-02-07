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
#include "share/scheduler/ob_dag_scheduler_config.h"
#include "storage/compaction/ob_compaction_diagnose.h"

namespace oceanbase
{
namespace storage
{

const int64_t MAX_MERGE_THREAD = 64;

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

struct PartTableInfo {
  PartTableInfo()
  : is_major_merge_(false),
    table_cnt_(0),
    snapshot_version_(0),
    start_scn_(0),
    end_scn_(0)
  {}
  void reset()
  {
    is_major_merge_ = false;
    table_cnt_ = 0;
    snapshot_version_ = 0;
    start_scn_ = 0;
    end_scn_ = 0;
  }
  void fill_info(char *buf, const int64_t buf_len) const;
  TO_STRING_KV(K_(is_major_merge), K_(table_cnt), K_(snapshot_version), K_(start_scn), K_(end_scn));
  bool is_major_merge_;
  int32_t table_cnt_;
  int64_t snapshot_version_;
  int64_t start_scn_;
  int64_t end_scn_;
};

struct ObSSTableMergeInfo final : public compaction::ObIDiagnoseInfo
{
public:
  ObSSTableMergeInfo();
  ~ObSSTableMergeInfo() = default;
  bool is_valid() const;
  int add(const ObSSTableMergeInfo &other);
  OB_INLINE bool is_major_merge_type() const { return compaction::is_major_merge_type(merge_type_); }
  void dump_info(const char *msg);
  void reset();
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(compaction_scn),
              "merge_type", merge_type_to_str(merge_type_), "merge_cost_time", merge_finish_time_ - merge_start_time_,
               K_(merge_start_time), K_(merge_finish_time), K_(dag_id), K_(occupy_size), K_(new_flush_occupy_size), K_(original_size),
               K_(compressed_size), K_(macro_block_count), K_(multiplexed_macro_block_count),
               K_(new_micro_count_in_new_macro), K_(multiplexed_micro_count_in_new_macro),
               K_(total_row_count), K_(incremental_row_count), K_(new_flush_data_rate),
               K_(is_full_merge), K_(progressive_merge_round), K_(progressive_merge_num),
               K_(concurrent_cnt), K_(start_cg_idx), K_(end_cg_idx), K_(suspect_add_time),
               K_(early_create_time), K_(dag_ret), K_(retry_cnt), K_(task_id), K_(error_location),
               K_(kept_snapshot_info), K_(merge_level), K_(parallel_merge_info), K_(filter_statistics), K_(participant_table_info),
               K_(macro_id_list), K_(comment));

  int fill_comment(char *buf, const int64_t buf_len, const char* other_info) const;
  void update_start_time();
  virtual void shallow_copy(ObIDiagnoseInfo *other) override;
  static const int64_t MERGE_INFO_COMMENT_LENGTH = 96;

public:
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  bool is_fake_;
  int64_t compaction_scn_; // major_scn OR minor end_log_ts
  compaction::ObMergeType merge_type_;
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
  int64_t start_cg_idx_;
  int64_t end_cg_idx_;
  // from suspect info
  int64_t suspect_add_time_;
  // from dag warn info
  int64_t early_create_time_;
  int64_t dag_ret_;
  int64_t retry_cnt_;
  common::ObCurTraceId::TraceId task_id_;
  share::ObDiagnoseLocation error_location_;
  ObParalleMergeInfo parallel_merge_info_;
  compaction::ObICompactionFilter::ObFilterStatistics filter_statistics_;
  PartTableInfo participant_table_info_;
  char macro_id_list_[common::OB_MACRO_ID_INFO_LENGTH];
  char comment_[MERGE_INFO_COMMENT_LENGTH];
  ObStorageSnapshotInfo kept_snapshot_info_;
  compaction::ObMergeLevel merge_level_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_SSTABLE_STRUCT_H_

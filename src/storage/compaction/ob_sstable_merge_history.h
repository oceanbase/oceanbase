//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_SSTABLE_MERGE_INFO_H_
#define OB_STORAGE_COMPACTION_SSTABLE_MERGE_INFO_H_
#include "common/ob_tablet_id.h"
#include "share/ob_ls_id.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_tenant_freeze_info_mgr.h"
#include "storage/compaction/ob_compaction_diagnose.h"
#include "share/compaction/ob_new_micro_info.h"
namespace oceanbase
{
namespace compaction
{

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
    bool is_empty() const { return 0 == count_; }
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

struct ObMergeStaticInfo
{
  ObMergeStaticInfo();
  ~ObMergeStaticInfo() {}
  void reset();
  bool is_valid() const;
  void shallow_copy(const ObMergeStaticInfo &other);
  int64_t to_string(char* buf, const int64_t buf_len) const;
  static const int64_t MDS_FILTER_INFO_LENGTH = 256;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObMergeType merge_type_;
  int64_t compaction_scn_; // major_scn OR minor end_log_ts
  int64_t concurrent_cnt_;
  int64_t progressive_merge_round_;
  int64_t progressive_merge_num_;
  storage::ObStorageSnapshotInfo kept_snapshot_info_;
  PartTableInfo participant_table_info_;
  char mds_filter_info_str_[MDS_FILTER_INFO_LENGTH];
  ObMergeLevel merge_level_;
  ObExecMode exec_mode_;
  ObAdaptiveMergePolicy::AdaptiveMergeReason merge_reason_;
  ObCOMajorSSTableStatus base_major_status_;
  ObCOMajorMergePolicy::ObCOMajorMergeType co_major_merge_type_;
  bool is_full_merge_;
  bool is_fake_;
};

struct ObMergeRunningInfo
{
  ObMergeRunningInfo();
  ~ObMergeRunningInfo() {}
  void reset();
  bool is_valid() const;
  void shallow_copy(const ObMergeRunningInfo &other);

  static const int64_t MERGE_INFO_COMMENT_LENGTH = 256;
  TO_STRING_KV(K_(merge_start_time), K_(merge_finish_time), K_(execute_time), K_(dag_id),
               K_(start_cg_idx), K_(end_cg_idx), K_(io_percentage), K_(parallel_merge_info));

  int64_t merge_start_time_;
  int64_t merge_finish_time_;
  // for parallel merge & column store, finish_time-start_time can't show the real execute time
  int64_t execute_time_;
  int64_t start_cg_idx_;
  int64_t end_cg_idx_;
  int64_t io_percentage_;
  common::ObCurTraceId::TraceId dag_id_;
  ObParalleMergeInfo parallel_merge_info_;
  char comment_[MERGE_INFO_COMMENT_LENGTH];
};

struct ObMergeBlockInfo
{
public:
  ObMergeBlockInfo();
  ~ObMergeBlockInfo() {}
  void reset();
  bool is_valid() const;
  bool is_empty() const { return 0 == macro_block_count_ && 0 == total_row_count_; }
  void shallow_copy(const ObMergeBlockInfo &other);
  void add(const ObMergeBlockInfo &block_info);
  void add_without_row_cnt(const ObMergeBlockInfo &block_info);
  void add_index_block_info(const ObMergeBlockInfo &block_info);
  TO_STRING_KV(K_(occupy_size), K_(original_size), K_(compressed_size), K_(macro_block_count), K_(multiplexed_macro_block_count),
    K_(new_micro_count_in_new_macro), K_(multiplexed_micro_count_in_new_macro),
    K_(total_row_count), K_(incremental_row_count), K_(new_micro_info), K_(block_io_us));

  int64_t occupy_size_; // including lob_macro
  int64_t original_size_;
  int64_t compressed_size_;
  int64_t macro_block_count_;
  int64_t multiplexed_macro_block_count_;
  int64_t new_micro_count_in_new_macro_;
  int64_t multiplexed_micro_count_in_new_macro_;
  int64_t total_row_count_;
  int64_t incremental_row_count_;
  int64_t new_flush_data_rate_; // KB per second
  ObNewMicroInfo new_micro_info_;
  int64_t block_io_us_;
  char macro_id_list_[common::OB_MACRO_ID_INFO_LENGTH];
};

struct ObMergeDiagnoseInfo
{
  ObMergeDiagnoseInfo();
  ~ObMergeDiagnoseInfo() {}
  void reset();
  void shallow_copy(const ObMergeDiagnoseInfo &other);
  bool is_empty() const { return 0 == dag_ret_ && 0 == retry_cnt_ && 0 == suspect_add_time_ && 0 == early_create_time_; }
  TO_STRING_KV(K_(dag_ret), K_(retry_cnt), K_(suspect_add_time), K_(early_create_time), K_(error_location));
  int64_t dag_ret_;
  int64_t retry_cnt_;
  // from suspect info
  int64_t suspect_add_time_;
  // from dag warn info
  int64_t early_create_time_;
  common::ObCurTraceId::TraceId error_trace_;
  share::ObDiagnoseLocation error_location_;
};

struct ObSSTableMergeHistory : public ObIDiagnoseInfo
{
  ObSSTableMergeHistory(const bool need_free_param = true);
  virtual ~ObSSTableMergeHistory() {}
  bool is_valid() const;
  void reset();
  virtual void shallow_copy(ObIDiagnoseInfo *other) override;
  int update_block_info(const ObMergeBlockInfo &block_info, const bool without_row_cnt);
  void update_execute_time(const int64_t cost_time) { running_info_.execute_time_ += cost_time; }
  int64_t get_macro_block_count() const { return block_info_.macro_block_count_; }
  int64_t get_multiplexed_macro_block_count() const { return block_info_.multiplexed_macro_block_count_; }
  bool is_major_merge_type() const { return compaction::is_major_merge_type(static_info_.merge_type_); }
  bool is_minor_merge_type() const { return compaction::is_minor_merge_type(static_info_.merge_type_); }
  bool is_mds_merge_type() const { return compaction::is_mds_merge(static_info_.merge_type_); }
  const ObNewMicroInfo &get_new_micro_info() const { return block_info_.new_micro_info_; }
  int fill_comment(char *buf, const int64_t buf_len, const char* other_info) const;
  void update_start_time();
  int64_t to_string(char* buf, const int64_t buf_len) const;

  ObMergeStaticInfo static_info_;
  ObMergeRunningInfo running_info_;
  ObMergeBlockInfo block_info_;
  ObMergeDiagnoseInfo diagnose_info_;
  lib::ObMutex lock_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SSTABLE_MERGE_INFO_H_

//Copyright (c) 2021 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_ROOTSERVER_FREEZE_MAJOR_MERGE_PROGRESS_UTIL_H_
#define OB_ROOTSERVER_FREEZE_MAJOR_MERGE_PROGRESS_UTIL_H_
#include "share/compaction/ob_compaction_time_guard.h"
#include "share/ob_delegate.h"
#include "share/ob_ls_id.h"
#include "share/ob_balance_define.h"
namespace oceanbase
{
namespace share
{
class ObTabletReplica;
}
namespace compaction
{

enum ObTabletCompactionStatus
{
  INITIAL = 0,
  COMPACTED, // tablet finished compaction
  CAN_SKIP_VERIFYING,  // tablet finished compaction and not need to verify
  STATUS_MAX
};

struct ObTableCompactionInfo {
public:
  enum Status : uint8_t
  {
    INITIAL = 0,
    // already finished compaction and verified tablet checksum
    COMPACTED,
    // already finished compaction and can skip verification due to the following two reasons:
    // 1. this table has no tablet.
    // 2. this table has tablets, but compaction_scn of tablets > frozen_scn of this round major compaction.
    // i.e., already launched another medium compaction for this table.
    CAN_SKIP_VERIFYING,
    // already verified index checksum
    INDEX_CKM_VERIFIED,
    // already verified all kinds of checksum (i.e., tablet checksum, index checksum, cross-cluster checksum)
    VERIFIED,
    TB_STATUS_MAX
  };
  const static char *TableStatusStr[];
  static const char *status_to_str(const Status &status);

  ObTableCompactionInfo();
  ~ObTableCompactionInfo() { reset(); }

  void reset()
  {
    table_id_ = OB_INVALID_ID;
    tablet_cnt_ = 0;
    status_ = Status::INITIAL;
    unfinish_index_cnt_ = INVALID_INDEX_CNT;
    need_check_fts_ = false;
  }

  ObTableCompactionInfo &operator=(const ObTableCompactionInfo &other);
  void set_status(const Status status) { status_ = status;}
  void set_uncompacted() { status_ = Status::INITIAL; }
  void set_compacted() { status_ = Status::COMPACTED; }
  bool is_compacted() const { return Status::COMPACTED == status_; }
  bool is_uncompacted() const { return Status::INITIAL == status_; }
  void set_can_skip_verifying() { status_ = Status::CAN_SKIP_VERIFYING; }
  bool can_skip_verifying() const { return Status::CAN_SKIP_VERIFYING == status_; }
  void set_index_ckm_verified() { status_ = Status::INDEX_CKM_VERIFIED; }
  bool is_index_ckm_verified() const { return Status::INDEX_CKM_VERIFIED == status_; }
  void set_verified() { status_ = Status::VERIFIED; }
  bool is_verified() const { return Status::VERIFIED == status_; }
  bool finish_compaction() const { return (is_compacted() ||  can_skip_verifying()); }
  bool finish_verified() const { return is_verified() || can_skip_verifying(); }
  bool finish_idx_verified() const { return finish_verified() || is_index_ckm_verified(); }
  const int64_t INVALID_INDEX_CNT = -1;
  bool is_index_table() const { return INVALID_INDEX_CNT == unfinish_index_cnt_; }

  TO_STRING_KV(K_(table_id), K_(tablet_cnt), "status", status_to_str(status_), K_(unfinish_index_cnt), K_(need_check_fts));
public:
  uint64_t table_id_;
  int64_t tablet_cnt_;
  int64_t unfinish_index_cnt_; // accurate for main table, record cnt of unfinish index_table
  Status status_;
  bool need_check_fts_;
};

struct ObMergeProgress
{
public:
  ObMergeProgress()
    : unmerged_tablet_cnt_(0),
      merged_tablet_cnt_(0),
      total_table_cnt_(0),
      table_cnt_(),
      merge_finish_(false)
  {
    MEMSET(table_cnt_, 0, sizeof(int64_t) * RECORD_TABLE_TYPE_CNT);
  }
  ~ObMergeProgress() {}
  void reset()
  {
    merge_finish_ = false;
    unmerged_tablet_cnt_ = 0;
    merged_tablet_cnt_ = 0;
    total_table_cnt_ = 0;
    MEMSET(table_cnt_, 0, sizeof(int64_t) * RECORD_TABLE_TYPE_CNT);
  }
  bool is_merge_finished() const
  {
    return total_table_cnt_ > 0 && merge_finish_
    && (total_table_cnt_ == get_finish_verified_table_cnt());
  }
  bool exist_uncompacted_table() const
  {
    return table_cnt_[ObTableCompactionInfo::INITIAL] > 0;
  }
  bool is_merge_abnomal() const
  {
    return total_table_cnt_ > 0 && merge_finish_
    && (total_table_cnt_ != get_finish_verified_table_cnt());
  }
  bool only_remain_special_table_to_verified() const
  {
    return total_table_cnt_ == get_finish_verified_table_cnt() + 1; // rest tables are in finish_verified status
  }
  void update_table_cnt(const ObTableCompactionInfo::Status status)
  {
    if (status >= ObTableCompactionInfo::INITIAL && status < RECORD_TABLE_TYPE_CNT) {
      ++table_cnt_[status];
    }
  }
  int64_t get_wait_index_ckm_table_cnt()
  {
    return table_cnt_[ObTableCompactionInfo::INITIAL] + table_cnt_[ObTableCompactionInfo::COMPACTED];
  }
  void deal_with_special_tablet()
  {
    ++table_cnt_[ObTableCompactionInfo::VERIFIED];
    merge_finish_ = true;
  }
  void clear_before_each_loop()
  {
    // clear info that will change in cur loop
    unmerged_tablet_cnt_ = 0;
    merged_tablet_cnt_ = 0;
    table_cnt_[ObTableCompactionInfo::INITIAL] = 0;
    table_cnt_[ObTableCompactionInfo::COMPACTED] = 0;
    table_cnt_[ObTableCompactionInfo::INDEX_CKM_VERIFIED] = 0;
  }
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  int64_t get_finish_verified_table_cnt() const
  {
    return table_cnt_[ObTableCompactionInfo::VERIFIED] + table_cnt_[ObTableCompactionInfo::CAN_SKIP_VERIFYING];
  }
public:
  static const int64_t RECORD_TABLE_TYPE_CNT = ObTableCompactionInfo::Status::TB_STATUS_MAX;
  int64_t unmerged_tablet_cnt_;
  int64_t merged_tablet_cnt_;
  int64_t total_table_cnt_;
  int64_t table_cnt_[RECORD_TABLE_TYPE_CNT];
  bool merge_finish_;
};

struct ObUnfinishTableIds
{
  ObUnfinishTableIds()
    : batch_start_idx_(0),
      array_()
  {
    array_.set_label("RSCompTableIds");
  }
  ~ObUnfinishTableIds() { reset(); }
  void reset()
  {
    batch_start_idx_ = 0;
    array_.reset();
  }
  CONST_DELEGATE_WITH_RET(array_, empty, bool);
  CONST_DELEGATE_WITH_RET(array_, count, int64_t);
  DELEGATE_WITH_RET(array_, assign, int);
  DELEGATE_WITH_RET(array_, push_back, int);
  uint64_t at(int64_t idx) const
  {
    OB_ASSERT(idx >= 0 && idx < array_.count());
    return array_.at(idx);
  }
  bool loop_finish() const
  {
    return batch_start_idx_ >= array_.count();
  }
  void start_looping()
  {
    batch_start_idx_ = 0;
  }
  TO_STRING_KV(K_(batch_start_idx), "count", array_.count());
  int64_t batch_start_idx_;
  // record the table_ids in the schema_guard obtained in check_merge_progress
  common::ObArray<uint64_t> array_;
};

typedef hash::ObHashMap<ObTabletID, ObTabletCompactionStatus> ObTabletStatusMap;
typedef common::ObArray<share::ObTabletLSPair> ObTabletLSPairArray;
typedef hash::ObHashMap<uint64_t, ObTableCompactionInfo> ObTableCompactionInfoMap;

struct ObCkmValidatorStatistics
{
  ObCkmValidatorStatistics() { reset(); }
  ~ObCkmValidatorStatistics() {}
  void reset()
  {
    query_ckm_sql_cnt_ = 0;
    use_cached_ckm_cnt_ = 0;
    write_ckm_sql_cnt_ = 0;
    update_report_scn_sql_cnt_ = 0;
    checker_validate_idx_cnt_ = 0;
  }
  TO_STRING_KV(K_(query_ckm_sql_cnt), K_(use_cached_ckm_cnt), K_(write_ckm_sql_cnt), K_(update_report_scn_sql_cnt), K_(checker_validate_idx_cnt));
  int64_t query_ckm_sql_cnt_;
  int64_t use_cached_ckm_cnt_;
  int64_t write_ckm_sql_cnt_;
  int64_t update_report_scn_sql_cnt_;
  int64_t checker_validate_idx_cnt_;
};

// single thread operation
struct ObTabletLSPairCache
{
public:
  ObTabletLSPairCache();
  ~ObTabletLSPairCache();
  void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  void reuse();
  void destroy();
  int try_refresh(const bool force_refresh = false);
  int get_tablet_ls_pairs(
    const uint64_t table_id,
    const ObIArray<ObTabletID> &tablet_ids,
    ObIArray<share::ObTabletLSPair> &pairs) const;
  TO_STRING_KV(K_(tenant_id), K_(last_refresh_ts), K_(max_task_id), "map_cnt", map_.size());
private:
  int refresh();
  int rebuild_map_by_tablet_cnt();
  int check_exist_new_transfer_task(bool &exist, share::ObTransferTaskID &max_task_id);
  const static int64_t RANGE_SIZE = 1000;
  const static int64_t REFRESH_CACHE_TIME_INTERVAL = 60 * 1000 * 1000; // 1m
  const static int64_t TABLET_LS_MAP_BUCKET_CNT = 3000;
  const static int64_t TABLET_LS_MAP_BUCKET_MAX_CNT = 300000;
  uint64_t tenant_id_;
  int64_t last_refresh_ts_;
  share::ObTransferTaskID max_task_id_;
  hash::ObHashMap<common::ObTabletID, share::ObLSID> map_;
};

struct ObUncompactInfo
{
public:
  ObUncompactInfo();
  ~ObUncompactInfo();
  void reset();
  void add_table(const uint64_t table_id);
  void add_tablet(const share::ObTabletReplica &replica);
  void add_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id);
  int get_uncompact_info(
    common::ObIArray<share::ObTabletReplica> &input_tablets,
    common::ObIArray<uint64_t> &input_table_ids) const;
  static const int64_t DEBUG_INFO_CNT = 3;
  common::SpinRWLock diagnose_rw_lock_;
  common::ObSEArray<share::ObTabletReplica, DEBUG_INFO_CNT> tablets_; // record for diagnose
  common::ObSEArray<uint64_t, DEBUG_INFO_CNT> table_ids_; // record for diagnose
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_ROOTSERVER_FREEZE_MAJOR_MERGE_PROGRESS_UTIL_H_

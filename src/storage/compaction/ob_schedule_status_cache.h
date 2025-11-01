//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_SCHEDULE_STATUS_CACHE_H_
#define OB_STORAGE_COMPACTION_SCHEDULE_STATUS_CACHE_H_
#include "share/scn.h"
#include "storage/compaction/ob_compaction_schedule_util.h"
#include "storage/compaction/ob_batch_freeze_tablets_dag.h"
#include "storage/tx_storage/ob_ls_handle.h"
namespace oceanbase
{
namespace storage
{
class ObLS;
class ObTablet;
}
namespace compaction
{
class ObMediumCompactionInfoList;

struct ObLSStatusCache final
{
  enum LSState : uint8_t
  {
    CAN_MERGE = 0,
    WEAK_READ_TS_NOT_READY,
    OFFLINE_OR_DELETED,
    RESTORE_NOT_READY,     //FARM COMPAT WHITELIST
    ALLOW_EMERGENCY_MERGE, //FARM COMPAT WHITELIST
    STATE_MAX,
  };
  static const char *ls_state_to_str(const LSState &state);
  static bool is_valid_ls_state(const LSState state)
  {
    return state >= CAN_MERGE && state < STATE_MAX;
  }
  ObLSStatusCache()
    : ls_handle_(),
      ls_id_(),
      weak_read_ts_(),
      is_leader_(false),
      state_(STATE_MAX)
  {}
  ~ObLSStatusCache() {}
  int init_for_major(const int64_t merge_version, storage::ObLSHandle &ls_handle);
  bool can_merge() const { return CAN_MERGE == state_ || ALLOW_EMERGENCY_MERGE == state_; }
  void reset();
  static void check_ls_state(storage::ObLS &ls, LSState &state);
  static bool is_restore_ready_for_merge(storage::ObLS &ls);
  static bool check_weak_read_ts_ready(
      const int64_t &merge_version,
      storage::ObLS &ls);
  bool is_valid() const { return ls_id_.is_valid() && ls_handle_.is_valid(); }
  storage::ObLS &get_ls()
  {
    OB_ASSERT_MSG(is_valid(), "ObLSStatusCache is not valid");
    return *ls_handle_.get_ls();
  }
  TO_STRING_KV(K_(ls_id), K_(weak_read_ts), K_(is_leader), "state", ls_state_to_str(state_));
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
  storage::ObLSHandle ls_handle_;
  share::ObLSID ls_id_;
  share::SCN weak_read_ts_;
  bool is_leader_;
  LSState state_;
};

struct ObTabletStatusCache
{
public:
  enum TabletExecuteState : uint8_t
  {
    CAN_MERGE = 0,
    DATA_NOT_COMPLETE,
    NO_MAJOR_SSTABLE,
    INVALID_LS_STATE, // for ss
    TENANT_SKIP_MERGE,
    RESTORE_STATUS_NOT_READY, //FARM COMPAT WHITELIST
    EMERGENCY_MERGE_REQUIRED, //FARM COMPAT WHITELIST
    EXECUTE_STATE_MAX,
  };
  static const char *tablet_execute_state_to_str(const TabletExecuteState &state);
  static bool is_valid_tablet_execute_state(const TabletExecuteState state)
  {
    return state >= CAN_MERGE && state < EXECUTE_STATE_MAX;
  }
  enum TabletScheduleNewRoundState : uint8_t
  {
    CAN_SCHEDULE_NEW_ROUND = 0,
    DURING_TRANSFER,
    DURING_SPLIT,
    NEED_CHECK_LAST_MEDIUM_CKM,
    EXIST_UNFINISH_MEDIUM,
    SCHEDULE_CONFLICT,
    DIAGNOSE_NORMAL, // for diagnose
    LOCKED_BY_TRANSFER_OR_SPLIT,
    NEW_ROUND_STATE_MAX,
  };
  static const char *new_round_state_to_str(const TabletScheduleNewRoundState &state);
  static bool is_valid_schedule_new_round_state(const TabletScheduleNewRoundState state)
  {
    return state >= CAN_SCHEDULE_NEW_ROUND && state < NEW_ROUND_STATE_MAX;
  }
  ObTabletStatusCache()
    : tablet_id_(),
      allocator_(ObMemAttr(MTL_ID(), "MediumList")),
      medium_list_(nullptr),
      tablet_merge_finish_(false),
      execute_state_(EXECUTE_STATE_MAX),
      new_round_state_(NEW_ROUND_STATE_MAX),
      is_inited_(false)
  {}
  virtual ~ObTabletStatusCache() { destroy(); }
  void destroy()
  {
    if (is_inited_) {
      inner_destroy();
    }
  }
  int init_for_major(
    storage::ObLS &ls,
    const int64_t merge_version,
    const storage::ObTablet &tablet,
    const bool ls_could_schedule_new_round);
  int init_for_diagnose(
    storage::ObLS &ls,
    const int64_t merge_version,
    const storage::ObTablet &tablet);
  static int check_unfinished_inc_major(
    storage::ObLS &ls,
    const int64_t schedule_scn,
    const storage::ObTablet &tablet,
    bool &exists_unfinished_inc_major);
  bool can_merge() const {
    return CAN_MERGE == execute_state_ || EMERGENCY_MERGE_REQUIRED == execute_state_;
  }
  bool need_diagnose() const;
  bool could_schedule_new_round() const { return can_merge() && inner_check_new_round_state(); }
  bool tablet_merge_finish() const { return tablet_merge_finish_; }

  bool is_emergency_merge_required() const { return EMERGENCY_MERGE_REQUIRED == execute_state_; }
  bool is_restore_status_blocking() const { return RESTORE_STATUS_NOT_READY == execute_state_; }
  static int get_tablet_inc_major_sstable_count(const storage::ObTablet &tablet, int64_t &sstable_count);

  // CAREFUL! medium list may be NULL for some situation
  const compaction::ObMediumCompactionInfoList *medium_list() const { return medium_list_; }
  TabletExecuteState get_execute_state() const { return execute_state_; }
  TabletScheduleNewRoundState get_new_round_state() const { return new_round_state_; }
  // when execute, check cound execute for different merge_type
  static int check_could_execute(const ObMergeType merge_type, const storage::ObTablet &tablet);
  TO_STRING_KV(K_(tablet_id), K_(tablet_merge_finish),
    "execute_state", tablet_execute_state_to_str(execute_state_),
    "new_round_state", new_round_state_to_str(new_round_state_), KPC_(medium_list));
protected:
  void inner_destroy();
  bool inner_check_new_round_state() const { return CAN_SCHEDULE_NEW_ROUND == new_round_state_; }
  int inner_init_state(
    const int64_t merge_version,
    ObLS &ls,
    const storage::ObTablet &tablet);
  int check_medium_list(
    const share::ObLSID &ls_id,
    const storage::ObTablet &tablet,
    const bool normal_schedule);
  int register_map(const ObTablet &tablet);
  void inner_init_could_schedule_new_round(
    const ObLSID &ls_id,
    const ObTablet &tablet,
    const bool ls_could_schedule_new_round,
    const bool normal_schedule);
  int update_tablet_report_status(
    storage::ObLS &ls,
    const storage::ObTablet &tablet);
  int check_execute_state_for_remote_tablet_(
    storage::ObLS &ls,
    const ObTablet &tablet);
  static const int64_t PRINT_LOG_INVERVAL = 2 * 60 * 1000 * 1000L; // 2m
protected:
  common::ObTabletID tablet_id_;
  ObArenaAllocator allocator_;
  const compaction::ObMediumCompactionInfoList *medium_list_;
  bool tablet_merge_finish_;
  TabletExecuteState execute_state_;
  TabletScheduleNewRoundState new_round_state_;
  bool is_inited_;
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_SCHEDULE_STATUS_CACHE_H_

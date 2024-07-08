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

#ifndef OCEABASE_STORAGE_OB_LS_META_
#define OCEABASE_STORAGE_OB_LS_META_

#include "lib/utility/utility.h"           // ObTimeGuard
#include "lib/lock/ob_spin_lock.h"
#include "logservice/palf/lsn.h"
#include "share/ob_ls_id.h"
#include "lib/ob_define.h"
#include "lib/function/ob_function.h"
#include "share/ob_unit_getter.h"
#include "storage/ls/ob_ls_state.h"
#include "storage/high_availability/ob_storage_ha_struct.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_garbage_collector.h"
#include "share/restore/ob_ls_restore_status.h"
#include "storage/tx/ob_id_service.h"
#include "storage/ls/ob_ls_saved_info.h"
#include "share/scn.h"
#include "storage/high_availability/ob_ls_transfer_info.h"
#include "storage/mview/ob_major_mv_merge_info.h"

namespace oceanbase
{
namespace storage
{
class ObLSCreateType
{
public:
  static const int64_t NORMAL = 0;
  static const int64_t RESTORE = 1;
  static const int64_t MIGRATE = 2;
  static const int64_t CLONE = 3;
};

class ObLSMeta
{
  friend class ObLSMetaPackage;

  OB_UNIS_VERSION_V(1);
public:
  ObLSMeta();
  ObLSMeta(const ObLSMeta &ls_meta);
  ~ObLSMeta() {}
  int init(const uint64_t tenant_id,
           const share::ObLSID &ls_id,
           const ObMigrationStatus &migration_status,
           const share::ObLSRestoreStatus &restore_status,
           const int64_t create_scn);
  void reset();
  bool is_valid() const;
  int set_start_work_state();
  int set_start_ha_state();
  int set_finish_ha_state();
  int set_remove_state();
  const ObLSPersistentState &get_persistent_state() const;
  ObLSMeta &operator=(const ObLSMeta &other);
  share::SCN get_clog_checkpoint_scn() const;
  palf::LSN &get_clog_base_lsn();
  int set_clog_checkpoint(const palf::LSN &clog_checkpoint_lsn,
                          const share::SCN &clog_checkpoint_scn,
                          const bool write_slog = true);
  int64_t get_rebuild_seq() const;
  int set_migration_status(const ObMigrationStatus &migration_status,
                           const bool write_slog = true);
  int get_migration_status (ObMigrationStatus &migration_status) const;
  int set_gc_state(const logservice::LSGCState &gc_state, const share::SCN &offline_scn);
  int get_gc_state(logservice::LSGCState &gc_state);
  int get_offline_scn(share::SCN &offline_scn);

  int set_restore_status(const share::ObLSRestoreStatus &restore_status);
  int get_restore_status(share::ObLSRestoreStatus &restore_status) const;
  int update_ls_replayable_point(const share::SCN &replayable_point);
  int get_ls_replayable_point(share::SCN &replayable_point);

  //for ha batch update ls meta element
  int update_ls_meta(
      const bool update_restore_status,
      const ObLSMeta &src_ls_meta);
  //for ha rebuild update ls meta
  int set_ls_rebuild();
  int check_valid_for_backup() const;
  share::SCN get_tablet_change_checkpoint_scn() const;
  int set_tablet_change_checkpoint_scn(const share::SCN &tablet_change_checkpoint_scn);
  share::SCN get_transfer_scn() const;
  int inc_update_transfer_scn(const share::SCN &transfer_scn);
  int update_id_meta(const int64_t service_type,
                     const int64_t limited_id,
                     const share::SCN &latest_scn,
                     const bool write_slog);
  int get_all_id_meta(transaction::ObAllIDMeta &all_id_meta) const;
  int get_saved_info(ObLSSavedInfo &saved_info);
  int build_saved_info();
  int set_saved_info(const ObLSSavedInfo &saved_info);
  int clear_saved_info();
  int get_migration_and_restore_status(
      ObMigrationStatus &migration_status,
      share::ObLSRestoreStatus &ls_restore_status);
  int set_rebuild_info(const ObLSRebuildInfo &rebuild_info);
  int get_rebuild_info(ObLSRebuildInfo &rebuild_info) const;
  int get_create_type(int64_t &create_type) const;
  int check_ls_need_online(bool &need_online) const;

  int init(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const ObMigrationStatus &migration_status,
      const share::ObLSRestoreStatus &restore_status,
      const share::SCN &create_scn);

  ObReplicaType get_replica_type() const
  { return unused_replica_type_; }
  // IF I have locked with W:
  //    lock with R/W will be succeed do nothing.
  // ELSE:
  //    lock with R/W
  class ObReentrantWLockGuard
  {
  public:
    ObReentrantWLockGuard(common::ObLatch &lock,
                          const bool try_lock = false,
                          const int64_t warn_threshold = 100 * 1000 /* 100 ms */);
    ~ObReentrantWLockGuard();
    inline int get_ret() const { return ret_; }
    void click(const char *mod = NULL) { time_guard_.click(mod); }
    bool locked() const { return common::OB_SUCCESS == ret_; }
  private:
    bool first_locked_;
    ObTimeGuard time_guard_;
    common::ObLatch &lock_;
    int ret_;
  };
  class ObReentrantRLockGuard
  {
  public:
    ObReentrantRLockGuard(common::ObLatch &lock,
                          const bool try_lock = false,
                          const int64_t warn_threshold = 100 * 1000 /* 100 ms */);
    ~ObReentrantRLockGuard();
    inline int get_ret() const { return ret_; }
    void click(const char *mod = NULL) { time_guard_.click(mod); }
    bool locked() const { return common::OB_SUCCESS == ret_; }
  private:
    bool first_locked_;
    ObTimeGuard time_guard_;
    common::ObLatch &lock_;
    int ret_;
  };
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(ls_persistent_state),
               K_(clog_checkpoint_scn), K_(clog_base_lsn),
               K_(rebuild_seq), K_(migration_status), K(gc_state_), K(offline_scn_),
               K_(restore_status), K_(replayable_point), K_(tablet_change_checkpoint_scn),
               K_(all_id_meta), K_(transfer_scn), K_(rebuild_info), K_(transfer_meta_info));
private:
  int check_can_update_();
public:
  mutable common::ObLatch rw_lock_;     // only for atomic read/write in memory.
  mutable common::ObLatch update_lock_; // only one process can update ls meta. both for write slog and memory
  uint64_t tenant_id_;
  share::ObLSID ls_id_;

private:
  void update_clog_checkpoint_in_ls_meta_package_(const share::SCN& clog_checkpoint_scn,
                                                  const palf::LSN& clog_base_lsn);
private:
  ObReplicaType unused_replica_type_;
  ObLSPersistentState ls_persistent_state_;
  typedef common::ObFunction<int(ObLSMeta &)> WriteSlog;
  // for test
  void set_write_slog_func_(WriteSlog write_slog);
  static WriteSlog write_slog_;

  // clog_checkpoint_scn_, meaning:
  // 1. dump points of all modules have exceeded clog_checkpoint_scn_
  // 2. all clog entries which log_scn are smaller than clog_checkpoint_scn_ can be recycled
  share::SCN clog_checkpoint_scn_;
  // clog_base_lsn_, meaning:
  // 1. all clog entries which lsn are smaller than clog_base_lsn_ have been recycled
  // 2. log_scn of log entry that clog_base_lsn_ points to is smaller than/equal to clog_checkpoint_scn_
  // 3. clog starts to replay log entries from clog_base_lsn_ on crash recovery
  palf::LSN clog_base_lsn_;
  int64_t rebuild_seq_;
  ObMigrationStatus migration_status_;
  logservice::LSGCState gc_state_;
  share::SCN offline_scn_;
  share::ObLSRestoreStatus restore_status_;
  share::SCN replayable_point_;
  //TODO(yaoying.yyy):modify this
  share::SCN tablet_change_checkpoint_scn_;
  transaction::ObAllIDMeta all_id_meta_;
  ObLSSavedInfo saved_info_;
  share::SCN transfer_scn_;
  ObLSRebuildInfo rebuild_info_;
  ObLSTransferMetaInfo transfer_meta_info_; //transfer_dml_ctrl_42x # placeholder
  ObMajorMVMergeInfo major_mv_merge_info_;
};

}  // namespace storage
}  // namespace oceanbase
#endif

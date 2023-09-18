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

#ifndef OCEABASE_STORAGE_OB_LS_
#define OCEABASE_STORAGE_OB_LS_

#include "lib/utility/ob_print_utils.h"
#include "common/ob_member_list.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_delegate.h"
#include "share/ob_tenant_info_proxy.h"
#include "lib/worker.h"
#include "storage/ls/ob_ls_lock.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/ls/ob_ls_tx_service.h"
#include "storage/ls/ob_ls_role_handler.h"
#include "storage/ls/ob_ls_fo_handler.h"
#include "storage/ls/ob_ls_backup_handler.h"
#include "storage/ls/ob_ls_rebuild_handler.h"
#include "storage/ls/ob_ls_archive_handler.h"
#include "storage/ls/ob_ls_meta.h"
#include "storage/ls/ob_freezer.h"
#include "storage/ls/ob_ls_sync_tablet_seq_handler.h"
#include "storage/ls/ob_ls_ddl_log_handler.h"
#include "storage/tx/wrs/ob_ls_wrs_handler.h"
#include "storage/ls/ob_ls_reserved_snapshot_mgr.h"
#include "storage/ls/ob_ls_storage_clog_handler.h"
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx/ob_keep_alive_ls_handler.h"
#include "storage/tx/ob_dup_table_util.h"
#include "storage/restore/ob_ls_restore_handler.h"
#include "logservice/applyservice/ob_log_apply_service.h"
#include "logservice/replayservice/ob_replay_handler.h"
#include "logservice/replayservice/ob_replay_status.h"
#include "logservice/rcservice/ob_role_change_handler.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"     // ObLogRestoreHandler
#include "logservice/ob_log_handler.h"
#ifdef OB_BUILD_ARBITRATION
#include "logservice/ob_arbitration_service.h"
#endif
#include "logservice/restoreservice/ob_log_restore_handler.h"     // ObLogRestoreHandler
#include "storage/ls/ob_ls_meta_package.h"
#include "storage/ls/ob_ls_get_mod.h"
#include "storage/tablelock/ob_lock_table.h"
#include "lib/hash/ob_multi_mod_ref_mgr.h"
#include "logservice/ob_garbage_collector.h"
#include "logservice/leader_coordinator/election_priority_impl/election_priority_impl.h"
#include "storage/high_availability/ob_ls_migration_handler.h"
#include "storage/high_availability/ob_ls_remove_member_handler.h"
#include "storage/high_availability/ob_ls_rebuild_cb_impl.h"
#include "storage/tx_storage/ob_tablet_gc_service.h"
#include "storage/tx_storage/ob_empty_shell_task.h"
#include "storage/high_availability/ob_transfer_handler.h"
#include "rootserver/ob_ls_recovery_stat_handler.h" //ObLSRecoveryStatHandler
#include "storage/high_availability/ob_ls_member_list_service.h"
#include "storage/high_availability/ob_ls_block_tx_service.h"
#include "storage/high_availability/ob_ls_transfer_info.h"
#include "observer/table/ttl/ob_tenant_tablet_ttl_mgr.h"

namespace oceanbase
{
namespace observer
{
class ObIMetaReport;
}
namespace share
{
class SCN;
}

namespace storage
{

struct ObLSVTInfo
{
  share::ObLSID ls_id_;
  ObReplicaType replica_type_;
  common::ObRole ls_state_;
  ObMigrationStatus migrate_status_;
  int64_t tablet_count_;
  share::SCN weak_read_scn_;
  bool need_rebuild_;
  share::SCN checkpoint_scn_;
  //TODO SCN
  int64_t checkpoint_lsn_;
  int64_t rebuild_seq_;
  share::SCN tablet_change_checkpoint_scn_;
  share::SCN transfer_scn_;
  bool tx_blocked_;
  TO_STRING_KV(K_(ls_id),
               K_(replica_type),
               K_(ls_state),
               K_(migrate_status),
               K_(tablet_count),
               K_(weak_read_scn),
               K_(checkpoint_scn),
               K_(checkpoint_lsn),
               K_(rebuild_seq),
               K_(tablet_change_checkpoint_scn),
               K_(transfer_scn),
               K_(tx_blocked));
};

// 诊断虚表统计信息
struct DiagnoseInfo
{
  DiagnoseInfo() { reset(); }
  ~DiagnoseInfo() { reset(); }
  bool is_role_sync() {
    return ((palf_diagnose_info_.election_role_ == palf_diagnose_info_.palf_role_) &&
            ((palf_diagnose_info_.palf_role_ == log_handler_diagnose_info_.log_handler_role_ &&
              palf_diagnose_info_.palf_proposal_id_ == log_handler_diagnose_info_.log_handler_proposal_id_) ||
             (palf_diagnose_info_.palf_role_ == restore_diagnose_info_.restore_role_ &&
              palf_diagnose_info_.palf_proposal_id_ == restore_diagnose_info_.restore_proposal_id_)));
  }
  int64_t ls_id_;
  logservice::LogHandlerDiagnoseInfo log_handler_diagnose_info_;
  palf::PalfDiagnoseInfo palf_diagnose_info_;
  logservice::RCDiagnoseInfo rc_diagnose_info_;
  logservice::ApplyDiagnoseInfo apply_diagnose_info_;
  logservice::ReplayDiagnoseInfo replay_diagnose_info_;
  logservice::GCDiagnoseInfo gc_diagnose_info_;
  checkpoint::CheckpointDiagnoseInfo checkpoint_diagnose_info_;
  logservice::RestoreDiagnoseInfo restore_diagnose_info_;
#ifdef OB_BUILD_ARBITRATION
  logservice::LogArbSrvDiagnoseInfo arb_srv_diagnose_info_;
#endif
  TO_STRING_KV(K(ls_id_),
               K(log_handler_diagnose_info_),
               K(palf_diagnose_info_),
               K(rc_diagnose_info_),
               K(apply_diagnose_info_),
               K(replay_diagnose_info_),
               K(gc_diagnose_info_),
               K(checkpoint_diagnose_info_),
               K(restore_diagnose_info_)
#ifdef OB_BUILD_ARBITRATION
               ,K(arb_srv_diagnose_info_)
#endif
               );
  void reset() {
    ls_id_ = -1;
    log_handler_diagnose_info_.reset();
    palf_diagnose_info_.reset();
    rc_diagnose_info_.reset();
    apply_diagnose_info_.reset();
    replay_diagnose_info_.reset();
    gc_diagnose_info_.reset();
    checkpoint_diagnose_info_.reset();
    restore_diagnose_info_.reset();
#ifdef OB_BUILD_ARBITRATION
    arb_srv_diagnose_info_.reset();
#endif
  }
};

class ObIComponentFactory;
enum class ObInnerLSStatus;

// sizeof(ObLS): 77248
class ObLS : public common::ObLink
{
public:
  friend ObLSLockGuard;
  friend class ObFreezer;
  friend class checkpoint::ObDataCheckpoint;
  friend class ObLSSwitchChecker;
public:
  static constexpr int64_t TOTAL_INNER_TABLET_NUM = 3;
  static const uint64_t INNER_TABLET_ID_LIST[TOTAL_INNER_TABLET_NUM];
  static const share::SCN LS_INNER_TABLET_FROZEN_SCN;
public:
  class ObLSInnerTabletIDIter
  {
  public:
    ObLSInnerTabletIDIter() : pos_(0) {}
    ~ObLSInnerTabletIDIter() { reset_(); }
    int get_next(common::ObTabletID &tablet_id);
    DISALLOW_COPY_AND_ASSIGN(ObLSInnerTabletIDIter);
  private:
    void reset_() { pos_ = 0; }
  private:
    int64_t pos_;
  };
public:
  ObLS();
  virtual ~ObLS();
  int init(const share::ObLSID &ls_id,
           const uint64_t tenant_id,
           const ObMigrationStatus &migration_status,
           const share::ObLSRestoreStatus &restore_status,
           const share::SCN &create_scn,
           observer::ObIMetaReport *reporter);
  // I am ready to work now.
  int start();
  int stop();
  void wait();
  int prepare_for_safe_destroy();
  bool safe_to_destroy();
  void destroy();
  int offline();
  int online();
  int offline_without_lock();
  int enable_for_restore();
  bool is_offline() const { return is_offlined_; } // mock function, TODO(@yanyuan)
  bool is_remove() const { return ATOMIC_LOAD(&is_remove_); }
  void set_is_remove() { return ATOMIC_STORE(&is_remove_, true); }

  ObLSTxService *get_tx_svr() { return &ls_tx_svr_; }
  ObLockTable *get_lock_table() { return &lock_table_; }
  ObTxTable *get_tx_table() { return &tx_table_; }
  ObLSWRSHandler *get_ls_wrs_handler() { return &ls_wrs_handler_; }
  rootserver::ObLSRecoveryStatHandler *get_ls_recovery_stat_handler() { return &ls_recovery_stat_handler_; }
  ObLSTabletService *get_tablet_svr() { return &ls_tablet_svr_; }
  share::ObLSID get_ls_id() const { return ls_meta_.ls_id_; }
  bool is_sys_ls() const { return ls_meta_.ls_id_.is_sys_ls(); }
  int get_replica_status(ObReplicaStatus &replica_status);
  uint64_t get_tenant_id() const { return ls_meta_.tenant_id_; }
  ObFreezer *get_freezer() { return &ls_freezer_; }
  common::ObMultiModRefMgr<ObLSGetMod> &get_ref_mgr() { return ref_mgr_; }
  checkpoint::ObCheckpointExecutor *get_checkpoint_executor() { return &checkpoint_executor_; }
  checkpoint::ObDataCheckpoint *get_data_checkpoint() { return &data_checkpoint_; }
  transaction::ObKeepAliveLSHandler *get_keep_alive_ls_handler() { return &keep_alive_ls_handler_; }
  ObLSRestoreHandler *get_ls_restore_handler() { return &ls_restore_handler_; }
  transaction::ObDupTableLSHandler *get_dup_table_ls_handler() { return &dup_table_ls_handler_; }
  ObLSDDLLogHandler *get_ddl_log_handler() { return &ls_ddl_log_handler_; }
  // ObObLogHandler interface:
  // get the log_service pointer
  logservice::ObLogHandler *get_log_handler() { return &log_handler_; }
  logservice::ObLogRestoreHandler *get_log_restore_handler() { return &restore_handler_; }
  logservice::ObRoleChangeHandler *get_role_change_handler() { return &role_change_handler_;}
  logservice::ObRoleChangeHandler *get_restore_role_change_handler() { return &restore_role_change_handler_;}
  logservice::ObGCHandler *get_gc_handler() { return &gc_handler_; }
  //migration handler
  ObLSMigrationHandler *get_ls_migration_handler() { return &ls_migration_handler_; }
  //migration handler
  ObTransferHandler *get_transfer_handler() { return &transfer_handler_; }
  ObLSTransferInfo &get_ls_startup_transfer_info() { return startup_transfer_info_; }

  //remove member handler
  ObLSRemoveMemberHandler *get_ls_remove_member_handler() { return &ls_remove_member_handler_; }

  checkpoint::ObTabletGCHandler *get_tablet_gc_handler() { return &tablet_gc_handler_; }
  ObLSMemberListService *get_member_list_service() { return &member_list_service_; }
  checkpoint::ObTabletEmptyShellHandler *get_tablet_empty_shell_handler() { return &tablet_empty_shell_handler_; }
  // make sure the schema version does not back off.
  int save_base_schema_version();

  // get ls info
  int get_ls_info(ObLSVTInfo &ls_info);
  int get_ls_role(ObRole &role);
  // report the ls replica info to RS.
  int report_replica_info();

  // set create state of ls.
  // @param[in] new_status, the new create state which will be set.
  void set_create_state(const ObInnerLSStatus new_status);
  ObInnerLSStatus get_create_state() const;

  bool is_create_committed() const;
  bool is_need_gc() const;
  bool is_in_gc();
  bool is_enable_for_restore() const;
  // for rebuild
  // remove inner tablet, the memtable and minor sstable of data tablet, disable replay
  // int prepare_rebuild();
  // int delete_tablet();
  // int disable_replay();

  // create myself at disk
  // @param[in] tenant_role, role of tenant, which determains palf access mode
  // @param[in] palf_base_info, all the info that palf needed
  // @param[in] allow_log_sync, if palf is allowed to sync log, will be removed
  // after migrating as learner
  int create_ls(const share::ObTenantRole tenant_role,
                const palf::PalfBaseInfo &palf_base_info,
                const common::ObReplicaType &replica_type,
                const bool allow_log_sync);
  // load ls info from disk
  // @param[in] tenant_role, role of tenant, which determains palf access mode
  // @param[in] palf_base_info, all the info that palf needed
  // @param[in] allow_log_sync, if palf is allowed to sync log, will be removed
  // after migrating as learner
  int load_ls(const share::ObTenantRole &tenant_role,
              const palf::PalfBaseInfo &palf_base_info,
              const bool allow_log_sync);
  // remove the durable info of myself from disk.
  int remove_ls();
  // create all the inner tablet.
  int create_ls_inner_tablet(const lib::Worker::CompatMode compat_mode,
                             const share::SCN &create_scn);
  // load all the inner tablet.
  int load_ls_inner_tablet();

  // get the meta package of ls: ObLSMeta, PalfBaseInfo
  // @param[in] check_archive, if need check archive,
  // for backup task is false, migration/rebuild is true
  // @param[out] meta_package, the meta package.
  int get_ls_meta_package(const bool check_archive, ObLSMetaPackage &meta_package);
  // get only the meta of ls.
  const ObLSMeta &get_ls_meta() const { return ls_meta_; }
  // get current ls meta.
  // @param[out] ls_meta, store ls's current meta.
  int get_ls_meta(ObLSMeta &ls_meta) const;
  // update the ls meta of ls.
  // @param[in] ls_meta, which is used to update the ls's meta.
  int set_ls_meta(const ObLSMeta &ls_meta);
  // finish ls create process. set the create state to COMMITTED or ABORTED.
  // @param[in] is_commit, whether the create process is commit or not.
  void finish_create(const bool is_commit);

  // for ls gc
  int block_tablet_transfer_in();
  int block_tx_start();
  int block_all();
  // for tablet transfer
  // this function is used for tablet transfer in
  // it will check if it is allowed to transfer in and then
  // do transfer in.
  // @param [in] tablet_id, the tablet want to transfer.
  // @return OB_OP_NOT_ALLOW, if the ls is blocked state there is no ls can transfer in.
  int tablet_transfer_in(const ObTabletID &tablet_id);

  // do the work after slog replay
  // 1) rewrite the migration status if it is failed.
  // 2) load inner tablet and start to work if it is a normal ls.
  int finish_slog_replay();

  // get tablet while replaying clog
  int replay_get_tablet(const common::ObTabletID &tablet_id,
                        const share::SCN &scn,
                        const bool is_update_mds_table,
                        ObTabletHandle &handle) const;
  // get tablet but don't check user_data while replaying clog, because user_data may not exist.
  int replay_get_tablet_no_check(
      const common::ObTabletID &tablet_id,
      const share::SCN &scn,
      ObTabletHandle &tablet_handle) const;

  int flush_if_need(const bool need_flush);
  int try_sync_reserved_snapshot(const int64_t new_reserved_snapshot, const bool update_flag);
  bool is_stopped() const { return is_stopped_; }
  int check_can_replay_clog(bool &can_replay);

  TO_STRING_KV(K_(ls_meta), K_(switch_epoch), K_(log_handler), K_(restore_handler), K_(is_inited), K_(tablet_gc_handler), K_(startup_transfer_info));
private:
  int ls_init_for_dup_table_();
  int ls_destory_for_dup_table_();
  int stop_();
  void wait_();
  int prepare_for_safe_destroy_();
  int flush_if_need_(const bool need_flush);
  int offline_(const int64_t start_ts);
  int offline_compaction_();
  int online_compaction_();
  int offline_tx_(const int64_t start_ts);
  int online_tx_();
  int offline_advance_epoch_();
  int online_advance_epoch_();
public:
  // ObLSMeta interface:
  int update_ls_meta(const bool update_restore_status,
                     const ObLSMeta &src_ls_meta);

  // int update_id_meta(const int64_t service_type,
  //                    const int64_t limited_id,
  //                    const int64_t latest_log_ts,
  //                    const bool write_slog);
  int get_transfer_scn(share::SCN &scn);
  DELEGATE_WITH_RET(ls_meta_, update_id_meta, int);
  int set_ls_rebuild();
  // protect in ls lock
  // int set_gc_state(const logservice::LSGCState &gc_state);
  int set_gc_state(const logservice::LSGCState &gc_state);
  int set_gc_state(const logservice::LSGCState &gc_state, const share::SCN &offline_scn);
  // int set_clog_checkpoint(const palf::LSN &clog_checkpoint_lsn,
  //                         const share::SCN &clog_checkpoint_scn,
  //                         const bool write_slog = true);
  DELEGATE_WITH_RET(ls_meta_, set_clog_checkpoint, int);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_clog_checkpoint_scn, share::SCN);
  DELEGATE_WITH_RET(ls_meta_, get_clog_base_lsn, palf::LSN &);
  DELEGATE_WITH_RET(ls_meta_, get_saved_info, int);
  // int build_saved_info();
  DELEGATE_WITH_RET(ls_meta_, build_saved_info, int);
  // int clear_saved_info_without_lock();
  DELEGATE_WITH_RET(ls_meta_, clear_saved_info, int);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_rebuild_seq, int64_t);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_tablet_change_checkpoint_scn, share::SCN);
  DELEGATE_WITH_RET(ls_meta_, set_tablet_change_checkpoint_scn, int);
  int set_restore_status(
      const share::ObLSRestoreStatus &restore_status,
      const int64_t rebuild_seq);
  // get restore status
  // @param [out] restore status.
  // int get_restore_status(share::ObLSRestoreStatus &status);
  DELEGATE_WITH_RET(ls_meta_, get_restore_status, int);
  int set_migration_status(
      const ObMigrationStatus &migration_status,
      const int64_t rebuild_seq,
      const bool write_slog = true);
  // get migration status
  // @param [out] migration status.
  // int get_migration_status(ObMigrationstatus &status);
  DELEGATE_WITH_RET(ls_meta_, get_migration_status, int);
  // get gc state
  // @param [in] gc state.
  // int get_gc_state(LSGCState &status);
  DELEGATE_WITH_RET(ls_meta_, get_gc_state, int);
  // get offline ts
  // @param [in] offline ts.
  // int get_offline_scn(const share::SCN &offline_scn);
  DELEGATE_WITH_RET(ls_meta_, get_offline_scn, int);
  // update replayable point
  // @param [in] replayable point.
  // int update_ls_replayable_point(const int64_t replayable_point);
  DELEGATE_WITH_RET(ls_meta_, update_ls_replayable_point, int);
  // update replayable point
  // get replayable point
  // @param [in] replayable point
  // int get_ls_replayable_point(int64_t &replayable_point);
  DELEGATE_WITH_RET(ls_meta_, get_ls_replayable_point, int);
  int inc_update_transfer_scn(const share::SCN &transfer_scn);
  int set_transfer_scn(const share::SCN &transfer_scn);
  // get ls_meta_package and unsorted tablet_ids, add read lock of LSLOCKLOGMETA.
  // @param [in] check_archive if need check archive, for backup task is false, migration/rebuild is true
  // @param [out] meta_package
  // @param [out] tablet_ids
  int get_ls_meta_package_and_tablet_ids(const bool check_archive,
                                         ObLSMetaPackage &meta_package,
                                         common::ObIArray<common::ObTabletID> &tablet_ids);
  DELEGATE_WITH_RET(ls_meta_, get_migration_and_restore_status, int);
  DELEGATE_WITH_RET(ls_meta_, set_rebuild_info, int);
  DELEGATE_WITH_RET(ls_meta_, get_rebuild_info, int);


  // get ls_meta_package and sorted tablet_metas for backup. tablet gc is forbidden meanwhile.
  // @param [in] check_archive if need check archive, migration/rebuild is true
  // @param [in] handle_ls_meta_f, ls meta callback, will be first called.
  // @param [in] handle_tablet_meta_f, tablet meta callback
  typedef common::ObFunction<int(const ObLSMetaPackage &meta_package)> HandleLSMetaFunc;
  int get_ls_meta_package_and_tablet_metas(
      const bool check_archive,
      const HandleLSMetaFunc &handle_ls_meta_f,
      const ObLSTabletService::HandleTabletMetaFunc &handle_tablet_meta_f);

  // ObLSTabletService interface:
  // ObLSTabletService interface:
  // update tablet by checkpoint
  // @param [in] key, key of tablet that will be updated
  // @param [in] new_addr, new addr of the tablet
  // @param [out] new_handle, new tablet handle
  DELEGATE_WITH_RET(ls_tablet_svr_, update_tablet_checkpoint, int);
  // get a tablet handle
  // @param [in] tablet_id, the tablet needed
  // @param [out] handle, store the tablet and inc ref.
  // @param [in] timeout_us, timeout(mircosecond) for get tablet
  // @param [in] mode, read mds tablet isolation level
  int get_tablet(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &handle,
      const int64_t timeout_us = ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
      const ObMDSGetTabletMode mode = ObMDSGetTabletMode::READ_READABLE_COMMITED)
  {
    return ls_tablet_svr_.get_tablet(tablet_id, handle, timeout_us, mode);
  }
 // get ls tablet iterator
  // @param [out] iterator, ls tablet iterator to iterate all tablets in ls
  // int build_tablet_iter(ObLSTabletIterator &iter);
  // int build_tablet_iter(ObLSTabletIDIterator &iter);
  DELEGATE_WITH_RET(ls_tablet_svr_, build_tablet_iter, int);
  // update medium compaction info for tablet
  DELEGATE_WITH_RET(ls_tablet_svr_, update_medium_compaction_info, int);
  // trim rebuild tablet
  // @param [in] tablet_id ObTabletID, is_rollback bool
  // @param [out] null
  // int trim_rebuild_tablet(
  //    const ObTabletID &tablet_id,
  //    const bool is_rollback = false);
  DELEGATE_WITH_RET(ls_tablet_svr_, trim_rebuild_tablet, int);
  // remove tablets
  // @param [in] tbalet_ids ObIArray<ObTabletId>
  // @param [out] null
  // int remote_tablets(
  //     const common::ObIArray<common::ObTabletID> &tablet_id_array);
  DELEGATE_WITH_RET(ls_tablet_svr_, remove_tablets, int);
  // create_ls_inner_tablet
  // @param [in] ls_id
  // @param [in] tablet_id
  // @param [in] memstore_version
  // @param [in] frozen_timestamp
  // @param [in] table_schema
  // @param [in] compat_mode
  // @param [in] create_scn
  // int create_ls_inner_tablet(
  //     const share::ObLSID &ls_id,
  //     const common::ObTabletID &tablet_id,
  //     const int64_t frozen_timestamp,
  //     const share::schema::ObTableSchema &table_schema,
  //     const lib::Worker::CompatMode &compat_mode,
  //     const share::SCN &create_scn);
  DELEGATE_WITH_RET(ls_tablet_svr_, create_ls_inner_tablet, int);
  // remove_ls_inner_tablet
  // @param [in] ls_id
  // @param [in] tablet_id
  // int remove_ls_inner_tablet(
  //     const share::ObLSID &ls_id,
  //     const common::ObTabletID &tablet_id);
  DELEGATE_WITH_RET(ls_tablet_svr_, remove_ls_inner_tablet, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, rebuild_create_tablet, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, update_tablet_ha_data_status, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, ha_get_tablet, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, update_tablet_restore_status, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, create_or_update_migration_tablet, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, flush_mds_table, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, enable_to_read, void);
  DELEGATE_WITH_RET(ls_tablet_svr_, disable_to_read, void);
  DELEGATE_WITH_RET(ls_tablet_svr_, get_tablet_with_timeout, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, get_mds_table_mgr, int);

  // ObLockTable interface:
  // check whether the lock op is conflict with exist lock.
  // @param[in] mem_ctx, the memtable ctx of current transaction.
  // @param[in] lock_op, which lock op will try to execute.
  // @param[out] conflict_tx_set, contain the conflict transaction it.
  // int check_lock_conflict(const ObMemtableCtx *mem_ctx,
  //                         const ObTableLockOp &lock_op,
  //                         ObTxIDSet &conflict_tx_set);
  DELEGATE_WITH_RET(lock_table_, check_lock_conflict, int);
  // lock a object
  // @param[in] ctx, store ctx for trans.
  // @param[in] param, contain the lock id, lock type and so on.
  // int lock(ObStoreCtx &ctx,
  //          const transaction::tablelock::ObLockParam &param);
  DELEGATE_WITH_RET(lock_table_, lock, int);
  // unlock a object
  // @param[in] ctx, store ctx for trans.
  // @param[in] param, contain the lock id, lock type and so on.
  // int unlock(ObStoreCtx &ctx,
  //            const transaction::tablelock::ObLockParam &param);
  DELEGATE_WITH_RET(lock_table_, unlock, int);
  // admin remove a lock op
  // @param[in] op_info, contain the lock id, lock type and so on.
  // void admin_remove_lock_op(const ObTableLockOp &op_info);
  DELEGATE_WITH_RET(lock_table_, admin_remove_lock_op, int);
  // used by admin tool. update lock op status.
  // @param[in] op_info, the lock/unlock op will be update by admin.
  // @param[in] commit_version, set the commit version.
  // @param[in] commit_scn, set the commit logts.
  // @param[in] status, the lock op status will be set.
  // int admin_update_lock_op(const ObTableLockOp &op_info,
  //                          const share::SCN &commit_version,
  //                          const share::SCN &commit_scn,
  //                          const ObTableLockOpStatus status);
  DELEGATE_WITH_RET(lock_table_, admin_update_lock_op, int);
  // get the lock memtable, used by ObMemtableCtx create process.
  // @param[in] handle, will store the memtable of lock table.
  // int get_lock_memtable(ObTableHandleV2 &handle)
  DELEGATE_WITH_RET(lock_table_, get_lock_memtable, int);
  // get all the lock id in the lock map
  // @param[out] iter, the iterator returned.
  // int get_lock_id_iter(ObLockIDIterator &iter);
  DELEGATE_WITH_RET(lock_table_, get_lock_id_iter, int);
  // get the lock op iterator of a obj lock
  // @param[in] lock_id, which obj lock's lock op will be iterated.
  // @param[out] iter, the iterator returned.
  // int get_lock_op_iter(const ObLockID &lock_id,
  //                      ObLockOpIterator &iter);
  DELEGATE_WITH_RET(lock_table_, get_lock_op_iter, int);
  // check and clear lock ops and obj locks in this ls (or lock_table)
  // @param[in] force_compact, if it's set to true, the gc thread will
  // force compact unlock op which is committed, even though there's
  // no paired lock op.
  // int check_and_clear_obj_lock(const bool force_compact)
  DELEGATE_WITH_RET(lock_table_, check_and_clear_obj_lock, int);

  // set the member_list of log_service
  // @param [in] member_list, the member list to be set.
  // @param [in] replica_num, the number of replica
  // int set_initial_member_list(const common::ObMemberList &member_list,
  //                             const int64_t paxos_replica_num);
  DELEGATE_WITH_RET(log_handler_, set_initial_member_list, int);
  // get the member list of log_service
  // @param [out] member_list, the member_list of current log_service
  // @param [out] quorum, the quorum of member_list
  // int get_paxos_member_list(common::ObMemberList &member_list, int64_t &quorum) const;
  CONST_DELEGATE_WITH_RET(log_handler_, get_paxos_member_list, int);
  // get paxos member list and learner list of log_service
  // @param [out] member_list, the member_list of current log_service
  // @param [out] quorum, the quorum of member_list
  // @param [out] learner_list, the learner list of log_service
  CONST_DELEGATE_WITH_RET(log_handler_, get_paxos_member_list_and_learner_list, int);
  // advance the base_lsn of log_handler.
  // @param[in] palf_base_info, the palf meta used to advance base lsn.
  // int advance_base_info(const palf::PalfBaseInfo &palf_base_info);
  DELEGATE_WITH_RET(log_handler_, advance_base_info, int);

  // get ls readable_scn considering readable scn, sync scn and replayable scn.
  // @param[out] readable_scn ls readable_scn
  // int get_ls_replica_readable_scn(share::SCN &readable_scn)
  DELEGATE_WITH_RET(ls_recovery_stat_handler_, get_ls_replica_readable_scn, int);

  // get ls level recovery_stat by LS leader.
  // If follower LS replica call this function, it will return OB_NOT_MASTER.
  // @param[out] ls_recovery_stat
  // int get_ls_replica_readable_scn(share::SCN &readable_scn)
  DELEGATE_WITH_RET(ls_recovery_stat_handler_, get_ls_level_recovery_stat, int);

  // disable clog sync.
  // with ls read lock and log write lock.
  int disable_sync();
  // WARNING: must has ls read lock and log write lock.
  int enable_replay();
  int enable_replay_without_lock();
  // @brief, disable replay for current ls.
  // with ls read lock and log write lock.
  int disable_replay();
  // WARNING: must has ls read lock and log write lock.
  int disable_replay_without_lock();
  // @brief, get max decided log scn considering both apply and replay.
  // @param[out] share::SCN&, max decided log scn.
  DELEGATE_WITH_RET(log_handler_, get_max_decided_scn, int);
  // @breif,get member stat: whether in paxos member list or learner list and whether is migrating
  // @param[in] const common::ObAddr, request server.
  // @param[out] bool &(is_valid_member),
  // @param[out] LogMemberGCStat&,
  DELEGATE_WITH_RET(log_handler_, get_member_gc_stat, int);
  // @brief append count bytes from the buffer starting at buf to the palf handle, return the LSN and timestamp
  // @param[in] const void *, the data buffer.
  // @param[in] const uint64_t, the length of data buffer.
  // @param[in] const int64_t, the base timestamp(ns), palf will ensure that the return tiemstamp will greater
  //            or equal than this field.
  // @param[in] const bool, decide this append option whether need block thread.
  // @param[int] AppendCb*, the callback of this append option, log handler will ensure that cb will be called after log has been committed
  // @param[out] LSN&, the append position.
  // @param[out] int64_t&, the append timestamp.
  // @retval
  //    OB_SUCCESS
  //    OB_NOT_MASTER, the prospoal_id of ObLogHandler is not same with PalfHandle.
  DELEGATE_WITH_RET(log_handler_, append, int);
  // @breif, palf enable vote
  // @param[in] null.
  // @param[out] null.
  DELEGATE_WITH_RET(log_handler_, enable_vote, int);
  // @breif, palf disable vote
  // @param[in] need_check.
  // @param[out] null.
  DELEGATE_WITH_RET(log_handler_, disable_vote, int);
  DELEGATE_WITH_RET(log_handler_, remove_member, int);
  DELEGATE_WITH_RET(log_handler_, remove_learner, int);
#ifdef OB_BUILD_ARBITRATION
  DELEGATE_WITH_RET(log_handler_, add_arbitration_member, int);
  DELEGATE_WITH_RET(log_handler_, remove_arbitration_member, int);
#endif
  DELEGATE_WITH_RET(log_handler_, is_in_sync, int);
  DELEGATE_WITH_RET(log_handler_, get_end_scn, int);
  DELEGATE_WITH_RET(log_handler_, disable_sync, int);
  DELEGATE_WITH_RET(log_handler_, change_replica_num, int);
  DELEGATE_WITH_RET(log_handler_, get_end_lsn, int);
  DELEGATE_WITH_RET(log_handler_, try_lock_config_change, int);
  DELEGATE_WITH_RET(log_handler_, unlock_config_change, int);
  DELEGATE_WITH_RET(log_handler_, get_config_change_lock_stat, int);
  DELEGATE_WITH_RET(log_handler_, switch_acceptor_to_learner, int);
  DELEGATE_WITH_RET(member_list_service_, switch_learner_to_acceptor, int);
  DELEGATE_WITH_RET(member_list_service_, add_member, int);
  DELEGATE_WITH_RET(member_list_service_, replace_member, int);
  DELEGATE_WITH_RET(member_list_service_, replace_member_with_learner, int);
  DELEGATE_WITH_RET(member_list_service_, get_config_version_and_transfer_scn, int);
  DELEGATE_WITH_RET(member_list_service_, get_max_tablet_transfer_scn, int);
  DELEGATE_WITH_RET(log_handler_, add_learner, int);
  DELEGATE_WITH_RET(log_handler_, replace_learners, int);
  DELEGATE_WITH_RET(block_tx_service_, ha_block_tx, int);
  DELEGATE_WITH_RET(block_tx_service_, ha_kill_tx, int);
  DELEGATE_WITH_RET(block_tx_service_, ha_unblock_tx, int);

  // Create a TxCtx whose tx_id is specified
  // @param [in] tx_id: transaction ID
  // @param [in] for_replay: Identifies whether the TxCtx is created for replay processing;
  // @param [out] existed: if it's true, means that an existing TxCtx with the same tx_id has
  //        been found, and the found TxCtx will be returned through the outgoing parameter tx_ctx;
  // @param [out] tx_ctx: newly allocated or already exsited transaction context
  // Return Values That Need Attention:
  // @return OB_SUCCESS, if the tx_ctx newly allocated or already existed
  // @return OB_NOT_MASTER, if this ls is a follower replica
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, create_tx_ctx, int);

  // Find specified TxCtx from the ObLSTxCtxMgr;
  // @param [in] tx_id: transaction ID
  // @param [in] for_replay: Identifies whether the TxCtx is used by replay processing;
  // @param [out] tx_ctx: context found through ObLSTxCtxMgr's hash table
  // Return Values That Need Attention:
  // @return OB_NOT_MASTER, if the LogStream is follower replica
  // @return OB_TRANS_CTX_NOT_EXIST, if the specified TxCtx is not found;
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, get_tx_ctx, int);

  // Decrease the specified tx_ctx's reference count
  // @param [in] tx_ctx: the TxCtx will be revert
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, revert_tx_ctx, int);

  CONST_DELEGATE_WITH_RET(ls_tx_svr_, get_read_store_ctx, int);
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, get_write_store_ctx, int);
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, revert_store_ctx, int);

  DELEGATE_WITH_RET(ls_tx_svr_, get_common_checkpoint_info, int);
  // check whether all the tx of this ls is cleaned up.
  // @return OB_SUCCESS, all the tx of this ls cleaned up
  // @return other, there is something wrong or there is some tx not cleaned up.
  // int check_all_tx_clean_up() const;
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, check_all_tx_clean_up, int);
  // check whether all readonly tx of this ls is cleaned up.
  // @return OB_SUCCESS, all the readonly tx of this ls cleaned up
  // @return other, there is something wrong or there is some readonly tx not cleaned up.
  // int check_all_readonly_tx_clean_up() const;
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, check_all_readonly_tx_clean_up, int);
  // block new tx in for ls.
  // @return OB_SUCCESS, ls is blocked
  // @return other, there is something wrong.
  // int block_tx();
  DELEGATE_WITH_RET(ls_tx_svr_, block_tx, int);
  // kill all the tx of this ls.
  // @param [in] graceful: kill all tx by write abort log or not
  // @return OB_SUCCESS, ls is blocked
  // @return other, there is something wrong.
  // int kill_all_tx(const bool graceful);
  DELEGATE_WITH_RET(ls_tx_svr_, kill_all_tx, int);
  // Check whether all the transactions that modify the specified tablet before
  // a schema version are finished.
  // @param [in] schema_version: the schema_version to check
  // @param [out] block_tx_id: a running transaction that modify the tablet before schema version.
  // Return Values That Need Attention:
  // @return OB_EAGAIN: Some TxCtx that has modify the tablet before schema
  // version is running;
  // int check_modify_schema_elapsed(const common::ObTabletID &tablet_id,
  //                                 const int64_t schema_version,
  //                                 ObTransID &block_tx_id);
  DELEGATE_WITH_RET(ls_tx_svr_, check_modify_schema_elapsed, int);
  // Check whether all the transactions that modify the specified tablet before
  // a timestamp are finished.
  // @param [in] timestamp: the timestamp to check
  // @param [out] block_tx_id: a running transaction that modify the tablet before timestamp.
  // Return Values That Need Attention:
  // @return OB_EAGAIN: Some TxCtx that has modify the tablet before timestamp
  // is running;
  // int check_modify_time_elapsed(const common::ObTabletID &tablet_id,
  //                               const int64_t timestamp,
  //                               ObTransID &block_tx_id);
  DELEGATE_WITH_RET(ls_tx_svr_, check_modify_time_elapsed, int);
  // get tx scheduler in ls tx service
  // @param [in] tx_id: wish to get this tx_id scheduler
  // @param [out] scheduler: scheduler of this tx_id
  // int get_tx_scheduler(const transaction::ObTransID &tx_id, ObAddr &scheduler) const;
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, get_tx_scheduler, int);
  // iterate the obj lock op at tx service.
  // int iterate_tx_obj_lock_op(ObLockOpIterator &iter) const;
  CONST_DELEGATE_WITH_RET(ls_tx_svr_, iterate_tx_obj_lock_op, int);

  DELEGATE_WITH_RET(ls_tx_svr_, get_tx_ctx_count, int);
  DELEGATE_WITH_RET(ls_tx_svr_, get_active_tx_count, int);
  DELEGATE_WITH_RET(ls_tx_svr_, print_all_tx_ctx, int);
  //dup table ls meta interface
  CONST_DELEGATE_WITH_RET(dup_table_ls_handler_, get_dup_table_ls_meta, int);
  DELEGATE_WITH_RET(dup_table_ls_handler_, set_dup_table_ls_meta, int);

  // ObReplayHandler interface:
  DELEGATE_WITH_RET(replay_handler_, replay, int);

  // ObFreezer interface:
  // freeze the data of ls:
  // @param [in] is_sync, only used for wait_freeze_finished()
  // @param [in] abs_timeout_ts, wait until timeout if lock conflict
  int logstream_freeze(const bool is_sync = false,
                       const int64_t abs_timeout_ts = INT64_MAX);
  // tablet freeze
  // @param [in] is_sync, only used for wait_freeze_finished()
  // @param [in] abs_timeout_ts, wait until timeout if lock conflict
  int tablet_freeze(const ObTabletID &tablet_id,
                    const bool is_sync = false,
                    const int64_t abs_timeout_ts = INT64_MAX);
  // force freeze tablet
  // @param [in] abs_timeout_ts, wait until timeout if lock conflict
  int force_tablet_freeze(const ObTabletID &tablet_id,
                          const int64_t abs_timeout_ts = INT64_MAX);
  // batch tablet freeze
  // @param [in] tablet_ids
  // @param [in] is_sync
  // @param [in] abs_timeout_ts, wait until timeout if lock conflict
  int batch_tablet_freeze(const ObIArray<ObTabletID> &tablet_ids,
                          const bool is_sync = false,
                          const int64_t abs_timeout_ts = INT64_MAX);

  // ObTxTable interface
  DELEGATE_WITH_RET(tx_table_, get_tx_table_guard, int);
  DELEGATE_WITH_RET(tx_table_, get_upper_trans_version_before_given_scn, int);
  DELEGATE_WITH_RET(tx_table_, generate_virtual_tx_data_row, int);
  DELEGATE_WITH_RET(tx_table_, dump_single_tx_data_2_text, int);

  // ObCheckpointExecutor interface:
  DELEGATE_WITH_RET(checkpoint_executor_, get_checkpoint_info, int);
  // advance the checkpoint of this ls
  // @param [in] abs_timeout_ts, wait until timeout if lock conflict
  int advance_checkpoint_by_flush(share::SCN recycle_scn,
                                  const int64_t abs_timeout_ts = INT64_MAX,
                                  const bool is_tenant_freeze = false);

  // ObDataCheckpoint interface:
  DELEGATE_WITH_RET(data_checkpoint_, get_freezecheckpoint_info, int);
  DELEGATE_WITH_RET(keep_alive_ls_handler_, get_min_start_scn, void);

  // update tablet table store here do not using Macro because need lock ls and tablet
  // update table store for tablet
  // @param [in] tablet_id, the tablet id for target tablet
  // @param [in] param, parameters needed to update tablet
  // @param [out] handle, new tablet handle
  int update_tablet_table_store(
      const ObTabletID &tablet_id,
      const ObUpdateTableStoreParam &param,
      ObTabletHandle &handle);
  int update_tablet_table_store(
      const int64_t ls_rebuild_seq,
      const ObTabletHandle &old_tablet_handle,
      const ObIArray<storage::ObITable *> &tables);
  int build_ha_tablet_new_table_store(
      const ObTabletID &tablet_id,
      const ObBatchUpdateTableStoreParam &param);
  int build_new_tablet_from_mds_table(
      const int64_t ls_rebuild_seq,
      const common::ObTabletID &tablet_id,
      const int64_t mds_construct_sequence,
      const share::SCN &flush_scn);
  int try_update_uppder_trans_version();
  int diagnose(DiagnoseInfo &info) const;

  DELEGATE_WITH_RET(reserved_snapshot_mgr_, replay_reserved_snapshot_log, int);
  DELEGATE_WITH_RET(reserved_snapshot_mgr_, get_min_reserved_snapshot, int64_t);
  DELEGATE_WITH_RET(reserved_snapshot_mgr_, add_dependent_medium_tablet, int);
  DELEGATE_WITH_RET(reserved_snapshot_mgr_, del_dependent_medium_tablet, int);
private:
  // StorageBaseUtil
  // table manager: create, remove and guard get.
  ObLSTabletService ls_tablet_svr_;
  // log service for ls
  // log_service manager: create, remove and get
  logservice::ObLogHandler log_handler_;
  logservice::ObRoleChangeHandler role_change_handler_;
  // trans service for ls
  ObLSTxService ls_tx_svr_;

  // for log replay
  logservice::ObReplayHandler replay_handler_;

  // for log restore
  logservice::ObLogRestoreHandler restore_handler_;
  logservice::ObRoleChangeHandler restore_role_change_handler_;

  // for obIcheckpoint manager
  checkpoint::ObCheckpointExecutor checkpoint_executor_;
  // for ls freeze
  ObFreezer ls_freezer_;
  // for GC
  logservice::ObGCHandler gc_handler_;
  // for FO
  // ObLSFailoverHandler ls_failover_handler_;
  // for Backup
  // ObLSBackupHandler ls_backup_handler_;
  // for rebuild
  // ObLSRebuildHandler ls_rebuild_handler_;
  // for log archive
  // ObLSArchiveHandler ls_archive_handler_;
  // for restore
  ObLSRestoreHandler ls_restore_handler_;
  ObTxTable tx_table_;
  checkpoint::ObDataCheckpoint data_checkpoint_;
  // for lock table
  ObLockTable lock_table_;
  // handler for TABLET_SEQ_SYNC_LOG
  ObLSSyncTabletSeqHandler ls_sync_tablet_seq_handler_;
  // log handler for DDL
  ObLSDDLLogHandler ls_ddl_log_handler_;
  // interface for submit keep alive log
  transaction::ObKeepAliveLSHandler keep_alive_ls_handler_;

  // dup_table ls interface ,alloc memory when discover a dup_table_tablet
  transaction::ObDupTableLSHandler dup_table_ls_handler_;

  ObLSWRSHandler ls_wrs_handler_;
  //for migration
  ObLSMigrationHandler ls_migration_handler_;
  //for remove member
  ObLSRemoveMemberHandler ls_remove_member_handler_;
  //for rebuild
  ObLSRebuildCbImpl ls_rebuild_cb_impl_;
  // for tablet gc
  checkpoint::ObTabletGCHandler tablet_gc_handler_;
  // for update tablet to empty shell
  checkpoint::ObTabletEmptyShellHandler tablet_empty_shell_handler_;
  // record reserved snapshot
  ObLSReservedSnapshotMgr reserved_snapshot_mgr_;
  ObLSResvSnapClogHandler reserved_snapshot_clog_handler_;
  ObMediumCompactionClogHandler medium_compaction_clog_handler_;
  rootserver::ObLSRecoveryStatHandler ls_recovery_stat_handler_;
  ObLSMemberListService member_list_service_;
  ObLSBlockTxService block_tx_service_;
  table::ObTenantTabletTTLMgr tablet_ttl_mgr_;
private:
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_stopped_;
  bool is_offlined_;
  bool is_remove_;
  uint64_t switch_epoch_;// started from 0, odd means online, even means offline
  ObLSMeta ls_meta_;
  observer::ObIMetaReport *rs_reporter_;
  ObLSLock lock_;
  common::ObMultiModRefMgr<ObLSGetMod> ref_mgr_;
  logservice::coordinator::ElectionPriorityImpl election_priority_;
  //for transfer
  ObTransferHandler transfer_handler_;
  // Record the dependent transfer information when restarting
  ObLSTransferInfo startup_transfer_info_;

};

}
}
#endif

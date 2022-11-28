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
#include "storage/checkpoint/ob_checkpoint_executor.h"
#include "storage/checkpoint/ob_data_checkpoint.h"
#include "storage/tx_table/ob_tx_table.h"
#include "storage/tx/ob_keep_alive_ls_handler.h"
#include "storage/restore/ob_ls_restore_handler.h"
#include "logservice/replayservice/ob_replay_handler.h"
#include "logservice/rcservice/ob_role_change_handler.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"     // ObLogRestoreHandler
#include "logservice/ob_log_handler.h"
#include "logservice/restoreservice/ob_log_restore_handler.h"     // ObLogRestoreHandler
#include "storage/ls/ob_ls_member_table.h"
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

namespace oceanbase
{
namespace observer
{
class ObIMetaReport;
}
namespace palf
{
class SCN;
}

namespace storage
{
#define UPDATE_LS_META_CHECK()                                          \
  if (IS_NOT_INIT) {                                                    \
    ret = OB_NOT_INIT;                                                  \
    STORAGE_LOG(WARN, "ls is not inited", K(ret), K(ls_meta_));         \
  } else if (OB_UNLIKELY(is_stopped_)) {                                \
    ret = OB_NOT_RUNNING;                                               \
    STORAGE_LOG(WARN, "ls stopped", K(ret), K_(ls_meta));               \
  } else if (!can_update_ls_meta(ls_meta_.ls_create_status_)) {         \
    ret = OB_STATE_NOT_MATCH;                                           \
    STORAGE_LOG(WARN, "state not match, cannot update ls meta", K(ret), K(ls_meta_)); \
  }

#define UPDATE_LS_META_CHECK_WITH_LOCK()                                \
  int64_t read_lock = LSLOCKLS;                                         \
  int64_t write_lock = 0;                                               \
  ObLSLockGuard lock_myself(lock_, read_lock, write_lock);              \
  UPDATE_LS_META_CHECK();

#define UPDATE_LSMETA_WITH_LOCK(delegate_obj, func_name)          \
  template <typename ...Args>                                     \
  int func_name(Args &&...args) {                                 \
    int ret = OB_SUCCESS;                                         \
    UPDATE_LS_META_CHECK_WITH_LOCK();                                       \
    if (OB_FAIL(ret)) {                                           \
    } else {                                                      \
      ret = delegate_obj.func_name(std::forward<Args>(args)...);  \
    }                                                             \
    return ret;                                                   \
  }

#define UPDATE_LSMETA_WITHOUT_LOCK(delegate_obj, func_name)       \
  template <typename ...Args>                                     \
  int func_name##_without_lock(Args &&...args) {                  \
    int ret = OB_SUCCESS;                                         \
    UPDATE_LS_META_CHECK();                                       \
    if (OB_FAIL(ret)) {                                           \
    } else {                                                      \
      ret = delegate_obj.func_name(std::forward<Args>(args)...);  \
    }                                                             \
    return ret;                                                   \
  }

const static int64_t LS_INNER_TABLET_FROZEN_TIMESTAMP = 1;

struct ObLSVTInfo
{
  share::ObLSID ls_id_;
  ObReplicaType replica_type_;
  common::ObRole ls_state_;
  ObMigrationStatus migrate_status_;
  int64_t tablet_count_;
  palf::SCN weak_read_scn_;
  bool need_rebuild_;
  //TODO SCN
  int64_t checkpoint_ts_;
  //TODO SCN
  int64_t checkpoint_lsn_;
};

class ObIComponentFactory;
enum class ObInnerLSStatus;

class ObLS : public common::ObLink
{
public:
  friend ObLSLockGuard;
public:
  static constexpr int64_t TOTAL_INNER_TABLET_NUM = 3;
  static const uint64_t INNER_TABLET_ID_LIST[TOTAL_INNER_TABLET_NUM];
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
           const ObReplicaType replica_type,
           const ObMigrationStatus &migration_status,
           const share::ObLSRestoreStatus &restore_status,
           const palf::SCN &create_scn,
           observer::ObIMetaReport *reporter);
  // I am ready to work now.
  int start();
  int stop();
  bool safe_to_destroy();
  void destroy();
  int offline();
  int online();

  ObLSTxService *get_tx_svr() { return &ls_tx_svr_; }
  ObLockTable *get_lock_table() { return &lock_table_; }
  ObTxTable *get_tx_table() { return &tx_table_; }
  ObLSWRSHandler *get_ls_wrs_handler() { return &ls_wrs_handler_; }
  ObLSTabletService *get_tablet_svr() { return &ls_tablet_svr_; }
  share::ObLSID get_ls_id() const { return ls_meta_.ls_id_; }
  bool is_sys_ls() const { return ls_meta_.ls_id_.is_sys_ls(); }
  ObReplicaType get_replica_type() const { return ls_meta_.replica_type_; }
  uint64_t get_tenant_id() const { return ls_meta_.tenant_id_; }
  ObFreezer *get_freezer() { return &ls_freezer_; }
  common::ObMultiModRefMgr<ObLSGetMod> &get_ref_mgr() { return ref_mgr_; }
  checkpoint::ObCheckpointExecutor *get_checkpoint_executor() { return &checkpoint_executor_; }
  checkpoint::ObDataCheckpoint *get_data_checkpoint() { return &data_checkpoint_; }
  transaction::ObKeepAliveLSHandler *get_keep_alive_ls_handler() { return &keep_alive_ls_handler_; }
  ObLSRestoreHandler *get_ls_restore_handler() { return &ls_restore_handler_; }
  // ObObLogHandler interface:
  // get the log_service pointer
  logservice::ObLogHandler *get_log_handler() { return &log_handler_; }
  logservice::ObLogRestoreHandler *get_log_restore_handler() { return &restore_handler_; }
  logservice::ObRoleChangeHandler *get_role_change_handler() { return &role_change_handler_;}
  logservice::ObRoleChangeHandler *get_restore_role_change_handler() { return &restore_role_change_handler_;}
  logservice::ObGCHandler *get_gc_handler() { return &gc_handler_; }
  //migration handler
  ObLSMigrationHandler *get_ls_migration_handler() { return &ls_migration_handler_; }

  //remove member handler
  ObLSRemoveMemberHandler *get_ls_remove_member_handler() { return &ls_remove_member_handler_; }

  checkpoint::ObTabletGCHandler *get_tablet_gc_handler() { return &tablet_gc_handler_; }
  // make sure the schema version does not back off.
  int save_base_schema_version();

  // get ls info
  int get_ls_info(ObLSVTInfo &ls_info);
  // report the ls replica info to RS.
  int report_replica_info();

  // set create state of ls.
  // @param[in] new_status, the new create state which will be set.
  void set_create_state(const ObInnerLSStatus new_status);
  ObInnerLSStatus get_create_state();
  bool is_need_gc() const;
  bool is_need_load_inner_tablet() const;
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
                             const palf::SCN &create_scn);
  // load all the inner tablet.
  int load_ls_inner_tablet();

  // get the meta package of ls: ObLSMeta, PalfBaseInfo
  // @param[out] meta_package, the meta package.
  int get_ls_meta_package(ObLSMetaPackage &meta_package);
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
                        const int64_t log_ts,
                        ObTabletHandle &handle) const;

  int flush_if_need(const bool need_flush);

  TO_STRING_KV(K_(ls_meta), K_(log_handler), K_(restore_handler), K_(is_inited), K_(tablet_gc_handler));
private:
  int prepare_for_safe_destroy_();
  int flush_if_need_(const bool need_flush);
  int offline_();
  int offline_log_();
  int online_log_();
  int offline_tx_();
  int online_tx_();
public:
  // ObLSMeta interface:
  int update_id_meta_with_writing_slog(const int64_t service_type,
                                       const int64_t limited_id,
                                       const palf::SCN &latest_scn);
  int update_id_meta_without_writing_slog(const int64_t service_type,
                                          const int64_t limited_id,
                                          const palf::SCN &latest_scn);
  // int set_ls_rebuild();
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, set_ls_rebuild);
  // protect in ls lock
  // int set_gc_state(const logservice::LSGCState &gc_state);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, set_gc_state);
  // int set_clog_checkpoint(const palf::LSN &clog_checkpoint_lsn,
  //                         const palf::SCN &clog_checkpoint_scn,
  //                         const bool write_slog = true);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, set_clog_checkpoint);
  UPDATE_LSMETA_WITHOUT_LOCK(ls_meta_, set_clog_checkpoint);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_clog_checkpoint_ts, int64_t);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_clog_checkpoint_scn, palf::SCN);
  DELEGATE_WITH_RET(ls_meta_, get_clog_base_lsn, palf::LSN &);
  DELEGATE_WITH_RET(ls_meta_, get_saved_info, int);
  // int build_saved_info();
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, build_saved_info);
  // int clear_saved_info_without_lock();
  UPDATE_LSMETA_WITHOUT_LOCK(ls_meta_, clear_saved_info);
  // int update_ls_meta(const bool update_restore_status,
  //                    const ObLSMeta &src_ls_meta);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, update_ls_meta);
  // int update_ls_meta_without_lock(const bool update_restore_status,
  //                                 const ObLSMeta &src_ls_meta);
  UPDATE_LSMETA_WITHOUT_LOCK(ls_meta_, update_ls_meta);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_rebuild_seq, int64_t);
  CONST_DELEGATE_WITH_RET(ls_meta_, get_tablet_change_checkpoint_scn, palf::SCN);
  // set restore status
  // @param [in] restore status.
  // int set_restore_status(const share::ObLSRestoreStatus &status);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, set_restore_status);
  // int set_restore_status_without_lock(const share::ObLSRestoreStatus &status);
  UPDATE_LSMETA_WITHOUT_LOCK(ls_meta_, set_restore_status);
  // get restore status
  // @param [out] restore status.
  // int get_restore_status(share::ObLSRestoreStatus &status);
  DELEGATE_WITH_RET(ls_meta_, get_restore_status, int);
  // set migration status
  // @param [in] migration status.
  // int set_migration_status_without_lock(const ObMigrationStatus &migration_status,
  //                                       const bool write_slog = true);
  UPDATE_LSMETA_WITHOUT_LOCK(ls_meta_, set_migration_status);
  // int set_migration_status(const ObMigrationStatus &migration_status,
  //                          const bool write_slog = true);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, set_migration_status);
  // get migration status
  // @param [out] migration status.
  // int get_migration_status(ObMigrationstatus &status);
  DELEGATE_WITH_RET(ls_meta_, get_migration_status, int);
  // get gc state
  // @param [in] gc state.
  // int get_gc_state(LSGCState &status);
  DELEGATE_WITH_RET(ls_meta_, get_gc_state, int);
  // set offline ts
  // @param [in] offline ts.
  // int set_offline_ts_ns(const int64_t offline_scn);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, set_offline_scn);
  // get offline ts
  // @param [in] offline ts.
  // int get_offline_scn(const SCN &offline_scn);
  DELEGATE_WITH_RET(ls_meta_, get_offline_scn, int);
  // update replayable point
  // @param [in] replayable point.
  // int update_ls_replayable_point(const int64_t replayable_point);
  UPDATE_LSMETA_WITH_LOCK(ls_meta_, update_ls_replayable_point);
  // get replayable point
  // @param [in] replayable point
  // int get_ls_replayable_point(int64_t &replayable_point);
  DELEGATE_WITH_RET(ls_meta_, get_ls_replayable_point, int);
  // set tablet_change_checkpoint_scn, add write lock of LSLOCKLOGMETA.
  // @param [in] scn
  int set_tablet_change_checkpoint_scn(const palf::SCN &scn);
  // get ls_meta_package and unsorted tablet_ids, add read lock of LSLOCKLOGMETA.
  // @param [out] meta_package
  // @param [out] tablet_ids
  int get_ls_meta_package_and_tablet_ids(ObLSMetaPackage &meta_package, common::ObIArray<common::ObTabletID> &tablet_ids);

  // ObLSTabletService interface:
  // create tablets in a ls
  // @param [in] arg, all the create parameters needed.
  // @param [in] is_replay, whether write log or not.
  // int batch_create_tablets(
  //     const obrpc::ObBatchCreateTabletArg &arg,
  //     const bool is_replay = false);
  DELEGATE_WITH_RET(ls_tablet_svr_, batch_create_tablets, int);
  // remove tablets
  // @param [in] arg, all the remove parameters needed.
  // @param [in] is_replay, whether write log or not.
  // int batch_remove_tablets(
  //     const obrpc::ObBatchRemoveTabletArg &arg,
  //     const bool is_replay = false);
  DELEGATE_WITH_RET(ls_tablet_svr_, batch_remove_tablets, int);
  // get a tablet handle
  // @param [in] tablet_id, the tablet needed
  // @param [out] handle, store the tablet and inc ref.
  // @param [in] timeout_us, timeout(mircosecond) for get tablet
  // int get_tablet(
  //     const ObTabletID &tablet_id,
  //     ObTabletHandle &handle,
  //     const int64_t timeout_us);
  DELEGATE_WITH_RET(ls_tablet_svr_, get_tablet, int);
  // get ls tablet iterator
  // @param [out] iterator, ls tablet iterator to iterate all tablets in ls
  // int build_tablet_iter(ObLSTabletIterator &iter);
  // int build_tablet_iter(ObLSTabletIDIterator &iter);
  DELEGATE_WITH_RET(ls_tablet_svr_, build_tablet_iter, int);
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
  //    const common::ObIArray<common::ObTabletID> &tablet_id_array);
  DELEGATE_WITH_RET(ls_tablet_svr_, remove_tablets, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, rebuild_create_tablet, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, update_tablet_ha_data_status, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, update_tablet_restore_status, int);
  DELEGATE_WITH_RET(ls_tablet_svr_, create_or_update_migration_tablet, int);


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
  // @breif, query lsn by timestamp, note that this function may be time-consuming
  // @param[in] const int64_t, specified timestamp(ns).
  // @param[out] LSN&, the lower bound lsn which include timestamp.
  // int locate_by_ts_ns_coarsely(const int64_t ts_ns, LSN &result_lsn, int64_t &result_ts_ns);
  DELEGATE_WITH_RET(log_handler_, locate_by_ts_ns_coarsely, int);
  // advance the base_lsn of log_handler.
  // @param[in] palf_base_info, the palf meta used to advance base lsn.
  // int advance_base_info(const palf::PalfBaseInfo &palf_base_info);
  DELEGATE_WITH_RET(log_handler_, advance_base_info, int);
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
  // @brief, get max decided log ts considering both apply and replay.
  // @param[out] int64_t&, max decided log ts ns.
  DELEGATE_WITH_RET(log_handler_, get_max_decided_log_ts_ns, int);
  // @brief, get max decided log scn considering both apply and replay.
  // @param[out] palf::SCN&, max decided log scn.
  DELEGATE_WITH_RET(log_handler_, get_max_decided_scn, int);
  // @breif, check request server is in self member list
  // @param[in] const common::ObAddr, request server.
  // @param[out] bool&, whether in self member list.
  DELEGATE_WITH_RET(log_handler_, is_valid_member, int);
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
  // @param[in] null.
  // @param[out] null.
  DELEGATE_WITH_RET(log_handler_, disable_vote, int);
  DELEGATE_WITH_RET(log_handler_, add_member, int);
  DELEGATE_WITH_RET(log_handler_, remove_member, int);
  DELEGATE_WITH_RET(log_handler_, remove_learner, int);
  DELEGATE_WITH_RET(log_handler_, replace_member, int);
  DELEGATE_WITH_RET(log_handler_, is_in_sync, int);
  DELEGATE_WITH_RET(log_handler_, get_end_ts_ns, int);
  DELEGATE_WITH_RET(log_handler_, get_end_scn, int);
  DELEGATE_WITH_RET(log_handler_, disable_sync, int);
  DELEGATE_WITH_RET(log_handler_, change_replica_num, int);
  DELEGATE_WITH_RET(log_handler_, get_end_lsn, int);


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

  // ObReplayHandler interface:
  DELEGATE_WITH_RET(replay_handler_, replay, int);

  // ObFreezer interface:
  // logstream freeze
  // @param [in] null
  // int logstream_freeze();
  // DELEGATE_WITH_RET(ls_freezer_, logstream_freeze, int);
  int logstream_freeze();
  // tablet freeze
  // @param [in] tablet_id
  // int tablet_freeze(const ObTabletID &tablet_id);
  // DELEGATE_WITH_RET(ls_freezer_, tablet_freeze, int);
  int tablet_freeze(const ObTabletID &tablet_id);
  // force freeze tablet
  // @param [in] tablet_id
  // int force_tablet_freeze(const ObTabletID &tablet_id);
  // DELEGATE_WITH_RET(ls_freezer_, force_tablet_freeze, int);
  int force_tablet_freeze(const ObTabletID &tablet_id);

  // ObTxTable interface
  DELEGATE_WITH_RET(tx_table_, get_tx_table_guard, int);
  DELEGATE_WITH_RET(tx_table_, get_upper_trans_version_before_given_scn, int);
  DELEGATE_WITH_RET(tx_table_, dump_single_tx_data_2_text, int);

  // ObCheckpointExecutor interface:
  DELEGATE_WITH_RET(checkpoint_executor_, get_checkpoint_info, int);

  // ObDataCheckpoint interface:
  DELEGATE_WITH_RET(data_checkpoint_, get_freezecheckpoint_info, int);

  // update tablet table store here do not using Macro because need lock ls and tablet
  // update table store for tablet
  // @param [in] tablet_id, the tablet id for target tablet
  // @param [in] param, parameters needed to update tablet
  // @param [out] handle, new tablet handle
  int update_tablet_table_store(
      const ObTabletID &tablet_id,
      const ObUpdateTableStoreParam &param,
      ObTabletHandle &handle);
  int build_ha_tablet_new_table_store(
      const ObTabletID &tablet_id,
      const ObBatchUpdateTableStoreParam &param);
  int try_update_uppder_trans_version();

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
  ObLSWRSHandler ls_wrs_handler_;
  //for migration
  ObLSMigrationHandler ls_migration_handler_;
  //for remove member
  ObLSRemoveMemberHandler ls_remove_member_handler_;
  //for rebuild
  ObLSRebuildCbImpl ls_rebuild_cb_impl_;
  // for tablet gc
  checkpoint::ObTabletGCHandler tablet_gc_handler_;
private:
  bool is_inited_;
  uint64_t tenant_id_;
  bool is_stopped_;
  ObLSMeta ls_meta_;
  observer::ObIMetaReport *rs_reporter_;
  ObLSLock lock_;
  common::ObMultiModRefMgr<ObLSGetMod> ref_mgr_;
  logservice::coordinator::ElectionPriorityImpl election_priority_;
};

}
}
#endif

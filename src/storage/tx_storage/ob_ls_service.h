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

#ifndef OCEABASE_STORAGE_LS_SERVICE_
#define OCEABASE_STORAGE_LS_SERVICE_

#include "lib/guard/ob_shared_guard.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"          // ObConcurrentFIFOAllocator
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/ob_storage_rpc.h"
#include "storage/ls/ob_ls_meta_package.h"                      // ObLSMetaPackage
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"
#include "storage/mview/ob_major_mv_merge_info.h"

namespace oceanbase
{
namespace observer
{
class ObIMetaReport;
}
namespace share
{
class ObLSID;
}
namespace blocksstable
{
class ObBaseStorageLogger;
class ObSuperBlockMetaEntry;
}

namespace storage
{
class ObLS;
class ObLSIterator;
class ObLSHandle;
struct ObLSMeta;

// Maintain the tenant <-> log streams mapping relationship
// Support log stream meta persistent and checkpoint
class ObLSService : public ObIResourceLimitCalculatorHandler
{
  static const int64_t DEFAULT_LOCK_TIMEOUT = 60_s;
  static const int64_t SMALL_TENANT_MEMORY_LIMIT = 4 * 1024 * 1024 * 1024L; // 4G
  static const int64_t TENANT_MEMORY_PER_LS_NEED = 200 * 1024 * 1024L; // 200MB
  static const int64_t TENANT_MEMORY_PER_LOGONLY_LS_NEED = 100 * 1024 * 1024L; // 100MB
public:
  int64_t break_point = -1; // just for test
public:
  ObLSService();
  virtual ~ObLSService();

  static int mtl_init(ObLSService* &ls_service);
  int init(const uint64_t tenant_id,
           observer::ObIMetaReport *reporter);
  int start();
  int stop();
  int wait();
  void destroy();
  bool is_empty();
  void inc_ls_safe_destroy_task_cnt();
  void dec_ls_safe_destroy_task_cnt();
  void inc_iter_cnt();
  void dec_iter_cnt();
public:
  // for limit calculator
  virtual int get_current_info(share::ObResourceInfo &info) override;
  virtual int get_resource_constraint_value(share::ObResoureConstraintValue &constraint_value) override;
  virtual int cal_min_phy_resource_needed(share::ObMinPhyResourceResult &min_phy_res) override;
  virtual int cal_min_phy_resource_needed(const int64_t num, share::ObMinPhyResourceResult &min_phy_res) override;
public:
  // create a LS
  // @param [in] arg, all the parameters that is need to create a LS.
  int create_ls(const obrpc::ObCreateLSArg &arg);
  // delete a LS
  // @param [in] ls_id, which LS is to be removed.
  int remove_ls(const share::ObLSID &ls_id);
  // create a LS for HighAvaiable
  // @param [in] meta_package, all the parameters that is needed to create a LS for ha
  int create_ls_for_ha(const share::ObTaskId task_id, const ObMigrationOpArg &arg);

  // create a LS for replay or update LS's meta
  // @param [in] ls_epoch, the epoch increases monotonically in tenant scope when an ls is created
  // @param [in] ls_meta, all the parameters that is needed to create a LS for replay
  int replay_create_ls(const int64_t ls_epoch, const ObLSMeta &ls_meta);
  // replay create ls commit slog.
  // @param [in] ls_id, the create process of which is committed.
  int replay_create_ls_commit(const share::ObLSID &ls_id);
  // create a LS for replay or update LS's meta
  // @param [in] ls_meta, all the parameters that is needed to create a LS for replay
  int replay_update_ls(const ObLSMeta &ls_meta);
  // update LS's meta for restore
  // @param [in] meta_package, all the parameters that is needed to for restore
  int restore_update_ls(const ObLSMetaPackage &meta_package);
  // set a LS to REMOVED state and gc it later.
  // for replay create ls abort or remove
  // @param [in] ls_id, which ls need to be set REMOVED.
  int replay_remove_ls(const share::ObLSID &ls_id);

  // @param [in] ls_id, which ls does we need, mod is the module to get ls
  // @param [out] handle, a guard of the specified logsream.
  int get_ls(const share::ObLSID &ls_id,
             ObLSHandle &handle,
             ObLSGetMod mod);
  int get_ls_replica(
      const ObLSID &ls_id,
      ObLSGetMod mod,
      share::ObLSReplica &replica);
  // @param [in] func, iterate all ls diagnose info
  int iterate_diagnose(const ObFunction<int(const storage::ObLS &ls)> &func);

  // remove the ls that is creating and write abort slog.
  int gc_ls_after_replay_slog();
  // online all ls
  int online_ls();

  // check whether a ls exist or not.
  // @param [in] ls_id, the ls we will check.
  // @param [out] exist, true if the ls exist, else false.
  int check_ls_exist(const share::ObLSID &ls_id,
                     bool &exist);
  // check whether a ls waiting for destroy or not.
  // @param [in] ls_id, the ls we will check.
  // @param [out] waiting, true if the ls waiting for destroy, else false.
  int check_ls_waiting_safe_destroy(const share::ObLSID &ls_id,
                                    bool &waiting);
  // get a log stream iterator.
  // @param [out] guard, the iterator created.
  // use guard just like a pointer of ObLSIterator
  int get_ls_iter(common::ObSharedGuard<ObLSIterator> &guard, ObLSGetMod mod);

  template<class FUNC>
  int foreach_ls(FUNC &func);

  // get all ls ids
  int get_ls_ids(common::ObIArray<share::ObLSID> &ls_id_array);

  // tablet operation in transactions
  // Create tablets for a ls
  // @param [in] tx_desc, trans descriptor
  // @param [in] arg, all the create parameters needed.
  // @param [out] result, the return code and trans result of the op.
  int create_tablets_in_trans(transaction::ObTxDesc &tx_desc,
                              const obrpc::ObBatchCreateTabletArg &batch_arg,
                              obrpc::ObCreateTabletBatchInTransRes &result);

  // remove tablets from a ls
  // @param [in] tx_desc, trans descriptor
  // @param [in] arg, all the remove parameters needed.
  // @param [out] result, the return code of the remove op.
  int remove_tablets_in_trans(transaction::ObTxDesc &tx_desc,
                              const obrpc::ObBatchRemoveTabletArg &batch_arg,
                              obrpc::ObRemoveTabletsInTransRes &result);

  obrpc::ObStorageRpcProxy *get_storage_rpc_proxy() { return &storage_svr_rpc_proxy_; }
  storage::ObStorageRpc *get_storage_rpc() { return &storage_rpc_; }
  ObLSMap *get_ls_map() { return &ls_map_; }
  int64_t get_ls_count() const { return ls_map_.get_ls_count(); }
  int dump_ls_info();

#ifdef OB_BUILD_SHARED_STORAGE
  int check_sslog_ls_exist();
  void report_tablet_id_for_tablet_version_gc(
      const share::ObLSID &ls_id,
      const common::ObTabletID &tablet_id)
  {
    int ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObTabletGCInfo tablet_info(tablet_id);
    if (OB_FAIL(get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
      STORAGE_LOG(WARN, "failed to get ls", K(ret), K(ls_id), K(tablet_id));
    } else {
      ls_handle.get_ls()->get_ls_private_block_gc_handler().report_tablet_id_for_gc_service(tablet_info);
    }
  }
#endif

  TO_STRING_KV(K_(tenant_id), K_(is_inited));
private:
  enum class ObLSCreateState {
      CREATE_STATE_INIT = 0, // begin
      CREATE_STATE_INNER_CREATED = 1, // inner_create_ls_ succ
      CREATE_STATE_ADDED_TO_MAP = 2, // add_ls_to_map_ succ
      CREATE_STATE_WRITE_PREPARE_SLOG = 3, // write_prepare_create_ls_slog_ succ
      CREATE_STATE_PALF_ENABLED = 4, // enable_palf succ
      CREATE_STATE_INNER_TABLET_CREATED = 5, // have created inner tablet
      CREATE_STATE_FINISH
  };
  struct ObCreateLSCommonArg {
    share::ObLSID ls_id_;
    share::SCN create_scn_;
    palf::PalfBaseInfo palf_base_info_;
    ObTenantRole tenant_role_;
    ObReplicaType replica_type_;
    lib::Worker::CompatMode compat_mode_;
    int64_t create_type_;
    ObMigrationStatus migration_status_;
    ObLSRestoreStatus restore_status_;
    share::ObTaskId task_id_;
    bool need_create_inner_tablet_;
    storage::ObMajorMVMergeInfo major_mv_merge_info_;
  };

  int create_ls_(const ObCreateLSCommonArg &arg,
                 const ObMigrationOpArg &mig_arg);
  // the tenant smaller than 5G can only create 8 ls.
  // other tenant can create 100 ls.
  int check_tenant_ls_num_(const bool is_check_for_logonly);
  int inner_create_ls_(const share::ObLSID &lsid,
                       const ObMigrationStatus &migration_status,
                       const share::ObLSRestoreStatus &restore_status,
                       const share::SCN &create_scn,
                       const ObMajorMVMergeInfo &major_mv_merge_info,
                       const ObLSStoreFormat &store_format,
                       const ObReplicaType &replica_type,
                       ObLS *&ls);
  int inner_del_ls_(ObLS *&ls);
  int add_ls_to_map_(ObLS *ls);
  int remove_ls_from_map_(const share::ObLSID &ls_id);
  void remove_ls_(ObLS *ls, const bool remove_from_disk, const bool write_slog);
  int safe_remove_ls_(ObLSHandle handle, const bool remove_from_disk);
  int restore_update_ls_(const ObLSMetaPackage &meta_package);
  int replay_remove_ls_(const share::ObLSID &ls_id);
  int replay_create_ls_(const int64_t ls_epoch, const ObLSMeta &ls_meta);
  int replay_update_ls_(const ObLSMeta &ls_meta);
  int post_create_ls_(const int64_t create_type,
                      ObLS *&ls);
  void del_ls_after_create_ls_failed_(ObLSCreateState& ls_create_state, ObLS *ls);

  int alloc_ls_(ObLS *&ls);
  bool is_ls_to_restore_(const obrpc::ObCreateLSArg &arg) const;
  bool is_ls_to_clone_(const obrpc::ObCreateLSArg &arg) const;
  bool need_create_inner_tablets_(const obrpc::ObCreateLSArg &arg) const;
  int get_restore_status_(
      share::ObLSRestoreStatus &restore_status);
  ObLSRestoreStatus get_restore_status_by_tenant_role_(const ObTenantRole& tenant_role);
  int64_t get_create_type_by_tenant_role_(const ObTenantRole& tenant_role);

  // for resource limit calculator
  int cal_min_phy_resource_needed_(const int64_t ls_cnt,
                                   ObMinPhyResourceResult &min_phy_res);
  int get_resource_constraint_value_(ObResoureConstraintValue &constraint_value,
                                     const bool is_check_for_logonly = false);
  // for get_ls_replica
  int get_replica_type_(
      const common::ObAddr &addr,
      const ObMemberList &ob_member_list,
      const GlobalLearnerList &learner_list,
      const common::ObLSStoreFormat &ls_store_format,
      const ObLSMeta &ls_meta,
      ObReplicaType &replica_type);

private:
  bool is_inited_;
  bool is_running_; // used by create/remove, only can be used after start and before stop.
  bool is_stopped_; // only for ls iter, get ls iter will cause OB_NOT_RUNNING after stop.
  uint64_t tenant_id_;
  // a map from ls id to ls
  ObLSMap ls_map_;

  common::ObConcurrentFIFOAllocator ls_allocator_;
  common::ObConcurrentFIFOAllocator iter_allocator_;
  // protect the create and remove process
  lib::ObMutex change_lock_;
  observer::ObIMetaReport *rs_reporter_;

  //TOD(muwei.ym) src rpc framework should be tenant level
  obrpc::ObStorageRpcProxy storage_svr_rpc_proxy_;
  storage::ObStorageRpc storage_rpc_;

  // for safe destroy
  // store the ls is removing
  int64_t safe_ls_destroy_task_cnt_;

  // record the count of ls iter
  int64_t iter_cnt_;

  // for limit calculator
  // the max ls cnt after observer start
  int64_t max_ls_cnt_;
  DISALLOW_COPY_AND_ASSIGN(ObLSService);
};

template <class FUNC>
int ObLSService::foreach_ls(FUNC &func)
{
  int ret = OB_SUCCESS;

  ObLSIterator *iter = NULL;
  common::ObSharedGuard<ObLSIterator> guard;
  if (OB_FAIL(get_ls_iter(guard, ObLSGetMod::TXSTORAGE_MOD))) {
    STORAGE_LOG(WARN, "get log stream iter failed", K(ret));
  } else if (OB_ISNULL(iter = guard.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "iter is NULL", K(ret));
  } else {
    ObLS *ls = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          STORAGE_LOG(WARN, "iter next ls failed", KR(ret), KP(this));
        }
      } else if (OB_FAIL(func(*ls))) {
        STORAGE_LOG(WARN, "do function on ls failed", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase
#endif

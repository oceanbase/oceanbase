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

#ifndef OCEABASE_STORAGE_TABLET_GROUP_RESTORE_
#define OCEABASE_STORAGE_TABLET_GROUP_RESTORE_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "observer/ob_rpc_processor_simple.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_restore_struct.h"
#include "ob_physical_copy_task.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_tablet_builder.h"

//ObTabletGroupRestoreDagNet is a tablet group restore dag net
//ObInitialTabletGroupRestoreDag is the dag net first dag. It creates two dags :
//  ObStartTabletGroupRestoreDag and ObFinishTabletGroupRestoreDag
//ObStartTabletGroupRestoreDag create some ObTabletRestoreDag which in restore tablet id list
//ObFinishTabletGroupRestoreDag is the end dag when dag net is running success or finally failed.

namespace oceanbase
{
namespace storage
{
enum class ObTabletGroupRestoreDagType : int64_t
{
  INITIAL_TABLET_GROUP_RESTORE_DAG = 0,
  START_TABLET_GROUP_RESTORE_DAG = 1,
  TABLET_GROUP_RESTORE_DAG = 2,
  FINISH_TABELT_GROUP_RESTORE_DAG = 3,
  MAX_TYPE,
};

struct ObTabletGroupRestoreCtx : public ObIHADagNetCtx
{
public:
  ObTabletGroupRestoreCtx();
  virtual ~ObTabletGroupRestoreCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::TABLET_GROUP_RESTORE; }
  virtual bool is_valid() const;

public:
  ObTabletGroupRestoreArg arg_;
  int64_t start_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  ObStorageHASrcInfo src_;
  ObStorageHATableInfoMgr ha_table_info_mgr_;
  ObArray<common::ObTabletID> tablet_id_array_;
  ObHATabletGroupCtx tablet_group_ctx_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;

  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(arg),
      K_(start_ts),
      K_(finish_ts),
      K_(task_id),
      K_(src),
      K_(tablet_id_array));
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupRestoreCtx);
};

struct ObTabletRestoreCtx final: public ObICopyTabletCtx
{
public:
  ObTabletRestoreCtx();
  virtual ~ObTabletRestoreCtx();
  bool is_valid() const;
  void reset();
  int set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status) override;
  int get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const override;
  VIRTUAL_TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), KPC_(restore_base_info), K_(is_leader),
      K_(action), KP_(meta_index_store), KP_(second_meta_index_store), K_(replica_type), KP_(ha_table_info_mgr), K_(status));

public:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObTabletHandle tablet_handle_;
  const ObRestoreBaseInfo *restore_base_info_;
  bool is_leader_;
  ObTabletRestoreAction::ACTION action_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObReplicaType replica_type_;
  ObStorageHATableInfoMgr *ha_table_info_mgr_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
private:
  common::SpinRWLock lock_;
  ObCopyTabletStatus::STATUS status_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletRestoreCtx);
};

struct ObTGRDagNetInitParam: public share::ObIDagInitParam
{
public:
  ObTGRDagNetInitParam();
  virtual ~ObTGRDagNetInitParam() {}
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id));
  ObTabletGroupRestoreArg arg_;
  share::ObTaskId task_id_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
};

class ObTabletGroupRestoreDagNet: public share::ObIDagNet
{
public:
  ObTabletGroupRestoreDagNet();
  virtual ~ObTabletGroupRestoreDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override
  {
    return OB_NOT_NULL(ctx_) && ctx_->is_valid();
  }
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;
  bool is_ha_dag_net() const override { return true; }

  ObTabletGroupRestoreCtx *get_restore_ctx() { return ctx_; }
  common::ObInOutBandwidthThrottle *get_bandwidth_throttle() { return bandwidth_throttle_; }
  obrpc::ObStorageRpcProxy *get_storage_rpc_proxy() { return svr_rpc_proxy_; }
  storage::ObStorageRpc *get_storage_rpc() { return storage_rpc_; }
  backup::ObBackupMetaIndexStoreWrapper *get_meta_index_store() { return ctx_->arg_.is_leader_ ? &meta_index_store_ : nullptr; }
  backup::ObBackupMetaIndexStoreWrapper *get_second_meta_index_store() { return ctx_->arg_.is_leader_ ? &second_meta_index_store_ : nullptr; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));
private:
  int start_running_for_restore_();
  int alloc_restore_ctx_();
  void free_restore_ctx_();
  int change_member_list_();
  int generate_migration_init_dag_();
  int report_result_();
private:
  bool is_inited_;
  ObTabletGroupRestoreCtx *ctx_;
  backup::ObBackupMetaIndexStoreWrapper meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper second_meta_index_store_;
  backup::ObBackupIndexKVCache *kv_cache_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupRestoreDagNet);
};

class ObTabletGroupRestoreDag : public ObStorageHADag
{
public:
  explicit ObTabletGroupRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObTabletGroupRestoreDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  ObTabletGroupRestoreCtx *get_ctx() const { return static_cast<ObTabletGroupRestoreCtx *>(ha_dag_net_ctx_); }

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupRestoreDag);
};

class ObInitialTabletGroupRestoreDag : public ObTabletGroupRestoreDag
{
public:
  ObInitialTabletGroupRestoreDag();
  virtual ~ObInitialTabletGroupRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObTabletGroupRestoreDag", ObTabletGroupRestoreDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialTabletGroupRestoreDag);
};

class ObInitialTabletGroupRestoreTask : public share::ObITask
{
public:
  ObInitialTabletGroupRestoreTask();
  virtual ~ObInitialTabletGroupRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialTabletGroupRestoreDag"), KP(this), KPC(ctx_));
private:
  int check_local_ls_restore_status_();
  int check_local_tablets_restore_status_();
  int choose_src_();
  int choose_leader_src_();
  int choose_follower_src_();
  int generate_tablet_restore_dags_();
  int renew_tablets_meta_();
  int init_ha_tablets_builder_();
  int build_tablet_group_ctx_();
  int record_server_event_();

private:
  bool is_inited_;
  ObTabletGroupRestoreCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDagNet *dag_net_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObLSHandle ls_handle_;
  ObStorageHATabletsBuilder ha_tablets_builder_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialTabletGroupRestoreTask);
};

class ObStartTabletGroupRestoreDag : public ObTabletGroupRestoreDag
{
public:
  ObStartTabletGroupRestoreDag();
  virtual ~ObStartTabletGroupRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net, share::ObIDag *finish_dag);
  INHERIT_TO_STRING_KV("ObTabletGroupRestoreDag", ObTabletGroupRestoreDag, KP(this));
protected:
  bool is_inited_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObStartTabletGroupRestoreDag );
};

class ObStartTabletGroupRestoreTask : public share::ObITask
{
public:
  ObStartTabletGroupRestoreTask();
  virtual ~ObStartTabletGroupRestoreTask();
  int init(
      share::ObIDag *finish_dag);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartTabletGroupRestoreTask"), KP(this), KPC(ctx_));
private:
  int create_tablets_sstable_();
  int generate_tablet_restore_dag_();
  int record_server_event_();
  int try_update_local_tablets_();

private:
  bool is_inited_;
  ObTabletGroupRestoreCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  share::ObIDag *finish_dag_;
  ObLSHandle ls_handle_;
  ObStorageHATabletsBuilder ha_tablets_builder_;
  DISALLOW_COPY_AND_ASSIGN(ObStartTabletGroupRestoreTask);
};

class ObFinishTabletGroupRestoreDag : public ObTabletGroupRestoreDag
{
public:
  ObFinishTabletGroupRestoreDag();
  virtual ~ObFinishTabletGroupRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObTabletGroupRestoreDag", ObTabletGroupRestoreDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishTabletGroupRestoreDag);
};

class ObFinishTabletGroupRestoreTask : public share::ObITask
{
public:
  ObFinishTabletGroupRestoreTask();
  virtual ~ObFinishTabletGroupRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishTabletGroupRestoreTask"), KP(this), KPC(ctx_));
private:
  int generate_restore_init_dag_();
  int record_server_event_();
private:
  bool is_inited_;
  ObTabletGroupRestoreCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishTabletGroupRestoreTask);
};

struct ObInitTabletRestoreParam final
{
  ObInitTabletRestoreParam();
  ~ObInitTabletRestoreParam();
  void reset();
  bool is_valid() const;

  TO_STRING_KV(
      K_(tenant_id),
      K_(ls_id),
      K_(tablet_id),
      KPC_(ha_dag_net_ctx),
      K_(action),
      KPC_(restore_base_info),
      KP_(second_meta_index_store),
      KP_(ha_table_info_mgr),
      KP_(tablet_group_ctx),
      K_(need_check_seq),
      K_(ls_rebuild_seq));

  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  bool is_leader_;
  ObTabletRestoreAction::ACTION action_;
  const ObRestoreBaseInfo *restore_base_info_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObStorageHATableInfoMgr *ha_table_info_mgr_;
  ObHATabletGroupCtx *tablet_group_ctx_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  DISALLOW_COPY_AND_ASSIGN(ObInitTabletRestoreParam);
};

class ObTabletRestoreDag : public ObStorageHADag
{
public:
  ObTabletRestoreDag();
  virtual ~ObTabletRestoreDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  virtual int inner_reset_status_for_retry() override;
  virtual int generate_next_dag(share::ObIDag *&dag);

  int init(const ObInitTabletRestoreParam &param);
  ObInOutBandwidthThrottle *get_bandwidth_throttle() { return bandwidth_throttle_; }
  obrpc::ObStorageRpcProxy *get_storage_rpc_proxy() { return svr_rpc_proxy_; }
  storage::ObStorageRpc *get_storage_rpc() { return storage_rpc_; }
  storage::ObLS *get_ls() { return ls_handle_.get_ls(); }

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this), K_(tablet_restore_ctx));
protected:
  int get_dag_net_task_id_(share::ObTaskId &task_id) const;

protected:
  bool is_inited_;
  ObTabletRestoreCtx tablet_restore_ctx_;
  //TODO(muwei.ym) put this into dag net ctx 4.3
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  ObLSHandle ls_handle_;
  ObHATabletGroupCtx *tablet_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletRestoreDag);
};

class ObTabletRestoreTask : public share::ObITask
{
public:
  ObTabletRestoreTask();
  virtual ~ObTabletRestoreTask();
  int init(ObTabletRestoreCtx &ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletRestoreTask"), KP(this), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
private:
  typedef bool (*IsRightTypeSSTableFunc)(const ObITable::TableType table_type);
  int generate_restore_tasks_();
  int generate_minor_restore_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_major_restore_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_ddl_restore_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_physical_restore_task_(
      const ObITable::TableKey &copy_table_key,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      ObITask *parent_task,
      ObITask *child_task);
  int generate_finish_restore_task_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      ObITask *parent_task);
  int get_src_info_();
  int generate_restore_task_(
      IsRightTypeSSTableFunc is_right_type_sstable,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int build_copy_table_key_info_();
  int build_copy_sstable_info_mgr_();
  int generate_tablet_copy_finish_task_(
      ObTabletCopyFinishTask *&tablet_copy_finish_task);
  int try_update_tablet_();
  int update_ha_status_(const ObCopyTabletStatus::STATUS &status);
  int check_need_copy_sstable_(
      const ObITable::TableKey &table_key,
      bool &need_copy);
  int record_server_event_();
  int check_src_sstable_exist_();

private:
  bool is_inited_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  ObTabletRestoreCtx *tablet_restore_ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  storage::ObLS *ls_;
  ObStorageHASrcInfo src_info_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;
  common::ObArray<ObITable::TableKey> copy_table_key_array_;
  ObStorageHACopySSTableInfoMgr copy_sstable_info_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletRestoreTask);
};

class ObTabletFinishRestoreTask : public share::ObITask
{
public:
  ObTabletFinishRestoreTask();
  virtual ~ObTabletFinishRestoreTask();
  int init(ObTabletRestoreCtx &ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletRestoreTask"), KP(this), KPC(ha_dag_net_ctx_), KPC(tablet_restore_ctx_));
private:
  int update_restore_status_();
  int record_server_event_();

private:
  bool is_inited_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  ObTabletRestoreCtx *tablet_restore_ctx_;
  storage::ObLS *ls_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishRestoreTask);
};

struct ObTabletGroupRestoreUtils
{
  static int init_ha_tablets_builder(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      const bool is_leader_restore,
      const bool need_check_seq,
      const int64_t ls_rebuild_seq,
      const ObStorageHASrcInfo src_info,
      ObLS *ls,
      const ObRestoreBaseInfo *restore_base_info,
      const ObTabletRestoreAction::ACTION &restore_action,
      backup::ObBackupMetaIndexStoreWrapper *meta_index_store,
      ObStorageHATableInfoMgr *ha_table_info_mgr,
      ObStorageHATabletsBuilder &ha_tablets_builder);
};


}
}
#endif

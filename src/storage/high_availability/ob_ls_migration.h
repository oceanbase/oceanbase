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

#ifndef OCEABASE_STORAGE_LS_MIGRATION_
#define OCEABASE_STORAGE_LS_MIGRATION_

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "observer/ob_rpc_processor_simple.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/scn.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "ob_physical_copy_task.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_tablet_builder.h"

namespace oceanbase
{
namespace storage
{

struct ObMigrationCtx :  public ObIHADagNetCtx
{
public:
  ObMigrationCtx();
  virtual ~ObMigrationCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::LS_MIGRATION; }
  virtual bool is_valid() const;

  void reset();
  void reuse();
public:
  typedef hash::ObHashMap<common::ObTabletID, ObCopyTabletSimpleInfo> CopyTabletSimpleInfoMap;
public:
  uint64_t tenant_id_;
  ObMigrationOpArg arg_;
  share::SCN local_clog_checkpoint_scn_;
  int64_t local_rebuild_seq_;

  int64_t start_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  ObStorageHASrcInfo minor_src_;
  ObStorageHASrcInfo major_src_;
  ObLSMetaPackage src_ls_meta_package_;
  ObArray<common::ObTabletID> sys_tablet_id_array_;
  ObArray<common::ObTabletID> data_tablet_id_array_;
  ObStorageHATableInfoMgr ha_table_info_mgr_;
  ObHATabletGroupMgr tablet_group_mgr_;
  int64_t check_tablet_info_cost_time_;
  CopyTabletSimpleInfoMap tablet_simple_info_map_;

  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(arg),
      K_(local_clog_checkpoint_scn),
      K_(local_rebuild_seq),
      K_(start_ts),
      K_(finish_ts),
      K_(task_id),
      K_(minor_src),
      K_(major_src),
      K_(src_ls_meta_package),
      K_(check_tablet_info_cost_time));

  DISALLOW_COPY_AND_ASSIGN(ObMigrationCtx);
};

struct ObCopyTabletCtx final: public ObICopyTabletCtx
{
public:
  ObCopyTabletCtx();
  virtual ~ObCopyTabletCtx();
  bool is_valid() const;
  void reset();
  int set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status) override;
  int get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const override;
  VIRTUAL_TO_STRING_KV(K_(tablet_id), K_(status));

public:
  common::ObTabletID tablet_id_;
  ObTabletHandle tablet_handle_;
private:
  common::SpinRWLock lock_;
  ObCopyTabletStatus::STATUS status_;
  DISALLOW_COPY_AND_ASSIGN(ObCopyTabletCtx);
};

struct ObMigrationDagNetInitParam: public share::ObIDagInitParam
{
public:
  ObMigrationDagNetInitParam();
  virtual ~ObMigrationDagNetInitParam() {}
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id));
  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
};

class ObMigrationDagNet: public share::ObIDagNet
{
public:
  ObMigrationDagNet();
  virtual ~ObMigrationDagNet();
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

  ObMigrationCtx *get_migration_ctx() { return ctx_; }
  common::ObInOutBandwidthThrottle *get_bandwidth_throttle() { return bandwidth_throttle_; }
  obrpc::ObStorageRpcProxy *get_storage_rpc_proxy() { return svr_rpc_proxy_; }
  storage::ObStorageRpc *get_storage_rpc() { return storage_rpc_; }
  common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));
private:
  int start_running_for_migration_();
  int alloc_migration_ctx_();
  void free_migration_ctx_();
  int generate_migration_init_dag_();

private:
  bool is_inited_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationDagNet);
};

class ObMigrationDag : public ObStorageHADag
{
public:
  explicit ObMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObMigrationDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  ObMigrationCtx *get_migration_ctx() const { return static_cast<ObMigrationCtx *>(ha_dag_net_ctx_); }

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
  DISALLOW_COPY_AND_ASSIGN(ObMigrationDag);
};

class ObInitialMigrationDag : public ObMigrationDag
{
public:
  ObInitialMigrationDag();
  virtual ~ObInitialMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObMigrationDag", ObMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialMigrationDag);
};

class ObInitialMigrationTask : public share::ObITask
{
public:
  ObInitialMigrationTask();
  virtual ~ObInitialMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialMigrationTask"), KP(this), KPC(ctx_));
private:
  int generate_migration_dags_();
  int record_server_event_();
private:
  bool is_inited_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialMigrationTask);
};

class ObStartMigrationDag : public ObMigrationDag
{
public:
  ObStartMigrationDag();
  virtual ~ObStartMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObMigrationDag", ObMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStartMigrationDag);
};

class ObStartMigrationTask : public share::ObITask
{
public:
  ObStartMigrationTask();
  virtual ~ObStartMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartMigrationTask"), KP(this), KPC(ctx_));
private:
  int deal_with_local_ls_();
  int update_ls_();
  int generate_tablets_migration_dag_();
  int report_ls_meta_table_();
  int choose_src_();
  int fill_restore_arg_if_needed_();
  int fetch_ls_info_(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const common::ObAddr &member_addr, obrpc::ObCopyLSInfo &ls_info);
  int get_local_ls_checkpoint_scn_(share::SCN &local_checkpoint_scn);
  int try_remove_member_list_();
  int get_tablet_id_array_(common::ObIArray<common::ObTabletID> &tablet_id_array);
  int check_ls_need_copy_data_(bool &need_copy);
  int check_before_ls_migrate_(const ObLSMeta &ls_meta);
  int build_ls_();
  int inner_build_ls_();
  int create_all_tablets_(ObCopyLSViewInfoObReader *ob_reader);
  int inner_build_ls_with_old_rpc_();
  int create_all_tablets_with_4_1_rpc_();

  int record_server_event_();

private:
  bool is_inited_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObStartMigrationTask);
};

class ObSysTabletsMigrationDag : public ObMigrationDag
{
public:
  ObSysTabletsMigrationDag();
  virtual ~ObSysTabletsMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObMigrationDag", ObMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTabletsMigrationDag);
};

class ObSysTabletsMigrationTask : public share::ObITask
{
public:
  ObSysTabletsMigrationTask();
  virtual ~ObSysTabletsMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSysTabletsMigrationTask"), KP(this), KPC(ctx_));
private:
  int build_tablets_sstable_info_();
  int generate_sys_tablet_migartion_dag_();
  int record_server_event_();

private:
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  ObStorageHATabletsBuilder ha_tablets_builder_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTabletsMigrationTask);
};

class ObTabletMigrationDag : public ObMigrationDag
{
public:
  ObTabletMigrationDag();
  virtual ~ObTabletMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int inner_reset_status_for_retry() override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(
      const common::ObTabletID &tablet_id,
      ObTabletHandle &tablet_handle,
      share::ObIDagNet *dag_net,
      ObHATabletGroupCtx *tablet_group_ctx = nullptr);
  int get_ls(ObLS *&ls);
  INHERIT_TO_STRING_KV("ObIMigrationDag", ObMigrationDag, KP(this), K(copy_tablet_ctx_));

protected:
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObCopyTabletCtx copy_tablet_ctx_;
  ObHATabletGroupCtx *tablet_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletMigrationDag);
};

class ObTabletFinishMigrationTask;
class ObTabletMigrationTask : public share::ObITask
{
public:
  ObTabletMigrationTask();
  virtual ~ObTabletMigrationTask();
  int init(ObCopyTabletCtx &ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletMigrationTask"), KP(this), KPC(ctx_));
private:
  typedef bool (*IsRightTypeSSTableFunc)(const ObITable::TableType table_type);
  int generate_migration_tasks_();
  int generate_minor_copy_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_major_copy_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_ddl_copy_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_copy_tasks_(
      IsRightTypeSSTableFunc is_right_type_sstable,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);
  int generate_physical_copy_task_(
      const ObStorageHASrcInfo &src_info,
      const ObITable::TableKey &copy_table_key,
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      ObITask *parent_task,
      ObITask *child_task);
  int generate_tablet_finish_migration_task_(
      ObTabletFinishMigrationTask *&tablet_finish_migration_task);
  int build_copy_table_key_info_();
  int build_copy_sstable_info_mgr_();
  int generate_tablet_copy_finish_task_(
      ObTabletCopyFinishTask *&tablet_copy_finish_task);
  int record_server_event_(const int64_t cost_us, const int64_t result);
  int try_update_tablet_();
  int check_tablet_replica_validity_(const common::ObTabletID &tablet_id);
  int update_ha_expected_status_(const ObCopyTabletStatus::STATUS &status);
  int check_need_copy_sstable_(
      const ObITable::TableKey &table_key,
      bool &need_copy);
  int check_transfer_seq_equal_(
      const ObMigrationTabletParam *src_tablet_meta);
  int generate_mds_copy_tasks_(
      ObTabletCopyFinishTask *tablet_copy_finish_task,
      share::ObITask *&parent_task);

private:
  bool is_inited_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  ObCopyTabletCtx *copy_tablet_ctx_;
  common::ObArray<ObITable::TableKey> copy_table_key_array_;
  ObStorageHACopySSTableInfoMgr copy_sstable_info_mgr_;

  DISALLOW_COPY_AND_ASSIGN(ObTabletMigrationTask);
};

class ObTabletFinishMigrationTask final : public share::ObITask
{
public:
  ObTabletFinishMigrationTask();
  virtual ~ObTabletFinishMigrationTask();
  int init(const int64_t task_gen_time, const int64_t copy_table_count,
      ObCopyTabletCtx &ctx, ObLS &ls);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletFinishMigrationTask"), KP(this), KPC(ha_dag_net_ctx_), KPC(copy_tablet_ctx_), KPC(ls_));
private:
  int update_data_and_expected_status_();
private:
  bool is_inited_;
  int64_t task_gen_time_;
  int64_t copy_table_count_;
  ObIHADagNetCtx *ha_dag_net_ctx_;
  ObCopyTabletCtx *copy_tablet_ctx_;
  ObLS *ls_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletFinishMigrationTask);
};

class ObDataTabletsMigrationDag : public ObMigrationDag
{
public:
  ObDataTabletsMigrationDag();
  virtual ~ObDataTabletsMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObMigrationDag", ObMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDataTabletsMigrationDag);
};

class ObTabletGroupMigrationDag;
class ObDataTabletsMigrationTask : public share::ObITask
{
public:
  ObDataTabletsMigrationTask();
  virtual ~ObDataTabletsMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObDataTabletsMigrationTask"), KP(this), KPC(ctx_));
private:
  int join_learner_list_();
  int ls_online_();
  int generate_tablet_group_migration_dag_();
  int generate_tablet_group_dag_(
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      common::ObIArray<ObTabletGroupMigrationDag *> &tablet_group_dag_array);
  int build_tablet_group_info_();
  int generate_tablet_group_dag_();
  int try_remove_unneeded_tablets_();
  int try_offline_ls_();
  int record_server_event_();

private:
  static const int64_t MAX_TABLET_GROUP_SIZE = 2 * 1024L * 1024L * 1024L; //2G
  static const int64_t MAX_TABLET_COUNT = 100;
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDag *finish_dag_;
  ObStorageHATabletsBuilder ha_tablets_builder_;
  DISALLOW_COPY_AND_ASSIGN(ObDataTabletsMigrationTask);
};

class ObTabletGroupMigrationDag : public ObMigrationDag
{
public:
  ObTabletGroupMigrationDag();
  virtual ~ObTabletGroupMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      share::ObIDagNet *dag_net,
      share::ObIDag *finish_dag,
      ObHATabletGroupCtx *tablet_group_ctx);

  INHERIT_TO_STRING_KV("ObIMigrationDag", ObMigrationDag, KP(this));
protected:
  bool is_inited_;
  ObArray<common::ObTabletID> tablet_id_array_;
  share::ObIDag *finish_dag_;
  ObHATabletGroupCtx *tablet_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupMigrationDag);
};

class ObTabletGroupMigrationTask : public share::ObITask
{
public:
  ObTabletGroupMigrationTask();
  virtual ~ObTabletGroupMigrationTask();
  int init(
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      share::ObIDag *finish_dag,
      ObHATabletGroupCtx *tablet_group_ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletGroupMigrationTask"), KP(this), KPC(ctx_));
private:
  int build_tablets_sstable_info_();
  int generate_tablet_migration_dag_();
  int try_remove_tablets_info_();
  int remove_tablets_info_();
  int record_server_event_();

private:
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObMigrationCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObArray<common::ObTabletID> tablet_id_array_;
  share::ObIDag *finish_dag_;
  ObStorageHATabletsBuilder ha_tablets_builder_;
  ObHATabletGroupCtx *tablet_group_ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupMigrationTask);
};

class ObMigrationFinishDag : public ObMigrationDag
{
public:
  ObMigrationFinishDag();
  virtual ~ObMigrationFinishDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObMigrationDag", ObMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationFinishDag);
};

class ObMigrationFinishTask : public share::ObITask
{
public:
  ObMigrationFinishTask();
  virtual ~ObMigrationFinishTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObMigrationFinishTask"), KP(this), KPC(ctx_));
private:
  int generate_migration_init_dag_();
  int record_server_event_();
private:
  bool is_inited_;
  ObMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObMigrationFinishTask);
};

struct ObLSMigrationUtils
{
  static int init_ha_tablets_builder(
      const uint64_t tenant_id,
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      const ObStorageHASrcInfo src_info,
      const int64_t local_rebuild_seq,
      const ObMigrationOpType::TYPE &type,
      ObLS *ls,
      ObStorageHATableInfoMgr *ha_table_info_mgr,
      ObStorageHATabletsBuilder &ha_tablets_builder);

};


}
}
#endif

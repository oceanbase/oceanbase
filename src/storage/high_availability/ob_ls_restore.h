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

#ifndef OCEABASE_STORAGE_LS_RETORE_
#define OCEABASE_STORAGE_LS_RETORE_

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
#include "ob_tablet_group_restore.h"
#include "ob_storage_ha_dag.h"
#include "ob_storage_ha_tablet_builder.h"

namespace oceanbase
{
namespace storage
{
struct ObLSRestoreCtx : public ObIHADagNetCtx
{
public:
  ObLSRestoreCtx();
  virtual ~ObLSRestoreCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::LS_RESTORE; }
  virtual bool is_valid() const;

public:
  ObLSRestoreArg arg_;
  int64_t start_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  ObStorageHASrcInfo src_;
  ObLSMetaPackage src_ls_meta_package_;
  ObArray<common::ObTabletID> sys_tablet_id_array_;
  ObArray<common::ObTabletID> data_tablet_id_array_;
  ObStorageHATableInfoMgr ha_table_info_mgr_;
  ObHATabletGroupMgr tablet_group_mgr_;
  bool need_check_seq_;
  int64_t ls_rebuild_seq_;

  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(arg),
      K_(start_ts),
      K_(finish_ts),
      K_(task_id),
      K_(src),
      K_(src_ls_meta_package));
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreCtx);
};

class ObLSRestoreDagNetInitParam: public share::ObIDagInitParam
{
public:
  ObLSRestoreDagNetInitParam();
  virtual ~ObLSRestoreDagNetInitParam() {}
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id));
  ObLSRestoreArg arg_;
  share::ObTaskId task_id_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
};

class ObLSRestoreDagNet: public share::ObIDagNet
{
public:
  ObLSRestoreDagNet();
  virtual ~ObLSRestoreDagNet();
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

  ObLSRestoreCtx *get_ls_restore_ctx() { return ctx_; }
  common::ObInOutBandwidthThrottle *get_bandwidth_throttle() { return bandwidth_throttle_; }
  obrpc::ObStorageRpcProxy *get_storage_rpc_proxy() { return svr_rpc_proxy_; }
  storage::ObStorageRpc *get_storage_rpc() { return storage_rpc_; }
  backup::ObBackupMetaIndexStoreWrapper *get_meta_index_store() { return ctx_->arg_.is_leader_ ? &meta_index_store_ : nullptr; }
  backup::ObBackupMetaIndexStoreWrapper *get_second_meta_index_store() { return ctx_->arg_.is_leader_ ? &second_meta_index_store_ : nullptr; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));
private:
  int start_running_for_ls_restore_();
  int alloc_ls_restore_ctx_();
  void free_ls_restore_ctx_();
  int generate_migration_init_dag_();
  int report_result_();
private:
  bool is_inited_;
  ObLSRestoreCtx *ctx_;
  backup::ObBackupMetaIndexStoreWrapper meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper second_meta_index_store_;
  backup::ObBackupIndexKVCache *kv_cache_;
  //TODO(muwei.ym) put bandwidth_throttle, svr_rpc_proxy, and storage_rpc into ctx 4.3
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreDagNet);
};

class ObLSRestoreDag : public ObStorageHADag
{
public:
  explicit ObLSRestoreDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObLSRestoreDag();
  ObLSRestoreCtx *get_ctx() const { return static_cast<ObLSRestoreCtx *>(ha_dag_net_ctx_); }
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this))
  DISALLOW_COPY_AND_ASSIGN(ObLSRestoreDag);
};

class ObInitialLSRestoreDag : public ObLSRestoreDag
{
public:
  ObInitialLSRestoreDag();
  virtual ~ObInitialLSRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObLSRestoreDag", ObLSRestoreDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialLSRestoreDag);
};

class ObInitialLSRestoreTask : public share::ObITask
{
public:
  ObInitialLSRestoreTask();
  virtual ~ObInitialLSRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialLSRestoreTask"), KP(this), KPC(ctx_));
private:
  int generate_ls_restore_dags_();
private:
  bool is_inited_;
  ObLSRestoreCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialLSRestoreTask);
};

class ObStartLSRestoreDag : public ObLSRestoreDag
{
public:
  ObStartLSRestoreDag();
  virtual ~ObStartLSRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObLSRestoreDag", ObLSRestoreDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStartLSRestoreDag);
};

class ObICopyLSViewInfoReader;
class ObStartLSRestoreTask : public share::ObITask
{
public:
  ObStartLSRestoreTask();
  virtual ~ObStartLSRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartLSRestoreTask"), KP(this), KPC(ctx_));
private:
  int deal_with_local_ls_();
  int update_ls_meta_();
  int generate_tablets_restore_dag_();

  int update_ls_meta_and_create_all_tablets_();
  int create_tablet_(
      const ObMigrationTabletParam &tablet_meta,
      ObLS *ls);
  void set_tablet_to_restore(ObMigrationTabletParam &tablet_meta);
  int alloc_copy_ls_view_reader_(ObICopyLSViewInfoReader *&reader);
  void free_copy_ls_view_reader_(ObICopyLSViewInfoReader *&reader);
  int generate_tablet_id_array_(
      const ObIArray<common::ObTabletID> &tablet_id_array);
private:
  bool is_inited_;
  ObLSRestoreCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  DISALLOW_COPY_AND_ASSIGN(ObStartLSRestoreTask);
};

class ObSysTabletsRestoreDag : public ObLSRestoreDag
{
public:
  ObSysTabletsRestoreDag();
  virtual ~ObSysTabletsRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObLSRestoreDag", ObLSRestoreDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTabletsRestoreDag);
};

class ObSysTabletsRestoreTask : public share::ObITask
{
public:
  ObSysTabletsRestoreTask();
  virtual ~ObSysTabletsRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObSysTabletsRestoreTask"), KP(this), KPC(ctx_));
private:
  int create_or_update_tablets_();
  int build_tablets_sstable_info_();
  int generate_sys_tablet_restore_dag_();

private:
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObLSRestoreCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  backup::ObBackupMetaIndexStoreWrapper *meta_index_store_;
  backup::ObBackupMetaIndexStoreWrapper *second_meta_index_store_;
  ObStorageHATabletsBuilder ha_tablets_builder_;
  DISALLOW_COPY_AND_ASSIGN(ObSysTabletsRestoreTask);
};

class ObDataTabletsMetaRestoreDag : public ObLSRestoreDag
{
public:
  ObDataTabletsMetaRestoreDag();
  virtual ~ObDataTabletsMetaRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObLSRestoreDag", ObLSRestoreDag, KP(this), KPC(ha_dag_net_ctx_));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObDataTabletsMetaRestoreDag);
};

class ObTabletGroupMetaRestoreDag;
class ObDataTabletsMetaRestoreTask : public share::ObITask
{
public:
  ObDataTabletsMetaRestoreTask();
  virtual ~ObDataTabletsMetaRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObDataTabletsRestoreTask"), KP(this), KPC(ctx_));
private:
  int build_tablet_group_info_();
  int generate_tablet_group_dag_();

private:
  bool is_inited_;
  ObLSRestoreCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDag *finish_dag_;

  DISALLOW_COPY_AND_ASSIGN(ObDataTabletsMetaRestoreTask);
};

class ObTabletGroupMetaRestoreDag : public ObLSRestoreDag
{
public:
  ObTabletGroupMetaRestoreDag();
  virtual ~ObTabletGroupMetaRestoreDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;

  int init(
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      share::ObIDagNet *dag_net,
      share::ObIDag *finish_dag);
  INHERIT_TO_STRING_KV("ObLSRestoreDag", ObLSRestoreDag, KP(this), KPC(ha_dag_net_ctx_));
protected:
  bool is_inited_;
  ObArray<ObTabletID> tablet_id_array_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupMetaRestoreDag);
};

class ObTabletGroupMetaRestoreTask : public share::ObITask
{
public:
  ObTabletGroupMetaRestoreTask();
  virtual ~ObTabletGroupMetaRestoreTask();
  int init(
      const common::ObIArray<common::ObTabletID> &tablet_id_array,
      share::ObIDag *finish_dag);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletGroupMetaRestoreTask"), KP(this), KPC(ctx_));
private:
  int create_or_update_tablets_();
  int create_or_update_tablet_(
      const common::ObTabletID &tablet_id,
      storage::ObLS *ls);

private:
  bool is_inited_;
  ObLSRestoreCtx *ctx_;
  common::ObArray<common::ObTabletID> tablet_id_array_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletGroupMetaRestoreTask);
};

class ObFinishLSRestoreDag : public ObLSRestoreDag
{
public:
  ObFinishLSRestoreDag();
  virtual ~ObFinishLSRestoreDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObLSRestoreDag", ObLSRestoreDag, KP(this), KPC(ha_dag_net_ctx_));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishLSRestoreDag);
};

class ObFinishLSRestoreTask : public share::ObITask
{
public:
  ObFinishLSRestoreTask();
  virtual ~ObFinishLSRestoreTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObLSRestoreFinishTask"), KP(this), KPC(ctx_));
private:
  int generate_initial_ls_restore_dag_();
private:
  bool is_inited_;
  ObLSRestoreCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishLSRestoreTask);
};


}
}
#endif

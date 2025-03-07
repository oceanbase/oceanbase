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

#ifndef OCEABASE_STORAGE_HIGH_AVAILABILITY_OB_HA_REBUILD_TABLET_H
#define OCEABASE_STORAGE_HIGH_AVAILABILITY_OB_HA_REBUILD_TABLET_H

#include "lib/thread/ob_work_queue.h"
#include "lib/thread/ob_dynamic_thread_pool.h"
#include "share/ob_common_rpc_proxy.h" // ObCommonRpcProxy
#include "share/ob_srv_rpc_proxy.h" // ObPartitionServiceRpcProxy
#include "share/scheduler/ob_sys_task_stat.h"
#include "observer/ob_rpc_processor_simple.h"
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

struct ObRebuildTabletCtx :  public ObIHADagNetCtx
{
public:
  ObRebuildTabletCtx();
  virtual ~ObRebuildTabletCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::REBUILD_TABLET; }
  virtual bool is_valid() const;
  void reset();
  void reuse();
public:
  uint64_t tenant_id_;
  ObMigrationOpArg arg_;
  int64_t start_ts_;
  int64_t finish_ts_;
  share::ObTaskId task_id_;
  ObStorageHASrcInfo src_;
  ObHATabletGroupCtx tablet_group_ctx_;
  int64_t src_ls_rebuild_seq_;

  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(tenant_id),
      K_(arg),
      K_(start_ts),
      K_(finish_ts),
      K_(task_id),
      K_(src),
      K_(src_ls_rebuild_seq));

  DISALLOW_COPY_AND_ASSIGN(ObRebuildTabletCtx);
};

struct ObRebuildTabletCopyCtx final: public ObICopyTabletCtx
{
public:
  ObRebuildTabletCopyCtx();
  virtual ~ObRebuildTabletCopyCtx();
  bool is_valid() const;
  void reset();
  void reuse();
  int set_copy_tablet_status(const ObCopyTabletStatus::STATUS &status) override;
  int get_copy_tablet_status(ObCopyTabletStatus::STATUS &status) const override;
  int get_copy_tablet_record_extra_info(ObCopyTabletRecordExtraInfo *&extra_info) override;
  VIRTUAL_TO_STRING_KV(K_(tablet_id), K_(status), K_(sstable_info), K_(copy_header));

public:
  common::ObTabletID tablet_id_;
  ObTabletHandle tablet_handle_;
  obrpc::ObCopyTabletSSTableInfo sstable_info_;
  obrpc::ObCopyTabletSSTableHeader copy_header_;
  ObCopyTabletRecordExtraInfo extra_info_;
private:
  common::SpinRWLock lock_;
  ObCopyTabletStatus::STATUS status_;
  DISALLOW_COPY_AND_ASSIGN(ObRebuildTabletCopyCtx);
};

struct ObRebuildTabletDagNetInitParam: public share::ObIDagInitParam
{
public:
  ObRebuildTabletDagNetInitParam();
  virtual ~ObRebuildTabletDagNetInitParam() {}
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id));
  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
};

class ObRebuildTabletDagNet: public share::ObIDagNet
{
public:
  ObRebuildTabletDagNet();
  virtual ~ObRebuildTabletDagNet();
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

  ObRebuildTabletCtx *get_rebuild_tablet_ctx() { return ctx_; }
  common::ObInOutBandwidthThrottle *get_bandwidth_throttle() { return bandwidth_throttle_; }
  obrpc::ObStorageRpcProxy *get_storage_rpc_proxy() { return svr_rpc_proxy_; }
  storage::ObStorageRpc *get_storage_rpc() { return storage_rpc_; }
  common::ObMySQLProxy *get_sql_proxy() { return sql_proxy_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, KPC_(ctx));
private:
  int start_running_for_rebuild_tablet_();
  int alloc_rebuild_tablet_ctx_();
  void free_rebuild_tablet_ctx_();
  int generate_rebuild_tablet_init_dag_();

private:
  bool is_inited_;
  ObRebuildTabletCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  DISALLOW_COPY_AND_ASSIGN(ObRebuildTabletDagNet);
};

class ObRebuildTabletDag : public ObStorageHADag
{
public:
  explicit ObRebuildTabletDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObRebuildTabletDag();
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  ObRebuildTabletCtx *get_rebuild_tablet_ctx() const { return static_cast<ObRebuildTabletCtx *>(ha_dag_net_ctx_); }

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
  DISALLOW_COPY_AND_ASSIGN(ObRebuildTabletDag);
};

class ObInitialRebuildTabletDag : public ObRebuildTabletDag
{
public:
  ObInitialRebuildTabletDag();
  virtual ~ObInitialRebuildTabletDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObRebuildTabletDag", ObRebuildTabletDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialRebuildTabletDag);
};

class ObInitialRebuildTabletTask : public share::ObITask
{
public:
  ObInitialRebuildTabletTask();
  virtual ~ObInitialRebuildTabletTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialRebuildTabletTask"), KP(this), KPC(ctx_));
private:
  int generate_rebuild_tablet_dags_();
  int build_tablet_group_ctx_();
  int check_tablet_status_();
private:
  bool is_inited_;
  ObRebuildTabletCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDagNet *dag_net_;
  ObArray<ObLogicTabletID> tablet_id_array_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialRebuildTabletTask);
};

class ObStartRebuildTabletDag : public ObRebuildTabletDag
{
public:
  ObStartRebuildTabletDag();
  virtual ~ObStartRebuildTabletDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net,
      share::ObIDag *finish_dag);
  INHERIT_TO_STRING_KV("ObRebuildTabletDag", ObRebuildTabletDag, KP(this));
protected:
  bool is_inited_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObStartRebuildTabletDag);
};

class ObStartRebuildTabletTask : public share::ObITask
{
public:
  ObStartRebuildTabletTask();
  virtual ~ObStartRebuildTabletTask();
  int init(share::ObIDag *finish_dag);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartRebuildTabletTask"), KP(this), KPC(ctx_));
private:
  int generate_rebuild_tablets_dag_();
  int get_src_ls_rebuild_seq_();
  int record_server_event_();

private:
  bool is_inited_;
  ObRebuildTabletCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObStartRebuildTabletTask);
};

class ObTabletRebuildMajorDag : public ObRebuildTabletDag
{
public:
  ObTabletRebuildMajorDag();
  virtual ~ObTabletRebuildMajorDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  virtual int inner_reset_status_for_retry() override;
  virtual int generate_next_dag(share::ObIDag *&dag);
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(
      const common::ObTabletID &tablet_id,
      share::ObIDagNet *dag_net,
      share::ObIDag *finish_dag);
  int get_ls(ObLS *&ls);
  INHERIT_TO_STRING_KV("ObRebuildTabletDag", ObRebuildTabletDag, KP(this));

protected:
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObRebuildTabletCopyCtx copy_tablet_ctx_;
  share::ObIDag *finish_dag_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletRebuildMajorDag);
};

class ObTabletFinishMigrationTask;
class ObTabletRebuildMajorTask : public share::ObITask
{
public:
  ObTabletRebuildMajorTask();
  virtual ~ObTabletRebuildMajorTask();
  int init(ObRebuildTabletCopyCtx &ctx);
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObTabletRebuildTask"), KP(this), KPC(ctx_));
private:
  typedef bool (*IsRightTypeSSTableFunc)(const ObITable::TableType table_type);
  int generate_tablet_rebuild_tasks_();
  int generate_major_copy_tasks_(
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

private:
  bool is_inited_;
  ObRebuildTabletCtx *ctx_;
  common::ObInOutBandwidthThrottle *bandwidth_throttle_;
  obrpc::ObStorageRpcProxy *svr_rpc_proxy_;
  storage::ObStorageRpc *storage_rpc_;
  common::ObMySQLProxy *sql_proxy_;
  ObRebuildTabletCopyCtx *copy_tablet_ctx_;
  common::ObArray<ObITable::TableKey> copy_table_key_array_;
  ObStorageHACopySSTableInfoMgr copy_sstable_info_mgr_;
  DISALLOW_COPY_AND_ASSIGN(ObTabletRebuildMajorTask);
};

class ObFinishRebuildTabletDag : public ObRebuildTabletDag
{
public:
  ObFinishRebuildTabletDag();
  virtual ~ObFinishRebuildTabletDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;

  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObRebuildTabletDag", ObRebuildTabletDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishRebuildTabletDag);
};

class ObFinishRebuildTabletTask : public share::ObITask
{
public:
  ObFinishRebuildTabletTask();
  virtual ~ObFinishRebuildTabletTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishRebuildTabletTask"), KP(this), KPC(ctx_));
private:
  int generate_rebuild_tablet_init_dag_();
  int record_server_event_();
private:
  bool is_inited_;
  ObRebuildTabletCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishRebuildTabletTask);
};


}
}
#endif

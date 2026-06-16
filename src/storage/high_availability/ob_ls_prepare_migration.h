/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_LS_PREPARE_MIGRATION
#define OCEABASE_STORAGE_LS_PREPARE_MIGRATION

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_tablet_backfill_tx.h"
#include "ob_storage_ha_dag.h"

namespace oceanbase
{
namespace storage
{

struct ObLSPrepareMigrationCtx : public ObIHADagNetCtx
{
public:
  ObLSPrepareMigrationCtx();
  virtual ~ObLSPrepareMigrationCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual bool is_valid() const;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::LS_PREPARE_MIGRATION; }
public:
  uint64_t tenant_id_;
  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;

  int64_t start_ts_;
  int64_t finish_ts_;
  share::SCN log_sync_scn_;
  ObArray<common::ObTabletID> tablet_id_array_;

  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(tenant_id),
      K_(arg),
      K_(task_id),
      K_(start_ts),
      K_(finish_ts),
      K_(log_sync_scn),
      K_(tablet_id_array));
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSPrepareMigrationCtx);
};

struct ObLSPrepareMigrationParam: public share::ObIDagInitParam
{
public:
  ObLSPrepareMigrationParam();
  virtual ~ObLSPrepareMigrationParam() {}
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id));
  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;
};

class ObLSPrepareMigrationDagNet: public share::ObIDagNet
{
public:
  ObLSPrepareMigrationDagNet();
  virtual ~ObLSPrepareMigrationDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;

  ObLSPrepareMigrationCtx *get_ctx() { return &ctx_; }
  virtual share::ObLSID get_ls_id() const override { return ctx_.arg_.ls_id_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));
private:
  int start_running_for_migration_();

private:
  bool is_inited_;
  ObLSPrepareMigrationCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSPrepareMigrationDagNet);
};

class ObPrepareMigrationDag : public ObStorageHADag
{
public:
  explicit ObPrepareMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObPrepareMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual uint64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int init(share::ObIDagNet *dag_net);
  int prepare_ctx(share::ObIDagNet *dag_net);

  INHERIT_TO_STRING_KV("ObStorageHADag", ObStorageHADag, KP(this));
protected:
  ObLSPrepareMigrationCtx *get_ctx() const { return static_cast<ObLSPrepareMigrationCtx *>(ha_dag_net_ctx_); }
  int fill_dag_key_helper_(const char *dag_name, char *buf, const int64_t buf_len) const;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObPrepareMigrationDag);
};

class ObInitialPrepareMigrationTask : public share::ObITask
{
public:
  ObInitialPrepareMigrationTask();
  virtual ~ObInitialPrepareMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialPrepareMigrationTask"), KP(this), KPC(ctx_));
private:
  int generate_migration_dags_();
private:
  bool is_inited_;
  ObLSPrepareMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialPrepareMigrationTask);
};

class ObStartPrepareMigrationTask : public share::ObITask
{
public:
  ObStartPrepareMigrationTask();
  virtual ~ObStartPrepareMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartPrepareMigrationTask"), KP(this), KPC(ctx_));

private:
  int deal_with_local_ls_();
  int wait_transfer_tablets_ready_();
  int wait_transfer_out_tablet_ready_(
      ObLS *ls, ObTablet *tablet);

private:
  bool is_inited_;
  ObLSPrepareMigrationCtx *ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObStartPrepareMigrationTask);
};

class ObFinishPrepareMigrationTask : public share::ObITask
{
public:
  ObFinishPrepareMigrationTask();
  virtual ~ObFinishPrepareMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishPrepareMigrationTask"), KP(this), KPC(ctx_));
private:
  int generate_prepare_initial_dag_();
private:
  bool is_inited_;
  ObLSPrepareMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishPrepareMigrationTask);
};

#define DEFINE_PREPARE_MIGRATION_DAG(DAG_NAME, DAG_TYPE, TASK_NAME)                                   \
  class DAG_NAME : public ObPrepareMigrationDag {                                   \
  public:                                                                      \
    DAG_NAME() : ObPrepareMigrationDag(DAG_TYPE) {} \
    virtual ~DAG_NAME() = default; \
    virtual int fill_dag_key(char *buf, const int64_t buf_len) const override { return fill_dag_key_helper_("DAG_NAME", buf, buf_len); } \
    virtual int create_first_task() override { TASK_NAME *task = nullptr; return create_task(nullptr, task); } \
    INHERIT_TO_STRING_KV("DAG_NAME", ObPrepareMigrationDag, KP(this)); \
  };

DEFINE_PREPARE_MIGRATION_DAG(ObInitialPrepareMigrationDag, ObDagType::DAG_TYPE_INITIAL_PREPARE_MIGRATION, ObInitialPrepareMigrationTask);
DEFINE_PREPARE_MIGRATION_DAG(ObStartPrepareMigrationDag, ObDagType::DAG_TYPE_START_PREPARE_MIGRATION, ObStartPrepareMigrationTask);
DEFINE_PREPARE_MIGRATION_DAG(ObFinishPrepareMigrationDag, ObDagType::DAG_TYPE_FINISH_PREPARE_MIGRATION, ObFinishPrepareMigrationTask);

}
}
#endif

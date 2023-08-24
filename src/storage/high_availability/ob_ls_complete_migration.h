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

#ifndef OCEABASE_STORAGE_LS_COMPLETE_MIGRATION
#define OCEABASE_STORAGE_LS_COMPLETE_MIGRATION

#include "share/scheduler/ob_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "ob_storage_ha_dag.h"
#include "logservice/palf/lsn.h"

namespace oceanbase
{
namespace storage
{

struct ObLSCompleteMigrationCtx : public ObIHADagNetCtx
{
public:
  ObLSCompleteMigrationCtx();
  virtual ~ObLSCompleteMigrationCtx();
  void reset();
  void reuse();
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual DagNetCtxType get_dag_net_ctx_type() { return ObIHADagNetCtx::LS_COMPLETE_MIGRATION; }
  virtual bool is_valid() const;
public:
  uint64_t tenant_id_;
  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;

  int64_t start_ts_;
  int64_t finish_ts_;
  int64_t rebuild_seq_;

  INHERIT_TO_STRING_KV(
      "ObIHADagNetCtx", ObIHADagNetCtx,
      K_(tenant_id),
      K_(arg),
      K_(task_id),
      K_(start_ts),
      K_(finish_ts),
      K_(rebuild_seq));
private:
  DISALLOW_COPY_AND_ASSIGN(ObLSCompleteMigrationCtx);
};

struct ObLSCompleteMigrationParam: public share::ObIDagInitParam
{
public:
  ObLSCompleteMigrationParam();
  virtual ~ObLSCompleteMigrationParam() {}
  virtual bool is_valid() const override;
  void reset();

  VIRTUAL_TO_STRING_KV(K_(arg), K_(task_id), K_(result), K_(rebuild_seq));
  ObMigrationOpArg arg_;
  share::ObTaskId task_id_;
  int32_t result_;
  int64_t rebuild_seq_;
};

class ObLSCompleteMigrationDagNet: public share::ObIDagNet
{
public:
  ObLSCompleteMigrationDagNet();
  virtual ~ObLSCompleteMigrationDagNet();
  virtual int init_by_param(const share::ObIDagInitParam *param) override;

  virtual bool is_valid() const override;
  virtual int start_running() override;
  virtual bool operator == (const share::ObIDagNet &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_comment(char *buf, const int64_t buf_len) const override;
  virtual int fill_dag_net_key(char *buf, const int64_t buf_len) const override;
  virtual int clear_dag_net_ctx() override;
  virtual int deal_with_cancel() override;
  bool is_ha_dag_net() const override { return true; }

  ObLSCompleteMigrationCtx *get_ctx() { return &ctx_; }
  const share::ObLSID &get_ls_id() const { return ctx_.arg_.ls_id_; }
  INHERIT_TO_STRING_KV("ObIDagNet", share::ObIDagNet, K_(ctx));
private:
  int start_running_for_migration_();
  int update_migration_status_(ObLS *ls);
  int report_ls_meta_table_(ObLS *ls);
  int report_result_();
  int trans_rebuild_fail_status_(
      ObLS &ls,
      const ObMigrationStatus &current_migration_status,
      ObMigrationStatus &new_migration_status);

private:
  bool is_inited_;
  ObLSCompleteMigrationCtx ctx_;
  DISALLOW_COPY_AND_ASSIGN(ObLSCompleteMigrationDagNet);
};

class ObCompleteMigrationDag : public ObStorageHADag
{
public:
  explicit ObCompleteMigrationDag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObCompleteMigrationDag();
  virtual bool operator == (const share::ObIDag &other) const override;
  virtual int64_t hash() const override;
  virtual int fill_info_param(compaction::ObIBasicInfoParam *&out_param, ObIAllocator &allocator) const override;
  int prepare_ctx(share::ObIDagNet *dag_net);

  INHERIT_TO_STRING_KV("ObIDag", ObStorageHADag, KP(this));
protected:
  DISALLOW_COPY_AND_ASSIGN(ObCompleteMigrationDag);
};

class ObInitialCompleteMigrationDag : public ObCompleteMigrationDag
{
public:
  ObInitialCompleteMigrationDag();
  virtual ~ObInitialCompleteMigrationDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteMigrationDag", ObCompleteMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialCompleteMigrationDag);
};

class ObInitialCompleteMigrationTask : public share::ObITask
{
public:
  ObInitialCompleteMigrationTask();
  virtual ~ObInitialCompleteMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObInitialCompleteMigrationTask"), KP(this), KPC(ctx_));
private:
  int generate_migration_dags_();
  int record_server_event_();
private:
  bool is_inited_;
  ObLSCompleteMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObInitialCompleteMigrationTask);
};

class ObStartCompleteMigrationDag : public ObCompleteMigrationDag
{
public:
  ObStartCompleteMigrationDag();
  virtual ~ObStartCompleteMigrationDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteMigrationDag", ObCompleteMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObStartCompleteMigrationDag);
};

class ObStartCompleteMigrationTask : public share::ObITask
{
public:
  ObStartCompleteMigrationTask();
  virtual ~ObStartCompleteMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObStartCompleteMigrationTask"), KP(this), KPC(ctx_));
private:
  int get_wait_timeout_(int64_t &timeout);
  int wait_log_sync_();
  int wait_log_replay_sync_();
  int wait_transfer_table_replace_();
  int change_member_list_with_retry_();
  int change_member_list_();
  int get_ls_transfer_scn_(
      ObLS *ls,
      share::SCN &transfer_scn);
  int switch_learner_to_acceptor_(ObLS *ls);
  int replace_member_with_learner_(ObLS *ls);
  int replace_learners_for_add_(ObLS *ls);
  int replace_learners_for_migration_(ObLS *ls);
  int check_need_wait_(
      ObLS *ls,
      bool &need_wait);
  int update_ls_migration_status_wait_();
  int update_ls_migration_status_hold_();
  int check_all_tablet_ready_();
  int check_tablet_ready_(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      const int64_t timeout);
  int check_tablet_transfer_table_ready_(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      const int64_t timeout);
  int inner_check_tablet_transfer_table_ready_(
      const common::ObTabletID &tablet_id,
      ObLS *ls,
      bool &need_skip);
  int check_need_wait_transfer_table_replace_(
      ObLS *ls,
      bool &need_wait);
  int wait_log_replay_to_max_minor_end_scn_();
  int check_ls_and_task_status_(
      ObLS *ls);
  int record_server_event_();
  int init_timeout_ctx_(
      const int64_t timeout,
      ObTimeoutCtx &timeout_ctx);

private:
  static const int64_t IS_REPLAY_DONE_THRESHOLD_US = 3L * 1000 * 1000L;
  static const int64_t CHECK_CONDITION_INTERVAL = 200_ms;
  bool is_inited_;
  ObLSHandle ls_handle_;
  ObLSCompleteMigrationCtx *ctx_;
  palf::LSN log_sync_lsn_;
  share::SCN max_minor_end_scn_;
  DISALLOW_COPY_AND_ASSIGN(ObStartCompleteMigrationTask);
};

class ObFinishCompleteMigrationDag : public ObCompleteMigrationDag
{
public:
  ObFinishCompleteMigrationDag();
  virtual ~ObFinishCompleteMigrationDag();
  virtual int fill_dag_key(char *buf, const int64_t buf_len) const override;
  virtual int create_first_task() override;
  int init(share::ObIDagNet *dag_net);
  INHERIT_TO_STRING_KV("ObCompleteMigrationDag", ObCompleteMigrationDag, KP(this));
protected:
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishCompleteMigrationDag);
};

class ObFinishCompleteMigrationTask : public share::ObITask
{
public:
  ObFinishCompleteMigrationTask();
  virtual ~ObFinishCompleteMigrationTask();
  int init();
  virtual int process() override;
  VIRTUAL_TO_STRING_KV(K("ObFinishCompleteMigrationTask"), KP(this), KPC(ctx_));
private:
  int generate_prepare_initial_dag_();

  int record_server_event_();
private:
  bool is_inited_;
  ObLSCompleteMigrationCtx *ctx_;
  share::ObIDagNet *dag_net_;
  DISALLOW_COPY_AND_ASSIGN(ObFinishCompleteMigrationTask);
};

}
}
#endif

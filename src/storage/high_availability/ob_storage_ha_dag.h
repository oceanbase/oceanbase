/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_HA_DAG
#define OCEABASE_STORAGE_HA_DAG

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ob_storage_rpc.h"
#include "ob_storage_ha_struct.h"
#include "ob_storage_restore_struct.h"

namespace oceanbase
{
namespace storage
{

struct ObStorageHAResultMgr final
{
public:
  ObStorageHAResultMgr();
  ~ObStorageHAResultMgr();
  int get_result(int32_t &result);
  int set_result(const int32_t result, const bool allow_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  int check_allow_retry(bool &allow_retry);
  void reuse();
  void reset();
  void reset_result();
  int get_retry_count(int32_t &retry_count);
  int get_first_failed_task_id(share::ObTaskId &task_id);
  TO_STRING_KV(K_(result), K_(retry_count), K_(allow_retry), K_(failed_task_id_list));

private:
  static const int64_t MAX_RETRY_CNT = 3;
  common::SpinRWLock lock_;
  int32_t result_;
  int32_t retry_count_;
  bool allow_retry_;
  common::ObSEArray<share::ObTaskId, MAX_RETRY_CNT> failed_task_id_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObStorageHAResultMgr);
};

struct ObIHADagNetCtx
{
public:
  enum DagNetCtxType { //FARM COMPAT WHITELIST
    LS_PREPARE_MIGRATION = 0,
    LS_MIGRATION = 1,
    LS_COMPLETE_MIGRATION = 2,
    LS_RESTORE = 3,
    TABLET_GROUP_RESTORE = 4,
    BACKFILL_TX = 5,
    TRANSFER_BACKFILL_TX = 6,
    REBUILD_TABLET = 7,
    MAX
  };

  ObIHADagNetCtx();
  virtual ~ObIHADagNetCtx();
  virtual int fill_comment(char *buf, const int64_t buf_len) const = 0;
  virtual DagNetCtxType get_dag_net_ctx_type() = 0;
  virtual bool is_valid() const = 0;
  int set_result(const int32_t result, const bool need_retry,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  bool is_failed() const;
  virtual int check_allow_retry(bool &allow_retry);
  int get_result(int32_t &result);
  void reuse();
  void reset();
  void reset_result();
  int check_is_in_retry(bool &is_in_retry);
  int get_retry_count(int32_t &retry_count);
  int get_first_failed_task_id(share::ObTaskId &task_id);

  VIRTUAL_TO_STRING_KV(K("ObIHADagNetCtx"), K_(result_mgr));
private:
  ObStorageHAResultMgr result_mgr_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObIHADagNetCtx);
};

class ObStorageHADag : public share::ObIDag
{
public:
  explicit ObStorageHADag(const share::ObDagType::ObDagTypeEnum &dag_type);
  virtual ~ObStorageHADag();
  virtual int inner_reset_status_for_retry();
  virtual bool inner_check_can_retry();
  int check_is_in_retry(bool &is_in_retry);

  int set_result(const int32_t result, const bool allow_retry = true,
      const enum share::ObDagType::ObDagTypeEnum type = ObDagType::DAG_TYPE_MAX);
  virtual int report_result();
  virtual lib::Worker::CompatMode get_compat_mode() const override
  { return compat_mode_; }
  virtual uint64_t get_consumer_group_id() const override
  { return consumer_group_id_; }
  ObIHADagNetCtx *get_ha_dag_net_ctx() const { return ha_dag_net_ctx_; }
  virtual bool is_ha_dag() const override { return true; }
  bool is_failed() const { return result_mgr_.is_failed(); }

  INHERIT_TO_STRING_KV("ObIDag", ObIDag, KPC_(ha_dag_net_ctx), K_(result_mgr));
protected:
  ObIHADagNetCtx *ha_dag_net_ctx_;
  ObStorageHAResultMgr result_mgr_;
  lib::Worker::CompatMode compat_mode_;
  DISALLOW_COPY_AND_ASSIGN(ObStorageHADag);
};

class ObHATabletGroupCtx
{
public:
  enum class TabletGroupCtxType
  {
    NORMAL_TYPE     = 0,
    CS_REPLICA_TYPE = 1,
    MAX_TYPE
  };
public:
  explicit ObHATabletGroupCtx(const TabletGroupCtxType type);
  virtual ~ObHATabletGroupCtx();
  int init(const common::ObIArray<ObLogicTabletID> &tablet_id_array);
  int get_next_tablet_id(ObLogicTabletID &logic_tablet_id);
  int get_all_tablet_ids(common::ObIArray<ObLogicTabletID> &tablet_id);
  bool is_cs_replica_ctx() const;
public:
  virtual void reuse();
  virtual void inner_reuse();
  virtual int inner_init() { return OB_SUCCESS; }
  TO_STRING_KV(K_(tablet_id_array), K_(index));
protected:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObArray<ObLogicTabletID> tablet_id_array_;
  int64_t index_;
  TabletGroupCtxType type_;
  DISALLOW_COPY_AND_ASSIGN(ObHATabletGroupCtx);
};

struct ObMigrationCtx;
class ObStorageHADagUtils
{
public:
  static int deal_with_fo(
      const int err,
      share::ObIDag *dag,
      const bool allow_retry = true);
  static int get_ls(
      const share::ObLSID &ls_id,
      ObLSHandle &ls_handle);
  static int check_self_is_valid_member(
      const share::ObLSID &ls_id,
      bool &is_valid_member);
  static int check_self_is_valid_member_after_inc_config_version(
      const share::ObLSID &ls_id,
      const bool with_leader,
      bool &is_valid_member);
  static int inc_member_list_config_version(
      const share::ObLSID &ls_id,
      const bool with_leader);
  static int get_migration_src_info(
      const ObMigrationOpArg &arg,
      const uint64_t tenant_id,
      const share::SCN &local_clog_checkpoint_scn,
      storage::ObStorageRpc *storage_rpc,
      ObStorageHASrcInfo &src_info);
  static int deal_with_non_migrated_tablet(
      const ObLSHandle &ls_handle,
      const ObLogicTabletID &logic_tablet_id,
      ObHATabletGroupCtx *tablet_group_ctx,
      ObMigrationCtx *ctx,
      ObTabletHandle &tablet_handle,
      bool &need_migrate);

  // Alloc DAG with priority, init it with forwarded args, insert it between parent_dag
  // and child_dag in the DAG dependency chain, and add it to the tenant DAG scheduler (MTL).
  // On failure the DAG is freed and the pointer is set to nullptr.
  template <typename DAG, typename... Args>
  static int alloc_and_schedule_dag(
      share::ObIDagNet *dag_net,
      share::ObIDag *parent_dag,
      share::ObIDag *child_dag,
      const share::ObDagPrio::ObDagPrioEnum prio,
      const bool emergency,
      DAG *&dag,
      Args&&... init_args);

  // Alloc a standalone DAG (no parent, no child), init it with forwarded args, and add it
  // to the tenant DAG scheduler (MTL).
  template <typename DAG, typename... Args>
  static int alloc_and_schedule_single_dag(
      share::ObIDagNet *dag_net,
      const share::ObDagPrio::ObDagPrioEnum prio,
      const bool emergency,
      DAG *&dag,
      Args&&... init_args);

  // Alloc two DAGs (first_dag, second_dag) with priority, init both with forwarded args,
  // set up the chain: parent_dag -> first_dag -> second_dag, create first tasks for both,
  // and schedule them (second_dag first, then first_dag) with proper error handling.
  // On failure both DAGs are freed and pointers set to nullptr.
  template <typename FIRST_DAG, typename SECOND_DAG, typename... Args>
  static int alloc_and_schedule_pair_dags(
      const share::ObDagPrio::ObDagPrioEnum prio,
      share::ObIDag *parent_dag,
      FIRST_DAG *&first_dag,
      SECOND_DAG *&second_dag,
      Args&&... init_args);

  // Alloc a task from dag, init it with forwarded args, set up parent-child chain
  // (parent_task -> new_task -> child_task), and add it to the dag.
  // parent_task and child_task can be nullptr (skipped if null).
  template <typename TaskType, typename... Args>
  static int alloc_and_add_task(
      share::ObIDag *dag,
      share::ObITask *parent_task,
      share::ObITask *child_task,
      TaskType *&task,
      Args&&... init_args);

  // Alloc a standalone task from dag (no parent, no child), init it with
  // forwarded args, and add it to the dag.
  template <typename TaskType, typename... Args>
  static int alloc_and_add_single_task(
      share::ObIDag *dag,
      TaskType *&task,
      Args&&... init_args);

private:
  static int inner_check_self_is_valid_member_(
      const share::ObLSID &ls_id,
      const common::ObMemberList &member_list,
      const common::GlobalLearnerList &learner_list,
      bool &is_valid_member);
};



class ObHATabletGroupMgr
{
public:
  ObHATabletGroupMgr();
  virtual ~ObHATabletGroupMgr();
  int init();
  int get_next_tablet_group_ctx(
      ObHATabletGroupCtx *&tablet_group_ctx);
  int build_tablet_group_ctx(
      const common::ObIArray<ObLogicTabletID> &tablet_id_array,
      const ObHATabletGroupCtx::TabletGroupCtxType type = ObHATabletGroupCtx::TabletGroupCtxType::NORMAL_TYPE);
  int alloc_and_new_tablet_group_ctx(
      const ObHATabletGroupCtx::TabletGroupCtxType type,
      ObHATabletGroupCtx *&tablet_group_ctx);
  void reuse();
  int64_t get_tablet_group_ctx_count() const;
  int get_tablet_group_ctx(const int64_t idx, ObHATabletGroupCtx *&tablet_group_ctx);

  TO_STRING_KV(K_(tablet_group_ctx_array), K_(index));
private:
  bool is_inited_;
  common::SpinRWLock lock_;
  ObArenaAllocator allocator_;
  ObArray<ObHATabletGroupCtx *> tablet_group_ctx_array_;
  int64_t index_;
  DISALLOW_COPY_AND_ASSIGN(ObHATabletGroupMgr);
};

class ObStorageHATaskUtils
{
public:
  static int check_need_copy_sstable(
      const ObMigrationSSTableParam &param,
      const bool &is_restore,
      ObTabletHandle &tablet_handle,
      bool &need_copy);
  static int check_need_copy_macro_blocks(
      const ObMigrationSSTableParam &param,
      const bool is_leader_restore,
      bool &need_copy);
private:
  static int check_major_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      const bool &is_restore,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

  static int check_minor_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

  static int check_ddl_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);

  static int check_inc_major_ddl_sstable_need_copy_(
      const ObMigrationSSTableParam &param,
      ObTabletHandle &tablet_handle,
      bool &need_copy);
};

class ObStorageHACancelDagNetUtils
{
public:
  static int cancel_task(const share::ObLSID &ls_id, const share::ObTaskId &task_id);
private:
  static int cancel_migration_task_(const share::ObTaskId &task_id, const ObLSHandle &ls_handle, bool &is_exist);
};

template <typename DAG, typename... Args>
int ObStorageHADagUtils::alloc_and_schedule_dag(
    share::ObIDagNet *dag_net,
    share::ObIDag *parent_dag,
    share::ObIDag *child_dag,
    const share::ObDagPrio::ObDagPrioEnum prio,
    const bool emergency,
    DAG *&dag,
    Args&&... init_args)
{
  int ret = OB_SUCCESS;
  dag = nullptr;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler*);
  if (OB_ISNULL(scheduler) || OB_ISNULL(dag_net)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument for alloc_and_schedule_dag", K(ret),
        KP(scheduler), KP(dag_net), KP(parent_dag), KP(child_dag));
  } else if (OB_FAIL(scheduler->alloc_dag(dag, true/*is_ha_dag*/))) {
    STORAGE_LOG(WARN, "failed to alloc dag with priority", K(ret), K(prio));
  } else if (ObDagPrio::is_valid_prio(prio) && FALSE_IT(dag->set_priority(prio))) {
  } else if (OB_FAIL(dag->init(std::forward<Args>(init_args)...))) {
    STORAGE_LOG(WARN, "failed to init dag", K(ret));
  } else if (OB_FAIL(dag_net->add_dag_into_dag_net(*dag))) {
    STORAGE_LOG(WARN, "failed to add dag into dag net", K(ret));
  } else if (OB_NOT_NULL(parent_dag) && OB_FAIL(parent_dag->add_child_without_inheritance(*dag))) {
    STORAGE_LOG(WARN, "failed to add child dag to parent dag", K(ret), KP(parent_dag));
  } else if (OB_FAIL(dag->create_first_task())) {
    STORAGE_LOG(WARN, "failed to create first task", K(ret));
  } else if (OB_NOT_NULL(child_dag) && OB_FAIL(dag->add_child_without_inheritance(*child_dag))) {
    STORAGE_LOG(WARN, "failed to add child dag", K(ret), KP(child_dag));
  } else if (OB_FAIL(scheduler->add_dag(dag, emergency))) {
    STORAGE_LOG(WARN, "failed to add dag to scheduler", K(ret), KPC(dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      ret = OB_EAGAIN;
    }
  }
  if (OB_FAIL(ret) && OB_NOT_NULL(dag)) {
    scheduler->free_dag(*dag);
    dag = nullptr;
  }
  return ret;
}

template <typename DAG, typename... Args>
int ObStorageHADagUtils::alloc_and_schedule_single_dag(
    share::ObIDagNet *dag_net,
    const share::ObDagPrio::ObDagPrioEnum prio,
    const bool emergency,
    DAG *&dag,
    Args&&... init_args)
{
  return alloc_and_schedule_dag(dag_net, nullptr /*parent_dag*/, nullptr /*child_dag*/,
                                prio, emergency, dag,
                                std::forward<Args>(init_args)...);
}

template <typename FIRST_DAG, typename SECOND_DAG, typename... Args>
int ObStorageHADagUtils::alloc_and_schedule_pair_dags(
    const share::ObDagPrio::ObDagPrioEnum prio,
    share::ObIDag *parent_dag,
    FIRST_DAG *&first_dag,
    SECOND_DAG *&second_dag,
    Args&&... init_args)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  first_dag = nullptr;
  second_dag = nullptr;
  share::ObTenantDagScheduler *scheduler = MTL(share::ObTenantDagScheduler*);
  if (OB_ISNULL(scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to get ObTenantDagScheduler from MTL", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, first_dag))) {
    STORAGE_LOG(WARN, "failed to alloc first dag", K(ret));
  } else if (OB_FAIL(scheduler->alloc_dag_with_priority(prio, second_dag))) {
    STORAGE_LOG(WARN, "failed to alloc second dag", K(ret));
  } else if (OB_FAIL(first_dag->init(std::forward<Args>(init_args)...))) {
    STORAGE_LOG(WARN, "failed to init first dag", K(ret));
  } else if (OB_FAIL(second_dag->init(std::forward<Args>(init_args)...))) {
    STORAGE_LOG(WARN, "failed to init second dag", K(ret));
  } else if (OB_NOT_NULL(parent_dag) && OB_FAIL(parent_dag->add_child(*first_dag))) {
    STORAGE_LOG(WARN, "failed to add first dag as child of parent", K(ret), KP(parent_dag));
  } else if (OB_FAIL(first_dag->create_first_task())) {
    STORAGE_LOG(WARN, "failed to create first task for first dag", K(ret));
  } else if (OB_FAIL(first_dag->add_child(*second_dag))) {
    STORAGE_LOG(WARN, "failed to add second dag as child of first dag", K(ret));
  } else if (OB_FAIL(second_dag->create_first_task())) {
    STORAGE_LOG(WARN, "failed to create first task for second dag", K(ret));
  } else if (OB_FAIL(scheduler->add_dag(second_dag))) {
    STORAGE_LOG(WARN, "failed to add second dag to scheduler", K(ret), KPC(second_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(scheduler->add_dag(first_dag))) {
    STORAGE_LOG(WARN, "failed to add first dag to scheduler", K(ret), KPC(first_dag));
    if (OB_SIZE_OVERFLOW != ret && OB_EAGAIN != ret) {
      ret = OB_EAGAIN;
    }
    if (OB_SUCCESS != (tmp_ret = scheduler->cancel_dag(second_dag))) {
      STORAGE_LOG(WARN, "failed to cancel second dag", K(tmp_ret), KPC(second_dag));
    } else {
      second_dag = nullptr;
    }
  } else {
    STORAGE_LOG(INFO, "succeed to schedule two dags", KPC(first_dag), KPC(second_dag));
    first_dag = nullptr;
    second_dag = nullptr;
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(second_dag)) {
      scheduler->free_dag(*second_dag);
      second_dag = nullptr;
    }
    if (OB_NOT_NULL(scheduler) && OB_NOT_NULL(first_dag)) {
      scheduler->free_dag(*first_dag);
      first_dag = nullptr;
    }
  }
  return ret;
}

template <typename TaskType, typename... Args>
int ObStorageHADagUtils::alloc_and_add_task(
    share::ObIDag *dag,
    share::ObITask *parent_task,
    share::ObITask *child_task,
    TaskType *&task,
    Args&&... init_args)
{
  int ret = OB_SUCCESS;
  task = nullptr;
  if (OB_ISNULL(dag)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument for alloc_and_add_task", K(ret), KP(dag));
  } else if (OB_FAIL(dag->create_task(parent_task, task, std::forward<Args>(init_args)...))) {
    STORAGE_LOG(WARN, "failed to create task", K(ret));
  } else if (OB_NOT_NULL(child_task) && OB_FAIL(task->add_child(*child_task))) {
    STORAGE_LOG(WARN, "failed to add child task", K(ret), KP(child_task));
  }
  return ret;
}

template <typename TaskType, typename... Args>
int ObStorageHADagUtils::alloc_and_add_single_task(
    share::ObIDag *dag,
    TaskType *&task,
    Args&&... init_args)
{
  return alloc_and_add_task(dag, nullptr /*parent*/, nullptr /*child*/, task,
                            std::forward<Args>(init_args)...);
}

}
}
#endif

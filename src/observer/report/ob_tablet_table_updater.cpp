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

#define USING_LOG_PREFIX SERVER

#include "ob_tablet_table_updater.h"

#include "lib/utility/ob_tracepoint.h"
#include "share/tablet/ob_tablet_info.h"            // for ObTabletInfo
#include "share/tablet/ob_tablet_table_operator.h"  // for ObTabletOperator
#include "observer/ob_service.h"                    // for is_mini_mode
#include "share/ob_tablet_replica_checksum_operator.h" // for ObTabletReplicaChecksumItem
#include "lib/mysqlclient/ob_mysql_transaction.h" // ObMySQLTransaction
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/thread_local/ob_tsi_factory.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "storage/compaction/ob_compaction_diagnose.h"

namespace oceanbase
{
using namespace common;
using namespace share;

namespace observer
{

void TSITabletTableUpdatStatistics::reset()
{
  suc_cnt_ = 0;
  fail_cnt_ = 0;
  remove_task_cnt_ = 0;
  update_task_cnt_ = 0;
  total_wait_us_ = 0;
  total_exec_us_ = 0;
}

void TSITabletTableUpdatStatistics::calc(
     int64_t succ_cnt,
     int64_t fail_cnt,
     int64_t remove_task_cnt,
     int64_t update_task_cnt,
     int64_t wait_us,
     int64_t exec_us)
{
  total_wait_us_ += wait_us;
  total_exec_us_ += exec_us;
  suc_cnt_ += succ_cnt;
  fail_cnt_ += fail_cnt;
  remove_task_cnt_ += remove_task_cnt;
  update_task_cnt_ += update_task_cnt;
}

void TSITabletTableUpdatStatistics::dump()
{
  int64_t total_cnt = suc_cnt_ + fail_cnt_;
  FLOG_INFO("[TABLET_TABLE_UPDATE_STATISTIC] dump tablet table update statistics",
           K_(suc_cnt), K_(fail_cnt), K_(remove_task_cnt), K_(update_task_cnt),
           "avg_wait_us", total_wait_us_ / total_cnt,
           "avg_exec_us", total_exec_us_ / total_cnt);
}

/*
 * ObTabletTableUpdateTask implement
 * */
ObTabletTableUpdateTask::~ObTabletTableUpdateTask()
{
}

int ObTabletTableUpdateTask::init(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t add_timestamp,
    const bool need_diagnose/*false*/)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !tablet_id.is_valid()
      || 0 >= add_timestamp)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("task init failed", KR(ret), K(ls_id), K(tablet_id),
              K(add_timestamp));
  } else {
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    add_timestamp_ = add_timestamp;
    need_diagnose_ = need_diagnose;
  }
  return ret;
}

int ObTabletTableUpdateTask::assign(const ObTabletTableUpdateTask &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    ls_id_ = other.get_ls_id();
    tablet_id_ = other.get_tablet_id();
    add_timestamp_ = other.get_add_timestamp();
    need_diagnose_ = other.need_diagnose_;
    start_timestamp_ = other.start_timestamp_;
  }
  return ret;
}

bool ObTabletTableUpdateTask::operator ==(const ObTabletTableUpdateTask &other) const
{
  bool equal = false;
  if (this == &other) { // same pointer
    equal = true;
  } else {
    equal = (ls_id_ == other.ls_id_
             && tablet_id_ == other.tablet_id_);
  }
  return equal;
}

void ObTabletTableUpdateTask::reset()
{
  ls_id_.reset();
  tablet_id_.reset();
  add_timestamp_ = OB_INVALID_TIMESTAMP;
  need_diagnose_ = false;
}

bool ObTabletTableUpdateTask::compare_without_version(
         const ObTabletTableUpdateTask &other) const
{
  bool equal = false;
  if (&other == this) {
    equal = true;
  } else  {
    equal = (ls_id_ == other.ls_id_
             && tablet_id_ == other.tablet_id_);
  }
  return equal;
}

void ObTabletTableUpdateTask::check_task_status() const
{
  int64_t now = ObTimeUtility::current_time();
  const int64_t safe_interval = TABLET_CHECK_INTERVAL;
  // need to print a WARN log if this task is not executed correctly since two minuts ago
  if (now - add_timestamp_ > safe_interval) {
    FLOG_WARN_RET(OB_ERR_UNEXPECTED, "tablet table update task cost too much time to execute",
              K(*this), K(safe_interval), "cost_time", now - add_timestamp_);
  }
}

void ObTabletTableUpdateTask::set_start_timestamp()
{
  start_timestamp_ = ObTimeUtility::current_time();
}

int64_t ObTabletTableUpdateTask::get_start_timestamp() const
{
  return start_timestamp_;
}

bool ObTabletTableUpdateTask::is_valid() const
{
  return ls_id_.is_valid()
      && tablet_id_.is_valid()
      && 0 < add_timestamp_;
}

bool ObTabletTableUpdateTask::is_barrier() const
{
  return false;
}

int64_t ObTabletTableUpdateTask::hash() const
{
  uint64_t hash_val = 0;
  hash_val = murmurhash(&ls_id_, sizeof(ls_id_), hash_val);
  hash_val = murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);
  return hash_val;
}

/*
 * ObTabletTableUpdater implement
 * */
int ObTabletTableUpdater::mtl_init(ObTabletTableUpdater *&tablet_table_updater)
{
  return tablet_table_updater->init();
}

int ObTabletTableUpdater::init()
{
  int ret = OB_SUCCESS;
  const int64_t update_queue_size = !lib::is_mini_mode()
                                    ? UPDATE_QUEUE_SIZE
                                    : MINI_MODE_UPDATE_QUEUE_SIZE;
  // TODO: allow set thread_cnt in config file
  const int64_t update_task_thread_cnt = cal_thread_count_();
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("inited twice", KR(ret));
  } else if (OB_FAIL(update_queue_.init(this,
                                        update_task_thread_cnt,
                                        update_queue_size,
                                        "TbltTblUp",
                                        MTL_ID()))) {
    LOG_WARN("init tablet table updater queue failed", KR(ret),
             "thread_count", update_task_thread_cnt,
             "queue_size", update_queue_size);
  } else {
    is_inited_ = true;
    is_stop_ = false;
    tenant_id_ = MTL_ID();
  }
  if (OB_SUCC(ret)) {
    LOG_INFO("init a ObTabletTableUpdater success", K(update_task_thread_cnt));
  }
  return ret;
}

void ObTabletTableUpdater::stop()
{
  if (is_inited_) {
    is_stop_ = true;
    update_queue_.stop();
    LOG_INFO("stop ObTabletTableUpdater success");
  }
}

void ObTabletTableUpdater::wait()
{
  if (is_inited_) {
    update_queue_.wait();
    LOG_INFO("wait ObTabletTableUpdater");
  }
}

void ObTabletTableUpdater::destroy()
{
  stop();
  wait();
  is_inited_ = false;
  is_stop_ = true;
  tenant_id_ = OB_INVALID_TENANT_ID;
}

int64_t ObTabletTableUpdater::cal_thread_count_()
{
  int tmp_ret = OB_SUCCESS;
  int64_t thread_cnt = MINI_MODE_UPDATE_TASK_THREAD_CNT;
  if (!lib::is_mini_mode()) {
    double max_cpu = 0;
    double min_cpu = 0;
    omt::ObMultiTenant *omt = GCTX.omt_;
    if (NULL == omt) {
      tmp_ret = OB_INVALID_ARGUMENT;
      LOG_WARN_RET(tmp_ret, "invalid argument", K(tmp_ret), KP(omt));
    } else if (OB_TMP_FAIL(omt->get_tenant_cpu(tenant_id_, min_cpu, max_cpu))) {
      LOG_WARN_RET(tmp_ret, "fail to get tenant cpu", K(tmp_ret), K(min_cpu), K(max_cpu));
    } else {
      thread_cnt = std::max(MIN_UPDATE_TASK_THREAD_CNT,
          lround(MIN_UPDATE_TASK_THREAD_CNT * UPDATE_TASK_THREAD_RATIO * max_cpu));
      thread_cnt = std::min(thread_cnt, MAX_UPDATE_TASK_THREAD_CNT);
    }
  }
  return thread_cnt;
}

int ObTabletTableUpdater::submit_tablet_update_task(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const bool need_diagnose/*false*/)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableUpdater is not inited", KR(ret));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(async_update(ls_id, tablet_id, need_diagnose))) {
    LOG_WARN("fail to async update tablet", KR(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObTabletTableUpdater::async_update(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const bool need_diagnose/*false*/)
{
  int ret = OB_SUCCESS;
  int64_t add_timestamp = ObTimeUtility::current_time();
  ObTabletTableUpdateTask task;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableUpdater is not inited", KR(ret));
  } else if (tablet_id.is_reserved_tablet()) {
    LOG_TRACE("no need to report reserved tablet", KR(ret), K(tablet_id));
  } else if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(task.init(ls_id,
                               tablet_id,
                               add_timestamp,
                               need_diagnose))) {
    LOG_WARN("set update task failed", KR(ret), K(ls_id), K(tablet_id),
             K(add_timestamp));
  } else if (OB_FAIL(add_task_(task))){
    LOG_WARN("fail to add task", KR(ret), K(ls_id), K(tablet_id),
             K(add_timestamp));
  }
  return ret;
}

int ObTabletTableUpdater::add_task_(
    const ObTabletTableUpdateTask &task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", KR(ret));
  } else if (!task.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid task", KR(ret), K(task));
  }
 #ifdef ERRSIM
  if (task.need_diagnose()) {
    ret = OB_E(EventTable::EN_COMPACTION_REPORT_ADD_TASK_FAILED) OB_SUCCESS;
    if (OB_FAIL(ret)) {
      STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_REPORT_ADD_TASK_FAILED", K(ret));
    }
  }
#endif
  if (FAILEDx(update_queue_.add(task))){
    // TODO: deal with barrier-tasks when execute
    if (OB_EAGAIN == ret) {
      LOG_TRACE("tablet table update task exist", K(task));
      ret = OB_SUCCESS;
    }
  }
  if (OB_FAIL(ret)) {
    LOG_WARN("add tablet table update task failed", KR(ret), K(task));
    if (task.need_diagnose() && OB_TMP_FAIL(compaction::ADD_SUSPECT_INFO(
        compaction::MEDIUM_MERGE, share::ObDiagnoseTabletType::TYPE_REPORT,
        task.get_ls_id(), task.get_tablet_id(),
        ObSuspectInfoType::SUSPECT_COMPACTION_REPORT_ADD_FAILED,
        static_cast<int64_t>(ret)))) {
      LOG_WARN_RET(tmp_ret, "fail to add suspect info", K(tmp_ret));
    }
  } else {
    if (task.need_diagnose()) {
      DEL_SUSPECT_INFO(
        compaction::MEDIUM_MERGE,
        task.get_ls_id(), task.get_tablet_id(),
        share::ObDiagnoseTabletType::TYPE_REPORT);
    }
    LOG_TRACE("add tablet table update task success", KR(ret), K(task));
  }
  return ret;
}

int ObTabletTableUpdater::reput_to_queue_(
    const ObIArray<ObTabletTableUpdateTask> &tasks)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else { // try to push task back to queue, ignore ret code
    ARRAY_FOREACH_NORET(tasks, i) {
      const ObTabletTableUpdateTask &task = tasks.at(i);
      if (OB_UNLIKELY(!task.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid task", KR(ret), K(task));
      } else if (OB_FAIL(add_task_(task))) {
        LOG_ERROR("fail to reput to queue", KR(ret), K(task));
      }
    }
  }
  return ret;
}

int ObTabletTableUpdater::check_tenant_status_(
    const uint64_t tenant_id,
    bool &tenant_dropped,
    bool &schema_not_ready)
{
  int ret = OB_SUCCESS;
  schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
  schema::ObSchemaGetterGuard guard;
  tenant_dropped = false;
  schema_not_ready = false;
  if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(guard.check_if_tenant_has_been_dropped(tenant_id, tenant_dropped))) {
    LOG_WARN("fail to check if tenant has been dropped", KR(ret), K(tenant_id));
  } else if (!schema_service->is_tenant_full_schema(tenant_id)) {
    // need wait schema refresh
    schema_not_ready = true;
  }
  return ret;
}

int ObTabletTableUpdater::process_barrier(
    const ObTabletTableUpdateTask &task,
    bool &stopped)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(task);
  UNUSED(stopped);
  LOG_WARN("not supported now", KR(ret), K(task), K(stopped));
  return ret;
}

int ObTabletTableUpdater::set_thread_count()
{
  int ret = OB_SUCCESS;
  int64_t thread_count = cal_thread_count_();
  if (OB_FAIL(update_queue_.set_thread_count(thread_count))) {
    LOG_WARN("fail to set thread count", K(ret), K(thread_count));
  } else {
    LOG_TRACE("success to set thread count", K(thread_count));
  }
  return ret;
}

int ObTabletTableUpdater::check_exist(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObTabletTableUpdateTask task(ls_id, tablet_id, ObClockGenerator::getClock());
    if (OB_FAIL(update_queue_.check_exist(task, exist))) {
      LOG_WARN("fail to check task exist", K(ret), K(task), K(exist));
    }
  }
  return ret;
}

int ObTabletTableUpdater::check_processing_exist(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    bool &exist)
{
  int ret = OB_SUCCESS;
  exist = false;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(tablet_id));
  } else {
    ObTabletTableUpdateTask task(ls_id, tablet_id, ObClockGenerator::getClock());
    if (OB_FAIL(update_queue_.check_processing_exist(task, exist))) {
      LOG_WARN("fail to check processing task exist", K(ret), K(task), K(exist));
    }
  }
  return ret;
}

int ObTabletTableUpdater::diagnose_existing_task(
    ObIArray<ObTabletTableUpdateTask> &waiting_tasks,
    ObIArray<ObTabletTableUpdateTask> &processing_tasks)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(update_queue_.diagnose_waiting_task(waiting_tasks))) {
    LOG_WARN("fail to diagnose waiting task", K(ret));
  } else if (OB_FAIL(update_queue_.diagnose_processing_task(processing_tasks))) {
    LOG_WARN("fail to diagnose processing task", K(ret));
  }
  return ret;
}

void ObTabletTableUpdater::diagnose_batch_tasks_(
    const ObIArray<ObTabletTableUpdateTask> &batch_tasks,
    const int error_code)
{
  int tmp_ret = OB_SUCCESS;
  int64_t diagnose_cnt = 0;
  for (int64_t i = 0; i < batch_tasks.count() && diagnose_cnt < DIAGNOSE_MAX_BATCH_COUNT; ++i) {
    const ObTabletTableUpdateTask &task = batch_tasks.at(i);
    if (task.need_diagnose()) {
      if (OB_TMP_FAIL(compaction::ADD_SUSPECT_INFO(
          compaction::MEDIUM_MERGE, share::ObDiagnoseTabletType::TYPE_MEDIUM_MERGE,
          task.get_ls_id(), task.get_tablet_id(),
          ObSuspectInfoType::SUSPECT_COMPACTION_REPORT_PROGRESS_FAILED,
          static_cast<int64_t>(error_code)))) {
        LOG_WARN_RET(tmp_ret, "fail to add suspect info", K(tmp_ret));
      } else {
        ++diagnose_cnt;
      }
    }
  }
}

int ObTabletTableUpdater::generate_tasks_(
    const ObIArray<ObTabletTableUpdateTask> &batch_tasks,
    ObArray<ObTabletReplica> &update_tablet_replicas,
    ObArray<ObTabletReplica> &remove_tablet_replicas,
    ObArray<ObTabletReplicaChecksumItem> &update_tablet_checksums,
    UpdateTaskList &update_tablet_tasks,
    RemoveTaskList &remove_tablet_tasks)
{
  int ret = OB_SUCCESS;
  int64_t count = UNIQ_TASK_QUEUE_BATCH_EXECUTE_NUM;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableUpdater is not inited", KR(ret));
  } else if (OB_ISNULL(GCTX.tablet_operator_)
             || OB_ISNULL(GCTX.ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.tablet_operator_), KP(GCTX.ob_service_));
  } else if (OB_UNLIKELY(batch_tasks.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch_tasks count <= 0", KR(ret), "tasks_count", batch_tasks.count());
  } else {
    ObTabletReplica replica;
    ObTabletReplicaChecksumItem checksum_item;
    FOREACH_CNT_X(task, batch_tasks, OB_SUCC(ret)) {
      // split tasks into remove and update
      if (OB_ISNULL(task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid task", KR(ret), K(task));
      } else {
        task->check_task_status();
        replica.reset();
        checksum_item.reset();
        if (OB_FAIL(GCTX.ob_service_->fill_tablet_report_info(tenant_id_,
                                                         task->get_ls_id(),
                                                         task->get_tablet_id(),
                                                         replica,
                                                         checksum_item))) {
          if (OB_TABLET_NOT_EXIST == ret
              || OB_LS_NOT_EXIST == ret
              || OB_TENANT_NOT_IN_SERVER == ret) {
            ret = OB_SUCCESS;
            // fill primary keys of the replica for removing
            if (OB_FAIL(replica.init(
                tenant_id_,
                task->get_tablet_id(),
                task->get_ls_id(),
                GCONF.self_addr_,
                1/*snapshot_version*/,
                1/*data_size*/,
                1/*required_size*/,
                0/*report_scn*/,
                ObTabletReplica::SCN_STATUS_IDLE))) {
              LOG_WARN("fail to init ObTabletReplica", KR(ret),
                  KPC(task), "server", GCONF.self_addr_);
            } else if (OB_FAIL(remove_tablet_tasks.reserve(count))) {
              // reserve() is reentrant, do not have to check whether first time
              LOG_WARN("fail to reserver remove_tablet_tasks", KR(ret), K(count));
            } else if (OB_FAIL(remove_tablet_tasks.push_back(*task))) {
              LOG_WARN("fail to push back remove task", KR(ret), KPC(task));
            } else if (OB_FAIL(remove_tablet_replicas.reserve(count))) {
              LOG_WARN("fail to reserver remove_tablet_replicas", KR(ret), K(count));
            } else if (OB_FAIL(remove_tablet_replicas.push_back(replica))) {
              LOG_WARN("fail to push back replica", KR(ret), K(replica));
            }
          } else {
            LOG_WARN("fail to fill tablet replica", KR(ret), K(task));
          }
        } else {
          LOG_TRACE("fill tablet success", K(task), K(replica));
          if (OB_FAIL(update_tablet_replicas.reserve(count))) {
            // reserve() is reentrant, do not have to check whether first time
            LOG_WARN("fail to reserve update_tablet_replicas", KR(ret), K(count));
          } else if (OB_FAIL(update_tablet_checksums.reserve(count))) {
            // reserve() is reentrant, do not have to check whether first time
            LOG_WARN("fail to reserve update_tablet_checksums", KR(ret), K(count));
          } else if (OB_FAIL(update_tablet_tasks.reserve(count))) {
            // reserve() is reentrant, do not have to check whether first time
            LOG_WARN("fail to reserve update_tablet_tasks", KR(ret), K(count));
          } else if (OB_FAIL(update_tablet_replicas.push_back(replica))) {
            LOG_WARN("fail to push back replica", KR(ret), K(replica));
          } else if (OB_FAIL(update_tablet_checksums.push_back(checksum_item))) {
            LOG_WARN("fail to push back checksum item", KR(ret), K(checksum_item));
          } else if (OB_FAIL(update_tablet_tasks.push_back(*task))) {
            LOG_WARN("fail to push back task", KR(ret), KPC(task));
          }
        }
      }
    } //FOREACH

    if (OB_SUCC(ret)
        && (update_tablet_tasks.count() != update_tablet_replicas.count()
            || update_tablet_tasks.count() != update_tablet_checksums.count()
            || update_tablet_tasks.count() + remove_tablet_tasks.count() !=
               batch_tasks.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet task count and replica count not match", KR(ret),
               "tablet_update_tasks count", update_tablet_tasks.count(),
               "tablet_update_replicas count", update_tablet_replicas.count(),
               "tablet_update_checksums count", update_tablet_checksums.count(),
               "tablet_remove_tasks count", remove_tablet_tasks.count(),
               "batch_tasks count", batch_tasks.count());
    }
  }
  return ret;
}

int ObTabletTableUpdater::batch_process_tasks(
    const ObIArray<ObTabletTableUpdateTask> &batch_tasks,
    bool &stopped)
{
  UNUSED(stopped);
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool tenant_dropped = false;
  bool schema_not_ready = false;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
  const int64_t start_time = ObTimeUtility::current_time();
  ObArray<ObTabletReplica> update_tablet_replicas;
  ObArray<ObTabletReplica> remove_tablet_replicas;
  ObArray<ObTabletReplicaChecksumItem> update_tablet_checksums;
  UpdateTaskList update_tablet_tasks;
  RemoveTaskList remove_tablet_tasks;
  ObCurTraceId::init(GCONF.self_addr_);
  int64_t succ_cnt = 0;
  int64_t update_task_cnt = 0;
  int64_t remove_task_cnt = 0;
  int64_t wait_cost = 0;
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; i < batch_tasks.count(); i++) { // overwrite ret
      wait_cost += (start_time - batch_tasks.at(i).get_add_timestamp());
    }
  }
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTabletTableUpdater is not inited", KR(ret));
  } else if (batch_tasks.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid batch_tasks", KR(ret), "task count", batch_tasks.count());
  } else {
    (void)check_tenant_status_(meta_tenant_id, tenant_dropped, schema_not_ready);
  }
  if (OB_FAIL(ret)) {
    // do nothing
  } else if (tenant_dropped) {
    if (REACH_TIME_INTERVAL(10_s)) { // 10s
      FLOG_INFO("REPORT: tasks can't be processed because it's superior tenant has been dropped",
          KR(ret), K(meta_tenant_id), K(batch_tasks));
    }
  } else if (schema_not_ready) { // need wait schema refresh
    ret = OB_NEED_WAIT;
    if (REACH_TIME_INTERVAL(1_s)) { // 1s
      LOG_WARN("tenant schema is not ready, need wait", KR(ret), K(meta_tenant_id), K(batch_tasks));
    }
    (void) throttle_(ret, ObTimeUtility::current_time() - start_time);
    if (OB_FAIL(reput_to_queue_(batch_tasks))) {
      LOG_WARN("fail to reput remove task to queue", KR(ret), K(batch_tasks));
    }
  } else if (OB_FAIL(generate_tasks_(
      batch_tasks,
      update_tablet_replicas,
      remove_tablet_replicas,
      update_tablet_checksums,
      update_tablet_tasks,
      remove_tablet_tasks))) {
    //There is a situation where there are too many tablet holds and cannot be obtained
    LOG_WARN("generate_tasks failed", KR(ret), "batch_tasks count", batch_tasks.count(),
              "update_tablet_replicas", update_tablet_replicas.count(),
              "remove_tablet_replicas", remove_tablet_replicas.count(),
              "update_tablet_checksums", update_tablet_checksums.count(),
              "update_tablet_tasks", update_tablet_tasks.count(),
              "remove_tablet_tasks", remove_tablet_tasks.count());
  } else {
    update_task_cnt = update_tablet_replicas.count();
    remove_task_cnt = remove_tablet_replicas.count();
    if (update_tablet_tasks.count() > 0) {
      tmp_ret = do_batch_update_(start_time, update_tablet_tasks, update_tablet_replicas, update_tablet_checksums);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("do_batch_update_ failed", KR(tmp_ret), K(start_time),
            "tasks count", update_tablet_tasks.count(),
            "tablet replicas count", update_tablet_replicas.count());
        diagnose_batch_tasks_(update_tablet_tasks, tmp_ret);
      } else {
        succ_cnt += update_task_cnt;
      }
    }
    if (remove_tablet_tasks.count() > 0) {
      tmp_ret = do_batch_remove_(start_time, remove_tablet_tasks, remove_tablet_replicas);
      if (OB_SUCCESS != tmp_ret) {
        ret = OB_SUCC(ret) ? tmp_ret : ret;
        LOG_WARN("do_batch_remove_ failed", KR(tmp_ret), K(start_time),
            "tasks count", remove_tablet_tasks.count(),
            "remove replicas count", remove_tablet_replicas.count());
        diagnose_batch_tasks_(remove_tablet_tasks, tmp_ret);
      } else {
        succ_cnt += remove_task_cnt;
      }
    }
  }
  const int64_t end = ObTimeUtility::current_time();
  auto* statistics = GET_TSI(TSITabletTableUpdatStatistics);
  if (OB_ISNULL(statistics)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get statistic", "ret", OB_ERR_UNEXPECTED);
  } else {
    (void)statistics->calc(succ_cnt, batch_tasks.count() - succ_cnt,
        remove_task_cnt, update_task_cnt, wait_cost, end - start_time);
    const int64_t interval = 10 * 1000 * 1000; // 1s
    if (TC_REACH_TIME_INTERVAL(interval)) {
      (void)statistics->dump();
      (void)statistics->reset();
    }
  }
  return ret;
}

int ObTabletTableUpdater::do_batch_remove_(
    const int64_t start_time,
    const ObIArray<ObTabletTableUpdateTask> &tasks,
    const ObIArray<ObTabletReplica> &replicas)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t tasks_count = tasks.count();
  const int64_t batch_remove_start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(tasks_count != replicas.count() || OB_ISNULL(GCTX.tablet_operator_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tasks count", KR(ret), K(tasks_count), KP(GCTX.tablet_operator_));
  } else {
    common::ObMySQLTransaction trans;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(GCTX.tablet_operator_->batch_remove(trans, tenant_id_, replicas))) {
      LOG_WARN("do tablet table remove failed, try to reput to queue", KR(ret),
               "escape time", ObTimeUtility::current_time() - start_time);
    } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_remove_with_trans(trans, tenant_id_, replicas))) {
      LOG_WARN("do tablet table checksum remove failed, try to reput to queue", KR(ret),
               "escape time", ObTimeUtility::current_time() - start_time);
    }

    if (trans.is_started()) {
      int trans_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("fail to end transaction", KR(trans_ret));
        ret = ((OB_SUCCESS == ret) ? trans_ret : ret);
      }
    }
    if (OB_FAIL(ret)) {
      (void) throttle_(ret, ObTimeUtility::current_time() - start_time);
      if (OB_SUCCESS != (tmp_ret = reput_to_queue_(tasks))) {
        LOG_ERROR("fail to reput remove task to queue", KR(tmp_ret), K(tasks_count));
      } else {
        LOG_TRACE("reput remove task to queue success", K(tasks_count));
      }
    }
  }
  LOG_INFO("REPORT: batch remove tablets finished", KR(ret), K(tasks_count), K(tasks),
      "cost_time", ObTimeUtility::current_time() - batch_remove_start_time);
  return ret;
}

int ObTabletTableUpdater::do_batch_update_(
    const int64_t start_time,
    const ObIArray<ObTabletTableUpdateTask> &tasks,
    const ObIArray<ObTabletReplica> &replicas,
    const ObIArray<ObTabletReplicaChecksumItem> &checksums)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t batch_update_start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (tasks.count() != replicas.count()
      || tasks.count() != checksums.count()
      || OB_ISNULL(GCTX.tablet_operator_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tasks num not match or invalid tablet_operator", KR(ret), "task_cnt", tasks.count(),
             "replica_cnt", replicas.count(), "checksum_cnt", checksums.count(), K(GCTX.tablet_operator_));
  } else {
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      FOREACH_CNT_X(task, tasks, OB_SUCC(ret)) {
        if (OB_NOT_NULL(task) && task->need_diagnose()) {
          ret = OB_E(EventTable::EN_COMPACTION_REPORT_PROCESS_TASK_FAILED) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            STORAGE_LOG(INFO, "ERRSIM EN_COMPACTION_REPORT_PROCESS_TASK_FAILED", K(ret));
          }
          DEBUG_SYNC(COMPACTION_REPORT_PROCESS);
        }
      }
    }
#endif
    common::ObMySQLTransaction trans;
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id_);
    if (FAILEDx(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
      LOG_WARN("fail to start transaction", KR(ret), K_(tenant_id), K(meta_tenant_id));
    } else if (OB_FAIL(GCTX.tablet_operator_->batch_update(trans, tenant_id_, replicas))) {
      LOG_WARN("do tablet table update failed, try to reput to queue", KR(ret),
             "escape time", ObTimeUtility::current_time() - start_time);
    } else if (OB_FAIL(ObTabletReplicaChecksumOperator::batch_update_with_trans(trans, tenant_id_, checksums))) {
      LOG_WARN("do tablet table checksum update failed, try to reput to queue", KR(ret),
             "escape time", ObTimeUtility::current_time() - start_time);
    }

    if (trans.is_started()) {
      int trans_ret = trans.end(OB_SUCCESS == ret);
      if (OB_SUCCESS != trans_ret) {
        LOG_WARN("fail to end transaction", KR(trans_ret));
        ret = ((OB_SUCCESS == ret) ? trans_ret : ret);
      }
    }
    if (OB_FAIL(ret)) {
      (void) throttle_(ret, ObTimeUtility::current_time() - start_time);
      if (OB_SUCCESS != (tmp_ret = reput_to_queue_(tasks))) {
        LOG_ERROR("fail to reput update task to queue", KR(tmp_ret), K(tasks.count()));
      } else {
        LOG_TRACE("reput update task to queue success", K(tasks.count()));
      }
    }
  }
  LOG_INFO("REPORT: batch update tablets finished", KR(ret), K(replicas.count()), K(tasks),
      "cost_time", ObTimeUtility::current_time() - batch_update_start_time);
  return ret;
}

int ObTabletTableUpdater::throttle_(
    const int return_code,
    const int64_t execute_time_us)
{
  int ret = OB_SUCCESS;
  int64_t sleep_us = 0;
  if (OB_SUCCESS != return_code) {
    sleep_us = 2l * 1000 * 1000; // 2s
  } else if (execute_time_us > 20 * 1000 * 1000) { // 20s
    sleep_us = MIN(1L * 1000 * 1000, (execute_time_us - 20 * 1000 * 1000));
    LOG_WARN("detected slow update, may be too many concurrent updating", K(sleep_us));
  }
  const static int64_t sleep_step_us = 20 * 1000; // 20ms
  for (; !is_stop_ && sleep_us > 0;
      sleep_us -= sleep_step_us) {
    ob_usleep(static_cast<int32_t>(std::min(sleep_step_us, sleep_us)));
  }
  return ret;
}

} // end namespace observer
} // end namespace oceanbase

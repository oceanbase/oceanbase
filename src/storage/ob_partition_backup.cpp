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

#define USING_LOG_PREFIX STORAGE
#include "lib/utility/ob_tracepoint.h"
#include "lib/hash/ob_hashset.h"
#include "share/ob_force_print_log.h"
#include "storage/ob_partition_backup.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_log.h"
#include "storage/ob_partition_base_data_ob_reader.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "clog/ob_clog_history_reporter.h"
#include "share/scheduler/ob_sys_task_stat.h"
#include "share/partition_table/ob_united_pt_operator.h"
#include "storage/ob_partition_base_data_restore_reader.h"
#include "storage/ob_partition_base_data_backup.h"
#include "share/ob_task_define.h"
#include "share/backup/ob_backup_struct.h"
#include "ob_table_mgr.h"
#include "ob_migrate_macro_block_writer.h"
#include "share/ob_debug_sync_point.h"
#include "ob_storage_struct.h"
#include "observer/omt/ob_tenant_node_balancer.h"
#include "storage/ob_partition_migration_status.h"
#include "storage/ob_pg_all_meta_checkpoint_reader.h"
#include "share/backup/ob_backup_path.h"
#include "storage/ob_partition_base_data_physical_restore.h"
#include "storage/ob_pg_storage.h"

namespace oceanbase {
using namespace common;
using namespace common::hash;
using namespace share;
using namespace obrpc;
using namespace blocksstable;
using namespace oceanbase::share::schema;
using namespace omt;
using namespace transaction;

namespace storage {

ObPartGroupBackupTask::ObPartGroupBackupTask() : ObPartGroupTask()
{}

ObPartGroupBackupTask::~ObPartGroupBackupTask()
{}

int ObPartGroupBackupTask::init(const ObIArray<ObReplicaOpArg>& task_list, const bool is_batch_mode,
    storage::ObPartitionService* partition_service, const share::ObTaskId& task_id)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_MAP_BUCKET_NUM = 1280;
  SMART_VAR(ObPartMigrationTask, tmp_task)
  {
    ObChangeMemberOption change_member_option = NORMAL_CHANGE_MEMBER_LIST;
    common::SpinWLockGuard guard(lock_);

    if (is_inited_) {
      ret = OB_INIT_TWICE;
      STORAGE_LOG(WARN, "cannot init twice", K(ret));
    } else if (NULL == partition_service || task_list.empty() || task_id.is_invalid()) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid args", K(ret), K(is_batch_mode), KP(partition_service), K(task_id), K(task_list));
    } else if (OB_FAIL(cond_.init(ObWaitEventIds::GROUP_MIGRATE_TASK_IDLE_WAIT))) {
      STORAGE_LOG(WARN, "failed to init cond", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < task_list.count(); ++i) {
        tmp_task.arg_ = task_list.at(i);
        tmp_task.arg_.cluster_id_ =
            (tmp_task.arg_.cluster_id_ == OB_INVALID_CLUSTER_ID) ? GCONF.cluster_id : tmp_task.arg_.cluster_id_;
        tmp_task.status_ = ObPartMigrationTask::INIT;
        tmp_task.result_ = OB_SUCCESS;

        if (OB_FAIL(task_list_.push_back(tmp_task))) {
          STORAGE_LOG(WARN, "failed to add task list", K(ret));
        }

        if (OB_SUCC(ret)) {
          if (0 == i) {
            type_ = task_list.at(i).type_;
            tenant_id_ = task_list.at(i).key_.get_tenant_id();
            change_member_option = task_list.at(i).change_member_option_;
            if (!is_replica_op_valid(type_)) {
              ret = OB_ERR_SYS;
              STORAGE_LOG(ERROR, "invalid type", K(ret), K(i), K(type_), K(task_list));
            }
          } else if (type_ != task_list.at(i).type_ || tenant_id_ != task_list.at(i).key_.get_tenant_id() ||
                     change_member_option != task_list.at(i).change_member_option_) {
            ret = OB_ERR_SYS;
            STORAGE_LOG(ERROR,
                "migration type or tenant_id or skip_change_member_list not same in task list",
                K(ret),
                K(i),
                K(task_list));
          }
        }
      }

      if (OB_SUCC(ret)) {
        is_inited_ = true;
        is_batch_mode_ = is_batch_mode;
        partition_service_ = partition_service;
        first_error_code_ = OB_SUCCESS;
        task_id_ = task_id;
        is_finished_ = false;
        need_idle_ = true;
        change_member_option_ = change_member_option;
        STORAGE_LOG(INFO, "succeed to init pg group backup task", K(*this));
      }
    }
  }
  return ret;
}

int ObPartGroupBackupTask::check_partition_validation()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else {
    ObRecoveryPointSchemaFilter backup_filter;
    const ObPhysicalBackupArg& backup_arg = task_list_[0].arg_.backup_arg_;
    if (OB_FAIL(backup_filter.init(backup_arg.tenant_id_,
            backup_arg.backup_schema_version_, /*backup_schema_version*/
            backup_arg.backup_schema_version_ /*current_schema_version*/))) {
      STORAGE_LOG(WARN, "backup schema filter init fail", K(ret));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObPartMigrationTask& sub_task = task_list_[i];
      ObPartitionKey& pkey = sub_task.arg_.key_;
      bool in_member_list = false;
      bool is_working_partition = partition_service_->is_working_partition(pkey);

      if (ObPartGroupMigrator::get_instance().is_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        STORAGE_LOG(WARN, "server is stopping", K(ret));
      } else if (BACKUP_REPLICA_OP != sub_task.arg_.type_) {
        ret = OB_ERR_SYS;
        LOG_ERROR("replica op type is not backup", K(ret), "arg", sub_task.arg_);
      } else {
        if (OB_UNLIKELY(!partition_service_->is_partition_exist(pkey))) {
          ret = OB_ENTRY_NOT_EXIST;
          ObTaskController::get().allow_next_syslog();
          STORAGE_LOG(WARN, "can not backup replica which is not exsit", K(ret), K(pkey), K(sub_task));
        } else {
          // backup filter base on target version schema
          bool is_exist = false;
          if (OB_FAIL(backup_filter.check_partition_exist(pkey, is_exist))) {
            STORAGE_LOG(WARN, "backup filter check partition failed", K(ret), K(pkey));
          } else if (OB_UNLIKELY(!is_exist)) {
            ret = OB_PARTITION_NOT_EXIST;
            STORAGE_LOG(WARN, "backup partition is not exist after filter", K(ret), K(pkey), K(sub_task));
          }
        }
      }
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = E(EventTable::EN_CHECK_SUB_MIGRATION_TASK) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        STORAGE_LOG(ERROR, "fake EN_CHECK_SUB_MIGRATION_TASK", K(ret));
      }
    }
#endif
  }

  return ret;
}

int ObPartGroupBackupTask::do_task()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObCurTraceId::set(task_id_);

  ObTaskController::get().allow_next_syslog();
  LOG_INFO("start pg group backup task", K(*this));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(check_before_do_task())) {
    LOG_WARN("Failed to check_before_do_task", K(ret));
  } else if (OB_FAIL(check_all_pg_backup_point_created())) {
    LOG_WARN("failed to check all pg backup point created", K(ret));
  } else if (OB_FAIL(do_part_group_backup_minor_task())) {
    LOG_WARN("failed to do part group backup minor task", K(ret));
  } else if (OB_FAIL(do_backup_pg_metas())) {
    LOG_WARN("failed to do backup pg metas", K(ret));
  } else if (OB_FAIL(do_part_group_backup_major_task())) {
    LOG_WARN("failed to do part group backup major task", K(ret));
  }

  if (OB_FAIL(ret)) {
    common::SpinWLockGuard guard(lock_);
    if (OB_SUCCESS == first_error_code_) {
      first_error_code_ = ret;
      LOG_INFO("set first error code", K(ret), K(first_error_code_));
    }
  }

  if (OB_SUCCESS != (tmp_ret = finish_group_backup_task())) {
    LOG_ERROR("failed to finish group backup task", K(ret));
  }

  ObTaskController::get().allow_next_syslog();
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("group backup task finish", K(cost_ts), K(*this));
  return ret;
}

int ObPartGroupBackupTask::check_before_do_task()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (OB_FAIL(check_partition_validation())) {
    STORAGE_LOG(WARN, "failed to check_partition_validation", K(ret));
  }

  if (OB_FAIL(ret)) {
    ObTaskController::get().allow_next_syslog();
    first_error_code_ = ret;
    STORAGE_LOG(WARN, "failed to check task, mark fail", K(ret), K(*this));
  }
  return ret;
}

int ObPartGroupBackupTask::do_backup_task(const ObBackupDataType& backup_data_type)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_finished = false;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObTaskController::get().switch_task(share::ObTaskType::DATA_MAINTAIN);
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("start pg group backup task", K(*this));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {

    while (!is_finished) {
      if (OB_SUCCESS != (tmp_ret = check_is_task_cancel())) {
        STORAGE_LOG(WARN, "failed to check is task cancel", K(tmp_ret));
      }

      if (OB_SUCCESS != (tmp_ret = try_schedule_new_partition_backup(backup_data_type))) {
        STORAGE_LOG(WARN, "failed to try_schedule_new_partition_migration", K(tmp_ret));
      }

      if (OB_SUCCESS != (tmp_ret = try_finish_group_backup(is_finished))) {
        STORAGE_LOG(WARN, "failed to try_finish_group_backup", K(tmp_ret));
      }

      if (!is_finished) {
        share::dag_yield();
        ObThreadCondGuard guard(cond_);
        if (need_idle_) {
          if (OB_SUCCESS != (tmp_ret = cond_.wait(PART_GROUP_TASK_IDLE_TIME_MS))) {
            if (OB_TIMEOUT != tmp_ret) {
              STORAGE_LOG(WARN, "failed to idle", K(tmp_ret));
            }
          }
        }
        need_idle_ = true;
      }
    }
  }

  ObTaskController::get().allow_next_syslog();
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("group backup task finish", K(cost_ts), K(task_list_), K(backup_data_type));
  return ret;
}

int ObPartGroupBackupTask::do_part_group_backup_minor_task()
{
  int ret = OB_SUCCESS;
  ObBackupDataType backup_data_type;
  int first_error_code = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (FALSE_IT(reset_tasks_status())) {
  } else if (FALSE_IT(backup_data_type.set_minor_data_backup())) {
  } else if (OB_FAIL(do_backup_task(backup_data_type))) {
    STORAGE_LOG(WARN, "failed to do backup task", K(ret));
  } else {
    {
      common::SpinRLockGuard guard(lock_);
      first_error_code = first_error_code_;
    }

    if (OB_SUCCESS != first_error_code) {
      ret = first_error_code;
      STORAGE_LOG(WARN, "first error code has set, set failed", K(ret), K(first_error_code));
    }
  }
  return ret;
}

int ObPartGroupBackupTask::do_part_group_backup_major_task()
{
  int ret = OB_SUCCESS;
  ObBackupDataType backup_data_type;
  int first_error_code = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (FALSE_IT(reset_tasks_status())) {
  } else if (FALSE_IT(backup_data_type.set_major_data_backup())) {
  } else if (OB_FAIL(do_backup_task(backup_data_type))) {
    STORAGE_LOG(WARN, "failed to do backup task", K(ret));
  } else {
    {
      common::SpinRLockGuard guard(lock_);
      first_error_code = first_error_code_;
    }

    if (OB_SUCCESS != first_error_code) {
      ret = first_error_code;
      STORAGE_LOG(WARN, "first error code has set, set failed", K(ret), K(first_error_code));
    }
  }
  return ret;
}

int ObPartGroupBackupTask::set_task_list_result(
    const int32_t first_error_code, const common::ObIArray<ObPartMigrationTask>& task_list)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (task_list_.count() != task_list.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup task list num is not equal", K(ret), K(task_list_.count()), K(task_list.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list.count(); ++i) {
      const ObPartMigrationTask& tmp_task = task_list.at(i);
      ObPartMigrationTask& task = task_list_.at(i);
      if (tmp_task.arg_.key_ != task.arg_.key_ || tmp_task.arg_.type_ != task.arg_.type_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("backup task is not same", K(ret), K(task), K(tmp_task));
      } else {
        task.during_migrating_ = tmp_task.during_migrating_;
        task.need_reset_migrate_status_ = tmp_task.need_reset_migrate_status_;
        task.result_ = tmp_task.result_;
        task.status_ = tmp_task.status_;
      }
    }

    if (OB_SUCC(ret)) {
      common::SpinWLockGuard guard(lock_);
      if (OB_SUCCESS == first_error_code_) {
        first_error_code_ = first_error_code;
        STORAGE_LOG(WARN, "set first_error_code_", K(ret));
      }
    }
  }
  return ret;
}

int ObPartGroupBackupTask::finish_group_backup_task()
{
  int ret = OB_SUCCESS;
  int first_error_code = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    {
      common::SpinRLockGuard guard(lock_);
      first_error_code = first_error_code_;
    }
    SMART_VAR(ObReportPartMigrationTask, tmp_task)
    {
      if (OB_FAIL(report_list_.reserve(task_list_.count()))) {
        STORAGE_LOG(WARN, "failed to reserve report list", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
        ObPartMigrationTask& sub_task = task_list_[i];
        if (ObIPartMigrationTask::INIT == sub_task.status_) {
          sub_task.result_ = first_error_code;
        }
        tmp_task.arg_ = sub_task.arg_;
        tmp_task.status_ = ObIPartMigrationTask::FINISH;
        tmp_task.result_ = sub_task.result_;
        tmp_task.need_report_checksum_ = sub_task.ctx_.need_report_checksum_;
        tmp_task.ctx_ = &sub_task.ctx_;
        if (OB_FAIL(report_list_.push_back(tmp_task))) {
          // report_list is reserved before, should not fail here
          STORAGE_LOG(ERROR, "failed to add report list", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        is_finished_ = true;
      }
    }
  }
  return ret;
}

int ObPartGroupBackupTask::do_backup_pg_metas()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  // backup meta
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (OB_UNLIKELY(BACKUP_REPLICA_OP != type_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "type must be backup", K(ret), K(type_));
  } else if (FALSE_IT(reset_tasks_status())) {
  } else {
    common::SpinRLockGuard guard(lock_);
    SMART_VAR(ObBackupMetaWriter, meta_writer)
    {
      common::ObInOutBandwidthThrottle* throttle = ObPartitionMigrator::get_instance().get_bandwidth_throttle();
      if (OB_ISNULL(throttle)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "BandwidthThrottle should not be null here", K(ret));
      } else if (OB_FAIL(meta_writer.open(*throttle, task_list_))) {
        STORAGE_LOG(WARN, "init meta writer fail", K(ret));
      } else {
        if (OB_FAIL(meta_writer.process())) {
          STORAGE_LOG(WARN, "write meta data fail", K(ret));
        }

        if (OB_SUCCESS != (tmp_ret = meta_writer.close())) {
          LOG_WARN("failed to close meta writer", K(tmp_ret), K(ret));
          if (OB_SUCC(ret)) {
            ret = tmp_ret;
          }
        }
      }
    }
  }
  return ret;
}

int ObPartGroupBackupTask::try_schedule_new_partition_backup(const ObBackupDataType& backup_data_type)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "part group backup task do not init", K(ret));
  } else if (!backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "try schedule new partition backup get invalid argument", K(ret), K(backup_data_type));
  } else {
    {
      common::SpinRLockGuard guard(lock_);
      if (OB_SUCCESS != first_error_code_) {
        ret = first_error_code_;
        STORAGE_LOG(WARN, "first_error_code_ is set, skip schedule new partition backup", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(try_schedule_partition_backup(backup_data_type))) {
        STORAGE_LOG(WARN, "failed to try schedule partition backup", K(ret));
      }
    }
  }
  return ret;
}

int ObPartGroupBackupTask::try_schedule_partition_backup(const ObBackupDataType& backup_data_type)
{
  int ret = OB_SUCCESS;
  bool need_schedule = true;
  ObPartMigrationTask* task = NULL;
  int64_t data_backup_concurrency = GCONF.backup_concurrency;
  int32_t up_limit = 0;
  if (OB_FAIL(ObDagScheduler::get_instance().get_up_limit(ObIDag::DAG_ULT_BACKUP, up_limit))) {
    STORAGE_LOG(WARN, "failed to get up limit", K(ret));
  } else {
    data_backup_concurrency = data_backup_concurrency == 0 ? up_limit : data_backup_concurrency;
    FLOG_INFO("backup concurrency", K(data_backup_concurrency), K(up_limit));
  }

  while (OB_SUCC(ret) && need_schedule &&
         ObDagScheduler::get_instance().get_dag_count(ObIDag::DAG_TYPE_BACKUP) < data_backup_concurrency) {
    if (OB_FAIL(inner_schedule_partition(task, need_schedule))) {
      STORAGE_LOG(WARN, "failed to inner schedule partition", K(ret));
    } else if (!need_schedule) {
      // do nothing
    } else if (OB_ISNULL(task)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "task need schedule but is NULL", K(ret), KP(task));
    } else if (OB_FAIL(schedule_backup_dag(backup_data_type, task->ctx_))) {
      STORAGE_LOG(WARN, "failed to schedule backup dag", K(ret), K(task->arg_));
    }
  }
  return ret;
}

int ObPartGroupBackupTask::inner_schedule_partition(ObPartMigrationTask*& task, bool& need_schedule)
{
  int ret = OB_SUCCESS;
  need_schedule = false;
  task = NULL;
  common::SpinRLockGuard guard(lock_);
  for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
    task = &task_list_.at(i);
    bool as_data_source = false;
    if (type_ != task->arg_.type_ || !is_replica_op_valid(task->arg_.type_)) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "invalid type, cannot handle this task", K(ret), K(i), K(*task));
    } else if (REMOVE_REPLICA_OP == type_) {
      // do nothing
      // TODO() remove replica op backup need cancel all task and notice rs need retry
    } else if (OB_FAIL(check_can_as_data_source(type_,
                   task->arg_.data_src_.get_replica_type(),
                   task->arg_.dst_.get_replica_type(),
                   as_data_source))) {
      STORAGE_LOG(WARN, "fail to check can as data source", K(ret), K(task->arg_));
    } else if (!as_data_source) {
      ret = OB_NOT_SUPPORTED;
      first_error_code_ = ret;
      LOG_ERROR("invalid data src", K(ret), K(task->arg_));
    } else if (ObPartMigrationTask::INIT == task->status_) {
      if (OB_FAIL(build_migrate_ctx(task->arg_, task->ctx_))) {
        STORAGE_LOG(WARN, "fail to build migrate ctx", K(ret));
      } else {
        need_schedule = true;
        break;
      }
    }
  }
  return ret;
}

// caller must not hold wlock
int ObPartGroupBackupTask::schedule_backup_dag(const ObBackupDataType& backup_data_type, ObMigrateCtx& migrate_ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(check_partition_checksum(migrate_ctx.replica_op_arg_.key_))) {
    STORAGE_LOG(WARN,
        "failed to check partition checksum, cannot schedule backup",
        K(ret),
        K(migrate_ctx.replica_op_arg_.key_));
  } else if (OB_FAIL(migrate_ctx.generate_and_schedule_backup_dag(backup_data_type))) {
    STORAGE_LOG(WARN, "failed to generate backup dag", K(ret));
  }

  if (OB_FAIL(ret)) {
    share::ObTaskId fake_task_id;
    const bool during_migrating = false;
    const int64_t fake_backup_set_id = 0;
    if (OB_SUCCESS !=
        (tmp_ret = set_part_task_finish(
             migrate_ctx.replica_op_arg_.key_, ret, fake_task_id, during_migrating, fake_backup_set_id))) {
      LOG_WARN("failed to finish part task", K(ret));
    }
  }

  return ret;
}

int ObPartGroupBackupTask::try_finish_group_backup(bool& is_finished)
{
  int ret = OB_SUCCESS;
  bool is_sub_task_finish = true;
  report_list_.reset();
  is_finished = false;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);

    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObPartMigrationTask& task = task_list_.at(i);
      if (ObPartMigrationTask::FINISH != task.status_) {
        if (OB_SUCCESS != first_error_code_ && ObPartMigrationTask::INIT == task.status_) {
          task.status_ = ObPartMigrationTask::FINISH;
          task.result_ = first_error_code_;
        } else {
          is_sub_task_finish = false;
        }
      } else if (OB_SUCCESS != task.result_) {
        if (OB_SUCCESS == first_error_code_) {
          first_error_code_ = OB_ERR_SYS;
          STORAGE_LOG(ERROR, "sub task has error , but first_error_code_ is succ, use OB_ERR_SYS", K(*this));
        }
      }
    }

    if (OB_SUCC(ret) && is_sub_task_finish) {
      is_finished = true;
    }
  }
  return ret;
}

void ObPartGroupBackupTask::reset_tasks_status()
{
  common::SpinWLockGuard guard(lock_);
  for (int64_t i = 0; i < task_list_.count(); ++i) {
    ObPartMigrationTask& task = task_list_.at(i);
    task.ctx_.rebuild_migrate_ctx();
    task.status_ = ObPartMigrationTask::INIT;
    task.result_ = OB_SUCCESS;
    task.need_reset_migrate_status_ = false;
    task.during_migrating_ = false;
  }
}

// TODO() wait for backup point created
int ObPartGroupBackupTask::check_all_pg_backup_point_created()
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObTaskController::get().allow_next_syslog();
  LOG_INFO("start check all pg backup point created", K(*this));
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    for (int64_t i = 0; OB_SUCC(ret) && i < task_list_.count(); ++i) {
      ObPartMigrationTask& task = task_list_.at(i);
      const ObPartitionKey& pkey = task.arg_.key_;
      const int64_t backup_snapshot_version = task.arg_.backup_arg_.backup_snapshot_version_;
      if (OB_FAIL(check_pg_backup_point_created(pkey, backup_snapshot_version))) {
        LOG_WARN("failed to check pg backup point created", K(ret), K(pkey), K(backup_snapshot_version));
      }
    }
  }

  ObTaskController::get().allow_next_syslog();
  const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
  LOG_INFO("end check all pg backup point created", K(cost_ts), K(task_list_.count()));
  return ret;
}

int ObPartGroupBackupTask::check_pg_backup_point_created(
    const ObPartitionKey& pg_key, const int64_t backup_snapshot_version)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool is_exist = false;
  // TODO() need wakeup minor merge

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!pg_key.is_valid() || backup_snapshot_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(
        WARN, "check pg backup point created get invalid argument", K(ret), K(pg_key), K(backup_snapshot_version));
  } else {
    while (OB_SUCC(ret)) {
      // check_backup_point exist
      if (ObPartGroupMigrator::get_instance().is_stop()) {
        ret = OB_SERVER_IS_STOPPING;
        STORAGE_LOG(WARN, "server is stopping", K(ret));
      } else {
        ObIPartitionGroupGuard guard;
        ObIPartitionGroup* partition_group = NULL;
        if (OB_FAIL(ObPartitionService::get_instance().get_partition(pg_key, guard))) {
          LOG_WARN("failed to get partition guard", K(ret), K(pg_key));
        } else if (OB_ISNULL(partition_group = guard.get_partition_group())) {
          ret = OB_ERR_SYS;
          LOG_ERROR("partition must not null", K(ret), K(pg_key));
        } else if (OB_FAIL(partition_group->get_pg_storage().get_recovery_data_mgr().check_backup_point_exist(
                       backup_snapshot_version, is_exist))) {
          LOG_WARN("failed to check backup point exist", K(ret), K(pg_key));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (is_exist) {
        break;
      } else {
        if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {  // 10s
          LOG_INFO("backup point is not exist, need wait", K(backup_snapshot_version), K(pg_key));
        }
        share::dag_yield();
        ObThreadCondGuard guard(cond_);
        if (need_idle_) {
          if (OB_SUCCESS != (tmp_ret = cond_.wait(PART_GROUP_BACKUP_POINT_CHECK_TIME_MS))) {
            if (OB_TIMEOUT != tmp_ret) {
              STORAGE_LOG(WARN, "failed to idle", K(tmp_ret));
            }
          }
        }
        need_idle_ = true;
      }
    }
  }
  return ret;
}

ObBackupPrepareTask::ObBackupPrepareTask()
    : ObITask(TASK_TYPE_MIGRATE_PREPARE),
      is_inited_(false),
      ctx_(NULL),
      cp_fty_(NULL),
      bandwidth_throttle_(NULL),
      partition_service_(NULL),
      backup_meta_reader_(NULL)
{}

ObBackupPrepareTask::~ObBackupPrepareTask()
{
  if (NULL != cp_fty_) {
    if (NULL != backup_meta_reader_) {
      cp_fty_->free(backup_meta_reader_);
    }
  }
}

int ObBackupPrepareTask::init(ObIPartitionComponentFactory& cp_fty,
    common::ObInOutBandwidthThrottle& bandwidth_throttle, ObPartitionService& partition_service)
{
  int ret = OB_SUCCESS;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot init twice", K(ret));
  } else if (ObIDag::DAG_TYPE_BACKUP != dag_->get_type()) {
    ret = OB_ERR_SYS;
    LOG_ERROR("dag type not match", K(ret), K(*dag_));
  } else {
    ctx_ = static_cast<ObMigrateDag*>(dag_)->get_ctx();
    is_inited_ = true;
    bandwidth_throttle_ = &bandwidth_throttle;
    partition_service_ = &partition_service;
    cp_fty_ = &cp_fty;
  }
  return ret;
}

int ObBackupPrepareTask::prepare_backup_reader()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(ERROR, "not inited", K(ret));
  } else if (BACKUP_REPLICA_OP == ctx_->replica_op_arg_.type_) {
    if (OB_ISNULL(ctx_->backup_meta_reader_ = cp_fty_->get_partition_group_meta_backup_reader())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "pg meta reader should not be NULL", K(ret));
    } else if (OB_FAIL(ctx_->backup_meta_reader_->init(ctx_->pg_meta_, ctx_->replica_op_arg_.backup_arg_))) {
      STORAGE_LOG(WARN, "fail to init backup meta reader", K(ret));
    }
  }
  return ret;
}

int ObBackupPrepareTask::process()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObITask* last_task = nullptr;
  if (NULL != ctx_) {
    ctx_->action_ = ObMigrateCtx::PREPARE;
    if (NULL != dag_) {
      ctx_->task_id_ = dag_->get_dag_id();
    }

    if (OB_SUCCESS != (tmp_ret = (ctx_->trace_id_array_.push_back(*ObCurTraceId::get_trace_id())))) {
      STORAGE_LOG(WARN, "failed to push back trace id to array", K(tmp_ret));
    }
    LOG_INFO("start ObBackupPrepareTask process", "arg", ctx_->replica_op_arg_);
  }

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(add_backup_status(ctx_))) {
    LOG_WARN("failed to add backup status", K(ret));
  } else if (OB_FAIL(prepare_backup())) {
    LOG_WARN("failed to prepare backup", K(ret));
  } else if (OB_FAIL(schedule_backup_tasks())) {
    LOG_WARN("failed to schedule backup tasks", K(ret), K(*ctx_));
  }

  if (OB_SUCC(ret)) {
    ObTaskController::get().allow_next_syslog();
    LOG_INFO("finish prepare task", K(*ctx_));
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    if (ctx_->is_need_retry(ret)) {
      ctx_->need_rebuild_ = true;
      LOG_INFO("migrate prepare task need retry", K(ret), K(*ctx_));
    }
    ctx_->set_result_code(ret);
  }

  LOG_INFO("end ObBackupPrepareTask process", K(ret));
  return ret;
}

int ObBackupPrepareTask::prepare_backup()
{
  int ret = OB_SUCCESS;
  const bool is_write_lock = true;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else {
    ObMigrateCtxGuard guard(is_write_lock, *ctx_);

    ctx_->use_slave_safe_read_ts_ = false;  // disable logical migrate
    if (!ctx_->is_valid()) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "invalid migrate ctx", K(ret), K(*this), K(*ctx_));
    } else if (1 != ctx_->doing_task_cnt_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "doing task must be 1 during prepare action", K(ret), K(*this), K(*ctx_));
    } else if (REMOVE_REPLICA_OP == ctx_->replica_op_arg_.type_) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(ERROR, "should not prepare migrate for remove replica", K(ret), K(ctx_->replica_op_arg_));
    } else if (OB_FAIL(build_backup_prepare_context())) {
      LOG_WARN("failed to build backup prepare context", K(ret));
    } else if (OB_FAIL(prepare_backup_reader())) {
      LOG_WARN("failed to prepare backup reader", K(ret));
    } else if (OB_FAIL(check_backup_data_continues())) {
      LOG_WARN("failed to check backup data continues", K(ret));
    } else if (OB_FAIL(build_backup_pg_partition_info())) {
      LOG_WARN("failed to build backup pg partition info", K(ret), K(*ctx_));
    } else {
      ctx_->need_rebuild_ = false;
      ObTaskController::get().allow_next_syslog();
      STORAGE_LOG(INFO, "finish prepare backup", "pkey", ctx_->replica_op_arg_.key_, K(*ctx_));
    }

    if (OB_FAIL(ret)) {
      if (NULL != ctx_) {
        ctx_->result_ = ret;
      }
    }
  }
  return ret;
}

int ObBackupPrepareTask::add_backup_status(ObMigrateCtx* ctx)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (NULL == ctx) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arg", K(ret));
  } else if (!ctx->is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(*ctx));
  } else if (OB_FAIL(add_partition_backup_status(*ctx))) {
    STORAGE_LOG(WARN, "failed to add partition_backup_status", K(ret), K(*ctx));
  } else {
    STORAGE_LOG(INFO, "succeed to add backup ctx", KP(ctx), K(*ctx));
  }
  return ret;
}

int ObBackupPrepareTask::add_partition_backup_status(const ObMigrateCtx& ctx)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret));
  } else if (!ctx.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), K(ctx));
  } else {
    ObPartitionMigrationStatus task;
    task.task_id_ = ctx.task_id_;
    task.migrate_type_ = ctx.replica_op_arg_.get_replica_op_type_str();
    task.pkey_ = ctx.replica_op_arg_.key_;
    task.clog_parent_.reset();  // we don't know it now
    task.src_.reset();          // we don't know it now
    task.dest_ = OBSERVER.get_self();
    task.result_ = ctx.result_;
    task.start_time_ = ctx.create_ts_;
    task.action_ = ctx.action_;
    task.replica_state_ = OB_UNKNOWN_REPLICA;  // we don't know it now
    task.doing_task_count_ = 0;
    task.total_task_count_ = 0;
    task.rebuild_count_ = 0;
    task.continue_fail_count_ = 0;
    task.data_statics_ = ctx.data_statics_;

    // allow comment truncation, no need to set ret
    (void)ctx.fill_comment(task.comment_, sizeof(task.comment_));

    if (OB_SUCCESS != (tmp_ret = ObPartitionMigrationStatusMgr::get_instance().add_status(task))) {
      STORAGE_LOG(WARN, "failed to add partition migration status", K(tmp_ret), K(task));
    }
  }

  return ret;
}

int ObBackupPrepareTask::build_backup_prepare_context()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition = NULL;
  ObPGStorage* pg_storage = NULL;
  int64_t trans_table_end_log_ts = 0;
  int64_t trans_table_timestamp = 0;
  ObPartitionGroupMeta local_pg_meta;
  ObPartGroupMigrationTask* group_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_NOT_NULL(partition = ctx_->get_partition())) {
    // do nothing
    pg_storage = &(partition->get_pg_storage());
    LOG_INFO("partition already hold, do not hold again");
  } else if (OB_FAIL(ObPartitionService::get_instance().get_partition(
                 ctx_->replica_op_arg_.key_, ctx_->partition_guard_))) {
    LOG_WARN("partition is not exist, cannot backup partition", K(ret), K(*ctx_));
  } else if (OB_ISNULL(partition = ctx_->partition_guard_.get_partition_group()) ||
             OB_ISNULL(pg_storage = &(partition->get_pg_storage()))) {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "partition must not null", K(ret));
  } else if (OB_FAIL(pg_storage->get_backup_pg_meta_data(
                 ctx_->replica_op_arg_.backup_arg_.backup_snapshot_version_, ctx_->pg_meta_))) {
    STORAGE_LOG(WARN, "failed to get backup pg meta data", K(ret), K(ctx_->replica_op_arg_));
  } else if (!ctx_->pg_meta_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "backup ctx pg meta is invalid", K(ret), K(*ctx_));
  } else if (OB_FAIL(
                 pg_storage->get_trans_table_end_log_ts_and_timestamp(trans_table_end_log_ts, trans_table_timestamp))) {
    LOG_WARN("failed to get trans table end_log_ts and timestamp", K(ret), K(ctx_->replica_op_arg_));
  } else {
    ctx_->local_publish_version_ = ctx_->pg_meta_.storage_info_.get_data_info().get_publish_version();
    ctx_->local_last_replay_log_id_ = ctx_->pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();
    ctx_->local_last_replay_log_ts_ =
        std::max(trans_table_end_log_ts, ctx_->pg_meta_.storage_info_.get_data_info().get_last_replay_log_ts());
    ctx_->migrate_src_info_.src_addr_ = ctx_->replica_op_arg_.data_src_.get_server();
    ctx_->migrate_src_info_.cluster_id_ = ctx_->replica_op_arg_.cluster_id_;
  }

  if (OB_FAIL(ret) && NULL != ctx_) {
    ctx_->can_rebuild_ = false;
    LOG_WARN("failed during build backup prepare context, set cannot rebuild", K(ret), K(*ctx_));
  }
  return ret;
}

int ObBackupPrepareTask::build_backup_pg_partition_info()
{
  int ret = OB_SUCCESS;
  ObIPGPartitionBaseDataMetaObReader* reader = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_partition_table_info_backup_reader(ctx_->migrate_src_info_, reader))) {
    LOG_WARN("failed to get partition info reader", K(ret), K(ctx_));
  } else if (OB_FAIL(build_table_partition_info(ctx_->replica_op_arg_, reader))) {
    LOG_WARN("failed to build table partition info", K(ret), K(ctx_->replica_op_arg_));
  }

  if (NULL != reader) {
    cp_fty_->free(reader);
  }
  return ret;
}

int ObBackupPrepareTask::build_backup_partition_info(const ObPGPartitionMetaInfo& partition_meta_info,
    const common::ObIArray<obrpc::ObFetchTableInfoResult>& table_info_res,
    const common::ObIArray<uint64_t>& table_id_list, ObPartitionMigrateCtx& part_migrate_ctx)
{
  int ret = OB_SUCCESS;
  ObMigrateTableInfo table_info;
  int64_t cost_ts = ObTimeUtility::current_time();
  const ObPartitionKey& pkey = partition_meta_info.meta_.pkey_;
  ObMigratePartitionInfo& info = part_migrate_ctx.copy_info_;
  part_migrate_ctx.ctx_ = ctx_;
  DEBUG_SYNC(BEFORE_BUILD_MIGRATE_PARTITION_INFO);

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!partition_meta_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build backup partition info get invalid argument", K(ret), K(partition_meta_info));
  } else if (OB_FAIL(info.meta_.deep_copy(partition_meta_info.meta_))) {
    LOG_WARN("fail to deep copy partition store meta", K(ret), K(partition_meta_info.meta_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < table_info_res.count(); ++i) {
      table_info.reuse();
      const obrpc::ObFetchTableInfoResult& table_res = table_info_res.at(i);
      const uint64_t table_id = table_id_list.at(i);
      LOG_INFO("build_backup_partition_info for table", "table_id", table_id);

      if (OB_FAIL(build_backup_table_info(table_id, pkey, table_res, table_info))) {
        LOG_WARN("failed to build backup table info", K(ret));
      } else if (OB_FAIL(info.table_id_list_.push_back(table_id))) {
        LOG_WARN("failed to push table id into array", K(ret));
      } else if (OB_FAIL(info.table_infos_.push_back(table_info))) {
        LOG_WARN("failed to add backup table info", K(ret));
      } else {
        LOG_INFO("add table info", K(table_info));
      }
    }
  }
  cost_ts = ObTimeUtility::current_time() - cost_ts;
  LOG_INFO("build_backup_partition_info",
      K(cost_ts),
      "pkey",
      info.meta_.pkey_,
      "src",
      info.src_,
      "table_count",
      info.table_id_list_.count());
  return ret;
}

int ObBackupPrepareTask::build_backup_table_info(const uint64_t table_id, const ObPartitionKey& pkey,
    const obrpc::ObFetchTableInfoResult& result, ObMigrateTableInfo& info)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObITable::TableKey>& local_tables_info = result.table_keys_;
  int64_t need_reserve_major_snapshot;
  info.reuse();
  info.table_id_ = table_id;
  ObBackupDag* backup_dag = NULL;
  ObBackupDataType backup_data_type;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_ISNULL(backup_dag = reinterpret_cast<ObBackupDag*>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get backup dag", K(ret));
  } else if (FALSE_IT(backup_data_type = backup_dag->get_backup_data_type())) {
  } else if (backup_data_type.is_major_backup()) {
    if (OB_FAIL(build_backup_major_sstable(local_tables_info, info.major_sstables_))) {
      LOG_WARN("failed to build backup major sstable", K(ret), K(table_id), K(pkey));
    }
  } else if (backup_data_type.is_minor_backup()) {
    if (OB_FAIL(build_backup_minor_sstable(local_tables_info, info.minor_sstables_))) {
      LOG_WARN("failed to build backup minor sstable", K(ret), K(table_id), K(pkey));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dag type is unexpected", K(ret), K(*ctx_), K(backup_data_type));
  }

  if (OB_FAIL(ret)) {
  } else {
    info.multi_version_start_ = result.multi_version_start_;
    info.ready_for_read_ = result.is_ready_for_read_;
    FLOG_INFO("succeed build_backup_table_info", K(result), K(info), K(pkey), K(table_id));
  }
  return ret;
}

int ObBackupPrepareTask::schedule_backup_tasks()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool need_schedule = false;
  ObArray<ObITask*> last_task_array;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_pg_backup_tasks(last_task_array))) {
    LOG_WARN("failed to generate backup task", K(ret));
  } else {
    if (OB_SUCCESS != (tmp_ret = ctx_->update_partition_migration_status())) {
      LOG_WARN("failed to update partition migration status", K(tmp_ret));
    }
  }
  return ret;
}

int ObBackupPrepareTask::build_backup_major_sstable(
    const ObIArray<ObITable::TableKey>& local_tables, ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup prepare task do not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < local_tables.count(); ++i) {
      const ObITable::TableKey& local_table = local_tables.at(i);
      if (local_table.is_major_sstable()) {
        ObMigrateTableInfo::SSTableInfo info;
        info.src_table_key_ = local_table;
        info.dest_base_version_ = local_table.get_base_version();
        info.dest_log_ts_range_ = local_table.log_ts_range_;
        if (OB_FAIL(copy_sstables.push_back(info))) {
          LOG_WARN("failed to push backup table into array", K(ret), K(local_table), K(copy_sstables));
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("build_backup_major_sstable", K(copy_sstables), K(local_tables));
    }
  }
  return ret;
}

int ObBackupPrepareTask::build_backup_minor_sstable(
    const ObIArray<ObITable::TableKey>& local_tables, ObIArray<ObMigrateTableInfo::SSTableInfo>& copy_sstables)
{
  int ret = OB_SUCCESS;
  ObArray<ObMigrateTableInfo::SSTableInfo> tmp_copy_sstables;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else {
    ObMigrateTableInfo::SSTableInfo info;
    for (int64_t i = 0; OB_SUCC(ret) && i < local_tables.count(); ++i) {
      const ObITable::TableKey& local_table = local_tables.at(i);
      if (local_table.is_minor_sstable()) {
        info.reset();
        info.src_table_key_ = local_table;
        info.dest_base_version_ = local_table.trans_version_range_.base_version_;
        info.dest_log_ts_range_ = local_table.log_ts_range_;
        if (OB_FAIL(copy_sstables.push_back(info))) {
          LOG_WARN("failed to push backup table into array", K(ret), K(local_table), K(copy_sstables));
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("build_backup_minor_sstable", K(copy_sstables), K(local_tables));
    }
  }
  return ret;
}

int ObBackupPrepareTask::generate_pg_backup_tasks(ObIArray<ObITask*>& last_task_array)
{
  int ret = OB_SUCCESS;
  ObITask* last_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (BACKUP_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unexcepted op type, ", K(ret), K(ctx_->replica_op_arg_));
  } else if (!ObReplicaTypeCheck::is_replica_with_ssstore(ctx_->replica_op_arg_.src_.get_replica_type())) {
    ret = OB_REPLICA_CANNOT_BACKUP;
    LOG_WARN("replica has no sstore.", K(ret), K(ctx_->replica_op_arg_));
  } else if (OB_FAIL(generate_backup_tasks(last_task))) {
    LOG_WARN("fail to generate backup tasks", K(ret));
  } else if (OB_FAIL(last_task_array.push_back(last_task))) {
    LOG_WARN("fail to push last task into last task array", K(ret));
  }
  return ret;
}

int ObBackupPrepareTask::generate_backup_tasks(ObITask*& last_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask* wait_backup_finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_wait_backup_finish_task(wait_backup_finish_task))) {
    LOG_WARN("failed to generate_wait_backup_finish_task", K(ret));
  } else if (OB_ISNULL(wait_backup_finish_task)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("wait_rebuild_finish_task must not null", K(ret));
  } else if (OB_FAIL(generate_backup_tasks(*wait_backup_finish_task))) {
    LOG_WARN("failed to generate_backup_tasks", K(ret));
  }

  if (OB_SUCC(ret)) {
    last_task = wait_backup_finish_task;
  }
  return ret;
}

int ObBackupPrepareTask::generate_backup_tasks(ObFakeTask& wait_backup_finish_task)
{
  int ret = OB_SUCCESS;
  ObITask* parent_task = this;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(generate_backup_sstable_tasks(parent_task))) {
    LOG_WARN("failed to generate backup major tasks", K(ret));
  } else if (OB_FAIL(parent_task->add_child(wait_backup_finish_task))) {
    LOG_WARN("failed to add wait_migrate_finish_task", K(ret));
  }
  return ret;
}

int ObBackupPrepareTask::generate_backup_sstable_tasks(share::ObITask*& parent_task)
{
  int ret = OB_SUCCESS;
  ObFakeTask* wait_finish_task = NULL;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (OB_ISNULL(parent_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(parent_task));
  } else if (OB_FAIL(dag_->alloc_task(wait_finish_task))) {
    LOG_WARN("failed to alloc wait finish task", K(ret));
  } else if (OB_FAIL(generate_backup_sstable_copy_task(parent_task, wait_finish_task))) {
    LOG_WARN("failed to generate major sstable copy task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_finish_task))) {
    LOG_WARN("failed to add wait finish task", K(ret));
  } else {
    parent_task = wait_finish_task;
    LOG_DEBUG("succeed to generate_backup_sstable_task");
  }
  return ret;
}

int ObBackupPrepareTask::generate_backup_sstable_copy_task(ObITask* parent_task, ObITask* child_task)
{
  int ret = OB_SUCCESS;
  ObBackupCopyPhysicalTask* copy_task = NULL;
  ObBackupFinishTask* finish_task = NULL;
  const int64_t task_idx = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(parent_task) || OB_ISNULL(child_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(parent_task), KP(child_task));
  } else if (OB_FAIL(dag_->alloc_task(finish_task))) {
    LOG_WARN("failed to alloc finish task", K(ret));
  } else if (OB_FAIL(build_backup_physical_ctx(ctx_->physical_backup_ctx_))) {
    LOG_WARN("failed to build physical sstable ctx", K(ret));
  } else if (OB_FAIL(finish_task->init(*ctx_))) {
    LOG_WARN("failed to init finish task", K(ret));
  } else if (OB_FAIL(finish_task->add_child(*child_task))) {
    LOG_WARN("failed to add child", K(ret));
  }

  if (OB_SUCC(ret)) {
    if (ctx_->physical_backup_ctx_.get_task_count() > 0) {
      // parent->copy->finish->child
      if (OB_FAIL(dag_->alloc_task(copy_task))) {
        LOG_WARN("failed to alloc copy task", K(ret));
      } else if (OB_FAIL(copy_task->init(task_idx, *ctx_))) {
        LOG_WARN("failed to init copy task", K(ret));
      } else if (OB_FAIL(parent_task->add_child(*copy_task))) {
        LOG_WARN("failed to add child copy task", K(ret));
      } else if (OB_FAIL(copy_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish task", K(ret));
      } else if (OB_FAIL(dag_->add_task(*copy_task))) {
        LOG_WARN("failed to add copy task to dag", K(ret));
      }
    } else {
      // parent->finish->child
      if (OB_FAIL(parent_task->add_child(*finish_task))) {
        LOG_WARN("failed to add child finish_task for parent", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dag_->add_task(*finish_task))) {
      LOG_WARN("failed to add finish task to dag", K(ret));
    }
  }
  return ret;
}

int ObBackupPrepareTask::build_backup_physical_ctx(ObBackupPhysicalPGCtx& ctx)
{
  int ret = OB_SUCCESS;
  ObBackupDataType backup_data_type;
  ObBackupDag* backup_dag = NULL;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(backup_dag = reinterpret_cast<ObBackupDag*>(get_dag()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get backup dag", K(ret));
  } else if (FALSE_IT(backup_data_type = backup_dag->get_backup_data_type())) {
  } else if (OB_FAIL(ctx.open(
                 *bandwidth_throttle_, ctx_->replica_op_arg_.backup_arg_, ctx_->pg_meta_.pg_key_, backup_data_type))) {
    LOG_WARN("ctx init failed", K(ret));
  } else if (OB_FAIL(fetch_backup_sstables(backup_data_type, ctx.table_keys_))) {
    LOG_WARN("failed to build sub task", K(ret));
  } else if (ctx.table_keys_.empty()) {
    LOG_INFO("pg don't have sstable, ", K(ret), K(ctx_->pg_meta_));
  } else if (OB_FAIL(build_backup_sub_task(ctx))) {
    LOG_WARN("failed to build sub task", K(ret));
  } else {
    ATOMIC_AAF(&ctx_->data_statics_.total_macro_block_, ctx.macro_block_count_);
    LOG_INFO("succeed to build physical sstable ctx", K(ctx));
  }

  return ret;
}

int ObBackupPrepareTask::fetch_backup_sstables(
    const ObBackupDataType& backup_data_type, ObIArray<ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (!backup_data_type.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fetch backup sstables get invalid argument", K(ret), K(backup_data_type));
  } else if (backup_data_type.is_major_backup()) {
    if (OB_FAIL(fetch_backup_major_sstables(table_keys))) {
      LOG_WARN("failed to fetch backup major sstables", K(ret));
    }
  } else {
    // backup minor
    if (OB_FAIL(fetch_backup_minor_sstables(table_keys))) {
      LOG_WARN("failed to fetch backup major sstables", K(ret));
    }
  }

  return ret;
}

int ObBackupPrepareTask::fetch_backup_major_sstables(ObIArray<ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else {
    // partition
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx& part_migrate_ctx = ctx_->part_ctx_array_.at(i);
      if (OB_LIKELY(!part_migrate_ctx.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_migrate_ctx is invalid", K(ret), K(part_migrate_ctx));
      } else {
        // tables
        const ObArray<ObMigrateTableInfo>& table_infos = part_migrate_ctx.copy_info_.table_infos_;
        for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
          const ObMigrateTableInfo& table_info = table_infos.at(i);
          // major sstables
          for (int64_t sstable_idx = 0; OB_SUCC(ret) && sstable_idx < table_info.major_sstables_.count();
               ++sstable_idx) {
            const ObITable::TableKey& major_table_key = table_info.major_sstables_.at(sstable_idx).src_table_key_;
            if (OB_UNLIKELY(!major_table_key.is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid major table key", K(ret), K(major_table_key));
            } else if (OB_UNLIKELY(!ObITable::is_major_sstable(major_table_key.table_type_))) {
              ret = OB_ERR_SYS;
              LOG_ERROR("table type is not major sstable", K(ret), K(major_table_key), K(table_info));
            } else if (OB_FAIL(table_keys.push_back(major_table_key))) {
              LOG_WARN("failed to push major table key into array", K(ret), K(major_table_key));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupPrepareTask::fetch_backup_minor_sstables(ObIArray<ObITable::TableKey>& table_keys)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else {
    // partition
    for (int64_t i = 0; OB_SUCC(ret) && i < ctx_->part_ctx_array_.count(); ++i) {
      ObPartitionMigrateCtx& part_migrate_ctx = ctx_->part_ctx_array_.at(i);
      if (OB_LIKELY(!part_migrate_ctx.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("part_migrate_ctx is invalid", K(ret), K(part_migrate_ctx));
      } else {
        // tables
        const ObArray<ObMigrateTableInfo>& table_infos = part_migrate_ctx.copy_info_.table_infos_;
        for (int64_t i = 0; OB_SUCC(ret) && i < table_infos.count(); ++i) {
          const ObMigrateTableInfo& table_info = table_infos.at(i);
          // minor sstables
          for (int64_t sstable_idx = 0; OB_SUCC(ret) && sstable_idx < table_info.minor_sstables_.count();
               ++sstable_idx) {
            const ObITable::TableKey& minor_table_key = table_info.minor_sstables_.at(sstable_idx).src_table_key_;
            if (OB_UNLIKELY(!minor_table_key.is_valid())) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("invalid major table key", K(ret), K(minor_table_key));
            } else if (OB_UNLIKELY(!ObITable::is_minor_sstable(minor_table_key.table_type_))) {
              ret = OB_ERR_SYS;
              LOG_ERROR("table type is not minor sstable", K(ret), K(minor_table_key), K(table_info));
            } else if (OB_FAIL(table_keys.push_back(minor_table_key))) {
              LOG_WARN("failed to push major table key into array", K(ret), K(minor_table_key));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObBackupPrepareTask::build_backup_sub_task(ObBackupPhysicalPGCtx& ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < ctx.table_keys_.count(); ++i) {
      ObTableHandle tmp_handle;
      ObSSTable* sstable = NULL;
      int64_t sstable_macro_count = 0;
      ObITable::TableKey& table_key = ctx.table_keys_.at(i);
      if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key, tmp_handle))) {
        STORAGE_LOG(WARN, "failed to get table", K(table_key), K(ret));
      } else if (OB_FAIL(tmp_handle.get_sstable(sstable))) {
        STORAGE_LOG(WARN, "failed to get table", K(table_key), K(ret));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "sstable should not be null", K(ret));
      } else if (0 == (sstable_macro_count = sstable->get_meta().get_total_macro_block_count())) {
        // empty sstable
      } else {
        ObBackupMacroBlockInfo block_info;
        block_info.table_key_ = table_key;
        block_info.start_index_ = 0;
        block_info.cur_block_count_ = sstable_macro_count;
        block_info.total_block_count_ = sstable_macro_count;
        if (OB_FAIL(ctx.add_backup_macro_block_info(block_info))) {
          STORAGE_LOG(WARN, "add backup block info failed", K(ret), K(block_info));
        }
      }
    }
  }
  return ret;
}

int ObBackupPrepareTask::generate_wait_backup_finish_task(ObFakeTask*& wait_backup_finish_task)
{
  int ret = OB_SUCCESS;
  wait_backup_finish_task = NULL;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not inited", K(ret));
  } else if (OB_FAIL(dag_->alloc_task(wait_backup_finish_task))) {
    LOG_WARN("failed to alloc wait_rebuild_finish_task", K(ret));
  } else if (OB_FAIL(add_child(*wait_backup_finish_task))) {
    LOG_WARN("failed to add child wait_rebuild_finish_task", K(ret));
  } else if (OB_FAIL(dag_->add_task(*wait_backup_finish_task))) {
    LOG_WARN("failed to add wait_rebuild_finish_task", K(ret));
  }
  return ret;
}

int ObBackupPrepareTask::build_table_partition_info(
    const ObReplicaOpArg& arg, ObIPGPartitionBaseDataMetaObReader* reader)
{
  int ret = OB_SUCCESS;
  ObPartitionMigrateCtx part_migrate_ctx;
  ObArray<obrpc::ObFetchTableInfoResult> table_info_res;
  ObArray<uint64_t> table_id_list;
  ObPGPartitionMetaInfo partition_meta_info;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (!arg.is_valid() || OB_ISNULL(reader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build table partition info get invalid argument", K(ret), K(arg), KP(reader));
  } else {
    while (OB_SUCC(ret)) {
      partition_meta_info.reset();
      table_info_res.reuse();
      table_id_list.reuse();
      part_migrate_ctx.reset();
      if (OB_FAIL(reader->fetch_pg_partition_meta_info(partition_meta_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to fetch pg partition meta info", K(ret), K(ctx_->replica_op_arg_));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(table_id_list.assign(partition_meta_info.table_id_list_))) {
        LOG_WARN("failed to assign table id list", K(ret));
      } else if (OB_FAIL(table_info_res.assign(partition_meta_info.table_info_))) {
        LOG_WARN("failed to copy needed table info", K(ret), K(partition_meta_info));
      } else if (OB_FAIL(build_backup_partition_info(
                     partition_meta_info, table_info_res, table_id_list, part_migrate_ctx))) {
        LOG_WARN("fail to build migrate partition info", K(ret), K(partition_meta_info));
      } else if (OB_FAIL(ctx_->part_ctx_array_.push_back(part_migrate_ctx))) {
        LOG_WARN("fail to push part migrate ctx into array", K(ret), K(part_migrate_ctx));
      }
    }
  }
  return ret;
}

int ObBackupPrepareTask::get_partition_table_info_backup_reader(
    const ObMigrateSrcInfo& src_info, ObIPGPartitionBaseDataMetaObReader*& reader)
{
  int ret = OB_SUCCESS;
  UNUSED(src_info);
  ObPGPartitionBaseDataMetaBackupReader* tmp_reader = NULL;

  if (OB_ISNULL(cp_fty_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("cp fty should not be NULL", K(ret), KP(cp_fty_));
  } else if (OB_ISNULL(tmp_reader = cp_fty_->get_pg_info_backup_reader())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to get macro block ob reader", K(ret));
  } else if (OB_ISNULL(ctx_->backup_meta_reader_)) {
    ret = OB_ERR_SYS;
    LOG_ERROR("ctx_->backup_meta_reader_ must not null", K(ret));
  } else if (OB_FAIL(tmp_reader->init(ctx_->pg_meta_.partitions_, ctx_->backup_meta_reader_))) {
    LOG_WARN("fail to init partition table info restore reader", K(ret));
  } else {
    reader = tmp_reader;
    tmp_reader = NULL;
  }

  if (NULL != cp_fty_) {
    if (NULL != tmp_reader) {
      cp_fty_->free(tmp_reader);
      tmp_reader = NULL;
    }
  }
  return ret;
}

int ObBackupPrepareTask::check_backup_data_continues()
{
  int ret = OB_SUCCESS;
  ObIPartitionGroup* partition_group = NULL;
  clog::ObPGLogArchiveStatus pg_log_archive_status;
  ObLogArchiveBackupInfo archive_backup_info;
  const int64_t start_ts = ObTimeUtility::current_time();
  static const int64_t OB_MAX_RETRY_TIME = 60 * 1000 * 1000;  // 60s
  static const int64_t OB_SINGLE_SLEEP_US = 1 * 1000 * 1000;  // 1s
  const int64_t last_replay_log_id = ctx_->pg_meta_.storage_info_.get_clog_info().get_last_replay_log_id();

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("migrate prepare task do not init", K(ret));
  } else if (BACKUP_REPLICA_OP != ctx_->replica_op_arg_.type_) {
    // do nothing
  } else if (OB_ISNULL(partition_group = ctx_->get_partition())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup local partition should not be NULL", K(ret), KP(partition_group), K(*ctx_));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_FAIL(partition_group->get_log_archive_status(pg_log_archive_status))) {
        LOG_WARN("failed to get log archive status", K(ret), K(*ctx_));
      } else if (OB_FAIL(ObBackupInfoMgr::get_instance().get_log_archive_backup_info(archive_backup_info))) {
        LOG_WARN("failed to get log archive backup info", K(ret));
      } else if (ObLogArchiveStatus::INVALID == pg_log_archive_status.status_) {
        if (ObLogArchiveStatus::DOING == archive_backup_info.status_.status_) {
          LOG_WARN("partition group log archive status is invalid, need retry",
              K(pg_log_archive_status),
              K(archive_backup_info));
          ret = OB_EAGAIN;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN(
              "server do not archive log, can not backup", K(ret), K(pg_log_archive_status), K(archive_backup_info));
        }
      } else if (ObLogArchiveStatus::DOING == pg_log_archive_status.status_) {
        if (pg_log_archive_status.archive_incarnation_ != archive_backup_info.status_.incarnation_ ||
            pg_log_archive_status.log_archive_round_ != archive_backup_info.status_.round_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("pg archive round or incarnation not equal to server archive info",
              K(ret),
              K(pg_log_archive_status),
              K(archive_backup_info));
        } else if (pg_log_archive_status.round_start_log_id_ < 1) {
          ret = OB_LOG_ARCHIVE_STAT_NOT_MATCH;
          LOG_WARN("log archive status is not match", K(ret), K(pg_log_archive_status));
        } else if (last_replay_log_id + 1 < pg_log_archive_status.round_start_log_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("base data and log archive data do not continues",
              K(ret),
              K(pg_log_archive_status),
              K(ctx_->pg_meta_),
              K(last_replay_log_id));
        } else {
          break;
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("log archive status is unexpected", K(ret), K(pg_log_archive_status));
      }

      if (OB_EAGAIN == ret) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        if (cost_ts < OB_MAX_RETRY_TIME) {
          usleep(OB_SINGLE_SLEEP_US);
          ret = OB_SUCCESS;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("server start do archive log, but partition retry many times status still wrong",
              K(ret),
              K(pg_log_archive_status),
              K(archive_backup_info),
              K(*ctx_));
        }
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

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

#define USING_LOG_PREFIX RS

#include "observer/ob_server_struct.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_root_service.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "rootserver/ddl_task/ob_column_redefinition_task.h"
#include "rootserver/ddl_task/ob_constraint_task.h"
#include "rootserver/ddl_task/ob_ddl_retry_task.h"
#include "rootserver/ddl_task/ob_ddl_scheduler.h"
#include "rootserver/ddl_task/ob_ddl_task.h"
#include "rootserver/ddl_task/ob_drop_index_task.h"
#include "rootserver/ddl_task/ob_drop_primary_key_task.h"
#include "rootserver/ddl_task/ob_index_build_task.h"
#include "rootserver/ddl_task/ob_modify_autoinc_task.h"
#include "rootserver/ddl_task/ob_table_redefinition_task.h"
#include "share/ob_ddl_common.h"
#include "share/ob_rpc_struct.h"
#include "share/scheduler/ob_sys_task_stat.h"

namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace common;

namespace rootserver
{

ObDDLTaskQueue::ObDDLTaskQueue()
  : task_list_(), task_map_(), lock_(), is_inited_(false)
{
}

ObDDLTaskQueue::~ObDDLTaskQueue()
{
}

void ObDDLTaskQueue::destroy()
{
  task_map_.destroy();
  task_id_map_.destroy();
  is_inited_ = false;
}

int ObDDLTaskQueue::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObDDLTaskQueue has already been inited", K(ret));
  } else if (bucket_num <= 0) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_map_.create(bucket_num, lib::ObLabel("DdlQue")))) {
    LOG_WARN("fail to create task set", K(ret), K(bucket_num));
  } else if (OB_FAIL(task_id_map_.create(bucket_num, lib::ObLabel("DdlQue")))) {
    LOG_WARN("fail to create task set", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDDLTaskQueue::push_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  bool task_add_to_list = false;
  bool task_add_to_map = false;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else if (!task_list_.add_last(task)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "unexpected error, add build index task failed", K(ret));
  } else {
    int is_overwrite = 0; // do not overwrite
    task_add_to_list = true;
    if (OB_FAIL(task_map_.set_refactored(task->get_task_key(), task, is_overwrite))) {
      if (common::OB_HASH_EXIST == ret) {
        ret = common::OB_ENTRY_EXIST;
      } else {
        LOG_WARN("fail to set task to task set", K(ret));
      }
    } else {
      task_add_to_map = true;
      if (OB_FAIL(task_id_map_.set_refactored(task->get_task_id(), task, is_overwrite))) {
        if (common::OB_HASH_EXIST == ret) {
          ret = common::OB_ENTRY_EXIST;
        } else {
          LOG_WARN("set task to task set", K(ret));
        }
      }
      LOG_INFO("add task", K(*task), KP(task), K(common::lbt()));
    }
  }
  if (OB_FAIL(ret) && task_add_to_list) {
    int tmp_ret = OB_SUCCESS;
    if (!task_list_.remove(task)) {
      tmp_ret = common::OB_ERR_UNEXPECTED;
      LOG_WARN("fail to remove task", K(tmp_ret), K(*task));
    }
    if (task_add_to_map) {
      if (OB_SUCCESS != (tmp_ret = task_map_.erase_refactored(task->get_task_key()))) {
        LOG_WARN("erase from task map failed", K(tmp_ret));
      }
    }
  }
  return ret;
}

int ObDDLTaskQueue::get_next_task(ObDDLTask *&task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (0 == task_list_.get_size()) {
    ret = common::OB_ENTRY_NOT_EXIST;
  } else if (OB_ISNULL(task = task_list_.remove_first())) {
    ret = common::OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, task must not be NULL", K(ret));
  }
  return ret;
}

int ObDDLTaskQueue::remove_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (OB_FAIL(task_map_.erase_refactored(task->get_task_key()))) {
    LOG_WARN("fail to erase from task set", K(ret));
  } else {
    LOG_INFO("succ to remove task", K(*task), KP(task));
  }
  if (OB_SUCCESS != (tmp_ret = task_id_map_.erase_refactored(task->get_task_id()))) {
    LOG_WARN("erase task from map failed", K(ret));
    ret = OB_SUCCESS == ret ? tmp_ret : ret;
  }
  return ret;
}

int ObDDLTaskQueue::add_task_to_last(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_ISNULL(task)) {
    ret = common::OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(task));
  } else if (!task_list_.add_last(task)) {
    ret = common::OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "error unexpected, fail to move task to last", K(ret));
  }
  return ret;
}

int ObDDLTaskQueue::get_task(const ObDDLTaskKey &task_key, ObDDLTask *&task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_key));
  } else if (OB_FAIL(task_map_.get_refactored(task_key, task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_key));
  }
  return ret;
}

int ObDDLTaskQueue::get_task(const int64_t task_id, ObDDLTask *&task)
{
  int ret = OB_SUCCESS;
  common::ObSpinLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    LOG_WARN("ObDDLTaskQueue has not been inited", K(ret));
  } else if (OB_UNLIKELY(task_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(task_id));
  } else if (OB_FAIL(task_id_map_.get_refactored(task_id, task))) {
    ret = OB_HASH_NOT_EXIST == ret ? OB_ENTRY_NOT_EXIST : ret;
    LOG_WARN("get from task map failed", K(ret), K(task_id));
  }
  return ret;
}

int ObDDLScheduler::DDLScanTask::schedule(int tg_id)
{
  return TG_SCHEDULE(tg_id, *this, DDL_TASK_SCAN_PERIOD, true);
}

void ObDDLScheduler::DDLScanTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ddl_scheduler_.recover_task())) {
    LOG_WARN("failed to recover ddl tasks", K(ret));
  }
}

ObDDLScheduler::ObDDLScheduler()
  : is_inited_(false), is_started_(false), tg_id_(-1), root_service_(nullptr), idle_stop_(false), idler_(idle_stop_), scan_task_(*this)
{

}

ObDDLScheduler::~ObDDLScheduler()
{

}

int ObDDLScheduler::init(ObRootService *root_service)
{
  static const int64_t MAX_TASK_NUM = 10000;
  static const int64_t DDL_TASK_MEMORY_LIMIT = 1024L * 1024L * 1024L;
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(root_service));
  } else if (OB_FAIL(allocator_.init(
      OB_MALLOC_BIG_BLOCK_SIZE, "ddl_task", OB_SYS_TENANT_ID, DDL_TASK_MEMORY_LIMIT))) {
    LOG_WARN("init allocator failed", K(ret));
  } else if (OB_FAIL(task_queue_.init(MAX_TASK_NUM))) {		
    LOG_WARN("init task queue failed", K(ret));
  } else if (OB_FAIL(TG_CREATE(lib::TGDefIDs::DDLTaskExecutor3, tg_id_))) {
    LOG_WARN("tg create failed", K(ret));
  } else {
    root_service_ = root_service;
    is_inited_ = true;
  }
  return ret;
}

int ObDDLScheduler::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_started_) {
    // do nothing
  } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
    LOG_WARN("tg set runnable failed", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("start ddl task scheduler failed", K(ret));
  } else if (OB_FAIL(TG_START(lib::TGDefIDs::DDLScanTask))) {
    LOG_WARN("start ddl scan task failed", K(ret));
  } else if (OB_FAIL(scan_task_.schedule(lib::TGDefIDs::DDLScanTask))) {
    LOG_WARN("failed to schedule ddl scan task", K(ret));
  } else {
    is_started_ = true;
    idle_stop_ = false;
  }
  return ret;
}

void ObDDLScheduler::stop()
{
  TG_STOP(tg_id_);
  TG_STOP(lib::TGDefIDs::DDLScanTask);
  idle_stop_ = true;
  is_started_ = false;
  destroy_all_tasks();
}

void ObDDLScheduler::wait()
{
  TG_WAIT(tg_id_);
  TG_WAIT(lib::TGDefIDs::DDLScanTask);
}

void ObDDLScheduler::run1()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    int ret = OB_SUCCESS;
    ObDDLTask *task = nullptr;
    ObDDLTask *first_retry_task = nullptr;
    (void)prctl(PR_SET_NAME, "DDLTaskExecutor", 0, 0, 0);
    while (!has_set_stop()) {
      THIS_WORKER.set_worker_level(1);
      THIS_WORKER.set_curr_request_level(1);
      while (!has_set_stop()) {
        if (OB_FAIL(task_queue_.get_next_task(task))) {
          if (common::OB_ENTRY_NOT_EXIST == ret) {
            break;
          } else {
            LOG_WARN("fail to get next task", K(ret));
            break;
          }
        } else if (OB_ISNULL(task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("error unexpected, task must not be NULL", K(ret));
        } else if (task == first_retry_task || !task->need_schedule()) {
          // add the task back to the queue
          if (OB_FAIL(task_queue_.add_task_to_last(task))) {
            STORAGE_LOG(ERROR, "fail to add task to last", K(ret), K(*task));
          }
          break;
        } else {
          ObCurTraceId::set(task->get_trace_id());
          int task_ret = task->process();
          task->calc_next_schedule_ts(task_ret);
          if (task->need_retry() && !has_set_stop()) {
            if (OB_FAIL(task_queue_.add_task_to_last(task))) {
              STORAGE_LOG(ERROR, "fail to add task to last, which should not happen", K(ret), K(*task));
            }
            first_retry_task = nullptr == first_retry_task ? task : first_retry_task;
          } else {
            if (OB_FAIL(task_queue_.remove_task(task))) {
              LOG_WARN("fail to remove task, which should not happen", K(ret), K(*task), KP(task));
            } else {
              remove_sys_task(task);
              free_ddl_task(task);
            }
          }
        }
      }
      first_retry_task = nullptr;
      idler_.idle(100 * 1000L);
    }
  }
}

int ObDDLScheduler::create_ddl_task(const ObCreateDDLTaskParam &param,
                                    ObISQLClient &proxy,
                                    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  task_record.reset();
  const obrpc::ObAlterTableArg *alter_table_arg = nullptr;
  const obrpc::ObCreateIndexArg *create_index_arg = nullptr;
  const obrpc::ObDropIndexArg *drop_index_arg = nullptr;
  ObRootService *root_service = GCTX.root_service_;
  LOG_INFO("create ddl task", K(param));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(root_service)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, root service must not be nullptr", K(ret));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(param));
  } else {
    switch (param.type_) {
      case DDL_CREATE_INDEX:
        create_index_arg = static_cast<const obrpc::ObCreateIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_build_index_task(proxy,
                                            param.src_table_schema_,
                                            param.dest_table_schema_,
                                            param.parallelism_,
                                            param.parent_task_id_,
                                            create_index_arg,
                                            *param.allocator_,
                                            task_record))) {
          LOG_WARN("fail to create build index task", K(ret));
        }
        break;
      case DDL_DROP_INDEX:
        // in this case, src_table_schema is data table, dest_table_schema is index table
        drop_index_arg = static_cast<const obrpc::ObDropIndexArg *>(param.ddl_arg_);
        if (OB_FAIL(create_drop_index_task(proxy,
                                           param.src_table_schema_,
                                           param.parent_task_id_,
                                           drop_index_arg,
                                           *param.allocator_,
                                           task_record))) {
          LOG_WARN("fail to create drop index task failed", K(ret));
        }
        break;
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_TABLE_REDEFINITION:
        if (OB_FAIL(create_table_redefinition_task(proxy,
                                                   param.type_,
                                                   param.src_table_schema_,
                                                   param.dest_table_schema_,
                                                   param.parallelism_,
                                                   static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                   *param.allocator_,
                                                   task_record))) {
          LOG_WARN("fail to create table redefinition task", K(ret));
        }
        break;
      case DDL_DROP_PRIMARY_KEY:
        alter_table_arg = static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_);
        if (OB_FAIL(create_drop_primary_key_task(proxy,
                                                 param.type_,
                                                 param.src_table_schema_,
                                                 param.dest_table_schema_,
                                                 param.parallelism_,
                                                 alter_table_arg,
                                                 *param.allocator_,
                                                 task_record))) {
          LOG_WARN("fail to create table redefinition task", K(ret));
        }
        break;
      case DDL_CHECK_CONSTRAINT:
      case DDL_FOREIGN_KEY_CONSTRAINT:
      case DDL_ADD_NOT_NULL_COLUMN:
        if (OB_FAIL(create_constraint_task(proxy,
                                           param.src_table_schema_,
                                           param.object_id_,
                                           param.type_,
                                           param.schema_version_,
                                           static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                           param.parent_task_id_,
                                           *param.allocator_,
                                           task_record))) {
          LOG_WARN("fail to create constraint task failed", K(ret));
        }
        break;
      case DDL_DROP_COLUMN:
      case DDL_ADD_COLUMN_OFFLINE:
      case DDL_COLUMN_REDEFINITION:
        if (OB_FAIL(create_column_redefinition_task(proxy,
                                                    param.type_,
                                                    param.src_table_schema_,
                                                    param.dest_table_schema_,
                                                    param.parallelism_,
                                                    static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                                    *param.allocator_,
                                                    task_record))) {
          LOG_WARN("fail to create column redefinition task", K(ret));
        }
        break;
      case DDL_MODIFY_AUTO_INCREMENT:
        if (OB_FAIL(create_modify_autoinc_task(proxy,
                                               param.tenant_id_,
                                               param.src_table_schema_->get_table_id(),
                                               param.schema_version_,
                                               static_cast<const obrpc::ObAlterTableArg *>(param.ddl_arg_),
                                               *param.allocator_,
                                               task_record))) {
          LOG_WARN("fail to create modify autoinc task", K(ret));
        }
        break;
      case DDL_DROP_DATABASE:
      case DDL_DROP_TABLE:
      case DDL_TRUNCATE_TABLE:
      case DDL_DROP_PARTITION:
      case DDL_DROP_SUB_PARTITION:
      case DDL_TRUNCATE_PARTITION:
      case DDL_TRUNCATE_SUB_PARTITION: {
        if (OB_FAIL(create_ddl_retry_task(proxy,
                                          param.tenant_id_,
                                          param.object_id_,
                                          param.schema_version_,
                                          param.type_,
                                          param.ddl_arg_,
                                          *param.allocator_,
                                          task_record))) {
          LOG_WARN("fail to create ddl retry task", K(ret));
        }
        break;
      }
      default:
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("error unexpected, ddl type is not supported", K(ret), K(param.type_));
    }
    LOG_INFO("create ddl task", K(param), K(task_record));
  }
  return ret;
}

int ObDDLScheduler::add_sys_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else {
    const int64_t parent_task_id = task->get_parent_task_id();
    const uint64_t tenant_id = task->get_tenant_id();
    if (0 == parent_task_id) {
      share::ObSysTaskStat sys_task_status;
      sys_task_status.start_time_ = ObTimeUtility::fast_current_time();
      sys_task_status.task_id_ = *ObCurTraceId::get_trace_id();
      sys_task_status.tenant_id_ = tenant_id;
      sys_task_status.task_type_ = DDL_TASK;
      if (OB_FAIL(SYS_TASK_STATUS_MGR.add_task(sys_task_status))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("sys task already exist", K(sys_task_status.task_id_));
        } else {
          LOG_WARN("add task failed", K(ret));
        }
      } else {
        task->set_sys_task_id(sys_task_status.task_id_);
        LOG_INFO("add sys task", K(sys_task_status.task_id_));
      }
    }
  }

  return ret;
}

int ObDDLScheduler::remove_sys_task(ObDDLTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(task));
  } else {
    const ObCurTraceId::TraceId &task_id = task->get_sys_task_id();
    if (!task_id.is_invalid()) {
      if (OB_FAIL(SYS_TASK_STATUS_MGR.del_task(task_id))) {
        LOG_WARN("del task failed", K(ret), K(task_id));
      } else {
        LOG_INFO("remove sys task", K(task_id));
      }
    }
  }
  return ret;
}

int ObDDLScheduler::create_build_index_task(
    common::ObISQLClient &proxy,
    const ObTableSchema *data_table_schema,
    const ObTableSchema *index_schema,
    const int64_t parallelism,
    const int64_t parent_task_id,
    const obrpc::ObCreateIndexArg *create_index_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObIndexBuildTask index_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(create_index_arg) || OB_ISNULL(data_table_schema) || OB_ISNULL(index_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(create_index_arg), K(data_table_schema), K(index_schema));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(index_task.init(data_table_schema->get_tenant_id(),
                                     task_id,
                                     data_table_schema,
                                     index_schema,
                                     index_schema->get_schema_version(),
                                     parallelism,
                                     *create_index_arg,
                                     parent_task_id))) {
    LOG_WARN("init global index task failed", K(ret), K(data_table_schema), K(index_schema));
  } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }

  LOG_INFO("ddl_scheduler create build index task finished", K(ret), K(index_task));
  return ret;
}

int ObDDLScheduler::create_drop_index_task(
    common::ObISQLClient &proxy,
    const share::schema::ObTableSchema *index_schema,
    const int64_t parent_task_id,
    const obrpc::ObDropIndexArg *drop_index_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropIndexTask index_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_ISNULL(index_schema) || OB_ISNULL(drop_index_arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(index_schema), KP(drop_index_arg));
  } else if (index_schema->is_domain_index()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("drop domain index is not supported", K(ret));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else {
    const uint64_t data_table_id = index_schema->get_data_table_id();
    const uint64_t index_table_id = index_schema->get_table_id();
    if (OB_FAIL(index_task.init(index_schema->get_tenant_id(),
                                task_id,
                                data_table_id,
                                index_table_id,
                                index_schema->get_schema_version(),
                                parent_task_id,
                                *drop_index_arg))) {
      LOG_WARN("init drop index task failed", K(ret), K(data_table_id), K(index_table_id));
    } else if (OB_FAIL(index_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
      LOG_WARN("set trace id failed", K(ret));
    } else if (OB_FAIL(insert_task_record(proxy, index_task, allocator, task_record))) {
      LOG_WARN("fail to insert task record", K(ret));
    }
  }
  LOG_INFO("ddl_scheduler create drop index task finished", K(ret), K(index_task));
  return ret;
}
int ObDDLScheduler::create_constraint_task(
    common::ObISQLClient &proxy,
    const share::schema::ObTableSchema *table_schema,
    const int64_t constraint_id,
    const ObDDLType ddl_type,
    const int64_t schema_version,
    const obrpc::ObAlterTableArg *arg,
    const int64_t parent_task_id,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObConstraintTask constraint_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(nullptr == table_schema || OB_INVALID_ID == constraint_id || schema_version <= 0
                         || nullptr == arg || !arg->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table_schema), K(constraint_id), K(schema_version), K(arg));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(constraint_task.init(task_id, table_schema, constraint_id, ddl_type, schema_version, *arg, parent_task_id))) {
    LOG_WARN("init constraint task failed", K(ret), K(table_schema), K(constraint_id));
  } else if (OB_FAIL(constraint_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, constraint_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create constraint task finished", K(ret), K(constraint_task));
  return ret;
}

int ObDDLScheduler::create_table_redefinition_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const obrpc::ObAlterTableArg *alter_table_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObTableRedefinitionTask, redefinition_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(alter_table_arg), KP(src_schema), KP(dest_schema));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(redefinition_task.init(src_schema->get_tenant_id(),
                                            task_id,
                                            type,
                                            src_schema->get_table_id(),
                                            dest_schema->get_table_id(),
                                            dest_schema->get_schema_version(),
                                            parallelism,
                                            *alter_table_arg))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(redefinition_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, redefinition_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create table redefinition task finished", K(ret), K(redefinition_task), K(common::lbt()));
  }
  return ret;
}

int ObDDLScheduler::create_drop_primary_key_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const ObTableSchema *src_schema,
    const ObTableSchema *dest_schema,
    const int64_t parallelism,
    const obrpc::ObAlterTableArg *alter_table_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObDropPrimaryKeyTask, drop_pk_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(alter_table_arg), KP(src_schema), KP(dest_schema));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(drop_pk_task.init(src_schema->get_tenant_id(),
                                       task_id,
                                       type,
                                       src_schema->get_table_id(),
                                       dest_schema->get_table_id(),
                                       dest_schema->get_schema_version(),
                                       parallelism,
                                       *alter_table_arg))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(drop_pk_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, drop_pk_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create drop primary key task finished", K(ret), K(drop_pk_task));
  }
  return ret;
}

int ObDDLScheduler::create_column_redefinition_task(
    common::ObISQLClient &proxy,
    const share::ObDDLType &type,
    const share::schema::ObTableSchema *src_schema,
    const share::schema::ObTableSchema *dest_schema,
    const int64_t parallelism,
    const obrpc::ObAlterTableArg *alter_table_arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  int64_t task_id = 0;
  SMART_VAR(ObColumnRedefinitionTask, redefinition_task) {
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_ISNULL(alter_table_arg) || OB_ISNULL(src_schema) || OB_ISNULL(dest_schema)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(alter_table_arg), KP(src_schema), KP(dest_schema));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(redefinition_task.init(src_schema->get_tenant_id(),
                                            task_id,
                                            type,
                                            src_schema->get_table_id(),
                                            dest_schema->get_table_id(),
                                            dest_schema->get_schema_version(),
                                            parallelism,
                                            *alter_table_arg))) {
    LOG_WARN("fail to init redefinition task", K(ret));
  } else if (OB_FAIL(redefinition_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, redefinition_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create column redefinition task finished", K(ret), K(redefinition_task));
  }
  return ret;
}

int ObDDLScheduler::create_modify_autoinc_task(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t table_id,
    const int64_t schema_version,
    const obrpc::ObAlterTableArg *arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObModifyAutoincTask modify_autoinc_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id
                         || schema_version <= 0 || nullptr == arg || !arg->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(schema_version), K(arg));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(modify_autoinc_task.init(tenant_id, task_id, table_id, schema_version, *arg))) {
    LOG_WARN("init global index task failed", K(ret), K(table_id), K(arg));
  } else if (OB_FAIL(modify_autoinc_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, modify_autoinc_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create modify autoinc task finished", K(ret), K(modify_autoinc_task));
  return ret;
}

// for drop database, drop table, drop partition, drop subpartition, 
// truncate table, truncate partition and truncate sub partition.
int ObDDLScheduler::create_ddl_retry_task(
    common::ObISQLClient &proxy,
    const uint64_t tenant_id,
    const uint64_t object_id,
    const int64_t schema_version,
    const share::ObDDLType &type,
    const obrpc::ObDDLArg *arg,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDDLRetryTask ddl_retry_task;
  int64_t task_id = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == object_id
                         || schema_version <= 0) || OB_ISNULL(arg)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(object_id), K(schema_version), K(arg));
  } else if (OB_FAIL(ObDDLTask::fetch_new_task_id(root_service_->get_sql_proxy(), task_id))) {
    LOG_WARN("fetch new task id failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task.init(tenant_id, task_id, object_id, schema_version, type, arg))) {
    LOG_WARN("init ddl retry task failed", K(ret), K(arg));
  } else if (OB_FAIL(ddl_retry_task.set_trace_id(*ObCurTraceId::get_trace_id()))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(insert_task_record(proxy, ddl_retry_task, allocator, task_record))) {
    LOG_WARN("fail to insert task record", K(ret));
  }
  LOG_INFO("ddl_scheduler create ddl retry task finished", K(ret), K(ddl_retry_task));
  return ret;
}

int ObDDLScheduler::insert_task_record(
    common::ObISQLClient &proxy,
    ObDDLTask &ddl_task,
    ObIAllocator &allocator,
    ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  task_record.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!ddl_task.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_task));
  } else if (OB_FAIL(ddl_task.convert_to_record(task_record, allocator))) {
    LOG_WARN("convert to ddl task record failed", K(ret), K(ddl_task));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::insert_record(proxy, task_record))) {
    if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
      ret = OB_ENTRY_EXIST;
    } else {
      LOG_WARN("insert ddl task record failed", K(ret), K(task_record));
    }
  }
  return ret;
}

int ObDDLScheduler::recover_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObArray<ObDDLTaskRecord> task_records;
    ObArenaAllocator allocator(lib::ObLabel("DdlTasRecord"));
    share::schema::ObMultiVersionSchemaService &schema_service = root_service_->get_schema_service();
    if (OB_FAIL(ObDDLTaskRecordOperator::get_all_record(root_service_->get_sql_proxy(), allocator, task_records))) {
      LOG_WARN("get all task records failed", K(ret));
    }
    LOG_INFO("start processing ddl recovery", K(task_records));
    for (int64_t i = 0; OB_SUCC(ret) && i < task_records.count(); ++i) {
      const ObDDLTaskRecord &cur_record = task_records.at(i);
      int64_t tenant_schema_version = 0;
      int64_t table_task_status = 0;
      int64_t execution_id = 0;
      ObMySQLTransaction trans;
      if (OB_FAIL(schema_service.get_tenant_schema_version(cur_record.tenant_id_, tenant_schema_version))) {
        LOG_WARN("failed to get tenant schema version", K(ret), K(cur_record));
      } else if (tenant_schema_version < cur_record.schema_version_) {
        // schema has not publish, by pass now
      } else if (OB_FAIL(trans.start(&root_service_->get_sql_proxy(), cur_record.tenant_id_))) {
        LOG_WARN("start transaction failed", K(ret));
      } else if (OB_FAIL(ObDDLTaskRecordOperator::select_for_update(trans,
                                                                    cur_record.tenant_id_,
                                                                    cur_record.task_id_,
                                                                    table_task_status,
                                                                    execution_id))) {
        LOG_WARN("select for update failed", K(ret), K(cur_record));
      } else if (OB_FAIL(schedule_ddl_task(cur_record))) {
        LOG_WARN("failed to schedule ddl task", K(ret), K(cur_record));
      }
      bool commit = (OB_SUCCESS == ret);
      int tmp_ret = trans.end(commit);
      if (OB_SUCCESS != tmp_ret) {
        ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

int ObDDLScheduler::schedule_ddl_task(const ObDDLTaskRecord &record)
{
  int ret = OB_SUCCESS;
  ObTraceIDGuard guard(record.trace_id_);
  if (OB_UNLIKELY(!record.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ddl task record is invalid", K(ret), K(record));
  } else {
    switch (record.ddl_type_) {
      case ObDDLType::DDL_CREATE_INDEX: {
        if (OB_FAIL(schedule_build_index_task(record))) {
          LOG_WARN("schedule global index task failed", K(ret), K(record));
        }
        break;
      }
      case ObDDLType::DDL_DROP_INDEX:
        if (OB_FAIL(schedule_drop_index_task(record))) {
          LOG_WARN("schedule drop index task failed", K(ret));
        }
        break;
      case DDL_DROP_PRIMARY_KEY:
        if (OB_FAIL(schedule_drop_primary_key_task(record))) {
          LOG_WARN("schedule drop primary key task failed", K(ret));
        }
        break;
      case DDL_MODIFY_COLUMN:
      case DDL_ADD_PRIMARY_KEY:
      case DDL_ALTER_PRIMARY_KEY:
      case DDL_ALTER_PARTITION_BY:
      case DDL_CONVERT_TO_CHARACTER:
      case DDL_TABLE_REDEFINITION:
        if (OB_FAIL(schedule_table_redefinition_task(record))) {
          LOG_WARN("schedule table redefinition task failed", K(ret));
        }
        break;
      case DDL_DROP_COLUMN:
      case DDL_ADD_COLUMN_OFFLINE:
      case DDL_COLUMN_REDEFINITION:
        if(OB_FAIL(schedule_column_redefinition_task(record))) {
          LOG_WARN("schedule column redefinition task failed", K(ret));
        }
        break;

      case DDL_CHECK_CONSTRAINT:
      case DDL_FOREIGN_KEY_CONSTRAINT:
      case DDL_ADD_NOT_NULL_COLUMN:
        if (OB_FAIL(schedule_constraint_task(record))) {
          LOG_WARN("schedule constraint task failed", K(ret));
        }
        break;
      case DDL_MODIFY_AUTO_INCREMENT:
        if (OB_FAIL(schedule_modify_autoinc_task(record))) {
          LOG_WARN("schedule modify autoinc task failed", K(ret));
        }
        break;
      case DDL_DROP_DATABASE:
      case DDL_DROP_TABLE:
      case DDL_TRUNCATE_TABLE:
      case DDL_DROP_PARTITION:
      case DDL_DROP_SUB_PARTITION:
      case DDL_TRUNCATE_PARTITION:
      case DDL_TRUNCATE_SUB_PARTITION:
        if (OB_FAIL(schedule_ddl_retry_task(record))) {
          LOG_WARN("schedule ddl retry task failed", K(ret));
        }
        break;
      default: {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not supported task type", K(ret), K(record));
        break;
      }
    }
    LOG_INFO("schedule ddl task", K(ret), K(record));
  }
  return ret;
}

int ObDDLScheduler::schedule_build_index_task(
    const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObIndexBuildTask *build_index_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(build_index_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else {
    if (OB_FAIL(build_index_task->init(task_record))) {
      LOG_WARN("init global_index_task failed", K(ret), K(task_record));
    } else if (OB_FAIL(build_index_task->set_trace_id(task_record.trace_id_))) {
      LOG_WARN("init build index task failed", K(ret));
    } else if (OB_FAIL(inner_schedule_ddl_task(build_index_task))) {
      if (OB_ENTRY_EXIST != ret) {
        LOG_WARN("inner schedule task failed", K(ret), K(*build_index_task));
      }
    }
  }
  if (OB_FAIL(ret) && nullptr != build_index_task) {
    build_index_task->~ObIndexBuildTask();
    allocator_.free(build_index_task);
    build_index_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_drop_primary_key_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropPrimaryKeyTask *drop_pk_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(drop_pk_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(drop_pk_task->ObTableRedefinitionTask::init(task_record))) {
    LOG_WARN("init drop primary key task failed", K(ret));
  } else if (OB_FAIL(drop_pk_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_pk_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*drop_pk_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != drop_pk_task) {
    drop_pk_task->~ObDropPrimaryKeyTask();
    allocator_.free(drop_pk_task);
    drop_pk_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_table_redefinition_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObTableRedefinitionTask *redefinition_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(redefinition_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->init(task_record))) {
    LOG_WARN("init table redefinition task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObTableRedefinitionTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_column_redefinition_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObColumnRedefinitionTask *redefinition_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(redefinition_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->init(task_record))) {
    LOG_WARN("init column redefinition task failed", K(ret));
  } else if (OB_FAIL(redefinition_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(redefinition_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret), K(*redefinition_task));
    }
  }
  if (OB_FAIL(ret) && nullptr != redefinition_task) {
    redefinition_task->~ObColumnRedefinitionTask();
    allocator_.free(redefinition_task);
    redefinition_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_ddl_retry_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDDLRetryTask *ddl_retry_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(ddl_retry_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task->init(task_record))) {
    LOG_WARN("init ddl retry task failed", K(ret));
  } else if (OB_FAIL(ddl_retry_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(ddl_retry_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != ddl_retry_task) {
    ddl_retry_task->~ObDDLRetryTask();
    allocator_.free(ddl_retry_task);
    ddl_retry_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_constraint_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObConstraintTask *constraint_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(constraint_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(constraint_task->init(task_record))) {
    LOG_WARN("init constraint task failed", K(ret));
  } else if (OB_FAIL(constraint_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(constraint_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != constraint_task) {
    constraint_task->~ObConstraintTask();
    allocator_.free(constraint_task);
    constraint_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_modify_autoinc_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObModifyAutoincTask *modify_autoinc_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(modify_autoinc_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(modify_autoinc_task->init(task_record))) {
    LOG_WARN("init constraint task failed", K(ret));
  } else if (OB_FAIL(modify_autoinc_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(modify_autoinc_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != modify_autoinc_task) {
    modify_autoinc_task->~ObModifyAutoincTask();
    allocator_.free(modify_autoinc_task);
    modify_autoinc_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::schedule_drop_index_task(const ObDDLTaskRecord &task_record)
{
  int ret = OB_SUCCESS;
  ObDropIndexTask *drop_index_task = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_FAIL(alloc_ddl_task(drop_index_task))) {
    LOG_WARN("alloc ddl task failed", K(ret));
  } else if (OB_FAIL(drop_index_task->init(task_record))) {
    LOG_WARN("init drop index task failed", K(ret));
  } else if (OB_FAIL(drop_index_task->set_trace_id(task_record.trace_id_))) {
    LOG_WARN("set trace id failed", K(ret));
  } else if (OB_FAIL(inner_schedule_ddl_task(drop_index_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("inner schedule task failed", K(ret));
    }
  }
  if (OB_FAIL(ret) && nullptr != drop_index_task) {
    drop_index_task->~ObDropIndexTask();
    allocator_.free(drop_index_task);
    drop_index_task = nullptr;
  }
  if (OB_ENTRY_EXIST == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObDDLScheduler::inner_schedule_ddl_task(ObDDLTask *ddl_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ddl_task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(ddl_task));
  } else if (OB_FAIL(task_queue_.push_task(ddl_task))) {
    if (OB_ENTRY_EXIST != ret) {
      LOG_WARN("push back task to task queue failed", K(ret));
    }
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(add_sys_task(ddl_task))) {
      LOG_WARN("add sys task failed", K(tmp_ret));
    }
    idler_.wakeup();
  }
  return ret;
}

int ObDDLScheduler::on_column_checksum_calc_reply(
    const common::ObTabletID &tablet_id,
    const ObDDLTaskKey &task_key,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  LOG_INFO("receive column checksum response", K(tablet_id), K(task_key), K(ret_code));
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!(task_key.is_valid() && tablet_id.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(tablet_id), K(ret_code));
  } else {
    ObDDLTask *ddl_task = nullptr;
    if (OB_FAIL(task_queue_.get_task(task_key, ddl_task))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get task failed", K(ret), K(task_key));
      }
    } else if (OB_ISNULL(ddl_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index task is null", K(ret));
    } else if (ObDDLType::DDL_CREATE_INDEX != ddl_task->get_task_type()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl task type not global index", K(ret), K(ddl_task));
    } else if (OB_FAIL(reinterpret_cast<ObIndexBuildTask *>(ddl_task)->update_column_checksum_calc_status(tablet_id, ret_code))) {
      LOG_WARN("update column checksum calc status failed", K(ret));
    }
  }
  return ret;
}

int ObDDLScheduler::on_sstable_complement_job_reply(
    const common::ObTabletID &tablet_id,
    const ObDDLTaskKey &task_key,
    const int64_t snapshot_version,
    const int64_t execution_id,
    const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!(task_key.is_valid() && snapshot_version > 0 && execution_id > 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(snapshot_version), K(execution_id), K(ret_code));
  } else {
    ObDDLTask *ddl_task = nullptr;
    if (OB_FAIL(task_queue_.get_task(task_key, ddl_task))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get task failed", K(ret), K(task_key));
      }
    } else if (OB_ISNULL(ddl_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index task is null", K(ret));
    } else {
      const int64_t task_type = ddl_task->get_task_type();
      switch (task_type) {
        case ObDDLType::DDL_CREATE_INDEX:
          if (OB_FAIL(static_cast<ObIndexBuildTask *>(ddl_task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code))) {
            LOG_WARN("update complete sstable job status failed", K(ret));
          }
          break;
        case ObDDLType::DDL_DROP_PRIMARY_KEY: 
          if (OB_FAIL(static_cast<ObDropPrimaryKeyTask *>(ddl_task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code))) {
            LOG_WARN("update complete sstable job status", K(ret));
          }
          break;
        case ObDDLType::DDL_ADD_PRIMARY_KEY:
        case ObDDLType::DDL_ALTER_PRIMARY_KEY:
        case ObDDLType::DDL_ALTER_PARTITION_BY:
        case ObDDLType::DDL_MODIFY_COLUMN:
        case ObDDLType::DDL_CONVERT_TO_CHARACTER:
        case ObDDLType::DDL_TABLE_REDEFINITION:
          if (OB_FAIL(static_cast<ObTableRedefinitionTask *>(ddl_task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code))) {
            LOG_WARN("update complete sstable job status", K(ret));
          }
          break;
        case ObDDLType::DDL_CHECK_CONSTRAINT:
        case ObDDLType::DDL_FOREIGN_KEY_CONSTRAINT:
        case ObDDLType::DDL_ADD_NOT_NULL_COLUMN:
          if (OB_FAIL(static_cast<ObConstraintTask *>(ddl_task)->update_check_constraint_finish(ret_code))) {
            LOG_WARN("update check constraint finish", K(ret));
          }
          break;
        case ObDDLType::DDL_DROP_COLUMN:
        case ObDDLType::DDL_ADD_COLUMN_OFFLINE:
        case ObDDLType::DDL_COLUMN_REDEFINITION:
          if (OB_FAIL(static_cast<ObColumnRedefinitionTask *>(ddl_task)->update_complete_sstable_job_status(tablet_id, snapshot_version, execution_id, ret_code))) {
            LOG_WARN("update complete sstable job status", K(ret), K(tablet_id), K(snapshot_version), K(ret_code));
          }
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported ddl task", K(ret), K(*ddl_task));
          break;
      }
    }
  }
  return ret;
}

int ObDDLScheduler::on_ddl_task_finish(
    const int64_t parent_task_id, 
    const ObDDLTaskKey &child_task_key, 
    const int ret_code,
    const ObCurTraceId::TraceId &parent_task_trace_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLScheduler has not been inited", K(ret));
  } else if (OB_UNLIKELY(parent_task_id <= 0 || !child_task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(parent_task_id), K(child_task_key));
  } else {
    ObDDLTask *ddl_task = nullptr;
    ObDDLRedefinitionTask *redefinition_task = nullptr;
    if (OB_FAIL(task_queue_.get_task(parent_task_id, ddl_task))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        bool is_cancel = false;
        int tmp_ret = OB_SUCCESS;
        if (OB_TMP_FAIL(SYS_TASK_STATUS_MGR.is_task_cancel(parent_task_trace_id, is_cancel))) {
          LOG_WARN("check task is canceled", K(tmp_ret), K(parent_task_trace_id));
        }
        if (is_cancel || OB_ENTRY_NOT_EXIST == tmp_ret) {
          ret = OB_CANCELED;
          LOG_INFO("parent task is canceled, return to cleaup child task", 
            K(ret), K(ret_code), K(parent_task_id), K(child_task_key), K(parent_task_trace_id));
        }
      }
      if (OB_FAIL(ret) && OB_CANCELED != ret) {
        LOG_WARN("get from task map failed", K(ret), K(parent_task_id), K(parent_task_trace_id));
      }
    } else if (OB_ISNULL(ddl_task)) {
      ret = OB_ERR_SYS;
      LOG_WARN("ddl task must not be nullptr", K(ret));
    } else if (FALSE_IT(redefinition_task = static_cast<ObDDLRedefinitionTask *>(ddl_task))) {
    } else if (OB_FAIL(redefinition_task->on_child_task_finish(child_task_key, ret_code))) {
      LOG_WARN("on child task finish failed", K(ret), K(child_task_key));
    }
  }
  return ret;
}

int ObDDLScheduler::notify_update_autoinc_end(const ObDDLTaskKey &task_key,
                                              const uint64_t autoinc_val,
                                              const int ret_code)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(!task_key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_key), K(ret_code));
  } else {
    ObDDLTask *ddl_task = nullptr;
    if (OB_FAIL(task_queue_.get_task(task_key, ddl_task))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("get task failed", K(ret), K(task_key));
      }
    } else if (OB_ISNULL(ddl_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index task is null", K(ret));
    } else {
      const int64_t task_type = ddl_task->get_task_type();
      switch (task_type) {
        case ObDDLType::DDL_MODIFY_COLUMN:
        case ObDDLType::DDL_ALTER_PARTITION_BY:
        case ObDDLType::DDL_TABLE_REDEFINITION:
          if (OB_FAIL(static_cast<ObTableRedefinitionTask *>(ddl_task)->notify_update_autoinc_finish(autoinc_val, ret_code))) {
            LOG_WARN("update complete sstable job status", K(ret));
          }
          break;
        case ObDDLType::DDL_MODIFY_AUTO_INCREMENT:
          if (OB_FAIL(static_cast<ObModifyAutoincTask *>(ddl_task)->notify_update_autoinc_finish(autoinc_val, ret_code))) {
            LOG_WARN("update complete sstable job status", K(ret), K(ret_code));
          }
          break;
        default:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("not supported ddl task", K(ret), K(*ddl_task));
          break;
      }
    }
  }
  return ret;
}

void ObDDLScheduler::free_ddl_task(ObDDLTask *ddl_task)
{
  if (nullptr != ddl_task) {
    ddl_task->~ObDDLTask();
    allocator_.free(ddl_task);
  }
}

void ObDDLScheduler::destroy_all_tasks()
{
  int ret = OB_SUCCESS;
  ObDDLTask *ddl_task = nullptr;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(task_queue_.get_next_task(ddl_task))) {
      if (common::OB_ENTRY_NOT_EXIST == ret) {
        break;
      }
    } else {
      remove_sys_task(ddl_task);
      task_queue_.remove_task(ddl_task);
      free_ddl_task(ddl_task);
    }
  }
}

} // end namespace rootserver
} // end namespace oceanbase



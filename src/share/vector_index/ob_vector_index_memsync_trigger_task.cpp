/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_vector_index_memsync_trigger_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "storage/ls/ob_ls.h"

namespace oceanbase
{
namespace share
{

bool ObVecIdxMemSyncTriggerExecutor::check_operation_allow()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool bret = true;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_4_2_1) {
    bret = false;
    LOG_DEBUG("memsync trigger task can not work with data version less than 4_4_2_1", K(tenant_data_version));
  }
  return bret;
}

int ObVecIdxMemSyncTriggerExecutor::check_and_set_thread_pool()
{
  return OB_SUCCESS;
}

int ObVecIdxMemSyncTriggerExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  UNUSED(task_trace_base_num);
  ObMySQLProxy *sql_proxy = GCTX.sql_proxy_;
  ObVecIndexTaskStatusArray task_result_arr;
  ObArenaAllocator allocator("MemSyncTrigger", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) {
    // skip
  } else if (OB_ISNULL(sql_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is null", K(ret));
  } else {
    // Read manual memsync trigger tasks from __all_vector_index_task table with PREPARE status
    ObVecIndexFieldArray filters;
    ObVecIndexTaskStatusField field1;
    field1.field_name_ = "trigger_type";
    field1.data_.uint_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_MANUAL;
    ObVecIndexTaskStatusField field2;
    field2.field_name_ = "status";
    field2.data_.uint_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
    ObVecIndexTaskStatusField field3;
    field3.field_name_ = "task_type";
    field3.data_.uint_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_MEMSYNC_TRIGGER;

    if (OB_FAIL(filters.push_back(field1))) {
      LOG_WARN("fail to push back field", K(ret));
    } else if (OB_FAIL(filters.push_back(field2))) {
      LOG_WARN("fail to push back field", K(ret));
    } else if (OB_FAIL(filters.push_back(field3))) {
      LOG_WARN("fail to push back field", K(ret));
    } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::read_vec_tasks(tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME,
        true /* for_update */, filters, ls_, *sql_proxy, task_result_arr, &allocator))) {
      LOG_WARN("fail to read memsync trigger tasks from inner table", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (task_result_arr.count() > 0 && OB_NOT_NULL(scheduler_)) {
    ObSEArray<common::ObTabletID, 8> tablet_ids;
    ObSEArray<uint64_t, 8> table_ids;
    hash::ObHashMap<common::ObTabletID, uint64_t> tablet_to_table_map;
    if (OB_FAIL(tablet_to_table_map.create(8, "MemSyncMap", "MemSyncMap", MTL_ID()))) {
      LOG_WARN("fail to create tablet_to_table_map", K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < task_result_arr.count(); ++i) {
      ObVecIndexTaskStatus &task_status = task_result_arr.at(i);
      int tmp_ret = tablet_to_table_map.set_refactored(task_status.tablet_id_, task_status.table_id_, 0);
      if (OB_SUCCESS == tmp_ret) {
        if (OB_FAIL(tablet_ids.push_back(task_status.tablet_id_))) {
          LOG_WARN("fail to push back tablet_id", K(ret), K(i));
        } else if (OB_FAIL(table_ids.push_back(task_status.table_id_))) {
          LOG_WARN("fail to push back table_id", K(ret), K(i));
        }
      } else if (OB_HASH_EXIST == tmp_ret) {
          // do nothing, duplicate tablet_id, skip
      } else {
        ret = tmp_ret;
        LOG_WARN("fail to set tablet_id into set", K(ret), K(i), K(task_status.tablet_id_));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(scheduler_->submit_memdata_sync_log_for_tablets(tablet_ids, table_ids))) {
        LOG_WARN("fail to submit memdata sync log for manual trigger", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
      }
    }
    for (int64_t i = 0; i < task_result_arr.count(); ++i) {
      ObVecIndexTaskStatus &task_status = task_result_arr.at(i);
      ObVecIndexTaskKey key(tenant_id_, task_status.table_id_, task_status.tablet_id_.id(), task_status.task_id_);
      ObVecIndexFieldArray update_fields;
      ObVecIndexTaskStatusField status_field;
      status_field.field_name_ = "status";
      status_field.data_.uint_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_FINISH;
      ObVecIndexTaskStatusField ret_code_field;
      ret_code_field.field_name_ = "ret_code";
      ret_code_field.data_.uint_ = ret;
      ObVecIndexTaskProgressInfo progress_info;
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(update_fields.push_back(status_field))) {
        LOG_WARN("fail to push back status field", K(tmp_ret));
      } else if (OB_TMP_FAIL(update_fields.push_back(ret_code_field))) {
        LOG_WARN("fail to push back ret_code field", K(tmp_ret));
      } else if (OB_TMP_FAIL(ObVecIndexAsyncTaskUtil::update_vec_task(tenant_id_, OB_ALL_VECTOR_INDEX_TASK_TNAME,
          *sql_proxy, key, update_fields, progress_info))) {
        LOG_WARN("fail to update task status to FINISH", K(tmp_ret), K(task_status.task_id_), K(key));
      }
    }
    if (tablet_to_table_map.created()) {
      tablet_to_table_map.destroy();
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase
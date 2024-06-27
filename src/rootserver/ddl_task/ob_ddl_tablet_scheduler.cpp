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

#include "ob_ddl_tablet_scheduler.h"
#include "rootserver/ob_root_service.h"
#include "share/ob_ddl_checksum.h"
#include "share/ob_ddl_error_message_table_operator.h"
#include "share/ob_ddl_common.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/scn.h"
#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "storage/ddl/ob_ddl_lock.h"

using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObDDLTabletScheduler::ObDDLTabletScheduler()
  : is_inited_(false), allocator_("TabletScheduler")
{

}
ObDDLTabletScheduler::~ObDDLTabletScheduler()
{

}

int ObDDLTabletScheduler::init(const uint64_t tenant_id,
                               const uint64_t table_id,
                               const uint64_t ref_data_table_id,
                               const int64_t  task_id,
                               const int64_t  parallelism,
                               const int64_t  snapshot_version,
                               const common::ObCurTraceId::TraceId &trace_id,
                               const ObIArray<ObTabletID> &tablets)
{
  int ret = OB_SUCCESS;
  common::ObAddr inner_sql_exec_addr;
  common::ObArray<ObString> running_sql_info;
  common::ObArray<ObLSID> ls_ids;
  common::ObArray<ObTabletID> ref_data_table_tablets;
  common::hash::ObHashMap<uint64_t, bool> tablet_checksum_status_map;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_ISNULL(root_service_ = GCTX.root_service_)) {
    ret = OB_ERR_SYS;
    LOG_WARN("root_service is null", K(ret), KP(root_service_));
  } else if (!root_service_->in_service()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("root service not in service", K(ret));
  } else if (OB_UNLIKELY(
        !(OB_INVALID_ID != tenant_id
          && OB_INVALID_ID != table_id
          && OB_INVALID_ID != ref_data_table_id
          && task_id > 0
          && parallelism > 0
          && snapshot_version > 0
          && trace_id.is_valid()
          && tablets.count() > 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(table_id), K(ref_data_table_id), K(task_id), K(parallelism), K(snapshot_version), K(trace_id), K(tablets.count()));
  } else if (OB_FAIL(ObDDLUtil::get_tablets(tenant_id, ref_data_table_id, ref_data_table_tablets))) {
    LOG_WARN("failed to get ref data table tablet ids", K(ret), K(tenant_id), K(ref_data_table_id), K(ref_data_table_tablets));
  } else if (OB_UNLIKELY(tablets.count() != ref_data_table_tablets.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index table tablets count is not equal to data table tablets count", K(ret), K(tablets.count()), K(ref_data_table_tablets.count()));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret));
  } else if (OB_FAIL(ObTabletToLSTableOperator::batch_get_ls(GCTX.root_service_->get_sql_proxy(), tenant_id, tablets, ls_ids))) {
    LOG_WARN("failed to batch get ls", K(ret), K(tenant_id), K(tablets), K(ls_ids));
  } else if (OB_UNLIKELY(tablets.count() != ls_ids.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablets count is not equal to ls id count", K(ret), K(tablets.count()), K(ls_ids.count()));
  } else if (OB_FAIL(all_ls_to_tablets_map_.create(tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create lsid to tablet id map", K(ret), K(tablets.count()));
  } else if (OB_FAIL(running_ls_to_tablets_map_.create(tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create lsid to tablet id map", K(ret), K(tablets.count()));
  } else if (OB_FAIL(ls_location_map_.create(tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create lsid location map", K(ret), K(tablets.count()));
  } else if (OB_FAIL(running_ls_to_execution_id_.create(tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create lsid to execution id map", K(ret), K(tablets.count()));
  } else if (OB_FAIL(tablet_checksum_status_map.create(tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret), K(tablets.count()));
  } else if (OB_FAIL(tablet_id_to_data_size_.create(ref_data_table_tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret), K(ref_data_table_tablets.count()));
  } else if (OB_FAIL(tablet_id_to_data_row_cnt_.create(ref_data_table_tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create column checksum map", K(ret), K(ref_data_table_tablets.count()));
  } else if (OB_FAIL(tablet_scheduled_times_statistic_.create(ref_data_table_tablets.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create tablet scheduled count statistic map", K(ret), K(ref_data_table_tablets.count()));
  } else if (OB_FAIL(ObDDLChecksumOperator::get_tablet_checksum_record_without_execution_id(
    tenant_id,
    table_id,
    task_id,
    tablets,
    GCTX.root_service_->get_sql_proxy(),
    tablet_checksum_status_map))) {
    LOG_WARN("fail to get tablet checksum status", K(ret), K(tenant_id), K(table_id), K(task_id), K(tablets));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::get_running_tasks_inner_sql(root_service_->get_sql_proxy(), trace_id, tenant_id, task_id, snapshot_version, inner_sql_exec_addr, allocator_, running_sql_info))) {
    LOG_WARN("get running tasks inner sql fail", K(ret), K(trace_id), K(tenant_id), K(task_id), K(snapshot_version), K(inner_sql_exec_addr), K(running_sql_info));
  } else {
    bool is_running_status = false;
    bool is_finished_status = false;
    int64_t tablet_data_size = 0;
    int64_t tablet_data_row_cnt = 0;
    ObArray<ObTabletID> part_tablets;
    ObArray<ObString> partition_names;
    for (int64_t i = 0; i < tablets.count() && OB_SUCC(ret); i++) {
      is_running_status = false;
      is_finished_status = false;
      tablet_data_size = 0;
      tablet_data_row_cnt = 0;
      part_tablets.reuse();
      partition_names.reuse();
      if (OB_FAIL(ObDDLUtil::get_tablet_data_size(tenant_id, ref_data_table_tablets.at(i), ls_ids.at(i), tablet_data_size))) {
        LOG_WARN("fail to get tablet data size", K(ret), K(tenant_id), K(ref_data_table_tablets.at(i)), K(ls_ids.at(i)), K(tablet_data_size));
      } else if (OB_FAIL(ObDDLUtil::get_tablet_data_row_cnt(tenant_id, ref_data_table_tablets.at(i), ls_ids.at(i), tablet_data_row_cnt))) {
        LOG_WARN("fail to get tablet data size", K(ret), K(tenant_id), K(ref_data_table_tablets.at(i)), K(ls_ids.at(i)), K(tablet_data_row_cnt));
      } else if (OB_FAIL(tablet_id_to_data_size_.set_refactored(ref_data_table_tablets.at(i).id(), tablet_data_size, true /* overwrite */))) {
        LOG_WARN("table id to data size map set fail", K(ret), K(ref_data_table_tablets.at(i).id()), K(tablet_data_size));
      } else if (OB_FAIL(tablet_id_to_data_row_cnt_.set_refactored(ref_data_table_tablets.at(i).id(), tablet_data_row_cnt, true /* overwrite */))) {
        LOG_WARN("table id to data size map set fail", K(ret), K(ref_data_table_tablets.at(i).id()), K(tablet_data_row_cnt));
      } else if (OB_FAIL(part_tablets.push_back(tablets.at(i)))) {
        LOG_WARN("fail to push back", K(ret), K(tablets.at(i)));
      } else if (OB_FAIL(ObDDLUtil::get_index_table_batch_partition_names(tenant_id, ref_data_table_id, table_id, part_tablets, allocator_, partition_names))) {
        LOG_WARN("fail to get index table batch partition names", K(ret), K(tenant_id), K(ref_data_table_id), K(table_id), K(part_tablets), K(partition_names));
      } else {
        if (OB_FAIL(tablet_checksum_status_map.get_refactored(tablets.at(i).id(), is_finished_status))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          }
        }
        for (int64_t j = 0; j < running_sql_info.count() && OB_SUCC(ret); j++) {
          is_running_status = false;
          if (OB_FAIL(ObDDLUtil::check_target_partition_is_running(running_sql_info.at(j), partition_names.at(0), allocator_, is_running_status))) {
            LOG_WARN("fail to check target partition is running", K(ret), K(running_sql_info.at(j)), K(partition_names.at(0)), K(is_running_status));
          } else if (is_running_status) {
            break;
          }
        }
        if (OB_SUCC(ret)) {
          if (!is_running_status && is_finished_status) {
            LOG_INFO("tablet has complemented data", K(ret), K(tenant_id), K(table_id), K(ref_data_table_id), K(tablets.at(i)));
          } else {
            common::ObAddr leader_addr;
            share::ObLocationService *location_service = nullptr;
            int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
            const int64_t retry_interval_us = 200 * 1000; // 200ms
            if (OB_ISNULL(location_service = GCTX.location_service_)) {
              ret = OB_ERR_SYS;
              LOG_WARN("location_cache is null", K(ret), KP(location_service));
            } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(GCONF.cluster_id,
              tenant_id, ls_ids.at(i), leader_addr, rpc_timeout, retry_interval_us))) {
              LOG_WARN("fail to get ls locaiton leader", K(ret), K(tenant_id), K(ls_ids.at(i)));
            } else if (OB_FAIL(ls_location_map_.set_refactored(ls_ids.at(i), leader_addr, true /* overwrite */))) {
              LOG_WARN("ls location map set fail", K(ret), K(ls_ids.at(i)), K(leader_addr));
            } else if (is_running_status) {
              if (OB_FAIL(ObDDLUtil::construct_ls_tablet_id_map(tenant_id, ls_ids.at(i), tablets.at(i), running_ls_to_tablets_map_))) {
                LOG_WARN("fail to create running lsid to tablet id map", K(ret), K(tenant_id), K(ls_ids.at(i)), K(tablets.at(i)), K(table_id), K(ref_data_table_id), K(running_ls_to_tablets_map_.size()));
              } else if (common::is_contain(running_task_ls_ids_before_, ls_ids.at(i))) {
              } else if (OB_FAIL(running_task_ls_ids_before_.push_back(ls_ids.at(i)))) {
                LOG_WARN("fail to push back", K(ret), K(tenant_id), K(table_id), K(ref_data_table_id), K(ls_ids.at(i)), K(is_finished_status));
              }
            }
            if (OB_SUCC(ret)) {
              if (OB_FAIL(all_tablets_.push_back(tablets.at(i)))) {
                LOG_WARN("fail to push back", K(ret), K(tablets.at(i)));
              } else if (OB_FAIL(ObDDLUtil::construct_ls_tablet_id_map(tenant_id, ls_ids.at(i), tablets.at(i), all_ls_to_tablets_map_))) {
                LOG_WARN("fail to create lsid to tablet id map", K(ret), K(tenant_id), K(ls_ids.at(i)), K(tablets.at(i)), K(table_id), K(ref_data_table_id), K(all_ls_to_tablets_map_.size()));
              }
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    table_id_ = table_id;
    ref_data_table_id_ = ref_data_table_id;
    task_id_ = task_id;
    parallelism_ = parallelism;
    snapshot_version_ = snapshot_version;
    trace_id_ = trace_id;
    is_inited_ = true;
    LOG_INFO("success to init", K(ret), K(tenant_id), K(table_id), K(ref_data_table_id), K(task_id), K(parallelism), K(snapshot_version), K(trace_id), K(tablets), K(all_ls_to_tablets_map_.size()), K(running_ls_to_tablets_map_.size()), K(running_task_ls_ids_before_.count()));
  } else {
    LOG_INFO("fail to init", K(ret), K(tenant_id), K(table_id), K(ref_data_table_id), K(task_id), K(parallelism), K(snapshot_version), K(trace_id), K(tablets), K(all_ls_to_tablets_map_.size()), K(running_ls_to_tablets_map_.size()), K(running_task_ls_ids_before_.count()));
    destroy();
  }
  return ret;
}

int ObDDLTabletScheduler::get_next_batch_tablets(int64_t &parallelism, int64_t &new_execution_id, share::ObLSID &ls_id, common::ObAddr &leader_addr, ObIArray<ObTabletID> &tablets)
{
  int ret = OB_SUCCESS;
  bool need_send_task = false;
  parallelism = 0;
  new_execution_id = 0;
  uint64_t tenant_data_version = 0;
  ls_id.reset();
  leader_addr.reset();
  tablets.reset();
  share::ObDDLType task_type = share::DDL_CREATE_PARTITIONED_LOCAL_INDEX;
  common::hash::ObHashMap<uint64_t, bool> tablet_checksum_status_map;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (is_all_tasks_finished()) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(determine_if_need_to_send_new_task(need_send_task))) {
    LOG_WARN("fail to get status of if need to send new task", K(ret), K(need_send_task));
  } else if (!need_send_task) {
    if (!is_running_tasks_before_finished()) {
      ObArray<share::ObLSID> running_task_ls_ids_now;
      ObArray<share::ObLSID> potential_finished_ls_ids;
      if (OB_FAIL(get_session_running_lsid(running_task_ls_ids_now))) {
        LOG_WARN("fail to get session running lsid", K(ret), K(running_task_ls_ids_now));
      } else if (OB_FAIL(get_potential_finished_lsid(running_task_ls_ids_now, potential_finished_ls_ids))) {
        LOG_WARN("fail to get potential finished lsid", K(ret), K(running_task_ls_ids_now), K(potential_finished_ls_ids));
      } else {
        for (int64_t i = 0; i < potential_finished_ls_ids.count() && OB_SUCC(ret); i++) {
          if (OB_FAIL(check_target_ls_tasks_completion_status(potential_finished_ls_ids.at(i)))) {
            LOG_WARN("fail to check target ls tasks completion status", K(ret), K(potential_finished_ls_ids.at(i)));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ret = OB_EAGAIN;
    }
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    LOG_WARN("get min data version failed", K(ret), K(tenant_id_));
  } else if (OB_FAIL(ObDDLTask::push_execution_id(tenant_id_, task_id_, task_type, true/*is ddl retryable*/, tenant_data_version, new_execution_id))) {
    LOG_WARN("failed to fetch new execution id", K(ret), K(tenant_id_), K(task_id_), K(new_execution_id));
  } else if (OB_FAIL(get_next_parallelism(parallelism))) {
    LOG_WARN("fail to get next parallelism", K(ret), K(parallelism));
  } else if (OB_FAIL(get_unfinished_tablets(new_execution_id, ls_id, leader_addr, tablets))) {
    LOG_WARN("failed to get unfinished tablets", K(ret), K(new_execution_id), K(tablets));
  }
  return ret;
}

int ObDDLTabletScheduler::confirm_batch_tablets_status(const int64_t execution_id, const bool finish_status, const ObLSID &ls_id, const ObIArray<ObTabletID> &tablets)
{
  int ret = OB_SUCCESS;
  int64_t ls_execution_id = 0;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_UNLIKELY(tablets.count() < 1 || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tablets array is null", K(ret), K(tablets.count()), K(ls_id));
  } else if (OB_UNLIKELY(all_ls_to_tablets_map_.size() == 0 || running_ls_to_tablets_map_.size() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls to tablets map is null", K(ret), K(all_ls_to_tablets_map_.size()), K(running_ls_to_tablets_map_.size()));
  } else {
    if (execution_id != -1) { //execution_id == -1 indicates the task before switching to rs is confirming
      if (OB_FAIL(running_ls_to_execution_id_.get_refactored(ls_id, ls_execution_id))) {
        LOG_WARN("fail to get execution id", K(ret), K(ls_id), K(ls_execution_id));
      } else if (OB_UNLIKELY(execution_id != ls_execution_id)) {
        ret = OB_TASK_EXPIRED;
        LOG_WARN("receive a mismatch execution result", K(ret), K(execution_id), K(ls_execution_id), K(tablets), K(finish_status));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(running_ls_to_tablets_map_.erase_refactored(ls_id))) {
        LOG_WARN("failed to erase ls id", K(ret), K(ls_id));
      } else if (finish_status) {
        ObTabletIdUpdater updater(tablets);
        if (OB_FAIL(all_ls_to_tablets_map_.atomic_refactored(ls_id, updater))) {
          LOG_WARN("fail to update tablet ids", K(ret), K(ls_id), K(tablets));
        } else {
          bool is_erased = false;
          HashMapEraseIfNull functor;
          if (OB_FAIL(all_ls_to_tablets_map_.erase_if(ls_id, functor, is_erased))) {
            LOG_WARN("fail to erase lsid", K(ret), K(ls_id), K(tablets));
          }
        }
      }
    }
  }
  LOG_INFO("confirm batch tablets status", K(ret), K(execution_id), K(finish_status), K(ls_id), K(tablets));
  return ret;
}

int ObDDLTabletScheduler::refresh_ls_location_map() {
  int ret = OB_SUCCESS;
  TCRLockGuard guard(lock_);
  common::hash::ObHashMap<share::ObLSID, common::ObAddr>::iterator iter;
  for (iter = ls_location_map_.begin(); iter != ls_location_map_.end() && OB_SUCC(ret); ++iter) {
    const ObLSID ls_id = iter->first;
    common::ObAddr leader_addr;
    share::ObLocationService *location_service = nullptr;
    int64_t rpc_timeout = ObDDLUtil::get_default_ddl_rpc_timeout();
    const int64_t retry_interval_us = 200 * 1000; // 200ms
    if (OB_ISNULL(location_service = GCTX.location_service_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("location_cache is null", K(ret), KP(location_service));
    } else if (OB_FAIL(location_service->get_leader_with_retry_until_timeout(GCONF.cluster_id,
      tenant_id_, ls_id, leader_addr, rpc_timeout, retry_interval_us))) {
      LOG_WARN("fail to get ls locaiton leader", K(ret), K(tenant_id_), K(ls_id));
    } else if (OB_FAIL(ls_location_map_.set_refactored(ls_id, leader_addr, true /* overwrite */))) {
      LOG_WARN("ls location map set fail", K(ret), K(ls_id), K(leader_addr));
    }
  }
  return ret;
}

int ObDDLTabletScheduler::get_next_parallelism(int64_t &parallelism)
{
  int ret = OB_SUCCESS;
  parallelism = 0;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    int64_t ls_num = all_ls_to_tablets_map_.size();
    if (ls_num > 0) { // it is ensured that the value of ls_num is greater than 0
      parallelism = (parallelism_ + ls_num - 1) / ls_num;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the all ls to tablet map size is less than 1", K(ret), K(all_ls_to_tablets_map_.size()), K(parallelism));
    }
  }
  return ret;
}

int ObDDLTabletScheduler::get_running_sql_parallelism(int64_t &parallelism)
{
  int ret = OB_SUCCESS;
  parallelism = 0;
  common::ObAddr inner_sql_exec_addr;
  common::ObArray<ObString> running_sql_info;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::get_running_tasks_inner_sql(root_service_->get_sql_proxy(), trace_id_, tenant_id_, task_id_, snapshot_version_, inner_sql_exec_addr, allocator_, running_sql_info))) {
    LOG_WARN("get running tasks inner sql fail", K(ret), K(tenant_id_), K(trace_id_), K(task_id_), K(snapshot_version_), K(inner_sql_exec_addr), K(running_sql_info));
  } else {
    for (int64_t i = 0; i < running_sql_info.count() && OB_SUCC(ret); i++) {
      ObString parallel_flag = ObString::make_string("parallel(");
      int64_t loc = ObCharset::instr(ObCollationType::CS_TYPE_UTF8MB4_BIN, running_sql_info.at(i).ptr(), running_sql_info.at(i).length(), parallel_flag.ptr(), parallel_flag.length());
      if (OB_UNLIKELY(0 != loc)) {
        uint64_t value;
        int err = 0;
        ObString parallel;
        if (OB_FAIL(ob_sub_str(allocator_, running_sql_info.at(i), loc+parallel_flag.length()-1, running_sql_info.at(i).length() - 1, parallel))) {
          LOG_WARN("failed to extract parallel info from running sql", K(ret), K(running_sql_info.at(i)), K(parallel_flag), K(parallel));
        } else {
          parallel = parallel.clip(parallel.find(')'));
          if (parallel.is_numeric()) {
            value = ObCharset::strntoull(parallel.ptr(), parallel.length(), 10, &err);
            parallelism = parallelism + value;
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parallel value is not int", K(ret), K(running_sql_info.at(i)), K(parallel_flag), K(parallel));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("running sql is wrong", K(ret), K(running_sql_info.at(i)), K(parallel_flag), K(parallelism));
      }
    }
  }
  return ret;
}

int ObDDLTabletScheduler::get_unfinished_tablets(const int64_t execution_id, share::ObLSID &ls_id, common::ObAddr &leader_addr, ObIArray<ObTabletID> &tablets)
{
  int ret = OB_SUCCESS;
  tablets.reset();
  ls_id.reset();
  leader_addr.reset();
  ObArray<ObTabletID> tablet_queue;
  uint64_t left_space_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(get_to_be_scheduled_tablets(ls_id, leader_addr, tablet_queue))) {
    LOG_WARN("fail to get to be scheduled tablets", K(ret), K(tablet_queue));
  } else if (OB_UNLIKELY(tablet_queue.count() < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet queue is null", K(ret), K(tablet_queue.count()));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !leader_addr.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("got argument is error", K(ret), K(ls_id), K(leader_addr));
  } else if (OB_FAIL(ObDDLUtil::get_ls_host_left_disk_space(tenant_id_, ls_id, leader_addr, left_space_size))) {
    LOG_WARN("fail to get ls host left disk space", K(ret), K(tenant_id_), K(ls_id), K(leader_addr), K(left_space_size));
  } else if (OB_FAIL(calculate_candidate_tablets(left_space_size, tablet_queue, tablets))) {
    LOG_WARN("fail to use strategy to get tablets", K(ret), K(left_space_size), K(tablet_queue), K(tablets));
  } else {
    TCWLockGuard guard(lock_);
    if (OB_FAIL(running_ls_to_execution_id_.set_refactored(ls_id, execution_id, true /* overwrite */))) {
      LOG_WARN("running ls to execution id map set fail", K(ret), K(ls_id), K(execution_id));
    } else {
      ObArray<ObTabletID> running_tablet_queue;
      if (OB_FAIL(running_tablet_queue.assign(tablets))) {
        LOG_WARN("ObArray assign failed", K(ret), K(tablets));
      } else if (OB_FAIL(running_ls_to_tablets_map_.set_refactored(ls_id, running_tablet_queue, true /* overwrite */))) {
        LOG_WARN("ls tablets map set fail", K(ret), K(ls_id), K(running_tablet_queue));
      }
    }
  }
  return ret;
}

int ObDDLTabletScheduler::get_to_be_scheduled_tablets(share::ObLSID &ls_id, common::ObAddr &leader_addr, ObIArray<ObTabletID> &tablets)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  leader_addr.reset();
  tablets.reset();
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(all_ls_to_tablets_map_.size() <= running_ls_to_tablets_map_.size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("all ls tablets map is less than running ls tablets map", K(ret), K(all_ls_to_tablets_map_.size()), K(running_ls_to_tablets_map_.size()));
  } else {
    common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>>::iterator iter;
    for (iter = all_ls_to_tablets_map_.begin(); iter != all_ls_to_tablets_map_.end() && OB_SUCC(ret); ++iter) {
      ls_id = iter->first;
      ObArray<ObTabletID> &tablet_queue = iter->second;
      ObArray<ObTabletID> running_tablet_queue;
      bool is_running_ls = true;
      if (OB_FAIL(running_ls_to_tablets_map_.get_refactored(ls_id, running_tablet_queue))) {
        if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
          is_running_ls = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get tablet queue from refactored", K(ret), K(ls_id));
        }
      }
      if (OB_SUCC(ret) && OB_LIKELY(!is_running_ls && tablet_queue.count() > 0)) {
        if (OB_FAIL(ls_location_map_.get_refactored(ls_id, leader_addr))) {
          LOG_WARN("fail to get leader addr from ls location map", K(ret), K(ls_id), K(leader_addr));
        } else if (OB_FAIL(tablets.assign(tablet_queue))) {
          LOG_WARN("ObArray assign failed", K(ret), K(tablet_queue));
        }
        break;
      }
    }
  }
  return ret;
}

int ObDDLTabletScheduler::calculate_candidate_tablets(const uint64_t left_space_size, const ObIArray<ObTabletID> &in_tablets, ObIArray<ObTabletID> &out_tablets)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *data_table_schema = nullptr;
  const ObTableSchema *index_schema = nullptr;
  out_tablets.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", K(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, ref_data_table_id_, data_table_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(ref_data_table_id_));
  } else if (OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("error unexpected, data table schema is null", K(ret), K(ref_data_table_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id_, index_schema))) {
    LOG_WARN("get table schema failed", K(ret), K(tenant_id_), K(table_id_));
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("error unexpected, index table schema is null", K(ret), K(table_id_));
  } else {
    ObPartition **data_partitions = data_table_schema->get_part_array();
    const ObPartitionLevel part_level = data_table_schema->get_part_level();
    if (OB_ISNULL(data_partitions)) {
      ret = OB_PARTITION_NOT_EXIST;
      LOG_WARN("data table part array is null", K(ret), KPC(this));
    } else {
      int64_t part_index = -1;
      int64_t subpart_index = -1;
      int64_t pre_data_size = 0;
      int64_t pre_data_row_cnt = 0;
      int64_t tablet_data_size = 0;
      int64_t tablet_data_row_cnt = 0;
      uint64_t task_max_data_size = 0;
      const int64_t task_max_data_row_cnt = 50000000;
      if (left_space_size > 0) {
        task_max_data_size = left_space_size / 30; // according to the estimated maximum temporary space amplification factor 30, ensure that the current remaining disk space can complete index construction
      } else {
        task_max_data_size = 5368709120; // 5GB
      }
      for (int64_t i = 0; i < in_tablets.count() && OB_SUCC(ret); i++) {
        tablet_data_size = 0;
        tablet_data_row_cnt = 0;
        if (OB_FAIL(index_schema->get_part_idx_by_tablet(in_tablets.at(i), part_index, subpart_index))) {
          LOG_WARN("failed to get part idx by tablet", K(ret), K(in_tablets.at(i)), K(part_index), K(subpart_index));
        } else {
          if (PARTITION_LEVEL_ONE == part_level) {
            if (OB_FAIL(tablet_id_to_data_size_.get_refactored(data_partitions[part_index]->get_tablet_id().id(), tablet_data_size))) {
              LOG_WARN("fail to get tablet data size", K(ret), K(data_partitions[part_index]->get_tablet_id()), K(tablet_data_size));
            } else if (OB_FAIL(tablet_id_to_data_row_cnt_.get_refactored(data_partitions[part_index]->get_tablet_id().id(), tablet_data_row_cnt))) {
              LOG_WARN("fail to get tablet data size", K(ret), K(data_partitions[part_index]->get_tablet_id()), K(tablet_data_row_cnt));
            }
          } else if (PARTITION_LEVEL_TWO == part_level) {
            ObSubPartition **data_subpart_array = data_partitions[part_index]->get_subpart_array();
            if (OB_ISNULL(data_subpart_array)) {
              ret = OB_PARTITION_NOT_EXIST;
              LOG_WARN("part array is null", K(ret), KPC(this));
            } else if (OB_FAIL(tablet_id_to_data_size_.get_refactored(data_subpart_array[subpart_index]->get_tablet_id().id(), tablet_data_size))) {
              LOG_WARN("fail to get tablet data size", K(ret), K(data_subpart_array[subpart_index]->get_tablet_id()), K(tablet_data_size));
            } else if (OB_FAIL(tablet_id_to_data_row_cnt_.get_refactored(data_subpart_array[subpart_index]->get_tablet_id().id(), tablet_data_row_cnt))) {
              LOG_WARN("fail to get tablet data size", K(ret), K(data_subpart_array[subpart_index]->get_tablet_id()), K(tablet_data_row_cnt));
            }
          }
          if (OB_SUCC(ret)) {
            if (pre_data_size == 0 || ((tablet_data_row_cnt + pre_data_row_cnt) <= task_max_data_row_cnt && (tablet_data_size + pre_data_size) <= task_max_data_size)) {
              if (OB_FAIL(out_tablets.push_back(in_tablets.at(i)))) {
                LOG_WARN("fail to push back", K(ret), K(in_tablets.at(i)));
              } else {
                pre_data_size = pre_data_size + tablet_data_size;
                pre_data_row_cnt = pre_data_row_cnt + tablet_data_row_cnt;
              }
              if (OB_SUCC(ret)) {
                int64_t has_scheduled_times = 0;
                if (OB_FAIL(tablet_scheduled_times_statistic_.get_refactored(in_tablets.at(i).id(), has_scheduled_times))) {
                  if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
                    has_scheduled_times = 0;
                    ret = OB_SUCCESS;
                  }
                } else {
                  LOG_WARN("tablet scheduled times exceed 1, need to refresh ls location map", K(ret), K(in_tablets.at(i).id()), K(has_scheduled_times));
                  if (OB_FAIL(refresh_ls_location_map())) {
                    LOG_WARN("fail to refresh ls location map", K(ret));
                  }
                }
                if (OB_SUCC(ret)) {
                  has_scheduled_times = has_scheduled_times + 1;
                  if (OB_FAIL(tablet_scheduled_times_statistic_.set_refactored(in_tablets.at(i).id(), has_scheduled_times, true /* overwrite */))) {
                    LOG_WARN("tablet scheduled times statistic map set fail", K(ret), K(in_tablets.at(i).id()), K(has_scheduled_times));
                  }
                }
              }
            } else {
              break;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLTabletScheduler::get_session_running_lsid(ObIArray<share::ObLSID> &running_ls_ids)
{
  int ret = OB_SUCCESS;
  common::ObAddr inner_sql_exec_addr;
  common::ObArray<ObString> running_sql_info;
  running_ls_ids.reset();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObDDLTaskRecordOperator::get_running_tasks_inner_sql(root_service_->get_sql_proxy(), trace_id_, tenant_id_, task_id_, snapshot_version_, inner_sql_exec_addr, allocator_, running_sql_info))) {
    LOG_WARN("get running tasks inner sql fail", K(ret), K(tenant_id_), K(trace_id_), K(task_id_), K(snapshot_version_), K(inner_sql_exec_addr), K(running_sql_info));
  } else {
    TCRLockGuard guard(lock_);
    common::hash::ObHashMap<share::ObLSID, ObArray<ObTabletID>>::iterator iter;
    for (iter = running_ls_to_tablets_map_.begin(); iter != running_ls_to_tablets_map_.end() && OB_SUCC(ret); ++iter) {
      share::ObLSID &ls_id = iter->first;
      ObArray<ObTabletID> &tablet_queue = iter->second;
      ObArray<ObString> partition_names;
      if (OB_FAIL(ObDDLUtil::get_index_table_batch_partition_names(tenant_id_, ref_data_table_id_, table_id_, tablet_queue, allocator_, partition_names))) {
        LOG_WARN("fail to get index table batch partition names", K(ret), K(tenant_id_), K(ref_data_table_id_), K(table_id_), K(tablet_queue), K(partition_names));
      } else {
        bool is_running_status = false;
        for (int64_t i = 0; i < partition_names.count() && OB_SUCC(ret); i++) {
          is_running_status = false;
          for (int64_t j = 0; j < running_sql_info.count() && OB_SUCC(ret); j++) {
            if (OB_FAIL(ObDDLUtil::check_target_partition_is_running(running_sql_info.at(j), partition_names.at(i), allocator_, is_running_status))) {
              LOG_WARN("fail to check target partition is running", K(ret), K(running_sql_info.at(j)), K(partition_names.at(i)), K(is_running_status));
            } else if (is_running_status) {
              break;
            }
          }
          if (is_running_status) {
            break;
          }
        }
        if (is_running_status && OB_SUCC(ret)) {
          if (OB_FAIL(running_ls_ids.push_back(ls_id))) {
            LOG_WARN("ObArray assign failed", K(ret), K(ls_id));
          }
        }
      }
    }
  }
  return ret;
}

int ObDDLTabletScheduler::get_target_running_ls_tablets(const share::ObLSID &ls_id, ObIArray<ObTabletID> &tablets)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_queue;
  tablets.reset();
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(running_ls_to_tablets_map_.get_refactored(ls_id, tablet_queue))) {
    if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) { // tasks in this ls have finished and reported reply
      LOG_WARN("ls id is not exist in map", K(ret), K(ls_id), K(tablets));
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get tablet queue from refactored", K(ret), K(ls_id), K(tablets));
    }
  } else if (OB_FAIL(tablets.assign(tablet_queue))) {
    LOG_WARN("ObArray assign failed", K(ret), K(tablet_queue));
  }
  return ret;
}

int ObDDLTabletScheduler::get_potential_finished_lsid(const ObIArray<share::ObLSID> &running_ls_ids_now, ObIArray<share::ObLSID> &potential_finished_ls_ids)
{
  int ret = OB_SUCCESS;
  ObArray<share::ObLSID> current_running_ls_ids;
  TCWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(common::get_difference(running_task_ls_ids_before_, running_ls_ids_now, potential_finished_ls_ids))) {
    LOG_WARN("get difference failed", K(ret), K(running_task_ls_ids_before_), K(running_ls_ids_now), K(potential_finished_ls_ids));
  } else if (OB_FAIL(common::get_difference(running_task_ls_ids_before_, potential_finished_ls_ids, current_running_ls_ids))) {
    LOG_WARN("get difference failed", K(ret), K(running_task_ls_ids_before_), K(potential_finished_ls_ids), K(current_running_ls_ids));
  } else if (OB_FAIL(running_task_ls_ids_before_.assign(current_running_ls_ids))) {
    LOG_WARN("ObArray assign failed", K(ret), K(current_running_ls_ids));
  }
  return ret;
}

int ObDDLTabletScheduler::determine_if_need_to_send_new_task(bool &status)
{
  int ret = OB_SUCCESS;
  status = false;
  TCRLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (running_ls_to_tablets_map_.size() == all_ls_to_tablets_map_.size()) {
    status = false;
  } else if (running_ls_to_tablets_map_.size() < all_ls_to_tablets_map_.size()) {
    status = true;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sub map size bigger than map size", K(ret), K(status), K(running_ls_to_tablets_map_.size()), K(all_ls_to_tablets_map_.size()));
  }
  return ret;
}

int ObDDLTabletScheduler::check_target_ls_tasks_completion_status(const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> running_tablet_queue;
  common::hash::ObHashMap<uint64_t, bool> tablet_checksum_status_map;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if ((OB_UNLIKELY(!ls_id.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the parameters is invalid", K(ret), K(ls_id));
  } else if (OB_FAIL(get_target_running_ls_tablets(ls_id, running_tablet_queue))) {
    LOG_WARN("fail to get target running ls tablets", K(ret), K(ls_id),K(running_tablet_queue));
  } else if (OB_UNLIKELY(running_tablet_queue.count() < 1)) {
    // do nothing, the ls tasks have finished and reported
  } else if (OB_FAIL(tablet_checksum_status_map.create(running_tablet_queue.count(), ObModIds::OB_SSTABLE_CREATE_INDEX))) {
    LOG_WARN("fail to create tablet checksum status map", K(ret), K(running_tablet_queue.count()));
  } else if (OB_ISNULL(GCTX.root_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice is null", K(ret));
  } else if (OB_FAIL(ObDDLChecksumOperator::get_tablet_checksum_record_without_execution_id(
    tenant_id_,
    table_id_,
    task_id_,
    running_tablet_queue,
    GCTX.root_service_->get_sql_proxy(),
    tablet_checksum_status_map))) {
    LOG_WARN("fail to get tablet checksum status", K(ret), K(tenant_id_), K(table_id_), K(task_id_), K(running_tablet_queue));
  } else {
    bool is_finished_status = true;
    for (int64_t i = 0; i < running_tablet_queue.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(tablet_checksum_status_map.get_refactored(running_tablet_queue.at(i).id(), is_finished_status))) {
        if (OB_HASH_NOT_EXIST == ret) {
          LOG_WARN("tablet checksum is not exist", K(ret), K(running_tablet_queue.at(i)), K(is_finished_status));
          is_finished_status = false;
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to get refactored", K(ret), K(running_tablet_queue.at(i)), K(is_finished_status));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(confirm_batch_tablets_status(-1, is_finished_status, ls_id, running_tablet_queue))) {
        LOG_WARN("fail to confirm batch tablets status", K(ret), K(is_finished_status), K(running_tablet_queue));
      }
    }
  }
  return ret;
}

 bool ObDDLTabletScheduler::is_all_tasks_finished()
 {
  TCRLockGuard guard(lock_);
  return all_ls_to_tablets_map_.size() < 1;
 }

  bool ObDDLTabletScheduler::is_running_tasks_before_finished()
 {
  TCRLockGuard guard(lock_);
  return running_task_ls_ids_before_.count() < 1;
 }

void ObDDLTabletScheduler::destroy()
{
  is_inited_ = false;
  tenant_id_ = 0;
  table_id_ = 0;
  ref_data_table_id_ = 0;
  task_id_ = 0;
  parallelism_ = 0;
  snapshot_version_ = 0;
  trace_id_.reset();
  all_tablets_.reset();
  running_task_ls_ids_before_.reset();
  all_ls_to_tablets_map_.destroy();
  running_ls_to_tablets_map_.destroy();
  ls_location_map_.destroy();
  running_ls_to_execution_id_.destroy();
  tablet_id_to_data_size_.destroy();
  tablet_id_to_data_row_cnt_.destroy();
  tablet_scheduled_times_statistic_.destroy();
}

int ObTabletIdUpdater::operator() (common::hash::HashMapPair<share::ObLSID, ObArray<ObTabletID>> &entry) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < tablets_->count() && OB_SUCC(ret); i++) {
    for (int64_t j = 0; j < entry.second.count() && OB_SUCC(ret); j++) {
      if (tablets_->at(i) == entry.second.at(j)) {
        if (OB_FAIL(entry.second.remove(j))) {
          LOG_WARN("failed to remove tablet id", K(ret), K(i), K(j), K(entry.second), KP(tablets_));
        }
        break;
      }
    }
  }
  LOG_INFO("remove tablet ids from hash map", K(entry), KP(tablets_));
  return ret;
}

bool HashMapEraseIfNull::operator() (common::hash::HashMapPair<share::ObLSID, ObArray<ObTabletID>> &entry) {
  return entry.second.count() == 0;
}
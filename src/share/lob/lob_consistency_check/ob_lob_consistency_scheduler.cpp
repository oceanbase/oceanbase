/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "ob_lob_consistency_scheduler.h"
#include "share/ob_debug_sync_point.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "share/table/ob_table_ttl_common.h"
#include "ob_lob_check_task.h"
#include "observer/table/ttl/ob_table_ttl_task.h"

namespace oceanbase {
namespace share {
#define HANDLE_USER_TTL()                                                                                              \
  if (!is_valid_lob_cmd(arg.cmd_code_)) {                                                                              \
  } else {                                                                                                             \
    uint64_t tenant_data_version = 0;                                                                                  \
    if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {                                              \
      LOG_WARN("get tenant data version failed", K(ret));                                                              \
    } else if (tenant_data_version < DATA_VERSION_4_5_1_0) {                                                           \
      ret = OB_NOT_SUPPORTED;                                                                                          \
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "tenant data version is less than 4.5.1.0, DBMS_LOB_MANAGER is not supported"); \
      LOG_WARN("tenant data version is less than 4.5.1.0, DBMS_LOB_MANAGER is not supported", K(ret), K(tenant_id_));  \
    } else {                                                                                                           \
      ObTTLTaskType user_lob_req_type = static_cast<ObTTLTaskType>(arg.cmd_code_);                                     \
      TRIGGER_TYPE trigger_type = arg.trigger_type_ == 1 ? USER_TRIGGER : PERIODIC_TRIGGER;                            \
      if (OB_FAIL(add_ttl_task(user_lob_req_type, trigger_type, arg.table_with_tablet_))) {                            \
        LOG_WARN("fail to add task", KR(ret), K_(tenant_id), K(user_lob_req_type));                                    \
      }                                                                                                                \
    }                                                                                                                  \
  }

#define CONSTRUCT_TASK_RECORD_FILTER()                                                                                 \
  ObTTLStatusField task_id_field;                                                                                      \
  task_id_field.field_name_ = ObString("task_id");                                                                     \
  task_id_field.type_ = ObTTLStatusField::UINT_TYPE;                                                                   \
  task_id_field.data_.uint_ = task_info.task_id_;                                                                      \
  ObTTLStatusField ls_id_field;                                                                                        \
  ls_id_field.field_name_ = ObString("ls_id");                                                                         \
  ls_id_field.type_ = ObTTLStatusField::UINT_TYPE;                                                                     \
  ls_id_field.data_.uint_ = task_info.ls_id_.id();                                                                     \
  if (OB_FAIL(filters.push_back(task_id_field))) {                                                                     \
    LOG_WARN("fail to push back task id field", KR(ret));                                                              \
  } else if (OB_FAIL(filters.push_back(ls_id_field))) {                                                                \
    LOG_WARN("fail to push back ls id field", KR(ret));                                                                \
  }

#define GENERATE_ONE_TABLET_TASK(is_repair_task)                                                                       \
  ObTimeGuard guard("GENERATE_ONE_TABLET_TASK", TTL_NORMAL_TIME_THRESHOLD);                                            \
  hash::ObHashMap<uint64_t, table::ObTTLTaskParam> param_map;                                                          \
  table::ObTTLTaskInfo task_info;                                                                                      \
  task_info.tenant_id_ = tenant_id_;                                                                                   \
  table::ObTTLTaskParam param;                                                                                         \
  param.tenant_id_ = tenant_id_;                                                                                       \
  param.is_lob_task_ = true;                                                                                           \
  ObSEArray<ObTabletTablePair, 1> tablet_table_pairs;                                                                  \
  if (IS_NOT_INIT) {                                                                                                   \
    ret = OB_NOT_INIT;                                                                                                 \
    LOG_WARN("tablet ttl mgr not init", KR(ret));                                                                      \
  } else if (need_skip_run()) {                                                                                        \
    ret = OB_EAGAIN;                                                                                                   \
    FLOG_INFO("skip this run cuz of leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));                   \
  } else if (!local_tenant_task_.row_key_.empty()) {                                                                   \
    if (OB_FAIL(ObLobCheckTask::parse_table_and_tablet(tenant_id_, ls_, local_tenant_task_.row_key_, is_repair_task,   \
                                                       0, 0, 1, tablet_table_pairs))) {                                \
      LOG_WARN("fail to parse table and tablet", KR(ret), K_(tenant_id));                                              \
    }                                                                                                                  \
  } else if (is_repair_task) {                                                                                         \
    if (OB_FAIL(ObLobCheckTask::read_orphan_data_to_pairs(tenant_id_, ls_, 0, tablet_table_pairs))) {                  \
      LOG_WARN("fail to read orphan data to pairs", KR(ret), K(tenant_id_), K(ls_->get_ls_id()));                      \
    }                                                                                                                  \
  } else if (OB_FAIL(ObLobCheckTask::cache_table_and_tablet_pairs(tenant_id_, ls_, 0, 0, 1, tablet_table_pairs))) {    \
    LOG_WARN("fail to cache table and tablet pairs", KR(ret), K_(tenant_id));                                          \
  }                                                                                                                    \
  if (OB_FAIL(ret) || tablet_table_pairs.empty()) {                                                                    \
  } else {                                                                                                             \
    const ObTabletTablePair &first_pair = tablet_table_pairs.at(0);                                                    \
    task_info.tablet_id_ = first_pair.get_tablet_id();                                                                 \
    task_info.table_id_ = first_pair.get_table_id();                                                                   \
    if (OB_FAIL(generate_one_tablet_task(task_info, param))) {                                                         \
      LOG_WARN("fail to generate task", KR(ret), K(task_info), K(param));                                              \
    } else {                                                                                                           \
      mark_tenant_checked();                                                                                           \
      LOG_INFO("generate LOB task successfully", K_(tenant_id), K(is_repair_task), K(task_info.table_id_),             \
               K(task_info.tablet_id_), "total_pairs", tablet_table_pairs.count());                                    \
    }                                                                                                                  \
  }                                                                                                                    \
  FLOG_INFO("finish generate tenant tasks", KR(ret), KPC_(ls), K_(tenant_id));

#define DELETE_LOB_RECORD(task_info, trans)                                                                            \
  ObSqlString sql;                                                                                                     \
  int64_t affected_rows = 0;                                                                                           \
  if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %ld AND task_id = %ld AND ls_id = %ld",                 \
                             OB_ALL_KV_TTL_TASK_TNAME, tenant_id_, task_info.task_id_, task_info.ls_id_.id()))) {      \
    LOG_WARN("fail to assign sql", K(ret));                                                                            \
  } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id_), sql.ptr(), affected_rows))) {                         \
    LOG_WARN("fail to execute sql", K(ret), K(sql));                                                                   \
  }

#define MOVE_ALL_TASK_TO_HISTORY_TABLE()                                                                               \
  int64_t one_move_rows = ObLobConsistencyUtil::LOB_CHECK_BATCH_LS_SIZE;                                               \
  int64_t delete_rows = ObLobConsistencyUtil::LOB_CHECK_BATCH_LS_SIZE;                                                 \
  while (OB_SUCC(ret) && delete_rows == ObLobConsistencyUtil::LOB_CHECK_BATCH_LS_SIZE) {                               \
    ObMySQLTransaction trans;                                                                                          \
    ObSqlString sql;                                                                                                   \
    delete_rows = 0;                                                                                                   \
    if (ATOMIC_LOAD(&need_do_for_switch_)) {                                                                           \
      ret = OB_EAGAIN;                                                                                                 \
      FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));             \
    } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {                                     \
      LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));                                                      \
    } else if (OB_FAIL(sql.assign_fmt("delete from %s where  tenant_id = %d and tablet_id >= 0 and table_id >= 0 and " \
                                      "task_id = %ld limit %ld",                                                       \
                                      share::OB_ALL_KV_TTL_TASK_TNAME, tenant_id_, tenant_task_.ttl_status_.task_id_,  \
                                      one_move_rows))) {                                                               \
      LOG_WARN("fail to assign sql", K(ret));                                                                          \
    } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id_), sql.ptr(), delete_rows))) {                         \
      LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id_));                                                  \
    }                                                                                                                  \
    if (trans.is_started()) {                                                                                          \
      int tmp_ret = OB_SUCCESS;                                                                                        \
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {                                                         \
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));                                                         \
        ret = OB_SUCC(ret) ? tmp_ret : ret;                                                                            \
      }                                                                                                                \
    }                                                                                                                  \
  }                                                                                                                    \
  if (OB_SUCC(ret)) {                                                                                                  \
    ObMySQLTransaction trans;                                                                                          \
    ObSqlString sql;                                                                                                   \
    if (ATOMIC_LOAD(&need_do_for_switch_)) {                                                                           \
      ret = OB_EAGAIN;                                                                                                 \
      FLOG_INFO("exit timer task once cuz leader switch", KR(ret), K_(is_leader), K_(need_do_for_switch));             \
    } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {                                     \
      LOG_WARN("fail start transaction", KR(ret), K_(tenant_id));                                                      \
    } else if (OB_FAIL(update_task_status(tenant_task_.ttl_status_.task_id_, OB_TTL_TASK_FINISH, trans))) {            \
      LOG_WARN("fail to update task status", KR(ret));                                                                 \
    } else if (OB_FAIL(                                                                                                \
                   sql.assign_fmt("delete from %s where tenant_id = %ld and table_id = %ld and tablet_id = %ld and "   \
                                  "task_id = %ld limit %ld",                                                           \
                                  share::OB_ALL_KV_TTL_TASK_TNAME, tenant_id_, get_tenant_task_table_id(),             \
                                  get_tenant_task_tablet_id(), tenant_task_.ttl_status_.task_id_, one_move_rows))) {   \
      LOG_WARN("fail to assign sql", K(ret));                                                                          \
    } else if (OB_FAIL(trans.write(gen_meta_tenant_id(tenant_id_), sql.ptr(), delete_rows))) {                         \
      LOG_WARN("fail to execute sql", K(ret), K(sql), K(tenant_id_));                                                  \
    } else if (delete_rows != 1) {                                                                                     \
      ret = OB_ERR_UNEXPECTED;                                                                                         \
      LOG_WARN("unexpected delete tenant task", K(ret), K(delete_rows), K(tenant_id_),                                 \
               K(tenant_task_.ttl_status_.task_id_));                                                                  \
    }                                                                                                                  \
    if (trans.is_started()) {                                                                                          \
      int tmp_ret = OB_SUCCESS;                                                                                        \
      if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {                                                         \
        LOG_WARN("fail to commit trans", KR(ret), K(tmp_ret));                                                         \
        ret = OB_SUCC(ret) ? tmp_ret : ret;                                                                            \
      }                                                                                                                \
    }                                                                                                                  \
  }

/* ---------------------------------------  ObLobCheckScheduler --------------------------------------- */
int ObLobCheckScheduler::handle_user_ttl(const obrpc::ObTTLRequestArg &arg)
{
  int ret = OB_SUCCESS;
  HANDLE_USER_TTL();
  ObTTLTaskType user_lob_req_type = static_cast<ObTTLTaskType>(arg.cmd_code_);
  if (user_lob_req_type == OB_LOB_CHECK_TRIGGER) in_processing_cleanup_ = true;
  return ret;
}

int ObLobCheckScheduler::check_all_table_finished(bool &all_finished)
{
  DEBUG_SYNC(BEFORE_CHECK_TTL_TASK_FINISH);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobConsistencyUtil::check_all_ls_finished(tenant_id_, false, tenant_task_.ttl_status_.row_key_,
                                                          tenant_task_.ttl_status_.task_id_, all_finished))) {
    LOG_WARN("fail to check all ls finished", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObLobCheckScheduler::clean_lob_exception_records() {
  int ret = OB_SUCCESS;
  int64_t processed_count = 0;
  const int64_t batch_size = share::ObLobConsistencyUtil::LOB_EXCEPTION_CLEAN_BATCH_SIZE;

  if (OB_FAIL(share::ObLobConsistencyUtil::clean_invalid_exception_records(tenant_id_,
                                                                            *sql_proxy_,
                                                                            last_clean_ls_id_,
                                                                            last_clean_table_id_,
                                                                            batch_size,
                                                                            processed_count))) {
    LOG_WARN("fail to clean orphaned LOB exception records", K(ret), K(tenant_id_),
              K(last_clean_ls_id_), K(last_clean_table_id_));
    // Don't fail the entire task, just log the error
  } else {
    if (processed_count > 0) {
      in_processing_cleanup_ = true;
      LOG_INFO("cleaned orphaned LOB exception records", K(tenant_id_), K(processed_count),
                K(last_clean_ls_id_), K(last_clean_table_id_));
    }

    // If completed a full scan, reset progress and update last clean time
    if (processed_count < batch_size) {
      last_clean_ls_id_ = 0;
      last_clean_table_id_ = 0;
      last_lob_clean_time_ = ObTimeUtility::current_time();
      in_processing_cleanup_ = false;
      LOG_WARN("cleaned invalid LOB exception records successfully", K(ret), K(tenant_id_));
    }
  }
  LOG_INFO("cleaned invalid LOB exception records successfully", K(ret), K(tenant_id_));
  return ret;
}

bool ObLobCheckScheduler::can_trigger_lob_clean_task() const {
  int ret = OB_SUCCESS;
  bool enable = false;
  uint64_t data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("get data version failed", K(ret));
  } else if (data_version >= DATA_VERSION_4_5_1_0) {
    const int64_t now = ObTimeUtility::current_time();
    const int64_t time_since_last_clean = now - last_lob_clean_time_;
    if (in_processing_cleanup_ || time_since_last_clean >= share::ObLobConsistencyUtil::LOB_EXCEPTION_CLEAN_INTERVAL) {
      enable = true;
    }
  }
  return enable;
}


void ObLobCheckScheduler::clear_ttl_history_task_record()
{
  ObCurTraceId::init(GCONF.self_addr_);
  int ret = OB_SUCCESS;
  bool enable_lob_clean = can_trigger_lob_clean_task();
  if (ObTTLUtil::check_can_do_work()) {
    const int64_t now = ObTimeUtility::current_time();
    ObSqlString sql;
    if (tenant_id_ == OB_SYS_TENANT_ID) {
    } else if (!ObTTLUtil::check_can_process_tenant_tasks(tenant_id_)) {
      // do nothinig
    } else {
      if (enable_lob_clean && OB_FAIL(clean_lob_exception_records())) {
        // overwrite ret
        LOG_WARN("fail to clean orphaned LOB exception records", KR(ret));
      }
    }
  }
}

int ObLobCheckScheduler::move_all_task_to_history_table()
{
  int ret = OB_SUCCESS;
  MOVE_ALL_TASK_TO_HISTORY_TABLE();
  return ret;
}

int ObLobCheckScheduler::calc_next_task_state(ObTTLTaskType user_cmd_type, ObTTLTaskStatus curr_state,
                                              ObTTLTaskStatus &next_state)
{
  int ret = OB_SUCCESS;
  if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND && user_cmd_type == ObTTLTaskType::OB_LOB_CHECK_RESUME) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE;
  } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE &&
             user_cmd_type == ObTTLTaskType::OB_LOB_CHECK_SUSPEND) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND;
  } else if (curr_state != ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL &&
             user_cmd_type == ObTTLTaskType::OB_LOB_CHECK_CANCEL) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
  } else {
    ret = OB_NOT_SUPPORTED;
    const char *status_cstr = ObTTLUtil::get_ttl_tenant_status_cstr(curr_state);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, status_cstr);
    LOG_WARN("lob check command is not allowed in current tenant lob check status", KR(ret), K(curr_state),
             K(user_cmd_type), K_(tenant_id));
  }
  return ret;
}

/* ---------------------------------------  ObLobCheckTabletScheduler --------------------------------------- */

int ObLobCheckTabletScheduler::check_and_generate_tablet_tasks()
{
  int ret = OB_SUCCESS;
  GENERATE_ONE_TABLET_TASK(false);
  return ret;
}

int ObLobCheckTabletScheduler::construct_task_record_filter(const table::ObTTLTaskInfo &task_info,
                                                            ObTTLStatusFieldArray &filters)
{
  int ret = OB_SUCCESS;
  CONSTRUCT_TASK_RECORD_FILTER();
  return ret;
}

bool ObLobCheckTabletScheduler::need_cancel_task(const int ret)
{
  return (OB_NOT_MASTER == ret || OB_LS_NOT_EXIST == ret);
}

int ObLobCheckTabletScheduler::generate_dag_task(table::ObTTLTaskInfo &task_info, table::ObTTLTaskParam &task_para)
{
  return generate_ttl_dag<ObLobCheckTask, table::ObTableTTLDag>(task_info, task_para);
}

int ObLobCheckTabletScheduler::handle_single_exception_table_op(ObArenaAllocator &allocator,
                                                                ObJsonNode *exception_tablets_json,
                                                                ObLobInconsistencyType inconsistency_type,
                                                                common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObJsonNode *existing_tablets = nullptr;
  // { "table_id1": [tablet_id1, tablet_id2, ...], "table_id2": [...], ... }

  if (exception_tablets_json->json_type() == ObJsonNodeType::J_OBJECT) {
    ObJsonObject *exception_obj = static_cast<ObJsonObject *>(exception_tablets_json);
    uint64_t table_count = exception_obj->element_count();

    for (uint64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
      ObString table_id_key;
      ObJsonNode *tablets_node = nullptr;

      if (OB_FAIL(exception_obj->get_key(i, table_id_key))) {
        LOG_WARN("fail to get key", K(ret), K(i));
      } else if (OB_ISNULL(tablets_node = exception_obj->get_value(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablets array", K(ret), K(i), K(table_id_key));
      } else if (tablets_node->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablets node is not array", K(ret), K(i), K(table_id_key));
      } else {
        uint64_t table_id = *reinterpret_cast<const uint64_t *>(table_id_key.ptr());
        LOG_WARN("wuhuang::table_id_key: %s, table_id: %lu", K(table_id_key), K(table_id));
        // Merge with existing tablets
        if (OB_FAIL(ObLobConsistencyUtil::insert_or_merge_exception_tablets(tenant_id_, ls_->get_ls_id().id(), table_id,
                                                                            tablets_node, inconsistency_type, trans))) {
          LOG_WARN("fail to merge exception tablets", K(ret), K(tenant_id_), K(ls_->get_ls_id().id()), K(table_id));
        }
      }
    }
  }
  return ret;
}

int ObLobCheckTabletScheduler::construct_sys_table_record(table::ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record)
{
  int ret = OB_SUCCESS;
  char* cnt_array = nullptr;
  ObSqlString sql;
  ObLobTaskInfo &lob_task_info = ctx->lob_task_info_;
  if (OB_FAIL(ObTabletTTLScheduler::construct_sys_table_record(ctx, ttl_record))) {
    LOG_WARN("fail to construct sys table record", K(ret));
  } else if (OB_FAIL(sql.append(ttl_record.ret_code_))) {
    LOG_WARN("fail to append ret code", K(ret));
  } else if (OB_FAIL(sql.append("|"))) {
    LOG_WARN("fail to append |", K(ret));
  } else if (OB_FAIL(sql.append_fmt("%ld|%ld|%ld", lob_task_info.lob_not_found_cnt_, lob_task_info.lob_length_mismatch_cnt_, lob_task_info.lob_orphan_cnt_))) {
    LOG_WARN("fail to append lob task info", K(ret));
  } else if (OB_FAIL(ob_write_string(lob_task_info.exception_allocator_, sql.string(), ttl_record.ret_code_))) {
    LOG_WARN("fail to write string", K(ret));
  }
  return ret;
}

int ObLobCheckTabletScheduler::handle_exception_table_op(table::ObTTLTaskCtx *ctx, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLobTaskInfo &lob_task_info = ctx->lob_task_info_;
  uint64_t ls_id = ctx->task_info_.ls_id_.id();

  // Step 1: Insert/merge new exception tablets
  if (OB_NOT_NULL(lob_task_info.orphan_tablets_json_)) {
    if (OB_FAIL(handle_single_exception_table_op(lob_task_info.exception_allocator_, lob_task_info.orphan_tablets_json_, LOB_ORPHAN_TYPE,
                                                 trans))) {
      LOG_WARN("fail to handle single exception table op", K(ret));
    } else {
      lob_task_info.orphan_tablets_json_ = nullptr;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(lob_task_info.not_found_tablets_json_)) {
    if (OB_FAIL(handle_single_exception_table_op(lob_task_info.exception_allocator_, lob_task_info.not_found_tablets_json_, LOB_MISS_TYPE,
                                                 trans))) {
      LOG_WARN("fail to handle single exception table op", K(ret));
    } else {
      lob_task_info.not_found_tablets_json_ = nullptr;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(lob_task_info.mismatch_len_tablets_json_)) {
    if (OB_FAIL(handle_single_exception_table_op(lob_task_info.exception_allocator_, lob_task_info.mismatch_len_tablets_json_,
                                                 LOB_MISMATCH_LEN_TYPE, trans))) {
      LOG_WARN("fail to handle single exception table op", K(ret));
    } else {
      lob_task_info.mismatch_len_tablets_json_ = nullptr;
    }
  }

  // Step 2: Remove tablets that have become consistent
  if (OB_SUCC(ret) && OB_NOT_NULL(lob_task_info.not_found_removed_tablets_json_)) {
    if (OB_FAIL(remove_consistent_exception_tablets(ls_id, lob_task_info.not_found_removed_tablets_json_,
                                                    LOB_MISS_TYPE, trans))) {
      LOG_WARN("fail to remove consistent not_found tablets", K(ret), K(ls_id));
    } else {
      lob_task_info.not_found_removed_tablets_json_ = nullptr;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(lob_task_info.mismatch_len_removed_tablets_json_)) {
    if (OB_FAIL(remove_consistent_exception_tablets(ls_id, lob_task_info.mismatch_len_removed_tablets_json_,
                                                    LOB_MISMATCH_LEN_TYPE, trans))) {
      LOG_WARN("fail to remove consistent mismatch_len tablets", K(ret), K(ls_id));
    } else {
      lob_task_info.mismatch_len_removed_tablets_json_ = nullptr;
    }
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(lob_task_info.orphan_removed_tablets_json_)) {
    if (OB_FAIL(remove_consistent_exception_tablets(ls_id, lob_task_info.orphan_removed_tablets_json_,
                                                    LOB_ORPHAN_TYPE, trans))) {
      LOG_WARN("fail to remove consistent orphan tablets", K(ret), K(ls_id));
    } else {
      lob_task_info.orphan_removed_tablets_json_ = nullptr;
    }
  }

  lob_task_info.exception_allocator_.reset();
  return ret;
}

int ObLobCheckTabletScheduler::remove_consistent_exception_tablets(uint64_t ls_id,
                                                                   ObJsonNode *removed_tablets_json,
                                                                   ObLobInconsistencyType inconsistency_type,
                                                                   common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(removed_tablets_json)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("removed_tablets_json is null", K(ret));
  } else if (removed_tablets_json->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("removed_tablets_json is not object", K(ret), K(removed_tablets_json->json_type()));
  } else {
    ObJsonObject *exception_obj = static_cast<ObJsonObject *>(removed_tablets_json);
    uint64_t table_count = exception_obj->element_count();

    for (uint64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
      ObString table_id_key;
      ObJsonNode *tablets_node = nullptr;

      if (OB_FAIL(exception_obj->get_key(i, table_id_key))) {
        LOG_WARN("fail to get key", K(ret), K(i));
      } else if (OB_ISNULL(tablets_node = exception_obj->get_value(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get tablets array", K(ret), K(i), K(table_id_key));
      } else if (tablets_node->json_type() != ObJsonNodeType::J_ARRAY) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablets node is not array", K(ret), K(i), K(table_id_key));
      } else {
        uint64_t table_id = *reinterpret_cast<const uint64_t *>(table_id_key.ptr());

        // Remove tablets that have become consistent from exception table
        if (OB_FAIL(ObLobConsistencyUtil::remove_exception_tablets(tenant_id_, ls_id, table_id,
                                                                   tablets_node, trans, inconsistency_type))) {
          LOG_WARN("fail to remove exception tablets", K(ret), K(tenant_id_), K(ls_id), K(table_id), K(inconsistency_type));
        } else {
          LOG_INFO("successfully removed consistent tablets from exception table", K(tenant_id_), K(ls_id),
                   K(table_id), K(inconsistency_type));
        }
      }
    }
  }
  return ret;
}

int ObLobCheckTabletScheduler::delete_lob_record(table::ObTTLTaskInfo &task_info, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  DELETE_LOB_RECORD(task_info, trans);
  return ret;
}

/* ---------------------------------------  ObLobRepairScheduler --------------------------------------- */

int ObLobRepairScheduler::check_all_table_finished(bool &all_finished)
{
  DEBUG_SYNC(BEFORE_CHECK_TTL_TASK_FINISH);
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObLobConsistencyUtil::check_all_ls_finished(tenant_id_, true, tenant_task_.ttl_status_.row_key_,
                                                          tenant_task_.ttl_status_.task_id_, all_finished))) {
    LOG_WARN("fail to check all ls finished", KR(ret), K_(tenant_id));
  }
  return ret;
}

int ObLobRepairScheduler::handle_user_ttl(const obrpc::ObTTLRequestArg &arg)
{
  int ret = OB_SUCCESS;
  HANDLE_USER_TTL();
  return ret;
}

int ObLobRepairScheduler::move_all_task_to_history_table()
{
  int ret = OB_SUCCESS;
  MOVE_ALL_TASK_TO_HISTORY_TABLE();
  return ret;
}

int ObLobRepairScheduler::calc_next_task_state(ObTTLTaskType user_cmd_type, ObTTLTaskStatus curr_state,
                                                ObTTLTaskStatus &next_state)
{
  int ret = OB_SUCCESS;
  if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND && user_cmd_type == ObTTLTaskType::OB_LOB_REPAIR_RESUME) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE;
  } else if (curr_state == ObTTLTaskStatus::OB_RS_TTL_TASK_CREATE &&
             user_cmd_type == ObTTLTaskType::OB_LOB_REPAIR_SUSPEND) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_SUSPEND;
  } else if (curr_state != ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL &&
             user_cmd_type == ObTTLTaskType::OB_LOB_REPAIR_CANCEL) {
    next_state = ObTTLTaskStatus::OB_RS_TTL_TASK_CANCEL;
  } else {
    ret = OB_NOT_SUPPORTED;
    const char *status_cstr = ObTTLUtil::get_ttl_tenant_status_cstr(curr_state);
    LOG_USER_ERROR(OB_NOT_SUPPORTED, status_cstr);
    LOG_WARN("lob repair command is not allowed in current tenant lob repair status", KR(ret), K(curr_state),
             K(user_cmd_type), K_(tenant_id));
  }
  return ret;
}

/* ---------------------------------------  ObLobRepairTabletScheduler --------------------------------------- */

int ObLobRepairTabletScheduler::check_and_generate_tablet_tasks()
{
  int ret = OB_SUCCESS;
  GENERATE_ONE_TABLET_TASK(true);
  return ret;
}

int ObLobRepairTabletScheduler::construct_task_record_filter(const table::ObTTLTaskInfo &task_info,
                                                              ObTTLStatusFieldArray &filters)
{
  int ret = OB_SUCCESS;
  CONSTRUCT_TASK_RECORD_FILTER();
  return ret;
}

int ObLobRepairTabletScheduler::generate_dag_task(table::ObTTLTaskInfo &task_info, table::ObTTLTaskParam &task_para)
{
  return generate_ttl_dag<ObLobCheckTask, table::ObTableTTLDag>(task_info, task_para);
}

int ObLobRepairTabletScheduler::handle_exception_table_op(table::ObTTLTaskCtx *ctx, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObLobTaskInfo &lob_task_info = ctx->lob_task_info_;
  if (OB_NOT_NULL(lob_task_info.orphan_tablets_json_)) {
    uint64_t ls_id = ctx->task_info_.ls_id_.id();

    if (lob_task_info.orphan_tablets_json_->json_type() == ObJsonNodeType::J_OBJECT) {
      ObJsonObject *exception_obj = static_cast<ObJsonObject *>(lob_task_info.orphan_tablets_json_);
      uint64_t table_count = exception_obj->element_count();

      for (uint64_t i = 0; OB_SUCC(ret) && i < table_count; ++i) {
        ObString table_id_key;
        ObJsonNode *tablets_node = nullptr;

        if (OB_FAIL(exception_obj->get_key(i, table_id_key))) {
          LOG_WARN("fail to get key", K(ret), K(i));
        } else if (OB_ISNULL(tablets_node = exception_obj->get_value(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get tablets array", K(ret), K(i), K(table_id_key));
        } else if (tablets_node->json_type() != ObJsonNodeType::J_ARRAY) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablets node is not array", K(ret), K(i), K(table_id_key));
        } else {
          uint64_t table_id = *reinterpret_cast<const uint64_t *>(table_id_key.ptr());

          // Remove repaired tablets from exception table
          if (OB_FAIL(
                  ObLobConsistencyUtil::remove_exception_tablets(tenant_id_, ls_id, table_id, tablets_node, trans))) {
            LOG_WARN("fail to remove exception tablets", K(ret), K(tenant_id_), K(ls_id), K(table_id));
          } else {
            LOG_INFO("successfully removed repaired tablets from exception table", K(tenant_id_), K(ls_id),
                     K(table_id));
          }
        }
      }
    }

    // Clear exception_table_tablets_ in ctx after sync, reset for next batch
    if (OB_SUCC(ret)) {
      lob_task_info.exception_allocator_.reset();
      lob_task_info.not_found_tablets_json_ = nullptr;
      lob_task_info.orphan_tablets_json_ = nullptr;
      lob_task_info.mismatch_len_tablets_json_ = nullptr;
      lob_task_info.not_found_removed_tablets_json_ = nullptr;
      lob_task_info.mismatch_len_removed_tablets_json_ = nullptr;
      lob_task_info.orphan_removed_tablets_json_ = nullptr;
    }
  }

  return ret;
}

bool ObLobRepairTabletScheduler::need_cancel_task(const int ret)
{
  return (OB_NOT_MASTER == ret || OB_LS_NOT_EXIST == ret);
}

int ObLobRepairTabletScheduler::delete_lob_record(table::ObTTLTaskInfo &task_info, common::ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  DELETE_LOB_RECORD(task_info, trans);
  return ret;
}

}  // namespace share
}  // namespace oceanbase
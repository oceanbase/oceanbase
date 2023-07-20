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

#include "storage/backup/ob_backup_operator.h"
#include "common/ob_smart_var.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "observer/ob_sql_client_decorator.h"
#include "share/backup/ob_backup_data_table_operator.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"

#include <algorithm>

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace backup {

int ObLSBackupOperator::insert_ls_backup_task_info(const uint64_t tenant_id, const int64_t task_id, const int64_t turn_id,
    const int64_t retry_id, const share::ObLSID &ls_id, const int64_t backup_set_id,
    const share::ObBackupDataType &backup_data_type, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml_splicer;
  int64_t affected_rows = 0;
  ObBackupLSTaskInfo task_info;
  task_info.task_id_ = task_id;
  task_info.tenant_id_ = tenant_id;
  task_info.ls_id_ = ls_id;
  task_info.turn_id_ = turn_id;
  task_info.retry_id_ = retry_id;
  task_info.backup_data_type_ = backup_data_type.type_;
  task_info.backup_set_id_ = backup_set_id;
  task_info.max_file_id_ = OB_INITIAL_BACKUP_MAX_FILE_ID;
  if (OB_FAIL(fill_ls_task_info_(task_info, dml_splicer))) {
    LOG_WARN("failed to fill ls task info", K(ret), K(task_info));
  } else if (OB_FAIL(dml_splicer.splice_insert_sql(OB_ALL_BACKUP_LS_TASK_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("insert ls task result", K(sql));
  }
  return ret;
}

int ObLSBackupOperator::report_ls_backup_task_info(const uint64_t tenant_id, const int64_t task_id, const int64_t turn_id,
    const int64_t retry_id, const share::ObBackupDataType &backup_data_type, const ObLSBackupStat &stat,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml_splicer;
  int64_t affected_rows = 0;
  ObBackupLSTaskInfo task_info;
  task_info.task_id_ = task_id;
  task_info.tenant_id_ = tenant_id;
  task_info.ls_id_ = stat.ls_id_;
  task_info.turn_id_ = turn_id;
  task_info.retry_id_ = retry_id;
  task_info.backup_data_type_ = backup_data_type.type_;
  task_info.backup_set_id_ = stat.backup_set_id_;
  task_info.input_bytes_ = stat.input_bytes_;
  task_info.output_bytes_ = stat.output_bytes_;
  task_info.tablet_count_ = stat.finish_tablet_count_;
  task_info.finish_tablet_count_ = stat.finish_tablet_count_;
  task_info.macro_block_count_ = stat.finish_macro_block_count_;
  task_info.finish_macro_block_count_ = stat.finish_macro_block_count_;
  task_info.max_file_id_ = stat.file_id_;
  if (OB_FAIL(fill_ls_task_info_(task_info, dml_splicer))) {
    LOG_WARN("failed to fill ls task info", K(ret), K(task_info));
  } else if (OB_FAIL(dml_splicer.splice_insert_update_sql(OB_ALL_BACKUP_LS_TASK_INFO_TNAME, sql))) {
    LOG_WARN("failed to splice insert update sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("report ls backup task info", K(sql));
  }
  return ret;
}

int ObLSBackupOperator::get_backup_ls_task_info(const uint64_t tenant_id, const int64_t task_id,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id,
    const share::ObBackupDataType &backup_data_type, const bool for_update, ObBackupLSTaskInfo &task_info,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  task_info.reset();
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || task_id < 0 || !ls_id.is_valid() || turn_id < 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN(
        "get invalid args", K(ret), K(tenant_id), K(task_id), K(ls_id), K(turn_id), K(retry_id), K(backup_data_type));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where task_id = %ld and tenant_id = %lu and "
                                    "ls_id = %ld and "
                                    "turn_id = %ld and retry_id = %ld and data_type = %ld",
                 OB_ALL_BACKUP_LS_TASK_INFO_TNAME,
                 task_id,
                 tenant_id,
                 ls_id.id(),
                 turn_id,
                 retry_id,
                 static_cast<int64_t>(backup_data_type.type_)))) {
    LOG_WARN("failed to append sql", K(ret));
  } else if (for_update && OB_FAIL(sql.append_fmt(" for update"))) {
    LOG_WARN("failed to append fmt", K(ret), K(sql));
  } else {
    ObArray<ObBackupLSTaskInfo> task_infos;
    HEAP_VAR(ObMySQLProxy::ReadResult, res)
    {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_ls_task_info_results_(*result, task_infos))) {
        LOG_WARN("failed to parse result", K(ret), K(result));
      } else if (task_infos.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("entry not exist", K(ret), K(sql));
      } else if (1 != task_infos.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("task infos should be 1", K(task_infos));
      } else {
        task_info = task_infos.at(0);
      }
    }
  }
  return ret;
}

int ObLSBackupOperator::get_all_retries(const int64_t task_id, const uint64_t tenant_id,
    const share::ObBackupDataType &backup_data_type, const share::ObLSID &ls_id,
    common::ObIArray<ObBackupRetryDesc> &retry_list, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (tenant_id == OB_INVALID_TENANT_ID || !backup_data_type.is_valid() || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invlaid argument", K(ret), K(tenant_id), K(backup_data_type), K(ls_id));
  } else if (OB_FAIL(sql.assign_fmt("select * from %s where task_id = %ld and tenant_id = %lu and "
                                    "data_type = %ld and final = 'True'",
                 OB_ALL_BACKUP_LS_TASK_INFO_TNAME,
                 task_id,
                 tenant_id,
                 static_cast<int64_t>(backup_data_type.type_)))) {
    LOG_WARN("failed to append sql", K(ret), K(task_id), K(tenant_id));
  } else if (0 != ls_id.id() && OB_FAIL(sql.append_fmt(" and ls_id = %ld", ls_id.id()))) {
    LOG_WARN("failed to append sql", K(ret), K(ls_id));
  } else {
    ObArray<ObBackupLSTaskInfo> task_infos;
    HEAP_VAR(ObMySQLProxy::ReadResult, res)
    {
      common::sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
        LOG_WARN("failed to exec sql", K(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret), K(sql));
      } else if (OB_FAIL(parse_ls_task_info_results_(*result, task_infos))) {
        LOG_WARN("failed to parse result", K(ret), K(retry_list));
      } else {
        ObBackupRetryDesc retry_desc;
        for (int64_t i = 0; OB_SUCC(ret) && i < task_infos.count(); ++i) {
          const ObBackupLSTaskInfo &task_info = task_infos.at(i);
          retry_desc.ls_id_ = task_info.ls_id_;
          retry_desc.turn_id_ = task_info.turn_id_;
          retry_desc.retry_id_ = task_info.retry_id_;
          retry_desc.last_file_id_ = task_info.max_file_id_;
          if (OB_INITIAL_BACKUP_MAX_FILE_ID == task_info.max_file_id_) {
            // do nothing
          } else if (OB_FAIL(retry_list.push_back(retry_desc))) {
            LOG_WARN("failed to push back", K(ret), K(retry_desc));
          }
        }
      }
    }
  }
  return ret;
}

int ObLSBackupOperator::mark_ls_task_info_final(const int64_t task_id, const uint64_t tenant_id,
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id,
    const share::ObBackupDataType &backup_data_type, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (task_id <= 0 || OB_INVALID_ID == tenant_id || !ls_id.is_valid() || turn_id <= 0 || retry_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(tenant_id), K(ls_id), K(turn_id), K(retry_id));
  } else if (OB_FAIL(sql.assign_fmt("update %s set final = 'True' where task_id = %ld and "
                                    "tenant_id = %lu and ls_id ="
                                    "%ld and turn_id = %ld and retry_id = %ld and data_type = %ld",
                 OB_ALL_BACKUP_LS_TASK_INFO_TNAME,
                 task_id,
                 tenant_id,
                 ls_id.id(),
                 turn_id,
                 retry_id,
                 static_cast<int64_t>(backup_data_type.type_)))) {
    LOG_WARN("failed to init sql", K(ret));
  } else if (OB_FAIL(sql_client.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    FLOG_INFO("success update ls task info to final", K(sql));
  }
  return ret;
}

int ObLSBackupOperator::get_prev_backup_set_desc(const uint64_t tenant_id, const int64_t backup_set_id, const int64_t dest_id,
    share::ObBackupSetFileDesc &prev_desc, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  share::ObBackupSetFileDesc cur_desc;
  const bool for_update = false;
  if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(
          sql_client, for_update, backup_set_id, OB_START_INCARNATION, tenant_id, dest_id, cur_desc))) {
    LOG_WARN("failed to get backup set", K(ret), K(backup_set_id), K(tenant_id));
  } else if (0 == cur_desc.prev_inc_backup_set_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur backup set is full backup", K(cur_desc));
  } else if (OB_FAIL(ObBackupSetFileOperator::get_backup_set_file(sql_client,
                 for_update,
                 cur_desc.prev_inc_backup_set_id_,
                 OB_START_INCARNATION,
                 tenant_id,
                 dest_id,
                 prev_desc))) {
    LOG_WARN("failed to get backup set", K(ret), K(cur_desc.prev_inc_backup_set_id_), K(tenant_id));
  }
  return ret;
}

int ObLSBackupOperator::report_ls_task_finish(const uint64_t tenant_id, const int64_t task_id, 
    const share::ObLSID &ls_id, const int64_t turn_id, const int64_t retry_id, const int64_t result, 
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = -1;
  if (task_id < 0 || OB_INVALID_ID == tenant_id || !ls_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(task_id), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(sql.assign_fmt("update %s set status = 'FINISH', result = %ld where tenant_id "
                                    "= %lu and task_id = %ld and ls_id = %ld and turn_id = %ld and retry_id = %ld",
                 OB_ALL_BACKUP_LS_TASK_TNAME,
                 result,
                 tenant_id,
                 task_id,
                 ls_id.id(),
                 turn_id,
                 retry_id))) {
    LOG_WARN("failed to init sql", K(ret), K(task_id), K(tenant_id), K(ls_id), K(turn_id), K(retry_id));
  } else if (OB_FAIL(sql_client.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to exec sql", K(ret), K(sql));
  } else {
    FLOG_INFO("report ls task result", K(sql));
  }
  return ret;
}

int ObLSBackupOperator::get_all_backup_ls_id(const uint64_t tenant_id, const int64_t task_id,
    common::ObIArray<share::ObLSID> &ls_array, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_INVALID_ID == tenant_id || task_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(task_id), K(tenant_id));
  } else if (OB_FAIL(construct_query_backup_sql_(tenant_id, task_id, sql))) {
    LOG_WARN("failed to construct query backup sql", K(ret), K(tenant_id), K(task_id));
  } else if (OB_FAIL(get_distinct_ls_id_(tenant_id, sql, ls_array, sql_client))) {
    LOG_WARN("failed to get distinct ls id", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObLSBackupOperator::get_all_archive_ls_id(const uint64_t tenant_id, const int64_t dest_id,
    const share::SCN &start_scn, const share::SCN &end_scn, common::ObIArray<share::ObLSID> &ls_array,
    common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t start_piece_id = 0;
  int64_t end_piece_id = 0;
  if (OB_INVALID_ID == tenant_id || dest_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_start_piece_id_(tenant_id, dest_id, start_scn, sql_client, start_piece_id))) {
    LOG_WARN("failed to get start piece id", K(ret), K(tenant_id), K(dest_id), K(start_scn));
  } else if (OB_FAIL(get_end_piece_id_(tenant_id, dest_id, end_scn, sql_client, end_piece_id))) {
    LOG_WARN("failed to get end piece id", K(ret), K(tenant_id), K(dest_id), K(end_scn));
  } else if (OB_FAIL(construct_query_archive_sql_(tenant_id, dest_id, start_piece_id, end_piece_id, sql))) {
    LOG_WARN("failed to construct query archive sql", K(ret), K(tenant_id), K(dest_id), K(start_piece_id), K(end_piece_id));
  } else if (OB_FAIL(get_distinct_ls_id_(tenant_id, sql, ls_array, sql_client))) {
    LOG_WARN("failed to get distinct ls id", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObLSBackupOperator::report_tablet_skipped(
    const uint64_t tenant_id, const ObBackupSkippedTablet &skipped_tablet, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObDMLSqlSplicer dml_splicer;
  int64_t affected_rows = 0;
  if (OB_INVALID_ID == tenant_id || !skipped_tablet.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(tenant_id), K(skipped_tablet));
  } else if (OB_FAIL(fill_backup_skipped_tablet_(skipped_tablet, dml_splicer))) {
    LOG_WARN("failed to fill backup skipped tablet", K(ret), K(skipped_tablet));
  } else if (OB_FAIL(dml_splicer.splice_insert_sql(OB_ALL_BACKUP_SKIPPED_TABLET_TNAME, sql))) {
    LOG_WARN("failed to splice update sql", K(ret), K(sql));
  } else if (OB_FAIL(sql_client.write(gen_meta_tenant_id(tenant_id), sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", K(ret), K(sql));
  } else {
    LOG_INFO("report backup tablet skipped", K(sql));
  }
  return ret;
}

int ObLSBackupOperator::fill_ls_task_info_(const ObBackupLSTaskInfo &task_info, share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(dml.add_pk_column("task_id", task_info.task_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", task_info.tenant_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("ls_id", task_info.ls_id_.id()))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("turn_id", task_info.turn_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("retry_id", task_info.retry_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("data_type", task_info.backup_data_type_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_column("backup_set_id", task_info.backup_set_id_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("input_bytes", task_info.input_bytes_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("output_bytes", task_info.output_bytes_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("tablet_count", task_info.tablet_count_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("finish_tablet_count", task_info.finish_tablet_count_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("macro_block_count", task_info.macro_block_count_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("finish_macro_block_count", task_info.finish_macro_block_count_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("max_file_id", task_info.max_file_id_))) {
    LOG_WARN("failed to add column", K(task_info));
  }
  return ret;
}

int ObLSBackupOperator::parse_ls_task_info_results_(
    sqlclient::ObMySQLResult &result, common::ObIArray<ObBackupLSTaskInfo> &task_infos)
{
  int ret = OB_SUCCESS;
  ObBackupLSTaskInfo task_info;
  while (OB_SUCC(ret)) {
    task_info.reset();
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else {
      int64_t tmp_ls_id = 0;
      EXTRACT_INT_FIELD_MYSQL(result, "task_id", task_info.task_id_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", task_info.tenant_id_, uint64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "ls_id", tmp_ls_id, int64_t);
      task_info.ls_id_ = tmp_ls_id;
      EXTRACT_INT_FIELD_MYSQL(result, "turn_id", task_info.turn_id_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "retry_id", task_info.retry_id_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "data_type", task_info.backup_data_type_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "backup_set_id", task_info.backup_set_id_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "input_bytes", task_info.input_bytes_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "output_bytes", task_info.output_bytes_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "tablet_count", task_info.tablet_count_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "finish_tablet_count", task_info.finish_tablet_count_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "macro_block_count", task_info.macro_block_count_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "finish_macro_block_count", task_info.finish_macro_block_count_, int64_t);
      EXTRACT_INT_FIELD_MYSQL(result, "max_file_id", task_info.max_file_id_, int64_t);

      int64_t tmp_real_str_len = 0;
      char final_str[common::OB_INNER_TABLE_DEFAULT_VALUE_LENTH] = "";
      EXTRACT_STRBUF_FIELD_MYSQL(result, "final", final_str, static_cast<int64_t>(sizeof(final_str)), tmp_real_str_len);
      if (OB_SUCC(ret)) {
        if (0 == strcmp(final_str, "True")) {
          task_info.is_final_ = true;
        } else {
          task_info.is_final_ = false;
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(task_infos.push_back(task_info))) {
          LOG_WARN("failed to push back task info", K(ret), K(task_info));
        }
      }
    }
  }
  return ret;
}

int ObLSBackupOperator::fill_backup_skipped_tablet_(const ObBackupSkippedTablet &task_info, share::ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int turn_id = task_info.data_type_.is_minor_backup() ? task_info.turn_id_ : share::ObBackupSkipTabletAttr::BASE_MAJOR_TURN_ID + task_info.turn_id_;
  if (OB_FAIL(dml.add_pk_column("task_id", task_info.task_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", task_info.tenant_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("turn_id", turn_id))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("retry_id", task_info.retry_id_))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_pk_column("tablet_id", task_info.tablet_id_.id()))) {
    LOG_WARN("failed to add pk column", K(task_info));
  } else if (OB_FAIL(dml.add_column("ls_id", task_info.ls_id_.id()))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("backup_set_id", task_info.backup_set_id_))) {
    LOG_WARN("failed to add column", K(task_info));
  } else if (OB_FAIL(dml.add_column("skipped_type", task_info.skipped_type_.str()))) {
    LOG_WARN("failed to add column", K(task_info));
  }
  return ret;
}

int ObLSBackupOperator::get_distinct_ls_id_(const uint64_t tenant_id, const common::ObSqlString &sql,
    common::ObIArray<share::ObLSID> &ls_array, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to exec sql", K(ret), K(sql), K(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result set from read is NULL", K(ret));
    } else {/*do nothing*/}

    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      int64_t ls_id = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "ls_id", ls_id, int64_t);
      if (OB_FAIL(ls_array.push_back(ObLSID(ls_id)))) {
        LOG_WARN("failed to push back ls id", K(ret), K(ls_id));
      }
    }

    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObLSBackupOperator::get_piece_id_(const uint64_t tenant_id, const common::ObSqlString &sql,
    int64_t &piece_id, common::ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObArray<int64_t> piece_array;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    if (OB_FAIL(sql_client.read(res, gen_meta_tenant_id(tenant_id), sql.ptr()))) {
      LOG_WARN("failed to exec sql", K(ret), K(sql), K(tenant_id));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result set from read is NULL", K(ret));
    } else {/*do nothing*/}

    while (OB_SUCC(ret) && OB_SUCC(result->next())) {
      int64_t piece_id = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "piece_id", piece_id, int64_t);
      if (OB_FAIL(piece_array.push_back(piece_id))) {
        LOG_WARN("failed to push back", K(piece_id), K(ret));
      }
    }
    if (OB_LIKELY(OB_ITER_END == ret)) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      if (piece_array.empty()) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("piece array empty", K(ret));
      } else if (piece_array.count() > 1) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("piece array count not correct", K(ret), K(piece_array));
      } else {
        piece_id = piece_array.at(0);

      }
    }
  }
  return ret;
}

// TODO(yangyi.yyy): currently, __all_ls_log_archive_progress is not cleaned,
// fix later if __all_ls_log_archive_progress is cleaned
int ObLSBackupOperator::get_start_piece_id_(const uint64_t tenant_id, const uint64_t dest_id,
    const share::SCN &start_scn, common::ObISQLClient &sql_client, int64_t &start_piece_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *sql_str = "SELECT piece_id "
                        "FROM %s "
                        "WHERE tenant_id = %lu AND dest_id = %ld "
                        "AND start_scn <= %ld ORDER BY piece_id DESC LIMIT 1";
  if (OB_INVALID_ID == tenant_id || dest_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(dest_id), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
      tenant_id, dest_id, start_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql_str), K(tenant_id), K(dest_id));
  } else if (OB_FAIL(get_piece_id_(tenant_id, sql, start_piece_id, sql_client))) {
    LOG_WARN("failed to get piece id", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObLSBackupOperator::get_end_piece_id_(const uint64_t tenant_id, const uint64_t dest_id,
    const share::SCN &checkpoint_scn, common::ObISQLClient &sql_client, int64_t &end_piece_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const char *sql_str = "SELECT piece_id "
                        "FROM %s "
                        "WHERE tenant_id = %lu AND dest_id = %ld "
                        "AND checkpoint_scn >= %ld ORDER BY piece_id ASC LIMIT 1";
  if (OB_INVALID_ID == tenant_id || dest_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(dest_id), K(tenant_id));
  } else if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
      tenant_id, dest_id, checkpoint_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("failed to append sql", K(ret), K(sql_str), K(tenant_id), K(dest_id));
  } else if (OB_FAIL(get_piece_id_(tenant_id, sql, end_piece_id, sql_client))) {
    LOG_WARN("failed to get piece id", K(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObLSBackupOperator::construct_query_backup_sql_(const uint64_t tenant_id, const int64_t task_id, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char *sql_str = "SELECT DISTINCT ls_id "
                        "FROM %s "
                        "WHERE tenant_id = %lu and task_id = %ld";
  if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_BACKUP_LS_TASK_TNAME, tenant_id, task_id))) {
    LOG_WARN("failed to append sql", K(ret), K(sql_str), K(tenant_id), K(task_id));
  }
  return ret;
}

int ObLSBackupOperator::construct_query_archive_sql_(const uint64_t tenant_id, const int64_t dest_id,
    const int64_t start_piece_id, const int64_t end_piece_id, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const char *sql_str = "SELECT DISTINCT ls_id "
                        "FROM %s "
                        "WHERE tenant_id = %lu AND dest_id = %ld "
                        "AND piece_id >= %ld AND piece_id <= %ld";
  if (OB_FAIL(sql.append_fmt(sql_str, OB_ALL_LS_LOG_ARCHIVE_PROGRESS_TNAME,
        tenant_id, dest_id, start_piece_id, end_piece_id))) {
    LOG_WARN("failed to append sql", K(ret), K(sql_str), K(tenant_id),
        K(dest_id), K(start_piece_id), K(end_piece_id));
  }
  return ret;
}

}  // namespace backup
}  // namespace oceanbase

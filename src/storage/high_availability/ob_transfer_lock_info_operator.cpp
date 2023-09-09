/**
 * Copyright (c) 2022 OceanBase
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

#include "ob_transfer_lock_info_operator.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"  // for OTTZ_MGR.get_tenant_tz
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/ob_common_id_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase {
namespace storage {

/* ObTransferLockInfoOperator */

int ObTransferLockInfoOperator::insert(const ObTransferTaskLockInfo &lock_info,
    const int32_t group_id, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = lock_info.tenant_id_;
  if (!lock_info.is_valid() || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(lock_info), K(group_id));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    if (OB_FAIL(fill_dml_splicer_(lock_info, dml_splicer))) {
      LOG_WARN("failed to fill dml splicer", K(ret), K(tenant_id), K(lock_info));
    } else if (OB_FAIL(dml_splicer.finish_row())) {
      LOG_WARN("failed to finish row", K(ret), K(tenant_id), K(lock_info));
    } else if (OB_FAIL(dml_splicer.splice_insert_sql(OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TNAME, sql))) {
      LOG_WARN("failed to splice insert sql", K(ret), K(tenant_id), K(sql));
    } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), group_id, affected_rows))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        ret = OB_ENTRY_EXIST;
      } else {
        LOG_WARN("fail to write sql", K(ret), K(tenant_id), K(sql), K(affected_rows));
      }
    } else {
#ifdef ERRSIM
    SERVER_EVENT_ADD("TRANSFER_LOCK", "INSERT_LOCK_INFO",
        "tenant_id", lock_info.tenant_id_,
        "ls_id", lock_info.ls_id_.id(),
        "task_id", lock_info.task_id_,
        "status", lock_info.status_.str(),
        "lock_owner", lock_info.lock_owner_,
        "sql", sql);
#endif
      LOG_INFO("insert transfer lock info success", K(tenant_id), K(sql), K(affected_rows));
    }
  }
  return ret;
}

int ObTransferLockInfoOperator::remove(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id,
    const ObTransferLockStatus &status, const int32_t group_id, common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid() || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id), K(group_id));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = %lu and ls_id = %ld"
                                    " and task_id = %ld and status = '%s'",
                 OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TNAME,
                 tenant_id,
                 ls_id.id(),
                 task_id,
                 status.str()))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(ls_id), K(task_id), K(status));
  } else if (OB_FAIL(sql_proxy.write(gen_meta_tenant_id(tenant_id), sql.ptr(), group_id, affected_rows))) {
    LOG_WARN("failed to write sql", K(ret), K(tenant_id), K(sql), K(affected_rows));
  } else if (OB_UNLIKELY(0 == affected_rows)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("incorrect affected rows", K(ret), K(tenant_id), K(task_id), K(affected_rows));
  } else if (OB_UNLIKELY(1 < affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delete more than one row", K(ret), K(tenant_id), K(affected_rows), K(sql));
  } else {
#ifdef ERRSIM
    SERVER_EVENT_ADD("TRANSFER_LOCK", "REMOVE_LOCK_INFO",
        "tenant_id", tenant_id,
        "ls_id", ls_id.id(),
        "task_id", task_id,
        "status", status.str(),
        "sql", sql);
#endif
    LOG_INFO("remove lock info success", K(tenant_id), K(ls_id), K(sql));
  }
  return ret;
}

int ObTransferLockInfoOperator::get(const ObTransferLockInfoRowKey &row_key, const int64_t task_id,
    const ObTransferLockStatus &status, const bool for_update, const int32_t group_id, ObTransferTaskLockInfo &lock_info,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = row_key.tenant_id_;
  if (OB_UNLIKELY(!row_key.is_valid() || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(task_id), K(for_update), K(group_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result)
    {
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu and ls_id = %ld"
                                 " and task_id = %ld and status = '%s' %s",
              OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TNAME,
              row_key.tenant_id_,
              row_key.ls_id_.id(),
              task_id,
              status.str(),
              for_update ? " FOR UPDATE" : ""))) {
        LOG_WARN("fail to assign sql", K(ret), K(tenant_id), K(row_key), K(task_id), K(status), K(for_update));
      } else if (OB_FAIL(sql_proxy.read(result, gen_meta_tenant_id(tenant_id), sql.ptr(), group_id))) {
        LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", K(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(construct_result_(*result.get_result(), lock_info))) {
        LOG_WARN("construct transfer task failed", K(ret), K(tenant_id), K(row_key), K(sql));
      } else {
        LOG_INFO("get lock info success", K(tenant_id), K(row_key), K(task_id), K(status), K(row_key), K(sql));
      }
    }
  }
  return ret;
}

int ObTransferLockInfoOperator::fetch_all(common::ObISQLClient &sql_proxy, const uint64_t tenant_id,
    const int32_t group_id, common::ObArray<ObTransferTaskLockInfo> &lock_infos)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || group_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(group_id));
  } else {
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, result)
    {
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = %lu",
              OB_ALL_LS_TRANSFER_MEMBER_LIST_LOCK_INFO_TNAME,
              tenant_id))) {
        LOG_WARN("fail to assign sql", K(ret), K(tenant_id));
      } else if (OB_FAIL(sql_proxy.read(result, gen_meta_tenant_id(tenant_id), sql.ptr(), group_id))) {
        LOG_WARN("execute sql failed", K(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", K(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(parse_sql_results_(*result.get_result(), lock_infos))) {
        LOG_WARN("construct transfer task failed", K(ret), K(tenant_id), K(sql));
      } else {
        LOG_INFO("get lock info success", K(tenant_id), K(lock_infos), K(sql));
      }
    }
  }
  return ret;
}

int ObTransferLockInfoOperator::fill_dml_splicer_(const ObTransferTaskLockInfo &lock_info, ObDMLSqlSplicer &dml_splicer)
{
  int ret = OB_SUCCESS;
  if (!lock_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(lock_info));
  } else if (OB_FAIL(dml_splicer.add_pk_column("tenant_id", lock_info.tenant_id_)) ||
             OB_FAIL(dml_splicer.add_pk_column("ls_id", lock_info.ls_id_.id())) ||
             OB_FAIL(dml_splicer.add_column("task_id", lock_info.task_id_)) ||
             OB_FAIL(dml_splicer.add_column("status", lock_info.status_.str())) ||
             OB_FAIL(dml_splicer.add_column("lock_owner", lock_info.lock_owner_)) ||
             OB_FAIL(dml_splicer.add_column("comment", lock_info.comment_.string()))) {
    LOG_WARN("fail to add column", K(ret), K(lock_info));
  }
  return ret;
}

int ObTransferLockInfoOperator::construct_result_(
    common::sqlclient::ObMySQLResult &res, ObTransferTaskLockInfo &lock_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(res.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("get next result failed", K(ret));
    }
  } else if (OB_FAIL(parse_sql_result_(res, lock_info))) {
    LOG_WARN("parse sql result failed", K(ret));
  } else if (OB_FAIL(res.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("get next result failed", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("read more than one row", K(ret));
  }
  return ret;
}

int ObTransferLockInfoOperator::parse_sql_result_(
    common::sqlclient::ObMySQLResult &res, ObTransferTaskLockInfo &lock_info)
{
  int ret = OB_SUCCESS;
  lock_info.reset();
  int64_t create_time = OB_INVALID_TIMESTAMP;
  int64_t finish_time = OB_INVALID_TIMESTAMP;
  int64_t tenant_id = 0;
  int64_t ls_id;
  int64_t task_id = 0;
  ObString status_str;
  int64_t lock_owner = 0;
  ObString comment;
  ObTransferLockStatus status;
  {
    ObTimeZoneInfoWrap tz_info_wrap;
    ObTZMapWrap tz_map_wrap;
    OZ(OTTZ_MGR.get_tenant_tz(tenant_id, tz_map_wrap));
    tz_info_wrap.set_tz_info_map(tz_map_wrap.get_tz_map());
    (void)GET_COL_IGNORE_NULL(res.get_timestamp, "gmt_create", tz_info_wrap.get_time_zone_info(), create_time);
    (void)GET_COL_IGNORE_NULL(res.get_timestamp, "gmt_modified", tz_info_wrap.get_time_zone_info(), finish_time);
  }
  (void)GET_COL_IGNORE_NULL(res.get_int, "tenant_id", tenant_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "ls_id", ls_id);
  (void)GET_COL_IGNORE_NULL(res.get_int, "task_id", task_id);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "status", status_str);
  (void)GET_COL_IGNORE_NULL(res.get_int, "lock_owner", lock_owner);
  (void)GET_COL_IGNORE_NULL(res.get_varchar, "comment", comment);

  if (OB_FAIL(status.parse_from_str(status_str))) {
    LOG_WARN("failed to parse from str", K(ret), K(status_str));
  } else if (FAILEDx(lock_info.set(tenant_id, ObLSID(ls_id), task_id, status, lock_owner, comment))) {
    LOG_WARN("failed to set lock info", K(ret), K(ls_id), K(task_id), K(status), K(lock_owner), K(comment));
  }
  return ret;
}

int ObTransferLockInfoOperator::parse_sql_results_(
    common::sqlclient::ObMySQLResult &result, common::ObArray<ObTransferTaskLockInfo> &lock_infos)
{
  int ret = OB_SUCCESS;
  lock_infos.reset();
  while (OB_SUCC(ret)) {
    ObTransferTaskLockInfo lock_info;
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get next row", K(ret));
      }
    } else if (OB_FAIL(parse_sql_result_(result, lock_info))) {
      LOG_WARN("failed to parse sql result", K(ret));
    } else if (OB_FAIL(lock_infos.push_back(lock_info))) {
      LOG_WARN("failed to push back lock info", K(ret));
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

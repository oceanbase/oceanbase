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

#define USING_LOG_PREFIX SHARE
#include "ob_snapshot_table_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_cluster_version.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_utils.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
using namespace lib;

namespace share {
ObSnapshotInfo::ObSnapshotInfo()
{
  reset();
}

void ObSnapshotInfo::reset()
{
  snapshot_type_ = share::MAX_SNAPSHOT_TYPE;
  snapshot_ts_ = 0;
  schema_version_ = 0;
  tenant_id_ = OB_INVALID_ID;
  table_id_ = OB_INVALID_ID;
  comment_ = NULL;
}

bool ObSnapshotInfo::is_valid() const
{
  bool bret = true;
  if (snapshot_type_ < share::SNAPSHOT_FOR_MAJOR || snapshot_type_ > MAX_SNAPSHOT_TYPE || snapshot_ts_ <= 0) {
    bret = false;
    LOG_WARN("invalid snapshot", K(bret), K(*this));
  }
  return bret;
}

////////////////////////////////////////////////
void TenantSnapshot::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  snapshot_ts_ = 0;
}

bool TenantSnapshot::is_valid() const
{
  return tenant_id_ >= 0 && snapshot_ts_ >= 0;
}
////////////////////////////////////////////////

int ObSnapshotTableProxy::gen_event_ts(int64_t& event_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObMutexGuard guard(lock_);
  event_ts = (last_event_ts_ >= now ? last_event_ts_ + 1 : now);
  last_event_ts_ = event_ts;
  return ret;
}
int ObSnapshotTableProxy::add_snapshot(
    common::ObMySQLTransaction& trans, const ObSnapshotInfo& info, const bool& need_lock_gc_snapshot)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t event_ts = 0;
  bool is_valid = false;
  if (OB_FAIL(check_snapshot_valid(trans, info, need_lock_gc_snapshot, is_valid))) {
    LOG_WARN("fail to check snapshot valid", K(ret), K(info));
  } else if (!is_valid) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("invalid snapshot info", K(ret), K(info));
  } else if (OB_FAIL(gen_event_ts(event_ts))) {
    LOG_WARN("fail to gen event ts", K(ret));
  } else if (OB_FAIL(dml.add_gmt_create(event_ts)) || OB_FAIL(dml.add_column("snapshot_type", info.snapshot_type_)) ||
             OB_FAIL(dml.add_column("snapshot_ts", info.snapshot_ts_)) ||
             OB_FAIL(dml.add_column("schema_version", info.schema_version_)) ||
             OB_FAIL(dml.add_column("tenant_id", info.tenant_id_)) ||
             OB_FAIL(dml.add_column("table_id", info.table_id_)) ||
             OB_FAIL(dml.add_column("extra_info", info.comment_))) {

    LOG_WARN("fail to add column", K(ret), K(info));
  } else {
    ObDMLExecHelper exec(trans, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_insert(OB_ALL_ACQUIRED_SNAPSHOT_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec insert", K(ret));
    } else if (1 != affected_rows) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("insert get invalid affected_rows", K(ret), K(affected_rows));
    }
  }
  return ret;
}

// only used by standby cluster
int ObSnapshotTableProxy::insert_or_update_snapshot(common::ObISQLClient& proxy, const TenantSnapshot& info)
{
  int ret = OB_SUCCESS;
  bool is_valid = false;
  ObSnapshotInfo old_snapshot;
  bool need_lock = false;
  ObMySQLTransaction trans;
  ObSqlString sql;
  ObSnapshotInfo tmp_snapshot;
  tmp_snapshot.tenant_id_ = info.tenant_id_;
  tmp_snapshot.snapshot_ts_ = info.snapshot_ts_;
  tmp_snapshot.snapshot_type_ = SNAPSHOT_FOR_MULTI_VERSION;
  tmp_snapshot.table_id_ = 0;
  int64_t affected_rows = 0;
  if (OB_FAIL(check_snapshot_valid(proxy, tmp_snapshot, need_lock, is_valid))) {
    LOG_WARN("fail to check snapshot valid", K(ret), K(info));
  } else if (!is_valid) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("invalid snapshot info", K(ret), K(info));
  } else if (OB_FAIL(trans.start(&proxy))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else if (OB_FAIL(get_snapshot(trans, info.tenant_id_, SNAPSHOT_FOR_MULTI_VERSION, old_snapshot))) {
    if (OB_ITER_END == ret) {
      const bool need_lock_gc_snapshot = false;
      if (OB_FAIL(add_snapshot(trans, tmp_snapshot, need_lock_gc_snapshot))) {
        LOG_WARN("fail to add snapshot", KR(ret), K(info));
      }
    } else {
      LOG_WARN("fail to get snapshot", KR(ret), K(info));
    }
  } else if (old_snapshot.snapshot_ts_ > info.snapshot_ts_) {
    LOG_WARN("snapshot ts should not fallback", K(old_snapshot), K(info));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET snapshot_ts = (if (%ld > snapshot_ts, %ld, snapshot_ts))"
                                    " WHERE tenant_id = %ld and snapshot_type = %d",
                 OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
                 info.snapshot_ts_,
                 info.snapshot_ts_,
                 info.tenant_id_,
                 SNAPSHOT_FOR_MULTI_VERSION))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(info));
  } else if (OB_FAIL(trans.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql));
  } else if (1 != affected_rows && 0 != affected_rows) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("update get invalid affected_rows, should be 1", KR(ret), K(affected_rows));
  }
  if (trans.is_started()) {
    bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to commit transaction", K(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::remove_snapshot(common::ObISQLClient& proxy, const ObSnapshotInfo& info)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  if (MAX_SNAPSHOT_TYPE <= info.snapshot_type_ || SNAPSHOT_FOR_MAJOR > info.snapshot_type_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (OB_FAIL(dml.add_pk_column("snapshot_type", info.snapshot_type_)) ||
             (info.snapshot_ts_ >= 0 && OB_FAIL(dml.add_pk_column("snapshot_ts", info.snapshot_ts_))) ||
             (info.schema_version_ > 0 && OB_FAIL(dml.add_pk_column("schema_version", info.schema_version_))) ||
             (info.tenant_id_ > 0 && OB_FAIL(dml.add_pk_column("tenant_id", info.tenant_id_))) ||
             (info.table_id_ > 0 && OB_FAIL(dml.add_pk_column("table_id", info.table_id_)))) {
    LOG_WARN("fail to add column", K(ret), K(info));
  } else {
    ObDMLExecHelper exec(proxy, OB_SYS_TENANT_ID);
    int64_t affected_rows = 0;
    if (OB_FAIL(exec.exec_delete(OB_ALL_ACQUIRED_SNAPSHOT_TNAME, dml, affected_rows))) {
      LOG_WARN("fail to exec insert", K(ret));
    }
  }
  return ret;
}

static inline int extract_snapshot(const ObMySQLResult& result, ObSnapshotInfo& snapshot)
{
  int ret = OB_SUCCESS;

  EXTRACT_INT_FIELD_MYSQL(result, "snapshot_type", snapshot.snapshot_type_, ObSnapShotType);
  EXTRACT_INT_FIELD_MYSQL(result, "snapshot_ts", snapshot.snapshot_ts_, int64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "tenant_id", snapshot.tenant_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "table_id", snapshot.table_id_, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "schema_version", snapshot.schema_version_, int64_t);
  snapshot.comment_ = NULL;

  return ret;
}

int ObSnapshotTableProxy::get_all_snapshots(common::ObISQLClient& proxy, ObIArray<ObSnapshotInfo>& snapshots)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;

    if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s", OB_ALL_ACQUIRED_SNAPSHOT_TNAME))) {
      LOG_WARN("fail to assign fmt", K(ret));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      ObSnapshotInfo snapshot;
      while (OB_SUCC(ret)) {
        snapshot.reset();

        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("result next failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(extract_snapshot(*result, snapshot))) {
          LOG_WARN("fail to extract snapshot", K(ret));
        } else if (OB_FAIL(snapshots.push_back(snapshot))) {
          LOG_WARN("push back failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::check_snapshot_valid(
    common::ObISQLClient& client, const ObSnapshotInfo& info, const bool& need_lock_gc_snapshot, bool& is_valid)
{
  int ret = OB_SUCCESS;
  int64_t gc_timestamp = 0;
  is_valid = false;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else if (need_lock_gc_snapshot) {
    if (GET_MIN_CLUSTER_VERSION() <= CLUSTER_VERSION_2000 &&
        OB_FAIL(ObZoneTableOperation::select_gc_timestamp_for_update(client, gc_timestamp))) {
      LOG_WARN("fail to select gc timstamp for update", K(ret));
    } else if (GET_MIN_CLUSTER_VERSION() > CLUSTER_VERSION_2000 &&
               OB_FAIL(ObGlobalStatProxy::select_gc_timestamp_for_update(client, gc_timestamp))) {
      LOG_WARN("fail to select gc timstamp for update", K(ret));
    } else if (info.snapshot_ts_ <= gc_timestamp) {
      is_valid = false;
      LOG_WARN("invalid snapshot info", K(ret), K(info), K(gc_timestamp));
    } else {
      is_valid = true;
    }
  } else {
    is_valid = true;
  }
  return ret;
}

int ObSnapshotTableProxy::get_snapshot(common::ObISQLClient& proxy, const int64_t tenant_id,
    share::ObSnapShotType snapshot_type, ObSnapshotInfo& snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_INVALID_ID == tenant_id || SNAPSHOT_FOR_MULTI_VERSION != snapshot_type) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
    } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE snapshot_type = %d AND tenant_id = %ld",
                   OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
                   snapshot_type,
                   tenant_id))) {
      LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(snapshot_type));
    } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        LOG_WARN("tenant snapshot not exist", KR(ret), K(tenant_id), K(snapshot_type));
      } else {
        LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
      }
    } else if (OB_FAIL(extract_snapshot(*result, snapshot_info))) {
      LOG_WARN("fail to extract snapshot", KR(ret));
    } else if (OB_ITER_END != result->next()) {
      if (OB_SUCC(ret)) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("get invalid next result", KR(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObSnapshotTableProxy::get_max_snapshot_info(common::ObISQLClient& proxy, ObSnapshotInfo& snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy::MySQLResult res;
  sqlclient::ObMySQLResult* result = NULL;
  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s ORDER BY SNAPSHOT_TS DESC LIMIT 1", OB_ALL_ACQUIRED_SNAPSHOT_TNAME))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
    LOG_WARN("fail to read", KR(ret), K(sql));
  } else if (OB_ISNULL(result = res.get_result())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get result failed", KR(ret));
  } else if (OB_FAIL(result->next())) {
    if (OB_ITER_END == ret) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("nothing exist in table", KR(ret));
    } else {
      LOG_WARN("fail to get next", KR(ret));
    }
  } else if (OB_FAIL(extract_snapshot(*result, snapshot_info))) {
    LOG_WARN("fail to extract snapshot", KR(ret));
  } else if (OB_ITER_END != result->next()) {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("get invalid next result", KR(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObSnapshotTableProxy::get_snapshot(common::ObISQLClient& proxy, const int64_t tenant_id,
    share::ObSnapShotType snapshot_type, const char* extra_info, ObSnapshotInfo& snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (tenant_id <= 0 || snapshot_type < SNAPSHOT_FOR_MAJOR || snapshot_type >= MAX_SNAPSHOT_TYPE ||
      NULL == extra_info) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(snapshot_type), KP(extra_info));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
      } else if (OB_INVALID_ID == tenant_id || SNAPSHOT_FOR_RESTORE_POINT != snapshot_type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(sql.assign_fmt(
                     "SELECT * FROM %s WHERE snapshot_type = %d AND tenant_id = %ld AND extra_info = '%s'",
                     OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
                     snapshot_type,
                     tenant_id,
                     extra_info))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
        } else {
          LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
        }
      } else if (OB_FAIL(extract_snapshot(*result, snapshot_info))) {
        LOG_WARN("fail to extract snapshot", KR(ret));
      } else if (!snapshot_info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("snapshot info is invalid", K(ret), K(snapshot_info));
      } else if (OB_ITER_END != result->next()) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("get invalid next result", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::get_snapshot(common::ObISQLClient& proxy, const int64_t tenant_id,
    const share::ObSnapShotType snapshot_type, const int64_t snapshot_ts, ObSnapshotInfo& snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (tenant_id <= 0 || snapshot_type < SNAPSHOT_FOR_MAJOR || snapshot_type >= MAX_SNAPSHOT_TYPE || snapshot_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(snapshot_type), K(snapshot_ts));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
      } else if (OB_INVALID_ID == tenant_id) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(sql.assign_fmt(
                     "SELECT * FROM %s WHERE snapshot_type = %d AND tenant_id = %ld AND snapshot_ts = %ld",
                     OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
                     snapshot_type,
                     tenant_id,
                     snapshot_ts))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
        } else {
          LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
        }
      } else if (OB_FAIL(extract_snapshot(*result, snapshot_info))) {
        LOG_WARN("fail to extract snapshot", KR(ret));
      } else if (OB_ITER_END != result->next()) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("get invalid next result", KR(ret));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::check_snapshot_exist(common::ObISQLClient& proxy, const int64_t tenant_id,
    const int64_t table_id, share::ObSnapShotType snapshot_type, bool& is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t latest_restore_point_value = 0;
  sqlclient::ObMySQLResult* result = NULL;
  is_exist = false;

  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || OB_INVALID_ID == table_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type), K(table_id));
  } else {
    {SMART_VAR(ObMySQLProxy::MySQLResult, res){
        if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) FROM %s WHERE snapshot_type = %d AND tenant_id = "
                                   "%ld ORDER BY gmt_create DESC LIMIT 1",
                OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
                snapshot_type,
                tenant_id))){LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(snapshot_type));
  }
  else if (OB_FAIL(proxy.read(res, sql.ptr())))
  {
    LOG_WARN("fail to read", KR(ret), K(sql));
  }
  else if (OB_ISNULL(result = res.get_result()))
  {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get result failed", KR(ret));
  }
  else if (OB_FAIL(result->next()))
  {
    if (OB_ITER_END == ret) {
      is_exist = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
    }
  }
  else if (OB_FAIL(result->get_int(0L, latest_restore_point_value)))
  {
    LOG_WARN("fail to get lastest restore point create time", KR(ret), K(tenant_id), K(latest_restore_point_value));
  }
  else
  {
    is_exist = true;
  }
}
}  // namespace share
{
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    const char* table_name = NULL;
    const uint64_t exec_tenant_id = schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObDMLSqlSplicer dml;
    if (OB_FAIL(ret) || !is_exist) {
      // do nothing
    } else if (OB_FAIL(schema::ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
      LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
    } else if (OB_FAIL(sql.assign_fmt("SELECT table_id FROM %s WHERE tenant_id = %ld AND table_id = %ld AND gmt_create "
                                      "<= usec_to_time(%ld) limit 1",
                   table_name,
                   schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   schema::ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                   latest_restore_point_value))) {
      LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(snapshot_type));
    } else if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", KR(ret));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        is_exist = false;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
      }
    } else {
      is_exist = true;
    }
  }
}
}  // namespace oceanbase
return ret;
}

/*
 * @description:
 * check snapshot exist
 *  @param[in] sql_proxy
 *  @param[in] snapshot_type
 *  @param[out] is_exist
 * */
int ObSnapshotTableProxy::check_snapshot_exist(
    common::ObISQLClient& proxy, const share::ObSnapShotType snapshot_type, bool& is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult* result = NULL;
  is_exist = false;

  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      if (OB_FAIL(sql.assign_fmt(
              "SELECT * FROM %s WHERE snapshot_type = %d LIMIT 1", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, snapshot_type))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          is_exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next", KR(ret), K(snapshot_type));
        }
      } else {
        is_exist = true;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::get_snapshot_count(
    common::ObISQLClient& proxy, const int64_t tenant_id, share::ObSnapShotType snapshot_type, int64_t& count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || snapshot_type < SNAPSHOT_FOR_MAJOR || snapshot_type >= MAX_SNAPSHOT_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(snapshot_type));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res)
    {
      sqlclient::ObMySQLResult* result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
      } else if (OB_INVALID_ID == tenant_id || SNAPSHOT_FOR_RESTORE_POINT != snapshot_type) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(sql.assign_fmt("SELECT count(*) as cnt FROM %s WHERE snapshot_type = %d AND tenant_id = %ld",
                     OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
                     snapshot_type,
                     tenant_id))) {
        LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get result failed", KR(ret));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
      }
    }
  }
  return ret;
}
}
}

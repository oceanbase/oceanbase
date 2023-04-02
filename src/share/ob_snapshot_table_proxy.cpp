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

#include "share/ob_snapshot_table_proxy.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_zone_table_operation.h"
#include "share/ob_global_stat_proxy.h"
#include "share/ob_cluster_version.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"
#include "share/schema/ob_schema_utils.h"
#include "share/ob_dml_sql_splicer.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::lib;

const char *ObSnapshotInfo::ObSnapShotTypeStr[] = {
    "SNAPSHOT_FOR_MAJOR",
    "SNAPSHOT_FOR_CREATE_INDEX",
    "SNAPSHOT_FOR_MULTI_VERSION",
    "SNAPSHOT_FOR_RESTORE_POINT",
    "SNAPSHOT_FOR_BACKUP_POINT", };

ObSnapshotInfo::ObSnapshotInfo()
{
  reset();
  STATIC_ASSERT(ObSnapShotType::MAX_SNAPSHOT_TYPE == ARRAYSIZEOF(ObSnapShotTypeStr), "snapshot type len is mismatch");
}

int ObSnapshotInfo::init(
    const uint64_t tenant_id,
    const uint64_t tablet_id,
    const ObSnapShotType &snapshot_type,
    const SCN &snapshot_scn,
    const int64_t schema_version,
    const char* comment)
{
  int ret = OB_SUCCESS;
  snapshot_type_ = snapshot_type;
  schema_version_ = schema_version;
  tenant_id_ = tenant_id;
  tablet_id_ = tablet_id;
  comment_ = comment;
  snapshot_scn_ = snapshot_scn;
  return ret;
}

void ObSnapshotInfo::reset()
{
  snapshot_type_ = share::MAX_SNAPSHOT_TYPE;
  snapshot_scn_.set_min();
  schema_version_ = 0;
  tenant_id_ = OB_INVALID_ID;
  tablet_id_ = OB_INVALID_ID;
  comment_ = NULL;
}

bool ObSnapshotInfo::is_valid() const
{
  bool bret = true;
  if (snapshot_type_ < share::SNAPSHOT_FOR_MAJOR
      || snapshot_type_ > MAX_SNAPSHOT_TYPE
      || !snapshot_scn_.is_valid()) {
    bret = false;
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid snapshot", K(bret), K(*this));
  }
  return bret;
}

const char * ObSnapshotInfo::get_snapshot_type_str() const
{
  const char * str = nullptr;
  if (OB_UNLIKELY(snapshot_type_ < SNAPSHOT_FOR_MAJOR || snapshot_type_ >= MAX_SNAPSHOT_TYPE)) {
    str = "invalid_snapshot_type";
  } else {
    str = ObSnapShotTypeStr[snapshot_type_];
  }
  return str;
}

////////////////////////////////////////////////
void TenantSnapshot::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  snapshot_scn_.set_min();
}

bool TenantSnapshot::is_valid() const
{
  return (tenant_id_ != OB_INVALID_TENANT_ID)
         && (snapshot_scn_.is_valid());
}

////////////////////////////////////////////////
int ObSnapshotTableProxy::gen_event_ts(int64_t &event_ts)
{
  int ret = OB_SUCCESS;
  const int64_t now = ObTimeUtility::current_time();
  ObMutexGuard guard(lock_);
  event_ts = (last_event_ts_ >= now ? last_event_ts_ + 1 : now);
  last_event_ts_ = event_ts;
  return ret;
}

int ObSnapshotTableProxy::fill_snapshot_item(
    const ObSnapshotInfo &info,
    ObDMLSqlSplicer &dml)
{
  int ret = OB_SUCCESS;
  int64_t event_ts = 0;
  const uint64_t snapshot_scn_val = info.snapshot_scn_.get_val_for_inner_table_field();

  if (!is_valid_tenant_id(info.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(info));
  } else if (OB_FAIL(gen_event_ts(event_ts))) {
    LOG_WARN("fail to gen event ts", KR(ret), K(info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", OB_INVALID_TENANT_ID))
             || OB_FAIL(dml.add_gmt_create(event_ts))
             || OB_FAIL(dml.add_column("snapshot_type", info.snapshot_type_))
             || OB_FAIL(dml.add_uint64_column("snapshot_scn", snapshot_scn_val))
             || OB_FAIL(dml.add_column("schema_version", info.schema_version_))
             || OB_FAIL(dml.add_column("tablet_id", info.tablet_id_))
             || OB_FAIL(dml.add_column("extra_info", info.comment_))) {
    LOG_WARN("fail to add column", KR(ret), K(info));
  }
  return ret;
}

int ObSnapshotTableProxy::add_snapshot(
    ObMySQLTransaction &trans,
    const ObSnapshotInfo &snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!snapshot.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(snapshot));
  } else {
    ObArray<ObTabletID> tablet_id_array;
    if (OB_FAIL(tablet_id_array.push_back(ObTabletID(snapshot.tablet_id_)))) {
      LOG_WARN("push back tablet id failed", K(ret));
    } else if (OB_FAIL(batch_add_snapshot(trans,
        snapshot.snapshot_type_,
        snapshot.tenant_id_,
        snapshot.schema_version_,
        snapshot.snapshot_scn_,
        snapshot.comment_,
        tablet_id_array))) {
      LOG_WARN("batch add snapshot failed", K(ret), K(snapshot));
    }
  }
  return ret;
}

int ObSnapshotTableProxy::batch_add_snapshot(
    ObMySQLTransaction &trans,
    const share::ObSnapShotType snapshot_type,
    const uint64_t tenant_id,
    const int64_t schema_version,
    const SCN &snapshot_scn,
    const char *comment,
    const common::ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObSqlString columns;
  ObSqlString values;
  ObDMLSqlSplicer dml;
  const int64_t BATCH_CNT = 500;
  if (OB_UNLIKELY(OB_INVALID_ID == tenant_id || schema_version < 0
      || !snapshot_scn.is_valid() || tablet_id_array.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tenant_id), K(schema_version), K(snapshot_scn), K(tablet_id_array));
  } else {
    SCN snapshot_gc_scn = SCN::min_scn();
    int64_t report_idx = 0;
    const int64_t exec_tenant_id = ObSchemaUtils::get_exec_tenant_id(tenant_id);
    ObSnapshotInfo info;
    bool is_valid = false;
    info.snapshot_type_ = snapshot_type;
    info.tenant_id_ = tenant_id;
    info.snapshot_scn_ = snapshot_scn;
    info.schema_version_ = schema_version;
    info.comment_ = comment;
    if (OB_FAIL(ObGlobalStatProxy::select_snapshot_gc_scn_for_update(trans, tenant_id, snapshot_gc_scn))) {
      LOG_WARN("fail to select gc timstamp for update", KR(ret), K(info), K(tenant_id));
    }
    while (OB_SUCC(ret) && report_idx < tablet_id_array.count()) {
      sql.reuse();
      columns.reuse();
      const int64_t remain_cnt = tablet_id_array.count() - report_idx;
      int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        info.tablet_id_ = tablet_id_array.at(report_idx + i).id();
        dml.reuse();
        if (OB_FAIL(check_snapshot_valid(snapshot_gc_scn, info, is_valid))) {
          LOG_WARN("fail to check snapshot valid", KR(ret), K(info), K(tenant_id));
        } else if (!is_valid) {
          ret = OB_SNAPSHOT_DISCARDED;
          LOG_WARN("invalid snapshot info", KR(ret), K(info));
        } else if (OB_FAIL(fill_snapshot_item(info, dml))) {
          LOG_WARN("fail to fill one item", K(ret), K(info));
        } else {
          if (0 == i) {
            if (OB_FAIL(dml.splice_column_names(columns))) {
              LOG_WARN("fail to splice column names", K(ret));
            } else if (OB_FAIL(sql.assign_fmt("INSERT /*+ use_plan_cache(none) */ INTO %s (%s) VALUES",
                    OB_ALL_ACQUIRED_SNAPSHOT_TNAME, columns.ptr()))) {
              LOG_WARN("fail to assign sql string", K(ret));
            }
          }

          if (OB_SUCC(ret)) {
            values.reset();
            if (OB_FAIL(dml.splice_values(values))) {
              LOG_WARN("fail to splice values", K(ret));
            } else if (OB_FAIL(sql.append_fmt("%s(%s)",
                    0 == i ? " " : " , ", values.ptr()))) {
              LOG_WARN("fail to assign sql string", K(ret));
            }
          }
        }
      }

      if (OB_SUCC(ret)) {
        int64_t affected_rows = 0;
        if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
          LOG_WARN("fail to execute sql", K(ret));
        } else if (OB_UNLIKELY(affected_rows != cur_batch_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid affected rows", K(ret), K(affected_rows), K(cur_batch_cnt));
        } else {
          report_idx += cur_batch_cnt;
          LOG_INFO("batch acquire snapshots", K(sql));
        }
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::remove_snapshot(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObSnapshotInfo &info)
{
  int ret = OB_SUCCESS;
  const uint64_t ext_tenant_id = schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObDMLExecHelper exec(proxy, tenant_id);

  if ((MAX_SNAPSHOT_TYPE <= info.snapshot_type_) ||
      (SNAPSHOT_FOR_MAJOR > info.snapshot_type_) ||
      (!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(info), K(tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", ext_tenant_id))
             || OB_FAIL(dml.add_pk_column("snapshot_type", info.snapshot_type_))
             || (info.snapshot_scn_.is_valid() &&
                 OB_FAIL(dml.add_uint64_pk_column("snapshot_scn", info.snapshot_scn_.get_val_for_inner_table_field())))
             || (info.schema_version_ > 0 && OB_FAIL(dml.add_pk_column("schema_version", info.schema_version_)))
             || (info.tablet_id_ > 0 && OB_FAIL(dml.add_pk_column("tablet_id", info.tablet_id_)))) {
    LOG_WARN("fail to add column", KR(ret), K(info));
  } else if (OB_FAIL(exec.exec_delete(OB_ALL_ACQUIRED_SNAPSHOT_TNAME, dml, affected_rows))) {
    LOG_WARN("fail to exec delete", KR(ret), K(tenant_id), K(info));
  }
  return ret;
}

int ObSnapshotTableProxy::remove_snapshot(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObSnapShotType snapshot_type)
{
  int ret = OB_SUCCESS;
  ObSnapshotInfo info;
  info.tenant_id_ = tenant_id;
  info.snapshot_type_ = snapshot_type;
  return remove_snapshot(proxy, tenant_id, info);
}

int ObSnapshotTableProxy::batch_remove_snapshots(
    common::ObISQLClient &proxy,
    share::ObSnapShotType snapshot_type,
    const uint64_t tenant_id,
    const int64_t schema_version,
    const SCN &snapshot_scn,
    const common::ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  const uint64_t ext_tenant_id = schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id);
  const int64_t BATCH_CNT = 256;

  if ((MAX_SNAPSHOT_TYPE <= snapshot_type) ||
      (SNAPSHOT_FOR_MAJOR > snapshot_type) ||
      (!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_type), K(tenant_id));
  } else {
    int64_t report_idx = 0;
    while (OB_SUCC(ret) && report_idx < tablet_ids.count()) {
      int64_t affected_rows = 0;
      ObSqlString sql;
      ObSqlString tablet_list;
      const int64_t remain_cnt = tablet_ids.count() - report_idx;
      const int64_t cur_batch_cnt = remain_cnt < BATCH_CNT ? remain_cnt : BATCH_CNT;
      for (int64_t i = 0; OB_SUCC(ret) && i < cur_batch_cnt; ++i) {
        const uint64_t &tablet_id = tablet_ids.at(report_idx + i).id();
        if (OB_FAIL(tablet_list.append_fmt("%s %lu", i == 0 ? "" : ",", tablet_id))) {
          LOG_WARN("fail to add column", K(ret), K(tablet_id));
        }
      }
      if (FAILEDx(sql.append_fmt(
          "DELETE /*+ use_plan_cache(none) */ FROM %s WHERE tenant_id = %lu AND snapshot_type = %i AND tablet_id IN (%.*s)",
          OB_ALL_ACQUIRED_SNAPSHOT_TNAME,
          ext_tenant_id, snapshot_type, tablet_list.string().length(), tablet_list.string().ptr()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (snapshot_scn.is_valid() && OB_FAIL(sql.append_fmt(" AND snapshot_scn = %lu",
          snapshot_scn.get_val_for_inner_table_field()))) {
        LOG_WARN("fail to append snapshot version", KR(ret), K(sql), K(snapshot_scn));
      } else if (schema_version > 0 && OB_FAIL(sql.append_fmt(
        " AND schema_version = %ld", schema_version))) {
        LOG_WARN("fail to append schema version", KR(ret), K(sql), K(schema_version));
      } else if (OB_FAIL(proxy.write(tenant_id, sql.ptr(), affected_rows))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql));
      } else if (OB_UNLIKELY(affected_rows < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows is unexpected", KR(ret), K(affected_rows));
      } else {
        report_idx += cur_batch_cnt;
      }
    }
  }

  return ret;
}

static inline
int extract_snapshot(const ObMySQLResult &result, ObSnapshotInfo &snapshot, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  SCN snapshot_scn;
  uint64_t snapshot_scn_val = share::OB_INVALID_SCN_VAL;
  ObSnapShotType snapshot_type = ObSnapShotType::MAX_SNAPSHOT_TYPE;
  uint64_t tablet_id = UINT64_MAX;
  int64_t schema_version = -1;

  EXTRACT_INT_FIELD_MYSQL(result, "snapshot_type", snapshot_type, ObSnapShotType);
  EXTRACT_UINT_FIELD_MYSQL(result, "snapshot_scn", snapshot_scn_val, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "tablet_id", tablet_id, uint64_t);
  EXTRACT_INT_FIELD_MYSQL(result, "schema_version", schema_version, int64_t);

  if (FAILEDx(snapshot_scn.convert_for_inner_table_field(snapshot_scn_val))) {
    LOG_WARN("fail to convert_for_inner_table_field", KR(ret), K(tablet_id), K(snapshot_scn_val));
  } else if (OB_FAIL(snapshot.init(tenant_id, tablet_id, snapshot_type, snapshot_scn, schema_version, NULL/*comment*/))) {
    LOG_WARN("fail to init snapshot info", KR(ret), K(tablet_id), K(snapshot_scn));
  }

  return ret;
}

int ObSnapshotTableProxy::get_all_snapshots(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObIArray<ObSnapshotInfo> &snapshots)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObMySQLResult *result = NULL;

      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu'", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, 
          schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(sql), K(tenant_id));
      } else if (NULL == (result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql), K(tenant_id));
      } else {
        ObSnapshotInfo snapshot;
        while (OB_SUCC(ret)) {
          snapshot.reset();

          if (OB_FAIL(result->next())) {
            if (OB_ITER_END != ret) {
              LOG_WARN("fail to get next result", KR(ret), K(sql), K(tenant_id));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(extract_snapshot(*result, snapshot, tenant_id))) {
            LOG_WARN("fail to extract snapshot", KR(ret));
          } else if (OB_FAIL(snapshots.push_back(snapshot))) {
            LOG_WARN("fail to push back snapshot info", KR(ret), K(tenant_id));
          }
        }
        FLOG_INFO("get all snapshots", K(ret), K(snapshots.count()), K(snapshots));
      }
    }
  }

  return ret;
}

int ObSnapshotTableProxy::check_snapshot_valid(
    const SCN &snapshot_gc_scn,
    const ObSnapshotInfo &info,
    bool &is_valid) const
{
  int ret = OB_SUCCESS;
  is_valid = false;
  if (!info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(info));
  } else if (info.snapshot_scn_ <= snapshot_gc_scn) {
    is_valid = false;
    LOG_WARN("invalid snapshot info", KR(ret), K(info), K(snapshot_gc_scn));
  } else {
    is_valid = true;
  }
  return ret;
}

int ObSnapshotTableProxy::get_snapshot(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObSnapShotType snapshot_type,
    ObSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (!is_valid_tenant_id(tenant_id) || 
      (SNAPSHOT_FOR_MULTI_VERSION != snapshot_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE snapshot_type = %d AND tenant_id = '%lu'",
          OB_ALL_ACQUIRED_SNAPSHOT_TNAME, snapshot_type, 
          schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          LOG_WARN("tenant snapshot not exist", KR(ret), K(tenant_id), K(snapshot_type));
        } else {
          LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
        }
      } else if (OB_FAIL(extract_snapshot(*result, snapshot_info, tenant_id))) {
        LOG_WARN("fail to extract snapshot", KR(ret), K(tenant_id));
      } else if (OB_ITER_END != result->next()) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("get invalid next result", KR(ret), K(tenant_id), K(snapshot_type));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::get_max_snapshot_info(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      ObSqlString sql;
      sqlclient::ObMySQLResult *result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
      } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE tenant_id = '%lu' "
          "ORDER BY SNAPSHOT_SCN DESC LIMIT 1", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, 
          schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("nothing exist in table", KR(ret), K(sql));
        } else {
          LOG_WARN("fail to get next", KR(ret), K(sql));
        }
      } else if (OB_FAIL(extract_snapshot(*result, snapshot_info, tenant_id))) {
        LOG_WARN("fail to extract snapshot", KR(ret), K(tenant_id));
      } else if (OB_ITER_END != result->next()) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("get invalid next result", KR(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::get_snapshot(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObSnapShotType snapshot_type,
    const char *extra_info,
    ObSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if ((!is_valid_tenant_id(tenant_id)) || (SNAPSHOT_FOR_RESTORE_POINT != snapshot_type) ||
      (NULL == extra_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(ctx));
      } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE snapshot_type = %d AND tenant_id = '%lu' "
          "AND extra_info = '%s'", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, snapshot_type, 
          schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id), extra_info))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
        } else {
          LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
        }
      } else if (OB_FAIL(extract_snapshot(*result, snapshot_info, tenant_id))) {
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

int ObSnapshotTableProxy::get_snapshot(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObSnapShotType snapshot_type,
    const SCN &snapshot_scn,
    ObSnapshotInfo &snapshot_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if ((!is_valid_tenant_id(tenant_id)) || (snapshot_type < SNAPSHOT_FOR_MAJOR) ||
      (snapshot_type >= MAX_SNAPSHOT_TYPE) || (!snapshot_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type), K(snapshot_scn));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(tenant_id), K(ctx));
      } else if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE snapshot_type = %d AND snapshot_scn = %lu "
          "AND tenant_id = '%lu'", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, snapshot_type,
          snapshot_scn.get_val_for_inner_table_field(),
          schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
        }
      } else if (OB_FAIL(extract_snapshot(*result, snapshot_info, tenant_id))) {
        LOG_WARN("fail to extract snapshot", KR(ret), K(tenant_id));
      } else if (OB_ITER_END != result->next()) {
        if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
        }
        LOG_WARN("get invalid next result", KR(ret), K(tenant_id));
      } else {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

int ObSnapshotTableProxy::check_snapshot_exist(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    const int64_t table_id,
    ObSnapShotType snapshot_type,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t latest_restore_point_value = 0;
  sqlclient::ObMySQLResult *result = NULL;
  is_exist = false;

  ObTimeoutCtx ctx;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type), K(table_id));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(tenant_id), K(ctx));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) FROM %s WHERE snapshot_type = %d AND "
                  "tenant_id = '%lu' ORDER BY gmt_create DESC LIMIT 1", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, 
                  snapshot_type, schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(tenant_id));
      } else if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          is_exist = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get next", KR(ret), K(tenant_id), K(snapshot_type));
        }
      } else if (OB_FAIL(result->get_int(0L, latest_restore_point_value))) {
        LOG_WARN("fail to get lastest restore point create time", KR(ret), K(tenant_id), 
          K(latest_restore_point_value));
      } else {
        is_exist = true;
      }
    }
    if (OB_SUCC(ret) && is_exist) {
      SMART_VAR(ObMySQLProxy::MySQLResult, res) {
        const char *table_name = NULL;
        const uint64_t exec_tenant_id = schema::ObSchemaUtils::get_exec_tenant_id(tenant_id);
        if (OB_FAIL(schema::ObSchemaUtils::get_all_table_name(exec_tenant_id, table_name))) {
          LOG_WARN("fail to get all table name", K(ret), K(exec_tenant_id));
        } else if (OB_FAIL(sql.assign_fmt("SELECT table_id FROM %s WHERE tenant_id = '%lu' AND "
                   "table_id = %ld AND gmt_create <= usec_to_time(%ld) limit 1", table_name,
                   schema::ObSchemaUtils::get_extract_tenant_id(exec_tenant_id, tenant_id),
                   schema::ObSchemaUtils::get_extract_schema_id(exec_tenant_id, table_id),
                   latest_restore_point_value))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
        } else if (OB_FAIL(proxy.read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to read", KR(ret), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get result", KR(ret), K(sql));
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
  }
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
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    const ObSnapShotType snapshot_type,
    bool &is_exist)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult *result = NULL;
  is_exist = false;
  ObTimeoutCtx ctx;

  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", KR(ret), K(tenant_id), K(ctx));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      if (OB_FAIL(sql.assign_fmt("SELECT * FROM %s WHERE snapshot_type = %d AND "
          "tenant_id = '%lu' LIMIT 1", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, snapshot_type,
          schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql));
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
  return ret;
}

int ObSnapshotTableProxy::get_snapshot_count(
    ObISQLClient &proxy,
    const uint64_t tenant_id,
    ObSnapShotType snapshot_type,
    int64_t &count)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if ((!is_valid_tenant_id(tenant_id)) || (snapshot_type != SNAPSHOT_FOR_RESTORE_POINT)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(snapshot_type));
  } else {
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      sqlclient::ObMySQLResult *result = NULL;
      ObTimeoutCtx ctx;
      if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
        LOG_WARN("fail to get timeout ctx", KR(ret), K(tenant_id), K(ctx));
      } else if (OB_FAIL(sql.assign_fmt("SELECT count(*) as cnt FROM %s WHERE snapshot_type = %d AND "
                 "tenant_id = '%lu'", OB_ALL_ACQUIRED_SNAPSHOT_TNAME, snapshot_type, 
                 schema::ObSchemaUtils::get_extract_tenant_id(tenant_id, tenant_id)))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(snapshot_type));
      } else if (OB_FAIL(proxy.read(res, tenant_id, sql.ptr()))) {
        LOG_WARN("fail to read", KR(ret), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get result", KR(ret), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("fail to get next", KR(ret), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", count, int64_t);
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase

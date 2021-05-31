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

#include "ob_freeze_info_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/time/ob_time_utility.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_global_stat_proxy.h"
#include "storage/ob_partition_service.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
using namespace storage;
using namespace share;
namespace share {
const char* ObFreezeInfoProxy::OB_ALL_FREEZE_INFO_TNAME = "__all_freeze_info";
const char* ObFreezeInfoProxy::FROZEN_VERSION_CNAME = "frozen_version";
const char* ObFreezeInfoProxy::FROZEN_TIMESTAMP_CNAME = "frozen_timestamp";
const char* ObFreezeInfoProxy::FREEZE_STATUS_CNAME = "freeze_status";
const char* ObFreezeInfoProxy::SCHEMA_VERSION_CNAME = "schema_version";
const char* ObFreezeInfoProxy::CLUSTER_VERSION_CNAME = "cluster_version";

OB_SERIALIZE_MEMBER(ObSimpleFrozenStatus, frozen_version_, frozen_timestamp_, cluster_version_);
OB_SERIALIZE_MEMBER(TenantIdAndSchemaVersion, tenant_id_, schema_version_);

int ObFreezeInfoProxy::get_freeze_info(
    ObISQLClient& sql_proxy, const int64_t major_version, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  int64_t frozen_version = 0;
  int64_t frozen_timestamp = 0;
  int64_t cluster_version = 0;
  bool exist = false;
  int64_t max_frozen_version = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_version))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_timestamp))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, cluster_version))) {
      if (OB_ERR_NULL_VALUE == ret) {
        cluster_version = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(frozen_version));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (major_version == 0) {
      if (frozen_version > max_frozen_version) {
        exist = true;
        max_frozen_version = frozen_version;
        frozen_status.frozen_version_ = frozen_version;
        frozen_status.frozen_timestamp_ = frozen_timestamp;
        frozen_status.cluster_version_ = cluster_version;
      }
    } else if (major_version == frozen_version) {
      exist = true;
      frozen_status.frozen_version_ = frozen_version;
      frozen_status.frozen_timestamp_ = frozen_timestamp;
      frozen_status.cluster_version_ = cluster_version;
      break;
    }
  }
  if (OB_SUCC(ret) && !exist) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find freeze info", K(ret), K(major_version));
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_inner(
    ObISQLClient& sql_proxy, const int64_t major_version, storage::ObFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  bool exist = false;
  int64_t max_frozen_version = 0;
  while (OB_SUCC(ret)) {
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    int64_t schema_version = 0;
    int64_t cluster_version = 0;

    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_version))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_timestamp))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(SCHEMA_VERSION_CNAME, schema_version))) {
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get int", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, cluster_version))) {
      if (OB_ERR_NULL_VALUE == ret) {
        cluster_version = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(frozen_version));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (major_version == 0) {
      if (frozen_version > max_frozen_version) {
        exist = true;
        max_frozen_version = frozen_version;
        frozen_status.frozen_version_ = frozen_version;
        frozen_status.frozen_timestamp_ = frozen_timestamp;
        frozen_status.status_ = common::COMMIT_SUCCEED;
        frozen_status.schema_version_ = schema_version;
        frozen_status.cluster_version_ = cluster_version;
      }
    } else if (major_version == frozen_version) {
      exist = true;
      frozen_status.frozen_version_ = frozen_version;
      frozen_status.frozen_timestamp_ = frozen_timestamp;
      frozen_status.status_ = common::COMMIT_SUCCEED;
      frozen_status.schema_version_ = schema_version;
      frozen_status.cluster_version_ = cluster_version;
      break;
    }
  }
  if (OB_SUCC(ret) && !exist) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("fail to find freeze info", K(ret), K(major_version));
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_larger_than_mock(
    ObISQLClient& sql_proxy, const int64_t major_version, ObIArray<ObFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_freeze_info_larger_than_inner(sql_proxy, major_version, frozen_statuses))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(major_version));
  } else {
    int64_t tenant_id = OB_SYS_TENANT_ID;
    ObSEArray<TenantIdAndSchemaVersion, 1> schema_versions;
    for (int64_t i = 0; i < frozen_statuses.count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(get_freeze_schema_v2(sql_proxy, tenant_id, frozen_statuses.at(i).frozen_version_, schema_versions))) {
        LOG_WARN("fail to get freeze schema version", KR(ret), K(i), K(frozen_statuses));
      } else if (schema_versions.count() <= 0) {
        // nothing todo
      } else {
        frozen_statuses.at(i).schema_version_ = schema_versions.at(0).schema_version_;
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_min_major_available_and_larger_info_inner(ObISQLClient& sql_proxy,
    const int64_t major_version, int64_t& min_major_version, ObIArray<ObFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  ObTimeoutCtx ctx;
  min_major_version = INT64_MAX;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  ObFrozenStatus frozen_status;
  while (OB_SUCC(ret)) {
    frozen_status.reset();
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.version_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(SCHEMA_VERSION_CNAME, frozen_status.schema_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.schema_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get int", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, frozen_status.cluster_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.cluster_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(frozen_status));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (FALSE_IT(min_major_version = MIN(min_major_version, frozen_status.frozen_version_.version_))) {
    } else if (frozen_status.frozen_version_ > major_version) {
      frozen_status.status_ = common::COMMIT_SUCCEED;
      if (OB_FAIL(frozen_statuses.push_back(frozen_status))) {
        LOG_WARN("fail to push back", K(ret), K(frozen_status));
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_larger_than_inner(
    ObISQLClient& sql_proxy, const int64_t major_version, ObIArray<ObFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  ObFrozenStatus frozen_status;
  while (OB_SUCC(ret)) {
    frozen_status.reset();
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.version_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(SCHEMA_VERSION_CNAME, frozen_status.schema_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.schema_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get int", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, frozen_status.cluster_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.cluster_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(frozen_status));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (frozen_status.frozen_version_ > major_version) {
      frozen_status.status_ = common::COMMIT_SUCCEED;
      if (OB_FAIL(frozen_statuses.push_back(frozen_status))) {
        LOG_WARN("fail to push back", K(ret), K(frozen_status));
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_frozen_status_without_schema_version_v2(
    ObISQLClient& sql_proxy, ObIArray<ObFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  int64_t frozen_version = 0;
  if (OB_FAIL(get_max_frozen_version_with_schema(sql_proxy, frozen_version))) {
    if (OB_ERR_NULL_VALUE == ret) {
      // overwrite ret
      if (OB_FAIL(get_frozen_status_without_schema_version_inner(sql_proxy, frozen_statuses))) {
        LOG_WARN("fail to get frozen status", KR(ret));
      }
    } else {
      LOG_WARN("fail to get max frozen version", KR(ret));
    }
  } else if (OB_FAIL(get_freeze_info_larger_than_inner(sql_proxy, frozen_version, frozen_statuses))) {
    LOG_WARN("fail to get freeze info larger than", KR(ret), K(frozen_version));
  }
  return ret;
}

int ObFreezeInfoProxy::get_max_frozen_version_with_schema(ObISQLClient& sql_proxy, int64_t& frozen_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    ObTimeoutCtx ctx;
    frozen_version = 0;
    if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
      LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
    } else if (OB_FAIL(sql.assign_fmt(
                   "SELECT MAX(FROZEN_VERSION) AS FROZEN_VERSION FROM %s", OB_ALL_FREEZE_SCHEMA_VERSION_TNAME))) {
      LOG_WARN("fail to assign fmt", KR(ret));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next", KR(ret));
    } else if (OB_FAIL(result->get_int("FROZEN_VERSION", frozen_version))) {
      if (OB_ERR_NULL_VALUE != ret) {
        LOG_WARN("fail to get int", KR(ret));
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_frozen_status_without_schema_version_inner(
    ObISQLClient& sql_proxy, ObIArray<ObFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  ObFrozenStatus frozen_status;
  while (OB_SUCC(ret)) {
    frozen_status.reset();
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.version_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(SCHEMA_VERSION_CNAME, frozen_status.schema_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.schema_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get int", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, frozen_status.cluster_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.cluster_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(frozen_status));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (0 == frozen_status.schema_version_) {
      frozen_status.status_ = common::COMMIT_SUCCEED;
      if (OB_FAIL(frozen_statuses.push_back(frozen_status))) {
        LOG_WARN("fail to push back", K(ret), K(frozen_status));
      }
    }
  }

  return ret;
}

int ObFreezeInfoProxy::get_max_frozen_status_with_schema_version_inner(
    ObISQLClient& sql_proxy, ObFrozenStatus& max_frozen_status)
{
  int ret = OB_SUCCESS;
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  max_frozen_status.reset();
  ObFrozenStatus frozen_status;
  while (OB_SUCC(ret)) {
    frozen_status.reset();
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.version_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(SCHEMA_VERSION_CNAME, frozen_status.schema_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.schema_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get int", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, frozen_status.cluster_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        frozen_status.cluster_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(frozen_status));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (frozen_status.schema_version_ > 0 && max_frozen_status.frozen_version_ < frozen_status.frozen_version_) {
      max_frozen_status = frozen_status;
    }
  }
  max_frozen_status.status_ = common::COMMIT_SUCCEED;
  return ret;
}
////////////////////////////////////
int ObFreezeInfoProxy::set_freeze_info(ObMySQLTransaction& trans, const storage::ObFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObCoreTableProxy kv(OB_ALL_FREEZE_INFO_TNAME, trans);
  if (OB_FAIL(dml.add_pk_column(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.major_)) ||
      OB_FAIL(dml.add_column(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_)) ||
      OB_FAIL(dml.add_column(CLUSTER_VERSION_CNAME, frozen_status.cluster_version_))) {
    LOG_WARN("fail to add column", KR(ret), K(frozen_status));
  } else if (OB_FAIL(kv.load_for_update())) {
    LOG_WARN("fail to load for update", K(ret));
  } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
    LOG_WARN("fail to splice core cells", K(ret));
  } else if (OB_FAIL(kv.incremental_replace_row(cells, affected_rows))) {
    LOG_WARN("fail to replace row", K(ret), K(frozen_status));
  }
  return ret;
}

int ObFreezeInfoProxy::update_frozen_schema(ObMySQLProxy& sql_proxy, const storage::ObFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObMySQLTransaction trans;
  if (GCTX.is_schema_splited()) {
    ret = OB_OP_NOT_ALLOW;
    LOG_WARN("should figure out tenant id", KR(ret), K(frozen_status));
  } else if (!frozen_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(frozen_status));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("fail to start", K(ret));
  } else {
    ObCoreTableProxy kv(OB_ALL_FREEZE_INFO_TNAME, trans);
    if (OB_FAIL(dml.add_pk_column(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.major_)) ||
        OB_FAIL(dml.add_column(SCHEMA_VERSION_CNAME, frozen_status.schema_version_)) ||
        OB_FAIL(dml.add_column(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_)) ||
        OB_FAIL(dml.add_column(CLUSTER_VERSION_CNAME, frozen_status.cluster_version_))) {
      LOG_WARN("fail to add column", KR(ret), K(frozen_status));
    } else if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("fail to load for update", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("fail to splice core cells", K(ret));
    } else if (OB_FAIL(kv.incremental_update_row(cells, affected_rows))) {
      LOG_WARN("fail to replace row", K(ret), K(frozen_status));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect update zore/one row", K(ret), K(affected_rows));
    }
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

int ObFreezeInfoProxy::batch_delete(
    ObMySQLProxy& sql_proxy, const int64_t min_frozen_timestamp, const int64_t min_frozen_version)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (min_frozen_timestamp <= 0 || min_frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_frozen_timestamp), K(min_frozen_version));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("fail to start", K(ret));
  } else {
    ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, trans);
    if (OB_FAIL(core_table.load_for_update())) {
      LOG_WARN("fail to load core table", K(ret));
    }
    int64_t row_id = -1;
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    const ObCoreTableProxy::Row* row = NULL;
    while (OB_SUCC(ret)) {
      frozen_version = 0;
      frozen_timestamp = 0;
      row_id = -1;
      row = NULL;
      if (OB_FAIL(core_table.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to next", K(ret));
        }
      } else if (OB_FAIL(core_table.get_cur_row(row))) {
        LOG_WARN("fail to get cur row", K(ret));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL row", K(ret));
      } else if (OB_FAIL(row->get_int("frozen_version", frozen_version))) {
        LOG_WARN("fail to get int", K(ret));
      } else if (OB_FAIL(row->get_int("frozen_timestamp", frozen_timestamp))) {
        LOG_WARN("fail to get int", K(ret));
      } else if (frozen_version < min_frozen_version && frozen_timestamp < min_frozen_timestamp) {
        row_id = row->get_row_id();
        if (row_id < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid row_id", K(ret), K(row_id));
        } else if (OB_FAIL(core_table.execute_delete_sql(row_id))) {
          LOG_WARN("fail to delete sql", K(ret), K(row_id));
        }
      }
    }
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

int ObFreezeInfoProxy::update_frozen_schema_v2(ObMySQLProxy& sql_proxy, const int64_t frozen_version,
    const common::ObIArray<TenantIdAndSchemaVersion>& id_versions)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;

  if (id_versions.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(id_versions));
  } else if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (TENANT_ID, FROZEN_VERSION, SCHEMA_VERSION) VALUES ",
                 OB_ALL_FREEZE_SCHEMA_VERSION_TNAME))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else {
    for (int64_t i = 0; i < id_versions.count() && OB_SUCC(ret); i++) {
      const TenantIdAndSchemaVersion& info = id_versions.at(i);
      if (0 == i) {
        if (OB_FAIL(sql.append_fmt("(%lu, %ld, %ld)", info.tenant_id_, frozen_version, info.schema_version_))) {
          LOG_WARN("fail to append fmt", KR(ret), K(info), K(frozen_version));
        }
      } else if (OB_FAIL(sql.append_fmt(", (%lu, %ld, %ld)", info.tenant_id_, frozen_version, info.schema_version_))) {
        LOG_WARN("fail to append fmt", KR(ret), K(info), K(frozen_version));
      }
    }
  }
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql));
  } else if (affected_rows != id_versions.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected row", KR(ret), K(affected_rows), "expect_affected_rows", id_versions.count());
  }
  return ret;
}

// tenant_id = 0: means get all tenant schema_version of major version
int ObFreezeInfoProxy::get_freeze_schema_v2(common::ObISQLClient& sql_proxy, const uint64_t tenant_id,
    const int64_t major_version, ObIArray<TenantIdAndSchemaVersion>& schema_version_infos)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  schema_version_infos.reset();
  ObTimeoutCtx ctx;
  if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (major_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(major_version));
  } else if (OB_FAIL(sql.assign_fmt(
                 "SELECT * FROM %s WHERE frozen_version = %ld", OB_ALL_FREEZE_SCHEMA_VERSION_TNAME, major_version))) {
    LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(major_version));
  } else if (tenant_id > 0) {
    if (OB_FAIL(sql.append_fmt(" AND TENANT_ID = %lu", tenant_id))) {
      LOG_WARN("fail to append fmt", KR(ret), K(tenant_id));
    }
  }

  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else {
      TenantIdAndSchemaVersion info;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        info.reset();
        EXTRACT_INT_FIELD_MYSQL(*result, "schema_version", info.schema_version_, int64_t);
        EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", info.tenant_id_, int64_t);
        if (OB_FAIL(ret)) {
          // nothing todo
        } else if (OB_FAIL(schema_version_infos.push_back(info))) {
          LOG_WARN("fail to push back", KR(ret), K(info));
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      ret = (OB_SUCCESS == ret) ? OB_ERR_UNEXPECTED : ret;
      LOG_WARN("next should return OB_ITER_END", KR(ret));
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_v2(common::ObISQLClient& sql_proxy, const int64_t major_version,
    storage::ObFrozenStatus& frozen_status, ObIArray<TenantIdAndSchemaVersion>& frozen_schema_versions)
{
  int ret = OB_SUCCESS;
  int64_t tenant_id = 0;
  frozen_status.reset();
  frozen_schema_versions.reset();
  if (OB_FAIL(get_freeze_info_inner(sql_proxy, major_version, frozen_status))) {
    LOG_WARN("fail to get freeze info", KR(ret));
  } else if (frozen_status.schema_version_ > 0) {
    // nothing todo
  } else if (OB_FAIL(
                 get_freeze_schema_v2(sql_proxy, tenant_id, frozen_status.frozen_version_, frozen_schema_versions))) {
    LOG_WARN("fail to get freeze schema", KR(ret));
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_v2(common::ObISQLClient& sql_proxy, const int64_t tenant_id,
    const int64_t major_version, storage::ObFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObArray<TenantIdAndSchemaVersion> info;
  frozen_status.reset();
  if (OB_INVALID_TENANT_ID == tenant_id || 0 > major_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(major_version));
  } else if (OB_FAIL(get_freeze_info_inner(sql_proxy, major_version, frozen_status))) {
    LOG_WARN("fail to get freeze info", KR(ret), K(tenant_id), K(major_version));
  } else if (frozen_status.schema_version_ > 0) {
    // schema version is valid, may be before schema_splited;
  } else if (OB_FAIL(get_freeze_schema_v2(sql_proxy, tenant_id, frozen_status.frozen_version_, info))) {
    LOG_WARN("fail to get freeze schema", KR(ret), K(tenant_id), K(major_version));
  } else if (0 == info.count()) {
    // schema_version is not set
  } else if (1 != info.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema version", KR(ret), K(info));
  } else {
    frozen_status.schema_version_ = info.at(0).schema_version_;
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_larger_than_v2(common::ObISQLClient& sql_proxy, const int64_t major_version,
    common::ObIArray<ObSimpleFrozenStatus>& frozen_statuses)
{
  int64_t unused = INT64_MAX;
  return get_min_major_available_and_larger_info(sql_proxy, major_version, unused, frozen_statuses);
}

int ObFreezeInfoProxy::get_min_major_available_and_larger_info(common::ObISQLClient& sql_proxy,
    const int64_t major_version, int64_t& min_major_version, common::ObIArray<ObSimpleFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  frozen_statuses.reset();
  min_major_version = INT64_MAX;
  ObArray<storage::ObFrozenStatus> frozen_infos;
  if (0 > major_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(major_version));
  } else if (OB_FAIL(get_min_major_available_and_larger_info_inner(
                 sql_proxy, major_version, min_major_version, frozen_infos))) {
    LOG_WARN("fail to get freeze info larger than and min major version", KR(ret));
  } else {
    ObSimpleFrozenStatus frozen_info;
    for (int64_t i = 0; i < frozen_infos.count() && OB_SUCC(ret); i++) {
      frozen_info.reset();
      frozen_info = frozen_infos.at(i);
      if (OB_FAIL(frozen_statuses.push_back(frozen_info))) {
        LOG_WARN("fail to push back", KR(ret), K(frozen_info));
      }
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_freeze_info_larger_than_v2(common::ObISQLClient& sql_proxy, const int64_t tenant_id,
    const int64_t major_version, common::ObIArray<storage::ObFrozenStatus>& frozen_statuses)
{
  int ret = OB_SUCCESS;
  ObArray<TenantIdAndSchemaVersion> schema_version_infos;
  int64_t max_version_with_schema = major_version;
  int64_t index = -1;
  if (0 > major_version || 0 >= tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(major_version));
  } else if (OB_FAIL(get_freeze_info_larger_than_inner(sql_proxy, major_version, frozen_statuses))) {
    LOG_WARN("fail to get freeze info larger than", KR(ret), K(tenant_id), K(major_version));
  } else {
    for (int64_t i = 0; i < frozen_statuses.count() && OB_SUCC(ret); i++) {
      const storage::ObFrozenStatus& status = frozen_statuses.at(i);
      if (status.schema_version_ != 0) {
        if (max_version_with_schema < status.frozen_version_.version_) {
          max_version_with_schema = status.frozen_version_.version_;
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("frozen version should in order", K(max_version_with_schema), K(status));
        }
      } else {
        index = i;
        break;
      }
    }
  }
  ObSqlString sql;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    sqlclient::ObMySQLResult* result = NULL;
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_FAIL(sql.assign_fmt("SELECT FROZEN_VERSION, SCHEMA_VERSION FROM %s WHERE TENANT_ID = %lu"
                                      " AND FROZEN_VERSION > %ld",
                   OB_ALL_FREEZE_SCHEMA_VERSION_TNAME,
                   tenant_id,
                   max_version_with_schema))) {
      LOG_WARN("fail to assign fmt", KR(ret), K(tenant_id), K(max_version_with_schema));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else {
      int64_t frozen_version = 0;
      int64_t schema_version = 0;
      while (OB_SUCC(ret) && OB_SUCC(result->next())) {
        if (index >= frozen_statuses.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid schema version", KR(ret), K(index), K(frozen_statuses));
        } else if (OB_FAIL(result->get_int("FROZEN_VERSION", frozen_version))) {
          LOG_WARN("fail to get int", KR(ret));
        } else if (OB_FAIL(result->get_int("SCHEMA_VERSION", schema_version))) {
          LOG_WARN("fail to get int", KR(ret));
        } else if (frozen_version != frozen_statuses.at(index).frozen_version_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid frozen version",
              KR(ret),
              K(frozen_version),
              "expect_version",
              frozen_statuses.at(index).frozen_version_);
        } else {
          frozen_statuses.at(index).schema_version_ = schema_version;
          index++;
        }
      }
    }
  }
  return ret;
}

// Get the minimal freeze info's schema version which is larger than snapshot_ts.
int ObFreezeInfoProxy::get_schema_info_larger_than(
    common::ObISQLClient& sql_proxy, const int64_t snapshot_ts, ObIArray<TenantIdAndSchemaVersion>& schema_versions)
{
  int ret = OB_SUCCESS;
  schema_versions.reset();
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  if (snapshot_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_ts));
  } else if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  ObFrozenStatus frozen_status;
  int64_t frozen_version = 0;
  while (OB_SUCC(ret)) {
    frozen_status.reset();
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, frozen_status.frozen_version_.version_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, frozen_status.frozen_timestamp_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (frozen_status.frozen_timestamp_ <= snapshot_ts) {  // suppose: the result is sorted by frozen_version
      continue;
    } else {
      frozen_version = frozen_status.frozen_version_;
      break;
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  int64_t tenant_id = 0;
  if (OB_FAIL(ret)) {
    // nothing todo
  } else if (0 == frozen_version) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("no frozen info less than snapshot", KR(ret), K(snapshot_ts));
  } else if (OB_FAIL(get_freeze_schema_v2(sql_proxy, tenant_id, frozen_version, schema_versions))) {
    LOG_WARN("fail to get freeze schema versions", KR(ret), K(tenant_id), K(frozen_version));
  }
  return ret;
}

// Get the max freeze info's schema version which is less than snapshot_ts
int ObFreezeInfoProxy::get_schema_info_less_than(
    common::ObISQLClient& sql_proxy, const int64_t snapshot_ts, ObIArray<TenantIdAndSchemaVersion>& schema_versions)
{
  int ret = OB_SUCCESS;
  schema_versions.reset();
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  ObSimpleFrozenStatus frozen_status;
  int64_t tenant_id = 0;
  if (0 >= snapshot_ts) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_ts));
  } else if (OB_FAIL(get_frozen_info_less_than(sql_proxy, snapshot_ts, frozen_status))) {
    LOG_WARN("fail to get frozen version less than snapshot ts", K(ret), K(snapshot_ts));
  } else if (!frozen_status.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_DEBUG("no frozen info less than snapshot", KR(ret), K(snapshot_ts), K(frozen_status));
  } else if (OB_FAIL(get_freeze_schema_v2(sql_proxy, tenant_id, frozen_status.frozen_version_, schema_versions))) {
    LOG_WARN("fail to get freeze schema versions", KR(ret), K(tenant_id), K(frozen_status));
  }
  return ret;
}

int ObFreezeInfoProxy::get_frozen_info_less_than(
    common::ObISQLClient& sql_proxy, const int64_t snapshot_ts, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  frozen_status.reset();
  ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, sql_proxy);
  if (0 >= snapshot_ts) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(snapshot_ts));
  } else if (OB_FAIL(core_table.load())) {
    LOG_WARN("fail to load core table", K(ret));
  }
  ObSimpleFrozenStatus tmp_frozen_status;
  while (OB_SUCC(ret)) {
    tmp_frozen_status.reset();
    if (OB_FAIL(core_table.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to next", K(ret));
      }
    } else if (OB_FAIL(core_table.get_int(FROZEN_VERSION_CNAME, tmp_frozen_status.frozen_version_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(FROZEN_TIMESTAMP_CNAME, tmp_frozen_status.frozen_timestamp_))) {
      LOG_WARN("fail to get int", K(ret));
    } else if (OB_FAIL(core_table.get_int(CLUSTER_VERSION_CNAME, tmp_frozen_status.cluster_version_))) {
      if (OB_ERR_NULL_VALUE == ret) {
        tmp_frozen_status.cluster_version_ = 0;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get int", KR(ret), K(tmp_frozen_status));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (tmp_frozen_status.frozen_timestamp_ <= snapshot_ts) {  // suppose: the result is sorted by frozen_verison
      frozen_status = tmp_frozen_status;
    } else {
      break;
    }
  }
  if (OB_FAIL(ret)) {
    // nothing
  } else if (!frozen_status.is_valid()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to find frozen status less than snapshot timestamp", K(ret), K(snapshot_ts));
  }
  return ret;
}

int ObFreezeInfoProxy::fix_tenant_schema(common::ObISQLClient& sql_proxy, const int64_t frozen_version,
    const int64_t tenant_id, const int64_t schema_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (frozen_version <= 0 || tenant_id <= 0 || schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET schema_version = %ld WHERE frozen_version = %ld AND "
                                    " tenant_id = %ld AND schema_version = %ld",
                 OB_ALL_FREEZE_SCHEMA_VERSION_TNAME,
                 schema_version,
                 frozen_version,
                 tenant_id,
                 OB_INVALID_SCHEMA_VERSION))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(sql_proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql));
  } else if (affected_rows > 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid affected row", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObFreezeInfoProxy::get_max_frozen_status_with_schema_version_v2(
    common::ObISQLClient& sql_proxy, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(
          sql.assign_fmt("SELECT MAX(FROZEN_VERSION) AS FROZEN_VERSION FROM (SELECT DISTINCT FROZEN_VERSION FROM %s)",
              OB_ALL_FREEZE_SCHEMA_VERSION_TNAME))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(get_max_frozen_status_with_schema_version_v2_inner(sql_proxy, sql, frozen_status))) {
    LOG_WARN("fail to get max frozen status", KR(ret), K(sql));
  }
  return ret;
}

int ObFreezeInfoProxy::get_frozen_status_less_than(common::ObISQLClient& sql_proxy, const int64_t tenant_id,
    const int64_t schema_version, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  UNUSED(tenant_id);
  if (OB_FAIL(sql.assign_fmt("SELECT MAX(FROZEN_VERSION) AS FROZEN_VERSION FROM %s where \
                              tenant_id = %ld and schema_version < %ld",
          OB_ALL_FREEZE_SCHEMA_VERSION_TNAME,
          tenant_id,
          schema_version))) {
    LOG_WARN("failed to assign fmt", K(ret), K(tenant_id), K(schema_version));
  } else if (OB_FAIL(get_max_frozen_status_with_schema_version_v2_inner(sql_proxy, sql, frozen_status))) {
    LOG_WARN("failed to get max frozen status with schema", K(ret), K(sql));
  }
  return ret;
}

int ObFreezeInfoProxy::get_max_frozen_status_with_schema_version_v2_inner(
    common::ObISQLClient& sql_proxy, ObSqlString& sql, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    int64_t frozen_version = 0;
    storage::ObFrozenStatus full_frozen_status;
    sqlclient::ObMySQLResult* result = NULL;
    if (sql.empty() || !sql.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.read(res, sql.ptr()))) {
      LOG_WARN("fail to read", KR(ret), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get result failed", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next", KR(ret));
    } else if (OB_FAIL(result->get_int("FROZEN_VERSION", frozen_version))) {
      if (OB_ERR_NULL_VALUE == ret) {
        ret = OB_SUCCESS;
        if (OB_FAIL(get_max_frozen_status_with_schema_version_inner(sql_proxy, full_frozen_status))) {
          LOG_WARN("fail to get max frozen status with schema version", KR(ret));
        } else {
          frozen_status.frozen_version_ = full_frozen_status.frozen_version_;
          frozen_status.frozen_timestamp_ = full_frozen_status.frozen_timestamp_;
        }
      } else {
        LOG_WARN("fail to get int", KR(ret));
      }
    } else if (OB_FAIL(get_freeze_info_inner(sql_proxy, frozen_version, full_frozen_status))) {
      LOG_WARN("fail to get freeze info", KR(ret), K(frozen_version));
    } else {
      frozen_status = full_frozen_status;
    }
  }
  return ret;
}

int ObFreezeInfoProxy::get_max_frozen_status_with_integrated_schema_version_v2(
    common::ObISQLClient& sql_proxy, ObSimpleFrozenStatus& frozen_status)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT MAX(FROZEN_VERSION) AS FROZEN_VERSION FROM (SELECT DISTINCT FROZEN_VERSION FROM "
                             "%s EXCEPT SELECT DISTINCT FROZEN_VERSION FROM %s WHERE SCHEMA_VERSION = %ld)",
          OB_ALL_FREEZE_SCHEMA_VERSION_TNAME,
          OB_ALL_FREEZE_SCHEMA_VERSION_TNAME,
          OB_INVALID_SCHEMA_VERSION))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(get_max_frozen_status_with_schema_version_v2_inner(sql_proxy, sql, frozen_status))) {
    LOG_WARN("fail to get max frozen status", KR(ret), K(sql));
  }
  return ret;
}

int ObFreezeInfoProxy::batch_delete_frozen_info_larger_than(
    common::ObISQLClient& proxy, const int64_t max_frozen_version)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_VERSION == max_frozen_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(max_frozen_version));
  } else {
    ObCoreTableProxy core_table(OB_ALL_FREEZE_INFO_TNAME, proxy);
    if (OB_FAIL(core_table.load_for_update())) {
      LOG_WARN("fail to load core table", K(ret));
    }
    int64_t row_id = -1;
    int64_t frozen_version = 0;
    int64_t frozen_timestamp = 0;
    const ObCoreTableProxy::Row* row = NULL;
    while (OB_SUCC(ret)) {
      frozen_version = 0;
      frozen_timestamp = 0;
      row_id = -1;
      row = NULL;
      if (OB_FAIL(core_table.next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("fail to next", K(ret));
        }
      } else if (OB_FAIL(core_table.get_cur_row(row))) {
        LOG_WARN("fail to get cur row", K(ret));
      } else if (OB_ISNULL(row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL row", K(ret));
      } else if (OB_FAIL(row->get_int("frozen_version", frozen_version))) {
        LOG_WARN("fail to get int", K(ret));
      } else if (frozen_version > max_frozen_version) {
        row_id = row->get_row_id();
        if (row_id < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid row_id", K(ret), K(row_id));
        } else if (OB_FAIL(core_table.execute_delete_sql(row_id))) {
          LOG_WARN("fail to delete sql", K(ret), K(row_id));
        }
      }
    }
  }
  return ret;
}
int ObFreezeInfoProxy::batch_delete_frozen_schema_larger_than(
    common::ObISQLClient& proxy, const int64_t max_fronze_version)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_INVALID_VERSION == max_fronze_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s  WHERE frozen_version > %ld",
                 OB_ALL_FREEZE_SCHEMA_VERSION_TNAME,
                 max_fronze_version))) {
    LOG_WARN("fail to assign fmt", KR(ret));
  } else if (OB_FAIL(proxy.write(sql.ptr(), affected_rows))) {
    LOG_WARN("fail to write sql", KR(ret), K(sql));
  }
  return ret;
}

}  // end namespace share
}  // end namespace oceanbase

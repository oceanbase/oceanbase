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

#include "ob_schema_status_proxy.h"
#include "lib/oblog/ob_log.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/ob_core_table_proxy.h"
#include "share/ob_dml_sql_splicer.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::schema;
namespace share
{
const char *ObSchemaStatusProxy::OB_ALL_SCHEMA_STATUS_TNAME = "__all_schema_status";
const char *ObSchemaStatusProxy::TENANT_ID_CNAME = "tenant_id";
const char *ObSchemaStatusProxy::SNAPSHOT_TIMESTAMP_CNAME = "snapshot_timestamp";
const char *ObSchemaStatusProxy::READABLE_SCHEMA_VERSION_CNAME = "readable_schema_version";

//created_schema_version is no use in memory, so ignore created_schema_version_ change
int ObSchemaStatusUpdater::operator() (common::hash::HashMapPair<uint64_t, share::schema::ObRefreshSchemaStatus> &entry) {
  int ret = OB_SUCCESS;
  if (entry.second.tenant_id_ != schema_status_.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema_status not matched", K(ret), K(entry), K_(schema_status));
  } else if (OB_INVALID_TIMESTAMP == schema_status_.snapshot_timestamp_) {
    if (schema_status_.snapshot_timestamp_ != entry.second.snapshot_timestamp_) {
      LOG_INFO("[SCHEMA_STATUS], reset schema status", "old_schema_status", entry.second,
               "new_schema_status", schema_status_);
    }
    entry.second = schema_status_;
  } else if (schema_status_.snapshot_timestamp_ >= entry.second.snapshot_timestamp_) {
    if (schema_status_.snapshot_timestamp_ != entry.second.snapshot_timestamp_) {
      LOG_INFO("[SCHEMA_STATUS] update schema status",
               "old_schema_status", entry.second,
               "new_schema_status", schema_status_);
    }
    entry.second = schema_status_;
  } else {
    LOG_INFO("[SCHEMA_STATUS] schema_status less than the old value, just ignore", K(entry), K(schema_status_));
  }
  return ret;
}

int ObSchemaStatusProxy::init()
{
  int ret = OB_SUCCESS;
  ObRefreshSchemaStatus schema_status;
  schema_status.tenant_id_ = OB_SYS_TENANT_ID;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(schema_status_cache_.create(TENANT_SCHEMA_STATUS_BUCKET_NUM,
                                                 ObModIds::OB_SCHEMA_STATUS_MAP,
                                                 ObModIds::OB_SCHEMA_STATUS_MAP))) {
    LOG_WARN("fail to init map", K(ret));
  } else if (OB_FAIL(schema_status_cache_.set_refactored(OB_SYS_TENANT_ID, schema_status))) {
    LOG_WARN("fail to set init value", K(ret), K(schema_status));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSchemaStatusProxy::check_inner_stat()
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
  }
  return ret;
}

int ObSchemaStatusProxy::nonblock_get(const uint64_t refresh_tenant_id,
    ObRefreshSchemaStatus &refresh_schema_status)
{
  int ret = OB_SUCCESS;
  refresh_schema_status.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == refresh_tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(refresh_tenant_id));
  } else if (OB_FAIL(schema_status_cache_.get_refactored(refresh_tenant_id, refresh_schema_status))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get schema_status from cache", K(ret), K(refresh_tenant_id));
    } else {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObSchemaStatusProxy::get_refresh_schema_status(
    const uint64_t refresh_tenant_id,
    ObRefreshSchemaStatus &refresh_schema_status)
{
  int ret = OB_SUCCESS;
  refresh_schema_status.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_INVALID_TENANT_ID == refresh_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", K(ret), K(refresh_tenant_id));
  } else if (OB_FAIL(schema_status_cache_.get_refactored(refresh_tenant_id, refresh_schema_status))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get schema_status from cache", K(ret), K(refresh_tenant_id));
    } else if (OB_FAIL(load_refresh_schema_status(refresh_tenant_id, refresh_schema_status))) { //overwrite ret
      LOG_WARN("fail to load_refresh_schema_status", KR(ret), K(refresh_tenant_id));
    }
  }
  return ret;
}

int ObSchemaStatusProxy::load_refresh_schema_status(
    const uint64_t refresh_tenant_id,
    ObRefreshSchemaStatus &refresh_schema_status)
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  refresh_schema_status.reset();

  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", KR(ret));
  } else if (OB_INVALID_TENANT_ID == refresh_tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant_id", KR(ret), K(refresh_tenant_id));
  } else if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start trans", KR(ret));
  } else {
    ObCoreTableProxy core_table(OB_ALL_SCHEMA_STATUS_TNAME, trans, OB_SYS_TENANT_ID);
    if (OB_FAIL(core_table.load())) {
      LOG_WARN("fail to load core table", KR(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int64_t snapshot_timestamp = OB_INVALID_TIMESTAMP;
      int64_t readable_schema_version = OB_INVALID_VERSION;
      bool exist = false;
      while(OB_SUCC(ret)) {
        if (OB_FAIL(core_table.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to next", KR(ret));
          }
        } else if (OB_FAIL(core_table.get_uint(TENANT_ID_CNAME, tenant_id))) {
          LOG_WARN("fail to get int", KR(ret));
        } else if (OB_FAIL(core_table.get_int(SNAPSHOT_TIMESTAMP_CNAME, snapshot_timestamp))) {
          LOG_WARN("fail to get int", KR(ret));
        } else if (OB_FAIL(core_table.get_int(READABLE_SCHEMA_VERSION_CNAME, readable_schema_version))) {
          LOG_WARN("fail to get int", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (refresh_tenant_id == tenant_id) {
          refresh_schema_status.tenant_id_ = tenant_id;
          refresh_schema_status.snapshot_timestamp_ = snapshot_timestamp;
          refresh_schema_status.readable_schema_version_ = readable_schema_version;
          exist = true;
          break;
        }
      }
      if (OB_SUCC(ret)) {
        if (!exist) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("tenant refresh schema status not exist", KR(ret), K(refresh_tenant_id));
        } else {
          ObSchemaStatusUpdater updater(refresh_schema_status);
          if (OB_FAIL(schema_status_cache_.atomic_refactored(refresh_tenant_id, updater))) {
            if (OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("fail to update schema_status", KR(ret), K(refresh_tenant_id), K(refresh_schema_status));
            } else if (OB_FAIL(schema_status_cache_.set_refactored(refresh_tenant_id, refresh_schema_status))) {
              if (OB_HASH_EXIST == ret) {
                LOG_WARN("concurrent set, just ignore", KR(ret), K(refresh_tenant_id), K(refresh_schema_status));
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to set schema_status", KR(ret), K(refresh_tenant_id), K(refresh_schema_status));
              }
            }
          }
        }
      }
    }
  }

  if (trans.is_started()) {
    bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to commit transaction", KR(tmp_ret), KR(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  return ret;
}

int ObSchemaStatusProxy::get_refresh_schema_status(
    ObIArray<ObRefreshSchemaStatus> &refresh_schema_status_array)
{
  int ret = OB_SUCCESS;
  refresh_schema_status_array.reset();
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    FOREACH_X(it, schema_status_cache_, OB_SUCC(ret)) {
      if (OB_FAIL(refresh_schema_status_array.push_back(it->second))) {
        LOG_WARN("fail to push back refresh schema status", K(ret), "schema_status", it->second);
      }
    }
  }
  return ret;
}

int ObSchemaStatusProxy::load_refresh_schema_status()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else {
    ObCoreTableProxy core_table(OB_ALL_SCHEMA_STATUS_TNAME, sql_proxy_, OB_SYS_TENANT_ID);
    if (OB_FAIL(core_table.load())) {
      LOG_WARN("fail to load core table", K(ret));
    } else {
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int64_t snapshot_timestamp = OB_INVALID_TIMESTAMP;
      int64_t readable_schema_version = OB_INVALID_VERSION;
      while(OB_SUCC(ret)) {
        if (OB_FAIL(core_table.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to next", K(ret));
          }
        } else if (OB_FAIL(core_table.get_uint(TENANT_ID_CNAME, tenant_id))) {
          LOG_WARN("fail to get int", K(ret));
        } else if (OB_FAIL(core_table.get_int(SNAPSHOT_TIMESTAMP_CNAME, snapshot_timestamp))) {
          LOG_WARN("fail to get int", K(ret));
        } else if (OB_FAIL(core_table.get_int(READABLE_SCHEMA_VERSION_CNAME, readable_schema_version))) {
          LOG_WARN("fail to get int", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else {
          ObRefreshSchemaStatus schema_status;
          schema_status.tenant_id_ = tenant_id;
          schema_status.snapshot_timestamp_ = snapshot_timestamp;
          schema_status.readable_schema_version_ = readable_schema_version;
          ObSchemaStatusUpdater updater(schema_status);
          if (OB_FAIL(schema_status_cache_.atomic_refactored(tenant_id, updater))) {
            if (OB_HASH_NOT_EXIST != ret) {
              LOG_WARN("fail to update schema_status", K(ret), K(tenant_id), K(schema_status));
            } else if (OB_FAIL(schema_status_cache_.set_refactored(tenant_id, schema_status))) {
              if (OB_HASH_EXIST == ret) {
                LOG_WARN("concurrent set, just ignore", K(ret), K(tenant_id), K(schema_status));
                ret = OB_SUCCESS;
              } else {
                LOG_WARN("fail to set schema_status", K(ret), K(tenant_id), K(schema_status));
              }
            }
          }
        }
      }
    }
  }
  
  LOG_INFO("[SCHEMA_STATUS] load refreshed schema status", K(ret));
  return ret;
}

int ObSchemaStatusProxy::set_tenant_schema_status(
    const ObRefreshSchemaStatus &refresh_schema_status)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (!refresh_schema_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("refresh schema status is invalid", K(ret), K(refresh_schema_status));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP != refresh_schema_status.snapshot_timestamp_
                         && 0 != refresh_schema_status.snapshot_timestamp_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("snapshot timestamp must invalid or zero", KR(ret), K(refresh_schema_status));
  } else if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start", K(ret));
  } else {
    ObCoreTableProxy kv(OB_ALL_SCHEMA_STATUS_TNAME, trans, OB_SYS_TENANT_ID);
    if (OB_FAIL(dml.add_pk_column(TENANT_ID_CNAME, refresh_schema_status.tenant_id_))
        || OB_FAIL(dml.add_column(SNAPSHOT_TIMESTAMP_CNAME, refresh_schema_status.snapshot_timestamp_))
        || OB_FAIL(dml.add_column(READABLE_SCHEMA_VERSION_CNAME, refresh_schema_status.readable_schema_version_))) {
      LOG_WARN("fail to add column", KR(ret), K(refresh_schema_status));
    } else if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("fail to load for update", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("fail to splice core cells", K(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("fail to replace row", K(ret), K(refresh_schema_status), K(refresh_schema_status));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should update/insert 0 or 1 row", K(ret), K(affected_rows));
    }
  }
  if (trans.is_started()) {
    bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to commit transaction", K(tmp_ret), K(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  const uint64_t tenant_id = refresh_schema_status.tenant_id_;
  ObSchemaStatusUpdater updater(refresh_schema_status);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(schema_status_cache_.atomic_refactored(tenant_id,
                                                            updater))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to set schema_status", K(ret), K(refresh_schema_status));
    } else if (OB_FAIL(schema_status_cache_.set_refactored(tenant_id, refresh_schema_status))) {
      if (OB_HASH_EXIST == ret) {
        LOG_WARN("concurrent set, just ignore", K(ret), K(tenant_id), K(refresh_schema_status));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to set schema_status", K(ret), K(tenant_id), K(refresh_schema_status));
      }
    }
  } else {

    LOG_INFO("[SCHEMA_STATUS] set create status", K(refresh_schema_status));
  }
  return ret;
}

int ObSchemaStatusProxy::del_tenant_schema_status(
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start", K(ret));
  } else {
    ObCoreTableProxy kv(OB_ALL_SCHEMA_STATUS_TNAME, trans, OB_SYS_TENANT_ID);
    if (OB_INVALID_TENANT_ID == tenant_id) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant_id", K(ret), K(tenant_id));
    } else if (OB_FAIL(dml.add_pk_column(TENANT_ID_CNAME, tenant_id))) {
      LOG_WARN("fail to del column", K(ret));
    } else if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("fail to load for update", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("fail to splice core cells", K(ret));
    } else if (OB_FAIL(kv.delete_row(cells, affected_rows))) {
      LOG_WARN("fail to delete row", K(ret), K(tenant_id));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should del 0 or 1 row", K(ret), K(affected_rows));
    }
  }
  if (trans.is_started()) {
    bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to commit transaction", K(tmp_ret), K(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  LOG_INFO("[SCHEMA_STATUS] del schema status", K(ret), K(tenant_id));
  return ret;
}


int ObSchemaStatusProxy::update_schema_status(const ObRefreshSchemaStatus &cur_schema_status)

{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObMySQLTransaction trans;
  if (OB_FAIL(check_inner_stat())) {
    LOG_WARN("check inner stat failed", K(ret));
  } else if (!cur_schema_status.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(cur_schema_status));
  } else if (OB_INVALID_TIMESTAMP == cur_schema_status.snapshot_timestamp_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(cur_schema_status));
  } else if (OB_FAIL(trans.start(&sql_proxy_, OB_SYS_TENANT_ID))) {
    LOG_WARN("fail to start", K(ret));
  } else {
    ObCoreTableProxy kv(OB_ALL_SCHEMA_STATUS_TNAME, trans, OB_SYS_TENANT_ID);
    if (OB_FAIL(dml.add_pk_column(TENANT_ID_CNAME, cur_schema_status.tenant_id_))
        || OB_FAIL(dml.add_column(READABLE_SCHEMA_VERSION_CNAME, cur_schema_status.readable_schema_version_))
        || OB_FAIL(dml.add_column(SNAPSHOT_TIMESTAMP_CNAME, cur_schema_status.snapshot_timestamp_))) {
      LOG_WARN("fail to add column", K(ret), K(cur_schema_status));
    } else if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("fail to load for update", K(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("fail to splice core cells", K(ret));
    } else if (OB_FAIL(kv.incremental_update_row(cells, affected_rows))) {
      LOG_WARN("fail to replace row", K(ret), K(cur_schema_status));
    } else if (affected_rows > 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("should update 0 or 1 row", K(ret), K(affected_rows));
    }
  }
  if (trans.is_started()) {
    bool is_commit = (OB_SUCCESS == ret);
    int tmp_ret = trans.end(is_commit);
    if (OB_SUCCESS != tmp_ret) {
      LOG_WARN("fail to commit transaction", K(tmp_ret), K(ret), K(is_commit));
      if (OB_SUCC(ret)) {
        ret = tmp_ret;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ObSchemaStatusUpdater updater(cur_schema_status);
    if (OB_FAIL(schema_status_cache_.atomic_refactored(cur_schema_status.tenant_id_, updater))) {
      LOG_WARN("fail to update schema_status", K(ret), K(cur_schema_status));
    }
  }
  LOG_INFO("[SCHEMA_STATUS] update schema status", K(ret), K(cur_schema_status));
  return ret;
}
} // end namespace share
} // end namespace oceanbase

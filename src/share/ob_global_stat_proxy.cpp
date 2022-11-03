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

#include "share/ob_global_stat_proxy.h"
#include "share/ob_cluster_version.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/config/ob_server_config.h"
#include "common/ob_timeout_ctx.h"
#include "rootserver/ob_root_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
namespace share
{
const char *ObGlobalStatProxy::TENANT_ID_CNAME = "tenant_id";
int ObGlobalStatProxy::set_init_value(const int64_t core_schema_version,
                                      const int64_t baseline_schema_version,
                                      const int64_t rootservice_epoch,
                                      const int64_t snapshot_gc_scn,
                                      const int64_t gc_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || core_schema_version <= 0 || baseline_schema_version < -1
      || snapshot_gc_scn < 0 || OB_INVALID_ID == rootservice_epoch || gc_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(rootservice_epoch),
        K(core_schema_version), K(baseline_schema_version), K(snapshot_gc_scn), K(gc_schema_version));
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem core_schema_version_item(list, "core_schema_version", core_schema_version);
    ObGlobalStatItem baseline_schema_version_item(list, "baseline_schema_version", baseline_schema_version);
    ObGlobalStatItem rootservice_epoch_item(list, "rootservice_epoch", rootservice_epoch);
    ObGlobalStatItem snapshot_gc_scn_item(list, "snapshot_gc_scn", snapshot_gc_scn);
    ObGlobalStatItem gc_schema_version_item(list, "gc_schema_version", gc_schema_version);

    if (OB_FAIL(update(list))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

int ObGlobalStatProxy::set_tenant_init_global_stat(
    const int64_t core_schema_version,
    const int64_t baseline_schema_version,
    const int64_t snapshot_gc_scn)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || core_schema_version <= 0 || baseline_schema_version < OB_INVALID_VERSION
      || (snapshot_gc_scn < OB_INVALID_TIMESTAMP)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), "self valid", is_valid(),
             K(core_schema_version), K(baseline_schema_version),
             K(snapshot_gc_scn));
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem core_schema_version_item(list, "core_schema_version", core_schema_version);
    ObGlobalStatItem baseline_schema_version_item(list, "baseline_schema_version", baseline_schema_version);
    // only Normal state tenant can refresh snapshot_gc_scn
    ObGlobalStatItem snapshot_gc_scn_item(list, "snapshot_gc_scn", snapshot_gc_scn);
    if (OB_FAIL(update(list))) {
      LOG_WARN("update failed", KR(ret), K(list));
    }
  }
  return ret;
}

#define SET_ITEM(name, value, is_incremental) \
  do { \
    ObGlobalStatItem::ItemList list; \
    ObGlobalStatItem item(list, name, value); \
    if (OB_FAIL(update(list, is_incremental))) { \
      LOG_WARN("update failed", K(list), K(ret)); \
    } \
  } while (false)

int ObGlobalStatProxy::set_core_schema_version(const int64_t core_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || core_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(core_schema_version));
  } else {
    bool is_incremental = true;
    SET_ITEM("core_schema_version", core_schema_version, is_incremental);
  }
  return ret;
}

int ObGlobalStatProxy::set_baseline_schema_version(const int64_t baseline_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || baseline_schema_version < -1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(baseline_schema_version));
  } else {
    bool is_incremental = true;
    SET_ITEM("baseline_schema_version", baseline_schema_version, is_incremental);
  }
  return ret;
}

int ObGlobalStatProxy::set_snapshot_gc_scn(const int64_t snapshot_gc_scn)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || snapshot_gc_scn <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_gc_scn), "self valid", is_valid());
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem snapshot_gc_scn_item(list, "snapshot_gc_scn", snapshot_gc_scn);
    if (OB_FAIL(update(list))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

int ObGlobalStatProxy::set_snapshot_info(const int64_t snapshot_gc_scn,
                                         const int64_t gc_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || snapshot_gc_scn <= 0 || gc_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_gc_scn), K(gc_schema_version), "self valid", is_valid());
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem snapshot_gc_scn_item(list, "snapshot_gc_scn", snapshot_gc_scn);
    ObGlobalStatItem gc_schema_version_item(list, "gc_schema_version", gc_schema_version);
    bool is_incremental = true;
    if (OB_FAIL(update(list, is_incremental))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

int ObGlobalStatProxy::get_snapshot_gc_scn(int64_t &snapshot_gc_scn)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem snapshot_gc_scn_item(list, "snapshot_gc_scn", snapshot_gc_scn);
  ObTimeoutCtx ctx;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    snapshot_gc_scn = snapshot_gc_scn_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::inc_rootservice_epoch()
{
  int ret = OB_SUCCESS;
  int64_t rootservice_epoch = 0;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get_rootservice_epoch(rootservice_epoch))) {
    LOG_WARN("fail to get rootservice_epoch", K(ret), K(rootservice_epoch));
  } else if (rootservice_epoch < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rootservice_epoch is invalid", K(ret), K(rootservice_epoch));
  } else {
    rootservice_epoch++;
    bool is_incremental = true;
    SET_ITEM("rootservice_epoch", rootservice_epoch, is_incremental);
  }
  return ret;
}

#undef SET_ITEM
int ObGlobalStatProxy::get_snapshot_info(int64_t &snapshot_gc_scn,
                                         int64_t &gc_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem snapshot_gc_scn_item(list, "snapshot_gc_scn", snapshot_gc_scn);
  ObGlobalStatItem gc_schema_version_item(list, "gc_schema_version", gc_schema_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    snapshot_gc_scn = snapshot_gc_scn_item.value_;
    gc_schema_version = gc_schema_version_item.value_;
  }
  return ret;

}

int ObGlobalStatProxy::update(const ObGlobalStatItem::ItemList &list,
                              const bool is_incremental)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  if (!is_valid() || list.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(),
        "list size", list.get_size());
  } else if (OB_FAIL(core_table_.load_for_update())) {
    LOG_WARN("core_table_load_for_update failed", K(ret));
  } else {
    const ObGlobalStatItem *it = list.get_first();
    if (NULL == it) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("NULL iterator", K(ret));
    }
    while (OB_SUCCESS == ret && it != list.get_header()) {
      if (OB_FAIL(dml.add_column(it->name_, it->value_))) {
        LOG_WARN("add column failed", K(ret));
      } else {
        it = it->get_next();
        if (NULL == it) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL iterator", K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dml.splice_core_cells(core_table_, cells))) {
    LOG_WARN("splice_core_cells failed", K(ret));
  } else if (!is_incremental && OB_FAIL(core_table_.replace_row(cells, affected_rows))) {
    LOG_WARN("replace_row failed", K(ret));
  } else if (is_incremental && OB_FAIL(core_table_.incremental_replace_row(cells, affected_rows))) {
    LOG_WARN("replace_row failed", K(ret));
  } else if (!is_incremental && !is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected_rows expected to be one", K(ret), K(affected_rows),
        K_(core_table));
  } else if (is_incremental && affected_rows >= 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected row should less than 2", K(ret), K(affected_rows));
  }
  return ret;
}

int ObGlobalStatProxy::get_core_schema_version(int64_t &core_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem core_schema_version_item(list, "core_schema_version", core_schema_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    core_schema_version = core_schema_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_baseline_schema_version(int64_t &baseline_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem baseline_schema_version_item(list, "baseline_schema_version", baseline_schema_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    baseline_schema_version = baseline_schema_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_rootservice_epoch(int64_t &rootservice_epoch)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem rootservice_epoch_item(list, "rootservice_epoch", rootservice_epoch);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    rootservice_epoch = rootservice_epoch_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get(ObGlobalStatItem::ItemList &list)
{
  int ret = OB_SUCCESS;
  ObTimeoutCtx ctx;
  if (!is_valid() || list.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(),
        "list size", list.get_size());
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(core_table_.load())) {
    LOG_WARN("core_table load failed", K(ret));
  } else {
    if (OB_FAIL(core_table_.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_EMPTY_RESULT;
        LOG_WARN("no row exist", K(ret));
      } else {
        LOG_WARN("next failed", K(ret));
      }
    } else {
      ObGlobalStatItem *it = list.get_first();
      if (NULL == it) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("NULL iterator", K(ret));
      }
      while (OB_SUCCESS == ret && it != list.get_header()) {
        if (OB_FAIL(core_table_.get_int(it->name_, it->value_))) {
          LOG_WARN("get int failed", "name", it->name_, K(ret));
        } else {
          it = it->get_next();
          if (NULL == it) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("NULL iterator", K(ret));
          }
        }
      }
      if (OB_SUCC(ret)) {
        ret = core_table_.next();
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        } else if (OB_SUCC(ret)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("__all_global_stat table more than one row", K(ret));
        } else {
          LOG_WARN("next failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObGlobalStatProxy::select_snapshot_gc_scn_for_update(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    int64_t &snapshot_gc_scn)
{
  int ret = OB_SUCCESS;
  snapshot_gc_scn = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = NULL;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt(
                "SELECT column_value FROM %s WHERE TABLE_NAME = '__all_global_stat' AND COLUMN_NAME"
                " = 'snapshot_gc_scn' FOR UPDATE", OB_ALL_CORE_TABLE_TNAME))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ObString snapshot_gc_scn_str;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "column_value", snapshot_gc_scn_str);

      char *endptr = NULL;
      char buf[common::MAX_ZONE_INFO_LENGTH];
      if (OB_SUCC(ret)) {
        const int64_t str_len = snapshot_gc_scn_str.length();
        const int64_t buf_len = sizeof(buf);
        if ((str_len <= 0) || snapshot_gc_scn_str.empty()) {
          ret = OB_INVALID_DATA;
          LOG_WARN("get invalid gc timestamp str", KR(ret), K(str_len), K(snapshot_gc_scn_str));
        } else if (str_len >= buf_len) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("buf is not enough to hold snapshot_gc_scn_str", KR(ret), K(str_len), K(buf_len));
        } else {
          MEMCPY(buf, snapshot_gc_scn_str.ptr(), str_len);
          buf[str_len] = '\0';
          //TODO:SCN strtolu
          snapshot_gc_scn = strtoll(buf, &endptr, 0);
          if ('\0' != *endptr) {
            ret = OB_INVALID_DATA;
            LOG_WARN("invalid data, is not int value", KR(ret), K(snapshot_gc_scn_str), 
              K(snapshot_gc_scn_str.ptr()), K(strlen(snapshot_gc_scn_str.ptr())));
          }
        }
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_FAIL(ret)) {
      //nothing todo
    } else if (OB_ITER_END != (tmp_ret = result->next())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get more row than one", KR(ret), KR(tmp_ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObGlobalStatProxy::update_snapshot_gc_scn(
    common::ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const int64_t snapshot_gc_scn,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  affected_rows = 0;
  if (!is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("UPDATE %s SET column_value = %ld WHERE table_name = '%s' AND "
        "column_name = '%s' AND column_value < %ld", OB_ALL_CORE_TABLE_TNAME, snapshot_gc_scn, 
        "__all_global_stat", "snapshot_gc_scn", snapshot_gc_scn))) {
      LOG_WARN("fail to append sql", KR(ret), K(tenant_id), K(snapshot_gc_scn));
    } else if (OB_FAIL(sql_client.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(sql));
    }
  }
  return ret;
}

}//end namespace share
}//end namespace oceanbase

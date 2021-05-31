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

namespace oceanbase {
using namespace common;
using namespace common::sqlclient;
namespace share {
const char* ObGlobalStatProxy::OB_ALL_GC_SCHEMA_VERSION_TNAME = "__all_gc_schema_version";
const char* ObGlobalStatProxy::TENANT_ID_CNAME = "tenant_id";
const char* ObGlobalStatProxy::GC_SCHEMA_VERSION_CNAME = "gc_schema_version";
int ObGlobalStatProxy::set_init_value(const int64_t core_schema_version, const int64_t baseline_schema_version,
    const int64_t frozen_version, const int64_t rootservice_epoch, const int64_t split_schema_verion,
    const int64_t split_schema_verion_v2, const int64_t snapshot_gc_ts, const int64_t gc_schema_version,
    const int64_t next_schema_version)
{
  int ret = OB_SUCCESS;
  // FIXME: split schema not yet enable, it is not need to check whether valid of split_schema_version
  if (!is_valid() || core_schema_version <= 0 || baseline_schema_version < -1 || snapshot_gc_ts < 0 ||
      frozen_version <= 0 || OB_INVALID_ID == rootservice_epoch || gc_schema_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument",
        K(ret),
        "self valid",
        is_valid(),
        K(rootservice_epoch),
        K(core_schema_version),
        K(baseline_schema_version),
        K(snapshot_gc_ts),
        K(frozen_version),
        K(gc_schema_version),
        K(next_schema_version));
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem core_schema_version_item(list, "core_schema_version", core_schema_version);
    ObGlobalStatItem baseline_schema_version_item(list, "baseline_schema_version", baseline_schema_version);
    ObGlobalStatItem frozen_version_item(list, "frozen_version", frozen_version);
    ObGlobalStatItem rootservice_epoch_item(list, "rootservice_epoch", rootservice_epoch);
    ObGlobalStatItem split_schema_version_item(list, "split_schema_version", split_schema_verion);
    ObGlobalStatItem split_schema_version_v2_item(list, "split_schema_version_v2", split_schema_verion_v2);
    ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
    ObGlobalStatItem gc_schema_version_item(list, "gc_schema_version", gc_schema_version);
    ObGlobalStatItem next_schema_version_item(list, "next_schema_version", next_schema_version);

    if (OB_FAIL(update(list))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

#define SET_ITEM(name, value, is_incremental)     \
  do {                                            \
    ObGlobalStatItem::ItemList list;              \
    ObGlobalStatItem item(list, name, value);     \
    if (OB_FAIL(update(list, is_incremental))) {  \
      LOG_WARN("update failed", K(list), K(ret)); \
    }                                             \
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

int ObGlobalStatProxy::set_frozen_version(const int64_t frozen_version, const bool is_incremental)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || frozen_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(frozen_version));
  } else {
    SET_ITEM("frozen_version", frozen_version, is_incremental);
  }
  return ret;
}

int ObGlobalStatProxy::set_snapshot_gc_ts(const int64_t snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || snapshot_gc_ts <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_gc_ts), "self valid", is_valid());
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
    if (OB_FAIL(update(list))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

int ObGlobalStatProxy::set_gc_schema_versions(
    common::ObISQLClient& sql_client, const ObIArray<TenantIdAndSchemaVersion>& gc_schema_versions)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  int64_t affected_rows = 0;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObCoreTableProxy kv(OB_ALL_GC_SCHEMA_VERSION_TNAME, sql_client);
  if (gc_schema_versions.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(gc_schema_versions));
  } else if (OB_FAIL(kv.load_for_update())) {
    LOG_WARN("fail to load for update", KR(ret));
  }
  for (int64_t i = 0; i < gc_schema_versions.count() && OB_SUCC(ret); i++) {
    const TenantIdAndSchemaVersion& schema_info = gc_schema_versions.at(i);
    cells.reset();
    dml.reset();
    if (OB_FAIL(dml.add_pk_column(TENANT_ID_CNAME, schema_info.tenant_id_)) ||
        OB_FAIL(dml.add_column(GC_SCHEMA_VERSION_CNAME, schema_info.schema_version_))) {
      LOG_WARN("fail to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("fail to splice core cells", KR(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("replace_row failed", K(ret));
    } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expected to be one", K(ret), K(affected_rows), K(kv));
    }
  }  // end for
  return ret;
}

int ObGlobalStatProxy::set_snapshot_info(const int64_t snapshot_gc_ts, const int64_t gc_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || snapshot_gc_ts <= 0 || gc_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_gc_ts), K(gc_schema_version), "self valid", is_valid());
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
    ObGlobalStatItem gc_schema_version_item(list, "gc_schema_version", gc_schema_version);
    bool is_incremental = true;
    if (OB_FAIL(update(list, is_incremental))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

int ObGlobalStatProxy::set_snapshot_info_v2(common::ObISQLClient& sql_proxy, const int64_t snapshot_gc_ts,
    const common::ObIArray<TenantIdAndSchemaVersion>& tenant_schema_versions, const bool set_gc_schema_version)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  int64_t affected_rows = 0;
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
  ObMySQLTransaction trans;
  ObCoreTableProxy kv(OB_ALL_GLOBAL_STAT_TNAME, trans);
  const ObGlobalStatItem* it = list.get_first();
  if (!is_valid() || snapshot_gc_ts <= 0 || (set_gc_schema_version && tenant_schema_versions.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(snapshot_gc_ts), K(tenant_schema_versions), "self valid", is_valid());
  } else if (OB_ISNULL(it)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected", KR(ret), K(it));
  } else if (OB_FAIL(trans.start(&sql_proxy))) {
    LOG_WARN("fail to start transaction", KR(ret));
  } else {
    if (OB_FAIL(kv.load_for_update())) {
      LOG_WARN("fail to load for update", KR(ret));
    } else if (OB_FAIL(dml.add_column(it->name_, it->value_))) {
      LOG_WARN("fail to add column", KR(ret));
    } else if (OB_FAIL(dml.splice_core_cells(kv, cells))) {
      LOG_WARN("fail to splice core cells", KR(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("replace_row failed", K(ret));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("affected_rows expected to be one", K(ret), K(affected_rows), K(kv));
    }
  }
  if (OB_SUCC(ret) && set_gc_schema_version) {
    ObCoreTableProxy kv2(OB_ALL_GC_SCHEMA_VERSION_TNAME, trans);
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_FAIL(kv2.load_for_update())) {
      LOG_WARN("fail to load for update", KR(ret));
    }
    for (int64_t i = 0; i < tenant_schema_versions.count() && OB_SUCC(ret); i++) {
      const TenantIdAndSchemaVersion& schema_info = tenant_schema_versions.at(i);
      cells.reset();
      dml.reset();
      if (OB_FAIL(dml.add_pk_column(TENANT_ID_CNAME, schema_info.tenant_id_)) ||
          OB_FAIL(dml.add_column(GC_SCHEMA_VERSION_CNAME, schema_info.schema_version_))) {
        LOG_WARN("fail to add column", KR(ret));
      } else if (dml.splice_core_cells(kv2, cells)) {
        LOG_WARN("fail to splice core cells", KR(ret));
      } else if (OB_FAIL(kv2.incremental_replace_row(cells, affected_rows))) {
        LOG_WARN("replace_row failed", K(ret));
      } else if (!is_single_row(affected_rows) && !is_zero_row(affected_rows)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("affected_rows expected to be one", K(ret), K(affected_rows), K(kv));
      }
    }  // end for
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

int ObGlobalStatProxy::get_snapshot_gc_ts(int64_t& snapshot_gc_ts)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    snapshot_gc_ts = snapshot_gc_ts_item.value_;
  }
  return ret;
}
// tenant_id = 0 represent getting all tenants`s schema_version of gc
int ObGlobalStatProxy::get_snapshot_info_v2(common::ObISQLClient& sql_proxy, const uint64_t tenant_id,
    const bool need_gc_schema_version, int64_t& snapshot_gc_ts, ObIArray<TenantIdAndSchemaVersion>& gc_schema_versions)
{
  int ret = OB_SUCCESS;
  gc_schema_versions.reset();
  ObMySQLTransaction trans;
  ObTimeoutCtx ctx;
  if (!is_valid()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("inner error", KR(ret));
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
    if (OB_FAIL(get(list))) {
      LOG_WARN("fail to get", KR(ret));
    } else {
      snapshot_gc_ts = snapshot_gc_ts_item.value_;
    }
    ObCoreTableProxy kv(OB_ALL_GC_SCHEMA_VERSION_TNAME, sql_proxy);
    if (OB_FAIL(ret)) {
    } else if (!need_gc_schema_version) {
      // nothing todo
    } else if (OB_FAIL(kv.load())) {
      LOG_WARN("fail to load", KR(ret));
    } else {
      const ObCoreTableProxy::Row* row = NULL;
      int64_t tmp_tenant_id = OB_INVALID_TENANT_ID;
      int64_t gc_schema_version = 0;
      while (OB_SUCC(ret)) {
        tmp_tenant_id = OB_INVALID_TENANT_ID;
        if (OB_FAIL(kv.next())) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("fail to get next", KR(ret));
          }
        } else if (OB_FAIL(kv.get_cur_row(row))) {
          LOG_WARN("fail to get cur row", K(ret));
        } else if (OB_ISNULL(row)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL row", K(ret));
        } else if (OB_FAIL(row->get_int(TENANT_ID_CNAME, tmp_tenant_id))) {
          LOG_WARN("fail to get int", KR(ret));
        } else if (tenant_id != tmp_tenant_id && tenant_id != 0) {
          // nothing todo
        } else if (OB_FAIL(row->get_int(GC_SCHEMA_VERSION_CNAME, gc_schema_version))) {
          LOG_WARN("fail to get int", KR(ret), K(tenant_id), K(tmp_tenant_id));
        } else if (OB_FAIL(gc_schema_versions.push_back(TenantIdAndSchemaVersion(tmp_tenant_id, gc_schema_version)))) {
          LOG_WARN("fail to push back", KR(ret), K(tmp_tenant_id), K(gc_schema_version));
        }
      }  // end while
    }
  }
  if (OB_FAIL(ret) || !need_gc_schema_version) {
  } else if (gc_schema_versions.count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("entry not exist", KR(ret), K(tenant_id));
  }
  return ret;
}

int ObGlobalStatProxy::set_split_schema_version(const int64_t split_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || split_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(split_schema_version));
  } else {
    bool is_incremental = true;
    SET_ITEM("split_schema_version", split_schema_version, is_incremental);
    LOG_INFO("[UPGRADE] set split schema_version", K(ret), K(split_schema_version));
  }
  return ret;
}

int ObGlobalStatProxy::set_split_schema_version_v2(const int64_t split_schema_version_v2)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || split_schema_version_v2 <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(split_schema_version_v2));
  } else {
    bool is_incremental = true;
    SET_ITEM("split_schema_version_v2", split_schema_version_v2, is_incremental);
    LOG_INFO("[UPGRADE] set split schema_version_v2", K(ret), K(split_schema_version_v2));
  }
  return ret;
}

int ObGlobalStatProxy::set_next_schema_version(const int64_t next_schema_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid() || next_schema_version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), K(next_schema_version));
  } else {
    bool is_incremental = true;
    SET_ITEM("next_schema_version", next_schema_version, is_incremental);
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
int ObGlobalStatProxy::get_snapshot_info(int64_t& snapshot_gc_ts, int64_t& gc_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem snapshot_gc_ts_item(list, "snapshot_gc_ts", snapshot_gc_ts);
  ObGlobalStatItem gc_schema_version_item(list, "gc_schema_version", gc_schema_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    snapshot_gc_ts = snapshot_gc_ts_item.value_;
    gc_schema_version = gc_schema_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::update(const ObGlobalStatItem::ItemList& list, const bool is_incremental)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  if (!is_valid() || list.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), "list size", list.get_size());
  } else if (OB_FAIL(core_table_.load_for_update())) {
    LOG_WARN("core_table_load_for_update failed", K(ret));
  } else {
    const ObGlobalStatItem* it = list.get_first();
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
    LOG_WARN("affected_rows expected to be one", K(ret), K(affected_rows), K_(core_table));
  } else if (is_incremental && affected_rows >= 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("affected row should less than 2", K(ret), K(affected_rows));
  }
  return ret;
}

int ObGlobalStatProxy::get_core_schema_version(const int64_t frozen_version, int64_t& core_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem core_schema_version_item(list, "core_schema_version", core_schema_version);
  // frozen_version can be invalid for read the newest value
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list, frozen_version))) {
    LOG_WARN("get failed", K(ret));
  } else {
    core_schema_version = core_schema_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_baseline_schema_version(const int64_t frozen_version, int64_t& baseline_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem baseline_schema_version_item(list, "baseline_schema_version", baseline_schema_version);
  // frozen_version can be invalid for read the newest value
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list, frozen_version))) {
    LOG_WARN("get failed", K(ret));
  } else {
    baseline_schema_version = baseline_schema_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_frozen_info(int64_t& frozen_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem frozen_version_item(list, "frozen_version", frozen_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    frozen_version = frozen_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_split_schema_version(int64_t& split_schema_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem split_schema_version_item(list, "split_schema_version", split_schema_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    split_schema_version = split_schema_version_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_split_schema_version_v2(int64_t& split_schema_version_v2)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem split_schema_version_v2_item(list, "split_schema_version_v2", split_schema_version_v2);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    if (OB_ERR_NULL_VALUE != ret) {
      LOG_WARN("get int value failed", K(ret));
    } else {
      ret = OB_SUCCESS;  // for compatible
      split_schema_version_v2 = OB_INVALID_VERSION;
    }
  } else {
    split_schema_version_v2 = split_schema_version_v2_item.value_;
  }
  return ret;
}

int ObGlobalStatProxy::get_rootservice_epoch(int64_t& rootservice_epoch)
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
int ObGlobalStatProxy::get_next_schema_version(int64_t& next_schema_version)
{
  int ret = OB_SUCCESS;
  next_schema_version = OB_INVALID_VERSION;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem next_schema_version_item(list, "next_schema_version", next_schema_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    if (OB_EMPTY_RESULT == ret) {
      next_schema_version = OB_INVALID_VERSION;
      ret = OB_SUCCESS;
      LOG_INFO("maybe in bootstrap phase", K(ret));
    } else {
      LOG_WARN("get failed", K(ret));
    }
  } else {
    next_schema_version = next_schema_version_item.value_;
  }
  return ret;
}
int ObGlobalStatProxy::get(ObGlobalStatItem::ItemList& list, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  // frozen_version can be invalid for read the newest value
  ObTimeoutCtx ctx;
  if (!is_valid() || list.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid(), "list size", list.get_size());
  } else if (OB_FAIL(rootserver::ObRootUtils::get_rs_default_timeout_ctx(ctx))) {
    LOG_WARN("fail to get timeout ctx", K(ret), K(ctx));
  } else if (OB_FAIL(core_table_.load(frozen_version))) {
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
      ObGlobalStatItem* it = list.get_first();
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

int ObGlobalStatProxy::select_gc_timestamp_for_update(common::ObISQLClient& sql_client, int64_t& gc_timestamp)
{
  int ret = OB_SUCCESS;
  gc_timestamp = 0;
  SMART_VAR(ObMySQLProxy::MySQLResult, res)
  {
    ObMySQLResult* result = NULL;
    ObSqlString sql;
    if (OB_FAIL(sql.assign_fmt("SELECT column_value FROM %s WHERE TABLE_NAME = '__all_global_stat' AND COLUMN_NAME = "
                               "'snapshot_gc_ts' FOR UPDATE",
            OB_ALL_CORE_TABLE_TNAME))) {
      LOG_WARN("assign sql failed", K(ret));
    } else if (OB_FAIL(sql_client.read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get sql result", K(ret));
    } else if (OB_FAIL(result->next())) {
      LOG_WARN("fail to get next row", K(ret));
    } else {
      ObString gc_timestamp_str;
      char* endptr = NULL;
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "column_value", gc_timestamp_str);
      char buf[common::MAX_ZONE_INFO_LENGTH];
      MEMCPY(buf, gc_timestamp_str.ptr(), gc_timestamp_str.length());
      buf[gc_timestamp_str.length()] = '\0';
      if (OB_FAIL(ret)) {
      } else if (gc_timestamp_str.empty()) {
        ret = OB_INVALID_DATA;
        LOG_WARN("get invalid gc timestamp str", K(ret));
      } else {
        gc_timestamp = strtoll(buf, &endptr, 0);
        if ('\0' != *endptr) {
          ret = OB_INVALID_DATA;
          LOG_WARN("invalid data, is not int value",
              K(ret),
              K(gc_timestamp_str),
              K(gc_timestamp_str.ptr()),
              K(strlen(gc_timestamp_str.ptr())));
        }
      }
    }
    if (OB_FAIL(ret)) {
      // nothing todo
    } else if (OB_ITER_END != result->next()) {
      LOG_WARN("get more row than one", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObGlobalStatProxy::set_schema_snapshot_version(const int64_t schema_snapshot_version, const int64_t frozen_version)
{
  int ret = OB_SUCCESS;
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(schema_snapshot_version), "self valid", is_valid());
  } else {
    ObGlobalStatItem::ItemList list;
    ObGlobalStatItem schema_version_item(list, "standby_snapshot_schema_version", schema_snapshot_version);
    ObGlobalStatItem frozen_version_item(list, "standby_snapshot_frozen_version", frozen_version);
    if (OB_FAIL(update(list))) {
      LOG_WARN("update failed", K(list), K(ret));
    }
  }
  return ret;
}

int ObGlobalStatProxy::get_schema_snapshot_version(int64_t& schema_snapshot_version, int64_t& frozen_version)
{
  int ret = OB_SUCCESS;
  ObGlobalStatItem::ItemList list;
  ObGlobalStatItem schema_version_item(list, "standby_snapshot_schema_version", schema_snapshot_version);
  ObGlobalStatItem frozen_version_item(list, "standby_snapshot_frozen_version", frozen_version);
  if (!is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), "self valid", is_valid());
  } else if (OB_FAIL(get(list))) {
    LOG_WARN("get failed", K(ret));
  } else {
    schema_snapshot_version = schema_version_item.value_;
    frozen_version = frozen_version_item.value_;
  }
  return ret;
}
}  // end namespace share
}  // end namespace oceanbase

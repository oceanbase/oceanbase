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

#include "share/tablet/ob_tablet_to_ls_operator.h"
#include "share/ob_errno.h" // KR(ret)
#include "share/inner_table/ob_inner_table_schema.h" // OB_ALL_TABLET_TO_LS_TNAME, OB_ALL_TABLET_TO_LS_TID
#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "lib/string/ob_sql_string.h" // ObSqlString
#include "lib/mysqlclient/ob_mysql_proxy.h" // ObISqlClient, SMART_VAR
#include "observer/ob_sql_client_decorator.h" // ObSQLClientRetryWeak

namespace oceanbase
{
using namespace common;

namespace share
{
int ObTabletToLSTableOperator::range_get_tablet(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTabletID &start_tablet_id,
    const int64_t range_size,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  tablet_ls_pairs.reset();
  if (OB_UNLIKELY(range_size <= 0)) { // do not check start_tablet_id
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(range_size));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSQLClientRetryWeak sql_client_retry_weak(
          &sql_proxy,
          tenant_id,
          OB_ALL_TABLET_TO_LS_TID);
      ObSqlString sql;
      if (FAILEDx(sql.append_fmt(
          "SELECT tablet_id, ls_id FROM %s WHERE tablet_id > %lu ORDER BY tablet_id LIMIT %ld",
          OB_ALL_TABLET_TO_LS_TNAME,
          start_tablet_id.id(),
          range_size))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_client_retry_weak.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_tablet_ls_pairs_(*result.get_result(), tablet_ls_pairs))) {
        LOG_WARN("construct tablet info failed", KR(ret), K(sql), K(tablet_ls_pairs));
      } else if (OB_UNLIKELY(tablet_ls_pairs.count() > range_size)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get too much tablets", KR(ret), K(sql),
            K(range_size), "tablet_ls_pairs count", tablet_ls_pairs.count());
      }
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::construct_tablet_ls_pairs_(
    common::sqlclient::ObMySQLResult &res,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCC(res.next())) {
    int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
    int64_t ls_id = ObLSID::INVALID_LS_ID;
    if (OB_FAIL(res.get_int("tablet_id", tablet_id))) {
      LOG_WARN("fail to get tablet_id from res", KR(ret));
    } else if (OB_FAIL(res.get_int("ls_id", ls_id))) {
      LOG_WARN("fail to get ls_id from res", KR(ret));
    } else if (OB_FAIL(tablet_ls_pairs.push_back(ObTabletLSPair(tablet_id, ls_id)))) {
      LOG_WARN("fail to push back", KR(ret), K(tablet_ls_pairs), K(tablet_id), K(ls_id));
    }
  }
  if (OB_ITER_END != ret) {
    if (OB_UNLIKELY(OB_SUCCESS == ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("fail to get next row to the end", KR(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletToLSTableOperator::batch_get_ls(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  ls_ids.reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, tablet_ids.count());
    while (OB_SUCC(ret) && start_idx < end_idx) {
      if (OB_FAIL(inner_batch_get_ls_by_sql_(
          sql_proxy,
          tenant_id,
          tablet_ids,
          start_idx,
          end_idx,
          ls_ids))) {
        LOG_WARN("fail to inner batch get by sql",
            KR(ret), K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, tablet_ids.count());
      }
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(ls_ids.count() != tablet_ids.count())) {
      ret = OB_ITEM_NOT_MATCH;
      LOG_WARN("count of ls_ids and tablet_ids do not match,"
          " there may be duplicates or nonexistent values in tablet_ids",
          KR(ret), "tablet_ids count", tablet_ids.count(), "ls_ids count", ls_ids.count(),
          K(tenant_id), K(tablet_ids), K(ls_ids));
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_get_ls_by_sql_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(
      OB_INVALID_TENANT_ID == tenant_id
      || tablet_ids.empty()
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSQLClientRetryWeak sql_client_retry_weak(
          &sql_proxy,
          tenant_id,
          OB_ALL_TABLET_TO_LS_TID);
      ObSqlString sql;
      ObSqlString tablet_list;
      for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
        const ObTabletID &tablet_id = tablet_ids.at(idx);
        if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id));
        } else if (OB_FAIL(tablet_list.append_fmt(
            "%s %lu",
            start_idx == idx ? "" : ",",
            tablet_id.id()))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(tablet_id));
        }
      }
      if (FAILEDx(sql.append_fmt(
          "SELECT ls_id FROM %s WHERE tablet_id IN (",
          OB_ALL_TABLET_TO_LS_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql.append(tablet_list.string()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql), K(tablet_list));
      } else if (OB_FAIL(sql.append_fmt(") ORDER BY FIELD(tablet_id, "))) {
        LOG_WARN("assign sql string failed", KR(ret), K(sql));
      } else if (OB_FAIL(sql.append(tablet_list.string()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql), K(tablet_list));
      } else if (OB_FAIL(sql.append_fmt(")"))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_client_retry_weak.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(construct_ls_ids_(*result.get_result(), ls_ids))) {
        LOG_WARN("construct log stream info failed", KR(ret), K(ls_ids));
      }
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::construct_ls_ids_(
    common::sqlclient::ObMySQLResult &res,
    ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCC(res.next())) {
    int64_t ls_id = ObLSID::INVALID_LS_ID;
    if (OB_FAIL(res.get_int("ls_id", ls_id))) {
      LOG_WARN("fail to get uint from res", KR(ret));
    } else if (OB_FAIL(ls_ids.push_back(ObLSID(ls_id)))) {
      LOG_WARN("fail to push back", KR(ret), K(ls_ids), K(ls_id));
    }
  }
  if (OB_ITER_END != ret) {
    if (OB_UNLIKELY(OB_SUCCESS == ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("fail to get next row to the end", KR(ret));
  } else {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObTabletToLSTableOperator::batch_update(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<ObTabletToLSInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(infos));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, infos.count());
    while (OB_SUCC(ret) && start_idx < end_idx) {
      if (OB_FAIL(inner_batch_update_by_sql_(sql_proxy, tenant_id, infos, start_idx, end_idx))) {
        LOG_WARN("fail to inner batch get by sql",
            KR(ret), K(tenant_id), K(infos), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, infos.count());
      }
    }
    if (OB_SUCC(ret)) {
      LOG_TRACE("batch update tablet_to_ls success", K(tenant_id), K(infos));
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_update_by_sql_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<ObTabletToLSInfo> &infos,
    const int64_t start_idx,
    const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || infos.empty()
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(infos), K(start_idx), K(end_idx));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletToLSInfo &info = infos.at(idx);
      if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid TabletToLSInfo", KR(ret), K(info));
      } else if (OB_FAIL(dml_splicer.add_pk_column("tablet_id", info.get_tablet_id().id()))
          || OB_FAIL(dml_splicer.add_column("ls_id", info.get_ls_id().id()))
          || OB_FAIL(dml_splicer.add_column("table_id", info.get_table_id()))) {
        LOG_WARN("fail to add column", KR(ret), K(info));
      } else if (OB_FAIL(dml_splicer.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret));
      }
    }
    if (FAILEDx(dml_splicer.splice_batch_insert_update_sql(OB_ALL_TABLET_TO_LS_TNAME, sql))) {
      LOG_WARN("fail to splice batch insert update sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql),
          K(affected_rows), K(infos), K(start_idx), K(end_idx));
    } else {
      LOG_TRACE("update tablet_to_ls success",
          K(tenant_id), K(affected_rows), K(start_idx), K(end_idx));
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::batch_remove(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = min(MAX_BATCH_COUNT, tablet_ids.count());
    while (OB_SUCC(ret) && start_idx < end_idx) {
      if (OB_FAIL(inner_batch_remove_by_sql_(
          sql_proxy,
          tenant_id,
          tablet_ids,
          start_idx,
          end_idx))) {
        LOG_WARN("fail to inner batch remove by sql",
            KR(ret), K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = min(start_idx + MAX_BATCH_COUNT, tablet_ids.count());
      }
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_remove_by_sql_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(
      OB_INVALID_TENANT_ID == tenant_id
      || tablet_ids.empty()
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > tablet_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret),
        K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx));
  } else if (OB_FAIL(sql.append_fmt(
      "DELETE FROM %s WHERE tablet_id IN (",
      OB_ALL_TABLET_TO_LS_TNAME))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    int64_t affected_rows = 0;
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletID &tablet_id = tablet_ids.at(idx);
      if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id));
      } else if (OB_FAIL(sql.append_fmt("%s %lu", start_idx == idx ? "" : ",", tablet_id.id()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(tablet_id));
      }
    }
    if (FAILEDx(sql.append_fmt(")"))) {
      LOG_WARN("fail to assign sql", KR(ret));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(tenant_id), K(sql), K(affected_rows));
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase

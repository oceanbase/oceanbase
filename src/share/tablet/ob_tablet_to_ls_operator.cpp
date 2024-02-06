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
#define RANGE_GET(sql_proxy, tenant_id, ls_white_list, start_tablet_id, range_size, tablets) \
    do { \
      ObSqlString subsql; \
      ObSqlString sql; \
      if (OB_FAIL(ret)) { \
      } else if (OB_UNLIKELY(range_size <= 0)) { /* do not check start_tablet_id */ \
        ret = OB_INVALID_ARGUMENT; \
        LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(range_size)); \
      } else if (OB_FAIL(construct_ls_white_list_where_sql_(ls_white_list, subsql))) { \
        LOG_WARN("construct sub sql string for LS white list fail", KR(ret), K(ls_white_list)); \
      } else if (OB_FAIL(sql.append_fmt( \
          "SELECT * FROM %s WHERE tablet_id > %lu %s ORDER BY tablet_id LIMIT %ld", \
          OB_ALL_TABLET_TO_LS_TNAME, \
          start_tablet_id.id(), \
          subsql.empty() ? "" : subsql.ptr(), \
          range_size))) { \
        LOG_WARN("fail to assign sql", KR(ret), K(sql), K(subsql), K(start_tablet_id)); \
      } else { \
        SMART_VAR(ObISQLClient::ReadResult, result) { \
          if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) { \
            LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql)); \
          } else if (OB_ISNULL(result.get_result())) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("get mysql result failed", KR(ret), K(sql)); \
          } else if (OB_FAIL(construct_results_(*result.get_result(), tenant_id, tablets))) { \
            LOG_WARN("construct tablet info failed", KR(ret), K(sql), K(tablets)); \
          } else if (OB_UNLIKELY(tablets.count() > range_size)) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("get too much tablets", KR(ret), K(sql), \
                K(range_size), "tablets count", tablets.count()); \
          } \
        } \
      } \
    } while (0)

#define INNER_BATCH_GET(sql_proxy, tenant_id, tablet_ids, start_idx, end_idx, query_column_str, keep_order, results) \
    do { \
      if (OB_FAIL(ret)) { \
      } else if (OB_UNLIKELY( \
          !is_valid_tenant_id(tenant_id) \
          || tablet_ids.empty() \
          || start_idx < 0 \
          || start_idx >= end_idx \
          || end_idx > tablet_ids.count())) { \
        ret = OB_INVALID_ARGUMENT; \
        LOG_WARN("invalid args", KR(ret), K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx)); \
      } else { \
        SMART_VAR(ObISQLClient::ReadResult, result) { \
          ObSQLClientRetryWeak sql_client_retry_weak( \
              &sql_proxy, \
              false,/*did_use_retry*/ \
              tenant_id, \
              OB_ALL_TABLET_TO_LS_TID); \
          ObSqlString sql; \
          ObSqlString tablet_list; \
          for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) { \
            const ObTabletID &tablet_id = tablet_ids.at(idx); \
            if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) { \
              ret = OB_INVALID_ARGUMENT; \
              LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id)); \
            } else if (OB_FAIL(tablet_list.append_fmt( \
                "%s%lu", \
                start_idx == idx ? "" : ",", \
                tablet_id.id()))) { \
              LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(tablet_id)); \
            } \
          } \
          if (FAILEDx(sql.append_fmt( \
              "SELECT %s FROM %s WHERE tablet_id IN (", \
              query_column_str, \
              OB_ALL_TABLET_TO_LS_TNAME))) { \
            LOG_WARN("fail to assign sql", KR(ret), K(sql)); \
          } else if (OB_FAIL(sql.append(tablet_list.string()))) { \
            LOG_WARN("fail to assign sql", KR(ret), K(sql), K(tablet_list)); \
          } \
          if (OB_SUCC(ret) && keep_order) { \
            if (OB_FAIL(sql.append_fmt(") ORDER BY FIELD(tablet_id, "))) { \
              LOG_WARN("assign sql string failed", KR(ret), K(sql)); \
            } else if (OB_FAIL(sql.append(tablet_list.string()))) { \
              LOG_WARN("fail to assign sql", KR(ret), K(sql), K(tablet_list)); \
            } \
          } \
          if (FAILEDx(sql.append_fmt(")"))) { \
            LOG_WARN("fail to assign sql", KR(ret), K(sql)); \
          } else if (OB_FAIL(sql_client_retry_weak.read(result, tenant_id, sql.ptr()))) { \
            LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql)); \
          } else if (OB_ISNULL(result.get_result())) { \
            ret = OB_ERR_UNEXPECTED; \
            LOG_WARN("get mysql result failed", KR(ret)); \
          } else if (OB_FAIL(construct_results_(*result.get_result(), tenant_id, results))) { \
            LOG_WARN("construct log stream info failed", KR(ret), K(results)); \
          } \
        } \
      } \
    } while(0)

#define BATCH_GET(sql_proxy, tenant_id, tablet_ids, results) \
    do { \
      results.reset(); \
      if (OB_FAIL(ret)) { \
      } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.empty())) { \
        ret = OB_INVALID_ARGUMENT; \
        LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids)); \
      } else { \
        int64_t start_idx = 0; \
        int64_t end_idx = min(MAX_BATCH_COUNT, tablet_ids.count()); \
        while (OB_SUCC(ret) && start_idx < end_idx) { \
          if (OB_FAIL(inner_batch_get_( \
              sql_proxy, \
              tenant_id, \
              tablet_ids, \
              start_idx, \
              end_idx, \
              results))) { \
            LOG_WARN("fail to inner batch get by sql", \
                KR(ret), K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx)); \
          } else { \
            start_idx = end_idx; \
            end_idx = min(start_idx + MAX_BATCH_COUNT, tablet_ids.count()); \
          } \
        } \
      } \
    } while(0)

int ObTabletToLSTableOperator::range_get_tablet(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTabletID &start_tablet_id,
    const int64_t range_size,
    common::ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  const ObArray<ObLSID> ls_white_list;
  RANGE_GET(sql_proxy, tenant_id, ls_white_list, start_tablet_id, range_size, tablet_ls_pairs);
  return ret;
}

int ObTabletToLSTableOperator::range_get_tablet_info(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<ObLSID> &ls_white_list,
    const ObTabletID &start_tablet_id,
    const int64_t range_size,
    common::ObIArray<ObTabletToLSInfo> &tablets)
{
  int ret = OB_SUCCESS;
  RANGE_GET(sql_proxy, tenant_id, ls_white_list, start_tablet_id, range_size, tablets);
  return ret;
}

int ObTabletToLSTableOperator::construct_ls_white_list_where_sql_(
    const ObIArray<ObLSID> &ls_white_list,
    ObSqlString &subsql)
{
  int ret = OB_SUCCESS;
  const int64_t ls_cnt = ls_white_list.count();
  if (ls_cnt <= 0) {
    // do nothing
  } else if (OB_FAIL(subsql.assign_fmt(" and ls_id in ("))) {
    LOG_WARN("assign string fail", KR(ret), K(subsql));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_cnt; i++) {
      const ObLSID &ls_id = ls_white_list.at(i);
      const char *str = (i < ls_cnt - 1) ? "," : ")";

      if (OB_FAIL(subsql.append_fmt("%ld%s", ls_id.id(), str))) {
        LOG_WARN("append fmt for sql string fail", KR(ret), K(subsql), K(ls_id), K(str), K(i),
            K(ls_cnt));
      }
    }
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
  BATCH_GET(sql_proxy, tenant_id, tablet_ids, ls_ids);
  if (OB_SUCC(ret) && OB_UNLIKELY(ls_ids.count() != tablet_ids.count())) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("count of ls_ids and tablet_ids do not match,"
        " there may be duplicates or nonexistent values in tablet_ids",
        KR(ret), "tablet_ids count", tablet_ids.count(), "ls_ids count", ls_ids.count(),
        K(tenant_id), K(tablet_ids), K(ls_ids));
  }
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_get_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObLSID> &ls_ids)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "ls_id";
  const bool keep_order = true;
  INNER_BATCH_GET(sql_proxy, tenant_id, tablet_ids, start_idx, end_idx,
      query_column_str, keep_order, ls_ids);
  return ret;
}

int ObTabletToLSTableOperator::construct_results_(
    common::sqlclient::ObMySQLResult &res,
    const uint64_t tenant_id,
    ObIArray<ObLSID> &ls_ids)
{
  UNUSED(tenant_id);
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
  uint64_t data_version = 0;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || infos.empty()
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > infos.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(infos), K(start_idx), K(end_idx));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("fail to get min data version", KR(ret));
  } else {
    ObSqlString sql;
    ObDMLSqlSplicer dml_splicer;
    int64_t affected_rows = 0;
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletToLSInfo &info = infos.at(idx);
      if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid TabletToLSInfo", KR(ret), K(info));
      } else if (data_version < DATA_VERSION_4_2_0_0 && 0 != info.get_transfer_seq()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("update transfer_seq when data_version is less than 4.2.0.0 is not supported", KR(ret), K(info));
      } else if (OB_FAIL(dml_splicer.add_pk_column("tablet_id", info.get_tablet_id().id()))
          || OB_FAIL(dml_splicer.add_column("ls_id", info.get_ls_id().id()))
          || OB_FAIL(dml_splicer.add_column("table_id", info.get_table_id())
          || (data_version >= DATA_VERSION_4_2_0_0
              && OB_FAIL(dml_splicer.add_column("transfer_seq", info.get_transfer_seq()))))) {
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

int ObTabletToLSTableOperator::update_ls_id_and_transfer_seq(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObTabletID &tablet_id,
    const int64_t old_transfer_seq,
    const ObLSID &old_ls_id,
    const int64_t new_transfer_seq,
    const ObLSID &new_ls_id,
    const int32_t group_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || 0 > old_transfer_seq
      || 0 > new_transfer_seq
      || old_transfer_seq == new_transfer_seq
      || !tablet_id.is_valid()
      || !old_ls_id.is_valid()
      || !new_ls_id.is_valid()
      || old_ls_id ==  new_ls_id
      || group_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id),
        K(tablet_id), K(old_transfer_seq), K(new_transfer_seq), K(old_ls_id), K(new_ls_id), K(group_id));
  } else if (OB_FAIL(sql.append_fmt(
      "UPDATE %s SET transfer_seq = %ld, ls_id = %ld "
      "WHERE tablet_id = %lu AND transfer_seq = %ld AND ls_id = %ld",
      OB_ALL_TABLET_TO_LS_TNAME,
      new_transfer_seq,
      new_ls_id.id(),
      tablet_id.id(),
      old_transfer_seq,
      old_ls_id.id()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(sql), K(tenant_id),
        K(tablet_id), K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
  } else {
    int64_t affected_rows = 0;
    if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), group_id, affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql), K(tenant_id));
    } else if (0 == affected_rows) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("no affected rows, the reason might be the tablet is not in the old ls, "
          "or old_transfer_seq does not match the transfer sequence value "
          "in table __all_tablet_to_ls.",
          KR(ret), K(sql), K(tenant_id), K(affected_rows), K(tablet_id),
          K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The error should not occur",  KR(ret), K(sql),
          K(tenant_id), K(affected_rows), K(tablet_id),
          K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
    } else {
      LOG_TRACE("update ls_id and transfer_seq in table __all_tablet_to_ls successfully",
          KR(ret), K(sql), K(tenant_id), K(affected_rows), K(tablet_id),
          K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::batch_get(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    ObIArray<ObTabletToLSInfo> &infos)
{
  int ret = OB_SUCCESS;
  BATCH_GET(sql_proxy, tenant_id, tablet_ids, infos);
  if (OB_SUCC(ret) && OB_UNLIKELY(infos.count() != tablet_ids.count())) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("count of infos and tablet_ids do not match,"
        " there may be duplicates or nonexistent values in tablet_ids",
        KR(ret), "tablet_ids count", tablet_ids.count(), "infos count", infos.count(),
        K(tenant_id), K(tablet_ids), K(infos));
  }
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_get_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObTabletToLSInfo> &infos)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "*";
  const bool keep_order = false;
  INNER_BATCH_GET(sql_proxy, tenant_id, tablet_ids, start_idx, end_idx,
      query_column_str, keep_order, infos);
  return ret;
}

int ObTabletToLSTableOperator::construct_results_(
    common::sqlclient::ObMySQLResult &res,
    const uint64_t tenant_id,
    ObIArray<ObTabletToLSInfo> &infos)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCC(res.next())) {
    int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
    int64_t ls_id = ObLSID::INVALID_LS_ID;
    uint64_t table_id = OB_INVALID_ID;
    int64_t transfer_seq = 0;
    ObTabletToLSInfo info;

    EXTRACT_INT_FIELD_MYSQL(res, "tablet_id", tablet_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "ls_id", ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "table_id", table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(res, "transfer_seq", transfer_seq, int64_t,
        true/*skip_null_error*/, true/*skip_column_error*/, 0/*default value*/);

    if (FAILEDx(info.init(ObTabletID(tablet_id), ObLSID(ls_id), table_id, transfer_seq))) {
      LOG_WARN("init failed", KR(ret), K(tablet_id), K(ls_id), K(table_id), K(transfer_seq));
    } else if (OB_FAIL(infos.push_back(info))) {
      LOG_WARN("fail to push back", KR(ret), K(info));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("construct_results failed", KR(ret), K(infos));
  }
  return ret;
}

int ObTabletToLSTableOperator::construct_results_(
    common::sqlclient::ObMySQLResult &res,
    const uint64_t tenant_id,
    ObIArray<ObTabletLSPair> &pairs)
{
  UNUSED(tenant_id);
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCC(res.next())) {
    int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
    int64_t ls_id = ObLSID::INVALID_LS_ID;

    EXTRACT_INT_FIELD_MYSQL(res, "tablet_id", tablet_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "ls_id", ls_id, int64_t);

    if (FAILEDx(pairs.push_back(ObTabletLSPair(ObTabletID(tablet_id), ObLSID(ls_id))))) {
      LOG_WARN("fail to push back", KR(ret), K(tablet_id), K(ls_id));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("construct results failed", KR(ret), K(pairs));
  }
  return ret;
}

int ObTabletToLSTableOperator::get_ls_by_tablet(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const common::ObTabletID &tablet_id,
    ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  ObSEArray<ObTabletID, 1> tablet_ids;
  ObSEArray<ObLSID, 1> ls_ids;
  if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
    LOG_WARN("push back failed", KR(ret), K(tablet_id));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = 1;
    if (OB_FAIL(inner_batch_get_(
        sql_proxy,
        tenant_id,
        tablet_ids,
        start_idx,
        end_idx,
        ls_ids))) {
      LOG_WARN("fail to inner batch get by sql",
          KR(ret), K(tenant_id), K(tablet_ids), K(start_idx), K(end_idx));
    } else if (ls_ids.empty()) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("tablet not found", KR(ret), K(tenant_id), K(tablet_id));
    } else if (1 != ls_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get too much ls_ids", KR(ret), K(tenant_id), K(tablet_id), K(ls_ids));
    } else {
      ls_id = ls_ids.at(0);
    }
  }
  return ret;
}

int ObTabletToLSTableOperator::batch_get_tablet_ls_cache(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const common::ObIArray<common::ObTabletID> &tablet_ids,
    common::ObIArray<ObTabletLSCache> &tablet_ls_caches)
{
  int ret = OB_SUCCESS;
  BATCH_GET(sql_proxy, tenant_id, tablet_ids, tablet_ls_caches);
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_get_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    common::ObIArray<ObTabletLSCache> &tablet_ls_caches)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "tablet_id, ls_id, ORA_ROWSCN";
  const bool keep_order = false;
  INNER_BATCH_GET(sql_proxy, tenant_id, tablet_ids, start_idx, end_idx,
      query_column_str, keep_order, tablet_ls_caches);
  return ret;
}

int ObTabletToLSTableOperator::construct_results_(
    common::sqlclient::ObMySQLResult &res,
    const uint64_t tenant_id,
    common::ObIArray<ObTabletLSCache> &tablet_ls_caches)
{
  int ret = OB_SUCCESS;
  ObTabletLSCache tablet_ls_cache;
  while (OB_SUCC(ret) && OB_SUCC(res.next())) {
    tablet_ls_cache.reset();
    uint64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
    int64_t ls_id = ObLSID::INVALID_LS_ID;
    int64_t row_scn = OB_MIN_SCN_TS_NS;
    EXTRACT_INT_FIELD_MYSQL(res, "tablet_id", tablet_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "ls_id", ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(res, "ORA_ROWSCN", row_scn, int64_t);
    const int64_t now = ObTimeUtility::fast_current_time();
    if (FAILEDx(tablet_ls_cache.init(
        tenant_id,
        ObTabletID(tablet_id),
        ObLSID(ls_id),
        now,
        row_scn))) {
      LOG_WARN("init tablet_ls_cache failed", KR(ret), K(tenant_id),
          K(tablet_id), K(ls_id), K(now), K(row_scn));
    } else if (OB_FAIL(tablet_ls_caches.push_back(tablet_ls_cache))) {
      LOG_WARN("fail to push back", KR(ret), K(tablet_ls_cache));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    if (OB_SUCC(ret)) {
      ret = OB_ERR_UNEXPECTED;
    }
    LOG_WARN("construct results failed", KR(ret), K(tablet_ls_caches));
  }
  return ret;
}

int ObTabletToLSTableOperator::batch_get_tablet_ls_pairs(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  BATCH_GET(sql_proxy, tenant_id, tablet_ids, tablet_ls_pairs);
  if (OB_SUCC(ret) && OB_UNLIKELY(tablet_ls_pairs.count() != tablet_ids.count())) {
    ret = OB_ITEM_NOT_MATCH;
    LOG_WARN("count of tablet_ls_pairs and tablet_ids do not match,"
        " there may be duplicates or nonexistent values in tablet_ids",
        KR(ret), "tablet_ids count", tablet_ids.count(), "tablet_ls_pairs count",
        tablet_ls_pairs.count(), K(tenant_id), K(tablet_ids), K(tablet_ls_pairs));
  }
  return ret;
}

int ObTabletToLSTableOperator::inner_batch_get_(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<ObTabletLSPair> &tablet_ls_pairs)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "tablet_id, ls_id";
  const bool keep_order = false;
  INNER_BATCH_GET(sql_proxy, tenant_id, tablet_ids, start_idx, end_idx,
      query_column_str, keep_order, tablet_ls_pairs);
  return ret;
}

} // end namespace share
} // end namespace oceanbase

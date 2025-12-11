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

#include "src/share/inner_table/ob_inner_table_schema_constants.h"
#include "storage/tablet/ob_tablet_to_global_temporary_table_operator.h"
#include "share/ob_dml_sql_splicer.h" // ObDMLSqlSplicer
#include "observer/ob_sql_client_decorator.h" // ObSQLClientRetryWeak
#include "storage/tablet/ob_session_tablet_info_map.h"

namespace oceanbase
{

using namespace common;

namespace share
{

int ObTabletToGlobalTmpTableOperator::batch_insert(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<storage::ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || infos.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(infos));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = MIN(MAX_BATCH_COUNT, infos.count());
    while (OB_SUCC(ret) && start_idx < end_idx) {
      if (OB_FAIL(inner_batch_insert_by_sql(sql_proxy, tenant_id, infos, start_idx, end_idx))) {
        LOG_WARN("fail to inner batch insert by sql", KR(ret), K(tenant_id), K(infos), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = MIN(start_idx + MAX_BATCH_COUNT, infos.count());
      }
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::batch_remove(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids));
  } else {
    int64_t start_idx = 0;
    int64_t end_idx = MIN(MAX_BATCH_COUNT, tablet_ids.count());
    while (OB_SUCC(ret) && start_idx < end_idx) {
      if (OB_FAIL(inner_batch_remove_by_sql(sql_proxy, tenant_id, tablet_ids, start_idx, end_idx))) {
        LOG_WARN("fail to inner batch remove by sql", KR(ret), K(tenant_id), K(tablet_ids),
            K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = MIN(start_idx + MAX_BATCH_COUNT, tablet_ids.count());
      }
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::batch_get(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    ObIArray<storage::ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const char *query_column_str = "*";
    if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(infos));
    } else {
      SMART_VAR(ObISQLClient::ReadResult, result) {
        ObSQLClientRetryWeak sql_client_retry_weak(
            &sql_proxy,
            false,/*did_use_retry*/
            tenant_id,
            OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TID);
        ObSqlString sql;

        if (FAILEDx(sql.append_fmt(
            "SELECT %s FROM %s",
            query_column_str,
            OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME))) {
          LOG_WARN("fail to assign sql", KR(ret), K(sql));
        }

        if (OB_FAIL(sql_client_retry_weak.read(result, tenant_id, sql.ptr()))) {
          LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get mysql result failed", KR(ret));
        } else {
          int64_t row_count = 0;
          result.get_result()->get_int("cnt", row_count);
          infos.reserve(row_count);
          if (OB_FAIL(construct_infos(*result.get_result(), infos))) {
            LOG_WARN("construct log stream info failed", KR(ret), K(infos));
          }
        }
      }
    }
    return ret;
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::inner_batch_insert_by_sql(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<storage::ObSessionTabletInfo> &infos,
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
      const storage::ObSessionTabletInfo &info = infos.at(idx);
      if (OB_UNLIKELY(!info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid ObSessionTabletInfo", KR(ret), K(info));
      }else if (data_version < DATA_VERSION_4_2_0_0 && 0 != info.get_transfer_seq()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("update transfer_seq when data_version is less than 4.2.0.0 is not supported", KR(ret), K(info));
      } else if (OB_FAIL(dml_splicer.add_pk_column("tablet_id", info.get_tablet_id().id()))
          || OB_FAIL(dml_splicer.add_column("ls_id", info.get_ls_id().id()))
          || OB_FAIL(dml_splicer.add_column("table_id", info.get_table_id()))
          || OB_FAIL(dml_splicer.add_column("sequence", info.get_sequence()))
          || OB_FAIL(dml_splicer.add_column("session_id", info.get_session_id()))
          || (data_version >= DATA_VERSION_4_2_0_0
             &&OB_FAIL(dml_splicer.add_column("transfer_seq", info.get_transfer_seq())))) {
        LOG_WARN("fail to add column", KR(ret), K(info));
      } else if (OB_FAIL(dml_splicer.finish_row())) {
        LOG_WARN("fail to finish row", KR(ret));
      }
    }
    if (FAILEDx(dml_splicer.splice_batch_insert_update_sql(OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME, sql))) {
      LOG_WARN("fail to splice batch insert sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql), K(affected_rows), K(infos), K(start_idx), K(end_idx));
    } else {
      LOG_TRACE("batch insert session tablet to temporary table success",
          KR(ret), K(tenant_id), K(affected_rows), K(infos), K(start_idx), K(end_idx));
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::inner_batch_remove_by_sql(
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
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tablet_id IN (",
      OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME))) {
    LOG_WARN("fail to assign sql", KR(ret));
  } else {
    int64_t affected_rows = 0;
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const uint64_t tablet_id = tablet_ids.at(idx).id();
      if (OB_UNLIKELY(OB_INVALID_ID == tablet_id)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet_id", KR(ret), K(tenant_id), K(tablet_id));
      } else if (OB_FAIL(sql.append_fmt("%s %lu", start_idx == idx ? "" : ",", tablet_id))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(tablet_id));
      }
    }
    if (FAILEDx(sql.append_fmt(")"))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql));
    } else if (OB_FAIL(sql_proxy.write(tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql), K(affected_rows), K(tablet_ids), K(start_idx), K(end_idx));
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::point_get(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const int64_t sequence,
    storage::ObSessionTabletInfo &info)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "*";
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || table_id == OB_INVALID_ID
      || sequence == INT64_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id), K(sequence));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      common::ObSEArray<storage::ObSessionTabletInfo, 1> infos;
      if (FAILEDx(sql.append_fmt(
          "SELECT %s FROM %s WHERE table_id = %lu AND sequence = %ld",
          query_column_str,
          OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME,
          table_id,
          sequence))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_infos(*result.get_result(), infos))) {
        LOG_WARN("construct log stream info failed", KR(ret), K(infos));
      }
      if (OB_SUCC(ret)) {
        if (infos.empty()) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("session tablet not exist", KR(ret), K(tenant_id), K(table_id), K(sequence), K(sql));
        } else if (1 == infos.count()) {
          info = infos.at(0);
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("session tablet should be one", KR(ret), K(tenant_id), K(table_id), K(sequence), K(sql));
        }
      }
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::construct_infos(
    common::sqlclient::ObMySQLResult &result,
    ObIArray<storage::ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret) && OB_SUCC(result.next())) {
    storage::ObSessionTabletInfo info;
    int64_t tablet_id = ObTabletID::INVALID_TABLET_ID;
    int64_t ls_id = ObLSID::INVALID_LS_ID;
    uint64_t table_id = OB_INVALID_ID;
    int64_t sequence = INT64_MAX;
    uint32_t session_id = UINT32_MAX;
    int64_t transfer_seq = 0;

    EXTRACT_INT_FIELD_MYSQL(result, "tablet_id", tablet_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "ls_id", ls_id, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "table_id", table_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "sequence", sequence, int64_t);
    EXTRACT_INT_FIELD_MYSQL(result, "session_id", session_id, uint32_t);
    EXTRACT_INT_FIELD_MYSQL_WITH_DEFAULT_VALUE(result, "transfer_seq", transfer_seq, int64_t,
      true/*skip_null_error*/, true/*skip_column_error*/, 0/*default value*/);

    if (FAILEDx(info.init(ObTabletID(tablet_id), ObLSID(ls_id), table_id, sequence,
        session_id, transfer_seq))) {
      LOG_WARN("fail to init info", KR(ret), K(tablet_id), K(ls_id), K(table_id), K(sequence), K(session_id), K(transfer_seq));
    } else if (OB_FAIL(infos.push_back(info))) {
      LOG_WARN("fail to push back info", KR(ret), K(info));
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

int ObTabletToGlobalTmpTableOperator::update_ls_id_and_transfer_seq(
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
      OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME,
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
          "in table __all_tablet_to_global_temporary_table.",
          KR(ret), K(sql), K(tenant_id), K(affected_rows), K(tablet_id),
          K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
    } else if (OB_UNLIKELY(1 != affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("The error should not occur",  KR(ret), K(sql),
          K(tenant_id), K(affected_rows), K(tablet_id),
          K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
    } else {
      LOG_TRACE("update ls_id and transfer_seq in table __all_tablet_to_global_temporary_table successfully",
          KR(ret), K(sql), K(tenant_id), K(affected_rows), K(tablet_id),
          K(old_ls_id), K(new_ls_id), K(old_transfer_seq), K(new_transfer_seq));
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::batch_get_by_table_ids(
    ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    ObIArray<storage::ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || table_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_ids));
  } else {
    infos.reset();
    infos.reserve(table_ids.count());
    int64_t start_idx = 0;
    int64_t end_idx = MIN(MAX_BATCH_COUNT, table_ids.count());
    while (OB_SUCC(ret) && start_idx < end_idx) {
      if (OB_FAIL(inner_batch_get_by_sql(sql_proxy, tenant_id, table_ids, start_idx, end_idx, infos))) {
        LOG_WARN("fail to inner batch get by sql", KR(ret), K(tenant_id), K(table_ids), K(start_idx), K(end_idx));
      } else {
        start_idx = end_idx;
        end_idx = MIN(start_idx + MAX_BATCH_COUNT, table_ids.count());
      }
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::inner_batch_get_by_sql(
    common::ObISQLClient &sql_proxy,
    const uint64_t tenant_id,
    const ObIArray<common::ObTableID> &table_ids,
    const int64_t start_idx,
    const int64_t end_idx,
    ObIArray<storage::ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  const char *query_column_str = "*";
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
      || table_ids.empty()
      || start_idx < 0
      || start_idx >= end_idx
      || end_idx > table_ids.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_ids), K(infos), K(start_idx), K(end_idx));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSQLClientRetryWeak sql_client_retry_weak(
          &sql_proxy,
          false,/*did_use_retry*/
          tenant_id,
          OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TID);
      ObSqlString sql;
      ObSqlString table_id_list;
      for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
        const uint64_t table_id = table_ids.at(idx);
        if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("invalid table_id", KR(ret), K(tenant_id), K(table_id));
        } else if (OB_FAIL(table_id_list.append_fmt(
            "%s%lu",
            start_idx == idx ? "" : ",",
            table_id))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(table_id));
        }
      }
      if (FAILEDx(sql.append_fmt(
          "SELECT %s FROM %s WHERE table_id IN (",
          query_column_str,
          OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql.append(table_id_list.string()))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql), K(table_id_list));
      }

      if (FAILEDx(sql.append_fmt(")"))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_client_retry_weak.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret));
      } else if (OB_FAIL(construct_infos(*result.get_result(), infos))) {
        LOG_WARN("construct log stream info failed", KR(ret), K(infos));
      }
    }
  }
  return ret;
}

int ObTabletToGlobalTmpTableOperator::get_by_table_id(
  common::ObISQLClient &sql_proxy,
  const uint64_t tenant_id,
  const common::ObTableID &table_id,
  ObIArray<storage::ObSessionTabletInfo> &infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id || table_id == OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(table_id));
  } else {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      ObSqlString sql;
      if (FAILEDx(sql.append_fmt(
          "SELECT * FROM %s WHERE table_id = %lu",
          OB_ALL_TABLET_TO_GLOBAL_TEMPORARY_TABLE_TNAME, table_id))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(sql_proxy.read(result, tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(construct_infos(*result.get_result(), infos))) {
        LOG_WARN("construct log stream info failed", KR(ret), K(infos));
      }
      if (OB_SUCC(ret)) {
        if (infos.empty()) {
          ret = OB_ENTRY_NOT_EXIST;
          LOG_WARN("session tablet not exist", KR(ret), K(tenant_id), K(table_id), K(sql));
        }
      }
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase


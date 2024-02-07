/**
 * Copyright (c) 2023 OceanBase
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
#include "ob_tablet_meta_table_compaction_operator.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/oblog/ob_log.h"
#include "lib/string/ob_sql_string.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/tablet/ob_tablet_filter.h"
#include "share/ob_service_epoch_proxy.h"
#include "share/scn.h"
#include "observer/ob_server_struct.h"
#include "share/tablet/ob_tablet_table_operator.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

int ObTabletMetaTableCompactionOperator::batch_set_info_status(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &tablet_ls_pairs,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  int64_t pairs_count = tablet_ls_pairs.count();
  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, pairs_count);
  while (OB_SUCC(ret) && (start_idx < end_idx)) {
    if (OB_FAIL(inner_batch_set_info_status_(
        tenant_id,
        tablet_ls_pairs,
        start_idx,
        end_idx,
        affected_rows))) {
      LOG_WARN("fail to inner batch set by sql",
          KR(ret), K(tenant_id), K(tablet_ls_pairs), K(start_idx), K(end_idx));
    } else {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, pairs_count);
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::inner_batch_set_info_status_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletLSPair> &tablet_ls_pairs,
    const int64_t start_idx,
    const int64_t end_idx,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_FAIL(sql.append_fmt(
      "UPDATE %s SET status = '%ld' WHERE tenant_id = %ld AND (tablet_id,ls_id) IN ((",
      OB_ALL_TABLET_META_TABLE_TNAME,
      (int64_t)ObTabletReplica::ScnStatus::SCN_STATUS_ERROR,
      tenant_id))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id));
  } else {
    for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
      const ObTabletID &tablet_id = tablet_ls_pairs.at(idx).get_tablet_id();
      const ObLSID &ls_id = tablet_ls_pairs.at(idx).get_ls_id();
      if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id)
          || !ls_id.is_valid_with_tenant(tenant_id))) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id), K(ls_id));
      } else if (OB_FAIL(sql.append_fmt(
          "'%lu', %ld%s",
          tablet_id.id(),
          ls_id.id(),
          ((idx == end_idx - 1) ? "))" : "), (")))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
      }
    }
  }
  int64_t tmp_affected_rows = 0;
  if (FAILEDx(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(), tmp_affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  } else if (tmp_affected_rows > 0) {
    affected_rows += tmp_affected_rows;
    LOG_INFO("success to update checksum error status", K(ret), K(sql), K(tenant_id), K(tablet_ls_pairs), K(tmp_affected_rows));
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::get_status(
    const ObTabletCompactionScnInfo &input_info,
    ObTabletCompactionScnInfo &ret_info)
{
  int ret = OB_SUCCESS;
  ret_info.reset();
  ObISQLClient *sql_client = GCTX.sql_proxy_;
  if (OB_UNLIKELY(!input_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(input_info));
  } else if (OB_ISNULL(sql_client)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql client is null", K(ret), KP(sql_client));
  } else if (OB_FAIL(do_select(*sql_client, false/*select_for_update*/, input_info, ret_info))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_WARN("failed to select from tablet compaction scn tablet", KR(ret), K(input_info));
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::do_select(
    ObISQLClient &sql_client,
    const bool select_with_update,
    const ObTabletCompactionScnInfo &input_info,
    ObTabletCompactionScnInfo &ret_info)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(input_info.tenant_id_);
  ret_info = input_info; // assign tenant_id / ls_id / tablet_id

  if (OB_FAIL(sql.append_fmt(
      "SELECT max(report_scn) as report_scn, max(status) as status"
      " FROM %s WHERE tenant_id = '%lu' AND ls_id = '%ld' AND tablet_id = '%ld'%s",
          OB_ALL_TABLET_META_TABLE_TNAME,
          input_info.tenant_id_,
          input_info.ls_id_,
          input_info.tablet_id_,
          select_with_update ? " FOR UPDATE" : ""))) {
    LOG_WARN("failed to append fmt", K(ret), K(input_info));
  } else {
    ret = execute_select_sql(sql_client, meta_tenant_id, sql, ret_info);
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::execute_select_sql(
    ObISQLClient &sql_client,
    const int64_t meta_tenant_id,
    const ObSqlString &sql,
    ObTabletCompactionScnInfo &ret_info)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    ObMySQLResult *result = nullptr;
    if (OB_FAIL(sql_client.read(res, meta_tenant_id, sql.ptr()))) {
      LOG_WARN("fail to do read", KR(ret), K(meta_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get result", KR(ret), K(meta_tenant_id), K(sql));
    } else if (OB_FAIL(construct_compaction_related_info(*result, ret_info))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("fail to get medium snapshot info", KR(ret), KP(result), K(sql));
      }
    } else {
      LOG_TRACE("success to get medium snapshot info", K(ret_info));
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::batch_update_unequal_report_scn_tablet(
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      const int64_t major_frozen_scn,
      const common::ObIArray<ObTabletID> &input_tablet_id_array)
{
  int ret = OB_SUCCESS;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t start_idx = 0;
  int64_t end_idx = min(MAX_BATCH_COUNT, input_tablet_id_array.count());
  common::ObSEArray<ObTabletID, 32> unequal_tablet_id_array;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy is unexpected null", K(ret));
  } else {
    LOG_INFO("start to update unequal tablet id array", KR(ret), K(ls_id), K(major_frozen_scn),
      "input_tablet_id_array_cnt", input_tablet_id_array.count());
  }
  while (OB_SUCC(ret) && (start_idx < end_idx)) {
    ObSqlString sql;
    if (OB_FAIL(sql.append_fmt(
        "select distinct(tablet_id) from %s where tenant_id = '%lu' AND ls_id = '%ld'"
          " AND tablet_id IN (",
            OB_ALL_TABLET_META_TABLE_TNAME,
            tenant_id,
            ls_id.id()))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(start_idx));
    } else if (OB_FAIL(append_tablet_id_array(tenant_id, input_tablet_id_array, start_idx, end_idx, sql))) {
      LOG_WARN("fail to append tablet id array", KR(ret), K(tenant_id),
        K(input_tablet_id_array.count()), K(start_idx), K(end_idx));
    } else if (OB_FAIL(sql.append_fmt(") AND report_scn < '%lu'", major_frozen_scn))) {
      LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(start_idx));
    } else {
      SMART_VAR(ObISQLClient::ReadResult, result) {
        if (OB_FAIL(GCTX.sql_proxy_->read(result, meta_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), "sql", sql.ptr());
        } else if (OB_ISNULL(result.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get mysql result", KR(ret), "sql", sql.ptr());
        } else if (OB_FAIL(construct_tablet_id_array(*result.get_result(), unequal_tablet_id_array))) {
          LOG_WARN("fail to construct tablet id array", KR(ret), "sql", sql.ptr());
        } else if (unequal_tablet_id_array.count() > 0) {
          LOG_TRACE("success to get uneuqal tablet_id array", K(ret), K(unequal_tablet_id_array));
        }
      }
      if (OB_FAIL(ret) || unequal_tablet_id_array.empty()) {
      } else if (unequal_tablet_id_array.count() < MAX_BATCH_COUNT
        && end_idx != input_tablet_id_array.count()) { // before last round, check count
        // do nothing
      } else if (OB_FAIL(inner_batch_update_unequal_report_scn_tablet(
              tenant_id,
              ls_id,
              major_frozen_scn,
              unequal_tablet_id_array))) {
        LOG_WARN("fail to update unequal tablet id array", KR(ret), "sql", sql.ptr());
      } else {
        unequal_tablet_id_array.reuse();
      }
    }
    if (OB_SUCC(ret)) {
      start_idx = end_idx;
      end_idx = min(start_idx + MAX_BATCH_COUNT, input_tablet_id_array.count());
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::get_min_compaction_scn(
    const uint64_t tenant_id,
    SCN &min_compaction_scn)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtil::current_time();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    int64_t estimated_timeout_us = 0;
    ObTimeoutCtx timeout_ctx;
    // set trx_timeout and query_timeout based on tablet_replica_cnt
    if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_estimated_timeout_us(tenant_id,
                                                     estimated_timeout_us))) {
      LOG_WARN("fail to get estimated_timeout_us", KR(ret), K(tenant_id));
    } else if (OB_FAIL(timeout_ctx.set_trx_timeout_us(estimated_timeout_us))) {
      LOG_WARN("fail to set trx timeout", KR(ret), K(estimated_timeout_us));
    } else if (OB_FAIL(timeout_ctx.set_timeout(estimated_timeout_us))) {
      LOG_WARN("fail to set abs timeout", KR(ret), K(estimated_timeout_us));
    } else {
      ObSqlString sql;
      SMART_VAR(ObISQLClient::ReadResult, res) {
        ObMySQLResult *result = nullptr;
        if (OB_FAIL(sql.assign_fmt("SELECT MIN(compaction_scn) as value FROM %s WHERE tenant_id ="
                                   " '%ld' ", OB_ALL_TABLET_META_TABLE_TNAME, tenant_id))) {
          LOG_WARN("failed to append fmt", K(ret), K(tenant_id));
        } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
          LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
        } else {
          uint64_t min_compaction_scn_val = UINT64_MAX;
          EXTRACT_UINT_FIELD_MYSQL(*result, "value", min_compaction_scn_val, uint64_t);
          if (FAILEDx(min_compaction_scn.convert_for_inner_table_field(min_compaction_scn_val))) {
            LOG_WARN("fail to convert uint64_t to SCN", KR(ret), K(min_compaction_scn_val));
          }
        }
      }
    }
    LOG_INFO("finish to get min_compaction_scn", KR(ret), K(tenant_id), K(min_compaction_scn),
             "cost_time_us", ObTimeUtil::current_time() - start_time_us, K(estimated_timeout_us));
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::construct_tablet_id_array(
    sqlclient::ObMySQLResult &result,
    common::ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  int64_t tablet_id = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(result.next())) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get next result", KR(ret));
      }
      break;
    } else if (OB_FAIL(result.get_int("tablet_id", tablet_id))) {
      LOG_WARN("fail to get uint", KR(ret));
    } else if (OB_FAIL(tablet_id_array.push_back(ObTabletID(tablet_id)))) {
      LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::append_tablet_id_array(
    const uint64_t tenant_id,
    const common::ObIArray<ObTabletID> &input_tablet_id_array,
    const int64_t start_idx,
    const int64_t end_idx,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  for (int64_t idx = start_idx; OB_SUCC(ret) && (idx < end_idx); ++idx) {
    const ObTabletID &tablet_id = input_tablet_id_array.at(idx);
    if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id));
    } else if (OB_FAIL(sql.append_fmt(
        "%s %ld",
        start_idx == idx ? "" : ",",
        tablet_id.id()))) {
      LOG_WARN("fail to assign sql", KR(ret), K(idx), K(tablet_id));
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::inner_batch_update_unequal_report_scn_tablet(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    const int64_t major_frozen_scn,
    const common::ObIArray<ObTabletID> &unequal_tablet_id_array)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_COMPACTION_UPDATE_REPORT_SCN) ret;
  if (OB_FAIL(ret)) {
    LOG_INFO("ERRSIM EN_COMPACTION_UPDATE_REPORT_SCN", K(ret));
  }
#endif
  if (FAILEDx(sql.append_fmt(
          "UPDATE %s t1 SET report_scn=if(compaction_scn>'%lu' ,'%lu', compaction_scn) WHERE "
          "tenant_id='%lu' AND ls_id='%ld' AND tablet_id IN (",
          OB_ALL_TABLET_META_TABLE_TNAME, major_frozen_scn, major_frozen_scn,
          tenant_id, ls_id.id()))) {
    LOG_WARN("failed to append fmt", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(append_tablet_id_array(tenant_id, unequal_tablet_id_array,
                                            0, unequal_tablet_id_array.count(),
                                            sql))) {
    LOG_WARN("fail to append tablet id array", KR(ret), K(tenant_id), K(unequal_tablet_id_array));
  } else if (OB_FAIL(sql.append_fmt(") AND report_scn <'%lu'", major_frozen_scn))) {
    LOG_WARN("failed to assign sql", K(ret), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(GCTX.sql_proxy_->write(meta_tenant_id, sql.ptr(),
                                            affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
  } else if (affected_rows > 0) {
    LOG_INFO("success to update unequal report_scn", K(ret), K(sql), K(tenant_id), K(ls_id), K(unequal_tablet_id_array.count()));
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::construct_compaction_related_info(
    sqlclient::ObMySQLResult &result,
    ObTabletCompactionScnInfo &info)
{
  int ret = OB_SUCCESS;
  uint64_t report_scn_in_table = 0;
  int64_t status = 0;
  if (OB_FAIL(result.get_uint("report_scn", report_scn_in_table))) {
    if (OB_ERR_NULL_VALUE == ret) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      LOG_WARN("failed to get int", KR(ret), K(info));
    }
  } else if (OB_FAIL(result.get_int("status", status))) {
    LOG_WARN("failed to get int", KR(ret), K(status));
  } else if (OB_UNLIKELY(!ObTabletReplica::is_status_valid((ObTabletReplica::ScnStatus)status))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("status is invalid", KR(ret), K(status));
  } else {
    info.report_scn_ = (int64_t)report_scn_in_table;
    info.status_ = ObTabletReplica::ScnStatus(status);
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::batch_update_report_scn(
    const uint64_t tenant_id,
    const uint64_t global_broadcast_scn_val,
    const ObTabletReplica::ScnStatus &except_status,
    const volatile bool &stop,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtil::current_time();
  const int64_t BATCH_UPDATE_CNT = 1000;
  uint64_t compat_version = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing until schema upgrade
  } else {
    LOG_INFO("start to batch update report scn", KR(ret), K(tenant_id), K(global_broadcast_scn_val), K(expected_epoch));
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    bool update_done = false;
    SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
      while (OB_SUCC(ret) && !update_done && !stop) {
        bool is_match = true;
        ObMySQLTransaction trans;
        ObSqlString sql;
        int64_t affected_rows = 0;
        if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_next_batch_tablet_ids(tenant_id,
                    BATCH_UPDATE_CNT, tablet_ids))) {
          LOG_WARN("fail to get next batch of tablet_ids", KR(ret), K(tenant_id), K(BATCH_UPDATE_CNT));
        } else if (0 == tablet_ids.count()) {
          update_done = true;
          LOG_INFO("finish all rounds of batch update report scn", KR(ret), K(tenant_id),
                   "cost_time_us", ObTimeUtil::current_time() - start_time_us);
        } else if (OB_FAIL(construct_batch_update_report_scn_sql_str_(tenant_id,
                   global_broadcast_scn_val, except_status, tablet_ids, sql))) {
          LOG_WARN("fail to construct batch update sql str", KR(ret), K(tenant_id),
                   K(global_broadcast_scn_val), K(except_status));
        } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
          LOG_WARN("fail to start transaction", KR(ret), K(tenant_id), K(meta_tenant_id));
        } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch_with_trans(trans, tenant_id,
                   ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
          LOG_WARN("fail to check service_epoch with trans", KR(ret), K(tenant_id), K(expected_epoch));
        } else if (is_match) {
          if (OB_FAIL(trans.write(meta_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
          }
        } else { // !is_match
          ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
          LOG_WARN("freeze_service_epoch mismatch, do not update report_scn on this server", KR(ret), K(tenant_id));
        }
        ret = trans.handle_trans_in_the_end(ret);
        LOG_INFO("finish one round of batch update report scn", KR(ret), K(tenant_id),
                 K(affected_rows), K(BATCH_UPDATE_CNT));
      }
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::batch_update_status(
    const uint64_t tenant_id,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  const int64_t start_time_us = ObTimeUtil::current_time();
  const int64_t BATCH_UPDATE_CNT = 1000;
  uint64_t compat_version = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
    // do nothing until schema upgrade
  } else {
    LOG_INFO("start to batch update status", KR(ret), K(tenant_id), K(expected_epoch));
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    bool update_done = false;
    SMART_VAR(ObArray<ObTabletID>, tablet_ids) {
      while (OB_SUCC(ret) && !update_done) {
        bool is_match = true;
        ObMySQLTransaction trans;
        ObSqlString sql;
        int64_t affected_rows = 0;
        if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_next_batch_tablet_ids(tenant_id,
                    BATCH_UPDATE_CNT, tablet_ids))) {
          LOG_WARN("fail to get next batch of tablet_ids", KR(ret), K(tenant_id), K(BATCH_UPDATE_CNT));
        } else if (0 == tablet_ids.count()) {
          update_done = true;
          LOG_INFO("finish all rounds of batch update status", KR(ret), K(tenant_id),
                   "cost_time_us", ObTimeUtil::current_time() - start_time_us);
        } else if (OB_FAIL(construct_batch_update_status_sql_str_(tenant_id, tablet_ids, sql))) {
          LOG_WARN("fail to construct batch update sql str", KR(ret), K(tenant_id));
        } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
          LOG_WARN("fail to start transaction", KR(ret), K(tenant_id), K(meta_tenant_id));
        } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch_with_trans(trans, tenant_id,
                   ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
          LOG_WARN("fail to check service_epoch with trans", KR(ret), K(tenant_id), K(expected_epoch));
        } else if (is_match) {
          if (OB_FAIL(trans.write(meta_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
          }
        } else { // !is_match
          ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
          LOG_WARN("freeze_service_epoch mismatch, do not update status on this server", KR(ret), K(tenant_id));
        }
        ret = trans.handle_trans_in_the_end(ret);
        LOG_INFO("finish one round of batch update status", KR(ret), K(tenant_id), K(affected_rows), K(BATCH_UPDATE_CNT));
      }
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::batch_get_tablet_ids(
    const uint64_t tenant_id,
    const ObSqlString &sql,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    tablet_ids.reuse();
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    SMART_VAR(ObISQLClient::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(construct_tablet_id_array(*result, tablet_ids))) {
        LOG_WARN("fail to push_back tablet_id", KR(ret));
      }
    }
    LOG_INFO("finish to batch get tablet_ids", KR(ret), K(tenant_id), K(sql));
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::construct_batch_update_report_scn_sql_str_(
    const uint64_t tenant_id,
    const uint64_t global_braodcast_scn_val,
    const ObTabletReplica::ScnStatus &except_status,
    const ObIArray<ObTabletID> &tablet_ids,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_ids_cnt = tablet_ids.count();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET report_scn = '%lu' WHERE tenant_id = '%ld' AND"
             " tablet_id >= '%lu' AND tablet_id <= '%lu' AND compaction_scn >= '%lu' AND report_scn"
             " < '%lu' AND status != '%ld'", OB_ALL_TABLET_META_TABLE_TNAME, global_braodcast_scn_val,
             tenant_id, tablet_ids.at(0).id(), tablet_ids.at(tablet_ids_cnt - 1).id(), global_braodcast_scn_val,
             global_braodcast_scn_val, (int64_t)except_status))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(global_braodcast_scn_val), K(except_status),
             "start_tablet_id", tablet_ids.at(0), "end_tablet_id", tablet_ids.at(tablet_ids_cnt - 1));
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::construct_batch_update_status_sql_str_(
    const uint64_t tenant_id,
    const ObIArray<ObTabletID> &tablet_ids,
    ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_ids_cnt = tablet_ids.count();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tablet_ids));
  } else if (OB_FAIL(sql.assign_fmt("UPDATE %s SET status = '%ld' WHERE tenant_id = '%ld' AND"
             " tablet_id >= '%lu' AND tablet_id <= '%lu' AND status = '%ld'",
             OB_ALL_TABLET_META_TABLE_TNAME, (int64_t)ObTabletReplica::ScnStatus::SCN_STATUS_IDLE,
             tenant_id, tablet_ids.at(0).id(), tablet_ids.at(tablet_ids_cnt - 1).id(),
             (int64_t)ObTabletReplica::ScnStatus::SCN_STATUS_ERROR))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), "start_tablet_id", tablet_ids.at(0),
             "end_tablet_id", tablet_ids.at(tablet_ids_cnt - 1));
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::get_estimated_timeout_us(
    const uint64_t tenant_id,
    int64_t &estimated_timeout_us)
{
  int ret = OB_SUCCESS;
  int64_t tablet_replica_cnt = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(ObTabletMetaTableCompactionOperator::get_tablet_replica_cnt(tenant_id,
                                                          tablet_replica_cnt))) {
    LOG_WARN("fail to get tablet replica cnt", KR(ret), K(tenant_id));
  } else {
    estimated_timeout_us = tablet_replica_cnt * 1000L; // 1ms for each tablet replica
    estimated_timeout_us = MAX(estimated_timeout_us, THIS_WORKER.get_timeout_remain());
    estimated_timeout_us = MIN(estimated_timeout_us, 3 * 3600 * 1000 * 1000L);
    estimated_timeout_us = MAX(estimated_timeout_us, GCONF.rpc_timeout);
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::get_tablet_replica_cnt(
    const uint64_t tenant_id,
    int64_t &tablet_replica_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    ObSqlString sql;
    SMART_VAR(ObISQLClient::ReadResult, res) {
      ObMySQLResult *result = nullptr;
      if (OB_FAIL(sql.append_fmt("SELECT COUNT(*) as cnt from %s WHERE tenant_id = '%lu' ",
                                 OB_ALL_TABLET_META_TABLE_TNAME,
                                 tenant_id))) {
        LOG_WARN("failed to append fmt", K(ret), K(tenant_id));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(res, meta_tenant_id, sql.ptr()))) {
        LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
      } else if (OB_ISNULL(result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to get mysql result", KR(ret), K(tenant_id), K(sql));
      } else if (OB_FAIL(result->next())) {
        LOG_WARN("get next result failed", KR(ret), K(tenant_id), K(sql));
      } else {
        EXTRACT_INT_FIELD_MYSQL(*result, "cnt", tablet_replica_cnt, int64_t);
      }
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::batch_update_report_scn(
    const uint64_t tenant_id,
    const uint64_t global_broadcast_scn_val,
    const ObIArray<ObTabletLSPair> &tablet_pairs,
    const ObTabletReplica::ScnStatus &except_status,
    const int64_t expected_epoch)
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  uint64_t compat_version = 0;
  ObDMLSqlSplicer dml;
  const int64_t all_pair_cnt = tablet_pairs.count();
  if (OB_UNLIKELY((all_pair_cnt < 1)
      || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(all_pair_cnt));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_1_0_0) {
  } else {
    const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
    for (int64_t i = 0; OB_SUCC(ret) && (i < all_pair_cnt); i += MAX_BATCH_COUNT) {
      const int64_t cur_end_idx = MIN(i + MAX_BATCH_COUNT, all_pair_cnt);
      ObMySQLTransaction trans;
      ObSqlString sql;
      bool is_match = true;
      if (OB_FAIL(sql.append_fmt(
          "UPDATE %s SET report_scn = '%lu' WHERE tenant_id = %ld AND (tablet_id,ls_id) IN (",
          OB_ALL_TABLET_META_TABLE_TNAME,
          global_broadcast_scn_val,
          tenant_id))) {
        LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(global_broadcast_scn_val));
      } else {
        // handle each batch tablet_ls_pairs
        for (int64_t idx = i; OB_SUCC(ret) && (idx < cur_end_idx); ++idx) {
          const ObTabletID &tablet_id = tablet_pairs.at(idx).get_tablet_id();
          const ObLSID &ls_id = tablet_pairs.at(idx).get_ls_id();
          if (OB_UNLIKELY(!tablet_id.is_valid_with_tenant(tenant_id)
              || !ls_id.is_valid_with_tenant(tenant_id))) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid tablet_id with tenant", KR(ret), K(tenant_id), K(tablet_id), K(ls_id));
          } else if (OB_FAIL(sql.append_fmt(
              "%s (%ld,%ld)",
              i == idx ? "" : ",",
              tablet_id.id(),
              ls_id.id()))) {
            LOG_WARN("fail to assign sql", KR(ret), K(tablet_id));
          }
        } // end for
        if (FAILEDx(sql.append_fmt(") AND compaction_scn >= '%lu' AND report_scn < '%lu' AND status != %ld",
            global_broadcast_scn_val, global_broadcast_scn_val, (int64_t)(except_status)))) {
          LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(except_status),
            K(global_broadcast_scn_val));
        } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, meta_tenant_id))) {
          LOG_WARN("fail to start transaction", KR(ret), K(tenant_id), K(meta_tenant_id));
        } else if (OB_FAIL(ObServiceEpochProxy::check_service_epoch_with_trans(trans, tenant_id,
                   ObServiceEpochProxy::FREEZE_SERVICE_EPOCH, expected_epoch, is_match))) {
          LOG_WARN("fail to check service_epoch with trans", KR(ret), K(tenant_id), K(expected_epoch));
        } else if (is_match) {
          if (OB_FAIL(trans.write(meta_tenant_id, sql.ptr(), affected_rows))) {
            LOG_WARN("fail to execute sql", KR(ret), K(tenant_id), K(meta_tenant_id), K(sql));
          } else {
            LOG_TRACE("success to update report_scn", KR(ret), K(tenant_id), K(meta_tenant_id), K(tablet_pairs), K(sql));
          }
        } else { // !is_match
          ret = OB_FREEZE_SERVICE_EPOCH_MISMATCH;
          LOG_WARN("freeze_service_epoch mismatch, do not update report_scn on this server", KR(ret), K(tenant_id));
        }
      }
      ret = trans.handle_trans_in_the_end(ret);
    }
  }

  return ret;
}

int ObTabletMetaTableCompactionOperator::get_next_batch_tablet_ids(
    const uint64_t tenant_id,
    const int64_t batch_update_cnt,
    ObIArray<ObTabletID> &tablet_ids)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || batch_update_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(batch_update_cnt));
  } else {
    ObSqlString sql;
    ObTabletID start_tablet_id = ObTabletID(ObTabletID::INVALID_TABLET_ID);
    if (tablet_ids.count() > 0) {
      start_tablet_id = tablet_ids.at(tablet_ids.count() - 1);
    }
    tablet_ids.reuse();
    if (OB_FAIL(sql.append_fmt("SELECT DISTINCT tablet_id from %s WHERE tenant_id = '%ld' "
                  "AND tablet_id > '%ld' ORDER BY tenant_id, tablet_id ASC LIMIT %ld",
                  OB_ALL_TABLET_META_TABLE_TNAME, tenant_id, start_tablet_id.id(), batch_update_cnt))) {
      LOG_WARN("failed to append fmt", K(ret), K(tenant_id), K(start_tablet_id), K(batch_update_cnt));
    } else if (OB_FAIL(batch_get_tablet_ids(tenant_id, sql, tablet_ids))) {
      LOG_WARN("fail to batch get tablet_ids", KR(ret), K(tenant_id), K(start_tablet_id), K(batch_update_cnt));
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::range_scan_for_compaction(
      const uint64_t tenant_id,
      const int64_t compaction_scn,
      const common::ObTabletID &start_tablet_id,
      const int64_t batch_size,
      const bool add_report_scn_filter,
      common::ObTabletID &end_tablet_id,
      ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  tablet_infos.reset();
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || batch_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(start_tablet_id), K(batch_size));
  } else if (start_tablet_id.id() == INT64_MAX) {
    ret = OB_ITER_END;
  } else {
    ObTabletID tmp_start_tablet_id = start_tablet_id;
    ObTabletID tmp_end_tablet_id;
    while (OB_SUCC(ret) && tmp_start_tablet_id.id() < INT64_MAX) {
      if (OB_SUCC(inner_range_scan_for_compaction(
              tenant_id, compaction_scn, tmp_start_tablet_id, batch_size,
              add_report_scn_filter, tmp_end_tablet_id, tablet_infos))) {
        if (tablet_infos.empty()) {
          tmp_start_tablet_id = tmp_end_tablet_id;
          tmp_end_tablet_id.reset();
        } else {
          break;
        }
      }
    } // end of while
    if (OB_SUCC(ret)) {
      end_tablet_id = tmp_end_tablet_id;
      if (tablet_infos.empty()) {
        ret = OB_ITER_END;
      }
    }
  }
  return ret;
}


int ObTabletMetaTableCompactionOperator::inner_range_scan_for_compaction(
    const uint64_t tenant_id,
    const int64_t compaction_scn,
    const common::ObTabletID &start_tablet_id,
    const int64_t batch_size,
    const bool add_report_scn_filter,
    common::ObTabletID &end_tablet_id,
    ObIArray<ObTabletInfo> &tablet_infos)
{
  int ret = OB_SUCCESS;
  ObTabletID max_tablet_id;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sql proxy in ctx is null", KR(ret), K(GCTX.sql_proxy_));
  } else if (OB_FAIL(inner_get_max_tablet_id_in_range(tenant_id, start_tablet_id, batch_size, max_tablet_id))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get max tablet id in range", KR(ret), K(start_tablet_id));
    } else {
      ret = OB_SUCCESS;
      max_tablet_id = ObTabletID(INT64_MAX);
    }
  }
  if (OB_SUCC(ret)) {
    SMART_VAR(ObISQLClient::ReadResult, result) {
      const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
      ObSqlString sql;
      ObSqlString report_scn_sql;
      if (add_report_scn_filter && OB_FAIL(report_scn_sql.append_fmt("AND report_scn < %ld", compaction_scn))) {
        LOG_WARN("fail to assign sql", KR(ret), K(report_scn_sql));
      } else if (OB_FAIL(sql.append_fmt(
          "SELECT * from %s where tenant_id=%lu AND tablet_id > %ld AND tablet_id <= %ld %s",
          OB_ALL_TABLET_META_TABLE_TNAME, tenant_id, start_tablet_id.id(), max_tablet_id.id(), add_report_scn_filter ? report_scn_sql.ptr() : ""))) {
        LOG_WARN("fail to assign sql", KR(ret), K(sql));
      } else if (OB_FAIL(GCTX.sql_proxy_->read(result, sql_tenant_id, sql.ptr()))) {
        LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql_tenant_id), K(sql));
      } else if (OB_ISNULL(result.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get mysql result failed", KR(ret), K(sql));
      } else if (OB_FAIL(ObTabletTableOperator::construct_tablet_infos(*result.get_result(), tablet_infos))) {
        LOG_WARN("construct tablet info failed", KR(ret), K(sql), K(tablet_infos));
      } else {
        end_tablet_id = max_tablet_id;
        LOG_INFO("success to get tablet info", KR(ret), K(batch_size), K(tablet_infos), K(end_tablet_id), K(add_report_scn_filter));
      }
    }
  }
  return ret;
}

int ObTabletMetaTableCompactionOperator::inner_get_max_tablet_id_in_range(
    const uint64_t tenant_id,
    const common::ObTabletID &start_tablet_id,
    const int64_t batch_size,
    common:: ObTabletID &end_tablet_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  sqlclient::ObMySQLResult *result_ptr = nullptr;
  const uint64_t sql_tenant_id = gen_meta_tenant_id(tenant_id);
  int64_t tablet_id = 0;
  SMART_VAR(ObISQLClient::ReadResult, result) {
    if (OB_FAIL(sql.append_fmt("SELECT tablet_id FROM %s "
        "WHERE tenant_id = %lu AND tablet_id > %ld ORDER BY tablet_id asc LIMIT 1 OFFSET %ld",
        OB_ALL_TABLET_META_TABLE_TNAME, tenant_id, start_tablet_id.id(), batch_size))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql));
    } else if (OB_FAIL(GCTX.sql_proxy_->read(result, sql_tenant_id, sql.ptr()))) {
      LOG_WARN("execute sql failed", KR(ret), K(tenant_id), K(sql_tenant_id), K(sql));
    } else if (OB_ISNULL(result_ptr = result.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get mysql result failed", KR(ret), K(sql));
    } else if (OB_FAIL(result_ptr->next())) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fail to get next result", KR(ret));
      }
    } else if (OB_FAIL(result_ptr->get_int("tablet_id", tablet_id))) {
      LOG_WARN("fail to get uint", KR(ret));
    } else {
      end_tablet_id = tablet_id;
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase

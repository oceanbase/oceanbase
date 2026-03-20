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

#include "share/ob_sync_standby_dest_operator.h"
namespace oceanbase
{
namespace share
{
const uint64_t ObSyncStandbyDestOperator::OB_SYNC_STANDBY_DEST_ID = 0;
ObSyncStandbyDestOperator::ObSyncStandbyDestOperator()
{
}

ObSyncStandbyDestOperator::~ObSyncStandbyDestOperator()
{
}

int ObSyncStandbyDestOperator::read_sync_standby_dest(ObISQLClient &sql_client,
  const uint64_t meta_tenant_id, const bool for_update, bool &is_empty,
  ObSyncStandbyDestStruct &sync_standby_dest, int64_t *ora_rowscn)
{
  int ret = OB_SUCCESS;
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    common::sqlclient::ObMySQLResult *result = NULL;
    is_empty = false;
    if (OB_NOT_NULL(ora_rowscn)) {
      *ora_rowscn = 0;
    }
    ObSqlString sql;
    if (OB_UNLIKELY(!is_valid_tenant_id(meta_tenant_id) || !is_meta_tenant(meta_tenant_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tenant id", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(sql.append_fmt("SELECT ORA_ROWSCN, * FROM %s WHERE tenant_id = %lu and dest_id = %lu %s",
        OB_ALL_SYNC_STANDBY_DEST_TNAME, gen_user_tenant_id(meta_tenant_id), OB_SYNC_STANDBY_DEST_ID,
        for_update ? " FOR UPDATE" : ""))) {
      LOG_WARN("failed to assign sql", KR(ret), K(meta_tenant_id));
    } else if (OB_FAIL(sql_client.read(res, meta_tenant_id, sql.ptr()))) {
      LOG_WARN("failed to read", KR(ret), K(meta_tenant_id), K(sql));
    } else if (OB_ISNULL(result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("result is null", KR(ret), KP(result));
    } else if (OB_FAIL(result->next())) {
      if (OB_ITER_END == ret) {
        is_empty = true;
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to get next", KR(ret));
      }
    } else {
      ObString value_str;
      int64_t current_ora_rowscn = 0;
      EXTRACT_INT_FIELD_MYSQL(*result, "ORA_ROWSCN", current_ora_rowscn, int64_t);
      EXTRACT_VARCHAR_FIELD_MYSQL(*result, "value", value_str);
      if (FAILEDx(sync_standby_dest.parse_standby_dest_from_str(value_str))) {
        LOG_WARN("failed to parse standby dest from string", KR(ret), K(value_str));
      } else if (result->next() != OB_ITER_END) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected more than one row", KR(ret));
      } else if (OB_NOT_NULL(ora_rowscn)) {
        *ora_rowscn = current_ora_rowscn;
      }
    }
  }
  return ret;
}
int ObSyncStandbyDestOperator::write_sync_standby_dest(ObISQLClient &sql_client, const uint64_t meta_tenant_id, const ObSyncStandbyDestStruct &sync_standby_dest)
{
  int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObSqlString sync_standby_dest_str;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(meta_tenant_id) || !is_meta_tenant(meta_tenant_id)
      || !sync_standby_dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", gen_user_tenant_id(meta_tenant_id)))) {
    LOG_WARN("failed to add pk column tenant_id", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(dml.add_pk_column("dest_id", OB_SYNC_STANDBY_DEST_ID))) {
    LOG_WARN("failed to add pk column", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(sync_standby_dest.gen_standby_dest_str(sync_standby_dest_str))) {
    LOG_WARN("failed to generate standby dest string", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(dml.add_column("value", sync_standby_dest_str.ptr()))) {
    LOG_WARN("failed to add column", KR(ret), K(meta_tenant_id), K(sync_standby_dest_str));
  } else if (OB_FAIL(dml.splice_insert_update_sql(OB_ALL_SYNC_STANDBY_DEST_TNAME, sql))) {
    LOG_WARN("failed to splice insert sql", KR(ret), K(meta_tenant_id), K(sql));
  } else if (OB_FAIL(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", KR(ret), K(meta_tenant_id), K(sql));
    // affected_rows=0: 1 row --> same row
    // affected_rows=1: empty --> 1 row
    // affected_rows=2: 1 row --> another row
  } else if (affected_rows < 0 || affected_rows > 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(meta_tenant_id), K(sql));
  }
  return ret;
}
int ObSyncStandbyDestOperator::delete_sync_standby_dest(ObISQLClient &sql_client, const uint64_t meta_tenant_id)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!is_valid_tenant_id(meta_tenant_id) || !is_meta_tenant(meta_tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(sql.append_fmt("DELETE FROM %s WHERE tenant_id = %lu and dest_id = %lu",
      OB_ALL_SYNC_STANDBY_DEST_TNAME, gen_user_tenant_id(meta_tenant_id), OB_SYNC_STANDBY_DEST_ID))) {
    LOG_WARN("failed to assign sql", KR(ret), K(meta_tenant_id));
  } else if (OB_FAIL(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to write", KR(ret), K(meta_tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows), K(meta_tenant_id), K(sql));
  }
  return ret;
}
} // namespace share
} // namespace oceanbase
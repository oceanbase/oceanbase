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

#include "share/ob_column_checksum_error_operator.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_freeze_info_proxy.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_smart_var.h"

namespace oceanbase
{
namespace share
{
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;

///////////////////////////////////////////////////////////////////////////////

bool ObColumnChecksumErrorInfo::is_valid() const
{
  return (tenant_id_ != OB_INVALID_TENANT_ID) && (frozen_scn_.is_valid())
         && (data_table_id_ != OB_INVALID_ID) && (index_table_id_ != OB_INVALID_ID);
}

void ObColumnChecksumErrorInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  frozen_scn_.reset();
  is_global_index_ = false;
  data_table_id_ = OB_INVALID_ID;
  index_table_id_ = OB_INVALID_ID;
  data_tablet_id_.reset();
  index_tablet_id_.reset();
  column_id_ = OB_INVALID_ID;
  data_column_checksum_ = -1;
  index_column_checksum_ = -1;
}

///////////////////////////////////////////////////////////////////////////////

int ObColumnChecksumErrorOperator::insert_column_checksum_err_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObColumnChecksumErrorInfo &info)
{
  return insert_column_checksum_err_info_(sql_client, tenant_id, info);
}

int ObColumnChecksumErrorOperator::insert_column_checksum_err_info_(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const ObColumnChecksumErrorInfo &info)
{
    int ret = OB_SUCCESS;
  ObDMLSqlSplicer dml;
  int64_t affected_rows = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  ObDMLExecHelper exec(sql_client, meta_tenant_id);
  if (!info.is_valid()
      || (tenant_id != info.tenant_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(dml.add_pk_column("tenant_id", tenant_id))
            || OB_FAIL(dml.add_pk_column("frozen_scn", info.frozen_scn_.get_val_for_inner_table_field()))
            || OB_FAIL(dml.add_pk_column("index_type", info.is_global_index_))
            || OB_FAIL(dml.add_pk_column("data_table_id", info.data_table_id_))
            || OB_FAIL(dml.add_pk_column("index_table_id", info.index_table_id_))
            || OB_FAIL(dml.add_pk_column("data_tablet_id", info.data_tablet_id_.id()))
            || OB_FAIL(dml.add_pk_column("index_tablet_id", info.index_tablet_id_.id()))
            || OB_FAIL(dml.add_column("column_id", info.column_id_))
            || OB_FAIL(dml.add_column("data_column_ckm", info.data_column_checksum_))
            || OB_FAIL(dml.add_column("index_column_ckm", info.index_column_checksum_))) {
    LOG_WARN("fail to add pk column", KR(ret), K(tenant_id), K(info));
  } else if (OB_FAIL(exec.exec_insert_update(OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TNAME, dml, affected_rows))) {
    LOG_WARN("fail to splice exec_insert_update", KR(ret), K(meta_tenant_id), K(info));
  } else if (affected_rows < 0 || affected_rows > 2) {
    // one ckm_error info may insert multi-times due to verifying checksum multi-times. if re-insert, cuz we
    // use 'on duplicate key update', the 'affected_rows' may be 0(not pk_column values unchanged)
    // or 2(not pk_column values changed)
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected affected rows", KR(ret), K(affected_rows));
  }
  return ret;
}

int ObColumnChecksumErrorOperator::delete_column_checksum_err_info(
    ObISQLClient &sql_client,
    const uint64_t tenant_id,
    const SCN &min_frozen_scn)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  int64_t affected_rows = 0;
  const uint64_t meta_tenant_id = gen_meta_tenant_id(tenant_id);
  if (OB_UNLIKELY((!is_valid_tenant_id(tenant_id))) || (!min_frozen_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(min_frozen_scn));
  } else if (OB_FAIL(sql.assign_fmt("DELETE FROM %s WHERE tenant_id = '%lu' AND frozen_scn < %lu",
             OB_ALL_COLUMN_CHECKSUM_ERROR_INFO_TNAME, tenant_id, min_frozen_scn.get_val_for_inner_table_field()))) {
    LOG_WARN("fail to assign sql", KR(ret), K(tenant_id), K(min_frozen_scn));
  } else if (OB_FAIL(sql_client.write(meta_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("fail to execute sql", KR(ret), K(meta_tenant_id), K(sql));
  } else {
    LOG_INFO("succ to delete column checksum error info", K(tenant_id), K(min_frozen_scn), K(affected_rows));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

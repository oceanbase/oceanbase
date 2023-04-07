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

#include "observer/ob_inner_sql_rpc_proxy.h"
#include "lib/utility/serialization.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace obrpc
{

OB_DEF_SERIALIZE(obrpc::ObInnerSQLTransmitArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
    ctrl_svr_,
    runner_svr_,
    tenant_id_,
    conn_id_,
    inner_sql_,
    operation_type_,
    is_oracle_mode_,
    source_cluster_id_,
    worker_timeout_,
    query_timeout_,
    trx_timeout_,
    sql_mode_,
    tz_info_wrap_,
    ddl_info_,
    is_load_data_exec_,
    nls_formats_,
    use_external_session_,
    consumer_group_id_);
  return ret;
}

OB_DEF_DESERIALIZE(obrpc::ObInnerSQLTransmitArg)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
    ctrl_svr_,
    runner_svr_,
    tenant_id_,
    conn_id_,
    inner_sql_,
    operation_type_,
    is_oracle_mode_,
    source_cluster_id_,
    worker_timeout_,
    query_timeout_,
    trx_timeout_,
    sql_mode_,
    tz_info_wrap_,
    ddl_info_,
    is_load_data_exec_,
    nls_formats_,
    use_external_session_,
    consumer_group_id_);
  if (OB_SUCC(ret)) {
    (void)sql::ObSQLUtils::adjust_time_by_ntp_offset(worker_timeout_);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(obrpc::ObInnerSQLTransmitArg)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
    ctrl_svr_,
    runner_svr_,
    tenant_id_,
    conn_id_,
    inner_sql_,
    operation_type_,
    is_oracle_mode_,
    source_cluster_id_,
    worker_timeout_,
    query_timeout_,
    trx_timeout_,
    sql_mode_,
    tz_info_wrap_,
    ddl_info_,
    is_load_data_exec_,
    nls_formats_,
    use_external_session_,
    consumer_group_id_);
  return len;
}
//
// OB_SERIALIZE_MEMBER(obrpc::ObInnerSQLTransmitResult, res_code_, conn_id_, affected_rows_, stmt_type_, scanner_, field_columns_);
OB_DEF_SERIALIZE(obrpc::ObInnerSQLTransmitResult)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, res_code_, conn_id_, affected_rows_, stmt_type_, scanner_, field_columns_);
  return ret;
}

OB_DEF_DESERIALIZE(obrpc::ObInnerSQLTransmitResult)
{
  int ret = OB_SUCCESS;
  common::ObSArray<common::ObField> field_columns;
  LST_DO_CODE(OB_UNIS_DECODE, res_code_, conn_id_, affected_rows_, stmt_type_, scanner_, field_columns);
  if (FAILEDx(copy_field_columns(field_columns))) {
    _OB_LOG(WARN, "copy_field_columns failed. err = %d", ret);
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(obrpc::ObInnerSQLTransmitResult)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, res_code_, conn_id_, affected_rows_, stmt_type_, scanner_, field_columns_);
  return len;
}

int ObInnerSQLTransmitResult::copy_field_columns(
    const common::ObIArray<common::ObField> &src_field_columns)
{
  int ret = OB_SUCCESS;
  field_columns_.reset();
  ObField field;
  int64_t N = src_field_columns.count();
  if (N > 0 && OB_FAIL(field_columns_.reserve(N))) {
    _OB_LOG(WARN, "failed to reserve field column array. err = %d, count = %ld", ret, N);
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    const ObField &ofield = src_field_columns.at(i);
    if (OB_FAIL(field.deep_copy(ofield, &allocator_))) {
      _OB_LOG(WARN, "deep copy field failed. err = %d", ret);
    } else if (OB_FAIL(field_columns_.push_back(field))) {
      _OB_LOG(WARN, "push back field column failed. err = %d", ret);
    }
  }

  return ret;
}

}/* ns obrpc */
}/* ns oceanbase */

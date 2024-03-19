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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_ls_getter.h"
#include "lib/oblog/ob_log_module.h"        // LOG_ERROR
#include "ob_log_instance.h"

namespace oceanbase
{
namespace libobcdc
{

const char* TenantLSQueryer::QUERY_LS_INFO_SQL_FORMAT =
    "SELECT LS_ID FROM "
    "(SELECT LS_ID, STATUS, CREATE_SCN, CASE WHEN STATUS = 'DROPPED' THEN ora_rowscn ELSE 1 END AS DROP_SCN FROM %s) "
    "WHERE CREATE_SCN <= %lu AND (DROP_SCN > %lu OR DROP_SCN = 1) AND STATUS NOT IN ('CREATE_ABORT', 'CREATING');";

int TenantLSQueryer::build_sql_statement_(const uint64_t tenant_id, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::SCN snapshot_scn;

  if (OB_FAIL(snapshot_scn.convert_for_gts(snapshot_ts_ns_))) {
    LOG_ERROR("convert_for_gts failed", KR(ret), K(tenant_id), K_(snapshot_ts_ns));
  } else if (OB_UNLIKELY(! snapshot_scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("snapshot_scn is not valid", KR(ret), K(tenant_id), K(snapshot_scn), K_(snapshot_ts_ns));
  } else {
    uint64_t snapshto_scn_val = snapshot_scn.get_val_for_inner_table_field();

    if (OB_FAIL(sql.assign_fmt(QUERY_LS_INFO_SQL_FORMAT, OB_ALL_LS_TNAME, snapshto_scn_val, snapshto_scn_val))) {
      LOG_ERROR("assign_fmt for TenantLSQuerySQL failed", KR(ret),
          K_(snapshot_ts_ns), K(snapshot_scn), K(snapshto_scn_val), K(sql));
    }
  }

  return ret;
}

int TenantLSQueryer::parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<LSIDArray> &result)
{
  int ret = OB_SUCCESS;
  int64_t ls_id = share::ObLSID::INVALID_LS_ID;
  EXTRACT_UINT_FIELD_MYSQL(sql_result, "LS_ID", ls_id, int64_t);
  share::ObLSID ob_ls_id(ls_id);

  if (OB_FAIL(ret)) {
    LOG_ERROR("extract ls_id field from sql_result failed", KR(ret), K_(snapshot_ts_ns), K(ob_ls_id));
  } else if (OB_UNLIKELY(! ob_ls_id.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls_id", KR(ret), K_(snapshot_ts_ns), K(ob_ls_id));
  } else if (OB_FAIL(result.get_data().push_back(ob_ls_id))) {
    LOG_ERROR("push_back ls_id into query_result failed", KR(ret), K_(snapshot_ts_ns), K(ob_ls_id), K(result));
  }

  return ret;
}

ObLogLsGetter::ObLogLsGetter() :
    is_inited_(false),
    tenant_ls_ids_cache_()
{
}

ObLogLsGetter::~ObLogLsGetter()
{
  destroy();
}

int ObLogLsGetter::init(const common::ObIArray<uint64_t> &tenant_ids, const int64_t start_tstamp_ns)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogLsGetter has been inited", KR(ret));
  } else if (OB_FAIL(tenant_ls_ids_cache_.init("TenantLSIDs"))) {
    LOG_ERROR("tenant_ls_ids_cache_ init failed", KR(ret));
  } else {
    ARRAY_FOREACH_N(tenant_ids, idx, count) {
      const uint64_t tenant_id = tenant_ids.at(idx);

      if (OB_SYS_TENANT_ID == tenant_id || is_meta_tenant(tenant_id)) {
        // do nothing
      } else if (OB_FAIL(query_and_set_tenant_ls_info_(tenant_id, start_tstamp_ns))) {
        LOG_ERROR("query_and_set_tenant_ls_info_ failed", KR(ret), K(tenant_id), K(start_tstamp_ns));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  return ret;
}

void ObLogLsGetter::destroy()
{
  if (is_inited_) {
    tenant_ls_ids_cache_.destroy();
    is_inited_ = false;
  }
}

int ObLogLsGetter::get_ls_ids(
    const uint64_t tenant_id,
    const int64_t snapshot_ts,
    common::ObIArray<share::ObLSID> &ls_id_array)
{
  int ret = OB_SUCCESS;
  LSIDArray ls_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLsGetter has not been inited", KR(ret));
  } else if (OB_FAIL(tenant_ls_ids_cache_.get(tenant_id, ls_ids))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("tenant_ls_ids_cache_ get failed", KR(ret), K(tenant_id), K(ls_ids));
    } else {
      ret = OB_SUCCESS;
      // query and set
      if (OB_FAIL(query_and_set_tenant_ls_info_(tenant_id, snapshot_ts))) {
        LOG_ERROR("query_and_set_tenant_ls_info_ failed", KR(ret), K(tenant_id));
      } else if (OB_FAIL(tenant_ls_ids_cache_.get(tenant_id, ls_ids))) {
        LOG_ERROR("get tenant_ls_ids from cache failed", KR(ret), K(tenant_id), K(snapshot_ts));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ARRAY_FOREACH_N(ls_ids, idx, count) {
      if (OB_FAIL(ls_id_array.push_back(ls_ids.at(idx)))) {
        LOG_ERROR("ls_id_array push_back failed", KR(ret), K(ls_id_array));
      }
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("get_ls_ids succ", K(tenant_id), K(ls_id_array), K(snapshot_ts));
    }
  }

  return ret;
}

int ObLogLsGetter::query_and_set_tenant_ls_info_(
    const uint64_t tenant_id,
    const int64_t snapshot_ts)
{
  int ret = OB_SUCCESS;
  LSIDArray ls_ids;

  if (OB_FAIL(query_tenant_ls_info_(tenant_id, snapshot_ts, ls_ids))) {
    LOG_ERROR("query_tenant_ls_info failed", KR(ret), K(tenant_id), K(snapshot_ts));
  } else if (OB_FAIL(tenant_ls_ids_cache_.insert(tenant_id, ls_ids))) {
    LOG_ERROR("tenant_ls_ids_cache_ insert data failed", KR(ret), K(tenant_id), K(ls_ids));
  } else {
    LOG_INFO("tenant_ls_ids_cache_ insert success", K(tenant_id), K(snapshot_ts), K(ls_ids));
  }

  return ret;
}

int ObLogLsGetter::query_tenant_ls_info_(
    const uint64_t tenant_id,
    const int64_t snapshot_ts,
    LSIDArray &ls_array)
{
  int ret = OB_SUCCESS;
  const static int64_t QUERY_TIMEOUT = 10 * _MIN_;
  ls_array.reset();
  ObCDCQueryResult<LSIDArray> query_result(ls_array);
  TenantLSQueryer queryer(snapshot_ts, TCTX.tenant_sql_proxy_.get_ob_mysql_proxy());

  if (OB_UNLIKELY(! is_user_tenant(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("expected user_tenant for TenantLSGetter", KR(ret), K(tenant_id));
  } else if (OB_FAIL(queryer.query(tenant_id, query_result, QUERY_TIMEOUT))) {
    LOG_ERROR("query tenant ls_id failed", KR(ret), K(tenant_id), K(snapshot_ts), K(ls_array));
  } else if (OB_UNLIKELY(query_result.is_empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tenant ls_id query result empty", KR(ret), K(tenant_id), K(snapshot_ts), K(query_result), K(ls_array));
  }

  return ret;
}


} // namespace libobcdc
} // namespace oceanbase

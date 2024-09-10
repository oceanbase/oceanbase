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
 *
 * definition of PartTransID(identify a PartTrans) and TenantTransID
 */

#include "ob_cdc_tenant_checkpoint.h"
#include "ob_log_instance.h"
#define USING_LOG_PREFIX OBLOG
namespace oceanbase
{
namespace libobcdc
{

void TenantCheckpoint::reset_data()
{
  tenant_role_ = share::ObTenantRole::INVALID_TENANT;
  palf_available_scn_.reset();
  archive_available_scn_.reset();
  restore_scn_.reset();
  readable_scn_.reset();
  replayable_scn_.reset();
  sync_scn_.reset();
  recovery_until_scn_.reset();
}

int TenantCheckpoint::set_tenant_id(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_id", KR(ret), K(tenant_id), KPC(this));
  } else if (tenant_id_ == OB_INVALID_TENANT_ID) {
    // assign tenant_id if self tenant_id is invalid
    tenant_id_ = tenant_id;
  } else if (tenant_id != tenant_id_) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("tenant_checkppoint already used by other tenant, can't reset tenant", KR(ret),
        "current_tenant", tenant_id_, "target_tenant", tenant_id);
  }

  return ret;
}

int TenantCheckpoint::assign(const TenantCheckpoint &other)
{
  int ret = OB_SUCCESS;

  if (OB_LIKELY(this != &other)) {
    if (OB_UNLIKELY(tenant_id_ != other.tenant_id_)) {
      ret = OB_STATE_NOT_MATCH;
      LOG_ERROR("can't assign tenant_checkpoint between different tenant", KR(ret), KPC(this), K(other));
    } else {
      tenant_role_ = other.tenant_role_;
      palf_available_scn_ = other.palf_available_scn_;
      archive_available_scn_ = other.archive_available_scn_;
      restore_scn_ = other.restore_scn_;
      readable_scn_ = other.readable_scn_;
      replayable_scn_ = other.replayable_scn_;
      sync_scn_ = other.sync_scn_;
      recovery_until_scn_ = other.recovery_until_scn_;
    }
  }

  return ret;
}

int TenantCheckpoint::refresh(const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_INVALID_TENANT_ID == tenant_id_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant_id", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(TCTX.is_online_sql_not_available() || ! is_user_tenant(tenant_id_))) {
    // won't refresh if sql not available
  } else {
    const bool is_cluster_sql_proxy_available = ! TCTX.is_tenant_sync_mode();
    common::ObMySQLProxy& sql_proxy = is_cluster_sql_proxy_available ? TCTX.tenant_sql_proxy_.get_ob_mysql_proxy() : TCTX.mysql_proxy_.get_ob_mysql_proxy();
    TenantCheckpointQueryer queryer(tenant_id_, is_cluster_sql_proxy_available, sql_proxy);

    if (OB_FAIL(queryer.get_tenant_checkpoint(*this, timeout))) {
      LOG_ERROR("get_tenant_checkpoint failed", KR(ret), KPC(this), K(timeout));
    } else {
      LOG_TRACE("get_tenant_checkpoint", KR(ret), KPC(this), K(timeout));
    }
  }

  return ret;
}

int TenantCheckpoint::check_primary_tenant_(bool &is_primary_tenant) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid tenant_id", KR(ret), K_(tenant_id));
  } else if (OB_UNLIKELY(TCTX.is_online_sql_not_available())) {
    // always served if cdc can't access observer(offline mode)
    is_primary_tenant = true;
  } else if (OB_UNLIKELY(! is_user_tenant(tenant_id_))) {
    // sys tenant and meta tenant should always be primary
    is_primary_tenant = true;
  } else if (OB_UNLIKELY(!tenant_role_.is_valid())) {
    ret = OB_NEED_RETRY;
    LOG_ERROR("tenant_role invalid, please invoke refresh_tenant_checkpoint and retry", KR(ret), KPC(this));
  } else {
    is_primary_tenant = tenant_role_.is_primary();
  }

  return ret;
}

int TenantCheckpoint::check_clog_served(const int64_t clog_scn_val, bool &is_served, bool &need_retry) const
{
  int ret = OB_SUCCESS;
  is_served = false;
  need_retry = false;
  share::SCN clog_scn;
  bool is_primary_tenant = false;

  if (OB_FAIL(check_primary_tenant_(is_primary_tenant))) {
    LOG_ERROR("check_primary_tenant_ failed", KR(ret), KPC(this));
  } else if (is_primary_tenant) {
    is_served = true;
    // always served if primary tenant
  } else if (OB_FAIL(clog_scn.convert_for_gts(clog_scn_val))) {
    LOG_ERROR("scn convert_for_gts failed", KR(ret), K(clog_scn_val));
  } else if (OB_UNLIKELY(clog_scn < restore_scn_)) {
    is_served = false;
    LOG_INFO("[CLOG_NOT_SERVED][REASON=BELOW_RESTORE_SCN]", K(clog_scn), KPC(this));
  } else if (OB_UNLIKELY(clog_scn >= recovery_until_scn_)) {
    is_served = false;
    LOG_INFO("[CLOG_NOT_SERVED][REASON=OVER_RECOVERY_SCN] please check tenant recovery_until_scn", K(clog_scn), KPC(this));
  } else if (OB_UNLIKELY(clog_scn > replayable_scn_)) {
    is_served = false;
    need_retry = true;
    LOG_INFO("[CLOG_NOT_SERVED][REASON=OVER_REPLAYABLE_SCN] please check tenant clog sync status, need retry until clog served", K(clog_scn), KPC(this));
  } else {
    is_served = true;
    LOG_INFO("CLOG_SERVED", K(clog_scn_val), KPC(this));
  }

  return ret;
}

int TenantCheckpoint::check_snapshot_readable_(const int64_t snapshot_scn_val, bool &is_readable)
{
  int ret = OB_SUCCESS;
  is_readable = false;
  share::SCN scn;
  bool is_primary_tenant = false;

  if (OB_FAIL(check_primary_tenant_(is_primary_tenant))) {
    LOG_ERROR("check_primary_tenant_ failed", KR(ret), KPC(this));
  } else if (is_primary_tenant) {
    is_readable = true;
    // always readable if primary tenant
  } else if (OB_FAIL(scn.convert_for_gts(snapshot_scn_val))) {
    LOG_ERROR("scn convert_for_gts failed", KR(ret), K(snapshot_scn_val));
  } else {
    is_readable = (readable_scn_ >= scn);
  }

  return ret;
}

int TenantCheckpoint::wait_until_readable(const int64_t snapshot_scn_val, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = get_timestamp();
  const int64_t end_ts = start_ts + timeout_us;
  bool is_readable = false;

  while (OB_SUCC(ret) && !is_readable) {
    if (OB_FAIL(check_snapshot_readable_(snapshot_scn_val, is_readable))) {
      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("check_snapshot_readable_ failed", KR(ret), K(snapshot_scn_val), KPC(this));
      }
    }

    if (OB_SUCC(ret) && OB_UNLIKELY(!is_readable)) {
      if (get_timestamp() >= end_ts) {
        ret = OB_TIMEOUT;
      } else {
        ob_usleep(100 * _MSEC_);

        if (OB_FAIL(refresh(timeout_us))) {
          if (OB_NEED_RETRY == ret) {
            LOG_WARN("refresh tenant_checkpoint failed", KR(ret), KPC(this), K(snapshot_scn_val), K(timeout_us));
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("refresh tenant_checkpoint failed", KR(ret), KPC(this), K(snapshot_scn_val), K(timeout_us));
          }
        }
        if (TC_REACH_TIME_INTERVAL(10 * _SEC_)) {
          LOG_INFO("[WAIT_UNTIL_READABLE]", K(snapshot_scn_val), KPC(this), "start_ts", TS_TO_STR(start_ts), "timeout", TVAL_TO_STR(timeout_us));
        }
      }
    }
  }

  return ret;
}

int TenantCheckpoint::wait_until_clog_served(const int64_t snapshot_scn_val, bool &is_clog_served, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = get_timestamp();
  const int64_t end_ts = start_ts + timeout;
  is_clog_served = false;
  bool need_retry = true;

  while (OB_SUCC(ret) && ! is_clog_served && need_retry) {
    if (OB_FAIL(check_clog_served(snapshot_scn_val, is_clog_served, need_retry))) {
      if (OB_NEED_RETRY == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("check_clog_served failed", KR(ret), K(snapshot_scn_val), KPC(this));
      }
    }

    if (OB_SUCC(ret) && OB_UNLIKELY(! is_clog_served)) {
      if (get_timestamp() >= end_ts) {
        ret = OB_TIMEOUT;
      } else if (OB_UNLIKELY(need_retry)) {
        // refresh tenant checkpoint
        // only retry when snapshot_scn_val >= replayable_scn and <= recovery_until_scn, which means obcdc can fetch clog if standby tenent continue sync and replay log
        ob_usleep(100 * _MSEC_);

        if (OB_FAIL(refresh(timeout))) {
          if (OB_NEED_RETRY == ret) {
            LOG_WARN("refresh tenant_checkpoint failed", KR(ret), KPC(this), K(snapshot_scn_val), K(timeout));
            ret = OB_SUCCESS;
          } else {
            LOG_ERROR("refresh tenant_checkpoint failed", KR(ret), KPC(this), K(snapshot_scn_val), K(timeout));
          }
        }

        if (TC_REACH_TIME_INTERVAL(10 * _SEC_)) {
          LOG_INFO("[WAIT_UNTIL_CLOG_SERVE] please check tenant clog sync status and replay status",
              K(snapshot_scn_val), KPC(this), "start_ts", TVAL_TO_STR(start_ts), "timeout", TVAL_TO_STR(timeout));
        }
      } // end refresh
    }
  } // end while

  return ret;
}

const char* TenantCheckpointQueryer::QUERY_SQL_FORMAT = "SELECT %s "
    "FROM %s T LEFT JOIN %s R ON T.TENANT_ID = R.RESTORE_TENANT_ID AND R.STATUS = 'SUCCESS' WHERE T.TENANT_ID = %lu";

int TenantCheckpointQueryer::get_tenant_checkpoint(TenantCheckpoint &tenant_checkpoint, const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = tenant_checkpoint.get_tenant_id();
  TenantCheckpoint tmp_tenant_checkpoint(query_tenant_id_);
  ObCDCQueryResult<TenantCheckpoint> query_result(tmp_tenant_checkpoint);
  // execute query in sys_tenant(don't need change tenant to user_tenant) on online_refresh mode because can't query DBA_OB_TENANTS if switch to oracle tenant
  // execute query in current_tenant on data_dict mode(don't need change tenant)
  // thus force exec_tenant_id = OB_SYS_TENANT_ID
  const uint64_t exec_tenant_id = OB_SYS_TENANT_ID;

  if (OB_FAIL(query(exec_tenant_id, query_result, timeout))) {
    LOG_ERROR("qeury tenant_checkpoint failed", KR(ret), K(tenant_id), K_(query_tenant_id), K(exec_tenant_id), K(timeout));
  } else if (query_result.is_empty()) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("can't find tenant_checkpoint record", KR(ret), K(tenant_id), K_(query_tenant_id), K(exec_tenant_id), K(timeout));
  } else if (OB_FAIL(tenant_checkpoint.assign(tmp_tenant_checkpoint))) {
    LOG_ERROR("assign tenant_checkpoint failed", KR(ret),
      "target_checkpoint", tmp_tenant_checkpoint, "current_checkpoint", tenant_checkpoint);
  } else {
    LOG_DEBUG("get_data_dict_in_log_info succ", K(tenant_id), K(tmp_tenant_checkpoint), K(timeout));
  }

  return ret;
}

int TenantCheckpointQueryer::build_sql_statement_(const uint64_t exec_tenant_id, ObSqlString &sql)
{
  int ret = OB_SUCCESS;

  const char *select_field_from_420 =
      "T.TENANT_ID as \"tenant_id\", T.TENANT_NAME as \"tenant_name\", R.RESTORE_SCN as \"restore_scn\" ";
  const char *select_field_from_421 =
      "T.TENANT_ID as \"tenant_id\", T.TENANT_NAME as \"tenant_name\", T.TENANT_ROLE as \"tenant_role\", T.SYNC_SCN as \"sync_scn\", "
      "T.REPLAYABLE_SCN as \"replayable_scn\", T.READABLE_SCN as \"readable_scn\", T.RECOVERY_UNTIL_SCN as \"recovery_until_scn\", R.RESTORE_SCN as \"restore_scn\" ";

  // OB version before 410 doesn't support show tenant_role in DBA_OB_TENANTS view;
  const char *select_field = IS_CLUSTER_VERSION_BEFORE_4_1_0_0 ? select_field_from_420 : select_field_from_421;

  if (OB_FAIL(sql.assign_fmt(QUERY_SQL_FORMAT, select_field, share::OB_DBA_OB_TENANTS_TNAME, share::OB_DBA_OB_RESTORE_HISTORY_TNAME, query_tenant_id_))) {
    LOG_ERROR("sql assign_fmt fail", KR(ret), K(exec_tenant_id), K(sql));
  } else {
    LOG_INFO("build_sql_statement_ succ", K(sql), K_(query_tenant_id), K(exec_tenant_id));
  }

  return ret;
}

int TenantCheckpointQueryer::parse_row_(common::sqlclient::ObMySQLResult &sql_result, ObCDCQueryResult<TenantCheckpoint> &result)
{
  int ret = OB_SUCCESS;
  result.get_data().reset_data();
  common::ObString tenant_name;
  const bool skip_null_val = true;
  const bool skip_col_err = true;

  EXTRACT_VARCHAR_FIELD_MYSQL(sql_result, "tenant_name", tenant_name);
  EXTRACT_UINT_FIELD_TO_CLASS_MYSQL_WITH_DEFAULT_VALUE(sql_result,
      restore_scn, result.get_data(), uint64_t, skip_null_val, skip_col_err, OB_MIN_SCN_TS_NS);

  if (IS_CLUSTER_VERSION_BEFORE_4_1_0_0) {
    // always set tenant_role to PRIMARY_TENANT_ROLE for ob_version before 410
    result.get_data().set_tenant_role(share::PRIMARY_TENANT_ROLE);
    result.get_data().set_readable_scn(OB_MAX_SCN_TS_NS);
    result.get_data().set_replayable_scn(OB_MAX_SCN_TS_NS);
    result.get_data().set_sync_scn(OB_MAX_SCN_TS_NS);
    result.get_data().set_recovery_until_scn(OB_MAX_SCN_TS_NS);
  } else {
    common::ObString tenant_role_str;
    EXTRACT_VARCHAR_FIELD_MYSQL(sql_result, "tenant_role", tenant_role_str);
    share::ObTenantRole tenant_role(tenant_role_str);
    result.get_data().set_tenant_role(tenant_role);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(sql_result, readable_scn, result.get_data(), uint64_t);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(sql_result, replayable_scn, result.get_data(), uint64_t);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(sql_result, sync_scn, result.get_data(), uint64_t);
    EXTRACT_UINT_FIELD_TO_CLASS_MYSQL(sql_result, recovery_until_scn, result.get_data(), uint64_t);
  }

  LOG_INFO("parse_record for tenant checkpoint finish", KR(ret), K(tenant_name), K(result), "tenant_checkpoint", result.get_data());

  return ret;
}


} // end namespace libobcdc
} // end namespace oceanbase

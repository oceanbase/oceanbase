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

#include "share/ob_tenant_info_proxy.h"
#include "share/ob_cluster_role.h"//ObClusterTYPE
#include "share/ob_share_util.h"//ObShareUtil
#include "share/config/ob_server_config.h"//GCONF
#include "share/inner_table/ob_inner_table_schema.h"//ALL_TENANT_INFO_TNAME
#include "share/ls/ob_ls_i_life_manager.h"//TODO SCN VALUE
#include "lib/string/ob_sql_string.h"//ObSqlString
#include "lib/mysqlclient/ob_mysql_transaction.h"//ObMySQLTrans
#include "common/ob_timeout_ctx.h"//ObTimeoutCtx

using namespace oceanbase;
using namespace oceanbase::common;
namespace oceanbase
{
namespace share
{
////////////ObAllTenantInfo
bool ObAllTenantInfo::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
         && sync_scn_.is_valid()
         && (sync_scn_ != SCN::min_scn())
         && replayable_scn_.is_valid()
         && (replayable_scn_ != SCN::min_scn())
         && standby_scn_.is_valid()
         && (standby_scn_ != SCN::min_scn())
         && recovery_until_scn_.is_valid()
         && (recovery_until_scn_ != SCN::min_scn())
         && tenant_role_.is_valid();
}

const SCN ObAllTenantInfo::get_ref_scn() const
{
  SCN ref_scn;
  if (!sync_scn_.is_valid()) {
    LOG_WARN("sync scn is invalid", K(sync_scn_));
  } else {
    ref_scn = sync_scn_;
  }
  return ref_scn;
}


int ObAllTenantInfo::init(const uint64_t tenant_id, const ObTenantRole tenant_role)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_role.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_role));
  } else {
    tenant_id_ = tenant_id;
    tenant_role_ = tenant_role;
    switchover_status_ = ObTenantSwitchoverStatus::NORMAL_STATUS;
    switchover_epoch_ = 0;
    sync_scn_.set_base();
    replayable_scn_.set_base();
    standby_scn_.set_base();
    recovery_until_scn_.set_max();
  }
  return ret;
}

int ObAllTenantInfo::init(
    const uint64_t tenant_id, const ObTenantRole &tenant_role, const ObTenantSwitchoverStatus &switchover_status,
    int64_t switchover_epoch, const SCN &sync_scn, const SCN &replayable_scn,
    const SCN &standby_scn, const SCN &recovery_until_scn)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
                  || !tenant_role.is_valid()
                  || !switchover_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(tenant_role), K(switchover_status));
  } else {
    tenant_id_ = tenant_id;
    tenant_role_ = tenant_role;
    switchover_status_ = switchover_status;
    switchover_epoch_ = switchover_epoch;
    sync_scn_ = sync_scn;
    replayable_scn_ = replayable_scn;
    standby_scn_ = standby_scn;
    recovery_until_scn_ = recovery_until_scn;
  
  }
  return ret;
}
 
int ObAllTenantInfo::assign(const ObAllTenantInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    tenant_role_ = other.tenant_role_;
    switchover_status_ = other.switchover_status_;
    switchover_epoch_ = other.switchover_epoch_;
    sync_scn_ = other.sync_scn_;
    replayable_scn_ = other.replayable_scn_;
    standby_scn_ = other.standby_scn_;
    recovery_until_scn_ = other.recovery_until_scn_;
  }
  return ret;
}

void ObAllTenantInfo::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_role_ = ObTenantRole::INVALID_TENANT;
  switchover_status_ = ObTenantSwitchoverStatus::INVALID_STATUS;
  switchover_epoch_ = OB_INVALID_VERSION;
  sync_scn_.set_min();
  replayable_scn_.set_min();
  standby_scn_.set_min() ;
  recovery_until_scn_.set_min();
}
OB_SERIALIZE_MEMBER(ObAllTenantInfo, tenant_id_, tenant_role_,
                    switchover_status_, switchover_epoch_, sync_scn_,
                    replayable_scn_, standby_scn_, recovery_until_scn_);

ObAllTenantInfo& ObAllTenantInfo::operator= (const ObAllTenantInfo &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(assign(other))) {
      LOG_ERROR("failed to assign", KR(ret), K(other));
    }
  }
  return *this;
}

////////////ObAllTenantInfoProxy
int ObAllTenantInfoProxy::init_tenant_info(
    const ObAllTenantInfo &tenant_info,
    ObISQLClient *proxy)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_info.get_tenant_id());
  ObSqlString sql;
  int64_t affected_rows = 0;
  if (OB_UNLIKELY(!tenant_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_info));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (!is_user_tenant(tenant_info.get_tenant_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta tenant no need init tenant info", KR(ret), K(tenant_info));
  } else if (OB_FAIL(sql.assign_fmt(
                 "insert into %s (tenant_id, tenant_role, "
                 "switchover_status, switchover_epoch, "
                 "sync_scn, replayable_scn, readable_scn) "
                 "values(%lu, '%s', '%s', %ld, %lu, %lu, %lu)",
                 OB_ALL_TENANT_INFO_TNAME, tenant_info.get_tenant_id(),
                 tenant_info.get_tenant_role().to_str(),
                 tenant_info.get_switchover_status().to_str(),
                 tenant_info.get_switchover_epoch(),
                 tenant_info.get_sync_scn().get_val_for_inner_table_field(),
                 tenant_info.get_replayable_scn().get_val_for_inner_table_field(),
                 tenant_info.get_standby_scn().get_val_for_inner_table_field()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_info), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObAllTenantInfoProxy::load_tenant_info(const uint64_t tenant_id,
                                           ObISQLClient *proxy,
                                           const bool for_update,
                                           ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (!is_user_tenant(tenant_id)) {
    //sys and meta tenant is primary
    if (OB_FAIL(tenant_info.init(tenant_id, share::PRIMARY_TENANT_ROLE))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id));
    }
  } else {
    ObSqlString sql;
    uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
    if (OB_FAIL(sql.assign_fmt("select * from %s where tenant_id = %lu ",
                   OB_ALL_TENANT_INFO_TNAME, tenant_id))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else if(for_update && OB_FAIL(sql.append(" for update"))) {
      LOG_WARN("failed to assign sql", KR(ret), K(sql));
    } else {
      HEAP_VAR(ObMySQLProxy::MySQLResult, res) {
        common::sqlclient::ObMySQLResult *result = NULL;
        if (OB_FAIL(proxy->read(res, exec_tenant_id, sql.ptr()))) {
          LOG_WARN("failed to read", KR(ret), K(exec_tenant_id), K(sql));
        } else if (OB_ISNULL(result = res.get_result())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get sql result", KR(ret));
        } else if (OB_FAIL(result->next())) {
          LOG_WARN("failed to get tenant info", KR(ret), K(sql));
        } else if (OB_FAIL(fill_cell(result, tenant_info))) {
          LOG_WARN("failed to fill cell", KR(ret), K(sql));
        }
      }
    }//end else
  }
  return ret;
}

int ObAllTenantInfoProxy::update_tenant_recovery_status(
    const uint64_t tenant_id, ObMySQLProxy *proxy,
    ObTenantSwitchoverStatus status, const SCN &sync_scn,
    const SCN &replay_scn, const SCN &readable_scn)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  common::ObMySQLTransaction trans;
  ObAllTenantInfo old_tenant_info;
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id ||
                  !status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id), K(status));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (!is_user_tenant(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("meta tenant no need init tenant info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(trans.start(proxy, exec_tenant_id))) {
    LOG_WARN("failed to start trans", KR(ret), K(exec_tenant_id));
  } else if (OB_FAIL(load_tenant_info(tenant_id, &trans, true, old_tenant_info))) {
    LOG_WARN("failed to load all tenant info", KR(ret), K(tenant_id));
  } else {
    const SCN new_sync_scn = old_tenant_info.get_sync_scn() > sync_scn ? old_tenant_info.get_sync_scn() : sync_scn;

    SCN new_replay_scn_tmp = old_tenant_info.get_replayable_scn() > replay_scn ? old_tenant_info.get_replayable_scn() : replay_scn;
    SCN new_replay_scn = new_replay_scn_tmp < new_sync_scn ? new_replay_scn_tmp : new_sync_scn;

    SCN new_sts_tmp = old_tenant_info.get_standby_scn() > readable_scn ? old_tenant_info.get_standby_scn() : readable_scn;
    SCN new_sts = new_sts_tmp < new_replay_scn ? new_sts_tmp : new_replay_scn;

    if (old_tenant_info.get_sync_scn() == new_sync_scn
        && old_tenant_info.get_replayable_scn() == new_replay_scn
        && old_tenant_info.get_standby_scn() == new_sts) {
      LOG_DEBUG("no need update", K(old_tenant_info), K(new_sync_scn), K(new_replay_scn), K(new_sts));
    } else if (OB_FAIL(sql.assign_fmt(
                 "update %s set sync_scn = %lu, replayable_scn = %lu, "
                 "readable_scn = %lu where tenant_id = %lu "
                 "and switchover_status = '%s'", OB_ALL_TENANT_INFO_TNAME,
                 new_sync_scn.get_val_for_inner_table_field(),
                 new_replay_scn.get_val_for_inner_table_field(),
                 new_sts.get_val_for_inner_table_field(),
                 tenant_id, status.to_str()))) {
      LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(status),
               K(sql));
    } else if (OB_FAIL(trans.write(exec_tenant_id, sql.ptr(), affected_rows))) {
      LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
    } else if (!is_single_row(affected_rows)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
    }
  }
  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = trans.end(OB_SUCC(ret)))) {
      LOG_WARN("failed to commit trans", KR(ret), KR(tmp_ret));
      ret = OB_SUCC(ret) ? tmp_ret : ret;
    }
  }
  return ret; 
}

int ObAllTenantInfoProxy::fill_cell(common::sqlclient::ObMySQLResult *result, ObAllTenantInfo &tenant_info)
{
  int ret = OB_SUCCESS;
  tenant_info.reset();
  if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("result is null", KR(ret));
  } else {
    ObString tenant_role_str;
    ObString status_str;
    uint64_t tenant_id = OB_INVALID_TENANT_ID;
    int64_t switchover_epoch = OB_INVALID_TIMESTAMP;
    int64_t sync_ts = OB_INVALID_SCN_VAL;
    int64_t replay_ts = OB_INVALID_SCN_VAL;
    int64_t sts = OB_INVALID_SCN_VAL;
    SCN sync_scn;
    SCN replay_scn;
    SCN sts_scn;
    SCN recovery_scn;
    recovery_scn.set_base();
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "tenant_role", tenant_role_str);
    EXTRACT_VARCHAR_FIELD_MYSQL(*result, "switchover_status", status_str);
    EXTRACT_INT_FIELD_MYSQL(*result, "tenant_id", tenant_id, uint64_t);
    EXTRACT_INT_FIELD_MYSQL(*result, "switchover_epoch", switchover_epoch, int64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "sync_scn", sync_ts, int64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "replayable_scn", replay_ts, int64_t);
    EXTRACT_UINT_FIELD_MYSQL(*result, "readable_scn", sts, int64_t);
    ObTenantRole tmp_tenant_role(tenant_role_str);
    ObTenantSwitchoverStatus tmp_tenant_sw_status(status_str);
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to get result", KR(ret));
      //tenant_id in inner table can be used directly
    } else if (OB_FAIL(sync_scn.convert_for_inner_table_field(sync_ts))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sync_ts));
    } else if (OB_FAIL(replay_scn.convert_for_inner_table_field(replay_ts))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(replay_ts));
    } else if (OB_FAIL(sts_scn.convert_for_inner_table_field(sts))) {
      LOG_WARN("failed to convert_for_inner_table_field", KR(ret), K(sts));
    } else if (OB_FAIL(tenant_info.init(
            tenant_id, tmp_tenant_role,
            tmp_tenant_sw_status, switchover_epoch,
            sync_scn, replay_scn, sts_scn, recovery_scn))) {
      LOG_WARN("failed to init tenant info", KR(ret), K(tenant_id), K(tmp_tenant_role), K(tenant_role_str),
          K(tmp_tenant_sw_status), K(status_str), K(switchover_epoch), K(sync_ts));
    }
  }
  return ret;
}

int ObAllTenantInfoProxy::update_tenant_role(
    const uint64_t tenant_id,
    ObISQLClient *proxy,
    int64_t old_switchover_epoch,
    const ObTenantRole &new_role, const ObTenantSwitchoverStatus &status,
    int64_t &new_switchover_ts)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  //update switchover epoch while role change
  new_switchover_ts = max(old_switchover_epoch + 1, ObTimeUtility::current_time());
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
    || OB_INVALID_VERSION == new_switchover_ts
    || !new_role.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id), K(old_switchover_epoch), K(new_role));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(sql.assign_fmt(
          "update %s set tenant_role = '%s', switchover_status = '%s', switchover_epoch = %ld "
          "where tenant_id = %lu and switchover_epoch = %ld",
          OB_ALL_TENANT_INFO_TNAME,
          new_role.to_str(), status.to_str(),
          new_switchover_ts, tenant_id, old_switchover_epoch))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(old_switchover_epoch),
             K(new_role), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover may concurrency, need retry", KR(ret), K(old_switchover_epoch));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

int ObAllTenantInfoProxy::update_tenant_switchover_status(
    const uint64_t tenant_id,
    ObISQLClient *proxy,
    int64_t switchover_epoch,
    const ObTenantSwitchoverStatus &old_status, const ObTenantSwitchoverStatus &status)
{
  int ret = OB_SUCCESS;
  const uint64_t exec_tenant_id = gen_meta_tenant_id(tenant_id);
  ObSqlString sql;
  int64_t affected_rows = 0;
  //update switchover epoch while role change
  if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id
    || OB_INVALID_VERSION == switchover_epoch
    || old_status == status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("tenant_info is invalid", KR(ret), K(tenant_id),
            K(switchover_epoch), K(old_status), K(status));
  } else if (OB_ISNULL(proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("proxy is null", KR(ret), KP(proxy));
  } else if (OB_FAIL(sql.assign_fmt(
          "update %s set switchover_status = '%s'"
          "where tenant_id = %lu and switchover_epoch = %ld and switchover_status = '%s'",
          OB_ALL_TENANT_INFO_TNAME,
          status.to_str(),
          tenant_id, switchover_epoch, old_status.to_str()))) {
    LOG_WARN("failed to assign sql", KR(ret), K(tenant_id), K(switchover_epoch),
             K(status), K(old_status), K(sql));
  } else if (OB_FAIL(proxy->write(exec_tenant_id, sql.ptr(), affected_rows))) {
    LOG_WARN("failed to execute sql", KR(ret), K(exec_tenant_id), K(sql));
  } else if (0 == affected_rows) {
    ret = OB_NEED_RETRY;
    LOG_WARN("switchover may concurrency, need retry", KR(ret), K(switchover_epoch), K(sql));
  } else if (!is_single_row(affected_rows)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expect updating one row", KR(ret), K(affected_rows), K(sql));
  }
  return ret;
}

}
}


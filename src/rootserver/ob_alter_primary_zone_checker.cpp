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
#define USING_LOG_PREFIX RS

#include "ob_alter_primary_zone_checker.h"

#include "ob_balance_ls_primary_zone.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
ObAlterPrimaryZoneChecker::ObAlterPrimaryZoneChecker(volatile bool &is_stopped)
    :is_stopped_(is_stopped),
    is_inited_(false),
    schema_service_(NULL)
{
}
ObAlterPrimaryZoneChecker::~ObAlterPrimaryZoneChecker()
{
}
int ObAlterPrimaryZoneChecker::init(share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(is_inited_));
  } else {
    schema_service_ = &schema_service;
    is_inited_ = true;
  }
  return ret;
}

int ObAlterPrimaryZoneChecker::check()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start check primary zone");
  ObArray<uint64_t> tenant_ids;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterPrimaryZoneChecker not init", KR(ret), K(is_inited_));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service_ is null", KR(ret), KP(schema_service_));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObAlterPrimaryZoneChecker stop", KR(ret));
  } else if (OB_FAIL(ObTenantUtils::get_tenant_ids(schema_service_, tenant_ids))) {
    LOG_WARN("fail to get tenant id array", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.count(); ++i) {
      // for each tenant, check if it has rs job ALTER_TENANT_PRIMARY_ZONE
      // if yes, check whether ls is balanced
      // **TODO(linqiucen.lqc): check __all_ls_election_reference_info
      const uint64_t tenant_id = tenant_ids.at(i);
      if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant_id", KR(ret), K(tenant_id));
      } else if (is_meta_tenant(tenant_id)) {
        // skip
      } else {
        ObCurTraceId::init(GCONF.self_addr_);
        LOG_INFO("start check primary zone", K(tenant_id));
        DEBUG_SYNC(BEFORE_CHECK_PRIMARY_ZONE);
        if (OB_TMP_FAIL(check_primary_zone_for_each_tenant_(tenant_id))) {
          LOG_WARN("fail to execute check_primary_zone_for_each_tenant", KR(ret), KR(tmp_ret), K(tenant_id));
        }
      }
    }
  }
  return ret;
}

int ObAlterPrimaryZoneChecker::create_alter_tenant_primary_zone_rs_job_if_needed(
    const obrpc::ObModifyTenantArg &arg,
    const uint64_t tenant_id,
    const share::schema::ObTenantSchema &orig_tenant_schema,
    const share::schema::ObTenantSchema &new_tenant_schema,
    ObMySQLTransaction &trans)
{
  int ret = OB_SUCCESS;
  ObArray<ObZone> orig_first_primary_zone;
  ObArray<ObZone> new_first_primary_zone;
  bool is_primary_zone_changed = false;
  uint64_t tenant_data_version = 0;
  bool need_skip = false;
  if (!arg.alter_option_bitset_.has_member(obrpc::ObModifyTenantArg::PRIMARY_ZONE)) {
    need_skip = true;
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("fail to get min data version", KR(ret), K(tenant_id));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
    need_skip = true;
  } else {
    // step 1: cancel rs job ALTER_TENANT_PRIMARY_ZONE if exists
    int64_t job_id = 0;
    if(OB_FAIL(RS_JOB_FIND(
        ALTER_TENANT_PRIMARY_ZONE,
        job_id,
        trans,
        "tenant_id", tenant_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to find rs job ALTER_TENANT_PRIMARY_ZONE", KR(ret));
      }
    } else {
      ret = RS_JOB_COMPLETE(job_id, OB_CANCELED, trans);
      FLOG_INFO("[ALTER_TENANT_PRIMARY_ZONE NOTICE] cancel an old inprogress rs job", KR(ret),
          K(tenant_id), K(job_id));
    }
  }
  int64_t new_job_id = 0;
  if (OB_FAIL(ret) || need_skip) {
  } else if (OB_FAIL(ObRootUtils::is_first_priority_primary_zone_changed(
      orig_tenant_schema,
      new_tenant_schema,
      orig_first_primary_zone,
      new_first_primary_zone,
      is_primary_zone_changed))) {
    // all the necessary checks such as input validity are in this func
    LOG_WARN("fail to execute is_first_priority_primary_zone_changed", KR(ret),
        K(orig_tenant_schema), K(new_first_primary_zone));
  } else {
    // step 2: create a new rs job ALTER_TENANT_PRIMARY_ZONE
    const int64_t extra_info_len = common::MAX_ROOTSERVICE_EVENT_EXTRA_INFO_LENGTH;
    HEAP_VAR(char[extra_info_len], extra_info) {
      memset(extra_info, 0, extra_info_len);
      int64_t pos = 0;
      if (OB_FAIL(databuff_printf(extra_info, extra_info_len, pos,
              "FROM: '%.*s', TO: '%.*s'", orig_tenant_schema.get_primary_zone().length(),
              orig_tenant_schema.get_primary_zone().ptr(), new_tenant_schema.get_primary_zone().length(),
              new_tenant_schema.get_primary_zone().ptr()))) {
        LOG_WARN("format extra_info failed", KR(ret), K(orig_tenant_schema), K(new_tenant_schema));
      } else if (OB_FAIL(RS_JOB_CREATE_WITH_RET(
        new_job_id,
        JOB_TYPE_ALTER_TENANT_PRIMARY_ZONE,
        trans,
        "tenant_id", tenant_id,
        "tenant_name", new_tenant_schema.get_tenant_name(),
        "sql_text", ObHexEscapeSqlStr(arg.ddl_stmt_str_),
        "extra_info", ObHexEscapeSqlStr(extra_info)))) {
        LOG_WARN("failed to create new job", KR(ret), K(new_job_id), K(tenant_id),
            K(extra_info), K(new_tenant_schema), K(arg));
      }
    }
    FLOG_INFO("[ALTER_TENANT_PRIMARY_ZONE NOTICE] create a new rs job", KR(ret), K(arg), K(new_job_id));
    if (OB_SUCC(ret) && !is_primary_zone_changed) {
      // step 3: complete the rs job if the first priority primary zone is not changed
      //         otherwise wait for alter_primary_zone_checker to complete it
      ret = RS_JOB_COMPLETE(new_job_id, 0, trans);
      FLOG_INFO("[ALTER_TENANT_PRIMARY_ZONE NOTICE] no change of ls, complete immediately", KR(ret), K(new_job_id));
    }
  }
  return ret;
}

int ObAlterPrimaryZoneChecker::check_primary_zone_for_each_tenant_(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // step 1: find rs job ALTER_TENANT_PRIMARY_ZONE
  // step 2: check ls balance if rs job exists
  // step 3: complete the rs job if ls is balanced
  // if we only find the rs job at the committing period,
  // we do not know whether the job has been changed during checking process
  // e.g. job 1 is the rs job before checking,
  //      right after checking, job 2 is created and job 1 is canceled by job 2,
  //      then committing process will find job 2 and complete job 2 immediately,
  //      which means, job 2 is completed without checking.
  int64_t job_id = 0;
  int check_ret = OB_NEED_WAIT;
  if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("GCTX.sql_proxy_ is null", KR(ret), KP(GCTX.sql_proxy_));
  } else if (OB_FAIL(RS_JOB_FIND(
      ALTER_TENANT_PRIMARY_ZONE,
      job_id,
      *GCTX.sql_proxy_,
      "tenant_id", tenant_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to find rs job ALTER_TENANT_PRIMARY_ZONE", KR(ret), K(tenant_id));
    }
  } else if (is_sys_tenant(tenant_id)
      && OB_FAIL(ObBalanceLSPrimaryZone::check_sys_ls_primary_zone_balanced(tenant_id, check_ret))) {
    LOG_WARN("fail to execute check_sys_ls_primary_zone_balanced", KR(ret), K(tenant_id));
  } else if (is_user_tenant(tenant_id)
      && OB_FAIL(ObRootUtils::check_tenant_ls_balance(tenant_id, check_ret))) {
    LOG_WARN("fail to execute check_tenant_ls_balance", KR(ret), K(tenant_id));
  } else if (OB_NEED_WAIT != check_ret) {
    DEBUG_SYNC(BEFORE_FINISH_PRIMARY_ZONE);
    common::ObMySQLTransaction trans;
    if (OB_FAIL(check_stop())) {
      LOG_WARN("ObAlterPrimaryZoneChecker stop", KR(ret));
    } else if (OB_FAIL(RS_JOB_COMPLETE(job_id, check_ret, *GCTX.sql_proxy_))) {
      if (OB_EAGAIN == ret) {
        FLOG_WARN("[ALTER_TENANT_PRIMARY_ZONE NOTICE] the specified rs job might has been already "
            "completed due to a new job or deleted in table manually",
            KR(ret), K(tenant_id), K(job_id), K(check_ret));
        ret = OB_SUCCESS; // no need to return error code
      } else {
        LOG_WARN("fail to complete rs job", KR(ret), K(tenant_id), K(job_id), K(check_ret));
      }
    } else {
      FLOG_INFO("[ALTER_TENANT_PRIMARY_ZONE NOTICE] complete an inprogress rs job", KR(ret),
          K(tenant_id), K(job_id), K(check_ret));
    }
  }
  return ret;
}

int ObAlterPrimaryZoneChecker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (is_stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("ObAlterPrimaryZoneChecker stopped", KR(ret), K(is_stopped_));
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase

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

#include "ob_alter_locality_finish_checker.h"
#include "share/ob_errno.h"                          // for KR(ret)
#include "share/ob_ls_id.h"                          // for ls_id
#include "share/ls/ob_ls_table_operator.h"           // for ObLSTableOperator
#include "rootserver/ob_disaster_recovery_worker.h"  // for ObDRWorker LocalityMap
#include "rootserver/ob_disaster_recovery_info.h"    // for DRLSInfo
#include "share/schema/ob_schema_mgr.h"              // for SimpleTenantSchema

namespace oceanbase
{
using namespace common;
using namespace share;
namespace rootserver
{
OB_SERIALIZE_MEMBER((ObCommitAlterTenantLocalityArg, ObDDLArg), tenant_id_, rs_job_id_, rs_job_check_ret_);

ObAlterLocalityFinishChecker::ObAlterLocalityFinishChecker(volatile bool &stop)
  : inited_(false),
    schema_service_(NULL),
    common_rpc_proxy_(NULL),
    self_(),
    unit_mgr_(NULL),
    zone_mgr_(NULL),
    sql_proxy_(NULL),
    stop_(stop)
{
}

ObAlterLocalityFinishChecker::~ObAlterLocalityFinishChecker()
{
}

int ObAlterLocalityFinishChecker::init(
    share::schema::ObMultiVersionSchemaService &schema_service,
    obrpc::ObCommonRpcProxy &common_rpc_proxy,
    common::ObAddr &addr,
    ObUnitManager &unit_mgr,
    ObZoneManager &zone_mgr,
    common::ObMySQLProxy &sql_proxy,
    share::ObLSTableOperator &lst_operator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(addr));
  } else {
    schema_service_ = &schema_service;
    common_rpc_proxy_ = &common_rpc_proxy;
    self_ = addr;
    unit_mgr_ = &unit_mgr;
    zone_mgr_ = &zone_mgr;
    sql_proxy_ = &sql_proxy;
    lst_operator_ = &lst_operator;
    inited_ = true;
  }
  return ret;
}

int ObAlterLocalityFinishChecker::check()
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  ObArray<const ObSimpleTenantSchema *> tenant_schemas;
  LOG_INFO("start to check alter locality finish");
  //STEP 0: previous check
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObAlterLocalityFinishChecker not init", KR(ret));
  } else if (OB_ISNULL(schema_service_)
             || OB_ISNULL(unit_mgr_)
             || OB_ISNULL(zone_mgr_)
             || !self_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP_(schema_service), KP_(unit_mgr), KP_(zone_mgr), K_(self));
  } else if (OB_FAIL(check_stop())) {
    LOG_WARN("ObAlterLocalityFinishChecker stopped", KR(ret));
  } else if (OB_FAIL(schema_service_->get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("fail to get sys tenant schema guard", KR(ret));
  //STEP 1: get all tenant schemas
  } else if (OB_FAIL(schema_guard.get_simple_tenant_schemas(tenant_schemas))) {
    LOG_WARN("fail to get tenant schemas", KR(ret));
  } else {
    //STEP 2: check each tenant whether finish alter locality
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_schemas.count(); ++i) {
      DEBUG_SYNC(BEFORE_CHECK_LOCALITY);
      bool alter_locality_finish = false;
      bool meta_alter_locality_finish = false;
      int check_ret = OB_NEED_WAIT;
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      int64_t job_id = 0;
      ObCurTraceId::init(GCONF.self_addr_);
      if (OB_ISNULL(tenant_schemas.at(i)) || OB_ISNULL(GCTX.sql_proxy_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema or GCTX.sql_proxy_ is null", KR(ret), "schema", tenant_schemas.at(i),
            KP(GCTX.sql_proxy_));
      } else if (!tenant_schemas.at(i)->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant schema", KR(ret), "schema", tenant_schemas.at(i));
      } else if (FALSE_IT(tenant_id = tenant_schemas.at(i)->get_tenant_id())) {
        // shall never be here
      } else if (is_meta_tenant(tenant_id)
          || tenant_schemas.at(i)->get_previous_locality_str().empty()) {
        continue;
      } else if (OB_FAIL(find_rs_job(tenant_id, job_id, *GCTX.sql_proxy_))) {
        // find the corresponding rs job at first, then check if we can complete it
        // if we only find the rs job at the committing period,
        // we do not know whether the job has been changed during checking process
        // e.g. job 1 is the rs job before checking,
        //      right after checking, job 2 is created and job 1 is canceled by job 2,
        //      then committing process will find job 2 and complete job 2 immediately,
        //      which means, job 2 is completed without checking.
        if (OB_ENTRY_NOT_EXIST == ret) {
          FLOG_WARN("[ALTER_TENANT_LOCALITY NOTICE] there exists locality changing without corresponding rs job",
              KR(ret), KPC(tenant_schemas.at(i)));
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to find rs job", KR(ret), K(tenant_id));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_SUCCESS != (tmp_ret = ObDRWorker::check_tenant_locality_match(
                         tenant_id,
                         *unit_mgr_,
                         *zone_mgr_,
                         alter_locality_finish))){
        LOG_WARN("fail to check tenant locality match", KR(tmp_ret), K(tenant_id), K(alter_locality_finish));
      } else if (is_user_tenant(tenant_id)
                 && OB_SUCCESS != (tmp_ret = ObDRWorker::check_tenant_locality_match(
                         gen_meta_tenant_id(tenant_id),
                         *unit_mgr_,
                         *zone_mgr_,
                         meta_alter_locality_finish))){
        LOG_WARN("fail to check tenant locality match", KR(tmp_ret), "meta_tenant_id",
                 gen_meta_tenant_id(tenant_id), K(meta_alter_locality_finish));
      } else if (OB_FAIL(ObRootUtils::check_tenant_ls_balance(tenant_id, check_ret))) {
        LOG_WARN("fail to execute check_tenant_ls_balance", KR(ret), K(tenant_id));
      } else if (alter_locality_finish
          && OB_NEED_WAIT != check_ret
          && (meta_alter_locality_finish || is_sys_tenant(tenant_id))) {
        DEBUG_SYNC(BEFORE_FINISH_LOCALITY);
        const int64_t timeout = GCONF.internal_sql_execute_timeout;  // 30s default
        rootserver::ObCommitAlterTenantLocalityArg arg;
        arg.tenant_id_ = tenant_id;
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        arg.rs_job_id_ = job_id;
        arg.rs_job_check_ret_ = check_ret;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("ObAlterLocalityFinishChecker stopped", KR(ret));
        } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy_->to(self_).timeout(timeout).commit_alter_tenant_locality(arg))) {
          LOG_WARN("fail to commit alter tenant locality", KR(tmp_ret), K(timeout), K(arg));
        }
      }
    }
  }
  return ret;
}

int ObAlterLocalityFinishChecker::find_rs_job(
    const uint64_t tenant_id,
    int64_t &job_id,
    ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(RS_JOB_FIND(
      ALTER_TENANT_LOCALITY,
      job_id,
      sql_proxy,
      "tenant_id", tenant_id))) {
  // good, find job
  } else if (OB_ENTRY_NOT_EXIST == ret && OB_SUCC(RS_JOB_FIND(
      ROLLBACK_ALTER_TENANT_LOCALITY,
      job_id,
      sql_proxy,
      "tenant_id", tenant_id))) {
  // good, find job
  }
  return ret;
}

int ObAlterLocalityFinishChecker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("ObAlterLocalityFinishChecker stopped", KR(ret), K(stop_));
  }
  return ret;
}
} // end namespace rootserver
} // end namespace oceanbase

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
OB_SERIALIZE_MEMBER((ObCommitAlterTenantLocalityArg, ObDDLArg), tenant_id_);

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
      bool alter_locality_finish = false;
      bool meta_alter_locality_finish = false;
      uint64_t tenant_id = OB_INVALID_TENANT_ID;
      if (OB_ISNULL(tenant_schemas.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tenant schema is null", KR(ret), "schema", tenant_schemas.at(i));
      } else if (!tenant_schemas.at(i)->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tenant schema", KR(ret), "schema", tenant_schemas.at(i));
      } else if (FALSE_IT(tenant_id = tenant_schemas.at(i)->get_tenant_id())) {
        // shall never be here
      } else if (is_meta_tenant(tenant_id)) {
        // by pass, no need to check meta tenant, because it will be checked under user tenant
      } else if (tenant_schemas.at(i)->get_previous_locality_str().empty()) {
        // by pass, no locality alteration for this tenant
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
      } else if (alter_locality_finish
                 && (meta_alter_locality_finish || is_sys_tenant(tenant_id))) {
        DEBUG_SYNC(BEFORE_FINISH_LOCALITY);
        rootserver::ObCommitAlterTenantLocalityArg arg;
        arg.tenant_id_ = tenant_id;
        arg.exec_tenant_id_ = OB_SYS_TENANT_ID;
        if (OB_FAIL(check_stop())) {
          LOG_WARN("ObAlterLocalityFinishChecker stopped", KR(ret));
        } else if (OB_SUCCESS != (tmp_ret = common_rpc_proxy_->to(self_).commit_alter_tenant_locality(arg))) {
          LOG_WARN("fail to commit alter tenant locality", KR(tmp_ret));
        }
      }
    }
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

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
#include "ob_rootservice_util_checker.h"

using namespace oceanbase;
using namespace oceanbase::rootserver;

ObRootServiceUtilChecker::ObRootServiceUtilChecker(volatile bool &stop)
  : inited_(false),
    stop_(stop),
    migrate_unit_finish_checker_(stop),
    alter_locality_finish_checker_(stop),
    shrink_expand_resource_pool_checker_(stop),
    alter_primary_zone_checker_(stop)
{
}

ObRootServiceUtilChecker::~ObRootServiceUtilChecker()
{
}

int ObRootServiceUtilChecker::init(
    ObUnitManager &unit_mgr,
    ObZoneManager &zone_mgr,
    obrpc::ObCommonRpcProxy &common_rpc_proxy,
    common::ObAddr &self,
    share::schema::ObMultiVersionSchemaService &schema_service,
    common::ObMySQLProxy &sql_proxy,
    share::ObLSTableOperator &lst_operator)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_FAIL(migrate_unit_finish_checker_.init(
          unit_mgr,
          zone_mgr,
          schema_service,
          sql_proxy,
          lst_operator))) {
    LOG_WARN("fail to init migrate unit finish checker", KR(ret));
  } else if (OB_FAIL(alter_locality_finish_checker_.init(
          schema_service,
          common_rpc_proxy,
          self,
          zone_mgr,
          sql_proxy,
          lst_operator))) {
    LOG_WARN("fail to init alter locality finish checker", KR(ret));
  } else if (OB_FAIL(shrink_expand_resource_pool_checker_.init(&schema_service,
                 &unit_mgr, lst_operator, sql_proxy))) {
    LOG_WARN("fail to init shrink resource pool", KR(ret));
  } else if (OB_FAIL(alter_primary_zone_checker_.init(schema_service))) {
    LOG_WARN("fail to init alter primary zone checker", KR(ret));
  }
  else {
    inited_ = true;
  }
  return ret;
}

int ObRootServiceUtilChecker::rootservice_util_check()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    // migrate unit finish checker
    if (OB_TMP_FAIL(migrate_unit_finish_checker_.check())) {
      LOG_WARN("fail to check migrate unit finish", KR(tmp_ret));
    }
    // alter locality finish checker
    if (OB_TMP_FAIL(alter_locality_finish_checker_.check())) {
      LOG_WARN("fail to check alter locality finish", KR(tmp_ret));
    }

    if (OB_TMP_FAIL(shrink_expand_resource_pool_checker_.check())) {
      LOG_WARN("fail to check shrink resource pool", KR(tmp_ret));
    }

    if (OB_TMP_FAIL(alter_primary_zone_checker_.check())) {
      LOG_WARN("fail to check alter primary zone", KR(tmp_ret));
    }
  }
  return ret;
}

int ObRootServiceUtilChecker::check_stop() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObRootServiceUtilChecker not init", KR(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
    LOG_WARN("ObRootServiceUtilChecker stopped", KR(ret), K(stop_));
  }
  return ret;
}

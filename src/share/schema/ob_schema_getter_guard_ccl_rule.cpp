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

#define USING_LOG_PREFIX SHARE_SCHEMA

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "sql/resolver/ob_schema_checker.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
using namespace common;
using namespace observer;

namespace share
{
namespace schema {
int ObSchemaGetterGuard::get_ccl_rule_with_name(
    const uint64_t tenant_id, const common::ObString &name,
    const ObCCLRuleSchema *&ccl_rule_schema) {
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ccl_rule_schema = NULL;
  ObNameCaseMode mode = OB_NAME_CASE_INVALID;
  const ObSimpleCCLRuleSchema *simple_ccl_rule_schema = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || name.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(name));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_name_case_mode(tenant_id, mode))) {
    LOG_WARN("fail to get_tenant_name_case_mode", K(ret), K(tenant_id));
  } else if (OB_NAME_CASE_INVALID == mode) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid case mode", K(ret), K(mode));
  } else if (OB_FAIL(mgr->ccl_rule_mgr_.get_schema_by_name(
                 tenant_id, mode, name, simple_ccl_rule_schema))) {
    LOG_WARN("get schema failed", KR(ret), K(tenant_id), K(name));
  } else if (NULL == simple_ccl_rule_schema) {
    LOG_INFO("ccl rule not exist", K(tenant_id), K(name));
  } else if (OB_FAIL(get_schema(
                 CCL_RULE_SCHEMA, simple_ccl_rule_schema->get_tenant_id(),
                 simple_ccl_rule_schema->get_ccl_rule_id(), ccl_rule_schema,
                 simple_ccl_rule_schema->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id),
             KPC(simple_ccl_rule_schema));
  } else if (OB_ISNULL(ccl_rule_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(ccl_rule_schema));
  } else {
    const_cast<ObCCLRuleSchema *>(ccl_rule_schema)
        ->set_name_case_mode(simple_ccl_rule_schema->get_name_case_mode());
  }
  return ret;
}

int ObSchemaGetterGuard::get_ccl_rule_with_ccl_rule_id(
    const uint64_t tenant_id, const uint64_t ccl_rule_id,
    const ObCCLRuleSchema *&ccl_rule_schema) {
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ccl_rule_schema = NULL;
  const ObSimpleCCLRuleSchema *simple_ccl_rule_schema = nullptr;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) ||
                         ccl_rule_id == common::OB_INVALID_ID)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id), K(ccl_rule_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(mgr->ccl_rule_mgr_.get_schema_by_id(
                 ccl_rule_id, simple_ccl_rule_schema))) {
    LOG_WARN("get schema failed", KR(ret), K(tenant_id), K(ccl_rule_id));
  } else if (NULL == simple_ccl_rule_schema) {
    LOG_INFO("ccl rule not exist", K(tenant_id), K(ccl_rule_id));
  } else if (OB_FAIL(get_schema(
                 CCL_RULE_SCHEMA, simple_ccl_rule_schema->get_tenant_id(),
                 simple_ccl_rule_schema->get_ccl_rule_id(), ccl_rule_schema,
                 simple_ccl_rule_schema->get_schema_version()))) {
    LOG_WARN("get outline schema failed", KR(ret), K(tenant_id),
             KPC(simple_ccl_rule_schema));
  } else if (OB_ISNULL(ccl_rule_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("NULL ptr", KR(ret), KP(ccl_rule_schema));
  } else {
    const_cast<ObCCLRuleSchema *>(ccl_rule_schema)
        ->set_name_case_mode(simple_ccl_rule_schema->get_name_case_mode());
  }
  return ret;
}

int ObSchemaGetterGuard::get_ccl_rule_infos(
    const uint64_t tenant_id, CclRuleContainsInfo contians_info,
    ObCCLRuleMgr::CCLRuleInfos *&ccl_rule_infos) {
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  ccl_rule_infos = NULL;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    ccl_rule_infos =
        const_cast<ObSchemaMgr *>(mgr)
            ->ccl_rule_mgr_.get_ccl_rule_belong_ccl_rule_infos(contians_info);
  }
  return ret;
}

int ObSchemaGetterGuard::get_ccl_rule_count(const uint64_t tenant_id,
                                            uint64_t &count) {
  int ret = OB_SUCCESS;
  const ObSchemaMgr *mgr = NULL;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_tenant_schema_guard(tenant_id))) {
    LOG_WARN("fail to check tenant schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(check_lazy_guard(tenant_id, mgr))) {
    LOG_WARN("fail to check lazy guard", KR(ret), K(tenant_id));
  } else {
    count = const_cast<ObSchemaMgr *>(mgr)->ccl_rule_mgr_.get_ccl_rule_count();
  }
  return ret;
}

} // end of namespace schema
} //end of namespace share
} //end of namespace oceanbase

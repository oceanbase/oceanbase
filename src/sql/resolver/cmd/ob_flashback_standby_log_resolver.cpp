/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_RESV

#include "sql/resolver/cmd/ob_flashback_standby_log_resolver.h"

#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "sql/session/ob_sql_session_info.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
using namespace share;
using namespace share::schema;
namespace sql
{
typedef ObAlterSystemResolverUtil Util;
int ObFlashbackStandbyLogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObFlashbackStandbyLogStmt *stmt = create_stmt<ObFlashbackStandbyLogStmt>();
  uint64_t target_tenant_id = OB_INVALID_TENANT_ID;
  if (OB_UNLIKELY(T_FLASHBACK_STANDBY_LOG != parse_tree.type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse node, type is not T_FLASHBACK_STANDBY_LOG", KR(ret), "type",
        get_type_name(parse_tree.type_));
  } else if (OB_ISNULL(stmt)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("create stmt fail", KR(ret));
  } else if (2 != parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_[0])
      || OB_ISNULL(session_info_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parse tree or session info", KR(ret), "num_child", parse_tree.num_child_,
        KP(parse_tree.children_[0]), KP(session_info_));
  } else if (OB_FAIL(Util::get_and_verify_tenant_name(
      parse_tree.children_[1],
      false, /* allow_sys_meta_tenant */
      session_info_->get_effective_tenant_id(),
      target_tenant_id,
      "Flashback Standby Log"))) {
    LOG_WARN("fail to execute get_and_verify_tenant_name", KR(ret),
        K(session_info_->get_effective_tenant_id()), KP(parse_tree.children_[1]));
  } else {
    SCN flashback_log_scn;
    uint64_t scn_val = parse_tree.children_[0]->value_;
    if (OB_FAIL(flashback_log_scn.convert_for_sql(scn_val))) {
      LOG_WARN("fail to convert scn", KR(ret), K(scn_val));
    } else if (OB_FAIL(stmt->get_arg().init(target_tenant_id, flashback_log_scn))) {
      LOG_WARN("fail to init ObFlashbackStandbyLogArg", KR(ret), K(target_tenant_id),
          K(flashback_log_scn));
    }
  }
  if (OB_SUCC(ret)) {
    stmt_ = stmt;
  }
  return ret;
}
} // sql
} // oceanbase
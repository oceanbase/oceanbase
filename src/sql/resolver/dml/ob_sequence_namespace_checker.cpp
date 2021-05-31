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

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/dml/ob_sequence_namespace_checker.h"
#include "lib/charset/ob_charset.h"
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/ob_resolver_define.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ob_stmt_resolver.h"
namespace oceanbase {
using namespace common;
namespace sql {

int ObSequenceNamespaceChecker::check_sequence_namespace(
    const ObQualifiedName& q_name, ObSynonymChecker& syn_checker, uint64_t& sequence_id)
{
  return check_sequence_namespace(q_name, syn_checker, params_.session_info_, params_.schema_checker_, sequence_id);
}

int ObSequenceNamespaceChecker::check_sequence_namespace(const ObQualifiedName& q_name, ObSynonymChecker& syn_checker,
    ObSQLSessionInfo* session_info, ObSchemaChecker* schema_checker, uint64_t& sequence_id)
{
  int ret = OB_NOT_IMPLEMENT;
  if (OB_ISNULL(schema_checker) || OB_ISNULL(session_info)) {
    ret = OB_NOT_INIT;
    LOG_WARN("schema checker is null", K(ret));
  } else if (!is_curr_or_next_val(q_name.col_name_)) {
    ret = OB_ERR_BAD_FIELD_ERROR;
  } else if (session_info->get_database_name().empty()) {
    ret = OB_ERR_NO_DB_SELECTED;
    LOG_WARN("No database selected", K(q_name), K(ret));
  } else {
    const ObString& database_name =
        q_name.database_name_.empty() ? session_info->get_database_name() : q_name.database_name_;
    const ObString& sequence_name = q_name.tbl_name_;
    const ObString& sequence_expr = q_name.col_name_;
    uint64_t tenant_id = session_info->get_effective_tenant_id();
    uint64_t database_id = OB_INVALID_ID;
    bool exist = false;
    if (OB_FAIL(schema_checker->get_database_id(tenant_id, database_name, database_id))) {
      LOG_WARN("failed to get database id", K(ret), K(tenant_id), K(database_name));
    } else if (OB_FAIL(check_sequence_with_synonym_recursively(
                   tenant_id, database_id, sequence_name, syn_checker, schema_checker, exist, sequence_id))) {
      LOG_WARN("fail recursively check sequence with name", K(q_name), K(database_name), K(ret));
    } else if (!exist) {
      ret = OB_ERR_BAD_FIELD_ERROR;
    } else if (0 != sequence_expr.case_compare("NEXTVAL") && 0 != sequence_expr.case_compare("CURRVAL")) {
      ret = OB_ERR_BAD_FIELD_ERROR;
    }
  }
  return ret;
}

int ObSequenceNamespaceChecker::check_sequence_with_synonym_recursively(const uint64_t tenant_id,
    const uint64_t database_id, const common::ObString& sequence_name, ObSynonymChecker& syn_checker,
    ObSchemaChecker* schema_checker, bool& exists, uint64_t& sequence_id)
{
  int ret = OB_SUCCESS;
  bool exist_with_synonym = false;
  ObString object_seq_name;
  uint64_t object_db_id;
  uint64_t synonym_id;
  if (OB_FAIL(
          schema_checker->check_sequence_exist_with_name(tenant_id, database_id, sequence_name, exists, sequence_id))) {
    LOG_WARN("failed to check sequence with name", K(ret), K(sequence_name), K(database_id));
  } else if (!exists) {  // check synonym
    if (OB_FAIL(schema_checker->get_synonym_schema(
            tenant_id, database_id, sequence_name, object_db_id, synonym_id, object_seq_name, exist_with_synonym))) {
      LOG_WARN("get synonym failed", K(ret), K(tenant_id), K(database_id), K(sequence_name));
    } else if (exist_with_synonym) {
      syn_checker.set_synonym(true);
      if (OB_FAIL(syn_checker.add_synonym_id(synonym_id))) {
        LOG_WARN("failed to add synonym id", K(ret));
      } else {
        if (OB_FAIL(check_sequence_with_synonym_recursively(
                tenant_id, object_db_id, object_seq_name, syn_checker, schema_checker, exists, sequence_id))) {
          LOG_WARN("failed to check sequence with synonym recursively", K(ret));
        }
      }
    } else {
      exists = false;
      LOG_INFO("sequence object does not exist", K(sequence_name), K(tenant_id), K(database_id));
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

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

#include "sql/resolver/ddl/ob_drop_mlog_resolver.h"
#include "sql/resolver/ddl/ob_drop_mlog_stmt.h"
#include "sql/resolver/ob_resolver_utils.h"
#include "sql/parser/ob_parser_utils.h"

namespace oceanbase
{
using namespace common;
using namespace obrpc;
namespace sql
{
/*
DROP MATERIALIZED VIEW LOG ON [ schema. ] table
;
*/
ObDropMLogResolver::ObDropMLogResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

int ObDropMLogResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ParseNode &parse_node = const_cast<ParseNode &>(parse_tree);
  ObDropMLogStmt *drop_mlog_stmt = nullptr;
  uint64_t compat_version = 0;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;

  if (OB_UNLIKELY(T_DROP_MLOG != parse_node.type_)
      || OB_UNLIKELY(ENUM_TOTAL_COUNT != parse_node.num_child_)
      || OB_ISNULL(parse_node.children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid parse tree", KR(ret));
  } else if (OB_ISNULL(session_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null session info", KR(ret), KP_(session_info));
  } else if (OB_FALSE_IT(tenant_id = session_info_->get_effective_tenant_id())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, compat_version))) {
    LOG_WARN("failed to get min data version", KR(ret), K(tenant_id));
  } else if (compat_version < DATA_VERSION_4_3_0_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("materialized view log before version 4.3 is not supported", KR(ret), K(compat_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "materialized view log before version 4.3 is");
  } else if (OB_ISNULL(drop_mlog_stmt = create_stmt<ObDropMLogStmt>())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create drop mlog stmt", KR(ret));
  } else {
    stmt_ = drop_mlog_stmt;
  }

  if (OB_SUCC(ret)) {
    bool has_synonym = false;
    ObString new_db_name;
    ObString new_tbl_name;
    ObString database_name;
    ObString data_table_name;
    ObString mlog_name;
    const ObTableSchema *data_table_schema = nullptr;
    const ObTableSchema *mlog_table_schema = nullptr;
    bool is_table_exist = false;
    // resolve data table name
    if (OB_ISNULL(parse_node.children_[ENUM_TABLE_NAME])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid parse tree", KR(ret));
    } else if (OB_FAIL(resolve_table_relation_node(parse_tree.children_[ENUM_TABLE_NAME],
                                                   data_table_name,
                                                   database_name))) {
      LOG_WARN("failed to resolve table name",
          KR(ret), K(data_table_name), K(database_name));
    } else if (OB_FAIL(schema_checker_->get_table_schema_with_synonym(
        session_info_->get_effective_tenant_id(),
        database_name,
        data_table_name,
        false/*is index table*/,
        has_synonym,
        new_db_name,
        new_tbl_name,
        data_table_schema))) {
      if (OB_TABLE_NOT_EXIST == ret) {
        LOG_USER_ERROR(OB_TABLE_NOT_EXIST, to_cstring(database_name), to_cstring(data_table_name));
        LOG_WARN("table not exist", KR(ret), K(database_name), K(data_table_name));
      } else {
        LOG_WARN("failed to get table schema", KR(ret));
      }
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data table schema is null", KR(ret));
    } else if (has_synonym) {
      ObString tmp_new_db_name;
      ObString tmp_new_tbl_name;
      // related issue :
      if (OB_FAIL(deep_copy_str(new_db_name, tmp_new_db_name))) {
        LOG_WARN("failed to deep copy new_db_name", KR(ret));
      } else if (OB_FAIL(deep_copy_str(new_tbl_name, tmp_new_tbl_name))) {
        LOG_WARN("failed to deep copy new_tbl_name", KR(ret));
      } else {
        database_name = tmp_new_db_name;
        data_table_name = tmp_new_tbl_name;
      }
    }

    if (OB_SUCC(ret)) {
      uint64_t mlog_table_id = data_table_schema->get_mlog_tid();
      if (!data_table_schema->has_mlog_table()) {
        ret = OB_ERR_TABLE_NO_MLOG;
        LOG_WARN("table has no materialized view log", KR(ret), K(mlog_table_id));
      } else if (OB_FAIL(schema_checker_->get_table_schema(tenant_id,
                                                           mlog_table_id,
                                                           mlog_table_schema,
                                                           false /*is_link*/))) {
        LOG_WARN("failed to get table schema", KR(ret), K(tenant_id), K(mlog_table_id));
      } else if (OB_ISNULL(mlog_table_schema)) {
        ret = OB_ERR_TABLE_NO_MLOG;
        LOG_WARN("mlog table schema is null", KR(ret));
      } else if(!mlog_table_schema->is_mlog_table()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table is not materialized view log",
            KR(ret), K(mlog_table_schema->get_table_type()));
      } else if (OB_FAIL(deep_copy_str(mlog_table_schema->get_table_name_str(), mlog_name))) {
        LOG_WARN("failed to deep copy mlog name", KR(ret));
      }
    }

    if (OB_SUCC(ret) && ObSchemaChecker::is_ora_priv_check()) {
      OZ (schema_checker_->check_ora_ddl_priv(
          tenant_id,
          session_info_->get_priv_user_id(),
          database_name,
          stmt::T_DROP_MLOG,
          session_info_->get_enable_role_array()),
          tenant_id, session_info_->get_user_id(), database_name);
    }

    if (OB_SUCC(ret)) {
      drop_mlog_stmt->set_table_name(data_table_name);
      drop_mlog_stmt->set_database_name(database_name);
      drop_mlog_stmt->set_tenant_id(tenant_id);
      drop_mlog_stmt->set_mlog_name(mlog_name);
      drop_mlog_stmt->set_index_action_type(ObIndexArg::IndexActionType::DROP_MLOG);
    }
  }

  return ret;
}
} // sql
} // oceanbase

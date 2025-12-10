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
#include "ob_ddl_resolver.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/ob_sql_utils.h"
#include "share/table/ob_ttl_util.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
using namespace share;
using namespace obrpc;
namespace sql
{

int ObDDLResolver::resolve_kv_attributes_option(uint64_t tenant_id, const ParseNode *option_node, bool is_index_option)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, tenant_data_version))) {
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_2_1_0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("kv attributes is not supported in data version less than 4.2.1", K(ret), K(tenant_data_version));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "kv attributes is not supported in data version less than 4.2.1");
  } else if (is_index_option) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("index option should not specify kv attributes", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "index option should not specify kv attributes");
  } else if (OB_ISNULL(option_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "(the children of option_node is null", K(option_node->children_), K(ret));
  } else if (OB_ISNULL(option_node->children_[0])) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(ERROR,"children can't be null", K(ret));
  } else {
    ObRawExpr *expr = NULL;
    ObString tmp_str;
    ObKVAttr attr; // used for check validity
    tmp_str.assign_ptr(const_cast<char *>(option_node->children_[0]->str_value_),
                          static_cast<int32_t>(option_node->children_[0]->str_len_));
    LOG_INFO("resolve kv attributes", K(tmp_str));
    if (OB_FAIL(ObSQLUtils::convert_sql_text_to_schema_for_storing(
                  *allocator_, session_info_->get_dtc_params(), tmp_str))) {
      LOG_WARN("fail to convert comment to utf8", K(ret));
    } else if (OB_FAIL(ObTTLUtil::parse_kv_attributes(tenant_id, tmp_str, attr))) {
      LOG_WARN("fail to parse kv attributes", K(ret));
    } else if (OB_FAIL(ob_write_string(*allocator_, tmp_str, kv_attributes_))) {
      SQL_RESV_LOG(WARN, "write string failed", K(ret));
    } else if (stmt::T_ALTER_TABLE == stmt_->get_stmt_type()) {
      if (attr.is_created_by_admin()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("alter table kv_attributes to created by admin is not supported", K(ret));
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "alter table kv attributes to created by admin");
      } else {
        const ObTableSchema *tbl_schema = nullptr;
        const ObTableSchema *index_schema = nullptr;
        ObAlterTableStmt *alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt_);
        if (OB_FAIL(get_table_schema_for_check(tbl_schema))) {
          LOG_WARN("get table schema failed", K(ret));
        } else if (OB_ISNULL(tbl_schema)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table schema is NULL", K(ret));
        } else if (OB_FAIL(ObTTLUtil::check_kv_attributes(attr, *tbl_schema, schema_checker_, &alter_table_stmt->get_alter_table_arg()))) {
          LOG_WARN("fail to check kv attributes", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(alter_table_bitset_.add_member(ObAlterTableArg::KV_ATTRIBUTES))) {
        SQL_RESV_LOG(WARN, "failed to add member to bitset!", K(ret));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

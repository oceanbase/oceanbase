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
#include "sql/resolver/ddl/ob_drop_func_resolver.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

ObDropFuncResolver::ObDropFuncResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObDropFuncResolver::~ObDropFuncResolver()
{
}

int ObDropFuncResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObDropFuncStmt *drop_func_stmt = NULL;
  ObCollationType cs_type;
  ObString lower_name;
  if (OB_ISNULL(session_info_)
      || OB_ISNULL(schema_checker_)
      || OB_ISNULL(allocator_)
      || (T_DROP_FUNC != parse_tree.type_)
      || 2 != parse_tree.num_child_
      || OB_ISNULL(parse_tree.children_)
      || (parse_tree.children_[0] != NULL && T_IF_EXISTS != parse_tree.children_[0]->type_)) {
    ret = OB_ERR_UNEXPECTED;
    SQL_RESV_LOG(WARN, "invalid parse tree!", K(ret));
  } else if (OB_FAIL(session_info_->get_collation_connection(cs_type))) {
    LOG_WARN("failed to get collation", K(ret));
  } else {
    ObString udf_name(parse_tree.children_[1]->str_len_, parse_tree.children_[1]->str_value_);
    if (OB_FAIL(ob_write_string(*allocator_, udf_name, lower_name))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("Malloc function name failed", K(ret));
    } else {
      ObCharset::casedn(CS_TYPE_UTF8MB4_GENERAL_CI, lower_name);
    }
  }

  if (OB_SUCC(ret)) {
    bool exist = false;
    const share::schema::ObUDF *udf_info = nullptr;
    uint64_t tenant_id = session_info_->get_effective_tenant_id();
    if (OB_FAIL(schema_checker_->get_udf_info(tenant_id, lower_name, udf_info, exist))) {
      LOG_WARN("failed to get udf info", K(ret));
    } else if (exist) {
      // dll udf
      if (OB_ISNULL(drop_func_stmt = create_stmt<ObDropFuncStmt>())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SQL_RESV_LOG(ERROR, "create drop func stmt failed");
      }
      //stmt_ = drop_func_stmt;
    } else {
      //pl udf
      ret = OB_ERR_FUNCTION_UNKNOWN;
    }
  }


  if (OB_SUCC(ret)) {
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    obrpc::ObDropUserDefinedFunctionArg &drop_func_arg = drop_func_stmt->get_drop_func_arg();
    drop_func_arg.tenant_id_ = tenant_id;
    drop_func_arg.name_ = lower_name;
    drop_func_arg.if_exist_ =  (NULL != parse_tree.children_[0]);
  }
  return ret;
}

}
}

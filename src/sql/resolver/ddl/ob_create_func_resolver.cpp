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
#include "sql/resolver/ddl/ob_create_func_resolver.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"

namespace oceanbase
{
using namespace share;
namespace sql
{

ObCreateFuncResolver::ObCreateFuncResolver(ObResolverParams &params)
    : ObDDLResolver(params)
{
}

ObCreateFuncResolver::~ObCreateFuncResolver()
{
}

int ObCreateFuncResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  UNUSED(parse_tree);
  ret = OB_NOT_SUPPORTED;
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "creating loadable function");
  return ret;

#if 0
  ParseNode *create_func_node = const_cast<ParseNode*>(&parse_tree);
  ObCreateFuncStmt *create_func_stmt = NULL;
  if (OB_ISNULL(create_func_node)
      || T_CREATE_FUNC != create_func_node->type_
      || 4 != create_func_node->num_child_
      || OB_ISNULL(create_func_node->children_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_RESV_LOG(WARN, "invalid argument.", K(ret));
  } else if (OB_ISNULL(create_func_node->children_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid node children", K(create_func_node), K(create_func_node->children_));
  } else if (OB_UNLIKELY(NULL == (create_func_stmt = create_stmt<ObCreateFuncStmt>()))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("failed to create create_func_stmt", K(ret));
  } else {
    stmt_ = create_func_stmt;
    const uint64_t tenant_id = session_info_->get_effective_tenant_id();
    obrpc::ObCreateUserDefinedFunctionArg &create_func_arg = create_func_stmt->get_create_func_arg();
    create_func_arg.udf_.set_tenant_id(tenant_id);
    int64_t dl_c_len = create_func_node->children_[3]->str_len_;
    const char* dl_c = create_func_node->children_[3]->str_value_;
    if (create_func_node->children_[0]->value_ == 2) {
      create_func_arg.udf_.set_type(schema::ObUDF::FUNCTION);
    } else if (create_func_node->children_[0]->value_ == 1) {
      create_func_arg.udf_.set_type(schema::ObUDF::AGGREGATE);
    }

    ObObj plugin_path;
    session_info_->get_sys_variable(SYS_VAR_PLUGIN_DIR, plugin_path);
    ObString plugin_path_str;
    if (OB_FAIL(plugin_path.get_string(plugin_path_str))) {
      LOG_WARN("get plugin path failed", K(ret));
    } else {
      create_func_arg.udf_.set_name(ObString(create_func_node->children_[1]->str_len_, create_func_node->children_[1]->str_value_));
      int64_t dl_len = plugin_path_str.length() + dl_c_len;
      char *dl_buf = (char*)alloca(sizeof(char)*(dl_len));
      MEMCPY(dl_buf, plugin_path_str.ptr(), plugin_path_str.length());
      MEMCPY(dl_buf + plugin_path_str.length(), dl_c, dl_c_len);
      ObString dl_str(dl_len, dl_buf);
      create_func_arg.udf_.set_dl(dl_str);
      switch (create_func_node->children_[2]->value_) {
        case 1:
          create_func_arg.udf_.set_ret(schema::ObUDF::STRING);
          break;
        case 2:
          create_func_arg.udf_.set_ret(schema::ObUDF::INTEGER);
          break;
        case 3:
          create_func_arg.udf_.set_ret(schema::ObUDF::REAL);
          break;
        case 4:
          create_func_arg.udf_.set_ret(schema::ObUDF::DECIMAL);
          break;
      }
    }

    if (OB_SUCC(ret)) {
      // verify the udf.
      const share::schema::ObUDF &udf = create_func_arg.udf_;
      ObUdfSoHandler dlhandle;
      ObUdfFuncAny func_origin;
      ObUdfFuncInit func_init;
      ObUdfFuncDeinit func_deinit;
      //helper function for aggregation udf
      ObUdfFuncClear func_clear;
      ObUdfFuncAdd func_add;
      if (udf.get_dl_str().empty() || udf.get_name_str().empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the udf meta is invalid", K(ret));
      } else if (OB_FAIL(ObUdfUtil::load_so(udf.get_dl_str(), dlhandle))) {
        LOG_WARN("load so error", K(ret));
      } else if (OB_FAIL(ObUdfUtil::load_function(udf.get_name_str(),
                                                  dlhandle,
                                                  ObString::make_string(""),
                                                  false, /* can't ignore error */
                                                  func_origin))) {
        // change the error code
        ret = OB_CANT_FIND_DL_ENTRY;
        LOG_WARN("load origin function failed", K(ret));
        LOG_USER_ERROR(OB_CANT_FIND_DL_ENTRY, udf.get_name_str().length(), udf.get_name_str().ptr());
      } else if (OB_FAIL(ObUdfUtil::load_function(udf.get_name_str(),
                                                  dlhandle,
                                                  ObString::make_string("_init"),
                                                  false, /* can't ignore error */
                                                  func_init))) {
        LOG_WARN("load init function failed", K(ret));
      } else if (OB_FAIL(ObUdfUtil::load_function(udf.get_name_str(),
                                                  dlhandle,
                                                  ObString::make_string("_deinit"),
                                                  true, /* ignore error */
                                                  func_deinit))) {
        LOG_WARN("load deinit function failed", K(ret));
      } else if (udf.get_type() == share::schema::ObUDF::UDFType::FUNCTION) {
        // do nothing
      } else if (OB_FAIL(ObUdfUtil::load_function(udf.get_name_str(),
                                                  dlhandle,
                                                  ObString::make_string("_clear"),
                                                  false, /* ignore error */
                                                  func_clear))) {
        LOG_WARN("load clear function error", K(ret));
      } else if (OB_FAIL(ObUdfUtil::load_function(udf.get_name_str(),
                                                  dlhandle,
                                                  ObString::make_string("_add"),
                                                  false, /* ignore error */
                                                  func_add))) {
        LOG_WARN("load add function error", K(ret));
      }
    }
  }
  return ret;
#endif
}

}
}


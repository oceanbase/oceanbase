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

#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#ifdef OB_BUILD_ORACLE_PL

#define USING_LOG_PREFIX PL

#include "lib/utility/ob_macro_utils.h"
#include "objit/common/ob_item_type.h"
#include "pl/wrap/ob_pl_wrap_allocator.h"
#include "pl/wrap/ob_pl_wrap_decoder.h"
#include "share/ob_define.h"
#include "share/ob_global_stat_proxy.h"
#include "sql/ob_sql_utils.h"
#include "sql/parser/ob_parser.h"
#include "sql/parser/parse_node.h"
#include "sql/resolver/ddl/ob_create_package_stmt.h"
#include "sql/resolver/ddl/ob_create_routine_stmt.h"
#include "sql/resolver/ddl/ob_create_udt_stmt.h"
#include "sql/resolver/ddl/ob_create_wrapped_resolver.h"

using namespace oceanbase::pl;

namespace oceanbase
{
namespace sql
{

#define CHECK_CIPHER_PARSE_TREE(parse_tree, type)                        \
  do {                                                                   \
    CK (OB_LIKELY(type == (parse_tree).type_));                          \
    CK (OB_LIKELY(2 == (parse_tree).num_child_));                        \
    CK (OB_NOT_NULL((parse_tree).children_));                            \
    CK (OB_NOT_NULL((parse_tree).children_[0]));                         \
    CK (OB_NOT_NULL((parse_tree).children_[1]));                         \
    CK (OB_LIKELY(T_SP_NAME == (parse_tree).children_[0]->type_));       \
    CK (OB_LIKELY(T_BASE64_CIPHER == (parse_tree).children_[1]->type_)); \
  } while (0)

#define CHECK_CREATE_DDL_PARSE_TREE(parse_tree)                               \
  do {                                                                        \
    /* T_STMT_LIST */                                                         \
    CK (OB_LIKELY(T_STMT_LIST == (parse_tree)->type_));                       \
    CK (OB_LIKELY(1 == (parse_tree)->num_child_));                            \
    CK (OB_NOT_NULL((parse_tree)->children_));                                \
    /* T_CREATE_XXX */                                                        \
    CK (OB_NOT_NULL((parse_tree)->children_[0]));                             \
    CK (OB_LIKELY(1 == (parse_tree)->children_[0]->num_child_));              \
    CK (OB_NOT_NULL((parse_tree)->children_[0]->children_));                  \
    /* T_XXX_SOURCE */                                                        \
    CK (OB_NOT_NULL((parse_tree)->children_[0]->children_[0]));               \
    CK (OB_NOT_NULL((parse_tree)->children_[0]->children_[0]->children_));    \
    /* T_SP_NAME */                                                           \
    CK (OB_NOT_NULL((parse_tree)->children_[0]->children_[0]->children_[0])); \
  } while (0)

int ObCreateWrappedResolver::resolve_base64_cipher(ObIAllocator& allocator,
                                                   const ObSQLSessionInfo& session,
                                                   const ParseNode& base64_cipher_node,
                                                   const bool is_or_replace,
                                                   const bool is_noneditionable,
                                                   ParseResult& plain_parse_result)
{
  int ret = OB_SUCCESS;
  uint8_t *out = nullptr;
  int64_t out_size = 0;
  char *splice_buf = nullptr;
  ObParser plain_ddl_parser(allocator, session.get_sql_mode());
  const char* prefix =
      is_or_replace ? is_noneditionable ? "CREATE OR REPLACE NONEDITIONABLE " : "CREATE OR REPLACE "
                    : is_noneditionable ? "CREATE NONEDITIONABLE " : "CREATE ";
  ObArenaAllocator arena_allocator("PLWrap", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObPLWAllocator wrap_allocator;
  OZ (init_plw_allocator(&wrap_allocator, &arena_allocator));
  ObPLWDecoder wrap_decoder(wrap_allocator);

  CK (OB_LIKELY(T_BASE64_CIPHER == base64_cipher_node.type_));
  OZ (wrap_decoder.decode(reinterpret_cast<const uint8_t*>(base64_cipher_node.str_value_),
                          base64_cipher_node.str_len_,
                          out,
                          out_size));
  CK (OB_NOT_NULL(splice_buf =
                      static_cast<char*>(arena_allocator.alloc(out_size + strlen(prefix) + 1))));
  CK (OB_NOT_NULL(out));
  OX (memset(splice_buf, 0, out_size + strlen(prefix) + 1));
  OX (memcpy(splice_buf, prefix, strlen(prefix)));
  OX (memcpy(splice_buf + strlen(prefix), out, out_size));
  OZ (plain_ddl_parser.parse(splice_buf, plain_parse_result));
  return ret;
}

int ObCreateWrappedResolver::check_object_name_match(const ParseNode *n1, const ParseNode *n2)
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(n1) && OB_NOT_NULL(n2));
  CK (OB_LIKELY(T_SP_NAME == n1->type_ && T_SP_NAME == n2->type_));
  CK (OB_LIKELY(2 == n1->num_child_ && 2 == n2->num_child_));
  CK (OB_NOT_NULL(n1->children_) && OB_NOT_NULL(n2->children_));

#define COMPARE_IDENT(ident1, ident2)                                              \
  do {                                                                             \
    CK (!(OB_NOT_NULL(ident1) ^ OB_NOT_NULL(ident2)));                             \
    if (OB_NOT_NULL(ident1) && OB_NOT_NULL(ident2)) {                              \
      CK (OB_LIKELY(T_IDENT == ident1->type_ && T_IDENT == ident2->type_));        \
      CK (OB_NOT_NULL(ident1->str_value_) && OB_NOT_NULL(ident2->str_value_));     \
      CK (OB_LIKELY(ident1->str_len_ == ident2->str_len_));                        \
      CK (0 == strncmp(ident1->str_value_, ident2->str_value_, ident1->str_len_)); \
    }                                                                              \
  } while (0)

  COMPARE_IDENT(n1->children_[0], n2->children_[0]);
  COMPARE_IDENT(n1->children_[1], n2->children_[1]);
#undef COMPARE_IDENT

  if (OB_FAIL(ret)) {
    ret = OB_ERR_MALFORMED_WRAPPED_UNIT;
    LOG_WARN("malformed or corrupted wrapped unit", K(ret));
  }
  return ret;
}

int ObCreateWrappedResolver::check_plwrap_version_compatible()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = OB_INVALID_TENANT_ID;
  uint64_t tenant_data_version = 0;
  CK (OB_NOT_NULL(params_.session_info_));
  OX (tenant_id = params_.session_info_->get_effective_tenant_id());
  OZ (GET_MIN_DATA_VERSION(tenant_id, tenant_data_version));
  if (OB_SUCC(ret) && DATA_VERSION_4_3_5_1 > tenant_data_version) {
    // binary rollback is not allowed after target data version is settled, which allows wrapped
    // system package to be created after the target_data_version is increased.
    CK (OB_NOT_NULL(params_.sql_proxy_));
    share::ObGlobalStatProxy proxy(*params_.sql_proxy_, tenant_id);
    uint64_t target_data_version = 0;
    OZ (proxy.get_target_data_version(false, target_data_version));
    if (OB_SUCC(ret) && DATA_VERSION_4_3_5_1 > target_data_version) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("target data version is less than 4.3.5.1, wrapped PL/SQL unit is not supported",
               K(ret), K(tenant_data_version));
      LOG_USER_ERROR(
          OB_NOT_SUPPORTED,
          "target data version is less than 4.3.5.1, wrapped PL/SQL unit is not supported");
    }
  }
  return ret;
}

int ObCreateWrappedPackageResolver::resolve(const ParseNode& cipher_parse_tree)
{
  int ret = OB_SUCCESS;
  ParseResult plain_parse_result;
  const ParseNode* plain_parse_tree = nullptr;
  ObCreatePackageStmt *stmt = nullptr;
  ObString package_source{cipher_parse_tree.str_len_, cipher_parse_tree.str_value_};

  OZ (check_plwrap_version_compatible());
  // 1. decode cipher ddl & parse plain ddl
  CHECK_CIPHER_PARSE_TREE(cipher_parse_tree, T_CREATE_WRAPPED_PACKAGE);
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(session_info_));
  OZ (resolve_base64_cipher(*allocator_,
                            *session_info_,
                            *cipher_parse_tree.children_[1],
                            cipher_parse_tree.int32_values_[0],
                            cipher_parse_tree.int32_values_[1],
                            plain_parse_result));

  // 2. resolve plain ddl parse tree
  CK (OB_NOT_NULL(plain_parse_tree = plain_parse_result.result_tree_));
  CHECK_CREATE_DDL_PARSE_TREE(plain_parse_tree);
  OZ (check_object_name_match(cipher_parse_tree.children_[0],
                              plain_parse_tree->children_[0]->children_[0]->children_[0]));
  OZ (ObCreatePackageResolver::resolve(*(plain_parse_tree->children_[0])));

  // 3. replace plain body with cipher body
  CK (OB_NOT_NULL(stmt = static_cast<ObCreatePackageStmt*>(get_basic_stmt())));
  OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
       *allocator_, session_info_->get_dtc_params(), package_source));
  OZ (stmt->get_create_package_arg().package_info_.set_source(package_source));

  return ret;
}

int ObCreateWrappedPackageBodyResolver::resolve(const ParseNode& cipher_parse_tree)
{
  int ret = OB_SUCCESS;
  ParseResult plain_parse_result;
  const ParseNode* plain_parse_tree = nullptr;
  ObCreatePackageStmt *stmt = nullptr;
  ObString package_body_source{cipher_parse_tree.str_len_, cipher_parse_tree.str_value_};

  OZ (check_plwrap_version_compatible());
  // 1. decode cipher ddl & parse plain ddl
  CHECK_CIPHER_PARSE_TREE(cipher_parse_tree, T_CREATE_WRAPPED_PACKAGE_BODY);
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(session_info_));
  OZ (resolve_base64_cipher(*allocator_,
                            *session_info_,
                            *cipher_parse_tree.children_[1],
                            cipher_parse_tree.int32_values_[0],
                            cipher_parse_tree.int32_values_[1],
                            plain_parse_result));

  // 2. resolve plain ddl parse tree
  CK (OB_NOT_NULL(plain_parse_tree = plain_parse_result.result_tree_));
  CHECK_CREATE_DDL_PARSE_TREE(plain_parse_tree);
  OZ (check_object_name_match(cipher_parse_tree.children_[0],
                              plain_parse_tree->children_[0]->children_[0]->children_[0]));
  OZ (ObCreatePackageBodyResolver::resolve(*(plain_parse_tree->children_[0])));

  // 3. replace plain body with cipher body
  CK (OB_NOT_NULL(stmt = static_cast<ObCreatePackageStmt*>(get_basic_stmt())));
  obrpc::ObCreatePackageArg &arg = stmt->get_create_package_arg();
  OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
       *allocator_, session_info_->get_dtc_params(), package_body_source));
  OZ (arg.package_info_.set_source(package_body_source));
  OX (ARRAY_FOREACH(arg.public_routine_infos_, routine_idx) {
    ObRoutineInfo &routine_info = arg.public_routine_infos_.at(routine_idx);
    routine_info.set_routine_body(ObString());
  });

  return ret;
}

int ObCreateWrappedTypeResolver::resolve(const ParseNode& cipher_parse_tree)
{
  int ret = OB_SUCCESS;
  ParseResult plain_parse_result;
  const ParseNode* plain_parse_tree = nullptr;
  ObCreateUDTStmt *stmt = nullptr;
  ObString type_source{cipher_parse_tree.str_len_, cipher_parse_tree.str_value_};

  OZ (check_plwrap_version_compatible());
  // 1. decode cipher ddl & parse plain ddl
  CHECK_CIPHER_PARSE_TREE(cipher_parse_tree, T_CREATE_WRAPPED_TYPE);
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(session_info_));
  OZ (resolve_base64_cipher(*allocator_,
                            *session_info_,
                            *cipher_parse_tree.children_[1],
                            cipher_parse_tree.int32_values_[0],
                            cipher_parse_tree.int32_values_[1],
                            plain_parse_result));

  // 2. resolve plain ddl parse tree
  CK (OB_NOT_NULL(plain_parse_tree = plain_parse_result.result_tree_));
  CHECK_CREATE_DDL_PARSE_TREE(plain_parse_tree);
  OZ (check_object_name_match(cipher_parse_tree.children_[0],
                              plain_parse_tree->children_[0]->children_[0]->children_[0]));
  OZ (ObCreateUDTResolver::resolve(*(plain_parse_tree->children_[0])));

  // 3. replace plain body with cipher body
  CK (OB_NOT_NULL(stmt = static_cast<ObCreateUDTStmt*>(get_basic_stmt())));
  OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
       *allocator_, session_info_->get_dtc_params(), type_source));
  if (OB_SUCC(ret)
      && stmt->get_udt_arg().udt_info_.is_object_spec_ddl()
      // NULL when error occurs during resolving udt
      && OB_NOT_NULL(stmt->get_udt_arg().udt_info_.get_object_info())) {
    OZ (stmt->get_udt_arg().udt_info_.get_object_info()->set_source(type_source));
  }

  return ret;
}

int ObCreateWrappedTypeBodyResolver::resolve(const ParseNode& cipher_parse_tree)
{
  int ret = OB_SUCCESS;
  ParseResult plain_parse_result;
  const ParseNode* plain_parse_tree = nullptr;
  ObCreateUDTStmt *stmt = nullptr;
  ObString type_body_source{cipher_parse_tree.str_len_, cipher_parse_tree.str_value_};

  OZ (check_plwrap_version_compatible());
  // 1. decode cipher ddl & parse plain ddl
  CHECK_CIPHER_PARSE_TREE(cipher_parse_tree, T_CREATE_WRAPPED_TYPE_BODY);
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(session_info_));
  OZ (resolve_base64_cipher(*allocator_,
                            *session_info_,
                            *cipher_parse_tree.children_[1],
                            cipher_parse_tree.int32_values_[0],
                            cipher_parse_tree.int32_values_[1],
                            plain_parse_result));

  // 2. resolve plain ddl parse tree
  CK (OB_NOT_NULL(plain_parse_tree = plain_parse_result.result_tree_));
  CHECK_CREATE_DDL_PARSE_TREE(plain_parse_tree);
  OZ (check_object_name_match(cipher_parse_tree.children_[0],
                              plain_parse_tree->children_[0]->children_[0]->children_[0]));
  OZ (ObCreateUDTBodyResolver::resolve(*(plain_parse_tree->children_[0])));

  // 3. replace plain body with cipher body
  CK (OB_NOT_NULL(stmt = static_cast<ObCreateUDTStmt*>(get_basic_stmt())));
  OZ (ObSQLUtils::convert_sql_text_to_schema_for_storing(
       *allocator_, session_info_->get_dtc_params(), type_body_source));
  CK (OB_LIKELY(stmt->get_udt_arg().udt_info_.is_object_body_ddl()));
  CK (OB_NOT_NULL(stmt->get_udt_arg().udt_info_.get_object_info()));
  OZ (stmt->get_udt_arg().udt_info_.get_object_info()->set_source(type_body_source));
  OX (ARRAY_FOREACH(stmt->get_udt_arg().public_routine_infos_, routine_idx) {
    ObRoutineInfo &routine_info = stmt->get_udt_arg().public_routine_infos_.at(routine_idx);
    routine_info.set_routine_body(ObString());
  });

  return ret;
}

int ObCreateWrappedFunctionResolver::resolve(const ParseNode& cipher_parse_tree)
{
  int ret = OB_SUCCESS;
  ParseResult plain_parse_result;
  const ParseNode* plain_parse_tree = nullptr;
  ObCreateRoutineStmt *stmt = nullptr;

  OZ (check_plwrap_version_compatible());
  // 1. decode cipher ddl & parse plain ddl
  CHECK_CIPHER_PARSE_TREE(cipher_parse_tree, T_CREATE_WRAPPED_FUNCTION);
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(session_info_));
  OZ (resolve_base64_cipher(*allocator_,
                            *session_info_,
                            *cipher_parse_tree.children_[1],
                            cipher_parse_tree.int32_values_[0],
                            cipher_parse_tree.int32_values_[1],
                            plain_parse_result));

  // 2. resolve plain ddl parse tree
  CK (OB_NOT_NULL(plain_parse_tree = plain_parse_result.result_tree_));
  CHECK_CREATE_DDL_PARSE_TREE(plain_parse_tree);
  OZ (check_object_name_match(cipher_parse_tree.children_[0],
                              plain_parse_tree->children_[0]->children_[0]->children_[0]));
  OZ (ObCreateFunctionResolver::resolve(*(plain_parse_tree->children_[0])));

  // 3. replace plain routine body with cipher routine body
  CK (OB_NOT_NULL(stmt = static_cast<ObCreateRoutineStmt*>(get_basic_stmt())));
  OZ (resolve_sp_body(&cipher_parse_tree, stmt->get_routine_arg().routine_info_));

  return ret;
}

int ObCreateWrappedProcedureResolver::resolve(const ParseNode& cipher_parse_tree)
{
  int ret = OB_SUCCESS;
  ParseResult plain_parse_result;
  const ParseNode* plain_parse_tree = nullptr;
  ObCreateRoutineStmt *stmt = nullptr;

  OZ (check_plwrap_version_compatible());
  // 1. decode cipher ddl & parse plain ddl
  CHECK_CIPHER_PARSE_TREE(cipher_parse_tree, T_CREATE_WRAPPED_PROCEDURE);
  CK (OB_NOT_NULL(allocator_));
  CK (OB_NOT_NULL(session_info_));
  OZ (resolve_base64_cipher(*allocator_,
                            *session_info_,
                            *cipher_parse_tree.children_[1],
                            cipher_parse_tree.int32_values_[0],
                            cipher_parse_tree.int32_values_[1],
                            plain_parse_result));

  // 2. resolve plain ddl parse tree
  CK (OB_NOT_NULL(plain_parse_tree = plain_parse_result.result_tree_));
  CHECK_CREATE_DDL_PARSE_TREE(plain_parse_tree);
  OZ (check_object_name_match(cipher_parse_tree.children_[0],
                              plain_parse_tree->children_[0]->children_[0]->children_[0]));
  OZ (ObCreateProcedureResolver::resolve(*(plain_parse_tree->children_[0])));

  // 3. replace plain routine body with cipher routine body
  CK (OB_NOT_NULL(stmt = static_cast<ObCreateRoutineStmt*>(get_basic_stmt())));
  OZ (resolve_sp_body(&cipher_parse_tree, stmt->get_routine_arg().routine_info_));

  return ret;
}

}  // namespace sql
}  // namespace oceanbase

#endif /* OB_BUILD_ORACLE_PL */

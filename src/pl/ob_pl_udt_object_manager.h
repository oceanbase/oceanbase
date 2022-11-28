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

#ifndef SRC_PL_OB_PL_UDT_OBJ_MANAGER_H_
#define SRC_PL_OB_PL_UDT_OBJ_MANAGER_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "sql/parser/parse_node.h"
#include "lib/container/ob_iarray.h"
#include "share/schema/ob_routine_info.h"
#include "pl/ob_pl_package.h"
#include "sql/plan_cache/ob_cache_object.h"
#include "pl/parser/parse_stmt_item_type.h"

namespace oceanbase
{
namespace common
{
class ObObj;
class ObString;
class ObIAllocator;
class ObMySQLProxy;
}

namespace sql
{
class ObExecContext;
class ObSchemaChecker;
}

namespace share
{
namespace schema
{
class ObSchemaGetterGuard;
class ObPackageInfo;
class ObUDTTypeInfo;
class ObUDTObjectType;
}
}

namespace pl
{
struct ObPLExecCtx;
class ObPLResolveCtx;
class ObUserDefinedType;
class ObPLVar;
class ObPLPackage;
class ObPLFunction;
class ObPLPackageState;
class ObPLCondition;
class ObPLCursor;

class ObPLUDTObjectTypeAST : public ObPLPackageAST {
public:
  explicit ObPLUDTObjectTypeAST(common::ObIAllocator &allocator) : ObPLPackageAST(allocator) {}
  virtual ~ObPLUDTObjectTypeAST() {}

  int init(const ObString &db_name, const ObString &package_name, ObPackageType package_type,
           uint64_t database_id, uint64_t package_id, int64_t package_version,
           ObPLUDTObjectTypeAST *parent_package_ast);

private:
  DISALLOW_COPY_AND_ASSIGN(ObPLUDTObjectTypeAST);
};

class ObPLUDTObject : public ObPLPackage
{
public:
  ObPLUDTObject(lib::MemoryContext &mem_context)
    : ObPLPackage(mem_context) {
      set_can_cached(false);
    }
  virtual ~ObPLUDTObject() {}
  int init(const ObPLUDTObjectTypeAST &type_ast);
};

class ParseNodeWrapper {
public:
  ParseNodeWrapper(ParseNode *node) : node_(node) {}
  ~ParseNodeWrapper() {node_ = NULL;}
  TO_STRING_KV(K_(node_->type));
  ParseNode *node_;
};
class ObPLUDTObjectManager {
public:
  ObPLUDTObjectManager();
  virtual ~ObPLUDTObjectManager();

public:
  // udt this 指针的形参，用在create的时候，用于构造obRoutineparam
  static int make_param_node_for_self(ParseNode *&param_list,
                                      const common::ObString &name,
                                      const common::ObString &type_name,
                                      common::ObIAllocator *allocator,
                                      const SpParam &param_mode);

  // udt this 指针的形参，用在create的时候，用于构造obRoutineparam
  static int make_param_node_for_self_impl(ParseNode *&self_param,
                                      const common::ObString &name,
                                      const common::ObString &type_name,
                                      common::ObIAllocator *allocator,
                                      const SpParam &param_mode);

  // udt this 指针的实参，用在obj.func(a)这种函数的self的参数，用于构造rawexpr，是实际参数
  // 它这个self参数是一个obj access，就是obj, 函数内部的attr被解析成obj.a1这样的引用.
  static int make_argument_node_for_self(ParseNode *&param_self,
                                         const common::ObIArray<common::ObString> &access_name,
                                         common::ObIAllocator *allocator);
  static int make_argument_node_for_self_impl(ParseNode *&access_node,
                                         const common::ObString &var_name,
                                         common::ObIAllocator *allocator);

  static int get_extra_param_name(const share::schema::ObUDTTypeInfo *udt_info,
                             common::ObIAllocator &allocator,
                             common::ObIArray<common::ObString> &name_arr,
                             common::ObIArray<common::ObString> &type_name_arr);

  static int get_extra_param_name(const common::ObString &type_name,
                             common::ObIAllocator &allocator,
                             common::ObIArray<common::ObString> &name_arr,
                             common::ObIArray<common::ObString> &type_name_arr);

  static int deep_copy_str(common::ObIAllocator &allocator,
                           const common::ObString &src,
                           common::ObString &dst);

  static int check_udt_routine_is_static(uint64_t routine_id,
                                         uint64_t udt_id,
                                         oceanbase::sql::ObSchemaChecker &schema_check,
                                         bool &is_static);
  static int check_udt_routine_is_static(const common::ObString &routine_name,
                                         uint64_t tenant_id,
                                         uint64_t db_id,
                                         uint64_t udt_id,
                                         share::schema::ObRoutineType routine_type,
                                         oceanbase::sql::ObSchemaChecker &schema_check,
                                         bool &is_static);

  static int verify_udt_body(ObIArray<share::schema::ObRoutineInfo> &spec_routine_infos,
                             uint64_t database_id,
                             uint64_t tenant_id,
                             sql::ObSchemaChecker &schema_check,
                             const ObIArray<share::schema::ObRoutineInfo> &body_routines);

  static int make_self_param_for_routines(ObIAllocator &allocator,
                                          const ObString &type_name,
                                          ObPLCompileUnitAST &unit_ast,
                                          ObPLResolver &resolver);
  static int make_return_node(ObIAllocator &allocator,
                              ParseNode *&ret_node);

  static int compile_udt_object(const ObUDTTypeInfo &udt_info,
                                pl::ObPLResolveCtx resolve_ctx,
                                ObPLUDTObjectTypeAST &object_ast,
                                ObPLUDTObject *object,
                                ObPLUDTObjectTypeAST &body_ast,
                                ObPLUDTObject *body_object);
  static int compile_udt_object_spec(share::schema::ObUDTObjectType &obj_spec,
                                      const ObString &type_name,
                                      pl::ObPLResolveCtx resolve_ctx,
                                      ObPLUDTObjectTypeAST *parent_ast,
                                      const sql::ObExecEnv &exec_env,
                                      ObPLUDTObjectTypeAST &object_ast,
                                      ObPLUDTObject *object);
  static int compile_udt_object_body(share::schema::ObUDTObjectType &obj_body,
                                      const ObString &type_name,
                                      pl::ObPLResolveCtx resolve_ctx,
                                      ObPLUDTObjectTypeAST *parent_ast,
                                      const sql::ObExecEnv &exec_env,
                                      ObPLUDTObjectTypeAST &object_ast,
                                      ObPLUDTObject *object);
  static int compile_udt_object_impl(share::schema::ObUDTObjectType &obj_spec,
                                        const ObString &type_name,
                                        pl::ObPLResolveCtx resolve_ctx,
                                        ObPLUDTObjectTypeAST *parent_ast,
                                        const sql::ObExecEnv &exec_env,
                                        ObPLUDTObjectTypeAST &type_ast,
                                        ObPLUDTObject *object);
  static int transform_sf_to_subr(ObIAllocator &allocator,
                                  ParseNode *stmt_list,
                                  const ObString &type_name);
  static int make_self_node(ObIAllocator &allocator,
                            ParseNode *obj_body,
                            const ObString &type_name);
  static int get_udt_function(const ObPLResolveCtx &resolve_ctx,
                                           sql::ObExecContext &exec_ctx,
                                           uint64_t udt_id,
                                           int64_t routine_id,
                                           ObPLFunction *&routine);
  static int find_parse_node(ParseNode *node,
                             ObItemType type,
                             ObIArray<int*> &out_node);
  static int get_object_from_cache(const ObPLResolveCtx &resolve_ctx,
                                    uint64_t object_id,
                                    ObPLUDTObject *&object);
  static int add_object_to_plan_cache(const ObPLResolveCtx &resolve_ctx,
                                       ObPLUDTObject *object);

  static int check_udt_legal(const ObPLRoutineTable &routine_tables,
                                  const ObString &type_name,
                                  const uint64_t udt_id,
                                  bool ignore_type_id = false);
  static int verify_udt_body_legal(const ObPLBlockNS *parent_ns,
                                               const ObPLPackageAST &package_ast);
  static int make_constructor_self_expr(const ObPLResolveCtx &resolve_ctx,
                                        const ObString &database_name,
                                        const ObString &func_name,
                                        const uint64_t tenant_id,
                                        ObRawExprFactory &expr_factory,
                                        pl::ObPLBlockNS &ns,
                                        ObRawExpr *&self_expr);
  static int check_overload_default_cons(const ObRoutineInfo *routine_info,
                                         const ObUDTTypeInfo *udt_info,
                                         ObIAllocator &allocator,
                                         bool &is_overloaded);
  static int check_overload_default_cons(const ObPLRoutineInfo *routine_info,
                                         const ObUDTTypeInfo *udt_info,
                                         ObIAllocator &allocator,
                                         bool &is_overloaded);

  static int check_routine_callable(const pl::ObPLBlockNS &ns, bool is_static, uint64_t &udt_id);
  static bool is_self_param(const ObString &param_name,
                            ObCollationType coll = CS_TYPE_UTF8MB4_BIN);
};

} // end namespace pl
} // end namespace oceanbase
#endif
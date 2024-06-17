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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_UDT_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_UDT_RESOLVER_H_

#include "sql/resolver/ddl/ob_create_routine_resolver.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace pl
{
class ObPLResolver;
class ObPLUDTObjectTypeAST;
}
namespace share
{
namespace schema
{
class ObUDTTypeInfo;
class ObUDTTypeAttr;
class ObUDTCollectionType;
}
}
namespace sql
{
class ObCreateUDTResolver: public ObDDLResolver
{
public:
  explicit ObCreateUDTResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual int resolve(const ParseNode &parse_tree);
private:
  int resolve_without_check_valid(const ParseNode &parse_tree);
  int create_udt_arg(obrpc::ObCreateUDTArg *&crt_udt_arg);
  int resolve_oid_clause(const ParseNode *oid_node,
                         share::schema::ObUDTTypeInfo *udt_info);
  int resolve_type_name(const ParseNode *name_node,
                        obrpc::ObCreateUDTArg* crt_udt_arg,
                        share::schema::ObUDTTypeInfo *udt_info);
  int resolve_udt_data_type(const ParseNode *type_node,
                        const share::schema::ObUDTTypeInfo* type_info,
                        int64_t position,
                        share::schema::ObUDTTypeAttr& type_attr,
                        const ObString& db_name);
  int resolve_final_node(const ParseNode *final_node);
  int resolve_type_attr(const ParseNode *attr_node,
                        int64_t position,
                        share::schema::ObUDTTypeInfo* udt_info,
                        ObString &db_name);
  int resolve_type_attr_list(const ParseNode *attr_list,
                        share::schema::ObUDTTypeInfo* udt_info,
                        ObString &db_name);
  int resolve_type_attrs(const ParseNode *invoke_accessby_node,
                        const ParseNode *attrs_node,
                        obrpc::ObCreateUDTArg &crt_udt_arg,
                        const ObString &object_spec);
  int resolve_type_object(const ParseNode *object_type_node,
                        obrpc::ObCreateUDTArg &crt_udt_arg,
                        const ObString &object_spec);
  int resolve_udt_data_type(const ParseNode *type_node,
                            share::schema::ObUDTCollectionType& coll_type,
                            const ObString& type_name,
                            const ObString& db_name);
  int resolve_type_varray(const ParseNode *varray_node,
                          obrpc::ObCreateUDTArg &crt_udt_arg);
  int resolve_type_nested_table(const ParseNode *nested_node,
                                obrpc::ObCreateUDTArg &crt_udt_arg);
  int resolve_type_define(const ParseNode *type_def_node,
                        obrpc::ObCreateUDTArg &crt_udt_arg,
                        const ObString &object_spec);
  int resolve_object_spec(const ParseNode *invoke_accessby_node,
                        const ParseNode *object_node, 
                        obrpc::ObCreateUDTArg &crt_udt_arg,
                        const ObString &db_name,
                        const ObString &object_spec);
  int detect_loop_dependency(const ObString &target_udt_name,
                             uint64_t target_database_id,
                             const ObUDTTypeInfo *udt_info,
                             bool &has_mutual_dep);
public:
  static int check_udt_validation(const ObSQLSessionInfo& session_info,
                                  ObSchemaGetterGuard& schema_guard,
                                  ObResolverParams &params,
                                  const ObString& db_name,
                                  const ObUDTTypeInfo& new_udt_info,
                                  bool& valid);
  static int check_udt_validation(const ObSQLSessionInfo& session_info,
                                  ObSchemaGetterGuard& schema_guard,
                                  ObResolverParams &params,
                                  const ObString& db_name,
                                  const ObString& new_udt_name,
                                  ObUDTTypeCode type_code,
                                  bool& valid);
  static int package_info_to_object_info(const share::schema::ObPackageInfo &pkg_info,
                                         share::schema::ObUDTObjectType &obj_info);
  static int object_info_to_package_info(const share::schema::ObUDTObjectType &obj_info,
                                         share::schema::ObPackageInfo &pkg_info);
  static int resolve_udt_spec_routines(share::schema::ObUDTTypeInfo &udt_info,
                        pl::ObPLUDTObjectTypeAST &type_ast,
                        pl::ObPLResolver &resolver,
                        ObIArray<share::schema::ObRoutineInfo> &public_routine_infos_,
                        const ObString &object_spec,
                        ObResolverParams &resolve_param,
                        bool is_invoker_right = false,
                        bool has_accessible_by = false);
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateUDTResolver);
};

class ObCreateUDTBodyResolver: public ObDDLResolver
{
public:
  explicit ObCreateUDTBodyResolver(ObResolverParams &params) : ObDDLResolver(params) {}
  virtual int resolve(const ParseNode &parse_tree);
  int resolve_type_body(const ParseNode *body_node,
                        obrpc::ObCreateUDTArg &crt_udt_arg,
                        const share::schema::ObUDTTypeInfo *udt_info);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateUDTBodyResolver);
};

} // end namespace sql
} // end namespace oceanbase



#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_CREATE_UDT_RESOLVER_H_ */

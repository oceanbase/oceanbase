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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_RESOLVER_H_

#include "sql/resolver//ddl/ob_ddl_resolver.h"
#include "pl/parser/parse_stmt_item_type.h"

namespace oceanbase
{
namespace pl
{
class ObPLPackageAST;
class ObPLCompiler;
}
namespace sql
{
class ObAlterUDTStmt;
class ObAlterUDTResolver: public ObDDLResolver
{
public:
  explicit ObAlterUDTResolver(ObResolverParams &params) : ObDDLResolver(params) {}

  virtual int resolve(const ParseNode &parse_tree);

private:
  int resolve_alter_clause(const ParseNode *alter_clause,
                           const ObString &db_name,
                           const ObString &udt_name,
                           ObAlterUDTStmt &alter_udt_stmt);

  int resolve_alter_compile_clause(const ParseNode *compile_clause,
                                   const ObString &db_name,
                                   const ObString &udt_name,
                                   ObAlterUDTStmt &alter_udt_stmt);
  int compile_udt(const ObString &db_name,
                  const ObString &udt_name,
                  int16_t compile_uint,
                  ObAlterUDTStmt &alter_udt_stmt);

  int analyze_udt(obrpc::ObAlterUDTArg &udt_arg,
                  share::schema::ObUDTTypeInfo &udt_info,
                  const share::schema::ObUDTTypeInfo &old_udt_info,
                  int64_t type_code,
                  int64_t compile_unit);

  int resolve_alter_attribute_definition_clause(const ParseNode *attribute_definition_clause,
                                                const ObString &db_name,
                                                const ObString &udt_name,
                                                ObAlterUDTStmt &alter_udt_stmt);

  int resolve_attribute_definition_list(const ParseNode *attribute_definition_list,
                                        share::schema::ObUDTTypeInfo *udt_info,
                                        obrpc::ObAlterUDTArg &udt_arg);

  int resolve_attribute_definition(const ParseNode *attribute_definition,
                                          share::schema::ObUDTTypeInfo *udt_info,
                                          obrpc::ObAlterUDTArg &udt_arg,
                                          common::ObIArray<ObString> &alter_attrs);
  int resolve_alter_add_or_modify_attributes(const ParseNode *attr_list_node,
                                            share::schema::ObUDTTypeInfo *udt_info,
                                            obrpc::ObAlterUDTArg &udt_arg,
                                            common::ObIArray<ObString> &alter_attrs,
                                            TypeAlterAttrOptions alter_attr_type);
  int resolve_alter_add_or_modify_attr(const ParseNode *attr_node,
                                        share::schema::ObUDTTypeInfo *udt_info,
                                        obrpc::ObAlterUDTArg &udt_arg,
                                        common::ObIArray<ObString> &alter_attrs,
                                        TypeAlterAttrOptions alter_attr_type);
  int resolve_modify_attr(share::schema::ObUDTTypeInfo *udt_info,
                          ObUDTTypeAttr &type_attr);
  int resolve_alter_drop_attributes(const ParseNode *identifier_list_node,
                              share::schema::ObUDTTypeInfo *udt_info,
                              obrpc::ObAlterUDTArg &udt_arg,
                              common::ObIArray<ObString> &alter_attrs);
  int resolve_type_attr_func_duplicate_check(const common::ObIArray<ObUDTTypeAttr*> &attrs,
                                             const common::ObSArray<share::schema::ObRoutineInfo> &methods,
                                              const ObString &name);
  int resolve_drop_attr(const ParseNode *identifier_node,
                        share::schema::ObUDTTypeInfo *udt_info,
                        obrpc::ObAlterUDTArg &udt_arg,
                        common::ObIArray<ObString> &alter_attrs);
  int reconstruct_udt_ddl(share::schema::ObUDTTypeInfo *udt_info);
  static int check_modify_attr_legal(ObUDTTypeAttr &new_attr, ObUDTTypeAttr &old_attr);
  static int get_udt_ddl_source(ObIAllocator &allocator,
                        const ObSQLSessionInfo &session_info,
                        ObString &udt_ddl,
                        ObString &udt_source);
  static int alter_type_attr_duplicate_check(const common::ObIArray<ObString> &attrs,
                                     const ObString &attr_name);
  int add_alter_attrs_array(common::ObIArray<ObString> &alter_attrs,
                            const ObString &attr_name);


private:
  DISALLOW_COPY_AND_ASSIGN(ObAlterUDTResolver);
};
} //namespace sql
} //namespace oceanbase

#endif /* OCEANBASE_SRC_SQL_RESOLVER_DDL_OB_ALTER_UDT_RESOLVER_H_ */
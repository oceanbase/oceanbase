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

#ifndef OCEANBASE_SQL_RESOLVER_DDL_OB_TRIGGER_RESOLVER_
#define OCEANBASE_SQL_RESOLVER_DDL_OB_TRIGGER_RESOLVER_

#include "sql/resolver/ob_stmt_resolver.h"

namespace oceanbase
{
namespace obrpc
{
struct ObCreateTriggerArg;
}
namespace share
{
namespace schema
{
class ObTriggerInfo;
}
}

namespace sql
{
class ObTriggerResolver : public ObStmtResolver
{
public:
  explicit ObTriggerResolver(ObResolverParams &params)
    : ObStmtResolver(params)
  {}
  virtual ~ObTriggerResolver()
  {}
  virtual int resolve(const ParseNode &parse_tree);
  static int analyze_trigger(ObSchemaGetterGuard &schema_guard,
                             ObSQLSessionInfo *session_info,
                             ObMySQLProxy *sql_proxy,
                             ObIAllocator &allocator,
                             const ObTriggerInfo &trigger_info,
                             const ObString &db_name,
                             ObIArray<ObDependencyInfo> &dep_infos,
                             bool is_alter_compile);
private:
  int resolve_create_trigger_stmt(const ParseNode &parse_node,
                                  obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_drop_trigger_stmt(const ParseNode &parse_node,
                                obrpc::ObDropTriggerArg &trigger_arg);
  int resolve_alter_trigger_stmt(const ParseNode &parse_node,
                                 obrpc::ObAlterTriggerArg &trigger_arg);
  int resolve_trigger_source(const ParseNode &parse_node,
                             obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_simple_dml_trigger(const ParseNode &parse_node,
                                 obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_instead_dml_trigger(const ParseNode &parse_node,
                                  obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_compound_dml_trigger(const ParseNode &parse_node,
                                   obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_compound_timing_point(const ParseNode &parse_node,
                                    obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_timing_point(int16_t before_or_after, int16_t stmt_or_row,
                           obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_dml_event_option(const ParseNode &parse_node,
                               obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_reference_names(const ParseNode *parse_node,
                              obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_trigger_status(int16_t enable_or_disable,
                             obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_when_condition(const ParseNode *parse_node,
                             obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_trigger_body(const ParseNode &parse_node,
                           obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_compound_trigger_body(const ParseNode &parse_node,
                                    obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_dml_event_list(const ParseNode &parse_node,
                             obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_sp_definer(const ParseNode *parse_node,
                         obrpc::ObCreateTriggerArg &trigger_arg);
  int resolve_schema_name(const ParseNode &parse_node,
                          common::ObString &database_name,
                          common::ObString &schema_name);
  int resolve_alter_clause(const ParseNode &alter_clause,
                           share::schema::ObTriggerInfo &tg_info,
                           const ObString &db_name,
                           bool &is_set_status,
                            bool &is_alter_compile);
  int fill_package_info(share::schema::ObTriggerInfo &trigger_info);

  int resolve_base_object(obrpc::ObCreateTriggerArg &trigger_arg, bool search_public_schema);
  int resolve_order_clause(const ParseNode *parse_node, obrpc::ObCreateTriggerArg &trigger_arg);
#ifdef OB_BUILD_ORACLE_PL
  int resolve_rename_trigger(const ParseNode &rename_clause,
                             ObSchemaGetterGuard &schema_guard,
                             share::schema::ObTriggerInfo &trigger_info,
                             common::ObIAllocator &alloc);
#endif

private:
  static const common::ObString REF_OLD;
  static const common::ObString REF_NEW;
  static const common::ObString REF_PARENT;
  DISALLOW_COPY_AND_ASSIGN(ObTriggerResolver);
};

} // namespace sql
} // namespace oceanbase

#endif // OCEANBASE_SQL_RESOLVER_DDL_OB_TRIGGER_RESOLVER_


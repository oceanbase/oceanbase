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

#ifndef OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_MODE_DML_RESOLVER_H_
#define OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_MODE_DML_RESOLVER_H_

#include "sql/resolver/dml/ob_dml_resolver.h"

namespace oceanbase
{
namespace sql
{
struct ObJtDmlCtx {
  ObJtDmlCtx(int32_t id, int64_t cur_column_id, ObDMLResolver* dml_resolver)
    : id_(id),
      cur_column_id_(cur_column_id),
      dml_resolver_(dml_resolver) {}
  int32_t id_;
  int64_t cur_column_id_;
  ObDMLResolver* dml_resolver_;
};

// Splitting of multi-mode related functions in dml_resolver
// json related functions are begin with json, so do xml and gis
class ObMultiModeDMLResolver
{
public:
  // json table & xml table common:
  static int multimode_table_resolve_item(const ParseNode &table_node,
                                          TableItem *&table_item,
                                          ObDMLResolver* dml_resolver);
  static int multimode_table_resolve_column_item(const TableItem &table_item,
                                                const ObString &column_name,
                                                ColumnItem *&col_item,
                                                ObDMLStmt *stmt);
  static int multimode_table_resolve_column_item(const ParseNode &parse_tree,
                                                TableItem *table_item,
                                                ObDmlJtColDef* col,
                                                ObIArray<ObDmlJtColDef*>& json_table_infos,
                                                ObJtDmlCtx& jt_ctx,
                                                int32_t parent);
  static int multimode_table_resolve_column_name_and_path(const ParseNode *name_node,
                                                          const ParseNode *path_node,
                                                          ObDMLResolver* dml_resolver,
                                                          ObDmlJtColDef *col_def,
                                                          MulModeTableType table_type);
  static int multimode_table_resolve_regular_column(const ParseNode &parse_tree,
                                                    TableItem *table_item,
                                                    ObDmlJtColDef *&col_def,
                                                    ObIArray<ObDmlJtColDef*>& json_table_infos,
                                                    ObJtDmlCtx& jt_dml_ctx,
                                                    int32_t parent);
  static int multimode_table_get_column_by_id(uint64_t table_id, ObDmlJtColDef *&col_def,
                                              ObIArray<ObDmlJtColDef*>& json_table_infos);
  static int multimode_table_resolve_column_type(const ParseNode &parse_tree,
                                                const int col_type,
                                                ObDataType &data_type,
                                                ObDmlJtColDef *col_def,
                                                ObSQLSessionInfo *session_info);
  static int multimode_table_generate_column_item(TableItem *table_item,
                                                  const ObDataType &data_type,
                                                  const ParseNode &data_type_node,
                                                  const ObString &column_name,
                                                  int64_t column_id,
                                                  ColumnItem *&col_item,
                                                  ObDMLResolver* dml_resolver);
  // json:
  // json_table:
  static int json_table_check_column_constrain(ObDmlJtColDef *col_def);
  static int json_table_check_dup_path(ObIArray<ObDmlJtColDef*>& columns,
                                      const ObString& column_name);
  static int json_table_check_dup_name(const ObJsonTableDef* table_def,
                                      const ObString& column_name,
                                      bool& exists);
  static int json_table_resolve_all_column_items(const TableItem &table_item,
                                                 ObIArray<ColumnItem> &col_items,
                                                 ObDMLStmt* stmt);
  static int json_table_path_printer(ParseNode *&tmp_path, ObJsonBuffer &res_str);
  static int json_table_make_json_path(const ParseNode &parse_tree, ObIAllocator* allocator,
                                      ObString& path_str, MulModeTableType table_type);
  static int json_table_resolve_nested_column(const ParseNode &parse_tree,
                                            TableItem *table_item,
                                            ObDmlJtColDef *&col_def,
                                            ObIArray<ObDmlJtColDef*>& json_table_infos,
                                            ObJtDmlCtx& jt_dml_ctx,
                                            int32_t parent);
  static int json_table_resolve_str_const(const ParseNode &parse_tree, ObString& path_str,
                                          ObDMLResolver* dml_resolver, MulModeTableType table_type);
  // json dot notation:
  static int json_pre_check_dot_notation(ParseNode &node, int8_t& depth, bool& exist_fun,
                                        ObJsonBuffer& sql_str, ObDMLResolver* dml_resolver);
  static int json_check_first_node_name(const ObString &node_name, bool &check_res,
                                        ObDMLResolver* dml_resolver);
  static int json_check_size_obj_access_ref(ParseNode *node);
  static int json_check_depth_obj_access_ref(ParseNode *node, int8_t &depth, bool &exist_fun,
                                            ObJsonBuffer &sql_str, bool obj_check = true);  // obj_check : whether need check dot notaion
  static int json_transform_dot_notation2_query(ParseNode &node, const ObString &sql_str,
                                                ObDMLResolver* dml_resolver);
  static int json_transform_dot_notation2_value(ParseNode &node, const ObString &sql_str,
                                                ObDMLResolver* dml_resolver);
  static int json_check_is_json_constraint(ObDMLResolver* dml_resolver, ParseNode *col_node,
                                          bool& is_json_cst, bool& is_json_type,
                                          int8_t only_is_json = 0); // 1 is json & json type ; 0 is json; 2 json type
  static int json_check_column_is_json_type(ParseNode *tab_col, ObDMLResolver* dml_resolver,
                                            ObColumnRefRawExpr *&column_expr, bool &is_json_cst,
                                            bool &is_json_type, int8_t only_is_json = 1);
  static int json_process_dot_notation_in_json_object(ParseNode*& expr_node,
                                                ParseNode* cur_node,
                                                ObDMLResolver* dml_resolver,
                                                int& pos);
  static int json_pre_process_json_constraint(ParseNode *node, ObDMLResolver* dml_resolver);
  static bool json_check_generated_column_with_json_constraint(const ObSelectStmt *stmt,
                                                              const ObColumnRefRawExpr *col_expr,
                                                              ObDMLResolver* dml_resolver);
  // other json expr:
  static int json_expand_column_in_json_object_star(ParseNode *node, ObDMLResolver* dml_resolver);
  static int json_pre_process_one_expr(ParseNode &node, ObDMLResolver* dml_resolver);
  static int json_pre_process_expr(ParseNode &node, ObDMLResolver* dml_resolver);
  static int json_process_object_array_expr_node(ParseNode *node, ObDMLResolver* dml_resolver);
  static int json_process_json_agg_node(ParseNode*& node, ObDMLResolver* dml_resolver);
  // xml
  static int xml_check_column_udt_type(ParseNode *root_node, ObDMLResolver* dml_resolver);
  static int xml_check_xpath(ObDmlJtColDef *col_def,
                            const ObDataType &data_type,
                            ObIArray<ObDmlJtColDef*>& json_table_infos,
                            ObIAllocator* allocator);
  static int xml_table_resolve_xml_namespaces(const ParseNode *namespace_node,
                                              ObJsonTableDef*& table_def,
                                              ObIAllocator* allocator);
  static int xml_replace_col_udt_qname(ObQualifiedName& q_name, ObDMLResolver* dml_resolver);
  static bool xml_table_check_path_need_transform(ObString& path);
  // geo
  static int geo_transform_dot_notation_attr(ParseNode &node, const ObString &sql_str,
                                             const ObColumnRefRawExpr &col_expr,
                                             ObDMLResolver* dml_resolver);
  static int geo_transform_udt_attrbute_name(const ObString &sql_str, ObIAllocator &allocator, ObString &attr_name);
  static int geo_pre_process_mvt_agg(ParseNode &node, ObDMLResolver* dml_resolver);
  static int geo_resolve_mbr_column(ObDMLResolver* dml_resolver);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_RESOLVER_DML_OB_MULTI_MODE_DML_RESOLVER_H_ */

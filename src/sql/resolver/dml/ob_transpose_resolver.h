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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_TRANSPOSE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_TRANSPOSE_RESOLVER_H_
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/resolver/dml/ob_dml_stmt.h"

namespace oceanbase
{
namespace sql
{
class ObDMLResolver;
class TransposeDef;

struct TransposeDef
{
  TransposeDef() : orig_table_item_(NULL), alias_name_() {}
  void reset()
  {
    orig_table_item_ = NULL;
    alias_name_.reset();
  }
  inline virtual bool is_pivot() const = 0;
  inline virtual bool is_unpivot() const = 0;
  inline virtual bool need_use_unpivot_op() const { return false; }
  virtual int assign(const TransposeDef &other);
  int deep_copy(ObIRawExprCopier &expr_copier, const TransposeDef &other);
  TO_STRING_KV(KPC_(orig_table_item), K_(alias_name));
  TableItem *orig_table_item_;
  ObString alias_name_;
};

struct PivotDef : TransposeDef
{
  struct AggrPair
  {
    ObRawExpr *expr_;
    ObString alias_name_;
    AggrPair() : expr_(NULL), alias_name_() {}
    ~AggrPair() {}
    int assign(const AggrPair &other);
    TO_STRING_KV(KPC_(expr), K_(alias_name));
  };

  struct InPair
  {
    common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> const_exprs_;
    ObString name_;
    InPair() : name_() {}
    ~InPair() {}
    int assign(const InPair &other);
    TO_STRING_KV(K_(const_exprs), K_(name));
  };

  PivotDef () {}
  void reset()
  {
    TransposeDef::reset();
    group_columns_.reset();
    group_column_names_.reset();
    for_columns_.reset();
    for_column_names_.reset();
    aggr_pairs_.reset();
    in_pairs_.reset();
  }
  inline bool is_pivot() const { return true; }
  inline bool is_unpivot() const { return false; }
  int assign(const PivotDef &other);
  INHERIT_TO_STRING_KV("BasicDef", TransposeDef, "type", "PIVOT",
               K_(aggr_pairs), K_(group_columns), K_(for_columns), K_(in_pairs), K_(alias_name));

  // group columns of pivot / old columns of unpivot
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> group_columns_;
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> group_column_names_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> for_columns_;
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> for_column_names_;
  common::ObSEArray<AggrPair, 4, common::ModulePageAllocator, true> aggr_pairs_;
  common::ObSEArray<InPair, 4, common::ModulePageAllocator, true> in_pairs_;
};

struct UnpivotDef : TransposeDef
{
  struct InPair
  {
    common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> const_exprs_;
    common::ObSEArray<ObString, 1, common::ModulePageAllocator, true> column_names_;
    common::ObSEArray<ObRawExpr*, 1, common::ModulePageAllocator, true> column_exprs_;
    InPair() {}
    ~InPair() {}
    int assign(const InPair &other);
    TO_STRING_KV(K_(const_exprs), K_(column_names), K_(column_exprs));
  };

  UnpivotDef() {}
  void reset()
  {
    TransposeDef::reset();
    is_include_null_ = false;
    orig_columns_.reset();
    orig_column_names_.reset();
    label_columns_.reset();
    value_columns_.reset();
    in_pairs_.reset();
  }
  inline bool is_pivot() const { return false; }
  inline bool is_unpivot() const { return true; }
  inline bool is_include_null() const { return is_include_null_; }
  inline bool is_exclude_null() const { return !is_include_null_; }
  inline bool need_use_unpivot_op() const { return in_pairs_.count() > 1; }
  void set_include_nulls(const bool is_include_nulls) { is_include_null_ = is_include_nulls; }
  int assign(const UnpivotDef &other);
  INHERIT_TO_STRING_KV("BasicDef", TransposeDef, "type", "UNPIVOT",
               K_(is_include_null), K_(orig_columns), K_(orig_column_names),
               K_(label_columns), K_(value_columns), K_(in_pairs));

  bool is_include_null_;
  common::ObSEArray<ObRawExpr *, 4, common::ModulePageAllocator, true> orig_columns_;
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> orig_column_names_;
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> label_columns_;
  common::ObSEArray<ObRawExprResType, 4, common::ModulePageAllocator, true> label_col_types_;
  common::ObSEArray<ObString, 4, common::ModulePageAllocator, true> value_columns_;
  common::ObSEArray<ObRawExprResType, 4, common::ModulePageAllocator, true> value_col_types_;
  common::ObSEArray<InPair, 4, common::ModulePageAllocator, true> in_pairs_;
};

class ObTransposeResolver
{
public:
  ObTransposeResolver(ObDMLResolver *cur_resolver)
    : cur_resolver_(cur_resolver) {}
  virtual ~ObTransposeResolver() {}

  int resolve(const ParseNode &parse_tree);

  TableItem *get_table_item() { return table_item_; }

private:
  int resolve_transpose_clause(const ParseNode &transpose_node,
                               TableItem &orig_table_item,
                               TransposeDef *&trans_def,
                               common::ObIArray<common::ObString> &columns_in_aggrs);
  int resolve_pivot_clause(const ParseNode &transpose_node,
                           TableItem &orig_table_item,
                           PivotDef &pivot_def,
                           common::ObIArray<common::ObString> &columns_in_aggrs);
  int resolve_unpivot_clause(const ParseNode &transpose_node,
                             TableItem &orig_table_item,
                             UnpivotDef &unpivot_def);
  int check_pivot_aggr_expr(ObRawExpr *expr) const;
  int resolve_column_names(const ParseNode &column_node,
                           ObIArray<ObString> &columns);
  int resolve_columns(const ParseNode &column_node,
                      TableItem &table_item,
                      ObIArray<ObString> &column_names,
                      ObIArray<ObRawExpr *> &columns);
  int resolve_const_exprs(const ParseNode &expr_node,
                          ObIArray<ObRawExpr *> &const_exprs);
  int resolve_table_column_item(const TableItem &table_item,
                                ObString &column_name,
                                ColumnItem *&column_item);
  int resolve_all_table_columns(const TableItem &table_item,
                                ObIArray<ColumnItem> &column_items);
  int get_old_or_group_column(const ObIArray<ObString> &columns_in_aggrs,
                              TableItem &orig_table_item,
                              TransposeDef &trans_def);
  int try_add_cast_to_unpivot(UnpivotDef &unpivot_def);
  int get_merge_type_for_unpivot(ObIArray<ObExprResType> &left_types,
                                 ObIArray<ObExprResType> &right_types,
                                 ObIArray<ObExprResType> &res_types);

  int transform_to_view(TableItem &orig_table_item,
                        TransposeDef &trans_def,
                        TableItem *&view_table);
  int get_exprs_for_pivot_table(PivotDef &pivot_def,
                                ObIArray<ObRawExpr *> &select_exprs,
                                ObIArray<ObRawExpr *> &group_exprs);
  int build_select_expr_for_pivot(PivotDef::AggrPair &aggr_pair,
                                  PivotDef::InPair &in_pair,
                                  ObIArray<ObRawExpr *> &for_exprs,
                                  ObRawExpr *&select_expr);
  int get_exprs_for_unpivot_table(UnpivotDef &unpivot_def,
                                  ObIArray<ObRawExpr *> &select_exprs,
                                  ObIArray<ObRawExpr *> &condition_exprs);
  int get_unpivot_item(UnpivotDef &unpivot_def,
                       ObIArray<ObRawExpr *> &select_exprs,
                       ObUnpivotItem *&unpivot_item);
  int push_transpose_table_into_view(TableItem *&view_table,
                                     TableItem *origin_table,
                                     TransposeDef &trans_def,
                                     ObIArray<ObRawExpr *> &select_exprs,
                                     ObIArray<ObRawExpr *> &conditions,
                                     ObIArray<ObRawExpr *> *group_exprs = NULL);
  int add_new_table_item(TableItem *&new_table_item, const ObString &alias_name);
  int generate_select_list(TableItem *table, ObIArray<ObRawExpr *> &basic_select_exprs);

  int get_column_item_idx_by_name(ObIArray<ColumnItem> &array,
                                  const ObString &var,
                                  int64_t &idx);

  int remove_column_item_by_names(ObIArray<ColumnItem> &column_items,
                                  const ObIArray<ObString> &names);
  int get_combine_name(ObIArray<ObString> &exprs, ObString &comb_name);
  int get_combine_name(ObIArray<ObRawExpr *> &exprs, ObString &comb_name);

private:
  TableItem *table_item_;
  ObDMLResolver *cur_resolver_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_TRANSPOSE_RESOLVER_H_ */

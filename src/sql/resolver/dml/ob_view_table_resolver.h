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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_VIEW_TABLE_RESOLVER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_VIEW_TABLE_RESOLVER_H_
#include "sql/resolver/dml/ob_select_resolver.h"
namespace oceanbase
{
namespace sql
{
class ObViewTableResolver : public ObSelectResolver
{
public:
  ObViewTableResolver(ObResolverParams &params,
                      const ObString &view_db_name, const ObString &view_name)
    : ObSelectResolver(params),
      parent_view_resolver_(NULL),
      is_create_view_(false),
      materialized_(false),
      auto_name_id_(1),
      view_db_name_(view_db_name),
      view_name_(view_name),
      ori_is_in_sys_view_(false)
      {
        is_resolving_view_ = true;
        params_.is_from_create_view_ = params.is_from_create_view_;
        params_.is_from_create_table_ = params.is_from_create_table_;
        params_.is_specified_col_name_ = params.is_specified_col_name_;
      }
  virtual ~ObViewTableResolver()
  {
    params_.is_in_sys_view_ = ori_is_in_sys_view_;
  }

  void set_current_view_item(const TableItem &view_item)
  {
    current_view_item = view_item;
    ori_is_in_sys_view_ = params_.is_in_sys_view_;
    params_.is_in_sys_view_ = params_.is_in_sys_view_ || is_sys_view(current_view_item.ref_id_);
  }
  void set_parent_view_resolver(ObViewTableResolver *parent_view_resolver)
  { parent_view_resolver_ = parent_view_resolver; }
  int check_need_use_sys_tenant(bool &use_sys_tenant) const;
  virtual int check_in_sysview(bool &in_sysview) const override;
  void set_is_create_view(bool is_create_view) { is_create_view_ = is_create_view; }
  void set_materialized(bool materialized) { materialized_ = materialized; }
  bool get_materialized() { return materialized_; }
  void set_auto_name_id(uint64_t auto_name_id) { auto_name_id_ = auto_name_id; }
  uint64_t get_auto_name_id() const { return auto_name_id_; }

protected:
  virtual int do_resolve_set_query(const ParseNode &parse_tree,
                                   ObSelectStmt *&child_stmt,
                                   const bool is_left_child = false);
  virtual int expand_view(TableItem &view_item);
  virtual int resolve_subquery_info(const common::ObIArray<ObSubQueryInfo> &subquery_info);
  int check_view_circular_reference(const TableItem &view_item);
  virtual int resolve_generate_table(const ParseNode &table_node, const ParseNode *alias_node, TableItem *&table_item);
  virtual int set_select_item(SelectItem &select_item, bool is_auto_gen);
  virtual const ObString get_view_db_name() const override { return view_db_name_; }
  virtual const ObString get_view_name() const override { return view_name_; }

protected:
  //在视图的namespace解析当中，所有子查询都必须由ObViewTableResolver来解析
  //current_view_item用来记录当前namespace是由哪个视图(用户创建的视图,不包含generated table)展开的
  TableItem current_view_item;
  //parent_view_resolver用来记录当前namespace所处的视图是由哪个视图展开的
  ObViewTableResolver *parent_view_resolver_;
  //ObViewTableResolver was called by create view stmt
  bool is_create_view_;
  bool materialized_;
  uint64_t auto_name_id_;
  const ObString view_db_name_;
  const ObString view_name_;
  bool ori_is_in_sys_view_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_VIEW_TABLE_RESOLVER_H_ */

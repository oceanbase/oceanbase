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

#ifndef OCEANBASE_SRC_SQL_RESOLVER_DML_OB_COLUMN_NAMESPACE_CHECKER_H_
#define OCEANBASE_SRC_SQL_RESOLVER_DML_OB_COLUMN_NAMESPACE_CHECKER_H_
#include "share/ob_define.h"
#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
namespace oceanbase {
namespace sql {
class ObResolverParams;
class ObDMLStmt;
class TableItem;
struct ObQualifiedName;
struct JoinedTable;
class ObColumnNamespaceChecker {
  class ObTableItemIterator {
  public:
    explicit ObTableItemIterator(const ObColumnNamespaceChecker& table_container)
        : next_table_item_idx_(0), table_container_(table_container)
    {}

    const TableItem* get_next_table_item();

  private:
    int64_t next_table_item_idx_;
    const ObColumnNamespaceChecker& table_container_;
  };

public:
  explicit ObColumnNamespaceChecker(ObResolverParams& resolver_params)
      : params_(resolver_params), equal_columns_(), cur_joined_table_(NULL), check_unique_(true)
  {}

  ~ObColumnNamespaceChecker(){};
  /**
   * check basic column whether exists in these tables of current namespace
   * @param q_name
   * @param table_item, if exists, will return the table item that contain this column
   * @return
   */
  int check_table_column_namespace(const ObQualifiedName& q_name, const TableItem*& table_item);
  int check_using_column_namespace(
      const common::ObString& column_name, const TableItem*& left_table, const TableItem*& right_table);
  int check_column_existence_in_using_clause(const uint64_t table_id, const common::ObString& column_name);
  int add_reference_table(TableItem* table_reference)
  {
    // clear current joined table
    cur_joined_table_ = NULL;
    return all_table_refs_.push_back(table_reference);
  }
  int remove_reference_table(int64_t tid);

  void add_current_joined_table(JoinedTable* joined_table)
  {
    cur_joined_table_ = joined_table;
  }

  int check_rowscn_table_column_namespace(const ObQualifiedName& q_name, const TableItem*& table_item);

  void enable_check_unique()
  {
    check_unique_ = true;
  }
  void disable_check_unique()
  {
    check_unique_ = false;
  }

  const ObResolverParams& get_resolve_params()
  {
    return params_;
  }

  int check_column_exists(const TableItem& table_item, const common::ObString& col_name, bool& is_exist);
  int set_equal_columns(const common::ObIArray<common::ObString>& columns);
  void clear_equal_columns();

private:
  int find_column_in_single_table(const TableItem& table_item, const ObQualifiedName& q_name, bool& need_check_unique);
  int find_column_in_joined_table(const JoinedTable& joined_table, const ObQualifiedName& q_name,
      const TableItem*& found_table, bool& need_check_unique);
  int find_column_in_table(const TableItem& table_item, const ObQualifiedName& q_name, const TableItem*& found_table,
      bool& need_check_unique);
  bool hit_join_table_using_name(const JoinedTable& joined_table, const ObQualifiedName& q_name);
  int check_column_existence_in_using_clause(
      const uint64_t table_id, const common::ObString& column_name, const TableItem& table_item, bool& exist);

private:
  ObResolverParams& params_;
  // record the table root reference by query
  // single(contain basic table, alias table or generated table) table is itself
  // joined table is the root of joined table tree
  common::ObArray<const TableItem*> all_table_refs_;
  common::ObArray<common::ObString> equal_columns_;  // for merge stmt usage
  const JoinedTable* cur_joined_table_;
  bool check_unique_;
  friend class ObTableItemIterator;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SRC_SQL_RESOLVER_DML_OB_COLUMN_NAMESPACE_CHECKER_H_ */

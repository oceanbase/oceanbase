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

#ifndef OCEANBASE_SRC_SQL_OB_DML_STMT_PRINTER_H_
#define OCEANBASE_SRC_SQL_OB_DML_STMT_PRINTER_H_

#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/resolver/expr/ob_raw_expr_printer.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
namespace sql
{

#define PRINT_TABLE_NAME(print_params, table_item)                          \
  do {                                                                		  \
    if (!print_params_.for_dblink_) {                                       \
      PRINT_TABLE_NAME_NORMAL(table_item);                                  \
    } else {                                                                \
      PRINT_TABLE_NAME_FOR_DBLINK(table_item);                              \
    }                                                                       \
  } while (0)

#define PRINT_TABLE_NAME_NORMAL(table_item)                                 \
  do {                                                                		  \
    ObString database_name = table_item->synonym_name_.empty() ?         \
                            ( table_item->is_link_table() ?                 \
                              table_item->link_database_name_ :             \
                              table_item->database_name_ ) :                 \
                             table_item->synonym_db_name_;                  \
    ObString table_name = table_item->synonym_name_.empty() ? table_item->table_name_ : table_item->synonym_name_ ; \
    if (table_item->cte_type_ == TableItem::NOT_CTE) {								      \
      if (!database_name.empty()) {                                         \
        PRINT_IDENT_WITH_QUOT(database_name);                               \
        DATA_PRINTF(".");                                                   \
      }                                                                     \
      PRINT_IDENT_WITH_QUOT(table_name);                                      \
      if (table_item->synonym_name_.empty() && table_item->is_link_type()) {  \
        const ObString &dblink_name = table_item->dblink_name_;               \
        DATA_PRINTF("@%.*s", LEN_AND_PTR(dblink_name));                       \
      } \
    } else {																																\
      PRINT_IDENT_WITH_QUOT(table_name);                                    \
    }																																				\
  } while (0)

#define PRINT_TABLE_NAME_FOR_DBLINK(table_item)                             \
  do {                                                                		  \
    ObString database_name = table_item->database_name_;                    \
    ObString table_name = table_item->table_name_;                          \
    if (table_item->cte_type_ == TableItem::NOT_CTE) {								\
      if (!database_name.empty()) {                                         \
        PRINT_IDENT_WITH_QUOT(database_name);                               \
        DATA_PRINTF(".");                                                   \
      }                                                                     \
      PRINT_IDENT_WITH_QUOT(table_name);                                    \
      if (table_item->is_link_type()) {                                    \
        const ObString &dblink_name = table_item->dblink_name_;             \
        if (table_item->is_reverse_link_) {                                 \
          DATA_PRINTF("@%.*s!", LEN_AND_PTR(dblink_name));                  \
        }  else {                                                           \
          DATA_PRINTF("@%.*s", LEN_AND_PTR(dblink_name));                   \
        }                                                                   \
      }                                                                     \
    } else {																																\
      PRINT_IDENT_WITH_QUOT(table_name);                                    \
    }																																			  \
  } while (0)

#define PRINT_COLUMN_NAME(column_name) \
  do {\
    if (column_name.empty()) { \
    } else {\
      PRINT_IDENT_WITH_QUOT(column_name);\
    }\
  } while (0);

class ObDMLStmtPrinter {
public:
  ObDMLStmtPrinter()=delete;
  ObDMLStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObDMLStmt *stmt,
                   ObSchemaGetterGuard *schema_guard,
                   common::ObObjPrintParams print_params,
                   const ParamStore *param_store = NULL);
  virtual ~ObDMLStmtPrinter();
  void enable_print_temp_table_as_cte() { print_cte_ = true; }
  void disable_print_temp_table_as_cte() { print_cte_ = false; }
  void init(char *buf, int64_t buf_len, int64_t *pos, ObDMLStmt *stmt);
  virtual int do_print() = 0;
  static int set_synonym_name_recursively(ObRawExpr * cur_expr, const ObDMLStmt *stmt);

  int print_from(bool need_from = true);
  int print_semi_join();
  int print_semi_info_to_subquery();
  int print_where();
  int print_order_by();
  int print_limit();
  int print_fetch();
  int print_returning();
  int print_json_table(const TableItem *table_item);
  int print_table(const TableItem *table_item,
                  bool no_print_alias = false);
  int print_table_with_subquery(const TableItem *table_item);
  int print_base_table(const TableItem *table_item);
  int print_hint();
  void set_is_root(bool is_root) { is_root_ = is_root; }
  void set_is_first_stmt_for_hint(bool is_first_stmt) { is_first_stmt_for_hint_ = is_first_stmt; }
  void set_print_params(const ObObjPrintParams& obj_print_params)
  {
    print_params_ = obj_print_params;
  }

  enum SubqueryPrintParam {
    PRINT_BRACKET          =  1 << 0,
    FORCE_COL_ALIAS        =  1 << 1,
    PRINT_CTE       =  1 << 2
  };

  int print_subquery(const ObSelectStmt *subselect_stmt,
                     uint64_t subquery_print_params);
  int print_temp_table_as_cte();

  int print_quote_for_const(ObRawExpr* expr, bool &print_quote);
  int print_expr_except_const_number(ObRawExpr* expr, ObStmtScope scope);
  int print_cte_define_title(TableItem* cte_table);
  int print_cte_define_title(const ObSelectStmt *sub_select_stmt);
  int print_search_and_cycle(const ObSelectStmt *sub_select_stmt);
  bool is_root_stmt() const { return is_root_; }
  int print_with();
private:
  // added for json table
  int print_json_table_nested_column(const TableItem *table_item, const ObDmlJtColDef& col_def);
  int print_json_return_type(int64_t value, ObDataType data_type);
  int print_mysql_json_return_type(int64_t value, ObDataType data_type);
  int print_binary_charset_collation(int64_t value, ObDataType data_type);
  int get_json_table_column_if_exists(int32_t id, ObDmlJtColDef* root, ObDmlJtColDef*& col);
  int build_json_table_nested_tree(const TableItem* table_item, ObIAllocator* allocator, ObDmlJtColDef*& root);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObDMLStmtPrinter);

protected:
  // data members
  char *buf_;
  int64_t buf_len_;
  int64_t *pos_;
  const ObDMLStmt *stmt_;
  bool is_root_;
  bool is_first_stmt_for_hint_;
  bool print_cte_;
  ObSchemaGetterGuard *schema_guard_;
  ObObjPrintParams print_params_;
  ObRawExprPrinter expr_printer_;
  const ParamStore *param_store_;
};

}
}

#endif /* OCEANBASE_SRC_SQL_OB_DML_STMT_PRINTER_H_ */

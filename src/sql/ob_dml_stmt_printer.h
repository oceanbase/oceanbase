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

#define PRINT_TABLE_NAME(table_item)                                		    \
  do {                                                                		  \
    ObString database_name = table_item->synonym_name_.empty() ? \
                              (table_item->is_link_table() ? \
                                 table_item->link_database_name_ : \
                                 table_item->database_name_) : \
                               table_item->synonym_db_name_; \
    ObString table_name = table_item->synonym_name_.empty() ? table_item->table_name_ : table_item->synonym_name_ ; \
    int64_t temp_buf_len = (table_name.length() + database_name.length()) * 4; \
    char temp_buf[temp_buf_len];  \
    ObDataBuffer data_buff(temp_buf, temp_buf_len); \
    if (OB_FAIL(ObCharset::charset_convert(data_buff, table_name, CS_TYPE_UTF8MB4_BIN, \
                                           print_params_.cs_type_, table_name))) { \
    } else if (OB_FAIL(ObCharset::charset_convert(data_buff, database_name, CS_TYPE_UTF8MB4_BIN, \
                                            print_params_.cs_type_, database_name))) { \
    } \
    bool is_oracle_mode = lib::is_oracle_mode();                            \
    if (table_item->cte_type_ == TableItem::NOT_CTE) {											\
      if (!database_name.empty()) {                                         \
        PRINT_QUOT;                                                         \
        DATA_PRINTF("%.*s", LEN_AND_PTR(database_name));                    \
        PRINT_QUOT;                                                         \
        DATA_PRINTF(".");                                                   \
      }                                                                     \
      PRINT_QUOT;                                                           \
      DATA_PRINTF("%.*s", LEN_AND_PTR(table_name));                         \
      PRINT_QUOT;                                                           \
      if (table_item->synonym_name_.empty() && table_item->is_link_table()) { \
        const ObString &dblink_name = table_item->dblink_name_; \
        DATA_PRINTF("@");                                                     \
        PRINT_QUOT;                                                           \
        DATA_PRINTF("%.*s", LEN_AND_PTR(dblink_name));                        \
        PRINT_QUOT;                                                           \
      } \
    } else {																																\
      PRINT_QUOT;                                                           \
      DATA_PRINTF("%.*s", LEN_AND_PTR(table_name));                         \
      PRINT_QUOT;                                                           \
    }																																				\
  } while (0)                                                         			\

class ObDMLStmtPrinter {
public:
  ObDMLStmtPrinter()=delete;
  ObDMLStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObDMLStmt *stmt,
                   ObSchemaGetterGuard *schema_guard,
                   common::ObObjPrintParams print_params,
                   const ParamStore *param_store = NULL);
  virtual ~ObDMLStmtPrinter();
  void enable_print_cte() { print_params_.print_with_cte_ = true; }
  void disable_print_cte() { print_params_.print_with_cte_ = false; }
  void init(char *buf, int64_t buf_len, int64_t *pos, ObDMLStmt *stmt);
  virtual int do_print() = 0;

  int print_from(bool need_from = true);
  int print_semi_info();
  int print_where();
  int print_order_by();
  int print_limit();
  int print_fetch();
  int print_returning();
  int print_json_table(const TableItem *table_item);
  int print_table(const TableItem *table_item,
                  bool no_print_alias = false);
  int print_base_table(const TableItem *table_item);
  int print_hint();
  void set_is_root(bool is_root) { is_root_ = is_root; }
  void set_print_params(const ObObjPrintParams& obj_print_params)
  {
    print_params_ = obj_print_params;
  }
  int print_inline_view(const ObSelectStmt *subselect_stmt,
                     bool print_bracket,
                     const bool force_col_alias = false);
  int print_cte_define();
  int print_cte_define_title(TableItem* cte_table);
  int print_cte_define_title(const ObSelectStmt *sub_select_stmt);
  int print_search_and_cycle(const ObSelectStmt *sub_select_stmt);
  bool is_root_stmt() const { return is_root_; }
  int print_with();
private:
  // added for json table
  int print_json_table_nested_column(const TableItem *table_item, const ObDmlJtColDef& col_def);
  int print_json_return_type(int64_t value, ObDataType data_type);
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
  ObSchemaGetterGuard *schema_guard_;
  ObObjPrintParams print_params_;
  ObRawExprPrinter expr_printer_;
  const ParamStore *param_store_;
};

}
}

#endif /* OCEANBASE_SRC_SQL_OB_DML_STMT_PRINTER_H_ */

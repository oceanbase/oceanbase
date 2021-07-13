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

#ifndef OCEANBASE_SQL_RESOLVER_EXPR_OB_RAW_EXPR_PRINTER_H_
#define OCEANBASE_SQL_RESOLVER_EXPR_OB_RAW_EXPR_PRINTER_H_

#include "sql/resolver/expr/ob_raw_expr.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_smart_call.h"
#include "sql/ob_sql_utils.h"
namespace oceanbase {
namespace common {
class ObTimeZoneInfo;
}
namespace sql {
class ObRawExprPrinter {
#define LEN_AND_PTR(str) (str.length()), (str.ptr())
#define SQL_ESCAPE_STR(str) (to_cstring(ObHexEscapeSqlStr(str)))
#define _DATA_PRINTF(...) databuff_printf(buf_, buf_len_, *pos_, __VA_ARGS__)
#define DATA_PRINTF(...)                               \
  do {                                                 \
    if (OB_SUCC(ret)) {                                \
      if (OB_ISNULL(buf_) || OB_ISNULL(pos_)) {        \
        ret = OB_ERR_UNEXPECTED;                       \
        LOG_WARN("buf_ or pos_ is null", K(ret));      \
      } else if (OB_FAIL(_DATA_PRINTF(__VA_ARGS__))) { \
        LOG_WARN("fail to print", K(ret));             \
      }                                                \
    }                                                  \
  } while (0)

#define PRINT_IDENT(ident_str)                                                                               \
  do {                                                                                                       \
    if (OB_SUCC(ret) &&                                                                                      \
        OB_FAIL(ObSQLUtils::print_identifier(buf_, buf_len_, (*pos_), print_params_.cs_type_, ident_str))) { \
      LOG_WARN("fail to print ident str", K(ret), K(ident_str));                                             \
    }                                                                                                        \
  } while (0)

#define CONVERT_CHARSET_FOR_RPINT(alloc, input_str)                                                           \
  do {                                                                                                        \
    if (OB_SUCC(ret) && OB_FAIL(ObCharset::charset_convert(                                                   \
                            allocator, input_str, CS_TYPE_UTF8MB4_BIN, print_params_.cs_type_, input_str))) { \
      LOG_WARN("fail to gen ident str", K(ret), K(input_str));                                                \
    }                                                                                                         \
  } while (0)

#define PRINT_EXPR(expr)                                         \
  do {                                                           \
    if (OB_SUCCESS == ret && OB_FAIL(SMART_CALL(print(expr)))) { \
      LOG_WARN("fail to print expr", K(ret));                    \
    }                                                            \
  } while (0)

#define SET_SYMBOL_IF_EMPTY(str) \
  do {                           \
    if (0 == symbol.length()) {  \
      symbol = str;              \
    }                            \
  } while (0)

#define BINARY_COLLATION 63
#define INVALID_COLLATION 0

public:
  ObRawExprPrinter();
  ObRawExprPrinter(
      char* buf, int64_t buf_len, int64_t* pos, common::ObObjPrintParams print_params = common::ObObjPrintParams());
  ObRawExprPrinter(bool);
  virtual ~ObRawExprPrinter();

  void init(char* buf, int64_t buf_len, int64_t* pos, ObObjPrintParams print_params);
  int do_print(ObRawExpr* expr, ObStmtScope scope, bool only_column_namespace = false);

private:
  int print(ObRawExpr* expr);

  int print(ObConstRawExpr* expr);
  int print(ObQueryRefRawExpr* expr);
  int print(ObColumnRefRawExpr* expr);
  int print(ObOpRawExpr* expr);
  int print(ObCaseOpRawExpr* expr);
  int print(ObSetOpRawExpr* expr);
  int print(ObAggFunRawExpr* expr);
  int print(ObSysFunRawExpr* expr);
  int print(ObWinFunRawExpr* expr);
  int print(ObFunMatchAgainst* expr);
  int print(ObPseudoColumnRawExpr* expr);

  int print_date_unit(ObRawExpr* expr);
  int print_cast_type(ObRawExpr* expr);

  int print_partition_exprs(ObWinFunRawExpr* expr);
  int print_order_items(ObWinFunRawExpr* expr);
  int print_window_clause(ObWinFunRawExpr* expr);

  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObRawExprPrinter);

private:
  // data members
  char* buf_;
  int64_t buf_len_;
  // avoid to update pos_ between different printers(mainly ObRawExprPrinter
  // and ObSelectStmtPrinter), we definate pointer of pos_ rather than object
  int64_t* pos_;
  ObStmtScope scope_;
  bool only_column_namespace_;
  bool is_inited_;
  const common::ObTimeZoneInfo* tz_info_;
  ObObjPrintParams print_params_;
  const ParamStore* params_;
};

}  // end namespace sql
}  // end namespace oceanbase

#endif  // OCEANBASE_SQL_RESOLVER_EXPR_OB_RAW_EXPR_PRINTER_H_

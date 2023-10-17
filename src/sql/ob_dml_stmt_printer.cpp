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

#define USING_LOG_PREFIX SQL
#include "sql/ob_dml_stmt_printer.h"
#include "sql/ob_select_stmt_printer.h"
#include "sql/ob_sql_context.h"
#include "sql/resolver/dml/ob_del_upd_stmt.h"
#include "common/ob_smart_call.h"
#include "lib/charset/ob_charset.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/monitor/ob_sql_plan.h"
namespace oceanbase
{
using namespace common;
namespace sql
{

ObDMLStmtPrinter::ObDMLStmtPrinter(char *buf, int64_t buf_len, int64_t *pos, const ObDMLStmt *stmt,
                                   ObSchemaGetterGuard *schema_guard,
                                   ObObjPrintParams print_params,
                                   const ParamStore *param_store)
  : buf_(buf),
    buf_len_(buf_len),
    pos_(pos),
    stmt_(stmt),
    is_root_(false),
    is_first_stmt_for_hint_(false),
    print_cte_(false),
    schema_guard_(schema_guard),
    print_params_(print_params),
    expr_printer_(buf, buf_len, pos, schema_guard_, print_params_, param_store),
    param_store_(param_store)
{
}

ObDMLStmtPrinter::~ObDMLStmtPrinter()
{
}

void ObDMLStmtPrinter::init(char *buf, int64_t buf_len, int64_t *pos, ObDMLStmt *stmt)
{
  buf_ = buf;
  buf_len_ = buf_len;
  pos_ = pos;
  stmt_ = stmt;
  print_cte_ = false;
}

int ObDMLStmtPrinter::set_synonym_name_recursively(ObRawExpr * cur_expr, const ObDMLStmt *stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_expr) || OB_ISNULL(stmt)) {
  } else if (cur_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *column_expr = static_cast<ObColumnRefRawExpr *>(cur_expr);
    const TableItem *table_item = stmt->get_table_item_by_id(column_expr->get_table_id());
    if (NULL != table_item && table_item->alias_name_.empty()) {
      column_expr->set_synonym_name(table_item->synonym_name_);
      column_expr->set_synonym_db_name(table_item->synonym_db_name_);
    }
  } else if (cur_expr->get_param_count() > 0) {
    for (int64_t param_idx = 0; param_idx < cur_expr->get_param_count(); ++param_idx) {
      ObRawExpr * param_expr = cur_expr->get_param_expr(param_idx);
      OZ (SMART_CALL(set_synonym_name_recursively(param_expr, stmt)));
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObDMLStmtPrinter::print_hint()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(stmt_->get_query_ctx()) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    const char *hint_begin = "/*+";
    const char *hint_end = " */";
    DATA_PRINTF("%s", hint_begin);
    if (OB_SUCC(ret)) {
      const ObQueryHint &query_hint = stmt_->get_query_ctx()->get_query_hint();
      PlanText plan_text;
      plan_text.buf_ = buf_;
      plan_text.buf_len_ = buf_len_;
      plan_text.pos_ = *pos_;
      plan_text.is_oneline_ = true;
      plan_text.type_ = print_params_.for_dblink_
                        ? EXPLAIN_DBLINK_STMT
                        : EXPLAIN_UNINITIALIZED;  // just for print hint, ExplainType set as invalid type
      if (OB_FAIL(query_hint.print_stmt_hint(plan_text, *stmt_, is_first_stmt_for_hint_))) {
        LOG_WARN("failed to print stmt hint", K(ret));
      } else if (plan_text.pos_ == *pos_) {
        // no hint, roolback buffer!
        *pos_ -= strlen(hint_begin);
      } else {
        *pos_ = plan_text.pos_;
        DATA_PRINTF("%s", hint_end);
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_from(bool need_from)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    int64_t from_item_size = stmt_->get_from_item_size();
    if (from_item_size > 0) {
      if (need_from) {
        DATA_PRINTF(" from ");
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < from_item_size; ++i) {
        const FromItem &from_item = stmt_->get_from_item(i);
        const TableItem *table_item = NULL;
        if (from_item.is_joined_) {
          table_item = stmt_->get_joined_table(from_item.table_id_);
        } else {
          table_item = stmt_->get_table_item_by_id(from_item.table_id_);
        }
        if (OB_ISNULL(table_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table_item should not be NULL", K(ret));
        } else if (OB_FAIL(print_table(table_item))) {
          LOG_WARN("fail to print table", K(ret), K(*table_item));
        } else {
          DATA_PRINTF(",");
        }
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    } else if (0 != stmt_->get_condition_exprs().count()) {
      // create view v as select 1 from dual where 1;
      DATA_PRINTF(" from DUAL");
    } else if (lib::is_oracle_mode() && 0 == from_item_size) {
      // select 1 from dual
      // in oracle mode
      DATA_PRINTF(" from DUAL");
    }
    if (OB_SUCC(ret) && !print_params_.for_dblink_) {
      if (OB_FAIL(print_semi_join())) {
        LOG_WARN("failed to print semi info", K(ret));
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_table_with_subquery(const TableItem *table_item)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_item) ||
      OB_ISNULL(table_item->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else {
    // force print alias name for select item in generated table. Otherwise,
    // create view v as select 1 from dual where 1 > (select `abs(c1)` from (select abs(c1) from t));
    // Definition of v would be:
    // CREATE VIEW `v` AS select 1 AS `1` from DUAL where (1 > (select `abs(c1)` from ((select abs(`test`.`t`.`c1`) from `test`.`t`)) ))
    // `abs(c1)` would be an unknown column.
    const uint64_t subquery_print_params =
                           PRINT_BRACKET |
                           (stmt_->is_select_stmt() ? FORCE_COL_ALIAS : 0);
    if (OB_FAIL(print_subquery(table_item->ref_query_,
                               subquery_print_params))) {
      LOG_WARN("failed to print subquery", K(ret));
    } else if (!table_item->alias_name_.empty()) {
      DATA_PRINTF(" ");
      PRINT_IDENT_WITH_QUOT(table_item->alias_name_);
    } else {
      DATA_PRINTF(" ");
      PRINT_TABLE_NAME(print_params_, table_item);
    }
  }
  return ret;
}

int ObDMLStmtPrinter::print_table(const TableItem *table_item,
                                  bool no_print_alias/*default false*/)
{
  int ret = OB_SUCCESS;
  bool is_stack_overflow = false;
  if (OB_FAIL(check_stack_overflow(is_stack_overflow))) {
    LOG_WARN("failed to check stack overflow", K(ret), K(is_stack_overflow));
  } else if (is_stack_overflow) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("too deep recursive", K(ret), K(is_stack_overflow));
  } else if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    switch (table_item->type_) {
    case TableItem::LINK_TABLE: {
        if (OB_FAIL(print_base_table(table_item))) {
          LOG_WARN("failed to print base table", K(ret), K(*table_item));
        } else if (!no_print_alias && !table_item->alias_name_.empty()) {
          DATA_PRINTF(lib::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`",
                      LEN_AND_PTR(table_item->alias_name_));
        }
        break;
      }
    case TableItem::BASE_TABLE: {
        if (OB_FAIL(print_base_table(table_item))) {
          LOG_WARN("failed to print base table", K(ret), K(*table_item));
        } else if (!no_print_alias && !table_item->alias_name_.empty()) {
          DATA_PRINTF(lib::is_oracle_mode() ? " \"%.*s\"" : " `%.*s`",
                      LEN_AND_PTR(table_item->alias_name_));
        }
        break;
      }
    case TableItem::ALIAS_TABLE: {
        if (OB_FAIL(print_base_table(table_item))) {
          LOG_WARN("failed to print base table", K(ret), K(*table_item));
        //table in insert all can't print alias(bug:
        } else if (!no_print_alias) {
          DATA_PRINTF(" ");
          PRINT_IDENT_WITH_QUOT(table_item->alias_name_);
        }
        break;
      }
    case TableItem::JOINED_TABLE: {
        const JoinedTable *join_table = static_cast<const JoinedTable*>(table_item);
        if (OB_ISNULL(join_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("join_table should not be NULL", K(ret));
        } else {
          // left table
          const TableItem *left_table = join_table->left_table_;
          if (OB_ISNULL(left_table)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("left_table should not be NULL", K(ret));
          } else {
            DATA_PRINTF("(");
            if (OB_SUCC(ret)) {
              if (OB_FAIL(SMART_CALL(print_table(left_table)))) {
                LOG_WARN("fail to print left table", K(ret), K(*left_table));
              }
            }
            // join type
            if (OB_SUCC(ret)) {
              // not support cross join and natural join
              ObString type_str("");
              switch (join_table->joined_type_) {
              case FULL_OUTER_JOIN: {
                  type_str = "full join";
                  break;
                }
              case LEFT_OUTER_JOIN: {
                  type_str = "left join";
                  break;
                }
              case RIGHT_OUTER_JOIN: {
                  type_str = "right join";
                  break;
                }
              case INNER_JOIN: {
                  type_str = "join";
                  break;
                }
              case CONNECT_BY_JOIN: {
                  type_str = "connect by join";
                  break;
                }
              default: {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("unknown join type", K(ret), K(join_table->joined_type_));
                  break;
                }
              }
              DATA_PRINTF(" %.*s ", LEN_AND_PTR(type_str));
            }
            // right table
            if (OB_SUCC(ret)) {
              const TableItem *right_table = join_table->right_table_;
              if (OB_ISNULL(right_table)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("right_table should not be NULL", K(ret));
              } else if (OB_FAIL(SMART_CALL(print_table(right_table)))) {
                LOG_WARN("fail to print right table", K(ret), K(*right_table));
              } else {
                // join conditions
                const ObIArray<ObRawExpr*> &join_conditions =
                    join_table->join_conditions_;
                int64_t join_conditions_size = join_conditions.count();
                if (join_conditions_size > 0) {
                  DATA_PRINTF(" on ");
                  for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_size;
                       ++i) {
                    if (OB_FAIL(expr_printer_.do_print(join_conditions.at(i), T_NONE_SCOPE))) {
                      LOG_WARN("fail to print join condition", K(ret));
                    }
                    DATA_PRINTF(" and ");
                  }
                  if (OB_SUCC(ret)) {
                    *pos_ -= 5; // strlen(" and ")
                  }
                } else {
                  DATA_PRINTF(" on 1=1 ");
                }
                DATA_PRINTF(")");
              }
            }
          }
        }
        break;
      }
    case TableItem::GENERATED_TABLE: {
        if (OB_ISNULL(table_item->ref_query_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table item ref query is null", K(ret));
        // generated_table cannot appear in view_definition
        // view在resolver阶段被转换为生成表, 需要当做基表处理
        } else if (table_item->is_view_table_ && !print_params_.for_dblink_) {
          PRINT_TABLE_NAME(print_params_, table_item);
          if (OB_SUCC(ret)) {
            if (table_item->alias_name_.length() > 0) {
              DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
            }
          }
        } else if (table_item->cte_type_ != TableItem::NOT_CTE && !print_params_.for_dblink_) {
          PRINT_TABLE_NAME(print_params_, table_item);
          if (table_item->alias_name_.length() > 0) {
            DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
          }
        } else if (OB_FAIL(print_table_with_subquery(table_item))) {
          LOG_WARN("failed to print table with subquery", K(ret));
        }
        break;
      }
    case TableItem::CTE_TABLE: {
        PRINT_TABLE_NAME(print_params_, table_item);
        if (! table_item->alias_name_.empty()) {
          DATA_PRINTF(" ");
          PRINT_IDENT_WITH_QUOT(table_item->alias_name_);
        }
        break;
      }
    case TableItem::FUNCTION_TABLE: {
      DATA_PRINTF("TABLE(");
      OZ (expr_printer_.do_print(table_item->function_table_expr_, T_FROM_SCOPE));
      DATA_PRINTF(") ");
      PRINT_IDENT_WITH_QUOT(table_item->alias_name_);
      break;
    }
    case TableItem::JSON_TABLE: {
      DATA_PRINTF("JSON_TABLE(");
      OZ (expr_printer_.do_print(table_item->json_table_def_->doc_expr_, T_FROM_SCOPE));
      OZ (print_json_table(table_item));
      DATA_PRINTF(")");
      DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
      break;
    }
    case TableItem::TEMP_TABLE: {
      if (!print_params_.for_dblink_) {
        PRINT_TABLE_NAME(print_params_, table_item);
        if (!table_item->alias_name_.empty()) {
          DATA_PRINTF(" ");
          PRINT_IDENT_WITH_QUOT(table_item->alias_name_);
        }
      } else if (OB_FAIL(print_table_with_subquery(table_item))) {
        LOG_WARN("failed to print table with subquery", K(ret));
      }
      break;
    }
    case TableItem::VALUES_TABLE: {
      int64_t column_cnt = stmt_->get_column_size(table_item->table_id_);
      const ObIArray<ObRawExpr *> &values = table_item->table_values_;
      if (OB_UNLIKELY(column_cnt <= 0 || values.empty() || values.count() % column_cnt != 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(column_cnt), K(values));
      } else {
        DATA_PRINTF("(VALUES ");
        for (int64_t i = 0; OB_SUCC(ret) && i < values.count(); ++i) {
          if (i % column_cnt == 0) {
            if (i == 0) {
              DATA_PRINTF("ROW(");
            } else {
              DATA_PRINTF("), ROW(");
            }
          }
          if (OB_SUCC(ret)) {
            OZ (expr_printer_.do_print(values.at(i), T_FROM_SCOPE));
            if (OB_SUCC(ret) && (i + 1) % column_cnt != 0) {
              DATA_PRINTF(", ");
            }
          }
        }
        DATA_PRINTF("))");
        DATA_PRINTF(" %.*s", LEN_AND_PTR(table_item->alias_name_));
      }
      break;
    }
    default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown table type", K(ret), K(table_item->type_));
        break;
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_json_return_type(int64_t value, ObDataType data_type)
{
  int ret = OB_SUCCESS;
  if (lib::is_mysql_mode()) {
    if (OB_FAIL(print_mysql_json_return_type(value, data_type))) {
      LOG_WARN("fail to print json table column in mysql mode", K(ret));
    }
  } else {
    ParseNode parse_node;
    parse_node.value_ = value;

    int16_t cast_type = parse_node.int16_values_[OB_NODE_CAST_TYPE_IDX];
    const ObLengthSemantics length_semantics = data_type.get_length_semantics();
    const ObScale scale = data_type.get_scale();

    switch (cast_type) {
      case T_CHAR: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        DATA_PRINTF("char(%d %s)", len, get_length_semantics_str(length_semantics));
        break;
      }
      case T_VARCHAR: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        const int32_t DEFAULT_VARCHAR_LEN = 4000;
        if (BINARY_COLLATION == collation) {
          DATA_PRINTF("varbinary(%d)", len);
        } else {
          // CHARACTER
          if (len == DEFAULT_VARCHAR_LEN) {
            break;
          } else if (length_semantics == LS_BYTE && len == -1) {
            DATA_PRINTF(" VARCHAR2");
            break;
          } else {
            DATA_PRINTF("varchar2(%d %s)", len, get_length_semantics_str(length_semantics));
          }
        }
        break;
      }
      case T_NVARCHAR2: {
        DATA_PRINTF("nvarchar2(%d)", parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
        break;
      }
      case T_NCHAR: {
        DATA_PRINTF("nchar(%d)", parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
        break;
      }
      case T_DATETIME: {
        //oracle mode treats date as datetime
        DATA_PRINTF("date");
        break;
      }
      case T_DATE: {
        DATA_PRINTF("date");
        break;
      }
      case T_TIME: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("time(%d)", scale);
        } else {
          DATA_PRINTF("time");
        }
        break;
      }
      case T_NUMBER: {
        int16_t precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        DATA_PRINTF("number(%d,%d)", precision, scale);
        break;
      }
      case T_NUMBER_FLOAT: {
        int16_t precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
        DATA_PRINTF("float(%d)", precision);
        break;
      }
      case T_TINYINT:
      case T_SMALLINT:
      case T_MEDIUMINT:
      case T_INT32:
      case T_INT: {
        DATA_PRINTF("signed");
        break;
      }
      case T_UTINYINT:
      case T_USMALLINT:
      case T_UMEDIUMINT:
      case T_UINT32:
      case T_UINT64: {
        DATA_PRINTF("unsigned");
        break;
      }
      case T_INTERVAL_YM: {
        int year_scale = ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(scale));
        DATA_PRINTF("interval year(%d) to month", year_scale);
        break;
      }
      case T_INTERVAL_DS: {
        int day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(scale));
        int fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(scale));
        DATA_PRINTF("interval day(%d) to second(%d)", day_scale, fs_scale);
        break;
      }
      case T_TIMESTAMP_TZ: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("timestamp(%d) with time zone", scale);
        } else {
          DATA_PRINTF("timestamp with time zone");
        }
        break;
      }
      case T_TIMESTAMP_LTZ: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("timestamp(%d) with local time zone", scale);
        } else {
          DATA_PRINTF("timestamp with local time zone");
        }
        break;
      }
      case T_TIMESTAMP_NANO: {
        int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
        if (scale >= 0) {
          DATA_PRINTF("timestamp(%d)", scale);
        } else {
          DATA_PRINTF("timestamp");
        }
        break;
      }
      case T_RAW: {
        int32_t len = parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX];
        DATA_PRINTF("raw(%d)", len);
        break;
      }
      case T_FLOAT: {
        const char *type_str = lib::is_oracle_mode() ? "binary_float" : "float";
        DATA_PRINTF("%s", type_str);
        break;
      }
      case T_DOUBLE: {
        const char *type_str = lib::is_oracle_mode() ? "binary_double" : "double";
        DATA_PRINTF("%s", type_str);
        break;
      }
      case T_UROWID: {
        DATA_PRINTF("urowid(%d)", parse_node.int32_values_[OB_NODE_CAST_C_LEN_IDX]);
        break;
      }
      case T_LOB: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        if (BINARY_COLLATION == collation) {
          DATA_PRINTF("blob");
        } else {
          DATA_PRINTF("clob");
        }
        break;
      }
      case T_JSON: {
        DATA_PRINTF("json");
        break;
      }
      case T_LONGTEXT: {
        int16_t collation = parse_node.int16_values_[OB_NODE_CAST_COLL_IDX];
        if (BINARY_COLLATION == collation) {
          DATA_PRINTF("blob");
        } else {
          DATA_PRINTF("clob");
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown cast type", K(ret), K(cast_type));
        break;
      }
    } // end switch
  } // oracle mode
  return ret;
}

int ObDMLStmtPrinter::print_binary_charset_collation(int64_t value, ObDataType data_type)
{
  INIT_SUCC(ret);
  if (data_type.is_binary_collation()) {
    DATA_PRINTF("binary ");
  }
  if (CHARSET_INVALID != data_type.get_charset_type()) {
    DATA_PRINTF("CHARACTER SET %s ", ObCharset::charset_name(data_type.get_charset_type()));
  }
  if (CS_TYPE_INVALID != data_type.get_collation_type()) {
    DATA_PRINTF("COLLATE %s ", ObCharset::collation_name(data_type.get_collation_type()));
  }
  return ret;
}

int ObDMLStmtPrinter::print_mysql_json_return_type(int64_t value, ObDataType data_type)
{
  int ret = OB_SUCCESS;

  ParseNode parse_node;
  parse_node.value_ = value;

  int16_t cast_type = data_type.get_obj_type();
  const ObLengthSemantics length_semantics = data_type.get_length_semantics();
  const ObScale scale = data_type.get_scale();

  switch (cast_type) {
    case T_CHAR: {
      int32_t len = parse_node.int32_values_[0];
      int32_t collation = parse_node.int32_values_[1];
      if (1 == collation) {
        DATA_PRINTF("binary(%d)", len);
      } else {
        DATA_PRINTF("char(%d) ", len);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(print_binary_charset_collation(value, data_type))) {
          LOG_WARN("fail to print binary,charset,collection clause", K(ret));
        }
      }
      break;
    }
    case T_VARCHAR: {
      int32_t collation = parse_node.int32_values_[1];
      int32_t len = parse_node.int32_values_[0];
      const int32_t DEFAULT_VARCHAR_LEN = 4000;
      if (1 == collation) {
        DATA_PRINTF("varbinary(%d) ", len);
      } else {
        DATA_PRINTF("varchar(%d) ", len);
        if (OB_SUCC(ret) && OB_FAIL(print_binary_charset_collation(value, data_type))) {
          LOG_WARN("fail to print binary,charset,collection clause", K(ret));
        }
      }
      break;
    }
    case T_BIT: {
      int32_t len = parse_node.int16_values_[0];
       DATA_PRINTF("bit(%d) ", len);
      break;
    }
    case T_TINYTEXT: {
      int16_t collation = parse_node.int32_values_[1];
      if (1 == collation) {
        DATA_PRINTF("TINYBLOB ");
      } else {
        DATA_PRINTF("TINYTEXT ");
      }
      if (OB_SUCC(ret) && !collation && OB_FAIL(print_binary_charset_collation(value, data_type))) {
        LOG_WARN("fail to print binary,charset,collection clause", K(ret));
      }
      break;
    }
    case T_TEXT: {
      int16_t collation = parse_node.int32_values_[1];
      if (1 == collation) {
        DATA_PRINTF("BLOB ");
      } else {
        DATA_PRINTF("TEXT ");
      }
      if (OB_SUCC(ret) && !collation && OB_FAIL(print_binary_charset_collation(value, data_type))) {
        LOG_WARN("fail to print binary,charset,collection clause", K(ret));
      }
      break;
    }
    case T_MEDIUMTEXT: {
      int16_t collation = parse_node.int32_values_[1];
      if (1 == collation) {
        DATA_PRINTF("MEDIUMBLOB ");
      } else {
        DATA_PRINTF("MEDIUMTEXT ");
      }
      if (OB_SUCC(ret) && !collation && OB_FAIL(print_binary_charset_collation(value, data_type))) {
        LOG_WARN("fail to print binary,charset,collection clause", K(ret));
      }
      break;
    }
    case T_LONGTEXT: {
      int16_t collation = parse_node.int32_values_[1];
      if (1 == collation) {
        DATA_PRINTF("LONGBLOB ");
      } else {
        DATA_PRINTF("LONGTEXT ");
      }
      if (OB_SUCC(ret) && !collation && OB_FAIL(print_binary_charset_collation(value, data_type))) {
        LOG_WARN("fail to print binary,charset,collection clause", K(ret));
      }
      break;
    }
    case T_DATETIME: {
      int16_t scale = parse_node.int16_values_[1];
      if (scale >= 0) {
        DATA_PRINTF("datetime(%d) ", scale);
      } else {
        DATA_PRINTF("datetime ");
      }
      break;
    }
    case T_DATE: {
      DATA_PRINTF("date ");
      break;
    }
    case T_YEAR: {
      DATA_PRINTF("year ");
      break;
    }
    case T_TIMESTAMP: {
      int16_t scale = parse_node.int16_values_[1];
      if (scale >= 0) {
        DATA_PRINTF("timestamp(%d) ", scale);
      } else {
        DATA_PRINTF("timestamp ");
      }
      break;
    }
    case T_TIME: {
      int16_t scale = parse_node.int16_values_[1];
      if (scale >= 0) {
        DATA_PRINTF("time(%d) ", scale);
      } else {
        DATA_PRINTF("time ");
      }
      break;
    }
    case T_NUMBER: {  // number, decimal, fixed, numeric
      int16_t precision = parse_node.int16_values_[OB_NODE_CAST_N_PREC_IDX];
      int16_t scale = parse_node.int16_values_[OB_NODE_CAST_N_SCALE_IDX];
      DATA_PRINTF("DECIMAL(%d,%d) ", precision, scale);
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_TINYINT: {
      if (parse_node.int16_values_[OB_NODE_CAST_COLL_IDX] == 1) {
        DATA_PRINTF("BOOL ");
      } else if (parse_node.int16_values_[OB_NODE_CAST_COLL_IDX] == 2) {
        DATA_PRINTF("BOOLEAN ");
      } else {
        DATA_PRINTF("TINYINT ");
        if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
          DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
        }
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_SMALLINT: {
      DATA_PRINTF("SMALLINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_MEDIUMINT: {
      DATA_PRINTF("MEDIUMINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_INT32: {
      DATA_PRINTF("INTEGER ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_INT: {
      DATA_PRINTF("BIGINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_UTINYINT: {
      DATA_PRINTF("TINYINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_USMALLINT:{
      DATA_PRINTF("SMALLINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_UMEDIUMINT:{
      DATA_PRINTF("MEDIUMINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_UINT32: {
      DATA_PRINTF("INTEGER ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_UINT64: {
      DATA_PRINTF("BIGINT ");
      if (OB_SUCC(ret) && parse_node.int16_values_[0] != -1){
        DATA_PRINTF("(%d)", parse_node.int16_values_[0]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_INTERVAL_YM: {
      int year_scale = ObIntervalScaleUtil::ob_scale_to_interval_ym_year_scale(static_cast<int8_t>(scale));
      DATA_PRINTF("interval year(%d) to month", year_scale);
      break;
    }
    case T_INTERVAL_DS: {
      int day_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_day_scale(static_cast<int8_t>(scale));
      int fs_scale = ObIntervalScaleUtil::ob_scale_to_interval_ds_second_scale(static_cast<int8_t>(scale));
      DATA_PRINTF("interval day(%d) to second(%d)", day_scale, fs_scale);
      break;
    }
    case T_FLOAT: {
      DATA_PRINTF("FLOAT ");
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[0] <= 0) {
      } else {
        DATA_PRINTF("(%d,%d) ", parse_node.int16_values_[0], parse_node.int16_values_[1]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_DOUBLE: {
      DATA_PRINTF("DOUBLE ");
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[0] <= 0) {
      } else {
        DATA_PRINTF("(%d,%d) ", parse_node.int16_values_[0], parse_node.int16_values_[1]);
      }
      if (OB_FAIL(ret)) {
      } else if (parse_node.int16_values_[3]) {
        DATA_PRINTF("UNSIGNED ");
      } else {
        DATA_PRINTF("SIGNED ");
      }
      if (OB_SUCC(ret) && parse_node.int16_values_[2]) {
        DATA_PRINTF("ZEROFILL ");
      }
      break;
    }
    case T_JSON: {
      DATA_PRINTF("json ");
      break;
    }
    case T_GEOMETRY: {
      int32_t flag = parse_node.int32_values_[1];
      if (flag == 0) {
        DATA_PRINTF("GEOMETRY ");
      } else if (flag == 1) {
        DATA_PRINTF("POINT ");
      } else if (flag == 2) {
        DATA_PRINTF("LINESTRING ");
      } else if (flag == 3) {
        DATA_PRINTF("POLYGON ");
      } else if (flag == 4) {
        DATA_PRINTF("MULTIPOINT ");
      } else if (flag == 5) {
        DATA_PRINTF("MULTILINESTRING ");
      } else if (flag == 6) {
        DATA_PRINTF("MULTIPOLYGON ");
      } else if (flag == 7) {
        DATA_PRINTF("GEOMETRYCOLLECTION ");
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unknown cast type", K(ret), K(cast_type));
      break;
    }
  } // end switch
  return ret;
}

int ObDMLStmtPrinter::get_json_table_column_if_exists(int32_t id, ObDmlJtColDef* root, ObDmlJtColDef*& col)
{
  INIT_SUCC(ret);
  common::ObArray<ObDmlJtColDef*> col_stack;
  if (OB_FAIL(col_stack.push_back(root))) {
    LOG_WARN("fail to store col node tmp", K(ret));
  }

  bool exists = false;

  while (OB_SUCC(ret) && !exists && col_stack.count() > 0) {
    ObDmlJtColDef* cur_col = col_stack.at(col_stack.count() - 1);
    if (OB_ISNULL(cur_col)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current column info is null", K(ret));
    } else if (cur_col->col_base_info_.id_ == id) {
      exists = true;
      col = cur_col;
    } else if (cur_col->col_base_info_.parent_id_ < 0
               || cur_col->col_base_info_.col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)) {
      col_stack.remove(col_stack.count() - 1);
      for (size_t i = 0; !exists && i < cur_col->nested_cols_.count(); ++i) {
        ObDmlJtColDef* nest_col = cur_col->nested_cols_.at(i);
        if (OB_ISNULL(nest_col)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current column info is null", K(ret));
        } else if (nest_col->col_base_info_.id_ == id) {
          exists = true;
          col = nest_col;
        } else if (nest_col->col_base_info_.col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)
                  && OB_FAIL(col_stack.push_back(nest_col))) {
          LOG_WARN("fail to store col node tmp", K(ret));
        }
      }
    }
  }

  if (OB_SUCC(ret) && !exists) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to find col node", K(ret));
  }
  return ret;
}

int ObDMLStmtPrinter::build_json_table_nested_tree(const TableItem* table_item, ObIAllocator* allocator, ObDmlJtColDef*& root)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObJtColBaseInfo*>& plain_def = table_item->json_table_def_->all_cols_;

  for (size_t i = 0; OB_SUCC(ret) && i < plain_def.count(); ++i) {
    const ObJtColBaseInfo& info = *plain_def.at(i);
    ObDmlJtColDef* col_def = static_cast<ObDmlJtColDef*>(allocator->alloc(sizeof(ObDmlJtColDef)));
    if (OB_ISNULL(col_def)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate col node", K(ret));
    } else {
      col_def = new (col_def) ObDmlJtColDef();
      col_def->col_base_info_.assign(info);

      if (info.col_type_ != NESTED_COL_TYPE) {
        ColumnItem* col_item = stmt_->get_column_item_by_id(table_item->table_id_, info.output_column_idx_);
        if (OB_ISNULL(col_item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get column item", K(ret), K(info.output_column_idx_));
        } else {
          col_def->error_expr_ = col_item->default_value_expr_;
          col_def->empty_expr_ = col_item->default_empty_expr_;
        }
      }
    }

    if (OB_SUCC(ret)) {
      ObDmlJtColDef* parent = nullptr;
      if (info.parent_id_ < 0) {
        root = col_def;
      } else if (OB_FAIL(get_json_table_column_if_exists(info.parent_id_, root, parent))) {
        LOG_WARN("fail to find col node parent", K(ret), K(info.parent_id_));
      } else if (info.col_type_ == static_cast<int32_t>(NESTED_COL_TYPE)) {
        if (OB_FAIL(parent->nested_cols_.push_back(col_def))) {
          LOG_WARN("fail to store col node", K(ret), K(parent->nested_cols_.count()));
        }
      } else if (OB_FAIL(parent->regular_cols_.push_back(col_def))) {
        LOG_WARN("fail to store col node", K(ret), K(parent->nested_cols_.count()));
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_json_table_nested_column(const TableItem *table_item, const ObDmlJtColDef& col_def)
{
  int ret = OB_SUCCESS;
  ObJsonTableDef* tbl_def  = table_item->json_table_def_;

  bool has_reg_column = false;
  for (size_t i = 0; OB_SUCC(ret) && i < col_def.regular_cols_.count(); ++i) {
    has_reg_column = true;
    ObDmlJtColDef* cur_def = col_def.regular_cols_.at(i);
    if (OB_ISNULL(cur_def)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current column info is null", K(ret));
    } else {
      const ObJtColBaseInfo& col_info = col_def.regular_cols_.at(i)->col_base_info_;
      if (i > 0) {
        DATA_PRINTF(", ");
      }
      if (col_info.is_name_quoted_) {
        PRINT_QUOT;
        DATA_PRINTF("%.*s", LEN_AND_PTR(col_info.col_name_));
        PRINT_QUOT;
        DATA_PRINTF(" ");
      } else {
        DATA_PRINTF("%.*s ", LEN_AND_PTR(col_info.col_name_));
      }

      if (OB_FAIL(ret)) {
      } else if (col_info.col_type_ == static_cast<int32_t>(COL_TYPE_ORDINALITY)) {
        DATA_PRINTF(" for ordinality");
      } else if (col_info.col_type_ == static_cast<int32_t>(COL_TYPE_EXISTS)) {
        // to print returning type
        OZ (print_json_return_type(col_info.res_type_, col_info.data_type_));
        if (OB_SUCC(ret) && col_info.truncate_) {
          DATA_PRINTF(" truncate");
        }

        DATA_PRINTF(" exists");
        if (OB_SUCC(ret) && col_info.path_.length() > 0) {
          DATA_PRINTF(" path \'%.*s\'", LEN_AND_PTR(col_info.path_));
        }
        if (OB_FAIL(ret)) {
        } else if (col_info.on_empty_ == 0) {
          DATA_PRINTF(" false on empty");
        } else if (col_info.on_empty_ == 1) {
          DATA_PRINTF(" true on empty");
        } else if (col_info.on_empty_ == 2) {
          DATA_PRINTF(" error on empty");
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_error_ == 0) {
          DATA_PRINTF(" false on error");
        } else if (col_info.on_error_ == 1) {
          DATA_PRINTF(" true on error");
        } else if (col_info.on_error_ == 2) {
          DATA_PRINTF(" error on error");
        }
      } else if (col_info.col_type_ == static_cast<int32_t>(COL_TYPE_QUERY)) {
        // to print returning type
        OZ (print_json_return_type(col_info.res_type_, col_info.data_type_));
        ObObjType cast_type = col_info.data_type_.get_obj_type();
        if (cast_type != ObJsonType){
          DATA_PRINTF(" format json");
        }

        if (OB_SUCC(ret) && col_info.truncate_) {
          DATA_PRINTF(" truncate");
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.allow_scalar_ == 0) {
          DATA_PRINTF(" allow scalars");
        } else if (col_info.allow_scalar_ == 1) {
          DATA_PRINTF(" disallow scalars");
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.wrapper_ == 0) {
          DATA_PRINTF(" without wrapper");
        } else if (col_info.wrapper_ == 1) {
          DATA_PRINTF(" without array wrapper");
        } else if (col_info.wrapper_ == 2) {
          DATA_PRINTF(" with wrapper");
        } else if (col_info.wrapper_ == 3) {
          DATA_PRINTF(" with array wrapper");
        } else if (col_info.wrapper_ == 4) {
          DATA_PRINTF(" with unconditional wrapper");
        } else if (col_info.wrapper_ == 5) {
          DATA_PRINTF(" with conditional wrapper");
        } else if (col_info.wrapper_ == 6) {
          DATA_PRINTF(" with unconditional array wrapper");
        } else if (col_info.wrapper_ == 7) {
          DATA_PRINTF(" with conditional array wrapper");
        }

        if (OB_SUCC(ret) && col_info.path_.length() > 0) {
          DATA_PRINTF(" path \'%.*s\'", LEN_AND_PTR(col_info.path_));
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_empty_ == 0) {
          DATA_PRINTF(" error on empty" );
        } else if (col_info.on_empty_ == 1) {
          DATA_PRINTF(" null on empty" );
        } else if (col_info.on_empty_ == 2) {
          DATA_PRINTF(" empty on empty" );
        } else if (col_info.on_empty_ == 3) {
          DATA_PRINTF(" empty array on empty" );
        } else if (col_info.on_empty_ == 4) {
          DATA_PRINTF(" empty object on empty" );
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_error_ == 0) {
          DATA_PRINTF(" error on error" );
        } else if (col_info.on_error_ == 1) {
          DATA_PRINTF(" null on error" );
        } else if (col_info.on_error_ == 2) {
          DATA_PRINTF(" empty on error" );
        } else if (col_info.on_error_ == 3) {
          DATA_PRINTF(" empty array on error" );
        } else if (col_info.on_error_ == 4) {
          DATA_PRINTF(" empty object on error" );
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_mismatch_ == 0) {
          DATA_PRINTF(" error on mismatch" );
        } else if (col_info.on_mismatch_ == 1) {
          DATA_PRINTF(" null on mismatch" );
        }
      } else if (col_info.col_type_ == static_cast<int32_t>(COL_TYPE_VALUE)) {
        OZ (print_json_return_type(col_info.res_type_, col_info.data_type_));
        if (col_info.truncate_) {
          DATA_PRINTF(" truncate" );
        }

        if (OB_SUCC(ret) && col_info.path_.length() > 0) {
          DATA_PRINTF(" path \'%.*s\'", LEN_AND_PTR(col_info.path_));
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_empty_ == 0) {
          DATA_PRINTF(" error on empty");
        } else if (col_info.on_empty_ == 1) {
          DATA_PRINTF(" null on empty");
        } else if (col_info.on_empty_ == 2) {
          DATA_PRINTF(" default ");
          if (OB_SUCC(ret)
              && OB_FAIL(expr_printer_.do_print(cur_def->empty_expr_, T_NONE_SCOPE))) {
            LOG_WARN("fail to print default value col", K(ret));
          }
          DATA_PRINTF(" on empty");
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_error_ == 0) {
          DATA_PRINTF(" error on error");
        } else if (col_info.on_error_ == 1) {
          DATA_PRINTF(" null on error");
        } else if (col_info.on_error_ == 2) {
          DATA_PRINTF(" default ");
          if (OB_SUCC(ret)
              && OB_FAIL(expr_printer_.do_print(cur_def->error_expr_, T_NONE_SCOPE))) {
            LOG_WARN("fail to print default value col", K(ret));
          }
          DATA_PRINTF(" on error");
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_mismatch_ == 0) {
          DATA_PRINTF(" error on mismatch");
        } else if (col_info.on_mismatch_ == 1) {
          DATA_PRINTF(" null on mismatch");
        } else if (col_info.on_mismatch_ == 2) {
          DATA_PRINTF(" ignore on mismatch");
        }

        if (OB_FAIL(ret)) {
        } else if (col_info.on_mismatch_type_ == 0) {
          DATA_PRINTF(" (missing data)");
        } else if (col_info.on_mismatch_type_ == 1) {
          DATA_PRINTF(" (extra data)");
        } else if (col_info.on_mismatch_type_ == 2) {
          DATA_PRINTF(" (type error)");
        }
      }
    }
  }

  for (size_t i = 0; OB_SUCC(ret) && i < col_def.nested_cols_.count(); ++i) {
    const ObJtColBaseInfo& col_info = col_def.nested_cols_.at(i)->col_base_info_;
    if (i > 0 || has_reg_column) {
      DATA_PRINTF(",");
    }

    DATA_PRINTF(" nested path \'%.*s\' columns(", LEN_AND_PTR(col_info.path_));
    OZ (print_json_table_nested_column(table_item, *col_def.nested_cols_.at(i)));
    DATA_PRINTF(")");
  }

  return ret;
}

int ObDMLStmtPrinter::print_json_table(const TableItem *table_item)
{
  int ret = OB_SUCCESS;
  ObJsonTableDef* tbl_def  = table_item->json_table_def_;

  ObArenaAllocator alloc;
  ObDmlJtColDef* root_def = nullptr;
  if (OB_SUCC(ret) && OB_FAIL(build_json_table_nested_tree(table_item, &alloc, root_def))) {
    LOG_WARN("fail to build column tree.", K(ret));
  } else if (root_def->col_base_info_.path_.length() > 0) {
    DATA_PRINTF(" , \'%.*s\'", LEN_AND_PTR(root_def->col_base_info_.path_));
  }

  if (OB_SUCC(ret)) {
    if (root_def->col_base_info_.on_empty_ == 0) {
      DATA_PRINTF(" error on empty");
    } else if (root_def->col_base_info_.on_empty_ == 1) {
      DATA_PRINTF(" null on empty");
    } else if (root_def->col_base_info_.on_empty_ == 2) {
      DATA_PRINTF(" default ");
      if (OB_FAIL(expr_printer_.do_print(root_def->empty_expr_, T_NONE_SCOPE))) {
        LOG_WARN("fail to print where expr", K(ret));
      }
      DATA_PRINTF(" on empty");
    }

    if (OB_FAIL(ret)) {
    } else if (root_def->col_base_info_.on_error_ == 0) {
      DATA_PRINTF(" error on error");
    } else if (root_def->col_base_info_.on_error_ == 1) {
      DATA_PRINTF(" null on error");
    } else if (root_def->col_base_info_.on_error_ == 2) {
      DATA_PRINTF(" default ");
      if (OB_SUCC(ret) && OB_FAIL(expr_printer_.do_print(root_def->error_expr_, T_NONE_SCOPE))) {
        LOG_WARN("fail to print default expr", K(ret));
      }
      DATA_PRINTF(" on error");
    }
  }

  DATA_PRINTF(" columns (");
  OZ (print_json_table_nested_column(table_item, *root_def));
  DATA_PRINTF(" )");

  return ret;
}

int ObDMLStmtPrinter::print_base_table(const TableItem *table_item)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (OB_ISNULL(table_item)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_item should not be NULL", K(ret));
  } else if (TableItem::BASE_TABLE != table_item->type_
      && TableItem::ALIAS_TABLE != table_item->type_
      && TableItem::LINK_TABLE != table_item->type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_type should be BASE_TABLE or ALIAS_TABLE or LINK_TABLE", K(ret),
             K(table_item->type_));
  } else {
    PRINT_TABLE_NAME(print_params_, table_item);
    if (OB_SUCC(ret)) {
      // partition
      if (!table_item->access_all_part()) {
        const ObIArray<ObString> &part_names = table_item->part_names_;
        DATA_PRINTF(" partition(");
        for (int64_t i = 0; OB_SUCC(ret) && i < part_names.count(); ++i) {
          PRINT_IDENT_WITH_QUOT(part_names.at(i));
          DATA_PRINTF(",");
        }
        if (OB_SUCC(ret)) {
          --*pos_;
          DATA_PRINTF(")");
        }
      }
      // flashback query
      if (OB_SUCC(ret)) {
        bool explain_non_extend = false;
        if (OB_NOT_NULL(stmt_->get_query_ctx()) &&
            OB_NOT_NULL(stmt_->get_query_ctx()->root_stmt_) &&
            stmt_->get_query_ctx()->root_stmt_->is_explain_stmt()) {
          explain_non_extend = !static_cast<const ObExplainStmt *>
                                (stmt_->get_query_ctx()->root_stmt_)->is_explain_extended();
        }
        if (OB_NOT_NULL(table_item->flashback_query_expr_) &&
            // do not print flashback of link table when explain [basic]
            !(table_item->is_link_table() && explain_non_extend)) {
          if (table_item->flashback_query_type_ == TableItem::USING_TIMESTAMP) {
            DATA_PRINTF(" as of timestamp "); 
            if (OB_FAIL(expr_printer_.do_print(table_item->flashback_query_expr_, T_NONE_SCOPE))) {
              LOG_WARN("fail to print where expr", K(ret));
            }
          } else if (table_item->flashback_query_type_ == TableItem::USING_SCN) {
            if (lib::is_oracle_mode()) {
              DATA_PRINTF(" as of scn "); 
            } else {
              DATA_PRINTF(" as of snapshot "); 
            }
            if (OB_FAIL(expr_printer_.do_print(table_item->flashback_query_expr_, T_NONE_SCOPE))) {
              LOG_WARN("fail to print where expr", K(ret));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected type", K(ret), K(table_item->flashback_query_type_));
          }
        }
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_semi_join()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < stmt_->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = stmt_->get_semi_infos().at(i);
    const TableItem *right_table = NULL;
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
    } else if (OB_ISNULL(right_table = stmt_->get_table_item_by_id(semi_info->right_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else {
      DATA_PRINTF(", ");
      // join type
      ObString type_str("");
      switch (semi_info->join_type_) {
      case LEFT_SEMI_JOIN: {
          type_str = "left semi join";
          break;
        }
      case RIGHT_SEMI_JOIN: {
          type_str = "right semi join";
          break;
        }
      case LEFT_ANTI_JOIN: {
          type_str = "left anti join";
          break;
        }
      case RIGHT_ANTI_JOIN: {
          type_str = "right anti join";
          break;
        }
      default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown join type", K(ret));
          break;
        }
      }
      DATA_PRINTF(" %.*s ", LEN_AND_PTR(type_str));
      // right table
      if (OB_SUCC(ret)) {
        if (OB_FAIL(print_table(right_table))) {
          LOG_WARN("fail to print right table", K(ret), K(*right_table));
        } else {
          // join conditions
          const ObIArray<ObRawExpr*> &join_conditions = semi_info->semi_conditions_;
          int64_t join_conditions_size = join_conditions.count();
          if (join_conditions_size > 0) {
            DATA_PRINTF(" on ");
            for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_size; ++i) {
              if (OB_FAIL(expr_printer_.do_print(join_conditions.at(i), T_NONE_SCOPE))) {
                LOG_WARN("fail to print join condition", K(ret));
              }
              DATA_PRINTF(" and ");
            }
            if (OB_SUCC(ret)) {
              *pos_ -= 5; // strlen(" and ")
            }
          } else {
            DATA_PRINTF(" on 1=1 ");
          }
        }
      }
    }
  }
  return ret;
}

int ObDMLStmtPrinter::print_semi_info_to_subquery()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  }
  for (int i = 0; OB_SUCC(ret) && i < stmt_->get_semi_info_size(); ++i) {
    SemiInfo *semi_info = stmt_->get_semi_infos().at(i);
    const TableItem *right_table = NULL;
    if (OB_ISNULL(semi_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null semi info", K(ret));
    } else if (OB_ISNULL(right_table = stmt_->get_table_item_by_id(semi_info->right_table_id_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpect null table item", K(ret));
    } else {
      DATA_PRINTF(" ");
      // join type
      ObString type_str("");
      switch (semi_info->join_type_) {
      case LEFT_SEMI_JOIN: {
          type_str = "exists";
          break;
        }
      case LEFT_ANTI_JOIN: {
          type_str = "not exists";
          break;
        }
      default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unknown join type", K(ret));
          break;
        }
      }
      DATA_PRINTF("%.*s(select 1 from ", LEN_AND_PTR(type_str));
      // right table
      if (OB_SUCC(ret)) {
        if (OB_FAIL(print_table(right_table))) {
          LOG_WARN("fail to print right table", K(ret), K(*right_table));
        } else {
          // join conditions
          const ObIArray<ObRawExpr*> &join_conditions = semi_info->semi_conditions_;
          int64_t join_conditions_size = join_conditions.count();
          if (join_conditions_size > 0) {
            DATA_PRINTF(" where ");
            for (int64_t i = 0; OB_SUCC(ret) && i < join_conditions_size; ++i) {
              if (OB_FAIL(expr_printer_.do_print(join_conditions.at(i), T_NONE_SCOPE))) {
                LOG_WARN("fail to print join condition", K(ret));
              }
              DATA_PRINTF(" and ");
            }
            if (OB_SUCC(ret)) {
              *pos_ -= 5; // strlen(" and ")
            }
          }
        }
      }
      DATA_PRINTF(") and ");
    }
  }
  if (OB_SUCC(ret) && stmt_->get_semi_info_size() > 0) {
    *pos_ -= 5; // strlen(" and ")
  }
  return ret;
}

int ObDMLStmtPrinter::print_where()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    const ObIArray<ObRawExpr*> &condition_exprs = stmt_->get_condition_exprs();
    int64_t condition_exprs_size = condition_exprs.count();
    if (condition_exprs_size > 0) {
      DATA_PRINTF(" where ");
      for (int64_t i = 0; OB_SUCC(ret) && i < condition_exprs_size; ++i) {
        if (OB_FAIL(expr_printer_.do_print(condition_exprs.at(i), T_WHERE_SCOPE))) {
          LOG_WARN("fail to print where expr", K(ret));
        }
        DATA_PRINTF(" and ");
      }
      if (OB_SUCC(ret)) {
        *pos_ -= 5; // strlen(" and ")
      }
    }
    if (print_params_.for_dblink_) {
      if (condition_exprs_size == 0 && stmt_->get_semi_info_size() > 0) {
        DATA_PRINTF(" where ");
      } else if (condition_exprs_size > 0 && stmt_->get_semi_info_size() > 0) {
        DATA_PRINTF(" and ");
      }
      if (OB_SUCC(ret) && OB_FAIL(print_semi_info_to_subquery())) {
        LOG_WARN("failed to print semi info to subquery", K(ret));
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_quote_for_const(ObRawExpr* expr, bool &print_quote)
{
  int ret = OB_SUCCESS;
  print_quote = false;
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret));
  } else if (expr->is_const_or_param_expr()) {
    print_quote = expr->get_result_type().is_numeric_type();
  }
  return ret;
}

int ObDMLStmtPrinter::print_expr_except_const_number(ObRawExpr* expr, ObStmtScope scope)
{
  int ret = OB_SUCCESS;
  bool print_quote = false;
  if (OB_FAIL(print_quote_for_const(expr, print_quote))) {
    LOG_WARN("failed to check is const number", K(ret));
  } else if (print_quote) {
    DATA_PRINTF("'");
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(expr_printer_.do_print(expr, scope))) {
    LOG_WARN("fail to print order by expr", K(ret));
  } else if (print_quote) {
    DATA_PRINTF("'");
  }
  return ret;
}

int ObDMLStmtPrinter::print_order_by()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else {
    ObArenaAllocator alloc;
    ObConstRawExpr expr(alloc);
    int64_t order_item_size = stmt_->get_order_item_size();
    if (order_item_size > 0) {
      DATA_PRINTF(" order by ");
      for (int64_t i = 0; OB_SUCC(ret) && i < order_item_size; ++i) {
        const OrderItem &order_item = stmt_->get_order_item(i);
        if (OB_FAIL(print_expr_except_const_number(order_item.expr_, T_ORDER_SCOPE))) {
          LOG_WARN("fail to print order by expr", K(ret));
        } else if (lib::is_mysql_mode()) {
          if (is_descending_direction(order_item.order_type_)) {
            DATA_PRINTF("desc");
          }
        } else if (order_item.order_type_ == NULLS_FIRST_ASC) {
          DATA_PRINTF("asc nulls first");
        } else if (order_item.order_type_ == NULLS_LAST_ASC) {//use default value
          /*do nothing*/
        } else if (order_item.order_type_ == NULLS_FIRST_DESC) {//use default value
          DATA_PRINTF("desc");
        } else if (order_item.order_type_ == NULLS_LAST_DESC) {
          DATA_PRINTF("desc nulls last");
        } else {/*do nothing*/}
        DATA_PRINTF(",");
      }
      if (OB_SUCC(ret)) {
        --*pos_;
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_limit()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (stmt_->has_fetch() || is_oracle_mode()) {
    /*有fetch,说明是oracle mode下的fetch填充的limit,这里不应该打印 */
  } else {
    ObRawExpr *offset_expr = stmt_->get_offset_expr();
    ObRawExpr *limit_expr = stmt_->get_limit_expr();
    if (NULL != offset_expr || NULL != limit_expr) {
      DATA_PRINTF(" limit ");
    }
    // offset
    if (OB_SUCC(ret)) {
      if (NULL != offset_expr) {
        if (NULL != print_params_.exec_ctx_) {
          ObArenaAllocator allocator("PrintDMLStmt");
          ObObj result;
          bool got_result = false;
          if (!offset_expr->is_static_const_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("offset expr should be a const int", K(ret), KPC(offset_expr));
          } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(print_params_.exec_ctx_,
                                                                       offset_expr,
                                                                       result,
                                                                       got_result,
                                                                       allocator))) {
            LOG_WARN("failed to calc offset expr", K(ret));
          } else if (!got_result || !result.is_int()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get the result of offset expr", K(ret), KPC(offset_expr));
          } else {
            DATA_PRINTF("%ld", result.get_int());
          }
        } else {
          if (OB_FAIL(expr_printer_.do_print(offset_expr, T_NONE_SCOPE))) {
            LOG_WARN("fail to print offset expr", K(ret));
          }
        }
        DATA_PRINTF(",");
      }
    }
    // limit
    if (OB_SUCC(ret)) {
      if (NULL != limit_expr) {
        if (NULL != print_params_.exec_ctx_) {
          ObArenaAllocator allocator("PrintDMLStmt");
          ObObj result;
          bool got_result = false;
          if (!limit_expr->is_static_const_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("limit expr should be a const int", K(ret), KPC(limit_expr));
          } else if (OB_FAIL(ObSQLUtils::calc_const_or_calculable_expr(print_params_.exec_ctx_,
                                                                       limit_expr,
                                                                       result,
                                                                       got_result,
                                                                       allocator))) {
            LOG_WARN("failed to calc limit expr", K(ret));
          } else if (!got_result || !result.is_int()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get the result of limit expr", K(ret), KPC(limit_expr));
          } else {
            DATA_PRINTF("%ld", result.get_int());
          }
        } else {
          if (OB_FAIL(expr_printer_.do_print(limit_expr, T_NONE_SCOPE))) {
            LOG_WARN("fail to print limit expr", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObDMLStmtPrinter::print_fetch()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(stmt_) || OB_ISNULL(buf_) || OB_ISNULL(pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("stmt_ is NULL or buf_ is NULL or pos_ is NULL", K(ret));
  } else if (stmt_->has_fetch() || (stmt_->has_limit() && is_oracle_mode())) {
    // offset
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_offset_expr()) {
        DATA_PRINTF(" offset ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_offset_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order offset expr", K(ret));
        }
        DATA_PRINTF(" rows");
      }
    }
    // fetch
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_limit_expr()) {
        DATA_PRINTF(" fetch next ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_limit_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order limit expr", K(ret));
        }
        DATA_PRINTF(" rows");
      }
    }
    //fetch percent
    if (OB_SUCC(ret)) {
      if (NULL != stmt_->get_limit_percent_expr()) {
        DATA_PRINTF(" fetch next ");
        if (OB_FAIL(expr_printer_.do_print(stmt_->get_limit_percent_expr(), T_NONE_SCOPE))) {
          LOG_WARN("fail to print order limit expr", K(ret));
        }
        DATA_PRINTF(" percent rows");
      }
    }
    //fetch only/with ties
    if (OB_SUCC(ret) &&
        (NULL != stmt_->get_limit_expr() || NULL != stmt_->get_limit_percent_expr()) ) {
      if (stmt_->is_fetch_with_ties()) {
        DATA_PRINTF(" with ties");
      } else {
        DATA_PRINTF(" only");
      }
    }

  }
  return ret;
}

int ObDMLStmtPrinter::print_returning()
{
  int ret = OB_SUCCESS;
  CK (OB_NOT_NULL(stmt_),
      OB_NOT_NULL(buf_),
      OB_NOT_NULL(pos_));
  CK (stmt_->is_insert_stmt() || stmt_->is_update_stmt() || stmt_->is_delete_stmt());
  if (OB_SUCC(ret)) {
    const ObDelUpdStmt &dml_stmt = static_cast<const ObDelUpdStmt&>(*stmt_);
    const ObIArray<ObRawExpr*> &returning_exprs = dml_stmt.get_returning_exprs();
    if (returning_exprs.count() > 0) {
      DATA_PRINTF(" returning ");
      OZ (set_synonym_name_recursively(returning_exprs.at(0), stmt_));
      OZ (expr_printer_.do_print(returning_exprs.at(0), T_NONE_SCOPE));
      for (uint64_t i = 1; OB_SUCC(ret) && i < returning_exprs.count(); ++i) {
        DATA_PRINTF(",");
        OZ (set_synonym_name_recursively(returning_exprs.at(i), stmt_));
        OZ (expr_printer_.do_print(returning_exprs.at(i), T_NONE_SCOPE));
      }
    }
  }
  return ret;
}

int ObDMLStmtPrinter::print_subquery(const ObSelectStmt *subselect_stmt,
                                     uint64_t subquery_print_params)
{
  int ret = OB_SUCCESS;
  ObSelectStmtPrinter printer(buf_, buf_len_, pos_,
                              subselect_stmt,
                              schema_guard_,
                              print_params_,
                              param_store_,
                              subquery_print_params & FORCE_COL_ALIAS);
  if (subquery_print_params & PRINT_CTE) {
    printer.enable_print_temp_table_as_cte();
  }
  if (subquery_print_params & PRINT_BRACKET) {
    DATA_PRINTF("(");
  }
  if (OB_FAIL(printer.do_print())) {
    LOG_WARN("failed to print sub select printer", K(ret));
  }
  if (subquery_print_params & PRINT_BRACKET) {
    DATA_PRINTF(")");
  }
  return ret;
}

int ObDMLStmtPrinter::print_temp_table_as_cte()
{
  int ret = OB_SUCCESS;
  ObSEArray<ObDMLStmt::TempTableInfo, 8> temp_table_infos;
  if (print_params_.print_origin_stmt_) {
    //do nothing
  } else if (!print_cte_) {
    //do nothing
  } else if (print_params_.for_dblink_) {
    // always print temp table as generated table
    // do nothing
  } else if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (OB_FAIL(const_cast<ObDMLStmt*>(stmt_)->collect_temp_table_infos(temp_table_infos))) {
    LOG_WARN("failed to collect temp table infos", K(ret));
  } else if (temp_table_infos.empty()) {
    //do nothing
  } else {
    DATA_PRINTF(is_oracle_mode() ? "WITH " : "WITH RECURSIVE ");
    for(int64_t i = 0; OB_SUCC(ret) && i < temp_table_infos.count(); ++i) {
      if (temp_table_infos.at(i).table_items_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the cte tableitem can not be empty", K(ret));
      } else {
        TableItem *cte_table = temp_table_infos.at(i).table_items_.at(0);
        if (OB_ISNULL(cte_table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpect null table item", K(ret));
        } else if (OB_FAIL(print_cte_define_title(cte_table))) {
          LOG_WARN("print column name failed", K(ret));
        } else if (OB_FAIL(print_subquery(cte_table->ref_query_, PRINT_BRACKET))) {
          LOG_WARN("print table failed", K(ret));
        } else if (OB_FAIL(print_search_and_cycle(cte_table->ref_query_))) {
          LOG_WARN("print search and cycle failed", K(ret));
        }
      }
      //打印尾巴
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (i == temp_table_infos.count() - 1) {
        DATA_PRINTF(" ");
      } else {
        DATA_PRINTF(", ");
      }
    }
  }
  return ret;
}


int ObDMLStmtPrinter::print_cte_define_title(TableItem* cte_table)
{
  int ret = OB_SUCCESS;
  //递归cte一定是定义了别名列表的
  //打印cte的表一定不要打印库名?
  //DATA_PRINTF(" %.*s)", LEN_AND_PTR(cte_table->table_name_));
  //cte在mysql中定义和使用是，不允许类似 db.cte_name这种使用方法，打印的时候我们
  //不能打印database name
  ObSelectStmt *sub_select_stmt = NULL;
  if (OB_ISNULL(cte_table) || OB_ISNULL(sub_select_stmt = cte_table->ref_query_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null params", K(ret));
  } else {
    PRINT_TABLE_NAME(print_params_, cte_table);
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(cte_table->node_) && OB_NOT_NULL(cte_table->node_->children_[1])) {
    DATA_PRINTF("(");
    const ObIArray<SelectItem> &sub_select_items = sub_select_stmt->get_select_items();
    //打印列
    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_SEARCH_COLUMN == sub_select_items.at(i).expr_->get_expr_type()
          || T_CTE_CYCLE_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        //伪列不需要打印出来
      } else {
        PRINT_IDENT_WITH_QUOT(sub_select_items.at(i).alias_name_);
        if (i != sub_select_items.count() - 1
            && T_CTE_SEARCH_COLUMN != sub_select_items.at(i+1).expr_->get_expr_type()
            && T_CTE_CYCLE_COLUMN != sub_select_items.at(i+1).expr_->get_expr_type()) {
          DATA_PRINTF(", ");
        }
      }
    }
    DATA_PRINTF(")");
  }
  if (OB_SUCC(ret)) {
    DATA_PRINTF(" as ");
  }
  return ret;
}

int ObDMLStmtPrinter::print_cte_define_title(const ObSelectStmt *sub_select_stmt)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sub_select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null params", K(ret));
  } else {
    DATA_PRINTF("(");
    const ObIArray<SelectItem> &sub_select_items = sub_select_stmt->get_select_items();
    //打印列
    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_SEARCH_COLUMN == sub_select_items.at(i).expr_->get_expr_type()
          || T_CTE_CYCLE_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        //伪列不需要打印出来
      } else {
        PRINT_COLUMN_NAME(sub_select_items.at(i).alias_name_);
        if (i != sub_select_items.count() - 1
            && T_CTE_SEARCH_COLUMN != sub_select_items.at(i+1).expr_->get_expr_type()
            && T_CTE_CYCLE_COLUMN != sub_select_items.at(i+1).expr_->get_expr_type()) {
          DATA_PRINTF(", ");
        }
      }
    }
    DATA_PRINTF(") as ");
  }
  return ret;
}

int ObDMLStmtPrinter::print_search_and_cycle(const ObSelectStmt *sub_select_stmt)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("PrintSelectStmt");
  if (OB_ISNULL(sub_select_stmt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the cte table must have ref sub query", K(ret));
  } else {
    const ObIArray<SelectItem> &sub_select_items = sub_select_stmt->get_select_items();
    const ObIArray<OrderItem> &search_items = sub_select_stmt->get_search_by_items();
    if (!search_items.empty()) {
      DATA_PRINTF(" search ");
      if (sub_select_stmt->is_breadth_search()) {
        DATA_PRINTF("breadth first by ");
      } else {
        DATA_PRINTF("depth first by ");
      }
    }

    for (int64_t i = 0; i < search_items.count() && OB_SUCC(ret); ++i) {
      allocator.reuse();
      ObString column_name = ((ObColumnRefRawExpr*)(search_items.at(i).expr_))->get_column_name();
      PRINT_COLUMN_NAME(column_name);
      if (lib::is_mysql_mode()) {
        if (is_descending_direction(search_items.at(i).order_type_)) {
          DATA_PRINTF("DESC ");
        }
      } else if (search_items.at(i).order_type_ == NULLS_FIRST_ASC) {
        DATA_PRINTF("ASC NULLS FIRST ");
      } else if (search_items.at(i).order_type_ == NULLS_LAST_ASC) {//use default value
        /*do nothing*/
      } else if (search_items.at(i).order_type_ == NULLS_FIRST_DESC) {//use default value
        DATA_PRINTF("DESC ");
      } else if (search_items.at(i).order_type_== NULLS_LAST_DESC) {
        DATA_PRINTF("DESC NULLS LAST ");
      }
      if (i != search_items.count() - 1) {
        DATA_PRINTF(", ");
      } else {
        DATA_PRINTF(" ");
      }
    }

    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_SEARCH_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        DATA_PRINTF("set ");
        PRINT_COLUMN_NAME(sub_select_items.at(i).alias_name_);
      }
    }

    const ObIArray<ColumnItem> &cycle_items = sub_select_stmt->get_cycle_items();
    if (!cycle_items.empty()) {
      DATA_PRINTF("cycle ");
    }

    for (int64_t i = 0; i < cycle_items.count() && OB_SUCC(ret); ++i) {
      allocator.reuse();
      ObString column_name = ((ObColumnRefRawExpr*)(cycle_items.at(i).expr_))->get_column_name();
      PRINT_COLUMN_NAME(column_name);
      if (i != cycle_items.count() - 1) {
        DATA_PRINTF(", ");
      } else {
        DATA_PRINTF(" ");
      }
    }

    for (int64_t i = 0; i < sub_select_items.count() && OB_SUCC(ret); ++i) {
      if (T_CTE_CYCLE_COLUMN == sub_select_items.at(i).expr_->get_expr_type()) {
        ObRawExpr *v;
        ObRawExpr *d_v;
        DATA_PRINTF("set ");
        PRINT_COLUMN_NAME(sub_select_items.at(i).alias_name_);
        DATA_PRINTF("to ");
        ((ObPseudoColumnRawExpr*)(sub_select_items.at(i).expr_))->get_cte_cycle_value(v, d_v);
        if (OB_ISNULL(v)
            || OB_ISNULL(d_v)
            || !v->is_const_raw_expr()
            || !d_v->is_const_raw_expr()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Unexpected const expr", K(ret), KPC(v), KPC(d_v));
        } else {
          ObObj non_cycle_vale = ((ObConstRawExpr*)v)->get_value();
          ObObj cycle_vale = ((ObConstRawExpr*)d_v)->get_value();
          DATA_PRINTF("'%.*s' ", LEN_AND_PTR(non_cycle_vale.get_string()));
          DATA_PRINTF("default ");
          DATA_PRINTF("'%.*s' ", LEN_AND_PTR(cycle_vale.get_string()));
        }
      }
    }
  }
  return ret;
}


//把cte的定义完整的恢复出来
int ObDMLStmtPrinter::print_with()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(stmt_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null stmt", K(ret));
  } else if (!print_params_.print_origin_stmt_) {
    //do nothing
  } else if (stmt_->get_cte_definition_size() == 0) {
      // don't print
  } else {
    const ObIArray<TableItem *> &cte_tables = stmt_->get_cte_definitions();
    DATA_PRINTF(is_oracle_mode() ? "WITH " : "WITH RECURSIVE ");
    //恢复定义，cte先放本stmt中T_WITH_CLAUSE产生的的cte，再放parent放过来的
    for (int64_t i = 0; OB_SUCC(ret) && i < cte_tables.count() && OB_SUCC(ret); i++) {
      TableItem* cte_table = cte_tables.at(i);
      //打印定义
      if (OB_ISNULL(cte_table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the cte tableitem can not be null", K(ret));
      } else if (TableItem::NORMAL_CTE == cte_table->cte_type_
                || TableItem::RECURSIVE_CTE == cte_table->cte_type_) {
        if (OB_FAIL(print_cte_define_title(cte_table))) {
          LOG_WARN("print column name failed", K(ret));
        } else if (OB_FAIL(print_subquery(cte_table->ref_query_, PRINT_BRACKET | FORCE_COL_ALIAS))) {
          LOG_WARN("print table failed", K(ret));
        } else if (OB_FAIL(print_search_and_cycle(cte_table->ref_query_))) {
          LOG_WARN("print search and cycle failed", K(ret));
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected cte type", K(ret), K(cte_table->cte_type_));
      }
      //打印尾巴
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (i == cte_tables.count() - 1) {
        DATA_PRINTF(" ");
      } else {
        DATA_PRINTF(", ");
      }
    }
  }
  return ret;
}

} //end of namespace sql
} //end of namespace oceanbase




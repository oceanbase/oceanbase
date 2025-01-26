/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SERVER
#include "ob_table_cg_service.h"
#include "sql/code_generator/ob_static_engine_cg.h"
#include "sql/engine/expr/ob_expr_autoinc_nextval.h" // for ObAutoincNextvalExtra
#include "sql/resolver/dml/ob_delete_resolver.h"  // for resolve partition expr
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/das/ob_das_ir_define.h"
#include "fts/ob_table_fts_cg_service.h"
#include "storage/ob_storage_util.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::sql;

namespace oceanbase
{
namespace table
{
/*
  generate column ref exprs.
  1. generate column reference expr which order is same as schema.
  2. generate stored generated column assign item expr
    when update stored generated column directly or update its reference exprs. such as:
      create table t(`c1` int primary key, `c2` varchar(10), `c3` varchar(10) generated always as (substring(`c2`, 1, 4) stored)));
      - update t set `c3`='abc' where `c1`=1;
      - update t set `c2`='abc' where `c1`=1;
      both need assign `c3`
*/
int ObTableExprCgService::generate_all_column_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  const ObTableSchema *table_schema = ctx.get_table_schema();
  uint64_t column_cnt = 0;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (FALSE_IT(column_cnt = table_schema->get_column_count())) {
  } else if (items.count() != column_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column item count not equal to column count", K(ret), K(items), K(column_cnt));
  } else {
    const ObColumnSchemaV2 *col_schema = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < column_cnt; i++) {
      ObTableColumnItem &item = items.at(i);
      if (OB_ISNULL(col_schema = table_schema->get_column_schema_by_idx(i))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(ctx.get_expr_factory(),
                                                           *col_schema,
                                                           item.expr_))) {
        LOG_WARN("fail to build column expr", K(ret), K(*col_schema));
      }
    }
  }

  // generate generated column assign item expr
  if (OB_SUCC(ret)) {
    const ObColumnSchemaV2 *col_schema = nullptr;
    ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      ObTableAssignment &assign = assigns.at(i);
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column info is null", K(ret), K(assign));
      } else if (!assign.column_info_->is_generated_column())  {
        // do nothing
      } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(assign.column_info_->column_id_))) {
        ret = OB_SCHEMA_ERROR;
        LOG_WARN("fail to get column schema", K(ret), K(assign));
      } else if (OB_FAIL(ObRawExprUtils::build_column_expr(ctx.get_expr_factory(),
                                                           *col_schema,
                                                           assign.column_item_->expr_))) {
        LOG_WARN("fail to build column expr", K(ret), K(*col_schema));
      }
    }
  }

  return ret;
}

/*
  expr tree:
                T_OP_LE
                /    \
    ttl_gen_expr     T_FUN_SYS_CUR_TIMESTAMP
*/
int ObTableExprCgService::generate_expire_expr(ObTableCtx &ctx,
                                               ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObRawExprFactory &expr_factory = ctx.get_expr_factory();
  ObSysFunRawExpr *now_func_expr = nullptr;
  ObOpRawExpr *expire_expr_tmp = nullptr;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), K(ctx));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_FUN_SYS_CUR_TIMESTAMP, now_func_expr))) {
    LOG_WARN("fail to create current timestamp expr", K(ret), K(expr_factory));
  } else if (OB_FAIL(expr_factory.create_raw_expr(T_OP_LE, expire_expr_tmp))) {
    LOG_WARN("fail to create T_OP_LE expr", K(ret), K(expr_factory));
  } else {
    const ObString &ttl_definition = ctx.get_ttl_definition();
    ObArray<ObQualifiedName> columns;
    ObSchemaChecker schema_checker;
    ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();
    ObSQLSessionInfo &sess_info = ctx.get_session_info();
    ObRawExpr *ttl_gen_expr = nullptr;
    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is NULL", K(ret));
    } else if (OB_FAIL(schema_checker.init(*schema_guard))) {
      LOG_WARN("fail to init schema checker", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(ttl_definition,
                                                                   expr_factory,
                                                                   sess_info,
                                                                   ttl_gen_expr,
                                                                   columns,
                                                                   table_schema,
                                                                   false, /* allow_sequence */
                                                                   nullptr,
                                                                   &schema_checker))) {
      LOG_WARN("fail to build expire expr", K(ret), K(ttl_definition));
    } else {
      // 找到生成列引用的列并替换为真正的列
      for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
        const ObQualifiedName &tmp_column = columns.at(i);
        const ObString &col_name = tmp_column.col_name_;
        ObColumnRefRawExpr *tmp_expr = nullptr;
        if (OB_FAIL(ctx.get_expr_from_column_items(col_name, tmp_expr))) {
          LOG_WARN("fail to get expr from column items", K(ret), K(col_name));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(tmp_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr to replace is null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(ttl_gen_expr, tmp_column.ref_expr_, tmp_expr))) {
          LOG_WARN("fail to replace column reference expr", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(expire_expr_tmp->set_param_exprs(ttl_gen_expr, now_func_expr))) {
          LOG_WARN("fail to set expire expr param exprs", K(ret), K(*ttl_gen_expr), K(*now_func_expr));
        } else if (OB_FAIL(expire_expr_tmp->formalize(&sess_info))) {
          LOG_WARN("fail to formailize expire expr", K(ret));
        } else {
          expr = expire_expr_tmp;
        }
      }
    }
  }

  return ret;
}

int ObTableExprCgService::generate_count_expr(ObTableCtx &ctx, ObAggFunRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObAggFunRawExpr *count_expr = nullptr;

  if (OB_FAIL(ctx.get_expr_factory().create_raw_expr(T_FUN_COUNT, count_expr))) {
    LOG_WARN("fail to create raw expr", K(ret));
  } else if (OB_ISNULL(count_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("count_expr is null");
  } else if (OB_FAIL(count_expr->formalize(&ctx.get_session_info()))) {
    LOG_WARN("fail to formailize count expr", K(ret));
  } else {
    expr = count_expr;
  }

  return ret;
}

/*
  expr tree:
      autoinc_nextval expr
            |
      column_conv expr
*/
int ObTableExprCgService::generate_autoinc_nextval_expr(ObTableCtx &ctx,
                                                        const ObTableColumnItem &item,
                                                        ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *column_cnv_expr = item.expr_;
  const ObTableColumnInfo *column_info = nullptr;
  if (OB_ISNULL(column_info = item.column_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column info is NULL", K(ret));
  } else if (!column_info->is_auto_increment_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column item", K(ret), K(item));
  } else if (OB_ISNULL(item.expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column item expr is null", K(ret), K(item));
  } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(ctx.get_expr_factory(),
                                                            ctx.get_allocator(),
                                                            *item.expr_,
                                                            column_cnv_expr,
                                                            &ctx.get_session_info()))) {
    LOG_WARN("fail to build column conv expr", K(ret), K(item));
  } else {
    ObSysFunRawExpr *autoinc_nextval_expr = NULL;
    if (OB_FAIL(ctx.get_expr_factory().create_raw_expr(T_FUN_SYS_AUTOINC_NEXTVAL, autoinc_nextval_expr))) {
      LOG_WARN("fail to create nextval expr", K(ret));
    } else {
      autoinc_nextval_expr->set_func_name(ObString::make_string(N_AUTOINC_NEXTVAL));
      if (OB_FAIL(autoinc_nextval_expr->add_param_expr(column_cnv_expr))) {
        LOG_WARN("fail to add collumn conv expr to function param", K(ret));
      } else if (OB_FAIL(autoinc_nextval_expr->formalize(&ctx.get_session_info()))) {
        LOG_WARN("fail to extract info", K(ret));
      } else if (OB_FAIL(ObAutoincNextvalExtra::init_autoinc_nextval_extra(&ctx.get_allocator(),
                                                                           reinterpret_cast<ObRawExpr *&>(autoinc_nextval_expr),
                                                                           column_info->table_id_,
                                                                           column_info->column_id_,
                                                                           ctx.get_table_name(),
                                                                           column_info->column_name_))) {
        LOG_WARN("fail to init autoinc_nextval_extra", K(ret), K(ctx.get_table_name()), K(item));
      } else {
        expr = autoinc_nextval_expr;
      }
    }
  }

  return ret;
}

/*
  expr tree:
      column conv expr
            |
      cur timestamp expr
*/
int ObTableExprCgService::generate_current_timestamp_expr(ObTableCtx &ctx,
                                                          const ObTableColumnItem &item,
                                                          ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObSysFunRawExpr *tmp_expr = NULL;
  const ObTableColumnInfo *column_info = nullptr;
  if (OB_ISNULL(column_info = item.column_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column info is NULL", K(ret));
  } else if ((!IS_DEFAULT_NOW_OBJ(column_info->default_value_)) && (!column_info->auto_filled_timestamp_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid column item", K(ret), K(item));
  } else if (OB_ISNULL(item.expr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("column item expr is null", K(ret), K(item));
  } else if (OB_FAIL(ctx.get_expr_factory().create_raw_expr(T_FUN_SYS_CUR_TIMESTAMP, tmp_expr))) {
    LOG_WARN("fail to create cur timestamp expr", K(ret));
  } else {
    tmp_expr->set_data_type(ObTimestampType);
    tmp_expr->set_accuracy(item.expr_->get_accuracy());
    tmp_expr->set_func_name(ObString::make_string(N_CUR_TIMESTAMP));
    if (OB_FAIL(tmp_expr->formalize(&ctx.get_session_info()))) {
      LOG_WARN("fail to formalize cur_timestamp_expr", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_column_conv_expr(ctx.get_expr_factory(),
                                                              ctx.get_allocator(),
                                                              *item.expr_,
                                                              reinterpret_cast<ObRawExpr *&>(tmp_expr),
                                                              &ctx.get_session_info()))) {
      LOG_WARN("fail to build column conv expr", K(ret), K(item));
    } else {
      expr = tmp_expr;
    }
  }

  return ret;
}

/*
  build generate column expr
  - delta_expr is for increment or append operation
  - item.expr_ is column ref expr, gen_expr is real calculate expr.
*/
int ObTableExprCgService::build_generated_column_expr(ObTableCtx &ctx,
                                                      ObTableColumnItem &item,
                                                      const ObString &expr_str,
                                                      ObRawExpr *&expr,
                                                      const bool is_inc_or_append/* = false*/,
                                                      sql::ObRawExpr *delta_expr/* = nullptr*/)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (is_inc_or_append && OB_ISNULL(delta_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("delta expr should not be null when do append or increment", K(ret));
  } else {
    ObArray<ObQualifiedName> columns;
    ObSchemaChecker schema_checker;
    ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();
    ObRawExprFactory &expr_factory = ctx.get_expr_factory();
    ObSQLSessionInfo &sess_info = ctx.get_session_info();
    ObRawExpr *gen_expr = nullptr;

    if (OB_ISNULL(schema_guard)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("schema guard is NULL", K(ret));
    } else if (OB_FAIL(schema_checker.init(*schema_guard))) {
      LOG_WARN("fail to init schema checker", K(ret));
    } else if (OB_FAIL(ObRawExprUtils::build_generated_column_expr(expr_str,
                                                                   expr_factory,
                                                                   sess_info,
                                                                   gen_expr,
                                                                   columns,
                                                                   table_schema,
                                                                   false, /* allow_sequence */
                                                                   nullptr,
                                                                   &schema_checker))) {
      LOG_WARN("fail to build generated expr", K(ret), K(expr_str), K(ctx));
    } else if (OB_ISNULL(gen_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("generated column expr is NULL", K(ret));
    } else {
      /*
        1 replace ref columns and add exprs to dependant_exprs.
          such as:
            `K` varbinary(1024),
            `G` varbinary(1024) generated always as (substring(`K`, 1, 4))
          1.1. gen_expr is substring(`K`, 1, 4) which is reference to `K`
          1.2. `G` is depend on `K`, so we record it.
        2. replace second expr with delta expr when do increment or append.
          2.1 increment expr: (IFNULL(`%s`, 0) + `%s`) - %s is column name
          2.2 append expr:(concat_ws('', `%s`, `%s`)) -  %s is column name
        3. get replace expr from assignments firstly.
          such as:
            `c2` varchar(20),
            `c3` varchar(20),
            `g` varchar(30) generated always as (concat(`c2`,`c3`)) stored
          3.1 when only update `c2`, 'g' should update as well.
          3.2 `g` expr should be concat(`c2_assign`,`c3`)
      */
      for (int64_t i = 0; OB_SUCC(ret) && i < columns.count(); i++) {
        const ObQualifiedName &tmp_column = columns.at(i);
        const ObString &col_name = tmp_column.col_name_;
        ObRawExpr *tmp_expr = nullptr;
        if (1 == i && is_inc_or_append) {
          tmp_expr = delta_expr;
        } else if (OB_FAIL(ctx.get_expr_from_assignments(col_name, tmp_expr))) {
          LOG_WARN("fail to get expr from assignments", K(ret), K(col_name));
        } else if (OB_ISNULL(tmp_expr) && OB_FAIL(ctx.get_expr_from_column_items(col_name, tmp_expr))) {
          LOG_WARN("fail to get expr from column items", K(ret), K(col_name));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(tmp_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr to replace is null", K(ret));
        } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(gen_expr, tmp_column.ref_expr_, tmp_expr))) {
          LOG_WARN("fail to replace column reference expr", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(item.dependant_exprs_, tmp_expr))) {
          LOG_WARN("fail to add expr to array", K(ret), K(item), K(*tmp_expr));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(gen_expr->formalize(&sess_info))) {
          LOG_WARN("fail to formailize column reference expr", K(ret));
        } else if (ObRawExprUtils::need_column_conv(item.expr_->get_result_type(), *gen_expr, true)
            && OB_FAIL(ObRawExprUtils::build_column_conv_expr(expr_factory,
                                                              ctx.get_allocator(),
                                                              *item.expr_,
                                                              gen_expr,
                                                              &sess_info))) {
          LOG_WARN("fail to build column conv expr", K(ret));
        } else if (is_inc_or_append) {
          expr = gen_expr; // expr should be a calculate expr in increment or append operation
        } else {
          gen_expr->set_for_generated_column();
          item.expr_->set_dependant_expr(gen_expr);
          expr = item.expr_;
        }
      }
    }
  }

  return ret;
}

/*
  construct exprs, include column ref expr and calculate expr.
  calculate expr:
  - auto increment expr
  - current timestamp expr
  - generate expr
*/
int ObTableExprCgService::resolve_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *table_schema = ctx.get_table_schema();
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  bool is_dml = ctx.is_dml();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (items.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column items is empty", K(ret));
  } else {
    ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();
    ObRawExpr *expr = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
      ObTableColumnItem &item = items.at(i);
      const ObTableColumnInfo *column_info = nullptr;
      if (OB_ISNULL(column_info = item.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret));
      } else {
        if (is_dml) {
          if (column_info->is_auto_increment_ && ctx.need_auto_inc_expr()) {
            if (OB_FAIL(generate_autoinc_nextval_expr(ctx, item, expr))) {
              LOG_WARN("fail to generate autoinc nextval expr", K(ret));
            }
          } else if (IS_DEFAULT_NOW_OBJ(column_info->default_value_)) { // defualt current time
            if (OB_FAIL(generate_current_timestamp_expr(ctx, item, expr))) {
              LOG_WARN("fail to generate autoinc nextval expr", K(ret));
            }
          } else if (column_info->is_generated_column()) {
            const ObString &expr_str = column_info->default_value_.get_string();
            if (OB_FAIL(build_generated_column_expr(ctx, item, expr_str, expr))) {
              LOG_WARN("fail to build generated column expr", K(ret), K(item), K(expr_str));
            } else if (column_info->is_doc_id_column() && ObTableFtsExprCgService::need_calc_doc_id(ctx)) {
              ObRawExpr *&doc_id_expr = static_cast<ObColumnRefRawExpr *>(expr)->get_dependant_expr();
              if (OB_FAIL(ObTableFtsExprCgService::fill_doc_id_expr_param(ctx, doc_id_expr))) {
                LOG_WARN("fail to fill doc_id expr param", K(ret));
              }
            }
          } else {
            expr = item.expr_;
          }
        } else {
          if (column_info->is_generated_column()) {
            const ObString &expr_str = column_info->default_value_.get_string();
            if (OB_FAIL(build_generated_column_expr(ctx, item, expr_str, expr))) {
              LOG_WARN("fail to build generated column expr", K(ret), K(item), K(expr_str));
            }
          } else {
            expr = item.expr_;
          }
        }
      }

      if (OB_SUCC(ret)) {
        item.raw_expr_ = expr;
        if (OB_FAIL(all_exprs.push_back(expr))) {
          LOG_WARN("fail to push back expr to all exprs", K(ret));
        }
      }
    }
  }

  return ret;
}

/*
  generate calc tablet_id exprs when need execute remote das task:
  for dml:
    - calculate global index table tablet_id
    - especailly for insertup/repalce/increment/append/ttl:
      - conflict_checker will lookup primary table to fetch conflict row and
      the confilct row is in remote tablet
  for query when global index back:
    - calculate tablet id to lookup the primary table
*/
int ObTableExprCgService::generate_calc_tablet_id_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &table_index_info = ctx.get_table_index_info();
  const ObTableOperationType::Type op_type = ctx.get_opertion_type();
  for (int64_t i = 0; i < table_index_info.count() && OB_SUCC(ret); i++) {
    const ObTableSchema *index_schema = nullptr;
    ObTableIndexInfo &index_info = table_index_info.at(i);
    if (OB_ISNULL(index_schema = index_info.index_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema is NULL", K(ret), K(i));
    } else {
      ObRawExpr *raw_expr = nullptr;
      if (index_info.is_primary_index_) { // primary index
        if (ctx.need_lookup_calc_tablet_id_expr()) {
          if (OB_FAIL(generate_calc_tablet_id_expr(ctx, *index_schema, raw_expr))) {
            LOG_WARN("fail to generate calc tablet id expr", K(ret));
          } else {
            index_info.lookup_part_id_expr_ = raw_expr;
          }
        }
      } else { // global index
        // for old row
        if (OB_FAIL(generate_calc_tablet_id_expr(ctx, *index_schema, raw_expr))) {
          LOG_WARN("fail to generate calc tablet id expr", K(ret));
        } else {
          index_info.old_part_id_expr_ = raw_expr;
        }
        // for new row
        if (OB_SUCC(ret) && ctx.need_new_calc_tablet_id_expr()) {
          if (OB_FAIL(generate_calc_tablet_id_expr(ctx, *index_schema, raw_expr))) {
            // use column ref expr to generate
            LOG_WARN("fail to generate calc tablet id expr", K(ret));
          } else if (OB_FAIL(replace_assign_column_ref_expr(ctx, raw_expr))) {
            LOG_WARN("fail to replace assign column ref expr", K(ret));
          } else {
            index_info.new_part_id_expr_ = raw_expr;
          }
        }
      }
    }
  } // end for
  return ret;
}

// replace the column_ref_expr in calc_tablet_id expr with assign column ref
int ObTableExprCgService::replace_assign_column_ref_expr(ObTableCtx &ctx, ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
  if (OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("expr is NULL", K(ret));
  }

  for (int64_t i = 0; i < assigns.count() && OB_SUCC(ret); i++) {
    ObRawExpr *column_expr = assigns.at(i).column_expr_;
    ObRawExpr *assign_expr = assigns.at(i).expr_;
    if (OB_ISNULL(column_expr) || OB_ISNULL(assign_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr or assign expr is NULL", K(ret), K(column_expr), K(assign_expr));
    } else if (OB_FAIL(ObRawExprUtils::replace_ref_column(expr, column_expr, assign_expr))) {
      LOG_WARN("fail to replace ref column", K(ret), K(i));
    }
  }
  return ret;
}

int ObTableExprCgService::generate_calc_tablet_id_expr(ObTableCtx &ctx,
                                                       const ObTableSchema &index_schema,
                                                       ObRawExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObRawExpr *part_expr = nullptr;
  ObRawExpr *subpart_expr = nullptr;
  ObRawExprFactory &expr_factory = ctx.get_expr_factory();
  ObSQLSessionInfo &sess_info = ctx.get_session_info();
  ObPartitionLevel part_level = index_schema.get_part_level();

  if (part_level == ObPartitionLevel::PARTITION_LEVEL_ZERO) {
    // do nothing
  } else if (part_level == ObPartitionLevel::PARTITION_LEVEL_ONE ||
             part_level == ObPartitionLevel::PARTITION_LEVEL_TWO) {
    const ObPartitionKeyInfo &partition_keys = index_schema.get_partition_key_info();
    ObSEArray<ObRawExpr*, 4> part_keys_expr;
    if (OB_FAIL(get_part_key_column_expr(ctx, partition_keys, part_keys_expr))) {
      LOG_WARN("fail to get part key column expr", K(ret));
    } else if (OB_FAIL(build_partition_expr(ctx, index_schema, part_keys_expr, false, part_expr))) {
      LOG_WARN("fail to build partition expr", K(ret));
    } else if (part_level == ObPartitionLevel::PARTITION_LEVEL_TWO) {
      ObSEArray<ObRawExpr*, 4> subpart_keys_expr;
      const ObPartitionKeyInfo &subpartition_keys = index_schema.get_subpartition_key_info();
      if (OB_FAIL(get_part_key_column_expr(ctx, subpartition_keys, subpart_keys_expr))) {
        LOG_WARN("fail to get sub part key column expr", K(ret));
      } else if (OB_FAIL(build_partition_expr(ctx, index_schema, subpart_keys_expr, true, subpart_expr))) {
        LOG_WARN("fail to build sub partition expr", K(ret));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part level", K(ret), K(part_level));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObRawExprUtils::build_calc_partition_tablet_id_expr(expr_factory,
                                                                    sess_info,
                                                                    index_schema.get_table_id(),
                                                                    part_level,
                                                                    part_expr,
                                                                    subpart_expr,
                                                                    expr))) {
      LOG_WARN("fail to build calc tablet id expr", K(ret));
    }
  }

  return ret;
}

int ObTableExprCgService::get_part_key_column_expr(ObTableCtx &ctx,
                                                  const ObPartitionKeyInfo &partition_keys,
                                                  ObIArray<sql::ObRawExpr*> &part_keys_expr)
{
  int ret = OB_SUCCESS;
  bool is_global_index_part_key = ctx.has_global_index();
  for (int64_t i = 0; OB_SUCC(ret) && i < partition_keys.get_size(); i++) {
    uint64_t column_id = OB_INVALID_ID;
    const ObTableColumnItem *column_item = nullptr;
    if (OB_FAIL(partition_keys.get_column_id(i, column_id))) {
      LOG_WARN("fail to get column id", K(ret), K(i));
    } else if (OB_FAIL(ctx.get_column_item_by_column_id(column_id, column_item))) {
      LOG_WARN("fail to get column item", K(ret), K(column_id), K(i));
    } else if (OB_ISNULL(column_item) || OB_ISNULL(column_item->column_info_) || OB_ISNULL(column_item->raw_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column item is NULL", K(ret), K(column_item));
    }  else if (!column_item->raw_expr_->is_column_ref_expr() ||
                (is_global_index_part_key &&
                 column_item->column_info_->is_generated_column())) {
      // allow generate col as part key here, because of obkv redis
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("non-common column partition key is not supported", K(ret), K(column_id));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "non-common column partition key");
    } else if (OB_FAIL(part_keys_expr.push_back(column_item->raw_expr_))) {
      LOG_WARN("fail to push back raw expr", K(ret));
    }
  }
  return ret;
}

/*
  generate partition expr
  1. using ObDeleteResolver to build partition expr
  2. replace the column ref expr in partition expr
*/
int ObTableExprCgService::build_partition_expr(ObTableCtx &ctx,
                                               const ObTableSchema &table_schema,
                                               const ObIArray<sql::ObRawExpr*> &part_column_exprs,
                                               bool is_sub_part,
                                               sql::ObRawExpr *&partition_key_expr)
{
  int ret = OB_SUCCESS;
  ObSchemaChecker schema_checker;
  ObResolverParams resolver_ctx;
  if (OB_FAIL(schema_checker.init(*ctx.get_schema_guard()))) {
    LOG_WARN("fail to init schema_checker", K(ret));
  } else {
    ObResolverParams resolver_ctx;
    ObStmtFactory stmt_factory(ctx.get_allocator());
    TableItem table_item;
    resolver_ctx.allocator_ = &ctx.get_allocator();
    resolver_ctx.schema_checker_ = &schema_checker;
    resolver_ctx.session_info_ = &ctx.get_session_info();
    resolver_ctx.disable_privilege_check_ = PRIV_CHECK_FLAG_DISABLE;
    resolver_ctx.expr_factory_ = &ctx.get_expr_factory();
    resolver_ctx.stmt_factory_ = &stmt_factory;
    resolver_ctx.query_ctx_ = stmt_factory.get_query_ctx();
    table_item.table_id_ = table_schema.get_table_id();
    table_item.ref_id_ = table_schema.get_table_id();
    table_item.type_ = TableItem::BASE_TABLE;
    ObPartitionFuncType part_type;
    SMART_VAR (ObDeleteResolver, delete_resolver, resolver_ctx) {
      ObString part_str;
      ObDeleteStmt *delete_stmt = delete_resolver.create_stmt<ObDeleteStmt>();
      if (OB_ISNULL(delete_stmt) || OB_ISNULL(resolver_ctx.query_ctx_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("delete_stmt or query_ctx is NULL", K(delete_stmt), K(resolver_ctx.query_ctx_));
      } else if (OB_FAIL(delete_stmt->get_table_items().push_back(&table_item))) {
        LOG_WARN("fail to push back table item", K(ret));
      } else if (OB_FAIL(delete_stmt->set_table_bit_index(table_schema.get_table_id()))) {
        LOG_WARN("fail to set table bit index", K(ret));
      } else if (!is_sub_part) {
        part_str = table_schema.get_part_option().get_part_func_expr_str();
        part_type = table_schema.get_part_option().get_part_func_type();
      } else {
        part_str = table_schema.get_sub_part_option().get_part_func_expr_str();
        part_type = table_schema.get_sub_part_option().get_part_func_type();
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(delete_resolver.resolve_partition_expr(table_item,
                                                           table_schema,
                                                           part_type,
                                                           part_str,
                                                           partition_key_expr))) {
          LOG_WARN("fail to resolve partition expr", K(ret), K(part_type));
        } else if (OB_ISNULL(partition_key_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition_key_expr is NULL", K(ret), K(part_type));
        }
      }
    } // end smart_var

    if (OB_SUCC(ret)) {
      // replace the column_ref_expr in part_expr:
      if (OB_FAIL(replace_column_ref_in_part_expr(part_column_exprs, partition_key_expr))) {
        LOG_WARN("fail to replace column ref in part expr", K(ret), K(part_type));
      } else if (OB_FAIL(partition_key_expr->formalize(&ctx.get_session_info()))) {
        LOG_WARN("fail to formalize partition expr", K(ret), K(part_type));
      }
    }
  }

  return ret;
}

// replace the column_ref_expr in partition_key_expr with sepcified partition column_ref_expr
int ObTableExprCgService::replace_column_ref_in_part_expr(const ObIArray<sql::ObRawExpr*> &part_column_exprs,
                                                          sql::ObRawExpr *&partition_key_expr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(partition_key_expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_key_expr is NULL", K(ret));
  } else if (partition_key_expr->is_column_ref_expr()) {
    ObColumnRefRawExpr *column_ref = static_cast<ObColumnRefRawExpr*>(partition_key_expr);
    ObColumnRefRawExpr *dst_column_ref = nullptr;
    if (part_column_exprs.count() != 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition key count is not 1", K(ret), K(part_column_exprs.count()));
    } else if (OB_ISNULL(part_column_exprs.at(0)) || !part_column_exprs.at(0)->is_column_ref_expr()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part column expr is NULL or not a column ref", K(ret), K(part_column_exprs.at(0)));
    } else if (FALSE_IT(dst_column_ref = static_cast<ObColumnRefRawExpr *>(part_column_exprs.at(0)))) {
    } else if (column_ref->get_column_id() != dst_column_ref->get_column_id()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column id is not match", K(ret), K(column_ref->get_column_id()), K(dst_column_ref->get_column_id()));
    } else {
      ObRawExpr *real_dest_col = dst_column_ref;
      // for generated col, the calculate expr may stored in dependant_expr,
      //  should replaced with calculate expr instead of column ref expr
      if (OB_NOT_NULL(dst_column_ref->get_dependant_expr())
          && dst_column_ref->get_dependant_expr()->is_for_generated_column()) {
        real_dest_col = dst_column_ref->get_dependant_expr();
      }
      if (OB_FAIL(ObRawExprUtils::replace_ref_column(partition_key_expr,
                                                      column_ref,
                                                      real_dest_col))) {
        LOG_WARN("fail to replace column ref expr", K(ret));
      }
    }
  } else {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < partition_key_expr->get_param_count(); i++) {
      ObRawExpr *sub_expr = partition_key_expr->get_param_expr(i);
      if (OB_ISNULL(sub_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sub_expr should not be null", K(ret));
      } else if (sub_expr->is_column_ref_expr()) {
        bool found = false;
        ObColumnRefRawExpr *src_column_ref = static_cast<ObColumnRefRawExpr*>(sub_expr);
        for (int64_t j = 0; OB_SUCC(ret) && !found && j < part_column_exprs.count(); j++) {
          ObColumnRefRawExpr *dst_column_ref = nullptr;
          if (OB_ISNULL(part_column_exprs.at(j)) || !part_column_exprs.at(j)->is_column_ref_expr()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("part column expr is NULL or not a column ref", K(ret), KPC(part_column_exprs.at(j)));
          } else if (FALSE_IT(dst_column_ref = static_cast<ObColumnRefRawExpr *>(part_column_exprs.at(j)))) {
          } else if (src_column_ref->get_column_id() == dst_column_ref->get_column_id()) {
            found = true;
            ObRawExpr *real_dest_col = dst_column_ref;
            // for generated col, the calculate expr may stored in dependant_expr
            //  should replaced with calculate expr instead of column ref expr
            if (OB_NOT_NULL(dst_column_ref->get_dependant_expr())
                && dst_column_ref->get_dependant_expr()->is_for_generated_column()) {
              real_dest_col = dst_column_ref->get_dependant_expr();
            }
            if (OB_FAIL(ObRawExprUtils::replace_ref_column(partition_key_expr,
                                                            src_column_ref,
                                                            real_dest_col))) {
              LOG_WARN("fail to replace column ref expr", K(ret), K(i), K(j), K(src_column_ref->get_column_id()));
            }
          }
        } // end for
      }
    } // end for
  }
  return ret;
}

//  generate assign expr(column ref expr or calculate expr) for assignment.
int ObTableExprCgService::generate_assign_expr(ObTableCtx &ctx, ObTableAssignment &assign)
{
  int ret = OB_SUCCESS;
  ObRawExpr *tmp_expr = nullptr;
  ObTableColumnItem *item = assign.column_item_;
  const ObTableColumnInfo *column_info = nullptr;
  if (OB_ISNULL(item) || OB_ISNULL(column_info = item->column_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column item is null", K(ret), KP(item));
  } else if (FALSE_IT(assign.column_expr_ = item->expr_)) {
  } else if (column_info->is_auto_increment_ && ctx.need_auto_inc_expr()) {
    if (OB_FAIL(generate_autoinc_nextval_expr(ctx, *item, tmp_expr))) {
      LOG_WARN("fail to generate autoinc nextval expr", K(ret));
    }
  } else if (IS_DEFAULT_NOW_OBJ(column_info->default_value_) || column_info->auto_filled_timestamp_) { // defualt current time or on update current_timestamp
    if (OB_FAIL(generate_current_timestamp_expr(ctx, *item, tmp_expr))) {
      LOG_WARN("fail to generate autoinc nextval expr", K(ret));
    }
  } else if (column_info->is_generated_column()) {
    // the gen_expr is dependent_expr in tmp_expr
    if (OB_FAIL(build_generated_column_expr(ctx, *item, column_info->generated_expr_str_, tmp_expr))) {
      LOG_WARN("fail to build generated column expr", K(ret), K(*item));
    }
  } else if (assign.is_inc_or_append_) {
    bool is_inc_or_append = true;
    if (OB_FAIL(build_generated_column_expr(ctx, *item, assign.generated_expr_str_, tmp_expr, is_inc_or_append, assign.delta_expr_))) {
      LOG_WARN("fail to build generated column expr", K(ret), K(*item));
    }
  } else {
    // generate column ref expr
    const ObTableSchema *table_schema = ctx.get_table_schema();
    const ObColumnSchemaV2 *col_schema = nullptr;
    ObColumnRefRawExpr *tmp_ref_expr = nullptr;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(column_info->column_id_))) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("fail to get column schema", K(ret), K(*item));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(ctx.get_expr_factory(), *col_schema, tmp_ref_expr))) {
      LOG_WARN("fail to build column expr", K(ret));
    }
    tmp_expr = tmp_ref_expr;
  }

  if (OB_SUCC(ret)) {
    assign.expr_ = tmp_expr;
  }

  return ret;
}

/*
  generate delta expr for increment or append operation.
  increment expr: IFNULL(`c1`, 0) + `c1_delta`
  append expr: concat_ws('', `c1`, `c1_delta`)
*/
int ObTableExprCgService::generate_delta_expr(ObTableCtx &ctx, ObTableAssignment &assign)
{
  int ret = OB_SUCCESS;
  ObTableColumnItem *item = assign.column_item_;
  const ObTableColumnInfo *column_info = nullptr;
  if (OB_ISNULL(item) || OB_ISNULL(column_info = item->column_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column item is null", K(ret), KP(item));
  }  else if (!assign.is_inc_or_append_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid assignment", K(ret), K(assign));
  } else {
    // generate column ref expr
    const ObTableSchema *table_schema = ctx.get_table_schema();
    const ObColumnSchemaV2 *col_schema = nullptr;
    ObColumnRefRawExpr *tmp_ref_expr = nullptr;
    if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is null", K(ret));
    } else if (OB_ISNULL(col_schema = table_schema->get_column_schema(column_info->column_id_))) {
      ret = OB_SCHEMA_ERROR;
      LOG_WARN("fail to get column schema", K(ret), K(*item));
    } else if (OB_FAIL(ObRawExprUtils::build_column_expr(ctx.get_expr_factory(), *col_schema, tmp_ref_expr))) {
      LOG_WARN("fail to build column expr", K(ret));
    }
    assign.delta_expr_ = tmp_ref_expr;
    assign.column_expr_ = item->expr_; // column_expr_ ref to old expr
  }

  return ret;
}

/*
  generate assign exprs for update or insertup operation.
  - increment and append operations use insertup executor.
  - generate delta expr for increment and append operations.
  - generate assign expr.
  - push back assign expr to all exprs, assign expr need alloc frame as well.
*/
int ObTableExprCgService::generate_assignments(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
  ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();

  if (!ctx.is_for_update() && !ctx.is_for_insertup()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected operation", K(ret), K(ctx));
  } else if (assigns.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("assigns is empty", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      ObTableAssignment &assign = assigns.at(i);
      if (assign.is_inc_or_append_ && OB_FAIL(generate_delta_expr(ctx, assign))) {
        LOG_WARN("fail to generate delta expr", K(ret), K(assign));
      } else if (OB_FAIL(generate_assign_expr(ctx, assign))) {
        LOG_WARN("fail to generate assign expr", K(ret), K(assign));
      } else if (OB_FAIL(all_exprs.push_back(assign.expr_))) {
        LOG_WARN("fail to push back expr to all exprs", K(ret));
      } else if (assign.is_inc_or_append_ && OB_FAIL(all_exprs.push_back(assign.expr_))) {
        LOG_WARN("fail to push back delta expr to all exprs", K(ret));
      }
    }
  }

  return ret;
}

// generate filter exprs and push back to all exprs.
// currently there is only expire expr in filter exprs.
int ObTableExprCgService::generate_filter_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (ctx.is_ttl_table()) {
    ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();
    ObIArray<sql::ObRawExpr *> &filter_exprs = ctx.get_filter_exprs();
    ObRawExpr *expire_expr = nullptr;
    if (OB_FAIL(generate_expire_expr(ctx, expire_expr))) {
      LOG_WARN("fail to generate expire expr", K(ret), K(ctx));
    } else if (OB_FAIL(filter_exprs.push_back(expire_expr))) {
      LOG_WARN("fail to push back expire expr", K(ret), K(filter_exprs));
    } else if (OB_FAIL(all_exprs.push_back(expire_expr))) {
      LOG_WARN("fail to push back expire expr to all exprs", K(ret));
    }
  }

  return ret;
}

int ObTableExprCgService::generate_aggregate_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;

  if (!ctx.is_count_all()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("only support count(*) pushdown", K(ret));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "non count(*) aggregate pushdown");
  } else {
    ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();
    ObIArray<sql::ObAggFunRawExpr *> &aggr_exprs = ctx.get_pushdown_aggr_exprs();
    ObAggFunRawExpr *count_expr = nullptr;
    if (OB_FAIL(generate_count_expr(ctx, count_expr))) {
      LOG_WARN("fail to generate count expr", K(ret), K(ctx));
    } else if (OB_FAIL(aggr_exprs.push_back(count_expr))) {
      LOG_WARN("fail to push back count expr", K(ret), K(aggr_exprs));
    } else if (OB_FAIL(all_exprs.push_back(count_expr))) {
      LOG_WARN("fail to push back count expr to all exprs", K(ret));
    }
  }

  return ret;
}

/*
  generate all expressions.
  - generate all column exprs firstly.
  - resolve_exprs is for generate calculate expr if need, and push back expr to all_exprs.
  - generate assign expr when update.
  - generate expr frame info finally.
*/
int ObTableExprCgService::generate_exprs(ObTableCtx &ctx,
                                         oceanbase::common::ObIAllocator &allocator,
                                         oceanbase::sql::ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_all_column_exprs(ctx))) { // 1. generate all column exprs and add to column item array
    LOG_WARN("fail to generate all column exprs", K(ret), K(ctx));
  } else if (OB_FAIL(resolve_exprs(ctx))) { // 2. resolve exprs, such as generate expr.
    LOG_WARN("fail to resolve exprs", K(ret), K(ctx));
  } else if ((ctx.is_for_update() || ctx.is_for_insertup()) && OB_FAIL(generate_assignments(ctx))) {
    LOG_WARN("fail to generate assign infos", K(ret), K(ctx));
  } else if (OB_FAIL(generate_filter_exprs(ctx))) {
    LOG_WARN("fail to generate filer exprs", K(ret), K(ctx));
  } else if (ctx.is_count_all() && OB_FAIL(generate_aggregate_exprs(ctx))) {
    LOG_WARN("fail to generate aggregate exprs", K(ret), K(ctx));
  } else if (ctx.need_dist_das() && OB_FAIL(generate_calc_tablet_id_exprs(ctx))) {
    LOG_WARN("fail to generate calc tablet id exprs", K(ret), K(ctx));
  } else if (ctx.is_text_retrieval_scan() && OB_FAIL(ObTableFtsExprCgService::generate_text_retrieval_dep_exprs(ctx, allocator))) {
    LOG_WARN("fail to generate text retrieval dependent exprs", K(ret));
  } else if (OB_FAIL(generate_expr_frame_info(ctx, allocator, expr_frame_info))) {
    LOG_WARN("fail to generate expr frame info", K(ret), K(ctx));
  }

  return ret;
}

// we need to make sure all column ref exprs have beed added, cause scan need column ref exprs.
int ObTableExprCgService::add_extra_column_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  for (int64_t i = 0; OB_SUCC(ret) && i < items.count(); i++) {
    const ObTableColumnItem &item = items.at(i);
    const ObTableColumnInfo *column_info = nullptr;
    if (OB_ISNULL(column_info = item.column_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(ret));
    } else if (column_info->is_auto_increment_) {
      // do nothing, auto_increment column ref expr has been add in column conv expr param
    } else if (OB_FAIL(add_var_to_array_no_dup(all_exprs, static_cast<ObRawExpr*>(item.expr_)))) {
      LOG_WARN("fail to add column expr", K(ret), K(all_exprs), K(item));
    }
  }

  return ret;
}

int ObTableExprCgService::add_all_calc_tablet_id_exprs(ObTableCtx &ctx)
{
  int ret = OB_SUCCESS;
  ObIArray<ObRawExpr *> &all_exprs = ctx.get_all_exprs_array();
  ObIArray<ObTableIndexInfo> &table_index_info = ctx.get_table_index_info();
  for (int64_t i = 0; i < table_index_info.count() && OB_SUCC(ret); i++) {
      ObTableIndexInfo &index_info = table_index_info.at(i);
    if (OB_NOT_NULL(index_info.old_part_id_expr_) &&
                OB_FAIL(all_exprs.push_back(index_info.old_part_id_expr_))) {
      LOG_WARN("fail to push back calc part id expr", K(ret));
    } else if (OB_NOT_NULL(index_info.new_part_id_expr_) &&
                OB_FAIL(all_exprs.push_back(index_info.new_part_id_expr_))) {
      LOG_WARN("fail to push back calc part id expr", K(ret));
    } else if (OB_NOT_NULL(index_info.lookup_part_id_expr_) &&
                OB_FAIL(all_exprs.push_back(index_info.lookup_part_id_expr_))) {
      LOG_WARN("fail to push back calc part id expr", K(ret));
    }
    LOG_DEBUG("push back calc tablet id expr: ", KP(index_info.old_part_id_expr_),
              KP(index_info.new_part_id_expr_), K(ctx.get_opertion_type()));
  }

  return ret;
}

// generate expr frame info, expr frame info represents the memory layout of the expr.
int ObTableExprCgService::generate_expr_frame_info(ObTableCtx &ctx,
                                                   common::ObIAllocator &allocator,
                                                   ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  ObStaticEngineExprCG expr_cg(allocator,
                               &ctx.get_session_info(),
                               ctx.get_schema_guard(),
                               0,
                               0,
                               ctx.get_cur_cluster_version());
  if (OB_FAIL(add_extra_column_exprs(ctx))) {
    LOG_WARN("fail to add extra column exprs", K(ret), K(ctx));
  } else if (ctx.need_dist_das() && OB_FAIL(add_all_calc_tablet_id_exprs(ctx))) {
    LOG_WARN("fail to add calc tablet id exprs", K(ret));
  } else if (ctx.is_text_retrieval_scan() &&
             OB_FAIL(ObTableFtsExprCgService::add_all_text_retrieval_scan_dep_exprs(ctx))) {
    LOG_WARN("fail to add all text retrieval scan dep exprs", K(ret));
  } else if (OB_FAIL(expr_cg.generate(ctx.get_all_exprs(), expr_frame_info))) {
    LOG_WARN("fail to generate expr frame info by expr cg", K(ret), K(ctx));
  }

  return ret;
}

// alloc expr's memory according to expr_frame_info.
int ObTableExprCgService::alloc_exprs_memory(ObTableCtx &ctx, ObExprFrameInfo &expr_frame_info)
{
  int ret = OB_SUCCESS;
  ObExecContext &exec_ctx = ctx.get_exec_ctx();
  uint64_t frame_cnt = 0;
  char **frames = NULL;
  common::ObArray<char*> param_frame_ptrs;

  if (OB_FAIL(expr_frame_info.alloc_frame(ctx.get_allocator(),
                                          param_frame_ptrs,
                                          frame_cnt,
                                          frames))) {
    LOG_WARN("fail to alloc frame", K(ret), K(expr_frame_info));
  } else {
    exec_ctx.set_frame_cnt(frame_cnt);
    exec_ctx.set_frames(frames);
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiScanSpec &spec)
{
  int ret = OB_SUCCESS;
  // init tsc_ctdef_
  if (OB_FAIL(ObTableTscCgService::generate_tsc_ctdef(ctx, alloc, spec.get_ctdef()))) {
    LOG_WARN("fail to generate table scan ctdef", K(ret), K(ctx));
  }

  return ret;
}

/*
             table_loc_id_    ref_table_id_
    主表:     主表table_id     主表table_id
    索引表:   主表table_id     索引表table_id
    回表:     主表table_id     主表table_id
*/
int ObTableLocCgService::generate_table_loc_meta(const ObTableCtx &ctx,
                                                 const ObSimpleTableSchemaV2 &simple_table_schema,
                                                 ObDASTableLocMeta &loc_meta,
                                                 ObIArray<ObTableID> *related_index_tids)
{
  int ret = OB_SUCCESS;
  int64_t route_policy = 0;
  loc_meta.reset();
  loc_meta.ref_table_id_ = simple_table_schema.get_table_id();
  loc_meta.table_loc_id_ = ctx.get_ref_table_id();
  loc_meta.is_dup_table_ = simple_table_schema.is_duplicate_table();
  if (ctx.is_weak_read()) {
    loc_meta.is_weak_read_ = 1;
    loc_meta.select_leader_ = 0;
  } else if (loc_meta.is_dup_table_) {
    loc_meta.select_leader_ = 0;
    loc_meta.is_weak_read_ = 0;
  } else {
    //strong consistency read policy is used by default
    loc_meta.select_leader_ = 1;
    loc_meta.is_weak_read_ = 0;
  }
  if (OB_FAIL(ctx.get_session_info().get_sys_variable(SYS_VAR_OB_ROUTE_POLICY, route_policy))) {
    LOG_WARN("fail to get route policy from session", K(ret));
  } else {
    loc_meta.route_policy_ = route_policy;
  }
  if (OB_FAIL(ret)) {
  } else if (simple_table_schema.is_user_table()) {
    if (OB_ISNULL(related_index_tids)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("related table ids is NULL", K(ret));
    } else {
      int64_t tids_count = related_index_tids->count();
      loc_meta.related_table_ids_.set_capacity(tids_count);
      for (int64_t i = 0; OB_SUCC(ret) && i < tids_count; i++) {
        uint64_t related_index_id = related_index_tids->at(i);
        if (OB_FAIL(loc_meta.related_table_ids_.push_back(related_index_id))) {
          LOG_WARN("fail to store related table id", K(ret), K(i));
        }
      } // end for
    }
  }

  return ret;
}

int ObTableExprCgService::refresh_update_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &new_row,
                                                     const ObTableEntity &entity)
{
  return ctx.is_inc_or_append() ? refresh_delta_exprs_frame(ctx, new_row, entity) :
                                  refresh_assign_exprs_frame(ctx, new_row, entity);
}

int ObTableExprCgService::refresh_ttl_exprs_frame(ObTableCtx &ctx,
                                                  const ObIArray<ObExpr *> &ins_new_row,
                                                  const ObTableEntity &entity)
{
  return refresh_insert_up_exprs_frame(ctx, ins_new_row, entity);
}

int ObTableExprCgService::refresh_insert_up_exprs_frame(ObTableCtx &ctx,
                                                        const ObIArray<ObExpr *> &ins_new_row,
                                                        const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObObj> &rowkey = entity.get_rowkey_objs();

  if (OB_FAIL(refresh_rowkey_exprs_frame(ctx, ins_new_row, rowkey))) {
    LOG_WARN("fail to init rowkey exprs frame", K(ret), K(ctx), K(rowkey));
  } else if (OB_FAIL(refresh_properties_exprs_frame(ctx, ins_new_row, entity))) {
    LOG_WARN("fail to init properties exprs frame", K(ret), K(ctx), K(ins_new_row), K(entity));
  }

  return ret;
}

int ObTableExprCgService::refresh_insert_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &exprs,
                                                     const ObTableEntity &entity)
{
  return refresh_exprs_frame(ctx, exprs, entity);
}

int ObTableExprCgService::refresh_replace_exprs_frame(ObTableCtx &ctx,
                                                      const ObIArray<ObExpr *> &exprs,
                                                      const ObTableEntity &entity)
{
  return refresh_exprs_frame(ctx, exprs, entity);
}

// only for htable
int ObTableExprCgService::refresh_delete_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &exprs,
                                                     const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObObj, 3> rowkey;
  if (ObTableEntityType::ET_HKV == ctx.get_entity_type()) {
    ObObj k_obj;
    ObObj q_obj;
    ObObj t_obj;
    int64_t time = 0;

    // htable场景rowkey都在properties中，所以需要从properties中提取出rowkey
    if (OB_FAIL(entity.get_property(ObHTableConstants::ROWKEY_CNAME_STR, k_obj))) {
      LOG_WARN("fail to get K", K(ret));
    } else if (OB_FAIL(entity.get_property(ObHTableConstants::CQ_CNAME_STR, q_obj))) {
      LOG_WARN("fail to get Q", K(ret));
    } else if (OB_FAIL(entity.get_property(ObHTableConstants::VERSION_CNAME_STR, t_obj))) {
      LOG_WARN("fail to get T", K(ret));
    } else if (OB_FAIL(rowkey.push_back(k_obj))) {
      LOG_WARN("fail to push back k_obj", K(ret), K(k_obj));
    } else if (OB_FAIL(rowkey.push_back(q_obj))) {
      LOG_WARN("fail to push back q_obj", K(ret), K(q_obj));
    } else if (FALSE_IT(time = t_obj.get_int())) {
      // do nothing
    } else if (FALSE_IT(t_obj.set_int(-1 * time))) {
      // do nothing
    } else if (OB_FAIL(rowkey.push_back(t_obj))) {
      LOG_WARN("fail to push back t_obj", K(ret), K(t_obj));
    }
  } else {
    if (OB_FAIL(rowkey.assign(entity.get_rowkey_objs()))) {
      LOG_WARN("fail to assign", K(ret), K(entity.get_rowkey_objs()));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresh_rowkey_exprs_frame(ctx, exprs, rowkey))) {
      LOG_WARN("fail to init rowkey exprs frame", K(ret), K(ctx), K(rowkey));
    } else if (OB_FAIL(refresh_properties_exprs_frame(ctx, exprs, entity))) {
      LOG_WARN("fail to init properties exprs frame", K(ret), K(ctx));
    }
  }

  return ret;
}

/*
  write auto increment expr datum.
  - auto increment expr tree:
      auto increment expr
              |
      column conv expr
  - specific value from user should fill to column conv expr.
*/
int ObTableExprCgService::write_autoinc_datum(ObTableCtx &ctx,
                                              const ObExpr &expr,
                                              ObEvalCtx &eval_ctx,
                                              const ObObj &obj)
{
  int ret = OB_SUCCESS;

  if (T_FUN_SYS_AUTOINC_NEXTVAL != expr.type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid expr type", K(ret), K(expr));
  } else if (expr.arg_cnt_ != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arg count for auto inc expr", K(ret), K(expr));
  } else if (expr.get_eval_info(eval_ctx).evaluated_ == true) {
    // do nothing
  } else {
    const ObExpr *conlumn_conv_expr = expr.args_[0];
    if (OB_ISNULL(conlumn_conv_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column conv expr below auto inc expr is null", K(ret));
    } else {
      ObDatum &datum = conlumn_conv_expr->locate_datum_for_write(eval_ctx);
      if (OB_FAIL(datum.from_obj(obj))) {
        LOG_WARN("fail to convert object from datum", K(ret), K(obj));
      } else {
        conlumn_conv_expr->get_eval_info(eval_ctx).evaluated_ = true;
        conlumn_conv_expr->get_eval_info(eval_ctx).projected_ = true;
      }
    }
  }

  return ret;
}

int ObTableExprCgService::write_datum(ObTableCtx &ctx,
                                      ObIAllocator &allocator,
                                      const ObTableColumnInfo &col_info,
                                      const ObExpr &expr,
                                      ObEvalCtx &eval_ctx,
                                      const ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObObj write_obj = obj;
  ObDatum &datum = expr.locate_datum_for_write(eval_ctx);
  if (expr.obj_meta_.is_fixed_len_char_type() &&
      is_pad_char_to_full_length(ctx.get_session_info().get_sql_mode()) &&
      OB_FAIL(storage::pad_column(col_info.type_.get_accuracy(), allocator, write_obj))) {
    LOG_WARN("fail to pad column", K(ret));
  } else if (OB_FAIL(datum.from_obj(write_obj))) {
    LOG_WARN("fail to convert object from datum", K(ret), K(obj));
  } else if (OB_FAIL(adjust_date_datum(expr, obj, datum))) {
    LOG_WARN("fail to adust date datum", K(ret), K(obj), K_(expr.datum_meta));
  } else if (is_lob_storage(obj.get_type()) && OB_FAIL(ob_adjust_lob_datum(datum, obj.get_meta(), expr.obj_meta_, allocator))) {
    // `ob_adjust_lob_datum()` will try to adjust datum form in_meta into out_meta
    LOG_WARN("fail to adjust lob datum", K(ret), K(datum), K(obj));
  } else {
    expr.get_eval_info(eval_ctx).evaluated_ = true;
    expr.get_eval_info(eval_ctx).projected_ = true;
  }

  return ret;
}

int ObTableExprCgService::adjust_date_datum(const ObExpr &expr, const ObObj &obj, ObDatum &datum)
{
  int ret = OB_SUCCESS;
  if (obj.is_date() && ob_is_mysql_date_tc(expr.datum_meta_.type_)) {
    ObMySQLDate mdate = 0;
    if (OB_FAIL(ObTimeConverter::date_to_mdate(obj.get_date(), mdate))) {
      LOG_WARN("fail to convert date to mysql date", K(ret), K(obj));
    } else {
      datum.set_mysql_date(mdate);
    }
  } else if (obj.is_datetime() && ob_is_mysql_datetime(expr.datum_meta_.type_)) {
    ObMySQLDateTime mdatetime = 0;
    if (OB_FAIL(ObTimeConverter::datetime_to_mdatetime(obj.get_datetime(), mdatetime))) {
      LOG_WARN("fail to convert datetime to mysql datetime", K(ret), K(obj));
    } else {
      datum.set_mysql_datetime(mdatetime);
    }
  }
  return ret;
}

int ObTableExprCgService::refresh_exprs_frame(ObTableCtx &ctx,
                                              const ObIArray<ObExpr *> &exprs,
                                              const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObObj> &rowkey = entity.get_rowkey_objs();

  if (OB_FAIL(refresh_rowkey_exprs_frame(ctx, exprs, rowkey))) {
    LOG_WARN("fail to init rowkey exprs frame", K(ret), K(ctx), K(rowkey));
  } else if (OB_FAIL(refresh_properties_exprs_frame(ctx, exprs, entity))) {
    LOG_WARN("fail to init properties exprs frame", K(ret), K(ctx));
  }

  return ret;
}

/*
  refresh rowkey frame
  1. The number of entity's rowkey may not equal the number of schema's rowkey
    when there is auto_increment column in primary keys or default current timestamp column.
  2. auto_increment expr tree is autoinc_nextval_expr - column_conv_expr,
    we need to fill value to column_conv_expr when user had set value.
  3. "IS_DEFAULT_NOW_OBJ(item.default_value_)" means default current_timestamp in column,
    we need to fill eval current timestamp when user not fill value.
*/
int ObTableExprCgService::refresh_rowkey_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &exprs,
                                                     const ObIArray<ObObj> &rowkey)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableColumnInfo *> &col_info_array = ctx.get_column_info_array();
  // schema cache
  int64_t schema_rowkey_cnt;
  ObKvSchemaCacheGuard *schema_cache_guard = ctx.get_schema_cache_guard();
  if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", K(ret));
  } else if (OB_FAIL(schema_cache_guard->get_rowkey_column_num(schema_rowkey_cnt))) {
    LOG_WARN("get rowkey column num failed", K(ret));
  } else {
    const int64_t entity_rowkey_cnt = rowkey.count();
    bool is_full_filled = (schema_rowkey_cnt == entity_rowkey_cnt); // did user fill all rowkey columns or not
    ObEvalCtx eval_ctx(ctx.get_exec_ctx());
    int64_t skip_pos = 0; // skip columns that do not need to be filled

    if (exprs.count() < schema_rowkey_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid expr count", K(ret), K(exprs), K(schema_rowkey_cnt));
    } else if (col_info_array.count() < schema_rowkey_cnt) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid column item count", K(ret), K(col_info_array), K(schema_rowkey_cnt));
    }

    // not always the primary key is the prefix of table schema
    // e.g., create table test(a varchar(1024), b int primary key);
    for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
      const ObTableColumnInfo *col_info = col_info_array.at(i);
      if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col info is NULL", K(ret), K(i));
      } else if (col_info->rowkey_position_ <= 0) {
        // normal column, do nothing
      } else {
        int64_t rowkey_position = col_info->rowkey_position_;
        const ObExpr *expr = exprs.at(i);
        if (T_FUN_SYS_AUTOINC_NEXTVAL == expr->type_) {
          if (is_full_filled && rowkey_position > entity_rowkey_cnt) {
            ret = OB_INDEX_OUT_OF_RANGE;
            LOG_WARN("idx out of range", K(ret), K(i), K(rowkey_position), K(entity_rowkey_cnt));
          } else {
            ObObj null_obj;
            null_obj.set_null();
            const ObObj *tmp_obj = nullptr;
            if (!is_full_filled) {
              tmp_obj = &null_obj;
              skip_pos++;
            } else {
              tmp_obj = &rowkey.at(rowkey_position-1);
            }
            if (OB_FAIL(write_autoinc_datum(ctx, *expr, eval_ctx, *tmp_obj))) {
              LOG_WARN("fail to write auto increment datum", K(ret), K(is_full_filled), K(*expr), K(*tmp_obj));
            }
          }
        } else if (!is_full_filled && IS_DEFAULT_NOW_OBJ(col_info->default_value_)) {
          ObDatum *tmp_datum = nullptr;
          expr->get_eval_info(eval_ctx).clear_evaluated_flag();
          if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
            LOG_WARN("fail to eval current timestamp expr", K(ret));
          } else {
            skip_pos++;
          }
        } else {
          int64_t pos = rowkey_position - 1 - skip_pos;
          if (pos >= entity_rowkey_cnt) {
            ret = OB_INDEX_OUT_OF_RANGE;
            LOG_WARN("idx out of range", K(ret), K(i), K(entity_rowkey_cnt), K(rowkey_position), K(skip_pos));
          } else if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNDEFINED;
            LOG_WARN("expr is null", K(ret));
          } else if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *col_info, *expr, eval_ctx, rowkey.at(pos)))) {
            LOG_WARN("fail to write datum", K(ret), K(rowkey_position), K(rowkey.at(pos)), K(*expr), K(pos));
          }
        }
      }
    }

  }

  return ret;
}

/*  build the refresh values:
    - refresh values array: its size and order is equal to columns in schema define
    - properties exist in entity will be add to the refresh values array
    - properties not in entity in array will be null
*/
int ObTableExprCgService::build_refresh_values(ObTableCtx &ctx,
                                               const ObTableEntity &entity,
                                               ObIArray<const ObObj*>& refresh_value_array)
{
  int ret = OB_SUCCESS;
  ObKvSchemaCacheGuard *schema_cache_guard = ctx.get_schema_cache_guard();
  if (OB_ISNULL(schema_cache_guard) || !schema_cache_guard->is_inited()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema cache guard is NULL or not inited", K(ret));
  } else {
    const ObIArray<ObString>& prop_names = entity.get_properties_names();
    const ObIArray<ObObj>& prop_values = entity.get_properties_values();
    if (prop_names.count() != prop_values.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("prop name count != prop values", K(ret));
    } else {
      for (int64_t i = 0; i < prop_names.count() && OB_SUCC(ret); i++) {
        int64_t idx = -1;
        if (OB_FAIL(schema_cache_guard->get_column_info_idx(prop_names.at(i), idx))) {
          LOG_WARN("fail to get column schema", K(ret), K(prop_names.at(i)));
          ret = OB_ERR_BAD_FIELD_ERROR;
          const ObString &table = schema_cache_guard->get_table_name_str();
          LOG_USER_ERROR(OB_ERR_BAD_FIELD_ERROR, prop_names.at(i).length(), prop_names.at(i).ptr(), table.length(), table.ptr());
        } else if (idx > refresh_value_array.count() || idx < 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("idx is not a invalid value", K(ret), K(idx), K(refresh_value_array.count()));
        } else {
          refresh_value_array.at(idx) = &prop_values.at(i);
        }
      }
    }
  }
  return ret;
}

/*
  refresh properties's exprs frame.
  - generate column expr eval.
  - auto increment expr fill user value or null obj to child expr(column conv expr).
  - current timestamp expr eval if user not fill value.
  - column ref expr fill user value or default value.
*/
int ObTableExprCgService::refresh_properties_exprs_frame(ObTableCtx &ctx,
                                                         const ObIArray<ObExpr *> &exprs,
                                                         const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableColumnInfo *> &col_info_array = ctx.get_column_info_array();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());
  ObSEArray<const ObObj*, 16> refresh_values;

  if (OB_ISNULL(ctx.get_exec_ctx().get_expr_op_ctx_store()) && OB_NOT_NULL(ctx.get_expr_frame_info()) && OB_FAIL(ctx.get_exec_ctx().init_expr_op(ctx.get_expr_frame_info()->need_ctx_cnt_))) {
    LOG_WARN("fail to initialize expression operation", K(ret), K(ctx));
  } else if (col_info_array.count() < exprs.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid column info count", K(ret), K(col_info_array.count()), K(exprs.count()));
  } else if (OB_FAIL(refresh_values.prepare_allocate(col_info_array.count()))) {
    LOG_WARN("fail to init columns refresh array", K(ret));
  } else if (OB_FAIL(build_refresh_values(ctx, entity, refresh_values))) {
    LOG_WARN("fail to build refresh values array", K(ret));
  } else {
    const ObObj *obj = nullptr;
    // not always the primary key is the prefix of table schema
    // e.g., create table test(a varchar(1024), b int primary key);
    for (int64_t i = 0; OB_SUCC(ret) && i < col_info_array.count(); i++) {
      const ObTableColumnInfo *col_info = col_info_array.at(i);
      if (OB_ISNULL(col_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("col info is NULL", K(ret), K(i));
      } else if (col_info->rowkey_position_ > 0) {
        // rowkey column, do nothing
      } else {
        const ObExpr *expr = exprs.at(i);
        if ((col_info->is_generated_column() && !col_info->is_doc_id_column()) ||
            (col_info->is_doc_id_column() && refresh_values.at(i) == nullptr)) {
          // generate column need eval first
          // In ttl_delete, we need to assign doc_id to doc_id column expr and its expr is column_ref_expr
          ObDatum *tmp_datum = nullptr;
          if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
            LOG_WARN("fail to eval generate expr", K(ret));
          }
        } else {
          bool not_found = refresh_values.at(i) == nullptr;
          if (not_found) {
            obj = &col_info->default_value_;
            if (!col_info->is_nullable_ && !col_info->is_auto_increment_ && obj->is_null()) {
              ret = OB_ERR_NO_DEFAULT_FOR_FIELD;
              ObCStringHelper helper;
              LOG_USER_ERROR(OB_ERR_NO_DEFAULT_FOR_FIELD, helper.convert(col_info->column_name_));
              LOG_WARN("column can not be null", K(ret), KPC(col_info));
            }
          } else {
            obj = refresh_values.at(i);
          }
          if (OB_FAIL(ret)) {
          } else if (T_FUN_SYS_AUTOINC_NEXTVAL == expr->type_) {
            ObObj null_obj;
            null_obj.set_null();
            obj = not_found ? &null_obj : obj;
            if (OB_FAIL(write_autoinc_datum(ctx, *expr, eval_ctx, *obj))) {
              LOG_WARN("fail to write auto increment datum", K(ret), K(not_found), K(*expr), K(*obj));
            }
          } else if (not_found && IS_DEFAULT_NOW_OBJ(col_info->default_value_)) {
            ObDatum *tmp_datum = nullptr;
            expr->get_eval_info(eval_ctx).clear_evaluated_flag();
            if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
              LOG_WARN("fail to eval current timestamp expr", K(ret));
            }
          } else {
            if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *col_info, *expr, eval_ctx, *obj))) {
              LOG_WARN("fail to write datum", K(ret), K(*obj), K(*expr));
            }
          }
        }
      }
    }
  }
  return ret;
}


// refresh delta expr's frame with user specific value.
int ObTableExprCgService::refresh_delta_exprs_frame(ObTableCtx &ctx,
                                                    const ObIArray<ObExpr *> &delta_row,
                                                    const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());

  if (!ctx.is_inc_or_append()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid operation type", K(ret), K(ctx));
  } else if (delta_row.count() > assigns.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid delta row length", K(ret), K(delta_row), K(assigns));
  } else {
    for (int64_t i = 0, idx = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      const ObTableAssignment &assign = assigns.at(i);
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is null", K(ret), K(assign));
      } else if (assign.column_info_->auto_filled_timestamp_ || assign.column_info_->is_generated_column()) {
        // do nothing
      } else if (idx >= delta_row.count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("index out of range", K(ret), K(assign), K(delta_row));
      } else {
        const ObExpr *expr = delta_row.at(idx);
        if (OB_ISNULL(expr)) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("expr is null", K(ret));
        } else if (!assign.is_assigned_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("not found delta value", K(ret), K(assign));
        } else if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *assign.column_info_, *expr, eval_ctx, assign.assign_value_))) {
          LOG_WARN("fail to write datum", K(ret), K(assign.assign_value_), K(*expr));
        } else {
          idx++;
        }
      }
    }
  }

  return ret;
}

/*
  refresh assign expr's frame when do update.
  - assign virtual generated column is not support.
  - eval current timestamp expr if user not assign specific value.
  - fill other expr's datum with user value.
*/
int ObTableExprCgService::refresh_assign_exprs_frame(ObTableCtx &ctx,
                                                     const ObIArray<ObExpr *> &new_row,
                                                     const ObTableEntity &entity)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
  ObEvalCtx eval_ctx(ctx.get_exec_ctx());

  for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
    const ObTableAssignment &assign = assigns.at(i);
    if (OB_ISNULL(assign.column_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("assign column item is null", K(ret), K(assign));
    } else if (new_row.count() < assign.column_info_->col_idx_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected assign projector_index_", K(ret), K(new_row), K(assign.column_item_));
    } else {
      // on update current timestamp will not find value
      const ObExpr *expr = new_row.at(assign.column_info_->col_idx_);
      if (OB_ISNULL(expr)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("expr is null", K(ret));
      } else if (!assign.is_assigned_) {
        if (!assign.column_info_->auto_filled_timestamp_ && !assign.column_info_->is_generated_column()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get assign propertity value", K(ret), K(assign));
        } else if (assign.column_info_->is_generated_column()) {
          // do nothing, generated column not need to fill
        } else { // on update current timestamp
          ObDatum *tmp_datum = nullptr;
          if (OB_FAIL(expr->eval(eval_ctx, tmp_datum))) {
            LOG_WARN("fail to eval current timestamp expr", K(ret));
          }
        }
      } else { // found
        if (OB_FAIL(write_datum(ctx, ctx.get_allocator(), *assign.column_info_, *expr, eval_ctx, assign.assign_value_))) {
          LOG_WARN("fail to write datum", K(ret), K(assign.assign_value_), K(*expr));
        }
      }
    }
  }

  return ret;
}

/*
  create table t(c1 int primary key,
                 c2 varchar(10),
                 c3 varchar(10),
                 c4 varchar(30) generated always as (concat(c2, c3)));
  generated expr: c4
  dependant_expr: concat(`c2`, `c3`)
  DAS need dependant_expr to calculate result, so we use dependant_expr.

  @param: use_column_ref_exprs is used to replace the column_ref exprs for old rows
*/
int ObTableDmlCgService::replace_exprs(ObTableCtx &ctx,
                                       ObTableIndexInfo &index_info,
                                       bool use_column_ref_exprs,
                                       ObIArray<ObRawExpr *> &dst_exprs)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  const ObTableSchema *index_schema = nullptr;
  ObSEArray<uint64_t, 8> column_ids;
  if (OB_ISNULL(index_schema = index_info.index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is NULL", K(ret));
  } else if (OB_FAIL(index_schema->get_column_ids(column_ids))) {
    LOG_WARN("fail to get column ids", K(ret));
  }

  for (int64_t i = 0; i < column_ids.count() && OB_SUCC(ret); i++) {
    const ObTableColumnItem *column_item = nullptr;
    const ObTableColumnInfo *col_info = nullptr;
    ObRawExpr *tmp_expr = nullptr;
    bool is_skip_add = false;
    if (OB_FAIL(ctx.get_column_item_by_column_id(column_ids.at(i), column_item))) {
      LOG_WARN("fail to get column item", K(ret), K(column_ids.at(i)), K(i));
    } else if (OB_ISNULL(column_item)) {
      if (is_shadow_column(column_ids.at(i))) {
        // unique global index has shadow_pk columns which is a projector of primary key columns
        is_skip_add = true;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column_item is NULL", K(ret), K(column_ids.at(i)), K(i));
      }
    } else if (OB_ISNULL(column_item->raw_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_item raw_expr is NULL", K(ret), K(column_item), K(column_ids.at(i)), K(i));
    } else if (OB_ISNULL(col_info = column_item->column_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(ret), K(column_ids.at(i)));
    } else if (col_info->is_generated_column()) {
      if (use_column_ref_exprs && col_info->is_doc_id_column()) { // doc_id column is acquired from storege
        tmp_expr = column_item->expr_;
      } else {
        ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(column_item->raw_expr_);
        tmp_expr = col_ref_expr->get_dependant_expr();
      }
    } else if (use_column_ref_exprs) {
      // old rows need to use column ref expr to store the storage old values.
      // if use calculate exprs here, it may calculate repeatedly and cause 4377 problem
      tmp_expr = column_item->expr_;
    } else {
      tmp_expr = column_item->raw_expr_;
    }
    if (OB_SUCC(ret) && !is_skip_add && OB_FAIL(dst_exprs.push_back(tmp_expr))) {
      LOG_WARN("fail to push back expr", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_calc_tablet_id_rt_expr(ObTableCtx &ctx, const ObRawExpr &raw_expr, ObExpr *&expr)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  if (OB_FAIL(cg.generate_rt_expr(raw_expr, expr))) {
    LOG_WARN("generate rt expr failed", K(ret));
  } else if (OB_FAIL(ObExprCalcPartitionBase::set_may_add_interval_part(expr,
                                                      MayAddIntervalPart::YES))) {
    LOG_WARN("fail to set partition info", K(ret));
  }
  return ret;
}

/*
  add column infos for check nullable before insert new_row to das.
*/
int ObTableDmlCgService::add_all_column_infos(ObTableCtx &ctx,
                                              ObTableIndexInfo &index_info,
                                              ObIAllocator &allocator,
                                              ColContentFixedArray &column_infos)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 64> column_ids;
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  const ObTableSchema *table_schema = index_info.index_schema_;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(column_infos.init(column_ids.count()))) {
    LOG_WARN("fail to init column infos capacity", K(ret), K(column_ids.count()));
  }

  for (int64_t i = 0; i < column_ids.count() && OB_SUCC(ret); i++) {
    const ObTableColumnItem *column_item = nullptr;
    ObColumnRefRawExpr *column_expr = nullptr;
    if (is_shadow_column(column_ids.at(i))) {
      // skip and do nothing, for the reason:
      // unique global index has shadow_pk columns which is a projector of primary key columns
    } else if (OB_FAIL(ctx.get_column_item_by_column_id(column_ids.at(i), column_item))) {
      LOG_WARN("fail to get column item", K(ret), K(column_ids.at(i)), K(i));
    } else if (OB_ISNULL(column_item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column_item is NULL", K(ret), K(i), K(column_ids.at(i)));
    } else if (OB_ISNULL(column_expr = column_item->expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column expr is NULL", K(ret), K(i), K(column_ids.at(i)));
    } else {
      ColumnContent column_content;
      int64_t idx = 0;
      column_content.auto_filled_timestamp_ = column_expr->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG);
      column_content.is_nullable_ = !column_expr->get_result_type().is_not_null_for_write();
      column_content.is_predicate_column_ = false;
      column_content.is_implicit_ = false;
      if (OB_FAIL(ob_write_string(allocator, column_expr->get_column_name(), column_content.column_name_))) {
        LOG_WARN("fail to copy column name", K(ret), K(column_expr->get_column_name()));
      } else if (FALSE_IT(column_content.projector_index_ = static_cast<uint64_t>(i))) {
        //do nothing
      } else if (OB_FAIL(column_infos.push_back(column_content))) {
        LOG_WARN("fail to store colum content to column infos", K(ret), K(column_content));
      }
    }
  }

  return ret;
}

/*
  genreate insert ctdef
  - replace exprs with depenedant expr if there are generated column.
  - construct new row.
  - old row is empty in insert ctdef
  - generate base ctdef which include column_ids, old_row and new_row.
  - generate das insert ctdef which include projector, table_id and so on.
  - generate related(index) insert ctdef.
*/
int ObTableDmlCgService::generate_insert_ctdef(ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObTableIndexInfo &index_info,
                                               ObTableInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  ObSEArray<ObRawExpr*, 64> tmp_exprs;

  if (OB_FAIL(replace_exprs(ctx, index_info, false, tmp_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret), K(ctx));
  } else if (OB_FAIL(new_row.assign(tmp_exprs))) {
    LOG_WARN("fail to assign new row", K(ret));
  } else if (OB_FAIL(generate_base_ctdef(ctx, index_info, ins_ctdef, old_row, new_row))) {
    LOG_WARN("fail to generate dml base ctdef", K(ret));
  } else if (OB_FAIL(add_all_column_infos(ctx, index_info, allocator, ins_ctdef.column_infos_))) {
    LOG_WARN("fail to add all column infos", K(ret));
  } else if (OB_FAIL(generate_das_ins_ctdef(ctx,
                                            index_info,
                                            index_info.index_table_id_,
                                            ins_ctdef.das_ctdef_,
                                            new_row))) {
    LOG_WARN("fail to generate das insert ctdef", K(ret));
  } else if (OB_FAIL(generate_related_ins_ctdef(ctx,
                                                allocator,
                                                index_info,
                                                new_row,
                                                ins_ctdef.related_ctdefs_))) {
    LOG_WARN("fail to generate related ins ctdef", K(ret));
  } else if (OB_NOT_NULL(index_info.old_part_id_expr_) &&
      OB_FAIL(generate_calc_tablet_id_rt_expr(ctx, *index_info.old_part_id_expr_, ins_ctdef.new_part_id_expr_))) {
    LOG_WARN("fail to generate calc tablet id expr", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_assign_row(ObTableCtx &ctx,
                                             ObTableIndexInfo &index_info,
                                             ObIArray<ObRawExpr*> &new_row,
                                             ObIArray<ObRawExpr*> &full_row,
                                             ObIArray<ObRawExpr*> &delta_row)
{
  int ret = OB_SUCCESS;
  const ObTableSchema *index_schema = nullptr;
  int64_t idx = 0;
  ObIArray<ObTableAssignment> &assignments = ctx.get_assignments();
  if (OB_ISNULL(index_schema = index_info.index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is NULL", K(ret));
  }
  for (ObTableSchema::const_column_iterator iter = index_schema->column_begin();
        OB_SUCC(ret) && iter != index_schema->column_end(); ++iter, idx++) {
    const ObColumnSchemaV2 *column_schema = *iter;
    uint64_t column_id = OB_INVALID_ID;
    const ObTableAssignment *assignment = nullptr;
    ObRawExpr *tmp_expr = nullptr;
    if (OB_ISNULL(column_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column schema is NULL", K(ret));
    } else if (FALSE_IT(column_id = column_schema->get_column_id())) {
    } else if (OB_FAIL(ctx.get_assignment_by_column_id(column_id, assignment))) {
      LOG_WARN("fail to get expr by column name", K(ret), K(column_id));
    } else if (OB_NOT_NULL(assignment)) {
      // assignment is NULL meaning that the column is not assign column
      if (assignment->column_expr_->is_generated_column()) {
        tmp_expr = assignment->column_expr_->get_dependant_expr();
      } else {
        tmp_expr = assignment->expr_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(full_row.push_back(tmp_expr))) {
        LOG_WARN("fail to push back expr to full row", K(ret));
      } else if (index_info.is_primary_index_ &&
                assignment->is_inc_or_append_ &&
                OB_FAIL(delta_row.push_back(assignment->delta_expr_))) {
        LOG_WARN("fail to add delta expr to delta row", K(ret), K(*assignment));
      } else {
        new_row.at(idx) = tmp_expr;
      }
    }
  } // end for
  return ret;
}

/*
  create table t(c1 int primary key, c2 int default null, c3 int default null);
  insert into t values(1,1,1);
  update t set c3=2 where c1=1;

  assign expr: c3'
  old row: c1, c2, c3
  new row: c1, c2, c3'
  full row: c1, c2, c3, c3'
*/
int ObTableDmlCgService::generate_update_ctdef(ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObTableIndexInfo &index_info,
                                               ObTableUpdCtDef &upd_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  ObSEArray<ObRawExpr*, 64> full_row;
  ObSEArray<ObRawExpr*, 64> delta_row;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  ObSEArray<ObRawExpr*, 64> tmp_old_exprs;
  ObSEArray<ObRawExpr*, 64> tmp_full_assign_exprs;
  if (OB_FAIL(replace_exprs(ctx, index_info, true, tmp_old_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(old_row.assign(tmp_old_exprs))) {
    LOG_WARN("fail to assign old row expr", K(ret));
  } else if (OB_FAIL(new_row.assign(old_row))) {
    LOG_WARN("fail to assign new row", K(ret));
  } else if (OB_FAIL(append(full_row, old_row))) {
    LOG_WARN("fail to append old row expr to full row", K(ret), K(old_row));
  } else if (OB_FAIL(generate_assign_row(ctx, index_info, new_row, full_row, delta_row))){
    LOG_WARN("fail to generate assign row", K(ret));
  } else if (OB_FAIL(generate_base_ctdef(ctx, index_info, upd_ctdef, old_row, new_row))) {
    LOG_WARN("fail to generate dml base ctdef", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(full_row, upd_ctdef.full_row_))) {
    LOG_WARN("fail to generate dml update full row exprs", K(ret), K(full_row));
  } else if (!delta_row.empty() && OB_FAIL(cg.generate_rt_exprs(delta_row, upd_ctdef.delta_row_))) {
    LOG_WARN("fail to generate dml update delta row exprs", K(ret), K(delta_row));
  } else if (OB_FAIL(generate_das_upd_ctdef(ctx,
                                            index_info,
                                            index_info.index_table_id_,
                                            upd_ctdef.das_ctdef_,
                                            old_row,
                                            new_row,
                                            full_row))) {
    LOG_WARN("fail to generate das upd ctdef", K(ret));
  } else if (OB_FAIL(generate_related_upd_ctdef(ctx,
                                                allocator,
                                                index_info,
                                                old_row,
                                                new_row,
                                                full_row,
                                                upd_ctdef.related_ctdefs_))) {
    LOG_WARN("fail to generate related upd ctdef", K(ret));
  } else if (OB_FAIL(generate_upd_assign_infos(ctx, index_info, allocator, upd_ctdef))) {
    LOG_WARN("fail to generate related upd assign info", K(ret));
  }

  if (OB_SUCC(ret) &&
      (ctx.is_for_insertup() ||
      (ctx.is_for_update() && ctx.has_global_index()))) {
    ObDMLCtDefAllocator<ObDASDelCtDef> ddel_allocator(allocator);
    ObDMLCtDefAllocator<ObDASInsCtDef> dins_allocator(allocator);
    if (OB_ISNULL(upd_ctdef.ddel_ctdef_ = ddel_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate das del ctdef", K(ret));
    } else if (OB_ISNULL(upd_ctdef.dins_ctdef_ = dins_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate das ins ctdef", K(ret));
    } else if (OB_FAIL(generate_das_del_ctdef(ctx,
                                              index_info,
                                              index_info.index_table_id_,
                                              *upd_ctdef.ddel_ctdef_,
                                              old_row))) {
      LOG_WARN("fail to generate das delete ctdef for update", K(ret));
    } else if (OB_FAIL(generate_related_del_ctdef(ctx,
                                                  allocator,
                                                  index_info,
                                                  old_row,
                                                  upd_ctdef.related_del_ctdefs_))) {
      LOG_WARN("fail to generate related del ctdef", K(ret));
    } else if (OB_FAIL(generate_das_ins_ctdef(ctx,
                                              index_info,
                                              index_info.index_table_id_,
                                              *upd_ctdef.dins_ctdef_,
                                              new_row))) {
      LOG_WARN("fail to generate das insert ctdef for update", K(ret));
    } else if (OB_FAIL(generate_related_ins_ctdef(ctx,
                                                  allocator,
                                                  index_info,
                                                  new_row,
                                                  upd_ctdef.related_ins_ctdefs_))) {
      LOG_WARN("fail to generate related ins ctdef", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_NOT_NULL(index_info.old_part_id_expr_) &&
        OB_FAIL(generate_calc_tablet_id_rt_expr(ctx, *index_info.old_part_id_expr_, upd_ctdef.old_part_id_expr_))) {
      LOG_WARN("fail to generate old calc tablet id", K(ret));
    } else if (OB_NOT_NULL(index_info.new_part_id_expr_) &&
        OB_FAIL(generate_calc_tablet_id_rt_expr(ctx, *index_info.new_part_id_expr_, upd_ctdef.new_part_id_expr_))) {
      LOG_WARN("fail to generate new calc tablet id", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_das_upd_ctdef(ObTableCtx &ctx,
                                                ObTableIndexInfo &index_info,
                                                uint64_t index_tid,
                                                ObDASUpdCtDef &das_upd_ctdef,
                                                const ObIArray<ObRawExpr*> &old_row,
                                                const ObIArray<ObRawExpr*> &new_row,
                                                const ObIArray<ObRawExpr*> &full_row)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 64> dml_column_ids;

  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_upd_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_updated_column_ids(ctx, das_upd_ctdef.column_ids_, das_upd_ctdef.updated_column_ids_))) {
    LOG_WARN("fail to add updated column ids", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx, index_info, dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids, // new row and old row's columns id
                                        das_upd_ctdef.column_ids_, // schmea column ids for given index_tid
                                        old_row,
                                        new_row,
                                        full_row,
                                        das_upd_ctdef))) {
    LOG_WARN("fail to generate projector", K(ret), K(full_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_updated_column_ids(ObTableCtx &ctx,
                                                     const ObIArray<uint64_t> &column_ids,
                                                     ObIArray<uint64_t> &updated_column_ids)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
  updated_column_ids.reset();
  if (column_ids.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_ids is empty", K(ret));
  } else if (OB_FAIL(updated_column_ids.reserve(column_ids.count()))) {
    LOG_WARN("fail to reserver buffer to update column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < assigns.count(); i++) {
      const ObTableAssignment &assign = assigns.at(i);
      int64_t idx = -1;
      if (OB_ISNULL(assign.column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("assign column item is null", K(ret), K(assign));
      } else if (has_exist_in_array(column_ids, assign.column_info_->column_id_, &idx)) {
        if (OB_FAIL(updated_column_ids.push_back(assign.column_info_->column_id_))) {
          LOG_WARN("fail to add updated column id", K(ret), K(assign));
        }
      }
    } // end for
  }

  return ret;
}

int ObTableDmlCgService::generate_upd_assign_infos(ObTableCtx &ctx,
                                                   ObTableIndexInfo &index_info,
                                                   ObIAllocator &allocator,
                                                   ObTableUpdCtDef &udp_ctdef)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableAssignment> &assigns = ctx.get_assignments();
  int64_t assign_cnt = assigns.count();
  ColContentFixedArray &assign_infos = udp_ctdef.assign_columns_;
  ObSEArray<uint64_t, 64> column_ids;
  const ObTableSchema *table_schema = index_info.index_schema_;

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(table_schema->get_column_ids(column_ids))) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(assign_infos.init(assign_cnt))) {
    LOG_WARN("fail to init assign info array", K(ret), K(assign_cnt));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
    const ObTableAssignment *assignment = nullptr;
    if (OB_FAIL(ctx.get_assignment_by_column_id(column_ids.at(i), assignment))) {
      LOG_WARN("fail to get assignment", K(ret), K(column_ids.at(i)), K(i));
    } else if (OB_NOT_NULL(assignment)) {
      ObColumnRefRawExpr *column_expr = nullptr;
      if (OB_ISNULL(column_expr = assignment->column_expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column expr is NULL", K(ret), K(i));
      } else {
        ColumnContent column_content;
        column_content.auto_filled_timestamp_ = column_expr->get_result_type().has_result_flag(ON_UPDATE_NOW_FLAG);
        column_content.is_nullable_ = !column_expr->get_result_type().is_not_null_for_write();
        column_content.is_predicate_column_ = false;
        column_content.is_implicit_ = false;
        if (OB_FAIL(ob_write_string(allocator,
                                    column_expr->get_column_name(),
                                    column_content.column_name_))) {
          LOG_WARN("fail to copy column name", K(ret), K(column_expr->get_column_name()));
        } else if (FALSE_IT(column_content.projector_index_ = static_cast<uint64_t>(i))) {
          //do nothing
        } else if (OB_FAIL(assign_infos.push_back(column_content))) {
          LOG_WARN("fail to store colum content to assign infos", K(ret), K(column_content));
        }
      }
    }
  }
  return ret;
}

int ObTableDmlCgService::generate_delete_ctdef(ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObTableIndexInfo &index_info,
                                               ObTableDelCtDef &del_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> old_row;
  ObSEArray<ObRawExpr*, 64> new_row;
  ObSEArray<ObRawExpr*, 64> table_column_exprs;
  if (OB_FAIL(replace_exprs(ctx, index_info, true, table_column_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(old_row.assign(table_column_exprs))) {
    LOG_WARN("fail to assign old row expr", K(ret));
  } else if (OB_FAIL(generate_base_ctdef(ctx, index_info, del_ctdef, old_row, new_row))) {
    LOG_WARN("fail to generate dml base ctdef", K(ret));
  } else if (OB_FAIL(generate_das_del_ctdef(ctx,
                                            index_info,
                                            index_info.index_table_id_,
                                            del_ctdef.das_ctdef_,
                                            old_row))) {
    LOG_WARN("fail to generate das delete ctdef", K(ret));
  } else if (OB_FAIL(generate_related_del_ctdef(ctx,
                                                allocator,
                                                index_info,
                                                old_row,
                                                del_ctdef.related_ctdefs_))) {
    LOG_WARN("fail to generate related del ctdef", K(ret));
  } else if (OB_NOT_NULL(index_info.old_part_id_expr_) &&
             OB_FAIL(generate_calc_tablet_id_rt_expr(ctx, *index_info.old_part_id_expr_, del_ctdef.old_part_id_expr_))) {
    LOG_WARN("fail to generate calc tablet id expr", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_del_ctdef(ObTableCtx &ctx,
                                                ObTableIndexInfo &index_info,
                                                uint64_t index_tid,
                                                ObDASDelCtDef &das_del_ctdef,
                                                const ObIArray<ObRawExpr*> &old_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_new_row;
  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_del_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx, index_info, dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids,
                                        das_del_ctdef.column_ids_,
                                        old_row,
                                        empty_new_row,
                                        old_row,
                                        das_del_ctdef))) {
    LOG_WARN("fail to add old row projector", K(ret), K(old_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_related_del_ctdef(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    ObTableIndexInfo &index_info,
                                                    const ObIArray<ObRawExpr*> &old_row,
                                                    DASDelCtDefArray &del_ctdefs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableID, 4> related_index_tids;
  related_index_tids.assign(index_info.related_index_ids_);
  del_ctdefs.set_capacity(related_index_tids.count());

  for (int64_t i = 0; OB_SUCC(ret) && i < related_index_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASDelCtDef> das_alloc(allocator);
    ObDASDelCtDef *related_das_ctdef = nullptr;
    if (OB_ISNULL(related_das_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate delete related das ctdef", K(ret));
    } else if (OB_FAIL(generate_das_del_ctdef(ctx,
                                              index_info,
                                              related_index_tids.at(i),
                                              *related_das_ctdef,
                                              old_row))) {
      LOG_WARN("fail to generate das del ctdef", K(ret));
    } else if (OB_FAIL(del_ctdefs.push_back(related_das_ctdef))) {
      LOG_WARN("fail to store related ctdef", K(ret));
    }
  }

  return ret;
}

/*
  generate replace ctdef which consists of insert ctdef and delete ctdef.
  - generate insert ctdef.
  - generate rowkey info which use for fetch duplicated rowkey.
  - generate delete ctdef.
*/
int ObTableDmlCgService::generate_replace_ctdef(ObTableCtx &ctx,
                                                ObIAllocator &allocator,
                                                ObTableIndexInfo &index_info,
                                                ObTableReplaceCtDef &replace_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(generate_insert_ctdef(ctx, allocator, index_info, replace_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), K(ctx));
  } else if (OB_FAIL(generate_table_rowkey_info(ctx, replace_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate table rowkey info", K(ret), K(ctx));
  } else if (OB_FAIL(generate_delete_ctdef(ctx, allocator, index_info, replace_ctdef.del_ctdef_))) {
    LOG_WARN("fail to generate delete ctdef", K(ret), K(ctx));
  }

  return ret;
}

int ObTableDmlCgService::generate_table_rowkey_info(ObTableCtx &ctx,
                                                    ObTableInsCtDef &ins_ctdef)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  ObDASInsCtDef &das_ins_ctdef = ins_ctdef.das_ctdef_;
  ObSEArray<uint64_t, 8> rowkey_column_ids;
  ObSEArray<ObRawExpr *, 8> rowkey_exprs;
  ObSEArray<ObObjMeta, 8> rowkey_column_types;


  if (OB_FAIL(get_rowkey_exprs(ctx, rowkey_exprs))) {
    LOG_WARN("fail to get table rowkey exprs", K(ret), K(ctx));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); i++) {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(ctx.get_column_item_by_expr(rowkey_exprs.at(i), item))) {
        LOG_WARN("fail to get column item", K(ret), K(rowkey_exprs), K(i));
      } else if (OB_ISNULL(item) || OB_ISNULL(item->column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret), KP(item));
      } else if (OB_FAIL(rowkey_column_ids.push_back(item->column_info_->column_id_))) {
        LOG_WARN("fail to push base column id", K(ret), KPC(item));
      } else if (OB_ISNULL(item->expr_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column ref expr is null", K(ret), KPC(item));
      } else if (OB_FAIL(rowkey_column_types.push_back(item->expr_->get_result_type()))) {
        LOG_WARN("fail to push column type", K(ret), KPC(item));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(das_ins_ctdef.table_rowkey_cids_.init(rowkey_column_ids.count()))) {
    LOG_WARN("fail to init table rowkey column ids", K(ret), K(rowkey_column_ids.count()));
  } else if (OB_FAIL(append(das_ins_ctdef.table_rowkey_cids_, rowkey_column_ids))) {
    LOG_WARN("fail to append table rowkey column id", K(ret), K(rowkey_column_ids));
  } else if (OB_FAIL(das_ins_ctdef.table_rowkey_types_.init(rowkey_column_types.count()))) {
    LOG_WARN("fail to init table_rowkey_types", K(ret), K(rowkey_column_types.count()));
  } else if (OB_FAIL(append(das_ins_ctdef.table_rowkey_types_, rowkey_column_types))) {
    LOG_WARN("fail to append table rowkey column type", K(ret), K(rowkey_column_types));
  }

  return ret;
}

int ObTableDmlCgService::get_rowkey_exprs(ObTableCtx &ctx, ObIArray<ObRawExpr*> &rowkey_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<uint64_t, 64> rowkey_column_ids;
  const ObTableSchema *table_schema = ctx.get_table_schema();

  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (OB_FAIL(table_schema->get_rowkey_column_ids(rowkey_column_ids))) {
    LOG_WARN("fail to get rowkey column ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_column_ids.count(); i++) {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(ctx.get_column_item_by_column_id(rowkey_column_ids.at(i), item))) {
        LOG_WARN("fail to get column item", K(ret), K(rowkey_column_ids), K(i));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item is null", K(ret));
      } else if (OB_FAIL(rowkey_exprs.push_back(item->raw_expr_))) {
        LOG_WARN("fail to push back rowkey expr", K(ret), K(i));
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_tsc_ctdef(ObTableCtx &ctx,
                                            ObIArray<ObRawExpr *> &access_exprs,
                                            ObDASScanCtDef &tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  tsc_ctdef.ref_table_id_ = ctx.get_index_table_id();
  const uint64_t tenant_id = ctx.get_tenant_id();
  ObSEArray<uint64_t, 64> column_ids;
  ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();
  ObSEArray<ObRawExpr*, 16> calc_raw_exprs;
  bool need_doc_id = ctx.has_fts_index();
  const ObTableSchema *table_schema = ctx.get_table_schema();
  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(schema_guard->get_schema_version(TABLE_SCHEMA,
                                                      tenant_id,
                                                      tsc_ctdef.ref_table_id_,
                                                      tsc_ctdef.schema_version_))) {
    LOG_WARN("fail to get schema version", K(ret), K(tenant_id), K(tsc_ctdef.ref_table_id_));
  } else if (OB_FAIL(cg.generate_rt_exprs(access_exprs, tsc_ctdef.pd_expr_spec_.access_exprs_))) {
    LOG_WARN("fail to generate rt exprs ", K(ret), K(access_exprs));
  } else if (OB_FAIL(tsc_ctdef.access_column_ids_.init(access_exprs.count()))) {
    LOG_WARN("fail to init access_column_ids_ ", K(ret));
  } else if (OB_FAIL(ctx.get_table_schema()->get_column_ids(column_ids))) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(tsc_ctdef.access_column_ids_.assign(column_ids))) {
    LOG_WARN("fail to assign column ids", K(ret), K(column_ids));
  } else if (need_doc_id) {
    uint64_t rowkey_doc_col_id = OB_INVALID_ID;
    uint64_t ft_col_id = OB_INVALID_ID;
    if (OB_FAIL(table_schema->get_fulltext_column_ids(rowkey_doc_col_id, ft_col_id))) {
      LOG_WARN("fail to get fulltext column ids", K(ret));
    } else {
      tsc_ctdef.doc_id_idx_ = table_schema->get_column_idx(rowkey_doc_col_id);
    }
  }

  if (OB_SUCC(ret)) {
    tsc_ctdef.table_param_.get_enable_lob_locator_v2() = (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
    if (OB_FAIL(tsc_ctdef.table_param_.convert(*table_schema, tsc_ctdef.access_column_ids_,
                                               tsc_ctdef.pd_expr_spec_.pd_storage_flag_))) {
      LOG_WARN("fail to convert table param", K(ret));
    } else if (OB_FAIL(generate_calc_raw_exprs(access_exprs, calc_raw_exprs))) {
      LOG_WARN("fail to genreate calc raw expr", K(ret));
    } else if (OB_FAIL(cg.generate_rt_exprs(calc_raw_exprs, tsc_ctdef.pd_expr_spec_.calc_exprs_))) {
      LOG_WARN("fali to generate calc rt exprs", K(ret));
    } else if (OB_FAIL(ObTableTscCgService::generate_das_result_output(tsc_ctdef, tsc_ctdef.access_column_ids_))) {
      LOG_WARN("generate das result output failed", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_single_constraint_info(ObTableCtx &ctx,
                                                         const ObTableSchema &index_schema,
                                                         const uint64_t table_id,
                                                         ObUniqueConstraintInfo &constraint_info)
{
  int ret = OB_SUCCESS;
  constraint_info.table_id_ = table_id;
  constraint_info.index_tid_ = index_schema.get_table_id();
  if (!index_schema.is_index_table()) {
    constraint_info.constraint_name_ = "PRIMARY";
  } else if (OB_FAIL(index_schema.get_index_name(constraint_info.constraint_name_))) {
    LOG_WARN("fail to get index name", K(ret));
  }

  if (OB_SUCC(ret)) {
    uint64_t rowkey_column_id = OB_INVALID_ID;
    ObIArray<ObColumnRefRawExpr*> &column_exprs = constraint_info.constraint_columns_;
    const ObRowkeyInfo &rowkey_info = index_schema.get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(rowkey_info.get_column_id(i, rowkey_column_id))) {
        LOG_WARN("fail to get rowkey column id", K(ret));
      } else if (OB_FAIL(ctx.get_column_item_by_column_id(rowkey_column_id, item))) {
        LOG_WARN("fail to get column item", K(ret), K(ctx), K(rowkey_column_id));
      } else if (OB_ISNULL(item)) {
        // do nothing, not found
      } else if (OB_FAIL(column_exprs.push_back(item->expr_))) {
        LOG_WARN("fail to push back column expr", K(ret), K(item));
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_constraint_infos(ObTableCtx &ctx,
                                                   ObIArray<ObUniqueConstraintInfo> &cst_infos)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();
  const ObTableSchema *table_schema = ctx.get_table_schema();
  const uint64_t ref_table_id = ctx.get_ref_table_id();
  ObUniqueConstraintInfo constraint_info;
  ObSEArray<ObAuxTableMetaInfo, 16> index_infos;

  // 1. primary key
  if (OB_ISNULL(table_schema) || OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret), KP(table_schema), KP(schema_guard));
  } else if (OB_FAIL(generate_single_constraint_info(ctx,
                                                     *table_schema,
                                                     ref_table_id,
                                                     constraint_info))) {
    LOG_WARN("fail to generate primary key constraint info", K(ret), K(ref_table_id));
  } else if (OB_FAIL(cst_infos.push_back(constraint_info))) {
    LOG_WARN("fail to push back constraint info", K(ret));
  }

  // 2. unique key
  if (FAILEDx(table_schema->get_simple_index_infos(index_infos))) {
    LOG_WARN("fail to get index infos", K(ret));
  } else {
    const ObTableSchema *index_schema = NULL;
    for (int64_t i = 0; OB_SUCC(ret) && i < index_infos.count(); i++) {
      constraint_info.reset();
      if (OB_FAIL(schema_guard->get_table_schema(ctx.get_session_info().get_effective_tenant_id(),
                                                index_infos.at(i).table_id_,
                                                index_schema))) {
        LOG_WARN("fail to get index schema", K(ret), K(index_infos.at(i).table_id_));
      } else if (OB_ISNULL(index_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("index schema null", K(ret));
      } else if (!index_schema->is_final_invalid_index() && index_schema->is_unique_index()) {
        if (OB_FAIL(generate_single_constraint_info(ctx,
                                                    *index_schema,
                                                    ref_table_id,
                                                    constraint_info))) {
          LOG_WARN("fail to generate unique key constraint info", K(ret));
        } else if (OB_FAIL(cst_infos.push_back(constraint_info))) {
          LOG_WARN("fail to push back constraint info", K(ret));
        }
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_constraint_ctdefs(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    sql::ObRowkeyCstCtdefArray &cst_ctdefs)
{
  int ret = OB_SUCCESS;
  ObDMLCtDefAllocator<ObRowkeyCstCtdef> cst_ctdef_allocator(allocator);
  ObSEArray<ObUniqueConstraintInfo, 2> cst_infos;
  ObRowkeyCstCtdef *rowkey_cst_ctdef = nullptr;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());

  if (OB_FAIL(generate_constraint_infos(ctx, cst_infos))) {
    LOG_WARN("fail to generate constraint infos", K(ret), K(ctx));
  } else if (OB_FAIL(cst_ctdefs.init(cst_infos.count()))) {
    LOG_WARN("fail to allocate conflict checker spec array", K(ret), K(cst_infos.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cst_infos.count(); i++) {
    const ObIArray<ObColumnRefRawExpr*> &cst_columns = cst_infos.at(i).constraint_columns_;
    if (OB_ISNULL(rowkey_cst_ctdef = cst_ctdef_allocator.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cst ctdef memory", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator,
                                       cst_infos.at(i).constraint_name_,
                                       rowkey_cst_ctdef->constraint_name_))) {
      LOG_WARN("fail to write string", K(ret), K(cst_infos.at(i).constraint_name_));
    } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_expr_.init(cst_columns.count()))) {
      LOG_WARN("fail to init rowkey", K(ret), K(cst_columns.count()));
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < cst_columns.count(); ++j) {
        const ObTableColumnItem *item = nullptr;
        const ObTableColumnInfo *col_info = nullptr;
        ObColumnRefRawExpr *ref_expr = cst_columns.at(j);
        ObRawExpr *raw_expr = nullptr;
        ObExpr *expr = nullptr;
        if (OB_FAIL(ctx.get_column_item_by_expr(ref_expr, item))) {
          LOG_WARN("fail to column item by expr", K(ret), K(*ref_expr));
        } else if (OB_ISNULL(item)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column item is null", K(ret), K(ctx));
        } else if (OB_ISNULL(col_info = item->column_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column info is null", K(ret));
        } else if (col_info->is_generated_column() && !col_info->is_doc_id_column()) {
          ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(item->raw_expr_);
          raw_expr = col_ref_expr->get_dependant_expr();
        } else {
          // why use column ref expr:
          // rowkey_cst_ctdef.rowkey_expr_ is used to build key in conflict checker map
          // conflict_checker_ctdef.table_column_exprs_ store the conflict row which lookup from data table
          // and table_column_exprs_ use column ref expr no need to calculate repeadtedly except generated cloumn expr
          raw_expr = item->expr_;
        }
        if (OB_FAIL(ret)) {
        } else if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("raw expr is NULL", K(ret));
        } else if (OB_FAIL(cg.generate_rt_expr(*raw_expr, expr))) {
          LOG_WARN("fail to generate rt expr", K(ret));
        } else if (OB_FAIL(rowkey_cst_ctdef->rowkey_expr_.push_back(expr))) {
          LOG_WARN("fail to push back rt expr", K(ret));
        }
      }

      if (FAILEDx(cst_ctdefs.push_back(rowkey_cst_ctdef))) {
        LOG_WARN("fail to push back rowkey constraint ctdef", K(ret));
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_conflict_checker_ctdef(ObTableCtx &ctx,
                                                         ObIAllocator &allocator,
                                                         ObTableIndexInfo &index_info,
                                                         ObConflictCheckerCtdef &conflict_checker_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 8> rowkey_exprs;
  ObSEArray<ObRawExpr*, 64> table_column_exprs;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  if (OB_FAIL(get_rowkey_exprs(ctx, rowkey_exprs))) {
    LOG_WARN("fail to get table rowkey exprs", K(ret), K(ctx));
  } else if (OB_FAIL(replace_exprs(ctx, index_info, true, table_column_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(generate_tsc_ctdef(ctx, table_column_exprs, conflict_checker_ctdef.das_scan_ctdef_))) {
    LOG_WARN("fail to generate das_scan_ctdef", K(ret), K(table_column_exprs));
  } else if (OB_FAIL(ObTableFtsDmlCgService::generate_scan_with_doc_id_ctdef_if_need(ctx, allocator,
                    conflict_checker_ctdef.das_scan_ctdef_,conflict_checker_ctdef.attach_spec_))) {
    LOG_WARN("fail to generate scan with doc id ctdef if need", K(ret));
  } else if (OB_FAIL(generate_constraint_ctdefs(ctx, allocator, conflict_checker_ctdef.cst_ctdefs_))) {
    LOG_WARN("fail to generate constraint infos", K(ret), K(ctx));
  } else if (OB_FAIL(cg.generate_rt_exprs(rowkey_exprs, conflict_checker_ctdef.data_table_rowkey_expr_))) {
    LOG_WARN("fail to generate data table rowkey expr", K(ret), K(rowkey_exprs));
  } else if (OB_FAIL(cg.generate_rt_exprs(table_column_exprs, conflict_checker_ctdef.table_column_exprs_))) {
    LOG_WARN("fail to generate table columns rt exprs ", K(ret), K(table_column_exprs));
  } else {
    conflict_checker_ctdef.use_dist_das_ = ctx.need_dist_das();
    conflict_checker_ctdef.rowkey_count_ = ctx.get_table_schema()->get_rowkey_column_num();
  }

  // 针对全局索引场景，生成回表的partition id表达式
  if (OB_SUCC(ret) && (ctx.need_dist_das())) {
    ObRawExpr *part_id_expr_for_lookup = NULL;
    ObExpr *rt_part_id_expr = NULL;
    ObSEArray<ObRawExpr *, 4> constraint_dep_exprs;
    ObSEArray<ObRawExpr *, 4> constraint_raw_exprs;
    if (OB_ISNULL(part_id_expr_for_lookup = index_info.lookup_part_id_expr_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part_id_expr for lookup is null", K(ret), K(index_info));
    } else if (OB_FAIL(cg.generate_rt_expr(*part_id_expr_for_lookup, rt_part_id_expr))) {
      LOG_WARN("generate rt part_id_expr failed", K(ret), KPC(part_id_expr_for_lookup));
    } else if (OB_ISNULL(rt_part_id_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("rt part_id_expr for lookup is null", K(ret));
    } else if (OB_FAIL(constraint_raw_exprs.push_back(part_id_expr_for_lookup))) {
      LOG_WARN("fail to push back part_id_expr_for_lookup", K(ret));
    } else if (OB_FAIL(generate_calc_raw_exprs(constraint_raw_exprs, constraint_dep_exprs))) {
      LOG_WARN("fail to generate tablet id dep exprs", K(ret));
    } else if (OB_FAIL(cg.generate_rt_exprs(constraint_dep_exprs,
                                            conflict_checker_ctdef.part_id_dep_exprs_))) {
      LOG_WARN("fail to generate part_id_dep_exprs_", K(ret));
    } else {
      conflict_checker_ctdef.calc_part_id_expr_ = rt_part_id_expr;
    }
  }
  return ret;
}

// only use for following func: generate_calc_raw_exprs()
struct FilterFunctor {
  FilterFunctor(ObIArray<ObRawExpr *> &dep_exprs)
      : dep_exprs_(dep_exprs)
  {}
  bool filter_func(ObRawExpr *e) { return !has_exist_in_array(dep_exprs_, e); }
  ObIArray<ObRawExpr *> &dep_exprs_;
};

int ObTableDmlCgService::generate_calc_raw_exprs(ObIArray<ObRawExpr *> &raw_exprs, ObIArray<ObRawExpr *> &calc_exprs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr *, 16> calc_raw_exprs;
  ObRawExprUniqueSet flattened_exprs(true);
  FilterFunctor fliter_functor(calc_exprs);
  std::function<bool(ObRawExpr *)> filter_func = std::bind(&FilterFunctor::filter_func, &fliter_functor, std::placeholders::_1);
  if (OB_FAIL(flattened_exprs.flatten_and_add_raw_exprs(raw_exprs, filter_func, true))) {
    LOG_WARN("fail to flatten rt exprs", K(ret));
  }
  const ObIArray<ObRawExpr *> &flattened_exprs_arr = flattened_exprs.get_expr_array();
  for (int64_t i = 0; OB_SUCC(ret) && i < flattened_exprs_arr.count(); i++) {
    ObRawExpr *raw_expr = flattened_exprs_arr.at(i);
    if (OB_ISNULL(raw_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("raw expr is NULL", K(ret));
    } else if (raw_expr->is_column_ref_expr() || raw_expr->is_const_expr()) {
      // do nothing
    } else if (OB_FAIL(calc_exprs.push_back(raw_expr))) {
      LOG_WARN("fail to push back raw expr", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_insert_up_ctdef(ObTableCtx &ctx,
                                                  ObIAllocator &allocator,
                                                  ObTableIndexInfo &index_info,
                                                  ObTableInsUpdCtDef &ins_up_ctdef)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(generate_insert_ctdef(ctx, allocator, index_info, ins_up_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), K(ctx));
  } else if (OB_FAIL(generate_table_rowkey_info(ctx, ins_up_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate table rowkey info", K(ret), K(ctx));
  } else if (OB_FAIL(generate_update_ctdef(ctx, allocator, index_info, ins_up_ctdef.upd_ctdef_))) {
    LOG_WARN("fail to generate update ctdef", K(ret), K(ctx));
  }

  return ret;
}

int ObTableDmlCgService::generate_ttl_ctdef(ObTableCtx &ctx,
                                            ObIAllocator &allocator,
                                            ObTableIndexInfo &index_info,
                                            ObTableTTLCtDef &ttl_ctdef)
{
  int ret = OB_SUCCESS;
  ObIArray<sql::ObRawExpr *> &filter_exprs = ctx.get_filter_exprs();
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());

  if (OB_FAIL(generate_insert_ctdef(ctx, allocator, index_info, ttl_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate insert ctdef", K(ret), K(ctx));
  } else if (OB_FAIL(generate_table_rowkey_info(ctx, ttl_ctdef.ins_ctdef_))) {
    LOG_WARN("fail to generate table rowkey info", K(ret), K(ctx));
  } else if (OB_FAIL(generate_delete_ctdef(ctx, allocator, index_info, ttl_ctdef.del_ctdef_))) {
    LOG_WARN("fail to generate delete ctdef", K(ret), K(ctx));
  } else if (OB_FAIL(generate_update_ctdef(ctx, allocator, index_info, ttl_ctdef.upd_ctdef_))) {
    LOG_WARN("fail to generate update ctdef", K(ret), K(ctx));
  } else if (filter_exprs.count() != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid filter exprs count", K(ret), K(filter_exprs));
  } else if (OB_FAIL(cg.generate_rt_expr(*filter_exprs.at(0), ttl_ctdef.expire_expr_))) {
    LOG_WARN("fail to generate expire rt expr", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_lock_ctdef(ObTableCtx &ctx,
                                             ObTableIndexInfo &index_info,
                                             ObTableLockCtDef &lock_ctdef)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  ObArray<ObRawExpr*> old_row;
  ObSEArray<ObRawExpr*, 64> tmp_exprs;

  if (OB_FAIL(replace_exprs(ctx, index_info, true, tmp_exprs))) {
    LOG_WARN("fail to replace exprs with dependant", K(ret));
  } else if (OB_FAIL(old_row.assign(tmp_exprs))) {
    LOG_WARN("fail to assign old row expr", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(old_row, lock_ctdef.old_row_))) {
    LOG_WARN("fail to generate lock rt exprs", K(ret), K(old_row));
  } else if (OB_FAIL(generate_das_lock_ctdef(ctx,
                                             index_info,
                                             index_info.index_table_id_,
                                             lock_ctdef.das_ctdef_,
                                             old_row))) {
    LOG_WARN("fail to generate das lock ctdef", K(ret));
  } else if (OB_NOT_NULL(index_info.old_part_id_expr_) &&
             OB_FAIL(generate_calc_tablet_id_rt_expr(ctx, *index_info.old_part_id_expr_, lock_ctdef.old_part_id_expr_))) {
    LOG_WARN("fail to generate calc tablet id expr", K(ret));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_lock_ctdef(ObTableCtx &ctx,
                                                 ObTableIndexInfo &index_info,
                                                 uint64_t index_tid,
                                                 ObDASLockCtDef &das_lock_ctdef,
                                                 const ObIArray<ObRawExpr*> &old_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_new_row;

  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_lock_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx, index_info, dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids,
                                        das_lock_ctdef.column_ids_,
                                        old_row,
                                        empty_new_row,
                                        old_row,
                                        das_lock_ctdef))) {
    LOG_WARN("fail to add old row projector", K(ret), K(old_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_base_ctdef(ObTableCtx &ctx,
                                             ObTableIndexInfo &index_info,
                                             ObTableDmlBaseCtDef &base_ctdef,
                                             ObIArray<ObRawExpr*> &old_row,
                                             ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());

  if (OB_FAIL(generate_column_ids(ctx, index_info, base_ctdef.column_ids_))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(old_row, base_ctdef.old_row_))) {
    LOG_WARN("fail to generate old row exprs", K(ret), K(old_row));
  } else if (OB_FAIL(cg.generate_rt_exprs(new_row, base_ctdef.new_row_))) {
    LOG_WARN("fail to generate new row exprs", K(ret), K(new_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_ins_ctdef(ObTableCtx &ctx,
                                                ObTableIndexInfo &index_info,
                                                uint64_t index_tid,
                                                ObDASInsCtDef &das_ins_ctdef,
                                                const ObIArray<ObRawExpr*> &new_row)
{
  int ret = OB_SUCCESS;
  ObArray<uint64_t> dml_column_ids;
  ObArray<ObRawExpr*> empty_old_row;
  if (OB_FAIL(generate_das_base_ctdef(index_tid, ctx, das_ins_ctdef))) {
    LOG_WARN("fail to generate das dml ctdef", K(ret));
  } else if (OB_FAIL(generate_column_ids(ctx, index_info, dml_column_ids))) {
    LOG_WARN("fail to generate dml column ids", K(ret));
  } else if (OB_FAIL(generate_projector(dml_column_ids, // new row and old row's columns id
                                        das_ins_ctdef.column_ids_, // schmea column ids for given index_tid
                                        empty_old_row,
                                        new_row,
                                        new_row,
                                        das_ins_ctdef))) {
    LOG_WARN("fail to add new row projector", K(ret), K(new_row));
  }

  return ret;
}

int ObTableDmlCgService::generate_das_base_ctdef(uint64_t index_tid,
                                                 ObTableCtx &ctx,
                                                 ObDASDMLBaseCtDef &base_ctdef)
{
  int ret = OB_SUCCESS;
  base_ctdef.index_tid_ = index_tid;  // index_table_id, include local_index
  base_ctdef.is_ignore_ = false; // insert ignore
  base_ctdef.is_batch_stmt_ = false;
  base_ctdef.is_table_api_ = true;
  ObSQLSessionInfo &session = ctx.get_session_info();
  base_ctdef.table_id_ = ctx.get_ref_table_id();  // loc_table_id
  ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(generate_column_info(index_tid, ctx, base_ctdef))) {
    LOG_WARN("fail to generate column info", K(ret), K(index_tid), K(ctx));
  } else if (OB_FAIL(schema_guard->get_schema_version(TABLE_SCHEMA,
                                                      ctx.get_tenant_id(),
                                                      index_tid,
                                                      base_ctdef.schema_version_))) {
    LOG_WARN("fail to get table schema version", K(ret));
  } else if (OB_FAIL(convert_table_param(ctx, base_ctdef))) {
    LOG_WARN("fail to convert table dml param", K(ret));
  } else {
    base_ctdef.tz_info_ = *session.get_tz_info_wrap().get_time_zone_info();
    base_ctdef.is_total_quantity_log_ = ctx.is_total_quantity_log();
    base_ctdef.encrypt_meta_.reset();
  }

  return ret;
}

// add column_ids, column_types, column_accuracys, rowkey_cnt, spk_cnt to ObDASDMLBaseCtDef
// according to table schema order
int ObTableDmlCgService::generate_column_info(ObTableID index_tid,
                                              ObTableCtx &ctx,
                                              ObDASDMLBaseCtDef &base_ctdef)
{
  int ret = OB_SUCCESS;
  base_ctdef.column_ids_.reset();
  base_ctdef.column_types_.reset();
  const ObTableSchema *index_schema = nullptr;
  ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(ctx.get_tenant_id(),
                                                      index_tid,
                                                      index_schema))) {
    LOG_WARN("fail to get table schema", K(ret), K(index_tid));
  } else {
    int64_t column_count = index_schema->get_column_count();
    base_ctdef.column_ids_.set_capacity(column_count);
    base_ctdef.column_types_.set_capacity(column_count);
    base_ctdef.column_accuracys_.set_capacity(column_count);
    base_ctdef.rowkey_cnt_ = index_schema->get_rowkey_info().get_size();
    base_ctdef.spk_cnt_ = index_schema->get_shadow_rowkey_info().get_size();

    /*
      add rowkey column infos:
      rowkey columns in rowkey_info is different:
      - for primary table: primary keys
      - for index table:
        - unique index (local and global): index columns + shadow primary keys
        - not unique index: index columns + primary keys
    */
    const ObRowkeyInfo &rowkey_info = index_schema->get_rowkey_info();
    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_info.get_size(); ++i) {
      const ObRowkeyColumn *rowkey_column = rowkey_info.get_column(i);
      const ObColumnSchemaV2 *column = index_schema->get_column_schema(rowkey_column->column_id_);
      ObObjMeta column_type;
      column_type = column->get_meta_type();
      column_type.set_scale(column->get_accuracy().get_scale());
      if (is_lob_storage(column_type.get_type())) {
        if (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
          column_type.set_has_lob_header();
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(base_ctdef.column_ids_.push_back(column->get_column_id()))) {
        LOG_WARN("fail to add column id", K(ret));
      } else if (OB_FAIL(base_ctdef.column_types_.push_back(column_type))) {
        LOG_WARN("fail to add column type", K(ret));
      } else if (OB_FAIL(base_ctdef.column_accuracys_.push_back(column->get_accuracy()))) {
        LOG_WARN("fail to add column accuracys", K(ret));
      }
    }

    // add normal column infos if need
    if (OB_SUCC(ret)) {
      ObTableSchema::const_column_iterator iter = index_schema->column_begin();
      for (; OB_SUCC(ret) && iter != index_schema->column_end(); ++iter) {
        const ObColumnSchemaV2 *column = *iter;
        if (column->is_rowkey_column() || column->is_virtual_generated_column()) {
          // do nothing
        } else {
          ObObjMeta column_type;
          column_type = column->get_meta_type();
          column_type.set_scale(column->get_accuracy().get_scale());
          if (is_lob_storage(column_type.get_type())) {
            if (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0) {
              column_type.set_has_lob_header();
            }
          }
          if (OB_FAIL(base_ctdef.column_ids_.push_back(column->get_column_id()))) {
            LOG_WARN("fail to add column id", K(ret));
          } else if (OB_FAIL(base_ctdef.column_types_.push_back(column_type))) {
            LOG_WARN("fail to add column type", K(ret));
          } else if (OB_FAIL(base_ctdef.column_accuracys_.push_back(column->get_accuracy()))) {
            LOG_WARN("fail to add column accuracys", K(ret));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableDmlCgService::convert_table_param(ObTableCtx &ctx,
                                             ObDASDMLBaseCtDef &base_ctdef)
{
  int ret = OB_SUCCESS;
  int64_t schema_version = OB_INVALID_VERSION;
  const ObTableSchema *table_schema = NULL;
  uint64_t tenant_id = ctx.get_tenant_id();
  ObSchemaGetterGuard *schema_guard = ctx.get_schema_guard();

  if (OB_ISNULL(schema_guard)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema guard is NULL", K(ret));
  } else if (OB_FAIL(schema_guard->get_table_schema(tenant_id,
                                                    base_ctdef.index_tid_,
                                                    table_schema))) {
    LOG_WARN("fail to get schema", K(ret), K(base_ctdef));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_SCHEMA_ERROR;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (OB_FAIL(schema_guard->get_schema_version(tenant_id, schema_version))) {
    LOG_WARN("fail to get tenant schema version", K(ret), K(tenant_id));
  } else if (OB_FAIL(base_ctdef.table_param_.convert(table_schema,
                                                     schema_version,
                                                     base_ctdef.column_ids_))) {
    LOG_WARN("fail to convert table param", K(ret), K(base_ctdef));
  }

  return ret;
}

int ObTableDmlCgService::generate_column_ids(ObTableCtx &ctx, ObTableIndexInfo &index_info, ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  const ObTableSchema *index_schema = nullptr;
  ObSEArray<uint64_t, 64> tmp_column_ids;
  column_ids.reset();
  if (OB_ISNULL(index_schema = index_info.index_schema_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("index schema is NULL", K(ret));
  } else if (OB_FAIL(index_schema->get_column_ids(tmp_column_ids))) {
    LOG_WARN("fail to get column ids", K(ret));
  } else if (OB_FAIL(column_ids.assign(tmp_column_ids))) {
    LOG_WARN("fail to assign column ids", K(ret));
  }

  return ret;
}

// 构造 das_ctdef 中的 old_row_projector 和 new_row_projector，
// 其中存储 storage column 对应表达式在 full row exprs 数组中下标
int ObTableDmlCgService::generate_projector(const ObIArray<uint64_t> &dml_column_ids,
                                            const ObIArray<uint64_t> &storage_column_ids,
                                            const ObIArray<ObRawExpr*> &old_row,
                                            const ObIArray<ObRawExpr*> &new_row,
                                            const ObIArray<ObRawExpr*> &full_row,
                                            ObDASDMLBaseCtDef &das_ctdef)
{
  int ret = OB_SUCCESS;
  IntFixedArray &old_row_projector = das_ctdef.old_row_projector_;
  IntFixedArray &new_row_projector = das_ctdef.new_row_projector_;

  // generate old row projector
  // 查找old row expr 在full row expr中的位置（投影）
  if (!old_row.empty()) {
    if (OB_FAIL(old_row_projector.prepare_allocate(storage_column_ids.count()))) {
      LOG_WARN("fail to init row projector array", K(ret), K(storage_column_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_column_ids.count(); ++i) {
      uint64_t storage_cid = storage_column_ids.at(i);
      uint64_t ref_cid = is_shadow_column(storage_cid) ?
                         storage_cid - OB_MIN_SHADOW_COLUMN_ID :
                         storage_cid;
      int64_t column_idx = OB_INVALID_INDEX;
      int64_t projector_idx = OB_INVALID_INDEX;
      old_row_projector.at(i) = OB_INVALID_INDEX;
      if (has_exist_in_array(dml_column_ids, ref_cid, &column_idx)) {
        ObRawExpr *column_expr = old_row.at(column_idx);
        if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row column not found in full row columns", K(ret),
                   K(column_idx), KPC(old_row.at(column_idx)));
        } else {
          old_row_projector.at(i) = projector_idx;
        }
      }
    }
  }

  // generate new row projector
  // 查找new row expr 在full row expr中的位置（投影）
  if (!new_row.empty() && OB_SUCC(ret)) {
    if (OB_FAIL(new_row_projector.prepare_allocate(storage_column_ids.count()))) {
      LOG_WARN("fail to init row projector array", K(ret), K(storage_column_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < storage_column_ids.count(); ++i) {
      uint64_t storage_cid = storage_column_ids.at(i);
      uint64_t ref_cid = is_shadow_column(storage_cid) ?
                         storage_cid - OB_MIN_SHADOW_COLUMN_ID :
                         storage_cid;
      int64_t column_idx = OB_INVALID_INDEX;
      int64_t projector_idx = OB_INVALID_INDEX;
      // 如果 projector[i] = j, 表达式按照 schema 顺序，第 i 个 column 在 full_row 中的下标为 j，如果在 new_row 中不存在，那么 j == -1
      new_row_projector.at(i) = OB_INVALID_INDEX;
      if (has_exist_in_array(dml_column_ids, ref_cid, &column_idx)) {
        ObRawExpr *column_expr = new_row.at(column_idx);
        if (!has_exist_in_array(full_row, column_expr, &projector_idx)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row column not found in full row columns", K(ret),
                   K(column_idx), KPC(new_row.at(column_idx)));
        } else {
          new_row_projector.at(i) = projector_idx; // projector_idx 为 column storage_column_ids[i] 在 full row expr 中的 index
        }
      }
    }
  }
  return ret;
}


int ObTableDmlCgService::generate_related_ins_ctdef(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    ObTableIndexInfo &index_info,
                                                    const ObIArray<ObRawExpr*> &new_row,
                                                    DASInsCtDefArray &ins_ctdefs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableID, 4> related_index_tids;
  related_index_tids.assign(index_info.related_index_ids_);
  ins_ctdefs.set_capacity(related_index_tids.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < related_index_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASInsCtDef> das_alloc(allocator);
    ObDASInsCtDef *related_das_ctdef = nullptr;
    if (OB_ISNULL(related_das_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate insert related das ctdef", K(ret));
    } else if (OB_FAIL(generate_das_ins_ctdef(ctx,
                                              index_info,
                                              related_index_tids.at(i),
                                              *related_das_ctdef,
                                              new_row))) {
      LOG_WARN("fail to generate das ins ctdef", K(ret));
    } else if (OB_FAIL(ins_ctdefs.push_back(related_das_ctdef))) {
      LOG_WARN("fail to store related ctdef", K(ret));
    }
  }

  return ret;
}

int ObTableDmlCgService::generate_related_upd_ctdef(ObTableCtx &ctx,
                                                    ObIAllocator &allocator,
                                                    ObTableIndexInfo &index_info,
                                                    const ObIArray<ObRawExpr*> &old_row,
                                                    const ObIArray<ObRawExpr*> &new_row,
                                                    const ObIArray<ObRawExpr*> &full_row,
                                                    DASUpdCtDefArray &upd_ctdefs)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTableID, 4> related_index_tids;
  related_index_tids.assign(index_info.related_index_ids_);
  upd_ctdefs.set_capacity(related_index_tids.count());

  for (int64_t i = 0; OB_SUCC(ret) && i < related_index_tids.count(); ++i) {
    ObDMLCtDefAllocator<ObDASUpdCtDef> das_alloc(allocator);
    ObDASUpdCtDef *related_das_ctdef = nullptr;
    if (OB_ISNULL(related_das_ctdef = das_alloc.alloc())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate update related das ctdef", K(ret));
    } else if (OB_FAIL(generate_das_upd_ctdef(ctx,
                                              index_info,
                                              related_index_tids.at(i),
                                              *related_das_ctdef,
                                              old_row,
                                              new_row,
                                              full_row))) {
      LOG_WARN("fail to generate das update ctdef", K(ret));
    } else if (related_das_ctdef->updated_column_ids_.empty()) {
      // ignore invalid update ctdef
    } else if (OB_FAIL(upd_ctdefs.push_back(related_das_ctdef))) {
      LOG_WARN("fail to store related ctdef", K(ret));
    }
  }

  return ret;
}

int ObTableTscCgService::generate_rt_exprs(const ObTableCtx &ctx,
                                           ObIAllocator &allocator,
                                           const ObIArray<ObRawExpr *> &src,
                                           ObIArray<ObExpr *> &dst)
{
  int ret = OB_SUCCESS;
  ObSQLSessionInfo *session_info = const_cast<ObSQLSessionInfo*>(&ctx.get_session_info());
  ObSchemaGetterGuard *schema_guard = const_cast<ObSchemaGetterGuard*>(ctx.get_schema_guard());
  ObStaticEngineExprCG expr_cg(allocator,
                               session_info,
                               schema_guard,
                               0,
                               0,
                               ctx.get_cur_cluster_version());
  if (!src.empty()) {
    if (OB_FAIL(dst.reserve(src.count()))) {
      LOG_WARN("fail to init fixed array", K(ret), K(src.count()));
    } else {
      ObArray<ObRawExpr*> exprs;
      for (int64_t i = 0; OB_SUCC(ret) && i < src.count(); i++) {
        ObExpr *e = nullptr;
        if (OB_FAIL(ObStaticEngineExprCG::generate_rt_expr(*src.at(i), exprs, e))) {
          LOG_WARN("fail to generate rt expr", K(ret));
        } else if (OB_ISNULL(e)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret));
        } else if (OB_FAIL(dst.push_back(e))) {
          LOG_WARN("fail to push back rt expr", K(ret), K(i));
        }
      }
    }
  }

  return ret;
}

// 访问虚拟生成列转换为访问其依赖的列
int ObTableTscCgService::replace_gen_col_exprs(const ObTableCtx &ctx,
                                               ObIArray<ObRawExpr*> &access_exprs)
{
  int ret = OB_SUCCESS;
  if (!ctx.has_generated_column()) {
    // do nothing
  } else {
    ObSEArray<ObRawExpr*, 64> res_access_expr;
    const ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
    ObColumnRefRawExpr *ref_expr = nullptr;

    for (int64_t i = 0; i < access_exprs.count() && OB_SUCC(ret); i++) {
      ObRawExpr *expr = access_exprs.at(i);
      if (!expr->is_column_ref_expr()) {
        if (OB_FAIL(res_access_expr.push_back(expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        }
      } else if (FALSE_IT(ref_expr = static_cast<ObColumnRefRawExpr*>(expr))) {
      } else if (!ref_expr->is_virtual_generated_column() || ref_expr->is_doc_id_column()) {
        if (OB_FAIL(res_access_expr.push_back(expr))) {
          LOG_WARN("fail to push back expr", K(ret));
        }
      } else {
        for (int j = 0; j < items.count() && OB_SUCC(ret); j++) {
          const ObTableColumnItem &item = items.at(j);
          if (item.expr_ == expr) {
            if (OB_FAIL(append_array_no_dup(res_access_expr, item.dependant_exprs_))) {
              LOG_WARN("fail to append array no dup", K(ret), K(res_access_expr), K(item));
            }
          }
        }
      }
    }

    if (OB_SUCC(ret)) {
      access_exprs.reset();
      if (OB_FAIL(access_exprs.assign(res_access_expr))) {
        LOG_WARN("fail to assign access expr", K(ret));
      }
    }
  }

  return ret;
}

int ObTableTscCgService::generate_pushdown_aggr_ctdef(const ObTableCtx &ctx,
                                                      ObDASScanCtDef &das_tsc_ctdef)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObAggFunRawExpr *> &aggr_exprs = ctx.get_pushdown_aggr_exprs();
  const uint64_t output_count = aggr_exprs.count();
  if (ctx.is_index_back() && output_count > 0) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("pushdown aggr to table scan not supported in index lookup", K(ctx));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "pushdown aggr to table scan not supported in index lookup");
  } else {
    ExprFixedArray &aggregate_output = das_tsc_ctdef.pd_expr_spec_.pd_storage_aggregate_output_;
    if (output_count > 0) {
      das_tsc_ctdef.pd_expr_spec_.pd_storage_flag_.set_aggregate_pushdown(true);
      OZ(das_tsc_ctdef.aggregate_column_ids_.init(output_count));
      if (OB_FAIL(aggregate_output.reserve(output_count))) {
        LOG_WARN("init aggregate output array", K(ret), K(output_count));
      } else {
        ObStaticEngineCG cg(ctx.get_cur_cluster_version());
        ObExpr *e = NULL;
        FOREACH_CNT_X(raw_expr, aggr_exprs, OB_SUCC(ret)) {
          CK(OB_NOT_NULL(*raw_expr));
          OZ(cg.generate_rt_expr(*(*raw_expr), e));
          CK(OB_NOT_NULL(e));
          OZ(aggregate_output.push_back(e));
          // aggr_exprs in convert_table_scan need to set IS_COLUMNLIZED flag
          (const_cast<ObAggFunRawExpr *>(*raw_expr))->clear_flag(IS_COLUMNLIZED);
        }
      }
    }

    ARRAY_FOREACH(aggr_exprs, i) {
      ObAggFunRawExpr *aggr_expr = aggr_exprs.at(i);
      ObRawExpr *param_expr = NULL;
      ObColumnRefRawExpr* col_expr = NULL;
      if (OB_ISNULL(aggr_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("aggr expr is null", K(ret));
      } else if (aggr_expr->get_real_param_exprs().empty()) {
        OZ(das_tsc_ctdef.aggregate_column_ids_.push_back(OB_COUNT_AGG_PD_COLUMN_ID)); // count(*)
      } else if (OB_ISNULL(param_expr = aggr_expr->get_param_expr(0)) ||
                 OB_UNLIKELY(!param_expr->is_column_ref_expr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expected basic column", K(ret), K(param_expr));
      } else if (OB_FALSE_IT(col_expr = static_cast<ObColumnRefRawExpr *>(param_expr))) {
      } else if (OB_UNLIKELY(col_expr->get_table_id() != ctx.get_index_table_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("expected basic column", K(ret), K(col_expr->get_table_id()),
                                           K(ctx.get_index_table_id()));
      } else {
        OZ(das_tsc_ctdef.aggregate_column_ids_.push_back(col_expr->get_column_id()));
      }
    }
  }
  return ret;
}


// 非索引扫描: access exprs = select exprs
// 普通索引表: access exprs = [index column exprs][rowkey expr]
// 索引回表: access expr = [rowkey expr][select without rowkey exprs]
// 全文索引表：access exprs = extract_text_ir_access_columns
int ObTableTscCgService::generate_access_ctdef(const ObTableCtx &ctx,
                                               ObIAllocator &allocator,
                                               ObDASScanCtDef &das_tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObRawExpr*, 64> access_exprs;
  const ObIArray<oceanbase::sql::ObColumnRefRawExpr *> &select_exprs = ctx.get_select_exprs();
  const ObIArray<oceanbase::sql::ObRawExpr *> &rowkey_exprs = ctx.get_rowkey_exprs();
  const ObIArray<oceanbase::sql::ObRawExpr *> &index_exprs = ctx.get_index_exprs();
  const ObTableID &scan_table_id = das_tsc_ctdef.ref_table_id_;
  bool is_fts_scan = ctx.is_text_retrieval_scan();
  bool is_normal_index_table = (!is_fts_scan && ctx.is_index_scan() && scan_table_id == ctx.get_index_table_id());
  bool is_primary_table = scan_table_id == ctx.get_ref_table_id();
  bool is_rowkey_doc_table = (ctx.get_fts_ctx() == nullptr) ?
              false : (scan_table_id == ctx.get_fts_ctx()->get_rowkey_doc_tid());
  if (is_fts_scan && !is_primary_table && !is_rowkey_doc_table) { // fts index: doc_id_idx, inv_idx, fwd_idx
    if (OB_FAIL(ObTableFtsTscCgService::extract_text_ir_access_columns(ctx, das_tsc_ctdef, access_exprs))) {
      LOG_WARN("fail to extract text ir access columns", K(ret));
    }
  } else if (ctx.is_tsc_with_doc_id() && is_rowkey_doc_table) { // rowkey docid table
    if (OB_FAIL(ObTableFtsTscCgService::extract_rowkey_doc_exprs(ctx, access_exprs))) {
      LOG_WARN("fail to extract rowkey doc id", K(ret));
    }
  } else if (!ctx.is_index_scan()) { // 非索引扫描
    for (int i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
      if (OB_FAIL(access_exprs.push_back(select_exprs.at(i)))) {
        LOG_WARN("fail to push back access exprs", K(ret), K(i));
      }
    }
  } else if (is_normal_index_table) { // 非全文索引表
    if (OB_FAIL(access_exprs.assign(index_exprs))) {
      LOG_WARN("fail to assign access exprs", K(ret), K(ctx.get_index_table_id()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_exprs.count(); i++) {
        if (is_in_array(index_exprs, rowkey_exprs.at(i))) {
          // 在index_exprs中的index expr不需要再次添加
        } else if (OB_FAIL(access_exprs.push_back(rowkey_exprs.at(i)))) {
          LOG_WARN("fail to push back rowkey expr", K(ret), K(i));
        }
      }
    }
  } else if (ctx.is_index_scan() && is_primary_table) { // 索引回表
    if (OB_FAIL(access_exprs.assign(rowkey_exprs))) {
      LOG_WARN("fail to assign access exprs", K(ret), K(ctx.get_ref_table_id()));
    } else {
      ObSEArray<uint64_t, 8> rowkey_column_ids;
      if (OB_FAIL(ctx.get_table_schema()->get_rowkey_column_ids(rowkey_column_ids))) {
        LOG_WARN("fail to get rowkey column ids", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
        if (has_exist_in_array(rowkey_column_ids, select_exprs.at(i)->get_column_id())) {
          // 已经在rowkey中，不需要再次添加
        } else if (OB_FAIL(access_exprs.push_back(select_exprs.at(i)))) {
          LOG_WARN("fail to push back select expr", K(ret), K(i));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (is_primary_table && OB_FAIL(replace_gen_col_exprs(ctx, access_exprs))) {
      LOG_WARN("fail to replace generate exprs", K(ret));
    } else if (OB_FAIL(generate_rt_exprs(ctx, allocator, access_exprs, das_tsc_ctdef.pd_expr_spec_.access_exprs_))) {
      LOG_WARN("fail to generate access rt exprs", K(ret), K(access_exprs));
    } else if (OB_FAIL(das_tsc_ctdef.access_column_ids_.init(access_exprs.count()))) {
      LOG_WARN("fail to init access column ids", K(ret), K(access_exprs.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < access_exprs.count(); i++) {
        ObRawExpr *raw_expr = access_exprs.at(i);
        if (OB_ISNULL(raw_expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("expr is null", K(ret), K(access_exprs));
        } else if (raw_expr->is_column_ref_expr()) {
          ObColumnRefRawExpr *col_ref_expr = static_cast<ObColumnRefRawExpr*>(raw_expr);
          if (OB_FAIL(das_tsc_ctdef.access_column_ids_.push_back(col_ref_expr->get_column_id()))) {
            LOG_WARN("fail to push back column id", K(ret));
          }
        } else { // calculate expr, find in column items
          const ObTableColumnItem *item = nullptr;
          if (OB_FAIL(ctx.get_column_item_by_expr(raw_expr, item))) {
            LOG_WARN("fail to get column item", K(ret), K(*raw_expr));
          } else if (OB_ISNULL(item) || OB_ISNULL(item->column_info_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column item not found or column info is NULL", K(ret), KP(item));
          } else if (OB_FAIL(das_tsc_ctdef.access_column_ids_.push_back(item->column_info_->column_id_))) {
            LOG_WARN("fail to push back column id", K(ret), K(*item));
          }
        }
      }
    }
  }

  return ret;
}

int ObTableTscCgService::generate_das_result_output(ObDASScanCtDef &das_tsc_ctdef,
                                                    const ObIArray<uint64_t> &output_cids)
{
  int ret = OB_SUCCESS;
  ExprFixedArray &access_exprs = das_tsc_ctdef.pd_expr_spec_.access_exprs_;
  ExprFixedArray &agg_exprs = das_tsc_ctdef.pd_expr_spec_.pd_storage_aggregate_output_;
  const ObIArray<uint64_t> &access_cids = das_tsc_ctdef.access_column_ids_;
  int64_t access_column_cnt = access_cids.count();
  int64_t access_expr_cnt = access_exprs.count();
  if (OB_UNLIKELY(access_column_cnt != access_expr_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("access column count is invalid", K(ret), K(access_column_cnt), K(access_expr_cnt));
  } else if (OB_FAIL(das_tsc_ctdef.result_output_.init(output_cids.count() + agg_exprs.count() + 1))) {
    LOG_WARN("fail to init result output", K(ret), K(output_cids.count()), K(agg_exprs.count()));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < output_cids.count(); i++) {
    for (int64_t j = 0; OB_SUCC(ret) && j < access_column_cnt; j++) {
      if (output_cids.at(i) == access_cids.at(j)) {
        if (OB_FAIL(das_tsc_ctdef.result_output_.push_back(access_exprs.at(j)))) {
          LOG_WARN("fail to push result output expr", K(ret), K(i), K(j));
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < agg_exprs.count(); ++i) {
      if (OB_FAIL(das_tsc_ctdef.result_output_.push_back(agg_exprs.at(i)))) {
        LOG_WARN("store agg expr to result output exprs failed", K(ret), K(i), K(agg_exprs.count()));
      }
    }
  }

  return ret;
}

int ObTableTscCgService::extract_select_output_column_ids(const ObTableCtx &ctx,
                                                          ObDASScanCtDef &das_tsc_ctdef,
                                                          const ObTableSchema *index_schema,
                                                          ObIArray<uint64_t> &tsc_out_cols)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnRefRawExpr *> &select_exprs = ctx.get_select_exprs();
  const ObTableID &table_id = das_tsc_ctdef.ref_table_id_;
  for (int64_t i = 0; OB_SUCC(ret) && i < select_exprs.count(); i++) {
    const ObTableColumnInfo *col_info = nullptr;
    ObColumnRefRawExpr *select_expr = select_exprs.at(i);
    if (OB_ISNULL(select_expr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("expr is null", K(ret));
    } else {
      const ObTableColumnItem *item = nullptr;
      if (OB_FAIL(ctx.get_column_item_by_expr(select_expr, item))) {
        LOG_WARN("fail to get column item", K(ret), KPC(select_expr));
      } else if (OB_ISNULL(item)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column item not found", K(ret), K(i));
      } else if (OB_ISNULL(col_info = item->column_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column info is NULL", K(ret), K(i));
      } else if (!col_info->is_generated_column() || col_info->is_doc_id_column()) {
        if (OB_FAIL(tsc_out_cols.push_back(col_info->column_id_))) {
          LOG_WARN("fail to push back column id", K(ret), K(tsc_out_cols), K(*item));
        }
      } else { // generate column. push dependent column ids
        for (int64_t j = 0; j < item->dependant_exprs_.count() && OB_SUCC(ret); j++) {
          ObColumnRefRawExpr *dep_col_expr = static_cast<ObColumnRefRawExpr*>(item->dependant_exprs_.at(j));
          if (OB_FAIL(add_var_to_array_no_dup(tsc_out_cols, dep_col_expr->get_column_id()))) {
            LOG_WARN("fail to add column id", K(ret), K(tsc_out_cols), K(*dep_col_expr));
          }
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (ctx.is_tsc_with_doc_id() && index_schema->is_user_table()) {
    uint64_t doc_id_col_id = OB_INVALID_ID;
    uint64_t ft_col_id = OB_INVALID_ID;
    if (OB_FAIL(index_schema->get_fulltext_column_ids(doc_id_col_id, ft_col_id))) {
      LOG_WARN("fail to get fulltext column ids", K(ret));
    } else if (OB_INVALID_ID == doc_id_col_id) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get doc id column", K(ret));
    } else {
      bool is_found = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < tsc_out_cols.count() && !is_found; ++i) {
        if (tsc_out_cols.at(i) == doc_id_col_id) {
          das_tsc_ctdef.doc_id_idx_ = i;
          is_found = true;
        }
      }
    }
  }
  LOG_DEBUG("[table cg service] extract das output column ids", K(ret), K(table_id),
            K(ctx.is_tsc_with_doc_id()), K(tsc_out_cols));
  return ret;
}

// tsc_out_cols
// 主表/索引回表/索引扫描不需要回表: select column ids
// 索引表: rowkey column ids
// rowkey_doc：rowkey+doc_id
// 全文检索：extract_text_ir_das_output_column_ids
int ObTableTscCgService::generate_table_param(const ObTableCtx &ctx,
                                              ObDASScanCtDef &das_tsc_ctdef,
                                              const bool query_cs_replica /*=false*/)
{
  int ret = OB_SUCCESS;
  const bool pd_agg = das_tsc_ctdef.pd_expr_spec_.pd_storage_flag_.is_aggregate_pushdown();
  ObSEArray<uint64_t, 64> tsc_out_cols;
  const ObTableSchema *index_schema = nullptr;
  const ObTableID &scan_table_id = das_tsc_ctdef.ref_table_id_;
  const ObTableFtsCtx *fts_ctx = ctx.get_fts_ctx();
  bool is_rowkey_doc_table = (fts_ctx == nullptr) ? false : (scan_table_id == fts_ctx->get_rowkey_doc_tid());
  bool is_primary_table = scan_table_id == ctx.get_ref_table_id();
  bool is_index_table = scan_table_id == ctx.get_index_table_id();
  if (ctx.is_text_retrieval_scan() && !is_primary_table) {
    if (OB_FAIL(ObTableFtsTscCgService::get_fts_schema(ctx, scan_table_id, index_schema))) {
      LOG_WARN("fail to get full text index schema", K(ret), K(scan_table_id));
    } else if (OB_FAIL(ObTableFtsTscCgService::extract_text_ir_das_output_column_ids(ctx, das_tsc_ctdef, tsc_out_cols))) {
      LOG_WARN("failed to extract text retrieval das output column ids", K(ret));
    }
  } else if (ctx.is_tsc_with_doc_id() && is_rowkey_doc_table) {
    index_schema = fts_ctx->get_rowkey_doc_schema();
    if (OB_FAIL(tsc_out_cols.assign(das_tsc_ctdef.access_column_ids_))) {
      LOG_WARN("fail to assgin tsc_out_cols", K(ret), K(das_tsc_ctdef.access_column_ids_));
    }
  } else if (is_primary_table // 主表扫描 + 索引扫描回主表
            || (ctx.is_index_scan() && !ctx.is_index_back())) { //索引扫描不需要回表
    if (ctx.is_index_scan() && !ctx.is_index_back()) {
      index_schema = ctx.get_index_schema();
    } else {
      index_schema = ctx.get_table_schema();
    }
    if (OB_FAIL(extract_select_output_column_ids(ctx, das_tsc_ctdef, index_schema, tsc_out_cols))) {
      LOG_WARN("fail to extract tsc output column ids", K(ret));
    }
  } else if (ctx.is_index_scan() && is_index_table) { // 索引表
    index_schema = ctx.get_index_schema();
    if (OB_FAIL(ctx.get_table_schema()->get_rowkey_column_ids(tsc_out_cols))) {
      LOG_WARN("fail to get rowkey column ids", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(index_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is NULL", K(ret));
  } else if (index_schema->is_fts_index() && FALSE_IT(das_tsc_ctdef.table_param_.set_is_fts_index(true))) {
  } else {
    das_tsc_ctdef.schema_version_ = index_schema->get_schema_version();
    das_tsc_ctdef.table_param_.get_enable_lob_locator_v2() = (ctx.get_cur_cluster_version() >= CLUSTER_VERSION_4_1_0_0);
    das_tsc_ctdef.table_param_.set_is_partition_table(index_schema->is_partitioned_table());
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(das_tsc_ctdef.table_param_.convert(*index_schema,
                                                        das_tsc_ctdef.access_column_ids_,
                                                        das_tsc_ctdef.pd_expr_spec_.pd_storage_flag_,
                                                        &tsc_out_cols,
                                                        false /*force_mysql_mode*/,
                                                        query_cs_replica))) {
    LOG_WARN("fail to convert schema", K(ret), KP(index_schema));
  } else if (OB_UNLIKELY(pd_agg && 0 == das_tsc_ctdef.aggregate_column_ids_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument", K(ret), K(pd_agg), K(das_tsc_ctdef.aggregate_column_ids_.count()));
  } else if (pd_agg &&
             OB_FAIL(das_tsc_ctdef.table_param_.convert_group_by(*index_schema,
                                                                 das_tsc_ctdef.access_column_ids_,
                                                                 das_tsc_ctdef.aggregate_column_ids_,
                                                                 das_tsc_ctdef.group_by_column_ids_,
                                                                 das_tsc_ctdef.pd_expr_spec_.pd_storage_flag_))) {
    LOG_WARN("convert group by failed", K(ret), KP(index_schema),
             K(das_tsc_ctdef.aggregate_column_ids_), K(das_tsc_ctdef.group_by_column_ids_));
  } else if (OB_FAIL(generate_das_result_output(das_tsc_ctdef, tsc_out_cols))) {
    LOG_WARN("fail to generate das result outpur", K(ret), K(tsc_out_cols));
  }

  return ret;
}

int ObTableTscCgService::generate_das_tsc_ctdef(const ObTableCtx &ctx,
                                                ObIAllocator &allocator,
                                                ObDASScanCtDef &das_tsc_ctdef,
                                                const bool query_cs_replica /*=false*/)
{
  int ret = OB_SUCCESS;
  das_tsc_ctdef.is_get_ = ctx.is_get();
  if (OB_FAIL(generate_access_ctdef(ctx, allocator, das_tsc_ctdef))) { // init access_column_ids_,pd_expr_spec_.access_exprs_
    LOG_WARN("fail to generate asccess ctdef", K(ret));
  } else if (OB_FAIL(generate_pushdown_aggr_ctdef(ctx, das_tsc_ctdef))) {
    LOG_WARN("fail to generate pushdown aggregate ctdef", K(ret));
  } else if (ctx.is_text_retrieval_scan() &&
             OB_FAIL(ObTableFtsTscCgService::generate_text_ir_pushdown_expr_ctdef(ctx, das_tsc_ctdef))) {
    LOG_WARN("failed to generate text ir pushdown aggregate ctdef", K(ret));
  } else if (OB_FAIL(generate_table_param(ctx, das_tsc_ctdef, query_cs_replica))) { // init table_param_, result_output_
    LOG_WARN("fail to generate table param", K(ret));
  }

  return ret;
}

int ObTableTscCgService::generate_output_exprs(const ObTableCtx &ctx,
                                               ObIArray<ObExpr *> &output_exprs)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const ObIArray<ObTableColumnItem> &items = ctx.get_column_items();
  const ObIArray<ObColumnRefRawExpr *> &select_exprs = ctx.get_select_exprs();

  for (int64_t i = 0; i < select_exprs.count() && OB_SUCC(ret); i++) {
    ObExpr *rt_expr = nullptr;
    ObColumnRefRawExpr *output_expr = select_exprs.at(i);
    ObRawExpr *raw_expr = output_expr;
    const ObTableColumnItem *item = nullptr;
    const ObTableColumnInfo *col_info = nullptr;
    if (OB_FAIL(ctx.get_column_item_by_expr(output_expr, item))) {
      LOG_WARN("fail to get column item", K(ret), KPC(output_expr));
    } else if (OB_ISNULL(item)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column item not found", K(ret), K(ctx));
    } else if (OB_ISNULL(col_info = item->column_info_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column info is NULL", K(ret), K(i));
    } else if (col_info->is_virtual_generated_column() && !col_info->is_doc_id_column()) { // output dependant expr when virtual expr
      raw_expr = item->expr_->get_dependant_expr();
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cg.generate_rt_expr(*raw_expr, rt_expr))) {
      LOG_WARN("fail to generate rt expr", K(ret));
    } else if (OB_FAIL(output_exprs.push_back(rt_expr))) {
      LOG_WARN("fail to push back rt expr", K(ret), K(output_exprs));
    }
  }

  return ret;
}

int ObTableTscCgService::generate_tsc_ctdef(const ObTableCtx &ctx,
                                            ObIAllocator &allocator,
                                            ObTableApiScanCtDef &tsc_ctdef)
{
  int ret = OB_SUCCESS;
  ObStaticEngineCG cg(ctx.get_cur_cluster_version());
  const int64_t filter_exprs_cnt = ctx.get_filter_exprs().count();
  bool query_cs_replica = false;
  ObDASBaseCtDef *root_ctdef = nullptr;
  ObDASDocIdMergeCtDef *doc_id_merge_ctdef = nullptr;
  bool need_attach = false;
  // init scan_ctdef_.ref_table_id_
  // when scan primary index, index_table_id == ref_table_id
  tsc_ctdef.scan_ctdef_.ref_table_id_ = ctx.get_index_table_id();
  if (ctx.is_text_retrieval_scan()) {
    tsc_ctdef.scan_ctdef_.ir_scan_type_ = ObTSCIRScanType::OB_IR_INV_IDX_SCAN;
  }
  if (OB_FAIL(tsc_ctdef.output_exprs_.init(ctx.get_select_exprs().count()))) {
    LOG_WARN("fail to init output exprs", K(ret));
  } else if (filter_exprs_cnt != 0 && OB_FAIL(tsc_ctdef.filter_exprs_.init(ctx.get_filter_exprs().count()))) {
    LOG_WARN("fail to init filter exprs", K(ret));
  } else if (OB_FAIL(generate_output_exprs(ctx, tsc_ctdef.output_exprs_))) {
    LOG_WARN("fail to generate output exprs", K(ret));
  } else if (OB_FAIL(cg.generate_rt_exprs(ctx.get_filter_exprs(), tsc_ctdef.filter_exprs_))) {
    LOG_WARN("fail to generate filter rt exprs ", K(ret));
  } else if (OB_FAIL(ctx.check_is_cs_replica_query(query_cs_replica))) {
    LOG_WARN("fail to check is cs replica query", K(ret));
  } else if (OB_FAIL(generate_das_tsc_ctdef(ctx, allocator, tsc_ctdef.scan_ctdef_, query_cs_replica))) { // init scan_ctdef_
    LOG_WARN("fail to generate das scan ctdef", K(ret));
  } else {
    root_ctdef = &tsc_ctdef.scan_ctdef_;
  }

  if (OB_SUCC(ret) && ctx.is_text_retrieval_scan()) {
    if (OB_FAIL(ObTableFtsTscCgService::generate_text_ir_ctdef(ctx, allocator, tsc_ctdef, root_ctdef))) {
      LOG_WARN("failed to generate text ir ctdef", K(ret));
    } else {
      need_attach = true;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (ctx.is_index_back()) {
    ObDASTableLookupCtDef *lookup_ctdef = nullptr;
    if (OB_FAIL(generate_table_lookup_ctdef(ctx, allocator, tsc_ctdef, root_ctdef, lookup_ctdef))) {
      LOG_WARN("generate table lookup ctdef failed", K(ret));
    } else {
      root_ctdef = lookup_ctdef;
    }
  } else if (ctx.is_tsc_with_doc_id()) {
    if (root_ctdef != &tsc_ctdef.scan_ctdef_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, root ctdef isn't equal to scan ctdef", K(ret));
    } else if (OB_FAIL(ObTableFtsTscCgService::generate_das_scan_ctdef_with_doc_id(allocator, ctx, tsc_ctdef,
          &tsc_ctdef.scan_ctdef_, doc_id_merge_ctdef))) {
      LOG_WARN("fail to generate das scan ctdef with doc id", K(ret));
    } else {
      root_ctdef = doc_id_merge_ctdef;
      need_attach = true;
    }
  }

  if (OB_SUCC(ret) && need_attach) {
    if (ctx.is_text_retrieval_scan()) {
      // lookup_ctdef is in attach_ctdef，to prevent create duplicate das task
      tsc_ctdef.lookup_ctdef_ = nullptr;
      tsc_ctdef.lookup_loc_meta_ = nullptr;
    }
    tsc_ctdef.attach_spec_.attach_ctdef_ = root_ctdef;
  }

  return ret;
}

int ObTableTscCgService::generate_table_lookup_ctdef(const ObTableCtx &ctx,
                                                     ObIAllocator &allocator,
                                                     ObTableApiScanCtDef &tsc_ctdef,
                                                     ObDASBaseCtDef *scan_ctdef,
                                                     ObDASTableLookupCtDef *&lookup_ctdef)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tsc_ctdef.lookup_loc_meta_ = OB_NEWx(ObDASTableLocMeta, &allocator, allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate lookup location meta buffer failed", K(ret));
  } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_SCAN,
                                                       allocator,
                                                       tsc_ctdef.lookup_ctdef_))) {
    LOG_WARN("alloc das ctdef failed", K(ret));
  } else {
    ObStaticEngineCG cg(ctx.get_cur_cluster_version());
    tsc_ctdef.lookup_ctdef_->ref_table_id_ = ctx.get_ref_table_id();
    ObTableIndexInfo primary_index_info = ctx.get_table_index_info().at(0);
    if (!primary_index_info.is_primary_index_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("primary index info is not primary index", K(ret), K(primary_index_info));
    } else if (OB_ISNULL(primary_index_info.index_schema_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table schema is NULL", K(ret));
    } else if (OB_FAIL(generate_das_tsc_ctdef(ctx, allocator, *tsc_ctdef.lookup_ctdef_))) {
      LOG_WARN("fail to generate das lookup scan ctdef", K(ret));
    } else if (OB_FAIL(ObTableLocCgService::generate_table_loc_meta(ctx,
                                                                    *primary_index_info.index_schema_,
                                                                    *tsc_ctdef.lookup_loc_meta_,
                                                                    &primary_index_info.related_index_ids_))) {
      LOG_WARN("fail to generate table loc meta", K(ret));
    }

    if (OB_SUCC(ret) && ctx.is_global_index_back()) {
      if (OB_ISNULL(primary_index_info.lookup_part_id_expr_) || ctx.get_rowkey_exprs().empty()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("calc_part_id_expr is null or rowkeys` count is zero", K(ret),
                  K(primary_index_info.lookup_part_id_expr_), K(ctx.get_rowkey_exprs().empty()));
      } else if (OB_FAIL(cg.generate_rt_expr(*primary_index_info.lookup_part_id_expr_,
                                              tsc_ctdef.calc_part_id_expr_))) {
        LOG_WARN("fail to generate calc part id expr", K(ret));
      } else if (OB_FAIL(tsc_ctdef.global_index_rowkey_exprs_.init(ctx.get_rowkey_exprs().count()))) {
        LOG_WARN("fail to init rowkey exprs", K(ret));
      } else if (OB_FAIL(cg.generate_rt_exprs(ctx.get_rowkey_exprs(),
                                              tsc_ctdef.global_index_rowkey_exprs_))) {
        LOG_WARN("fail to generate rowkey exprs", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      ObArray<ObRawExpr*> rowkey_exprs;
      if (OB_FAIL(rowkey_exprs.assign(ctx.get_rowkey_exprs()))) {
        LOG_WARN("failed to assign rowkey exprs", K(ret));
      } else if (OB_FAIL(cg.generate_rt_exprs(rowkey_exprs, tsc_ctdef.lookup_ctdef_->rowkey_exprs_))) {
        LOG_WARN("failed to generate main table rowkey exprs", K(ret));
      } else if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(DAS_OP_TABLE_LOOKUP, allocator, lookup_ctdef))) {
        LOG_WARN("alloc aux lookup ctdef failed", K(ret));
      } else if (OB_ISNULL(lookup_ctdef->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator, 2))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        lookup_ctdef->children_cnt_ = 2;
        if (OB_FAIL(tsc_ctdef.attach_spec_.attach_loc_metas_.push_back(tsc_ctdef.lookup_loc_meta_))) {
          LOG_WARN("store scan loc meta failed", K(ret));
        } else {
          lookup_ctdef->children_[0] = scan_ctdef;
          lookup_ctdef->children_[1] = static_cast<ObDASBaseCtDef *>(tsc_ctdef.lookup_ctdef_);
        }
      }
    }
  }
  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiInsertSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc insert ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableInsCtDef *ins_ctdef = nullptr;
    if (OB_ISNULL(ins_ctdef = OB_NEWx(ObTableInsCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_insert_ctdef(ctx, alloc, index_info, *ins_ctdef))) {
      LOG_WARN("fail to generate insert ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = ins_ctdef;
    }
  }
  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiUpdateSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc update ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableUpdCtDef *upd_ctdef = nullptr;
    if (OB_ISNULL(upd_ctdef = OB_NEWx(ObTableUpdCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_update_ctdef(ctx, alloc, index_info, *upd_ctdef))) {
      LOG_WARN("fail to generate update ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = upd_ctdef;
    }
  }
  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiDelSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc delete ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableDelCtDef *del_ctdef = nullptr;
    if (OB_ISNULL(del_ctdef = OB_NEWx(ObTableDelCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_delete_ctdef(ctx, alloc, index_info, *del_ctdef))) {
      LOG_WARN("fail to generate delete ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = del_ctdef;
    }
  }
  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiReplaceSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc replace ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableReplaceCtDef *replace_ctdef = nullptr;
    if (OB_ISNULL(replace_ctdef = OB_NEWx(ObTableReplaceCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_replace_ctdef(ctx, alloc, index_info, *replace_ctdef))) {
      LOG_WARN("fail to generate replace ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = replace_ctdef;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableDmlCgService::generate_conflict_checker_ctdef(ctx,
                                                                          alloc,
                                                                          dml_index_info.at(0),
                                                                          spec.get_conflict_checker_ctdef()))) {
    LOG_WARN("fail to generate conflict checker ctdef", K(ret));
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiInsertUpSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc insertup ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableInsUpdCtDef *insup_ctdef = nullptr;
    if (OB_ISNULL(insup_ctdef = OB_NEWx(ObTableInsUpdCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_insert_up_ctdef(ctx, alloc, index_info, *insup_ctdef))) {
      LOG_WARN("fail to generate insertup ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = insup_ctdef;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableDmlCgService::generate_conflict_checker_ctdef(ctx,
                                                                          alloc,
                                                                          dml_index_info.at(0),
                                                                          spec.get_conflict_checker_ctdef()))) {
    LOG_WARN("fail to generate conflict checker ctdef", K(ret));
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(common::ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiLockSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc lock ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableLockCtDef *lock_ctdef = nullptr;
    if (OB_ISNULL(lock_ctdef = OB_NEWx(ObTableLockCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_lock_ctdef(ctx, index_info, *lock_ctdef))) {
      LOG_WARN("fail to generate lock ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = lock_ctdef;
    }
  }

  return ret;
}

int ObTableSpecCgService::generate_spec(common::ObIAllocator &alloc,
                                        ObTableCtx &ctx,
                                        ObTableApiTTLSpec &spec)
{
  int ret = OB_SUCCESS;
  ObIArray<ObTableIndexInfo> &dml_index_info = ctx.get_table_index_info();
  if (dml_index_info.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dml index info is empty", K(ret));
  } else if (OB_FAIL(spec.init_ctdefs_array(dml_index_info.count()))) {
    LOG_WARN("fail to alloc ttl ctdefs array", K(ret));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < dml_index_info.count(); i++) {
    ObTableIndexInfo &index_info = dml_index_info.at(i);
    ObTableTTLCtDef *ttl_ctdef = nullptr;
    if (OB_ISNULL(ttl_ctdef = OB_NEWx(ObTableTTLCtDef, (&alloc), alloc))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc ctdef fail", K(ret));
    } else if (OB_FAIL(ObTableDmlCgService::generate_ttl_ctdef(ctx, alloc, index_info, *ttl_ctdef))) {
      LOG_WARN("fail to generate ttl ctdef", K(ret), K(i));
    } else {
      spec.get_ctdefs().at(i) = ttl_ctdef;
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTableDmlCgService::generate_conflict_checker_ctdef(ctx,
                                                                          alloc,
                                                                          dml_index_info.at(0),
                                                                          spec.get_conflict_checker_ctdef()))) {
    LOG_WARN("fail to generate conflict checker ctdef", K(ret));
  }

  return ret;
}

}  // namespace table
}  // namespace oceanbase

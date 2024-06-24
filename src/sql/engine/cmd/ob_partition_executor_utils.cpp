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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/cmd/ob_partition_executor_utils.h"
#include "share/object/ob_obj_cast.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_common_rpc_proxy.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/code_generator/ob_expr_generator_impl.h"
#include "sql/parser/ob_parser.h"
#include "sql/ob_sql_utils.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"

namespace oceanbase
{
using namespace common;
using namespace share::schema;
namespace sql
{


int ObPartitionExecutorUtils::calc_values_exprs(
    ObExecContext &ctx,
    ObCreateTableStmt &stmt) {
  int ret = OB_SUCCESS;
  ObArray<ObTableSchema *> table_schema_array;

  if (OB_FAIL(table_schema_array.push_back(&(stmt.get_create_table_arg().schema_)))) {
    LOG_WARN("fail to push back schema", KR(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < stmt.get_create_table_arg().mv_ainfo_.count(); i ++) {
    if (OB_FAIL(table_schema_array.push_back(&(stmt.get_create_table_arg().mv_ainfo_.at(i).container_table_schema_)))) {
      LOG_WARN("fail to push back schema", KR(ret));
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < table_schema_array.count(); i ++) {
    ObTableSchema &table_schema = *(table_schema_array.at(i));
    ObPartitionLevel level = table_schema.get_part_level();
    if (PARTITION_LEVEL_ONE == level) {
      if (OB_FAIL(calc_values_exprs(ctx, stmt::T_CREATE_TABLE, table_schema, stmt, false))) {
        LOG_WARN("fail to calc_values_exprs", K(ret));
      }
    } else if (PARTITION_LEVEL_TWO == level) {
      if (OB_FAIL(calc_values_exprs(ctx, stmt::T_CREATE_TABLE, table_schema, stmt, false))) {
        LOG_WARN("fail to calc_values_exprs", K(ret));
      } else if (OB_FAIL(calc_values_exprs(ctx, stmt::T_CREATE_TABLE, table_schema, stmt, true))) {
        LOG_WARN("fail to calc_values_exprs", K(ret));
      }
    }
    if (OB_SUCC(ret) && table_schema.is_partitioned_table()) {
      if (OB_FAIL(sort_list_paritition_if_need(table_schema))) {
        LOG_WARN("failed to sort list partition if need", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::calc_values_exprs_for_alter_table(ObExecContext &ctx,
                                                                ObTableSchema &table_schema,
                                                                ObPartitionedStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObPartitionLevel level = table_schema.get_part_level();
  if (PARTITION_LEVEL_ONE == level) {
    if (OB_FAIL(calc_values_exprs(ctx, stmt::T_ALTER_TABLE, table_schema, stmt, false))) {
      LOG_WARN("fail to calc_values_exprs", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == level) {
    if (OB_FAIL(calc_values_exprs(ctx, stmt::T_ALTER_TABLE, table_schema, stmt, false))) {
      LOG_WARN("fail to calc_values_exprs", K(ret));
    } else if (OB_FAIL(calc_values_exprs(ctx, stmt::T_ALTER_TABLE, table_schema, stmt, true))) {
      LOG_WARN("fail to calc_values_exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && table_schema.is_partitioned_table()) {
    if (OB_FAIL(sort_list_paritition_if_need(table_schema))) {
      LOG_WARN("failed to sort list partition if need", K(ret));
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::check_transition_interval_valid(const stmt::StmtType stmt_type,
                                                              ObExecContext &ctx,
                                                              ObRawExpr *transition_expr,
                                                              ObRawExpr *interval_expr)
{
  int ret = OB_SUCCESS;
  ParamStore dummy_params;
  ObObj temp_obj;
  ObRawExprFactory raw_expr_factory(ctx.get_allocator());

  CK (transition_expr != NULL);
  CK (interval_expr != NULL);
  CK (interval_expr->is_const_expr());
  OZ (ObSQLUtils::calc_simple_expr_without_row(ctx.get_my_session(),
                                   interval_expr, temp_obj, &dummy_params, ctx.get_allocator()));
  if (OB_SUCC(ret) && temp_obj.is_zero()) {
    ret = OB_ERR_INTERVAL_CANNOT_BE_ZERO;
    LOG_WARN("interval can not be zero"); 
  }
  if (OB_SUCC(ret) && ((transition_expr->get_data_type() == ObDateTimeType 
          ||transition_expr->get_data_type() == ObTimestampNanoType)
        && (interval_expr->get_data_type() == ObIntervalYMType))) {
    ObOpRawExpr *add_expr = NULL;
    ObRawExpr *tmp_expr = transition_expr;
    /* 对于年月的间隔，最多需要加12次，才能判断是否合法 */
    for (int i = 0; OB_SUCC(ret) && i < 12; i ++) {
      OX (add_expr = NULL);
      OZ (raw_expr_factory.create_raw_expr(T_OP_ADD, add_expr));
      if (NULL != add_expr) {
        OZ (add_expr->set_param_exprs(tmp_expr, interval_expr));
        OX (tmp_expr = add_expr);
      }
    }
    OZ (add_expr->formalize(ctx.get_my_session()));
    OZ (ObSQLUtils::calc_simple_expr_without_row(ctx.get_my_session(),
                                  add_expr, temp_obj, &dummy_params, ctx.get_allocator()));
    if (OB_ERR_DAY_OF_MONTH_RANGE == ret) {
      ret = OB_ERR_INVALID_INTERVAL_HIGH_BOUNDS;
      LOG_WARN("fail to calc value", K(ret), KPC(add_expr));
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::calc_values_exprs(ObExecContext &ctx,
                                                const stmt::StmtType stmt_type,
                                                ObTableSchema &table_schema,
                                                ObPartitionedStmt &stmt,
                                                bool is_subpart)
{
  int ret = OB_SUCCESS;
  const int64_t fun_expr_num = is_subpart ?
      stmt.get_subpart_fun_exprs().count() : stmt.get_part_fun_exprs().count();

  if (fun_expr_num <= 0) {
    // do nothing
  } else if (is_subpart) {
    if (table_schema.is_list_subpart()) {
      if (stmt.use_def_sub_part()) {
        if (OB_FAIL(set_list_part_rows(ctx, stmt, stmt_type, table_schema, stmt.get_subpart_fun_exprs(),
                                       stmt.get_template_subpart_values_exprs() ,is_subpart))) {
          LOG_WARN("failed to set list part rows", K(ret));
        }
      } else if (OB_FAIL(set_individual_list_part_rows(ctx, stmt, stmt_type, table_schema,
                                                       stmt.get_subpart_fun_exprs(),
                                                       stmt.get_individual_subpart_values_exprs()))) {
        LOG_WARN("failed to set individual list part rows", K(ret));
      }
    } else if (table_schema.is_range_subpart()) {
      if (stmt.use_def_sub_part()) {
        if (OB_FAIL(set_range_part_high_bound(ctx, stmt_type, table_schema, stmt, is_subpart))) {
          LOG_WARN("failed to set range part high bound", K(ret));
        } else if (OB_FAIL(check_increasing_range_value(table_schema.get_def_subpart_array(),
                                                        table_schema.get_def_sub_part_num(),
                                                        stmt_type))) {
          LOG_WARN("failed to check range value exprs", K(ret));
        }
      } else if (OB_FAIL(set_individual_range_part_high_bound(ctx, stmt_type, table_schema, stmt))) {
        LOG_WARN("failed to set individual range part high bound", K(ret));
      }
    }
  } else {
    if (table_schema.is_list_part()) {
      if (OB_FAIL(set_list_part_rows(ctx, stmt, stmt_type, table_schema, stmt.get_part_fun_exprs(),
                                     stmt.get_part_values_exprs(), is_subpart))) {
        LOG_WARN("failed to set list part rows", K(ret));
      }
    } else if (table_schema.is_range_part()) {
      if (OB_FAIL(set_range_part_high_bound(ctx, stmt_type, table_schema, stmt, is_subpart))) {
        LOG_WARN("failed to set range part high bound", K(ret));
      } else if (OB_FAIL(check_increasing_range_value(table_schema.get_part_array(),
                                                      table_schema.get_first_part_num(),
                                                      stmt_type))) {
        LOG_WARN("failed to check range value exprs", K(ret));
      } else if (stmt.get_interval_expr() != NULL) {
        int64_t part_num = stmt.get_part_values_exprs().count();
        CK (part_num >= 1);
        OZ (check_transition_interval_valid(stmt_type,
                                            ctx, 
                                            stmt.get_part_values_exprs().at(part_num - 1), 
                                            stmt.get_interval_expr()));
        OZ (set_interval_value(ctx, stmt_type, table_schema, stmt.get_interval_expr()));
      }
    } 
  }
  return ret;
}

int ObPartitionExecutorUtils::calc_values_exprs(ObExecContext &ctx,
                                                ObCreateIndexStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTableSchema &index_schema = stmt.get_create_index_arg().index_schema_;
  ObPartitionLevel level = index_schema.get_part_level();
  if (PARTITION_LEVEL_ONE == level) {
    if (OB_FAIL(calc_values_exprs(ctx, stmt::T_CREATE_INDEX, index_schema, stmt, false))) {
      LOG_WARN("fail to calc values exprs", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == level) {
    if (OB_FAIL(calc_values_exprs(ctx, stmt::T_CREATE_INDEX, index_schema, stmt, false))) {
      LOG_WARN("fail to calc values exprs", K(ret));
    } else if (OB_FAIL(calc_values_exprs(ctx, stmt::T_CREATE_INDEX, index_schema, stmt, true))) {
      LOG_WARN("fail to calc values exprs", K(ret));
    }
  }
  if (OB_SUCC(ret) && index_schema.is_partitioned_table()) {
    if (OB_FAIL(sort_list_paritition_if_need(index_schema))) {
      LOG_WARN("failed to sort list partition if need", K(ret));
    }
  }
  return ret;
}

template<class T>
int check_list_value_duplicate(T **partition_array,
                               int64_t part_count,
                               const ObNewRow &row,
                               bool &is_dup) {
  int ret = OB_SUCCESS;
  is_dup = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < part_count; i ++) {
    const common::ObIArray<common::ObNewRow> &list_values_exprs = partition_array[i]->get_list_row_values();
    for (int64_t j = 0; OB_SUCC(ret) && j < list_values_exprs.count(); j ++) {
      const ObNewRow &tmp_row = list_values_exprs.at(j);
      if (tmp_row.get_count() != row.get_count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("object count not equal should no be here", K(tmp_row), K(row), K(j));
      }
      for (int64_t z = 0; OB_SUCC(ret) && z < tmp_row.get_count(); z++) {
        if (!tmp_row.get_cell(z).is_null() && !row.get_cell(z).is_null() &&
            !ObSQLUtils::is_same_type_for_compare(tmp_row.get_cell(z).get_meta(),
                                                  row.get_cell(z).get_meta())) {
          ret = OB_ERR_PARTITION_VALUE_ERROR;
          LOG_WARN("partiton value should have same meta info", K(ret), K(tmp_row), K(row), K(j));
        }
      }
      if (OB_SUCC(ret) && tmp_row == row) {
        is_dup = true;
        break;
      }
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::cast_list_expr_to_obj(
    ObExecContext &ctx,
    const stmt::StmtType stmt_type,
    const bool is_subpart,
    const int64_t real_part_num,
    ObPartition **partition_array,
    ObSubPartition **subpartition_array,
    ObIArray<ObRawExpr *> &list_fun_expr,
    ObIArray<ObRawExpr *> &list_values_exprs)
{
  int ret = OB_SUCCESS;

  int32_t default_count = 0;
  ObRawExpr *row_expr = NULL;
  ObRawExpr *value_expr = NULL;
  ObPartition *part_info = NULL;
  ObSubPartition *subpart_info = NULL;
  if (OB_UNLIKELY(list_values_exprs.count() != real_part_num)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected list values exprs count", K(ret), K(real_part_num),
        K(list_values_exprs.count()));
  }

  // 检查default值的合法性
  for (int64_t i = 0; OB_SUCC(ret) && i < list_values_exprs.count(); ++i) {
    if (OB_ISNULL(row_expr = list_values_exprs.at(i)) ||
        OB_UNLIKELY(T_OP_ROW != row_expr->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(row_expr));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < row_expr->get_param_count(); ++j) {
      if (OB_ISNULL(value_expr = row_expr->get_param_expr(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(value_expr));
      } else if (ObMaxType == value_expr->get_data_type()) {
        ++default_count;
        if (i != list_values_exprs.count() - 1) {
          ret = OB_ERR_DEFAULT_NOT_AT_LAST_IN_LIST_PART;
        } else if (row_expr->get_param_count() != 1) {
          ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
        } else if (default_count > 1) {
          ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
        }
      }
    }
  }

  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> list_partition_obj;
  ObSEArray<ObRawExpr*, OB_DEFAULT_ARRAY_SIZE> list_value_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < list_values_exprs.count(); ++i) {
    list_partition_obj.reset();
    list_value_exprs.reset();
    row_expr = list_values_exprs.at(i);
    if (OB_ISNULL(row_expr = list_values_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(row_expr));
    } else if ((is_subpart && OB_ISNULL(subpart_info = subpartition_array[i])) ||
               (!is_subpart && OB_ISNULL(part_info = partition_array[i]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected part or subpart", K(ret), K(part_info), K(subpart_info));
    } else if (row_expr->get_param_count() == 1 && ObMaxType == row_expr->get_param_expr(0)->get_data_type()) {
      // default
      ObNewRow row;
      common::ObIAllocator &allocator = ctx.get_allocator();
      ObObj* obj_array = (ObObj *)allocator.alloc(sizeof(ObObj));
      obj_array[0].set_max_value();
      row.assign(obj_array, 1);
      if (is_subpart) {
        if (OB_FAIL(subpart_info->add_list_row(row))) {
          LOG_WARN("deep_copy_str fail", K(ret));
        }
      } else if (OB_FAIL(part_info->add_list_row(row))) {
        LOG_WARN("deep_copy_str fail", K(ret));
      }
    } else {
      if (OB_FAIL(row_expr_to_array(row_expr, list_value_exprs))) {
        LOG_WARN("failed to push row expr to array", K(ret));
      } else if (OB_FAIL(cast_expr_to_obj(ctx, stmt_type, true, list_fun_expr,
                                          list_value_exprs, list_partition_obj))) {
        LOG_WARN("fail to cast_expr_to_obj", K(ret));
      } else {
        int64_t fun_expr_num = list_fun_expr.count();
        int64_t element_pair_count = list_partition_obj.count() / list_fun_expr.count();
        ObIAllocator &allocator = ctx.get_allocator();
        for (int64_t j = 0; OB_SUCC(ret) && j < element_pair_count; ++j) {
          ObNewRow row;
          ObObj* obj_array = (ObObj *)allocator.alloc(fun_expr_num * sizeof(ObObj));
          for (int64_t k = 0; OB_SUCC(ret) && k < fun_expr_num; ++k) {
            new (obj_array + k) ObObj();
            obj_array[k] = (&list_partition_obj.at(j * fun_expr_num))[k];
          }
          if (OB_SUCC(ret)) {
            bool is_dup = true;
            row.assign(obj_array, fun_expr_num);
            if (is_subpart) {
              if (OB_FAIL(check_list_value_duplicate(subpartition_array, i + 1, row, is_dup))) {
                LOG_WARN("fail to check list value duplicate", K(ret));
              } else if (is_dup) {
                ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
              } else if (OB_FAIL(subpart_info->add_list_row(row))) {
                LOG_WARN("deep_copy_str fail", K(ret));
              }
            } else {
              if (OB_FAIL(check_list_value_duplicate(partition_array, i + 1, row, is_dup))) {
                LOG_WARN("fail to check list value duplicate", K(ret));
              } else if (is_dup) {
                ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
              } else if (OB_FAIL(part_info->add_list_row(row))) {
                LOG_WARN("deep_copy_str fail", K(ret));
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          auto &list_row_values = is_subpart
                                  ? subpartition_array[i]->list_row_values_
                                  : partition_array[i]->list_row_values_;
          InnerPartListVectorCmp part_list_vector_op;
          lib::ob_sort(list_row_values.begin(),  list_row_values.end(), part_list_vector_op);
          if (OB_FAIL(part_list_vector_op.get_ret())) {
            LOG_WARN("fail to sort list row values", K(ret));
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t array_count = list_values_exprs.count();
    // 这里排序是为了对list分区的顺序做规范化，保证属于同一个table group的list分区能够以相同的顺序迭代出来
    // 不在这里排序一级list分区是因为对于非模板化二级分区，排序后会导致partition array与
    // individual_subpart_values_exprs对应不上。
    if (is_subpart) {
      lib::ob_sort(subpartition_array,
                subpartition_array + array_count,
                ObBasePartition::list_part_func_layout);
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::cast_expr_to_obj(ObExecContext &ctx,
                                               const stmt::StmtType stmt_type,
                                               bool is_list_part,
                                               ObIArray<ObRawExpr *> &partition_fun_expr,
                                               ObIArray<ObRawExpr *> &partition_value_exprs,
                                               ObIArray<ObObj> &partition_value_objs)
{
  int ret = OB_SUCCESS;

  int64_t fun_expr_num = partition_fun_expr.count();
  int64_t values_exprs_num = partition_value_exprs.count();
  if (fun_expr_num <= 0) {
    // do nothing
  } else if (values_exprs_num <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpect partition value exprs", K(ret), K(values_exprs_num));
  } else {
    ObRawExpr *fun_expr = NULL;
    ObRawExpr *expr = NULL;
    const int64_t partition_num = values_exprs_num / fun_expr_num;
    //calc range values and set high_bound_value rowkey
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
      for (int64_t j = 0;  OB_SUCC(ret) && j < fun_expr_num; j++) {
        ObObj value_obj;
        if (OB_ISNULL(fun_expr = partition_fun_expr.at(j)) ||
            OB_ISNULL(expr = partition_value_exprs.at(i * fun_expr_num + j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret), K(fun_expr), K(expr));
        } else if (ObMaxType == expr->get_data_type()) {
          value_obj = ObObj::make_max_obj();
        } else {
          ObObjType fun_expr_type = fun_expr->get_data_type();
          LOG_DEBUG("will cast_expr_to_obj", K(fun_expr_type), KPC(fun_expr), KPC(expr),
                    K(partition_fun_expr), K(partition_value_exprs));
          if ((stmt::T_CREATE_TABLE == stmt_type || stmt::T_ALTER_TABLE == stmt_type) &&
              OB_FAIL(expr_cal_and_cast_with_check_varchar_len(stmt_type,
                                                               is_list_part,
                                                               ctx,
                                                               fun_expr->get_result_type(),
                                                               expr,
                                                               value_obj))) {
            LOG_WARN("failed to expr cal and cast with check varchar len", K(ret));
          } else if (stmt::T_CREATE_INDEX == stmt_type &&
                     OB_FAIL(expr_cal_and_cast(stmt_type,
                                               is_list_part,
                                               ctx,
                                               fun_expr->get_result_type(),
                                               fun_expr->get_collation_type(),
                                               expr,
                                               value_obj))) {
            LOG_WARN("expr cal and cast fail", K(ret));
          } else if ((!value_obj.is_null() || !is_list_part) &&
                     value_obj.get_collation_type() != fun_expr->get_collation_type()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("value_obj's collation type is not the same with fun_expr", K(ret),
                      K(value_obj), K(*fun_expr));
          }
        } //end of else
        if (OB_SUCC(ret)) {
          if (OB_FAIL(partition_value_objs.push_back(value_obj))) {
            LOG_WARN("array push back fail", K(ret));
          }
        }
      } //end of for j
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::set_range_part_high_bound(ObExecContext &ctx,
                                                        const stmt::StmtType stmt_type,
                                                        ObTableSchema &table_schema,
                                                        ObPartitionedStmt &stmt,
                                                        bool is_subpart)
{
  int ret = OB_SUCCESS;
  ObPartition **partition_array = table_schema.get_part_array();
  ObSubPartition **subpartition_array = table_schema.get_def_subpart_array();
  ObPartition *part_info = NULL;
  ObSubPartition *subpart_info = NULL;
  ObDDLStmt::array_t &range_fun_exprs = is_subpart ?
      stmt.get_subpart_fun_exprs() : stmt.get_part_fun_exprs();
  ObDDLStmt::array_t &range_values_exprs = is_subpart ?
      stmt.get_template_subpart_values_exprs() : stmt.get_part_values_exprs();
  const int64_t part_num = is_subpart ?
      table_schema.get_def_sub_part_num() : table_schema.get_first_part_num();
  const int64_t fun_expr_num = range_fun_exprs.count();
  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> range_partition_obj;
  if (is_subpart && OB_UNLIKELY(!stmt.use_def_sub_part())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart shoud be template", K(ret));
  } else if (is_subpart && (OB_ISNULL(subpartition_array))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpartition_array is NULL", K(ret));
  } else if (!is_subpart && OB_ISNULL(partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_array is NULL", K(ret));
  } else if (OB_FAIL(cast_expr_to_obj(ctx, stmt_type, false /*is_list_part*/, range_fun_exprs,
                                      range_values_exprs, range_partition_obj))) {
    LOG_WARN("fail to cast_expr_to_obj", K(ret));
  } else if (part_num * fun_expr_num != range_partition_obj.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid partition num", K(part_num), K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
      ObRowkey high_rowkey(&range_partition_obj.at(i * fun_expr_num), fun_expr_num);
      if (is_subpart) {
        if (OB_ISNULL(subpart_info = subpartition_array[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("subpart_info is null", K(ret));
        } else if (OB_FAIL(subpart_info->set_high_bound_val(high_rowkey))) {
          LOG_WARN("deep_copy_str fail", K(ret));
        }
      } else if (OB_ISNULL(part_info= partition_array[i])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part_info is null", K(ret));
      } else if (OB_FAIL(part_info->set_high_bound_val(high_rowkey))) {
        LOG_WARN("deep_copy_str fail", K(ret));
      }
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::set_interval_value(ObExecContext &ctx,
                                                 const stmt::StmtType stmt_type,
                                                 ObTableSchema &table_schema,
                                                 ObRawExpr *interval_expr)
{
  ObObj value_obj;
  int ret = OB_SUCCESS;
  if (OB_FAIL(expr_cal_and_cast_with_check_varchar_len(stmt_type, false /*is_list_part*/, 
                                                       ctx,
                                                       interval_expr->get_result_type(),
                                                       interval_expr,
                                                       value_obj))) {
    LOG_WARN("fail to cast_expr_to_obj", K(ret));
  } else {
    ObRowkey interval_rowkey;
    interval_rowkey.assign(&value_obj, 1);
    
    OX (table_schema.get_part_option().set_part_func_type(PARTITION_FUNC_TYPE_INTERVAL));
    OZ (table_schema.set_interval_range(interval_rowkey));
  }
  return ret;
}

int ObPartitionExecutorUtils::calc_values_exprs(
    ObExecContext &ctx,
    ObCreateTablegroupStmt &stmt) {
  int ret = OB_SUCCESS;
  ObTablegroupSchema &tablegroup_schema = stmt.get_create_tablegroup_arg().tablegroup_schema_;
  ObPartitionLevel level = tablegroup_schema.get_part_level();
  if (PARTITION_LEVEL_ONE == level) {
    if (OB_FAIL(calc_values_exprs(ctx, stmt, false))) {
      LOG_WARN("fail to calc_values_exprs", K(ret));
    }
  } else if (PARTITION_LEVEL_TWO == level) {
    if (OB_FAIL(calc_values_exprs(ctx, stmt, false))) {
      LOG_WARN("fail to calc_values_exprs", K(ret));
    } else if (OB_FAIL(calc_values_exprs(ctx, stmt, true))) {
      LOG_WARN("fail to calc_values_exprs", K(ret));
    }
  }
  return ret;
}

//FIXME:支持非模版化二级分区
int ObPartitionExecutorUtils::calc_values_exprs(
    ObExecContext &ctx,
    ObCreateTablegroupStmt &stmt,
    bool is_subpart)
{
  int ret = OB_SUCCESS;
  int64_t fun_expr_num = is_subpart ?
                         stmt.get_sub_part_func_expr_num() :
                         stmt.get_part_func_expr_num();

  ObTablegroupSchema &tablegroup_schema = stmt.get_create_tablegroup_arg().tablegroup_schema_;

  if (fun_expr_num > 0) {
    if ((!is_subpart && tablegroup_schema.is_list_part()) ||
      (is_subpart && tablegroup_schema.is_list_subpart())) {
      if (OB_FAIL(cast_list_expr_to_obj(ctx, stmt, is_subpart))) {
        LOG_WARN("cast expr to obj fail", K(ret));
      }
    } else if ((!is_subpart && tablegroup_schema.is_range_part()) ||
      (is_subpart && tablegroup_schema.is_range_subpart())) {
      ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> range_partition_obj;
      if (OB_FAIL(cast_range_expr_to_obj(ctx, stmt, is_subpart, range_partition_obj))) {
        LOG_WARN("cast expr to obj fail", K(ret));
      } else {
        int64_t part_num = range_partition_obj.count() / fun_expr_num;
        ObPartition **part_array = tablegroup_schema.get_part_array();
        ObSubPartition **subpart_array = tablegroup_schema.get_def_subpart_array();
        if (is_subpart && tablegroup_schema.is_range_subpart()) {
          ret = check_increasing_range_value(subpart_array, part_num, stmt::T_CREATE_TABLE);
        } else if (!is_subpart && tablegroup_schema.is_range_part()) {
          ret = check_increasing_range_value(part_array, part_num, stmt::T_CREATE_TABLE);
        }
        if (OB_FAIL(ret)) {
          LOG_WARN("failed to check range value exprs", K(ret));
        }
      }
    }
  }
  return ret;
}

template<typename T>
int ObPartitionExecutorUtils::check_increasing_range_value(T **array,
                                                           int64_t part_num,
                                                           const stmt::StmtType stmt_type)
{
  int ret = OB_SUCCESS;
  const ObRowkey *rowkey_last = NULL;
  T* last_part = NULL;
  T* cur_part = NULL;
  if (OB_ISNULL(array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Empty partition", K(ret));
  } else if (OB_ISNULL(array[0])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Empty partition", K(ret));
  } else {
    rowkey_last = &array[0]->get_high_bound_val();
    last_part = array[0];
  }
  for (int64_t i = 1; OB_SUCC(ret) && i < part_num; ++i) {
    const ObRowkey *rowkey_cur = NULL;
    if (OB_ISNULL(cur_part = array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Empty partition", K(i), K(ret));
    } else {
      bool is_increasing = true;
      bool need_check_maxvalue = false;
      rowkey_cur = &array[i]->get_high_bound_val();
      if (rowkey_cur->get_obj_cnt() != rowkey_last->get_obj_cnt()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid object count", K(*rowkey_cur), K(*rowkey_last), K(ret));
      } else {
        for (int64_t j = 0; j < rowkey_cur->get_obj_cnt() && OB_SUCC(ret); j++) {
          if (!ObSQLUtils::is_same_type_for_compare(rowkey_cur->get_obj_ptr()[j].get_meta(),
                                                    rowkey_last->get_obj_ptr()[j].get_meta())
              && !rowkey_cur->get_obj_ptr()[j].is_max_value()
              && !rowkey_last->get_obj_ptr()[j].is_max_value()) {
            ret = OB_ERR_PARTITION_VALUE_ERROR;
            LOG_WARN("partiton value should have same meta info", K(ret), K(*rowkey_cur), K(*rowkey_last), K(j));
          } else if (rowkey_cur->get_obj_ptr()[j].is_max_value() &&
                     rowkey_last->get_obj_ptr()[j].is_max_value()) {
            need_check_maxvalue = true;
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (*rowkey_cur <= *rowkey_last) {
          is_increasing = false;
        } else {
          if (OB_UNLIKELY(need_check_maxvalue)) {
            for (int64_t j = 0; j < rowkey_cur->get_obj_cnt() && is_increasing && OB_SUCC(ret); j++) {
              if (rowkey_cur->get_obj_ptr()[j].is_max_value() &&
                  rowkey_last->get_obj_ptr()[j].is_max_value()) {
                is_increasing = false;
              } else if (0 != rowkey_cur->get_obj_ptr()[j].check_collation_free_and_compare(rowkey_last->get_obj_ptr()[j])) {
                //  p0 (1, maxvalue), p1 (2, maxvalue) is allowed
                //  p0 (1, maxvalue), p1 (1, maxvalue) is not allowed
                //  p0 (maxvalue, 1), p1 (maxvalue, 2) is not allowed
                break;
              }
            }
          }
          if (is_increasing) {
            rowkey_last = rowkey_cur;
            last_part = cur_part;
          }
        }
      }
      if (OB_SUCC(ret) && !is_increasing) {
        if (stmt::T_ALTER_TABLE == stmt_type) {
          ret = OB_ERR_ADD_PART_BOUN_NOT_INC;
          LOG_WARN("Range values should be increasing", K(*rowkey_cur), K(*rowkey_last), K(ret));
        } else {
          ret = OB_ERR_RANGE_NOT_INCREASING_ERROR;
          LOG_WARN("Range values should be increasing", K(*rowkey_cur), K(*rowkey_last), K(ret));
          const ObString &err_msg = last_part->get_part_name();
          LOG_USER_ERROR(OB_ERR_RANGE_NOT_INCREASING_ERROR,
                        lib::is_oracle_mode() ? err_msg.length() : 0, err_msg.ptr());
        }
      }
    }
  }
  return ret;
}


int ObPartitionExecutorUtils::expr_cal_and_cast(
    const stmt::StmtType &stmt_type,
    bool is_list_part,
    ObExecContext &ctx,
    const sql::ObExprResType &dst_res_type,
    const ObCollationType fun_collation_type,
    ObRawExpr *expr,
    ObObj &value_obj)
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObObj temp_obj;
  ObObjType fun_expr_type = dst_res_type.get_type();
  if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type, ctx, ctx.get_allocator(), expr_ctx))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else {
    //CREATE TABLE t1 (a date) PARTITION BY RANGE (TO_DAYS(a)) (PARTITION p311 VALUES LESS THAN (TO_DAYS('abc')))
    //TO_DAYS('abc')跟mysql兼容，不论session中设置的cast_mode是什么，这里都需要为WARN_ON_FAIL
    //因为abc是无效参数，让to_days返回NULL
    expr_ctx.cast_mode_ = CM_WARN_ON_FAIL; //always set to WARN_ON_FAIL to allow calculate
    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    ObNewRow tmp_row;
    RowDesc row_desc;
    ObTempExpr *temp_expr = NULL;
    CK(OB_NOT_NULL(ctx.get_sql_ctx()));
    OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(expr,
       row_desc, ctx.get_allocator(), ctx.get_my_session(),
       ctx.get_sql_ctx()->schema_guard_, temp_expr));
    CK(OB_NOT_NULL(temp_expr));
    OZ(temp_expr->eval(ctx, tmp_row, temp_obj));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (temp_obj.is_null()) {
      if (!is_list_part) {
        ret = OB_EER_NULL_IN_VALUES_LESS_THAN;
        LOG_WARN("Not allowed to use NULL value in VALUES LESS THAN", K(ret));
      }
    } else {
      if (ob_is_uint_tc(fun_expr_type)) {
        //create table t1 (a bigint unsigned) partition by range (a) (partition p0 values less than (-1));
        //如果range表达式是unsigned, 那么这里需要判断value的值是否是uint
        if (ob_is_int_tc(temp_obj.get_type()) && temp_obj.get_int() < 0) {
          ret = OB_ERR_PARTITION_CONST_DOMAIN_ERROR;
          LOG_WARN("Partition constant is out of partition function domain", K(ret),
                   K(fun_expr_type), "value_type", temp_obj.get_type());
        }
      }
    }
    //将计算后的obj转换成partition 表达式的类型
    if (OB_SUCC(ret)) {
      ObObjType expected_obj_type = fun_expr_type;
      //对value表达式类型进行cast的时候，要进行提升
      //create table t1 (c1 tinyint) partition by range(c1) (partitions p0 values less than (2500));
      //虽然2500超过了tinyint的值域，但是mysql仍然允许建表成功
      if (ob_is_integer_type(fun_expr_type)) {
        if (ob_is_integer_type(temp_obj.get_type())) {
          //提升为int64或uint64
          expected_obj_type = ob_is_int_tc(fun_expr_type) ? ObIntType : ObUInt64Type;
        } else if (!temp_obj.is_null()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value expr should be integer type too",
                   "type", temp_obj.get_type(), K(ret));
        }
      } else if (ObTimestampLTZType == fun_expr_type) {
        //need store value with time zone, compat for oracle
        expected_obj_type = ObTimestampTZType;
      }
      const ObObj *out_val_ptr = NULL;
      EXPR_DEFINE_CAST_CTX(expr_ctx, CM_NONE);
      //cast_ctx.dest_collation_ = temp_obj.get_collation_type();
      cast_ctx.dest_collation_ = fun_collation_type;
      ObAccuracy res_acc;
      if (ObDecimalIntType == expected_obj_type) {
        res_acc = dst_res_type.get_accuracy();
        cast_ctx.res_accuracy_ = &res_acc;
        if (is_list_part) {
          cast_ctx.cast_mode_ |= CM_COLUMN_CONVERT;
        }
      }
      EXPR_CAST_OBJ_V2(expected_obj_type, temp_obj, out_val_ptr);
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(out_val_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to cast obj, but out_val_ptr is NULL", K(ret),
                   K(expected_obj_type), K(fun_expr_type), K(temp_obj));
        } else {
          value_obj = *out_val_ptr;
        }
      } else if (OB_ERR_UNEXPECTED != ret) {
        ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
      } else { }//do nothing
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::expr_cal_and_cast_with_check_varchar_len(
    const stmt::StmtType &stmt_type,
    bool is_list_part,
    ObExecContext &ctx,
    const ObExprResType &dst_res_type,
    ObRawExpr *expr,
    ObObj &value_obj)
{
  int ret = OB_SUCCESS;
  ObExprCtx expr_ctx;
  ObObj temp_obj;
  const ObObjType fun_expr_type = dst_res_type.get_type();
  const ObCollationType fun_collation_type = dst_res_type.get_collation_type();
  if (OB_FAIL(ObSQLUtils::wrap_expr_ctx(stmt_type, ctx, ctx.get_allocator(), expr_ctx))) {
    LOG_WARN("Failed to wrap expr ctx", K(ret));
  } else {
    //CREATE TABLE t1 (a date) PARTITION BY RANGE (TO_DAYS(a)) (PARTITION p311 VALUES LESS THAN (TO_DAYS('abc')))
    //TO_DAYS('abc')跟mysql兼容，不论session中设置的cast_mode是什么，这里都需要为WARN_ON_FAIL
    //因为abc是无效参数，让to_days返回NULL
    expr_ctx.cast_mode_ = CM_WARN_ON_FAIL; //always set to WARN_ON_FAIL to allow calculate
    EXPR_SET_CAST_CTX_MODE(expr_ctx);
    ObNewRow tmp_row;
    RowDesc row_desc;
    ObTempExpr *temp_expr = NULL;
    CK(OB_NOT_NULL(ctx.get_sql_ctx()));
    OZ(ObStaticEngineExprCG::gen_expr_with_row_desc(expr,
       row_desc, ctx.get_allocator(), ctx.get_my_session(),
       ctx.get_sql_ctx()->schema_guard_, temp_expr));
    CK(OB_NOT_NULL(temp_expr));
    if (OB_SUCC(ret)
        && NULL != temp_expr->rt_exprs_.at(temp_expr->expr_idx_).args_
        && NULL != temp_expr->rt_exprs_.at(temp_expr->expr_idx_).args_[0]) {
      temp_expr->rt_exprs_.at(temp_expr->expr_idx_).args_[0]->extra_ |= CM_WARN_ON_FAIL;
    }
    OZ(temp_expr->eval(ctx, tmp_row, temp_obj));
    if (OB_FAIL(ret)) {
    } else if (temp_obj.is_null()) {
      if (!is_list_part) {
        ret = OB_EER_NULL_IN_VALUES_LESS_THAN;
        LOG_WARN("Not allowed to use NULL value in VALUES LESS THAN", K(ret));
      }
    } else {
      if (ob_is_uint_tc(fun_expr_type)) {
        //create table t1 (a bigint unsigned) partition by range (a) (partition p0 values less than (-1));
        //如果range表达式是unsigned, 那么这里需要判断value的值是否是uint
        if (ob_is_int_tc(temp_obj.get_type()) && temp_obj.get_int() < 0) {
          ret = OB_ERR_PARTITION_CONST_DOMAIN_ERROR;
          LOG_WARN("Partition constant is out of partition function domain", K(ret),
                   K(fun_expr_type), "value_type", temp_obj.get_type());
        }
      }
    }
    //将计算后的obj转换成partition 表达式的类型
    if (OB_SUCC(ret)) {
      ObObjType expected_obj_type = fun_expr_type;
      //对value表达式类型进行cast的时候，要进行提升
      //create table t1 (c1 tinyint) partition by range(c1) (partitions p0 values less than (2500));
      //虽然2500超过了tinyint的值域，但是mysql仍然允许建表成功
      if (ob_is_integer_type(fun_expr_type)) {
        if (ob_is_integer_type(temp_obj.get_type())) {
          //提升为int64或uint64
          expected_obj_type = ob_is_int_tc(fun_expr_type) ? ObIntType : ObUInt64Type;
        } else if (!temp_obj.is_null()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("value expr should be integer type too",
                   "type", temp_obj.get_type(), K(ret));
        }
      } else if (ObTimestampLTZType == fun_expr_type) {
        //need store value with time zone, compat for oracle
        expected_obj_type = ObTimestampTZType;
      }
      const ObObj *out_val_ptr = NULL;
      ObCastMode cm = CM_NONE;
      ObAccuracy res_acc;
      EXPR_DEFINE_CAST_CTX(expr_ctx, cm);
      if (ObDecimalIntType == expected_obj_type) {
        // set res accuracy
        res_acc = dst_res_type.get_accuracy();
        cast_ctx.res_accuracy_ = &res_acc;
        if (is_list_part) {
          cast_ctx.cast_mode_ |= CM_COLUMN_CONVERT;
        } else if (!lib::is_oracle_mode()) {
          // range columns in mysql mode
          cast_ctx.cast_mode_ |= CM_COLUMN_CONVERT;
        }
      }
      //cast_ctx.dest_collation_ = temp_obj.get_collation_type();
      cast_ctx.dest_collation_ = fun_collation_type;
      EXPR_CAST_OBJ_V2(expected_obj_type, temp_obj, out_val_ptr);
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(out_val_ptr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("succ to cast obj, but out_val_ptr is NULL", K(ret),
                   K(expected_obj_type), K(fun_expr_type), K(temp_obj));
        } else if (!lib::is_oracle_mode() && !is_list_part && ObDecimalIntType == expected_obj_type) {
          // range columns in mysql mode
          cast_ctx.cast_mode_ &= ~CM_WARN_ON_FAIL;
          if (OB_FAIL(common::obj_accuracy_check(cast_ctx, dst_res_type.get_accuracy(), fun_collation_type, tmp_out_obj, tmp_out_obj, out_val_ptr))) {
            LOG_WARN("obj_accuracy_check", K(ret));
            if (ret == OB_ERR_DATA_TOO_LONG) {
               ret = OB_ERR_DATA_TOO_LONG_IN_PART_CHECK;
            }
          } else {
            value_obj = *out_val_ptr;
          }
        } else {
          if (lib::is_oracle_mode() && OB_FAIL(common::obj_accuracy_check(cast_ctx, dst_res_type.get_accuracy(), fun_collation_type, tmp_out_obj, tmp_out_obj, out_val_ptr))) {
            LOG_WARN("obj_accuracy_check", K(ret));
            if (ret == OB_ERR_DATA_TOO_LONG) {
               ret = OB_ERR_DATA_TOO_LONG_IN_PART_CHECK;
            }
          } else {
            value_obj = *out_val_ptr;
          }
        }
      } else if (OB_ERR_UNEXPECTED != ret) {
        ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
      } else { }//do nothing
    }
  }
  return ret;
}

// FIXME: 支持非模版化二级分区
int ObPartitionExecutorUtils::cast_range_expr_to_obj(
    ObExecContext &ctx,
    ObCreateTablegroupStmt &stmt,
    bool is_subpart,
    ObIArray<ObObj> &range_partition_obj)
{
  int ret = OB_SUCCESS;

  ObTablegroupSchema &tablegroup_schema = stmt.get_create_tablegroup_arg().tablegroup_schema_;
  ObPartition **partition_array = tablegroup_schema.get_part_array();
  ObSubPartition **subpartition_array = tablegroup_schema.get_def_subpart_array();
  ObIArray<ObRawExpr *> &range_values_exprs = is_subpart ?
    stmt.get_template_subpart_values_exprs() : stmt.get_part_values_exprs();
  if (is_subpart) {
    if (OB_ISNULL(subpartition_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition_array is NULL", K(ret));
    }
  } else {
    if (OB_ISNULL(partition_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_array is NULL", K(ret));
    }
  }
  const int64_t fun_expr_num = !is_subpart ?
                               stmt.get_part_func_expr_num() :
                               stmt.get_sub_part_func_expr_num();
  if (OB_FAIL(ret)) {
    //skip
  } else if (OB_FAIL(ObPartitionExecutorUtils::cast_range_expr_to_obj(
                               ctx,
                               range_values_exprs,
                               fun_expr_num,
                               stmt::T_CREATE_TABLEGROUP,
                               is_subpart,
                               is_subpart ? tablegroup_schema.get_def_sub_part_num() : tablegroup_schema.get_first_part_num(),
                               partition_array,
                               subpartition_array,
                               range_partition_obj))) {
    LOG_WARN("partition_array is NULL", K(ret));
  }
  return ret;
}

int ObPartitionExecutorUtils::cast_range_expr_to_obj(
    ObExecContext &ctx,
    ObIArray<ObRawExpr *> &range_values_exprs,
    const int64_t fun_expr_num,
    const stmt::StmtType stmt_type,
    const bool is_subpart,
    const int64_t real_part_num,
    ObPartition **partition_array,
    ObSubPartition **subpartition_array,
    ObIArray<ObObj> &range_partition_obj)
{
  int ret = OB_SUCCESS;

  int64_t range_values_exprs_num = range_values_exprs.count();
  if (fun_expr_num <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid fun_expr_num", K(ret), K(fun_expr_num));
  } else if (range_values_exprs_num <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no range partition expr", K(ret));
  } else {
    int64_t partition_num = range_values_exprs_num / fun_expr_num;
    if (partition_num != real_part_num) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition num", K(partition_num), K(real_part_num), K(ret));
    } else {
      ObRawExpr *expr = NULL;
      ObPartition *part_info = NULL;
      ObSubPartition *subpart_info = NULL;
      //calc range values and set high_bound_value rowkey
      //记录各column第一个非maxvalue的column type
      ObArray<ObObjType> fun_expr_type_array;
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        for (int64_t j = 0;  OB_SUCC(ret) && j < fun_expr_num; j++) {
          expr = range_values_exprs.at(i * fun_expr_num + j);
          ObObjType fun_expr_type = expr->get_data_type();
          // 对于tablegroup分区语法而言，由于缺乏column type信息，需要校验同列的value的类型是否一致
          if (fun_expr_type_array.count() < j + 1) {
            if (OB_FAIL(fun_expr_type_array.push_back(fun_expr_type))) {
              LOG_WARN("array push back fail", K(ret), K(j), "count", fun_expr_type_array.count());
            }
          } else if (fun_expr_type_array.at(j) == ObMaxType) {
            fun_expr_type_array.at(j) = fun_expr_type;
          } else if (fun_expr_type != ObMaxType && fun_expr_type_array.at(j) != fun_expr_type) {
            ret = OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR;
            LOG_USER_ERROR(OB_ERR_WRONG_TYPE_COLUMN_VALUE_ERROR);
            LOG_WARN("object type is invalid ", K(ret), K(fun_expr_type), "pre_fun_expr_type", fun_expr_type_array.at(j));
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (ObMaxType == expr->get_data_type()) {
            ObObj value_obj = ObObj::make_max_obj();
            if (OB_FAIL(range_partition_obj.push_back(value_obj))) {
              LOG_WARN("array push back fail", K(ret));
            } else {
              if (fun_expr_type_array.count() < j + 1) {
                if (OB_FAIL(fun_expr_type_array.push_back(ObMaxType))) {
                  LOG_WARN("array push back fail", K(ret), K(j), "count", fun_expr_type_array.count());
                }
              }
            }
          } else {
            ObObj value_obj;
            if (OB_FAIL(ObPartitionExecutorUtils::expr_cal_and_cast(
                  stmt_type, false, ctx, expr->get_result_type(), expr->get_collation_type(), expr,
                  value_obj))) {
              LOG_WARN("expr cal and cast fail", K(ret));
            } else if (OB_FAIL(range_partition_obj.push_back(value_obj))) {
              LOG_WARN("array push back fail", K(ret));
            } else {} // do nothing
          }
        } //end of for j
        if (OB_SUCC(ret)) {
          if (range_partition_obj.count() != (i+1) * fun_expr_num) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("Range partition obj count error",
                     K(i), "count", range_partition_obj.count(), K(fun_expr_num), K(ret));
          } else {
            ObRowkey high_rowkey(&range_partition_obj.at(i * fun_expr_num), fun_expr_num);
            if (is_subpart) {
              subpart_info = subpartition_array[i];
              if (OB_ISNULL(subpart_info)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("subpart_info is null", K(ret));
              } else if (OB_FAIL(subpart_info->set_high_bound_val(high_rowkey))) {
                LOG_WARN("deep_copy_str fail", K(ret));
              } else { }
            } else {
              part_info = partition_array[i];
              if (OB_ISNULL(part_info)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("part_info is null", K(ret));
              } else if (OB_FAIL(part_info->set_high_bound_val(high_rowkey))) {
                LOG_WARN("deep_copy_str fail", K(ret));
              } else { }
            }
          }
        }
      }
    }
  }
  return ret;
}

//FIXME:支持非模版化二级分区
int ObPartitionExecutorUtils::cast_list_expr_to_obj(
    ObExecContext &ctx,
    ObCreateTablegroupStmt &stmt,
    bool is_subpart)
{
  int ret = OB_SUCCESS;
  ObTablegroupSchema &tablegroup_schema = stmt.get_create_tablegroup_arg().tablegroup_schema_;
  ObPartition **partition_array = tablegroup_schema.get_part_array();
  ObSubPartition **subpartition_array = tablegroup_schema.get_def_subpart_array();
  if (is_subpart) {
    if (OB_ISNULL(subpartition_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("subpartition_array is NULL", K(ret));
    }
  } else {
    if (OB_ISNULL(partition_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("partition_array is NULL", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObPartitionExecutorUtils::cast_list_expr_to_obj(
                                 ctx,
                                 stmt,
                                 is_subpart,
                                 partition_array,
                                 subpartition_array))) {
      LOG_WARN("partition_array is NULL", K(ret));
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::cast_list_expr_to_obj(
    ObExecContext &ctx,
    ObTablegroupStmt &stmt,
    const bool is_subpart,
    ObPartition **partition_array,
    ObSubPartition **subpartition_array)
{
  int ret = OB_SUCCESS;

  int64_t fun_expr_num = is_subpart ?
                         stmt.get_sub_part_func_expr_num() :
                         stmt.get_part_func_expr_num();

  ObDDLStmt::array_t &list_values_exprs = is_subpart ?
    stmt.get_template_subpart_values_exprs() : stmt.get_part_values_exprs();

  int32_t default_count = 0;
  ObRawExpr *row_expr = NULL;
  ObRawExpr *value_expr = NULL;
  ObPartition *part_info = NULL;
  ObSubPartition *subpart_info = NULL;
  for (int64_t i = 0; OB_SUCC(ret) && i < list_values_exprs.count(); i ++) {
    if (OB_ISNULL(row_expr = list_values_exprs.at(i)) ||
        OB_UNLIKELY(T_OP_ROW != row_expr->get_expr_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(row_expr));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < row_expr->get_param_count(); ++j) {
      if (OB_ISNULL(value_expr = row_expr->get_param_expr(j))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected expr", K(ret), K(value_expr));
      } else if (ObMaxType == value_expr->get_data_type()) {
        ++default_count;
        if (row_expr->get_param_count() != 1) {
          ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
        } else if (default_count > 1) {
          ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
        }
      }
    }
  }

  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> list_partition_obj;
  ObSEArray<ObRawExpr*, OB_DEFAULT_ARRAY_SIZE> list_value_exprs;
  for (int64_t i = 0; OB_SUCC(ret) && i < list_values_exprs.count(); i ++) {
    list_partition_obj.reset();
    list_value_exprs.reset();
    if (OB_ISNULL(row_expr = list_values_exprs.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected expr", K(ret), K(row_expr));
    } else if ((is_subpart && OB_ISNULL(subpart_info = subpartition_array[i])) ||
               (!is_subpart && OB_ISNULL(part_info = partition_array[i]))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected part or subpart", K(ret), K(part_info), K(subpart_info));
    } else if (row_expr->get_param_count() == 1 && ObMaxType == row_expr->get_param_expr(0)->get_data_type()) {
      // default
      ObNewRow row;
      common::ObIAllocator &allocator = ctx.get_allocator();
      ObObj* obj_array = (ObObj *)allocator.alloc(sizeof(ObObj));
      obj_array[0].set_max_value();
      row.assign(obj_array, 1);
      if (is_subpart) {
        if (OB_FAIL(subpart_info->add_list_row(row))) {
          LOG_WARN("deep_copy_str fail", K(ret));
        }
      } else if (OB_FAIL(part_info->add_list_row(row))) {
        LOG_WARN("deep_copy_str fail", K(ret));
      }
    } else {
      if (OB_FAIL(row_expr_to_array(row_expr, list_value_exprs))) {
        LOG_WARN("failed to push row expr to array", K(ret));
      } else if (OB_FAIL(cast_expr_to_obj(ctx, fun_expr_num, list_value_exprs, list_partition_obj))) {
        LOG_WARN("fail to cast_expr_to_obj", K(ret));
      } else {
        int64_t element_pair_count = list_partition_obj.count() / fun_expr_num;
        common::ObIAllocator &allocator = ctx.get_allocator();
        for (int64_t j = 0; OB_SUCC(ret) && j < element_pair_count; j ++) {
          ObNewRow row;
          ObObj* obj_array = (ObObj *)allocator.alloc(fun_expr_num * sizeof(ObObj));
          for (int64_t k = 0; OB_SUCC(ret) && k < fun_expr_num; k ++) {
            new (obj_array + k) ObObj();
            obj_array[k] = (&list_partition_obj.at(j * fun_expr_num))[k];
          }
          if (OB_SUCC(ret)) {
            bool is_dup = true;
            row.assign(obj_array, fun_expr_num);
            if (is_subpart) {
              if (OB_FAIL(check_list_value_duplicate(subpartition_array, i + 1, row, is_dup))) {
                LOG_WARN("fail to check list value duplicate", K(ret));
              } else if (is_dup) {
                ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
              } else if (OB_FAIL(subpart_info->add_list_row(row))) {
                LOG_WARN("deep_copy_str fail", K(ret));
              }
            } else {
              if (OB_FAIL(check_list_value_duplicate(partition_array, i + 1, row, is_dup))) {
                LOG_WARN("fail to check list value duplicate", K(ret));
              } else if (is_dup) {
                ret = OB_ERR_MULTIPLE_DEF_CONST_IN_LIST_PART;
              } else if (OB_FAIL(part_info->add_list_row(row))) {
                LOG_WARN("deep_copy_str fail", K(ret));
              }
            }
          }
          if (OB_SUCC(ret)) {
            auto &list_row_values = is_subpart
                                    ? subpartition_array[i]->list_row_values_
                                    : partition_array[i]->list_row_values_;
            InnerPartListVectorCmp part_list_vector_op;
            lib::ob_sort(list_row_values.begin(), list_row_values.end(), part_list_vector_op);
            if (OB_FAIL(part_list_vector_op.get_ret())) {
              LOG_WARN("fail to sort list row values", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    const int64_t array_count = list_values_exprs.count();
    // TODO(yibo) tablegroup 支持二级分区异构后，一级分区的排序也要延迟处理
    if (is_subpart) {
      lib::ob_sort(subpartition_array,
                subpartition_array + array_count,
                ObBasePartition::list_part_func_layout);
    } else {
      lib::ob_sort(partition_array,
                partition_array + array_count,
                ObBasePartition::list_part_func_layout);
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::cast_expr_to_obj(
    ObExecContext &ctx,
    int64_t fun_expr_num,
    ObIArray<ObRawExpr *> &range_values_exprs,
    ObIArray<ObObj> &range_partition_obj)
{
  int ret = OB_SUCCESS;

  int64_t range_values_exprs_num = range_values_exprs.count();
  if (fun_expr_num > 0) {
    if (range_values_exprs_num <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no range partition expr", K(ret));
    } else {
      int64_t partition_num = range_values_exprs_num / fun_expr_num;
      ObRawExpr *expr = NULL;
      //calc range values and set high_bound_value rowkey
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_num; i++) {
        for (int64_t j = 0;  OB_SUCC(ret) && j < fun_expr_num; j++) {
          expr = range_values_exprs.at(i * fun_expr_num + j);
          if (OB_ISNULL(expr)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr is null", K(ret));
          } else if (ObMaxType == expr->get_data_type()) {
            ObObj value_obj = ObObj::make_max_obj();
            if (OB_FAIL(range_partition_obj.push_back(value_obj))) {
              LOG_WARN("array push back fail", K(ret));
            }
          } else {
            ObObjType fun_expr_type = expr->get_data_type();
            ObObj value_obj;
            const stmt::StmtType stmt_type = stmt::T_CREATE_TABLE;
            if (OB_FAIL(expr_cal_and_cast(
                    stmt_type, false, ctx, expr->get_result_type(),
                    expr->get_collation_type(), expr, value_obj))) {
              LOG_WARN("expr cal and cast fail", K(ret));
            } else if (OB_FAIL(range_partition_obj.push_back(value_obj))) {
              LOG_WARN("array push back fail", K(ret));
            } else { } //do nothing
          } //end of else
        } //end of for j
      }
    }
  }
  return ret;
}

// for first part and template subpart
int ObPartitionExecutorUtils::set_list_part_rows(ObExecContext &ctx,
                                                 ObPartitionedStmt &stmt,
                                                 const stmt::StmtType stmt_type,
                                                 ObTableSchema &table_schema,
                                                 ObIArray<ObRawExpr *> &list_fun_exprs,
                                                 ObIArray<ObRawExpr *> &list_values_exprs,
                                                 bool is_subpart)
{
  int ret = OB_SUCCESS;
  ObPartition **partition_array = table_schema.get_part_array();
  ObSubPartition **subpartition_array = table_schema.get_def_subpart_array();
  const int64_t part_num = is_subpart ?
      table_schema.get_def_sub_part_num() : table_schema.get_first_part_num();
  if (is_subpart && OB_UNLIKELY(!stmt.use_def_sub_part())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart shoud be template", K(ret));
  } else if (is_subpart && (OB_ISNULL(subpartition_array))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null subpartition_array", K(ret));
  } else if (!is_subpart && OB_ISNULL(partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null partition_array", K(ret));
  } else if (OB_FAIL(cast_list_expr_to_obj(ctx, stmt_type, is_subpart, part_num, partition_array,
                                           subpartition_array, list_fun_exprs, list_values_exprs))) {
    LOG_WARN("failed to cast list expr to obj", K(ret));
  }
  return ret;
}

// for individual range subpart
int ObPartitionExecutorUtils::set_individual_range_part_high_bound(ObExecContext &ctx,
                                                                   const stmt::StmtType stmt_type,
                                                                   ObTableSchema &table_schema,
                                                                   ObPartitionedStmt &stmt)
{
  int ret = OB_SUCCESS;
  ObPartition **partition_array = table_schema.get_part_array();
  ObSubPartition **subpartition_array = NULL;
  ObPartition *partition = NULL;
  ObSubPartition *subpartition = NULL;
  ObSEArray<ObObj, OB_DEFAULT_ARRAY_SIZE> range_partition_obj;

  const int64_t part_num = table_schema.get_part_option().get_part_num();
  ObIArray<ObRawExpr *> &range_fun_expr = stmt.get_subpart_fun_exprs();
  ObDDLStmt::array_array_t &range_values_exprs_array = stmt.get_individual_subpart_values_exprs();
  const int64_t fun_expr_num = range_fun_expr.count();

  if (OB_UNLIKELY(stmt.use_def_sub_part())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart is template", K(ret));
  } else if (OB_ISNULL(partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_array is NULL", K(ret));
  }
  for (int64_t i =0; OB_SUCC(ret) && i < part_num; ++i) {
    range_partition_obj.reset();
    if (OB_ISNULL(partition = partition_array[i]) ||
        OB_ISNULL(subpartition_array = partition->get_subpart_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(partition), K(subpartition_array));
    } else if (OB_FAIL(cast_expr_to_obj(ctx, stmt_type, false, range_fun_expr,
                                        range_values_exprs_array.at(i), range_partition_obj))) {
      LOG_WARN("fail to cast expr to obj", K(ret));
    } else if (partition->get_sub_part_num() * fun_expr_num != range_partition_obj.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition num", K(part_num), K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < partition->get_sub_part_num(); ++j) {
      ObRowkey high_rowkey(&range_partition_obj.at(j * fun_expr_num), fun_expr_num);
      if (OB_ISNULL(subpartition = subpartition_array[j])) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null subpartition", K(ret));
      } else if (OB_FAIL(subpartition->set_high_bound_val(high_rowkey))) {
        LOG_WARN("failed to set high bound val", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(check_increasing_range_value(subpartition_array,
                                               partition->get_sub_part_num(),
                                               stmt_type))) {
        LOG_WARN("failed to check range value exprs", K(ret));
      }
    }
  }
  return ret;
}

// for individual list subpart
int ObPartitionExecutorUtils::set_individual_list_part_rows(ObExecContext &ctx,
                                                            ObPartitionedStmt &stmt,
                                                            const stmt::StmtType stmt_type,
                                                            ObTableSchema &table_schema,
                                                            ObIArray<ObRawExpr *> &list_fun_exprs,
                                                            ObDDLStmt::array_array_t &list_values_exprs_array)
{
  int ret = OB_SUCCESS;
  ObPartition *partition = NULL;
  ObSubPartition *subpartition = NULL;
  ObPartition **partition_array = table_schema.get_part_array();
  const int64_t part_num = table_schema.get_first_part_num();

  if (OB_UNLIKELY(stmt.use_def_sub_part())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("subpart is template", K(ret));
  } else if (OB_ISNULL(partition_array)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition_array is NULL", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
    if (OB_ISNULL(partition = partition_array[i])) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret), K(partition), K(subpartition));
    } else if (OB_FAIL(cast_list_expr_to_obj(ctx, stmt_type, true, partition->get_sub_part_num(),
                                             partition_array, partition->get_subpart_array(),
                                             list_fun_exprs, list_values_exprs_array.at(i)))) {
      LOG_WARN("failed to cast list expr to obj", K(ret));
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::row_expr_to_array(ObRawExpr *row_expr,
                                                ObIArray<ObRawExpr *> &list_values_expr_array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row_expr) || OB_UNLIKELY(T_OP_ROW != row_expr->get_expr_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr", K(ret), K(row_expr));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_expr->get_param_count(); ++i) {
    if (OB_FAIL(list_values_expr_array.push_back(row_expr->get_param_expr(i)))) {
      LOG_WARN("failed to push back expr", K(ret));
    }
  }
  return ret;
}

int ObPartitionExecutorUtils::sort_list_paritition_if_need(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  if (table_schema.is_list_part()) {
    ObPartition **partition_array = table_schema.get_part_array();
    const int64_t array_count = table_schema.get_partition_num();
    if (OB_ISNULL(partition_array)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      lib::ob_sort(partition_array,
                partition_array + array_count,
                ObBasePartition::list_part_func_layout);
    }
  }
  return ret;
}

} //end namespace sql
} //end namespace oceanbase

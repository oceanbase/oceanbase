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

#include <iostream>
#include <getopt.h>
#include <gtest/gtest.h>
#include "../test_sql_utils.h"
#include "share/ob_define.h"
#include "ob_stdin_iter.h"
#include "string.h"
#define protected public
#define private public
#include "sql/engine/sort/ob_sort.h"
#include "sql/engine/basic/ob_material.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/time/ob_time_utility.h"
#include "lib/worker.h"
#include "sql/engine/join/ob_nested_loop_join.h"
#include "sql/engine/join/ob_merge_join.h"
#include "sql/engine/join/ob_hash_join.h"
#include "sql/engine/aggregate/ob_hash_groupby.h"
#include "sql/engine/aggregate/ob_merge_groupby.h"
#include "sql/engine/aggregate/ob_scalar_aggregate.h"
#include "sql/ob_sql_init.h"
#include "sql/session/ob_sql_session_info.h"
#include "unistd.h"
#include "common/row/ob_row_util.h"

using namespace std;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;
using namespace test;

char run_file_name[] = "sort.run";

ObArray<char *> schema_file_names;
ObArray<int64_t> table_sizes;
ObArray<int64_t> projector_counts;
ObArray<int64_t> seed_mins;
ObArray<int64_t> seed_maxs;
ObArray<int64_t> seed_steps;
ObArray<int64_t> seed_step_lengths;
ObArray<int64_t> order_columns;

int64_t limit_count = -1;
bool use_random = false;

char *op_type = NULL;
bool print_output = false;
int64_t sort_column_count = 1;
int64_t equal_cond_count = 1;
int64_t other_cond_count = 1;
bool run_as_ut = true;
bool experimental = false;
int64_t time_to_sleep = 0;
int64_t info_type = 0;
int64_t common_prefix_len = 10;

TestSqlUtils test_util;

#define STRTOL_ERR(val) ((errno == ERANGE && ((val) == LONG_MAX || (val) == LONG_MIN)))

#define FIRST_TABLE_ID 1099511677877
#define SECOND_TABLE_ID 1099511677878

int setup_env() {
  int ret = OB_SUCCESS;
  if (OB_FAIL(oceanbase::sql::init_sql_factories())) {
    SQL_OPT_LOG(ERROR, "failed to init sql factories");
  } else {
    test_util.init();
    for (int64_t i = 0; i < schema_file_names.count(); ++i) {
      test_util.load_schema_from_file(schema_file_names.at(i));
    }
  }
  return ret;
}


void print_obj(const ObObj &obj) {
  if (obj.get_type_class() == ObIntTC) {
    cout << obj.get_int() <<" | ";
  } else if (obj.get_type_class() == ObStringTC) {
    int64_t len = obj.get_string().length();
    char buf[len + 1];
    memset(buf, 0, len + 1);
    obj.get_string().to_string(buf, len + 1);
    cout << buf <<" | ";
  } else if (obj.get_type_class() == ObNumberTC) {
    cout << obj.get_number().format() <<" | ";
  } else if (obj.get_type_class() == ObDoubleTC) {
    cout << obj.get_double() <<" | ";
  } else if (obj.get_type_class() == ObUIntTC) {
    cout << obj.get_uint64() <<" | ";
  } else {
    cout << obj.get_int() <<" | ";
  }
}


//extern int64_t equal_cond_eval_count;
//extern int64_t equal_cond_eval_time;
//extern int64_t other_cond_eval_count;
//extern int64_t other_cond_eval_time;
//extern int64_t right_cache_push_count;
//extern int64_t right_cache_acc_count;
//extern int64_t match_group_count;
//
//extern int64_t equal_cond_pass_count;
//extern int64_t equal_cond_pass_time;
//extern int64_t equal_row_count;
//extern int64_t no_matched_right;
//extern int64_t no_matched_left;
//extern int64_t left_rows;
int join_test(int64_t type) {

  int ret = OB_SUCCESS;
  ObMalloc buf;
  ObStdinIter gens[2](buf);
  //ObStdinIter *gens = static_cast<ObStdinIter *>(buf.alloc(sizeof(ObStdinIter) * 2));
  ObExprOperatorFactory factory(buf);
  const ObTableSchema *schemas[2] = {NULL};

  if (OB_FAIL(test_util.get_schema_guard().get_table_schema(FIRST_TABLE_ID, schemas[0]))) {
    SQL_OPT_LOG(WARN, "failed to get table schema");
  } else if (OB_ISNULL(schemas[0])) {
    ret = OB_ERR_TABLE_EXIST;
  } else {
    if (OB_FAIL(test_util.get_schema_guard().get_table_schema(SECOND_TABLE_ID, schemas[1]))) {
      SQL_OPT_LOG(WARN, "failed to get table schema");
    } else if (OB_ISNULL(schemas[1])) {
      ret = OB_ERR_TABLE_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    if (use_random) {
      for (int64_t n = 0; n < schemas[0]->get_column_count(); ++n) {
        gens[0].set_random_column(n);
      }
      for (int64_t n = 0; n < schemas[1]->get_column_count(); ++n) {
        gens[1].set_random_column(n);
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < 2; ++i) {
      gens[i].set_pure_random(true);
      gens[i].set_eof_behavior(ObStdinIter::RANDOM);

      if (seed_mins.count() > i) {
        gens[i].seed_min = seed_mins.at(i);
      }
      if (seed_maxs.count() > i) {
        gens[i].seed_max = seed_maxs.at(i);
      }
      if (seed_steps.count() > i) {
        gens[i].seed_step = seed_steps.at(i);
      }
      if (seed_step_lengths.count() > i) {
        gens[i].seed_step_length = seed_step_lengths.at(i);
      }

      if (OB_FAIL(gens[i].init_schema(*schemas[i]))) {
        SQL_OPT_LOG(WARN, "failed to init row gen");
      } else {
        if (table_sizes.count() < 2) {
          ret = OB_NOT_INIT;
          SQL_OPT_LOG(WARN, "not enough param");
        } else {
          gens[i].set_need_row_count(table_sizes.at(i));
        }
      }
    }
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 1000*1000*1000);

    ObPhysicalPlan plan;
    plan.set_stmt_type(stmt::T_SELECT, false);

    int32_t *left_projector = NULL;
    int64_t left_projector_count = 0;
    int32_t *right_projector = NULL;
    int64_t right_projector_count = 0;
    if (projector_counts.count() >= 1) {
      left_projector_count = projector_counts.at(0);
      left_projector = static_cast<int32_t *>(buf.alloc(left_projector_count * sizeof(int32_t)));
      for (int32_t i = 0; i < left_projector_count; ++i) {
        left_projector[i] = i;
      }
    }
    if (projector_counts.count() >= 2) {
      right_projector_count = projector_counts.at(1);
      right_projector = static_cast<int32_t *>(buf.alloc(right_projector_count * sizeof(int32_t)));
      for (int32_t i = 0; i < right_projector_count; ++i) {
        right_projector[i] = i;
      }
    }

    MyMockOperator input_left;
    input_left.iter = &gens[0];
    input_left.set_id(1);
    input_left.set_column_count(gens[0].new_row_.count_);
    input_left.set_phy_plan(&plan);
    if (left_projector_count > 0)
      input_left.set_projector(left_projector, left_projector_count);

    MyMockOperator input_right;
    input_right.iter = &gens[1];
    input_right.set_id(2);
    input_right.set_column_count(gens[1].new_row_.count_);
    input_right.set_phy_plan(&plan);
    input_right.set_type(PHY_TABLE_SCAN);
    if (right_projector_count > 0)
      input_right.set_projector(right_projector, right_projector_count);


    ObSqlExpression *equal_conds[equal_cond_count];
    ObPostExprItem expr_item;
    ObExprOperator *op;
    factory.alloc(T_OP_EQ, op);
    op->set_real_param_num(2);
    for (int64_t i = 0; i < equal_cond_count; ++i) {
      equal_conds[i] = new ObSqlExpression(buf, 3);
      ObPostExprItem item_col_left;
      ObPostExprItem item_col_right;
      ObPostExprItem item_op;
      item_op.assign(op);
      item_col_left.set_column(i);
      item_col_right.set_column(i + schemas[1]->get_column_count());
      equal_conds[i]->add_expr_item(item_col_left);
      equal_conds[i]->add_expr_item(item_col_right);
      equal_conds[i]->add_expr_item(item_op);

     // cout << "EQ COND "<< i << endl;
    }

    ObExprOperator *leop;
   // ObExprOperator *addop;
    factory.alloc(T_OP_LE, leop);
   // factory.alloc(T_OP_ADD, addop);
    ObSqlExpression *other_conds[other_cond_count];
    int64_t rand_val;
    for (int64_t i = 0; i < other_cond_count; ++i) {
      other_conds[i] = new ObSqlExpression(buf, 3);
      ObPostExprItem item_col_left;
     // ObPostExprItem item_col_right;
      ObPostExprItem item_op_lt;
      ObPostExprItem item_op_int;
     // ObPostExprItem item_op_add;

      item_op_lt.assign(leop);
      item_col_left.set_column(i);
      //item_col_right.set_column(i + schemas[1]->get_column_count());
      //rand_val = table_sizes.at(0) > table_sizes.at(1) ? table_sizes.at(0) : table_sizes.at(1);
      //rand_val = (int64_t)((double)(rand_val * 5) * ((double)rand() / (double)RAND_MAX));
      rand_val = rand() % (table_sizes.at(0) > table_sizes.at(1) ? table_sizes.at(0) : table_sizes.at(1));
      item_op_int.set_int(rand_val);
      //item_op_add.assign(addop);

      other_conds[i]->add_expr_item(item_col_left);
     // other_conds[i]->add_expr_item(item_col_right);
     // other_conds[i]->add_expr_item(item_op_add);
      other_conds[i]->add_expr_item(item_op_int);
      other_conds[i]->add_expr_item(item_op_lt);
     // cout << "OTHER COND "<< i << endl;
    }


    ObJoin *pjoin = NULL;
    if (0 == type) {
      pjoin = static_cast<ObNestedLoopJoin *>(buf.alloc(sizeof(ObNestedLoopJoin)));
      new (pjoin) ObNestedLoopJoin(buf);
    } else if (1 == type) {
      pjoin = static_cast<ObMergeJoin *>(buf.alloc(sizeof(ObMergeJoin)));
      new (pjoin) ObMergeJoin(buf);
    } else if (2 == type) {
      pjoin = static_cast<ObHashJoin *>(buf.alloc(sizeof(ObHashJoin)));
      new (pjoin) ObHashJoin(buf);
    }

    pjoin->set_phy_plan(&plan);
    pjoin->set_id(0);
    pjoin->set_column_count((left_projector_count > 0 ? left_projector_count : input_left.get_column_count())
                        + (right_projector_count > 0 ? right_projector_count : input_right.get_column_count()));
    input_left.set_rows(table_sizes.at(0));
    input_right.set_rows(table_sizes.at(1));
    pjoin->set_child(0, input_left);
    pjoin->set_child(1, input_right);
    pjoin->set_join_type(FULL_OUTER_JOIN);

    for (int64_t i = 0; i < equal_cond_count; ++i) {
      ret = pjoin->add_equijoin_condition(equal_conds[i]);
    }

    for (int64_t i = 0; i < other_cond_count; ++i) {
      ret = pjoin->add_other_join_condition(other_conds[i]);
    }

    ObExecContext exec_ctx;
    ObSQLSessionInfo session;
   // exec_ctx.init(3);
    exec_ctx.set_my_session(&session);
    int64_t row_count = 0;
    int64_t join_begin = 0;
    if (session.init_system_variables(false, false)) {
      SQL_OPT_LOG(WARN, "failed init sys var", K(ret));
    } else if (OB_FAIL(exec_ctx.init_phy_op(3))) {
      SQL_OPT_LOG(WARN, "failed create phy plan", K(ret));
    } else if (exec_ctx.create_physical_plan_ctx()) {
      SQL_OPT_LOG(WARN, "failed create phy plan ctx", K(ret));
    } else if (OB_FAIL(pjoin->open(exec_ctx))) {
      SQL_OPT_LOG(WARN, "failed to open join");
    } else {
      join_begin = ObTimeUtility::current_time();
//      no_matched_right = 0;
//      no_matched_left = 0;
//      equal_row_count = 0;
      const ObNewRow *row = NULL;
      while (OB_SUCC(pjoin->get_next_row(exec_ctx, row))) {
        if (print_output) {
          cout << "Result Row "<< row_count <<" : " << "| ";
          for (int64_t i = 0; i < row->get_count(); i++) {
            cout << row->get_cell(i).get_int() <<" | ";
          }
          cout << endl;
        }
        ++row_count;
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
    }
    int64_t join_end = ObTimeUtility::current_time();

//2    cout << "row_count : " << row_count << endl
// 3        << "total_time : " << join_end - join_begin << endl
// 4        << "join_time : " <<join_end - join_begin  - input_left.time_ - input_right.time_ <<endl
// 5        << "join_time except conds : " <<join_end - join_begin  - input_left.time_ - input_right.time_ - equal_cond_eval_time - other_cond_eval_time<<endl
// 6        << "equal_eval : " << equal_cond_eval_count << endl
//  7       << "equal_pass : " << equal_cond_pass_count << endl
//  8       << "equal_pass_time : " <<  equal_cond_pass_time << endl
//  9       << "equal_time : " << equal_cond_eval_time << endl
//   10      << "other_eval : " << other_cond_eval_count << endl
//     11    << "other_time : " << other_cond_eval_time << endl
//       12  << "right_cache_put : " << right_cache_push_count << endl
//        13 << "right_cache_acc : " << right_cache_acc_count << endl
//        14 << "match_group_count : " << match_group_count <<endl;

//
//    cout << row_count << ","
//        << join_end - join_begin  - input_left.time_ - input_right.time_ - equal_cond_eval_time - other_cond_eval_time<<","
//        << equal_cond_eval_count << ","
//        << other_cond_eval_count << ","
//        << right_cache_push_count << ","
//        << right_cache_acc_count << ","
//        << match_group_count << endl;
    cout << row_count << ","
       // << no_matched_right << ", "<< no_matched_left << ", "
        << join_end - join_begin  - input_left.time_ - input_right.time_ << endl;
  }
  return ret;
}

int scan_op(ObPhyOperator &op, ObExecContext &exec_ctx, bool print, int64_t& row_count) {
  int ret = OB_SUCCESS;
  const ObNewRow *row = NULL;
  while (OB_SUCC(op.get_next_row(exec_ctx, row))) {
    if (print) {
      cout << "Result Row "<< row_count <<" : " << "| ";
      for (int64_t i = 0; i < row->get_count(); i++) {
        print_obj(row->get_cell(i));
      }
      cout << endl;
    }
    ++row_count;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int scan_sort(ObBaseSort &op, ObNewRow &row, bool print, int64_t& row_count) {
  int ret = OB_SUCCESS;
  while (OB_SUCC(op.get_next_row(row))) {
    if (print) {
      cout << "Result Row "<< row_count <<" : " << "| ";
      for (int64_t i = 0; i < row.get_count(); i++) {
        print_obj(row.get_cell(i));
      }
      cout << endl;
    }
    ++row_count;
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int group_by_test(int64_t type, int64_t &rc) {

  int ret = OB_SUCCESS;
  ObMalloc buf;
  rc = 0;
  ObStdinIter gen(buf);
  const ObTableSchema *schema = NULL;
  if (OB_FAIL(test_util.get_schema_guard().get_table_schema(FIRST_TABLE_ID, schema))) {
    SQL_OPT_LOG(WARN, "fail to get_table_schema");
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_TABLE_EXIST;
  }
  if (OB_SUCC(ret)) {
    gen.set_pure_random(true);
    gen.set_eof_behavior(ObStdinIter::RANDOM);
    if (seed_mins.count() > 0) {
      gen.seed_min = seed_mins.at(0);
    }
    if (seed_maxs.count() > 0) {
      gen.seed_max = seed_maxs.at(0);
    }
    if (seed_steps.count() > 0) {
      gen.seed_step = seed_steps.at(0);
    }
    if (seed_step_lengths.count() > 0) {
      gen.seed_step_length = seed_step_lengths.at(0);
    }
    if (OB_FAIL(gen.init_schema(*schema))) {
      SQL_OPT_LOG(WARN, "failed to init row gen");
    } else if (table_sizes.count() < 1) {
      SQL_OPT_LOG(WARN, "not enough param");
    } else {
      gen.set_need_row_count(table_sizes.at(0));
    }
  }



  if (OB_SUCC(ret)) {
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 1000*1000*1000);

    ObPhysicalPlan plan;
    plan.set_stmt_type(stmt::T_SELECT, false);

    int32_t *projector = NULL;
    int64_t projector_count = 0;
    if (projector_counts.count() >= 1) {
      projector_count = projector_counts.at(0);
      projector = static_cast<int32_t *>(buf.alloc(projector_count * sizeof(int32_t)));
      for (int32_t i = 0; i < projector_count; ++i) {
        projector[i] = i;
      }
    }

    MyMockOperator input;
    input.iter = &gen;
    input.set_id(2);
    input.set_column_count(gen.new_row_.count_);
    input.set_phy_plan(&plan);
    if (projector_count > 0) {
      input.set_projector(projector, projector_count);
    }

    ObMaterial material(plan.get_allocator());
    material.set_id(1);

    if (projector_counts.count() != 0) {
      material.set_column_count(projector_counts.at(0));
    } else {
      material.set_column_count(schema->get_column_count());
    }
    material.set_child(0, input);
    material.set_phy_plan(&plan);


    ObGroupBy *grp = NULL;
    ObMergeGroupBy merge_groupby(plan.get_allocator());
    ObHashGroupBy hash_groupby(plan.get_allocator());
    ObScalarAggregate scalar_groupby(plan.get_allocator());

    if (0 == type) {
      grp = &hash_groupby;
    } else if (1 == type) {
      grp = &merge_groupby;
    } else {
      grp = &scalar_groupby;
    }

    if (OB_SUCC(ret)) {
      if (0 == type || 1 == type) {
        if(OB_FAIL(grp->init(other_cond_count))) {
          SQL_OPT_LOG(WARN, "fail to init arr");
        }
      }
    }
     if (OB_SUCC(ret)) {
      grp->set_id(0);
      grp->set_phy_plan(&plan);
      grp->set_column_count((projector_count > 0 ? projector_count : gen.new_row_.count_) + equal_cond_count);
      grp->set_child(0, material);

      ObAggregateExpression *aggr_expr = NULL;
      ObPostExprItem expr_item;
      for (int64_t i = 0; OB_SUCC(ret) && i < equal_cond_count; ++i) {
        if (OB_FAIL(ObSqlExpressionUtil::make_sql_expr(&plan, aggr_expr))) {
          SQL_OPT_LOG(WARN, "fail to make expr");
        } else {
          if (OB_FAIL(expr_item.set_column(0))) {
          } else if (OB_FAIL(aggr_expr->set_item_count(1))) {
            SQL_OPT_LOG(WARN, "failed init aggr expr", K(ret));
          } else if (OB_FAIL(aggr_expr->add_expr_item(expr_item))) {
            SQL_OPT_LOG(WARN, "failed init aggr expr", K(ret));
          } else if (OB_FAIL(grp->add_aggr_column(aggr_expr))) {
            SQL_OPT_LOG(WARN, "failed init aggr expr", K(ret));
          } else {
            aggr_expr->set_result_index((projector_count > 0 ? projector_count : gen.new_row_.count_) + i);
            aggr_expr->set_aggr_func(T_FUN_AVG, false);
            aggr_expr->set_collation_type(CS_TYPE_UTF8MB4_BIN);
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (0 == type || 1 == type) {
          if (OB_FAIL(grp->add_group_column_idx(0, CS_TYPE_UTF8MB4_BIN))) {
            SQL_OPT_LOG(WARN, "failed add group col", K(ret));
          } else {
            for (int64_t i = 1; OB_SUCC(ret) && i < other_cond_count; ++i) {
              if (OB_FAIL(grp->add_group_column_idx(i, CS_TYPE_UTF8MB4_BIN))) {
                SQL_OPT_LOG(WARN, "failed add group col", K(ret));
              }
            }
          }
        }
      }


      ObExecContext exec_ctx;
      ObSQLSessionInfo session;

      if (OB_SUCC(ret)) {
        if (OB_FAIL(exec_ctx.init_phy_op(3))) {
        } else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {
          SQL_OPT_LOG(WARN, "failed create phy plan", K(ret));
        } else {
          exec_ctx.set_my_session(&session);
        }
      }


      ObObj group_concat_max_len;
      group_concat_max_len.set_uint64(123456789);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(session.init_system_variables(false, false))) {
          SQL_OPT_LOG(WARN, "failed init sys var", K(ret));
        } else if (OB_FAIL(session.update_sys_variable(SYS_VAR_GROUP_CONCAT_MAX_LEN, group_concat_max_len))) {
          SQL_OPT_LOG(WARN, "failed set sys var", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t material_begin = 0, material_end = 0, grp_begin = 0, grp_end = 0;
        //warm up
        if (OB_FAIL(grp->open(exec_ctx))) {
          SQL_OPT_LOG(WARN, "failed to open group by");
        } else {
          int64_t row_count = 1;
          if (OB_FAIL(scan_op(*grp, exec_ctx, false, row_count))) {
            SQL_OPT_LOG(WARN, "failed to scan group by");
          }
        }

        if (OB_SUCC(ret)) {
          //get material scan time
          material_begin  = ObTimeUtility::current_time();
          if (OB_FAIL(material.rescan(exec_ctx))) {
            SQL_OPT_LOG(WARN, "failed to rescan material");
          } else {
            int64_t row_count = 1;
            if (OB_FAIL(scan_op(material, exec_ctx, false, row_count))) {
              SQL_OPT_LOG(WARN, "failed to scan material");
            }
          }
          material_end  = ObTimeUtility::current_time();
        }
        //get grp scan time

        if (OB_SUCC(ret)) {
          grp_begin  = ObTimeUtility::current_time();
          if (OB_FAIL(grp->rescan(exec_ctx))) {
            SQL_OPT_LOG(WARN, "failed to open group by");
          } else {
            int64_t row_count = 1;
            if (OB_FAIL(scan_op(*grp, exec_ctx, print_output, row_count))) {
              SQL_OPT_LOG(WARN, "failed to scan group by");
            }
            row_count -= 1;
            grp_end  = ObTimeUtility::current_time();
            cout << row_count << ","
                << grp_end - grp_begin  - (material_end - material_begin) << endl;
            rc = row_count;
          }
        }
      }
    }
  }
  return ret;
}

//
//int material_test() {
//  int ret = OB_SUCCESS;
//  ObMalloc buf;
//  ObStdinIter *gen = static_cast<ObStdinIter *>(buf.alloc(sizeof(ObStdinIter)));
//  gen = new (static_cast<void *>(gen)) ObStdinIter(buf);
//  if (NULL == gen) {
//    ret = OB_ERROR;
//    SQL_OPT_LOG(WARN, "failed to alloc mem");
//  } else {
//
//    const ObTableSchema *schema = NULL;
//    if (OB_FAIL(test_util.get_schema_guard().get_table_schema(FIRST_TABLE_ID, schema))) {
//      SQL_OPT_LOG(WARN, "fail to get_table_schema");
//    } else if (NULL == schema) {
//      SQL_OPT_LOG(WARN, "failed to get schema");
//    } else {
//      //todo
//      for (int64_t n = 0; n < schema->get_column_count(); ++n) {
//        gen->set_random_column(n);
//      }
//      gen->set_pure_random(true);
//      gen->set_eof_behavior(ObStdinIter::RANDOM);
//
//      if (OB_FAIL(gen->init_schema(*schema))) {
//        SQL_OPT_LOG(WARN, "failed to init row gen");
//      } else {
//        //todo : seperate row count for each table
//        gen->set_need_row_count(table_sizes.at(0));
//
//        if (projector_counts.count() != 0)
//        {
//          int64_t projector_count = projector_counts.at(0);
//          gen->init_projector(projector_count);
//          for (int64_t i = 0; i < projector_count; ++i) {
//            gen->add_projector(i);
//          }
//        }
//
//
//      }
//    }
//
//    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 1000*1000*1000);
//
//    ObPhysicalPlan plan;
//    plan.set_stmt_type(stmt::T_SELECT, false);
//
//    MyMockOperator input_op;
//    input_op.iter = gen;
//    input_op.set_id(1);
//    input_op.set_phy_plan(&plan);
//
//    int32_t projectors[256] = {0};
//    if (projector_counts.count() != 0)
//    {
//      int64_t projector_count = projector_counts.at(0);
//      for (int32_t i = 0; i < projector_count; ++i) {
//        projectors[i] = i;
//      }
//      input_op.set_projector(projectors, projector_count);
//    }
//
//
//    ObMaterial material(plan.get_allocator());
//    material.set_id(0);
//
//    if (projector_counts.count() != 0) {
//      material.set_column_count(projector_counts.at(0));
//    } else {
//      material.set_column_count(schema->get_column_count());
//    }
//    material.set_child(0, input_op);
//    material.set_phy_plan(&plan);
//
//
//    ObExecContext exec_ctx;
//    ObPhysicalPlanCtx *plan_ctx = NULL;
//    ObSQLSessionInfo session;
//
//    exec_ctx.init_phy_op(2);
//    exec_ctx.create_physical_plan_ctx(plan_ctx);
//    exec_ctx.set_my_session(&session);
//
//    int64_t material_start = ObTimeUtility::current_time();
//    if (OB_FAIL(material.open(exec_ctx))) {
//      SQL_OPT_LOG(WARN, "failed to open material");
//    } else {
//      int64_t row_count = 0;
//      const ObNewRow *row = NULL;
//      while (OB_SUCC(material.get_next_row(exec_ctx, row))) {
//        if (print_output) {
//          cout << "Result Row "<< row_count <<" : " << "| ";
//          for (int64_t i = 0; i < row->get_count(); i++) {
//            if (row->get_cell(i).get_type_class() == ObIntTC)
//              cout << row->get_cell(i).get_int() <<" | ";
//            else
//              cout << row->get_cell(i).get_number().format() <<" | ";
//          }
//          cout << endl;
//        }
//        ++row_count;
//      }
//      if (OB_ITER_END == ret) {
//        ret = OB_SUCCESS;
//      }
//    }
//    int64_t material_end = ObTimeUtility::current_time();
//    cout << material_end - material_start  - input_op.time_ << endl;
//  }
//
//  return ret;
//}

int sort_test(int64_t &rc) {

  int ret = OB_SUCCESS;

  rc = 0;
  ObStdinIter gen(THIS_WORKER.get_allocator());
  const ObTableSchema *schema = NULL;
  if (OB_FAIL(test_util.get_schema_guard().get_table_schema(FIRST_TABLE_ID, schema))) {
    SQL_OPT_LOG(WARN, "fail to get_table_schema");
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_TABLE_EXIST;
  }
  if (OB_SUCC(ret)) {
//    if (use_random) {
//      for (int64_t n = 0; n < schema->get_column_count(); ++n) {
//        gen.set_random_column(n);
//      }
//    }
    gen.set_pure_random(true);
    gen.set_eof_behavior(ObStdinIter::RANDOM);
    if (seed_mins.count() > 0) {
      gen.seed_min = seed_mins.at(0);
    }
    if (seed_maxs.count() > 0) {
      gen.seed_max = seed_maxs.at(0);
    }
    if (seed_steps.count() > 0) {
      gen.seed_step = seed_steps.at(0);
    }
    if (seed_step_lengths.count() > 0) {
      gen.seed_step_length = seed_step_lengths.at(0);
    }
    if (OB_FAIL(gen.init_schema(*schema))) {
      SQL_OPT_LOG(WARN, "failed to init row gen");
    } else if (table_sizes.count() < 1) {
      SQL_OPT_LOG(WARN, "not enough param");
    } else {
      gen.set_need_row_count(table_sizes.at(0));
      gen.set_common_prefix_len(common_prefix_len);
    }
  }



  if (OB_SUCC(ret)) {
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 1000*1000*1000);

    ObPhysicalPlan plan(THIS_WORKER.get_allocator());
    plan.set_stmt_type(stmt::T_SELECT, false);


    int32_t *projector = NULL;
    int64_t projector_count = 0;
    if (projector_counts.count() >= 1) {
      projector_count = projector_counts.at(0);
      projector = static_cast<int32_t *>(THIS_WORKER.get_allocator().alloc(projector_count * sizeof(int32_t)));
      for (int32_t i = 0; i < projector_count; ++i) {
        projector[i] = i;
      }
    }

    MyMockOperator input;
    input.iter = &gen;
    input.set_id(2);
    input.set_column_count(gen.new_row_.count_);
    input.set_phy_plan(&plan);
    if (projector_count > 0) {
      input.set_projector(projector, projector_count);
    }

    ObMaterial material(plan.get_allocator());
    material.set_id(1);

    if (projector_counts.count() != 0) {
      material.set_column_count(projector_counts.at(0));
    } else {
      material.set_column_count(schema->get_column_count());
    }
    material.set_child(0, input);
    material.set_phy_plan(&plan);


    ObSort sort(plan.get_allocator());
    sort.set_id(0);
    //sort.set_mem_limit(10L * 1024 * 1024 * 1024); //ensure in-mem sort
    sort.set_run_filename(ObString::make_string(run_file_name));
    if (projector_counts.count() != 0) {
      sort.set_column_count(projector_counts.at(0));
    } else {
      sort.set_column_count(schema->get_column_count());
    }

    sort.set_child(0, material);
    sort.set_phy_plan(&plan);


    if (order_columns.count() > 0) {
      sort.init_sort_columns(order_columns.count());
      for (int64_t i = 0; i < order_columns.count(); ++i) {
        sort.add_sort_column(order_columns.at(i), CS_TYPE_UTF8MB4_BIN, true);
      }
    } else {
      sort.init_sort_columns(sort_column_count);
      for (int64_t i = 0; i < sort_column_count; ++i) {
        sort.add_sort_column(i, CS_TYPE_UTF8MB4_BIN, true);
      }
    }

    if (experimental) {
      sort.set_use_compact(false);
    } else {
      sort.set_use_compact(true);
    }

    ObSqlExpression limit_expr(plan.get_allocator());
    limit_expr.set_item_count(1);
    ObPostExprItem item;
    item.set_int(limit_count);
    limit_expr.add_expr_item(item);

    if (limit_count >= 0) {
      sort.set_topn_expr(&limit_expr);
    }


    if (OB_SUCC(ret)) {

      ObExecContext exec_ctx;
      ObSQLSessionInfo session;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(exec_ctx.init_phy_op(3))) {
        } else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {
          SQL_OPT_LOG(WARN, "failed create phy plan", K(ret));
        } else {
          exec_ctx.set_my_session(&session);
        }
      }


      ObObj group_concat_max_len;
      group_concat_max_len.set_uint64(123456789);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(session.init_system_variables(false, false))) {
          SQL_OPT_LOG(WARN, "failed init sys var", K(ret));
        } else if (OB_FAIL(session.update_sys_variable(SYS_VAR_GROUP_CONCAT_MAX_LEN, group_concat_max_len))) {
          SQL_OPT_LOG(WARN, "failed set sys var", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t material_begin = 0, material_end = 0, add_begin = 0, add_end = 0, sort_begin = 0, sort_end = 0, get_end = 0;
        //warm up

        if (OB_FAIL(sort.open(exec_ctx))) {
          SQL_OPT_LOG(WARN, "failed to open group by");
        } else {
          int64_t row_count = 1;
          if (OB_FAIL(scan_op(sort, exec_ctx, false, row_count))) {
            SQL_OPT_LOG(WARN, "failed to scan group by");
          }
        }

        if (OB_SUCC(ret)) {
          //get material scan time
          if (print_output)
            cout << "--- raw data ---" << endl;
          material_begin  = ObTimeUtility::current_time();
          if (OB_FAIL(material.rescan(exec_ctx))) {
            SQL_OPT_LOG(WARN, "failed to rescan material");
          } else {
            int64_t row_count = 1;
            if (OB_FAIL(scan_op(material, exec_ctx, print_output, row_count))) {
              SQL_OPT_LOG(WARN, "failed to scan material");
            }
          }
          material_end  = ObTimeUtility::current_time();
        }
        //get grp scan time

        if (OB_SUCC(ret)) {
          usleep(static_cast<int>(time_to_sleep));

          if (OB_FAIL(sort.rescan(exec_ctx))) {
            SQL_OPT_LOG(WARN, "failed to open group by");
          } else {

            if (5 == info_type) {
              int64_t row_count = 1;
              add_begin = ObTimeUtility::current_time();
              if (OB_FAIL(scan_op(sort, exec_ctx, print_output, row_count))) {
                SQL_OPT_LOG(WARN, "failed to scan group by");
              } else {
                get_end = ObTimeUtility::current_time();
                row_count --;
                cout << row_count << ","
                     << get_end - add_begin  - (material_end - material_begin) << endl;
              }

            } else {


              ObSort::ObSortCtx *sort_ctx = NULL;
              sort_ctx = GET_PHY_OPERATOR_CTX(ObSort::ObSortCtx, exec_ctx, 0);
              sort_ctx->is_first_ = false;


              const ObNewRow *row = NULL;


              sort_ctx->base_sort_.set_sort_columns(sort.get_sort_columns(), 0);

              add_begin = ObTimeUtility::current_time();
              bool need_sort = false;
              while (OB_SUCC(ret) && OB_SUCC(material.get_next_row(exec_ctx, row))) {
                sort_ctx->sort_row_count_++;
                if (OB_ISNULL(row)) {
                  ret = OB_BAD_NULL_ERROR;
                  SQL_OPT_LOG(WARN, "input row is null");
                } else if (OB_FAIL(sort_ctx->sort_iter_->add_row(*row, need_sort))) {
                  SQL_OPT_LOG(WARN, "failed to add row", K(ret));
                }
              } // end while
              if (OB_ITER_END == ret) {
                ret = OB_SUCCESS;
              }

              add_end = ObTimeUtility::current_time();
              int64_t row_count = 1;

              if (4 != info_type && OB_SUCC(ret)) {
                sort_begin  = ObTimeUtility::current_time();
                if (OB_FAIL(sort_ctx->base_sort_.sort_rows())) {
                  SQL_OPT_LOG(WARN, "failed to add row", K(ret));
                } else {
                  sort_end  = ObTimeUtility::current_time();



                  if (0 == info_type || 2 == info_type) {
                    if (print_output)
                      cout << "--- sorted data ---" << endl;
                    if (OB_FAIL(scan_sort(sort_ctx->base_sort_, sort_ctx->get_cur_row(), print_output, row_count))) {
                      SQL_OPT_LOG(WARN, "failed to scan sort");
                    }
                    get_end  = ObTimeUtility::current_time();
                  }
                }
              }

              if (OB_SUCC(ret)) {
                row_count -= 1;
                if (0 == info_type) {
                  cout << row_count << ","
                      << get_end - add_begin  - (material_end - material_begin) << endl;
                } else if (1 == info_type) {
                  cout << row_count << ","
                      << sort_end - sort_begin << endl;
                } else if (2 == info_type) {
                  cout << row_count << ","
                      << get_end - sort_end << endl;
                } else if (3 == info_type) {
                  cout << row_count << "," << sort.get_column_count() << "," << sort.get_sort_column_size() << ","
                      << sort_ctx->base_sort_.get_used_mem_size()  << endl;
                } else if (4 == info_type) {
                  cout << row_count << "," << add_end - add_begin - (material_end - material_begin) << endl;
                }
                rc = row_count;
              }
            }
          }
        }
      }
    }
  }

  return ret;
}


int ob_array_test() {
  int ret = OB_SUCCESS;

  ObArray<void *> arr;
  if (table_sizes.count() < 1) {
    ret = OB_INVALID_ARGUMENT;
    SQL_OPT_LOG(WARN, "not enough table_size");
  } else {
    int64_t rc = table_sizes.at(0);
    void *ptr = static_cast<void *>(0);
    int64_t begin = ObTimeUtility::current_time();
    for (int64_t i = 0; OB_SUCC(ret) && i < rc; ++i) {
      if (OB_FAIL(arr.push_back(ptr))) {
        SQL_OPT_LOG(WARN, "failed to push arr");
      }
    }
    int64_t end = ObTimeUtility::current_time();
    if (OB_SUCC(ret)) {
      cout << end - begin << endl;
    }
  }
  return ret;
}

int material_test(int64_t &rc) {

  int ret = OB_SUCCESS;

  rc = 0;
  ObStdinIter gen(THIS_WORKER.get_allocator());
  const ObTableSchema *schema = NULL;
  if (OB_FAIL(test_util.get_schema_guard().get_table_schema(FIRST_TABLE_ID, schema))) {
    SQL_OPT_LOG(WARN, "fail to get_table_schema");
  } else if (OB_ISNULL(schema)) {
    ret = OB_ERR_TABLE_EXIST;
  }
  if (OB_SUCC(ret)) {
    if (use_random) {
      for (int64_t n = 0; n < schema->get_column_count(); ++n) {
        gen.set_random_column(n);
      }
    }
    gen.set_pure_random(true);
    gen.set_eof_behavior(ObStdinIter::RANDOM);
    if (seed_mins.count() > 0) {
      gen.seed_min = seed_mins.at(0);
    }
    if (seed_maxs.count() > 0) {
      gen.seed_max = seed_maxs.at(0);
    }
    if (seed_steps.count() > 0) {
      gen.seed_step = seed_steps.at(0);
    }
    if (seed_step_lengths.count() > 0) {
      gen.seed_step_length = seed_step_lengths.at(0);
    }
    if (OB_FAIL(gen.init_schema(*schema))) {
      SQL_OPT_LOG(WARN, "failed to init row gen");
    } else if (table_sizes.count() < 1) {
      SQL_OPT_LOG(WARN, "not enough param");
    } else {
      gen.set_need_row_count(table_sizes.at(0));
    }
  }



  if (OB_SUCC(ret)) {
    THIS_WORKER.set_timeout_ts(ObTimeUtility::current_time() + 1000*1000*1000);

    ObPhysicalPlan plan(THIS_WORKER.get_allocator());
    plan.set_stmt_type(stmt::T_SELECT, false);


    int32_t *projector = NULL;
    int64_t projector_count = 0;
    if (projector_counts.count() >= 1) {
      projector_count = projector_counts.at(0);
      projector = static_cast<int32_t *>(THIS_WORKER.get_allocator().alloc(projector_count * sizeof(int32_t)));
      for (int32_t i = 0; i < projector_count; ++i) {
        projector[i] = i;
      }
    }

    MyMockOperator input;
    input.iter = &gen;
    input.set_id(1);
    input.set_column_count(gen.new_row_.count_);
    input.set_phy_plan(&plan);
    if (projector_count > 0) {
      input.set_projector(projector, projector_count);
    }

    ObMaterial material(plan.get_allocator());
    material.set_id(0);
    if (projector_counts.count() != 0) {
      material.set_column_count(projector_counts.at(0));
    } else {
      material.set_column_count(schema->get_column_count());
    }
    material.set_child(0, input);
    material.set_phy_plan(&plan);


    if (OB_SUCC(ret)) {

      ObExecContext exec_ctx;
      ObSQLSessionInfo session;
      if (OB_SUCC(ret)) {
        if (OB_FAIL(exec_ctx.init_phy_op(2))) {
        } else if (OB_FAIL(exec_ctx.create_physical_plan_ctx())) {
          SQL_OPT_LOG(WARN, "failed create phy plan", K(ret));
        } else {
          exec_ctx.set_my_session(&session);
        }
      }


      ObObj group_concat_max_len;
      group_concat_max_len.set_uint64(123456789);

      if (OB_SUCC(ret)) {
        if (OB_FAIL(session.init_system_variables(false, false))) {
          SQL_OPT_LOG(WARN, "failed init sys var", K(ret));
        } else if (OB_FAIL(session.update_sys_variable(SYS_VAR_GROUP_CONCAT_MAX_LEN, group_concat_max_len))) {
          SQL_OPT_LOG(WARN, "failed set sys var", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        int64_t material_begin = 0, material_end = 0, add_begin = 0, add_end = 0, get_end = 0;
        //warm up

        if (OB_FAIL(material.open(exec_ctx))) {
          SQL_OPT_LOG(WARN, "failed to open group by");
        } else {
          int64_t row_count = 1;
          if (OB_FAIL(scan_op(material, exec_ctx, false, row_count))) {
            SQL_OPT_LOG(WARN, "failed to scan group by");
          }
        }

        if (OB_SUCC(ret)) {
          //get material scan time
//          if (print_output)
//            cout << "--- raw data ---" << endl;
          material_begin  = ObTimeUtility::current_time();
          if (OB_FAIL(material.rescan(exec_ctx))) {
            SQL_OPT_LOG(WARN, "failed to rescan material");
          } else {
            int64_t row_count = 1;
            if (OB_FAIL(scan_op(material, exec_ctx, false, row_count))) {
              SQL_OPT_LOG(WARN, "failed to scan material");
            }
          }
          material_end  = ObTimeUtility::current_time();
        }
        //get grp scan time

        if (OB_SUCC(ret)) {
          usleep(static_cast<int>(time_to_sleep));
          if (OB_FAIL(material.rescan(exec_ctx))) {
            SQL_OPT_LOG(WARN, "failed to rescan source");
          } else {
            ObRowStore store;
            const ObRowStore::StoredRow *srow = NULL;
            const ObNewRow *row = NULL;
            ObNewRow newrow;
            ObArray<const ObRowStore::StoredRow *> arr;
            ObObj objs[256];
            newrow.cells_ = objs;
            if (projector_counts.count() != 0) {
              newrow.count_ = projector_counts.at(0);
            } else {
              newrow.count_ = schema->get_column_count();
            }
            add_begin = ObTimeUtility::current_time();
            while (OB_SUCC(ret) && OB_SUCC(material.get_next_row(exec_ctx, row))) {
              if (OB_FAIL(store.add_row(*row, srow, 0, true))) {
                SQL_OPT_LOG(WARN, "failed to add row", K(ret));
              } else {
                if (3 == info_type || 4 == info_type) {
                  if (OB_FAIL(arr.push_back(srow))) {
                    SQL_OPT_LOG(WARN, "failed to push arr");
                  }
                }
              }
            }
            if (OB_ITER_END == ret) {
              ret = OB_SUCCESS;
            }
            if (OB_SUCC(ret)) {
              add_end = ObTimeUtility::current_time();

              if (4 == info_type) {
                std::random_shuffle(arr.data_, arr.data_ + arr.count());
              }
              int64_t row_count = 1;
              if (0 == info_type || 1 == info_type || 2 == info_type) {
                ObRowStore::Iterator iter = store.begin();
                while (OB_SUCC(iter.get_next_row(newrow))) {
                  if (print_output) {
                    cout << "Result Row "<< row_count <<" : " << "| ";
                    for (int64_t i = 0; i < newrow.get_count(); i++) {
                      print_obj(newrow.get_cell(i));
                    }
                    cout << endl;
                  }
                  row_count++;
                }
              } else if (3 == info_type || 4 == info_type) {
                for (int64_t i = 0; OB_SUCC(ret) && i < arr.count(); ++i) {
                  srow = arr.at(i);
                  if (OB_FAIL(ObRowUtil::convert(srow->get_compact_row(), newrow))) {
                    OB_LOG(WARN, "fail to convert compact row to ObRow", K(ret));
                  } else {
                    if (print_output) {
                      cout << "Result Row "<< row_count <<" : " << "| ";
                      for (int64_t i = 0; i < newrow.get_count(); i++) {
                        print_obj(newrow.get_cell(i));
                      }
                      cout << endl;
                    }
                    row_count++;
                  }
                }
              }
              row_count--;
              if (OB_ITER_END == ret || OB_SUCCESS == ret) {
                ret = OB_SUCCESS;
                get_end = ObTimeUtility::current_time();
                if (0 == info_type) {
                  cout << row_count << "," << get_end - add_begin - (material_end - material_begin) << endl;
                } else if (1 == info_type) {
                  cout << row_count << "," << add_end - add_begin - (material_end - material_begin) <<  endl;
                } else if (2 == info_type || 3 == info_type || 4 == info_type){
                  cout << row_count << "," << get_end - add_end << endl;
                }
              } else {
                SQL_OPT_LOG(WARN, "failed to scan row store");
              }
            }

          }
//          else if (OB_FAIL(material_t.inner_close(exec_ctx))) {
//            SQL_OPT_LOG(WARN, "failed to close material");
//          } else {
//            add_begin = ObTimeUtility::current_time();
//            if (OB_FAIL(material_t.inner_open(exec_ctx))) {
//              SQL_OPT_LOG(WARN, "failed to open material");
//            } else {
//              add_end = ObTimeUtility::current_time();
//              int64_t row_count = 1;
//              if (print_output)
//                cout << "--- material data ---" << endl;
//              if (OB_FAIL(scan_op(material_t, exec_ctx, print_output, row_count))) {
//                SQL_OPT_LOG(WARN, "failed to scan group by");
//              } else {
//                get_end = ObTimeUtility::current_time();
//                row_count -= 1;
//                cout << row_count << "," << add_end - add_begin - (material_end - material_begin) << "," << get_end - add_end << endl;
//              }
//            }
//          }
        }
      }
    }
  }
  return ret;
}





void set_rt_and_bind_cpu()
{
  cpu_set_t cpu_set;
  struct sched_param param;
  CPU_ZERO(&cpu_set);
  CPU_SET(5, &cpu_set);
  if(sched_setaffinity(static_cast<pid_t>(gettid()), sizeof(cpu_set), &cpu_set) == -1) {
    perror("sched_setaffinity() error!\n");
    exit(1);
  }
  param.sched_priority = 99;
  if(sched_setscheduler(static_cast<pid_t>(gettid()), SCHED_FIFO, &param) == -1){
    perror("sched_setscheduler() error!\n");
    exit(1);
  }
}


void print_usage(const char *program_name) {
  printf("%s\n", program_name);
  printf("cost model benchmark utility, used to collect statistics of performance\n");
  printf("    -h      need help\n");
  printf("    -s      schema file\n");
  printf("    -r      row count according to schema\n");
  printf("    -c      sort column count\n");
  printf("    -p      input projector count\n");
  printf("    -O      print out put\n");
  printf("    -e      equal cond count\n");
  printf("    -o      other cond count\n");
  printf("    -t      operator to test, sort, material, nestloop, merge\n");
  printf("    -B      set RT and bind cpu\n");
  printf("    -Z      seed min\n");
  printf("    -X      seed max\n");
  printf("    -C      seed step\n");
  printf("    -V      seed step length\n");
  printf("    -L      limit (for top-n sort)\n");
  printf("    -R      random\n");
  printf("    -K      experimental\n");
  printf("    -S      sleep micro sec before real testing (for bianque) \n");
  printf("    -T      add sort column \n");
  printf("    -i      info type needed \n");
  printf("    -l      common prefix len \n");
}

TEST(CostModelUtilityFuncCheck, group_by) {
  int ret = OB_SUCCESS;
  int64_t row_count = 0;
  ret = group_by_test(0, row_count);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_EQ(row_count, 1000);
  ret = group_by_test(1, row_count);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_EQ(row_count, 1000);
  ret = group_by_test(2, row_count);
  EXPECT_EQ(ret, OB_SUCCESS);
  EXPECT_EQ(row_count, 1);
}

int main (int argc, char **argv) {

  int ret = 0;
  system("rm -f cost_model_util.log*");
  OB_LOGGER.set_file_name("cost_model_util.log", true);
  OB_LOGGER.set_log_level(OB_LOG_LEVEL_ERROR);

  int param = 0;
  const char *short_opts = "hs:r:c:t:Bp:Oe:Z:X:C:V:o:GL:RKS:WT:i:l:";
  char *opt_arg_end = NULL;
  bool need_help = false;
  bool need_bind = false;

  while (OB_SUCCESS == ret && -1 != (param = getopt(argc,argv,short_opts))) {
    switch (param) {
    case 's': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        schema_file_names.push_back(optarg);
      }
      break;
    }
    case 'B': {
      need_bind = true;
      break;
    }
    case 'r': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t row_count = 0;
        row_count = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(row_count)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          table_sizes.push_back(row_count);
        }
      }
      break;
    }
    case 'p': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t proj_count = 0;
        proj_count = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(proj_count)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          projector_counts.push_back(proj_count);
        }
      }
      break;
    }
    case 'e': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t equal = 0;
        equal = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(equal)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          equal_cond_count = equal;
        }
      }
      break;
    }
    case 'o': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t other = 0;
        other = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(other)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          other_cond_count = other;
        }
      }
      break;
    }
    case 'c': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t column_count = 0;
        column_count = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(column_count)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          sort_column_count = column_count;
        }
      }
      break;
    }
    case 'Z': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t num = 0;
        num = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(num)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          seed_mins.push_back(num);
        }
      }
      break;
    }
    case 'X': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t num = 0;
        num = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(num)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          seed_maxs.push_back(num);
        }
      }
      break;
    }
    case 'C': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t num = 0;
        num = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(num)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          seed_steps.push_back(num);
        }
      }
      break;
    }
    case 'V': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t num = 0;
        num = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(num)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          seed_step_lengths.push_back(num);
        }
      }
      break;
    }
    case 't': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        op_type = optarg;
      }
      break;
    }
    case 'O': {
      print_output = true;
      break;
    }
    case 'h': {
      need_help = true;
      break;
    }
    case 'G': {
      run_as_ut = false;
      break;
    }
    case 'L': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        limit_count = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(limit_count)) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      break;
    }
    case 'l': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        common_prefix_len = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(common_prefix_len)) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      break;
    }
    case 'R': {
      use_random = true;
      break;
    }
    case 'K': {
      experimental = true;
      break;
    }
    case 'S': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        time_to_sleep = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(time_to_sleep)) {
          ret = OB_INVALID_ARGUMENT;
        }
      }
      break;
    }
    case 'W' : {
      OB_LOGGER.set_log_level(OB_LOG_LEVEL_WARN);
      break;
    }
    case 'T': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t sort_column = 0;
        sort_column = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(sort_column)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          order_columns.push_back(sort_column);
        }
      }
      break;
    }
    case 'i': {
      if (NULL == optarg) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        int64_t num = 0;
        num = strtol(optarg, &opt_arg_end, 10);
        if (STRTOL_ERR(num)) {
          ret = OB_INVALID_ARGUMENT;
        } else {
          info_type = num;
        }
      }
      break;
    }
    }
  }

  if (!run_as_ut) {
    if (need_help
        || schema_file_names.count() <= 0
        || table_sizes.count() <= 0
        || NULL == op_type) {
      print_usage(argv[0]);
      ret = OB_INVALID_ARGUMENT;
      exit(ret);
    }
  } else {
    char file_name[] = "cost_model_utils/c10k1.schema";
    schema_file_names.push_back(file_name);
    table_sizes.push_back(10000);
    projector_counts.push_back(5);
    seed_mins.push_back(1);
    seed_step_lengths.push_back(10);
    equal_cond_count = 1;
    other_cond_count = 1;
  }

  if (need_bind) {
    set_rt_and_bind_cpu();
  }

  if ((strcmp(op_type, "array") != 0) && OB_FAIL(setup_env())) {

  } else {
    if (run_as_ut) {
      int argc_f = 1;
      char arg_f[] = "fake";
      char *argv[] = {arg_f};
      testing::InitGoogleTest(&argc_f, argv);
      return RUN_ALL_TESTS();
    } else {
      int64_t rc = 0;
      if (strcmp(op_type, "array") == 0) {
        if (OB_FAIL(ob_array_test())) {

        } else {

        }
      } else if (strcmp(op_type, "sort") == 0) {
        if (OB_FAIL(sort_test(rc))) {

        } else {

        }
      } else if (strcmp(op_type, "material") == 0) {
        if (OB_FAIL(material_test(rc))) {

        } else {

        }
      } else if (strcmp(op_type, "nestloop") == 0) {
        if (OB_FAIL(join_test(0))) {

        } else {

        }
      } else if (strcmp(op_type, "merge") == 0) {
        if (OB_FAIL(join_test(1))) {

        } else {

        }
      } else if (strcmp(op_type, "hash") == 0) {
        if (OB_FAIL(join_test(2))) {

        } else {

        }
      } else if (strcmp(op_type, "mg") == 0) {
        if (OB_FAIL(group_by_test(1, rc))) {

        } else {

        }
      } else if (strcmp(op_type, "hg") == 0) {
        if (OB_FAIL(group_by_test(0, rc))) {

        } else {

        }
      } else if (strcmp(op_type, "scalar") == 0) {
        if (OB_FAIL(group_by_test(2, rc))) {

        } else {

        }
      }
    }
  }

  return ret;
}

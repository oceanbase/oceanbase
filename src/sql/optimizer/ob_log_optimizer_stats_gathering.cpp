/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_log_optimizer_stats_gathering.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/rewrite/ob_transform_utils.h"
#include "pl/sys_package/ob_dbms_stats.h"
#include "sql/optimizer/ob_log_plan.h"
#include "sql/optimizer/ob_optimizer_context.h"
#include "ob_opt_est_cost.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_struct.h"

using namespace oceanbase;
using namespace sql;
using namespace oceanbase::common;

namespace oceanbase
{
namespace sql
{

const char *ObLogOptimizerStatsGathering::get_name() const
{
  static const char *stats_gathering_name_[2] =
  {
    "OPTIMIZER STATS MERGE",
    "OPTIMIZER STATS GATHER",
  };
  return stats_gathering_name_[osg_type_ == OSG_TYPE::MERGE_OSG ? 0 : 1];
}
int ObLogOptimizerStatsGathering::get_op_exprs(ObIArray<ObRawExpr*> &all_exprs)
{
  int ret = OB_SUCCESS;
  if (NULL != calc_part_id_expr_ && OB_FAIL(all_exprs.push_back(calc_part_id_expr_))) {
    LOG_WARN("failed to push back expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, get_col_conv_exprs()))) {
    LOG_WARN("fail to append column conv expr", K(ret));
  } else if (OB_FAIL(append(all_exprs, get_generated_column_exprs()))) {
    LOG_WARN("fail to append column conv expr", K(ret));
  } else if (OB_FAIL(ObLogicalOperator::get_op_exprs(all_exprs))) {
    LOG_WARN("failed to get exprs", K(ret));
  } else { /*do nothing*/ }
  return ret;
}

// use this function to calc the part num, instead of the class member(part_num_);
int ObLogOptimizerStatsGathering::inner_get_table_schema(const ObTableSchema *&table_schema) {
  int ret = OB_SUCCESS;
  ObSqlSchemaGuard *schema_guard = NULL;
  ObOptimizerContext *opt_ctx = NULL;
  const ObDMLStmt *stmt = NULL;
  if (OB_ISNULL(get_plan())
      || OB_ISNULL(opt_ctx = &get_plan()->get_optimizer_context())
      || OB_ISNULL(schema_guard = opt_ctx->get_sql_schema_guard())
      || OB_ISNULL(stmt = get_plan()->get_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (OB_UNLIKELY(!stmt->is_insert_stmt())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stmt", K(ret), KPC(stmt));
  } else {
    const ObInsertStmt *ins_stmt = static_cast<const ObInsertStmt*>(stmt);
    uint64_t table_id = ins_stmt->get_insert_table_info().table_id_;
    uint64_t ref_table_id = ins_stmt->get_insert_table_info().ref_table_id_;
    if (OB_FAIL(schema_guard->get_table_schema(table_id, ref_table_id, ins_stmt, table_schema))) {
      LOG_WARN("fail to get table schema", K(ref_table_id), K(table_schema), K(ret));
    } else if (OB_ISNULL(table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index schema should not be null", K(table_schema), K(ret));
    }
  }
  return ret;
}

int ObLogOptimizerStatsGathering::est_cost()
{
  int ret = OB_SUCCESS;
  ObLogicalOperator *child = NULL;
  int64_t parallel = get_parallel();
  uint64_t total_part_num = 0;
  const ObTableSchema *tab_schema = NULL;
  if (OB_ISNULL(child = get_child(first_child))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(child), K(ret));
  } else if (OB_FAIL(inner_get_table_schema(tab_schema))) {
    LOG_WARN("fail to get table schema", K(ret));
  } else if (OB_FAIL(inner_get_stat_part_cnt(tab_schema, total_part_num))) {
    LOG_WARN("fail to get pstat num", K(ret));
  } else if (OB_UNLIKELY(parallel < 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected parallel", K(parallel), K(ret));
  } else {
    ObOptimizerContext &opt_ctx = get_plan()->get_optimizer_context();
    op_cost_ = 0;
    if (osg_type_ != OSG_TYPE::GATHER_OSG) {
      //for normal_osg and merge_osg calc the inner_sql_cost;
      uint64_t stats_size = ((get_col_conv_exprs().count() + get_generated_column_exprs().count() + 1)
                              * (total_part_num));
      double sql_num = stats_size / 2000; //2000 is the max_num of wirte stats in ObDbmsStatsUtils::split_batch_write
      op_cost_ += sql_num * 12800;
    }
    if (osg_type_ != OSG_TYPE::MERGE_OSG) {
      //for normal_osg and merge_osg calc the calc_stats cost;
      //TODO: use a more accurate model.
      op_cost_ += ObOptEstCost::cost_get_rows(child->get_card() / parallel, opt_ctx);
    }
    set_cost(op_cost_ + child->get_cost());
    set_card(child->get_card());
    set_width(child->get_width());
  }
  return ret;
}

int ObLogOptimizerStatsGathering::get_target_osg_id(uint64_t &target_id)
{
  int ret = OB_SUCCESS;
  target_id = OB_INVALID_ID;
  ObLogicalOperator *node = get_parent();
  if (OB_ISNULL(node)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    bool is_find = false;
    while (NULL != node && !is_find) {
      if (!node->is_osg_operator() || !static_cast<ObLogOptimizerStatsGathering *>(node)->is_merge_osg()) {
        node = node->get_parent();
      } else {
        is_find = true;
        target_id = node->get_op_id();
      }
    }
    if (OB_SUCC(ret) && !is_find) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not find next producer id", K(ret));
    }
  }
  return ret;
}

int ObLogOptimizerStatsGathering::inner_replace_op_exprs(ObRawExprReplacer &replacer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(replace_exprs_action(replacer, col_conv_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  } else if (OB_FAIL(replace_exprs_action(replacer, generated_column_exprs_))) {
    LOG_WARN("failed to replace exprs", K(ret));
  }
  return ret;
}

// calc stats cnt base on part_type and part_cnt;
int ObLogOptimizerStatsGathering::inner_get_stat_part_cnt(const ObTableSchema *table_schema, uint64_t &part_num)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table schema is null", K(ret));
  } else if (!table_schema->is_partitioned_table()) {
    part_num = 1; //for non-part table, only have global stats.
  } else {
    part_num = 0;
    if (share::schema::PARTITION_LEVEL_ONE == table_schema->get_part_level()) {
      part_num += table_schema->get_part_option().get_part_num() + 1;
    } else if  (share::schema::PARTITION_LEVEL_TWO == table_schema->get_part_level()) {
      part_num = table_schema->get_part_option().get_part_num();
      int total_subpart_num = 0;
      ObPartition *cur_part = NULL;
      for (int64_t i = 0; OB_SUCC(ret) && i < part_num; ++i) {
        if (OB_ISNULL(cur_part = (table_schema->get_part_array())[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          total_subpart_num += cur_part->get_sub_part_num();
        }
      }
      if (OB_SUCC(ret)) {
        part_num += total_subpart_num + 1;
      }
    }
  }
  return ret;
}

}
}
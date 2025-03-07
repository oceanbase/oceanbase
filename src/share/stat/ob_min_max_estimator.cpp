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
#include "share/stat/ob_min_max_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"
namespace oceanbase
{
namespace common
{

int ObStatMinMaxSubquery::gen_expr(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  char hint[OB_MAX_TABLE_NAME_LENGTH + 10];
  int64_t hint_pos = 0;
  if (OB_ISNULL(col_param_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (col_param_->index_name_.empty() &&
             OB_FAIL(databuff_printf(hint, sizeof(hint), hint_pos, "FULL(T)"))) {
    LOG_WARN("failed to print buf", K(ret));
  } else if (!col_param_->index_name_.empty() &&
             OB_FAIL(databuff_printf(hint, sizeof(hint), hint_pos, "INDEX(T %.*s)",
                                     col_param_->index_name_.length(),
                                     col_param_->index_name_.ptr()))) {
    LOG_WARN("failed to print  buf", K(ret));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos,
                                     lib::is_oracle_mode() ?
                                     " (SELECT /*+ %.*s */ %.*s FROM \"%.*s\".\"%.*s\" %.*s T WHERE %.*s IS NOT NULL ORDER BY 1 %s FETCH FIRST 1 ROWS ONLY)" :
                                     " (SELECT /*+ %.*s */ %.*s FROM `%.*s`.`%.*s` %.*s T WHERE %.*s IS NOT NULL ORDER BY 1 %s LIMIT 1)",
                                     (int)hint_pos,
                                     hint,
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr(),
                                     db_name_.length(),
                                     db_name_.ptr(),
                                     from_table_.length(),
                                     from_table_.ptr(),
                                     partition_string_.length(),
                                     partition_string_.ptr(),
                                     col_param_->column_name_.length(),
                                     col_param_->column_name_.ptr(),
                                     is_min_ ? "ASC" : "DESC"))) {
    LOG_WARN("failed to print buf", K(ret));
  }
  return ret;
}

int ObStatMinMaxSubquery::decode(ObObj &obj, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(col_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("col stat is not given", K(ret), K(col_stat_));
  } else if (OB_FAIL(ObDbmsStatsUtils::truncate_string_for_opt_stats(obj, allocator))) {
    LOG_WARN("fail to truncate string", K(ret));
  } else if (is_min_) {
    col_stat_->set_min_value(obj);
  } else {
    col_stat_->set_max_value(obj);
  }
  return ret;
}

ObMinMaxEstimator::ObMinMaxEstimator(ObExecContext &ctx, ObIAllocator &allocator)
  : ObBasicStatsEstimator(ctx, allocator)
{}

int ObMinMaxEstimator::add_min_max_stat_items(ObIAllocator &allocator,
                                              const ObOptStatGatherParam &param,
                                              const ObIArray<ObColumnStatParam> &column_params,
                                              ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(opt_stat.table_stat_) ||
      OB_UNLIKELY(opt_stat.column_stats_.count() != column_params.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(opt_stat), K(column_params));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
      const ObColumnStatParam *col_param = &column_params.at(i);
      if (OB_ISNULL(opt_stat.column_stats_.at(i)) ||
          OB_UNLIKELY(col_param->column_id_ != opt_stat.column_stats_.at(i)->get_column_id())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), KPC(opt_stat.column_stats_.at(i)), KPC(col_param));
      } else if (!col_param->need_refine_min_max()) {
        //do nothing
      } else {
        void *p = NULL;
        ObStatMinMaxSubquery *min_subquery = NULL;
        ObStatMinMaxSubquery *max_subquery = NULL;
        if (OB_ISNULL(p = allocator.alloc(sizeof(ObStatMinMaxSubquery)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (OB_FALSE_IT(min_subquery = new(p) ObStatMinMaxSubquery(col_param,
                                                                          opt_stat.column_stats_.at(i),
                                                                          db_name_,
                                                                          from_table_,
                                                                          partition_string_,
                                                                          true))) {
        } else if (OB_FAIL(stat_items_.push_back(min_subquery))) {
          LOG_WARN("failed to push back array", K(ret));
        } else if (OB_ISNULL(p = allocator.alloc(sizeof(ObStatMinMaxSubquery)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("allocate memory failed", K(ret));
        } else if (OB_FALSE_IT(max_subquery = new(p) ObStatMinMaxSubquery(col_param,
                                                                          opt_stat.column_stats_.at(i),
                                                                          db_name_,
                                                                          from_table_,
                                                                          partition_string_,
                                                                          false))) {
        } else if (OB_FAIL(stat_items_.push_back(max_subquery))) {
          LOG_WARN("failed to push back array", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObMinMaxEstimator::estimate(const ObOptStatGatherParam &param,
                                ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ObMinMaxEst", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObString no_rewrite("NO_REWRITE DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', 256)");
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  ObSEArray<ObOptStat, 1> tmp_opt_stats;
  if (OB_FAIL(add_from_table(allocator, param.db_name_, param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_UNLIKELY(param.partition_infos_.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (!param.partition_infos_.empty() &&
             OB_FAIL(fill_partition_info(allocator, param, param.partition_infos_.at(0)))) {
    LOG_WARN("failed to add partition info", K(ret));
  } else if (OB_FAIL(add_min_max_stat_items(allocator,
                                            param,
                                            param.column_params_,
                                            opt_stat))) {
    LOG_WARN("failed to add min max stat items", K(ret));
  } else if (get_item_size() <= 0) {
    // do nothing
  } else if (OB_FAIL(add_hint(no_rewrite, allocator))) {
    LOG_WARN("failed to add no_rewrite", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(pack_sql(raw_sql))) {
    LOG_WARN("failed to pack raw sql", K(ret));
  } else if (OB_FAIL(tmp_opt_stats.push_back(opt_stat))) {
    LOG_WARN("failed to push back", K(ret));
  } else if (OB_FAIL(do_estimate(param, raw_sql.string(), false,
                                 opt_stat, tmp_opt_stats))) {
    LOG_WARN("failed to evaluate basic stats", K(ret));
  } else {
    LOG_TRACE("succeed to gather min max value from index", K(opt_stat.column_stats_));
  }
  stat_items_.reuse();
  return ret;
}

int ObMinMaxEstimator::pack_sql(ObSqlString &raw_sql_str)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(gen_select_filed())) {
    LOG_WARN("failed to generate select filed", K(ret));
  } else if (OB_FAIL(raw_sql_str.append_fmt("SELECT %.*s %.*s FROM DUAL",
                                            other_hints_.length(),
                                            other_hints_.ptr(),
                                            static_cast<int32_t>(select_fields_.length()),
                                            select_fields_.ptr()))) {
    LOG_WARN("failed to build query sql stmt", K(ret));
  } else {
    LOG_TRACE("OptStat: min max stat query sql", K(raw_sql_str));
  }
  return ret;
}

} // end of namespace common
} // end of namespace oceanbase

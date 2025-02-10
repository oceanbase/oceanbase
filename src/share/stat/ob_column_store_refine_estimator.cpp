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
#include "share/stat/ob_column_store_refine_estimator.h"
#include "share/stat/ob_dbms_stats_utils.h"
namespace oceanbase
{
namespace common
{

ObColumnStoreRefineEstimator::ObColumnStoreRefineEstimator(ObExecContext &ctx, ObIAllocator &allocator)
  : ObBasicStatsEstimator(ctx, allocator)
{

}

int ObColumnStoreRefineEstimator::estimate(const ObOptStatGatherParam &param,
                                           ObOptStat &opt_stat)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("ObCSRStatsEst", OB_MALLOC_NORMAL_BLOCK_SIZE, param.tenant_id_);
  ObSEArray<ObOptStat, 1> tmp_opt_stats;
  ObOptTableStat *src_tab_stat = opt_stat.table_stat_;
  ObIArray<ObOptColumnStat*> &src_col_stats = opt_stat.column_stats_;
  int64_t duration_time = -1;
  const ObIArray<ObColumnStatParam> &column_params = param.column_params_;
  ObSqlString raw_sql;
  if (OB_UNLIKELY(param.partition_infos_.count() > 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param));
  } else if (OB_FAIL(add_from_table(allocator, param.db_name_, param.tab_name_))) {
    LOG_WARN("failed to add from table", K(ret));
  } else if (OB_FAIL(fill_hints(allocator, param.tab_name_, param.gather_vectorize_,
                                true, !param.partition_infos_.empty()))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to add query sql parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(fill_specify_scn_info(allocator, param.sepcify_scn_))) {
    LOG_WARN("failed to fill specify scn info", K(ret));
  } else if (OB_FAIL(add_stat_item(ObStatRowCount(src_tab_stat)))) {
    LOG_WARN("failed to add row count", K(ret));
  } else if (!param.partition_infos_.empty() &&
              OB_FAIL(fill_partition_info(allocator, param, param.partition_infos_.at(0)))) {
    LOG_WARN("failed to add partition info", K(ret));
  } else {
    src_tab_stat->set_partition_id(param.partition_infos_.at(0).part_id_);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    const ObColumnStatParam *col_param = &column_params.at(i);
    if (!col_param->need_cs_refine_min_max()) {
      // do nothing
    } else if (OB_FAIL(add_stat_item(ObStatNumNull(col_param, src_tab_stat, src_col_stats.at(i))))) {
      LOG_WARN("failed to add statistic item", K(ret));
    } else if (OB_FAIL(add_stat_item(ObStatMaxValue(col_param, src_col_stats.at(i)))) ||
               OB_FAIL(add_stat_item(ObStatMinValue(col_param, src_col_stats.at(i))))) {
      LOG_WARN("failed to add statistic item", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(pack(raw_sql))) {
      LOG_WARN("failed to pack raw sql", K(ret));
    } else if (OB_FAIL(tmp_opt_stats.push_back(opt_stat))) {
      LOG_WARN("failed to push back tmp opt stats", K(ret));
    } else if (OB_FAIL(do_estimate(param, raw_sql.string(), false, opt_stat, tmp_opt_stats))) {
      LOG_WARN("failed to evaluate basic stats", K(ret));
    } else {
      LOG_TRACE("column store refined min/max/count");
    }
  }
  return ret;
}

}
}
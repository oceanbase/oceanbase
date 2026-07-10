/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_basic_catalog_stats_estimator.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/ob_dbms_stats_utils.h"
#include "share/stat/ob_stat_item.h"
#include "share/stat/ob_stat_define.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_sql_context.h"
#include "share/schema/ob_schema_struct.h"
#include "share/catalog/ob_catalog_meta_getter.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/external_table/ob_external_table_file_mgr.h"

namespace oceanbase
{
using namespace sql;
namespace common
{

ObBasicCatalogStatsEstimator::ObBasicCatalogStatsEstimator(ObExecContext &ctx,
                                                           ObIAllocator &allocator,
                                                           bool can_re_estimate)
    : ObCatalogStatsEstimator(ctx, allocator), can_re_estimate_(can_re_estimate)
{
}

int ObBasicCatalogStatsEstimator::estimate(const ObOptCatalogStatGatherParam &param,
                                           ObIArray<ObOptCatalogStat> &dst_opt_catalog_stats)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObCatalogColumnStatParam> &column_params = param.column_params_;
  ObString calc_part_val_str;
  ObOptCatalogTableStat table_stat;
  ObOptCatalogStat src_opt_catalog_stat;
  src_opt_catalog_stat.table_stat_ = &table_stat;

  ObOptCatalogTableStat *src_table_stat = src_opt_catalog_stat.table_stat_;
  ObIArray<ObOptCatalogColumnStat*> &src_col_stats = src_opt_catalog_stat.column_stats_;

  ObArenaAllocator allocator("ObBasCatStatEst", OB_MALLOC_NORMAL_BLOCK_SIZE, param.table_identity_.tenant_id_);
  ObSqlString raw_sql;
  int64_t duration_time = -1;
  const int64_t part_cnt = param.partition_infos_.count();
  // NOTICE!! Same as internal table order.
  // Note that there are dependences between different kinds of statistics
  //            1. RowCount should be added at the first
  //            2. NumDistinct should be estimated before TopKHist
  //            3. AvgRowLen should be added at the last
  if (dst_opt_catalog_stats.empty() || OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected empty", K(ret), K(dst_opt_catalog_stats.empty()), K(param.allocator_));
  } else if (OB_FAIL(init_escape_char_names(allocator, param))) {
    LOG_WARN("failed to init escape char names", K(ret));
  } else if (OB_FALSE_IT(set_from_table(tab_name_))) {
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::init_catalog_col_stats(column_params.count(),
                                                                     allocator,
                                                                     src_col_stats))) {
    LOG_WARN("failed to init catalog col stats", K(ret));
  } else if (OB_FAIL(fill_hints(allocator,
                                from_table_,
                                part_cnt,
                                param.gather_vectorize_))) {
    LOG_WARN("failed to fill hints", K(ret));
  } else if (OB_FAIL(fill_parallel_info(allocator, param.degree_))) {
    LOG_WARN("failed to add query sql parallel info", K(ret));
  } else if (OB_FAIL(ObDbmsStatsUtils::get_valid_duration_time(param.gather_start_time_,
                                                               param.max_duration_time_,
                                                               duration_time))) {
    LOG_WARN("failed to get valid duration time", K(ret));
  } else if (OB_FAIL(fill_query_timeout_info(allocator, duration_time))) {
    LOG_WARN("failed to fill query timeout info", K(ret));
  } else if (OB_FAIL(fill_sample_info(allocator, param.sample_info_))) {
    LOG_WARN("failed to fill sample info", K(ret));
  } else if (OB_FAIL(add_stat_item(ObCatalogStatRowCount(src_table_stat)))) {
    LOG_WARN("failed to add row count", K(ret));
  } else if (!param.is_split_gather_) {
    if (dst_opt_catalog_stats.count() > 1) {
      if (OB_FAIL(fill_group_by_partition_columns(param, allocator, calc_part_val_str))) {
        LOG_WARN("failed to fill group by partition columns", K(ret));
      } else if (OB_FAIL(add_stat_item(ObCatalogPartitionValue(src_table_stat, calc_part_val_str)))) {
        LOG_WARN("failed to add catalog partition value", K(ret));
      }
      else if (param.is_specify_partition_
                 && OB_FAIL(ObCatalogStatsEstimator::fill_partition_info(param.part_cols_,
                                                                         allocator,
                                                                         param.partition_infos_))) {
        LOG_WARN("failed to add partition info", K(ret));
      }
    } else if (OB_UNLIKELY(param.partition_infos_.count() > 1
                           || OB_ISNULL(dst_opt_catalog_stats.at(0).table_stat_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.partition_infos_));
    } else if (share::ObLakeTableFormat::ICEBERG == param.external_info_.lake_table_format_
               && param.stat_level_ == PARTITION_LEVEL
               && !param.partition_infos_.empty()
               && !param.partition_infos_.at(0).partition_.empty()
               ) {
      if (OB_FAIL(ObDbmsCatalogStatsUtils::build_iceberg_partition_sql_clause(
                      allocator, param.partition_infos_.at(0), partition_string_))) {
        LOG_WARN("failed to build iceberg partition clause", K(ret), K(param.partition_infos_.at(0)));
      } else if (OB_FAIL(ob_write_string(allocator,
                                     dst_opt_catalog_stats.at(0).table_stat_->get_partition_value(),
                                     src_table_stat->get_partition_value()))) {
        LOG_WARN("failed to write iceberg partition value", K(ret), K(param.partition_infos_.at(0)));
      }
    } else if (param.stat_level_ == PARTITION_LEVEL
               && !param.partition_infos_.empty()
               && OB_FAIL(ObCatalogStatsEstimator::fill_single_partition_info(
                   param.part_cols_,
                   allocator,
                   param.partition_infos_.at(0).partition_values_))) {
      LOG_WARN("failed to add partition info for single partition", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator,
                                   dst_opt_catalog_stats.at(0).table_stat_->get_partition_value(),
                                   src_table_stat->get_partition_value()))) {
      LOG_WARN("failed to write partition value", K(ret));
    }
  } else {
    if (dst_opt_catalog_stats.count() > 1) {
      if (OB_FAIL(fill_group_by_partition_columns(param, allocator, calc_part_val_str))) {
        LOG_WARN("failed to fill group by partition columns", K(ret));
      } else if (OB_FAIL(add_stat_item(ObCatalogPartitionValue(src_table_stat, calc_part_val_str)))) {
        LOG_WARN("failed to add catalog partition value", K(ret));
      }
      else if (OB_FAIL(ObCatalogStatsEstimator::fill_partition_info(param.part_cols_,
                                                                      allocator,
                                                                      param.partition_infos_))) {
        LOG_WARN("failed to add partition info", K(ret));
      }
    } else if (OB_UNLIKELY(param.partition_infos_.count() > 1)
               || OB_ISNULL(dst_opt_catalog_stats.at(0).table_stat_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(param.partition_infos_));
    } else if (share::ObLakeTableFormat::ICEBERG == param.external_info_.lake_table_format_
               && param.stat_level_ == PARTITION_LEVEL
               && !param.partition_infos_.empty()
               && !param.partition_infos_.at(0).partition_.empty()) {
      if (OB_FAIL(ObDbmsCatalogStatsUtils::build_iceberg_partition_sql_clause(
                      allocator, param.partition_infos_.at(0), partition_string_))) {
        LOG_WARN("failed to build iceberg partition clause", K(ret), K(param.partition_infos_.at(0)));
      } else if (OB_FAIL(ob_write_string(allocator,
                                     dst_opt_catalog_stats.at(0).table_stat_->get_partition_value(),
                                     src_table_stat->get_partition_value()))) {
        LOG_WARN("failed to write iceberg partition value", K(ret), K(param.partition_infos_.at(0)));
      }
    } else if (param.stat_level_ == PARTITION_LEVEL
               && !param.partition_infos_.empty()
               && OB_FAIL(
                   fill_single_partition_info(param.part_cols_,
                                              allocator,
                                              param.partition_infos_.at(0).partition_values_))) {
      LOG_WARN("failed to add partition info", K(ret));
    } else {
      src_table_stat->set_partition_value(dst_opt_catalog_stats.at(0).table_stat_->get_partition_value());
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < column_params.count(); ++i) {
    const ObCatalogColumnStatParam *col_param = &column_params.at(i);
    if (OB_FAIL(add_stat_item(ObCatalogStatMaxValue(col_param, src_col_stats.at(i))))
        || OB_FAIL(add_stat_item(ObCatalogStatMinValue(col_param, src_col_stats.at(i))))
        || OB_FAIL(add_stat_item(ObCatalogStatNumNull(col_param, src_table_stat, src_col_stats.at(i))))
        || OB_FAIL(add_stat_item(
            ObCatalogStatNumDistinct(col_param, src_col_stats.at(i), param.need_approx_ndv_)))
        || OB_FAIL(add_stat_item(ObCatalogStatAvgLen(col_param, src_col_stats.at(i))))
        || OB_FAIL(add_stat_item(ObCatalogStatLlcBitmap(col_param, src_col_stats.at(i))))) {
      LOG_WARN("failed to add statistic item", K(ret));
    } else { /*do nothing*/
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(add_stat_item(ObCatalogStatAvgRowLen(src_table_stat, src_col_stats)))) {
      LOG_WARN("failed to add avg row size estimator", K(ret));
    } else if (OB_FAIL(pack(raw_sql))) {
      LOG_WARN("failed to pack raw sql", K(ret));
    } else if (OB_FAIL(do_estimate(param, raw_sql.string(), true, src_opt_catalog_stat, dst_opt_catalog_stats))) {
      LOG_WARN("failed to evaluate basic stats", K(ret));
    } else if (OB_FAIL(fetch_and_set_external_table_file_stats(param, dst_opt_catalog_stats))) {
      LOG_WARN("failed to fetch external table file stats", K(ret));
    // TODO(bitao): check if we need to refine basic stats.
    } else if (OB_FAIL(refine_basic_stats(param, dst_opt_catalog_stats))) {
      LOG_WARN("failed to refine basic stats", K(ret));
    } else {
      LOG_TRACE("basic stats is collected", K(dst_opt_catalog_stats.count()), K(raw_sql.string()));
    }
  }
  return ret;
}

int ObBasicCatalogStatsEstimator::fill_hints(common::ObIAllocator &alloc,
                                             const ObString &table_name,
                                             const int64_t part_cnt,
                                             const int64_t gather_vectorize)
{
  int ret = OB_SUCCESS;
  ObSqlString default_hints;
  if (OB_UNLIKELY(table_name.empty() || gather_vectorize < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(table_name), K(gather_vectorize));
  } else if (OB_FAIL(default_hints.append_fmt(
                 " NO_REWRITE DBMS_STATS OPT_PARAM('ROWSETS_MAX_ROWS', %ld) ",
                 gather_vectorize))) {
    LOG_WARN("failed to append fmt", K(ret), K(gather_vectorize));
  } else if (part_cnt > 1
             && OB_FAIL(default_hints.append_fmt("OPT_PARAM('INLIST_REWRITE_THRESHOLD', %ld) ",
                                                 part_cnt + 1))) {
    // Why threshold is part_cnt + 1?
    // Because the inlist rewrite threshold is the number of partitions + 1.
    LOG_WARN("failed to append inlist rewrite threshold", K(ret), K(part_cnt));
  } else if (OB_FAIL(
                 default_hints.append_fmt(" OPT_PARAM('APPROX_COUNT_DISTINCT_PRECISION', 10) "))) {
    LOG_WARN("failed to append fmt", K(ret));
  } else if (OB_FAIL(add_hint(default_hints.string(), alloc))) {
    LOG_WARN("failed to add hint", K(ret));
  }
  return ret;
}

int ObBasicCatalogStatsEstimator::check_stat_need_re_estimate(
    const ObOptCatalogStatGatherParam &origin_param,
    ObOptCatalogStat &catalog_stat,
    bool &need_re_estimate,
    ObOptCatalogStatGatherParam &new_param)
{
  int ret = OB_SUCCESS;
  need_re_estimate = false;
  if (OB_ISNULL(catalog_stat.table_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(catalog_stat.table_stat_));
  } else {
    int64_t row_cnt = catalog_stat.table_stat_->get_row_count();
    int64_t sample_size = catalog_stat.table_stat_->get_sample_size();
    ObString partition_value = catalog_stat.table_stat_->get_partition_value();
    // NOTE: row_cnt is not 0 if the table/partition is not empty.
    // Original code: row_cnt * sample_value_ / 100 >= MAGIC_MIN_SAMPLE_SIZE
    if (OB_LIKELY(0 < row_cnt)) {
      // do nothing
      LOG_INFO("skip re-estimate", K(ret), K(origin_param.column_params_), K(catalog_stat), K(sample_value_), K(row_cnt), K(MAGIC_MIN_SAMPLE_SIZE));
    } else if (OB_FAIL(new_param.assign(origin_param))) {
      LOG_WARN("failed to assign", K(ret));
    } else {
      need_re_estimate = true;
      int64_t total_row_count = row_cnt;
      // 1.set sample ratio
      // Original code: total_row_count <= MAGIC_SAMPLE_SIZE
      if (OB_UNLIKELY(0 >= total_row_count)) {
        new_param.sample_info_.reset();
      } else {
        new_param.sample_info_.reset();
        new_param.sample_info_.set_percent((MAGIC_SAMPLE_SIZE * 100.0) / total_row_count);
        new_param.sample_info_.set_sample_mode(
            ObCatalogAnalyzeSampleInfo::ROW);
      }
      // 2. Set partition info for re-estimation
      // For partitioned catalog tables, we need to set partition_infos_ to only query this
      // partition. Must copy partition_values_ from origin_param for fill_single_partition_info.
      if (!partition_value.empty()) {
        new_param.partition_infos_.reset();
        bool found = false;
        for (int64_t i = 0; OB_SUCC(ret) && !found && i < origin_param.partition_infos_.count(); ++i) {
          if (origin_param.partition_infos_.at(i).partition_ == partition_value) {
            found = true;
            if (OB_FAIL(new_param.partition_infos_.push_back(origin_param.partition_infos_.at(i)))) {
              LOG_WARN("failed to push back partition info", K(ret), K(partition_value));
            }
          }
        }
        if (OB_SUCC(ret) && !found) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition not found in origin_param", K(ret), K(partition_value));
        }
      }
      // 3. Reset catalog stat (align with internal table)
      if (OB_SUCC(ret)) {
        catalog_stat.table_stat_->set_row_count(0);
        catalog_stat.table_stat_->set_data_size(0);
        for (int64_t j = 0; OB_SUCC(ret) && j < catalog_stat.column_stats_.count(); ++j) {
          if (OB_ISNULL(catalog_stat.column_stats_.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected error", K(ret));
          } else {
            ObObj null_val;
            null_val.set_null();
            catalog_stat.column_stats_.at(j)->set_max_value(null_val);
            catalog_stat.column_stats_.at(j)->set_min_value(null_val);
            catalog_stat.column_stats_.at(j)->set_num_not_null(0);
            catalog_stat.column_stats_.at(j)->set_num_null(0);
            catalog_stat.column_stats_.at(j)->set_num_distinct(0);
            catalog_stat.column_stats_.at(j)->set_avg_length(0);
            MEMSET(const_cast<char *>(catalog_stat.column_stats_.at(j)->get_llc_bitmap()),
                   0,
                   catalog_stat.column_stats_.at(j)->get_llc_bitmap_size());
          }
        }
      }
    }
  }
  return ret;
}

int ObBasicCatalogStatsEstimator::fill_group_by_partition_columns(
    const ObOptCatalogStatGatherParam &param,
    ObIAllocator &allocator,
    ObString &partition_cols_str)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString, 4> partition_col_names;
  ObSqlString group_by_str;

  for (int64_t i = 0; OB_SUCC(ret) && i < param.part_cols_.count(); ++i) {
    if (OB_FAIL(partition_col_names.push_back(param.part_cols_.at(i)))) {
      LOG_WARN("failed to push back partition column name", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (partition_col_names.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition column names is empty, table should be partitioned", K(ret));
  } else {
    if (OB_FAIL(group_by_str.append("GROUP BY "))) {
      LOG_WARN("failed to append GROUP BY", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_names.count(); ++i) {
        if (i > 0 && OB_FAIL(group_by_str.append(", "))) {
          LOG_WARN("failed to append comma", K(ret));
        } else if (OB_FAIL(group_by_str.append(partition_col_names.at(i)))) {
          LOG_WARN("failed to append partition expression", K(ret));
        }
      }
    }

    // Save group_by_string_ (kept as "GROUP BY col1, col2")
    if (OB_SUCC(ret)) {
      char *buf = nullptr;
      int64_t buf_len = group_by_str.length();
      if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for group by string", K(ret), K(buf_len));
      } else {
        MEMCPY(buf, group_by_str.ptr(), buf_len);
        group_by_string_.assign(buf, buf_len);
      }
    }

    // Build CONCAT expression for partition_cols_str used by ObCatalogPartitionValue.
    // Produces Hive partition spec format to match dst partition_value_.
    // e.g. single col:  CONCAT('o_orderdate=', o_orderdate)
    //      multi col:   CONCAT('year=', year, '/', 'month=', month)
    // When __HIVE_DEFAULT_PARTITION__ exists, wrap with IFNULL to produce
    // the matching partition_value string instead of NULL.
    if (OB_SUCC(ret)) {
      bool has_default_partition = false;
      for (int64_t i = 0; !has_default_partition && i < param.partition_infos_.count(); ++i) {
        for (int64_t j = 0; j < param.partition_infos_.at(i).partition_values_.count(); ++j) {
          if (ObDbmsCatalogStatsUtils::is_hive_default_partition(
                  param.partition_infos_.at(i).partition_values_.at(j))) {
            has_default_partition = true;
            break;
          }
        }
      }

      ObSqlString concat_str;
      if (OB_FAIL(concat_str.append("CONCAT("))) {
        LOG_WARN("failed to append", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < partition_col_names.count(); ++i) {
        if (i > 0 && OB_FAIL(concat_str.append(", '/', "))) {
          LOG_WARN("failed to append separator", K(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (has_default_partition) {
          if (OB_FAIL(concat_str.append_fmt(
                  "'%.*s=', IFNULL(CAST(%.*s AS CHAR), '__HIVE_DEFAULT_PARTITION__')",
                  partition_col_names.at(i).length(),
                  partition_col_names.at(i).ptr(),
                  partition_col_names.at(i).length(),
                  partition_col_names.at(i).ptr()))) {
            LOG_WARN("failed to append concat part", K(ret));
          }
        } else {
          if (OB_FAIL(concat_str.append_fmt(
                  "'%.*s=', %.*s",
                  partition_col_names.at(i).length(),
                  partition_col_names.at(i).ptr(),
                  partition_col_names.at(i).length(),
                  partition_col_names.at(i).ptr()))) {
            LOG_WARN("failed to append concat part", K(ret));
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(concat_str.append(")"))) {
        LOG_WARN("failed to append closing paren", K(ret));
      }
      if (OB_SUCC(ret)) {
        char *concat_buf = nullptr;
        int64_t concat_len = concat_str.length();
        if (OB_ISNULL(concat_buf = static_cast<char *>(allocator.alloc(concat_len)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc memory for concat expr", K(ret), K(concat_len));
        } else {
          MEMCPY(concat_buf, concat_str.ptr(), concat_len);
          partition_cols_str.assign(concat_buf, concat_len);
        }
      }
    }
  }

  return ret;
}

int ObBasicCatalogStatsEstimator::refine_basic_stats(const ObOptCatalogStatGatherParam &param,
                                                     ObIArray<ObOptCatalogStat> &dst_opt_stats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else if (sample_value_ >= 0.000001 && sample_value_ < 100.0) {
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_opt_stats.count(); ++i) {
      bool need_re_estimate = false;
      ObOptCatalogStatGatherParam new_param;
      ObSEArray<ObOptCatalogStat, 1> tmp_opt_stats;
      ObBasicCatalogStatsEstimator basic_re_est(ctx_, *param.allocator_, false);
      if (OB_FAIL(check_stat_need_re_estimate(param,
                                              dst_opt_stats.at(i),
                                              need_re_estimate,
                                              new_param))) {
        LOG_WARN("failed to check stat need re-estimate", K(ret));
      } else if (!need_re_estimate) {
        // do nothing
      } else if (!can_re_estimate_) {
        LOG_INFO("can re-estimate only once",
                 K(sample_value_),
                 K(param.sample_info_.percent_),
                 K(new_param.sample_info_.percent_));
      } else if (OB_FAIL(tmp_opt_stats.push_back(dst_opt_stats.at(i)))) {
        LOG_WARN("failed to push back", K(ret));
      } else if (OB_FAIL(basic_re_est.estimate(new_param, tmp_opt_stats))) {
        LOG_WARN("failed to estimate basic statistics", K(ret));
      } else {
        LOG_TRACE("Suceed to re-estimate stats", K(new_param), K(param));
      }
    }
  }
  return ret;
}

int ObBasicCatalogStatsEstimator::fetch_and_set_external_table_file_stats(
    const ObOptCatalogStatGatherParam &param,
    ObIArray<ObOptCatalogStat> &dst_opt_catalog_stats)
{
  int ret = OB_SUCCESS;

  // Notes: the dst_opt_catalog_stats is same order as param.partition_infos_
  if (OB_UNLIKELY(dst_opt_catalog_stats.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dst_opt_catalog_stats is empty", K(ret));
  } else {
    // Set file statistics for each stat
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_opt_catalog_stats.count(); ++i) {
      share::ObOptCatalogTableStat *tbl_stat = dst_opt_catalog_stats.at(i).table_stat_;
      const ObCatalogExtPartitionInfo &part_info = param.partition_infos_.at(i);
      if (OB_ISNULL(tbl_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table_stat is null", K(ret), K(i));
      } else {
        tbl_stat->set_file_num(part_info.file_num_);
        tbl_stat->set_data_size(part_info.data_size_);
        tbl_stat->set_last_analyzed(part_info.modify_ts_);
        tbl_stat->set_schema_version(part_info.schema_version_);
        // Set last_analyzed for column stats as well.
        ObIArray<share::ObOptCatalogColumnStat *> &col_stats
            = dst_opt_catalog_stats.at(i).column_stats_;
        for (int64_t j = 0; OB_SUCC(ret) && j < col_stats.count(); ++j) {
          if (OB_ISNULL(col_stats.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column_stat is null", K(ret), K(i), K(j));
          } else {
            col_stats.at(j)->set_last_analyzed(part_info.modify_ts_);
          }
        }
      }
    }
  }

  return ret;
}

} // namespace common
} // namespace oceanbase

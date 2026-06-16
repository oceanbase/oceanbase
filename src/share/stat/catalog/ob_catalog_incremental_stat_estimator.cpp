/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_catalog_incremental_stat_estimator.h"
#include "share/stat/ob_stat_item.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace common
{

int ObCatalogIncrementalStatEstimator::derive_global_tbl_stat(
    const ObCatalogTableStatParam &param,
    const ObIArray<ObOptCatalogStat> &part_stats,
    ObOptCatalogStat &global_stat)
{
  int ret = OB_SUCCESS;
  share::ObOptCatalogTableStat *first_tbl_stat = NULL;

  if (OB_UNLIKELY(part_stats.empty()) || OB_ISNULL(first_tbl_stat = part_stats.at(0).table_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(part_stats.count()), KP(first_tbl_stat));
  } else {
    void *ptr = NULL;
    if (OB_ISNULL(ptr = param.allocator_->alloc(sizeof(share::ObOptCatalogTableStat)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory", K(ret));
    } else {
      share::ObOptCatalogTableStat *tbl_stat = new (ptr) share::ObOptCatalogTableStat();
      global_stat.table_stat_ = tbl_stat;

      // Aggregate statistics from all partitions
      int64_t total_row_count = 0;
      int64_t total_data_size = 0;
      int64_t total_file_num = 0;
      int64_t total_sample_size = 0;
      double weighted_avg_row_len = 0.0;
      int64_t max_schema_version = 0;

      for (int64_t i = 0; OB_SUCC(ret) && i < part_stats.count(); ++i) {
        share::ObOptCatalogTableStat *part_tbl_stat = part_stats.at(i).table_stat_;
        if (OB_ISNULL(part_tbl_stat)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("partition table stat is null", K(ret), K(i));
        } else {
          int64_t part_row_count = part_tbl_stat->get_row_count();
          total_row_count += part_row_count;
          total_data_size += part_tbl_stat->get_data_size();
          total_file_num += part_tbl_stat->get_file_num();
          total_sample_size += part_tbl_stat->get_sample_size();
          // Weighted average for avg_row_len
          weighted_avg_row_len += static_cast<double>(part_row_count)
                                  * static_cast<double>(part_tbl_stat->get_avg_row_len());
          max_schema_version = std::max(max_schema_version, part_tbl_stat->get_schema_version());
          LOG_TRACE("set schema version to catalog table stat", K(ret), K(max_schema_version));
        }
      }

      if (OB_SUCC(ret)) {
        // Calculate weighted average row length
        int64_t avg_row_len = 0;
        if (total_row_count > 0) {
          avg_row_len
              = static_cast<int64_t>(weighted_avg_row_len / static_cast<double>(total_row_count));
        }

        // Set global table stat values
        tbl_stat->set_tenant_id(param.table_identity_.tenant_id_);
        tbl_stat->set_catalog_id(param.table_identity_.catalog_id_);
        tbl_stat->get_database_name() = param.table_identity_.db_name_;
        tbl_stat->get_table_name() = param.table_identity_.tab_name_;
        tbl_stat->get_partition_value().reset(); // Global stat has empty partition value
        tbl_stat->set_row_count(total_row_count);
        tbl_stat->set_data_size(total_data_size);
        tbl_stat->set_file_num(total_file_num);
        tbl_stat->set_sample_size(total_sample_size);
        tbl_stat->set_avg_row_len(avg_row_len);
        tbl_stat->set_schema_version(max_schema_version);
      }
    }
  }
  return ret;
}

int ObCatalogIncrementalStatEstimator::derive_global_col_stat(
    const ObCatalogTableStatParam &param,
    const ObIArray<ObOptCatalogStat> &part_stats,
    ObOptCatalogStat &global_stat)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = param.column_params_.count();

  if (OB_UNLIKELY(part_stats.empty()) || OB_UNLIKELY(column_cnt <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(part_stats.count()), K(column_cnt));
  } else if (OB_ISNULL(global_stat.table_stat_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("global table stat is null", K(ret));
  } else if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    // Process each column
    for (int64_t col_idx = 0; OB_SUCC(ret) && col_idx < column_cnt; ++col_idx) {
      const ObCatalogColumnStatParam &col_param = param.column_params_.at(col_idx);
      void *ptr = NULL;

      if (OB_ISNULL(ptr = param.allocator_->alloc(sizeof(share::ObOptCatalogColumnStat)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret));
      } else {
        share::ObOptCatalogColumnStat *col_stat = new (ptr) share::ObOptCatalogColumnStat();

        // Use ObGlobalNdvEval to merge llc_bitmap for accurate NDV estimation
        // Same approach as internal table in ObIncrementalStatEstimator::derive_global_col_stat
        ObGlobalMinEval min_eval;
        ObGlobalMaxEval max_eval;
        ObGlobalNullEval null_eval;
        ObGlobalNotNullEval not_null_eval;
        ObGlobalNdvEval ndv_eval;
        ObGlobalAvglenEval avglen_eval;
        int64_t total_row_count = 0;
        ObCollationType cs_type = CS_TYPE_INVALID;

        for (int64_t part_idx = 0; OB_SUCC(ret) && part_idx < part_stats.count(); ++part_idx) {
          const ObOptCatalogStat &part_stat = part_stats.at(part_idx);
          share::ObOptCatalogColumnStat *part_col_stat = NULL;

          // Find the column stat for this column in this partition
          for (int64_t k = 0; part_col_stat == NULL && k < part_stat.column_stats_.count(); ++k) {
            share::ObOptCatalogColumnStat *tmp_stat = part_stat.column_stats_.at(k);
            if (OB_NOT_NULL(tmp_stat) && tmp_stat->get_column_name() == col_param.column_name_) {
              part_col_stat = tmp_stat;
            }
          }

          if (OB_ISNULL(part_col_stat)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("partition column stat not found", K(ret), K(col_param.column_name_));
          } else {
            // Get row count from partition table stat
            int64_t part_row_count = 0;
            if (OB_NOT_NULL(part_stat.table_stat_)) {
              part_row_count = part_stat.table_stat_->get_row_count();
            }
            total_row_count += part_row_count;
            cs_type = part_col_stat->get_collation_type();

            // Skip empty partitions (same as internal table logic)
            if (part_col_stat->get_num_distinct() == 0 && part_col_stat->get_num_null() == 0) {
              // skip
            } else {
              null_eval.add(part_col_stat->get_num_null());
              if (part_col_stat->get_num_distinct() != 0) {
                min_eval.add(part_col_stat->get_min_value());
                max_eval.add(part_col_stat->get_max_value());
                // Use llc_bitmap for accurate NDV merging
                ndv_eval.add(part_col_stat->get_num_distinct(), part_col_stat->get_llc_bitmap());
                not_null_eval.add(part_row_count - part_col_stat->get_num_null());
              }
              if (part_col_stat->get_avg_length() != 0) {
                avglen_eval.add(part_col_stat->get_avg_length());
              }
            }
          }
        }

        if (OB_SUCC(ret)) {
          // Set column stat values
          col_stat->set_tenant_id(param.table_identity_.tenant_id_);
          col_stat->set_catalog_id(param.table_identity_.catalog_id_);
          col_stat->get_database_name() = param.table_identity_.db_name_;
          col_stat->get_table_name() = param.table_identity_.tab_name_;
          col_stat->get_partition_value().reset(); // Global stat has empty partition value
          col_stat->get_column_name() = col_param.column_name_;
          col_stat->set_num_null(null_eval.get());
          col_stat->set_num_not_null(not_null_eval.get());
          col_stat->set_num_distinct(ndv_eval.get());
          col_stat->set_avg_length(avglen_eval.get());
          col_stat->set_min_value(min_eval.get());
          col_stat->set_max_value(max_eval.get());
          col_stat->set_collation_type(cs_type);

          // Allocate and set merged llc_bitmap for global stat
          char *global_bitmap = NULL;
          if (OB_ISNULL(global_bitmap = static_cast<char *>(param.allocator_->alloc(
                            share::ObOptCatalogColumnStat::NUM_BITMAP_BUCKET)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory for global bitmap", K(ret));
          } else {
            ndv_eval.get_llc_bitmap(global_bitmap,
                                    share::ObOptCatalogColumnStat::NUM_BITMAP_BUCKET);
            col_stat->set_llc_bitmap(global_bitmap,
                                     share::ObOptCatalogColumnStat::NUM_BITMAP_BUCKET);
          }

          if (OB_SUCC(ret)) {
            if (OB_FAIL(global_stat.column_stats_.push_back(col_stat))) {
              LOG_WARN("failed to push back column stat", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObCatalogIncrementalStatEstimator::prepare_catalog_get_opt_stats_param(
    const ObCatalogTableStatParam &param,
    const bool derive_part_stat,
    ObCatalogTableStatParam &new_param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(new_param.assign(param))) {
    LOG_WARN("failed to assign catalog stat param", K(ret));
  } else {
    // For catalog tables, reset partition info list for reading
    new_param.part_infos_.reset();

    if (!derive_part_stat) {
      // Read all partition stats for global derivation
      if (OB_FAIL(new_param.part_infos_.assign(param.all_part_infos_))) {
        LOG_WARN("failed to assign partition infos", K(ret));
      }
    }
    // derive_part_stat = true: For catalog tables without subpartitions,
    // this path is not typically used, so no additional processing needed
  }
  return ret;
}

int ObCatalogIncrementalStatEstimator::generate_all_catalog_opt_stat(
    const ObIArray<share::ObOptCatalogTableStat *> &table_stats,
    const ObIArray<share::ObOptCatalogColumnStat *> &column_stats,
    const int64_t col_cnt,
    ObIArray<ObOptCatalogStat> &all_opt_stats)
{
  int ret = OB_SUCCESS;
  // For catalog tables, column stats are laid out as: table[0]->col[0..col_cnt-1],
  // table[1]->col[col_cnt..2*col_cnt-1], etc.
  for (int64_t i = 0; OB_SUCC(ret) && i < table_stats.count(); ++i) {
    ObOptCatalogStat opt_stat;
    if (OB_ISNULL(table_stats.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("table stat is null", K(ret), K(i));
    } else {
      opt_stat.table_stat_ = table_stats.at(i);
      // Match column stats for this partition
      for (int64_t j = 0; OB_SUCC(ret) && j < col_cnt; ++j) {
        int64_t col_idx = i * col_cnt + j;
        if (OB_UNLIKELY(col_idx >= column_stats.count())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column index out of bounds", K(ret), K(col_idx), K(column_stats.count()));
        } else if (OB_ISNULL(column_stats.at(col_idx))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column stat is null", K(ret), K(col_idx));
        } else if (OB_FAIL(opt_stat.column_stats_.push_back(column_stats.at(col_idx)))) {
          LOG_WARN("failed to push back column stat", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(opt_stat.column_stats_.count() != col_cnt)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("column count mismatch", K(ret), K(opt_stat.column_stats_.count()), K(col_cnt));
        } else if (OB_FAIL(all_opt_stats.push_back(opt_stat))) {
          LOG_WARN("failed to push back opt stat", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObCatalogIncrementalStatEstimator::do_derive_catalog_global_stat(
    const ObCatalogTableStatParam &param,
    const ObIArray<ObOptCatalogStat> &part_opt_stats,
    ObOptCatalogStat &out_stat)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(part_opt_stats.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition opt stats is empty", K(ret));
  } else if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    // Derive global table stat from partition stats
    if (OB_FAIL(derive_global_tbl_stat(param, part_opt_stats, out_stat))) {
      LOG_WARN("failed to derive global table stat", K(ret));
    } else if (OB_FAIL(derive_global_col_stat(param, part_opt_stats, out_stat))) {
      LOG_WARN("failed to derive global column stat", K(ret));
    }
  }
  return ret;
}

int ObCatalogIncrementalStatEstimator::derive_split_gather_stats(
    sql::ObExecContext &ctx,
    ObMySQLTransaction &trans,
    const ObCatalogTableStatParam &param,
    ObOptStatGatherAudit *audit,
    bool derive_part_stat,
    bool is_all_columns_gather,
    ObIArray<share::ObOptCatalogTableStat *> &all_tstats)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("allocator is null", K(ret));
  } else {
    // Step 1: Prepare param with gather flags reset
    ObCatalogTableStatParam new_param;
    ObArenaAllocator allocator("CatDrvStats", OB_MALLOC_NORMAL_BLOCK_SIZE, param.table_identity_.tenant_id_);
    ObSEArray<share::ObOptCatalogTableStat *, 4> cur_table_stats;
    ObSEArray<share::ObOptCatalogColumnStat *, 4> cur_column_stats;
    ObSEArray<ObOptCatalogStat, 4> part_opt_stats;
    ObSEArray<ObOptCatalogStat, 4> derive_opt_stats;

    if (OB_FAIL(prepare_catalog_get_opt_stats_param(param, derive_part_stat, new_param))) {
      LOG_WARN("failed to prepare catalog stat param", K(ret));
    }
    // Step 2: Get current opt stats from database
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::get_current_opt_stats(allocator,
                                                                      trans.get_connection(),
                                                                      new_param,
                                                                      cur_table_stats,
                                                                      cur_column_stats))) {
      LOG_WARN("failed to get current opt stats", K(ret));
    } else {
      int64_t fetched_part_cnt = 0;
      int64_t fetched_row_sum = 0;
      for (int64_t i = 0; i < cur_table_stats.count(); ++i) {
        if (OB_NOT_NULL(cur_table_stats.at(i))) {
          fetched_part_cnt++;
          fetched_row_sum += cur_table_stats.at(i)->get_row_count();
        }
      }
    }
    // Step 3: Combine table and column stats into ObOptCatalogStat array
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(generate_all_catalog_opt_stat(cur_table_stats,
                                                     cur_column_stats,
                                                     param.column_params_.count(),
                                                     part_opt_stats))) {
      LOG_WARN("failed to generate all catalog opt stat", K(ret));
    } else {
      int64_t part_row_sum = 0;
      for (int64_t i = 0; i < part_opt_stats.count(); ++i) {
        if (OB_NOT_NULL(part_opt_stats.at(i).table_stat_)) {
          part_row_sum += part_opt_stats.at(i).table_stat_->get_row_count();
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (derive_part_stat) {
      // For catalog tables, this path is not typically used (no subpartitions)
      LOG_WARN("catalog table derive_part_stat path not fully implemented", K(ret));
    } else {
      // Derive global stat from partition stats
      ObOptCatalogStat global_opt_stat;
      if (OB_FAIL(do_derive_catalog_global_stat(param, part_opt_stats, global_opt_stat))) {
        LOG_WARN("failed to derive catalog global stat", K(ret));
      } else if (OB_FAIL(derive_opt_stats.push_back(global_opt_stat))) {
        LOG_WARN("failed to push back global opt stat", K(ret));
      }
    }

    // Step 5-7: Classify and write stats (aligned with internal table flow)
    if (OB_FAIL(ret)) {
    } else {
      ObSEArray<share::ObOptCatalogTableStat *, 4> tmp_all_tstats;
      ObSEArray<share::ObOptCatalogColumnStat *, 4> all_cstats;
      int64_t start_time = ObTimeUtility::current_time();
      if (OB_FAIL(ObDbmsCatalogStatsUtils::classify_catalog_opt_stat(derive_opt_stats,
                                                                     tmp_all_tstats,
                                                                     all_cstats))) {
        LOG_WARN("failed to classify opt stat", K(ret));
      } else {
        int64_t cur_row_cnt = 0;
        if (tmp_all_tstats.count() > 0 && OB_NOT_NULL(tmp_all_tstats.at(0))) {
          cur_row_cnt = tmp_all_tstats.at(0)->get_row_count();
        }
        int64_t all_row_cnt = 0;
        if (all_tstats.count() > 0 && OB_NOT_NULL(all_tstats.at(0))) {
          all_row_cnt = all_tstats.at(0)->get_row_count();
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        // derive_global_tbl_stat computes the full avg_row_len (weighted average from all
        // partitions' complete avg_row_len). Reset before merge to avoid accumulation across
        // column batches — unlike direct-gather where each batch contributes partial avg_row_len.
        for (int64_t i = 0; i < all_tstats.count(); ++i) {
          if (OB_NOT_NULL(all_tstats.at(i))) {
            all_tstats.at(i)->set_avg_row_len(0);
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDbmsCatalogStatsUtils::merge_split_gather_tab_stats(all_tstats,
                                                                               tmp_all_tstats))) {
        LOG_WARN("failed to merge split gather tab stats", K(ret));
      } else {
        int64_t merged_row_cnt = 0;
        if (all_tstats.count() > 0 && OB_NOT_NULL(all_tstats.at(0))) {
          merged_row_cnt = all_tstats.at(0)->get_row_count();
        }
      }
      ObSEArray<share::ObOptCatalogTableStat *, 1> empty_tstats;
      ObIArray<share::ObOptCatalogTableStat *> *flush_tstats = nullptr;
      // In split global-derive mode, table-level global stat is invariant across column batches.
      // Skip intermediate table flushes and flush table stat only in the last column batch.
      if (!derive_part_stat && !is_all_columns_gather) {
        flush_tstats = &empty_tstats;
      } else {
        flush_tstats = is_all_columns_gather ? &all_tstats : &tmp_all_tstats;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(ObDbmsCatalogStatsUtils::split_batch_write(param,
                                                                    ctx,
                                                                    trans.get_connection(),
                                                                    *flush_tstats,
                                                                    all_cstats))) {
        LOG_WARN("failed to split batch write", K(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (OB_NOT_NULL(audit)
                 && OB_FAIL(
                     audit->add_flush_stats_audit(ObTimeUtility::current_time() - start_time))) {
        LOG_WARN("failed to add flush stats audit", K(ret));
      }
    }

    LOG_TRACE("succeed to derive split gather stats for catalog table",
              K(param),
              K(derive_part_stat),
              K(part_opt_stats.count()));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_dbms_catalog_stats_executor.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/catalog/ob_catalog_incremental_stat_estimator.h"
#include "share/stat/ob_opt_stat_manager.h"
#include "sql/engine/ob_exec_context.h"
#include "share/schema/ob_schema_struct.h"
#include "sql/ob_sql_context.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_gather.h"
#include "share/stat/catalog/ob_opt_catalog_stat_sql_service.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_preferences.h"

namespace oceanbase
{
namespace common
{
using namespace sql;

ObDbmsCatalogStatsExecutor::ObDbmsCatalogStatsExecutor()
{
}

int ObDbmsCatalogStatsExecutor::get_catalog_stats_collect_batch_size(
    ObMySQLProxy *mysql_proxy,
    const ObCatalogTableIdentity &table_identity,
    int64_t &batch_part_size)
{
  int ret = OB_SUCCESS;
  ObObj result;
  ObString opt_name("CATALOG_STATS_BATCH_SIZE");
  ObArenaAllocator tmp_alloc("OptCatPrefs",
                             OB_MALLOC_NORMAL_BLOCK_SIZE,
                             table_identity.tenant_id_);
  if (OB_FAIL(ObDbmsCatalogStatsPreferences::get_prefs(mysql_proxy,
                                                       tmp_alloc,
                                                       table_identity,
                                                       opt_name,
                                                       result))) {
    LOG_WARN("failed to get catalog prefs", K(ret), K(table_identity), K(opt_name));
  } else if (!result.is_null()) {
    ObCastCtx cast_ctx(&tmp_alloc, NULL, CM_NONE, ObCharset::get_system_collation());
    ObObj dest_obj;
    if (OB_FAIL(ObObjCaster::to_type(ObNumberType, cast_ctx, result, dest_obj))) {
      LOG_WARN("failed to cast number", K(ret));
    } else if (OB_FAIL(dest_obj.get_number().extract_valid_int64_with_trunc(batch_part_size))) {
      LOG_WARN("failed to extract batch size", K(ret), K(result));
    } else if (batch_part_size < 0) {
      ret = OB_ERR_DBMS_STATS_PL;
      LOG_WARN("illegal catalog gather stats batch size", K(ret), K(batch_part_size));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsExecutor::gather_table_stats(const ObCatalogTableStatParam &param,
                                                   ObExecContext &ctx,
                                                   ObOptStatRunningMonitor &running_monitor)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather catalog table stats", K(param));

  CatalogGatherHelper gather_helper(running_monitor);
  ObSEArray<ObString, 4> failed_partition_names;

  // Step 1: Prepare gather helper with batch configuration
  if (OB_FAIL(prepare_gather_stats(ctx, const_cast<ObCatalogTableStatParam &>(param), gather_helper))) {
    LOG_WARN("failed to prepare gather stats", K(ret));
  // Step 2: Gather partition and global stats
  } else if (OB_FAIL(gather_partition_stats(ctx, param, gather_helper, failed_partition_names))) {
    LOG_WARN("failed to gather stats", K(ret), K(failed_partition_names));
    // Step 3: Handle timeout - derive global stats from already-collected partition stats
    if (OB_TIMEOUT == ret) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = derive_global_stats_on_timeout(ctx, param, gather_helper))) {
        LOG_WARN("fail to derive global stats on timeout", K(tmp_ret));
      }
    }
  }

  ObOptStatGatherAudit &audit = gather_helper.running_monitor_.audit_;
  LOG_INFO("catalog gather timing summary",
           K(ret),
           K(param.table_identity_),
           K(audit));

  return ret;
}

int ObDbmsCatalogStatsExecutor::prepare_gather_stats(ObExecContext &ctx,
                                                     ObCatalogTableStatParam &param,
                                                     CatalogGatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  int64_t origin_column_cnt = param.get_need_gather_column();
  int64_t partition_cnt = param.part_infos_.count();
  int64_t batch_part_size = DEFAULT_CATALOG_GATHER_PART_BATCH_SIZE;

  if (OB_FAIL(get_catalog_stats_collect_batch_size(ctx.get_sql_proxy(),
                                                   param.table_identity_,
                                                   batch_part_size))) {
    LOG_WARN("failed to get catalog gather batch size", K(ret), K(param.table_identity_));
  }

  if (OB_FAIL(ret)) {
  } else if (batch_part_size == 0) {
    gather_helper.maximum_gather_part_cnt_ = partition_cnt > 0 ? partition_cnt : 1;
  } else {
    gather_helper.maximum_gather_part_cnt_
        = partition_cnt > 0 ? std::min(partition_cnt, batch_part_size) : 1;
  }
  // Iceberg partitions can evolve, so partition-level NDV is unreliable.
  // Skip partition-level collection and only gather global stats.
  // normalize_iceberg_gather_param_to_table_level() handles the actual param reset.
  if (OB_SUCC(ret) && share::ObLakeTableFormat::ICEBERG == param.external_info_.lake_table_format_
      && param.part_level_ != share::schema::PARTITION_LEVEL_ZERO
      && partition_cnt > 0) {
    param.part_level_ = share::schema::PARTITION_LEVEL_ZERO;
    param.part_stat_param_.need_modify_ = false;
    param.global_stat_param_.need_modify_ = true;
    partition_cnt = 0;
  }
  gather_helper.maximum_gather_col_cnt_
      = std::min(origin_column_cnt,
                 static_cast<int64_t>(MAX_GATHER_COLUMN_COUNT_PER_QUERY_FOR_LARGE_TENANT));
  gather_helper.is_split_gather_ = gather_helper.maximum_gather_part_cnt_ < partition_cnt
                                   || gather_helper.maximum_gather_col_cnt_ < origin_column_cnt;
  // Specific format type and block sample to sample on one column scenrio.
  sql::ObExternalFileFormat::FormatType format_type = param.external_info_.format_type_;
  bool is_block_sample = param.gather_options_.sample_info_.is_block_family_sample();
  if (is_block_sample
      && (sql::ObExternalFileFormat::PARQUET_FORMAT == format_type
          || sql::ObExternalFileFormat::ORC_FORMAT == format_type)) {
    gather_helper.maximum_gather_col_cnt_ = 1;
    gather_helper.is_split_gather_ = gather_helper.maximum_gather_part_cnt_ < partition_cnt
                                     || gather_helper.maximum_gather_col_cnt_ < origin_column_cnt;
  }

  // For Parquet/ORC format, enable refine min/max using file metadata.
  // FILE sample scans selected files fully, so min/max are already precise — skip refine.
  if (!param.gather_options_.sample_info_.is_file_sample()
      && (param.external_info_.format_type_ == sql::ObExternalFileFormat::PARQUET_FORMAT
          || param.external_info_.format_type_ == sql::ObExternalFileFormat::ORC_FORMAT)) {
    bool has_refine_column = false;
    for (int64_t i = 0; i < param.column_params_.count(); ++i) {
      ObCatalogColumnStatParam &column_param = param.column_params_.at(i);
      if (column_param.need_basic_stat()
          && ObCatalogColumnStatParam::is_valid_refine_min_max_type(column_param.column_type_)) {
        column_param.set_need_refine_min_max();
        has_refine_column = true;
        LOG_TRACE("set need refine min/max for column", K(column_param.column_name_));
      }
    }
    param.gather_options_.need_refine_min_max_ = has_refine_column;
  }

  LOG_TRACE("prepare gather stats for catalog table",
            K(partition_cnt), K(origin_column_cnt), K(gather_helper));
  return ret;
}

int ObDbmsCatalogStatsExecutor::gather_partition_stats(ObExecContext &ctx,
                                                       const ObCatalogTableStatParam &param,
                                                       CatalogGatherHelper &gather_helper,
                                                       ObIArray<ObString> &failed_partition_names)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather partition stats for catalog table", K(param));

  typedef ObSEArray<CatalogTaskColumnParam, 2> TaskColumnParamInfo;
  typedef ObSEArray<CatalogGatherPartInfos, 2> BatchTaskPartInfo;

  SMART_VARS_2((BatchTaskPartInfo, batch_part_infos), (TaskColumnParamInfo, batch_col_infos))
  {
    if (OB_FAIL(split_catalog_part_param(param, gather_helper, batch_part_infos))) {
      LOG_WARN("failed to split partition param", K(ret));
    } else if (OB_FAIL(split_catalog_column_param(param, gather_helper, batch_col_infos))) {
      LOG_WARN("failed to split column param", K(ret));
    } else {
      // Process each partition batch
      // Initialize global stats with filtered partitions' statistics
      // These are partitions that were skipped during filtering phase due to unchanged modification time
      int64_t gloabl_file_count = param.filtered_stats_.filtered_file_count_;
      int64_t gloabl_data_size = param.filtered_stats_.filtered_data_size_;
      int64_t global_last_analyzed = param.filtered_stats_.filtered_last_analyzed_;
      int64_t global_schema_version = param.filtered_stats_.filtered_schema_version_;
      int64_t batch_start_time = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < batch_part_infos.count(); ++i) {
        SMART_VAR(ObCatalogTableStatParam, derive_param)
        {
          ObMySQLTransaction gather_trans;
          CatalogGatherPartInfos &task_info = batch_part_infos.at(i);

          if (task_info.is_invalid_task()) {
            // Skip invalid task
            continue;
          }
          batch_start_time = ObTimeUtility::current_time();

          // Initialize derive_param for this batch
          if (OB_FAIL(gather_trans.start(ctx.get_sql_proxy(), param.table_identity_.tenant_id_))) {
            LOG_WARN("fail to start transaction", K(ret));
          } else if (OB_FAIL(derive_param.assign(param))) {
            LOG_WARN("failed to assign param", K(ret));
          } else if (OB_FAIL(derive_param.part_infos_.assign(task_info.part_infos_))) {
            LOG_WARN("failed to assign part infos", K(ret));
          } else {
            // Set gather_global flag from task_info
            derive_param.global_stat_param_.need_modify_ = task_info.gather_global_;
          }

          // Execute gather stats
          if (OB_FAIL(ret)) {
            // Skip if initialization failed
          } else if (!task_info.gather_global_) {
            if (OB_FAIL(do_split_part_gather_stats(ctx,
                                                   gather_trans,
                                                   task_info,
                                                   batch_col_infos,
                                                   derive_param,
                                                   gather_helper,
                                                   gloabl_file_count,
                                                   gloabl_data_size,
                                                   global_last_analyzed,
                                                   global_schema_version))) {
              LOG_WARN("failed to do split part gather stats", K(ret));
            }
          } else if (task_info.gather_global_) {
            if (OB_FAIL(do_split_global_gather_stats(ctx,
                                                     gather_trans,
                                                     task_info,
                                                     batch_col_infos,
                                                     gloabl_file_count,
                                                     gloabl_data_size,
                                                     global_last_analyzed,
                                                     global_schema_version,
                                                     derive_param,
                                                     gather_helper))) {
              LOG_WARN("failed to do split global gather stats", K(ret));
            }
          }

          // End gather transaction
          if (OB_SUCC(ret)) {
            if (OB_FAIL(gather_trans.end(true))) {
              LOG_WARN("fail to commit transaction", K(ret));
            } else {
              LOG_TRACE("catalog stats batch completed",
                        K(i),
                        K(batch_part_infos.count()),
                        K(task_info.part_infos_.count()),
                        K(task_info.gather_global_),
                        K(param.table_identity_),
                        "batch_elapsed_s",
                        static_cast<double>(ObTimeUtility::current_time() - batch_start_time)
                            / 1000000.0);
            }
          } else {
            int tmp_ret = OB_SUCCESS;
            if (OB_SUCCESS != (tmp_ret = gather_trans.end(false))) {
              LOG_WARN("fail to roll back transaction", K(tmp_ret));
            } else {
              LOG_TRACE("catalog stats batch failed",
                        K(i),
                        K(batch_part_infos.count()),
                        K(task_info.part_infos_.count()),
                        K(task_info.gather_global_),
                        K(param.table_identity_),
                        "batch_elapsed_s",
                        static_cast<double>(ObTimeUtility::current_time() - batch_start_time)
                            / 1000000.0);
            }
          }

          // Handle failure: collect failed partition names for error reporting
          if (OB_FAIL(ret)) {
            int tmp_ret = ret;
            if (OB_SUCCESS != (ret = collect_failed_partition_names(derive_param, failed_partition_names))) {
              LOG_WARN("failed to collect failed partition names", K(ret));
            }
            ret = tmp_ret;
          }

          // Reset split column flags for next batch
          gather_helper.is_split_column_ = false;
          gather_helper.is_all_col_gathered_ = false;
        }
      }
    }
  }

  return ret;
}

int ObDbmsCatalogStatsExecutor::split_catalog_part_param(
    const ObCatalogTableStatParam &param,
    const CatalogGatherHelper &gather_helper,
    ObIArray<CatalogGatherPartInfos> &batch_part_infos)
{
  int ret = OB_SUCCESS;

  if (param.part_stat_param_.need_modify_
      && param.part_level_ != share::schema::PARTITION_LEVEL_ZERO) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
      if (i % gather_helper.maximum_gather_part_cnt_ == 0) {
        CatalogGatherPartInfos new_gather_info;
        if (OB_FAIL(batch_part_infos.push_back(new_gather_info))) {
          LOG_WARN("fail to push back gather info", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(batch_part_infos.at(batch_part_infos.count() - 1)
                     .part_infos_.push_back(param.part_infos_.at(i)))) {
        LOG_WARN("failed to push back part info", K(ret));
      }
    }
  }

  // If no partition batches created, create one for global stats only
  if (OB_SUCC(ret) && param.global_stat_param_.need_modify_) {
    CatalogGatherPartInfos global_info;
    global_info.gather_global_ = true;
    // For non-partitioned table (PARTITION_LEVEL_ZERO), carry part_infos_ for modify_ts_
    if (param.part_level_ == share::schema::PARTITION_LEVEL_ZERO
        && OB_FAIL(global_info.part_infos_.assign(param.part_infos_))) {
      LOG_WARN("failed to assign part infos for non-part table", K(ret));
    } else if (OB_FAIL(batch_part_infos.push_back(global_info))) {
      LOG_WARN("fail to push back global gather info", K(ret));
    }
  }

  LOG_TRACE("split catalog part param done",
            K(param.part_infos_.count()), K(batch_part_infos.count()));
  return ret;
}

int ObDbmsCatalogStatsExecutor::split_catalog_column_param(
    const ObCatalogTableStatParam &param,
    const CatalogGatherHelper &gather_helper,
    ObIArray<CatalogTaskColumnParam> &batch_col_infos)
{
  int ret = OB_SUCCESS;
  int64_t column_cnt = param.column_params_.count();
  int64_t max_col_per_batch = gather_helper.maximum_gather_col_cnt_;

  if (column_cnt <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column count is invalid", K(ret), K(column_cnt));
  } else {
    int64_t start = 0;
    while (OB_SUCC(ret) && start < column_cnt) {
      CatalogTaskColumnParam col_param;
      col_param.column_params_ = &param.column_params_;
      col_param.start_ = static_cast<int32_t>(start);
      col_param.end_ = static_cast<int32_t>(std::min(start + max_col_per_batch, column_cnt));

      if (OB_FAIL(batch_col_infos.push_back(col_param))) {
        LOG_WARN("failed to push back column param", K(ret));
      } else {
        start = col_param.end_;
      }
    }
  }

  LOG_TRACE("split catalog column param done",
            K(column_cnt), K(max_col_per_batch), K(batch_col_infos.count()));
  return ret;
}

int ObDbmsCatalogStatsExecutor::do_split_part_gather_stats(
    ObExecContext &ctx,
    ObMySQLTransaction &trans,
    const CatalogGatherPartInfos &task_info,
    const ObIArray<CatalogTaskColumnParam> &batch_col_infos,
    ObCatalogTableStatParam &derived_param,
    CatalogGatherHelper &gather_helper,
    int64_t &global_file_count,
    int64_t &global_data_size,
    int64_t &global_last_analyzed,
    int64_t &global_schema_version)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("CatSPartStat",
                             OB_MALLOC_NORMAL_BLOCK_SIZE,
                             derived_param.table_identity_.tenant_id_);
  derived_param.allocator_ = &allocator;
  ObSEArray<ObOptCatalogTableStat *, 4> all_catalog_table_stats;

  if (task_info.gather_global_ || task_info.part_infos_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected global gather status in part gather",
             K(ret),
             K(task_info.gather_global_),
             K(task_info.part_infos_.empty()));
  }

  if (OB_FAIL(ret)) {
  } else if (batch_col_infos.count() > 1) {
    gather_helper.is_split_column_ = true;
  }

  if (OB_FAIL(ret)) {
  } else {
    all_catalog_table_stats.reset();

    if (OB_FAIL(ObDbmsCatalogStatsUtils::init_catalog_table_stats(task_info.part_infos_.count(),
                                                                  allocator,
                                                                  all_catalog_table_stats))) {
      LOG_WARN("failed to init table stats", K(ret));
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_col_infos.count(); j++) {
      const CatalogTaskColumnParam &col_param = batch_col_infos.at(j);
      gather_helper.is_all_col_gathered_ = (j == batch_col_infos.count() - 1);

      if (OB_FAIL(ObDbmsCatalogStatsUtils::assign_col_param(col_param.column_params_,
                                                            col_param.start_,
                                                            col_param.end_,
                                                            derived_param.column_params_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(
                ObOptStatRunningPhase::GATHER_PART_STATS))) {
          LOG_WARN("failed to add add monitor info", K(ret));
      } else if (OB_FAIL(do_gather_catalog_stats(ctx,
                                                 trans,
                                                 StatLevel::PARTITION_LEVEL,
                                                 task_info.part_infos_,
                                                 gather_helper,
                                                 derived_param,
                                                 all_catalog_table_stats))) {
          LOG_WARN("failed to do gather stats", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_catalog_table_stats.count(); ++i) {
      if (OB_NOT_NULL(all_catalog_table_stats.at(i))) {
        global_file_count += all_catalog_table_stats.at(i)->get_file_num();
        global_data_size += all_catalog_table_stats.at(i)->get_data_size();

        // Record the latest last_analyzed from all partitions for global stats
        int64_t part_last_analyzed = all_catalog_table_stats.at(i)->get_last_analyzed();
        if (part_last_analyzed > global_last_analyzed) {
          global_last_analyzed = part_last_analyzed;
        }
        int64_t part_schema_version = all_catalog_table_stats.at(i)->get_schema_version();
        if (part_schema_version > global_schema_version) {
          global_schema_version = part_schema_version;
        }

        // After collect file_count/data_size/last_analyzed, then clean it.
        all_catalog_table_stats.at(i)->~ObOptCatalogTableStat();
      }
    }
    allocator.reset();
  }
  return ret;
}

int ObDbmsCatalogStatsExecutor::do_split_global_gather_stats(
    ObExecContext &ctx,
    ObMySQLTransaction &trans,
    const CatalogGatherPartInfos &task_info,
    const ObIArray<CatalogTaskColumnParam> &batch_col_infos,
    const int64_t &global_file_count,
    const int64_t &global_data_size,
    const int64_t &global_last_analyzed,
    const int64_t &global_schema_version,
    ObCatalogTableStatParam &derived_param,
    CatalogGatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator("CatSGStat",
                             OB_MALLOC_NORMAL_BLOCK_SIZE,
                             derived_param.table_identity_.tenant_id_);
  derived_param.allocator_ = &allocator;
  ObSEArray<ObOptCatalogTableStat *, 4> all_catalog_table_stats;
  // For partitioned table, task_info.part_infos_ should be empty (global stats derived from part stats)
  // For non-partitioned table (PARTITION_LEVEL_ZERO), task_info.part_infos_ contains modify_ts_ info
  if (!task_info.gather_global_
      || (!task_info.part_infos_.empty()
          && derived_param.part_level_ != share::schema::PARTITION_LEVEL_ZERO)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part gather status in global gather",
             K(ret),
             K(task_info.gather_global_),
             K(task_info.part_infos_.empty()),
             K(derived_param.part_level_));
  }

  if (OB_FAIL(ret)) {
  } else if (batch_col_infos.count() > 1) {
    gather_helper.is_split_column_ = true;
  }

  if (OB_FAIL(ret)) {
  } else {
    all_catalog_table_stats.reset();
    ObString temp_db_name;
    ObString temp_table_name;

    gather_helper.is_approx_gather_ = derived_param.global_stat_param_.gather_approx_;
    LOG_TRACE("do_split_global_gather_stats mode",
              K(gather_helper.is_approx_gather_),
              K(derived_param.part_level_),
              "will_derive_from_part",
              gather_helper.is_approx_gather_
                  && derived_param.part_level_ != share::schema::PARTITION_LEVEL_ZERO);
    if (OB_FAIL(ObDbmsCatalogStatsUtils::init_catalog_table_stats(1,
                                                                  allocator,
                                                                  all_catalog_table_stats))) {
      LOG_WARN("failed to init table stats", K(ret));
    } else if (1 != all_catalog_table_stats.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected count for gloabl table stat",
               K(ret),
               K(all_catalog_table_stats.count()));
    } else if (OB_FAIL(ob_write_string(allocator, derived_param.table_identity_.db_name_, temp_db_name))
               || OB_FAIL(ob_write_string(allocator, derived_param.table_identity_.tab_name_, temp_table_name))) {
      LOG_WARN("failed to setup temp db/table name", K(ret), K(derived_param));
    } else if (OB_NOT_NULL(all_catalog_table_stats.at(0))) {
      // Initialize the object as valid first, so it won't be completely replaced during merge
      all_catalog_table_stats.at(0)->set_tenant_id(derived_param.table_identity_.tenant_id_);
      all_catalog_table_stats.at(0)->set_catalog_id(derived_param.table_identity_.catalog_id_);
      // Setup writing temp db/table name to avoid nullptr.
      all_catalog_table_stats.at(0)->set_database_name(temp_db_name);
      all_catalog_table_stats.at(0)->set_table_name(temp_table_name);
      // Set empty partition_value for global stats
      all_catalog_table_stats.at(0)->set_partition_value(ObString::make_empty_string());

      all_catalog_table_stats.at(0)->set_file_num(global_file_count);
      all_catalog_table_stats.at(0)->set_data_size(global_data_size);
      // For partitioned tables, set last_analyzed to the latest partition's time
      // merge_split_gather_tab_stats will take max(this, gathered), so this will be used
      all_catalog_table_stats.at(0)->set_last_analyzed(global_last_analyzed);
      all_catalog_table_stats.at(0)->set_schema_version(global_schema_version);
    }
    for (int64_t j = 0; OB_SUCC(ret) && j < batch_col_infos.count(); j++) {
      const CatalogTaskColumnParam &col_param = batch_col_infos.at(j);
      gather_helper.is_all_col_gathered_ = (j == batch_col_infos.count() - 1);
      if (OB_FAIL(ObDbmsCatalogStatsUtils::assign_col_param(col_param.column_params_,
                                                            col_param.start_,
                                                            col_param.end_,
                                                            derived_param.column_params_))) {
        LOG_WARN("failed to assign", K(ret));
      } else if (gather_helper.is_approx_gather_
                 && derived_param.part_level_ != share::schema::PARTITION_LEVEL_ZERO) {
        // Derive global stats from partition stats (approx gather mode)
        int64_t derive_start_time = ObTimeUtility::current_time();
        if (OB_FAIL(ObCatalogIncrementalStatEstimator::derive_split_gather_stats(
                ctx,
                trans,
                derived_param,
                &gather_helper.running_monitor_.audit_,
                false /*derive_part_stat*/,
                gather_helper.is_all_col_gathered_,
                all_catalog_table_stats))) {
          LOG_WARN("failed to derive split gather stats", K(ret));
        } else {
          gather_helper.running_monitor_.audit_.acc_catalog_global_derive_time_
              += ObTimeUtility::current_time() - derive_start_time;
        }
      } else {
        // Direct gather global stats (non-approx mode)
        // For non-partitioned table (PARTITION_LEVEL_ZERO), use derived_param.part_infos_ which
        // contains the modify_ts_ for setting last_analyzed correctly
        int64_t global_direct_start_time = ObTimeUtility::current_time();
        if (OB_FAIL(gather_helper.running_monitor_.add_monitor_info(
                ObOptStatRunningPhase::GATHER_GLOBAL_STATS))) {
          LOG_WARN("failed to add add monitor info", K(ret));
        } else if (OB_FAIL(do_gather_catalog_stats(ctx,
                                                   trans,
                                                   StatLevel::TABLE_LEVEL,
                                                   derived_param.part_infos_,
                                                   gather_helper,
                                                   derived_param,
                                                   all_catalog_table_stats))) {
          LOG_WARN("failed to do gather stats", K(ret));
        } else {
          gather_helper.running_monitor_.audit_.acc_catalog_global_direct_time_
              += ObTimeUtility::current_time() - global_direct_start_time;
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < all_catalog_table_stats.count(); ++i) {
      if (NULL != all_catalog_table_stats.at(i)) {
        all_catalog_table_stats.at(i)->~ObOptCatalogTableStat();
      }
    }
    allocator.reuse();
  }
  return ret;
}

int ObDbmsCatalogStatsExecutor::collect_failed_partition_names(
    const ObCatalogTableStatParam &param,
    ObIArray<ObString> &failed_partition_names)
{
  int ret = OB_SUCCESS;
  // Collect partition names that were being processed when failure occurred
  for (int64_t i = 0; OB_SUCC(ret) && i < param.part_infos_.count(); ++i) {
    if (OB_FAIL(failed_partition_names.push_back(param.part_infos_.at(i).partition_))) {
      LOG_WARN("failed to push back partition name", K(ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsExecutor::derive_global_stats_on_timeout(
    ObExecContext &ctx,
    const ObCatalogTableStatParam &origin_param,
    CatalogGatherHelper &gather_helper)
{
  int ret = OB_SUCCESS;
  if (!origin_param.global_stat_param_.need_modify_
      || origin_param.part_level_ == share::schema::PARTITION_LEVEL_ZERO) {
  } else {
    SMART_VAR(ObCatalogTableStatParam, derive_param)
    {
      ObArenaAllocator allocator("CatTODrvStat",
                                 OB_MALLOC_NORMAL_BLOCK_SIZE,
                                 origin_param.table_identity_.tenant_id_);
      ObMySQLTransaction trans;
      ObSEArray<share::ObOptCatalogTableStat *, 1> all_tstats;

      int64_t origin_timeout = THIS_WORKER.get_timeout_ts();
      sql::ObSQLSessionInfo *origin_session = THIS_WORKER.get_session();
      THIS_WORKER.set_session(NULL);
      THIS_WORKER.set_timeout_ts(10000000L + ObTimeUtility::current_time());

      if (OB_FAIL(derive_param.assign(origin_param))) {
        LOG_WARN("failed to assign param", K(ret));
      } else if (FALSE_IT(derive_param.allocator_ = &allocator)) {
      } else if (OB_FAIL(trans.start(ctx.get_sql_proxy(),
                                     origin_param.table_identity_.tenant_id_))) {
        LOG_WARN("fail to start transaction", K(ret));
      } else {
        int64_t derive_start_time = ObTimeUtility::current_time();
        if (OB_FAIL(ObCatalogIncrementalStatEstimator::derive_split_gather_stats(
                ctx,
                trans,
                derive_param,
                &gather_helper.running_monitor_.audit_,
                false /*derive_part_stat*/,
                true /*is_all_columns_gather*/,
                all_tstats))) {
          LOG_WARN("failed to derive global stats on timeout", K(ret));
        } else {
          gather_helper.running_monitor_.audit_.acc_catalog_global_derive_time_
              += ObTimeUtility::current_time() - derive_start_time;
        }

        if (OB_SUCC(ret)) {
          if (OB_FAIL(trans.end(true))) {
            LOG_WARN("fail to commit transaction", K(ret));
          }
        } else {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
            LOG_WARN("fail to roll back transaction", K(tmp_ret));
          }
        }
      }

      THIS_WORKER.set_session(origin_session);
      THIS_WORKER.set_timeout_ts(origin_timeout);
    }
  }
  return ret;
}

int ObDbmsCatalogStatsExecutor::do_gather_catalog_stats(
    ObExecContext &ctx,
    ObMySQLTransaction &trans,
    StatLevel stat_level,
    const ObIArray<ObCatalogExtPartitionInfo> &gather_partition_infos,
    CatalogGatherHelper &gather_helper,
    ObCatalogTableStatParam &derive_param,
    ObIArray<ObOptCatalogTableStat *> &all_tstats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptCatalogStat, 4> opt_stats;
  ObSEArray<ObOptCatalogColumnStat *, 4> all_cstats;
  ObArenaAllocator allocator("CatSplitGStat", OB_MALLOC_NORMAL_BLOCK_SIZE, derive_param.table_identity_.tenant_id_);
  SMART_VAR(ObOptCatalogStatGatherParam, gather_param)
  {
    gather_param.allocator_ = &allocator;
    if (OB_UNLIKELY(!is_valid_tenant_id(derive_param.table_identity_.tenant_id_))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("tenant id invalid", KR(ret), K(derive_param.table_identity_.tenant_id_));
    } else if (OB_ISNULL(gather_param.allocator_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid args with null allocator", KR(ret));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::prepare_gather_stat_param(
                   derive_param,
                   stat_level,
                   true,
                   gather_helper.gather_vectorize_,
                   gather_param))) {
      LOG_WARN("failed to prepare gather stat param", K(ret));
    } else if (OB_FAIL(do_gather_catalog_stats(ctx,
                                               trans,
                                               gather_param,
                                               gather_partition_infos,
                                               derive_param.column_params_,
                                               gather_helper.is_all_col_gathered_,
                                               gather_helper.running_monitor_.audit_,
                                               opt_stats,
                                               all_tstats,
                                               all_cstats))) {
      LOG_WARN("failed to do gather catalog stats", K(ret));
    }
  }
  return ret;
}

int ObDbmsCatalogStatsExecutor::do_gather_catalog_stats(
    ObExecContext &ctx,
    ObMySQLTransaction &trans,
    ObOptCatalogStatGatherParam &param,
    const ObIArray<ObCatalogExtPartitionInfo> &gather_partition_infos,
    const ObIArray<ObCatalogColumnStatParam> &gather_column_params,
    bool is_all_columns_gather,
    ObOptStatGatherAudit &audit,
    ObIArray<ObOptCatalogStat> &opt_stats,
    ObIArray<ObOptCatalogTableStat *> &all_tstats,
    ObIArray<ObOptCatalogColumnStat *> &all_cstats)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObOptCatalogTableStat *, 4> tmp_all_tstats;
  int64_t start_time = 0;
  if (OB_FAIL(THIS_WORKER.check_status())) {
    LOG_WARN("check status failed", KR(ret));
  } else if (OB_FAIL(param.partition_infos_.assign(gather_partition_infos))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(param.column_params_.assign(gather_column_params))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsGather::gather_stats(param, ctx, audit, opt_stats))) {
    LOG_WARN("failed to gather stats", K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::classify_catalog_opt_stat(opt_stats,
                                                                        tmp_all_tstats,
                                                                        all_cstats))) {
    LOG_WARN("failed to classify opt stat", K(ret));
  } else if (OB_FALSE_IT(start_time = ObTimeUtility::current_time())) {
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::merge_split_gather_tab_stats(all_tstats,
                                                                           tmp_all_tstats))) {
    LOG_WARN("failed to merge split gather tab stats", K(ret));
  } else if (OB_FAIL(ObDbmsCatalogStatsUtils::sync_column_stat_last_analyzed(all_tstats,
                                                                             all_cstats))) {
    LOG_WARN("failed to sync column stat last analyzed", K(ret));
  } else {
    bool is_refine_write = param.sample_info_.is_specify_sample() && param.need_refine_min_max_;
    // Create a temporary ObCatalogTableStatParam for split_batch_write
    // This mirrors the internal table approach - construct parameter from gather_param which has allocator_
    ObCatalogTableStatParam temp_table_param;
    temp_table_param.table_identity_.tenant_id_ = param.table_identity_.tenant_id_;
    temp_table_param.table_identity_.catalog_id_ = param.table_identity_.catalog_id_;
    temp_table_param.table_identity_.catalog_name_ = param.table_identity_.catalog_name_;
    temp_table_param.table_identity_.db_name_ = param.table_identity_.db_name_;
    temp_table_param.table_identity_.tab_name_ = param.table_identity_.tab_name_;
    temp_table_param.allocator_ = param.allocator_;
    temp_table_param.part_level_ = param.stat_level_ == PARTITION_LEVEL
                                       ? share::schema::PARTITION_LEVEL_ONE
                                       : share::schema::PARTITION_LEVEL_ZERO;
    // Copy column_params from gather_param which has the correct allocator context
    if (OB_FAIL(temp_table_param.column_params_.assign(param.column_params_))) {
      LOG_WARN("failed to assign column params", K(ret));
    } else if (OB_FAIL(ObDbmsCatalogStatsUtils::split_batch_write(
                   temp_table_param,
                   ctx,
                   trans.get_connection(),
                   is_all_columns_gather ? all_tstats : tmp_all_tstats,
                   all_cstats))) {
      LOG_WARN("failed to split batch write", K(ret));
    } else {
      int64_t flush_elapsed = ObTimeUtility::current_time() - start_time;
      audit.acc_flush_time_ += flush_elapsed;
      if (OB_FAIL(audit.add_flush_stats_audit(flush_elapsed))) {
        LOG_WARN("failed to add flush stats audit", K(ret));
      }
    }
  }
  return ret;
}

} // namespace common
} // namespace oceanbase
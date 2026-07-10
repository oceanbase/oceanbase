/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_dbms_catalog_stats_gather.h"
#include "share/stat/catalog/ob_basic_catalog_stats_estimator.h"
#include "share/stat/catalog/ob_catalog_min_max_estimator.h"
#include "share/stat/catalog/ob_dbms_catalog_stats_utils.h"
#include "share/stat/catalog/ob_catalog_stat_define.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
using namespace pl;
namespace common
{

ObDbmsCatalogStatsGather::ObDbmsCatalogStatsGather()
{
}

int ObDbmsCatalogStatsGather::gather_stats(const ObOptCatalogStatGatherParam &param,
                                           ObExecContext &ctx,
                                           ObOptStatGatherAudit &audit,
                                           ObIArray<ObOptCatalogStat> &opt_catalog_stats)
{
  int ret = OB_SUCCESS;
  LOG_TRACE("begin to gather catalog table stats", K(param));
  if (OB_ISNULL(param.allocator_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected error", K(ret), K(param.allocator_));
  } else if (OB_FAIL(init_opt_catalog_stats(*param.allocator_, param, opt_catalog_stats))) {
    LOG_WARN("failed to init opt catalog stats", K(ret), K(param));
  } else if (OB_FAIL(adjust_sample_param(opt_catalog_stats, const_cast<ObOptCatalogStatGatherParam&>(param)))) {
    LOG_WARN("failed to adjust sample param", K(ret), K(param));
  } else if (opt_catalog_stats.empty()) {
    LOG_INFO("no stats to estimate", K(param.table_identity_.tab_name_));
  } else if (!opt_catalog_stats.empty()) {
    int64_t start_time = ObTimeUtility::current_time();
    int64_t basic_duration_time = 0;
    // TODO(bitao): add more estimators for other table formats, such as Delta Lake, Hudi, etc.
    ObBasicCatalogStatsEstimator basic_est(ctx, *param.allocator_);
    if (OB_FAIL(basic_est.estimate(param, opt_catalog_stats))) {
      LOG_WARN("failed to estimate basic catalog statistics", K(ret));
    }

    basic_duration_time = ObTimeUtility::current_time() - start_time;
    audit.acc_basic_est_time_ += basic_duration_time;
    if (OB_FAIL(ret)) {
    } else {
      // Add audit for catalog basic stats gathering
      ObSEArray<ObString, 4> partition_names;
      for (int64_t i = 0; OB_SUCC(ret) && i < param.partition_infos_.count(); ++i) {
        if (OB_FAIL(partition_names.push_back(param.partition_infos_.at(i).partition_))) {
          LOG_WARN("failed to push back partition name", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(audit.add_catalog_basic_estimate_audit(partition_names,
                                                                basic_duration_time))) {
        LOG_WARN("failed to add catalog basic estimate audit", K(ret));
      }
    }

    // 2. Refine min/max by reading file metadata (Parquet/ORC; Iceberg uses same path after basic gather)
    if (OB_FAIL(ret)) {
    } else if (param.sample_info_.is_specify_sample() && param.need_refine_min_max_) {
      start_time = ObTimeUtility::current_time();
      ObCatalogMinMaxEstimator min_max_est(ctx, *param.allocator_);
      if (OB_FAIL(min_max_est.estimate(param, opt_catalog_stats))) {
        LOG_WARN("failed to estimate min/max", K(ret));
      }

      int64_t refine_elapsed = ObTimeUtility::current_time() - start_time;
      audit.acc_refine_time_ += refine_elapsed;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(audit.add_catalog_refine_estimate_audit(param.table_identity_.catalog_id_,
                                                                 param.table_identity_.db_name_,
                                                                 param.table_identity_.tab_name_,
                                                                 ObString::make_empty_string(),
                                                                 refine_elapsed))) {
        LOG_WARN("failed to add catalog refine estimate audit", K(ret));
      }
    }
    // Note: Catalog table don't need histogram and use column store compared to INTERNAL TABLES.
  }

  return ret;
}

int ObDbmsCatalogStatsGather::init_opt_catalog_stats(ObIAllocator &allocator,
                                                     const ObOptCatalogStatGatherParam &param,
                                                     ObIArray<ObOptCatalogStat> &opt_catalog_stats)
{
  int ret = OB_SUCCESS;
  if (TABLE_LEVEL == param.stat_level_) {
    ObOptCatalogStat stat;
    if (OB_FAIL(init_opt_catalog_stat(allocator,
                                      param,
                                      ObString::make_empty_string(),
                                      param.stattype_,
                                      stat))) {
      LOG_WARN("failed to init opt stat", K(ret));
    } else if (OB_FAIL(opt_catalog_stats.push_back(stat))) {
      LOG_WARN("failed to push back stat", K(ret));
    }
  } else if (PARTITION_LEVEL == param.stat_level_) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param.partition_infos_.count(); ++i) {
      ObOptCatalogStat stat;
      if (OB_FAIL(init_opt_catalog_stat(allocator,
                                        param,
                                        param.partition_infos_.at(i).partition_,
                                        param.partition_infos_.at(i).part_stattype_,
                                        stat))) {
        LOG_WARN("failed to init opt stat");
      } else if (OB_FAIL(opt_catalog_stats.push_back(stat))) {
        LOG_WARN("failed to push back stat");
      } else { /*do nothing*/
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected stat level", K(ret), K(param.stat_level_));
  }
  return ret;
}

int ObDbmsCatalogStatsGather::init_opt_catalog_stat(ObIAllocator &allocator,
                                                    const ObOptCatalogStatGatherParam &param,
                                                    const ObString &part_name,
                                                    const int64_t part_stattype,
                                                    ObOptCatalogStat &stat)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;

  ObOptCatalogTableStat *&tab_stat = stat.table_stat_;
  if (OB_FAIL(stat.column_stats_.prepare_allocate(param.column_params_.count()))) {
    LOG_WARN("failed to prepare allocate column stat", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObOptCatalogTableStat)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("memory is not enough", K(ret), K(ptr));
  } else {
    tab_stat = new (ptr) ObOptCatalogTableStat();
    tab_stat->set_tenant_id(param.table_identity_.tenant_id_);
    tab_stat->set_catalog_id(param.table_identity_.catalog_id_);
    tab_stat->set_database_name(param.table_identity_.db_name_);
    tab_stat->set_table_name(param.table_identity_.tab_name_);
    tab_stat->set_partition_value(part_name);
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < param.column_params_.count(); ++i) {
    ObOptCatalogColumnStat *&col_stat = stat.column_stats_.at(i);
    if (OB_UNLIKELY(!param.column_params_.at(i).need_col_stat())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected error", K(ret), K(i), K(param));
    } else if (OB_ISNULL(col_stat = ObOptCatalogColumnStat::malloc_new_column_stat(allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("memory is not enough", K(ret), K(col_stat));
    } else if (OB_UNLIKELY(param.column_params_.at(i).column_name_.empty())) {
      // Validate column name is not empty to prevent invalid SQL generation
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("column name is empty in column parameter", K(ret), K(i), K(param.column_params_.at(i)));
    } else {
      col_stat->set_catalog_id(param.table_identity_.catalog_id_);
      col_stat->set_database_name(param.table_identity_.db_name_);
      col_stat->set_table_name(param.table_identity_.tab_name_);
      col_stat->set_partition_value(part_name);
      col_stat->set_column_name(param.column_params_.at(i).column_name_);
      col_stat->set_collation_type(param.column_params_.at(i).cs_type_);
    }
  }
  return ret;
}

int ObDbmsCatalogStatsGather::adjust_sample_param(
    const ObIArray<ObOptCatalogStat> &opt_catalog_stats,
    ObOptCatalogStatGatherParam &param)
{
  int ret = OB_SUCCESS;
  UNUSED(opt_catalog_stats);
  UNUSED(param);
  return ret;
}

int ObDbmsCatalogStatsGather::classfy_catalog_column_histogram(
    const ObOptCatalogStatGatherParam &param,
    ObOptCatalogStat &opt_stat)
{
  int ret = OB_NOT_SUPPORTED;
  UNUSED(param);
  UNUSED(opt_stat);
  LOG_WARN("catalog column histogram classification is not supported yet", K(ret));
  LOG_USER_ERROR(OB_NOT_SUPPORTED, "catalog column histogram");
  return ret;
}

} // namespace common
} // namespace oceanbase

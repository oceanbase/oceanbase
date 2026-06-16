/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "share/stat/catalog/ob_catalog_stat_define.h"

namespace oceanbase
{
namespace common
{

const int64_t ObCatalogColumnStatParam::DEFAULT_HISTOGRAM_BUCKET_NUM = 254;

int ObCatalogColumnStatParam::assign(const ObCatalogColumnStatParam &other)
{
  int ret = OB_SUCCESS;
  column_name_ = other.column_name_;
  cs_type_ = other.cs_type_;
  size_mode_ = other.size_mode_;
  bucket_num_ = other.bucket_num_;
  column_attribute_ = other.column_attribute_;
  gather_flag_ = other.gather_flag_;
  column_type_ = other.column_type_;
  ndv_scale_algo_ = other.ndv_scale_algo_;
  return ret;
}

bool ObCatalogColumnStatParam::is_valid_opt_col_type(const ObObjType type, bool is_online_stat)
{
  bool ret = false;
  // currently, we only support the following type to collect histogram
  ColumnTypeClass type_class = ob_obj_type_class(type);
  if (type_class == ColumnTypeClass::ObIntTC ||
      type_class == ColumnTypeClass::ObUIntTC ||
      type_class == ColumnTypeClass::ObFloatTC ||
      type_class == ColumnTypeClass::ObDoubleTC ||
      type_class == ColumnTypeClass::ObNumberTC ||
      type_class == ColumnTypeClass::ObDateTimeTC ||
      type_class == ColumnTypeClass::ObDateTC ||
      type_class == ColumnTypeClass::ObTimeTC ||
      type_class == ColumnTypeClass::ObYearTC ||
      type_class == ColumnTypeClass::ObStringTC ||
      type_class == ColumnTypeClass::ObRawTC ||
      type_class == ColumnTypeClass::ObOTimestampTC ||
      type_class == ColumnTypeClass::ObBitTC ||
      type_class == ColumnTypeClass::ObEnumSetTC ||
      type_class == ColumnTypeClass::ObIntervalTC ||
      type_class == ColumnTypeClass::ObDecimalIntTC ||
      (!is_online_stat && lib::is_mysql_mode() && type_class == ColumnTypeClass::ObTextTC) ||
      type_class == ColumnTypeClass::ObMySQLDateTC ||
      type_class == ColumnTypeClass::ObMySQLDateTimeTC) {
    ret = true;
  }
  return ret;
}

bool ObCatalogColumnStatParam::is_valid_avglen_type(const ObObjType type)
{
  bool ret = false;
  // currently, we only support the following type to collect avg len
  ColumnTypeClass type_class = ob_obj_type_class(type);
  if (is_valid_opt_col_type(type) ||
      type_class == ColumnTypeClass::ObTextTC ||
      type_class == ColumnTypeClass::ObRowIDTC ||
      type_class == ColumnTypeClass::ObLobTC) {
    ret = true;
  }
  return ret;
}

bool ObCatalogColumnStatParam::is_valid_refine_min_max_type(const ObObjType type)
{
  bool ret = false;
  ColumnTypeClass type_class = ob_obj_type_class(type);
  if (type_class == ColumnTypeClass::ObIntTC ||
      type_class == ColumnTypeClass::ObUIntTC ||
      type_class == ColumnTypeClass::ObFloatTC ||
      type_class == ColumnTypeClass::ObDoubleTC ||
      type_class == ColumnTypeClass::ObNumberTC ||
      type_class == ColumnTypeClass::ObDateTimeTC ||
      type_class == ColumnTypeClass::ObDateTC ||
      type_class == ColumnTypeClass::ObTimeTC ||
      type_class == ColumnTypeClass::ObYearTC ||
      type_class == ColumnTypeClass::ObOTimestampTC ||
      type_class == ColumnTypeClass::ObDecimalIntTC) {
    ret = true;
  }
  return ret;
}

int ObCatalogTableIdentity::assign(const ObCatalogTableIdentity &other)
{
  int ret = OB_SUCCESS;
  tenant_id_ = other.tenant_id_;
  catalog_id_ = other.catalog_id_;
  catalog_name_ = other.catalog_name_;
  db_name_ = other.db_name_;
  tab_name_ = other.tab_name_;
  return ret;
}

int ObCatalogExternalAccessInfo::assign(const ObCatalogExternalAccessInfo &other)
{
  int ret = OB_SUCCESS;
  location_ = other.location_;
  access_info_ = other.access_info_;
  format_type_ = other.format_type_;
  lake_table_format_ = other.lake_table_format_;
  return ret;
}

int ObCatalogFilteredPartStats::assign(const ObCatalogFilteredPartStats &other)
{
  int ret = OB_SUCCESS;
  filtered_file_count_ = other.filtered_file_count_;
  filtered_data_size_ = other.filtered_data_size_;
  filtered_last_analyzed_ = other.filtered_last_analyzed_;
  filtered_schema_version_ = other.filtered_schema_version_;
  return ret;
}

int ObCatalogAnalyzeSampleInfo::assign(const ObCatalogAnalyzeSampleInfo &other)
{
  int ret = OB_SUCCESS;
  is_sample_ = other.is_sample_;
  mode_ = other.mode_;
  percent_ = other.percent_;
  seed_ = other.seed_;
  return ret;
}

void ObCatalogAnalyzeSampleInfo::reset()
{
  is_sample_ = false;
  mode_ = ROW;
  percent_ = 0.0;
  seed_ = -1;
}

void ObCatalogAnalyzeSampleInfo::set_percent(double percent)
{
  is_sample_ = true;
  percent_ = percent;
}

void ObCatalogAnalyzeSampleInfo::set_sample_mode(const SampleMode mode)
{
  mode_ = mode;
}

bool ObCatalogAnalyzeSampleInfo::is_specify_sample() const
{
  return is_sample_ && percent_ >= 0.000001 && percent_ < 100.0;
}

int ObCatalogGatherOptions::assign(const ObCatalogGatherOptions &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(sample_info_.assign(other.sample_info_))) {
    LOG_WARN("failed to assign sample info", K(ret));
  }
  method_opt_ = other.method_opt_;
  degree_ = other.degree_;
  granularity_ = other.granularity_;
  granularity_type_ = other.granularity_type_;
  stat_own_ = other.stat_own_;
  force_ = other.force_;
  stattype_ = other.stattype_;
  need_approx_ndv_ = other.need_approx_ndv_;
  need_refine_min_max_ = other.need_refine_min_max_;
  duration_time_ = other.duration_time_;
  consumer_group_id_ = other.consumer_group_id_;
  return ret;
}

int ObCatalogTableStatParam::assign(const ObCatalogTableStatParam &other)
{
  int ret = OB_SUCCESS;
  allocator_ = other.allocator_;
  part_name_ = other.part_name_;
  part_level_ = other.part_level_;
  global_stat_param_ = other.global_stat_param_;
  part_stat_param_ = other.part_stat_param_;
  global_modified_ts_ = other.global_modified_ts_;

  if (OB_FAIL(table_identity_.assign(other.table_identity_))) {
    LOG_WARN("failed to assign table identity", K(ret));
  } else if (OB_FAIL(gather_options_.assign(other.gather_options_))) {
    LOG_WARN("failed to assign gather options", K(ret));
  } else if (OB_FAIL(filtered_stats_.assign(other.filtered_stats_))) {
    LOG_WARN("failed to assign filtered stats", K(ret));
  } else if (OB_FAIL(external_info_.assign(other.external_info_))) {
    LOG_WARN("failed to assign external info", K(ret));
  } else if (OB_FAIL(part_cols_.assign(other.part_cols_))) {
    LOG_WARN("failed to assign part cols", K(ret));
  } else if (OB_FAIL(part_infos_.assign(other.part_infos_))) {
    LOG_WARN("failed to assign part infos", K(ret));
  } else if (OB_FAIL(all_part_infos_.assign(other.all_part_infos_))) {
    LOG_WARN("failed to assign all part infos", K(ret));
  } else if (OB_FAIL(column_params_.assign(other.column_params_))) {
    LOG_WARN("failed to assign column params", K(ret));
  }

  return ret;
}

bool ObCatalogTableStatParam::is_specify_partition() const
{
  return !part_name_.empty() || !part_infos_.empty();
}

bool ObCatalogTableStatParam::is_specify_column() const
{
  return !column_params_.empty();
}

int64_t ObCatalogTableStatParam::get_need_gather_column() const
{
  return column_params_.count();
}

int ObOptCatalogStatGatherParam::assign(const ObOptCatalogStatGatherParam &other)
{
  int ret = OB_SUCCESS;
  stat_level_ = other.stat_level_;
  gather_start_time_ = other.gather_start_time_;
  stattype_ = other.stattype_;
  is_split_gather_ = other.is_split_gather_;
  max_duration_time_ = other.max_duration_time_;
  need_approx_ndv_ = other.need_approx_ndv_;
  need_refine_min_max_ = other.need_refine_min_max_;
  degree_ = other.degree_;
  gather_vectorize_ = other.gather_vectorize_;
  consumer_group_id_ = other.consumer_group_id_;
  allocator_ = other.allocator_;
  if (OB_FAIL(sample_info_.assign(other.sample_info_))) {
    LOG_WARN("failed to assign sample info", K(ret));
  } else if (OB_FAIL(table_identity_.assign(other.table_identity_))) {
    LOG_WARN("failed to assign table identity", K(ret));
  } else if (OB_FAIL(external_info_.assign(other.external_info_))) {
    LOG_WARN("failed to assign external info", K(ret));
  } else if (OB_FAIL(partition_infos_.assign(other.partition_infos_))) {
    LOG_WARN("failed to assign partition infos", K(ret));
  } else if (OB_FAIL(part_cols_.assign(other.part_cols_))) {
    LOG_WARN("failed to assign part cols", K(ret));
  } else if (OB_FAIL(column_params_.assign(other.column_params_))) {
    LOG_WARN("failed to assign column params", K(ret));
  }

  return ret;
}

ObOptCatalogStat::~ObOptCatalogStat()
{
  if (NULL != table_stat_) {
    // table_stat_->~ObOptCatalogTableStat();
    table_stat_ = NULL;
  }
  for (int64_t i = 0; i < column_stats_.count(); ++i) {
    if (NULL != column_stats_.at(i)) {
      // column_stats_.at(i)->~ObOptCatalogColumnStat();
      column_stats_.at(i) = NULL;
    }
  }
}

} // end namespace common
} // end namespace oceanbase

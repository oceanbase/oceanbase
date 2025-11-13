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
#define USING_LOG_PREFIX SQL_ENG

#include "ob_lake_table_reader_profile.h"

namespace oceanbase
{
using namespace common;
using namespace lib;
namespace sql {

void ObLakeTableIMetrics::set_label(const common::ObString &label)
{
  label_.assign_ptr(label.ptr(), label.length());
}

int ObLakeTableORCReaderMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_ORC_FILE_READER);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SELECTED_FILE_COUNT, selected_file_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SKIPPED_FILE_COUNT, skipped_file_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SELECTED_STRIPE_COUNT, selected_stripe_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SKIPPED_STRIPE_COUNT, skipped_stripe_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SELECTED_ROW_GROUP_COUNT, selected_row_group_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SKIPPED_ROW_GROUP_COUNT, skipped_row_group_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_READ_ROW_COUNT, read_rows_count_);
  return ret;
}

void ObLakeTableORCReaderMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTableParquetReaderMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_PARQUET_FILE_READER);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SELECTED_FILE_COUNT, selected_file_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SKIPPED_FILE_COUNT, skipped_file_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SELECTED_ROW_GROUP_COUNT, selected_row_group_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SKIPPED_ROW_GROUP_COUNT, skipped_row_group_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SELECTED_PAGE_COUNT, selected_page_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SKIPPED_PAGE_COUNT, skipped_page_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_READ_ROW_COUNT, read_rows_count_);
  return ret;
}

void ObLakeTableParquetReaderMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTablePreBufferMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_PREFETCH);
  if (label_.case_compare_equal(PREBUFFER_METRICS_LABEL)) {
    ret = update_specific_profile_(ObProfileId::LAKE_TABLE_NON_EAGER);
  } else if (label_.case_compare_equal(EAGER_PREBUFFER_METRICS_LABEL)) {
    ret = update_specific_profile_(ObProfileId::LAKE_TABLE_EAGER);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid metric label", K_(label), K(ret));
  }
  return ret;
}

int ObLakeTablePreBufferMetrics::update_specific_profile_(ObProfileId eager_intend)
{
  int ret = OB_SUCCESS;
  ObProfileSwitcher switcher(eager_intend);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_PREBUFFER_COUNT, prebuffer_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MISS_COUNT, miss_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_HIT_COUNT, hit_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_ASYNC_IO_COUNT, async_io_count_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_ASYNC_IO_SIZE, async_io_size_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_TOTAL_IO_WAIT_TIME, total_io_wait_time_us_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MAX_IO_WAIT_TIME, max_io_wait_time_us_);
  SET_METRIC_VAL(ObMetricId::LAKE_TABLE_TOTAL_READ_SIZE, total_read_size_);
  return ret;
}

void ObLakeTablePreBufferMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTableIOMetrics::update_profile()
{
  int ret = OB_SUCCESS;
  if (label_.case_compare_equal(IO_METRICS_LABEL)) {
    ret = update_specific_profile_(ObProfileId::LAKE_TABLE_NON_EAGER);
  } else if (label_.case_compare_equal(EAGER_IO_METRICS_LABEL)) {
    ret = update_specific_profile_(ObProfileId::LAKE_TABLE_EAGER);
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid metric label", K_(label), K(ret));
  }
  return ret;
}

int ObLakeTableIOMetrics::update_specific_profile_(ObProfileId eager_intend)
{
  int ret = OB_SUCCESS;
  {
    ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_FILE_READER);
    ObProfileSwitcher eager_switcher(eager_intend);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_READ_COUNT, access_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_SYNC_READ_COUNT, sync_read_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_ASYNC_READ_COUNT, async_read_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_READ_IO_SIZE, access_io_size_);
  }
  {
    ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_MEM_CACHE);
    ObProfileSwitcher eager_switcher(eager_intend);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MEM_CACHE_HIT_COUNT, mem_cache_hit_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MEM_CACHE_MISS_COUNT, mem_cache_miss_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MEM_CACHE_HIT_IO_SIZE, mem_cache_hit_io_size_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MEM_CACHE_MISS_IO_SIZE, mem_cache_miss_io_size_);
  }
  {
    ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_DISK_CACHE);
    ObProfileSwitcher eager_switcher(eager_intend);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_DISK_CACHE_HIT_COUNT, disk_cache_hit_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_DISK_CACHE_MISS_COUNT, disk_cache_miss_count_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_DISK_CACHE_HIT_IO_SIZE, disk_cache_hit_io_size_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_DISK_CACHE_MISS_IO_SIZE, disk_cache_miss_io_size_);
  }
  {
    ObProfileSwitcher switcher(ObProfileId::LAKE_TABLE_STORAGE_IO);
    ObProfileSwitcher eager_switcher(eager_intend);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_MAX_IO_TIME, max_io_time_us_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_AVG_IO_TIME,
                   (io_count_ > 0 ? total_io_time_us_ / io_count_ : 0));
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_TOTAL_IO_TIME, total_io_time_us_);
    SET_METRIC_VAL(ObMetricId::LAKE_TABLE_STORAGE_IO_COUNT, io_count_);
  }
  return ret;
}

void ObLakeTableIOMetrics::dump_metrics()
{
  LOG_INFO("dump metrics", K_(label), KPC(this));
}

int ObLakeTableReaderProfile::update_profile()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < metrics_list_.count(); ++i) {
    metrics_list_.at(i)->update_profile();
  }
  return ret;
}

int ObLakeTableReaderProfile::register_metrics(ObLakeTableIMetrics *metrics, const ObString &label)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(metrics)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("is null", K(ret));
  } else if (FALSE_IT(metrics->set_label(label))) {
  } else if (OB_FAIL(metrics_list_.push_back(metrics))) {
    LOG_WARN("failed to add metrics", K(ret));
  }
  return ret;
}

void ObLakeTableReaderProfile::dump_metrics()
{
  for (int64_t i = 0; i < metrics_list_.count(); ++i) {
    metrics_list_.at(i)->dump_metrics();
  }
}


} // end of oceanbase namespace
} // end of oceanbase namespace
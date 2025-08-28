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

#define USING_LOG_PREFIX COMMON
#include "share/stat/ob_lake_table_stat.h"

#include "share/stat/ob_stat_item.h"
#include "sql/table_format/iceberg/ob_iceberg_utils.h"
#include "sql/optimizer/ob_opt_selectivity.h"
#include "sql/optimizer/file_prune/ob_iceberg_file_pruner.h"

namespace oceanbase
{
using namespace share;
using namespace sql;
namespace  common
{

void ObLakeColumnStat::reset()
{
  min_val_.set_min_value();
  max_val_.set_max_value();
  null_count_ = 0;
  size_ = 0;
  record_count_ = 0;
}

int ObLakeColumnStat::merge(ObLakeColumnStat &other)
{
  int ret = OB_SUCCESS;
  null_count_ += other.null_count_;
  size_ += other.size_;
  record_count_ = other.record_count_;
  if (!other.min_val_.is_min_value()) {
    if (min_val_.is_null()) {
      min_val_ = other.min_val_;
    } else if (min_val_ > other.min_val_) {
      min_val_ = other.min_val_;
    }
  }
  if (!other.max_val_.is_max_value()) {
    if (max_val_.is_null()) {
      max_val_ = other.max_val_;
    } else if (max_val_ < other.max_val_) {
      max_val_ = other.max_val_;
    }
  }
  return ret;
}

int ObLakeTableStatUtils::construct_stat_from_iceberg(const uint64_t tenant_id,
                                                      const ObIArray<uint64_t> &column_ids,
                                                      ObIArray<ObIcebergFileDesc*> &file_descs,
                                                      ObLakeTableStat &table_stat,
                                                      ObIArray<ObLakeColumnStat*> &column_stats,
                                                      ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator temp_allocator("LakeTableStat", OB_MALLOC_NORMAL_BLOCK_SIZE, tenant_id);
  ObSEArray<int32_t, 8> field_ids;
  uint64_t last_analyzed = common::ObTimeUtil::current_time();
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null allocator");
  } else if (OB_UNLIKELY(column_ids.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected column ids", K(column_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      int32_t field_id
          = static_cast<int32_t>(iceberg::ObIcebergUtils::get_iceberg_field_id(column_ids.at(i)));
      ObLakeColumnStat *stat = OB_NEWx(ObLakeColumnStat, allocator);
      if (OB_ISNULL(stat)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocato memory for ObLakeColumnStat");
      } else if (OB_FAIL(field_ids.push_back(field_id))) {
        LOG_WARN("failed to push back field id");
      } else if (OB_FAIL(column_stats.push_back(stat))) {
        LOG_WARN("failed to push back column stat", K(field_id));
      } else {
        stat->last_analyzed_ = last_analyzed;
      }
    }
    ObLakeColumnStat tmp_column_stat;
    table_stat.pruned_row_count_ = file_descs.count();
    table_stat.last_analyzed_ = last_analyzed;
    for (int64_t i = 0; OB_SUCC(ret) && i < file_descs.count(); ++i) {
      ObIcebergFileDesc *file_desc = file_descs.at(i);
      iceberg::ManifestEntry *entry = nullptr;
      if (OB_ISNULL(file_desc) || OB_ISNULL(entry = file_desc->entry_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null", K(file_desc), K(entry));
      } else {
        table_stat.data_size_ += entry->data_file.file_size_in_bytes;
        table_stat.pruned_row_count_ += entry->data_file.record_count;
      }
      for (int64_t j = 0; OB_SUCC(ret) && j < field_ids.count(); ++j) {
        if (column_ids.at(j) < OB_APP_MIN_COLUMN_ID) {
          // hidden pseudo column
        } else {
          int32_t field_id = field_ids.at(j);
          int64_t count = 0;
          ObString bound;
          tmp_column_stat.reset();
          if (iceberg::ObIcebergUtils::get_map_value(entry->data_file.null_value_counts, field_id, count)) {
            tmp_column_stat.null_count_ = count;
          }
          if (iceberg::ObIcebergUtils::get_map_value(entry->data_file.column_sizes, field_id, count)) {
            tmp_column_stat.size_ = count;
          }
          if (iceberg::ObIcebergUtils::get_map_value(entry->data_file.value_counts, field_id, count)) {
            tmp_column_stat.record_count_ = count;
          }
          if (iceberg::ObIcebergUtils::get_map_value(entry->data_file.lower_bounds, field_id, bound)) {
            ObObj min_obj;
            // TODO binary to obj
            // min_obj = binary_to_obj(bound, temp_allocator);
            if (!min_obj.is_null()) {
            } else {
              tmp_column_stat.min_val_ = min_obj;
            }
          }
          if (iceberg::ObIcebergUtils::get_map_value(entry->data_file.upper_bounds, field_id, bound)) {
            ObObj max_obj;
            // TODO binary to obj
            // max_obj = binary_to_obj(bound, temp_allocator);
            if (!max_obj.is_null()) {
              tmp_column_stat.min_val_ = max_obj;
            }
          }
          if (OB_SUCC(ret)) {
            if (OB_ISNULL(column_stats.at(j))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get null column stat");
            } else if (OB_FAIL(column_stats.at(j)->merge(tmp_column_stat))) {
              LOG_WARN("failed to merge lake table stat");
            }

          }
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); ++i) {
      if (OB_ISNULL(column_stats.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null column stat");
      } else if (OB_FAIL(ob_write_obj(*allocator, column_stats.at(i)->min_val_, column_stats.at(i)->min_val_))) {
        LOG_WARN("failed to write obj");
      } else if (OB_FAIL(ob_write_obj(*allocator, column_stats.at(i)->max_val_, column_stats.at(i)->max_val_))) {
        LOG_WARN("failed to write obj");
      }
    }
  }
  if (OB_SUCC(ret)) {
    table_stat.total_row_count_ = table_stat.pruned_row_count_;
  }
  return ret;
}

int ObLakeTableStatUtils::scale_column_stats(const int64_t row_cnt,
                                             const double scale_ratio,
                                             ObIArray<ObLakeColumnStat*> &column_stats)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < column_stats.count(); ++i) {
    ObLakeColumnStat *column_stat = column_stats.at(i);
    if (OB_ISNULL(column_stat)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null column stat");
    } else if (column_stat->last_analyzed_ > 0) {
      column_stat->null_count_ = std::min(row_cnt, static_cast<int64_t>(column_stat->null_count_ * scale_ratio));
      column_stat->record_count_ = row_cnt;

      if (scale_ratio < 1.0) {
        column_stat->num_distinct_ = ObOptSelectivity::scale_distinct(row_cnt, row_cnt / scale_ratio, column_stat->num_distinct_);
      }
      column_stat->num_distinct_ = std::min(row_cnt, column_stat->num_distinct_);
    }
  }
  return ret;
}

int ObLakeTableStatUtils::merge_iceberg_column_stats(const ObIArray<ObLakeColumnStat*> &common_column_stats,
                                                     ObIArray<ObLakeColumnStat*> &file_column_stats)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(common_column_stats.count() != file_column_stats.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column stats count", K(common_column_stats.count()), K(file_column_stats.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < common_column_stats.count(); ++i) {
      ObLakeColumnStat *common_column_stat = common_column_stats.at(i);
      ObLakeColumnStat *file_column_stat = file_column_stats.at(i);
      if (OB_ISNULL(common_column_stat) || OB_ISNULL(file_column_stat)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column stat", K(common_column_stat), K(file_column_stat));
      } else if (common_column_stat->last_analyzed_ > 0) {
        file_column_stat->num_distinct_ = common_column_stat->num_distinct_;
      } else {
        file_column_stat->num_distinct_ = std::min(10000L, file_column_stat->record_count_);
      }
    }
  }
  return ret;
}

int ObLakeTableStatUtils::scale_table_stat(const double scale_ratio,
                                           ObLakeTableStat &table_stat)
{
  int ret = OB_SUCCESS;

  if (scale_ratio <= 0.0 || scale_ratio > 1.0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid scale ratio", K(ret), K(scale_ratio));
  } else if (table_stat.last_analyzed_ > 0) {
    // Scale row counts and data size based on the scale ratio
    table_stat.total_row_count_ = static_cast<int64_t>(table_stat.total_row_count_ * scale_ratio);
    table_stat.pruned_row_count_ = static_cast<int64_t>(table_stat.pruned_row_count_ * scale_ratio);
    table_stat.data_size_ = static_cast<int64_t>(table_stat.data_size_ * scale_ratio);
    table_stat.part_cnt_ = std::max(1L, static_cast<int64_t>(table_stat.part_cnt_ * scale_ratio));
    table_stat.file_cnt_ = std::max(1L, static_cast<int64_t>(table_stat.file_cnt_ * scale_ratio));
  }

  return ret;
}

}
}

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

#define USING_LOG_PREFIX SHARE

#include "share/stat/ob_opt_external_table_stat_builder.h"
#include "lib/oblog/ob_log_module.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase {
namespace share {

ObOptExternalTableStatBuilder::ObOptExternalTableStatBuilder()
    : tenant_id_(0), catalog_id_(0), database_name_(),
      table_name_(), partition_value_(), row_count_(0), file_num_(0),
      data_size_(0), last_analyzed_(0), partition_num_(0),
      is_basic_info_set_(false),
      is_stat_info_set_(false) {}

ObOptExternalTableStatBuilder::~ObOptExternalTableStatBuilder() { reset(); }

void ObOptExternalTableStatBuilder::reset() {
  tenant_id_ = 0;
  catalog_id_ = 0;
  database_name_.reset();
  table_name_.reset();
  partition_value_.reset();
  row_count_ = 0;
  file_num_ = 0;
  data_size_ = 0;
  last_analyzed_ = 0;
  partition_num_ = 0;
  is_basic_info_set_ = false;
  is_stat_info_set_ = false;
}

int ObOptExternalTableStatBuilder::set_basic_info(
    uint64_t tenant_id, uint64_t catalog_id,
    const common::ObString &database_name, const common::ObString &table_name,
    const common::ObString &partition_value) {
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(tenant_id == OB_INVALID_ID ||
                  catalog_id == OB_INTERNAL_CATALOG_ID ||
                  database_name.empty() || table_name.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid arguments for basic info", K(ret), K(tenant_id),
             K(catalog_id), K(database_name), K(table_name),
             K(partition_value));
  } else {
    tenant_id_ = tenant_id;
    catalog_id_ = catalog_id;
    database_name_ = database_name;
    table_name_ = table_name;
    partition_value_ = partition_value;
    is_basic_info_set_ = true;
  }

  return ret;
}

int ObOptExternalTableStatBuilder::set_stat_info(int64_t row_count,
                                                 int64_t file_num,
                                                 int64_t data_size,
                                                 int64_t last_analyzed) {
  int ret = OB_SUCCESS;
  row_count_ = row_count;
  file_num_ = file_num;
  data_size_ = data_size;
  last_analyzed_ = last_analyzed;
  is_stat_info_set_ = true;
  return ret;
}

int ObOptExternalTableStatBuilder::merge_table_stat(
    const ObOptExternalTableStat &other) {
  int ret = OB_SUCCESS;

  // Merge statistical values (accumulate)
  row_count_ += other.get_row_count();
  file_num_ += other.get_file_num();
  data_size_ += other.get_data_size();

  // Use the latest analysis time
  if (other.get_last_analyzed() > last_analyzed_) {
    last_analyzed_ = other.get_last_analyzed();
  }

  is_stat_info_set_ = true;

  return ret;
}

int ObOptExternalTableStatBuilder::merge_stat_values(int64_t row_count,
                                                     int64_t file_num,
                                                     int64_t data_size) {
  int ret = OB_SUCCESS;
  row_count_ += row_count;
  file_num_ += file_num;
  data_size_ += data_size;
  return ret;
}

int ObOptExternalTableStatBuilder::build(ObIAllocator &allocator,
                                         ObOptExternalTableStat *&stat) const {
  int ret = OB_SUCCESS;
  stat = nullptr;

  const int64_t size = calculate_size();
  char *buf = nullptr;

  if (OB_UNLIKELY(!is_ready_to_build())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("builder is not ready to build", K(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory for external table stat", K(ret),
             K(size));
  } else {
    int64_t pos = sizeof(ObOptExternalTableStat);
    ObOptExternalTableStat *tmp_stat = new (buf) ObOptExternalTableStat();

    // Set basic information
    tmp_stat->tenant_id_ = tenant_id_;
    tmp_stat->catalog_id_ = catalog_id_;

    // Deep copy strings
    if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
            buf, size, pos, database_name_, tmp_stat->database_name_))) {
      LOG_WARN("failed to deep copy database name", K(ret), K(database_name_));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, size, pos, table_name_, tmp_stat->table_name_))) {
      LOG_WARN("failed to deep copy table name", K(ret), K(table_name_));
    } else if (OB_FAIL(ObDbmsStatsUtils::deep_copy_string(
                   buf, size, pos, partition_value_,
                   tmp_stat->partition_value_))) {
      LOG_WARN("failed to deep copy partition value", K(ret),
               K(partition_value_));
    } else {
      // Set statistical information
      tmp_stat->row_count_ = row_count_;
      tmp_stat->file_num_ = file_num_;
      tmp_stat->data_size_ = data_size_;
      tmp_stat->last_analyzed_ = last_analyzed_;
      tmp_stat->partition_num_ = partition_num_;

      stat = tmp_stat;

      LOG_TRACE("external table stat built successfully", K(tenant_id_),
                K(catalog_id_), K(database_name_), K(table_name_),
                K(partition_value_), K(row_count_), K(file_num_), K(data_size_),
                K(last_analyzed_));
    }
  }

  return ret;
}

int64_t ObOptExternalTableStatBuilder::calculate_size() const {
  return sizeof(ObOptExternalTableStat) + database_name_.length() +
         table_name_.length() + partition_value_.length();
}

bool ObOptExternalTableStatBuilder::is_ready_to_build() const {
  return is_basic_info_set_ && is_stat_info_set_;
}

} // end of namespace share
} // end of namespace oceanbase
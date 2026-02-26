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

#ifndef _OB_OPT_EXTERNAL_TABLE_STAT_BUILDER_H_
#define _OB_OPT_EXTERNAL_TABLE_STAT_BUILDER_H_

#include "lib/allocator/ob_allocator.h"
#include "share/stat/ob_opt_external_table_stat.h"

namespace oceanbase {
namespace share {

class ObOptExternalTableStatBuilder {
public:
  explicit ObOptExternalTableStatBuilder();
  ~ObOptExternalTableStatBuilder();

  /**
   * @brief Reset the builder to initial state
   */
  void reset();

  /**
   * @brief Set basic table information
   */
  int set_basic_info(uint64_t tenant_id, uint64_t catalog_id,
                     const common::ObString &database_name,
                     const common::ObString &table_name,
                     const common::ObString &partition_value);

  /**
   * @brief Set statistical information
   */
  int set_stat_info(int64_t row_count, int64_t file_num, int64_t data_size,
                    int64_t last_analyzed);

  /**
   * @brief Merge with another external table stat
   */
  int merge_table_stat(const ObOptExternalTableStat &other);

  /**
   * @brief Merge statistical values (add row_count, file_num, data_size)
   */
  int merge_stat_values(int64_t row_count, int64_t file_num, int64_t data_size);

  /**
   * @brief Create a new ObOptExternalTableStat object on allocator
   * @param[in] allocator The allocator to use for the new object
   * @param[out] stat Pointer to the created external table stat object
   */
  int build(ObIAllocator &allocator, ObOptExternalTableStat *&stat) const;

  /**
   * @brief Calculate the required size for the final stat object
   */
  int64_t calculate_size() const;

  /**
   * @brief Check if the builder has all required information set
   */
  bool is_ready_to_build() const;

  // Getters for current values (useful for debugging and validation)
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_catalog_id() const { return catalog_id_; }
  const common::ObString &get_database_name() const { return database_name_; }
  const common::ObString &get_table_name() const { return table_name_; }
  const common::ObString &get_partition_value() const {
    return partition_value_;
  }
  int64_t get_row_count() const { return row_count_; }
  int64_t get_file_num() const { return file_num_; }
  int64_t get_data_size() const { return data_size_; }
  int64_t get_last_analyzed() const { return last_analyzed_; }
  void add_partition_num(int64_t partition_num) {
    partition_num_ += partition_num;
  }

  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(database_name_), K(table_name_),
               K(partition_value_), K(row_count_), K(file_num_), K(data_size_),
               K(last_analyzed_), K(is_basic_info_set_), K(is_stat_info_set_));

private:

  // Basic information
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString partition_value_;

  // Statistical information
  int64_t row_count_;
  int64_t file_num_;
  int64_t data_size_;
  int64_t last_analyzed_;
  int64_t partition_num_;

  // Status flags
  bool is_basic_info_set_;
  bool is_stat_info_set_;
};

} // end of namespace share
} // end of namespace oceanbase

#endif /* _OB_OPT_EXTERNAL_TABLE_STAT_BUILDER_H_ */
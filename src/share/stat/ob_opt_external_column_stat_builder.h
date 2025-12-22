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

#ifndef _OB_OPT_EXTERNAL_COLUMN_STAT_BUILDER_H_
#define _OB_OPT_EXTERNAL_COLUMN_STAT_BUILDER_H_

#include "common/object/ob_object.h"
#include "lib/allocator/ob_allocator.h"
#include "share/stat/ob_opt_external_column_stat.h"

namespace oceanbase {
namespace share {

class ObHiveHLL;

class ObHiveFMSketch;
/**
 * @brief Bitmap type enumeration for external column statistics
 */
enum class ObExternalBitmapType {
  UNKNOWN = 0,
  HIVE_AUTO_DETECT = 1, // Hive bitmap with auto-detection (FM or HLL)
  HIVE_FM = 2,          // Hive FM Sketch bitmap (for low cardinality)
  HIVE_HLL = 3,         // Hive HyperLogLog bitmap (for high cardinality)
  OTHER = 4             // Other bitmap types (no detection needed)
};

class ObOptExternalColumnStatBuilder {
public:
  explicit ObOptExternalColumnStatBuilder(common::ObIAllocator &allocator);
  ~ObOptExternalColumnStatBuilder();

  /**
   * @brief Reset the builder to initial state
   */
  void reset();

  /**
   * @brief Set basic table and column information
   */
  int set_basic_info(uint64_t tenant_id, uint64_t catalog_id,
                     const common::ObString &database_name,
                     const common::ObString &table_name,
                     const common::ObString &partition_value,
                     const common::ObString &column_name);

  /**
   * @brief Set statistical information
   */
  int set_stat_info(int64_t num_null, int64_t num_not_null,
                    int64_t num_distinct, int64_t avg_length,
                    int64_t last_analyzed, common::ObCollationType cs_type);

  /**
   * @brief Set min/max values
   */
  int set_min_value(const common::ObObj &min_value);
  int set_max_value(const common::ObObj &max_value);

  /**
   * @brief Set bitmap data
   */
  int set_bitmap(const char *bitmap, int64_t bitmap_size);

  /**
   * @brief Merge with another external column stat
   */
  int merge_column_stat(const ObOptExternalColumnStat &other);

  /**
   * @brief Merge statistical values (add num_null, num_not_null, etc.)
   */
  int merge_stat_values(int64_t num_null, int64_t num_not_null,
                        int64_t num_distinct, int64_t avg_length);

  /**
   * @brief Merge min value
   */
  int merge_min_value(const common::ObObj &min_value);

  /**
   * @brief Merge max value
   */
  int merge_max_value(const common::ObObj &max_value);

  /**
   * @brief Merge bitmap data
   */
  int merge_bitmap(const char *bitmap, int64_t bitmap_size);

  int add_hhl_value(uint64_t hash_value);

  /**
   * @brief Set bitmap type (should be called before any bitmap operations)
   */
  int set_bitmap_type(ObExternalBitmapType bitmap_type);

  /**
   * @brief Get current bitmap type
   */
  ObExternalBitmapType get_bitmap_type() const { return bitmap_type_; }

  /**
   * @brief Finalize bitmap and calculate final NDV (should be called before
   * build)
   */
  int finalize_bitmap();

  /**
   * @brief Create a new ObOptExternalColumnStat object on allocator
   * @param[in] allocator The allocator to use for the new object
   * @param[out] stat Pointer to the created external column stat object
   * @note Should call finalize_bitmap() before this method
   */
  int build(ObIAllocator &allocator, ObOptExternalColumnStat *&stat) const;

  int64_t calculate_size() const;

  void inc_partition_key_num_distinct() { ++num_distinct_; }
  int64_t get_num_distinct() const { return num_distinct_; }
  TO_STRING_KV(K(tenant_id_), K(catalog_id_), K(database_name_), K(table_name_),
               K(partition_value_), K(column_name_), K(num_null_),
               K(num_not_null_), K(num_distinct_), K(avg_length_),
               K(min_value_), K(max_value_), K(bitmap_size_),
               K(is_min_value_set_), K(is_max_value_set_), K(is_bitmap_set_),
               K(min_obj_buf_), K(min_obj_buf_size_), K(max_obj_buf_),
               K(max_obj_buf_size_), K(bitmap_type_), K(final_bitmap_),
               K(final_bitmap_size_), K(final_ndv_), K(is_bitmap_finalized_));

private:
  int merge_min_max(common::ObObj &cur, const common::ObObj &other,
                    bool is_cmp_min);
  int alloc_bitmap(int64_t size);
  int ensure_min_max_buffer(bool is_min, int64_t required_size);
  int copy_obj_to_buffer(common::ObObj &dest, const common::ObObj &src,
                         bool is_min_obj);

  // Hive bitmap type detection and merge methods
  ObExternalBitmapType detect_hive_bitmap_type(const char *bitmap,
                                               int64_t bitmap_size);
  int merge_hive_fm_bitmap(const char *bitmap, int64_t bitmap_size);
  int merge_hive_hll_bitmap(const char *bitmap, int64_t bitmap_size);
  int merge_other_bitmap(const char *bitmap, int64_t bitmap_size);

private:
  common::ObIAllocator &allocator_;

  // Basic information
  uint64_t tenant_id_;
  uint64_t catalog_id_;
  common::ObString database_name_;
  common::ObString table_name_;
  common::ObString partition_value_;
  common::ObString column_name_;

  // Statistical information
  int64_t num_null_;
  int64_t num_not_null_;
  int64_t num_distinct_;
  int64_t avg_length_;
  common::ObObj min_value_;
  common::ObObj max_value_;
  int64_t bitmap_size_;
  char *bitmap_;
  int64_t last_analyzed_;
  common::ObCollationType cs_type_;
  char *min_obj_buf_;
  int64_t min_obj_buf_size_;
  char *max_obj_buf_;
  int64_t max_obj_buf_size_;

  // Bitmap type management
  ObExternalBitmapType bitmap_type_;

  // Deserialized bitmap objects for efficient merging
  ObHiveFMSketch *hive_fm_sketch_;
  ObHiveHLL *hive_hll_;

  // Finalized bitmap and NDV (set by finalize_bitmap())
  char *final_bitmap_;
  int64_t final_bitmap_size_;
  int64_t final_ndv_;
  bool is_bitmap_finalized_;

  bool is_basic_info_set_;
  bool is_stat_info_set_;
  bool is_min_value_set_;
  bool is_max_value_set_;
  bool is_bitmap_set_;
};

} // end of namespace share
} // end of namespace oceanbase

#endif /* _OB_OPT_EXTERNAL_COLUMN_STAT_BUILDER_H_ */
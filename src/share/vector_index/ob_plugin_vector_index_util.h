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
 * This file is for define of plugin vector index util
 */

#ifndef OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_UTIL_H_
#define OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_UTIL_H_

#include "common/object/ob_obj_type.h"
#include "common/row/ob_row_iterator.h"
#include "lib/vector/ob_vector_util.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "src/share/vector_index/ob_vector_index_util.h"

namespace oceanbase
{
namespace sql
{
class ObDASIter;
}
namespace share
{

class ObVectorQueryRowkeyIterator
{
public:
  ObVectorQueryRowkeyIterator(int64_t total, ObIArray<common::ObRowkey> *rowkeys)
      : is_init_(false), total_(total), cur_pos_(0), batch_size_(0), scan_iter_(nullptr), rowkeys_(rowkeys) {};
  ObVectorQueryRowkeyIterator()
      : is_init_(false), total_(0), cur_pos_(0), batch_size_(0), scan_iter_(nullptr), rowkeys_(nullptr) {};
  virtual ~ObVectorQueryRowkeyIterator() {
    scan_iter_ = nullptr;
    rowkeys_ = nullptr;
  };

  OB_INLINE bool is_init() const
  {
    return is_init_;
  }
  OB_INLINE bool is_get_from_scan_iter() const
  {
    return OB_NOT_NULL(scan_iter_);
  }
  int init(int64_t total, ObIArray<common::ObRowkey> *rowkeys);
  int init(sql::ObDASIter *rowkey_scan_iter);
  void set_batch_size(int64_t batch_size)
  {
    batch_size_ = batch_size;
  }
  int get_next_row(common::ObRowkey &rowkey);
  int get_next_row();
  int get_next_rows(ObIArray<common::ObRowkey> &rowkeys, int64_t &row_count);
  int get_next_rows(int64_t &count);
  void reset();

private:
  bool is_init_;
  int64_t total_;
  int64_t cur_pos_;
  int64_t batch_size_;
  sql::ObDASIter *scan_iter_;
  ObIArray<common::ObRowkey> *rowkeys_;
};

class ObVectorQueryVidIterator
{
public:
  ObVectorQueryVidIterator(int64_t total, int64_t *vid, int64_t extra_column_count, int64_t extra_info_actual_size, ObIAllocator *allocator) 
    : is_init_(false),
      total_(total),
      cur_pos_(0),
      batch_size_(0),
      vids_(vid),
      extra_column_count_(extra_column_count),
      extra_info_ptr_(),
      row_(nullptr),
      obj_(nullptr),
      allocator_(allocator) {};

  explicit ObVectorQueryVidIterator(int64_t extra_column_count, int64_t extra_info_actual_size)
      : is_init_(false),
        total_(0),
        cur_pos_(0),
        batch_size_(0),
        vids_(nullptr),
        extra_column_count_(extra_column_count),
        extra_info_actual_size_(extra_info_actual_size),
        extra_info_ptr_(),
        row_(nullptr),
        obj_(nullptr),
        allocator_(nullptr){};
  virtual ~ObVectorQueryVidIterator() {};
  int init();
  int init(int64_t total, int64_t *vids, float *distance, ObIAllocator *allocator);
  int init(int64_t need_count, ObIAllocator *allocator);
  int add_results(int64_t add_cnt, int64_t *add_vids, float *add_distance, const ObVecExtraInfoPtr &extra_infos);
  int add_result(int64_t add_vids, float add_distance, const char *extra_info);
  int64_t get_total() const { return total_; }
  int64_t* get_vids() const { return vids_; }
  float* get_distance() const { return distance_; }
  int64_t get_extra_column_count() const { return extra_column_count_; }
  const ObVecExtraInfoPtr &get_extra_info() const { return extra_info_ptr_; }
  int64_t get_alloc_size() const { return alloc_size_; }
  int init(int64_t total, int64_t *vids, float *distance, const ObVecExtraInfoPtr &extra_info_ptr, ObIAllocator *allocator);
  void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }
  bool get_enough() { return total_ >= alloc_size_; }

  virtual int get_next_row(ObNewRow *&row, const sql::ExprFixedArray& res_exprs);
  virtual int get_next_rows(ObNewRow *&row, int64_t &size, const sql::ExprFixedArray& res_exprs);
  virtual void reset();
  TO_STRING_EMPTY();

private:
  bool is_init_;
  int64_t alloc_size_;// total alloc size of vids_ and distance_
  int64_t total_;     // total inited size if vids_ and distance_
  int64_t cur_pos_;   // current query pos of vids_
  int64_t batch_size_;
  int64_t *vids_;
  float *distance_;
  int64_t extra_column_count_;
  int64_t extra_info_actual_size_;
  ObVecExtraInfoPtr extra_info_ptr_;
  ObNewRow *row_;
  ObObj *obj_;
  ObIAllocator *allocator_;
};

struct ObVsagQueryResult
{
  int64_t total_;
  const int64_t *vids_;
  const float *distances_;
  ObVecExtraInfoPtr extra_info_ptr_;
};

class ObPluginVectorIndexHelper final
{
public:
  static int driect_merge_delta_and_snap_vids(const ObVsagQueryResult &first, 
                                              const ObVsagQueryResult &second,
                                              int64_t &actual_cnt,
                                              int64_t *&vids_result,
                                              float *&float_result,
                                              ObVecExtraInfoPtr &extra_info_result);
  static int sort_merge_delta_and_snap_vids(const ObVsagQueryResult &first, 
                                            const ObVsagQueryResult &second,
                                            const int64_t total, 
                                            int64_t &actual_cnt,
                                            int64_t *&vids_result,
                                            float *&float_result,
                                            ObVecExtraInfoPtr &extra_info_result);
  static int get_vector_memory_limit_size(const uint64_t tenant_id,
                                          int64_t& memory_limit);
};

};
};

#endif

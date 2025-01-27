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
  ObVectorQueryRowkeyIterator(int64_t total, ObIArray<common::ObRowkey *> *rowkeys)
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
  int init(int64_t total, ObIArray<common::ObRowkey *> *rowkeys);
  int init(sql::ObDASIter *rowkey_scan_iter);
  void set_batch_size(int64_t batch_size)
  {
    batch_size_ = batch_size;
  }
  int get_next_row(common::ObRowkey *&rowkey);
  int get_next_row();
  int get_next_rows(ObIArray<common::ObRowkey *> &rowkeys, int64_t &row_count);
  int get_next_rows(int64_t &count);
  void reset();

private:
  bool is_init_;
  int64_t total_;
  int64_t cur_pos_;
  int64_t batch_size_;
  sql::ObDASIter *scan_iter_;
  ObIArray<common::ObRowkey *> *rowkeys_;
};

class ObVectorQueryVidIterator : public common::ObNewRowIterator
{
public:
  ObVectorQueryVidIterator(int64_t total, int64_t *vid, ObIAllocator *allocator)
    : is_init_(false),
      total_(total),
      cur_pos_(0),
      batch_size_(0),
      vids_(vid),
      row_(nullptr),
      obj_(nullptr),
      allocator_(allocator) {};

  ObVectorQueryVidIterator()
    : is_init_(false),
      total_(0),
      cur_pos_(0),
      batch_size_(0),
      vids_(nullptr),
      row_(nullptr),
      obj_(nullptr),
      allocator_(nullptr) {};
  virtual ~ObVectorQueryVidIterator() {};
  int init();
  int init(int64_t total, int64_t *vids, float *distance, ObIAllocator *allocator);
  void set_batch_size(int64_t batch_size) { batch_size_ = batch_size; }

  virtual int get_next_row(ObNewRow *&row) override;
  virtual int get_next_rows(ObNewRow *&row, int64_t &size) override;
  virtual int get_next_row() override { return OB_NOT_IMPLEMENT; }
  virtual void reset() override;

private:
  bool is_init_;
  int64_t total_;
  int64_t cur_pos_;
  int64_t batch_size_;
  int64_t *vids_;
  float *distance_;
  ObNewRow *row_;
  ObObj *obj_;
  ObIAllocator *allocator_;
};

struct ObVsagQueryResult
{
  int64_t total_;
  const int64_t *vids_;
  const float *distances_;
};

class ObPluginVectorIndexHelper final
{
public:
  static int merge_delta_and_snap_vids(const ObVsagQueryResult &first,
                                       const ObVsagQueryResult &second,
                                       const int64_t total,
                                       int64_t &actual_cnt,
                                       int64_t *&vids_result,
                                       float *&float_result);

  static int get_vector_memory_value_and_limit(const uint64_t tenant_id,
                                               int64_t& value,
                                               int64_t& upper_limit);

  static int is_ob_vector_memory_valid(const uint64_t tenant_id,
                                       bool& is_valid);

  static int get_vector_memory_limit_size(const uint64_t tenant_id,
                                          int64_t& memory_limit);
  static int vsag_errcode_2ob(int vsag_errcode);
};

};
};

#endif

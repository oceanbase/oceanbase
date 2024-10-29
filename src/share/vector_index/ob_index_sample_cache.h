/**
 * Copyright (c) 2024 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SRC_SHARE_VECTOR_INDEX_OB_INDEX_SAMPLE_CACHE_H_
#define SRC_SHARE_VECTOR_INDEX_OB_INDEX_SAMPLE_CACHE_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/vector/ob_vector.h"

namespace oceanbase
{
namespace share
{

class ObIndexSampleCache
{
public:
  ObIndexSampleCache()
    : is_inited_(false),
      tenant_id_(OB_INVALID_TENANT_ID),
      total_cnt_(0),
      sample_cnt_(0),
      cur_idx_(0),
      samples_()
  {}
  ~ObIndexSampleCache() { destroy(); }
  int init(
      const int64_t tenant_id,
      ObArray<ObTypeVector*> samples,
      lib::ObLabel samples_label);
  virtual void destroy();
  bool is_inited() const { return is_inited_; }
  virtual int read();
  virtual int get_next_vector(ObTypeVector &vector);
  virtual int get_random_vector(ObTypeVector &vector);
  int get_vector(const int64_t offset, ObTypeVector *&next_vector);
  void reuse() { cur_idx_ = 0; }

  int64_t get_total_cnt() const { return total_cnt_; }
  int64_t get_sample_cnt() const { return 0 == sample_cnt_ ? total_cnt_ : samples_.count(); }
  virtual DECLARE_TO_STRING;
protected:
  virtual int get_next_vector_by_cache(ObTypeVector &vector);
  bool is_inited_;
  int64_t tenant_id_;
  int64_t total_cnt_;
  int64_t sample_cnt_; // expect sample cnt
  int64_t cur_idx_;
  ObArray<ObTypeVector*> samples_;
};
} // share
} // oceanbase

#endif

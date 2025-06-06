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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_ARRAY_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_WRITE_CACHE_ARRAY_H_

#include "lib/container/ob_array.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFilePage;

// Wraps ObArray to add support for automatic memory release
class ObTmpFilePageArray
{
public:
  // align to 16 * OB_MALLOC_NORMAL_BLOCK_SIZE
  static const int64_t MAX_BUCKET_CAPACITY = 15872;

  ObTmpFilePageArray() : is_inited_(false), size_(0), buckets_() {}
  ~ObTmpFilePageArray() { destroy(); }
  int init();
  void destroy();
  int push_back(ObTmpFilePage *page);
  void pop_back();
  int64_t size() const { return size_; }
  int64_t count() const { return size(); }
  OB_INLINE ObTmpFilePage &operator[] (const int64_t idx)
  {
    return *get_ptr(idx);
  }
  OB_INLINE ObTmpFilePage *get_ptr(const int64_t idx)
  {
    const int64_t bucket_idx = idx / MAX_BUCKET_CAPACITY;
    const int64_t bucket_offset = idx % MAX_BUCKET_CAPACITY;
    return buckets_[bucket_idx]->at(bucket_offset);
  }
private:
  int add_new_bucket_();
private:
  bool is_inited_;
  int64_t size_;
  ObArray<ObArray<ObTmpFilePage *> *> buckets_;
  ModulePageAllocator allocator_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif

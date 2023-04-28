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

#ifndef _OB_HJ_BUF_MGR_H
#define _OB_HJ_BUF_MGR_H 1

#include "lib/utility/ob_unify_serialize.h"
#include "sql/engine/basic/ob_chunk_row_store.h"

namespace oceanbase
{
namespace sql
{
namespace join
{

class ObHJBufMgr : public ObSqlMemoryCallback
{
public:
  ObHJBufMgr() :
    reserve_memory_size_(0), pre_total_alloc_size_(0), total_alloc_size_(0),
    page_size_(0), dumped_size_(0)
  {}

  ~ObHJBufMgr() {}

  inline void set_page_size(int64_t page_size) { page_size_ = page_size; }
  inline int64_t get_page_size() { return page_size_; }

  void set_reserve_memory_size(int64_t reserve_memory_size)
  {
    reserve_memory_size_ = reserve_memory_size;
  }
  int64_t get_reserve_memory_size()
  {
    return reserve_memory_size_;
  }
  int64_t get_pre_total_alloc_size()
  {
    return pre_total_alloc_size_;
  }
  void set_pre_total_alloc_size(int64_t size)
  {
    pre_total_alloc_size_ = size;
  }
  int64_t get_total_alloc_size()
  {
    return total_alloc_size_;
  }
  int64_t get_dumped_size()
  {
    return dumped_size_;
  }

  void reuse()
  {
    dumped_size_ = 0;
  }
  OB_INLINE virtual void alloc(int64_t mem)
  {
    total_alloc_size_ += mem;
  }  
  OB_INLINE virtual void free(int64_t mem)
  {
    total_alloc_size_ -= mem;
  }
  OB_INLINE virtual void dumped(int64_t size)
  {
    dumped_size_ += size;
  }
  OB_INLINE bool is_full()
  {
    return reserve_memory_size_ < page_size_ + total_alloc_size_;
  }

  OB_INLINE bool need_dump()
  {
    return reserve_memory_size_ * RATIO / 100 < total_alloc_size_;
  }
private:
  const static int64_t RATIO = 80;
  int64_t reserve_memory_size_;
  int64_t pre_total_alloc_size_;
  int64_t total_alloc_size_;
  int64_t page_size_;
  int64_t dumped_size_;
};


}
}
}

#endif /* _OB_HJ_BUF_MGR_H */



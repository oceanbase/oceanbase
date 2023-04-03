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

#ifndef OCEANBASE_LIB_CONTAINER_OB_RING_ARRAY_
#define OCEANBASE_LIB_CONTAINER_OB_RING_ARRAY_

#include "lib/allocator/ob_qsync.h"
#include "lib/container/ob_ring_buffer.h"

namespace oceanbase
{
namespace common
{
inline ObQSync& get_rb_qs()
{
  static ObQSync s_rb_qs;
  return s_rb_qs;
}
template<typename T>
class ObRingArray
{
public:
  typedef ObLink Link;
  ObRingArray(): alloc_(NULL) {}
  ~ObRingArray() {}

  void set_label(const lib::ObLabel &label)
  {
    ObMemAttr attr;
    attr.label_ = label;
    alloc_->set_attr(attr);
  }

  void set_mem_attr(const ObMemAttr &attr) {
    alloc_->set_attr(attr);
  }

  int init(int64_t start, common::ObIAllocator *alloc)
  {
    int ret = OB_SUCCESS;
    if (start < 0) {
      ret = OB_INVALID_ARGUMENT;
    } else {
      const int64_t size = OB_MALLOC_NORMAL_BLOCK_SIZE - sizeof(ObRingBuffer::Node);
      const int64_t rbuffer_blk_size = size - (size % sizeof(T));
      if (OB_FAIL(rbuffer_.init(alloc, rbuffer_blk_size))) {
        // do nothing
      } else {
        alloc_ = alloc;
        ret = rbuffer_.truncate(get_start_pos(start));
      }
    }
    return ret;
  }

  void destroy()
  {
    rbuffer_.destroy();
  }

  // return code:
  // OB_ERR_OUT_OF_LOWER_BOUND
  // OB_ALLOCATE_MEMORY_FAILED
  // other error return by T.write_to()
  int set(int64_t idx, T &data)
  {
    int ret = OB_EAGAIN;
    while (OB_EAGAIN == ret) {
      CriticalGuard(get_rb_qs());
      char *buf = NULL;
      bool is_for_write = true;
      if (OB_SUCC(rbuffer_.get(get_end_pos(idx), buf, is_for_write))) {
        T *p = (T *)(buf - sizeof(T));
        ret = data.write_to(p);
      } else if (OB_ERR_OUT_OF_UPPER_BOUND == ret) {
        if (OB_SUCC(rbuffer_.extend(get_end_pos(idx)))) {
          ret = OB_EAGAIN;
        }
      }
    }
    return ret;
  }

  // return code:
  // OB_ERR_OUT_OF_UPPER_BOUND
  // OB_ERR_OUT_OF_LOWER_BOUND
  // OB_ENTRY_NOT_EXIST
  // other error return by T.read_from()
  int get(int64_t idx, T &data)
  {
    int ret = OB_EAGAIN;
    while (OB_EAGAIN == ret) {
      CriticalGuard(get_rb_qs()); // ObRingBuffer::get may access a to-be-freed node
      char *buf = NULL;
      bool is_for_write = false;
      if (OB_SUCC(rbuffer_.get(get_end_pos(idx), buf, is_for_write))) {
        T *p = (T *)(buf - sizeof(T));
        ret = data.read_from(p);
      }
    }
    return ret;
  }

  // shall not return error
  int truncate(int64_t idx)
  {
    int ret = OB_EAGAIN;
    while (OB_EAGAIN == (ret = rbuffer_.truncate(get_start_pos(idx))))
      ;
    try_purge();
    return ret;
  }

private:
  void try_purge()
  {
    Link *del_list = NULL;
    {
      CriticalGuard(get_rb_qs());
      while(OB_EAGAIN == rbuffer_.purge(del_list))
        ;
    }
    if (NULL != del_list) {
      WaitQuiescent(get_rb_qs());
      rbuffer_.destroy_list(del_list);
    }
  }
private:
  static int64_t get_start_pos(int64_t idx) { return idx * sizeof(T); }
  static int64_t get_end_pos(int64_t idx) { return (idx + 1) * sizeof(T); }
private:
  common::ObIAllocator *alloc_;
  ObRingBuffer rbuffer_;
  DISALLOW_COPY_AND_ASSIGN(ObRingArray);
};

}; // end namespace common
}; // end namespace oceanbase

#endif // OCEANBASE_LIB_CONTAINER_OB_RING_ARRAY_

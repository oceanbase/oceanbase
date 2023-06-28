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

#ifndef OCEANBASE_LIB_ALLOCATOR_NORMAL_FIFO_
#define OCEANBASE_LIB_ALLOCATOR_NORMAL_FIFO_

#include <pthread.h>
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_define.h"

class ObFIFOAllocatorSpecialPageListTest;
class ObFIFOAllocatorSpecialPageParamTest;
class ObFIFOAllocatorAlignParamTest;
class ObFIFOAllocatorNormalPageListTest;
class UsedTotalChecker;

namespace oceanbase
{
namespace common
{
class ObFIFOAllocator : public common::ObIAllocator
{
  friend class ::ObFIFOAllocatorSpecialPageParamTest;
  friend class ::ObFIFOAllocatorAlignParamTest;
  friend class ::ObFIFOAllocatorSpecialPageListTest;
  friend class ::ObFIFOAllocatorNormalPageListTest;
  friend class UsedTotalChecker;
private:
  static const int32_t PAGE_HEADER = 0xe1fba00c;
  // Alloc Header Magic Number. f1f0allc for FIFOALLC
  static const int32_t ALLOC_HEADER = 0xf1f0a11c;
  // Mark already free(double free detect). for[FreeBefore] f5eebef0
  static const int32_t ALREADY_FREE = 0xf5eebef0;
  static const int64_t SPECIAL_FLAG = -1;
public:
  struct BasePageHeader
  {
    using LinkedNode = common::ObDLinkNode<BasePageHeader*>;
    BasePageHeader()
      : magic_num_(PAGE_HEADER)
    {
      node_.get_data() = this;
    }
    int32_t magic_num_;
    // multi inherit is annoying so do not inherit ObDLinkNode
    LinkedNode node_;
    union
    {
      int64_t ref_count_;
      int64_t flag_;
    };
  };

  struct NormalPageHeader : public BasePageHeader
  {
    char *offset_;
  };

  struct SpecialPageHeader : public BasePageHeader
  {
    // real_size_ is used to compute special_total_.
    // attention: aligned space may be before AND after user data.
    int64_t real_size_;
  };

  using PageList = common::ObDList<BasePageHeader::LinkedNode>;

  // this is used to manage 16 bytes memory before ptr(return to user).
  struct AllocHeader
  {
    int32_t magic_num_;
    int32_t size_;
    BasePageHeader *page_header_;
  } __attribute__((aligned (16)));
public:
  explicit ObFIFOAllocator(const uint64_t tenant_id = OB_SERVER_TENANT_ID);
  virtual ~ObFIFOAllocator();

  int init(ObIAllocator *allocator,
           const int64_t page_size,
           const ObMemAttr &attr,
           const int64_t init_size=0,
           const int64_t idle_size=256L << 10,
           const int64_t max_size=INT64_MAX);
  void reset();
  void *alloc(const int64_t size);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  virtual void *alloc_align(const int64_t size, const int64_t align);
  void free(void *p);
  void set_label(const lib::ObLabel &label) { attr_.label_ = label; }
  void set_attr(const ObMemAttr &attr) { attr_ = attr; }
  int set_idle(const int64_t idle_size, const bool sync=false);
  int set_max(const int64_t max_size, const bool sync=false);
  int64_t get_max() const { return max_size_; }
  bool is_inited() const { return is_inited_; }

  inline int64_t normal_used() const
  {
    return normal_used_;
  }

  inline int64_t used() const
  {
    return normal_used_ + special_total_;
  }

  inline int64_t normal_total() const
  {
    const int64_t page_count =
      (free_page_list_.get_size() + using_page_list_.get_size() + (current_using_ != nullptr ? 1 : 0));
    return page_count * page_size_;
  }

  inline int64_t total() const
  {
    return normal_total() + special_total_;
  }

private:
  BasePageHeader *get_page_header(void *p);
  bool check_param(const int64_t size, const int64_t align);
  bool check_magic(void *p, int64_t &size);
  bool is_normal_page_enough(const int64_t size, const int64_t align)
  {
    int64_t start_offset = (sizeof(NormalPageHeader) + sizeof(AllocHeader) + align - 1);
    int64_t max_free_size = page_size_ - start_offset;
    return (max_free_size >= size);
  }
  void *try_alloc(const int64_t size, const int64_t align);
  void alloc_new_normal_page();
  void *alloc_normal(const int64_t size, const int64_t align);
  void free_normal(NormalPageHeader *page, int64_t size);
  void *alloc_special(const int64_t size, const int64_t align);
  void free_special(SpecialPageHeader *page);
  int sync_idle(const int64_t idle_size, const int64_t max_size) ;
private:
  bool is_inited_;
  ObIAllocator *allocator_;
  // default allocator
  common::ObMalloc malloc_allocator_;
  int64_t page_size_;
  ObMemAttr attr_;
  int64_t idle_size_;
  int64_t max_size_;
  PageList free_page_list_;
  PageList using_page_list_;
  NormalPageHeader *current_using_;
  PageList special_page_list_;
  int64_t normal_used_;
  int64_t special_total_;
  mutable ObSpinLock lock_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFIFOAllocator);
};

}
}

#endif

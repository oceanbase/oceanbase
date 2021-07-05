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

#ifndef _OB_BUDDY_ALLOCATOR_H
#define _OB_BUDDY_ALLOCATOR_H
#include "lib/allocator/ob_allocator.h"
#include "lib/ob_errno.h"
class BuddyAllocatorTest;
namespace oceanbase {
namespace common {
class ObBuddyAllocator : public common::ObIAllocator {
  friend class ::BuddyAllocatorTest;

public:
  ObBuddyAllocator(ObIAllocator& alloc);
  ~ObBuddyAllocator();
  int32_t init(const int64_t min_block_size);
  void* alloc(const int64_t sz);
  void free(void* ptr);
  // void set_label(const lib::ObLabel &label) {allocator_.set_label(label);};

  int print_info(char* buf, const int64_t buf_len, int64_t& pos) const;

protected:
  int32_t fix_index_array(int32_t order);
  bool find_buddy(int64_t index, int64_t& buddy_index, int32_t order) const;
  void* compute_addr_in(int64_t index, int32_t order) const;

private:
  // uncopyable class
  ObBuddyAllocator(const ObBuddyAllocator&);
  ObBuddyAllocator& operator=(const ObBuddyAllocator&);
  int64_t pow(const int32_t order) const;
  int32_t log2(const int64_t number) const;
  bool is_pow_of_2(const int64_t number) const;
  int64_t get_next_pow(const int64_t number) const;
  int32_t pre_alloc(const int32_t size_order, void*& address_inner, void*& address_user);
  void* linear_alloc(int64_t target_index, int32_t target_order, int64_t ancesstor_index, int32_t ancesstor_order);
  void* single_alloc(int64_t target_index, int32_t target_order);
  int32_t combine_free(int64_t index, int64_t buddy_index, int32_t current_order);
  int32_t single_free(void*& ptr, int64_t index, int32_t order);

private:
  static const int MAX_MEMORY_ORDER = 20;  // max alloc 1MB memory
  static const int MAX_AREA_SIZE = MAX_MEMORY_ORDER + 10;
  bool inited_;
  ObIAllocator& allocator_;
  int64_t min_size_;   // block size: 1 for 1B
  int32_t max_order_;  // MAX_MEMORY_ORDER - log2(min_size_);
  char* free_area_[MAX_AREA_SIZE];
  int64_t index_array_[MAX_AREA_SIZE];  // each order list's first 1 block's location
  void* base_addr_in_;
};
}  // namespace common
}  // namespace oceanbase
#endif /*_OB_BUDDY_ALLOCATOR_H*/

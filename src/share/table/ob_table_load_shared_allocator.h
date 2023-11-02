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

#pragma once

#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace table
{
class ObTableLoadSharedAllocator
{
public:
  ObTableLoadSharedAllocator();
  ~ObTableLoadSharedAllocator();

  void *alloc(const int64_t size);
  void free(void *ptr);
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
  common::ObArenaAllocator &get_allocator() { return allocator_; }

private:
  common::ObArenaAllocator allocator_;
  int64_t ref_count_;
};

class ObTableLoadSharedAllocatorHandle
{
public:
  ObTableLoadSharedAllocatorHandle() : allocator_(nullptr) {}
  ObTableLoadSharedAllocatorHandle(ObTableLoadSharedAllocator *allocator);
  ObTableLoadSharedAllocatorHandle(const ObTableLoadSharedAllocatorHandle &other);
  ~ObTableLoadSharedAllocatorHandle();

  ObTableLoadSharedAllocatorHandle &operator=(const ObTableLoadSharedAllocatorHandle &other);
  ObTableLoadSharedAllocator *operator->();
  ObTableLoadSharedAllocator *operator->() const;
  ObTableLoadSharedAllocator &operator*();
  operator bool () const;
  static ObTableLoadSharedAllocatorHandle make_handle();
  void reset();

private:
  ObTableLoadSharedAllocator *allocator_;
};
} // namespace table
} // namespace oceanbase
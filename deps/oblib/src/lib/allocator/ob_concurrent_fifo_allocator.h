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

#ifndef  OCEANBASE_COMMON_CONCURRENT_FIFO_ALLOCATOR_H_
#define  OCEANBASE_COMMON_CONCURRENT_FIFO_ALLOCATOR_H_
#include "lib/allocator/ob_lf_fifo_allocator.h"

namespace oceanbase
{
namespace common
{
class ObConcurrentFIFOAllocator : public common::ObIAllocator
{
public:
  ObConcurrentFIFOAllocator();
  virtual ~ObConcurrentFIFOAllocator();
public:
  int init(const int64_t total_limit,
           const int64_t hold_limit,
           const int64_t page_size);
  int init(const int64_t page_size,
           const lib::ObLabel &label,
           const uint64_t tenant_id,
           const int64_t total_limit);
  int init(const int64_t page_size,
           const lib::ObMemAttr &attr,
           const int64_t total_limit);
  void destroy();
  void purge();
public:
  void set_label(const lib::ObLabel &label);
  void set_attr(const lib::ObMemAttr &attr);
  void set_tenant_id(const uint64_t tenant_id);
  void set_nway(int nway) { inner_allocator_.set_nway(nway); }
  void *alloc(const int64_t size);
  void *alloc(const int64_t size, const ObMemAttr &attr);
  void free(void *ptr);
  int64_t allocated() const;
  int64_t hold() const {return 0;}
  int set_hold_limit(int64_t hold_limit);
  void set_total_limit(int64_t total_limit);
private:
  static const int64_t STORAGE_SIZE_TIMES = 2;
private:
  DISALLOW_COPY_AND_ASSIGN(ObConcurrentFIFOAllocator);
private:
  ObLfFIFOAllocator inner_allocator_;
};
}
}

#endif //OCEANBASE_COMMON_FIFO_ALLOCATOR_H_

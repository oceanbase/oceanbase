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

#ifndef  OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_
#define  OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "ob_pl.h"
#include "ob_pl_user_type.h"

namespace oceanbase
{
namespace pl
{
/* ObPLAllocator can be dynamically scaled， to reduce memory consumption introduced by deep copy
 *  when curr_.used > next_threshold_ or (src copy size) * 2
 *  we alloc a new mem and reset old
 *
 */
class ObPLAllocator : public common::ObIAllocator
{
public:
  ObPLAllocator()
    : allocator1_(ObModIds::OB_PL_TEMP, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()), // PLAllocator use current tenant memory set by MTL_ID()
      allocator2_(ObModIds::OB_PL_TEMP, OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      curr_(&allocator1_),
      backup_(&allocator2_),
      threshold_(PL_ALLOC_THRESHOLD),
      next_threshold_(threshold_),
      can_shrink_(false) {}

  virtual ~ObPLAllocator() {
    allocator1_.reset();
    allocator2_.reset();
    curr_ = NULL;
    backup_ = NULL;
    threshold_ = PL_ALLOC_THRESHOLD;
    next_threshold_ = threshold_;
  }

  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override;
  virtual void *alloc(const int64_t size) override
  {
    return alloc(size, ObMemAttr(MTL_ID(), ObModIds::OB_PL_TEMP));
  }
  virtual void free(void *ptr) override { UNUSED(ptr); }
  virtual void reset();
  virtual int shrink();
  virtual int copy_all_element_with_new_allocator(ObIAllocator *alloc) = 0;

  ObIAllocator* get_allocator() { return curr_; }

  int64_t get_used() { return allocator1_.used() + allocator2_.used(); }

private:
  const static int PL_ALLOC_THRESHOLD = 1024 * 1024; // 1M

  common::ObArenaAllocator allocator1_;
  common::ObArenaAllocator allocator2_;
  ObIAllocator *curr_;
  ObIAllocator *backup_;
  int64_t threshold_;
  int64_t next_threshold_;
  bool can_shrink_;
};

class ObPLCollAllocator : public ObPLAllocator
{
public:
  ObPLCollAllocator(ObPLCollection *coll) :
    ObPLAllocator(), coll_(coll) {}

  virtual int copy_all_element_with_new_allocator(ObIAllocator *allocator) override;
  static int free_child_coll(ObPLCollection &dest);

private:
  ObPLCollection* coll_;
};

class ObPLSymbolAllocator : public ObPLAllocator
{
public:
  ObPLSymbolAllocator(ObPLExecCtx *pl) :
    ObPLAllocator(), pl_(pl) {}

  virtual int copy_all_element_with_new_allocator(ObIAllocator *allocator) override;

private:
  ObPLExecCtx *pl_;
};

class ObPLPackageState;
class ObPLPkgAllocator : public ObPLAllocator
{
public:
  ObPLPkgAllocator(ObPLPackageState *state) :
    ObPLAllocator(), state_(state) {}

  virtual int copy_all_element_with_new_allocator(ObIAllocator *allocator) override;

private:
  ObPLPackageState *state_;
};

}
}

#endif //OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_

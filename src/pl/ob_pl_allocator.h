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

#ifdef  PL_MOD_DEF
PL_MOD_DEF(OB_PL_ANONY_PARAMETER, "AnonyParam")
PL_MOD_DEF(OB_PL_SET_VAR, "SetVar")
PL_MOD_DEF(OB_PL_STATIC_SQL_EXEC, "StaticExec")
PL_MOD_DEF(OB_PL_DYNAMIC_SQL_EXEC, "DyncmicExec")
PL_MOD_DEF(OB_PL_BULK_INTO, "BulkInto")
PL_MOD_DEF(OB_PL_UDT, "Udt")
PL_MOD_DEF(OB_PL_DEBUG_MOD, "PlDebug")
PL_MOD_DEF(OB_PL_DEBUG_SYS_MOD, "PlDebugSys")
PL_MOD_DEF(OB_PL_ANY_DATA, "AnyData")
PL_MOD_DEF(OB_PL_ANY_TYPE, "AnyType")
PL_MOD_DEF(OB_PL_CODE_GEN, "PlCodeGen")
PL_MOD_DEF(OB_PL_JIT, "PlJit")
PL_MOD_DEF(OB_PL_PROFILER, "PlProfiler")
#endif


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


enum PL_MOD_IDX {
  OB_MOD_INVALID = -1,
#define PL_MOD_DEF(type, name) type,
#include "pl/ob_pl_allocator.h"
#undef PL_MOD_DEF
  PL_MOD_IDX_NUM
};

static constexpr const char* OB_PL_MOD_DEF[PL_MOD_IDX_NUM] =
{
#define PL_MOD_DEF(type, name) name,
#include "pl/ob_pl_allocator.h"
#undef PL_MOD_DEF
};

#define GET_PL_MOD_STRING(type)                                                \
  (type > oceanbase::pl::OB_MOD_INVALID &&                                     \
    type < oceanbase::pl::PL_MOD_IDX_NUM)                                      \
      ? oceanbase::pl::OB_PL_MOD_DEF[type]                                     \
      : "PlTemp"
}
}

#endif //OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_

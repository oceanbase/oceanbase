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
PL_MOD_DEF(OB_PL_ARENA, "PlArena")
PL_MOD_DEF(OB_PL_INIT_SESSION_VAR, "PlInitSessVar")
PL_MOD_DEF(OB_PL_SYMBOL_TABLE, "PlTemp")
PL_MOD_DEF(OB_PL_RECORD, "PlTemp")
PL_MOD_DEF(OB_PL_COLLECTION, "PlTemp")
PL_MOD_DEF(OB_PL_PACKAGE_SYMBOL, "PlTemp")
PL_MOD_DEF(OB_PL_BLOCK_EXPR, "PlTemp")
PL_MOD_DEF(OB_PL_MULTISET, "PlMultiset")
#endif


#ifndef  OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_
#define  OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "ob_pl.h"
#include "ob_pl_user_type.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace pl
{

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


class PlMemEntifyDestroyGuard
{
public:
  PlMemEntifyDestroyGuard(lib::MemoryContext &entity) : ref_(entity) {}
  ~PlMemEntifyDestroyGuard();
private:
  lib::MemoryContext &ref_;
};

class ObPLAllocator1 : public common::ObIAllocator
{
public:
  ObPLAllocator1(PL_MOD_IDX idx, common::ObIAllocator *parent_alloc) :
    is_inited_(false),
    memattr_(ObMemAttr(MTL_ID(), "PlTemp", ObCtxIds::DEFAULT_CTX_ID)),
    parent_allocator_(parent_alloc),
    allocator_(nullptr) {}

  virtual ~ObPLAllocator1() {
    destroy();
  }
  int init(ObIAllocator *alloc);
  bool is_inited() const { return is_inited_; }

  virtual void *alloc(const int64_t size, const ObMemAttr &attr);
  virtual void *alloc(const int64_t size)
  {
    return alloc(size, memattr_);
  }
  virtual void* realloc(const void *ptr, const int64_t size, const ObMemAttr &attr);
  virtual void free(void *ptr);
  void reset();
  void destroy();

  ObIAllocator* get_allocator()
  {
    return this;
  }

  ObIAllocator* get_actual_allocator()
  {
    return allocator_;
  }

  ObIAllocator* get_parent_allocator() { return parent_allocator_; }

  int64_t get_used()
  {
    int64_t total = 0;
    if (OB_NOT_NULL(allocator_)) {
      total = allocator_->used();
    }
    return total;
  }
  const ObMemAttr &get_attr() { return memattr_; }

  TO_STRING_KV(K_(is_inited), K_(memattr));

private:
  bool is_inited_;
  ObMemAttr memattr_;
  common::ObIAllocator *parent_allocator_;
  common::ObBlockAllocMgr alloc_mgr_;
  common::ObIAllocator *allocator_;
};

}
}

#endif //OCEANBASE_SRC_PL_OB_PL_ALLOCATOR_H_

/**
 * Copyright (c) 2025 OceanBase
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
#include "lib/container/ob_array.h"
#include "observer/table_load/plan/ob_table_load_plan_common.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadStoreCtx;
class ObTableLoadOp;
class ObTableLoadTableOp;
class ObTableLoadTableChannel;

class ObTableLoadPlan
{
  template <typename Derived>
  friend class ObTableLoadOpFactory;

  template <typename TableOpType>
  friend class ObTableLoadTableOpBuilder;

  friend class ObTableLoadTableOp;

public:
  ObTableLoadPlan(ObTableLoadStoreCtx *store_ctx);
  virtual ~ObTableLoadPlan();
  virtual int generate() = 0;
  ObTableLoadWriteType::Type get_write_type();

  bool is_generated() const { return nullptr != first_table_op_ && nullptr != finish_op_; }

  ObTableLoadStoreCtx *get_store_ctx() { return store_ctx_; }
  ObIAllocator &get_allocator() { return allocator_; }
  ObTableLoadTableOp *get_first_table_op() { return first_table_op_; }
  ObTableLoadOp *get_finish_op() { return finish_op_; }

  static int create_plan(ObTableLoadStoreCtx *store_ctx, ObIAllocator &allocator,
                         ObTableLoadPlan *&plan);

  DECLARE_TO_STRING;

protected:
  int finish_generate(ObTableLoadTableOp *first_table_op);

  // 所有op最终都会在这里分配
  template <typename T, typename... Args>
  int alloc_op(T *&op, Args &&... args)
  {
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc buf", KR(ret));
    } else if (FALSE_IT(op = new (buf) T(args...))) {
    } else if (OB_FAIL(ops_.push_back(op))) {
      SERVER_LOG(WARN, "fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(op)) {
        op->~T();
        op = nullptr;
      }
      if (OB_NOT_NULL(buf)) {
        allocator_.free(buf);
        buf = nullptr;
      }
    }
    return ret;
  }

  template <typename T, typename... Args>
  int alloc_table_op(T *&op, Args &&... args)
  {
    int ret = OB_SUCCESS;
    void *buf = nullptr;
    if (OB_ISNULL(buf = allocator_.alloc(sizeof(T)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(WARN, "fail to alloc buf", KR(ret));
    } else if (FALSE_IT(op = new (buf) T(this, args...))) {
    } else if (OB_FAIL(table_ops_.push_back(op))) {
      SERVER_LOG(WARN, "fail to push back", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(op)) {
        op->~T();
        op = nullptr;
      }
      if (OB_NOT_NULL(buf)) {
        allocator_.free(buf);
        buf = nullptr;
      }
    }
    return ret;
  }

  virtual int alloc_channel(ObTableLoadTableOp *up_table_op, ObTableLoadTableOp *down_table_op,
                            ObTableLoadTableChannel *&table_channel) = 0;

protected:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadStoreCtx *store_ctx_;
  ObArenaAllocator allocator_;
  // TableOp的孩子Op会使用OpCtx的分配器分配内存, 析构的时候需要孩子Op先析构
  ObArray<ObTableLoadOp *> ops_;
  ObArray<ObTableLoadTableOp *> table_ops_;
  ObArray<ObTableLoadTableChannel *> channels_;
  ObTableLoadTableOp *first_table_op_;
  ObTableLoadOp *finish_op_;
};

} // namespace observer
} // namespace oceanbase

/**
 * Copyright (c) 2024 OceanBase
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

#include "lib/container/ob_array.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadStoreCtx;

struct ObTableLoadMergeOpType
{
#define OB_TABLE_LOAD_MERGE_OP_TYPE_DEF(DEF) \
  DEF(INVALID_OP_TYPE, = 0)                  \
  DEF(ROOT, )                                \
  DEF(INSERT_PHASE, )                        \
  DEF(DELETE_PHASE, )                        \
  DEF(ACK_PHASE, )                           \
  DEF(DATA_TABLE, )                          \
  DEF(DELETE_PHASE_DATA_TABLE, )             \
  DEF(ACK_PHASE_DATA_TABLE, )                \
  DEF(INDEX_TABLE, )                         \
  DEF(INDEXES_TABLE, )                       \
  DEF(MERGE_DATA, )                          \
  DEF(DEL_LOB, )                             \
  DEF(RESCAN, )                              \
  DEF(MEM_SORT, )                            \
  DEF(COMPACT_TABLE, )                       \
  DEF(INSERT_SSTABLE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_MERGE_OP_TYPE_DEF, static);
};

// type, classType
#define OB_TABLE_LOAD_MERGE_CHILD_OP_DEF(DEF)                                                  \
  DEF(ObTableLoadMergeOpType::INSERT_PHASE, ObTableLoadMergeInsertPhaseOp)                     \
  DEF(ObTableLoadMergeOpType::DELETE_PHASE, ObTableLoadMergeDeletePhaseOp)                     \
  DEF(ObTableLoadMergeOpType::ACK_PHASE, ObTableLoadMergeAckPhaseOp)                           \
  DEF(ObTableLoadMergeOpType::DATA_TABLE, ObTableLoadMergeDataTableOp)                         \
  DEF(ObTableLoadMergeOpType::DELETE_PHASE_DATA_TABLE, ObTableLoadMergeDeletePhaseDataTableOp) \
  DEF(ObTableLoadMergeOpType::ACK_PHASE_DATA_TABLE, ObTableLoadMergeAckPhaseDataTableOp)       \
  DEF(ObTableLoadMergeOpType::INDEX_TABLE, ObTableLoadMergeIndexTableOp)                       \
  DEF(ObTableLoadMergeOpType::INDEXES_TABLE, ObTableLoadMergeIndexesTableOp)                   \
  DEF(ObTableLoadMergeOpType::MERGE_DATA, ObTableLoadMergeDataOp)                              \
  DEF(ObTableLoadMergeOpType::DEL_LOB, ObTableLoadMergeDelLobOp)                               \
  DEF(ObTableLoadMergeOpType::RESCAN, ObTableLoadMergeRescanOp)                                \
  DEF(ObTableLoadMergeOpType::MEM_SORT, ObTableLoadMergeMemSortOp)                             \
  DEF(ObTableLoadMergeOpType::COMPACT_TABLE, ObTableLoadMergeCompactTableOp)                   \
  DEF(ObTableLoadMergeOpType::INSERT_SSTABLE, ObTableLoadMergeInsertSSTableOp)

#define OB_TABLE_LOAD_MERGE_OP_DEF(DEF)                     \
  DEF(ObTableLoadMergeOpType::ROOT, ObTableLoadMergeRootOp) \
  OB_TABLE_LOAD_MERGE_CHILD_OP_DEF(DEF)

// switch_next_op只能在op内部调用
// 所有op的生命周期由root_op保证

class ObTableLoadMergeOp
{
public:
  ObTableLoadMergeOp(ObTableLoadMergeOp *parent);
  virtual ~ObTableLoadMergeOp();
  virtual int on_success() { return OB_ERR_UNEXPECTED; }
  virtual void stop();

  VIRTUAL_TO_STRING_KV(KP_(parent), KP_(ctx), KP_(store_ctx));

protected:
  ObTableLoadMergeOp(ObTableLoadTableCtx *ctx, ObTableLoadStoreCtx *store_ctx,
                     ObIAllocator *allocator, ObTableLoadMergeOp *parent);
  virtual int switch_next_op(bool is_parent_called) = 0;

  int switch_parent_op();
  int switch_child_op(ObTableLoadMergeOpType::Type child_op_type);

#define OB_TABLE_LOAD_MERGE_ACQUIRE_CHILD_OP(type, classType, ...)                \
  case type:                                                                      \
    if (OB_ISNULL(child = OB_NEWx(classType, &allocator, this, ##__VA_ARGS__))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                            \
      LOG_WARN("fail to new " #classType, KR(ret));                               \
    }                                                                             \
    break;

#define OB_TABLE_LOAD_MERGE_UNEXPECTED_CHILD_OP_TYPE(type)  \
  default:                                                  \
    ret = OB_ERR_UNEXPECTED;                                \
    LOG_WARN("unexpected child op type", KR(ret), K(type)); \
    break;

  virtual int acquire_child_op(ObTableLoadMergeOpType::Type child_op_type, ObIAllocator &allocator,
                               ObTableLoadMergeOp *&child)
  {
    return OB_ERR_UNEXPECTED;
  }

public:
  ObTableLoadTableCtx *ctx_;
  ObTableLoadStoreCtx *store_ctx_;
  ObIAllocator *allocator_;

protected:
  ObTableLoadMergeOp *parent_;
  ObArray<ObTableLoadMergeOp *> childs_;
};

class ObTableLoadMergeRootOp final : public ObTableLoadMergeOp
{
public:
  ObTableLoadMergeRootOp(ObTableLoadStoreCtx *store_ctx);
  virtual ~ObTableLoadMergeRootOp() = default;
  int start() { return switch_next_op(true /*is_parent_called*/); }

protected:
  int switch_next_op(bool is_parent_called) override;
  int acquire_child_op(ObTableLoadMergeOpType::Type child_op_type, ObIAllocator &allocator,
                       ObTableLoadMergeOp *&child) override;

private:
  enum Status
  {
    NONE = 0,
    INSERT_PHASE,
    DELETE_PHASE,
    ACK_PHASE,
    COMPLETED,
  };
  Status status_;
};

} // namespace observer
} // namespace oceanbase

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

#define USING_LOG_PREFIX SERVER

#include "observer/table_load/plan/ob_table_load_inc_plan.h"
#include "observer/table_load/ob_table_load_store_ctx.h"
#include "observer/table_load/ob_table_load_store_table_ctx.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/plan/ob_table_load_data_table_row_handler.h"
#include "observer/table_load/plan/ob_table_load_data_to_delete_data_table_channel.h"
#include "observer/table_load/plan/ob_table_load_data_to_index_table_channel.h"
#include "observer/table_load/plan/ob_table_load_data_to_insert_data_table_channel.h"
#include "observer/table_load/plan/ob_table_load_data_to_lob_table_channel.h"
#include "observer/table_load/plan/ob_table_load_normal_row_handler.h"
#include "observer/table_load/plan/ob_table_load_table_op_define.h"
#include "observer/table_load/plan/ob_table_load_unique_index_table_row_handler.h"
#include "observer/table_load/plan/ob_table_load_unique_index_to_delete_data_table_channel.h"

namespace oceanbase
{
namespace observer
{
using namespace storage;

static const uint64_t OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG =
  ObTableLoadTableOpOpenFlag::NEED_TRANS_PARAM;

#define OB_TABLE_LOAD_TABLE_CHANNEL_CREATOR(up_table_op_type, down_table_op_type, table_channel,   \
                                            desc)                                                  \
  template <>                                                                                      \
  ObTableLoadTableChannel *                                                                        \
    new_table_channel<ObTableLoadOpType::up_table_op_type, ObTableLoadOpType::down_table_op_type>( \
      ObTableLoadTableOp * up_table_op, ObTableLoadTableOp * down_table_op,                        \
      ObIAllocator & allocator)                                                                    \
  {                                                                                                \
    return OB_NEWx(table_channel, &allocator, up_table_op, down_table_op);                         \
  }

#define OB_TABLE_LOAD_TABLE_CHANNEL_SELECTOR(up_table_op_type, down_table_op_type, \
                                             table_channel_class, desc)            \
  else if (up_table_op->get_op_type() == ObTableLoadOpType::up_table_op_type &&    \
           down_table_op->get_op_type() == ObTableLoadOpType::down_table_op_type)  \
  {                                                                                \
    table_channel = new_table_channel<ObTableLoadOpType::up_table_op_type,         \
                                      ObTableLoadOpType::down_table_op_type>(      \
      up_table_op, down_table_op, allocator_);                                     \
  }

#define OB_TABLE_LOAD_TABLE_ALLOC_CHANNEL_DEFINE()                                                \
  template <ObTableLoadOpType::Type up_table_op_type, ObTableLoadOpType::Type down_table_op_type> \
  ObTableLoadTableChannel *new_table_channel(                                                     \
    ObTableLoadTableOp *up_table_op, ObTableLoadTableOp *down_table_op, ObIAllocator &allocator); \
  OB_TABLE_LOAD_TABLE_CHANNEL_DEF(OB_TABLE_LOAD_TABLE_CHANNEL_CREATOR)                            \
  int alloc_channel(ObTableLoadTableOp *up_table_op, ObTableLoadTableOp *down_table_op,           \
                    ObTableLoadTableChannel *&table_channel) override                             \
  {                                                                                               \
    int ret = OB_SUCCESS;                                                                         \
    table_channel = nullptr;                                                                      \
    if (false) {                                                                                  \
    }                                                                                             \
    OB_TABLE_LOAD_TABLE_CHANNEL_DEF(OB_TABLE_LOAD_TABLE_CHANNEL_SELECTOR)                         \
    else                                                                                          \
    {                                                                                             \
      ret = OB_ERR_UNEXPECTED;                                                                    \
      LOG_WARN("unexpected select table channel", KR(ret), KPC(up_table_op), KPC(down_table_op)); \
    }                                                                                             \
    if (OB_FAIL(ret)) {                                                                           \
    } else if (OB_ISNULL(table_channel)) {                                                        \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                            \
      LOG_WARN("fail to new table channel", KR(ret));                                             \
    } else if (OB_FAIL(channels_.push_back(table_channel))) {                                     \
      LOG_WARN("fail to push back", KR(ret));                                                     \
    }                                                                                             \
    if (OB_FAIL(ret)) {                                                                           \
      if (nullptr != table_channel) {                                                             \
        table_channel->~ObTableLoadTableChannel();                                                \
        allocator_.free(table_channel);                                                           \
        table_channel = nullptr;                                                                  \
      }                                                                                           \
    }                                                                                             \
    return ret;                                                                                   \
  }

// 有主键表
class ObTableLoadIncPKTablePlan final : public ObTableLoadIncPlan
{
public:
  ObTableLoadIncPKTablePlan(ObTableLoadStoreCtx *store_ctx) : ObTableLoadIncPlan(store_ctx) {}
  virtual ~ObTableLoadIncPKTablePlan() = default;
  int generate() override;

  //////////////////////// 定义TableOp ////////////////////////
  // 格式: op_type, op_name, input_type, input_data_type, merge_mode, insert_sstable_type,
  //       dml_row_handler, open_flag
private:
  // 数据表插入算子
  // 用INSERT代替DELETE&INSERT,此算子保留但是走不到
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_INSERT_OP, IncDataTableInsertEputOp, WRITE_INPUT,
                                     ADAPTIVE_FULL_ROW, MERGE_WITH_CONFLICT_CHECK, INC,
                                     ObTableLoadDataTableInsertRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG |
                                       ObTableLoadTableOpOpenFlag::NEED_RESERVED_PARALLEL |
                                       ObTableLoadTableOpOpenFlag::NEED_ONLINE_OPT_STAT_GATHER |
                                       ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB);
  //数据表导入非冲突行
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_INSERT_OP, IncDataTableInsertOp, WRITE_INPUT,
                                     ADAPTIVE_FULL_ROW, MERGE_WITH_CONFLICT_CHECK_WITHOUT_ROW, INC_MAJOR,
                                     ObTableLoadDataTableInsertRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG |
                                       ObTableLoadTableOpOpenFlag::NEED_RESERVED_PARALLEL |
                                       ObTableLoadTableOpOpenFlag::NEED_ONLINE_OPT_STAT_GATHER |
                                       ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB);
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_INSERT_OP, IncMinorDataTableInsertOp,
                                     WRITE_INPUT, ADAPTIVE_FULL_ROW,
                                     MERGE_WITH_CONFLICT_CHECK_WITHOUT_ROW, INC,
                                     ObTableLoadDataTableInsertRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG |
                                       ObTableLoadTableOpOpenFlag::NEED_RESERVED_PARALLEL |
                                       ObTableLoadTableOpOpenFlag::NEED_ONLINE_OPT_STAT_GATHER |
                                       ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB);
  //数据表删除冲突行，DELETE行只保留主键，用于非DELETE_INSERT_ENGINE
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_DELETE_CONFLICT_OP,
                                     IncDataTableDeleteConflictOp, CHANNEL_INPUT, FULL_ROW,
                                     NORMAL_WITH_PK_DELETE_ROW, INC, ObTableLoadNormalRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  //数据表删除冲突行，DELETE行保留完成行，用于DELETE_INSERT_ENGINE
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_DELETE_CONFLICT_OP, IncDataTableFullDeleteConflictOp,
                                     CHANNEL_INPUT, FULL_ROW, NORMAL, INC,
                                     ObTableLoadNormalRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  //数据表插入冲突行
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_INSERT_CONFLICT_OP, IncDataTableInsertConflictOp, CHANNEL_INPUT,
                                     FULL_ROW, NORMAL, INC, ObTableLoadNormalRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG |
                                       ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB);
  // lob表删除算子
  OB_DEFINE_TABLE_LOAD_LOB_TABLE_OP(INC_LOB_TABLE_DELETE_OP, IncLobTableDeleteOp, CHANNEL_INPUT,
                                    LOB_ID, MERGE_WITH_ORIGIN_QUERY_FOR_LOB, INC,
                                    ObTableLoadNormalRowHandler, OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  // 普通索引表插入算子
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_INDEX_TABLE_INSERT_OP, IncIndexTableInsertOp,
                                      CHANNEL_INPUT, FULL_ROW, NORMAL, INC,
                                      ObTableLoadNormalRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);

  //////////////////////// 定义TableChannel ////////////////////////
  // 格式: up_table_op_type, down_table_op_type, table_channel, desc
private:
#define OB_TABLE_LOAD_TABLE_CHANNEL_DEF(DEF)                                                   \
  DEF(INC_DATA_TABLE_INSERT_OP, INC_LOB_TABLE_DELETE_OP, ObTableLoadDataToLobTableChannel,     \
      "data_insert -> lob_delete")                                                             \
  DEF(INC_DATA_TABLE_INSERT_OP, INC_DATA_TABLE_DELETE_CONFLICT_OP, ObTableLoadDataToDeleteDataTableChannel,   \
      "data_insert -> data_delete_conflict")                                                   \
  DEF(INC_DATA_TABLE_INSERT_OP, INC_DATA_TABLE_INSERT_CONFLICT_OP, ObTableLoadDataToInsertDataTableChannel,   \
      "data_insert -> data_insert_conflict")                                                   \
  DEF(INC_DATA_TABLE_INSERT_OP, INC_INDEX_TABLE_INSERT_OP, ObTableLoadDataToIndexTableChannel, \
      "data_insert -> index_insert")

  OB_TABLE_LOAD_TABLE_ALLOC_CHANNEL_DEFINE();

#undef OB_TABLE_LOAD_TABLE_CHANNEL_DEF
};

int ObTableLoadIncPKTablePlan::generate()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_generated())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected generate twice", KR(ret));
  } else {
    ObTableLoadTableOp *data_table_insert_op = nullptr;
    ObTableLoadTableOp *data_table_insert_delete_op = nullptr;
    ObTableLoadIncDataTableInsertConflictOp *data_table_insert_insert_op = nullptr;
    // 数据表插入算子
    if (store_ctx_->ctx_->param_.enable_inc_major_) {
      //inc major
      ObTableLoadIncDataTableInsertOp *inc_major_data_table_insert_op = nullptr;
      if (OB_FAIL(ObTableLoadIncDataTableInsertOp::build(this, store_ctx_->data_store_table_ctx_,
                                                       get_write_type(), inc_major_data_table_insert_op))) {
        LOG_WARN("fail to create data table insert op", KR(ret));
      } else {
        data_table_insert_op = inc_major_data_table_insert_op;
      }
    } else {
      //inc minor
      ObTableLoadIncMinorDataTableInsertOp *inc_minor_data_table_insert_op = nullptr;
      if (OB_FAIL(ObTableLoadIncMinorDataTableInsertOp::build(this, store_ctx_->data_store_table_ctx_,
                                                       get_write_type(), inc_minor_data_table_insert_op))) {
        LOG_WARN("fail to create data table insert op", KR(ret));
      } else {
        data_table_insert_op = inc_minor_data_table_insert_op;
      }
    }
    if (OB_SUCC(ret)) {
    // 数据表冲突数据删除算子
      if (store_ctx_->data_store_table_ctx_->schema_->is_delete_insert_engine_) {
        ObTableLoadIncDataTableFullDeleteConflictOp *data_table_insert_full_delete_op = nullptr;
        if (OB_FAIL(ObTableLoadIncDataTableFullDeleteConflictOp::build(
              this, store_ctx_->data_store_table_ctx_, data_table_insert_full_delete_op))) {
          LOG_WARN("fail to build inc data table insert full delete op", KR(ret));
        } else {
          data_table_insert_delete_op = data_table_insert_full_delete_op;
        }
      } else {
        ObTableLoadIncDataTableDeleteConflictOp *data_table_insert_pt_delete_op = nullptr;
        if (OB_FAIL(ObTableLoadIncDataTableDeleteConflictOp::build(
              this, store_ctx_->data_store_table_ctx_, data_table_insert_pt_delete_op))) {
          LOG_WARN("fail to build inc data table insert delete op", KR(ret));
        } else {
          data_table_insert_delete_op = data_table_insert_pt_delete_op;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(data_table_insert_delete_op->add_dependency(data_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
      LOG_WARN("fail to add dependency", KR(ret));
    // 数据表冲突数据插入算子
    } else if (OB_FAIL(ObTableLoadIncDataTableInsertConflictOp::build(
               this, store_ctx_->data_store_table_ctx_, data_table_insert_insert_op))) {
      LOG_WARN("fail to build inc data table insert op", KR(ret));
    } else if (OB_FAIL(data_table_insert_insert_op->add_dependency(
                 data_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
      LOG_WARN("fail to add dependency", KR(ret));
    } else if (OB_FAIL(data_table_insert_insert_op->add_dependency(
                 data_table_insert_delete_op, ObTableLoadDependencyType::BUSINESS_DEPENDENCY))) {
      LOG_WARN("fail to add dependency", KR(ret), KPC(data_table_insert_delete_op));
    }
    // lob表删除算子
    else if (nullptr != store_ctx_->data_store_table_ctx_->lob_table_ctx_) {
      ObTableLoadIncLobTableDeleteOp *lob_delete_op = nullptr;
      if (OB_FAIL(ObTableLoadIncLobTableDeleteOp::build(
            this, store_ctx_->data_store_table_ctx_->lob_table_ctx_, lob_delete_op))) {
        LOG_WARN("fail to build inc lob table delete op", KR(ret));
      }
      // lob数据来源于有主键表冲突检测
      else if (OB_FAIL(lob_delete_op->add_dependency(data_table_insert_op,
                                                     ObTableLoadDependencyType::DATA_DEPENDENCY))) {
        LOG_WARN("fail to add dependency", KR(ret));
      }
    }
    // 索引表插入算子
    for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
      ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
      if (OB_UNLIKELY(index_table_ctx->schema_->is_local_unique_index())) { // 唯一索引
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected table has unique index", KR(ret));
      } else { // 普通索引
        ObTableLoadIncIndexTableInsertOp *index_table_insert_op = nullptr;
        if (OB_FAIL(ObTableLoadIncIndexTableInsertOp::build(this, index_table_ctx,
                                                            index_table_insert_op))) {
          LOG_WARN("fail to alloc op", KR(ret));
        }
        // 普通索引表数据来源于数据表
        else if (OB_FAIL(index_table_insert_op->add_dependency(
                   data_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
          LOG_WARN("fail to add dependency", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_generate(data_table_insert_op))) {
        LOG_WARN("fail to set finish generate", KR(ret), KPC(data_table_insert_op));
      }
    }
  }
  return ret;
}

// 无主键表
class ObTableLoadIncHeapTablePlan final : public ObTableLoadIncPlan
{
public:
  ObTableLoadIncHeapTablePlan(ObTableLoadStoreCtx *store_ctx) : ObTableLoadIncPlan(store_ctx) {}
  virtual ~ObTableLoadIncHeapTablePlan() = default;
  int generate() override;

  //////////////////////// 定义TableOp ////////////////////////
  // 格式: op_type, op_name, input_type, input_data_type, merge_mode, insert_sstable_type,
  //       dml_row_handler, open_flag
private:
  // ------------ InsertPhase ------------ //
  // 数据表插入算子
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_INSERT_OP, IncDataTableInsertOp, WRITE_INPUT,
                                     ADAPTIVE_FULL_ROW, NORMAL, INC_MAJOR,
                                     ObTableLoadDataTableInsertRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG |
                                       ObTableLoadTableOpOpenFlag::NEED_RESERVED_PARALLEL |
                                       ObTableLoadTableOpOpenFlag::NEED_ONLINE_OPT_STAT_GATHER |
                                       ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB);
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_INSERT_OP, IncMinorDataTableInsertOp,
                                     WRITE_INPUT, ADAPTIVE_FULL_ROW, NORMAL, INC,
                                     ObTableLoadDataTableInsertRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG |
                                       ObTableLoadTableOpOpenFlag::NEED_RESERVED_PARALLEL |
                                       ObTableLoadTableOpOpenFlag::NEED_ONLINE_OPT_STAT_GATHER |
                                       ObTableLoadTableOpOpenFlag::NEED_INSERT_LOB);
  // 普通索引表插入算子
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_INDEX_TABLE_INSERT_OP, IncIndexTableInsertOp,
                                      CHANNEL_INPUT, FULL_ROW, NORMAL, INC_MAJOR,
                                      ObTableLoadNormalRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_INDEX_TABLE_INSERT_OP, IncMinorIndexTableInsertOp,
                                      CHANNEL_INPUT, FULL_ROW, NORMAL, INC,
                                      ObTableLoadNormalRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  // 唯一索引表插入非冲突行算子
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_UNIQUE_INDEX_TABLE_INSERT_OP, IncUniqueIndexTableInsertOp,
                                      CHANNEL_INPUT, FULL_ROW, MERGE_WITH_CONFLICT_CHECK_WITHOUT_ROW, INC_MAJOR,
                                      ObTableLoadUniqueIndexTableInsertRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_UNIQUE_INDEX_TABLE_INSERT_OP, IncMinorUniqueIndexTableInsertOp,
                                      CHANNEL_INPUT, FULL_ROW, MERGE_WITH_CONFLICT_CHECK_WITHOUT_ROW, INC,
                                      ObTableLoadUniqueIndexTableInsertRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  //唯一索引表删除冲突行算子
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_UNIQUE_INDEX_TABLE_DELETE_CONFLICT_OP, IncUniqueIndexTableDeleteConflictOp,
                                      CHANNEL_INPUT, FULL_ROW, NORMAL_WITH_PK_DELETE_ROW, INC,
                                      ObTableLoadNormalRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  //唯一索引表插入冲突行算子
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_UNIQUE_INDEX_TABLE_INSERT_CONFLICT_OP, IncUniqueIndexTableInsertConflictOp,
                                      CHANNEL_INPUT, FULL_ROW, NORMAL, INC,
                                      ObTableLoadNormalRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);

  // ------------ DeletePhase ------------ //
  // 数据表删除算子，删除行保留主键，用于非DELETE_INSERT_ENGINE
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_DELETE_OP, IncDataTableDeleteOp, CHANNEL_INPUT,
                                     ROWKEY, MERGE_WITH_ORIGIN_QUERY_FOR_DATA, INC,
                                     ObTableLoadNormalRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  //数据表删除算子，插入完整行，用于DELETE_INSERT_ENGINE
  OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(INC_DATA_TABLE_DELETE_OP, IncDataTableFullDeleteOp,
                                     CHANNEL_INPUT, ROWKEY,
                                     MERGE_WITH_ORIGIN_QUERY_FOR_DATA_WITH_FULL_DELETE, INC,
                                     ObTableLoadNormalRowHandler,
                                     OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  // lob表删除算子
  OB_DEFINE_TABLE_LOAD_LOB_TABLE_OP(INC_LOB_TABLE_DELETE_OP, IncLobTableDeleteOp, CHANNEL_INPUT,
                                    LOB_ID, MERGE_WITH_ORIGIN_QUERY_FOR_LOB, INC,
                                    ObTableLoadNormalRowHandler, OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  // 普通索引表删除算子
  OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(INC_INDEX_TABLE_DELETE_OP, IncIndexTableDeleteOp,
                                      CHANNEL_INPUT, FULL_ROW, NORMAL_WITH_PK_DELETE_ROW, INC,
                                      ObTableLoadNormalRowHandler,
                                      OB_TABLE_LOAD_INC_BASIC_OPEN_FLAG);
  //////////////////////// 定义TableChannel ////////////////////////
  // 格式: up_table_op_type, down_table_op_type, table_channel, desc
private:
#define OB_TABLE_LOAD_TABLE_CHANNEL_DEF(DEF)                                                   \
  DEF(INC_DATA_TABLE_INSERT_OP, INC_INDEX_TABLE_INSERT_OP, ObTableLoadDataToIndexTableChannel, \
      "data_insert -> index_insert")                                                           \
  DEF(INC_DATA_TABLE_INSERT_OP, INC_UNIQUE_INDEX_TABLE_INSERT_OP,                              \
      ObTableLoadDataToIndexTableChannel, "data_insert -> unique_index_insert")                \
  DEF(INC_UNIQUE_INDEX_TABLE_INSERT_OP, INC_DATA_TABLE_DELETE_OP,                              \
      ObTableLoadUniqueIndexToDeleteDataTableChannel, "unique_index_insert -> data_delete")    \
  DEF(INC_UNIQUE_INDEX_TABLE_INSERT_OP, INC_UNIQUE_INDEX_TABLE_DELETE_CONFLICT_OP,             \
      ObTableLoadDataToDeleteDataTableChannel,                                                 \
      "unique_index_insert -> unique_index_delete_conflict")                                   \
  DEF(INC_UNIQUE_INDEX_TABLE_INSERT_OP, INC_UNIQUE_INDEX_TABLE_INSERT_CONFLICT_OP,             \
      ObTableLoadDataToInsertDataTableChannel,                                                 \
      "unique_index_insert -> unique_index_insert_conflict")                                   \
  DEF(INC_DATA_TABLE_DELETE_OP, INC_LOB_TABLE_DELETE_OP, ObTableLoadDataToLobTableChannel,     \
      "data_delete -> lob_delete")                                                             \
  DEF(INC_DATA_TABLE_DELETE_OP, INC_INDEX_TABLE_DELETE_OP, ObTableLoadDataToIndexTableChannel, \
      "data_delete -> index_delete")

  OB_TABLE_LOAD_TABLE_ALLOC_CHANNEL_DEFINE();

#undef OB_TABLE_LOAD_TABLE_CHANNEL_DEF
};

int ObTableLoadIncHeapTablePlan::generate()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_generated())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected generate twice", KR(ret));
  } else {
    ObTableLoadTableOp *data_table_insert_op = nullptr;
    ObTableLoadTableOp *unique_index_table_insert_op = nullptr;
    ObTableLoadTableOp *data_table_delete_op = nullptr;
    // ------------ InsertPhase ------------ //
    // 数据表插入算子
    if (store_ctx_->ctx_->param_.enable_inc_major_) {
      //inc major
      ObTableLoadIncDataTableInsertOp *inc_major_data_table_insert_op = nullptr;
      if (OB_FAIL(ObTableLoadIncDataTableInsertOp::build(this, store_ctx_->data_store_table_ctx_,
                                                       get_write_type(), inc_major_data_table_insert_op))) {
        LOG_WARN("fail to create data table insert op", KR(ret));
      } else {
        data_table_insert_op = inc_major_data_table_insert_op;
      }
    } else {
      //inc minor
      ObTableLoadIncMinorDataTableInsertOp *inc_minor_data_table_insert_op = nullptr;
      if (OB_FAIL(ObTableLoadIncMinorDataTableInsertOp::build(this, store_ctx_->data_store_table_ctx_,
                                                       get_write_type(), inc_minor_data_table_insert_op))) {
        LOG_WARN("fail to create data table insert op", KR(ret));
      } else {
        data_table_insert_op = inc_minor_data_table_insert_op;
      }
    }
    // 索引表插入算子
    for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
      ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
      if (index_table_ctx->schema_->is_local_unique_index()) {
        if (store_ctx_->ctx_->param_.enable_inc_major_) {
          // inc major
          ObTableLoadIncUniqueIndexTableInsertOp *inc_unique_index_table_insert_op = nullptr;
          if (OB_FAIL(ObTableLoadIncUniqueIndexTableInsertOp::build(
                this, index_table_ctx, inc_unique_index_table_insert_op))) {
            LOG_WARN("fail to alloc op", KR(ret));
          } else {
            unique_index_table_insert_op = inc_unique_index_table_insert_op;
          }
        } else {
          // inc minor
          ObTableLoadIncMinorUniqueIndexTableInsertOp *inc_minor_unique_index_table_insert_op =
            nullptr;
          if (OB_FAIL(ObTableLoadIncMinorUniqueIndexTableInsertOp::build(
                this, index_table_ctx, inc_minor_unique_index_table_insert_op))) {
            LOG_WARN("fail to alloc op", KR(ret));
          } else {
            unique_index_table_insert_op = inc_minor_unique_index_table_insert_op;
          }
        }
        if (OB_SUCC(ret)) {
          ObTableLoadIncUniqueIndexTableDeleteConflictOp *unique_index_table_delete_conflict_op =
            nullptr;
          ObTableLoadIncUniqueIndexTableInsertConflictOp *unique_index_table_insert_conflict_op =
            nullptr;
          // 唯一索引表数据来源于数据表
          if (OB_FAIL(unique_index_table_insert_op->add_dependency(
                data_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
            LOG_WARN("fail to add dependency", KR(ret));
          }
          //唯一索引删除冲突行
          else if (OB_FAIL(ObTableLoadIncUniqueIndexTableDeleteConflictOp::build(
                     this, index_table_ctx, unique_index_table_delete_conflict_op))) {
            LOG_WARN("fail to alloc op", KR(ret));
          } else if (OB_FAIL(unique_index_table_delete_conflict_op->add_dependency(
                       unique_index_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
            LOG_WARN("fail to add dependency", KR(ret));
          }
          //唯一索引插入冲突行
          else if (OB_FAIL(ObTableLoadIncUniqueIndexTableInsertConflictOp::build(
                     this, index_table_ctx, unique_index_table_insert_conflict_op))) {
            LOG_WARN("fail to alloc op", KR(ret));
          } else if (OB_FAIL(unique_index_table_insert_conflict_op->add_dependency(
                       unique_index_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
            LOG_WARN("fail to add dependency", KR(ret));
          } else if (OB_FAIL(unique_index_table_insert_conflict_op->add_dependency(
                       unique_index_table_delete_conflict_op,
                       ObTableLoadDependencyType::BUSINESS_DEPENDENCY))) {
            LOG_WARN("fail to add dependency", KR(ret));
          }
        }
      } else { // 普通索引
        ObTableLoadTableOp *index_table_insert_op = nullptr;
        if (store_ctx_->ctx_->param_.enable_inc_major_) {
          // inc major
          ObTableLoadIncIndexTableInsertOp *inc_major_index_table_insert_op = nullptr;
          if (OB_FAIL(ObTableLoadIncIndexTableInsertOp::build(this, index_table_ctx,
                                                              inc_major_index_table_insert_op))) {
            LOG_WARN("fail to alloc op", KR(ret));
          } else {
            index_table_insert_op = inc_major_index_table_insert_op;
          }
        } else {
          // inc minor
          ObTableLoadIncMinorIndexTableInsertOp *inc_minor_index_table_insert_op = nullptr;
          if (OB_FAIL(ObTableLoadIncMinorIndexTableInsertOp::build(
                this, index_table_ctx, inc_minor_index_table_insert_op))) {
            LOG_WARN("fail to alloc op", KR(ret));
          } else {
            index_table_insert_op = inc_minor_index_table_insert_op;
          }
        }
        if (OB_SUCC(ret)) {
          // 普通索引表数据来源于数据表
          if (OB_FAIL(index_table_insert_op->add_dependency(
                data_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
            LOG_WARN("fail to add dependency", KR(ret));
          }
        }
      }
    }
    // ------------ DeletePhase ------------ //
    // 唯一索引冲突检测会产生数据表行的待删除rowkey
    // 通过rowkey数据删除 数据表、lob表、普通索引表 的对应记录
    if (OB_SUCC(ret) && nullptr != unique_index_table_insert_op) {
      if (store_ctx_->data_store_table_ctx_->schema_->is_delete_insert_engine_) {
        ObTableLoadIncDataTableFullDeleteOp *full_delete_op = nullptr;
        if (OB_FAIL(ObTableLoadIncDataTableFullDeleteOp::build(this, store_ctx_->data_store_table_ctx_,
                                                              full_delete_op))) {
          LOG_WARN("fail to build inc data table insert op", KR(ret));
        } else {
          data_table_delete_op = full_delete_op;
        }
      } else {
        ObTableLoadIncDataTableDeleteOp *pt_delete_op = nullptr;
        if (OB_FAIL(ObTableLoadIncDataTableDeleteOp::build(this, store_ctx_->data_store_table_ctx_,
                                                           pt_delete_op))) {
          LOG_WARN("fail to build inc data table insert op", KR(ret));
        } else {
          data_table_delete_op =pt_delete_op;
        }
      }
      // 数据来源于唯一索引冲突检测
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(data_table_delete_op->add_dependency(
                 unique_index_table_insert_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
        LOG_WARN("fail to add dependency", KR(ret));
      }
      // 需要删除lob
      else if (nullptr != store_ctx_->data_store_table_ctx_->lob_table_ctx_) {
        ObTableLoadIncLobTableDeleteOp *lob_delete_op = nullptr;
        if (OB_FAIL(ObTableLoadIncLobTableDeleteOp::build(
              this, store_ctx_->data_store_table_ctx_->lob_table_ctx_, lob_delete_op))) {
          LOG_WARN("fail to build inc lob table delete op", KR(ret));
        } else if (OB_FAIL(lob_delete_op->add_dependency(
                     data_table_delete_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
          LOG_WARN("fail to add dependency", KR(ret));
        }
      }
      // 需要删除索引
      for (int64_t i = 0; OB_SUCC(ret) && i < store_ctx_->index_store_table_ctxs_.count(); ++i) {
        ObTableLoadStoreIndexTableCtx *index_table_ctx = store_ctx_->index_store_table_ctxs_.at(i);
        ObTableLoadIncIndexTableDeleteOp *index_table_delete_op = nullptr;
        if (index_table_ctx->schema_->is_local_unique_index()) {
          // 唯一索引不需要删除
          continue;
        } else if (OB_FAIL(ObTableLoadIncIndexTableDeleteOp::build(this, index_table_ctx,
                                                                   index_table_delete_op))) {
          LOG_WARN("fail to alloc op", KR(ret));
        } else if (OB_FAIL(index_table_delete_op->add_dependency(
                     data_table_delete_op, ObTableLoadDependencyType::DATA_DEPENDENCY))) {
          LOG_WARN("fail to add dependency", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(finish_generate(data_table_insert_op))) {
        LOG_WARN("fail to set finish generate", KR(ret), KPC(data_table_insert_op));
      }
    }
  }
  return ret;
}

/**
 * ObTableLoadIncPlan
 */

int ObTableLoadIncPlan::create_plan(ObTableLoadStoreCtx *store_ctx, ObIAllocator &allocator,
                                    ObTableLoadPlan *&plan)
{
  int ret = OB_SUCCESS;
  if (store_ctx->data_store_table_ctx_->schema_->is_table_without_pk_) {
    if (OB_ISNULL(plan = OB_NEWx(ObTableLoadIncHeapTablePlan, &allocator, store_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadIncHeapTablePlan", KR(ret));
    }
  } else {
    if (OB_ISNULL(plan = OB_NEWx(ObTableLoadIncPKTablePlan, &allocator, store_ctx))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new ObTableLoadIncPKTablePlan", KR(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

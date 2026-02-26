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

#include "observer/table_load/plan/ob_table_load_table_op.h"
#include "observer/table_load/plan/ob_table_load_table_op_builder.h"
#include "observer/table_load/plan/ob_table_load_table_op_driver.h"

namespace oceanbase
{
namespace observer
{
// 定义TableOp Builder
// 1. WRITE_INPUT
//   static int build(ObTableLoadPlan *plan,
//                    ObTableLoadStoreTableCtx *store_table_ctx,
//                    ObTableLoadWriteType::Type write_type,
//                    ObTableLoadTableOp *&table_op);
// 2. CHANNEL_INPUT
//   static int build(ObTableLoadPlan *plan,
//                    ObTableLoadStoreTableCtx *store_table_ctx,
//                    ObTableLoadTableOp *&table_op);

#define OB_DEFINE_TABLE_LOAD_TABLE_OP_BUILDER(op_name, input_type) \
  CONCAT(OB_DEFINE_TABLE_LOAD_TABLE_OP_BUILDER_WITH_, input_type)(op_name)

#define OB_DEFINE_TABLE_LOAD_TABLE_OP_BUILDER_WITH_WRITE_INPUT(op_name)                          \
  static int build(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx,             \
                   const ObTableLoadWriteType::Type write_type, ObTableLoad##op_name *&table_op) \
  {                                                                                              \
    return ObTableLoadTableOpBuilder<ObTableLoad##op_name>::build(plan, store_table_ctx,         \
                                                                  write_type, table_op);         \
  }

#define OB_DEFINE_TABLE_LOAD_TABLE_OP_BUILDER_WITH_CHANNEL_INPUT(op_name)                \
  static int build(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx,     \
                   ObTableLoad##op_name *&table_op)                                      \
  {                                                                                      \
    return ObTableLoadTableOpBuilder<ObTableLoad##op_name>::build(plan, store_table_ctx, \
                                                                  table_op);             \
  }

// 定义TableOp
// 基本格式:
// op_type, op_name, input_type, input_data_type, merge_mode, insert_sstable_type, dml_row_handler,
// open_flag

#define OB_DEFINE_TABLE_LOAD_TABLE_OP(table_type, op_type, op_name, input_type, input_data_type,   \
                                      merge_mode, insert_sstable_type, dml_row_handler, open_flag) \
  class ObTableLoad##op_name final                                                                 \
    : public ObTableLoadTableOpDriver<                                                             \
        ObTableLoadTableType::table_type, ObTableLoadInputDataType::input_data_type,               \
        storage::ObDirectLoadMergeMode::merge_mode,                                                \
        storage::ObDirectLoadInsertSSTableType::insert_sstable_type, dml_row_handler, open_flag>   \
  {                                                                                                \
  public:                                                                                          \
    using SuperClass = ObTableLoadTableOpDriver<                                                   \
      ObTableLoadTableType::table_type, ObTableLoadInputDataType::input_data_type,                 \
      storage::ObDirectLoadMergeMode::merge_mode,                                                  \
      storage::ObDirectLoadInsertSSTableType::insert_sstable_type, dml_row_handler, open_flag>;    \
    ObTableLoad##op_name(ObTableLoadPlan *plan, ObTableLoadStoreTableCtx *store_table_ctx)         \
      : SuperClass(plan, store_table_ctx)                                                          \
    {                                                                                              \
      op_type_ = ObTableLoadOpType::op_type;                                                       \
      table_type_ = ObTableLoadTableType::table_type;                                              \
      input_type_ = ObTableLoadInputType::input_type;                                              \
    }                                                                                              \
    OB_DEFINE_TABLE_LOAD_TABLE_OP_BUILDER(op_name, input_type);                                    \
  }

// DataTableOp
#define OB_DEFINE_TABLE_LOAD_DATA_TABLE_OP(...) \
  OB_DEFINE_TABLE_LOAD_TABLE_OP(DATA_TABLE, __VA_ARGS__)

// LobTableOp
#define OB_DEFINE_TABLE_LOAD_LOB_TABLE_OP(...) OB_DEFINE_TABLE_LOAD_TABLE_OP(LOB_TABLE, __VA_ARGS__)

// IndexTableOp
#define OB_DEFINE_TABLE_LOAD_INDEX_TABLE_OP(...) \
  OB_DEFINE_TABLE_LOAD_TABLE_OP(INDEX_TABLE, __VA_ARGS__)

} // namespace observer
} // namespace oceanbase

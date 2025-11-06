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

#include "lib/string/ob_string.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{
namespace observer
{
// TableType
//  * DATA_TABLE: 数据表
//  * LOB_TABLE: lob表
//  * INDEX_TABLE: 索引表
struct ObTableLoadTableType
{
#define OB_TABLE_LOAD_TABLE_TYPE_DEF(DEF) \
  DEF(INVALID_TABLE_TYPE, = 0)            \
  DEF(DATA_TABLE, )                       \
  DEF(LOB_TABLE, )                        \
  DEF(INDEX_TABLE, )                      \
  DEF(MAX_TABLE_TYPE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_TABLE_TYPE_DEF, static);
};

// TableOp之间的依赖关系
//  * BUSINESS_DEPENDENCY: 业务依赖
//  * DATA_DEPENDENCY: 数据依赖
//  * NESTED_DATA_DEPENDENCY: 嵌套数据依赖, 目前只有数据表和lob表之间存在这种依赖关系
struct ObTableLoadDependencyType
{
#define OB_TABLE_LOAD_DEPENDENCY_TYPE_DEF(DEF) \
  DEF(INVALID_DEPENDENCY_TYPE, = 0)            \
  DEF(BUSINESS_DEPENDENCY, )                   \
  DEF(DATA_DEPENDENCY, )                       \
  DEF(NESTED_DATA_DEPENDENCY, )                \
  DEF(MAX_DEPENDENCY_TYPE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_DEPENDENCY_TYPE_DEF, static);
};

// TableOp的数据输入类型
//  * NONE_INPUT: 无输入, 目前没有这种情况, 占个位
//  * WRITE_INPUT: 数据来自写入任务
//  * CHANNEL_INPUT: 数据来自内部通道
struct ObTableLoadInputType
{
#define OB_TABLE_LOAD_INPUT_TYPE_DEF(DEF) \
  DEF(INVALID_INPUT_TYPE, = 0)            \
  DEF(NONE_INPUT, )                       \
  DEF(WRITE_INPUT, )                      \
  DEF(CHANNEL_INPUT, )                    \
  DEF(MAX_INPUT_TYPE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_INPUT_TYPE_DEF, static);
};

// TableOp的数据类型
//  * FULL_ROW: 完整行数据, 如果是堆表则包含主键
//  * ADAPTIVE_FULL_ROW: 完整行数据, 如果是堆表则不包含主键
//  * ROWKEY: 只有rowkey
//  * LOB_ID: 只有lob_id
struct ObTableLoadInputDataType
{
#define OB_TABLE_LOAD_INPUT_DATA_TYPE_DEF(DEF) \
  DEF(INVALID_INPUT_DATA_TYPE, = 0)            \
  DEF(FULL_ROW, )                              \
  DEF(ADAPTIVE_FULL_ROW, )                     \
  DEF(ROWKEY, )                                \
  DEF(LOB_ID, )                                \
  DEF(MAX_INPUT_DATA_TYPE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_INPUT_DATA_TYPE_DEF, static);
};

struct ObTableLoadWriteType
{
#define OB_TABLE_LOAD_WRITE_TYPE_DEF(DEF) \
  DEF(INVALID_WRITE_TYPE, = 0)            \
  DEF(DIRECT_WRITE, )                     \
  DEF(STORE_WRITE, )                      \
  DEF(PRE_SORT_WRITE, )                   \
  DEF(MAX_WRITE_TYPE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_WRITE_TYPE_DEF, static);
};

struct ObTableLoadOpType
{
#define OB_TABLE_LOAD_OP_TYPE_DEF(DEF)             \
  DEF(INVALID_OP_TYPE, = 0)                        \
  DEF(DIRECT_WRITE_OP, )                           \
  DEF(STORE_WRITE_OP, )                            \
  DEF(PRE_SORT_WRITE_OP, )                         \
  DEF(MERGE_DATA_OP, )                             \
  DEF(MEM_SORT_OP, )                               \
  DEF(COMPACT_DATA_OP, )                           \
  DEF(INSERT_SSTABLE_OP, )                         \
  DEF(TABLE_OP_OPEN_OP, )                          \
  DEF(TABLE_OP_CLOSE_OP, )                         \
  DEF(FINISH_OP, )                                 \
  DEF(MIN_TABLE_OP, = 1000)                        \
  DEF(FULL_DATA_TABLE_OP, )                        \
  DEF(INC_DATA_TABLE_INSERT_OP, )                  \
  DEF(INC_DATA_TABLE_DELETE_CONFLICT_OP, )         \
  DEF(INC_DATA_TABLE_INSERT_CONFLICT_OP, )         \
  DEF(INC_INDEX_TABLE_INSERT_OP, )                 \
  DEF(INC_UNIQUE_INDEX_TABLE_INSERT_OP, )          \
  DEF(INC_UNIQUE_INDEX_TABLE_DELETE_CONFLICT_OP, ) \
  DEF(INC_UNIQUE_INDEX_TABLE_INSERT_CONFLICT_OP, ) \
  DEF(INC_DATA_TABLE_DELETE_OP, )                  \
  DEF(INC_LOB_TABLE_DELETE_OP, )                   \
  DEF(INC_INDEX_TABLE_DELETE_OP, )                 \
  DEF(INC_DATA_TABLE_ACK_OP, )                     \
  DEF(MAX_TABLE_OP, )                              \
  DEF(MAX_OP_TYPE, )

  DECLARE_ENUM(Type, type, OB_TABLE_LOAD_OP_TYPE_DEF, static);

  // ExecutableOp
  static bool is_executable_op(const Type type)
  {
    return type == DIRECT_WRITE_OP || type == STORE_WRITE_OP || type == PRE_SORT_WRITE_OP ||
           type == MEM_SORT_OP || type == COMPACT_DATA_OP || type == INSERT_SSTABLE_OP ||
           type == TABLE_OP_OPEN_OP || type == TABLE_OP_CLOSE_OP || type == FINISH_OP;
  }

  // TableOp
  static bool is_table_op(const Type type) { return type > MIN_TABLE_OP && type < MAX_TABLE_OP; }
  static bool is_data_table_op(const Type type)
  {
    return type == FULL_DATA_TABLE_OP || type == INC_DATA_TABLE_INSERT_OP ||
           type == INC_DATA_TABLE_DELETE_OP || type == INC_DATA_TABLE_ACK_OP ||
           type == INC_DATA_TABLE_DELETE_CONFLICT_OP || type == INC_DATA_TABLE_INSERT_CONFLICT_OP;
  }
  static bool is_lob_table_op(const Type type) { return type == INC_LOB_TABLE_DELETE_OP; }
  static bool is_index_table_op(const Type type)
  {
    return type == INC_INDEX_TABLE_INSERT_OP || type == INC_UNIQUE_INDEX_TABLE_INSERT_OP ||
           type == INC_INDEX_TABLE_DELETE_OP;
  }
};

} // namespace observer
} // namespace oceanbase

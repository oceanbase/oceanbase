// Copyright (c) 2025 OceanBase
// SPDX-License-Identifier: Apache-2.0
#include "storage/compaction/ob_block_op.h"

namespace oceanbase
{
namespace compaction
{

const char * ObBlockOp::block_op_str[] = {
    "NONE",
    "OPEN",
    "REWRITE",
    "FILTER"
};

const char* ObBlockOp::get_block_op_str(const BlockOp &block_op)
{
  STATIC_ASSERT(static_cast<int64_t>(OP_MAX) == ARRAYSIZEOF(block_op_str), "block op array is mismatch");
  return is_valid(block_op) ? block_op_str[block_op] : "OP_INVALID";
}

} // namespace compaction
} // namespace oceanbase

//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
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

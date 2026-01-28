//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_COMPACTION_BLOCK_OP_H_
#define OB_STORAGE_COMPACTION_BLOCK_OP_H_
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
namespace compaction
{

struct ObBlockOp {
  enum BlockOp: uint8_t {
    // must be ascending order, OP_NONE < OP_OPEN / OP_REWRITE < OP_FILTER
    OP_NONE = 0, // reuse
    OP_OPEN = 1, // open range to iter micro[Macro]/row[Micro]
    OP_REWRITE = 2, // [for compat] open range to iter row
    OP_FILTER = 3, // filter range, skip this macro/micro block
    OP_MAX
  };

  ObBlockOp(const BlockOp block_op = OP_NONE)
    : block_op_(block_op)
  {}
  ~ObBlockOp() {}
  OB_INLINE void reset() { block_op_ = OP_MAX; }
  OB_INLINE bool is_max() const { return block_op_ == OP_MAX; }
  OB_INLINE bool is_valid() const { return is_valid(block_op_); }
  OB_INLINE bool is_none() const { return block_op_ == OP_NONE; }
  OB_INLINE bool is_rewrite() const { return block_op_ == OP_REWRITE; }
  OB_INLINE bool is_open() const { return block_op_ == OP_OPEN; }
  OB_INLINE bool is_filter() const { return block_op_ == OP_FILTER; }
  OB_INLINE void set_rewrite() { block_op_ = OP_REWRITE; }
  OB_INLINE void set_open() { block_op_ = OP_OPEN; }
  OB_INLINE void set_none() { block_op_ = OP_NONE; }
  OB_INLINE void set_filter() { block_op_ = OP_FILTER; }
  OB_INLINE ObBlockOp& operator=(const ObBlockOp &other) { block_op_ = other.block_op_; return *this; }
  OB_INLINE static bool is_valid(const BlockOp &block_op) { return block_op < OP_MAX && block_op >= OP_NONE; }
  OB_INLINE void fuse(const ObBlockOp &other) { block_op_ = MAX(block_op_, other.block_op_); }
  static const char* get_block_op_str(const BlockOp &block_op);
  const char* get_block_op_str() const { return get_block_op_str(block_op_); }

  TO_STRING_KV("op", get_block_op_str());

  BlockOp block_op_;
private:
  const static char * block_op_str[];
};

} // namespace compaction
} // namespace oceanbase

#endif // OB_STORAGE_COMPACTION_BLOCK_OP_H_

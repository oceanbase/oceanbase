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

#include "ob_shared_block_info.h"

namespace oceanbase
{
namespace blocksstable
{

/**
 * ---------------------------------------ObBlockInfo----------------------------------------
 */
ObBlockInfo::~ObBlockInfo()
{
  reset();
}

void ObBlockInfo::reset()
{
  nested_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  nested_offset_ = 0;
  macro_id_.reset();
}

bool ObBlockInfo::is_valid() const
{
  return macro_id_.is_valid()
      && nested_offset_ >= 0
      && nested_size_ >= 0;
}

bool ObBlockInfo::is_small_sstable() const
{
  return nested_offset_ > 0 && OB_DEFAULT_MACRO_BLOCK_SIZE != nested_size_;
}

} // namespace blocksstable
} // namespace oceanbase
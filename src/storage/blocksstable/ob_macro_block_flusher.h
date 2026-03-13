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

#ifndef OB_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_FLUSHER_H_
#define OB_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_FLUSHER_H_

namespace oceanbase
{
namespace blocksstable
{
class ObMacroBlock;
/**
 * -----------------------------------------------------------------ObIMacroBlockFlusher-------------------------------------------------------------------
 */
class ObIMacroBlockFlusher
{
public:
  ObIMacroBlockFlusher() = default;
  virtual ~ObIMacroBlockFlusher() = default;
  virtual int write_disk(ObMacroBlock& macro_block, const bool is_close_flush) = 0;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif //OB_STORAGE_BLOCKSSTABLE_OB_MACRO_BLOCK_FLUSHER_H_
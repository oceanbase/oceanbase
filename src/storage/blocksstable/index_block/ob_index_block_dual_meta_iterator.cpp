/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/ob_macro_block_bare_iterator.h"

namespace oceanbase {
using namespace storage;
namespace blocksstable {

ObDualMacroMetaIterator::ObDualMacroMetaIterator()
  : allocator_(nullptr), macro_iter_(), sec_meta_iter_(), iter_end_(false), is_inited_(false) {}

void ObDualMacroMetaIterator::reset()
{
  macro_iter_.reset();
  sec_meta_iter_.reset();
  allocator_ = nullptr;
  iter_end_ = false;
  is_inited_ = false;
}

int ObDualMacroMetaIterator::open(
    ObSSTable &sstable,
    const ObDatumRange &query_range,
    const ObITableReadInfo &rowkey_read_info,
    ObIAllocator &allocator,
    const bool is_reverse_scan,
    const bool need_record_micro_info)
{
  UNUSED(need_record_micro_info);
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", K(ret));
  } else if (OB_FAIL(macro_iter_.open(
      sstable,
      query_range,
      rowkey_read_info,
      allocator,
      is_reverse_scan,
      true /* need record micro index info */))) {
    LOG_WARN("Fail to open index block tree iterator", K(ret));
  } else if (OB_FAIL(sec_meta_iter_.open(
      query_range,
      blocksstable::DATA_BLOCK_META,
      sstable,
      rowkey_read_info,
      allocator,
      is_reverse_scan))) {
    LOG_WARN("Fail to open secondary meta iterator", K(ret), K(rowkey_read_info));
  } else {
    allocator_ = &allocator;
    iter_end_ = false;
    is_inited_ = true;
  }
  return ret;
}

int ObDualMacroMetaIterator::get_next_macro_block(ObMacroBlockDesc &block_desc)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta *macro_meta = block_desc.macro_meta_;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Dual macro meta iterator not inited", K(ret));
  } else if (OB_ISNULL(macro_meta)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid null pointer to read macro meta", K(ret), K(block_desc), KP(macro_meta));
  } else if (OB_UNLIKELY(iter_end_)) {
    ret = OB_ITER_END;
  } else if (OB_SUCC(macro_iter_.get_next_macro_block(block_desc))) {
    if (OB_FAIL(sec_meta_iter_.get_next(*macro_meta))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_UNEXPECTED;
      }
      LOG_WARN("Fail to get next secondary meta iterator", K(ret), K_(macro_iter), K_(sec_meta_iter));
    } else if (OB_UNLIKELY(block_desc.macro_block_id_ != macro_meta->get_macro_id())) {
      ret = OB_ERR_SYS;
      LOG_WARN("Logic macro block id from iterated macro block and merge info not match",
          K(ret), K(block_desc), KPC(macro_meta));
    }
  } else if (OB_UNLIKELY(OB_ITER_END != ret)) {
    LOG_WARN("Fail to get next macro block from index tree", K(ret));
  } else {
    int tmp_ret = sec_meta_iter_.get_next(*macro_meta);
    if (OB_UNLIKELY(OB_ITER_END != tmp_ret)) {
      ret = OB_ERR_SYS;
      LOG_WARN("Dual macro meta iterated different range",
          K(ret), K(tmp_ret), KPC(macro_meta), K(sec_meta_iter_), K(macro_iter_));
    } else {
      iter_end_ = true;
    }
  }
  return ret;
}

} // namespace blocksstable
} // namespace oceanbase

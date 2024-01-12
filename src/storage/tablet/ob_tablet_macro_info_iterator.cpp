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

#define USING_LOG_PREFIX STORAGE

#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/ob_super_block_struct.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
ObTabletBlockInfo::ObTabletBlockInfo()
  : macro_id_(), block_type_(ObTabletMacroType::INVALID_TYPE), occupy_size_(0)
{
}

ObTabletBlockInfo::ObTabletBlockInfo(
    const blocksstable::MacroBlockId &macro_id,
    const ObTabletMacroType block_type,
    const int64_t occupy_size)
  : macro_id_(macro_id), block_type_(block_type), occupy_size_(occupy_size)
{
}

ObTabletBlockInfo::~ObTabletBlockInfo()
{
  reset();
}

void ObTabletBlockInfo::reset()
{
  macro_id_.reset();
  block_type_ = ObTabletMacroType::INVALID_TYPE;
  occupy_size_ = 0;
}

ObMacroInfoIterator::ObMacroInfoIterator()
  : macro_info_(nullptr), block_reader_(), cur_pos_(0), cur_size_(0),
    cur_type_(ObTabletMacroType::INVALID_TYPE),
    target_type_(ObTabletMacroType::INVALID_TYPE), block_info_arr_(),
    allocator_("MacroInfoIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    is_linked_(false), is_inited_(false)
{
}

ObMacroInfoIterator::~ObMacroInfoIterator()
{
  destroy();
}

void ObMacroInfoIterator::destroy()
{
  block_reader_.reset();
  cur_pos_ = 0;
  cur_size_ = 0;
  cur_type_ = ObTabletMacroType::INVALID_TYPE;
  target_type_ = ObTabletMacroType::INVALID_TYPE;
  block_info_arr_.reset();
  is_linked_ = false;
  macro_info_ = nullptr;
  is_inited_ = false;
}

int ObMacroInfoIterator::init(const ObTabletMacroType target_type, const ObTabletMacroInfo &macro_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Macro Info Iterator has been inited", K(ret));
  } else if (OB_UNLIKELY(!macro_info.is_valid() || ObTabletMacroType::INVALID_TYPE == target_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(macro_info));
  } else {
    const MacroBlockId &entry_block = macro_info.entry_block_;
    ObMemAttr mem_attr(MTL_ID(), "TabletBlockId");
    if (!IS_EMPTY_BLOCK_LIST(entry_block)) {
      if (OB_FAIL(block_reader_.init(entry_block, mem_attr))) {
        LOG_WARN("fail to init block reader", K(ret), K(entry_block));
      } else {
        is_linked_ = true;
      }
    } else {
      is_linked_ = false;
    }
  }
  if (OB_SUCC(ret)) {
    block_info_arr_.reset();
    cur_pos_ = 0;
    cur_size_ = 0;
    cur_type_ = ObTabletMacroType::MAX;
    target_type_ = target_type;
    macro_info_ = &macro_info;
    is_inited_ = true;
  }
  return ret;
}

int ObMacroInfoIterator::reuse()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro info iterator hasn't been inited", K(ret));
  } else if (OB_ISNULL(macro_info_) || OB_UNLIKELY(!macro_info_->is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected macro info", K(ret), K_(macro_info));
  } else {
    block_info_arr_.reset();
    cur_pos_ = 0;
    cur_size_ = 0;
    cur_type_ = ObTabletMacroType::MAX;
    if (is_linked_) {
      block_reader_.reset();
      const MacroBlockId &entry_block = macro_info_->entry_block_;
      ObMemAttr mem_attr(MTL_ID(), "TabletBlockId");
      if (OB_UNLIKELY(!entry_block.is_valid() || IS_EMPTY_BLOCK_LIST(entry_block))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected entry block", K(ret), K(entry_block), K(is_linked_));
      } else if (OB_FAIL(block_reader_.init(entry_block, mem_attr))) {
        LOG_WARN("fail to init block reader", K(ret), K(entry_block));
      }
    }
  }
  return ret;
}

int ObMacroInfoIterator::get_next(ObTabletBlockInfo &block_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("macro info iterator hasn't been inited", K(ret));
  }
  while (OB_SUCC(ret) && cur_pos_ == cur_size_) {
    if (!is_linked_) {
      if (OB_FAIL(read_from_memory())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to read block info from memory", K(ret));
        }
      }
    } else if (ObTabletMacroType::LINKED_BLOCK == cur_type_) {
      ret = OB_ITER_END;
    } else {
      if (OB_FAIL(read_from_disk())) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to read block info from disk", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      cur_pos_ = 0;
      cur_size_ = block_info_arr_.cnt_;
    }
  }
  if (OB_SUCC(ret)) {
    block_info = block_info_arr_.arr_[cur_pos_++];
  }
  return ret;
}

int ObMacroInfoIterator::read_from_memory()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  if (OB_ISNULL(macro_info_) || OB_UNLIKELY(ObTabletMacroType::INVALID_TYPE == target_type_
      || ObTabletMacroType::INVALID_TYPE == cur_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid membership", K(ret), KP_(macro_info), K_(target_type), K_(cur_type));
  } else if (OB_UNLIKELY(ObTabletMacroType::LINKED_BLOCK == target_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("don't support", K(ret));
  } else if (ObTabletMacroType::MAX == target_type_) {
    switch (cur_type_) {
      case ObTabletMacroType::MAX:
        cur_type_ = ObTabletMacroType::META_BLOCK;
        break;
      case ObTabletMacroType::META_BLOCK:
        cur_type_ = ObTabletMacroType::DATA_BLOCK;
        break;
      case ObTabletMacroType::DATA_BLOCK:
        cur_type_ = ObTabletMacroType::SHARED_META_BLOCK;
        break;
      case ObTabletMacroType::SHARED_META_BLOCK:
        cur_type_ = ObTabletMacroType::SHARED_DATA_BLOCK;
        break;
      case ObTabletMacroType::SHARED_DATA_BLOCK:
        ret = OB_ITER_END;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected macro type", K(ret), K(cur_type_));
        break;
    }
  } else {
    if (cur_type_ == target_type_) {
      ret = OB_ITER_END;
    } else {
      cur_type_ = target_type_;
    }
  }

  if (OB_SUCC(ret)) {
    switch (cur_type_) {
      case ObTabletMacroType::META_BLOCK:
        if (OB_FAIL(reuse_info_arr(macro_info_->meta_block_info_arr_.cnt_))) {
          LOG_WARN("fail to reuse block_info_arr_", K(ret), K(macro_info_->meta_block_info_arr_));
        } else if (OB_FAIL(convert_to_block_info(macro_info_->meta_block_info_arr_))) {
          LOG_WARN("fail to convert to block info", K(ret), K(macro_info_->meta_block_info_arr_));
        }
        break;
      case ObTabletMacroType::DATA_BLOCK:
        if (OB_FAIL(reuse_info_arr(macro_info_->data_block_info_arr_.cnt_))) {
          LOG_WARN("fail to reuse block_info_arr_", K(ret), K(macro_info_->data_block_info_arr_));
        } else if (OB_FAIL(convert_to_block_info(macro_info_->data_block_info_arr_))) {
          LOG_WARN("fail to convert to block info", K(ret), K(macro_info_->data_block_info_arr_));
        }
        break;
      case ObTabletMacroType::SHARED_META_BLOCK:
        if (OB_FAIL(reuse_info_arr(macro_info_->shared_meta_block_info_arr_.cnt_))) {
          LOG_WARN("fail to reuse block_info_arr_", K(ret), K(macro_info_->shared_meta_block_info_arr_));
        } else if (OB_FAIL(convert_to_block_info(macro_info_->shared_meta_block_info_arr_))) {
          LOG_WARN("fail to convert to block info", K(ret), K(macro_info_->shared_meta_block_info_arr_));
        }
        break;
      case ObTabletMacroType::SHARED_DATA_BLOCK:
        if (OB_FAIL(reuse_info_arr(macro_info_->shared_data_block_info_arr_.cnt_))) {
          LOG_WARN("fail to reuse block_info_arr_", K(ret), K(macro_info_->shared_data_block_info_arr_));
        } else if (OB_FAIL(convert_to_block_info(macro_info_->shared_data_block_info_arr_))) {
          LOG_WARN("fail to convert to block info", K(ret), K(macro_info_->shared_data_block_info_arr_));
        }
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cur_type is invalid", K(ret), K(cur_type_));
        break;
    }
  }
  return ret;
}

int ObMacroInfoIterator::read_from_disk()
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObMetaDiskAddr addr;
  int64_t pos = 0;
  const ObTabletMacroType prev_type = cur_type_;
  const bool target_iter = ObTabletMacroType::MAX != target_type_;

  if (OB_UNLIKELY(ObTabletMacroType::INVALID_TYPE == target_type_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid target_type", K(ret));
  } else if (OB_UNLIKELY(ObTabletMacroType::LINKED_BLOCK == target_type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("don't support", K(ret));
  } else {
    do {
      pos = 0;
      buf = nullptr;
      buf_len = 0;
      if (OB_FAIL(block_reader_.get_next_item(buf, buf_len, addr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("fail to get next item", K(ret));
        }
      } else if (OB_FAIL(serialization::decode_i16(buf, buf_len, pos, reinterpret_cast<int16_t *>(&cur_type_)))) {
        LOG_WARN("fail to deserialize macro type", K(ret), K(buf_len), K(pos));
      } else if (OB_UNLIKELY(ObTabletMacroType::INVALID_TYPE == cur_type_ || ObTabletMacroType::MAX == cur_type_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid macor type", K(ret));
      } else if (target_iter && prev_type == target_type_ && cur_type_ != target_type_) {
        ret = OB_ITER_END; // if ObTabletMacroType::MAX is not equal to target_type_, we only need to iterate targeted ids
      }
    } while (OB_SUCC(ret) && target_iter && cur_type_ != target_type_);
  }

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (ObTabletMacroType::SHARED_DATA_BLOCK == cur_type_) {
    ObTabletMacroInfo::ObBlockInfoArray<ObSharedBlockInfo> tmp_arr;
    if (OB_FAIL(tmp_arr.deserialize(allocator, buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize block info arr", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(reuse_info_arr(tmp_arr.cnt_))) {
      LOG_WARN("fail to reuse block_info_arr_", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(convert_to_block_info(tmp_arr))) {
      LOG_WARN("fail to convert to block info", K(ret), K(tmp_arr));
    }
  } else {
    ObTabletMacroInfo::ObBlockInfoArray<MacroBlockId> tmp_arr;
    if (OB_FAIL(tmp_arr.deserialize(allocator, buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize block info arr", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(reuse_info_arr(tmp_arr.cnt_))) {
      LOG_WARN("fail to reuse block_info_arr_", K(ret), K(buf_len), K(pos));
    } else if (OB_FAIL(convert_to_block_info(tmp_arr))) {
      LOG_WARN("fail to convert to block info", K(ret), K(tmp_arr));
    }
  }

  if (OB_ITER_END == ret && !target_iter) {
    ret = OB_SUCCESS;
    cur_type_ = ObTabletMacroType::LINKED_BLOCK;
    const ObIArray<MacroBlockId> &meta_block_list = block_reader_.get_meta_block_list();
    const int64_t block_cnt = meta_block_list.count();
    if (OB_FAIL(reuse_info_arr(block_cnt))) {
      LOG_WARN("fail to reuse block_info_arr_", K(ret), K(block_cnt));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < block_cnt; i++) {
      const MacroBlockId &tmp_macro_id = meta_block_list.at(i);
      if (OB_UNLIKELY(!tmp_macro_id.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro id is invalid", K(ret), K(tmp_macro_id));
      } else {
        block_info_arr_.arr_[i] = ObTabletBlockInfo(tmp_macro_id, cur_type_, OB_DEFAULT_MACRO_BLOCK_SIZE);
      }
    }
  }
  return ret;
}

int ObMacroInfoIterator::convert_to_block_info(const ObTabletMacroInfo::ObBlockInfoArray<ObSharedBlockInfo> &tmp_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_arr.cnt_; i++) {
    if (OB_UNLIKELY(!tmp_arr.arr_[i].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block info is invalid", K(ret), K(tmp_arr.arr_[i]));
    } else {
      block_info_arr_.arr_[i] = ObTabletBlockInfo(tmp_arr.arr_[i].shared_macro_id_, cur_type_, tmp_arr.arr_[i].occupy_size_);
    }
  }
  return ret;
}

int ObMacroInfoIterator::convert_to_block_info(const ObTabletMacroInfo::ObBlockInfoArray<blocksstable::MacroBlockId> &tmp_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_arr.cnt_; i++) {
    if (OB_UNLIKELY(!tmp_arr.arr_[i].is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("block info is invalid", K(ret), K(tmp_arr.arr_[i]));
    } else {
      block_info_arr_.arr_[i] = ObTabletBlockInfo(tmp_arr.arr_[i], cur_type_, OB_DEFAULT_MACRO_BLOCK_SIZE);
    }
  }
  return ret;
}

int ObMacroInfoIterator::reuse_info_arr(const int64_t cnt)
{
  int ret = OB_SUCCESS;
  if (block_info_arr_.capacity_ >= cnt) {
    block_info_arr_.cnt_ = cnt;
  } else {
    block_info_arr_.reset();
    allocator_.reuse();
    if (OB_FAIL(block_info_arr_.reserve(cnt, allocator_))) {
      LOG_WARN("fail to init block_info_arr_", K(ret), K(cnt));
    }
  }
  return ret;
}

} // storage
} // oceanbase
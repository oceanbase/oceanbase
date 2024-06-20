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

#include "lib/ob_errno.h"
#include "storage/blocksstable/ob_block_manager.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"


namespace oceanbase
{
using namespace blocksstable;
namespace storage
{

using namespace blocksstable;

bool ObLinkedMacroBlockHeader::is_valid() const
{
  bool b_ret =
    LINKED_MACRO_BLOCK_HEADER_VERSION == version_ && LINKED_MACRO_BLOCK_HEADER_MAGIC == magic_;
  return b_ret;
}

int ObLinkedMacroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(buf), K(buf_len));
  } else if (pos + get_serialize_size() > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_ERROR("data buffer is not enough", K(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("linked header is invalid", K(ret), K(*this));
  } else {
    ObLinkedMacroBlockHeader *linked_header =
      reinterpret_cast<ObLinkedMacroBlockHeader *>(buf + pos);
    linked_header->version_ = version_;
    linked_header->magic_ = magic_;
    linked_header->item_count_ = item_count_;
    linked_header->fragment_offset_ = fragment_offset_;
    linked_header->previous_macro_block_id_ = previous_macro_block_id_;
    pos += linked_header->get_serialize_size();
  }
  return ret;
}

int ObLinkedMacroBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_UNLIKELY(data_len - pos < get_serialize_size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("buffer not enough", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    const ObLinkedMacroBlockHeader *ptr =
      reinterpret_cast<const ObLinkedMacroBlockHeader *>(buf + pos);
    version_ = ptr->version_;
    magic_ = ptr->magic_;
    item_count_ = ptr->item_count_;
    fragment_offset_ = ptr->fragment_offset_;
    previous_macro_block_id_ = ptr->previous_macro_block_id_;

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      STORAGE_LOG(ERROR, "deserialize error", K(ret), K(*this));
    } else {
      pos += get_serialize_size();
    }
  }
  return ret;
}

ObMetaBlockListHandle::ObMetaBlockListHandle()
  : meta_handles_(), cur_handle_pos_(0)
{
  meta_handles_[0].reset();
  meta_handles_[1].reset();
}

ObMetaBlockListHandle::~ObMetaBlockListHandle()
{
  reset();
}

int ObMetaBlockListHandle::add_macro_blocks(const ObIArray<blocksstable::MacroBlockId> &block_list)
{
  int ret = OB_SUCCESS;
  ObMacroBlocksHandle &new_handle = meta_handles_[1 - cur_handle_pos_];
  for (int64_t i = 0; OB_SUCC(ret) && i < block_list.count(); ++i) {
    if (OB_FAIL(new_handle.add(block_list.at(i)))) {
      LOG_WARN("fail to add macro block handle", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
    reset_new_handle();
  } else {
    switch_handle();
  }
  return ret;
}

void ObMetaBlockListHandle::reset()
{
  cur_handle_pos_ = 0;
  meta_handles_[0].reset();
  meta_handles_[1].reset();
}

int ObMetaBlockListHandle::reserve(const int64_t block_count)
{
  int ret = OB_SUCCESS;
  if (block_count > 0) {
    if (OB_FAIL(meta_handles_[1 - cur_handle_pos_].reserve(block_count))) {
      LOG_WARN("fail to reserve meta handle", K(ret));
    }
  }
  return ret;
}

const ObIArray<MacroBlockId> &ObMetaBlockListHandle::get_meta_block_list() const
{
  return meta_handles_[cur_handle_pos_].get_macro_id_list();
}

void ObMetaBlockListHandle::switch_handle()
{
  meta_handles_[cur_handle_pos_].reset();
  cur_handle_pos_ = 1 - cur_handle_pos_;
}

void ObMetaBlockListHandle::reset_new_handle()
{
  meta_handles_[1 - cur_handle_pos_].reset();
}
}  // end namespace storage
}  // end namespace oceanbase

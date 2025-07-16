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

#include "storage/slog_ckpt/ob_linked_macro_block_struct.h"
#include "storage/tablet/ob_tablet_persister.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{

using namespace blocksstable;

bool ObLinkedMacroBlockHeader::is_valid() const
{
  bool b_ret = (LINKED_MACRO_BLOCK_HEADER_VERSION_V1 == version_ || LINKED_MACRO_BLOCK_HEADER_VERSION_V2 == version_)
      && LINKED_MACRO_BLOCK_HEADER_MAGIC == magic_;
  return b_ret;
}

int ObLinkedMacroBlockHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(LINKED_MACRO_BLOCK_HEADER_VERSION_V2 != version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected ObLinkedMacroBlockHeader verison", K(ret), K(*this));
  }
  SERIALIZE_MEMBER_WITH_MEMCPY(version_);
  SERIALIZE_MEMBER_WITH_MEMCPY(magic_);
  SERIALIZE_MEMBER_WITH_MEMCPY(item_count_);
  SERIALIZE_MEMBER_WITH_MEMCPY(fragment_offset_);

  if (OB_SUCC(ret)) {
    if (OB_FAIL(previous_macro_block_id_.serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to serialize previous_macro_block_id", K(ret), K(*this));
    }
  }
  return ret;
}

int ObLinkedMacroBlockHeader::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == buf || data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument.", K(ret), KP(buf), K(data_len), K(pos));
  }
  DESERIALIZE_MEMBER_WITH_MEMCPY(version_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(magic_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(item_count_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(fragment_offset_);

  if (OB_SUCC(ret)) {
    if (LINKED_MACRO_BLOCK_HEADER_VERSION_V1 == version_) {
      if (OB_FAIL(previous_macro_block_id_.memcpy_deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize previous_macro_block_id", K(ret), K(*this));
      } else {
        version_ = LINKED_MACRO_BLOCK_HEADER_VERSION_V2;
      }
    } else if (LINKED_MACRO_BLOCK_HEADER_VERSION_V2 == version_) {
      if (OB_FAIL(previous_macro_block_id_.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize previous_macro_block_id", K(ret), K(*this));
      }
    } else {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("unexpected ObLinkedMacroBlockHeader version", K(ret), K(*this));
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
  ObStorageObjectsHandle &new_handle = meta_handles_[1 - cur_handle_pos_];
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

bool ObLinkedMacroInfoWriteParam::is_valid() const
{
  bool is_valid = false;
  switch (type_) {
    case ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO : {
      is_valid = tablet_id_.is_valid() && tablet_transfer_seq_ >= 0;
      break;
    }
    case ObLinkedMacroBlockWriteType::SHARED_MAJOR_MACRO_INFO : {
      is_valid = tablet_id_.is_valid() && start_macro_seq_ >= 0;
      break;
    }
    case ObLinkedMacroBlockWriteType::SHARED_INC_MACRO_INFO : {
      is_valid = ls_id_.is_valid() && tablet_id_.is_valid() && start_macro_seq_ >= 0 && reorganization_scn_ >= 0;
      break;
    }
    default : {
      int ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arguments", K(ret), KPC(this));
    }
  }
  return is_valid;
}

int ObLinkedMacroInfoWriteParam::build_linked_marco_info_param(
    const ObTabletPersisterParam &persist_param,
    const int64_t cur_macro_seq)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!persist_param.is_valid() || cur_macro_seq < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(persist_param), K(cur_macro_seq));
  } else if (persist_param.is_major_shared_object()) {
    type_            = ObLinkedMacroBlockWriteType::SHARED_MAJOR_MACRO_INFO;
    tablet_id_       = persist_param.tablet_id_;
    start_macro_seq_ = cur_macro_seq;
    write_callback_  = persist_param.ddl_redo_callback_;
  } else if (persist_param.is_inc_shared_object()) {
#ifdef OB_BUILD_SHARED_STORAGE
    type_               = ObLinkedMacroBlockWriteType::SHARED_INC_MACRO_INFO;
    ls_id_              = persist_param.ls_id_;
    tablet_id_          = persist_param.tablet_id_;
    op_id_              = persist_param.op_handle_->get_atomic_op()->get_op_id();
    start_macro_seq_    = cur_macro_seq;
    reorganization_scn_ = persist_param.reorganization_scn_;
    write_callback_     = persist_param.ddl_redo_callback_;
#endif
  } else { // private
    type_                = ObLinkedMacroBlockWriteType::PRIV_MACRO_INFO;
    tablet_id_           = persist_param.tablet_id_;
    tablet_transfer_seq_ = persist_param.tablet_transfer_seq_;
    write_callback_      = persist_param.ddl_redo_callback_;
  }
  return ret;
}

void ObLinkedMacroInfoWriteParam::reset()
{
  type_ = ObLinkedMacroBlockWriteType::LMI_MAX_TYPE;
  ls_id_.reset();
  tablet_id_.reset();
  tablet_transfer_seq_ = -1;
  start_macro_seq_  = -1;
  reorganization_scn_ = -1;
  op_id_ = 0;
  write_callback_ = nullptr;
}
}  // end namespace storage
}  // end namespace oceanbase

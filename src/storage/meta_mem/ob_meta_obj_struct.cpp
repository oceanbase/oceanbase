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

#include "ob_meta_obj_struct.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"

namespace oceanbase
{
namespace storage
{

ObMetaDiskAddr::ObMetaDiskAddr()
  : first_id_(0),
    second_id_(0),
    third_id_(0),
    fourth_id_(0),
    fifth_id_(0),
    sixth_id_(0)
{
  static_assert(DiskType::MAX <= MAX_TYPE, "ObMetaDiskAddr's disk type is overflow");
  type_ = DiskType::MAX;
}

void ObMetaDiskAddr::reset()
{
  first_id_ = 0;
  second_id_ = 0;
  third_id_ = 0;
  fourth_id_ = 0;
  fifth_id_ = 0;
  sixth_id_ = 0;
  type_ = DiskType::MAX;
}

int ObMetaDiskAddr::get_block_addr(
    blocksstable::MacroBlockId &macro_id,
    int64_t &offset,
    int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_block())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't block, not support", K(ret), KPC(this));
  } else {
    blocksstable::MacroBlockId id(first_id_, second_id_, third_id_, fifth_id_);
    id.set_version_v2();
    macro_id = id;
    offset = offset_;
    size = size_;
  }
  return ret;
}

int ObMetaDiskAddr::set_block_addr(
    const blocksstable::MacroBlockId &macro_id,
    const int64_t offset,
    const int64_t size,
    const DiskType block_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!macro_id.is_valid()
                || offset < 0 || offset > MAX_OFFSET
                || size < 0 || size > MAX_SIZE
                || (DiskType::RAW_BLOCK != block_type && DiskType::BLOCK != block_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_id), K(offset), K(size), K(block_type));
  } else {
    first_id_ = macro_id.first_id();
    second_id_ = macro_id.second_id();
    third_id_ = macro_id.third_id();
    fifth_id_ = macro_id.fourth_id();
    offset_ = offset;
    size_ = size;
    type_ = block_type;
  }
  return ret;
}

int ObMetaDiskAddr::get_file_addr(int64_t &file_id, int64_t &offset, int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DiskType::FILE != type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't block, not support", K(ret), KPC(this));
  } else {
    file_id = file_id_;
    offset = offset_;
    size = size_;
  }
  return ret;
}

int ObMetaDiskAddr::set_file_addr(const int64_t file_id, const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(file_id <= 0 || offset < 0 || offset > MAX_OFFSET || size < 0 || size > MAX_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(file_id), K(offset), K(size));
  } else {
    file_id_ = file_id;
    offset_ = offset;
    size_ = size;
    type_ = DiskType::FILE;
  }
  return ret;
}

int ObMetaDiskAddr::get_mem_addr(int64_t &offset, int64_t &size) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(DiskType::MEM != type_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't block, not support", K(ret), KPC(this));
  } else {
    offset = offset_;
    size = size_;
  }
  return ret;
}

int ObMetaDiskAddr::set_mem_addr(const int64_t offset, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(offset < 0 || offset > MAX_OFFSET || size < 0 || size > MAX_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(offset), K(size));
  } else {
    offset_ = offset;
    size_ = size;
    type_ = DiskType::MEM;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObMetaDiskAddr,
                    first_id_,
                    second_id_,
                    third_id_,
                    fourth_id_,
                    fifth_id_ // FOR the fourth_id_ of MacroBlockId // FARM COMPAT WHITELIST
                    );

bool ObMetaDiskAddr::is_valid() const
{
  bool ret = false;
  switch (type_) {
    case DiskType::FILE:
      ret = file_id_ > 0
         && (offset_ < ObLogConstants::MAX_LOG_FILE_SIZE)
         && size_ > 0
         && size_ <= ObLogConstants::MAX_LOG_FILE_SIZE;
      break;
    case DiskType::BLOCK:
    case DiskType::RAW_BLOCK:
      ret = second_id_ >= -1 && second_id_ < INT64_MAX && size_ > 0;
      break;
    case DiskType::MEM:
      ret = size_ > 0;
      break;
    case DiskType::NONE:
      ret = true;
      break;
    case DiskType::MAX:
      break;
    default:
      LOG_ERROR("unknown disk address type", K(type_), K(*this));
  }
  return ret;
}

int64_t ObMetaDiskAddr::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  // 1. print detail info of first_id
  block_id().first_id_to_string(buf, buf_len, pos);

  // 2. print other info
  switch (type_) {
  case FILE:
    databuff_printf(buf, buf_len, pos,
                     "[2nd=%lu][3rd=%lu][file_id=%ld,offset=%lu,size=%lu,type=%lu,seq=%lu][5th=%lu][6th=%lu]}",
                     second_id_,
                     third_id_,
                     (uint64_t) offset_,
                     (uint64_t) size_,
                     (uint64_t) type_,
                     (uint64_t) seq_,
                     fifth_id_,
                     sixth_id_,
                     file_id_);
    break;
  default:
    databuff_printf(buf, buf_len, pos,
                    "[2nd=%lu][3rd=%lu][offset=%lu,size=%lu,type=%lu,seq=%lu][trans_seq=%lu, sec_part=%lu][6th=%lu]}",
                    second_id_,
                    third_id_,
                    (uint64_t) offset_,
                    (uint64_t) size_,
                    (uint64_t) type_,
                    (uint64_t) seq_,
                    (uint64_t) block_id().macro_transfer_seq(),
                    (uint64_t) block_id().tenant_seq(),
                    sixth_id_);
    break;
  };

  return pos;
}

bool ObMetaDiskAddr::operator ==(const ObMetaDiskAddr &other) const
{
  return is_equal_for_persistence(other) && sixth_id_ == other.sixth_id_;
}

bool ObMetaDiskAddr::is_equal_for_persistence(const ObMetaDiskAddr &other) const
{
  return first_id_  == other.first_id_
      && second_id_ == other.second_id_
      && third_id_  == other.third_id_
      && fourth_id_ == other.fourth_id_
      && fifth_id_ == other.fifth_id_;
}

bool ObMetaDiskAddr::operator !=(const ObMetaDiskAddr &other) const
{
  return !(other == *this);
}

int ObMetaDiskAddr::memcpy_deserialize(const char* buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0 || pos >= data_len)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", KP(buf), K(data_len), K(pos), K(ret));
  }
  DESERIALIZE_MEMBER_WITH_MEMCPY(first_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(second_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(third_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(fourth_id_);
  DESERIALIZE_MEMBER_WITH_MEMCPY(fifth_id_);
  sixth_id_  = 0;
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

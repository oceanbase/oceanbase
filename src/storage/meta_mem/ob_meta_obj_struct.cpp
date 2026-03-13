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
#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/incremental/atomic_protocol/ob_atomic_tablet_meta_file.h"
#endif

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
  if (is_block()) {
    if (OB_FAIL(get_block_addr_from_block_(macro_id, offset, size))) {
      LOG_WARN("failed to get block addr from block", K(ret), KPC(this));
    }
  }
#ifdef OB_BUILD_SHARED_STORAGE
  else if (is_sslog()) {
    if (OB_FAIL(get_block_addr_from_sslog_tablet_meta_(macro_id, offset, size))) {
      LOG_WARN("failed to get block addr from sslog tablet meta", K(ret), KPC(this));
    }
  }
#endif
  else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type is not supported", K(ret), KPC(this));
  }
  return ret;
}

int ObMetaDiskAddr::get_block_addr_from_block_(
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

#ifdef OB_BUILD_SHARED_STORAGE
int ObMetaDiskAddr::get_block_addr_from_sslog_tablet_meta_(
    blocksstable::MacroBlockId &macro_id,
    int64_t &offset,
    int64_t &size) const
{
  //macro id from sslog, the size and offset is useless, only for desiralize tablet
  int ret = OB_SUCCESS;
  ObStorageObjectOpt opt;
  const int64_t op_id = 0;
  const ObTabletID tablet_id(tablet_id_);
  const ObAtomicFileType type = ObAtomicFileType::TABLET_META;
  if (OB_UNLIKELY(!is_sslog())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("type isn't sslog, not support", K(ret), KPC(this));
  } else if (OB_FAIL(ObAtomicTabletMetaFile::generate_file_obj_opt(type,
                                                        ls_id_,
                                                        tablet_id_,
                                                        op_id,
                                                        tablet_id.is_ls_inner_tablet(),
                                                        opt))) {
    LOG_WARN("generate file obj opt failed", K(ret), K(type_), K(ls_id_), K(tablet_id_));
  } else if (OB_FAIL(ObObjectManager::ss_get_object_id(opt, macro_id))) {
    LOG_WARN("failed to generate obj id", K(ret), K(opt), K(macro_id));
  } else {
    size = size_;
    offset = offset_;
  }
  return ret;
}
#endif

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

/// NOTE: Ignore size of tablet meta which stores at table
int ObMetaDiskAddr::get_size_for_tablet_space_usage(
    /*out*/int64_t &size) const
{
  int ret = OB_SUCCESS;
  MacroBlockId block_id;
  int64_t offset = 0;
  size = 0;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid meta disk addr", K(ret), KPC(this));
  } else if (OB_FAIL(get_block_addr(block_id, offset, size))) {
    LOG_WARN("failed to get block addr", K(ret), KPC(this));
  } else if (block_id.is_shared_tablet_sub_meta_in_table()) {
    size = 0;
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObMetaDiskAddr::set_sslog_tablet_meta_addr(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &reorg_scn,
    const share::SCN &row_scn,
    const int64_t offset,
    const int64_t tablet_serialize_size)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid()
      || !tablet_id.is_valid()
      || !reorg_scn.is_valid()
      || !row_scn.is_valid()
      || offset < 0
      || offset > MAX_OFFSET
      || tablet_serialize_size <= 0
      || tablet_serialize_size > MAX_SIZE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set sslog addr get invalid argument", K(ret), K(ls_id), K(tablet_id), K(reorg_scn), K(row_scn),
      K(offset), K(tablet_serialize_size));
  } else {
    offset_ = offset;
    size_ = tablet_serialize_size;
    type_ = DiskType::SSLOG_TABLET_META;
    ls_id_ = ls_id.id();
    tablet_id_ = tablet_id.id();
    reorg_scn_ = reorg_scn.get_val_for_tx();
    row_scn_ = row_scn.get_val_for_tx();
  }
  return ret;
}

int ObMetaDiskAddr::get_sslog_tablet_meta_addr(
    share::ObLSID &ls_id,
    common::ObTabletID &tablet_id,
    share::SCN &reorg_scn,
    share::SCN &row_scn) const
{
  int ret = OB_SUCCESS;
  ls_id.reset();
  tablet_id.reset();
  reorg_scn.reset();
  row_scn.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr is invalid, unexpected", K(ret), KPC(this));
  } else if (!is_sslog()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr type is not sslog, unexpected", K(ret), KPC(this));
  } else {
    ls_id = ls_id_;
    tablet_id = tablet_id_;
    if (OB_FAIL(reorg_scn.convert_for_tx(reorg_scn_))) {
      LOG_WARN("failed to convert reorg scn", K(ret), KPC(this));
    } else if (OB_FAIL(row_scn.convert_for_tx(row_scn_))) {
      LOG_WARN("failed to conver to row scn", K(ret), KPC(this));
    }
  }
  return ret;
}

int ObMetaDiskAddr::get_sslog_tablet_row_scn(
    share::SCN &row_scn) const
{
  int ret = OB_SUCCESS;
  row_scn.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr is invalid, unexpected", K(ret), KPC(this));
  } else if (!is_sslog()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr type is not sslog, unexpected", K(ret), KPC(this));
  } else if (OB_FAIL(row_scn.convert_for_tx(row_scn_))) {
    LOG_WARN("failed to conver to row scn", K(ret), KPC(this));
  }
  return ret;
}

#endif

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
         && (offset_ < ObLogConstants::MAX_LOG_FILE_SIZE_IN_HISTORY)
         && size_ > 0
         && size_ <= ObLogConstants::MAX_LOG_FILE_SIZE_IN_HISTORY;
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
    case DiskType::SSLOG_TABLET_META:
      ret = ls_id_ > 0 && tablet_id_ > 0 && reorg_scn_ >= 0 && row_scn_ > 0;
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
  MacroBlockId block_id;

  if (is_sslog()) {
    //do nothing
  } else if (OB_SUCCESS != get_macro_block_id(block_id)) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "failed to get block id", KPC(this));
  } else {
    // 1. print detail info of macro_info, 1st, 2nd, 3rd, 5th
    pos = block_id.to_string(buf, buf_len);
  }

  // 2. print offset and other info: 4th, 6th
  switch (type_) {
  case FILE:
    (void)databuff_printf(buf, buf_len, pos,
                     "{[file_id=%ld,offset=%lu,size=%lu,type=%lu,seq=%lu,epoch=%lu][6th=%lu]}",
                     file_id_,
                     (uint64_t) offset_,
                     (uint64_t) size_,
                     (uint64_t) type_,
                     (uint64_t) seq_,
                     (uint64_t) epoch_,
                     sixth_id_);
    break;
  case SSLOG_TABLET_META:
    (void)databuff_printf(buf, buf_len, pos,
                     "{[ls_id=%ld,tablet_id=%lu,reorg_scn=%ld,type=%lu,row_scn=%ld,seq=%lu,epoch=%lu,offset=%lu,size=%lu][6th=%lu]}",
                     ls_id_,
                     tablet_id_,
                     reorg_scn_,
                     (uint64_t) type_,
                     row_scn_,
                     (uint64_t) seq_,
                     (uint64_t) epoch_,
                     (uint64_t) offset_,
                     (uint64_t) size_,
                     sixth_id_);
    break;
  default:
    (void)databuff_printf(buf, buf_len, pos,
                    "{[offset=%lu,size=%lu,type=%lu,seq=%lu,epoch=%lu][6th=%lu]}",
                    (uint64_t) offset_,
                    (uint64_t) size_,
                    (uint64_t) type_,
                    (uint64_t) seq_,
                    (uint64_t) epoch_,
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

// ObMetaDiskAddr order: SSLOG < FILE < BLOCK < RAW_BLOCK
// the FILE disk type is only for empty shell tablet
bool ObMetaDiskAddr::operator<(const ObMetaDiskAddr &r) const
{
  bool b_ret = false;
  if (is_sslog() && r.is_sslog()) {
    if (tablet_id_ < r.tablet_id_) {
      b_ret = true;
    } else {
      b_ret = false;
    }
  } else if (is_sslog()) {
    b_ret = true;
  } else if (r.is_sslog()) {
    b_ret = false;
  } else if (type_ < r.type_) {
    b_ret = true;
  } else if (type_ == r.type_) {
    if (is_block()) {
      MacroBlockId left_block_id(first_id_, second_id_, third_id_, fifth_id_);
      MacroBlockId right_block_id(r.first_id_, r.second_id_, r.third_id_, r.fifth_id_);
      if (left_block_id < right_block_id) {
        b_ret = true;
      } else {
        // addrs in same block are no need to sort between themself by offset
      }
    } else {
      // FILE addrs are no need to sort between themself
    }
  }
  return b_ret;
}

int ObMetaDiskAddr::get_macro_block_id(blocksstable::MacroBlockId &block_id) const
{
  int ret = OB_SUCCESS;
  block_id.reset();
  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta disk addr is invalid, unexpected", K(ret), KPC(this));
  } else if (is_sslog()) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("meta disk addr is sslog meta addr, do not support to change to block id", K(ret), KPC(this));
  } else {
    blocksstable::MacroBlockId id(first_id_, second_id_, third_id_, fifth_id_);
    id.set_version_v2();
    block_id = id;
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase

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

#include "mds_dump_node.h"
#include "lib/list/ob_dlist.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include <type_traits>
#include "storage/multi_data_source/mds_table_handle.h"
#include "meta_programming/ob_meta_swap.h"
#include "mds_dump_obj_printer.h"

using namespace oceanbase::transaction;

namespace oceanbase
{
namespace storage
{
namespace mds
{

void MdsDumpKey::swap(MdsDumpKey &other) noexcept
{
  meta::swap(*this, other, mds_table_id_, mds_unit_id_, crc_check_number_, allocator_, key_);
}

uint32_t MdsDumpKey::generate_hash() const
{
  #define PRINT_WRAPPER K(*this)
  MDS_TG(1_ms);
  uint64_t result = 0;
  result = murmurhash(&mds_table_id_, sizeof(mds_table_id_), result);
  result = murmurhash(&mds_unit_id_, sizeof(mds_unit_id_), result);
  if (!key_.empty()) {
    result = murmurhash(key_.ptr(), key_.length(), result);
  }
  return static_cast<uint32_t>(result);
  #undef PRINT_WRAPPER
}

void MdsDumpKey::reset()
{
  MDS_TG(1_ms);
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(key_.ptr())) {
      allocator_->free(key_.ptr());
      key_.reset();
    }
    allocator_ = nullptr;
  }
  new (this) MdsDumpKey();
}

bool MdsDumpKey::is_valid() const
{
  return mds_table_id_ != UINT8_MAX && mds_unit_id_ != UINT8_MAX;
}

int MdsDumpKey::compare(const MdsDumpKey &rhs, int &result) const
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(rhs), K(result)
  MDS_TG(1_ms);
  int ret = OB_SUCCESS;
  MdsDumpObjComparer helper;
  if (rhs.mds_table_id_ != mds_table_id_) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_NONE(ERROR, "can not compare in different mds table");
  } else if (rhs.mds_unit_id_ != mds_unit_id_) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG_NONE(ERROR, "can not compare in different unit");
  } else if (MDS_FAIL(helper.help_compare<0>(*this, rhs, result))) {
    MDS_LOG_NONE(ERROR, "fail to do compare operation");
  }
  return ret;
  #undef PRINT_WRAPPER
}

MdsDumpNode::MdsDumpNode()
  : mds_table_id_(UINT8_MAX),
    mds_unit_id_(UINT8_MAX),
    crc_check_number_(UINT32_MAX),
    status_(),
    allocator_(nullptr),
    writer_id_(INVALID_VALUE),
    seq_no_(),
    redo_scn_(),
    end_scn_(),
    trans_version_(),
    user_data_()
{
}

void MdsDumpNode::swap(MdsDumpNode &other) noexcept
{
  meta::swap(*this, other,
             mds_table_id_,
             mds_unit_id_,
             crc_check_number_,
             status_,
             allocator_,
             writer_id_,
             seq_no_,
             redo_scn_,
             end_scn_,
             trans_version_,
             user_data_);
}

bool MdsDumpNode::is_valid() const
{
  return mds_table_id_ != UINT8_MAX && mds_unit_id_ != UINT8_MAX;
}

void MdsDumpNode::reset() {
  MDS_TG(1_ms);
  if (OB_NOT_NULL(allocator_)) {
    if (OB_NOT_NULL(user_data_.ptr())) {
      allocator_->free(user_data_.ptr());
    }
    allocator_ = nullptr;
  }
  user_data_.reset();
  new (this) MdsDumpNode();
}

uint32_t MdsDumpNode::generate_hash() const
{
  #define PRINT_WRAPPER K(*this)
  MDS_TG(1_ms);
  uint64_t result = 0;
  result = murmurhash(&mds_table_id_, sizeof(mds_table_id_), result);
  result = murmurhash(&mds_unit_id_, sizeof(mds_unit_id_), result);
  result = murmurhash(&writer_id_, sizeof(writer_id_), result);
  result = murmurhash(&seq_no_, sizeof(seq_no_), result);
  result = murmurhash(&redo_scn_, sizeof(redo_scn_), result);
  result = murmurhash(&end_scn_, sizeof(end_scn_), result);
  result = murmurhash(&trans_version_, sizeof(trans_version_), result);
  result = murmurhash(&status_.union_.value_, sizeof(status_.union_.value_), result);
  if (user_data_.empty()) {
    const int64_t ret = OB_ERR_SYS;//only used for log
    MDS_LOG_NONE(ERROR, "user data should not be empty");
  } else {
    result = murmurhash(user_data_.ptr(), user_data_.length(), result);
  }
  return static_cast<uint32_t>(result);
  #undef PRINT_WRAPPER
}

int64_t MdsDumpKey::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "mds_table_id:%ld, ", (int64_t)mds_table_id_);
  databuff_printf(buf, buf_len, pos, "mds_unit_id:%ld, ", (int64_t)mds_unit_id_);
  databuff_printf(buf, buf_len, pos, "crc_check_number:0x%lx, ", (int64_t)crc_check_number_);
  databuff_printf(buf, buf_len, pos, "allocator:%p, ", (void*)allocator_);
  MdsDumpObjPrinter print_helper;
  print_helper.help_print<0>(true, mds_table_id_, mds_unit_id_, key_, buf, buf_len, pos);
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

int64_t MdsDumpNode::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "mds_table_id:%ld, ", (int64_t)mds_table_id_);
  databuff_printf(buf, buf_len, pos, "mds_unit_id:%ld, ", (int64_t)mds_unit_id_);
  databuff_printf(buf, buf_len, pos, "crc_check_number:0x%lx, ", (int64_t)crc_check_number_);
  databuff_printf(buf, buf_len, pos, "allocator:%p, ", (void*)allocator_);
  databuff_printf(buf, buf_len, pos, "writer_id:%ld, ", writer_id_);
  databuff_printf(buf, buf_len, pos, "seq_no:%s, ", to_cstring(seq_no_));
  databuff_printf(buf, buf_len, pos, "redo_scn:%s, ", obj_to_string(redo_scn_));
  databuff_printf(buf, buf_len, pos, "end_scn:%s, ", obj_to_string(end_scn_));
  databuff_printf(buf, buf_len, pos, "trans_version:%s, ", obj_to_string(trans_version_));
  databuff_printf(buf, buf_len, pos, "status:%s, ", to_cstring(status_));
  MdsDumpObjPrinter print_helper;
  if (user_data_.empty()) {
    databuff_printf(buf, buf_len, pos, "user_data:null");
  } else {
    print_helper.help_print<0>(false, mds_table_id_, mds_unit_id_, user_data_, buf, buf_len, pos);
  }
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

int64_t MdsDumpNode::simple_to_string(char *buf, const int64_t buf_len, int64_t &pos) const
{
  databuff_printf(buf, buf_len, pos, "{status:%s, ", to_cstring(status_));
  MdsDumpObjPrinter print_helper;
  if (user_data_.empty()) {
    databuff_printf(buf, buf_len, pos, "user_data:null");
  } else {
    print_helper.help_print<0>(false, mds_table_id_, mds_unit_id_, user_data_, buf, buf_len, pos);
  }
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

int MdsDumpKey::assign(const MdsDumpKey &rhs, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;

  if (this != &rhs) {
    char *buffer = nullptr;
    const common::ObString &rhs_key = rhs.key_;
    const int64_t length = rhs_key.length();
    reset();

    if (!rhs_key.empty()) {
      if (OB_ISNULL(buffer = static_cast<char *>(alloc.alloc(length)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        MDS_LOG(WARN, "fail to alloc memory", KR(ret), K(length));
      } else if (FALSE_IT(key_.assign_buffer(buffer, length))) {
      } else if (OB_UNLIKELY(length != key_.write(rhs_key.ptr(), length))) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(WARN, "failed to write key", K(ret), K(length));
      }
    }
    if (OB_SUCC(ret)) {
      mds_table_id_ = rhs.mds_table_id_;
      mds_unit_id_ = rhs.mds_unit_id_;
      crc_check_number_ = rhs.crc_check_number_;
      allocator_ = &alloc;
    }
  }

  return ret;
}

int MdsDumpNode::assign(const MdsDumpNode &rhs, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;

  if (this != &rhs) {
    char *buffer = nullptr;
    const common::ObString &rhs_user_data = rhs.user_data_;
    const int64_t length = rhs_user_data.length();
    reset();

    if (0 == length) {
      // dump node is empty, do nothing
    } else if (OB_ISNULL(buffer = static_cast<char *>(alloc.alloc(length)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "fail to alloc memory", KR(ret), K(length));
    }

    if (OB_FAIL(ret)) {
    } else {
      mds_table_id_ = rhs.mds_table_id_;
      mds_unit_id_ = rhs.mds_unit_id_;
      crc_check_number_ = rhs.crc_check_number_;
      status_ = rhs.status_;
      allocator_ = &alloc;
      writer_id_ = rhs.writer_id_;
      seq_no_ = rhs.seq_no_;
      redo_scn_ = rhs.redo_scn_;
      end_scn_ = rhs.end_scn_;
      trans_version_ = rhs.trans_version_;
      if (0 == length) {
      } else if (FALSE_IT(user_data_.assign_buffer(buffer, length))) {
      } else if (OB_UNLIKELY(length != user_data_.write(rhs_user_data.ptr(), length))) {
        ret = OB_ERR_UNEXPECTED;
        MDS_LOG(WARN, "failed to write user data", K(ret), K(length));
      }
    }
  }


  return ret;
}

MdsDumpKV::MdsDumpKV()
  : k_(),
    v_()
{
}

void MdsDumpKV::swap(MdsDumpKV &other)
{
  meta::swap(*this, other, k_, v_);
}

// strong exception safety : if any exception happens, like this action never happened
int MdsDumpKV::convert_from_adapter(common::ObIAllocator &allocator, MdsDumpKVStorageAdapter &adapter)
{
  #define PRINT_WRAPPER KR(ret), K(temp_kv), K(adapter)
  int ret = OB_SUCCESS;
  MdsDumpKV temp_kv;// RAII to ensure exception safty
  if (!adapter.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG_NONE(ERROR, "invalid argument");
  } else {
    // copy key
    char *p_key = nullptr;
    if (adapter.key_.empty() || (p_key = (char *)allocator.alloc(adapter.key_.length()))) {
      MdsDumpKey &key = temp_kv.k_;
      key.mds_table_id_ = adapter.meta_info_.mds_table_id_;
      key.mds_unit_id_ = adapter.type_;
      key.crc_check_number_ = adapter.meta_info_.key_crc_check_number_;
      key.allocator_ = &allocator;
      if (!adapter.key_.empty()) {
        memcpy(p_key, adapter.key_.ptr(), adapter.key_.length());
        key.key_.assign(p_key, adapter.key_.length());
      }
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    // copy data
    if (OB_FAIL(ret)) {
    } else if (char *p_data = (char *)allocator.alloc(adapter.user_data_.length())) {
      MdsDumpNode &value = temp_kv.v_;
      value.mds_table_id_ = adapter.meta_info_.mds_table_id_;
      value.mds_unit_id_ = adapter.type_;
      value.crc_check_number_ = adapter.meta_info_.data_crc_check_number_;
      value.status_ = adapter.meta_info_.status_;
      value.allocator_ = &allocator;
      value.writer_id_ = adapter.meta_info_.writer_id_;
      value.seq_no_ = adapter.meta_info_.seq_no_;
      value.redo_scn_ = adapter.meta_info_.redo_scn_;
      value.end_scn_ = adapter.meta_info_.end_scn_;
      value.trans_version_ = adapter.meta_info_.trans_version_;
      memcpy(p_data, adapter.user_data_.ptr(), adapter.user_data_.length());
      value.user_data_.assign(p_data, adapter.user_data_.length());
    } else {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    // check CRC number
    if (OB_FAIL(ret)) {
    } else {
      uint32_t key_hash = temp_kv.k_.generate_hash();
      uint32_t val_hash = temp_kv.v_.generate_hash();
      if (OB_UNLIKELY(key_hash != temp_kv.k_.crc_check_number_)) {
        ret = OB_CHECKSUM_ERROR;
        MDS_LOG_NONE(ERROR, "key's crc check number is not equal", K(key_hash));
      } else if (OB_UNLIKELY(val_hash != temp_kv.v_.crc_check_number_)) {
        ret = OB_CHECKSUM_ERROR;
        MDS_LOG_NONE(ERROR, "value's crc check number is not equal", K(val_hash));
      } else {
        swap(temp_kv);
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

void MdsDumpKV::reset()
{
  k_.reset();
  v_.reset();
}

int64_t MdsDumpKV::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "{");
  databuff_printf(buf, buf_len, pos, "k:%s, ", to_cstring(k_));
  databuff_printf(buf, buf_len, pos, "v:%s", to_cstring(v_));
  databuff_printf(buf, buf_len, pos, "}");
  return pos;
}

bool MdsDumpKV::is_valid() const
{
  return k_.is_valid() && v_.is_valid(); // TODO(@bowen.gbw): add more rules
}

int MdsDumpKV::assign(const MdsDumpKV &rhs, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (this != &rhs) {
    reset();

    if (OB_FAIL(k_.assign(rhs.k_, alloc))) {
      MDS_LOG(WARN, "fail to assign key", KR(ret));
    } else if (OB_FAIL(v_.assign(rhs.v_, alloc))) {
      MDS_LOG(WARN, "fail to assign value", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (OB_NOT_NULL(k_.key_.ptr())) {
        alloc.free(k_.key_.ptr());
        k_.key_.reset();
      }
      if (OB_NOT_NULL(v_.user_data_.ptr())) {
        alloc.free(v_.user_data_.ptr());
        v_.user_data_.reset();
      }
    }
  }
  return ret;
}

int MdsDumpKV::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              UNIS_VERSION,
              k_,
              v_);

  return ret;
}

int MdsDumpKV::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;

  LST_DO_CODE(OB_UNIS_DECODE,
              version,
              k_);

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(v_.deserialize(allocator, buf, data_len, pos))) {
    MDS_LOG(WARN, "failed to deserialize", K(ret));
  }

  return ret;
}

int64_t MdsDumpKV::get_serialize_size() const
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              UNIS_VERSION,
              k_,
              v_);

  return len;
}

int MdsDumpNode::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  LST_DO_CODE(OB_UNIS_ENCODE,
              UNIS_VERSION,
              mds_table_id_,
              mds_unit_id_,
              writer_id_,
              seq_no_,
              redo_scn_,
              end_scn_,
              trans_version_,
              status_,
              crc_check_number_,
              user_data_);

  return ret;
}

int MdsDumpNode::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  ObString user_data;

  LST_DO_CODE(OB_UNIS_DECODE,
              version,
              mds_table_id_,
              mds_unit_id_,
              writer_id_);


  if (OB_FAIL(ret)) {
  } else if (UNIS_VERSION_V1 == version) {
    int64_t seq_no = 0;
    LST_DO_CODE(OB_UNIS_DECODE, seq_no);
    if (OB_SUCC(ret)) {
      // compat logic
      seq_no_ = (0 == seq_no ? ObTxSEQ::MIN_VAL() : ObTxSEQ::mk_v0(seq_no));
    }
  } else {
    LST_DO_CODE(OB_UNIS_DECODE, seq_no_);
  }

  LST_DO_CODE(OB_UNIS_DECODE,
              redo_scn_,
              end_scn_,
              trans_version_,
              status_,
              crc_check_number_,
              user_data);

  if (OB_SUCC(ret)) {
    allocator_ = &allocator;
    const int64_t len = user_data.length();
    char *buffer = nullptr;
    if (0 == len) {
    } else if (OB_ISNULL(buffer = static_cast<char*>(allocator_->alloc(len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "failed to allocate memory", K(ret), K(len));
    } else {
      MEMCPY(buffer, user_data.ptr(), len);
      user_data_.assign(buffer, len);
    }
  }

  if (OB_FAIL(ret)) {
  } else if (UNIS_VERSION_V1 == version) {
    if (seq_no_.is_min()) {
      crc_check_number_ = generate_hash();
    }
  }

  return ret;
}

int64_t MdsDumpNode::get_serialize_size() const
{
  int64_t len = 0;

  LST_DO_CODE(OB_UNIS_ADD_LEN,
              UNIS_VERSION,
              mds_table_id_,
              mds_unit_id_,
              writer_id_,
              seq_no_,
              redo_scn_,
              end_scn_,
              trans_version_,
              status_,
              crc_check_number_,
              user_data_);

  return len;
}

}
}
}

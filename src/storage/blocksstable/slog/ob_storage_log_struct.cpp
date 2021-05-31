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

#define USING_LOG_PREFIX STORAGE_REDO
#include "ob_storage_log_struct.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase {
using namespace common;
namespace blocksstable {

ObBaseStorageLogHeader::ObBaseStorageLogHeader()
    : trans_id_(0),
      log_seq_(0),
      subcmd_(0),
      log_len_(0),
      tenant_id_(OB_INVALID_TENANT_ID),
      data_file_id_(OB_INVALID_DATA_FILE_ID)
{}

ObBaseStorageLogHeader::~ObBaseStorageLogHeader()
{}

OB_SERIALIZE_MEMBER(ObBaseStorageLogHeader, trans_id_, log_seq_, subcmd_, log_len_, tenant_id_, data_file_id_);

ObBaseStorageLogBuffer::ObBaseStorageLogBuffer() : ObBufferHolder()
{}

ObBaseStorageLogBuffer::~ObBaseStorageLogBuffer()
{}

int ObBaseStorageLogBuffer::assign(char* buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  if (NULL == buf || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments, ", K(ret), KP(buf), K(buf_len));
  } else if (NULL != data_ || 0 != capacity_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The object has been inited.", K(ret));
  } else {
    data_ = buf;
    capacity_ = buf_len;
    pos_ = 0;
  }

  return ret;
}

int ObBaseStorageLogBuffer::append_log(const int64_t trans_id, const int64_t log_seq, const int64_t subcmd,
    const ObStorageLogAttribute& log_attr, ObIBaseStorageLogEntry& data)
{
  int ret = OB_SUCCESS;
  int64_t log_len = data.get_serialize_size();

  int64_t pos = 0;

  if (trans_id < 0 || log_seq < 0 || subcmd < 0 || !data.is_valid() || !log_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(trans_id), K(log_seq), K(subcmd), K(log_attr), K(data));
  } else if (NULL == data_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObBaseStorageLogBuffer has not been inited.", K(ret));
  } else if (OB_FAIL(append_log_head(trans_id, log_seq, subcmd, log_len, log_attr))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      LOG_WARN("Fail to append log head, ", K(ret));
    }
  } else if (OB_FAIL(data.serialize(data_ + pos_, log_len, pos))) {
    LOG_WARN("Fail to serialize data, ", K(ret));
  } else {
    pos_ += log_len;
  }

  return ret;
}

int ObBaseStorageLogBuffer::read_log(ObBaseStorageLogHeader& header, char*& log_data)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(data_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObBaseStorageLogBuffer has not been inited.", K(ret));
  } else if (pos_ >= capacity_) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(header.deserialize(data_, capacity_, pos_))) {
    LOG_WARN("Fail to deserialize log header, ", K(ret));
  } else {
    log_data = data_ + pos_;
    pos_ += header.log_len_;
  }

  return ret;
}

void ObBaseStorageLogBuffer::reset()
{
  data_ = NULL;
  capacity_ = 0;
  pos_ = 0;
}

void ObBaseStorageLogBuffer::reuse()
{
  pos_ = 0;
}

bool ObBaseStorageLogBuffer::is_empty() const
{
  return NULL == data_ || capacity_ <= 0 || pos_ <= 0;
}

DEFINE_SERIALIZE(ObBaseStorageLogBuffer)
{
  int ret = OB_SUCCESS;

  if (NULL == data_) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObBaseStorageLogBuffer has not been inited.", K(ret));
  } else if (NULL == buf || buf_len < 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid arguments, ", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (pos + pos_ > buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    MEMCPY(buf + pos, data_, pos_);
    pos += pos_;
  }

  return ret;
}

DEFINE_DESERIALIZE(ObBaseStorageLogBuffer)
{
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  return OB_NOT_SUPPORTED;
}

DEFINE_GET_SERIALIZE_SIZE(ObBaseStorageLogBuffer)
{
  return pos_;
}

int ObBaseStorageLogBuffer::append_log_head(const int64_t trans_id, const int64_t log_seq, const int64_t subcmd,
    const int64_t log_len, const ObStorageLogAttribute& log_attr)
{
  int ret = OB_SUCCESS;
  if (trans_id < 0 || log_seq < 0 || subcmd < 0 || log_len < 0 || !log_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(trans_id), K(log_seq), K(subcmd), K(log_attr), K(log_len));
  } else {
    ObBaseStorageLogHeader log_header;
    log_header.trans_id_ = trans_id;
    log_header.log_seq_ = log_seq;
    log_header.subcmd_ = subcmd;
    log_header.log_len_ = log_len;
    log_header.tenant_id_ = log_attr.tenant_id_;
    log_header.data_file_id_ = log_attr.data_file_id_;
    int64_t header_len = log_header.get_serialize_size();

    if (NULL == data_) {
      ret = OB_NOT_INIT;
      LOG_WARN("The ObBaseStorageLogBuffer has not been inited.", K(ret));
    } else if (pos_ + header_len + log_len > capacity_) {
      if (header_len + log_len > capacity_) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("The log is too long, ", K(ret), K(capacity_), "len", (header_len + log_len));
      } else {
        ret = OB_BUF_NOT_ENOUGH;
        LOG_WARN("The buffer is not enough, ", K(ret), K(capacity_), K(pos_), "len", (header_len + log_len));
      }
    } else if (OB_FAIL(log_header.serialize(data_, capacity_, pos_))) {
      LOG_WARN("Fail to serialize log header, ", K(ret));
    }
  }
  return ret;
}

ObStorageLogValidRecordEntry::ObStorageLogValidRecordEntry()
{
  MEMSET(extent_list_, 0, sizeof(extent_list_));
  extent_count_ = 0;
  MEMSET(savepoint_list_, 0, sizeof(savepoint_list_));
  savepoint_count_ = 0;
  rollback_count_ = 0;
}

ObStorageLogValidRecordEntry::~ObStorageLogValidRecordEntry()
{}

void ObStorageLogValidRecordEntry::reset()
{
  extent_count_ = 0;
  savepoint_count_ = 0;
  rollback_count_ = 0;
}
bool ObStorageLogValidRecordEntry::is_valid() const
{
  return extent_count_ >= 0 && rollback_count_ >= 0 && savepoint_count_ >= 0;
}

int ObStorageLogValidRecordEntry::find_extent(int64_t log_seq, int64_t& index)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (log_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(log_seq));
  } else {
    for (int64_t i = 0; i < extent_count_; ++i) {
      if (extent_list_[i].start_ <= log_seq && log_seq <= extent_list_[i].end_) {
        index = i;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObStorageLogValidRecordEntry::find_savepoint(int64_t savepoint, int64_t& index)
{
  int ret = OB_ENTRY_NOT_EXIST;
  if (savepoint < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(savepoint));
  } else {
    for (int64_t i = 0; i < savepoint_count_; ++i) {
      if (savepoint_list_[i] == savepoint) {
        index = i;
        ret = OB_SUCCESS;
        break;
      }
    }
  }
  return ret;
}

int ObStorageLogValidRecordEntry::add_extent(int64_t start, int64_t end)
{
  int ret = OB_SUCCESS;
  if (start < 0 || end < 0 || start > end) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(start), K(end));
  } else if (extent_count_ >= MAX_EXTENT_COUNT) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("cannot add more extent entry.", K(ret), K(extent_count_));
  } else {
    extent_list_[extent_count_].start_ = start;
    extent_list_[extent_count_].end_ = end;
    ++extent_count_;
  }
  return ret;
}

int ObStorageLogValidRecordEntry::add_log_entry(int64_t log_seq)
{
  int ret = OB_SUCCESS;

  if (log_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(log_seq));
  } else if (0 == log_seq) {
    if (extent_count_ != 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("add first log entry but already had extents.", K(ret), K(log_seq), K(extent_count_));
    } else if (OB_FAIL(add_extent(log_seq, log_seq))) {
      LOG_WARN("add extent entry failed.", K(ret), K(log_seq), K(extent_count_));
    }
  } else if (extent_count_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add log entry in wrong.", K(ret), K(log_seq), K(extent_count_));
  } else if (log_seq <= extent_list_[extent_count_ - 1].end_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("add log entry in wrong.", K(ret), K(log_seq), K(extent_list_[extent_count_ - 1].end_));
  } else if (log_seq == extent_list_[extent_count_ - 1].end_ + 1) {
    extent_list_[extent_count_ - 1].end_ = log_seq;
  } else if (OB_FAIL(add_extent(log_seq, log_seq))) {
    LOG_WARN("add extent entry failed.", K(ret), K(log_seq), K(extent_count_));
  }
  return ret;
}

int ObStorageLogValidRecordEntry::add_savepoint(int64_t log_seq)
{
  int ret = OB_SUCCESS;
  int64_t index = 0;
  if (log_seq < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(log_seq));
  } else if (OB_FAIL(find_savepoint(log_seq, index))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      if (savepoint_count_ >= MAX_SAVEPOINT_COUNT) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("Too many save point, ", K(ret));
      } else {
        savepoint_list_[savepoint_count_] = log_seq;
        ++savepoint_count_;
      }
    }
  }
  return ret;
}

int ObStorageLogValidRecordEntry::rollback(const int64_t savepoint)
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  int64_t savepoint_index = 0;
  int64_t extent_index = 0;
  if (savepoint < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(savepoint));
  } else if (OB_FAIL(find_savepoint(savepoint, savepoint_index))) {
    LOG_WARN("cannot rollback to the save point does not exist.", K(ret), K(savepoint));
  } else {
    for (i = extent_count_ - 1; i >= 0; --i) {
      if (extent_list_[i].start_ <= savepoint) {
        extent_index = i;
        break;
      }
    }

    if (extent_list_[extent_index].end_ > savepoint) {
      extent_list_[extent_index].end_ = savepoint;
    }
    extent_count_ = extent_index + 1;  // erase all log entry record behind save point;
    savepoint_count_ = savepoint_index + 1;
    ++rollback_count_;
  }
  return ret;
}

DEFINE_SERIALIZE(ObStorageLogValidRecordEntry)
{
  int ret = OB_SUCCESS;
  int64_t version = VALID_RECORD_VERSION;
  int64_t size = get_serialize_size();

  if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, version))) {
    LOG_WARN("fail to encode version, ", K(ret), KP(buf), K(buf_len), K(pos), K(version));
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, pos, size))) {
    LOG_WARN("fail to encode size, ", K(ret), KP(buf), K(buf_len), K(pos), K(size));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, rollback_count_))) {
    LOG_WARN("fail to encode rollback_count_, ", K(ret), KP(buf), K(buf_len), K(pos), K(rollback_count_));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, extent_count_))) {
    LOG_WARN("fail to encode extent_count_, ", K(ret), KP(buf), K(buf_len), K(pos), K(extent_count_));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < extent_count_; ++i) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, extent_list_[i].start_))) {
        LOG_WARN("fail to encode extent start. ", K(ret), KP(buf), K(buf_len), K(pos), K(extent_list_[i].start_));
      } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, extent_list_[i].end_))) {
        LOG_WARN("fail to encode extent end. ", K(ret), KP(buf), K(buf_len), K(pos), K(extent_list_[i].end_));
      }
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObStorageLogValidRecordEntry)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t size = 0;
  int64_t extent_count = 0;

  if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &version))) {
    LOG_WARN("fail to decode version, ", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, pos, &size))) {
    LOG_WARN("fail to decode size, ", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &rollback_count_))) {
    LOG_WARN("fail to decode rollback_count_, ", K(ret), KP(buf), K(data_len), K(pos));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &extent_count))) {
    LOG_WARN("fail to decode extent_count_, ", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    int64_t start = 0;
    int64_t end = 0;
    for (int i = 0; OB_SUCC(ret) && i < extent_count; ++i) {
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &start))) {
        LOG_WARN("fail to decode extent start.", K(ret), KP(buf), K(data_len), K(pos));
      } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &end))) {
        LOG_WARN("fail to decode extent end.", K(ret), KP(buf), K(data_len), K(pos));
      } else if (OB_FAIL(add_extent(start, end))) {
        LOG_WARN("fail to add extent ", K(ret), KP(buf), K(data_len), K(pos));
      }
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObStorageLogValidRecordEntry)
{
  int64_t len = serialization::encoded_length_vi64(rollback_count_);
  len += serialization::encoded_length_vi64(extent_count_);
  for (int i = 0; i < extent_count_; ++i) {
    len += serialization::encoded_length_vi64(extent_list_[i].start_);
    len += serialization::encoded_length_vi64(extent_list_[i].end_);
  }
  // version and size
  len += serialization::encoded_length_i64(0) * 2;
  return len;
}

ObStorageLogActiveTrans::ObStorageLogActiveTrans() : cmd_(OB_LOG_NOP), log_count_(0)
{}

ObStorageLogActiveTrans::~ObStorageLogActiveTrans()
{}

bool ObStorageLogActiveTrans::is_valid() const
{
  return log_count_ >= 0 && valid_record_.is_valid() && log_buffer_.is_valid();
}

int ObStorageLogActiveTrans::append_log(
    const int64_t trans_id, const int64_t subcmd, const ObStorageLogAttribute& log_attr, ObIBaseStorageLogEntry& data)
{
  int ret = OB_SUCCESS;
  if (trans_id < 0 || subcmd < 0 || !data.is_valid() || !log_attr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(ret), K(trans_id), K(subcmd), K(log_attr));
  } else if (OB_FAIL(log_buffer_.append_log(trans_id, log_count_, subcmd, log_attr, data))) {
    LOG_WARN("append log to buffer failed.", K(ret), K(trans_id), K(log_count_), K(subcmd));
  } else if (OB_FAIL(valid_record_.add_log_entry(log_count_))) {
    LOG_WARN("add log to valid record failed.", K(ret), K(trans_id), K(log_count_), K(subcmd));
  } else {
    ++log_count_;
  }
  return ret;
}

int ObStorageLogActiveTrans::assign(char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  if (nullptr == buf || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(buf), K(buf_size));
  } else {
    log_buffer_.reset();
    if (OB_FAIL(log_buffer_.assign(buf, buf_size))) {
      LOG_WARN("assign buf fail", K(ret), KP(buf), K(buf_size));
    }
  }
  return ret;
}

void ObStorageLogActiveTrans::reuse()
{
  cmd_ = common::OB_LOG_NOP;
  log_count_ = 0;
  start_cursor_.reset();
  log_buffer_.reuse();
  valid_record_.reset();
}
}  // end namespace blocksstable
}  // end namespace oceanbase

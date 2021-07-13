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

#include "ob_log_entry.h"

#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/serialization.h"

namespace oceanbase {
using namespace common;
namespace clog {

// clog
ObLogEntry::ObLogEntry() : header_(), buf_(NULL)
{}

ObLogEntry::~ObLogEntry()
{
  destroy();
}

int ObLogEntry::generate_entry(const ObLogEntryHeader& header, const char* buf)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(header_.shallow_copy(header))) {
    // do nothing, add log if necessary, should always be ob_success for now
  } else {
    buf_ = buf;
  }
  return ret;
}

bool ObLogEntry::check_integrity() const
{
  int64_t data_len = header_.get_data_len();
  return header_.check_integrity(buf_, data_len);
}

bool ObLogEntry::check_integrity(const bool ignore_batch_commit_flag)
{
  bool bool_ret = false;
  // get and clear batch_commit mark
  bool is_batch_committed = header_.is_batch_committed();
  if (ignore_batch_commit_flag && is_batch_committed) {
    header_.clear_trans_batch_commit_flag();
  }

  bool_ret = check_integrity();

  // recover batch_commit mark
  if (ignore_batch_commit_flag && is_batch_committed) {
    header_.set_trans_batch_commit_flag();
  }

  return bool_ret;
}

void ObLogEntry::reset()
{
  header_.reset();
  buf_ = NULL;
}

void ObLogEntry::destroy()
{
  buf_ = NULL;
}

bool ObLogEntry::operator==(const ObLogEntry& entry) const
{
  return (header_ == entry.header_) && (0 == MEMCMP(buf_, entry.buf_, header_.get_data_len()));
}

// copy myself to the other
int ObLogEntry::deep_copy_to_(ObLogEntry& entry) const
{
  int ret = OB_SUCCESS;
  char* buf = NULL;
  if (0 == header_.get_data_len() || NULL == buf_) {
    if (OB_FAIL(entry.header_.shallow_copy(header_))) {
      CLOG_LOG(WARN, "header shallow copy fail", K(ret), K(header_));
    } else {
      entry.buf_ = NULL;
      CLOG_LOG(DEBUG, "data is null", K_(header));
    }
  } else {
    buf = static_cast<char*>(TMA_MGR_INSTANCE.alloc_log_entry_buf(header_.get_data_len()));
    if (NULL == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      CLOG_LOG(ERROR, "allocate memory fail", K(ret), "size", header_.get_data_len());
    } else if (OB_FAIL(entry.header_.shallow_copy(header_))) {
      CLOG_LOG(WARN, "header shallow copy fail", K(ret), K(header_));
    } else {
      MEMCPY(buf, buf_, header_.get_data_len());
      entry.buf_ = buf;
    }
  }
  return ret;
}

int ObLogEntry::deep_copy_to(ObLogEntry& entry) const
{
  return deep_copy_to_(entry);
}

int ObLogEntry::get_next_replay_ts_for_rg(int64_t& next_replay_ts) const
{
  int ret = OB_SUCCESS;
  if (OB_LOG_AGGRE == header_.get_log_type()) {
    int64_t pos = 0;
    int32_t next_log_offset = 0;
    const char* log_buf = get_buf();
    const int64_t log_buf_len = header_.get_data_len();
    if (OB_FAIL(serialization::decode_i32(log_buf, log_buf_len, pos, &next_log_offset))) {
      REPLAY_LOG(
          ERROR, "serialization decode_i32 failed", KR(ret), K(log_buf_len), K(pos), K(header_), K(next_log_offset));
    } else if (OB_FAIL(serialization::decode_i64(log_buf, log_buf_len, pos, &next_replay_ts))) {
      REPLAY_LOG(
          ERROR, "serialization decode_i64 failed", KR(ret), K(log_buf_len), K(pos), K(header_), K(next_log_offset));
    } else { /*do nothing*/
    }
  } else {
    // for non_aggregate_log, just assign next_replay_ts with submit_timestamp_
    next_replay_ts = header_.get_submit_timestamp();
  }
  return ret;
}

DEFINE_SERIALIZE(ObLogEntry)
{
  int ret = OB_SUCCESS;
  int64_t data_len = header_.get_data_len();
  int64_t new_pos = pos;
  if (OB_UNLIKELY((NULL == buf) || (buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(TRACE, "header serialize error", K(ret), K(buf_len), K(new_pos));
  } else if ((buf_len - new_pos) < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    CLOG_LOG(TRACE, "buf not enough", K(buf_len), K(data_len), K(new_pos));
  } else {
    MEMCPY(static_cast<char*>(buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObLogEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    ret = OB_DESERIALIZE_ERROR;
    CLOG_LOG(TRACE, "header deserialize error", K(ret), K(data_len), K(new_pos));
  } else if (data_len - new_pos < header_.get_data_len()) {
    ret = OB_DESERIALIZE_ERROR;
    CLOG_LOG(TRACE, "buf is not enough to deserialize clog entry buf", K(ret));
  } else if (header_.get_data_len() < 0) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(WARN, "get invalid data len", K(ret), "data_len", header_.get_data_len());
  } else {
    buf_ = header_.get_data_len() > 0 ? const_cast<char*>(buf + new_pos) : NULL;
    pos = new_pos + header_.get_data_len();
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObLogEntry)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  size += header_.get_data_len();
  return size;
}

// ilog
ObIndexEntry::ObIndexEntry()
    : magic_(0),
      version_(0),
      partition_key_(),
      log_id_(OB_INVALID_ID),
      file_id_(0),
      offset_(-1),
      size_(0),
      submit_timestamp_(0),
      accum_checksum_(0)
{}

int ObIndexEntry::init(const common::ObPartitionKey& partition_key, const uint64_t log_id, const file_id_t file_id,
    const offset_t offset, const int32_t size, const int64_t submit_timestamp, const int64_t accum_checksum,
    const bool batch_committed)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!partition_key.is_valid()) || OB_UNLIKELY(log_id <= 0) || OB_UNLIKELY(!is_valid_file_id(file_id)) ||
      OB_UNLIKELY(!is_valid_offset(offset)) || OB_UNLIKELY(size <= 0) || OB_UNLIKELY(submit_timestamp < 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN,
        "ilog init error",
        K(ret),
        K(partition_key),
        K(log_id),
        K(file_id),
        K(offset),
        K(size),
        K(submit_timestamp),
        K(accum_checksum));
  } else {
    magic_ = INDEX_MAGIC;
    version_ = INDEX_VERSION;
    partition_key_ = partition_key;
    log_id_ = log_id;
    file_id_ = file_id;
    offset_ = offset;
    size_ = size;
    submit_timestamp_ = submit_timestamp;
    accum_checksum_ = accum_checksum;
    if (batch_committed) {
      submit_timestamp_ = submit_timestamp_ | MASK;
    }
  }
  return ret;
}

void ObIndexEntry::reset()
{
  magic_ = 0;
  version_ = 0;
  partition_key_.reset();
  log_id_ = OB_INVALID_ID;
  file_id_ = 0;
  offset_ = 0;
  size_ = 0;
  submit_timestamp_ = 0;
  accum_checksum_ = 0;
}

// copy the other to me
int ObIndexEntry::shallow_copy(const ObIndexEntry& index)
{
  int ret = OB_SUCCESS;
  magic_ = index.magic_;
  version_ = index.version_;
  partition_key_ = index.partition_key_;
  log_id_ = index.log_id_;
  file_id_ = index.file_id_;
  offset_ = index.offset_;
  size_ = index.size_;
  submit_timestamp_ = index.submit_timestamp_;
  accum_checksum_ = index.accum_checksum_;
  return ret;
}

file_id_t ObIndexEntry::get_file_id() const
{
  return file_id_;
}

bool ObIndexEntry::operator==(const ObIndexEntry& index) const
{
  return (magic_ == index.magic_) && (version_ == index.version_) && (partition_key_ == index.partition_key_) &&
         (log_id_ == index.log_id_) && (file_id_ == index.file_id_) && (offset_ == index.offset_) &&
         (size_ == index.size_) && (submit_timestamp_ == index.submit_timestamp_) &&
         (accum_checksum_ == index.accum_checksum_);
}

OB_SERIALIZE_MEMBER(ObIndexEntry, magic_, version_, partition_key_, log_id_, file_id_, offset_, size_,
    submit_timestamp_, accum_checksum_);

ObPaddingEntry::ObPaddingEntry() : magic_(PADDING_MAGIC), version_(PADDING_VERSION), entry_size_(OB_INVALID_SIZE)
{}

uint32_t ObPaddingEntry::get_padding_size(const int64_t offset, const uint32_t align_size)
{
  const ObPaddingEntry padding_entry;
  uint32_t padding_size = 0;
  if (0 == align_size) {
    padding_size = 0;
  } else if (0 != (offset % align_size)) {
    padding_size = (uint32_t)(upper_align(offset + padding_entry.get_serialize_size(), align_size) - offset);
  }
  return padding_size;
}

int64_t ObPaddingEntry::get_entry_size()
{
  return entry_size_;
}

int ObPaddingEntry::set_entry_size(int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t max_size = CLOG_DIO_ALIGN_SIZE + get_serialize_size();
  const int64_t min_size = get_serialize_size();
  if (OB_UNLIKELY(size > max_size || size < min_size)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "padding entry set entry size error, invalid argument", K(ret), K(size), K(max_size), K(min_size));
  } else {
    entry_size_ = size;
  }
  return ret;
}

DEFINE_SERIALIZE(ObPaddingEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "padding entry serialize error", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_))) {
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, version_))) {
  } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, new_pos, entry_size_))) {
  } else {
    pos = new_pos;
  }
  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "padding entry serialize error", K(ret), K(new_pos), K(pos), "entry", *this);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObPaddingEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "padding entry deserialize error", K(ret), KP(buf), K(data_len));
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_))) {
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &version_))) {
  } else if (OB_FAIL(serialization::decode_i64(buf, data_len, new_pos, &entry_size_))) {
  } else {
    pos = new_pos;
  }
  if (OB_FAIL(ret)) {
    CLOG_LOG(
        TRACE, "padding entry deserialize error", K(ret), KP(buf), K(data_len), K(pos), K(new_pos), "entry", *this);
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObPaddingEntry)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i16(version_);
  size += serialization::encoded_length_i64(entry_size_);
  return size;
}

ObCompressedLogEntryHeader::ObCompressedLogEntryHeader()
    : magic_(COMPRESS_INVALID_MAGIC), orig_data_len_(0), compressed_data_len_(0)
{}

ObCompressedLogEntryHeader::~ObCompressedLogEntryHeader()
{
  magic_ = 0;
  orig_data_len_ = 0;
  compressed_data_len_ = 0;
}

void ObCompressedLogEntryHeader::set_meta_len(int32_t original_len, int32_t compressed_len)
{
  orig_data_len_ = original_len;
  compressed_data_len_ = compressed_len;
}

int ObCompressedLogEntryHeader::shallow_copy(const ObCompressedLogEntryHeader& other)
{
  int ret = OB_SUCCESS;
  magic_ = other.magic_;
  orig_data_len_ = other.orig_data_len_;
  compressed_data_len_ = other.compressed_data_len_;
  return ret;
}

int ObCompressedLogEntryHeader::get_compressor_type(common::ObCompressorType& compressor_type) const
{
  int ret = OB_SUCCESS;
  compressor_type = INVALID_COMPRESSOR;
  if (COMPRESS_ZSTD_MAGIC == magic_) {
    compressor_type = ZSTD_COMPRESSOR;
  } else if (COMPRESS_LZ4_MAGIC == magic_) {
    compressor_type = LZ4_COMPRESSOR;
  } else if (COMPRESS_ZSTD_1_3_8_MAGIC == magic_) {
    compressor_type = ZSTD_1_3_8_COMPRESSOR;
  } else {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "compressor type is not supported", K(compressor_type), K(ret));
  }
  return ret;
}

int ObCompressedLogEntryHeader::set_compressor_type(common::ObCompressorType compressor_type)
{
  int ret = OB_SUCCESS;
  if (ZSTD_COMPRESSOR == compressor_type) {
    magic_ = COMPRESS_ZSTD_MAGIC;
  } else if (LZ4_COMPRESSOR == compressor_type) {
    magic_ = COMPRESS_LZ4_MAGIC;
  } else if (ZSTD_1_3_8_COMPRESSOR == compressor_type) {
    magic_ = COMPRESS_ZSTD_1_3_8_MAGIC;
  } else {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid compressor type", K(compressor_type), K(ret));
  }
  return ret;
}

DEFINE_SERIALIZE(ObCompressedLogEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY((buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::encode_i16(buf, buf_len, new_pos, magic_)) ||
             OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, orig_data_len_)) ||
             OB_FAIL(serialization::encode_i32(buf, buf_len, new_pos, compressed_data_len_))) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(TRACE, "ObCompressedLogEntryHeader serialize error", K(buf), K(buf_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObCompressedLogEntryHeader)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || OB_UNLIKELY((data_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(serialization::decode_i16(buf, data_len, new_pos, &magic_)) ||
             OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &orig_data_len_)) ||
             OB_FAIL(serialization::decode_i32(buf, data_len, new_pos, &compressed_data_len_))) {
    ret = OB_DESERIALIZE_ERROR;
    CLOG_LOG(TRACE, "ObCompressedLogEntryHeader serialize error", K(buf), K(data_len), K(pos), K(new_pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObCompressedLogEntryHeader)
{
  int64_t size = 0;
  size += serialization::encoded_length_i16(magic_);
  size += serialization::encoded_length_i32(orig_data_len_);
  size += serialization::encoded_length_i32(compressed_data_len_);
  return size;
}

ObCompressedLogEntry::ObCompressedLogEntry() : header_(), buf_(NULL)
{}

ObCompressedLogEntry::~ObCompressedLogEntry()
{}

DEFINE_SERIALIZE(ObCompressedLogEntry)
{
  int ret = OB_SUCCESS;
  int64_t data_len = header_.get_compressed_data_len();
  int64_t new_pos = pos;
  if (OB_UNLIKELY((NULL == buf) || (buf_len <= 0))) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header_.serialize(buf, buf_len, new_pos))) {
    ret = OB_SERIALIZE_ERROR;
    CLOG_LOG(TRACE, "header serialize error", K(ret), K(buf_len), K(new_pos));
  } else if ((buf_len - new_pos) < data_len) {
    ret = OB_BUF_NOT_ENOUGH;
    CLOG_LOG(TRACE, "buf not enough", K(buf_len), K(data_len), K(new_pos));
  } else {
    MEMCPY(static_cast<char*>(buf + new_pos), buf_, data_len);
    pos = new_pos + data_len;
  }
  return ret;
}

DEFINE_DESERIALIZE(ObCompressedLogEntry)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_UNLIKELY(NULL == buf) || OB_UNLIKELY(data_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(header_.deserialize(buf, data_len, new_pos))) {
    ret = OB_DESERIALIZE_ERROR;
    CLOG_LOG(TRACE, "header deserialize error", K(ret), K(data_len), K(new_pos));
  } else if (data_len - new_pos < header_.get_compressed_data_len()) {
    ret = OB_DESERIALIZE_ERROR;
    CLOG_LOG(TRACE, "buf is not enough to deserialize compressed clog entry", K(ret));
  } else if (header_.get_compressed_data_len() < 0) {
    ret = OB_INVALID_DATA;
    CLOG_LOG(WARN, "get invalid data len", K(ret), K(header_));
  } else {
    buf_ = header_.get_compressed_data_len() > 0 ? const_cast<char*>(buf + new_pos) : NULL;
    pos = new_pos + header_.get_compressed_data_len();
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObCompressedLogEntry)
{
  int64_t size = 0;
  size += header_.get_serialize_size();
  ;
  size += header_.get_compressed_data_len();
  return size;
}

}  // end namespace clog
}  // end namespace oceanbase

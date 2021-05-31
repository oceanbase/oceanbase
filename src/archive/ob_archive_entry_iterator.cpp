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

#include "ob_archive_entry_iterator.h"
#include "lib/utility/utility.h"
#include "ob_archive_log_file_store.h"
#include "ob_log_archive_struct.h"
#include "clog/ob_log_entry.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace clog;
namespace archive {
LogBufPackage::LogBufPackage() : cur_(NULL), end_(NULL), rbuf_()
{}

LogBufPackage::~LogBufPackage()
{
  cur_ = NULL;
  end_ = NULL;
}

bool LogBufPackage::is_empty()
{
  return NULL == rbuf_.buf_ || NULL == cur_ || end_ == cur_;
}

ObArchiveEntryIterator::ObArchiveEntryIterator()
    : is_inited_(false),
      need_limit_bandwidth_(false),
      file_store_(NULL),
      pg_key_(),
      real_tenant_id_(OB_INVALID_TENANT_ID),
      file_id_(OB_INVALID_ARCHIVE_FILE_ID),
      cur_offset_(OB_INVALID_OFFSET),
      cur_block_start_offset_(OB_INVALID_OFFSET),
      cur_block_end_offset_(OB_INVALID_OFFSET),
      buf_cur_(NULL),
      buf_end_(NULL),
      block_end_(NULL),
      rbuf_(),
      read_cost_(),
      dd_buf_(),
      origin_buf_(),
      timeout_(OB_INVALID_TIMESTAMP),
      io_cost_(0),
      io_count_(0),
      limit_bandwidth_cost_(0),
      has_load_entire_file_(false),
      last_block_meta_()
{}

void ObArchiveEntryIterator::reset()
{
  is_inited_ = false;
  need_limit_bandwidth_ = false;
  file_store_ = NULL;
  pg_key_.reset();
  real_tenant_id_ = OB_INVALID_TENANT_ID;
  file_id_ = OB_INVALID_ARCHIVE_FILE_ID;
  cur_offset_ = OB_INVALID_OFFSET;
  cur_block_start_offset_ = OB_INVALID_OFFSET;
  cur_block_end_offset_ = OB_INVALID_OFFSET;
  buf_cur_ = NULL;
  buf_end_ = NULL;
  block_end_ = NULL;

  // if archive block is neither compressed nor encrypt, only buffer pointer is copied,
  // so origin_buf_ does not need to be released here
  origin_buf_.rbuf_.buf_ = NULL;

  if (NULL != rbuf_.buf_) {
    ob_free(rbuf_.buf_);
    rbuf_.buf_ = NULL;
  }

  if (NULL != dd_buf_.rbuf_.buf_) {
    ob_free(dd_buf_.rbuf_.buf_);
    dd_buf_.rbuf_.buf_ = NULL;
  }

  read_cost_.reset();
  timeout_ = OB_INVALID_TIMESTAMP;
  io_cost_ = 0;
  io_count_ = 0;
  limit_bandwidth_cost_ = 0;
  has_load_entire_file_ = false;
  last_block_meta_.reset();
}

int ObArchiveEntryIterator::init(ObIArchiveLogFileStore* file_store, const ObPGKey& pg_key, const uint64_t file_id,
    const int64_t start_offset, const int64_t timeout, const bool need_limit_bandwidth, const uint64_t real_tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    ARCHIVE_LOG(WARN, "ObArchiveEntryIterator has already been inited", KR(ret));
  } else if (OB_UNLIKELY(
                 NULL == file_store || 0 == file_id || !pg_key.is_valid() || 0 > start_offset || 0 >= timeout)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(WARN, "invalid arguments", KR(ret), K(pg_key), KP(file_store), K(file_id), K(start_offset), K(timeout));
  } else if (OB_ISNULL(rbuf_.buf_ = static_cast<char*>(ob_malloc(MAX_READ_BUF_SIZE, "ARCHIVE_ITER")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "failed to alloc rbuf_", KR(ret), K(pg_key), K(file_id), K(start_offset));
  } else if (OB_ISNULL(dd_buf_.rbuf_.buf_ = static_cast<char*>(ob_malloc(MAX_ARCHIVE_BLOCK_SIZE, "ARCHIVE_DD")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    ARCHIVE_LOG(WARN, "failed to alloc dd_buf_", KR(ret), K(pg_key), K(file_id), K(start_offset));
  } else {
    rbuf_.buf_len_ = MAX_READ_BUF_SIZE;
    file_store_ = file_store;
    pg_key_ = pg_key;
    // only for restore
    real_tenant_id_ = real_tenant_id == OB_INVALID_TENANT_ID ? pg_key_.get_tenant_id() : real_tenant_id;
    file_id_ = file_id;
    cur_offset_ = start_offset;
    cur_block_start_offset_ = start_offset;
    cur_block_end_offset_ = start_offset;
    timeout_ = timeout;
    buf_cur_ = NULL;
    buf_end_ = NULL;
    need_limit_bandwidth_ = need_limit_bandwidth;
    io_cost_ = 0;
    io_count_ = 0;
    limit_bandwidth_cost_ = 0;
    is_inited_ = true;
  }
  if (OB_SUCCESS != ret && OB_INIT_TWICE != ret) {
    destroy();
  }
  ARCHIVE_LOG(TRACE, "success to init archive entry iterator", KR(ret), KPC(this));
  return ret;
}

int ObArchiveEntryIterator::next_entry(clog::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    ARCHIVE_LOG(WARN, "ObArchiveEntryIterator is not inited", KR(ret));
  } else {
    bool done = false;
    do {
      // 1. consume buf, get log entry
      if (OB_SUCC(ret)) {
        // if deserialize buffer is not enough in get_next_entry_, return OB_BUF_NOT_ENOUGH
        if (OB_FAIL(get_next_entry_(entry))) {
          if (OB_BUF_NOT_ENOUGH == ret && has_load_entire_file_) {
            ret = OB_ITER_END;
          } else if (OB_EAGAIN != ret && OB_BUF_NOT_ENOUGH != ret) {
            ARCHIVE_LOG(WARN, "get next entry fail", KR(ret), KPC(this));
          } else {
            if (REACH_TIME_INTERVAL(10 * 1000 * 1000L)) {
              // retry is normal
              ARCHIVE_LOG(TRACE, "buffer not enough or need retry", KR(ret), K(file_id_), K(cur_offset_));
            }
          }
        } else {
          done = true;
        }
      }

      // 2. if buf is not enough, read archived file
      // reset OB_BUF_NOT_ENOUGH
      if (OB_BUF_NOT_ENOUGH == ret) {
        if (OB_FAIL(prepare_buffer_())) {
          if (OB_ITER_END != ret) {
            ARCHIVE_LOG(WARN, "prepare buffer error", KR(ret), KPC(this));
          }
        } else {
          ARCHIVE_LOG(TRACE, "prepare buffer succ", K(pg_key_), K(file_id_));
        }
      }
    } while (OB_EAGAIN == ret || (OB_SUCCESS == ret && !done));
  }

  ARCHIVE_LOG(TRACE, "raw iter next_entry", KR(ret), K(entry));

  return ret;
}

int ObArchiveEntryIterator::prepare_buffer_()
{
  int ret = OB_SUCCESS;
  ObArchiveReadParam param;
  param.file_id_ = file_id_;
  param.offset_ = cur_offset_;
  param.read_len_ = MAX_READ_BUF_SIZE;
  param.timeout_ = timeout_;
  param.pg_key_ = pg_key_;
  ObReadRes read_res;

  const int64_t begin_time = ObClockGenerator::getClock();
  if (OB_FAIL(file_store_->read_data_direct(param, rbuf_, read_res)) && OB_BACKUP_FILE_NOT_EXIST != ret) {
    ARCHIVE_LOG(ERROR, "failed to read_data_direct", KR(ret), K(param), KPC(this));
  } else if (0 == read_res.data_len_ || OB_BACKUP_FILE_NOT_EXIST == ret) {
    // To end
    ret = OB_ITER_END;
  } else if (OB_ISNULL(read_res.buf_) || OB_UNLIKELY(0 > read_res.data_len_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(WARN, "read result is invalid", KR(ret), K(param), K(read_res));
  } else {
    // update buffer pointers
    buf_cur_ = const_cast<char*>(read_res.buf_);
    buf_end_ = buf_cur_ + read_res.data_len_;

    if (read_res.data_len_ < param.read_len_) {
      has_load_entire_file_ = true;
    }
    const int64_t io_end_time = ObClockGenerator::getClock();
    io_cost_ += io_end_time - begin_time;
    io_count_++;

    if (need_limit_bandwidth_) {
      int tmp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (tmp_ret = (limit_bandwidth_and_sleep_(read_res.data_len_)))) {
        ARCHIVE_LOG(WARN, "read result is invalid", KR(tmp_ret), K(pg_key_), K(read_res));
      } else {
        const int64_t limit_bandwidth_end_time = ObClockGenerator::getClock();
        limit_bandwidth_cost_ += limit_bandwidth_end_time - io_end_time;
      }
    }
  }

  return ret;
}

void ObArchiveEntryIterator::advance_(const int64_t step)
{
  cur_offset_ += step;
  ARCHIVE_LOG(TRACE, "advance", K(step), K(file_id_), K(cur_offset_), KP(buf_cur_));
}

int ObArchiveEntryIterator::parse_archive_item_type(const char* buf, const int64_t len, ObArchiveItemType& type)
{
  int ret = OB_SUCCESS;
  type = UNKNOWN_TYPE;
  int16_t magic = 0;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || len < serialization::encoded_length(magic)) {
    ret = OB_INVALID_ARGUMENT;
    ARCHIVE_LOG(ERROR, "invalid argument", KR(ret), KP(buf), K(len));
  } else if (OB_FAIL(serialization::decode_i16(buf, len, pos, &magic))) {
    ARCHIVE_LOG(ERROR, "decode magic number fail", KR(ret), KP(buf), K(pos), K(magic));
  } else {
    // distinguish by magic number
    if (ObArchiveBlockMeta::check_magic_number(magic)) {
      type = ARCHIVE_BLOCK_META;
    } else if (ObLogEntryHeader::check_magic_number(magic)) {
      type = CLOG_ENTRY;
    } else if (ObArchiveCompressedChunkHeader::check_magic_number(magic)) {
      type = ARCHIVE_COMPRESSED_CHUNK;
    } else {
      ret = OB_ERR_UNEXPECTED;
      ARCHIVE_LOG(ERROR, "unexpected magic num", KR(ret), K(magic));
    }
  }
  return ret;
}

int ObArchiveEntryIterator::get_entry_type_(ObArchiveItemType& type) const
{
  int ret = OB_SUCCESS;
  int64_t remain_buf_len = buf_end_ - buf_cur_;
  if (OB_ISNULL(buf_cur_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "buf_cur_ is NULL", KR(ret), KPC(this));
  } else if (remain_buf_len < MAGIC_NUM_LEN) {
    // buf not enough
    ret = OB_BUF_NOT_ENOUGH;
    ARCHIVE_LOG(TRACE, "remain_buf is not enough", K(remain_buf_len), KPC(this));
  } else if (OB_FAIL(parse_archive_item_type(buf_cur_, sizeof(int16_t), type))) {
    ARCHIVE_LOG(ERROR, "get_archive_item_type fail", KR(ret), K(type));
  } else { /*do nothing*/
  }
  return ret;
}

// only ObLogEntry exist in dd_buf_ / origin_buf_
int ObArchiveEntryIterator::get_next_entry_(clog::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  bool done = false;

  // 0. prepare data buffer
  if (buf_cur_ != NULL && block_end_ - buf_cur_ == ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE) {
    // four bytes(record the offset of pre block in the archived file) exist after a whole block
    // need skip these four bytes
    buf_cur_ += ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
  }

  // 1. produce log entry buffer
  if (dd_buf_.is_empty() && origin_buf_.is_empty()) {
    if (OB_FAIL(try_construct_log_buf_())) {
      if (OB_EAGAIN != ret && OB_BUF_NOT_ENOUGH != ret) {
        ARCHIVE_LOG(WARN, "try_construct_log_buf_ fail", KR(ret), K(pg_key_));
      }
    }
  }

  // 2. consume dd buffer
  if (OB_SUCC(ret) && !dd_buf_.is_empty()) {
    if (OB_FAIL(consume_decompress_decrypt_buf_(entry))) {
      ARCHIVE_LOG(WARN, "consume_decompress_decrypt_buf_ fail", KR(ret), K(pg_key_));
    } else {
      done = true;
      ARCHIVE_LOG(TRACE, "consume_decompress_decrypt_buf_ succ", KR(ret), K(entry));
    }
  }

  // 3. consume origin buffer
  if (OB_SUCC(ret) && !done && !origin_buf_.is_empty()) {
    if (OB_FAIL(consume_origin_buf_(entry))) {
      ARCHIVE_LOG(WARN, "consume_origin_buf_ fail", KR(ret), K(pg_key_));
    } else {
      ARCHIVE_LOG(TRACE, "consume_origin_buf_ succ", KR(ret), K(entry));
    }
  }

  return ret;
}

void ObArchiveEntryIterator::reset_log_buf_package_()
{
  dd_buf_.cur_ = NULL;
  dd_buf_.end_ = NULL;

  origin_buf_.cur_ = NULL;
  origin_buf_.end_ = NULL;
}

int ObArchiveEntryIterator::consume_decompress_decrypt_buf_(clog::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_FAIL(get_log_entry_(dd_buf_.cur_, dd_buf_.end_ - dd_buf_.cur_, pos, entry))) {
    ARCHIVE_LOG(WARN, "get_log_entry_ fail", KR(ret), K(pg_key_));
  } else {
    dd_buf_.cur_ += pos;
  }

  return ret;
}

int ObArchiveEntryIterator::consume_origin_buf_(clog::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_FAIL(get_log_entry_(origin_buf_.cur_, origin_buf_.end_ - origin_buf_.cur_, pos, entry))) {
    ARCHIVE_LOG(WARN, "get_log_entry_ fail", KR(ret), K(pg_key_));
  } else {
    origin_buf_.cur_ += pos;
  }

  return ret;
}

int ObArchiveEntryIterator::get_log_entry_(char* buf, const int64_t buf_size, int64_t& pos, clog::ObLogEntry& entry)
{
  int ret = OB_SUCCESS;
  pos = 0;
  const bool ignore_batch_commit_flag = true;

  if (OB_FAIL(entry.deserialize(buf, buf_size, pos))) {
    ARCHIVE_LOG(ERROR, "ObLogEntry deserialize fail", KR(ret), K(pg_key_));
  } else if (OB_UNLIKELY(!entry.check_integrity(ignore_batch_commit_flag))) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(ERROR, "ObLogEntry check integrity fail", KR(ret), K(pg_key_));
  }

  return ret;
}

int ObArchiveEntryIterator::try_construct_log_buf_()
{
  int ret = OB_SUCCESS;
  ObArchiveItemType item_type = UNKNOWN_TYPE;
  ObArchiveBlockMeta block_meta;

  // reset log buf package
  reset_log_buf_package_();

  if (NULL == buf_end_ || buf_end_ - buf_cur_ <= 0) {
    // no data exist, need prepare buffer
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_FAIL(extract_block_meta_(block_meta))) {
    if (OB_BUF_NOT_ENOUGH != ret) {
      ARCHIVE_LOG(WARN, "extract_block_meta_ fail", KR(ret), K(pg_key_));
    }
  } else if (OB_FAIL(get_entry_type_(item_type))) {
    ARCHIVE_LOG(WARN, "get_entry_type_ fail", KR(ret), K(pg_key_), K(block_meta));
  } else {
    switch (item_type) {
      case ARCHIVE_COMPRESSED_CHUNK:
        ret = decompress_buf_(block_meta);
        break;
      case ARCHIVE_ENCRYPTED_CHUNK:
        ret = OB_NOT_SUPPORTED;
        break;
      case ARCHIVE_COMPRESSED_ENCRYPTED_CHUNK:
        ret = OB_NOT_SUPPORTED;
        break;
      case CLOG_ENTRY:
        ret = fill_origin_buf_(block_meta);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        ARCHIVE_LOG(ERROR, "unexpected archive item type", KR(ret), K(item_type), K(pg_key_), K(block_meta));
        break;
    }
  }

  return ret;
}

int ObArchiveEntryIterator::extract_block_meta_(ObArchiveBlockMeta& meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_FAIL(meta.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
    handle_serialize_ret_(ret);
  } else if (buf_end_ - buf_cur_ < meta.get_total_len()) {
    ret = OB_BUF_NOT_ENOUGH;
  } else if (OB_UNLIKELY(!meta.check_meta_checksum())) {
    ret = OB_INVALID_DATA;
    ARCHIVE_LOG(WARN, "check block meta checksum error, maybe not integrated block", KR(ret), K(pg_key_), K(meta));
  } else {
    const int64_t len = pos + meta.get_data_len() + ARCHIVE_BLOCK_META_OFFSET_ENCODE_SIZE;
    last_block_meta_ = meta;
    advance_(len);
    block_end_ = buf_cur_ + len;
    buf_cur_ += pos;
    ARCHIVE_LOG(TRACE, "extract block meta succ", K(pg_key_), K(file_id_), K(meta));
  }

  return ret;
}

int ObArchiveEntryIterator::decompress_buf_(ObArchiveBlockMeta& block_meta)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  const int64_t dst_buf_size = MAX_ARCHIVE_BLOCK_SIZE;
  int64_t decompress_data_size = 0;
  const int64_t chunk_size = block_meta.get_data_len();
  ObArchiveCompressedChunk compress_chunk;
  const ObArchiveCompressedChunkHeader& compress_header = compress_chunk.get_header();

  // 1. deserialize CompressedChunk
  if (OB_FAIL(compress_chunk.deserialize(buf_cur_, chunk_size, pos))) {
    ARCHIVE_LOG(WARN, "compress chunk deserialize fail", KR(ret), K(pg_key_), K(block_meta));
  } else if (OB_UNLIKELY(!compress_header.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid compress header", KR(ret), K(pg_key_), K(compress_header));
  } else {
    buf_cur_ += pos;
  }

  // 2. decompress data buffer
  if (OB_SUCC(ret)) {
    const char* compressed_buf = compress_chunk.get_buf();
    common::ObCompressorType compress_type = compress_header.get_compressor_type();
    const int64_t compress_data_len = compress_header.get_compressed_data_len();
    if (OB_FAIL(decompress_(compressed_buf,
            compress_data_len,
            compress_type,
            dd_buf_.rbuf_.buf_,
            dst_buf_size,
            decompress_data_size))) {
      ARCHIVE_LOG(WARN, "decompress_ fail", KR(ret), K(pg_key_), K(block_meta), K(compress_data_len));
    } else {
      fill_log_package_(dd_buf_.rbuf_.buf_, decompress_data_size);
      ARCHIVE_LOG(TRACE, "decompress buf succ", K(pg_key_), K(file_id_), K(block_meta));
    }
  }

  return ret;
}

int ObArchiveEntryIterator::decompress_(const char* src_buf, const int64_t src_size,
    common::ObCompressorType compress_type, char* dst_buf, const int64_t dst_buf_size, int64_t& dst_data_size)
{
  int ret = OB_SUCCESS;
  common::ObCompressor* compressor = NULL;

  if (OB_FAIL(common::ObCompressorPool::get_instance().get_compressor(compress_type, compressor))) {
    ARCHIVE_LOG(WARN, "get_compressor fail", KR(ret), K(pg_key_), K(compress_type));
  } else if (OB_ISNULL(compressor)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "compressor is NULL", KR(ret), K(compress_type), K(compressor));
  } else if (OB_FAIL(compressor->decompress(src_buf,  // src_buf
                 src_size,                            // src_size
                 dst_buf,                             // dst_buf
                 dst_buf_size,                        // dst_buf size
                 dst_data_size /*dst_data size*/))) {
    ARCHIVE_LOG(WARN, "decompress fail", KR(ret), K(pg_key_));
  } else if (OB_UNLIKELY(MAX_ARCHIVE_BLOCK_SIZE < dst_data_size || 0 >= dst_data_size)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "invalid dst_data_size", KR(ret), K(pg_key_), K(dst_data_size));
  }

  return ret;
}

void ObArchiveEntryIterator::fill_log_package_(char* buf, const int64_t buf_len)
{
  dd_buf_.cur_ = buf;
  dd_buf_.end_ = buf + buf_len;
}

int ObArchiveEntryIterator::fill_origin_buf_(ObArchiveBlockMeta& block_meta)
{
  int ret = OB_SUCCESS;
  const int64_t log_buf_len = block_meta.get_data_len();
  origin_buf_.rbuf_.buf_ = buf_cur_;
  origin_buf_.rbuf_.buf_len_ = log_buf_len;

  origin_buf_.cur_ = buf_cur_;
  origin_buf_.end_ = buf_cur_ + log_buf_len;
  buf_cur_ += log_buf_len;
  return ret;
}

void ObArchiveEntryIterator::handle_serialize_ret_(int& ret_code)
{
  if (OB_DESERIALIZE_ERROR == ret_code) {
    // buf not enough
    ret_code = OB_BUF_NOT_ENOUGH;
  } else {
    ARCHIVE_LOG(ERROR, "deserialize error, maybe data is corrupted", KR(ret_code), K(pg_key_));
  }
}

int ObArchiveEntryIterator::limit_bandwidth_and_sleep_(const int64_t read_size)
{
  int ret = OB_SUCCESS;
  common::ObInOutBandwidthThrottle* bandwidth_throttle = NULL;
  const int64_t last_active_time = ObTimeUtility::current_time();
  if (OB_ISNULL(bandwidth_throttle = GCTX.bandwidth_throttle_)) {
    ret = OB_ERR_UNEXPECTED;
    ARCHIVE_LOG(ERROR, "bandwidth_throttle is NULL", K(pg_key_), K(read_size), K(last_active_time), KR(ret));
  } else if (OB_FAIL(bandwidth_throttle->limit_in_and_sleep(read_size, last_active_time, MAX_IDLE_TIME))) {
    ARCHIVE_LOG(WARN, "failed to limit_in_and_sleep", K(pg_key_), K(read_size), K(last_active_time), KR(ret));
  }
  return ret;
}

}  // end of namespace archive
}  // namespace oceanbase

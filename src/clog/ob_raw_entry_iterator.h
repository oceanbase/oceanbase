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

#ifndef OCEANBASE_CLOG_OB_RAW_ENTRY_ITERATOR_H_
#define OCEANBASE_CLOG_OB_RAW_ENTRY_ITERATOR_H_

#include "lib/compress/ob_compressor_pool.h"
#include "ob_log_block.h"
#include "ob_log_common.h"
#include "ob_log_define.h"
#include "ob_log_compress.h"
#include "ob_log_reader_interface.h"
#include "storage/blocksstable/ob_store_file_system.h"

namespace oceanbase {
namespace clog {
enum ObCLogItemType {
  UNKNOWN_TYPE = 0,
  CLOG_ENTRY = 1,
  ILOG_ENTRY = 2,
  BLOCK_META = 3,
  PADDING_ENTRY = 4,
  // Version 2.0 will no longer write EOF, but in order to be compatible with logs
  // written by version 1.x, this flag is still required
  EOF_BUF = 5,
  //  NEW_LOG_FILE_TYPE is used to distinguish the file after reused
  NEW_LOG_FILE_TYPE = 6,
  CLOG_ENTRY_COMPRESSED_ZSTD = 7,
  CLOG_ENTRY_COMPRESSED_LZ4 = 8,
  CLOG_ENTRY_COMPRESSED_ZSTD_138 = 9,
};

inline int parse_log_item_type(const char* buf, const int64_t len, ObCLogItemType& type)
{
  int ret = common::OB_SUCCESS;
  type = UNKNOWN_TYPE;
  int16_t magic = 0;
  int64_t pos = 0;

  if (OB_ISNULL(buf) || len < common::serialization::encoded_length(magic)) {
    ret = common::OB_INVALID_ARGUMENT;
    CLOG_LOG(ERROR, "invalid argument", KP(buf), K(len));
  } else if (OB_FAIL(common::serialization::decode_i16(buf, len, pos, &magic))) {
    CLOG_LOG(ERROR, "decode magic number fail", KP(buf), K(pos), K(magic));
  } else {
    // distinguish by magic number
    if (ObLogBlockMetaV2::check_magic_number(magic)) {
      type = BLOCK_META;
    } else if (*buf == 'E' && *(buf + 1) == 'R') {
      type = CLOG_ENTRY;
    } else if (*buf == 'P' && *(buf + 1) == 'D') {
      type = PADDING_ENTRY;
    } else if (*buf == 'e' && *(buf + 1) == 'n') {
      type = EOF_BUF;
    } else if (*buf == 'N' && *(buf + 1) == 'L') {
      type = NEW_LOG_FILE_TYPE;
    } else if (*buf == 'C' && *(buf + 1) == 0x01) {
      type = CLOG_ENTRY_COMPRESSED_ZSTD;
    } else if (*buf == 'C' && *(buf + 1) == 0x02) {
      type = CLOG_ENTRY_COMPRESSED_LZ4;
    } else if (*buf == 'C' && *(buf + 1) == 0x03) {
      type = CLOG_ENTRY_COMPRESSED_ZSTD_138;
    } else {
      type = ILOG_ENTRY;
    }
  }
  return ret;
}

// ObRawEntryIterator has the following four usage scenarios:
// 1. Iterate the last file in the startup phase and confirm the start_file_id(start_offset) written by log
// writer.
// 2. In restart phase, used for scan_runnable to scan all files.
// 3. Used for ilog cache for iterating a complete ilog file.
// 4. Used last_file_cursor for iterating last ilog file.
//
// The use of these four scenarios is different:
// 1. Only read one file(last file), read from offset equal zero, file trailer is invalid;
// 2. Read more than ones file, read from offset equal zero, trailer is valid.
// 3. Only read one file(any file), read from offset equal zero, file trailer is valid;
// 4. Only read one file(last file), read from any offset, file trailer is valid;
//
// Regardless of Whethter the offset is zero, the implementation can ensure that the offset must correspond
// to a valid block header.
//
// When tailer is valid, we can confirm that the end of the file is read by direct_reader returning
// OB_READ_NOTHING.
// When tailer is invalid, we must confirm that the end of the file is to read by using logical judgment.
//
// If position after cur_offset_ is a valid block, meanwhile, the timestamp record in block header is smaller
// than the timestamp has been read, it is read to the end of the file.
// If position after cur_offset_ is an invalid block(failed to check magic, or failed to check meat checksum,
// or the magic of this block is a ilog entry or clog entry), we need to read all subsequent contents of this
// file, check whether there is a valid block and the timestamp recorded in the block header is greater than
// or equal to last_block_ts, if not, the end of the file is read.
template <class Type, class Interface>
class ObRawEntryIterator : public Interface {
public:
  ObRawEntryIterator();
  virtual ~ObRawEntryIterator();

public:
  int init(ObILogDirectReader* direct_reader, const file_id_t start_file_id, const offset_t start_offset,
      const file_id_t end_file_id, const int64_t timeout);
  void destroy();
  int next_entry(Type& entry, ObReadParam& param, int64_t& persist_len);
  ObReadCost get_read_cost() const
  {
    return read_cost_;
  }

private:
  ObCLogItemType get_type_() const;
  inline void advance_(const int64_t step);
  inline int get_next_file_id_(file_id_t& next_file_id);
  inline int switch_file_();
  int handle_block_(const ObLogBlockMetaV2& meta);
  int prepare_buffer_(bool force_read);
  int check_read_result_(const ObReadParam& param, const ObReadRes& res) const;
  int get_next_entry_(Type& entry, ObReadParam& param, bool& force_read, int64_t& persist_len);
  bool check_last_block_(const file_id_t file_id, const offset_t start_offset, const int64_t last_block_ts) const;
  bool is_compressed_item_(const ObCLogItemType item_type) const;
  int check_compressed_entry_length_(const char* buf, const int64_t buf_size) const;

private:
  // magic number length in byte.
  //   "en"    EOF
  //   "LB"    block meta
  //   "ER"    clog entry
  //   "PD"    padding entry
  //   "NL"    new log file
  //   5Bytes  ilog entry.
  static const int64_t MAGIC_NUM_LEN = 2;
  // When timestamp in block header is less than last_block_ts and exceeds CHECK_LAST_BLOCK_TS_INTERVAL,
  // considered ITER_END.
  // This is mainly to prevent clock rollback. The 1.4x version used ObTimeUtility::current_time to get
  // the timestamp for block header, if clock jumps, may cause iterate file failuer.
  //
  // We used ObClockGenerator::getClock to ensure that the monotonic increase of the clock.
  //
  // Since our log disk space is large enough, log files will not be reused within two seconds,
  // so this constant is safe in the scenario of reusing files.
  static const int64_t CHECK_LAST_BLOCK_TS_INTERVAL = 2000 * 1000;  // 2s
private:
  bool is_inited_;
  ObILogDirectReader* reader_;
  file_id_t file_id_;
  file_id_t end_file_id_;
  offset_t cur_offset_;
  offset_t cur_block_start_offset_;
  offset_t cur_block_end_offset_;
  int64_t last_block_ts_;
  int64_t timeout_;
  ObReadBuf rbuf_;
  ObReadBuf trailer_rbuf_;
  ObReadBuf compress_rbuf_;
  char* buf_cur_;
  char* buf_end_;
  ObReadCost read_cost_;
  DISALLOW_COPY_AND_ASSIGN(ObRawEntryIterator);
};

template <class Type, class Interface>
ObRawEntryIterator<Type, Interface>::ObRawEntryIterator()
    : is_inited_(false),
      reader_(NULL),
      file_id_(common::OB_INVALID_FILE_ID),
      end_file_id_(common::OB_INVALID_FILE_ID),
      cur_offset_(OB_INVALID_OFFSET),
      cur_block_start_offset_(OB_INVALID_OFFSET),
      cur_block_end_offset_(OB_INVALID_OFFSET),
      last_block_ts_(common::OB_INVALID_TIMESTAMP),
      timeout_(0),
      rbuf_(),
      trailer_rbuf_(),
      compress_rbuf_(),
      buf_cur_(NULL),
      buf_end_(NULL),
      read_cost_()
{}

template <class Type, class Interface>
ObRawEntryIterator<Type, Interface>::~ObRawEntryIterator()
{
  destroy();
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::init(ObILogDirectReader* reader, const file_id_t start_file_id,
    const offset_t start_offset, const file_id_t end_file_id, const int64_t timeout)
{
  int ret = common::OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRawEntryIterator has already been inited", K(ret));
  } else if (OB_UNLIKELY(NULL == reader || !is_valid_file_id(start_file_id) || !is_valid_offset(start_offset) ||
                         0 >= timeout)) {
    ret = common::OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "raw iterator init error", K(ret), KP(reader), K(start_file_id), K(start_offset), K(timeout));
  } else {
    reader_ = reader;
    if (OB_FAIL(reader_->alloc_buf(common::ObModIds::OB_LOG_DIRECT_READER_ITER_ID, rbuf_))) {
      CLOG_LOG(WARN, "alloc buffer error", K(ret), K(rbuf_));
    } else if (OB_FAIL(reader_->alloc_buf(common::ObModIds::OB_LOG_DIRECT_READER_ITER_ID, trailer_rbuf_))) {
      CLOG_LOG(WARN, "alloc buffer error", K(ret), K(trailer_rbuf_));
    } else if (OB_FAIL(reader_->alloc_buf(common::ObModIds::OB_LOG_DIRECT_READER_ITER_ID, compress_rbuf_))) {
      CLOG_LOG(WARN, "alloc buffer error", K(ret), K(trailer_rbuf_));
    } else {
      file_id_ = start_file_id;
      end_file_id_ = end_file_id;
      cur_offset_ = start_offset;
      cur_block_start_offset_ = start_offset;
      cur_block_end_offset_ = start_offset;
      last_block_ts_ = common::OB_INVALID_TIMESTAMP;
      timeout_ = timeout;
      is_inited_ = true;
    }
  }
  if (common::OB_SUCCESS != ret && common::OB_INIT_TWICE != ret) {
    destroy();
  }
  CLOG_LOG(TRACE, "raw iterator init finish", K(ret), K(start_file_id), K(start_offset));
  return ret;
}

template <class Type, class Interface>
void ObRawEntryIterator<Type, Interface>::destroy()
{
  if (is_inited_) {
    is_inited_ = false;
    file_id_ = common::OB_INVALID_FILE_ID;
    end_file_id_ = common::OB_INVALID_FILE_ID;
    cur_offset_ = OB_INVALID_OFFSET;
    cur_block_start_offset_ = OB_INVALID_OFFSET;
    cur_block_end_offset_ = OB_INVALID_OFFSET;
    last_block_ts_ = common::OB_INVALID_TIMESTAMP;
    timeout_ = common::OB_INVALID_TIMESTAMP;
    buf_cur_ = NULL;
    buf_end_ = NULL;
    read_cost_.reset();
    if (NULL != reader_) {
      reader_->free_buf(rbuf_);
      reader_->free_buf(trailer_rbuf_);
      reader_->free_buf(compress_rbuf_);
    }
    reader_ = NULL;
  }
}

// advance_ cur_offset_ and buf_cur_ by step
template <class Type, class Interface>
void ObRawEntryIterator<Type, Interface>::advance_(const int64_t step)
{
  // we may advance_ buf_cur_ far beyond buf_end_ in which case we will prepare next buffer.
  cur_offset_ += static_cast<offset_t>(step);
  buf_cur_ += step;
  CLOG_LOG(DEBUG, "advance", K(step), K(file_id_), K(end_file_id_), K(cur_offset_), KP(buf_cur_), KP(buf_end_));
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::get_next_file_id_(file_id_t& next_file_id)
{
  int ret = common::OB_SUCCESS;
  ObReadParam param;
  param.file_id_ = file_id_;
  param.timeout_ = timeout_;
  int64_t start_pos = -1;
  file_id_t trailer_next_fid = common::OB_INVALID_FILE_ID;
  if (OB_FAIL(reader_->read_trailer(param, trailer_rbuf_, start_pos, trailer_next_fid, read_cost_))) {
    if (common::OB_READ_NOTHING == ret || common::OB_INVALID_DATA == ret) {
      // If file trailer has never been written, read trailer may return OB_READ_NOTHING or
      // OB_INVALID_DATA
      //
      // If read a file has been reused, read trailed may return OB_INVALID_DATA
      ret = common::OB_READ_NOTHING;
    } else {
      CLOG_LOG(WARN, "read_trailer failed", K(ret), K(file_id_));
    }
  } else if (trailer_next_fid != file_id_ + 1) {
    ret = common::OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "read_trailer wrong data", K(ret), K(trailer_next_fid), K(file_id_));
  } else {
    next_file_id = file_id_ + 1;
  }
  return ret;
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::switch_file_()
{
  int ret = common::OB_SUCCESS;
  file_id_t next_file_id = common::OB_INVALID_FILE_ID;
  if (file_id_ >= end_file_id_) {
    ret = common::OB_ITER_END;
  } else if (OB_FAIL(get_next_file_id_(next_file_id))) {
    if (common::OB_READ_NOTHING == ret) {
      ret = common::OB_ITER_END;
    } else {
      CLOG_LOG(WARN, "get_next_file_id_ failed", K(ret), K(file_id_));
    }
  } else {
    file_id_ = next_file_id;
    cur_offset_ = 0;
    cur_block_start_offset_ = 0;
    cur_block_end_offset_ = 0;
    buf_cur_ = NULL;
    buf_end_ = NULL;
  }
  return ret;
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::handle_block_(const ObLogBlockMetaV2& meta)
{
  int ret = common::OB_EAGAIN;
  ObBlockType type = meta.get_block_type();
  // This is to deal with compatibility. there is a scenario where the timestamp
  // recorded in 1.4 version of the meta block is not guaranteed to be incremented.
  // The code calling sequence is as follows:
  // 329     // 1. prepare block meta
  // 330     if (OB_SUCC(ret)) {
  // 331       ObLogBlockMetaV2::MetaContent meta;
  // 332       int64_t meta_pos = 0;
  // 333       if (OB_FAIL(OB_I(g) meta.generate_block(task->get_buf(), task->get_data_len(), OB_DATA_BLOCK))) {
  // 334         CLOG_LOG(ERROR, "generate meta fail", K(ret), K(*task));
  // 335       } else if (OB_FAIL(OB_I(s) meta.serialize(task->get_buf() - block_meta_len_, block_meta_len_,
  // 336                                                 meta_pos))) {
  // 337         CLOG_LOG(WARN, "meta serialize", K(ret), "task.buf", OB_P(task->get_buf()), K(meta), K(meta_pos));
  // 338       }
  // 339     }
  // 340     // 2. write data
  // 341     if (OB_SUCC(ret)) {
  // 342       const int64_t write_len = block_meta_len_ + task->get_data_len(); // meta + data
  // 343       const bool with_eof = true;
  // 344       if (need_switch_file(write_len) && OB_FAIL(switch_file_without_lock_())) {
  // 345         CLOG_LOG(WARN, "switch file fail", K(ret), "file_id", ATOMIC_LOAD(&file_id_), K(offset_));
  //
  // Firstly, generate a block header1, and then if should switch file, it will write a InfoBlock,
  // and the block header1 will write into next file. so, the timestamp between two consecutive files
  // is out of ordered.
  //
  // Fixed the problem by ingored InfoBlock header when need record last_block_ts
  if (OB_INFO_BLOCK != type) {
    last_block_ts_ = meta.get_timestamp();
  }
  cur_block_end_offset_ =
      cur_block_start_offset_ + static_cast<offset_t>(meta.get_total_len() - meta.get_padding_len());
  CLOG_LOG(DEBUG,
      "handle_block_",
      K(cur_block_start_offset_),
      K(cur_block_end_offset_),
      "total_len",
      meta.get_total_len(),
      "padding_len",
      meta.get_padding_len());
  switch (type) {
    case OB_DATA_BLOCK:
      advance_(meta.get_serialize_size());
      break;
    case OB_HEADER_BLOCK:
      // file header is CLOG_DIO_ALIGN_SIZE bytes,
      // skip it and read again
      advance_(meta.get_total_len());
      break;
    case OB_INFO_BLOCK:
      if (OB_FAIL(switch_file_()) && common::OB_ITER_END != ret) {
        CLOG_LOG(WARN, "iterator switch file error", K(ret), K(file_id_), K(end_file_id_), K(cur_offset_));
      } else if (OB_SUCC(ret)) {
        ret = common::OB_EAGAIN;
      } else {
        // OB_ITER_END == ret, do nothing
      }
      break;
    case OB_TRAILER_BLOCK:
      // iter will never encounter a trailer for InfoBlock lies ahead
      ret = common::OB_INVALID_DATA;
      CLOG_LOG(
          ERROR, "iterator encounter a trailer block", K(ret), K(meta), K(file_id_), K(end_file_id_), K(cur_offset_));
      break;
    default:
      ret = common::OB_INVALID_DATA;
      CLOG_LOG(ERROR, "unknown block meta type", K(ret), K(meta), K(file_id_), K(end_file_id_), K(cur_offset_));
      break;
  }
  return ret;
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::prepare_buffer_(bool force_read)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRawEntryIterator is not inited", K(ret));
  } else if (force_read || NULL == buf_cur_ || NULL == buf_end_ || (buf_end_ - buf_cur_) < MAGIC_NUM_LEN) {
    // read from file if no data prepared before or
    // no enough data to distinguish between these magic numbers
    ObReadParam param;
    param.file_id_ = file_id_;
    param.offset_ = cur_offset_;
    param.read_len_ = common::OB_MAX_LOG_BUFFER_SIZE;
    param.timeout_ = timeout_;
    ObReadRes read_res;
    if (OB_FAIL(reader_->read_data_direct(param, rbuf_, read_res, read_cost_))) {
      if (common::OB_READ_NOTHING == ret) {
        CLOG_LOG(TRACE, "read nothing to iter end", K(cur_offset_), KP(buf_cur_), KP(buf_end_));
      } else {
        CLOG_LOG(WARN, "read data from file error", K(ret), K(param), K(rbuf_), K(read_res));
      }
    } else if (OB_FAIL(check_read_result_(param, read_res)) && common::OB_READ_NOTHING != ret) {
      CLOG_LOG(WARN, "check read result fail", K(ret), K(param), K(read_res));
    } else {
      // update buffer pointers
      buf_cur_ = const_cast<char*>(read_res.buf_);
      buf_end_ = buf_cur_ + read_res.data_len_;
    }
  } else {
    // no need prepare new buffer
    // do nothing
  }
  return ret;
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::check_read_result_(const ObReadParam& param, const ObReadRes& res) const
{
  int ret = common::OB_SUCCESS;
  if (OB_ISNULL(res.buf_) || OB_UNLIKELY(res.data_len_ <= 0)) {
    ret = common::OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "read result is invalid", K(ret), K(param), K(res));
  }
  return ret;
}

// if return ILOG_ENTRY, that means ilog_entry prepared well
template <class Type, class Interface>
ObCLogItemType ObRawEntryIterator<Type, Interface>::get_type_() const
{
  int ret = common::OB_SUCCESS;
  ObCLogItemType type = UNKNOWN_TYPE;

  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(INFO, "ObRawEntryIterator is not inited", K(ret));
  } else if (OB_ISNULL(buf_cur_)) {
    ret = common::OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "error unexpected", KP_(buf_cur));
  } else if (OB_FAIL(clog::parse_log_item_type(buf_cur_, sizeof(int16_t), type))) {
    CLOG_LOG(ERROR, "get_log_item_type fail", K(ret), KP_(buf_cur), K(type));
  }
  return type;
}

template <class Type, class Interface>
bool ObRawEntryIterator<Type, Interface>::is_compressed_item_(const ObCLogItemType item_type) const
{
  return (CLOG_ENTRY_COMPRESSED_ZSTD == item_type || CLOG_ENTRY_COMPRESSED_LZ4 == item_type ||
          CLOG_ENTRY_COMPRESSED_ZSTD_138 == item_type);
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::check_compressed_entry_length_(const char* buf, const int64_t buf_size) const
{
  int ret = common::OB_SUCCESS;
  ObCompressedLogEntry comp_entry;
  int64_t consume_buf_len = 0;
  if (OB_FAIL(comp_entry.deserialize(buf, buf_size, consume_buf_len))) {
    CLOG_LOG(WARN, "failed to deserialize ObCompressedLogEntry", K(ret));
  } else if (OB_UNLIKELY(consume_buf_len > buf_size)) {
    ret = common::OB_DESERIALIZE_ERROR;
    CLOG_LOG(WARN, "buf not enough", K(ret), K(consume_buf_len), K(buf_size));
  }
  return ret;
}

// return OB_EAGAIN: to prepare buffer and do get_next_entry_ again
template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::get_next_entry_(
    Type& entry, ObReadParam& param, bool& force_read, int64_t& persist_len)
{
  int ret = common::OB_SUCCESS;
  force_read = false;
  const ObCLogItemType item_type = get_type_();

  if (cur_offset_ < cur_block_end_offset_) {
    const int64_t buf_size = buf_end_ - buf_cur_;
    if (ILOG_ENTRY == item_type || CLOG_ENTRY == item_type || is_compressed_item_(item_type)) {
      CLOG_LOG(DEBUG, "ilog/clog entry magic", K(cur_offset_), K(item_type));
      int64_t pos = 0;
      if (is_compressed_item_(item_type)) {
        int64_t local_pos = 0;
        int64_t uncompress_len = 0;
        if (OB_FAIL(check_compressed_entry_length_(buf_cur_, buf_size))) {
          CLOG_LOG(WARN, "failed to check compressed entry length", K(ret), K(param), K(cur_offset_), K(buf_size));
        } else if (OB_FAIL(uncompress(
                       buf_cur_, buf_size, compress_rbuf_.buf_, compress_rbuf_.buf_len_, uncompress_len, pos))) {
          CLOG_LOG(WARN, "failed to uncompress", K(ret), K(param), K(buf_size));
        } else if (OB_FAIL(entry.deserialize(compress_rbuf_.buf_, uncompress_len, local_pos))) {
          if (common::OB_DESERIALIZE_ERROR == ret) {
            ret = common::OB_INVALID_DATA;
            CLOG_LOG(ERROR,
                "log entry deserialize error, maybe corrupted",
                K(ret),
                K(item_type),
                KP(buf_cur_),
                KP(buf_end_),
                K(pos),
                K(local_pos),
                K(cur_offset_),
                K(file_id_));
          }
        } else { /*do nothing*/
        }
      } else {
        ret = entry.deserialize(buf_cur_, buf_size, pos);
        if (OB_SUCC(ret) && OB_UNLIKELY(pos > buf_size)) {
          ret = common::OB_DESERIALIZE_ERROR;
          CLOG_LOG(WARN, "buf not enough", K(ret), K(entry), K(buf_size), K(pos));
        }
      }
      if (OB_FAIL(ret)) {
        if (common::OB_DESERIALIZE_ERROR == ret) {
          // buf not enough
          force_read = true;
          ret = common::OB_EAGAIN;
        } else if (common::OB_NOT_SUPPORTED == ret) {
          ret = common::OB_INVALID_DATA;
          CLOG_LOG(ERROR,
              "entry deserialize error, maybe corrupted",
              K(ret),
              K(item_type),
              KP(buf_cur_),
              KP(buf_end_),
              K(pos),
              K(cur_offset_),
              K(file_id_));
        } else {
          CLOG_LOG(ERROR,
              "entry deserialize error, maybe corrupted",
              K(ret),
              K(item_type),
              KP(buf_cur_),
              KP(buf_end_),
              K(pos),
              K(cur_offset_),
              K(file_id_));
        }
      } else if (!entry.check_integrity()) {
        // fatal error, data corrupt
        ret = common::OB_INVALID_DATA;
        CLOG_LOG(ERROR,
            "iterate next entry error",
            K(ret),
            K(cur_offset_),
            K(pos),
            KP(buf_cur_),
            KP(buf_end_),
            K(entry),
            K(cur_block_start_offset_),
            K(cur_block_end_offset_));
      } else {
        // iterate entry success
        param.file_id_ = file_id_;
        param.offset_ = cur_offset_;
        advance_(pos);
        persist_len = pos;
        ret = common::OB_SUCCESS;
      }
    } else {
      ret = common::OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "invalid item_type", K(ret), K(item_type), K(file_id_), K(end_file_id_), K(cur_offset_));
    }
    if (cur_offset_ > cur_block_end_offset_) {
      ret = common::OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR,
          "invalid offset",
          K(ret),
          K(item_type),
          K(file_id_),
          K(end_file_id_),
          K(cur_offset_),
          K(cur_block_end_offset_));
    }
  } else if (cur_offset_ == cur_block_end_offset_) {
    if (EOF_BUF == item_type || NEW_LOG_FILE_TYPE == item_type) {
      CLOG_LOG(DEBUG, "eof magic or new log file", K(cur_offset_));
      param.file_id_ = file_id_;
      param.offset_ = cur_offset_;
      ret = common::OB_ITER_END;
      CLOG_LOG(INFO, "reach eof", K(ret), K(file_id_), K(end_file_id_), K(cur_offset_));
    } else if (BLOCK_META == item_type) {
      CLOG_LOG(DEBUG, "block magic", K(cur_offset_));
      cur_block_start_offset_ = cur_offset_;
      ObLogBlockMetaV2 meta;
      int64_t pos = 0;
      if (buf_end_ - buf_cur_ < meta.get_serialize_size()) {
        // buf not enough
        force_read = true;
        ret = common::OB_EAGAIN;
      } else if (OB_FAIL(meta.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
        CLOG_LOG(ERROR, "meta deserialize error", K(ret), K(meta), K_(buf_cur), K_(buf_end), K(pos));
      } else if (!meta.check_meta_checksum()) {
        // fatal error, data corrupt
        ret = common::OB_INVALID_DATA;
        CLOG_LOG(ERROR, "check block meta checksum error", K(ret), K(meta));
      } else if (common::OB_INVALID_TIMESTAMP != last_block_ts_ &&
                 last_block_ts_ - CHECK_LAST_BLOCK_TS_INTERVAL > meta.get_timestamp()) {
        param.file_id_ = file_id_;
        param.offset_ = cur_offset_;
        ret = common::OB_ITER_END;
        CLOG_LOG(INFO, "reach iter end", K(ret), K(last_block_ts_), "meta_timestamp", meta.get_timestamp());
      } else if (OB_FAIL(handle_block_(meta))) {
        if (common::OB_EAGAIN != ret && common::OB_ITER_END != ret) {
          CLOG_LOG(WARN,
              "handle block error",
              K(ret),
              K(meta),
              K(file_id_),
              K(end_file_id_),
              K(cur_offset_),
              KP(buf_cur_),
              KP(buf_end_));
        }
        if (common::OB_ITER_END == ret) {
          param.file_id_ = file_id_;
          param.offset_ = cur_offset_;
        }
      }
    } else if (PADDING_ENTRY == item_type) {
      CLOG_LOG(DEBUG, "padding entry magic", K(cur_offset_));
      // data written by new writer
      ObPaddingEntry pe;
      int64_t pos = 0;
      if (OB_FAIL(pe.deserialize(buf_cur_, buf_end_ - buf_cur_, pos))) {
        if (common::OB_DESERIALIZE_ERROR == ret) {
          ret = common::OB_EAGAIN;
          force_read = true;
        } else {
          CLOG_LOG(ERROR, "padding entry deserialize error");
        }
      } else {
        advance_(pe.get_entry_size());
        cur_block_start_offset_ = cur_offset_;
        cur_block_end_offset_ = cur_offset_;
        ret = common::OB_EAGAIN;
      }
    } else if (check_last_block_(file_id_, cur_offset_, last_block_ts_)) {
      param.file_id_ = file_id_;
      param.offset_ = cur_offset_;
      ret = common::OB_ITER_END;
    } else {
      ret = common::OB_ERR_UNEXPECTED;
      CLOG_LOG(
          ERROR, "check last block failed", K(ret), K(file_id_), K(end_file_id_), K(cur_offset_), K(last_block_ts_));
    }
  } else {
    ret = common::OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "invalid offset",
        K(ret),
        K(item_type),
        K(file_id_),
        K(end_file_id_),
        K(cur_offset_),
        K(cur_block_end_offset_));
  }
  return ret;
}

template <class Type, class Interface>
int ObRawEntryIterator<Type, Interface>::next_entry(Type& entry, ObReadParam& param, int64_t& persist_len)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRawEntryIterator is not inited", K(ret));
  } else {
    bool force_read = false;
    do {
      if (OB_FAIL(prepare_buffer_(force_read))) {
        if (common::OB_READ_NOTHING == ret) {
          param.file_id_ = file_id_;
          param.offset_ = cur_offset_;
          ret = common::OB_ITER_END;
        } else {
          CLOG_LOG(WARN, "prepare buffer error", K(ret), K(force_read), K(file_id_), K(end_file_id_), K(cur_offset_));
        }
      } else {
        ret = get_next_entry_(entry, param, force_read, persist_len);
      }
    } while (common::OB_EAGAIN == ret);

    if (common::OB_INVALID_DATA == ret) {
      CLOG_LOG(ERROR, "invalid data, not unbroken write", K(ret), K(entry), K(param));
      if (check_last_block_(file_id_, cur_block_start_offset_, last_block_ts_)) {
        param.file_id_ = file_id_;
        param.offset_ = cur_block_start_offset_;
        ret = common::OB_ITER_END;
      }
    }
  }
  CLOG_LOG(DEBUG, "raw iter next_entry", K(ret), K(entry), K(param));
  return ret;
}

// last_block_ts must be vaild, because of this:
// 1. Write file header is atomic, therefore, the last_block_ts is valid
// 2. else, file header is ObNewLogFileBuf
template <class Type, class Interface>
bool ObRawEntryIterator<Type, Interface>::check_last_block_(
    const file_id_t file_id, const offset_t start_offset, const int64_t last_block_ts) const
{
  int ret = common::OB_SUCCESS;
  bool bool_ret = false;

  ObReadParam param;
  ObReadRes res;
  int16_t magic = 0;
  int64_t m_pos = 0;
  int64_t pos = 0;

  ObReadBufGuard guard(common::ObModIds::OB_LOG_DIRECT_READER_ITER_ID);
  ObReadBuf& rbuf = guard.get_read_buf();
  if (OB_UNLIKELY(!rbuf.is_valid())) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(ERROR, "alloc_buf failed", K(file_id), K(start_offset), K(ret));
  } else {
    ObLogBlockMetaV2 meta;
    for (offset_t offset = start_offset + static_cast<offset_t>(meta.get_serialize_size());
         OB_SUCC(ret) && offset < CLOG_FILE_SIZE;
         offset += static_cast<offset_t>(common::OB_MAX_LOG_BUFFER_SIZE - 2 * CLOG_DIO_ALIGN_SIZE)) {
      param.file_id_ = file_id;
      param.offset_ = offset;
      param.read_len_ = min(OB_MAX_LOG_BUFFER_SIZE, CLOG_FILE_SIZE - offset);
      ObReadCost dummy_cost;
      if (OB_FAIL(reader_->read_data_direct(param, rbuf, res, dummy_cost))) {
        CLOG_LOG(ERROR, "read_data failed", K(param), K(ret));
      } else {
        for (int64_t index = 0; OB_SUCC(ret) && index < res.data_len_ - CLOG_DIO_ALIGN_SIZE; index++) {
          meta.reset();
          pos = 0;
          magic = 0;
          m_pos = 0;
          if (OB_FAIL(common::serialization::decode_i16(res.buf_, sizeof(int16_t), m_pos, &magic))) {
            CLOG_LOG(ERROR, "decode magic failed", K(ret), K(res), K(m_pos), K(magic));
          } else if (!ObLogBlockMetaV2::check_magic_number(magic)) {
            // otherwise skip
            continue;
          } else if (OB_FAIL(meta.deserialize(res.buf_, res.data_len_, pos))) {
            CLOG_LOG(ERROR, "meta deserialize failed", K(param), K(ret));
          } else if (!meta.check_meta_checksum()) {
            continue;
          } else if (meta.get_timestamp() > last_block_ts) {
            ret = common::OB_ERR_UNEXPECTED;
            CLOG_LOG(
                ERROR, "check last block failed", K(ret), K(last_block_ts), "meta timestamp", meta.get_timestamp());
          } else {
            // do nothing
          }
        }
      }
    }
  }

  if (common::OB_SUCCESS == ret) {
    bool_ret = true;
  }

  CLOG_LOG(INFO, "check_last_block", K(file_id), K(start_offset), K(last_block_ts), K(ret), K(bool_ret));
  return bool_ret;
}

typedef ObRawEntryIterator<ObLogEntry, ObIRawLogIterator> ObRawLogIterator;
typedef ObRawEntryIterator<ObIndexEntry, ObIRawIndexIterator> ObRawIndexIterator;
}  // namespace clog
}  // end namespace oceanbase

#endif  // OCEANBASE_CLOG_OB_RAW_ENTRY_ITERATOR_H_

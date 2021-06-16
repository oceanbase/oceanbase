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

#include "lib/lock/ob_mutex.h"
#include "lib/oblog/ob_base_log_buffer.h"
#include "share/redolog/ob_log_file_store.h"
#include "ob_clog_file_writer.h"
#include "ob_info_block_handler.h"
#include "ob_log_block.h"
#include "ob_log_cache.h"
#include "ob_log_define.h"
#include "ob_log_entry.h"
#include "ob_log_file_trailer.h"
#include "ob_log_timer_utility.h"
#include "observer/ob_server_struct.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace clog {
ObCLogBaseFileWriter::ObCLogBaseFileWriter()
    : is_inited_(false),
      log_ctrl_(NULL),
      shm_buf_(NULL),
      shm_data_buf_(NULL),
      buf_write_pos_(0),
      file_offset_(0),
      buf_padding_size_(0),
      align_size_(0),
      store_(NULL),
      file_id_(0)
{
  log_dir_[0] = '\0';
}

ObCLogBaseFileWriter::~ObCLogBaseFileWriter()
{
  destroy();
}

int ObCLogBaseFileWriter::init(
    const char* log_dir, const char* shm_path, const uint32_t align_size, const ObILogFileStore* file_store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "already inited", K(ret));
  } else if (OB_ISNULL(log_dir) || OB_ISNULL(shm_path) || OB_ISNULL(file_store)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(log_dir), K(align_size), KP(file_store));
  } else if (OB_FAIL(ObBaseLogBufferMgr::get_instance().get_buffer(shm_path, log_ctrl_))) {
    CLOG_LOG(WARN, "get log buf failed", K(ret), K(log_dir));
  } else {
    shm_buf_ = log_ctrl_->base_buf_;
    shm_data_buf_ = log_ctrl_->data_buf_;
    log_dir_[sizeof(log_dir_) - 1] = '\0';
    (void)snprintf(log_dir_, sizeof(log_dir_) - 1, log_dir);
    align_size_ = align_size;
    store_ = const_cast<ObILogFileStore*>(file_store);
  }
  return OB_SUCCESS;
}

void ObCLogBaseFileWriter::destroy()
{
  is_inited_ = false;
  log_ctrl_ = NULL;
  shm_buf_ = NULL;
  shm_data_buf_ = NULL;
  buf_write_pos_ = 0;
  file_offset_ = 0;
  buf_padding_size_ = 0;
  align_size_ = 0;
  file_id_ = 0;
  log_dir_[0] = '\0';
  store_ = nullptr;
}

bool ObCLogBaseFileWriter::enough_file_space(const uint64_t write_len) const
{
  bool enough = false;
  if (IS_NOT_INIT) {
    CLOG_LOG(WARN, "not inited", K(write_len));
  } else {
    uint64_t new_file_offset = file_offset_ + write_len;
    if (need_align()) {
      new_file_offset += ObPaddingEntry::get_padding_size(new_file_offset, align_size_);
    }
    enough = new_file_offset < CLOG_MAX_DATA_OFFSET;
  }
  return enough;
}

int ObCLogLocalFileWriter::load_file(uint32_t& file_id, uint32_t& offset, bool enable_pre_creation)
{
  UNUSED(enable_pre_creation);
  int ret = OB_SUCCESS;
  ObAtomicFilePos file_pos;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else if (0 == file_id) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(file_id), K(ret));
  } else if (0 != file_id_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file is not end", K(ret), K_(file_id));
  } else if (OB_FAIL(store_->open(file_id))) {
    CLOG_LOG(WARN, "open file failed", K(file_id), K(ret));
  } else {
    file_id_ = file_id;
    file_pos.file_id_ = file_id;
    file_pos.file_offset_ = offset;
    CLOG_LOG(INFO, "load start, ", K(file_pos), K(shm_buf_->file_write_pos_), K(shm_buf_->file_flush_pos_));

    if (0 == shm_buf_->file_flush_pos_.atomic_ || 0 == shm_buf_->file_write_pos_.atomic_) {
      // first start or start after server restart
      ATOMIC_STORE(&shm_buf_->file_flush_pos_.atomic_, file_pos.atomic_);
      ATOMIC_STORE(&shm_buf_->file_write_pos_.atomic_, file_pos.atomic_);
    } else if (shm_buf_->file_write_pos_.file_id_ + 1 == file_pos.file_id_ ||
               shm_buf_->file_flush_pos_.file_id_ + 1 == file_pos.file_id_) {
      // observer restart just after creating new_file()
      if (0 != file_pos.file_offset_ || shm_buf_->file_write_pos_ < shm_buf_->file_flush_pos_) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR,
            "The clog new file start pos is unexpected, ",
            K(ret),
            K(file_pos),
            K(shm_buf_->file_flush_pos_),
            K(shm_buf_->file_write_pos_),
            K(shm_buf_->log_dir_));
      } else {
        ATOMIC_STORE(&shm_buf_->file_write_pos_.atomic_, file_pos.atomic_);
        ATOMIC_STORE(&shm_buf_->file_flush_pos_.atomic_, file_pos.atomic_);
        CLOG_LOG(INFO, "Success to sync new file pos", K(file_pos));
      }
    } else {
      // start after observer process restart and there is data in share memory
      if (shm_buf_->file_flush_pos_.file_id_ != file_pos.file_id_ ||
          shm_buf_->file_write_pos_.file_id_ != file_pos.file_id_ || shm_buf_->file_flush_pos_ > file_pos ||
          shm_buf_->file_write_pos_ < shm_buf_->file_flush_pos_) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR,
            "The clog start pos is unexpected, ",
            K(ret),
            K(file_id),
            K(offset),
            K(shm_buf_->file_flush_pos_),
            K(shm_buf_->file_write_pos_),
            K(shm_buf_->log_dir_));
      } else if (shm_buf_->file_write_pos_ > shm_buf_->file_flush_pos_) {
        // the write buffer is not flushed, need flush
        // if need alignment, also include previous unaligned part + padding part
        buf_write_pos_ = shm_buf_->file_write_pos_.file_offset_ - shm_buf_->file_flush_pos_.file_offset_;
        if (need_align()) {
          buf_write_pos_ += (shm_buf_->file_flush_pos_.file_offset_ % align_size_);
          buf_write_pos_ += ObPaddingEntry::get_padding_size(buf_write_pos_);
        }
        file_offset_ = shm_buf_->file_flush_pos_.file_offset_;

        CLOG_LOG(INFO,
            "Flush remaining buf, ",
            K(ret),
            K(file_id),
            K(offset),
            K(shm_buf_->file_flush_pos_),
            K(shm_buf_->file_write_pos_),
            K(shm_buf_->log_dir_),
            K_(file_offset),
            K_(buf_write_pos));

        if (buf_write_pos_ > shm_buf_->buf_len_) {
          ret = OB_ERR_UNEXPECTED;
          CLOG_LOG(ERROR,
              "The buf pos is unexpected, ",
              K(ret),
              K_(buf_write_pos),
              K(shm_buf_->buf_len_),
              K(shm_buf_->file_flush_pos_),
              K(shm_buf_->file_write_pos_),
              K(shm_buf_->log_dir_));
        } else if (OB_FAIL(flush_buf())) {
          CLOG_LOG(ERROR,
              "Fail to flush share memory buffer to log file, ",
              K(ret),
              K(errno),
              K_(buf_write_pos),
              K(shm_buf_->log_dir_));
        } else {
          ATOMIC_STORE(&shm_buf_->file_flush_pos_.atomic_, shm_buf_->file_write_pos_.atomic_);
          CLOG_LOG(INFO, "Success to flush log buf to file!");
        }
      } else {
        // equal write and flush pos
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (shm_buf_->file_flush_pos_ < file_pos) {
      ATOMIC_STORE(&shm_buf_->file_flush_pos_.atomic_, file_pos.atomic_);
      ATOMIC_STORE(&shm_buf_->file_write_pos_.atomic_, file_pos.atomic_);
    }
  }

  // Load last time unaligned part if it is aligned system
  // Append only system is not needed, so just reset buf empty
  if (OB_SUCC(ret)) {
    if (need_align()) {
      buf_write_pos_ = shm_buf_->file_flush_pos_.file_offset_ % align_size_;
      int64_t read_size = 0;
      if (buf_write_pos_ > 0 && OB_FAIL(store_->read(shm_data_buf_,
                                    align_size_,
                                    lower_align(shm_buf_->file_flush_pos_.file_offset_, align_size_),
                                    read_size))) {
        CLOG_LOG(ERROR,
            "Fail to read data from log file, ",
            K(ret),
            K_(buf_write_pos),
            K(shm_buf_->file_flush_pos_.file_offset_),
            K(shm_buf_->log_dir_));
      } else if (read_size != align_size_) {
        CLOG_LOG(INFO, "Log file size is not aligned. ", K(read_size), K_(align_size), K(shm_buf_->file_flush_pos_));
      } else {
        CLOG_LOG(
            INFO, "Read data from log file to shared memory buffer, ", K(buf_write_pos_), K(shm_buf_->file_flush_pos_));
      }
    } else {
      reset_buf();
    }
  }

  if (OB_FAIL(ret)) {
    CLOG_LOG(WARN, "log writer start failed", K(ret), K(file_id), K(offset));
  } else {
    file_offset_ = shm_buf_->file_flush_pos_.file_offset_;
    CLOG_LOG(INFO, "load success", K(file_id), K(offset));
  }
  return ret;
}

int ObCLogBaseFileWriter::append_trailer_entry(const uint32_t info_block_offset)
{
  int ret = OB_SUCCESS;
  ObLogFileTrailer trailer;
  int64_t pos = 0;
  const file_id_t phy_file_id = file_id_ + 1;
  // build trailer from last 512 byte offset (4096-512)
  int64_t trailer_pos = CLOG_DIO_ALIGN_SIZE - CLOG_TRAILER_SIZE;
  char* buf = shm_data_buf_ + trailer_pos;
  reset_buf();

  if (CLOG_TRAILER_OFFSET != file_offset_) {  // Defense code
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file_offset_ mismatch trailer offset", K(ret), K_(file_offset), LITERAL_K(CLOG_TRAILER_OFFSET));
  } else if (OB_FAIL(trailer.build_serialized_trailer(buf, CLOG_TRAILER_SIZE, info_block_offset, phy_file_id, pos))) {
    CLOG_LOG(WARN,
        "build_serialized_trailer fail",
        K(ret),
        LITERAL_K(CLOG_DIO_ALIGN_SIZE),
        K(info_block_offset),
        K_(file_id),
        K(phy_file_id));
  } else {
    buf_write_pos_ += (uint32_t)CLOG_DIO_ALIGN_SIZE;
  }

  return ret;
}

int ObCLogBaseFileWriter::flush_trailer_entry()
{
  int ret = OB_SUCCESS;
  if (CLOG_TRAILER_OFFSET != file_offset_) {  // Defense code
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file offset mismatch", K_(file_offset), LITERAL_K(CLOG_TRAILER_OFFSET));
  } else if (CLOG_DIO_ALIGN_SIZE != buf_write_pos_) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "buf write position mismatch", K_(buf_write_pos), LITERAL_K(CLOG_DIO_ALIGN_SIZE));
  } else if (OB_FAIL(store_->write(shm_data_buf_, buf_write_pos_, CLOG_TRAILER_ALIGN_WRITE_OFFSET))) {
    CLOG_LOG(ERROR,
        "write fail",
        K(ret),
        K(buf_write_pos_),
        K_(file_offset),
        LITERAL_K(CLOG_TRAILER_ALIGN_WRITE_OFFSET),
        K(errno));
  }
  return ret;
}

int ObCLogBaseFileWriter::append_info_block_entry(ObIInfoBlockHandler* info_getter)
{
  int ret = OB_SUCCESS;
  ObLogBlockMetaV2 meta;
  uint32_t block_meta_len = (uint32_t)meta.get_serialize_size();
  int64_t buf_len = shm_buf_->buf_len_ - block_meta_len;
  int64_t data_len = 0;
  int64_t pos = 0;
  char* buf = shm_data_buf_ + block_meta_len;

  reset_buf();

  // write info block to buf first to get data length, and then write block meta
  if (OB_FAIL(info_getter->build_info_block(buf, buf_len, data_len))) {
    // build_info_block will reset flying info_block for next file
    CLOG_LOG(WARN, "read partition meta fail", K(ret), KP(buf), K(buf_len), K_(file_offset), K_(buf_padding_size));

  } else if (OB_FAIL(meta.build_serialized_block(shm_data_buf_, block_meta_len, buf, data_len, OB_INFO_BLOCK, pos))) {
    CLOG_LOG(WARN, "build serialized block fail", K(ret), K_(file_offset), K_(buf_padding_size));
  } else {
    buf_write_pos_ += (block_meta_len + (uint32_t)data_len);
  }

  if (OB_SUCC(ret) && OB_FAIL(align_buf())) {
    CLOG_LOG(WARN, "fail to pad info block", K(ret));
  }
  return ret;
}

int ObCLogLocalFileWriter::create_next_file()
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else if (!is_valid_file_id(file_id_)) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file not start", K_(file_id), K(ret));
  } else if (0 != buf_write_pos_) {  // defend code
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file not correct end before", K_(buf_write_pos));
  } else if (OB_FAIL(store_->open(file_id_ + 1))) {
    CLOG_LOG(WARN, "open file fail", K(ret), K_(file_id));
  } else {
    ++file_id_;
    file_offset_ = 0;
    ObAtomicFilePos file_pos;
    file_pos.file_id_ = file_id_;
    file_pos.file_offset_ = file_offset_;

    ATOMIC_STORE(&shm_buf_->file_write_pos_.atomic_, file_pos.atomic_);
    ATOMIC_STORE(&shm_buf_->file_flush_pos_.atomic_, file_pos.atomic_);

    // fild_id is 32 bits. The first clog file_id is 1 and each clog file size is 64MB.
    // Suppose the maximum log write throughput is 1GB/s.
    // (By experience, the speed usually is smaller than that, 300MB/s is the maximum value
    // we've seen from production. In such case, we would recommend to open compression)
    // Suppose the hardware life period is smaller than 8.5 years.
    // So the 32 bits long file_id is enough, no need to used 64 bits.
    // For safety, add monitoring here. Raise warning 180 days before the file_id exhausted.
    if (file_id_ >= UINT32_MAX - 16 * 180 * 24 * 3600 && REACH_TIME_INTERVAL(60 * 1000 * 1000)) {
      CLOG_LOG(ERROR, "file_id is too large, please attention!", K(file_id_));
    }
  }
  return ret;
}

int ObCLogBaseFileWriter::append_padding_entry(const uint32_t padding_size)
{
  int ret = OB_SUCCESS;
  if (padding_size > 0) {
    int64_t serialize_pos = 0;
    ObPaddingEntry padding_entry;
    char* buf = shm_data_buf_ + buf_write_pos_;

    if (buf_write_pos_ + padding_size > shm_buf_->buf_len_) {
      ret = OB_BUF_NOT_ENOUGH;
      CLOG_LOG(WARN,
          "padding entry size over buf length",
          K(ret),
          K(padding_size),
          K_(buf_write_pos),
          K(shm_buf_->buf_len_));
    } else if (OB_FAIL(padding_entry.set_entry_size(padding_size))) {
      CLOG_LOG(WARN, "padding entry set size error", K(ret), K(padding_size));
    } else if (OB_FAIL(padding_entry.serialize(buf, padding_size, serialize_pos))) {
      CLOG_LOG(WARN, "padding entry serialize error", K(ret), K(padding_entry), K(serialize_pos));
    } else {
      memset(buf + serialize_pos, 0, padding_size - serialize_pos);
      buf_write_pos_ += padding_size;
    }
  }
  return ret;
}

int ObCLogBaseFileWriter::cache_buf(ObLogCache* log_cache, const char* buf, const uint32_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || 0 == buf_len) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid args", K(ret), KP(buf), K(buf_len));
  } else {
    const common::ObAddr addr = GCTX.self_addr_;
    if (OB_FAIL(log_cache->append_data(addr, buf, file_id_, file_offset_, buf_len))) {
      CLOG_LOG(WARN, "fail to cache buf, ", K(ret), K_(file_id), K_(file_offset), K(buf_len));
    } else {
      file_offset_ += buf_len;
    }
  }
  return ret;
}

int ObCLogBaseFileWriter::append_log_entry(const char* item_buf, const uint32_t len)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else if (NULL == item_buf || 0 == len) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(item_buf), K(len));
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file not start", K_(file_id), K(ret));
  } else {
    // copy log to share memory buffer
    lib::ObMutexGuard buf_guard(log_ctrl_->buf_mutex_);
    memcpy(shm_data_buf_ + buf_write_pos_, item_buf, len);
    buf_write_pos_ += (uint32_t)len;

    if (OB_FAIL(align_buf())) {
      CLOG_LOG(ERROR, "fail to add padding, ", K(ret));
    } else {
      ObAtomicFilePos file_pos;
      file_pos.file_id_ = file_id_;
      file_pos.file_offset_ = file_offset_ + len;
      ATOMIC_STORE(&shm_buf_->file_write_pos_.atomic_, file_pos.atomic_);
    }
  }

  return ret;
}

int ObCLogLocalFileWriter::flush(
    ObIInfoBlockHandler* info_getter, ObLogCache* log_cache, ObTailCursor* tail, int64_t& flush_start_offset)
{
  UNUSEDx(info_getter, log_cache, tail);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else if (file_id_ != shm_buf_->file_flush_pos_.file_id_ ||
             file_offset_ != shm_buf_->file_flush_pos_.file_offset_) {  // Defense code
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR,
        "file position not match",
        K(ret),
        K_(file_id),
        K_(file_offset),
        K(shm_buf_->file_flush_pos_),
        K(shm_buf_->file_write_pos_));
  }

  if (OB_SUCC(ret)) {
    flush_start_offset = file_offset_;
    if (OB_FAIL(flush_buf())) {
      CLOG_LOG(WARN, "Fail to flush clog to disk, ", K(ret));
    } else {
      lib::ObMutexGuard buf_guard(log_ctrl_->buf_mutex_);
      ATOMIC_STORE(&shm_buf_->file_flush_pos_.atomic_, shm_buf_->file_write_pos_.atomic_);
      file_offset_ = shm_buf_->file_write_pos_.file_offset_;
      truncate_buf();
    }
  }

  return ret;
}

int ObCLogLocalFileWriter::align_buf()
{
  int ret = OB_SUCCESS;
  if (0 == (buf_write_pos_ % align_size_)) {  // already aligned
    buf_padding_size_ = 0;
    // do nothing
  } else {
    buf_padding_size_ = ObPaddingEntry::get_padding_size(buf_write_pos_, align_size_);
    if (OB_FAIL(append_padding_entry(buf_padding_size_))) {
      CLOG_LOG(WARN, "inner add padding entry error", K(ret), K_(buf_padding_size));
    }
  }
  return ret;
}

///            ObCLogLocalFileWriter             ///
int ObCLogLocalFileWriter::init(
    const char* log_dir, const char* shm_path, const uint32_t align_size, const ObILogFileStore* file_store)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "already inited", K(ret));
  } else if (OB_FAIL(ObCLogBaseFileWriter::init(log_dir, shm_path, align_size, file_store))) {
    CLOG_LOG(WARN, "ObCLogBaseFileWriter init fail", K(ret));
  } else if (NULL == (blank_buf_ = (char*)ob_malloc(OB_MAX_LOG_BUFFER_SIZE, ObModIds::OB_LOG_WRITER))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc blank buf failed", K(ret), K_(blank_buf));
  } else {
    memset(blank_buf_, 'B', OB_MAX_LOG_BUFFER_SIZE);
    is_inited_ = true;
  }

  if (IS_NOT_INIT) {
    destroy();
  }
  return OB_SUCCESS;
}

void ObCLogLocalFileWriter::destroy()
{
  if (NULL != blank_buf_) {
    ob_free(blank_buf_);
    blank_buf_ = NULL;
  }
  ObCLogBaseFileWriter::destroy();
}

void ObCLogLocalFileWriter::reset()
{
  buf_write_pos_ = 0;
  file_offset_ = 0;
  buf_padding_size_ = 0;
  if (0 != file_id_) {
    store_->close();
  }
  file_id_ = 0;
}

uint32_t ObCLogBaseFileWriter::get_min_using_file_id() const
{
  int ret = OB_SUCCESS;
  uint32_t min_file_using_id = OB_INVALID_FILE_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    min_file_using_id = store_->get_min_using_file_id();
  }

  return min_file_using_id;
}

uint32_t ObCLogBaseFileWriter::get_min_file_id() const
{
  int ret = OB_SUCCESS;
  uint32_t min_file_id = OB_INVALID_FILE_ID;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    min_file_id = store_->get_min_file_id();
  }

  return min_file_id;
}

int64_t ObCLogBaseFileWriter::get_free_quota() const
{
  int ret = OB_SUCCESS;
  int64_t free_quota = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    free_quota = store_->get_free_quota();
  }

  return free_quota;
}

void ObCLogBaseFileWriter::update_min_using_file_id(const uint32_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    store_->update_min_using_file_id(file_id);
  }
}

void ObCLogBaseFileWriter::update_min_file_id(const uint32_t file_id)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    store_->update_min_file_id(file_id);
  }
}

void ObCLogBaseFileWriter::try_recycle_file()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    store_->try_recycle_file();
  }
}

int ObCLogBaseFileWriter::update_free_quota()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else if (OB_FAIL(store_->update_free_quota())) {
    CLOG_LOG(WARN, "file store update quota fail.", K(ret));
  }
  return ret;
}

bool ObCLogBaseFileWriter::free_quota_warn() const
{
  int ret = OB_SUCCESS;
  bool b_warn = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else {
    b_warn = store_->free_quota_warn();
  }

  return b_warn;
}

int ObCLogLocalFileWriter::end_current_file(ObIInfoBlockHandler* info_getter, ObLogCache* log_cache, ObTailCursor* tail)
{
  int ret = OB_SUCCESS;
  uint32_t info_block_offset = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "not inited", K(ret));
  } else if (OB_ISNULL(info_getter) || OB_ISNULL(log_cache) || OB_ISNULL(tail)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", KP(info_getter), KP(log_cache), KP(tail));
  } else if (OB_UNLIKELY(!is_valid_file_id(file_id_))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file not start", K_(file_id), K(ret));
  }

  // - Cache previous padding entry to log cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cache_last_padding_entry(log_cache))) {
      CLOG_LOG(WARN, "fail cache last padding entry", K(ret));
    } else {
      // remember info block start position here
      // trailer entry need to record it
      info_block_offset = file_offset_;
    }
  }

  // - Flush info block to log file (aligned if need)
  // - Cache info block to log cache
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_info_block_entry(info_getter))) {
      CLOG_LOG(WARN, "fail to add info block", K(ret), K(info_getter));
    } else if (OB_FAIL(flush_buf())) {
      CLOG_LOG(WARN, "fail to flush info block", K(ret));
    } else if (OB_FAIL(cache_buf(log_cache, shm_data_buf_, buf_write_pos_))) {
      CLOG_LOG(WARN, "fail to cache info block", K(ret));
    }
  }

  // - Fill blank space before trailer entry
  if (OB_SUCC(ret)) {
    if (OB_FAIL(cache_blank_space(log_cache))) {
      CLOG_LOG(WARN, "fail to fill in blank space", K(ret));
    }
  }

  // - Flush trailer entry to log file
  // - Cache trailer entry to log cache
  char* trailer_buf = shm_data_buf_ + CLOG_DIO_ALIGN_SIZE - CLOG_TRAILER_SIZE;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(append_trailer_entry(info_block_offset))) {
      CLOG_LOG(WARN, "fail to add trailer", K(ret));
    } else if (OB_FAIL(flush_trailer_entry())) {
      CLOG_LOG(WARN, "fail to flush trailer", K(ret));
    } else if (OB_FAIL(cache_buf(log_cache, trailer_buf, CLOG_TRAILER_SIZE))) {
      CLOG_LOG(WARN, "fail to cache trailer", K(ret), KP(trailer_buf), LITERAL_K(CLOG_TRAILER_SIZE));
    } else if (CLOG_FILE_SIZE != file_offset_) {  // Defense code
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "file_offset_ mismatch file size", K(ret), K_(file_offset));
    } else {
      tail->advance(file_id_ + 1, 0);
      reset_buf();
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(store_->close())) {
      CLOG_LOG(WARN, "close file failed", K(ret), K_(file_id));
    }
  }
  return ret;
}

int ObCLogLocalFileWriter::cache_last_padding_entry(ObLogCache* log_cache)
{
  int ret = OB_SUCCESS;
  uint32_t padding_size = 0;
  if (0 == (file_offset_ % align_size_) && 0 != buf_padding_size_) {  // Defense code
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "file_offset_ mismatch", K(ret), K_(file_offset), K_(buf_padding_size));
  } else if (0 == (file_offset_ % align_size_)) {  // already aligned
    padding_size = 0;
    // do nothing
  } else {
    // reset buf so that last padding entry is written from buf start
    reset_buf();
    padding_size = ObPaddingEntry::get_padding_size(file_offset_, align_size_);
    if (OB_FAIL(append_padding_entry(padding_size))) {
      CLOG_LOG(WARN, "inner add padding entry error", K(ret), K(padding_size));
    } else if (OB_FAIL(cache_buf(log_cache, shm_data_buf_, buf_write_pos_))) {
      CLOG_LOG(WARN, "fail to cache last padding", K(ret));
    }
  }

  // Post check
  if (OB_SUCC(ret)) {
    if (need_align() && 0 != (file_offset_ % align_size_)) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN, "file offset not align", K(ret), K_(file_offset), K_(align_size));
    } else if (file_offset_ < shm_buf_->file_flush_pos_.file_offset_ ||
               file_id_ != shm_buf_->file_flush_pos_.file_id_ ||
               shm_buf_->file_flush_pos_ != shm_buf_->file_write_pos_) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(WARN,
          "file position mismatch",
          K(ret),
          K_(file_offset),
          K(shm_buf_->file_flush_pos_),
          K(shm_buf_->file_write_pos_));
    }
  }

  return ret;
}

int ObCLogLocalFileWriter::cache_blank_space(ObLogCache* log_cache)
{
  int ret = OB_SUCCESS;
  if (OB_LIKELY(file_offset_ < CLOG_TRAILER_OFFSET)) {
    const int64_t total_blank_len = CLOG_TRAILER_OFFSET - file_offset_;
    int64_t step_len = 0;
    int64_t finished_len = 0;
    const common::ObAddr addr = GCTX.self_addr_;
    while (finished_len < total_blank_len && OB_SUCC(ret)) {
      step_len = std::min(total_blank_len - finished_len, OB_MAX_LOG_BUFFER_SIZE);
      if (OB_FAIL(
              log_cache->append_data(addr, blank_buf_, file_id_, (offset_t)(file_offset_ + finished_len), step_len))) {
        CLOG_LOG(ERROR, "fail to cache blank", K(ret), K_(file_offset), K(finished_len), K(step_len));
      } else {
        finished_len += step_len;
        CLOG_LOG(INFO, "fill_blank_space success", K_(file_offset), K(finished_len), K(step_len));
      }
    }

    if (OB_SUCC(ret)) {
      file_offset_ = CLOG_TRAILER_OFFSET;
    }
  } else if (file_offset_ > CLOG_TRAILER_OFFSET) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "info block data occupy trailer", K_(file_offset));
  } else {
    CLOG_LOG(INFO, "empty blank cache", K_(file_offset));
  }
  return ret;
}

void ObCLogLocalFileWriter::truncate_buf()
{
  if (!need_align()) {
    reset_buf();
  } else {
    uint32_t tail_part_start = 0;
    // move the tail unaligned part to head
    tail_part_start = (uint32_t)lower_align(buf_write_pos_ - buf_padding_size_, align_size_);
    buf_write_pos_ = (buf_write_pos_ - buf_padding_size_) % align_size_;
    if (buf_write_pos_ > 0) {
      memmove(shm_data_buf_, shm_data_buf_ + tail_part_start, buf_write_pos_);
    }
  }
}

int ObCLogLocalFileWriter::flush_buf()
{
  int ret = OB_SUCCESS;

  if (need_align() && 0 != (buf_write_pos_ % align_size_)) {  // Defense code
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "buf write position not aligned", K(ret), K_(buf_write_pos), K_(align_size));
  } else {
    ObLogTimerUtility timer;
    timer.start_timer();
    uint32_t file_write_pos = file_offset_;
    if (need_align()) {
      // buf has been pad and include last flush time remaining unaligned part
      file_write_pos = (uint32_t)lower_align(file_offset_, align_size_);
    }
    if (OB_FAIL(store_->write(shm_data_buf_, buf_write_pos_, file_write_pos))) {
      CLOG_LOG(ERROR, "write fail", K(ret), K(buf_write_pos_), K(file_write_pos), K(errno));
    }
    timer.finish_timer(__FILE__, __LINE__, CLOG_PERF_WARN_THRESHOLD);
  }
  return ret;
}
}  // namespace clog
}  // namespace oceanbase

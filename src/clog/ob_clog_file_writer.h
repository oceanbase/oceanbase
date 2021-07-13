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

#ifndef OB_CLOG_FILE_WRITER_H_
#define OB_CLOG_FILE_WRITER_H_

#include <stdint.h>
#include "lib/utility/ob_macro_utils.h"
#include "lib/oblog/ob_base_log_buffer.h"
#include "clog/ob_log_define.h"

namespace oceanbase {
namespace common {
struct ObBaseLogBuffer;
class ObILogFileStore;
}  // namespace common
namespace clog {
class ObIInfoBlockHandler;
class ObLogCache;
class ObTailCursor;

// This class is responsible for writing log entry to file. ObCLogLocalFileWriter
// and ObCLogOFSFileWriter will support different behavior between local disk and OFS.
// Local disk IO should be aligned and can be in-place-update.
// OFS AIO can be unaligned but should be append-only.
class ObCLogBaseFileWriter {
public:
  ObCLogBaseFileWriter();
  virtual ~ObCLogBaseFileWriter();

  virtual int init(
      const char* log_dir, const char* shm_path, const uint32_t align_size, const common::ObILogFileStore* file_store);
  virtual void destroy();

  // When log engine start, need to flush remaining content in shared memory buffer to log file
  // Meanwhile, if file system need alignment, to reuse shared buffer, need read unaligned
  // content back to shared buffer.
  // For OFS, it has additional logic to switch to new file, so file_id and offset can be changed
  virtual int load_file(uint32_t& file_id, uint32_t& offset, bool enable_pre_creation = false) = 0;
  // This function will format log file ending part according to different file system
  // Local disk format:
  // |--previous data entry--|--padding entry--|--info block--|--blank--|--tail entry--|
  // OFS write info block to the new file head
  virtual int end_current_file(ObIInfoBlockHandler* info_getter, ObLogCache* log_cache, ObTailCursor* tail) = 0;
  virtual int create_next_file() = 0;
  // flush all thing in buffer to file, OFS may switch to new file if meet failure
  virtual int flush(
      ObIInfoBlockHandler* info_getter, ObLogCache* log_cache, ObTailCursor* tail, int64_t& flush_start_offset) = 0;

  virtual bool is_write_hang() const
  {
    return false;
  }

  uint32_t get_min_using_file_id() const;
  uint32_t get_min_file_id() const;
  int64_t get_free_quota() const;
  void update_min_using_file_id(const uint32_t file_id);
  void update_min_file_id(const uint32_t file_id);
  void try_recycle_file();
  int update_free_quota();
  bool free_quota_warn() const;

  OB_INLINE uint32_t get_cur_file_len() const
  {
    return file_offset_;
  }
  OB_INLINE uint32_t get_cur_file_id() const
  {
    return file_id_;
  }
  OB_INLINE const char* get_dir_name() const
  {
    return log_dir_;
  }
  bool enough_file_space(const uint64_t write_len) const;
  // append log item meta and data to buffer
  int append_log_entry(const char* item_buf, const uint32_t len);

protected:
  // align memory buffer, append padding_entry if need
  virtual int align_buf() = 0;

protected:
  int append_padding_entry(const uint32_t padding_size);
  int append_info_block_entry(ObIInfoBlockHandler* info_getter);
  int append_trailer_entry(const uint32_t info_block_offset);
  int flush_trailer_entry();
  // append all data in buffer to log cache
  int cache_buf(ObLogCache* log_cache, const char* buf, const uint32_t buf_len);

  OB_INLINE bool need_align() const
  {
    return 0 != align_size_;
  }
  OB_INLINE void reset_buf()
  {
    buf_write_pos_ = 0;
  }

protected:
  bool is_inited_;
  common::ObBaseLogBufferCtrl* log_ctrl_;
  common::ObBaseLogBuffer* shm_buf_;
  char* shm_data_buf_;
  uint32_t buf_write_pos_;
  uint32_t file_offset_;
  // the last aligned part padding size of the buffer
  uint32_t buf_padding_size_;
  uint32_t align_size_;
  common::ObILogFileStore* store_;
  uint32_t file_id_;
  char log_dir_[common::MAX_PATH_SIZE];

private:
  DISALLOW_COPY_AND_ASSIGN(ObCLogBaseFileWriter);
};

class ObCLogLocalFileWriter : public ObCLogBaseFileWriter {
public:
  ObCLogLocalFileWriter() : blank_buf_(NULL)
  {}
  virtual ~ObCLogLocalFileWriter()
  {
    destroy();
  }

  virtual int init(const char* log_dir, const char* shm_path, const uint32_t align_size,
      const common::ObILogFileStore* file_store) override;
  virtual void destroy();

  virtual int load_file(uint32_t& file_id, uint32_t& offset, bool enable_pre_creation = false) override;
  // Local disk format:
  // |--previous data entry--|--padding entry--|--info block--|--blank--|--tail entry--|
  // The whole process is:
  // - Cache previous padding entry to log cache
  // - Flush info block to log file
  // - Cache info block to log cache
  // - Cache blank buffer to log cache
  // - Flush trailer entry to log file
  // - Cache trailer entry to log cache
  virtual int end_current_file(ObIInfoBlockHandler* info_getter, ObLogCache* log_cache, ObTailCursor* tail) override;
  virtual int create_next_file() override;
  virtual int flush(ObIInfoBlockHandler* info_getter, ObLogCache* log_cache, ObTailCursor* tail,
      int64_t& flush_start_offset) override;

  void reset();

protected:
  virtual int align_buf() override;

private:
  // last padding entry need sync to log cache
  int cache_last_padding_entry(ObLogCache* log_cache);
  // cache blank space between info block and trailer entry
  int cache_blank_space(ObLogCache* log_cache);
  int flush_buf();
  // truncate buf after flush
  void truncate_buf();

private:
  char* blank_buf_;

  DISALLOW_COPY_AND_ASSIGN(ObCLogLocalFileWriter);
};
}  // namespace clog
}  // namespace oceanbase

#endif /* OB_CLOG_FILE_WRITER_H_ */

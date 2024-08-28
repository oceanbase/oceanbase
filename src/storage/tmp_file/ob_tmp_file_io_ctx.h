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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_IO_CTX_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_IO_CTX_H_

#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/tmp_file/ob_tmp_file_cache.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOCtx
{
public:
  ObTmpFileIOCtx();
  ~ObTmpFileIOCtx();

  int init(const int64_t fd, const int64_t dir_id,
           const bool is_read,
           const common::ObIOFlag io_flag,
           const int64_t io_timeout_ms,
           const bool disable_page_cache,
           const bool disable_block_cache);
  void reuse();
  void reset();
  bool is_valid() const;
  int prepare_read(char *read_buf, const int64_t read_size);
  int prepare_read(char *read_buf, const int64_t read_size, const int64_t read_offset);
  int prepare_write(char *write_buf, const int64_t write_size);
  int update_data_size(const int64_t size);
  int wait();

public:
  OB_INLINE bool check_buf_range_valid(const char* buffer, const int64_t length) const
  {
    return buffer != nullptr && buffer >= buf_ && buffer + length <= buf_ + buf_size_;
  }
  OB_INLINE int64_t get_fd() const { return fd_; }
  OB_INLINE int64_t get_dir_id() const { return dir_id_; }
  OB_INLINE bool is_read() const { return is_read_; }
  OB_INLINE char *get_buffer() { return buf_; }
  OB_INLINE char *get_buffer() const { return buf_; }
  OB_INLINE char *get_todo_buffer() { return buf_ + done_size_; }
  OB_INLINE char *get_todo_buffer() const { return buf_ + done_size_; }
  OB_INLINE int64_t get_done_size() const { return done_size_; }
  OB_INLINE int64_t get_todo_size() const { return todo_size_; }
  OB_INLINE int64_t get_read_offset_in_file() const { return read_offset_in_file_; }
  OB_INLINE void set_read_offset_in_file(const int64_t offset) { read_offset_in_file_ = offset; }
  OB_INLINE bool is_disable_page_cache() const { return disable_page_cache_; }
  OB_INLINE bool is_disable_block_cache() const { return disable_block_cache_; }
  OB_INLINE common::ObIOFlag get_io_flag() const { return io_flag_; }
  OB_INLINE int64_t get_io_timeout_ms() const { return io_timeout_ms_; }
  OB_INLINE void set_is_unaligned_read(const bool is_unaligned_read) { is_unaligned_read_ = is_unaligned_read; }
  OB_INLINE bool is_unaligned_read() { return is_unaligned_read_; }

  TO_STRING_KV(K(is_inited_), K(is_read_),
               K(fd_), K(dir_id_), KP(buf_),
               K(buf_size_), K(done_size_), K(todo_size_),
               K(read_offset_in_file_),
               K(disable_page_cache_),
               K(disable_block_cache_),
               K(io_flag_), K(io_timeout_ms_));

public:
  struct ObIReadHandle
  {
    ObIReadHandle();
    ObIReadHandle(char *dest_user_read_buf,
                  const int64_t offset_in_src_data_buf,
                  const int64_t read_size);
    ~ObIReadHandle();
    ObIReadHandle(const ObIReadHandle &other);
    ObIReadHandle &operator=(const ObIReadHandle &other);
    virtual bool is_valid() = 0;
    TO_STRING_KV(KP(dest_user_read_buf_), K(offset_in_src_data_buf_), K(read_size_));

    char *dest_user_read_buf_; // user buf
    int64_t offset_in_src_data_buf_;
    int64_t read_size_;
  };

  struct ObIOReadHandle final : public ObIReadHandle
  {
    ObIOReadHandle();
    ObIOReadHandle(char *dest_user_read_buf,
                   const int64_t offset_in_src_data_buf, const int64_t read_size,
                   ObTmpFileBlockHandle block_handle);
    ~ObIOReadHandle();
    ObIOReadHandle(const ObIOReadHandle &other);
    ObIOReadHandle &operator=(const ObIOReadHandle &other);
    virtual bool is_valid() override;
    INHERIT_TO_STRING_KV("ObIReadHandle", ObIReadHandle, K(handle_), K(block_handle_));
    blocksstable::ObMacroBlockHandle handle_;
    ObTmpFileBlockHandle block_handle_;
  };

  struct ObBlockCacheHandle final : public ObIReadHandle
  {
    ObBlockCacheHandle();
    ObBlockCacheHandle(const ObTmpBlockValueHandle &block_handle_, char *dest_user_read_buf,
                       const int64_t offset_in_src_data_buf,
                       const int64_t read_size);
    ~ObBlockCacheHandle();
    ObBlockCacheHandle(const ObBlockCacheHandle &other);
    ObBlockCacheHandle &operator=(const ObBlockCacheHandle &other);
    virtual bool is_valid() override;
    INHERIT_TO_STRING_KV("ObIReadHandle", ObIReadHandle, K(block_handle_));
    ObTmpBlockValueHandle block_handle_;
  };

  struct ObPageCacheHandle final : public ObIReadHandle
  {
    ObPageCacheHandle();
    ObPageCacheHandle(const ObTmpPageValueHandle &page_handle, char *dest_user_read_buf,
                      const int64_t offset_in_src_data_buf,
                      const int64_t read_size);
    ~ObPageCacheHandle();
    ObPageCacheHandle(const ObPageCacheHandle &other);
    ObPageCacheHandle &operator=(const ObPageCacheHandle &other);
    virtual bool is_valid() override;
    INHERIT_TO_STRING_KV("ObIReadHandle", ObIReadHandle, K(page_handle_));
    ObTmpPageValueHandle page_handle_;
  };

  common::ObIArray<ObIOReadHandle> &get_io_handles()
  {
    return io_handles_;
  }
  common::ObIArray<ObPageCacheHandle> &get_page_cache_handles()
  {
    return page_cache_handles_;
  }
  common::ObIArray<ObBlockCacheHandle> &get_block_cache_handles()
  {
    return block_cache_handles_;
  }

private:
  int wait_read_finish_(const int64_t timeout_ms);
  int do_read_wait_();

private:
  bool is_inited_;
  bool is_read_;
  int64_t fd_;
  int64_t dir_id_;
  char *buf_;
  int64_t buf_size_;
  int64_t done_size_;
  int64_t todo_size_;
  int64_t read_offset_in_file_;
  bool disable_page_cache_;
  bool disable_block_cache_; // only used in ut, to control whether read data from block cache
  bool is_unaligned_read_; //for statistics
  common::ObIOFlag io_flag_;
  int64_t io_timeout_ms_;
  common::ObSEArray<ObIOReadHandle, 1> io_handles_;
  common::ObSEArray<ObPageCacheHandle, 1> page_cache_handles_;
  common::ObSEArray<ObBlockCacheHandle, 1> block_cache_handles_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOCtx);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_IO_CTX_H_

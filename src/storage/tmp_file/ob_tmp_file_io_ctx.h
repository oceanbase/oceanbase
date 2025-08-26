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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_CTX_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_CTX_H_

#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/tmp_file/ob_tmp_file_cache.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/blocksstable/ob_storage_object_handle.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOBaseCtx
{
public:
  ObTmpFileIOBaseCtx();
  virtual ~ObTmpFileIOBaseCtx();

  int init(const int64_t fd,
           const common::ObIOFlag io_flag,
           const int64_t io_timeout_ms);
  virtual void reuse();
  virtual void reset();
  virtual bool is_valid() const;
  virtual int update_data_size(const int64_t size);
  virtual int prepare(char *buf, const int64_t size);

public:
  OB_INLINE bool check_buf_range_valid(const char* buffer, const int64_t length) const
  {
    return buffer != nullptr && buffer >= buf_ && buffer + length <= buf_ + buf_size_;
  }
  OB_INLINE int64_t get_fd() const { return fd_; }
  OB_INLINE char *get_buffer() { return buf_; }
  OB_INLINE char *get_buffer() const { return buf_; }
  OB_INLINE char *get_todo_buffer() { return buf_ + done_size_; }
  OB_INLINE char *get_todo_buffer() const { return buf_ + done_size_; }
  OB_INLINE int64_t get_done_size() const { return done_size_; }
  OB_INLINE int64_t get_todo_size() const { return todo_size_; }
  OB_INLINE common::ObIOFlag get_io_flag() const { return io_flag_; }
  OB_INLINE int64_t get_io_timeout_ms() const { return io_timeout_ms_; }
  TO_STRING_KV(K(is_inited_), K(fd_), KP(buf_),
               K(buf_size_), K(done_size_), K(todo_size_),
               K(io_flag_), K(io_timeout_ms_));
protected:
  bool is_inited_;
  int64_t fd_;
  char *buf_;
  int64_t buf_size_;
  int64_t done_size_;
  int64_t todo_size_;
  common::ObIOFlag io_flag_;
  int64_t io_timeout_ms_;
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOBaseCtx);
};

class ObTmpFileIOWriteCtx final: public ObTmpFileIOBaseCtx
{
public:
  ObTmpFileIOWriteCtx();
  ~ObTmpFileIOWriteCtx();

  virtual void reset() override;

public:
  INHERIT_TO_STRING_KV("ObTmpFileIOBaseCtx", ObTmpFileIOBaseCtx,
                       K(is_unaligned_write_),
                       K(write_persisted_tail_page_cnt_), K(lack_page_cnt_));
public:
  // for virtual table stat info
  OB_INLINE void set_is_unaligned_write(const bool is_unaligned_write) { is_unaligned_write_ = is_unaligned_write; }
  OB_INLINE bool is_unaligned_write() const { return is_unaligned_write_; }

  OB_INLINE void add_write_persisted_tail_page_cnt() { write_persisted_tail_page_cnt_++; }
  OB_INLINE int64_t get_write_persisted_tail_page_cnt() const { return write_persisted_tail_page_cnt_; }

  OB_INLINE void add_lack_page_cnt() { lack_page_cnt_++; }
  OB_INLINE int64_t get_lack_page_cnt() const { return lack_page_cnt_; }

private:
  /********for virtual table statistics begin********/
  bool is_unaligned_write_;
  int64_t write_persisted_tail_page_cnt_;
  int64_t lack_page_cnt_;
  /********for virtual table statistics end********/
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOWriteCtx);
};

class ObTmpFileIOReadCtx final : public ObTmpFileIOBaseCtx
{
public:
  ObTmpFileIOReadCtx();
  ~ObTmpFileIOReadCtx();

  int init(const int64_t fd,
           const common::ObIOFlag io_flag,
           const int64_t io_timeout_ms,
           const bool disable_page_cache,
           const bool prefetch);
  virtual void reuse() override;
  virtual void reset() override;
  virtual bool is_valid() const override;
  virtual int update_data_size(const int64_t size) override;
  virtual int prepare(char *buf, const int64_t size) override;
  int prepare(char *buf, const int64_t size, const int64_t read_offset);
  int wait();

public:
  OB_INLINE int64_t get_read_offset_in_file() const { return read_offset_in_file_; }
  OB_INLINE int64_t get_start_read_offset_in_file() const { return read_offset_in_file_; }
  OB_INLINE int64_t get_end_read_offset_in_file() const { return read_offset_in_file_ + todo_size_; }
  OB_INLINE void set_read_offset_in_file(const int64_t offset) { read_offset_in_file_ = offset; }
  OB_INLINE bool is_disable_page_cache() const { return disable_page_cache_; }
  OB_INLINE bool is_prefetch() const { return prefetch_; }
  INHERIT_TO_STRING_KV("ObTmpFileIOBaseCtx", ObTmpFileIOBaseCtx,
                       K(read_offset_in_file_),
                       K(disable_page_cache_),
                       K(prefetch_), K(is_unaligned_read_),
                       K(total_truncated_page_read_cnt_), K(total_kv_cache_page_read_cnt_),
                       K(total_uncached_page_read_cnt_), K(total_wbp_page_read_cnt_),
                       K(truncated_page_read_hits_), K(kv_cache_page_read_hits_),
                       K(uncached_page_read_hits_), K(aggregate_read_io_cnt_), K(wbp_page_read_hits_));
public:
  OB_INLINE void set_is_unaligned_read(const bool is_unaligned_read) { is_unaligned_read_ = is_unaligned_read; }
  OB_INLINE bool is_unaligned_read() const { return is_unaligned_read_; }

  OB_INLINE void update_read_truncated_stat(const int64_t page_num)
  {
    total_truncated_page_read_cnt_ += page_num;
    truncated_page_read_hits_++;
  }
  OB_INLINE int64_t get_total_truncated_page_read_cnt() const { return total_truncated_page_read_cnt_; }
  OB_INLINE int64_t get_truncated_page_read_hits() const { return truncated_page_read_hits_; }

  OB_INLINE void update_read_kv_cache_page_stat(const int64_t page_num, const int64_t read_cnt)
  {
    total_kv_cache_page_read_cnt_ += page_num;
    kv_cache_page_read_hits_ += read_cnt;
  }
  OB_INLINE int64_t get_total_kv_cache_page_read_cnt() const { return total_kv_cache_page_read_cnt_; }
  OB_INLINE int64_t get_kv_cache_page_read_hits() const { return kv_cache_page_read_hits_; }

  OB_INLINE void update_read_uncached_page_stat(const int64_t page_num,
                                                const int64_t uncached_read_cnt,
                                                const int64_t aggregate_read_cnt)
  {
    total_uncached_page_read_cnt_ += page_num;
    uncached_page_read_hits_ += uncached_read_cnt;
    aggregate_read_io_cnt_ += aggregate_read_cnt;
  }
  OB_INLINE int64_t get_aggregate_read_io_cnt() const { return aggregate_read_io_cnt_; }
  OB_INLINE int64_t get_total_uncached_page_read_cnt() const { return total_uncached_page_read_cnt_; }
  OB_INLINE int64_t get_uncached_page_read_hits() const { return uncached_page_read_hits_; }

  #ifdef OB_BUILD_SHARED_STORAGE
  // for ss mode
  OB_INLINE void update_read_wbp_page_stat(const int64_t page_num)
  {
    total_wbp_page_read_cnt_ += page_num;
    wbp_page_read_hits_++;
  }
  #endif
  // for sn mode
  OB_INLINE void update_read_wbp_page_stat(const int64_t hit_cache_cnt, const int64_t read_cache_cnt)
  {
    total_wbp_page_read_cnt_ += hit_cache_cnt;
    wbp_page_read_hits_ += read_cache_cnt;
  }
  OB_INLINE int64_t get_total_wbp_page_read_cnt() const { return total_wbp_page_read_cnt_; }
  OB_INLINE int64_t get_wbp_page_read_hits() const { return wbp_page_read_hits_; }

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
                   const int64_t offset_in_src_data_buf, const int64_t read_size);
    ObIOReadHandle(char *dest_user_read_buf,
                   const int64_t offset_in_src_data_buf, const int64_t read_size,
                   ObTmpFileBlockHandle block_handle);
    ~ObIOReadHandle();
    ObIOReadHandle(const ObIOReadHandle &other);
    ObIOReadHandle &operator=(const ObIOReadHandle &other);
    virtual bool is_valid() override;
    INHERIT_TO_STRING_KV("ObIReadHandle", ObIReadHandle, K(handle_), K(block_handle_));
    blocksstable::ObStorageObjectHandle handle_;
    ObTmpFileBlockHandle block_handle_;
  };

  // TODO: wanyue.wy
  // remove ObPageCacheHandle, directly memcpy when hit page kv cache
  struct ObPageCacheHandle final : public ObIReadHandle
  {
    ObPageCacheHandle();
    ObPageCacheHandle(char* dest_user_read_buf, const int64_t offset_in_src_data_buf, const int64_t read_size);
    ~ObPageCacheHandle();
    int init(const ObTmpPageValueHandle &page_handle) {
      return page_handle_.assign(page_handle);
    }
    int assign(const ObPageCacheHandle &other)
    {
      int ret = OB_SUCCESS;
      if (OB_FAIL(this->page_handle_.assign(other.page_handle_))) {
        COMMON_LOG(WARN, "fail to assign page_handle", K(ret));
      } else {
        ObIReadHandle::operator=(other);
      }
      return ret;
    }
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

private:
  int64_t read_offset_in_file_;   // this var means the start offset for read.
                                  // if this var is negative, it will be set by
                                  // read_offset of file in file's read function
  bool disable_page_cache_;
  bool prefetch_;
  common::ObSEArray<ObIOReadHandle, 1> io_handles_;
  common::ObSEArray<ObPageCacheHandle, 1> page_cache_handles_;
  /********for virtual table statistics begin********/
  bool is_unaligned_read_;
  int64_t total_truncated_page_read_cnt_;
  int64_t total_kv_cache_page_read_cnt_;
  int64_t total_uncached_page_read_cnt_;
  int64_t total_wbp_page_read_cnt_;
  int64_t truncated_page_read_hits_;
  int64_t kv_cache_page_read_hits_;
  int64_t uncached_page_read_hits_;
  int64_t aggregate_read_io_cnt_;
  int64_t wbp_page_read_hits_;
  /********for virtual table statistics end********/
  DISALLOW_COPY_AND_ASSIGN(ObTmpFileIOReadCtx);
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_IO_CTX_H_

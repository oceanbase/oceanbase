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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_H_

#include "lib/lock/ob_tc_rwlock.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_index_cache.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOCtx;
class ObTmpWriteBufferPool;
class ObTmpFileFlushPriorityManager;

//for virtual table show
class ObTmpFileInfo
{
public:
  ObTmpFileInfo() :
    trace_id_(),
    tenant_id_(OB_INVALID_TENANT_ID),
    dir_id_(ObTmpFileGlobal::INVALID_TMP_FILE_DIR_ID),
    fd_(ObTmpFileGlobal::INVALID_TMP_FILE_FD),
    file_size_(0),
    truncated_offset_(0),
    is_deleting_(false),
    cached_data_page_num_(0),
    write_back_data_page_num_(0),
    flushed_data_page_num_(0),
    ref_cnt_(0),
    birth_ts_(-1),
    tmp_file_ptr_(nullptr),
    label_(),
    write_info_(),
    read_info_() {}
  virtual ~ObTmpFileInfo() { reset(); }
  virtual int init(const ObCurTraceId::TraceId &trace_id,
                   const uint64_t tenant_id,
                   const int64_t dir_id,
                   const int64_t fd,
                   const int64_t file_size,
                   const int64_t truncated_offset,
                   const bool is_deleting,
                   const int64_t cached_page_num,
                   const int64_t write_back_data_page_num,
                   const int64_t flushed_data_page_num,
                   const int64_t ref_cnt,
                   const int64_t birth_ts,
                   const void* const tmp_file_ptr,
                   const char* const label,
                   const int64_t write_req_cnt,
                   const int64_t unaligned_write_req_cnt,
                   const int64_t write_persisted_tail_page_cnt,
                   const int64_t lack_page_cnt,
                   const int64_t last_modify_ts,
                   const int64_t read_req_cnt,
                   const int64_t unaligned_read_req_cnt,
                   const int64_t total_truncated_page_read_cnt,
                   const int64_t total_kv_cache_page_read_cnt,
                   const int64_t total_uncached_page_read_cnt,
                   const int64_t total_wbp_page_read_cnt,
                   const int64_t truncated_page_read_hits,
                   const int64_t kv_cache_page_read_hits,
                   const int64_t uncached_page_read_hits,
                   const int64_t wbp_page_read_hits,
                   const int64_t total_read_size,
                   const int64_t last_access_ts);
  virtual void reset();
public:
  struct ObTmpFileWriteInfo
  {
  public:
    ObTmpFileWriteInfo() :
      write_req_cnt_(0),
      unaligned_write_req_cnt_(0),
      write_persisted_tail_page_cnt_(0),
      lack_page_cnt_(0),
      last_modify_ts_(-1) {}
    void reset();
  public:
    int64_t write_req_cnt_;
    int64_t unaligned_write_req_cnt_;
    int64_t write_persisted_tail_page_cnt_;
    int64_t lack_page_cnt_;
    int64_t last_modify_ts_;
    TO_STRING_KV(K_(write_req_cnt), K_(unaligned_write_req_cnt),
        K_(write_persisted_tail_page_cnt), K_(lack_page_cnt), K_(last_modify_ts));
  };
  struct ObTmpFileReadInfo
  {
  public:
    ObTmpFileReadInfo() :
      read_req_cnt_(0),
      unaligned_read_req_cnt_(0),
      total_truncated_page_read_cnt_(0),
      total_kv_cache_page_read_cnt_(0),
      total_uncached_page_read_cnt_(0),
      total_wbp_page_read_cnt_(0),
      truncated_page_read_hits_(0),
      kv_cache_page_read_hits_(0),
      uncached_page_read_hits_(0),
      wbp_page_read_hits_(0),
      total_read_size_(0),
      last_access_ts_(-1) {}
    void reset();
  public:
    int64_t read_req_cnt_;
    int64_t unaligned_read_req_cnt_;
    int64_t total_truncated_page_read_cnt_;          // the total read count of truncated pages
    int64_t total_kv_cache_page_read_cnt_;           // the total read count of pages in kv_cache
    int64_t total_uncached_page_read_cnt_;           // the total read count of pages with io
    int64_t total_wbp_page_read_cnt_;                // the total read count of pages in wbp
    int64_t truncated_page_read_hits_;               // the hit count of truncated pages when read
    int64_t kv_cache_page_read_hits_;                // the hit count of pages in kv_cache when read
    int64_t uncached_page_read_hits_;                // the hit count of persisted pages when read
    int64_t wbp_page_read_hits_;                     // the hit count of pages in wbp when read
    int64_t total_read_size_;
    int64_t last_access_ts_;
    TO_STRING_KV(K_(read_req_cnt), K_(unaligned_read_req_cnt), K_(total_truncated_page_read_cnt),
        K_(total_kv_cache_page_read_cnt), K_(total_uncached_page_read_cnt),
        K_(total_wbp_page_read_cnt), K_(truncated_page_read_hits), K_(kv_cache_page_read_hits),
        K_(uncached_page_read_hits), K_(wbp_page_read_hits), K_(total_read_size), K_(last_access_ts));
  };

public:
  // common info
  common::ObCurTraceId::TraceId trace_id_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
  int64_t file_size_;
  int64_t truncated_offset_;
  bool is_deleting_;
  int64_t cached_data_page_num_;
  int64_t write_back_data_page_num_;
  int64_t flushed_data_page_num_;
  int64_t ref_cnt_;
  int64_t birth_ts_;
  const void *tmp_file_ptr_;
  ObFixedLengthString<ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1> label_;
  ObTmpFileWriteInfo write_info_;
  ObTmpFileReadInfo read_info_;

  TO_STRING_KV(K(trace_id_), K(tenant_id_), K(dir_id_), K(fd_), K(file_size_),
               K(truncated_offset_), K(is_deleting_), K(cached_data_page_num_),
               K(write_back_data_page_num_), K(flushed_data_page_num_),
               K(ref_cnt_), K(birth_ts_), KP(tmp_file_ptr_), K(label_),
               K(write_info_), K(read_info_));
};

class ObITmpFile
{
public:
  ObITmpFile();
  virtual ~ObITmpFile();
  virtual int init(const int64_t tenant_id,
                   const int64_t dir_id,
                   const int64_t fd,
                   ObTmpWriteBufferPool *wbp,
                   ObTmpFileFlushPriorityManager *flush_prio_mgr,
                   ObIAllocator *callback_allocator,
                   ObIAllocator *wbp_index_cache_allocator,
                   ObIAllocator *wbp_index_cache_bkt_allocator,
                   const char* label);
  virtual void reset();
  virtual int release_resource() = 0;
  int delete_file();
  int seal();
  int aio_pread(ObTmpFileIOCtx &io_ctx);
  int aio_write(ObTmpFileIOCtx &io_ctx);
  // truncate offset is open interval
  virtual int truncate(const int64_t truncate_offset);

protected:
  int inner_read_truncated_part_(ObTmpFileIOCtx &io_ctx);
  virtual int inner_read_from_disk_(const int64_t expected_read_disk_size, ObTmpFileIOCtx &io_ctx) = 0;
  int inner_read_from_wbp_(ObTmpFileIOCtx &io_ctx);

  virtual int swap_page_to_disk_(const ObTmpFileIOCtx &io_ctx) = 0;

  int inner_write_(ObTmpFileIOCtx &io_ctx);
  int inner_fill_tail_page_(ObTmpFileIOCtx &io_ctx);
  virtual int load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx) = 0;
  virtual int append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx) = 0;
  int inner_write_continuous_pages_(ObTmpFileIOCtx &io_ctx);
  int alloc_and_write_pages_(const ObTmpFileIOCtx &io_ctx,
                             ObArray<uint32_t> &alloced_page_id,
                             int64_t &actual_write_size);

  int truncate_cached_pages_(const int64_t truncate_offset, const int64_t wbp_begin_offset);
  virtual int truncate_the_first_wbp_page_() = 0;
  virtual int truncate_persistent_pages_(const int64_t truncate_offset) = 0;

  virtual int inner_seal_() = 0;
  virtual int inner_delete_file_() = 0;
public:
  struct ObTmpFileNode : public ObDLinkBase<ObTmpFileNode>
  {
    ObTmpFileNode(ObITmpFile &file) : file_(file) {}
    ObITmpFile &file_;
  };
  enum class ObTmpFileMode
  {
    INVALID = -1,
    SHARED_NOTHING = 0,
    SHARED_STORAGE,
    MAX
  };

public:
  bool can_remove();
  bool is_deleting();
  bool is_sealed();
  bool is_flushing();

  int64_t get_file_size();
  int64_t cal_wbp_begin_offset();
  void update_read_offset(int64_t read_offset);
  int64_t get_dirty_data_page_size_with_lock();

  OB_INLINE ObTmpFileMode get_mode() const { return mode_; }
  OB_INLINE int64_t get_fd() const { return fd_; }
  OB_INLINE int64_t get_dir_id() const { return dir_id_; }
  OB_INLINE void inc_ref_cnt() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE void dec_ref_cnt() { ATOMIC_AAF(&ref_cnt_, -1); }
  OB_INLINE int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE void set_data_page_flush_level(int64_t data_page_flush_level)
  {
    data_page_flush_level_ = data_page_flush_level;
  }
  OB_INLINE int64_t get_data_page_flush_level() const { return data_page_flush_level_; }
  OB_INLINE ObTmpFileNode *get_data_flush_node() { return &data_flush_node_; }
  int reinsert_data_flush_node();
  int remove_data_flush_node();

  VIRTUAL_TO_STRING_KV(K(is_inited_), K(mode_), K(tenant_id_), K(dir_id_), K(fd_),
                       K(is_deleting_), K(is_sealed_),
                       K(ref_cnt_), K(truncated_offset_), K(read_offset_),
                       K(file_size_),
                       K(cached_page_nums_), K(write_back_data_page_num_),
                       K(begin_page_id_), K(begin_page_virtual_id_),
                       K(end_page_id_),
                       K(data_page_flush_level_),
                       KP(data_flush_node_.get_next()),
                       KP(wbp_), KP(flush_prio_mgr_), KP(&page_idx_cache_), KP(callback_allocator_),
                       K(trace_id_), K(birth_ts_), K(label_),
                       K(write_req_cnt_), K(unaligned_write_req_cnt_),
                       K(write_persisted_tail_page_cnt_), K(lack_page_cnt_), K(last_modify_ts_),
                       K(read_req_cnt_), K(unaligned_read_req_cnt_),
                       K(total_truncated_page_read_cnt_), K(total_uncached_page_read_cnt_),
                       K(total_kv_cache_page_read_cnt_), K(total_wbp_page_read_cnt_),
                       K(truncated_page_read_hits_), K(uncached_page_read_hits_),
                       K(kv_cache_page_read_hits_), K(wbp_page_read_hits_),
                       K(total_read_size_), K(last_access_ts_));

public:
  // for virtual table
  void set_read_stats_vars(const ObTmpFileIOCtx &ctx, const int64_t read_size);
  void set_write_stats_vars(const ObTmpFileIOCtx &ctx);
  virtual int copy_info_for_virtual_table(ObTmpFileInfo &tmp_file_info) = 0;

protected:
  // for virtual table
  virtual void inner_set_read_stats_vars_(const ObTmpFileIOCtx &ctx, const int64_t read_size);
  virtual void inner_set_write_stats_vars_(const ObTmpFileIOCtx &ctx);

protected:
  int64_t cal_wbp_begin_offset_() const;
  virtual bool is_flushing_() = 0;
  int reinsert_data_flush_node_();
  int insert_or_update_data_flush_node_();
  virtual int64_t get_dirty_data_page_size_() const = 0;

  OB_INLINE bool has_unfinished_page_() const { return file_size_ % ObTmpFileGlobal::PAGE_SIZE != 0; }
  OB_INLINE int64_t get_page_begin_offset_(const int64_t offset) const
  {
    return common::lower_align(offset, ObTmpFileGlobal::PAGE_SIZE);
  }
  OB_INLINE int64_t get_page_end_offset_(const int64_t offset) const
  {
    return common::upper_align(offset, ObTmpFileGlobal::PAGE_SIZE);
  }
  OB_INLINE int64_t get_page_begin_offset_by_virtual_id_(const int64_t virtual_page_id) const
  {
    return virtual_page_id * ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_page_end_offset_by_virtual_id_(const int64_t virtual_page_id) const
  {
    return (virtual_page_id + 1) * ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_page_virtual_id_(const int64_t offset, const bool is_open_interval) const
  {
    return is_open_interval ?
           common::upper_align(offset, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE - 1 :
           offset / ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_end_page_virtual_id_() const
  {
    return cached_page_nums_ == 0 ?
           ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID :
           get_page_virtual_id_(file_size_, true /*is_open_interval*/);
  }
  OB_INLINE int64_t get_offset_in_page_(const int64_t offset) const
  {
    return offset % ObTmpFileGlobal::PAGE_SIZE;
  }

  OB_INLINE int64_t get_offset_in_block_(int64_t file_offset) const
  {
    int64_t ret = 0;
    if(!GCTX.is_shared_storage_mode()) {
      ret = file_offset % ObTmpFileGlobal::SN_BLOCK_SIZE;
    #ifdef OB_BUILD_SHARED_STORAGE
    } else {
      ret = file_offset % ObTmpFileGlobal::SS_BLOCK_SIZE;
    #endif
    }
    return ret;
  }

protected:
  bool is_inited_;
  ObTmpFileMode mode_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
  bool is_deleting_;
  bool is_sealed_;
  int64_t ref_cnt_;
  int64_t truncated_offset_;      // read data befor truncated_offset will be set as 0
  int64_t read_offset_;           // read offset is on the entire file
  int64_t file_size_;             // has written size of this file
  int64_t cached_page_nums_;      // page nums in write buffer pool
  int64_t write_back_data_page_num_;
  uint32_t begin_page_id_;        // the first page index in write buffer pool
  int64_t begin_page_virtual_id_;
  uint32_t end_page_id_;          // the last page index in write buffer pool
  int64_t diag_log_print_cnt_;    // print diagnosis log every PRINT_LOG_FILE_SIZE
  int64_t data_page_flush_level_;
  ObTmpFileNode data_flush_node_;
  common::TCRWLock meta_lock_; // handle conflicts between writing and reading meta tree and meta data of file
  ObSpinLock multi_write_lock_; // handle conflicts between multiple writes
  ObSpinLock last_page_lock_; // handle conflicts between writing and evicting for last page
  ObTmpWriteBufferPool *wbp_;
  ObTmpFileFlushPriorityManager *flush_prio_mgr_;
  ObTmpFileWBPIndexCache page_idx_cache_;
  ObIAllocator *callback_allocator_;
  /********for virtual table begin********/
  // common info
  common::ObCurTraceId::TraceId trace_id_;
  int64_t birth_ts_;
  ObFixedLengthString<ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1> label_;
  // write info
  int64_t write_req_cnt_;
  int64_t unaligned_write_req_cnt_;
  int64_t write_persisted_tail_page_cnt_;
  int64_t lack_page_cnt_;
  int64_t last_modify_ts_;
  // read info
  int64_t read_req_cnt_;
  int64_t unaligned_read_req_cnt_;
  int64_t total_truncated_page_read_cnt_;
  int64_t total_kv_cache_page_read_cnt_;
  int64_t total_uncached_page_read_cnt_;
  int64_t total_wbp_page_read_cnt_;
  int64_t truncated_page_read_hits_;
  int64_t kv_cache_page_read_hits_;
  int64_t uncached_page_read_hits_;
  int64_t wbp_page_read_hits_;
  int64_t total_read_size_;
  int64_t last_access_ts_;
  /********for virtual table end********/
};

class ObITmpFileHandle
{
public:
  ObITmpFileHandle() : ptr_(nullptr) {}
  ObITmpFileHandle(ObITmpFile *tmp_file);
  ObITmpFileHandle(const ObITmpFileHandle &handle);
  ObITmpFileHandle & operator=(const ObITmpFileHandle &other);
  ~ObITmpFileHandle() { reset(); }
  OB_INLINE ObITmpFile * get() const {return ptr_; }
  bool is_inited() { return nullptr != ptr_; }
  void reset();
  int init(ObITmpFile *tmp_file);
  TO_STRING_KV(KP(ptr_));
protected:
  ObITmpFile *ptr_;
};

struct ObTmpFileKey final
{
  explicit ObTmpFileKey(const int64_t fd) : fd_(fd) {}
  OB_INLINE int hash(uint64_t &hash_val) const
  {
    hash_val = murmurhash(&fd_, sizeof(int64_t), 0);
    return OB_SUCCESS;
  }
  OB_INLINE bool operator==(const ObTmpFileKey &other) const { return fd_ == other.fd_; }
  TO_STRING_KV(K(fd_));
  int64_t fd_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_I_TMP_FILE_H_

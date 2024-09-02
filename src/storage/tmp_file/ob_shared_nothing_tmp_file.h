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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_SHARE_NOTHING_TMP_FILE_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_SHARE_NOTHING_TMP_FILE_H_

#include "storage/tmp_file/ob_tmp_file_cache.h"
#include "storage/tmp_file/ob_tmp_file_write_buffer_index_cache.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "storage/tmp_file/ob_tmp_file_meta_tree.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOCtx;
class ObTmpWriteBufferPool;
class ObTmpFileBlockPageBitmap;
class ObTmpFileBlockManager;
class ObTmpFileEvictionManager;
class ObTmpFileFlushPriorityManager;
class ObTmpFileFlushInfo;
class ObTmpFileFlushTask;
class ObTmpFilePageCacheController;
class ObTmpFileFlushManager;
class ObTmpFileTreeFlushContext;
class ObTmpFileDataFlushContext;

//for virtual table show
class ObSNTmpFileInfo
{
public:
  ObSNTmpFileInfo() :
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
    write_req_cnt_(0),
    unaligned_write_req_cnt_(0),
    read_req_cnt_(0),
    unaligned_read_req_cnt_(0),
    total_read_size_(0),
    last_access_ts_(-1),
    last_modify_ts_(-1),
    birth_ts_(-1),
    tmp_file_ptr_(nullptr),
    label_(),
    meta_tree_epoch_(0),
    meta_tree_level_cnt_(0),
    meta_size_(0),
    cached_meta_page_num_(0),
    write_back_meta_page_num_(0),
    all_type_page_flush_cnt_(0) {}
  int init(const ObCurTraceId::TraceId &trace_id,
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
           const int64_t write_req_cnt,
           const int64_t unaligned_write_req_cnt,
           const int64_t read_req_cnt,
           const int64_t unaligned_read_req_cnt,
           const int64_t total_read_size,
           const int64_t last_access_ts,
           const int64_t last_modify_ts,
           const int64_t birth_ts,
           const void* const tmp_file_ptr,
           const char* const label);
  void reset();
public:
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
  int64_t write_req_cnt_;
  int64_t unaligned_write_req_cnt_;
  int64_t read_req_cnt_;
  int64_t unaligned_read_req_cnt_;
  int64_t total_read_size_;
  int64_t last_access_ts_;
  int64_t last_modify_ts_;
  int64_t birth_ts_;
  const void *tmp_file_ptr_;
  ObFixedLengthString<ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1> label_;
  int64_t meta_tree_epoch_;
  int64_t meta_tree_level_cnt_;
  int64_t meta_size_;
  int64_t cached_meta_page_num_;
  int64_t write_back_meta_page_num_;
  int64_t all_type_page_flush_cnt_;

  TO_STRING_KV(K(trace_id_), K(tenant_id_), K(dir_id_), K(fd_), K(file_size_),
               K(truncated_offset_), K(is_deleting_), K(cached_data_page_num_),
               K(write_back_data_page_num_), K(flushed_data_page_num_),
               K(ref_cnt_), K(write_req_cnt_), K(unaligned_write_req_cnt_),
               K(read_req_cnt_), K(unaligned_read_req_cnt_), K(total_read_size_),
               K(last_access_ts_), K(last_modify_ts_), K(birth_ts_),
               KP(tmp_file_ptr_), K(label_), K(meta_tree_epoch_),
               K(meta_tree_level_cnt_), K(meta_size_), K(cached_meta_page_num_),
               K(write_back_meta_page_num_), K(all_type_page_flush_cnt_));
};

class ObSharedNothingTmpFile final
{
public:
  struct ObTmpFileNode : public ObDLinkBase<ObTmpFileNode>
  {
    ObTmpFileNode(ObSharedNothingTmpFile &file) : file_(file) {}
    ObSharedNothingTmpFile &file_;
  };

  struct InnerFlushInfo
  {
  public:
    InnerFlushInfo();
    ~InnerFlushInfo() { reset(); }
    void reset();
    bool has_data() const { return flush_data_page_num_ > 0; }
    bool has_meta() const { return !flush_meta_page_array_.empty(); }
    int init_by_tmp_file_flush_info(const ObTmpFileFlushInfo& flush_info);
    TO_STRING_KV(K(flush_data_page_disk_begin_id_), K(flush_data_page_num_), K(flush_virtual_page_id_), K(file_size_),
                 K(flush_meta_page_array_));
  public:
    bool update_meta_data_done_;
    // information for updating data
    int64_t flush_data_page_disk_begin_id_;  // record begin page id in the macro block，for updating meta tree item
    int64_t flush_data_page_num_;
    int64_t flush_virtual_page_id_;          // record virtual_page_id while copying data, pass to meta tree while inserting items
    int64_t file_size_;                      // if file_size > 0, it means the last page is in flushing
    // information for updating meta tree
    ObArray<ObTmpFileTreeIOInfo> flush_meta_page_array_;
  };

  struct InnerFlushContext
  {
    InnerFlushContext()
      : flush_seq_(ObTmpFileGlobal::INVALID_FLUSH_SEQUENCE),
        data_finished_continuous_flush_info_num_(0),
        meta_finished_continuous_flush_info_num_(0),
        data_flush_infos_(),
        meta_flush_infos_(),
        need_to_wait_for_the_previous_data_flush_req_to_complete_(false),
        need_to_wait_for_the_previous_meta_flush_req_to_complete_(false)
        {
          data_flush_infos_.set_attr(ObMemAttr(MTL_ID(), "TmpFileFInfo"));
          meta_flush_infos_.set_attr(ObMemAttr(MTL_ID(), "TmpFileFInfo"));
        }
    void reset();
    int update_finished_continuous_flush_info_num(const bool is_meta, const int64_t end_pos);
    bool is_all_finished() const { return is_data_finished() && is_meta_finished(); }
    bool is_data_finished() const
    {
      return data_flush_infos_.size() == data_finished_continuous_flush_info_num_;
    }
    bool is_meta_finished() const
    {
      return meta_flush_infos_.size() == meta_finished_continuous_flush_info_num_;
    }
    bool is_flushing() const
    {
      return !data_flush_infos_.empty() || !meta_flush_infos_.empty();
    }
    TO_STRING_KV(K(flush_seq_), K(data_flush_infos_.size()), K(meta_flush_infos_.size()),
                 K(data_finished_continuous_flush_info_num_),
                 K(meta_finished_continuous_flush_info_num_),
                 K(need_to_wait_for_the_previous_data_flush_req_to_complete_),
                 K(need_to_wait_for_the_previous_meta_flush_req_to_complete_));
    int64_t flush_seq_;
    int64_t data_finished_continuous_flush_info_num_;
    int64_t meta_finished_continuous_flush_info_num_;
    ObArray<InnerFlushInfo> data_flush_infos_;
    ObArray<InnerFlushInfo> meta_flush_infos_;
    bool need_to_wait_for_the_previous_data_flush_req_to_complete_;
    bool need_to_wait_for_the_previous_meta_flush_req_to_complete_;
  };
public:
  ObSharedNothingTmpFile();
  ~ObSharedNothingTmpFile();
  int init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
           ObTmpFileBlockManager *block_manager,
           ObIAllocator *callback_allocator,
           ObIAllocator *wbp_index_cache_allocator,
           ObIAllocator *wbp_index_cache_bkt_allocator,
           ObTmpFilePageCacheController *page_cache_controller,
           const char* label);
  int destroy();
  void reset();
  bool can_remove();
  bool is_deleting();
  int delete_file();

// ATTENTION!!!
// Currently, K(tmp_file) is used to print the ObSharedNothingTmpFile structure without holding
// the file lock. Before adding the print field, make sure it is thread-safe.
  TO_STRING_KV(K(is_inited_), K(is_deleting_),
               K(tenant_id_), K(dir_id_), K(fd_),
               K(ref_cnt_), K(truncated_offset_), K(read_offset_),
               K(file_size_), K(flushed_data_page_num_), K(write_back_data_page_num_),
               K(cached_page_nums_),
               K(begin_page_id_), K(begin_page_virtual_id_),
               K(flushed_page_id_), K(flushed_page_virtual_id_),
               K(end_page_id_),
               K(data_page_flush_level_), K(meta_page_flush_level_),
               KP(data_flush_node_.get_next()),
               KP(meta_flush_node_.get_next()),
               K(is_in_data_eviction_list_), K(is_in_meta_eviction_list_),
               KP(data_eviction_node_.get_next()),
               KP(meta_eviction_node_.get_next()), K(trace_id_),
               K(write_req_cnt_), K(unaligned_write_req_cnt_), K(read_req_cnt_),
               K(unaligned_read_req_cnt_), K(total_read_size_),
               K(last_access_ts_), K(last_modify_ts_), K(birth_ts_), K(label_));
// XXX Currently, K(tmp_file) is used to print the ObSharedNothingTmpFile structure without holding
// the file lock. Before adding the print field, make sure it is thread-safe.

public:
  int aio_pread(ObTmpFileIOCtx &io_ctx);
  int aio_write(ObTmpFileIOCtx &io_ctx);

  int evict_data_pages(const int64_t expected_evict_page_num,
                       int64_t &actual_evict_page_num,
                       int64_t &remain_flushed_page_num);
  int evict_meta_pages(const int64_t expected_evict_page_num,
                       int64_t &actual_evict_page_num);
  // truncate offset is open interval
  int truncate(const int64_t truncate_offset);

public:
  int update_meta_after_flush(const int64_t info_idx, const bool is_meta, bool &reset_ctx);
  int generate_data_flush_info(ObTmpFileFlushTask &flush_task,
                               ObTmpFileFlushInfo &info,
                               ObTmpFileDataFlushContext &data_flush_context,
                               const int64_t flush_sequence,
                               const bool need_flush_tail);
  int generate_meta_flush_info(ObTmpFileFlushTask &flush_task,
                               ObTmpFileFlushInfo &info,
                               ObTmpFileTreeFlushContext &meta_flush_context,
                               const int64_t flush_sequence,
                               const bool need_flush_tail);
  int insert_meta_tree_item(const ObTmpFileFlushInfo &info, int64_t block_index);
public:
  int remove_flush_node(const bool is_meta);
  int reinsert_flush_node(const bool is_meta);
  int insert_or_update_data_flush_node_();
  int insert_or_update_meta_flush_node_();
  bool is_in_meta_flush_list();
  bool is_flushing();
  OB_INLINE ObTmpFileNode &get_data_flush_node() { return data_flush_node_; }
  OB_INLINE ObTmpFileNode &get_meta_flush_node() { return meta_flush_node_; }
  OB_INLINE ObTmpFileNode &get_data_eviction_node() { return data_eviction_node_; }
  OB_INLINE ObTmpFileNode &get_meta_eviction_node() { return meta_eviction_node_; }
  OB_INLINE const ObTmpFileNode &get_data_eviction_node() const { return data_eviction_node_; }
  OB_INLINE const ObTmpFileNode &get_meta_eviction_node() const { return meta_eviction_node_; }

  OB_INLINE int64_t get_fd() const { return fd_; }
  OB_INLINE int64_t get_dir_id() const { return dir_id_; }
  OB_INLINE void inc_ref_cnt() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE void dec_ref_cnt() { ATOMIC_AAF(&ref_cnt_, -1); }
  OB_INLINE int64_t get_ref_cnt() const { return ATOMIC_LOAD(&ref_cnt_); }
  void update_read_offset(int64_t read_offset);
  int64_t get_file_size();

  OB_INLINE void set_data_page_flush_level(int64_t data_page_flush_level)
  {
    data_page_flush_level_ = data_page_flush_level;
  }
  OB_INLINE int64_t get_data_page_flush_level() const { return data_page_flush_level_; }
  OB_INLINE void set_meta_page_flush_level(int64_t meta_page_flush_level)
  {
    meta_page_flush_level_ = meta_page_flush_level;
  }
  OB_INLINE int64_t get_meta_page_flush_level() const { return meta_page_flush_level_; }
  int64_t get_dirty_data_page_size() const;
  int64_t get_dirty_data_page_size_with_lock();
  void get_dirty_meta_page_num(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num) const;
  void get_dirty_meta_page_num_with_lock(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num);
  int64_t cal_wbp_begin_offset();
  void set_read_stats_vars(const bool is_unaligned_read, const int64_t read_size);
  int copy_info_for_virtual_table(ObSNTmpFileInfo &tmp_file_info);

private:
  int inner_read_truncated_part_(ObTmpFileIOCtx &io_ctx);
  int inner_read_from_wbp_(ObTmpFileIOCtx &io_ctx);
  int inner_read_from_disk_(const int64_t expected_read_disk_size, ObTmpFileIOCtx &io_ctx);
  int inner_seq_read_from_block_(const int64_t block_index,
                                 const int64_t begin_read_offset_in_block, const int64_t end_read_offset_in_block,
                                 ObTmpFileIOCtx &io_ctx, int64_t &actual_read_size);
  int inner_rand_read_from_block_(const int64_t block_index,
                                  const int64_t begin_read_offset_in_block, const int64_t end_read_offset_in_block,
                                  ObTmpFileIOCtx &io_ctx, int64_t &actual_read_size);
  int collect_pages_in_block_(const int64_t block_index,
                              const int64_t begin_page_idx_in_block,
                              const int64_t end_page_idx_in_block,
                              ObTmpFileBlockPageBitmap &bitmap,
                              ObIArray<ObTmpPageValueHandle> &page_value_handles);
  int inner_read_continuous_cached_pages_(const int64_t begin_read_offset_in_block,
                                          const int64_t end_read_offset_in_block,
                                          const ObArray<ObTmpPageValueHandle> &page_value_handles,
                                          const int64_t start_array_idx,
                                          ObTmpFileIOCtx &io_ctx);
  int inner_read_continuous_uncached_pages_(const int64_t block_index,
                                            const int64_t begin_read_offset_in_block,
                                            const int64_t end_read_offset_in_block,
                                            ObTmpFileIOCtx &io_ctx);
  int inner_truncate_(const int64_t truncate_offset, const int64_t wbp_begin_offset);
private:
  int inner_write_(ObTmpFileIOCtx &io_ctx);
  int inner_fill_tail_page_(ObTmpFileIOCtx &io_ctx);
  int load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx);
  int append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx);
  int inner_write_continuous_pages_(ObTmpFileIOCtx &io_ctx);
  int alloc_and_write_pages_(const ObTmpFileIOCtx &io_ctx,
                             ObIArray<uint32_t> &alloced_page_id,
                             int64_t &actual_write_size);
  int truncate_the_first_wbp_page_();

  int copy_flush_data_from_wbp_(ObTmpFileFlushTask &flush_task, ObTmpFileFlushInfo &info,
      ObTmpFileDataFlushContext &data_flush_context,
      const uint32_t copy_begin_page_id, const int64_t copy_begin_page_virtual_id,
      const uint32_t copy_end_page_id, const int64_t flush_sequence, const bool need_flush_tail);
  int cal_end_position_(ObArray<InnerFlushInfo> &flush_infos,
                        const int64_t start_pos,
                        int64_t &end_pos,
                        int64_t &flushed_data_page_num);
  int cal_next_flush_page_id_from_flush_ctx_or_file_(const ObTmpFileDataFlushContext &data_flush_context,
                                                     uint32_t &next_flush_page_id,
                                                     int64_t &next_flush_page_virtual_id);
  int get_next_flush_page_id_(uint32_t& next_flush_page_id, int64_t& next_flush_page_virtual_id) const;
  int get_flush_end_page_id_(uint32_t& flush_end_page_id, const bool need_flush_tail) const;
  int get_physical_page_id_in_wbp(const int64_t virtual_page_id, uint32_t& page_id) const ;
  int remove_useless_page_in_data_flush_infos_(const int64_t start_pos,
                                               const int64_t end_pos,
                                               const int64_t flushed_data_page_num,
                                               int64_t &new_start_pos,
                                               int64_t &new_flushed_data_page_num);
  int update_file_meta_after_flush_(const int64_t start_pos,
                                    const int64_t end_pos,
                                    const int64_t flushed_data_page_num);
  int update_meta_tree_after_flush_(const int64_t start_pos, const int64_t end_pos);

  int generate_data_flush_info_(ObTmpFileFlushTask &flush_task,
                                ObTmpFileFlushInfo &info,
                                ObTmpFileDataFlushContext &data_flush_context,
                                const int64_t flush_sequence,
                                const bool need_flush_tail);
  int generate_meta_flush_info_(ObTmpFileFlushTask &flush_task,
                                ObTmpFileFlushInfo &info,
                                ObTmpFileTreeFlushContext &meta_flush_context,
                                const int64_t flush_sequence,
                                const bool need_flush_tail);
private:
  int reinsert_flush_node_(const bool is_meta);
  OB_INLINE bool has_unfinished_page_() const { return file_size_ % ObTmpFileGlobal::PAGE_SIZE != 0; }

  int64_t cal_wbp_begin_offset_() const;
  OB_INLINE int64_t get_page_begin_offset_by_file_or_block_offset_(const int64_t offset) const
  {
    return common::lower_align(offset, ObTmpFileGlobal::PAGE_SIZE);
  }
  OB_INLINE int64_t get_page_end_offset_by_file_or_block_offset_(const int64_t offset) const
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
  OB_INLINE int64_t get_page_virtual_id_from_offset_(const int64_t page_offset_in_file, const bool is_open_interval) const
  {
    return is_open_interval ?
           common::upper_align(page_offset_in_file, ObTmpFileGlobal::PAGE_SIZE) / ObTmpFileGlobal::PAGE_SIZE - 1 :
           page_offset_in_file / ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_page_id_in_block_(const int64_t page_offset_in_block) const
  {
    return page_offset_in_block / ObTmpFileGlobal::PAGE_SIZE;
  }
  OB_INLINE int64_t get_page_offset_from_file_or_block_offset_(const int64_t offset) const
  {
    return offset % ObTmpFileGlobal::PAGE_SIZE;
  }

private:
  ObTmpFileBlockManager *tmp_file_block_manager_;
  ObIAllocator *callback_allocator_;
  ObTmpFilePageCacheController *page_cache_controller_;
  ObTmpFileFlushPriorityManager *flush_prio_mgr_;
  ObTmpFileEvictionManager *eviction_mgr_;
  ObTmpWriteBufferPool *wbp_;
  ObTmpFileWBPIndexCache page_idx_cache_;
  bool is_inited_;
  bool is_deleting_;
  bool is_in_data_eviction_list_;
  bool is_in_meta_eviction_list_;
  int64_t data_page_flush_level_;
  int64_t meta_page_flush_level_;
  uint64_t tenant_id_;
  int64_t dir_id_;
  int64_t fd_;
  int64_t ref_cnt_;
  int64_t truncated_offset_;      // read data befor truncated_offset will be set as 0
  int64_t read_offset_;           // read offset is on the entire file
  int64_t file_size_;             // has written size of this file
  int64_t flushed_data_page_num_; // equal to the page num between [begin_page_id_, flushed_page_id_] in wbp
  int64_t write_back_data_page_num_;
  int64_t cached_page_nums_;      // page nums in write buffer pool
  uint32_t begin_page_id_;        // the first page index in write buffer pool
  int64_t begin_page_virtual_id_;
  uint32_t flushed_page_id_;      // the last flushed page index in write buffer pool.
                                  // specifically, if flushed_page_id_ == end_page_id_,
                                  // it means the last page has been flushed. However,
                                  // the last page might be appending written after flushing.
                                  // thus, in this case, whether "flushed_page_id_" page in wbp is flushed
                                  // needs to be checked with page status (with some get functions).
  int64_t flushed_page_virtual_id_;
  uint32_t end_page_id_;          // the last page index in write buffer pool
  ObSharedNothingTmpFileMetaTree meta_tree_;
  ObTmpFileNode data_flush_node_;
  ObTmpFileNode meta_flush_node_;
  ObTmpFileNode data_eviction_node_;
  ObTmpFileNode meta_eviction_node_;
  common::TCRWLock meta_lock_; // handle conflicts between writing and reading meta tree and meta data of file
  ObSpinLock last_page_lock_; // handle conflicts between writing and evicting for last page
  ObSpinLock multi_write_lock_; // handle conflicts between multiple writes
  SpinRWLock truncate_lock_; // handle conflicts between truncate and flushing
  InnerFlushContext inner_flush_ctx_; // file-level flush context
  /********for virtual table begin********/
  common::ObCurTraceId::TraceId trace_id_;
  int64_t write_req_cnt_;
  int64_t unaligned_write_req_cnt_;
  int64_t read_req_cnt_;
  int64_t unaligned_read_req_cnt_;
  int64_t total_read_size_;
  int64_t last_access_ts_;
  int64_t last_modify_ts_;
  int64_t birth_ts_;
  ObFixedLengthString<ObTmpFileGlobal::TMP_FILE_MAX_LABEL_SIZE + 1> label_;
  /********for virtual table end********/
};

class ObTmpFileHandle final
{
public:
  ObTmpFileHandle() : ptr_(nullptr) {}
  ObTmpFileHandle(ObSharedNothingTmpFile *tmp_file);
  ObTmpFileHandle(const ObTmpFileHandle &handle);
  ObTmpFileHandle & operator=(const ObTmpFileHandle &other);
  ~ObTmpFileHandle() { reset(); }
  OB_INLINE ObSharedNothingTmpFile * get() const {return ptr_; }
  bool is_inited() { return nullptr != ptr_; }
  void reset();
  int init(ObSharedNothingTmpFile *tmp_file);
  TO_STRING_KV(KP(ptr_));
private:
  ObSharedNothingTmpFile *ptr_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_SHARE_NOTHING_TMP_FILE_H_

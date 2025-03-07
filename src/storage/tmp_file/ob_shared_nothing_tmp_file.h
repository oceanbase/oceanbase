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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_NOTHING_TMP_FILE_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_NOTHING_TMP_FILE_H_

#include "storage/tmp_file/ob_tmp_file_cache.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_i_tmp_file.h"
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
class ObTmpFileBlockPageBitmap;
class ObTmpFileBlockManager;
class ObTmpFileEvictionManager;
class ObTmpFileFlushPriorityManager;
class ObTmpFileFlushInfo;
class ObTmpFileFlushTask;
class ObTmpFilePageCacheController;
class ObTmpFileTreeFlushContext;
class ObTmpFileDataFlushContext;

//for virtual table show
class ObSNTmpFileInfo final : public ObTmpFileInfo
{
public:
  ObSNTmpFileInfo() :
    ObTmpFileInfo(),
    meta_tree_epoch_(0),
    meta_tree_level_cnt_(0),
    meta_size_(0),
    cached_meta_page_num_(0),
    write_back_meta_page_num_(0),
    all_type_page_flush_cnt_(0) {}
  virtual ~ObSNTmpFileInfo() { reset(); }
  virtual void reset() override;
public:
  int64_t meta_tree_epoch_;
  int64_t meta_tree_level_cnt_;
  int64_t meta_size_;
  int64_t cached_meta_page_num_;
  int64_t write_back_meta_page_num_;
  int64_t all_type_page_flush_cnt_;
  INHERIT_TO_STRING_KV("ObTmpFileInfo", ObTmpFileInfo,
                       K(meta_tree_epoch_),
                       K(meta_tree_level_cnt_), K(meta_size_), K(cached_meta_page_num_),
                       K(write_back_meta_page_num_), K(all_type_page_flush_cnt_));
};

class ObSharedNothingTmpFile : public ObITmpFile
{
public:
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
    InnerFlushContext();
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
  virtual ~ObSharedNothingTmpFile();
  int init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
           ObTmpFileBlockManager *block_manager,
           ObIAllocator *callback_allocator,
           ObIAllocator *wbp_index_cache_allocator,
           ObIAllocator *wbp_index_cache_bkt_allocator,
           ObTmpFilePageCacheController *page_cache_controller,
           const char* label);
  virtual void reset() override;
  virtual int release_resource() override;
  int evict_data_pages(const int64_t expected_evict_page_num,
                       int64_t &actual_evict_page_num,
                       int64_t &remain_flushed_page_num);
  int evict_meta_pages(const int64_t expected_evict_page_num,
                       int64_t &actual_evict_page_num);
  // truncate offset is open interval
  virtual int truncate(const int64_t truncate_offset) override;

  // ATTENTION!!!
  // Currently, K(tmp_file) is used to print the ObSharedNothingTmpFile structure without holding
  // the file lock. Before adding the print field, make sure it is thread-safe.
  INHERIT_TO_STRING_KV("ObITmpFile", ObITmpFile,
                       K(flushed_data_page_num_),
                       K(flushed_page_id_), K(flushed_page_virtual_id_),
                       K(meta_page_flush_level_), KP(meta_flush_node_.get_next()),
                       K(is_in_data_eviction_list_), K(is_in_meta_eviction_list_),
                       KP(data_eviction_node_.get_next()),
                       KP(meta_eviction_node_.get_next()));

public:
  // for flush
  int update_meta_after_flush(const int64_t info_idx, const bool is_meta, bool &reset_ctx);
  virtual int generate_data_flush_info(ObTmpFileFlushTask &flush_task,
                               ObTmpFileFlushInfo &info,
                               ObTmpFileDataFlushContext &data_flush_context,
                               const int64_t flush_sequence,
                               const bool need_flush_tail);
  virtual int generate_meta_flush_info(ObTmpFileFlushTask &flush_task,
                               ObTmpFileFlushInfo &info,
                               ObTmpFileTreeFlushContext &meta_flush_context,
                               const int64_t flush_sequence,
                               const bool need_flush_tail);
  int insert_meta_tree_item(const ObTmpFileFlushInfo &info, int64_t block_index);
  int copy_finish();

public:
  int remove_meta_flush_node();
  int reinsert_meta_flush_node();
  bool is_in_meta_flush_list();
  int64_t get_cached_page_num();
  OB_INLINE ObTmpFileNode *get_meta_flush_node() { return &meta_flush_node_; }
  OB_INLINE ObTmpFileNode &get_data_eviction_node() { return data_eviction_node_; }
  OB_INLINE ObTmpFileNode &get_meta_eviction_node() { return meta_eviction_node_; }
  OB_INLINE const ObTmpFileNode &get_data_eviction_node() const { return data_eviction_node_; }
  OB_INLINE const ObTmpFileNode &get_meta_eviction_node() const { return meta_eviction_node_; }

  OB_INLINE void set_meta_page_flush_level(int64_t meta_page_flush_level)
  {
    meta_page_flush_level_ = meta_page_flush_level;
  }
  OB_INLINE int64_t get_meta_page_flush_level() const { return meta_page_flush_level_; }
  void get_dirty_meta_page_num(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num) const;
  void get_dirty_meta_page_num_with_lock(int64_t &non_rightmost_dirty_page_num, int64_t &rightmost_dirty_page_num);
  virtual int copy_info_for_virtual_table(ObTmpFileInfo &tmp_file_info) override;

private:
  virtual int inner_read_from_disk_(const int64_t expected_read_disk_size, ObTmpFileIOCtx &io_ctx) override;
  int inner_direct_read_from_block_(const int64_t block_index,
                                    const int64_t begin_read_offset_in_block, const int64_t end_read_offset_in_block,
                                    ObTmpFileIOCtx &io_ctx);
  int inner_cached_read_from_block_(const int64_t block_index,
                                    const int64_t begin_read_offset_in_block, const int64_t end_read_offset_in_block,
                                    ObTmpFileIOCtx &io_ctx,
                                    ObTmpFileInfo::ObTmpFileReadInfo &read_stat);
  int inner_cached_read_from_block_with_prefetch_(const int64_t block_index,
                                                  const int64_t begin_read_offset_in_block,
                                                  const int64_t end_offset_in_block,
                                                  const int64_t user_read_size,
                                                  ObTmpFileIOCtx &io_ctx,
                                                  ObTmpFileInfo::ObTmpFileReadInfo &read_stat);
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
                                            const int64_t begin_io_read_offset,
                                            const int64_t end_io_read_offset,
                                            const int64_t user_read_size,
                                            ObTmpFileIOCtx &io_ctx);

  virtual int swap_page_to_disk_(const ObTmpFileIOCtx &io_ctx) override;
  virtual int load_disk_tail_page_and_rewrite_(ObTmpFileIOCtx &io_ctx) override;
  virtual int append_write_memory_tail_page_(ObTmpFileIOCtx &io_ctx) override;

  virtual int truncate_the_first_wbp_page_() override;
  virtual int truncate_persistent_pages_(const int64_t truncate_offset) override;

private:
  int collect_flush_data_page_id_(ObTmpFileFlushTask &flush_task, ObTmpFileFlushInfo &info,
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
  virtual int64_t get_dirty_data_page_size_() const override;
  int get_next_flush_page_id_(uint32_t& next_flush_page_id, int64_t& next_flush_page_virtual_id) const;
  int get_flush_end_page_id_(uint32_t& flush_end_page_id, const bool need_flush_tail) const;
  int get_physical_page_id_in_wbp_(const int64_t virtual_page_id, uint32_t& page_id) const ;
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
  int reinsert_meta_flush_node_();
  int insert_or_update_meta_flush_node_();
  virtual bool is_flushing_() override;

  virtual int inner_seal_() override;
  virtual int inner_delete_file_() override;

private:
  ObTmpFileBlockManager *tmp_file_block_manager_;
  ObTmpFilePageCacheController *page_cache_controller_;
  ObTmpFileEvictionManager *eviction_mgr_;
  bool is_in_data_eviction_list_;
  bool is_in_meta_eviction_list_;
  int64_t meta_page_flush_level_;
  int64_t flushed_data_page_num_; // equal to the page num between [begin_page_id_, flushed_page_id_] in wbp
  uint32_t flushed_page_id_;      // the last flushed page index in write buffer pool.
                                  // specifically, if flushed_page_id_ == end_page_id_,
                                  // it means the last page has been flushed. However,
                                  // the last page might be appending written after flushing.
                                  // thus, in this case, whether "flushed_page_id_" page in wbp is flushed
                                  // needs to be checked with page status (with some get functions).
  int64_t flushed_page_virtual_id_;
  ObSharedNothingTmpFileMetaTree meta_tree_;
  ObTmpFileNode meta_flush_node_;
  ObTmpFileNode data_eviction_node_;
  ObTmpFileNode meta_eviction_node_;
  SpinRWLock truncate_lock_; // handle conflicts between truncate and flushing
  InnerFlushContext inner_flush_ctx_; // file-level flush context
};

class ObSNTmpFileHandle final : public ObITmpFileHandle
{
public:
  ObSNTmpFileHandle() : ObITmpFileHandle() {}
  ObSNTmpFileHandle(ObSharedNothingTmpFile *ptr) : ObITmpFileHandle(ptr) {}
  ObSNTmpFileHandle(const ObSNTmpFileHandle &handle) : ObITmpFileHandle(handle) {}
  ObSNTmpFileHandle & operator=(const ObSNTmpFileHandle &other)
  {
    ObITmpFileHandle::operator=(other);
    return *this;
  }
  OB_INLINE ObSharedNothingTmpFile * get() const { return static_cast<ObSharedNothingTmpFile*>(ptr_); }
};

}  // end namespace tmp_file
}  // end namespace oceanbase

#endif // OCEANBASE_STORAGE_TMP_FILE_OB_SHARE_NOTHING_TMP_FILE_H_

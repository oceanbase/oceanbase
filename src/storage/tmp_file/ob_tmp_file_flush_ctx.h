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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_CTX_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_CTX_H_

#include "lib/queue/ob_link_queue.h"
#include "lib/utility/ob_print_utils.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "storage/tmp_file/ob_shared_nothing_tmp_file.h"
#include "storage/tmp_file/ob_tmp_file_global.h"
#include "storage/tmp_file/ob_tmp_file_flush_list_iterator.h"
#include "storage/tmp_file/ob_tmp_file_meta_tree.h"
#include "storage/tmp_file/ob_tmp_file_thread_job.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileFlushTG;

struct ObTmpFileDataFlushContext
{
public:
  ObTmpFileDataFlushContext ()
    : is_valid_(false),
      has_flushed_last_partially_written_page_(false),
      flushed_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      flushed_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID),
      next_flush_page_id_(ObTmpFileGlobal::INVALID_PAGE_ID),
      next_flush_page_virtual_id_(ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID) {}

  ~ObTmpFileDataFlushContext () { reset(); }
  void reset() {
    is_valid_ = false;
    has_flushed_last_partially_written_page_ = false;
    flushed_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    flushed_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
    next_flush_page_id_ = ObTmpFileGlobal::INVALID_PAGE_ID;
    next_flush_page_virtual_id_ = ObTmpFileGlobal::INVALID_VIRTUAL_PAGE_ID;
  }
  OB_INLINE bool is_valid() const { return is_valid_; }
  OB_INLINE void set_is_valid(const bool is_valid) { is_valid_ = is_valid; }
  OB_INLINE bool has_flushed_last_partially_written_page() const {
    return has_flushed_last_partially_written_page_;
  }
  OB_INLINE void set_has_flushed_last_partially_written_page(bool has_flushed_last_partially_written_page) {
    has_flushed_last_partially_written_page_ = has_flushed_last_partially_written_page;
  }
  OB_INLINE uint32_t get_flushed_page_id() const { return flushed_page_id_; }
  OB_INLINE void set_flushed_page_id(const uint32_t flushed_page_id) { flushed_page_id_ = flushed_page_id; }
  OB_INLINE int64_t get_flushed_page_virtual_id() const {
    return flushed_page_virtual_id_;
  }
  OB_INLINE void set_flushed_page_virtual_id(int64_t virtual_page_id) {
    flushed_page_virtual_id_ = virtual_page_id;
  }
  OB_INLINE uint32_t get_next_flush_page_id() const { return next_flush_page_id_; }
  OB_INLINE void set_next_flush_page_id(const uint32_t next_flush_page_id) {
    next_flush_page_id_ = next_flush_page_id;
  }
  OB_INLINE void set_next_flush_page_virtual_id(const int64_t virtual_page_id) {
    next_flush_page_virtual_id_ = virtual_page_id;
  }
  OB_INLINE int64_t get_next_flush_page_virtual_id() const { return next_flush_page_virtual_id_; }
  TO_STRING_KV(K(is_valid_), K(has_flushed_last_partially_written_page_), K(flushed_page_id_),
               K(flushed_page_virtual_id_),
               K(next_flush_page_id_), K(next_flush_page_virtual_id_));
public:
  bool is_valid_;
  bool has_flushed_last_partially_written_page_;
  uint32_t flushed_page_id_;
  int64_t flushed_page_virtual_id_;
  uint32_t next_flush_page_id_;
  int64_t next_flush_page_virtual_id_;
};

struct ObTmpFileSingleFlushContext
{
public:
  ObTmpFileSingleFlushContext() : file_handle_(), data_ctx_(), meta_ctx_() {}
  ObTmpFileSingleFlushContext(ObTmpFileHandle file_handle) : file_handle_(file_handle), data_ctx_(), meta_ctx_() {}
  TO_STRING_KV(K(file_handle_), K(data_ctx_), K(meta_ctx_));
public:
  ObTmpFileHandle file_handle_;
  ObTmpFileDataFlushContext data_ctx_;
  ObTmpFileTreeFlushContext meta_ctx_;
};

// The ObTmpFileFlushManager maintains multiple file flushing contexts during a round of flushing
class ObTmpFileBatchFlushContext
{
public:
  ObTmpFileBatchFlushContext()
    : is_inited_(),
      fail_too_many_(false),
      expect_flush_size_(0),
      actual_flush_size_(0),
      flush_seq_ctx_(),
      flush_monitor_ptr_(nullptr),
      state_(ObTmpFileGlobal::FlushCtxState::FSM_F1),
      iter_(),
      file_ctx_hash_(),
      flush_failed_array_()
  {
  }
  ~ObTmpFileBatchFlushContext() { destroy(); }
  int init();
  int prepare_flush_ctx(const int64_t expect_flush_size,
                        ObTmpFileFlushPriorityManager *prio_mgr,
                        ObTmpFileFlushMonitor *flush_monitor);
  int clear_flush_ctx(ObTmpFileFlushPriorityManager &prio_mgr);
  void destroy();
  void record_flush_stage();
  void record_flush_task(const int64_t data_length);
  void update_actual_flush_size(const ObTmpFileFlushTask &flush_task);
  void try_update_prepare_finished_cnt(const ObTmpFileFlushTask &flush_task, bool& recorded);
public:
  static const int64_t MAX_COPY_FAIL_COUNT = 512;
  typedef hash::ObHashMap<int64_t,
                          ObTmpFileSingleFlushContext,
                          common::hash::NoPthreadDefendMode> ObTmpFileFlushCtxHash;
  struct ObTmpFileFlushFailRecord
  {
  public:
    ObTmpFileFlushFailRecord() : is_meta_(false), file_handle_() {}
    ObTmpFileFlushFailRecord(bool is_meta, ObTmpFileHandle file_handle) : is_meta_(is_meta), file_handle_(file_handle) {}
    TO_STRING_KV(K(is_meta_), K(file_handle_));
  public:
    bool is_meta_;
    ObTmpFileHandle file_handle_;
  };
  struct FlushSequenceContext
  {
    FlushSequenceContext() : create_flush_task_cnt_(0), prepare_finished_cnt_(0), flush_sequence_(0) {}
    int64_t create_flush_task_cnt_; // created flush task number in one round
    int64_t prepare_finished_cnt_;
    int64_t flush_sequence_;  // increate flush seq only when all task in this round send io succ
    TO_STRING_KV(K(create_flush_task_cnt_), K(prepare_finished_cnt_), K(flush_sequence_));
  };
  struct RemoveFileOp
  {
  public:
    RemoveFileOp(ObTmpFileFlushPriorityManager &flush_priority_mgr) : flush_priority_mgr_(flush_priority_mgr) {}
    int operator () (hash::HashMapPair<int64_t, ObTmpFileSingleFlushContext> &kv);
  private:
    ObTmpFileFlushPriorityManager &flush_priority_mgr_;
  };
  bool can_clear_flush_ctx() const
  {
    return flush_seq_ctx_.prepare_finished_cnt_ == flush_seq_ctx_.create_flush_task_cnt_ &&
           flush_seq_ctx_.prepare_finished_cnt_ > 0;
  }
  OB_INLINE void set_fail_too_many(const bool fail_too_many) { fail_too_many_ = fail_too_many; }
  OB_INLINE bool is_fail_too_many() { return fail_too_many_; }
  OB_INLINE ObTmpFileFlushListIterator &get_flush_list_iterator() { return iter_; }
  OB_INLINE void set_expect_flush_size(const int64_t flush_size) { expect_flush_size_ = flush_size; }
  OB_INLINE int64_t get_expect_flush_size() { return expect_flush_size_; }
  OB_INLINE void add_actual_flush_size(const int64_t flush_size) { actual_flush_size_ += flush_size; }
  OB_INLINE int64_t get_actual_flush_size() { return actual_flush_size_; }
  OB_INLINE bool is_inited() { return is_inited_; }
  OB_INLINE void set_state(const FlushCtxState state) { state_ = state; }
  OB_INLINE FlushCtxState get_state() { return state_; }
  OB_INLINE ObTmpFileFlushCtxHash &get_file_ctx_hash() { return file_ctx_hash_; }
  OB_INLINE ObArray<ObTmpFileFlushFailRecord> &get_flush_failed_array() { return flush_failed_array_; }
  OB_INLINE ObTmpFileFlushMonitor *get_flush_monitor() { return flush_monitor_ptr_; }
  OB_INLINE void inc_create_flush_task_cnt() { ++flush_seq_ctx_.create_flush_task_cnt_; }
  OB_INLINE int64_t get_flush_sequence() { return flush_seq_ctx_.flush_sequence_; }
  TO_STRING_KV(K(is_inited_), K(fail_too_many_), K(expect_flush_size_), K(actual_flush_size_), K(flush_seq_ctx_), K(state_), K(iter_));
private:
  bool is_inited_;
  bool fail_too_many_;          // indicate wether the number of file copy failures exceeds MAX_COPY_FAIL_COUNT in one round.
  int64_t expect_flush_size_;
  int64_t actual_flush_size_;
  FlushSequenceContext flush_seq_ctx_;
  ObTmpFileFlushMonitor *flush_monitor_ptr_;
  FlushCtxState state_;
  ObTmpFileFlushListIterator iter_;
  ObTmpFileFlushCtxHash file_ctx_hash_; // maintain the data/meta flushing offset for each file in one round
  ObArray<ObTmpFileFlushFailRecord> flush_failed_array_;  // record files that failed to copy data/meta pages, re-insert them into flush priority mgr
                                                          // after the iteration ends to prevent these files from appearing multiple times in flush iterator.
};

// represent up to 2MB flush information of a file,
// if a macro block contains multiple tmp files, it will include multiple flush infos
struct ObTmpFileFlushInfo
{
public:
  ObTmpFileFlushInfo();
  ~ObTmpFileFlushInfo() { reset(); }
  void reset();
  bool has_data() const { return flush_data_page_num_ > 0; }
  bool has_meta() const { return !flush_meta_page_array_.empty(); }
  TO_STRING_KV(K(fd_), K(batch_flush_idx_), K(has_last_page_lock_), K(insert_meta_tree_done_), K(update_meta_data_done_),
               K(flush_data_page_disk_begin_id_), K(flush_data_page_num_), K(flush_virtual_page_id_), K(file_size_),
               K(flush_meta_page_array_), KP(file_handle_.get()));
public:
  int64_t fd_;
  int64_t batch_flush_idx_; // during one round of flushing, multiple FlushInfo may be generated.
                            // records the sequence number to which this info belongs (starting from 0).
  bool has_last_page_lock_;                // indicate the last page is in flushing and holds last_page_lock_ in file
  bool insert_meta_tree_done_;             // indicate the insertion of the corresponding data item into the meta tree is completed
  bool update_meta_data_done_;             // indicate the file metadata or metadata tree update is completed
  ObTmpFileHandle file_handle_;

  // information for updating data
  int64_t flush_data_page_disk_begin_id_;  // record begin page id in the macro block, for updating meta tree item
  int64_t flush_data_page_num_;
  int64_t flush_virtual_page_id_;          // record virtual_page_id while copying data, pass to meta tree while inserting items
  int64_t file_size_;                      // if file_size > 0, it means the last page is in flushing
  // information for updating meta tree
  ObArray<ObTmpFileTreeIOInfo> flush_meta_page_array_;
};

// Each ObTmpFileFlushTask corresponds to a flushing macro block, which can be exclusively used by one file
// or shared by multiple files based on observer config. The task internally maintains the state machine,
// and the ObTmpFileFlushManager continuously advances each task to TFFT_FINISH terminal state.
// Each task that are higher than TFFT_FILL_BLOCK_BUF will be retried if errors occurred.
struct ObTmpFileFlushTask : public common::ObSpLinkQueue::Link
{
public:
  ObTmpFileFlushTask();
  ~ObTmpFileFlushTask() { destroy(); }
  enum ObTmpFileFlushTaskState
  {
    TFFT_INITED = 0,
    TFFT_ALLOC_BLOCK_BUF = 1,
    TFFT_CREATE_BLOCK_INDEX = 2,
    TFFT_FILL_BLOCK_BUF = 3,
    TFFT_INSERT_META_TREE = 4,
    TFFT_ASYNC_WRITE = 5,
    TFFT_WAIT = 6,
    TFFT_FINISH = 7,
    TFFT_ABORT = 8,
  };
public:
  void destroy();
  int prealloc_block_buf();
  int write_one_block();
  int wait_macro_block_handle();
  int64_t get_total_page_num() const;
  OB_INLINE ObKVCacheInstHandle& get_inst_handle() { return inst_handle_; }
  OB_INLINE ObKVCachePair*& get_kvpair() { return kvpair_; }
  OB_INLINE ObTmpBlockValueHandle& get_block_handle() { return block_handle_; }
  OB_INLINE bool is_valid() const { return OB_NOT_NULL(get_data_buf()); }
  OB_INLINE bool is_full() const { return data_length_ == OB_STORAGE_OBJECT_MGR.get_macro_object_size(); }
  OB_INLINE char *get_data_buf() const { return block_handle_.value_ == nullptr ? nullptr : block_handle_.value_->get_buffer(); }
  OB_INLINE void atomic_set_ret_code(int ret_code) { ATOMIC_SET(&ret_code_, ret_code); }
  OB_INLINE int atomic_get_ret_code() const { return ATOMIC_LOAD(&ret_code_); }
  OB_INLINE void atomic_set_write_block_ret_code(int write_block_ret_code) {
    ATOMIC_SET(&write_block_ret_code_, write_block_ret_code);
  }
  OB_INLINE int atomic_get_write_block_ret_code() const {
    return ATOMIC_LOAD(&write_block_ret_code_);
  }
  OB_INLINE void set_data_length(const int64_t len) { data_length_ = len; }
  OB_INLINE int64_t get_data_length() const { return data_length_; }
  OB_INLINE void set_block_index(const int64_t block_index) { block_index_ = block_index; }
  OB_INLINE int64_t get_block_index() const { return block_index_; }
  OB_INLINE void set_flush_seq(const int64_t flush_seq) { flush_seq_ = flush_seq; }
  OB_INLINE int64_t get_flush_seq() const { return flush_seq_; }
  OB_INLINE void set_create_ts(const int64_t create_ts) { create_ts_ = create_ts; }
  OB_INLINE int64_t get_create_ts() const { return create_ts_; }
  OB_INLINE void atomic_set_io_finished(const bool is_finished) { ATOMIC_SET(&is_io_finished_, is_finished); }
  OB_INLINE bool atomic_get_io_finished() const { return ATOMIC_LOAD(&is_io_finished_); }
  OB_INLINE void set_is_fast_flush_tree(const bool is_fast_flush_tree) { fast_flush_tree_page_ = is_fast_flush_tree; }
  OB_INLINE bool get_is_fast_flush_tree() const { return fast_flush_tree_page_; }
  OB_INLINE void mark_recorded_as_prepare_finished() { recorded_as_prepare_finished_ = true; }
  OB_INLINE bool get_recorded_as_prepare_finished() const { return recorded_as_prepare_finished_; }
  OB_INLINE void set_state(const ObTmpFileFlushTaskState state) { task_state_ = state; }
  OB_INLINE ObTmpFileFlushTaskState get_state() const { return task_state_; }
  OB_INLINE void set_tmp_file_block_handle(const ObTmpFileBlockHandle &tfb_handle) { tmp_file_block_handle_ = tfb_handle; }
  OB_INLINE ObTmpFileBlockHandle &get_tmp_file_block_handle() { return tmp_file_block_handle_; }
  OB_INLINE void set_macro_block_handle(const blocksstable::ObMacroBlockHandle &handle) { handle_ = handle; }
  OB_INLINE blocksstable::ObMacroBlockHandle &get_macro_block_handle() { return handle_; }
  OB_INLINE ObArray<ObTmpFileFlushInfo> &get_flush_infos() { return flush_infos_; }
  OB_INLINE int64_t get_next_free_page_id() { return get_total_page_num(); }
  OB_INLINE bool check_buf_range_valid(const char* buffer, const int64_t length) const
  {
    return buffer != nullptr && get_data_buf() != nullptr &&
           buffer >= get_data_buf() && buffer + length <= get_data_buf() + OB_STORAGE_OBJECT_MGR.get_macro_object_size();
  }
  TO_STRING_KV(KP(this), KP(kvpair_), K(write_block_ret_code_), K(ret_code_), K(data_length_),
               K(block_index_), K(flush_seq_), K(create_ts_), K(is_io_finished_),
               K(fast_flush_tree_page_), K(recorded_as_prepare_finished_), K(task_state_), K(tmp_file_block_handle_), K(flush_infos_));
private:
  ObKVCacheInstHandle inst_handle_;
  ObKVCachePair *kvpair_;
  ObTmpBlockValueHandle block_handle_;
  int write_block_ret_code_;
  int ret_code_;
  int64_t data_length_;       // data length (including padding to make length upper align to page size)
  int64_t block_index_;       // tmp file block logical index in ObTmpFileBlockManager
  int64_t flush_seq_;         // flush sequence, for verification purpose
  int64_t create_ts_;
  bool is_io_finished_;
  bool fast_flush_tree_page_; // indicate the task requires fast flush tree pages
  bool recorded_as_prepare_finished_;
  ObTmpFileFlushTaskState task_state_;
  ObTmpFileBlockHandle tmp_file_block_handle_;// hold a reference to the corresponding tmp file block to prevent it from being released
  blocksstable::ObMacroBlockHandle handle_;
  ObArray<ObTmpFileFlushInfo> flush_infos_;   // multi file flush into one block if size > 0
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_BLOCKSSTABLE_TMP_FILE_OB_TMP_FILE_FLUSH_CTX_H_

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

#ifndef OB_STORE_FILE_H_
#define OB_STORE_FILE_H_

#include "lib/io/ob_io_manager.h"
#include "lib/atomic/ob_atomic.h"
#include "ob_block_sstable_struct.h"
#include "ob_local_file_system.h"
#include "ob_macro_block_meta_mgr.h"
#include "storage/blocksstable/ob_micro_block_index_mgr.h"
#include "share/ob_unit_getter.h"
#include "share/ob_thread_mgr.h"
#include "storage/blocksstable/ob_macro_block_struct.h"

#define OB_STORE_FILE (oceanbase::blocksstable::ObStoreFile::get_instance())

namespace oceanbase {
namespace storage {
class ObPartitionService;
class ObSSTableRowScanner;
class ObMicroBlockIndexHandleMgr;
}  // namespace storage
namespace compaction {
class ObMacroBlockLoader;
}
namespace blocksstable {
class ObStoreFile;

struct ObMacroBlockInfo {
  int32_t ref_cnt_;
  int32_t write_seq_;
  bool is_free_;
  int64_t access_time_;
  ObMacroBlockInfo() : ref_cnt_(0), write_seq_(0), is_free_(false), access_time_(0)
  {}
  TO_STRING_KV(K_(ref_cnt), K_(write_seq), K_(is_free), K_(access_time));
};

struct ObMacroBlockWriteInfo {
  const char* buffer_;
  int64_t size_;
  ObFullMacroBlockMeta meta_;
  common::ObIODesc io_desc_;

  ObMacroBlocksWriteCtx* block_write_ctx_;
  // reuse_block_ctx_ are only used when reuse old block id
  const ObMacroBlockCtx* reuse_block_ctx_;
  ObMacroBlockWriteInfo();
  OB_INLINE bool is_valid() const;
  OB_INLINE bool is_reuse_macro_block() const
  {
    return NULL != reuse_block_ctx_;
  }
  TO_STRING_KV(KP_(buffer), K_(meta), K_(size), K_(io_desc), K_(block_write_ctx), K_(reuse_block_ctx));
};

struct ObMacroBlockReadInfo {
  const ObMacroBlockCtx* macro_block_ctx_;
  int64_t offset_;
  int64_t size_;
  common::ObIODesc io_desc_;
  common::ObIOCallback* io_callback_;
  ObMacroBlockReadInfo() : macro_block_ctx_(NULL), offset_(), size_(), io_desc_(), io_callback_(NULL)
  {}
  OB_INLINE bool is_valid() const;
  TO_STRING_KV(K_(offset), K_(size), K_(io_desc), KP_(io_callback), K_(macro_block_ctx));
};

class ObMacroBlocksHandle {
public:
  ObMacroBlocksHandle();
  virtual ~ObMacroBlocksHandle();
  int add(const MacroBlockId& macro_id);
  int assign(const common::ObIArray<MacroBlockId>& list);
  int64_t count() const
  {
    return macro_id_list_.count();
  }
  MacroBlockId at(const int64_t i) const
  {
    return macro_id_list_.at(i);
  }
  const common::ObArray<MacroBlockId>& get_macro_id_list() const
  {
    return macro_id_list_;
  }
  common::ObArray<MacroBlockId>& get_macro_id_list()
  {
    return macro_id_list_;
  }
  void reset();
  int reserve(const int64_t block_cnt);
  void set_storage_file(ObStorageFile* file)
  {
    file_ = file;
  }
  ObStorageFile* get_storage_file()
  {
    return file_;
  }
  TO_STRING_KV(K_(macro_id_list), "tenant_id", nullptr == file_ ? 0 : file_->get_tenant_id(), "file_id",
      nullptr == file_ ? 0 : file_->get_file_id());

private:
  common::ObArray<MacroBlockId> macro_id_list_;
  DISALLOW_COPY_AND_ASSIGN(ObMacroBlocksHandle);
  ObStorageFile* file_;
};

class ObMacroBlockHandle {
public:
  ObMacroBlockHandle() : block_write_ctx_(NULL), file_(NULL)
  {}
  virtual ~ObMacroBlockHandle();
  ObMacroBlockHandle(const ObMacroBlockHandle& other);
  void set_file(ObStorageFile* file)
  {
    file_ = file;
  }
  ObMacroBlockHandle& operator=(const ObMacroBlockHandle& other);
  void reset();
  void reuse();
  OB_INLINE MacroBlockId get_macro_id() const
  {
    return macro_id_;
  }
  OB_INLINE int64_t get_data_size()
  {
    return io_handle_.get_data_size();
  }
  OB_INLINE bool is_valid()
  {
    return io_handle_.is_valid();
  }
  OB_INLINE bool is_empty()
  {
    return io_handle_.is_empty();
  }
  int wait(const int64_t timeout_ms);
  OB_INLINE const char* get_buffer()
  {
    return io_handle_.get_buffer();
  }
  int set_macro_block_id(const MacroBlockId& macro_block_id);
  common::ObIOHandle& get_io_handle()
  {
    return io_handle_;
  }
  OB_INLINE void set_block_write_ctx(ObMacroBlocksWriteCtx* block_write_ctx)
  {
    block_write_ctx_ = block_write_ctx;
  }
  TO_STRING_KV(K_(macro_id), K_(io_handle), KP_(block_write_ctx));

private:
  MacroBlockId macro_id_;
  common::ObIOHandle io_handle_;
  ObMacroBlocksWriteCtx* block_write_ctx_;
  ObStorageFile* file_;
};

class ObMacroBlockHandleV1 {
public:
  ObMacroBlockHandleV1();
  virtual ~ObMacroBlockHandleV1();
  ObMacroBlockHandleV1(const ObMacroBlockHandleV1& other);
  ObMacroBlockHandleV1& operator=(const ObMacroBlockHandleV1& other);
  void reset();
  OB_INLINE MacroBlockId get_macro_id() const
  {
    return macro_id_;
  }
  OB_INLINE int64_t get_data_size()
  {
    return io_handle_.get_data_size();
  }
  OB_INLINE bool is_valid()
  {
    return io_handle_.is_valid();
  }
  OB_INLINE bool is_empty()
  {
    return io_handle_.is_empty();
  }
  int wait(const int64_t timeout_ms);
  OB_INLINE const char* get_buffer()
  {
    return io_handle_.get_buffer();
  }
  int set_macro_block_id(const MacroBlockId& macro_block_id);
  common::ObIOHandle& get_io_handle()
  {
    return io_handle_;
  }
  TO_STRING_KV(K_(macro_id), K_(io_handle));

private:
  MacroBlockId macro_id_;
  common::ObIOHandle io_handle_;
};

class ObStoreFileGCTask : public common::ObTimerTask {
public:
  ObStoreFileGCTask();
  virtual ~ObStoreFileGCTask();
  virtual void runTimerTask();
};

struct ObBadBlockInfo {
public:
  ObBadBlockInfo()
  {
    reset();
  }
  ~ObBadBlockInfo()
  {}
  OB_INLINE void reset()
  {
    MEMSET(this, 0, sizeof(*this));
  }
  TO_STRING_KV(K(disk_id_), K(macro_block_id_), K(error_type_), K(store_file_path_), K(error_msg_), K(check_time_));

public:
  int64_t disk_id_;
  MacroBlockId macro_block_id_;
  int64_t error_type_;
  char store_file_path_[MAX_PATH_SIZE];
  char error_msg_[common::OB_MAX_ERROR_MSG_LEN];
  int64_t check_time_;
};

class ObAllMacroIdIterator : public ObIMacroIdIterator {
public:
  ObAllMacroIdIterator();
  virtual ~ObAllMacroIdIterator();
  virtual int get_next_macro_id(MacroBlockId& block_id);

private:
  uint32_t cur_pos_;
};

template <typename T>
class ObSegmentArray final {
public:
  ObSegmentArray();
  ~ObSegmentArray();
  int reserve(const int64_t size);
  const T& operator[](const int64_t index) const;
  T& operator[](const int64_t index);
  void reset();

private:
  static const int64_t ELEMENTS_PER_SEGMENT = 1638400;
  static const int64_t MAX_SEGMENT_CNT = 1024;  // max supported file 1600T
  common::ObArray<T> segments_[MAX_SEGMENT_CNT];
  int64_t segment_cnt_;            // segment cnt
  int64_t last_segment_free_cnt_;  // last postive segment element count
};
template <typename T>
ObSegmentArray<T>::ObSegmentArray() : segments_(), segment_cnt_(0), last_segment_free_cnt_(0)
{}
template <typename T>
ObSegmentArray<T>::~ObSegmentArray()
{}
template <typename T>
void ObSegmentArray<T>::reset()
{
  for (int64_t i = 0; i < segment_cnt_; ++i) {
    segments_[i].reset();
  }
  segment_cnt_ = 0;
  last_segment_free_cnt_ = 0;
}
template <typename T>
int ObSegmentArray<T>::reserve(const int64_t size)
{
  int ret = common::OB_SUCCESS;
  const int64_t delta_size = size - (segment_cnt_ * ELEMENTS_PER_SEGMENT - last_segment_free_cnt_);
  if (delta_size > last_segment_free_cnt_) {
    const int64_t new_delta_segment_cnt = (delta_size - last_segment_free_cnt_) / ELEMENTS_PER_SEGMENT + 1;
    const int64_t new_segment_cnt = segment_cnt_ + new_delta_segment_cnt;
    const int64_t new_last_segment_free_cnt = new_segment_cnt * ELEMENTS_PER_SEGMENT - size;
    if (new_segment_cnt > MAX_SEGMENT_CNT) {
      ret = common::OB_NOT_SUPPORTED;
      STORAGE_LOG(WARN, "file size is too large, not supported", K(ret), K(size));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < new_delta_segment_cnt; ++i) {
      if (OB_FAIL(segments_[segment_cnt_ + i].prepare_allocate(ELEMENTS_PER_SEGMENT))) {
        STORAGE_LOG(WARN, "fail to reserve segment", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      segment_cnt_ = new_segment_cnt;
      last_segment_free_cnt_ = new_last_segment_free_cnt;
    }
  } else {
    last_segment_free_cnt_ -= delta_size;
  }
  return ret;
}
template <typename T>
T& ObSegmentArray<T>::operator[](const int64_t index)
{
  const int64_t segment_idx = index / ELEMENTS_PER_SEGMENT;
  const int64_t element_idx = index % ELEMENTS_PER_SEGMENT;
  return segments_[segment_idx][element_idx];
}
template <typename T>
const T& ObSegmentArray<T>::operator[](const int64_t index) const
{
  const int64_t segment_idx = index / ELEMENTS_PER_SEGMENT;
  const int64_t element_idx = index % ELEMENTS_PER_SEGMENT;
  return segments_[segment_idx][element_idx];
}

class ObStoreFile {
public:
  static ObStoreFile& get_instance();
  int init(const ObStorageEnv& storage_env, ObStoreFileSystem* store_file_system);
  int open(const bool is_physical_flashback = false);
  void destroy();
  void stop();
  void wait();
  int write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle);
  int async_write_block(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandle& macro_handle);
  int async_write_block_v1(const ObMacroBlockWriteInfo& write_info, ObMacroBlockHandleV1& macro_handle);
  int read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle);
  int async_read_block(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandle& macro_handle);
  int async_read_block_v1(const ObMacroBlockReadInfo& read_info, ObMacroBlockHandleV1& macro_handle);
  int fsync();
  void inc_ref(const MacroBlockId macro_id);
  void dec_ref(const MacroBlockId macro_id);
  void inc_ref(const common::ObIArray<MacroBlockId>& macro_id_list);
  void dec_ref(const common::ObIArray<MacroBlockId>& macro_id_list);
  int check_disk_full(const int64_t required_size) const;
  bool is_disk_full() const;
  int get_store_status(ObMacroBlockMarkerStatus& status);
  int get_macro_block_info(const int64_t block_index, ObMacroBlockInfo& macro_block_info);
  int report_bad_block(const MacroBlockId& macro_block_id, const int64_t error_type, const char* error_msg);
  int get_bad_block_infos(common::ObArray<ObBadBlockInfo>& bad_block_infos);
  OB_INLINE const char* get_store_file_path()
  {
    return sstable_dir_;
  }
  OB_INLINE int64_t get_free_macro_block_count() const
  {
    return free_block_cnt_;
  }
  int add_disk(
      const common::ObString& diskgroup_name, const common::ObString& disk_path, const common::ObString& alias_name);
  int drop_disk(const common::ObString& diskgroup_name, const common::ObString& alias_name);
  int is_free_block(const int64_t block_index, bool& is_free);
  int resize_file(const int64_t new_data_file_size, const int64_t new_data_file_disk_percentage);

private:
  friend class ObStoreFileGCTask;
  friend class ObFileSystemInspectBadBlockTask;
  friend class ObAllMacroIdIterator;
  ObStoreFile();
  virtual ~ObStoreFile();
  int alloc_block(ObMacroBlockHandle& macro_handle);
  void free_block(const uint32_t block_idx, bool& is_freed);
  int mark_macro_blocks();
  void mark_and_sweep();
  OB_INLINE bool is_valid(const MacroBlockId macro_id);
  OB_INLINE void bitmap_set(const int64_t block_idx);
  OB_INLINE bool bitmap_test(const int64_t block_idx);
  bool is_bad_block(const MacroBlockId& macro_block_id);
  int read_checkpoint_and_replay_log(bool& is_replay_old);
  void disable_mark_sweep()
  {
    ATOMIC_SET(&is_mark_sweep_enabled_, false);
  }
  void enable_mark_sweep()
  {
    ATOMIC_SET(&is_mark_sweep_enabled_, true);
  }
  bool is_mark_sweep_enabled()
  {
    return ATOMIC_LOAD(&is_mark_sweep_enabled_);
  }
  int wait_mark_sweep_finish();
  void set_mark_sweep_doing();
  void set_mark_sweep_done();
  int alloc_memory(const int64_t total_macro_block_cnt, uint32_t*& free_block_array, uint64_t*& macro_block_bitmap,
      ObSegmentArray<ObMacroBlockInfo>& macro_block_info_array);
  int64_t get_macro_bitmap_array_cnt(const int64_t macro_block_cnt)
  {
    return macro_block_cnt / 64 + 1;
  }

private:
  static const int64_t RECYCLE_DELAY_US = 5 * 1000 * 1000;  // 5s
  static const int64_t INSPECT_DELAY_US = 1 * 1000 * 1000;  // 1s
  bool is_inited_;
  bool is_opened_;
  char sstable_dir_[common::OB_MAX_FILE_NAME_LENGTH];
  common::ObLfFIFOAllocator allocator_;
  lib::ObMutex block_lock_;
  int64_t free_block_push_pos_;
  int64_t free_block_pop_pos_;
  uint32_t free_block_cnt_;
  uint32_t* free_block_array_;
  uint64_t* macro_block_bitmap_;
  ObSegmentArray<ObMacroBlockInfo> macro_block_info_;
  lib::ObMutex ckpt_lock_;
  lib::ObMutex meta_array_lock_;
  int64_t cur_meta_array_pos_;
  common::ObArray<MacroBlockId> meta_block_ids_[2];
  ObStoreFileGCTask gc_task_;
  ObFileSystemInspectBadBlockTask inspect_bad_block_task_;
  char* print_buffer_;
  int64_t print_buffer_size_;
  int64_t mark_cost_time_;
  int64_t sweep_cost_time_;
  int64_t hold_macro_cnt_;
  int64_t used_macro_cnt_[ObMacroBlockCommonHeader::MaxMacroType];
  lib::ObMutex bad_block_lock_;
  common::ObArray<ObBadBlockInfo> bad_block_infos_;
  ObStoreFileSystem* store_file_system_;
  bool is_mark_sweep_enabled_;
  bool is_doing_mark_sweep_;
  ObThreadCond cond_;  // for mark sweep
};

OB_INLINE bool ObStoreFile::is_valid(const MacroBlockId macro_id)
{
  return macro_id.block_index() < store_file_system_->get_total_macro_block_count();
}

OB_INLINE void ObStoreFile::bitmap_set(const int64_t block_idx)
{
  uint64_t mask = 1;
  macro_block_bitmap_[block_idx / 64] |= (mask << (block_idx % 64));
}

OB_INLINE bool ObStoreFile::bitmap_test(const int64_t block_idx)
{
  uint64_t mask = 1;
  return 0 != (macro_block_bitmap_[block_idx / 64] & (mask << (block_idx % 64)));
}

bool ObMacroBlockWriteInfo::is_valid() const
{
  bool bool_ret = true;

  if (!io_desc_.is_valid() || NULL == block_write_ctx_) {
    bool_ret = false;
  } else if (NULL != reuse_block_ctx_) {  // reuse block
    if (NULL != buffer_ || 0 != size_ || !meta_.is_valid()) {
      bool_ret = false;
    }
  } else {  // not reuse block
    if (NULL == buffer_ || 0 >= size_ || !meta_.is_valid()) {
      bool_ret = false;
    }
  }
  return bool_ret;
}

bool ObMacroBlockReadInfo::is_valid() const
{
  return OB_NOT_NULL(macro_block_ctx_) && offset_ >= 0 && size_ > 0 && io_desc_.is_valid();
}

}  // namespace blocksstable
}  // namespace oceanbase

#endif /* OB_STORE_FILE_H_ */

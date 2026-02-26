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
#include "storage/tmp_file/ob_tmp_file_meta_tree.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"
#include "lib/lock/ob_spin_rwlock.h"
#include "lib/lock/ob_spin_lock.h"
#include "lib/allocator/ob_fifo_allocator.h"
#include "lib/queue/ob_fixed_queue.h"
#include "storage/tmp_file/ob_tmp_file_io_ctx.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFileIOReadCtx;
class ObTmpFileIOWriteCtx;
class ObTmpFileBlockPageBitmap;
class ObTmpFileBlockManager;

class ObSharedNothingTmpFile : public ObITmpFile
{
public:
  ObSharedNothingTmpFile();
  virtual ~ObSharedNothingTmpFile();
  int init(const uint64_t tenant_id, const int64_t fd, const int64_t dir_id,
           ObTmpFileBlockManager *block_manager,
           ObTmpFileWriteCache *write_cache,
           ObIAllocator *callback_allocator,
           const char* label);
  virtual void reset() override;
  virtual int release_resource() override;

  virtual int write(ObTmpFileIOWriteCtx &io_ctx) override;
  int write(ObTmpFileIOWriteCtx &io_ctx, int64_t &cur_file_size);

  // truncate offset is open interval
  virtual int truncate(const int64_t truncate_offset) override;

  // ATTENTION!!!
  // Currently, K(tmp_file) is used to print the ObSharedNothingTmpFile structure without holding
  // the file lock. Before adding the print field, make sure it is thread-safe.
  INHERIT_TO_STRING_KV("ObITmpFile", ObITmpFile,
                       K(allocated_file_size_),
                       K(pre_allocated_file_size_),
                       K(pre_allocated_batch_page_num_),
                       K(write_info_), K(read_info_));
public:
  virtual int copy_info_for_virtual_table(ObTmpFileBaseInfo &tmp_file_info) override;

private:
  virtual int inner_delete_file_() override;
  virtual int inner_read_valid_part_(ObTmpFileIOReadCtx &io_ctx) override;
  int inner_read_cached_page_(ObTmpFileIOReadCtx &io_ctx, ObIArray<int64_t> &uncached_pages);
  int inner_read_uncached_page_(ObTmpFileIOReadCtx &io_ctx, ObIArray<int64_t> &uncached_pages);
  int inner_read_uncached_continuous_page_(ObTmpFileIOReadCtx &io_ctx, ObIArray<int64_t> &uncached_pages,
                                           const int64_t range_start_idx, const int64_t range_end_idx,
                                           ObSharedNothingTmpFileDataItem& previous_item);
  int inner_direct_read_from_block_(ObTmpFileIOReadCtx &io_ctx, const int64_t block_index,
                                    const int64_t block_begin_read_offset, const int64_t read_size,
                                    char *read_buf);
  int inner_cached_read_from_block_(ObTmpFileIOReadCtx &io_ctx, const int64_t block_index,
                                    const int64_t block_begin_read_offset,
                                    const int64_t file_begin_read_offset,
                                    const int64_t file_end_read_offset,
                                    const int64_t user_read_size,
                                    char *read_buf);

  int alloc_write_range_(const ObTmpFileIOWriteCtx &io_ctx, int64_t &start_write_offset, int64_t &end_write_offset);
  int alloc_write_range_from_exclusive_block_(const int64_t timeout_ms, int64_t &cur_pre_allocated_page_virtual_id);
  int alloc_write_range_from_shared_block_(const int64_t timeout_ms, const int64_t expected_last_page_virtual_id, int64_t &cur_pre_allocated_page_virtual_id);
  int inner_batch_write_(ObTmpFileIOWriteCtx &io_ctx, const int64_t start_write_offset, const int64_t end_write_offset);
  int write_fully_page_(ObTmpFileIOWriteCtx &io_ctx,
                        const int64_t virtual_page_id,
                        const ObTmpFilePageId &page_id,
                        const int64_t page_offset,
                        const int64_t write_size,
                        const char* write_buf,
                        ObTmpFileBlockHandle &block_handle);
  int write_partly_page_(ObTmpFileIOWriteCtx &io_ctx,
                         const int64_t virtual_page_id,
                         const ObTmpFilePageId &page_id,
                         const int64_t page_offset,
                         const int64_t write_size,
                         const char* write_buf,
                         ObTmpFileBlockHandle &block_handle);
  int alloc_partly_write_page_(ObTmpFileIOWriteCtx &io_ctx,
                               const int64_t virtual_page_id,
                               const ObTmpFilePageId &page_id,
                               const ObTmpFileBlockHandle &block_handle,
                               ObTmpFilePageHandle &page_handle);
  int inner_write_page_(const int64_t page_offset,
                        const int64_t write_size,
                        const char* write_buf,
                        ObTmpFilePageHandle &page_handle,
                        ObTmpFileBlockHandle &block_handle);

  int truncate_write_cache_(const int64_t truncate_offset);

private:
  // for virtual table
  virtual void inner_set_read_stats_vars_(const ObTmpFileIOReadCtx &ctx, const int64_t read_size);
  virtual void inner_set_write_stats_vars_(const ObTmpFileIOWriteCtx &ctx);
private:
  ObTmpFileBlockManager *tmp_file_block_manager_;
  ObTmpFileWriteCache *write_cache_;
  ObSharedNothingTmpFileMetaTree meta_tree_;
  ObSpinLock serial_write_lock_; // handle conflicts between multiple writes
  ObSpinLock alloc_incomplete_page_lock_; // handle conflicts between multiple writes allocating same page
  // the offset in [0, file_size) is readable data
  // the offset in [file_size, allocated_file_size) is writing data
  // the offset in [allocated_file_size, pre_allocated_file_size) is pre-allocated data which has inserted in meta_tree_
  int64_t allocated_file_size_;
  int64_t pre_allocated_file_size_;
  int64_t pre_allocated_batch_page_num_;
  ObTmpFileWriteInfo write_info_;
  ObTmpFileReadInfo read_info_;
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

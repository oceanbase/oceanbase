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

#ifndef OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_FLUSH_TASK_H_
#define OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_FLUSH_TASK_H_

#include "lib/container/ob_array.h"
#include "lib/queue/ob_link_queue.h"
#include "lib/task/ob_timer_service.h"
#include "storage/tmp_file/ob_tmp_file_write_cache.h"
#include "storage/tmp_file/ob_tmp_file_write_cache_page.h"
#include "storage/blocksstable/ob_macro_block_handle.h"
#include "storage/tmp_file/ob_tmp_file_block_manager.h"

namespace oceanbase
{
namespace tmp_file
{
class ObTmpFilePageHandle;
class ObTmpFileFlushTask;
typedef ObTmpFilePage::PageListNode PageNode;

struct ObTmpFileWriteBlockTimerTask : public common::ObTimerTask
{
public:
  ObTmpFileWriteBlockTimerTask(ObTmpFileFlushTask &flush_task)
    : flush_task_(flush_task) {}
  virtual ~ObTmpFileWriteBlockTimerTask() {}
  virtual void runTimerTask() override;
private:
  ObTmpFileFlushTask &flush_task_;
};

class ObTmpFileFlushTask : public common::ObSpLinkQueue::Link
{
public:
  ObTmpFileFlushTask();
  virtual ~ObTmpFileFlushTask();
  int init(ObFIFOAllocator *flush_allocator, ObTmpFileBlockHandle block_handle);
  void reset();
  int write();
  int wait();
  int cancel(int ret_code);
  int add_page(ObTmpFilePageHandle &page_handle);
  void set_write_ret(int write_ret) { ATOMIC_SET(&write_ret_, write_ret); }
  void set_is_written(bool is_written) { ATOMIC_SET(&is_written_, is_written); }
  void set_page_idx(int32_t page_idx) { page_idx_ = page_idx; }
  void set_page_cnt(int32_t page_cnt) { page_cnt_ = page_cnt; }
  int32_t get_page_idx() const { return page_idx_; }
  int32_t get_page_cnt() const { return page_cnt_; }
  int get_ret_code() const { return ATOMIC_LOAD(&ret_code_); }
  int get_write_ret() const { return ATOMIC_LOAD(&write_ret_); }
  bool is_finished() const { return ATOMIC_LOAD(&is_finished_); }
  bool is_written() { return ATOMIC_LOAD(&is_written_); }
  ObIArray<ObTmpFilePageHandle> &get_page_array() {return page_array_; }
  ObTmpFileBlockHandle get_block_handle() { return tmp_file_block_handle_; }
  ObTmpFileWriteBlockTimerTask &get_write_block_task() { return write_block_task_; }
  OB_INLINE bool check_buf_range_valid(const char* buffer, const int64_t length) const
  {
    return length > 0 && buffer != nullptr && buffer >= buf_ && buffer + length <= buf_ + size_;
  }
  TO_STRING_KV(KP(this), K(is_written_), K(is_finished_), K(ret_code_),
               K(size_), K(page_idx_), K(page_cnt_),
               KP(buf_), K(create_ts_), K(page_array_.count()),
               K(io_handle_), K(tmp_file_block_handle_));
private:
  static const int64_t SCHEDULE_TIME_WARN_MS = 30 * 1000;
  int memcpy_pages_();
  int calc_and_set_meta_page_checksum_(char* page_buff);
private:
  bool is_finished_;
  bool is_written_;
  int write_ret_;
  int ret_code_;
  int32_t page_idx_;
  int32_t page_cnt_;
  int32_t size_;
  char *buf_;
  int64_t create_ts_;
  ObFIFOAllocator *allocator_;
  ObTmpFileBlockHandle tmp_file_block_handle_;
  common::ObIOHandle io_handle_;
  ObSEArray<ObTmpFilePageHandle, ObTmpFileGlobal::BLOCK_PAGE_NUMS> page_array_;
  ObTmpFileWriteBlockTimerTask write_block_task_;
};

}  // end namespace tmp_file
}  // end namespace oceanbase
#endif // OCEANBASE_STORAGE_TMP_FILE_OB_TMP_FILE_FLUSH_TASK_H_

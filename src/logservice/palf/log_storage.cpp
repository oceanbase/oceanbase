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

#define USING_LOG_PREFIX PALF
#include "log_storage.h"
#include "lib/ob_errno.h"            // OB_INVALID_ARGUMENT
#include "log_reader_utils.h"        // ReadBuf
#include "share/scn.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace palf
{
class LogReader;
LogStorage::LogStorage() :
    block_mgr_(),
    log_reader_(),
    log_tail_(),
    log_block_header_(),
    curr_block_writable_size_(0),
    need_append_block_header_(false),
    palf_id_(INVALID_PALF_ID),
    logical_block_size_(0),
    tail_info_lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
    delete_block_lock_(common::ObLatchIds::PALF_LOG_ENGINE_LOCK),
    update_manifest_cb_(),
    is_inited_(false)
{}

LogStorage::~LogStorage()
{
  destroy();
}

int LogStorage::init(const char *base_dir, const char *sub_dir, const LSN &base_lsn,
                     const int64_t palf_id, const int64_t logical_block_size,
                     const int64_t align_size, const int64_t align_buf_size,
                     const UpdateManifestCallback &update_manifest_cb,
                     ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(do_init_(base_dir,
                              sub_dir,
                              base_lsn,
                              palf_id,
                              logical_block_size,
                              align_size,
                              align_buf_size,
                              update_manifest_cb,
                              log_block_pool))) {
    PALF_LOG(WARN, "LogStorage do_init_ failed", K(ret), K(base_dir), K(sub_dir), K(palf_id));
  } else {
    PALF_LOG(INFO, "LogStorage init success", K(ret), K(base_dir), K(sub_dir),
             K(palf_id), K(base_lsn));
  }
  return ret;
}

int LogStorage::load_manifest_for_meta_storage(block_id_t &expected_next_block_id)
{
  int ret = OB_SUCCESS;
  block_id_t log_tail_block_id = lsn_2_block(log_tail_, PALF_BLOCK_SIZE);
  // if last block is full, last_block_id will be the next block id of 'last block'
  // NB: nowdays, there is no possible which last block is empty but the header of this block is valid.
  block_id_t last_block_id = (0 == curr_block_writable_size_ ? log_tail_block_id - 1 : log_tail_block_id);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  // NB: nowdays, we not support switch block when updat manifest failed, therefore, we don't need
  // handle this case.
  //
  // If we need support switch block when write failed, the solution is that:
	// 1. only delete prev block when in append_meta interface;
	// 2. if last meta block is empty, we alose need read its block header.
  } else if (OB_FAIL(
                 read_block_header_(last_block_id, log_block_header_))) {
    PALF_LOG(WARN, "read_block_header_ failed", K(ret), KPC(this));
  } else {
    expected_next_block_id= lsn_2_block(log_block_header_.get_min_lsn(), PALF_BLOCK_SIZE);
    PALF_LOG(INFO, "load_manifest_for_meta_storage success", K(ret), KPC(this), K(expected_next_block_id));
  }
  return ret;
}

void LogStorage::destroy()
{
  is_inited_ = false;
  logical_block_size_ = 0;
  palf_id_ = INVALID_PALF_ID;
  need_append_block_header_ = false;
  curr_block_writable_size_ = 0;
  log_block_header_.reset();
  readable_log_tail_.reset();
  log_tail_.reset();
  log_reader_.destroy();
  block_mgr_.destroy();
  PALF_LOG(INFO, "LogStorage destroy success");
}

int LogStorage::writev(const LSN &lsn, const LogWriteBuf &write_buf, const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t write_size = write_buf.get_total_size();
  // Nowdays, no need to get_log_tail_guarded_by_lock_
  // const LSN &log_tail = get_log_tail_guarded_by_lock_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogStorage not inited!!!", K(ret));
  } else if (false == write_buf.is_valid() || false == lsn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(lsn), K(write_buf));
  } else if (true == log_tail_.is_valid() && lsn != log_tail_) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, log_tail_ is not continous with lsn", K(ret),
             K(log_tail_), K(lsn));
    // NB: 'switch_next_block' firstly, and then write BlockHeader of this block
  } else if (lsn + write_buf.get_total_size()
             > LSN((lsn_2_block(lsn, logical_block_size_) + 1) * logical_block_size_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "not support cross-file write", K(ret), KPC(this), K(lsn), K(write_buf));
  } else if (true == need_switch_block_() && OB_FAIL(inner_switch_block_())) {
    PALF_LOG(ERROR, "switch_next_block failed", K(ret), K(lsn), K(log_tail_));
    // For restart, the last block may have no data, however, we need append_block_header_
    // before first writev opt.
  } else if (true == need_append_block_header_
             && OB_FAIL(append_block_header_(lsn, scn))) {
    PALF_LOG(ERROR, "append_block_header_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(block_mgr_.writev(
                 lsn_2_block(lsn, logical_block_size_), get_phy_offset_(lsn), write_buf))) {
    PALF_LOG(ERROR, "LogVirtualFileMgr writev failed", K(ret), K(write_buf), K(lsn));
  } else {
    curr_block_writable_size_ -= write_size;
    update_log_tail_guarded_by_lock_(write_size);
    PALF_LOG(TRACE, "LogStorage writev success", K(ret), K(log_block_header_), K(lsn),
             K(log_tail_), K(write_buf), KPC(this));
  }
  return ret;
}

int LogStorage::writev(const LSNArray &lsn_array,
                       const LogWriteBufArray &write_buf_array,
                       const SCNArray &scn_array)
{
  int ret = OB_SUCCESS;
  int64_t count = lsn_array.count();
  if (count <= 0 || false == lsn_array[0].is_valid() || OB_ISNULL(write_buf_array[0])
      || (!scn_array[0].is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(ret), K(count));
  } else {
    // 'merge_start_idx' used to record the start index of 'write_buf_array' which to be merged.
    int64_t merge_start_idx = 0;
    do {
      LSN lsn = lsn_array[merge_start_idx];
      LogWriteBuf *write_buf = write_buf_array[merge_start_idx];
      SCN scn = scn_array[merge_start_idx];
      int64_t writable_size =
          (0 == curr_block_writable_size_ ? logical_block_size_ : curr_block_writable_size_)
          - write_buf->get_total_size();
      bool has_merged = true;
      // termination conditions for merging:
      // 1. 'writable_size' is smaller than or equal to 0;
      // 2. there is no LogWriteBuf to be merged;
      // 3. last LogWriteBuf has not been merged.
      //
      // 'merge_start_idx' used to record the index of 'write_buf_array' which to be merged.
      int64_t idx_to_be_merged = merge_start_idx + 1;
      while (true == has_merged && OB_SUCC(ret) && 0 < writable_size && idx_to_be_merged < count) {
        LogWriteBuf *write_buf_to_be_merged = write_buf_array[idx_to_be_merged];
        if (OB_ISNULL(write_buf_to_be_merged)) {
          ret = OB_ERR_UNEXPECTED;
          PALF_LOG(ERROR, "write_buf_array has nulllptr, unexpected error!!!", K(ret),
                   KP(write_buf_to_be_merged), K(idx_to_be_merged));
        } else {
          const int64_t write_buf_to_be_merged_size = write_buf_to_be_merged->get_total_size();
          // If size of LogWriteBuf which to be merged is greater than 'writable_size', unexpected error.
          if (writable_size - write_buf_to_be_merged_size < 0) {
            ret = OB_ERR_UNEXPECTED;
            PALF_LOG(ERROR, "nowdays, we don't support there is any one write opt cross file", K(ret),
                K(writable_size), K(write_buf_to_be_merged));
          } else if (OB_FAIL(write_buf->merge(*write_buf_to_be_merged, has_merged))) {
            PALF_LOG(ERROR, "merge write_buf failed", K(ret), KPC(write_buf),
                     KPC(write_buf_to_be_merged), K(merge_start_idx));
          } else if (false == has_merged) {
            PALF_LOG(INFO, "no need to merge", K(ret), KPC(this), K(write_buf),
                     KPC(write_buf_to_be_merged));
          } else {
            idx_to_be_merged++;
            writable_size -= write_buf_to_be_merged_size;
          }
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(writev(lsn, *write_buf, scn))) {
        PALF_LOG(ERROR, "writev failed", K(ret), K(scn), K(lsn_array), K(write_buf_array));
      } else {
        // update 'merge_start_idx' to 'idx_to_be_merged' after writev successfully.
        merge_start_idx = idx_to_be_merged;
        PALF_LOG(TRACE, "writev one success", K(ret), K(merge_start_idx), K(merge_start_idx),
            K(writable_size), KPC(this), K(count), K(lsn_array));
      }
    } while (merge_start_idx < count && OB_SUCC(ret));
  }
  return ret;
}

int LogStorage::append_meta(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const bool need_switch_block = need_switch_block_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (NULL == buf || 0 >= buf_len || buf_len != MAX_INFO_BLOCK_SIZE) {
    ret = OB_INVALID_ARGUMENT;
  } else if (log_tail_ + buf_len
             > LSN((lsn_2_block(log_tail_, logical_block_size_) + 1) * logical_block_size_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "not support cross-file write", K(ret), KPC(this));
  } else if (true == need_switch_block && OB_FAIL(inner_switch_block_())) {
    PALF_LOG(ERROR, "switch_next_block failed", K(ret), K(log_tail_));
  } else if (true == need_append_block_header_
             && OB_FAIL(append_block_header_used_for_meta_storage_())) {
    PALF_LOG(ERROR, "append_block_header_used_for_meta_storage_ failed", K(ret), KPC(this));
  } else if (OB_FAIL(block_mgr_.pwrite(lsn_2_block(log_tail_, logical_block_size_),
                                       get_phy_offset_(log_tail_),
                                       buf,
                                       buf_len))) {
    PALF_LOG(ERROR, "LogBlockMgr pwrite failed", K(ret), KPC(this));
    // need delete prev meta block when first write success after switch next block.
  } else if (true == need_switch_block 
             && OB_FAIL(delete_prev_block_for_meta_())) {
    PALF_LOG(ERROR, "delete_prev_block_ failed", K(ret), KPC(this));
  } else {
    curr_block_writable_size_ -= buf_len;
    update_log_tail_guarded_by_lock_(buf_len);
    PALF_LOG(INFO, "LogStorage append meta success", K(ret), K(log_block_header_),
             K(log_tail_), KPC(this));
  }
  return ret;
}

int LogStorage::pread(const LSN &read_lsn, const int64_t in_read_size, ReadBuf &read_buf,
                      int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  bool need_read_with_block_header = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogStorage not inited!!!", K(ret));
  } else if (false == read_lsn.is_valid() || 0 >= in_read_size
             || false == read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(read_lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(inner_pread_(read_lsn, in_read_size, need_read_with_block_header, read_buf, out_read_size))) {
    PALF_LOG(WARN, "inner_pread_ failed", K(ret), K(read_lsn), K(in_read_size), KPC(this));
  } else {
  }
  return ret;
}

int LogStorage::pread_without_block_header(const LSN &read_lsn,
                                           const int64_t in_read_size,
                                           ReadBuf &read_buf,
                                           int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  bool need_read_with_block_header = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    PALF_LOG(ERROR, "LogStorage not inited!!!", K(ret));
  } else if (false == read_lsn.is_valid() || 0 >= in_read_size || false == read_buf.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "Invalid argument!!!", K(ret), K(read_lsn), K(in_read_size), K(read_buf));
  } else if (OB_FAIL(inner_pread_(read_lsn, in_read_size, need_read_with_block_header, read_buf, out_read_size))) {
    PALF_LOG(WARN, "inner_pread_ failed", K(ret), K(read_lsn), K(in_read_size), KPC(this));
  } else {
  }
  return ret;
}

int LogStorage::truncate(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  // Nowdays, no need to get_log_tail_guarded_by_lock_
  // const LSN &log_tail = get_log_tail_guarded_by_lock_();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
  } else if (lsn > log_tail_) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), K(log_tail_));
  } else if (lsn < log_tail_ && OB_FAIL(inner_truncate_(lsn))) {
    PALF_LOG(WARN, "LogBlockMgr inner_truncat_ failed", K(ret), K(lsn));
  } else {
    PALF_LOG(INFO, "truncate success", K(ret), K(lsn), KPC(this));
  }
  return ret;
}

int LogStorage::inner_truncate_(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  const block_id_t lsn_block_id = lsn_2_block(lsn, logical_block_size_);
  const block_id_t log_tail_block_id = lsn_2_block(log_tail_, logical_block_size_);
  const block_id_t expected_next_block_id = lsn_block_id + 1;
  if (lsn_block_id != log_tail_block_id && OB_FAIL(update_manifest_cb_(expected_next_block_id))) {
    PALF_LOG(WARN,
             "inner_truncat_ update_manifest_cb_ failed",
             K(ret),
             K(expected_next_block_id),
             KPC(this));
  } else if (OB_FAIL(block_mgr_.truncate(lsn_2_block(lsn, logical_block_size_),
                                         get_phy_offset_(lsn)))) {
    PALF_LOG(WARN, "block_mgr_ truncate success", K(ret), K(lsn), KPC(this));
  } else {
    reset_log_tail_for_last_block_(lsn, true);
    PALF_LOG(INFO, "inner_truncate_ success", K(ret), K(lsn), KPC(this));
  }
  return ret;
}

void LogStorage::truncate_block_header_(const LSN &lsn)
{
  if (0 == lsn_2_offset(lsn, logical_block_size_)) {
    log_block_header_.reset();
  }
}

int LogStorage::truncate_prefix_blocks(const LSN &lsn)
{
  int ret = OB_SUCCESS;
  block_id_t block_id = lsn_2_block(lsn, logical_block_size_);
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t truncate_end_block_id = LOG_INVALID_BLOCK_ID;
  // case1: 'block_id' locate in (infinity, min_using_block_id), avoid hole, we need
  // delete all blocks.(Nowdays don't support) case2: 'block_id' locate in
  // [min_using_block_id, infinity), we don't need ensure that there are at least two
  // blocks, the prev LogInfo has been saved in LogMeta.
  if (OB_FAIL(get_block_id_range(min_block_id, max_block_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret), KPC(this));
  } else if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    PALF_LOG(INFO, "there is no block on disk, truncate_prefix_blocks success", KPC(this));
  } else {
    // If 'block_id' is smaller than or equal to 'max_block_id', need delete all blocks
    // before 'block_id' (not include 'block_id'), otherwise, need delete all blocks
    // before 'max_block_id'(include 'max_block_id') and reset 'log_tail_' to 'lsn';
    truncate_end_block_id = MIN(block_id, max_block_id + 1);
    PALF_LOG(INFO, "truncate_prefix_blocks trace", K(truncate_end_block_id), KPC(this));
    for (block_id_t i = min_block_id; i < truncate_end_block_id && OB_SUCC(ret); i++) {
      if (OB_FAIL(delete_block(i)) && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        PALF_LOG(ERROR, "ObLogStorage delete_block failed", K(ret), KPC(this), K(i),
                 K(min_block_id), K(truncate_end_block_id));
      } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        PALF_LOG(INFO, "file not exist, may be deleted by other modules", K(ret),
                 KPC(this), K(i), K(min_block_id), K(truncate_end_block_id));
        ret = OB_SUCCESS;
      } else {
        PALF_LOG(INFO, "delete block success", K(ret), KPC(this), K(i), K(min_block_id),
                 K(truncate_end_block_id));
      }
    }
  }
  if (OB_SUCC(ret) && block_id > max_block_id) {
    PALF_LOG(WARN, "need reset log_tail", K(ret), K(block_id),
             KPC(this));
		reset_log_tail_for_last_block_(lsn, false);
    block_mgr_.reset(lsn_2_block(lsn, logical_block_size_));
  }
  PALF_EVENT("LogStorage truncate_prefix_blocks finihsed", palf_id_, K(ret), KPC(this),
             K(lsn), K(block_id), K(min_block_id), K(max_block_id),
             K(truncate_end_block_id));
  return ret;
}

// step1. create tmp block.
// step2. update manifest.
// step3. reset log tail.
int LogStorage::begin_flashback(const LSN &start_lsn_of_block)
{
  int ret = OB_SUCCESS;
  const block_id_t tmp_block_id = lsn_2_block(start_lsn_of_block, logical_block_size_);
  // create block with 'tmp_block_id.tmp', and swap it with 'curr_writable_handler_' in 'block_mgr_'
  if (start_lsn_of_block >= log_tail_) {
    ret = OB_INVALID_ARGUMENT;
    PALF_LOG(ERROR, "invalid argument", K(ret), KPC(this), K(start_lsn_of_block));
  } else if (OB_FAIL(block_mgr_.create_tmp_block_handler(tmp_block_id))) {
    PALF_LOG(ERROR, "LogBlockMgr create_tmp_block_handler failed", K(ret), KPC(this), K(start_lsn_of_block));
  } else {
    const LSN origin_log_tail = log_tail_;
    // make tmp block be writeable, set log_tail_ to start_lsn_of_block.
    reset_log_tail_for_last_block_(start_lsn_of_block, true);
    ObSpinLockGuard guard(tail_info_lock_);
    // In process of flashback, each block after start_lsn_of_block is still readable.
    readable_log_tail_ = origin_log_tail;
    PALF_EVENT("[BEGIN STORAGE FLASHBACK]", palf_id_, KPC(this), K(start_lsn_of_block));
  }
  return ret;
}

// step1: delete each block after start_lsn_of_block
// step2: rename tmp block to normal
// step2: set readable_log_tail_ to log_tail_, make each block after start_lsn_of_block is not
// readable.
int LogStorage::end_flashback(const LSN &start_lsn_of_block)
{
  int ret = OB_SUCCESS;
  const block_id_t block_id = lsn_2_block(start_lsn_of_block, logical_block_size_);
  // update manifest
  const block_id_t log_tail_block_id = lsn_2_block(log_tail_, logical_block_size_);
  const block_id_t expected_next_block_id = log_tail_block_id + 1;
  if (OB_FAIL(update_manifest_cb_(expected_next_block_id))) {
    PALF_LOG(WARN, "update_manifest_cb_ failed", K(ret), KPC(this), K(block_id),
				K(expected_next_block_id), K(start_lsn_of_block));
	} else if (OB_FAIL(block_mgr_.delete_block_from_back_to_front_until(block_id))) {
    PALF_LOG(ERROR, "delete_block_from_back_to_front_until failed", K(ret),
				KPC(this), K(start_lsn_of_block));
  } else if (OB_FAIL(block_mgr_.rename_tmp_block_handler_to_normal(block_id))) {
    PALF_LOG(ERROR, "LogBlockMgr rename_tmp_block_handler_to_normal failed", K(ret), KPC(this),
        K(start_lsn_of_block));
  } else {
		ObSpinLockGuard guard(tail_info_lock_);
    readable_log_tail_ = log_tail_;
    PALF_EVENT("[END STORAGE FLASHBACK]", palf_id_, KPC(this), K(start_lsn_of_block));
  }
  return ret;
}

int LogStorage::delete_block(const block_id_t &block_id)
{
  int ret = OB_SUCCESS;
  // NB: delete_block will be called by 'BlockGC' and 'truncate_prefix_blocks', and
  // delete_block is not atomic('::unlink')
  ObSpinLockGuard guard(delete_block_lock_);
  if (OB_FAIL(block_mgr_.delete_block(block_id))) {
    PALF_LOG(WARN, "LogBlockMgr delete_block failed", K(ret), K(block_id), K(log_tail_));
    // when delete last block, we need reset 'log_block_header_' and
    // 'log_tail_'('truncate_prefix_blocks' will delete last block).
  } else {
    PALF_LOG(INFO, "LogStorage delete_block success", K(ret), K(block_id), KPC(this));
  }
  return ret;
}

int LogStorage::get_block_id_range(block_id_t &min_block_id,
                                   block_id_t &max_block_id) const
{
  return block_mgr_.get_block_id_range(min_block_id, max_block_id);
}

int LogStorage::get_block_min_scn(const block_id_t &block_id, SCN &min_scn) const
{
  int ret = OB_SUCCESS;
  LogBlockHeader block_header;
  if (!is_valid_block_id(block_id)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(read_block_header_(block_id, block_header))) {
    PALF_LOG(WARN, "read_block_header_ failed", K(ret), K(block_id), KPC(this));
  } else {
    min_scn = block_header.get_min_scn();
    PALF_LOG(TRACE, "get_block_min_scn success", K(block_id), K(min_scn), KPC(this));
  }
  return ret;
}

const LSN LogStorage::get_begin_lsn() const
{
  int ret = OB_SUCCESS;
  LSN lsn;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  if (OB_FAIL(get_block_id_range(min_block_id, max_block_id))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      lsn = log_tail_;
    } else {
      PALF_LOG(WARN, "get_block_id_range failed", K(ret), KPC(this));
    }
  } else {
    lsn.val_ = logical_block_size_ * min_block_id;
  }
  return lsn;
}

const LSN LogStorage::get_end_lsn() const
{
  ObSpinLockGuard guard(tail_info_lock_);
  return log_tail_;
}
  
// @brief this function is called for 'switch_next_block'(redo log).
int LogStorage::update_manifest_used_for_meta_storage(const block_id_t expected_max_block_id)
{
  int ret = OB_SUCCESS;
  block_id_t log_tail_block_id = lsn_2_block(log_tail_, logical_block_size_);
  block_id_t last_block_id = (0 == curr_block_writable_size_ ? log_tail_block_id - 1 : log_tail_block_id);
  // for meta storage, it will record manifest for log storage in block header,
  // we can not write block header in 'log_tail_block_id', this will cause write 
  // log error in LogBlockMgr because 'log_tail_block_id' is not same as 'curr_writable_block_id'(LogBlockMgr)
  // assume 'log_tail_' is equal to PALF_PHY_BLOCK_SIZE, 'log_tail_block_id' is 1, however
  // 'curr_writable_block_id' is 0.
  if (OB_FAIL(update_block_header_(last_block_id, LSN(expected_max_block_id*PALF_BLOCK_SIZE), SCN::min_scn()))) {
    PALF_LOG(WARN, "append_block_header_ failed", K(ret), KPC(this), K(last_block_id), K(log_tail_block_id));
  } else {
    PALF_LOG(INFO, "update_manifest_used_for_meta_storage success", K(ret), KPC(this));
  }
  return ret;
}

bool LogStorage::need_switch_block_() const
{
  // NB: Nowdays, each block is fulled with data.
  OB_ASSERT(curr_block_writable_size_ >= 0);
  return 0ul == curr_block_writable_size_;
}

int LogStorage::load_last_block_(const block_id_t min_block_id,
                                 const block_id_t max_block_id)
{
  int ret = OB_SUCCESS;
  // defense code
  // if the last block is full of data, 'last_block_offset' is the tail of logical block
  const offset_t last_block_offset = LSN((max_block_id + 1) * logical_block_size_) == log_tail_
                                         ? logical_block_size_
                                         : lsn_2_offset(log_tail_, logical_block_size_);
  if (false == log_tail_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(block_mgr_.load_block_handler(
                 max_block_id, last_block_offset + MAX_INFO_BLOCK_SIZE))) {
    PALF_LOG(WARN, "load_block_handler_ failed", K(ret), K(log_tail_));
  } else {
    curr_block_writable_size_ = logical_block_size_ - last_block_offset;
    // NB: the first block must has valid LogBlockHeader, otherwise, if the last block is
    // not first block, meanwhile, it's empty, we need execute 'append_block_header_' in
    // first writev(append) opt.
    need_append_block_header_ =
        (curr_block_writable_size_ == logical_block_size_) ? true : false;
    // update 'curr_block_id_' of LogBlockHeader
    OB_ASSERT(curr_block_writable_size_ <= logical_block_size_);
  }
  return ret;
}

int LogStorage::do_init_(const char *base_dir,
                         const char *sub_dir,
                         const LSN &base_lsn,
                         const int64_t palf_id,
                         const int64_t logical_block_size,
                         const int64_t align_size,
                         const int64_t align_buf_size,
                         const UpdateManifestCallback &update_manifest_cb,
                         ILogBlockPool *log_block_pool)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  char log_dir[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
  if (0 > (tmp_ret =
               snprintf(log_dir, OB_MAX_FILE_NAME_LENGTH, "%s/%s", base_dir, sub_dir))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "LogStorage snprintf failed", K(ret), K(tmp_ret));
  } else if (FALSE_IT(memset(block_header_serialize_buf_, '\0', MAX_INFO_BLOCK_SIZE))) {
  } else if (OB_FAIL(block_mgr_.init(log_dir,
                                     lsn_2_block(base_lsn, logical_block_size),
                                     align_size,
                                     align_buf_size,
                                     logical_block_size + MAX_INFO_BLOCK_SIZE,
                                     log_block_pool))) {
    PALF_LOG(ERROR, "LogBlockMgr init failed", K(ret), K(log_dir));
  } else if (OB_FAIL(log_reader_.init(log_dir, logical_block_size + MAX_INFO_BLOCK_SIZE))) {
    PALF_LOG(ERROR, "LogReader init failed", K(ret), K(log_dir));
  } else {
    log_tail_ = readable_log_tail_ = base_lsn;
    log_block_header_.reset();
    curr_block_writable_size_ = 0;
    need_append_block_header_ = true;
    palf_id_ = palf_id;
    logical_block_size_ = logical_block_size;
    update_manifest_cb_ = update_manifest_cb;
    is_inited_ = true;
  }
  if (OB_FAIL(ret) && OB_INIT_TWICE != ret) {
    destroy();
  }
  return ret;
}

bool LogStorage::check_read_out_of_lower_bound_(const block_id_t &block_id) const
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  if (OB_FAIL(get_block_id_range(min_block_id, max_block_id))
      && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(WARN, "get_block_id_range failed", K(ret), K(min_block_id), K(max_block_id));
  } else {
    bool_ret = (min_block_id > block_id || max_block_id < block_id || OB_ENTRY_NOT_EXIST == ret);
    PALF_LOG(TRACE, "curr block id range", K(min_block_id), K(max_block_id));
  }
  return bool_ret;
}

int LogStorage::inner_switch_block_()
{
  int ret = OB_SUCCESS;
  const block_id_t block_id = lsn_2_block(log_tail_, logical_block_size_);
  // 'expected_next_block_id' used to check whether disk is integral, we make sure that either it's
  // empty or it doesn't exist.
  const block_id_t expected_next_block_id = block_id + 1;
  if (OB_FAIL(block_mgr_.switch_next_block(block_id))) {
    PALF_LOG(ERROR, "switch_next_block failed", K(ret));
  } else if (OB_FAIL(update_manifest_cb_(expected_next_block_id))) {
    PALF_LOG(WARN, "update_manifest_cb_ failed", K(ret), KPC(this), K(block_id));
  } else {
    PALF_LOG(INFO, "inner_switch_block_ success", K(ret), K(log_block_header_),
             K(block_id));
    curr_block_writable_size_ = logical_block_size_;
    need_append_block_header_ = true;
  }
  return ret;
}

int LogStorage::append_block_header_used_for_meta_storage_()
{
  // For meta storage, the 'log_block_header_' is always valid except the first write
  //
  // 1. After restart, 'log_block_header_' will reinit to the block header of last valid block.
  // 2. In case of switching block, 'log_block_header' will be the result of last update.
	//
	// NB: nowdays, we no need to handle the case append block header into meta block failed.
  int ret = OB_SUCCESS;
  if (OB_FAIL(append_block_header_(log_block_header_.get_min_lsn(), SCN::min_scn()))) {
    PALF_LOG(WARN, "append_block_header_ failed", K(ret), KPC(this));
  } else {
    PALF_LOG(INFO, "append_block_header_used_for_meta_storage_ success", K(ret), KPC(this));
  }
  return ret;
}

int LogStorage::update_block_header_(const block_id_t block_id,
                                     const LSN &block_min_lsn,
                                     const SCN &block_min_scn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  log_block_header_.update_lsn_and_scn(block_min_lsn, block_min_scn);
  log_block_header_.update_palf_id_and_curr_block_id(
      palf_id_, lsn_2_block(log_tail_, logical_block_size_));
  log_block_header_.calc_checksum();

  if (FALSE_IT(memset(block_header_serialize_buf_, '\0', MAX_INFO_BLOCK_SIZE))) {
  } else if (OB_FAIL(log_block_header_.serialize(block_header_serialize_buf_,
                                                 MAX_INFO_BLOCK_SIZE, pos))) {
    PALF_LOG(ERROR, "serialize info block failed", K(ret));
  } else if (OB_FAIL(block_mgr_.pwrite(block_id, 0, block_header_serialize_buf_,
                                       MAX_INFO_BLOCK_SIZE))) {
    PALF_LOG(ERROR, "write info block failed", K(ret), K(block_id), KPC(this));
  } else {
    PALF_LOG(INFO, "append_block_header_ success", K(ret), K(block_id), K(log_block_header_));
    need_append_block_header_ = false;
  }
  return ret;
}

int LogStorage::append_block_header_(const LSN &block_min_lsn,
                                     const SCN &block_min_scn)
{
  const block_id_t block_id = lsn_2_block(log_tail_, logical_block_size_);
  return update_block_header_(block_id, block_min_lsn, block_min_scn);
}


void LogStorage::update_log_tail_guarded_by_lock_(const int64_t log_size)
{
  ObSpinLockGuard guard(tail_info_lock_);
  log_tail_ = log_tail_ + log_size;
  // NB: In the process of flashback, 'readable_log_tail_' is the back of last block
  // 'log_tail_' is the front of last block.
  if (readable_log_tail_ < log_tail_) {
    readable_log_tail_ = log_tail_;
  }
}

void LogStorage::update_log_tail_guarded_by_lock_(const LSN &lsn)
{
  ObSpinLockGuard guard(tail_info_lock_);
  log_tail_ = lsn;
  readable_log_tail_ = log_tail_;
}

const LSN &LogStorage::get_log_tail_guarded_by_lock_() const
{
  ObSpinLockGuard guard(tail_info_lock_);
  return log_tail_;
}

const LSN &LogStorage::get_readable_log_tail_guarded_by_lock_() const
{
  ObSpinLockGuard guard(tail_info_lock_);
  return readable_log_tail_;
}

offset_t LogStorage::get_phy_offset_(const LSN &lsn) const
{
  return lsn_2_offset(lsn, logical_block_size_) + MAX_INFO_BLOCK_SIZE;
}

int LogStorage::read_block_header_(const block_id_t block_id,
                                   LogBlockHeader &log_block_header) const
{
  int ret = OB_SUCCESS;
  const int64_t in_read_size = MAX_INFO_BLOCK_SIZE;
  int64_t out_read_size = 0;
  int64_t pos = 0;
  ReadBufGuard read_buf_guard("LogStorage", in_read_size);
  ReadBuf &read_buf = read_buf_guard.read_buf_;

  // 'log_tail' and 'block_header' are snapshot, we can read valid data even if the block
  // is deleted. NB: we need ensure that the lsn_2_block('log_tail') is smaller than or
  // equal to 'max_block_id'.
  LSN log_tail = get_readable_log_tail_guarded_by_lock_();
  block_id_t max_block_id = lsn_2_block(log_tail, logical_block_size_);
  bool last_block_has_data = (0 == lsn_2_offset(log_tail, logical_block_size_) ? false : true);
  if (block_id > max_block_id || (block_id == max_block_id && false == last_block_has_data)) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    PALF_LOG(WARN, "block_id is large than max_block_id", K(ret), K(block_id),
             K(log_tail), K(max_block_id), K(log_block_header));
  } else if (OB_FAIL(
                 log_reader_.pread(block_id, 0, in_read_size, read_buf, out_read_size))) {
    PALF_LOG(WARN, "read info block failed", K(ret), K(read_buf));
  } else if (OB_FAIL(log_block_header.deserialize(read_buf.buf_, out_read_size, pos))) {
    PALF_LOG(WARN, "deserialize info block failed", K(ret), K(read_buf),
             K(out_read_size));
  } else if (false == log_block_header.check_integrity()) {
    ret = OB_INVALID_DATA;
    PALF_LOG(ERROR, "info block has been corrupted!!!", K(log_block_header), K(block_id));
  } else {
    PALF_LOG(TRACE, "read_block_header_ success", K(ret), K(block_id),
             K(log_block_header));
  }

  if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
    if (true == check_read_out_of_lower_bound_(block_id)) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
      PALF_LOG(WARN, "this block has been deleted", K(ret), K(block_id));
    } else {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "unexpected error, maybe deleted by human!!!", K(ret), K(block_id));
    }
  }
  return ret;
}

// NB: delete each block before last block, and last block must exist valid data.
int LogStorage::delete_prev_block_for_meta_()
{
  int ret = OB_SUCCESS;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  if (OB_FAIL(block_mgr_.get_block_id_range(min_block_id, max_block_id))) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR, "unexpected error, there are must some blocks", K(ret), KPC(this));
  } else {
    for (block_id_t delete_block_id = min_block_id;
         OB_SUCC(ret) && delete_block_id < max_block_id; delete_block_id++) {
      if (OB_FAIL(block_mgr_.delete_block(delete_block_id))) {
        PALF_LOG(WARN, "delete_block failed", K(ret), KPC(this));
      }
    }
  }
  return ret;
}

int LogStorage::inner_pread_(const LSN &read_lsn,
                             const int64_t in_read_size,
                             const bool need_read_log_block_header,
                             ReadBuf &read_buf,
                             int64_t &out_read_size)
{
  int ret = OB_SUCCESS;
  // NB: don't support read data from diffent file.
  const LSN log_tail = get_readable_log_tail_guarded_by_lock_();
  const block_id_t read_block_id = lsn_2_block(read_lsn, logical_block_size_);
  const LSN curr_block_end_lsn = LSN((read_block_id + 1) * logical_block_size_);
  const LSN &max_readable_lsn = MIN(log_tail, curr_block_end_lsn);
  const int64_t real_in_read_size = MIN(max_readable_lsn - read_lsn, in_read_size);
  const offset_t read_offset = lsn_2_offset(read_lsn, logical_block_size_);
  const offset_t real_read_offset =
    read_offset == 0 && true ==  need_read_log_block_header ? 0 : get_phy_offset_(read_lsn);

  if (read_lsn >= log_tail) {
    ret = OB_ERR_OUT_OF_UPPER_BOUND;
    PALF_LOG(WARN, "read something out of upper bound", K(ret), K(read_lsn), K(log_tail_));
  } else if (OB_FAIL(log_reader_.pread(read_block_id,
                                       real_read_offset,
                                       real_in_read_size,
                                       read_buf,
                                       out_read_size))) {
    PALF_LOG(
        WARN, "LogReader pread failed", K(ret), K(read_lsn), K(log_tail_), K(real_in_read_size));
  } else {
    PALF_LOG(TRACE,
             "inner_pread success",
             K(ret),
             K(read_lsn),
             K(in_read_size),
             K(real_in_read_size),
             K(read_lsn),
             K(out_read_size),
             K(log_tail));
  }

  if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
    if (true == check_read_out_of_lower_bound_(lsn_2_block(read_lsn, logical_block_size_))) {
      ret = OB_ERR_OUT_OF_LOWER_BOUND;
      PALF_LOG(WARN, "this block has been deleted", K(ret), K(read_lsn));
    } else {
      ret = OB_ERR_UNEXPECTED;
      PALF_LOG(ERROR, "unexpected error, maybe deleted by human!!!", K(ret), K(read_lsn));
    }
  }
  return ret;
}

void LogStorage::reset_log_tail_for_last_block_(const LSN &lsn, bool last_block_exist)
{
  ObSpinLockGuard guard(tail_info_lock_);
  offset_t logical_offset = lsn_2_offset(lsn, logical_block_size_);
  (void)truncate_block_header_(lsn);
  curr_block_writable_size_ = (true == last_block_exist) ? logical_block_size_ - logical_offset : 0;
  need_append_block_header_ = (curr_block_writable_size_ == logical_block_size_) ? true : false;
  log_tail_ = readable_log_tail_ = lsn;
}
} // end namespace palf
} // end namespace oceanbase

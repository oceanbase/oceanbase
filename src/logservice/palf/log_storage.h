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

#ifndef OCEANBASE_LOGSERVICE_LOG_STORAGE_
#define OCEANBASE_LOGSERVICE_LOG_STORAGE_

#include "lib/lock/ob_spin_lock.h" // ObSpinLock
#include "share/ob_errno.h"        // errno
#include "log_block_header.h"      // LogBlockHeader
#include "log_block_mgr.h"         // LogBlockMgr
#include "log_reader.h"            // LogReader
#include "log_storage_interface.h" // ILogStorage
#include "log_writer_utils.h"      // LogWriteBuf
#include "lsn.h"                   // LSN
#include "palf_iterator.h"         // PalfIteraor
#include "palf_callback_wrapper.h"

namespace oceanbase
{
namespace common
{
class ObILogAllocator;
}
namespace share
{
class SCN;
}
namespace palf
{
class ReadBuf;
class LogHotCache;
class LogStorage : public ILogStorage
{
public:
  using UpdateManifestCallback = ObFunction<int(const block_id_t, const bool in_restart)>;
  LogStorage();
  ~LogStorage();
  int init(const char *log_dir,
           const char *sub_dir,
           const LSN &base_lsn,
           const int64_t palf_id,
           const int64_t logical_block_size,
           const int64_t align_size,
           const int64_t align_buf_size,
           const UpdateManifestCallback &update_manifest_cb,
           ILogBlockPool *log_block_pool,
           LogPlugins *plugins,
           LogHotCache *hot_cache);

  template <class EntryHeaderType>
  int load(const char *log_dir,
           const char *sub_dir,
           const LSN &base_lsn,
           const int64_t palf_id,
           const int64_t logical_block_size,
           const int64_t align_size,
           const int64_t align_buf_size,
           const UpdateManifestCallback &update_manifest_cb,
           ILogBlockPool *log_block_pool,
           LogPlugins *plugins,
           LogHotCache *hot_cache,
           EntryHeaderType &entry_header,
           LSN &lsn);

  int load_manifest_for_meta_storage(block_id_t &expected_next_block_id);
  void destroy();

  int writev(const LSNArray &lsn_array, const LogWriteBufArray &write_buf_array, const SCNArray &scn_array);
  int writev(const LSN &lsn, const LogWriteBuf &write_buf, const share::SCN &scn);

  int append_meta(const char *buf, const int64_t buf_len);

  // @retval
  //   OB_SUCCESS
  //   OB_INVALID_ARGUMENT
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //   OB_ERR_UNEXPECTED, file maybe deleted by human.
  int pread(const LSN &lsn,
            const int64_t in_read_size,
            ReadBuf &read_buf,
            int64_t &out_read_size) final;

  int pread_without_block_header(const LSN &read_lsn,
                                 const int64_t in_read_size,
                                 ReadBuf &read_buf,
                                 int64_t &out_read_size);

  int truncate(const LSN &lsn);
  int truncate_prefix_blocks(const LSN &lsn);

  int begin_flashback(const LSN &start_lsn_of_block);
  int end_flashback(const LSN &start_lsn_of_block);

  int delete_block(const block_id_t &block_id);
  int get_block_id_range(block_id_t &min_block_id, block_id_t &max_block_id) const;
  // @retval
  //   OB_SUCCESS
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //   OB_ERR_OUT_OF_UPPER_BOUND, 'block_id' is the active block, and there is no data in this
  //   block. OB_ERR_UNEXPECTED, file maybe deleted by human. OB_INVALID_DATA, data has been
  //   corrupted
  int get_block_min_scn(const block_id_t &block_id, share::SCN &min_scn) const;
  const LSN get_begin_lsn() const;
  const LSN get_end_lsn() const;

  int update_manifest_used_for_meta_storage(const block_id_t expected_max_block_id);

  int get_logical_block_size(int64_t &logical_block_size) const;

  TO_STRING_KV(K_(log_tail),
               K_(readable_log_tail),
               K_(log_block_header),
               K_(block_mgr),
               K(logical_block_size_),
               K(curr_block_writable_size_),
               KP(block_header_serialize_buf_),
               K_(flashback_version));

private:
  int do_init_(const char *log_dir,
               const char *sub_dir,
               const LSN &base_lsn,
               const int64_t palf_id,
               const int64_t logical_block_size,
               const int64_t align_size,
               const int64_t align_buf_size,
               const UpdateManifestCallback &update_manifest_cb,
               ILogBlockPool *log_block_pool,
               LogPlugins *plugins,
               LogHotCache *hot_cache);
  // @ret val:
  //   OB_SUCCESS
  //   OB_ERR_OUT_OF_LOWER_BOUND
  //      the block has been recycled.
  //   OB_ERR_OUT_OF_UPPER_BOUND
  //      in flashback, (flashback_block_id, max_block_id] may be deleted, however, fetch log may read
  //      some blocks in range of (flashback_block_id, max_block_id].
  //   OB_NEED_RETRY, open the block need to be flashbacked failed or there is flashbacking during read data.
  //   OB_ERR_UNEXPECTED
  int check_read_out_of_bound_(const block_id_t &block_id,
                               const int64_t flashback_version,
                               const bool no_such_block) const;
  int inner_switch_block_();
  int append_block_header_used_for_meta_storage_();
  int append_block_header_(const LSN &block_min_lsn, const share::SCN &block_min_scn);
  int update_block_header_(const block_id_t block_id, const LSN &block_min_lsn, const share::SCN &block_min_scn);
  bool need_switch_block_() const;
  // 1. 使用group_entry_iterator迭代最后一个文件, 重建block_header;
  // 2. 从iterator中获取终点信息；
  // 3. 初始化dio_aligned_buf.
  template <class EntryHeaderType>
  int locate_log_tail_and_last_valid_entry_header_(const block_id_t min_block_id,
                                                   const block_id_t max_block_id,
                                                   EntryHeaderType &entry_header,
                                                   LSN &lsn);
  int load_last_block_(const block_id_t min_block_id, const block_id_t max_block_id);
  int inner_truncate_(const LSN &lsn);
  void truncate_block_header_(const LSN &lsn);

  void update_log_tail_guarded_by_lock_(const int64_t log_size);
  void update_log_tail_guarded_by_lock_(const LSN &lsn);
  const LSN &get_log_tail_guarded_by_lock_() const;
  void get_readable_log_tail_guarded_by_lock_(LSN &readable_log_tail,
                                              int64_t &flashback_version) const;
  offset_t get_phy_offset_(const LSN &lsn) const;
  int read_block_header_(const block_id_t block_id, LogBlockHeader &block_header) const;
  bool check_last_block_is_full_(const block_id_t max_block_id) const;
  int delete_prev_block_for_meta_();
  int inner_pread_(const LSN &read_lsn,
                   const int64_t in_read_size,
                   const bool need_read_block_header,
                   ReadBuf &read_buf,
                   int64_t &out_read_size);
  void reset_log_tail_for_last_block_(const LSN &lsn, bool last_block_exist);
  int update_manifest_(const block_id_t expected_next_block_id, const bool in_restart = false);
  int check_read_integrity_(const block_id_t &block_id);
private:
  // Used to perform IO tasks in the background
  LogBlockMgr block_mgr_;
  LogReader log_reader_;
  LSN log_tail_;
  // always same as 'log_tail_' except in process of flashback.
  LSN readable_log_tail_;
  LogBlockHeader log_block_header_;
  // Used to detemine whether need switch block.
  int64_t curr_block_writable_size_;
  // Whether need to append block header;
  bool need_append_block_header_;
  int64_t palf_id_;
  int64_t logical_block_size_;
  // used to protect log_tail_ and log_block_header_
  mutable ObSpinLock tail_info_lock_;
  mutable ObSpinLock delete_block_lock_;
  UpdateManifestCallback update_manifest_cb_;
  LogPlugins *plugins_;
  char block_header_serialize_buf_[MAX_INFO_BLOCK_SIZE];
  LogHotCache *hot_cache_;
  int64_t last_accum_read_statistic_time_;
  int64_t accum_read_io_count_;
  int64_t accum_read_log_size_;
  int64_t accum_read_cost_ts_;
  int64_t flashback_version_;
  bool is_inited_;
};

// Iterate last non-empty block:
// 1. set 'log_tail_' to the end of last valid entry;
// 2. 'load_last_block_':
//    1. reload last not aligned data;
//    2. set 'curr_block_writable_size_';
//    2. set 'need_append_block_header_' to true if 'curr_block_writable_size_' is 0.
template <class EntryHeaderType>
int LogStorage::load(const char *base_dir,
                     const char *sub_dir,
                     const LSN &base_lsn,
                     const int64_t palf_id,
                     const int64_t logical_block_size,
                     const int64_t align_size,
                     const int64_t align_buf_size,
                     const UpdateManifestCallback &update_manifest_cb,
                     ILogBlockPool *log_block_pool,
                     LogPlugins *plugins,
                     LogHotCache *hot_cache,
                     EntryHeaderType &entry_header,
                     LSN &lsn)
{
  int ret = OB_SUCCESS;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  lsn.reset();
  entry_header.reset();
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
                              log_block_pool,
                              plugins,
                              hot_cache))) {
    PALF_LOG(WARN, "LogStorage do_init_ failed", K(ret), K(base_dir), K(sub_dir), K(palf_id));
    // NB: if there is no valid data on disk, no need to load last block
  } else if (OB_FAIL(block_mgr_.get_block_id_range(min_block_id, max_block_id))
             && OB_ENTRY_NOT_EXIST != ret) {
    PALF_LOG(WARN, "get_block_id_range failed", KR(ret), KPC(this));
  } else {
    // If there is no block, reinit LogStorage
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      PALF_LOG(
          INFO, "there is no block on disk", K(ret), K(min_block_id), K(max_block_id), KPC(this));
    } else if (OB_FAIL(locate_log_tail_and_last_valid_entry_header_(
                   min_block_id, max_block_id, entry_header, lsn))) {
      PALF_LOG(WARN, "locate_log_tail_and_last_valid_entry_header_ failed", KR(ret), KPC(this));
    } else if (OB_FAIL(load_last_block_(min_block_id, max_block_id))) {
      PALF_LOG(WARN, "load_last_block_ failed", KR(ret), KPC(this), K(entry_header), K(lsn));
    } else {
    }
    PALF_LOG(INFO, "LogStorage load finish", KR(ret), KPC(this), K(min_block_id), K(max_block_id));
  }
  return ret;
}

template <class EntryHeaderType>
int LogStorage::locate_log_tail_and_last_valid_entry_header_(const block_id_t min_block_id,
                                                             const block_id_t max_block_id,
                                                             EntryHeaderType &entry_header,
                                                             LSN &lsn)
{
  int ret = OB_SUCCESS;
  using EntryType = typename EntryHeaderType::ENTRYTYPE;
  block_id_t iterate_block_id = max_block_id;
  update_log_tail_guarded_by_lock_(LSN((max_block_id + 1) * logical_block_size_));
  // the last block may has not valid data, we need iterate prev block
  // for GC, we must ensure that the block which include 'max_committed_lsn' will no be reused
  const bool need_print_error = false;
  while (OB_SUCC(ret) && true == is_valid_block_id(iterate_block_id)
         && iterate_block_id >= min_block_id) {
    // NB: 'log_tail_' need point to the tail of 'iterate_block_id', because 'pread' interface
    //      check whether iterating to the end of redo log block depends on this field.
    log_block_header_.reset();
    log_block_header_.update_palf_id_and_curr_block_id(palf_id_, iterate_block_id);
    PalfIterator<DiskIteratorStorage, EntryType> iterator;
    auto get_file_end_lsn = []() { return LSN(LOG_MAX_LSN_VAL); };
    LSN start_lsn(iterate_block_id * logical_block_size_);
    if (OB_FAIL(iterator.init(start_lsn, get_file_end_lsn, this))) {
      PALF_LOG(WARN, "PalfGroupBufferIterator init failed", K(ret), K(start_lsn));
    } else {
      iterator.set_need_print_error(need_print_error);
      EntryType curr_entry;
      LSN curr_lsn;
      while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
        if (OB_FAIL(iterator.get_entry(curr_entry, curr_lsn))) {
          PALF_LOG(WARN, "get entry failed", K(ret));
        } else {
          entry_header = curr_entry.get_header();
          lsn = curr_lsn;
        }
      }
      if (OB_ITER_END == ret
          || ((OB_CHECKSUM_ERROR == ret || OB_INVALID_DATA == ret) && true == iterator.check_is_the_last_entry())) {
        ret = OB_SUCCESS;
        // NB: lsn is valid when there are some valid data in last block, otherwise, we need
        // iterate prev block.
        if (true == lsn.is_valid()) {
          update_log_tail_guarded_by_lock_(lsn + entry_header.get_data_len() + entry_header.get_serialize_size());
          break;
        } else {
          PALF_LOG(INFO,
                   "this block is empty, has no data, need iterate prev block",
                   K(ret),
                   K(iterate_block_id));
          // NB: nowdays, each block' will be full of data.
          update_log_tail_guarded_by_lock_(LSN(iterate_block_id * logical_block_size_));
          iterate_block_id--;
        }
      } else {
        PALF_LOG(ERROR,
                 "locate_log_tail_and_last_valid_entry_header_ failed",
                 K(ret),
                 K(curr_entry),
                 K(iterator));
      }
    }
  }
  // defense code
  // Nowdays, we only create block when prev block is full of data.
  if (iterate_block_id != max_block_id && log_tail_ != LSN(max_block_id * logical_block_size_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR,
             "unexpected error, the last block is empty but its' prev block is not full",
             K(ret),
             K(iterate_block_id),
             K(max_block_id));
  }
  if (OB_SUCC(ret)) {
    PALF_LOG(INFO,
             "locate_log_tail_and_last_valid_entry_header_ success",
             K(ret),
             K(log_tail_),
             KPC(this));
  }

  return ret;
}
} // end namespace palf
} // end namespace oceanbase
#endif

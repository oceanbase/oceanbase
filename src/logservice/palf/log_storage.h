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
#include "log_async_io_struct.h"   // AsyncPwriteRequest
#include "log_reader.h"            // LogReader
#include "log_storage_interface.h" // ILogStorage
#include "log_writer_utils.h"      // LogWriteBuf
#include "lsn.h"                   // LSN
#include "palf_iterator.h"         // PalfIteraor
#include "palf_callback_wrapper.h"
#include "log_cache.h"

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
class LogCache;
class LogIOContext;
class IAsyncPalfIOCtx;
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
           LogCache *log_cache,
           LogIOAdapter *io_adapter);

  // 加载磁盘 block 并重建逻辑 tail。enable_async_recovery 仅允许 redo storage
  // 修复最后一个 block 中、最后一条完整日志之后的异步未发布脏尾；关闭时保持同步恢复语义。
  // entry_header 和 lsn 返回最后一条完整 entry 的 header 与起始 LSN。
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
           LogCache *log_cache,
           LogIOAdapter *io_adapter,
           EntryHeaderType &entry_header,
           LSN &lsn,
           const bool enable_async_recovery);

  int load_manifest_for_meta_storage(block_id_t &expected_next_block_id);

  void destroy();

  int writev(const LSNArray &lsn_array, const LogWriteBufArray &write_buf_array, const SCNArray &scn_array);
  int writev(const LSN &lsn, const LogWriteBuf &write_buf, const share::SCN &scn);

  // ======================== Async write API ========================
  //
  // 异步写把原 writev 契约拆成四步：planner 在块边界先准备新块和块头；随后
  // 提交物理 AIO；ObIOManager 回调线程把完成事件直接交给所属 ctx；ctx 按
  // LSN 连续顺序调用 commit_async_append 发布完成区间。
  //
  // async_pwrite 是底层物理 AIO 接口。aligned_begin_lsn、aligned_buf_len 和
  // buffer 地址都必须按 LOG_DIO_ALIGN_SIZE 对齐，写入不能跨块；字节级的
  // fragment 范围由 AsyncPalfIOCtx 维护，提交成功不会推进已发布 tail。
  int async_pwrite(const AsyncPwriteRequest &req,
                   common::ObIOHandle &out_handle);

  // commit_async_append 要求 begin_lsn 等于当前 log_tail_；成功后把 log_tail_
  // 推进到 end_lsn，单调推进 readable_log_tail_，并刷新当前块剩余空间。
  // 该接口不切块也不写块头，这些操作由写入侧在提交首笔 AIO 前完成。
  int commit_async_append(const LSN &begin_lsn, const LSN &end_lsn);

  // prepare_async_block_for_write 仅在 log_tail_ 位于块边界时使用。它以
  // log_tail_ 作为新块最小 LSN，按同步写路径的规则切换或打开块并写入块头。
  int prepare_async_block_for_write(const share::SCN &new_block_min_scn);
  // 通过普通 PALF pread 从指定的 DIO 对齐 LSN 读取一页。buf 必须非空且
  // buf_len 必须等于 LOG_DIO_ALIGN_SIZE；接口不会确认该 LSN 就是当前 tail。
  int read_log_storage_tail_page(const LSN &page_begin_lsn,
                                 char *buf,
                                 const int64_t buf_len,
                                 int64_t &read_size);

  // 异步 planner 使用的存储状态：log_tail 是已发布尾部；
  // curr_block_writable_size 是当前块剩余逻辑空间，0 表示位于块边界；
  // need_append_block_header 表示当前块头仍需写入。
  struct AsyncStorageSnapshot {
    LSN log_tail;
    int64_t curr_block_writable_size;
    bool need_append_block_header;

    TO_STRING_KV(K(log_tail), K(curr_block_writable_size),
                 K(need_append_block_header));
  };
  // Copy the published tail and block state while holding tail_info_lock_.
  void get_async_storage_snapshot(AsyncStorageSnapshot &out) const;
  // ===========================================================================

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
            int64_t &out_read_size,
            LogIOContext &io_ctx) final;

  int pread_with_block_header(const LSN &read_lsn,
                              const int64_t in_read_size,
                              ReadBuf &read_buf,
                              int64_t &out_read_size,
                              LogIOContext &io_ctx);

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  inline const libpalf::LibPalfIteratorMemoryStorageFFI * get_memory_storage() { return nullptr; }
#endif
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

  LogReader *get_log_reader();
  int fill_cache_when_slide(const LSN &begin_lsn, const int64_t size);

  int get_io_statistic_info(int64_t &last_working_time,
                            int64_t &last_write_size,
                            int64_t &accum_write_size,
                            int64_t &accum_write_count,
                            int64_t &accum_write_rt) const;
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
               LogCache *log_cache,
               LogIOAdapter *io_adapter);
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
  int prepare_block_for_write_(const LSN &block_min_lsn, const share::SCN &block_min_scn);
  bool need_switch_block_() const;
  // 从最后一个 block 向前定位最后一个非空 block，并在 block 内从前向后解析日志：
  // 1. lsn 返回最后一条完整 entry 的起始 LSN，log_tail_ 指向该 entry 结束位置；
  // 2. allow_mid_log_hole 为 true 时，仅允许最后一个 block 在连续 tail 后存在异步脏尾；
  // 3. hole_tail 返回需要截断的位置，无可恢复脏尾时保持 invalid。
  template <class EntryHeaderType>
  int locate_log_tail_and_last_valid_entry_header_(const block_id_t min_block_id,
                                                   const block_id_t max_block_id,
                                                   EntryHeaderType &entry_header,
                                                   LSN &lsn,
                                                   const bool allow_mid_log_hole,
                                                   LSN &hole_tail);
  template <class EntryType>
  bool is_tail_locate_result_acceptable_(const int iter_ret,
                                         const block_id_t iterate_block_id,
                                         const block_id_t max_block_id,
                                         const bool allow_mid_log_hole,
                                         const bool has_valid_entry,
                                         PalfIterator<EntryType> &iterator,
                                         bool &is_async_dirty_suffix_candidate) const;
  // 比较连续逻辑 tail 与最后一个 block 的物理非零 tail，诊断是否存在可恢复异步脏尾。
  int detect_last_block_mid_log_hole_(const block_id_t max_block_id,
                                      const LSN &contiguous_tail_lsn,
                                      LSN &hole_tail);
  // 从 block 末尾反向查找最后一个非零字节，返回其后一位 LSN；全零时返回 invalid。
  int locate_last_nonzero_tail_lsn_in_block_(const block_id_t block_id,
                                             LSN &last_nonzero_tail_lsn);
  // 将最后一个 block 截断到已确认的连续 tail，并保留该 block 供后续追加。
  int truncate_async_recovery_tail_(const LSN &restart_tail_lsn,
                                    const block_id_t max_block_id);
  int load_last_block_(const block_id_t min_block_id, const block_id_t max_block_id);
  int inner_truncate_(const LSN &lsn);
  void truncate_block_header_(const LSN &lsn);

  void update_log_tail_guarded_by_lock_(const int64_t log_size);
  void update_log_tail_guarded_by_lock_(const LSN &lsn);
  const LSN &get_log_tail_guarded_by_lock_() const;
  void get_readable_log_tail_guarded_by_lock_(LSN &readable_log_tail,
                                              int64_t &flashback_version) const;
  void get_flashback_version_guarded_by_lock_(int64_t &flashback_version) const;
  offset_t get_phy_offset_(const LSN &lsn) const;
  int read_block_header_(const block_id_t block_id, LogBlockHeader &block_header) const;
  bool check_last_block_is_full_(const block_id_t max_block_id) const;
  int delete_prev_block_for_meta_();
  int inner_pread_(const LSN &read_lsn,
                   const int64_t in_read_size,
                   const bool need_read_block_header,
                   ReadBuf &read_buf,
                   int64_t &out_read_size,
                   LogIOContext &io_ctx);
  void reset_log_tail_for_last_block_(const LSN &lsn, bool last_block_exist);
  int update_manifest_(const block_id_t expected_next_block_id, const bool in_restart = false);
  int check_read_integrity_(const block_id_t &block_id);
  bool is_log_cache_inited_();
  bool check_in_flashback_(const int64_t flashback_version) const;

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
  LogCache *log_cache_;
  int64_t flashback_version_;
  bool is_inited_;
};

// load 的恢复流程：
// 1. 定位最后一条完整 entry，并将 log_tail_ 设置到其结束位置；
// 2. 异步模式发现可恢复脏尾时先截断，再关闭 hole 容忍重新定位，验证截断结果；
// 3. 加载最后一个 block 的非对齐尾页，恢复 curr_block_writable_size_ 和 block header 状态。
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
                     LogCache *log_cache,
                     LogIOAdapter *io_adapter,
                     EntryHeaderType &entry_header,
                     LSN &lsn,
                     const bool enable_async_recovery)
{
  int ret = OB_SUCCESS;
  block_id_t min_block_id = LOG_INVALID_BLOCK_ID;
  block_id_t max_block_id = LOG_INVALID_BLOCK_ID;
  LSN restart_hole_truncate_lsn;
  LSN unused_hole_lsn;
  bool need_truncate_restart_tail = false;
  lsn.reset();
  entry_header.reset();
  restart_hole_truncate_lsn.reset();
  unused_hole_lsn.reset();
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
                              log_cache,
                              io_adapter))) {
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
    } else if (OB_FAIL(locate_log_tail_and_last_valid_entry_header_(min_block_id,
                                                                    max_block_id,
                                                                    entry_header,
                                                                    lsn,
                                                                    enable_async_recovery,
                                                                    restart_hole_truncate_lsn))) {
      PALF_LOG(WARN, "locate_log_tail_and_last_valid_entry_header_ failed", KR(ret), KPC(this));
    } else if (FALSE_IT(need_truncate_restart_tail = restart_hole_truncate_lsn.is_valid())) {
    } else if (!need_truncate_restart_tail
               && OB_FAIL(load_last_block_(min_block_id, max_block_id))) {
      PALF_LOG(WARN, "load_last_block_ failed", KR(ret), KPC(this), K(entry_header), K(lsn));
    } else if (!need_truncate_restart_tail) {
    } else {
      // 第一次定位允许识别异步脏尾；截断后必须按同步规则重新扫描，确保磁盘已经成为
      // sync/async 都能正常解析的连续日志。
      if (OB_FAIL(truncate_async_recovery_tail_(restart_hole_truncate_lsn, max_block_id))) {
        PALF_LOG(ERROR, "truncate async recovery tail failed",
                 KR(ret), K(restart_hole_truncate_lsn), K(max_block_id), KPC(this));
      } else if (OB_FAIL(block_mgr_.get_block_id_range(min_block_id, max_block_id))) {
        PALF_LOG(WARN, "get block range after async recovery truncate failed",
                 KR(ret), K(restart_hole_truncate_lsn), KPC(this));
      } else if (OB_FAIL(locate_log_tail_and_last_valid_entry_header_(min_block_id,
                                                                      max_block_id,
                                                                      entry_header,
                                                                      lsn,
                                                                      false /* allow_mid_log_hole */,
                                                                      unused_hole_lsn))) {
        PALF_LOG(WARN, "relocate tail after mid-log hole truncate failed",
                 KR(ret), K(restart_hole_truncate_lsn), KPC(this));
      } else if (OB_FAIL(load_last_block_(min_block_id, max_block_id))) {
        PALF_LOG(WARN, "reload last block after mid-log hole truncate failed",
                 KR(ret), KPC(this), K(entry_header), K(lsn));
      } else {
        PALF_LOG(INFO, "async-write crash recovery: truncated storage to restart tail",
                 K(restart_hole_truncate_lsn), K(entry_header), K(lsn), KPC(this));
      }
    }
    PALF_LOG(INFO, "LogStorage load finish", KR(ret), KPC(this), K(min_block_id), K(max_block_id));
  }
  return ret;
}

template <class EntryType>
bool LogStorage::is_tail_locate_result_acceptable_(const int iter_ret,
                                                   const block_id_t iterate_block_id,
                                                   const block_id_t max_block_id,
                                                   const bool allow_mid_log_hole,
                                                   const bool has_valid_entry,
                                                   PalfIterator<EntryType> &iterator,
                                                   bool &is_async_dirty_suffix_candidate) const
{
  bool bool_ret = false;
  const bool is_partial_log_after_valid_entry = OB_PARTIAL_LOG == iter_ret && has_valid_entry;
  is_async_dirty_suffix_candidate = false;
  // 正常迭代到 block 末尾时直接接受；异步恢复只放宽最后一个 block 的脏尾错误。
  if (OB_ITER_END == iter_ret) {
    bool_ret = true;
  } else if (allow_mid_log_hole
             && iterate_block_id == max_block_id
             // group iterator 在完整 group 后读到裸 LogEntryHeader 时返回 OB_PARTIAL_LOG。
             // 异步 DIO 写对齐尾页时可能把该未发布内容一并落盘，但 block 内必须已有完整 entry。
             && (OB_CHECKSUM_ERROR == iter_ret || OB_INVALID_DATA == iter_ret
                 || is_partial_log_after_valid_entry)) {
    is_async_dirty_suffix_candidate = true;
    bool_ret = true;
  } else if (!allow_mid_log_hole
             // 同步恢复保留原有语义：错误只有位于 block 最后一条物理 entry 时才接受。
             && (OB_CHECKSUM_ERROR == iter_ret || OB_INVALID_DATA == iter_ret)
             && true == iterator.check_is_the_last_entry()) {
    bool_ret = true;
  } else {
  }
  return bool_ret;
}

template <class EntryHeaderType>
int LogStorage::locate_log_tail_and_last_valid_entry_header_(const block_id_t min_block_id,
                                                             const block_id_t max_block_id,
                                                             EntryHeaderType &entry_header,
                                                             LSN &lsn,
                                                             const bool allow_mid_log_hole,
                                                             LSN &hole_tail)
{
  int ret = OB_SUCCESS;
  using EntryType = typename EntryHeaderType::ENTRYTYPE;
  block_id_t iterate_block_id = max_block_id;
  hole_tail.reset();
  update_log_tail_guarded_by_lock_(LSN((max_block_id + 1) * logical_block_size_));
  // 最后一个 block 可能只有 header 或全零数据，因此从 max_block_id 向前找到最后一个非空 block。
  // 扫描期间 log_tail_ 始终覆盖当前 block，避免 pread 的上界检查提前截断 iterator。
  while (OB_SUCC(ret) && true == is_valid_block_id(iterate_block_id)
         && iterate_block_id >= min_block_id) {
    log_block_header_.reset();
    log_block_header_.update_palf_id_and_curr_block_id(palf_id_, iterate_block_id);
    PalfIterator<EntryType> iterator;
    auto get_file_end_lsn = []() { return LSN(LOG_MAX_LSN_VAL); };
    LSN start_lsn(iterate_block_id * logical_block_size_);
    if (OB_FAIL(iterator.init(start_lsn, get_file_end_lsn, this))) {
      PALF_LOG(WARN, "PalfGroupBufferIterator init failed", K(ret), K(start_lsn));
    } else if (OB_FAIL(iterator.set_io_context(palf::LogIOContext(MTL_ID(), palf_id_, palf::LogIOUser::RESTART)))) {
      PALF_LOG(WARN, "set_io_context failed", K(ret), K(start_lsn));
    } else {
      const bool need_print_error = false;
      iterator.set_need_print_error(need_print_error);
      EntryType curr_entry;
      LSN curr_lsn;
      EntryHeaderType block_entry_header;
      LSN block_lsn;
      block_entry_header.reset();
      block_lsn.reset();
      // block 内始终从起点向后解析，block_lsn/block_entry_header 只保存最后一条完整 entry。
      while (OB_SUCC(ret) && OB_SUCC(iterator.next())) {
        if (OB_FAIL(iterator.get_entry(curr_entry, curr_lsn))) {
          PALF_LOG(WARN, "get entry failed", K(ret));
        } else {
          block_entry_header = curr_entry.get_header();
          block_lsn = curr_lsn;
        }
      }
      bool is_async_dirty_suffix_candidate = false;
      if (is_tail_locate_result_acceptable_(ret,
                                            iterate_block_id,
                                            max_block_id,
                                            allow_mid_log_hole,
                                            block_lsn.is_valid(),
                                            iterator,
                                            is_async_dirty_suffix_candidate)) {
        ret = OB_SUCCESS;
        // block_lsn 有效表示当前 block 至少包含一条完整 entry；否则继续检查前一个 block。
        if (true == block_lsn.is_valid()) {
          entry_header = block_entry_header;
          lsn = block_lsn;
          update_log_tail_guarded_by_lock_(lsn + entry_header.get_data_len() + entry_header.get_serialize_size());
          if (is_async_dirty_suffix_candidate) {
            // 此时 log_tail_ 是最后一条完整 entry 的结束位置，也是候选截断点。
            hole_tail = log_tail_;
            PALF_LOG(WARN, "async-write crash recovery: recoverable dirty suffix found",
                     K(palf_id_), K(iterate_block_id), K(max_block_id), K(hole_tail),
                     K(entry_header), K(lsn));
          }
          break;
        } else {
          PALF_LOG(INFO,
                   "this block is empty, has no data, need iterate prev block",
                   K(ret),
                   K(iterate_block_id));
          // 正常切 block 前一个 block 必须已写满，空 block 的起点就是当前连续 tail。
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
  // 防御检查：正常流程只有前一个 block 写满后才会创建新 block，因此异步未发布脏尾只能
  // 出现在最后一个 block；如果最后 block 为空，前一个 block 也必须正好写满到其边界。
  const bool has_async_recovery_tail = allow_mid_log_hole && hole_tail.is_valid();
  if (has_async_recovery_tail && iterate_block_id != max_block_id) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR,
             "unexpected async recovery tail outside last block",
             K(ret),
             K(iterate_block_id),
             K(max_block_id),
             K(hole_tail));
  } else if (!has_async_recovery_tail
             && iterate_block_id != max_block_id
             && log_tail_ != LSN(max_block_id * logical_block_size_)) {
    ret = OB_ERR_UNEXPECTED;
    PALF_LOG(ERROR,
             "unexpected error, the last block is empty but its' prev block is not full",
             K(ret),
             K(iterate_block_id),
             K(max_block_id));
  }
  // iterator 只能给出首个解析失败位置，不能证明其后没有更远的非零数据。因此异步恢复
  // 必须继续扫描物理尾；即使已经找到候选 hole，也要校验全部脏尾不超过一个 40MB group buffer。
  const LSN contiguous_tail_lsn = hole_tail.is_valid() ? hole_tail : log_tail_;
  if (OB_SUCC(ret) && allow_mid_log_hole && contiguous_tail_lsn.is_valid()
      && contiguous_tail_lsn >= LSN(max_block_id * logical_block_size_)
      && contiguous_tail_lsn <= LSN((max_block_id + 1) * logical_block_size_)) {
    LSN physical_hole_tail;
    if (OB_FAIL(detect_last_block_mid_log_hole_(max_block_id, contiguous_tail_lsn, physical_hole_tail))) {
      PALF_LOG(ERROR, "detect_last_block_mid_log_hole_ failed",
               K(ret), K(hole_tail), K(physical_hole_tail), K(contiguous_tail_lsn), K(allow_mid_log_hole),
               K(min_block_id), K(max_block_id), KPC(this));
    } else if (physical_hole_tail.is_valid()) {
      // detect 返回的 hole tail 等于连续逻辑 tail；赋回用于统一触发后续截断流程。
      hole_tail = physical_hole_tail;
    }
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

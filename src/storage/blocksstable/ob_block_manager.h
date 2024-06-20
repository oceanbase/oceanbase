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

#ifndef OCEANBASE_BLOCKSSTABLE_OB_BLOCK_MANAGER_H_
#define OCEANBASE_BLOCKSSTABLE_OB_BLOCK_MANAGER_H_

#include "common/storage/ob_io_device.h"
#include "share/io/ob_io_struct.h"
#include "lib/atomic/ob_atomic.h"
#include "lib/hash/ob_hashset.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "lib/lock/ob_bucket_lock.h"
#include "share/ob_thread_mgr.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/blocksstable/ob_macro_block_checker.h"
#include "storage/blocksstable/ob_super_block_buffer_holder.h"
#include "storage/ob_super_block_struct.h"
#include "storage/tablet/ob_tablet_block_aggregated_info.h"

namespace oceanbase
{

namespace storage
{
class ObTenantCheckpointSlogHandler;
class ObTabletHandle;
struct ObTabletBlockInfo;
}

namespace blocksstable
{

class ObMacroBlockHandle;
struct ObMacroBlocksWriteCtx;

class ObSuperBlockPreadChecker : public common::ObIODPreadChecker
{
public:
  ObSuperBlockPreadChecker() : super_block_() {};
  virtual ~ObSuperBlockPreadChecker() = default;

  virtual int do_check(void *read_buf, const int64_t read_size) override;
  OB_INLINE const storage::ObServerSuperBlock& get_super_block() const { return super_block_; }

private:
  storage::ObServerSuperBlock super_block_;
  DISALLOW_COPY_AND_ASSIGN(ObSuperBlockPreadChecker);
};

struct ObMacroBlockInfo final
{
public:
  ObMacroBlockInfo() : ref_cnt_(0),  is_free_(false), access_time_(0) {}
  ~ObMacroBlockInfo() = default;
  TO_STRING_KV(K_(ref_cnt), K_(is_free), K_(access_time));
public:
  int32_t ref_cnt_;
  bool is_free_;
  int64_t access_time_;
};

struct ObBadBlockInfo final
{
public:
  ObBadBlockInfo() { reset(); }
  ~ObBadBlockInfo() {}
  OB_INLINE void reset() { MEMSET(this, 0, sizeof(*this)); }
  TO_STRING_KV(
      K(disk_id_),
      K(macro_block_id_),
      K(error_type_),
      K(store_file_path_),
      K(error_msg_),
      K(check_time_));

public:
  int64_t disk_id_;
  MacroBlockId macro_block_id_;
  int64_t error_type_;
  char store_file_path_[MAX_PATH_SIZE];
  char error_msg_[common::OB_MAX_ERROR_MSG_LEN];
  int64_t check_time_;
};

struct ObMacroBlockWriteInfo final
{
public:
  ObMacroBlockWriteInfo()
    : buffer_(NULL), offset_(0), size_(0), io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS), io_desc_(), io_callback_(NULL)
  {}
  ~ObMacroBlockWriteInfo() = default;
  OB_INLINE bool is_valid() const
  {
    return io_desc_.is_valid() && NULL != buffer_ && offset_ >= 0 && size_ > 0 && io_timeout_ms_ > 0;
  }
  TO_STRING_KV(KP_(buffer), K_(offset), K_(size), K_(io_timeout_ms), K_(io_desc), KP_(io_callback));
public:
  const char *buffer_;
  int64_t offset_;
  int64_t size_;
  int64_t io_timeout_ms_;
  common::ObIOFlag io_desc_;
  common::ObIOCallback *io_callback_;
};

struct ObMacroBlockReadInfo final
{
public:
  ObMacroBlockReadInfo()
    : macro_block_id_(), offset_(), size_(), io_timeout_ms_(DEFAULT_IO_WAIT_TIME_MS),
    io_desc_(), io_callback_(NULL), buf_(NULL)
  {}
  ~ObMacroBlockReadInfo() = default;
  OB_INLINE bool is_valid() const
  {
    return macro_block_id_.is_valid() && offset_ >= 0 && size_ > 0
        && io_desc_.is_valid() && (nullptr != io_callback_ || nullptr != buf_);
  }
  TO_STRING_KV(K_(offset), K_(size), K_(io_timeout_ms), K_(io_desc), KP_(io_callback),
      KP_(buf), K_(macro_block_id));
public:
  blocksstable::MacroBlockId macro_block_id_;
  int64_t offset_;
  int64_t size_;
  int64_t io_timeout_ms_;
  common::ObIOFlag io_desc_;
  common::ObIOCallback *io_callback_;
  char *buf_;
};

class ObMacroBlockSeqGenerator final
{
public:
  // start to give an alarm when remaining 10T size to rewrite.
  static const int64_t BLOCK_SEQUENCE_WARNING_LINE = MacroBlockId::MAX_WRITE_SEQ - 5000000;
public:
  ObMacroBlockSeqGenerator();
  ~ObMacroBlockSeqGenerator();
  void reset();

  // thread safe
  int generate_next_sequence(uint64_t &blk_seq);
  OB_INLINE void update_sequence(const uint64_t blk_seq)
  {
    common::SpinWLockGuard guard(lock_);
    rewrite_seq_ = blk_seq;
  }
  TO_STRING_KV(K_(rewrite_seq));

private:
  uint64_t rewrite_seq_;
  common::SpinRWLock lock_;
};

class ObBlockManager
{
public:
  ObBlockManager();
  virtual ~ObBlockManager();

  int init(
      common::ObIODevice *io_device,
      const int64_t block_size);
  int start(const int64_t reserved_size);
  void stop();
  void wait();
  void destroy();

  // block interfaces
  int alloc_block(
      ObMacroBlockHandle &macro_handle);
  static int async_read_block(
      const ObMacroBlockReadInfo &read_info,
      ObMacroBlockHandle &macro_handle);
  static int async_write_block(
      const ObMacroBlockWriteInfo &write_info,
      ObMacroBlockHandle &macro_handle);
  static int write_block(
      const ObMacroBlockWriteInfo &write_info,
      ObMacroBlockHandle &macro_handle);
  static int read_block(
      const ObMacroBlockReadInfo &read_info,
      ObMacroBlockHandle &macro_handle);

  int read_super_block(storage::ObServerSuperBlock &super_block);
  int write_super_block(const storage::ObServerSuperBlock &super_block);
  OB_INLINE const storage::ObServerSuperBlock &get_server_super_block() const
  {
    return super_block_;
  }
  int update_super_block(const common::ObLogCursor &replay_start_point,
                         const blocksstable::MacroBlockId &tenant_meta_entry);

  int64_t get_macro_block_size() const;
  int64_t get_total_macro_block_count() const;
  int64_t get_free_macro_block_count() const;
  int64_t get_used_macro_block_count() const;
  int64_t get_max_macro_block_count(int64_t reserved_size) const;
  int get_all_macro_ids(ObArray<MacroBlockId> &ids_array);

  int check_macro_block_free(const MacroBlockId &macro_id, bool &is_free) const;
  int get_bad_block_infos(common::ObIArray<ObBadBlockInfo> &bad_block_infos);
  int report_bad_block(
      const MacroBlockId &macro_block_id,
      const int64_t error_type,
      const char *error_msg,
      const char *file_path);

  int resize_file(
      const int64_t new_data_file_size,
      const int64_t new_data_file_disk_percentage,
      const int64_t reserved_size);

  // reference count interfaces
  int inc_ref(const MacroBlockId &macro_id);
  int dec_ref(const MacroBlockId &macro_id);
  // If update_to_max_time is true, it means modify the last_write_time_ of the block to max,
  // which is used to skip the bad block inspection.
  int update_write_time(const MacroBlockId &macro_id, const bool update_to_max_time = false);

  // mark and sweep
  int get_marker_status(ObMacroBlockMarkerStatus &status);
  void mark_and_sweep();
  int first_mark_device();

  bool is_started() { return is_started_; }
private:
  struct BlockInfo
  {
    int64_t ref_cnt_;
    int64_t access_time_;
    int64_t last_write_time_;
    BlockInfo() : ref_cnt_(0), access_time_(0), last_write_time_(INT64_MAX) {}
    void reset()
    {
      ref_cnt_ = 0;
      access_time_ = 0;
      last_write_time_ = INT64_MAX;
    }
    TO_STRING_KV(K_(ref_cnt), K_(access_time), K_(last_write_time));
  };

  class GetAllMacroBlockIdFunctor final
  {
  public:
    GetAllMacroBlockIdFunctor(common::ObIArray<MacroBlockId> &block_ids)
      : ret_code_(common::OB_SUCCESS), block_ids_(block_ids) {}
    ~GetAllMacroBlockIdFunctor() = default;

    bool operator()(const MacroBlockId &key, const BlockInfo &value);
    int get_ret_code() const { return ret_code_; }

  private:
    int ret_code_;
    common::ObIArray<MacroBlockId> &block_ids_;
  };

private:
  static constexpr double MARK_THRESHOLD = 0.2;
  static const int64_t SUPER_BLOCK_OFFSET = 0;
  static const int64_t DEFAULT_LOCK_BUCKET_COUNT = 2048;
  static const int64_t DEFAULT_PENDING_FREE_COUNT = 1024;

  static const int64_t RECYCLE_DELAY_US = 30 * 1000 * 1000; // 30s
  static const int64_t INSPECT_DELAY_US = 1  * 1000 * 1000; // 1s
  static const int64_t AUTO_EXTEND_LEAST_FREE_BLOCK_CNT = 512; // 1G

  typedef common::ObLinearHashMap<MacroBlockId, BlockInfo> BlockMap;
  typedef common::ObLinearHashMap<MacroBlockId, bool> MacroBlkIdMap;

  class GetOldestHoldBlockFunctor final
  {
  public:
    GetOldestHoldBlockFunctor(
        common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &id_set,
        ObSimpleMacroBlockInfo &info)
      : ret_code_(common::OB_SUCCESS),
        macro_id_set_(id_set),
        oldest_hold_block_info_(info)
    {}
    ~GetOldestHoldBlockFunctor() = default;
    bool operator()(const MacroBlockId &key, const BlockInfo &value);
    int get_ret_code() const { return ret_code_; }
  private:
    int ret_code_;
    common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set_;
    ObSimpleMacroBlockInfo &oldest_hold_block_info_;
  };

  class GetPendingFreeBlockFunctor final
  {
  public:
    GetPendingFreeBlockFunctor(
        const int64_t max_free_blk_cnt,
        MacroBlkIdMap &blk_map,
        int64_t &hold_count)
      : ret_code_(common::OB_SUCCESS),
        max_free_blk_cnt_(max_free_blk_cnt),
        blk_map_(blk_map),
        hold_count_(hold_count)
    {}
    ~GetPendingFreeBlockFunctor() = default;

    bool operator()(const MacroBlockId &key, const BlockInfo &value);
    int get_ret_code() const { return ret_code_; }

  private:
    int ret_code_;
    int64_t max_free_blk_cnt_;
    MacroBlkIdMap &blk_map_;
    int64_t &hold_count_;
  };

  class DoBlockSweepFunctor final
  {
  public:
    DoBlockSweepFunctor(ObBlockManager& block_manager)
      : ret_code_(common::OB_SUCCESS),
        block_manager_(block_manager)
    {}
    ~DoBlockSweepFunctor() = default;

    bool operator()(const MacroBlockId &macro_id, const bool can_free);
    int get_ret_code() const { return ret_code_; }

  private:
    int ret_code_;
    ObBlockManager& block_manager_;
  };

private:
  void update_partial_status(const ObMacroBlockMarkerStatus &tmp_status);
  int get_macro_block_info(const MacroBlockId &macro_id,
                           ObMacroBlockInfo &macro_block_info,
                           ObMacroBlockHandle &macro_block_handle);
  bool is_bad_block(const MacroBlockId &macro_block_id);
  int mark_macro_blocks(
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_held_shared_block(
      const MacroBlockId &macro_id,
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_tenant_blocks(
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_tablet_block(
      MacroBlkIdMap &mark_info,
      storage::ObTabletHandle &handle,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int do_mark_tablet_block(
      const ObTabletBlockInfo &block_info,
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_sstable_blocks(
      MacroBlkIdMap &mark_info,
      storage::ObTabletHandle &handle,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_sstable_meta_block(
      const blocksstable::ObSSTable &sstable,
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_tablet_meta_blocks(
      MacroBlkIdMap &mark_info,
      storage::ObTabletHandle &handle,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_tenant_ckpt_blocks(
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      storage::ObTenantCheckpointSlogHandler &hdl,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_tmp_file_blocks(
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int mark_server_meta_blocks(
      MacroBlkIdMap &mark_info,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      ObMacroBlockMarkerStatus &tmp_status);
  int set_group_id(const uint64_t tenant_id);
  int do_sweep(MacroBlkIdMap &mark_info);
  int sweep_one_block(const MacroBlockId& macro_id);

  int update_mark_info(
      const common::ObIArray<MacroBlockId> &macro_block_list,
      common::hash::ObHashSet<MacroBlockId, common::hash::NoPthreadDefendMode> &macro_id_set,
      MacroBlkIdMap &mark_info);
  int update_mark_info(
      const MacroBlockId &macro_id,
      MacroBlkIdMap &mark_info);
  void update_marker_status(const ObMacroBlockMarkerStatus &tmp_status);
  void disable_mark_sweep() { ATOMIC_SET(&is_mark_sweep_enabled_, false); }
  void enable_mark_sweep() { ATOMIC_SET(&is_mark_sweep_enabled_, true); }
  bool is_mark_sweep_enabled() { return ATOMIC_LOAD(&is_mark_sweep_enabled_); }

  int  extend_file_size_if_need();
  bool check_can_be_extend(
      const int64_t reserved_size);

private:
  // not thread-safe, only for first mark device.
  class BlockMapIterator : public common::ObIBlockIterator
  {
  public:
    BlockMapIterator(BlockMap &blk_map) : iter_(blk_map), max_write_seq_(0) {}
    virtual ~BlockMapIterator() = default;

    int get_next_block(common::ObIOFd &block_id) override;
    OB_INLINE uint64_t get_max_write_sequence() const { return max_write_seq_; }

  private:
    BlockMap::BlurredIterator iter_;
    uint64_t max_write_seq_;
  };

  class MarkBlockTask : public common::ObTimerTask
  {
  public:
    MarkBlockTask(ObBlockManager &blk_mgr)
        : blk_mgr_(blk_mgr)
    {
      disable_timeout_check();
    }
    virtual ~MarkBlockTask() = default;
    virtual void runTimerTask() override;

  private:
    ObBlockManager &blk_mgr_;
  };

  class InspectBadBlockTask : public common::ObTimerTask
  {
  public:
    InspectBadBlockTask(ObBlockManager &blk_mgr);
    virtual ~InspectBadBlockTask();
    virtual void runTimerTask();
    void reset();

  private:
    int check_block(ObMacroBlockHandle &macro_block_handle);
    void inspect_bad_block();

  private:
    static const int64_t ACCESS_TIME_INTERVAL;
    static const int64_t MIN_OPEN_BLOCKS_PER_ROUND;
    static const int64_t MAX_SEARCH_COUNT_PER_ROUND;

    ObBlockManager &blk_mgr_;
    int64_t last_macro_idx_;
    int64_t last_check_time_;
  private:
    DISALLOW_COPY_AND_ASSIGN(InspectBadBlockTask);
  };

private:
  friend class InspectBadBlockTask;

  common::SpinRWLock lock_;
  ObBucketLock bucket_lock_;
  BlockMap block_map_;

  ObIOFd super_block_fd_;
  storage::ObServerSuperBlock super_block_; // read only memory cache
  ObSuperBlockBufferHolder super_block_buf_holder_;
  int64_t default_block_size_;
  ObMacroBlockMarkerStatus marker_status_;
  common::SpinRWLock marker_lock_;

  bool is_mark_sweep_enabled_;
  common::SpinRWLock sweep_lock_;

  MarkBlockTask mark_block_task_;
  InspectBadBlockTask inspect_bad_block_task_;
  common::ObTimer timer_;

  lib::ObMutex bad_block_lock_;
  common::ObArray<ObBadBlockInfo> bad_block_infos_;

  common::ObIODevice *io_device_;
  ObMacroBlockSeqGenerator blk_seq_generator_;
  int64_t alloc_num_;
  lib::ObMutex resize_file_lock_;

  // for resource_isolation
  uint64_t group_id_;

  bool is_inited_;
  bool is_started_;
};

class ObServerBlockManager : public ObBlockManager
{
public:
  static ObServerBlockManager &get_instance();

private:
  ObServerBlockManager() = default;
  virtual ~ObServerBlockManager() = default;
};

}
}

#define OB_SERVER_BLOCK_MGR (oceanbase::blocksstable::ObServerBlockManager::get_instance())

#endif /* OCEANBASE_BLOCKSSTABLE_OB_BLOCK_MANAGER_H_ */

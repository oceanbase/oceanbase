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

#ifndef OCEANBASE_STORAGE_OB_DDL_REDO_LOG_WRITER_H
#define OCEANBASE_STORAGE_OB_DDL_REDO_LOG_WRITER_H
#include "common/ob_tablet_id.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "storage/blocksstable/ob_imacro_block_flush_callback.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{

namespace blocksstable
{
struct ObSSTableMergeRes;
}

namespace logservice
{
class ObLogHandler;
}


namespace storage
{
class ObLSHandle;
class ObDDLNeedStopWriteChecker
{
public:
  virtual bool check_need_stop_write() = 0;
};

class ObDDLFullNeedStopWriteChecker : public ObDDLNeedStopWriteChecker
{
public:
  ObDDLFullNeedStopWriteChecker(ObDDLKvMgrHandle &ddl_kv_mgr_handle) : ddl_kv_mgr_handle_(ddl_kv_mgr_handle) {}
  virtual ~ObDDLFullNeedStopWriteChecker() {}
  virtual bool check_need_stop_write() override;
public:
  ObDDLKvMgrHandle &ddl_kv_mgr_handle_;
};

class ObDDLIncNeedStopWriteChecker : public ObDDLNeedStopWriteChecker
{
public:
  ObDDLIncNeedStopWriteChecker(ObTablet &tablet) : tablet_(tablet) {}
  virtual ~ObDDLIncNeedStopWriteChecker() {}
  virtual bool check_need_stop_write() override;
public:
  ObTablet &tablet_;
};

// control the write speed of ddl clog for 4.0 . More detailly,
// a. set write speed to the log archive speed if archive is on;
// b. set write speed to the out bandwidth throttle rate if archive is off.
// c. control ddl clog space used at tenant level rather than observer/logstream level.
class ObDDLCtrlSpeedItem final
{
public:
  ObDDLCtrlSpeedItem(): is_inited_(false), ls_id_(share::ObLSID::INVALID_LS_ID),
      next_available_write_ts_(-1), write_speed_(750), disk_used_stop_write_threshold_(-1),
      need_stop_write_(false), ref_cnt_(0) {}
  ~ObDDLCtrlSpeedItem() {};
  void reset_need_stop_write() { need_stop_write_ = false; }
  int init(const share::ObLSID &ls_id);
  int refresh();
  int limit_and_sleep(const int64_t bytes,
                      const uint64_t tenant_id,
                      const int64_t task_id,
                      ObDDLNeedStopWriteChecker &checker,
                      int64_t &real_sleep_us);
  int check_need_stop_write(ObDDLNeedStopWriteChecker &checker,
                            bool &is_need_stop_write);
  // for ref_cnt_
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }

  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(next_available_write_ts),
    K_(write_speed), K_(disk_used_stop_write_threshold), K_(need_stop_write), K_(ref_cnt));
private:
  int check_cur_node_is_leader(bool &is_leader);
  int cal_limit(const int64_t bytes, int64_t &next_available_ts);
  int do_sleep(const int64_t next_available_ts,
               const uint64_t tenant_id,
               const int64_t task_id,
               ObDDLNeedStopWriteChecker &checker,
               int64_t &real_sleep_us);
private:
  static const int64_t MIN_WRITE_SPEED = 50L;
  static const int64_t SLEEP_INTERVAL = 1 * 1000; // 1ms
  bool is_inited_;
  share::ObLSID ls_id_;
  int64_t next_available_write_ts_;
  int64_t write_speed_;
  int64_t disk_used_stop_write_threshold_; // stop write threshold on tenant level.
  bool need_stop_write_;
  int64_t ref_cnt_; // reference count
  DISALLOW_COPY_AND_ASSIGN(ObDDLCtrlSpeedItem);
};

class ObDDLCtrlSpeedHandle final
{
public:
  int init();
  static ObDDLCtrlSpeedHandle &get_instance();
  int limit_and_sleep(const uint64_t tenant_id,
                      const share::ObLSID &ls_id,
                      const int64_t bytes,
                      const int64_t task_id,
                      ObDDLNeedStopWriteChecker &checker,
                      int64_t &real_sleep_us);

private:
  struct SpeedHandleKey {
    public:
      SpeedHandleKey()
        : tenant_id_(OB_INVALID_TENANT_ID), ls_id_() {}
      ~SpeedHandleKey() {}
      int64_t hash() const {return tenant_id_ + ls_id_.hash();}
      int hash(uint64_t &hash_val) const {hash_val = hash(); return OB_SUCCESS;}
      bool is_valid() const {
        return OB_INVALID_TENANT_ID != tenant_id_ && ls_id_.is_valid();}
      bool operator == (const SpeedHandleKey &other) const {
        return tenant_id_ == other.tenant_id_ && ls_id_ == other.ls_id_;}
      TO_STRING_KV(K_(tenant_id), K_(ls_id));
    public:
      uint64_t tenant_id_;
      share::ObLSID ls_id_;
  };
private:
  class RefreshSpeedHandleTask: public common::ObTimerTask
  {
  public:
    RefreshSpeedHandleTask();
    virtual ~RefreshSpeedHandleTask();
    int init(int tg_id);
    virtual void runTimerTask() override;
  private:
#ifdef ERRSIM
    const static int64_t REFRESH_INTERVAL = 100 * 1000; // 100ms
#else
    const static int64_t REFRESH_INTERVAL = 1 * 1000 * 1000; // 1s
#endif
    bool is_inited_;
    DISABLE_COPY_ASSIGN(RefreshSpeedHandleTask);
  };
private:
  struct UpdateSpeedHandleItemFn final
  {
  public:
    UpdateSpeedHandleItemFn() = default;
    ~UpdateSpeedHandleItemFn() = default;
    int operator() (common::hash::HashMapPair<SpeedHandleKey, ObDDLCtrlSpeedItem*> &entry);
  };
  struct GetNeedRemoveItemsFn final
  {
  public:
    GetNeedRemoveItemsFn() :
      remove_items_() { }
    ~GetNeedRemoveItemsFn() = default;
    int operator() (common::hash::HashMapPair<SpeedHandleKey, ObDDLCtrlSpeedItem*> &entry);
  public:
    ObArray<SpeedHandleKey> remove_items_;
  };
private:
  class ObDDLCtrlSpeedItemHandle final
  {
  public:
    ObDDLCtrlSpeedItemHandle(): item_(nullptr) { }
    ~ObDDLCtrlSpeedItemHandle() { reset(); }
    int set_ctrl_speed_item(
        ObDDLCtrlSpeedItem *item);
    int get_ctrl_speed_item(
        ObDDLCtrlSpeedItem*& item) const;
    void reset();
  private:
    ObDDLCtrlSpeedItem *item_;
    DISALLOW_COPY_AND_ASSIGN(ObDDLCtrlSpeedItemHandle);
  };
private:
  ObDDLCtrlSpeedHandle();
  ~ObDDLCtrlSpeedHandle();
  int refresh();
  int add_ctrl_speed_item(const SpeedHandleKey &speed_handle_key, ObDDLCtrlSpeedItemHandle &item_handle);
  int remove_ctrl_speed_item(const ObIArray<SpeedHandleKey> &remove_items);

private:
  static const int64_t MAP_BUCKET_NUM  = 1024;
  bool is_inited_;
  common::hash::ObHashMap<SpeedHandleKey, ObDDLCtrlSpeedItem*> speed_handle_map_;
  common::ObArenaAllocator allocator_;
  common::ObBucketLock bucket_lock_;
  RefreshSpeedHandleTask refreshTimerTask_;
};

struct ObDDLRedoLogHandle final
{
public:
  static const int64_t DDL_REDO_LOG_TIMEOUT = 60 * 1000 * 1000; // 1min
  static const int64_t CHECK_DDL_REDO_LOG_FINISH_INTERVAL = 1000; // 1ms
  ObDDLRedoLogHandle();
  ~ObDDLRedoLogHandle();
  int wait(const int64_t timeout = DDL_REDO_LOG_TIMEOUT);
  void reset();
  bool is_valid() const { return nullptr != cb_  && scn_.is_valid_and_not_min(); }
public:
  ObDDLMacroBlockClogCb *cb_;
  share::SCN scn_;
};

class ObDDLCommitLogHandle final
{
public:
  ObDDLCommitLogHandle();
  ~ObDDLCommitLogHandle();
  int wait(const int64_t timeout = ObDDLRedoLogHandle::DDL_REDO_LOG_TIMEOUT);
  void reset();
  share::SCN get_commit_scn() const { return commit_scn_; }
public:
  ObDDLCommitClogCb *cb_;
  share::SCN commit_scn_;
};

class ObDDLRedoLock final
{
  friend class ObDDLRedoLockGuard;
public:
  static ObDDLRedoLock &get_instance();
  int init();
private:
  ObDDLRedoLock();
  ~ObDDLRedoLock();
private:
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
};

class ObDDLRedoLockGuard
{
public:
  explicit ObDDLRedoLockGuard(const uint64_t hash_val)
    : guard_(ObDDLRedoLock::get_instance().bucket_lock_, hash_val) {}
  ~ObDDLRedoLockGuard() {}
private:
  common::ObBucketHashWLockGuard guard_;
};


// This class should be the entrance to write redo log and commit log
class ObDDLRedoLogWriter final
{
public:
  ObDDLRedoLogWriter();
  ~ObDDLRedoLogWriter();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id);
  void reset();
  int write_start_log(
      const ObITable::TableKey &table_key,
      const int64_t execution_id,
      const uint64_t data_format_version,
      const ObDirectLoadType direct_load_type,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      ObDDLKvMgrHandle &lob_kv_mgr_handle,
      ObTabletDirectLoadMgrHandle &mgr_handle,
      uint32_t &lock_tid,
      share::SCN &start_scn);
  int write_macro_block_log(
      const storage::ObDDLMacroBlockRedoInfo &redo_info,
      const blocksstable::MacroBlockId &macro_block_id,
      const bool allow_remote_write,
      const int64_t task_id);
  int wait_macro_block_log_finish(
      const storage::ObDDLMacroBlockRedoInfo &redo_info,
      const blocksstable::MacroBlockId &macro_block_id);
  int write_commit_log(
      const bool allow_remote_write,
      const ObITable::TableKey &table_key,
      const share::SCN &start_scn,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      ObTabletHandle &tablet_handle,
      share::SCN &commit_scn,
      bool &is_remote_write,
      uint32_t &lock_tid);
  int write_commit_log_with_retry(
      const bool allow_remote_write,
      const ObITable::TableKey &table_key,
      const share::SCN &start_scn,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      ObTabletHandle &tablet_handle,
      share::SCN &commit_scn,
      bool &is_remote_write,
      uint32_t &lock_tid);
  static const int64_t DEFAULT_RETRY_TIMEOUT_US = 60L * 1000L * 1000L; // 1min
  static bool need_retry(int ret_code);
private:
  int switch_to_remote_write();
  int local_write_ddl_start_log(
      const ObDDLStartLog &log,
      ObLSHandle &ls_handle,
      logservice::ObLogHandler *log_handler,
      ObDDLKvMgrHandle &ddl_kv_mgr_handle,
      ObDDLKvMgrHandle &lob_kv_mgr_handle,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      uint32_t &lock_tid,
      share::SCN &start_scn);
  int local_write_ddl_commit_log(
      const ObDDLCommitLog &log,
      const ObDDLClogType clog_type,
      const share::ObLSID &ls_id,
      logservice::ObLogHandler *log_handler,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      ObTabletDirectLoadMgrHandle &lob_direct_load_mgr_handle,
      ObDDLCommitLogHandle &handle,
      uint32_t &lock_tid);
  int remote_write_ddl_commit_redo(
      const obrpc::ObRpcRemoteWriteDDLCommitLogArg &arg,
      share::SCN &commit_scn);
  int retry_remote_write_macro_redo(
      const int64_t task_id,
      const storage::ObDDLMacroBlockRedoInfo &redo_info);
  int retry_remote_write_commit_clog(
      const obrpc::ObRpcRemoteWriteDDLCommitLogArg &arg,
      share::SCN &commit_scn);
  int local_write_ddl_macro_redo(
      const storage::ObDDLMacroBlockRedoInfo &redo_info,
      const share::ObLSID &ls_id,
      const int64_t task_id,
      logservice::ObLogHandler *log_handler,
      const blocksstable::MacroBlockId &macro_block_id,
      char *buffer,
      ObDDLRedoLogHandle &handle);
  int remote_write_ddl_macro_redo(
      const int64_t task_id,
      const storage::ObDDLMacroBlockRedoInfo &redo_info);
private:
  bool is_inited_;
  bool remote_write_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObDDLRedoLogHandle ddl_redo_handle_;
  ObAddr leader_addr_;
  share::ObLSID leader_ls_id_;
  char *buffer_;
};

// write macro redo for data block, need to set lsn on ObDDLRedoLogWriter when commit.
class ObDDLRedoLogWriterCallback : public blocksstable::ObIMacroBlockFlushCallback
{
public:
  ObDDLRedoLogWriterCallback();
  virtual ~ObDDLRedoLogWriterCallback();
  int init(
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const storage::ObDDLMacroBlockType block_type,
      const ObITable::TableKey &table_key,
      const int64_t task_id,
      const share::SCN &start_scn,
      const uint64_t data_format_version,
      const storage::ObDirectLoadType direct_load_type,
      const int64_t row_id_offset = -1);
  void reset();
  int write(
      blocksstable::ObMacroBlockHandle &macro_handle,
      const blocksstable::ObLogicMacroBlockId &logic_id,
      char *buf,
      const int64_t buf_len,
      const int64_t row_count);
  int wait();
  virtual int64_t get_ddl_start_row_offset() const override { return row_id_offset_; }
private:
  bool is_column_group_info_valid() const;
  int retry(const int64_t timeout_us);
private:
  bool is_inited_;
  storage::ObDDLMacroBlockRedoInfo redo_info_;
  storage::ObDDLMacroBlockType block_type_;
  ObITable::TableKey table_key_;
  blocksstable::MacroBlockId macro_block_id_;
  ObDDLRedoLogWriter ddl_writer_;
  int64_t task_id_;
  share::SCN start_scn_;
  uint64_t data_format_version_;
  storage::ObDirectLoadType direct_load_type_;
  // if has one macro block with 100 rows before, this macro block's ddl_start_row_offset will be 100.
  // if current macro block finish with 50 rows, current macro block's end_row_offset will be 149.
  // end_row_offset = ddl_start_row_offset + curr_row_count - 1.
  int64_t row_id_offset_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif //OCEANBASE_STORAGE_OB_DDL_REDO_LOG_WRITER_H

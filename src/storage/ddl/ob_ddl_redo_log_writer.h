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
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDDLMacroBlockRedoInfo;
struct ObSSTableMergeRes;
}
namespace logservice
{
class ObLogHandler;
}

namespace storage
{
class ObDDLKV;
class ObDDLKVPendingGuard;
class ObLSHandle;

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
  int limit_and_sleep(const int64_t bytes, int64_t &real_sleep_us);

  // for ref_cnt_
  void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1); }
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }

  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(next_available_write_ts),
    K_(write_speed), K_(disk_used_stop_write_threshold), K_(need_stop_write), K_(ref_cnt));
private:
  int cal_limit(const int64_t bytes, int64_t &next_available_ts);
  int do_sleep(const int64_t next_available_ts, int64_t &real_sleep_us);
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
  int limit_and_sleep(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t bytes, int64_t &real_sleep_us);

private:
  struct SpeedHandleKey {
    public:
      SpeedHandleKey()
        : tenant_id_(OB_INVALID_TENANT_ID), ls_id_() {}
      ~SpeedHandleKey() {}
      int64_t hash() const {return tenant_id_ + ls_id_.hash();}
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
    const static int64_t REFRESH_INTERVAL = 1 * 1000 * 1000; // 1s
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
  int remove_ctrl_speed_item(const SpeedHandleKey &speed_handle_key);

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
  static const int64_t DDL_REDO_LOG_TIMEOUT = 10 * 1000 * 1000; // 10
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

class ObDDLRedoLogWriter final
{
public:
  static ObDDLRedoLogWriter &get_instance();
  int init();
  int write(ObTabletHandle &tablet_handle,
            ObDDLKvMgrHandle &ddl_kv_mgr_handle,
            const ObDDLRedoLog &log,
            const uint64_t tenant_id,
            const share::ObLSID &ls_id,
            logservice::ObLogHandler *log_handler,
            const blocksstable::MacroBlockId &macro_block_id,
            char *buffer,
            ObDDLRedoLogHandle &handle);
  int write_ddl_start_log(ObTabletHandle &tablet_handle,
                          ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                          const ObDDLStartLog &log,
                          logservice::ObLogHandler *log_handler,
                          share::SCN &start_scn);
  template <typename T>
  int write_ddl_commit_log(ObTabletHandle &tablet_handle,
                           ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                           const T &log,
                           const ObDDLClogType clog_type,
                           const share::ObLSID &ls_id,
                           logservice::ObLogHandler *log_handler,
                           ObDDLCommitLogHandle &handle);
private:
  ObDDLRedoLogWriter();
  ~ObDDLRedoLogWriter();
  struct ObDDLRedoLogStat final
  {
  public:
    ObDDLRedoLogStat();
    ~ObDDLRedoLogStat();
  public:
  };
  // TODO: traffic control
private:
  bool is_inited_;
  common::ObBucketLock bucket_lock_;
};


class ObDDLMacroBlockRedoWriter final
{
public:
  static int write_macro_redo(ObTabletHandle &tablet_handle,
                              ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                              const blocksstable::ObDDLMacroBlockRedoInfo &redo_info,
                              const share::ObLSID &ls_id,
                              logservice::ObLogHandler *log_handler,
                              const blocksstable::MacroBlockId &macro_block_id,
                              char *buffer,
                              ObDDLRedoLogHandle &handle);
  static int remote_write_macro_redo(const ObAddr &leader_addr,
                                     const share::ObLSID &leader_ls_id,
                                     const blocksstable::ObDDLMacroBlockRedoInfo &redo_info);
private:
  ObDDLMacroBlockRedoWriter() = default;
  ~ObDDLMacroBlockRedoWriter() = default;
};

// This class should be the entrance to write redo log and commit log
class ObDDLSSTableRedoWriter final
{
public:
  ObDDLSSTableRedoWriter();
  ~ObDDLSSTableRedoWriter();
  int init(const share::ObLSID &ls_id, const ObTabletID &tablet_id);
  int start_ddl_redo(const ObITable::TableKey &table_key,
                     const int64_t execution_id,
                     const int64_t ddl_cluster_version,
                     ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int end_ddl_redo_and_create_ddl_sstable(ObLSHandle &ls_handle,
                                          const ObITable::TableKey &table_key,
                                          const uint64_t table_id,
                                          const int64_t execution_id,
                                          const int64_t ddl_task_id);
  int write_redo_log(const blocksstable::ObDDLMacroBlockRedoInfo &redo_info,
                     const blocksstable::MacroBlockId &macro_block_id,
                     const bool allow_remote_write,
                     ObTabletHandle &tablet_handle,
                     ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int wait_redo_log_finish(const blocksstable::ObDDLMacroBlockRedoInfo &redo_info,
                           const blocksstable::MacroBlockId &macro_block_id);
  int write_commit_log(ObTabletHandle &tablet_handle,
                       ObDDLKvMgrHandle &ddl_kv_mgr_handle,
                       const ObITable::TableKey &table_key,
                       const int64_t table_id,
                       const int64_t execution_id,
                       const int64_t ddl_task_id,
                       share::SCN &commit_scn);
  OB_INLINE void set_start_scn(const share::SCN &start_scn) { start_scn_.atomic_set(start_scn); }
  OB_INLINE share::SCN get_start_scn() const { return start_scn_.atomic_get(); }
private:
  int switch_to_remote_write();
private:
  bool is_inited_;
  bool remote_write_;
  share::SCN start_scn_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  ObDDLRedoLogHandle ddl_redo_handle_;
  ObAddr leader_addr_;
  share::ObLSID leader_ls_id_;
  char *buffer_;
};

// write macro redo for data block, need to set lsn on ObDDLSSTableRedoWriter when commit.
class ObDDLRedoLogWriterCallback : public blocksstable::ObIMacroBlockFlushCallback
{
public:
  ObDDLRedoLogWriterCallback();
  virtual ~ObDDLRedoLogWriterCallback();
  int init(const blocksstable::ObDDLMacroBlockType block_type, const ObITable::TableKey &table_key, ObDDLSSTableRedoWriter *ddl_writer, ObDDLKvMgrHandle &ddl_kv_mgr_handle);
  int write(
      const ObMacroBlockHandle &macro_handle,
      const blocksstable::ObLogicMacroBlockId &logic_id,
      char *buf,
      const int64_t data_seq);
  int wait();
  int prepare_block_buffer_if_need();
private:
  bool is_inited_;
  blocksstable::ObDDLMacroBlockRedoInfo redo_info_;
  blocksstable::ObDDLMacroBlockType block_type_;
  ObITable::TableKey table_key_;
  blocksstable::MacroBlockId macro_block_id_;
  ObDDLSSTableRedoWriter *ddl_writer_;
  char *block_buffer_;
  ObTabletHandle tablet_handle_;
  ObDDLKvMgrHandle ddl_kv_mgr_handle_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif //OCEANBASE_STORAGE_OB_DDL_REDO_LOG_WRITER_H

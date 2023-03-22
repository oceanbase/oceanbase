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

#ifndef OCEANBASE_STORAGE_OB_I_MEMTABLE_MGR
#define OCEANBASE_STORAGE_OB_I_MEMTABLE_MGR

#include "storage/ob_i_table.h"
#include "storage/memtable/ob_multi_source_data.h"
#include "storage/ob_storage_schema_recorder.h"

namespace oceanbase
{
namespace logservice
{
class ObLogHandler;
}
namespace memtable
{
class ObIMemtable;
class ObIMultiSourceDataUnit;
}

namespace storage
{
class ObFreezer;

using ObTableHdlArray = common::ObIArray<ObTableHandleV2>;

class ObIMemtableMgr
{

public:
  ObIMemtableMgr()
    : is_inited_(false),
      ref_cnt_(0),
      tablet_id_(),
      freezer_(nullptr),
      table_type_(ObITable::TableType::MAX_TABLE_TYPE),
      memtable_head_(0),
      memtable_tail_(0),
      t3m_(nullptr),
      lock_()
  {
    memset(tables_, 0, sizeof(tables_));
  }
  virtual ~ObIMemtableMgr() { reset_tables(); }

  int init(
      const ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const int64_t max_saved_schema_version,
      logservice::ObLogHandler *log_handler,
      ObFreezer *freezer,
      ObTenantMetaMemMgr *t3m);
  virtual int create_memtable(const int64_t clog_checkpoint_ts,
                              const int64_t schema_version,
                              const bool for_replay=false) = 0;

  virtual int get_active_memtable(ObTableHandleV2 &handle) const;

  virtual int get_first_nonempty_memtable(ObTableHandleV2 &handle) const;

  virtual int get_all_memtables(ObTableHdlArray &handle);

  virtual void destroy() = 0;

  virtual int get_boundary_memtable(ObTableHandleV2 &handle) { return OB_SUCCESS; }

  virtual int get_memtable_for_replay(int64_t replay_log_ts, ObTableHandleV2 &handle)
  {
    return OB_SUCCESS;
  }

  virtual int get_multi_source_data_unit(
      memtable::ObIMultiSourceDataUnit *const multi_source_data_unit,
      ObIAllocator *allocator = nullptr) const;

  virtual int get_memtable_for_multi_source_data_unit(
      ObTableHandleV2 &handle,
      const memtable::MultiSourceDataUnitType type) const;

  int release_memtables(const int64_t log_ts);
  // force release all memtables
  // WARNING: this will release all the ref of memtable, make sure you will not use it again.
  int release_memtables();

  bool has_memtable()
  {
    SpinRLockGuard lock_guard(lock_);
    return has_memtable_();
  }

  int get_newest_clog_checkpoint_ts(int64_t &clog_checkpoint_ts);

  int get_newest_snapshot_version(int64_t &snapshot_version);

  OB_INLINE int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */); }
  OB_INLINE int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE void reset()
  {
    destroy();
    ATOMIC_STORE(&ref_cnt_, 0);
  }
  virtual int init_storage_schema_recorder(
      const ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const int64_t max_saved_schema_version,
      logservice::ObLogHandler *log_handler)
  { // do nothing
    UNUSED(tablet_id);
    UNUSED(ls_id);
    UNUSED(max_saved_schema_version),
    UNUSED(log_handler);
    return OB_NOT_SUPPORTED;
  }
  virtual int reset_storage_schema_recorder()
  { // do nothing
    return OB_NOT_SUPPORTED;
  }
  virtual int remove_memtables_from_data_checkpoint() { return OB_SUCCESS; }
  DECLARE_VIRTUAL_TO_STRING;
protected:
  static int64_t get_memtable_idx(const int64_t pos) { return pos & (MAX_MEMSTORE_CNT - 1); }
  virtual int release_head_memtable_(memtable::ObIMemtable *memtable, const bool force = false) = 0;
  int add_memtable_(ObTableHandleV2 &memtable);
  int get_ith_memtable(const int64_t pos, ObTableHandleV2 &handle) const;
  int64_t get_memtable_count_() const { return memtable_tail_ - memtable_head_; }
  bool has_memtable_() const { return get_memtable_count_() > 0; }
  //for test
  void set_freezer(ObFreezer *freezer) { freezer_ = freezer; }
  void reset_tables();
  void release_head_memtable();
  void release_tail_memtable();
  virtual int init(const ObTabletID &tablet_id,
                   const share::ObLSID &ls_id,
                   ObFreezer *freezer,
                   ObTenantMetaMemMgr *t3m) = 0;

protected:
  bool is_inited_;
  volatile int64_t ref_cnt_;
  common::ObTabletID tablet_id_;
  // FIXME : whether freeze_handler on base class or not ?
  ObFreezer *freezer_;
  ObITable::TableType table_type_;
  int64_t memtable_head_;
  int64_t memtable_tail_;
  ObTenantMetaMemMgr *t3m_;
  memtable::ObIMemtable *tables_[MAX_MEMSTORE_CNT];
  mutable common::SpinRWLock lock_;
};

class ObMemtableMgrHandle final
{
public:
  ObMemtableMgrHandle();
  ObMemtableMgrHandle(ObIMemtableMgr *memtable_mgr, ObITenantMetaObjPool *pool = nullptr);
  ~ObMemtableMgrHandle();

  bool is_valid() const;
  void reset();

  OB_INLINE ObIMemtableMgr *get_memtable_mgr() { return memtable_mgr_; }
  OB_INLINE const ObIMemtableMgr *get_memtable_mgr() const { return memtable_mgr_; }

  ObMemtableMgrHandle(const ObMemtableMgrHandle &other);
  ObMemtableMgrHandle &operator= (const ObMemtableMgrHandle &other);

  int set_memtable_mgr(ObIMemtableMgr *memtable_mgr, ObITenantMetaObjPool *pool = nullptr);

  TO_STRING_KV(KPC_(memtable_mgr), KP_(pool));

private:
  ObIMemtableMgr *memtable_mgr_;
  ObITenantMetaObjPool *pool_;
};

}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_TABLET_MEMTABLE_MGR

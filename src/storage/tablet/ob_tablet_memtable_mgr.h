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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MEMTABLE_MGR
#define OCEANBASE_STORAGE_OB_TABLET_MEMTABLE_MGR

#include "common/ob_tablet_id.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/ob_i_memtable_mgr.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_storage_schema_recorder.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/mds_table_mgr.h"
#include "storage/checkpoint/ob_checkpoint_diagnose.h"

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
}

namespace storage
{
class ObIMemtable;
class ObTenantMetaMemMgr;
class ObFreezer;

class ObTabletMemtableMgr : public ObIMemtableMgr
{
public:
  friend class memtable::ObMemtable;
public:
  typedef common::ObIArray<ObTableHandleV2> ObTableHdlArray;

public:
  ObTabletMemtableMgr();
  virtual ~ObTabletMemtableMgr();

  bool has_active_memtable();
  int get_memtables_nolock(ObTableHdlArray &handle);
  int get_first_frozen_memtable(ObTableHandleV2 &handle);
  ObStorageSchemaRecorder &get_storage_schema_recorder() { return schema_recorder_; }
  compaction::ObTabletMediumCompactionInfoRecorder &get_medium_info_recorder() { return medium_info_recorder_; }
  int unset_logging_blocked_for_active_memtable(ObITabletMemtable *memtable);
  int resolve_left_boundary_for_active_memtable(ObITabletMemtable *memtable,
                                                const share::SCN start_scn);
  int freeze_direct_load_memtable(ObITabletMemtable *tablet_memtable);
  int get_direct_load_memtables_for_write(ObTableHdlArray &handles);

public: // derived from ObIMemtableMgr
  virtual int init(const common::ObTabletID &tablet_id,
                   const share::ObLSID &ls_id,
                   ObFreezer *freezer,
                   ObTenantMetaMemMgr *t3m) override;

  virtual int get_active_memtable(ObTableHandleV2 &handle) const override;
  virtual int get_all_memtables(ObTableHdlArray &handle) override;
  virtual void destroy() override;
  virtual int get_memtable_for_replay(const share::SCN &replay_scn,
                                      ObTableHandleV2 &handle) override;
  virtual int get_boundary_memtable(ObTableHandleV2 &handle) override;
  virtual int create_memtable(const CreateMemtableArg &arg) override;
  virtual int get_last_frozen_memtable(ObTableHandleV2 &handle) override;
  virtual int set_is_tablet_freeze_for_active_memtable(ObTableHandleV2 &handle,
                                                       const int64_t trace_id = checkpoint::INVALID_TRACE_ID);
  virtual int init_storage_recorder(const ObTabletID &tablet_id,
                                    const share::ObLSID &ls_id,
                                    const int64_t max_saved_schema_version,
                                    const int64_t max_saved_medium_scn,
                                    const lib::Worker::CompatMode compat_mode,
                                    logservice::ObLogHandler *log_handler) override;
  virtual int set_frozen_for_all_memtables() override;

  DECLARE_VIRTUAL_TO_STRING;

protected:
  virtual int release_head_memtable_(ObIMemtable *memtable,
                                     const bool force = false) override;

private:
  void block_freeze_if_memstore_full_(ObITabletMemtable *new_tablet_memtable);
  void clean_tail_memtable_();
  int resolve_boundary_(ObITabletMemtable *new_tablet_memtable, const CreateMemtableArg &arg);
  int get_active_memtable_(ObTableHandleV2 &handle) const;
  int get_boundary_memtable_(ObTableHandleV2 &handle);
  int get_memtables_(ObTableHdlArray &handle, const int64_t start_point, const bool include_active_memtable);
  int add_tables_(const int64_t start_pos, const bool include_active_memtable, ObTableHdlArray &handle);
  int find_start_pos_(const int64_t start_point, int64_t &start_pos);
  int find_last_data_memtable_pos_(int64_t &last_pos);
  int get_first_frozen_memtable_(ObTableHandleV2 &handle);
  int get_last_frozen_memtable_(ObTableHandleV2 &handle);
  int try_resolve_boundary_on_create_memtable_for_leader_(ObITabletMemtable *last_frozen_tablet_memtable,
                                                          ObITabletMemtable *new_tablet_memtable);
  int check_boundary_memtable_(const uint32_t logstream_freeze_clock);
  void resolve_data_memtable_boundary_(ObITabletMemtable *frozen_tablet_memtable,
                                       ObITabletMemtable *active_tablet_memtable,
                                       const CreateMemtableArg &arg);
  void resolve_direct_load_memtable_boundary_(ObITabletMemtable *frozen_tablet_memtable,
                                              ObITabletMemtable *active_tablet_memtable,
                                              const CreateMemtableArg &arg);
  int create_memtable_(const CreateMemtableArg &arg, const uint32_t logstream_freeze_clock, ObTimeGuard &tg);
  int acquire_tablet_memtable_(const bool for_inc_direct_load, ObTableHandleV2 &handle);
  ObITabletMemtable *get_active_memtable_();
  ObITabletMemtable *get_memtable_(const int64_t pos) const;

  DISALLOW_COPY_AND_ASSIGN(ObTabletMemtableMgr);

private:
  ObLS *ls_; // 8B
  common::SpinRWLock lock_def_; //8B
  int64_t retry_times_; // 8B
  ObStorageSchemaRecorder schema_recorder_; // 120B
  compaction::ObTabletMediumCompactionInfoRecorder medium_info_recorder_; // 96B
};

class ObTabletMemtableMgrPool
{
public:
  ObTabletMemtableMgrPool()
    : allocator_(sizeof(ObTabletMemtableMgr), lib::ObMemAttr(MTL_ID(), "TltMemtablMgr")),
      count_(0) {}
  static int mtl_init(ObTabletMemtableMgrPool* &m) { return OB_SUCCESS; }
  void destroy() {}
  ObTabletMemtableMgr* acquire()
  {
    void *ptr = allocator_.alloc();
    ObTabletMemtableMgr *ret = NULL;
    if (OB_NOT_NULL(ptr)) {
      ret = new(ptr)ObTabletMemtableMgr();
      ATOMIC_INC(&count_);
    }
    return ret;
  }
  void release(ObTabletMemtableMgr *mgr)
  {
    OB_ASSERT(OB_NOT_NULL(mgr));
    mgr->~ObTabletMemtableMgr();
    allocator_.free(static_cast<void*>(mgr));
    ATOMIC_DEC(&count_);
  }

  int64_t get_count() { return ATOMIC_LOAD(&count_); }
private:
  common::ObSliceAlloc allocator_;
  int64_t count_;
};

}
}

#endif // OCEANBASE_STORAGE_OB_TABLET_MEMTABLE_MGR

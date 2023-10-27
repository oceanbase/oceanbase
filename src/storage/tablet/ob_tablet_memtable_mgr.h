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

namespace oceanbase
{
namespace memtable
{
class ObMemtable;
class ObIMemtable;
class ObIMultiSourceDataUnit;
}

namespace storage
{
class ObIPartitionComponentFactory;
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

  virtual int init(const common::ObTabletID &tablet_id,
                   const share::ObLSID &ls_id,
                   ObFreezer *freezer,
                   ObTenantMetaMemMgr *t3m) override;

  virtual int get_active_memtable(ObTableHandleV2 &handle) const override;
  virtual int get_all_memtables(ObTableHdlArray &handle) override;
  virtual void destroy() override;
  uint32_t get_ls_freeze_clock();

  bool has_active_memtable();
  int64_t get_memtable_count() const;
  virtual int get_memtable_for_replay(share::SCN replay_scn,
                                      ObTableHandleV2 &handle) override;
  int get_last_frozen_memtable(ObTableHandleV2 &handle) const;
  virtual int get_boundary_memtable(ObTableHandleV2 &handle) override;
  int release_tail_memtable(memtable::ObIMemtable *memtable);
  virtual int create_memtable(
      const share::SCN clog_checkpoint_scn,
      const int64_t schema_version,
      const share::SCN newest_clog_checkpoint_scn,
      const bool for_replay) override;
  int get_memtables(
      ObTableHdlArray &handle,
      const bool reset_handle = true,
      const int64_t start_point = -1,
      const bool include_active_memtable = true);
  int get_memtables_v2(
      ObTableHdlArray &handle,
      const int64_t start_log_ts,
      const int64_t start_snapshot_version,
      const bool reset_handle = true,
      const bool include_active_memtable = true);
  int get_memtables_nolock(ObTableHdlArray &handle);
  int get_first_frozen_memtable(ObTableHandleV2 &handle) const;
  int set_is_tablet_freeze_for_active_memtable(ObTableHandleV2 &handle,
                                               bool is_force_freeze = false);

  ObStorageSchemaRecorder &get_storage_schema_recorder() { return schema_recorder_; }
  compaction::ObTabletMediumCompactionInfoRecorder &get_medium_info_recorder() { return medium_info_recorder_; }

  virtual int init_storage_recorder(
      const ObTabletID &tablet_id,
      const share::ObLSID &ls_id,
      const int64_t max_saved_schema_version,
      const int64_t max_saved_medium_scn,
      const lib::Worker::CompatMode compat_mode,
      logservice::ObLogHandler *log_handler) override;
  virtual int reset_storage_recorder() override;
  virtual int set_frozen_for_all_memtables() override;
  DECLARE_VIRTUAL_TO_STRING;

protected:
  virtual int release_head_memtable_(memtable::ObIMemtable *memtable,
                                     const bool force = false) override;

private:
  //minor freeze
  int64_t get_unmerged_memtable_count_() const;
  memtable::ObMemtable *get_active_memtable_();
  int get_active_memtable_(ObTableHandleV2 &handle) const;
  int get_memtables_(
      ObTableHdlArray &handle,
      const int64_t start_point,
      const bool include_active_memtable);
  int add_tables_(
      const int64_t start_pos,
      const bool include_active_memtable,
      ObTableHdlArray &handle);
  memtable::ObMemtable *get_memtable_(const int64_t pos) const;
  int find_start_pos_(const int64_t start_point, int64_t &start_pos);
  int find_start_pos_(
      const int64_t start_log_ts,
      const int64_t start_snapshot_version,
      int64_t &start_pos);
  int get_first_frozen_memtable_(ObTableHandleV2 &handle) const;
  void clean_tail_memtable_();
  int get_last_frozen_memtable_(ObTableHandleV2 &handle) const;
  int resolve_left_boundary_for_active_memtable(memtable::ObIMemtable *memtable,
                                                share::SCN start_scn,
                                                share::SCN snapshot_version);
  int unset_logging_blocked_for_active_memtable(memtable::ObIMemtable *memtable);
  void unlink_memtable_mgr_and_memtable_(memtable::ObMemtable *memtable);
  void wait_memtable_mgr_op_cnt_(memtable::ObMemtable *memtable);

  DISALLOW_COPY_AND_ASSIGN(ObTabletMemtableMgr);

private:
  static const int64_t PRINT_READABLE_INFO_DURATION_US = 1000 * 1000 * 60 * 10L; //10min

private:
  ObLS *ls_; // 8B
  common::SpinRWLock lock_def_; //8B
  int64_t retry_times_; // 8B
  ObStorageSchemaRecorder schema_recorder_; // 120B
  compaction::ObTabletMediumCompactionInfoRecorder medium_info_recorder_; // 96B
};
}
}

#endif // OCEANBASE_STORAGE_OB_TABLET_MEMTABLE_MGR

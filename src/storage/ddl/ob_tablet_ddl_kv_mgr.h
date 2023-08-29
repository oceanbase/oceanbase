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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DDL_KV_MGR_H
#define OCEANBASE_STORAGE_OB_TABLET_DDL_KV_MGR_H

#include "common/ob_tablet_id.h"
#include "lib/allocator/ob_concurrent_fifo_allocator.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "lib/ob_define.h"
#include "share/scn.h"
#include "share/ob_ls_id.h"
#include "storage/ob_i_table.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

namespace oceanbase
{
namespace storage
{

struct ObTabletDDLParam;
struct ObDDLTableMergeDagParam;

class ObTabletDDLKvMgr final
{
public:
  ObTabletDDLKvMgr();
  ~ObTabletDDLKvMgr();
  int init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id); // init before memtable mgr
  int ddl_start_nolock(const ObITable::TableKey &table_key, const share::SCN &start_scn, const int64_t data_format_version, const int64_t execution_id, const share::SCN &checkpoint_scn);
  int ddl_start(ObLS &ls, ObTablet &tablet, const ObITable::TableKey &table_key, const share::SCN &start_scn, const int64_t data_format_version, const int64_t execution_id, const share::SCN &checkpoint_scn);
  int ddl_commit(ObTablet &tablet, const share::SCN &start_scn, const share::SCN &commit_scn); // schedule build a major sstable
  int schedule_ddl_dump_task(ObTablet &tablet, const share::SCN &start_scn, const share::SCN &rec_scn);
  int schedule_ddl_merge_task(ObTablet &tablet, const share::SCN &start_scn, const share::SCN &commit_scn); // try wait build major sstable
  int wait_ddl_merge_success(ObTablet &tablet, const share::SCN &start_scn, const share::SCN &commit_scn);
  int get_ddl_param(ObTabletDDLParam &ddl_param);
  int get_or_create_ddl_kv(ObTablet &tablet, const share::SCN &start_scn, const share::SCN &scn, ObTableHandleV2 &kv_handle); // used in active ddl kv guard
  int get_freezed_ddl_kv(const share::SCN &freeze_scn, ObTableHandleV2 &kv_handle); // locate ddl kv with exeact freeze log ts
  int get_ddl_kvs(const bool frozen_only, ObTablesHandleArray &kv_handle_array); // get all freeze ddl kvs
  int get_ddl_kvs_for_query(ObTablet &tablet, ObTablesHandleArray &kv_handle_array);
  int freeze_ddl_kv(ObTablet &tablet, const share::SCN &freeze_scn = share::SCN::min_scn()); // freeze the active ddl kv, when memtable freeze or ddl commit
  int release_ddl_kvs(const share::SCN &rec_scn); // release persistent ddl kv, used in ddl merge task for free ddl kv
  int check_has_effective_ddl_kv(bool &has_ddl_kv); // used in ddl log handler for checkpoint
  int get_ddl_kv_min_scn(share::SCN &min_scn); // for calculate rec_scn of ls
  share::SCN get_start_scn() const { return start_scn_.atomic_load(); }
  bool is_started() const { return share::SCN::min_scn() != start_scn_; }
  void set_commit_scn_nolock(const share::SCN &scn) { commit_scn_ = scn; }
  int set_commit_scn(const ObTabletMeta &tablet_meta, const share::SCN &scn);
  share::SCN get_commit_scn(const ObTabletMeta &tablet_meta);
  int set_commit_success(const share::SCN &start_scn);
  bool is_commit_success();
  void reset_commit_success();
  common::ObTabletID get_tablet_id() const { return tablet_id_; }
  share::ObLSID get_ls_id() const { return ls_id_; }
  int cleanup();
  int online();
  bool is_execution_id_older(const int64_t execution_id);
  int register_to_tablet(const share::SCN &ddl_start_scn, ObDDLKvMgrHandle &kv_mgr_handle);
  int unregister_from_tablet(const share::SCN &ddl_start_scn, ObDDLKvMgrHandle &kv_mgr_handle);
  int rdlock(const int64_t timeout_us, uint32_t &lock_tid);
  int wrlock(const int64_t timeout_us, uint32_t &lock_tid);
  void unlock(const uint32_t lock_tid);
  int update_tablet(ObTablet &tablet, const share::SCN &start_scn, const int64_t snapshot_version, const int64_t data_format_version, const int64_t execution_id, const share::SCN &ddl_checkpoint_scn);
  int64_t get_count();
  OB_INLINE void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */); }
  OB_INLINE int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE void reset() { destroy(); }
  bool can_schedule_major_compaction_nolock(const ObTabletMeta &tablet_meta);
  int get_ddl_major_merge_param(ObTablet &tablet, ObDDLTableMergeDagParam &merge_param);
  int get_rec_scn(share::SCN &rec_scn);
  TO_STRING_KV(K_(is_inited), K_(success_start_scn), K_(ls_id), K_(tablet_id), K_(table_key),
      K_(data_format_version), K_(start_scn), K_(commit_scn), K_(max_freeze_scn),
      K_(execution_id), K_(head), K_(tail), K_(ref_cnt));

private:
  int64_t get_idx(const int64_t pos) const;
  int alloc_ddl_kv(ObTablet &tablet, ObTableHandleV2 &kv_handle);
  void free_ddl_kv(const int64_t idx);
  int get_active_ddl_kv_impl(ObTableHandleV2 &kv_handle);
  void try_get_ddl_kv_unlock(const share::SCN &scn, ObTableHandleV2 &kv_handle);
  int get_ddl_kvs_unlock(const bool frozen_only, ObTablesHandleArray &kv_handle_array);
  int64_t get_count_nolock() const;
  int update_ddl_major_sstable(ObTablet &tablet);
  int create_empty_ddl_sstable(ObTablet &tablet, common::ObArenaAllocator &allocator, blocksstable::ObSSTable &sstable);
  void cleanup_unlock();
  void destroy();
public:
  static const int64_t MAX_DDL_KV_CNT_IN_STORAGE = 16;
  static const int64_t TRY_LOCK_TIMEOUT = 10 * 1000000; // 10s
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;

  // state_lock_ guarded members
  share::SCN success_start_scn_;
  ObITable::TableKey table_key_;
  int64_t data_format_version_;
  share::SCN start_scn_;
  share::SCN commit_scn_;
  int64_t execution_id_;
  common::ObLatch state_lock_;

  // lock_ guarded members
  share::SCN max_freeze_scn_;
  ObTableHandleV2 ddl_kv_handles_[MAX_DDL_KV_CNT_IN_STORAGE];
  int64_t head_;
  int64_t tail_;
  common::ObLatch lock_;

  volatile int64_t ref_cnt_ CACHE_ALIGNED;
  DISALLOW_COPY_AND_ASSIGN(ObTabletDDLKvMgr);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PG_DDL_KV_MGR_H

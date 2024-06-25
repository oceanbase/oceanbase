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
class ObTablet;

// ddl kv: create, get, freeze, dump, release
// checkpoint manage
// recycle scn manage
class ObTabletDDLKvMgr final
{
public:
  ObTabletDDLKvMgr();
  ~ObTabletDDLKvMgr();
  int register_to_tablet(ObDDLKvMgrHandle &kv_mgr_handle);
  int init(const share::ObLSID &ls_id, const common::ObTabletID &tablet_id); // init before memtable mgr
  int set_max_freeze_scn(const share::SCN &checkpoint_scn);
  int get_or_create_ddl_kv(
      const share::SCN &macro_redo_scn,
      const share::SCN &macro_redo_start_scn,
      ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      ObDDLKVHandle &kv_handle); // used in active ddl kv guard
  int get_freezed_ddl_kv(const share::SCN &freeze_scn, ObDDLKVHandle &kv_handle); // locate ddl kv with exeact freeze log ts
  int get_ddl_kvs(const bool frozen_only, ObIArray<ObDDLKVHandle> &kv_handle_array); // get all freeze ddl kvs
  int get_ddl_kvs_for_query(ObTablet &tablet, ObIArray<ObDDLKVHandle> &kv_handle_array);
  int freeze_ddl_kv(
      const share::SCN &start_scn,
      const int64_t snapshot_version,
      const uint64_t data_format_version,
      const share::SCN &freeze_scn = share::SCN::min_scn()); // freeze the active ddl kv, when memtable freeze or ddl commit
  int release_ddl_kvs(const share::SCN &rec_scn); // release persistent ddl kv, used in ddl merge task for free ddl kv
  int check_has_effective_ddl_kv(bool &has_ddl_kv); // used in ddl log handler for checkpoint
  int try_flush_ddl_commit_scn(
      ObLSHandle &ls_handle,
      const ObTabletHandle &tablet_handle,
      const ObTabletDirectLoadMgrHandle &direct_load_mgr_handle,
      const share::SCN &commit_scn);
  int check_has_freezed_ddl_kv(bool &has_freezed_ddl_kv);
  int64_t get_count();
  void set_ddl_kv(const int64_t idx, ObDDLKVHandle &kv_handle); //for unittest
  OB_INLINE void inc_ref() { ATOMIC_INC(&ref_cnt_); }
  OB_INLINE int64_t dec_ref() { return ATOMIC_SAF(&ref_cnt_, 1 /* just sub 1 */); }
  OB_INLINE int64_t get_ref() const { return ATOMIC_LOAD(&ref_cnt_); }
  OB_INLINE void reset() { destroy(); }
  int get_rec_scn(share::SCN &rec_scn); // when data persisted, should return INT64_MAX
  ObTabletID get_tablet_id() { return tablet_id_; }
  int online();
  int cleanup();
  bool can_freeze();
  TO_STRING_KV(K_(is_inited), K_(ls_id), K_(tablet_id),
      K_(max_freeze_scn),
      K_(head), K_(tail), K_(ref_cnt));

private:
  int64_t get_idx(const int64_t pos) const;
  int alloc_ddl_kv(
    const share::SCN &start_scn,
    const int64_t snapshot_version,
    const uint64_t data_format_version,
    ObDDLKVHandle &kv_handle);
  void free_ddl_kv(const int64_t idx);
  int get_active_ddl_kv_impl(ObDDLKVHandle &kv_handle);
  void try_get_ddl_kv_unlock(const share::SCN &scn, ObDDLKVHandle &kv_handle);
  int get_ddl_kvs_unlock(const bool frozen_only, ObIArray<ObDDLKVHandle> &kv_handle_array);
  int64_t get_count_nolock() const;
  int get_ddl_kv_min_scn(share::SCN &min_scn); // for calculate rec_scn of ls
  int create_empty_ddl_sstable(common::ObArenaAllocator &allocator, blocksstable::ObSSTable &sstable);
  void destroy();
  int rdlock(const int64_t timeout_us, uint32_t &lock_tid);
  int wrlock(const int64_t timeout_us, uint32_t &lock_tid);
  void unlock(const uint32_t lock_tid);
  void cleanup_unlock();
public:
  static const int64_t MAX_DDL_KV_CNT_IN_STORAGE = 16;
  static const int64_t TRY_LOCK_TIMEOUT = 10 * 1000000; // 10s
private:
  bool is_inited_;
  share::ObLSID ls_id_;
  common::ObTabletID tablet_id_;
  share::SCN max_freeze_scn_;
  ObDDLKVHandle ddl_kv_handles_[MAX_DDL_KV_CNT_IN_STORAGE];

  int64_t head_;
  int64_t tail_;
  common::ObLatch lock_;

  volatile int64_t ref_cnt_ CACHE_ALIGNED;
  DISALLOW_COPY_AND_ASSIGN(ObTabletDDLKvMgr);
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_PG_DDL_KV_MGR_H

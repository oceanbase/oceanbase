/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_MGR_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_MGR_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/lock/ob_spin_lock.h"
#include "storage/ob_handle_cache.h"
#include "storage/fts/dict/ob_ft_slot_mgr.h"
#include "storage/fts/dict/ob_ft_dict_bg_task_mgr.h"

namespace oceanbase
{
namespace storage
{

// Single MTL instance: build slot and row_scn cache.
class ObFTDictMgr
{
public:
  // Cached stamp (row_scn, row_count, snapshot_version) for dict table change detection.
  struct DictTableSnapshot
  {
    int64_t get_handle_size() const { return sizeof(DictTableSnapshot); }
    void reset() {
      row_scn_ = 0;
      row_count_ = 0;
      snapshot_version_ = 0;
    }
    TO_STRING_KV(K_(row_scn), K_(row_count), K_(snapshot_version));
    int64_t row_scn_;
    int64_t row_count_;
    int64_t snapshot_version_;
  };

  static constexpr int64_t ROW_SCN_CACHE_SIZE = 32;

public:

  ObFTDictMgr();
  ~ObFTDictMgr();

  static int mtl_init(ObFTDictMgr *&mgr);

  int init();
  int start();
  void stop();
  void wait();
  void destroy();

  void push_access_row_scn_cache_task(const uint64_t table_id);

  int acquire_slot(const uint64_t table_id, int64_t &slot_idx) { return slot_mgr_.acquire_slot(table_id, slot_idx); }
  void release_slot(const int64_t slot_idx) { slot_mgr_.release_slot(slot_idx); }
  int wait_build_complete(const uint64_t table_id, const int64_t slot_idx) { return slot_mgr_.wait_build_complete(table_id, slot_idx); }
  int get_table_snapshot(const uint64_t table_id, DictTableSnapshot &snapshot);
  int update_table_snapshot(const uint64_t table_id, DictTableSnapshot &snapshot);

  TO_STRING_KV(K_(is_inited), K(get_row_scn_cache_hold()));

private:
  friend class ObFTDictRefreshTask;

private:
  int collect_table_ids(uint64_t *table_ids, int64_t &table_id_cnt) const;
  int64_t get_row_scn_cache_hold() const { return row_scn_cache_.hold(); }

private:
  ObFTSlotMgr slot_mgr_;
  mutable common::ObSpinLock cache_lock_;
  ObHandleCache<uint64_t, DictTableSnapshot, ROW_SCN_CACHE_SIZE> row_scn_cache_;
  ObFTDictRefreshTaskMgr refresh_task_mgr_;
  ObFTAccessRowScnCacheTaskMgr access_cache_mgr_;
  bool is_started_;
  bool is_inited_;

  DISALLOW_COPY_AND_ASSIGN(ObFTDictMgr);
};

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_MGR_H_

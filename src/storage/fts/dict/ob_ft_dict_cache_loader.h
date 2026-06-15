/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_CACHE_LOADER_H_
#define _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_CACHE_LOADER_H_

#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/fts/dict/ob_ft_cache_container.h"
#include "storage/fts/dict/ob_ft_dict_def.h"
#include "storage/fts/dict/ob_ft_dict_iterator.h"
#include "storage/fts/dict/ob_ft_dict_mgr.h"

namespace oceanbase
{
namespace storage
{

// Missing range information for partial load
struct ObMissingRangeInfo final
{
  ObFTSingleToken start_token_;
  ObFTSingleToken end_token_;
  int32_t start_range_id_;
  int64_t range_count_;
  int64_t snapshot_version_;

  TO_STRING_KV("start_token", start_token_.get_token(), "end_token", end_token_.get_token(),
               K_(start_range_id), K_(range_count), K_(snapshot_version));
};

// Base class for cache loading strategies
class ObFTDictCacheLoaderBase
{
public:
  ObFTDictCacheLoaderBase();
  virtual ~ObFTDictCacheLoaderBase() = default;

  // Main entry point for loading cache
  virtual int load_cache(const ObFTDictDesc &desc,
                        ObFTCacheRangeContainer &range_container) = 0;

protected:
  // Methods used by all three subclasses
  int do_full_build(const ObFTDictDesc &desc,
                    ObFTCacheRangeContainer &range_container);
  int build_ranges_from_table(const ObFTDictDesc &desc,
                              const int64_t snapshot_version,
                              ObFTCacheRangeContainer &range_container,
                              const ObIArray<ObMissingRangeInfo> *partial_ranges = nullptr);
  int get_first_range_handle(const ObFTCacheRangeContainer &range_container,
                             ObFTCacheRangeHandle *&first_handle);
  int update_first_range_cache(const ObFTDictDesc &desc,
                               const ObFTDAT *dat_block,
                               const int64_t snapshot_version,
                               const int32_t range_count,
                               ObFTCacheRangeContainer &range_container);
  int get_table_row_scn_and_count(const ObFTDictDesc &desc,
                                  const int64_t snapshot_version,
                                  int64_t &row_scn,
                                  int64_t &row_count);

  // Methods used by DDL and Exec
  int try_load_cache(const ObFTDictDesc &desc,
                     ObFTCacheRangeContainer &range_container,
                     ObIArray<ObMissingRangeInfo> *missing_ranges = nullptr);

  // Methods used by Refresh and DDL
  int check_row_scn(const ObFTDictDesc &desc,
                     bool &is_need_build,
                     ObFTDictMgr::DictTableSnapshot &row_scn_info);
  int update_snapshot_version(const ObFTDictDesc &desc,
                              const int64_t new_snapshot_version,
                              ObFTCacheRangeContainer &range_container);

  int refresh_cache_common(const ObFTDictDesc &desc,
                           ObFTDictMgr::DictTableSnapshot &row_scn_info,
                           ObFTCacheRangeContainer &range_container);

  virtual int validate_cache(const ObFTDictDesc &desc,
                              ObFTCacheRangeContainer &range_container,
                              bool &is_valid);

private:
  // Internal helper methods
  int get_current_snapshot_version(int64_t &snapshot_version);
  int build_ranges(const ObFTDictDesc &desc,
                   ObIFTDictIterator &iter,
                   ObFTCacheRangeContainer &range_container,
                   const int64_t snapshot_version,
                   const ObIArray<ObMissingRangeInfo> *partial_ranges = nullptr);
  int build_one_range(const ObFTDictDesc &desc,
                      const int32_t range_id,
                      ObIFTDictIterator &iter,
                      ObFTCacheRangeContainer &container,
                      bool &build_next_range,
                      ObIAllocator &persistent_alloc,
                      ObFTDAT *&pending_dat_buff);
  int collect_missing_ranges(const ObFTSingleToken &start_token,
                             const ObFTSingleToken &end_token,
                             const int32_t start_range_id,
                             const int64_t range_count,
                             const int64_t snapshot_version,
                             ObIArray<ObMissingRangeInfo> *missing_ranges);
  int handle_missing_ranges(const ObFTCacheRangeContainer &range_container,
                            const int32_t range_count,
                            const int64_t snapshot_version,
                            ObIArray<ObMissingRangeInfo> *missing_ranges);

protected:
  // RAII wrapper for slot management
  class ObFTSlotGuard
  {
  public:
    ObFTSlotGuard(ObFTDictMgr *dict_mgr, uint64_t table_id)
        : dict_mgr_(dict_mgr), table_id_(table_id), slot_idx_(-1), acquired_(false) {}

    ~ObFTSlotGuard()
    {
      if (acquired_) {
        dict_mgr_->release_slot(slot_idx_);
      }
    }

    int acquire()
    {
      int ret = OB_SUCCESS;
      if (OB_SUCC(dict_mgr_->acquire_slot(table_id_, slot_idx_))) {
        acquired_ = true;
      }
      return ret;
    }

    int wait_build_complete()
    {
      return dict_mgr_->wait_build_complete(table_id_, slot_idx_);
    }

    bool is_acquired() const { return acquired_; }

  private:
    ObFTDictMgr *dict_mgr_;
    uint64_t table_id_;
    int64_t slot_idx_;
    bool acquired_;
  };

protected:
  ObFTDictMgr *dict_mgr_;
  uint64_t tenant_id_;
};

// for DDL
class ObFTDictCacheLoaderDDL final : public ObFTDictCacheLoaderBase
{
public:
  int load_cache(const ObFTDictDesc &desc,
                 ObFTCacheRangeContainer &range_container) override;

private:
  int build_and_fetch_cache(const ObFTDictDesc &desc,
                             ObFTDictMgr::DictTableSnapshot &row_scn_info,
                             ObFTCacheRangeContainer &range_container);
};

// for DML/SELECT
class ObFTDictCacheLoaderExec final : public ObFTDictCacheLoaderBase
{
public:
  int load_cache(const ObFTDictDesc &desc,
                 ObFTCacheRangeContainer &range_container) override;

private:
  int check_snapshot_version(const ObFTDictDesc &desc,
                             const int64_t snapshot_version,
                             bool &is_need_reload_cache);
  int do_partial_build(const ObFTDictDesc &desc,
                       ObFTCacheRangeContainer &range_container,
                       ObIArray<ObMissingRangeInfo> &missing_ranges,
                       bool &is_need_full_build);
};

// for refresh
class ObFTDictCacheLoaderRefresh final : public ObFTDictCacheLoaderBase
{
public:
  int load_cache(const ObFTDictDesc &desc,
                 ObFTCacheRangeContainer &range_container) override;
  int load_cache(const ObFTDictDesc &desc);

protected:
  int validate_cache(const ObFTDictDesc &desc,
                      ObFTCacheRangeContainer &range_container,
                      bool &is_valid) override;

private:
  int check_cache_valid(const ObFTDictDesc &desc,
                        bool &is_valid,
                        ObFTCacheRangeContainer *range_container = nullptr);
  int build_and_refresh_cache(const ObFTDictDesc &desc,
                               ObFTDictMgr::DictTableSnapshot &row_scn_info);
};

} // namespace storage
} // namespace oceanbase

#endif // _OCEANBASE_STORAGE_FTS_DICT_OB_FT_DICT_CACHE_LOADER_H_

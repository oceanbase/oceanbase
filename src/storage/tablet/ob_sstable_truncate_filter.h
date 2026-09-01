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

#ifndef OCEANBASE_STORAGE_OB_SSTABLE_TRUNCATE_FILTER_H
#define OCEANBASE_STORAGE_OB_SSTABLE_TRUNCATE_FILTER_H

#include "lib/container/ob_iarray.h"
#include "lib/utility/ob_print_utils.h"
#include "share/scn.h"

namespace oceanbase
{
namespace storage
{
class ObITable;
class ObTablet;
class ObUpdateTableStoreParam;
class ObBatchUpdateTableStoreParam;

// Filter SSTable candidates by the tablet's truncate MDS data when building or
// updating a table store (merge / transfer / split). Used to drop SSTables whose
// data has been logically truncated.
//
// Rules:
//   - major / meta_major sstable : drop if snapshot_version < truncate_commit_version_
//   - minor / ddl sstable        : drop if end_scn   <= truncate_commit_scn_
//                                  keep if end_scn > truncate_commit_scn_
//   - mds sstable                : never filtered (carries the truncate info itself)
//
// The filter is a value-semantic, lock-free helper. Constructed by the scheduler
// layer (which already holds a non-const tablet) and passed by const reference
// into table store interfaces.
class ObSSTableTruncateFilter final
{
public:
  static ObSSTableTruncateFilter dummy_filter();

public:
  ObSSTableTruncateFilter();
  ~ObSSTableTruncateFilter() = default;

  int init(const share::SCN &truncate_commit_scn,
           const int64_t truncate_commit_version);
  void reset();
  bool is_inited() const { return is_inited_; }
  const share::SCN &get_truncate_commit_scn() const { return truncate_commit_scn_; }
  int64_t get_truncate_commit_version() const { return truncate_commit_version_; }
  /// Check whether the merge result sstable in @a param would be dropped by the
  /// truncate filter. If so, return OB_NO_NEED_MERGE so the caller can skip the
  /// table store update entirely.
  ///
  /// @param[in] param  The merge update param whose sstable_ is checked.
  /// @retval OB_SUCCESS        The merge is needed (sstable would be kept, or
  ///                           sstable_ is NULL / no truncate data).
  /// @retval OB_NO_NEED_MERGE  The merge result would be filtered out.
  int check_if_need_merge(const ObUpdateTableStoreParam &param) const;
  /// Rebuild @a new_param from @a param by filtering out SSTables that have been
  /// logically truncated. Only applies to transfer-replace scenarios
  /// (is_transfer_replace_ == true); for non-transfer HA replace the param is
  /// copied as-is.
  ///
  /// @param[in]  param      The original batch update param.
  /// @param[out] new_param  The rebuilt param with truncated SSTables removed.
  int rebuild_param_for_transfer_replace(
      ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const ObBatchUpdateTableStoreParam &param,
      ObBatchUpdateTableStoreParam &new_param) const;
  TO_STRING_KV(K_(truncate_commit_scn),
               K_(truncate_commit_version),
               K_(is_inited));

private:
  bool need_filter_() const;
  int check_sstable_(const ObITable &table, bool &should_keep) const;
  int create_empty_major_for_new_param_(
      ObArenaAllocator &allocator,
      const ObTablet &tablet,
      const int64_t snapshot_version,
      ObBatchUpdateTableStoreParam &new_param) const;

private:
  share::SCN truncate_commit_scn_;
  int64_t truncate_commit_version_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_SSTABLE_TRUNCATE_FILTER_H

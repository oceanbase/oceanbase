//Copyright (c) 2025 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_STORAGE_TRANS_VERSION_SKIP_INDEX_UTIL_H_
#define OB_STORAGE_TRANS_VERSION_SKIP_INDEX_UTIL_H_
#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "storage/blocksstable/index_block/ob_index_block_row_struct.h"
namespace oceanbase
{
namespace blocksstable
{
struct ObMacroBlockDesc;
struct ObMicroBlock;
struct ObStorageDatum;
class ObIndexBlockAggregator;
}
namespace storage
{

struct ObTransVersionSkipIndexInfo
{
  ObTransVersionSkipIndexInfo()
    : min_snapshot_(0),
      max_snapshot_(INT64_MAX)
  {}
  ObTransVersionSkipIndexInfo(const int64_t min_snapshot, const int64_t max_snapshot)
    : min_snapshot_(min_snapshot),
      max_snapshot_(max_snapshot)
  {}
  OB_INLINE void reset() {
    min_snapshot_ = 0;
    max_snapshot_ = INT64_MAX;
  }
  OB_INLINE bool is_valid() const { return min_snapshot_ <= max_snapshot_; }
  // if skip index MAX_SNAPSHOT is INT64_MAX, could use block_meta to fill max_snapshot
  OB_INLINE bool is_inited() const { return max_snapshot_ != INT64_MAX; }
  int set(const blocksstable::ObStorageDatum &min_datum, const blocksstable::ObStorageDatum &max_datum);
  int set(const blocksstable::ObStorageDatum *min_datum, const blocksstable::ObStorageDatum *max_datum);
  OB_INLINE void set(const int64_t min_snapshot, const int64_t max_snapshot)
  {
    min_snapshot_ = min_snapshot;
    max_snapshot_ = max_snapshot;
  }
  TO_STRING_KV(K_(min_snapshot), K_(max_snapshot));
  int64_t min_snapshot_;
  int64_t max_snapshot_;
};

struct ObTransVersionSkipIndexReader {
public:
  static int read_min_max_snapshot(
    const blocksstable::ObMacroBlockDesc &macro_desc,
    const int64_t trans_version_col_idx,
    ObTransVersionSkipIndexInfo &skip_index_info);
  static int read_min_max_snapshot(
    const blocksstable::ObMicroBlock &micro_block,
    const int64_t trans_version_col_idx,
    ObTransVersionSkipIndexInfo &skip_index_info);
  static int read_min_max_snapshot(
    const blocksstable::ObIndexBlockAggregator &aggregator,
    const int64_t trans_version_col_idx,
    ObTransVersionSkipIndexInfo &skip_index_info);
  static int read_min_max_snapshot(
    const blocksstable::ObMicroIndexInfo &micro_index_info,
    const int64_t trans_version_col_idx,
    ObTransVersionSkipIndexInfo &skip_index_info);
private:
  static int inner_read_min_max_snapshot(
    const int64_t col_idx,
    const char *buf,
    const int64_t buf_size,
    ObTransVersionSkipIndexInfo &skip_index_info);
};

} // namespace storage
} // namespace oceanbase

#endif // OB_STORAGE_TRANS_VERSION_SKIP_INDEX_UTIL_H_

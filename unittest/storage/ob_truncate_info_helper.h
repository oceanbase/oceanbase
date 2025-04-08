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

#ifndef OCEANBASE_UNITTEST_TRUNCATE_INFO_HELPER
#define OCEANBASE_UNITTEST_TRUNCATE_INFO_HELPER

#include <stdint.h>
#define protected public
#define private public

namespace oceanbase
{
namespace common
{
class ObIAllocator;
class ObArenaAllocator;
class ObRowkey;
class ObTabletID;
struct ObVersionRange;
}
namespace share
{
class ObLSID;
}
namespace storage
{
struct ObTruncateInfo;
struct ObTruncatePartition;
struct ObTruncateInfoArray;
class ObTablet;
class ObTabletHandle;
class TruncateInfoHelper
{
public:
  enum MockTruncateInfoCol : uint8_t
  {
    TRANS_ID = 0,
    COMMIT_VERSION,
    SCHEMA_VERSION,
    LOWER_BOUND,
    UPPER_BOUND,
    COL_MAX
  };
  static void mock_truncate_info(
    common::ObIAllocator &allocator,
    const int64_t trans_id,
    const int64_t schema_version,
    ObTruncateInfo &info);
  static void mock_truncate_info(
    common::ObIAllocator &allocator,
    const int64_t trans_id,
    const int64_t schema_version,
    const int64_t commit_version,
    ObTruncateInfo &info);
  static int mock_truncate_partition(
    common::ObIAllocator &allocator,
    const int64_t low_bound_val,
    const int64_t high_bound_val,
    ObTruncatePartition &part);
  static int mock_truncate_partition(
    common::ObIAllocator &allocator,
    const common::ObRowkey &low_bound_val,
    const common::ObRowkey &high_bound_val,
    ObTruncatePartition &part);
  static int mock_truncate_partition(
    common::ObIAllocator &allocator,
    const int64_t *list_val,
    const int64_t list_val_cnt,
    ObTruncatePartition &part);
  static int get_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle);
  static int mock_part_key_idxs(
    common::ObIAllocator &allocator,
    const int64_t cnt,
    const int64_t *col_idxs,
    ObTruncatePartition &part);
  static int mock_part_key_idxs(
    common::ObIAllocator &allocator,
    const int64_t col_idx,
    ObTruncatePartition &part)
    {
      return mock_part_key_idxs(allocator, 1, &col_idx, part);
    }
  static int read_distinct_truncate_info_array(
    common::ObArenaAllocator &allocator,
    ObTablet &tablet,
    const common::ObVersionRange &read_version_range,
    storage::ObTruncateInfoArray &truncate_info_array);
  static int batch_mock_truncate_info(
    common::ObArenaAllocator &allocator,
    const char *key_data,
    storage::ObTruncateInfoArray &truncate_info_array);
};

} // storage
} // oceanbase

#endif // OCEANBASE_UNITTEST_TRUNCATE_INFO_HELPER

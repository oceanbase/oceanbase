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

#ifndef OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_READER
#define OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_READER

#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"
#include "storage/tablet/ob_mds_range_query_iterator.h"
#include "storage/multi_data_source/mds_table_iterator.h"

namespace oceanbase
{
namespace storage
{
class ObTabletMediumInfoReader
{
public:
  ObTabletMediumInfoReader();
  ~ObTabletMediumInfoReader();
public:
  int init(
      const ObTablet &tablet,
      ObTableScanParam &scan_param);
  int get_next_medium_info(
      common::ObIAllocator &allocator,
      compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);
  int get_specified_medium_info(
      common::ObIAllocator &allocator,
      const compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);

  int get_min_medium_snapshot(
      const int64_t last_major_snapshot_version,
      int64_t &min_medium_snapshot);
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  ObStoreCtx store_ctx_;
  ObMdsRangeQueryIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> iter_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_READER

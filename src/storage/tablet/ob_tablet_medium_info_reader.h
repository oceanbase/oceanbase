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

#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/tablet/ob_tablet_dumped_medium_info.h"

namespace oceanbase
{
namespace storage
{
class ObTabletDumpedMediumInfo;

class ObTabletMediumInfoReader
{
public:
  ObTabletMediumInfoReader(const ObTablet &tablet);
  ~ObTabletMediumInfoReader();
public:
  int init(common::ObArenaAllocator &allocator);
  void reset();

  // TODO(@bowen.gbw): filter uncommitted node
  int get_next_medium_info(
      common::ObIAllocator &allocator,
      compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);
  int get_specified_medium_info(
      common::ObIAllocator &allocator,
      const compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);

  int get_min_medium_snapshot(int64_t &min_medium_snapshot);
  int get_max_medium_snapshot(int64_t &max_medium_snapshot);
private:
  int advance_mds_iter();
  int advance_dump_iter();
  int output_medium_info_from_mds_iter(
      common::ObIAllocator &allocator,
      compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);
  int output_medium_info_from_dump_iter(
      common::ObIAllocator &allocator,
      compaction::ObMediumCompactionInfoKey &key,
      compaction::ObMediumCompactionInfo &medium_info);
private:
  bool is_inited_;
  const ObTablet &tablet_;
  common::ObArenaAllocator *allocator_;

  // mds iter will hold mds table handle
  mds::ObMdsUnitRowNodeScanIterator<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> mds_iter_;
  ObTabletDumpedMediumInfoIterator dump_iter_;
  compaction::ObMediumCompactionInfoKey mds_key_;
  mds::UserMdsNode<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo> *mds_node_;
  compaction::ObMediumCompactionInfoKey dump_key_;
  const compaction::ObMediumCompactionInfo *dump_medium_info_;
  bool mds_end_;
  bool dump_end_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_MEDIUM_INFO_READER

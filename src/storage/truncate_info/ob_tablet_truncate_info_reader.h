/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_INFO_READER
#define OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_INFO_READER

#include "storage/tablet/ob_mds_cl_range_query_iterator.h"
#include "storage/truncate_info/ob_truncate_info.h"

namespace oceanbase
{
namespace storage
{

template <typename Key, typename Value>
class ObTabletMDSInfoReader final
{
public:
  ObTabletMDSInfoReader() : is_inited_(false), allocator_("mds_info_reader"), iter_() {}

public:
  int init(const ObTablet &tablet, ObTableScanParam &scan_param);

  int get_next_mds_kv(ObIAllocator &allocator, Key &key, Value &value);

  int get_next_mds_kv(ObIAllocator &allocator, mds::MdsDumpKV *&kv);

private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  ObMdsRangeQueryIterator<Key, Value> iter_;
};

using ObTabletTruncateInfoReader = ObTabletMDSInfoReader<ObTruncateInfoKey, ObTruncateInfo>;

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_TRUNCATE_INFO_READER

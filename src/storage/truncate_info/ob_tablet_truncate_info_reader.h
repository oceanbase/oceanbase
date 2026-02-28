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

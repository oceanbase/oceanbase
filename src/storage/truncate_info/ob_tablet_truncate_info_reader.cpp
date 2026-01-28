/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/truncate_info/ob_tablet_truncate_info_reader.h"
#include "storage/compaction_ttl/ob_tablet_ttl_filter_info_reader.h"
#include "storage/tablet/ob_tablet.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{

template <typename Key, typename Value>
int ObTabletMDSInfoReader<Key, Value>::init(const ObTablet &tablet, ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Init twice", KR(ret));
  } else if (OB_FAIL(tablet.mds_range_query(scan_param, iter_))) {
    LOG_WARN("Fail to init mds range query iterator", KR(ret), K(scan_param), K(tablet));
  } else {
    is_inited_ = true;
  }

  return ret;
}

template <typename Key, typename Value>
int ObTabletMDSInfoReader<Key, Value>::get_next_mds_kv(ObIAllocator &allocator,
                                                       Key &key,
                                                       Value &value)
{
  int ret = OB_SUCCESS;

  key.reset();
  value.destroy();

  mds::MdsDumpKV *kv = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret));
  } else if (OB_FAIL(iter_.get_next_mds_kv(allocator_, kv))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Fail to get next mds kv", KR(ret));
    }
  } else {
    const ObString &key_str = kv->k_.key_;
    const ObString &value_str = kv->v_.user_data_;
    int64_t key_pos = 0;
    int64_t value_pos = 0;
    if (OB_FAIL(key.mds_deserialize(key_str.ptr(), key_str.length(), key_pos))) {
      LOG_WARN("Fail to deserialize key", KR(ret), K(key_str));
    } else if (OB_FAIL(value.deserialize(allocator, value_str.ptr(), value_str.length(), value_pos))) {
      LOG_WARN("Fail to deserialize value", KR(ret), K(value_str));
    }
  }

  // always free mds kv and reuse memory
  iter_.free_mds_kv(allocator_, kv);
  allocator_.reuse();

  return ret;
}

template <typename Key, typename Value>
int ObTabletMDSInfoReader<Key, Value>::get_next_mds_kv(ObIAllocator &allocator, mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;

  kv = nullptr;

  if (OB_FAIL(iter_.get_next_mds_kv(allocator, kv))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("Fail to get next mds kv", KR(ret));
    }
  }

  return ret;
}

template class ObTabletMDSInfoReader<ObTruncateInfoKey, ObTruncateInfo>;
template class ObTabletMDSInfoReader<ObTTLFilterInfoKey, ObTTLFilterInfo>;

} // namespace storage
} // namespace oceanbase

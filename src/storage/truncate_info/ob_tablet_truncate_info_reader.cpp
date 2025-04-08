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
#include "lib/ob_errno.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_mds_data.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletTruncateInfoReader::ObTabletTruncateInfoReader()
  : is_inited_(false),
    allocator_("mds_range_iter"),
    iter_()
{
}

ObTabletTruncateInfoReader::~ObTabletTruncateInfoReader()
{
}

int ObTabletTruncateInfoReader::init(
    const ObTablet &tablet,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const common::ObTabletID &tablet_id = tablet.get_tablet_id();
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL((tablet.mds_range_query<ObTruncateInfoKey, ObTruncateInfo>(
      scan_param,
      iter_)))) {
    LOG_WARN("fail to do build query range iter", K(ret), K(ls_id), K(tablet_id), K(scan_param));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTabletTruncateInfoReader::get_next_truncate_info(
    common::ObIAllocator &allocator,
    ObTruncateInfoKey &key,
    ObTruncateInfo &truncate_info)
{
  int ret = OB_SUCCESS;
  key.reset();
  truncate_info.destroy();
  mds::MdsDumpKV *kv = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K_(is_inited));
  } else if (OB_FAIL(iter_.get_next_mds_kv(allocator_, kv))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("iter end", K(ret));
    } else {
      LOG_WARN("fail to get next mds kv", K(ret));
    }
  } else {
    const common::ObString &key_str = kv->k_.key_;
    const common::ObString &node_str = kv->v_.user_data_;
    int64_t key_pos = 0;
    int64_t node_pos = 0;
    if (OB_FAIL(key.mds_deserialize(key_str.ptr(), key_str.length(), key_pos))) {
      LOG_WARN("fail to deserialize key", K(ret), K(key_str), KPC(kv));
    } else if (OB_FAIL(truncate_info.deserialize(allocator, node_str.ptr(), node_str.length(), node_pos))) {
      LOG_WARN("fail to deserialize truncate info", K(ret), KPC(kv));
    }
  }

  // always free mds kv and reuse memory
  iter_.free_mds_kv(allocator_, kv);
  allocator_.reuse();

  return ret;
}

int ObTabletTruncateInfoReader::get_next_mds_kv(
    common::ObIAllocator &allocator,
    mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  if (OB_FAIL(iter_.get_next_mds_kv(allocator, kv))) {
    if (OB_ITER_END == ret) {
      LOG_DEBUG("iter end", K(ret));
    } else {
      LOG_WARN("fail to get next mds kv", K(ret));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

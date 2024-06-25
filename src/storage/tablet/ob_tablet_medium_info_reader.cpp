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

#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "lib/ob_errno.h"
#include "storage/compaction/ob_medium_compaction_info.h"
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
ObTabletMediumInfoReader::ObTabletMediumInfoReader()
  : is_inited_(false),
    allocator_("mds_range_iter"),
    store_ctx_(),
    iter_()
{
}

ObTabletMediumInfoReader::~ObTabletMediumInfoReader()
{
}

int ObTabletMediumInfoReader::init(
    const ObTablet &tablet,
    ObTableScanParam &scan_param)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_ls_id();
  const common::ObTabletID &tablet_id = tablet.get_tablet_id();

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL((tablet.mds_range_query<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
      scan_param,
      store_ctx_,
      iter_)))) {
    LOG_WARN("fail to do build query range iter", K(ret), K(ls_id), K(tablet_id), K(scan_param));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTabletMediumInfoReader::get_next_medium_info(
    common::ObIAllocator &allocator,
    compaction::ObMediumCompactionInfoKey &key,
    compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;
  key.reset();
  medium_info.reset();
  mds::MdsDumpKV *kv = nullptr;

  if (OB_FAIL(iter_.get_next_mds_kv(allocator_, kv))) {
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
      LOG_WARN("fail to deserialize key", K(ret));
    } else if (OB_FAIL(medium_info.deserialize(allocator, node_str.ptr(), node_str.length(), node_pos))) {
      LOG_WARN("fail to deserialize medium info", K(ret));
    }
  }

  // always free mds kv and reuse memory
  iter_.free_mds_kv(allocator_, kv);
  allocator_.reuse();

  return ret;
}

int ObTabletMediumInfoReader::get_specified_medium_info(
    common::ObIAllocator &allocator,
    const compaction::ObMediumCompactionInfoKey &key,
    compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  // TODO(@bowen.gbw): in the future we should use ObTablet::get_snapshot interface
  mds::MdsDumpKV *kv = nullptr;
  compaction::ObMediumCompactionInfoKey tmp_key;
  bool found = false;
  int compare_result = 0;

  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(iter_.get_next_mds_kv(allocator_, kv))) {
      if (OB_ITER_END == ret) {
        LOG_DEBUG("iter end", K(ret));
      } else {
        LOG_WARN("fail to get next mds kv", K(ret));
      }
    } else {
      const common::ObString &key_str = kv->k_.key_;
      int64_t pos = 0;
      if (OB_FAIL(tmp_key.mds_deserialize(key_str.ptr(), key_str.length(), pos))) {
        LOG_WARN("fail to deserialize key", K(ret));
      } else if (OB_FAIL(mds::compare_binary_key(tmp_key, key, compare_result))) {
        LOG_WARN("fail to comapre binary key", K(ret), K(tmp_key), K(key));
      } else if (compare_result < 0) {
        // do nothing
      } else if (compare_result > 0) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("medium info doest not exist", K(ret), K(tmp_key), K(key));
      } else if (compare_result == 0) {
        const common::ObString &node_str = kv->v_.user_data_;
        pos = 0;
        if (OB_FAIL(medium_info.deserialize(allocator, node_str.ptr(), node_str.length(), pos))) {
          LOG_WARN("fail to deserialize medium info", K(ret));
        } else {
          found = true;
        }
      }

      // always free mds kv and reuse memory
      iter_.free_mds_kv(allocator_, kv);
      allocator_.reuse();
    }
  }

  return ret;
}

int ObTabletMediumInfoReader::get_min_medium_snapshot(
    const int64_t last_major_snapshot_version,
    int64_t &min_medium_snapshot)
{
  int ret = OB_SUCCESS;
  min_medium_snapshot = INT64_MAX;
  mds::MdsDumpKV *kv = nullptr;
  compaction::ObMediumCompactionInfoKey tmp_key;
  bool found = false;

  while (OB_SUCC(ret) && !found) {
    if (OB_FAIL(iter_.get_next_mds_kv(allocator_, kv))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next mds kv", K(ret));
      }
    } else {
      const common::ObString &key_str = kv->k_.key_;
      int64_t pos = 0;
      if (OB_FAIL(tmp_key.mds_deserialize(key_str.ptr(), key_str.length(), pos))) {
        LOG_WARN("fail to deserialize key", K(ret));
      } else if (tmp_key.get_medium_snapshot() > last_major_snapshot_version) {
        min_medium_snapshot = tmp_key.get_medium_snapshot();
        found = true;
      }

      // always free mds kv and reuse memory
      iter_.free_mds_kv(allocator_, kv);
      allocator_.reuse();
    }
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase

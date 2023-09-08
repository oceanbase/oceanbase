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
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_mds_data.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletMediumInfoReader::ObTabletMediumInfoReader(const ObTablet &tablet)
  : is_inited_(false),
    tablet_(tablet),
    allocator_(nullptr),
    mds_iter_(),
    dump_iter_(),
    mds_key_(),
    mds_node_(nullptr),
    dump_key_(),
    dump_medium_info_(nullptr),
    mds_end_(false),
    dump_end_(false)
{
}

ObTabletMediumInfoReader::~ObTabletMediumInfoReader()
{
  reset();
}

int ObTabletMediumInfoReader::init(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet_.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet_.get_tablet_meta().tablet_id_;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(ls_id), K(tablet_id), K_(is_inited));
  } else {
    mds::MdsTableHandle mds_table;
    const ObTabletDumpedMediumInfo *dumped_medium_info = nullptr;
    if (OB_FAIL(tablet_.inner_get_mds_table(mds_table, false/*not_exist_create*/))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_WARN("failed to get mds table", K(ret), K(ls_id), K(tablet_id));
      } else {
        mds_end_ = true; // no mds table, directly iter end
        ret = OB_SUCCESS;
        LOG_DEBUG("no mds table", K(ret), K(ls_id), K(tablet_id), K_(mds_end));
      }
    } else if (OB_FAIL(mds_iter_.init(mds_table))) {
      LOG_WARN("failed to init mds iter", K(ret), K(ls_id), K(tablet_id));
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObTabletMdsData::load_medium_info_list(allocator, tablet_.mds_data_.medium_info_list_, dumped_medium_info))) {
      LOG_WARN("failed to load medium info list", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(dump_iter_.init(allocator, dumped_medium_info))) {
      LOG_WARN("failed to init dumped iter", K(ret), K(ls_id), K(tablet_id));
    } else {
      allocator_ = &allocator;
    }

    ObTabletMdsData::free_medium_info_list(allocator, dumped_medium_info);
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  return ret;
}

void ObTabletMediumInfoReader::reset()
{
  dump_end_ = false;
  mds_end_ = false;
  dump_medium_info_ = nullptr;
  dump_key_.reset();
  mds_node_ = nullptr;
  mds_key_.reset();
  dump_iter_.reset();
  allocator_ = nullptr;
  is_inited_ = false;
  // TODO(@xianzhi) clear mds table lock
}

int ObTabletMediumInfoReader::get_next_medium_info(
    common::ObIAllocator &allocator,
    compaction::ObMediumCompactionInfoKey &key,
    compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    if (OB_FAIL(ret)) {
    } else if (!mds_key_.is_valid()) {
      if (OB_FAIL(advance_mds_iter())) {
        LOG_WARN("failed to advance mds iter", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (!dump_key_.is_valid()) {
      if (OB_FAIL(advance_dump_iter())) {
        LOG_WARN("failed to advance dump iter", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (mds_end_ && dump_end_) {
      // both end, just output iter end
      ret = OB_ITER_END;
      LOG_DEBUG("iter end", K(ret));
    } else if (mds_end_) {
      // mds end, output dump node
      if (OB_FAIL(output_medium_info_from_dump_iter(allocator, key, medium_info))) {
        LOG_WARN("failed to output medium info", K(ret));
      } else if (OB_FAIL(advance_dump_iter())) {
        LOG_WARN("failed to advance dump iter", K(ret));
      }
    } else if (dump_end_) {
      // dump end, output mds node
      if (OB_FAIL(output_medium_info_from_mds_iter(allocator, key, medium_info))) {
        LOG_WARN("failed to output medium info", K(ret));
      } else if (OB_FAIL(advance_mds_iter())) {
        LOG_WARN("failed to advance mds iter", K(ret));
      }
    } else if (OB_UNLIKELY(!mds_key_.is_valid() || !dump_key_.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, mds key and dump key should both be valid", K(ret), K_(mds_key), K_(dump_key));
    } else if (dump_key_ < mds_key_) {
      // output dump node
      if (OB_FAIL(output_medium_info_from_dump_iter(allocator, key, medium_info))) {
        LOG_WARN("failed to output medium info", K(ret));
      } else if (OB_FAIL(advance_dump_iter())) {
        LOG_WARN("failed to advance dump iter", K(ret));
      }
    } else if (dump_key_ == mds_key_) {
      // output mds node
      if (OB_FAIL(output_medium_info_from_mds_iter(allocator, key, medium_info))) {
        LOG_WARN("failed to output medium info", K(ret));
      } else if (OB_FAIL(advance_mds_iter())) {
        LOG_WARN("failed to advance mds iter", K(ret));
      } else if (OB_FAIL(advance_dump_iter())) {
        LOG_WARN("failed to advance dump iter", K(ret));
      }
    } else {
      // output mds node
      if (OB_FAIL(output_medium_info_from_mds_iter(allocator, key, medium_info))) {
        LOG_WARN("failed to output medium info", K(ret));
      } else if (OB_FAIL(advance_mds_iter())) {
        LOG_WARN("failed to advance mds iter", K(ret));
      }
    }
  }

  return ret;
}

// temp solution, TODO(@xianzhi)
int ObTabletMediumInfoReader::get_specified_medium_info(
    common::ObIAllocator &allocator,
    const compaction::ObMediumCompactionInfoKey &input_key,
    compaction::ObMediumCompactionInfo &input_medium_info)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  compaction::ObMediumCompactionInfoKey tmp_key;
  compaction::ObMediumCompactionInfo tmp_medium_info;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_medium_info(tmp_allocator, tmp_key, tmp_medium_info))) {
      if (OB_ITER_END == ret) {
        ret = OB_ENTRY_NOT_EXIST;
        break;
      } else {
        LOG_WARN("failed to get medium info", K(ret));
      }
    } else if (OB_UNLIKELY(tmp_key > input_key)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("medium info not exist", K(ret), K(tmp_key), K(input_key));
    } else if (tmp_key == input_key) {
      if (OB_FAIL(input_medium_info.init(allocator, tmp_medium_info))) {
        LOG_WARN("failed to init medium info", K(ret));
      } else {
        break;
      }
    }
    tmp_medium_info.reset();
  } // end of while
  return ret;
}

// temp solution, TODO(@xianzhi)
int ObTabletMediumInfoReader::get_min_medium_snapshot(int64_t &min_medium_snapshot)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  compaction::ObMediumCompactionInfoKey tmp_key;
  compaction::ObMediumCompactionInfo tmp_medium_info;
  min_medium_snapshot = INT64_MAX;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_medium_info(tmp_allocator, tmp_key, tmp_medium_info))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get medium info", K(ret));
      }
    } else {
      min_medium_snapshot = tmp_key.get_medium_snapshot();
      break;
    }
  } // end of while
  return ret;
}

// temp solution!!!, TODO(@xianzhi)
int ObTabletMediumInfoReader::get_max_medium_snapshot(int64_t &max_medium_snapshot)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator;
  compaction::ObMediumCompactionInfoKey tmp_key;
  compaction::ObMediumCompactionInfo tmp_medium_info;
  max_medium_snapshot = 0;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(get_next_medium_info(tmp_allocator, tmp_key, tmp_medium_info))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("failed to get medium info", K(ret));
      }
    } else {
      max_medium_snapshot = tmp_key.get_medium_snapshot();
    }
    tmp_medium_info.reset();
  } // end of while
  return ret;
}

int ObTabletMediumInfoReader::advance_mds_iter()
{
  int ret = OB_SUCCESS;

  if (mds_end_) {
  } else if (OB_FAIL(mds_iter_.get_next(mds_key_, mds_node_))) {
    if (OB_ITER_END == ret) {
      mds_end_ = true;
      ret = OB_SUCCESS;
      LOG_DEBUG("mds iter end", K(ret), K_(mds_end));
    } else {
      LOG_WARN("failed to get mds node", K(ret));
    }
  } else {
    LOG_DEBUG("get next mds node", K_(mds_key), K_(mds_node));
  }

  return ret;
}

int ObTabletMediumInfoReader::advance_dump_iter()
{
  int ret = OB_SUCCESS;

  if (dump_end_) {
  } else if (OB_FAIL(dump_iter_.get_next_medium_info(dump_key_, dump_medium_info_))) {
    if (OB_ITER_END == ret) {
      dump_end_ = true;
      ret = OB_SUCCESS;
      LOG_DEBUG("dump iter end", K(ret), K_(dump_end));
    } else {
      LOG_WARN("failed to get dump node", K(ret));
    }
  } else {
    LOG_DEBUG("get next dump node", K_(dump_key), K_(dump_medium_info));
  }

  return ret;
}

int ObTabletMediumInfoReader::output_medium_info_from_mds_iter(
    common::ObIAllocator &allocator,
    compaction::ObMediumCompactionInfoKey &key,
    compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  const compaction::ObMediumCompactionInfo &user_data = mds_node_->user_data_;
  if (OB_FAIL(medium_info.assign(allocator, user_data))) {
    LOG_WARN("failed to copy medium info", K(ret), K(user_data));
  } else {
    key = mds_key_;
  }

  return ret;
}

int ObTabletMediumInfoReader::output_medium_info_from_dump_iter(
    common::ObIAllocator &allocator,
    compaction::ObMediumCompactionInfoKey &key,
    compaction::ObMediumCompactionInfo &medium_info)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(medium_info.assign(allocator, *dump_medium_info_))) {
    LOG_WARN("failed to copy medium info", K(ret));
  } else {
    key = dump_key_;
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase

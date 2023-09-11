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

#include "storage/tablet/ob_tablet_full_memory_mds_data.h"
#include "lib/allocator/ob_allocator.h"
#include "storage/blockstore/ob_shared_block_reader_writer.h"
#include "storage/tablet/ob_tablet_mds_data.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletFullMemoryMdsData::ObTabletFullMemoryMdsData()
  : is_inited_(false),
    tablet_status_uncommitted_kv_(),
    tablet_status_committed_kv_(),
    aux_tablet_info_uncommitted_kv_(),
    aux_tablet_info_committed_kv_(),
    medium_info_list_(),
    auto_inc_seq_()
{
}

ObTabletFullMemoryMdsData::~ObTabletFullMemoryMdsData()
{
  reset();
}

int ObTabletFullMemoryMdsData::init(common::ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(medium_info_list_.medium_info_list_.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int ObTabletFullMemoryMdsData::init(common::ObArenaAllocator &allocator, const ObTabletMdsData &mds_data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.tablet_status_.uncommitted_kv_, tablet_status_uncommitted_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.tablet_status_.committed_kv_, tablet_status_committed_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.aux_tablet_info_.uncommitted_kv_, aux_tablet_info_uncommitted_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_mds_dump_kv(allocator, mds_data.aux_tablet_info_.committed_kv_, aux_tablet_info_committed_kv_))) {
    LOG_WARN("failed to read mds dump kv", K(ret));
  } else if (OB_FAIL(read_medium_info_list(allocator, mds_data.medium_info_list_, medium_info_list_.medium_info_list_))) {
    LOG_WARN("failed to assign medium info list", K(ret), K(mds_data));
  } else if (OB_FAIL(read_auto_inc_seq(allocator, mds_data.auto_inc_seq_, auto_inc_seq_))) {
    LOG_WARN("failed to read auto inc seq", K(ret));
  } else {
    medium_info_list_.extra_medium_info_.info_ = mds_data.extra_medium_info_.info_;
    medium_info_list_.extra_medium_info_.last_medium_scn_ = mds_data.extra_medium_info_.last_medium_scn_;

    is_inited_ = true;
  }

  return ret;
}

void ObTabletFullMemoryMdsData::reset()
{
  tablet_status_uncommitted_kv_.reset();
  tablet_status_committed_kv_.reset();
  aux_tablet_info_uncommitted_kv_.reset();
  aux_tablet_info_committed_kv_.reset();
  medium_info_list_.reset();
  auto_inc_seq_.reset();
  is_inited_ = false;
}

int ObTabletFullMemoryMdsData::assign(const ObTabletFullMemoryMdsData &other, common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();

    if (OB_FAIL(tablet_status_uncommitted_kv_.assign(other.tablet_status_uncommitted_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(tablet_status_committed_kv_.assign(other.tablet_status_committed_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(aux_tablet_info_uncommitted_kv_.assign(other.aux_tablet_info_uncommitted_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(aux_tablet_info_committed_kv_.assign(other.aux_tablet_info_committed_kv_, allocator))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(medium_info_list_.assign(allocator, other.medium_info_list_))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else if (OB_FAIL(auto_inc_seq_.assign(allocator, other.auto_inc_seq_))) {
      LOG_WARN("failed to assign", K(ret), K(other));
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletFullMemoryMdsData::read_mds_dump_kv(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<mds::MdsDumpKV> &mds_dump_kv_addr,
    mds::MdsDumpKV &dump_kv)
{
  int ret = OB_SUCCESS;
  const mds::MdsDumpKV *ptr = nullptr;

  if (OB_UNLIKELY(!mds_dump_kv_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(mds_dump_kv_addr));
  } else if (OB_FAIL(ObTabletMdsData::load_mds_dump_kv(allocator, mds_dump_kv_addr, ptr))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  } else if (nullptr == ptr) {
    // do nothing
    dump_kv.reset();
  } else if (OB_FAIL(dump_kv.assign(*ptr, allocator))) {
    LOG_WARN("failed to copy mds dump kv", K(ret));
  }

  ObTabletMdsData::free_mds_dump_kv(allocator, ptr);

  return ret;
}

int ObTabletFullMemoryMdsData::read_medium_info_list(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &medium_info_list_addr,
    ObTabletDumpedMediumInfo &medium_info_list)
{
  int ret = OB_SUCCESS;
  const ObTabletDumpedMediumInfo *ptr = nullptr;

  if (OB_UNLIKELY(!medium_info_list_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(medium_info_list_addr));
  } else if (OB_FAIL(ObTabletMdsData::load_medium_info_list(allocator, medium_info_list_addr, ptr))) {
    LOG_WARN("failed to load medium info list", K(ret));
  } else if (nullptr == ptr && OB_FAIL(medium_info_list.init_for_first_creation(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  } else if (nullptr != ptr && OB_FAIL(medium_info_list.assign(*ptr, allocator))) {
    LOG_WARN("failed to copy medium info list", K(ret));
  }

  ObTabletMdsData::free_medium_info_list(allocator, ptr);

  return ret;
}

int ObTabletFullMemoryMdsData::read_auto_inc_seq(
    common::ObArenaAllocator &allocator,
    const ObTabletComplexAddr<share::ObTabletAutoincSeq> &auto_inc_seq_addr,
    share::ObTabletAutoincSeq &auto_inc_seq)
{
  int ret = OB_SUCCESS;
  const share::ObTabletAutoincSeq *ptr = nullptr;

  if (OB_FAIL(ObTabletMdsData::load_auto_inc_seq(allocator, auto_inc_seq_addr, ptr))) {
    LOG_WARN("failed to load auto inc seq", K(ret), K(auto_inc_seq_addr));
  } else if (nullptr == ptr) {
    // do nothing
  } else if (OB_FAIL(auto_inc_seq.assign(allocator, *ptr))) {
    LOG_WARN("failed to copy auto inc seq", K(ret));
  }

  return ret;
}

int ObTabletFullMemoryMdsData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tablet_status_uncommitted_kv_,
      tablet_status_committed_kv_,
      aux_tablet_info_uncommitted_kv_,
      aux_tablet_info_committed_kv_,
      medium_info_list_,
      auto_inc_seq_);

  return ret;
}

int ObTabletFullMemoryMdsData::deserialize(common::ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_status_uncommitted_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(tablet_status_committed_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(aux_tablet_info_uncommitted_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(aux_tablet_info_committed_kv_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(medium_info_list_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(auto_inc_seq_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else {
    is_inited_ = true;
  }

  return ret;
}

int64_t ObTabletFullMemoryMdsData::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tablet_status_uncommitted_kv_,
      tablet_status_committed_kv_,
      aux_tablet_info_uncommitted_kv_,
      aux_tablet_info_committed_kv_,
      medium_info_list_,
      auto_inc_seq_);

  return len;
}
} // namespace storage
} // namespace oceanbase

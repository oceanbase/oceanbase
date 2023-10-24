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

#include <algorithm>
#include "storage/tablet/ob_tablet_mds_data.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/compaction/ob_medium_compaction_mgr.h"
#include "storage/compaction/ob_medium_list_checker.h"
#include "storage/compaction/ob_extra_medium_info.h"
#include "storage/tablet/ob_tablet_full_memory_mds_data.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/tablet/ob_i_tablet_mds_interface.h"
#include "storage/tablet/ob_tablet_binding_info.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
ObTabletMdsDumpStruct::ObTabletMdsDumpStruct()
  : uncommitted_kv_(),
    committed_kv_()
{
}

ObTabletMdsDumpStruct::~ObTabletMdsDumpStruct()
{
  reset();
}

int ObTabletMdsDumpStruct::init(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, uncommitted_kv_.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, committed_kv_.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  }

  return ret;
}

void ObTabletMdsDumpStruct::reset()
{
  uncommitted_kv_.reset();
  committed_kv_.reset();
}

int ObTabletMdsDumpStruct::assign(
    const ObTabletMdsDumpStruct &other,
    common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (this != &other) {
    reset();

    if (OB_FAIL(init(allocator))) {
      LOG_WARN("failed to init", K(ret));
    } else if (OB_FAIL(uncommitted_kv_.ptr_->assign(*other.uncommitted_kv_.ptr_, allocator))) {
      LOG_WARN("failed to assign mds dump kv", K(ret));
    } else if (OB_FAIL(committed_kv_.ptr_->assign(*other.committed_kv_.ptr_, allocator))) {
      LOG_WARN("failed to assign mds dump kv", K(ret));
    }
  }

  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletMdsDumpStruct,
    uncommitted_kv_,
    committed_kv_)


ObTabletMdsData::ObTabletMdsData()
  : is_inited_(false),
    tablet_status_(),
    aux_tablet_info_(),
    extra_medium_info_(),
    medium_info_list_(),
    auto_inc_seq_(),
    tablet_status_cache_(),
    aux_tablet_info_cache_()
{
}

ObTabletMdsData::~ObTabletMdsData()
{
  reset();
}

void ObTabletMdsData::reset()
{
  auto_inc_seq_.reset();
  medium_info_list_.reset();
  extra_medium_info_.reset();
  aux_tablet_info_.reset();
  tablet_status_.reset();
  tablet_status_cache_.reset();
  aux_tablet_info_cache_.reset();
  is_inited_ = false;
}

void ObTabletMdsData::reset(common::ObIAllocator &allocator)
{
  free_auto_inc_seq(allocator, auto_inc_seq_.ptr_);
  free_medium_info_list(allocator, medium_info_list_.ptr_);
  free_mds_dump_kv(allocator, aux_tablet_info_.uncommitted_kv_.ptr_);
  free_mds_dump_kv(allocator, aux_tablet_info_.committed_kv_.ptr_);
  free_mds_dump_kv(allocator, tablet_status_.uncommitted_kv_.ptr_);
  free_mds_dump_kv(allocator, tablet_status_.committed_kv_.ptr_);

  auto_inc_seq_.ptr_ = nullptr;
  auto_inc_seq_.addr_.reset();
  medium_info_list_.ptr_ = nullptr;
  medium_info_list_.addr_.reset();
  aux_tablet_info_.uncommitted_kv_.ptr_ = nullptr;
  aux_tablet_info_.uncommitted_kv_.addr_.reset();
  aux_tablet_info_.committed_kv_.ptr_ = nullptr;
  aux_tablet_info_.committed_kv_.addr_.reset();
  tablet_status_.uncommitted_kv_.ptr_ = nullptr;
  tablet_status_.uncommitted_kv_.addr_.reset();
  tablet_status_.committed_kv_.ptr_ = nullptr;
  tablet_status_.committed_kv_.addr_.reset();

  is_inited_ = false;
}

bool ObTabletMdsData::is_valid() const
{
  // TODO(@bowen.gbw): add more check rules
  return is_inited_;
}

int ObTabletMdsData::init_for_first_creation(common::ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    auto_inc_seq_.addr_.set_none_addr();
    medium_info_list_.addr_.set_none_addr();
    aux_tablet_info_.uncommitted_kv_.addr_.set_none_addr();
    aux_tablet_info_.committed_kv_.addr_.set_none_addr();
    tablet_status_.uncommitted_kv_.addr_.set_none_addr();
    tablet_status_.committed_kv_.addr_.set_none_addr();

    is_inited_ = true;
  }

  return ret;
}

int ObTabletMdsData::init_with_tablet_status(
    common::ObIAllocator &allocator,
    const ObTabletStatus::Status &tablet_status,
    const ObTabletMdsUserDataType &data_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_status_.init(allocator))) {
    LOG_WARN("failed to init tablet status", K(ret));
  } else if (OB_FAIL(set_tablet_status(allocator, tablet_status, data_type))) {
    LOG_WARN("failed to set tablet status", K(ret));
  } else {
    auto_inc_seq_.addr_.set_none_addr();
    medium_info_list_.addr_.set_none_addr();
    aux_tablet_info_.uncommitted_kv_.addr_.set_none_addr();
    aux_tablet_info_.committed_kv_.addr_.set_none_addr();
    tablet_status_.uncommitted_kv_.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
    tablet_status_.committed_kv_.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));

    is_inited_ = true;
  }

  return ret;
}

int ObTabletMdsData::init_for_mds_table_dump(
    common::ObIAllocator &allocator,
    const ObTabletMdsData &mds_table_data,
    const ObTabletMdsData &base_data,
    const int64_t finish_medium_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    if (OB_FAIL(init_single_complex_addr(allocator, mds_table_data.tablet_status_.uncommitted_kv_, base_data.tablet_status_.uncommitted_kv_, tablet_status_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, mds_table_data.tablet_status_.committed_kv_, base_data.tablet_status_.committed_kv_, tablet_status_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, mds_table_data.aux_tablet_info_.uncommitted_kv_, base_data.aux_tablet_info_.uncommitted_kv_, aux_tablet_info_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, mds_table_data.aux_tablet_info_.committed_kv_, base_data.aux_tablet_info_.committed_kv_, aux_tablet_info_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, mds_table_data.auto_inc_seq_, base_data.auto_inc_seq_, auto_inc_seq_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, mds_table_data.medium_info_list_, base_data.medium_info_list_, finish_medium_scn, medium_info_list_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(update_user_data_from_complex_addr(tablet_status_.committed_kv_, tablet_status_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", tablet_status_.committed_kv_);
    } else if (OB_FAIL(update_user_data_from_complex_addr(aux_tablet_info_.committed_kv_, aux_tablet_info_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", aux_tablet_info_.committed_kv_);
    } else {
      // always use base data to set extra medium info
      extra_medium_info_ = base_data.extra_medium_info_;
      is_inited_ = true;
      LOG_INFO("succeeded to init mds data", K(ret),
          "mds_table_medium_info", mds_table_data.medium_info_list_,
          "base_medium_info", base_data.medium_info_list_,
          K(finish_medium_scn), K_(medium_info_list), K_(extra_medium_info));
    }

    if (OB_FAIL(ret)) {
      reset(allocator);
    }
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<mds::MdsDumpKV> &mds_table_data,
    const ObTabletComplexAddr<mds::MdsDumpKV> &base_data,
    ObTabletComplexAddr<mds::MdsDumpKV> &fused_data)
{
  int ret = OB_SUCCESS;
  fused_data.reset();
  mds::MdsDumpKV *&fused_data_ptr = fused_data.ptr_;
  const mds::MdsDumpKV *mds_table_ptr = nullptr;
  const mds::MdsDumpKV *base_ptr = nullptr;

  if (OB_FAIL(load_mds_dump_kv(allocator, mds_table_data, mds_table_ptr))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  } else if (OB_FAIL(load_mds_dump_kv(allocator, base_data, base_ptr))) {
    LOG_WARN("failed to load mds dump kv", K(ret));
  } else if (nullptr == mds_table_ptr && nullptr == base_ptr) {
    // data in mds table and base is both empty, so set addr to NONE
    fused_data.addr_.set_none_addr();
  } else {
    if (nullptr == mds_table_ptr && nullptr != base_ptr) {
      fused_data_ptr = const_cast<mds::MdsDumpKV*>(base_ptr);
    } else if (nullptr != mds_table_ptr) {
      // data in mds table is not empty, ignore data in base, use data in mds table as fused data
      fused_data_ptr = const_cast<mds::MdsDumpKV*>(mds_table_ptr);
      free_mds_dump_kv(allocator, base_ptr);
    }

    if (OB_FAIL(ret)) {
      fused_data_ptr = nullptr;
      free_mds_dump_kv(allocator, mds_table_ptr);
      free_mds_dump_kv(allocator, base_ptr);
    } else {
      fused_data.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
    }
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<share::ObTabletAutoincSeq> &mds_table_data,
    const ObTabletComplexAddr<share::ObTabletAutoincSeq> &base_data,
    ObTabletComplexAddr<share::ObTabletAutoincSeq> &fused_data)
{
  int ret = OB_SUCCESS;
  fused_data.reset();
  share::ObTabletAutoincSeq *&fused_data_ptr = fused_data.ptr_;
  const share::ObTabletAutoincSeq *mds_table_ptr = nullptr;
  const share::ObTabletAutoincSeq *base_ptr = nullptr;

  if (OB_FAIL(load_auto_inc_seq(allocator, mds_table_data, mds_table_ptr))) {
    LOG_WARN("failed to load auto inc seq", K(ret), K(mds_table_data));
  } else if (OB_FAIL(load_auto_inc_seq(allocator, base_data, base_ptr))) {
    LOG_WARN("failed to load auto inc seq", K(ret), K(base_data));
  } else if (nullptr == mds_table_ptr && nullptr == base_ptr) {
    // both data in mds table and base is invalid, no need to fuse, set fused data to NONE
    fused_data.addr_.set_none_addr();
  } else {
    if (nullptr == mds_table_ptr && nullptr != base_ptr) {
      fused_data_ptr = const_cast<share::ObTabletAutoincSeq*>(base_ptr);
    } else if (nullptr != mds_table_ptr) {
      // data in mds table is not empty, ignore data in base, use data in mds table as fused data
      fused_data_ptr = const_cast<share::ObTabletAutoincSeq*>(mds_table_ptr);
      free_auto_inc_seq(allocator, base_ptr);
    }

    if (OB_FAIL(ret)) {
      fused_data_ptr = nullptr;
      free_auto_inc_seq(allocator, mds_table_ptr);
      free_auto_inc_seq(allocator, base_ptr);
    } else {
      fused_data.addr_.set_mem_addr(0, sizeof(share::ObTabletAutoincSeq));
    }
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &mds_table_data,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &base_data,
    const int64_t finish_medium_scn,
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &fused_data)
{
  int ret = OB_SUCCESS;
  fused_data.reset();
  ObTabletDumpedMediumInfo *&fused_data_ptr = fused_data.ptr_;
  const ObTabletDumpedMediumInfo *mds_table_ptr = nullptr;
  const ObTabletDumpedMediumInfo *base_ptr = nullptr;

  if (OB_FAIL(load_medium_info_list(allocator, mds_table_data, mds_table_ptr))) {
    LOG_WARN("failed to load medium info list", K(ret), K(mds_table_data));
  } else if (OB_FAIL(load_medium_info_list(allocator, base_data, base_ptr))) {
    LOG_WARN("failed to load medium info list", K(ret), K(base_data));
  } else if (nullptr == mds_table_ptr && nullptr == base_ptr) {
    // data in mds table and base is both empty, set fused data to NONE
    fused_data.addr_.set_none_addr();
  } else {
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, fused_data_ptr))) {
      LOG_WARN("failed to alloc and new", K(ret));
    } else if (nullptr == mds_table_ptr && nullptr != base_ptr) {
      if (OB_FAIL(fused_data_ptr->init_for_evict_medium_info(allocator, finish_medium_scn, *base_ptr))) {
        LOG_WARN("failed to init", K(ret));
      }
    } else if (nullptr != mds_table_ptr && nullptr == base_ptr) {
      if (OB_FAIL(fused_data_ptr->init_for_evict_medium_info(allocator, finish_medium_scn, *mds_table_ptr))) {
        LOG_WARN("failed to init", K(ret));
      }
    } else if (nullptr != mds_table_ptr && nullptr != base_ptr) {
      if (OB_FAIL(fused_data_ptr->init_for_mds_table_dump(allocator, finish_medium_scn, *mds_table_ptr, *base_ptr))) {
        LOG_WARN("failed to init", K(ret));
      }
    }

    if (OB_FAIL(ret)) {
      free_medium_info_list(allocator, fused_data_ptr);
      fused_data_ptr = nullptr;
    } else {
      fused_data.addr_.set_mem_addr(0, sizeof(ObTabletDumpedMediumInfo));
    }

    // always free medium info list after usage
    free_medium_info_list(allocator, mds_table_ptr);
    free_medium_info_list(allocator, base_ptr);
  }

  return ret;
}

int ObTabletMdsData::init_for_evict_medium_info(
    common::ObIAllocator &allocator,
    const ObTabletMdsData &other,
    const int64_t finish_medium_scn,
    const ObMergeType merge_type)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    if (OB_FAIL(init_single_complex_addr(allocator, other.tablet_status_.uncommitted_kv_, tablet_status_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.tablet_status_.committed_kv_, tablet_status_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.aux_tablet_info_.uncommitted_kv_, aux_tablet_info_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.aux_tablet_info_.committed_kv_, aux_tablet_info_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.auto_inc_seq_, auto_inc_seq_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.medium_info_list_, finish_medium_scn, medium_info_list_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(update_user_data_from_complex_addr(tablet_status_.committed_kv_, tablet_status_cache_))) {
      LOG_WARN("failed to update user data", K(ret), "complex_addr", tablet_status_.committed_kv_);
    } else if (OB_FAIL(update_user_data_from_complex_addr(aux_tablet_info_.committed_kv_, aux_tablet_info_cache_))) {
      LOG_WARN("failed to update user data", K(ret), "complex_addr", aux_tablet_info_.committed_kv_);
    } else if (is_major_merge_type(merge_type)) {
      extra_medium_info_.last_compaction_type_ = is_major_merge(merge_type) ? compaction::ObMediumCompactionInfo::MAJOR_COMPACTION : compaction::ObMediumCompactionInfo::MEDIUM_COMPACTION;
      extra_medium_info_.last_medium_scn_ = finish_medium_scn;
      extra_medium_info_.wait_check_flag_ = true;
    } else {
      extra_medium_info_ = other.extra_medium_info_;
    }

    if (OB_FAIL(ret)) {
      reset(allocator);
    } else {
      is_inited_ = true;
      LOG_INFO("succeeded to init mds data", K(ret), K(finish_medium_scn),
          "src_medium_info_list", other.medium_info_list_,
          "src_extra_medium_info", other.extra_medium_info_,
          K_(medium_info_list), K_(extra_medium_info));
    }
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<mds::MdsDumpKV> &src_addr,
    ObTabletComplexAddr<mds::MdsDumpKV> &dst_addr)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  const mds::MdsDumpKV *ptr = nullptr;

  if (OB_FAIL(load_mds_dump_kv(allocator, src_addr, ptr))) {
    LOG_WARN("failed to load mds dump kv", K(ret), K(src_addr));
  } else if (nullptr == ptr) {
    dst_addr.addr_.set_none_addr();
  } else {
    dst_addr.ptr_ = const_cast<mds::MdsDumpKV*>(ptr);
    dst_addr.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<share::ObTabletAutoincSeq> &src_addr,
    ObTabletComplexAddr<share::ObTabletAutoincSeq> &dst_addr)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  const share::ObTabletAutoincSeq *ptr = nullptr;
  share::ObTabletAutoincSeq *&dst_data = dst_addr.ptr_;

  if (OB_FAIL(load_auto_inc_seq(allocator, src_addr, ptr))) {
    LOG_WARN("failed to load auto inc seq", K(ret), K(src_addr));
  } else if (nullptr == ptr) {
    // ptr is emtpy, no need to copy it, set dst addr to NONE
    dst_addr.addr_.set_none_addr();
  } else {
    dst_addr.ptr_ = const_cast<share::ObTabletAutoincSeq*>(ptr);
    dst_addr.addr_.set_mem_addr(0, sizeof(share::ObTabletAutoincSeq));
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &src_addr,
    const int64_t finish_medium_scn,
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &dst_addr)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  const ObTabletDumpedMediumInfo *ptr = nullptr;
  ObTabletDumpedMediumInfo *&dst_data = dst_addr.ptr_;

  if (OB_FAIL(load_medium_info_list(allocator, src_addr, ptr))) {
    LOG_WARN("failed to load medium info list", K(ret), K(src_addr));
  } else if (nullptr == ptr) {
    dst_addr.addr_.set_none_addr();
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, dst_data))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(dst_data->init_for_evict_medium_info(allocator, finish_medium_scn, *ptr))) {
    LOG_WARN("failed to init", K(ret));
  } else if (dst_data->medium_info_list_.empty()) {
    // dst data is empty, so no need to keep it
    free_medium_info_list(allocator, dst_data);
    dst_data = nullptr;
    dst_addr.addr_.set_none_addr();
  } else {
    dst_addr.addr_.set_mem_addr(0, sizeof(ObTabletDumpedMediumInfo));
  }

  if (OB_FAIL(ret)) {
    free_medium_info_list(allocator, dst_data);
    dst_data = nullptr;
  }

  // always free ptr after usage
  free_medium_info_list(allocator, ptr);

  return ret;
}

int ObTabletMdsData::init_by_full_memory_mds_data(
    common::ObIAllocator &allocator,
    const ObTabletFullMemoryMdsData &full_memory_mds_data)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    if (OB_FAIL(init_single_complex_addr(allocator, full_memory_mds_data.tablet_status_uncommitted_kv_, tablet_status_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, full_memory_mds_data.tablet_status_committed_kv_, tablet_status_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, full_memory_mds_data.aux_tablet_info_uncommitted_kv_, aux_tablet_info_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, full_memory_mds_data.aux_tablet_info_committed_kv_, aux_tablet_info_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, full_memory_mds_data.auto_inc_seq_, auto_inc_seq_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, full_memory_mds_data.medium_info_list_.medium_info_list_, medium_info_list_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(update_user_data_from_complex_addr(tablet_status_.committed_kv_, tablet_status_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", tablet_status_.committed_kv_);
    } else if (OB_FAIL(update_user_data_from_complex_addr(aux_tablet_info_.committed_kv_, aux_tablet_info_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", aux_tablet_info_.committed_kv_);
    } else {
      extra_medium_info_ = full_memory_mds_data.medium_info_list_.extra_medium_info_;
    }

    if (OB_FAIL(ret)) {
      reset(allocator);
    } else {
      is_inited_ = true;
      LOG_INFO("succeeded to init mds data", K(ret),
          "src_medium_info_list", full_memory_mds_data.medium_info_list_.medium_info_list_,
          "src_extra_medium_info", full_memory_mds_data.medium_info_list_.extra_medium_info_,
          K_(medium_info_list), K_(extra_medium_info));
    }
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV &src_data,
    ObTabletComplexAddr<mds::MdsDumpKV> &dst_addr)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  mds::MdsDumpKV *&dst_data = dst_addr.ptr_;
  const mds::MdsDumpNode &dump_node = src_data.v_;

  if (dump_node.user_data_.empty()) {
    // src data is empty actually, dst should be set to NONE
    dst_addr.addr_.set_none_addr();
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, dst_data))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(dst_data->assign(src_data, allocator))) {
    LOG_WARN("failed to copy", K(ret), K(src_data));
  } else {
    dst_addr.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
  }

  if (OB_FAIL(ret)) {
    free_mds_dump_kv(allocator, dst_data);
    dst_data = nullptr;
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const share::ObTabletAutoincSeq &src_data,
    ObTabletComplexAddr<share::ObTabletAutoincSeq> &dst_addr)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  share::ObTabletAutoincSeq *&dst_data = dst_addr.ptr_;

  if (!src_data.is_valid()) {
    // src data is invalid, dst should be set to NONE
    dst_addr.addr_.set_none_addr();
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, dst_data))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(dst_data->assign(allocator, src_data))) {
    LOG_WARN("failed to copy", K(ret), K(src_data));
  } else {
    dst_addr.addr_.set_mem_addr(0, sizeof(share::ObTabletAutoincSeq));
  }

  if (OB_FAIL(ret)) {
    free_auto_inc_seq(allocator, dst_data);
    dst_data = nullptr;
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr(
    common::ObIAllocator &allocator,
    const ObTabletDumpedMediumInfo &src_data,
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &dst_addr)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  ObTabletDumpedMediumInfo *&dst_data = dst_addr.ptr_;

  if (!src_data.is_valid() || src_data.medium_info_list_.empty()) {
    // src data is invalid or medium info list is empty, dst should be set to NONE
    dst_addr.addr_.set_none_addr();
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, dst_data))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(dst_data->assign(src_data, allocator))) {
    LOG_WARN("failed to copy", K(ret));
  } else {
    dst_addr.addr_.set_mem_addr(0, sizeof(ObTabletDumpedMediumInfo));
  }

  if (OB_FAIL(ret)) {
    free_medium_info_list(allocator, dst_data);
    dst_data = nullptr;
  }

  return ret;
}

int ObTabletMdsData::init_single_complex_addr_and_extra_info(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &src_addr,
    const compaction::ObExtraMediumInfo &src_addr_extra_info,
    const ObTabletDumpedMediumInfo &src_data,
    const compaction::ObExtraMediumInfo &src_data_extra_info,
    const int64_t finish_medium_scn,
    ObTabletComplexAddr<ObTabletDumpedMediumInfo> &dst_addr,
    compaction::ObExtraMediumInfo &dst_extra_info)
{
  int ret = OB_SUCCESS;
  dst_addr.reset();
  const ObTabletDumpedMediumInfo *ptr = nullptr;
  ObTabletDumpedMediumInfo *&dst_data = dst_addr.ptr_;
  bool empty_data_from_src_data = (!src_data.is_valid() || src_data.medium_info_list_.empty());

  if (OB_FAIL(load_medium_info_list(allocator, src_addr, ptr))) {
    LOG_WARN("failed to load medium info list", K(ret), K(src_addr));
  } else if ((nullptr == ptr) && empty_data_from_src_data) {
    dst_addr.addr_.set_none_addr();
  } else {
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, dst_data))) {
      LOG_WARN("failed to alloc and new", K(ret));
    } else if (nullptr == ptr && !empty_data_from_src_data) {
      if (OB_FAIL(dst_data->init_for_evict_medium_info(allocator, finish_medium_scn, src_data))) {
        LOG_WARN("failed to copy medium info", K(ret), K(finish_medium_scn));
      }
    } else if ((nullptr != ptr) && empty_data_from_src_data) {
      if (OB_FAIL(dst_data->init_for_evict_medium_info(allocator, finish_medium_scn, *ptr))) {
        LOG_WARN("failed to copy medium info", K(ret), K(finish_medium_scn));
      }
    } else if ((nullptr != ptr) && !empty_data_from_src_data) {
      if (OB_FAIL(dst_data->init_for_mds_table_dump(allocator, finish_medium_scn, *ptr, src_data))) {
        LOG_WARN("failed to copy medium info", K(ret), K(finish_medium_scn));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected branch", K(ret), K(src_addr), KPC(ptr), K(src_data));
    }

    if (OB_FAIL(ret)) {
      free_medium_info_list(allocator, dst_data);
      dst_data = nullptr;
    } else if (dst_data->medium_info_list_.empty()) {
      free_medium_info_list(allocator, dst_data);
      dst_data = nullptr;
      dst_addr.addr_.set_none_addr();
    } else {
      dst_addr.addr_.set_mem_addr(0, sizeof(ObTabletDumpedMediumInfo));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (finish_medium_scn < src_addr_extra_info.last_medium_scn_
      || src_addr_extra_info.last_medium_scn_ < src_data_extra_info.last_medium_scn_) {
    dst_extra_info.last_compaction_type_ = src_data_extra_info.last_compaction_type_;
    dst_extra_info.last_medium_scn_ = src_data_extra_info.last_medium_scn_;
    if (0 == src_data_extra_info.last_medium_scn_) {
      dst_extra_info.wait_check_flag_ = false;
    } else {
      dst_extra_info.wait_check_flag_ = true;
    }
  } else {
    dst_extra_info = src_addr_extra_info;
  }

  // always free ptr after usage
  free_medium_info_list(allocator, ptr);

  return ret;
}

int ObTabletMdsData::init_for_merge_with_full_mds_data(
    common::ObIAllocator &allocator,
    const ObTabletMdsData &other,
    const ObTabletFullMediumInfo &full_memory_medium_info_list,
    const int64_t finish_medium_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    if (OB_FAIL(init_single_complex_addr(allocator, other.tablet_status_.uncommitted_kv_, tablet_status_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.tablet_status_.committed_kv_, tablet_status_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.aux_tablet_info_.uncommitted_kv_, aux_tablet_info_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.aux_tablet_info_.committed_kv_, aux_tablet_info_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.auto_inc_seq_, auto_inc_seq_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(update_user_data_from_complex_addr(tablet_status_.committed_kv_, tablet_status_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", tablet_status_.committed_kv_);
    } else if (OB_FAIL(update_user_data_from_complex_addr(aux_tablet_info_.committed_kv_, aux_tablet_info_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", aux_tablet_info_.committed_kv_);
    } else if (OB_FAIL(init_single_complex_addr_and_extra_info(
        allocator,
        other.medium_info_list_,
        other.extra_medium_info_,
        full_memory_medium_info_list.medium_info_list_,
        full_memory_medium_info_list.extra_medium_info_,
        finish_medium_scn,
        medium_info_list_,
        extra_medium_info_))) {
      LOG_WARN("failed to init single complex addr", K(ret), K(finish_medium_scn));
    }

    if (OB_FAIL(ret)) {
      reset(allocator);
    } else {
      is_inited_ = true;
      LOG_INFO("succeeded to init mds data", K(ret), K(finish_medium_scn),
          "other_memory_medium_info_list", other.medium_info_list_,
          "other_memory_extra_medium_info", other.extra_medium_info_,
          "full_memory_medium_info_list", full_memory_medium_info_list.medium_info_list_,
          "full_memory_extra_medium_info", full_memory_medium_info_list.extra_medium_info_,
          K_(medium_info_list), K_(extra_medium_info));
    }
  }

  return ret;
}

int ObTabletMdsData::init_with_update_medium_info(
    common::ObIAllocator &allocator,
    const ObTabletMdsData &other)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else {
    if (OB_FAIL(init_single_complex_addr(allocator, other.tablet_status_.uncommitted_kv_, tablet_status_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.tablet_status_.committed_kv_, tablet_status_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.aux_tablet_info_.uncommitted_kv_, aux_tablet_info_.uncommitted_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.aux_tablet_info_.committed_kv_, aux_tablet_info_.committed_kv_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(init_single_complex_addr(allocator, other.auto_inc_seq_, auto_inc_seq_))) {
      LOG_WARN("failed to init single complex addr", K(ret));
    } else if (OB_FAIL(update_user_data_from_complex_addr(tablet_status_.committed_kv_, tablet_status_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", tablet_status_.committed_kv_);
    } else if (OB_FAIL(update_user_data_from_complex_addr(aux_tablet_info_.committed_kv_, aux_tablet_info_cache_))) {
      LOG_WARN("failed to update user data cache", K(ret), "complex_addr", aux_tablet_info_.committed_kv_);
    } else {
      const int64_t finish_medium_scn = other.extra_medium_info_.last_medium_scn_;
      if (OB_FAIL(init_single_complex_addr(allocator, other.medium_info_list_, finish_medium_scn, medium_info_list_))) {
        LOG_WARN("failed to init single complex addr", K(ret));
      } else {
        extra_medium_info_.last_compaction_type_ = other.extra_medium_info_.last_compaction_type_;
        extra_medium_info_.last_medium_scn_ = other.extra_medium_info_.last_medium_scn_;
        extra_medium_info_.wait_check_flag_ = false;
      }
    }

    if (OB_FAIL(ret)) {
      reset(allocator);
    } else {
      is_inited_ = true;
    }
  }

  return ret;
}

int ObTabletMdsData::init_empty_shell(
    const ObTabletCreateDeleteMdsUserData &tablet_status)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_status_cache_.assign(tablet_status))) {
    LOG_WARN("failed to copy", K(ret), K(tablet_status));
  } else {
    tablet_status_.uncommitted_kv_.addr_.set_none_addr();
    tablet_status_.committed_kv_.addr_.set_none_addr();
    aux_tablet_info_.uncommitted_kv_.addr_.set_none_addr();
    aux_tablet_info_.committed_kv_.addr_.set_none_addr();
    extra_medium_info_.reset();
    medium_info_list_.addr_.set_none_addr();
    auto_inc_seq_.addr_.set_none_addr();

    is_inited_ = true;
  }

  return ret;
}

void ObTabletMdsData::set_mem_addr()
{
  auto_inc_seq_.addr_.set_mem_addr(0, sizeof(share::ObTabletAutoincSeq));
  medium_info_list_.addr_.set_mem_addr(0, sizeof(ObTabletDumpedMediumInfo));
  aux_tablet_info_.uncommitted_kv_.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
  aux_tablet_info_.committed_kv_.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
  tablet_status_.uncommitted_kv_.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
  tablet_status_.committed_kv_.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
}

int ObTabletMdsData::init_single_mds_dump_kv(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV *input_kv,
    ObTabletComplexAddr<mds::MdsDumpKV> &kv)
{
  int ret = OB_SUCCESS;

  if (nullptr == input_kv) {
    kv.ptr_ = nullptr;
    kv.addr_.set_none_addr();
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, kv.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(kv.ptr_->assign(*input_kv, allocator))) {
    LOG_WARN("failed to copy mds dump kv", K(ret));
  } else {
    kv.addr_.set_mem_addr(0, sizeof(mds::MdsDumpKV));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != kv.ptr_) {
      allocator.free(kv.ptr_);
      kv.ptr_ = nullptr;
    }
  }

  return ret;
}

int ObTabletMdsData::copy_medium_info_list(
    const int64_t finish_medium_scn,
    const ObTabletDumpedMediumInfo &input_medium_info_list,
    ObTabletDumpedMediumInfo &medium_info_list)
{
  int ret = OB_SUCCESS;
  const common::ObIArray<compaction::ObMediumCompactionInfo*> &array = input_medium_info_list.medium_info_list_;

  for (int64_t i = 0; OB_SUCC(ret) && i < array.count(); ++i) {
    const compaction::ObMediumCompactionInfo *input_medium_info = array.at(i);
    if (OB_ISNULL(input_medium_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, medium info is null", K(ret), K(i), KP(input_medium_info));
    } else if (input_medium_info->medium_snapshot_ <= finish_medium_scn) {
      // medium snapshot no bigger than finish medium scn(which is from last major sstable),
      // no need to copy it
    } else if (input_medium_info->medium_snapshot_ <= medium_info_list.get_max_medium_snapshot()) {
      // medium info no bigger than current max medium snapshot,
      // no need to copy it
    } else if (OB_FAIL(medium_info_list.append(*input_medium_info))) {
      LOG_WARN("failed to append medium info", K(ret), K(i), KPC(input_medium_info));
    }
  }

  return ret;
}

int ObTabletMdsData::copy_medium_info_list(
    const int64_t finish_medium_scn,
    const ObTabletDumpedMediumInfo &input_medium_info_list1,
    const ObTabletDumpedMediumInfo &input_medium_info_list2,
    ObTabletDumpedMediumInfo &medium_info_list)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> array1;
  common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> array2;

  if (OB_FAIL(array1.assign(input_medium_info_list1.medium_info_list_))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (OB_FAIL(array2.assign(input_medium_info_list2.medium_info_list_))) {
    LOG_WARN("failed to assign", K(ret));
  } else {
    // sort first
    std::sort(array1.begin(), array1.end(), ObTabletDumpedMediumInfo::compare);
    std::sort(array2.begin(), array2.end(), ObTabletDumpedMediumInfo::compare);

    // merge
    bool contain = false;
    int64_t i = 0;
    int64_t j = 0;
    while (OB_SUCC(ret) && i < array1.count() && j < array2.count()) {
      const compaction::ObMediumCompactionInfo *info1 = array1.at(i);
      const compaction::ObMediumCompactionInfo *info2 = array2.at(j);
      const compaction::ObMediumCompactionInfo *chosen_info = nullptr;

      if (OB_ISNULL(info1) || OB_ISNULL(info2)) {
        LOG_WARN("medium info is null", K(ret), K(i), K(j), KP(info1), KP(info2));
      } else if (info1->medium_snapshot_ < info2->medium_snapshot_) {
        chosen_info = info1;
        ++i;
      } else if (info1->medium_snapshot_ > info2->medium_snapshot_) {
        chosen_info = info2;
        ++j;
      } else {
        chosen_info = info2;
        ++i;
        ++j;
      }

      if (OB_FAIL(ret)) {
      } else if (chosen_info->medium_snapshot_ <= finish_medium_scn) {
        // medium snapshot no bigger than finish medium scn(which is from last major sstable),
        // no need to copy it
      } else if (OB_FAIL(medium_info_list.is_contain(*chosen_info, contain))) {
        LOG_WARN("failed to check medium info existence", K(ret));
      } else if (contain) {
        // do nothing
      } else if (OB_FAIL(medium_info_list.append(*chosen_info))) {
        LOG_WARN("failed to append medium info", K(ret), K(i), K(j), KPC(chosen_info));
      }
    }

    for (; OB_SUCC(ret) && i < array1.count(); ++i) {
      const compaction::ObMediumCompactionInfo *info = array1.at(i);
      if (info->medium_snapshot_ <= finish_medium_scn) {
        // medium snapshot no bigger than finish medium scn(which is from last major sstable),
        // no need to copy it
      } else if (OB_FAIL(medium_info_list.is_contain(*info, contain))) {
        LOG_WARN("failed to check medium info existence", K(ret));
      } else if (contain) {
        // do nothing
      } else if (OB_FAIL(medium_info_list.append(*info))) {
        LOG_WARN("failed to append medium info", K(ret), K(i), KPC(info));
      }
    }

    for (; OB_SUCC(ret) && j < array2.count(); ++j) {
      const compaction::ObMediumCompactionInfo *info = array2.at(j);
      if (info->medium_snapshot_ <= finish_medium_scn) {
        // medium snapshot no bigger than finish medium scn(which is from last major sstable),
        // no need to copy it
      } else if (OB_FAIL(medium_info_list.is_contain(*info, contain))) {
        LOG_WARN("failed to check medium info existence", K(ret));
      } else if (contain) {
        // do nothing
      } else if (OB_FAIL(medium_info_list.append(*info))) {
        LOG_WARN("failed to append medium info", K(ret), K(j), KPC(info));
      }
    }
  }

  return ret;
}

int ObTabletMdsData::load_mds_dump_kv(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<mds::MdsDumpKV> &complex_addr,
    const mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  kv = nullptr;
  mds::MdsDumpKV *ptr = nullptr;

  if (OB_UNLIKELY(!complex_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(ret), K(complex_addr));
  } else if (complex_addr.is_none_object()) {
    // do nothing
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, ptr))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (complex_addr.is_memory_object()) {
    const mds::MdsDumpKV *src_kv = complex_addr.ptr_;
    if (src_kv->v_.user_data_.empty()) {
      free_mds_dump_kv(allocator, ptr);
      ptr = nullptr;
      LOG_INFO("read empty user data", K(ret), K(complex_addr));
    } else if (OB_FAIL(ptr->assign(*src_kv, allocator))) {
      LOG_WARN("failed to copy mds dump kv", K(ret));
    }
  } else if (complex_addr.is_disk_object()) {
    const ObMetaDiskAddr &addr = complex_addr.addr_;
    ObArenaAllocator arena_allocator("dump_kv_reader");
    char *buf = nullptr;
    int64_t len = 0;
    int64_t pos = 0;
    if (OB_FAIL(ObTabletObjLoadHelper::read_from_addr(arena_allocator, addr, buf, len))) {
      LOG_WARN("failed to read from addr", K(ret), K(addr));
    } else if (OB_FAIL(ptr->deserialize(allocator, buf, len, pos))) {
      LOG_WARN("failed to deserialize", K(ret));
    } else {
      // for compat logic
      // if user data is empty, we consider that nothing is read from disk,
      // and ptr should be set to null
      const mds::MdsDumpNode &dump_node = ptr->v_;
      if (dump_node.user_data_.empty()) {
        free_mds_dump_kv(allocator, ptr);
        ptr = nullptr;
        LOG_INFO("read empty user data", K(ret), K(complex_addr));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected complex addr type", K(ret), K(complex_addr));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != ptr) {
      allocator.free(ptr);
    }
  } else {
    kv = ptr;
  }

  return ret;
}

int ObTabletMdsData::load_medium_info_list(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<ObTabletDumpedMediumInfo> &complex_addr,
    const ObTabletDumpedMediumInfo *&medium_info_list)
{
  int ret = OB_SUCCESS;
  medium_info_list = nullptr;
  ObTabletDumpedMediumInfo *ptr = nullptr;

  if (OB_UNLIKELY(!complex_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(ret), K(complex_addr));
  } else if (complex_addr.is_none_object()) {
    // do nothing
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, ptr))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (complex_addr.is_memory_object()) {
    const ObTabletDumpedMediumInfo *src_medium_info = complex_addr.ptr_;
    if (src_medium_info->medium_info_list_.empty()) {
      free_medium_info_list(allocator, ptr);
      ptr = nullptr;
      LOG_INFO("read empty medium info", K(ret), K(complex_addr));
    } else if (OB_FAIL(ptr->assign(*src_medium_info, allocator))) {
      LOG_INFO("failed to copy medium info list", K(ret));
    }
  } else if (complex_addr.is_disk_object()) {
    if (OB_FAIL(read_medium_info(allocator, complex_addr.addr_, ptr->medium_info_list_))) {
      LOG_WARN("failed to read medium info", K(ret), "addr", complex_addr.addr_);
    } else if (ptr->medium_info_list_.empty()) {
      free_medium_info_list(allocator, ptr);
      ptr = nullptr;
      LOG_INFO("read empty medium info", K(ret), K(complex_addr));
    } else {
      std::sort(ptr->medium_info_list_.begin(), ptr->medium_info_list_.end(), ObTabletDumpedMediumInfo::compare);

      ptr->allocator_ = &allocator;
      ptr->is_inited_ = true;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected complex addr type", K(ret), K(complex_addr));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != ptr) {
      allocator.free(ptr);
    }
  } else {
    medium_info_list = ptr;
  }

  return ret;
}

int ObTabletMdsData::load_auto_inc_seq(
    common::ObIAllocator &allocator,
    const ObTabletComplexAddr<share::ObTabletAutoincSeq> &complex_addr,
    const share::ObTabletAutoincSeq *&auto_inc_seq)
{
  int ret = OB_SUCCESS;
  auto_inc_seq = nullptr;
  share::ObTabletAutoincSeq *ptr = nullptr;

  if (OB_UNLIKELY(!complex_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid addr", K(ret), K(complex_addr));
  } else if (complex_addr.is_none_object()) {
    // do nothing
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, ptr))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (complex_addr.is_memory_object()) {
    const share::ObTabletAutoincSeq *src_auto_inc_seq = complex_addr.ptr_;
    if (!src_auto_inc_seq->is_valid()) {
      free_auto_inc_seq(allocator, ptr);
      ptr = nullptr;
      LOG_INFO("read empty auto inc seq", K(ret), K(complex_addr));
    } else if (OB_FAIL(ptr->assign(allocator, *src_auto_inc_seq))) {
      LOG_WARN("failed to copy auto inc seq", K(ret));
    }
  } else if (complex_addr.is_disk_object()) {
    ObTabletMemberWrapper<share::ObTabletAutoincSeq> wrapper;
    ObStorageMetaHandle handle;
    ObStorageMetaKey meta_key(MTL_ID(), complex_addr.addr_);
    const ObTablet *tablet = nullptr; // no use here
    const share::ObTabletAutoincSeq *auto_inc_seq_cache = nullptr;
    if (OB_FAIL(OB_STORE_CACHE.get_storage_meta_cache().get_meta(ObStorageMetaValue::MetaType::AUTO_INC_SEQ,
        meta_key, handle, tablet))) {
      LOG_WARN("get meta failed", K(ret), K(meta_key));
    } else if (OB_FAIL(wrapper.set_cache_handle(handle))) {
      LOG_WARN("wrapper set cache handle failed", K(ret), K(meta_key), K(complex_addr));
    } else if (OB_ISNULL(auto_inc_seq_cache = wrapper.get_member())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null member", K(ret));
    } else if (!auto_inc_seq_cache->is_valid()) {
      // no need to copy
      free_auto_inc_seq(allocator, ptr);
      ptr = nullptr;
      LOG_INFO("empty auto inc seq", K(ret));
    } else if (OB_FAIL(ptr->assign(allocator, *auto_inc_seq_cache))) {
      LOG_WARN("failed to copy", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected complex addr type", K(ret), K(complex_addr));
  }

  if (OB_FAIL(ret)) {
    if (nullptr != ptr) {
      allocator.free(ptr);
    }
  } else {
    auto_inc_seq = ptr;
  }

  return ret;
}

void ObTabletMdsData::free_mds_dump_kv(
    common::ObIAllocator &allocator,
    const mds::MdsDumpKV *kv)
{
  if (nullptr != kv) {
    kv->mds::MdsDumpKV::~MdsDumpKV();
    allocator.free(const_cast<mds::MdsDumpKV*>(kv));
  }
}

void ObTabletMdsData::free_auto_inc_seq(
    common::ObIAllocator &allocator,
    const share::ObTabletAutoincSeq *auto_inc_seq)
{
  if (nullptr != auto_inc_seq) {
    auto_inc_seq->share::ObTabletAutoincSeq::~ObTabletAutoincSeq();
    allocator.free(const_cast<share::ObTabletAutoincSeq*>(auto_inc_seq));
  }
}

void ObTabletMdsData::free_medium_info_list(
    common::ObIAllocator &allocator,
    const ObTabletDumpedMediumInfo *medium_info_list)
{
  if (nullptr != medium_info_list) {
    medium_info_list->~ObTabletDumpedMediumInfo();
    allocator.free(const_cast<ObTabletDumpedMediumInfo*>(medium_info_list));
  }
}

int ObTabletMdsData::read_medium_info(
    common::ObIAllocator &allocator,
    const ObMetaDiskAddr &addr,
    common::ObSEArray<compaction::ObMediumCompactionInfo*, 1> &array)
{
  int ret = OB_SUCCESS;
  ObSharedBlockLinkIter iter;
  compaction::ObMediumCompactionInfo *info = nullptr;
  char *buf = nullptr;
  int64_t len = 0;

  if (OB_UNLIKELY(!addr.is_block())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("addr is not block addr", K(ret), K(addr));
  } else if (OB_FAIL(iter.init(addr))) {
    LOG_WARN("failed to init link iter", K(ret), K(addr));
  } else {
    while (OB_SUCC(ret)) {
      info = nullptr;
      buf = nullptr;
      len = 0;
      int64_t pos = 0;

      if (OB_FAIL(iter.get_next_block(allocator, buf, len))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get next item", K(ret));
        }
      } else if (OB_ISNULL(buf) || OB_UNLIKELY(0 == len)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, buf is null or len is 0", K(ret), KP(buf), K(len));
      } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, info))) {
        LOG_WARN("failed to alloc and new", K(ret));
      } else if (OB_FAIL(info->deserialize(allocator, buf, len, pos))) {
        LOG_WARN("failed to deserialize medium info", K(ret));
      } else if (OB_FAIL(array.push_back(info))) {
        LOG_WARN("failed to push back to array", K(ret), KPC(info));
      }

      if (OB_FAIL(ret)) {
        if (nullptr != info) {
          allocator.free(info);
        }
      }
    }
  }

  return ret;
}

int ObTabletMdsData::build_mds_data(
    common::ObArenaAllocator &allocator,
    const share::ObTabletAutoincSeq &auto_inc_seq,
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const share::SCN &create_commit_scn,
    const ObTabletBindingInfo &ddl_data,
    const share::SCN &clog_checkpoint_scn,
    const compaction::ObMediumCompactionInfoList &info_list,
    ObTabletMdsData &mds_data)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(build_tablet_status(allocator, tx_data, create_commit_scn, mds_data))) {
    LOG_WARN("failed to build tablet status", K(ret));
  } else if (OB_FAIL(build_aux_tablet_info(allocator, tx_data, ddl_data, clog_checkpoint_scn, mds_data))) {
    LOG_WARN("failed to build binding info", K(ret));
  } else if (OB_FAIL(build_auto_inc_seq(allocator, auto_inc_seq, mds_data))) {
    LOG_WARN("failed to build auto inc seq", K(ret));
  } else {
    mds_data.extra_medium_info_ = info_list.get_extra_medium_info();
    const compaction::ObMediumCompactionInfoList::MediumInfoList &medium_info_list = info_list.get_list();
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, mds_data.medium_info_list_.ptr_))) {
      LOG_WARN("failed to alloc and new mda data medium info list", K(ret));
    } else if (OB_FAIL(mds_data.medium_info_list_.ptr_->init_for_first_creation(allocator))) {
      LOG_WARN("failed to init mda data medium info list", K(ret));
    }

    DLIST_FOREACH(info, medium_info_list) {
      if (OB_FAIL(mds_data.medium_info_list_.ptr_->append(*info))) {
        LOG_WARN("failed to assign medium info", K(ret), K(*info));
      }
    }
  }

  return ret;
}

int ObTabletMdsData::build_tablet_status(
    common::ObArenaAllocator &allocator,
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const share::SCN &create_commit_scn,
    ObTabletMdsData &mds_data)
{
  int ret = OB_SUCCESS;
  ObTabletComplexAddr<mds::MdsDumpKV> &uncommitted_kv = mds_data.tablet_status_.uncommitted_kv_;
  ObTabletComplexAddr<mds::MdsDumpKV> &committed_kv = mds_data.tablet_status_.committed_kv_;
  mds::MdsDumpKey *key = nullptr;
  mds::MdsDumpNode *node = nullptr;

  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, uncommitted_kv.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, committed_kv.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (tx_data.is_in_tx()) {
    key = &uncommitted_kv.ptr_->k_;
    node = &uncommitted_kv.ptr_->v_;
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret), KP(node));
    } else {
      key->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      key->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;
      key->allocator_ = &allocator;
      // no need to serialize dummy key
      key->key_.reset();

      node->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      node->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, ObTabletCreateDeleteMdsUserData>::value;

      node->status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
      node->status_.union_.field_.writer_type_ = mds::WriterType::TRANSACTION;
      node->status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_PREPARE;

      node->allocator_ = &allocator;
      node->writer_id_ = tx_data.tx_id_.get_id();
      //node->seq_no_ = ;
      node->redo_scn_ = tx_data.tx_scn_;
      node->end_scn_ = share::SCN::invalid_scn();
      node->trans_version_ = tx_data.tx_scn_;
    }
  } else {
    key = &committed_kv.ptr_->k_;
    node = &committed_kv.ptr_->v_;
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret), KP(node));
    } else {
      key->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      key->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;
      key->allocator_ = &allocator;
      // no need to serialize dummy key
      key->key_.reset();

      node->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      node->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, ObTabletCreateDeleteMdsUserData>::value;

      node->status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
      node->status_.union_.field_.writer_type_ = mds::WriterType::TRANSACTION;
      if (ObTabletStatus::NORMAL == tx_data.tablet_status_) {
        node->status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_COMMIT;
      } else if (ObTabletStatus::DELETED == tx_data.tablet_status_) {
        // state set as ON_COMMIT even if it may be create abort transaction
        node->status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_COMMIT;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet status", K(ret), "tx_data", tx_data);
      }

      node->allocator_ = &allocator;
      node->writer_id_ = 0;
      //node->seq_no_ = ;
      node->redo_scn_ = share::SCN::invalid_scn();
      node->end_scn_ = tx_data.tx_scn_;
      node->trans_version_ = tx_data.tx_scn_;
    }
  }

  if (OB_SUCC(ret)) {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = tx_data.tablet_status_;
    user_data.create_commit_scn_ = create_commit_scn;
    user_data.create_commit_version_ = tx_data.tx_scn_.get_val_for_tx();
    user_data.transfer_scn_ = tx_data.transfer_scn_;
    user_data.transfer_ls_id_ = tx_data.transfer_ls_id_;
    if (ObTabletStatus::DELETED == tx_data.tablet_status_) {
      //TODO(bizhu) check deleted trans scn
      user_data.delete_commit_scn_ = tx_data.tx_scn_;
      //user_data.delete_commit_version_ = tx_data.tx_scn_;
    }
    const int64_t serialize_size = user_data.get_serialize_size();
    int64_t pos = 0;
    char *buffer = nullptr;
    if (OB_ISNULL(buffer = static_cast<char*>(allocator.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
    } else if (OB_FAIL(user_data.serialize(buffer, serialize_size, pos))) {
      LOG_WARN("user data serialize failed", K(ret), K(user_data));
    } else {
      node->user_data_.assign(buffer, serialize_size);
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(buffer)) {
      allocator.free(buffer);
    }
  }

  return ret;
}

int ObTabletMdsData::build_aux_tablet_info(
    common::ObArenaAllocator &allocator,
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const ObTabletBindingInfo &ddl_data,
    const share::SCN &clog_checkpoint_scn,
    ObTabletMdsData &mds_data)
{
  int ret = OB_SUCCESS;
  ObTabletComplexAddr<mds::MdsDumpKV> &uncommitted_kv = mds_data.aux_tablet_info_.uncommitted_kv_;
  ObTabletComplexAddr<mds::MdsDumpKV> &committed_kv = mds_data.aux_tablet_info_.committed_kv_;
  mds::MdsDumpKey *key = nullptr;
  mds::MdsDumpNode *node = nullptr;

  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, uncommitted_kv.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, committed_kv.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (tx_data.is_in_tx()) {
    key = &uncommitted_kv.ptr_->k_;
    node = &uncommitted_kv.ptr_->v_;
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret), KP(node));
    } else {
      key->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      key->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
      key->allocator_ = &allocator;
      // no need to serialize dummy key
      key->key_.reset();

      node->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      node->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, ObTabletBindingMdsUserData>::value;

      node->status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
      node->status_.union_.field_.writer_type_ = mds::WriterType::TRANSACTION;
      node->status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_PREPARE;

      node->allocator_ = &allocator;
      node->writer_id_ = tx_data.tx_id_.get_id();
      //node->seq_no_ = ;

      node->redo_scn_ = tx_data.tx_scn_;
      node->end_scn_ = share::SCN::invalid_scn();
      node->trans_version_ = tx_data.tx_scn_;
    }
  } else {
    key = &committed_kv.ptr_->k_;
    node = &committed_kv.ptr_->v_;
    if (OB_ISNULL(node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("node is null", K(ret), KP(node));
    } else {
      key->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      key->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletBindingMdsUserData>>::value;
      key->allocator_ = &allocator;
      // no need to serialize dummy key
      key->key_.reset();

      node->mds_table_id_ = mds::TupleTypeIdx<mds::MdsTableTypeTuple, mds::NormalMdsTable>::value;
      node->mds_unit_id_ = mds::TupleTypeIdx<mds::NormalMdsTable, ObTabletBindingMdsUserData>::value;

      node->status_.union_.field_.node_type_ = mds::MdsNodeType::SET;
      node->status_.union_.field_.writer_type_ = mds::WriterType::TRANSACTION;
      node->status_.union_.field_.state_ = mds::TwoPhaseCommitState::ON_COMMIT; // ON_ABORT?

      node->allocator_ = &allocator;
      node->writer_id_ = 0;
      //node->seq_no_ = ;

      node->redo_scn_ = clog_checkpoint_scn;
      node->end_scn_ = clog_checkpoint_scn;
      node->trans_version_ = clog_checkpoint_scn;
    }
  }

  if (OB_SUCC(ret)) {
    ObTabletBindingMdsUserData user_data;
    user_data.redefined_ = ddl_data.redefined_;
    user_data.snapshot_version_ = ddl_data.snapshot_version_;
    user_data.schema_version_ = ddl_data.schema_version_;
    user_data.data_tablet_id_ = ddl_data.data_tablet_id_;
    if (!ddl_data.hidden_tablet_ids_.empty()) {
      user_data.hidden_tablet_id_ = ddl_data.hidden_tablet_ids_.at(0); // only have one valid hidden tablet
    }
    user_data.lob_meta_tablet_id_ = ddl_data.lob_meta_tablet_id_;
    user_data.lob_piece_tablet_id_ = ddl_data.lob_piece_tablet_id_;

    const int64_t serialize_size = user_data.get_serialize_size();
    int64_t pos = 0;
    char *buffer = nullptr;
    if (OB_ISNULL(buffer = static_cast<char*>(allocator.alloc(serialize_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(serialize_size));
    } else if (OB_FAIL(user_data.serialize(buffer, serialize_size, pos))) {
      LOG_WARN("user data serialize failed", K(ret), K(user_data));
    } else {
      node->user_data_.assign(buffer, serialize_size);
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(buffer)) {
      allocator.free(buffer);
    }
  }

  return ret;
}

int ObTabletMdsData::build_auto_inc_seq(
    common::ObArenaAllocator &allocator,
    const share::ObTabletAutoincSeq &auto_inc_seq,
    ObTabletMdsData &mds_data)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, mds_data.auto_inc_seq_.ptr_))) {
    LOG_WARN("failed to alloc and new", K(ret));
  } else if (OB_FAIL(mds_data.auto_inc_seq_.ptr_->assign(allocator, auto_inc_seq))) {
    LOG_WARN("failed to copy auto inc seq", K(ret), K(auto_inc_seq));
  }

  return ret;
}

int ObTabletMdsData::set_tablet_status(
    common::ObIAllocator &allocator,
    const ObTabletStatus::Status &tablet_status,
    const ObTabletMdsUserDataType &data_type)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  user_data.tablet_status_ = tablet_status;
  user_data.data_type_ = data_type;

  const int64_t length = user_data.get_serialize_size();
  char *buffer = static_cast<char*>(allocator.alloc(length));
  int64_t pos = 0;
  if (OB_ISNULL(buffer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", K(ret), K(length));
  } else if (OB_FAIL(user_data.serialize(buffer, length, pos))) {
    LOG_WARN("failed to serialize", K(ret));
  } else if (OB_FAIL(tablet_status_cache_.assign(user_data))) {
    LOG_WARN("failed to assign tablet status cache", K(ret));
  } else {
    mds::MdsDumpNode &node = tablet_status_.committed_kv_.get_ptr()->v_;
    node.allocator_ = &allocator;
    node.user_data_.assign(buffer, length);
  }

  if (OB_FAIL(ret)) {
    if (nullptr != buffer) {
      allocator.free(buffer);
    }
  }
  return ret;
}

int ObTabletMdsData::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tablet_status_,
      aux_tablet_info_,
      extra_medium_info_,
      medium_info_list_,
      auto_inc_seq_,
      tablet_status_cache_);

  return ret;
}

int ObTabletMdsData::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_status_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(aux_tablet_info_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(extra_medium_info_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(medium_info_list_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(auto_inc_seq_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else if (OB_FAIL(tablet_status_cache_.deserialize(buf, data_len, pos))) {
    LOG_WARN("failed to deserialize", K(ret));
  } else {
    is_inited_ = true;
    LOG_INFO("succeeded to deserialize mds data", K(ret), KPC(this));
  }

  return ret;
}

int64_t ObTabletMdsData::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tablet_status_,
      aux_tablet_info_,
      extra_medium_info_,
      medium_info_list_,
      auto_inc_seq_,
      tablet_status_cache_);

  return len;
}
} // namespace storage
} // namespace oceanbase

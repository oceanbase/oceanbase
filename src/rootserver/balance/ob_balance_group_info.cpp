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
#define USING_LOG_PREFIX BALANCE

#include "ob_balance_group_info.h"

namespace oceanbase
{
using namespace share;
namespace rootserver
{
///////////////////////////////////////////////

ObBalanceGroupInfo::~ObBalanceGroupInfo()
{
  inited_ = false;
  bg_id_.reset();
  ls_id_.reset();
  last_part_group_ = nullptr;
  if (OB_NOT_NULL(pg_container_)) {
    pg_container_->~ObPartGroupContainer();
    alloc_.free(pg_container_);
    pg_container_ = nullptr;
  }
}

int ObBalanceGroupInfo::init(
    const ObBalanceGroupID &bg_id,
    const share::ObLSID &ls_id,
    const int64_t balanced_ls_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || !ls_id.is_valid() || balanced_ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_id), K(ls_id), K(balanced_ls_num));
  } else if (OB_FAIL(create_part_group_container_(bg_id, balanced_ls_num))) {
    LOG_WARN("create part group container failed", KR(ret), K(bg_id), K(balanced_ls_num));
  } else {
    bg_id_ = bg_id;
    ls_id_ = ls_id;
    last_part_group_ = nullptr;
    inited_ = true;
  }
  return ret;
}

int ObBalanceGroupInfo::create_part_group_container_(
    const ObBalanceGroupID &bg_id,
    const int64_t balanced_ls_num)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_ || nullptr != pg_container_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K(inited_), KP(pg_container_));
  } else {
    void *buf = nullptr;
    if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObPartGroupContainer)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret), K(bg_id), K(balanced_ls_num));
    } else if (OB_ISNULL(pg_container_ = new(buf) ObPartGroupContainer(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret));
    } else if (OB_FAIL(pg_container_->init(bg_id, balanced_ls_num))) {
      LOG_WARN("failed to init pg container", KR(ret), K(bg_id), K(balanced_ls_num));
    }
  }
  return ret;
}

int ObBalanceGroupInfo::append_part(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const ObTransferPartInfo &part,
    const int64_t data_size,
    const uint64_t part_group_uid,
    const int64_t balance_weight)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(
      !part.is_valid()
      || data_size < 0
      || !is_valid_id(part_group_uid))
      || !table_schema.is_valid()
      || balance_weight < 0) {
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size),
        K(balance_weight), K(part_group_uid), K(table_schema));
  } else {
    if (OB_FAIL(create_new_part_group_if_needed_(table_schema, part_group_uid))) {
      LOG_WARN("create new part group if needed failed", KR(ret), K(part_group_uid));
    } else if (OB_ISNULL(last_part_group_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("no part group in this balance group", KR(ret), KPC(this), K(part));
    } else if (OB_FAIL(last_part_group_->add_part(part, data_size, balance_weight))) {
      LOG_WARN("add part into partition group fail", KR(ret), KPC(last_part_group_), K(part),
          K(data_size), K(balance_weight), K(part_group_uid), KPC(this));
    } else {
      LOG_TRACE("[ObBalanceGroupInfo] append part", K(part), K(data_size),
          K(part_group_uid), K(balance_weight), K(table_schema));
    }
  }
  return ret;
}

// Only create new part group when part_group_uid is different.
int ObBalanceGroupInfo::create_new_part_group_if_needed_(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid)
{
int ret = OB_SUCCESS;
if (OB_UNLIKELY(!is_valid_id(part_group_uid))) {
  ret = OB_INVALID_ARGUMENT;
  LOG_WARN("invalid args", KR(ret), K(part_group_uid));
} else if (OB_ISNULL(last_part_group_) || !last_part_group_->is_same_pg(part_group_uid)) {
  // Non-partiitoned tables: a balance group contains multiple bg_unit splited by database_id.
  // Partitioned tables and tablegroup: a balance group contains only one bg_unit.
  //
  // Note: bg_unit_id is part_group level, not part level, because in sharding none tablegroup,
  //       a part_group will contain a global index without tablegroup_id.
  uint64_t bg_unit_id = (OB_INVALID_ID != table_schema.get_tablegroup_id()
      ? table_schema.get_tablegroup_id()
      : table_schema.get_database_id());
  ObPartGroupInfo *part_group = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObPartGroupInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory for partition group fail", KR(ret), K(buf));
  } else if (OB_ISNULL(part_group = new(buf) ObPartGroupInfo(alloc_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("construct ObPartGroupInfo fail", KR(ret), K(buf));
  } else if (OB_FAIL(part_group->init(part_group_uid, bg_unit_id))) {
    LOG_WARN("init part group failed", KR(ret), K(part_group_uid), K(bg_unit_id));
  } else if (OB_ISNULL(pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP_(pg_container));
  } else if (OB_FAIL(pg_container_->append_part_group(part_group))) {
    LOG_WARN("append new part group failed", KR(ret),  K(part_group_uid), KPC_(pg_container));
  } else {
    last_part_group_ = part_group;
  }
}
return ret;
}

int ObBalanceGroupInfo::transfer_out_by_round_robin(
    ObBalanceGroupInfo &dest_bg_info,
    ObPartGroupInfo *&part_group)
{
  return inner_transfer_out_(0/*balance_weight*/, dest_bg_info, part_group);
}

int ObBalanceGroupInfo::transfer_out_by_balance_weight(
    const int64_t balance_weight,
    ObBalanceGroupInfo &dest_bg_info,
    ObPartGroupInfo *&part_group)
{
  return inner_transfer_out_(balance_weight, dest_bg_info, part_group);
}

int ObBalanceGroupInfo::inner_transfer_out_(
    const int64_t balance_weight,
    ObBalanceGroupInfo &dest_bg_info,
    ObPartGroupInfo *&part_group)
{
  int ret = OB_SUCCESS;
  part_group = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(balance_weight < 0
      || !dest_bg_info.is_valid()
      || bg_id_ != dest_bg_info.bg_id_
      || ls_id_ == dest_bg_info.ls_id_)) {
    // dest_bg_info must have different ls id from this
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest_bg_info is invalid", KR(ret), K(balance_weight), K(*this), K(dest_bg_info));
  } else if (OB_ISNULL(pg_container_) || OB_ISNULL(dest_bg_info.pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP(pg_container_), KP(dest_bg_info.pg_container_));
  } else if (OB_FAIL(pg_container_->select(balance_weight, *dest_bg_info.pg_container_, part_group))) {
    LOG_WARN("fail to select a part group to transfer out",
        KR(ret), KPC(pg_container_), KPC(dest_bg_info.pg_container_), K(balance_weight));
  } else if (OB_ISNULL(part_group)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid pg", KR(ret), K(part_group), KPC(pg_container_),
        KPC(dest_bg_info.pg_container_), K(balance_weight));
  } else if (OB_FAIL(pg_container_->remove_part_group(part_group))) {
    LOG_WARN("fail to remove selected part group", KR(ret), KPC_(pg_container), KPC(part_group));
  } else if (OB_FAIL(dest_bg_info.pg_container_->append_part_group(part_group))) {
    LOG_WARN("fail to append selected part group to dest_bg_info", KR(ret), K(dest_bg_info), K(part_group));
  }
  return ret;
}

int ObBalanceGroupInfo::swap_largest_for_smallest_pg(ObBalanceGroupInfo &dest_bg_info)
{
  int ret = OB_SUCCESS;
  ObPartGroupInfo *largest_pg = nullptr;
  ObPartGroupInfo *smallest_pg = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_UNLIKELY(!dest_bg_info.is_valid()
      || bg_id_ != dest_bg_info.bg_id_
      || ls_id_ == dest_bg_info.ls_id_)) {
    // dest_bg_info must have different ls id from this
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest_bg_info is invalid", KR(ret), K(*this), K(dest_bg_info));
  } else if (OB_ISNULL(pg_container_) || OB_ISNULL(dest_bg_info.pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP(pg_container_), KP(dest_bg_info.pg_container_));
  } else if (OB_FAIL(pg_container_->get_largest_part_group(largest_pg))) {
    LOG_WARN("get largest pg info failed", KR(ret), KPC(pg_container_));
  } else if (OB_FAIL(dest_bg_info.pg_container_->get_smallest_part_group(smallest_pg))) {
    LOG_WARN("get smallest pg info failed", KR(ret), K(dest_bg_info));
  } else if (OB_ISNULL(largest_pg) || OB_ISNULL(smallest_pg)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null pg info", KR(ret), KPC(pg_container_), KP(largest_pg), KP(smallest_pg));
  } else if (OB_UNLIKELY(largest_pg->get_data_size() <= smallest_pg->get_data_size())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("largest pg can not smaller than smallest", KR(ret),
        KPC(pg_container_), KPC(largest_pg), KPC(smallest_pg));
  } else if (OB_FAIL(pg_container_->remove_part_group(largest_pg))) {
    LOG_WARN("remove failed", KR(ret), KPC(pg_container_), K(largest_pg));
  } else if (OB_FAIL(dest_bg_info.pg_container_->append_part_group(largest_pg))) {
    LOG_WARN("append failed", KR(ret), K(dest_bg_info), K(largest_pg));
  } else if (OB_FAIL(dest_bg_info.pg_container_->remove_part_group(smallest_pg))) {
    LOG_WARN("remove failed", KR(ret), K(dest_bg_info), K(smallest_pg));
  } else if (OB_FAIL(pg_container_->append_part_group(smallest_pg))) {
    LOG_WARN("append failed", KR(ret), KPC(pg_container_), K(smallest_pg));
  }
  return ret;
}

int ObBalanceGroupInfo::get_balance_weight_array(
    ObIArray<int64_t> &weight_arr)
{
  int ret = OB_SUCCESS;
  weight_arr.reset();
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(pg_container_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_));
  } else if (OB_FAIL(pg_container_->get_balance_weight_array(weight_arr))) {
    LOG_WARN("get balance weight array failed", KR(ret));
  }
  return ret;
}

int ObBalanceGroupInfo::get_largest_part_group(ObPartGroupInfo *&pg) const
{
  int ret = OB_SUCCESS;
  pg = nullptr;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(pg_container_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_), KP(pg_container_));
  } else if (OB_FAIL(pg_container_->get_largest_part_group(pg))) {
    LOG_WARN("fail to get largest pg", KR(ret), KPC(pg_container_));
  }
  return ret;
}

int ObBalanceGroupInfo::get_smallest_part_group(ObPartGroupInfo *&pg) const
{
  int ret = OB_SUCCESS;
  pg = nullptr;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(pg_container_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_), KP(pg_container_));
  } else if (OB_FAIL(pg_container_->get_smallest_part_group(pg))) {
    LOG_WARN("fail to get smallest_pg", KR(ret), KPC(pg_container_));
  }
  return ret;
}

int ObBalanceGroupInfo::split_out_weighted_bg_info(ObBalanceGroupInfo &weighted_bg_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_) || OB_ISNULL(pg_container_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_), KP(pg_container_));
  } else if (OB_UNLIKELY(!weighted_bg_info.is_valid()
      || weighted_bg_info.get_ls_id() != ls_id_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid weighted_bg_info", KR(ret), K(weighted_bg_info), KPC(this));
  } else {
    ObPartGroupContainer *weighted_container = weighted_bg_info.pg_container_;
    if (OB_ISNULL(weighted_container)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null weighted_container", KR(ret), K(weighted_bg_info));
    } else if (OB_FAIL(pg_container_->split_out_weighted_part_groups(*weighted_container))) {
      LOG_WARN("split out weighted part groups failed", KR(ret), KPC(this));
    }
  }
  return ret;
}

}
}

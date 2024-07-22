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

int ObTransferPartGroup::add_part(const ObTransferPartInfo &part, int64_t data_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!part.is_valid() || data_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size));
  } else if (OB_FAIL(part_list_.push_back(part))) {
    LOG_WARN("push back part into part info fail", KR(ret), K(part), K(part_list_));
  } else {
    data_size_ += data_size;
  }
  return ret;
}

///////////////////////////////////////////////

ObBalanceGroupInfo::~ObBalanceGroupInfo()
{
  inited_ = false;
  bg_id_.reset();
  ls_id_.reset();
  last_part_group_uid_ = OB_INVALID_ID;
  last_part_group_ = nullptr;
  if (OB_NOT_NULL(pg_container_)) {
    pg_container_->~ObIPartGroupContainer();
    alloc_.free(pg_container_);
    pg_container_ = nullptr;
  }
}

int ObBalanceGroupInfo::init(
    const ObBalanceGroupID &bg_id,
    const share::ObLSID &ls_id,
    const int64_t balanced_ls_num,
    const ObPartDistributionMode &part_distribution_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || !ls_id.is_valid() || balanced_ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_id), K(ls_id), K(balanced_ls_num));
  } else if (OB_FAIL(create_part_group_container_(bg_id, balanced_ls_num, part_distribution_mode))) {
    LOG_WARN("create part group container failed", KR(ret), K(bg_id), K(balanced_ls_num),
            K(part_distribution_mode));
  } else {
    bg_id_ = bg_id;
    ls_id_ = ls_id;
    last_part_group_uid_ = OB_INVALID_ID;
    last_part_group_ = nullptr;
    inited_ = true;
  }
  return ret;
}

int ObBalanceGroupInfo::append_part(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    const ObTransferPartInfo &part,
    const int64_t data_size)
{
  int ret = OB_SUCCESS;
  ObTransferPartGroup *part_group = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!part.is_valid() || data_size < 0
                        || !is_valid_id(part_group_uid))
                        || !table_schema.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size), K(part_group_uid), K(table_schema));
  } else if (OB_FAIL(create_new_part_group_if_needed_(table_schema, part_group_uid))) {
    LOG_WARN("create new part group if needed failed", KR(ret), K(part_group_uid),
            K_(last_part_group_uid));
  } else if (OB_ISNULL(part_group = last_part_group_)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no partition groups in this balance group", KPC(this), KR(ret), K(part));
  } else if (OB_FAIL(part_group->add_part(part, data_size))) {
    LOG_WARN("add part into partition group fail", KR(ret), KPC(part_group), K(part),
            K(data_size), K(part_group_uid), KPC(this));
  } else {
    LOG_TRACE("[ObBalanceGroupInfo] append part", K(part), K(data_size), K(table_schema),
              K(part_group_uid), "part_group_count", get_part_group_count(), KPC(part_group));
  }
  return ret;
}

int ObBalanceGroupInfo::create_new_part_group_if_needed_(
    const schema::ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  if (part_group_uid != last_part_group_uid_) {
    // only create new part group when part_group_uid is different from last_part_group_uid_
    // (Scenarios with invalid last_part_group_uid_ have been included)
    ObTransferPartGroup *part_group = nullptr;
    const int64_t part_group_size = sizeof(ObTransferPartGroup);
    void *buf = alloc_.alloc(part_group_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for partition group fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_ISNULL(part_group = new(buf) ObTransferPartGroup(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("construct ObTransferPartGroup fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_ISNULL(pg_container_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("part group container is null", KR(ret), KP_(pg_container));
    } else if (OB_FAIL(pg_container_->append_new_part_group(table_schema, part_group_uid, part_group))) {
      LOG_WARN("fail to append part group to the container", KR(ret), K(table_schema),
              K(part_group_uid), KPC_(pg_container));
    } else {
      last_part_group_uid_ = part_group_uid;
      last_part_group_ = part_group;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::create_part_group_container_(
    const ObBalanceGroupID &bg_id,
    const int64_t balanced_ls_num,
    const ObPartDistributionMode &part_distribution_mode)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_ || nullptr != pg_container_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", KR(ret), K_(inited), KPC_(pg_container));
  } else if (ObPartDistributionMode::CONTINUOUS == part_distribution_mode) {
    ObContinuousPartGroupContainer *pg_container_continuous = nullptr;
    if (OB_ISNULL(pg_container_continuous = reinterpret_cast<ObContinuousPartGroupContainer *>(
        alloc_.alloc(sizeof(ObContinuousPartGroupContainer))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret));
    } else if (OB_ISNULL(pg_container_continuous =
        new(pg_container_continuous) ObContinuousPartGroupContainer(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret));
    } else if (OB_FAIL(pg_container_continuous->init())) {
      LOG_WARN("failed to init continuous part group container", KR(ret));
    } else {
      pg_container_ = pg_container_continuous;
    }
  } else if (ObPartDistributionMode::ROUND_ROBIN == part_distribution_mode) {
    ObRRPartGroupContainer *pg_container_rr = nullptr;
    if (OB_ISNULL(pg_container_rr = reinterpret_cast<ObRRPartGroupContainer *>(
        alloc_.alloc(sizeof(ObRRPartGroupContainer))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret));
    } else if (OB_ISNULL(pg_container_rr = new(pg_container_rr) ObRRPartGroupContainer(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc mem fail", KR(ret));
    } else if (OB_FAIL(pg_container_rr->init(bg_id, balanced_ls_num))) {
      LOG_WARN("failed to init round robin part group container", KR(ret), K(balanced_ls_num));
    } else {
      pg_container_ = pg_container_rr;
    }
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("unknown part distribution mode", K(part_distribution_mode));
  }
  return ret;
}

int ObBalanceGroupInfo::transfer_out(
    ObBalanceGroupInfo &dst_bg_info,
    ObIPartGroupInfo *&pg_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!dst_bg_info.is_valid() || bg_id_ != dst_bg_info.bg_id_
                        || ls_id_ == dst_bg_info.ls_id_)) {
    // dst_bg_info must have different ls id from this
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dst_bg_info is invalid", KR(ret), K(*this), K(dst_bg_info));
  } else if (OB_ISNULL(pg_container_) || OB_ISNULL(dst_bg_info.pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP_(pg_container), K_(dst_bg_info.pg_container));
  } else if (OB_FAIL(pg_container_->select(*dst_bg_info.pg_container_, pg_info))) {
    LOG_WARN("fail to select a part group to transfer out", KR(ret), KPC_(pg_container),
            KPC_(dst_bg_info.pg_container));
  } else if (OB_ISNULL(pg_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group info is null", KR(ret), K(pg_info));
  } else if (OB_FAIL(pg_container_->remove(*pg_info))) {
    LOG_WARN("fail to remove selected part group", KR(ret), KPC_(pg_container), KPC(pg_info));
  } else if (OB_FAIL(dst_bg_info.pg_container_->append(*pg_info))) {
    LOG_WARN("fail to append selected part group to dst_bg_info", KR(ret), K(dst_bg_info),
            KPC(pg_info));
  } else {
    if (last_part_group_ == pg_info->part_group()) {
      last_part_group_ = nullptr;
      last_part_group_uid_ = OB_INVALID_ID;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::get_largest_part_group(
    ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_ISNULL(pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP_(pg_container));
  } else if (OB_FAIL(pg_container_->get_largest(pg_info))) {
    LOG_WARN("fail to get the largest part group", KR(ret), KPC_(pg_container));
  }
  return ret;
}

int ObBalanceGroupInfo::get_smallest_part_group(
    ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_ISNULL(pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP_(pg_container));
  } else if (OB_FAIL(pg_container_->get_smallest(pg_info))) {
    LOG_WARN("fail to get the smallest part group", KR(ret), KPC_(pg_container));
  }
  return ret;
}

int ObBalanceGroupInfo::remove_part_group(const ObIPartGroupInfo &pg_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!pg_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg_info is invalid", KR(ret), K(pg_info));
  } else if (OB_ISNULL(pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP_(pg_container));
  } else if (OB_FAIL(pg_container_->remove(pg_info))) {
    LOG_WARN("fail to remove part group", KR(ret), KPC_(pg_container), K(pg_info));
  } else if (last_part_group_ == pg_info.part_group()) {
    last_part_group_ = nullptr;
    last_part_group_uid_ = OB_INVALID_ID;
  }
  return ret;
}

int ObBalanceGroupInfo::append_part_group(const ObIPartGroupInfo &pg_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else if (OB_UNLIKELY(!pg_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg_info is invalid", KR(ret), K(pg_info));
  } else if (OB_ISNULL(pg_container_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group container is null", KR(ret), KP_(pg_container));
  } else if (OB_FAIL(pg_container_->append(pg_info))) {
    LOG_WARN("fail to append part group", KR(ret), KPC_(pg_container), K(pg_info));
  }
  return ret;
}

}
}

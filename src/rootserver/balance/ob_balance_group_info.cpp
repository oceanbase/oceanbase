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
  if (OB_UNLIKELY(! part.is_valid() || data_size < 0)) {
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
  // for each partition group in array, release its memory
  for (int64_t i = 0; i < part_groups_.count(); i++) {
    ObTransferPartGroup *part_group = part_groups_.at(i);
    if (OB_NOT_NULL(part_group)) {
      part_group->~ObTransferPartGroup();
      alloc_.free(part_group);
      part_group = NULL;
    }
  }

  part_groups_.destroy();
}


int ObBalanceGroupInfo::append_part(ObTransferPartInfo &part,
    const int64_t data_size,
    const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! part.is_valid() || data_size < 0 || !is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part), K(data_size));
  } else if (OB_FAIL(create_new_part_group_if_needed_(part_group_uid))) {
    LOG_WARN("create new part group if needed failed", KR(ret), K(part_group_uid), K_(last_part_group_uid));
  } else if (OB_UNLIKELY(part_groups_.count() <= 0)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("no partition groups in this balance group", KPC(this), KR(ret), K(part));
  } else {
    ObTransferPartGroup *part_group = part_groups_.at(part_groups_.count() - 1);

    if (OB_ISNULL(part_group)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid data", KR(ret), KPC(part_group), KPC(this));
    } else if (OB_FAIL(part_group->add_part(part, data_size))) {
      LOG_WARN("add part into partition group fail", KR(ret),
          KPC(part_group), K(part), K(data_size), K(part_group_uid), KPC(this));
    }

    LOG_TRACE("[ObBalanceGroupInfo] append part", K(part), K(data_size), K(part_group_uid),
        "part_group_count", part_groups_.count(), KPC(part_group));
  }
  return ret;
}

int ObBalanceGroupInfo::create_new_part_group_if_needed_(const uint64_t part_group_uid)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid_id(part_group_uid))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_group_uid", KR(ret), K(part_group_uid));
  } else if (part_group_uid != last_part_group_uid_) {
    // only create new part group when part_group_uid is different from last_part_group_uid_
    // (Scenarios with invalid last_part_group_uid_ have been included)
    ObTransferPartGroup *part_group = NULL;
    const int64_t part_group_size = sizeof(ObTransferPartGroup);
    void *buf = alloc_.alloc(part_group_size);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for partition group fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_ISNULL(part_group = new(buf) ObTransferPartGroup(alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("construct ObTransferPartGroup fail", KR(ret), K(buf), K(part_group_size));
    } else if (OB_FAIL(part_groups_.push_back(part_group))) {
      LOG_WARN("push back new partition group fail", KR(ret), K(part_group), K(part_groups_));
    } else {
      last_part_group_uid_ = part_group_uid;
    }
  }
  return ret;
}

int ObBalanceGroupInfo::pop_back(const int64_t part_group_count,
    share::ObTransferPartList &part,
    int64_t &popped_part_count)
{
  int ret = OB_SUCCESS;

  popped_part_count = 0;
  if (OB_UNLIKELY(part_group_count > part_groups_.count() || part_group_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part_group_count", KR(ret), K(part_group_count), K(part_groups_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < part_group_count; i++) {
      ObTransferPartGroup *pg = NULL;
      if (OB_FAIL(part_groups_.pop_back(pg))) {
        LOG_WARN("pop back from part group array fail", KR(ret), K(part_groups_));
      } else if (OB_ISNULL(pg)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid part group, is NULL, unexpected", K(pg), K(i), K(part_groups_));
      } else if (FALSE_IT(popped_part_count += pg->count())) {
      } else if (OB_FAIL(append(part, pg->get_part_list()))) {
        LOG_WARN("append array to part list fail", KR(ret), K(part), KPC(pg));
      } else {
        // the last part group has been popped, reset the uid
        last_part_group_uid_ = OB_INVALID_ID;
      }

      // free pg memory anyway
      if (OB_NOT_NULL(pg)) {
        pg->~ObTransferPartGroup();
        alloc_.free(pg);
        pg = NULL;
      }
    }
  }
  return ret;
}

}
}

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

#include "ob_part_group_container.h"
#include "ob_balance_group_info.h"

namespace oceanbase
{
using namespace share;
namespace rootserver
{

///////////////////////////////////////////////
// ObContinuousPartGroupInfo

int ObContinuousPartGroupInfo::init_(
    const int64_t pg_idx,
    ObTransferPartGroup *const part_group)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(pg_idx < 0 || nullptr == part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pg_idx), K(part_group));
  } else {
    pg_idx_ = pg_idx;
    part_group_ = part_group;
    is_inited_ = true;
  }
  return ret;
}

void ObContinuousPartGroupInfo::reset()
{
  pg_idx_ = OB_INVALID_INDEX;
  ObIPartGroupInfo::reset();
}

///////////////////////////////////////////////
// ObContinuousPartGroupContainer

ObContinuousPartGroupContainer::~ObContinuousPartGroupContainer()
{
  ARRAY_FOREACH_NORET(part_groups_, idx) {
    ObTransferPartGroup *part_group = part_groups_.at(idx);
    if (OB_NOT_NULL(part_group)) {
      part_group->~ObTransferPartGroup();
      alloc_.free(part_group);
      part_group = nullptr;
    }
  }
  part_groups_.destroy();
}

int ObContinuousPartGroupContainer::append_new_part_group(
    const share::schema::ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    ObTransferPartGroup *const part_group)
{
  UNUSEDx(table_schema, part_group_uid);
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_group));
  } else if (OB_FAIL(part_groups_.push_back(part_group))) {
    LOG_WARN("fail to append part group", KR(ret), K_(part_groups), KPC(part_group));
  }
  return ret;
}

int ObContinuousPartGroupContainer::append(const ObIPartGroupInfo &pg_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!pg_info.is_valid()
                        || ObPartDistributionMode::CONTINUOUS != pg_info.get_part_distribution_mode())) {
    LOG_WARN("invalid argument", KR(ret), K(pg_info));
  } else {
    const ObContinuousPartGroupInfo &pg_info_continuous =
      static_cast<const ObContinuousPartGroupInfo&>(pg_info);
    ObTransferPartGroup *part_group = pg_info_continuous.part_group();
    if (OB_ISNULL(part_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), KP(part_group));
    } else if (OB_FAIL(part_groups_.push_back(part_group))) {
      LOG_WARN("fail to append part group", KR(ret), K_(part_groups), KPC(part_group));
    }
  }
  return ret;
}

int ObContinuousPartGroupContainer::remove(const ObIPartGroupInfo &pg_info) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!pg_info.is_valid()
                        || ObPartDistributionMode::CONTINUOUS != pg_info.get_part_distribution_mode())) {
    LOG_WARN("invalid argument", KR(ret), K(pg_info));
  } else {
    const ObContinuousPartGroupInfo &pg_info_continuous =
      static_cast<const ObContinuousPartGroupInfo &>(pg_info);
    const int64_t pg_idx = pg_info_continuous.pg_idx_;
    ObTransferPartGroup *part_group = pg_info_continuous.part_group();
    if (pg_idx < 0 || pg_idx >= part_groups_.count()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part group index", KR(ret), K_(part_groups), K(pg_idx));
    } else if (OB_UNLIKELY(part_groups_.at(pg_idx) != part_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part group to remove", KR(ret), K_(part_groups),
              K(pg_idx), KPC(part_group));
    } else if (OB_FAIL(part_groups_.remove(pg_idx))) {
      LOG_WARN("fail to remove part group", KR(ret), K_(part_groups), K(pg_idx));
    }
  }
  return ret;
}

int ObContinuousPartGroupContainer::select(
    ObIPartGroupContainer &dst_pg,
    ObIPartGroupInfo *&pg_info) const
{
  UNUSED(dst_pg);
  int ret = OB_SUCCESS;
  ObContinuousPartGroupInfo *pg_info_continuous = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(part_groups_.empty())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("part groups are empty", KR(ret), K_(part_groups));
  } else if (OB_FAIL(create_part_group_info_if_needed_(pg_info, pg_info_continuous))) {
    LOG_WARN("create part group info failed", KR(ret), KP(pg_info));
  } else if (OB_ISNULL(pg_info_continuous)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group info", KR(ret), KP(pg_info_continuous));
  } else {
    int64_t pg_idx = part_groups_.count() - 1;
    ObTransferPartGroup *part_group = part_groups_.at(pg_idx);
    if (OB_FAIL(pg_info_continuous->init_(pg_idx, part_group))) {
      LOG_WARN("failed to init part group info", KR(ret), K(pg_idx), KPC(this));
    }
  }
  return ret;
}

int ObContinuousPartGroupContainer::get_largest(ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  ObContinuousPartGroupInfo *pg_info_continuous = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(create_part_group_info_if_needed_(pg_info, pg_info_continuous))) {
    LOG_WARN("create part group info failed", KR(ret), KP(pg_info));
  } else if (OB_ISNULL(pg_info_continuous)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group info", KR(ret), KP(pg_info_continuous));
  } else {
    int64_t largest_pg_size = 0;
    int64_t largest_pg_idx = OB_INVALID_INDEX;
    ObTransferPartGroup *largest_part_group = nullptr;
    ARRAY_FOREACH(part_groups_, pg_idx) {
      ObTransferPartGroup *part_group = part_groups_.at(pg_idx);
      if (OB_ISNULL(part_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group is null", KR(ret), K(part_group));
      } else if (OB_INVALID_INDEX == largest_pg_idx
                || part_group->get_data_size() > largest_pg_size) {
        largest_pg_idx = pg_idx;
        largest_pg_size = part_group->get_data_size();
        largest_part_group = part_group;
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == largest_pg_idx)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the largest part group", KR(ret));
    } else if (OB_FAIL(pg_info_continuous->init_(largest_pg_idx, largest_part_group))) {
      LOG_WARN("fail to init pg_info", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObContinuousPartGroupContainer::get_smallest(ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  ObContinuousPartGroupInfo *pg_info_continuous = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(create_part_group_info_if_needed_(pg_info, pg_info_continuous))) {
    LOG_WARN("create part group info failed", KR(ret), KP(pg_info));
  } else if (OB_ISNULL(pg_info_continuous)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group info", KR(ret), KP(pg_info_continuous));
  } else {
    int64_t smallest_pg_size = 0;
    int64_t smallest_pg_idx = OB_INVALID_INDEX;
    ObTransferPartGroup *smallest_part_group = nullptr;
    ARRAY_FOREACH(part_groups_, pg_idx) {
      ObTransferPartGroup *part_group = part_groups_.at(pg_idx);
      if (OB_ISNULL(part_group)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group is null", KR(ret), K(part_group));
      } else if (OB_INVALID_INDEX == smallest_pg_idx
                || part_group->get_data_size() < smallest_pg_size) {
        smallest_pg_idx = pg_idx;
        smallest_pg_size = part_group->get_data_size();
        smallest_part_group = part_group;
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == smallest_pg_idx)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the smallest part group", KR(ret));
    } else if (OB_FAIL(pg_info_continuous->init_(smallest_pg_idx, smallest_part_group))) {
      LOG_WARN("fail to init pg_info", KR(ret), KPC(this));
    }
  }
  return ret;
}

int ObContinuousPartGroupContainer::create_part_group_info_if_needed_(
    ObIPartGroupInfo *&pg_info,
    ObContinuousPartGroupInfo *&pg_info_continuous) const
{
  int ret = OB_SUCCESS;
  pg_info_continuous = nullptr;
  if (OB_ISNULL(pg_info)) {
    if (OB_ISNULL(pg_info_continuous = reinterpret_cast<ObContinuousPartGroupInfo *>(
        alloc_.alloc(sizeof(ObContinuousPartGroupInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for part group info", KR(ret));
    } else if (OB_ISNULL(pg_info_continuous = new(pg_info_continuous) ObContinuousPartGroupInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for part group info", KR(ret));
    } else {
      pg_info = pg_info_continuous;
    }
  } else if (ObPartDistributionMode::CONTINUOUS != pg_info->get_part_distribution_mode()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(pg_info));
  } else {
    pg_info->reset();
    pg_info_continuous = static_cast<ObContinuousPartGroupInfo *>(pg_info);
  }
  return ret;
}

///////////////////////////////////////////////
// ObRRPartGroupInfo

ObRRPartGroupInfo::~ObRRPartGroupInfo()
{
  bg_unit_id_ = OB_INVALID_ID;
  bucket_idx_ = OB_INVALID_INDEX;
  pg_idx_ = OB_INVALID_INDEX;
}

int ObRRPartGroupInfo::init_(const ObObjectID &bg_unit_id,
                            const int64_t bucket_idx,
                            const int64_t pg_idx,
                            ObTransferPartGroup *const part_group)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!is_valid_id(bg_unit_id) || bucket_idx < 0 || pg_idx < 0
                        || nullptr == part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_unit_id), K(bucket_idx), K(pg_idx), KP(part_group));
  } else {
    bg_unit_id_ = bg_unit_id;
    bucket_idx_ = bucket_idx;
    pg_idx_ = pg_idx;
    part_group_ = part_group;
    is_inited_ = true;
  }
  return ret;
}

void ObRRPartGroupInfo::reset()
{
  bg_unit_id_ = OB_INVALID_ID;
  bucket_idx_ = OB_INVALID_INDEX;
  pg_idx_ = OB_INVALID_INDEX;
  ObIPartGroupInfo::reset();
}

///////////////////////////////////////////////
// ObBalanceGroupUnit

ObBalanceGroupUnit::~ObBalanceGroupUnit()
{
  inited_ = false;
  ARRAY_FOREACH_NORET(part_group_buckets_, i) {
    ObPartGroupBucket &bucket = part_group_buckets_.at(i);
    ARRAY_FOREACH_NORET(bucket, j) {
      ObTransferPartGroup *part_group = bucket.at(j);
      if (OB_NOT_NULL(part_group)) {
        part_group->~ObTransferPartGroup();
        alloc_.free(part_group);
        part_group = NULL;
      }
    }
    bucket.destroy();
  }
  part_group_buckets_.destroy();
  part_group_cnt_ = 0;
}

int ObBalanceGroupUnit::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  part_group_buckets_.reset();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bucket_num));
  } else if (OB_FAIL(part_group_buckets_.reserve(bucket_num))) {
    LOG_WARN("fail to reserve part group buckets", KR(ret), K(bucket_num));
  } else {
    part_group_cnt_ = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_num; i++) {
      ObPartGroupBucket bucket(OB_MALLOC_NORMAL_BLOCK_SIZE,
                              ModulePageAllocator(alloc_, "PartGroupBucket"));
      if (OB_FAIL(part_group_buckets_.push_back(bucket))) {
        LOG_WARN("failed to push back bucket", KR(ret), K_(part_group_buckets), K(bucket));
      }
    }
    if (OB_SUCC(ret)) {
      inited_ = true;
    }
  }
  return ret;
}

int ObBalanceGroupUnit::append_part_group(const uint64_t part_group_uid,
                                          ObTransferPartGroup *const part_group)
{
  int ret = OB_SUCCESS;
  int64_t bucket_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(part_group_uid) || nullptr == part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_group_uid), KP(part_group));
  } else if (FALSE_IT(bucket_idx = part_group_uid % part_group_buckets_.count())) {
  } else if (OB_FAIL(append_part_group_into_bucket(bucket_idx, part_group))) {
    LOG_WARN("failed to append part group", KR(ret), K(bucket_idx), KPC(part_group));
  }
  return ret;
}

int ObBalanceGroupUnit::append_part_group_into_bucket(const int64_t bucket_idx,
                                                      ObTransferPartGroup *const part_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(bucket_idx < 0 || bucket_idx >= part_group_buckets_.count()
                        || nullptr == part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket_idx", KR(ret), K(bucket_idx), KP(part_group));
  } else if (OB_FAIL(part_group_buckets_.at(bucket_idx).push_back(part_group))) {
    LOG_WARN("failed to push back part group", KR(ret), K(part_group_buckets_.at(bucket_idx)),
            KPC(part_group));
  } else {
    part_group_cnt_++;
  }
  return ret;
}

int ObBalanceGroupUnit::remove_part_group(const int64_t bucket_idx, const int64_t pg_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(bucket_idx < 0 || bucket_idx >= part_group_buckets_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid bucket_idx", KR(ret), K(bucket_idx));
  } else if (OB_UNLIKELY(pg_idx < 0 || pg_idx >= part_group_buckets_.at(bucket_idx).count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid pg_idx", KR(ret), K(pg_idx));
  } else if (OB_FAIL(part_group_buckets_.at(bucket_idx).remove(pg_idx))) {
    LOG_WARN("failed to remove part group", KR(ret), K(part_group_buckets_.at(bucket_idx)),
            K(pg_idx));
  } else {
    part_group_cnt_--;
  }
  return ret;
}

int ObBalanceGroupUnit::get_transfer_out_part_group(
    const ObBalanceGroupUnit &dst_unit,
    int64_t &bucket_idx,
    int64_t &pg_idx,
    ObTransferPartGroup *&part_group) const
{
  int ret = OB_SUCCESS;
  bucket_idx = OB_INVALID_INDEX;
  part_group = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_unit.is_valid()
                        || dst_unit.part_group_buckets_.count() != part_group_buckets_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dst_unit", KR(ret));
  } else if (OB_UNLIKELY(part_group_cnt_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no part group in this unit", KR(ret), K_(part_group_cnt));
  } else if (OB_FAIL(get_transfer_out_bucket_(dst_unit, bucket_idx))) {
    LOG_WARN("failed to get the bucket to transfer out", KR(ret), K(*this), K(dst_unit));
  } else if (OB_UNLIKELY(bucket_idx < 0 || bucket_idx >= part_group_buckets_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket_idx", KR(ret), K(bucket_idx));
  } else if (OB_UNLIKELY(part_group_buckets_.at(bucket_idx).empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group bucket is empty", KR(ret), K(part_group_buckets_.at(bucket_idx)));
  } else if (FALSE_IT(pg_idx = part_group_buckets_.at(bucket_idx).count() - 1)) {
  } else if (OB_ISNULL(part_group = part_group_buckets_.at(bucket_idx).at(pg_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group, is NULL, unexpected", KR(ret), K(part_group),
            K(part_group_buckets_));
  }
  return ret;
}

int ObBalanceGroupUnit::get_largest_part_group(int64_t &bucket_idx,
                                              int64_t &pg_idx,
                                              ObTransferPartGroup *&part_group) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else {
    int64_t largest_pg_size = 0;
    bucket_idx = OB_INVALID_INDEX;
    pg_idx = OB_INVALID_INDEX;
    ARRAY_FOREACH(part_group_buckets_, i) {
      const ObPartGroupBucket &bucket = part_group_buckets_.at(i);
      ARRAY_FOREACH(bucket, j) {
        ObTransferPartGroup *pg = NULL;
        if (OB_ISNULL(pg = bucket.at(j))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part group is null", KR(ret), K(pg));
        } else if (OB_INVALID_INDEX == bucket_idx || pg->get_data_size() > largest_pg_size) {
          bucket_idx = i;
          pg_idx = j;
          largest_pg_size = pg->get_data_size();
        }
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == bucket_idx || OB_INVALID_INDEX == pg_idx)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the largest part group", KR(ret), K(bucket_idx), K(pg_idx));
    } else {
      part_group = part_group_buckets_.at(bucket_idx).at(pg_idx);
    }
  }
  return ret;
}

int ObBalanceGroupUnit::get_smallest_part_group(int64_t &bucket_idx,
                                                int64_t &pg_idx,
                                                ObTransferPartGroup *&part_group) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(inited));
  } else {
    int64_t smallest_pg_size = 0;
    bucket_idx = OB_INVALID_INDEX;
    pg_idx = OB_INVALID_INDEX;
    ARRAY_FOREACH(part_group_buckets_, i) {
      const ObPartGroupBucket &bucket = part_group_buckets_.at(i);
      ARRAY_FOREACH(bucket, j) {
        const ObTransferPartGroup *pg = bucket.at(j);
        if (OB_ISNULL(pg)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("part group is null", KR(ret), K(pg));
        } else if (OB_INVALID_INDEX == bucket_idx || pg->get_data_size() < smallest_pg_size) {
          bucket_idx = i;
          pg_idx = j;
          smallest_pg_size = pg->get_data_size();
        }
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_UNLIKELY(OB_INVALID_INDEX == bucket_idx || OB_INVALID_INDEX == pg_idx)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the smallest part group", KR(ret), K(bucket_idx), K(pg_idx));
    } else {
      part_group = part_group_buckets_.at(bucket_idx).at(pg_idx);
    }
  }
  return ret;
}

int ObBalanceGroupUnit::get_transfer_out_bucket_(const ObBalanceGroupUnit &dst_unit,
                                                int64_t &bucket_idx) const
{
  int ret = OB_SUCCESS;
  double ratio_max = -DBL_MAX;
  bucket_idx = OB_INVALID_INDEX;
  ARRAY_FOREACH_NORET(part_group_buckets_, i) {
    const ObArray<ObTransferPartGroup*> &src_part_groups = part_group_buckets_.at(i);
    const ObArray<ObTransferPartGroup*> &dst_part_groups = dst_unit.part_group_buckets_.at(i);
    int64_t src_cnt = src_part_groups.count();
    int64_t dst_cnt = dst_part_groups.count();
    if (src_cnt != 0) {
      double ratio = static_cast<double>(dst_cnt) / static_cast<double>(src_cnt);
      // first valid bucket
      if (OB_INVALID_INDEX == bucket_idx) {
        ratio_max = ratio;
        bucket_idx = i;
      // rate_max == ratio: transfer from the bucket with the minimum number of part groups
      } else if (fabs(ratio - ratio_max) < OB_DOUBLE_EPSINON) {
        if (src_cnt < part_group_buckets_.at(bucket_idx).count()) {
          ratio_max = ratio;
          bucket_idx = i;
        }
      } else if (ratio > ratio_max) {
        ratio_max = ratio;
        bucket_idx = i;
      }
    }
  }
  if (OB_UNLIKELY(OB_INVALID_INDEX == bucket_idx)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get the bucket to transfer out", KR(ret), K_(part_group_buckets),
            K_(dst_unit.part_group_buckets));
  }
  return ret;
}

///////////////////////////////////////////////
// ObRRPartGroupContainer

ObRRPartGroupContainer::~ObRRPartGroupContainer()
{
  FOREACH(it, bg_units_) {
    ObBalanceGroupUnit *bg_unit = it->second;
    if (OB_NOT_NULL(bg_unit)) {
      bg_unit->~ObBalanceGroupUnit();
      alloc_.free(bg_unit);
      bg_unit = nullptr;
    }
  }
  bg_units_.destroy();
  part_group_cnt_ = 0;
}

int ObRRPartGroupContainer::init(const ObBalanceGroupID &bg_id, const int64_t ls_num)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_id), K(ls_num));
  } else if (OB_FAIL(bg_units_.create(MAP_BUCKET_NUM, "BGUnits"))) {
    LOG_WARN("fail to create bg_units", KR(ret));
  } else {
    part_group_cnt_ = 0;
    bucket_num_ = bg_id.is_non_part_table_bg() ? 1 : ls_num;
    is_inited_ = true;
  }
  return ret;
}

int ObRRPartGroupContainer::append_new_part_group(
    const share::schema::ObSimpleTableSchemaV2 &table_schema,
    const uint64_t part_group_uid,
    ObTransferPartGroup *const part_group)
{
  int ret = OB_SUCCESS;
  ObBalanceGroupUnit *bg_unit = nullptr;
  ObObjectID bg_unit_id = OB_INVALID_ID != table_schema.get_tablegroup_id()
      ? table_schema.get_tablegroup_id()
      : table_schema.get_database_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!table_schema.is_valid() || !is_valid_id(part_group_uid)
                        || nullptr == part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(table_schema), K(part_group));
  } else if (OB_FAIL(get_or_create_bg_unit_(bg_unit_id, bg_unit))) {
    LOG_WARN("get or create new balance group unit fail", KR(ret), K(bg_unit_id));
  } else if (OB_ISNULL(bg_unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bg_unit is null", KR(ret), K(bg_unit));
  } else if (OB_FAIL(bg_unit->append_part_group(part_group_uid, part_group))) {
    LOG_WARN("failed to push back part group", KR(ret), K(part_group_uid), K(part_group));
  } else {
    part_group_cnt_++;
  }
  return ret;
}

int ObRRPartGroupContainer::append(const ObIPartGroupInfo &pg_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!pg_info.is_valid()
                        || ObPartDistributionMode::ROUND_ROBIN != pg_info.get_part_distribution_mode())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pg_info));
  } else {
    const ObRRPartGroupInfo &pg_info_rr = static_cast<const ObRRPartGroupInfo &>(pg_info);
    const ObObjectID bg_unit_id = pg_info_rr.bg_unit_id_;
    const int64_t bucket_idx = pg_info_rr.bucket_idx_;
    ObTransferPartGroup *part_group = pg_info_rr.part_group();
    ObBalanceGroupUnit *bg_unit = nullptr;
    if (OB_UNLIKELY(!is_valid_id(bg_unit_id)
                    || bucket_idx < 0 || bucket_idx >= bucket_num_
                    || nullptr == part_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", KR(ret), K(pg_info_rr));
    } else if (OB_FAIL(get_or_create_bg_unit_(bg_unit_id, bg_unit))) {
      LOG_WARN("get or create balance group unit fail", KR(ret), K(bg_unit_id));
    } else if (OB_ISNULL(bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bg_unit is null", KR(ret), K(bg_unit));
    } else if (OB_FAIL(bg_unit->append_part_group_into_bucket(bucket_idx, part_group))) {
      LOG_WARN("failed to append part group into bucket", KR(ret), KPC(bg_unit),
              K(bucket_idx), KPC(part_group));
    } else {
      part_group_cnt_++;
    }
  }
  return ret;
}

int ObRRPartGroupContainer::remove(const ObIPartGroupInfo &pg_info) {
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!pg_info.is_valid()
                        || ObPartDistributionMode::ROUND_ROBIN != pg_info.get_part_distribution_mode())) {
    LOG_WARN("invalid argument", KR(ret), K(pg_info));
  } else {
    const ObRRPartGroupInfo &pg_info_rr = static_cast<const ObRRPartGroupInfo &>(pg_info);
    const ObObjectID bg_unit_id = pg_info_rr.bg_unit_id_;
    const int64_t bucket_idx = pg_info_rr.bucket_idx_;
    const int64_t pg_idx = pg_info_rr.pg_idx_;
    ObTransferPartGroup *part_group = pg_info_rr.part_group();
    ObBalanceGroupUnit *bg_unit = nullptr;
    if (OB_UNLIKELY(!is_valid_id(bg_unit_id)
                    || bucket_idx < 0 || bucket_idx >= bucket_num_
                    || nullptr == part_group)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part group info", KR(ret), K(pg_info_rr));
    } else if (OB_FAIL(bg_units_.get_refactored(bg_unit_id, bg_unit))) {
      LOG_WARN("failed to get balance group unit", KR(ret), K(bg_unit_id));
    } else if (OB_ISNULL(bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bg_unit is null", KR(ret), K(bg_unit_id));
    } else if (OB_FAIL(bg_unit->remove_part_group(bucket_idx, pg_idx))) {
      LOG_WARN("failed to remove part group", KR(ret), KPC(bg_unit), K(bucket_idx), K(pg_idx));
    } else {
      part_group_cnt_--;
    }
  }
  return ret;
}

int ObRRPartGroupContainer::select(
    ObIPartGroupContainer &dst_pg,
    ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  ObRRPartGroupInfo *pg_info_rr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_pg.is_valid()
                        || ObPartDistributionMode::ROUND_ROBIN != dst_pg.get_part_distribution_mode())) {
  } else if (OB_UNLIKELY(0 == count())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("part groups are empty", KR(ret));
  } else if (OB_FAIL(create_part_group_info_if_needed_(pg_info, pg_info_rr))) {
    LOG_WARN("create part group info fail", KR(ret), KP(pg_info));
  } else if (OB_ISNULL(pg_info_rr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group info", KR(ret), KP(pg_info_rr));
  } else {
    ObRRPartGroupContainer &dst_pg_rr = static_cast<ObRRPartGroupContainer &>(dst_pg);
    ObObjectID bg_unit_id = OB_INVALID_ID;
    ObBalanceGroupUnit *src_unit = nullptr;
    ObBalanceGroupUnit *dst_unit = nullptr;
    int64_t bucket_idx = OB_INVALID_INDEX;
    int64_t pg_idx = OB_INVALID_INDEX;
    ObTransferPartGroup *part_group = nullptr;
    if (OB_FAIL(get_transfer_out_unit_(dst_pg_rr, bg_unit_id))) {
    LOG_WARN("failed to get the unit to transfer out", KR(ret), KPC(this), K(dst_pg_rr));
    } else if (OB_FAIL(bg_units_.get_refactored(bg_unit_id, src_unit))) {
      // src_unit must exist
      LOG_WARN("get src_unit fail", KR(ret), K(bg_unit_id));
    } else if (OB_FAIL(dst_pg_rr.get_or_create_bg_unit_(bg_unit_id, dst_unit))) {
      // dst_unit may not exist
      LOG_WARN("create balance group unit fail", KR(ret), K(dst_pg_rr), K(bg_unit_id));
    } else if (OB_ISNULL(src_unit) || OB_ISNULL(dst_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src_unit or dst_unit is null", KR(ret), K(src_unit), K(dst_unit));
    } else if (OB_FAIL(src_unit->get_transfer_out_part_group(*dst_unit, bucket_idx, pg_idx,
                                                            part_group))) {
      LOG_WARN("failed to transfer out part group in the unit", KR(ret), K(bg_unit_id),
              K(*this), KPC(dst_unit));
    } else if (OB_FAIL(pg_info_rr->init_(bg_unit_id, bucket_idx, pg_idx, part_group))) {
      LOG_WARN("failed to init part group info", KR(ret), K(bg_unit_id), K(bucket_idx),
              K(pg_idx), KPC(part_group));
    }
  }
  return ret;
}

int ObRRPartGroupContainer::get_largest(ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  ObRRPartGroupInfo *pg_info_rr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(create_part_group_info_if_needed_(pg_info, pg_info_rr))) {
    LOG_WARN("create part group info fail", KR(ret), KP(pg_info));
  } else if (OB_ISNULL(pg_info_rr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group info", KR(ret), KP(pg_info_rr));
  } else {
    int64_t largest_pg_size = 0;
    ObObjectID largest_unit = OB_INVALID_ID;
    int64_t largest_bucket = OB_INVALID_INDEX;
    int64_t largest_pg_idx = OB_INVALID_INDEX;
    ObTransferPartGroup *largest_part_group = nullptr;
    FOREACH_X(it, bg_units_, OB_SUCC(ret)) {
      const ObObjectID &bg_unit_id = it->first;
      const ObBalanceGroupUnit *bg_unit = it->second;
      ObTransferPartGroup *part_group_cur = NULL;
      int64_t bucket_idx = OB_INVALID_INDEX;
      int64_t pg_idx = OB_INVALID_INDEX;
      if (OB_ISNULL(bg_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("balance group unit is null", KR(ret), K(bg_unit));
      } else if (OB_FAIL(bg_unit->get_largest_part_group(bucket_idx, pg_idx, part_group_cur))) {
        if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get the largest part group", KR(ret), K(bg_unit));
        }
      } else if (OB_ISNULL(part_group_cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group is null", KR(ret), K(part_group_cur));
      } else if (OB_INVALID_ID == largest_unit
                || part_group_cur->get_data_size() > largest_pg_size) {
        largest_unit = bg_unit_id;
        largest_bucket = bucket_idx;
        largest_pg_idx = pg_idx;
        largest_pg_size = part_group_cur->get_data_size();
        largest_part_group = part_group_cur;
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_UNLIKELY(OB_INVALID_ID == largest_unit)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the largest part group", KR(ret));
    } else if (OB_FAIL(pg_info_rr->init_(largest_unit, largest_bucket, largest_pg_idx,
                                        largest_part_group))) {
      LOG_WARN("failed to init part group info", KR(ret), K(largest_unit), K(largest_bucket),
              K(largest_pg_idx), KPC(largest_part_group));
    }
  }
  return ret;
}

int ObRRPartGroupContainer::get_smallest(ObIPartGroupInfo *&pg_info) const
{
  int ret = OB_SUCCESS;
  ObRRPartGroupInfo *pg_info_rr = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(is_inited));
  } else if (OB_FAIL(create_part_group_info_if_needed_(pg_info, pg_info_rr))) {
    LOG_WARN("create part group info fail", KR(ret), KP(pg_info));
  } else if (OB_ISNULL(pg_info_rr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid part group info", KR(ret), KP(pg_info_rr));
  } else {
    int64_t smallest_pg_size = 0;
    ObObjectID smallest_unit = OB_INVALID_ID;
    int64_t smallest_bucket = OB_INVALID_INDEX;
    int64_t smallest_pg_idx = OB_INVALID_INDEX;
    ObTransferPartGroup *smallest_part_group = nullptr;
    FOREACH_X(it, bg_units_, OB_SUCC(ret)) {
      const ObObjectID &bg_unit_id = it->first;
      const ObBalanceGroupUnit *bg_unit = it->second;
      ObTransferPartGroup *part_group_cur = NULL;
      int64_t bucket_idx = OB_INVALID_INDEX;
      int64_t pg_idx = OB_INVALID_INDEX;
      if (OB_ISNULL(bg_unit)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("balance group unit is null", KR(ret), K(bg_unit));
      } else if (OB_FAIL(bg_unit->get_smallest_part_group(bucket_idx, pg_idx, part_group_cur))) {
        if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get the smallest part group", KR(ret), K(bg_unit));
        }
      } else if (OB_ISNULL(part_group_cur)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("part group is null", KR(ret), K(part_group_cur));
      } else if (OB_INVALID_ID == smallest_unit
                || part_group_cur->get_data_size() < smallest_pg_size) {
        smallest_unit = bg_unit_id;
        smallest_bucket = bucket_idx;
        smallest_pg_idx = pg_idx;
        smallest_pg_size = part_group_cur->get_data_size();
        smallest_part_group = part_group_cur;
      }
    }
    if (OB_FAIL(ret)) { // empty
    } else if (OB_UNLIKELY(OB_INVALID_ID == smallest_unit)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("failed to get the smallest part group", KR(ret));
    } else if (OB_FAIL(pg_info_rr->init_(smallest_unit, smallest_bucket, smallest_pg_idx,
                                        smallest_part_group))) {
      LOG_WARN("failed to init part group info", KR(ret), K(smallest_unit), K(smallest_bucket),
              K(smallest_pg_idx), KPC(smallest_part_group));
    }
  }
  return ret;
}

// example1:
//        unit_db0  unit_db1  unit_db2
// ls0:       10      10         10
// ls1:       10      10         0
// it will only transfer from unit_db2 =>
//        unit_db0  unit_db1  unit_db2
// ls0:       10      10         5
// ls1:       10      10         5
//
// example2:
//        unit_db0  unit_db1  unit_db2
// ls0:       2       4          6
// ls1:       0       0          0
// it will transfer out 6 part group from unit_db2, unit_db1, unit_db0, unit_db2,
// unit_db1, unit_db2, respectively
//        unit_db0  unit_db1  unit_db2
// ls0:       1       2          3
// ls1:       1       2          3
int ObRRPartGroupContainer::get_transfer_out_unit_(
    const ObRRPartGroupContainer &dst_pgs,
    ObObjectID &bg_unit_id) const
{
  int ret = OB_SUCCESS;
  double ratio_min = DBL_MAX;
  int64_t src_cnt_max = 0;
  bg_unit_id = OB_INVALID_ID;
  FOREACH_X(it, bg_units_, OB_SUCC(ret)) {
    const ObObjectID unit_id_cur = it->first;
    const ObBalanceGroupUnit *src_bg_unit = it->second;
    ObBalanceGroupUnit *dst_bg_unit = NULL;
    int64_t src_cnt = 0;
    int64_t dst_cnt = 0;
    double ratio = 0;
    if (OB_ISNULL(src_bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src_bg_unit is null", KR(ret), K(src_bg_unit));
    } else if (FALSE_IT(src_cnt = src_bg_unit->get_part_group_count())) {
    } else if (OB_FAIL(dst_pgs.bg_units_.get_refactored(unit_id_cur, dst_bg_unit))) {
      if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
        dst_cnt = 0;
      } else {
        LOG_WARN("get balance group unit fail", KR(ret), K(unit_id_cur));
      }
    } else if (OB_ISNULL(dst_bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dst_bg_unit is null", KR(ret), K(dst_bg_unit));
    } else {
      dst_cnt = dst_bg_unit->get_part_group_count();
    }
    if (OB_FAIL(ret)) {
    } else if (0 != src_cnt) {
      ratio = static_cast<double>(dst_cnt) / static_cast<double>(src_cnt);
      // first valid unit
      if (OB_INVALID_ID == bg_unit_id) {
        ratio_min = ratio;
        bg_unit_id = unit_id_cur;
        src_cnt_max = src_cnt;
      // ratio == ratio min: transfer from the unit with the maximum number of part groups
      } else if (fabs(ratio - ratio_min) < OB_DOUBLE_EPSINON) {
        if (src_cnt > src_cnt_max) {
          ratio_min = ratio;
          bg_unit_id = unit_id_cur;
          src_cnt_max = src_cnt;
        }
      } else if (ratio < ratio_min) {
        ratio_min = ratio;
        bg_unit_id = unit_id_cur;
        src_cnt_max = src_cnt;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(OB_INVALID_ID == bg_unit_id)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get the unit to transfer out", KR(ret), K(dst_pgs));
  }
  return ret;
}

int ObRRPartGroupContainer::get_or_create_bg_unit_(
    const ObObjectID &bg_unit_id,
    ObBalanceGroupUnit *&bg_unit)
{
  int ret = OB_SUCCESS;
  void *buf = NULL;
  bg_unit = NULL;
  if (OB_FAIL(bg_units_.get_refactored(bg_unit_id, bg_unit))) {
    if (OB_LIKELY(OB_HASH_NOT_EXIST == ret)) {
      if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObBalanceGroupUnit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for ObBalanceGroupUnit fail", KR(ret), K(buf));
      } else if (OB_ISNULL(bg_unit = new(buf) ObBalanceGroupUnit(alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("construct ObBalanceGroupUnit fail", KR(ret));
      } else if (OB_FAIL(bg_unit->init(bucket_num_))) {
        LOG_WARN("init ObBalanceGroupUnit fail", KR(ret), K_(bucket_num), KPC(bg_unit));
      } else if (OB_FAIL(bg_units_.set_refactored(bg_unit_id, bg_unit))) {
        LOG_WARN("set bg unit fail", KR(ret), K(bg_unit_id), KPC(bg_unit));
      }
    } else {
      LOG_WARN("failed to get balance group unit", K(bg_unit_id));
    }
  }
  return ret;
}

int ObRRPartGroupContainer::create_part_group_info_if_needed_(
    ObIPartGroupInfo *&pg_info,
    ObRRPartGroupInfo *&pg_info_rr) const
{
  int ret = OB_SUCCESS;
  pg_info_rr = nullptr;
  if (OB_ISNULL(pg_info)) {
    if (OB_ISNULL(pg_info_rr = reinterpret_cast<ObRRPartGroupInfo *>(
        alloc_.alloc(sizeof(ObRRPartGroupInfo))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for part group info", KR(ret));
    } else if (OB_ISNULL(pg_info_rr = new(pg_info_rr) ObRRPartGroupInfo())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for part group info", KR(ret));
    } else {
      pg_info = pg_info_rr;
    }
  } else if (ObPartDistributionMode::ROUND_ROBIN != pg_info->get_part_distribution_mode()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KPC(pg_info));
  } else {
    pg_info->reset();
    pg_info_rr = static_cast<ObRRPartGroupInfo *>(pg_info);
  }
  return ret;
}

}
}

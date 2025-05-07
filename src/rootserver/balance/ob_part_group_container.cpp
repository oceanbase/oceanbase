/**
 * Copyright (c) 2025 OceanBase
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
#include "rootserver/balance/ob_balance_group_info.h" // ObBalanceGroupInfo

namespace oceanbase
{
using namespace share;
namespace rootserver
{
///////////////////////////////////////////////
// ObBalanceGroupUnit

ObBalanceGroupUnit::~ObBalanceGroupUnit()
{
  inited_ = false;
  bg_unit_id_ = OB_INVALID_ID;
  ARRAY_FOREACH_NORET(pg_buckets_, i) {
    ObPartGroupBucket &bucket = pg_buckets_.at(i);
    ARRAY_FOREACH_NORET(bucket, j) {
      ObPartGroupInfo *part_group = bucket.at(j);
      if (OB_NOT_NULL(part_group)) {
        part_group->~ObPartGroupInfo();
        alloc_.free(part_group);
        part_group = nullptr;
      }
    }
    bucket.destroy();
  }
  pg_buckets_.destroy();
}

int ObBalanceGroupUnit::init(const uint64_t bg_unit_id, const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  pg_buckets_.reset();
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!is_valid_id(bg_unit_id) || bucket_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_unit_id), K(bucket_num));
  } else if (FALSE_IT(pg_buckets_.set_block_allocator(ModulePageAllocator(alloc_, "PGBuckets")))) {
  // memory optimization, otherwise the default block size is 8k
  } else if (FALSE_IT(pg_buckets_.set_block_size(sizeof(ObPartGroupBucket) * bucket_num))) {
  } else if (OB_FAIL(pg_buckets_.reserve(bucket_num))) {
    LOG_WARN("fail to reserve part group buckets", KR(ret), K(bucket_num));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < bucket_num; i++) {
      ObPartGroupBucket bucket(1024/*block_size*/, ModulePageAllocator(alloc_, "PGBucket")); // memory optimization
      if (OB_FAIL(pg_buckets_.push_back(bucket))) {
        LOG_WARN("failed to push back bucket", KR(ret), K(pg_buckets_), K(bucket));
      }
    }
    if (OB_SUCC(ret)) {
      bg_unit_id_ = bg_unit_id;
      inited_ = true;
    }
  }
  return ret;
}

int ObBalanceGroupUnit::append_part_group(
    ObPartGroupInfo *const part_group)
{
  int ret = OB_SUCCESS;
  int64_t bucket_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!inited_ || pg_buckets_.empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), KPC(this));
  } else if (OB_ISNULL(part_group) || OB_UNLIKELY(!part_group->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(part_group));
  } else if (FALSE_IT(bucket_idx = part_group->get_part_group_uid() % pg_buckets_.count())) {
  } else if (OB_FAIL(pg_buckets_.at(bucket_idx).push_back(part_group))) {
    LOG_WARN("failed to push back part group", KR(ret), K(pg_buckets_), KPC(part_group));
  }
  return ret;
}

int ObBalanceGroupUnit::remove_part_group(ObPartGroupInfo *const part_group)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_ || pg_buckets_.empty())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(inited_), K(pg_buckets_));
  } else if (OB_ISNULL(part_group)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null part_group", KR(ret), KP(part_group));
  } else {
    int64_t bucket_idx = part_group->get_part_group_uid() % pg_buckets_.count();
    // It's more efficient to iterate from back to front because the last one is often chosen
    ObPartGroupBucket &bucket = pg_buckets_.at(bucket_idx);
    bool found = false;
    for (int64_t i = bucket.count() - 1; i >= 0 && OB_SUCC(ret) && !found; --i) {
      if (bucket.at(i) == part_group) {
        found = true;
        if (OB_FAIL(bucket.remove(i))) {
          LOG_WARN("remove failed", KR(ret), K(i), KPC(part_group), K(bucket));
        }
      }
    }
    if (OB_SUCC(ret) && !found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("part_group not found", KR(ret), KPC(part_group), K(bucket));
    }
  }
  return ret;
}

int ObBalanceGroupUnit::get_transfer_out_part_group(
    const ObBalanceGroupUnit &dst_unit,
    ObPartGroupInfo *&part_group) const
{
  int ret = OB_SUCCESS;
  int64_t bucket_idx = OB_INVALID_INDEX;
  int64_t pg_idx = OB_INVALID_INDEX;
  part_group = nullptr;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_unit.is_valid()
      || dst_unit.pg_buckets_.count() != pg_buckets_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dst_unit", KR(ret), K(dst_unit), K(pg_buckets_));
  } else if (OB_UNLIKELY(get_pg_count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no part group in this unit", KR(ret), "pg count", get_pg_count());
  } else if (OB_FAIL(get_transfer_out_bucket_(dst_unit, bucket_idx))) {
    LOG_WARN("failed to get the bucket to transfer out", KR(ret), K(*this), K(dst_unit));
  } else if (OB_UNLIKELY(bucket_idx < 0 || bucket_idx >= pg_buckets_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid bucket_idx", KR(ret), K(bucket_idx));
  } else if (OB_UNLIKELY(pg_buckets_.at(bucket_idx).empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group bucket is empty", KR(ret), K(pg_buckets_.at(bucket_idx)));
  } else if (FALSE_IT(pg_idx = pg_buckets_.at(bucket_idx).count() - 1)) {
  } else if (OB_ISNULL(part_group = pg_buckets_.at(bucket_idx).at(pg_idx))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("part group is null", KR(ret), K(part_group), K(pg_buckets_));
  }
  return ret;
}

// Fill one bucket first. Select the bucket with the highest dest/src ratio.
// example: {object_id, object_id}
//               src unit                            dest unit
//     bucket1   bucket2   bucket3          bucket1   bucket2   bucket3
//     {1,4,7}   {2,5,8}    {3,6}            {}         {}        {}
// -->
//     {1,4}     {2,5,8}    {3,6}            {7}        {}        {}
// -->
//     {1}       {2,5,8}    {3,6}            {4,7}      {}        {}
// -->
//     {}        {2,5,8}    {3,6}            {1,4,7}    {}        {}
//
int ObBalanceGroupUnit::get_transfer_out_bucket_(
    const ObBalanceGroupUnit &dst_unit,
    int64_t &bucket_idx) const
{
  int ret = OB_SUCCESS;
  double ratio_max = 0;
  bucket_idx = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!dst_unit.is_valid()
      || dst_unit.pg_buckets_.count() != pg_buckets_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dst_unit", KR(ret), K(dst_unit), K(pg_buckets_));
  } else {
    ARRAY_FOREACH_NORET(pg_buckets_, i) {
      const ObPartGroupBucket &src_part_groups = pg_buckets_.at(i);
      const ObPartGroupBucket &dst_part_groups = dst_unit.pg_buckets_.at(i);
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
          if (src_cnt < pg_buckets_.at(bucket_idx).count()) {
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
      LOG_WARN("failed to get the bucket to transfer out", KR(ret), K(pg_buckets_), K(dst_unit.pg_buckets_));
    }
  }
  return ret;
}

int64_t ObBalanceGroupUnit::get_pg_count() const
{
  int64_t count = 0;
  ARRAY_FOREACH_NORET(pg_buckets_, i) {
    count += pg_buckets_.at(i).count();
  }
  return count;
}

int64_t ObBalanceGroupUnit::get_data_size() const
{
  int64_t data_size = 0;
  ARRAY_FOREACH_NORET(pg_buckets_, i) {
    ARRAY_FOREACH_NORET(pg_buckets_.at(i), j) {
      if (OB_NOT_NULL(pg_buckets_.at(i).at(j))) {
        data_size += pg_buckets_.at(i).at(j)->get_data_size();
      }
    }
  }
  return data_size;
}

int64_t ObBalanceGroupUnit::get_balance_weight() const
{
  int64_t weight = 0;
  ARRAY_FOREACH_NORET(pg_buckets_, i) {
    ARRAY_FOREACH_NORET(pg_buckets_.at(i), j) {
      if (OB_NOT_NULL(pg_buckets_.at(i).at(j))) {
        weight += pg_buckets_.at(i).at(j)->get_weight();
      }
    }
  }
  return weight;
}

///////////////////////////////////////////////
// ObPartGroupContainer

int ObPartGroupContainer::Iterator::next(ObPartGroupInfo *&pg)
{
  int ret = OB_SUCCESS;
  bool find = false;
  pg = nullptr;
  while (OB_SUCC(ret) && !is_end_ && !find) {
    if (bg_unit_iter_ == container_.bg_units_.end()) {
      is_end_ = true;
      ret = OB_ITER_END;
      break;
    }
    ObBalanceGroupUnit *bg_unit = *bg_unit_iter_;
    if (OB_ISNULL(bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null bg_unit", KR(ret), KP(bg_unit), K(*this));
    } else {
      ObArray<ObPartGroupBucket> &buckets = bg_unit->get_pg_buckets();
      while (OB_SUCC(ret) && bucket_idx_ < buckets.count() && !find) {
        ObPartGroupBucket &bucket = buckets.at(bucket_idx_);
        if (pg_idx_ < bucket.count()) {
          ObPartGroupInfo *cur_part_group = bucket.at(pg_idx_);
          if (OB_ISNULL(cur_part_group)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null part group", KR(ret));
          } else { // find
            pg = cur_part_group;
            ++pg_idx_;
            find = true;
          }
        } else { // next bucket
          ++bucket_idx_;
          pg_idx_ = 0;
        }
      }
      // next unit
      if (bucket_idx_ >= buckets.count()) {
        ++bg_unit_iter_;
        bucket_idx_ = 0;
        pg_idx_ = 0;
      }
    }
  }
  return ret;
}

void ObPartGroupContainer::Iterator::reset()
{
  bg_unit_iter_ = container_.bg_units_.begin();
  bucket_idx_ = 0;
  pg_idx_ = 0;
  is_end_ = false;
}

ObPartGroupContainer::~ObPartGroupContainer()
{
  is_inited_ = false;
  bucket_num_ = 0;
  FOREACH(it, bg_units_) {
    ObBalanceGroupUnit *bg_unit = *it;
    if (OB_NOT_NULL(bg_unit)) {
      bg_unit->~ObBalanceGroupUnit();
      alloc_.free(bg_unit);
      bg_unit = nullptr;
    }
  }
  bg_units_.destroy();
}

int ObPartGroupContainer::init(const ObBalanceGroupID &bg_id, const int64_t ls_num)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_UNLIKELY(!bg_id.is_valid() || ls_num <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(bg_id), K(ls_num));
  } else {
    bg_units_.reset();
    // Non-partitioned tables are unordered and only one bucket is required for each balance group unit.
    // Partitioned tables need to be divided into buckets based on part_group_uid.
    bucket_num_ = bg_id.is_non_part_table_bg() ? 1 : ls_num;
    is_inited_ = true;
  }
  return ret;
}

int ObPartGroupContainer::append_part_group(
    ObPartGroupInfo *const part_group)
{
  int ret = OB_SUCCESS;
  ObBalanceGroupUnit *bg_unit = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(part_group) || OB_UNLIKELY(!part_group->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(part_group));
  } else if (OB_FAIL(get_or_create_bg_unit_(part_group->get_bg_unit_id(), bg_unit))) {
    LOG_WARN("get or create new balance group unit fail", KR(ret), KPC(part_group));
  } else if (OB_ISNULL(bg_unit)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("bg_unit is null", KR(ret), K(bg_unit));
  } else if (OB_FAIL(bg_unit->append_part_group(part_group))) {
    LOG_WARN("failed to push back part group", KR(ret), KPC(part_group));
  }
  return ret;
}

int ObPartGroupContainer::remove_part_group(ObPartGroupInfo *const part_group)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_ISNULL(part_group) || OB_UNLIKELY(!part_group->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(part_group));
  } else {
    const uint64_t bg_unit_id = part_group->get_bg_unit_id();
    ObBalanceGroupUnit *bg_unit = nullptr;
    if (OB_UNLIKELY(!is_valid_id(bg_unit_id))) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid part group info", KR(ret), K(part_group));
    } else if (OB_FAIL(get_bg_unit_(bg_unit_id, bg_unit))) {
      LOG_WARN("failed to get balance group unit", KR(ret), K(bg_unit_id));
    } else if (OB_ISNULL(bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("bg_unit is null", KR(ret), K(bg_unit_id));
    } else if (OB_FAIL(bg_unit->remove_part_group(part_group))) {
      LOG_WARN("failed to remove part group", KR(ret), K(part_group), KPC(bg_unit));
    }
  }
  return ret;
}

int ObPartGroupContainer::select(
    const int64_t balance_weight,
    ObPartGroupContainer &dst_container,
    ObPartGroupInfo *&part_group) const
{
  int ret = OB_SUCCESS;
  part_group = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret));
  } else if (OB_UNLIKELY(!dst_container.is_inited() || balance_weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(dst_container), K(balance_weight));
  } else if (OB_UNLIKELY(0 == get_part_group_count())) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("part groups are empty", KR(ret));
  } else if (balance_weight > 0) {
    PGFunctor::IsSameWeightPG is_same_weight_pg(balance_weight);
    if (OB_FAIL(find_(part_group, is_same_weight_pg))) {
      LOG_WARN("find failed", KR(ret), KPC(this));
    }
  } else { // balance_weight is not specified
    ObBalanceGroupUnit *src_unit = nullptr;
    ObBalanceGroupUnit *dst_unit = nullptr;
    if (OB_FAIL(get_transfer_out_unit_(dst_container, src_unit))) {
      // src_unit must exist
      LOG_WARN("failed to get the unit to transfer out", KR(ret), KPC(this), K(dst_container));
    } else if (OB_ISNULL(src_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null ptr", KR(ret), KP(src_unit), KPC(this));
    } else if (OB_FAIL(dst_container.get_or_create_bg_unit_(src_unit->get_bg_unit_id(), dst_unit))) {
      // dst_unit may not exist
      LOG_WARN("create balance group unit fail", KR(ret), K(dst_container), KPC(src_unit));
    } else if (OB_ISNULL(src_unit) || OB_ISNULL(dst_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src_unit or dst_unit is null", KR(ret), K(src_unit), K(dst_unit));
    } else if (OB_FAIL(src_unit->get_transfer_out_part_group(*dst_unit, part_group))) {
      LOG_WARN("failed to transfer out part group in the unit", KR(ret),
          KPC(src_unit), K(*this), KPC(dst_unit));
    }
  }
  return ret;
}

int ObPartGroupContainer::get_largest_part_group(ObPartGroupInfo *&pg) const
{
  int ret = OB_SUCCESS;
  pg = nullptr;
  int64_t max_size = OB_INVALID_SIZE;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    PGFunctor::GetMaxSizePG get_max_size(max_size, pg);
    if (OB_FAIL(for_each_(get_max_size))) {
      LOG_WARN("traverse failed", KR(ret));
    } else if (OB_ISNULL(pg)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("get largest_pg failed", KR(ret), K(max_size), K(*this));
    }
  }
  return ret;
}

int ObPartGroupContainer::get_smallest_part_group(ObPartGroupInfo *&pg) const
{
  int ret = OB_SUCCESS;
  pg = nullptr;
  int64_t min_size = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    PGFunctor::GetMinSizePG get_min_size(min_size, pg);
    if (OB_FAIL(for_each_(get_min_size))) {
      LOG_WARN("traverse failed", KR(ret));
    } else if (OB_ISNULL(pg)) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("get smallest_pg failed", KR(ret), K(min_size), K(*this));
    }
  }
  return ret;
}

int64_t ObPartGroupContainer::get_part_group_count() const
{
  int64_t cnt = 0;
  FOREACH(it, bg_units_) {
    if (OB_NOT_NULL(*it)) {
      cnt += (*it)->get_pg_count();
    }
  }
  return cnt;
}
int64_t ObPartGroupContainer::get_data_size() const
{
  int64_t data_size = 0;
  FOREACH(it, bg_units_) {
    if (OB_NOT_NULL(*it)) {
      data_size += (*it)->get_data_size();
    }
  }
  return data_size;
}
int64_t ObPartGroupContainer::get_balance_weight() const
{
  int64_t weight = 0;
  FOREACH(it, bg_units_) {
    if (OB_NOT_NULL(*it)) {
      weight += (*it)->get_balance_weight();
    }
  }
  return weight;
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
int ObPartGroupContainer::get_transfer_out_unit_(
    const ObPartGroupContainer &dst_pgs,
    ObBalanceGroupUnit *&bg_unit) const
{
  int ret = OB_SUCCESS;
  bg_unit = nullptr;
  double ratio_min = DBL_MAX;
  int64_t src_cnt_max = 0;
  FOREACH_X(it, bg_units_, OB_SUCC(ret)) {
    ObBalanceGroupUnit *src_bg_unit = *it;
    ObBalanceGroupUnit *dst_bg_unit = nullptr;
    int64_t src_cnt = 0;
    int64_t dst_cnt = 0;
    double ratio = 0;
    if (OB_ISNULL(src_bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("src_bg_unit is null", KR(ret), K(src_bg_unit));
    } else if (FALSE_IT(src_cnt = src_bg_unit->get_pg_count())) {
    } else if (OB_FAIL(dst_pgs.get_bg_unit_(src_bg_unit->get_bg_unit_id(), dst_bg_unit))) {
      if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
        ret = OB_SUCCESS;
        dst_cnt = 0;
      } else {
        LOG_WARN("get balance group unit fail", KR(ret), KPC(src_bg_unit));
      }
    } else if (OB_ISNULL(dst_bg_unit)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dst_bg_unit is null", KR(ret), K(dst_bg_unit));
    } else {
      dst_cnt = dst_bg_unit->get_pg_count();
    }
    if (OB_FAIL(ret)) {
    } else if (0 != src_cnt) {
      ratio = static_cast<double>(dst_cnt) / static_cast<double>(src_cnt);
      // first valid unit
      if (nullptr == bg_unit) {
        ratio_min = ratio;
        src_cnt_max = src_cnt;
        bg_unit = src_bg_unit;
      // ratio == ratio min: transfer from the unit with the maximum number of part groups
      } else if (fabs(ratio - ratio_min) < OB_DOUBLE_EPSINON) {
        if (src_cnt > src_cnt_max) {
          ratio_min = ratio;
          src_cnt_max = src_cnt;
          bg_unit = src_bg_unit;
        }
      } else if (ratio < ratio_min) {
        ratio_min = ratio;
        src_cnt_max = src_cnt;
        bg_unit = src_bg_unit;
      }
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(bg_unit)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("failed to get the unit to transfer out", KR(ret), K(dst_pgs), KPC(this));
  }
  return ret;
}

int ObPartGroupContainer::get_bg_unit_(
  const uint64_t bg_unit_id,
  ObBalanceGroupUnit *&bg_unit) const
{
  int ret = OB_SUCCESS;
  bg_unit = nullptr;
  if (OB_UNLIKELY(!is_valid_id(bg_unit_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", KR(ret), K(bg_unit_id));
  } else {
    FOREACH(iter, bg_units_) {
      if (OB_NOT_NULL(*iter) && (*iter)->get_bg_unit_id() == bg_unit_id) {
        bg_unit = *iter;
        break;
      }
    }
    if (OB_ISNULL(bg_unit)) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

int ObPartGroupContainer::get_or_create_bg_unit_(
    const uint64_t bg_unit_id,
    ObBalanceGroupUnit *&bg_unit)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  bg_unit = nullptr;
  if (OB_FAIL(get_bg_unit_(bg_unit_id, bg_unit))) {
    if (OB_LIKELY(OB_ENTRY_NOT_EXIST == ret)) {
      if (OB_ISNULL(buf = alloc_.alloc(sizeof(ObBalanceGroupUnit)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for ObBalanceGroupUnit fail", KR(ret), K(buf));
      } else if (OB_ISNULL(bg_unit = new(buf) ObBalanceGroupUnit(alloc_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("construct ObBalanceGroupUnit fail", KR(ret));
      } else if (OB_FAIL(bg_unit->init(bg_unit_id, bucket_num_))) {
        LOG_WARN("init ObBalanceGroupUnit fail", KR(ret), K_(bucket_num), KPC(bg_unit));
      } else if (OB_FAIL(bg_units_.push_back(bg_unit))) {
        LOG_WARN("set bg unit fail", KR(ret), K(bg_unit_id), KPC(bg_unit));
      }
    } else {
      LOG_WARN("failed to get balance group unit", K(bg_unit_id));
    }
  }
  return ret;
}

int ObPartGroupContainer::split_out_weighted_part_groups(
    ObPartGroupContainer &pg_container)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!pg_container.is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("pg container is not inited", KR(ret), K(pg_container));
  } else {
    while (OB_SUCC(ret)) {
      PGFunctor::IsWeightedPG is_weighted_pg;
      ObPartGroupInfo *pg = nullptr;
      if (OB_FAIL(find_(pg, is_weighted_pg))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          break; // no more weighted pg
        } else {
          LOG_WARN("traverse failed", KR(ret));
        }
      } else if (OB_FAIL(pg_container.append_part_group(pg))) {
        LOG_WARN("push back failed", KR(ret));
      } else if (OB_FAIL(remove_part_group(pg))) {
        LOG_WARN("remove failed", KR(ret));
      } else {
        LOG_TRACE("split out weighted pg", KPC(pg));
      }
    }
  }
  return ret;
}

int ObPartGroupContainer::get_balance_weight_array(
    ObIArray<int64_t> &weight_arr) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K(is_inited_));
  } else {
    PGFunctor::GetWeightArray get_weight_arr(weight_arr);
    if (OB_FAIL(for_each_(get_weight_arr))) {
      LOG_WARN("traverse failed", KR(ret), KPC(this));
    }
  }
  return ret;
}

}
}

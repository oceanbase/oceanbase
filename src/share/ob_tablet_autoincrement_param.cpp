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

#define USING_LOG_PREFIX SHARE
#include "share/ob_tablet_autoincrement_param.h"

namespace oceanbase
{
namespace share
{

OB_SERIALIZE_MEMBER(ObTabletAutoincInterval,
                    tablet_id_,
                    start_,
                    end_)

void ObTabletCacheInterval::set(uint64_t start, uint64_t end)
{
  next_value_ = start;
  start_ = start;
  end_ = end;
}

int ObTabletCacheInterval::next_value(uint64_t &next_value)
{
  int ret = OB_SUCCESS;
  next_value = ATOMIC_FAA(&next_value_, 1);
  if (next_value > end_) {
    ret = OB_EAGAIN;
  }
  return ret;
}

int ObTabletCacheInterval::fetch(uint64_t count, ObTabletCacheInterval &dest)
{
  int ret = OB_SUCCESS;
  uint64_t start = ATOMIC_LOAD(&next_value_);
  uint64_t end = 0;
  uint64_t old_start = start;
  while ((end = start + count - 1) <= end_ &&
         old_start != (start = ATOMIC_CAS(&next_value_, old_start, end + 1))) {
    old_start = start;
    PAUSE();
  }
  if (end > end_) {
    ret = OB_EAGAIN;
  } else {
    dest.set(start, end);
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletAutoincParam,
                    tenant_id_,
                    auto_increment_cache_size_);

OB_SERIALIZE_MEMBER(ObMigrateTabletAutoincSeqParam, src_tablet_id_, dest_tablet_id_, ret_code_, autoinc_seq_);

ObTabletAutoincSeq::ObTabletAutoincSeq() : intervals_()
{
  intervals_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "TabletAutoInc"));
}

int ObTabletAutoincSeq::assign(const ObTabletAutoincSeq &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(intervals_.assign(other.intervals_))) {
    LOG_WARN("failed to assign intervals", K(ret));
  }
  return ret;
}

int ObTabletAutoincSeq::deep_copy(const ObIMultiSourceDataUnit *src, ObIAllocator *allocator)
{
  UNUSED(allocator);
  int ret = OB_SUCCESS;
  if (OB_ISNULL(src)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid src info", K(ret));
  } else if (OB_UNLIKELY(src->type() != type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid type", K(ret));
  } else if (OB_FAIL(assign(*static_cast<const ObTabletAutoincSeq *>(src)))) {
    LOG_WARN("failed to copy tablet autoinc seq", K(ret));
  }
  return ret;
}

void ObTabletAutoincSeq::reset()
{
  intervals_.reset();
}

bool ObTabletAutoincSeq::is_valid() const
{
  bool valid = true;

  if (intervals_.empty()) {
    valid = false;
  }
  // TODO(shuangcan.yjw): verify elemetns in array

  return valid;
}

int ObTabletAutoincSeq::get_autoinc_seq_value(uint64_t &autoinc_seq)
{
  int ret = OB_SUCCESS;
  if (intervals_.count() == 0) {
    autoinc_seq = 1;
  } else if (intervals_.count() == 1) {
    // currently, there will only be one interval
    for (int64_t i = 0; OB_SUCC(ret) && i < intervals_.count(); i++) {
      const ObTabletAutoincInterval &interval = intervals_.at(i);
      if (interval.end_ > interval.start_) {
        autoinc_seq = interval.start_;
        break;
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoinc seq interval count", K(ret));
  }
  return ret;
}

int ObTabletAutoincSeq::set_autoinc_seq_value(const uint64_t autoinc_seq)
{
  int ret = OB_SUCCESS;
  // currently, there will only be one interval
  if (intervals_.count() == 0) {
    ObTabletAutoincInterval interval;
    interval.start_ = autoinc_seq;
    interval.end_ = INT64_MAX;
    if (OB_FAIL(intervals_.push_back(interval))) {
      LOG_WARN("failed to push autoinc interval", K(ret));
    }
  } else if (intervals_.count() == 1) {
    intervals_.at(0).start_ = autoinc_seq;
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected autoinc seq interval count", K(ret));
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTabletAutoincSeq, intervals_);

}// end namespace share
}// end namespace oceanbase

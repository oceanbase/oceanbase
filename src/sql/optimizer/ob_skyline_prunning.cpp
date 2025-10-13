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

#define USING_LOG_PREFIX SQL_OPT

#include "ob_skyline_prunning.h"
#include "sql/optimizer/ob_optimizer_util.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

/*
 * 是否需要回表的比较
 * */
int ObIndexBackDim::compare(const ObSkylineDim &other, CompareStat &status) const
{
  int ret = OB_SUCCESS;
  status = UNCOMPARABLE;
  if (other.get_dim_type() != get_dim_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension type is different",
             "dim_type", get_dim_type(), "other.dim_type", other.get_dim_type());
  } else {
    const ObIndexBackDim &tmp = static_cast<const ObIndexBackDim&>(other);
    if (need_index_back_ == tmp.need_index_back_) {
      status = EQUAL;
    } else if (!need_index_back_ && tmp.need_index_back_) {
      status = LEFT_DOMINATED;
    } else if (need_index_back_ && !tmp.need_index_back_) {
      status = RIGHT_DOMINATED;
    }
  }
  return ret;
}

int ObInterestOrderDim::add_filter_column_ids(const common::ObIArray<uint64_t> &filter_column_ids)
{
  int ret = OB_SUCCESS;
  if (filter_column_ids.count() < 0 || filter_column_ids.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many columns", K(ret), K(filter_column_ids.count()));
  } else if (OB_FAIL(add_column_ids(filter_column_ids_, filter_column_ids))) {
    LOG_WARN("failed to add column ids", K(ret));
  }
  return ret;
}
/*
 * 比较interesting order
 * */
int ObInterestOrderDim::compare(const ObSkylineDim &other, CompareStat &status) const
{
  int ret = OB_SUCCESS;
  if (other.get_dim_type() != get_dim_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension type is different",
             "dim_type", get_dim_type(), "other.dim_type", other.get_dim_type());
  } else {
    const ObInterestOrderDim &tmp = static_cast<const ObInterestOrderDim &>(other);
    if (is_interesting_order_ && tmp.is_interesting_order_) {
      KeyPrefixComp comp;
      if (OB_FAIL(comp(column_ids_, const_column_info_,
                       tmp.column_ids_, tmp.const_column_info_))) {
        LOG_WARN("compare key prefix failed", K(ret), K(*this), K(other));
      } else {
        status = comp.get_result();
      }
    } else if (!is_interesting_order_ && !tmp.is_interesting_order_) {
      status = EQUAL;
    } else if (is_interesting_order_ && !tmp.is_interesting_order_) {
      status = LEFT_DOMINATED;
    } else if (!is_interesting_order_ && tmp.is_interesting_order_) {
      status = RIGHT_DOMINATED;
    }
    if (OB_SUCC(ret) && !can_extract_range_ && !tmp.can_extract_range_ &&
        need_index_back_ && tmp.need_index_back_ &&
        (LEFT_DOMINATED == status or RIGHT_DOMINATED == status)) {
      RangeSubsetComp comp;
      if (OB_FAIL(comp(filter_column_ids_, tmp.filter_column_ids_))) {
        LOG_WARN("compare query range failed", K(ret), K(*this), K(other));
      } else if (LEFT_DOMINATED == status &&
                 (LEFT_DOMINATED == comp.get_result() || EQUAL == comp.get_result())) {
        /*do nothing*/
      } else if (RIGHT_DOMINATED == status &&
                 (RIGHT_DOMINATED == comp.get_result() || EQUAL == comp.get_result())) {
        /* do nothing*/
      } else {
        status = UNCOMPARABLE;
      }
    } else { /*do nothing*/ }
  }
  return ret;
}

int ObInterestOrderDim::add_interest_prefix_ids(const common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (column_ids.count() < 0 || column_ids.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many rowkey ids", K(ret), K(column_ids.count()));
  } else if (OB_FAIL(column_ids_.assign(column_ids))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

int ObInterestOrderDim::add_const_column_info(const common::ObIArray<bool> &const_column_info)
{
  int ret = OB_SUCCESS;
  if (const_column_info.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many rowkey ids", K(ret), K(const_column_info.count()));
  } else if (OB_FAIL(const_column_info_.assign(const_column_info))) {
    LOG_WARN("failed to assign", K(ret));
  }
  return ret;
}

/*
 * 比较left 和right之间的关系, id按索引列的顺序排序
 * [16] RIGHT_DOMINATED [16, 17]
 * [16, 17] LEFT_DOMINATED [17]
 * [16, 17] UNCOMPARABLE [17, 16]
 * [16, 18] EQUAL [16, 18]
 * */
int KeyPrefixComp::operator()(const ObIArrayWrap<uint64_t> &left, const ObIArrayWrap<bool> &left_const,
                              const ObIArrayWrap<uint64_t> &right, const ObIArrayWrap<bool> &right_const)
{
  int ret = OB_SUCCESS;
  const int64_t left_cnt = left.count();
  const int64_t right_cnt = right.count();

  if (OB_UNLIKELY(left_cnt < 0) || OB_UNLIKELY(right_cnt < 0) ||
      OB_UNLIKELY(left_cnt != left_const.count()) ||
      OB_UNLIKELY(right_cnt != right_const.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(left_cnt), K(right_cnt), K(ret));
  } else if (0 == left_cnt && 0 == right_cnt) {
    status_ = ObSkylineDim::EQUAL;
  } else if (left_cnt == 0 || right_cnt == 0) {
    status_ = left_cnt > right_cnt
        ? ObSkylineDim::LEFT_DOMINATED : ObSkylineDim::RIGHT_DOMINATED;
  } else if (left_cnt <= right_cnt) {
    if (OB_FAIL(do_compare(left, right, right_const, status_))) {
      LOG_WARN("compare key prefix failed", K(ret));
    }
  } else {
    //reverse
    ObSkylineDim::CompareStat tmp = ObSkylineDim::UNCOMPARABLE;
    if (OB_FAIL(do_compare(right, left, left_const, tmp))) {
      LOG_WARN("compare key prefix failed", K(ret));
    } else {
      if (ObSkylineDim::RIGHT_DOMINATED == tmp) {
        status_ = ObSkylineDim::LEFT_DOMINATED;
      } else {
        status_ = tmp;
      }
    }
  }
  return ret;
}

int KeyPrefixComp::do_compare(const ObIArrayWrap<uint64_t> &left,
                              const ObIArrayWrap<uint64_t> &right,
                              const ObIArrayWrap<bool> &right_const,
                              ObSkylineDim::CompareStat &status)
{
  int ret = OB_SUCCESS;
  const int64_t left_cnt = left.count();
  const int64_t right_cnt = right.count();
  if (left_cnt > right_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left cnt is bigger than right count", K(left_cnt), K(right_cnt), K(ret));
  } else {
    status = ObSkylineDim::EQUAL;
    int i = 0;
    int j = 0;
    while (i < left_cnt && j < right_cnt && ObSkylineDim::EQUAL == status) {
      if (left.at(i) == right.at(j)) {
        i++;
        j++;
      } else if (right_const.at(j)) {
        j++;
      } else {
        status = ObSkylineDim::UNCOMPARABLE;
      }
    }
    if (ObSkylineDim::EQUAL == status) {
      if (i < left_cnt && j >= right_cnt) {
        status = ObSkylineDim::UNCOMPARABLE;
      } else if (left_cnt < right_cnt) {
        status = ObSkylineDim::RIGHT_DOMINATED;
      } else { /*do nothing*/ }
    }
  }
  return ret;
}

/*
 * 比较left 和right 之间的关系, 加进数组之前会先排序
 * [16, 17] EQUAL [16, 17]
 * [16, 17, 18] LEFT_DOMINATED [16, 17] 
 * [16, 18] RIGHT_DOMINATED [16, 18 ,20]
 * [16, 19] UNCOMPARABLE [17, 18]
 * */
int RangeSubsetComp::operator()(const ObIArrayWrap<uint64_t> &left, const ObIArrayWrap<uint64_t> &right)
{
  int ret = OB_SUCCESS;
  const int64_t left_cnt = left.count();
  const int64_t right_cnt = right.count();
  if (left_cnt < 0 || right_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left_cnt), K(right_cnt), K(ret));
  } else if (0 == left_cnt && 0 == right_cnt) {
    status_ = ObSkylineDim::EQUAL;
  } else if (left_cnt == 0 || right_cnt == 0) {
    status_ = left_cnt > right_cnt
        ? ObSkylineDim::LEFT_DOMINATED : ObSkylineDim::RIGHT_DOMINATED;
  } else if (left_cnt <= right_cnt) {
    if (OB_FAIL(do_compare(left, right, status_))) {
      LOG_WARN("compare key prefix failed", K(ret));
    }
  } else {
    //reverse
    ObSkylineDim::CompareStat tmp = ObSkylineDim::UNCOMPARABLE;
    if (OB_FAIL(do_compare(right, left, tmp))) {
      LOG_WARN("compare range subset failed", K(ret));
    } else {
      if (ObSkylineDim::RIGHT_DOMINATED == tmp) {
        status_ = ObSkylineDim::LEFT_DOMINATED;
      } else {
        status_ = tmp;
      }
    }
  }
  return ret;
}

/**
 * @status could be EQUAL, UNCOMPARABLE, ANTI_DOMINATED
 */
int RangeSubsetComp::do_compare(const ObIArrayWrap<uint64_t> &left,
                                const ObIArrayWrap<uint64_t> &right,
                                ObSkylineDim::CompareStat &status)
{
  int ret = OB_SUCCESS;
  const int64_t left_cnt = left.count();
  const int64_t right_cnt = right.count();
  if (OB_UNLIKELY(left_cnt > right_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left cnt is bigger than right count", K(left_cnt), K(right_cnt));
  } else {
    status = ObSkylineDim::EQUAL;
    //left [16,18] right [15, 16, 17, 18, 20], left is subset of right
    int64_t found_cnt = 0;
    int64_t last_pos = 0;
    for (int64_t i = 0; i < left_cnt; ++i) {
      bool found = false;
      for (int64_t j = last_pos; !found && j < right_cnt; ++j) {
        if (left.at(i) == right.at(j)) {
          found = true;
          found_cnt++;
          last_pos = ++j;
        } else {
          LOG_TRACE("not equal", K(left.at(i)), K(right.at(j)));
        }
      }
      if (!found) {
        status = ObSkylineDim::UNCOMPARABLE;
        break;
      }
    }
    if (status != ObSkylineDim::UNCOMPARABLE) {
      if (found_cnt == right_cnt) {
        status = ObSkylineDim::EQUAL;
      } else if (found_cnt < right_cnt) {
        status = ObSkylineDim::RIGHT_DOMINATED;
      }
    }
  }
  return ret;
}

int ObSkylineDim::add_column_ids(common::ObIArray<uint64_t> &dst_column_ids,
                                 const common::ObIArray<uint64_t> &src_column_ids)
{
  int ret = OB_SUCCESS;
  if (src_column_ids.count() < 0 || src_column_ids.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too many rowkey ids", K(ret), K(src_column_ids.count()));
  } else if (OB_FAIL(dst_column_ids.assign(src_column_ids))) {
    LOG_WARN("failed to assign", K(ret));
  } else if (dst_column_ids.empty()) {
    // do nothing
  } else {
    lib::ob_sort(&dst_column_ids.at(0), &dst_column_ids.at(0) + dst_column_ids.count());//do sort, for quick compare
  }
  return ret;
}

int ObSkylineDim::compare_columns(const common::ObIArray<uint64_t> &left_column_ids,
                                  const common::ObIArray<uint64_t> &right_column_ids,
                                  CompareStat &status)
{
  int ret = OB_SUCCESS;
  status = EQUAL;
  RangeSubsetComp comp;
  if (OB_FAIL(comp(left_column_ids, right_column_ids))) {
    LOG_WARN("compare query range failed", K(ret));
  } else {
    status = comp.get_result();
  }
  return ret;
}

int ObQueryRangeDim::compare(const ObSkylineDim &other, CompareStat &status) const
{
  int ret = OB_SUCCESS;
  status = EQUAL;
  if (other.get_dim_type() != get_dim_type()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dimension type is different",
             "dim_type", get_dim_type(), "other.dim_type", other.get_dim_type());
  } else {
    const ObQueryRangeDim &tmp = static_cast<const ObQueryRangeDim &>(other);
    CompareStat offset_status = UNCOMPARABLE;
    bool skip_scan_comparable = skip_scan_comparable_ && tmp.skip_scan_comparable_;
    if (contain_always_false_ && tmp.contain_always_false_) {
      status = EQUAL;
    } else if (contain_always_false_ || tmp.contain_always_false_) {
      status = UNCOMPARABLE;
    } else if (OB_FAIL(compare_columns(range_column_ids_, tmp.range_column_ids_, status))) {
      LOG_WARN("failed to compare columns", K(ret));
    } else if (EQUAL == status) {
      // range is equal, compare skip scan info
      CompareStat range_status = UNCOMPARABLE;
      CompareStat offset_status = UNCOMPARABLE;
      if (ss_range_column_ids_.empty() && tmp.ss_range_column_ids_.empty()) {
        status = EQUAL;
      } else if (ss_range_column_ids_.empty() && !tmp.ss_range_column_ids_.empty()) {
        status = RIGHT_DOMINATED;
      } else if (!ss_range_column_ids_.empty() && tmp.ss_range_column_ids_.empty()) {
        status = LEFT_DOMINATED;
      } else if (OB_FAIL(compare_columns(ss_range_column_ids_, tmp.ss_range_column_ids_, range_status))) {
        LOG_WARN("failed to compare columns", K(ret));
      } else if (UNCOMPARABLE == range_status) {
        status = UNCOMPARABLE;
      } else if (OB_FAIL(compare_columns(ss_offset_column_ids_, tmp.ss_offset_column_ids_, offset_status))) {
        LOG_WARN("failed to compare columns", K(ret));
      } else {
        // prefer ss index which has less offset column and more range column
        if (EQUAL == range_status && EQUAL == offset_status) {
          status = EQUAL;
        } else if (range_status == offset_status) {
          status = UNCOMPARABLE;
        } else if (LEFT_DOMINATED == range_status ||
                   RIGHT_DOMINATED == offset_status) {
          status = LEFT_DOMINATED;
        } else if (RIGHT_DOMINATED == range_status ||
                   LEFT_DOMINATED == offset_status) {
          status = RIGHT_DOMINATED;
        }
      }
      if (!skip_scan_comparable &&
          (LEFT_DOMINATED == status ||
           RIGHT_DOMINATED == status )) {
        status = UNCOMPARABLE;
      }
    }
  }
  return ret;
}

int ObUniqueRangeDim::compare(const ObSkylineDim &other, CompareStat &status) const
{
  int ret = OB_SUCCESS;
  status = EQUAL;
  if (other.get_dim_type() != get_dim_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension type is different",
             "dim_type", get_dim_type(), "other.dim_type", other.get_dim_type());
  } else {
    const ObUniqueRangeDim &tmp = static_cast<const ObUniqueRangeDim &>(other);
    if (range_cnt_ > tmp.range_cnt_) {
      status = RIGHT_DOMINATED;
    } else if (range_cnt_ < tmp.range_cnt_) {
      status = LEFT_DOMINATED;
    } else {
      status = EQUAL;
    }
  }
  return ret;
}

int ObShardingInfoDim::compare(const ObSkylineDim &other, CompareStat &status) const
{
  int ret = OB_SUCCESS;
  if (other.get_dim_type() != get_dim_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension type is different",
             "dim_type", get_dim_type(), "other.dim_type", other.get_dim_type());
  // if one of them is unstable global index, we treat the other one as dominated
  } else if (is_unstable_global_index() &&
             !static_cast<const ObShardingInfoDim &>(other).is_unstable_global_index()) {
    status = ObSkylineDim::RIGHT_DOMINATED;
  } else if (!is_unstable_global_index() &&
             static_cast<const ObShardingInfoDim &>(other).is_unstable_global_index()) {
    status = ObSkylineDim::LEFT_DOMINATED;
  } else {
    status = ObSkylineDim::EQUAL;
    const ObShardingInfoDim &tmp = static_cast<const ObShardingInfoDim &>(other);
    DominateRelation strong_relation = DominateRelation::OBJ_UNCOMPARABLE;
    EqualSets dummy;
    if (is_single_get_ && tmp.is_single_get_) {
      status = ObSkylineDim::EQUAL;
    } else if (OB_FAIL(ObOptimizerUtil::compute_sharding_relationship(sharding_info_,
                                              tmp.sharding_info_,
                                              dummy,
                                              strong_relation))) {
      LOG_WARN("failed to compute sharding relationship", K(ret));
    } else if (strong_relation == DominateRelation::OBJ_EQUAL) {
      status = ObSkylineDim::EQUAL;
    } else if (strong_relation == DominateRelation::OBJ_LEFT_DOMINATE) {
      status = ObSkylineDim::LEFT_DOMINATED;
    } else if (strong_relation == DominateRelation::OBJ_RIGHT_DOMINATE) {
      status = ObSkylineDim::RIGHT_DOMINATED;
    } else {
      status = ObSkylineDim::UNCOMPARABLE;
    }
  }
  return ret;
}

/*
 * 对四个维度进行比较
 * 有一个维度UNCOMPARABLE， 则不能比较
 * A LEFT_DOMINATED B, 则A至少存在某个维度上比B好, 其它维度必须EQUAL
 * */
int ObIndexSkylineDim::compare(const ObIndexSkylineDim &other, ObSkylineDim::CompareStat &status) const
{
  int ret = OB_SUCCESS;
  status = ObSkylineDim::EQUAL;
  int64_t compare_result = 0;
  LOG_TRACE("begin skyline compare index", KPC(this), K(other));
  OPT_TRACE("compare index", get_index_id(), "to index", other.get_index_id());
  OPT_TRACE_BEGIN_SECTION;
  for (int i = 0; OB_SUCC(ret) && status != ObSkylineDim::UNCOMPARABLE && i < dim_count_; ++i) {
    const ObSkylineDim *left_dim = skyline_dims_[i];
    const ObSkylineDim *right_dim = other.skyline_dims_[i];
    ObSkylineDim::CompareStat tmp_status = ObSkylineDim::UNCOMPARABLE;
    if (OB_ISNULL(left_dim) && OB_ISNULL(right_dim)) {
      //do nothing
    } else if (OB_ISNULL(left_dim) || OB_ISNULL(right_dim)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("skyline dimension should not be null", K(ret), K(i), K(left_dim), K(right_dim));
    } else if (OB_FAIL(left_dim->compare(*right_dim, tmp_status))) {
      LOG_WARN("compare skyline dimension failed", K(ret), K(i),
               K(*left_dim), K(*right_dim));
    } else {
      if (ObSkylineDim::UNCOMPARABLE == tmp_status) {
        status = ObSkylineDim::UNCOMPARABLE;
      } else if (ObSkylineDim::EQUAL == tmp_status) {
        //continue;
      } else if (ObSkylineDim::LEFT_DOMINATED == tmp_status) {
        if (compare_result >= 0) {
          compare_result++;
        } else {
          status = ObSkylineDim::UNCOMPARABLE;
        }
      } else if (ObSkylineDim::RIGHT_DOMINATED == tmp_status) {
        if (compare_result <= 0) {
          compare_result--;
        } else {
          status = ObSkylineDim::UNCOMPARABLE;
        }
      }
      LOG_TRACE("skyline compare dim", K(i), K(tmp_status), K(compare_result), KPC(left_dim), KPC(right_dim));
      OPT_TRACE("compare dim", static_cast<int64_t>(i), "result:", static_cast<int64_t>(tmp_status));
      OPT_TRACE("left dim: ", *left_dim);
      OPT_TRACE("right dim:", *right_dim);
    }
  }
  if (OB_SUCC(ret)) {
    if (ObSkylineDim::UNCOMPARABLE != status) {
      if (compare_result == 0) {
        status = ObSkylineDim::EQUAL;
      } else {
        status = compare_result > 0 ? ObSkylineDim::LEFT_DOMINATED : ObSkylineDim::RIGHT_DOMINATED;
      }
    }
    if (ObSkylineDim::EQUAL == status) {
      if (is_get_ && !other.is_get_) {
        status = ObSkylineDim::LEFT_DOMINATED;
        OPT_TRACE("all dims are equal, while the left is table get");
      } else if (!is_get_ && other.is_get_) {
        status = ObSkylineDim::RIGHT_DOMINATED;
        OPT_TRACE("all dims are equal, while the right is table get");
      }
    }
  }
  LOG_TRACE("finish skyline compare index", K(status));
  OPT_TRACE("compare result (UNCOMPARABLE:-2, RIGHT_DOMINATED:-1, EQUAL:0, LEFT_DOMINATED:1):", static_cast<int64_t>(status));
  OPT_TRACE_END_SECTION;
  return ret;
}

int ObIndexSkylineDim::add_skyline_dim(const ObSkylineDim &dim)
{
  int ret = OB_SUCCESS;
  const int64_t idx = static_cast<int64_t>(dim.get_dim_type());
  if (idx < 0 || idx >= ObSkylineDim::DIM_COUNT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid idx", K(ret));
  } else if (skyline_dims_[idx] != NULL) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("skyline dim is not null", K(ret), K(idx), K(dim));
  } else {
    skyline_dims_[idx] = &dim;
  }
  return ret;
}

int ObIndexSkylineDim::add_index_back_dim(const bool is_index_back,
                                          ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObIndexBackDim *dim = NULL;
  if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(allocator, dim))) {
    LOG_WARN("failed to create index_back dimension", K(ret));
  } else if (OB_ISNULL(dim)){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create dimension", K(ret));
  } else {
    dim->set_index_back(is_index_back);
    if (OB_FAIL(add_skyline_dim(*dim))) {
      LOG_WARN("failed to add skyline dimension", K(ret));
    } else {
      LOG_TRACE("add index back dim success", K(ret), K(*dim));
    }
  }
  return ret;
}

int ObIndexSkylineDim::add_interesting_order_dim(const bool is_index_back,
                                                 const bool can_extract_range,
                                                 const ObIArray<uint64_t> &filter_column_ids,
                                                 const ObIArray<uint64_t> &interest_column_ids,
                                                 const ObIArray<bool> &const_column_info,
                                                 ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObInterestOrderDim *dim = NULL;
  if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(allocator, dim))) {
    LOG_WARN("failed to create interesting_order dimension", K(ret));
  } else if (OB_ISNULL(dim)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create dimension", K(ret));
  } else {
    dim->set_index_back(is_index_back);
    dim->set_extract_range(can_extract_range);
    if (is_index_back && !can_extract_range) {
      if (OB_FAIL(dim->add_filter_column_ids(filter_column_ids))) {
        LOG_WARN("failed to add filter column id", K(ret));
      } else { /*do nothing*/ }
    }
    if (OB_SUCC(ret)) {
      if (interest_column_ids.count() > 0) {
        dim->set_interesting_order(true);
        if (OB_FAIL(dim->add_interest_prefix_ids(interest_column_ids))) {
          LOG_WARN("failed to add interest prefix id", K(ret));
        } else if (OB_FAIL(dim->add_const_column_info(const_column_info))) {
          LOG_WARN("failed to add const column info", K(ret));
        }
      } else {
        dim->set_interesting_order(false);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_skyline_dim(*dim))) {
        LOG_WARN("failed to add skyline dimension", K(ret));
      } else {
        LOG_TRACE("add interesting order dim success", K(ret), K(*dim));
      }
    }
  }
  return ret;
}

int ObIndexSkylineDim::add_query_range_dim(const ObIArray<uint64_t> &prefix_range_ids,
                                           const common::ObIArray<uint64_t> &ss_range_ids,
                                           const common::ObIArray<uint64_t> &ss_offset_ids,
                                           ObIAllocator &allocator,
                                           bool contain_always_false,
                                           bool skip_scan_comparable)
{
  int ret = OB_SUCCESS;
  ObQueryRangeDim *dim = NULL;
  if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(allocator, dim))) {
    LOG_WARN("failed to create key prefix dimension", K(ret));
  } else if (OB_ISNULL(dim)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create dimension", K(ret));
  } else {
    if (OB_SUCC(ret)) {
      dim->set_contain_always_false(contain_always_false);
      dim->set_skip_scan_comparable(skip_scan_comparable);
      if (OB_FAIL(dim->add_range_column_ids(prefix_range_ids))) {
        LOG_WARN("failed to add rowkey ids", K(ret));
      } else if (OB_FAIL(dim->add_ss_range_column_ids(ss_range_ids))) {
        LOG_WARN("failed to add rowkey ids", K(ret));
      } else if (OB_FAIL(dim->add_ss_offset_column_ids(ss_offset_ids))) {
        LOG_WARN("failed to add rowkey ids", K(ret));
      } else if (OB_FAIL(add_skyline_dim(*dim))) {
        LOG_WARN("failed to add_skyline_dim", K(ret));
      } else {
        LOG_TRACE("add query range dim success", K(ret), K(*dim));
      }
    }
  }
  return ret;
}

int ObIndexSkylineDim::add_unique_range_dim(int64_t range_cnt, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObUniqueRangeDim *dim = NULL;
  if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(allocator, dim))) {
    LOG_WARN("failed to create key prefix dimension", K(ret));
  } else if (OB_ISNULL(dim)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create dimension", K(ret));
  } else {
    dim->set_range_count(range_cnt);
    if (OB_FAIL(add_skyline_dim(*dim))) {
      LOG_WARN("failed to add_skyline_dim", K(ret));
    } else {
      LOG_TRACE("add query range dim success", K(ret), K(*dim));
    }
  }
  return ret;
}

int ObIndexSkylineDim::add_sharding_info_dim(ObShardingInfo *sharding_info,
                                             bool is_get,
                                             bool is_global_index,
                                             bool is_index_back,
                                             bool can_extract_range,
                                             ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObShardingInfoDim *dim = NULL;
  if (OB_FAIL(ObSkylineDimFactory::get_instance().create_skyline_dim(allocator, dim))) {
    LOG_WARN("failed to create key prefix dimension", K(ret));
  } else if (OB_ISNULL(dim)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to create dimension", K(ret));
  } else {
    dim->set_sharding_info(sharding_info);
    dim->set_is_single_get(is_get);
    dim->set_is_global_index(is_global_index);
    dim->set_is_index_back(is_index_back);
    dim->set_can_extract_range(can_extract_range);
    if (OB_FAIL(add_skyline_dim(*dim))) {
      LOG_WARN("failed to add_skyline_dim", K(ret));
    } else {
      LOG_TRACE("add partition num dim success", K(ret), K(*dim));
    }
  }
  return ret;
}

int ObSkylineDimRecorder::add_index_dim(const ObIndexSkylineDim &dim, bool &has_add)
{
  int ret = OB_SUCCESS;
  has_add = false;
  ObArray<int64_t> remove_idxs;
  bool need_add = true;
  LOG_TRACE("Skyline Pruning try to add index", K(dim));
  OPT_TRACE("try to add index", dim.get_index_id());
  OPT_TRACE_BEGIN_SECTION;
  if (!dim.can_prunning()) {
    //can't prunning, just add
    if (OB_FAIL(index_dims_.push_back(&dim))) {
      LOG_WARN("failed to push_back index dim", K(ret), K(dim));
    } else {
      has_add = true;
    }
    LOG_TRACE("Index can not be pruning");
    OPT_TRACE("Index can not be pruning");
  } else {
    if (OB_FAIL(has_dominate_dim(dim, remove_idxs, need_add))) {
      LOG_WARN("check has dominate index failed", K(dim));
    } else if (need_add) {
      //remove from back, idx id is in ascending order
      //if need to remove
      //需要添加的情况下，把那些被dim dominated的索引剪掉
      for (int64_t i = remove_idxs.count() - 1; OB_SUCC(ret) && i >= 0; --i) {
        if (OB_FAIL(index_dims_.remove(remove_idxs.at(i)))) {
          LOG_WARN("remove index dimension failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_FAIL(index_dims_.push_back(&dim))) {
          LOG_WARN("failed to push_back index dim", K(ret), K(dim));
        } else {
          has_add = true;
        }
      }
      LOG_TRACE("succeed to add index", K(dim));
      OPT_TRACE("succeed to add index ", dim.get_index_id());
    }
  }
  OPT_TRACE_END_SECTION;
  return ret;
}

int ObSkylineDimRecorder::get_dominated_idx_ids(ObIArray<uint64_t> &dominated_idxs)
{
  int ret = OB_SUCCESS;
  dominated_idxs.reset();
  for (int i = 0; OB_SUCC(ret) && i < index_dims_.count(); ++i) {
    const ObIndexSkylineDim *index_dim = index_dims_.at(i);
    if (OB_ISNULL(index_dim)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index_dim should not be null", K(ret));
    } else if (OB_FAIL(dominated_idxs.push_back(index_dim->get_index_id()))) {
      LOG_WARN("push_back dominated index failed", K(ret), K(i));
    }
  }
  return ret;
}

/*
 * 判断index_dims_是否有比dim好的索引
 * (1) 存在比dim好的索引A，则dim不用添加，可以剪掉
 * (2) dim比recorder的某些维度好，会记录索引id到 remove_idxs里面
 * need_add表示dim是否需要添加
 * */
int ObSkylineDimRecorder::has_dominate_dim(const ObIndexSkylineDim &dim,
                                           ObIArray<int64_t> &remove_idxs,
                                           bool &need_add)
{
  int ret = OB_SUCCESS;
  need_add = true;
  remove_idxs.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < index_dims_.count(); ++i) {
    ObSkylineDim::CompareStat status = ObSkylineDim::UNCOMPARABLE;
    const ObIndexSkylineDim *index_dim = index_dims_.at(i);
    if (!index_dim->can_prunning()) {
      LOG_TRACE("ignore index that can't not be prunning", K(*index_dim));
      OPT_TRACE("index", index_dim->get_index_id(), "can't be prunning");
    } else if (OB_FAIL(index_dim->compare(dim, status))) {
      LOG_WARN("compare skyline dimension failed", K(ret), K(dim), K(i)); 
    } else if (ObSkylineDim::LEFT_DOMINATED == status) {
      if (remove_idxs.count() > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid compare, has anti_dominated index add before",
                 K(ret), K(i), K(remove_idxs), K(*index_dim), K(dim));
      } else {
        need_add = false;
        LOG_TRACE("index is prunning by exists index", KPC(index_dim));
        OPT_TRACE("index", dim.get_index_id(), "is prunning by index", index_dim->get_index_id());
        break;//not continue
      }
    } else if (ObSkylineDim::RIGHT_DOMINATED == status) {
      //record those ANTI_DOMINATED INDEXS
      if (OB_FAIL(remove_idxs.push_back(i))) {
        LOG_WARN("failed to add dominate idx", K(ret));
      }
      LOG_TRACE("index right dominated exists index", KPC(index_dim));
      OPT_TRACE("index", dim.get_index_id(), "prune index", index_dim->get_index_id());
    }
  }
  return ret;
}

int ObSkylineDimRecorder::extract_column_ids(const common::ObIArray<ObRawExpr*> &keys,
                                             const int64_t prefix_count,
                                             common::ObIArray<uint64_t> &column_ids)
{
  int ret = OB_SUCCESS;
  if (keys.count() < prefix_count) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("keys count is small than prefix_count", K(ret), K(prefix_count), K(keys.count()));
  } else {
    column_ids.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < prefix_count; ++i) {
      ObRawExpr *expr = keys.at(i);
      ObColumnRefRawExpr *column_expr = static_cast<ObColumnRefRawExpr *>(expr);
      if (OB_ISNULL(expr) || OB_ISNULL(column_expr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("expr should not be null", K(ret), K(expr), K(column_expr));
      } else {
        const uint64_t column_id = column_expr->get_column_id();
        if (OB_FAIL(column_ids.push_back(column_id))) {
          LOG_WARN("push back column_id failed", K(column_id));
        }
      }
    }
  }
  return ret;
}

ObSkylineDimFactory &ObSkylineDimFactory::get_instance()
{
  static ObSkylineDimFactory instance;
  return instance;
}

 
}
}

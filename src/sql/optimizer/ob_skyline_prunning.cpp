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
#include "sql/resolver/expr/ob_raw_expr.h"


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
      status = UNCOMPARABLE;
      //在都抽不出query range和 没有 interesting order的情况下
      //我们考虑两边的列的大小
      //只有左边的restrict info是右边的super set的情况下
      //并且 左边的列比右边的列少的情况下
      //左边才算dominated右边
      if (!has_interesting_order_ && !can_extract_range_
          && !tmp.has_interesting_order_ && !tmp.can_extract_range_) {
        //both not interesting order and not extract range
        if (tmp.filter_column_cnt_ == 0) {
          //右边抽不出条件，会走索引全表扫描+ 回表 剪掉
          status = LEFT_DOMINATED;
        } else if (index_column_cnt_ <= tmp.index_column_cnt_) {
          status = LEFT_DOMINATED;
        }
      } else {
        status = LEFT_DOMINATED;
      }
    } else if (need_index_back_ && !tmp.need_index_back_) {
      status = UNCOMPARABLE;
      if (!has_interesting_order_ && !can_extract_range_
          && !tmp.has_interesting_order_ && !tmp.can_extract_range_) {
        if (0 == filter_column_cnt_) {
          //左边抽不出条件，会走索引全表扫描+回表， 剪掉
          status = RIGHT_DOMINATED;
        } else if (index_column_cnt_ >= tmp.index_column_cnt_) {
          status = RIGHT_DOMINATED;
        }
      } else {
        status = RIGHT_DOMINATED;
      }
    }
  }
  return ret;
}

int ObIndexBackDim::add_filter_column_ids(const common::ObIArray<uint64_t> &filter_column_ids)
{
  int ret = OB_SUCCESS;
  if (filter_column_ids.count() < 0 || filter_column_ids.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many columns", K(ret), K(filter_column_ids.count()));
  } else {
    MEMSET(filter_column_ids_, 0, sizeof(uint64_t) * OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    for (int i = 0; OB_SUCC(ret) && i < filter_column_ids.count(); ++i) {
      filter_column_ids_[i] = filter_column_ids.at(i);
    }
    filter_column_cnt_ = filter_column_ids.count();
    std::sort(filter_column_ids_, filter_column_ids_ + filter_column_cnt_);//do sort, for quick compare
  }
  return ret;
}

int ObInterestOrderDim::add_filter_column_ids(const common::ObIArray<uint64_t> &filter_column_ids)
{
  int ret = OB_SUCCESS;
  if (filter_column_ids.count() < 0 || filter_column_ids.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many columns", K(ret), K(filter_column_ids.count()));
  } else {
    MEMSET(filter_column_ids_, 0, sizeof(uint64_t) * OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    for (int i = 0; OB_SUCC(ret) && i < filter_column_ids.count(); ++i) {
      filter_column_ids_[i] = filter_column_ids.at(i);
    }
    filter_column_cnt_ = filter_column_ids.count();
    std::sort(filter_column_ids_, filter_column_ids_ + filter_column_cnt_);//do sort, for quick compare
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
      if (OB_FAIL(comp(column_ids_, const_column_info_, column_cnt_,
                       tmp.column_ids_, tmp.const_column_info_, tmp.column_cnt_))) {
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
      if (OB_FAIL(comp(filter_column_ids_, filter_column_cnt_,
                       tmp.filter_column_ids_, tmp.filter_column_cnt_))) {
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
  } else {
    MEMSET(column_ids_, 0, sizeof(uint64_t) * OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    for (int i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
      column_ids_[i] = column_ids.at(i);
    }
    column_cnt_ = column_ids.count();
  }
  return ret;
}

int ObInterestOrderDim::add_const_column_info(const common::ObIArray<bool> &const_column_info)
{
  int ret = OB_SUCCESS;
  if (const_column_info.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many rowkey ids", K(ret), K(const_column_info.count()));
  } else {
    MEMSET(const_column_info_, 0, sizeof(bool) * OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    for (int i= 0; OB_SUCC(ret) && i < const_column_info.count(); i++) {
      const_column_info_[i] = const_column_info.at(i);
    }
    column_cnt_ = const_column_info.count();
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
int KeyPrefixComp::operator()(const uint64_t *left, const bool *left_const,
                              const int64_t left_cnt, const uint64_t *right,
                              const bool *right_const, const int64_t right_cnt)
{
  int ret = OB_SUCCESS;
  if (left_cnt < 0 || right_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arugment", K(ret), K(left_cnt), K(right_cnt), K(ret));
  } else if (0 == left_cnt && 0 == right_cnt) {
    status_ = ObSkylineDim::EQUAL;
  } else if (left_cnt == 0 || right_cnt == 0) {
    status_ = left_cnt > right_cnt
        ? ObSkylineDim::LEFT_DOMINATED : ObSkylineDim::RIGHT_DOMINATED;
  } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ptr should not be null", K(ret), K(left), K(right));
  } else if (left_cnt <= right_cnt) {
    if (OB_FAIL(do_compare(left, left_cnt, right, right_const, right_cnt, status_))) {
      LOG_WARN("compare key prefix failed", K(ret));
    }
  } else {
    //reverse
    ObSkylineDim::CompareStat tmp = ObSkylineDim::UNCOMPARABLE;
    if (OB_FAIL(do_compare(right, right_cnt, left, left_const, left_cnt, tmp))) {
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

int KeyPrefixComp::do_compare(const uint64_t *left, const int64_t left_cnt,
                              const uint64_t *right, const bool *right_const,
                              const int64_t right_cnt, ObSkylineDim::CompareStat &status)
{
  int ret = OB_SUCCESS;
  if (left_cnt > right_cnt) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("left cnt is bigger than right count", K(left_cnt), K(right_cnt), K(ret));
  } else {
    status = ObSkylineDim::EQUAL;
    int i = 0;
    int j = 0;
    while (i < left_cnt && j < right_cnt && ObSkylineDim::EQUAL == status) {
      if (left[i] == right[j]) {
        i++;
        j++;
      } else if (right_const[j]) {
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
int RangeSubsetComp::operator()(const uint64_t *left, const int64_t left_cnt,
                                const uint64_t *right, const int64_t right_cnt)
{
  int ret = OB_SUCCESS;
  if (left_cnt < 0 || right_cnt < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(left_cnt), K(right_cnt), K(ret));
  } else if (0 == left_cnt && 0 == right_cnt) {
    status_ = ObSkylineDim::EQUAL;
  } else if (left_cnt == 0 || right_cnt == 0) {
    status_ = left_cnt > right_cnt
        ? ObSkylineDim::LEFT_DOMINATED : ObSkylineDim::RIGHT_DOMINATED;
  } else if (OB_ISNULL(left) || OB_ISNULL(right)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ptr should not be null", K(ret), K(left), K(right));
  } else if (left_cnt <= right_cnt) {
    if (OB_FAIL(do_compare(left, left_cnt, right, right_cnt, status_))) {
      LOG_WARN("compare key prefix failed", K(ret));
    }
  } else {
    //reverse
    ObSkylineDim::CompareStat tmp = ObSkylineDim::UNCOMPARABLE;
    if (OB_FAIL(do_compare(right, right_cnt, left, left_cnt, tmp))) {
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
int RangeSubsetComp::do_compare(const uint64_t *left, const int64_t left_cnt,
                                const uint64_t *right, const int64_t right_cnt,
                                ObSkylineDim::CompareStat &status)
{
  int ret = OB_SUCCESS;
  if (left_cnt > right_cnt) {
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
        if (left[i] == right[j]) {
          found = true;
          found_cnt++;
          last_pos = ++j;
        } else {
          LOG_TRACE("not equal", K(left[i]), K(right[j]));
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

int ObQueryRangeDim::compare(const ObSkylineDim &other, CompareStat &status) const
{
  int ret = OB_SUCCESS;
  status = EQUAL;
  if (other.get_dim_type() != get_dim_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dimension type is different",
             "dim_type", get_dim_type(), "other.dim_type", other.get_dim_type());
  } else {
    const ObQueryRangeDim &tmp = static_cast<const ObQueryRangeDim &>(other);
    if (column_cnt_ == 0 && tmp.column_cnt_ == 0) {
      status = EQUAL; //both can't not extract query range, equal
    } else {
      RangeSubsetComp comp;
      if (OB_FAIL(comp(column_ids_, column_cnt_,
                       tmp.column_ids_, tmp.column_cnt_))) {
        LOG_WARN("compare query range failed", K(ret),
                 K(*this), K(other));
      } else {
        status = comp.get_result();
      }
    }
  }
  return ret;
}

int ObQueryRangeDim::add_rowkey_ids(const common::ObIArray<uint64_t> &rowkey_ids)
{
  int ret = OB_SUCCESS;
  if (rowkey_ids.count() < 0 || rowkey_ids.count() > OB_USER_MAX_ROWKEY_COLUMN_NUMBER) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("too many rowkey ids", K(ret), K(rowkey_ids.count()));
  } else {
    MEMSET(column_ids_, 0, sizeof(uint64_t) * OB_USER_MAX_ROWKEY_COLUMN_NUMBER);
    for (int i = 0; OB_SUCC(ret) && i < rowkey_ids.count(); ++i) {
      column_ids_[i] = rowkey_ids.at(i);
    }
    column_cnt_ = rowkey_ids.count();
    std::sort(column_ids_, column_ids_ + column_cnt_);//do sort, for quick compare
  }
  return ret;
}


/*
 * 对三个维度进行比较
 * 有一个维度UNCOMPARABLE， 则不能比较
 * A LEFT_DOMINATED B, 则A至少存在某个维度上比B好, 其它维度必须EQUAL
 * */
int ObIndexSkylineDim::compare(const ObIndexSkylineDim &other, ObSkylineDim::CompareStat &status) const
{
  int ret = OB_SUCCESS;
  status = ObSkylineDim::EQUAL;
  int64_t compare_result = 0;
  for (int i = 0; OB_SUCC(ret) && status != ObSkylineDim::UNCOMPARABLE && i < dim_count_; ++i) {
    const ObSkylineDim *left_dim = skyline_dims_[i];
    const ObSkylineDim *right_dim = other.skyline_dims_[i];
    ObSkylineDim::CompareStat tmp_status = ObSkylineDim::UNCOMPARABLE;
    if (OB_ISNULL(left_dim) || OB_ISNULL(right_dim)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("skeyline dimension should not be null", K(ret), K(i), K(left_dim), K(right_dim));
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
  }
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
                                          const bool has_interest_order,
                                          const bool can_extract_range,
                                          const int64_t index_column_cnt,
                                          const ObIArray<uint64_t> &filter_column_ids,
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
    dim->set_interesting_order(has_interest_order);
    dim->set_extract_range(can_extract_range);
    dim->set_index_column_cnt(index_column_cnt);
    if (!has_interest_order && !can_extract_range && is_index_back) {
      if (OB_FAIL(dim->add_filter_column_ids(filter_column_ids))) {
        LOG_WARN("failed to add restrcit_ids", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_skyline_dim(*dim))) {
        LOG_WARN("failed to add skyline dimension", K(ret));
      } else {
        LOG_TRACE("add index back dim success", K(ret), K(*dim));
      }
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
        dim->set_intereting_order(true);
        if (OB_FAIL(dim->add_interest_prefix_ids(interest_column_ids))) {
          LOG_WARN("failed to add interest prefix id", K(ret));
        } else if (OB_FAIL(dim->add_const_column_info(const_column_info))) {
          LOG_WARN("failed to add const column info", K(ret));
        }
      } else {
        dim->set_intereting_order(false);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_skyline_dim(*dim))) {
        LOG_WARN("failed to add skylined dimension", K(ret));
      } else {
        LOG_TRACE("add interesting order dim success", K(ret), K(*dim));
      }
    }
  }
  return ret;
}

int ObIndexSkylineDim::add_query_range_dim(const ObIArray<uint64_t> &prefix_range_ids,
                                           ObIAllocator &allocator)
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
      if (OB_FAIL(dim->add_rowkey_ids(prefix_range_ids))) {
        LOG_WARN("failed to add rowkey ids", K(ret));
      } else if (OB_FAIL(add_skyline_dim(*dim))) {
        LOG_WARN("failed to add_skylined_dim", K(ret));
      } else {
        LOG_TRACE("add query range dim success", K(ret), K(*dim));
      }
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
  if (!dim.can_prunning()) {
    //can't prunning, just add
    if (OB_FAIL(index_dims_.push_back(&dim))) {
      LOG_WARN("failed to push_back index dim", K(ret), K(dim));
    } else {
      has_add = true;
    }
  } else {
    if (OB_FAIL(has_dominate_dim(dim, remove_idxs, need_add))) {
      ret = OB_ERR_UNEXPECTED;
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
    }
  }
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
      LOG_WARN("push_back dominiated index failed", K(ret), K(i));
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
    } else if (OB_FAIL(index_dim->compare(dim, status))) {
      LOG_WARN("compare skyline dimension failed", K(ret), K(dim), K(i)); 
    } else if (ObSkylineDim::LEFT_DOMINATED == status) {
      if (remove_idxs.count() > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid compare, has anti_dominated index add before",
                 K(ret), K(i), K(remove_idxs), K(*index_dim), K(dim));
      } else {
        need_add = false;
        break;//not continue
      }
    } else if (ObSkylineDim::RIGHT_DOMINATED == status) {
      //record those ANIT_DOMINATED INDEXS
      if (OB_FAIL(remove_idxs.push_back(i))) {
        LOG_WARN("failed to add dominate idx", K(ret));
      }
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

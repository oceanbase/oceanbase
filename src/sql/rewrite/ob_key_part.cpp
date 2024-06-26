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

#define USING_LOG_PREFIX SQL_REWRITE
#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/rewrite/ob_key_part.h"
#include "sql/rewrite/ob_query_range.h"
#include "sql/engine/expr/ob_expr_result_type_util.h"
#include "sql/optimizer/ob_optimizer_util.h"
#include "sql/engine/expr/ob_datum_cast.h"

namespace oceanbase
{
using namespace common;

namespace sql
{
void ObKeyPart::reset()
{
  common::ObDLinkBase<ObKeyPart>::reset();
  reset_key();
  id_.table_id_ = OB_INVALID_ID;
  id_.column_id_ = OB_INVALID_ID;
  pos_.offset_ = -1;
  key_type_ = T_NORMAL_KEY;
  item_next_ = NULL;
  or_next_ = NULL;
  and_next_ = NULL;
  rowid_column_idx_ = OB_INVALID_ID;
  is_phy_rowid_key_part_ = false;
}

void ObKeyPart::reset_key()
{
  if (is_normal_key()) {
    normal_keypart_->start_.reset();
    normal_keypart_->end_.reset();
    normal_keypart_->always_true_ = false;
    normal_keypart_->always_false_ = false;
    normal_keypart_->include_start_ = false;
    normal_keypart_->include_end_ = false;
  } else if (is_like_key()) {
    like_keypart_->pattern_.reset();
    like_keypart_->escape_.reset();
  } else if (is_in_key()) {
    in_keypart_->reset();
  } else if (is_domain_key()) {
    domain_keypart_->const_param_.reset();
    domain_keypart_->extra_param_.reset();
  }
}

// can be unioned as one
// ignore the edge, k1 >= 0 or k1 < 0 can union as (min, max)
// return true if union happened, otherwise false
bool ObKeyPart::union_key(const ObKeyPart *other)
{
  bool has_union = true;
  if (OB_UNLIKELY(NULL == other)
      || OB_UNLIKELY(!is_normal_key())
      || OB_UNLIKELY(!other->is_normal_key())) {
    has_union = false;
  } else {
    if (is_question_mark()
        || other->is_question_mark()
        || NULL != item_next_
        || NULL != other->item_next_
        || is_always_false()
        || other->is_always_false()
        || (is_phy_rowid_key_part() != other->is_phy_rowid_key_part())) {
      has_union = false;
    } else {
      ObObj &s1 = normal_keypart_->start_;
      ObObj &e1 = normal_keypart_->end_;
      ObObj &s2 = other->normal_keypart_->start_;
      ObObj &e2 = other->normal_keypart_->end_;
      int cmp_s2_e1 = 0;
      int cmp_e2_s1 = 0;
      if ((cmp_s2_e1 = s2.compare(e1)) > 0 || (cmp_e2_s1 = e2.compare(s1)) < 0) {
        has_union = false;
      } else if (OB_UNLIKELY(0 == cmp_s2_e1)) {
        if (!normal_keypart_->include_end_ && !other->normal_keypart_->include_start_) {
          has_union = false;
        }
      } else if (OB_UNLIKELY(0 == cmp_e2_s1)) {
        if (!normal_keypart_->include_start_ && !other->normal_keypart_->include_end_) {
          has_union = false;
        }
      } // a < 0 or a > 0 can't be union as (MIN, MAX)
      if (has_union) {
        int cmp = s2.compare(s1);
        if (cmp < 0) {
          s1 = s2;
          normal_keypart_->include_start_ = other->normal_keypart_->include_start_;
        } else if (0 == cmp) {
          normal_keypart_->include_start_ =
              (normal_keypart_->include_start_ || other->normal_keypart_->include_start_);
          if (normal_keypart_->include_start_ && s1.is_null()) {
            null_safe_ = null_safe_ || other->null_safe_;
          }
        }
        cmp = e2.compare(e1);
        if (cmp > 0) {
          e1 = e2;
          normal_keypart_->include_end_ = other->normal_keypart_->include_end_;
        } else if (0 == cmp) {
          normal_keypart_->include_end_ =
              (normal_keypart_->include_end_ || other->normal_keypart_->include_end_);
          if (normal_keypart_->include_end_ && e1.is_null()) {
            null_safe_ = null_safe_ || other->null_safe_;
          }
        }
      }
    }
  }
  return has_union;
}

bool ObKeyPart::equal_to(const ObKeyPart *other)
{
  bool bret = true;
  if (OB_UNLIKELY(NULL == other)) {
    bret = false;
  } else {
    // 1. check item list
    ObKeyPart *item_this = this;
    const ObKeyPart *item_other = other;
    bool is_done = false;
    while (!is_done && NULL != item_this && NULL != item_other) {
      if (!item_this->key_node_is_equal(item_other)) {
        bret = false;
        is_done = true;
      } else {
        item_this = item_this->item_next_;
        item_other = item_other->item_next_;
        if (NULL == item_this || NULL == item_other) {
          // not both NULL
          if (item_this != item_other) {
            bret = false;
            is_done = true;
          }
        }
      }
    }
    // 2. check and_next_
    if (bret) {
      if (NULL != and_next_) {
        bret = and_next_->equal_to(other->and_next_);
      } else if (NULL != other->and_next_) {
        bret = false;
      }
    }
    // 3. check or_next_
    if (bret) {
      bool is_over = false;
      item_this = or_next_;
      item_other = other->or_next_;
      while (!is_over && NULL != item_this && NULL != item_other) {
        if (!item_this->equal_to(item_other)) {
          bret = false;
          is_over = true;
        } else {
          item_this = item_this->item_next_;
          item_other = item_other->item_next_;
          if (NULL == item_this || NULL == item_other) {
            // not both NULL
            if (item_this != item_other) {
              bret = false;
              is_over = true;
            }
          }
        }
      }
    }
  }
  return bret;
}

bool ObKeyPart::key_node_is_equal(const ObKeyPart *other)
{
  bool ret = true;
  if (OB_UNLIKELY(other == NULL)) {
    ret = false;
  } else if (is_normal_key() && other->is_normal_key()) {
    ret = (!is_question_mark() && !(other->is_question_mark()))
          && (is_phy_rowid_key_part_ == other->is_phy_rowid_key_part_)
          && (id_ == other->id_)
          && (pos_ == other->pos_)
          && (normal_keypart_->start_ == other->normal_keypart_->start_)
          && (normal_keypart_->end_ == other->normal_keypart_->end_)
          && (normal_keypart_->include_start_ == other->normal_keypart_->include_start_)
          && (normal_keypart_->include_end_ == other->normal_keypart_->include_end_)
          && (NULL == item_next_)
          && (NULL == other->item_next_);
  } else if (is_in_key() && other->is_in_key()) {
    if (is_question_mark() || other->is_question_mark() ||
        !in_keypart_->offsets_same_to(other->in_keypart_)) {
      ret = false;
    } else {
      // cur in key already has same param count to other if reach here
      int64_t param_cnt = in_keypart_->in_params_.count();
      for (int64_t i = 0; ret && i < param_cnt; ++i) {
        InParamMeta *left = in_keypart_->in_params_.at(i);
        InParamMeta *right = other->in_keypart_->in_params_.at(i);
        if (OB_UNLIKELY(left == NULL || right == NULL ||
                        left->pos_ != right->pos_ ||
                        left->vals_.count() != right->vals_.count())) {
          ret = false;
        } else {
          for (int64_t j = 0; ret && j < left->vals_.count(); ++j) {
            if (left->vals_.at(j) != right->vals_.at(j)) {
              ret = false;
            }
          }
        }
      }
    }
  } else { // like key is regarded as not equal
    ret = false;
  }
  return ret;
}

bool ObKeyPart::is_equal_condition() const
{
  bool bret = false;
  for (const ObKeyPart *cur_key = this; !bret && cur_key != NULL; cur_key = cur_key->item_next_) {
    if (cur_key->is_in_key()) {
      bret = true;
    } else if (cur_key->is_always_false() || cur_key->is_always_true()) {
      bret = false;
    } else if (cur_key->is_normal_key()) {
      if (!cur_key->normal_keypart_->include_end_ || !cur_key->normal_keypart_->include_start_) {
        bret = false;
      } else if (cur_key->normal_keypart_->start_.get_type() != cur_key->normal_keypart_->end_.get_type()) {
        bret = false;
      } else if (cur_key->normal_keypart_->start_.is_unknown() && cur_key->normal_keypart_->end_.is_unknown()) {
        bret = (cur_key->normal_keypart_->start_.get_unknown() == cur_key->normal_keypart_->end_.get_unknown());
      } else if (0 == cur_key->normal_keypart_->start_.compare(cur_key->normal_keypart_->end_)) {
        bret = true;
      }
    }
  }
  return bret;
}

// item next is not expected here
bool ObKeyPart::is_range_condition() const
{
  bool bret = false;
  if (is_always_false() || is_always_true() || item_next_ != NULL) {
    bret = false;
  } else if (is_normal_key() || is_in_key()) {
    bret = true;
  }
  return bret;
}

bool ObKeyPart::is_question_mark() const
{
  bool bret = false;
  if (is_like_key()) {
    bret = like_keypart_->pattern_.is_unknown() || like_keypart_->escape_.is_unknown();
  } else if (is_normal_key()) {
    bret = normal_keypart_->start_.is_unknown() || normal_keypart_->end_.is_unknown();
  } else if (is_in_key()) {
    bret = in_keypart_->contain_questionmark_;
  } else if (is_domain_key()) {
    bret = domain_keypart_->const_param_.is_unknown();
  }
  return bret;
}

bool ObKeyPart::has_intersect(const ObKeyPart *other) const
{
  bool bret = true;
  if (OB_UNLIKELY(NULL == other)
      || OB_UNLIKELY(!is_normal_key())
      || OB_UNLIKELY(!other->is_normal_key())) {
    bret = false;
  } else {
    ObObj &s1 = normal_keypart_->start_;
    ObObj &e1 = normal_keypart_->end_;
    bool s1_flag = normal_keypart_->include_start_;
    bool e1_flag = normal_keypart_->include_end_;
    ObObj &s2 = other->normal_keypart_->start_;
    bool s2_flag = other->normal_keypart_->include_start_;
    ObObj &e2 = other->normal_keypart_->end_;
    bool e2_flag = other->normal_keypart_->include_end_;
    int cmp_s2_e1 = 0;
    int cmp_e2_s1 = 0;
    if ((cmp_s2_e1 = s2.compare(e1)) > 0 || (cmp_e2_s1 = e2.compare(s1)) < 0
        || (0 == cmp_s2_e1 && (false == s2_flag || false == e1_flag))
        || (0 == cmp_e2_s1 && (false == e2_flag || false == s1_flag))) {
      bret = false;
    }
  }
  return bret;
}

int ObKeyPart::intersect(ObKeyPart *other, bool contain_row)
{
  UNUSED(contain_row);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == other)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_REWRITE_LOG(WARN, "other should not be null");
  } else if (OB_UNLIKELY(!is_normal_key() || !other->is_normal_key()) ||
             OB_UNLIKELY(pos_ != other->pos_)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_REWRITE_LOG(WARN, "ObKeyPart not equal", K(*this), K(*other));
  } else {
    //bug:
    ObObj &s1 = normal_keypart_->start_;
    ObObj &e1 = normal_keypart_->end_;
    ObObj &s2 = other->normal_keypart_->start_;
    ObObj &e2 = other->normal_keypart_->end_;
    if ((is_phy_rowid_key_part() && !other->is_phy_rowid_key_part()) ||
        (!is_phy_rowid_key_part() && other->is_phy_rowid_key_part())) {
      if (other->is_phy_rowid_key_part()) {
        normal_keypart_->start_ = other->normal_keypart_->start_;
        normal_keypart_->end_ = other->normal_keypart_->end_;
        normal_keypart_->include_start_ = other->normal_keypart_->include_start_;
        normal_keypart_->include_end_ =  other->normal_keypart_->include_end_;
        is_phy_rowid_key_part_ = true;
      } else {/*do nothing*/}
    } else if (!has_intersect(other)) {
      // update this
      normal_keypart_->start_.set_max_value();
      normal_keypart_->end_.set_min_value();
      normal_keypart_->include_start_ = false;
      normal_keypart_->include_end_ = false;
      normal_keypart_->always_false_ = true;
    } else {
      ObObj *s1 = &(normal_keypart_->start_);
      ObObj *e1 = &(normal_keypart_->end_);
      bool s1_flag = normal_keypart_->include_start_;
      bool e1_flag = normal_keypart_->include_end_;
      ObObj *s2 = &(other->normal_keypart_->start_);
      bool s2_flag = other->normal_keypart_->include_start_;
      ObObj *e2 = &(other->normal_keypart_->end_);
      bool e2_flag = other->normal_keypart_->include_end_;
      int cmp = 0;
      SQL_REWRITE_LOG(DEBUG, "has intersect", KPC(this), KPC(other));

      //取大
      cmp = s1->compare(*s2);
      if (cmp > 0) {
        // do nothing
      } else if (cmp < 0) {
        s1 = s2;
        s1_flag = s2_flag;
      } else {
        s1_flag = (s1_flag && s2_flag);
      }
      if (s1->is_null() && s1_flag) {
        null_safe_ = null_safe_ && other->null_safe_;
      }

      // 取小
      cmp = e1->compare(*e2);
      if (cmp > 0) {
        e1 = e2;
        e1_flag = e2_flag;
      } else if (cmp < 0) {
        // do nothing
      } else {
        e1_flag = (e1_flag && e2_flag);
      }
      if (e1->is_null() && e1_flag) {
        null_safe_ = null_safe_ && other->null_safe_;
      }

      // we need to set the always_true[false] flag accordingly to the intersection
      if (s1 == &normal_keypart_->start_ && e1 == &normal_keypart_->end_) {
        // do thing
      } else if (s1 == &other->normal_keypart_->start_ && e1 == &other->normal_keypart_->end_) {
        normal_keypart_->always_true_ = other->normal_keypart_->always_true_;
        normal_keypart_->always_false_ = other->normal_keypart_->always_false_;
      } else {
        normal_keypart_->always_true_ = false;
        normal_keypart_->always_false_ = false;
      }

      //set data
      if (s1 != &normal_keypart_->start_) {
        normal_keypart_->start_ = *s1;
      }
      if (e1 != &normal_keypart_->end_) {
        normal_keypart_->end_ = *e1;
      }
      normal_keypart_->include_start_ = s1_flag;
      normal_keypart_->include_end_ = e1_flag;
    }
  }
  return ret;
}

int ObKeyPart::intersect_in(ObKeyPart *other)
{
  int ret = OB_SUCCESS;
  InParamMeta *param_meta = NULL;
  ObSEArray<int64_t, 16> removed_val_idx;
  if (OB_ISNULL(other) || OB_UNLIKELY(!is_in_key()) ||
      OB_UNLIKELY(!other->is_normal_key() || other->is_phy_rowid_key_part_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(*other));
  } else if (OB_UNLIKELY(!in_keypart_->find_param(other->pos_.offset_, param_meta))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find param", K(ret), K(*other), K(*this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_meta->vals_.count(); ++i) {
      const ObObj &s1 = param_meta->vals_.at(i);
      const ObObj &e1 = param_meta->vals_.at(i);
      ObObj &s2 = other->normal_keypart_->start_;
      ObObj &e2 = other->normal_keypart_->end_;

      bool b_has_intersect = true;
      bool s2_flag = other->normal_keypart_->include_start_;
      bool e2_flag = other->normal_keypart_->include_end_;
      int cmp_s2_e1 = 0;
      int cmp_e2_s1 = 0;
      if ((cmp_s2_e1 = s2.compare(e1)) > 0 || (cmp_e2_s1 = e2.compare(s1)) < 0
          || (0 == cmp_s2_e1 && false == s2_flag)
          || (0 == cmp_e2_s1 && false == e2_flag)) {
        b_has_intersect = false;
      }
      if (!b_has_intersect && OB_FAIL(removed_val_idx.push_back(i))) {
        LOG_WARN("failed to push back false value index", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(other->convert_to_true_or_false(false))) {
      LOG_WARN("failed to convert to false key part", K(ret));
    } else if (OB_FAIL(remove_in_params_vals(removed_val_idx))) {
      LOG_WARN("failed to adjust in param values", K(ret));
    }
  }
  return ret;
}

// TODO: this func can be optimized by inner hash join algorithm
int ObKeyPart::intersect_two_in_keys(ObKeyPart *other, const ObIArray<int64_t> &common_offsets)
{
  int ret = OB_SUCCESS;
  SameValIdxMap lr_idx;
  if (OB_ISNULL(other) || OB_UNLIKELY(common_offsets.empty()) ||
     (OB_UNLIKELY(!is_in_key() || !other->is_in_key()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(other), K(common_offsets.count()));
  } else if (OB_FAIL(lr_idx.create(
            hash::cal_next_prime(in_keypart_->in_params_.at(0)->vals_.count()),   // no need to check params
            "SameValMap", "SameValMap"))) {
    LOG_WARN("failed to init hash map", K(ret));
  } else {
    bool is_valid = true;
    for (int64_t i = 0; OB_SUCC(ret) && is_valid && i < common_offsets.count(); ++i) {
      InParamMeta *left_param = NULL;
      InParamMeta *right_param = NULL;
      if (!in_keypart_->find_param(common_offsets.at(i), left_param) ||
          !other->in_keypart_->find_param(common_offsets.at(i), right_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to find param", K(ret), K(common_offsets), K(*this), K(*other));
      } else if (OB_FAIL(collect_same_val_idxs(i == 0, left_param, right_param, lr_idx))) {
        LOG_WARN("failed to collect sam value indexs", K(ret));
      } else {
        is_valid = (lr_idx.size() != 0);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (!is_valid) {
      if (OB_FAIL(convert_to_true_or_false(false))) {
        LOG_WARN("failed to convert to always false", K(ret));
      }
    } else if (OB_FAIL(merge_two_in_keys(other, lr_idx))) {
      LOG_WARN("failed to merge two in keys", K(ret));
    }
  }
  return ret;
}

int ObKeyPart::collect_same_val_idxs(const bool is_first_offset,
                                     const InParamMeta *left_param,
                                     const InParamMeta *right_param,
                                     SameValIdxMap &lr_idx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(left_param) || OB_ISNULL(right_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < left_param->vals_.count(); ++i) {
      ObSEArray<int64_t, 16> r_same_val_idx;
      for (int64_t j = 0; OB_SUCC(ret) && j < right_param->vals_.count(); ++j) {
        if (left_param->vals_.at(i).compare(right_param->vals_.at(j)) == 0) {
          if (OB_FAIL(r_same_val_idx.push_back(j))) {
            LOG_WARN("failed to push back same value idx", K(ret));
          }
        }
      }
      ObSEArray<int64_t, 16> existed_idx;
      if (OB_FAIL(ret)) {
        // do nothing
      } else if (is_first_offset && r_same_val_idx.empty()) {
        // do nothing
      } else if (is_first_offset && !r_same_val_idx.empty()) {
        if (OB_FAIL(lr_idx.set_refactored(i, r_same_val_idx))) {
          LOG_WARN("failed to set refactored", K(ret));
        }
      } else if (!is_first_offset && r_same_val_idx.empty()) {
        if (OB_FAIL(lr_idx.erase_refactored(i))) {
          if (OB_HASH_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to remove index", K(ret));
          }
        }
      } else if (OB_FAIL(lr_idx.get_refactored(i, existed_idx))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get index", K(ret));
        }
      } else {
        ObSEArray<int64_t, 16> common_idx;
        if (OB_FAIL(ObOptimizerUtil::intersect(r_same_val_idx, existed_idx, common_idx))) {
          LOG_WARN("failed to intersect index", K(ret));
        } else if (common_idx.empty()) {
          if (OB_FAIL(lr_idx.erase_refactored(i))) {
            LOG_WARN("failed to remove index", K(ret));
          }
        } else if (OB_FAIL(lr_idx.set_refactored(i, common_idx, 1))) {
          LOG_WARN("failed to set idx", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObKeyPart::merge_two_in_keys(ObKeyPart *other, const SameValIdxMap &lr_idx)
{
  int ret = OB_SUCCESS;
  ObSEArray<InParamMeta *, 4> new_params;
  if (OB_ISNULL(other) || OB_UNLIKELY(!other->is_in_key()) || OB_UNLIKELY(!is_in_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(other), K(key_type_));
  } else if (OB_FAIL(append_array_no_dup(in_keypart_->offsets_,
                                         other->in_keypart_->offsets_))) {
    LOG_WARN("failed to append right offsets", K(ret));
  } else {
    int64_t offsets_cnt = in_keypart_->offsets_.count();
    // lib::ob_sort(left_in->in_keypart_->offsets_.begin(), left_in->in_keypart_->offsets_.end());
    for (int64_t i = 0; OB_SUCC(ret) && i < offsets_cnt; ++i) {
      InParamMeta *cur_param = NULL;
      InParamMeta *new_param = NULL;
      ObSEArray<ObObj, 4> vals;
      if (OB_ISNULL(new_param = in_keypart_->create_param_meta(allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate param", K(ret));
      } else if (in_keypart_->find_param(in_keypart_->offsets_.at(i), cur_param)) {
        for (auto it = lr_idx.begin(); OB_SUCC(ret) && it != lr_idx.end(); ++it) {
          int64_t val_idx = it->first;
          int64_t copy_cnt = it->second.count();
          if (OB_UNLIKELY(val_idx < 0 || val_idx >= cur_param->vals_.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get invalid value idx", K(ret));
          } else {
            const ObObj &val = cur_param->vals_.at(val_idx);
            for (int64_t j = 0; OB_SUCC(ret) && j < copy_cnt; ++j) {
              ObObj copied_val;
              if (OB_FAIL(ob_write_obj(allocator_, val, copied_val))) {
                LOG_WARN("failed to copy obj", K(ret));
              } else if (OB_FAIL(vals.push_back(copied_val))) {
                LOG_WARN("failed to push back val", K(ret));
              }
            }
          }
        }
      } else if (other->in_keypart_->find_param(in_keypart_->offsets_.at(i), cur_param)) {
        for (auto it = lr_idx.begin(); OB_SUCC(ret) && it != lr_idx.end(); ++it) {
          int64_t val_idx = it->first;
          const ObSEArray<int64_t, 16> &r_val_idx = it->second;
          for (int64_t j = 0; OB_SUCC(ret) && j < r_val_idx.count(); ++j) {
            if (OB_FAIL(vals.push_back(cur_param->vals_.at(r_val_idx.at(j))))) {
              LOG_WARN("failed to push back val", K(ret));
            }
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("can't find param", K(ret));
      }
      if (OB_SUCC(ret)) {
        new_param->pos_ = cur_param->pos_;
        if (OB_FAIL(new_param->vals_.assign(vals))) {
          LOG_WARN("failed to assign vals", K(ret), K(vals.count()));
        } else if (OB_FAIL(new_params.push_back(new_param))) {
          LOG_WARN("failed to push back new param", K(ret));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(in_keypart_->in_params_.assign(new_params))) {
      LOG_WARN("failed to assign new params", K(ret));
    } else if (OB_FAIL(formalize_keypart(false))) {
      LOG_WARN("failed to formalize key part", K(ret));
    }
  }
  return ret;
}

/*
 * 整个链都可以表示成and，没有多余的东西
 *  (A or B) and C    general_or_next is null
 *  (A and C) or (B and C)    general_or_next is NULL or (B and C) ?
 *  (A and B) or C    general_or_next is C
 */
ObKeyPart *ObKeyPart::general_or_next()
{
  ObKeyPart *gt_or = or_next_;
  while (NULL != gt_or && gt_or->and_next_ == and_next_) {
    gt_or = gt_or->or_next_;
  }
  return gt_or;
}

ObKeyPart *ObKeyPart::cut_general_or_next()
{
  ObKeyPart *prev_or = this;
  ObKeyPart *gt_or = or_next_;
  while (NULL != gt_or && gt_or->and_next_ == and_next_) {
    prev_or = gt_or;
    gt_or = gt_or->or_next_;
  }
  prev_or->or_next_ = NULL;
  return gt_or;
}

/**
 * @brief
 * (A1 or A2 or A3) and B    (A1 and B) or (A2 and B) or (A3 and B)
 * graph:                     link gt:
 *      or    or                      or     or
 *    A1---->A2---->A3            A1---->A2---->A3
 *and |                        and | and | and |
 *    B                            B     B     B
 * @param and_next
 */
void ObKeyPart::link_gt(ObKeyPart *and_next)
{
  ObKeyPart *cur = this;
  while (NULL != cur) {
    cur->and_next_ = and_next;
    cur = cur->or_next_;
  }
}

int ObKeyPart::deep_node_copy(const ObKeyPart &other)
{
  int ret = OB_SUCCESS;
  id_ = other.id_;
  pos_ = other.pos_;
  null_safe_ = other.null_safe_;
  rowid_column_idx_ = other.rowid_column_idx_;
  is_phy_rowid_key_part_ = other.is_phy_rowid_key_part_;
  item_next_ = NULL;
  or_next_ = NULL;
  and_next_ = NULL;
  if (other.is_normal_key()) {
    if (OB_FAIL(create_normal_key())) {
      LOG_WARN("create normal key failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, other.normal_keypart_->start_, normal_keypart_->start_))) {
      LOG_WARN("deep copy start obj failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, other.normal_keypart_->end_, normal_keypart_->end_))) {
      LOG_WARN("deep copy end obj failed", K(ret));
    } else {
      normal_keypart_->always_false_ = other.normal_keypart_->always_false_;
      normal_keypart_->always_true_ = other.normal_keypart_->always_true_;
      normal_keypart_->include_start_ = other.normal_keypart_->include_start_;
      normal_keypart_->include_end_ = other.normal_keypart_->include_end_;
    }
  } else if (other.is_like_key()) {
    if (OB_FAIL(create_like_key())) {
      LOG_WARN("create like key failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, other.like_keypart_->pattern_, like_keypart_->pattern_))) {
      LOG_WARN("deep copy like pattern failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, other.like_keypart_->escape_, like_keypart_->escape_))) {
      LOG_WARN("deep copy like escape failed", K(ret));
    }
  } else if (other.is_in_key()) {
    if (OB_FAIL(create_in_key())) {
      LOG_WARN("create in key failed", K(ret));
    } else if (OB_FAIL(in_keypart_->offsets_.assign(other.in_keypart_->offsets_))) {
      LOG_WARN("failed to assign key offsets", K(ret));
    } else if (OB_FAIL(in_keypart_->missing_offsets_.assign(other.in_keypart_->missing_offsets_))) {
      LOG_WARN("failed to assign key missing offsets", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < other.in_keypart_->in_params_.count(); ++i) {
        InParamMeta *param = other.in_keypart_->in_params_.at(i);
        InParamMeta *new_param = NULL;
        if (OB_ISNULL(new_param = in_keypart_->create_param_meta(allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate param meta", K(ret));
        } else if (OB_FAIL(new_param->assign(*param, allocator_))) {
          LOG_WARN("failed to assign new param", K(ret));
        } else if (OB_FAIL(in_keypart_->in_params_.push_back(new_param))) {
          LOG_WARN("failed to push back new param", K(ret));
        }
      }
      in_keypart_->table_id_ = other.in_keypart_->table_id_;
      in_keypart_->in_type_ = other.in_keypart_->in_type_;
      in_keypart_->is_strict_in_ = other.in_keypart_->is_strict_in_;
      in_keypart_->contain_questionmark_ = other.in_keypart_->contain_questionmark_;
    }
  } else if (other.is_domain_key()) {
    if (OB_FAIL(create_domain_key())) {
      LOG_WARN("create geo key failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, other.domain_keypart_->const_param_, domain_keypart_->const_param_))) {
      LOG_WARN("deep copy geo wkb failed", K(ret));
    } else if (OB_FAIL(ob_write_obj(allocator_, other.domain_keypart_->extra_param_, domain_keypart_->extra_param_))) {
      LOG_WARN("deep copy geo distance failed", K(ret));
    } else {
      domain_keypart_->domain_op_ = other.domain_keypart_->domain_op_;
    }
  }
  return ret;
}

int ObKeyPart::shallow_node_copy(const ObKeyPart &other)
{
  int ret = OB_SUCCESS;
  reset_key();
  id_ = other.id_;
  pos_ = other.pos_;
  null_safe_ = other.null_safe_;
  rowid_column_idx_ = other.rowid_column_idx_;
  is_phy_rowid_key_part_ = other.is_phy_rowid_key_part_;
  if (other.is_normal_key()) {
    normal_keypart_ = other.normal_keypart_;
    key_type_ = other.key_type_;
  } else if (other.is_like_key()) {
    like_keypart_ = other.like_keypart_;
    key_type_ = other.key_type_;
  } else if (other.is_in_key()) {
    in_keypart_ = other.in_keypart_;
    key_type_ = other.key_type_;
  } else if (other.is_domain_key()) {
    domain_keypart_ = other.domain_keypart_;
    key_type_ = other.key_type_;
  }
  return ret;
}

int InParamMeta::assign(const InParamMeta &other, ObIAllocator &alloc)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(other.vals_.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid in keypart", K(ret));
  } else if (OB_FAIL(pos_.assign(other.pos_))) {
    LOG_WARN("failed to assign other", K(ret));
  } else if (OB_FAIL(vals_.reserve(other.vals_.count()))) {
    LOG_WARN("failed to reserve vals count", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < other.vals_.count(); ++i) {
      ObObj new_val;
      if (OB_FAIL(ob_write_obj(alloc, other.vals_.at(i), new_val))) {
        LOG_WARN("failed to copy obj", K(ret));
      } else if (OB_FAIL(vals_.push_back(new_val))) {
        LOG_WARN("failed to push back new val", K(ret));
      }
    }
  }
  return ret;
}

InParamMeta* ObInKeyPart::create_param_meta(ObIAllocator &alloc)
{
  void *ptr = NULL;
  InParamMeta *new_param = NULL;
  if (OB_NOT_NULL(ptr = alloc.alloc(sizeof(InParamMeta)))) {
    new_param = new(ptr) InParamMeta();
    new_param->vals_.set_block_allocator(ModulePageAllocator(alloc));
    new_param->pos_.enum_set_values_.set_block_allocator(ModulePageAllocator(alloc));
  }
  return new_param;
}

int64_t ObInKeyPart::get_valid_offset_cnt(int64_t max_valid_off) const
{
  int64_t valid_off_cnt = 0;
  for (int64_t i = 0; i < offsets_.count(); ++i) {
    if (offsets_.at(i) <= max_valid_off) {
      ++valid_off_cnt;
    } else {
      break;
    }
  }
  return valid_off_cnt;
}

bool ObInKeyPart::find_param(const int64_t offset, InParamMeta *&param_meta)
{
  bool found = false;
  param_meta = NULL;
  for (int64_t i = 0; !found && i < in_params_.count(); ++i) {
    if (in_params_.at(i)->pos_.offset_ == offset) {
      param_meta = in_params_.at(i);
      found = true;
    }
  }
  return found;
}

bool ObInKeyPart::offsets_same_to(const ObInKeyPart *other) const
{
  bool bret = true;
  if (OB_ISNULL(other) || offsets_.count() != other->offsets_.count()) {
    bret = false;
  } else {
    for (int64_t i = 0; i < offsets_.count(); ++i) {
      if (offsets_.at(i) != other->offsets_.at(i)) {
        bret = false;
      }
    }
  }
  return bret;
}

int ObInKeyPart::union_in_key(ObInKeyPart *other)
{
  int ret = OB_SUCCESS;
  if (!offsets_same_to(other)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only in key with same offsets can be unioned", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < in_params_.count(); ++i) {
      InParamMeta *cur_param = in_params_.at(i);
      InParamMeta *other_param = other->in_params_.at(i);
      if (OB_UNLIKELY(cur_param->pos_ != other_param->pos_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid argument", K(ret), K(*cur_param), K(*other_param));
      } else if (OB_FAIL(append(cur_param->vals_, other_param->vals_))) {
        LOG_WARN("failed to append array no dup", K(ret));
      }
    }
  }
  return ret;
}

int ObInKeyPart::get_dup_vals(int64_t offset, const ObObj &val, ObIArray<int64_t> &dup_val_idx)
{
  int ret = OB_SUCCESS;
  InParamMeta *param_meta = NULL;
  if (!find_param(offset, param_meta)) {
    // do nothing
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_meta->vals_.count(); ++i) {
      if (param_meta->vals_.at(i).compare(val) == 0) {
        ret = dup_val_idx.push_back(i);
      }
    }
  }
  return ret;
}

int ObInKeyPart::remove_in_dup_vals()
{
  int ret = OB_SUCCESS;
  int param_cnt = in_params_.count();
  int val_cnt = get_param_val_cnt();
  common::hash::ObHashSet<InParamValsWrapper> distinct_param_val_set;
  ObSEArray<InParamValsWrapper, 16> distinct_param_val_arr;
  ObSEArray<obj_cmp_func, MAX_EXTRACT_IN_COLUMN_NUMBER> cmp_funcs;
  if (OB_UNLIKELY(param_cnt == 0 || val_cnt == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid in keypart", K(ret), K(param_cnt), K(val_cnt));
  } else if (OB_FAIL(distinct_param_val_set.create(val_cnt))) {
    LOG_WARN("failed to create partition macro id set", K(ret));
  } else if (OB_FAIL(get_obj_cmp_funcs(cmp_funcs))) {
    LOG_WARN("failed to get cmp funcs", K(ret));
  }
  bool has_dup = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < val_cnt; ++i) {
    InParamValsWrapper cur_param_vals;
    for (int64_t j = 0; OB_SUCC(ret) && j < param_cnt; ++j) {
      InParamMeta *cur_param = in_params_.at(j);
      if (OB_ISNULL(cur_param) || OB_UNLIKELY(val_cnt != cur_param->vals_.count())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid argument", K(ret), K(val_cnt), K(cur_param), K(i), K(j));
      } else if (OB_FAIL(cur_param_vals.param_vals_.push_back(cur_param->vals_.at(i)))) {
        LOG_WARN("failed to push back val", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cur_param_vals.cmp_funcs_.assign(cmp_funcs))) {
      LOG_WARN("failed to assign cmp func", K(ret));
    } else if (OB_HASH_EXIST == (ret = distinct_param_val_set.set_refactored(cur_param_vals, 0))) {
      ret = OB_SUCCESS;
      has_dup = true;
    } else if (OB_UNLIKELY(OB_SUCCESS != ret)) {
      LOG_WARN("failed to set range", K(ret));
    } else if (OB_FAIL(distinct_param_val_arr.push_back(cur_param_vals))) {
      LOG_WARN("failed to push back param values", K(ret));
    }
  }
  if (OB_SUCC(ret) && has_dup) {
    for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
      in_params_.at(i)->vals_.reuse();
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < distinct_param_val_arr.count(); ++i) {
      const InParamValsWrapper &cur_param_vals = distinct_param_val_arr.at(i);
      if (OB_UNLIKELY(cur_param_vals.param_vals_.count() != param_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get invalid param cnt", K(ret), K(param_cnt), K(cur_param_vals.param_vals_.count()));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < param_cnt; ++j) {
          if (OB_ISNULL(in_params_.at(j))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get unexpected null", K(ret));
          } else if (OB_FAIL(in_params_.at(j)->vals_.push_back(cur_param_vals.param_vals_.at(j)))) {
            LOG_WARN("failed to push back val", K(ret));
          }
        }
      }
    }
  }
  LOG_TRACE("succeed to remove duplicated values from in keypart", K(has_dup), K(val_cnt), K(distinct_param_val_arr.count()));
  return ret;
}

int ObInKeyPart::get_obj_cmp_funcs(ObIArray<obj_cmp_func> &cmp_funcs)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < in_params_.count(); ++i) {
    InParamMeta *cur_param = in_params_.at(i);
    obj_cmp_func cmp_op_func = NULL;
    if (OB_ISNULL(cur_param) || OB_UNLIKELY(cur_param->vals_.count() == 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get invalid argument", K(ret), K(cur_param), K(i));
    } else {
      const ObObjTypeClass obj_tc = cur_param->vals_.at(0).get_meta().get_type_class();
      if (OB_FAIL(ObObjCmpFuncs::get_cmp_func(obj_tc, obj_tc, CO_EQ, cmp_op_func))) {
        LOG_WARN("failed to get cmp func", K(ret), K(obj_tc));
      } else {
        OB_ASSERT(cmp_op_func != NULL);
        ret = cmp_funcs.push_back(cmp_op_func);
      }
    }
  }
  return ret;
}

// TODO: can be optimized by hash join algorithm, see ObKeyPart::intersect_two_in_keys
int ObKeyPart::union_in_dup_vals(ObKeyPart *other, bool &is_unioned)
{
  int ret = OB_SUCCESS;
  InParamMeta *cur_param = NULL;
  is_unioned = false;
  if (OB_ISNULL(other) || OB_UNLIKELY(!is_in_key() || !other->is_in_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null", K(ret), K(other));
  } else if (!in_keypart_->find_param(other->in_keypart_->get_min_offset(), cur_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to find param", K(ret), K(*other), K(*this));
  } else {
    ObSEArray<int64_t, 16> dup_val_idx;
    int64_t in_param_cnt = in_keypart_->in_params_.count();
    int64_t other_in_param_cnt = other->in_keypart_->in_params_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < cur_param->vals_.count(); ++i) {
      bool is_all_same = true;
      int64_t idx = 0;
      ObSEArray<int64_t, 16> other_dup_val_idx;
      for (; OB_SUCC(ret) && is_all_same &&
             idx < in_param_cnt && idx < other_in_param_cnt; ++idx) {
        cur_param = in_keypart_->in_params_.at(idx);
        ObSEArray<int64_t, 16> tmp_other_dup_val_idx;
        ObSEArray<int64_t, 16> common_idx;
        if (OB_FAIL(other->in_keypart_->get_dup_vals(cur_param->pos_.offset_,
                                                     cur_param->vals_.at(i),
                                                     tmp_other_dup_val_idx))) {
          LOG_WARN("failed to get duplicate values", K(ret));
        } else if (tmp_other_dup_val_idx.empty()) {
          is_all_same = false;
        } else if (other_dup_val_idx.empty()) {
          ret = other_dup_val_idx.assign(tmp_other_dup_val_idx);
        } else if (OB_FAIL(ObOptimizerUtil::intersect(tmp_other_dup_val_idx,
                                                      other_dup_val_idx,
                                                      common_idx))) {
          LOG_WARN("failed to intersect index", K(ret));
        } else if (common_idx.empty()) {
          is_all_same = false;
        } else if (OB_FAIL(other_dup_val_idx.assign(common_idx))) {
          LOG_WARN("failed to set idx", K(ret));
        }
      }
      if (OB_SUCC(ret) && is_all_same) {
        is_unioned = true;
        if (idx < other_in_param_cnt) {
          // c1 in or (c1, c2) in
          if (OB_FAIL(other->remove_in_params_vals(other_dup_val_idx))) {
            LOG_WARN("failed to remove in param values", K(ret));
          } else if (other->is_always_false()) {
            break;
          }
        } else {
          // (c1, c2) in or c1 in
          // (c1, c2) in or (c1, c2) in
          ret = dup_val_idx.push_back(i);
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(remove_in_params_vals(dup_val_idx))) {
        LOG_WARN("failed to remove in param vals", K(ret));
      }
    }
  }
  return ret;
}


OB_SERIALIZE_MEMBER(ObKeyPartId, table_id_, column_id_);

OB_SERIALIZE_MEMBER(ObKeyPartPos, offset_, column_type_, enum_set_values_);

int ObKeyPartPos::set_enum_set_values(common::ObIAllocator &allocator,
                                      const common::ObIArray<common::ObString> &enum_set_values)
{
  int ret = OB_SUCCESS;
  ObString value;
  enum_set_values_.set_block_allocator(ModulePageAllocator(allocator));
  for (int64_t i = 0; OB_SUCC(ret) && i < enum_set_values.count(); ++i) {
    value.reset();
    if (OB_FAIL(ob_write_string(allocator, enum_set_values.at(i), value))) {
      LOG_WARN("fail to copy obstring", K(enum_set_values), K(value), K(ret));
    } else if (OB_FAIL(enum_set_values_.push_back(value))){
      LOG_WARN("fail to push back value", K(enum_set_values), K(value), K(ret));
    }
  }
  return ret;
}

int ObKeyPartPos::assign(const ObKeyPartPos &other)
{
  int ret = OB_SUCCESS;
  offset_ = other.offset_;
  column_type_ = other.column_type_;
  if (OB_FAIL(enum_set_values_.assign(other.enum_set_values_))) {
    LOG_WARN("failed to assign enum set values", K(ret));
  }
  return ret;
}

OB_DEF_SERIALIZE(ObKeyPart)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(id_);
  OB_UNIS_ENCODE(pos_);
  OB_UNIS_ENCODE(static_cast<int64_t>(key_type_));
  if (OB_SUCC(ret)) {
    if (is_normal_key()) {
      OB_UNIS_ENCODE(normal_keypart_->start_);
      OB_UNIS_ENCODE(normal_keypart_->include_start_);
      OB_UNIS_ENCODE(normal_keypart_->end_);
      OB_UNIS_ENCODE(normal_keypart_->include_end_);
      OB_UNIS_ENCODE(normal_keypart_->always_true_);
      OB_UNIS_ENCODE(normal_keypart_->always_false_);
    } else if (is_like_key()) {
      OB_UNIS_ENCODE(like_keypart_->pattern_);
      OB_UNIS_ENCODE(like_keypart_->escape_);
    } else if (is_in_key()) {
      int64_t param_cnt = in_keypart_->in_params_.count();
      int64_t val_cnt = in_keypart_->get_param_val_cnt();
      OB_UNIS_ENCODE(in_keypart_->table_id_);
      OB_UNIS_ENCODE(in_keypart_->in_type_);
      OB_UNIS_ENCODE(in_keypart_->contain_questionmark_);
      OB_UNIS_ENCODE(in_keypart_->is_strict_in_);
      OB_UNIS_ENCODE(in_keypart_->offsets_);
      OB_UNIS_ENCODE(in_keypart_->missing_offsets_);
      OB_UNIS_ENCODE(param_cnt);
      OB_UNIS_ENCODE(val_cnt);
      for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
        InParamMeta *param_meta = in_keypart_->in_params_.at(i);
        if (OB_ISNULL(param_meta)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else {
          OB_UNIS_ENCODE(param_meta->pos_);
          for (int64_t j = 0; OB_SUCC(ret) && j < val_cnt; ++j) {
            OB_UNIS_ENCODE(param_meta->vals_.at(j));
          }
        }
      }
    } else if (is_domain_key()) {
      OB_UNIS_ENCODE(domain_keypart_->const_param_);
      OB_UNIS_ENCODE(domain_keypart_->domain_op_);
      OB_UNIS_ENCODE(domain_keypart_->extra_param_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected key type", K_(key_type));
    }
  }
  OB_UNIS_ENCODE(null_safe_);
  OB_UNIS_ENCODE(rowid_column_idx_);
  OB_UNIS_ENCODE(is_phy_rowid_key_part_);
  return ret;
}

OB_DEF_DESERIALIZE(ObKeyPart)
{
  int ret = OB_SUCCESS;
  //要做到向前兼容，因为null_safe的范围比范围比not null safe的范围更大，对于老版本没有去filter的plan
  //宁愿range变得更大，不能接受range被缩小，所以这里将null_safe_初始化为true
  null_safe_ = true;
  OB_UNIS_DECODE(id_);
  OB_UNIS_DECODE(pos_);
  OB_UNIS_DECODE(key_type_);
  if (OB_SUCC(ret)) {
    if (T_NORMAL_KEY == key_type_) {
      if (OB_FAIL(create_normal_key())) {
        LOG_WARN("create normal key failed", K(ret));
      }
      OB_UNIS_DECODE(normal_keypart_->start_);
      OB_UNIS_DECODE(normal_keypart_->include_start_);
      OB_UNIS_DECODE(normal_keypart_->end_);
      OB_UNIS_DECODE(normal_keypart_->include_end_);
      OB_UNIS_DECODE(normal_keypart_->always_true_);
      OB_UNIS_DECODE(normal_keypart_->always_false_);
    } else if (T_LIKE_KEY == key_type_) {
      if (OB_FAIL(create_like_key())) {
        LOG_WARN("create like key failed", K(ret));
      }
      OB_UNIS_DECODE(like_keypart_->pattern_);
      OB_UNIS_DECODE(like_keypart_->escape_);
    } else if (T_IN_KEY == key_type_) {
      if (OB_FAIL(create_in_key())) {
        LOG_WARN("create in key failed", K(ret));
      }
      int64_t param_cnt = 0;
      int64_t val_cnt = 0;
      OB_UNIS_DECODE(in_keypart_->table_id_);
      OB_UNIS_DECODE(in_keypart_->in_type_);
      OB_UNIS_DECODE(in_keypart_->contain_questionmark_);
      OB_UNIS_DECODE(in_keypart_->is_strict_in_);
      OB_UNIS_DECODE(in_keypart_->offsets_);
      OB_UNIS_DECODE(in_keypart_->missing_offsets_);
      OB_UNIS_DECODE(param_cnt);
      OB_UNIS_DECODE(val_cnt);
      for (int64_t i = 0; OB_SUCC(ret) && i < param_cnt; ++i) {
        InParamMeta *param_meta = NULL;
        ObKeyPartPos key_pos;
        OB_UNIS_DECODE(key_pos);
        if (OB_ISNULL(param_meta = in_keypart_->create_param_meta(allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("callocate memory failed", K(ret));
        } else {
          param_meta->pos_ = key_pos;
          for (int64_t j = 0; OB_SUCC(ret) && j < val_cnt; ++j) {
            ObObj val;
            OB_UNIS_DECODE(val);
            if (OB_FAIL(param_meta->vals_.push_back(val))) {
              LOG_WARN("failed to push back val", K(ret));
            }
          }
          if (OB_SUCC(ret) &&
              OB_FAIL(in_keypart_->in_params_.push_back(param_meta))) {
            LOG_WARN("failed to push back param meta", K(ret));
          }
        }
      }
    } else if (T_DOMAIN_KEY == key_type_) {
      if (OB_FAIL(create_domain_key())) {
        LOG_WARN("create domain key failed", K(ret));
      }
      OB_UNIS_DECODE(domain_keypart_->const_param_);
      OB_UNIS_DECODE(domain_keypart_->domain_op_);
      OB_UNIS_DECODE(domain_keypart_->extra_param_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected key type", K_(key_type));
    }
  }
  OB_UNIS_DECODE(null_safe_);
  OB_UNIS_DECODE(rowid_column_idx_);
  OB_UNIS_DECODE(is_phy_rowid_key_part_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObKeyPart)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(id_);
  OB_UNIS_ADD_LEN(pos_);
  OB_UNIS_ADD_LEN(static_cast<int64_t>(key_type_));
  if (is_normal_key()) {
    OB_UNIS_ADD_LEN(normal_keypart_->start_);
    OB_UNIS_ADD_LEN(normal_keypart_->include_start_);
    OB_UNIS_ADD_LEN(normal_keypart_->end_);
    OB_UNIS_ADD_LEN(normal_keypart_->include_end_);
    OB_UNIS_ADD_LEN(normal_keypart_->always_true_);
    OB_UNIS_ADD_LEN(normal_keypart_->always_false_);
  } else if (is_like_key()) {
    OB_UNIS_ADD_LEN(like_keypart_->pattern_);
    OB_UNIS_ADD_LEN(like_keypart_->escape_);
  } else if (is_in_key()) {
    int64_t param_cnt = in_keypart_->in_params_.count();
    int64_t val_cnt = in_keypart_->get_param_val_cnt();
    OB_UNIS_ADD_LEN(in_keypart_->table_id_);
    OB_UNIS_ADD_LEN(in_keypart_->in_type_);
    OB_UNIS_ADD_LEN(in_keypart_->contain_questionmark_);
    OB_UNIS_ADD_LEN(in_keypart_->is_strict_in_);
    OB_UNIS_ADD_LEN(in_keypart_->offsets_);
    OB_UNIS_ADD_LEN(in_keypart_->missing_offsets_);
    OB_UNIS_ADD_LEN(param_cnt);
    OB_UNIS_ADD_LEN(val_cnt);
    for (int64_t i = 0; i < param_cnt; ++i) {
      InParamMeta *param_meta = in_keypart_->in_params_.at(i);
      if (OB_ISNULL(param_meta)) {
        // do nothing
      } else {
        OB_UNIS_ADD_LEN(param_meta->pos_);
        for (int64_t j = 0; j < val_cnt; ++j) {
          OB_UNIS_ADD_LEN(param_meta->vals_.at(j));
        }
      }
    }
  } else if (is_domain_key()) {
    OB_UNIS_ADD_LEN(domain_keypart_->const_param_);
    OB_UNIS_ADD_LEN(domain_keypart_->domain_op_);
    OB_UNIS_ADD_LEN(domain_keypart_->extra_param_);
  }
  OB_UNIS_ADD_LEN(null_safe_);
  OB_UNIS_ADD_LEN(rowid_column_idx_);
  OB_UNIS_ADD_LEN(is_phy_rowid_key_part_);
  return len;
}

int ObKeyPart::formalize_keypart(bool contain_row)
{
  int ret = OB_SUCCESS;
  if (is_always_true() || is_always_false()) {
    // do nothing
  } else if (is_normal_key()) {
    if (normal_keypart_->start_.is_min_value() && normal_keypart_->end_.is_max_value()) {
      normal_keypart_->always_true_ = true;
    } else if (!normal_keypart_->start_.can_compare(normal_keypart_->end_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("start obj can not compare with end obj",
              "start", normal_keypart_->start_, "end", normal_keypart_->end_, K(ret));
    } else {
      int cmp = normal_keypart_->start_.compare(normal_keypart_->end_);
      if ((cmp > 0) || (0 == cmp && (!normal_keypart_->include_start_ || !normal_keypart_->include_end_))) {
        if (contain_row && pos_.offset_ > 0) {
          normal_keypart_->always_false_ = false;
        } else {
          normal_keypart_->always_false_ = true;
        }
      } else {
        normal_keypart_->always_true_ = false;
        normal_keypart_->always_false_ = false;
      }
    }
  } else if (is_in_key()) {
    lib::ob_sort(in_keypart_->offsets_.begin(), in_keypart_->offsets_.end());
    lib::ob_sort(in_keypart_->in_params_.begin(),
              in_keypart_->in_params_.end(),
              [] (const InParamMeta *e1, const InParamMeta *e2) {
                return e1->pos_.offset_ <= e2->pos_.offset_;
              });
    int64_t off = -1;
    in_keypart_->is_strict_in_ = true;
    in_keypart_->missing_offsets_.reuse();
    for (int64_t i = 0; OB_SUCC(ret) && i < in_keypart_->offsets_.count(); ++i) {
      int64_t cur_off = in_keypart_->offsets_.at(i);
      if (off == -1) {
        off = cur_off;
      } else if (off == cur_off) {
        // duplicated
      } else if (cur_off != ++off) {
        in_keypart_->is_strict_in_ = false;
        while (off < cur_off) {
          ret = in_keypart_->missing_offsets_.push_back(off);
          ++off;
        }
      }
    }
    // merge duplicated key. eg: (c1, c1) in ((1,2),(2,2)) -> c1 in (2)
    if (!in_keypart_->contain_questionmark_) {
      ObSEArray<int64_t, 4> dup_param_idx;
      ObSEArray<int64_t, 4> invalid_val_idx;
      if (OB_FAIL(get_dup_param_and_vals(dup_param_idx, invalid_val_idx))) {
        LOG_WARN("failed to get duplicated param and values", K(ret));
      } else if (OB_FAIL(remove_in_params(dup_param_idx, false))) {
        LOG_WARN("failed to adjust in param", K(ret));
      } else if (OB_FAIL(remove_in_params_vals(invalid_val_idx))) {
        LOG_WARN("failed to adjust in param values", K(ret));
      } else if (OB_FAIL(remove_in_dup_vals())) {
        LOG_WARN("failed to remove duplicated values", K(ret));
      }
    }
    if (OB_SUCC(ret) && is_in_key() &&
       (in_keypart_->in_params_.empty() || in_keypart_->get_param_val_cnt() == 0)) {
      if (OB_FAIL(convert_to_true_or_false(false))) {
        LOG_WARN("failed to convert to always true");
      }
    }
  }
  return ret;
}

int ObKeyPart::remove_in_dup_vals()
{
  int ret = OB_SUCCESS;
  if (!is_in_key()) {
  } else if (OB_FAIL(in_keypart_->remove_in_dup_vals())) {
    LOG_WARN("failed to remove in dup values", K(ret));
  } else if (in_keypart_->get_param_val_cnt() == 0) {
    ret = convert_to_true_or_false(false);
  }
  return ret;
}

int ObKeyPart::get_dup_param_and_vals(ObIArray<int64_t> &dup_param_idx, ObIArray<int64_t> &invalid_val_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_in_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret));
  } else {
    int64_t i = 0;
    while (OB_SUCC(ret) && i < in_keypart_->offsets_.count() - 1) {
      InParamMeta *start_param = in_keypart_->in_params_.at(i);
      while (OB_SUCC(ret) && i < in_keypart_->offsets_.count() - 1 &&
             in_keypart_->offsets_.at(i) == in_keypart_->offsets_.at(i + 1)) {
        InParamMeta *next_param = in_keypart_->in_params_.at(i + 1);
        if (OB_ISNULL(start_param) || OB_ISNULL(next_param)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected null", K(ret));
        } else if (OB_FAIL(dup_param_idx.push_back(i + 1))) {
          LOG_WARN("failed to push back removed param idx", K(ret));
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < start_param->vals_.count(); ++j) {
            if (OB_UNLIKELY(start_param->vals_.count() != next_param->vals_.count())) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("values count must be the same", K(ret), K(*start_param), K(*next_param));
            } else if (!is_contain(invalid_val_idx, j) &&
                next_param->vals_.at(j) != start_param->vals_.at(j)) {
              ret = invalid_val_idx.push_back(j);
            }
          }
        }
        ++i;
      }
      ++i;
    }
  }
  return ret;
}

int ObKeyPart::remove_in_params(const ObIArray<int64_t> &invalid_param_idx, bool always_true)
{
  int ret = OB_SUCCESS;
  if (is_always_true() || is_always_false() || invalid_param_idx.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(!is_in_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else {
    ObSEArray<int64_t, 4> new_offsets;
    ObSEArray<InParamMeta *, 4> new_params;
    for (int64_t i = 0; OB_SUCC(ret) && i < in_keypart_->offsets_.count(); ++i) {
      int64_t cur_offset = in_keypart_->offsets_.at(i);
      InParamMeta *cur_param = in_keypart_->in_params_.at(i);
      if (is_contain(invalid_param_idx, i)) {
        // invalid, do nothing
      } else if (OB_FAIL(new_offsets.push_back(cur_offset))) {
        LOG_WARN("failed to push back offset", K(ret));
      } else if (OB_FAIL(new_params.push_back(cur_param))) {
        LOG_WARN("failed to push back new param", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(new_offsets.empty())) {
      if (OB_FAIL(convert_to_true_or_false(always_true))) {
        LOG_WARN("failed to convert to always false", K(ret));
      }
    } else if (OB_FAIL(in_keypart_->offsets_.assign(new_offsets))) {
      LOG_WARN("failed to assign new offsets", K(ret));
    } else if (OB_FAIL(in_keypart_->in_params_.assign(new_params))) {
      LOG_WARN("failed to assign new params", K(ret));
    }
  }
  return ret;
}

int ObKeyPart::remove_in_params_vals(const ObIArray<int64_t> &val_idx)
{
  int ret = OB_SUCCESS;
  if (is_always_true() || is_always_false() || val_idx.empty()) {
    // do nothing
  } else if (OB_UNLIKELY(!is_in_key())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get unexpected null", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < in_keypart_->in_params_.count(); ++i) {
      InParamMeta *param_meta = in_keypart_->in_params_.at(i);
      ObSEArray<ObObj, 128> new_vals;
      for (int64_t j = 0; OB_SUCC(ret) && j < param_meta->vals_.count(); ++j) {
        if (!is_contain(val_idx, j) &&
            OB_FAIL(new_vals.push_back(param_meta->vals_.at(j)))) {
          LOG_WARN("failed to push back val", K(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY(new_vals.empty())) {
        if (OB_FAIL(convert_to_true_or_false(false))) {
          LOG_WARN("failed to convert to always false", K(ret));
        } else {
          break;
        }
      } else if (OB_FAIL(param_meta->vals_.assign(new_vals))) {
        LOG_WARN("failed to assign new values", K(ret));
      }
    }
  }
  return ret;
}

int ObKeyPart::cast_value_type(const ObDataTypeCastParams &dtc_params, const int64_t cur_datetime,
                               bool contain_row, bool &is_bound_modified)
{
  int ret = OB_SUCCESS;
  int64_t start_cmp = 0;
  int64_t end_cmp = 0;
  if (OB_UNLIKELY(!is_normal_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("keypart isn't normal key", K_(key_type));
  } else if (OB_FAIL(ObKeyPart::try_cast_value(dtc_params, cur_datetime, allocator_, pos_,
                                               normal_keypart_->start_, start_cmp, common::CO_LE, true))) {
    LOG_WARN("failed to try cast value type", K(ret));
  } else if (OB_FAIL(ObKeyPart::try_cast_value(dtc_params, cur_datetime, allocator_, pos_,
                                               normal_keypart_->end_, end_cmp, common::CO_LE, false))) {
    LOG_WARN("failed to try cast value type", K(ret));
  } else {
    if (start_cmp < 0) {
      // after cast, precise becomes bigger, ( -> [
      normal_keypart_->include_start_ = true;
      is_bound_modified = true;
    } else if (start_cmp > 0) {
      // after cast, the result becomes smaller, [ -> (
      normal_keypart_->include_start_ = false;
      is_bound_modified = true;
    }
    if (end_cmp < 0) {
      // after cast, the result becomes bigger, ] -> )
      normal_keypart_->include_end_ = false;
      is_bound_modified = true;
    } else if (end_cmp > 0) {
      // after cast, the result becomes smaller, ) -> ]
      normal_keypart_->include_end_ = true;
      is_bound_modified = true;
    }
    normal_keypart_->start_.set_collation_type(pos_.column_type_.get_collation_type());
    normal_keypart_->end_.set_collation_type(pos_.column_type_.get_collation_type());
    if (OB_FAIL(formalize_keypart(contain_row))) {
      LOG_WARN("formalize keypart failed", K(ret));
    }
  }
  return ret;
}

int ObKeyPart::try_cast_value(const ObDataTypeCastParams &dtc_params, const int64_t cur_datetime,
                              ObIAllocator &alloc, const ObKeyPartPos &pos, ObObj &value, int64_t &cmp,
                              common::ObCmpOp cmp_op /* CO_EQ */, bool left_border /*true*/)
{
  int ret = OB_SUCCESS;
  if (!value.is_min_value() && !value.is_max_value() && !value.is_unknown()
      && (!ObSQLUtils::is_same_type_for_compare(value.get_meta(), pos.column_type_.get_obj_meta())
          || value.is_decimal_int())) {
    const ObObj *dest_val = NULL;
    ObCollationType collation_type = pos.column_type_.get_collation_type();
    ObCastCtx cast_ctx(&alloc, &dtc_params, cur_datetime, CM_WARN_ON_FAIL, collation_type);
    ObAccuracy acc(pos.column_type_.get_accuracy());
    if (pos.column_type_.is_decimal_int()) {
      cast_ctx.res_accuracy_ = &acc;
      int32_t out_bytes = wide::ObDecimalIntConstValue::get_int_bytes_by_precision(acc.get_precision());
      int32_t in_bytes = value.get_int_bytes();
      ObScale out_scale = acc.get_scale();
      ObScale in_scale = value.get_scale();
      if ((value.is_decimal_int()
            && ObDatumCast::need_scale_decimalint(in_scale, in_bytes, out_scale, out_bytes))
          || !value.is_decimal_int()) {
        cast_ctx.cast_mode_ = ObRelationalExprOperator::get_const_cast_mode(cmp_op, !left_border);
      }
    }
    ObObj &tmp_start = value;
    ObExpectType expect_type;
    expect_type.set_type(pos.column_type_.get_type());
    expect_type.set_collation_type(collation_type);
    expect_type.set_type_infos(&pos.get_enum_set_values());
    EXPR_CAST_OBJ_V2(expect_type, tmp_start, dest_val);
    // to check if EXPR CAST losses number precise
    ObObjType cmp_type = ObMaxType;
    if (OB_FAIL(ret)) {
      SQL_REWRITE_LOG(WARN, "cast obj to dest type failed", K(ret), K(value), K_(pos.column_type));
    } else if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(cmp_type,
                                                                     value.get_type(),
                                                                     dest_val->get_type()))) {
      LOG_WARN("get compare type failed", K(ret));
    } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(cmp,
                                                                  value,
                                                                  *dest_val,
                                                                  cast_ctx,
                                                                  cmp_type,
                                                                  collation_type))) {
      SQL_REWRITE_LOG(WARN, "compare obj value failed", K(ret));
    } else {
      value = *dest_val;
    }
  }
  return ret;
}

int ObKeyPart::create_normal_key()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObNormalKeyPart)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    key_type_ = T_NORMAL_KEY;
    normal_keypart_ = new(ptr) ObNormalKeyPart();
  }
  return ret;
}

int ObKeyPart::create_like_key()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObLikeKeyPart)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    key_type_ = T_LIKE_KEY;
    like_keypart_ = new(ptr) ObLikeKeyPart();
  }
  return ret;
}

int ObKeyPart::create_in_key()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObInKeyPart)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else {
    key_type_ = T_IN_KEY;
    in_keypart_ = new(ptr) ObInKeyPart();
    in_keypart_->in_type_ = T_IN_KEY_PART;
  }
  return ret;
}

// to be implemented
int ObKeyPart::create_not_in_key()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_.alloc(sizeof(ObInKeyPart)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed", K(ret));
  } else {
    key_type_ = T_IN_KEY;
    in_keypart_ = new(ptr) ObInKeyPart();
    in_keypart_->in_type_ = T_NOT_IN_KEY_PART;
  }
  return ret;
}

int ObKeyPart::create_domain_key()
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_UNLIKELY(NULL == (ptr = allocator_.alloc(sizeof(ObDomainKeyPart))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    key_type_ = T_DOMAIN_KEY;
    domain_keypart_ = new(ptr) ObDomainKeyPart();
  }
  return ret;
}

int ObKeyPart::convert_to_true_or_false(bool is_always_true)
{
  int ret = OB_SUCCESS;
  if (is_in_key()) {
    // set the first key part
    ObKeyPartPos pos = in_keypart_->in_params_.at(0)->pos_;
    if (OB_FAIL(create_normal_key())) {
      LOG_WARN("failed to create normal key", K(ret));
    } else {
      pos_ = pos;
      id_.table_id_ = in_keypart_->table_id_;
    }
  }
  if (OB_SUCC(ret)) {
    if (is_always_true) {
      normal_keypart_->start_.set_min_value();
      normal_keypart_->end_.set_max_value();
    } else {
      normal_keypart_->start_.set_max_value();
      normal_keypart_->end_.set_min_value();
    }
    normal_keypart_->include_start_ = false;
    normal_keypart_->include_end_ = false;
    normal_keypart_->always_true_ = is_always_true;
    normal_keypart_->always_false_ = !is_always_true;
  }
  return ret;
}

DEF_TO_STRING(ObKeyPart)
{
  int64_t pos = 0;
  J_OBJ_START();
  if (!is_in_key()) {
    J_KV(N_INDEX_ID, id_,
         N_POS, pos_,
         K_(key_type),
         K_(null_safe),
         K_(rowid_column_idx),
         K_(is_phy_rowid_key_part));
  } else {
    J_KV(K_(key_type));
  }
  if (is_normal_key()) {
    J_COMMA();
    J_KV(N_START_VAL, normal_keypart_->start_,
         N_END_VAL, normal_keypart_->end_,
         N_INCLUDE_START, normal_keypart_->include_start_,
         N_INCLUDE_END, normal_keypart_->include_end_,
         N_ALWAYS_TRUE, normal_keypart_->always_true_,
         N_ALWAYS_FALSE, normal_keypart_->always_false_);
  } else if (is_like_key()) {
    J_COMMA();
    J_KV(N_PATTERN_VAL, like_keypart_->pattern_,
         N_ESCAPE_VAL, like_keypart_->escape_);
  } else if (is_in_key()) {
    J_COMMA();
    J_KV("table_id_", in_keypart_->table_id_,
         N_IS_STRICT_IN, in_keypart_->is_strict_in_,
         N_CONTAIN_QUESTIONMARK, in_keypart_->contain_questionmark_,
         N_OFFSETS, in_keypart_->offsets_,
         N_MISSING_OFFSETS, in_keypart_->missing_offsets_,
         N_IN_PARAMS, in_keypart_->in_params_);
  } else if (is_domain_key()) {
    J_COMMA();
    J_KV("const_param_", domain_keypart_->const_param_,
         "domain_type_", domain_keypart_->domain_op_,
         "extra_param_", domain_keypart_->extra_param_);
  }
  J_OBJ_END();
  return pos;
}
} // namespace sql
} // namespace oceanbase

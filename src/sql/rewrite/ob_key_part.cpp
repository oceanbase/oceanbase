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

namespace oceanbase {
using namespace common;

namespace sql {
void ObKeyPart::reset()
{
  common::ObDLinkBase<ObKeyPart>::reset();
  id_.table_id_ = OB_INVALID_ID;
  id_.column_id_ = OB_INVALID_ID;
  pos_.offset_ = -1;
  key_type_ = T_NORMAL_KEY;
  item_next_ = NULL;
  or_next_ = NULL;
  and_next_ = NULL;
}

bool ObKeyPart::has_intersect(const ObKeyPart* other) const
{
  bool bret = true;
  if (OB_UNLIKELY(NULL == other) || OB_UNLIKELY(!is_normal_key()) || OB_UNLIKELY(!other->is_normal_key())) {
    bret = false;
  } else {
    ObObj& s1 = normal_keypart_->start_;
    ObObj& e1 = normal_keypart_->end_;
    bool s1_flag = normal_keypart_->include_start_;
    bool e1_flag = normal_keypart_->include_end_;
    ObObj& s2 = other->normal_keypart_->start_;
    bool s2_flag = other->normal_keypart_->include_start_;
    ObObj& e2 = other->normal_keypart_->end_;
    bool e2_flag = other->normal_keypart_->include_end_;
    int cmp_s2_e1 = 0;
    int cmp_e2_s1 = 0;
    if ((cmp_s2_e1 = s2.compare(e1)) > 0 || (cmp_e2_s1 = e2.compare(s1)) < 0 ||
        (0 == cmp_s2_e1 && (false == s2_flag || false == e1_flag)) ||
        (0 == cmp_e2_s1 && (false == e2_flag || false == s1_flag))) {
      bret = false;
    }
  }
  return bret;
}

// can be unioned as one
// ignore the edge, k1 >= 0 or k1 < 0 can union as (min, max)
bool ObKeyPart::can_union(const ObKeyPart* other) const
{
  bool bret = true;
  if (OB_UNLIKELY(NULL == other) || OB_UNLIKELY(!is_normal_key()) || OB_UNLIKELY(!other->is_normal_key())) {
    bret = false;
  } else {
    if (is_question_mark() || other->is_question_mark() || NULL != item_next_ || NULL != other->item_next_ ||
        is_always_false() || other->is_always_false()) {
      bret = false;
    } else {
      ObObj& s1 = normal_keypart_->start_;
      ObObj& e1 = normal_keypart_->end_;
      ObObj& s2 = other->normal_keypart_->start_;
      ObObj& e2 = other->normal_keypart_->end_;
      int cmp_s2_e1 = 0;
      int cmp_e2_s1 = 0;
      if ((cmp_s2_e1 = s2.compare(e1)) > 0 || (cmp_e2_s1 = e2.compare(s1)) < 0) {
        bret = false;
      } else if (OB_UNLIKELY(0 == cmp_s2_e1)) {
        if (!normal_keypart_->include_end_ && !other->normal_keypart_->include_start_) {
          bret = false;
        }
      } else if (OB_UNLIKELY(0 == cmp_e2_s1)) {
        if (!normal_keypart_->include_start_ && !other->normal_keypart_->include_end_) {
          bret = false;
        }
      }  // a < 0 or a > 0 can't be union as (MIN, MAX)
    }
  }
  return bret;
}

bool ObKeyPart::equal_to(const ObKeyPart* other)
{
  bool bret = true;
  if (OB_UNLIKELY(NULL == other)) {
    bret = false;
  } else {
    // 1. check item list
    ObKeyPart* item_this = this;
    const ObKeyPart* item_other = other;
    bool is_done = false;
    while (!is_done && NULL != item_this && NULL != item_other) {
      if (!item_this->normal_key_is_equal(item_other)) {
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

int ObKeyPart::intersect(ObKeyPart* other, bool contain_row)
{
  UNUSED(contain_row);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(NULL == other)) {
    ret = OB_INVALID_ARGUMENT;
    SQL_REWRITE_LOG(WARN, "other should not be null");
  } else if (OB_UNLIKELY((id_ != other->id_) || (pos_ != other->pos_)) || OB_UNLIKELY(!is_normal_key()) ||
             OB_UNLIKELY(!other->is_normal_key())) {
    ret = OB_INVALID_ARGUMENT;
    SQL_REWRITE_LOG(WARN, "ObKeyPart not equal", K(*this), K(*other));
  } else {
    if (!has_intersect(other)) {
      // update this
      normal_keypart_->start_.set_max_value();
      normal_keypart_->end_.set_min_value();
      normal_keypart_->include_start_ = false;
      normal_keypart_->include_end_ = false;
      normal_keypart_->always_false_ = true;
    } else {
      ObObj* s1 = &(normal_keypart_->start_);
      ObObj* e1 = &(normal_keypart_->end_);
      bool s1_flag = normal_keypart_->include_start_;
      bool e1_flag = normal_keypart_->include_end_;
      ObObj* s2 = &(other->normal_keypart_->start_);
      bool s2_flag = other->normal_keypart_->include_start_;
      ObObj* e2 = &(other->normal_keypart_->end_);
      bool e2_flag = other->normal_keypart_->include_end_;
      int cmp = 0;
      SQL_REWRITE_LOG(DEBUG, "has intersect");

      cmp = s1->compare(*s2);
      if (cmp > 0) {
        // do nothing
      } else if (cmp < 0) {
        s1 = s2;
        s1_flag = s2_flag;
      } else {
        s1_flag = (s1_flag && s2_flag);
      }

      cmp = e1->compare(*e2);
      if (cmp > 0) {
        e1 = e2;
        e1_flag = e2_flag;
      } else if (cmp < 0) {
        // do nothing
      } else {
        e1_flag = (e1_flag && e2_flag);
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

      // set data
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

ObKeyPart* ObKeyPart::general_or_next()
{
  ObKeyPart* gt_or = or_next_;
  while (NULL != gt_or && gt_or->and_next_ == and_next_) {
    gt_or = gt_or->or_next_;
  }
  return gt_or;
}

ObKeyPart* ObKeyPart::cut_general_or_next()
{
  ObKeyPart* prev_or = this;
  ObKeyPart* gt_or = or_next_;
  while (NULL != gt_or && gt_or->and_next_ == and_next_) {
    prev_or = gt_or;
    gt_or = gt_or->or_next_;
  }
  prev_or->or_next_ = NULL;
  return gt_or;
}

void ObKeyPart::link_gt(ObKeyPart* and_next)
{
  ObKeyPart* cur = this;
  while (NULL != cur) {
    cur->and_next_ = and_next;
    cur = cur->or_next_;
  }
}

int ObKeyPart::deep_node_copy(const ObKeyPart& other)
{
  int ret = OB_SUCCESS;
  id_ = other.id_;
  pos_ = other.pos_;
  null_safe_ = other.null_safe_;
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
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObKeyPartId, table_id_, column_id_);

OB_SERIALIZE_MEMBER(ObKeyPartPos, offset_, column_type_, enum_set_values_);

int ObKeyPartPos::set_enum_set_values(
    common::ObIAllocator& allocator, const common::ObIArray<common::ObString>& enum_set_values)
{
  int ret = OB_SUCCESS;
  ObString value;
  for (int64_t i = 0; OB_SUCC(ret) && i < enum_set_values.count(); ++i) {
    value.reset();
    if (OB_FAIL(ob_write_string(allocator, enum_set_values.at(i), value))) {
      LOG_WARN("fail to copy obstring", K(enum_set_values), K(value), K(ret));
    } else if (OB_FAIL(enum_set_values_.push_back(value))) {
      LOG_WARN("fail to push back value", K(enum_set_values), K(value), K(ret));
    }
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
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected key type", K_(key_type));
    }
  }
  OB_UNIS_ENCODE(null_safe_);
  return ret;
}

OB_DEF_DESERIALIZE(ObKeyPart)
{
  int ret = OB_SUCCESS;
  int64_t key_type = 0;
  null_safe_ = true;
  OB_UNIS_DECODE(id_);
  OB_UNIS_DECODE(pos_);
  OB_UNIS_DECODE(key_type);
  key_type_ = static_cast<ObKeyPartType>(key_type);
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
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected key type", K_(key_type));
    }
  }
  OB_UNIS_DECODE(null_safe_);
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
  }
  OB_UNIS_ADD_LEN(null_safe_);
  return len;
}

int ObKeyPart::formalize_keypart(bool contain_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_normal_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root isn't normal key");
  } else if (normal_keypart_->start_.is_min_value() && normal_keypart_->end_.is_max_value()) {
    normal_keypart_->always_true_ = true;
  } else if (!normal_keypart_->start_.can_compare(normal_keypart_->end_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start obj can not compare with end obj",
        "start",
        normal_keypart_->start_,
        "end",
        normal_keypart_->end_,
        K(ret));
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
  return ret;
}

int ObKeyPart::cast_value_type(const ObDataTypeCastParams& dtc_params, bool contain_row)
{
  int ret = OB_SUCCESS;
  ObObj cast_obj;
  const ObObj* dest_val = NULL;

  if (OB_UNLIKELY(!is_normal_key())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("keypart isn't normal key", K_(key_type));
  } else if (!normal_keypart_->start_.is_min_value() && !normal_keypart_->start_.is_max_value() &&
             !normal_keypart_->start_.is_unknown() &&
             !ObSQLUtils::is_same_type_for_compare(
                 normal_keypart_->start_.get_meta(), pos_.column_type_.get_obj_meta())) {
    ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, pos_.column_type_.get_collation_type());
    ObObj& tmp_start = normal_keypart_->start_;
    ObExpectType expect_type;
    expect_type.set_type(pos_.column_type_.get_type());
    expect_type.set_collation_type(pos_.column_type_.get_collation_type());
    expect_type.set_type_infos(&pos_.get_enum_set_values());
    EXPR_CAST_OBJ_V2(expect_type, tmp_start, dest_val);
    if (OB_FAIL(ret)) {
      SQL_REWRITE_LOG(WARN, "cast obj to dest type failed", K_(normal_keypart_->start), K_(pos_.column_type));
    } else {
      int64_t cmp = 0;
      ObObjType cmp_type = ObMaxType;
      if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(
              cmp_type, normal_keypart_->start_.get_type(), dest_val->get_type()))) {
        LOG_WARN("get compare type failed", K(ret));
      } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(cmp,
                     normal_keypart_->start_,
                     *dest_val,
                     cast_ctx,
                     cmp_type,
                     pos_.column_type_.get_collation_type()))) {
        SQL_REWRITE_LOG(WARN, "compare obj value failed", K(ret));
      } else if (cmp < 0) {
        normal_keypart_->include_start_ = true;
      } else if (cmp > 0) {
        normal_keypart_->include_start_ = false;
      }
      normal_keypart_->start_ = *dest_val;
    }
  }

  if (OB_SUCC(ret)) {
    if (!normal_keypart_->end_.is_min_value() && !normal_keypart_->end_.is_max_value() &&
        !normal_keypart_->end_.is_unknown() &&
        !ObSQLUtils::is_same_type_for_compare(normal_keypart_->end_.get_meta(), pos_.column_type_.get_obj_meta())) {
      ObCastCtx cast_ctx(&allocator_, &dtc_params, CM_WARN_ON_FAIL, pos_.column_type_.get_collation_type());
      ObExpectType expect_type;
      expect_type.set_type(pos_.column_type_.get_type());
      expect_type.set_collation_type(pos_.column_type_.get_collation_type());
      expect_type.set_type_infos(&pos_.get_enum_set_values());
      ObObj& tmp_end = normal_keypart_->end_;
      EXPR_CAST_OBJ_V2(expect_type, tmp_end, dest_val);
      if (OB_FAIL(ret)) {
        SQL_REWRITE_LOG(WARN, "cast obj to dest type failed", K_(normal_keypart_->end), K_(pos_.column_type));
      } else {
        int64_t cmp = 0;
        ObObjType cmp_type = ObMaxType;
        if (OB_FAIL(ObExprResultTypeUtil::get_relational_cmp_type(
                cmp_type, normal_keypart_->end_.get_type(), dest_val->get_type()))) {
          LOG_WARN("get compare type failed", K(ret));
        } else if (OB_FAIL(ObRelationalExprOperator::compare_nullsafe(cmp,
                       normal_keypart_->end_,
                       *dest_val,
                       cast_ctx,
                       cmp_type,
                       pos_.column_type_.get_collation_type()))) {
          SQL_REWRITE_LOG(WARN, "compare obj value failed", K(ret));
        } else if (cmp > 0) {
          normal_keypart_->include_end_ = true;
        } else if (cmp < 0) {
          normal_keypart_->include_end_ = false;
        }
        normal_keypart_->end_ = *dest_val;
      }
    }
  }

  if (OB_SUCC(ret)) {
    normal_keypart_->start_.set_collation_type(pos_.column_type_.get_collation_type());
    normal_keypart_->end_.set_collation_type(pos_.column_type_.get_collation_type());
  }
  if (OB_SUCC(ret) && OB_FAIL(formalize_keypart(contain_row))) {
    LOG_WARN("formalize keypart failed", K(ret));
  }
  return ret;
}

int ObKeyPart::create_normal_key()
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_UNLIKELY(NULL == (ptr = allocator_.alloc(sizeof(ObNormalKeyPart))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    key_type_ = T_NORMAL_KEY;
    normal_keypart_ = new (ptr) ObNormalKeyPart();
  }
  return ret;
}

int ObKeyPart::create_like_key()
{
  int ret = OB_SUCCESS;
  void* ptr = NULL;
  if (OB_UNLIKELY(NULL == (ptr = allocator_.alloc(sizeof(ObLikeKeyPart))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("alloc memory failed");
  } else {
    key_type_ = T_LIKE_KEY;
    like_keypart_ = new (ptr) ObLikeKeyPart();
  }
  return ret;
}

DEF_TO_STRING(ObKeyPart)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(N_INDEX_ID, id_, N_POS, pos_, K_(key_type), K_(null_safe));
  if (is_normal_key()) {
    J_COMMA();
    J_KV(N_START_VAL,
        normal_keypart_->start_,
        N_END_VAL,
        normal_keypart_->end_,
        N_INCLUDE_START,
        normal_keypart_->include_start_,
        N_INCLUDE_END,
        normal_keypart_->include_end_,
        N_ALWAYS_TRUE,
        normal_keypart_->always_true_,
        N_ALWAYS_FALSE,
        normal_keypart_->always_false_);
  } else if (is_like_key()) {
    J_COMMA();
    J_KV(N_PATTERN_VAL, like_keypart_->pattern_, N_ESCAPE_VAL, like_keypart_->escape_);
  }
  J_OBJ_END();
  return pos;
}
}  // namespace sql
}  // namespace oceanbase

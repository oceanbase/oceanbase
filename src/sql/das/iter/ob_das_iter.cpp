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


#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/iter/ob_das_iter.h"


namespace oceanbase
{
using namespace common;
namespace sql
{

int ObDASIter::set_merge_status(MergeType merge_type)
{
  int ret = OB_SUCCESS;
  ObDASIter *child = child_;
  for (; child != nullptr && OB_SUCC(ret); child = child->right_) {
    if (OB_FAIL(child->set_merge_status(merge_type))) {
      LOG_WARN("failed to set merge status", K(ret));
    }
  }
  return ret;
}

int ObDASIter::init(ObDASIterParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("das iter init twice", K(ret));
  } else if (!param.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid das iter param", K(param), K(ret));
  } else {
    inited_ = true;
    type_ = param.type_;
    max_size_ = param.max_size_;
    eval_ctx_ = param.eval_ctx_;
    exec_ctx_ = param.exec_ctx_;
    output_ = param.output_;
    group_id_expr_ = param.group_id_expr_;
    child_ = param.child_;
    right_ = param.right_;
    if (OB_FAIL(inner_init(param))) {
      LOG_WARN("failed to inner init das iter", K(param), K(ret));
    }
  }

  return ret;
}

int ObDASIter::reuse()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("reuse das iter before init", K(ret));
  } else if (OB_FAIL(inner_reuse())) {
    LOG_WARN("failed to inner reuse das iter", K(ret), KPC(this));
  }
  return ret;
}

int ObDASIter::release()
{
  int ret = OB_SUCCESS;
  int child_ret = OB_SUCCESS;
  ObDASIter *child = child_;
  int tmp_ret = OB_SUCCESS;
  while (child != nullptr) {
    ObDASIter *right = child->right_;
    if (OB_TMP_FAIL(child->release())) {
      LOG_WARN("failed to release child iter", K(tmp_ret), KPC(child));
      child_ret = tmp_ret;
    }
    child = right;
  }
  if (OB_FAIL(inner_release())) {
    LOG_WARN("failed to inner release das iter", K(ret), KPC(this));
  } else {
    ret = child_ret;
  }
  inited_ = false;
  right_ = nullptr;
  child_ = nullptr;
  group_id_expr_ = nullptr;
  output_ = nullptr;
  exec_ctx_ = nullptr;
  eval_ctx_ = nullptr;
  max_size_ = 0;
  type_ = ObDASIterType::DAS_ITER_INVALID;
  return ret;
}

int ObDASIter::get_next_row()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das iter get next row before init", K(ret));
  } else {
    ret = inner_get_next_row();
  }
  return ret;
}


int ObDASIter::get_next_rows(int64_t &count, int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("das iter get next rows before init", K(ret));
  } else {
    ret = inner_get_next_rows(count, capacity);
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

/**
 * Copyright (c) 2022 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 *
 * ob_das_attach_define.cpp
 *
 *      Author: yuming<>
 */
#define USING_LOG_PREFIX SQL_DAS
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/das/ob_das_factory.h"
namespace oceanbase {
namespace sql {

OB_SERIALIZE_MEMBER(ObDASAttachCtDef,
                    result_output_);

OB_SERIALIZE_MEMBER(ObDASAttachRtDef);

OB_SERIALIZE_MEMBER((ObDASTableLookupCtDef, ObDASAttachCtDef));

const ObDASScanCtDef *ObDASTableLookupCtDef::get_lookup_scan_ctdef() const
{
  const ObDASScanCtDef *scan_ctdef = nullptr;
  OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
  if (DAS_OP_TABLE_SCAN == children_[1]->op_type_) {
    scan_ctdef = static_cast<const ObDASScanCtDef*>(children_[1]);
  }
  return scan_ctdef;
}

OB_SERIALIZE_MEMBER((ObDASTableLookupRtDef, ObDASAttachRtDef));

ObDASScanRtDef *ObDASTableLookupRtDef::get_lookup_scan_rtdef()
{
  ObDASScanRtDef *scan_rtdef = nullptr;
  OB_ASSERT(2 == children_cnt_ && children_ != nullptr);
  if (DAS_OP_TABLE_SCAN == children_[1]->op_type_) {
    scan_rtdef = static_cast<ObDASScanRtDef*>(children_[1]);
  }
  return scan_rtdef;
}

OB_SERIALIZE_MEMBER((ObDASSortCtDef, ObDASAttachCtDef),
                    sort_exprs_,
                    sort_collations_,
                    sort_cmp_funcs_,
                    limit_expr_,
                    offset_expr_,
                    fetch_with_ties_);

OB_SERIALIZE_MEMBER((ObDASSortRtDef, ObDASAttachRtDef));

OB_DEF_SERIALIZE(ObDASAttachSpec)
{
  int ret = OB_SUCCESS;
  bool has_attach_ctdef = attach_ctdef_ != nullptr;
  OB_UNIS_ENCODE(has_attach_ctdef);
  if (has_attach_ctdef) {
    OB_UNIS_ENCODE(attach_loc_metas_.size());
    FOREACH_X(it, attach_loc_metas_, OB_SUCC(ret)) {
      const ObDASTableLocMeta *loc_meta = *it;
      OB_UNIS_ENCODE(*loc_meta);
    }
    OZ(serialize_ctdef_tree(buf, buf_len, pos, attach_ctdef_));
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObDASAttachSpec)
{
  int ret = OB_SUCCESS;
  bool has_attach_ctdef = false;
  OB_UNIS_DECODE(has_attach_ctdef);
  if (OB_SUCC(ret) && has_attach_ctdef) {
    int64_t list_size = 0;
    OB_UNIS_DECODE(list_size);
    for (int i = 0; OB_SUCC(ret) && i < list_size; ++i) {
      ObDASTableLocMeta *loc_meta = OB_NEWx(ObDASTableLocMeta, &allocator_, allocator_);
      if (OB_ISNULL(loc_meta)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate table location meta failed", K(ret));
      } else if (OB_FAIL(attach_loc_metas_.push_back(loc_meta))) {
        LOG_WARN("store attach loc meta failed", K(ret));
      } else {
        OB_UNIS_DECODE(*loc_meta);
      }
    }
    OZ(deserialize_ctdef_tree(buf, data_len, pos, attach_ctdef_));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObDASAttachSpec)
{
  int64_t len = 0;
  bool has_attach_ctdef = attach_ctdef_ != nullptr;
  OB_UNIS_ADD_LEN(has_attach_ctdef);
  if (has_attach_ctdef) {
    OB_UNIS_ADD_LEN(attach_loc_metas_.size());
    FOREACH(it, attach_loc_metas_) {
      const ObDASTableLocMeta *loc_meta = *it;
      OB_UNIS_ADD_LEN(*loc_meta);
    }
    len += get_ctdef_tree_serialize_size(attach_ctdef_);
  }
  return len;
}

int ObDASAttachSpec::serialize_ctdef_tree(char *buf,
                                          const int64_t buf_len,
                                          int64_t &pos,
                                          const ObDASBaseCtDef *root) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("root ctdef is nullptr", K(ret));
  } else {
    ObDASOpType op_type = root->op_type_;
    bool has_main_ctdef = (scan_ctdef_ == root);
    OB_UNIS_ENCODE(has_main_ctdef);
    if (!has_main_ctdef) {
      OB_UNIS_ENCODE(op_type);
      OB_UNIS_ENCODE(*root);
      OB_UNIS_ENCODE(root->children_cnt_);
      for (int i = 0; OB_SUCC(ret) && i < root->children_cnt_; ++i) {
        OZ(serialize_ctdef_tree(buf, buf_len, pos, root->children_[i]));
      }
    }
  }
  return ret;
}

int64_t ObDASAttachSpec::get_ctdef_tree_serialize_size(const ObDASBaseCtDef *root) const
{
  int64_t len = 0;
  if (OB_NOT_NULL(root)) {
    ObDASOpType op_type = root->op_type_;
    bool has_main_ctdef = (scan_ctdef_ == root);
    OB_UNIS_ADD_LEN(has_main_ctdef);
    if (!has_main_ctdef) {
      OB_UNIS_ADD_LEN(op_type);
      OB_UNIS_ADD_LEN(*root);
      OB_UNIS_ADD_LEN(root->children_cnt_);
      for (int i = 0; i < root->children_cnt_; ++i) {
        len += get_ctdef_tree_serialize_size(root->children_[i]);
      }
    }
  }
  return len;
}

int ObDASAttachSpec::deserialize_ctdef_tree(const char *buf,
                                            const int64_t data_len,
                                            int64_t &pos,
                                            ObDASBaseCtDef *&root)
{
  int ret = OB_SUCCESS;
  ObDASOpType op_type = DAS_OP_INVALID;
  bool has_main_ctdef = 0;
  OB_UNIS_DECODE(has_main_ctdef);
  if (!has_main_ctdef) {
    OB_UNIS_DECODE(op_type);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(ObDASTaskFactory::alloc_das_ctdef(op_type, allocator_, root))) {
        LOG_WARN("allooc das ctde failed", K(ret), K(op_type));
      }
    }
    OB_UNIS_DECODE(*root);
    OB_UNIS_DECODE(root->children_cnt_);
    if (OB_SUCC(ret) && root->children_cnt_ > 0) {
      if (OB_ISNULL(root->children_ = OB_NEW_ARRAY(ObDASBaseCtDef*, &allocator_, root->children_cnt_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc child buffer failed", K(ret), K(root->children_cnt_));
      }
    }
    for (int i = 0; OB_SUCC(ret) && i < root->children_cnt_; ++i) {
      OZ(deserialize_ctdef_tree(buf, data_len, pos, root->children_[i]));
    }
  } else {
    root = scan_ctdef_;
  }
  return ret;
}

const ObDASTableLocMeta *ObDASAttachSpec::get_attach_loc_meta(int64_t table_location_id,
                                                              int64_t ref_table_id) const
{
  const ObDASTableLocMeta *loc_meta = nullptr;
  FOREACH_X(it, attach_loc_metas_, nullptr == loc_meta) {
    const ObDASTableLocMeta *tmp_loc_meta = *it;
    if (tmp_loc_meta->table_loc_id_ == table_location_id &&
        tmp_loc_meta->ref_table_id_ == ref_table_id) {
      loc_meta = tmp_loc_meta;
    }
  }
  return loc_meta;
}

int ObDASAttachSpec::set_calc_exprs(const ExprFixedArray &calc_exprs, const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (nullptr != attach_ctdef_) {
    if (OB_UNLIKELY(!ObDASTaskFactory::is_attached(attach_ctdef_->op_type_))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected attach op type", K(ret), K(attach_ctdef_->op_type_));
    }
    OZ(set_calc_exprs_tree(static_cast<ObDASAttachCtDef *>(attach_ctdef_), calc_exprs, max_batch_size));
  }
  return ret;
}

int ObDASAttachSpec::set_calc_exprs_tree(ObDASAttachCtDef *root,
                                         const ExprFixedArray &calc_exprs,
                                         const int64_t max_batch_size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(root)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null root attach ctdef", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < root->children_cnt_; ++i) {
    ObDASBaseCtDef *child = root->children_[i];
    if (ObDASTaskFactory::is_attached(child->op_type_)) {
      ObDASAttachCtDef *attach_child = static_cast<ObDASAttachCtDef *>(child);
      OZ(set_calc_exprs_tree(attach_child, calc_exprs, max_batch_size));
    } else if (child->op_type_ == DAS_OP_TABLE_SCAN) {
      if (OB_FAIL(static_cast<ObDASScanCtDef *>(child)->pd_expr_spec_.set_calc_exprs(calc_exprs, max_batch_size))) {
        LOG_WARN("failed to set scan calc exprs", K(ret), KPC(child));
      }
    }
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

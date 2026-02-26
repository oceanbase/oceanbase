/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_odps_table_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"

namespace oceanbase {
namespace sql {

int ObODPSTableUtils::create_array_helper(ObExecContext &exec_ctx,
                                          ObIAllocator &allocator,
                                          const ObExpr &cur_expr,
                                          ObODPSArrayHelper *&array_helper)
{
  int ret = OB_SUCCESS;
  uint16_t subschema_id = cur_expr.obj_meta_.get_subschema_id();
  ObSubSchemaValue sub_meta;
  if (OB_UNLIKELY(ObCollectionSQLType != cur_expr.obj_meta_.get_type())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected expr type", K(cur_expr.obj_meta_.get_type()));
  } else if (OB_FAIL(exec_ctx.get_sqludt_meta_by_subschema_id(subschema_id, sub_meta))) {
    LOG_WARN("failed to get collection subschema", K(subschema_id));
  } else if (OB_UNLIKELY(sub_meta.type_ != OB_SUBSCHEMA_COLLECTION_TYPE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected subschema value", K(sub_meta));
  } else if (OB_ISNULL(sub_meta.value_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null");
  } else if (OB_FAIL(recursive_create_array_helper(allocator,
                                                   static_cast<ObSqlCollectionInfo*>(sub_meta.value_)->collection_meta_,
                                                   array_helper))) {
    LOG_WARN("failed to recursive create array helper");
  }
  return ret;
}

int ObODPSTableUtils::recursive_create_array_helper(ObIAllocator &allocator,
                                                    const ObCollectionTypeBase *coll_meta,
                                                    ObODPSArrayHelper *&array_helper)
{
  int ret = OB_SUCCESS;
  void *ptr = allocator.alloc(sizeof(ObODPSArrayHelper));
  ObODPSArrayHelper *helper = nullptr;
  if (OB_ISNULL(coll_meta) || OB_UNLIKELY(coll_meta->type_id_ != ObNestedType::OB_ARRAY_TYPE)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected collection meta", KPC(coll_meta));
  } else if (OB_ISNULL(ptr)){
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory for array helper");
  } else {
    const ObCollectionArrayType *array_type = static_cast<const ObCollectionArrayType *>(coll_meta);
    helper = new(ptr) ObODPSArrayHelper(allocator);
    if (OB_FAIL(ObArrayTypeObjFactory::construct(allocator, *array_type, helper->array_, false))) {
      LOG_WARN("failed to construct array");
    } else if (ObNestedType::OB_ARRAY_TYPE == array_type->element_type_->type_id_) {
      if (OB_FAIL(SMART_CALL(recursive_create_array_helper(allocator,
                                                           array_type->element_type_,
                                                           helper->child_helper_)))) {
        LOG_WARN("failed to recursive create array helper");
      } else {
        helper->element_type_ = ObCollectionSQLType;
        helper->element_precision_ = -1;
        helper->element_scale_ = -1;
        helper->element_collation_ = CS_TYPE_BINARY;
        helper->element_length_ = 0;
      }
    } else if (ObNestedType::OB_BASIC_TYPE == array_type->element_type_->type_id_) {
      ObCollectionBasicType *basic_type = static_cast<ObCollectionBasicType *>(array_type->element_type_);
      helper->element_type_ = basic_type->basic_meta_.get_obj_type();
      helper->element_precision_ = basic_type->basic_meta_.get_precision();
      helper->element_scale_ = basic_type->basic_meta_.get_scale();
      helper->element_collation_ = basic_type->basic_meta_.get_collation_type();
      helper->element_length_ = basic_type->basic_meta_.get_length();
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected nested array type", K(coll_meta->type_id_));
    }
    if (OB_SUCC(ret)) {
      array_helper = helper;
    }
  }
  return ret;
}

} // sql
} // oceanbase
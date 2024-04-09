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

#define USING_LOG_PREFIX PL

#include "ob_pl_allocator.h"
#include "ob_pl_package_state.h"

namespace oceanbase
{
namespace pl
{

void* ObPLAllocator::alloc(const int64_t size, const ObMemAttr &attr)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  if (OB_ISNULL(curr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("current allocator is null", K(ret), K(curr_));
  } else if (can_shrink_ && curr_->used() > next_threshold_) {
    if (OB_FAIL(shrink())) {
      LOG_WARN("failed to shrink buffer", K(ret), K(next_threshold_), K(threshold_));
    }
  }
  if (OB_SUCC(ret)) {
    ptr = curr_->alloc(size, attr);
    if (curr_->used() > 100 * 1024 * 1024) {
      LOG_INFO("[WARNING] PlTemp memory hold too much, should check...", K(curr_->used()), K(lbt()));
    }
  }
  return ptr;
}

void ObPLAllocator::reset()
{
  allocator1_.reset();
  allocator2_.reset();
  curr_ = &allocator1_;
  backup_ = &allocator2_;
}

int ObPLAllocator::shrink()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(curr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current allocator is null", K(ret), K(curr_));
  } else if (curr_->used() < next_threshold_) {
    // do nothing ...
  } else if (OB_FAIL(copy_all_element_with_new_allocator(backup_))) {
    LOG_WARN("failed to copy all element",
             K(ret), K(next_threshold_), K(curr_->used()), K(backup_->used()));
  } else {
    // calc next threshold
    next_threshold_ = std::max(2 * backup_->used(), threshold_);
    LOG_INFO("NOTICE: shrink allocator done!",
              K(next_threshold_),
              K(threshold_),
              K(backup_->used()), K(curr_->used()),
              K(curr_), K(backup_));
    // replace current allocator
    ObIAllocator *tmp = curr_;
    curr_->reset();
    curr_ = backup_;
    backup_ = tmp;
  }
  return ret;
}

int ObPLCollAllocator::free_child_coll(ObPLCollection &dest)
{
  int ret = OB_SUCCESS;

  for (int64_t i = 0; OB_SUCC(ret) && i < dest.get_count(); ++i) {
    OZ (ObUserDefinedType::destruct_obj(dest.get_data()[i], NULL));
  }
  return ret;
}

int ObPLCollAllocator::copy_all_element_with_new_allocator(ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
    UNUSED(allocator);
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret));
#else
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy allocator is null", K(ret), K(allocator));
  } else if (OB_ISNULL(coll_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("collection is null", K(ret), K(coll_));
  } else {

#define DEEP_COPY_COLLECTION(type, class) \
  case type: { \
    if (OB_ISNULL(dest = reinterpret_cast<class*>(allocator->alloc(sizeof(class))))) { \
      ret = OB_ALLOCATE_MEMORY_FAILED; \
      LOG_WARN("failed to alloc memory for collection", K(ret), K(allocator), KPC(coll_), KPC(dest)); \
    } else if (FALSE_IT(dest = new (dest) class(coll_->get_id()))) { \
    } else if (OB_FAIL((reinterpret_cast<class*>(dest))->deep_copy(coll_, allocator))) { \
      LOG_WARN("failed to deep copy", K(ret), K(allocator), KPC(coll_), KPC(dest)); \
      int tmp = ObPLCollAllocator::free_child_coll(reinterpret_cast<ObPLCollection&>(*dest));   \
      if (OB_SUCCESS != tmp) {                                      \
        LOG_WARN("failed to free child memory", K(tmp));    \
      }  \
    } \
    break; \
  }
    ObPLCollection* dest = NULL;
    switch (coll_->get_type()) {
      DEEP_COPY_COLLECTION(PL_NESTED_TABLE_TYPE, ObPLNestedTable);
      DEEP_COPY_COLLECTION(PL_ASSOCIATIVE_ARRAY_TYPE, ObPLAssocArray);
      DEEP_COPY_COLLECTION(PL_VARRAY_TYPE, ObPLVArray);
      default: {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknow collection type", K(ret), K(coll_->get_type()), KPC(coll_));
        break;
      }
    }
#undef DEEP_COPY_COLLECTION

    if (OB_SUCC(ret)) {
      dest->set_allocator(this);
      for (int64_t i = 0; OB_SUCC(ret) && i < coll_->get_count(); ++i) {
        if (OB_FAIL(ObUserDefinedType::destruct_obj(coll_->get_data()[i], NULL))) {
          LOG_WARN("failed to destruct collection", K(ret), K(coll_->get_data()[i]), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        coll_->set_allocator(dest->get_allocator());
        coll_->set_data(dest->get_data());
        if (PL_ASSOCIATIVE_ARRAY_TYPE == coll_->get_type()) {
          ObPLAssocArray* src = static_cast<ObPLAssocArray*>(coll_);
          ObPLAssocArray* dst = static_cast<ObPLAssocArray*>(dest);
          if (OB_ISNULL(src) || OB_ISNULL(dst)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected associative array pointer", K(ret), KPC(coll_), KPC(dst));
          } else {
            src->set_key(dst->get_key());
            src->set_sort(dst->get_sort());
          }
        }
      }
    }
    LOG_INFO("copy all element with new allocator in collection", K(ret), KPC(coll_), KPC(dest), K(lbt()));
  }
#endif
  return ret;
}

int ObPLSymbolAllocator::copy_all_element_with_new_allocator(ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("copy allocator is null", K(ret), K(allocator));
  } else if (OB_ISNULL(pl_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(pl_->params_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl symbols is null", K(ret), K(pl_->params_));
  } else if (OB_ISNULL(pl_->result_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("pl result is null", K(ret), K(pl_->result_));
  } else {
    ParamStore *params = pl_->params_;
    for (int64_t i = 0; OB_SUCC(ret) && i < params->count(); ++i) {
      ObObj dst;
      ObObj *src = &(params->at(i));
      OZ (deep_copy_obj(*allocator, *src, dst));
      CK (params->at(i).apply(dst));
      OX (params->at(i).set_param_meta());
    }
    ObObj dst_result;
    OZ (deep_copy_obj(*allocator, *(pl_->result_), dst_result));
    OX ((*pl_->result_).apply(dst_result));
  }
  return ret;
}

int ObPLPkgAllocator::copy_all_element_with_new_allocator(ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  LOG_INFO("copy all element with new allocator in package state");
  CK (OB_NOT_NULL(allocator));
  CK (OB_NOT_NULL(state_));
  if (OB_SUCC(ret)) {
    ObIArray<ObObj> &vars = state_->get_vars();
    for (int64_t i = 0; OB_SUCC(ret) && i < vars.count(); ++i) {
      ObObj dst;
      if (vars.at(i).is_pl_extend()
          && vars.at(i).get_meta().get_extend_type() != PL_CURSOR_TYPE
          && vars.at(i).get_meta().get_extend_type() != PL_REF_CURSOR_TYPE) {
        OZ (pl::ObUserDefinedType::deep_copy_obj(*allocator, vars.at(i), dst, true));
        OZ (pl::ObUserDefinedType::destruct_obj(vars.at(i), nullptr));
      } else {
        OZ (deep_copy_obj(*allocator, vars.at(i), dst));
      }
      OX (vars.at(i) = dst);
    }
  }
  return ret;
}

}
}

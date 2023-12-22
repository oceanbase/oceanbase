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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/sort/ob_sort_vec_op_provider.h"

namespace oceanbase {
using namespace common;
namespace sql {

/************************* start ObSortVecOpProvider ***************************/
ObSortVecOpProvider::~ObSortVecOpProvider()
{
  reset();
}

void ObSortVecOpProvider::reset()
{
  if (nullptr != sort_op_impl_) {
    sort_op_impl_->~ObISortVecOpImpl();
    alloc_->free(sort_op_impl_);
    sort_op_impl_ = nullptr;
    is_inited_ = false;
  }
}

bool ObSortVecOpProvider::is_basic_cmp_type(VecValueTypeClass vec_tc)
{
  bool b_ret = false;
  switch(vec_tc) {
    case VEC_TC_INTEGER:
    case VEC_TC_UINTEGER:
    case VEC_TC_DATE:
    case VEC_TC_TIME:
    case VEC_TC_DATETIME:
    case VEC_TC_YEAR:
    case VEC_TC_BIT:
    case VEC_TC_ENUM_SET:
    case VEC_TC_INTERVAL_YM:
    case VEC_TC_DEC_INT32:
    case VEC_TC_DEC_INT64:
    case VEC_TC_DEC_INT128:
    case VEC_TC_DEC_INT256:
    case VEC_TC_DEC_INT512:
      b_ret = true;
      break;
    default:
      b_ret = false;
      break;
  }
  return b_ret;
}

int ObSortVecOpProvider::decide_sort_key_type(ObSortVecOpContext &ctx)
{
  int ret = OB_SUCCESS;
  if (ctx.prefix_pos_ > 0) {
    sort_type_ = ObSortVecOpProvider::SORT_TYPE_PREFIX;
  } else {
    sort_type_ = ObSortVecOpProvider::SORT_TYPE_GENERAL;
  }
  if (ctx.topn_cnt_ != INT64_MAX) {
    sort_key_type_ = ObSortVecOpProvider::SK_TYPE_TOPN;
  } else {
    sort_key_type_ = ObSortVecOpProvider::SK_TYPE_GENERAL;
  }
  if (!ctx.enable_encode_sortkey_) {
    is_basic_cmp_ = true;
    for (int64_t i = 0; is_basic_cmp_ && i < ctx.sk_exprs_->count(); i++) {
      VecValueTypeClass vec_tc = ctx.sk_exprs_->at(i)->get_vec_value_tc();
      is_basic_cmp_ = is_basic_cmp_type(vec_tc);
    }
  }
  return ret;
}

template <typename SORT_CLASS>
int ObSortVecOpProvider::alloc_sort_impl_instance(ObISortVecOpImpl *&sort_op_impl)
{
  int ret = OB_SUCCESS;
  sort_op_impl = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = alloc_->alloc(sizeof(SORT_CLASS)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create sort impl instance", K(ret));
  } else {
    sort_op_impl = new (buf) SORT_CLASS(op_monitor_info_, mem_context_);
  }
  return ret;
}

template <ObSortVecOpProvider::SortType sort_type, ObSortVecOpProvider::SortKeyType sk_type, bool has_addon>
int ObSortVecOpProvider::init_sort_impl_instance(ObISortVecOpImpl *&sort_op_impl)
{
  int ret = OB_SUCCESS;
  if (is_basic_cmp_) {
    if (OB_SUCCESS
        != (ret = alloc_sort_impl_instance<RTSortImplType<sort_type, true, sk_type, has_addon>>(
              sort_op_impl))) {
      LOG_WARN("failed to alloc sort impl instance", K(ret));
    }
  } else if (OB_SUCCESS
             != (ret =
                   alloc_sort_impl_instance<RTSortImplType<sort_type, false, sk_type, has_addon>>(
                     sort_op_impl))) {
    LOG_WARN("failed to alloc sort impl instance", K(ret));
  }
  return ret;
}

int ObSortVecOpProvider::init_sort_impl(ObSortVecOpContext &ctx, ObISortVecOpImpl *&sort_op_impl)
{
#define INIT_SORT_IMPL_INSTANCE(sort_type, sk_type, has_addon)                                     \
  do {                                                                                             \
    if (has_addon) {                                                                               \
      if (OB_SUCCESS != (ret = init_sort_impl_instance<sort_type, sk_type, true>(sort_op_impl))) { \
        LOG_WARN("failed to init sort impl instance", K(ret));                                     \
      }                                                                                            \
    } else {                                                                                       \
      if (OB_SUCCESS != (ret = init_sort_impl_instance<sort_type, sk_type, false>(sort_op_impl))) {\
        LOG_WARN("failed to init sort impl instance", K(ret));                                     \
      }                                                                                            \
    }                                                                                              \
  } while (0)

  int ret = OB_SUCCESS;
  sort_op_impl = nullptr;
  has_addon_ = ctx.has_addon_;
  if (OB_FAIL(decide_sort_key_type(ctx))) {
    LOG_WARN("failed to decide sort key type", K(ret));
  } else if (SORT_TYPE_GENERAL == sort_type_) {
    if (SK_TYPE_TOPN == sort_key_type_) {
      INIT_SORT_IMPL_INSTANCE(SORT_TYPE_GENERAL, SK_TYPE_TOPN, has_addon_);
    } else if (SK_TYPE_GENERAL == sort_key_type_) {
      INIT_SORT_IMPL_INSTANCE(SORT_TYPE_GENERAL, SK_TYPE_GENERAL, has_addon_);
    }
  } else if (SORT_TYPE_PREFIX == sort_type_) {
    if (SK_TYPE_TOPN == sort_key_type_) {
      INIT_SORT_IMPL_INSTANCE(SORT_TYPE_PREFIX, SK_TYPE_TOPN, has_addon_);
    } else if (SK_TYPE_GENERAL == sort_key_type_) {
      INIT_SORT_IMPL_INSTANCE(SORT_TYPE_PREFIX, SK_TYPE_GENERAL, has_addon_);
    }
  }
  return ret;
}

int ObSortVecOpProvider::init_mem_context(uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(tenant_id, ObModIds::OB_SQL_SORT_ROW, ObCtxIds::WORK_AREA)
    .set_properties(lib::USE_TL_PAGE_OPTIONAL);
  if (nullptr == mem_context_ && OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(mem_context_, param))) {
    LOG_WARN("create entity failed", K(ret));
  } else if (NULL == mem_context_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("null memory entity returned", K(ret));
  } else {
    alloc_ = &mem_context_->get_malloc_allocator();
  }
  return ret;
}

int ObSortVecOpProvider::init(ObSortVecOpContext &context)
{
  int ret = OB_SUCCESS;
  if (is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_INVALID_ID == context.tenant_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(context.tenant_id_));
  } else if (OB_ISNULL(context.sk_exprs_) || OB_ISNULL(context.addon_exprs_)
             || OB_ISNULL(context.sk_collations_) || OB_ISNULL(context.eval_ctx_)
             || OB_ISNULL(context.exec_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument: argument is null", K(ret), K(context.tenant_id_),
             K(context.sk_collations_), K(context.eval_ctx_), K(context.exec_ctx_));
  } else if (OB_FAIL(init_mem_context(context.tenant_id_))) {
    LOG_WARN("failed to init mem context", K(ret), K(context.tenant_id_));
  } else if (OB_FAIL(init_sort_impl(context, sort_op_impl_))) {
    LOG_WARN("failed to init sort impl instance", K(ret));
  } else if (OB_FAIL(sort_op_impl_->init(context))) {
    LOG_WARN("failed to init sort impl", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObSortVecOpProvider::add_batch(const ObBatchRows &input_brs, bool &sort_need_dump)
{
  check_status();
  return sort_op_impl_->add_batch(input_brs, sort_need_dump);
}

int ObSortVecOpProvider::get_next_batch(const int64_t max_cnt, int64_t &read_rows)
{
  check_status();
  return sort_op_impl_->get_next_batch(max_cnt, read_rows);
}

int ObSortVecOpProvider::sort()
{
  check_status();
  return sort_op_impl_->sort();
}

int ObSortVecOpProvider::add_batch_stored_row(int64_t &row_size,
                                              const ObCompactRow **sk_stored_rows,
                                              const ObCompactRow **addon_stored_rows)
{
  check_status();
  return sort_op_impl_->add_batch_stored_row(row_size, sk_stored_rows, addon_stored_rows);
}

int64_t ObSortVecOpProvider::get_extra_size(bool is_sort_key)
{
  check_status();
  return sort_op_impl_->get_extra_size(is_sort_key);
}

void ObSortVecOpProvider::unregister_profile()
{
  if (is_inited_) { sort_op_impl_->unregister_profile(); }
}

void ObSortVecOpProvider::unregister_profile_if_necessary()
{
  if (is_inited_) { sort_op_impl_->unregister_profile_if_necessary(); }
}

void ObSortVecOpProvider::collect_memory_dump_info(ObMonitorNode &info)
{
  if (is_inited_) { sort_op_impl_->collect_memory_dump_info(info); }
}

void ObSortVecOpProvider::set_input_rows(int64_t input_rows)
{
  check_status();
  return sort_op_impl_->set_input_rows(input_rows);
}

void ObSortVecOpProvider::set_input_width(int64_t input_width)
{
  check_status();
  return sort_op_impl_->set_input_width(input_width);
}

void ObSortVecOpProvider::set_operator_type(ObPhyOperatorType op_type)
{
  check_status();
  return sort_op_impl_->set_operator_type(op_type);
}

void ObSortVecOpProvider::set_operator_id(uint64_t op_id)
{
  check_status();
  return sort_op_impl_->set_operator_id(op_id);
}

void ObSortVecOpProvider::set_io_event_observer(ObIOEventObserver *observer)
{
  check_status();
  return sort_op_impl_->set_io_event_observer(observer);
}
/************************* end ObSortVecOpProvider ***************************/

} // end namespace sql
} // end namespace oceanbase

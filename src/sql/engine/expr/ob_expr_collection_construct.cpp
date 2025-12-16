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

#include "ob_expr_collection_construct.h"
#include "pl/ob_pl_package.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(
    (ObExprCollectionConstruct, ObFuncExprOperator),
    type_, not_null_, elem_type_, capacity_, udt_id_);

OB_SERIALIZE_MEMBER(ObExprCollectionConstruct::ExtraInfo,
                    type_, not_null_, elem_type_, capacity_, udt_id_);

int ObExprCollectionConstruct::ExtraInfo::deep_copy(common::ObIAllocator &allocator,
                                                    const ObExprOperatorType type,
                                                    ObIExprExtraInfo *&copied_info) const
{
  int ret = OB_SUCCESS;
  OZ(ObExprExtraInfoFactory::alloc(allocator, type, copied_info));
  ExtraInfo &other = *static_cast<ExtraInfo *>(copied_info);
  if (OB_SUCC(ret)) {
    other = *this;
  }
  return ret;
}

ObExprCollectionConstruct::ObExprCollectionConstruct(common::ObIAllocator &alloc)
    : ObFuncExprOperator(
        alloc, T_FUN_PL_COLLECTION_CONSTRUCT, N_PL_COLLECTION_CONSTRUCT,
        PARAM_NUM_UNKNOWN, VALID_FOR_GENERATED_COL, NOT_ROW_DIMENSION, false, INTERNAL_IN_ORACLE_MODE),
      type_(pl::ObPLType::PL_INVALID_TYPE),
      not_null_(false),
      elem_type_(),
      capacity_(OB_INVALID_SIZE),
      udt_id_(OB_INVALID_ID) {}

ObExprCollectionConstruct::~ObExprCollectionConstruct() {}

int ObExprCollectionConstruct::calc_result_typeN(ObExprResType &type,
                                                 ObExprResType *types,
                                                 int64_t param_num,
                                                 ObExprTypeCtx &type_ctx) const
{
  int ret = OB_SUCCESS;
  const ObCollectionConstructRawExpr *pl_expr
          = static_cast<const ObCollectionConstructRawExpr*>(type_ctx.get_raw_expr());
  CK(OB_NOT_NULL(pl_expr));
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
    if ((ObExtendType == elem_type_.get_obj_type()
          && types[i].get_type() != ObExtendType && types[i].get_type() != ObNullType && !(0 == i % 2 && pl_expr->is_associative_array_with_param_assign_op_))
        ||(ObExtendType == types[i].get_type() && elem_type_.get_obj_type() != ObExtendType)) {
      ObSchemaGetterGuard schema_guard;
      int64_t tenant_id = type_ctx.get_session()->get_effective_tenant_id();
      const ObUDTTypeInfo *udt_info = NULL;
      OZ (GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard));
      OZ (schema_guard.get_udt_info(tenant_id, udt_id_, udt_info));
      if (OB_SUCC(ret)) {
        ret = OB_ERR_CALL_WRONG_ARG;
        if (OB_NOT_NULL(udt_info)) {
          LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, udt_info->get_type_name().length(), udt_info->get_type_name().ptr());
        }
      }
      LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret));
    } else {
      types[i].set_calc_accuracy(elem_type_.get_accuracy());
      types[i].set_calc_meta(elem_type_.get_meta_type());
    }
    CK(OB_NOT_NULL(pl_expr));
    if(pl_expr->is_associative_array_with_param_assign_op_) {
      if(0 == i % 2) {
        pl::ObPLDataType index_type = pl_expr->get_index_type();
        CK(OB_NOT_NULL(index_type.get_data_type()));
        OX(types[i].set_calc_accuracy(index_type.get_data_type()->get_accuracy()));
        OX(types[i].set_calc_meta(index_type.get_data_type()->get_meta_type()));
      }
    }
  }
  OX (type.set_type(ObExtendType));
  OX (type.set_extend_type(type_));
  OX (type.set_udt_id(udt_id_));
  return ret;
}

int ObExprCollectionConstruct::cg_expr(ObExprCGCtx &op_cg_ctx,
                                       const ObRawExpr &raw_expr,
                                       ObExpr &rt_expr) const
{
  int ret = OB_SUCCESS;

  ObIAllocator &alloc = *op_cg_ctx.allocator_;
  if (OB_SUCC(ret)) {
    ExtraInfo *info = OB_NEWx(ExtraInfo, (&alloc), alloc, T_FUN_PL_COLLECTION_CONSTRUCT);
    if (NULL == info) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret));
    } else {
      const ObCollectionConstructRawExpr &pl_expr
          = static_cast<const ObCollectionConstructRawExpr&>(raw_expr);
      info->type_ = pl_expr.get_type();
      info->not_null_ = pl_expr.is_not_null();
      info->capacity_ = pl_expr.get_capacity();
      info->udt_id_ = pl_expr.get_udt_id();
      info->is_associative_array_with_param_assign_op_ = pl_expr.is_associative_array_with_param_assign_op_;
      if (info->is_associative_array_with_param_assign_op_) {
        CK(OB_NOT_NULL(pl_expr.get_index_type().get_data_type()));
        OX(info->index_type_ = *pl_expr.get_index_type().get_data_type());
      }
      if (pl_expr.get_elem_type().is_obj_type()) {
        CK(OB_NOT_NULL(pl_expr.get_elem_type().get_data_type()));
        OX(info->elem_type_ = *pl_expr.get_elem_type().get_data_type());
      } else {
        info->elem_type_.set_obj_type(ObExtendType);
      }

      if (OB_SUCC(ret)) {
        rt_expr.extra_info_ = info;
        rt_expr.eval_func_ = &eval_collection_construct;
      }
    }
  }
  return ret;
}


int ObExprCollectionConstruct::check_match(const ObObj &element_obj, pl::ObElemDesc &desc, pl::ObPLINS &ns)
{
  int ret = OB_SUCCESS;
  pl::ObPLType type = desc.get_pl_type();
  bool is_comp = false;
  if (pl::PL_NESTED_TABLE_TYPE == element_obj.get_meta().get_extend_type() ||
      pl::PL_VARRAY_TYPE == element_obj.get_meta().get_extend_type()) {
    is_comp = pl::PL_NESTED_TABLE_TYPE == type || pl::PL_VARRAY_TYPE == type;
  } else {
    is_comp = element_obj.get_meta().get_extend_type() == type;
  }

  if (is_comp) {
    pl::ObPLComposite *composite = reinterpret_cast<pl::ObPLComposite*>(element_obj.get_ext());
    CK (OB_NOT_NULL(composite));
    if (OB_FAIL(ret)) {
    } else if (composite->get_id() != desc.get_udt_id()) {
      if (composite->is_record()) {
        OZ (pl::ObPLResolver::check_composite_compatible(ns, composite->get_id(),
                                                         desc.get_udt_id(),
                                                         is_comp));
      } else if (pl::is_mocked_anonymous_array_id(composite->get_id())) {
        // anonymous_array compatible check has done in resolve phase
        is_comp = true;
      } else {
        is_comp = false;
      }
    }
  }
  if (OB_SUCC(ret) && !is_comp) {
    ret = OB_ERR_CALL_WRONG_ARG;
    LOG_WARN("invalid argument. unexpected composite value", K(ret));
  }

  return ret;
}

int ObExprCollectionConstruct::eval_collection_construct(const ObExpr &expr,
                                                         ObEvalCtx &ctx,
                                                         ObDatum &expr_datum)
{
  int ret = OB_SUCCESS;
#ifndef OB_BUILD_ORACLE_PL
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support", K(ret));
#else
  pl::ObPLCollection *coll = NULL;
  ObSQLSessionInfo *session = ctx.exec_ctx_.get_my_session();
  ObExecContext &exec_ctx = ctx.exec_ctx_;
  const ExtraInfo *info = static_cast<const ExtraInfo *>(expr.extra_info_);
  CK(NULL != info);
  // check types
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    const ObExpr *e = expr.args_[i];
    if (0 == i % 2 && info->is_associative_array_with_param_assign_op_) {
      if (ObNullType != e->datum_meta_.type_
        && e->datum_meta_.type_ != info->index_type_.get_obj_type()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("check type failed", K(ret), K(*e), K(info->elem_type_));
      }
    }else if (ObNullType != e->datum_meta_.type_
        && e->datum_meta_.type_ != info->elem_type_.get_obj_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("check type failed", K(ret), K(*e), K(info->elem_type_));
    }
  }

  ObIAllocator *alloc = &ctx.exec_ctx_.get_allocator();
  pl::ObPLExecCtx *pl_exec_ctx = nullptr;
  // for collection construct in pl, use top_expr_allocator
  // we will destroy this obj in pl final interface
  if (OB_NOT_NULL(session) &&
      OB_NOT_NULL(session->get_pl_context()) &&
      OB_NOT_NULL(pl_exec_ctx = session->get_pl_context()->get_current_ctx()) &&
      pl_exec_ctx->get_exec_ctx() == &ctx.exec_ctx_) {
    alloc = pl_exec_ctx->get_top_expr_allocator();
  }
  if (OB_SUCC(ret)) {
    if (pl::PL_NESTED_TABLE_TYPE == info->type_) {
      if (NULL == (coll =
      static_cast<pl::ObPLNestedTable*>(alloc->alloc(sizeof(pl::ObPLNestedTable) + 8)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
      } else {
        coll = new(coll)pl::ObPLNestedTable(info->udt_id_);
      }
    } else if (pl::PL_VARRAY_TYPE == info->type_) {
      if (NULL == (coll =
      static_cast<pl::ObPLVArray*>(alloc->alloc(sizeof(pl::ObPLVArray) + 8)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
      } else {
        coll = new(coll)pl::ObPLVArray(info->udt_id_);
        static_cast<pl::ObPLVArray*>(coll)->set_capacity(info->capacity_);
      }
    } else if (pl::PL_ASSOCIATIVE_ARRAY_TYPE == info->type_) {
       if (NULL == (coll = 
        static_cast<pl::ObPLAssocArray*>(alloc->alloc(sizeof(pl::ObPLAssocArray) + 8)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
        } else {
          coll = new(coll)pl::ObPLAssocArray(info->udt_id_);
        }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected collection type to construct", K(info->type_), K(ret));
    }
  }
  OZ (coll->init_allocator(*alloc, true));
  OZ(expr.eval_param_value(ctx));

  for (int64_t i = 0; OB_SUCC(ret) && info->not_null_ && i < expr.arg_cnt_; ++i) {
    if (expr.args_[i]->locate_expr_datum(ctx).is_null()) {
      ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
      LOG_WARN("not null check violated", K(coll),
                                          K(coll->is_not_null()),
                                          K(coll->get_count()),
                                          K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    const pl::ObUserDefinedType *type = NULL;
    const pl::ObCollectionType *collection_type = NULL;
    pl::ObElemDesc elem_desc;
    pl::ObPLPackageGuard package_guard(session->get_effective_tenant_id());
    ObSchemaGetterGuard *schema_guard = NULL;
    // if called by check_default_value in ddl resolver, no sql ctx, get guard from session cache
    if (OB_ISNULL(exec_ctx.get_sql_ctx()) || OB_ISNULL(exec_ctx.get_sql_ctx()->schema_guard_)) {
      schema_guard = &session->get_cached_schema_guard_info().get_schema_guard();
    } else {
      schema_guard = exec_ctx.get_sql_ctx()->schema_guard_;
    }
    pl::ObPLResolveCtx resolve_ctx(*alloc,
                                   *session,
                                   *(schema_guard),
                                   package_guard,
                                   *(exec_ctx.get_sql_proxy()),
                                   false);
    OZ (package_guard.init());
    pl::ObPLINS *ns = NULL;
    if (NULL == session->get_pl_context()) {
      ns = &resolve_ctx;
    } else {
      ns = session->get_pl_context()->get_current_ctx();
    }

    if (info->elem_type_.get_meta_type().is_ext()) {
      int64_t field_cnt = OB_INVALID_COUNT;
      CK (OB_NOT_NULL(ns));
      OZ (ns->get_user_type(info->udt_id_, type));
      OV (OB_NOT_NULL(type), OB_ERR_UNEXPECTED, K(info->udt_id_));
      CK (type->is_collection_type());
      CK (OB_NOT_NULL(collection_type = static_cast<const pl::ObCollectionType*>(type)));
      OX (elem_desc.set_obj_type(common::ObExtendType));
      OX (elem_desc.set_pl_type(collection_type->get_element_type().get_type()));
      OZ (collection_type->get_element_type().get_field_count(*ns, field_cnt));
      OX (elem_desc.set_field_count(field_cnt));
      OX (elem_desc.set_udt_id(collection_type->get_element_type().get_user_type_id()));
    } else {
      OX (elem_desc.set_data_type(info->elem_type_));
      OX (elem_desc.set_field_count(1));
      OX (elem_desc.set_pl_type(pl::PL_OBJ_TYPE));
    }
    OX (coll->set_element_desc(elem_desc));
    OX (coll->set_not_null(info->not_null_));

    // recursive construction is not needed
    if (OB_SUCC(ret)) {
      if(!info->is_associative_array_with_param_assign_op_) {
        OZ (ObSPIService::spi_set_collection(session->get_effective_tenant_id(),
                                             ns,
                                             *coll,
                                             expr.arg_cnt_)); 
        if (OB_SUCC(ret) && coll->is_associative_array() && expr.arg_cnt_ > 0) {
          OX (coll->set_first(1));
          OX (coll->set_last(coll->get_count()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (info->elem_type_.get_meta_type().is_ext()) {
        // for associative array with param assign op
        int64_t index = OB_INVALID_INDEX;
        int64_t search_end = OB_INVALID_INDEX;
        bool is_repeated_key = false;
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
          const ObDatum &d = expr.args_[i]->locate_expr_datum(ctx);
          ObObj v;
          if (!info->is_associative_array_with_param_assign_op_) {
            CK (OB_NOT_NULL(coll->get_data()));
          }
          OZ(d.to_obj(v, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_));
          // check type match first if value is not null
          if (OB_SUCC(ret) && !v.is_null()) {
            if (pl::PL_OPAQUE_TYPE == v.get_meta().get_extend_type()
                      || pl::PL_CURSOR_TYPE == v.get_meta().get_extend_type()
                      || pl::PL_REF_CURSOR_TYPE == v.get_meta().get_extend_type()) {
              if (v.get_meta().get_extend_type() != coll->get_element_desc().get_pl_type()) {
                const ObUDTTypeInfo *udt_info = NULL;
                OZ (schema_guard->get_udt_info(session->get_effective_tenant_id(), info->udt_id_, udt_info));
                if (OB_SUCC(ret)) {
                  ret = OB_ERR_CALL_WRONG_ARG;
                  if (OB_NOT_NULL(udt_info)) {
                    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, udt_info->get_type_name().length(), udt_info->get_type_name().ptr());
                  }
                }
                LOG_WARN("invalid argument. unexpected composite value", K(ret), K(v), KPC(coll));
              }
            } else {
              if (0 == i % 2 && info->is_associative_array_with_param_assign_op_) {
                TYPE_CHECK(v, info->index_type_.get_meta_type().get_type());
              } else {
                TYPE_CHECK(v, info->elem_type_.get_meta_type().get_type());
                OZ (check_match(v, coll->get_element_desc(), *ns));
              }
              if (OB_ERR_CALL_WRONG_ARG == ret) {
                int tmp_ret = ret;
                ret = OB_SUCCESS;
                const ObUDTTypeInfo *udt_info = NULL;
                OZ (schema_guard->get_udt_info(session->get_effective_tenant_id(), info->udt_id_, udt_info));
                if (OB_SUCC(ret)) {
                  if (OB_NOT_NULL(udt_info)) {
                    LOG_USER_ERROR(OB_ERR_CALL_WRONG_ARG, udt_info->get_type_name().length(), udt_info->get_type_name().ptr());
                  }
                  ret = tmp_ret;
                }
              }
            }
          }
          if (OB_SUCC(ret) && info->not_null_ && !v.is_null() 
              && !(info->is_associative_array_with_param_assign_op_ && 0 == i % 2)) {
            ObObjParam v1 = v;
            OZ (ObSPIService::spi_check_composite_not_null(&v1));
          }
          if (OB_SUCC(ret)) {
            if (info->is_associative_array_with_param_assign_op_) {
              pl::ObPLAssocArray* assoc_array = static_cast<pl::ObPLAssocArray*>(coll);
              CK(OB_NOT_NULL(assoc_array));

              if (0 == i % 2) {
                if(OB_SUCC(ret) && v.is_null()) {
                  ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
                  LOG_WARN("null key is not allowed", K(ret), K(v));
                  LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, 23, "null key is not allowed");
                }
                OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                      info->index_type_.get_obj_type(),
                                                      info->index_type_.get_accuracy(),
                                                      alloc,
                                                      &v));
                index = OB_INVALID_INDEX;
                search_end = OB_INVALID_INDEX;
                if(i != 0) {
                  OZ(assoc_array->search_key(v, index, search_end, assoc_array->get_count()));
                } else {
                  OX(search_end = 0);
                }
                is_repeated_key = false;
                if (OB_SUCC(ret) && 0 != i && index != OB_INVALID_INDEX) {
                  // It means repeated key, there is no need to extend or insert key, record the index is enough.
                  is_repeated_key = true;
                } else {
                  OZ((ObSPIService::spi_extend_assoc_array(session->get_effective_tenant_id(),
                                                           session->get_pl_context()->get_current_ctx(),
                                                           *alloc,
                                                           *assoc_array,
                                                           1)));
                  OZ(assoc_array->insert_sort(v, assoc_array->get_count() - 1, search_end, assoc_array->get_count() - 1));
                  OZ(assoc_array->update_first_last(OB_INVALID_INDEX == search_end ? 0 : search_end));
    
                  CK (OB_NOT_NULL(assoc_array->get_key()));
                  OZ (deep_copy_obj(*coll->get_allocator(),
                                    v,
                                    static_cast<ObObj*>(assoc_array->get_key())[assoc_array->get_count() - 1]));
                }
              } else {
                OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                        info->elem_type_.get_obj_type(),
                                                        info->elem_type_.get_accuracy(),
                                                        alloc,
                                                        &v));
                if (v.is_null()) {
                  ObObj new_composite;
                  int64_t ptr = 0;
                  int64_t init_size = OB_INVALID_SIZE;
                  OZ (collection_type->get_element_type().newx(*coll->get_allocator(), ns, ptr));
                  OZ (collection_type->get_element_type().get_size(pl::PL_TYPE_INIT_SIZE, init_size));
                  OX (new_composite.set_extend(ptr, collection_type->get_element_type().get_type(), init_size));
                  int64_t pos = is_repeated_key ? index : (assoc_array->get_count() - 1);
                  if (is_repeated_key) {
                    OZ (pl::ObUserDefinedType::destruct_objparam(*coll->get_allocator(),
                                                                 static_cast<ObObj&>(static_cast<ObObj*>(coll->get_data())[pos]),
                                                                 nullptr));
                  }
                  OX (static_cast<ObObj*>(coll->get_data())[pos] = new_composite);
                } else {
                  int64_t pos = is_repeated_key ? index : (assoc_array->get_count() - 1);
                  if (is_repeated_key) {
                    OZ (pl::ObUserDefinedType::destruct_objparam(*coll->get_allocator(),
                                                                 static_cast<ObObj&>(static_cast<ObObj*>(assoc_array->get_data())[pos]),
                                                                 nullptr));
                  }
                  OZ (pl::ObUserDefinedType::deep_copy_obj(*coll->get_allocator(),
                                                           v,
                                                           static_cast<ObObj*>(assoc_array->get_data())[pos],
                                                           true));
                }
              }
            } else {
              if (v.is_null()) {
                ObObj new_composite;
                int64_t ptr = 0;
                int64_t init_size = OB_INVALID_SIZE;
                OZ (collection_type->get_element_type().newx(*coll->get_allocator(), ns, ptr));
                OZ (collection_type->get_element_type().get_size(pl::PL_TYPE_INIT_SIZE, init_size));
                OX (new_composite.set_extend(ptr, collection_type->get_element_type().get_type(), init_size));
                OX (static_cast<ObObj*>(coll->get_data())[i] = new_composite);
              } else if (pl::PL_OPAQUE_TYPE == v.get_meta().get_extend_type()
                        || pl::PL_CURSOR_TYPE == v.get_meta().get_extend_type()
                        || pl::PL_REF_CURSOR_TYPE == v.get_meta().get_extend_type()) {
                OZ (pl::ObUserDefinedType::deep_copy_obj(*coll->get_allocator(),
                                                         v,
                                                         static_cast<ObObj*>(coll->get_data())[i],
                                                         true));
              } else {
                OZ (pl::ObUserDefinedType::deep_copy_obj(*coll->get_allocator(),
                                                         v,
                                                         static_cast<ObObj*>(coll->get_data())[i],
                                                         true));
              }
            }
          }
        }
      } else {
        if(info->is_associative_array_with_param_assign_op_) {
          pl::ObPLAssocArray* assoc_array = static_cast<pl::ObPLAssocArray*>(coll);
          for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i += 2) {
            const ObDatum &d = expr.args_[i]->locate_expr_datum(ctx);
            ObObj key;
            int64_t index = OB_INVALID_INDEX;
            int64_t search_end = OB_INVALID_INDEX;
            const ObDatum &d_for_data = expr.args_[i + 1]->locate_expr_datum(ctx);
            ObObj v;
            OZ(d.to_obj(key, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_));
            if(OB_SUCC(ret) && key.is_null()) {
              ret = OB_ERR_NUMERIC_OR_VALUE_ERROR;
              LOG_WARN("null key is not allowed", K(ret), K(key));
              LOG_USER_ERROR(OB_ERR_NUMERIC_OR_VALUE_ERROR, 23, "null key is not allowed");
            }
            if (OB_SUCC(ret) && !key.is_null()) {
              TYPE_CHECK(key, info->index_type_.get_obj_type());
            }
            CK(OB_NOT_NULL(assoc_array));
            if(i != 0) {
              OZ(assoc_array->search_key(key, index, search_end, assoc_array->get_count()));
            } else {
              OX(search_end = 0);
            }
            // check for repeated key in associative array, overwrite the value if key exists
            if (OB_SUCC(ret) && 0 != i && index != OB_INVALID_INDEX) {
              OZ (d_for_data.to_obj(v, expr.args_[i + 1]->obj_meta_, expr.args_[i + 1]->obj_datum_map_));
              if (OB_SUCC(ret) && !v.is_null()) {
                TYPE_CHECK(v, info->elem_type_.get_meta_type().get_type());
              }
              OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                        info->elem_type_.get_obj_type(),
                                                        info->elem_type_.get_accuracy(),
                                                        alloc,
                                                        &v));
              CK (OB_NOT_NULL(assoc_array->get_data()));
              // Free previously deep-copied memory at this slot to avoid leak when overwriting
              OZ (pl::ObUserDefinedType::destruct_objparam(*coll->get_allocator(),
                                                           static_cast<ObObj&>(static_cast<ObObj*>(assoc_array->get_data())[index]),
                                                           nullptr));
              OZ (deep_copy_obj(*coll->get_allocator(),
                                v,
                                static_cast<ObObj*>(assoc_array->get_data())[index]));
            } else {
              OZ((ObSPIService::spi_extend_assoc_array(session->get_effective_tenant_id(),
                                                       session->get_pl_context()->get_current_ctx(),
                                                       *alloc,
                                                       *assoc_array,
                                                       1)));
              OZ(assoc_array->insert_sort(key, assoc_array->get_count() - 1, search_end, assoc_array->get_count() - 1));
              OZ(assoc_array->update_first_last(OB_INVALID_INDEX == search_end ? 0 : search_end));

              OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                        info->index_type_.get_obj_type(),
                                                        info->index_type_.get_accuracy(),
                                                        alloc,
                                                        &key));
              CK (OB_NOT_NULL(assoc_array->get_key()));
              OZ (deep_copy_obj(*coll->get_allocator(),
                                key,
                                static_cast<ObObj*>(assoc_array->get_key())[assoc_array->get_count() - 1]));
              
              OZ(d_for_data.to_obj(v, expr.args_[i + 1]->obj_meta_, expr.args_[i + 1]->obj_datum_map_));
              if (OB_SUCC(ret) && !v.is_null()) {
                TYPE_CHECK(v, info->elem_type_.get_meta_type().get_type());
              }
              OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                        info->elem_type_.get_obj_type(),
                                                        info->elem_type_.get_accuracy(),
                                                        alloc,
                                                        &v));
              CK (OB_NOT_NULL(assoc_array->get_data()));
              OZ (deep_copy_obj(*coll->get_allocator(),
                                v,
                                static_cast<ObObj*>(assoc_array->get_data())[assoc_array->get_count() - 1]));
            }
          }
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
            const ObDatum &d = expr.args_[i]->locate_expr_datum(ctx);
            ObObj v;
            CK (OB_NOT_NULL(coll->get_data()));
            OZ(d.to_obj(v, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_));
            if (OB_SUCC(ret) && !v.is_null()) {
              TYPE_CHECK(v, info->elem_type_.get_meta_type().get_type());
            }
            OZ (ObSPIService::spi_pad_char_or_varchar(session,
                                                      info->elem_type_.get_obj_type(),
                                                      info->elem_type_.get_accuracy(),
                                                      alloc,
                                                      &v));
            OZ (deep_copy_obj(*coll->get_allocator(),
                              v,
                              static_cast<ObObj*>(coll->get_data())[i]));
          }
        }
      }
    }

    ObObj result;
    result.set_extend(reinterpret_cast<int64_t>(coll), coll->get_type());
    OZ(expr_datum.from_obj(result, expr.obj_datum_map_));
    //Collection constructed here must be recorded and destructed at last
    if (OB_NOT_NULL(coll->get_allocator())) {
      int tmp_ret = OB_SUCCESS;
      if (OB_ISNULL(exec_ctx.get_pl_ctx())) {
        tmp_ret = exec_ctx.init_pl_ctx();
      }
      if (OB_SUCCESS == tmp_ret && OB_NOT_NULL(exec_ctx.get_pl_ctx())) {
        tmp_ret = exec_ctx.get_pl_ctx()->add(result);
      }
      if (OB_SUCCESS != tmp_ret) {
        int tmp = pl::ObUserDefinedType::destruct_obj(result, nullptr);
        LOG_WARN("fail to collect pl collection allocator, try to free memory", K(tmp_ret), K(tmp));
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
#endif
  return ret;
}

} /* sql */
} /* oceanbase */

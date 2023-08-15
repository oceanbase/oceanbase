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
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/ob_spi.h"
#include "pl/ob_pl.h"
#include "pl/ob_pl_user_type.h"
#include "pl/ob_pl_package.h"
#include "pl/ob_pl_resolver.h"

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
  UNUSED(type_ctx);
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
    if ((ObExtendType == elem_type_.get_obj_type()
          && types[i].get_type() != ObExtendType && types[i].get_type() != ObNullType)
        ||(ObExtendType == types[i].get_type() && elem_type_.get_obj_type() != ObExtendType)) {
      ret = OB_ERR_CALL_WRONG_ARG;
      LOG_WARN("PLS-00306: wrong number or types of arguments in call", K(ret));
    } else {
      types[i].set_calc_accuracy(elem_type_.get_accuracy());
      types[i].set_calc_meta(elem_type_.get_meta_type());
    }
  }
  OX (type.set_type(ObExtendType));
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
  auto session = ctx.exec_ctx_.get_my_session();
  auto &exec_ctx = ctx.exec_ctx_;
  const ExtraInfo *info = static_cast<const ExtraInfo *>(expr.extra_info_);
  CK(NULL != info);
  // check types
  for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; i++) {
    const ObExpr *e = expr.args_[i];
    if (ObNullType != e->datum_meta_.type_
        && e->datum_meta_.type_ != info->elem_type_.get_obj_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("check type failed", K(ret), K(*e), K(info->elem_type_));
    }
  }

  ObIAllocator &alloc = ctx.exec_ctx_.get_allocator();
  if (OB_SUCC(ret)) {
    if (pl::PL_NESTED_TABLE_TYPE == info->type_) {
      if (NULL == (coll =
      static_cast<pl::ObPLNestedTable*>(alloc.alloc(sizeof(pl::ObPLNestedTable) + 8)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
      } else {
        coll = new(coll)pl::ObPLNestedTable(info->udt_id_);
      }
    } else if (pl::PL_VARRAY_TYPE == info->type_) {
      if (NULL == (coll =
      static_cast<pl::ObPLVArray*>(alloc.alloc(sizeof(pl::ObPLVArray) + 8)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for pl collection", K(ret), K(coll));
      } else {
        coll = new(coll)pl::ObPLVArray(info->udt_id_);
        static_cast<pl::ObPLVArray*>(coll)->set_capacity(info->capacity_);
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("Unexpected collection type to construct", K(info->type_), K(ret));
    }
  }

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
    pl::ObPLResolveCtx resolve_ctx(alloc,
                                   *session,
                                   *(exec_ctx.get_sql_ctx()->schema_guard_),
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
    }
    OX (coll->set_element_desc(elem_desc));
    OX (coll->set_not_null(info->not_null_));

    // recursive construction is not needed
    OZ (ObSPIService::spi_set_collection(session->get_effective_tenant_id(),
                                         ns,
                                         alloc,
                                         *coll,
                                         expr.arg_cnt_));
    if (OB_SUCC(ret)) {
      if (info->elem_type_.get_meta_type().is_ext()) {
        for (int64_t i = 0; OB_SUCC(ret) && i < expr.arg_cnt_; ++i) {
          const ObDatum &d = expr.args_[i]->locate_expr_datum(ctx);
          ObObj v;
          CK (OB_NOT_NULL(coll->get_data()));
          OZ(d.to_obj(v, expr.args_[i]->obj_meta_, expr.args_[i]->obj_datum_map_));
          // check type match first if value is not null
          if (OB_SUCC(ret) && !v.is_null()) {
            if (pl::PL_OPAQUE_TYPE == v.get_meta().get_extend_type()
                      || pl::PL_CURSOR_TYPE == v.get_meta().get_extend_type()
                      || pl::PL_REF_CURSOR_TYPE == v.get_meta().get_extend_type()) {
              if (v.get_meta().get_extend_type() != coll->get_element_desc().get_pl_type()) {
                ret = OB_ERR_CALL_WRONG_ARG;
                LOG_WARN("invalid argument. unexpected composite value", K(ret), K(v), KPC(coll));
              }
            } else {
              TYPE_CHECK(v, info->elem_type_.get_meta_type().get_type());
              OZ (check_match(v, coll->get_element_desc(), *ns));
            }
          }
          if (OB_SUCC(ret) && info->not_null_ && !v.is_null()) {
            ObObjParam v1 = v;
            OZ (ObSPIService::spi_check_composite_not_null(&v1));
          }
          if (OB_SUCC(ret)) {
            if (v.is_null()) {
              ObObj new_composite;
              int64_t ptr = 0;
              int64_t init_size = OB_INVALID_SIZE;
              OZ (collection_type->get_element_type().newx(*coll->get_allocator(), ns, ptr));
              OZ (collection_type->get_element_type().get_size(*ns, pl::PL_TYPE_INIT_SIZE, init_size));
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
                                                    coll->get_allocator(),
                                                    &v));
          OZ (deep_copy_obj(*coll->get_allocator(),
                            v,
                            static_cast<ObObj*>(coll->get_data())[i]));
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
        LOG_ERROR("fail to collect pl collection allocator, may be exist memory issue", K(tmp_ret));
      }
      ret = OB_SUCCESS == ret ? tmp_ret : ret;
    }
  }
#endif
  return ret;
}

} /* sql */
} /* oceanbase */

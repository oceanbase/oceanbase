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
#include "ob_user_defined_function.h"
#include "sql/engine/user_defined_function/ob_udf_util.h"
#include "share/ob_define.h"
#include "share/ob_i_sql_expression.h"


namespace oceanbase
{

using namespace common;

namespace sql
{

ObUdfFunction::~ObUdfFunction()
{
  IGNORE_RETURN ObUdfUtil::unload_so(dlhandle_);
}

/*
 * try to load the so file to ob.
 * try to load origin function and all helper function to ob.
 *
 * */
int ObUdfFunction::init(const share::schema::ObUDFMeta &udf_meta)
{
  int ret = OB_SUCCESS;
  if (udf_meta.dl_.empty() || udf_meta.name_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the udf meta is invalid", K(ret));
  } else if (OB_FAIL(ObUdfUtil::load_so(udf_meta.dl_, dlhandle_))) {
    LOG_WARN("load so error", K(ret));
  } else if (OB_FAIL(ObUdfUtil::load_function(udf_meta.name_,
                                              dlhandle_,
                                              ObString::make_string(""),
                                              false, /* can't ignore error */
                                              func_origin_))) {
    // change the error code
    ret = OB_CANT_FIND_DL_ENTRY;
    LOG_WARN("load origin function failed", K(ret));
    LOG_USER_ERROR(OB_CANT_FIND_DL_ENTRY, udf_meta.name_.length(), udf_meta.name_.ptr());
  } else if (OB_FAIL(ObUdfUtil::load_function(udf_meta.name_,
                                              dlhandle_,
                                              ObString::make_string("_init"),
                                              false, /* can't ignore error */
                                              func_init_))) {
    LOG_WARN("load init function failed", K(ret));
  } else if (OB_FAIL(ObUdfUtil::load_function(udf_meta.name_,
                                              dlhandle_,
                                              ObString::make_string("_deinit"),
                                              true, /* ignore error */
                                              func_deinit_))) {
    LOG_WARN("load deinit function failed", K(ret));
  } else if (udf_meta.type_ == share::schema::ObUDF::UDFType::FUNCTION) {
    // do nothing
  } else if (OB_FAIL(ObUdfUtil::load_function(udf_meta.name_,
                                              dlhandle_,
                                              ObString::make_string("_clear"),
                                              false, /* ignore error */
                                              func_clear_))) {
    LOG_WARN("load clear function error", K(ret));
  } else if (OB_FAIL(ObUdfUtil::load_function(udf_meta.name_,
                                              dlhandle_,
                                              ObString::make_string("_add"),
                                              false, /* ignore error */
                                              func_add_))) {
    LOG_WARN("load add function error", K(ret));
  }
  if (OB_SUCC(ret)) {
    IGNORE_RETURN udf_meta_.assign(udf_meta);
  }
  return ret;
}

int ObUdfFunction::process_init_func(ObUdfFunction::ObUdfCtx &udf_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(func_init_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the init function is null", K(ret));
  } else {
    char init_msg_buff[OB_MYSQL_ERRMSG_SIZE];
    MEMSET(init_msg_buff, 0, OB_MYSQL_ERRMSG_SIZE);
    bool error = func_init_(&udf_ctx.udf_init_, &udf_ctx.udf_args_, init_msg_buff);
    if (error) {
      ret = OB_CANT_INITIALIZE_UDF;
      LOG_WARN("do init func failed", K(init_msg_buff), K(ret), K(error));
      LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta_.name_.length(), udf_meta_.name_.ptr());
    }
  }
  IGNORE_RETURN ObUdfUtil::print_udf_args_to_log(udf_ctx.udf_args_);
  return ret;
}

void ObUdfFunction::process_deinit_func(ObUdfFunction::ObUdfCtx &udf_ctx) const
{
  if (OB_ISNULL(func_deinit_)) {
    LOG_DEBUG("the deinit function is null");
  } else {
    IGNORE_RETURN func_deinit_(&udf_ctx.udf_init_);
  }
}

int ObNormalUdfFunction::process_origin_func(ObObj &result,
                                     const ObObj *objs_stack,
                                     int64_t param_num,
                                     ObExprCtx &expr_ctx,
                                     ObUdfCtx &udf_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;
  IGNORE_RETURN ObUdfUtil::print_udf_args_to_log(udf_ctx.udf_args_);
  ObUdfCtx *tmp_ctx = (ObUdfCtx *)&udf_ctx;
  //COPY_UDF_CTX_TO_STACK(udf_ctx, tmp_ctx);
  /*
   * step 1. Deep copy the objs. It's not a good idea to assume the udf do not change
   * the input row cell.
   * step 2. Set cell to args.
   * step 3. Invoke the interface defined by user.
   * */
  if (OB_FAIL(ret)) {
    /* do nothing */
  } else if (OB_ISNULL(expr_ctx.calc_buf_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the calc_buf_ is null", K(ret));
  } else if (OB_FAIL(ObUdfUtil::deep_copy_udf_args_cell(*expr_ctx.calc_buf_,
                                                        param_num,
                                                        objs_stack,
                                                        objs))) {
    LOG_WARN("failed to deep copy udf", K(ret));
  } else if (OB_FAIL(ObUdfUtil::set_udf_args(expr_ctx, param_num, objs, tmp_ctx->udf_args_))) {
    LOG_WARN("failed to set udf args", K(ret));
  } else if (OB_FAIL(ObUdfUtil::process_udf_func(udf_meta_.ret_,
                                                 *expr_ctx.calc_buf_,
                                                 udf_ctx.udf_init_,
                                                 tmp_ctx->udf_args_,
                                                 func_origin_,
                                                 result))) {
    LOG_WARN("failed to process udf function", K(ret));
  }
  return ret;
}

int ObAggUdfFunction::process_origin_func(ObIAllocator &allocator,
                                          ObObj &agg_result,
                                          ObUdfCtx &udf_ctx) const
{
  int ret = OB_SUCCESS;
  ObUdfCtx *tmp_ctx = &udf_ctx;
  IGNORE_RETURN ObUdfUtil::print_udf_args_to_log(udf_ctx.udf_args_);
  //COPY_UDF_CTX_TO_STACK(udf_ctx, tmp_ctx);
  if (OB_FAIL(ObUdfUtil::process_udf_func(udf_meta_.ret_,
                                          allocator,
                                          udf_ctx.udf_init_,
                                          tmp_ctx->udf_args_,
                                          func_origin_,
                                          agg_result))) {
    LOG_WARN("failed to process udf function", K(ret));
  }
  return ret;
}

int ObAggUdfFunction::process_add_func(ObExprCtx &expr_ctx,
                                       ObArenaAllocator &allocator,
                                       const ObNewRow &row,
                                       ObUdfCtx &udf_ctx) const
{
  int ret = OB_SUCCESS;
  ObObj *objs = nullptr;
  int64_t cells_count = row.get_count();
  /* do some prepare works: copy udf ctx to this function stack; deep copy this cell; */
  ObUdfCtx *tmp_ctx = &udf_ctx;
  //COPY_UDF_CTX_TO_STACK(udf_ctx, tmp_ctx);
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(func_add_)) {
      ret = OB_NOT_INIT;
      LOG_WARN("the add func is null", K(ret));
    } else if (OB_ISNULL(objs = (ObObj*)allocator.alloc(sizeof(ObObj)*cells_count))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(objs));
    } else {
      /* be careful with the cells' order */
      for (int64_t i = 0; i < cells_count && OB_SUCC(ret); ++i) {
        if (OB_FAIL(ob_write_obj(allocator, row.get_cell(i), objs[i]))) {
          LOG_WARN("failed to copy obj", K(ret));
        }
      }
    }
  }
  /* do add row */
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObUdfUtil::set_udf_args(expr_ctx, cells_count, objs, tmp_ctx->udf_args_))) {
      LOG_WARN("failed to set udf args", K(ret));
    } else if (OB_FAIL(ObUdfUtil::process_add_func(udf_ctx.udf_init_, tmp_ctx->udf_args_, func_add_))) {
      LOG_WARN("failed to process add row", K(ret));
    }
  }

  /* no matter success or failed, reuse this memory */
  allocator.free(objs);
  return ret;
}


int ObAggUdfFunction::process_add_func(common::ObIAllocator &allocator,
                                       const common::ObDatum *datums,
                                       const common::ObIArray<ObExpr *> &exprs,
                                       ObUdfCtx &udf_ctx) const
{
  int ret = OB_SUCCESS;
  CK(NULL != func_add_ && NULL != datums && exprs.count() == udf_ctx.udf_args_.arg_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < exprs.count(); i++) {
    OZ(ObUdfUtil::set_udf_arg(allocator, datums[i], *exprs.at(i), udf_ctx.udf_args_, i));
  }
  OZ(ObUdfUtil::process_add_func(udf_ctx.udf_init_, udf_ctx.udf_args_, func_add_));
  return ret;
}

int ObAggUdfFunction::process_clear_func(ObUdfCtx &udf_ctx) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObUdfUtil::process_clear_func(udf_ctx.udf_init_, func_clear_))) {
    LOG_WARN("failed to process add row", K(ret));
  }
  return ret;
}

ObAggUdfMeta &ObAggUdfMeta::operator=(const ObAggUdfMeta &other)
{
  if (this != &other) {
    udf_meta_ = other.udf_meta_;
    int ret = OB_SUCCESS;
    if (OB_FAIL(udf_attributes_.assign(other.udf_attributes_))) {
      LOG_ERROR("assign searray error", K(ret));
    } else if (OB_FAIL(udf_attributes_types_.assign(other.udf_attributes_types_))) {
      LOG_ERROR("assign searray error", K(ret));
    } else if (OB_FAIL(calculable_results_.assign(other.calculable_results_))) {
      LOG_ERROR("assign searray error", K(ret));
    }
  }
  return *this;
}

OB_SERIALIZE_MEMBER(ObAggUdfMeta,
                    udf_meta_,
                    udf_attributes_,
                    udf_attributes_types_,
                    calculable_results_);

// we will des/ser the sql_calc_ at the operator group
OB_SERIALIZE_MEMBER(ObUdfConstArgs,
                    idx_in_udf_arg_);

}
}

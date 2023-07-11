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
#include "ob_udf_util.h"
#include "observer/mysql/obsm_utils.h"
#include "lib/file/file_directory_utils.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/user_defined_function/ob_user_defined_function.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/ob_i_sql_expression.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace obmysql;


const char *ObUdfUtil::load_function_postfix[5]=
{
  "",
  "_init",
  "_deinit",
  "_clear",
  "_add",
};

int ObUdfUtil::calc_udf_result_type(common::ObIAllocator &allocator,
                                    const ObUdfFunction *udf_func,
                                    const share::schema::ObUDFMeta &udf_meta,
                                    const common::ObIArray<common::ObString> &udf_attributes,
                                    const common::ObIArray<ObExprResType> &udf_attributes_types,
                                    ObExprResType &type,
                                    ObExprResType *param_types,
                                    const int64_t param_num,
                                    ObExprTypeCtx &type_ctx)
{
  int ret = OB_SUCCESS;
  bool init_succ = false;
  /*
   *  基于如下事实：
   *  1. OB计算expr的result type是在ExprDeduceType的时候去被调用的，在resolver阶段/改写阶段/CG阶段
   *  都会被调用。在CG阶段会将ObExprResType的ObAccuracy设置到ObPostExprItem中去，作为计算时候规整
   *  数据的依据。
   *  例如数据在ob_expr_udf的calc_resultN中计算结果为3.1666666666, 最后变成3.1667是依赖于后缀表达式，
   *  后缀表达式规整的依据又是ObAccuracy。
   *  2. 用户UDF的init函数，可以对UDF_INIT结构进行赋值，设定max_length和decimal，对类型的精度
   *  做出了指定。也就要求在最早的计划生成阶段就执行init函数，获得max_length和decimal。
   *  3. 一个物理Expr在执行过程中，udf函数接口要使用的UDF_INIT和UDF_ARG两个结构作为成员变量，Expr
   *  需要将这两个结构以非const的姿态传给了用户编写的函数。例如用户可以在自己的执行udf函数中
   *  对UDF_INIT结构体中的ptr指向的内存进行操作。
   *      从2的角度来说，UDF_INIT和UDF_ARG是静态的属性；从3的角度说他们又是更像是执行ctx。
   *  在这种前提下，Expr必须将UDF_INIT/UDF_ARG作为执行期的ctx组成部分，又必须执行init函数用以
   *  获得精度，使其作为逻辑阶段就必须要有的参数，那么比较好的解决方法就是先调用一次udf的init
   *  和deinit函数，专门用于获得精度。但是这里又有一个问题，就是如果init函数比较废或者是阻塞，
   *  这个计划的生成阶段也就很费了。
   * */
  /* do the user defined init func */
  ObUdfFunction::ObUdfCtx udf_ctx;
  if (OB_FAIL(ObUdfUtil::init_udf_args(allocator,
                                       udf_attributes,
                                       udf_attributes_types,
                                       udf_ctx.udf_args_))) {
    LOG_WARN("failed to set udf args", K(ret));
  } else if (OB_FAIL(udf_func->process_init_func(udf_ctx))) {
    LOG_WARN("do agg init func failed", K(ret));
  } else {
    /* further infomation about scale, precision and length, just see the ob_resolver_utils.cpp */
    init_succ = true;
    type.set_default_collation_type();
    switch(udf_meta.ret_) {
    case share::schema::ObUDF::STRING :
    {
      /* for string and decimal, mysql use char* as result */
      type.set_varchar();
      const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObVarcharType];
      /* OB_MAX_VARCHAR_LENGTH(1024*1024) is bigger than ObUdfUtil::MAX_FIELD_WIDTH(255*3 + 1) */
      if (udf_ctx.udf_init_.max_length > ObUdfUtil::UDF_MAX_FIELD_WIDTH) {
        ret = OB_CANT_INITIALIZE_UDF;
        LOG_WARN("the length is invalid", K(ret), K(udf_ctx.udf_init_.max_length));
        LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta.name_.length(), udf_meta.name_.ptr());
      } else if (udf_ctx.udf_init_.max_length != 0) {
        type.set_length((ObLength)udf_ctx.udf_init_.max_length);
      } else {
        type.set_length(default_accuracy.get_length());
        // TODO muhang.zb how about collation?
        /*
         * type.set_charset_type(charset_type);
         * type.set_collation_type(collation_type);
         * type.set_binary_collation(is_binary);
         * */
      }
      break;
    }
    case share::schema::ObUDF::UDFRetType::DECIMAL :
    {
      /* for string and decimal, mysql use char* as result */
      type.set_number();
      const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObNumberType];
      /*
       * set scale
       * OB_MAX_DECIMAL_SCALE is 30, OB_MAX_DOUBLE_FLOAT_PRECISION is 53,
       * ObUdfUtil::DECIMAL_MAX_STR_LENGTH is 9*9 + 2, the same.
       * */
      if (udf_ctx.udf_init_.decimals > OB_MAX_DECIMAL_SCALE) {
        ret = OB_CANT_INITIALIZE_UDF;
        LOG_WARN("the decimal is invalid", K(ret), K(udf_ctx.udf_init_.decimals));
        LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta.name_.length(), udf_meta.name_.ptr());
      } else if (udf_ctx.udf_init_.decimals != 0) {
        type.set_scale((ObScale)udf_ctx.udf_init_.decimals);
      } else {
        type.set_scale(default_accuracy.get_scale());
      }
      /* set precision */
      if (OB_SUCC(ret)) {
        if (udf_ctx.udf_init_.max_length > OB_MAX_DECIMAL_PRECISION) {
          ret = OB_CANT_INITIALIZE_UDF;
          LOG_WARN("the max length is invalid", K(ret), K(udf_ctx.udf_init_.max_length));
          LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta.name_.length(), udf_meta.name_.ptr());
        } else if (udf_ctx.udf_init_.max_length != 0) {
          type.set_precision((ObPrecision)udf_ctx.udf_init_.max_length);
        } else {
          type.set_precision(default_accuracy.get_precision());
        }
      }
      break;
    }
    case share::schema::ObUDF::UDFRetType::REAL :
    {
      // for real, mysql use double as result
      type.set_double();
      const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObDoubleType];
      /* set scale */
      if (udf_ctx.udf_init_.decimals > OB_MAX_DOUBLE_FLOAT_SCALE) {
        ret = OB_CANT_INITIALIZE_UDF;
        LOG_WARN("the decimal is invalid", K(ret), K(udf_ctx.udf_init_.decimals));
        LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta.name_.length(), udf_meta.name_.ptr());
      } else if (udf_ctx.udf_init_.decimals != 0) {
        type.set_scale((ObScale)udf_ctx.udf_init_.decimals);
      } else {
        type.set_scale(default_accuracy.get_scale());
      }
      /* set precision */
      if (OB_SUCC(ret)) {
        if (udf_ctx.udf_init_.max_length > OB_MAX_DOUBLE_FLOAT_PRECISION) {
          ret = OB_CANT_INITIALIZE_UDF;
          LOG_WARN("the max length is invalid", K(ret), K(udf_ctx.udf_init_.max_length));
          LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta.name_.length(), udf_meta.name_.ptr());
        } else if (udf_ctx.udf_init_.max_length != 0) {
          type.set_precision((ObPrecision)udf_ctx.udf_init_.max_length);
        } else {
          type.set_precision(default_accuracy.get_precision());
        }
      }
      break;
    }
    case share::schema::ObUDF::UDFRetType::INTEGER :
    {
      // for integer, mysql use long long as result
      type.set_int();
      const ObAccuracy &default_accuracy = ObAccuracy::DDL_DEFAULT_ACCURACY[ObIntType];
      if (udf_ctx.udf_init_.max_length > OB_MAX_INTEGER_DISPLAY_WIDTH) {
        ret = OB_CANT_INITIALIZE_UDF;
        LOG_WARN("the max length is invalid", K(ret), K(udf_ctx.udf_init_.max_length),
            K(udf_ctx.udf_init_.decimals));
        LOG_USER_ERROR(OB_CANT_INITIALIZE_UDF, udf_meta.name_.length(), udf_meta.name_.ptr());
      } else if (udf_ctx.udf_init_.max_length != 0) {
        type.set_precision((ObPrecision)udf_ctx.udf_init_.max_length);
        type.set_scale(0);
      } else {
        type.set_precision(default_accuracy.get_precision());
      }
      break;
    }
    default :
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unhandled udf result type", K(ret));
    }
    /* do udf deinit function */
    if (init_succ) {
      IGNORE_RETURN udf_func->process_deinit_func(udf_ctx);
    }
  }
  if (OB_SUCC(ret)) {
    // set parameter calc type for static engine.
    CK(param_num == udf_ctx.udf_args_.arg_count
       && (param_num <= 0 || NULL != udf_ctx.udf_args_.arg_type));
    for (int64_t i = 0; OB_SUCC(ret) && i < param_num; i++) {
      switch (udf_ctx.udf_args_.arg_type[i]) {
        case STRING_RESULT: {
          ObExprResType calc_type;
          calc_type.set_varchar();
          OZ(ObExprOperator::aggregate_charsets_for_string_result(
                  calc_type, param_types + i, 1, type_ctx.get_coll_type()));
          if (OB_SUCC(ret)) {
            param_types[i].set_calc_type(ObVarcharType);
            param_types[i].set_calc_collation_type(calc_type.get_collation_type());
          }
          break;
        }
        case REAL_RESULT: {
          param_types[i].set_calc_type(ObDoubleType);
          break;
        }
        case INT_RESULT: {
          param_types[i].set_calc_type(ObIntType);
          break;
        }

        case DECIMAL_RESULT: {
          param_types[i].set_calc_type(ObNumberType);
          break;
        }

        case ROW_RESULT:
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected type", K(i), K(udf_ctx.udf_args_.arg_type[i]));
        }
      }
    }
  }
  LOG_DEBUG("udf get result type", K(type));
  return ret;
}

int ObUdfUtil::load_so(const common::ObString dl, ObUdfSoHandler &handler)
{
  int ret = OB_SUCCESS;
  ObUdfSoHandler handler_tmp = nullptr;
  //由于ObString没有存最后一个\0 所以还是采用这种方式，不然dlopen会失败。
  int so_name_max_len = OB_MAX_UDF_NAME_LENGTH + ObUdfUtil::UDF_MAX_EXPAND_LENGTH;
  char so_name[OB_MAX_UDF_NAME_LENGTH + ObUdfUtil::UDF_MAX_EXPAND_LENGTH];
  MEMSET(so_name, 0, so_name_max_len);
  MEMCPY(so_name, dl.ptr(), dl.length());
  if (OB_ISNULL(handler_tmp = dlopen(so_name, RTLD_NOW))) {
    /* some error happened */
    ret = OB_CANT_OPEN_LIBRARY;
    char *error_msg = dlerror();
    LOG_WARN("failed to open dl", K(ret), K(error_msg));
  } else {
    /* we got the so handler success */
    handler = handler_tmp;
    LOG_DEBUG("udf get dll handler", K(handler));
  }
  return ret;
}

void ObUdfUtil::unload_so(ObUdfSoHandler &handler)
{
  if (nullptr != handler) {
    dlclose(handler);
  }
}

int ObUdfUtil::deep_copy_udf_args_cell(common::ObIAllocator &allocator, int64_t param_num, const common::ObObj *src_objs, common::ObObj *& dst_objs)
{
  int ret = OB_SUCCESS;
  ObObj *objs_tmp = nullptr;
  if (OB_ISNULL(src_objs) || param_num < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret), K(src_objs), K(param_num));
  } else if (param_num == 0) {
  } else if (OB_ISNULL(objs_tmp = (ObObj*)allocator.alloc(param_num*sizeof(ObObj)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate failed", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_num; ++i) {
    if (OB_FAIL(ob_write_obj(allocator, src_objs[i], objs_tmp[i]))) {
      LOG_WARN("deep copy obj failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    dst_objs = objs_tmp;
  }
  return ret;
}

int ObUdfUtil::init_udf_args(ObIAllocator &allocator,
                             const common::ObIArray<common::ObString> &udf_attributes,
                             const common::ObIArray<ObExprResType> &udf_attributes_types,
                             ObUdfArgs &udf_args)
{
  int ret = OB_SUCCESS;
  if (udf_attributes.count() > INT64_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("too much args", K(ret));
  } else if (udf_attributes.count() != udf_attributes_types.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected input", K(ret), K(udf_attributes.count()), K(udf_attributes_types.count()));
  } else {
    udf_args.arg_count = (unsigned int)udf_attributes.count();
    udf_args.args = (char**)allocator.alloc(sizeof(char*)*udf_args.arg_count);
    udf_args.lengths = (unsigned long *)allocator.alloc(sizeof(unsigned long)*udf_args.arg_count);
    udf_args.arg_type = (enum UdfItemResult*)allocator.alloc(sizeof(enum UdfItemResult)*udf_args.arg_count);
    udf_args.maybe_null = (char*)allocator.alloc(sizeof(char)*udf_args.arg_count);
    udf_args.attributes = (char**)allocator.alloc(sizeof(char*)*udf_args.arg_count);
    udf_args.attribute_lengths = (unsigned long*)allocator.alloc(sizeof(unsigned long)*udf_args.arg_count);
  }
  if (OB_SUCC(ret)) {
    if (udf_args.arg_count !=0 &&
        (OB_ISNULL(udf_args.args)
         || OB_ISNULL(udf_args.lengths)
         || OB_ISNULL(udf_args.arg_type)
         || OB_ISNULL(udf_args.maybe_null)
         || OB_ISNULL(udf_args.attributes)
         || OB_ISNULL(udf_args.attribute_lengths))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(udf_args.arg_count));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < udf_args.arg_count; ++i) {
    udf_args.lengths[i] = 0;
    udf_args.args[i] = nullptr;
    udf_args.attribute_lengths[i] = udf_attributes.at(i).length();
    if (OB_FAIL(convert_ob_type_to_udf_type(udf_attributes_types.at(i).get_type(), udf_args.arg_type[i]))) {
      LOG_WARN("failt to convert ob type to udf type", K(udf_attributes_types.at(i).get_type()), K(udf_attributes_types.at(i).get_calc_type()));
    } else if (udf_args.attribute_lengths[i] > 0) {
      if (OB_ISNULL(udf_args.attributes[i] = (char*)allocator.alloc(udf_args.attribute_lengths[i]))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory", K(ret), K(udf_args.attribute_lengths[i]));
      } else {
        IGNORE_RETURN MEMCPY(udf_args.attributes[i], udf_attributes.at(i).ptr(), udf_attributes.at(i).length());
      }
    } else {
      udf_args.attributes[i] =  nullptr;
    }
    if (OB_SUCC(ret)) {
      udf_args.maybe_null[i] = udf_attributes_types.at(i).has_result_flag(NOT_NULL_FLAG);
    }
  }
  IGNORE_RETURN print_udf_args_to_log(udf_args);
  return ret;
}

int ObUdfUtil::init_const_args(ObIAllocator &allocator,
                               const common::ObIArray<ObUdfConstArgs> &const_results,
                               ObUdfArgs &udf_args,
                               ObExprCtx &expr_ctx) {
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  ObNewRow empty_row;
  const auto &Objs = expr_ctx.phy_plan_ctx_->get_param_store();
  UNUSED(Objs);
  for (int64_t i = 0; i < const_results.count() && OB_SUCC(ret); ++i) {
    const ObUdfConstArgs &const_args = const_results.at(i);
    ObObj value;
    if (const_args.idx_in_udf_arg_ >= udf_args.arg_count || OB_ISNULL(const_args.sql_calc_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid idx", K(ret), K(const_args.idx_in_udf_arg_), K(udf_args.arg_count));
    } else if (OB_FAIL(const_args.sql_calc_->calc(expr_ctx, empty_row, value))) {
      LOG_WARN("failed to calc sql expression", K(ret));
    } else {
      udf_args.args[const_args.idx_in_udf_arg_] = (char*) value.get_data_ptr();
      udf_args.lengths[const_args.idx_in_udf_arg_] = value.get_string_len();
    }
    LOG_DEBUG("calculable value", K(value));
  }
  return ret;
}

int ObUdfUtil::set_udf_arg(common::ObIAllocator &alloc,
                           const ObDatum &src_datum,
                           const ObExpr &expr,
                           ObUdfArgs &udf_args,
                           const int64_t arg_idx)
{
  int ret = OB_SUCCESS;
  ObDatum d;
  CK(arg_idx >= 0 && arg_idx < udf_args.arg_count
     && NULL != udf_args.args
     && NULL != udf_args.lengths
     && NULL != udf_args.arg_type);
  if (OB_SUCC(ret)) {
    if (src_datum.is_null()) {
      udf_args.args[arg_idx] = NULL;
      udf_args.lengths[arg_idx] = 0;
    } else if (OB_FAIL(d.deep_copy(src_datum, alloc))) {
      LOG_WARN("copy datum failed", K(ret));
    } else {
      auto const arg_type = udf_args.arg_type[arg_idx];
      auto const datum_type = expr.datum_meta_.type_;
      if (datum_type != udf_type2obj_type(arg_type)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret), K(arg_type), K(datum_type));
      } else {
        if (DECIMAL_RESULT == arg_type) {
          // pass decimal value as string to udf
          number::ObNumber nmb = d.get_number();
          char buf[OB_CAST_TO_VARCHAR_MAX_LENGTH] = {0};
          void *mem = NULL;
          ObScale in_scale = expr.datum_meta_.scale_;
          int64_t len = 0;
          if (OB_FAIL(nmb.format(buf, sizeof(buf), len, in_scale))) {
            LOG_WARN("fail to format", K(ret), K(nmb));
          } else if (NULL == (mem = alloc.alloc(len))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("allocate memory failed", K(ret), K(len));
          } else {
            MEMCPY(mem, buf, len);
            udf_args.args[arg_idx] = static_cast<char *>(mem);
            udf_args.lengths[arg_idx] = len;
          }
        } else {
          udf_args.args[arg_idx] = const_cast<char *>(d.ptr_);
          udf_args.lengths[arg_idx] = d.len_;
        }
      }
    }
  }
  return ret;
}

int ObUdfUtil::set_udf_args(common::ObExprCtx &expr_ctx, int64_t param_num, common::ObObj *src_objs, ObUdfArgs &udf_args)
{
  int ret = OB_SUCCESS;
  /*
   * mysql需要各种缓冲区将数据拷贝到临时内存中给予UDF去使用，
   * 由于我们在前面已经将传入的ObObj全部都深拷过一次了，所以
   * 这些内存直接给UDF拿去用就好了不需要再拷贝一次了。
   * 差别在于mysql的int/double这段区域是连续的，如果UDF利用了
   * 这个隐藏条件，可能会导致core。
   * */
  if (param_num != udf_args.arg_count) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input", K(ret), K(param_num), K(udf_args.arg_count));
  }
  for (uint64_t i = 0; i < udf_args.arg_count && OB_SUCC(ret); i++) {
    udf_args.args[i] = 0;
    switch (udf_args.arg_type[i]) {
      case STRING_RESULT:
      case DECIMAL_RESULT: {
        if (src_objs[i].is_null()) {
          udf_args.lengths[i] = 0;
        } else {
          if (!src_objs[i].is_string_type()) {
            EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
            if (OB_FAIL(ObObjCaster::to_type(ObVarcharType, cast_ctx, src_objs[i], src_objs[i]))) {
              LOG_WARN("failed to cast", K(ret));
            }
          }
          if (OB_SUCC(ret)) {
            udf_args.args[i] = (char*) (src_objs[i].get_data_ptr());
            udf_args.lengths[i] = src_objs[i].get_string_len();
          }
        }
        break;
      }
      case INT_RESULT:
        /* int, udf should not use lengths */
        if (OB_UNLIKELY(ObIntTC != src_objs[i].get_type_class())) {
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
          if (OB_FAIL(ObObjCaster::to_type(ObIntType, cast_ctx, src_objs[i], src_objs[i]))) {
            LOG_WARN("failed to cast", K(ret));
          }
        }
        if (OB_SUCC(ret) && !src_objs[i].is_null()) {
          int64_t num = src_objs[i].get_int();
          src_objs[i].set_int(num);
          udf_args.args[i] = (char*)src_objs[i].get_data_ptr();
        }
        break;
      case REAL_RESULT:
        /* double, udf should not use lengths */
        if (!src_objs[i].is_double()) {
          EXPR_DEFINE_CAST_CTX(expr_ctx, CM_WARN_ON_FAIL);
          if (OB_FAIL(ObObjCaster::to_type(ObDoubleType, cast_ctx, src_objs[i], src_objs[i]))) {
            LOG_WARN("failed to cast", K(ret));
          }
        }
        if (OB_SUCC(ret) && !src_objs[i].is_null()) {
          udf_args.args[i] = (char*)src_objs[i].get_data_ptr();
        }
        break;
      case ROW_RESULT:
      default:
        // This case should never be chosen
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected type", K(ret), K(udf_args.arg_type[i]));
        break;
    }
  }
  IGNORE_RETURN ObUdfUtil::print_udf_args_to_log(udf_args);
  return ret;
}

int ObUdfUtil::process_udf_func(share::schema::ObUDF::UDFRetType ret_type,
                                common::ObIAllocator &allocator,
                                ObUdfInit &udf_init,
                                ObUdfArgs &udf_args,
                                const ObUdfFuncAny func_origin_,
                                common::ObObj &result)
{
  int ret = OB_SUCCESS;
  switch (ret_type) {
  case share::schema::ObUDF::STRING: {
      if (OB_FAIL(ObUdfUtil::process_str(allocator, udf_init, udf_args, func_origin_, result))) {
        LOG_WARN("failed to process str", K(ret));
      }
      break;
    }
  case share::schema::ObUDF::DECIMAL: {
      if (OB_FAIL(ObUdfUtil::process_dec(allocator, udf_init, udf_args, func_origin_, result))) {
        LOG_WARN("failed to process dec", K(ret));
      }
      break;
    }
  case share::schema::ObUDF::INTEGER: {
      if (OB_FAIL(ObUdfUtil::process_int(allocator, udf_init, udf_args, func_origin_, result))) {
        LOG_WARN("failed to process int", K(ret));
      }
      break;
    }
  case share::schema::ObUDF::REAL: {
      if (OB_FAIL(ObUdfUtil::process_real(allocator, udf_init, udf_args, func_origin_, result))) {
        LOG_WARN("failed to process real", K(ret));
      }
      break;
    }
  default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected udf ret type", K(ret), K(ret_type));
    }
  }
  return ret;
}

int ObUdfUtil::process_str(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, ObObj &result)
{
  int ret = OB_SUCCESS;
  unsigned long res_length = UDF_MAX_FIELD_WIDTH;
  unsigned char is_null_tmp = 0;
  unsigned char error = 0;
  char *str_for_user = (char*)allocator.alloc(UDF_MAX_FIELD_WIDTH);
  IGNORE_RETURN MEMSET(str_for_user, 0, UDF_MAX_FIELD_WIDTH);
  ObUdfFuncString func_str = (ObUdfFuncString)func;
  char *res = func_str(&udf_init, &udf_args, str_for_user, &res_length,
                       &is_null_tmp, &error);
  if (error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happened while do the user's code", K(ret));
  } else if (nullptr == res || is_null_tmp) {
    /* set null to result */
    result.set_null();
  } else if (res_length >= UDF_MAX_FIELD_WIDTH) {
    /*
     * if res_length is bigger than MAX_FIELD_WIDTH, it must be some error happened.
     * */
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the string is too large, maybe some error happen in user defined function", K(ret), K(res_length));
  } else {
    //result.set_char_value(res, (ObString::obstr_size_t)res_length);
    result.set_string(ObVarcharType, res, (ObString::obstr_size_t)res_length);
    result.set_default_collation_type();
  }
  return ret;
}

int ObUdfUtil::process_dec(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, ObObj &result)
{
  int ret = OB_SUCCESS;
  unsigned long res_length = UDF_DECIMAL_MAX_STR_LENGTH;
  unsigned char is_null_tmp = 0;
  unsigned char error = 0;
  char *buf_for_user = (char*)allocator.alloc(UDF_DECIMAL_MAX_STR_LENGTH);
  IGNORE_RETURN MEMSET(buf_for_user, 0, UDF_DECIMAL_MAX_STR_LENGTH);
  ObUdfFuncString func_str = (ObUdfFuncString)func;

  common::number::ObNumber num;
  ObPrecision res_precision = -1;
  ObScale res_scale = -1;

  char *res = func_str(&udf_init, &udf_args, buf_for_user, &res_length,
                       &is_null_tmp, &error);
  if (error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happened while do the user's code", K(ret));
  } else if (nullptr == res || is_null_tmp) {
    /* set null to result */
    result.set_null();
  } else if (res_length >= UDF_DECIMAL_MAX_STR_LENGTH) {
    /*
     * if res_length is bigger than DECIMAL_MAX_STR_LENGTH, it must be some error happened.
     * */
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the string is too large, maybe some error happen in user defined function", K(ret), K(res_length));
  } else if (OB_FAIL(num.from(res, (long int)res_length, allocator, &res_precision, &res_scale))) {
    LOG_WARN("fail to convert char* to decimal/obnumber", K(ret));
  } else {
    result.set_number(num);
  }
  return ret;
}

int ObUdfUtil::process_int(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  unsigned char is_null_tmp = 0;
  unsigned char error = 0;

  ObUdfFuncLonglong func_int = (ObUdfFuncLonglong)func;
  long long int tmp = func_int(&udf_init, &udf_args, &is_null_tmp, &error);
  if (error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happened while do the user's code", K(ret));
  } else if (is_null_tmp) {
    /* set null to result */
    result.set_null();
  } else {
    result.set_int(tmp);
  }
  return ret;
}

int ObUdfUtil::process_real(common::ObIAllocator &allocator, ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAny func, ObObj &result)
{
  int ret = OB_SUCCESS;
  UNUSED(allocator);
  unsigned char is_null_tmp = 0;
  unsigned char error = 0;

  ObUdfFuncDouble func_double = (ObUdfFuncDouble)func;
  double tmp = func_double(&udf_init, &udf_args, &is_null_tmp, &error);
  if (error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happened while do the user's code", K(ret));
  } else if (is_null_tmp) {
    /* set null to result */
    result.set_null();
  } else {
    result.set_double(tmp);
  }
  return ret;
}

int ObUdfUtil::process_add_func(ObUdfInit &udf_init, ObUdfArgs &udf_args, const ObUdfFuncAdd func)
{
  int ret = OB_SUCCESS;
  unsigned char is_null_tmp = 0;
  unsigned char error = 0;
  IGNORE_RETURN func(&udf_init, &udf_args, &is_null_tmp, &error);
  if (error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happened while do the user's code", K(ret));
  }
  UNUSED(is_null_tmp);
  return ret;
}

int ObUdfUtil::process_clear_func(ObUdfInit &udf_init, const ObUdfFuncClear func)
{
  int ret = OB_SUCCESS;
  unsigned char error = 0;
  unsigned char is_null_tmp = 0;
  IGNORE_RETURN func(&udf_init, &is_null_tmp, &error);
  if (error) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error happened while do the user's code", K(ret), K(error), K(is_null_tmp));
  }
  return ret;
}

void ObUdfUtil::construct_udf_args(ObUdfArgs &args)
{
  args.arg_count = 0;
  args.arg_type = nullptr;
  args.args = nullptr;
  args.attribute_lengths = nullptr;
  args.attributes = nullptr;
  args.extension = nullptr;
  args.maybe_null = nullptr;
}

void ObUdfUtil::construct_udf_init(ObUdfInit &init)
{
  init.const_item = 0;
  init.decimals = 0;
  init.extension = nullptr;
  init.max_length = 0;
  init.maybe_null = 0;
  init.ptr = nullptr;
}

const char* ObUdfUtil::result_type_name(enum UdfItemResult result)
{
  int64_t offset = 0;
  if (INVALID_RESULT == result) {
    offset = 0;
  } else {
    offset = (int64_t)result;
  }
  static const char *result_string[7] = {
      "INVALID_RESULT",
      "STRING_RESULT",
      "REAL_RESULT",
      "INT_RESULT",
      "ROW_RESULT",
      "DECIMAL_RESULT",
  };
  return result_string[offset + 1];
}

void ObUdfUtil::print_udf_args_to_log(const ObUdfArgs &args)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObString,16> args_strings;
  ObSEArray<ObString,16> atts_strings;
  ObSEArray<int64_t,16> maybe_nulls;
  ObSEArray<ObString, 16> arg_type_strings;
  for (int64_t i = 0; i < args.arg_count && OB_SUCC(ret); ++i) {
    if (args.lengths[i] > 0 && OB_FAIL(args_strings.push_back(ObString(args.lengths[i], args.args[i])))) {
      LOG_WARN("push back failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      if (args.attribute_lengths[i] > 0 && OB_FAIL(atts_strings.push_back(ObString(args.attribute_lengths[i],
                                                                                   args.attributes[i])))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(maybe_nulls.push_back(args.maybe_null[i] == 1 ? 1 : 0))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(arg_type_strings.push_back(ObString(STRLEN(result_type_name(args.arg_type[i])), result_type_name(args.arg_type[i]))))) {
        LOG_WARN("push back failed", K(ret));
      }
    }
  }
  LOG_DEBUG("UDF ARGS", K(ret), K(args.arg_count), K(args_strings), K(atts_strings), K(maybe_nulls), K(arg_type_strings), K(lbt()));
}

void ObUdfUtil::print_udf_init_to_log(const ObUdfInit &init)
{
  LOG_DEBUG("UDF INIT", K(init.maybe_null), K(init.decimals), K(init.max_length), K(init.ptr), K(init.const_item));
}

void ObUdfUtil::assign_udf_args(const ObUdfArgs &src_args, ObUdfArgs &dst_args)
{
  dst_args.arg_count = src_args.arg_count;
  dst_args.extension = src_args.extension;
  /*dst_args.arg_type = src_args.arg_type;
  dst_args.args = src_args.args;
  dst_args.attribute_lengths = src_args.attribute_lengths;
  dst_args.attributes = src_args.attributes;
  dst_args.extension = src_args.extension;
  dst_args.maybe_null = src_args.maybe_null;*/
}

void ObUdfUtil::assign_udf_init(const ObUdfInit &src_init, ObUdfInit &dst_init)
{
  dst_init.const_item = src_init.const_item;
  dst_init.decimals = src_init.decimals;
  dst_init.extension = src_init.extension;
  dst_init.max_length = src_init.max_length;
  dst_init.maybe_null = src_init.maybe_null;
  dst_init.ptr = src_init.ptr;
}

int ObUdfUtil::is_udf_ctx_valid(const ObUdfArgs &udf_args, const ObUdfInit &udf_init)
{
  int ret = OB_SUCCESS;
  UNUSED(udf_init);
  if (udf_args.arg_type == nullptr
      || udf_args.args == nullptr
      || udf_args.lengths == nullptr
      || udf_args.maybe_null == nullptr
      || udf_args.attributes == nullptr
      || udf_args.attribute_lengths == nullptr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("some field is null", K(udf_args.arg_type), K(udf_args.args), K(udf_args.maybe_null),
        K(udf_args.attributes), K(udf_args.attribute_lengths), K(udf_args.arg_count));
  } else {
    for (int64_t i = 0; i < udf_args.arg_count && OB_SUCC(ret); ++i) {
      if (udf_args.args[i] == nullptr
          || udf_args.attributes[i] == nullptr) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("some field is null", K(ret), K(udf_args.args[i]), K(udf_args.attributes[i]));
      }
    }
  }
  return ret;
}

int ObUdfUtil::convert_ob_type_to_udf_type(common::ObObjType ob_type, UdfItemResult &udf_type)
{
  int ret = OB_SUCCESS;
  //mysql 5.6 type.
  obmysql::EMySQLFieldType mysql_type;
  uint16_t flags;
  ObScale num_decimals;
  if (OB_FAIL(ObSMUtils::get_mysql_type(ob_type, mysql_type, flags, num_decimals))) {
    LOG_WARN("get mysql type failed", K(ret));
  } else if (OB_FAIL(convert_mysql_type_to_udf_type(mysql_type, udf_type))) {
    LOG_WARN("get udf type failed", K(ret));
  }
  LOG_DEBUG("udf type change", K(ob_type), K(mysql_type), K(udf_type));
  return ret;
}

int ObUdfUtil::convert_mysql_type_to_udf_type(const obmysql::EMySQLFieldType &mysql_type, UdfItemResult &udf_type)
{
  int ret = OB_SUCCESS;
  /*
   * mysql的表达式item中，似乎每个种类返回硬编码的result type和field type.
   *
   * */
  switch (mysql_type) {
    case EMySQLFieldType::MYSQL_TYPE_TINY:
    case EMySQLFieldType::MYSQL_TYPE_SHORT:
    case EMySQLFieldType::MYSQL_TYPE_INT24:
    case EMySQLFieldType::MYSQL_TYPE_LONG:
    case EMySQLFieldType::MYSQL_TYPE_LONGLONG:
      udf_type = INT_RESULT;
      break;
    case EMySQLFieldType::MYSQL_TYPE_DECIMAL:
    case EMySQLFieldType::MYSQL_TYPE_NEWDECIMAL:
    case EMySQLFieldType::MYSQL_TYPE_OB_NUMBER_FLOAT:
      udf_type = DECIMAL_RESULT;
      break;
    case EMySQLFieldType::MYSQL_TYPE_FLOAT:
    case EMySQLFieldType::MYSQL_TYPE_DOUBLE:
      udf_type = REAL_RESULT;
      break;
    case EMySQLFieldType::MYSQL_TYPE_VARCHAR:
    case EMySQLFieldType::MYSQL_TYPE_VAR_STRING:
    case EMySQLFieldType::MYSQL_TYPE_STRING:
    case EMySQLFieldType::MYSQL_TYPE_OB_NCHAR:
    case EMySQLFieldType::MYSQL_TYPE_OB_NVARCHAR2:
    case EMySQLFieldType::MYSQL_TYPE_OB_UROWID:
      udf_type = STRING_RESULT;
      break;
    case EMySQLFieldType::MYSQL_TYPE_YEAR:
      udf_type = INT_RESULT;
      break;
    case EMySQLFieldType::MYSQL_TYPE_TIMESTAMP:
    case EMySQLFieldType::MYSQL_TYPE_DATE:
    case EMySQLFieldType::MYSQL_TYPE_TIME:
    case EMySQLFieldType::MYSQL_TYPE_DATETIME:
    case EMySQLFieldType::MYSQL_TYPE_NEWDATE:
    case EMySQLFieldType::MYSQL_TYPE_BIT:
    //case MYSQL_TYPE_TIMESTAMP2:
    //case MYSQL_TYPE_DATETIME2:
    //case MYSQL_TYPE_TIME2:
    //case MYSQL_TYPE_JSON:
    case EMySQLFieldType::MYSQL_TYPE_ENUM:
    case EMySQLFieldType::MYSQL_TYPE_SET:
    case EMySQLFieldType::MYSQL_TYPE_GEOMETRY:
    case EMySQLFieldType::MYSQL_TYPE_NULL:
    case EMySQLFieldType::MYSQL_TYPE_TINY_BLOB:
    case EMySQLFieldType::MYSQL_TYPE_BLOB:
    case EMySQLFieldType::MYSQL_TYPE_MEDIUM_BLOB:
    case EMySQLFieldType::MYSQL_TYPE_LONG_BLOB:
    case EMySQLFieldType::MYSQL_TYPE_JSON:
      udf_type = STRING_RESULT;
      break;
    default:
      udf_type = INVALID_RESULT;
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unhandled mysql type", K(ret));
  }
  return ret;
}

ObObjType ObUdfUtil::udf_type2obj_type(const UdfItemResult udf_type)
{
  static ObObjType mapping_table[] = {
    ObVarcharType,
    ObDoubleType,
    ObIntType,
    ObNullType,
    ObNumberType
  };
  ObObjType obj_type = ObNullType;
  if (udf_type >= 0 && udf_type < ARRAYSIZEOF(mapping_table)) {
    obj_type = mapping_table[udf_type];
  }
  return obj_type;
}

}
}

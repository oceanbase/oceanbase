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

#ifndef OB_USER_DEFINED_FUNCTION_H_
#define OB_USER_DEFINED_FUNCTION_H_

#include "ob_udf_registration_types.h"
#include "share/schema/ob_udf.h"
#include "sql/engine/expr/ob_expr_res_type.h"
#include "sql/engine/expr/ob_expr.h"


namespace oceanbase
{
namespace common
{
struct ObExprCtx;
}

namespace sql
{

#define COPY_UDF_CTX_TO_STACK(SRC_UDF_CTX, TMP_UDF_CTX)                                                                                                       \
    tmp_ctx = (ObUdfCtx*)alloca(sizeof(ObUdfCtx));                                                                                                            \
    if (OB_ISNULL(tmp_ctx)) {                                                                                                                                 \
      ret = OB_ALLOCATE_MEMORY_FAILED;                                                                                                                        \
      LOG_WARN("failed to allocate memory from stack", K(ret));                                                                                               \
    } else {                                                                                                                                                  \
      IGNORE_RETURN ObUdfUtil::assign_udf_args((SRC_UDF_CTX).udf_args_, (TMP_UDF_CTX)->udf_args_);                                                            \
      if (OB_FAIL(ObUdfUtil::is_udf_ctx_valid((SRC_UDF_CTX).udf_args_, (SRC_UDF_CTX).udf_init_))) {                                                           \
        LOG_WARN("the udf ctx is invalid", K(ret));                                                                                                           \
      } else {                                                                                                                                                \
        (TMP_UDF_CTX)->udf_args_.args = (char**)alloca(sizeof(char*)*(SRC_UDF_CTX).udf_args_.arg_count);                                                      \
        (TMP_UDF_CTX)->udf_args_.lengths = (unsigned long *)alloca(sizeof(unsigned long)*(SRC_UDF_CTX).udf_args_.arg_count);                                  \
        (TMP_UDF_CTX)->udf_args_.arg_type = (enum UdfItemResult*)alloca(sizeof(enum UdfItemResult)*(SRC_UDF_CTX).udf_args_.arg_count);                        \
        (TMP_UDF_CTX)->udf_args_.maybe_null = (char*)alloca(sizeof(char)*(SRC_UDF_CTX).udf_args_.arg_count);                                                  \
        (TMP_UDF_CTX)->udf_args_.attributes = (char**)alloca(sizeof(char*)*(SRC_UDF_CTX).udf_args_.arg_count);                                                \
        (TMP_UDF_CTX)->udf_args_.attribute_lengths = (unsigned long*)alloca(sizeof(unsigned long)*(SRC_UDF_CTX).udf_args_.arg_count);                         \
        for (int64_t i = 0; OB_SUCC(ret) && i < (SRC_UDF_CTX).udf_args_.arg_count; ++i) {                                                                     \
          (TMP_UDF_CTX)->udf_args_.lengths[i] = (SRC_UDF_CTX).udf_args_.lengths[i];                                                                           \
          (TMP_UDF_CTX)->udf_args_.attribute_lengths[i] = (SRC_UDF_CTX).udf_args_.attribute_lengths[i];                                                       \
          (TMP_UDF_CTX)->udf_args_.arg_type[i] = (SRC_UDF_CTX).udf_args_.arg_type[i];                                                                         \
          if ((SRC_UDF_CTX).udf_args_.lengths[i] > 0) {                                                                                                       \
            (TMP_UDF_CTX)->udf_args_.args[i] = (char*)alloca((SRC_UDF_CTX).udf_args_.lengths[i]);                                                             \
            IGNORE_RETURN MEMCPY((TMP_UDF_CTX)->udf_args_.args[i], (SRC_UDF_CTX).udf_args_.args[i], (SRC_UDF_CTX).udf_args_.lengths[i]);                      \
          } else {                                                                                                                                            \
            (TMP_UDF_CTX)->udf_args_.args[i] =  nullptr;                                                                                                      \
          }                                                                                                                                                   \
          if ((SRC_UDF_CTX).udf_args_.attribute_lengths[i] > 0) {                                                                                             \
            (TMP_UDF_CTX)->udf_args_.attributes[i] = (char*)alloca((SRC_UDF_CTX).udf_args_.attribute_lengths[i]);                                             \
            IGNORE_RETURN MEMCPY((TMP_UDF_CTX)->udf_args_.attributes[i], (SRC_UDF_CTX).udf_args_.attributes[i], (SRC_UDF_CTX).udf_args_.attribute_lengths[i]);\
          } else {                                                                                                                                            \
            (TMP_UDF_CTX)->udf_args_.attributes[i] =  nullptr;                                                                                                \
          }                                                                                                                                                   \
        }                                                                                                                                                     \
      }                                                                                                                                                       \
    }

class ObAggUdfMeta;
class ObSqlExpression;

struct ObUdfConstArgs {
  OB_UNIS_VERSION_V(1);
public:
  ObUdfConstArgs() : sql_calc_(nullptr), idx_in_udf_arg_(common::OB_INVALID_INDEX) {};
  virtual ~ObUdfConstArgs() = default;
  ObUdfConstArgs &operator=(const ObUdfConstArgs &other)
  {
    sql_calc_ = other.sql_calc_;
    idx_in_udf_arg_ = other.idx_in_udf_arg_;
    return *this;
  }
  TO_STRING_KV(K_(idx_in_udf_arg));
  ObSqlExpression *sql_calc_;
  int64_t idx_in_udf_arg_;
};

class ObUdfFunction
{
private :
  static const int OB_MYSQL_ERRMSG_SIZE = 512;
public:
  friend class ObGetUdfFunctor;
  friend class ObResetUdfFunctor;
  friend class ObForceDelUdfFunctor;
  enum ObUdfState
  {
    UDF_UNINITIALIZED,
    UDF_INIT,
    UDF_EXE,
    UDF_DEINIT,
  };
  class ObUdfCtx
  {
  public:
    ObUdfCtx() : state_(UDF_UNINITIALIZED), udf_init_(), udf_args_() {}
    virtual ~ObUdfCtx() = default;
    ObUdfState state_;
    ObUdfInit udf_init_;
    ObUdfArgs udf_args_;
  };
public:
  ObUdfFunction() :
    udf_meta_(),
    dlhandle_(nullptr),
    func_origin_(nullptr),
    func_init_(nullptr),
    func_deinit_(nullptr),
    func_clear_(nullptr),
    func_add_(nullptr)
  {}
  virtual ~ObUdfFunction();

  // try to load the .so to ob
  virtual int init(const share::schema::ObUDFMeta &udf_meta);
  virtual int process_init_func(ObUdfFunction::ObUdfCtx &udf_ctx) const;
  virtual void process_deinit_func(ObUdfFunction::ObUdfCtx &udf_ctx) const;

public:
  share::schema::ObUDFMeta udf_meta_;
  ObUdfSoHandler dlhandle_;
  ObUdfFuncAny func_origin_;
  ObUdfFuncInit func_init_;
  ObUdfFuncDeinit func_deinit_;
  //helper function for aggregation udf
  ObUdfFuncClear func_clear_;
  ObUdfFuncAdd func_add_;
};

class ObNormalUdfFunction : public ObUdfFunction
{
private:

public:
  ObNormalUdfFunction() = default;
  virtual ~ObNormalUdfFunction() = default;
  int process_origin_func(common::ObObj &result,
                          const common::ObObj *objs_stack,
                          int64_t param_num,
                          common::ObExprCtx &expr_ctx,
                          ObUdfCtx &udf_ctx) const;


};


class ObAggUdfFunction : public ObUdfFunction
{
private:

public:
  ObAggUdfFunction() = default;
  virtual ~ObAggUdfFunction() = default;

  int process_origin_func(common::ObIAllocator &allocator,
                          common::ObObj &result,
                          ObUdfCtx &udf_ctx) const;

  int process_add_func(common::ObExprCtx &expr_ctx,
                       common::ObArenaAllocator &allocator,
                       const common::ObNewRow &row,
                       ObUdfCtx &udf_ctx) const;

  int process_add_func(common::ObIAllocator &allocator,
                       const common::ObDatum *datums,
                       const common::ObIArray<sql::ObExpr *> &exprs,
                       ObUdfCtx &udf_ctx) const;

  int process_clear_func(ObUdfCtx &udf_ctx) const;

};

class ObAggUdfMeta
{
  OB_UNIS_VERSION_V(1);
public:
  ObAggUdfMeta() : udf_meta_(), udf_attributes_(), udf_attributes_types_(),
  calculable_results_() {}
  explicit ObAggUdfMeta(const share::schema::ObUDFMeta &meta) : udf_meta_(meta),
      udf_attributes_(), udf_attributes_types_(), calculable_results_() {}
  virtual ~ObAggUdfMeta() = default;
  ObAggUdfMeta &operator=(const ObAggUdfMeta &other);
  TO_STRING_KV(K_(udf_meta),
               K_(udf_attributes),
               K_(udf_attributes_types),
               K_(calculable_results));
  share::schema::ObUDFMeta udf_meta_; /* all the info we need about udf*/
  common::ObSEArray<common::ObString, 16> udf_attributes_; /* udf's input, args' name */
  common::ObSEArray<ObExprResType, 16> udf_attributes_types_; /* udf's input, aatribute type */
  common::ObSEArray<ObUdfConstArgs, 16> calculable_results_; /* const input expr' param idx */
};

class ObAggUdfExeUnit
{
public:
  ObAggUdfExeUnit (ObAggUdfFunction *agg_func, ObUdfFunction::ObUdfCtx *udf_ctx) :
    agg_func_(agg_func), udf_ctx_(udf_ctx) {}
  ObAggUdfExeUnit () : agg_func_(nullptr), udf_ctx_(nullptr) {}
  virtual ~ObAggUdfExeUnit() = default;
  ObAggUdfFunction *agg_func_;
  ObUdfFunction::ObUdfCtx *udf_ctx_;
};

class ObNormalUdfExeUnit
{
public:
  ObNormalUdfExeUnit (ObNormalUdfFunction *normal_func, ObUdfFunction::ObUdfCtx *udf_ctx) :
    normal_func_(normal_func), udf_ctx_(udf_ctx) {}
  ObNormalUdfExeUnit () : normal_func_(nullptr), udf_ctx_(nullptr) {}
  virtual ~ObNormalUdfExeUnit() = default;
  const ObNormalUdfFunction *normal_func_;
  ObUdfFunction::ObUdfCtx *udf_ctx_;
};

}
}

#endif

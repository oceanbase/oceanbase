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

#ifndef _OB_EXPR_CAST_H_
#define _OB_EXPR_CAST_H_

#include "common/object/ob_obj_type.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "pl/ob_pl_type.h"

namespace oceanbase
{
namespace pl
{
class ObPLCollection;
class ObCollectionType;
}
namespace sql
{

// The length of array need to be equal to the number of types defined at ObObjType
static const int32_t CAST_STRING_DEFUALT_LENGTH[ObMaxType + 1] = {
  0, //null
  4, //tinyint
  6, //smallint
  9, //medium
  11,//int32
  20,//int
  3,//utinyint
  5,//usmallint
  8,//umedium
  10,//uint32
  20,//uint
  12,//float
  23,//double  here we set it larger than mysql, mysql is 22, sometimes it is 23,for safely.we set 23.
  12,//ufloat
  23,//udouble
  11,//decimal
  11,//decimal
  19,//datetime
  19,//timestamp
  10,//date
  10,//time
  4,//year
  1,//varchar
  1,//char
  1,//binary
  0,//extend
  0,//unknown
  1,//tinytext  text share default length with varchar
  1,//text
  1,//mediumtext
  1,//longtext
  64,//bit
  1,//enum
  1,//set
  1,//enuminner
  1,//setinner
  25,//timestampTZ  timestamp + time zone('+00:00')
  25,//timestampLTZ
  19,//timestampNano  timestamp + nanosecond scale
  1,//raw
  7,//intervalYM  yyyy-mm
  19,//intervalDS  ddd hh:mm:ss.fractionsecond(6)
  12,//numberfloat  same with float
  1,//nvarchar2  share default length with varchar
  1,//nchar  share default length with char
  1,//urowid
  1,//lob
  1,//json
  1,//geometry
  1,//udt
  11, // decimal int
  1,//collection
  10,//mysql date
  19,//mysql datetime
  1,//roaringbitmap
  0//max, invalid type, or count of obj type
};

static_assert(common::ObMaxType + 1 == sizeof(CAST_STRING_DEFUALT_LENGTH) / sizeof(int32_t),
  "Please keep the length of CAST_STRING_DEFUALT_LENGTH must equal to the number of types");
extern int cast_eval_arg_batch(const ObExpr &, ObEvalCtx &, const ObBitVector &, const int64_t);
class ObExprCast: public ObFuncExprOperator
{
  OB_UNIS_VERSION_V(1);
  const static int32_t OB_LITERAL_MAX_INT_LEN = 21;
public:
  ObExprCast();
  explicit  ObExprCast(common::ObIAllocator &alloc);
  virtual ~ObExprCast() {};

  struct CastMultisetExtraInfo : public ObIExprExtraInfo
  {
    OB_UNIS_VERSION(1);
  public:
    CastMultisetExtraInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
        : ObIExprExtraInfo(alloc, type),
        pl_type_(pl::ObPLType::PL_INVALID_TYPE),
        not_null_(false),
        elem_type_(),
        capacity_(common::OB_INVALID_SIZE),
        udt_id_(common::OB_INVALID_ID)
    {
    }
    virtual ~CastMultisetExtraInfo() { }
    virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

    pl::ObPLType pl_type_;
    bool not_null_;
    common::ObDataType elem_type_;
    int64_t capacity_;
    uint64_t udt_id_;
  };

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;

  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  // extra_serialize_ == 1 : is implicit cast
  void set_implicit_cast(bool v) { extra_serialize_ =  v ? 1 : 0; }

  static int eval_cast_multiset(const sql::ObExpr &expr,
                                sql::ObEvalCtx &ctx,
                                sql::ObDatum &res_datum);
  virtual int is_valid_for_generated_column(const ObRawExpr*expr, const common::ObIArray<ObRawExpr *> &exprs, bool &is_valid) const;
  static int get_cast_type(const bool enable_decimal_int,
                           const ObExprResType param_type2,
                           const ObCastMode cast_mode,
                           ObExprResType &dst_type);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  int get_explicit_cast_cm(const ObExprResType &src_type,
                           const ObExprResType &dst_type,
                           const ObSQLSessionInfo &session,
                           ObSQLMode sql_mode,
                           const ObRawExpr &cast_raw_expr,
                           common::ObCastMode &cast_mode) const;
  bool check_cast_allowed(const common::ObObjType orig_type,
                          const common::ObCollationType orig_cs_type,
                          const common::ObObjType expect_type,
                          const common::ObCollationType expect_cs_type,
                          const bool is_explicit_cast) const;

  int cg_cast_multiset(ObExprCGCtx &op_cg_ctx,
                       const ObRawExpr &raw_expr,
                       ObExpr &rt_expr) const;
  static int get_subquery_iter(const sql::ObExpr &expr,
                               sql::ObEvalCtx &ctx,
                               ObExpr **&subquery_row,
                               ObEvalCtx *&subquery_ctx,
                               ObSubQueryIterator *&iter);
  static int fill_element(const sql::ObExpr &expr,
                          ObEvalCtx &ctx,
                          sql::ObDatum &res_datum,
                          pl::ObPLCollection *coll,
                          pl::ObPLINS *ns,
                          const pl::ObCollectionType *collection_type,
                          ObExpr **subquery_row,
                          ObEvalCtx *subquery_ctx,
                          ObSubQueryIterator *subquery_iter);
  static int construct_collection(const sql::ObExpr &expr,
                                  ObEvalCtx &ctx,
                                  sql::ObDatum &res_datum,
                                  ObExpr **subquery_row,
                                  ObEvalCtx *subquery_ctx,
                                  ObSubQueryIterator *subquery_iter);
  int adjust_udt_cast_type(const ObExprResType &src_type, ObExprResType &dst_type, ObExprTypeCtx &type_ctx) const;

private:
  int get_cast_string_len(ObExprResType &type1,
                          ObExprResType &type2,
                          common::ObExprTypeCtx &type_ctx,
                          int32_t &res_len,
                          int16_t &length_semantics,
                          common::ObCollationType conn,
                          common::ObCastMode cast_mode) const;
  int get_cast_inttc_len(ObExprResType &type1,
                         ObExprResType &type2,
                         common::ObExprTypeCtx &type_ctx,
                         int32_t &res_len,
                         int16_t &length_semantics,
                         common::ObCollationType conn,
                         common::ObCastMode cast_mode) const;
  int do_implicit_cast(common::ObExprCtx &expr_ctx,
                       const common::ObCastMode &cast_mode,
                       const common::ObObjType &dst_type,
                       const common::ObObj &src_obj,
                       common::ObObj &dst_obj) const;
  // disallow copy
  ObExprCast(const ObExprCast &other);
  ObExprCast &operator=(const ObExprCast &other);
};

}
}
#endif  /* _OB_EXPR_CAST_H_ */

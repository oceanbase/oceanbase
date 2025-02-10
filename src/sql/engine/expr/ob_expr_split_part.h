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

#ifndef _OB_SQL_EXPR_SPLIT_PART_H_
#define _OB_SQL_EXPR_SPLIT_PART_H_
#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
class ObExprSplitPart : public ObStringExprOperator
{
public:
  explicit ObExprSplitPart(common::ObIAllocator &alloc);

  virtual ~ObExprSplitPart();

  virtual int calc_result_typeN(ObExprResType &type,
                                 ObExprResType *types,
                                 int64_t param_num,
                                 common::ObExprTypeCtx &type_ctx) const;


  virtual int cg_expr(ObExprCGCtx &op_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;

  template<bool is_oracle_mode>
  OB_INLINE static int calc_split_part(ObCollationType cs_type,
                                     const ObString &str,
                                     const ObString &delimiter,
                                     int64_t start_part, int64_t end_part,
                                     bool &null_res,
                                     ObString &result)
  {
    int ret = OB_SUCCESS;
    null_res = false;
    if (delimiter.empty()) {
      if (start_part == 1 || start_part == 0 || start_part == -1) {
        if (is_oracle_mode && str.empty()) {
          null_res = true;
        } else {
          result.assign_ptr(str.ptr(), str.length());
        }
      } else {
        if (is_oracle_mode) {
          null_res = true;
        } else {
          result.reset();
        }
      }
    } else if (str.empty()) {
      if (is_oracle_mode) {
        null_res = true;
      } else {
        result.reset();
      }
    } else {
      int64_t total_splits = 0;
      int64_t cur_pos = 0;
      ObSEArray<int64_t, 32> splits_pos;
      // the array is [padding_start, ...split_positions, padding_end]
      int64_t idx = 0;
      OZ(splits_pos.push_back(-delimiter.length())); // padding_start
      while (OB_SUCC(ret) && cur_pos <= str.length()) {
        idx = ObCharset::instrb(cs_type, str.ptr() + cur_pos, str.length() - cur_pos,
                                        delimiter.ptr(), delimiter.length());
        if (idx == -1 || (end_part > 0 && total_splits + 1 > end_part)) {
          break;
        } else {
          OZ(splits_pos.push_back(idx + cur_pos));
          total_splits++;
          cur_pos += idx + delimiter.length();
        }
      }
      OZ(splits_pos.push_back(str.length())); // padding_end
      if (OB_SUCC(ret)) {
        // handle start_pos_int
        if (start_part < 0 && end_part < 0) {
          start_part = start_part + splits_pos.count();
          end_part = end_part + splits_pos.count();
        } else if (start_part < 0 || end_part < 0) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("Invalid part position", K(ret), K(start_part), K(end_part));
        }
        if (OB_FAIL(ret)){
        } else if (total_splits + 1 < start_part || 1 > start_part) {
          // out of range
          if (is_oracle_mode) {
            null_res = true;
          } else {
            result.reset();
          }
        } else {
          int64_t start_pos = 0;
          int64_t end_pos = 0;
          // the return string is [start_pos, end_pos)
          start_pos = splits_pos.at(start_part - 1) + delimiter.length();
          end_pos = end_part > total_splits + 1 ? str.length() : splits_pos.at(end_part);
          if (start_pos >= end_pos) {
            if (is_oracle_mode) {
              null_res = true;
            } else {
              result.reset();
            }
          } else {
            result.assign_ptr(str.ptr() + start_pos, end_pos - start_pos);
          }
        }
      }
    }
    return ret;
  }
  static int calc_split_part_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                        ObDatum &res);
  static int calc_split_part_expr_vec(const ObExpr &expr,
                                      ObEvalCtx &ctx,
                                      const ObBitVector &skip,
                                      const EvalBound &bound);

  template <bool is_oracle_mode, typename StartPartVecType, typename EndPartVecType>
  static int calc_split_part_expr_dispatch(const ObExpr &expr,
                                            ObEvalCtx &ctx,
                                            ObCollationType cs_type,
                                            const ObBitVector &skip,
                                            const EvalBound &bound,
                                            ObBitVector &eval_flags,
                                            ObIVector *res_vec,
                                            ObIVector *str_vec,
                                            ObIVector *delimiter_vec,
                                            StartPartVecType *start_part_vec,
                                            EndPartVecType *end_part_vec);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprSplitPart);
};
}
}

#endif /* _OB_SQL_EXPR_SPLIT_PART_H_ */

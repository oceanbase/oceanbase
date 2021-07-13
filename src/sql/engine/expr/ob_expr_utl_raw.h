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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_RAW_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_RAW_

#include "sql/engine/expr/ob_expr_operator.h"
#include "share/object/ob_obj_cast.h"
namespace oceanbase {
namespace sql {
class ObExprUtlRawCastToRaw : public ObStringExprOperator {
public:
  explicit ObExprUtlRawCastToRaw(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawCastToRaw();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& text, common::ObCastCtx& cast_ctx);
  virtual int calc_result1(common::ObObj& result, const common::ObObj& text, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawCastToRaw);
};

class ObExprUtlRawCastToVarchar2 : public ObStringExprOperator {
public:
  explicit ObExprUtlRawCastToVarchar2(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawCastToVarchar2();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& text, common::ObCastCtx& cast_ctx);
  virtual int calc_result1(common::ObObj& result, const common::ObObj& text, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawCastToVarchar2);
};

class ObExprUtlRawLength : public ObFuncExprOperator {
public:
  explicit ObExprUtlRawLength(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawLength();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result1(common::ObObj& result, const common::ObObj& text, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawLength);
};

class ObRawBitwiseExprOperator : public ObExprOperator {
public:
  ObRawBitwiseExprOperator(common::ObIAllocator& alloc, ObExprOperatorType type, const char* name, int32_t param_num)
      : ObExprOperator(alloc, type, name, param_num)
  {}
  virtual ~ObRawBitwiseExprOperator()
  {}
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;

protected:
  enum BitOperator {
    BIT_AND,
    BIT_OR,
    BIT_XOR,
    BIT_MAX,
  };
  int calc_result_type2_(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  int calc_result2_(common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2,
      common::ObCastCtx& cast_ctx, BitOperator op) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRawBitwiseExprOperator);
};

class ObExprUtlRawBitAnd : public ObRawBitwiseExprOperator {
public:
  explicit ObExprUtlRawBitAnd(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawBitAnd();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawBitAnd);
};

class ObExprUtlRawBitOr : public ObRawBitwiseExprOperator {
public:
  explicit ObExprUtlRawBitOr(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawBitOr();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawBitOr);
};

class ObExprUtlRawBitXor : public ObRawBitwiseExprOperator {
public:
  explicit ObExprUtlRawBitXor(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawBitXor();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawBitXor);
};

class ObExprUtlRawBitComplement : public ObStringExprOperator {
public:
  explicit ObExprUtlRawBitComplement(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawBitComplement();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& type1, common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result1(common::ObObj& result, const common::ObObj& obj1, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawBitComplement);
};

class ObExprUtlRawReverse : public ObStringExprOperator {
public:
  explicit ObExprUtlRawReverse(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawReverse();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj& text, common::ObCastCtx& cast_ctx);
  virtual int calc_result1(common::ObObj& result, const common::ObObj& text, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawReverse);
};

class ObExprUtlRawCopies : public ObStringExprOperator {
public:
  explicit ObExprUtlRawCopies(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawCopies();
  virtual int calc_result_type2(
      ObExprResType& type, ObExprResType& type1, ObExprResType& type2, common::ObExprTypeCtx& type_ctx) const;
  static int calc(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObCastCtx& cast_ctx);
  virtual int calc_result2(
      common::ObObj& result, const common::ObObj& obj1, const common::ObObj& obj2, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawCopies);
};

class ObExprUtlRawCompare : public ObStringExprOperator {
public:
  explicit ObExprUtlRawCompare(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawCompare();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num);
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawCompare);
};

class ObExprUtlRawSubstr : public ObStringExprOperator {
public:
  explicit ObExprUtlRawSubstr(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawSubstr();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num);
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawSubstr);
};

class ObExprUtlRawConcat : public ObStringExprOperator {
public:
  explicit ObExprUtlRawConcat(common::ObIAllocator& alloc);
  virtual ~ObExprUtlRawConcat();
  virtual int calc_result_typeN(
      ObExprResType& type, ObExprResType* types_stack, int64_t param_num, common::ObExprTypeCtx& type_ctx) const;
  static int calc(common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num);
  virtual int calc_resultN(
      common::ObObj& result, const common::ObObj* objs_stack, int64_t param_num, common::ObExprCtx& expr_ctx) const;

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprUtlRawConcat);
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_UTL_RAW_ */

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

#ifndef OCEANBASE_SQL_OB_EXPR_COLLATION_H_
#define OCEANBASE_SQL_OB_EXPR_COLLATION_H_

#include "sql/engine/expr/ob_expr_operator.h"
namespace oceanbase
{
namespace sql
{
/// Returns the character set of the string argument.
class ObExprCharset: public ObStringExprOperator
{
public:
  //ObExprCharset();
  explicit  ObExprCharset(common::ObIAllocator &alloc);
  virtual ~ObExprCharset();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, 
                       ObExpr &rt_expr) const;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprCharset);
  // function members
private:
  // data members
};


/// Returns the collation of the string argument.
class ObExprCollation: public ObStringExprOperator
{
public:
  //ObExprCollation();
  explicit  ObExprCollation(common::ObIAllocator &alloc);
  virtual ~ObExprCollation();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, 
                       ObExpr &rt_expr) const;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprCollation);
  // function members
private:
  // data members
};

/// Returns the collation coercibility value of the string argument.
/// @see ObCollationLevel
class ObExprCoercibility: public ObExprOperator
{
public:
  //ObExprCoercibility();
  explicit  ObExprCoercibility(common::ObIAllocator &alloc);
  virtual ~ObExprCoercibility();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, 
                       ObExpr &rt_expr) const;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprCoercibility);
  // function members
private:
  // data members
};

/// change collation of the input argument
/// used to implement COLLATE clause, e.g. C1 collate utf8_general_ci, 'abc' collate utf8_bin
/// format: SET_COLLATION(expr, utf8_general_ci)
class ObExprSetCollation: public ObExprOperator
{
public:
  //ObExprSetCollation();
  explicit  ObExprSetCollation(common::ObIAllocator &alloc);
  virtual ~ObExprSetCollation();

  virtual int calc_result_type2(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, 
                       ObExpr &rt_expr) const;
private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprSetCollation);
  // function members
private:
  // data members
};

/// Returns the meta used for comparison
/// @note for debug purpose
class ObExprCmpMeta: public ObStringExprOperator
{
public:
  //ObExprCmpMeta();
  explicit  ObExprCmpMeta(common::ObIAllocator &alloc);
  virtual ~ObExprCmpMeta();

  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr, 
                       ObExpr &rt_expr) const;

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObExprCmpMeta);
  // function members
private:
  // data members
};

/// Maps collation_type (int) to charset name. Used by INFORMATION_SCHEMA.VIEWS etc. to avoid virtual table lookup.
/// collation_type_to_charset(collation_type_int) -> varchar
class ObExprCollationTypeToCharset : public ObStringExprOperator
{
public:
  explicit ObExprCollationTypeToCharset(common::ObIAllocator &alloc);
  virtual ~ObExprCollationTypeToCharset();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int eval_collation_type_to_charset(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCollationTypeToCharset);
};

/// Maps collation_type (int) to collation name. Used by INFORMATION_SCHEMA.VIEWS etc. to avoid virtual table lookup.
/// collation_type_to_collation(collation_type_int) -> varchar
class ObExprCollationTypeToCollation : public ObStringExprOperator
{
public:
  explicit ObExprCollationTypeToCollation(common::ObIAllocator &alloc);
  virtual ~ObExprCollationTypeToCollation();
  virtual int calc_result_type1(ObExprResType &type,
                                ObExprResType &type1,
                                common::ObExprTypeCtx &type_ctx) const;
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int eval_collation_type_to_collation(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res_datum);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprCollationTypeToCollation);
};

} // end namespace sql
} // end namespace oceanbase

#endif //OCEANBASE_SQL_OB_EXPR_COLLATION_H_

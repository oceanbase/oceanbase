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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INET_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INET_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase {
namespace sql {
class ObExprInetCommon {
  public :
  static int str_to_ipv4(int len, const char *str, bool& is_ip_format_invalid, in_addr* ipv4addr);
  static int str_to_ipv6(int len, const char *str, bool& is_ip_format_invalid, in6_addr* ipv6addr);
  static int ip_to_str(ObString& ip_binary, bool& is_ip_format_invalid, ObString& ip_str);
};

class ObExprInetAton : public ObFuncExprOperator {
public:
  explicit ObExprInetAton(common::ObIAllocator& alloc);
  virtual ~ObExprInetAton();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_inet_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // helper func
  template <typename T>
  static int ob_inet_aton(T& result, const common::ObString& text, bool& is_ip_format_invalid);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInetAton);
};

inline int ObExprInetAton::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_int();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].precision_);
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObIntType].scale_);
  // set calc type
  text.set_calc_type(common::ObVarcharType);
  return common::OB_SUCCESS;
}

class ObExprInet6Ntoa : public ObStringExprOperator {
public:
  explicit ObExprInet6Ntoa(common::ObIAllocator& alloc);
  virtual ~ObExprInet6Ntoa();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_inet6_ntoa(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInet6Ntoa);
};

class ObExprInet6Aton : public ObFuncExprOperator {
public:
  explicit ObExprInet6Aton(common::ObIAllocator& alloc);
  virtual ~ObExprInet6Aton();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_inet6_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // helper func
  static int inet6_aton(const ObString& ip, bool& is_ip_format_invalid, ObString& str_result);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprInet6Aton);
};

inline int ObExprInet6Aton::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_varbinary();
  type.set_length(16);
  type.set_collation_level(common::CS_LEVEL_COERCIBLE);
  text.set_calc_type(common::ObVarcharType);
  return common::OB_SUCCESS;
}

class ObExprIsIpv4 : public ObFuncExprOperator {
public:
  explicit ObExprIsIpv4(common::ObIAllocator& alloc);
  virtual ~ObExprIsIpv4();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_is_ipv4(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // helper func
  template <typename T>
  static int is_ipv4(T& result, const common::ObString& text);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsIpv4);
};

inline int ObExprIsIpv4::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_tinyint();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].precision_);
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].scale_);
  // set calc type
  text.set_calc_type(common::ObVarcharType);
  return common::OB_SUCCESS;
}

class ObExprIsIpv4Mapped : public ObFuncExprOperator {
public:
  explicit ObExprIsIpv4Mapped(common::ObIAllocator& alloc);
  virtual ~ObExprIsIpv4Mapped();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_is_ipv4_mapped(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // helper func
  template <typename T>
  static void is_ipv4_mapped(T& result, const common::ObString& num_val);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsIpv4Mapped);
};

inline int ObExprIsIpv4Mapped::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(text);
  type.set_tinyint();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].precision_);
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].scale_);
  return common::OB_SUCCESS;
}

class ObExprIsIpv4Compat : public ObFuncExprOperator {
public:
  explicit ObExprIsIpv4Compat(common::ObIAllocator& alloc);
  virtual ~ObExprIsIpv4Compat();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_is_ipv4_compat(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // helper func
  template <typename T>
  static void is_ipv4_compat(T& result, const common::ObString& num_val);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsIpv4Compat);
};

inline int ObExprIsIpv4Compat::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  UNUSED(text);
  type.set_tinyint();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].precision_);
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].scale_);
  return common::OB_SUCCESS;
}

class ObExprIsIpv6 : public ObFuncExprOperator {
public:
  explicit ObExprIsIpv6(common::ObIAllocator& alloc);
  virtual ~ObExprIsIpv6();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_is_ipv6(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  // helper func
  template <typename T>
  static int is_ipv6(T& result, const common::ObString& num_val);

private:
  DISALLOW_COPY_AND_ASSIGN(ObExprIsIpv6);
};

inline int ObExprIsIpv6::calc_result_type1(
    ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const
{
  UNUSED(type_ctx);
  type.set_tinyint();
  type.set_precision(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].precision_);
  type.set_scale(common::ObAccuracy::DDL_DEFAULT_ACCURACY[common::ObTinyIntType].scale_);
  // set calc type
  text.set_calc_type(common::ObVarcharType);
  return common::OB_SUCCESS;
}

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_OB_EXPR_INET6_NTOA_ */

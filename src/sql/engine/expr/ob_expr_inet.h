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
  inline static int str_to_ipv4_nosimd(int len, const char *str, bool& is_ip_format_invalid, in_addr* ipv4addr);
  inline static int str_to_ipv4(int len, const char *str, bool& is_ip_format_invalid, in_addr* ipv4addr);
  inline static int str_to_ipv6(int len, const char *str, bool& is_ip_format_invalid, in6_addr* ipv6addr);
  inline static int str_to_ip(int len, const char *str, bool& is_ip_format_invalid, in6_addr* ipv6addr, bool is_ipv6, bool& is_pure_ipv4);
  static int ip_to_str(ObString& ip_binary, bool& is_ip_format_invalid, ObString& ip_str);
  static int ipv4_to_str(char* ipv4_binary_ptr, ObString& ip_str);
  static int ipv6_to_str(char* ipv6_binary_ptr, ObString& ip_str);
  inline static uint8_t unhex(char c);
};

class ObExprInetAton : public ObFuncExprOperator {
public:
  explicit ObExprInetAton(common::ObIAllocator& alloc);
  virtual ~ObExprInetAton();
  virtual int calc_result_type1(ObExprResType& type, ObExprResType& text, common::ObExprTypeCtx& type_ctx) const;
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  static int calc_inet_aton(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  DECLARE_SET_LOCAL_SESSION_VARS;

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
  static int calc_inet6_ntoa_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  // vectorization helper functions
  static inline int inet6_ntoa_vector_inner(const ObExpr& expr,
                                             const ObString &num_val,
                                             ObString &ip_str,
                                             bool& is_ip_format_invalid,
                                             ObEvalCtx &ctx,
                                             int64_t idx);

  template <typename ArgVec, typename ResVec>
  static int inet6_ntoa_vector(VECTOR_EVAL_FUNC_ARG_DECL);

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
  static int calc_inet6_aton_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  DECLARE_SET_LOCAL_SESSION_VARS;

private:
  // helper func
  static int inet6_aton(const ObString& ip, bool& is_ip_format_invalid, ObString& str_result);

  // vector helper functions
  static inline int inet6_aton_vector_inner(const ObExpr &expr,
                                             const ObString &ip_str,
                                             ObString &str_result,
                                             bool& is_ip_format_invalid,
                                             int64_t idx,
                                             ObEvalCtx &ctx);
  template <typename ArgVec, typename ResVec>
  static int inet6_aton_vector(VECTOR_EVAL_FUNC_ARG_DECL);

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
  static int calc_is_ipv4_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  // helper func
  template <typename T>
  static int is_ipv4(T& result, const common::ObString& text);
  template <typename ArgVec, typename ResVec>
  static int is_ipv4_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  template <typename ResVec>
  static inline int is_ipv4_vector_inner(const common::ObString &str_val, ResVec *res_vec, int64_t idx);

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
  static inline bool str_is_ipv4_mapped(struct in6_addr * num_val);
  static int calc_is_ipv4_mapped(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);
  static int calc_is_ipv4_mapped_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  // helper func
  template <typename T>
  static void is_ipv4_mapped(T& result, const common::ObString& num_val);

  // vectorization helper functions
  template<typename ResVec>
  static inline int is_ipv4_mapped_vector_inner(const common::ObString &num_val, ResVec *res_vec, int64_t idx);

  template <typename ArgVec, typename ResVec>
  static int is_ipv4_mapped_vector(VECTOR_EVAL_FUNC_ARG_DECL);

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
  static int calc_is_ipv4_compat_vector(VECTOR_EVAL_FUNC_ARG_DECL);

private:
  // helper func
  template <typename T>
  static void is_ipv4_compat(T& result, const common::ObString& num_val);

  static inline bool str_is_ipv4_compat(struct in6_addr * num_val);
  template<typename ResVec>
  static inline int is_ipv4_compat_vector_inner(const common::ObString &num_val, ResVec *res_vec, int64_t idx);

  template <typename ArgVec, typename ResVec>
  static int is_ipv4_compat_vector(VECTOR_EVAL_FUNC_ARG_DECL);

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
  static int calc_is_ipv6_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  // helper func
  template <typename T>
  static int is_ipv6(T& result, const common::ObString& num_val);
  template <typename ArgVec, typename ResVec>
  static int is_ipv6_vector(VECTOR_EVAL_FUNC_ARG_DECL);
  template <typename ResVec>
  static inline int is_ipv6_vector_inner(const common::ObString &str_val, ResVec *res_vec, int64_t idx);

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

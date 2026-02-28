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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_OB_SQL_EXPR_LRPAD_
#define OCEANBASE_SQL_ENGINE_EXPR_OB_SQL_EXPR_LRPAD_

#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/expr/ob_i_expr_extra_info.h"

namespace oceanbase
{
namespace sql
{
class ObExprBaseLRpad : public ObStringExprOperator
{

public:
  class ObExprLRPadContext : public ObExprOperatorCtx
  {
  public:
    ObExprLRPadContext()
        : ObExprOperatorCtx(),
          buf_(NULL),
          result_buf_(NULL),
          pad_len_(0)
    {}
    OB_INLINE char * get_pad_buf() { return buf_; }
    OB_INLINE void set_pad_buf(char *buf) { buf_ = buf; }
    OB_INLINE int64_t get_pad_len() { return pad_len_; }
    OB_INLINE void set_pad_len(int64_t len) { pad_len_ = len; }
    OB_INLINE char ** get_result_buf() { return result_buf_; }
    OB_INLINE void set_result_buf(char **buf) { result_buf_ = buf; }
    OB_INLINE int64_t * get_char_pos() { return char_pos_; }
    OB_INLINE void set_char_pos(int64_t *pos) { char_pos_ = pos; }
    TO_STRING_KV(K_(buf), K_(result_buf), K_(pad_len));
    char *buf_;
    char **result_buf_;
    int64_t *char_pos_;
    int64_t pad_len_;
  };
  virtual bool need_rt_ctx() const override { return true; }

public:
  enum LRpadType { LPAD_TYPE = 0, RPAD_TYPE = 1};
  explicit ObExprBaseLRpad(common::ObIAllocator &alloc,
                            ObExprOperatorType type,
                            const char *name,
                            int32_t param_num);

  virtual ~ObExprBaseLRpad();

  int calc_type(ObExprResType &type,
                ObExprResType &text,
                ObExprResType &len,
                ObExprResType *pad_text,
                common::ObExprTypeCtx &type_ctx) const;

  static int padding(LRpadType type,
                     const common::ObCollationType coll_type,
                     const char *text,
                     const int64_t &text_size,
                     const char *pad,
                     const int64_t &pad_size,
                     const int64_t &prefix_size,
                     const int64_t &repeat_count,
                     const bool &pad_space, // for oracle
                     common::ObIAllocator *allocator,
                     char* &result,
                     int64_t &size,
                     ObObjType res_type,
                     bool has_lob_header);
  static int padding_inner(LRpadType type,
                           const char *text,
                           const int64_t &text_size,
                           const char *pad,
                           const int64_t &pad_size,
                           const int64_t &prefix_size,
                           const int64_t &repeat_count,
                           const bool &pad_space,
                           ObString &space_str,
                           char* &result);

  static int get_padding_info_mysql(const common::ObCollationType &cs,
                                    const common::ObString &str_text,
                                    const int64_t text_len,
                                    const int64_t &len,
                                    const common::ObString &str_padtext,
                                    const int64_t max_result_size,
                                    int64_t &repeat_count,
                                    int64_t &prefix_size,
                                    int64_t &size);

  static int calc_type_length_mysql(const ObExprResType result_type,
                                    const common::ObObj &text,
                                    const common::ObObj &pad_text,
                                    const common::ObObj &len,
                                    const ObExprTypeCtx &type_ctx,
                                    int64_t &result_size);
  static int calc_type_length_oracle(const ObExprResType &result_type,
                                     const common::ObObj &text,
                                     const common::ObObj &pad_text,
                                     const common::ObObj &len,
                                     int64_t &result_size);

  static int get_padding_info_oracle(const common::ObCollationType cs,
                                     const common::ObString &str_text,
                                     const int64_t &width,
                                     const common::ObString &str_padtext,
                                     const int64_t max_result_size,
                                     int64_t &repeat_count,
                                     int64_t &prefix_size,
                                     bool &pad_space);
  // for engine 3.0
  static int calc_mysql_pad_expr(const ObExpr &expr, ObEvalCtx &ctx, LRpadType pad_type,
                                 ObDatum &res);
  static int calc_mysql(const LRpadType pad_type, const ObExpr &expr, ObEvalCtx &ctx,
                        const common::ObDatum &text,
                        const common::ObDatum &len, const common::ObDatum &pad_text,
                        const ObSQLSessionInfo &session, common::ObIAllocator &res_alloc,
                        ObDatum &res);
  static int calc_mysql_inner(const LRpadType pad_type,
                              const ObExpr &expr,
                              const ObDatum &len,
                              int64_t &max_result_size,
                              const ObString &str_text,
                              const ObString &str_pad,
                              ObIAllocator &res_alloc,
                              ObDatum &res);

  static int padding_vector(LRpadType type,
                            const ObCollationType coll_type,
                            const char *text,
                            const int64_t &text_size,
                            const char *pad,
                            const int64_t &pad_size,
                            const int64_t &prefix_size,
                            const int64_t &repeat_count,
                            const bool &pad_space, // for oracle
                            char* result,
                            int64_t &size);
  OB_INLINE static int get_padding_info_mysql_vector(const ObCollationType &cs,
                                           const ObString &str_text,
                                           const int64_t text_len,
                                           const int64_t &len,
                                           const ObString &str_padtext,
                                           int64_t pad_len,
                                           const int64_t max_result_size,
                                           int64_t &repeat_count,
                                           int64_t &prefix_size,
                                           int64_t &size);
 static int calc_mysql_pad_expr_vector(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound,
                                       LRpadType pad_type);
  template<typename ResVec, typename TextVec, typename LenVec, typename PadVec>
  static int calc_mysql_vector(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               const ObBitVector &skip,
                               const EvalBound &bound,
                               const LRpadType pad_type,
                               const ObSQLSessionInfo &session);
  template<typename ResVec>
  static int calc_mysql_inner_vector(const LRpadType pad_type,
                                     const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const int64_t int_len,
                                     int64_t &max_result_size,
                                     const ObString &str_text,
                                     const ObString &str_pad,
                                     int64_t pad_len,
                                     ResVec *res_vec,
                                     const int64_t idx,
                                     bool is_ascii);

  template<typename ResVec, typename TextVec>
  static int calc_mysql_vector_optimized(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound,
                                         const LRpadType pad_type,
                                         const ObSQLSessionInfo &session);

  template<typename ResVec, typename TextVec, bool IsLeftPad, bool IsAscii, bool CanDoAsciiOptimize>
  static int calc_mysql_inner_vector_optimized(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound,
                                                 const int64_t mbmaxlen,
                                                 const int64_t max_result_size,
                                                 ObExprLRPadContext *pad_ctx);

  static int calc_oracle_pad_expr(const ObExpr &expr, ObEvalCtx &ctx, LRpadType pad_type,
                                  ObDatum &res);
  static int calc_oracle(LRpadType pad_type, const ObExpr &expr, const common::ObDatum &text,
                         const common::ObDatum &len, const common::ObDatum &pad_text,
                         common::ObIAllocator &res_alloc, ObDatum &res, bool &is_unchanged_clob);

  OB_INLINE static int get_padding_info_oracle_vector(const ObCollationType &cs,
                                           const ObString &str_text,
                                           const int64_t text_width,
                                           const int64_t &width,
                                           const ObString &str_padtext,
                                           int64_t pad_width,
                                           const int64_t max_result_size,
                                           int64_t &repeat_count,
                                           int64_t &prefix_size,
                                           bool &pad_space);
 static int calc_oracle_pad_expr_vector(const ObExpr &expr,
                                       ObEvalCtx &ctx,
                                       const ObBitVector &skip,
                                       const EvalBound &bound,
                                       LRpadType pad_type);
  template<typename ResVec, typename TextVec, typename LenVec, typename PadVec>
  static int calc_oracle_vector(const ObExpr &expr,
                               ObEvalCtx &ctx,
                               const ObBitVector &skip,
                               const EvalBound &bound,
                               const LRpadType pad_type,
                               const ObSQLSessionInfo &session);
  template<typename ResVec>
  static int calc_oracle_inner_vector(const LRpadType pad_type,
                                     const ObExpr &expr,
                                     ObEvalCtx &ctx,
                                     const int64_t width,
                                     int64_t &max_result_size,
                                     const ObString &str_text,
                                     const ObString &str_pad,
                                     int64_t pad_len,
                                     ResVec *res_vec,
                                     const int64_t idx,
                                     bool is_ascii);

  template<typename ResVec, typename TextVec>
  static int calc_oracle_vector_optimized(const ObExpr &expr,
                                         ObEvalCtx &ctx,
                                         const ObBitVector &skip,
                                         const EvalBound &bound,
                                         const LRpadType pad_type,
                                         const ObSQLSessionInfo &session);

  template<typename ResVec, typename TextVec, bool IsLeftPad, bool IsAscii, bool CanDoAsciiOptimize>
  static int calc_oracle_inner_vector_optimized(const ObExpr &expr,
                                                 ObEvalCtx &ctx,
                                                 const ObBitVector &skip,
                                                 const EvalBound &bound,
                                                 const int64_t mbmaxlen,
                                                 const int64_t max_result_size,
                                                 const int64_t len_int,
                                                 const ObString &str_pad,
                                                 ObExprLRPadContext *pad_ctx);

  static int init_pad_ctx(const ObExpr &expr,
                          ObEvalCtx &ctx,
                          ObExecContext &exec_ctx,
                          uint64_t pad_id,
                          const ObString &str_pad,
                          int64_t length,
                          int64_t buffer_size,
                          bool is_mysql_mode,
                          const int64_t max_result_size,
                          ObExprLRPadContext *&pad_ctx);
  int get_origin_len_obj(ObObj &len_obj) const;
  DECLARE_SET_LOCAL_SESSION_VARS;
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprBaseLRpad);
};

class ObExprLpad : public ObExprBaseLRpad
{
public:
  explicit ObExprLpad(common::ObIAllocator &alloc);
  virtual ~ObExprLpad();

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &text,
                                ObExprResType &len,
                                ObExprResType &pad_text,
                                common::ObExprTypeCtx &type_ctx) const override;

  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const override;
  static int calc_mysql_lpad_expr(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &res);
  static int calc_mysql_lpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprLpad);
};

class ObExprRpad : public ObExprBaseLRpad
{
public:
  explicit ObExprRpad(common::ObIAllocator &alloc);
  explicit ObExprRpad(common::ObIAllocator &alloc,
                      ObExprOperatorType type,
                      const char *name);

  virtual ~ObExprRpad();

  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &text,
                                ObExprResType &len,
                                ObExprResType &pad_text,
                                common::ObExprTypeCtx &type_ctx) const override;

  // for engine 3.0
  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int calc_mysql_rpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res);
  static int calc_mysql_rpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprRpad);
};

class ObExprOracleLpad : public ObExprBaseLRpad
{
public:
  explicit ObExprOracleLpad(common::ObIAllocator &alloc);
  virtual ~ObExprOracleLpad();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int calc_oracle_lpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res);
  static int calc_oracle_lpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleLpad);
};

class ObExprOracleRpad : public ObExprBaseLRpad
{
public:
  explicit ObExprOracleRpad(common::ObIAllocator &alloc);
  virtual ~ObExprOracleRpad();

  virtual int calc_result_typeN(ObExprResType &type,
                                ObExprResType *types_array,
                                int64_t param_num,
                                common::ObExprTypeCtx &type_ctx) const override;

  virtual int cg_expr(ObExprCGCtx &expr_cg_ctx, const ObRawExpr &raw_expr,
                      ObExpr &rt_expr) const;
  static int calc_oracle_rpad_expr(const ObExpr &expr, ObEvalCtx &ctx,
                                           ObDatum &res);
  static int calc_oracle_rpad_expr_vector(VECTOR_EVAL_FUNC_ARG_DECL);
private:
  DISALLOW_COPY_AND_ASSIGN(ObExprOracleRpad);
};

struct ObExprOracleLRpadInfo : public ObIExprExtraInfo
{
  OB_UNIS_VERSION(1);
public:
  ObExprOracleLRpadInfo(common::ObIAllocator &alloc, ObExprOperatorType type)
      : ObIExprExtraInfo(alloc, type),
        is_called_in_sql_(true)
  {
  }

  virtual int deep_copy(common::ObIAllocator &allocator,
                        const ObExprOperatorType type,
                        ObIExprExtraInfo *&copied_info) const override;

public:
  bool is_called_in_sql_;
};

} // namespace sql
} // namespace oceanbase
#endif // OCEANBASE_SQL_ENGINE_EXPR_OB_SQL_EXPR_RPAD_

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

#ifndef OCEANBASE_SQL_ENGINE_EXPR_LIKE_
#define OCEANBASE_SQL_ENGINE_EXPR_LIKE_

#include "sql/engine/expr/ob_expr_operator.h"

namespace oceanbase
{
namespace sql
{
enum INSTR_MODE
{
  START_WITH_PERCENT_SIGN = 0, //"%%a%b",etc
  START_END_WITH_PERCENT_SIGN = 1, //"%abc%%dd%",etc
  END_WITH_PERCENT_SIGN = 2,//"aa%%%%%%",etc
  MIDDLE_PERCENT_SIGN = 3,  //"ab%cde",etc
  ALL_PERCENT_SIGN = 4, //"%%",etc
  INVALID_INSTR_MODE = 5
};

class ObExprLike : public ObFuncExprOperator
{
  struct InstrInfo
  {
    InstrInfo() :
        allocator_(NULL),
        instr_mode_(INVALID_INSTR_MODE),
        instr_starts_(NULL),
        instr_lengths_(NULL),
        instr_total_length_(0),
        instr_cnt_(0),
        instr_info_buf_size_(0),
        instr_buf_(NULL),
        instr_buf_length_(0)
    { }

    int record_pattern(char *&pattern_buf, const common::ObString &pattern);
    OB_INLINE void set_allocator(common::ObIAllocator &allocator) { allocator_ = &allocator; }
    int add_instr_info(const char *start, const uint32_t length);
    bool empty() const { return 0 == instr_cnt_; }
    void reuse()
    {
      instr_mode_ = INVALID_INSTR_MODE;
      instr_total_length_ = 0;
      instr_cnt_ = 0;
    }
    TO_STRING_KV(K_(instr_mode), K_(instr_total_length), K_(instr_cnt),
        K(static_cast<void *>(instr_buf_)),
        K(common::ObArrayWrap<uint32_t>(instr_lengths_, instr_cnt_)));

    common::ObIAllocator *allocator_;
    INSTR_MODE instr_mode_;

    // for pattern '%abc%ef%efga%1234', instr_cnt_ is 4;
    // instr_start_ record address of 'abc', 'ef', 'efga' and '1234';
    // instr_lengths_ is [3, 2, 4, 4].
    const char **instr_starts_;
    uint32_t *instr_lengths_;
    uint32_t instr_total_length_;
    uint32_t instr_cnt_;
    // current buf size of instr_starts_ and instr_lengths_.
    uint32_t instr_info_buf_size_;

    char *instr_buf_;
    uint32_t instr_buf_length_;
  };
  class ObExprLikeContext : public ObExprOperatorCtx
  {
  public:
    //member functions
    ObExprLikeContext()
        : ObExprOperatorCtx(),
          instr_info_(),
          is_analyzed_(false),
          is_checked_(false),
          last_pattern_(NULL),
          last_pattern_len_(0),
          pattern_buf_len_(0),
          last_escape_(NULL),
          last_escape_len_(0),
          escape_buf_len_(0),
          same_as_last(false)
    {}
    OB_INLINE bool is_analyzed() const {return is_analyzed_;}
    OB_INLINE void set_analyzed() {is_analyzed_ = true;}
    OB_INLINE bool is_instr_mode() const {
      return instr_info_.instr_mode_ >= 0 && instr_info_.instr_mode_ < INVALID_INSTR_MODE;
    }
    OB_INLINE void set_checked() { is_checked_ = true; }
    OB_INLINE bool is_checked() const { return is_checked_; }
    OB_INLINE INSTR_MODE get_instr_mode() { return instr_info_.instr_mode_; }
    OB_INLINE void set_instr_mode(INSTR_MODE mode) { instr_info_.instr_mode_ = mode; }
    TO_STRING_KV(K_(instr_info), K_(is_analyzed));
    InstrInfo instr_info_;
    bool is_analyzed_;
    bool is_checked_;
    // record last pattern and escape.
    char *last_pattern_;
    uint32_t last_pattern_len_;
    uint32_t pattern_buf_len_;
    char *last_escape_;
    uint32_t last_escape_len_;
    uint32_t escape_buf_len_;
    bool same_as_last;
  };
  OB_UNIS_VERSION_V(1);
public:
  explicit  ObExprLike(common::ObIAllocator &alloc);
  virtual ~ObExprLike();
  virtual int calc_result_type3(ObExprResType &type,
                                ObExprResType &type1,
                                ObExprResType &type2,
                                ObExprResType &type3,
                                common::ObExprTypeCtx &type_ctx) const;
  template <bool NullCheck, bool UseInstrMode, INSTR_MODE InstrMode>
  static int match_text_batch(BATCH_EVAL_FUNC_ARG_DECL,
                                        const common::ObCollationType coll_type,
                                        const int32_t escape_wc,
                                        const common::ObString &pattern_val,
                                        const InstrInfo instr_info);
  template <bool percent_sign_start, bool percent_sign_end>
  static int64_t match_with_instr_mode(const common::ObString &text_val,
                                       const InstrInfo instr_info);
  template <typename T>
  static int calc_with_non_instr_mode(T &result,
                                      const common::ObCollationType coll_type,
                                      const common::ObCollationType escape_coll,
                                      const common::ObString &text_val,
                                      const common::ObString &pattern_val,
                                      const common::ObString &escape_val);
  int assign(const ObExprOperator &other);
  OB_INLINE bool is_pattern_literal() const {return is_pattern_literal_;}
  OB_INLINE void set_pattern_is_literal(bool b) {is_pattern_literal_ = b;}
  OB_INLINE bool is_text_literal() const {return is_text_literal_;}
  OB_INLINE void set_text_is_literal(bool b) {is_text_literal_ = b;}
  OB_INLINE bool is_escape_literal() const {return is_escape_literal_;}
  OB_INLINE void set_escape_is_literal(bool b) {is_escape_literal_ = b;}
  virtual int cg_expr(ObExprCGCtx &op_cg_ctx,
                      const ObRawExpr &raw_expr, ObExpr &rt_expr) const override;
  virtual bool need_rt_ctx() const override { return true; }

  static int like_varchar(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &expr_datum);
  static int eval_like_expr_batch_only_text_vectorized(BATCH_EVAL_FUNC_ARG_DECL);
  static int like_varchar_inner(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &expr_datum,
                                ObDatum &text, ObDatum &pattern, ObDatum &escape);
  static int like_text_vectorized_inner(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const int64_t size,
                                        ObExpr &text, ObDatum *pattern_datum, ObDatum *escape_datum);
private:
  static int set_instr_info(common::ObIAllocator *exec_allocator,
                            const common::ObCollationType cs_type,
                            const common::ObString &pattern,
                            const common::ObString &escape,
                            const common::ObCollationType escape_coll,
                            ObExprLikeContext &like_ctx);
  static int is_escape(const common::ObCollationType cs_type,
                       const char *buf_start,
                       int32_t char_len,
                       int32_t escape_wc,
                       bool &res);
  static int calc_escape_wc(const common::ObCollationType escape_coll,
                            const common::ObString &escape,
                            int32_t &escape_wc);
  template <typename T>
  inline static int calc_with_instr_mode(T &result,
                                          const common::ObCollationType cs_type,
                                          const common::ObString &text,
                                          const ObExprLikeContext &like_ctx);
  template <bool is_static_engine, typename T>
  static int check_pattern_valid(const T &pattern, const T &escape,
                                const common::ObCollationType escape_coll,
	                              common::ObCollationType coll_type,
                                ObExecContext *exec_ctx, const uint64_t like_id,
                                const bool check_optimization);
  template <bool is_static_engine>
  OB_INLINE static void record_last_check(ObExprLikeContext &like_ctx,
                                          const common::ObString pattern_val,
                                          const common::ObString escape_val,
                                          common::ObIAllocator *buf_alloc);
  template <bool is_static_engine>
  OB_INLINE static bool checked_already(const ObExprLikeContext &like_ctx, bool null_pattern,
                                        const common::ObString pattern_val, bool null_escape,
                                        const common::ObString escape_val);
  DISALLOW_COPY_AND_ASSIGN(ObExprLike);
private:
  //we may perform optimization via instr only when it holds that
  //is_text_literal_ == false and is_pattern_literal_ == true and is_escape_literal_ == true
  bool is_pattern_literal_;
  bool is_text_literal_;
  bool is_escape_literal_;
  int16_t like_id_;
};

template <typename T>
int ObExprLike::calc_with_non_instr_mode(T &result,
                                          const ObCollationType coll_type,
                                          const ObCollationType escape_coll,
                                          const ObString &text_val,
                                          const ObString &pattern_val,
                                          const ObString &escape_val)
{
  // convert escape char
   // escape use its own collation,
   // @see
   // try this query in MySQL:
   // mysql>  select 'a%' like 'A2%' ESCAPE X'32', X'32';
   // +------------------------------+-------+
   // | 'a%' like 'A2%' ESCAPE X'32' | X'32' |
   // +------------------------------+-------+
   // |                            1 | 2     |
   // +------------------------------+-------+
  int ret = OB_SUCCESS;
  int32_t escape_wc = 0;
  if (OB_FAIL(calc_escape_wc(escape_coll, escape_val, escape_wc))) {
    ret = OB_INVALID_ARGUMENT;
    SQL_LOG(WARN, "failed to get the wc of escape", K(ret), K(escape_val), K(escape_coll), K(escape_wc));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "ESCAPE");
  } else if (text_val.length() <= 0 && pattern_val.length() <= 0) {
    // empty string
    result.set_int(1);
  } else {
    bool b = ObCharset::wildcmp(coll_type, text_val, pattern_val, escape_wc,
                                static_cast<int32_t>('_'), static_cast<int32_t>('%'));
    SQL_LOG(DEBUG, "calc_with_non_instr_mode1", K(escape_coll), K(escape_val), K(escape_wc), K(pattern_val),
             K(coll_type), KPHEX(text_val.ptr(), text_val.length()), KPHEX(pattern_val.ptr(), pattern_val.length()), K(b));
    result.set_int(static_cast<int64_t>(b));
  }
  return ret;
}

}
}
#endif /* OCEANBASE_SQL_ENGINE_EXPR_LIKE_ */

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

namespace oceanbase {
namespace sql {
class ObExprLike : public ObFuncExprOperator {
  class ObExprLikeContext : public ObExprOperatorCtx {
  public:
    enum INSTR_MODE {
      START_WITH_PERCENT_SIGN = 0,      //"%%a",etc
      START_END_WITH_PERCENT_SIGN = 1,  //"%abc%%",etc
      END_WITH_PERCENT_SIGN = 2,        //"aa%%%%%%",etc
      INVALID_INSTR_MODE = 3
    };
    // member functions
    ObExprLikeContext()
        : ObExprOperatorCtx(),
          instr_mode_(INVALID_INSTR_MODE),
          instr_start_(NULL),
          instr_length_(0),
          instr_buf_(NULL),
          instr_buf_length_(0),
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
    OB_INLINE void reset()
    {
      instr_mode_ = INVALID_INSTR_MODE;
      instr_start_ = NULL;
      instr_length_ = 0;
      is_analyzed_ = false;
      is_checked_ = false;
    }
    OB_INLINE bool is_analyzed() const
    {
      return is_analyzed_;
    }
    OB_INLINE void set_analyzed()
    {
      is_analyzed_ = true;
    }
    OB_INLINE bool is_instr_mode() const
    {
      return instr_mode_ >= 0 && instr_mode_ < INVALID_INSTR_MODE;
    }
    OB_INLINE void set_checked()
    {
      is_checked_ = true;
    }
    OB_INLINE bool is_checked() const
    {
      return is_checked_;
    }
    TO_STRING_KV(K_(instr_mode), K_(instr_length), K_(is_analyzed));
    // data members
    INSTR_MODE instr_mode_;
    const char* instr_start_;
    uint32_t instr_length_;
    char* instr_buf_;
    uint32_t instr_buf_length_;
    bool is_analyzed_;
    bool is_checked_;
    char* last_pattern_;
    uint32_t last_pattern_len_;
    uint32_t pattern_buf_len_;
    char* last_escape_;
    uint32_t last_escape_len_;
    uint32_t escape_buf_len_;
    bool same_as_last;
  };
  OB_UNIS_VERSION_V(1);

public:
  explicit ObExprLike(common::ObIAllocator& alloc);
  virtual ~ObExprLike();
  virtual int calc_result_type3(ObExprResType& type, ObExprResType& type1, ObExprResType& type2, ObExprResType& type3,
      common::ObExprTypeCtx& type_ctx) const;
  virtual int calc_result3(common::ObObj& result, const common::ObObj& obj, const common::ObObj& pattern,
      const common::ObObj& escape, common::ObExprCtx& expr_ctx) const;
  static int calc(common::ObObj& result, common::ObCollationType coll_type, const common::ObObj& obj,
      const common::ObObj& pattern, const common::ObObj& escape, common::ObExprCtx& expr_ctx,
      const bool is_going_optimization, const uint64_t like_id, const bool check_optimization);
  template <typename T>
  static int calc_with_non_instr_mode(T& result, const common::ObCollationType coll_type,
      const common::ObCollationType escape_coll, const common::ObString& text_val, const common::ObString& pattern_val,
      const common::ObString& escape_val);
  int assign(const ObExprOperator& other);
  OB_INLINE bool is_pattern_literal() const
  {
    return is_pattern_literal_;
  }
  OB_INLINE void set_pattern_is_literal(bool b)
  {
    is_pattern_literal_ = b;
  }
  OB_INLINE bool is_text_literal() const
  {
    return is_text_literal_;
  }
  OB_INLINE void set_text_is_literal(bool b)
  {
    is_text_literal_ = b;
  }
  OB_INLINE bool is_escape_literal() const
  {
    return is_escape_literal_;
  }
  OB_INLINE void set_escape_is_literal(bool b)
  {
    is_escape_literal_ = b;
  }
  virtual int cg_expr(ObExprCGCtx& op_cg_ctx, const ObRawExpr& raw_expr, ObExpr& rt_expr) const override;
  virtual bool need_rt_ctx() const override
  {
    return true;
  }

  static int like_varchar(const ObExpr& expr, ObEvalCtx& ctx, ObDatum& expr_datum);

private:
  enum STATE {
    INIT = 0,
    PERCENT = 1,                     //"%", "%%", "%%%",etc
    PERCENT_NONPERCENT = 2,          //"%a", "%%a" "%%ab",etc
    NONPERCENT_PERCENT = 3,          //"a%" "a%%" "aa%" "aa%%",etc
    NONPERCENT = 4,                  //"a" "aa" "aaa",etc
    PERCENT_NONPERCENT_PENCENT = 5,  //"%a%" "%%aa%",etc
    END = 6
  };
  static int set_instr_info(common::ObIAllocator* exec_allocator, const common::ObCollationType cs_type,
      const common::ObString& text, const common::ObString& pattern, const common::ObString& escape,
      const common::ObCollationType escape_coll, ObExprLikeContext& like_ctx);
  static int is_escape(
      const common::ObCollationType cs_type, const char* buf_start, int32_t char_len, int32_t escape_wc, bool& res);
  static int calc_escape_wc(
      const common::ObCollationType escape_coll, const common::ObString& escape, int32_t& escape_wc);
  static void state_trans_with_percent_sign(STATE& current_state);
  static void state_trans_with_nonpercent_char(STATE& current_state);
  template <typename T>
  inline static int calc_with_instr_mode(T& result, const common::ObCollationType cs_type, const common::ObString& text,
      const ObExprLikeContext& like_ctx);
  template <typename T>
  static int check_pattern_valid(const T& pattern, const T& escape, const common::ObCollationType escape_coll,
      common::ObCollationType coll_type, ObExecContext* exec_ctx, const uint64_t like_id, const bool check_optimization,
      bool is_static_engine);
  OB_INLINE static void record_last_check(ObExprLikeContext& like_ctx, const common::ObString pattern_val,
      const common::ObString escape_val, common::ObIAllocator* buf_alloc, bool is_static_engine);
  OB_INLINE static bool checked_already(const ObExprLikeContext& like_ctx, bool null_pattern,
      const common::ObString pattern_val, bool null_escape, const common::ObString escape_val, bool is_static_engine);
  DISALLOW_COPY_AND_ASSIGN(ObExprLike);

private:
  // we may perform optimization via instr only when it holds that
  // is_text_literal_ == false and is_pattern_literal_ == true and is_escape_literal_ == true
  bool is_pattern_literal_;
  bool is_text_literal_;
  bool is_escape_literal_;
  int16_t like_id_;
};

}  // namespace sql
}  // namespace oceanbase
#endif /* OCEANBASE_SQL_ENGINE_EXPR_LIKE_ */

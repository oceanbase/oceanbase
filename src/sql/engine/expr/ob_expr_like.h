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

  class StringSearcher {
#if defined(__x86_64__)
  private:
    static constexpr int m128_size_ = sizeof(__m128i);
    const int page_size_ = sysconf(_SC_PAGESIZE);

    bool page_safe(const void *const ptr) const {
      return ((page_size_ - 1) & reinterpret_cast<std::uintptr_t>(ptr)) <= page_size_ - m128_size_;
    }
    template <typename T>
    T unaligned_load(const void *address) const
    {
      T res{};
      memcpy(&res, address, sizeof(res));
      return res;
    }
#endif

  public:
    StringSearcher() : pattern_(nullptr), pattern_end_(nullptr), pattern_len_(0) {}
    StringSearcher(const char *pattern, const size_t pattern_size) {
      init(pattern, pattern_size);
    }
    __attribute__((target("sse4.1")))
    inline void init(const char *pattern, size_t len) {
      if (0 == len) {
        return;
      }
      pattern_ = pattern;
      pattern_end_ = pattern_ + len;
      pattern_len_ = len;

      first_ = *pattern;
#if defined(__x86_64__)
      first_pattern_ = _mm_set1_epi8(first_);
      if (pattern_ + 1 < pattern_end_) {
        second_ = *(pattern_ + 1);
        second_pattern_ = _mm_set1_epi8(second_);
      }

      const char *pattern_pos = pattern_;
      for (size_t i = 0; i < m128_size_; i++) {
        cache_ = _mm_srli_si128(cache_, 1);
        if (pattern_pos != pattern_end_) {
          cache_ = _mm_insert_epi8(cache_, *pattern_pos, m128_size_ - 1);
          cache_mask_ |= 1 << i;
          ++pattern_pos;
        }
      }
#endif
    }
    inline const char *get_pattern() { return pattern_; }
    inline const char *get_patterne_end() { return pattern_end_; }
    inline size_t get_pattern_length() { return pattern_len_; }

  public:
    // determines if pattern_ is a substring of text
    inline bool is_substring(const char *text, const char *text_end) const {
      // pattern_ will not be null becauce it is preprared in set_instr_info().
      if (text == text_end) {
        return false;
      }
#if defined(__x86_64__)
      // here is the quick path when pattern_len_ is 1.
      if (pattern_len_ == 1) {
        while (text < text_end) {
          if (text + m128_size_ <= text_end && page_safe(text)) {
            __m128i v_text = _mm_loadu_si128(reinterpret_cast<const __m128i *>(text));
            __m128i v_against_pattern = _mm_cmpeq_epi8(v_text, first_pattern_);
            int mask = _mm_movemask_epi8(v_against_pattern);
            if (mask == 0) {
              text += m128_size_;
              continue;
            }
            int offset = __builtin_ctz(mask);
            text += offset;
            return true;
          }
          if (text == text_end) {
            return false;
          }
          if (*text == first_) {
            return true;
          }
          ++text;
        }
        return false;
      }
#endif
      while (text < text_end && text_end - text >= pattern_len_) {
#if defined(__x86_64__)
        if ((text + 1 + m128_size_) <= text_end && page_safe(text)) {
          // find first and second byte of text
          __m128i first_block = _mm_loadu_si128(reinterpret_cast<const __m128i *>(text));
          __m128i second_block = _mm_loadu_si128(reinterpret_cast<const __m128i *>(text + 1));
          __m128i first_cmp = _mm_cmpeq_epi8(first_block, first_pattern_);
          __m128i second_cmp = _mm_cmpeq_epi8(second_block, second_pattern_);
          int mask = _mm_movemask_epi8(_mm_and_si128(first_cmp, second_cmp));
          // first and second byte not present in 16 octets starting at `text`
          if (mask == 0) {
            text += m128_size_;
            continue;
          }
          int offset = __builtin_ctz(mask);
          text += offset;
          if (text + m128_size_ <= text_end && page_safe(text)) {
            // check for first 16 octets
            __m128i v_text_offset = _mm_loadu_si128(reinterpret_cast<const __m128i *>(text));
            __m128i v_against_cache = _mm_cmpeq_epi8(v_text_offset, cache_);
            int mask_offset = _mm_movemask_epi8(v_against_cache);
            if (0xffff == cache_mask_) {
              if (mask_offset == cache_mask_) {
                const char *text_pos = text + m128_size_;
                const char *pattern_pos = pattern_ + m128_size_;
                while (text_pos < text_end &&
                       pattern_pos < pattern_end_ &&
                       *text_pos == *pattern_pos) {
                  ++text_pos, ++pattern_pos;
                }
                if (pattern_pos == pattern_end_) {
                  return true;
                }
              }
            } else if ((mask_offset & cache_mask_) == cache_mask_) {
              return true;
            }
            ++text;
            continue;
          }
        }
#endif
        if (text == text_end) {
          return false;
        }
        if (*text == first_) {
          const char *text_pos = text + 1;
          const char *pattern_pos = pattern_ + 1;
          while (text_pos < text_end && pattern_pos < pattern_end_ && *text_pos == *pattern_pos) {
            ++text_pos, ++pattern_pos;
          }
          if (pattern_pos == pattern_end_) {
            return true;
          }
        }
        ++text;
      }
      return false;
    }
    // determines if text starts with pattern_
    inline bool start_with(const char *text, const char *text_end) const {
      // pattern_ will not be null becauce it is preprared in set_instr_info().
      if (pattern_len_ > text_end - text) {
        return false;
      }
      return memequal_opt(text, pattern_, pattern_len_);
    }
    // determines if text ends with pattern_
    inline bool end_with(const char *text, const char *text_end) const {
      // pattern_ will not be null becauce it is preprared in set_instr_info().
      if (pattern_len_ > text_end - text) {
        return false;
      }
      return memequal_opt(text_end - pattern_len_, pattern_, pattern_len_);
    }
    // determines if text equals with pattern_
    inline bool equal(const char *text, const char *text_end) const {
      // pattern_ will not be null becauce it is preprared in set_instr_info().
      if (pattern_len_ != text_end - text) {
        return false;
      }
      return memequal_opt(text, pattern_, pattern_len_);
    }

    inline bool memequal_opt(const char *s1, const char *s2, size_t n) const {
      switch (n)
      {
      case 1:
        return *s1 == *s2;
      case 2:
        return unaligned_load<uint16_t>(s1) == unaligned_load<uint16_t>(s2);
      case 3:
        return unaligned_load<uint16_t>(s1) == unaligned_load<uint16_t>(s2) &&
               unaligned_load<uint8_t>(s1 + 2) == unaligned_load<uint8_t>(s2 + 2);
      case 4:
        return unaligned_load<uint32_t>(s1) == unaligned_load<uint32_t>(s2);
      case 5:
        return unaligned_load<uint32_t>(s1) == unaligned_load<uint32_t>(s2) &&
               unaligned_load<uint8_t>(s1 + 4) == unaligned_load<uint8_t>(s2 + 4);
      case 6:
        return unaligned_load<uint32_t>(s1) == unaligned_load<uint32_t>(s2) &&
               unaligned_load<uint16_t>(s1 + 4) == unaligned_load<uint16_t>(s2 + 4);
      case 7:
        return unaligned_load<uint32_t>(s1) == unaligned_load<uint32_t>(s2) &&
               unaligned_load<uint16_t>(s1 + 4) == unaligned_load<uint16_t>(s2 + 4) &&
               unaligned_load<uint8_t>(s1 + 6) == unaligned_load<uint8_t>(s2 + 6);
      case 8:
        return unaligned_load<uint64_t>(s1) == unaligned_load<uint64_t>(s2);
      default:
        break;
      }
      if (n <= 16) {
        return unaligned_load<uint64_t>(s1) == unaligned_load<uint64_t>(s2) &&
               unaligned_load<uint64_t>(s1 + n - 8) == unaligned_load<uint64_t>(s2 + n - 8);
      }
#if defined(__x86_64__)
      while (n >= 64) {
        if (memequal_sse<4>(s1, s2)) {
          s1 += 64;
          s2 += 64;
          n -= 64;
        } else {
          return false;
        }
      }
      switch (n / 16) {
      case 3:
        if (!memequal_sse<1>(s1 + 32, s2 + 32)) {
          return false;
        }
      case 2:
        if (!memequal_sse<1>(s1 + 16, s2 + 16)) {
          return false;
        }
      case 1:
        if (!memequal_sse<1>(s1, s2)) {
          return false;
        }
      }
      return memequal_sse<1>(s1 + n - 16, s2 + n - 16);
#else
      return 0 == MEMCMP(s1, s2, n);
#endif
    }
#if defined(__x86_64__)
    // cnt means the count of __m128i to compare
    template <int cnt>
    inline bool memequal_sse(const char *p1, const char *p2) const {
      if (cnt == 1) {
        return 0xFFFF == _mm_movemask_epi8(
            _mm_cmpeq_epi8(
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))));
      }
      if (cnt == 4) {
        return 0xFFFF == _mm_movemask_epi8(
            _mm_and_si128(
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1)),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2))),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 1),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 1))),
                _mm_and_si128(
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 2),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 2)),
                    _mm_cmpeq_epi8(
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p1) + 3),
                        _mm_loadu_si128(reinterpret_cast<const __m128i *>(p2) + 3)))));
      }
    }
#endif
  private:
    // string to be searched for
    const char *pattern_;
    const char *pattern_end_;
    size_t pattern_len_;
    // first byte of `pattern_`
    uint8_t first_;
#if defined(__x86_64__)
    // second byte of `pattern_`
    uint8_t second_;
    // vector filled `first_` or `second_`
    __m128i first_pattern_;
    __m128i second_pattern_;
    // vector filled first 16 bytes of `pattern_` and the mask of cache
    __m128i cache_ = _mm_setzero_si128();
    int cache_mask_ = 0;
#endif
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
    // string helper to calc substring etc.
    StringSearcher string_searcher_;
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
                                        const InstrInfo instr_info,
                                        const StringSearcher &string_searcher);
template <typename TextVec, typename ResVec, bool NullCheck, bool UseInstrMode, INSTR_MODE InstrMode>
  static int match_text_vector(VECTOR_EVAL_FUNC_ARG_DECL,
                                        const common::ObCollationType coll_type,
                                        const int32_t escape_wc,
                                        const common::ObString &pattern_val,
                                        const InstrInfo instr_info,
                                        const StringSearcher &string_searcher);
  template <bool percent_sign_start, bool percent_sign_end>
  static int64_t match_with_instr_mode(const common::ObString &text_val,
                                       const InstrInfo instr_info,
                                       const StringSearcher &string_searcher);
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
  template <typename TextVec, typename ResVec>
  static int vector_like(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                         const EvalBound &bound);
  static int eval_like_expr_vector_only_text_vectorized(VECTOR_EVAL_FUNC_ARG_DECL);
  static int like_varchar_inner(const ObExpr &expr, ObEvalCtx &ctx,  ObDatum &expr_datum,
                                ObDatum &text, ObDatum &pattern, ObDatum &escape);
  static int like_text_vectorized_inner(const ObExpr &expr, ObEvalCtx &ctx,
                                        const ObBitVector &skip, const int64_t size,
                                        ObExpr &text, ObDatum *pattern_datum, ObDatum *escape_datum);
  template <typename TextVec, typename ResVec>
  static int like_text_vectorized_inner_vec2(const ObExpr &expr, ObEvalCtx &ctx,
                                             const ObBitVector &skip, const EvalBound &bound,
                                             ObExpr &text, ObDatum *pattern_datum);

  DECLARE_SET_LOCAL_SESSION_VARS;
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

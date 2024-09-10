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

#ifndef  OCEANBASE_SQL_ENGINE_REGEX_OB_POSIX_REGEX_
#define  OCEANBASE_SQL_ENGINE_REGEX_OB_POSIX_REGEX_
#include "easy_define.h"  // for conflict of macro likely
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_mysql_global.h"
#include "lib/charset/ob_ctype.h"
#include <sys/types.h>
#include <assert.h>
#include "lib/charset/ob_charset.h"
#include <icu/i18n/unicode/uregex.h>
#include "sql/engine/expr/ob_expr_operator.h"
#if defined(__x86_64__)
#include <hyperscan/hs/hs.h>
#endif

// this regex is compatible with mysql 8.0

namespace oceanbase
{
namespace sql
{

struct ObExprRegexpSessionVariables {
  TO_STRING_KV(K_(regexp_stack_limit), K_(regexp_time_limit));
  ObExprRegexpSessionVariables():
    regexp_stack_limit_(0),
    regexp_time_limit_(0)
  {}
  int64_t regexp_stack_limit_;
  int64_t regexp_time_limit_;
  OB_UNIS_VERSION(1);
};

class ObExprRegexContext : public ObExprOperatorCtx
{
public:
  ObExprRegexContext();
  virtual ~ObExprRegexContext();
public:
  static const char *icu_version_string() { return U_ICU_VERSION; }
  inline bool is_inited() const { return inited_; }
  void destroy();
  void reset();

  // The previous regex compile result can be used if pattern not change, if %reusable is true.
  // %string_buf must be the same with previous init too if %reusable is true.
  int init(ObExprStringBuf &string_buf,
           const ObExprRegexpSessionVariables &regex_vars,
           const ObString &origin_pattern,
           const uint32_t cflags,
           const bool reusable,
           const ObCollationType cs_type);

  int match(ObExprStringBuf &string_buf,
            const ObString &text,
            const ObCollationType,
            const int64_t start,
            bool &result) const;

  int find(ObExprStringBuf &string_buf,
           const ObString &text,
           const ObCollationType cs_type,
           const int64_t start,
           const int64_t occurrence,
           const int64_t return_option,
           const int64_t subexpr,
           int64_t &result) const;

  int count(ObExprStringBuf &string_buf,
            const ObString &text,
            const ObCollationType cs_type,
            const int32_t start,
            int64_t &result) const;

  int substr(ObExprStringBuf &string_buf,
             const ObString &text,
             const ObCollationType cs_type,
             const int64_t start,
             const int64_t occurrence,
             const int64_t subexpr,
             ObString &result) const;

  int replace(ObExprStringBuf &string_buf,
              const ObString &text_string,
              const ObCollationType cs_type,
              const ObString &replace_string,
              const int64_t start,
              const int64_t occurrence,
              ObString &result) const;

  int append_head(ObExprStringBuf &string_buf,
                  const int32_t current_pos,
                  UChar *&replace_buff,
                  int32_t &buff_size,
                  int32_t &buff_pos) const;

  int append_replace_str(ObExprStringBuf &string_buf,
                         const UChar *u_replace,
                         const int32_t u_replace_length,
                         UChar *&replace_buff,
                         int32_t &buff_size,
                         int32_t &buff_pos) const;

  int append_tail(ObExprStringBuf &string_buf,
                  UChar *&replace_buff,
                  int32_t &buff_size,
                  int32_t &buff_pos) const;

  static int get_regexp_flags(const ObString &match_param,
                              const bool is_case_sensitive,
                              const bool is_som_leftmost,
                              const bool is_single_match,
                              uint32_t &flags);

  static int check_need_utf8(ObRawExpr *expr, bool &is_nstring);

  static inline bool is_binary_string(const ObExprResType &type) {
    return CS_TYPE_BINARY == type.get_collation_type() && (ObVarcharType == type.get_type() || ObHexStringType == type.get_type());
  }

  static inline bool is_binary_compatible(const ObExprResType &type) {
    return CS_TYPE_BINARY == type.get_collation_type() || !ob_is_string_or_lob_type(type.get_type());
  }
  TO_STRING_KV(K_(inited));

  static int check_binary_compatible(const ObExprResType *types, int64_t num);

private:
  int preprocess_pattern(common::ObExprStringBuf &string_buf,
                         const common::ObString &origin_pattern,
                         common::ObString &pattern);
  int check_icu_regexp_status(UErrorCode u_error_code, const UParseError *parse_error = NULL) const;
  int get_valid_unicode_string(ObExprStringBuf &string_buf,
                               const ObString &origin_str,
                               UChar *&u_str,
                               int32_t &u_str_len) const;
  int get_valid_replace_string(ObIAllocator &alloc,
                               const ObString &origin_replace,
                               UChar *&u_replace,
                               int32_t &u_replace_len) const;
private:
  bool inited_;
  ObInplaceAllocator pattern_allocator_;
  common::ObString pattern_;
  int cflags_;
  ObInplaceAllocator pattern_wc_allocator_;
  URegularExpression *regexp_engine_;
};

#if defined(__x86_64__)
class ObExprHsRegexCtx : public ObExprOperatorCtx
{
public:
  ObExprHsRegexCtx();
  virtual ~ObExprHsRegexCtx();
public:
  inline bool is_inited() const { return inited_; }
  static int get_regexp_flags(const ObString &match_param,
                              const bool is_case_sensitive,
                              const bool is_som_leftmost,
                              const bool is_single_match,
                              uint32_t &flags);
  int init(ObExprStringBuf &string_buf,
           const ObExprRegexpSessionVariables &regex_vars,
           const ObString &origin_pattern,
           const uint32_t flags,
           const bool reusable,
           const ObCollationType cs_type);
  int match(ObExprStringBuf &string_buf,
            const ObString &text,
            const ObCollationType cs_type,
            const int64_t start,
            bool &result) const;

  int find(ObExprStringBuf &string_buf,
           const ObString &text,
           const ObCollationType cs_type,
           const int64_t start,
           const int64_t occurrence,
           const int64_t return_option,
           const int64_t subexpr,
           int64_t &result) const;

  int count(ObExprStringBuf &string_buf,
            const ObString &text,
            const ObCollationType cs_type,
            const int32_t start,
            int64_t &result) const;

  int substr(ObExprStringBuf &string_buf,
             const ObString &text,
             const ObCollationType cs_type,
             const int64_t start,
             const int64_t occurrence,
             const int64_t subsexpr,
             ObString &result) const;
  int replace(ObExprStringBuf &string_buf,
              const ObString &text_string,
              const ObCollationType cs_type,
              const ObString &replace_string,
              const int64_t start,
              const int64_t occurrence,
              ObString &result) const;
  void destroy();
  void reset();
private:
  int check_hs_regexp_status(hs_error_t status) const;
private:
  struct MatchInfo
  {
    int32_t from_;
    int32_t to_;
    MatchInfo(int32_t from, int32_t to) : from_(from), to_(to) {}
    MatchInfo() : from_(-1), to_(-1) {}

    TO_STRING_KV(K_(from), K_(to));
  };
  using MatchChain = common::ObSEArray<MatchInfo, 16>;
  static int match_handler(unsigned int id, unsigned long long from, unsigned long long to,
                           unsigned int flags, void *ctx);

private:
  bool inited_;
  common::ObString pattern_;
  uint32_t hs_flags_;
  hs_database_t *hs_db_;
  hs_scratch_t *hs_scratch_;
  hs_compile_error_t *hs_compile_err_;
};
#else
  // empty class
  class ObExprHsRegexCtx {};
#endif
}
}

#endif //OCEANBASE_SQL_ENGINE_REGEX_OB_POSIX_REGEX_

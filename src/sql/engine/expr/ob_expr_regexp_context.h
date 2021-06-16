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

#ifndef OCEANBASE_SQL_ENGINE_REGEX_OB_POSIX_REGEX_
#define OCEANBASE_SQL_ENGINE_REGEX_OB_POSIX_REGEX_
#include "easy_define.h"  // for conflict of macro likely
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_mysql_global.h"
#include "lib/charset/ob_ctype.h"
#include <sys/types.h>
#include <assert.h>
#include "lib/charset/ob_charset.h"
#include "lib/regex/regex/regalone.h"
#include "lib/regex/regex/ob_regex.h"
#include "sql/engine/expr/ob_expr_operator.h"

// this regex is compatible with mysql 5.6.16

namespace oceanbase {
namespace sql {

// Return same address if alloc size less than reserved size.
// Prepare is needed for every allocation.
class ObInplaceAllocator : public common::ObIAllocator {
public:
  ObInplaceAllocator() : alloc_(NULL), mem_(NULL), len_(0)
  {}

  void prepare(common::ObIAllocator& alloc)
  {
    alloc_ = &alloc;
  }

  virtual void* alloc(const int64_t size) override;

private:
  common::ObIAllocator* alloc_;
  void* mem_;
  int64_t len_;
};

class ObExprRegexContext : public ObExprOperatorCtx {
public:
  ObExprRegexContext();
  virtual ~ObExprRegexContext();

public:
  inline bool is_inited() const
  {
    return inited_;
  }
  void destroy();
  void reset();

  // The previous regex compile result can be used if pattern not change, if %reusable is true.
  // %string_buf must be the same with previous init too if %reusable is true.
  int init(const common::ObString& pattern, int cflags, common::ObExprStringBuf& string_buf, const bool reusable);

  int match(
      const common::ObString& text, int64_t start_offset, bool& is_match, common::ObExprStringBuf& string_buf) const;
  int substr(const common::ObString& text, int64_t occurrence, int64_t subexpr, common::ObString& sub,
      common::ObExprStringBuf& string_buf, bool from_begin, bool from_end,
      common::ObSEArray<uint32_t, 4>& begin_locations) const;
  int count_match_str(const common::ObString& text, int64_t subexpr, int64_t& sub, common::ObExprStringBuf& string_buf,
      bool from_begin, bool from_end, common::ObSEArray<uint32_t, 4>& begin_locations) const;
  int instr(const common::ObString& text, int64_t occurrence, int64_t return_option, int64_t subexpr, int64_t& sub,
      common::ObExprStringBuf& string_buf, bool from_begin, bool from_end,
      common::ObSEArray<uint32_t, 4>& begin_locations) const;
  int like(const common::ObString& text, int64_t occurrence, bool& sub, common::ObExprStringBuf& string_buf,
      bool from_begin, bool from_end, common::ObSEArray<uint32_t, 4>& begin_locations) const;
  int replace_substr(const common::ObString& text_string, const common::ObString& replace_string, int64_t occurrence,
      common::ObExprStringBuf& string_buf, common::ObIArray<size_t>& ch_position, common::ObString& sub,
      bool from_begin, bool from_end, common::ObSEArray<uint32_t, 4>& begin_locations) const;
  int replace(const common::ObString& text, const common::ObString& to,
      common::ObSEArray<uint32_t, 4>& locations_and_length, common::ObExprStringBuf& string_buf,
      common::ObIArray<common::ObSEArray<common::ObString, 8> >& subexpr_arrays, common::ObString& sub) const;
  int pre_process_replace_str(const common::ObString& text, const common::ObString& to,
      common::ObExprStringBuf& string_buf, common::ObIArray<common::ObSEArray<common::ObString, 8> >& subexpr_arrays,
      common::ObIArray<common::ObString>& subexpr_array) const;
  int extract_subpre_string(const wchar_t* wc_text, int64_t wc_length, int64_t start_pos, ob_regmatch_t pmatch[],
      uint64_t pmatch_size, common::ObExprStringBuf& string_buf,
      common::ObIArray<common::ObString>& subexpr_array) const;
  TO_STRING_KV(K_(inited));

private:
  void reset_reg();
  int getwc(const common::ObString& text, wchar_t*& wc, int64_t& wc_length, common::ObExprStringBuf& string_buf) const;
  int w2c(
      const wchar_t* wc, int64_t length, char*& chr, int64_t& chr_length, common::ObExprStringBuf& string_buf) const;
  int convert_reg_err_code_to_ob_err_code(int reg_err) const;

private:
  bool inited_;
  ob_regex_t reg_;

  ObInplaceAllocator pattern_allocator_;
  common::ObString pattern_;

  ObInplaceAllocator pattern_wc_allocator_;
};
}  // namespace sql
}  // namespace oceanbase

#endif  // OCEANBASE_SQL_ENGINE_REGEX_OB_POSIX_REGEX_

/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_DAS

#include "sql/das/search/ob_das_msm_runtime_util.h"

#include <cctype>
#include <climits>

#include "share/datum/ob_datum.h"
#include "lib/alloc/alloc_assist.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_fast_convert.h"
#include "lib/utility/utility.h"
#include "common/ob_smart_call.h"

namespace oceanbase
{
namespace sql
{
static int ob_das_msm_parse_int32_decimal_strict(const common::ObString &s, int32_t &out)
{
  int ret = OB_SUCCESS;
  if (s.empty() || s.length() >= 32) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("minimum_should_match decimal token empty or exceeds strict parse length limit",
             KR(ret), K(s.length()));
  } else {
    bool valid = false;
    const char *const p = s.ptr();
    const char *const e = s.ptr() + s.length();
    const int32_t v = common::ObFastAtoi<int32_t>::atoi(p, e, valid);
    if (!valid) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("minimum_should_match decimal token is not a valid strict int32", KR(ret), K(s));
    } else {
      out = v;
    }
  }
  return ret;
}

static int ob_das_msm_normalize_spaces_around_lt(
    const common::ObString &spec, char *normalize_buf, const int buf_cap, int &out_normalized_len)
{
  int ret = OB_SUCCESS;
  out_normalized_len = 0;
  if (buf_cap <= 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("minimum_should_match '<' normalization buffer capacity invalid", KR(ret), K(buf_cap));
  } else {
    int write_pos = 0;
    int64_t i = 0;
    const int64_t L = spec.length();
    const int cap = buf_cap;
    for (; OB_SUCC(ret) && i < L && write_pos < cap - 1;) {
      const unsigned char c = static_cast<unsigned char>(spec[i]);
      if (c == '<') {
        while (write_pos > 0 && isspace(static_cast<unsigned char>(normalize_buf[write_pos - 1])) != 0) {
          --write_pos;
        }
        normalize_buf[write_pos++] = '<';
        ++i;
        while (i < L && isspace(static_cast<unsigned char>(spec[i])) != 0) {
          ++i;
        }
      } else if (isspace(c) != 0) {
        int64_t j = i;
        while (j < L && isspace(static_cast<unsigned char>(spec[j])) != 0) {
          ++j;
        }
        if (j < L && spec[j] == '<') {
          i = j;
        } else {
          if (write_pos > 0) {
            normalize_buf[write_pos++] = ' ';
          }
          i = j;
        }
      } else {
        normalize_buf[write_pos++] = static_cast<char>(c);
        ++i;
      }
    }
    if (OB_SUCC(ret) && i < L) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("minimum_should_match '<' normalization exceeded buffer or invalid structure", KR(ret), K(L), K(cap));
    }
    out_normalized_len = write_pos;
  }
  return ret;
}

// Compact "70   %" / "-10 %" style gaps into "70%" / "-10%" so split_on(' ') still sees one token per msm clause.
static void ob_das_msm_collapse_spaces_before_percent_inplace(char *buf, int &len)
{
  int w = 0;
  int r = 0;
  const int L = len;
  while (r < L) {
    bool neg = false;
    if (buf[r] == '-') {
      if (r + 1 >= L || !isdigit(static_cast<unsigned char>(buf[r + 1]))) {
        buf[w++] = buf[r++];
        continue;
      }
      neg = true;
      ++r;
    }
    if (!isdigit(static_cast<unsigned char>(buf[r]))) {
      buf[w++] = buf[r++];
      continue;
    }
    const int d0 = r;
    while (r < L && isdigit(static_cast<unsigned char>(buf[r]))) {
      ++r;
    }
    const int d1 = r;
    int sp = r;
    while (sp < L && isspace(static_cast<unsigned char>(buf[sp]))) {
      ++sp;
    }
    if (sp < L && buf[sp] == '%') {
      if (neg) {
        buf[w++] = '-';
      }
      for (int k = d0; k < d1; ++k) {
        buf[w++] = buf[k];
      }
      buf[w++] = '%';
      r = sp + 1;
    } else {
      if (neg) {
        buf[w++] = '-';
      }
      for (int k = d0; k < d1; ++k) {
        buf[w++] = buf[k];
      }
      r = d1;
    }
  }
  len = w;
}

int ObDASMinShouldMatchSpec::string_calc(common::ObIAllocator &alloc,
                                         const int64_t optional_clause_count,
                                         const common::ObString &spec_in,
                                         int64_t &msm_result)
{
  int ret = OB_SUCCESS;
  int64_t result = optional_clause_count;
  const common::ObString spec = spec_in.trim();

  if (spec.empty() || optional_clause_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
    LOG_WARN("invalid minimum should match specification", KR(ret), K(spec));
  } else if (nullptr != spec.find('<')) {
    const int32_t spec_len = spec.length();
    const int32_t lt_scratch_bytes = spec_len + 2;
    char *lt_normalized_buf = nullptr;
    int lt_normalized_len = 0;
    common::ObSEArray<common::ObString, 32> space_segments;
    bool upper_bound_matched = false;
    if (OB_ISNULL(lt_normalized_buf = static_cast<char *>(alloc.alloc(lt_scratch_bytes)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", KR(ret), K(lt_scratch_bytes));
    } else if (OB_FAIL(ob_das_msm_normalize_spaces_around_lt(spec, lt_normalized_buf,
                       static_cast<int>(lt_scratch_bytes), lt_normalized_len))) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
      LOG_WARN("fail to normalize spaces around lt", KR(ret));
    } else {
      ob_das_msm_collapse_spaces_before_percent_inplace(lt_normalized_buf, lt_normalized_len);
      common::ObString spec_for_space_split(lt_normalized_len, lt_normalized_buf);
      if (OB_FAIL(oceanbase::common::split_on(spec_for_space_split, ' ', space_segments))) {
        LOG_WARN("fail to split msm spec by space", KR(ret));
      }
      for (int32_t seg_idx = 0; OB_SUCC(ret) && !upper_bound_matched && seg_idx < space_segments.count(); ++seg_idx) {
        common::ObString rhs_after_first_lt = space_segments[seg_idx];
        const common::ObString upper_bound_str = rhs_after_first_lt.split_on('<');
        int32_t upper_bound = 0;
        if (upper_bound_str.empty() || rhs_after_first_lt.empty()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
          LOG_WARN("invalid minimum_should_match specification", KR(ret), K(spec));
        } else if (OB_FAIL(ob_das_msm_parse_int32_decimal_strict(upper_bound_str, upper_bound))) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
        } else if (optional_clause_count <= upper_bound) {
          upper_bound_matched = true;
        } else if (OB_FAIL(SMART_CALL(ObDASMinShouldMatchSpec::string_calc(
                           alloc, optional_clause_count, rhs_after_first_lt, result)))) {
          LOG_WARN("fail to calculate min should match", KR(ret));
        }
      }
    }
  } else if (nullptr != spec.find('%')) {
    const int32_t spec_len = spec.length();
    char *pct_fold_buf = nullptr;
    if (spec_len <= 0) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
      LOG_WARN("invalid minimum_should_match specification", KR(ret), K(spec));
    } else if (OB_ISNULL(pct_fold_buf = static_cast<char *>(alloc.alloc(spec_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", KR(ret), K(spec_len));
    } else {
      MEMCPY(pct_fold_buf, spec.ptr(), spec_len);
      int pct_fold_len = spec_len;
      ob_das_msm_collapse_spaces_before_percent_inplace(pct_fold_buf, pct_fold_len);
      common::ObString pct_spec_trimmed(pct_fold_len, pct_fold_buf);
      if (pct_spec_trimmed.length() < 2 || pct_spec_trimmed[pct_spec_trimmed.length() - 1] != '%') {
        ret = OB_INVALID_ARGUMENT;
        LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
        LOG_WARN("invalid minimum_should_match percentage specification", KR(ret), K(spec));
      } else {
        const common::ObString pct_spec(static_cast<int32_t>(pct_spec_trimmed.length() - 1), pct_spec_trimmed.ptr());
        int32_t percent = 0;
        if (OB_FAIL(ob_das_msm_parse_int32_decimal_strict(pct_spec, percent))) {
          LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
        } else {
          const float ratio = static_cast<float>(result * percent) * (1.0f / 100.0f);
          result = (ratio < 0.0f) ? result + static_cast<int64_t>(ratio) : static_cast<int64_t>(ratio);
        }
      }
    }
  } else {
    int32_t parsed_int = 0;
    if (OB_FAIL(ob_das_msm_parse_int32_decimal_strict(spec, parsed_int))) {
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
    } else {
      const int64_t parsed_i64 = static_cast<int64_t>(parsed_int);
      result = (parsed_i64 < 0) ? result + parsed_i64 : parsed_i64;
    }
  }

  if (OB_FAIL(ret)) {
    if (optional_clause_count < 0) {
      msm_result = 0;
    }
  } else {
    msm_result = (result < 0) ? 0 : result;
  }
  return ret;
}

int ObDASMinShouldMatchSpec::int_calc(const int64_t optional_clause_count,
                                      const int64_t raw_int,
                                      int64_t &msm_out)
{
  int ret = OB_SUCCESS;
  if (optional_clause_count < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
    LOG_WARN("invalid optional_clause_count for int_calc", KR(ret), K(optional_clause_count));
  } else if (raw_int < static_cast<int64_t>(INT32_MIN) || raw_int > static_cast<int64_t>(INT32_MAX)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid minimum_should_match specification");
    LOG_WARN("integral msm out of int32 range", KR(ret), K(raw_int));
  } else {
    const int64_t parsed_i64 = raw_int;
    int64_t result = optional_clause_count;
    result = (parsed_i64 < 0) ? result + parsed_i64 : parsed_i64;
    msm_out = (result < 0) ? 0 : result;
  }
  return ret;
}

int ObDASMSMRuntimeUtil::calc_bool_should_msm(
    common::ObIAllocator &alloc,
    const int64_t should_n,
    const bool is_msm_unresolved_expr,
    const common::ObDatum *msm_datum,
    int64_t &msm_out)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (OB_ISNULL(msm_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null msm_datum", KR(ret));
  } else if (is_msm_unresolved_expr) {
    common::ObArenaAllocator msm_parse_alloc;
    const common::ObString msm_spec = msm_datum->get_string().trim();
    if (OB_FAIL(ObDASMinShouldMatchSpec::string_calc(msm_parse_alloc, should_n, msm_spec, msm_out))) {
      LOG_WARN("bool minimum_should_match string_calc failed", KR(ret), K(should_n), K(msm_spec));
    }
  } else if (OB_FAIL(ObDASMinShouldMatchSpec::int_calc(should_n, msm_datum->get_int(), msm_out))) {
    LOG_WARN("bool minimum_should_match int_calc failed", KR(ret), K(should_n), K(msm_datum->get_int()));
  }
  // ES: effective MSM in [0, should_n]; same upper bound DisjunctionFilterOp used to apply in do_init.
  if (OB_SUCC(ret)) {
    msm_out = MAX(static_cast<int64_t>(0), MIN(msm_out, should_n));
  }
  return ret;
}

int ObDASMSMRuntimeUtil::calc_match_msm(
    common::ObIAllocator &alloc,
    const int64_t token_n,
    const bool is_msm_unresolved_expr,
    const common::ObDatum *msm_datum,
    int64_t &msm_out)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (OB_ISNULL(msm_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null msm_datum", KR(ret));
  } else if (is_msm_unresolved_expr) {
    common::ObArenaAllocator msm_parse_alloc;
    const common::ObString msm_spec = msm_datum->get_string().trim();
    if (OB_FAIL(ObDASMinShouldMatchSpec::string_calc(msm_parse_alloc, token_n, msm_spec, msm_out))) {
      LOG_WARN("minimum_should_match string_calc failed", KR(ret), K(token_n), K(msm_spec));
    }
  } else if (OB_FAIL(ObDASMinShouldMatchSpec::int_calc(token_n, msm_datum->get_int(), msm_out))) {
    LOG_WARN("minimum_should_match int_calc failed", KR(ret), K(token_n), K(msm_datum->get_int()));
  }
  // Match doc: MSM=0 -> 1; effective MSM in [1, token_n].
  if (OB_SUCC(ret)) {
    msm_out = MAX(static_cast<int64_t>(1), MIN(msm_out, token_n));
  }
  return ret;
}

int ObDASMSMRuntimeUtil::calc_multi_or_query_string_msm(
    common::ObIAllocator &alloc,
    const int64_t n,
    const bool is_msm_unresolved_expr,
    const common::ObDatum *msm_datum,
    int64_t &msm_out)
{
  int ret = OB_SUCCESS;
  UNUSED(alloc);
  if (OB_ISNULL(msm_datum)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null msm_datum", KR(ret));
  } else if (is_msm_unresolved_expr) {
    common::ObArenaAllocator msm_parse_alloc;
    const common::ObString msm_spec = msm_datum->get_string().trim();
    if (OB_FAIL(ObDASMinShouldMatchSpec::string_calc(msm_parse_alloc, n, msm_spec, msm_out))) {
      LOG_WARN("minimum_should_match string_calc failed", KR(ret), K(n), K(msm_spec));
    }
  } else if (OB_FAIL(ObDASMinShouldMatchSpec::int_calc(n, msm_datum->get_int(), msm_out))) {
    LOG_WARN("minimum_should_match int_calc failed", KR(ret), K(n), K(msm_datum->get_int()));
  }
  // Match / multi_match / query_string doc: MSM=0 -> 1; effective MSM in [1, n].
  if (OB_SUCC(ret)) {
    msm_out = MAX(static_cast<int64_t>(1), MIN(msm_out, n));
  }
  return ret;
}

}  // namespace sql
}  // namespace oceanbase

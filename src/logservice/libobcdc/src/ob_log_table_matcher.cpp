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
 *
 * Table Matcher
 */

#include "ob_log_table_matcher.h"

#include "share/ob_define.h"

#include "ob_log_utils.h"           // ob_cdc_malloc

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [TABLE_MATCHER] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)

namespace oceanbase
{
namespace libobcdc
{
using namespace common;

ObLogTableMatcher::ObLogTableMatcher() :
    patterns_(),
    buf_(NULL),
    buf_size_(0),
    black_patterns_(),
    black_buf_(NULL),
    black_buf_size_(0),
    pg_patterns_(),
    pg_buf_(NULL),
    pg_buf_size_(0),
    black_pg_patterns_(),
    black_pg_buf_(NULL),
    black_pg_buf_size_(0)
{ }

ObLogTableMatcher::~ObLogTableMatcher()
{
  (void)destroy();
}

int ObLogTableMatcher::table_match_pattern_(const bool is_black,
    const char* tenant_name,
    const char* db_name,
    const char* tb_name,
    bool& matched,
    const int fnmatch_flags)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_name) || OB_ISNULL(db_name) || OB_ISNULL(tb_name)) {
    OBLOG_LOG(ERROR, "invalid arguments", K(tenant_name), K(db_name), K(tb_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PatternArray *ptns = is_black ? &black_patterns_ : &patterns_;

    matched = false;

    for (int64_t idx = 0, cnt = ptns->count(); OB_SUCCESS == ret && !matched && idx < cnt; ++idx) {
      const Pattern &pattern = ptns->at(idx);
      int err = 0;
      const char *not_match_part = "UNKNOW";

      // Tenant name.
      if (0 != (err = fnmatch(pattern.tenant_pattern_.ptr(),
          tenant_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "TENANT_PATTERN";
      }
      // Database name.
      else if (0 != (err = fnmatch(pattern.database_pattern_.ptr(),
          db_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "DATABASE_PATTERN";
      }
      // Table name.
      else if (0 != (err = fnmatch(pattern.table_pattern_.ptr(),
          tb_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "TABLE_PATTERN";
      }
      else {
        // Matched.
        matched = true;
      }

      if (matched) {
        _ISTAT("[%s_PATTERN_MATCHED] PATTERN='%s.%s.%s' TABLE='%s.%s.%s'",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), pattern.database_pattern_.ptr(),
            pattern.table_pattern_.ptr(),
            tenant_name, db_name, tb_name);
      } else {
        _DSTAT("[%s_PATTERN_NOT_MATCH] PATTERN='%s.%s.%s' TABLE='%s.%s.%s' NOT_MATCH_PATTERN=%s",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), pattern.database_pattern_.ptr(),
            pattern.table_pattern_.ptr(),
            tenant_name, db_name, tb_name,
            not_match_part);
      }

      // fnmatch() err.
      // OB_SUCCESS == 0.
      if (OB_SUCCESS != err && FNM_NOMATCH != err) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "err exec fnmatch", KR(ret), K(err));
      }
    }
  }

  return ret;
}

int ObLogTableMatcher::database_match_pattern_(const bool is_black,
    const char *tenant_name,
    const char *db_name,
    bool &matched,
    const int fnmatch_flags)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_name) || OB_ISNULL(db_name)) {
    ret = OB_INVALID_ARGUMENT;
    OBLOG_LOG(ERROR, "invalid arguments", KR(ret), K(tenant_name), K(db_name));
  } else {
    PatternArray *ptns = is_black ? &black_patterns_ : &patterns_;

    matched = false;

    for (int64_t idx = 0, cnt = ptns->count(); OB_SUCCESS == ret && !matched && idx < cnt; ++idx) {
      const Pattern &pattern = ptns->at(idx);
      int err = 0;
      const char *not_match_part = "UNKNOW";
      // Tenant name.
      if (0 != (err = fnmatch(pattern.tenant_pattern_.ptr(),
          tenant_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "TENANT_PATTERN";
      }
      // Database name.
      else if (0 != (err = fnmatch(pattern.database_pattern_.ptr(),
          db_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "DATABASE_PATTERN";
      } else {
        matched = true;
      }

      if (matched) {
        _ISTAT("[%s_PATTERN_MATCHED] PATTERN='%s.%s' DATABASE='%s.%s'",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), pattern.database_pattern_.ptr(),
            tenant_name, db_name);
      } else {
        _DSTAT("[%s_PATTERN_NOT_MATCH] PATTERN='%s.%s' DATABASE='%s.%s' NOT_MATCH_PATTERN=%s",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), pattern.database_pattern_.ptr(),
            tenant_name, db_name,
            not_match_part);
      }

      // fnmatch() err.
      // OB_SUCCESS == 0.
      if (OB_SUCCESS != err && FNM_NOMATCH != err) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "err exec fnmatch", KR(ret), K(err));
      }
    }
  }
  return ret;
}

int ObLogTableMatcher::tenant_match_pattern_(const bool is_black,
    const char* tenant_name,
    bool& matched,
    const int fnmatch_flags)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_name)) {
    OBLOG_LOG(ERROR, "invalid arguments", K(tenant_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PatternArray *ptns = is_black ? &black_patterns_ : &patterns_;

    matched = false;

    for (int64_t idx = 0, cnt = ptns->count(); OB_SUCCESS == ret && !matched && idx < cnt; ++idx) {
      const Pattern &pattern = ptns->at(idx);
      int err = 0;

      // Tenant name.
      if (0 != (err = fnmatch(pattern.tenant_pattern_.ptr(),
          tenant_name,
          fnmatch_flags))) {
        // Not matched.
      }
      else {
        // Matched.
        matched = true;

        // 这里影响tenant_id集合的维护和DDL语句的过滤逻辑
        // 如果是黑名单匹配:
        // 1. tt1.*.*格式，说明该租户全部数据要过滤
        // 2. tt1.*.*_t格式，只过滤该租户_t结尾的表数据，但是租户信息不会过滤
        //    同样tt1.db1.*格式，只过滤该租户db1的全部数据, 但是租户信息不会过滤
        // 3. 配置影子表：*.*.*_t|*.*.*_[0-9][a-z], 此时对于租户黑名单匹配时候,不会过滤
        if (is_black) {
          // 始终保证pattern都是\0结尾的字符串,参见build_patterns_()实现
          // 因此构建相同的格式, ObString::case_compare会比较长度
          char tmp_str[2];
          tmp_str[0] = '*';
          tmp_str[1] = '\0';
          ObString match_all_str(0, 2, tmp_str);

          if ((0 == strcmp(pattern.database_pattern_.ptr(), tmp_str))
              && (0 == strcmp(pattern.table_pattern_.ptr(), tmp_str))) {
            // 依赖fnmatch匹配结果
          } else {
            // 只要存在db或者table不是*,那么说明该租户不能过滤
            matched = false;
          }
        }
      }

      if (matched) {
        _ISTAT("[%s_PATTERN_MATCHED] PATTERN='%s' TENANT='%s'",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), tenant_name);
      } else {
        _DSTAT("[%s_PATTERN_NOT_MATCH] PATTERN='%s' TENANT='%s'",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), tenant_name);
      }

      // fnmatch() err.
      // OB_SUCCESS == 0.
      if (OB_SUCCESS != err && FNM_NOMATCH != err) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "err exec fnmatch", KR(ret), K(err));
      }
    }
  }

  return ret;
}

int ObLogTableMatcher::table_match(const char* tenant_name,
                             const char* db_name,
                             const char* tb_name,
                             bool& matched,
                             const int fnmatch_flags)
{
  int ret = OB_SUCCESS;
  bool white_matched = false;
  bool black_matched = false;

  matched = false;

  // First filter by whitelist, if whitelist matches, match blacklist
  if (OB_FAIL(table_match_pattern_(false, tenant_name, db_name, tb_name, white_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match white pattern fail", KR(ret), K(tenant_name), K(db_name), K(tb_name),
        K(white_matched), K(fnmatch_flags));
  } else if (white_matched && OB_FAIL(table_match_pattern_(true, tenant_name, db_name, tb_name,
      black_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match black pattern fail", KR(ret), K(tenant_name), K(db_name), K(tb_name),
        K(white_matched), K(fnmatch_flags));
  } else {
    matched = (white_matched && ! black_matched);

    _ISTAT("[%sTABLE_PATTERNS_MATCHED] TABLE='%s.%s.%s' WHITE_PATTERN_COUNT=%ld "
        "BLACK_PATTERN_COUNT=%ld WHITE_MATCHED=%d BLACK_MATCHED=%d",
        matched ? "" : "NO_",
        tenant_name, db_name, tb_name, patterns_.count(), black_patterns_.count(),
        white_matched, black_matched);
  }

  return ret;
}

int ObLogTableMatcher::database_match(const char *tenant_name,
    const char *db_name,
    bool &matched,
    const int fnmatch_flags)
{
  int ret = OB_SUCCESS;
  bool white_matched = false;
  bool black_matched = false;

  matched = false;

  // First filter by whitelist, if whitelist matches, match blacklist
  if (OB_FAIL(database_match_pattern_(false, tenant_name, db_name, white_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match white pattern fail", KR(ret), K(tenant_name), K(db_name),
        K(white_matched), K(fnmatch_flags));
  } else if (white_matched && OB_FAIL(database_match_pattern_(true, tenant_name, db_name,
      black_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match black pattern fail", KR(ret), K(tenant_name), K(db_name),
        K(white_matched), K(fnmatch_flags));
  } else {
    matched = (white_matched && ! black_matched);
    _ISTAT("[%sDATABASE_PATTERNS_MATCHED] DATABASE='%s.%s' WHITE_PATTERN_COUNT=%ld "
        "BLACK_PATTERN_COUNT=%ld WHITE_MATCHED=%d BLACK_MATCHED=%d",
        matched ? "" : "NO_",
        tenant_name, db_name, patterns_.count(), black_patterns_.count(),
        white_matched, black_matched);
  }

  return ret;
}

int ObLogTableMatcher::tenant_match(const char* tenant_name,
                             bool& matched,
                             const int fnmatch_flags)
{
  int ret = OB_SUCCESS;
  bool white_matched = false;
  bool black_matched = false;

  matched = false;

  // Tenant matching is only considered for whitelisting, as tenants may be duplicated
  // The tenant blacklist matching mechanism is supported
  // Aone:
  if (OB_FAIL(tenant_match_pattern_(false, tenant_name, white_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match white pattern fail", KR(ret), K(tenant_name), K(white_matched),
        K(fnmatch_flags));
  } else if (white_matched && OB_FAIL(tenant_match_pattern_(true, tenant_name, black_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "tenant match black pattern fail", K(ret), K(tenant_name), K(black_matched),
        K(fnmatch_flags));
  } else {
    //make blacklists always mismatch
    matched = (white_matched && ! black_matched);

    _ISTAT("[%sTENANT_PATTERNS_MATCHED] TENANT='%s' WHITE_PATTERN_COUNT=%ld "
        "BLACK_PATTERN_COUNT=%ld WHITE_MATCHED=%d BLACK_MATCHED=%d",
        matched ? "" : "NO_",
        tenant_name, patterns_.count(), black_patterns_.count(),
        white_matched, black_matched);
  }

  return ret;
}

int ObLogTableMatcher::match(const char* pattern1,
                             const ObIArray<ObString>& pattern2,
                             bool& matched,
                             const int fnmatch_flags)
{
  int ret = OB_SUCCESS;
  matched = false;

  // Param check.
  if (NULL == pattern1) {
    ret = OB_INVALID_ARGUMENT;
    OBLOG_LOG(ERROR, "invalid args", KR(ret), K(pattern1));
  } else if (pattern2.count() <= 0) {
    matched = false;
  } else {
    // Copy.
    char *pattern_buf = NULL;
    int64_t pattern_buf_size = 0;
    if (OB_SUCC(ret)) {
      int tmp_ret = 0;
      pattern_buf_size = 1 + static_cast<int64_t>(strlen(pattern1));
      if (NULL == (pattern_buf =
          reinterpret_cast<char*>(ob_cdc_malloc(pattern_buf_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        OBLOG_LOG(ERROR, "err alloc pattern buf", KR(ret), K(pattern_buf_size));
      }
      else if (pattern_buf_size <= (tmp_ret =
          snprintf(pattern_buf, pattern_buf_size, "%s", pattern1)) || (tmp_ret < 0)) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "err copy pattern", KR(ret), K(tmp_ret));
      }
      else {
        _OBLOG_LOG(INFO, "[STAT] [PATTERN_MATCH] PATTERN=%s", pattern_buf);
      }
    }

    // Cut pattern1.
    const char delimiter = '|';
    if (OB_SUCC(ret)) {
      for (int64_t idx = 0, cnt = pattern_buf_size; idx < cnt; ++idx) {
        char &cur = pattern_buf[idx];
        if (delimiter == cur) {
          cur = '\0';
        }
      }
    }

    // Match.
    int64_t iter = 0;
    while (OB_SUCCESS == ret && iter < pattern_buf_size && !matched) {
      const char *p1 = pattern_buf + iter;
      const char *p2 = NULL;
      for (int64_t idx = 0, cnt = pattern2.count();
          OB_SUCCESS == ret && idx < cnt && !matched;
          ++idx) {
        const ObString &pattern2_str = pattern2.at(idx);
        p2 = pattern2_str.ptr();
        // fnmatch.
        int tmp_ret = 0;
        if (0 == (tmp_ret = fnmatch(p1, p2, fnmatch_flags))) {
          matched = true;
        }
        else if (FNM_NOMATCH == tmp_ret) {
          // Not matched.
        }
        else {
          ret = OB_ERR_UNEXPECTED;
          OBLOG_LOG(ERROR, "err exec fnmatch", KR(ret), K(tmp_ret), K(p1), K(p2));
        }

        _OBLOG_LOG(INFO, "[STAT] [PATTERN_MATCH] MATCH('%s', '%s') => %s",
            p1, p2, matched ? "true" : "false");
      }
      // Move to next pattern.
      if (OB_SUCCESS == ret && !matched) {
        iter += (1 + static_cast<int64_t>(strlen(p1)));
      }
    }

    // Release mem.
    if (NULL != pattern_buf) {
      ob_cdc_free(pattern_buf);
      pattern_buf = NULL;
    }
  }

  return ret;
}

int ObLogTableMatcher::tablegroup_match(const char *tenant_name,
    const char *tablegroup_name,
    bool &matched,
    const int fnmatch_flags)
{
  int ret = OB_SUCCESS;
  bool white_matched = false;
  bool black_matched = false;

  matched = false;

  // First filter by whitelist, if whitelist matches, match blacklist
  if (OB_FAIL(tablegroup_match_pattern_(false, tenant_name, tablegroup_name, white_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match white pattern fail", KR(ret), K(tenant_name), K(tablegroup_name),
        K(white_matched), K(fnmatch_flags));
  } else if (white_matched && OB_FAIL(tablegroup_match_pattern_(true, tenant_name, tablegroup_name,
      black_matched, fnmatch_flags))) {
    OBLOG_LOG(ERROR, "match black pattern fail", KR(ret), K(tenant_name), K(tablegroup_name),
        K(white_matched), K(fnmatch_flags));
  } else {
    matched = (white_matched && ! black_matched);

    _ISTAT("[%sPG_PATTERNS_MATCHED] TABLEGROUP='%s.%s' WHITE_PATTERN_COUNT=%ld "
        "BLACK_PATTERN_COUNT=%ld WHITE_MATCHED=%d BLACK_MATCHED=%d",
        matched ? "" : "NO_",
        tenant_name, tablegroup_name, pg_patterns_.count(), black_pg_patterns_.count(),
        white_matched, black_matched);
  }

  return ret;
}

int ObLogTableMatcher::tablegroup_match_pattern_(const bool is_black,
    const char* tenant_name,
    const char* tablegroup_name,
    bool& matched,
    const int fnmatch_flags)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tenant_name) || OB_ISNULL(tablegroup_name)) {
    OBLOG_LOG(ERROR, "invalid arguments", K(tenant_name), K(tablegroup_name));
    ret = OB_INVALID_ARGUMENT;
  } else {
    PgPatternArray *ptns = is_black ? &black_pg_patterns_ : &pg_patterns_;

    matched = false;

    for (int64_t idx = 0, cnt = ptns->count(); OB_SUCC(ret) && !matched && idx < cnt; ++idx) {
      const PgPattern &pattern = ptns->at(idx);
      int err = 0;
      const char *not_match_part = "UNKNOW";

      // Tenant name.
      if (0 != (err = fnmatch(pattern.tenant_pattern_.ptr(),
          tenant_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "TENANT_PATTERN";
      }
      // TableGroup name.
      else if (0 != (err = fnmatch(pattern.tablegroup_pattern_.ptr(),
          tablegroup_name,
          fnmatch_flags))) {
        // Not matched.
        not_match_part = "TABLEGROUP_PATTERN";
      }
      else {
        // Matched.
        matched = true;
      }

      if (matched) {
        _ISTAT("[%s_PG_PATTERN_MATCHED] PATTERN='%s.%s' TABLEGROUP='%s.%s'",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), pattern.tablegroup_pattern_.ptr(),
            tenant_name, tablegroup_name);
      } else {
        _ISTAT("[%s_PG_PATTERN_NOT_MATCH] PATTERN='%s.%s' TABLEGROUP='%s.%s' NOT_MATCH_PATTERN=%s",
            is_black ? "BLACK" : "WHITE",
            pattern.tenant_pattern_.ptr(), pattern.tablegroup_pattern_.ptr(),
            tenant_name, tablegroup_name,
            not_match_part);
      }

      // fnmatch() err.
      // OB_SUCCESS == 0.
      if (OB_SUCCESS != err && FNM_NOMATCH != err) {
        ret = OB_ERR_UNEXPECTED;
        OBLOG_LOG(ERROR, "err exec fnmatch", KR(ret), K(err));
      }
    } // for
  }

  return ret;
}

int ObLogTableMatcher::cluster_match(bool &matched)
{
  int ret = OB_SUCCESS;
  matched = false;
  const int64_t idx = 0;

  if (1 != patterns_.count()) {
    matched = false;
  } else {
    const Pattern &pattern = patterns_.at(idx);
    // always ensure that patterns are strings ending in \0, see build_patterns_() for implementation
    // so build the same format, ObString::case_compare will compare the lengths
    char tmp_str[2];
    tmp_str[0] = '*';
    tmp_str[1] = '\0';
    ObString match_all_str(0, 2, tmp_str);

    if ((0 == pattern.tenant_pattern_.case_compare(match_all_str))
        && (0 == pattern.database_pattern_.case_compare(match_all_str))
        && (0 == pattern.table_pattern_.case_compare(match_all_str))
        && (0 == strcmp(black_buf_, "|"))) {
      OBLOG_LOG(INFO, "[TABLE_MATCHER] cluster_match succ", K(pattern), K(black_buf_));
      matched = true;
    } else {
      OBLOG_LOG(INFO, "[TABLE_MATCHER] cluster_match false", K(pattern), K(black_buf_));
      matched = false;
    }
  }

  return ret;
}

int ObLogTableMatcher::init(const char *tb_white_list,
    const char *tb_black_list,
    const char *tg_white_list,
    const char *tg_black_list)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(tb_white_list) || OB_ISNULL(tb_black_list)
      || OB_ISNULL(tg_white_list) || OB_ISNULL(tg_black_list)) {
    ret = OB_INVALID_ARGUMENT;
    OBLOG_LOG(ERROR, "invalid_argument", KR(ret), K(tb_white_list), K(tb_black_list),
        K(tg_white_list), K(tg_black_list));
  } else if (OB_FAIL(set_pattern_(tb_white_list))) {
    OBLOG_LOG(ERROR, "set table white list pattern fail", KR(ret), K(tb_white_list));
  } else if (OB_FAIL(set_black_pattern_(tb_black_list))) {
    OBLOG_LOG(ERROR, "set table black list pattern fail", KR(ret), K(tb_black_list));
  } else if (OB_FAIL(set_pg_pattern_(tg_white_list))) {
    OBLOG_LOG(ERROR, "set tablegroup white list pattern fail", KR(ret), K(tg_white_list));
  } else if (OB_FAIL(set_black_pg_pattern_(tg_black_list))) {
    OBLOG_LOG(ERROR, "set tablegroup black list pattern fail", KR(ret), K(tg_black_list));
  } else {
    // succ
  }

  return ret;
}

int ObLogTableMatcher::destroy()
{
  int ret = OB_SUCCESS;

  // Free pattern buffer.
  if (NULL != buf_) {
    ob_cdc_free(buf_);
    buf_ = NULL;
  }

  buf_size_ = 0;

  // Free pattern array.
  patterns_.reset();

  // Free pattern buffer.
  if (NULL != black_buf_) {
    ob_cdc_free(black_buf_);
    black_buf_ = NULL;
  }

  black_buf_size_ = 0;

  black_patterns_.reset();

  return ret;
}

int ObLogTableMatcher::set_pattern_(const char* pattern_str)
{
  bool is_black = false;
  bool is_pg = false;

  return set_pattern_internal_(pattern_str, is_pg, is_black);
}

int ObLogTableMatcher::set_black_pattern_(const char* black_pattern_str)
{
  bool is_black = true;
  bool is_pg = false;

  return set_pattern_internal_(black_pattern_str, is_pg, is_black);
}

int ObLogTableMatcher::set_pg_pattern_(const char* pattern_str)
{
  bool is_black = false;
  bool is_pg = true;

  return set_pattern_internal_(pattern_str, is_pg, is_black);
}

int ObLogTableMatcher::set_black_pg_pattern_(const char* black_pattern_str)
{
  bool is_black = true;
  bool is_pg = true;

  return set_pattern_internal_(black_pattern_str, is_pg, is_black);
}

int ObLogTableMatcher::set_pattern_internal_(const char* pattern_str,
    const bool is_pg,
    const bool is_black)
{
  int ret = OB_SUCCESS;
  char **buffer = NULL;
  int64_t *buffer_size = NULL;

  if (! is_pg) {
    buffer = is_black ? &black_buf_ : &buf_;
    buffer_size = is_black ? &black_buf_size_ : &buf_size_;
  } else {
    buffer = is_black ? &black_pg_buf_ : &pg_buf_;
    buffer_size = is_black ? &black_pg_buf_size_ : &pg_buf_size_;
  }

  if (OB_ISNULL(pattern_str)) {
    ret = OB_INVALID_ARGUMENT;
    OBLOG_LOG(ERROR, "NULL pattern", KR(ret), K(pattern_str));
  }
  // Copy pattern string.
  else {
    int tmp_ret = 0;
    *buffer_size = strlen(pattern_str) + 1;
    // Alloc buffer.
    if (OB_ISNULL(*buffer = reinterpret_cast<char*>(ob_cdc_malloc(*buffer_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      OBLOG_LOG(ERROR, "err alloc pattern string buffer", KR(ret), K(buffer_size));
    }
    // Copy.
    else if (*buffer_size <= (tmp_ret = snprintf(*buffer, *buffer_size, "%s", pattern_str))
             || (tmp_ret < 0)) {
      ret = OB_ERR_UNEXPECTED;
      OBLOG_LOG(ERROR, "err snprintf", KR(ret), K(tmp_ret), K(buffer_size), K(pattern_str));
    }
    else {
      OBLOG_LOG(DEBUG, "pattern string", K(pattern_str), K(is_black));
    }
  }

  if (OB_SUCC(ret)) {
    // Split string.
    if (! is_pg) {
      if (OB_FAIL(build_patterns_(is_black))) {
        OBLOG_LOG(ERROR, "err build patterns", KR(ret), K(is_pg), K(is_black));
      }
    } else {
      if (OB_FAIL(build_pg_patterns_(is_black))) {
        OBLOG_LOG(ERROR, "err build patterns", KR(ret), K(is_pg), K(is_black));
      }
    }
  }

  // Free buf on error.
  if (OB_SUCCESS != ret && NULL != *buffer) {
    ob_cdc_free(*buffer);
    *buffer = NULL;
    *buffer_size = 0;
  }

  return ret;
}

int ObLogTableMatcher::build_patterns_(const bool is_black)
{
  int ret = OB_SUCCESS;
  const char pattern_delimiter = '|';
  const char name_delimiter = '.';
  bool done = false;

  PatternArray *ptrn_array = is_black ? &black_patterns_ : &patterns_;

  if ((is_black && OB_ISNULL(black_buf_)) || (! is_black && OB_ISNULL(buf_))) {
    OBLOG_LOG(ERROR, "invalid buffer", K(is_black), K(black_buf_), K(buf_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    char **buffer = is_black ? &black_buf_ : &buf_;

    ObString remain(strlen(*buffer), *buffer);
    Pattern pattern;
    ObString cur_pattern;

    // Skip empty blacklist
    if (0 == strcmp(*buffer, "|")) {
      done = true;
    }

    while (OB_SUCCESS == ret && !done) {
      // Split Pattern & get current pattern.
      cur_pattern = remain.split_on(pattern_delimiter);
      if (cur_pattern.empty()) {
        cur_pattern = remain;
        done = true;
      }
      if (OB_SUCC(ret)) {
        ObString &str = cur_pattern;
        *(str.ptr() + str.length()) = '\0';
        str.set_length(1 + str.length());
      }

      // Split names.
      pattern.reset();
      // Tenant name.
      if (OB_SUCC(ret)) {
        pattern.tenant_pattern_ = cur_pattern.split_on(name_delimiter);
        if (pattern.tenant_pattern_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          OBLOG_LOG(ERROR, "invalid argment", KR(ret), K(cur_pattern));
        }
        else {
          ObString &str = pattern.tenant_pattern_;
          *(str.ptr() + str.length()) = '\0';
           // Here set_length does not change the length, because the split_on implementation ensures that the buffer_size and length are the same
           // set_length will check the buffer_size. redirect
          str.assign_ptr(str.ptr(), 1 + str.length());
        }
      }
      // Database name.
      if (OB_SUCC(ret)) {
        pattern.database_pattern_ = cur_pattern.split_on(name_delimiter);
        if (pattern.database_pattern_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          OBLOG_LOG(ERROR, "invalid argment", KR(ret), K(cur_pattern));
        }
        else {
          ObString &str = pattern.database_pattern_;
          *(str.ptr() + str.length()) = '\0';
           // Here set_length does not change the length, because the split_on implementation ensures that the buffer_size and length are the same
           // set_length will check the buffer_size. redirect
          str.assign_ptr(str.ptr(), 1 + str.length());
        }
      }
      // Table name.
      if (OB_SUCC(ret)) {
        pattern.table_pattern_= cur_pattern;
        if (pattern.table_pattern_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          OBLOG_LOG(ERROR, "invalid argment", KR(ret), K(cur_pattern));
        }
        else {
          ObString &str = pattern.table_pattern_;
          *(str.ptr() + str.length()) = '\0';
          str.assign_ptr(str.ptr(), 1 + str.length());
        }
      }

      if (OB_SUCC(ret)) {
        // Save pattern.
        if (OB_SUCCESS != (ret = ptrn_array->push_back(pattern))) {
          OBLOG_LOG(ERROR, "err push back pattern", KR(ret));
        }
        else {
          _ISTAT("[ADD_PATTERN] IS_BLACK=%d TENANT='%s' DATABASE='%s' TABLE='%s'",
              is_black,
              pattern.tenant_pattern_.ptr(),
              pattern.database_pattern_.ptr(),
              pattern.table_pattern_.ptr());
        }
      }
    } // while
  }

  return ret;
}

int ObLogTableMatcher::build_pg_patterns_(const bool is_black)
{
  int ret = OB_SUCCESS;
  const char pattern_delimiter = '|';
  const char name_delimiter = '.';
  bool done = false;

  PgPatternArray *ptrn_array = is_black ? &black_pg_patterns_ : &pg_patterns_;

  if ((is_black && OB_ISNULL(black_pg_buf_)) || (! is_black && OB_ISNULL(pg_buf_))) {
    OBLOG_LOG(ERROR, "invalid buffer", K(is_black), K(black_pg_buf_), K(pg_buf_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    char **buffer = is_black ? &black_pg_buf_ : &pg_buf_;

    ObString remain(strlen(*buffer), *buffer);
    PgPattern pattern;
    ObString cur_pattern;

    // skip empty blacklist
    if (0 == strcmp(*buffer, "|")) {
      done = true;
    }

    while (OB_SUCCESS == ret && !done) {
      // Split Pattern & get current pattern.
      cur_pattern = remain.split_on(pattern_delimiter);
      if (cur_pattern.empty()) {
        cur_pattern = remain;
        done = true;
      }
      if (OB_SUCC(ret)) {
        ObString &str = cur_pattern;
        *(str.ptr() + str.length()) = '\0';
        str.set_length(1 + str.length());
      }

      // Split names.
      pattern.reset();
      // Tenant name.
      if (OB_SUCC(ret)) {
        pattern.tenant_pattern_ = cur_pattern.split_on(name_delimiter);
        if (pattern.tenant_pattern_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          OBLOG_LOG(ERROR, "invalid argment", KR(ret), K(cur_pattern));
        }
        else {
          ObString &str = pattern.tenant_pattern_;
          *(str.ptr() + str.length()) = '\0';
          str.set_length(1 + str.length());
        }
      }
      // Tablegroup name.
      if (OB_SUCC(ret)) {
        pattern.tablegroup_pattern_= cur_pattern;
        if (pattern.tablegroup_pattern_.empty()) {
          ret = OB_INVALID_ARGUMENT;
          OBLOG_LOG(ERROR, "invalid argment", KR(ret), K(cur_pattern));
        }
        else {
          ObString &str = pattern.tablegroup_pattern_;
          *(str.ptr() + str.length()) = '\0';
          str.set_length(1 + str.length());
        }
      }

      if (OB_SUCC(ret)) {
        // Save pattern.
        if (OB_FAIL(ptrn_array->push_back(pattern))) {
          OBLOG_LOG(ERROR, "err push back pattern", KR(ret));
        }
        else {
          _ISTAT("[ADD_PG_PATTERN] IS_BLACK=%d TENANT='%s' TABLEGROUP='%s'",
              is_black,
              pattern.tenant_pattern_.ptr(),
              pattern.tablegroup_pattern_.ptr());
        }
      }
    } // while
  }

  return ret;
}

void ObLogTableMatcher::Pattern::reset()
{
  tenant_pattern_.reset();
  database_pattern_.reset();
  table_pattern_.reset();
}

void ObLogTableMatcher::PgPattern::reset()
{
  tenant_pattern_.reset();
  tablegroup_pattern_.reset();
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef _ISTAT
#undef _DSTAT

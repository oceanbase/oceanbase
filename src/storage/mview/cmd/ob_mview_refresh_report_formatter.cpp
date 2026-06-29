/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "storage/mview/cmd/ob_mview_refresh_report_formatter.h"

#include "lib/time/ob_time_utility.h"
#include "lib/timezone/ob_time_convert.h"
#include "share/ob_errno.h"
#include "share/ob_time_utility2.h"
#include "share/scn.h"
#include "storage/mview/cmd/ob_mview_refresh_report.h"
#include "storage/mview/cmd/ob_mview_refresh_report_executor.h"
#include "storage/mview/ob_mview_refresh_plan_format.h"

namespace oceanbase
{
namespace storage
{
using namespace common;

// ====== Display constants — shared between TEXT and JSON formatters ======
static constexpr const char *STATUS_RUNNING = "RUNNING";
static constexpr const char *STATUS_PENDING = "PENDING";
static constexpr const char *ROLE_TGT = "TGT";
static constexpr const char *NA_STRING = "N/A";

// ====== Table layout — single-source column descriptors ======
// ColDescriptor is defined in ob_mview_refresh_report.h.
// Separators and headers are generated from the array,
// so they can never diverge from the format strings.

// MV Summary Table: #|Role|MV Name|Type|Start|End|Sched Delay|Elapsed|Changes|Throughput|Server|Result
static constexpr ColDescriptor MV_TBL_COLS[] = {
{"#", 2},
{"Role", 4},
{"MV Name", 20},
{"Type", 5},
{"Start", 26},
{"End", 26},
{"Sched Delay", 11},
{"Elapsed", 14},
{"Changes", 7},
{"Throughput", 10},
{"Server", 20},
{"Result", 20},
};
static constexpr int64_t MV_TBL_NCOL = ARRAYSIZEOF(MV_TBL_COLS);

// Step Execution Timeline: Step|SQL ID|Start Time|Elapsed|% of MV|Server|Result
static constexpr ColDescriptor STEP_TBL_COLS[] = {
{"Step", 4},
{"SQL ID", 32},
{"Start Time", 26},
{"Elapsed", 9},
{"% of MV", 7},
{"Result", 16},
};
static constexpr int64_t STEP_TBL_NCOL = ARRAYSIZEOF(STEP_TBL_COLS);

// Retry History: Retry#|Start|End|Elapsed|Result
static constexpr ColDescriptor RETRY_TBL_COLS[] = {
{"Retry #", 8},
{"Start", 26},
{"End", 26},
{"Elapsed", 10},
{"Result", 19},
};
static constexpr int64_t RETRY_TBL_NCOL = ARRAYSIZEOF(RETRY_TBL_COLS);

// Per-MV Resource Breakdown: MV Name|CPU|IO Wait|Disk IO|Memory
static constexpr ColDescriptor RES_TBL_COLS[] = {
{"MV Name", 19},
{"CPU", 8},
{"IO Wait", 8},
{"Disk IO", 8},
{"Memory", 9},
};
static constexpr int64_t RES_TBL_NCOL = ARRAYSIZEOF(RES_TBL_COLS);

// Base Table Changes: Base Table|INS|UPD|DEL|Base Rows
static constexpr ColDescriptor CHG_TBL_COLS[] = {
{"Base Table", 20},
{"INS", 8},
{"UPD", 8},
{"DEL", 8},
{"Base Rows", 9},
};
static constexpr int64_t CHG_TBL_NCOL = ARRAYSIZEOF(CHG_TBL_COLS);

// Buffer sizes for heap-allocated formatting buffers (per coding standard:
// stack variables must not exceed 256 bytes).
static constexpr int64_t TBL_SEP_BUF_LEN = 256;
static constexpr int64_t RESULT_DETAIL_BUF_LEN = 128;
static constexpr int64_t RESULT_WITH_MSG_BUF_LEN = OB_MAX_ERROR_MSG_LEN + 32;
static constexpr int64_t FMT_BUF_LEN = 64;
static constexpr int64_t SMALL_BUF_LEN = 32;
// Extra bytes for ":<port>" suffix in server column ("<ip>:<port>").
static constexpr int64_t SVR_PORT_FMT_EXTRA = 21;

static const MViewReportMVData &get_final_mv(const ObIArray<MViewReportMVData> &mv_array,
                                             const MViewReportMVGroup &group)
{
  return mv_array.at(group.attempt_range_.last_);
}

static double get_elapsed_pct(const MViewReportMVData &mv, const int64_t total_elapsed)
{
  double elapsed_pct = 0.0;
  if (total_elapsed > 0) {
    elapsed_pct = static_cast<double>(mv.elapsed_time_) / static_cast<double>(total_elapsed) * 100.0;
  }
  return elapsed_pct;
}

static bool try_get_throughput(const MViewReportMVData &mv, const int64_t changes, double &throughput)
{
  bool known = false;
  const double elapsed_s = static_cast<double>(mv.elapsed_time_) / 1000000.0;
  if (elapsed_s > 0.0 && changes > 0) {
    throughput = static_cast<double>(changes) / elapsed_s;
    known = true;
  } else {
    throughput = 0.0;
  }
  return known;
}

static bool is_slowest_group(const int64_t group_idx,
                             const int64_t group_count,
                             const MViewReportSummaryInfo &summary)
{
  return group_idx == summary.slowest_grp_ && group_count > 1;
}

// ====== TEXT report shared constants ======
static constexpr const char
*TEXT_SEPARATOR = "================================================================================\n";
static constexpr const char
*SECTION_LINE = "--------------------------------------------------------------------------------\n";

// ====== Low-level formatting utilities ======

static int format_run_result(char *buf,
                             const int64_t buf_len,
                             const MViewReportRunData &run_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret));
  } else if (run_data.is_success()) {
    ret = databuff_printf(buf, buf_len, "OK");
  } else if (run_data.is_running()) {
    ret = databuff_printf(buf, buf_len, "%s", STATUS_RUNNING);
  } else if (!run_data.error_message_.empty()) {
    ret = databuff_printf(buf,
                          buf_len,
                          "FAIL(%ld, %s) -- %.*s",
                          run_data.result_,
                          ob_error_name(static_cast<int>(run_data.result_)),
                          run_data.error_message_.length(),
                          run_data.error_message_.ptr());
  } else {
    ret = databuff_printf(buf,
                          buf_len,
                          "FAIL(%ld, %s)",
                          run_data.result_,
                          ob_error_name(static_cast<int>(run_data.result_)));
  }
  return ret;
}

static int format_parallelism(char *buf,
                              const int64_t buf_len,
                              const MViewReportRunData &run_data)
{
  int ret = OB_SUCCESS;
  if (run_data.is_default_parallelism()) {
    ret = databuff_printf(buf, buf_len, "default");
  } else {
    ret = databuff_printf(buf, buf_len, "%ld", run_data.parallelism_);
  }
  return ret;
}

static int format_mv_table_result(char *buf,
                                  const int64_t buf_len,
                                  const MViewReportMVData &mv,
                                  const int64_t num_retries,
                                  const bool is_slowest)
{
  int ret = OB_SUCCESS;
  const char *retry_tag = (num_retries > 0) ? "[R]" : "";
  const char *slow_tag = is_slowest ? "[S]" : "";
  if (mv.is_success()) {
    ret = databuff_printf(buf, buf_len, "OK%s%s", retry_tag, slow_tag);
  } else if (mv.is_running()) {
    ret = databuff_printf(buf, buf_len, "%s%s", STATUS_RUNNING, slow_tag);
  } else {
    ret = databuff_printf(buf, buf_len, "FAIL(%ld)%s%s", mv.result_, retry_tag, slow_tag);
  }
  return ret;
}

static int format_mv_detail_result(char *buf,
                                   const int64_t buf_len,
                                   const MViewReportMVData &mv)
{
  int ret = OB_SUCCESS;
  if (mv.is_success()) {
    ret = databuff_printf(buf, buf_len, "SUCCESS");
  } else if (mv.is_running()) {
    ret = databuff_printf(buf, buf_len, "%s", STATUS_RUNNING);
  } else {
    ret = databuff_printf(buf,
                          buf_len,
                          "FAILED (%ld, %s)",
                          mv.result_,
                          ob_error_name(static_cast<int>(mv.result_)));
  }
  return ret;
}

static int format_mv_attempt_result(char *buf,
                                    const int64_t buf_len,
                                    const MViewReportMVData &mv)
{
  int ret = OB_SUCCESS;
  if (mv.is_success()) {
    ret = databuff_printf(buf, buf_len, "OK");
  } else if (mv.is_running()) {
    ret = databuff_printf(buf, buf_len, "%s", STATUS_RUNNING);
  } else {
    ret = databuff_printf(buf, buf_len, "FAIL(%ld)", mv.result_);
  }
  return ret;
}

static int format_stmt_result(char *buf,
                              const int64_t buf_len,
                              const MViewReportStmtData &stmt)
{
  int ret = OB_SUCCESS;
  if (stmt.is_success()) {
    ret = databuff_printf(buf, buf_len, "OK");
  } else if (stmt.is_running()) {
    ret = databuff_printf(buf, buf_len, "%s", STATUS_PENDING);
  } else {
    ret = databuff_printf(buf, buf_len, "FAIL(%ld)", stmt.result_);
  }
  return ret;
}

static bool is_valid_json_object_or_array(const ObString &json_str)
{
  bool is_valid = false;
  const char *json_ptr = json_str.ptr();
  const int64_t json_len = json_str.length();
  int64_t first = 0;
  int64_t last = json_len - 1;
  if (OB_NOT_NULL(json_ptr) && json_len > 0) {
    for (; first < json_len && isspace(json_ptr[first]); ++first) {
      /* skip leading whitespace */
    }
    for (; last >= 0 && isspace(json_ptr[last]); --last) {
      /* skip trailing whitespace */
    }
    if (first <= last) {
      is_valid = (('{' == json_ptr[first]) && ('}' == json_ptr[last]))
                 || (('[' == json_ptr[first]) && (']' == json_ptr[last]));
    }
  }
  return is_valid;
}

static int append_text_execution_plan(ObIAllocator &allocator,
                                      const MViewReportStmtData &stmt,
                                      ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  ObSqlString plan_text;
  uint64_t plan_hash = 0;
  if (stmt.execution_plan_.empty()) {
  } else if (OB_FAIL(render_mview_plan_text(allocator, stmt.execution_plan_, plan_hash, plan_text))) {
    LOG_WARN("fail to render plan text", KR(ret));
  } else if (plan_hash != 0 && OB_FAIL(report_text.append_fmt("      Plan Hash Value: %lu\n", plan_hash))) {
    LOG_WARN("fail to append plan hash", KR(ret));
  } else if (plan_text.empty()) {
    // Hash-only mode: no operator tree to render.
  } else if (OB_FAIL(report_text.append("      Execution Plan:\n"))) {
    LOG_WARN("fail to append plan header", KR(ret));
  } else {
    // Indent each rendered plan line with 8 spaces so the whole block
    // aligns visually under the "Execution Plan:" header above.
    const char *buf = plan_text.ptr();
    const int64_t len = plan_text.length();
    int64_t line_start = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < len; ++i) {
      if (buf[i] == '\n') {
        if (OB_FAIL(report_text.append_fmt("        %.*s\n",
                                           static_cast<int>(i - line_start),
                                           buf + line_start))) {
          LOG_WARN("fail to append plan line", KR(ret));
        } else {
          line_start = i + 1;
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (line_start < len
               && OB_FAIL(report_text.append_fmt("        %.*s\n",
                                                 static_cast<int>(len - line_start),
                                                 buf + line_start))) {
      LOG_WARN("fail to append plan tail line", KR(ret));
    }
  }
  return ret;
}

static int append_json_execution_plan(ObSqlString &report_text,
                                      const MViewReportStmtData &stmt)
{
  int ret = OB_SUCCESS;
  if (stmt.execution_plan_.empty()) {
  } else if (is_valid_json_object_or_array(stmt.execution_plan_)) {
    if (OB_FAIL(report_text.append(", \"execution_plan\": "))) {
      LOG_WARN("fail to append plan key", KR(ret));
    } else if (OB_FAIL(report_text.append(stmt.execution_plan_))) {
      LOG_WARN("fail to append plan body", KR(ret));
    }
  } else {
    LOG_WARN("execution_plan_ is not valid JSON, fallback to null");
    if (OB_FAIL(report_text.append(", \"execution_plan\": null"))) {
      LOG_WARN("fail to append null plan", KR(ret));
    }
  }
  return ret;
}

static int build_tbl_sep(const ColDescriptor *cols, int64_t ncols, int64_t indent, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; i < indent && pos < buf_len - 1; ++i) {
      buf[pos++] = ' ';
    }
    if (pos < buf_len - 1) {
      buf[pos++] = '+';
    }
    for (int64_t c = 0; OB_SUCC(ret) && c < ncols; ++c) {
      int64_t dash_w = cols[c].width + 2;
      for (int64_t i = 0; i < dash_w && pos < buf_len - 1; ++i) {
        buf[pos++] = '-';
      }
      if (pos < buf_len - 1) {
        buf[pos++] = '+';
      }
    }
    if (pos < buf_len - 1) {
      buf[pos++] = '\n';
    }
    buf[pos < buf_len ? pos : buf_len - 1] = '\0';
  }
  return ret;
}

static int build_tbl_header(const ColDescriptor *cols, int64_t ncols, int64_t indent, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < indent && pos < buf_len - 1; ++i) {
      buf[pos++] = ' ';
    }
    if (OB_SUCC(ret) && pos < buf_len - 1) {
      buf[pos++] = '|';
    }
    for (int64_t c = 0; OB_SUCC(ret) && c < ncols; ++c) {
      int written = snprintf(buf + pos, buf_len - pos, " %-*s ", cols[c].width, cols[c].label);
      if (OB_UNLIKELY(written < 0 || written >= buf_len - pos)) {
        ret = OB_BUF_NOT_ENOUGH;
      } else {
        pos += written;
        if (pos < buf_len - 1) {
          buf[pos++] = '|';
        } else {
          ret = OB_BUF_NOT_ENOUGH;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (pos < buf_len - 1) {
        buf[pos++] = '\n';
      }
      buf[pos < buf_len ? pos : buf_len - 1] = '\0';
    }
  }
  return ret;
}

static int format_timestamp(int64_t usec, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (0 == usec) {
    pos = snprintf(buf, buf_len, "N/A");
    if (OB_UNLIKELY(pos < 0 || pos >= buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
    }
  } else if (OB_FAIL(share::ObTimeUtility2::usec_to_str(usec, buf, buf_len, pos))) {
    LOG_WARN("fail to format timestamp", KR(ret), K(usec));
  }
  return ret;
}

static int format_elapsed(int64_t us, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (us < 1000) {
    pos = snprintf(buf, buf_len, "%ld us", us);
  } else if (us < 1000000) {
    pos = snprintf(buf, buf_len, "%.1f ms", static_cast<double>(us) / 1000.0);
  } else if (us < 60000000) {
    pos = snprintf(buf, buf_len, "%.3f s", static_cast<double>(us) / 1000000.0);
  } else {
    int64_t min = us / 60000000;
    double sec = static_cast<double>(us % 60000000) / 1000000.0;
    pos = snprintf(buf, buf_len, "%ldm %.1fs", min, sec);
  }
  if (OB_UNLIKELY(pos < 0 || pos >= buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

static int format_number(int64_t val, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (val < 0) {
    int64_t pos = snprintf(buf, buf_len, "%ld", val);
    if (OB_UNLIKELY(pos < 0 || pos >= buf_len)) {
      ret = OB_BUF_NOT_ENOUGH;
    }
  } else {
    char tmp[64];
    int64_t tmp_pos = snprintf(tmp, sizeof(tmp), "%ld", val);
    if (OB_UNLIKELY(tmp_pos < 0 || tmp_pos >= static_cast<int64_t>(sizeof(tmp)))) {
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      int64_t out_pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < tmp_pos; ++i) {
        if (i > 0 && 0 == ((tmp_pos - i) % 3) && out_pos < buf_len - 1) {
          buf[out_pos++] = ',';
        }
        if (out_pos < buf_len - 1) {
          buf[out_pos++] = tmp[i];
        } else {
          ret = OB_BUF_NOT_ENOUGH;
        }
      }
      if (OB_SUCC(ret)) {
        buf[out_pos] = '\0';
      }
    }
  }
  return ret;
}

// Format an SCN raw value with a human-readable nanosecond timestamp appended.
// Output: "N/A" if unknown, or "1234567890 (YYYY-MM-DD HH:MM:SS.fffffffff)".
static int format_scn_with_ts(uint64_t scn_val, const ObTimeZoneInfo *sys_tz_info, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  char raw_buf[32];
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (scn_is_unknown(scn_val)) {
    if (OB_FAIL(databuff_printf(buf, buf_len, "N/A"))) {
    }
  } else if (OB_FAIL(databuff_printf(raw_buf, sizeof(raw_buf), "%lu", scn_val))) {
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "%s (", raw_buf))) {
  } else if (OB_FAIL(ObTimeConverter::scn_to_str(scn_val, sys_tz_info, buf, buf_len, pos))) {
    LOG_WARN("scn_to_str failed, fallback to raw scn", KR(ret));
    ret = OB_SUCCESS;
    if (OB_FAIL(databuff_printf(buf, buf_len, "%lu", scn_val))) {
      LOG_WARN("buf overflow in format_scn_with_ts", KR(ret));
    }
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, ")"))) {
  }
  return ret;
}

static int format_memory(int64_t bytes, char *buf, int64_t buf_len)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
  } else if (bytes < 1024 * 1024) {
    pos = snprintf(buf, buf_len, "%.1f KB", static_cast<double>(bytes) / 1024.0);
  } else if (bytes < 1024LL * 1024 * 1024) {
    pos = snprintf(buf, buf_len, "%.1f MB", static_cast<double>(bytes) / (1024.0 * 1024.0));
  } else {
    pos = snprintf(buf, buf_len, "%.1f GB", static_cast<double>(bytes) / (1024.0 * 1024.0 * 1024.0));
  }
  if (OB_UNLIKELY(pos < 0 || pos >= buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
  }
  return ret;
}

// str is NOT required to be null-terminated; len specifies the exact
// number of bytes to encode.  Pass len < 0 to treat str as a C string.
static int append_json_string(ObSqlString &out, const char *str, int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str)) {
    ret = out.append("null");
  } else if (OB_FAIL(out.append("\""))) {
    LOG_WARN("fail to append quote", KR(ret));
  } else {
    const char *const end = (len >= 0) ? (str + len) : NULL;
    const char *p = str;
    const char *seg_start = p;
    bool done = false;
    while (OB_SUCC(ret) && !done) {
      // stop when we hit the explicit end, or \0 for C-string mode
      if (end != NULL) {
        done = (p >= end);
      } else {
        done = (*p == '\0');
      }
      if (!done) {
        const char *esc = NULL;
        char unicode_buf[8];
        const unsigned char ch = static_cast<unsigned char>(*p);
        switch (*p) {
          case '\\':
            esc = "\\\\";
            break;
          case '"':
            esc = "\\\"";
            break;
          case '\n':
            esc = "\\n";
            break;
          case '\r':
            esc = "\\r";
            break;
          case '\t':
            esc = "\\t";
            break;
          default:
            if (ch < 0x20) {
              if (OB_FAIL(databuff_printf(unicode_buf, sizeof(unicode_buf), "\\u%04x", ch))) {
                LOG_WARN("fail to format json unicode escape", KR(ret), K(ch));
              } else {
                esc = unicode_buf;
              }
            } else if (ch >= 0x80) {
              // Decode valid UTF-8 multi-byte sequences and pass through as-is;
              // escape stray continuation bytes and invalid lead bytes.
              int extra = 0;
              if ((ch & 0xE0) == 0xC0) {
                extra = 1;
              } else if ((ch & 0xF0) == 0xE0) {
                extra = 2;
              } else if ((ch & 0xF8) == 0xF0) {
                extra = 3;
              }
              if (extra > 0) {
                bool valid = true;
                for (int j = 1; valid && j <= extra; ++j) {
                  if (end != NULL) {
                    if (p + j >= end) {
                      valid = false;
                    }
                  } else {
                    if (p[j] == '\0') {
                      valid = false;
                    }
                  }
                  if (valid) {
                    const unsigned char cb = static_cast<unsigned char>(p[j]);
                    if (cb < 0x80 || cb > 0xBF) {
                      valid = false;
                    }
                  }
                }
                if (valid) {
                  p += extra;
                } else {
                  if (OB_FAIL(databuff_printf(unicode_buf, sizeof(unicode_buf), "\\u%04x", ch))) {
                    LOG_WARN("fail to format json unicode escape", KR(ret), K(ch));
                  } else {
                    esc = unicode_buf;
                  }
                }
              } else {
                if (OB_FAIL(databuff_printf(unicode_buf, sizeof(unicode_buf), "\\u%04x", ch))) {
                  LOG_WARN("fail to format json unicode escape", KR(ret), K(ch));
                } else {
                  esc = unicode_buf;
                }
              }
            }
            break;
        }
        if (OB_SUCC(ret) && OB_NOT_NULL(esc)) {
          if (p > seg_start && OB_FAIL(out.append(seg_start, static_cast<int32_t>(p - seg_start)))) {
            LOG_WARN("fail to append segment", KR(ret));
          } else if (OB_FAIL(out.append(esc))) {
            LOG_WARN("fail to append escape", KR(ret));
          } else {
            seg_start = p + 1;
          }
        }
      }
      if (OB_SUCC(ret) && !done) {
        ++p;
      }
    }
    if (OB_SUCC(ret) && p > seg_start) {
      if (OB_FAIL(out.append(seg_start, static_cast<int32_t>(p - seg_start)))) {
        LOG_WARN("fail to append final segment", KR(ret));
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(out.append("\""))) {
      LOG_WARN("fail to append closing quote", KR(ret));
    }
  }
  return ret;
}

static int append_json_string(ObSqlString &out, const common::ObString &str)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(str.ptr())) {
    ret = out.append("null");
  } else {
    ret = append_json_string(out, str.ptr(), str.length());
  }
  return ret;
}

// ====== Shared helper functions ======

// Avoid passing NULL ObString::ptr() to %s format specifiers.
static const char *safe_ptr_str(const common::ObString &s)
{
  return s.empty() ? "" : s.ptr();
}

// ====== TEXT report section functions ======

static int build_text_header(const MViewReportRunData &run_data, ObSqlString &report_text, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  char *gen_buf = static_cast<char *>(allocator.alloc(FMT_BUF_LEN));
  if (OB_ISNULL(gen_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc gen_buf", KR(ret));
  } else if (OB_FAIL(format_timestamp(ObTimeUtility::current_time(), gen_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format generated time", KR(ret));
  } else if (OB_FAIL(report_text.append(TEXT_SEPARATOR))) {
    LOG_WARN("fail to append separator", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("                  MATERIALIZED VIEW REFRESH REPORT\n"
                                            "                  Refresh ID: %ld\n"
                                            "                  Generated:  %s\n",
                                            run_data.refresh_id_,
                                            gen_buf))) {
    LOG_WARN("fail to append header", KR(ret));
  } else if (OB_FAIL(report_text.append(TEXT_SEPARATOR))) {
    LOG_WARN("fail to append separator", KR(ret));
  }
  return ret;
}

static int build_text_summary(const MViewRefreshReport &report,
                              const ObTimeZoneInfo *sys_tz_info,
                              ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  const MViewReportRunData &run_data = *report.run_data_;
  const ObIArray<MViewReportMVData> &mv_array = *report.mv_array_;
  const MViewReportSummaryInfo &summary = report.summary_;
  const int64_t total_execute_time = summary.total_execute_time_us_;
  const int64_t total_retry_overhead = summary.total_retry_overhead_us_;
  const int64_t total_sched_overhead = summary.total_sched_overhead_us_;
  const int64_t num_failures = summary.num_failures_;
  const int64_t total_retries = summary.total_retries_;
  const int64_t num_distinct_mvs = summary.num_distinct_mvs_;
  const bool has_retry_overhead = total_retry_overhead > 0;
  const bool target_mv_missing = is_target_mv_missing(run_data, mv_array);
  const int64_t total_mv_count = num_distinct_mvs + (target_mv_missing ? 1 : 0);
  const ObString &target_mv_name = run_data.display_mview_name();
  const char *target_name = run_data.has_target_mview() ? safe_ptr_str(target_mv_name) : NA_STRING;
  int64_t target_name_len = run_data.has_target_mview() ? target_mv_name.length()
                                                         : static_cast<int64_t>(strlen(NA_STRING));

  char *ts_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *elapsed_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *scn_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *para_buf = NULL;
  char *retry_buf = has_retry_overhead ? static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN)) : NULL;
  if (OB_ISNULL(ts_buf) || OB_ISNULL(elapsed_buf) || OB_ISNULL(scn_buf)
      || (has_retry_overhead && OB_ISNULL(retry_buf))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc fmt bufs", KR(ret));
  } else if (OB_FAIL(report_text.append("\nREFRESH SUMMARY\n"))) {
    LOG_WARN("fail to append section title", KR(ret));
  } else if (OB_FAIL(report_text.append(SECTION_LINE))) {
    LOG_WARN("fail to append section line", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  Run Owner:            %.*s\n"
                                            "  Refresh ID:           %ld\n"
                                            "  Trace ID:             %.*s\n",
                                            run_data.run_owner_.length(),
                                            safe_ptr_str(run_data.run_owner_),
                                            run_data.refresh_id_,
                                            run_data.trace_id_.length(),
                                            safe_ptr_str(run_data.trace_id_)))) {
    LOG_WARN("fail to append summary basics", KR(ret));
  } else if (OB_FAIL(format_timestamp(run_data.start_time_, ts_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format start_time", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  Start Time:           %s\n", ts_buf))) {
    LOG_WARN("fail to append start_time", KR(ret));
  } else if (OB_FAIL(format_timestamp(run_data.end_time_, ts_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format end_time", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  End Time:             %s\n", ts_buf))) {
    LOG_WARN("fail to append end_time", KR(ret));
  } else if (OB_FAIL(format_elapsed(summary.elapsed_us_, elapsed_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format elapsed", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  Elapsed Time:         %s\n", elapsed_buf))) {
    LOG_WARN("fail to append elapsed", KR(ret));
  } else if (OB_FAIL(format_elapsed(total_execute_time, elapsed_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format total exec time", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  Total Execute Time:   %s  (sum of MV elapsed times)\n", elapsed_buf))) {
    LOG_WARN("fail to append total exec", KR(ret));
  } else if (has_retry_overhead && OB_FAIL(format_elapsed(total_retry_overhead, retry_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format retry overhead", KR(ret));
  } else if (has_retry_overhead
             && OB_FAIL(report_text.append_fmt("    Retry Overhead:      %s  (elapsed of failed retries)\n",
                                               retry_buf))) {
    LOG_WARN("fail to append retry overhead", KR(ret));
  } else if (OB_FAIL(format_elapsed(total_sched_overhead, elapsed_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format sched overhead", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  Total Sched Overhead: %s  (sum of MV scheduling delays)\n",
                                            elapsed_buf))) {
    LOG_WARN("fail to append sched overhead", KR(ret));
  } else if (run_data.is_failed() && 0 == num_failures) {
    if (OB_FAIL(report_text.append_fmt("  Status:               %s\n", summary.status_str_))) {
      LOG_WARN("fail to append status", KR(ret));
    }
  } else if (OB_FAIL(report_text.append_fmt("  Status:               %s (%ld failures, %ld retries)\n",
                                            summary.status_str_,
                                            num_failures,
                                            total_retries))) {
    LOG_WARN("fail to append status", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (run_data.is_failed()) {
    char *run_result_buf = static_cast<char *>(report.allocator_->alloc(RESULT_WITH_MSG_BUF_LEN));
    if (OB_ISNULL(run_result_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc run_result_buf", KR(ret));
    } else if (OB_FAIL(format_run_result(run_result_buf,
                                         RESULT_WITH_MSG_BUF_LEN,
                                         run_data))) {
      LOG_WARN("fail to format run result", KR(ret), K(run_data.result_));
    } else if (OB_FAIL(report_text.append_fmt("  Result:               %s\n", run_result_buf))) {
      LOG_WARN("fail to append run result", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(report_text.append_fmt("  Refresh Method:       %s\n",
                                            summary.method_display_ != NULL ? summary.method_display_ : "AUTO"))) {
    LOG_WARN("fail to append method", KR(ret));
  } else if (OB_FAIL(format_scn_with_ts(run_data.data_target_scn_, sys_tz_info, scn_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format target data scn", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("  Target Data SCN:      %s\n", scn_buf))) {
    LOG_WARN("fail to append target data scn", KR(ret));
  } else {
    para_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
    if (OB_ISNULL(para_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc para_buf", KR(ret));
    } else if (OB_FAIL(format_parallelism(para_buf, SMALL_BUF_LEN, run_data))) {
      LOG_WARN("para_buf overflow", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("  Number of MVs:        %ld\n"
                                              "  Parallelism:          %s\n"
                                              "  Nested Refresh:       %s\n"
                                              "  Target MV:            %.*s\n"
                                              "  Pending Queue Length: N/A\n",
                                              total_mv_count,
                                              para_buf,
                                              run_data.nested_ ? "YES" : "NO",
                                              static_cast<int>(target_name_len),
                                              target_name))) {
      LOG_WARN("fail to append summary fields", KR(ret));
    }
  }

  return ret;
}

static int build_text_mv_table(const MViewRefreshReport &report, ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  char *ts_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *num_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *elapsed_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *mv_tbl_sep = static_cast<char *>(report.allocator_->alloc(TBL_SEP_BUF_LEN));
  if (OB_ISNULL(ts_buf) || OB_ISNULL(num_buf) || OB_ISNULL(elapsed_buf) || OB_ISNULL(mv_tbl_sep)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc fmt bufs", KR(ret));
  }

  const MViewReportRunData &run_data = *report.run_data_;
  const ObIArray<MViewReportMVData> &mv_array = *report.mv_array_;
  const MViewReportSummaryInfo &summary = report.summary_;
  const ObIArray<MViewReportMVGroup> &mv_groups = report.mv_groups_;

  const int64_t num_distinct_mvs = summary.num_distinct_mvs_;
  const bool target_mv_missing = is_target_mv_missing(run_data, mv_array);
  const bool has_mv_rows = num_distinct_mvs > 0 || target_mv_missing;

  // Local copy of column descriptors for dynamic string-column widths.
  ColDescriptor mv_local_cols[MV_TBL_NCOL];
  for (int64_t i = 0; i < MV_TBL_NCOL; ++i) {
    mv_local_cols[i] = MV_TBL_COLS[i];
  }

  if (OB_FAIL(ret)) {
  } else if (has_mv_rows) {
    int64_t mv_name_w = MV_TBL_COLS[2].width;
    int64_t server_w = MV_TBL_COLS[11].width;
    for (int64_t gi = 0; gi < num_distinct_mvs; ++gi) {
      const MViewReportMVGroup &group_scan = mv_groups.at(gi);
      const MViewReportMVData &mv_scan = get_final_mv(mv_array, group_scan);
      const int64_t full_mv_name_len = mv_scan.display_name().length();
      if (full_mv_name_len > mv_name_w) {
        mv_name_w = full_mv_name_len;
      }
      if (mv_scan.has_server()) {
        int64_t srv_len = mv_scan.svr_ip_.length() + SVR_PORT_FMT_EXTRA;
        if (srv_len > server_w) {
          server_w = srv_len;
        }
      }
    }
    if (target_mv_missing && run_data.display_mview_name().length() > mv_name_w) {
      mv_name_w = run_data.display_mview_name().length();
    }
    mv_local_cols[2].width = mv_name_w;
    mv_local_cols[11].width = server_w;
    if (OB_FAIL(report_text.append("\n  MView Refresh Summary (topo order):\n"))) {
      LOG_WARN("fail to append mv summary title", KR(ret));
    } else if (OB_FAIL(build_tbl_sep(mv_local_cols, MV_TBL_NCOL, 2, mv_tbl_sep, TBL_SEP_BUF_LEN))) {
      LOG_WARN("fail to build mv tbl sep", KR(ret));
    } else if (OB_FAIL(report_text.append(mv_tbl_sep))) {
      LOG_WARN("fail to append sep", KR(ret));
    } else if (OB_FAIL(build_tbl_header(mv_local_cols, MV_TBL_NCOL, 2, mv_tbl_sep, TBL_SEP_BUF_LEN))) {
      LOG_WARN("fail to build mv tbl hdr", KR(ret));
    } else if (OB_FAIL(report_text.append(mv_tbl_sep))) {
      LOG_WARN("fail to append header", KR(ret));
    } else if (OB_FAIL(build_tbl_sep(mv_local_cols, MV_TBL_NCOL, 2, mv_tbl_sep, TBL_SEP_BUF_LEN))) {
      LOG_WARN("fail to build mv tbl sep", KR(ret));
    } else if (OB_FAIL(report_text.append(mv_tbl_sep))) {
      LOG_WARN("fail to append sep", KR(ret));
    }
  }

  char *start_hms = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *end_hms = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *sched_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *thpt_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *elapsed_pct_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *server_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *result_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  if (OB_ISNULL(start_hms) || OB_ISNULL(end_hms) || OB_ISNULL(sched_buf) || OB_ISNULL(thpt_buf)
      || OB_ISNULL(elapsed_pct_buf) || OB_ISNULL(server_buf) || OB_ISNULL(result_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc loop bufs", KR(ret));
  }

  for (int64_t gi = 0; OB_SUCC(ret) && gi < num_distinct_mvs; ++gi) {
    const MViewReportMVGroup &group = mv_groups.at(gi);
    const MViewReportMVData &mv = get_final_mv(mv_array, group);
    const int64_t display_num = gi + 1;
    const char *type_str = mv.type_short_name();
    const char *role_str = run_data.mv_role_label(mv.mview_id_);
    const int64_t mv_changes = group.changes_;
    const bool is_slowest = is_slowest_group(gi, num_distinct_mvs, summary);
    const int64_t num_table_retries = group.num_retries();
    const int64_t sched_delay = mv.sched_delay_us();
    const double elapsed_pct = get_elapsed_pct(mv, summary.elapsed_us_);
    double throughput = 0.0;
    const bool throughput_known = try_get_throughput(mv, group.changes_, throughput);
    start_hms[0] = '\0';
    end_hms[0] = '\0';
    if (OB_FAIL(format_timestamp(mv.start_time_, start_hms, SMALL_BUF_LEN))) {
      LOG_WARN("fail to format mv start", KR(ret));
    } else if (OB_FAIL(format_timestamp(mv.end_time_, end_hms, SMALL_BUF_LEN))) {
      LOG_WARN("fail to format mv end", KR(ret));
    } else if (sched_delay >= 0 && OB_FAIL(format_elapsed(sched_delay, sched_buf, SMALL_BUF_LEN))) {
      LOG_WARN("fail to format sched delay", KR(ret));
    } else if (sched_delay < 0 && OB_FAIL(databuff_printf(sched_buf, SMALL_BUF_LEN, "0 us"))) {
      LOG_WARN("sched_buf overflow", KR(ret));
    } else if (OB_FAIL(format_elapsed(mv.elapsed_time_, elapsed_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format mv elapsed", KR(ret));
    } else if (!throughput_known && OB_FAIL(databuff_printf(thpt_buf, SMALL_BUF_LEN, "N/A"))) {
      LOG_WARN("thpt_buf overflow", KR(ret));
    } else if (throughput_known
               && OB_FAIL(databuff_printf(thpt_buf,
                                          SMALL_BUF_LEN,
                                          "%ld/s",
                                          static_cast<int64_t>(throughput)))) {
      LOG_WARN("thpt_buf overflow", KR(ret));
    } else if (OB_FAIL(databuff_printf(elapsed_pct_buf,
                                       SMALL_BUF_LEN,
                                       "%s (%.*f%%)",
                                       elapsed_buf,
                                       (elapsed_pct >= 10.0) ? 0 : 1,
                                       elapsed_pct))) {
      LOG_WARN("elapsed_pct_buf overflow", KR(ret));
    } else if (OB_FAIL(format_mv_table_result(result_buf,
                                              FMT_BUF_LEN,
                                              mv,
                                              num_table_retries,
                                              is_slowest))) {
      LOG_WARN("result_buf overflow", KR(ret));
    } else if (OB_FAIL(format_number(mv_changes, num_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format changes", KR(ret));
    } else if (mv.has_server()
               && OB_FAIL(databuff_printf(server_buf,
                                          FMT_BUF_LEN,
                                          "%.*s:%ld",
                                          mv.svr_ip_.length(),
                                          safe_ptr_str(mv.svr_ip_),
                                          mv.svr_port_))) {
      LOG_WARN("server_buf overflow", KR(ret));
    } else if (!mv.has_server() && OB_FAIL(databuff_printf(server_buf, FMT_BUF_LEN, "-"))) {
      LOG_WARN("server_buf overflow", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("  | %*ld | %-*s | %-*.*s | %-*s | %-*.*s | %-*.*s | %*s | %*s | %*s "
                                              "| %*s | %-*s | %-*s |\n",
                                              mv_local_cols[0].width,
                                              display_num,
                                              mv_local_cols[1].width,
                                              role_str,
                                              mv_local_cols[2].width,
                                              mv.display_name().length(),
                                              safe_ptr_str(mv.display_name()),
                                              mv_local_cols[3].width,
                                              type_str,
                                              mv_local_cols[4].width,
                                              mv_local_cols[4].width,
                                              start_hms,
                                              mv_local_cols[5].width,
                                              mv_local_cols[5].width,
                                              end_hms,
                                              mv_local_cols[6].width,
                                              sched_buf,
                                              mv_local_cols[7].width,
                                              elapsed_pct_buf,
                                              mv_local_cols[8].width,
                                              num_buf,
                                              mv_local_cols[9].width,
                                              thpt_buf,
                                              mv_local_cols[10].width,
                                              server_buf,
                                              mv_local_cols[11].width,
                                              result_buf))) {
      LOG_WARN("fail to append mv row", KR(ret));
    }
  }

  if (OB_SUCC(ret) && target_mv_missing) {
    const int64_t placeholder_num = num_distinct_mvs + 1;
    if (OB_FAIL(report_text.append_fmt("  | %*ld | %-*s | %-*.*s | %-*s | %-*.*s | %-*.*s | %*s | %*s | %*s | %*s "
                                       "| %-*s | %-*s |\n",
                                       mv_local_cols[0].width,
                                       placeholder_num,
                                       mv_local_cols[1].width,
                                       ROLE_TGT,
                                       mv_local_cols[2].width,
                                       static_cast<int>(run_data.display_mview_name().length()),
                                       safe_ptr_str(run_data.display_mview_name()),
                                       mv_local_cols[3].width,
                                       NA_STRING,
                                       mv_local_cols[4].width,
                                       mv_local_cols[4].width,
                                       NA_STRING,
                                       mv_local_cols[5].width,
                                       mv_local_cols[5].width,
                                       NA_STRING,
                                       mv_local_cols[6].width,
                                       NA_STRING,
                                       mv_local_cols[7].width,
                                       NA_STRING,
                                       mv_local_cols[8].width,
                                       NA_STRING,
                                       mv_local_cols[9].width,
                                       "N/A",
                                       mv_local_cols[10].width,
                                       "N/A",
                                       mv_local_cols[11].width,
                                       "N/A"))) {
      LOG_WARN("fail to append target placeholder row", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (has_mv_rows) {
    if (OB_FAIL(build_tbl_sep(mv_local_cols, MV_TBL_NCOL, 2, mv_tbl_sep, TBL_SEP_BUF_LEN))) {
      LOG_WARN("fail to build mv tbl sep", KR(ret));
    } else if (OB_FAIL(report_text.append(mv_tbl_sep))) {
      LOG_WARN("fail to append table footer", KR(ret));
    } else if (OB_FAIL(report_text.append("  [R] = Retried    [S] = Slowest sub-task\n"))) {
      LOG_WARN("fail to append legend", KR(ret));
    }
  }

  return ret;
}

static int build_text_resource_overview(const MViewRefreshReport &report, ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  char *num_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *mem_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *res_tbl_sep = NULL;
  char *cpu_buf = NULL;
  char *io_buf = NULL;
  char *mv_cpu_buf = NULL;
  char *mv_io_buf = NULL;
  char *mv_mem_buf = NULL;
  if (OB_ISNULL(num_buf) || OB_ISNULL(mem_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc fmt bufs", KR(ret));
  }

  const ObIArray<MViewReportMVData> &mv_array = *report.mv_array_;
  const MViewReportSummaryInfo &summary = report.summary_;
  const MViewReportResourceOverview &resources = report.resources_;
  const ObIArray<MViewReportMVGroup> &mv_groups = report.mv_groups_;

  const int64_t total_steps = resources.total_steps_;
  const int64_t total_cpu = resources.total_cpu_us_;
  const int64_t total_io_wait = resources.total_io_wait_us_;
  const int64_t total_disk_reads = resources.total_disk_reads_;
  const int64_t max_memory = resources.max_memory_bytes_;
  const int64_t num_distinct_mvs = summary.num_distinct_mvs_;

  if (OB_FAIL(ret)) {
  } else if (total_steps > 0) {
    cpu_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
    io_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
    if (OB_ISNULL(cpu_buf) || OB_ISNULL(io_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc cpu/io bufs", KR(ret));
    } else if (OB_FAIL(format_elapsed(total_cpu, cpu_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format cpu", KR(ret));
    } else if (OB_FAIL(format_elapsed(total_io_wait, io_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format io", KR(ret));
    } else if (OB_FAIL(format_memory(total_disk_reads, num_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format disk io", KR(ret));
    } else if (OB_FAIL(format_memory(max_memory, mem_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format memory", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("\n  Resource Overview:\n"
                                              "    Total SQL Steps: %ld    CPU: %s    IO Wait: %s\n"
                                              "    Disk IO: %s     Max Memory: %s\n",
                                              total_steps,
                                              cpu_buf,
                                              io_buf,
                                              num_buf,
                                              mem_buf))) {
      LOG_WARN("fail to append resource overview", KR(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (num_distinct_mvs > 0) {
      int64_t res_name_w = RES_TBL_COLS[0].width;
      for (int64_t gi = 0; gi < num_distinct_mvs; ++gi) {
        const MViewReportMVGroup &group_scan = mv_groups.at(gi);
        const MViewReportMVData &mv_scan = get_final_mv(mv_array, group_scan);
        const int64_t full_mv_name_len = mv_scan.display_name().length();
        if (full_mv_name_len > res_name_w) {
          res_name_w = full_mv_name_len;
        }
      }
      ColDescriptor res_local_cols[RES_TBL_NCOL];
      for (int64_t i = 0; i < RES_TBL_NCOL; ++i) {
        res_local_cols[i] = RES_TBL_COLS[i];
      }
      res_local_cols[0].width = res_name_w;
      res_tbl_sep = static_cast<char *>(report.allocator_->alloc(TBL_SEP_BUF_LEN));
      if (OB_ISNULL(res_tbl_sep)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc res_tbl_sep", KR(ret));
      } else if (OB_FAIL(build_tbl_sep(res_local_cols, RES_TBL_NCOL, 2, res_tbl_sep, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build res tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append("\n  Per-MV Resource Breakdown:\n"))) {
        LOG_WARN("fail to append per-mv header", KR(ret));
      } else if (OB_FAIL(report_text.append(res_tbl_sep))) {
        LOG_WARN("fail to append res sep", KR(ret));
      } else if (OB_FAIL(build_tbl_header(res_local_cols, RES_TBL_NCOL, 2, res_tbl_sep, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build res hdr", KR(ret));
      } else if (OB_FAIL(report_text.append(res_tbl_sep))) {
        LOG_WARN("fail to append res hdr", KR(ret));
      } else if (OB_FAIL(build_tbl_sep(res_local_cols, RES_TBL_NCOL, 2, res_tbl_sep, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build res tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append(res_tbl_sep))) {
        LOG_WARN("fail to append res sep", KR(ret));
      }

      mv_cpu_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
      mv_io_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
      mv_mem_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
      if (OB_ISNULL(mv_cpu_buf) || OB_ISNULL(mv_io_buf) || OB_ISNULL(mv_mem_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc mv resource bufs", KR(ret));
      }

      for (int64_t gi = 0; OB_SUCC(ret) && gi < num_distinct_mvs; ++gi) {
        const MViewReportMVGroup &group = mv_groups.at(gi);
        const MViewReportMVData &mv = get_final_mv(mv_array, group);
        const int64_t mv_cpu = group.cpu_time_us_;
        const int64_t mv_io = group.io_wait_time_us_;
        const int64_t mv_dr = group.disk_reads_;
        const int64_t mv_mem = group.max_memory_bytes_;
        if (OB_FAIL(format_elapsed(mv_cpu, mv_cpu_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format", KR(ret));
        } else if (OB_FAIL(format_elapsed(mv_io, mv_io_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format", KR(ret));
        } else if (OB_FAIL(format_memory(mv_dr, num_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format disk io", KR(ret));
        } else if (OB_FAIL(format_memory(mv_mem, mv_mem_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format memory", KR(ret));
        } else if (OB_FAIL(report_text.append_fmt("  | %-*.*s | %*s | %*s | %*s | %*s |\n",
                                                  res_local_cols[0].width,
                                                  mv.display_name().length(),
                                                  safe_ptr_str(mv.display_name()),
                                                  res_local_cols[1].width,
                                                  mv_cpu_buf,
                                                  res_local_cols[2].width,
                                                  mv_io_buf,
                                                  res_local_cols[3].width,
                                                  num_buf,
                                                  res_local_cols[4].width,
                                                  mv_mem_buf))) {
          LOG_WARN("fail to append per-mv row", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append(res_tbl_sep))) {
        LOG_WARN("fail to append per-mv footer", KR(ret));
      }
    }
  } else if (report.run_data_->is_failed() || report.run_data_->is_running()) {
    if (OB_FAIL(report_text.append("\n  Resource Overview: N/A (job did not execute any refresh steps)\n"))) {
      LOG_WARN("fail to append resource na", KR(ret));
    }
  } else if (OB_FAIL(report_text.append("\n  Resource Overview: N/A (no step-level data)\n"
                                        "    Hint: step-level metrics (CPU, IO, Plan) require ADVANCED collection "
                                        "level.\n"
                                        "    Run: CALL DBMS_MVIEW_STATS.SET_MVREF_STATS_PARAMS('<mv_name>',"
                                        " COLLECTION_LEVEL => 'ADVANCED');\n"))) {
    LOG_WARN("fail to append resource na", KR(ret));
  }

  return ret;
}

static int build_text_per_mv_detail(const MViewRefreshReport &report,
                                    const ObTimeZoneInfo *sys_tz_info,
                                    ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  char *ts_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *num_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *elapsed_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *rows_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *result_detail_buf = NULL;
  char *tbl_sep_buf = NULL;
  char para_buf[SMALL_BUF_LEN];
  if (OB_ISNULL(ts_buf) || OB_ISNULL(num_buf) || OB_ISNULL(elapsed_buf) || OB_ISNULL(rows_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc fmt bufs", KR(ret));
  }

  const MViewReportRunData &run_data = *report.run_data_;
  const ObIArray<MViewReportMVData> &mv_array = *report.mv_array_;
  const ObIArray<MViewReportChangeData> &change_array = *report.change_array_;
  const ObIArray<MViewReportStmtData> &stmt_array = *report.stmt_array_;
  const MViewReportSummaryInfo &summary = report.summary_;
  const ObIArray<MViewReportMVGroup> &mv_groups = report.mv_groups_;

  const int64_t num_distinct_mvs = summary.num_distinct_mvs_;

  // COMPLETE refresh has no per-MV detail data worth showing.
  const bool skip_per_mv = should_skip_per_mv_detail(num_distinct_mvs, mv_array);
  if (OB_FAIL(ret)) {
  } else if (!skip_per_mv && OB_FAIL(report_text.append_fmt("\nPER-MVIEW DETAIL\n%s", SECTION_LINE))) {
    LOG_WARN("fail to append section title", KR(ret));
  }

  char *retry_elapsed = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *retry_result = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *retry_start = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *retry_end = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *start_hms = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *end_hms = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *sched_detail_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *changes_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *thpt_detail_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *s_str = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *step_result = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  char *cpu_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *io_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *mem_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *pct_buf = static_cast<char *>(report.allocator_->alloc(SMALL_BUF_LEN));
  tbl_sep_buf = static_cast<char *>(report.allocator_->alloc(TBL_SEP_BUF_LEN));
  result_detail_buf = static_cast<char *>(report.allocator_->alloc(RESULT_DETAIL_BUF_LEN));
  if (OB_ISNULL(retry_elapsed) || OB_ISNULL(retry_result) || OB_ISNULL(retry_start) || OB_ISNULL(retry_end)
      || OB_ISNULL(start_hms) || OB_ISNULL(end_hms) || OB_ISNULL(sched_detail_buf) || OB_ISNULL(changes_buf)
      || OB_ISNULL(thpt_detail_buf) || OB_ISNULL(s_str) || OB_ISNULL(step_result) || OB_ISNULL(cpu_buf)
      || OB_ISNULL(io_buf) || OB_ISNULL(mem_buf) || OB_ISNULL(pct_buf) || OB_ISNULL(tbl_sep_buf)
      || OB_ISNULL(result_detail_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc loop bufs", KR(ret));
  }

  for (int64_t gi = 0; OB_SUCC(ret) && gi < num_distinct_mvs; ++gi) {
    const MViewReportMVGroup &group = mv_groups.at(gi);
    const int64_t last_idx = group.attempt_range_.last_;
    const int64_t first_idx = group.attempt_range_.first_;
    const MViewReportMVData &mv = get_final_mv(mv_array, group);
    const int64_t display_num = gi + 1;
    const char *type_str = mv.type_name();
    const int64_t mv_changes = group.changes_;
    const int64_t num_retries = group.num_retries();
    const bool has_changes = group.has_changes();
    const bool has_stmts = group.has_stmts();
    const int64_t slowest_step_idx = group.slowest_stmt_idx_;
    const double mv_pct = get_elapsed_pct(mv, summary.elapsed_us_);
    double throughput = 0.0;
    const bool throughput_known = try_get_throughput(mv, group.changes_, throughput);
    const char *result_str = NULL;
    if (OB_FAIL(format_mv_detail_result(result_detail_buf, RESULT_DETAIL_BUF_LEN, mv))) {
      LOG_WARN("result_detail_buf overflow", KR(ret), K(mv.result_));
    } else {
      result_str = result_detail_buf;
    }
    const char *role_str = run_data.nested_role_suffix(mv.mview_id_);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_elapsed(mv.elapsed_time_, elapsed_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format mv elapsed", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("\n  #%ld  %.*s%s                            [%.1f%% of total]\n"
                                              "  "
                                              "........................................................................"
                                              "\n",
                                              display_num,
                                              mv.display_name().length(),
                                              safe_ptr_str(mv.display_name()),
                                              role_str,
                                              mv_pct))) {
      LOG_WARN("fail to append mv detail header", KR(ret));
    }

    // Retry timeline: show all attempts when this MV was retried.
    if (OB_FAIL(ret)) {
    } else if (group.num_retries() > 0) {
      if (OB_FAIL(build_tbl_sep(RETRY_TBL_COLS, RETRY_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build retry tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append("\n    Retry History:\n"))) {
        LOG_WARN("fail to append retry title", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append retry sep", KR(ret));
      } else if (OB_FAIL(build_tbl_header(RETRY_TBL_COLS, RETRY_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build retry hdr", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append retry hdr", KR(ret));
      } else if (OB_FAIL(build_tbl_sep(RETRY_TBL_COLS, RETRY_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build retry tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append retry sep", KR(ret));
      }

      for (int64_t ri = first_idx; OB_SUCC(ret) && ri <= last_idx; ++ri) {
        const MViewReportMVData &rv = mv_array.at(ri);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(format_mv_attempt_result(retry_result, SMALL_BUF_LEN, rv))) {
          LOG_WARN("retry_result buffer overflow", KR(ret));
        } else if (OB_FAIL(format_timestamp(rv.start_time_, retry_start, FMT_BUF_LEN))) {
          LOG_WARN("fail to format retry start", KR(ret));
        } else if (OB_FAIL(format_elapsed(rv.elapsed_time_, retry_elapsed, SMALL_BUF_LEN))) {
          LOG_WARN("fail to format retry elapsed", KR(ret));
        } else if (OB_FAIL(format_timestamp(rv.end_time_, retry_end, FMT_BUF_LEN))) {
          LOG_WARN("fail to format retry end", KR(ret));
        }
        const char *final_mark = (ri == last_idx) ? "  <- FINAL" : "";
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(report_text.append_fmt("    | %*ld | %-*.*s | %-*.*s | %*s | %-*s |%s\n",
                                                  RETRY_TBL_COLS[0].width,
                                                  rv.retry_id_,
                                                  RETRY_TBL_COLS[1].width,
                                                  RETRY_TBL_COLS[1].width,
                                                  retry_start,
                                                  RETRY_TBL_COLS[2].width,
                                                  RETRY_TBL_COLS[2].width,
                                                  retry_end,
                                                  RETRY_TBL_COLS[3].width,
                                                  retry_elapsed,
                                                  RETRY_TBL_COLS[4].width,
                                                  retry_result,
                                                  final_mark))) {
          LOG_WARN("fail to append retry row", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append retry footer", KR(ret));
      }
    }

    // Refresh type, result, retries, elapsed (shown for every MV).
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append_fmt("    Refresh Type: %-10s Result: %-10s Retries: %ld    Elapsed: %s\n",
                                              type_str,
                                              result_str,
                                              num_retries,
                                              elapsed_buf))) {
      LOG_WARN("fail to append mv detail basics", KR(ret));
    }

    // Start / End / Sched Delay / Changes / Throughput.
    start_hms[0] = '\0';
    end_hms[0] = '\0';
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_timestamp(mv.start_time_, start_hms, SMALL_BUF_LEN))) {
      LOG_WARN("fail to format mv start", KR(ret));
    } else if (OB_FAIL(format_timestamp(mv.end_time_, end_hms, SMALL_BUF_LEN))) {
      LOG_WARN("fail to format mv end", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      const int64_t sched_delay = mv.sched_delay_us();
      const int64_t sched_delay_display = sched_delay >= 0 ? sched_delay : 0;
      if (OB_FAIL(format_elapsed(sched_delay_display, sched_detail_buf, SMALL_BUF_LEN))) {
        LOG_WARN("fail to format sched delay", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_number(mv_changes, changes_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format changes", KR(ret));
    } else if (!throughput_known && OB_FAIL(databuff_printf(thpt_detail_buf, SMALL_BUF_LEN, "N/A"))) {
      LOG_WARN("thpt_detail_buf overflow", KR(ret));
    } else if (throughput_known
               && OB_FAIL(databuff_printf(thpt_detail_buf,
                                          SMALL_BUF_LEN,
                                          "%ld/s",
                                          static_cast<int64_t>(throughput)))) {
      LOG_WARN("thpt_detail_buf overflow", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("    Start: %s  End: %s  Sched Delay: %s\n"
                                              "    Changes: %s    Throughput: %s\n",
                                              start_hms,
                                              end_hms,
                                              sched_detail_buf,
                                              changes_buf,
                                              thpt_detail_buf))) {
      LOG_WARN("fail to append mv timing overview", KR(ret));
    }

    // Initial / Final rows.
    if (OB_FAIL(ret)) {
    } else if (mv.is_success()) {
      if (OB_FAIL(format_number(mv.initial_num_rows_, num_buf, FMT_BUF_LEN))) {
        LOG_WARN("fail to format initial rows", KR(ret));
      } else if (OB_FAIL(format_number(mv.final_num_rows_, rows_buf, FMT_BUF_LEN))) {
        LOG_WARN("fail to format final rows", KR(ret));
      } else if (OB_FAIL(report_text.append_fmt("    Initial/Final Rows:    %s / %s\n", num_buf, rows_buf))) {
        LOG_WARN("fail to append rows", KR(ret));
      }
    } else if (OB_FAIL(report_text.append("    Initial/Final Rows:    N/A / N/A\n"))) {
      LOG_WARN("fail to append rows na", KR(ret));
    }

    // Parallelism.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_parallelism(para_buf, SMALL_BUF_LEN, run_data))) {
      LOG_WARN("para_buf overflow", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("    Parallelism:            %s\n", para_buf))) {
      LOG_WARN("fail to append parallelism", KR(ret));
    }

    // SCN fields: current data, last refresh, current refresh.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_scn_with_ts(mv.base_table_start_scn_, sys_tz_info, s_str, FMT_BUF_LEN))) {
      LOG_WARN("fail to format current data scn", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("    Current Data SCN:       %s\n", s_str))) {
      LOG_WARN("fail to append current data scn", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_scn_with_ts(mv.mv_refresh_start_scn_, sys_tz_info, s_str, FMT_BUF_LEN))) {
      LOG_WARN("fail to format last refresh scn", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("    Last Refresh SCN:       %s\n", s_str))) {
      LOG_WARN("fail to append last refresh scn", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_scn_with_ts(mv.mv_refresh_end_scn_, sys_tz_info, s_str, FMT_BUF_LEN))) {
      LOG_WARN("fail to format current refresh scn", KR(ret));
    } else if (OB_FAIL(report_text.append_fmt("    Current Refresh SCN:    %s\n", s_str))) {
      LOG_WARN("fail to append current refresh scn", KR(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (has_changes) {
      int64_t chg_name_w = CHG_TBL_COLS[0].width;
      for (int64_t ci = group.final_change_range_.first_;
           ci <= group.final_change_range_.last_ && ci < change_array.count();
           ++ci) {
        const MViewReportChangeData &ch_scan = change_array.at(ci);
        int64_t full_len = ch_scan.display_name().length();
        if (full_len > chg_name_w) {
          chg_name_w = full_len;
        }
      }
      ColDescriptor chg_local_cols[CHG_TBL_NCOL];
      for (int64_t i = 0; i < CHG_TBL_NCOL; ++i) {
        chg_local_cols[i] = CHG_TBL_COLS[i];
      }
      chg_local_cols[0].width = chg_name_w;
      if (OB_FAIL(build_tbl_sep(chg_local_cols, CHG_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build chg tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append("\n    Base Table Changes:\n"))) {
        LOG_WARN("fail to append change title", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append chg sep", KR(ret));
      } else if (OB_FAIL(build_tbl_header(chg_local_cols, CHG_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build chg hdr", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append chg hdr", KR(ret));
      } else if (OB_FAIL(build_tbl_sep(chg_local_cols, CHG_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build chg tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append chg sep", KR(ret));
      }

      for (int64_t ci = group.final_change_range_.first_;
           OB_SUCC(ret) && ci <= group.final_change_range_.last_ && ci < change_array.count();
           ++ci) {
        const MViewReportChangeData &ch = change_array.at(ci);
        if (OB_FAIL(report_text.append_fmt("    | %-*.*s | %*ld | %*ld | %*ld | %*ld |\n",
                                           chg_local_cols[0].width,
                                           static_cast<int>(ch.display_name().length()),
                                           safe_ptr_str(ch.display_name()),
                                           chg_local_cols[1].width,
                                           ch.num_rows_ins_,
                                           chg_local_cols[2].width,
                                           ch.num_rows_upd_,
                                           chg_local_cols[3].width,
                                           ch.num_rows_del_,
                                           chg_local_cols[4].width,
                                           ch.num_rows_))) {
          LOG_WARN("fail to append change row", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append chg footer", KR(ret));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (has_stmts) {
      if (OB_FAIL(build_tbl_sep(STEP_TBL_COLS, STEP_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build step tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append("\n    Step Execution Timeline:\n"))) {
        LOG_WARN("fail to append step title", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append sep", KR(ret));
      } else if (OB_FAIL(build_tbl_header(STEP_TBL_COLS, STEP_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build step hdr", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append step hdr", KR(ret));
      } else if (OB_FAIL(build_tbl_sep(STEP_TBL_COLS, STEP_TBL_NCOL, 4, tbl_sep_buf, TBL_SEP_BUF_LEN))) {
        LOG_WARN("fail to build step tbl sep", KR(ret));
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append sep", KR(ret));
      }

      for (int64_t si = group.final_stmt_range_.first_;
           OB_SUCC(ret) && si <= group.final_stmt_range_.last_ && si < stmt_array.count();
           ++si) {
        const MViewReportStmtData &st = stmt_array.at(si);
        double step_pct = (mv.elapsed_time_ > 0)
                          ? (static_cast<double>(st.execution_time_) / static_cast<double>(mv.elapsed_time_) * 100.0)
                          : 0.0;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(format_elapsed(st.execution_time_, elapsed_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format step elapsed", KR(ret));
        } else if (OB_FAIL(format_timestamp(st.start_time_, ts_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format step start", KR(ret));
        }
        const char *slowest_mark = (si == slowest_step_idx) ? "  <- SLOWEST" : "";
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(format_stmt_result(step_result, SMALL_BUF_LEN, st))) {
          LOG_WARN("step_result buffer overflow", KR(ret));
        } else if (OB_FAIL(databuff_printf(pct_buf, SMALL_BUF_LEN, "%.1f%%", step_pct))) {
          LOG_WARN("fail to format step pct", KR(ret), K(step_pct));
        } else if (OB_FAIL(report_text.append_fmt("    | %*ld | %-*.*s | %-*.*s | %*s | %*s | %-*s |%s\n",
                                                  STEP_TBL_COLS[0].width,
                                                  st.step_,
                                                  STEP_TBL_COLS[1].width,
                                                  st.sqlid_.length(),
                                                  safe_ptr_str(st.sqlid_),
                                                  STEP_TBL_COLS[2].width,
                                                  STEP_TBL_COLS[2].width,
                                                  ts_buf,
                                                  STEP_TBL_COLS[3].width,
                                                  elapsed_buf,
                                                  STEP_TBL_COLS[4].width,
                                                  pct_buf,
                                                  STEP_TBL_COLS[5].width,
                                                  step_result,
                                                  slowest_mark))) {
          LOG_WARN("fail to append step row", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append(tbl_sep_buf))) {
        LOG_WARN("fail to append step footer", KR(ret));
      }

      // Show resource usage and the rendered execution plan for every step
      // that has plan data (either full plan or hash-only).
      for (int64_t si = group.final_stmt_range_.first_;
           OB_SUCC(ret) && si <= group.final_stmt_range_.last_ && si < stmt_array.count();
           ++si) {
        const MViewReportStmtData &st = stmt_array.at(si);
        if (st.execution_plan_.empty()) {
        } else if (OB_FAIL(report_text.append_fmt("\n    Step (#%ld, SQL_ID: %.*s):\n",
                                                  st.step_,
                                                  st.sqlid_.length(),
                                                  safe_ptr_str(st.sqlid_)))) {
          LOG_WARN("fail to append step header", KR(ret));
        } else if (OB_FAIL(format_elapsed(st.cpu_time_, cpu_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format cpu", KR(ret));
        } else if (OB_FAIL(format_elapsed(st.io_wait_time_, io_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format io", KR(ret));
        } else if (OB_FAIL(format_memory(st.disk_reads_, num_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format disk io", KR(ret));
        } else if (OB_FAIL(format_memory(st.memory_used_, mem_buf, FMT_BUF_LEN))) {
          LOG_WARN("fail to format memory", KR(ret));
        } else if (OB_FAIL(report_text.append_fmt(
                       "      Resource Usage:  CPU %s | IO Wait %s | Disk IO %s | Memory %s\n",
                       cpu_buf, io_buf, num_buf, mem_buf))) {
          LOG_WARN("fail to append resource usage", KR(ret));
        } else if (OB_FAIL(append_text_execution_plan(*report.allocator_, st, report_text))) {
          LOG_WARN("fail to append execution plan", KR(ret));
        }
      }
    }
  }

  return ret;
}

static int build_text_footer(ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(report_text.append_fmt("\n%s", TEXT_SEPARATOR))) {
    LOG_WARN("fail to append footer", KR(ret));
  }
  return ret;
}

// ====== format_text_report — orchestrates all TEXT sections ======

static int format_text_report(const MViewRefreshReport &report,
                              const ObTimeZoneInfo *sys_tz_info,
                              ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(report.allocator_) || OB_ISNULL(report.run_data_) || OB_ISNULL(report.mv_array_)
      || OB_ISNULL(report.change_array_) || OB_ISNULL(report.stmt_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mview refresh report", KR(ret), KP(report.allocator_), KP(report.run_data_));
  } else {
    const MViewReportRunData &run_data = *report.run_data_;
    if (OB_FAIL(build_text_header(run_data, report_text, *report.allocator_))) {
      LOG_WARN("fail to build text header", KR(ret));
    } else if (OB_FAIL(build_text_summary(report, sys_tz_info, report_text))) {
      LOG_WARN("fail to build text summary", KR(ret));
    } else if (OB_FAIL(build_text_mv_table(report, report_text))) {
      LOG_WARN("fail to build text mv table", KR(ret));
    } else if (OB_FAIL(build_text_resource_overview(report, report_text))) {
      LOG_WARN("fail to build text resource overview", KR(ret));
    } else if (OB_FAIL(build_text_per_mv_detail(report, sys_tz_info, report_text))) {
      LOG_WARN("fail to build text per-mv detail", KR(ret));
    } else if (OB_FAIL(build_text_footer(report_text))) {
      LOG_WARN("fail to build text footer", KR(ret));
    }
  }

  return ret;
}

// ====== JSON report section functions ======

static int build_json_summary(const MViewRefreshReport &report, ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  char *ts_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  char *result_json_buf = NULL;
  if (OB_ISNULL(ts_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ts_buf", KR(ret));
  }

  const MViewReportRunData &run_data = *report.run_data_;
  const ObIArray<MViewReportMVData> &mv_array = *report.mv_array_;
  const MViewReportSummaryInfo &summary = report.summary_;
  const MViewReportResourceOverview &resources = report.resources_;
  const ObIArray<MViewReportMVGroup> &mv_groups = report.mv_groups_;

  const int64_t total_execute_time = summary.total_execute_time_us_;
  const int64_t total_cpu = resources.total_cpu_us_;
  const int64_t total_io_wait = resources.total_io_wait_us_;
  const int64_t total_disk_reads = resources.total_disk_reads_;
  const int64_t max_memory = resources.max_memory_bytes_;
  const int64_t total_steps = resources.total_steps_;
  const int64_t total_retry_overhead = summary.total_retry_overhead_us_;
  const int64_t num_distinct_mvs = summary.num_distinct_mvs_;
  const int64_t total_sched_overhead = summary.total_sched_overhead_us_;
  const int64_t num_failures = summary.num_failures_;
  const bool has_target_mv = run_data.has_target_mview();

  char *gen_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  result_json_buf = static_cast<char *>(report.allocator_->alloc(RESULT_DETAIL_BUF_LEN));
  if (OB_ISNULL(gen_buf) || OB_ISNULL(result_json_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc gen_buf or result_json_buf", KR(ret));
  } else if (OB_FAIL(format_timestamp(ObTimeUtility::current_time(), gen_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format generated time", KR(ret));
  } else if (OB_FAIL(report_text.append("\"summary\": {"))) {
    LOG_WARN("fail to open summary", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt("\"refresh_id\": %ld", run_data.refresh_id_))) {
    LOG_WARN("fail to append refresh_id", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"generated\": "))) {
    LOG_WARN("fail to append key", KR(ret));
  } else if (OB_FAIL(append_json_string(report_text, gen_buf, -1))) {
    LOG_WARN("fail to append generated", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"trace_id\": "))) {
    LOG_WARN("fail to append key", KR(ret));
  } else if (OB_FAIL(append_json_string(report_text, run_data.trace_id_))) {
    LOG_WARN("fail to append trace_id", KR(ret));
  } else if (OB_FAIL(format_timestamp(run_data.start_time_, ts_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format start_time", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"start_time\": "))) {
    LOG_WARN("fail to append key", KR(ret));
  } else if (OB_FAIL(append_json_string(report_text, ts_buf, -1))) {
    LOG_WARN("fail to append start_time", KR(ret));
  } else if (OB_FAIL(format_timestamp(run_data.end_time_, ts_buf, FMT_BUF_LEN))) {
    LOG_WARN("fail to format end_time", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"end_time\": "))) {
    LOG_WARN("fail to append key", KR(ret));
  } else if (OB_FAIL(append_json_string(report_text, ts_buf, -1))) {
    LOG_WARN("fail to append end_time", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt(", \"elapsed_time_us\": %ld"
                                            ", \"total_execute_time_us\": %ld"
                                            ", \"total_sched_overhead_us\": %ld"
                                            ", \"retry_overhead_us\": %ld"
                                            ", \"status\": \"%s\"",
                                            summary.elapsed_us_,
                                            total_execute_time,
                                            total_sched_overhead,
                                            total_retry_overhead,
                                            summary.status_str_))) {
    LOG_WARN("fail to append time/status", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"refresh_method\": "))) {
    LOG_WARN("fail to append key", KR(ret));
  } else if (OB_FAIL(append_json_string(report_text,
                                        summary.method_display_ != NULL ? summary.method_display_ : "AUTO",
                                        -1))) {
    LOG_WARN("fail to append method", KR(ret));
  } else if (!run_data.data_target_scn_known() && OB_FAIL(report_text.append(", \"target_data_scn\": null"))) {
    LOG_WARN("fail to append null target data scn", KR(ret));
  } else if (run_data.data_target_scn_known()
             && OB_FAIL(report_text.append_fmt(", \"target_data_scn\": %lu", run_data.data_target_scn_))) {
    LOG_WARN("fail to append target data scn", KR(ret));
  } else if (OB_FAIL(report_text.append_fmt(", \"num_mvs\": %ld"
                                            ", \"parallelism\": %ld"
                                            ", \"nested\": %s",
                                            num_distinct_mvs,
                                            run_data.parallelism_,
                                            run_data.nested_ ? "true" : "false"))) {
    LOG_WARN("fail to append params", KR(ret));
  } else if (has_target_mv && OB_FAIL(report_text.append(", \"target_mv\": "))) {
    LOG_WARN("fail to append key", KR(ret));
  } else if (has_target_mv && OB_FAIL(append_json_string(report_text, run_data.display_mview_name()))) {
    LOG_WARN("fail to append target_mv", KR(ret));
  } else if (!has_target_mv && OB_FAIL(report_text.append(", \"target_mv\": null"))) {
    LOG_WARN("fail to append null target", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"pending_queue_length\": null"))) {
    LOG_WARN("fail to append pending_queue", KR(ret));
  } else if (OB_FAIL(report_text.append(", \"mv_refresh_summary\": ["))) {
    LOG_WARN("fail to open mv_refresh_summary", KR(ret));
  }

  for (int64_t gi = 0; OB_SUCC(ret) && gi < num_distinct_mvs; ++gi) {
    const MViewReportMVGroup &group = mv_groups.at(gi);
    const MViewReportMVData &mv = get_final_mv(mv_array, group);
    if (gi > 0 && OB_FAIL(report_text.append(", "))) {
      LOG_WARN("fail to append comma", KR(ret));
    }
    // result_str setup (may set ret on failure)
    const char *result_str = NULL;
    if (mv.is_success()) {
      result_str = "OK";
    } else if (mv.is_running()) {
      result_str = STATUS_RUNNING;
    } else if (OB_FAIL(databuff_printf(result_json_buf,
                                       RESULT_DETAIL_BUF_LEN,
                                       "FAIL(%ld) %s",
                                       mv.result_,
                                       ob_error_name(static_cast<int>(mv.result_))))) {
      LOG_WARN("result_json_buf overflow", KR(ret), K(mv.result_));
    } else {
      result_str = result_json_buf;
    }
    const char *role_str = run_data.mv_role_label(mv.mview_id_);
    const int64_t display_num = gi + 1;
    const char *type_str = mv.type_name();
    const int64_t changes = group.changes_;
    double throughput = 0.0;
    const bool throughput_known = try_get_throughput(mv, group.changes_, throughput);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append_fmt("{\"topo_order\": %ld, \"role\": \"%s\"", display_num, role_str))) {
      LOG_WARN("fail to append topo/role", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else {
      if (OB_FAIL(report_text.append(", \"mv_name\": "))) {
        LOG_WARN("fail to append key", KR(ret));
      } else if (OB_FAIL(append_json_string(report_text, mv.display_name()))) {
        LOG_WARN("fail to append mv_name", KR(ret));
      } else if (OB_FAIL(report_text.append_fmt(", \"refresh_type\": \"%s\"", type_str))) {
        LOG_WARN("fail to append type", KR(ret));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_timestamp(mv.start_time_, ts_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format mv start", KR(ret));
    } else if (OB_FAIL(report_text.append(", \"start_time\": "))) {
      LOG_WARN("fail to append key", KR(ret));
    } else if (OB_FAIL(append_json_string(report_text, ts_buf, -1))) {
      LOG_WARN("fail to append start_time", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(format_timestamp(mv.end_time_, ts_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format mv end", KR(ret));
    } else if (OB_FAIL(report_text.append(", \"end_time\": "))) {
      LOG_WARN("fail to append key", KR(ret));
    } else if (OB_FAIL(append_json_string(report_text, ts_buf, -1))) {
      LOG_WARN("fail to append end_time", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (mv.has_deps_ready_time() && OB_FAIL(format_timestamp(mv.deps_ready_time_, ts_buf, FMT_BUF_LEN))) {
      LOG_WARN("fail to format deps_ready_time", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (mv.has_deps_ready_time()) {
      if (OB_FAIL(report_text.append(", \"deps_ready_time\": "))) {
        LOG_WARN("fail to append key", KR(ret));
      } else if (OB_FAIL(append_json_string(report_text, ts_buf, -1))) {
        LOG_WARN("fail to append deps_ready_time", KR(ret));
      }
    } else if (OB_FAIL(report_text.append(", \"deps_ready_time\": null"))) {
      LOG_WARN("fail to append null deps_ready", KR(ret));
    }
    {
      const int64_t raw_sched_delay = mv.sched_delay_us();
      const int64_t sched_delay = raw_sched_delay >= 0 ? raw_sched_delay : 0;
      const double elapsed_pct = get_elapsed_pct(mv, summary.elapsed_us_);
      const bool is_slowest = is_slowest_group(gi, num_distinct_mvs, summary);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append_fmt(", \"elapsed_time_us\": %ld"
                                                ", \"sched_delay_us\": %ld"
                                                ", \"elapsed_pct\": %.1f"
                                                ", \"changes\": %ld"
                                                ", \"throughput_per_sec\": ",
                                                mv.elapsed_time_,
                                                sched_delay,
                                                elapsed_pct,
                                                changes))) {
        LOG_WARN("fail to append mv summary start", KR(ret));
      } else if (throughput_known) {
        ret = report_text.append_fmt("%.1f", throughput);
      } else {
        ret = report_text.append("null");
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to append throughput", KR(ret));
      } else if (OB_FAIL(report_text.append_fmt(", \"result\": \"%s\""
                                                ", \"retried\": %s"
                                                ", \"slowest\": %s}",
                                                result_str,
                                                (group.num_retries() > 0) ? "true" : "false",
                                                is_slowest ? "true" : "false"))) {
        LOG_WARN("fail to append rest", KR(ret));
      }
    }
  }

  // When the target MV was not refreshed, add a placeholder entry.
  if (OB_FAIL(ret)) {
  } else if (run_data.has_nested_target()) {
    if (is_target_mv_missing(run_data, mv_array)) {
      const int64_t placeholder_num = num_distinct_mvs + 1;
      if (num_distinct_mvs > 0 && OB_FAIL(report_text.append(", "))) {
        LOG_WARN("fail to append comma", KR(ret));
      } else if (OB_FAIL(report_text.append_fmt("{\"topo_order\": %ld, \"role\": \"%s\"", placeholder_num, ROLE_TGT))) {
        LOG_WARN("fail to append placeholder header", KR(ret));
      } else if (OB_FAIL(report_text.append(", \"mv_name\": "))) {
        LOG_WARN("fail to append mv_name key", KR(ret));
      } else if (OB_FAIL(append_json_string(report_text, run_data.display_mview_name()))) {
        LOG_WARN("fail to append mv_name", KR(ret));
      } else if (OB_FAIL(report_text.append(", \"refresh_type\": null"
                                            ", \"start_time\": null"
                                            ", \"end_time\": null"
                                            ", \"elapsed_time_us\": null"
                                            ", \"sched_delay_us\": null"
                                            ", \"elapsed_pct\": null"
                                            ", \"changes\": null"
                                            ", \"throughput_per_sec\": null"
                                            ", \"result\": null"
                                            ", \"retried\": false"
                                            ", \"slowest\": false}"))) {
        LOG_WARN("fail to append placeholder rest", KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(report_text.append("]"))) {
    LOG_WARN("fail to close mv_refresh_summary", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (total_steps > 0) {
    if (OB_FAIL(report_text.append_fmt(", \"resource_overview\": {"
                                       "\"total_steps\": %ld"
                                       ", \"cpu_time_us\": %ld"
                                       ", \"io_wait_time_us\": %ld"
                                       ", \"disk_reads\": %ld"
                                       ", \"max_memory_bytes\": %ld}",
                                       total_steps,
                                       total_cpu,
                                       total_io_wait,
                                       total_disk_reads,
                                       max_memory))) {
      LOG_WARN("fail to append resource_overview", KR(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (num_distinct_mvs > 0) {
      if (OB_FAIL(report_text.append(", \"per_mv_resource\": ["))) {
        LOG_WARN("fail to open per_mv_resource", KR(ret));
      }

      bool first_mv_res = true;
      for (int64_t gi = 0; OB_SUCC(ret) && gi < num_distinct_mvs; ++gi) {
        const MViewReportMVGroup &group_res = mv_groups.at(gi);
        const MViewReportMVData &mv = get_final_mv(mv_array, group_res);
        if (!first_mv_res && OB_FAIL(report_text.append(", "))) {
          LOG_WARN("fail to append comma", KR(ret));
        }
        const int64_t mv_cpu = group_res.cpu_time_us_;
        const int64_t mv_io = group_res.io_wait_time_us_;
        const int64_t mv_dr = group_res.disk_reads_;
        const int64_t mv_mem = group_res.max_memory_bytes_;
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(report_text.append("{\"mv_name\": "))) {
          LOG_WARN("fail to append key", KR(ret));
        } else if (OB_FAIL(append_json_string(report_text, mv.display_name()))) {
          LOG_WARN("fail to append mv_name", KR(ret));
        } else if (OB_FAIL(report_text.append_fmt(", \"cpu_time_us\": %ld"
                                                  ", \"io_wait_time_us\": %ld"
                                                  ", \"disk_reads\": %ld"
                                                  ", \"max_memory_bytes\": %ld}",
                                                  mv_cpu,
                                                  mv_io,
                                                  mv_dr,
                                                  mv_mem))) {
          LOG_WARN("fail to append per_mv resource", KR(ret));
        }
        first_mv_res = false;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append("]"))) {
        LOG_WARN("fail to close per_mv_resource", KR(ret));
      }
    }
  } else if (OB_FAIL(report_text.append(", \"resource_overview\": null"))) {
    LOG_WARN("fail to append null resource_overview", KR(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(report_text.append("}"))) {
    LOG_WARN("fail to close summary", KR(ret));
  }

  return ret;
}

static int build_json_per_mv_detail(const MViewRefreshReport &report, ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  char *ts_buf = static_cast<char *>(report.allocator_->alloc(FMT_BUF_LEN));
  if (OB_ISNULL(ts_buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ts_buf", KR(ret));
  }

  const MViewReportRunData &run_data = *report.run_data_;
  const ObIArray<MViewReportMVData> &mv_array = *report.mv_array_;
  const ObIArray<MViewReportChangeData> &change_array = *report.change_array_;
  const ObIArray<MViewReportStmtData> &stmt_array = *report.stmt_array_;
  const MViewReportSummaryInfo &summary = report.summary_;
  const ObIArray<MViewReportMVGroup> &mv_groups = report.mv_groups_;

  const int64_t num_distinct_mvs = summary.num_distinct_mvs_;

  const bool skip_per_mv = should_skip_per_mv_detail(num_distinct_mvs, mv_array);
  if (OB_FAIL(ret)) {
  } else if (skip_per_mv) {
  } else if (OB_FAIL(report_text.append(", \"per_mv_detail\": ["))) {
    LOG_WARN("fail to open per_mv_detail", KR(ret));
  }

  for (int64_t gi = 0; OB_SUCC(ret) && !skip_per_mv && gi < num_distinct_mvs; ++gi) {
    const MViewReportMVGroup &group = mv_groups.at(gi);
    const int64_t last_idx = group.attempt_range_.last_;
    const int64_t first_idx = group.attempt_range_.first_;
    const MViewReportMVData &mv = mv_array.at(last_idx);
    if (gi > 0 && OB_FAIL(report_text.append(", "))) {
      LOG_WARN("fail to append comma", KR(ret));
    }
    // per-MV object opening + basic fields + rows.
    {
      const int64_t display_num = gi + 1;
      const char *type_str = mv.type_name();
      const char *role_str = run_data.mv_role_label(mv.mview_id_);
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append("{\"mv_name\": "))) {
        LOG_WARN("fail to open per_mv obj", KR(ret));
      } else if (OB_FAIL(append_json_string(report_text, mv.display_name()))) {
        LOG_WARN("fail to append mv_name", KR(ret));
      } else if (OB_FAIL(report_text.append_fmt(", \"display_order\": %ld"
                                                ", \"topo_order\": %ld"
                                                ", \"role\": \"%s\""
                                                ", \"refresh_type\": \"%s\""
                                                ", \"elapsed_time_us\": %ld"
                                                ", \"parallelism\": %ld",
                                                display_num,
                                                mv.topo_order_,
                                                role_str,
                                                type_str,
                                                mv.elapsed_time_,
                                                run_data.parallelism_))) {
        LOG_WARN("fail to append per_mv basics", KR(ret));
      }
      if (OB_FAIL(ret)) {
      } else if (mv.is_success()) {
        if (OB_FAIL(report_text.append_fmt(", \"initial_num_rows\": %ld"
                                           ", \"final_num_rows\": %ld",
                                           mv.initial_num_rows_,
                                           mv.final_num_rows_))) {
          LOG_WARN("fail to append rows", KR(ret));
        }
      } else if (OB_FAIL(report_text.append(", \"initial_num_rows\": null"
                                            ", \"final_num_rows\": null"))) {
        LOG_WARN("fail to append null rows", KR(ret));
      }
    }

    // Retry history.
    if (OB_FAIL(ret)) {
    } else if (group.num_retries() > 0) {
      if (OB_FAIL(report_text.append(", \"retry_history\": ["))) {
        LOG_WARN("fail to open retry_history", KR(ret));
      }

      for (int64_t ri = first_idx; OB_SUCC(ret) && ri <= last_idx; ++ri) {
        const MViewReportMVData &rv = mv_array.at(ri);
        if (ri > first_idx && OB_FAIL(report_text.append(", "))) {
          LOG_WARN("fail to append comma", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (rv.is_success()
                   && OB_FAIL(report_text.append_fmt("{\"retry_id\": %ld"
                                                     ", \"elapsed_time_us\": %ld"
                                                     ", \"result\": \"OK\""
                                                     ", \"error_code\": null}",
                                                     rv.retry_id_,
                                                     rv.elapsed_time_))) {
          LOG_WARN("fail to append retry", KR(ret));
        } else if (rv.is_running()
                   && OB_FAIL(report_text.append_fmt("{\"retry_id\": %ld"
                                                     ", \"elapsed_time_us\": %ld"
                                                     ", \"result\": \"%s\""
                                                     ", \"error_code\": null}",
                                                     rv.retry_id_,
                                                     rv.elapsed_time_,
                                                     STATUS_RUNNING))) {
          LOG_WARN("fail to append retry", KR(ret));
        } else if (rv.is_failed()
                   && OB_FAIL(report_text.append_fmt("{\"retry_id\": %ld"
                                                     ", \"elapsed_time_us\": %ld"
                                                     ", \"result\": \"FAIL\""
                                                     ", \"error_code\": %ld}",
                                                     rv.retry_id_,
                                                     rv.elapsed_time_,
                                                     rv.result_))) {
          LOG_WARN("fail to append retry", KR(ret));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(report_text.append("]"))) {
        LOG_WARN("fail to close retry_history", KR(ret));
      }
    }

    // SCN fields.
    if (OB_FAIL(ret)) {
    } else if (!mv.base_table_start_scn_known()
               && OB_FAIL(report_text.append(", \"current_data_scn\": null"))) {
      LOG_WARN("fail to append null current data scn", KR(ret));
    } else if (mv.base_table_start_scn_known()
               && OB_FAIL(report_text.append_fmt(", \"current_data_scn\": %lu", mv.base_table_start_scn_))) {
      LOG_WARN("fail to append current data scn", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (!mv.mv_refresh_start_scn_known()
               && OB_FAIL(report_text.append(", \"last_refresh_scn\": null"))) {
      LOG_WARN("fail to append null last refresh scn", KR(ret));
    } else if (mv.mv_refresh_start_scn_known()
               && OB_FAIL(report_text.append_fmt(", \"last_refresh_scn\": %lu", mv.mv_refresh_start_scn_))) {
      LOG_WARN("fail to append last refresh scn", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (!mv.mv_refresh_end_scn_known()
               && OB_FAIL(report_text.append(", \"current_refresh_scn\": null"))) {
      LOG_WARN("fail to append null current refresh scn", KR(ret));
    } else if (mv.mv_refresh_end_scn_known()
               && OB_FAIL(report_text.append_fmt(", \"current_refresh_scn\": %lu", mv.mv_refresh_end_scn_))) {
      LOG_WARN("fail to append current refresh scn", KR(ret));
    }

    // Server info.
    if (OB_FAIL(ret)) {
    } else if (!mv.has_server() && OB_FAIL(report_text.append(", \"svr_ip\": null, \"svr_port\": null"))) {
      LOG_WARN("fail to append null svr", KR(ret));
    } else if (mv.has_server()) {
      if (OB_FAIL(report_text.append(", \"svr_ip\": "))) {
        LOG_WARN("fail to append key", KR(ret));
      } else if (OB_FAIL(append_json_string(report_text, mv.svr_ip_))) {
        LOG_WARN("fail to append svr_ip", KR(ret));
      } else if (OB_FAIL(report_text.append_fmt(", \"svr_port\": %ld", mv.svr_port_))) {
        LOG_WARN("fail to append svr_port", KR(ret));
      }
    }

    // Base table changes.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append(", \"base_table_changes\": ["))) {
      LOG_WARN("fail to open changes", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (group.has_changes()) {
      bool first_ch = true;
      for (int64_t ci = group.final_change_range_.first_;
           OB_SUCC(ret) && ci <= group.final_change_range_.last_ && ci < change_array.count();
           ++ci) {
        const MViewReportChangeData &ch = change_array.at(ci);
        if (!first_ch && OB_FAIL(report_text.append(", "))) {
          LOG_WARN("fail to append comma", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(report_text.append("{\"table\": "))) {
          LOG_WARN("fail to append key", KR(ret));
        } else if (OB_FAIL(append_json_string(report_text, ch.display_name()))) {
          LOG_WARN("fail to append table", KR(ret));
        } else if (OB_FAIL(report_text.append_fmt(", \"ins\": %ld, \"upd\": %ld, \"del\": %ld, \"base_rows\": %ld}",
                                                  ch.num_rows_ins_,
                                                  ch.num_rows_upd_,
                                                  ch.num_rows_del_,
                                                  ch.num_rows_))) {
          LOG_WARN("fail to append change values", KR(ret));
        }
        first_ch = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append("]"))) {
      LOG_WARN("fail to close changes", KR(ret));
    }

    // Steps — mark slowest per MV and include captured plans for every step.
    int64_t mv_slowest_idx = group.slowest_stmt_idx_;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append(", \"steps\": ["))) {
      LOG_WARN("fail to open steps", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (group.has_stmts()) {
      bool first_st = true;
      for (int64_t si = group.final_stmt_range_.first_;
           OB_SUCC(ret) && si <= group.final_stmt_range_.last_ && si < stmt_array.count();
           ++si) {
        const MViewReportStmtData &st = stmt_array.at(si);
        if (!first_st && OB_FAIL(report_text.append(", "))) {
          LOG_WARN("fail to append comma", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(report_text.append_fmt("{\"step\": %ld", st.step_))) {
          LOG_WARN("fail to append step", KR(ret));
        } else if (OB_FAIL(report_text.append(", \"sql_id\": "))) {
          LOG_WARN("fail to append key", KR(ret));
        } else if (OB_FAIL(append_json_string(report_text, st.sqlid_))) {
          LOG_WARN("fail to append sqlid", KR(ret));
        } else if (OB_FAIL(report_text.append_fmt(", \"execution_time_us\": %ld, \"result\": %ld",
                                                  st.execution_time_,
                                                  st.result_))) {
          LOG_WARN("fail to append step stats", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (si == mv_slowest_idx) {
          if (OB_FAIL(report_text.append_fmt(", \"slowest\": true"
                                             ", \"cpu_time_us\": %ld"
                                             ", \"io_wait_time_us\": %ld"
                                             ", \"disk_reads\": %ld"
                                             ", \"memory_bytes\": %ld",
                                             st.cpu_time_,
                                             st.io_wait_time_,
                                             st.disk_reads_,
                                             st.memory_used_))) {
            LOG_WARN("fail to append slowest resources", KR(ret));
          }
        } else if (OB_FAIL(report_text.append(", \"slowest\": false"))) {
          LOG_WARN("fail to append slowest false", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(append_json_execution_plan(report_text, st))) {
          LOG_WARN("fail to append step execution plan", KR(ret));
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(report_text.append("}"))) {
          LOG_WARN("fail to close step obj", KR(ret));
        }
        first_st = false;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append("]"))) {
      LOG_WARN("fail to close steps", KR(ret));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(report_text.append("}"))) {
      LOG_WARN("fail to close per_mv obj", KR(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (skip_per_mv) {
  } else if (OB_FAIL(report_text.append("]"))) {
    LOG_WARN("fail to close per_mv_detail", KR(ret));
  }

  return ret;
}

// ====== format_json_report — orchestrates all JSON sections ======

static int format_json_report(const MViewRefreshReport &report,
                              const ObTimeZoneInfo * /*sys_tz_info*/,
                              ObSqlString &report_text)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(report.allocator_) || OB_ISNULL(report.run_data_) || OB_ISNULL(report.mv_array_)
      || OB_ISNULL(report.change_array_) || OB_ISNULL(report.stmt_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid mview refresh report", KR(ret), KP(report.allocator_), KP(report.run_data_));
  } else if (OB_FAIL(report_text.append("{"))) {
    LOG_WARN("fail to open root", KR(ret));
  } else if (OB_FAIL(build_json_summary(report, report_text))) {
    LOG_WARN("fail to build json summary", KR(ret));
  } else if (OB_FAIL(build_json_per_mv_detail(report, report_text))) {
    LOG_WARN("fail to build json per_mv_detail", KR(ret));
  } else if (OB_FAIL(report_text.append("}"))) {
    LOG_WARN("fail to close root", KR(ret));
  }

  return ret;
}

// ====== Public wrappers ======

int ob_mview_refresh_report_format_text_report(const MViewRefreshReport &report,
                                               const ObTimeZoneInfo *sys_tz_info,
                                               ObSqlString &report_text)
{
  return format_text_report(report, sys_tz_info, report_text);
}

int ob_mview_refresh_report_format_json_report(const MViewRefreshReport &report,
                                               const ObTimeZoneInfo *sys_tz_info,
                                               ObSqlString &report_text)
{
  return format_json_report(report, sys_tz_info, report_text);
}

}  // namespace storage
}  // namespace oceanbase

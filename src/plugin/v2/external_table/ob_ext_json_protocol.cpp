/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/v2/external_table/ob_ext_json_protocol.h"
#include "plugin/v2/external_table/ob_ext_json_internal.h"  // shared JSON accessors

#include "lib/string/ob_sql_string.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::internal;

// =============================================================================
// ext_type name <-> enum
// =============================================================================

namespace
{
struct ExtTypeName { ob_ext_obj_type type; const char *name; };
const ExtTypeName EXT_TYPE_NAMES[] = {
  {OB_EXT_TYPE_NULL, "NULL"},           {OB_EXT_TYPE_BOOL, "BOOL"},
  {OB_EXT_TYPE_TINYINT, "TINYINT"},     {OB_EXT_TYPE_SMALLINT, "SMALLINT"},
  {OB_EXT_TYPE_INT, "INT"},             {OB_EXT_TYPE_BIGINT, "BIGINT"},
  {OB_EXT_TYPE_FLOAT, "FLOAT"},         {OB_EXT_TYPE_DOUBLE, "DOUBLE"},
  {OB_EXT_TYPE_DECIMAL, "DECIMAL"},     {OB_EXT_TYPE_STRING, "STRING"},
  {OB_EXT_TYPE_VARCHAR, "VARCHAR"},     {OB_EXT_TYPE_CHAR, "CHAR"},
  {OB_EXT_TYPE_BINARY, "BINARY"},       {OB_EXT_TYPE_VARBINARY, "VARBINARY"},
  {OB_EXT_TYPE_DATE, "DATE"},           {OB_EXT_TYPE_DATETIME, "DATETIME"},
  {OB_EXT_TYPE_TIMESTAMP, "TIMESTAMP"}, {OB_EXT_TYPE_TIME, "TIME"},
  {OB_EXT_TYPE_ARRAY, "ARRAY"},         {OB_EXT_TYPE_MAP, "MAP"},
  {OB_EXT_TYPE_UNKNOWN, "UNKNOWN"},
};
const int64_t EXT_TYPE_NAME_COUNT = sizeof(EXT_TYPE_NAMES) / sizeof(EXT_TYPE_NAMES[0]);
} // namespace

ob_ext_obj_type ext_type_from_name(const char *name, int64_t len)
{
  ob_ext_obj_type ret = OB_EXT_TYPE_UNKNOWN;
  if (OB_NOT_NULL(name) && len > 0) {
    const ObString s(static_cast<ObString::obstr_size_t>(len), name);
    for (int64_t i = 0; i < EXT_TYPE_NAME_COUNT; ++i) {
      if (0 == s.case_compare(EXT_TYPE_NAMES[i].name)) { ret = EXT_TYPE_NAMES[i].type; break; }
    }
  }
  return ret;
}

// =============================================================================
// JSON tree accessors (find_member/member_int/member_str/serialize_node) and
// dup_cstr live in ob_ext_json_internal.h, shared with ob_ext_schema_parser.
// =============================================================================

namespace
{

// STRICT validation: log every key on a task object that is not one of the
// known scan-task fields. Called for each task element.
int warn_unknown_task_keys(const ObJsonNode *task)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(task) && task->json_type() == ObJsonNodeType::J_OBJECT) {
    const ObJsonObject *o = static_cast<const ObJsonObject *>(task);
    const uint64_t n = o->element_count();
    for (uint64_t i = 0; i < n; ++i) {
      ObString k;
      ObJsonNode *v = nullptr;
      if (OB_FAIL(o->get_value_by_idx(i, k, v))) {
        LOG_WARN("get task key by idx failed", K(ret), K(i));
        break;
      }
      if (0 != k.compare(OB_EXT_K_ROW_COUNT) && 0 != k.compare(OB_EXT_K_BYTE_SIZE)
          && 0 != k.compare(OB_EXT_K_PAYLOAD_B64) && 0 != k.compare(OB_EXT_K_FILES)
          && 0 != k.compare(OB_EXT_K_MIN_MAX) && 0 != k.compare(OB_EXT_K_SPLITTABLE)) {
        LOG_WARN("ext scan-task JSON: unknown key ignored", K(k));
      }
    }
  }
  return ret;
}

int append_escaped(ObSqlString &s, const char *str)
{
  int ret = OB_SUCCESS;
  for (const char *p = str; OB_SUCC(ret) && OB_NOT_NULL(p) && *p != '\0'; ++p) {
    const unsigned char c = static_cast<unsigned char>(*p);
    switch (c) {
      case '"':  ret = s.append("\\\""); break;
      case '\\': ret = s.append("\\\\"); break;
      case '\b': ret = s.append("\\b"); break;
      case '\f': ret = s.append("\\f"); break;
      case '\n': ret = s.append("\\n"); break;
      case '\r': ret = s.append("\\r"); break;
      case '\t': ret = s.append("\\t"); break;
      default:
        if (c < 0x20) { ret = s.append_fmt("\\u%04x", c); }
        else { ret = s.append(p, 1); }
        break;
    }
  }
  return ret;
}

} // namespace

// =============================================================================
// Public API
// =============================================================================

int parse_scan_tasks_json(ObIAllocator &alloc, const char *json, int64_t len,
                          ObExtScanTaskArray &out_scan_tasks)
{
  int ret = OB_SUCCESS;
  out_scan_tasks.data = nullptr;
  out_scan_tasks.count = 0;
  ObArenaAllocator tmp("ExtJsonParse");
  ObJsonNode *root = nullptr;
  const char *syntaxerr = nullptr;
  uint64_t err_offset = 0;
  if (OB_ISNULL(json) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty scan tasks json", K(ret), K(len));
  } else if (OB_FAIL(ObJsonParser::parse_json_text(&tmp, json, static_cast<uint64_t>(len),
                                                   syntaxerr, &err_offset, root))) {
    LOG_WARN("parse scan tasks json failed", K(ret), KCSTRING(syntaxerr), K(err_offset));
  } else if (OB_ISNULL(root) || root->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("scan tasks json root is not object", K(ret));
  } else {
    const ObJsonNode *sp = find_member(root, OB_EXT_K_TASKS);
    if (OB_ISNULL(sp) || sp->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("scan tasks json missing 'tasks' array", K(ret));
    } else {
      const uint64_t n = array_size(sp);
      if (0 == n) {
        out_scan_tasks.data = nullptr;
        out_scan_tasks.count = 0;
      } else {
        void *buf = alloc.alloc(sizeof(ObExtScanTask) * n);
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc scan tasks failed", K(ret), K(n));
        } else {
          ObExtScanTask *splits = static_cast<ObExtScanTask *>(buf);
          int32_t i = 0;
          for (uint64_t idx = 0; OB_SUCC(ret) && idx < n; ++idx, ++i) {
            const ObJsonNode *it = array_at(sp, idx);
            new (&splits[i]) ObExtScanTask();
            splits[i].row_count = member_int(it, OB_EXT_K_ROW_COUNT, -1);
            splits[i].byte_size = member_int(it, OB_EXT_K_BYTE_SIZE, -1);
            // STRICT: payload_b64 is required — reader_open_task cannot proceed
            // without it (it carries the format-private split bytes).
            const ObString payload = member_str(it, OB_EXT_K_PAYLOAD_B64);
            if (payload.empty()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("scan task missing required 'payload_b64'", K(ret), K(i));
            } else if (OB_FAIL(serialize_node(alloc, it, splits[i].data, splits[i].size))) {
              LOG_WARN("serialize scan task failed", K(ret), K(i));
            } else {
              (void)warn_unknown_task_keys(it);
            }
          }
          if (OB_SUCC(ret)) {
            out_scan_tasks.data = splits;
            out_scan_tasks.count = static_cast<int32_t>(n);
          }
        }
      }
    }
  }
  return ret;
}

int build_options_json(ObIAllocator &alloc, const char *const *keys,
                       const char *const *vals, int32_t count, ObString &out_json,
                       const char *const *raw_keys, int32_t raw_count)
{
  int ret = OB_SUCCESS;
  ObSqlString s;
  if (OB_ISNULL(keys) || OB_ISNULL(vals)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("null options arrays", K(ret));
  } else if (OB_FAIL(s.append("{"))) {
    LOG_WARN("append failed", K(ret));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      // Is this key's value a verbatim JSON fragment (raw)? Linear scan — count
      // is tiny (handful of options). Raw values get no quotes/escaping.
      bool is_raw = false;
      if (raw_keys != nullptr && raw_count > 0) {
        for (int32_t r = 0; r < raw_count; ++r) {
          if (raw_keys[r] != nullptr && keys[i] != nullptr
              && 0 == strcmp(raw_keys[r], keys[i])) {
            is_raw = true;
            break;
          }
        }
      }
      if (i > 0 && OB_FAIL(s.append(","))) {
      } else if (OB_FAIL(s.append("\""))) {
      } else if (OB_FAIL(append_escaped(s, keys[i]))) {
      } else if (is_raw) {
        // value is caller-guaranteed valid JSON; append verbatim, no quotes.
        if (OB_FAIL(s.append("\":"))) {
        } else if (OB_FAIL(s.append(vals[i] != nullptr ? vals[i] : "null"))) {
        }
      } else if (OB_FAIL(s.append("\":\""))) {
      } else if (OB_FAIL(append_escaped(s, vals[i]))) {
      } else if (OB_FAIL(s.append("\""))) {
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(s.append("}"))) {
      LOG_WARN("append failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    char *buf = static_cast<char *>(alloc.alloc(s.length() + 1));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc options json failed", K(ret), K(s.length()));
    } else {
      if (s.length() > 0) { MEMCPY(buf, s.ptr(), s.length()); }
      buf[s.length()] = '\0';
      out_json.assign_ptr(buf, static_cast<ObString::obstr_size_t>(s.length()));
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#undef USING_LOG_PREFIX

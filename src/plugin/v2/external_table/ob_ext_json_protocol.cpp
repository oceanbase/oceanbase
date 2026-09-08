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
          && 0 != k.compare(OB_EXT_K_PLUGIN_SPLIT) && 0 != k.compare(OB_EXT_K_FILES)
          && 0 != k.compare(OB_EXT_K_MIN_MAX) && 0 != k.compare(OB_EXT_K_SPLITTABLE)
          && 0 != k.compare(OB_EXT_K_OB_FILE_SCAN)
          && 0 != k.compare(OB_EXT_K_PARTITION_VALUES)) {
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

int copy_json_string(ObIAllocator &alloc, const ObJsonNode *node, ObString &dst)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(node) || ObJsonNodeType::J_STRING != node->json_type()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const ObString src(static_cast<int32_t>(node->get_data_length()), node->get_data());
    if (OB_FAIL(ob_write_string(alloc, src, dst))) {
      LOG_WARN("copy json string failed", K(ret));
    }
  }
  return ret;
}

bool validate_ob_file_scan(const ObJsonNode *ob_file_scan,
                           ObExtFileScanFormat &file_format,
                           uint64_t &file_count)
{
  bool valid = false;
  file_format = ObExtFileScanFormat::INVALID;
  file_count = 0;
  if (OB_NOT_NULL(ob_file_scan)
      && ObJsonNodeType::J_OBJECT == ob_file_scan->json_type()) {
    ObString file_format_name;
    const ObJsonNode *version = find_member(ob_file_scan, OB_EXT_K_OB_FILE_SCAN_VERSION);
    if (OB_NOT_NULL(version)
        && (ObJsonNodeType::J_INT == version->json_type()
            || ObJsonNodeType::J_UINT == version->json_type())
        && 1 == version->get_int()) {
      file_format_name = member_str(ob_file_scan, OB_EXT_K_OB_FILE_SCAN_FORMAT);
      if (0 == file_format_name.case_compare("parquet")) {
        file_format = ObExtFileScanFormat::PARQUET;
      } else if (0 == file_format_name.case_compare("orc")) {
        file_format = ObExtFileScanFormat::ORC;
      }
      const ObJsonNode *files = find_member(ob_file_scan, OB_EXT_K_FILES);
      file_count = array_size(files);
      valid = ObExtFileScanFormat::INVALID != file_format && file_count > 0;
      for (uint64_t i = 0; valid && i < file_count; ++i) {
        const ObJsonNode *file = array_at(files, i);
        const ObJsonNode *path = find_member(file, OB_EXT_K_OB_FILE_SCAN_PATH);
        const ObJsonNode *byte_size = find_member(file, OB_EXT_K_OB_FILE_SCAN_BYTE_SIZE);
        const ObJsonNode *row_count = find_member(file, OB_EXT_K_ROW_COUNT);
        valid = OB_NOT_NULL(file) && ObJsonNodeType::J_OBJECT == file->json_type()
            && OB_NOT_NULL(path) && ObJsonNodeType::J_STRING == path->json_type()
            && path->get_data_length() > 0 && OB_NOT_NULL(byte_size)
            && (ObJsonNodeType::J_INT == byte_size->json_type()
                || ObJsonNodeType::J_UINT == byte_size->json_type())
            && byte_size->get_int() >= 0 && OB_NOT_NULL(row_count)
            && (ObJsonNodeType::J_INT == row_count->json_type()
                || ObJsonNodeType::J_UINT == row_count->json_type())
            && row_count->get_int() >= 0;
      }
    }
  }
  return valid;
}

} // namespace

// =============================================================================
// Public API
// =============================================================================

int parse_scan_tasks_json(ObIAllocator &alloc, const char *json, int64_t len,
                          ObExtScanTaskArray &out_scan_tasks)
{
  int ret = OB_SUCCESS;
  out_scan_tasks.tasks = nullptr;
  out_scan_tasks.count = 0;
  out_scan_tasks.partition_filter_applied = false;
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
    const ObJsonNode *proof = find_member(root, OB_EXT_K_PARTITION_FILTER_APPLIED);
    if (OB_NOT_NULL(proof) && ObJsonNodeType::J_BOOLEAN != proof->json_type()) {
      LOG_WARN("invalid partition-filter planning proof; ignored");
    } else {
      out_scan_tasks.partition_filter_applied = member_bool(
          root, OB_EXT_K_PARTITION_FILTER_APPLIED, false);
    }
    if (OB_ISNULL(sp) || sp->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("scan tasks json missing 'tasks' array", K(ret));
    } else {
      const uint64_t n = array_size(sp);
      if (0 == n) {
        out_scan_tasks.tasks = nullptr;
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
            // STRICT: plugin_split is required — reader_open_task cannot proceed
            // without it (it carries the format-private split bytes).
            const ObString plugin_split = member_str(it, OB_EXT_K_PLUGIN_SPLIT);
            if (plugin_split.empty()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("scan task missing required 'plugin_split'", K(ret), K(i));
            } else if (OB_FAIL(serialize_node(
                           alloc, it, splits[i].task_json, splits[i].task_json_len))) {
              LOG_WARN("serialize scan task failed", K(ret), K(i));
            } else {
              (void)warn_unknown_task_keys(it);
            }
          }
          if (OB_SUCC(ret)) {
            out_scan_tasks.tasks = splits;
            out_scan_tasks.count = static_cast<int32_t>(n);
          }
        }
      }
    }
  }
  return ret;
}

int ObExtTaskPartitionValues::parse(ObIAllocator &alloc,
                                    const char *json,
                                    const int64_t len,
                                    ObExtTaskPartitionValues &values)
{
  int ret = OB_SUCCESS;
  values = ObExtTaskPartitionValues();
  ObArenaAllocator tmp("ExtTaskPart");
  ObJsonNode *root = nullptr;
  const char *syntaxerr = nullptr;
  uint64_t err_offset = 0;
  if (OB_ISNULL(json) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty plugin task json", K(ret), K(len));
  } else if (OB_FAIL(ObJsonParser::parse_json_text(
                 &tmp, json, static_cast<uint64_t>(len), syntaxerr, &err_offset, root))) {
    LOG_WARN("parse plugin task json failed", K(ret), KCSTRING(syntaxerr), K(err_offset));
  } else if (OB_ISNULL(root) || ObJsonNodeType::J_OBJECT != root->json_type()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("plugin task json root is not object", K(ret));
  } else {
    const ObJsonNode *partition_values = find_member(root, OB_EXT_K_PARTITION_VALUES);
    if (OB_ISNULL(partition_values)) {
      // Absence is interpreted against the table schema by the OB consumer.
    } else if (ObJsonNodeType::J_ARRAY != partition_values->json_type()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("plugin task partition_values is not array", K(ret));
    } else {
      const uint64_t count = array_size(partition_values);
      void *buf = count > 0 ? alloc.alloc(sizeof(ObExtTaskPartitionValue) * count) : nullptr;
      if (count > 0 && OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate task partition values failed", K(ret), K(count));
      } else {
        values.values_ = static_cast<ObExtTaskPartitionValue *>(buf);
        values.count_ = static_cast<int64_t>(count);
        for (uint64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
          const ObJsonNode *entry = array_at(partition_values, i);
          const ObJsonNode *field_id = find_member(entry, OB_EXT_K_FIELD_ID);
          const ObJsonNode *value = find_member(entry, OB_EXT_K_VALUE);
          if (OB_ISNULL(entry) || ObJsonNodeType::J_OBJECT != entry->json_type()
              || OB_ISNULL(field_id)
              || (ObJsonNodeType::J_INT != field_id->json_type()
                  && ObJsonNodeType::J_UINT != field_id->json_type())
              || field_id->get_int() < 0 || OB_ISNULL(value)
              || (ObJsonNodeType::J_STRING != value->json_type()
                  && ObJsonNodeType::J_NULL != value->json_type())) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("invalid task partition value", K(ret), K(i));
          } else {
            const int64_t id = field_id->get_int();
            for (uint64_t j = 0; OB_SUCC(ret) && j < i; ++j) {
              if (id == values.values_[j].field_id_) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("duplicate task partition field id", K(ret), K(id));
              }
            }
            if (OB_SUCC(ret)) {
              new (&values.values_[i]) ObExtTaskPartitionValue();
              values.values_[i].field_id_ = id;
              values.values_[i].is_null_ = ObJsonNodeType::J_NULL == value->json_type();
              if (!values.values_[i].is_null_
                  && OB_FAIL(copy_json_string(alloc, value, values.values_[i].value_))) {
                LOG_WARN("copy task partition value failed", K(ret), K(i));
              }
            }
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    values = ObExtTaskPartitionValues();
  }
  return ret;
}

int ObExtFileScanDescriptor::parse(
    ObIAllocator &alloc,
    const char *json,
    const int64_t len,
    ObExtFileScanDescriptor *&descriptor)
{
  int ret = OB_SUCCESS;
  descriptor = nullptr;
  ObArenaAllocator tmp("ExtFileScan");
  ObJsonNode *root = nullptr;
  const char *syntaxerr = nullptr;
  uint64_t err_offset = 0;
  if (OB_ISNULL(json) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty OB file scan task json", K(ret), K(len));
  } else if (OB_FAIL(ObJsonParser::parse_json_text(
                 &tmp, json, static_cast<uint64_t>(len), syntaxerr, &err_offset, root))) {
    LOG_WARN("parse OB file scan task json failed", K(ret), KCSTRING(syntaxerr), K(err_offset));
  } else if (OB_ISNULL(root) || ObJsonNodeType::J_OBJECT != root->json_type()) {
    ret = OB_INVALID_DATA;
    LOG_WARN("OB file scan task json root is not object", K(ret));
  } else {
    const ObJsonNode *ob_file_scan = find_member(root, OB_EXT_K_OB_FILE_SCAN);
    ObExtFileScanFormat file_format = ObExtFileScanFormat::INVALID;
    uint64_t file_count = 0;
    if (OB_ISNULL(ob_file_scan)) {
      // Descriptor is optional.
    } else if (!validate_ob_file_scan(ob_file_scan, file_format, file_count)) {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid OB file scan descriptor", K(ret));
    } else {
      void *desc_buf = alloc.alloc(sizeof(ObExtFileScanDescriptor));
      void *file_buf = alloc.alloc(sizeof(ObExtFileScanEntry) * file_count);
      if (OB_ISNULL(desc_buf) || OB_ISNULL(file_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate OB file scan descriptor failed", K(ret), K(file_count));
      } else {
        descriptor = new (desc_buf) ObExtFileScanDescriptor();
        descriptor->file_format_ = file_format;
        descriptor->files_ = static_cast<ObExtFileScanEntry *>(file_buf);
        descriptor->file_count_ = static_cast<int64_t>(file_count);
        const ObJsonNode *files = find_member(ob_file_scan, OB_EXT_K_FILES);
        for (uint64_t i = 0; OB_SUCC(ret) && i < file_count; ++i) {
          const ObJsonNode *file = array_at(files, i);
          new (&descriptor->files_[i]) ObExtFileScanEntry();
          descriptor->files_[i].byte_size_ = member_int(file, OB_EXT_K_OB_FILE_SCAN_BYTE_SIZE, -1);
          descriptor->files_[i].row_count_ = member_int(file, OB_EXT_K_ROW_COUNT, -1);
          if (OB_FAIL(copy_json_string(
                  alloc,
                  find_member(file, OB_EXT_K_OB_FILE_SCAN_PATH),
                  descriptor->files_[i].file_path_))) {
            LOG_WARN("copy OB file scan path failed", K(ret), K(i));
          }
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    descriptor = nullptr;
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

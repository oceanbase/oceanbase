/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/v2/external_table/ob_ext_schema_parser.h"

#include "plugin/v2/external_table/ob_ext_json_internal.h"  // shared JSON accessors
#include "plugin/v2/external_table/ob_ext_json_protocol.h"  // ext_type_from_name
#include "plugin/v2/external_table/ob_ext_type_mapper.h"    // ext_type_apply_to_column

#include "lib/oblog/ob_log_module.h"

#include <stdint.h>

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::internal;

namespace
{

typedef schema::ObColumnSchemaV2 ObExtParsedColumnSchema;

static const int64_t MAX_EXT_FIELD_ID =
    static_cast<int64_t>(common::OB_MIN_SHADOW_COLUMN_ID - common::OB_APP_MIN_COLUMN_ID - 1);

bool contains_field_id(const ObIArray<int64_t> &field_ids, int64_t field_id)
{
  bool found = false;
  for (int64_t i = 0; !found && i < field_ids.count(); ++i) {
    found = (field_ids.at(i) == field_id);
  }
  return found;
}

bool contains_column_name(const ObIArray<ObString> &names, const ObString &name)
{
  bool found = false;
  for (int64_t i = 0; !found && i < names.count(); ++i) {
    found = (0 == names.at(i).compare(name));
  }
  return found;
}

int get_i32_schema_attr(const ObJsonNode *obj, const char *key, int32_t &out)
{
  int ret = OB_SUCCESS;
  const int64_t value = member_int(obj, key, -1);
  if (value < -1 || value > INT32_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema integer attribute out of int32 range", K(ret), K(key), K(value));
  } else {
    out = static_cast<int32_t>(value);
  }
  return ret;
}

// STRICT validation: log every key on a column object that is not a known
// schema field. Called for each column element.
int warn_unknown_column_keys(const ObJsonNode *col)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(col) && col->json_type() == ObJsonNodeType::J_OBJECT) {
    const ObJsonObject *o = static_cast<const ObJsonObject *>(col);
    const uint64_t n = o->element_count();
    for (uint64_t i = 0; i < n; ++i) {
      ObString k;
      ObJsonNode *v = nullptr;
      if (OB_FAIL(o->get_value_by_idx(i, k, v))) {
        LOG_WARN("get column key by idx failed", K(ret), K(i));
        break;
      }
      if (0 != k.compare(OB_EXT_K_FIELD_ID) && 0 != k.compare(OB_EXT_K_NAME)
          && 0 != k.compare(OB_EXT_K_EXT_TYPE) && 0 != k.compare(OB_EXT_K_PRECISION)
          && 0 != k.compare(OB_EXT_K_SCALE) && 0 != k.compare(OB_EXT_K_LENGTH)
          && 0 != k.compare(OB_EXT_K_NULLABLE) && 0 != k.compare(OB_EXT_K_CHILDREN)) {
        LOG_WARN("ext schema JSON: unknown column key ignored", K(k));
      }
    }
  }
  return ret;
}

// Build the full OB collection-type SQL string for a column object, recursing
// through `children` for ARRAY/MAP. For a primitive column this is just its
// element SQL string (e.g. "INT"); for ARRAY it is "ARRAY(<element>)" where
// <element> is built recursively from children[0] (so "ARRAY(ARRAY(INT))"
// works); for MAP it is "MAP(<key>, <value>)" from children[0]/children[1].
// `col_json` is the column's ObJsonNode object. Used only for ARRAY/MAP
// columns — the caller drives primitives through ext_type_apply_to_column
// instead.
int build_type_sql(const ObJsonNode *col_json, ObSqlString &out)
{
  int ret = OB_SUCCESS;
  const ObString ext_str = member_str(col_json, OB_EXT_K_EXT_TYPE);
  const ob_ext_obj_type ext_type = ext_type_from_name(ext_str.ptr(), ext_str.length());
  if (ext_type == OB_EXT_TYPE_ARRAY) {
    const ObJsonNode *children = find_member(col_json, OB_EXT_K_CHILDREN);
    if (OB_ISNULL(children) || children->json_type() != ObJsonNodeType::J_ARRAY
        || array_size(children) != 1) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ARRAY column requires exactly one element child", K(ret));
    } else {
      const ObJsonNode *elem = array_at(children, 0);
      if (OB_ISNULL(elem) || elem->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ARRAY element is not a json object", K(ret));
      } else {
        ObSqlString elem_sql;
        if (OB_FAIL(build_type_sql(elem, elem_sql))) {
          LOG_WARN("build array element type sql failed", K(ret));
        } else if (OB_FAIL(out.append_fmt("ARRAY(%.*s)",
                                          static_cast<int32_t>(elem_sql.length()),
                                          elem_sql.ptr()))) {
          LOG_WARN("append ARRAY(...) wrapper failed", K(ret));
        }
      }
    }
  } else if (ext_type == OB_EXT_TYPE_MAP) {
    const ObJsonNode *children = find_member(col_json, OB_EXT_K_CHILDREN);
    if (OB_ISNULL(children) || children->json_type() != ObJsonNodeType::J_ARRAY
        || array_size(children) != 2) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("MAP column requires exactly two children (key, value)", K(ret));
    } else {
      const ObJsonNode *key = array_at(children, 0);
      const ObJsonNode *value = array_at(children, 1);
      if (OB_ISNULL(key) || key->json_type() != ObJsonNodeType::J_OBJECT
          || OB_ISNULL(value) || value->json_type() != ObJsonNodeType::J_OBJECT) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("MAP key/value child is not a json object", K(ret));
      } else {
        ObSqlString key_sql;
        ObSqlString value_sql;
        if (OB_FAIL(build_type_sql(key, key_sql))) {
          LOG_WARN("build map key type sql failed", K(ret));
        } else if (OB_FAIL(build_type_sql(value, value_sql))) {
          LOG_WARN("build map value type sql failed", K(ret));
        } else if (OB_FAIL(out.append_fmt("MAP(%.*s, %.*s)",
                                          static_cast<int32_t>(key_sql.length()), key_sql.ptr(),
                                          static_cast<int32_t>(value_sql.length()),
                                          value_sql.ptr()))) {
          LOG_WARN("append MAP(...) wrapper failed", K(ret));
        }
      }
    }
  } else {
    int32_t p = -1;
    int32_t s = -1;
    int32_t l = -1;
    if (OB_FAIL(get_i32_schema_attr(col_json, OB_EXT_K_PRECISION, p))
        || OB_FAIL(get_i32_schema_attr(col_json, OB_EXT_K_SCALE, s))
        || OB_FAIL(get_i32_schema_attr(col_json, OB_EXT_K_LENGTH, l))) {
      LOG_WARN("invalid nested schema integer attribute", K(ret), K(ext_type));
    } else if (OB_FAIL(ext_type_to_element_sql(ext_type, p, s, l, out))) {
      LOG_WARN("ext_type_to_element_sql failed", K(ret), K(ext_type));
    }
  }
  return ret;
}

} // namespace

int parse_schema_json(ObIAllocator &alloc, const char *json, int64_t len,
                      bool is_oracle_mode,
                      ObCharsetType cs_type, ObCollationType collation,
                      ObIArray<schema::ObColumnSchemaV2*> &out_columns,
                      ObIArray<ObString> *out_partition_key_names,
                      ObString *out_catalog_context)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp("ExtSchParse");
  ObJsonNode *root = nullptr;
  const char *syntaxerr = nullptr;
  uint64_t err_offset = 0;
  if (OB_ISNULL(json) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty schema json", K(ret), K(len));
  } else if (OB_FAIL(ObJsonParser::parse_json_text(&tmp, json, static_cast<uint64_t>(len),
                                                   syntaxerr, &err_offset, root))) {
    LOG_WARN("parse schema json failed", K(ret), KCSTRING(syntaxerr), K(err_offset));
  } else if (OB_ISNULL(root) || root->json_type() != ObJsonNodeType::J_OBJECT) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("schema json root is not object", K(ret));
  } else {
    // STRICT: unknown top-level keys are warned, not silently swallowed.
    {
      const ObJsonObject *o = static_cast<const ObJsonObject *>(root);
      const uint64_t n = o->element_count();
      for (uint64_t i = 0; OB_SUCC(ret) && i < n; ++i) {
        ObString k;
        ObJsonNode *v = nullptr;
        if (OB_FAIL(o->get_value_by_idx(i, k, v))) {
          LOG_WARN("get top-level key by idx failed", K(ret), K(i));
        } else if (0 != k.compare(OB_EXT_K_COLUMNS)
                   && 0 != k.compare(OB_EXT_K_PARTITION_KEYS)
                   && 0 != k.compare(OB_EXT_K_CATALOG_CONTEXT)) {
          LOG_WARN("ext schema JSON: unknown top-level key ignored", K(k));
        }
      }
    }

    if (OB_SUCC(ret) && OB_NOT_NULL(out_catalog_context)) {
      out_catalog_context->reset();
      const ObJsonNode *ctx = find_member(root, OB_EXT_K_CATALOG_CONTEXT);
      if (OB_NOT_NULL(ctx)) {
        const char *buf = nullptr;
        int32_t buflen = 0;
        if (OB_FAIL(serialize_node(alloc, ctx, buf, buflen))) {
          LOG_WARN("serialize catalog_context failed", K(ret));
        } else {
          // catalog_context is later consumed as a C string (build_options_json
          // embeds it via strlen in pruner's options_json): the backing buffer
          // MUST be NUL-terminated. ob_write_string does NOT add one.
          char *dst = static_cast<char *>(alloc.alloc(buflen + 1));
          if (OB_ISNULL(dst)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("alloc catalog_context failed", K(ret), K(buflen));
          } else {
            MEMCPY(dst, buf, buflen);
            dst[buflen] = '\0';
            out_catalog_context->assign_ptr(
                dst, static_cast<ObString::obstr_size_t>(buflen));
          }
        }
      }
    }

    // Parse partition_keys (option B: mark partition columns, do NOT build OB
    // partitions). Absent member => no partition columns (not an error). A
    // present-but-non-array member is a hard error (STRICT). Names are deep-
    // copied into the caller's `alloc`.
    if (OB_SUCC(ret) && OB_NOT_NULL(out_partition_key_names)) {
      const ObJsonNode *pkeys = find_member(root, OB_EXT_K_PARTITION_KEYS);
      if (OB_NOT_NULL(pkeys)) {
        if (pkeys->json_type() != ObJsonNodeType::J_ARRAY) {
          ret = OB_INVALID_ARGUMENT;
          LOG_WARN("schema json 'partition_keys' is not an array", K(ret));
        } else {
          const uint64_t cnt = array_size(pkeys);
          if (OB_FAIL(out_partition_key_names->reserve(cnt))) {
            LOG_WARN("reserve partition key names failed", K(ret));
          } else {
            for (uint64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
              const ObJsonNode *it = array_at(pkeys, i);
              if (OB_ISNULL(it) || it->json_type() != ObJsonNodeType::J_STRING) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("partition_keys entry is not a string", K(ret));
              } else {
                const ObString s(static_cast<int32_t>(it->get_data_length()), it->get_data());
                ObString copied;
                if (OB_FAIL(ob_write_string(alloc, s, copied))) {
                  LOG_WARN("deep copy partition key name failed", K(ret));
                } else if (OB_FAIL(out_partition_key_names->push_back(copied))) {
                  LOG_WARN("push back partition key name failed", K(ret));
                }
              }
            }
          }
        }
      }
    }

    const ObJsonNode *cols = find_member(root, OB_EXT_K_COLUMNS);
    if (OB_ISNULL(cols) || cols->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("schema json missing 'columns' array", K(ret));
    } else {
      const uint64_t cnt = array_size(cols);
      const int32_t n = static_cast<int32_t>(cnt);
      if (n <= 0) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("schema json 'columns' array is empty", K(ret));
      } else if (OB_FAIL(out_columns.reserve(n))) {
        LOG_WARN("reserve columns failed", K(ret), K(n));
      } else {
        int32_t i = 0;
        ObSEArray<int64_t, 16> field_ids;
        ObSEArray<ObString, 16> column_names;
        for (uint64_t idx = 0; OB_SUCC(ret) && idx < cnt; ++idx, ++i) {
          const ObJsonNode *it = array_at(cols, idx);
          if (OB_ISNULL(it) || it->json_type() != ObJsonNodeType::J_OBJECT) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column is not a json object", K(ret), K(i));
          } else {
            (void)warn_unknown_column_keys(it);

            // STRICT required fields: name, ext_type, field_id.
            const ObString name = member_str(it, OB_EXT_K_NAME);
            const ObString ext_type_str = member_str(it, OB_EXT_K_EXT_TYPE);
            const int64_t field_id = member_int(it, OB_EXT_K_FIELD_ID, -1);
            if (name.empty()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("schema column missing required 'name'", K(ret), K(i));
            } else if (ext_type_str.empty()) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("schema column missing required 'ext_type'", K(ret), K(i));
            } else if (field_id < 0 || field_id > MAX_EXT_FIELD_ID) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("schema column missing/invalid 'field_id'", K(ret), K(i), K(field_id),
                       K(MAX_EXT_FIELD_ID));
            } else if (contains_field_id(field_ids, field_id)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("schema column duplicate 'field_id'", K(ret), K(i), K(field_id));
            } else if (contains_column_name(column_names, name)) {
              ret = OB_INVALID_ARGUMENT;
              LOG_WARN("schema column duplicate 'name'", K(ret), K(i), K(name));
            } else {
              if (OB_FAIL(field_ids.push_back(field_id))) {
                LOG_WARN("push field id failed", K(ret), K(field_id));
              } else if (OB_FAIL(column_names.push_back(name))) {
                LOG_WARN("push column name failed", K(ret), K(name));
              }
            }
            if (OB_SUCC(ret)) {
              const ob_ext_obj_type ext_type =
                  ext_type_from_name(ext_type_str.ptr(), ext_type_str.length());
              ObExtParsedColumnSchema *col = OB_NEWx(ObExtParsedColumnSchema, &alloc, &alloc);
              bool col_pushed = false;
              if (OB_ISNULL(col)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("alloc column schema failed", K(ret), K(i));
              } else if (OB_FAIL(col->set_column_name(name))) {
                // set_column_name deep-copies into the column's allocator (alloc).
                LOG_WARN("set column name failed", K(ret), K(i), K(name));
              } else {
                // Carry raw field_id in column_id; the sql caller finalizes it to
                // field_id + OB_APP_MIN_COLUMN_ID and builds the column-id property.
                col->set_column_id(static_cast<uint64_t>(field_id));
                col->set_nullable(member_bool(it, OB_EXT_K_NULLABLE, true));
                int32_t precision = -1;
                int32_t scale = -1;
                int32_t length = -1;
                if (OB_FAIL(get_i32_schema_attr(it, OB_EXT_K_PRECISION, precision))
                    || OB_FAIL(get_i32_schema_attr(it, OB_EXT_K_SCALE, scale))
                    || OB_FAIL(get_i32_schema_attr(it, OB_EXT_K_LENGTH, length))) {
                  LOG_WARN("invalid schema integer attribute", K(ret), K(i), K(ext_type));
                } else if (ext_type == OB_EXT_TYPE_ARRAY || ext_type == OB_EXT_TYPE_MAP) {
                  // Collection: build "ARRAY(<element>)" / "MAP(<key>, <value>)"
                  // recursively and set ObCollectionSQLType + extended_type_info.
                  // ARRAY's element is children[0]; MAP's key/value are
                  // children[0]/children[1]; leaf types come from
                  // ext_type_to_element_sql (or a nested recursion).
                  ObSqlString type_sql;
                  if (OB_FAIL(build_type_sql(it, type_sql))) {
                    LOG_WARN("build collection type sql failed", K(ret), K(i));
                  } else if (OB_FAIL(ext_collection_apply_to_column(
                                 ObString(type_sql.length(), type_sql.ptr()), *col))) {
                    LOG_WARN("ext_collection_apply_to_column failed", K(ret), K(i));
                  } else if (OB_FAIL(out_columns.push_back(col))) {
                    LOG_WARN("push_back column failed", K(ret), K(i));
                  } else {
                    col_pushed = true;
                  }
                } else if (OB_FAIL(ext_type_apply_to_column(ext_type, is_oracle_mode, cs_type, collation,
                                                            precision, scale, length, *col))) {
                  LOG_WARN("ext_type_apply_to_column failed", K(ret), K(i), K(ext_type));
                } else if (OB_FAIL(out_columns.push_back(col))) {
                  LOG_WARN("push_back column failed", K(ret), K(i));
                } else {
                  col_pushed = true;
                }
              }
              if (OB_FAIL(ret) && OB_NOT_NULL(col) && !col_pushed) {
                OB_DELETEx(ObExtParsedColumnSchema, &alloc, col);
                col = nullptr;
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#undef USING_LOG_PREFIX

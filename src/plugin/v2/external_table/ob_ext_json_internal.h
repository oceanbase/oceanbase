/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_json_internal.h
/// \brief Internal JSON-tree accessors shared by the control-plane codecs
/// (`ob_ext_json_protocol.cpp` for scan-tasks/options, `ob_ext_schema_parser.cpp`
/// for schema). NOT a public header — do not include from outside share/
/// external_table. `static inline` so each TU gets its own copy with no linkage
/// friction; the single definition here is the one place these helpers live.
///
/// Backed by OB's rapidjson-based `lib/json_type` (`ObJsonParser::parse_json_text`
/// → `ObJsonNode`), which correctly handles standard JSON escapes (`\"`, `\\`,
/// etc.). The older `lib/json` `json::Parser` was swapped out because its
/// string tokenizer did not honor `\"` and broke on any string value that
/// contained an embedded quote (e.g. the plugin's `options_json` field carries
/// an escaped JSON document). See ob_ext_json_protocol.cpp / ob_ext_schema_parser.cpp
/// for the parse call sites.

#ifndef OB_EXT_JSON_INTERNAL_H
#define OB_EXT_JSON_INTERNAL_H

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_string_buffer.h"  // ObStringBuffer = ObJsonBuffer
#include "lib/json_type/ob_json_parse.h"   // ObJsonParser::parse_json_text
#include "lib/json_type/ob_json_tree.h"    // ObJsonNode / ObJsonObject / ObJsonArray
#include "lib/json_type/ob_json_common.h"  // ObJsonBuffer typedef
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace share
{
namespace internal
{

using common::ObIAllocator;
using common::ObJsonNode;
using common::ObJsonObject;
using common::ObJsonArray;
using common::ObJsonNodeType;
using common::ObJsonParser;
using common::ObString;
using common::ObStringBuffer;

/// Find a member by key on a JSON object node. Returns null if `obj` is not an
/// object or the key is absent.
static inline const ObJsonNode *find_member(const ObJsonNode *obj, const char *key)
{
  const ObJsonNode *ret = nullptr;
  if (OB_NOT_NULL(obj) && obj->json_type() == ObJsonNodeType::J_OBJECT) {
    const ObJsonObject *o = static_cast<const ObJsonObject *>(obj);
    // get_value(const ObString&) const — const overload returning ObJsonNode*.
    ret = o->get_value(ObString(key));
  }
  return ret;
}

/// Read an integer member, or `def` if absent / not a number.
static inline int64_t member_int(const ObJsonNode *obj, const char *key, int64_t def)
{
  int64_t ret = def;
  const ObJsonNode *v = find_member(obj, key);
  if (OB_NOT_NULL(v)) {
    switch (v->json_type()) {
      case ObJsonNodeType::J_INT:
      case ObJsonNodeType::J_UINT:
        ret = v->get_int();
        break;
      case ObJsonNodeType::J_DECIMAL:
      case ObJsonNodeType::J_DOUBLE:
        ret = static_cast<int64_t>(v->get_double());
        break;
      default:
        break;
    }
  }
  return ret;
}

/// Read a string member as an ObString view (points into the parsed tree's
/// memory; valid until the parser's arena is freed). Empty if absent / not a
/// string.
static inline ObString member_str(const ObJsonNode *obj, const char *key)
{
  ObString ret;
  const ObJsonNode *v = find_member(obj, key);
  if (OB_NOT_NULL(v) && v->json_type() == ObJsonNodeType::J_STRING) {
    ret = ObString(static_cast<int32_t>(v->get_data_length()), v->get_data());
  }
  return ret;
}

/// Read a boolean member, or `def` if absent.
static inline bool member_bool(const ObJsonNode *obj, const char *key, bool def)
{
  bool ret = def;
  const ObJsonNode *v = find_member(obj, key);
  if (OB_NOT_NULL(v) && v->json_type() == ObJsonNodeType::J_BOOLEAN) {
    ret = v->get_boolean();
  }
  return ret;
}

/// Re-serialize one JSON node into `alloc` as a JSON document text (used to
/// hand a single scan-task's text back to reader_create). The rapidjson-backed
/// tree does not expose a direct `to_string`; `ObIJsonBase::print` writes into
/// an ObJsonBuffer (= ObStringBuffer), which we then copy into the caller's
/// allocator.
static inline int serialize_node(ObIAllocator &alloc, const ObJsonNode *v,
                                 const char *&out, int32_t &out_len)
{
  int ret = OB_SUCCESS;
  out = nullptr;
  out_len = 0;
  if (OB_ISNULL(v)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObJsonBuffer jbuf(&alloc);
    if (OB_FAIL(v->print(jbuf, /*is_quoted=*/true))) {
      LOG_WARN("print json node failed", K(ret));
    } else if (jbuf.length() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("serialized json node is empty", K(ret));
    } else {
      char *dst = static_cast<char *>(alloc.alloc(jbuf.length()));
      if (OB_ISNULL(dst)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc node text failed", K(ret), K(jbuf.length()));
      } else {
        MEMCPY(dst, jbuf.ptr(), jbuf.length());
        out = dst;
        out_len = static_cast<int32_t>(jbuf.length());
      }
    }
  }
  return ret;
}

/// Get the array element count (0 if not an array).
static inline uint64_t array_size(const ObJsonNode *arr)
{
  uint64_t ret = 0;
  if (OB_NOT_NULL(arr) && arr->json_type() == ObJsonNodeType::J_ARRAY) {
    ret = static_cast<const ObJsonArray *>(arr)->element_count();
  }
  return ret;
}

/// Get array element by index, or null if out of range / not an array.
static inline const ObJsonNode *array_at(const ObJsonNode *arr, uint64_t i)
{
  const ObJsonNode *ret = nullptr;
  if (OB_NOT_NULL(arr) && arr->json_type() == ObJsonNodeType::J_ARRAY) {
    ret = static_cast<const ObJsonArray *>(arr)->operator[](i);
  }
  return ret;
}

} // namespace internal
} // namespace share
} // namespace oceanbase

#endif // OB_EXT_JSON_INTERNAL_H

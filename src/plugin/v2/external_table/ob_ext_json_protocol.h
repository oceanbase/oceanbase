/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_json_protocol.h
/// \brief OB-side codec for the external-table plugin control-plane JSON that is
/// format-neutral (scan tasks, options, the protocol type-name table).
///
/// The schema (columns) JSON has its own codec — `ob_ext_schema_parser` — that
/// parses directly into `ObColumnSchemaV2` (no intermediate struct). This header
/// is kept light on purpose: it must NOT include `ob_column_schema.h`, so that
/// `ob_ext_table_plugin_row_iter.h` (which needs only `ObExtScanTaskArray`) does not
/// drag the column-schema header into every translation unit that touches the
/// row iterator.
///
/// All parse outputs are allocated in the caller-provided allocator; there is no
/// destroy — lifetime follows the allocator. Field names come from the shared
/// `OB_EXT_K_*` constants in ob_external_table_plugin.h (single source of truth).
/// Parsing is STRICT: required keys missing or type-mismatched => error; unknown
/// keys => loud LOG_WARN (never silently swallowed).

#ifndef OB_EXT_JSON_PROTOCOL_H
#define OB_EXT_JSON_PROTOCOL_H

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_iarray.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // ob_ext_obj_type + OB_EXT_K_*

namespace oceanbase
{
namespace share
{

enum class ObExtFileScanFormat
{
  INVALID = 0,
  PARQUET,
  ORC,
};

struct ObExtFileScanEntry
{
  common::ObString file_path_;
  int64_t byte_size_ = -1;
  int64_t row_count_ = -1;  // unset until the descriptor supplies an exact non-negative value
};

struct ObExtTaskPartitionValue
{
  int64_t field_id_ = -1;
  bool is_null_ = true;
  common::ObString value_;
};

struct ObExtTaskPartitionValues
{
  static int parse(common::ObIAllocator &alloc,
                   const char *json,
                   int64_t len,
                   ObExtTaskPartitionValues &values);

  ObExtTaskPartitionValue *values_ = nullptr;
  int64_t count_ = 0;
};

struct ObExtFileScanDescriptor
{
  /// Parse the optional ob_file_scan object from one task JSON. A missing
  /// descriptor leaves `descriptor` null; a present invalid descriptor is an
  /// error. Returned objects are allocator-owned.
  /// @param [in] alloc allocator that owns all returned objects
  /// @param [in] json single task JSON text
  /// @param [in] len task JSON byte length
  /// @param [out] descriptor parsed descriptor, or null when absent
  /// @return OB_SUCCESS for absence or a valid descriptor; otherwise a JSON,
  /// protocol, or allocation error
  static int parse(
      common::ObIAllocator &alloc,
      const char *json,
      int64_t len,
      ObExtFileScanDescriptor *&descriptor);

  ObExtFileScanFormat file_format_ = ObExtFileScanFormat::INVALID;
  ObExtFileScanEntry *files_ = nullptr;
  int64_t file_count_ = 0;

  TO_STRING_KV(
      K_(file_format),
      KP_(files),
      K_(file_count));
};

/// One scan task parsed from the scan-tasks JSON. `task_json`/`task_json_len`
/// point at the
/// single task's JSON text (re-serialized into the allocator) that OB later
/// hands to `reader_create` as `task_json`. row_count/byte_size are the generic
/// fields OB uses for scheduling (-1 = unknown).
struct ObExtScanTask
{
  const char *task_json = nullptr;   // single-task JSON text, allocator-owned
  int32_t task_json_len = 0;
  int64_t row_count = -1;
  int64_t byte_size = -1;
};

/// All scan tasks of a scan (parse output of plan_create's JSON).
struct ObExtScanTaskArray
{
  ObExtScanTask *tasks = nullptr;
  int32_t count = 0;
  bool partition_filter_applied = false;
};

/// Canonical `ext_type` name <-> enum. Both OB and every plugin share these exact
/// strings in the control-plane JSON.
ob_ext_obj_type ext_type_from_name(const char *name, int64_t len);

/// Parse the scan-tasks JSON into `out_scan_tasks` (array + per-task JSON text
/// allocated in `alloc`). `tasks` must be an array and every task must carry
/// `plugin_split`; an empty plan is valid. Unknown keys are LOG_WARN'd.
int parse_scan_tasks_json(common::ObIAllocator &alloc,
                          const char *json, int64_t len,
                          ObExtScanTaskArray &out_scan_tasks);

/// Build the options JSON `{"k":"v",...}` (values JSON-escaped) into `out_json`,
/// allocated in `alloc`. `keys`/`vals` are parallel arrays of `count` C strings.
/// `raw_keys` (optional, `raw_count` entries) names keys whose value is already a
/// valid JSON fragment (e.g. an inline object) and is appended VERBATIM — no
/// quotes, no escaping — so the caller can embed a nested object as-is. The
/// caller guarantees such a value is valid JSON; OB otherwise never inspects it.
int build_options_json(common::ObIAllocator &alloc,
                       const char *const *keys, const char *const *vals,
                       int32_t count, common::ObString &out_json,
                       const char *const *raw_keys = nullptr,
                       int32_t raw_count = 0);

} // namespace share
} // namespace oceanbase

#endif // OB_EXT_JSON_PROTOCOL_H

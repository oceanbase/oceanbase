/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_external_table_protocol.h
/// \brief Control-plane vocabulary for the external-table plugin contract —
/// the PROTOCOL plane, split out of ob_external_table_plugin.h (the ABI
/// plane: vtable layout, host API, ABI version). Two kinds of vocabulary:
///   - contract errnos (OB_EXT_*): int values returned across the boundary;
///   - JSON field names (OB_EXT_K_*): keys of the control-plane documents.
///
/// Neither affects binary compatibility: no struct layout, function
/// signature, or entry symbol is involved, and independently compiled
/// OB/plugin builds tolerate unknown/missing JSON keys by design. Evolve this
/// file freely (new errno, new JSON key); ob_external_table_plugin.h and its
/// ABI version only change on true binary-contract changes.

#ifndef OB_EXTERNAL_TABLE_PROTOCOL_H
#define OB_EXTERNAL_TABLE_PROTOCOL_H

// =============================================================================
// Contract errnos — the only error values that may cross the boundary, shared by
// OB and every plugin as the single source of truth. The values are OB errnos
// VERBATIM (deps/oblib/src/lib/ob_errno.h): OB assigns a plugin's return directly
// to `ret` with no translation layer, and a plugin compiles WITHOUT ob_errno.h,
// so both sides MUST reference these constants rather than re-defining the
// numbers locally. OB compile-time-checks the equality (see the static_asserts
// in ob_ext_plugin_loader.cpp); never add a value here that is not an OB errno.
// =============================================================================
enum {
  OB_EXT_SUCCESS                = 0,      // == OB_SUCCESS
  OB_EXT_INVALID_ARGUMENT       = -4002,  // == OB_INVALID_ARGUMENT
  OB_EXT_NOT_SUPPORTED          = -4007,  // == OB_NOT_SUPPORTED
  OB_EXT_IO_ERROR               = -4009,  // == OB_IO_ERROR
  OB_EXT_ALLOCATE_MEMORY_FAILED = -4013,  // == OB_ALLOCATE_MEMORY_FAILED
  OB_EXT_ERR_UNEXPECTED         = -4016,  // == OB_ERR_UNEXPECTED
  OB_EXT_ENTRY_NOT_EXIST        = -4018,  // == OB_ENTRY_NOT_EXIST
  OB_EXT_FILE_NOT_EXIST         = -4027,  // == OB_FILE_NOT_EXIST
  OB_EXT_DESERIALIZE_ERROR      = -4034,  // == OB_DESERIALIZE_ERROR
  OB_EXT_DIR_NOT_EXIST          = -4066,  // == OB_DIR_NOT_EXIST
  OB_EXT_INVALID_DATA           = -4070,  // == OB_INVALID_DATA
  OB_EXT_OLD_SCHEMA_VERSION     = -4177,  // == OB_OLD_SCHEMA_VERSION
};

// =============================================================================
// Control-plane JSON field names — the single source of truth.
// OB and every plugin MUST reference these constants (not bare string literals)
// when producing or consuming the schema / scan-tasks JSON, so a typo or rename
// is a compile error at this one definition instead of a silent runtime mismatch.
// `static const char *const` is valid in both C and C++ (internal linkage; one
// copy per TU, negligible for this handful of short literals). Unknown keys in
// an input document are NOT silently ignored — OB logs a loud WARN; required
// keys missing/type-mismatched are a hard error.
// =============================================================================
static const char *const OB_EXT_K_COLUMNS     = "columns";   // schema: top array
static const char *const OB_EXT_K_FIELD_ID    = "field_id";  // schema column
static const char *const OB_EXT_K_NAME        = "name";      // schema column
static const char *const OB_EXT_K_EXT_TYPE    = "ext_type";  // schema column
static const char *const OB_EXT_K_PRECISION   = "precision"; // schema column
static const char *const OB_EXT_K_SCALE       = "scale";     // schema column
static const char *const OB_EXT_K_LENGTH      = "length";    // schema column
static const char *const OB_EXT_K_NULLABLE    = "nullable";  // schema column
static const char *const OB_EXT_K_CHILDREN    = "children";  // schema column (ARRAY element) | predicate node children
// schema: top-level array of partition column NAMES (option B: mark partition
// columns, do NOT build OB partitions). Empty/absent => no partition columns.
static const char *const OB_EXT_K_PARTITION_KEYS = "partition_keys";
// Opaque plugin-defined blob from load_schema (T0). OB stores it verbatim and
// passes it back inside options_json at plan_create and reader_create (raw
// JSON object under OB_EXT_K_CATALOG_CONTEXT); the plugin interprets it
// (schema version, etc.).
static const char *const OB_EXT_K_CATALOG_CONTEXT = "catalog_context";
static const char *const OB_EXT_K_TASKS       = "tasks";     // scan tasks: top array
static const char *const OB_EXT_K_ROW_COUNT   = "row_count"; // scan task
static const char *const OB_EXT_K_BYTE_SIZE   = "byte_size"; // scan task
static const char *const OB_EXT_K_PAYLOAD_B64 = "payload_b64"; // scan task
// scan task: reserved generic fields (OB reads row_count/byte_size today; the
// rest OB learns to exploit incrementally). All are known keys — the strict
// unknown-key WARN must not fire on them.
static const char *const OB_EXT_K_FILES       = "files";     // scan task: [{"path","size"}]
static const char *const OB_EXT_K_MIN_MAX     = "min_max";   // scan task: {"<col_idx>":["lo","hi"]}
static const char *const OB_EXT_K_SPLITTABLE  = "splittable";// scan task
// read_projection (OB->plugin): {"field_ids":[..]} (null/absent -> read all).
static const char *const OB_EXT_K_FIELD_IDS   = "field_ids";

// Predicate-tree JSON (OB->plugin), shared by predicate_json and
// partition_filter_json. See the protocol comment in ob_external_table_plugin.h
// for the grammar.
static const char *const OB_EXT_K_KIND    = "kind";     // lit/col/cmp/in/not_in/is_null/is_not_null/and/or/not
static const char *const OB_EXT_K_OP      = "op";       // cmp: eq/ne/lt/le/gt/ge
static const char *const OB_EXT_K_COL_IDX = "col_idx"; // col node: column field_id (informational)
static const char *const OB_EXT_K_VALUE   = "value";    // lit node: value as string

// Recognize-context JSON (OB->plugin, catalog format detection). Passed to the
// optional recognize_table slot so each format owns its own detection rules.
static const char *const OB_EXT_K_CATALOG_TYPE  = "catalog_type";   // "filesystem" | "hms"
static const char *const OB_EXT_K_DIRS          = "dirs";           // filesystem: child dir names
static const char *const OB_EXT_K_OUTPUT_FORMAT = "output_format";  // hms: sd.outputFormat

// options_json (OB->plugin) per-plan-call JSON object. Known OB keys:
//   "location", "access_info" — structural (plugin skips when building scan opts)
//   OB_EXT_K_CATALOG_CONTEXT — opaque object from load_schema (plugin-only)
//   OB_EXT_K_EXT_OPTIONS — per-query tuning blob (nested object, merged flat)
// OB passes ext_options and catalog_context as verbatim JSON fragments.
static const char *const OB_EXT_K_EXT_OPTIONS  = "ext_options";

#endif  // OB_EXTERNAL_TABLE_PROTOCOL_H

/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_schema_parser.h
/// \brief THE authoritative `ObColumnSchemaV2` <-> control-plane JSON codec for
/// the external-table plugin contract.
///
/// This is the single place that turns a plugin's schema JSON into OB column
/// schemas — there is no intermediate struct (no `ObExtColumnDesc`). It parses
/// each column object straight into an `ObColumnSchemaV2`: name (deep-copied by
/// `set_column_name`), raw `field_id` carried in `column_id` (finalized by the
/// sql caller to `field_id + OB_APP_MIN_COLUMN_ID`), nullability, and the data
/// type/accuracy via `ext_type_apply_to_column` (which dispatches onto
/// `ObExternalTableColumnSchemaHelper::setup_*`).
///
/// Parsing is STRICT (route one): field names come from the shared `OB_EXT_K_*`
/// constants in ob_external_table_plugin.h; required keys missing or
/// type-mismatched => `OB_INVALID_ARGUMENT`; unknown keys => loud `LOG_WARN`
/// (never silently swallowed). Output columns are heap-allocated in the caller's
/// allocator and live until it is reset (mirrors iceberg's `Schema::parse_field_`).

#ifndef OB_EXT_SCHEMA_PARSER_H
#define OB_EXT_SCHEMA_PARSER_H

#include "lib/ob_define.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_iarray.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // ob_ext_obj_type + OB_EXT_K_*
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace share
{

/// Parse the table-schema JSON into `out_columns` (one heap `ObColumnSchemaV2*`
/// per column, allocated in `alloc`; name deep-copied into `alloc`; raw
/// `field_id` stored in `column_id` for the caller to finalize). STRICT
/// validation. Returns an OB errno.
///
/// If `out_partition_key_names` is non-null, also collects the top-level
/// `partition_keys` name list (option B: mark partition columns, do NOT build
/// OB partitions). Each name is deep-copied into `alloc`. Absent/empty list is
/// valid (no partition columns); a non-array member is a hard error.
///
/// If `out_catalog_context` is non-null, deep-copies the optional top-level
/// `catalog_context` object (serialized JSON text). Empty when absent. OB
/// passes it back inside options_json at plan_create; only the plugin interprets it.
int parse_schema_json(common::ObIAllocator &alloc,
                      const char *json, int64_t len,
                      bool is_oracle_mode,
                      common::ObCharsetType cs_type,
                      common::ObCollationType collation,
                      common::ObIArray<schema::ObColumnSchemaV2*> &out_columns,
                      common::ObIArray<common::ObString> *out_partition_key_names = nullptr,
                      common::ObString *out_catalog_context = nullptr);

} // namespace share
} // namespace oceanbase

#endif // OB_EXT_SCHEMA_PARSER_H

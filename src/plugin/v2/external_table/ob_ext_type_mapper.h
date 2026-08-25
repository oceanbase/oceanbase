/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_type_mapper.h
/// \brief Apply the contract's `ob_ext_obj_type` (the "protocol type" the plugin
/// emits) onto an `ObColumnSchemaV2` — the "协议化类型在ob转为ob的类型" step.
///
/// This is the ONLY place that maps a protocol type to an OB column data type /
/// accuracy. It delegates to the existing `ObExternalTableColumnSchemaHelper::
/// setup_*` helpers already used by native lake-table paths — it does NOT
/// reimplement type/charset logic. Column name / nullability / column-id are set
/// by the caller (the schema parser), NOT here; this function owns type +
/// accuracy only.
///
/// BINARY / VARBINARY / NULL / UNKNOWN / MAP are NOT mapped as primitive columns
/// (OB_NOT_SUPPORTED). ARRAY is handled via the collection mechanism
/// (`ext_collection_apply_to_column` + `ext_type_to_element_sql`), not `setup_*`.

#ifndef OB_EXT_TYPE_MAPPER_H
#define OB_EXT_TYPE_MAPPER_H

#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "lib/string/ob_string.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // ob_ext_obj_type
#include "share/schema/ob_column_schema.h"

namespace oceanbase
{
namespace share
{

/// Apply `ext_type` (+ precision/scale/length for the accuracy-bearing types)
/// onto `column`'s data type and accuracy by dispatching onto
/// `ObExternalTableColumnSchemaHelper::setup_*`. `cs_type`/`collation` apply to
/// the string-family types (STRING/VARCHAR/CHAR). Does NOT touch column name,
/// nullability, or column_id. Returns OB_NOT_SUPPORTED for unmapped types
/// (incl. ARRAY/MAP — those go through ext_collection_apply_to_column /
/// ext_type_to_element_sql).
int ext_type_apply_to_column(ob_ext_obj_type ext_type,
                             bool is_oracle_mode,
                             common::ObCharsetType cs_type,
                             common::ObCollationType collation,
                             int32_t precision,
                             int32_t scale,
                             int32_t length,
                             schema::ObColumnSchemaV2 &column);

/// Map a PRIMITIVE `ext_type` to the OB element SQL string used inside a
/// collection type, e.g. INT -> "INT", STRING -> "VARCHAR(65535)" (OB array
/// elements cannot be LOB strings, so STRING is substituted), DECIMAL ->
/// "DECIMAL(p,s)". Used by the schema parser when building ARRAY type strings.
/// Returns OB_NOT_SUPPORTED for ARRAY/MAP/NULL/UNKNOWN (only the parser's
/// recursion handles ARRAY; MAP is not supported).
int ext_type_to_element_sql(ob_ext_obj_type ext_type,
                            int32_t precision, int32_t scale, int32_t length,
                            common::ObSqlString &out);

/// Set `column` to a collection (ARRAY) type: data_type = ObCollectionSQLType,
/// extended_type_info = {`type_str`} where `type_str` is the full OB collection
/// type string, e.g. "ARRAY(INT)" / "ARRAY(ARRAY(VARCHAR(65535)))". The parser
/// builds `type_str` (recursively for nested arrays) and hands it here;
/// set_extended_type_info deep-copies into the column's allocator.
int ext_collection_apply_to_column(const common::ObString &type_str,
                                   schema::ObColumnSchemaV2 &column);

} // namespace share
} // namespace oceanbase

#endif // OB_EXT_TYPE_MAPPER_H

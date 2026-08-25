/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_table_metadata.h
/// \brief Generic external-table metadata reader for the plugin contract.
///
/// Implements `share::ObILakeTableMetadata` by driving the plugin vtable
/// (`ObExtTablePluginApi::load_schema`) — the plugin returns a schema JSON
/// document, which `share::parse_schema_json` (the authoritative
/// `ObColumnSchemaV2` <-> JSON codec) turns directly into `ObColumnSchemaV2`
/// columns. It supports any plugin-backed format. If the format's plugin .so is not loaded,
/// `do_build_table_schema` returns OB_NOT_SUPPORTED — i.e. a format is
/// selectable only when its .so is present.

#ifndef OB_EXT_TABLE_METADATA_H
#define OB_EXT_TABLE_METADATA_H

#include "share/catalog/ob_external_catalog.h"  // ObILakeTableMetadata
#include "plugin/v2/include/ob_external_table_plugin.h"
#include "plugin/v2/external_table/ob_ext_schema_parser.h"  // parse_schema_json
#include "share/schema/ob_column_schema.h"             // ObColumnSchemaV2

#include <optional>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

class ObExtTableMetadata final : public share::ObILakeTableMetadata
{
public:
  explicit ObExtTableMetadata(ObIAllocator &allocator)
      : ObILakeTableMetadata(allocator) {}
  ~ObExtTableMetadata();

  // The plugin identity is the enum value itself (a plugin-placeholder range
  // value encoding the recognizing plugin's slot). It flows through plan/CG/iter
  // as the lake_table_format_ field already present in every plan struct — no
  // separate plugin name/slot field, no OB_UNIS_VERSION bump.
  share::ObLakeTableFormat get_format_type() const override { return table_format_; }
  // The plugin format name is exposed only for the schema-render path
  // (set_external_file_format_ builds the PLUGIN_TYPE string from it). Available
  // only after load_ext_schema_ has resolved the plugin api via the slot encoded
  // in table_format_; returns empty before that.
  common::ObString get_plugin_name() const override
  {
    return OB_NOT_NULL(api_) && OB_NOT_NULL(api_->format_name) ? ObString(api_->format_name())
                                                               : ObString();
  }
  bool is_ext_plugin_metadata() const override { return true; }
  int64_t get_convert_size() const override { return -1; }
  // Accessors for the pruner (plugin plan_create needs the table URI + access info).
  const ObString &get_table_location() const { return table_location_; }
  const ObString &get_access_info() const { return access_info_; }
  // Partition column ids (option B): the OB column ids (field_id + OB_APP_MIN_COLUMN_ID)
  // of columns the plugin declared as partition keys. Used by the pruner/iter to split
  // WHERE predicates into partition_filter_json (partition pruning) vs predicate_json
  // (row filter). Empty when the plugin declared no partition keys. No OB partitions
  // are built — partition pruning is delegated to the plugin/SDK.
  const common::ObIArray<uint64_t> &get_partition_col_ids() const { return partition_col_ids_; }
  // Opaque catalog context from load_schema (plugin-defined JSON). Passed verbatim
  // to plan_create; OB does not interpret it.
  const common::ObString &get_catalog_context_json() const { return catalog_context_json_; }

  int init(const uint64_t tenant_id,
           const uint64_t catalog_id,
           const uint64_t database_id,
           const uint64_t table_id,
           const ObString &namespace_name,
           const ObString &table_name,
           const ObNameCaseMode case_mode,
           share::ObLakeTableFormat table_format,
           const ObString &table_location,
           const int64_t location_object_id,
           const ObString &location_object_sub_path,
           const ObString &access_info);

protected:
  int do_build_table_schema(std::optional<int32_t> schema_id,
                            std::optional<int64_t> snapshot_id,
                            share::schema::ObTableSchema *&table_schema) override;

private:
  int load_ext_schema_();
  int setup_columns_(share::schema::ObTableSchema &table_schema);
  int set_external_file_format_(const ObString &db_name,
                                const ObString &tbl_name,
                                share::schema::ObTableSchema &table_schema);

  share::ObLakeTableFormat table_format_ = share::ObLakeTableFormat::INVALID;
  ObString table_location_;
  int64_t location_object_id_ = OB_INVALID_ID;
  ObString location_object_sub_path_;
  ObString access_info_;
  // Plugin vtable + the parsed columns (heap in allocator_; the plugin's JSON
  // buffer is released via the plugin's schema_destroy after parsing).
  const ObExtTablePluginApi *api_ = nullptr;
  // Parsed directly from the schema JSON by share::parse_schema_json — no
  // intermediate struct. Each column's column_id carries the raw field_id until
  // setup_columns_ finalizes it.
  common::ObSEArray<share::schema::ObColumnSchemaV2*, 8> columns_;
  // OB column ids (field_id + OB_APP_MIN_COLUMN_ID) of partition-key columns,
  // resolved from the schema JSON's top-level partition_keys name list.
  common::ObSEArray<uint64_t, 4> partition_col_ids_;
  // Opaque plugin catalog context (serialized JSON object text from load_schema).
  common::ObString catalog_context_json_;
};

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#endif // OB_EXT_TABLE_METADATA_H

/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL

#include "plugin/v2/external_table/ob_ext_table_metadata.h"

#include "plugin/v2/host/ob_ext_host_provider.h"
#include "plugin/v2/host/ob_ext_malloc_guard.h"
#include "plugin/v2/external_table/ob_ext_plugin_util.h"
#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "plugin/v2/external_table/ob_ext_json_protocol.h"  // build_options_json
#include "share/schema/ob_table_schema.h"
#include "sql/resolver/dml/ob_dml_resolver.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/allocator/page_arena.h"
#include "share/rc/ob_tenant_base.h"

#include <string>
#include <vector>

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{

using namespace common;
using namespace share::schema;

ObExtTableMetadata::~ObExtTableMetadata()
{
  // Parsed columns_ are heap ObColumnSchemaV2* in allocator_ and released with it;
  // the plugin's JSON buffer was released right after parsing via schema_destroy.
}

int ObExtTableMetadata::init(const uint64_t tenant_id,
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
                             const ObString &access_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObILakeTableMetadata::init(tenant_id, catalog_id, database_id, table_id,
                                         namespace_name, table_name, case_mode))) {
    LOG_WARN("failed to init lake table metadata", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, table_location, table_location_, true))) {
    LOG_WARN("failed to copy table location", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, location_object_sub_path,
                                     location_object_sub_path_, true))) {
    LOG_WARN("failed to copy location object sub path", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, access_info, access_info_, true))) {
    LOG_WARN("failed to copy access info", K(ret));
  } else {
    location_object_id_ = location_object_id;
    // The enum value carries the plugin slot identity.
    table_format_ = table_format;
  }
  return ret;
}

int ObExtTableMetadata::load_ext_schema_()
{
  int ret = OB_SUCCESS;
  // Resolve the plugin vtable by the slot encoded in the enum value (not by
  // name — the name is gone from the runtime flow). An invalid/unknown slot
  // means the plugin .so for this format is not resident -> not selectable.
  const share::ObPluginSlot slot = share::lake_plugin_slot_of(table_format_);
  api_ = share::ObExtFormatRegistry::get_instance().get_plugin_by_slot(slot);
  if (OB_ISNULL(api_) || OB_ISNULL(api_->load_schema)) {
    // The plugin .so for this format is not present -> the format is not selectable.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("ext plugin not loaded for format", K(ret), K(table_format_), K(slot));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external table plugin not loaded");
  }

  if (OB_SUCC(ret)) {
    // Tag the plugin's internal mallocs (and the host pool's attr) with the
    // plugin's own format name so each format's memory is separately visible.
    const char *fmt = OB_NOT_NULL(api_->format_name) ? api_->format_name() : nullptr;
    ObExtMallocGuard guard(fmt);  // covers the load_schema call below

    // Host capability table + its OB context (stack-local: load_schema is synchronous).
    ObExtHostCtx host_ctx;
    if (OB_FAIL(host_ctx.default_fs.set_path(table_location_))) {
      LOG_WARN("failed to set fs path", K(ret));
    } else if (OB_FAIL(host_ctx.default_fs.set_access_info(access_info_))) {
      LOG_WARN("failed to set fs access info", K(ret));
    }
    host_ctx.default_fs.set_cache_options(ObExternalFileCacheOptions());
    host_ctx.default_fs.set_tenant_id(tenant_id_);
    host_ctx.select_arrow_pool(true);
    host_ctx.pool->set_attr(get_ext_mem_attr(fmt));
    host_ctx.executor = nullptr;
    ObExtTableHostApi host;
    build_ext_host_api(host, &host_ctx);

    // Options: location + access info as kv pairs -> JSON (plugin interprets).
    // Arena copies are NUL-terminated and valid for the whole stage.
    const ObString &loc_str = host_ctx.default_fs.path();
    const ObString &acc_str = host_ctx.default_fs.access_info();
    const char *keys[] = {"location", "access_info"};
    const char *vals[] = {loc_str.ptr(), acc_str.ptr()};
    ObString options_json;
    char *schema_json = nullptr;
    int32_t schema_len = 0;
    if (OB_FAIL(ret)) {
      // set_path/set_access_info failed above
    } else if (OB_FAIL(share::build_options_json(allocator_, keys, vals, 2, options_json))) {
      LOG_WARN("failed to build options json", K(ret));
    } else {
      columns_.reuse();
      partition_col_ids_.reuse();
      catalog_context_json_.reset();
      // Oracle mode is not supported for the generic plugin schema conversion (the
      // setup_* helpers below target mysql types). Guarded here so the parse never
      // runs in oracle mode.
      const bool is_oracle = lib::is_oracle_mode();
      const ObCharsetType cs_type = ObCharsetType::CHARSET_UTF8MB4;
      const ObCollationType collation = ObCollationType::CS_TYPE_UTF8MB4_BIN;
      // Collect the partition-key name list alongside the columns (option B) so the
      // pruner/iter can split WHERE predicates without OB-side partition info.
      ObSEArray<ObString, 4> partition_key_names;
      // The plugin returns an OB errno verbatim and logs its own diagnostic (with the
      // plugin-side source location) via host->log before returning — grep "[ExtPlugin]"
      // in observer.log for the stack. OB consumes only the errno here.
      int rc = api_->load_schema(loc_str.ptr(), options_json.ptr(), &host,
                                 &schema_json, &schema_len);
      if (rc != OB_SUCCESS || OB_ISNULL(schema_json) || schema_len <= 0) {
        ret = (rc != OB_SUCCESS) ? rc : OB_ERR_UNEXPECTED;  // rc is already an OB errno
        LOG_WARN("plugin load_schema failed", K(ret));
      } else if (is_oracle) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "ext plugin schema conversion in oracle mode");
        LOG_WARN("ext plugin schema conversion in oracle mode is not supported", K(ret));
      } else if (OB_FAIL(share::parse_schema_json(allocator_, schema_json, schema_len,
                                                  /*is_oracle_mode=*/false, cs_type, collation,
                                                  columns_, &partition_key_names,
                                                  &catalog_context_json_))) {
        LOG_WARN("failed to parse schema json", K(ret));
      } else {
        // Resolve partition key names to OB column ids (field_id + OB_APP_MIN_COLUMN_ID).
        // Names not found in the column set are logged and skipped (STRICT would be too
        // harsh here — a stale partition list must not break schema load).
        for (int64_t i = 0; OB_SUCC(ret) && i < partition_key_names.count(); ++i) {
          const ObString &pk_name = partition_key_names.at(i);
          bool found = false;
          for (int64_t j = 0; !found && j < columns_.count(); ++j) {
            const ObColumnSchemaV2 *col = columns_.at(j);
            if (OB_NOT_NULL(col) && 0 == col->get_column_name_str().compare(pk_name)) {
              // column_id still holds the raw field_id here (finalized in setup_columns_).
              const uint64_t field_id = col->get_column_id();
              const uint64_t ob_col_id = field_id + OB_APP_MIN_COLUMN_ID;
              if (ob_col_id >= OB_MIN_SHADOW_COLUMN_ID) {
                ret = OB_INVALID_ARGUMENT;
                LOG_WARN("ext schema partition field id out of OB column id range",
                         K(ret), K(field_id), K(ob_col_id));
              } else if (OB_FAIL(partition_col_ids_.push_back(ob_col_id))) {
                LOG_WARN("push back partition col id failed", K(ret));
              } else {
                found = true;
              }
            }
          }
          if (OB_SUCC(ret) && !found) {
            LOG_WARN("ext schema: partition key name not found in columns, skipped",
                     K(pk_name));
          }
        }
      }
      // The schema JSON is the plugin's output buffer; the plugin — not OB — owns its
      // release (it may be a static constant or allocator-owned). OB calls the plugin's
      // schema_destroy, which decides free vs no-op.
      ob_ext_schema_destroy(api_, schema_json, schema_len, &host);
    }
  }
  return ret;
}

int ObExtTableMetadata::set_external_file_format_(const ObString &db_name,
                                                  const ObString &tbl_name,
                                                  ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  UNUSED(db_name);
  UNUSED(tbl_name);
  ObExternalFileFormat format;
  format.format_type_ = ObExternalFileFormat::FormatType::CPP_PLUGIN_FORMAT;
  // The format name is read from the resolved plugin api (api_->format_name());
  // it is only needed here to render the PLUGIN_TYPE string into external_file_format_.
  // This is the sole remaining name surface for lake plugin tables; the runtime
  // flow carries the enum value (table_format_), not the name.
  if (OB_ISNULL(api_) || OB_ISNULL(api_->format_name)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("plugin api not resolved before rendering format string", K(ret), K(table_format_));
  } else {
    const char *fmt = api_->format_name();
    format.get_cpp_plugin_format().plugin_name_ = ObString(fmt);
    ObArenaAllocator tmp_allocator;
    ObString format_str;
    if (OB_FAIL(format.to_string_with_alloc(format_str, tmp_allocator))) {
      LOG_WARN("failed to render external file format string", K(ret), KCSTRING(fmt));
    } else if (OB_FAIL(table_schema.set_external_file_format(format_str))) {
      LOG_WARN("failed to set external file format", K(ret));
    }
  }
  return ret;
}

int ObExtTableMetadata::setup_columns_(ObTableSchema &table_schema)
{
  int ret = OB_SUCCESS;
  // Oracle-mode guard also lives in load_ext_schema_ (parse is skipped); keep it
  // here too so a direct call still fails safe.
  if (lib::is_oracle_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "ext plugin schema conversion in oracle mode");
    LOG_WARN("ext plugin schema conversion in oracle mode is not supported", K(ret));
  } else if (columns_.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ext schema is empty", K(ret), K(columns_.count()));
  } else {
    for (int32_t i = 0; OB_SUCC(ret) && i < columns_.count(); ++i) {
      ObColumnSchemaV2 *col = columns_.at(i);
      if (OB_ISNULL(col)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parsed column is null", K(ret), K(i));
        continue;
      }
      // The parser carried the raw format-internal field id in column_id; finalize
      // it to OB's scheme (field id + OB_APP_MIN_COLUMN_ID).
      const uint64_t field_id = col->get_column_id();
      const uint64_t ob_col_id = field_id + OB_APP_MIN_COLUMN_ID;
      if (ob_col_id >= OB_MIN_SHADOW_COLUMN_ID) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("ext schema field id out of OB column id range", K(ret), K(field_id), K(ob_col_id));
      } else {
        col->set_column_id(ob_col_id);
      }
      ObSqlString prop_str;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(prop_str.append_fmt("%s%ld", N_EXTERNAL_TABLE_COLUMN_ID,
                                             static_cast<int64_t>(field_id)))) {
        LOG_WARN("failed to append column id property", K(ret));
      } else if (OB_FAIL(ObDMLResolver::set_basic_column_properties(*col, prop_str.string()))) {
        LOG_WARN("failed to set basic column properties", K(ret), K(i));
      } else {
        col->set_table_id(table_id_);
        if (OB_FAIL(table_schema.add_column(*col))) {
          LOG_WARN("failed to add column", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObExtTableMetadata::do_build_table_schema(std::optional<int32_t> schema_id,
                                              std::optional<int64_t> snapshot_id,
                                              share::schema::ObTableSchema *&table_schema)
{
  int ret = OB_SUCCESS;
  UNUSED(schema_id);
  UNUSED(snapshot_id);
  if (OB_FAIL(load_ext_schema_())) {  // guard lives inside load_ext_schema_ (format name known there)
    LOG_WARN("failed to load ext table schema", K(ret), K(namespace_name_), K(table_name_));
  } else if (OB_ISNULL(table_schema = OB_NEWx(schema::ObTableSchema, &allocator_, &allocator_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    table_schema->set_tenant_id(tenant_id_);
    table_schema->set_database_id(database_id_);
    table_schema->set_table_id(table_id_);
    table_schema->set_table_name(table_name_);
    table_schema->set_lake_table_format(table_format_);
    // table_format_ carries the plugin slot identity; the PLUGIN_TYPE string
    // rendered by set_external_file_format_ below is only the schema-load surface
    // for external_file_format_.
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(set_external_file_format_(namespace_name_, table_name_, *table_schema))) {
      LOG_WARN("failed to set external file format", K(ret));
    } else if (OB_FAIL(table_schema->set_external_file_location(table_location_))) {
      LOG_WARN("failed to set external file location", K(ret));
    } else if (location_object_id_ != OB_INVALID_ID) {
      table_schema->set_external_location_id(location_object_id_);
      if (OB_FAIL(table_schema->set_external_sub_path(location_object_sub_path_))) {
        LOG_WARN("failed to set external sub path", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(setup_columns_(*table_schema))) {
      LOG_WARN("failed to setup columns", K(ret));
    }
  }
  return ret;
}

} // namespace ext_plugin
} // namespace sql
} // namespace oceanbase

#undef USING_LOG_PREFIX

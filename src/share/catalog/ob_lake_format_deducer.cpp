/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "share/catalog/ob_lake_format_deducer.h"
#include "share/ob_define.h"

#include "plugin/v2/external_table/ob_ext_format_registry.h"
#include "plugin/v2/include/ob_external_table_plugin.h"
#include "lib/string/ob_sql_string.h"

namespace oceanbase
{
namespace share
{

namespace
{

// Built-in fallbacks for native (non-plugin-backed) formats only.
static const char *const LAKE_FMT_ICEBERG_METADATA_DIR = "metadata";

static const char *const LAKE_FMT_CATALOG_FILESYSTEM = "filesystem";
static const char *const LAKE_FMT_CATALOG_HMS = "hms";

static int append_json_escaped_string_(ObSqlString &buf, const ObString &str)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < str.length(); i++) {
    const char c = str.ptr()[i];
    if ('"' == c || '\\' == c) {
      OZ(buf.append("\\"));
    }
    char one[2] = { c, '\0' };
    OZ(buf.append(one));
  }
  return ret;
}

static int build_filesystem_recognize_json_(ObIAllocator &allocator,
                                            const ObIArray<ObString> &table_dirs,
                                            ObString &out_json)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  OZ(buf.append_fmt("{\"%s\":\"%s\",\"%s\":[",
                    OB_EXT_K_CATALOG_TYPE, LAKE_FMT_CATALOG_FILESYSTEM,
                    OB_EXT_K_DIRS));
  for (int64_t i = 0; OB_SUCC(ret) && i < table_dirs.count(); i++) {
    if (i > 0) {
      OZ(buf.append(","));
    }
    OZ(buf.append("\""));
    OZ(append_json_escaped_string_(buf, table_dirs.at(i)));
    OZ(buf.append("\""));
  }
  OZ(buf.append("]}"));
  OZ(ob_write_string(allocator, buf.string(), out_json, true));
  return ret;
}

static int build_hms_recognize_json_(ObIAllocator &allocator,
                                     const ObString &output_format,
                                     ObString &out_json)
{
  int ret = OB_SUCCESS;
  ObSqlString buf;
  OZ(buf.append_fmt("{\"%s\":\"%s\",\"%s\":\"",
                    OB_EXT_K_CATALOG_TYPE, LAKE_FMT_CATALOG_HMS,
                    OB_EXT_K_OUTPUT_FORMAT));
  OZ(append_json_escaped_string_(buf, output_format));
  OZ(buf.append("\"}"));
  OZ(ob_write_string(allocator, buf.string(), out_json, true));
  return ret;
}

static bool dirs_contain_(const ObIArray<ObString> &dirs, const char *name)
{
  bool found = false;
  for (int64_t i = 0; !found && i < dirs.count(); i++) {
    if (0 == dirs.at(i).case_compare(name)) {
      found = true;
    }
  }
  return found;
}

static int deduce_native_from_filesystem_dirs_(const ObIArray<ObString> &table_dirs,
                                               ObLakeTableFormat &table_format)
{
  int ret = OB_SUCCESS;
  table_format = ObLakeTableFormat::INVALID;
  if (dirs_contain_(table_dirs, LAKE_FMT_ICEBERG_METADATA_DIR)) {
    table_format = ObLakeTableFormat::ICEBERG;
  }
  return ret;
}

static int try_plugin_recognize_(const ObString &table_uri,
                                 const ObString &recognize_json,
                                 const ObExtTableHostApi *host,
                                 ObPluginSlot &plugin_slot)
{
  int ret = OB_SUCCESS;
  plugin_slot = -1;
  const ObExtTablePluginApi *api = nullptr;
  if (OB_FAIL(ObExtFormatRegistry::get_instance().recognize_format(
          table_uri, recognize_json, host, api))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("plugin recognize_format failed", K(ret));
    }
  } else if (OB_NOT_NULL(api)) {
    // recognize_format returned the recognizing plugin's vtable pointer; resolve it
    // to the stable slot. The plugin identity is then encoded directly into a
    // plugin-placeholder ObLakeTableFormat value that flows through plan/CG/iter
    // — no separate name or slot field is carried.
    plugin_slot = ObExtFormatRegistry::get_instance().get_slot_by_api(api);
    if (!is_valid_lake_plugin_slot(plugin_slot)) {
      // api not found among resident plugins — should not happen for an api just
      // returned by recognize_format; an invalid slot is likewise not encodable.
      // Treat either case as "no plugin recognized".
      LOG_WARN("recognize_format returned an api with no encodable resident slot",
               K(ret), K(plugin_slot));
      plugin_slot = -1;
    }
  }
  return ret;
}

} // namespace

int ObLakeFormatDeducer::deduce_from_filesystem(ObIAllocator &allocator,
                                                const ObString &table_uri,
                                                const ObIArray<ObString> &table_dirs,
                                                const ObExtTableHostApi *host,
                                                ObLakeTableFormat &table_format)
{
  int ret = OB_SUCCESS;
  ObString recognize_json;
  table_format = ObLakeTableFormat::INVALID;
  ObPluginSlot plugin_slot = -1;
  if (OB_FAIL(build_filesystem_recognize_json_(allocator, table_dirs, recognize_json))) {
    LOG_WARN("build filesystem recognize json failed", K(ret));
  } else if (OB_FAIL(try_plugin_recognize_(table_uri, recognize_json, host, plugin_slot))) {
    LOG_WARN("plugin recognize failed", K(ret));
  } else if (plugin_slot >= 0) {
    // Encode the plugin slot directly into the enum value — the enum IS the identity.
    table_format = lake_plugin_format_of(plugin_slot);
  } else if (ObLakeTableFormat::INVALID == table_format) {
    OZ(deduce_native_from_filesystem_dirs_(table_dirs, table_format));
  }
  return ret;
}

int ObLakeFormatDeducer::deduce_from_hms(ObIAllocator &allocator,
                                         const ObHmsTableDeduceInput &input,
                                         ObLakeTableFormat &table_format,
                                         ObString &table_location)
{
  int ret = OB_SUCCESS;
  table_format = ObLakeTableFormat::INVALID;
  table_location.reset();
  ObPluginSlot plugin_slot = -1;

  // Iceberg: HMS table parameter metadata_location (native, not plugin-backed yet).
  if (!input.iceberg_metadata_location.empty()) {
    table_format = ObLakeTableFormat::ICEBERG;
    OZ(ob_write_string(allocator, input.iceberg_metadata_location, table_location, true));
  }

  if (OB_SUCC(ret) && ObLakeTableFormat::INVALID == table_format) {
    ObString recognize_json;
    if (OB_FAIL(build_hms_recognize_json_(allocator, input.output_format, recognize_json))) {
      LOG_WARN("build hms recognize json failed", K(ret));
    } else if (OB_FAIL(try_plugin_recognize_(input.sd_location, recognize_json,
                                             nullptr, plugin_slot))) {
      LOG_WARN("plugin recognize failed", K(ret));
    }
  }

  if (OB_SUCC(ret) && plugin_slot >= 0) {
    // Encode the plugin slot directly into the enum value — the enum IS the identity.
    table_format = lake_plugin_format_of(plugin_slot);
  }

  if (OB_SUCC(ret) && plugin_slot < 0 && ObLakeTableFormat::INVALID == table_format) {
    table_format = ObLakeTableFormat::HIVE;
  }

  if (OB_SUCC(ret) && table_location.empty()) {
    OZ(ob_write_string(allocator, input.sd_location, table_location, true));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#undef USING_LOG_PREFIX

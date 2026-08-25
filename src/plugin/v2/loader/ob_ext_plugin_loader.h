/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_plugin_loader.h
/// \brief dlopen loader for the generic external-table format plugin contract.
///
/// Plugins are loaded LAZILY from the `ext_plugin_config` cluster config: the
/// first registry lookup (ObExtFormatRegistry::get_plugin_by_slot /
/// recognize_format) reads the config and dlopens every named plugin. A format
/// that has not been loaded is simply absent from the registry — accessing its
/// tables fails with "plugin not loaded".
///
/// The .so is located either:
///   - by explicit absolute path (the optional `path` field of a config entry), or
///   - by SONAME convention `lib_ob_<name>.so` (lowercase) under LD_LIBRARY_PATH.
///     `_ob_additional_lib_path` (':' -separated multi-path OK) is ensured into
///     LD_LIBRARY_PATH first, then the unified env is searched — same path as
///     libhdfs.so / libjvm.so (see ObLdLibraryPathUtil).
///
/// A successfully version-checked handle is RESIDENT for the process lifetime
/// (NEVER dlclose'd — its function pointers and plugin-initialized global
/// state are in use). A failure (dlopen / dlsym / ABI mismatch / not found)
/// leaves nothing resident: the broken handle is dlclose'd by load_ext_plugin
/// (safe — its symbols aren't in use before the version check passes), so the
/// operator can fix/replace the .so and it is picked up on the next restart.

#ifndef OB_EXT_PLUGIN_LOADER_H
#define OB_EXT_PLUGIN_LOADER_H

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "plugin/v2/include/ob_external_table_plugin.h"

namespace oceanbase
{
namespace common { class ObSqlString; }
namespace share
{

/// Outcome category of a plugin load attempt, produced by the loader and
/// consumed by ObExtFormatRegistry for diagnostic logging.
enum ObExtPluginLoadStatus
{
  EXT_PLUGIN_NOT_ATTEMPTED = 0,
  EXT_PLUGIN_SUCCESS,
  EXT_PLUGIN_NOT_FOUND,        // soname not on LD_LIBRARY_PATH
  EXT_PLUGIN_DLOPEN_FAILED,    // dlopen error
  EXT_PLUGIN_NO_SYMBOL,        // dlsym(ob_ext_table_plugin_get_api) missing
  EXT_PLUGIN_ABI_MISMATCH,     // plugin rejected ABI version
  EXT_PLUGIN_NO_FORMAT_NAME,   // api->format_name() returned NULL
  EXT_PLUGIN_INVALID_ARG,      // empty name / bad argument
  EXT_PLUGIN_ALREADY_LOADED,   // a plugin with this format_name is resident (set by registry dedup)
};

/// One loaded external-table plugin .so: the resident dlopen handle, the immutable
/// vtable returned by ob_ext_table_plugin_get_api, and the canonical format name
/// the plugin claims (api->format_name()). The format-name pointer is owned by the
/// plugin and stays valid for the process lifetime because the dl handle is NEVER
/// dlclose'd (resident), so it may be held without copying.
struct ObExtLoadedPlugin
{
  void *dl_handle_;                    // resident (never dlclose)
  const ObExtTablePluginApi *api_;     // immutable vtable
  const char *format_name_;            // api->format_name(), plugin-owned, resident
  int16_t slot_;                       // == the entry's position in ext_plugin_config (0..255). A failed config entry leaves its slot as a hole (no ObExtLoadedPlugin occupies it; api_==nullptr). -1 until assigned.
  // Absolute path the loader dlopen'd (the config `path` override, or the soname
  // resolved under LD_LIBRARY_PATH). Copied at load success; empty before/without.
  char path_[common::MAX_PATH_SIZE];
  ObExtLoadedPlugin() : dl_handle_(nullptr), api_(nullptr), format_name_(nullptr), slot_(-1)
  { path_[0] = '\0'; }
};

/// Low-level: dlopen `so_path`, resolve `ob_ext_table_plugin_get_api`, run the ABI
/// version check, and on success fill `out` (resident handle / vtable / format_name).
/// On any failure the (possibly open) handle is dlclose'd, `err_msg` gets a short
/// human-readable diagnostic, and `status_out` (if non-null) gets the precise
/// failure category. Returns:
///   OB_SUCCESS           — loaded, `out` populated;
///   OB_ERR_UNEXPECTED     — dlopen failed (DLOPEN_FAILED) or no format_name;
///   OB_ENTRY_NOT_EXIST    — dlsym(ob_ext_table_plugin_get_api) not found;
///   OB_VERSION_NOT_MATCH  — plugin rejected the ABI version.
int load_ext_plugin(const char *so_path, ObExtLoadedPlugin &out,
                     common::ObSqlString &err_msg,
                     ObExtPluginLoadStatus *status_out = nullptr);

/// Load ONE plugin by canonical name, the entry point invoked by the registry's
/// lazy load from `ext_plugin_config`.
///   - `path_override != nullptr`: dlopen that absolute path directly (skips
///     LD_LIBRARY_PATH search).
///   - otherwise: resolve SONAME `lib_ob_<lower(plugin_name)>.so` under
///     LD_LIBRARY_PATH (ObLdLibraryPathUtil) and dlopen it.
/// On failure nothing is left resident (broken handle dlclose'd), `err_msg`
/// describes the cause, and `status_out` (if non-null) gets the category.
/// Returns the same codes as load_ext_plugin, plus `OB_ENTRY_NOT_EXIST` when
/// the SONAME is not found on the search path (NOT_FOUND), or
/// `OB_INVALID_ARGUMENT` for an empty name.
int load_ext_plugin_by_name(const char *plugin_name,
                            const char *path_override,
                            ObExtLoadedPlugin &out,
                            common::ObSqlString &err_msg,
                            ObExtPluginLoadStatus *status_out = nullptr);

} // namespace share
} // namespace oceanbase

#endif // OB_EXT_PLUGIN_LOADER_H

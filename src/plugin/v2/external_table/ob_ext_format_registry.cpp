/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "plugin/v2/external_table/ob_ext_format_registry.h"

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_sql_string.h"
#include "lib/time/ob_time_utility.h"
#include "lib/utility/ob_ld_library_path_util.h"
#include "share/config/ob_server_config.h"
#include "lib/allocator/page_arena.h"  // ObArenaAllocator
#include "plugin/v2/external_table/ob_ext_json_internal.h"  // JSON parse + array/member helpers

#include <dlfcn.h>
#include <cstring>
#include <cctype>

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::internal;

namespace
{

// Lowercase `src` into `buf` (NUL-terminated, truncated to buf_size-1). Used so
// the canonical lowercased name from the config canonicalizes to one identity.
static void lowercase_cstr_(const ObString &src, char *buf, int64_t buf_size)
{
  int64_t n = std::min<int64_t>(src.length(), buf_size - 1);
  for (int64_t i = 0; i < n; i++) {
    buf[i] = static_cast<char>(tolower(static_cast<unsigned char>(src.ptr()[i])));
  }
  buf[n] = '\0';
}

static void copy_cstr_(char *dst, int64_t dst_size, const char *src)
{
  if (OB_ISNULL(dst) || dst_size <= 0) {
    return;
  }
  if (OB_ISNULL(src)) {
    dst[0] = '\0';
    return;
  }
  int64_t n = std::min<int64_t>(static_cast<int64_t>(strlen(src)), dst_size - 1);
  memcpy(dst, src, n);
  dst[n] = '\0';
}

// Build the intended soname string for a config entry, for the diagnostic row's
// lib_path column on the FAILURE path (where no resolved path exists):
// `path_cstr` non-null/non-empty => the absolute path override the loader tried;
// else the canonical soname lib_ob_<name>.so.
static void build_status_soname_(const char *name_lower, const char *path_cstr,
                                 char *buf, int64_t buf_size)
{
  if (OB_NOT_NULL(path_cstr) && path_cstr[0] != '\0') {
    copy_cstr_(buf, buf_size, path_cstr);
  } else {
    ObSqlString soname;
    if (OB_SUCCESS == soname.append("lib_ob_")
        && OB_SUCCESS == soname.append(name_lower)
        && OB_SUCCESS == soname.append(".so")) {
      copy_cstr_(buf, buf_size, soname.ptr());
    } else {
      if (buf_size > 0) { buf[0] = '\0'; }
    }
  }
}

// The plugin's self-reported version string for the diagnostic row
// (api->plugin_version()). The loader does not require plugin_version to be
// non-null (only format_name is validated), so guard both levels; returns
// nullptr when unavailable, which record_info_ stores as an empty string.
static const char *ext_plugin_version_of_(const ObExtTablePluginApi *api)
{
  const char *version = nullptr;
  if (OB_NOT_NULL(api) && OB_NOT_NULL(api->plugin_version)) {
    version = api->plugin_version();
  }
  return version;
}

} // namespace

// Lazy-load gate: only the first lookup reads `ext_plugin_config` and dlopens;
// every later lookup hits the once-flag and no-ops, so the config is declarative
// (restart-only). The first-run errno is saved in load_first_ret_ (written under
// the once-flag, then only read); later no-op calls return it (typically
// OB_SUCCESS — a deferred config change is not an error).
int ObExtFormatRegistry::ensure_loaded_()
{
  std::call_once(load_once_, &ObExtFormatRegistry::load_config_impl_, this);
  return load_first_ret_;
}

// Lazy-load impl (run exactly once per process — see ensure_loaded_): dlopen
// every plugin named in `ext_plugin_config` that is not yet resident. The config
// value is a JSON array of objects, e.g.
//   [{"name":"paimon"}, {"name":"odps","path":"/abs/lib_ob_odps.so"}]
// `path` is optional (absent/empty => resolve lib_ob_<name>.so via LD_LIBRARY_PATH).
// Using JSON (not a custom split) makes escaping authoritative: the JSON parser
// handles \" \\ \n etc. so a path with backslashes/quotes/unicode is parsed
// correctly with no hand-rolled unescaping. A per-entry failure is logged and
// does NOT abort the remaining entries; the first errno seen is stored in
// load_first_ret_ (informational only — ensure_loaded_'s callers ignore it and
// look up whatever did load). Invoked via std::call_once(&...::load_config_impl_).
void ObExtFormatRegistry::load_config_impl_()
{
  int ret = OB_SUCCESS;
  // `ext_plugin_config` is an ObConfigStringItem (DEF_STR) holding JSON text.
  // get_value_string() returns the configured value under the item lock; an
  // empty string => nothing to load (not an error — default is paimon, so this
  // branch is only reached if the operator deliberately cleared the config).
  const ObString json_text = ObServerConfig::get_instance().ext_plugin_config.get_value_string();
  if (json_text.empty()) {
    LOG_INFO("ext_plugin_config is empty, no external-table plugin loaded");
  } else {
    // Parse the JSON into a throwaway arena. The parsed name/path strings are
    // views into this arena, so we deep-copy each into stack buffers before they
    // go out of scope (load_one_under_lock_ only needs them during the call).
    common::ObArenaAllocator parse_arena("ExtPluginCfg");
    ObJsonNode *root = nullptr;
    const char *syntaxerr = nullptr;
    uint64_t err_offset = 0;
    if (OB_FAIL(ObJsonParser::parse_json_text(&parse_arena, json_text.ptr(),
                                               static_cast<uint64_t>(json_text.length()),
                                               syntaxerr, &err_offset, root))) {
      LOG_WARN("parse ext_plugin_config json failed", K(ret), KCSTRING(syntaxerr), K(err_offset));
    } else if (OB_ISNULL(root) || root->json_type() != ObJsonNodeType::J_ARRAY) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("ext_plugin_config json root is not an array", K(ret));
    } else {
      const uint64_t n = array_size(root);
      // No mutex: load_config_impl_ runs under load_once_ (ensure_loaded_), so
      // it is the single writer of plugins_[]/info_[] and is never concurrent
      // with readers (readers block in call_once until this returns).
      for (uint64_t i = 0; i < n; ++i) {
        const ObJsonNode *elem = array_at(root, i);
        if (OB_ISNULL(elem) || elem->json_type() != ObJsonNodeType::J_OBJECT) {
          LOG_WARN("ext_plugin_config entry is not an object, skipped", K(i));
          continue;
        }
        const ObString name = member_str(elem, "name");
        const ObString path = member_str(elem, "path");
        if (name.empty()) {
          LOG_WARN("ext_plugin_config entry has empty 'name', skipped", K(i));
          continue;
        }
        // Deep-copy name (lowercased) and path into stack buffers that outlive the
        // parse arena; load_one_under_lock_ only reads them during the call.
        char name_lower[64];
        if (name.length() >= static_cast<int64_t>(sizeof(name_lower))) {
          // Truncation would change the plugin identity; skip rather than risk a
          // mismatched load. (The loader name key is short by construction.)
          LOG_WARN("ext_plugin_config entry name too long, skipped", K(i), K(name.length()));
          continue;
        }
        lowercase_cstr_(name, name_lower, sizeof(name_lower));
        char path_buf[common::MAX_PATH_SIZE];
        const char *path_cstr = nullptr;
        if (!path.empty()) {
          int64_t plen = std::min<int64_t>(path.length(), static_cast<int64_t>(sizeof(path_buf) - 1));
          MEMCPY(path_buf, path.ptr(), static_cast<size_t>(plen));
          path_buf[plen] = '\0';
          if (plen < path.length()) {
            LOG_WARN("ext_plugin_config entry path truncated", K(i), K(path.length()), K(plen));
          }
          path_cstr = path_buf;
        }
        ObSqlString tmp_err;  // per-entry diagnostic; lazy load has no client to surface to.
        // The plugin's stable slot IS its position in the config array (i), so the
        // slot is determined solely by the config and is identical across every
        // server with the same config — a failed entry leaves its slot as a hole
        // and never shifts later entries down. Cap at MAX_PLUGINS: extra config
        // entries are dropped (logged), since a slot must fit in [0, MAX_PLUGINS).
        if (i >= static_cast<uint64_t>(MAX_PLUGINS)) {
          LOG_WARN("ext_plugin_config has more entries than MAX_PLUGINS, truncated",
                   K(i), K(MAX_PLUGINS));
          break;
        }
        const int one_ret = load_one_under_lock_(name_lower, path_cstr,
                                                 static_cast<ObPluginSlot>(i), tmp_err);
        if (OB_SUCCESS != one_ret && OB_SUCCESS == ret) {
          ret = one_ret;  // record first failure, but keep loading the rest.
        }
      }  // end for
    }
  }
  load_first_ret_ = ret;  // informational only — callers ignore it and look up whatever loaded.
}

// Load (or recognize as already resident) one plugin. Driven only by
// load_config_impl_ (the lazy-load path), which runs under `load_once_` — so this
// is single-threaded by construction and needs no mutex. `name_lower` is the
// canonical lowercased name; `path_cstr` an optional absolute-path override
// (nullptr/empty => resolve lib_ob_<name>.so). `err_msg` is caller-owned and gets
// a short diagnostic on the non-success paths (logged by load_config_impl_).
// Returns OB_SUCCESS if resident (newly loaded OR already loaded), else the
// loader errno.
int ObExtFormatRegistry::load_one_under_lock_(const char *name_lower, const char *path_cstr,
                                               ObPluginSlot config_slot, ObSqlString &err_msg)
{
  int ret = OB_SUCCESS;
  // The intended soname/path for the diagnostic row's lib_path column. Used as-is
  // on the failure path; resident rows overwrite it with the absolute path the
  // loader actually dlopen'd (ObExtLoadedPlugin::path_).
  char lib_path_buf[common::MAX_PATH_SIZE];
  build_status_soname_(name_lower, path_cstr, lib_path_buf, sizeof(lib_path_buf));

  // The plugin's stable slot IS config_slot (its position in ext_plugin_config).
  // Scan plugins_[0..MAX_PLUGINS) skipping unoccupied slots (api_==nullptr — holes
  // left by failed config entries) for idempotency/dedup checks.
  bool already_resident = false;
  const ObExtLoadedPlugin *resident_entry = nullptr;
  for (int64_t i = 0; i < MAX_PLUGINS; i++) {
    const ObExtLoadedPlugin &cur = plugins_[i];
    const char *fname = cur.format_name_;
    if (OB_ISNULL(cur.api_) || OB_ISNULL(fname)) {
      continue;  // hole: a failed config entry left this slot unoccupied
    } else if (0 == ObString::make_string(fname).case_compare(name_lower)) {
      already_resident = true;
      resident_entry = &cur;
      LOG_INFO("ext plugin already loaded, skip", KCSTRING(name_lower), "format", fname,
               "resident_slot", cur.slot_);
      break;
    }
  }

  if (already_resident) {
    // no-op; ret stays OB_SUCCESS. Still record a row so the virtual table shows
    // the config declared this name (marked ALREADY_LOADED).
    record_info_(name_lower, resident_entry->path_, ext_plugin_version_of_(resident_entry->api_),
                 EXT_PLUGIN_ALREADY_LOADED, "plugin already loaded");
  } else {
    ObExtLoadedPlugin plugin;
    ObSqlString loader_err;
    ObExtPluginLoadStatus status = EXT_PLUGIN_NOT_ATTEMPTED;
    if (OB_FAIL(load_ext_plugin_by_name(name_lower, path_cstr, plugin, loader_err, &status))) {
      if (!loader_err.empty()) {
        err_msg.append(loader_err.ptr());
      }
      // Failure: record the row (status + err) so the virtual table surfaces
      // "which plugin failed + why", not just an absent row. This config entry
      // stays a hole until restart, and later entries keep their own (higher)
      // indices — they never shift down into config_slot.
      record_info_(name_lower, lib_path_buf, nullptr, status, loader_err.ptr());
      LOG_WARN("load ext plugin failed", K(ret), KCSTRING(name_lower), K(config_slot),
               "status", static_cast<int>(status), "err", loader_err.ptr());
    } else {
      // Dedup by format_name(): a second .so claiming the same format is rejected
      // (its handle is dropped) — the first one wins. Scan holes too (see above).
      bool dup = false;
      for (int64_t i = 0; !dup && i < MAX_PLUGINS; i++) {
        const ObExtLoadedPlugin &cur = plugins_[i];
        const char *fname = cur.format_name_;
        if (OB_ISNULL(cur.api_) || OB_ISNULL(fname)) {
        } else if (OB_NOT_NULL(plugin.format_name_) && 0 == strcasecmp(fname, plugin.format_name_)) {
          dup = true;
        }
      }
      if (dup) {
        char format_buf[64];
        copy_cstr_(format_buf, sizeof(format_buf), plugin.format_name_);
        if (OB_NOT_NULL(plugin.dl_handle_)) {
          dlclose(plugin.dl_handle_);
        }
        record_info_(name_lower, plugin.path_, ext_plugin_version_of_(plugin.api_),
                     EXT_PLUGIN_ALREADY_LOADED, "plugin already loaded");
        LOG_INFO("skip duplicate ext plugin format", KCSTRING(name_lower), "format", format_buf,
                 K(config_slot));
      } else {
        plugin.slot_ = config_slot;
        plugins_[config_slot] = plugin;
        // No counter to bump: the plugin's slot IS config_slot (its config
        // position), not a runtime ordinal. The read path scans
        // [0, MAX_PLUGINS) skipping holes (api_==nullptr) under call_once's
        // happens-before, so no atomic publish is needed here.
        err_msg.reset();
        record_info_(name_lower, plugin.path_, ext_plugin_version_of_(plugin.api_),
                     EXT_PLUGIN_SUCCESS, "");
        LOG_INFO("ext plugin loaded", KCSTRING(name_lower), "format", plugin.format_name_,
                 "slot", plugin.slot_);
      }
    }
  }
  return ret;
}

const ObExtTablePluginApi *ObExtFormatRegistry::get_plugin_by_slot(ObPluginSlot slot)
{
  // Lazy load from ext_plugin_config on the first lookup; later lookups hit the
  // once-flag and skip. The plugin's slot IS its array index in plugins_[] (==
  // its position in ext_plugin_config), so this is a direct O(1) index, not a
  // scan. A slot that is out of range or whose entry never loaded (a hole left
  // by a failed config entry — api_==nullptr) returns nullptr and the caller
  // surfaces "plugin not loaded".
  (void) ensure_loaded_();
  const ObExtTablePluginApi *api = nullptr;
  if (slot >= 0 && slot < MAX_PLUGINS) {
    api = plugins_[slot].api_;  // nullptr if this slot is a hole
  }
  return api;
}

ObPluginSlot ObExtFormatRegistry::get_slot_by_api(const ObExtTablePluginApi *api)
{
  // Reverse lookup used at the deduce boundary: recognize_format identifies the
  // recognizing plugin's api pointer; this resolves it to the stable slot that
  // then flows through plan/CG/iter as an int16. O(n) is fine — called once per
  // deduce. Scans [0, MAX_PLUGINS) skipping holes. Returns -1 if the api is not
  // a resident plugin (should not happen for an api returned by recognize_format).
  ObPluginSlot slot = -1;
  if (OB_NOT_NULL(api)) {
    for (int64_t i = 0; i < MAX_PLUGINS; i++) {
      const ObExtLoadedPlugin &cur = plugins_[i];
      if (OB_ISNULL(cur.api_)) {
      } else if (cur.api_ == api) {
        slot = cur.slot_;
        break;
      }
    }
  }
  return slot;
}

int ObExtFormatRegistry::recognize_format(const ObString &table_uri,
                                          const ObString &recognize_json,
                                          const ObExtTableHostApi *host,
                                          const ObExtTablePluginApi *&out_api)
{
  // Lazy load from ext_plugin_config on the first lookup; later lookups hit the
  // once-flag and skip. Scans [0, MAX_PLUGINS) skipping holes (failed entries).
  (void) ensure_loaded_();
  int ret = OB_ENTRY_NOT_EXIST;
  out_api = nullptr;
  for (int64_t i = 0; i < MAX_PLUGINS; i++) {
    const ObExtTablePluginApi *api = plugins_[i].api_;
    if (OB_ISNULL(api) || OB_ISNULL(api->recognize_table)) {
      continue;
    }
    const char *uri = table_uri.empty() ? "" : table_uri.ptr();
    const char *json = recognize_json.empty() ? "" : recognize_json.ptr();
    // recognize_table returns OB_SUCCESS if the plugin owns this table, any
    // other errno => "not mine"; the plugin logs diagnostics via host->log.
    const int pr = api->recognize_table(uri, json, host);
    if (OB_SUCCESS == pr) {
      out_api = api;
      ret = OB_SUCCESS;
      break;
    }
  }
  return ret;
}

int ObExtFormatRegistry::get_plugin_statuses(ObIArray<ObExtPluginStatus> &out)
{
  // Trigger the one-shot lazy load so the table reflects whatever ext_plugin_config
  // declared, then snapshot the diagnostic rows. No mutex: after ensure_loaded_()
  // returns, call_once guarantees load_config_impl_ has finished, so info_count_
  // is at its terminal value and info_[0..info_count_) is stable — readers and
  // the single writer cannot overlap (readers were blocked in call_once until the
  // writer returned). The snapshot includes both loaded and failed entries (a
  // failure carries status + error_msg) — a failure is recorded once and not
  // retried until restart.
  int ret = OB_SUCCESS;
  (void) ensure_loaded_();
  const int64_t cnt = info_count_;
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
    if (OB_FAIL(out.push_back(info_[i]))) {
      LOG_WARN("push back plugin status failed", K(ret), K(i));
    }
  }
  return ret;
}

int ObExtFormatRegistry::record_info_(const char *name_lower, const char *lib_path,
                                       const char *plugin_version,
                                       ObExtPluginLoadStatus status,
                                       const char *err_msg)
{
  int ret = OB_SUCCESS;
  // Single-threaded (call_once body); info_count_ only grows here. Drop the row
  // (and log the gap) if the table is full — diagnostic-only, must not fail the load.
  if (info_count_ < MAX_PLUGINS) {
    ObExtPluginStatus &row = info_[info_count_++];
    row = ObExtPluginStatus();
    copy_cstr_(row.plugin_name_, sizeof(row.plugin_name_), name_lower);
    copy_cstr_(row.lib_path_, sizeof(row.lib_path_), lib_path);
    copy_cstr_(row.plugin_version_, sizeof(row.plugin_version_), plugin_version);
    row.status_ = status;
    copy_cstr_(row.error_msg_, sizeof(row.error_msg_), OB_ISNULL(err_msg) ? "" : err_msg);
    row.load_time_us_ = ObTimeUtility::current_time();
  } else {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("ext plugin info table full, dropping diagnostic row", K(ret), KCSTRING(name_lower));
  }
  return ret;
}

} // namespace share
} // namespace oceanbase

#undef USING_LOG_PREFIX

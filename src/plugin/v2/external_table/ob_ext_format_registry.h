/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_ext_format_registry.h
/// \brief Process-wide registry of loaded external-table format plugins.
///
/// Plugins are loaded LAZILY from the `ext_plugin_config` cluster config: the
/// first lookup that needs the registry (get_plugin_by_slot / recognize_format)
/// runs a std::call_once that reads the config (a JSON array of
/// {"name":"...","path":"..."} objects) and dlopens every named plugin. There is
/// no `LOAD PLUGIN` command, no startup scan, and no per-attempt status table —
/// the config is the single declarative source of which plugins a process loads.
///
/// The config is read at most once per process: a later
/// `ALTER SYSTEM SET ext_plugin_config` only changes the persisted value and
/// takes effect after the next restart; the already-resident set is unaffected.
/// A per-plugin load failure (dlopen/dlsym/ABI mismatch/not found) is logged and
/// skipped — it does not abort the remaining entries and does not fail the
/// lookup (the missing plugin simply returns "not loaded").
///
/// A successfully version-checked handle is RESIDENT for the process lifetime
/// (NEVER dlclose'd — its function pointers and plugin-initialized global state
/// are in use; dlclose would be unsafe). A failed load leaves nothing resident:
/// the broken handle is dlclose'd by the loader.
///
/// Memory model: the load path (`load_config_impl_`) runs exactly once per
/// process under `std::call_once` (see ensure_loaded_); call_once's
/// synchronizes-with separates that single write phase from all read phases, so
/// no mutex is needed. The read path (get_plugin_by_slot / recognize_format) is
/// lock-free: readers run after call_once returns, so they see a consistent
/// snapshot of plugins_[]; a hole (api_==nullptr) is a slot whose config entry
/// failed to load, and skipped.
///
/// Plugin identity is carried as a STABLE SLOT (int16, [0, MAX_PLUGINS)) through
/// the runtime flow (plan struct / CG / iter / DAS), NOT as the format-name
/// string. The slot is the array index the plugin occupies in plugins_[] and is
/// assigned at load time as the entry's position in `ext_plugin_config` (the
/// JSON array order) — it is NOT a runtime counter, so it is identical across
/// every server with the same config regardless of per-entry load success. A
/// failed config entry leaves its slot as a hole (plugins_[i] stays
/// api_==nullptr); a later entry still uses its own index, never shifting down.
/// The format name survives only inside this registry (config parsing +
/// recognize_format's per-plugin probe); the runtime flow carries only the
/// slot. Native formats such as ICEBERG and HIVE continue to use their
/// dedicated code paths and do not touch the slot axis.

#ifndef OB_EXT_FORMAT_REGISTRY_H
#define OB_EXT_FORMAT_REGISTRY_H

#include <atomic>
#include <mutex>

#include "lib/ob_define.h"
#include "lib/container/ob_iarray.h"
#include "lib/string/ob_string.h"
#include "lib/string/ob_sql_string.h"
#include "plugin/v2/include/ob_external_table_plugin.h"
#include "plugin/v2/loader/ob_ext_plugin_loader.h"
#include "share/catalog/ob_catalog_properties.h"

namespace oceanbase
{
namespace share
{

/// One external-table plugin row of `__all_virtual_plugin_info` — a read-only
/// snapshot of one config-declared plugin's load outcome (success OR failure),
/// recorded inside the call_once body of load_config_impl_ and copied out by the
/// virtual table via get_plugin_statuses(). The load-status enum
/// (ObExtPluginLoadStatus) is defined in ob_ext_plugin_loader.h.
struct ObExtPluginStatus
{
  char plugin_name_[64];        // canonical lowercased format name
  // resident rows: the absolute path actually dlopen'd (the config `path`
  // override, or lib_ob_<name>.so resolved under LD_LIBRARY_PATH);
  // failed rows: the intended soname/path the loader tried.
  char lib_path_[common::MAX_PATH_SIZE];
  char plugin_version_[64];     // api->plugin_version(); empty for failed loads
  char error_msg_[512];         // load failure reason; empty for resident rows
  ObExtPluginLoadStatus status_;
  int64_t load_time_us_;        // ObTimeUtility::current_time() when the load concluded
  ObExtPluginStatus()
      : status_(EXT_PLUGIN_NOT_ATTEMPTED), load_time_us_(0)
  { plugin_name_[0] = lib_path_[0] = plugin_version_[0] = error_msg_[0] = '\0'; }
  // Required by ob_print_utils.h (ObIArray<ObExtPluginStatus>::to_string
  // instantiates when the array is logged). status_ printed as int because the
  // enum has no to_string overload.
  TO_STRING_KV(K_(plugin_name), K_(lib_path), K_(plugin_version),
               "status", static_cast<int>(status_),
               K_(load_time_us), K_(error_msg));
};

class ObExtFormatRegistry
{
public:
  static ObExtFormatRegistry &get_instance()
  {
    static ObExtFormatRegistry instance;
    return instance;
  }

  /// Look up a plugin by its stable slot. Triggers the one-shot lazy load from
  /// `ext_plugin_config` on the first call. Returns nullptr if the slot is out
  /// of range or no plugin occupies it — the caller surfaces "plugin not loaded".
  /// This is the runtime lookup used by the iter path (plan carries slot, not name).
  const ObExtTablePluginApi *get_plugin_by_slot(ObPluginSlot slot);

  /// Reverse lookup: the slot occupied by a given resident plugin vtable pointer.
  /// O(n) scan; called only at the deduce boundary after recognize_format
  /// identifies the recognizing plugin's api. Returns -1 if not found.
  ObPluginSlot get_slot_by_api(const ObExtTablePluginApi *api);

  /// Iterate loaded plugins with recognize_table; returns the first plugin that
  /// returns OB_SUCCESS. Triggers the one-shot lazy load on the first call.
  /// `host` may be nullptr (HMS-only JSON probes). Returns OB_ENTRY_NOT_EXIST
  /// when no loaded plugin recognizes the table. The recognizing plugin's api
  /// pointer is returned in `out_api`; the caller resolves it to a slot via
  /// get_slot_by_api(). The ObExtTablePluginApi struct itself is plugin-owned
  /// and carries no OB-side slot field.
  int recognize_format(const common::ObString &table_uri,
                       const common::ObString &recognize_json,
                       const ObExtTableHostApi *host,
                       const ObExtTablePluginApi *&out_api);

  /// Copy out a snapshot of every resident (loaded) plugin, for the
  /// external-table plugin rows of `__all_virtual_plugin_info`. Lock-free: after
  /// ensure_loaded_() returns, call_once guarantees the single writer has
  /// finished, so info_count_ is terminal and info_[0..info_count_) is stable.
  /// Both loaded and failed config entries are reported (a failure carries its
  /// status + error_msg). Triggers the one-shot lazy load on the first call (so
  /// the table reflects whatever the config declared).
  int get_plugin_statuses(common::ObIArray<ObExtPluginStatus> &out);

private:
  ObExtFormatRegistry() = default;
  ~ObExtFormatRegistry() = default;
  DISALLOW_COPY_AND_ASSIGN(ObExtFormatRegistry);

  // Ensure the registry has been populated from `ext_plugin_config`. Runs the
  // config read + dlopens exactly once per process via `load_once_`; later
  // calls are no-ops. Best-effort: per-plugin failures are logged and skipped
  // inside load_config_impl_(), so the return value (the first errno seen) is
  // informational only — callers ignore it and proceed to look up whatever did
  // load. Idempotent and thread-safe (std::call_once).
  int ensure_loaded_();

  // The once-gated impl behind ensure_loaded_: parses `ext_plugin_config` and
  // dlopens each named plugin not yet resident. Stores the first errno seen in
  // load_first_ret_ (informational). Invoked by ensure_loaded_ via
  // std::call_once(&ObExtFormatRegistry::load_config_impl_, this).
  void load_config_impl_();

  // Load (or recognize as already resident) one plugin. Called only from
  // load_config_impl_, hence under `load_once_` — single-threaded, no mutex.
  // `name_lower` canonical lowercased name; `path_cstr` optional absolute-path
  // override (nullptr/empty => resolve lib_ob_<name>.so); `config_slot` the
  // position of this entry in the ext_plugin_config JSON array ([0, MAX_PLUGINS))
  // — the plugin's stable slot IS this index (NOT a runtime counter), so the slot
  // is determined solely by the config and is identical across every server with
  // the same config, regardless of per-entry load success. A failed entry leaves
  // its slot as a hole (plugins_[config_slot] stays api_==nullptr). `err_msg`
  // caller-owned diagnostic (logged only — there is no client to surface to in the
  // lazy-load path). Returns OB_SUCCESS if resident, else the loader errno.
  int load_one_under_lock_(const char *name_lower, const char *path_cstr,
                           ObPluginSlot config_slot, common::ObSqlString &err_msg);

  // Append one diagnostic row (info_[info_count_++]) for one config-declared
  // entry's load outcome — success OR failure — so the virtual table shows what
  // `ext_plugin_config` declared, not just what loaded. A failure is recorded
  // once (status + err) and NOT retried: lazy load runs once per process under
  // call_once, so a failed entry stays failed until restart. `lib_path` is the
  // absolute path actually dlopen'd for resident rows, the intended soname/path
  // for failed rows. `plugin_version` is the plugin's self-reported version
  // (api->plugin_version()) for resident rows, nullptr for failed loads. Returns
  // OB_SIZE_OVERFLOW (diagnostic-only) if the table is full and the row is
  // dropped — callers ignore the return. Single-threaded; no mutex.
  int record_info_(const char *name_lower, const char *lib_path,
                    const char *plugin_version, ObExtPluginLoadStatus status,
                    const char *err_msg);

  static constexpr int64_t MAX_PLUGINS = LAKE_PLUGIN_PLACEHOLDER_COUNT;

  // Gate for ensure_loaded_: lets only the first lookup read `ext_plugin_config`
  // and dlopen; every later lookup hits the once-flag and no-ops. The config is
  // declarative (restart-only). call_once's synchronizes-with already separates
  // the single write phase (load_config_impl_) from all read phases, so no mutex
  // is needed on top of it.
  std::once_flag load_once_;
  // First-run errno of load_config_impl_, written under load_once_ and only
  // read afterwards; returned by every ensure_loaded_ call (informational).
  int load_first_ret_{OB_SUCCESS};
  ObExtLoadedPlugin plugins_[MAX_PLUGINS];
  // One diagnostic row per config-declared entry (success OR failure): the
  // soname the loader used, the outcome (status/loaded), the ABI version, and
  // the load timestamp. Written by record_info_() in every branch of
  // load_one_under_lock_ (run once per config entry under call_once), so the
  // virtual table shows what `ext_plugin_config` declared — not just what
  // loaded. Indexed independently of plugins_[] (failures don't occupy a
  // resident slot). info_count_ is terminal after call_once completes.
  ObExtPluginStatus info_[MAX_PLUGINS];
  int64_t info_count_{0};
};

} // namespace share
} // namespace oceanbase

#endif // OB_EXT_FORMAT_REGISTRY_H

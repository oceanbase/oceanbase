/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

/// \file ob_external_table_plugin.h
/// \brief Format-agnostic external-table plugin contract (pure C, JSON control plane).
///
/// OceanBase defines this contract; every external-table format implements it
/// as a standalone .so. OB has ONE generic
/// external-table read path that drives this vtable; adding a new format =
/// dropping in a .so, with zero per-format code in OB.
///
/// ## Two planes
/// - **Data plane = Arrow C Data.** Row batches flow through `reader_next_batch`
///   as ArrowArray/ArrowSchema — binary, zero-copy, high throughput. NEVER JSON.
/// - **Control plane = JSON text.** Schema, predicate, statistics and the scan-task
///   descriptor are passed as UTF-8 JSON strings (`const char*` + length).
///   The ABI surface is thus just the vtable function signatures + `char*`
///   buffers + the host API + the opaque reader state refs. Evolving a control
///   structure (adding a column attribute, a predicate operator, a stat, a scan-task
///   field) is a JSON schema change, NOT an ABI break — old plugins ignore
///   unknown keys, new OB tolerates missing keys. This is what lets independently
///   compiled/shipped .so's coexist across versions.
///
/// ## Memory ownership (STRICT)
/// Plugin OUTPUT (schema / scan-tasks / stats JSON) is released **by the plugin**,
/// never by OB. The plugin may have built that buffer from a static constant, from
/// `host->mem.mem_alloc`, or from its own allocator — so only the plugin knows how to
/// release it. Each output therefore has a paired destroy callback
/// (`schema_destroy` / `tasks_destroy` / `stats_destroy`); OB calls it once it has
/// copied/parsed the buffer, and the plugin decides inside: static -> no-op,
/// host-allocated -> `host->mem.mem_free`, own-allocator -> own free. The `host` arg is
/// passed to each destroy so a host-allocating plugin can still reach
/// `host->mem.mem_free`.
///   - OB INPUT to the plugin (options / predicate / projection JSON): OB owns the
///     buffer; it is valid only for the duration of the call. The plugin parses it
///     into its own native form and MUST NOT retain the pointer past the call.
///   - The only non-JSON handles crossing the boundary are the opaque,
///     plugin-owned reader state refs — released together via `reader_close`.
///   - `host->mem.mem_alloc` / `host->mem.mem_free` remain in the host API for the plugin's
///     OWN internal buffers (e.g. its arrow memory pool); they are NOT how OB frees
///     a plugin output.
///
/// ## JSON value encoding (avoid precision loss)
///   bool                -> JSON bool
///   int <= int32        -> JSON number
///   int64 / bigint      -> JSON string (avoids the 2^53 double limit)
///   float / double      -> JSON number
///   decimal             -> JSON string  ("1.23")
///   date                -> JSON number  (days since epoch)
///   datetime/timestamp/time -> JSON string ("2024-01-01 00:00:00")
///   binary / varbinary  -> JSON string, base64
///   string/varchar/char -> JSON string
///   null / unknown      -> JSON null
///
/// ## Control-plane JSON schemas (the protocol)
///   options (OB->plugin):
///     {"location":"..","access_info":"..", "ext_options":{<format-private knobs>}}
///     ext_options is an opaque nested object OB passes through verbatim (it does
///     not know any inner key); the plugin unwraps + validates it. Absent = default.
///   table schema (plugin->OB), `ext_type` is the enum NAME below:
///     {"columns":[{"name":"..","field_id":N,"ext_type":"BIGINT",
///                  "precision":P,"scale":S,"length":L,"nullable":true}]}
///   ARRAY columns carry their element as a single child (recursive, same shape):
///     {"columns":[{"name":"tags","field_id":N,"ext_type":"ARRAY","nullable":true,
///                  "children":[{"name":"element","field_id":M,"ext_type":"STRING",
///                               "nullable":true}]}]}
///   (nested ARRAY of ARRAY is just a child whose ext_type is ARRAY.) MAP
///   columns carry key/value as two children (recursive, same shape):
///     {"columns":[{"name":"attrs","field_id":N,"ext_type":"MAP","nullable":true,
///                  "children":[{"name":"key","field_id":M,"ext_type":"STRING",
///                               "nullable":false},
///                              {"name":"value","field_id":K,"ext_type":"INT",
///                               "nullable":true}]}]}
///   Partition columns are carried as a top-level name list (option B: mark, do
///   NOT build OB partitions — partition pruning is delegated to the plugin/SDK
///   via partition_filter_json at plan_create):
///     {"columns":[...],"partition_keys":["ds","hr"]}
///   (Field names below are the OB_EXT_K_* constants — OB and every plugin MUST
///   use those constants, not bare string literals, so a typo is a compile error
///   at the single definition. Unknown keys are NOT silently ignored: OB logs a
///   loud WARN and required keys missing/type-mismatched cause a hard error.)
///   predicate / partition_filter (OB->plugin), expression tree:
///     {"kind":"cmp","op":"eq",
///      "children":[{"kind":"col","col_idx":N,"name":".."},
///                  {"kind":"lit","value":".."}]}
///     kinds: lit/col/cmp/in/not_in/is_null/is_not_null/and/or/not.
///       col: "col_idx" = the column's field_id (informational); "name" is the
///            column name the plugin resolves to its schema field index/type.
///       lit: "value" is the literal as a string; the plugin converts it to the
///            field type of the sibling col (resolved by name).
///       cmp: binary, children=[col, lit], op in eq/ne/lt/le/gt/ge.
///       in/not_in: children=[col, lit, lit, ...].
///       is_null/is_not_null: children=[col].
///       and/or: children=[pred, ...]; not: children=[pred].
///     ops:   eq/ne/lt/le/gt/ge. NULL or "" root = no pushdown.
///   For partition_filter the plugin flattens eq/AND conjuncts into
///   SetPartitionFilter's vector<map<string,string>> (OR->vector, AND->map,
///   eq col=lit -> entry keyed by col name); non-eq partition predicates are
///   demoted to the row predicate (the plugin logs and skips them for
///   SetPartitionFilter).
///   read_projection (OB->plugin):
///     {"field_ids":[..]}   (null / absent -> read all columns)
///   scan tasks (plugin->OB) — two-part: a generic part OB understands (for
///   scheduling / runtime filter / finer parallelism) + a format-private
///   `payload_b64` (opaque bytes the plugin serialized, base64; OB never parses).
///   "Scan task" (not "split") is the neutral term: every plugin projects its
///   format-specific work item onto this generic shape:
///     {"tasks":[{"row_count":N,"byte_size":N,
///                "files":[{"path":"..","size":N}],
///                "min_max":{"<col_idx>":["lo","hi"]},
///                "splittable":true,
///                "payload_b64":".."}]}
///     OB today reads only row_count / byte_size; the rest is reserved for OB to
///     learn to exploit incrementally without a protocol change.
///   statistics (plugin->OB):
///     {"table":{"row_count":N,"byte_size":N},
///      "columns":[{"col_idx":N,"row_count":N,"null_count":N,"ndv":N,
///                  "min":"..","max":".."}]}
///
/// ## Error model
/// Every error-returning function RETURNS an OB errno directly (0 = OB_EXT_SUCCESS,
/// negative = error), using the contract errnos defined in this header (the
/// OB_EXT_* enum below — plugins compile without OB's ob_errno.h). On error the
/// plugin MUST log a diagnostic via `host->log(...)` (the plugin's `OBEXT_LOG_*`
/// macros, which capture `__FILE__`/`__LINE__`/`__func__` so OB's observer.log
/// shows the plugin-side stack) BEFORE returning the errno. There is NO error
/// object crossing the boundary — no `ObExtError`, no `error_message`/
/// `error_destroy` slots. OB consumes only the int return; the message lives in
/// the log line, with full plugin source location.
///   - A succeeding call returns OB_EXT_SUCCESS and logs nothing.
///   - The errno values the plugin returns are OB errnos verbatim (e.g.
///     OB_EXT_NOT_SUPPORTED == OB_NOT_SUPPORTED == -4007); OB assigns the return
///     directly to `ret`, no translation layer.
///
/// ## Optional slots (degrade gracefully; OB feature-detects for NULL)
///   init/deinit == NULL        -> no process-wide setup.
///   fetch_statistics == NULL   -> no stats; OB uses default cardinality.
///   schema_destroy/tasks_destroy/stats_destroy == NULL -> only safe if the
///     plugin guarantees the matching output is always a static constant (no free
///     needed); OB's helpers null-check the slot. A plugin that ever allocates an
///     output buffer MUST implement the matching destroy or it leaks.
///   predicate/partition_filter/read_projection == NULL -> no pushdown / read all.
///   recognize_table == NULL -> OB uses built-in fallback rules for that format.
/// A plugin implementing only {load_schema, plan_create, reader_create,
/// reader_open_scan, reader_open_task, reader_next_batch, reader_close_task,
/// reader_close_scan, reader_close} is fully functional.

#ifndef OB_EXTERNAL_TABLE_PLUGIN_H
#define OB_EXTERNAL_TABLE_PLUGIN_H

#include <stdint.h>

// Arrow C Data Interface: the official arrow/c/abi.h (on the include path for OB
// and every plugin — all external-table formats emit Arrow batches). Defines
// ArrowSchema / ArrowArray. No redefinition here.
#include <arrow/c/abi.h>

#ifdef __cplusplus
extern "C" {
#endif

// Bumped on incompatible changes to the vtable layout / entry contract. With the
// control plane now JSON, ordinary schema/predicate/stat/split evolution does NOT
// require a bump. The plugin returns NULL for a mismatched version.
#define OB_EXT_TABLE_PLUGIN_ABI_VERSION 1

// =============================================================================
// The PROTOCOL plane lives in ob_external_table_protocol.h (included by bare
// name — the two headers always sit in the SAME directory in every copy):
//   - contract errnos (OB_EXT_SUCCESS, ...) — return-value vocabulary, and
//   - control-plane JSON field names (OB_EXT_K_*).
// Neither affects binary compatibility: adding an errno or a JSON key never
// changes the vtable layout / entry contract. Included here so existing users
// of this header see no change.
// =============================================================================
#include "ob_external_table_protocol.h"

// =============================================================================
// Protocol type vocabulary — the "协议化类型". The plugin decides a column's
// protocol type; OB maps it to ObObjType. In the control-plane JSON this appears
// as the enum's NAME string (e.g. "BIGINT"), not the integer — the name<->enum
// mapping is shared by OB and every plugin (OB's `ext_type_from_name` in
// ob_ext_json_protocol.h). The enum values remain stable because OB's
// ob_ext_obj_type -> ObObjType mapper depends on them. It preserves the
// distinctions Arrow loses (varchar vs string vs char, decimal precision/scale,
// datetime vs timestamp, nested array/map).
// =============================================================================

typedef enum {
  OB_EXT_TYPE_NULL      = 0,
  OB_EXT_TYPE_BOOL      = 1,
  OB_EXT_TYPE_TINYINT   = 2,
  OB_EXT_TYPE_SMALLINT  = 3,
  OB_EXT_TYPE_INT       = 4,
  OB_EXT_TYPE_BIGINT    = 5,
  OB_EXT_TYPE_FLOAT     = 6,
  OB_EXT_TYPE_DOUBLE    = 7,
  OB_EXT_TYPE_DECIMAL   = 8,   // precision/scale in the column desc
  OB_EXT_TYPE_STRING    = 9,   // OB string (unbounded)
  OB_EXT_TYPE_VARCHAR   = 10,  // OB varchar, length in the column desc
  OB_EXT_TYPE_CHAR      = 11,  // OB char, length in the column desc
  OB_EXT_TYPE_BINARY    = 12,
  OB_EXT_TYPE_VARBINARY = 13,
  OB_EXT_TYPE_DATE      = 14,
  OB_EXT_TYPE_DATETIME  = 15,
  OB_EXT_TYPE_TIMESTAMP = 16,
  OB_EXT_TYPE_TIME      = 17,
  OB_EXT_TYPE_ARRAY     = 18,  // children[0] = element type
  OB_EXT_TYPE_MAP       = 19,  // children[0] = key, children[1] = value
  OB_EXT_TYPE_UNKNOWN   = 127,
} ob_ext_obj_type;

// Canonical NAME <-> enum mapping used in the control-plane JSON `ext_type` /
// literal `type` fields. Both sides share these exact strings. Defined inline so
// the contract header alone is the single source of truth (no .c needed).
//   "NULL","BOOL","TINYINT","SMALLINT","INT","BIGINT","FLOAT","DOUBLE","DECIMAL",
//   "STRING","VARCHAR","CHAR","BINARY","VARBINARY","DATE","DATETIME","TIMESTAMP",
//   "TIME","ARRAY","MAP","UNKNOWN"

// =============================================================================
// Opaque handles (plugin-internal).
// =============================================================================

typedef struct ObExtTableReaderWorkerState ObExtTableReaderWorkerState;
typedef struct ObExtTableReaderScanState ObExtTableReaderScanState;
typedef struct ObExtTableReaderTaskState ObExtTableReaderTaskState;

typedef ObExtTableReaderWorkerState *ObExtTableReaderWorkerStateRef;
typedef ObExtTableReaderScanState *ObExtTableReaderScanStateRef;
typedef ObExtTableReaderTaskState *ObExtTableReaderTaskStateRef;

// Outer-scope states are read-only inputs to inner-scope open calls: the call
// may only mutate the state object it opens (open_scan -> scan, open_task -> task).
typedef const ObExtTableReaderWorkerState *ObExtTableReaderWorkerStateConstRef;
typedef const ObExtTableReaderScanState *ObExtTableReaderScanStateConstRef;

// =============================================================================
// The HOST API (OB -> plugin: memory / executor / io / log callbacks) lives in
// ob_ext_host_api.h (included by bare name — the headers always sit in the SAME
// directory in every copy).
// =============================================================================
#include "ob_ext_host_api.h"

// =============================================================================
// Plugin vtable. One entry symbol (ob_ext_table_plugin_get_api) returns it; OB
// resolves that single symbol via dlsym and calls the rest through the table.
// All control-plane data is JSON text (see the protocol section above). Optional
// slots (init, deinit, fetch_statistics) may be NULL.
// =============================================================================

struct ObExtTablePluginApi {
  // ---- identity ----
  // plugin_name  : human-readable plugin name.
  // format_name  : canonical format string OB registers dynamically in its format
  //                tables.
  const char* (*plugin_name)(void);
  const char* (*plugin_version)(void);
  const char* (*format_name)(void);
  int  (*init)(void);   // process-wide, optional (may be NULL)
  void (*deinit)(void); // optional (may be NULL)

  // ---- format recognition (catalog) ----
  // This ABI has not crossed a release boundary yet; bump ABI_VERSION only on
  // incompatible vtable layout changes.
  // Decide whether this plugin owns the table described by `recognize_json`
  // (see OB_EXT_K_CATALOG_TYPE / OB_EXT_K_DIRS / OB_EXT_K_OUTPUT_FORMAT).
  // `table_uri` is the table root when known; may be "" for HMS-only probes.
  // `host` may be NULL when the plugin only needs the JSON.
  // Returns OB_SUCCESS if recognized; any other errno => "not mine".
  int  (*recognize_table)(const char *table_uri, const char *recognize_json,
                          const ObExtTableHostApi *host);

  // ---- schema (catalog, cached per table) ----
  // Plugin builds the table-schema JSON, returns the buffer in *out_schema_json
  // (+ length in *out_len). OB parses it and then releases the buffer via
  // schema_destroy below. The plugin owns the release (see "Memory ownership"):
  // it may have produced the buffer from a static constant, host->mem.mem_alloc, or
  // its own allocator; schema_destroy decides free vs no-op.
  int  (*load_schema)(const char* table_uri, const char* options_json,
                      const ObExtTableHostApi* host,
                      char** out_schema_json, int32_t* out_len);
  // Release the buffer returned by load_schema. May be NULL only if the plugin
  // guarantees load_schema never returns a buffer that needs freeing (e.g. always
  // static); otherwise OB leaks. `host` is the same host passed to load_schema.
  void (*schema_destroy)(char* schema_json, int32_t len,
                         const ObExtTableHostApi* host);

  // ---- plan / scan tasks ----
  // The plugin prunes partitions (partition_filter) and files (predicate)
  // internally, reads metadata/files via `host`, and returns ALL scan tasks at once
  // as one JSON document in *out_tasks_json. OB parses it, copies the tasks it
  // needs (each task's JSON object is what OB later hands to reader_open_task), then
  // releases the buffer via tasks_destroy below. As with schema, the plugin owns
  // the release.
  // partition_filter / predicate are OB-built predicate-tree JSON, valid only for
  // the call (NULL or "" = no pushdown). catalog_context from load_schema is
  // carried inside options_json (OB_EXT_K_CATALOG_CONTEXT); the plugin may
  // return OB_OLD_SCHEMA_VERSION on catalog/plan schema drift. limit == -1 =>
  // unlimited; desired_task_count is a PX parallelism hint (may be ignored).
  int  (*plan_create)(const char* table_uri, const char* options_json,
                      const char* partition_filter_json, const char* predicate_json,
                      int64_t limit, int32_t desired_task_count,
                      const ObExtTableHostApi* host,
                      char** out_tasks_json, int32_t* out_len);
  // Release the buffer returned by plan_create (may be NULL if always static).
  void (*tasks_destroy)(char* tasks_json, int32_t len,
                        const ObExtTableHostApi* host);

  // ---- reader (three persistent state objects per row iterator) ----
  // Each state is written only by its own scope's calls; outer scopes are
  // passed const to inner calls:
  //   worker: reader_create / reader_close. Also holds the read configuration
  //           known at iterator init (table_uri / options / projection).
  //   scan:   reader_open_scan / reader_close_scan. Holds the read pipeline
  //           built from worker constants + this scan's predicate.
  //   task:   reader_open_task / reader_next_batch / reader_close_task. Holds
  //           the split + batch reader.
  // JSON inputs are OB-owned, valid only for the call. options_json has the
  // same shape as plan_create's. NULL projection -> read all columns. Outputs
  // stay NULL on failure.
  int (*reader_create)(
      const ObExtTableHostApi *host,
      const char *table_uri,
      const char *options_json,
      const char *read_projection_json,
      ObExtTableReaderWorkerStateRef *out_worker,
      ObExtTableReaderScanStateRef *out_scan,
      ObExtTableReaderTaskStateRef *out_task);
  // Begin one scan/rescan: builds the scan's read pipeline from worker
  // constants + predicate_json (NULL/"" = no pushdown). Returns an OB errno.
  int (*reader_open_scan)(
      ObExtTableReaderWorkerStateConstRef,
      ObExtTableReaderScanStateRef,
      const char *predicate_json);
  // Open one task on the scan's pipeline: deserializes task_json's payload_b64
  // and creates the batch reader. Writes task state only. start_row /
  // row_count = row-range subdivision (pass 0 / task_row_count for the whole
  // task). The caller brackets every scan/rescan with reader_open_scan and
  // reader_close_scan.
  int (*reader_open_task)(
      ObExtTableReaderWorkerStateConstRef,
      ObExtTableReaderScanStateConstRef,
      ObExtTableReaderTaskStateRef,
      const char *task_json,
      int32_t task_len,
      uint64_t start_row,
      uint64_t row_count);
  // 0 = batch available (caller owns & must release arr/sch); 1 = EOF;
  // <0 = OB errno on error (the plugin logs the diagnostic via host->log).
  int (*reader_next_batch)(
      ObExtTableReaderWorkerStateConstRef,
      ObExtTableReaderTaskStateRef,
      struct ArrowArray *arr,
      struct ArrowSchema *sch);
  // End the active task; the task backing object, the scan's read pipeline,
  // and worker resources survive.
  void (*reader_close_task)(ObExtTableReaderTaskStateRef);
  // End the current scan and release its read pipeline; the scan backing
  // object and worker resources survive.
  void (*reader_close_scan)(ObExtTableReaderWorkerStateConstRef, ObExtTableReaderScanStateRef);
  // Final destruction of all three backing objects. No plugin object may use
  // `host` after this call.
  void (*reader_close)(
      ObExtTableReaderWorkerStateRef,
      ObExtTableReaderScanStateRef,
      ObExtTableReaderTaskStateRef);

  // ---- statistics (OPTIONAL: NULL => no stats, OB uses default cardinality) ----
  // Plugin builds the statistics JSON, returns it in *out_stats_json (+ *out_len).
  // OB parses it, then releases the buffer via stats_destroy below.
  // partition_filter scopes the stats (NULL = whole table).
  int  (*fetch_statistics)(const char* table_uri, const char* options_json,
                           const char* partition_filter_json,
                           char** out_stats_json, int32_t* out_len,
                           const ObExtTableHostApi* host);
  // Release the buffer returned by fetch_statistics (may be NULL if always static).
  void (*stats_destroy)(char* stats_json, int32_t len,
                        const ObExtTableHostApi* host);


};

/// Single entry symbol exported by every external-table plugin .so. OB resolves it
/// via dlsym and receives the full vtable. Returns NULL on ABI mismatch (or if the
/// .so is absent — dlsym fails — which OB reports as an error, so a format is
/// selectable only when its .so is present).
typedef const struct ObExtTablePluginApi* (*ob_ext_table_plugin_get_api_fn)(unsigned int abi_version);
const struct ObExtTablePluginApi* ob_ext_table_plugin_get_api(unsigned int abi_version);

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // OB_EXTERNAL_TABLE_PLUGIN_H

/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_EXT_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_EXT_FILE_PRUNER_H

#include "sql/optimizer/file_prune/ob_i_lake_table_file_pruner.h"
#include "share/catalog/ob_catalog_properties.h"  // ObLakeTableFormat
#include "plugin/v2/include/ob_external_table_plugin.h"  // ObExtTablePluginApi (fwd decls only here)

namespace oceanbase
{
namespace sql
{
namespace ext_plugin
{
class ObExtTableMetadata;
}

enum class ObExtTableDispatchMode
{
  ROUND_ROBIN = 0,
  PARTITION_BUCKET_WISE = 1
};

/// Prune-stage scan-task descriptor for the generic plugin contract (analogous
/// to ObIcebergFileDesc / ObHiveFileDesc). Payload = contract task_json.
/// Converted to ObOptPluginFile when attaching to a tablet loc.
struct ObPluginSplitDesc
{
public:
  explicit ObPluginSplitDesc(common::ObIAllocator &allocator)
      : task_json_(), record_count_(0), allocator_(allocator) {}
  int assign(common::ObIAllocator &allocator, const ObPluginSplitDesc &other);
  void reset();
  TO_STRING_KV(K_(task_json), K_(record_count));

  ObString task_json_;                 // single scan-task JSON text (carries payload_b64)
  int64_t record_count_;               // task row_count (-1 => 0 unknown)
  common::ObIAllocator &allocator_;
};

/// Push-model pruner for plugin-backed lake tables. Drives the
/// plugin contract's `plan_create` to obtain ALL scan tasks at once, then emits
/// one `ObPluginSplitDesc` per task (task_json_ = the single-task JSON text the
/// row iter later hands to `reader_create`; record_count_ = task row_count). The
/// existing `select_location_for_plugin` distributes them across PX servers — the
/// PX plumbing is reused unchanged.
class ObExtFilePruner : public ObILakeTableFilePruner
{
public:
  explicit ObExtFilePruner(common::ObIAllocator &allocator);
  virtual ~ObExtFilePruner() {}

  int init(const ObDMLStmt &stmt,
           ObExecContext *exec_ctx,
           const uint64_t table_id,
           const uint64_t ref_table_id,
           const ext_plugin::ObExtTableMetadata *ext_metadata,
           const common::ObIArray<ObRawExpr *> &filter_exprs);

  int prune_ext_splits(ObExecContext &exec_ctx,
                       common::ObIArray<ObPluginSplitDesc *> &splits,
                       ObExtTableDispatchMode &dispatch_mode);

  virtual int assign(const ObILakeTableFilePruner &o) override;
  virtual int clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const override;

private:
  common::ObString table_uri_;
  common::ObString access_info_;
  share::ObLakeTableFormat plugin_format_ = share::ObLakeTableFormat::INVALID;
  const ext_plugin::ObExtTableMetadata *ext_metadata_;
  common::ObSEArray<ObRawExpr *, 4> filter_exprs_;
  ObExecContext *exec_ctx_;
  // Per-query tuning blob from the EXT_TABLE_OPTIONS opt_param hint: an opaque
  // JSON string passed verbatim into the plugin's options_json (under
  // OB_EXT_K_EXT_OPTIONS). Empty when the hint is absent (no tuning). OB never
  // inspects the contents; the plugin unwraps + validates it.
  common::ObString ext_options_hint_;
};

} // namespace sql
} // namespace oceanbase

#endif // _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_EXT_FILE_PRUNER_H

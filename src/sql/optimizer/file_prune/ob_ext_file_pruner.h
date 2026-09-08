/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_EXT_FILE_PRUNER_H
#define _OCEANBASE_SQL_OPTIMIZER_FILE_PRUNE_OB_EXT_FILE_PRUNER_H

#include "sql/optimizer/file_prune/ob_i_lake_table_file_pruner.h"
#include "sql/optimizer/file_prune/ob_lake_table_fwd.h"
#include "share/catalog/ob_catalog_properties.h"  // ObLakeTableFormat
#include "share/external_table/ob_external_table_part_info.h"
#include "plugin/v2/include/ob_external_table_plugin.h"  // ObExtTablePluginApi (fwd decls only here)

namespace oceanbase
{
namespace share
{
struct ObExtFileScanDescriptor;
struct ObExtScanTask;
struct ObExtTaskPartitionValue;
}
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

/// Prune-stage plugin task, converted to ObOptPluginFile at location selection.
/// Plugin-reader tasks carry plugin_task_json_; OB-file tasks carry one physical file.
struct ObPluginSplitDesc
{
public:
  ObPluginSplitDesc()
      : plugin_task_json_(), file_url_(), file_size_(0), part_id_(OB_INVALID_PARTITION_ID),
        record_count_(0), reader_type_(ObPluginReaderType::INVALID) {}
  TO_STRING_KV(K_(plugin_task_json), K_(file_url), K_(file_size), K_(part_id),
               K_(record_count), K_(reader_type));

  ObString plugin_task_json_;          // Empty for OB file tasks
  ObString file_url_;                  // Empty for plugin-reader tasks
  int64_t file_size_;
  int64_t part_id_;
  int64_t record_count_;               // Plugin estimate (0 unknown); OB file exact row count
  ObPluginReaderType reader_type_;
};

/// Produces the complete scan-task transport for a plugin lake table.
///
///   plan_create root task
///     |-- plugin_split -> plugin-reader split(plugin_task_json)
///     `-- ob_file_scan -> OB file splits(path, byte size, row count, Parquet/ORC)
///              |
///              `--> location/PX --> ObPluginScanTask --> AccessService
///
/// Partition values are converted once into the standard CTDEF partition rows;
/// every split from the same root task keeps that root's part_id.
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

  virtual int get_part_id_and_range_exprs(
      common::ObIArray<uint64_t> &part_column_ids,
      common::ObIArray<ObRawExpr *> &range_exprs) override;

  int copy_partition_infos_to(
      common::ObIAllocator &target_allocator,
      share::ObExternalTablePartInfoArray &target) const;
  virtual int assign(const ObILakeTableFilePruner &o) override;
  virtual int clone(common::ObIAllocator &allocator, ObILakeTableFilePruner *&pruner) const override;

private:
  int cast_task_partition_value(
      const share::ObExtTaskPartitionValue &source,
      const ObColumnMeta &column_meta,
      common::ObObj &result);
  int build_task_partition_info(
      const share::ObExtScanTask &task,
      const common::ObIArray<uint64_t> &partition_col_ids,
      const common::ObIArray<int64_t> &partition_col_idxs,
      int64_t part_id,
      share::ObExternalTablePartInfo &part_info);
  int append_scan_task_splits(
      const share::ObExtScanTask &task,
      int64_t part_id,
      ObPluginReaderType reader_type,
      const share::ObExtFileScanDescriptor *ob_file_scan,
      common::ObIArray<ObPluginSplitDesc *> &splits);
  int build_plugin_splits(
      const char *tasks_json,
      int32_t tasks_len,
      const common::ObIArray<uint64_t> &partition_col_ids,
      const common::ObIArray<ObRawExpr *> &partition_filter_exprs,
      const common::ObString &predicate_json,
      common::ObIArray<ObPluginSplitDesc *> &splits);
  common::ObString table_uri_;
  common::ObString access_info_;
  share::ObLakeTableFormat plugin_format_ = share::ObLakeTableFormat::INVALID;
  const ext_plugin::ObExtTableMetadata *ext_metadata_;
  common::ObSEArray<ObRawExpr *, 4> filter_exprs_;
  common::ObSEArray<uint64_t, 4> proven_part_column_ids_;
  common::ObSEArray<ObRawExpr *, 4> proven_part_filter_exprs_;
  share::ObExternalTablePartInfoArray partition_infos_;
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

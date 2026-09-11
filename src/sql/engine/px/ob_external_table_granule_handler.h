/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OCEANBASE_SQL_ENGINE_PX_OB_EXTERNAL_TABLE_GRANULE_HANDLER_H
#define _OCEANBASE_SQL_ENGINE_PX_OB_EXTERNAL_TABLE_GRANULE_HANDLER_H

#include "ob_granule_util.h"
#include "ob_granule_pump.h"

namespace oceanbase
{
namespace sql
{

/// Polymorphic granule splitting for external tables: one handler per format
/// family. Handlers are stateless; the factory hands out a fresh instance per
/// call on the caller's allocator.
class ObIExternalTableGranuleHandler
{
public:
  virtual ~ObIExternalTableGranuleHandler() = default;
  virtual int split_granule(ObGranulePumpArgs &args,
                            const ObTableScanSpec *tsc,
                            const common::ObIArray<common::ObNewRange> &ranges,
                            const common::ObIArray<ObDASTabletLoc *> &tablets,
                            bool partition_granule,
                            common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                            common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
                            common::ObIArray<int64_t> &granule_idx) = 0;
};

/// CSV / local-disk / kafka file external tables, with parallel-parse helpers
/// and runner builder.
class ObCsvGranuleHandler : public ObIExternalTableGranuleHandler
{
public:
  virtual int split_granule(ObGranulePumpArgs &args,
                            const ObTableScanSpec *tsc,
                            const common::ObIArray<common::ObNewRange> &input_ranges,
                            const common::ObIArray<ObDASTabletLoc *> &tablet_array,
                            bool partition_granule,
                            common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                            common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
                            common::ObIArray<int64_t> &granule_idx) override;
private:
  int split_granule_for_parallel_resolve_csv_for_range_prepare(
      ObExecContext &exec_ctx,
      common::ObIAllocator &args_ctx_allocator,
      const ObString &location, const ObString &access_info,
      const ObString &format, int64_t parallelism,
      int64_t tsc_op_id, int64_t op_id, int64_t csv_large_file_size_threshold,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
  int split_granule_for_parallel_resolve_csv(
      ObExecContext &exec_ctx,
      common::ObIAllocator &allocator,
      const ObString &location,
      const ObString &access_info,
      const ObString &format,
      int64_t parallelism, GITaskGenRunner *runner,
      int64_t csv_large_file_size_threshold,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
  int create_runner_for_csv(ObExecContext &exec_ctx,
                            ObGranulePump &gi_pump,
                            int64_t tsc_op_id,
                            int64_t gi_op_id,
                            const ObString &location,
                            const ObString &access_info,
                            const ObString &format,
                            int64_t parallelism,
                            GITaskGenRunner *&runner);
};

/// ODPS lake table: fine-grained split — reads this SQC's scan units from the
/// lake file map, turns them back into ObExternalFileInfo and dispatches to
/// the ODPS range_prepare split (tunnel: GIOdpsParallelTaskGen JIT session pull
/// + worker-level re-split; storage byte/row: session + split/row ranges are
/// already baked into the units at optimize time). Falls back to the generic
/// lake split when the map has no entry for the tablet (empty table).
class ObOdpsGranuleHandler : public ObIExternalTableGranuleHandler
{
public:
  virtual int split_granule(ObGranulePumpArgs &args,
                            const ObTableScanSpec *tsc,
                            const common::ObIArray<common::ObNewRange> &ranges,
                            const common::ObIArray<ObDASTabletLoc *> &tablets,
                            bool partition_granule,
                            common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                            common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
                            common::ObIArray<int64_t> &granule_idx) override;
private:
  int split_granule_for_odps_by_line_tunnel_partition_for_range_prepare(
      ObExecContext &exec_ctx, common::ObIAllocator &args_ctx_allocator,
      const ObString &properties, int64_t parallelism, int64_t tsc_op_id, int64_t op_id,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
  int split_granule_for_odps_by_line_tunnel_partition(
      ObExecContext &exec_ctx, common::ObIAllocator &allocator,
      const ObString &properties, int64_t parallelism,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
  int split_granule_for_odps_by_total_byte(
      ObIAllocator &allocator, int64_t parallelism,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
  int split_granule_for_odps_by_total_row(
      ObIAllocator &allocator, int64_t parallelism,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
  int create_runner_for_odps(ObExecContext &exec_ctx,
                             ObGranulePump &gi_pump,
                             int64_t tsc_op_id,
                             int64_t gi_op_id,
                             const ObString &properties,
                             int64_t parallelism,
                             GITaskGenRunner *&runner);
};

/// Iceberg / Hive / cpp-plugin lake tables: the generic lake file map split.
class ObLakeGranuleHandler : public ObIExternalTableGranuleHandler
{
public:
  virtual int split_granule(ObGranulePumpArgs &args,
                            const ObTableScanSpec *tsc,
                            const common::ObIArray<common::ObNewRange> &ranges,
                            const common::ObIArray<ObDASTabletLoc *> &tablets,
                            bool partition_granule,
                            common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                            common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
                            common::ObIArray<int64_t> &granule_idx) override;
  /// The generic lake split core; also used by the ODPS handler as its
  /// empty-table / map-miss fallback.
  static int split_granule_for_lake_table(
      ObExecContext &exec_ctx,
      common::ObIAllocator &allocator,
      const ObTableScanSpec *tsc,
      const common::ObIArray<common::ObNewRange> &ranges,
      const common::ObIArray<ObDASTabletLoc *> &tablets,
      bool force_partition_granule,
      common::ObIArray<ObDASTabletLoc *> &granule_tablets,
      common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
      common::ObIArray<int64_t> &granule_idx);
};

class ObExternalTableGranuleHandlerFactory
{
public:
  /// Dispatch by scan format: file external tables (CSV/local/kafka) take the
  /// CSV handler, ODPS lake tables the ODPS handler, all other lake formats
  /// the generic lake handler.
  static int create(common::ObIAllocator &allocator,
                    const ObDASScanCtDef &scan_ctdef,
                    ObIExternalTableGranuleHandler *&handler);
};

} // namespace sql
} // namespace oceanbase
#endif

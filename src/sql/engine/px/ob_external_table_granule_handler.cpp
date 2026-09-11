/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_EXE

#include "ob_external_table_granule_handler.h"
#include "sql/ob_sql_utils.h"
#include "sql/engine/px/ob_px_sqc_handler.h"
#include "src/sql/engine/px/ob_dfo.h"
#include "src/sql/engine/px/ob_px_sqc_handler.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/external_table/ob_csv_table_utils.h"
#include "share/external_table/ob_odps_table_utils.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/das/ob_das_simple_op.h"
#include "src/sql/engine/px/ob_granule_iterator_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{

int ObLakeGranuleHandler::split_granule_for_lake_table(ObExecContext &exec_ctx,
                                                ObIAllocator &allocator,
                                                const ObTableScanSpec *tsc,
                                                const ObIArray<ObNewRange> &ranges,
                                                const ObIArray<ObDASTabletLoc *> &tablets,
                                                bool force_partition_granule,
                                                ObIArray<ObDASTabletLoc *> &granule_tablets,
                                                ObIArray<ObIExtTblScanTask*> &granule_tasks,
                                                ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  ObLakeTableFileMap *map = nullptr;
  if (OB_UNLIKELY(tablets.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(tablets.count()));
  } else if (OB_UNLIKELY(ranges.empty())) {
    // always false
    ObIExtTblScanTask *scan_task = NULL;
    if (OB_UNLIKELY(tablets.count() != 1) || OB_ISNULL(tablets.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected tablets counts", K(tablets));
    } else if (OB_FAIL(ObExternalTableUtils::alloc_empty_lake_table_scan_task(
                   allocator,
                   OB_ISNULL(tsc) ? share::ObLakeTableFormat::INVALID
                                  : tsc->tsc_ctdef_.scan_ctdef_.lake_table_format_,
                   tablets.at(0)->partition_id_, scan_task))) {
      LOG_WARN("failed to alloc lake empty scan task", K(ret));
    } else if (OB_FAIL(granule_tablets.push_back(tablets.at(0)))) {
      LOG_WARN("failed to push basck tablet loc");
    } else if (OB_FAIL(granule_tasks.push_back(scan_task))) {
      LOG_WARN("failed to push back scan task");
    } else if (OB_FAIL(granule_idx.push_back(0))) {
      LOG_WARN("failed to push back granule idx");
    }
  } else if (OB_FAIL(exec_ctx.get_lake_table_file_map(map))) {
    LOG_WARN("failed to get lake table file map");
  } else if (OB_ISNULL(map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lake talbe file map");
  } else {
    int64_t pk_idx = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablets.count(); ++i) {
      ObDASTabletLoc* tablet_loc = tablets.at(i);
      ObLakeTableFileArray* files = nullptr;
      if (OB_ISNULL(tablet_loc) || OB_ISNULL(tablet_loc->loc_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null tablet loc", KP(tablet_loc));
      } else if (OB_FAIL(map->get_refactored(ObLakeTableFileMapKey(tablet_loc->loc_meta_->table_loc_id_, tablet_loc->tablet_id_),
                                             files))) {
        LOG_WARN("failed to get refactored");
      } else if (OB_ISNULL(files)) {
        ObIExtTblScanTask *dummy_file = nullptr;
        if (OB_UNLIKELY(tablets.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected tablets counts", K(tablets));
        } else if (OB_FAIL(ObExternalTableUtils::alloc_empty_lake_table_scan_task(
                       allocator,
                       OB_ISNULL(tsc) ? share::ObLakeTableFormat::INVALID
                                      : tsc->tsc_ctdef_.scan_ctdef_.lake_table_format_,
                       tablet_loc->partition_id_, dummy_file))) {
          LOG_WARN("failed to alloc lake empty scan task", K(ret));
        }

        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(granule_tablets.push_back(tablet_loc))) {
          LOG_WARN("failed to push basck tablet loc");
        } else if (OB_FAIL(granule_tasks.push_back(dummy_file))) {
          LOG_WARN("failed to push back range");
        } else if (OB_FAIL(granule_idx.push_back(pk_idx))) {
          LOG_WARN("failed to push back granule idx");
        }
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < files->count(); ++j) {
          ObFileScanTask *scan_task = static_cast<ObFileScanTask *>(files->at(j));
          if (OB_ISNULL(scan_task)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get null file");
          } else if (FALSE_IT(scan_task->assign_granule_identity(j, tablet_loc->partition_id_))) {
          } else if (OB_FAIL(granule_tablets.push_back(tablet_loc))) {
            LOG_WARN("failed to push basck tablet loc");
          } else if (OB_FAIL(granule_tasks.push_back(scan_task))) {
            LOG_WARN("failed to push back range");
          } else if (OB_FAIL(granule_idx.push_back(pk_idx))) {
            LOG_WARN("failed to push back granule idx");
          } else if (!force_partition_granule) {
            pk_idx++;
          }
        }
        if (force_partition_granule) {
          pk_idx++;
        }
      }
    }
  }
  LOG_TRACE("check lake table split ranges", K(force_partition_granule), K(ranges), K(granule_tasks));
  return ret;
}

int ObLakeGranuleHandler::split_granule(ObGranulePumpArgs &args,
                                        const ObTableScanSpec *tsc,
                                        const common::ObIArray<common::ObNewRange> &ranges,
                                        const common::ObIArray<ObDASTabletLoc *> &tablets,
                                        bool partition_granule,
                                        common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                                        common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
                                        common::ObIArray<int64_t> &granule_idx)
{
  return split_granule_for_lake_table(*args.ctx_, args.ctx_->get_allocator(), tsc, ranges,
                                      tablets, partition_granule, granule_tablets,
                                      granule_tasks, granule_idx);
}

int ObOdpsGranuleHandler::create_runner_for_odps(ObExecContext &exec_ctx,
                                          ObGranulePump &gi_pump,
                                          int64_t tsc_op_id,
                                          int64_t gi_op_id,
                                          const ObString &properties,
                                          int64_t parallelism,
                                          GITaskGenRunner *&runner)
{
  int ret = OB_SUCCESS;
  GIOdpsParallelTaskGen *generator = NULL;
  if (OB_ISNULL(gi_pump.get_granule_pump_arg(gi_op_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gi_op_id is invalid", K(ret), K(gi_op_id));
  } else {
    uint64_t gi_attri_flag = gi_pump.get_granule_pump_arg(gi_op_id)->gi_attri_flag_;
    GITaskGenRunnerBuilder builder(tsc_op_id, exec_ctx, &gi_pump);
    builder.add<GIOdpsParallelTaskGen, ObOdpsTaskGenContext>(
              generator, properties, tsc_op_id, gi_attri_flag, &gi_pump, &exec_ctx, parallelism);
    if (OB_FAIL(builder.build(runner))) {
      LOG_WARN("failed to build task gen runner", K(ret));
    }
  }
  return ret;
}

int ObOdpsGranuleHandler::split_granule_for_odps_by_line_tunnel_partition_for_range_prepare(
        ObExecContext &exec_ctx, common::ObIAllocator &args_ctx_allocator,
        const ObString &properties, int64_t parallelism, int64_t tsc_op_id,
        int64_t gi_op_id,
        const common::ObIArray<ObDASTabletLoc *> &tablets,
        const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
        common::ObIArray<ObDASTabletLoc *> &granule_tablets,
        common::ObIArray<ObIExtTblScanTask*> &granule_tasks,
        common::ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == gi_op_id) {
    // DAS
    if (OB_FAIL(split_granule_for_odps_by_line_tunnel_partition(
            exec_ctx, args_ctx_allocator, properties, parallelism, tablets,
            external_table_files, granule_tablets, granule_tasks,
            granule_idx))) {
      LOG_WARN("failed to split granule for odps by partition line tunnel",
               K(ret));
    }
  } else {
    ObGranulePump &gi_pump = exec_ctx.get_sqc_handler()->get_sqc_ctx().gi_pump_;
    GITaskGenRunner *runner = NULL;
    bool is_rescan_process = false;

    if (OB_FAIL(ObGranuleUtil::get_external_task_runner_rescan_status(gi_pump, tsc_op_id, runner, is_rescan_process))) {
      LOG_WARN("failed to get external task runner rescan status", K(ret));
    } else if (is_rescan_process) {
      if (OB_FAIL(ObGranuleUtil::fill_final_task(runner, tsc_op_id, granule_tablets, granule_tasks, granule_idx))) {
        LOG_WARN("failed to fill final task", K(ret));
      }
    } else {
      if (OB_ISNULL(runner) && OB_FAIL(create_runner_for_odps(
              exec_ctx, gi_pump, tsc_op_id, gi_op_id, properties, parallelism, runner))) {
        LOG_WARN("failed to create runner for odps", K(ret));
      }
      // runner may created and rerun at regenerate_gi_task in ObPxTaskProcess::execute
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(split_granule_for_odps_by_line_tunnel_partition(
              exec_ctx, args_ctx_allocator, properties, parallelism, tablets,
              external_table_files, granule_tablets, granule_tasks, granule_idx))) {
        LOG_WARN("failed to fill original tasks for odps", K(ret));
      }
    }
  }
  return ret;
}

int ObOdpsGranuleHandler::split_granule_for_odps_by_line_tunnel_partition(
    ObExecContext &exec_ctx, common::ObIAllocator &args_ctx_allocator,
    const ObString &properties, int64_t parallelism,
    const common::ObIArray<ObDASTabletLoc *> &tablets,
    const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
    common::ObIArray<ObDASTabletLoc *> &granule_tablets,
    common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
    common::ObIArray<int64_t> &granule_idx) {
  int ret = OB_SUCCESS;
  int64_t task_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
    const ObExternalFileInfo &external_info = external_table_files.at(i);
    ObOdpsScanTask *scan_task = NULL;
    // pump args ctx From exec_ctx ref: add_ObGranuleSplitter::split_gi_task
    if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, (&args_ctx_allocator)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    }
    int64_t file_start =
        external_info.row_count_ != 0 ? external_info.row_start_ : 0;
    int64_t file_end = 0;
    if (external_info.row_count_ == INT64_MAX ||
        external_info.row_count_ == 0) {
      file_end = INT64_MAX; // 空分区
    } else {
      file_end = file_start + external_info.row_count_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObOdpsTableUtils::make_odps_scan_task(
                   external_info.file_url_, external_info.part_id_, file_start,
                   file_end,
                   external_info.session_id_, // correctness guarantee
                   0, 0, *scan_task))) {
      LOG_WARN("failed to make external table scan range", K(ret));
    } else if ((OB_FAIL(granule_tasks.push_back(scan_task)) ||
                OB_FAIL(granule_idx.push_back(task_idx++)) ||
                OB_FAIL(granule_tablets.push_back(tablets.at(0))))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObOdpsGranuleHandler::split_granule_for_odps_by_total_byte(ObIAllocator &allocator, int64_t parallelism, const ObIArray<ObDASTabletLoc *> &tablets,
    const ObIArray<ObExternalFileInfo> &external_table_files, ObIArray<ObDASTabletLoc *> &granule_tablets,
    ObIArray<ObIExtTblScanTask*> &granule_tasks, ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
    ObString session_str = external_table_files.at(i).session_id_;
    ObString part_str = external_table_files.at(i).file_url_;
    int64_t start_split_idx = external_table_files.at(i).file_id_;
    int64_t split_count = external_table_files.at(i).file_size_;
    ObOdpsScanTask *scan_task = NULL;
    if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, &allocator))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    } else if (OB_FAIL(ObOdpsTableUtils::make_odps_scan_task(part_str,  // file 实际上不需要
                    0,          // table id不需要用外表的参数
                    0,          // 开始位置 这个数值这理不使用
                    INT64_MAX,  // 到结束为止 这个数值这里不使用
                    session_str,
                    start_split_idx,  // split 左闭右开
                    start_split_idx + split_count,
                    *scan_task))) {
      LOG_WARN("failed to make external table scan range", K(ret));
    } else {
      OZ(granule_tasks.push_back(scan_task));
      OZ(granule_idx.push_back(task_idx++));
      OZ(granule_tablets.push_back(tablets.at(0)));
    }
  }
  return ret;
}

int ObOdpsGranuleHandler::split_granule_for_odps_by_total_row(ObIAllocator &allocator, int64_t parallelism,
  const ObIArray<ObDASTabletLoc *> &tablets, const ObIArray<ObExternalFileInfo> &external_table_files,
  ObIArray<ObDASTabletLoc *> &granule_tablets, ObIArray<ObIExtTblScanTask*> &granule_tasks,
  ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = 0;
  // split by rows
  for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
    ObString session_str = external_table_files.at(i).session_id_;
    ObString part_str = external_table_files.at(i).file_url_;
    int64_t start_row_count = external_table_files.at(i).row_start_;
    int64_t end_row_count = start_row_count + external_table_files.at(i).row_count_;
    ObOdpsScanTask *scan_task = NULL;
    if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, &allocator))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    } else if (OB_FAIL(ObOdpsTableUtils::make_odps_scan_task(part_str,  // session has kown partition
                    0,                // table id不需要用外表的参数
                    start_row_count,  // start index of all table
                    end_row_count,        // number of records
                    session_str,
                    0,  // split by size won't use in this branch
                    0,  // split by size won't use in this branch
                    *scan_task))) {
      LOG_WARN("failed to make external table scan range", K(ret));
    } else {
      OZ(granule_tasks.push_back(scan_task));
      OZ(granule_tablets.push_back(tablets.at(0)));
      OZ(granule_idx.push_back(task_idx++));
    }
  }
  return ret;
}


int ObOdpsGranuleHandler::split_granule(ObGranulePumpArgs &args,
                                        const ObTableScanSpec *tsc,
                                        const common::ObIArray<common::ObNewRange> &ranges,
                                        const common::ObIArray<ObDASTabletLoc *> &tablets,
                                        bool partition_granule,
                                        common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                                        common::ObIArray<ObIExtTblScanTask *> &granule_tasks,
                                        common::ObIArray<int64_t> &granule_idx)
{
  UNUSED(partition_granule);
  ObExecContext &exec_ctx = *args.ctx_;
  common::ObIAllocator &allocator = args.ctx_->get_allocator();
  const int64_t parallelism = args.parallelism_;
  const int64_t gi_op_id = args.gi_op_id_;
  int ret = OB_SUCCESS;
  ObLakeTableFileMap *map = nullptr;
  ObSEArray<share::ObExternalFileInfo, 16> external_table_files;
  bool has_files = false;
  if (OB_ISNULL(tsc) || OB_UNLIKELY(tablets.count() < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid argument", K(ret), KP(tsc), K(tablets.count()));
  } else if (OB_UNLIKELY(ranges.empty())) {
    // always false: the generic lake split emits the typed dummy task
    if (OB_FAIL(ObLakeGranuleHandler::split_granule_for_lake_table(exec_ctx, allocator, tsc, ranges, tablets,
                                             false, granule_tablets, granule_tasks, granule_idx))) {
      LOG_WARN("failed to split granule for empty lake table", K(ret));
    }
  } else if (OB_FAIL(exec_ctx.get_lake_table_file_map(map))) {
    LOG_WARN("failed to get lake table file map", K(ret));
  } else if (OB_ISNULL(map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lake table file map", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablets.count(); ++i) {
      ObDASTabletLoc *tablet_loc = tablets.at(i);
      ObLakeTableFileArray *files = nullptr;
      if (OB_ISNULL(tablet_loc) || OB_ISNULL(tablet_loc->loc_meta_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get null tablet loc", K(ret), KP(tablet_loc));
      } else if (OB_FAIL(map->get_refactored(
                     ObLakeTableFileMapKey(tablet_loc->loc_meta_->table_loc_id_,
                                           tablet_loc->tablet_id_),
                     files))) {
        if (OB_HASH_NOT_EXIST == ret) {
          // no entry: the optimizer guarantees one, but stay defensive and let
          // the generic lake split below emit a typed dummy task
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get lake table files", K(ret));
        }
      }
      for (int64_t j = 0; OB_SUCC(ret) && OB_NOT_NULL(files) && j < files->count(); ++j) {
        ObOdpsScanTask *scan_task = static_cast<ObOdpsScanTask *>(files->at(j));
        share::ObExternalFileInfo info;
        if (OB_ISNULL(scan_task) || LakeFileType::ODPS != scan_task->get_file_type()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected lake table scan task", K(ret), KP(scan_task));
        } else {
          info.file_url_ = scan_task->file_url_;
          info.part_id_ = scan_task->part_id_;
          info.session_id_ = scan_task->session_id_;
          // storage byte mode: file_id_/file_size_ carry the split range
          info.file_id_ = scan_task->first_split_idx_;
          info.file_size_ = scan_task->last_split_idx_ - scan_task->first_split_idx_;
          // tunnel / storage row mode: row range, left-closed right-open
          info.row_start_ = scan_task->first_lineno_;
          info.row_count_ = (INT64_MAX == scan_task->last_lineno_)
                                ? INT64_MAX
                                : scan_task->last_lineno_ - scan_task->first_lineno_;
          if (OB_FAIL(external_table_files.push_back(info))) {
            LOG_WARN("failed to push back external table file", K(ret));
          } else {
            has_files = true;
          }
        }
      }
    }
  }
  if (OB_FAIL(ret) || has_files) {
  } else if (OB_FAIL(ObLakeGranuleHandler::split_granule_for_lake_table(exec_ctx, allocator, tsc, ranges, tablets,
                                                  false, granule_tablets, granule_tasks,
                                                  granule_idx))) {
    // empty table / map miss: the generic lake split emits the typed dummy task
    LOG_WARN("failed to split granule for empty odps lake table", K(ret));
  }
  if (OB_FAIL(ret) || !has_files) {
  } else {
    const ObString &format_str = tsc->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_;
    bool is_odps_external_table = false;
    ObODPSGeneralFormat::ApiMode odps_api_mode = ObODPSGeneralFormat::ApiMode::TUNNEL_API;
    if (OB_FAIL(ObSQLUtils::get_odps_api_mode(format_str, is_odps_external_table, odps_api_mode))) {
      LOG_WARN("failed to get odps api mode", K(ret));
    } else if (OB_UNLIKELY(!is_odps_external_table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not an odps external table", K(ret));
    } else if (ObODPSGeneralFormat::ApiMode::TUNNEL_API == odps_api_mode) {
      // units without a pre-fetched session are JIT-pulled and re-split by the
      // real readers_parallelism in GIOdpsParallelTaskGen (created on demand by
      // the range_prepare variant); units with a session pass through.
      if (OB_FAIL(split_granule_for_odps_by_line_tunnel_partition_for_range_prepare(
              exec_ctx, allocator, format_str, parallelism, tsc->get_id(), gi_op_id,
              tablets, external_table_files, granule_tablets, granule_tasks, granule_idx))) {
        LOG_WARN("failed to split granule for odps by line tunnel partition", K(ret));
      }
    } else if (ObODPSGeneralFormat::ApiMode::BYTE == odps_api_mode) {
      // session + split ranges are baked into the units at optimize time
      if (OB_FAIL(split_granule_for_odps_by_total_byte(allocator, parallelism, tablets,
                                                       external_table_files, granule_tablets,
                                                       granule_tasks, granule_idx))) {
        LOG_WARN("failed to split granule for odps by total byte", K(ret));
      }
    } else {
      if (OB_FAIL(split_granule_for_odps_by_total_row(allocator, parallelism, tablets,
                                                      external_table_files, granule_tablets,
                                                      granule_tasks, granule_idx))) {
        LOG_WARN("failed to split granule for odps by total row", K(ret));
      }
    }
  }
  LOG_TRACE("check odps lake table split ranges", K(ranges), K(granule_tasks));
  return ret;
}

int ObCsvGranuleHandler::create_runner_for_csv(ObExecContext &exec_ctx,
                                         ObGranulePump &gi_pump,
                                         int64_t tsc_op_id,
                                         int64_t gi_op_id,
                                         const ObString &location,
                                         const ObString &access_info,
                                         const ObString &format,
                                         int64_t parallelism,
                                         GITaskGenRunner *&runner)
{
  int ret = OB_SUCCESS;
  GICsvGamblingParallelTaskGen *gambling_generator = NULL;
  GICsvFullScanParallelTaskGen *full_scan_generator = NULL;
  if (OB_ISNULL(gi_pump.get_granule_pump_arg(gi_op_id))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("gi_op_id is invalid", K(ret), K(gi_op_id));
  } else {
    uint64_t gi_attri_flag = gi_pump.get_granule_pump_arg(gi_op_id)->gi_attri_flag_;
    GITaskGenRunnerBuilder builder(tsc_op_id, exec_ctx, &gi_pump);
    builder.add<GICsvGamblingParallelTaskGen, ObCsvTaskGenContext>(
              gambling_generator, format, tsc_op_id, gi_attri_flag, &gi_pump, &exec_ctx, parallelism, location, access_info)
               .add<GICsvFullScanParallelTaskGen, ObCsvTaskGenContext>(
              full_scan_generator, format, tsc_op_id, gi_attri_flag, &gi_pump, &exec_ctx, parallelism, location, access_info);
    if (OB_FAIL(builder.build(runner))) {
      LOG_WARN("failed to build task gen runner", K(ret));
    }
  }
  return ret;
}

int ObCsvGranuleHandler::split_granule_for_parallel_resolve_csv_for_range_prepare(
                                        ObExecContext &exec_ctx,
                                        common::ObIAllocator &args_ctx_allocator,
                                        const ObString &location, const ObString &access_info,
                                        const ObString &format, int64_t parallelism,
                                        int64_t tsc_op_id, int64_t gi_op_id,
                                        int64_t csv_large_file_size_threshold,
                                        const common::ObIArray<ObDASTabletLoc *> &tablets,
                                        const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
                                        common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                                        common::ObIArray<ObIExtTblScanTask*> &granule_tasks,
                                        common::ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  ObGranulePump &gi_pump = exec_ctx.get_sqc_handler()->get_sqc_ctx().gi_pump_;
  GITaskGenRunner *runner = NULL;
  bool is_rescan_process = false;

  if (OB_FAIL(ObGranuleUtil::get_external_task_runner_rescan_status(gi_pump, tsc_op_id, runner, is_rescan_process))) {
    LOG_WARN("failed to get external task runner rescan status", K(ret));
  } else if (is_rescan_process) {
    if (OB_FAIL(ObGranuleUtil::fill_final_task(runner, tsc_op_id, granule_tablets, granule_tasks, granule_idx))) {
      LOG_WARN("failed to fill final task", K(ret));
    }
  } else {
    if (OB_ISNULL(runner) && OB_FAIL(create_runner_for_csv(
            exec_ctx, gi_pump, tsc_op_id, gi_op_id, location, access_info, format,
            parallelism, runner))) {
      LOG_WARN("failed to create runner for csv", K(ret));
    }
    // runner may created and rerun at regenerate_gi_task in ObPxTaskProcess::execute
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(split_granule_for_parallel_resolve_csv(
            exec_ctx, args_ctx_allocator, location, access_info, format,
            parallelism, runner, csv_large_file_size_threshold, tablets,
            external_table_files, granule_tablets, granule_tasks, granule_idx))) {
      LOG_WARN("failed to split granule for parallel resolve csv", K(ret));
    }
  }

  return ret;
}

int ObCsvGranuleHandler::split_granule_for_parallel_resolve_csv(
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
                  common::ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  ObCsvTaskGenContext *task_gen_ctx = nullptr;
  sql::ObExternalFileFormat external_file_format;

  if (OB_ISNULL(runner)
      || OB_ISNULL(task_gen_ctx = static_cast<ObCsvTaskGenContext *>(runner->get_ctx()))){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("runner or ctx is null", K(ret));
  } else if (OB_FAIL(external_file_format.load_from_string(format, allocator))) {
    LOG_WARN("failed to load from string", K(ret), K(format));
  }

  int64_t task_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
    const ObExternalFileInfo &file_info = external_table_files.at(i);
    const int64_t file_size = file_info.file_size_;
    if (file_size >= csv_large_file_size_threshold) {  // large csv file, split into chunks
      int64_t chunk_cnt = parallelism;
      int64_t avg_chunk_size = file_size / chunk_cnt;
      if (external_file_format.csv_format_.field_escaped_char_ == INT64_MAX) {  // no escaped char
        for (int64_t j = 0; j < chunk_cnt; ++j) {
          int64_t start_pos = j * avg_chunk_size;
          int64_t end_pos = (j == chunk_cnt - 1) ? file_size : start_pos + avg_chunk_size;
          ObExtTableScanTask *scan_task = NULL;
          if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, &allocator))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(scan_task->init_parallel_parse_csv_info(allocator))) {
            LOG_WARN("failed to init parallel parse csv info", K(ret));
          } else if (OB_FAIL(ObCsvTableUtils::make_parallel_parse_csv_task(file_info,
                                                                                1, INT64_MAX,
                                                                                start_pos, end_pos,
                                                                                j, chunk_cnt,
                                                                                scan_task))) {
            LOG_WARN("failed to make parallel parse csv task", K(ret));
          } else {
            if (OB_ISNULL(scan_task->parallel_parse_csv_info_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parallel parse csv info is null", K(ret));
            } else {
              scan_task->parallel_parse_csv_info_->csv_task_type_ = CsvTaskType::GAMBLING_BOUND;
            }
            OZ (granule_tasks.push_back(scan_task));
            OZ (granule_idx.push_back(task_idx++));
            OZ (granule_tablets.push_back(tablets.at(0)));
          }
        }
      } else {
        int64_t last_end_pos = 0;
        ObArray<int64_t> split_points;
        OZ (split_points.push_back(0));

        ObExternalStreamFileReader file_reader;
        const char escape = external_file_format.csv_format_.field_escaped_char_;
        OZ (file_reader.init(location, access_info,
                            external_file_format.csv_format_.compression_algorithm_,
                            allocator));
        ObSqlString full_path;
        if (!ObExternalTableUtils::is_abs_url(file_info.file_url_)) {
          OZ (full_path.append_fmt("%.*s%s%.*s", location.length(), location.ptr(),
                                    (location.empty() || location[location.length() - 1] == '/') ? "" : "/",
                                    file_info.file_url_.length(), file_info.file_url_.ptr()));
        } else {
          OZ (full_path.assign(file_info.file_url_));
        }
        OZ (file_reader.open(full_path.string()));

        for (int64_t j = 0; OB_SUCC(ret) && j < chunk_cnt - 1; ++j) {
          int64_t start_pos = last_end_pos;
          int64_t expected_end_pos = start_pos + avg_chunk_size;
          int64_t end_pos = expected_end_pos;
          // ensure the last char in the chunk is not the escape char
          OZ(ObCsvTableUtils::adjust_end_pos_skip_escape(file_reader, escape, start_pos, end_pos));
          if (OB_SUCC(ret) && end_pos > start_pos && end_pos < file_size) {
            OZ (split_points.push_back(end_pos));
            last_end_pos = end_pos;
          } else {
            last_end_pos = expected_end_pos;
          }
        }
        file_reader.close();

        int64_t actual_chunk_cnt = split_points.count();
        for (int64_t j = 0; OB_SUCC(ret) && j < actual_chunk_cnt; ++j) {
          int64_t start_pos = split_points.at(j);
          int64_t end_pos = (j == actual_chunk_cnt - 1) ? file_size : split_points.at(j + 1);
          ObExtTableScanTask *scan_task = NULL;
          if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, &allocator))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(scan_task->init_parallel_parse_csv_info(allocator))) {
            LOG_WARN("failed to init parallel parse csv info", K(ret));
          } else if (OB_FAIL(ObCsvTableUtils::make_parallel_parse_csv_task(file_info,
                                                                                1, INT64_MAX,
                                                                                start_pos, end_pos,
                                                                                j, actual_chunk_cnt,
                                                                                scan_task))) {
            LOG_WARN("failed to make parallel parse csv task", K(ret));
          } else {
            if (OB_ISNULL(scan_task->parallel_parse_csv_info_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parallel parse csv info is null", K(ret));
            } else {
              scan_task->parallel_parse_csv_info_->csv_task_type_ = CsvTaskType::GAMBLING_BOUND;
            }
            OZ (granule_tasks.push_back(scan_task));
            OZ (granule_idx.push_back(task_idx++));
            OZ (granule_tablets.push_back(tablets.at(0)));
          }
        }
      }
    } else {  // small csv file, temporarily add to ctx
      ObExtTableScanTask *scan_task = NULL;
      if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, &allocator))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to new a ptr", K(ret));
      } else if (OB_FAIL(ObExternalTableUtils::make_file_scan_task(file_info.file_url_,
                                                                   file_info.content_digest_,
                                                                   file_info.file_size_,
                                                                   file_info.modify_time_,
                                                                   file_info.file_id_,
                                                                   0,
                                                                   1, INT64_MAX,
                                                                   scan_task))) {
        LOG_WARN("failed to make normal parse csv task", K(ret));
      } else {
        GICsvTaskResult csv_task_result;
        csv_task_result.scan_task_ = scan_task;
        csv_task_result.tablet_loc_ = tablets.at(0);
        OZ (task_gen_ctx->data_scan_tasks_.push_back(csv_task_result));
      }
    }
  }
  LOG_INFO("split count, ", K(ret), K(granule_tasks.count()), K(task_gen_ctx->data_scan_tasks_.count()));
  if (OB_LOGGER.need_to_print(OB_LOG_LEVEL_TRACE)) {
    for (int64_t i = 0; i < granule_tasks.count(); i++) {
      LOG_TRACE("split detail, gambling task", K(ret), KPC(granule_tasks.at(i)));
    }
    for (int64_t i = 0; OB_NOT_NULL(task_gen_ctx) && i < task_gen_ctx->data_scan_tasks_.count(); i++) {
      LOG_TRACE("split detail, data scan task", K(ret), K(task_gen_ctx->data_scan_tasks_.at(i)));
    }
  }
  return ret;
}



int ObCsvGranuleHandler::split_granule(ObGranulePumpArgs &args,
                                       const ObTableScanSpec *tsc,
                                       const common::ObIArray<common::ObNewRange> &input_ranges,
                                       const common::ObIArray<ObDASTabletLoc *> &tablet_array,
                                       bool partition_granule,
                                       common::ObIArray<ObDASTabletLoc *> &granule_tablets,
                                       common::ObIArray<ObIExtTblScanTask*> &granule_tasks,
                                       common::ObIArray<int64_t> &granule_idx)
{
  UNUSED(partition_granule);
  UNUSED(tsc);
  int ret = OB_SUCCESS;

  sql::ObExternalFileFormat external_file_format;
  if (tablet_array.count() < 1 || OB_ISNULL(tsc) || OB_ISNULL(args.ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the invalid argument", K(ret), K(tablet_array.count()));
  } else {
    ObExecContext &exec_ctx = *args.ctx_;
    common::ObIAllocator &args_ctx_allocator = args.ctx_->get_allocator();
    const common::ObIArray<share::ObExternalFileInfo> &external_table_files =
        args.external_table_files_;
    int64_t parallelism = args.parallelism_;
    if (OB_UNLIKELY(parallelism <= 0)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("the parallelism is invalid", KR(ret), K(parallelism));
    } else if (OB_FAIL(external_file_format.load_from_string(
                   tsc->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_,
                   args_ctx_allocator))) {
      LOG_WARN("failed to load from string", K(ret),
               K(tsc->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_));
    } else if (OB_UNLIKELY(input_ranges.empty())) {
      // always false range
      ObExtTableScanTask *scan_task = NULL;
      if (OB_ISNULL(scan_task =
                        OB_NEWx(ObExtTableScanTask, (&args_ctx_allocator)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to new a ptr", K(ret));
      } else if (OB_FAIL(
                     ObExternalTableUtils::convert_external_table_empty_task(
                         ObExternalTableUtils::dummy_file_name(),
                         ObString(""), // content_digest
                         0,            // file_size
                         0,            // modify_time
                         0,            // file_id
                         0,            // ref_table_id
                         args_ctx_allocator, scan_task))) {
        LOG_WARN("failed to convert external table empty task", K(ret));
      } else if (OB_FAIL(granule_tasks.push_back(scan_task)) ||
                 OB_FAIL(granule_idx.push_back(0)) ||
                 OB_FAIL(granule_tablets.push_back(tablet_array.at(0)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (external_table_files.count() == 1 &&
               external_table_files.at(0).file_id_ == INT64_MAX) {
      // dealing dummy file
      ObExtTableScanTask *scan_task = NULL;
      if (OB_ISNULL(scan_task =
                        OB_NEWx(ObExtTableScanTask, (&args_ctx_allocator)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to new a ptr", K(ret));
      } else if (OB_FAIL(
                     ObExternalTableUtils::convert_external_table_empty_task(
                         external_table_files.at(0).file_url_,
                         external_table_files.at(0).content_digest_,
                         external_table_files.at(0).file_size_,
                         external_table_files.at(0).modify_time_,
                         external_table_files.at(0).file_id_, 0,
                         args_ctx_allocator, scan_task))) {
        LOG_WARN("failed to convert external table empty range", K(ret));
      } else if (OB_FAIL(granule_tasks.push_back(scan_task)) ||
                 OB_FAIL(granule_idx.push_back(
                     external_table_files.at(0).file_id_)) ||
                 OB_FAIL(granule_tablets.push_back(tablet_array.at(0)))) {
        LOG_WARN("fail to push back", K(ret));
      }
    } else if (args.gi_op_id_ != OB_INVALID_ID
             && ObExternalFileFormat::CSV_FORMAT == external_file_format.format_type_
             && ObCsvTableUtils::is_satisfied_for_parallel_parse_csv(
                    external_table_files, external_file_format.csv_format_, parallelism)) {
    if (OB_FAIL(split_granule_for_parallel_resolve_csv_for_range_prepare(
                                                       exec_ctx, args_ctx_allocator,
                                                       tsc->tsc_ctdef_.scan_ctdef_.external_file_location_.str_,
                                                       tsc->tsc_ctdef_.scan_ctdef_.external_file_access_info_.str_,
                                                       tsc->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_,
                                                       parallelism, tsc->get_id(), args.gi_op_id_,
                                                       external_file_format.csv_format_.parallel_parse_file_size_threshold_,
                                                       tablet_array, external_table_files,
                                                       granule_tablets, granule_tasks,
                                                       granule_idx))) {
      LOG_WARN("failed to split granule for parallel resolve csv", K(ret));
    }
  } else {
      bool is_kafka_format = (ObExternalFileFormat::KAFKA_FORMAT == external_file_format.format_type_);
      int64_t files_per_worker = (external_table_files.count() + parallelism - 1) / parallelism;

      for (int64_t i = 0; OB_SUCC(ret) && i < input_ranges.count(); ++i) {
        for (int64_t j = 0; OB_SUCC(ret) && j < external_table_files.count();
             ++j) {
          ObExtTableScanTask *scan_task = NULL;
          bool is_valid = false;
          int64_t granule_idx_to_use = 0;
          if (is_kafka_format) {
            //We want to aggregate some files so that a single PX worker thread can process them.
            granule_idx_to_use = j / files_per_worker;
          } else {
            granule_idx_to_use = external_table_files.at(j).file_id_;
          }

          if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask,
                                            (&args_ctx_allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(
                         ObExternalTableUtils::convert_external_table_scan_task(
                             external_table_files.at(j).file_url_,
                             external_table_files.at(j).content_digest_,
                             external_table_files.at(j).file_size_,
                             external_table_files.at(j).modify_time_,
                             external_table_files.at(j).file_id_,
                             external_table_files.at(j).part_id_,
                             input_ranges.at(i), args_ctx_allocator, scan_task,
                             is_valid))) {
            LOG_WARN("failed to convert external table new range", K(ret));
          } else if (is_valid &&
                     (OB_FAIL(granule_tasks.push_back(scan_task)) ||
                      OB_FAIL(granule_idx.push_back(granule_idx_to_use)) ||
                      OB_FAIL(granule_tablets.push_back(tablet_array.at(0))))) {
            LOG_WARN("fail to push back", K(ret));
          }
        }
      }
      if (is_kafka_format) {
        LOG_INFO("split granule for external table", KR(ret), K(parallelism), K(granule_idx));
      }
    }
    LOG_DEBUG("check external split ranges", K(input_ranges), K(granule_tasks),
              K(external_table_files));
  }
  return ret;
}

int ObExternalTableGranuleHandlerFactory::create(common::ObIAllocator &allocator,
                                                 const ObDASScanCtDef &scan_ctdef,
                                                 ObIExternalTableGranuleHandler *&handler)
{
  int ret = OB_SUCCESS;
  handler = nullptr;
  if (scan_ctdef.is_ob_external_table()) {
    handler = OB_NEWx(ObCsvGranuleHandler, &allocator);
  } else if (scan_ctdef.is_lake_external_table()
             && share::is_odps_lake_table(scan_ctdef.lake_table_format_)) {
    handler = OB_NEWx(ObOdpsGranuleHandler, &allocator);
  } else if (scan_ctdef.is_lake_external_table()) {
    handler = OB_NEWx(ObLakeGranuleHandler, &allocator);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("not an external table scan", K(ret), K(scan_ctdef.is_ob_external_table()),
             K(scan_ctdef.is_lake_external_table()), K(scan_ctdef.lake_table_format_));
  }
  if (OB_SUCC(ret) && OB_ISNULL(handler)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate granule handler", K(ret));
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

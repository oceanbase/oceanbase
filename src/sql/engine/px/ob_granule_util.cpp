/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SQL_EXE

#include "ob_granule_util.h"
#include "src/sql/engine/px/ob_dfo.h"
#include "share/external_table/ob_external_table_utils.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/das/ob_das_simple_op.h"
#include "src/sql/engine/px/ob_granule_iterator_op.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
namespace oceanbase
{
namespace sql
{


void ObParallelBlockRangeTaskParams::reset()
{
  parallelism_ = 0;
  expected_task_load_ = sql::OB_EXPECTED_TASK_LOAD;
  min_task_count_per_thread_ = sql::OB_MIN_PARALLEL_TASK_COUNT;
  max_task_count_per_thread_ = sql::OB_MAX_PARALLEL_TASK_COUNT;
  min_task_access_size_ = GCONF.px_task_size >> 10;
}

int ObParallelBlockRangeTaskParams::valid() const
{
  int ret = OB_SUCCESS;
  if (min_task_count_per_thread_ <= 0
      || max_task_count_per_thread_ <= 0
      || min_task_access_size_ <= 0
      || parallelism_ <= 0
      || expected_task_load_ <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("params is invalid", K(*this), K(ret));
  }
  return ret;
}

int ObGranuleUtil::use_partition_granule(ObGranulePumpArgs &args, bool &partition_granule)
{
  int ret = OB_SUCCESS;
  partition_granule = false;
  const ObGranuleIteratorSpec *gi_op = args.op_info_.gi_op_;
  const ObIArray<const ObTableScanSpec *> &scan_ops = args.op_info_.get_scan_ops();
  int64_t partition_count = args.tablet_arrays_.at(0).count();
  const ObExecContext *exec_ctx = args.ctx_;
  ObSQLSessionInfo *session_info = nullptr;
  int64_t partition_scan_hold = 0;
  int64_t hash_partition_scan_hold = 0;
  bool hash_part = false;
  if (OB_UNLIKELY(scan_ops.count() != 1 || args.tablet_arrays_.count() != 1)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected count", K(scan_ops.count()), K(args.tablet_arrays_.count()));
  } else if (OB_ISNULL(session_info = exec_ctx->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Get unexpected null");
  } else if (gi_op->get_phy_plan()->get_min_cluster_version() > CLUSTER_VERSION_4_4_1_0) {
    hash_part = gi_op->hash_part_;
  } else {
    // for compaction, plan generated in lower version has not set hash_part_ flag
    uint64_t ref_table_id = scan_ops.at(0)->get_ref_table_id();
    int64_t tenant_id = exec_ctx->get_my_session()->get_effective_tenant_id();
    const ObTableSchema *table_schema = nullptr;
    ObSchemaGetterGuard schema_guard;
    if (OB_FAIL(GCTX.schema_service_->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("Failed to get schema guard", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, ref_table_id, table_schema))) {
      LOG_WARN("Failed to get table schema", K(ret), K(ref_table_id));
    } else if (OB_ISNULL(table_schema)) {
      // may be fake table, skip
    } else {
      hash_part = table_schema->is_hash_part() || table_schema->is_hash_subpart()
                  || table_schema->is_key_part() || table_schema->is_key_subpart();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR__PX_PARTITION_SCAN_THRESHOLD,
                                                    partition_scan_hold))) {
    LOG_WARN("failed to get sys variable px partition scan threshold", K(ret));
  } else if (OB_FAIL(session_info->get_sys_variable(share::SYS_VAR__PX_MIN_GRANULES_PER_SLAVE,
                                                    hash_partition_scan_hold))) {
    LOG_WARN("failed to get sys variable px min granule per slave", K(ret));
  } else if (scan_ops.at(0)->is_external_table_) {
    partition_granule = false;
  } else {
    partition_granule = ObGranuleUtil::use_partition_granule(args.tablet_arrays_.at(0).count(),
                                                             args.parallelism_, partition_scan_hold,
                                                             hash_partition_scan_hold, hash_part);
  }
  return ret;
}

bool ObGranuleUtil::use_partition_granule(int64_t partition_count,
                                         int64_t parallelism,
                                         int64_t partition_scan_hold,
                                         int64_t hash_partition_scan_hold,
                                         bool hash_part)
{
  bool partition_granule = false;
  // if parallelism is too small, we use partition granule.
  if (hash_part) {
    partition_granule = partition_count >= hash_partition_scan_hold * parallelism || 1 == parallelism;
  } else {
    partition_granule = partition_count >= partition_scan_hold * parallelism || 1 == parallelism;
  }
  return partition_granule;
}

int ObGranuleUtil::split_granule_by_partition_line_tunnel(ObIAllocator &allocator, const ObIArray<ObDASTabletLoc *> &tablets,
    const ObIArray<ObExternalFileInfo> &external_table_files, ObIArray<ObDASTabletLoc *> &granule_tablets,
    ObIArray<ObIExtTblScanTask*> &granule_tasks, ObIArray<int64_t> &granule_idx)
{
  int ret = OB_SUCCESS;
  int64_t task_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
    const ObExternalFileInfo &external_info = external_table_files.at(i);
    ObOdpsScanTask *scan_task = NULL;
    if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, (&allocator)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    }
    int64_t file_start = external_info.row_count_ != 0 ? external_info.row_start_ : 0;
    int64_t file_end = 0;
    if (external_info.row_count_ == INT64_MAX) {
      file_end = INT64_MAX;
    } else if (external_info.row_count_ == 0) {
      file_end = external_info.file_size_ > 0 ? external_info.file_size_ : INT64_MAX;
    } else {
      file_end = file_start + external_info.row_count_;
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(external_info.file_url_,
            external_info.part_id_,
            file_start,
            file_end,
            ObString::make_empty_string(),
            0,
            0,
            *scan_task))) {
      LOG_WARN("failed to make external table scan range", K(ret));
    } else if ((OB_FAIL(granule_tasks.push_back(scan_task)) || OB_FAIL(granule_idx.push_back(task_idx++)) ||
                   OB_FAIL(granule_tablets.push_back(tablets.at(0))))) {
      LOG_WARN("fail to push back", K(ret));
    }
  }
  return ret;
}

int ObGranuleUtil::split_granule_by_total_byte(ObIAllocator &allocator, int64_t parallelism, const ObIArray<ObDASTabletLoc *> &tablets,
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
    } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(part_str,  // file 实际上不需要
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

int ObGranuleUtil::split_granule_by_total_row(ObIAllocator &allocator, int64_t parallelism, const ObIArray<ObDASTabletLoc *> &tablets,
    const ObIArray<ObExternalFileInfo> &external_table_files, ObIArray<ObDASTabletLoc *> &granule_tablets,
    ObIArray<ObIExtTblScanTask*> &granule_tasks, ObIArray<int64_t> &granule_idx)
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
    } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(part_str,  // session has kown partition
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
      OZ(granule_idx.push_back(task_idx++));
      OZ(granule_tablets.push_back(tablets.at(0)));
    }
  }
  return ret;
}
int ObGranuleUtil::split_granule_for_external_table(ObIAllocator &allocator,
                                                    const ObTableScanSpec *tsc,
                                                    const ObIArray<ObNewRange> &ranges,
                                                    const ObIArray<ObDASTabletLoc *> &tablets,
                                                    const ObIArray<ObExternalFileInfo> &external_table_files,
                                                    int64_t parallelism,
                                                    ObIArray<ObDASTabletLoc *> &granule_tablets,
                                                    ObIArray<ObIExtTblScanTask*> &granule_tasks,
                                                    ObIArray<int64_t> &granule_idx)
{
  UNUSED(tsc);
  int ret = OB_SUCCESS;
  sql::ObExternalFileFormat external_file_format;
  if (tablets.count() < 1 || OB_ISNULL(tsc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the invalid argument", K(ret), K(tablets.count()));
  } else if (OB_FAIL(external_file_format.load_from_string(tsc->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_, allocator))) {
    LOG_WARN("failed to load from string", K(ret), K(tsc->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_));
  } else if (OB_UNLIKELY(ranges.empty())) {
    // always false range
    ObExtTableScanTask *scan_task = NULL;
    if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, (&allocator)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_empty_task(
                                                              ObExternalTableUtils::dummy_file_name(),
                                                              ObString(""), // content_digest
                                                              0, // file_size
                                                              0, // modify_time
                                                              0, // file_id
                                                              0, // ref_table_id
                                                              allocator,
                                                              scan_task))) {
      LOG_WARN("failed to convert external table empty task", K(ret));
    } else if (OB_FAIL(granule_tasks.push_back(scan_task)) ||
               OB_FAIL(granule_idx.push_back(0)) ||
               OB_FAIL(granule_tablets.push_back(tablets.at(0)))) {
      LOG_WARN("fail to push back", K(ret));
    }
  } else if (external_table_files.count() == 1 &&
             external_table_files.at(0).file_id_ == INT64_MAX) {
    // dealing dummy file
    ObExtTableScanTask *scan_task = NULL;
    if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, (&allocator)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_empty_task(
                                                        external_table_files.at(0).file_url_,
                                                        external_table_files.at(0).content_digest_,
                                                        external_table_files.at(0).file_size_,
                                                        external_table_files.at(0).modify_time_,
                                                        external_table_files.at(0).file_id_,
                                                        0,
                                                        allocator,
                                                        scan_task))) {
      LOG_WARN("failed to convert external table empty range", K(ret));
    } else if (OB_FAIL(granule_tasks.push_back(scan_task)) ||
               OB_FAIL(granule_idx.push_back(external_table_files.at(0).file_id_)) ||
               OB_FAIL(granule_tablets.push_back(tablets.at(0)))) {
      LOG_WARN("fail to push back", K(ret));
    }
  } else if (!external_table_files.empty() &&
             ObExternalFileFormat::ODPS_FORMAT == external_file_format.format_type_) {
    LOG_TRACE("odps external table granule switch", K(ret), K(external_table_files.count()), K(external_table_files));
    if (!GCONF._use_odps_jni_connector) {
#if defined(OB_BUILD_CPP_ODPS)
      if (OB_FAIL(split_granule_by_partition_line_tunnel(allocator, tablets, external_table_files, granule_tablets, granule_tasks, granule_idx))) {
        LOG_WARN("failed to split granule by partition line", K(ret));
      }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
    LOG_WARN("not support odps table in opensource", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (external_file_format.odps_format_.api_mode_ == ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
         // tunnel api
        if (OB_FAIL(split_granule_by_partition_line_tunnel(
                allocator, tablets, external_table_files, granule_tablets, granule_tasks, granule_idx))) {
          LOG_WARN("failed to split granule by partition line", K(ret));
        }
      } else {
        if (external_file_format.odps_format_.api_mode_ == ObODPSGeneralFormat::ApiMode::BYTE) {
          if (OB_FAIL(split_granule_by_total_byte(allocator, parallelism, tablets, external_table_files, granule_tablets, granule_tasks, granule_idx))) {
            LOG_WARN("failed to split granule by total byte", K(ret));
          }
        } else {
          if (OB_FAIL(split_granule_by_total_row(allocator, parallelism, tablets, external_table_files, granule_tablets, granule_tasks, granule_idx))) {
            LOG_WARN("failed to split granule by total row", K(ret));
          }
        }
      }
#else
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
    LOG_WARN("not support odps table in opensource", K(ret));
#endif
    }

  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ranges.count(); ++i) {
      for (int64_t j = 0; OB_SUCC(ret) && j < external_table_files.count(); ++j) {
        ObExtTableScanTask *scan_task = NULL;
        bool is_valid = false;
        if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, (&allocator)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to new a ptr", K(ret));
        } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_scan_task(
                                                              external_table_files.at(j).file_url_,
                                                              external_table_files.at(j).content_digest_,
                                                              external_table_files.at(j).file_size_,
                                                              external_table_files.at(j).modify_time_,
                                                              external_table_files.at(j).file_id_,
                                                              external_table_files.at(j).part_id_,
                                                              ranges.at(i),
                                                              allocator,
                                                              scan_task,
                                                              is_valid))) {
          LOG_WARN("failed to convert external table new range", K(ret));
        } else if (is_valid && (OB_FAIL(granule_tasks.push_back(scan_task)) ||
                                OB_FAIL(granule_idx.push_back(external_table_files.at(j).file_id_)) ||
                                OB_FAIL(granule_tablets.push_back(tablets.at(0))))) {
          LOG_WARN("fail to push back", K(ret));
        }
      }
    }
  }
  LOG_DEBUG("check external split ranges", K(ranges), K(granule_tasks), K(external_table_files));
  return ret;
}

int ObGranuleUtil::split_granule_for_lake_table(ObExecContext &exec_ctx,
                                                ObIAllocator &allocator,
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
    ObExtTableScanTask *scan_task = NULL;
    if (OB_UNLIKELY(tablets.count() != 1) || OB_ISNULL(tablets.at(0))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected tablets counts", K(tablets));
    } else if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, (&allocator)))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to new a ptr", K(ret));
    } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_empty_task(ObExternalTableUtils::dummy_file_name(),
                                                                          ObString(""), // content_digest
                                                                          0, // file_size
                                                                          0, // modify_time
                                                                          0, // file_id
                                                                          tablets.at(0)->partition_id_, // ref_table_id
                                                                          allocator,
                                                                          scan_task))) {
      LOG_WARN("failed to make lake table scan task");
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
        ObHiveScanTask *dummy_file = nullptr;
        if (OB_UNLIKELY(tablets.count() != 1)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get unexpected tablets counts", K(tablets));
        } else if (OB_ISNULL(dummy_file = OB_NEWx(ObHiveScanTask, (&allocator)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for ObFileScanTask");
        } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_empty_task(
                                                            ObExternalTableUtils::dummy_file_name(),
                                                            ObString(""), // content_digest
                                                            0, // file_size
                                                            0, // modify_time
                                                            0, // file_id
                                                            tablet_loc->partition_id_, // ref_table_id
                                                            allocator,
                                                            dummy_file))) {
          LOG_WARN("failed to convert external table empty task", K(ret));
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
          } else if (OB_FAIL(ObExternalTableUtils::convert_lake_table_scan_task(
                         j,
                         tablet_loc->partition_id_,
                         scan_task))) {
            LOG_WARN("failed to convert lake table scan task", K(ret));
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

int ObGranuleUtil::split_block_ranges(ObExecContext &exec_ctx,
                                      ObIAllocator &allocator,
                                      const ObTableScanSpec *tsc,//may be is null, attention use
                                      const ObIArray<common::ObNewRange> &in_ranges,
                                      const ObIArray<ObDASTabletLoc*> &tablets,
                                      int64_t parallelism,
                                      int64_t tablet_size,
                                      bool force_partition_granule,
                                      common::ObIArray<ObDASTabletLoc*> &granule_tablets,
                                      common::ObIArray<common::ObNewRange> &granule_ranges,
                                      common::ObIArray<int64_t> &granule_idx,
                                      bool range_independent)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObNewRange, 16> ranges;
  bool only_empty_range = false;

  /**
   * prepare
   */
  if (tablets.count() <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ranges/tablets is empty", K(in_ranges), K(tablets), K(ret));
  } else if (in_ranges.empty()) {
    int64_t pk_idx = 0;
    FOREACH_CNT_X(tablet, tablets, OB_SUCC(ret)) {
      if (OB_FAIL(granule_tablets.push_back(*tablet))) {
        LOG_WARN("push basck tablet failed", K(ret));
      } else if (OB_FAIL(granule_idx.push_back(pk_idx))) {
        LOG_WARN("push back pk_idx failed", K(ret));
      } else {
        pk_idx++;
      }
    }
    LOG_TRACE("always false range gi partition granule");
  } else if (OB_FAIL(remove_empty_range(in_ranges, ranges, only_empty_range))) {
    LOG_WARN("failed to remove empty range", K(ret));
  } else if (force_partition_granule
             || only_empty_range) {
    // partition granule iterator
    // 按照partition粒度切分任务的情况下，任务的个数等于partition的个数（`tablets.count()`)
    int64_t pk_idx = 0;
    FOREACH_CNT_X(tablet, tablets, OB_SUCC(ret)) {
      FOREACH_CNT_X(range, ranges, OB_SUCC(ret)) {
        if (OB_FAIL(granule_tablets.push_back(*tablet))) {
          LOG_WARN("push basck tablet failed", K(ret));
        } else if (OB_FAIL(granule_ranges.push_back(*range))) {
          LOG_WARN("push back range failed", K(ret));
        } else if (OB_FAIL(granule_idx.push_back(pk_idx))) {
          LOG_WARN("push back pk_idx failed", K(ret));
        } else if (range_independent) {
          pk_idx++;
        }
      }
      if (!range_independent) {
        pk_idx++;
      }
    }
    LOG_TRACE("gi partition granule", K(range_independent));
  } else if (OB_FAIL(split_block_granule(exec_ctx,
                                         allocator,
                                         tsc,
                                         ranges,
                                         tablets,
                                         parallelism,
                                         tablet_size,
                                         granule_tablets,
                                         granule_ranges,
                                         granule_idx,
                                         range_independent))) {
    LOG_WARN("failed to split block granule tasks", K(ret));
  } else {
    LOG_TRACE("get the splited results through the new gi split method",
      K(ret), K(granule_tablets.count()), K(granule_ranges.count()), K(granule_idx));
  }
  return ret;
}

int ObGranuleUtil::remove_empty_range(const common::ObIArray<common::ObNewRange> &in_ranges,
                                      common::ObIArray<common::ObNewRange> &ranges,
                                      bool &only_empty_range) {
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < in_ranges.count() && OB_SUCC(ret); ++i) {
    if (!in_ranges.at(i).empty()) {
      if (OB_FAIL(ranges.push_back(in_ranges.at(i)))) {
        LOG_WARN("fail to push back ranges", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && ranges.empty()) {
    if (OB_FAIL(ranges.assign(in_ranges))) {
      LOG_WARN("failed to assign ranges", K(ret));
    } else {
      only_empty_range = true;
    }
  }
  return ret;
}

int ObGranuleUtil::split_block_granule(ObExecContext &exec_ctx,
                                      ObIAllocator &allocator,
                                      const ObTableScanSpec *tsc,//may be is null, attention use!
                                      const ObIArray<ObNewRange> &input_ranges,
                                      const ObIArray<ObDASTabletLoc*> &tablets,
                                      int64_t parallelism,
                                      int64_t tablet_size,
                                      ObIArray<ObDASTabletLoc*> &granule_tablets,
                                      ObIArray<ObNewRange> &granule_ranges,
                                      ObIArray<int64_t> &granule_idx,
                                      bool range_independent)
{
  //  the step for split task by block granule method:
  //  1. check the validity of input parameters
  //  2. get size for each partition, and calc the total size for all partitions
  //  3. calculate the total number of tasks
  //  4. each partition gets its number of tasks by the weight of partition data in the total data
  //  5. calculate task ranges for each partition, and get the result

  int ret = OB_SUCCESS;
  ObAccessService *access_service = MTL(ObAccessService *);
  // 1. check the validity of input parameters
  if (input_ranges.count() < 1 || tablets.count() < 1 || parallelism < 1 || tablet_size < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the invalid argument",
      K(ret), K(input_ranges.count()), K(tablets.count()), K(parallelism), K(tablet_size));
  }

  // 2. get size for each partition, and calc the total size for all partitions
  common::ObSEArray<int64_t, 16> size_each_partitions;
  int64_t total_size = 0;
  int64_t empty_partition_cnt = 0;
  ObSEArray<ObStoreRange, 16> input_store_ranges;
  bool need_convert_new_range = true;//only rowid range need extra convert.
  if (OB_SUCC(ret)) {
    for (int i = 0; i < tablets.count() && OB_SUCC(ret); i++) {
      const ObDASTabletLoc &tablet = *tablets.at(i);
      int64_t partition_size = 0;
      // get partition size from storage
      if (need_convert_new_range &&
          OB_FAIL(convert_new_range_to_store_range(allocator,
                                                   tsc,
                                                   tablet.tablet_id_,
                                                   input_ranges,
                                                   input_store_ranges,
                                                   need_convert_new_range))) {
        LOG_WARN("failed to convert new range to store range", K(ret));
      } else if (OB_FAIL(ObDASSimpleUtils::get_multi_ranges_cost(exec_ctx, tablets.at(i),
                                                                 input_store_ranges,
                                                                 partition_size))) {
        LOG_WARN("failed to get multi ranges cost", K(ret), K(tablet));
      } else {
        // B to KB
        partition_size = partition_size / 1024;
        LOG_TRACE("print partition_size", K(partition_size), "tsc_op_id",
                  tsc == nullptr ? 0 : tsc->get_id());
      }

      if (OB_SUCC(ret)) {
        if (partition_size == 0) {
          empty_partition_cnt++;
        }
        if (OB_FAIL(size_each_partitions.push_back(partition_size))) {
          LOG_WARN("failed to push partition size", K(ret));
        } else {
          total_size += partition_size;
        }
      }
    }
    LOG_TRACE("get multi ranges cost", K(empty_partition_cnt), K(size_each_partitions));
  }

  // 3. calc the total number of tasks for all partitions
  int64_t esti_task_cnt_by_data_size = 0;
  if (OB_SUCC(ret)) {
    ObParallelBlockRangeTaskParams params;
    params.parallelism_ = parallelism;
    params.expected_task_load_ = tablet_size/1024;
    if (OB_FAIL(compute_total_task_count(params, total_size, esti_task_cnt_by_data_size))) {
      LOG_WARN("compute task count failed", K(ret));
    } else {
      esti_task_cnt_by_data_size += empty_partition_cnt;
      // 确保total task count是大于等于partition的个数的
      if (esti_task_cnt_by_data_size < tablets.count()) {
        esti_task_cnt_by_data_size = tablets.count();
      }
    }
  }

  // 4. split the total number of tasks into each partition
  common::ObSEArray<int64_t, 16> task_cnt_each_partitions;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(compute_task_count_each_partition(total_size,
                                                  esti_task_cnt_by_data_size,
                                                  size_each_partitions,
                                                  task_cnt_each_partitions))) {
      LOG_WARN("failed to compute task count for each partition", K(ret));
    }
  }

  // 5. calc task ranges for each partition, and get the result
  if (OB_SUCC(ret)) {
    int64_t tablet_idx = 0;
    for (int i = 0; i < tablets.count() && OB_SUCC(ret); i++) {
      ObDASTabletLoc *tablet = tablets.at(i);
      int64_t expected_task_cnt = task_cnt_each_partitions.at(i);
      // split input ranges to n task by PG interface
      if (need_convert_new_range &&
          OB_FAIL(convert_new_range_to_store_range(allocator,
                                                   tsc,
                                                   tablet->tablet_id_,
                                                   input_ranges,
                                                   input_store_ranges,
                                                   need_convert_new_range))) {
        LOG_WARN("failed to convert new range to store range", K(ret));
      } else if (OB_FAIL(get_tasks_for_partition(exec_ctx,
                                                 allocator,
                                                 expected_task_cnt,
                                                 *tablet,
                                                 input_store_ranges,
                                                 granule_tablets,
                                                 granule_ranges,
                                                 granule_idx,
                                                 tablet_idx,
                                                 range_independent))) {
        LOG_WARN("failed to get tasks for partition", K(ret));
      } else {
        LOG_TRACE("get tasks for partition",
          K(ret), KPC(tablet), K(granule_ranges.count()), K(granule_tablets), K(granule_idx));
      }
    }
    if (OB_SUCC(ret)) {
      if (granule_tablets.empty() ||
          granule_tablets.count() != granule_ranges.count() ||
          granule_tablets.count() != granule_idx.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the ranges or offsets are empty", K(ret), K(granule_tablets.count()),  K(granule_ranges.count()),
                                      K(granule_idx.count()), K(granule_tablets), K(granule_ranges), K(granule_idx));
      }
    }
  }
  return ret;
}

int ObGranuleUtil::compute_total_task_count(const ObParallelBlockRangeTaskParams &params,
                                      int64_t total_size,
                                      int64_t &total_task_count)
{
  int ret = OB_SUCCESS;
  int64_t tmp_total_task_count = -1;
  if (OB_FAIL(params.valid())) {
    LOG_WARN("params is invalid" , K(ret));
  } else {
    // total size
    int64_t total_access_size = total_size;
    // default value is 2 MB
    int64_t min_task_access_size = NON_ZERO_VALUE(params.min_task_access_size_);
    // default value of expected_task_load_ is 128 MB
    int64_t expected_task_load = max(params.expected_task_load_, min_task_access_size);

    LOG_TRACE("compute task count: ", K(total_access_size), K(expected_task_load));

    // lower bound size: dop*128M*13
    int64_t lower_bound_size = params.parallelism_ * expected_task_load * params.min_task_count_per_thread_;
    // hight bound size: dop*128M*100
    int64_t upper_bound_size = params.parallelism_ * expected_task_load * params.max_task_count_per_thread_;

    if (total_access_size < 0 || lower_bound_size < 0 || upper_bound_size < 0 ) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("params is invalid",
        K(total_access_size), K(lower_bound_size), K(upper_bound_size), K(params));
    } else if (total_access_size < lower_bound_size) {
      // the data size is less than lower bound size
      // when the amount of data is small,
      // more tasks can easily achieve better dynamic load balancing
      tmp_total_task_count = min(params.min_task_count_per_thread_ * params.parallelism_,
                                 total_access_size/min_task_access_size);
      tmp_total_task_count = max(tmp_total_task_count, total_access_size / expected_task_load);
      LOG_TRACE("the data is less than lower bound size", K(ret), K(tmp_total_task_count),
                K(total_size), K(params));
    } else if (total_access_size > upper_bound_size) {
      // the data size is greater than upper bound size
      tmp_total_task_count = params.max_task_count_per_thread_ * params.parallelism_;
      LOG_TRACE("the data size is greater upper bound size", K(ret), K(tmp_total_task_count),
                K(total_size), K(params));
    } else {
      // the data size is between lower bound size and upper bound size
      tmp_total_task_count = total_access_size / expected_task_load;
      LOG_TRACE("the data size is between lower bound size and upper bound size",
        K(ret), K(tmp_total_task_count), K(total_size), K(params));
    }
  }
  if (OB_SUCC(ret)) {
    // the result of task count must be greater than or equal to zero
    total_task_count = tmp_total_task_count;
  }
  return ret;
}

int ObGranuleUtil::compute_task_count_each_partition(int64_t total_size,
                                                     int64_t total_task_cnt,
                                              const common::ObIArray<int64_t> &size_each_partition,
                                              common::ObIArray<int64_t> &task_cnt_each_partition)
{
  int ret = OB_SUCCESS;
  // must ensure at least one task per partition.
  if (total_size <=0 || total_task_cnt == size_each_partition.count()) {
    // if the total count of tasks is equal to the number of partitions,
    // each partition just has one task.
    for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
      // only one task for each partition
      if (OB_FAIL(task_cnt_each_partition.push_back(1))) {
        LOG_WARN("failed to push back array", K(ret));
      }
    }
    LOG_TRACE("compute task count for each partition, each partition has only one task", K(ret));
  } else {
    // allocate task count for each partition by the weight of partition data in the total data
    int64_t alloc_task_cnt = 0;
    for (int i = 0; i < size_each_partition.count() && OB_SUCC(ret); i++) {
      int64_t partition_size = size_each_partition.at(i);
      int64_t task_cnt = ((double) partition_size / (double) total_size) * total_task_cnt;
      // if the data volume of a partition is very small, but it still needs a task.
      if (task_cnt == 0) {
        task_cnt = 1;
      }
      alloc_task_cnt += task_cnt;
      if (OB_FAIL(task_cnt_each_partition.push_back(task_cnt))) {
        LOG_WARN("failed to push task cnt", K(ret));
      }
    }
    LOG_TRACE("compute task count for partition, allocate task count",
      K(ret), K(alloc_task_cnt), K(total_task_cnt));
  }
  // check the size of task_cnt_each_partition array
  if (OB_SUCC(ret) && task_cnt_each_partition.count() != size_each_partition.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("the size of task count each partition is not right",
      K(ret), K(size_each_partition.count()), K(task_cnt_each_partition.count()));
  }
  // check the returned result
  for (int i = 0; i < task_cnt_each_partition.count() && OB_SUCC(ret); i++) {
    if (task_cnt_each_partition.at(i) < 1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("the partition has error task number", K(ret), K(task_cnt_each_partition.at(i)));
    }
  }

  return ret;
}

int ObGranuleUtil::get_tasks_for_partition(ObExecContext &exec_ctx,
                                           ObIAllocator &allocator,
                                           int64_t expected_task_cnt,
                                           ObDASTabletLoc &tablet,
                                           ObIArray<ObStoreRange> &input_storage_ranges,
                                           common::ObIArray<ObDASTabletLoc*> &granule_tablets,
                                           common::ObIArray<common::ObNewRange> &granule_ranges,
                                           common::ObIArray<int64_t> &granule_idx,
                                           int64_t &tablet_idx,
                                           bool range_independent)
{
  int ret = OB_SUCCESS;
  ObAccessService *access_service = MTL(ObAccessService *);
  ObArrayArray<ObStoreRange> multi_range_split_array;
  if (expected_task_cnt < 1) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(expected_task_cnt));
  } else if (expected_task_cnt == 1) {
    // no need to split the input_ranges, if the expected count of task.
    for (int i = 0; i < input_storage_ranges.count() && OB_SUCC(ret); i++) {
      ObNewRange new_range;
      input_storage_ranges.at(i).to_new_range(new_range);
      if (OB_FAIL(granule_tablets.push_back(&tablet))) {
        LOG_WARN("failed to push back tablet", K(ret));
      } else if (OB_FAIL(granule_ranges.push_back(new_range))) {
        LOG_WARN("failed to push back range", K(ret));
      } else if (OB_FAIL(granule_idx.push_back(tablet_idx))) {
        LOG_WARN("failed to push back idx", K(ret));
      } else if (range_independent) {
        tablet_idx++;
      }
    }
    if (!range_independent) {
      tablet_idx++;
    }
  } else if (OB_FAIL(ObDASSimpleUtils::split_multi_ranges(exec_ctx,
                                                          &tablet,
                                                          input_storage_ranges,
                                                          expected_task_cnt,
                                                          multi_range_split_array))) {
    LOG_WARN("failed to split multi ranges", K(ret), K(tablet), K(expected_task_cnt));
  } else {
    LOG_TRACE("split multi ranges",
      K(ret), K(tablet), K(input_storage_ranges),
      K(expected_task_cnt == multi_range_split_array.count()), K(multi_range_split_array));
    // convert ObStoreRange array to ObNewRange array
    for (int i = 0; i < multi_range_split_array.count() && OB_SUCC(ret); i++) {
      ObIArray<ObStoreRange> &storage_task_ranges = multi_range_split_array.at(i);
      for (int j = 0; j < storage_task_ranges.count() && OB_SUCC(ret); j++) {
        ObNewRange new_range;
        storage_task_ranges.at(j).to_new_range(new_range);
        if (OB_INVALID_INDEX == new_range.table_id_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid table id", K(ret), K(new_range), K(multi_range_split_array.at(i)));
        } else if (OB_FAIL(granule_tablets.push_back(&tablet))) {
          LOG_WARN("failed to push back tablet", K(ret), K(tablet));
        } else  if (OB_FAIL(granule_ranges.push_back(new_range))) {
          LOG_WARN("failed to push back new task range", K(ret), K(new_range));
        } else if (OB_FAIL(granule_idx.push_back(tablet_idx))) {
          LOG_WARN("failed to push back idx", K(ret), K(tablet_idx));
        } else if (range_independent) {
          tablet_idx++;
        }
      }
      if (!range_independent) {
        tablet_idx++;
      }
    }
  }
  return ret;
}

int ObGranuleUtil::convert_new_range_to_store_range(ObIAllocator &allocator,
                                                    const ObTableScanSpec *tsc,
                                                    const ObTabletID &tablet_id,
                                                    const ObIArray<ObNewRange> &input_ranges,
                                                    ObIArray<ObStoreRange> &input_store_ranges,
                                                    bool &need_convert_new_range)
{
  int ret = OB_SUCCESS;
  ObStoreRange store_range;
  input_store_ranges.reuse();
  need_convert_new_range = false;
  for (int64_t i = 0; OB_SUCC(ret) && i < input_ranges.count(); i++) {
    if (input_ranges.at(i).is_physical_rowid_range_) {
      ObNewRange new_range;
      if (OB_ISNULL(tsc) || OB_UNLIKELY(tsc->get_columns_desc().count() < 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected error", K(ret), K(tsc));
      } else {
        ObArrayWrap<ObColDesc> rowkey_descs(&tsc->get_columns_desc().at(0),
                                            tsc->get_rowkey_cnt());
        if (OB_FAIL(deep_copy_range(allocator, input_ranges.at(i), new_range))) {
          LOG_WARN("failed to deep copy range", K(ret));
        } else if (OB_FAIL(ObTableScanOp::transform_physical_rowid(allocator,
                                                                   tablet_id,
                                                                   rowkey_descs,
                                                                   new_range))) {
          LOG_WARN("transform physical rowid for range failed", K(ret), K(new_range));
        } else {
          store_range.assign(new_range);
          if (OB_FAIL(input_store_ranges.push_back(store_range))) {
            LOG_WARN("failed to push back input store range", K(ret));
          } else {
            need_convert_new_range = true;
          }
        }
      }
    } else {
      store_range.assign(input_ranges.at(i));
      if (OB_FAIL(input_store_ranges.push_back(store_range))) {
        LOG_WARN("failed to push back input store range", K(ret));
      }
    }
  }
  return ret;
}

ObGranuleSplitterType ObGranuleUtil::calc_split_type(uint64_t gi_attr_flag)
{
  ObGranuleSplitterType res = GIT_UNINITIALIZED;
  if (access_all(gi_attr_flag)) {
    res = GIT_ACCESS_ALL;
  } else if (pwj_gi(gi_attr_flag) &&
             affinitize(gi_attr_flag)) {
    res = GIT_PARTITION_WISE_WITH_AFFINITY;
  } else if (affinitize(gi_attr_flag)) {
    res = GIT_AFFINITY;
  } else if (pwj_gi(gi_attr_flag)) {
    res = GIT_FULL_PARTITION_WISE;
  } else {
    res = GIT_RANDOM;
  }
  return res;
}

}
}


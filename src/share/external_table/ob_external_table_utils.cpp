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
#define USING_LOG_PREFIX SQL
#include "share/external_table/ob_external_table_utils.h"

#include "share/external_table/ob_external_table_file_rpc_processor.h"
#include "sql/executor/ob_task_spliter.h"
#include "sql/engine/table/ob_csv_table_row_iter.h"
#include "share/config/ob_server_config.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#include "plugin/interface/ob_plugin_external_intf.h"
#include "sql/engine/basic/ob_consistent_hashing_load_balancer.h"
#include "lib/restore/ob_object_device.h"
#include "src/share/ob_device_manager.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace share
{
const char *ObExternalTableUtils::dummy_file_name()
{
  return "#######DUMMY_FILE#######";
}

bool ObExternalTableUtils::is_left_edge(const ObObj &value)
{
  bool ret = false;
  bool is_oracle = lib::is_oracle_mode();
  if ((is_oracle && value.is_min_value()) ||
      (!is_oracle && (value.is_min_value() || value.is_null()))) {
    ret = true;
  }
  return ret;
}

bool ObExternalTableUtils::is_right_edge(const ObObj &value)
{
  bool ret = false;
  bool is_oracle = lib::is_oracle_mode();
  if ((is_oracle && (value.is_max_value() || value.is_null())) ||
      (!is_oracle && value.is_max_value())) {
    ret = true;
  }
  return ret;
}

int ObExternalTableUtils::is_file_id_in_ranges(const ObIArray<ObNewRange *> &range_filter,
                                               const int64_t &file_id,
                                               bool &in_ranges)
{
  int ret = OB_SUCCESS;
  in_ranges = false;
  for (int64_t i = 0; OB_SUCC(ret) && !in_ranges && i < range_filter.count(); ++i) {
    int64_t start_file_id = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_FILE_ID;
    int64_t end_file_id = INT64_MAX;
    if (OB_ISNULL(range_filter.at(i))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get Null ptr", K(ret));
    } else if (OB_FAIL(resolve_file_id_range(*range_filter.at(i), 0, start_file_id, end_file_id))) {
      LOG_WARN("failed to resolve range in external table", K(ret));
    } else if (file_id >= start_file_id && file_id <= end_file_id) {
      in_ranges = true;
    }
  }
  if (0 == file_id) {
    in_ranges = true;
  }
  return ret;
}

int64_t ObExternalTableUtils::get_edge_value(const ObObj &edge) {
  int64_t value = 1;
  if (is_left_edge(edge)) {
    // file_id and line_number are begin at 1
    value = 1;
  } else if (is_right_edge(edge)) {
    value = INT64_MAX;
  } else {
    value = edge.get_int();
  }
  return value;
}


int ObExternalTableUtils::resolve_file_id_range(const ObNewRange &range,
                                                const int64_t &column_idx,
                                                int64_t &start_file,
                                                int64_t &end_file)
{
  int ret = OB_SUCCESS;
  start_file = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_FILE_ID;
  end_file = INT64_MAX;
  if (column_idx >= range.get_start_key().get_obj_cnt() ||
      column_idx >= range.get_end_key().get_obj_cnt() ||
      column_idx < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. input column idx invalid", K(ret), K(range), K(column_idx));
  } else {
    const ObObj &start_obj = range.get_start_key().get_obj_ptr()[column_idx];
    const ObObj &end_obj = range.get_end_key().get_obj_ptr()[column_idx];
    start_file = get_edge_value(start_obj);
    end_file = get_edge_value(end_obj);
  }
  return ret;
}

int ObExternalTableUtils::resolve_line_number_range(const ObNewRange &range,
                                                    const int64_t &column_idx,
                                                    int64_t &start_lineno,
                                                    int64_t &end_lineno)
{
  int ret = OB_SUCCESS;
  start_lineno = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER;
  end_lineno = INT64_MAX;
  if (column_idx >= range.get_start_key().get_obj_cnt() ||
      column_idx >= range.get_end_key().get_obj_cnt() ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. input column idx invalid", K(ret), K(range), K(column_idx));
  } else {
    const ObObj &start_obj = range.get_start_key().get_obj_ptr()[column_idx];
    const ObObj &end_obj = range.get_end_key().get_obj_ptr()[column_idx];
    start_lineno = get_edge_value(start_obj);
    end_lineno = get_edge_value(end_obj);
    if (!is_left_edge(start_obj) && !is_right_edge(start_obj) && !range.border_flag_.inclusive_start()) {
      start_lineno++;
    }
    if (!is_left_edge(end_obj) && !is_right_edge(end_obj) && !range.border_flag_.inclusive_end()) {
      end_lineno--;
    }
  }
  return ret;
}

int ObExternalTableUtils::resolve_odps_start_step(const ObOdpsScanTask *scan_task,
  int64_t &start,
  int64_t &step)
{
  int ret = OB_SUCCESS;
  start = scan_task->first_lineno_;
  int64_t end = scan_task->last_lineno_;
  if (end != INT64_MAX) {
    step = end - start;
  } else {
    step = INT64_MAX;
  }
  return ret;
}

int ObExternalTableUtils::convert_external_table_scan_task(const ObString &file_url,
                                                           const ObString &content_digest,
                                                           const int64_t file_size,
                                                           const int64_t modify_time,
                                                           const int64_t file_id,
                                                           const uint64_t part_id,
                                                           const ObNewRange &range,
                                                           ObIAllocator &allocator,
                                                           ObFileScanTask *scan_task,
                                                           bool &is_valid)
{
  int ret = OB_SUCCESS;
  int64_t start_file_id = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_FILE_ID;
  int64_t end_file_id = INT64_MAX;
  int64_t start_lineno = ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER;
  int64_t end_lineno = INT64_MAX;
  ObObj start_obj;
  ObObj end_obj;
  is_valid = false;
  if (OB_UNLIKELY(range.get_start_key().get_obj_cnt() != 2 ||
                  range.get_end_key().get_obj_cnt() != 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. get unexpected params", K(ret), K(range));
  } else if (OB_FAIL(resolve_file_id_range(range, 0, start_file_id, end_file_id))) {
    LOG_WARN("failed to resolve range in external table", K(ret));
  } else if (file_id >= start_file_id && file_id <= end_file_id) {
    if (file_id == start_file_id) {
      start_obj = ObObj(range.get_start_key().get_obj_ptr()[1]);
    } else {
      start_obj.set_min_value();
    }
    if (file_id == end_file_id) {
      end_obj = ObObj(range.get_end_key().get_obj_ptr()[1]);
    } else {
      end_obj.set_max_value();
    }
    /* 1. line number is invalid as min:min or max:max.
     * 2. start_lineno > end_lineno. eg, (min:1)--> [1, 1) --> [1, 0]
     */
    bool start_min = is_left_edge(start_obj);
    bool start_max = is_right_edge(start_obj);
    bool end_min = is_left_edge(end_obj);
    bool end_max = is_right_edge(end_obj);
    if (!(start_min && end_min) && !(start_max && end_max)) {
      start_lineno = get_edge_value(start_obj);
      end_lineno = get_edge_value(end_obj);
      if (!start_min && !start_max && !range.border_flag_.inclusive_start()) {
        start_lineno++;
      }
      if (!end_min && !end_max && !range.border_flag_.inclusive_end()) {
        end_lineno--;
      }
      if (end_lineno >= start_lineno) {
        is_valid = true;
      }
    }
  }
  if (OB_SUCC(ret) && is_valid) {
    if (OB_FAIL(make_file_scan_task(file_url,
                                    content_digest,
                                    file_size,
                                    modify_time,
                                    file_id,
                                    part_id,
                                    start_lineno,
                                    end_lineno,
                                    scan_task))) {
      LOG_WARN("failed to make external table scan task", K(ret));
    }
  }
  return ret;
}

int ObExternalTableUtils::convert_lake_table_scan_task(const int64_t file_id,
                                                       const uint64_t part_id,
                                                       ObFileScanTask *scan_task)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(scan_task)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected null scan task", K(ret), KP(scan_task));
  } else if (scan_task->get_file_type() == LakeFileType::ICEBERG) {
    scan_task->file_id_ = file_id;
    scan_task->part_id_ = part_id;
  } else if (scan_task->get_file_type() == LakeFileType::HIVE) {
    scan_task->file_id_ = file_id;
  }
  return ret;
}

int ObExternalTableUtils::convert_external_table_empty_task(const ObString &file_url,
                                                             const ObString &content_digest,
                                                             const int64_t file_size,
                                                             const int64_t modify_time,
                                                             const int64_t file_id,
                                                             const uint64_t ref_table_id,
                                                             ObIAllocator &allocator,
                                                             ObFileScanTask *scan_task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(make_file_scan_task(file_url,
                                  content_digest,
                                  file_size,
                                  modify_time,
                                  file_id,
                                  ref_table_id,
                                  ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER,
                                  INT64_MAX,
                                  scan_task))) {
    LOG_WARN("failed to make external table scan task", K(ret));
  }
  return ret;
}

int ObExternalTableUtils::make_file_scan_task(const common::ObString &file_url,
                                              const common::ObString &content_digest,
                                              const int64_t file_size,
                                              const int64_t modify_time,
                                              const int64_t file_id,
                                              const uint64_t part_id,
                                              const int64_t first_lineno,
                                              const int64_t last_lineno,
                                              ObFileScanTask *scan_task)
{
  int ret = OB_SUCCESS;
  scan_task->file_url_ = file_url;
  scan_task->file_size_ = file_size;
  scan_task->modification_time_ = modify_time;
  scan_task->part_id_ = part_id;
  scan_task->first_lineno_ = first_lineno;
  scan_task->last_lineno_ = last_lineno;
  scan_task->file_id_ = file_id;
  scan_task->content_digest_ = content_digest;
  return ret;
}

int ObExternalTableUtils::make_odps_scan_task(const common::ObString &file_url,
                                              const uint64_t part_id,
                                              const int64_t first_lineno,
                                              const int64_t last_lineno,
                                              const common::ObString &session_id,
                                              const int64_t first_split_idx,
                                              const int64_t last_split_idx,
                                              ObOdpsScanTask &scan_task)
{
  int ret = OB_SUCCESS;
  scan_task.file_url_ = file_url;
  scan_task.part_id_ = part_id;
  scan_task.session_id_ = session_id;
  scan_task.first_split_idx_ = first_split_idx;
  scan_task.last_split_idx_ = last_split_idx;
  scan_task.first_lineno_ = first_lineno;
  scan_task.last_lineno_ = last_lineno;
  return ret;
}

int ObExternalTableUtils::prepare_single_scan_task(const uint64_t tenant_id,
                                                    const ObDASScanCtDef &das_ctdef,
                                                    ObDASScanRtDef *das_rtdef,
                                                    ObExecContext &exec_ctx,
                                                    ObIArray<int64_t> &partition_ids,
                                                    ObIArray<ObNewRange *> &ranges,
                                                    ObIAllocator &allocator,
                                                    ObIArray<ObIExtTblScanTask *> &scan_tasks,
                                                    bool is_file_on_disk,
                                                    ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ranges.empty())) {
    // always false
    scan_tasks.reset();
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
    } else if (OB_FAIL(scan_tasks.push_back(scan_task))) {
      LOG_WARN("failed to push back scan task");
    }
  } else if (OB_FAIL(prepare_single_scan_task_(tenant_id, das_ctdef, das_rtdef,exec_ctx,
                                                partition_ids, ranges, allocator,
                                                scan_tasks, is_file_on_disk, ctx))) {
    LOG_WARN("failed to prepare single scan range");
  }
  return ret;
}

int ObExternalTableUtils::prepare_single_scan_task_(const uint64_t tenant_id,
                                                     const ObDASScanCtDef &das_ctdef,
                                                     ObDASScanRtDef *das_rtdef,
                                                     ObExecContext &exec_ctx,
                                                     ObIArray<int64_t> &partition_ids,
                                                     ObIArray<ObNewRange *> &ranges,
                                                     ObIAllocator &allocator,
                                                     ObIArray<ObIExtTblScanTask *> &scan_tasks,
                                                     bool is_file_on_disk,
                                                     ObExecContext &ctx)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObExternalFileInfo, 16> file_urls;
  ObSEArray<ObNewRange *, 4> tmp_ranges;
  ObSEArray<ObAddr, 16> all_locations;
  ObExternalFileFormat::FormatType external_table_type;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_FAIL(tmp_ranges.assign(ranges))) {
    LOG_WARN("failed to assign array", K(ret));
  }
  const uint64_t table_id = das_ctdef.ref_table_id_;
  const ObString &table_format_or_properties = das_ctdef.external_file_format_str_.str_;
  if (OB_SUCC(ret)) {
    if (is_external_object_id(table_id)) {
      if (OB_FAIL(ObExternalTableFileManager::get_instance().get_mocked_external_table_files(
                                            tenant_id, partition_ids, ctx, das_ctdef, file_urls))) {
        LOG_WARN("failed to get mocked external table files", K(ret));
      }
    } else {
      if (OB_FAIL(ObExternalTableFileManager::get_instance().get_external_files_by_part_ids(
                            tenant_id, table_id, partition_ids, is_file_on_disk,
                            allocator, file_urls, tmp_ranges.empty() ? NULL : &tmp_ranges))) {
        LOG_WARN("failed to get external files by part ids", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(GCTX.location_service_->external_table_get(tenant_id, all_locations))) {
        LOG_WARN("fail to get external table location", K(ret));
    } else if (is_file_on_disk
              && OB_FAIL(ObExternalTableUtils::filter_files_in_locations(file_urls,
                                                                        all_locations))) {
        //For recovered cluster, the file addr may not in the cluster. Then igore it.
        LOG_WARN("filter files in location failed", K(ret));
    } else {
      scan_tasks.reset();
    }
  }
  bool is_odps_external_table = false;
  ObODPSGeneralFormat::ApiMode odps_api_mode;
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObSQLUtils::get_odps_api_mode(table_format_or_properties, is_odps_external_table, odps_api_mode))) {
    LOG_WARN("failed to check is odps external table or not", K(ret), K(table_format_or_properties));
  } else if (!file_urls.empty() && is_odps_external_table) {
    const ExprFixedArray &ext_file_column_expr = das_ctdef.pd_expr_spec_.ext_file_column_exprs_;
    if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
      for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
        const ObExternalFileInfo &external_info = file_urls.at(i);
        ObOdpsScanTask *scan_task = NULL;
        if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, (&allocator)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to new a ptr", K(ret));
        } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(external_info.file_url_,
                       external_info.part_id_,
                       0,
                       INT64_MAX,
                       ObString::make_string(""),
                       0,
                       0,
                       *scan_task))) {
          LOG_WARN("failed to make external table scan task", K(ret));
        } else {
          /*
           * 单机单线程每个part一个scan task
           */
          OZ(scan_tasks.push_back(scan_task));
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (odps_api_mode == ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        // tunnel api
        for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
          const ObExternalFileInfo &external_info = file_urls.at(i);
          ObOdpsScanTask *scan_task = NULL;
          if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, (&allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(external_info.file_url_,
                         external_info.part_id_,
                         0,
                         INT64_MAX,
                         ObString::make_string(""),
                         0,
                         0,
                         *scan_task))) {
            LOG_WARN("failed to make external table scan task", K(ret));
          } else {
            /*
             * 单机单线程每个part一个task
             */
            OZ(scan_tasks.push_back(scan_task));
          }
        }
      } else {
        ObSqlString part_spec_str;
        ObString part_str;
        int64_t part_count = file_urls.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
          const ObExternalFileInfo &external_info = file_urls.at(i);
          if (0 == external_info.file_url_.compare(ObExternalTableUtils::dummy_file_name())) {
            // do nothing
          } else if (OB_FAIL(part_spec_str.append(external_info.file_url_))) {
            LOG_WARN("failed to append file url", K(ret), K(external_info.file_url_));
          } else if (i < part_count - 1 && OB_FAIL(part_spec_str.append("#"))) {
            LOG_WARN("failed to append comma", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(allocator, part_spec_str.string(), part_str, true))) {
          LOG_WARN("failed to write string", K(ret), K(part_spec_str));
        } else if (odps_api_mode == ObODPSGeneralFormat::ApiMode::BYTE) {
          ObString session_str;
          int64_t split_count = 0;
          ObOdpsScanTask *scan_task = NULL;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_api_total_task(
                         exec_ctx,
                         ext_file_column_expr,
                         part_str,
                         das_ctdef,
                         das_rtdef,
                         1,
                         session_str,
                         split_count,
                         allocator))) {
            LOG_WARN("failed to make total task", K(ret));
          } else if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, &allocator))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(part_str,
                         0,  // external_info.part_id_ 原来part id会存在列中可以反解出分区列的值, 现在不需要了
                         0,
                         INT64_MAX,
                         session_str,
                         0,
                         split_count,
                         *scan_task))) {
            LOG_WARN("failed to make external table scan task", K(ret));
          } else {
            OZ(scan_tasks.push_back(scan_task));
          }
        } else {
          ObString session_str;
          int64_t total_row_count = 0;
          ObOdpsScanTask *scan_task = NULL;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_api_split_by_row(
                         exec_ctx,
                         ext_file_column_expr,
                         part_str,
                         das_ctdef,
                         das_rtdef,
                         1,
                         session_str,
                         total_row_count,
                         allocator))) {
            LOG_WARN("failed to make total task", K(ret));
          } else if (OB_ISNULL(scan_task = OB_NEWx(ObOdpsScanTask, &allocator))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::make_odps_scan_task(part_str,
                         0,  // external_info.part_id_ 原来part id会存在列中可以反解出分区列的值, 现在不需要了
                         0,
                         total_row_count,
                         session_str,
                         0,
                         0,
                         *scan_task))) {
            LOG_WARN("failed to make external table scan task", K(ret));
          } else {
            OZ(scan_tasks.push_back(scan_task));
          }
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS JNI connector is not enabled", K(ret));
#endif
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_ranges.count(); ++i) {
      if (OB_ISNULL(tmp_ranges.at(i))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected NULL ptr", K(ret));
      } else {
        for (int64_t j = 0; OB_SUCC(ret) && j < file_urls.count(); ++j) {
          ObExtTableScanTask *scan_task = NULL;
          bool is_valid = false;
          if (OB_ISNULL(scan_task = OB_NEWx(ObExtTableScanTask, (&allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_scan_task(
                                                                      file_urls.at(j).file_url_,
                                                                      file_urls.at(j).content_digest_,
                                                                      file_urls.at(j).file_size_,
                                                                      file_urls.at(j).modify_time_,
                                                                      file_urls.at(j).file_id_,
                                                                      file_urls.at(j).part_id_,
                                                                      *tmp_ranges.at(i),
                                                                      allocator,
                                                                      scan_task,
                                                                      is_valid))) {
            LOG_WARN("failed to convert external table new task", K(ret), K(file_urls.at(j)),
                     K(ranges.at(i)));
          } else if (is_valid) {
            OZ (scan_tasks.push_back(scan_task));
          }
        }
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::prepare_lake_table_single_scan_task(ObExecContext &exec_ctx,
                                                               ObDASTableLoc *tab_loc,
                                                               ObDASTabletLoc *tablet_loc,
                                                               ObIAllocator &allocator,
                                                               ObIArray<ObNewRange *> &ranges,
                                                               ObIArray<ObIExtTblScanTask *> &scan_tasks)
{
  int ret = OB_SUCCESS;
  ObLakeTableFileMap *map = nullptr;
  if (OB_ISNULL(tab_loc) || OB_ISNULL(tablet_loc) || OB_ISNULL(tablet_loc->loc_meta_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null table loc or tablet loc", KP(tab_loc), K(tablet_loc));
  } else if (OB_UNLIKELY(ranges.empty())) {
    // always false
    ObHiveScanTask * lake_file = NULL;
    if (OB_ISNULL(lake_file = OB_NEWx(ObHiveScanTask, (&allocator)))) {
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
                                                          lake_file))) {
      LOG_WARN("failed to convert external table empty task", K(ret));
    } else if (OB_FAIL(scan_tasks.push_back(lake_file))) {
      LOG_WARN("failed to push back scan task");
    }
  } else if (OB_FALSE_IT(ranges.reset())) {
  } else if (OB_FAIL(exec_ctx.get_lake_table_file_map(map))) {
    LOG_WARN("failed to get lake table file map");
  } else if (OB_ISNULL(map)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null lake talbe file map");
  } else {
    ObLakeTableFileArray* files = nullptr;
    if (OB_FAIL(map->get_refactored(ObLakeTableFileMapKey(tablet_loc->loc_meta_->table_loc_id_, tablet_loc->tablet_id_),
                                    files))) {
      LOG_WARN("failed to get refactored", K(tablet_loc->loc_meta_->table_loc_id_), K(tablet_loc->tablet_id_), KP(map), K(map->size()));
    } else if (OB_ISNULL(files)) {
      ObHiveScanTask * lake_file = NULL;
      if (OB_UNLIKELY(tab_loc->get_tablet_locs().size() != 1)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected tablets counts", K(tab_loc->get_tablet_locs()));
      } else if (OB_ISNULL(lake_file = OB_NEWx(ObHiveScanTask, (&allocator)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate memory for ObHiveScanTask");
      } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_empty_task(
                                                            ObExternalTableUtils::dummy_file_name(),
                                                            ObString(""), // content_digest
                                                            0, // file_size
                                                            0, // modify_time
                                                            0, // file_id
                                                            tablet_loc->partition_id_, // ref_table_id
                                                            allocator,
                                                            lake_file))) {
        LOG_WARN("failed to convert external table empty task", K(ret));
      } else if (OB_FAIL(scan_tasks.push_back(lake_file))) {
        LOG_WARN("failed to push back scan task");
      }
    } else {
      for (int64_t j = 0; OB_SUCC(ret) && j < files->count(); ++j) {
        sql::ObFileScanTask *scan_task = static_cast<sql::ObFileScanTask *>(files->at(j));
        if (OB_ISNULL(scan_task)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get null scan task");
        } else if (OB_FAIL(convert_lake_table_scan_task(j, tablet_loc->partition_id_, scan_task))) {
          LOG_WARN("failed to convert lake table scan task", K(ret));
        } else if (OB_FAIL(scan_tasks.push_back(scan_task))) {
          LOG_WARN("failed to push back scan task");
        }
      }
    }
  }
  return ret;
}


bool ObExternalPathFilter::is_inited() {
  return regex_ctx_.is_inited();
}

int ObExternalPathFilter::is_filtered(const ObString &path, bool &is_filtered)
{
  int ret = OB_SUCCESS;
  bool match = false;
  ObString out_text;
  if (OB_FAIL(ObExprUtil::convert_string_collation(path,
                                                   CS_TYPE_UTF8MB4_BIN,
                                                   out_text,
                                                   CS_TYPE_UTF16_BIN,
                                                   temp_allocator_))) {
    LOG_WARN("convert charset failed", K(ret), K(path));
  } else if (OB_FAIL(regex_ctx_.match(temp_allocator_, out_text, CS_TYPE_UTF16_BIN, 0, match))) {
    LOG_WARN("regex match failed", K(ret));
  }
  is_filtered = !match;
  temp_allocator_.reuse();
  return ret;
}

int ObExternalPathFilter::init(const ObString &pattern,
                               const ObExprRegexpSessionVariables &regexp_vars)
{
  int ret = OB_SUCCESS;
  if (regex_ctx_.is_inited()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("fail to init", K(ret));
  } else {
    uint32_t flags = 0;
    ObString match_string;
    if (OB_FAIL(ObExprRegexContext::get_regexp_flags(match_string, true, false, false, flags))) {
      LOG_WARN("failed to get regexp flags", K(ret));
    } else if (OB_FAIL(regex_ctx_.init(allocator_, regexp_vars,
                                       pattern, flags, true, CS_TYPE_UTF8MB4_BIN))) {
      LOG_WARN("init regex context failed", K(ret), K(pattern));
    }
  }
  return ret;
}

// dfo.get_sqcs(sqcs) sqcs 里面是dfo的地址
int ObExternalTableUtils::assign_odps_file_to_sqcs(
  ObDfo &dfo,
  ObExecContext &exec_ctx,
  ObIArray<ObPxSqcMeta> &sqcs,
  int64_t parallel,
  ObODPSGeneralFormat::ApiMode odps_api_mode)
{
  int ret = OB_SUCCESS;
  common::ObIArray<share::ObExternalFileInfo> &files = dfo.get_external_table_files();
  bool one_partition_per_thread = false;
  ObSEArray<const ObTableScanSpec *, 2> scan_ops;
  const ObTableScanSpec *scan_op = nullptr;
  const ObOpSpec *root_op = NULL;
  dfo.get_root(root_op);
  if (OB_ISNULL(root_op)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ptr", K(ret));
  } else if (OB_FAIL(ObTaskSpliter::find_scan_ops(scan_ops, *root_op))) {
    LOG_WARN("failed to find scan_ops", K(ret), KP(root_op));
  } else if (scan_ops.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty scan_ops", K(ret));
  }

  int64_t split_task_count = 0;
  int64_t table_total_row_count = 0;
  ObString session_str;
  ObString part_str;

  if (OB_SUCC(ret)) {
    if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
      if (OB_FAIL(fetch_odps_partition_info_for_task_assign(
                                              dfo.get_allocator(),
                                              scan_ops.at(0),
                                              exec_ctx,
                                              MTL_ID(),
                                              files,
                                              parallel,
                                              one_partition_per_thread))) {
        LOG_WARN("failed to fetch row count", K(ret));
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (odps_api_mode == sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        if (OB_FAIL(fetch_odps_partition_info_for_task_assign(
                                                dfo.get_allocator(),
                                                scan_ops.at(0),
                                                exec_ctx,
                                                MTL_ID(),
                                                files,
                                                parallel,
                                                one_partition_per_thread))) {
          LOG_WARN("failed to fetch row count", K(ret));
        }
      } else {
        ObSqlString part_spec_str;
        int64_t part_count = files.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
          const ObExternalFileInfo &external_info = files.at(i);
          if (0 == external_info.file_url_.compare(ObExternalTableUtils::dummy_file_name())) {
            // do nothing
          } else if (OB_FAIL(part_spec_str.append(external_info.file_url_))){
            LOG_WARN("failed to append file url", K(ret), K(external_info.file_url_));
          } else if (i < part_count - 1 && OB_FAIL(part_spec_str.append("#"))) {
            LOG_WARN("failed to append comma", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(dfo.get_allocator(), part_spec_str.string(), part_str, true))) {
          LOG_WARN("failed to write string", K(ret), K(part_spec_str));
        }

        const ExprFixedArray& exprs = scan_ops.at(0)->tsc_ctdef_.scan_ctdef_.pd_expr_spec_.ext_file_column_exprs_;
        if (OB_FAIL(ret)) {
        } else if(odps_api_mode == sql::ObODPSGeneralFormat::ApiMode::BYTE) {
          if (OB_FAIL(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_api_total_task(
            exec_ctx,
            exprs,
            part_spec_str.string(),
            scan_ops.at(0)->tsc_ctdef_.scan_ctdef_,
            nullptr, // das_rtdef
            parallel,
            session_str,
            split_task_count,
            dfo.get_allocator()
          ))) {
            LOG_WARN("failed to get task count ", K(ret));
          }
        } else {
          if (OB_FAIL(ObOdpsPartitionJNIDownloaderMgr::fetch_storage_api_split_by_row(
            exec_ctx,
            exprs,
            part_spec_str.string(),
            scan_ops.at(0)->tsc_ctdef_.scan_ctdef_,
            nullptr, // das_rtdef
            parallel,
            session_str,
            table_total_row_count,
            dfo.get_allocator()
          ))) {
            LOG_WARN("failed to get total row count ", K(ret));
          }
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS JNI connector is not enabled", K(ret));
#endif
    }
  }

  if (OB_FAIL(ret)) {
    /* do nothing */
  } else {
    if (!GCONF._use_odps_jni_connector) {
      if (one_partition_per_thread) {
        int64_t sqc_idx = 0;
        int64_t sqc_count = sqcs.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
          OZ (sqcs.at(sqc_idx++ % sqc_count).get_access_external_table_files().push_back(files.at(i)));
        }
      } else {
        if (OB_FAIL(split_odps_to_sqcs_process_tunnel(files, sqcs, parallel))) {
          LOG_WARN("failed to split odps to sqc process", K(ret));
        }
      }
    } else {
      if (odps_api_mode == sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        if (one_partition_per_thread) {
          int64_t sqc_idx = 0;
          int64_t sqc_count = sqcs.count();
          for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
            OZ (sqcs.at(sqc_idx++ % sqc_count).get_access_external_table_files().push_back(files.at(i)));
          }
        } else {
          if (OB_FAIL(split_odps_to_sqcs_process_tunnel(files, sqcs, parallel))) {
            LOG_WARN("failed to split odps to sqc process", K(ret));
          }
        }
      } else {
        if (OB_FAIL(split_odps_to_sqcs_storage_api(
                split_task_count, table_total_row_count, session_str, part_str, sqcs, parallel, dfo.get_allocator(), odps_api_mode))) {
          LOG_WARN("failed ot split odps task to sqcs in storage api mode", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // 作为结尾标志放给sqc, 每个sqc固定以dummy_file_name结尾
    ObExternalFileInfo dummy_file;
    dummy_file.file_id_ = INT64_MAX;
    dummy_file.file_url_ = ObExternalTableUtils::dummy_file_name();
    for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); ++i) {
      if (sqcs.at(i).get_access_external_table_files().empty()) {
        OZ(sqcs.at(i).get_access_external_table_files().push_back(dummy_file));
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::fetch_odps_partition_info_for_task_assign(
                                                      ObIAllocator &allocator,
                                                      const ObTableScanSpec *scan_op,
                                                      ObExecContext &exec_ctx,
                                                      uint64_t tenant_id,
                                                      ObIArray<ObExternalFileInfo> &external_table_files,
                                                      int64_t parallel,
                                                      bool &one_partition_per_thread)
{
  int ret = OB_SUCCESS;
  int64_t total_file_count = external_table_files.count();
  int64_t collected_file_count = 0;
  int64_t uncollected_file_count = 0;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
  int64_t max_collect_statistic_file_count = 5;
  if (OB_LIKELY(tenant_config.is_valid())) {
    max_collect_statistic_file_count = tenant_config->_max_partition_count_to_collect_statistic;
  }
  for (int i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
    const share::ObExternalFileInfo &odps_partition = external_table_files.at(i);
    if (odps_partition.file_size_ >= 0) {
      ++collected_file_count;
    } else {
      ++uncollected_file_count;
    }
  }

  if (collected_file_count == total_file_count) {
    // do nothing
  } else if (uncollected_file_count > max_collect_statistic_file_count) {
    one_partition_per_thread = true;
  } else {
    int64_t ref_table_id = scan_op->tsc_ctdef_.scan_ctdef_.ref_table_id_;
    ObString properties = scan_op->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_;
    common::hash::ObHashMap<ObOdpsPartitionKey, int64_t> partition_str_to_file_size;

    // get row count for odps partition
    OZ(ObExternalTableUtils::fetch_odps_partition_info(properties, external_table_files, 0,
                                 tenant_id, ref_table_id, parallel,
                                 partition_str_to_file_size, allocator));

    for (int64_t i = 0; OB_SUCC(ret) && i < external_table_files.count(); ++i) {
      share::ObExternalFileInfo &odps_partition = external_table_files.at(i);
      if (INT64_MAX == odps_partition.file_id_ && 0 == odps_partition.file_url_.compare(ObExternalTableUtils::dummy_file_name())) {
        // do nothing
      } else {
        int64_t file_size = 0;
        OZ(partition_str_to_file_size.get_refactored(ObOdpsPartitionKey(ref_table_id, odps_partition.file_url_), file_size));
        OX(odps_partition.file_size_ = file_size);
        LOG_INFO("ODPS assign task ROW COUNT", K(odps_partition.file_url_), K(file_size));
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::split_odps_to_sqcs_storage_api(int64_t split_task_count, int64_t table_total_row_count,
    const ObString& session_str, const ObString &new_file_urls, ObIArray<ObPxSqcMeta> &sqcs, int parallel, ObIAllocator &range_allocator, ObODPSGeneralFormat::ApiMode odps_api_mode)
{
  int ret = OB_SUCCESS;
  int64_t sqc_count = sqcs.count();
  if (odps_api_mode == sql::ObODPSGeneralFormat::ApiMode::BYTE) {
    int64_t start = 0;
    if (split_task_count == 0) {
      LOG_INFO("no task for reader", K(lbt()));
    } else {

      int64_t sqc_idx = 0;
      for (int i = 0; OB_SUCC(ret) && i < split_task_count; i += 1) {
        ObExternalFileInfo info;
        info.file_url_ = new_file_urls;
        info.session_id_ = session_str;
        info.file_id_ = 0;
        info.file_size_ = 1;
        info.file_id_ = i;
        info.part_id_ = 0;  // 由于现在partition是多个一起合成的
        info.row_start_ = 0;
        info.row_count_ = 0;
        if (OB_FAIL(sqcs.at(sqc_idx).get_access_external_table_files().push_back(info))) {
          LOG_WARN("failed to push back task info", K(ret));
        } else {
          sqc_idx = (sqc_idx + 1) % sqc_count;
        }
      }
    }
  } else {

    int64_t start = 0;
    if (table_total_row_count == 0) {
      // do nothing
    } else {
      int32_t task_count = parallel;
      int64_t step = (table_total_row_count + task_count - 1) / task_count;
      if (step < 1000) {
        step = 1000;
      }
      int64_t sqc_idx = 0;
      for (int64_t start = 0; OB_SUCC(ret) && start < table_total_row_count; start += step) {
        if (start + step > table_total_row_count) {
          step = table_total_row_count - start;
        }
        ObExternalFileInfo info;
        info.file_url_ = new_file_urls;
        info.session_id_ = session_str;
        info.file_id_ = 0;
        info.file_size_ = 0;
        info.file_id_ = 0;
        info.part_id_ = 0;  // for urls contain multi parts, part id has no meanings in current situation
        info.row_start_ = start;
        info.row_count_ = step;
        if (OB_FAIL(sqcs.at(sqc_idx).get_access_external_table_files().push_back(info))) {
          LOG_WARN("failed to push back task info", K(ret), K(start), K(step));
        } else {
          sqc_idx = (sqc_idx + 1) % sqc_count;
        }
      }
    }
  }

  return ret;
}

int ObExternalTableUtils::split_odps_to_sqcs_process_tunnel(
    common::ObIArray<share::ObExternalFileInfo> &files, ObIArray<ObPxSqcMeta> &sqcs, int parallel)
{
  int ret = OB_SUCCESS;
  int64_t sqc_idx = 0;
  int64_t sqc_count = sqcs.count();
  /*
   The main task of this conditional branch is to distribute several ODPS partitions as evenly as possible to SQC,
   with each SQC performing a secondary even split on the assigned partitions.
   When splitting partitions for SQC, only large partitions (those with more than 100,000 rows of data,
   referred to as large partitions) are split;small partitions are directly distributed in a round-robin manner to each
   SQC.

   step 1. pick up big partitions(alias file), calc row count sum of big partitions(called big_file_row_count),
           calc avg row count expected assign to per sqc(called avg_row_count_to_sqc).
   step 2. Assign avg_row_count_to_sqc rows to per sqc, use DROP_FACTOR to avoid a small tail of current partition
   assigned to next sqc. step 3. Assgin small partitions to per sqc in round robin method. step 4. Divide the partitions
   assigned to each SQC into smaller tasks, similar to the previous three steps.
   */
  ObSEArray<share::ObExternalFileInfo, 8> temp_block_files;
  ObSEArray<int64_t, 8> sqc_row_counts;
  int64_t big_file_row_count = 0;
  int64_t small_file_count = 0;
  const int64_t SMALL_FILE_SIZE = 100000;
  const double DROP_FACTOR = 0.2;
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); ++i) {
    if (OB_FAIL(sqc_row_counts.push_back(0))) {
      LOG_WARN("failed to init sqc_row_counts", K(i), K(ret));
    }
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    if (files.at(i).file_size_ > SMALL_FILE_SIZE) {
      big_file_row_count += files.at(i).file_size_;
    } else {
      ++small_file_count;
    }
  }
  int64_t avg_row_count_to_sqc = big_file_row_count / sqc_count;
  int64_t expected_row_count_to_sqc = 0;
  int64_t row_tail = big_file_row_count % sqc_count;
  int64_t file_idx = 0;
  int64_t file_start = 0;
  int64_t row_count_assigned_to_sqc = 0;
  int64_t remaining_row_count_in_file = 0;
  int64_t row_count_needed_to_sqc = 0;
  int64_t droped_row_count_on_last_loop = 0;
  sqc_idx = 0;
  LOG_TRACE("before split odps file",
      K(ret),
      K(small_file_count),
      K(big_file_row_count),
      K(sqc_count),
      K(row_tail),
      K(avg_row_count_to_sqc),
      K(files));
  while (OB_SUCC(ret) && big_file_row_count && sqc_idx < sqc_count) {
    expected_row_count_to_sqc = avg_row_count_to_sqc + droped_row_count_on_last_loop;
    row_count_assigned_to_sqc = 0;
    droped_row_count_on_last_loop = 0;
    if (sqc_idx == sqc_count - 1) {
      expected_row_count_to_sqc += row_tail;
    }
    while (OB_SUCC(ret) && row_count_assigned_to_sqc != expected_row_count_to_sqc) {
      while (file_idx < files.count() && files.at(file_idx).file_size_ < SMALL_FILE_SIZE) {
        ++file_idx;
      }
      if (file_idx >= files.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("big odps files is iter end",
            K(sqc_idx),
            K(sqc_count),
            K(row_count_assigned_to_sqc),
            K(expected_row_count_to_sqc),
            K(avg_row_count_to_sqc),
            K(row_tail),
            K(files.count()),
            K(file_idx));
      } else {
        remaining_row_count_in_file = files.at(file_idx).file_size_ - file_start;
        row_count_needed_to_sqc = expected_row_count_to_sqc - row_count_assigned_to_sqc;
        if (remaining_row_count_in_file >= row_count_needed_to_sqc) {
          ObExternalFileInfo splited_file_info = files.at(file_idx);
          splited_file_info.row_start_ = file_start;
          if (remaining_row_count_in_file <= (1 + DROP_FACTOR) * row_count_needed_to_sqc) {
            splited_file_info.row_count_ = remaining_row_count_in_file;
            row_count_assigned_to_sqc += remaining_row_count_in_file;
            file_start += remaining_row_count_in_file;
            droped_row_count_on_last_loop = expected_row_count_to_sqc - row_count_assigned_to_sqc;
            expected_row_count_to_sqc = row_count_assigned_to_sqc;
          } else {
            splited_file_info.row_count_ = row_count_needed_to_sqc;
            row_count_assigned_to_sqc += row_count_needed_to_sqc;
            file_start += row_count_needed_to_sqc;
          }
          OZ(sqcs.at(sqc_idx).get_access_external_table_files().push_back(splited_file_info));
        } else if (remaining_row_count_in_file > 0) {
          ObExternalFileInfo splited_file_info = files.at(file_idx);
          splited_file_info.row_start_ = file_start;
          splited_file_info.row_count_ = remaining_row_count_in_file;
          row_count_assigned_to_sqc += remaining_row_count_in_file;
          file_start += remaining_row_count_in_file;
          if (remaining_row_count_in_file >= (1 - DROP_FACTOR) * row_count_needed_to_sqc) {
            droped_row_count_on_last_loop = expected_row_count_to_sqc - row_count_assigned_to_sqc;
            expected_row_count_to_sqc = row_count_assigned_to_sqc;
          }
          OZ(sqcs.at(sqc_idx).get_access_external_table_files().push_back(splited_file_info));
        } else {  // remaining_row_count_in_file == 0
          ++file_idx;
          file_start = 0;
        }
      }
    }
    sqc_row_counts.at(sqc_idx) = row_count_assigned_to_sqc;
    sqc_idx++;
    LOG_TRACE("assigned big odps file to sqc",
        K(ret),
        K(sqc_row_counts),
        K(row_count_assigned_to_sqc),
        K(expected_row_count_to_sqc),
        K(avg_row_count_to_sqc),
        K(file_idx),
        K(sqc_idx),
        K(remaining_row_count_in_file));
  }
  const int64_t block_cnt = parallel / sqcs.count() * 3;
  LOG_TRACE("split big odps file to sqc, going to split files to block in sqc",
      K(ret),
      K(sqc_row_counts),
      K(sqcs.count()),
      K(block_cnt),
      K(parallel),
      K(small_file_count));
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); ++i) {
    ObIArray<share::ObExternalFileInfo> &sqc_files = sqcs.at(i).get_access_external_table_files();
    if (sqc_files.empty() || sqc_row_counts.at(i) <= 0) {
      continue;
    }
    temp_block_files.reuse();
    int64_t actual_block_cnt = block_cnt;
    if (i < small_file_count % sqcs.count()) {
      --actual_block_cnt;
    }
    LOG_WARN("split big odps file twice",
        K(i),
        K(i < small_file_count % sqcs.count()),
        K(actual_block_cnt),
        K(sqc_files),
        K(ret));
    int64_t avg_row_count_to_block = sqc_row_counts.at(i) / actual_block_cnt;
    int64_t expected_row_count_to_block = 0;
    int64_t block_row_tail = sqc_row_counts.at(i) % actual_block_cnt;
    int64_t block_idx = 0;
    int64_t row_count_assigned_to_block = 0;
    file_idx = 0;
    file_start = sqc_row_counts.at(i) ? sqc_files.at(file_idx).row_start_
                                      : 0;  // sqc_row_counts.at(i) means sqc_files is not empty
    droped_row_count_on_last_loop = 0;
    while (OB_SUCC(ret) && sqc_row_counts.at(i) && block_idx < actual_block_cnt) {
      expected_row_count_to_block = avg_row_count_to_block + droped_row_count_on_last_loop;
      row_count_assigned_to_block = 0;
      droped_row_count_on_last_loop = 0;
      if (block_idx == actual_block_cnt - 1) {
        expected_row_count_to_block += block_row_tail;
      }
      while (OB_SUCC(ret) && row_count_assigned_to_block != expected_row_count_to_block) {
        if (file_idx >= sqc_files.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("files in sqc iter end",
              K(i),
              K(block_idx),
              K(actual_block_cnt),
              K(row_count_assigned_to_block),
              K(expected_row_count_to_block),
              K(avg_row_count_to_block),
              K(block_row_tail),
              K(file_start),
              K(file_idx),
              K(sqc_files));
        } else {
          int64_t remaining_row_count_in_file =
              sqc_files.at(file_idx).row_count_ - (file_start - sqc_files.at(file_idx).row_start_);
          int64_t row_count_needed_to_block = expected_row_count_to_block - row_count_assigned_to_block;
          if (remaining_row_count_in_file >= row_count_needed_to_block) {
            ObExternalFileInfo splited_file_info = sqc_files.at(file_idx);
            splited_file_info.row_start_ = file_start;
            if (remaining_row_count_in_file <= (1 + DROP_FACTOR) * row_count_needed_to_block) {
              splited_file_info.row_count_ = remaining_row_count_in_file;
              row_count_assigned_to_block += remaining_row_count_in_file;
              file_start += remaining_row_count_in_file;
              droped_row_count_on_last_loop = expected_row_count_to_block - row_count_assigned_to_block;
              expected_row_count_to_block = row_count_assigned_to_block;
            } else {
              splited_file_info.row_count_ = row_count_needed_to_block;
              row_count_assigned_to_block += row_count_needed_to_block;
              file_start += row_count_needed_to_block;
            }
            if (splited_file_info.row_count_ + splited_file_info.row_start_ == splited_file_info.file_size_) {
              splited_file_info.row_count_ = INT64_MAX;
            }
            OZ(temp_block_files.push_back(splited_file_info));
          } else if (remaining_row_count_in_file > 0) {
            ObExternalFileInfo splited_file_info = sqc_files.at(file_idx);
            splited_file_info.row_start_ = file_start;
            splited_file_info.row_count_ = remaining_row_count_in_file;
            row_count_assigned_to_block += remaining_row_count_in_file;
            file_start += remaining_row_count_in_file;
            if (remaining_row_count_in_file >= (1 - DROP_FACTOR) * row_count_needed_to_block) {
              droped_row_count_on_last_loop = expected_row_count_to_block - row_count_assigned_to_block;
              expected_row_count_to_block = row_count_assigned_to_block;
            }
            if (splited_file_info.row_count_ + splited_file_info.row_start_ == splited_file_info.file_size_) {
              splited_file_info.row_count_ = INT64_MAX;
            }
            OZ(temp_block_files.push_back(splited_file_info));
          } else {  // remaining_row_count_in_file == 0
            ++file_idx;
            file_start = file_idx >= sqc_files.count() ? 0 : sqc_files.at(file_idx).row_start_;
          }
        }
      }
      block_idx++;
    }
    sqc_files.reuse();
    int64_t part_id = OB_INVALID_ID;
    for (int64_t j = 0; OB_SUCC(ret) && j < temp_block_files.count(); ++j) {
      if (part_id != temp_block_files.at(j).part_id_) {  // to find first distinct file
        OZ(sqc_files.push_back(temp_block_files.at(j)));
        part_id = temp_block_files.at(j).part_id_;
      }
    }
    part_id = OB_INVALID_ID;
    for (int64_t j = 0; OB_SUCC(ret) && j < temp_block_files.count(); ++j) {
      if (part_id != temp_block_files.at(j).part_id_) {
        part_id = temp_block_files.at(j).part_id_;
      } else {
        OZ(sqc_files.push_back(temp_block_files.at(j)));
      }
    }
  }
  sqc_idx = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    if (files.at(i).file_size_ <= SMALL_FILE_SIZE) {
      share::ObExternalFileInfo small_file = files.at(i);
      small_file.file_size_ = INT64_MAX;
      OZ(sqcs.at(sqc_idx++ % sqc_count).get_access_external_table_files().push_back(small_file));
    }
  }
  return ret;
}

int ObExternalTableUtils::plugin_split_tasks(
    ObIAllocator &allocator,
    const ObString &external_table_format_str,
    ObDfo &dfo,
    ObIArray<ObPxSqcMeta> &sqcs,
    int64_t parallel)
{
  int ret = OB_SUCCESS;
  ObExternalFileFormat external_file_format;
  ObString engine_type;
  plugin::ObExternalDataEngine *engine = nullptr;
  ObArray<ObString> tasks;
  tasks.set_label(plugin::OB_PLUGIN_MEMORY_LABEL);
  tasks.set_tenant_id(MTL_ID());

  if (sqcs.count() == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sqc node not found", K(ret), K(sqcs.count()));
  } else if (OB_FAIL(external_file_format.load_from_string(external_table_format_str, allocator))) {
    LOG_WARN("failed to load external file format from string", K(external_table_format_str), K(ret));
  } else if (ObExternalFileFormat::PLUGIN_FORMAT != external_file_format.format_type_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("external format is not plugin", K(ret), K(external_table_format_str));
  } else if (OB_FAIL(external_file_format.plugin_format_.create_engine(allocator, engine))) {
    LOG_WARN("failed to get external engine type property", K(ret));
  } else if (OB_FAIL(engine->split_task(allocator, parallel, tasks))) {
    LOG_WARN("failed to assign tasks to nodes", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tasks.count(); i++) {
      const int64_t node_index = i % sqcs.count();
      ObExternalFileInfo file_info;
      file_info.file_url_ = tasks.at(i);
      if (OB_FAIL(sqcs.at(node_index).get_access_external_table_files().push_back(file_info))) {
        LOG_WARN("failed to pushback task into sqc", K(ret), K(node_index), K(tasks.at(i)));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else {
    ObExternalFileInfo dummy_file;
    dummy_file.file_url_ = ObExternalTableUtils::dummy_file_name();
    for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); i++) {
      if (sqcs.at(i).get_access_external_table_files().empty()) {
        OZ(sqcs.at(i).get_access_external_table_files().push_back(dummy_file));
      }
    }
  }

  if (OB_NOT_NULL(engine)) {
    OB_DELETEx(ObExternalDataEngine, &allocator, engine);
    engine = nullptr;
  }
  return ret;
}

int ObExternalTableUtils::calc_assigned_files_to_sqcs(
  const ObIArray<ObExternalFileInfo> &files,
  ObIArray<int64_t> &assigned_idx,
  int64_t sqc_count)
{
  int ret = OB_SUCCESS;

  struct SqcFileSet {
    int64_t total_file_size_;
    int64_t sqc_idx_;
    TO_STRING_KV(K(total_file_size_), K(sqc_idx_));
  };

  struct SqcFileSetCmp {
    bool operator()(const SqcFileSet &l, const SqcFileSet &r) {
      return l.total_file_size_ > r.total_file_size_;
    }
    int get_error_code() { return OB_SUCCESS; }
  };

  struct FileInfoWithIdx {
    const ObExternalFileInfo *file_info_;
    int64_t file_idx_;
    TO_STRING_KV(K(file_idx_));
  };
  SqcFileSetCmp temp_cmp;
  ObBinaryHeap<SqcFileSet, SqcFileSetCmp> heap(temp_cmp);
  ObArray<FileInfoWithIdx> sorted_files;
  OZ (sorted_files.reserve(files.count()));
  OZ (assigned_idx.prepare_allocate(files.count()));
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); i++) {
    FileInfoWithIdx file_info;
    file_info.file_info_ = &(files.at(i));
    file_info.file_idx_ = i;
    OZ (sorted_files.push_back(file_info));
  }
  lib::ob_sort(sorted_files.begin(), sorted_files.end(),
            [](const FileInfoWithIdx &l, const FileInfoWithIdx &r) -> bool {
              return l.file_info_->file_size_ > r.file_info_->file_size_; });
  for (int64_t i = 0; OB_SUCC(ret) && i < sqc_count; i++) {
    SqcFileSet new_set;
    new_set.total_file_size_ = sorted_files.at(i).file_info_->file_size_;
    new_set.sqc_idx_ = i;
    OZ (heap.push(new_set));
    assigned_idx.at(sorted_files.at(i).file_idx_) = i;
  }
  for (int64_t i = sqc_count; OB_SUCC(ret) && i < sorted_files.count(); i++) {
    //assign file to set with the minimum total file size
    SqcFileSet cur_min_set = heap.top();
    cur_min_set.total_file_size_ += sorted_files.at(i).file_info_->file_size_;
    assigned_idx.at(sorted_files.at(i).file_idx_) = cur_min_set.sqc_idx_;
    OZ (heap.pop());
    OZ (heap.push(cur_min_set));
  }
  return ret;
}

int ObExternalTableUtils::assigned_files_to_sqcs_by_load_balancer(
  const common::ObIArray<ObExternalFileInfo> &files, const ObIArray<ObPxSqcMeta> &sqcs,
  common::ObIArray<int64_t> &assigned_idx)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "AddrMap");
  common::hash::ObHashMap<ObAddr, int64_t> worker_map;
  ObSEArray<ObAddr, 8> target_servers;
  ObDefaultLoadBalancer load_balancer;
  OZ (assigned_idx.prepare_allocate(files.count()));
  OZ (worker_map.create(common::hash::cal_next_prime(100), attr, attr));
  for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); ++i) {
    const ObPxSqcMeta &sqc_meta = sqcs.at(i);
    OZ (target_servers.push_back(sqc_meta.get_sqc_addr()));
    OZ (worker_map.set_refactored(sqc_meta.get_sqc_addr(), i));
  }
  OZ (load_balancer.add_server_list(target_servers));
  ObAddr addr;
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
    int64_t server_idx = 0;
    const ObExternalFileInfo &file_info = files.at(i);
    if (OB_FAIL(load_balancer.select_server(file_info.file_url_, addr))) {
      LOG_WARN("failed to select server", K(ret));
    } else if (OB_FAIL(worker_map.get_refactored(addr, server_idx))) {
      LOG_WARN("failed to get server idx", K(ret), K(addr));
    } else {
      assigned_idx.at(i) = server_idx;
    }
  }
  return ret;
}

int ObExternalTableUtils::select_external_table_loc_by_load_balancer(
  const common::ObIArray<ObExternalFileInfo> &files,
  const ObIArray<ObAddr> &all_locations,
  ObIArray<ObAddr> &target_locations)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "AddrMap");
  common::hash::ObHashSet<ObAddr> worker_set;
  ObDefaultLoadBalancer load_balancer;
  OZ (worker_set.create(common::hash::cal_next_prime(100), attr, attr));
  OZ (load_balancer.add_server_list(all_locations));
  ObAddr addr;
  for (int64_t i = 0; OB_SUCC(ret) && target_locations.count() < all_locations.count()
      && i < files.count(); ++i) {
    const ObExternalFileInfo &file_info = files.at(i);
    if (OB_FAIL(load_balancer.select_server(file_info.file_url_, addr))) {
      LOG_WARN("failed to select server", K(ret));
    } else if (OB_FAIL(worker_set.exist_refactored(addr))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        OZ (worker_set.set_refactored(addr));
        OZ (target_locations.push_back(addr));
      } else if (OB_HASH_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("failed to select addr", K(ret));
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::filter_files_in_locations(common::ObIArray<share::ObExternalFileInfo> &files,
                                    common::ObIArray<common::ObAddr> &locations)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); i++) {
    ObExternalFileInfo &table_info = files.at(i);
    bool found = false;
    for (int64_t j = 0; OB_SUCC(ret) && !found && j < locations.count(); j++) {
      if (table_info.file_addr_ == locations.at(j)) {
        found = true;
      }
    }
    if (OB_SUCC(ret) && !found) {
      std::swap(files.at(i), files.at(files.count() - 1));
      files.pop_back();
      i--;
    }
  }
  return ret;
}

int ObExternalTableUtils::collect_local_files_on_servers(
    const uint64_t tenant_id,
    const ObString &location,
    const ObString &pattern,
    const ObExprRegexpSessionVariables &regexp_vars,
    ObIArray<ObAddr> &all_servers,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes,
    ObIArray<int64_t> &modify_times,
    ObIArray<ObString> &content_digests,
    ObSqlString &partition_path,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObAddr, 8> target_servers;
  ObArray<ObString> server_ip_port;

  bool is_absolute_path = false;
  const int64_t PREFIX_LEN = STRLEN(OB_FILE_PREFIX);
  if (location.length() <= PREFIX_LEN) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid location", K(ret), K(location));
  } else {
    is_absolute_path = ('/' == location.ptr()[PREFIX_LEN]);
  }

  struct Functor {
    bool operator()(const ObAddr &l, const ObAddr &r) {
      return l < r;
    }
  };

  if (OB_SUCC(ret)) {
    if (is_absolute_path) {
      Functor functor;
      lib::ob_sort(all_servers.get_data(), all_servers.get_data() + all_servers.count(),
                functor);
      ObAddr pre_addr;
      for (int64_t i = 0; OB_SUCC(ret) && i < all_servers.count(); i++) {
        ObAddr &cur_addr = all_servers.at(i);
        if (!cur_addr.is_equal_except_port(pre_addr)) {
          pre_addr = cur_addr;
          OZ(target_servers.push_back(cur_addr));
        }
      }
    } else {
      OZ (target_servers.assign(all_servers));
    }
  }

  if (OB_SUCC(ret)) {
    ObAsyncRpcTaskWaitContext<ObRpcAsyncLoadExternalTableFileCallBack> context;
    int64_t send_task_count = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < target_servers.count(); i++) {
      const int64_t ip_len = 64;
      char *ip_port_buffer = nullptr;
      if (OB_ISNULL(ip_port_buffer = (char*)(allocator.alloc(ip_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate ip memory", K(ret));
      }
      OZ (target_servers.at(i).ip_port_to_string(ip_port_buffer, ip_len));
      OZ (server_ip_port.push_back(ObString(ip_port_buffer)));
    }
    OZ (context.init());
    OZ (context.get_cb_list().reserve(target_servers.count()));
    for (int64_t i = 0; OB_SUCC(ret) && i < target_servers.count(); i++) {
      const int64_t timeout = 10 * 1000000L; //10s
      ObRpcAsyncLoadExternalTableFileCallBack* async_cb = nullptr;
      ObLoadExternalFileListReq req;
      req.location_ = location;
      req.pattern_ = pattern;
      req.regexp_vars_ = regexp_vars;

      if (OB_ISNULL(async_cb = OB_NEWx(ObRpcAsyncLoadExternalTableFileCallBack, (&allocator), (&context)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate async cb memory", K(ret));
      }
      OZ (context.get_cb_list().push_back(async_cb));
      OZ (GCTX.external_table_proxy_->to(target_servers.at(i))
                                        .by(tenant_id)
                                        .timeout(timeout)
                                        .load_external_file_list(req, async_cb));
      if (OB_SUCC(ret)) {
        send_task_count++;
      }
    }

    context.set_task_count(send_task_count);

    do {
      int temp_ret = context.wait_executing_tasks();
      if (OB_SUCCESS != temp_ret) {
        LOG_WARN("fail to wait executing task", K(temp_ret));
        if (OB_SUCC(ret)) {
          ret = temp_ret;
        }
      }
    } while(0);

    for (int64_t i = 0; OB_SUCC(ret) && i < context.get_cb_list().count(); i++) {
      if (OB_FAIL(context.get_cb_list().at(i)->get_task_resp().rcode_.rcode_)) {
        LOG_WARN("async load files process failed", K(ret));
      } else {
        const ObIArray<ObString> &resp_file_urls =
          context.get_cb_list().at(i)->get_task_resp().file_urls_;
        OZ (append(file_sizes, context.get_cb_list().at(i)->get_task_resp().file_sizes_));
        for (int64_t j = 0; OB_SUCC(ret) && j < resp_file_urls.count(); j++) {
          ObSqlString tmp_file_url;
          ObString file_url;
          OZ (tmp_file_url.append(server_ip_port.at(i)));
          OZ (tmp_file_url.append("%"));
          OZ (tmp_file_url.append(partition_path.string()));
          OZ (tmp_file_url.append(resp_file_urls.at(j)));
          OZ (ob_write_string(allocator, tmp_file_url.string(), file_url));
          OZ (file_urls.push_back(file_url));
        }
        if (OB_FAIL(ret)) {
        } else if (GET_MIN_CLUSTER_VERSION() >= CLUSTER_VERSION_4_4_1_0) {
          const ObIArray<ObString> &resp_content_digests =
            context.get_cb_list().at(i)->get_task_resp().content_digests_;
          OZ (append(modify_times, context.get_cb_list().at(i)->get_task_resp().modify_times_));
          for (int64_t j = 0; OB_SUCC(ret) && j < resp_content_digests.count(); j++) {
            ObString content_digest;
            OZ(ob_write_string(allocator, resp_content_digests.at(j), content_digest));
            OZ(content_digests.push_back(content_digest));
          }
        }
      }
    }

    for (int64_t i = 0; i < context.get_cb_list().count(); i++) {
      context.get_cb_list().at(i)->~ObRpcAsyncLoadExternalTableFileCallBack();
    }
  }
  LOG_TRACE("update external table file list", K(ret), K(file_urls), K(location), K(pattern), K(all_servers));
  return ret;
}

int ObExternalTableUtils::sort_external_files(
  ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos)
{
  int ret = OB_SUCCESS;
  lib::ob_sort(basic_file_infos.get_data(), basic_file_infos.get_data() + basic_file_infos.count(),
               ExternalTableFileUrlCompare());
  LOG_TRACE("after filter external table files", K(ret), K(basic_file_infos));
  return ret;
}

/*
* 全局只在这里pull partition了
*/
int ObExternalTableUtils::collect_external_file_list(
    const ObSQLSessionInfo* session_ptr_in,
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
    const ObString &properties,
    const bool &is_partitioned_table,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIAllocator &allocator,
    ObSqlString &full_path,
    ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos)
{
  int ret = OB_SUCCESS;

  if (!properties.empty()) {
    sql::ObExternalFileFormat ex_format;
    if (OB_FAIL(ex_format.load_from_string(properties, allocator))) {
      LOG_WARN("failed to load from string", K(ret));
    } else if (sql::ObExternalFileFormat::PLUGIN_FORMAT == ex_format.format_type_) {
      // do nothing
    } else if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
      // Since each partition information of an ODPS table obtained by the ODPS
      // driver is a string, OceanBase treat partition string as an external
      // table filename, one file corresponds to one odps partition, the number
      // of files corresponds to the number of partitions.
      sql::ObODPSTableRowIterator odps_driver;
      if (OB_FAIL(odps_driver.init_tunnel(ex_format.odps_format_))) {
        LOG_WARN("failed to init tunnel", K(ret));
      } else if (OB_FAIL(odps_driver.pull_partition_info())) {
        LOG_WARN("failed to pull partition info", K(ret));
      } else if (odps_driver.is_part_table()) {
        if (!is_partitioned_table) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("remote odps table is partitioned table, but local odps "
                   "external table is not partitioned table",
                   K(ret));
          LOG_USER_ERROR(
              OB_EXTERNAL_ODPS_UNEXPECTED_ERROR,
              "remote odps table is partitioned table, but local odps "
              "external table is not partitioned table");
        }
        ObIArray<sql::ObODPSTableRowIterator::OdpsPartition> &part_list_info =
            odps_driver.get_partition_info();
        for (int64_t i = 0; OB_SUCC(ret) && i < part_list_info.count(); ++i) {
          const char *part_spec_src = part_list_info.at(i).name_.c_str();
          int64_t part_spec_src_len = STRLEN(part_spec_src);
          char *part_spec = NULL;
          if (OB_ISNULL(part_spec = reinterpret_cast<char *>(
                            allocator.alloc(part_spec_src_len)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc mem", K(part_spec_src_len), K(ret));
          } else {
            MEMCPY(part_spec, part_spec_src, part_spec_src_len);
            share::ObExternalTableBasicFileInfo basic_file_info;
            basic_file_info.size_ = part_list_info.at(i).record_count_;
            basic_file_info.url_.assign_ptr(part_spec, part_spec_src_len);
            OZ(basic_file_infos.push_back(basic_file_info));
          }
        }
      } else {
        share::ObExternalTableBasicFileInfo basic_file_info;
        ObIArray<sql::ObODPSTableRowIterator::OdpsPartition> &part_list_info =
            odps_driver.get_partition_info();
        if (is_partitioned_table) {
          ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
          LOG_WARN("remote odps table is not partitioned table, but local odps "
                   "external table is partitioned table",
                   K(ret));
          LOG_USER_ERROR(
              OB_EXTERNAL_ODPS_UNEXPECTED_ERROR,
              "remote odps table is not partitioned table, but local "
              "odps external table is partitioned table");
        } else if (1 != part_list_info.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected count of partition info", K(ret),
                   K(part_list_info.count()));
        }
        /*
         * ODPSQz NON spartition EMPTY PARTITION
         */
        OX (basic_file_info.size_ = part_list_info.at(0).record_count_);
        OX (basic_file_info.url_.assign_ptr("", strlen("")));
        OZ(basic_file_infos.push_back(basic_file_info));
      }
#else
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("not support odps driver", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      // Since each partition information of an ODPS table obtained by the ODPS
      // driver is a string, OceanBase treat partition string as an external
      // table filename, one file corresponds to one odps partition, the number
      // of files corresponds to the number of partitions.
      sql::ObODPSJNITableRowIterator odps_jni_iter;
      if (is_oracle_mode()) {
        ret = OB_ERR_UNSUPPORTED_TYPE;
        LOG_WARN("Current not support to execute in oracle mode", K(ret));
      } else if (OB_FAIL(ex_format.load_from_string(properties, allocator))) {
        LOG_WARN("failed to load from string", K(ret));
      } else {
        // option
        int64_t count = 0;
        if (ex_format.odps_format_.api_mode_ != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
          // split by row of byte use one options
          if (OB_FAIL(odps_jni_iter.init_jni_schema_scanner(ex_format.odps_format_, session_ptr_in))) {
            LOG_WARN("failed to init and open jni schema scanner", K(ret));
          }
        } else {
          // tunnel api
          if (OB_FAIL(odps_jni_iter.init_jni_meta_scanner(ex_format.odps_format_, session_ptr_in))) {
            LOG_WARN("failed to init and open jni schema scanner", K(ret));
          }
        }
        if(OB_FAIL(ret)) {
        } else if (OB_FAIL(odps_jni_iter.pull_partition_info())) {
          LOG_WARN("failed to pull columns info", K(ret));
        } else {
          if (odps_jni_iter.is_part_table()) {
            // If remote odps table is partition table but local defined not
            // partititon table.
            if (!is_partitioned_table) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("remote odps table is partitioned table, but local odps "
                       "external table is not partitioned table",
                  K(ret));
              LOG_USER_ERROR(OB_EXTERNAL_ODPS_UNEXPECTED_ERROR,
                  "remote odps table is partitioned table, but local odps "
                  "external table is not partitioned table");
            } else {
              ObIArray<sql::ObODPSJNITableRowIterator::OdpsJNIPartition> &partition_specs =
                  odps_jni_iter.get_partition_specs();
              for (int64_t i = 0; OB_SUCC(ret) && i < partition_specs.count(); ++i) {
                ObString partition_spec = partition_specs.at(i).partition_spec_;
                const char *part_spec_src = partition_spec.ptr();
                int64_t part_spec_src_len = partition_spec.length();
                char *part_spec = NULL;
                if (OB_ISNULL(part_spec = reinterpret_cast<char *>(allocator.alloc(part_spec_src_len)))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed to alloc mem", K(part_spec_src_len), K(ret));
                } else {
                  MEMCPY(part_spec, part_spec_src, part_spec_src_len);
                  share::ObExternalTableBasicFileInfo basic_file_info;
                  basic_file_info.size_ = partition_specs.at(i).record_count_;
                  basic_file_info.url_.assign_ptr(part_spec, part_spec_src_len);
                  OZ(basic_file_infos.push_back(basic_file_info));
                }
              }
            }
          } else {
            ObIArray<sql::ObODPSJNITableRowIterator::OdpsJNIPartition> &partition_specs =
                odps_jni_iter.get_partition_specs();
            if (is_partitioned_table) {
              ret = OB_EXTERNAL_ODPS_UNEXPECTED_ERROR;
              LOG_WARN("remote odps table is not partitioned table, but local odps "
                       "external table is partitioned table",
                  K(ret));
              LOG_USER_ERROR(OB_EXTERNAL_ODPS_UNEXPECTED_ERROR,
                  "remote odps table is not partitioned table, but local "
                  "odps external table is partitioned table");
            } else if (1 != partition_specs.count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected count of partition info", K(ret), K(partition_specs.count()));
            }
            /*
             * ODPSQz NON spartition EMPTY PARTITION
             */
            share::ObExternalTableBasicFileInfo basic_file_info;
            basic_file_info.size_ = partition_specs.at(0).record_count_;
            basic_file_info.url_.assign_ptr("", strlen(""));
            OZ(basic_file_infos.push_back(basic_file_info));
          }
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "external odps table");
      LOG_WARN("not support odps table in opensource", K(ret));
#endif
    }
  } else if (location.empty()) {
    //do nothing
  } else {
    ObArray<ObString> file_urls;
    ObArray<int64_t> file_sizes;
    ObArray<int64_t> modify_times;
    ObArray<ObString> content_digests;
    ObSEArray<ObAddr, 8> all_servers;
    share::schema::ObSchemaGetterGuard schema_guard;
    OZ (GCTX.location_service_->external_table_get(tenant_id, all_servers));
    const bool is_local_storage = ObSQLUtils::is_external_files_on_local_disk(location);
    if (OB_SUCC(ret) && full_path.length() > 0
            && *(full_path.ptr() + full_path.length() - 1) != '/' ) {
      OZ (full_path.append("/"));
    }
    if (OB_FAIL(ret)) {
    } else if (is_local_storage) {
      OZ(collect_local_files_on_servers(tenant_id, location, pattern, regexp_vars, all_servers,
                                        file_urls, file_sizes, modify_times, content_digests,
                                        full_path, allocator));
    } else {
      OZ(ObExternalTableFileManager::get_external_file_list_on_device(
        location, pattern, regexp_vars, file_urls, file_sizes, modify_times, content_digests,
        access_info, allocator));
      for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
        ObSqlString tmp_file_url;
        ObString &file_url = file_urls.at(i);
        OZ (tmp_file_url.append(full_path.string()));
        OZ (tmp_file_url.append(file_urls.at(i)));
        OZ (ob_write_string(allocator, tmp_file_url.string(), file_url, true));
      }
    }
    OZ(build_basic_file_infos(file_urls, file_sizes, modify_times, content_digests,
                              basic_file_infos));
    OZ(ObExternalTableUtils::sort_external_files(basic_file_infos));
  }
  return ret;
}

int ObExternalTableUtils::build_basic_file_infos(
  const ObIArray<ObString> &file_urls, const ObIArray<int64_t> &file_sizes,
  const ObIArray<int64_t> &modify_times, const ObIArray<ObString> &content_digests,
  ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos)

{
  int ret = OB_SUCCESS;
  int64_t file_count = file_urls.count();
  if (file_count != file_sizes.count()
      || (content_digests.count() > 0
          && file_count != content_digests.count())
      || (modify_times.count() > 0
          && file_count != modify_times.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size error", K(ret), K(file_count), K(file_sizes.count()),
             K(content_digests.count()), K(modify_times.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
      if ((file_count == content_digests.count()) && (file_count == modify_times.count())) {
        if (OB_FAIL(basic_file_infos.push_back(share::ObExternalTableBasicFileInfo(
              file_urls.at(i), file_sizes.at(i), modify_times.at(i), content_digests.at(i))))) {
          LOG_WARN("failed to push back", K(ret));
        }
      } else {
        if (OB_FAIL(basic_file_infos.push_back(
              share::ObExternalTableBasicFileInfo(file_urls.at(i), file_sizes.at(i))))) {
          LOG_WARN("failed to push back", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObExternalFileListArrayOpWithFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, entry is null");
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, d_name is null");
  } else {
    const ObString file_name(entry->d_name);
    ObString tmp_file;
    ObString tmp_content_digests;
    bool is_filtered = false;
    if (!file_name.empty() && file_name[file_name.length() - 1] != '/') {
      if (OB_NOT_NULL(filter_) && OB_FAIL(filter_->is_filtered(file_name, is_filtered))) {
        LOG_WARN("fail check is filtered", K(ret));
      } else if (!is_filtered) {
        const ObFileExtraInfo &extra_info = get_extra_info();
        if (OB_FAIL(ob_write_string(allocator_, file_name, tmp_file, true))) {
          OB_LOG(WARN, "fail to save file name", K(ret), K(file_name));
        } else if (OB_FAIL(name_array_.push_back(tmp_file))) {
          OB_LOG(WARN, "fail to push filename to array", K(ret), K(tmp_file));
        } else if (OB_FAIL(file_size_.push_back(get_size()))) {
          OB_LOG(WARN, "fail to push size to array", K(ret));
        } else if (extra_info.is_last_modify_time_valid()
                   && OB_FAIL(modify_times_.push_back(extra_info.last_modified_time_ms_))) {
          OB_LOG(WARN, "fail to push last modify time to array", K(ret));
        } else if (extra_info.is_etag_valid()) {
          const ObString content_digests(extra_info.etag_len_, extra_info.etag_);
          if (OB_FAIL(ob_write_string(allocator_, content_digests, tmp_content_digests, true))) {
            OB_LOG(WARN, "fail to save content digests", K(ret), K(content_digests));
          } else if (OB_FAIL(content_digests_.push_back(tmp_content_digests))) {
            OB_LOG(WARN, "fail to push etag to array", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLocalFileListArrayOpWithFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, entry is null");
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, d_name is null");
  } else {
    const ObString file_name(entry->d_name);
    ObSqlString full_path;
    ObString tmp_file;
    bool is_filtered = false;
    ObString cur_path = path_;
    ObString filter_path;
    if (file_name.case_compare(".") == 0 || file_name.case_compare("..") == 0) {
      // do nothing
    } else if (OB_FAIL(full_path.assign(cur_path))) {
      OB_LOG(WARN, "assign string failed", K(ret));
    } else if (full_path.length() > 0 && *(full_path.ptr() + full_path.length() - 1) != '/'
               && OB_FAIL(full_path.append("/"))) {
      OB_LOG(WARN, "append failed", K(ret));
    } else if (OB_FAIL(full_path.append(file_name))) {
      OB_LOG(WARN, "append file name failed", K(ret));
    } else {
      filter_path = full_path.string();
      filter_path += origin_path_.length();  // 只匹配location下的子路径
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_NOT_NULL(filter_)
               && OB_FAIL(filter_->is_filtered(filter_path, is_filtered))) {
      LOG_WARN("fail check is filtered", K(ret));
    } else if (!is_filtered) {
      ObString target = full_path.string();
      if (!is_dir_scan()) {
        target += origin_path_.length();
        if (!target.empty() && '/' == target[0]) {
          target += 1;
        }
      }
      if (target.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("empty dir or name", K(full_path), K(origin_path_));
      } else if (OB_FAIL(ob_write_string(allocator_, target, tmp_file, true /*c_style*/))) {
        OB_LOG(WARN, "fail to save file name", K(ret), K(file_name));
      } else if (OB_FAIL(name_array_.push_back(tmp_file))) {
        OB_LOG(WARN, "fail to push filename to array", K(ret), K(tmp_file));
      } else if (OB_FAIL(file_size_.push_back(get_size()))) {
        OB_LOG(WARN, "fail to push size to array", K(ret), K(tmp_file));
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::collect_external_file_list_with_cache(
    const uint64_t tenant_id,
    const ObIArray<ObString> &part_path,
    const ObIArray<int64_t> &part_id,
    const ObIArray<int64_t> &part_modify_ts,
    const ObString &access_info,
    const ObString &pattern,
    ObIAllocator &allocator,
    int64_t refresh_interval_ms,
    ObIArray<ObHiveFileDesc> &hive_file_desc)
{
  int ret = OB_SUCCESS;

  ObArenaAllocator tmp_allocator;
  ObSEArray<ObExternalTableFiles *, 4> tmp_external_table_files;
  ObSEArray<int64_t, 4> reorder_part_id;
  if (OB_FAIL(
          ObExternalTableFileManager::get_instance().get_external_file_list_on_device_with_cache(
              part_path,
              tenant_id,
              part_id,
              part_modify_ts,
              pattern,
              access_info,
              tmp_allocator,
              refresh_interval_ms,
              tmp_external_table_files,
              reorder_part_id))) {
    LOG_WARN("fail to get file list with cache", K(ret), K(part_path));
  }

  ObHiveFileDesc *desc = NULL;
  if (reorder_part_id.count() < tmp_external_table_files.count()) {
    // 可能是个空分区， tmp_external_table_files 就为空
    // 但会存在 part_id
    // 所以 part_id 的个数需要大于等于文件个数
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to check count", K(reorder_part_id.count()), K(tmp_external_table_files.count()));
  }
  for (int64_t k = 0; OB_SUCC(ret) && k < tmp_external_table_files.count(); ++k) {
    ObExternalTableFiles *tmp_table_files = tmp_external_table_files.at(k);
    int64_t tmp_part_id = reorder_part_id.at(k);
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_table_files->file_urls_.count(); i++) {
      if (OB_ISNULL(desc = hive_file_desc.alloc_place_holder())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc memory for hive file", K(ret), K(tmp_table_files));
      } else {
        OZ(ob_write_string(allocator, tmp_table_files->file_urls_.at(i), desc->file_path_));
        desc->file_size_ = tmp_table_files->file_sizes_.at(i);
        desc->modify_ts_ = tmp_table_files->modify_times_.at(i);
        desc->part_id_ = tmp_part_id;
      }
    }
  }

  return ret;
}

int ObExternalTableUtils::collect_partitions_info_with_cache(
    const ObTableSchema &table_schema,
    ObSqlSchemaGuard &sql_schema_guard,
    ObIAllocator &allocator,
    int64_t refresh_interval_ms,
    ObArray<PartitionInfo*> &partition_infos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(ObExternalTableFileManager::get_instance().get_partitions_info_with_cache(
          table_schema,
          sql_schema_guard,
          allocator,
          refresh_interval_ms,
          partition_infos))) {
    LOG_WARN("fail to get partition with cache", K(ret), K(table_schema));
  }

  return ret;
}


bool ObExternalTableUtils::is_skipped_insert_column(const schema::ObColumnSchemaV2& column)
{
  bool is_skip = false;
  if (OB_HIDDEN_FILE_ID_COLUMN_ID == column.get_column_id()
      || OB_HIDDEN_LINE_NUMBER_COLUMN_ID == column.get_column_id()) {
    // 外表插入时不写隐藏列
    is_skip = true;
  } else if (column.is_tbl_part_key_column()) {
    // 外表插入时不写分区键的列
    is_skip = true;
  }
  return is_skip;
}

int ObExternalTableUtils::get_external_file_location(const ObTableSchema &table_schema,
                                                     ObSchemaGetterGuard &schema_guard,
                                                     ObIAllocator &allocator,
                                                     ObString &file_location)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t location_id = table_schema.get_external_location_id();
  if (OB_INVALID_ID != location_id) {
    const ObLocationSchema *location_schema = NULL;
    if (OB_FAIL(schema_guard.get_location_schema_by_id(tenant_id, location_id, location_schema))) {
      LOG_WARN("fail to get location schema", K(ret), K(location_id), K(tenant_id));
    } else if (OB_ISNULL(location_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location schema is null", K(ret), K(location_id), K(tenant_id));
    } else {
      ObSqlString full_path;
      ObString sub_path = table_schema.get_external_sub_path();
      OZ (full_path.append(location_schema->get_location_url_str()));
      if (OB_SUCC(ret) && full_path.length() > 0
          && *(full_path.ptr() + full_path.length() - 1) != '/'
          && !sub_path.empty()
          && sub_path[0] != '/') {
        OZ (full_path.append("/"));
      }
      OZ (full_path.append(sub_path));
      if (OB_SUCC(ret)) {
        ob_write_string(allocator, full_path.string(), file_location, true);
      }
    }
  } else {
    file_location = table_schema.get_external_file_location();
  }
  return ret;
}

int ObExternalTableUtils::collect_file_basic_info(const common::ObString &location,
                                                  const ObString &access_info,
                                                  const common::ObString &file_url,
                                                  common::ObIAllocator &allocator,
                                                  int64_t &file_size, int64_t &modify_time,
                                                  ObString &content_digest)
{
  int ret = OB_SUCCESS;
  ObExternalFileInfoCollector collector(allocator);
  if (OB_FAIL(collector.init(location, access_info))) {
    LOG_WARN("failed to init", K(ret));
  } else if (OB_FAIL(collector.collect_file_content_digest(file_url, content_digest))) {
    LOG_WARN("failed to collect file content digest", K(ret), K(file_url));
  } else if (OB_FAIL(collector.collect_file_modify_time(file_url, modify_time))) {
    LOG_WARN("failed to collect file last modify time", K(ret), K(file_url));
  } else if (OB_FAIL(collector.collect_file_size(file_url, file_size))) {
    LOG_WARN("failed to collect file size", K(ret), K(file_url));
  }
  return ret;
}

int ObExternalTableUtils::classification_file_basic_info(
  const ObIArray<share::ObExternalTableBasicFileInfo> &basic_file_infos,
  ObIArray<common::ObString> &file_urls, ObIArray<int64_t> *file_sizes,
  ObIArray<common::ObString> *content_digests, ObIArray<int64_t> *modify_times)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < basic_file_infos.count(); ++i) {
    if (OB_FAIL(file_urls.push_back(basic_file_infos.at(i).url_))) {
      LOG_WARN("failed to push file url", K(ret));
    } else if (nullptr != content_digests
               && OB_FAIL(content_digests->push_back(basic_file_infos.at(i).content_digest_))) {
      LOG_WARN("failed to push content digest", K(ret));
    } else if (nullptr != file_sizes
               && OB_FAIL(file_sizes->push_back(basic_file_infos.at(i).size_))) {
      LOG_WARN("failed to push file size", K(ret));
    } else if (nullptr != modify_times
               && OB_FAIL(modify_times->push_back(basic_file_infos.at(i).last_modify_time_))) {
      LOG_WARN("failed to push last modify time", K(ret));
    }
  }
  return ret;
}

int ObExternalTableUtils::get_external_file_location_access_info(const ObTableSchema &table_schema,
                                                                 ObSchemaGetterGuard &schema_guard,
                                                                 ObString &access_info)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = table_schema.get_tenant_id();
  const uint64_t location_id = table_schema.get_external_location_id();
  if (OB_INVALID_ID != location_id) {
    const ObLocationSchema *location_schema = NULL;
    if (OB_FAIL(schema_guard.get_location_schema_by_id(tenant_id, location_id, location_schema))) {
      LOG_WARN("fail to get location schema", K(ret), K(location_id), K(tenant_id));
    } else if (OB_ISNULL(location_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("location schema is null", K(ret), K(location_id), K(tenant_id));
    } else {
      access_info = location_schema->get_location_access_info_str();
    }
  } else {
    access_info = table_schema.get_external_file_location_access_info();
  }
  return ret;
}

int ObExternalTableUtils::remove_external_file_list(const uint64_t tenant_id,
                                                    const ObString &location,
                                                    const ObString &access_info,
                                                    const ObString &pattern,
                                                    const sql::ObExprRegexpSessionVariables &regexp_vars,
                                                    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (location.empty()) {
    //do nothing
  } else {
    const bool is_local_storage = ObSQLUtils::is_external_files_on_local_disk(location);
    if (is_local_storage) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support local storage");
    } else {
      ObStorageType device_type = OB_STORAGE_MAX_TYPE;
      OZ (get_storage_type_from_path(location, device_type));
      // hdfs支持直接删除目录, 删除hdfs下的某个路径时不需要先获取文件
      bool is_del_all = (pattern.empty() || pattern == "*") && (OB_STORAGE_HDFS == device_type);
      ObArray<ObString> file_urls;
      ObArray<share::ObExternalTableBasicFileInfo> basic_file_infos;
      ObSqlString full_path;
      full_path.append(location);
      if (!is_del_all) {
        OZ(collect_external_file_list(nullptr, tenant_id, -1, location, access_info, pattern, "",
                                      false, regexp_vars, allocator, full_path, basic_file_infos));
      }
      if (OB_SUCC(ret)) {
        ObArray<int64_t> failed_files_idx;
        common::ObObjectStorageInfo *storage_access_info = NULL;
        share::ObBackupStorageInfo backup_storage_info;
        share::ObHDFSStorageInfo hdfs_storage_info;
        if (device_type == OB_STORAGE_HDFS) {
          storage_access_info = &hdfs_storage_info;
        } else {
          storage_access_info = &backup_storage_info;
        }
        OZ (storage_access_info->set(device_type, access_info.ptr()));

        ObIODOpts opts;
        ObIODOpt opt; //only one para
        opts.opts_ = &(opt);
        opts.opt_cnt_ = 1;
        opt.key_ = "storage_info";
        ObIODevice *dev_handle = NULL;
        const ObStorageIdMod storage_id_mod = ObStorageIdMod::get_default_id_mod();
        if (OB_FAIL(ret)) {
          // do nothing
        } else if (FALSE_IT(opt.value_.value_str = access_info.ptr())) {
          // do nothing
        } else if (OB_FAIL(ObDeviceManager::get_instance().get_device(location, *storage_access_info,
                                                                      storage_id_mod, dev_handle))) {
          OB_LOG(WARN, "fail to get device!", KR(ret), KPC(storage_access_info), K(location), K(storage_id_mod));
        } else if (OB_ISNULL(dev_handle)) {
          ret = OB_ERR_UNEXPECTED;
          OB_LOG(WARN, "returned device is null", KR(ret), KPC(storage_access_info), K(location), K(storage_id_mod));
        } else if (OB_FAIL(dev_handle->start(opts))) {
          OB_LOG(WARN, "fail to start device!", KR(ret), KPC(storage_access_info), K(location), K(storage_id_mod));
        } else if (OB_FAIL(classification_file_basic_info(basic_file_infos, file_urls))) {
          OB_LOG(WARN, "fail to extra file url", KR(ret));
        } else if (!is_del_all && OB_FAIL(dev_handle->batch_del_files(file_urls, failed_files_idx))) {
          OB_LOG(WARN, "fail to del files", KR(ret), KPC(storage_access_info), K(location), K(storage_id_mod));
        } else if (is_del_all && OB_FAIL(dev_handle->rmdir(location.ptr()))) {
          OB_LOG(WARN, "fail to rmdir", KR(ret), KPC(storage_access_info), K(location), K(storage_id_mod));
        }
        if (OB_NOT_NULL(dev_handle)) {
          if (OB_FAIL(ObDeviceManager::get_instance().release_device(dev_handle))) {
            OB_LOG(WARN, "fail to release device", K(ret), KP(dev_handle));
          } else {
            dev_handle = nullptr;
          }
        }
      }
    }
  }
  return ret;
}

int ObExternalFileInfoCollector::init(const common::ObString &location, const ObString &access_info)
{
  int ret = OB_SUCCESS;
  const char DUMMY_EMPTY_CHAR = '\0';
  if (OB_FAIL(get_storage_type_from_path_for_external_table(location, storage_type_))) {
    LOG_WARN("fail to resolve storage type", K(ret));
  } else {
    ObString access_info_cstr;
    if (storage_type_ == OB_STORAGE_HDFS) {
      storage_info_ = &hdfs_storage_info_;
    } else {
      storage_info_ = &backup_storage_info_;
    }
    if (storage_type_ == OB_STORAGE_FILE) {
      access_info_cstr.assign_ptr(&DUMMY_EMPTY_CHAR, strlen(&DUMMY_EMPTY_CHAR));
    } else if (OB_FAIL(ob_write_string(allocator_, access_info, access_info_cstr, true))) {
      LOG_WARN("failed to write string into access_info_cstr", K(ret), K(access_info));
    }
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(storage_info_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("failed to get storage info", K(ret), K(storage_type_));
    } else if (OB_FAIL(storage_info_->set(storage_type_, access_info_cstr.ptr()))) {
      LOG_WARN("failed to set storage info", K(ret), K(storage_type_));
    } else if (OB_UNLIKELY(!storage_info_->is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid storage info", KR(ret), KPC(storage_info_));
    }
  }
  return ret;
}

int ObExternalFileInfoCollector::collect_file_content_digest(const common::ObString &url,
                                                             ObString &content_digest)
{
  int ret = OB_SUCCESS;
  char *digest_buf = nullptr;
  if (OB_ISNULL(digest_buf = static_cast<char *>(allocator_.alloc(MAX_CONTENT_DIGEST_LEN)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else if (OB_FAIL(ObExternalIoAdapter::get_file_content_digest(url,
                                                                  storage_info_,
                                                                  digest_buf,
                                                                  MAX_CONTENT_DIGEST_LEN))) {
    LOG_WARN("failed to get file content digest", K(ret));
  } else {
    content_digest.assign_ptr(digest_buf, STRLEN(digest_buf));
  }
  return ret;
}

int ObExternalFileInfoCollector::collect_files_content_digest(
  const common::ObIArray<common::ObString> &file_urls, common::ObIArray<ObString> &content_digests)
{
  int ret = OB_SUCCESS;
  int64_t N = file_urls.count();
  char *digest_buf = nullptr;
  if (N <= 0) {
    // do nothing
  } else if (OB_ISNULL(digest_buf =
                         static_cast<char *>(allocator_.alloc(MAX_CONTENT_DIGEST_LEN * N)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
      if (OB_FAIL(ObExternalIoAdapter::get_file_content_digest(file_urls.at(i),
                                                               storage_info_,
                                                               digest_buf,
                                                               MAX_CONTENT_DIGEST_LEN))) {
        LOG_WARN("failed to get file content digest", K(ret));
      } else if (OB_FAIL(content_digests.push_back(ObString(STRLEN(digest_buf), digest_buf)))) {
        LOG_WARN("failed to pushback", K(ret));
      } else {
        digest_buf += MAX_CONTENT_DIGEST_LEN;
      }
    }
  }
  return ret;
}

int ObExternalFileInfoCollector::collect_files_modify_time(
  const common::ObIArray<common::ObString> &file_urls, common::ObIArray<int64_t> &modify_times)
{
  int ret = OB_SUCCESS;
  int64_t last_modify_time = 0;
  int64_t N = file_urls.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; i++) {
    if (OB_FAIL(ObExternalIoAdapter::get_file_modify_time(file_urls.at(i),
                                                          storage_info_,
                                                          last_modify_time))) {
      LOG_WARN("failed to get file modify time", K(ret));
    } else if (OB_FAIL(modify_times.push_back(last_modify_time))) {
      LOG_WARN("failed to pushback", K(ret));
    }
  }
  return ret;
}

int ObExternalFileInfoCollector::collect_file_modify_time(const common::ObString &url,
                                                          int64_t &modify_time)
{
  int ret = OB_SUCCESS;
  modify_time = 0;
  if (OB_FAIL(ObExternalIoAdapter::get_file_modify_time(url, storage_info_, modify_time))) {
    LOG_WARN("failed to get file modify time", K(ret));
  }
  return ret;
}

int ObExternalFileInfoCollector::collect_file_size(const common::ObString &url,
                                                   int64_t &file_size,
                                                   bool enable_cache)
{
  int ret = OB_SUCCESS;
  file_size = 0;
  if (enable_cache) {
    ObCachedExternalFileInfoCollector &cached_collector
        = ObCachedExternalFileInfoCollector::get_instance();
    if (OB_FAIL(cached_collector.collect_file_size(url, storage_info_, file_size))) {
      LOG_WARN("failed to get cached file size", K(ret));
    }
  } else {
    if (OB_FAIL(ObExternalIoAdapter::get_file_length(url, storage_info_, file_size))) {
      LOG_WARN("failed to get file size", K(ret));
    }
  }
  return ret;
}

int ObExternalFileInfoCollector::convert_to_full_file_urls(
  const common::ObString &location, const common::ObIArray<common::ObString> &file_urls,
  common::ObIArray<common::ObString> &full_file_urls)
{
  int ret = OB_SUCCESS;
  const char *split_char = "/";
  for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
    ObString file_url;
    ObSqlString tmp_file_url;
    OZ(tmp_file_url.append_fmt(
      "%.*s%s%.*s", location.length(), location.ptr(),
      (location.empty() || location[location.length() - 1] == '/') ? "" : split_char,
      file_urls.at(i).length(), file_urls.at(i).ptr()));
    OZ(ob_write_string(allocator_, tmp_file_url.string(), file_url));
    OZ(full_file_urls.push_back(file_url));
  }
  return ret;
}

int ObExternalFileInfoCollector::get_file_list(const common::ObString &path,
                                               const common::ObString &pattern,
                                               const ObExprRegexpSessionVariables &regexp_vars,
                                               common::ObIArray<common::ObString> &file_urls,
                                               common::ObIArray<int64_t> &file_sizes,
                                               common::ObIArray<int64_t> &modify_times,
                                               common::ObIArray<ObString> &content_digests)
{
  int ret = OB_SUCCESS;
  ObExprRegexContext regexp_ctx;
  ObExternalPathFilter filter(regexp_ctx, allocator_);
  ObString path_cstring;
  ObSEArray<ObString, 16> temp_file_urls;
  CONSUMER_GROUP_FUNC_GUARD(PRIO_IMPORT);

  int max_visit_count = 1000000;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    max_visit_count = tenant_config->_max_access_entries_for_external_table_partition;
  }
  if (OB_UNLIKELY(!storage_info_->is_valid())) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", KR(ret), K_(storage_info));
  } else if (!pattern.empty() && OB_FAIL(filter.init(pattern, regexp_vars))) {
    LOG_WARN("fail to init filter", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator_, path, path_cstring, true /*c_style*/))) {
    LOG_WARN("fail to copy string", KR(ret), K(path));
  } else if (OB_STORAGE_FILE == storage_type_ || OB_STORAGE_HDFS == storage_type_) {
    ObSEArray<ObString, 4> file_dirs;
    bool is_dir = false;

    if (OB_STORAGE_FILE == storage_type_) {
      ObString path_without_prifix;
      path_without_prifix = path_cstring;
      path_without_prifix += strlen(OB_FILE_PREFIX);

      OZ(FileDirectoryUtils::is_directory(path_without_prifix.ptr(), is_dir));
      if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_without_prifix));
      } else {
        OZ(file_dirs.push_back(path_cstring));
      }
    } else {
      // OB_STORAGE_HDFS
      if (OB_FAIL(ObExternalIoAdapter::is_directory(path_cstring, storage_info_, is_dir))) {
        if (OB_HDFS_PATH_NOT_FOUND == ret) {
          // Skip to collect a directory that not exists
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to check if directory", K(ret), K(path_cstring), KPC(storage_info_));
        }
      } else if (!is_dir) {
        LOG_INFO("external location is not a directory", K(path_cstring));
      } else {
        OZ(file_dirs.push_back(path_cstring));
      }
    }
    ObArray<int64_t> useless_size;
    for (int64_t i = 0; OB_SUCC(ret) && OB_SUCC(THIS_WORKER.check_status()) && i < file_dirs.count(); i++) {
      ObString file_dir = file_dirs.at(i);
      ObLocalFileListArrayOpWithFilter dir_op(file_dirs, useless_size, file_dir, path_cstring, NULL,
                                              allocator_);
      ObLocalFileListArrayOpWithFilter file_op(file_urls, file_sizes, file_dir, path_cstring,
                                               pattern.empty() ? NULL : &filter, allocator_);
      dir_op.set_dir_flag();
      if (OB_FAIL(ObExternalIoAdapter::list_files(file_dir, storage_info_, file_op))) {
        LOG_WARN("fail to list files", KR(ret), K(file_dir), KPC(storage_info_));
      } else if (OB_FAIL(ObExternalIoAdapter::list_directories(file_dir, storage_info_, dir_op))) {
        LOG_WARN("fail to list dirs", KR(ret), K(file_dir), KPC(storage_info_));
      } else if (file_dirs.count() + file_urls.count() > max_visit_count) {
        ret = OB_SIZE_OVERFLOW;
        LOG_WARN("too many files and dirs to visit", K(ret));
      }
    }
    OZ(convert_to_full_file_urls(path, file_urls, temp_file_urls));
    OZ(collect_files_content_digest(temp_file_urls, content_digests));
    OZ(collect_files_modify_time(temp_file_urls, modify_times));
  } else {
    ObExternalFileListArrayOpWithFilter file_op(file_urls, file_sizes, modify_times,
                                                content_digests, pattern.empty() ? NULL : &filter,
                                                allocator_);
    if (OB_FAIL(ObExternalIoAdapter::list_files(path_cstring, storage_info_, file_op))) {
      LOG_WARN("fail to list files", KR(ret), K(path_cstring), KPC(storage_info_));
    } else if (!file_op.is_valid_content_digests() || !file_op.is_valid_modify_times()) {
      OZ(convert_to_full_file_urls(path, file_urls, temp_file_urls));
      if (!file_op.is_valid_content_digests()) {
        OZ(collect_files_content_digest(temp_file_urls, content_digests));
      }
      if (!file_op.is_valid_modify_times()) {
        OZ(collect_files_modify_time(temp_file_urls, modify_times));
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::create_external_file_url_info(const common::ObString &file_location,
                                                       const common::ObString &access_info,
                                                       const common::ObString &full_file_name,
                                                       common::ObIAllocator &allocator,
                                                       ObExternalFileUrlInfo *&file_info)
{
  int ret = OB_SUCCESS;
  int64_t file_size = 0;
  int64_t modify_time = 0;
  ObString file_content_digest;
  void *ptr = NULL;

  if (OB_FAIL(ObExternalTableUtils::collect_file_basic_info(file_location, access_info,
          full_file_name, allocator, file_size, modify_time, file_content_digest))) {
    LOG_WARN("failed to collect file basic info", K(ret));
  } else if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObExternalFileUrlInfo)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate file info", K(ret), K(sizeof(ObExternalFileUrlInfo)));
  } else {
    file_info = new (ptr) ObExternalFileUrlInfo(file_location, access_info, full_file_name,
                                               file_content_digest, file_size, modify_time);
  }
  return ret;
}

int ObExternalTableUtils::get_credential_field_name(ObSqlString &str, int64_t opt)
{
  int ret = OB_SUCCESS;
  if (opt == 1) {
    OZ (str.append(common::ACCESS_ID));
  } else if (opt == 2) {
    OZ (str.append(common::ACCESS_KEY));
  } else if (opt == 3) {
    OZ (str.append(common::HOST));
  } else if (opt == 4) {
    OZ (str.append(common::APPID));
  } else if (opt == 5) {
    OZ (str.append(common::REGION));
  } else if (opt == 6) {
    OZ (str.append(share::PRINCIPAL));
  } else if (opt == 7) {
    OZ (str.append(share::KEYTAB));
  } else if (opt == 8) {
    OZ (str.append(share::KRB5CONF));
  } else if (opt == 9) {
    OZ (str.append(share::HDFS_CONFIGS));
  } else if (opt == 10) {
    OZ (str.append(share::HADOOP_USERNAME));
  } else {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid opt", K(ret), K(opt));
  }
  return ret;
}

bool ObCachedExternalFileInfoKey::operator==(const common::ObIKVCacheKey &other) const
{
  const ObCachedExternalFileInfoKey &other_key
      = reinterpret_cast<const ObCachedExternalFileInfoKey &>(other);
  return tenant_id_ == other_key.tenant_id_ && file_path_ == other_key.file_path_;
}

uint64_t ObCachedExternalFileInfoKey::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = murmurhash(&tenant_id_, sizeof(uint64_t), hash_ret);
  hash_ret = murmurhash(file_path_.ptr(), file_path_.length(), hash_ret);
  return hash_ret;
}

int64_t ObCachedExternalFileInfoKey::size() const
{
  return sizeof(*this) + file_path_.length();
}

uint64_t ObCachedExternalFileInfoKey::get_tenant_id() const
{
  return tenant_id_;
}

int ObCachedExternalFileInfoKey::deep_copy(char *buf,
                                           const int64_t buf_len,
                                           common::ObIKVCacheKey *&key) const
{
  int ret = OB_SUCCESS;
  ObDataBuffer allocator(buf, buf_len);
  ObCachedExternalFileInfoKey *new_key = NULL;
  if (OB_ISNULL(new_key = OB_NEWx(ObCachedExternalFileInfoKey, &allocator))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret));
  } else if (OB_FAIL(ob_write_string(allocator, file_path_, new_key->file_path_))) {
    LOG_WARN("failed to write file path", K(ret));
  } else {
    new_key->tenant_id_ = tenant_id_;
    key = new_key;
  }
  return ret;
}

int64_t ObCachedExternalFileInfoValue::size() const
{
  return sizeof(*this);
}

int ObCachedExternalFileInfoValue::deep_copy(char *buf,
                                             const int64_t buf_len,
                                             ObIKVCacheValue *&value) const
{
  int ret = OB_SUCCESS;
  ObCachedExternalFileInfoValue *cache_value = new (buf) ObCachedExternalFileInfoValue();
  if (OB_ISNULL(cache_value)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to create cache value", K(ret));
  } else {
    cache_value->file_size_ = file_size_;
    value = cache_value;
  }
  return ret;
}

ObCachedExternalFileInfoCollector &ObCachedExternalFileInfoCollector::get_instance()
{
  static ObCachedExternalFileInfoCollector instance_;
  return instance_;
}

int ObCachedExternalFileInfoCollector::init()
{
  int ret = OB_SUCCESS;
  OZ(kv_cache_.init("cached_file_info"));
  OZ(bucket_lock_.init(bucket_num_));
  return ret;
}

int ObCachedExternalFileInfoCollector::collect_file_size(
    const common::ObString &url,
    const common::ObObjectStorageInfo *storage_info,
    int64_t &file_size)
{
  int ret = OB_SUCCESS;
  ObCachedExternalFileInfoKey cached_key;
  cached_key.tenant_id_ = MTL_ID();
  cached_key.file_path_ = url;
  file_size = OB_INVALID_SIZE;

  if (OB_SUCC(ret)) {
    ObBucketHashRLockGuard(bucket_lock_, cached_key.tenant_id_);
    ObKVCacheHandle handle;
    const ObCachedExternalFileInfoValue *cached_value;
    if (OB_FAIL(kv_cache_.get(cached_key, cached_value, handle))) {
      if (ret != OB_ENTRY_NOT_EXIST) {
        LOG_WARN("failed to get cached file info", K(ret));
      }
    } else if (OB_ISNULL(cached_value)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null cached value", K(ret));
    } else {
      file_size = cached_value->file_size_;
    }
  }

  if (OB_ENTRY_NOT_EXIST == ret) {
    ret = OB_SUCCESS;
    ObBucketHashWLockGuard(bucket_lock_, cached_key.tenant_id_);
    if (OB_FAIL(ObExternalIoAdapter::get_file_length(url, storage_info, file_size))) {
      LOG_WARN("failed to get file size", K(ret));
    } else {
      ObCachedExternalFileInfoValue cache_value;
      cache_value.file_size_ = file_size;
      OZ(kv_cache_.put(cached_key, cache_value));
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase

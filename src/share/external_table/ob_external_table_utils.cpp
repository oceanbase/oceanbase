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
#include "sql/engine/table/ob_odps_jni_table_row_iter.h"
#include "sql/engine/basic/ob_consistent_hashing_load_balancer.h"
#include "lib/restore/ob_object_device.h"
#include "src/share/ob_device_manager.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace share
{



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

int ObExternalTableUtils::resolve_odps_start_step(const ObNewRange &range,
                                                    const int64_t &column_idx,
                                                    int64_t &start,
                                                    int64_t &step)
{
  int ret = OB_SUCCESS;
  if (column_idx >= range.get_start_key().get_obj_cnt() ||
      column_idx >= range.get_end_key().get_obj_cnt() ) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. input column idx invalid", K(ret), K(range), K(column_idx));
  } else {
    const ObObj &start_obj = range.get_start_key().get_obj_ptr()[column_idx];
    const ObObj &end_obj = range.get_end_key().get_obj_ptr()[column_idx];
    start = start_obj.get_int();
    step = end_obj.get_int();
  }
  return ret;
}

int ObExternalTableUtils::convert_external_table_new_range(const ObString &file_url,
                                                           const int64_t file_id,
                                                           const uint64_t part_id,
                                                           const ObNewRange &range,
                                                           ObIAllocator &allocator,
                                                           ObNewRange &new_range,
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
    if (OB_FAIL(make_external_table_scan_range(file_url,
                                               file_id,
                                               part_id,
                                               start_lineno,
                                               end_lineno,
                                               ObString::make_empty_string(),
                                               0,
                                               0,
                                               allocator,
                                               new_range))) {
      LOG_WARN("failed to make external table scan range", K(ret));
    }
  }
  return ret;
}

int ObExternalTableUtils::convert_external_table_empty_range(const ObString &file_url,
                                                             const int64_t file_id,
                                                             const uint64_t ref_table_id,
                                                             ObIAllocator &allocator,
                                                             ObNewRange &new_range)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(make_external_table_scan_range(file_url,
                                             file_id,
                                             ref_table_id,
                                             ObCSVTableRowIterator::MIN_EXTERNAL_TABLE_LINE_NUMBER,
                                             INT64_MAX,
                                             ObString::make_empty_string(),
                                             0,
                                             0,
                                             allocator,
                                             new_range))) {
    LOG_WARN("failed to make external table scan range", K(ret));
  }
  return ret;
}

int ObExternalTableUtils::make_external_table_scan_range(const common::ObString &file_url,
                                                         const int64_t file_id,
                                                         const uint64_t part_id,
                                                         const int64_t first_lineno,
                                                         const int64_t last_lineno,
                                                         const common::ObString &session_id,
                                                         const int64_t first_split_idx,
                                                         const int64_t last_split_idx,
                                                         common::ObIAllocator &allocator,
                                                         common::ObNewRange &new_range)
{
  int ret = OB_SUCCESS;
  ObObj *obj_start = NULL;
  ObObj *obj_end = NULL;
  if (OB_UNLIKELY(first_lineno > last_lineno && file_id != 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed. get invalid params", K(ret), K(first_lineno), K(last_lineno));
  } else if (OB_ISNULL(obj_start = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) *
                                                                    MAX_EXTERNAL_FILE_SCANKEY)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else if (OB_ISNULL(obj_end = static_cast<ObObj*>(allocator.alloc(sizeof(ObObj) *
                                                                    MAX_EXTERNAL_FILE_SCANKEY)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc memory", K(ret));
  } else {
    obj_start[PARTITION_ID] = ObObj();
    obj_start[PARTITION_ID].set_uint64(part_id);
    obj_end[PARTITION_ID] = ObObj();
    obj_end[PARTITION_ID].set_uint64(part_id);

    obj_start[FILE_URL] = ObObj();
    obj_start[FILE_URL].set_varchar(file_url);
    obj_start[FILE_URL].set_collation_type(ObCharset::get_system_collation());
    obj_end[FILE_URL] = ObObj();
    obj_end[FILE_URL].set_varchar(file_url);
    obj_end[FILE_URL].set_collation_type(ObCharset::get_system_collation());

    obj_start[FILE_ID] = ObObj();
    obj_start[FILE_ID].set_int(file_id);
    obj_end[FILE_ID] = ObObj();
    obj_end[FILE_ID].set_int(file_id);

    obj_start[ROW_GROUP_NUMBER].set_min_value();
    obj_end[ROW_GROUP_NUMBER].set_max_value();
    obj_start[LINE_NUMBER] = ObObj();
    obj_start[LINE_NUMBER].set_int(first_lineno);
    obj_end[LINE_NUMBER] = ObObj();
    obj_end[LINE_NUMBER].set_int(last_lineno);

    obj_start[SESSION_ID] = ObObj();
    obj_start[SESSION_ID].set_varchar(session_id);
    obj_start[SESSION_ID].set_collation_type(ObCharset::get_system_collation());
    obj_end[SESSION_ID] = ObObj();
    obj_end[SESSION_ID].set_varchar(session_id);
    obj_end[SESSION_ID].set_collation_type(ObCharset::get_system_collation());

    obj_start[SPLIT_IDX] = ObObj();
    obj_start[SPLIT_IDX].set_int(first_split_idx);
    obj_end[SPLIT_IDX] = ObObj();
    obj_end[SPLIT_IDX].set_int(last_split_idx);

    new_range.border_flag_.set_inclusive_start();
    new_range.border_flag_.set_inclusive_end();
    new_range.start_key_.assign(obj_start, MAX_EXTERNAL_FILE_SCANKEY);
    new_range.end_key_.assign(obj_end, MAX_EXTERNAL_FILE_SCANKEY);
  }
  return ret;
}

int ObExternalTableUtils::prepare_single_scan_range(const uint64_t tenant_id,
                                                    const ObDASScanCtDef &das_ctdef,
                                                    ObDASScanRtDef *das_rtdef,
                                                    ObExecContext &exec_ctx,
                                                    ObIArray<int64_t> &partition_ids,
                                                    ObIArray<ObNewRange *> &ranges,
                                                    ObIAllocator &range_allocator,
                                                    ObIArray<ObNewRange *> &new_range,
                                                    bool is_file_on_disk,
                                                    ObExecContext &ctx) {
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
                            range_allocator, file_urls, tmp_ranges.empty() ? NULL : &tmp_ranges))) {
        LOG_WARN("failed to get external files by part ids", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(GCTX.location_service_->external_table_get(tenant_id, table_id, all_locations))) {
        LOG_WARN("fail to get external table location", K(ret));
    } else if (is_file_on_disk
              && OB_FAIL(ObExternalTableUtils::filter_files_in_locations(file_urls,
                                                                        all_locations))) {
        //For recovered cluster, the file addr may not in the cluster. Then igore it.
        LOG_WARN("filter files in location failed", K(ret));
    } else {
      new_range.reset();
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
        ObNewRange *range = NULL;
        if (OB_ISNULL(range = OB_NEWx(ObNewRange, (&range_allocator)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to new a ptr", K(ret));
        } else if (OB_FAIL(ObExternalTableUtils::make_external_table_scan_range(external_info.file_url_,
                       external_info.file_id_,
                       external_info.part_id_,
                       0,
                       INT64_MAX,
                       ObString::make_string(""),
                       0,
                       0,
                       range_allocator,
                       *range))) {
          LOG_WARN("failed to make external table scan range", K(ret));
        } else {
          /*
           * 单机单线程每个part一个range
           */
          OZ(new_range.push_back(range));
        }
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (odps_api_mode != ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        ObSqlString part_spec_str;
        ObString part_str;
        int64_t part_count = file_urls.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
          const ObExternalFileInfo &external_info = file_urls.at(i);
          if (0 == external_info.file_url_.compare("#######DUMMY_FILE#######")) {
            // do nothing
          } else if (OB_FAIL(part_spec_str.append(external_info.file_url_))) {
            LOG_WARN("failed to append file url", K(ret), K(external_info.file_url_));
          } else if (i < part_count - 1 && OB_FAIL(part_spec_str.append("#"))) {
            LOG_WARN("failed to append comma", K(ret));
          }
        }
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ob_write_string(range_allocator, part_spec_str.string(), part_str, true))) {
          LOG_WARN("failed to write string", K(ret), K(part_spec_str));
        } else if (odps_api_mode == ObODPSGeneralFormat::ApiMode::BYTE) {
          ObString session_str;
          int64_t split_count = 0;
          ObNewRange *range = NULL;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_storage_api_total_task(
                         exec_ctx,
                         ext_file_column_expr,
                         part_str,
                         das_ctdef,
                         das_rtdef,
                         1,
                         session_str,
                         split_count,
                         range_allocator))) {
            LOG_WARN("failed to make total task", K(ret));
          } else if (OB_ISNULL(range = OB_NEWx(ObNewRange, (&range_allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::make_external_table_scan_range(part_str,
                         0,
                         0,  // external_info.part_id_ 原来part id会存在列中可以反解出分区列的值, 现在不需要了
                         0,
                         INT64_MAX,
                         session_str,
                         0,
                         split_count,
                         range_allocator,
                         *range))) {
            LOG_WARN("failed to make external table scan range", K(ret));
          } else {
            OZ(new_range.push_back(range));
          }
        } else {
          ObString session_str;
          int64_t total_row_count = 0;
          ObNewRange *range = NULL;
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_storage_api_split_by_row(
                         exec_ctx,
                         ext_file_column_expr,
                         part_str,
                         das_ctdef,
                         das_rtdef,
                         1,
                         session_str,
                         total_row_count,
                         range_allocator))) {
            LOG_WARN("failed to make total task", K(ret));
          } else if (OB_ISNULL(range = OB_NEWx(ObNewRange, (&range_allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::make_external_table_scan_range(part_str,
                         0,
                         0,  // external_info.part_id_ 原来part id会存在列中可以反解出分区列的值, 现在不需要了
                         0,
                         total_row_count,
                         session_str,
                         0,
                         0,
                         range_allocator,
                         *range))) {
            LOG_WARN("failed to make external table scan range", K(ret));
          } else {
            OZ(new_range.push_back(range));
          }
        }
      } else {
        // tunnel api
        for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
          const ObExternalFileInfo &external_info = file_urls.at(i);
          ObNewRange *range = NULL;
          if (OB_ISNULL(range = OB_NEWx(ObNewRange, (&range_allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::make_external_table_scan_range(external_info.file_url_,
                         external_info.file_id_,
                         external_info.part_id_,
                         0,
                         INT64_MAX,
                         ObString::make_string(""),
                         0,
                         0,
                         range_allocator,
                         *range))) {
            LOG_WARN("failed to make external table scan range", K(ret));
          } else {
            /*
             * 单机单线程每个part一个range
             */
            OZ(new_range.push_back(range));
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
          ObNewRange *range = NULL;
          bool is_valid = false;
          if (OB_ISNULL(range = OB_NEWx(ObNewRange, (&range_allocator)))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to new a ptr", K(ret));
          } else if (OB_FAIL(ObExternalTableUtils::convert_external_table_new_range(
                                                                            file_urls.at(j).file_url_,
                                                                            file_urls.at(j).file_id_,
                                                                            file_urls.at(j).part_id_,
                                                                            *tmp_ranges.at(i),
                                                                            range_allocator,
                                                                            *range,
                                                                            is_valid))) {
            LOG_WARN("failed to convert external table new range", K(ret), K(file_urls.at(j)),
                      K(ranges.at(i)));
          } else if (is_valid) {
            OZ (new_range.push_back(range));
          }
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
  ObIArray<ObPxSqcMeta *> &sqcs,
  int64_t parallel,
  ObODPSGeneralFormat::ApiMode odps_api_mode)
{
  int ret = OB_SUCCESS;
  common::ObIArray<share::ObExternalFileInfo> &files = dfo.get_external_table_files();
  bool use_partition_gi = false;
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
      if (OB_FAIL(ObOdpsPartitionDownloaderMgr::fetch_row_count(
              MTL_ID(),
              scan_ops.at(0)
                  ->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_,
              files,
              use_partition_gi))) {
        LOG_WARN("failed to fetch row count", K(ret));
      }
#else
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("ODPS CPP connector is not enabled", K(ret));
#endif
    } else {
#if defined (OB_BUILD_JNI_ODPS)
      if (odps_api_mode != sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        ObSqlString part_spec_str;
        int64_t part_count = files.count();
        for (int64_t i = 0; OB_SUCC(ret) && i < part_count; ++i) {
          const ObExternalFileInfo &external_info = files.at(i);
          if (0 == external_info.file_url_.compare("#######DUMMY_FILE#######")) {
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
          if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_storage_api_total_task(
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
          if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_storage_api_split_by_row(
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
      } else {
        if (OB_FAIL(ObOdpsPartitionJNIScannerMgr::fetch_row_count(
                exec_ctx,
                MTL_ID(),
                scan_ops.at(0)
                    ->tsc_ctdef_.scan_ctdef_.external_file_format_str_.str_,
                files,
                use_partition_gi))) {
          LOG_WARN("failed to fetch row count", K(ret));
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
  } else if (use_partition_gi) {
    int64_t sqc_idx = 0;
    int64_t sqc_count = sqcs.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < files.count(); ++i) {
      OZ (sqcs.at(sqc_idx++ % sqc_count)->get_access_external_table_files().push_back(files.at(i)));
    }
  } else {
    if (!GCONF._use_odps_jni_connector) {
      if (OB_FAIL(split_odps_to_sqcs_process_tunnel(files, sqcs, parallel))) {
        LOG_WARN("failed to split odps to sqc process", K(ret));
      }
    } else {
      if (odps_api_mode != sql::ObODPSGeneralFormat::ApiMode::TUNNEL_API) {
        if (OB_FAIL(split_odps_to_sqcs_storage_api(
                split_task_count, table_total_row_count, session_str, part_str, sqcs, parallel, dfo.get_allocator(), odps_api_mode))) {
          LOG_WARN("failed ot split odps task to sqcs in storage api mode", K(ret));
        }
      } else {
        if (OB_FAIL(split_odps_to_sqcs_process_tunnel(files, sqcs, parallel))) {
          LOG_WARN("failed to split odps to sqc process in tunnel mode", K(ret));
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // 作为结尾标志放给sqc
    ObExternalFileInfo dummy_file;
    const char* dummy_file_name = "#######DUMMY_FILE#######";
    dummy_file.file_url_ = dummy_file_name;
    for (int64_t i = 0; OB_SUCC(ret) && i < sqcs.count(); ++i) {
      if (sqcs.at(i)->get_access_external_table_files().empty()) {
        OZ(sqcs.at(i)->get_access_external_table_files().push_back(dummy_file));
      }
    }
  }
  return ret;
}

int ObExternalTableUtils::split_odps_to_sqcs_storage_api(int64_t split_task_count, int64_t table_total_row_count,
    const ObString& session_str, const ObString &new_file_urls, ObIArray<ObPxSqcMeta *> &sqcs, int parallel, ObIAllocator &range_allocator, ObODPSGeneralFormat::ApiMode odps_api_mode)
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
        if (OB_FAIL(sqcs.at(sqc_idx)->get_access_external_table_files().push_back(info))) {
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
        if (OB_FAIL(sqcs.at(sqc_idx)->get_access_external_table_files().push_back(info))) {
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
    common::ObIArray<share::ObExternalFileInfo> &files, ObIArray<ObPxSqcMeta *> &sqcs, int parallel)
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
          OZ(sqcs.at(sqc_idx)->get_access_external_table_files().push_back(splited_file_info));
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
          OZ(sqcs.at(sqc_idx)->get_access_external_table_files().push_back(splited_file_info));
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
    ObIArray<share::ObExternalFileInfo> &sqc_files = sqcs.at(i)->get_access_external_table_files();
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
      OZ(sqcs.at(sqc_idx++ % sqc_count)->get_access_external_table_files().push_back(small_file));
    }
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
  const common::ObIArray<ObExternalFileInfo> &files, const ObIArray<ObPxSqcMeta *> &sqcs,
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
    ObPxSqcMeta *sqc_meta = sqcs.at(i);
    OZ (target_servers.push_back(sqc_meta->get_sqc_addr()));
    OZ (worker_map.set_refactored(sqc_meta->get_sqc_addr(), i));
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
        const ObIArray<ObString> &resp_array = context.get_cb_list().at(i)->get_task_resp().file_urls_;
        OZ (append(file_sizes, context.get_cb_list().at(i)->get_task_resp().file_sizes_));
        for (int64_t j = 0; OB_SUCC(ret) && j < resp_array.count(); j++) {
          ObSqlString tmp_file_url;
          ObString file_url;
          OZ (tmp_file_url.append(server_ip_port.at(i)));
          OZ (tmp_file_url.append("%"));
          OZ (tmp_file_url.append(partition_path.string()));
          OZ (tmp_file_url.append(resp_array.at(j)));
          OZ (ob_write_string(allocator, tmp_file_url.string(), file_url));
          OZ (file_urls.push_back(file_url));
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

int ObExternalTableUtils::sort_external_files(ObIArray<ObString> &file_urls,
                                              ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;
  const int64_t count = file_urls.count();
  ObSEArray<int64_t, 8> tmp_file_sizes;
  hash::ObHashMap<ObString, int64_t> file_map;
  if (0 == count) {
    /* do nothing */
  } else if (OB_UNLIKELY(count != file_sizes.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size error", K(ret));
  } else if (OB_FAIL(file_map.create(count, "ExtFileMap", "ExtFileMap"))) {
      LOG_WARN("fail to init hashmap", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
      if (OB_FAIL(file_map.set_refactored(file_urls.at(i), file_sizes.at(i)))) {
        LOG_WARN("failed to set refactored to file_map", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      lib::ob_sort(file_urls.get_data(), file_urls.get_data() + file_urls.count());
      for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
        int64_t file_size = 0;
        if (OB_FAIL(file_map.get_refactored(file_urls.at(i), file_size))) {
          if (OB_UNLIKELY(OB_HASH_NOT_EXIST == ret)) {
            ret = OB_ERR_UNEXPECTED;
          }
          LOG_WARN("failed to get key meta", K(ret));
        } else if (OB_FAIL(tmp_file_sizes.push_back(file_size))) {
          LOG_WARN("failed to push back into tmp_file_sizes", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_sizes.assign(tmp_file_sizes))) {
        LOG_WARN("failed to assign file_sizes", K(ret));
      } else if (OB_FAIL(file_map.destroy())) {
        LOG_WARN("failed to destory file_map");
      }
    }
  }
  LOG_TRACE("after filter external table files", K(ret), K(file_urls));
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
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;

  if (!properties.empty()) {
    if (!GCONF._use_odps_jni_connector) {
#if defined (OB_BUILD_CPP_ODPS)
      // Since each partition information of an ODPS table obtained by the ODPS
      // driver is a string, OceanBase treat partition string as an external
      // table filename, one file corresponds to one odps partition, the number
      // of files corresponds to the number of partitions.
      sql::ObODPSTableRowIterator odps_driver;
      sql::ObExternalFileFormat ex_format;
      ex_format.format_type_ = sql::ObExternalFileFormat::ODPS_FORMAT;
      if (OB_FAIL(ex_format.load_from_string(properties, allocator))) {
        LOG_WARN("failed to load from string", K(ret));
      } else if (OB_FAIL(odps_driver.init_tunnel(ex_format.odps_format_))) {
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
            OZ(file_sizes.push_back(part_list_info.at(i).record_count_));
            OZ(file_urls.push_back(ObString(part_spec_src_len, part_spec)));
          }
        }
      } else {
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
        OZ(file_sizes.push_back(part_list_info.at(0).record_count_));
        OZ(file_urls.push_back(ObString("")));
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
      sql::ObExternalFileFormat ex_format;
      sql::ObODPSJNITableRowIterator odps_jni_iter;
      ex_format.format_type_ = sql::ObExternalFileFormat::ODPS_FORMAT;
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
                  OZ(file_sizes.push_back(partition_specs.at(i).record_count_));
                  OZ(file_urls.push_back(ObString(part_spec_src_len, part_spec)));
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
             * 没有partition
             */
            OZ(file_sizes.push_back(partition_specs.at(0).record_count_));
            OZ(file_urls.push_back(ObString("")));
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
    ObSEArray<ObAddr, 8> all_servers;
    share::schema::ObSchemaGetterGuard schema_guard;
    OZ (GCTX.location_service_->external_table_get(tenant_id, table_id, all_servers));
    const bool is_local_storage = ObSQLUtils::is_external_files_on_local_disk(location);
    if (OB_SUCC(ret) && full_path.length() > 0
            && *(full_path.ptr() + full_path.length() - 1) != '/' ) {
      OZ (full_path.append("/"));
    }
    if (OB_FAIL(ret)) {
    } else if (is_local_storage) {
      OZ (collect_local_files_on_servers(tenant_id, location, pattern, regexp_vars, all_servers, file_urls, file_sizes, full_path, allocator));
    } else {
      OZ (ObExternalTableFileManager::get_instance().get_external_file_list_on_device(
            location, pattern, regexp_vars, file_urls, file_sizes, access_info, allocator));
      for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); i++) {
        ObSqlString tmp_file_url;
        ObString &file_url = file_urls.at(i);
        OZ (tmp_file_url.append(full_path.string()));
        OZ (tmp_file_url.append(file_urls.at(i)));
        OZ (ob_write_string(allocator, tmp_file_url.string(), file_url, true));
      }
    }

    OZ (ObExternalTableUtils::sort_external_files(file_urls, file_sizes));
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
      ObArray<int64_t> file_sizes;
      ObSqlString full_path;
      full_path.append(location);
      if (!is_del_all) {
        OZ (collect_external_file_list(nullptr, tenant_id, -1, location, access_info,
                                       pattern, "", false, regexp_vars, allocator, full_path, file_urls, file_sizes));
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

}  // namespace share
}  // namespace oceanbase

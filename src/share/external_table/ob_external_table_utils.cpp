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
#include "share/external_table/ob_external_table_file_mgr.h"
#include "share/external_table/ob_external_table_utils.h"

#include "common/ob_range.h"
#include "common/object/ob_object.h"
#include "share/external_table/ob_external_table_file_mgr.h"
#include "sql/engine/expr/ob_expr_regexp_context.h"
#include "sql/engine/expr/ob_expr_util.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_external_table_access_service.h"
#include "sql/ob_sql_utils.h"
#include "sql/rewrite/ob_query_range.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "deps/oblib/src/lib/net/ob_addr.h"
#include "share/external_table/ob_external_table_file_rpc_processor.h"
#include "share/external_table/ob_external_table_file_rpc_proxy.h"

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
      column_idx >= range.get_end_key().get_obj_cnt() ) {
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
                                                         common::ObIAllocator &allocator,
                                                         common::ObNewRange &new_range)
{
  int ret = OB_SUCCESS;
  ObObj *obj_start = NULL;
  ObObj *obj_end = NULL;
  if (OB_UNLIKELY(first_lineno > last_lineno)) {
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
    new_range.border_flag_.set_inclusive_start();
    new_range.border_flag_.set_inclusive_end();
    new_range.start_key_.assign(obj_start, MAX_EXTERNAL_FILE_SCANKEY);
    new_range.end_key_.assign(obj_end, MAX_EXTERNAL_FILE_SCANKEY);
  }
  return ret;
}

int ObExternalTableUtils::prepare_single_scan_range(const uint64_t tenant_id,
                                                    const uint64_t table_id,
                                                    ObIArray<int64_t> &partition_ids,
                                                    ObIArray<ObNewRange *> &ranges,
                                                    ObIAllocator &range_allocator,
                                                    ObIArray<ObNewRange *> &new_range,
                                                    bool is_file_on_disk) {
  int ret = OB_SUCCESS;
  ObSEArray<ObExternalFileInfo, 16> file_urls;
  ObSEArray<ObNewRange *, 4> tmp_ranges;
  ObSEArray<ObAddr, 16> all_locations;
  if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret));
  } else if (OB_FAIL(tmp_ranges.assign(ranges))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(ObExternalTableFileManager::get_instance().get_external_files_by_part_ids(tenant_id,
                                  table_id, partition_ids, is_file_on_disk, range_allocator, file_urls,
                                  tmp_ranges.empty() ? NULL : &tmp_ranges))) {
    LOG_WARN("get external table file error", K(ret), K(partition_ids));
  } else if (OB_FAIL(GCTX.location_service_->external_table_get(tenant_id, table_id, all_locations))) {
      LOG_WARN("fail to get external table location", K(ret));
  } else if (is_file_on_disk
            && OB_FAIL(ObExternalTableUtils::filter_files_in_locations(file_urls,
                                                                       all_locations))) {
      //For recovered cluster, the file addr may not in the cluster. Then igore it.
      LOG_WARN("filter files in location failed", K(ret));
  } else {
    new_range.reset();
  }
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
    LOG_WARN("convert charset failed", K(ret));
  } else if (OB_FAIL(regex_ctx_.match(temp_allocator_, out_text, 0, match))) {
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
    if (OB_FAIL(ObExprRegexContext::get_regexp_flags(match_string, true, flags))) {
      LOG_WARN("failed to get regexp flags", K(ret));
    } else if (OB_FAIL(regex_ctx_.init(allocator_, regexp_vars,
                                       pattern, flags, true, CS_TYPE_UTF8MB4_BIN))) {
      LOG_WARN("init regex context failed", K(ret), K(pattern));
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

int ObExternalTableUtils::collect_external_file_list(
    const uint64_t tenant_id,
    const uint64_t table_id,
    const ObString &location,
    const ObString &access_info,
    const ObString &pattern,
    const sql::ObExprRegexpSessionVariables &regexp_vars,
    ObIAllocator &allocator,
    ObSqlString &full_path,
    ObIArray<ObString> &file_urls,
    ObIArray<int64_t> &file_sizes)
{
  int ret = OB_SUCCESS;

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
      OZ (ob_write_string(allocator, tmp_file_url.string(), file_url));
    }
  }

  OZ (ObExternalTableUtils::sort_external_files(file_urls, file_sizes));
  return ret;
}

}  // namespace share
}  // namespace oceanbase

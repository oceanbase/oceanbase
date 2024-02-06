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
                                                           const uint64_t ref_table_id,
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
                                               ref_table_id,
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
                                                         const uint64_t ref_table_id,
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
    obj_start[PARTITION_ID].set_uint64(ref_table_id);
    obj_end[PARTITION_ID] = ObObj();
    obj_end[PARTITION_ID].set_uint64(ref_table_id);
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
                                                    ObIArray<ObNewRange *> &ranges,
                                                    ObIAllocator &range_allocator,
                                                    ObIArray<ObNewRange *> &new_range,
                                                    bool is_file_on_disk) {
  int ret = OB_SUCCESS;
  ObSEArray<ObExternalFileInfo, 16> file_urls;
  ObSEArray<ObNewRange *, 4> tmp_ranges;
  if (OB_FAIL(tmp_ranges.assign(ranges))) {
    LOG_WARN("failed to assign array", K(ret));
  } else if (OB_FAIL(ObExternalTableFileManager::get_instance().get_external_files(tenant_id,
                                  table_id, is_file_on_disk, range_allocator, file_urls,
                                  tmp_ranges.empty() ? NULL : &tmp_ranges))) {
    LOG_WARN("get external table file error", K(ret));
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
                                                                          table_id,
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

int ObExternalTableUtils::filter_external_table_files(const ObString &pattern,
                                                      ObExecContext &exec_ctx,
                                                      ObIArray<ObString> &file_urls)
{
  int ret = OB_SUCCESS;
  if (!pattern.empty()) {
    const common::ObCollationType cs_type_pattern = CS_TYPE_UTF8MB4_BIN;
    const common::ObCollationType cs_type_file = CS_TYPE_UTF8MB4_BIN;
    const common::ObCollationType cs_type_match = CS_TYPE_UTF16_BIN;
    ObExprRegexContext regex_ctx;
    ObArenaAllocator allocator;
    uint32_t flags = 0;
    ObString match_string;
    ObSEArray<ObString, 8> tmp_file_urls;
    if (OB_FAIL(ObExprRegexContext::get_regexp_flags(match_string, true, flags))) {
      LOG_WARN("failed to get regexp flags", K(ret));
    } else if (OB_FAIL(regex_ctx.init(exec_ctx.get_allocator(),
                                      exec_ctx.get_my_session(),
                                      pattern,
                                      flags,
                                      true,
                                      cs_type_pattern))) {
      LOG_WARN("init regex context failed", K(ret), K(pattern));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < file_urls.count(); ++i) {
        bool match = false;
        ObString out_text;
        if (OB_FAIL(ObExprUtil::convert_string_collation(file_urls.at(i),
                                                         cs_type_file,
                                                         out_text,
                                                         cs_type_match,
                                                         allocator))) {
          LOG_WARN("convert charset failed", K(ret));
        } else if (OB_FAIL(regex_ctx.match(allocator, out_text, 0, match))) {
          LOG_WARN("regex match failed", K(ret));
        } else if (match && OB_FAIL(tmp_file_urls.push_back(file_urls.at(i)))) {
          LOG_WARN("failed to push back into tmp_file_urls", K(ret));
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(file_urls.assign(tmp_file_urls))) {
        LOG_WARN("failed to assign file_urls", K(ret));
      }
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
      return l.total_file_size_ < r.total_file_size_;
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
  std::sort(sorted_files.begin(), sorted_files.end(),
            [](const FileInfoWithIdx &l, const FileInfoWithIdx &r) -> bool {
              return l.file_info_->file_size_ >= r.file_info_->file_size_; });
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

}  // namespace share
}  // namespace oceanbase

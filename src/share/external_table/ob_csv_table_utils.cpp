/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL
#include "share/external_table/ob_csv_table_utils.h"

#include <cmath>

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace share
{

int ObCsvTableUtils::make_parallel_parse_csv_task(const ObExternalFileInfo &file_info,
                                                       const int64_t first_lineno,
                                                       const int64_t last_lineno,
                                                       const int64_t start_pos,
                                                       const int64_t end_pos,
                                                       const int64_t chunk_idx,
                                                       const int64_t chunk_cnt,
                                                       ObExtTableScanTask *scan_task)
{
  int ret = OB_SUCCESS;
  scan_task->file_url_ = file_info.file_url_;
  scan_task->file_size_ = file_info.file_size_;
  scan_task->modification_time_ = file_info.modify_time_;
  scan_task->file_id_ = file_info.file_id_;
  scan_task->content_digest_ = file_info.content_digest_;
  scan_task->first_lineno_ = first_lineno;
  scan_task->last_lineno_ = last_lineno;
  ObCsvParallelInfo *csv_parallel_info = scan_task->parallel_parse_csv_info_;
  if (OB_ISNULL(csv_parallel_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel parse csv info is null", K(ret));
  } else {
    csv_parallel_info->start_pos_ = start_pos;
    csv_parallel_info->end_pos_ = end_pos;
    csv_parallel_info->chunk_idx_ = chunk_idx;
    csv_parallel_info->chunk_cnt_ = chunk_cnt;
  }
  return ret;
}

int ObCsvTableUtils::make_parallel_parse_csv_task(const ObExtTableScanTask &original_task,
                                                       const int64_t first_lineno,
                                                       const int64_t last_lineno,
                                                       const int64_t start_pos,
                                                       const int64_t end_pos,
                                                       const int64_t chunk_idx,
                                                       const int64_t chunk_cnt,
                                                       ObExtTableScanTask *scan_task)
{
  int ret = OB_SUCCESS;
  scan_task->file_url_ = original_task.file_url_;
  scan_task->file_size_ = original_task.file_size_;
  scan_task->modification_time_ = original_task.modification_time_;
  scan_task->file_id_ = original_task.file_id_;
  scan_task->content_digest_ = original_task.content_digest_;
  scan_task->first_lineno_ = first_lineno;
  scan_task->last_lineno_ = last_lineno;
  ObCsvParallelInfo *csv_parallel_info = scan_task->parallel_parse_csv_info_;
  if (OB_ISNULL(csv_parallel_info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parallel parse csv info is null", K(ret));
  } else {
    csv_parallel_info->start_pos_ = start_pos;
    csv_parallel_info->end_pos_ = end_pos;
    csv_parallel_info->chunk_idx_ = chunk_idx;
    csv_parallel_info->chunk_cnt_ = chunk_cnt;
  }
  return ret;
}

namespace {
bool is_external_file_size_uniform(const common::ObIArray<share::ObExternalFileInfo> &external_table_files)
{
  static constexpr double FILE_SIZE_CV_THRESHOLD = 0.5;
  static constexpr double FILE_SIZE_MAX_AVG_RATIO = 2.0;
  bool is_uniform = true;
  const int64_t file_cnt = external_table_files.count();
  if (file_cnt > 1) {
    int64_t total_size = 0;
    int64_t max_size = 0;
    for (int64_t i = 0; i < file_cnt; ++i) {
      const int64_t file_size = external_table_files.at(i).file_size_;
      total_size += file_size;
      max_size = MAX(max_size, file_size);
    }
    if (total_size > 0) {
      const double mean = static_cast<double>(total_size) / static_cast<double>(file_cnt);
      double var_sum = 0.0;
      for (int64_t i = 0; i < file_cnt; ++i) {
        const double diff = static_cast<double>(external_table_files.at(i).file_size_) - mean;
        var_sum += diff * diff;
      }
      const double stddev = std::sqrt(var_sum / static_cast<double>(file_cnt));
      const double cv = stddev / mean;
      if (cv > FILE_SIZE_CV_THRESHOLD || static_cast<double>(max_size) > mean * FILE_SIZE_MAX_AVG_RATIO) {
        is_uniform = false;
      }
    }
  }
  return is_uniform;
}
}

bool ObCsvTableUtils::is_satisfied_for_parallel_parse_csv(
                             const common::ObIArray<share::ObExternalFileInfo> &external_table_files,
                             const ObCSVGeneralFormat &csv_format,
                             const int64_t parallelism)
{
  bool basic_condition = ObCSVGeneralFormat::ObCSVCompression::NONE == csv_format.compression_algorithm_
                         && csv_format.parallel_parse_on_single_file_
                         && parallelism > 1;
  bool further_condition = false;
  if (basic_condition) {
    ObCollationType collation_type = ObCharset::get_default_collation(csv_format.cs_type_);
    const ObCharsetInfo *charset_info = ObCharset::get_charset(collation_type);
    if (OB_ISNULL(charset_info)) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "got null ptr", K(collation_type), K(lbt()));
    } else if (charset_info->mbmaxlen == 1) {
      further_condition = true;
    } else if (csv_format.line_term_str_.length() == 1
               && csv_format.field_term_str_.length() == 1) {
      if (csv_format.cs_type_ == CHARSET_UTF8MB4) {
        further_condition = true;
      } else if (csv_format.cs_type_ == CHARSET_GBK
                 || csv_format.cs_type_ == CHARSET_GB18030
                 || csv_format.cs_type_ == CHARSET_GB18030_2022) {
        if ((csv_format.field_enclosed_char_ < '0' || csv_format.field_enclosed_char_ > '9')
             && (csv_format.field_escaped_char_ < '0' || csv_format.field_escaped_char_ > '9')
             && (csv_format.line_term_str_[0] < '0' || csv_format.line_term_str_[0] > '9')
             && (csv_format.field_term_str_[0] < '0' || csv_format.field_term_str_[0] > '9')) {
          further_condition = true;
          if (csv_format.cs_type_ == CHARSET_GBK && csv_format.field_escaped_char_ == '\\') {
            further_condition = false;
          }
        }
      }
    }
  }
  bool need_parallel_parse = false;
  if (basic_condition && further_condition && external_table_files.count() > 0) {
    const int64_t file_cnt = external_table_files.count();
    bool has_large_file = false;
    bool has_file_to_chunk = false;
    for (int64_t i = 0; i < file_cnt && (!has_large_file || !has_file_to_chunk); ++i) {
      const int64_t file_size = external_table_files.at(i).file_size_;
      if (file_size >= csv_format.parallel_parse_file_size_threshold_) {
        has_large_file = true;
      }
      if (file_size >= ObExternalTableUtils::PARALLEL_PARSE_CSV_CHUNK_SIZE) {
        has_file_to_chunk = true;
      }
    }
    const bool is_uniform = is_external_file_size_uniform(external_table_files);
    const bool few_files = parallelism * 2 >= file_cnt;
    if (is_uniform) {
      need_parallel_parse = few_files && has_large_file && has_file_to_chunk;
    } else {
      need_parallel_parse = has_large_file && has_file_to_chunk;
    }
  }
  return need_parallel_parse;
}

int ObCsvTableUtils::adjust_end_pos_skip_escape(sql::ObExternalStreamFileReader &file_reader,
                                                     const char escape,
                                                     const int64_t start_pos,
                                                     int64_t &end_pos)
{
  int ret = OB_SUCCESS;
  const int64_t BLOCK_SIZE = 4096;
  char buf[BLOCK_SIZE];
  int64_t read_size = 0;

  if (OB_UNLIKELY(start_pos >= end_pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(start_pos), K(end_pos));
  }
  bool found = false;
  while (OB_SUCC(ret) && end_pos > start_pos && !found && !file_reader.eof()) {
    int64_t read_start = MAX(start_pos, end_pos - BLOCK_SIZE);
    int64_t read_len = end_pos - read_start;
    file_reader.advance(read_start);
    if (OB_FAIL(file_reader.read(buf, read_len, read_size))) {
      LOG_WARN("failed to read file", K(ret), K(read_start), K(read_len));
    } else if (read_size != read_len) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("read size mismatch", K(ret), K(read_size), K(read_len));
    } else {
      for (int64_t i = read_size - 1; !found && i >= 0; --i) {
        if (buf[i] != escape) {
          end_pos = read_start + i + 1;
          found = true;
        }
      }
      if (!found) {
        end_pos = read_start;
      }
    }
  }

  if (OB_SUCC(ret) && !found) {
    end_pos = start_pos;
    LOG_TRACE("end_pos adjusted to start_pos, chunk all escape chars", K(start_pos), K(end_pos));
  }

  return ret;
}

}  // namespace share
}  // namespace oceanbase

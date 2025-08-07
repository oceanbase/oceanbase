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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/engine/basic/ob_select_into_basic.h"
#include "sql/engine/basic/ob_external_file_writer.h"
#include "sql/engine/cmd/ob_load_data_file_reader.h"
#include "sql/engine/basic/ob_select_into_op.h"
#include "sql/session/ob_basic_session_info.h"

using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObDiagnosisManager::~ObDiagnosisManager()
{
  if (OB_NOT_NULL(log_file_writer_)) {
    log_file_writer_->~ObDiagnosisFileWriter();
    log_file_writer_ = nullptr;
  }
  if (OB_NOT_NULL(bad_file_writer_)) {
    bad_file_writer_->~ObDiagnosisFileWriter();
    bad_file_writer_ = nullptr;
  }
}

ObDiagnosisFileWriter::~ObDiagnosisFileWriter()
{
  if (OB_NOT_NULL(data_writer_)) {
    data_writer_->~ObCsvFileWriter();
    data_writer_ = nullptr;
  }
  allocator_ = nullptr;
}

int ObDiagnosisManager::add_warning_info(int err_ret, int line_idx) {
  int ret = OB_SUCCESS;
  if (OB_FAIL(rets_.push_back(err_ret))) {
    LOG_WARN("failed to push back error code into array", K(ret), K(err_ret));
  } else if (OB_FAIL(idxs_.push_back(line_idx))) {
    LOG_WARN("failed to push back line number into array", K(ret), K(line_idx));
  }
  return ret;
}

int ObDiagnosisManager::calc_first_file_path(ObString &path, int64_t sqc_id, int64_t task_id,
                                            bool is_log, ObDiagnosisFileWriter &file_writer)
{
  int ret = OB_SUCCESS;
  ObSqlString file_name_suffix;
  ObString input_file_name = file_writer.file_location_ != IntoFileLocation::SERVER_DISK
                             ? path.split_on('?').trim()
                             : path;
  if (input_file_name.length() == 0 || path.length() == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "invalid file path");
    LOG_WARN("invalid file path", K(ret));
  } else {
    if (input_file_name.ptr()[input_file_name.length() - 1] == '/') {
      OZ(file_name_suffix.append_fmt("%.*s", input_file_name.length(), input_file_name.ptr()));
      OZ(file_name_suffix.append(is_log ? "diagnosis.log" : "data.bad"));
    } else {

      ObCSVGeneralFormat::ObCSVCompression compression_format;
      OZ(compression_algorithm_from_suffix(input_file_name, compression_format));
      if (OB_SUCC(ret)) {
        if (compression_format != ObCSVGeneralFormat::ObCSVCompression::NONE) {
          ObString path_without_suffix = input_file_name.split_on(input_file_name.reverse_find('.'));
          OZ(file_name_suffix.append_fmt("%.*s", path_without_suffix.length(),
                                        path_without_suffix.ptr()));
        } else {
          OZ(file_name_suffix.append_fmt("%.*s", input_file_name.length(), input_file_name.ptr()));
        }
      }
    }
    OZ(file_name_suffix.append_fmt(".%ld_%ld_%ld_%ld",
                                  MTL_ID(), sqc_id, task_id, file_writer.file_id_));

    OZ(file_name_suffix.append(compression_algorithm_to_suffix(file_writer.compress_type_)));

    if (OB_SUCC(ret) && OB_FAIL(ob_write_string(*file_writer.allocator_, file_name_suffix.string(),
                                                file_writer.file_name_))) {
      LOG_WARN("failed to write string", K(ret));
    }
  }
  return ret;
}

int ObDiagnosisManager::init_file_name(const ObString& file_path, int64_t sqc_id, int64_t task_id,
                                      bool is_log, ObDiagnosisFileWriter &file_writer)
{
  int ret = OB_SUCCESS;

  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    file_writer.file_max_size_ = tenant_config->load_data_diagnosis_log_max_size;

    if (OB_FAIL(ObFileReadParam::parse_compression_format(
                                          tenant_config->load_data_diagnosis_log_compression.str(),
                                          file_path, file_writer.compress_type_))) {
      LOG_WARN("failed to set audit log compress type", K(ret));
    }
  }

  ObBackupDest dest;
  ObBackupPath path;
  ObBackupIoAdapter util;

  ObSqlString file_location_with_prefix;

  ObString log_file_path = file_path.trim();
  if (log_file_path.prefix_match_ci(OB_OSS_PREFIX)) {
    file_writer.file_location_ = IntoFileLocation::REMOTE_OSS;
  } else if (log_file_path.prefix_match_ci(OB_COS_PREFIX)) {
    file_writer.file_location_  = IntoFileLocation::REMOTE_COS;
  } else if (log_file_path.prefix_match_ci(OB_S3_PREFIX)) {
    file_writer.file_location_  = IntoFileLocation::REMOTE_S3;
  } else if (log_file_path.prefix_match_ci(OB_FILE_PREFIX)) {
    file_writer.file_location_  = IntoFileLocation::SERVER_DISK;
  } else {
    file_writer.file_location_  = IntoFileLocation::SERVER_DISK;
    OZ(file_location_with_prefix.append(OB_FILE_PREFIX));
    OZ(file_location_with_prefix.append(log_file_path));
    log_file_path = file_location_with_prefix.string();
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(dest.set(log_file_path.ptr()))) {
      LOG_WARN("failed to set backup dest", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    bool is_exist = false;
    if (OB_FAIL(calc_first_file_path(log_file_path, sqc_id, task_id, is_log, file_writer))) {
      LOG_WARN("failed to calculate first file path", K(ret));
    } else if (OB_FAIL(util.is_exist(file_writer.file_name_, dest.get_storage_info(), is_exist))) {
      LOG_WARN("fail to check file exist", KR(ret), K(file_writer.file_name_));
    } else if (is_exist) {
      ret = OB_FILE_ALREADY_EXIST;
      LOG_WARN("file already exist", KR(ret), K(file_writer.file_name_), K(dest.get_storage_info()));
    } else if (OB_FAIL(util.mk_parent_dir(file_writer.file_name_, dest.get_storage_info()))) {
      LOG_WARN("failed to make parent dir", K(ret), K(dest));
    } else if (IntoFileLocation::SERVER_DISK == file_writer.file_location_ &&
              OB_FAIL(ObSelectIntoOp::check_secure_file_path(
                                          ObString(log_file_path.length() - strlen(OB_FILE_PREFIX),
                                                  log_file_path.ptr() + strlen(OB_FILE_PREFIX))))) {
      LOG_WARN("failed to check secure file path", K(ret));
    }
  }
  return ret;
}

int ObDiagnosisManager::gen_csv_line_data(int64_t err_ret, int64_t idx, ObString file_name,
                          ObString err_message, ObString& result, common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  ObSqlString line_data;
  if (!is_header_written_) {
    OZ (line_data.append("\'ERROR CODE\',\'FILE NAME\',\'LINE NUMBER\',\'ERROR MESSAGE\'\n"));
    is_header_written_ = true;
  }

  OZ (line_data.append_fmt("\'%ld\',\'%.*s\',\'%ld\',\'%.*s\'\n",
                      err_ret, file_name.length(), file_name.ptr(),
                      idx, err_message.length(), err_message.ptr()));

  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, line_data.string(), result))) {
    LOG_WARN("failed to write string", K(ret));
  }
  return ret;
}



int ObDiagnosisManager::calc_next_file_path(ObString& file_name,
                                            int64_t& file_id,
                                            ObCSVGeneralFormat::ObCSVCompression compress_type,
                                            common::ObIAllocator &allocator)
{

  int ret = OB_SUCCESS;

  ObSqlString url_with_suffix;
  ObString file_path;

  file_path = file_name.split_on(file_name.reverse_find('_'));
  if (OB_FAIL(url_with_suffix.assign(file_path))) {
    LOG_WARN("failed to assign string", K(ret));
  } else if (OB_FAIL(url_with_suffix.append_fmt("_%ld", ++file_id))) {
    LOG_WARN("failed to append string", K(ret));
  }

  OZ(url_with_suffix.append(compression_algorithm_to_suffix(compress_type)));

  if (OB_SUCC(ret) && OB_FAIL(ob_write_string(allocator, url_with_suffix.string(), file_name))) {
    LOG_WARN("failed to write string", K(ret));
  }
  return ret;
}

int ObDiagnosisFileWriter::create_data_writer(const ObString& log_file)
{
  int ret = OB_SUCCESS;
  has_compress_ = compress_type_ != CsvCompressType::NONE;

  if (file_location_ != IntoFileLocation::SERVER_DISK) {
    ObString log_file_path = log_file.trim();
    ObString temp_url = log_file_path.split_on('?');
    ObString storage_info;
    common::ObStorageType device_type;
    if (OB_FAIL(get_storage_type_from_path(file_name_, device_type))) {
      LOG_WARN("failed to get storage type from path", K(ret), K(file_name_));
    } else if (OB_FAIL(ob_write_string(*allocator_, log_file_path, storage_info, true))) {
      LOG_WARN("failed to append string", K(ret));
    } else if (OB_FAIL(access_info_.set(device_type, storage_info.ptr()))) {
      LOG_WARN("failed to set access info", K(ret), K(log_file));
    } else if (!access_info_.is_valid()) {
      ret = OB_FILE_NOT_EXIST;
      LOG_WARN("file path not exist", K(ret), K(file_name_), K(access_info_));
    }
  }

  void *ptr = NULL;
  if (OB_ISNULL(ptr = allocator_->alloc(sizeof(ObCsvFileWriter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate data writer", K(ret), K(sizeof(ObCsvFileWriter)));
  } else {
    data_writer_ = new(ptr) ObCsvFileWriter(&access_info_, file_location_, use_shared_buf_,
                                            has_compress_, has_lob_, write_offset_);
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(data_writer_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null", K(ret));
    } else {
      // for local disk file, "file://" should be removed
      ObString file_name = IntoFileLocation::SERVER_DISK == file_location_ ?
                                              ObString(file_name_.length() - strlen(OB_FILE_PREFIX),
                                                      file_name_.ptr() + strlen(OB_FILE_PREFIX)) :
                                              file_name_;
      if (OB_FAIL(ob_write_string(*allocator_, file_name, data_writer_->url_))) {
        LOG_WARN("failed to append string", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (has_compress_ && OB_ISNULL(data_writer_->get_compress_stream_writer())
               && OB_FAIL(data_writer_->init_compress_writer(*allocator_,
                                                                compress_type_,
                                                                2LL * 1024 * 1024))) {
      LOG_WARN("failed to init compress stream writer", K(ret));
    }
  }
  return ret;
}

int ObDiagnosisManager::close()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(log_file_writer_) &&
      OB_NOT_NULL(log_file_writer_->data_writer_) &&
      OB_FAIL(log_file_writer_->data_writer_->close_data_writer())) {
    LOG_WARN("failed to close data writer", K(ret));
  }

  if (OB_NOT_NULL(bad_file_writer_) &&
      OB_NOT_NULL(bad_file_writer_->data_writer_) &&
      OB_FAIL(bad_file_writer_->data_writer_->close_data_writer())) {
    LOG_WARN("failed to close data writer", K(ret));
  }
  return ret;
}

int ObDiagnosisManager::init_file_writer(common::ObIAllocator &allocator,
                                        const ObString& file_name,
                                        int64_t sqc_id,
                                        int64_t task_id,
                                        bool is_log_file,
                                        ObDiagnosisFileWriter *&file_writer)
{
  int ret = OB_SUCCESS;
  ObDiagnosisFileWriter *local_file_writer = NULL;
  if (OB_UNLIKELY(file_writer != NULL)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("file_writer is not null");
  } else if (OB_UNLIKELY(NULL == (local_file_writer = static_cast<ObDiagnosisFileWriter *>(
      allocator.alloc(sizeof(ObDiagnosisFileWriter)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to create file_writer");
  } else {
    file_writer = new (local_file_writer) ObDiagnosisFileWriter();
    file_writer->allocator_ = &allocator;
  }

  if (OB_SUCC(ret)) {
    ObString cstyle_file_name;
    if (OB_FAIL(ob_write_string(allocator, file_name, cstyle_file_name, true /* c_style */))) {
      LOG_WARN("failed to write string with c_style", K(ret), K(file_name));
    } else if (OB_FAIL(init_file_name(cstyle_file_name, sqc_id, task_id, is_log_file, *file_writer))) {
      LOG_WARN("failed to init log file name", K(ret));
    } else if (OB_FAIL(file_writer->create_data_writer(cstyle_file_name))) {
        LOG_WARN("failed to get device", K(ret));
    }
  }
  return ret;
}

bool ObDiagnosisFileWriter::should_split() {
  return data_writer_->get_curr_file_pos() > file_max_size_;
}

int ObDiagnosisFileWriter::split_file()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_writer_->close_file())) {
    LOG_WARN("failed to close file", K(ret));
  } else if (ObDiagnosisManager::calc_next_file_path(file_name_, file_id_,
                                                    compress_type_, *allocator_)) {
    LOG_WARN("failed to calculate next file path", K(ret), K(file_name_));
  }

  if (OB_SUCC(ret)) {
    ObString file_name = IntoFileLocation::SERVER_DISK == file_location_ ?
                                            ObString(file_name_.length() - strlen(OB_FILE_PREFIX),
                                                    file_name_.ptr() + strlen(OB_FILE_PREFIX)) :
                                            file_name_;
    if (OB_FAIL(ob_write_string(*allocator_, file_name, data_writer_->url_))) {
      LOG_WARN("failed to append string", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (has_compress_) {
      data_writer_->get_compress_stream_writer()->reuse();
    }
  }
  return ret;
}

int ObDiagnosisManager::handle_warning(int64_t err_ret, int64_t idx, ObString col_name,
                                      ObString log_file, ObWarningBuffer *buffer, int64_t limit_cnt)
{
  int ret = OB_SUCCESS;

  ObString line_data;
  ObArenaAllocator allocator;
  ObSqlString err_msg;
  int64_t line_num = !col_name.empty() ? idx + cur_line_number_ : idx;
  if (!col_name.empty()) {
    if (OB_FAIL(err_msg.append_fmt("fail to scan file %.*s at line %ld for column %.*s, error: %s",
                                                  cur_file_url_.length(), cur_file_url_.ptr(),
                                                  line_num, col_name.length(), col_name.ptr(),
                                                  common::ob_strerror(err_ret)))) {
      LOG_WARN("failed to append error message", K(err_ret));
    }
  } else {
    if (OB_FAIL(err_msg.append_fmt("error: %s for file %.*s at line %ld",
                                                  common::ob_strerror(err_ret),
                                                  cur_file_url_.length(), cur_file_url_.ptr(),
                                                  line_num))) {
      LOG_WARN("failed to append error message", K(err_ret));
    }
  }

  if (OB_SUCC(ret) && !log_file.empty()) {
    if  (OB_FAIL(gen_csv_line_data(err_ret, line_num, cur_file_url_,
                                  err_msg.string(), line_data, allocator))) {
      LOG_WARN("fail to gen csv line data", K(ret));
    } else if (OB_FAIL(log_file_writer_->data_writer_->flush_data(line_data.ptr(),
                                                                  line_data.length()))) {
      LOG_WARN("failed to flush data", K(ret), K(line_data));
    } else if (log_file_writer_->should_split()) {
      if (OB_FAIL(log_file_writer_->split_file())) {
        LOG_WARN("failed to split data file", K(ret));
      } else {
        is_header_written_ = false;
      }
    }
  }

  if (OB_SUCC(ret)) {
    buffer->append_warning(err_msg.ptr(), err_ret);

    if (limit_cnt >= 0 && buffer->get_total_warning_count() > limit_cnt) {
      ret = OB_REACH_DIAGNOSIS_ERROR_LIMIT;
      LOG_WARN("the count of warnings has exceeded the limit.", K(ret), K(limit_cnt));
    }
  }

  return ret;
}

void ObDiagnosisManager::reuse()
{
  idxs_.reuse();
  rets_.reuse();
  col_names_.reuse();
  data_.reuse();
  missing_col_idxs_.reuse();

  allocator_.reuse();
}

int ObDiagnosisManager::do_diagnosis(ObBitVector &skip,
                                    const ObDiagnosisInfo& diagnosis_info,
                                    int64_t sqc_id,
                                    int64_t task_id,
                                    common::ObIAllocator &allocator) {
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (idxs_.count() != rets_.count() || idxs_.count() != col_names_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("idxs_, col_names_ and rets_ count mismatch",
            K(ret), K(idxs_.count()), K(col_names_.count()), K(rets_.count()));
  } else if (idxs_.count() > 0 || missing_col_idxs_.count() > 0) {
    // init file writer if needed
    if (!is_file_writer_inited_) {
      if (!diagnosis_info.log_file_.empty() &&
          OB_FAIL(init_file_writer(allocator, diagnosis_info.log_file_,
                                  sqc_id, task_id, true, log_file_writer_))) {
        LOG_WARN("failed to init log file writer", K(ret));
      } else if (!diagnosis_info.bad_file_.empty() &&
                OB_FAIL(init_file_writer(allocator, diagnosis_info.bad_file_,
                                        sqc_id, task_id, false, bad_file_writer_))) {
        LOG_WARN("failed to init log file writer", K(ret));
      } else {
        is_file_writer_inited_ = true;
      }
    }

    if (OB_SUCC(ret)) {
      if (cur_file_url_.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("missing cur_file_url", K(ret));
      } else {
        ObWarningBuffer *buffer = ob_get_tsi_warning_buffer();

        for (int i = 0; OB_SUCC(ret) && i < idxs_.count(); i++) {
          int64_t idx = idxs_.at(i);
          if (skip.at(idx)) {
            continue;
          }

          // bad file
          if (OB_SUCC(ret) && !diagnosis_info.bad_file_.empty()) {
            if (idx >= data_.count()) {
              LOG_WARN("data index overflow in do_diagnosis", K(ret), K(idx), K(data_.count()));
            } else if (OB_FAIL(bad_file_writer_->data_writer_->flush_data(data_.at(idx).ptr(),
                                                                  data_.at(idx).length()))) {
              LOG_WARN("failed to flush data", K(ret), K(data_.at(idx)));
            } else if(bad_file_writer_->should_split() && OB_FAIL(bad_file_writer_->split_file())) {
              LOG_WARN("failed to split data file", K(ret));
            }
          }

          // warning log
          if (OB_SUCC(ret)) {
            if (OB_FAIL(handle_warning(rets_.at(i), idx, col_names_.at(i), diagnosis_info.log_file_,
                                      buffer, diagnosis_info.limit_num_))) {
              LOG_WARN("failed to handle warning", K(ret), K(rets_.at(i)), K(idx));
            } else {
              skip.set(idx);
            }
          }
        }

        // lines that doesn't contain all columns(found by csv parser)
        for (int i = 0; OB_SUCC(ret) && i < missing_col_idxs_.count(); i++) {
          int64_t idx = missing_col_idxs_.at(i);
          int64_t err_ret = OB_WARN_TOO_FEW_RECORDS;

          if (OB_FAIL(handle_warning(err_ret, idx, ObString::make_empty_string(),
                                    diagnosis_info.log_file_, buffer, diagnosis_info.limit_num_))) {
            LOG_WARN("failed to handle warning", K(ret), K(err_ret), K(idx));
          }
        }

        reuse();
      }
    }
  } else {
    reuse();
  }

  return ret;
}
}/* ns sql*/
}/* ns oceanbase */

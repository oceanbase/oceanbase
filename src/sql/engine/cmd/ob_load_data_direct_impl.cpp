/**
 * Copyright (c) 2023 OceanBase
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

#include "sql/engine/cmd/ob_load_data_direct_impl.h"
#include "observer/omt/ob_tenant.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_coordinator_ctx.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_task.h"
#include "observer/table_load/ob_table_load_task_scheduler.h"
#include "observer/mysql/ob_query_driver.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/ob_device_manager.h"
#include "share/backup/ob_backup_io_adapter.h"
#include "observer/table_load/backup/ob_table_load_backup_table.h"
#include "share/stat/ob_dbms_stats_utils.h"

namespace oceanbase
{
namespace sql
{
using namespace common;
using namespace observer;
using namespace share;
using namespace storage;
using namespace table;
using namespace omt;

/**
 * DataAccessParam
 */

ObLoadDataDirectImpl::DataAccessParam::DataAccessParam()
  : file_column_num_(0), file_cs_type_(CS_TYPE_INVALID)
{
}

bool ObLoadDataDirectImpl::DataAccessParam::is_valid() const
{
  return file_column_num_ > 0 && CS_TYPE_INVALID != file_cs_type_;
}

/**
 * LoadExecuteParam
 */

ObLoadDataDirectImpl::LoadExecuteParam::LoadExecuteParam()
  : tenant_id_(OB_INVALID_ID),
    database_id_(OB_INVALID_ID),
    table_id_(OB_INVALID_ID),
    parallel_(0),
    thread_count_(0),
    batch_row_count_(0),
    data_mem_usage_limit_(0),
    need_sort_(false),
    online_opt_stat_gather_(false),
    max_error_rows_(-1),
    ignore_row_num_(-1),
    dup_action_(ObLoadDupActionType::LOAD_INVALID_MODE),
    method_(ObDirectLoadMethod::INVALID_METHOD),
    insert_mode_(ObDirectLoadInsertMode::INVALID_INSERT_MODE)
{
  column_ids_.set_tenant_id(MTL_ID());
}

bool ObLoadDataDirectImpl::LoadExecuteParam::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && OB_INVALID_ID != database_id_ &&
         OB_INVALID_ID != table_id_ && !database_name_.empty() && !table_name_.empty() &&
         !combined_name_.empty() && parallel_ > 0 && thread_count_ > 0 && batch_row_count_ > 0 &&
         data_mem_usage_limit_ > 0 && max_error_rows_ >= 0 && ignore_row_num_ >= 0 &&
         ObLoadDupActionType::LOAD_INVALID_MODE != dup_action_ &&
         ObDirectLoadMethod::is_type_valid(method_) &&
         ObDirectLoadInsertMode::is_type_valid(insert_mode_) &&
         (storage::ObDirectLoadMethod::is_full(method_)
            ? storage::ObDirectLoadInsertMode::is_valid_for_full_method(insert_mode_)
            : true) &&
         (storage::ObDirectLoadMethod::is_incremental(method_)
            ? storage::ObDirectLoadInsertMode::is_valid_for_incremental_method(insert_mode_)
            : true) &&
         (storage::ObDirectLoadInsertMode::INC_REPLACE == insert_mode_
            ? sql::ObLoadDupActionType::LOAD_REPLACE == dup_action_
            : true) &&
         data_access_param_.is_valid() && !column_ids_.empty();
}

/**
 * LoadExecuteContext
 */

ObLoadDataDirectImpl::LoadExecuteContext::LoadExecuteContext()
  : allocator_(nullptr),
    direct_loader_(nullptr),
    job_stat_(nullptr),
    logger_(nullptr)
{
}

bool ObLoadDataDirectImpl::LoadExecuteContext::is_valid() const
{
  return exec_ctx_.is_valid() && nullptr != direct_loader_ && nullptr != job_stat_ &&
         nullptr != logger_;
}

/**
 * Logger
 */

const char *ObLoadDataDirectImpl::Logger::log_file_column_names =
  "\nFile\tRow\tErrCode\tErrMsg\t\n";
const char *ObLoadDataDirectImpl::Logger::log_file_row_fmt = "%.*s\t%ld\t%d\t%s\t\n";

ObLoadDataDirectImpl::Logger::Logger()
  : is_oracle_mode_(false),
    buf_(nullptr),
    is_create_log_succ_(false),
    err_cnt_(0),
    max_error_rows_(0),
    is_inited_(false)
{
}

ObLoadDataDirectImpl::Logger::~Logger()
{
  if (nullptr != buf_) {
    ob_free(buf_);
    buf_ = nullptr;
  }
}

int ObLoadDataDirectImpl::Logger::init(const ObString &load_info, int64_t max_error_rows)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::Logger init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(load_info.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(load_info));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(
                         ob_malloc(DEFAULT_BUF_LENGTH, ObMemAttr(MTL_ID(), "MTL_LogBuffer"))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    if (OB_SUCCESS != create_log_file(load_info)) {
      is_create_log_succ_ = false;
    } else {
      is_create_log_succ_ = true;
    }
    is_oracle_mode_ = lib::is_oracle_mode();
    max_error_rows_ = max_error_rows;
    is_inited_ = true;
  }
  return ret;
}

int ObLoadDataDirectImpl::Logger::create_log_file(const ObString &load_info)
{
  int ret = OB_SUCCESS;
  ObString file_name;
  if (OB_FAIL(generate_log_file_name(buf_, DEFAULT_BUF_LENGTH, file_name))) {
    LOG_WARN("fail to generate log file name", KR(ret));
  } else if (OB_FAIL(file_appender_.open(file_name, false, true))) {
    LOG_WARN("fail to open file", KR(ret), K(file_name));
  } else if (OB_FAIL(file_appender_.append(load_info.ptr(), load_info.length(), true))) {
    LOG_WARN("fail to append log", KR(ret));
  } else if (OB_FAIL(file_appender_.append(log_file_column_names, strlen(log_file_column_names),
                                            true))) {
    LOG_WARN("fail to append log", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectImpl::Logger::generate_log_file_name(char *buf, int64_t size,
                                                         ObString &file_name)
{
  int ret = OB_SUCCESS;
  const char *dict = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
  const int dict_len = strlen(dict); // length of dict
  const char *file_prefix = "log/obloaddata.log.";
  const int64_t prefix_len = strlen(file_prefix);
  const int64_t log_file_id_len = 6;
  if (OB_UNLIKELY(prefix_len + log_file_id_len > size)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("log file name buf overflow", KR(ret), K(size));
  } else {
    const int64_t cur_ts = ObTimeUtil::current_time();
    uint32_t hash_ts = ::murmurhash2(&cur_ts, sizeof(cur_ts), 0);
    // copy prefix
    MEMCPY(buf, file_prefix, prefix_len);
    // generate file id
    char *id_buf = buf + prefix_len;
    for (int64_t i = 0; i < log_file_id_len; ++i) {
      id_buf[i] = dict[hash_ts % dict_len];
      hash_ts /= dict_len;
    }
    // assign string
    file_name.assign(buf, prefix_len + log_file_id_len);
  }
  return ret;
}

int ObLoadDataDirectImpl::Logger::log_error_line(const ObString &file_name, int64_t line_no,
                                                 int err_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::Logger not init", KR(ret), KP(this));
  } else {
    if (is_create_log_succ_) {
      int tmp_ret = OB_SUCCESS;
      const char *err_msg = ob_errpkt_strerror(err_code, is_oracle_mode_);
      const int err_no = ob_errpkt_errno(err_code, is_oracle_mode_);
      int64_t pos = 0;
      lib::ObMutexGuard guard(mutex_);
      if (OB_TMP_FAIL(databuff_printf(buf_, DEFAULT_BUF_LENGTH, pos, log_file_row_fmt,
                                      file_name.length(), file_name.ptr(), line_no, err_no,
                                      err_msg))) {
        LOG_WARN("fail to databuff printf", KR(tmp_ret), K(line_no), K(err_no), K(err_msg));
      } else if (OB_TMP_FAIL(file_appender_.append(buf_, pos, false))) {
        LOG_WARN("fail to append log", KR(tmp_ret), K(pos), K(line_no), K(err_no), K(err_msg));
      }
    }
    if (inc_error_count() > max_error_rows_) {
      ret = OB_ERR_TOO_MANY_ROWS;
      LOG_WARN("error row count reaches its maximum value", KR(ret), K(max_error_rows_),
               K(err_cnt_));
    }
  }
  return ret;
}

/**
 * DataDescIterator
 */

ObLoadDataDirectImpl::DataDescIterator::DataDescIterator()
  : pos_(0)
{
  data_descs_.set_tenant_id(MTL_ID());
}

ObLoadDataDirectImpl::DataDescIterator::~DataDescIterator()
{
}

int ObLoadDataDirectImpl::DataDescIterator::copy(const ObLoadFileIterator &file_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!file_iter.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(file_iter));
  } else {
    ObLoadFileIterator copy_file_iter;
    DataDesc data_desc;
    int64_t file_idx = 0;
    if (OB_FAIL(copy_file_iter.copy(file_iter))) {
      LOG_WARN("fail to copy file iter", KR(ret));
    }
    while (OB_SUCC(ret)) {
      data_desc.file_idx_ = file_idx++;
      if (OB_FAIL(copy_file_iter.get_next_file(data_desc.filename_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next file", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(data_descs_.push_back(data_desc))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::DataDescIterator::copy(const DataDescIterator &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(0 == other.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(other));
  } else if (OB_FAIL(data_descs_.assign(other.data_descs_))) {
    LOG_WARN("fail to assign data descs", KR(ret));
  } else {
    pos_ = 0;
  }
  return ret;
}

int ObLoadDataDirectImpl::DataDescIterator::add_data_desc(const DataDesc &data_desc)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(data_descs_.push_back(data_desc))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectImpl::DataDescIterator::get_next_data_desc(DataDesc &data_desc, int64_t &pos)
{
  int ret = OB_SUCCESS;
  if (pos_ >= data_descs_.count()) {
    ret = OB_ITER_END;
  } else {
    pos = pos_;
    data_desc = data_descs_.at(pos_++);
  }
  return ret;
}

/**
 * DataBuffer
 */

ObLoadDataDirectImpl::DataBuffer::DataBuffer()
  : file_buffer_(nullptr), pos_(0), is_end_file_(false)
{
}

ObLoadDataDirectImpl::DataBuffer::~DataBuffer()
{
  reset();
}

void ObLoadDataDirectImpl::DataBuffer::reuse()
{
  if (nullptr != file_buffer_) {
    file_buffer_->reset();
  }
  pos_ = 0;
  is_end_file_ = false;
}

void ObLoadDataDirectImpl::DataBuffer::reset()
{
  if (nullptr != file_buffer_) {
    file_buffer_->~ObLoadFileBuffer();
    ob_free(file_buffer_);
    file_buffer_ = nullptr;
  }
  pos_ = 0;
}

int ObLoadDataDirectImpl::DataBuffer::init(int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr != file_buffer_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::DataBuffer init twice", KR(ret), KPC(file_buffer_));
  } else if (OB_UNLIKELY(capacity <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid buffer capacity", KR(ret), K(capacity));
  } else {
    const int64_t alloc_size =
      MIN(capacity + sizeof(ObLoadFileBuffer), ObLoadFileBuffer::MAX_BUFFER_SIZE);
    ObMemAttr attr(MTL_ID(), "MTL_DataBuffer");
    void *buf = nullptr;
    if (OB_ISNULL(buf = ob_malloc(alloc_size, attr))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory", KR(ret), K(alloc_size));
    } else {
      file_buffer_ = new (buf) ObLoadFileBuffer(alloc_size - sizeof(ObLoadFileBuffer));
    }
  }
  return ret;
}

bool ObLoadDataDirectImpl::DataBuffer::is_valid() const
{
  return nullptr != file_buffer_ && pos_ >= 0 && pos_ <= file_buffer_->get_data_len();
}

int64_t ObLoadDataDirectImpl::DataBuffer::get_data_length() const
{
  int64_t len = 0;
  if (is_valid()) {
    len = file_buffer_->get_data_len() - pos_;
  }
  return len;
}

int64_t ObLoadDataDirectImpl::DataBuffer::get_remain_length() const
{
  int64_t len = 0;
  if (is_valid()) {
    len = file_buffer_->get_remain_len();
  }
  return len;
}

bool ObLoadDataDirectImpl::DataBuffer::empty() const { return 0 == get_data_length(); }

char *ObLoadDataDirectImpl::DataBuffer::data() const
{
  char *buf = nullptr;
  if (is_valid()) {
    buf = file_buffer_->begin_ptr() + pos_;
  }
  return buf;
}

void ObLoadDataDirectImpl::DataBuffer::advance(int64_t length)
{
  OB_ASSERT(get_data_length() >= length);
  pos_ += length;
}

void ObLoadDataDirectImpl::DataBuffer::update_data_length(int64_t length)
{
  OB_ASSERT(get_remain_length() >= length);
  file_buffer_->update_pos(length);
}

int ObLoadDataDirectImpl::DataBuffer::squash()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid buffer", KR(ret));
  } else {
    const int64_t length = file_buffer_->get_data_len() - pos_;
    if (length > 0) {
      MEMMOVE(file_buffer_->begin_ptr(), file_buffer_->begin_ptr() + pos_, length);
    }
    reuse();
    file_buffer_->update_pos(length);
  }
  return ret;
}

void ObLoadDataDirectImpl::DataBuffer::swap(DataBuffer &other)
{
  std::swap(file_buffer_, other.file_buffer_);
  std::swap(pos_, other.pos_);
}

/**
 * DataReader
 */

ObLoadDataDirectImpl::DataReader::DataReader()
    : allocator_("TLD_DataReader"),
      execute_ctx_(nullptr),
      file_reader_(nullptr),
      end_offset_(-1),
      read_raw_(false),
      is_iter_end_(false),
      is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObLoadDataDirectImpl::DataReader::~DataReader()
{
  if (OB_NOT_NULL(file_reader_)) {
    file_reader_->~ObFileReader();
    file_reader_ = nullptr;
  }
}

int ObLoadDataDirectImpl::DataReader::init(const DataAccessParam &data_access_param,
                                           LoadExecuteContext &execute_ctx,
                                           const DataDesc &data_desc, bool read_raw)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::DataReader init twice", KR(ret), KP(this));
  } else {
    allocator_.set_tenant_id(MTL_ID());
    execute_ctx_ = &execute_ctx;
    read_raw_ = read_raw;
    if (OB_FAIL(csv_parser_.init(data_access_param.file_format_, data_access_param.file_column_num_,
                                 data_access_param.file_cs_type_))) {
      LOG_WARN("fail to init csv parser", KR(ret), K(data_access_param));
    }
    if (OB_SUCC(ret) && !read_raw) {
      ObCSVFormats formats;
      formats.init(data_access_param.file_format_);
      if (OB_FAIL(data_trimer_.init(allocator_, formats))) {
        LOG_WARN("fail to init data trimer", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      end_offset_ = data_desc.end_;

      ObFileReadParam file_read_param;
      file_read_param.file_location_ = data_access_param.file_location_;
      file_read_param.filename_      = data_desc.filename_;
      file_read_param.access_info_   = data_access_param.access_info_;
      file_read_param.packet_handle_ = &execute_ctx.exec_ctx_.get_session_info()->get_pl_query_sender()->get_packet_sender();
      file_read_param.session_       = execute_ctx.exec_ctx_.get_session_info();
      file_read_param.timeout_ts_    = THIS_WORKER.get_timeout_ts();

      if (OB_FAIL(ObFileReader::open(file_read_param, allocator_, file_reader_))) {
        LOG_WARN("failed to open file", KR(ret), K(data_desc));
      } else if (file_reader_->seekable()) {

        if (end_offset_ == -1 && OB_FAIL(file_reader_->get_file_size(end_offset_))) {
          LOG_WARN("fail to get file size", KR(ret), K(data_desc));
        } else {
          file_reader_->seek(data_desc.start_);
          ATOMIC_AAF(&execute_ctx_->job_stat_->total_bytes_, (end_offset_ - data_desc.start_));
        }
      } else if (data_desc.start_ != 0) {
        ret = OB_NOT_SUPPORTED; // should not happen
        LOG_WARN("file reader asked to seek while not supported by unseekable file", KR(ret), K(file_read_param));
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::DataReader::read_buffer(ObLoadFileBuffer &file_buffer)
{
  int ret = OB_SUCCESS;
  int64_t read_count = file_buffer.get_remain_len();
  if (0 == read_count) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("cannot read more data as buffer is full", KR(ret));
  } else if (!is_end_file()) {
    int64_t read_size = 0;
    if (end_offset_ > 0 && read_count > (end_offset_ - file_reader_->get_offset())) {
      read_count = end_offset_ - file_reader_->get_offset();
    }

    if (OB_FAIL(file_reader_->readn(file_buffer.current_ptr(), read_count, read_size))) {
      LOG_WARN("fail to read file", KR(ret));
    } else if (0 == read_size) {
      LOG_TRACE("read nothing", K(is_end_file()));
    } else {
      file_buffer.update_pos(read_size); // 更新buffer中数据长度
      LOG_TRACE("read file sucess", K(read_size));
      ATOMIC_AAF(&execute_ctx_->job_stat_->read_bytes_, read_size);
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::DataReader::get_next_buffer(ObLoadFileBuffer &file_buffer,
                                                      int64_t &line_count, int64_t limit)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::DataReader not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(limit <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(limit));
  } else if (OB_UNLIKELY(read_raw_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read complete line buffer", KR(ret));
  } else if (is_iter_end_) {
    ret = OB_ITER_END;
  } else {
    file_buffer.reset();
    line_count = 0;

    // 1. 从data_trimer中恢复出上次读取留下的数据
    if (OB_FAIL(data_trimer_.recover_incomplate_data(file_buffer))) {
      LOG_WARN("fail to recover incomplate data", KR(ret));
    }
    // 2. 读取数据，然后从buffer中找出完整的行，剩下的数据缓存到data_trimer
    if (OB_SUCC(ret)) {
      int64_t complete_cnt = limit;
      int64_t complete_len = 0;

      if (OB_FAIL(read_buffer(file_buffer))) {
        LOG_WARN("failed to read buffer as there is not enough data to parse", KR(ret));
      } else if (!file_buffer.is_valid()) {
        is_iter_end_ = true;
        ret = OB_ITER_END;
      } else {
        int64_t complete_cnt = limit;
        int64_t complete_len = 0;
        if (OB_FAIL(ObLoadDataBase::pre_parse_lines(file_buffer, csv_parser_, is_end_file(),
                                                    complete_len, complete_cnt))) {
          LOG_WARN("fail to fast_lines_parse", KR(ret));
        } else if (OB_UNLIKELY(0 == complete_len)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("direct-load does not support big row", KR(ret), "size",
                   file_buffer.get_data_len());
          FORWARD_USER_ERROR_MSG(ret, "direct-load does not support big row, row_size = %ld", file_buffer.get_data_len());
        } else if (OB_FAIL(data_trimer_.backup_incomplate_data(file_buffer, complete_len))) {
          LOG_WARN("fail to back up data", KR(ret));
        } else {
          line_count = complete_cnt;
          LOG_DEBUG("LOAD DATA backup", "data", data_trimer_.get_incomplate_data_string());
        }
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::DataReader::get_next_raw_buffer(DataBuffer &data_buffer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::DataReader not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!read_raw_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected read raw buffer", KR(ret));
  } else if (is_end_file()) {
    ret = OB_ITER_END;
  } else if (data_buffer.get_remain_length() > 0) {
    int64_t read_count = data_buffer.get_remain_length();
    if (file_reader_->seekable() && read_count > end_offset_ - file_reader_->get_offset()) {
      read_count = end_offset_ - file_reader_->get_offset();
    }
    int64_t read_size = 0;
    if (OB_FAIL(file_reader_->readn(data_buffer.data() + data_buffer.get_data_length(), read_count,
                                  read_size))) {
      LOG_WARN("fail to read file", KR(ret));
    } else if (read_size > 0) {
      data_buffer.update_data_length(read_size);
      ATOMIC_AAF(&execute_ctx_->job_stat_->read_bytes_, read_size);
    } else {
      // read_size == 0
      if (is_end_file()) {
        ret = OB_ITER_END;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("read buffer got unexpected bytes.", K(read_size), K(read_count), K(ret));
      }
     }
   }
   return ret;
 }

bool ObLoadDataDirectImpl::DataReader::is_end_file() const
{
  bool ret = false;
  if (file_reader_->eof()) {
    ret = true;
  } else if (end_offset_ >= 0) {
    ret = file_reader_->get_offset() >= end_offset_;
  }
  return ret;
}

/**
 * DataParser
 */

ObLoadDataDirectImpl::DataParser::DataParser()
  : data_buffer_(nullptr),
    start_line_no_(0),
    pos_(0),
    logger_(nullptr),
    is_inited_(false)
{
}

ObLoadDataDirectImpl::DataParser::~DataParser()
{
}

int ObLoadDataDirectImpl::DataParser::init(const DataAccessParam &data_access_param, Logger &logger)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::DataParser init twice", KR(ret), KP(this));
  } else {
    if (OB_FAIL(csv_parser_.init(data_access_param.file_format_, data_access_param.file_column_num_,
                                 data_access_param.file_cs_type_))) {
      LOG_WARN("fail to init csv parser", KR(ret));
    } else if (OB_FAIL(escape_buffer_.init())) {
      LOG_WARN("fail to init data buffer", KR(ret));
    } else {
      logger_ = &logger;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::DataParser::parse(const ObString &file_name, int64_t start_line_no,
                                            DataBuffer &data_buffer)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::DataParser not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!data_buffer.is_valid() || data_buffer.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(data_buffer));
  } else {
    file_name_ = file_name;
    start_line_no_ = start_line_no;
    pos_ = 0;
    data_buffer_ = &data_buffer;
  }
  return ret;
}

int ObLoadDataDirectImpl::DataParser::get_next_row(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::DataParser not init", KR(ret), KP(this));
  } else if (OB_ISNULL(data_buffer_) || OB_ISNULL(row.cells_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(data_buffer_), K(row));
  } else if (data_buffer_->empty()) {
    ret = OB_ITER_END;
  } else {
    auto handle_one_line = [](ObIArray<ObCSVGeneralParser::FieldValue> &fields_per_line) -> int {
      UNUSED(fields_per_line);
      return OB_SUCCESS;
    };
    while (OB_SUCC(ret)) {
      const char *str = data_buffer_->data();
      const char *end = str + data_buffer_->get_data_length();
      ObSEArray<ObCSVGeneralParser::LineErrRec, 1> err_records;
      int64_t nrows = 1;
      ret = csv_parser_.scan<decltype(handle_one_line), true>(
        str, end, nrows, escape_buffer_.file_buffer_->begin_ptr(),
        escape_buffer_.file_buffer_->begin_ptr() + escape_buffer_.file_buffer_->get_buffer_size(),
        handle_one_line, err_records, data_buffer_->is_end_file_);
      if (OB_FAIL(ret)) {
        LOG_WARN("fail to scan", KR(ret));
      } else if (0 == nrows) {
        ret = OB_ITER_END;
      } else {
        ++pos_;
        data_buffer_->advance(str - data_buffer_->data());
        if (OB_UNLIKELY(!err_records.empty())) {
          if (OB_FAIL(log_error_line(err_records.at(0).err_code, start_line_no_ + pos_))) {
            LOG_WARN("fail to log error line", KR(ret));
          }
        } else {
          const ObIArray<ObCSVGeneralParser::FieldValue> &field_values_in_file =
            csv_parser_.get_fields_per_line();
          for (int64_t i = 0; OB_SUCC(ret) && i < row.count_; ++i) {
            const ObCSVGeneralParser::FieldValue &str_v = field_values_in_file.at(i);
            ObObj &obj = row.cells_[i];
            if (str_v.is_null_) {
              obj.set_null();
            } else {
              obj.set_string(ObVarcharType, ObString(str_v.len_, str_v.ptr_));
              obj.set_collation_type(
                ObCharset::get_default_collation(csv_parser_.get_format().cs_type_));
            }
          }
          break;
        }
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::DataParser::log_error_line(int err_ret, int64_t err_line_no)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(logger_->log_error_line(file_name_, err_line_no, err_ret))) {
    LOG_WARN("fail to log error line", KR(ret), K(err_ret), K(err_line_no));
  }
  return ret;
}

/**
 * SimpleDataSplitUtils
 */

bool ObLoadDataDirectImpl::SimpleDataSplitUtils::is_simple_format(
  const ObDataInFileStruct &file_format, ObCollationType file_cs_type)
{
  bool bret = false;
  ObCharsetType char_set = ObCharset::charset_type_by_coll(file_cs_type);
  if (char_set == CHARSET_UTF8MB4 && file_format.line_term_str_.length() == 1 &&
      file_format.line_start_str_.empty() && file_format.field_term_str_.length() == 1 &&
      file_format.field_enclosed_char_ == INT64_MAX &&
      file_format.field_escaped_char_ == INT64_MAX &&
      file_format.line_term_str_.ptr()[0] != file_format.field_term_str_.ptr()[0]) {
    bret = true;
  }
  return bret;
}

int ObLoadDataDirectImpl::SimpleDataSplitUtils::split(const DataAccessParam &data_access_param,
                                                      const DataDesc &data_desc, int64_t count,
                                                      DataDescIterator &data_desc_iter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!data_access_param.is_valid() || count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(data_access_param), K(count));
  } else if (OB_UNLIKELY(!is_simple_format(data_access_param.file_format_,
                                           data_access_param.file_cs_type_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected data format", KR(ret), K(data_access_param));
  } else if (1 == count || (ObLoadFileLocation::CLIENT_DISK == data_access_param.file_location_)) {
    if (OB_FAIL(data_desc_iter.add_data_desc(data_desc))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  } else {
    ObArenaAllocator allocator;
    allocator.set_tenant_id(MTL_ID());

    int64_t end_offset = data_desc.end_;

    ObFileReadParam file_read_param;
    file_read_param.file_location_ = data_access_param.file_location_;
    file_read_param.filename_      = data_desc.filename_;
    file_read_param.access_info_   = data_access_param.access_info_;
    file_read_param.packet_handle_ = NULL;
    file_read_param.session_       = NULL;
    file_read_param.timeout_ts_    = THIS_WORKER.get_timeout_ts();

    ObFileReader *file_reader = NULL;
    if (OB_FAIL(ObFileReader::open(file_read_param, allocator, file_reader))) {
      LOG_WARN("failed to open file.", KR(ret), K(data_desc));
    } else if (!file_reader->seekable()) {
      if (OB_FAIL(data_desc_iter.add_data_desc(data_desc))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    } else if (-1 == end_offset && OB_FAIL(file_reader->get_file_size(end_offset))) {
      LOG_WARN("fail to get io device file size", KR(ret), K(end_offset));
    } else {
      const int64_t file_size = end_offset - data_desc.start_;
      if (file_size < count * ObLoadFileBuffer::MAX_BUFFER_SIZE * 2) {
        // file is too small
        if (OB_FAIL(data_desc_iter.add_data_desc(data_desc))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      } else {
        const char line_term_char = data_access_param.file_format_.line_term_str_.ptr()[0];
        const int64_t buf_size = (128LL << 10) + 1;
        const int64_t split_size = file_size / count;
        char *buf = nullptr;
        int64_t read_size = 0;
        DataDesc data_desc_ret;
        data_desc_ret.file_idx_ = data_desc.file_idx_;
        data_desc_ret.filename_ = data_desc.filename_;
        data_desc_ret.start_ = data_desc.start_;
        if (OB_ISNULL(buf = static_cast<char *>(allocator.alloc(buf_size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory", KR(ret));
        }
        for (int64_t i = 0; OB_SUCC(ret) && i < count - 1; ++i) {
          int64_t read_offset = data_desc.start_ + split_size * (i + 1);
				  file_reader->seek(read_offset);
          char *found = nullptr;
          while (OB_SUCC(ret) && end_offset > file_reader->get_offset() && nullptr == found) {
            read_offset = file_reader->get_offset();
            const int64_t read_count = MIN(end_offset - read_offset, buf_size - 1);
            if (OB_FAIL(file_reader->readn(buf, read_count, read_size))) {
              LOG_WARN("fail to do read", KR(ret), K(read_offset), K(read_count));
            } else if (OB_UNLIKELY(read_count != read_size)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected read size", KR(ret), K(read_count), K(read_size));
            } else {
              buf[read_size] = '\0';
              found = STRCHR(buf, line_term_char);
            }
          }
          if (OB_SUCC(ret)) {
            if (nullptr == found) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected large row", KR(ret));
            } else {
              data_desc_ret.end_ = read_offset + (found - buf + 1);
              if (OB_FAIL(data_desc_iter.add_data_desc(data_desc_ret))) {
                LOG_WARN("fail to push back", KR(ret));
              } else {
                data_desc_ret.start_ = data_desc_ret.end_;
              }
            }
          }
        }
        if (OB_SUCC(ret)) {
          data_desc_ret.end_ = data_desc.end_;
          if (OB_FAIL(data_desc_iter.add_data_desc(data_desc_ret))) {
            LOG_WARN("fail to push back", KR(ret));
          }
        }
      }
    }

    if (OB_NOT_NULL(file_reader)) {
      file_reader->~ObFileReader();
      allocator.free(file_reader);
    }
  }
  return ret;
}

/**
 * FileLoadExecutor
 */

ObLoadDataDirectImpl::FileLoadExecutor::FileLoadExecutor()
  : allocator_("TLD_FileLoad"),
    execute_param_(nullptr),
    execute_ctx_(nullptr),
    task_scheduler_(nullptr),
    worker_count_(0),
    worker_ctx_array_(nullptr),
    total_line_count_(0),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
  handle_resource_.set_tenant_id(MTL_ID());
}

ObLoadDataDirectImpl::FileLoadExecutor::~FileLoadExecutor()
{
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    execute_ctx_->allocator_->free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  if (nullptr != worker_ctx_array_) {
    for (int64_t i = 0; i < worker_count_; ++i) {
      WorkerContext *worker_ctx = worker_ctx_array_ + i;
      worker_ctx->~WorkerContext();
    }
    execute_ctx_->allocator_->free(worker_ctx_array_);
    worker_ctx_array_ = nullptr;
  }
  for (int64_t i = 0; i < handle_resource_.count(); ++i) {
    TaskHandle *handle = handle_resource_.at(i);
    handle->~TaskHandle();
    execute_ctx_->allocator_->free(handle);
  }
  handle_resource_.reset();
  trans_ctx_.reset();
}

int ObLoadDataDirectImpl::FileLoadExecutor::inner_init(const LoadExecuteParam &execute_param,
                                                       LoadExecuteContext &execute_ctx,
                                                       int64_t worker_count, int64_t handle_count)
{
  int ret = OB_SUCCESS;
  execute_param_ = &execute_param;
  execute_ctx_ = &execute_ctx;
  worker_count_ = worker_count;
  // init task_allocator_
  if (OB_FAIL(task_allocator_.init("TLD_TaskPool", execute_param_->tenant_id_))) {
    LOG_WARN("fail to init allocator", KR(ret));
  }
  // init task_scheduler_
  else if (OB_ISNULL(task_scheduler_ =
                         OB_NEWx(ObTableLoadTaskThreadPoolScheduler, (execute_ctx_->allocator_),
                                 worker_count_, execute_param_->table_id_, "Parse"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->init())) {
    LOG_WARN("fail to init task scheduler", KR(ret));
  } else if (OB_FAIL(task_scheduler_->start())) {
    LOG_WARN("fail to start task scheduler", KR(ret));
  }
  // init worker_ctx_array_
  else if (OB_FAIL(init_worker_ctx_array())) {
    LOG_WARN("fail to init worker ctx array", KR(ret));
  }
  // task ctrl
  else if (OB_FAIL(task_controller_.init(handle_count))) {
    LOG_WARN("fail to init task controller", KR(ret), K(handle_count));
  } else if (OB_FAIL(handle_reserve_queue_.init(handle_count))) {
    LOG_WARN("fail to init handle reserve queue", KR(ret), K(handle_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < handle_count; ++i) {
    TaskHandle *handle = nullptr;
    if (OB_ISNULL(handle = OB_NEWx(TaskHandle, execute_ctx_->allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate handler", KR(ret));
    } else if (OB_FAIL(handle->data_buffer_.init())) {
      LOG_WARN("fail to init data buffer", KR(ret));
    } else if (OB_FAIL(handle_reserve_queue_.push_back(handle))) {
      LOG_WARN("fail to push back handle to queue", KR(ret));
    } else if (OB_FAIL(handle_resource_.push_back(handle))) {
      LOG_WARN("fail to push back handle to array", KR(ret));
    }
    if (OB_FAIL(ret)) {
      if (nullptr != handle) {
        handle->~TaskHandle();
        execute_ctx_->allocator_->free(handle);
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::FileLoadExecutor::init_worker_ctx_array()
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_ISNULL(buf = execute_ctx_->allocator_->alloc(sizeof(WorkerContext) * worker_count_))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret));
  } else {
    worker_ctx_array_ = new (buf) WorkerContext[worker_count_];
    for (int64_t i = 0; OB_SUCC(ret) && i < worker_count_; ++i) {
      WorkerContext *worker_ctx = worker_ctx_array_ + i;
      if (OB_FAIL(worker_ctx->data_parser_.init(execute_param_->data_access_param_,
                                                *execute_ctx_->logger_))) {
        LOG_WARN("fail to init data parser", KR(ret), K(execute_param_->data_access_param_));
      } else if (OB_FAIL(
                   worker_ctx->objs_.create(execute_param_->data_access_param_.file_column_num_ *
                                              execute_param_->batch_row_count_,
                                            *execute_ctx_->allocator_))) {
        LOG_WARN("fail to create obj array", KR(ret));
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::FileLoadExecutor::execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::FileLoadExecutor not init", KR(ret), KP(this));
  } else {
    if (OB_FAIL(prepare_execute())) {
      LOG_WARN("fail to prepare execute", KR(ret));
    } else if (OB_FAIL(execute_ctx_->direct_loader_->start_trans(trans_ctx_,
                                                                 ObTableLoadInstance::DEFAULT_SEGMENT_ID,
                                                                 allocator_))) {
      LOG_WARN("fail to start trans", KR(ret));
    }

    LOG_TRACE("file load executor prepare execute done", K(ret));
    while (OB_SUCC(ret) && OB_SUCC(execute_ctx_->exec_ctx_.check_status())) {
      TaskHandle *handle = nullptr;
      if (OB_FAIL(get_next_task_handle(handle))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next task handle", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ObTableLoadTask *task = nullptr;
        if (OB_FAIL(alloc_task(task))) {
          LOG_WARN("fail to alloc task", KR(ret));
        } else if (OB_FAIL(fill_task(handle, task))) {
          LOG_WARN("fail to fill task", KR(ret));
        } else if (OB_FAIL(task_scheduler_->add_task(handle->worker_idx_, task))) {
          LOG_WARN("fail to add task", KR(ret), K(handle->worker_idx_), KPC(task));
        }
        if (OB_FAIL(ret)) {
          if (nullptr != task) {
            free_task(task);
          }
        }
      }
      if (OB_FAIL(ret)) {
        if (nullptr != handle) {
          task_finished(handle);
        }
      }
    }

    wait_all_task_finished();

    if (OB_SUCC(ret)) {
      if (OB_FAIL(handle_all_task_result())) {
        LOG_WARN("fail to handle all task result", KR(ret));
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(execute_ctx_->direct_loader_->commit_trans(trans_ctx_))) {
        LOG_WARN("fail to commit trans", KR(ret));
      }
    }
  }

  LOG_TRACE("large file load executor init done", K(ret));
  return ret;
}

int ObLoadDataDirectImpl::FileLoadExecutor::alloc_task(ObTableLoadTask *&task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::FileLoadExecutor not init", KR(ret), KP(this));
  } else {
    if (OB_ISNULL(task = task_allocator_.alloc(execute_param_->tenant_id_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc task", KR(ret));
    }
  }
  return ret;
}

void ObLoadDataDirectImpl::FileLoadExecutor::free_task(ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::FileLoadExecutor not init", KR(ret), KP(this));
  } else if (OB_ISNULL(task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(task));
  } else {
    task_allocator_.free(task);
  }
}

int ObLoadDataDirectImpl::FileLoadExecutor::fetch_task_handle(TaskHandle *&handle)
{
  int ret = OB_SUCCESS;
  handle = nullptr;
  if (OB_FAIL(task_controller_.on_next_task())) {
    LOG_WARN("fail to on next task", KR(ret));
  } else {
    if (OB_FAIL(handle_reserve_queue_.pop(handle))) {
      LOG_WARN("fail to pop handle", KR(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null handle", KR(ret));
    } else if (OB_FAIL(handle_task_result(handle->task_id_, handle->result_))) {
      LOG_WARN("fail to handle task result", KR(ret), KPC(handle));
    }
    if (OB_FAIL(ret)) {
      // 主动调用on_task_finished, 防止wait_all_task_finished卡住
      task_controller_.on_task_finished();
    }
  }
  return ret;
}

void ObLoadDataDirectImpl::FileLoadExecutor::task_finished(TaskHandle *handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(handle)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null handle", KR(ret));
  } else {
    handle->result_.finished_ts_ = ObTimeUtil::current_time();
    int ret1 = handle_reserve_queue_.push_back(handle);
    MEM_BARRIER();
    int ret2 = task_controller_.on_task_finished();
    if (OB_UNLIKELY(OB_FAIL(ret1) || OB_FAIL(ret2))) {
      LOG_ERROR("fail to finished task", KR(ret1), KR(ret2));
    }
  }
}

int ObLoadDataDirectImpl::FileLoadExecutor::handle_task_result(int64_t task_id, TaskResult &result)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(result.ret_)) {
    LOG_WARN("task result is failed", KR(ret), K(task_id));
  } else {
    total_line_count_ += result.parsed_row_count_;
  }
  /*
  if (0 != result.created_ts_) {
    int64_t wait_us = 0, proccess_us = 0;
    if (0 != result.start_process_ts_) {
      wait_us = result.start_process_ts_ - result.created_ts_;
      proccess_us = result.finished_ts_ - result.start_process_ts_;
    } else {
      wait_us = result.finished_ts_ - result.created_ts_;
    }
  }
  */
  result.reset();
  return ret;
}

int ObLoadDataDirectImpl::FileLoadExecutor::handle_all_task_result()
{
  int ret = OB_SUCCESS;
  TaskHandle *handle = nullptr;
  while (OB_SUCC(ret) && handle_reserve_queue_.count() > 0) {
    if (OB_FAIL(handle_reserve_queue_.pop(handle))) {
      LOG_WARN("fail to pop handle", KR(ret));
    } else if (OB_ISNULL(handle)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null handle", KR(ret));
    } else if (OB_FAIL(handle_task_result(handle->task_id_, handle->result_))) {
      LOG_WARN("fail to handle task result", KR(ret), KPC(handle));
    }
  }
  return ret;
}

void ObLoadDataDirectImpl::FileLoadExecutor::wait_all_task_finished()
{
  const int64_t processing_task_cnt = task_controller_.get_processing_task_cnt();
  const int64_t total_task_cnt = task_controller_.get_total_task_cnt();
  LOG_INFO("LOAD DATA wait all task finish", K(processing_task_cnt), K(total_task_cnt));
  task_controller_.wait_all_task_finish(execute_param_->combined_name_.ptr(), THIS_WORKER.get_timeout_ts());
}

int ObLoadDataDirectImpl::FileLoadExecutor::process_task_handle(TaskHandle *handle,
                                                                int64_t &parsed_line_count)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::FileLoadExecutor not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == handle || handle->worker_idx_ < 0 ||
                         handle->worker_idx_ >= worker_count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KPC(execute_param_), KP(handle));
  } else {
    WorkerContext &worker_ctx = worker_ctx_array_[handle->worker_idx_];
    const int64_t column_count = execute_param_->data_access_param_.file_column_num_;
    const int64_t data_buffer_length = handle->data_buffer_.get_data_length();
    int64_t parsed_bytes = 0;
    int64_t processed_line_count = 0;
    int64_t total_processed_line_count = 0;
    parsed_line_count = 0;
    ObNewRow row;
    bool is_iter_end = false;
    if (OB_FAIL(worker_ctx.data_parser_.parse(handle->data_desc_.filename_, handle->start_line_no_,
                                              handle->data_buffer_))) {
      LOG_WARN("fail to parse data", KR(ret), KPC(handle));
    } else {
      row.cells_ = worker_ctx.objs_.ptr();
      row.count_ = column_count;
    }
    while (OB_SUCC(ret) && !is_iter_end) {
      // 每个新的batch需要分配一个新的shared_allocator
      ObTableLoadSharedAllocatorHandle allocator_handle =
        ObTableLoadSharedAllocatorHandle::make_handle("TLD_share_alloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      if (!allocator_handle) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to make allocator handle", KR(ret));
      }
      ObTableLoadObjRowArray obj_rows;
      obj_rows.set_allocator(allocator_handle);

      while (OB_SUCC(ret) && (processed_line_count < execute_param_->batch_row_count_)) {
        if (OB_FAIL(worker_ctx.data_parser_.get_next_row(row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            ret = OB_SUCCESS;
            is_iter_end = true;
            break;
          }
        } else {
          //此时row中的每个obj的内容指向的是data parser中的内存
          //因此得把它们深拷贝一遍
          ObTableLoadObjRow tmp_obj_row;
          tmp_obj_row.seq_no_= handle->get_next_seq_no();
          tmp_obj_row.cells_ = row.cells_;
          tmp_obj_row.count_ = row.count_;
          ObTableLoadObjRow row;
          if (OB_FAIL(row.deep_copy(tmp_obj_row, allocator_handle))) {
            LOG_WARN("failed to deep copy add assign to tmp_obj_row", KR(ret));
          } else if (OB_FAIL(obj_rows.push_back(row))) {
            LOG_WARN("failed to add tmp_obj_row to obj_rows", KR(ret));
          } else {
            ++processed_line_count;
            row.cells_ += column_count;
          }
        }
      } // end while()

      if (OB_SUCC(ret) && (processed_line_count > 0)) {
        if (OB_FAIL(execute_ctx_->direct_loader_->write_trans(trans_ctx_, handle->session_id_,
                                                              obj_rows))) {
          LOG_WARN("fail to write objs", KR(ret));
        } else {
          total_processed_line_count += processed_line_count;
          processed_line_count = 0;
          row.cells_ = worker_ctx.objs_.ptr();
        }
      }
    } // end while()
    parsed_line_count = worker_ctx.data_parser_.get_parsed_row_count();
    parsed_bytes = data_buffer_length - handle->data_buffer_.get_data_length();
    handle->result_.proccessed_row_count_ += total_processed_line_count;
    handle->result_.parsed_row_count_ += parsed_line_count;
    handle->result_.parsed_bytes_ += parsed_bytes;
    ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_rows_, parsed_line_count);
    ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_bytes_, parsed_bytes);
  }
  return ret;
}

/**
 * FileLoadTaskCallback
 */

class ObLoadDataDirectImpl::FileLoadTaskCallback : public ObITableLoadTaskCallback
{
public:
  FileLoadTaskCallback(FileLoadExecutor *load_executor, TaskHandle *handle)
    : load_executor_(load_executor), handle_(handle)
  {
  }
  virtual ~FileLoadTaskCallback() = default;
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    handle_->result_.ret_ = ret_code;
    load_executor_->task_finished(handle_);
    load_executor_->free_task(task);
  }
private:
  FileLoadExecutor *load_executor_;
  TaskHandle *handle_;
};

/**
 * LargeFileLoadTaskProcessor
 */

class ObLoadDataDirectImpl::LargeFileLoadTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  LargeFileLoadTaskProcessor(ObTableLoadTask &task, FileLoadExecutor *file_load_executor,
                             TaskHandle *handle)
    : ObITableLoadTaskProcessor(task), file_load_executor_(file_load_executor), handle_(handle)
  {
  }
  virtual ~LargeFileLoadTaskProcessor() = default;
  int process() override;
  INHERIT_TO_STRING_KV("task_processor", ObITableLoadTaskProcessor, KPC_(handle));
private:
  FileLoadExecutor *file_load_executor_;
  TaskHandle *handle_;
};

int ObLoadDataDirectImpl::LargeFileLoadTaskProcessor::process()
{
  int ret = OB_SUCCESS;
  handle_->result_.start_process_ts_ = ObTimeUtil::current_time();
  int64_t line_count = 0;
  if (OB_FAIL(file_load_executor_->process_task_handle(handle_, line_count))) {
    LOG_WARN("fail to process task handle", KR(ret));
  } else if (OB_UNLIKELY(line_count > ObTableLoadSequenceNo::MAX_CHUNK_SEQ_NO)){
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", KR(ret), K(line_count));
  }
  return ret;
}

/**
 * LargeFileLoadExecutor
 */

ObLoadDataDirectImpl::LargeFileLoadExecutor::LargeFileLoadExecutor()
  : next_worker_idx_(0),
    next_chunk_id_(0),
    total_line_no_(0)
{
}

ObLoadDataDirectImpl::LargeFileLoadExecutor::~LargeFileLoadExecutor()
{
}

int ObLoadDataDirectImpl::LargeFileLoadExecutor::init(const LoadExecuteParam &execute_param,
                                                      LoadExecuteContext &execute_ctx,
                                                      const DataDescIterator &data_desc_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::LargeFileLoadExecutor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!execute_param.is_valid() || !execute_ctx.is_valid() ||
                         1 != data_desc_iter.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(execute_param), K(execute_ctx), K(data_desc_iter));
  } else {
    int64_t data_id = 0;
    DataDescIterator copy_data_desc_iter;
    DataDesc data_desc;
    if (OB_FAIL(inner_init(execute_param, execute_ctx, execute_param.thread_count_,
                           execute_param.data_mem_usage_limit_))) {
      LOG_WARN("fail to init inner", KR(ret));
    }
    // data_desc_
    else if (OB_FAIL(copy_data_desc_iter.copy(data_desc_iter))) {
      LOG_WARN("fail to copy data desc iter", KR(ret));
    } else if (OB_FAIL(copy_data_desc_iter.get_next_data_desc(data_desc, data_id))) {
      LOG_WARN("fail to get next data desc", KR(ret));
    }
    // expr_buffer_
    else if (OB_FAIL(expr_buffer_.init())) {
      LOG_WARN("fail to init data buffer", KR(ret));
    }
    // data_reader_
    else if (OB_FAIL(
               data_reader_.init(execute_param_->data_access_param_, *execute_ctx_, data_desc))) {
      LOG_WARN("fail to init data reader", KR(ret));
    } else {
      data_desc_ = data_desc;
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::LargeFileLoadExecutor::prepare_execute()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(skip_ignore_rows())) {
    LOG_WARN("fail to skip ignore rows", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectImpl::LargeFileLoadExecutor::get_next_task_handle(TaskHandle *&handle)
{
  int ret = OB_SUCCESS;
  int64_t current_line_count = 0;
  const int64_t chunk_id = next_chunk_id_ ++;
  expr_buffer_.reuse();
  if (OB_UNLIKELY(chunk_id > ObTableLoadSequenceNo::MAX_CHUNK_ID)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", KR(ret), K(chunk_id));
  } else if (OB_FAIL(data_reader_.get_next_buffer(*expr_buffer_.file_buffer_, current_line_count))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next buffer", KR(ret));
    }
  } else if (OB_FAIL(fetch_task_handle(handle))) {
    LOG_WARN("fail to fetch task handle", KR(ret));
  } else {
    handle->task_id_ = task_controller_.get_next_task_id();
    handle->worker_idx_ = get_worker_idx();
    handle->session_id_ = handle->worker_idx_ + 1;
    handle->data_desc_ = data_desc_;
    handle->start_line_no_ = total_line_no_ ;
    handle->result_.created_ts_ = ObTimeUtil::current_time();
    handle->sequence_no_.sequence_no_ = chunk_id;
    handle->sequence_no_.sequence_no_ <<= ObTableLoadSequenceNo::CHUNK_ID_SHIFT;
    handle->data_buffer_.swap(expr_buffer_);
    handle->data_buffer_.is_end_file_ = data_reader_.is_end_file();
    total_line_no_ += current_line_count;
  }
  return ret;
}

int ObLoadDataDirectImpl::LargeFileLoadExecutor::fill_task(TaskHandle *handle,
                                                           ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task->set_processor<LargeFileLoadTaskProcessor>(this, handle))) {
    LOG_WARN("fail to set large file load task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<FileLoadTaskCallback>(this, handle))) {
    LOG_WARN("fail to set file load task callback", KR(ret));
  }
  return ret;
}

int64_t ObLoadDataDirectImpl::LargeFileLoadExecutor::get_worker_idx()
{
  if (next_worker_idx_ >= worker_count_) {
    next_worker_idx_ = 0;
  }
  return next_worker_idx_++;
}

int ObLoadDataDirectImpl::LargeFileLoadExecutor::skip_ignore_rows()
{
  int ret = OB_SUCCESS;
  const int64_t ignore_row_num = execute_param_->ignore_row_num_;
  if (ignore_row_num > 0) {
    int64_t skip_line_count = 0;
    int64_t line_count = 0;
    int64_t skip_bytes = 0;
    while (OB_SUCC(ret) && skip_line_count < ignore_row_num) {
      if (OB_FAIL(data_reader_.get_next_buffer(*expr_buffer_.file_buffer_, line_count,
                                               ignore_row_num - skip_line_count))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next buffer", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        skip_line_count += line_count;
        skip_bytes += expr_buffer_.file_buffer_->get_data_len();
      }
    }
    if (OB_SUCC(ret)) {
      total_line_count_ += skip_line_count;
      ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_rows_, skip_line_count);
      ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_bytes_, skip_bytes);
    }
    LOG_INFO("LOAD DATA skip ignore rows", KR(ret), K(ignore_row_num),
             K(skip_line_count), K(skip_bytes));
  }
  return ret;
}

/**
 * MultiFilesLoadTaskProcessor
 */

class ObLoadDataDirectImpl::MultiFilesLoadTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  MultiFilesLoadTaskProcessor(ObTableLoadTask &task, const LoadExecuteParam *execute_param,
                              LoadExecuteContext *execute_ctx, FileLoadExecutor *file_load_executor,
                              TaskHandle *handle)
    : ObITableLoadTaskProcessor(task),
      execute_param_(execute_param),
      execute_ctx_(execute_ctx),
      file_load_executor_(file_load_executor),
      handle_(handle)
  {
  }
  virtual ~MultiFilesLoadTaskProcessor() = default;
  int process() override;
  INHERIT_TO_STRING_KV("task_processor", ObITableLoadTaskProcessor, KPC_(handle));
private:
  int skip_ignore_rows(int64_t &skip_line_count);
private:
  const LoadExecuteParam *execute_param_;
  LoadExecuteContext *execute_ctx_;
  FileLoadExecutor *file_load_executor_;
  TaskHandle *handle_;
  DataReader data_reader_;
};

int ObLoadDataDirectImpl::MultiFilesLoadTaskProcessor::process()
{
  int ret = OB_SUCCESS;
  handle_->result_.start_process_ts_ = ObTimeUtil::current_time();
  int64_t current_line_count = 0;
  int64_t total_line_count = 0;
  if (OB_FAIL(data_reader_.init(execute_param_->data_access_param_, *execute_ctx_,
                                handle_->data_desc_, true))) {
    LOG_WARN("fail to init data reader", KR(ret));
  } else if (0 == handle_->data_desc_.file_idx_ && 0 == handle_->data_desc_.start_) {
    if (OB_FAIL(skip_ignore_rows(current_line_count))) {
      LOG_WARN("fail to skip ignore rows", KR(ret));
    } else if (OB_UNLIKELY(current_line_count < execute_param_->ignore_row_num_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("direct-load does not support ignore rows exceed the first file", KR(ret),
               K(current_line_count), K(execute_param_->ignore_row_num_));
      FORWARD_USER_ERROR_MSG(ret, "direct-load does not support ignore rows exceed the first file");
    } else if (!handle_->data_buffer_.empty()) {
      handle_->data_buffer_.is_end_file_ = data_reader_.is_end_file();
      handle_->start_line_no_ = handle_->result_.parsed_row_count_ + 1;
      current_line_count = 0;
      if (OB_FAIL(file_load_executor_->process_task_handle(handle_, current_line_count))) {
        LOG_WARN("fail to process task handle", KR(ret));
      } else {
        total_line_count += current_line_count;
        if (OB_UNLIKELY(total_line_count > ObTableLoadSequenceNo::MAX_DATA_SEQ_NO)){
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("size is overflow", KR(ret), K(total_line_count));
        }
      }
    }
  }
  while (OB_SUCC(ret) && OB_SUCC(execute_ctx_->exec_ctx_.check_status())) {
    if (OB_FAIL(handle_->data_buffer_.squash())) {
      LOG_WARN("fail to squash data buffer", KR(ret));
    } else if (OB_FAIL(data_reader_.get_next_raw_buffer(handle_->data_buffer_))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next buffer", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else {
      handle_->data_buffer_.is_end_file_ = data_reader_.is_end_file();
      handle_->start_line_no_ = handle_->result_.parsed_row_count_ + 1;
      current_line_count = 0;
      if (OB_FAIL(file_load_executor_->process_task_handle(handle_, current_line_count))) {
        LOG_WARN("fail to process task handle", KR(ret));
      } else if (OB_UNLIKELY(0 == current_line_count)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("direct-load does not support big row", KR(ret), "size",
                 handle_->data_buffer_.get_data_length());
        FORWARD_USER_ERROR_MSG(ret, "direct-load does not support big row");
      } else {
        total_line_count += current_line_count;
        if (OB_UNLIKELY(total_line_count > ObTableLoadSequenceNo::MAX_DATA_SEQ_NO)){
          ret = OB_SIZE_OVERFLOW;
          LOG_WARN("size is overflow", KR(ret), K(total_line_count));
        }
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::MultiFilesLoadTaskProcessor::skip_ignore_rows(int64_t &skip_line_count)
{
  int ret = OB_SUCCESS;
  const int64_t ignore_row_num = execute_param_->ignore_row_num_;
  skip_line_count = 0;
  int64_t skip_bytes = 0;
  if (ignore_row_num > 0) {
    DataBuffer &data_buffer = handle_->data_buffer_;
    data_buffer.reuse();
    while (OB_SUCC(ret) && skip_line_count < ignore_row_num) {
      if (OB_FAIL(data_buffer.squash())) {
        LOG_WARN("fail to squash data buffer", KR(ret));
      } else if (OB_FAIL(data_reader_.get_next_raw_buffer(data_buffer))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("fail to get next buffer", KR(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_UNLIKELY(data_buffer.empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected empty data buffer", KR(ret));
      } else {
        int64_t complete_cnt = ignore_row_num - skip_line_count;
        int64_t complete_len = 0;
        if (OB_FAIL(ObLoadDataBase::pre_parse_lines(
              *data_buffer.file_buffer_, data_reader_.get_csv_parser(), data_reader_.is_end_file(),
              complete_len, complete_cnt))) {
          LOG_WARN("fail to fast_lines_parse", KR(ret));
        } else if (OB_UNLIKELY(0 == complete_len)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("direct-load does not support big row", KR(ret), "size",
                   data_buffer.get_data_length());
          FORWARD_USER_ERROR_MSG(ret, "direct-load does not support big row");
        } else {
          data_buffer.advance(complete_len);
          skip_line_count += complete_cnt;
          skip_bytes += complete_len;
        }
      }
    }
    if (OB_SUCC(ret)) {
      handle_->result_.parsed_row_count_ += skip_line_count;
      handle_->result_.parsed_bytes_ += skip_bytes;
      ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_rows_, skip_line_count);
      ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_bytes_, skip_bytes);
    }
    LOG_INFO("LOAD DATA skip ignore rows", KR(ret), K(ignore_row_num), K(skip_line_count), K(skip_bytes));
  }
  return ret;
}

/**
 * MultiFilesLoadExecutor
 */

int ObLoadDataDirectImpl::MultiFilesLoadExecutor::init(const LoadExecuteParam &execute_param,
                                                       LoadExecuteContext &execute_ctx,
                                                       const DataDescIterator &data_desc_iter)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::MultiFilesLoadExecutor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!execute_param.is_valid() || !execute_ctx.is_valid() ||
                         data_desc_iter.count() <= 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(execute_param), K(execute_ctx), K(data_desc_iter));
  } else {
    const int64_t parse_thread_count = MIN(data_desc_iter.count(), execute_param.thread_count_);
    if (OB_FAIL(inner_init(execute_param, execute_ctx, parse_thread_count, parse_thread_count))) {
      LOG_WARN("fail to init inner", KR(ret));
    } else if (OB_FAIL(data_desc_iter_.copy(data_desc_iter))) {
      LOG_WARN("fail to copy data desc iter", KR(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::MultiFilesLoadExecutor::prepare_execute()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < handle_resource_.count(); ++i) {
    TaskHandle *task_handle = handle_resource_.at(i);
    task_handle->worker_idx_ = i;
    task_handle->session_id_ = i + 1;
  }
  return ret;
}

int ObLoadDataDirectImpl::MultiFilesLoadExecutor::get_next_task_handle(TaskHandle *&handle)
{
  int ret = OB_SUCCESS;
  DataDesc data_desc;
  int64_t data_id = 0;
  if (OB_FAIL(data_desc_iter_.get_next_data_desc(data_desc, data_id))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      LOG_WARN("fail to get next data desc", KR(ret));
    }
  } else if (OB_UNLIKELY(data_id > ObTableLoadSequenceNo::MAX_DATA_ID)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("size is overflow", KR(ret), K(data_id));
  } else if (OB_FAIL(fetch_task_handle(handle))) {
    LOG_WARN("fail to fetch task handle", KR(ret));
  } else {
    handle->task_id_ = task_controller_.get_next_task_id();
    handle->data_desc_ = data_desc;
    handle->start_line_no_ = 0;
    handle->result_.created_ts_ = ObTimeUtil::current_time();
    handle->sequence_no_.sequence_no_ = data_id;
    handle->sequence_no_.sequence_no_ <<= ObTableLoadSequenceNo::DATA_ID_SHIFT;
  }
  return ret;
}

int ObLoadDataDirectImpl::MultiFilesLoadExecutor::fill_task(TaskHandle *handle,
                                                            ObTableLoadTask *task)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(task->set_processor<MultiFilesLoadTaskProcessor>(execute_param_, execute_ctx_, this,
                                                               handle))) {
    LOG_WARN("fail to set multi files load task processor", KR(ret));
  } else if (OB_FAIL(task->set_callback<FileLoadTaskCallback>(this, handle))) {
    LOG_WARN("fail to set file load task callback", KR(ret));
  }
  return ret;
}

/**
 * BackupLoadTaskProcessor
 */

class ObLoadDataDirectImpl::BackupLoadTaskProcessor : public ObITableLoadTaskProcessor
{
public:
  BackupLoadTaskProcessor(ObTableLoadTask &task, BackupLoadExecutor *load_executor,
                          int32_t session_id)
    : ObITableLoadTaskProcessor(task), load_executor_(load_executor), session_id_(session_id)
  {
  }
  virtual ~BackupLoadTaskProcessor() = default;
  int process() override;
  INHERIT_TO_STRING_KV("task_processor", ObITableLoadTaskProcessor, KP_(load_executor));
private:
  BackupLoadExecutor *load_executor_;
  int32_t session_id_;
};

int ObLoadDataDirectImpl::BackupLoadTaskProcessor::process()
{
  int ret = OB_SUCCESS;
  int64_t partition_idx = -1;
  int64_t subpart_count = 0;
  int64_t subpart_idx = -1;
  while (OB_SUCC(ret)) {
    if (OB_FAIL(load_executor_->check_status())) {
      LOG_WARN("fail to check status", KR(ret), K(session_id_));
    } else if (OB_FAIL(load_executor_->get_next_partition_task(partition_idx, subpart_count,
                                                               subpart_idx))) {
      if (OB_UNLIKELY(OB_ITER_END != ret)) {
        LOG_WARN("fail to get next partition idx", KR(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_FAIL(load_executor_->process_partition(session_id_, partition_idx, subpart_count,
                                                         subpart_idx))) {
      LOG_WARN("fail to process partition", KR(ret), K(session_id_), K(partition_idx),
               K(subpart_count), K(subpart_idx));
    }
  }
  return ret;
}

/**
 * BackupLoadTaskCallback
 */

class ObLoadDataDirectImpl::BackupLoadTaskCallback : public ObITableLoadTaskCallback
{
public:
  BackupLoadTaskCallback(BackupLoadExecutor *load_executor) : load_executor_(load_executor) {}
  virtual ~BackupLoadTaskCallback() = default;
  void callback(int ret_code, ObTableLoadTask *task) override
  {
    load_executor_->task_finished(task, ret_code);
  }
private:
  BackupLoadExecutor *load_executor_;
};

/**
 * BackupLoadExecutor
 */

ObLoadDataDirectImpl::BackupLoadExecutor::BackupLoadExecutor()
  : allocator_("TLD_BackupLoad"),
    execute_param_(nullptr),
    execute_ctx_(nullptr),
    backup_table_(nullptr),
    task_scheduler_(nullptr),
    worker_count_(0),
    partition_count_(0),
    subpart_count_(0),
    next_partition_idx_(0),
    next_subpart_idx_(0),
    total_line_count_(0),
    task_error_code_(OB_SUCCESS),
    is_inited_(false)
{
  allocator_.set_tenant_id(MTL_ID());
}

ObLoadDataDirectImpl::BackupLoadExecutor::~BackupLoadExecutor()
{
  if (nullptr != task_scheduler_) {
    task_scheduler_->stop();
    task_scheduler_->wait();
    task_scheduler_->~ObITableLoadTaskScheduler();
    allocator_.free(task_scheduler_);
    task_scheduler_ = nullptr;
  }
  if (nullptr != backup_table_) {
    backup_table_->~ObTableLoadBackupTable();
    allocator_.free(backup_table_);
    backup_table_ = nullptr;
  }
}

int ObLoadDataDirectImpl::BackupLoadExecutor::init(const LoadExecuteParam &execute_param,
                                                   LoadExecuteContext &execute_ctx,
                                                   const ObString &path)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObLoadDataDirectImpl::BackupLoadExecutor init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!execute_param.is_valid() || !execute_ctx.is_valid() || path.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(execute_param), K(execute_ctx), K(path));
  } else {
    execute_param_ = &execute_param;
    execute_ctx_ = &execute_ctx;
    const DataAccessParam &data_access_param = execute_param.data_access_param_;
    if (OB_FAIL(check_support_direct_load())) {
      LOG_WARN("fail to check support direct load", KR(ret));
    } else if (OB_FAIL(ObTableLoadBackupTable::get_table(ObTableLoadBackupVersion::V_1_4,
                                                         backup_table_, allocator_))) {
      LOG_WARN("fail to get backup table", KR(ret));
    } else if (OB_FAIL(backup_table_->init(&data_access_param.access_info_, path))) {
      LOG_WARN("fail to init backup table", KR(ret));
    } else {
      const int64_t max_partition_count = ObTableLoadSequenceNo::MAX_BACKUP_PARTITION_IDX + 1;
      const int64_t max_subpart_count = ObTableLoadSequenceNo::MAX_BACKUP_SUBPART_IDX + 1;
      worker_count_ = execute_param.thread_count_;
      partition_count_ = backup_table_->get_partition_count();
      if (OB_UNLIKELY(worker_count_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected worker count", KR(ret), K(worker_count_));
      } else if (OB_UNLIKELY(partition_count_ <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected partition count", KR(ret), K(partition_count_));
      } else if (OB_UNLIKELY(partition_count_ > max_partition_count)) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("table partition too much", KR(ret), K(partition_count_), K(max_partition_count));
      } else {
        // 计算subpart_count_
        if (partition_count_ >= worker_count_ * MIN_TASK_PER_WORKER) {
          // 分区数目足够, 不对分区进行拆分
          subpart_count_ = 1;
        } else {
          subpart_count_ =
            (worker_count_ * MIN_TASK_PER_WORKER + partition_count_ - 1) / partition_count_;
          subpart_count_ = MIN(subpart_count_, max_subpart_count);
        }
        if (OB_FAIL(task_controller_.init(worker_count_))) {
          LOG_WARN("fail to init task controller", KR(ret), K(worker_count_));
        } else if (OB_FAIL(task_allocator_.init("TLD_TaskPool", execute_param_->tenant_id_))) {
          LOG_WARN("fail to init allocator", KR(ret));
        } else if (OB_ISNULL(task_scheduler_ =
                               OB_NEWx(ObTableLoadTaskThreadPoolScheduler, &allocator_,
                                       worker_count_, execute_param_->table_id_, "Parse"))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new ObTableLoadTaskThreadPoolScheduler", KR(ret));
        } else if (OB_FAIL(task_scheduler_->init())) {
          LOG_WARN("fail to init task scheduler", KR(ret));
        } else if (OB_FAIL(task_scheduler_->start())) {
          LOG_WARN("fail to start task scheduler", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::BackupLoadExecutor::check_support_direct_load()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const uint64_t table_id = execute_param_->table_id_;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;
  bool has_lob_column = false;
  if (OB_FAIL(
        ObTableLoadSchema::get_table_schema(tenant_id, table_id, schema_guard, table_schema))) {
    LOG_WARN("fail to get table schema", KR(ret), K(tenant_id), K(table_id));
  }
  // check has lob column
  else if (OB_FAIL(ObTableLoadSchema::check_has_lob_column(table_schema, has_lob_column))) {
    LOG_WARN("fail to check has lob column", KR(ret));
  } else if (has_lob_column) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("direct-load backup does not support table has lob column", KR(ret));
  }
  return ret;
}

int ObLoadDataDirectImpl::BackupLoadExecutor::execute()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::BackupLoadExecutor not init", KR(ret), KP(this));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < worker_count_; ++i) {
      if (OB_FAIL(task_controller_.on_next_task())) {
        LOG_WARN("fail to on next task", KR(ret));
      } else {
        ObTableLoadTask *task = nullptr;
        // 1. 分配task
        if (OB_ISNULL(task = task_allocator_.alloc(execute_param_->tenant_id_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc task", KR(ret));
        }
        // 2. 设置processor
        else if (OB_FAIL(task->set_processor<BackupLoadTaskProcessor>(this, i + 1))) {
          LOG_WARN("fail to set backup load task processor", KR(ret));
        }
        // 3. 设置callback
        else if (OB_FAIL(task->set_callback<BackupLoadTaskCallback>(this))) {
          LOG_WARN("fail to set backup load task callback", KR(ret));
        }
        // 4. 把task放入调度器
        else if (OB_FAIL(task_scheduler_->add_task(i, task))) {
          LOG_WARN("fail to add task", KR(ret), K(i), KPC(task));
        }
        if (OB_FAIL(ret)) {
          task_controller_.on_task_finished();
          if (nullptr != task) {
            task_allocator_.free(task);
            task = nullptr;
          }
        }
      }
    }

    task_controller_.wait_all_task_finish(execute_param_->combined_name_.ptr(), THIS_WORKER.get_timeout_ts());

    if (OB_SUCC(ret)) {
      if (OB_FAIL(ATOMIC_LOAD(&task_error_code_))) {
        LOG_WARN("task error", KR(ret));
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::BackupLoadExecutor::get_next_partition_task(int64_t &partition_idx,
                                                                      int64_t &subpart_count,
                                                                      int64_t &subpart_idx)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (next_partition_idx_ >= partition_count_) {
    ret = OB_ITER_END;
  } else {
    partition_idx = next_partition_idx_;
    subpart_count = subpart_count_;
    subpart_idx = next_subpart_idx_++;
    if (next_subpart_idx_ >= subpart_count_) {
      // switch next partition
      next_partition_idx_++;
      next_subpart_idx_ = 0;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::BackupLoadExecutor::process_partition(int32_t session_id,
                                                                int64_t partition_idx,
                                                                int64_t subpart_count,
                                                                int64_t subpart_idx)
{
  LOG_INFO("start process partition", K(partition_idx), K(subpart_count), K(subpart_idx));
  int ret = OB_SUCCESS;
  int64_t total_processed_line_count = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::BackupLoadExecutor not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(partition_idx < 0 || partition_idx >= partition_count_ ||
                         subpart_count <= 0 || subpart_idx < 0 || subpart_idx >= subpart_count)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), K(partition_idx), K(partition_count_), K(subpart_idx),
             K(subpart_count));
  } else {
    ObArenaAllocator allocator("TLD_BackupLoad");
    allocator.set_tenant_id(MTL_ID());
    const int64_t column_count = execute_param_->data_access_param_.file_column_num_;
    ObTableLoadInstance *direct_loader = execute_ctx_->direct_loader_;
    ObTableLoadInstance::TransCtx trans_ctx;
    ObNewRowIterator *row_iter = nullptr;
    ObNewRow *new_row = nullptr;
    bool is_iter_end = false;
    int64_t processed_line_count = 0;
    const bool is_heap_table = direct_loader->get_table_ctx()->schema_.is_heap_table_;
    ObTableLoadSequenceNo sequence_no(
      (partition_idx << ObTableLoadSequenceNo::BACKUP_PARTITION_IDX_SHIFT) +
      (subpart_idx << ObTableLoadSequenceNo::BACKUP_SUBPART_IDX_SHIFT));
    const int64_t segment_id = partition_idx * subpart_count + subpart_idx + 1;
    if (OB_FAIL(direct_loader->start_trans(trans_ctx, segment_id, allocator))) {
      LOG_WARN("fail to start trans", KR(ret), K(segment_id), K(partition_idx), K(subpart_count),
               K(subpart_idx));
    } else if (OB_FAIL(backup_table_->scan(partition_idx, row_iter, allocator, subpart_count,
                                           subpart_idx))) {
      LOG_WARN("fail to scan backup table partition", KR(ret), KP_(backup_table), K(partition_idx),
               K(subpart_count), K(subpart_idx));
    }
    while (OB_SUCC(ret) && !is_iter_end) {
      // 每个新的batch需要分配一个新的shared_allocator
      ObTableLoadSharedAllocatorHandle allocator_handle;
      ObTableLoadObjRowArray obj_rows;
      processed_line_count = 0;
      if (OB_FAIL(check_status())) {
        LOG_WARN("fail to check status", KR(ret), K(partition_idx), K(session_id));
      } else {
        allocator_handle = ObTableLoadSharedAllocatorHandle::make_handle(
          "TLD_share_alloc", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        if (OB_UNLIKELY(!allocator_handle)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to make allocator handle", KR(ret));
        } else {
          obj_rows.set_allocator(allocator_handle);
        }
      }

      while (OB_SUCC(ret) && processed_line_count < execute_param_->batch_row_count_) {
        if (OB_FAIL(row_iter->get_next_row(new_row))) {
          if (OB_UNLIKELY(OB_ITER_END != ret)) {
            LOG_WARN("fail to get next row", KR(ret));
          } else {
            is_iter_end = true;
            ret = OB_SUCCESS;
            break;
          }
        } else {
          //此时row中的每个obj的内容指向的是data parser中的内存
          //因此得把它们深拷贝一遍
          ObTableLoadObjRow tmp_obj_row;
          tmp_obj_row.seq_no_= sequence_no++;
          tmp_obj_row.cells_ = (!is_heap_table ? new_row->cells_ : new_row->cells_ + 1);
          tmp_obj_row.count_ = (!is_heap_table ? new_row->count_ : new_row->count_ - 1);
          ObTableLoadObjRow obj_row;
          if (OB_FAIL(obj_row.deep_copy(tmp_obj_row, allocator_handle))) {
            LOG_WARN("failed to deep copy add assign to tmp_obj_row", KR(ret));
          } else if (OB_FAIL(obj_rows.push_back(obj_row))) {
            LOG_WARN("failed to push back", KR(ret));
          } else {
            ++processed_line_count;
          }
        }
      }
      if (OB_SUCC(ret) && processed_line_count > 0) {
        ATOMIC_AAF(&execute_ctx_->job_stat_->parsed_rows_, processed_line_count);
        if (OB_FAIL(direct_loader->write_trans(trans_ctx, session_id, obj_rows))) {
          LOG_WARN("fail to write objs", KR(ret), K(session_id));
        } else {
          total_processed_line_count += processed_line_count;
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(direct_loader->commit_trans(trans_ctx))) {
        LOG_WARN("fail to commit trans", KR(ret), K(partition_idx));
      }
    }
    ATOMIC_AAF(&total_line_count_, total_processed_line_count);
    if (nullptr != row_iter) {
      row_iter->~ObNewRowIterator();
      allocator.free(row_iter);
      row_iter = nullptr;
    }
  }
  LOG_INFO("finish process partition", K(partition_idx), K(subpart_count), K(subpart_idx), KR(ret),
           K(ATOMIC_LOAD(&total_line_count_)), K(total_processed_line_count));
  return ret;
}

void ObLoadDataDirectImpl::BackupLoadExecutor::task_finished(ObTableLoadTask *task, int ret_code)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::BackupLoadExecutor not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == task)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(task), K(worker_count_));
  } else {
    if (OB_UNLIKELY(OB_SUCCESS != ret_code)) {
      ATOMIC_VCAS(&task_error_code_, common::OB_SUCCESS, ret_code);
    }
    task_allocator_.free(task);
    task_controller_.on_task_finished();
  }
}

int ObLoadDataDirectImpl::BackupLoadExecutor::check_status()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLoadDataDirectImpl::BackupLoadExecutor not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(OB_SUCCESS != task_error_code_)) {
    ret = OB_CANCELED;
    LOG_WARN("load is error, canceled", KR(ret), K(task_error_code_));
  } else if (OB_FAIL(execute_ctx_->exec_ctx_.check_status())) {
    LOG_WARN("fail to check exec ctx status", KR(ret));
  } else if (OB_FAIL(execute_ctx_->direct_loader_->check_status())) {
    LOG_WARN("fail to check direct loader", KR(ret));
  }
  return ret;
}

/**
 * ObLoadDataDirectImpl
 */

ObLoadDataDirectImpl::ObLoadDataDirectImpl()
  : ctx_(nullptr), load_stmt_(nullptr)
{
}

ObLoadDataDirectImpl::~ObLoadDataDirectImpl()
{
}

int ObLoadDataDirectImpl::execute(ObExecContext &ctx, ObLoadDataStmt &load_stmt)
{
  int ret = OB_SUCCESS;
  ctx_ = &ctx;
  load_stmt_ = &load_stmt;
  const ObLoadArgument &load_args = load_stmt_->get_load_arguments();
  ObSQLSessionInfo *session = nullptr;
  ObSchemaGetterGuard *schema_guard = nullptr;
  int64_t total_line_count = 0;
  const bool is_backup = ObLoadDataFormat::OB_BACKUP_1_4 == load_args.access_info_.get_load_data_format();

  if (OB_UNLIKELY(load_args.file_iter_.count() > ObTableLoadSequenceNo::MAX_DATA_ID)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not support file counts more than 65535", KR(ret), K(load_args.file_iter_.count()));
    FORWARD_USER_ERROR_MSG(ret, "not support file counts %ld more than 65535", load_args.file_iter_.count());
  } else if (OB_ISNULL(session = ctx.get_my_session()) || OB_ISNULL(ctx.get_stmt_factory()) ||
             OB_ISNULL(ctx.get_stmt_factory()->get_query_ctx()) || OB_ISNULL(ctx_->get_sql_ctx()) ||
             OB_ISNULL(schema_guard = ctx_->get_sql_ctx()->schema_guard_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ctx is unexpected", KR(ret), K(ctx));
  } else if (OB_FAIL(plan_.set_vars(ctx.get_stmt_factory()->get_query_ctx()->variables_))) {
    LOG_WARN("fail to set vars", KR(ret));
  } else if (OB_FAIL(session->set_cur_phy_plan(&plan_))) {
    LOG_WARN("fail to set cur phy plan", KR(ret));
  } else if (FALSE_IT(ctx.reference_my_plan(&plan_))) {
  } else if (OB_FAIL(ctx.init_phy_op(1))) {
    LOG_WARN("fail to init phy op", KR(ret));
  }

  if (OB_SUCC(ret)) {
    int64_t query_timeout = 0;
    if (OB_FAIL(load_stmt_->get_hints().get_value(ObLoadDataHint::QUERY_TIMEOUT, query_timeout))) {
      LOG_WARN("fail to get value", K(ret));
    } else if (query_timeout < 0) {
      ret = OB_TIMEOUT;
      LOG_WARN("session is timeout", K(ret));
    } else if (0 == query_timeout) {
      if (OB_FAIL(session->get_query_timeout(query_timeout))) {
        LOG_WARN("fail to get query timeout", KR(ret));
      } else if (query_timeout <= 0) {
        ret = OB_TIMEOUT;
        LOG_WARN("session is timeout", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t timeout_ts = session->get_query_start_time() + query_timeout;
      ctx.get_physical_plan_ctx()->set_timeout_timestamp(timeout_ts);
      THIS_WORKER.set_timeout_ts(timeout_ts);
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(init_execute_param())) {
      LOG_WARN("fail to init execute param", KR(ret), K(ctx), K(load_stmt));
    } else if (OB_FAIL(ObTableLoadService::check_support_direct_load(*schema_guard,
                                                                     execute_param_.table_id_,
                                                                     execute_param_.method_,
                                                                     execute_param_.insert_mode_,
                                                                     ObDirectLoadMode::LOAD_DATA))) {
      LOG_WARN("fail to check support direct load", KR(ret));
    } else if (OB_FAIL(init_execute_context())) {
      LOG_WARN("fail to init execute context", KR(ret), K(ctx), K(load_stmt));
    } else {
      LOG_INFO("LOAD DATA init finish", K_(execute_param), "file_path", load_args.file_name_);
      ObLoadDataStat *job_stat = execute_ctx_.job_stat_;
      OZ(ob_write_string(job_stat->allocator_, load_args.file_name_, job_stat->file_path_));
      job_stat->file_column_ = execute_param_.data_access_param_.file_column_num_;
      job_stat->load_mode_ = static_cast<int64_t>(execute_param_.dup_action_);
    }
  }

  if (OB_SUCC(ret) && !is_backup) {
    FileLoadExecutor *file_load_executor = nullptr;
    DataDescIterator data_desc_iter;
    if (1 == load_args.file_iter_.count() && 0 == execute_param_.ignore_row_num_ &&
        SimpleDataSplitUtils::is_simple_format(execute_param_.data_access_param_.file_format_,
                                               execute_param_.data_access_param_.file_cs_type_)) {
      DataDesc data_desc;
      data_desc.filename_ = load_args.file_name_;
      if (OB_FAIL(SimpleDataSplitUtils::split(execute_param_.data_access_param_, data_desc,
                                              execute_param_.thread_count_, data_desc_iter))) {
        LOG_WARN("fail to split data", KR(ret));
      }
    } else {
      if (OB_FAIL(data_desc_iter.copy(load_args.file_iter_))) {
        LOG_WARN("fail to copy file iter", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (1 == data_desc_iter.count()) {
        // large file load
        if (OB_ISNULL(file_load_executor =
                        OB_NEWx(LargeFileLoadExecutor, execute_ctx_.allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new LargeFileLoadExecutor", KR(ret));
        }
      } else if (data_desc_iter.count() > 1) {
        // multi files load
        if (OB_ISNULL(file_load_executor =
                        OB_NEWx(MultiFilesLoadExecutor, execute_ctx_.allocator_))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to new MultiFilesLoadExecutor", KR(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(file_load_executor->init(execute_param_, execute_ctx_, data_desc_iter))) {
        LOG_WARN("fail to init file load executor", KR(ret));
      } else if (OB_FAIL(file_load_executor->execute())) {
        LOG_WARN("fail to execute file load", KR(ret));
      } else {
        total_line_count = file_load_executor->get_total_line_count();
      }
    }
    if (nullptr != file_load_executor) {
      file_load_executor->~FileLoadExecutor();
      file_load_executor = nullptr;
    }
  }
  if (OB_SUCC(ret) && is_backup) {
    BackupLoadExecutor *backup_load_executor = nullptr;
    if (OB_ISNULL(backup_load_executor = OB_NEWx(BackupLoadExecutor, execute_ctx_.allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to new BackupLoadExecutor", KR(ret));
    } else if (OB_FAIL(backup_load_executor->init(execute_param_, execute_ctx_, load_args.file_name_))) {
      LOG_WARN("fail to init backup load executor", KR(ret));
    } else if (OB_FAIL(backup_load_executor->execute())) {
      LOG_WARN("fail to execute backup load executor", KR(ret));
    } else {
      total_line_count = backup_load_executor->get_total_line_count();
    }
    if (nullptr != backup_load_executor) {
      backup_load_executor->~BackupLoadExecutor();
      backup_load_executor = nullptr;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(direct_loader_.commit())) {
      LOG_WARN("fail to commit direct loader", KR(ret));
    } else {
      const ObTableLoadResultInfo &result_info = direct_loader_.get_result_info();
      ObPhysicalPlanCtx *phy_plan_ctx = ctx.get_physical_plan_ctx();
      phy_plan_ctx->set_affected_rows(result_info.rows_affected_);
      phy_plan_ctx->set_row_matched_count(total_line_count);
      phy_plan_ctx->set_row_deleted_count(result_info.deleted_);
      phy_plan_ctx->set_row_duplicated_count(result_info.skipped_);
    }
  }

  direct_loader_.destroy();
  if (OB_NOT_NULL(session)) {
    session->reset_cur_phy_plan_to_null();
  }

  return ret;
}

int ObLoadDataDirectImpl::init_execute_param()
{
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt_->get_load_arguments();
  const ObLoadDataHint &hint = load_stmt_->get_hints();
  const ObIArray<ObLoadDataStmt::FieldOrVarStruct> &field_or_var_list =
    load_stmt_->get_field_or_var_list();
  const bool is_backup = ObLoadDataFormat::OB_BACKUP_1_4 == load_args.access_info_.get_load_data_format();
  execute_param_.tenant_id_ = load_args.tenant_id_;
  execute_param_.database_id_ = load_args.database_id_;
  execute_param_.table_id_ = load_args.table_id_;
  execute_param_.database_name_ = load_args.database_name_;
  execute_param_.table_name_ = load_args.table_name_;
  execute_param_.combined_name_ = load_args.combined_name_;
  execute_param_.ignore_row_num_ = load_args.ignore_rows_;
  execute_param_.dup_action_ = load_args.dupl_action_;
  // parallel_
  if (OB_SUCC(ret)) {
    ObTenant *tenant = nullptr;
    int64_t hint_parallel = 0;
    if (OB_FAIL(hint.get_value(ObLoadDataHint::PARALLEL_THREADS, hint_parallel))) {
      LOG_WARN("fail to get value of PARALLEL_THREADS", KR(ret), K(hint));
    } else if (OB_FAIL(GCTX.omt_->get_tenant(execute_param_.tenant_id_, tenant))) {
      LOG_WARN("fail to get tenant handle", KR(ret), K(execute_param_.tenant_id_));
    } else {
      hint_parallel = hint_parallel > 0 ? hint_parallel : DEFAULT_PARALLEL_THREAD_COUNT;
      execute_param_.parallel_ = hint_parallel;
    }
  }
  // batch_row_count_
  if (OB_SUCC(ret)) {
    int64_t hint_batch_size = 0;
    if (OB_FAIL(hint.get_value(ObLoadDataHint::BATCH_SIZE, hint_batch_size))) {
      LOG_WARN("fail to get value of BATCH_SIZE", KR(ret), K(hint));
    } else {
      execute_param_.batch_row_count_ =
        hint_batch_size > 0 ? hint_batch_size : DEFAULT_BUFFERRED_ROW_COUNT;
    }
  }
  // direct load hint
  if (OB_SUCC(ret)) {
    const ObDirectLoadHint &direct_load_hint = hint.get_direct_load_hint();
    if (direct_load_hint.is_enable()) {
      execute_param_.need_sort_ = direct_load_hint.need_sort();
      execute_param_.max_error_rows_ = direct_load_hint.get_max_error_row_count();
      execute_param_.method_ =
        (direct_load_hint.is_inc_direct_load() ? ObDirectLoadMethod::INCREMENTAL
                                               : ObDirectLoadMethod::FULL);
      execute_param_.insert_mode_ = ObDirectLoadInsertMode::NORMAL;
      if (OB_UNLIKELY(direct_load_hint.is_inc_load_method())) {
        if (OB_UNLIKELY(ObLoadDupActionType::LOAD_REPLACE == load_args.dupl_action_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("replace for inc load method not supported", KR(ret),
                   K(direct_load_hint), K(load_args.dupl_action_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace for inc load method in direct load is");
        }
      } else if (direct_load_hint.is_inc_replace_load_method()) {
        if (OB_UNLIKELY(ObLoadDupActionType::LOAD_STOP_ON_DUP != load_args.dupl_action_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("replace or ignore for inc_replace load method not supported", KR(ret),
                   K(direct_load_hint), K(load_args.dupl_action_));
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "replace or ignore for inc_replace load method in direct load is");
        } else {
          execute_param_.dup_action_ = ObLoadDupActionType::LOAD_REPLACE; // rewrite dup action
          execute_param_.insert_mode_ = ObDirectLoadInsertMode::INC_REPLACE;
        }
      }
    } else { // append
      execute_param_.need_sort_ = true;
      execute_param_.max_error_rows_ = 0;
      execute_param_.method_ = ObDirectLoadMethod::FULL;
      execute_param_.insert_mode_ = ObDirectLoadInsertMode::NORMAL;
    }
  }
  // online_opt_stat_gather_
  if (OB_SUCC(ret)) {
    int64_t no_gather_optimizer_statistics = 0 ;
    ObSQLSessionInfo *session = nullptr;
    ObObj obj;
    if (OB_ISNULL(session = ctx_->get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("session is null", KR(ret));
    } else if (OB_FAIL(session->get_sys_variable(SYS_VAR__OPTIMIZER_GATHER_STATS_ON_LOAD, obj))) {
      LOG_WARN("fail to get sys variable", K(ret));
    } else if (OB_FAIL(hint.get_value(ObLoadDataHint::NO_GATHER_OPTIMIZER_STATISTICS, no_gather_optimizer_statistics))) {
      LOG_WARN("fail to get value of APPEND", K(ret));
    } else if (no_gather_optimizer_statistics == 0 && obj.get_bool()) {
      execute_param_.online_opt_stat_gather_  = true;
    } else {
      execute_param_.online_opt_stat_gather_ = false;
    }
  }
  // data_access_param_
  if (OB_SUCC(ret)) {
    DataAccessParam &data_access_param = execute_param_.data_access_param_;
    data_access_param.file_location_ = load_args.load_file_storage_;
    data_access_param.file_column_num_ = field_or_var_list.count();
    data_access_param.file_format_ = load_stmt_->get_data_struct_in_file();
    data_access_param.file_cs_type_ = load_args.file_cs_type_;
    data_access_param.access_info_ = load_args.access_info_;
  }
  // column_ids_
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard *schema_guard = ctx_->get_sql_ctx()->schema_guard_;
    int64_t column_count = 0;
    execute_param_.column_ids_.reset();
    if (is_backup) {
      if (OB_FAIL(ObTableLoadSchema::get_column_ids(*schema_guard,
                                                   execute_param_.tenant_id_,
                                                   execute_param_.table_id_,
                                                   execute_param_.column_ids_))) {
          LOG_WARN("fail to get column ids for backup", KR(ret));
      }
    } else {
      if (OB_FAIL(ObTableLoadSchema::get_user_column_count(*schema_guard,
                                                           execute_param_.tenant_id_,
                                                           execute_param_.table_id_,
                                                           column_count))) {
        LOG_WARN("fail to get user column count", KR(ret));
      } else if (OB_UNLIKELY(column_count != field_or_var_list.count())) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("not contain all columns is not supported", KR(ret), K(column_count),
                 K(field_or_var_list));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < field_or_var_list.count(); ++i) {
        const ObLoadDataStmt::FieldOrVarStruct &field_or_var_struct = field_or_var_list.at(i);
        if (OB_UNLIKELY(!field_or_var_struct.is_table_column_)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("var is not supported", KR(ret), K(field_or_var_struct), K(i), K(field_or_var_list));
        } else if (OB_FAIL(execute_param_.column_ids_.push_back(field_or_var_struct.column_id_))) {
          LOG_WARN("fail to push back column id", KR(ret));
        }
      }
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::init_execute_context()
{
  int ret = OB_SUCCESS;
  const ObLoadArgument &load_args = load_stmt_->get_load_arguments();
  const bool is_backup = ObLoadDataFormat::OB_BACKUP_1_4 == load_args.access_info_.get_load_data_format();
  execute_ctx_.exec_ctx_.exec_ctx_ = ctx_;
  execute_ctx_.allocator_ = &ctx_->get_allocator();
  ObCompressorType table_compressor_type = ObCompressorType::NONE_COMPRESSOR;
  ObTableLoadParam load_param;
  load_param.tenant_id_ = execute_param_.tenant_id_;
  load_param.table_id_ = execute_param_.table_id_;
  load_param.parallel_ = execute_param_.parallel_;
  load_param.session_count_ = execute_param_.parallel_;
  load_param.batch_size_ = execute_param_.batch_row_count_;
  load_param.max_error_row_count_ = execute_param_.max_error_rows_;
  load_param.column_count_ = execute_param_.column_ids_.count();
  load_param.need_sort_ = is_backup ? false : execute_param_.need_sort_;
  load_param.dup_action_ = execute_param_.dup_action_;
  load_param.px_mode_ = false;
  load_param.online_opt_stat_gather_ = execute_param_.online_opt_stat_gather_;
  load_param.method_ = execute_param_.method_;
  load_param.insert_mode_ = execute_param_.insert_mode_;
  load_param.load_mode_ = ObDirectLoadMode::LOAD_DATA;

  double online_sample_percent = 100.;
  if (OB_SUCC(ret)) {
    if (execute_param_.online_opt_stat_gather_ &&
               OB_FAIL(ObDbmsStatsUtils::get_sys_online_estimate_percent(*ctx_,
                                                                         online_sample_percent))) {
      LOG_WARN("failed to get sys online sample percent", K(ret));
    } else {
      load_param.online_sample_percent_ = online_sample_percent;
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTableLoadSchema::get_table_compressor_type(
          execute_param_.tenant_id_, execute_param_.table_id_, table_compressor_type))) {
      LOG_WARN("fail to get table compressor type", KR(ret));
    } else if (OB_FAIL(ObDDLUtil::get_temp_store_compress_type(
                 table_compressor_type, execute_param_.parallel_, load_param.compressor_type_))) {
      LOG_WARN("fail to get tmp store compressor type", KR(ret));
    } else if (OB_FAIL(ObDbmsStatsUtils::get_sys_online_estimate_percent(*ctx_,
                                                                         load_param.online_sample_percent_))) {
      LOG_WARN("fail to get sys online estimate percent", KR(ret));
    } else if (OB_FAIL(direct_loader_.init(load_param, execute_param_.column_ids_,
                                           &execute_ctx_.exec_ctx_))) {
      LOG_WARN("fail to init direct loader", KR(ret));
    } else if (OB_FAIL(init_logger())) {
      LOG_WARN("fail to init logger", KR(ret));
    }
  }
  if (OB_SUCC(ret)) {
    execute_ctx_.direct_loader_ = &direct_loader_;
    execute_ctx_.job_stat_ = direct_loader_.get_job_stat();
    execute_ctx_.logger_ = &logger_;
    const ObTableLoadTableCtx *tmp_ctx = nullptr;
    if (OB_ISNULL(tmp_ctx = direct_loader_.get_table_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get table ctx", KR(ret));
    } else {
      execute_param_.thread_count_ = tmp_ctx->param_.write_session_count_;
      execute_param_.data_mem_usage_limit_ = MIN(execute_param_.thread_count_ * 2, MAX_DATA_MEM_USAGE_LIMIT);
      tmp_ctx = nullptr;
    }
  }
  return ret;
}

int ObLoadDataDirectImpl::init_logger()
{
  int ret = OB_SUCCESS;
  ObString load_info;
  char *buf = nullptr;
  const int64_t buf_len = MAX_BUFFER_SIZE;
  int64_t pos = 0;
  ObSQLSessionInfo *session = nullptr;
  if (OB_ISNULL(session = ctx_->get_my_session())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session is null", KR(ret));
  } else if (OB_ISNULL(buf = static_cast<char *>(ctx_->get_allocator().alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory", KR(ret), K(buf_len));
  } else {
    const ObString &cur_query_str = session->get_current_query_string();
    const ObLoadArgument &load_args = load_stmt_->get_load_arguments();
    int64_t current_time = ObTimeUtil::current_time();
    OZ(databuff_printf(buf, buf_len, pos,
                       "Tenant name:\t%.*s\n"
                       "File name:\t%.*s\n"
                       "Into table:\t%.*s\n"
                       "Parallel:\t%ld\n"
                       "Batch size:\t%ld\n"
                       "SQL trace:\t%s\n",
                       session->get_tenant_name().length(), session->get_tenant_name().ptr(),
                       load_args.file_name_.length(), load_args.file_name_.ptr(),
                       load_args.combined_name_.length(), load_args.combined_name_.ptr(),
                       execute_param_.thread_count_, execute_param_.batch_row_count_,
                       ObCurTraceId::get_trace_id_str()));
    OZ(databuff_printf(buf, buf_len, pos, "Start time:\t"));
    OZ(ObTimeConverter::datetime_to_str(current_time, TZ_INFO(session), ObString(),
                                        MAX_SCALE_FOR_TEMPORAL, buf, buf_len, pos, true));
    OZ(databuff_printf(buf, buf_len, pos, "\n"));
    OZ(databuff_printf(buf, buf_len, pos, "Load query: \n%.*s\n", cur_query_str.length(),
                       cur_query_str.ptr()));
    OX(load_info.assign_ptr(buf, pos));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(logger_.init(load_info, execute_param_.max_error_rows_))) {
      LOG_WARN("fail to init logger", KR(ret));
    }
  }
  return ret;
}

} // namespace sql
} // namespace oceanbase

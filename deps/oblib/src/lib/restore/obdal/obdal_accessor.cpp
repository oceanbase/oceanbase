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

#include "obdal_accessor.h"
#include "lib/restore/ob_object_storage_base.h"

namespace oceanbase
{
namespace common
{

void convert_obdal_error(const opendal_error *error, int &ob_errcode);

void opendal_bytes_init(opendal_bytes &bytes, const char *buf, const int64_t buf_size, const int64_t buf_capacity)
{
  bytes.data = (uint8_t *)buf;
  bytes.len = static_cast<uintptr_t>(buf_size);
  bytes.capacity = static_cast<uintptr_t>(buf_capacity);
}


// @brief Wrap the function in opendal.h and redirect its return
// value to void or opendal_error*
class ObDalWrapper
{
public:
  static opendal_error *obdal_init_env(void *malloc, void *free, void *log_handler, const int32_t log_level, const int64_t thread_cnt, const int64_t pool_max_idle_per_host, const int64_t pool_max_idle_time_s, const int64_t connect_timeout_s) 
  {
    return opendal_init_env(malloc, free, log_handler, log_level, thread_cnt, pool_max_idle_per_host, pool_max_idle_time_s, connect_timeout_s); 
  }
  static void obdal_fin_env()
  {
    opendal_fin_env();
  }
  // span
  static void obdal_span_new(ObSpan *&span, const int64_t tenant_id, const char *trace_id)
  {
    span = ob_new_span(tenant_id, trace_id);
  }
  static void obdal_span_free(ObSpan *&span)
  {
    ob_drop_span(span);
  } 
  // operator
  static void obdal_operator_options_new(opendal_operator_options *&options)
  {
    options = opendal_operator_options_new();
  }
  static opendal_error *obdal_operator_options_set(opendal_operator_options *options, const char *key, const char *value)
  {
    return opendal_operator_options_set(options, key, value);
  }
  static void obdal_operator_options_free(opendal_operator_options *&options)
  {
    opendal_operator_options_free(options);
  }

  static opendal_error *obdal_operator_new(const char *scheme, const opendal_operator_options *options, opendal_operator *&op)
  {
    opendal_result_operator_new result = opendal_operator_new(scheme, options);
    op = result.op;
    return result.error;
  }
  static void obdal_operator_free(opendal_operator *op)
  {
    opendal_operator_free(op);
  }
  static opendal_error *obdal_operator_write(const opendal_operator *op, const char *path, const char *buf, const int64_t buf_size)
  {
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);
    
    return opendal_operator_write(op, path, &bytes);
  }
  static opendal_error *obdal_operator_reader(const opendal_operator *op, const char *path, opendal_reader *&reader)
  {
    opendal_result_operator_reader result = opendal_operator_reader(op, path);
    reader = result.reader;
    return result.error;
  }
  static opendal_error *obdal_operator_writer(const opendal_operator *op, const char *path, opendal_writer *&writer)
  {
    opendal_result_operator_writer result = opendal_operator_writer(op, path);
    writer = result.writer;
    return result.error;
  }
  static opendal_error *obdal_operator_append_writer(const opendal_operator *op, const char *path, opendal_writer *&writer)
  {
    opendal_result_operator_writer result = opendal_operator_append_writer(op, path);
    writer = result.writer;
    return result.error;
  }
  static opendal_error *obdal_operator_multipart_writer(const opendal_operator *op, const char *path, opendal_multipart_writer *&writer)
  {
    opendal_result_operator_multipart_writer result = opendal_operator_multipart_writer(op, path);
    writer = result.multipart_writer;
    return result.error;
  }
  static opendal_error *obdal_operator_delete(const opendal_operator *op, const char *path)
  {
    return opendal_operator_delete(op, path);
  }
  static opendal_error *obdal_operator_stat(const opendal_operator *op, const char *path, opendal_metadata *&meta)
  {
    opendal_result_stat result = opendal_operator_stat(op, path);
    meta = result.meta;
    return result.error;
  }
  static opendal_error *obdal_operator_list(const opendal_operator *op, const char *path, const int64_t limit, const bool recursive, const char *start_after, opendal_lister *&lister)
  {
    opendal_result_list result = opendal_operator_list(op, path, limit, recursive, start_after);
    lister = result.lister;
    return result.error;
  }
  static opendal_error *obdal_operator_deleter(const opendal_operator *op, opendal_deleter *&deleter)
  {
    opendal_result_operator_deleter result = opendal_operator_deleter(op);
    deleter = result.deleter;
    return result.error;
  }

  static opendal_error *obdal_operator_put_object_tagging(const opendal_operator *op, const char *path, const opendal_object_tagging *tagging)
  {
    return opendal_operator_put_object_tagging(op, path, tagging);
  }
  static opendal_error *obdal_operator_get_object_tagging(const opendal_operator *op, const char *path, opendal_object_tagging *&tagging)
  {
    opendal_result_get_object_tagging result = opendal_operator_get_object_tagging(op, path);
    tagging = result.tagging;
    return result.error;
  }
    
  // tagging
  static void obdal_object_tagging_new(opendal_object_tagging *&tagging)
  {
    tagging = opendal_object_tagging_new();
  }
  static void obdal_object_tagging_set(opendal_object_tagging *tagging, const char *key, const char *value)
  {
    opendal_object_tagging_set(tagging, key, value);
  }
  static opendal_error *obdal_object_tagging_get(const opendal_object_tagging *tagging, const char *key, opendal_bytes &value)
  {
    opendal_result_object_tagging_get result = opendal_object_tagging_get(tagging, key);
    value = result.value;
    return result.error;
  }
  static void obdal_object_tagging_free(opendal_object_tagging *tagging)
  {
    opendal_object_tagging_free(tagging);
  }

  // list
  static opendal_error *obdal_lister_next(opendal_lister *lister, struct opendal_entry *&entry)
  {
    opendal_result_lister_next result = opendal_lister_next(lister);
    entry = result.entry;
    return result.error;
  }
  static void obdal_lister_free(opendal_lister *lister)
  {
    opendal_lister_free(lister);
  }

  // entry
  static void obdal_entry_path(const opendal_entry *entry, char *&path)
  {
    path = opendal_entry_path(entry);
  }
  static void obdal_entry_name(const opendal_entry *entry, char *&name)
  {
    name = opendal_entry_name(entry);
  }
  static void obdal_entry_metadata(const opendal_entry *entry, struct opendal_metadata *&meta)
  {
    meta = opendal_entry_metadata(entry);
  }
  static void obdal_entry_free(opendal_entry *entry)
  {
    opendal_entry_free(entry);
  }

  // metadata
  static void obdal_metadata_content_length(const opendal_metadata *metadata, int64_t &content_length)
  {
    content_length = opendal_metadata_content_length(metadata);
  }
  static void obdal_metadata_free(opendal_metadata *metadata)
  {
    opendal_metadata_free(metadata);
  }

  // reader
  static opendal_error *obdal_reader_read(opendal_reader *reader, char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size)
  {
    opendal_result_reader_read result = opendal_reader_read(reader, reinterpret_cast<uint8_t *>(buf), static_cast<uintptr_t>(buf_size), static_cast<uintptr_t>(offset));
    read_size = result.size;
    return result.error;
  }
  static void obdal_reader_free(opendal_reader *reader)
  {
    opendal_reader_free(reader);
  }

  // writer
  static opendal_error *obdal_writer_write(opendal_writer *writer, const char *buf, const int64_t buf_size, int64_t &write_size)
  {
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);
    
    opendal_result_writer_write result = opendal_writer_write(writer, &bytes);
    write_size = result.size;
    return result.error;
  }
  static opendal_error *obdal_writer_write_with_offset(opendal_writer *writer, const int64_t offset, const char *buf, const int64_t buf_size, int64_t &write_size)
  {
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);
     
    opendal_result_writer_write result = opendal_writer_write_with_offset(writer, offset, &bytes);
    write_size = result.size;
    return result.error;
  }
  static opendal_error *obdal_writer_close(opendal_writer *writer)
  {
    return opendal_writer_close(writer);
  }
  static opendal_error *obdal_writer_abort(opendal_writer *writer)
  {
    return opendal_writer_abort(writer);
  }
  static void obdal_writer_free(opendal_writer *writer)
  {
    opendal_writer_free(writer);
  }

  // multipart_writer
  static opendal_error *obdal_multipart_writer_initiate(opendal_multipart_writer *writer)
  {
    return opendal_multipart_writer_initiate(writer);
  }
  static opendal_error *obdal_multipart_writer_write(opendal_multipart_writer *writer, const char *buf, const int64_t buf_size, const int64_t part_id, int64_t &write_size)
  {
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);
    opendal_result_writer_write result = opendal_multipart_writer_write(writer, &bytes, part_id);
    write_size = result.size;
    return result.error;
  }
  static opendal_error *obdal_multipart_writer_close(opendal_multipart_writer *writer)
  {
    return opendal_multipart_writer_close(writer);
  }
  static opendal_error *obdal_multipart_writer_abort(opendal_multipart_writer *writer)
  {
    return opendal_multipart_writer_abort(writer);
  }

  static void obdal_multipart_writer_free(opendal_multipart_writer *writer)
  {
    opendal_multipart_writer_free(writer);
  }

  // deleter
  static opendal_error *obdal_deleter_delete(opendal_deleter *deleter, const char *path)
  {
    return opendal_deleter_delete(deleter, path);
  }
  static opendal_error *obdal_deleter_deleted(opendal_deleter *deleter, const char *path, bool &deleted)
  {
    opendal_result_deleter_deleted result = opendal_deleter_deleted(deleter, path);
    deleted = result.deleted;
    return result.error;
  }
  static opendal_error *obdal_deleter_flush(opendal_deleter *deleter, int64_t &deleted)
  {
    opendal_result_deleter_flush result = opendal_deleter_flush(deleter);
    deleted = result.deleted;
    return result.error;
  }
  static void obdal_deleter_free(opendal_deleter *&deleter)
  {
    opendal_deleter_free(deleter);
  }
  static void obdal_error_free(opendal_error *error)
  {
    opendal_error_free(error);
  }
  static void obdal_bytes_free(opendal_bytes *bytes)
  {
    opendal_bytes_free(bytes);
  }
  static void obdal_c_char_free(char *c_char)
  {
    opendal_c_char_free(c_char);
  }
};

class ObDalLogSpanGuard
{
public:
  ObDalLogSpanGuard();
  ~ObDalLogSpanGuard();
private:
  ObSpan *ob_span_;
};

template <typename OutcomeType = opendal_error *>
class ObStorageObdalRetryStrategy: public ObStorageIORetryStrategy<OutcomeType>
{
public:
  using typename ObStorageIORetryStrategy<OutcomeType>::RetType;
  using ObStorageIORetryStrategy<OutcomeType>::start_time_us_;
  using ObStorageIORetryStrategy<OutcomeType>::timeout_us_;

  ObStorageObdalRetryStrategy(const int64_t timeout_us = ObObjectStorageTenantGuard::get_timeout_us(), const int64_t errsim_enable = true)
      : ObStorageIORetryStrategy<OutcomeType>(timeout_us),
        errsim_enable_(errsim_enable)
  {}
  virtual ~ObStorageObdalRetryStrategy() {}

  virtual void log_error(
      const RetType &outcome, const int64_t attempted_retries) const override
  {
    int ret = OB_SUCCESS;
    if (outcome != nullptr) {
      convert_obdal_error(outcome, ret);
      ObString message(outcome->message.len, (char *) outcome->message.data);
      OB_LOG(WARN, "ObDal log error", K(ret),
          K(start_time_us_), K(timeout_us_), K(attempted_retries),
          K(outcome->code), K(message), K(outcome->is_temporary));
      ObDalWrapper::obdal_error_free(outcome);
    }
  }

protected:
  virtual bool should_retry_impl_(
      const RetType &outcome, const int64_t attempted_retries) const override
  {
    bool bret = false;
    if (errsim_enable_ && OB_SUCCESS != EventTable::EN_OBJECT_STORAGE_IO_RETRY) {
      bret = true;
      if (outcome == nullptr) {
        OB_LOG(INFO, "errsim object storage IO retry");
      } else {
        OB_LOG(INFO, "errsim object storage IO retry", K(outcome->code));
      }
    } else if (OB_ISNULL(outcome)) {
      bret = false;
    } else if (outcome->is_temporary) {
      bret = true;
    }
    return bret;
  }
private:
  bool errsim_enable_;
};

// @brief ObDalRetryLayer is responsible for:
// 1. Handle error return by operations in ObDalWrapper
// 2. retry the necessary IO in case of an error
class ObDalRetryLayer
{
public:
  static void handle_obdal_error_and_free(opendal_error *&error, int &ob_errcode)
  {
    if (OB_NOT_NULL(error)) {
      convert_obdal_error(error, ob_errcode);
      ObString message(error->message.len, (char *) error->message.data);
      OB_LOG_RET(WARN, ob_errcode, "obdal fail", K(error->code), K(message), K(error->is_temporary));
      ObDalWrapper::obdal_error_free(error);
      error = nullptr;
    }
  }
public:
  static int obdal_init_env(void *malloc, void *free, void *log_handler, const int32_t log_level, const int64_t thread_cnt, const int64_t pool_max_idle_per_host, const int64_t pool_max_idle_time_s, const int64_t connect_timeout_s)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_init_env(malloc, free, log_handler, log_level, thread_cnt, pool_max_idle_per_host, pool_max_idle_time_s, connect_timeout_s);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_fin_env()
  {
    ObDalWrapper::obdal_fin_env();
  }
  // span
  static void obdal_span_new(ObSpan *&span, const int64_t tenant_id, const char *trace_id)
  {
    ObDalWrapper::obdal_span_new(span, tenant_id, trace_id);
  }
  static void obdal_span_free(ObSpan *span)
  {
    ObDalWrapper::obdal_span_free(span);
  }
  // operator
  static void obdal_operator_options_new(opendal_operator_options *&options)
  {
    ObDalWrapper::obdal_operator_options_new(options);
  }
  static int obdal_operator_options_set(opendal_operator_options *options, const char *key, const char *value)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_options_set(options, key, value);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_operator_options_free(opendal_operator_options *&options)
  {
    ObDalWrapper::obdal_operator_options_free(options);
  }

  static int obdal_operator_new(const char *scheme, const opendal_operator_options *options, opendal_operator *&op)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_new(scheme, options, op);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_operator_free(opendal_operator *op)
  {
    ObDalWrapper::obdal_operator_free(op);
  }
  
  // need retry
  static int obdal_operator_write(const opendal_operator *op, const char *path, const char *buf, const int64_t buf_size)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_operator_write, op, path, buf, buf_size);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_reader(const opendal_operator *op, const char *path, opendal_reader *&reader)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_reader(op, path, reader);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_writer(const opendal_operator *op, const char *path, opendal_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_writer(op, path, writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_append_writer(const opendal_operator *op, const char *path, opendal_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_append_writer(op, path, writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_multipart_writer(const opendal_operator *op, const char *path, opendal_multipart_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_multipart_writer(op, path, writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_delete(const opendal_operator *op, const char *path)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_operator_delete, op, path);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  
  // need retry
  static int obdal_operator_stat(const opendal_operator *op, const char *path, opendal_metadata *&meta)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_operator_stat, op, path, std::ref(meta));
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_list(const opendal_operator *op, const char *path, const int64_t limit, const bool recursive, const char *start_after, opendal_lister *&lister)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_list(op, path, limit, recursive, start_after, lister);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret; 
  }
  static int obdal_operator_deleter(const opendal_operator *op, opendal_deleter *&deleter)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_operator_deleter(op, deleter);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret; 
  }
  // need retry
  static int obdal_operator_put_object_tagging(const opendal_operator *op, const char *path, const opendal_object_tagging *tagging)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_operator_put_object_tagging, op, path, tagging);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  // need retry
  static int obdal_operator_get_object_tagging(const opendal_operator *op, const char *path, opendal_object_tagging *&tagging)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_operator_get_object_tagging, op, path, std::ref(tagging));
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_object_tagging_new(opendal_object_tagging *&tagging)
  {
    ObDalWrapper::obdal_object_tagging_new(tagging);
  }
  static void obdal_object_tagging_set(opendal_object_tagging *tagging, const char *key, const char *value)
  {
    ObDalWrapper::obdal_object_tagging_set(tagging, key, value);
  }
  static int obdal_object_tagging_get(const opendal_object_tagging *tagging, const char *key, opendal_bytes &value)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_object_tagging_get(tagging, key, value);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_object_tagging_free(opendal_object_tagging *tagging)
  {
    ObDalWrapper::obdal_object_tagging_free(tagging);
  }
  static int obdal_lister_next(opendal_lister *lister, struct opendal_entry *&entry)
  {
    int ret = OB_SUCCESS;
    // Each call to obdal_lister_next does not necessarily involve I/O operations. 
    // When I/O does not occur, ignoring the return value (is_temporary) and retrying is not idempotent. 
    // Therefore, it is important to disregard the errsim test in this context. 
    // As a result, the errsim retry tests in ob_admin will ignore the error injection for this function.
    ObStorageObdalRetryStrategy<> strategy(ObObjectStorageTenantGuard::get_timeout_us(), false/* errsim_enable */);
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_lister_next, lister, std::ref(entry));
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_lister_free(opendal_lister *lister)
  {
    ObDalWrapper::obdal_lister_free(lister);
  }
  static void obdal_entry_path(const opendal_entry *entry, char *&path)
  {
    ObDalWrapper::obdal_entry_path(entry, path);
  }
  static void obdal_entry_name(const opendal_entry *entry, char *&name)
  {
    ObDalWrapper::obdal_entry_name(entry, name);
  }
  static void obdal_entry_metadata(const opendal_entry *entry, struct opendal_metadata *&meta)
  {
    ObDalWrapper::obdal_entry_metadata(entry, meta);
  }
  static void obdal_entry_free(opendal_entry *&entry)
  {
    ObDalWrapper::obdal_entry_free(entry);
  }
  static void obdal_metadata_content_length(const opendal_metadata *metadata, int64_t &content_length)
  {
    ObDalWrapper::obdal_metadata_content_length(metadata, content_length);
  }
  static void obdal_metadata_free(opendal_metadata *metadata)
  {
    ObDalWrapper::obdal_metadata_free(metadata);
  }
  static int obdal_reader_read(opendal_reader *reader, char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_reader_read, reader, buf, buf_size, offset, std::ref(read_size));
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_reader_free(opendal_reader *reader)
  {
    ObDalWrapper::obdal_reader_free(reader);
  }
  static int obdal_writer_write(opendal_writer *writer, const char *buf, const int64_t buf_size, int64_t &write_size)
  {
    int ret = OB_SUCCESS;
    // The operation is not idempotent, so it is not retried
    opendal_error *error = ObDalWrapper::obdal_writer_write(writer, buf, buf_size, write_size);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_writer_write_with_offset(opendal_writer *writer, const int64_t offset, const char *buf, const int64_t buf_size, int64_t &write_size)
  {
    int ret = OB_SUCCESS;
    // The operation is not idempotent, so it is not retried
    opendal_error *error = ObDalWrapper::obdal_writer_write_with_offset(writer, offset, buf, buf_size, write_size);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_writer_close(opendal_writer *writer)
  {
    int ret = OB_SUCCESS;
    // The operation is not idempotent, so it is not retried
    opendal_error *error = ObDalWrapper::obdal_writer_close(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_writer_abort(opendal_writer *writer)
  {
    int ret = OB_SUCCESS;
    // The operation is not idempotent, so it is not retried
    opendal_error *error = ObDalWrapper::obdal_writer_abort(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_writer_free(opendal_writer *writer)
  {
    ObDalWrapper::obdal_writer_free(writer);
  }
  static int obdal_multipart_writer_initiate(opendal_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_multipart_writer_initiate(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_multipart_writer_write(opendal_multipart_writer *writer, const char *buf, const int64_t buf_size, const int64_t part_id, int64_t &write_size)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_multipart_writer_write(writer, buf, buf_size, part_id, write_size);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_multipart_writer_close(opendal_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    // The operation is not idempotent, so it is not retried
    opendal_error *error = ObDalWrapper::obdal_multipart_writer_close(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_multipart_writer_abort(opendal_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_multipart_writer_abort(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static void obdal_multipart_writer_free(opendal_multipart_writer *writer)
  {
    ObDalWrapper::obdal_multipart_writer_free(writer);
  }

  // deleter
  static int obdal_deleter_delete(opendal_deleter *deleter, const char *path)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_deleter_delete(deleter, path);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_deleter_deleted(opendal_deleter *deleter, const char *path, bool &deleted)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = ObDalWrapper::obdal_deleter_deleted(deleter, path, deleted);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_deleter_flush(opendal_deleter *deleter, int64_t &deleted)
  {
    int ret = OB_SUCCESS;
    ObStorageObdalRetryStrategy<> strategy;
    opendal_error *error = execute_until_timeout(strategy, ObDalWrapper::obdal_deleter_flush, deleter, std::ref(deleted));
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_deleter_free(opendal_deleter *&deleter)
  {
    ObDalWrapper::obdal_deleter_free(deleter);
  }
  static void obdal_bytes_free(opendal_bytes *bytes)
  {
    ObDalWrapper::obdal_bytes_free(bytes); 
  }
  static void obdal_c_char_free(char *c_char)
  {
    ObDalWrapper::obdal_c_char_free(c_char);
  }
};

template<typename Function, typename ... Args>
static int do_safely(Function f, Args && ... args)
{
  int ret = OB_SUCCESS;
  try {
    ret = f(std::forward<Args>(args)...);
  } catch (const std::exception &e) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "caught exception when doing obdal operation", K(ret), K(e.what()));
  } catch (...) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "caught unknown exception when doing obdal operation", K(ret));
  }
  return ret;
}

template<typename Function, typename ... Args>
static int do_safely_without_ret(Function f, Args && ... args)
{
  int ret = OB_SUCCESS;
  try {
    f(std::forward<Args>(args)...);
  } catch (const std::exception &e) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "caught exception when doing obdal operation", K(ret), K(e.what()));
  } catch (...) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "caught unknown exception when doing obdal operation", K(ret));
  }
  return ret;
}

void convert_obdal_error(const opendal_error *error, int &ob_errcode)
{
  ob_errcode = OB_SUCCESS;
  if (OB_NOT_NULL(error)) {
     if (error->code == OPENDAL_UNEXPECTED) {
      // returning it back. For example, s3 returns an internal service error.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_UNSUPPORTED) {
      // Underlying service doesn't support this operation.
      ob_errcode = OB_NOT_SUPPORTED;
    } else if (error->code == OPENDAL_CONFIG_INVALID) {
      // The config for backend is invalid.
      ob_errcode = OB_INVALID_ARGUMENT;
    } else if (error->code == OPENDAL_NOT_FOUND) {
      // The given path is not found.
      ob_errcode = OB_OBJECT_NOT_EXIST;
    } else if (error->code == OPENDAL_PERMISSION_DENIED) {
      // The given path doesn't have enough permission for this operation.
      ob_errcode = OB_OBJECT_STORAGE_PERMISSION_DENIED;
    } else if (error->code == OPENDAL_IS_A_DIRECTORY) {
      // The given path is a directory.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_NOT_A_DIRECTORY) {
      // The given path is not a directory.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_ALREADY_EXISTS) {
      // The given path already exists thus we failed to the specified operation on it.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_RATE_LIMITED) {
      // Requests that sent to this path is over the limit, please slow down.
      ob_errcode = OB_IO_LIMIT;
    } else if (error->code == OPENDAL_IS_SAME_FILE) {
      // The given file paths are same.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_CONDITION_NOT_MATCH) {
      // The condition of this operation is not match.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_RANGE_NOT_SATISFIED) {
      // The range of the content is not satisfied.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_INVALID_OBJECT_STORAGE_ENDPOINT) {
      ob_errcode = OB_INVALID_OBJECT_STORAGE_ENDPOINT;
    } else if (error->code == OPENDAL_CHECKSUM_ERROR) {
      // checksum error are offten caused by network issues, so we convert it to 
      // io error to make it easier for user to retry.
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    } else if (error->code == OPENDAL_REGION_MISMATCH) {
      ob_errcode = OB_S3_REGION_MISMATCH;
    } else if (error->code == OPENDAL_TIMED_OUT) {
      ob_errcode = OB_TIMEOUT;
    } else if (error->code == OPENDAL_CHECKSUM_UNSUPPORTED) {
      ob_errcode = OB_CHECKSUM_TYPE_NOT_SUPPORTED;
    } else if (error->code == OPENDAL_PWRITE_OFFSET_NOT_MATCH) {
      ob_errcode = OB_OBJECT_STORAGE_PWRITE_OFFSET_NOT_MATCH;
    } else {
      ob_errcode = OB_OBJECT_STORAGE_IO_ERROR;
    }
  }
}

//========================= ObDalLogSpanGuard =========================

ObDalLogSpanGuard::ObDalLogSpanGuard()
{
  int ret = OB_SUCCESS;
  const int64_t tenant_id = ob_get_tenant_id();
  const char *trace_id = common::ObCurTraceId::get_trace_id_str();

  if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_span_new, ob_span_, tenant_id, trace_id))) {
    OB_LOG(WARN, "failed new span", K(ret), K(tenant_id), K(trace_id));
  } else if (OB_ISNULL(ob_span_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to new span", K(ret), K(tenant_id), K(trace_id));
  }
}

ObDalLogSpanGuard::~ObDalLogSpanGuard()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ob_span_)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_span_free, ob_span_))) {
      OB_LOG(WARN, "failed to free span", K(ret), KP(ob_span_)); 
    }
  }
}

//========================= ObDalAccessor =========================

int ObDalAccessor::init_env(
    void *malloc, 
    void *free, 
    void *log_handler, 
    const int32_t log_level,
    const int64_t thread_cnt, 
    const int64_t pool_max_idle_per_host, 
    const int64_t pool_max_idle_time_s,
    const int64_t connect_timeout_s)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(malloc) || OB_ISNULL(free) || OB_ISNULL(log_handler) 
      || OB_UNLIKELY(log_level < 0 || log_level >= OB_LOG_LEVEL_MAX)
      || OB_UNLIKELY(thread_cnt <= 0 || pool_max_idle_per_host <= 0 || pool_max_idle_time_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(malloc), KP(free), KP(log_handler), K(log_level), K(thread_cnt), K(pool_max_idle_per_host), K(pool_max_idle_time_s));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_init_env, malloc, free, log_handler, log_level, thread_cnt, pool_max_idle_per_host, pool_max_idle_time_s, connect_timeout_s))) {
    OB_LOG(WARN, "failed init obdal env", K(ret), KP(malloc), KP(free), KP(log_handler), K(log_level), K(thread_cnt), K(pool_max_idle_per_host), K(pool_max_idle_time_s)); 
  }
  return ret;
}

void ObDalAccessor::fin_env()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_fin_env))) {
    OB_LOG(WARN, "failed to fin obdal env", K(ret));
  }
}

int ObDalAccessor::obdal_operator_options_new(opendal_operator_options *&options)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  options = nullptr;
  if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_operator_options_new, std::ref(options)))) {
    OB_LOG(WARN, "failed to new operator options", K(ret));
  } else if (OB_ISNULL(options)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to new operator options", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_options_set(
    opendal_operator_options *options, 
    const char *key, 
    const char *value)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(options) || OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(options), KP(key), KP(value));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_options_set, options, key, value))) {
    OB_LOG(WARN, "failed to set operator options", K(ret), KP(options), K(key), K(value));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_options_free(opendal_operator_options *&options)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(options)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_operator_options_free, options))) {
      OB_LOG(WARN, "failed to free operator options", K(ret));
    }
    options = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_operator_new(
    const char *scheme,
    const opendal_operator_options *options,
    opendal_operator *&op)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  op = nullptr;
  if (OB_ISNULL(scheme) || OB_ISNULL(options)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(scheme), KP(options));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_new, scheme, options, std::ref(op)))) {
    OB_LOG(WARN, "failed to new obdal operator", K(ret), K(scheme));
  } else if (OB_ISNULL(op)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to new obdal operator", K(ret), K(scheme));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_free(opendal_operator *&op)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(op)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_operator_free, op))) {
      OB_LOG(WARN, "failed to free operator options", K(ret));
    }
    op = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_operator_write(
    const opendal_operator *op,
    const char *path,
    const char *buf,
    const int64_t buf_size)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(path) || OB_ISNULL(buf) || OB_UNLIKELY(buf_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path), KP(buf), K(buf_size));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_write, op, path, buf, buf_size))) {
    OB_LOG(WARN, "failed to exec operator write", K(ret), K(path), K(buf_size));
  }
  return ret;
}
int ObDalAccessor::obdal_operator_reader(
    const opendal_operator *op,
    const char *path,
    opendal_reader *&reader)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  reader = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_reader, op, path, std::ref(reader)))) {
    OB_LOG(WARN, "fail to get obdal reader", K(ret), K(path));
  } else if (OB_ISNULL(reader)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to get obdal reader without errmsg", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_writer(
    const opendal_operator *op,
    const char *path,
    opendal_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_writer, op, path, std::ref(writer)))) {
    OB_LOG(WARN, "fail to get operator writer", K(ret), K(path));
  } else if (OB_ISNULL(writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to get operator writer without errmsg", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_append_writer(
    const opendal_operator *op,
    const char *path,
    opendal_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_append_writer, op, path, std::ref(writer)))) {
    OB_LOG(WARN, "fail to get operator append writer", K(ret), K(path));
  } else if (OB_ISNULL(writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to get operator append writer without errmsg", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_multipart_writer(
      const opendal_operator *op,
      const char *path,
      opendal_multipart_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_multipart_writer, op, path, std::ref(writer)))) {
    OB_LOG(WARN, "fail to get operator multipart writer", K(ret), K(path));
  } else if (OB_ISNULL(writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to get operator multipart writer without errmsg", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_delete(
    const opendal_operator *op, 
    const char *path)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_delete, op, path))) {
    OB_LOG(WARN, "fail to exec operator delete", K(ret), K(path));
  } 
  return ret;
}

int ObDalAccessor::obdal_operator_stat(
    const opendal_operator *op, 
    const char *path, 
    opendal_metadata *&meta)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_result_stat result; 
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_stat, op, path, std::ref(meta)))) {
    OB_LOG(WARN, "failed to get meta", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_list(
    const struct opendal_operator *op,
    const char *path,
    const int64_t limit,
    const bool recursive,
    const char *start_after,
    opendal_lister *&lister)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  lister = nullptr;
  if (OB_ISNULL(op) || OB_UNLIKELY(limit <= 0) || OB_ISNULL(start_after)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", K(ret), KP(op), KP(path), K(limit), K(recursive), KP(start_after));
  } else if (path == nullptr) {
    if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_list, op, "", limit, recursive, start_after, std::ref(lister)))) {
      OB_LOG(WARN, "failed to get obdal lister", K(ret), K(path), K(limit), K(recursive), K(start_after));
    }
  } else {
    if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_list, op, path, limit, recursive, start_after, std::ref(lister)))) {
      OB_LOG(WARN, "failed to get obdal lister", K(ret), K(path), K(limit), K(recursive), K(start_after));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(lister)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to get obdal lister", K(ret), K(path), K(limit), K(recursive), K(start_after));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_deleter(
    const opendal_operator *op, 
    opendal_deleter *&deleter)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  deleter = nullptr;
  if (OB_ISNULL(op)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_deleter, op, std::ref(deleter)))) {
    OB_LOG(WARN, "failed to get operator deleter", K(ret));
  } else if (OB_ISNULL(deleter)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to get operator deleter without errmsg", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_put_object_tagging(
    const opendal_operator *op,
    const char *path,
    const opendal_object_tagging *tagging)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(op) || OB_ISNULL(path) || OB_ISNULL(tagging)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path), KP(tagging));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_put_object_tagging, op, path, tagging))) {
    OB_LOG(WARN, "failed to put object tagging", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_get_object_tagging(
    const opendal_operator *op,
    const char *path,
    opendal_object_tagging *&tagging)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  tagging = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_operator_get_object_tagging, op, path, tagging))) {
    OB_LOG(WARN, "failed to exec opendal_operator_get_object_tagging", K(ret), K(path));
  } else if (OB_ISNULL(tagging)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to get object tagging without errmsg", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_object_tagging_new(opendal_object_tagging *&tagging)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  tagging = nullptr;
  if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_object_tagging_new, std::ref(tagging)))) {
    OB_LOG(WARN, "failed to new object tagging", K(ret));
  } else if (OB_ISNULL(tagging)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to new object tagging", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_object_tagging_set(opendal_object_tagging *tagging, const char *key, const char *value)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tagging) || OB_ISNULL(key) || OB_ISNULL(value)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(tagging), KP(key), KP(value));
  } else if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_object_tagging_set, tagging, key, value))) {
    OB_LOG(WARN, "failed to set object tagging", K(ret), K(key), K(value));
  }
  return ret;
}

int ObDalAccessor::obdal_object_tagging_get(const opendal_object_tagging *tagging, const char *key, opendal_bytes &value)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tagging) || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(tagging), KP(key));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_object_tagging_get, tagging, key, std::ref(value)))) {
    OB_LOG(WARN, "faield to exec opendal_object_tagging_get", K(ret), K(key));
  }
  return ret;
}

int ObDalAccessor::obdal_object_tagging_free(opendal_object_tagging *&tagging)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tagging)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_object_tagging_free, tagging))) {
      OB_LOG(WARN, "failed to free object tagging", K(ret));
    }
    tagging = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_lister_next(opendal_lister *lister, opendal_entry *&entry)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(lister)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(lister));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_lister_next, lister, std::ref(entry)))) {
    OB_LOG(WARN, "failed to lister next", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_lister_free(opendal_lister *&lister)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(lister)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_lister_free, lister))) {
      OB_LOG(WARN, "failed to free lister", K(ret));
    }
    lister = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_entry_path(const opendal_entry *entry, char *&path)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  path = nullptr;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(entry));
  } else if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_entry_path, entry, std::ref(path)))) {
    OB_LOG(WARN, "failed to get entry path", K(ret));
  } else if (OB_ISNULL(path)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to get entry path", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_entry_name(const opendal_entry *entry, char *&name)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  name = nullptr;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(entry));
  } else if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_entry_name, entry, std::ref(name)))) {
    OB_LOG(WARN, "failed to get entry name", K(ret));
  } else if (OB_ISNULL(name)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to get entry name", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_entry_metadata(const opendal_entry *entry, opendal_metadata *&meta)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(entry));
  } else if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_entry_metadata, entry, std::ref(meta)))) {
    OB_LOG(WARN, "failed to get entry metadata", K(ret));
  } else if (OB_ISNULL(meta)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "failed to get entry metadata", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_entry_free(opendal_entry *&entry)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(entry)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_entry_free, entry))) {
      OB_LOG(WARN, "failed to free entry", K(ret));
    }
    entry = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_metadata_content_length(
    const opendal_metadata *metadata,
    int64_t &content_length)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(metadata)) {
    int ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(metadata));
  } else if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_metadata_content_length, metadata, std::ref(content_length)))) {
    OB_LOG(WARN, "failed to get metadata content length", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_metadata_free(opendal_metadata *&metadata)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(metadata)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_metadata_free, metadata))) {
      OB_LOG(WARN, "failed to free metadata", K(ret));
    }
    metadata = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_reader_read(
    opendal_reader *reader, 
    char *buf, 
    const int64_t buf_size, 
    const int64_t offset, 
    int64_t &read_size)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(reader) || OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0) || OB_UNLIKELY(offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(reader), KP(buf), K(buf_size), K(offset));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_reader_read, reader, buf, buf_size, offset, std::ref(read_size)))) {
    OB_LOG(WARN, "failed to reader range read", K(ret), K(buf_size), K(offset));
  }
  return ret;
}

int ObDalAccessor::obdal_reader_free(opendal_reader *&reader)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(reader)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_reader_free, reader))) {
      OB_LOG(WARN, "failed to free reader", K(ret));
    }
    reader = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_writer_write(
    opendal_writer *writer,
    const char *buf,
    const int64_t buf_size)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (OB_ISNULL(writer) || OB_ISNULL(buf) || OB_UNLIKELY(buf_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer), KP(buf), K(buf_size));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_writer_write, writer, buf, buf_size, std::ref(write_size)))) {
    OB_LOG(WARN, "failed to exec writer write", K(ret), K(buf_size));
  } else if (OB_UNLIKELY(write_size != buf_size)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "write size not equal to real size", K(ret), K(write_size), K(buf_size));
  }
  return ret;
}

int ObDalAccessor::obdal_writer_write_with_offset(
    opendal_writer *writer,
    const int64_t offset,
    const char *buf,
    const int64_t buf_size)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (OB_ISNULL(writer) || OB_UNLIKELY(offset < 0) || OB_ISNULL(buf) || OB_UNLIKELY(buf_size < 0)) {
    int ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer), K(offset), KP(buf), K(buf_size));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_writer_write_with_offset, writer, offset, buf, buf_size, std::ref(write_size)))) {
    OB_LOG(WARN, "failed to exec writer write with offset", K(ret), K(offset), K(buf_size));
  } else if (OB_UNLIKELY(write_size != buf_size)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "write size not equal to real size", K(ret), K(write_size), K(buf_size));
  }
  return ret;
}

int ObDalAccessor::obdal_writer_close(opendal_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_error *error = nullptr;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_writer_close, writer))) {
    OB_LOG(WARN, "failed to close writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_writer_abort(opendal_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_error *error = nullptr;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_writer_abort, writer))) {
    OB_LOG(WARN, "failed to abort writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_writer_free(opendal_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(writer)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_writer_free, writer))) {
      OB_LOG(WARN, "failed to free writer", K(ret));
    }
    writer = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_multipart_writer_initiate(opendal_multipart_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_error *error = nullptr;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_multipart_writer_initiate, writer))) {
    OB_LOG(WARN, "failed to init multipart writer", K(ret));
  } 
  return ret;
}

int ObDalAccessor::obdal_multipart_writer_write(
    opendal_multipart_writer *writer,
    const char *buf,
    const int64_t buf_size,
    const int64_t part_id)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  int64_t write_size = 0;
  if (OB_ISNULL(writer) || OB_ISNULL(buf) || OB_UNLIKELY(buf_size < 0) || OB_UNLIKELY(part_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer), KP(buf), K(buf_size), K(part_id));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_multipart_writer_write, writer, buf, buf_size, part_id, std::ref(write_size)))) {
    OB_LOG(WARN, "failed to exec multipart writer write", K(ret));
  } else if (OB_UNLIKELY(write_size != buf_size)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "write size not equal to real size", K(ret), K(write_size), K(buf_size));
  }
  return ret;
}

int ObDalAccessor::obdal_multipart_writer_close(opendal_multipart_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_error *error = nullptr;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_multipart_writer_close, writer))) {
    OB_LOG(WARN, "failed to close multipart writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_multipart_writer_abort(opendal_multipart_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_multipart_writer_abort, writer))) {
    OB_LOG(WARN, "failed to abort multipart writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_multipart_writer_free(opendal_multipart_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(writer)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_multipart_writer_free, writer))) {
      OB_LOG(WARN, "failed to free multipart writer", K(ret));
    }
    writer = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_deleter_delete(opendal_deleter *deleter, const char *path)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(deleter) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_deleter_delete, deleter, path))) {
    OB_LOG(WARN, "failed to add file to deleter", K(ret), K(path));
  } 
  return ret;
}

int ObDalAccessor::obdal_deleter_deleted(
    opendal_deleter *deleter,
    const char *path,
    bool &deleted)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(deleter) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(deleter), KP(path));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_deleter_deleted, deleter, path, std::ref(deleted)))) {
    OB_LOG(WARN, "failed to check path is deleted", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_deleter_flush(opendal_deleter *deleter, int64_t &deleted)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;

  if (OB_ISNULL(deleter)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(deleter));
  } else if (OB_FAIL(do_safely(ObDalRetryLayer::obdal_deleter_flush, deleter, std::ref(deleted)))) {
    OB_LOG(WARN, "failed to flush deleter", K(ret));
  } 
  return ret;
}

int ObDalAccessor::obdal_deleter_free(opendal_deleter *&deleter)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(deleter)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_deleter_free, deleter))) {
      OB_LOG(WARN, "failed to free deleter", K(ret));
    }
    deleter = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_bytes_free(opendal_bytes *bytes)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(bytes)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_bytes_free, bytes))) {
      OB_LOG(WARN, "failed to free opendal_bytes", K(ret));
    }
    bytes = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_c_char_free(char *&c_char)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(c_char)) {
    if (OB_FAIL(do_safely_without_ret(ObDalRetryLayer::obdal_c_char_free, c_char))) {
      OB_LOG(WARN, "failed to free c_char", K(ret));
    }
    c_char = nullptr;
  }
  return ret;
}

} //namespace common
} //namespace oceanbase
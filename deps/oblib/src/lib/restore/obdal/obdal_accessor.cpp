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

namespace oceanbase
{
namespace common
{


void opendal_bytes_init(opendal_bytes &bytes, const char *buf, const int64_t buf_size, const int64_t buf_capacity)
{
  bytes.data = (uint8_t *)buf;
  bytes.len = static_cast<uintptr_t>(buf_size);
  bytes.capacity = static_cast<uintptr_t>(buf_capacity);
}

void handle_obdal_error_and_free(opendal_error *&error, int &ob_errcode)
{
  if (OB_NOT_NULL(error)) {
    convert_obdal_error(error, ob_errcode);
    ObString message(error->message.len, (char *) error->message.data);
    OB_LOG_RET(WARN, ob_errcode, "obdal fail", K(error->code), K(message), K(error->is_temporary));
    opendal_error_free(error);
    error = nullptr;
  }
}

// @brief Wrap the function in opendal.h and redirect its return
// value to void or opendal_error*
class ObDalWrapper
{
public:
  static int obdal_init_env(
      void *malloc,
      void *free,
      void *log_handler,
      const int32_t log_level,
      const int64_t work_thread_cnt,
      const int64_t blocking_thread_cnt,
      const int64_t block_thread_keep_alive_time_s,
      const int64_t pool_max_idle_per_host,
      const int64_t pool_max_idle_time_s,
      const int64_t connect_timeout_s)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_init_env(malloc,
                                            free,
                                            log_handler,
                                            log_level,
                                            work_thread_cnt,
                                            blocking_thread_cnt,
                                            block_thread_keep_alive_time_s,
                                            pool_max_idle_per_host,
                                            pool_max_idle_time_s,
                                            connect_timeout_s);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static void obdal_fin_env()
  {
    opendal_fin_env();
  }

  static void obdal_get_tenant_id(int64_t &tenant_id)
  {
    tenant_id = opendal_get_tenant_id();
  }
  // span
  static ObSpan *obdal_span_new(const int64_t tenant_id, const char *trace_id)
  {
    return ob_new_span(tenant_id, trace_id);
  }
  static void obdal_span_free(ObSpan *&span)
  {
    ob_drop_span(span);
  }

  static int obdal_operator_new(const char *scheme, const opendal_operator_config *config, opendal_operator *&op)
  {
    int ret = OB_SUCCESS;
    opendal_result_operator_new result = opendal_operator_new2(scheme, config);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    op = result.op;
    return ret;
  }
  static void obdal_operator_free(opendal_operator *op)
  {
    opendal_operator_free(op);
  }
  static int obdal_async_operator_new(const char *scheme, const opendal_operator_config *config, opendal_async_operator *&op)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_async_operator_new(scheme, config, &op);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_async_operator_free(opendal_async_operator *&op)
  {
    opendal_async_operator_free(op);
  }

  static int obdal_async_operator_multipart_writer(const opendal_async_operator *op, const char *path, opendal_async_multipart_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_async_operator_multipart_writer(op, path, &writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static int obdal_async_multipart_writer_initiate(opendal_async_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_async_multipart_writer_initiate(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static int obdal_async_multipart_writer_close(opendal_async_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_async_multipart_writer_close(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static int obdal_async_multipart_writer_abort(opendal_async_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_async_multipart_writer_abort(writer);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static void obdal_async_multipart_writer_free(opendal_async_multipart_writer *&writer)
  {
    opendal_async_multipart_writer_free(writer);
  }

  static int obdal_operator_write(const opendal_operator *op, const char *path, const char *buf, const int64_t buf_size)
  {
    int ret = OB_SUCCESS;
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);

    opendal_error *error = opendal_operator_write(op, path, &bytes);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_write_with_if_not_exists(const opendal_operator *op, const char *path, const char *buf, const int64_t buf_size)
  {
    int ret = OB_SUCCESS;
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);
    opendal_error *error = opendal_operator_write_with_if_not_exists(op, path, &bytes);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_reader(const opendal_operator *op, const char *path, opendal_reader *&reader)
  {
    int ret = OB_SUCCESS;
    opendal_result_operator_reader result = opendal_operator_reader(op, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    reader = result.reader;
    return ret;
  }
  static int obdal_operator_writer(const opendal_operator *op, const char *path, opendal_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_result_operator_writer result = opendal_operator_writer(op, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    writer = result.writer;
    return ret;
  }
  static int obdal_operator_append_writer(const opendal_operator *op, const char *path, opendal_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_result_operator_writer result = opendal_operator_append_writer(op, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    writer = result.writer;
    return ret;
  }
  static int obdal_operator_multipart_writer(const opendal_operator *op, const char *path, opendal_multipart_writer *&writer)
  {
    int ret = OB_SUCCESS;
    opendal_result_operator_multipart_writer result = opendal_operator_multipart_writer(op, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    writer = result.multipart_writer;
    return ret;
  }
  static int obdal_operator_delete(const opendal_operator *op, const char *path)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_operator_delete(op, path);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_stat(const opendal_operator *op, const char *path, opendal_metadata *&meta)
  {
    int ret = OB_SUCCESS;
    opendal_result_stat result = opendal_operator_stat(op, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    meta = result.meta;
    return ret;
  }
  static int obdal_operator_list(const opendal_operator *op, const char *path, const int64_t limit, const bool recursive, const char *start_after, opendal_lister *&lister)
  {
    int ret = OB_SUCCESS;
    opendal_result_list result = opendal_operator_list(op, path, limit, recursive, start_after);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    lister = result.lister;
    return ret;
  }
  static int obdal_operator_deleter(const opendal_operator *op, opendal_deleter *&deleter)
  {
    int ret = OB_SUCCESS;
    opendal_result_operator_deleter result = opendal_operator_deleter(op);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    deleter = result.deleter;
    return ret;
  }

  static int obdal_operator_put_object_tagging(const opendal_operator *op, const char *path, const opendal_object_tagging *tagging)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_operator_put_object_tagging(op, path, tagging);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_operator_get_object_tagging(const opendal_operator *op, const char *path, opendal_object_tagging *&tagging)
  {
    int ret = OB_SUCCESS;
    opendal_result_get_object_tagging result = opendal_operator_get_object_tagging(op, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    tagging = result.tagging;
    return ret;
  }

  // tagging
  static opendal_object_tagging *obdal_object_tagging_new()
  {
    return opendal_object_tagging_new();
  }
  static void obdal_object_tagging_set(opendal_object_tagging *tagging, const char *key, const char *value)
  {
    opendal_object_tagging_set(tagging, key, value);
  }
  static int obdal_object_tagging_get(const opendal_object_tagging *tagging, const char *key, opendal_bytes &value)
  {
    int ret = OB_SUCCESS;
    opendal_result_object_tagging_get result = opendal_object_tagging_get(tagging, key);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    value = result.value;
    return ret;
  }
  static void obdal_object_tagging_free(opendal_object_tagging *tagging)
  {
    opendal_object_tagging_free(tagging);
  }

  // list
  static int obdal_lister_next(opendal_lister *lister, struct opendal_entry *&entry)
  {
    int ret = OB_SUCCESS;
    opendal_result_lister_next result = opendal_lister_next(lister);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    entry = result.entry;
    return ret;
  }
  static void obdal_lister_free(opendal_lister *lister)
  {
    opendal_lister_free(lister);
  }

  // entry
  static char *obdal_entry_path(const opendal_entry *entry)
  {
    return opendal_entry_path(entry);
  }
  static char *obdal_entry_name(const opendal_entry *entry)
  {
    return opendal_entry_name(entry);
  }
  static opendal_metadata *obdal_entry_metadata(const opendal_entry *entry)
  {
    return opendal_entry_metadata(entry);
  }
  static void obdal_entry_free(opendal_entry *entry)
  {
    opendal_entry_free(entry);
  }

  // metadata
  static int64_t obdal_metadata_content_length(const opendal_metadata *metadata)
  {
    return opendal_metadata_content_length(metadata);
  }
  static int64_t obdal_metadata_last_modified(const opendal_metadata *metadata)
  {
    return opendal_metadata_last_modified_ms(metadata) / 1000LL;
  }
  static char *obdal_metadata_etag(const opendal_metadata *metadata)
  {
    return opendal_metadata_etag(metadata);
  }
  static char *obdal_metadata_content_md5(const opendal_metadata *metadata)
  {
    return opendal_metadata_content_md5(metadata);
  }
  static void obdal_metadata_free(opendal_metadata *metadata)
  {
    opendal_metadata_free(metadata);
  }

  // reader
  static int obdal_reader_read(opendal_reader *reader, char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size)
  {
    int ret = OB_SUCCESS;
    opendal_result_reader_read result = opendal_reader_read(reader, reinterpret_cast<uint8_t *>(buf), static_cast<uintptr_t>(buf_size), static_cast<uintptr_t>(offset));
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    read_size = result.size;
    return ret;
  }
  static void obdal_reader_free(opendal_reader *reader)
  {
    opendal_reader_free(reader);
  }

  // writer
  static int obdal_writer_write(opendal_writer *writer, const char *buf, const int64_t buf_size, int64_t &write_size)
  {
    int ret = OB_SUCCESS;
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);

    opendal_result_writer_write result = opendal_writer_write(writer, &bytes);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    write_size = result.size;
    return ret;
  }
  static int obdal_writer_write_with_offset(opendal_writer *writer, const int64_t offset, const char *buf, const int64_t buf_size, int64_t &write_size)
  {
    int ret = OB_SUCCESS;
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);

    opendal_result_writer_write result = opendal_writer_write_with_offset(writer, offset, &bytes);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    write_size = result.size;
    return ret;
  }
  static int obdal_writer_close(opendal_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_writer_close(writer);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_writer_abort(opendal_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_writer_abort(writer);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static void obdal_writer_free(opendal_writer *writer)
  {
    opendal_writer_free(writer);
  }

  // multipart_writer
  static int obdal_multipart_writer_initiate(opendal_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_multipart_writer_initiate(writer);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_multipart_writer_write(opendal_multipart_writer *writer, const char *buf, const int64_t buf_size, const int64_t part_id, int64_t &write_size)
  {
    int ret = OB_SUCCESS;
    opendal_bytes bytes;
    opendal_bytes_init(bytes, buf, buf_size, buf_size);
    opendal_result_writer_write result = opendal_multipart_writer_write(writer, &bytes, part_id);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    write_size = result.size;
    return ret;
  }
  static int obdal_multipart_writer_close(opendal_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_multipart_writer_close(writer);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_multipart_writer_abort(opendal_multipart_writer *writer)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_multipart_writer_abort(writer);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }

  static void obdal_multipart_writer_free(opendal_multipart_writer *writer)
  {
    opendal_multipart_writer_free(writer);
  }

  // deleter
  static int obdal_deleter_delete(opendal_deleter *deleter, const char *path)
  {
    int ret = OB_SUCCESS;
    opendal_error *error = opendal_deleter_delete(deleter, path);
    if (OB_UNLIKELY(nullptr != error)) {
      handle_obdal_error_and_free(error, ret);
    }
    return ret;
  }
  static int obdal_deleter_deleted(opendal_deleter *deleter, const char *path, bool &deleted)
  {
    int ret = OB_SUCCESS;
    opendal_result_deleter_deleted result = opendal_deleter_deleted(deleter, path);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    deleted = result.deleted;
    return ret;
  }
  static int obdal_deleter_flush(opendal_deleter *deleter, int64_t &deleted)
  {
    int ret = OB_SUCCESS;
    opendal_result_deleter_flush result = opendal_deleter_flush(deleter);
    if (OB_UNLIKELY(nullptr != result.error)) {
      handle_obdal_error_and_free(result.error, ret);
    }
    deleted = result.deleted;
    return ret;
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
  static char *obdal_calc_md5(const char *buf, const int64_t buf_size)
  {
    return opendal_calc_md5(reinterpret_cast<const uint8_t *>(buf), static_cast<uintptr_t>(buf_size));
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
      ob_errcode = OB_OBJECT_STORAGE_CONDITION_NOT_MATCH;
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
    } else if (error->code == OPENDAL_FILE_IMMUTABLE) {
      ob_errcode = OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM;
    } else if (error->code == OPENDAL_OVERWRITE_CONTENT_MISMATCH) {
      ob_errcode = OB_OBJECT_STORAGE_OVERWRITE_CONTENT_MISMATCH;
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

  if (OB_ISNULL(ob_span_ = ObDalWrapper::obdal_span_new(tenant_id, trace_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed new span", K(ret), K(tenant_id), K(trace_id));
  }
}

ObDalLogSpanGuard::~ObDalLogSpanGuard()
{
  if (OB_NOT_NULL(ob_span_)) {
    ObDalWrapper::obdal_span_free(ob_span_);
  }
}

//========================= ObDalAccessor =========================

int ObDalAccessor::init_env(
    void *malloc,
    void *free,
    void *log_handler,
    const int32_t log_level,
    const int64_t work_thread_cnt,
    const int64_t blocking_thread_cnt,
    const int64_t block_thread_keep_alive_time_s,
    const int64_t pool_max_idle_per_host,
    const int64_t pool_max_idle_time_s,
    const int64_t connect_timeout_s)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(malloc) || OB_ISNULL(free) || OB_ISNULL(log_handler)
      || OB_UNLIKELY(log_level < 0 || log_level >= OB_LOG_LEVEL_MAX)
      || OB_UNLIKELY(work_thread_cnt <= 0 || blocking_thread_cnt <= 0 || block_thread_keep_alive_time_s <= 0 || pool_max_idle_per_host <= 0 || pool_max_idle_time_s <= 0 || connect_timeout_s <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(malloc), KP(free), KP(log_handler), K(log_level), K(work_thread_cnt), K(pool_max_idle_per_host), K(pool_max_idle_time_s), K(connect_timeout_s));
  } else if (OB_FAIL(ObDalWrapper::obdal_init_env(malloc, free, log_handler, log_level, work_thread_cnt, blocking_thread_cnt, block_thread_keep_alive_time_s, pool_max_idle_per_host, pool_max_idle_time_s, connect_timeout_s))) {
    OB_LOG(WARN, "failed init obdal env", K(ret), KP(malloc), KP(free), KP(log_handler), K(log_level), K(work_thread_cnt), K(blocking_thread_cnt), K(block_thread_keep_alive_time_s), K(pool_max_idle_per_host), K(pool_max_idle_time_s), K(connect_timeout_s));
  }
  return ret;
}

void ObDalAccessor::fin_env()
{
  ObDalWrapper::obdal_fin_env();
}

int64_t ObDalAccessor::obdal_get_tenant_id()
{
  int64_t tenant_id = OB_SERVER_TENANT_ID;
  ObDalWrapper::obdal_get_tenant_id(tenant_id);
  return tenant_id;
}

const char *ObDalAccessor::obdal_get_trace_id()
{
  return ::opendal_get_trace_id();
}

int ObDalAccessor::obdal_operator_config_new(opendal_operator_config *&config)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  config = nullptr;
  if (OB_ISNULL(config = ::opendal_operator_config_new())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to new operator config", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_config_free(opendal_operator_config *&config)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(config)) {
    ::opendal_operator_config_free(config);
    config = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_async_operator_new(const char *scheme, const opendal_operator_config *config, opendal_async_operator *&op)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  op = nullptr;
  if (OB_ISNULL(scheme) || OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(scheme), KP(config));
  } else if (OB_FAIL(ObDalWrapper::obdal_async_operator_new(scheme, config, op))) {
    OB_LOG(WARN, "failed to new obdal async operator", K(ret), K(scheme));
  } else if (OB_ISNULL(op)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to new obdal async operator", K(ret), K(scheme));
  }
  return ret;
}

void ObDalAccessor::obdal_async_operator_free(opendal_async_operator *&op)
{
  ObDalLogSpanGuard obdal_span;
  if (OB_NOT_NULL(op)) {
    ObDalWrapper::obdal_async_operator_free(op);
    op = nullptr;
  }
}

void ObDalAccessor::obdal_async_operator_read(
    const opendal_async_operator *op,
    const char *path,
    char *buf,
    const int64_t buf_size,
    const int64_t offset,
    OpenDalAsyncCallbackFn callback,
    void *ctx)
{
  ObDalLogSpanGuard obdal_span;
  opendal_async_operator_read(op, path, reinterpret_cast<uint8_t *>(buf), buf_size, offset, callback, ctx);
}

void ObDalAccessor::obdal_async_operator_write(
    const opendal_async_operator *op,
    const char *path,
    const char *buf,
    const int64_t buf_size,
    OpenDalAsyncCallbackFn callback,
    void *ctx)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_bytes bytes;
  opendal_bytes_init(bytes, buf, buf_size, buf_size);
  opendal_async_operator_write(op, path, &bytes, callback, ctx);
}

void ObDalAccessor::obdal_async_operator_write_with_worm_check(
    const opendal_async_operator *op,
    const char *path,
    const char *buf,
    const int64_t buf_size,
    OpenDalAsyncCallbackFn callback,
    void *ctx)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_bytes bytes;
  opendal_bytes_init(bytes, buf, buf_size, buf_size);
  opendal_async_operator_write_with_worm_check(op, path, &bytes, callback, ctx);
}

void ObDalAccessor::obdal_async_operator_write_with_if_match(
    const opendal_async_operator *op,
    const char *path,
    const char *buf,
    const int64_t buf_size,
    OpenDalAsyncCallbackFn callback,
    void *ctx)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  opendal_bytes bytes;
  opendal_bytes_init(bytes, buf, buf_size, buf_size);
  opendal_async_operator_write_with_if_match(op, path, &bytes, callback, ctx);
}

int ObDalAccessor::obdal_async_operator_multipart_writer(const opendal_async_operator *op, const char *path, opendal_async_multipart_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  writer = nullptr;
  if (OB_ISNULL(op) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(op), KP(path));
  } else if (OB_FAIL(ObDalWrapper::obdal_async_operator_multipart_writer(op, path, writer))) {
    OB_LOG(WARN, "failed to get async multipart writer", K(ret), K(path));
  } else if (OB_ISNULL(writer)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to get async multipart writer without errmsg", K(ret), K(path));
  }
  return ret;
}

int ObDalAccessor::obdal_async_multipart_writer_initiate(opendal_async_multipart_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(ObDalWrapper::obdal_async_multipart_writer_initiate(writer))) {
    OB_LOG(WARN, "failed to initiate async multipart writer", K(ret));
  }
  return ret;
}

void ObDalAccessor::obdal_async_multipart_writer_write(
    opendal_async_multipart_writer *writer,
    const char *buf,
    const int64_t buf_size,
    const int64_t part_id,
    OpenDalAsyncCallbackFn callback,
    void *ctx)
{
  ObDalLogSpanGuard obdal_span;
  opendal_bytes bytes;
  opendal_bytes_init(bytes, buf, buf_size, buf_size);
  opendal_async_multipart_writer_write(writer, &bytes, part_id, callback, ctx);
}

int ObDalAccessor::obdal_async_multipart_writer_close(opendal_async_multipart_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(ObDalWrapper::obdal_async_multipart_writer_close(writer))) {
    OB_LOG(WARN, "failed to close async multipart writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_async_multipart_writer_abort(opendal_async_multipart_writer *writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_ISNULL(writer)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(writer));
  } else if (OB_FAIL(ObDalWrapper::obdal_async_multipart_writer_abort(writer))) {
    OB_LOG(WARN, "failed to abort async multipart writer", K(ret));
  }
  return ret;
}

void ObDalAccessor::obdal_async_multipart_writer_free(opendal_async_multipart_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  if (OB_NOT_NULL(writer)) {
    ObDalWrapper::obdal_async_multipart_writer_free(writer);
    writer = nullptr;
  }
}

int ObDalAccessor::obdal_operator_new(
  const char *scheme,
  const opendal_operator_config *config,
  opendal_operator *&op)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  op = nullptr;
  if (OB_ISNULL(scheme) || OB_ISNULL(config)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(scheme), KP(config));
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_new(scheme, config, op))) {
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
    ObDalWrapper::obdal_operator_free(op);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_write(op, path, buf, buf_size))) {
    OB_LOG(WARN, "failed to exec operator write", K(ret), K(path), K(buf_size));
  }
  return ret;
}

int ObDalAccessor::obdal_operator_write_with_if_not_exists(
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_write_with_if_not_exists(op, path, buf, buf_size))) {
    OB_LOG(WARN, "failed to exec operator write with if match", K(ret), K(path), K(buf_size));
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_reader(op, path, reader))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_writer(op, path, writer))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_append_writer(op, path, writer))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_multipart_writer(op, path, writer))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_delete(op, path))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_stat(op, path, meta))) {
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
    if (OB_FAIL(ObDalWrapper::obdal_operator_list(op, "", limit, recursive, start_after, lister))) {
      OB_LOG(WARN, "failed to get obdal lister", K(ret), K(path), K(limit), K(recursive), K(start_after));
    }
  } else {
    if (OB_FAIL(ObDalWrapper::obdal_operator_list(op, path, limit, recursive, start_after, lister))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_deleter(op, deleter))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_put_object_tagging(op, path, tagging))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_operator_get_object_tagging(op, path, tagging))) {
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
  if (OB_ISNULL(tagging = ObDalWrapper::obdal_object_tagging_new())) {
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
  } else {
    ObDalWrapper::obdal_object_tagging_set(tagging, key, value);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_object_tagging_get(tagging, key, value))) {
    OB_LOG(WARN, "faield to exec opendal_object_tagging_get", K(ret), K(key));
  }
  return ret;
}

int ObDalAccessor::obdal_object_tagging_free(opendal_object_tagging *&tagging)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(tagging)) {
    ObDalWrapper::obdal_object_tagging_free(tagging);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_lister_next(lister, entry))) {
    OB_LOG(WARN, "failed to lister next", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_lister_free(opendal_lister *&lister)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(lister)) {
    ObDalWrapper::obdal_lister_free(lister);
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
  } else if (OB_ISNULL(path = ObDalWrapper::obdal_entry_path(entry))) {
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
  } else if (OB_ISNULL(name = ObDalWrapper::obdal_entry_name(entry))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
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
  } else if (OB_ISNULL(meta = ObDalWrapper::obdal_entry_metadata(entry))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to get entry metadata", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_entry_free(opendal_entry *&entry)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(entry)) {
    ObDalWrapper::obdal_entry_free(entry);
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
  } else {
    content_length = ObDalWrapper::obdal_metadata_content_length(metadata);
  }
  return ret;
}

int ObDalAccessor::obdal_metadata_last_modified(
    const opendal_metadata *metadata,
    int64_t &last_modified_time_s)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  last_modified_time_s = -1;
  if (OB_ISNULL(metadata)) {
    int ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(metadata));
  } else {
    last_modified_time_s = ObDalWrapper::obdal_metadata_last_modified(metadata);
  }
  return ret;
}

int ObDalAccessor::obdal_metadata_etag(const opendal_metadata *metadata, char *&etag)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  etag = nullptr;
  if (OB_ISNULL(metadata)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(metadata));
  } else if (OB_ISNULL(etag = ObDalWrapper::obdal_metadata_etag(metadata))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to get metadata etag", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_metadata_content_md5(const opendal_metadata *metadata, char *&content_md5)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  content_md5 = nullptr;
  if (OB_ISNULL(metadata)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret), KP(metadata));
  } else if (OB_ISNULL(content_md5 = ObDalWrapper::obdal_metadata_content_md5(metadata))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(WARN, "failed to get metadata content md5", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_metadata_free(opendal_metadata *&metadata)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(metadata)) {
    ObDalWrapper::obdal_metadata_free(metadata);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_reader_read(reader, buf, buf_size, offset, read_size))) {
    OB_LOG(WARN, "failed to reader range read", K(ret), K(buf_size), K(offset));
  }
  return ret;
}

int ObDalAccessor::obdal_reader_free(opendal_reader *&reader)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(reader)) {
    ObDalWrapper::obdal_reader_free(reader);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_writer_write(writer, buf, buf_size, write_size))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_writer_write_with_offset(writer, offset, buf, buf_size, write_size))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_writer_close(writer))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_writer_abort(writer))) {
    OB_LOG(WARN, "failed to abort writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_writer_free(opendal_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(writer)) {
    ObDalWrapper::obdal_writer_free(writer);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_multipart_writer_initiate(writer))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_multipart_writer_write(writer, buf, buf_size, part_id, write_size))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_multipart_writer_close(writer))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_multipart_writer_abort(writer))) {
    OB_LOG(WARN, "failed to abort multipart writer", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_multipart_writer_free(opendal_multipart_writer *&writer)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(writer)) {
    ObDalWrapper::obdal_multipart_writer_free(writer);
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
  } else if (OB_FAIL(ObDalWrapper::obdal_deleter_delete(deleter, path))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_deleter_deleted(deleter, path, deleted))) {
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
  } else if (OB_FAIL(ObDalWrapper::obdal_deleter_flush(deleter, deleted))) {
    OB_LOG(WARN, "failed to flush deleter", K(ret));
  }
  return ret;
}

int ObDalAccessor::obdal_deleter_free(opendal_deleter *&deleter)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(deleter)) {
    ObDalWrapper::obdal_deleter_free(deleter);
    deleter = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_bytes_free(opendal_bytes *bytes)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(bytes)) {
    ObDalWrapper::obdal_bytes_free(bytes);
    bytes = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_c_char_free(char *&c_char)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(c_char)) {
    ObDalWrapper::obdal_c_char_free(c_char);
    c_char = nullptr;
  }
  return ret;
}

int ObDalAccessor::obdal_calc_md5(const char *buf, const int64_t buf_size, char *&md5_hex)
{
  ObDalLogSpanGuard obdal_span;
  int ret = OB_SUCCESS;
  md5_hex = nullptr;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(buf), K(buf_size));
  } else if (OB_ISNULL(md5_hex = ObDalWrapper::obdal_calc_md5(buf, buf_size))) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "failed to calc md5", KR(ret), K(buf_size));
  }
  return ret;
}

} //namespace common
} //namespace oceanbase
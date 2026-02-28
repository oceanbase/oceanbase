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

#include "ob_storage_async.h"
#include "lib/restore/ob_storage.h"
#include "lib/restore/ob_storage_obdal_base.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/stat/ob_diagnostic_info_guard.h"
#include "opendal.h"

namespace oceanbase
{
namespace common
{

bool is_valid_async_access_type(const ObStorageAccessType &access_type)
{
  return access_type == OB_STORAGE_ACCESS_READER
         || access_type == OB_STORAGE_ACCESS_NOHEAD_READER
         || access_type == OB_STORAGE_ACCESS_OVERWRITER
         || access_type == OB_STORAGE_ACCESS_DIRECT_MULTIPART_WRITER
         || access_type == OB_STORAGE_ACCESS_BUFFERED_MULTIPART_WRITER;
}

bool is_valid_async_storage_type(const ObStorageType &storage_type)
{
  return storage_type == OB_STORAGE_OSS
         || storage_type == OB_STORAGE_S3
         || storage_type == OB_STORAGE_AZBLOB;
}

//===================== ObStorageAsyncLogContextGuard ==========================
class ObStorageAsyncLogContextGuard
{
public:
  ObStorageAsyncLogContextGuard()
  {
    const int64_t tenant_id = ObDalAccessor::obdal_get_tenant_id();
    const char *trace_id = ObDalAccessor::obdal_get_trace_id();

    if (trace_id != nullptr) {
      ObCurTraceId::set(trace_id);
    }
    if (!is_valid_tenant_id(tenant_id)) {
      ob_get_tenant_id() = tenant_id;
    }
  }
  ~ObStorageAsyncLogContextGuard()
  {
    ob_get_tenant_id() = OB_INVALID_TENANT_ID;
    ObCurTraceId::reset();
  }
};

//===================== ObStorageAsyncBase ==========================

ObStorageAsyncBase::ObStorageAsyncBase()
  : ObStorageObDalBase(),
    is_opened_(false),
    file_length_(-1),
    async_op_(nullptr)
{}

ObStorageAsyncBase::~ObStorageAsyncBase() { ObStorageAsyncBase::reset(); }

void ObStorageAsyncBase::reset()
{
  if (OB_NOT_NULL(async_op_)) {
    opendal_async_operator_free(async_op_);
    async_op_ = nullptr;
  }
  is_opened_ = false;
  file_length_ = -1;
  ObStorageObDalBase::reset();
}

int ObStorageAsyncBase::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  const char *scheme = nullptr;
  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "async base is already opened", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_UNLIKELY(!is_storage_type_match(uri, storage_info->get_type()))) {
    ret = OB_INVALID_STORAGE_DEST;
    OB_LOG(WARN, "uri prefix does not match the expected device type", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObStorageObDalBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open obdal base", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(parse_obdal_scheme_from_storage_type(storage_type_, scheme))) {
    OB_LOG(WARN, "failed to parse obdal scheme", KR(ret), K(storage_type_));
  } else if (OB_FAIL(ObDalAccessor::obdal_async_operator_new(scheme, config_, async_op_))) {
    OB_LOG(WARN, "failed to new obdal async operator", KR(ret), K(scheme));
  } else {
    file_length_ = 0;
    is_opened_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

// please notice the concurrent safe
int ObStorageAsyncBase::alloc_and_assign_async_context(
    ObStorageAsyncCallback callback,
    void *ctx,
    ObStorageAsyncContext *&context,
    const int64_t offset,
    const int64_t expected_bytes)
{
  int ret = OB_SUCCESS;
  context = nullptr;
  ObMemAttr attr(OB_SERVER_TENANT_ID, OB_STORAGE_OBDAL_ALLOCATOR);
  if (OB_ISNULL(callback) || OB_ISNULL(ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(callback), KP(ctx));
  } else if (OB_ISNULL(context = static_cast<ObStorageAsyncContext *>(
                           ob_malloc(sizeof(ObStorageAsyncContext), attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to alloc async context", KR(ret));
  } else {
    new (context) ObStorageAsyncContext(this, callback, ctx, offset, expected_bytes);
    this->inc_ref();
  }
  return ret;
}

void ObStorageAsyncBase::free_async_context(ObStorageAsyncContext *&context)
{
  if (OB_NOT_NULL(context)) {
    this->dec_ref();
    context->~ObStorageAsyncContext();
    ob_free(context);
    context = nullptr;
  }
}

//===================== ObStorageAsyncReader ==========================

ObStorageAsyncReader::ObStorageAsyncReader()
  : ObStorageAsyncBase(),
    has_meta_(false)
{}

ObStorageAsyncReader::~ObStorageAsyncReader() { ObStorageAsyncReader::reset(); }

void ObStorageAsyncReader::reset()
{
  has_meta_ = false;
  ObStorageAsyncBase::reset();
}

int ObStorageAsyncReader::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info,
    const bool has_meta)
{
  int ret = OB_SUCCESS;
  ObDalObjectMeta meta;
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "async reader is already opened", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObStorageAsyncBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open async base", KR(ret), K(uri), KPC(storage_info));
  } else {
    if (has_meta) {
      if (OB_FAIL(get_file_meta(meta))) {
        OB_LOG(WARN, "failed to get file meta", KR(ret), K(uri));
      } else if (!meta.is_exist_) {
        ret = OB_OBJECT_NOT_EXIST;
        OB_LOG(WARN, "object is not exist", KR(ret), K(uri), K_(bucket), K_(object));
      } else {
        file_length_ = meta.length_;
        has_meta_ = true;
      }

      if (OB_FAIL(ret)) {
        EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_FAIL_COUNT);
      }
      EVENT_INC(ObStatEventIds::OBJECT_STORAGE_IO_HEAD_COUNT);
    }
  }

  if (OB_FAIL(ret)) {
    reset();
  } else {
    is_opened_ = true;
  }
  return ret;
}

int ObStorageAsyncReader::pread(
    char *buf,
    const int64_t buf_size,
    const int64_t offset,
    ObStorageAsyncCallback cb,
    void *ctx)
{
  int ret = OB_SUCCESS;
  ObStorageAsyncContext *context = nullptr;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "aysnc reader is not opened", KR(ret));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(buf_size <= 0 || offset < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(buf), K(buf_size), K(offset));
  } else {
    int64_t expected_bytes = buf_size;
    if (has_meta_) {
      if (file_length_ < offset) {
        ret = OB_DATA_OUT_OF_RANGE;
        OB_LOG(WARN, "offset is larger than file length", KR(ret), K(offset), K(file_length_), K(bucket_), K(object_));
      } else {
        expected_bytes = MIN(buf_size, file_length_ - offset);
      }
    }
    // Notice:
    // Once the context is successfully allocated, obdal_async_operator_read must be called.
    // ObDal guarantees that this function will call the callback regardless of success or failure,
    // and only then will the context be released from memory. Otherwise, a memory leak will occur.
    // Therefore, you cannot add any operations that might fail between these two lines.
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(alloc_and_assign_async_context(cb, ctx, context, offset, expected_bytes))) {
      OB_LOG(WARN, "failed to alloc and assign async context", KR(ret), KP(cb), KP(ctx));
    } else {
      ObDalAccessor::obdal_async_operator_read(async_op_, object_.ptr(), buf, buf_size, offset, callback, context);
    }
  }
  return ret;
}

int ObStorageAsyncReader::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

void ObStorageAsyncReader::callback(
    opendal_error *error,
    int64_t bytes,
    void *raw_ctx)
{
  ObStorageAsyncLogContextGuard log_context_guard;
  int ret = OB_SUCCESS;
  ObStorageAsyncContext *context = nullptr;
  ObStorageAsyncReader *async_reader = nullptr;
  if (OB_ISNULL(raw_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "read callback ctx is null", KR(ret), KP(raw_ctx));
  } else if (FALSE_IT(context = static_cast<ObStorageAsyncContext *>(raw_ctx))) {
  } else if (OB_UNLIKELY(context->base_ == nullptr || context->callback_ == nullptr)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "read callback ctx is invalid, base or callback is null",
           KR(ret), KP(context->base_), KP(context->callback_));
  } else {
    async_reader = static_cast<ObStorageAsyncReader *>(context->base_);
    if (async_reader->has_meta() && context->expected_bytes_ == 0) {
      if (OB_NOT_NULL(error)) {
        opendal_error_free(error);
        OB_LOG(DEBUG, "read callback error is not null, has_meta is true and expected_bytes is 0", KR(ret), K(context->expected_bytes_));
      }
      bytes = 0;
    } else {
      if (OB_UNLIKELY(error != nullptr)) {
        handle_obdal_error_and_free(error, ret);
      }

      if (OB_FAIL(ret)) {
      } else if (OB_UNLIKELY((context->expected_bytes_ < bytes)
                || (async_reader->has_meta() && context->expected_bytes_ != bytes))) {
        ret = OB_OBJECT_STORAGE_IO_ERROR;
        OB_LOG(WARN, "read size is not equal to file length", KR(ret), K(async_reader->has_meta()),
            K(context->offset_), K(context->expected_bytes_), K(bytes), K(async_reader->get_length()), K(async_reader->get_bucket()), K(async_reader->get_object()));
      }
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(context->callback_(ret, bytes, context->ctx_))) {
      OB_LOG(WARN, "read callback failed", KR(tmp_ret), KR(ret), K(bytes),
            KP(context->ctx_));
    }
    EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_DELAY, ObTimeUtility::current_time() - context->start_ts_);
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_READ_FAIL_COUNT);
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_BYTES, bytes);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_READ_COUNT);
  if (OB_NOT_NULL(context) && OB_NOT_NULL(context->base_)) {
    context->base_->free_async_context(context);
  }
}

//===================== ObStorageAsyncWriter ==========================



ObStorageAsyncWriter::ObStorageAsyncWriter()
  : ObStorageAsyncBase()
{}

ObStorageAsyncWriter::~ObStorageAsyncWriter() { ObStorageAsyncWriter::reset(); }

void ObStorageAsyncWriter::reset()
{
  ObStorageAsyncBase::reset();
}

int ObStorageAsyncWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "async writer is already opened", KR(ret), K(uri), KP(storage_info));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObStorageAsyncBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open async base", KR(ret), K(uri), KPC(storage_info));
  } else {
    is_opened_ = true;
  }
  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageAsyncWriter::write(
    const char *buf,
    const int64_t size,
    ObStorageAsyncCallback cb,
    void *ctx)
{
  int ret = OB_SUCCESS;
  ObStorageAsyncContext *context = nullptr;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), KP(buf), K(size));
  // Notice:
  // Once the context is successfully allocated, obdal_async_operator_write must be called.
  // ObDal guarantees that this function will call the callback regardless of success or failure,
  // and only then will the context be released from memory. Otherwise, a memory leak will occur.
  // Therefore, you cannot add any operations that might fail between these two lines.
  } else if (OB_FAIL(alloc_and_assign_async_context(cb, ctx, context))) {
    OB_LOG(WARN, "failed to alloc async context", KR(ret), KP(cb), KP(ctx));
  } else {
    if (obdal_account_.enable_worm_) {
      ObDalAccessor::obdal_async_operator_write_with_worm_check(async_op_, object_.ptr(), buf, size, callback, context);
    } else if (is_write_with_if_match_) {
      ObDalAccessor::obdal_async_operator_write_with_if_match(async_op_, object_.ptr(), buf, size, callback, context);
    } else {
      ObDalAccessor::obdal_async_operator_write(async_op_, object_.ptr(), buf, size, callback, context);
    }
  }
  return ret;
}

int ObStorageAsyncWriter::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

void ObStorageAsyncWriter::callback(
    opendal_error *error,
    int64_t bytes,
    void *raw_ctx)
{
  ObStorageAsyncLogContextGuard log_context_guard;
  int ret = OB_SUCCESS;
  ObStorageAsyncContext *context = nullptr;
  ObStorageAsyncWriter *async_writer = nullptr;
  if (OB_ISNULL(raw_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "write callback ctx is null", KR(ret), KP(raw_ctx));
  } else if (FALSE_IT(context = static_cast<ObStorageAsyncContext *>(raw_ctx))) {
  } else if (OB_UNLIKELY(context->base_ == nullptr || context->callback_ == nullptr)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "write callback ctx is invalid, base or callback is null",
           KR(ret), KP(context->base_), KP(context->callback_));
  } else {
    async_writer = static_cast<ObStorageAsyncWriter *>(context->base_);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    } else if (FALSE_IT(async_writer->set_length(bytes))) {
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(context->callback_(ret, bytes, context->ctx_))) {
      OB_LOG(WARN, "write callback failed", KR(tmp_ret), KR(ret), K(bytes),
             KP(context->ctx_));
    }
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - context->start_ts_);
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, bytes);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);

  if (OB_NOT_NULL(context) && OB_NOT_NULL(context->base_)) {
    context->base_->free_async_context(context);
  }
}
//===================== ObStorageAsyncMultiPartWriter ==========================

ObStorageAsyncMultiPartWriter::ObStorageAsyncMultiPartWriter()
  : ObStorageAsyncBase(),
    async_multipart_writer_(nullptr)
{}

ObStorageAsyncMultiPartWriter::~ObStorageAsyncMultiPartWriter()
{
  ObStorageAsyncMultiPartWriter::reset();
}

void ObStorageAsyncMultiPartWriter::reset()
{
  if (OB_NOT_NULL(async_multipart_writer_)) {
    ObDalAccessor::obdal_async_multipart_writer_free(async_multipart_writer_);
    async_multipart_writer_ = nullptr;
  }
  ObStorageAsyncBase::reset();
}

int ObStorageAsyncMultiPartWriter::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  OBJECT_STORAGE_GUARD(storage_info, uri, IO_HANDLED_SIZE_ZERO);

  if (OB_UNLIKELY(is_opened_)) {
    ret = OB_OPEN_TWICE;
    OB_LOG(WARN, "async multi part writer is already opened", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(uri.empty() || !storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObStorageAsyncBase::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open async base", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObDalAccessor::obdal_async_operator_multipart_writer(async_op_,
                     object_.ptr(), async_multipart_writer_))) {
    OB_LOG(WARN, "failed to new async multipart writer", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(ObDalAccessor::obdal_async_multipart_writer_initiate(async_multipart_writer_))) {
    OB_LOG(WARN, "failed to initiate async multipart writer", KR(ret), K(uri), KPC(storage_info));
  } else {
    is_opened_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageAsyncMultiPartWriter::upload_part(
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    ObStorageAsyncCallback cb,
    void *ctx)
{
  int ret = OB_SUCCESS;
  ObStorageAsyncContext *context = nullptr;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || part_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(bucket_), K(object_), KP(buf), K(size));
  // Notice:
  // Once the context is successfully allocated, obdal_async_multipart_writer_write must be called.
  // ObDal guarantees that this function will call the callback regardless of success or failure,
  // and only then will the context be released from memory. Otherwise, a memory leak will occur.
  // Therefore, you cannot add any operations that might fail between these two lines.
  } else if (OB_FAIL(alloc_and_assign_async_context(cb, ctx, context))) {
    OB_LOG(WARN, "failed to alloc async context", KR(ret));
  } else {
    ObDalAccessor::obdal_async_multipart_writer_write(async_multipart_writer_,
                     buf, size, part_id, callback, context);
  }
  return ret;
}

int ObStorageAsyncMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (ATOMIC_LOAD(&file_length_) == 0) {
    if (OB_FAIL(ObDalAccessor::obdal_operator_write(op_, object_.ptr(), "", 0))) {
      OB_LOG(WARN, "complete an empty multipart upload, but fail to write an empty object",
          KR(ret), K(file_length_), K(bucket_), K(object_));
    }
  } else if (OB_FAIL(ObDalAccessor::obdal_async_multipart_writer_close(
                 async_multipart_writer_))) {
    OB_LOG(WARN, "failed to complete async multipart writer", KR(ret), K(bucket_), K(object_));
  }
  return ret;
}

int ObStorageAsyncMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_FAIL(ObDalAccessor::obdal_async_multipart_writer_abort(async_multipart_writer_))) {
    OB_LOG(WARN, "failed to abort async multipart writer", KR(ret), K(bucket_), K(object_));
  }
  return ret;
}

int ObStorageAsyncMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

void ObStorageAsyncMultiPartWriter::callback(opendal_error *error, int64_t bytes, void *raw_ctx)
{
  ObStorageAsyncLogContextGuard log_context_guard;
  int ret = OB_SUCCESS;
  ObStorageAsyncContext *context = nullptr;
  ObStorageAsyncMultiPartWriter *async_multipart_writer = nullptr;
  if (OB_ISNULL(raw_ctx)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "multipart writer callback ctx is null", KR(ret), KP(raw_ctx));
  } else if (FALSE_IT(context = static_cast<ObStorageAsyncContext *>(raw_ctx))) {
  } else if (OB_UNLIKELY(context->base_ == nullptr || context->callback_ == nullptr)) {
    ret = OB_OBJECT_STORAGE_IO_ERROR;
    OB_LOG(ERROR, "multipart writer callback ctx is invalid, base or callback is null",
           KR(ret), KP(context->base_), KP(context->callback_));
  } else {
    async_multipart_writer = static_cast<ObStorageAsyncMultiPartWriter *>(context->base_);
    if (OB_UNLIKELY(error != nullptr)) {
      handle_obdal_error_and_free(error, ret);
    } else if (FALSE_IT(async_multipart_writer->add_length(bytes))) {
    }

    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(context->callback_(ret, bytes, context->ctx_))) {
      OB_LOG(WARN, "multipart writer callback failed", KR(tmp_ret), KR(ret),
             K(bytes), KP(context->ctx_));
    }
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - context->start_ts_);
  }

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, bytes);
  }
  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);

  if (OB_NOT_NULL(context) && OB_NOT_NULL(context->base_)) {
    context->base_->free_async_context(context);
  }
}

//===================== ObStorageAsyncDirectMultiPartWriter ==========================

ObStorageAsyncDirectMultiPartWriter::ObStorageAsyncDirectMultiPartWriter()
    : ObStorageAsyncMultiPartWriter(), cur_part_id_(-1) {}

ObStorageAsyncDirectMultiPartWriter::~ObStorageAsyncDirectMultiPartWriter()
{
  ObStorageAsyncDirectMultiPartWriter::reset();
}

void ObStorageAsyncDirectMultiPartWriter::reset()
{
  cur_part_id_ = 0;
  ObStorageAsyncMultiPartWriter::reset();
}

int ObStorageAsyncDirectMultiPartWriter::open(const ObString &uri, ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageAsyncMultiPartWriter::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open async multi part writer", KR(ret), K(uri),
           KPC(storage_info));
  } else {
    cur_part_id_ = 0;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::upload_part(
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    ObStorageAsyncCallback cb,
    void *ctx)
{
  int ret = OB_SUCCESS;
  int64_t obdal_part_id = part_id;
  if (storage_type_ == OB_STORAGE_S3 || storage_type_ == OB_STORAGE_OSS) {
    obdal_part_id -= 1;
  }

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async direct multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_UNLIKELY(part_id < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(part_id));
  } else if (OB_FAIL(ObStorageAsyncMultiPartWriter::upload_part(buf, size, obdal_part_id, cb, ctx))) {
    OB_LOG(WARN, "failed to spawn async upload part", KR(ret), K(part_id), K(size), K(bucket_), K(object_));
  }
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::complete()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async direct multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_FAIL(ObStorageAsyncMultiPartWriter::complete())) {
    OB_LOG(WARN, "failed to complete async multi part writer", KR(ret),
           K(bucket_), K(object_));
  }
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::abort()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async direct multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_FAIL(ObStorageAsyncMultiPartWriter::abort())) {
    OB_LOG(WARN, "failed to abort async multi part writer", KR(ret), K(bucket_), K(object_));
  }
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::close()
{
  int ret = OB_SUCCESS;
  reset();
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::buf_append_part(
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id,
    bool &is_full)
{
  UNUSED(buf);
  UNUSED(size);
  UNUSED(tenant_id);

  int ret = OB_SUCCESS;
  is_full = false;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async direct multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else {
    is_full = true;
  }
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::get_part_id(bool &is_exist, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  part_id = -1;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async direct multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else {
    is_exist = true;
    part_id = ATOMIC_AAF(&cur_part_id_, 1);
  }
  return ret;
}

int ObStorageAsyncDirectMultiPartWriter::get_part_size(
    const int64_t part_id,
    int64_t &part_size) const
{
  int ret = OB_NOT_SUPPORTED;
  OB_LOG(WARN, "async direct multi part writer do not support get part size", KR(ret));
  return ret;
}

//===================== ObStorageAsyncBufferedMultiPartWriter ==========================

ObStorageAsyncBufferedMultiPartWriter::ObStorageAsyncBufferedMultiPartWriter()
  : ObStorageAsyncDirectMultiPartWriter(),
    lock_(common::ObLatchIds::OBJECT_DEVICE_LOCK),
    cur_buf_(nullptr),
    cur_buf_pos_(0),
    part_id_to_data_map_()
{}

ObStorageAsyncBufferedMultiPartWriter::~ObStorageAsyncBufferedMultiPartWriter()
{
  ObStorageAsyncBufferedMultiPartWriter::reset();
}

void ObStorageAsyncBufferedMultiPartWriter::reset()
{
  if (OB_NOT_NULL(cur_buf_)) {
    ob_free(cur_buf_);
    cur_buf_ = nullptr;
  }
  cur_buf_pos_ = 0;
  hash::ObHashMap<int64_t, PartData>::iterator iter =
      part_id_to_data_map_.begin();
  while (iter != part_id_to_data_map_.end()) {
    free_part_data_(iter->second);
    iter++;
  }
  part_id_to_data_map_.destroy();
  ObStorageAsyncDirectMultiPartWriter::reset();
}

int ObStorageAsyncBufferedMultiPartWriter::open(
    const ObString &uri,
    ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObStorageAsyncDirectMultiPartWriter::open(uri, storage_info))) {
    OB_LOG(WARN, "failed to open async direct multi part writer", KR(ret), K(uri), KPC(storage_info));
  } else if (OB_FAIL(part_id_to_data_map_.create(7, ALLOC_TAG))) {
    OB_LOG(WARN, "failed to create part_id_to_data_map_", KR(ret), K(uri), KPC(storage_info));
  } else {
    cur_buf_ = nullptr;
    cur_buf_pos_ = 0;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObStorageAsyncBufferedMultiPartWriter::upload_part(
    const char *buf,
    const int64_t size,
    const int64_t part_id,
    ObStorageAsyncCallback cb,
    void *ctx)
{
  UNUSED(buf);
  UNUSED(size);

  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async buffered multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else {
    PartData para_data;
    {
      SpinWLockGuard guard(lock_);
      if (OB_FAIL(part_id_to_data_map_.erase_refactored(part_id, &para_data))) {
        OB_LOG(WARN, "fail to get part data from map", KR(ret), K(part_id));
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!para_data.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "unexpected error, part data is null or invalid",
          KR(ret), K(part_id), K(para_data));
    } else if (OB_FAIL(ObStorageAsyncDirectMultiPartWriter::upload_part(para_data.data_,
                                                                        para_data.size_,
                                                                        part_id,
                                                                        cb,
                                                                        ctx))) {
      OB_LOG(WARN, "fail to upload specified part", KR(ret), K(part_id), K(para_data));
    }

    // obdal will copy the data to owned before enter the async environment
    // so we can free the data here
    free_part_data_(para_data);
  }
  return ret;
}

int ObStorageAsyncBufferedMultiPartWriter::buf_append_part(
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id,
    bool &is_full)
{
  int ret = OB_SUCCESS;
  is_full = false;

  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async buffered multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(buf), K(size), K(tenant_id));
  } else {
    SpinWLockGuard guard(lock_);
    if (OB_FAIL(append_buf_(buf, size, tenant_id))) {
      OB_LOG(WARN, "fail to append data into cur buf",
          KR(ret), K(cur_buf_pos_), K(size), K(tenant_id));
    } else {
      is_full = (cur_buf_pos_ >= PART_SIZE_THRESHOLD);
    }
  }
  return ret;
}

int ObStorageAsyncBufferedMultiPartWriter::get_part_id(bool &is_exist, int64_t &part_id)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  part_id = -1;
  SpinWLockGuard guard(lock_);
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async buffered multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_NOT_NULL(cur_buf_)) {
    if (OB_FAIL(save_buf_to_map_())) {
      OB_LOG(WARN, "fail to save cur buf to map", KR(ret), K(cur_buf_pos_));
    } else {
      is_exist = true;
      part_id = cur_part_id_;
    }
  }
  return ret;
}

int ObStorageAsyncBufferedMultiPartWriter::get_part_size(const int64_t part_id, int64_t &part_size) const
{
  int ret = OB_SUCCESS;
  part_size = 0;
  if (OB_UNLIKELY(!is_opened_)) {
    ret = OB_NOT_OPEN;
    OB_LOG(WARN, "async buffered multi part writer is not opened", KR(ret), K(bucket_), K(object_));
  } else if (OB_UNLIKELY(part_id <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(part_id));
  } else {
    SpinWLockGuard guard(lock_);
    PartData part_data;
    if (OB_FAIL(part_id_to_data_map_.get_refactored(part_id, part_data))) {
      if (ret == OB_HASH_NOT_EXIST) {
        ret = OB_INVALID_ARGUMENT;
        OB_LOG(WARN, "the specified part does not exist", KR(ret), K(part_id));
      }
    } else {
      part_size = part_data.size_;
    }
  }
  return ret;
}

int ObStorageAsyncBufferedMultiPartWriter::append_buf_(
    const char *buf,
    const int64_t size,
    const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = nullptr;
  int64_t final_size = cur_buf_pos_ + size;
  ObMemAttr attr(tenant_id, ALLOC_TAG);

  if (OB_ISNULL(buf) || OB_UNLIKELY(size <= 0 || !is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid arguments", KR(ret), KP(buf), K(size), K(tenant_id));
  } else if (OB_ISNULL(tmp_buf = static_cast<char *>(ob_malloc(final_size, attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "fail to realloc buf",
        KR(ret), K_(cur_buf_pos), K(size), K(final_size), K(tenant_id));
  } else {
    MEMCPY(tmp_buf + cur_buf_pos_, buf, size);
    if (OB_NOT_NULL(cur_buf_)) {
      MEMCPY(tmp_buf, cur_buf_, cur_buf_pos_);
      ob_free(cur_buf_);
      cur_buf_ = nullptr;
    }
    cur_buf_ = tmp_buf;
    cur_buf_pos_ = final_size;
  }
  return ret;
}

int ObStorageAsyncBufferedMultiPartWriter::save_buf_to_map_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_buf_) || OB_UNLIKELY(cur_buf_pos_ <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    OB_LOG(WARN, "cur buf is empty, cannot insert into map",
        KR(ret), KP_(cur_buf), K_(cur_buf_pos));
  } else {
    cur_part_id_++;
    PartData part_data;
    part_data.data_ = cur_buf_;
    part_data.size_ = cur_buf_pos_;
    if (OB_FAIL(part_id_to_data_map_.set_refactored(cur_part_id_, part_data))) {
      OB_LOG(WARN, "fail to insert part data into map",
          KR(ret), K_(cur_part_id), K_(cur_buf_pos));
    } else {
      cur_buf_pos_ = 0;
      cur_buf_ = nullptr;
    }
  }
  return ret;
}

void ObStorageAsyncBufferedMultiPartWriter::free_part_data_(PartData &part_data)
{
  if (OB_NOT_NULL(part_data.data_)) {
    ob_free(part_data.data_);
    part_data.data_ = nullptr;
  }
}

} // namespace common
} // namespace oceanbase
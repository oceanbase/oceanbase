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

template<class LogEntryType>
ObRemoteLogIterator<LogEntryType>::ObRemoteLogIterator(GetSourceFunc &get_source_func,
    UpdateSourceFunc &update_source_func,
    RefreshStorageInfoFunc &refresh_storage_info_func) :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  id_(),
  start_lsn_(),
  cur_lsn_(),
  cur_scn_(),
  end_lsn_(),
  single_read_size_(0),
  source_guard_(),
  data_buffer_(),
  gen_(NULL),
  buf_(NULL),
  buf_size_(0),
  buffer_pool_(NULL),
  log_ext_handler_(NULL),
  get_source_func_(get_source_func),
  update_source_func_(update_source_func),
  refresh_storage_info_func_(refresh_storage_info_func)
{}

template<class LogEntryType>
ObRemoteLogIterator<LogEntryType>::~ObRemoteLogIterator()
{
  reset();
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::init(const uint64_t tenant_id,
    const ObLSID &id,
    const share::SCN &pre_scn,
    const LSN &start_lsn,
    const LSN &end_lsn,
    archive::LargeBufferPool *buffer_pool,
    logservice::ObLogExternalStorageHandler *log_ext_handler,
    const int64_t single_read_size)
{
  int ret = OB_SUCCESS;
  ObRemoteLogParent *source = NULL;
  const int64_t DEFAULT_BUF_SIZE = 64 * 1024 * 1024L;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRemoteLogIterator already init", K(ret), K(inited_), K(id_));
  } else if (OB_UNLIKELY(NULL == (buffer_pool_ = buffer_pool)
        || OB_INVALID_TENANT_ID == tenant_id
        || ! id.is_valid()
        || ! start_lsn.is_valid()
        || (end_lsn.is_valid() && end_lsn <= start_lsn)
        || NULL == log_ext_handler
        || single_read_size < 2 * 1024 * 1024)) {  // TODO set size
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), KP(buffer_pool),
        K(tenant_id), K(id), K(start_lsn), K(end_lsn), KP(log_ext_handler));
  } else if (OB_FAIL(get_source_func_(id, source_guard_))) {
    CLOG_LOG(WARN, "get source failed", K(ret), K(id));
  } else if (OB_ISNULL(source = source_guard_.get_source())) {
    ret = OB_EAGAIN;
    CLOG_LOG(WARN, "source is NULL", K(ret), K(id));
  } else if (OB_UNLIKELY(! share::is_location_log_source_type(source->get_source_type())
        && ! share::is_raw_path_log_source_type(source->get_source_type()))) {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "source type not support", K(ret), K(id), KPC(source));
  } else if (OB_ISNULL(buf_ = buffer_pool_->acquire(DEFAULT_BUF_SIZE))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "acquire buf failed", K(ret));
  } else {
    buf_size_ = DEFAULT_BUF_SIZE;
    tenant_id_ = tenant_id;
    id_ = id;
    start_lsn_ = start_lsn;
    end_lsn_ = end_lsn;
    single_read_size_ = single_read_size;
    log_ext_handler_ = log_ext_handler;
    ret = build_data_generator_(pre_scn, source, refresh_storage_info_func_);
    CLOG_LOG(INFO, "ObRemoteLogIterator init", K(ret), K(tenant_id), K(id), K(pre_scn), K(start_lsn), K(end_lsn));
  }

  if (OB_SUCC(ret)) {
    inited_ = true;
  }
  return ret;
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::next(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRemoteLogIterator not init", K(ret), K(inited_));
  } else {
    ret = next_entry_(entry, lsn, buf, buf_size);
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_RESTORE_LOG_FROM_SOURCE_FAILED) OB_SUCCESS;
  }
#endif
  return ret;
}

template<class LogEntryType>
void ObRemoteLogIterator<LogEntryType>::reset()
{
  inited_ = false;
  if (NULL != gen_) {
    MTL_DELETE(RemoteDataGenerator, "ResDataGen", gen_);
    gen_ = NULL;
  }

  if (NULL != buf_ && NULL != buffer_pool_) {
    buffer_pool_->reclaim(buf_);
    buf_ = NULL;
    buf_size_ = 0;
  }

  log_ext_handler_ = NULL;

  id_.reset();
  start_lsn_.reset();
  cur_lsn_.reset();
  cur_scn_.reset();
  end_lsn_.reset();
  single_read_size_ = 0;
  data_buffer_.reset();
  source_guard_.reset();
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::pre_read(bool &empty)
{
  int ret = OB_SUCCESS;
  empty = true;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRemoteLogIterator not init", K(ret), K(inited_));
  } else if (OB_FAIL(prepare_buf_())) {
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    } else {
      CLOG_LOG(WARN, "prepare buffer failed", K(ret), KPC(this));
    }
  } else {
    empty = false;
  }
  return ret;
}

template<class LogEntryType>
void ObRemoteLogIterator<LogEntryType>::update_source_cb()
{
  int ret = OB_SUCCESS;
  if (inited_ && OB_FAIL(update_source_func_(id_, source_guard_.get_source()))) {
    CLOG_LOG(WARN, "update source failed", K(ret), KPC(this));
  }
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::build_data_generator_(const share::SCN &pre_scn,
    ObRemoteLogParent *source,
    RefreshStorageInfoFunc &refresh_storage_info_func)
{
  int ret = OB_SUCCESS;
  const share::ObLogRestoreSourceType &type = source->get_source_type();
  if (is_service_log_source_type(type)) {
    ObRemoteSerivceParent *service_source = static_cast<ObRemoteSerivceParent *>(source);
    ret = OB_NOT_SUPPORTED;
  } else if (is_raw_path_log_source_type(type)) {
    ObRemoteRawPathParent *dest_source = static_cast<ObRemoteRawPathParent *>(source);
    ret = build_dest_data_generator_(pre_scn, dest_source);
  } else if (is_location_log_source_type(type)) {
    ObRemoteLocationParent *location_source = static_cast<ObRemoteLocationParent *>(source);
    ret = build_location_data_generator_(pre_scn, location_source, refresh_storage_info_func);
  } else {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::build_dest_data_generator_(const share::SCN &pre_scn, ObRemoteRawPathParent *source)
{
  int ret = OB_SUCCESS;
  UNUSED(pre_scn);
  logservice::DirArray array;
  share::SCN end_scn;
  int64_t piece_index = 0;
  int64_t min_file_id = 0;
  int64_t max_file_id = 0;
  source->get(array, end_scn);
  source->get_locate_info(piece_index, min_file_id, max_file_id);
  gen_ = MTL_NEW(RawPathDataGenerator, "ResDataGen", tenant_id_, id_, start_lsn_, end_lsn_,
      array, end_scn, piece_index, min_file_id, max_file_id, log_ext_handler_);
  if (OB_ISNULL(gen_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc dest data generator failed", K(ret), KPC(this));
  }
  return ret;
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::build_location_data_generator_(const share::SCN &pre_scn,
    ObRemoteLocationParent *source,
    const std::function<int(share::ObBackupDest &dest)> &refresh_storage_info_func)
{
  int ret = OB_SUCCESS;
  UNUSED(refresh_storage_info_func);
  share::SCN end_scn;
  share::ObBackupDest *dest = NULL;
  ObLogArchivePieceContext *piece_context = NULL;
  source->get(dest, piece_context, end_scn);
  gen_ = MTL_NEW(LocationDataGenerator, "ResDataGen", tenant_id_, pre_scn,
      id_, start_lsn_, end_lsn_, end_scn, dest, piece_context, buf_, buf_size_,
      single_read_size_, log_ext_handler_);
  if (OB_ISNULL(gen_)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    CLOG_LOG(WARN, "alloc location data generator failed", K(ret), KPC(this));
  }
  return ret;
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::next_entry_(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  bool done = false;
  do {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(get_entry_(entry, lsn, buf, buf_size))) {
      if (need_prepare_buf_(ret)) {
        CLOG_LOG(TRACE, "buf not enough, need read data", K(ret), KPC(this));
      } else {
        CLOG_LOG(WARN, "get entry failed", K(ret), KPC(this));
      }
    } else {
      cur_lsn_ = lsn + entry.get_serialize_size();
      cur_scn_ = entry.get_scn();
      if (lsn < start_lsn_) {
        // do nothing
      } else {
        done = true;
      }
    }

    if (need_prepare_buf_(ret)) {
      if (OB_FAIL(prepare_buf_())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "prepare buffer failed", K(ret));
        } else {
          CLOG_LOG(TRACE, "prepare buffer to end", K(ret), KPC(this));
        }
      }
    }
  } while (OB_SUCCESS == ret && ! done);

  if (OB_FAIL(ret) && OB_ITER_END != ret && ! is_io_error(ret)) {
    mark_source_error_(ret);
  }

  if (OB_SUCC(ret)) {
    advance_data_gen_lsn_();
  }
  return ret;
}

template<class LogEntryType>
bool ObRemoteLogIterator<LogEntryType>::need_prepare_buf_(const int ret_code) const
{
  return OB_BUF_NOT_ENOUGH == ret_code || OB_NEED_RETRY == ret_code;
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::prepare_buf_()
{
  int ret = OB_SUCCESS;
  palf::LSN lsn;
  char *buf = NULL;
  int64_t buf_size = 0;
  if (OB_ISNULL(gen_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(gen_->next_buffer(lsn, buf, buf_size))) {
    if (OB_ITER_END != ret) {
      CLOG_LOG(WARN, "next buffer failed", K(ret), KPC(this));
    } else {
      CLOG_LOG(INFO, "next buffer to end", KPC(this));
    }
  } else if (OB_FAIL(data_buffer_.set(lsn, buf, buf_size))) {
    CLOG_LOG(WARN, "data buffer set failed", K(ret), K(lsn), K(buf), K(buf_size), KPC(this));
  } else {
    EVENT_TENANT_ADD(ObStatEventIds::RESTORE_READ_LOG_SIZE, buf_size, tenant_id_);
    CLOG_LOG(INFO, "data buffer init succ", K(ret), K_(data_buffer), KPC(this));
  }
  return ret;
}

template<class LogEntryType>
int ObRemoteLogIterator<LogEntryType>::get_entry_(LogEntryType &entry, LSN &lsn, const char *&buf, int64_t &buf_size)
{
  int ret = OB_SUCCESS;
  if (data_buffer_.is_empty() || ! data_buffer_.is_valid()) {
    ret = OB_BUF_NOT_ENOUGH;
    update_data_gen_max_lsn_();
  } else {
    ret = data_buffer_.next(entry, lsn, buf, buf_size);
  }
  return ret;
}

template<class LogEntryType>
void ObRemoteLogIterator<LogEntryType>::update_data_gen_max_lsn_()
{
  if (NULL != gen_ && cur_lsn_.is_valid()) {
    gen_->update_max_lsn(cur_lsn_);
  }
}

template<class LogEntryType>
void ObRemoteLogIterator<LogEntryType>::advance_data_gen_lsn_()
{
  if (NULL != gen_ && cur_lsn_.is_valid()) {
    gen_->advance_step_lsn(cur_lsn_);
  }
}

template<class LogEntryType>
void ObRemoteLogIterator<LogEntryType>::mark_source_error_(const int ret_code)
{
  int ret = OB_SUCCESS;
  ObRemoteLogParent *source = NULL;
  if (OB_ISNULL(source = source_guard_.get_source())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(WARN, "source is NULL", K(ret), K(ret_code), K(id_));
  } else {
    source->mark_error(*ObCurTraceId::get_trace_id(), ret_code);
  }
}

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

#include "ob_storage.h"
#include "lib/restore/ob_i_storage.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/container/ob_array_iterator.h"
#include "common/ob_smart_var.h"
#include "common/storage/ob_device_common.h"
#include "lib/atomic/ob_atomic.h"

namespace oceanbase
{
namespace common
{

const char *OB_STORAGE_TYPES_STR[] = {"OSS", "FILE", "COS", "LOCAL"};

void print_access_storage_log(
    const char *msg,
    const common::ObString &uri,
    const int64_t start_ts,
    const int64_t size,
    bool *is_slow)
{
  if (NULL != is_slow) {
    *is_slow = false;
  }

  if (NULL != msg) {
    int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
    const int64_t warn_cost_time = 1000 * 1000;
    double speed = 0;
    if (cost_ts > warn_cost_time) {
      if (NULL != is_slow) {
        *is_slow = true;
      }
      speed = 1.0 * (double)size / 1024 / 1024 * 1000 * 1000 / (double)cost_ts;
      _STORAGE_LOG_RET(WARN, OB_SUCCESS, "access storage op=%s uri=%.*s size=%ld Byte cost_ts=%ld us speed=%.2f MB/s",
        msg, uri.length(), uri.ptr(), size, cost_ts, speed);
    }
  }
}

int validate_uri_type(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  if (!uri.prefix_match(OB_OSS_PREFIX) &&
      !uri.prefix_match(OB_COS_PREFIX) &&
      !uri.prefix_match(OB_FILE_PREFIX)) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "invalid backup uri", K(ret), K(uri));
  }
  return ret;  
}

int get_storage_type_from_path(const common::ObString &uri, ObStorageType &type)
{
  int ret = OB_SUCCESS;
  type = OB_STORAGE_MAX_TYPE;

  if (uri.prefix_match(OB_OSS_PREFIX)) {
    type = OB_STORAGE_OSS;
  } else if (uri.prefix_match(OB_COS_PREFIX)) {
    type = OB_STORAGE_COS;
  } else if (uri.prefix_match(OB_FILE_PREFIX)) {
    type = OB_STORAGE_FILE;
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "invlaid backup uri", K(ret), K(uri));
  }
  return ret;
}

const char *get_storage_type_str(const ObStorageType &type)
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(static_cast<int64_t>(OB_STORAGE_MAX_TYPE) == ARRAYSIZEOF(OB_STORAGE_TYPES_STR), "ObStorageType count mismatch");
  if (type >= 0 && type < OB_STORAGE_MAX_TYPE) {
    str = OB_STORAGE_TYPES_STR[type];
  }
  return str;
}

bool is_io_error(const int result)
{
  return OB_IO_ERROR == result || OB_OSS_ERROR == result || OB_COS_ERROR == result;
}

int get_storage_type_from_name(const char *type_str, ObStorageType &type)
{
  int ret = OB_SUCCESS;
  type = ObStorageType::OB_STORAGE_MAX_TYPE;
  const int64_t count = ARRAYSIZEOF(OB_STORAGE_TYPES_STR);
  STATIC_ASSERT(static_cast<int64_t>(ObStorageType::OB_STORAGE_MAX_TYPE) == count, "status count mismatch");
  for (int64_t i = 0; i < count; ++i) {
    if (0 == strcmp(type_str, OB_STORAGE_TYPES_STR[i])) {
      type = static_cast<ObStorageType>(i);
      break;
    }
  }
  return ret;
}

/**
 * ------------------------------ObStorageGlobalIns---------------------
 */
ObStorageGlobalIns::ObStorageGlobalIns() 
  :io_prohibited_(false)
{

}

ObStorageGlobalIns& ObStorageGlobalIns::get_instance()
{
  static ObStorageGlobalIns instance;
  return instance;
}

int ObStorageGlobalIns::init() 
{
  int ret = OB_SUCCESS;

  io_prohibited_ = false;

  return ret;
}
  
void ObStorageGlobalIns::fin()
{

}

void ObStorageGlobalIns::set_io_prohibited(bool prohibited)
{
  STORAGE_LOG_RET(WARN, OB_SUCCESS, "set_io_prohibited", K_(io_prohibited), K(prohibited));
  io_prohibited_ = prohibited;
}

bool ObStorageGlobalIns::is_io_prohibited() const
{
  return io_prohibited_;
}


/**
 * ------------------------------ObExternalIOCounter---------------------
 */
int64_t ObExternalIOCounter::flying_io_cnt_ = 0;

void ObExternalIOCounter::inc_flying_io()
{
  ATOMIC_INC(&ObExternalIOCounter::flying_io_cnt_);
}

void ObExternalIOCounter::dec_flying_io()
{
  ATOMIC_DEC(&ObExternalIOCounter::flying_io_cnt_);
}

int64_t ObExternalIOCounter::get_flying_io_cnt()
{
  return ATOMIC_LOAD(&ObExternalIOCounter::flying_io_cnt_);
}

/**
 * ------------------------------ObExternalIOCounterGuard---------------------
 */
ObExternalIOCounterGuard::ObExternalIOCounterGuard()
{
  io_counter_.inc_flying_io();
}

ObExternalIOCounterGuard::~ObExternalIOCounterGuard()
{
  io_counter_.dec_flying_io();
}


ObStorageUtil::ObStorageUtil()
  : file_util_(),
    oss_util_(),
    cos_util_(),
    util_(NULL),
    storage_info_(NULL),
    init_state(false)
{
}

/*this fun just like the get_util*/
int ObStorageUtil::open(common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObStorageType device_type = OB_STORAGE_MAX_TYPE;

  if (is_init()) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "double init the storage util", K(ret));
  } else if (OB_ISNULL(storage_info) || OB_UNLIKELY(!storage_info->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(storage_info));
  } else if (OB_FALSE_IT(device_type = storage_info->get_type())) {
  } else if (OB_STORAGE_OSS == device_type) {
    util_ = &oss_util_;
  } else if (OB_STORAGE_COS == device_type) {
    util_ = &cos_util_;
  } else if (OB_STORAGE_FILE == device_type) {
    util_ = &file_util_;
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "Invalid device type", K(ret), K(device_type));
  }

  if (OB_SUCC(ret) && NULL != util_) {
    if (OB_FAIL(util_->open(storage_info))) {
      STORAGE_LOG(WARN, "failed to open util", K(ret), K(device_type));
      util_ = NULL;
    } else {
      storage_info_ = storage_info;
      init_state = true;
    }
  }
  return ret;
}

void ObStorageUtil::close()
{
  if (NULL != util_) {
    util_->close();
    util_ = NULL;
    storage_info_ = NULL;
    init_state = false;
  }
}

int ObStorageUtil::is_exist(const common::ObString &uri, bool &exist)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  exist = false;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_IS_EXIST) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
    //do nothing
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->is_exist(uri, exist))) {
    STORAGE_LOG(WARN, "failed to check is exist", K(ret), K(uri));
  }

  print_access_storage_log("is_exist", uri, start_ts);
  return ret;
}

int ObStorageUtil::get_file_length(const common::ObString &uri, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  file_length = -1;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_GET_FILE_LENGTH) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->get_file_length(uri, file_length))) {
    if (OB_BACKUP_FILE_NOT_EXIST == ret) {
      STORAGE_LOG(INFO, "cannot get file length for not exist file", K(ret), K(uri));
    } else {
      STORAGE_LOG(WARN, "failed to get_file_length", K(ret), K(uri));
    }
  }
    
  if (file_length <= 0) {
    STORAGE_LOG(INFO, "this file is empty", K(ret), K(uri), K(file_length));
  }
  print_access_storage_log("get_file_length", uri, start_ts);
  return ret;
}

int ObStorageUtil::del_file(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_DEL_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else {
    const int max_try_cnt = 5;
    int try_cnt = 0;
    while (try_cnt < max_try_cnt) {
      if (OB_FAIL(util_->del_file(uri))) {
        try_cnt++;
        OB_LOG(WARN, "failed to delete file", K(ret), K(try_cnt), K(uri));
      } else {
        break;
      }
    }

    if (OB_SUCC(ret)) {
      OB_LOG(DEBUG, "succeed to delete file", K(ret), K(uri));
    } else {
      OB_LOG(WARN, "failed to delete file", K(ret), K(uri));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_DEL_FILE) OB_SUCCESS;
  }
#endif
  print_access_storage_log("del_file", uri, start_ts);

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_DEL_FAIL_COUNT);
  }
  EVENT_INC(ObStatEventIds::BACKUP_DELETE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_DELETE_DELAY, ObTimeUtility::current_time() - start_ts);
  return ret;
}

int ObStorageUtil::mkdir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  STORAGE_LOG(DEBUG, "mkdir", K(uri));
#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_MKDIR) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->mkdir(uri))) {
    STORAGE_LOG(WARN, "failed to mkdir", K(ret), K(uri));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_MKDIR) OB_SUCCESS;
  }
#endif
  print_access_storage_log("mkdir", uri, start_ts);
  return ret;
}

int ObStorageUtil::list_files(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->list_files(uri, op))) {
    STORAGE_LOG(WARN, "failed to list_files", K(ret), K(uri));
  }

  print_access_storage_log("list_files", uri, start_ts, 0);
  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_LS_FAIL_COUNT);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_LS_COUNT);

  return ret;
}

int ObStorageUtil::write_single_file(const common::ObString &uri, const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_WRITE_SINGLE_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->write_single_file(uri, buf, size))) {
        STORAGE_LOG(WARN, "failed to write single file", K(ret), K(uri));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = OB_E(EventTable::EN_BACKUP_IO_AFTER_WRITE_SINGLE_FILE) OB_SUCCESS;
  }
#endif
  print_access_storage_log("write_single_file", uri, start_ts, size);

  if (OB_FAIL(ret)) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageUtil::del_dir(const common::ObString &uri)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

  if (OB_FAIL(ret)) {
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->del_dir(uri))) {
    STORAGE_LOG(WARN, "failed to del_file", K(ret), K(uri));
  }
  print_access_storage_log("del_file", uri, start_ts);
  return ret;
}

int ObStorageUtil::list_directories(const common::ObString &uri, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (!is_init()) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "util is not inited", K(ret), K(uri));
  } else if (OB_FAIL(util_->list_directories(uri, op))) {
    STORAGE_LOG(WARN, "failed to list_files", K(ret),  K(uri));
  } 
  print_access_storage_log("list_files", uri, start_ts, 0);
  return ret;
}


int ObStorageUtil::is_tagging(const common::ObString &uri, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  is_tagging = false;
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (OB_FAIL(validate_uri_type(uri))) {
    STORAGE_LOG(WARN, "fail to validate uri!", K(uri));
  } else if (OB_FAIL(util_->is_tagging(uri, is_tagging))) {
    STORAGE_LOG(WARN, "failed to check is tagging", K(ret), K(uri));
  }
  print_access_storage_log("is_tagging", uri, start_ts, 0);
  return ret;
}

ObStorageReader::ObStorageReader()
  : file_length_(-1),
    reader_(NULL),
    file_reader_(),
    oss_reader_(),
    cos_reader_(),
    start_ts_(0)
{
  uri_[0] = '\0';
}

ObStorageReader::~ObStorageReader()
{
  if (NULL != reader_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "reader not closed", KCSTRING(uri_));
  }
}

int ObStorageReader::open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_READER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != reader_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  } else if (OB_STORAGE_OSS == type) {
    reader_ = &oss_reader_;
  } else if (OB_STORAGE_COS == type) {
    reader_ = &cos_reader_;
  } else if (OB_STORAGE_FILE == type) {
    reader_ = &file_reader_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(reader_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "reader_ is null", K(ret), K(uri));
    } else if (OB_FAIL(reader_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open reader", K(ret), K(uri));
    } else {
      file_length_ = reader_->get_length();
    }
  }

  if(OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close read file", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageReader::pread(char *buf,const int64_t buf_size, int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_READER_PREAD) OB_SUCCESS;
#endif

  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(offset), K(buf_size));
  } else if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(buf) || offset < 0 || offset > file_length_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(offset), K(file_length_));
  } else if (OB_FAIL(reader_->pread(buf, buf_size, offset, read_size))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_READ_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to read file", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_BYTES, read_size);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_READ_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_READ_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageReader::close()
{
  int ret = OB_SUCCESS;

  if (NULL != reader_) {
    print_access_storage_log("storage reader", uri_, start_ts_, file_length_);
  }

  if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(reader_->close())) {
    STORAGE_LOG(WARN, "failed to close reader", K(ret));
  }
  reader_  = NULL;
  file_length_ = -1;
  start_ts_ = 0;
  uri_[0] = '\0';
  return ret;
}


ObStorageWriter::ObStorageWriter()
  : writer_(NULL),
    file_writer_(),
    oss_writer_(),
    cos_writer_(),
    start_ts_(0)
{
    uri_[0] = '\0';
}

ObStorageWriter::~ObStorageWriter()
{
  if (NULL != writer_) {
    STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "writer not close");
  }
}

int ObStorageWriter::open(const common::ObString &uri, common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_WRITE_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != writer_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  } else if (OB_STORAGE_OSS == type) {
    writer_ = &oss_writer_;
  } else if (OB_STORAGE_COS == type) {
    writer_ = &cos_writer_;
  } else if (OB_STORAGE_FILE == type) {
    writer_ = &file_writer_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(writer_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "writer_ is null", K(ret), K(uri));
    } else if (OB_FAIL(writer_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close write file", K(ret), K(tmp_ret), K(uri));
    }
  }
  return ret;

}

int ObStorageWriter::write(const char *buf,const int64_t size)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_WRITE_WRITE) OB_SUCCESS;
#endif

  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(size));
  } else if (OB_ISNULL(writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if(OB_FAIL(writer_->write(buf, size))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to write", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageWriter::close()
{
  int ret = OB_SUCCESS;

  if (NULL != writer_) {
    print_access_storage_log("storage writer", uri_, start_ts_, writer_->get_length());
  }

  if (OB_ISNULL(writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(writer_->close())) {
    STORAGE_LOG(WARN, "failed to close writer", K(ret));
  }
  writer_  = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  return ret;
}

ObStorageAppender::ObStorageAppender()
  : appender_(NULL),
    file_appender_(),
    oss_appender_(),
    cos_appender_(),
    start_ts_(0),
    is_opened_(false),
    storage_info_()
{
    uri_[0] = '\0';
}

ObStorageAppender::ObStorageAppender(StorageOpenMode mode)
  : appender_(NULL),
    file_appender_(mode),
    oss_appender_(),
    cos_appender_(),
    start_ts_(0),
    is_opened_(false),
    storage_info_()
{
    uri_[0] = '\0';
}

ObStorageAppender::~ObStorageAppender()
{
  if (is_opened_ && NULL != appender_) {
    STORAGE_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "appender not close");
  }
}

int ObStorageAppender::open(
    const common::ObString &uri, 
    common::ObObjectStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_APPENDER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(uri));
  } else if (NULL != appender_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_UNLIKELY(uri.empty()) || OB_ISNULL(storage_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  } else if (OB_STORAGE_OSS == type || OB_STORAGE_COS == type) {
    if (OB_FAIL(storage_info_.assign(*storage_info))) {
      STORAGE_LOG(WARN, "failed to copy storage info", K(ret));
    } else if (OB_STORAGE_OSS == type) {
      appender_ = &oss_appender_;
    } else if (OB_STORAGE_COS == type) {
      appender_ = &cos_appender_;
    }
  } else if (OB_STORAGE_FILE == type) {
    appender_ = &file_appender_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_ISNULL(appender_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "appender_ is null", K(ret), K(uri));
    } else if (OB_FAIL(appender_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri));
    } else {
      is_opened_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close write file", K(ret), K(tmp_ret), K(uri));
    }
  }
  return ret;
}

int ObStorageAppender::write(const char *buf,const int64_t size)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_APPENDER_WRITE) OB_SUCCESS;
#endif

  const int64_t start_ts = ObTimeUtility::current_time();
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(size));
  } else if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if(OB_FAIL(appender_->write(buf, size))) {
    EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_FAIL_COUNT);
    STORAGE_LOG(WARN, "failed to write", K(ret));
  } else {
    EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_BYTES, size);
  }

  EVENT_INC(ObStatEventIds::BACKUP_IO_WRITE_COUNT);
  EVENT_ADD(ObStatEventIds::BACKUP_IO_WRITE_DELAY, ObTimeUtility::current_time() - start_ts);

  return ret;
}

int ObStorageAppender::repeatable_pwrite_(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  int64_t read_size = 0;
  int64_t read_buf_size = 0;
  int64_t actual_write_offset = 0;
  char *read_buffer = nullptr;
  ObStorageReader reader;
  ObArenaAllocator allocator;

  if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(reader.open(uri_, &storage_info_))) {
    STORAGE_LOG(WARN, "failed to open reader", K(ret));
  } else if (reader.get_length() <= offset) {
    // This situation also has concurrency issues.
    // The length read by the reader may be old, so offset not match needs to be returned for retry.
    ret = OB_BACKUP_PWRITE_OFFSET_NOT_MATCH;
    STORAGE_LOG(WARN, "offset is invalid", K(offset), "length", reader.get_length(), K(ret));
  } else if (OB_FALSE_IT(actual_write_offset = reader.get_length() - offset)) {
  } else if (OB_FALSE_IT(read_buf_size = std::min(actual_write_offset, size))) {
  } else if (OB_ISNULL(read_buffer = static_cast<char *>(allocator.alloc(read_buf_size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    OB_LOG(WARN, "failed to allocate memory", K(ret), K(size));
  } else if (OB_FAIL(reader.pread(read_buffer, read_buf_size, offset, read_size))) {
    STORAGE_LOG(WARN, "failed to pread", K(ret));
  } else if (0 != MEMCMP(buf, read_buffer, read_buf_size)) {
    ret = OB_BACKUP_PWRITE_CONTENT_NOT_MATCH;
    STORAGE_LOG(WARN, "data inconsistent", K(ret));
  } else if (offset + size > reader.get_length()) {
    if (OB_FAIL(appender_->pwrite(buf + actual_write_offset, size - actual_write_offset, reader.get_length()))) {
      if (OB_BACKUP_PWRITE_OFFSET_NOT_MATCH == ret) {
        ret = OB_IO_ERROR;
        STORAGE_LOG(WARN, "There may be concurrency problems that require the caller to retry", K(ret));
      }
    }
  }

  if (OB_SUCCESS != (tmp_ret = reader.close())) {
    STORAGE_LOG(WARN, "failed to close reader", K(tmp_ret));
  }

  return ret;
}

int ObStorageAppender::pwrite(const char *buf, const int64_t size, const int64_t offset)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_APPENDER_WRITE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (ObStorageGlobalIns::get_instance().is_io_prohibited()) {
    ret = OB_BACKUP_IO_PROHIBITED;
    STORAGE_LOG(WARN, "current observer backup io is prohibited", K(ret), K(offset), K(size));
  } else if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(appender_->pwrite(buf, size, offset))) {
    STORAGE_LOG(WARN, "failed to write", K(ret));
  }

  if (OB_BACKUP_PWRITE_OFFSET_NOT_MATCH == ret) {
    if (OB_FAIL(repeatable_pwrite_(buf, size, offset))) {
      STORAGE_LOG(WARN, "failed to repeatable_pwrite", K(ret));
    } else {
      STORAGE_LOG(DEBUG, "repeatable pwrite success", K(ret));
    }
  }

  return ret;
}

int64_t ObStorageAppender::get_length()
{
  int64_t ret_int = -1;

  if (OB_ISNULL(appender_)) {
    STORAGE_LOG_RET(WARN, common::OB_ERR_UNEXPECTED, "appender not opened");
  } else {
    ret_int = appender_->get_length();
  }
  return ret_int;
}

int ObStorageAppender::close()
{
  int ret = OB_SUCCESS;

  if (NULL != appender_) {
    print_access_storage_log("storage appender_", uri_, start_ts_, appender_->get_length());
  }

  if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(appender_->close())) {
    STORAGE_LOG(WARN, "failed to close writer", K(ret));
  }
  appender_  = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  is_opened_ = false;
  return ret;
}


}
}//oceanbase

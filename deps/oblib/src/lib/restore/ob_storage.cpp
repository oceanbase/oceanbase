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

namespace oceanbase {
namespace common {

const char* OB_STORAGE_TYPES_STR[] = {"OSS", "FILE", "COS"};

void print_access_storage_log(
    const char* msg, const common::ObString& uri, const int64_t start_ts, const int64_t size, bool* is_slow)
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
      _STORAGE_LOG(WARN,
          "access storage op=%s uri=%.*s size=%ld Byte cost_ts=%ld us speed=%.2f MB/s",
          msg,
          uri.length(),
          uri.ptr(),
          size,
          cost_ts,
          speed);
    }
  }
}

int get_storage_type_from_path(const common::ObString& uri, ObStorageType& type)
{
  int ret = OB_SUCCESS;
  type = OB_STORAGE_MAX_TYPE;

  if (uri.prefix_match(OB_OSS_PREFIX)) {
    type = OB_STORAGE_OSS;
  } else if (uri.prefix_match(OB_FILE_PREFIX)) {
    type = OB_STORAGE_FILE;
  } else if (uri.prefix_match(OB_COS_PREFIX)) {
    type = OB_STORAGE_COS;
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG(ERROR, "invlaid backup uri", K(ret), K(uri));
  }
  return ret;
}

const char* get_storage_type_str(const ObStorageType& type)
{
  const char* str = "UNKNOWN";
  STATIC_ASSERT(
      static_cast<int64_t>(OB_STORAGE_MAX_TYPE) == ARRAYSIZEOF(OB_STORAGE_TYPES_STR), "ObStorageType count mismatch");
  if (type >= 0 && type < OB_STORAGE_MAX_TYPE) {
    str = OB_STORAGE_TYPES_STR[type];
  }
  return str;
}

int get_storage_type_from_name(const char* type_str, ObStorageType& type)
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

ObStorageUtil::ObStorageUtil(const bool need_retry, const int64_t max_retry_duraion_us, const uint32_t retry_sleep_us)
    : file_util_(), max_retry_duraion_us_(max_retry_duraion_us), retry_sleep_us_(retry_sleep_us)
{
  if (!need_retry) {
    max_retry_duraion_us_ = 0;
  }
}

int ObStorageUtil::is_exist(const common::ObString& uri, const common::ObString& storage_info, bool& exist)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  exist = false;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_IS_EXIST) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else if (OB_FAIL(util->is_exist(uri, storage_info, exist))) {
    STORAGE_LOG(WARN, "failed to check is exist", K(ret), K(uri));
  }

  print_access_storage_log("is_exist", uri, start_ts);
  return ret;
}

int ObStorageUtil::get_file_length(
    const common::ObString& uri, const common::ObString& storage_info, int64_t& file_length)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  file_length = -1;
  int64_t retry_times = 0;
  bool need_retry = true;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_GET_FILE_LENGTH) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && need_retry) {
      need_retry = false;
      if (OB_FAIL(util->get_file_length(uri, storage_info, file_length))) {
        if (OB_BACKUP_FILE_NOT_EXIST == ret) {
          need_retry = false;
          STORAGE_LOG(INFO, "cannot get file length for not exist file", K(ret), K(uri));
        } else {
          const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
          STORAGE_LOG(
              WARN, "failed to get_file_length", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
          if (cost_ts < max_retry_duraion_us_) {
            usleep(retry_sleep_us_);
            ++retry_times;
            need_retry = true;
            ret = OB_SUCCESS;
          }
        }
      }
    }
  }

  if (file_length <= 0) {
    STORAGE_LOG(INFO, "this file is empty", K(ret), K(uri), K(storage_info), K(file_length));
  }
  print_access_storage_log("get_file_length", uri, start_ts);
  return ret;
}

int ObStorageUtil::del_file(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_BEFORE_DEL_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    const int max_try_cnt = 5;
    int try_cnt = 0;
    while (try_cnt < max_try_cnt) {
      if (OB_FAIL(util->del_file(uri, storage_info))) {
        try_cnt++;
        OB_LOG(WARN, "failed to delete file", K(ret), K(try_cnt), K(uri), K(storage_info));
      } else {
        break;
      }
    }

    if (OB_SUCC(ret)) {
      OB_LOG(DEBUG, "succeed to delete file", K(ret), K(uri), K(storage_info));
    } else {
      OB_LOG(WARN, "failed to delete file", K(ret), K(uri), K(storage_info));
    }
  }

#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BACKUP_IO_AFTER_DEL_FILE) OB_SUCCESS;
  }
#endif
  print_access_storage_log("del_file", uri, start_ts);
  return ret;
}

int ObStorageUtil::mkdir(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;

  STORAGE_LOG(DEBUG, "mkdir", K(uri));
#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_BEFORE_MKDIR) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else if (OB_FAIL(util->mkdir(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to mkdir", K(ret), K(uri), K(storage_info));
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BACKUP_IO_AFTER_MKDIR) OB_SUCCESS;
  }
#endif
  print_access_storage_log("mkdir", uri, start_ts);
  return ret;
}

int ObStorageUtil::mk_parent_dir(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];

  STORAGE_LOG(INFO, "mk_parent_dir", K(uri));
  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invlaid args", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(path, sizeof(path), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill path", K(ret), K(path));
  } else if (path[strlen(path) - 1] == '/') {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "cannot mk parent dir for dir", K(ret), K(path));
  } else {
    bool found = false;
    for (int64_t i = strlen(path) - 1; i >= 0; --i) {
      if (path[i] == '/') {
        path[i] = '\0';
        found = true;
        STORAGE_LOG(INFO, "found parent dir", K(i), K(path));
        break;
      }
    }

    if (!found) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "no dir found in uri", K(ret), K(uri));
    } else if (OB_FAIL(mkdir(path, storage_info))) {
      STORAGE_LOG(WARN, "failed to mkdir", K(ret), K(uri));
    }
  }
  return ret;
}

int ObStorageUtil::read_single_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
    const int64_t buf_size, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_times = 0;
  bool need_retry = true;

  while (OB_SUCC(ret) && need_retry) {
    need_retry = false;
    if (OB_FAIL(do_read_single_file(uri, storage_info, buf, buf_size, read_size))) {
      const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      STORAGE_LOG(
          WARN, "failed to do_read_single_file", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
      if (cost_ts < max_retry_duraion_us_) {
        usleep(retry_sleep_us_);
        ++retry_times;
        need_retry = true;
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObStorageUtil::do_read_single_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
    const int64_t buf_size, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageReader reader;

  read_size = -1;

  if (OB_FAIL(reader.open(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to open reader", K(ret), K(uri));
  } else {
    if (OB_FAIL(reader.pread(buf, buf_size, 0, read_size))) {
      STORAGE_LOG(WARN, "failed to read", K(ret));
    } else if (read_size != reader.get_length()) {
      ret = OB_BUF_NOT_ENOUGH;
      STORAGE_LOG(
          WARN, "not whole file read, maybe buf not enough", K(ret), K(read_size), K(reader.get_length()), K(uri));
    }

    if (OB_SUCCESS != (tmp_ret = reader.close())) {
      STORAGE_LOG(WARN, "failed to close reader", K(tmp_ret), K(uri));
    }
  }

  return ret;
}

// has '\0' in the end
int ObStorageUtil::read_single_text_file(
    const common::ObString& uri, const common::ObString& storage_info, char* buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = -1;

  if (OB_FAIL(ObStorageUtil::read_single_file(uri, storage_info, buf, buf_size, read_size))) {
    STORAGE_LOG(WARN, "failed to read_single_object", K(ret), K(uri));
  } else if (read_size < 0 || read_size >= buf_size) {
    ret = OB_BUF_NOT_ENOUGH;
    STORAGE_LOG(WARN, "buf not enough", K(ret), K(read_size), K(buf_size));
  } else {
    buf[read_size] = '\0';
  }

  return ret;
}

int ObStorageUtil::update_file_modify_time(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  int64_t retry_times = 0;
  bool need_retry = true;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_UPDATE_FILE_MODIFY_TIME) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && need_retry) {
      need_retry = false;
      if (OB_FAIL(util->update_file_modify_time(uri, storage_info))) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        STORAGE_LOG(WARN,
            "failed to update_file_modify_time",
            K(ret),
            K(cost_ts),
            K(retry_times),
            K_(max_retry_duraion_us),
            K(uri));
        if (cost_ts < max_retry_duraion_us_) {
          usleep(retry_sleep_us_);
          ++retry_times;
          need_retry = true;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  print_access_storage_log("update_file_modify_time", uri, start_ts, 0);
  return ret;
}

int ObStorageUtil::list_files(const common::ObString& uri, const common::ObString& storage_info,
    common::ObIAllocator& allocator, common::ObIArray<common::ObString>& file_names)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  int64_t retry_times = 0;
  bool need_retry = true;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && need_retry) {
      need_retry = false;
      if (OB_FAIL(util->list_files(uri, storage_info, allocator, file_names))) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        STORAGE_LOG(WARN, "failed to list_files", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
        if (cost_ts < max_retry_duraion_us_) {
          usleep(retry_sleep_us_);
          ++retry_times;
          need_retry = true;
          ret = OB_SUCCESS;
        }
      }
    }
  }
  print_access_storage_log("list_files", uri, start_ts, 0);
  return ret;
}

int ObStorageUtil::write_single_file(
    const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  int64_t retry_times = 0;
  bool need_retry = true;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_BEFORE_WRITE_SINGLE_FILE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && need_retry) {
      need_retry = false;
      if (OB_FAIL(util->write_single_file(uri, storage_info, buf, size))) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        STORAGE_LOG(
            WARN, "failed to write single file", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
        if (cost_ts < max_retry_duraion_us_) {
          usleep(retry_sleep_us_);
          ++retry_times;
          need_retry = true;
          ret = OB_SUCCESS;
        }
      }
    }
  }
#ifdef ERRSIM
  if (OB_SUCC(ret)) {
    ret = E(EventTable::EN_BACKUP_IO_AFTER_WRITE_SINGLE_FILE) OB_SUCCESS;
  }
#endif
  print_access_storage_log("write_single_file", uri, start_ts, size);
  return ret;
}

int ObStorageUtil::get_util(const common::ObString& uri, ObIStorageUtil*& util)
{
  int ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  util = NULL;

  if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get storage type", K(ret));
  }
#ifdef _WITH_OSS
  else if (OB_STORAGE_OSS == type) {
    util = &oss_util_;
  }
#endif
  else if (OB_STORAGE_FILE == type) {
    util = &file_util_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unknown storage type", K(ret), K(uri));
  }

  return ret;
}

int ObStorageUtil::read_part_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
    const int64_t buf_size, const int64_t offset, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t retry_times = 0;
  bool need_retry = true;
  read_size = 0;

  while (OB_SUCC(ret) && need_retry) {
    need_retry = false;
    if (OB_FAIL(do_read_part_file(uri, storage_info, buf, buf_size, offset, read_size))) {
      const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
      STORAGE_LOG(WARN, "failed to do_dump_file", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
      if (cost_ts < max_retry_duraion_us_) {
        usleep(retry_sleep_us_);
        ++retry_times;
        need_retry = true;
        ret = OB_SUCCESS;
      }
    }
  }

  return ret;
}

int ObStorageUtil::do_read_part_file(const common::ObString& uri, const common::ObString& storage_info, char* buf,
    const int64_t buf_size, const int64_t offset, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageReader reader;

  read_size = -1;

  if (OB_FAIL(reader.open(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to open reader", K(ret), K(uri));
  } else {
    if (OB_FAIL(reader.pread(buf, buf_size, offset, read_size))) {
      STORAGE_LOG(WARN, "failed to read", K(ret));
    }
    if (OB_SUCCESS != (tmp_ret = reader.close())) {
      STORAGE_LOG(WARN, "failed to close reader", K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageUtil::del_dir(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else if (OB_FAIL(util->del_dir(uri, storage_info))) {
    STORAGE_LOG(WARN, "failed to del_file", K(ret), K(uri), K(storage_info));
  }

  print_access_storage_log("del_file", uri, start_ts);
  return ret;
}

int ObStorageUtil::get_pkeys_from_dir(
    const common::ObString& uri, const common::ObString& storage_info, common::ObIArray<common::ObPartitionKey>& pkeys)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  int64_t retry_times = 0;
  bool need_retry = true;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && need_retry) {
      need_retry = false;
      if (OB_FAIL(util->get_pkeys_from_dir(uri, storage_info, pkeys))) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        STORAGE_LOG(WARN, "failed to list_files", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
        if (cost_ts < max_retry_duraion_us_) {
          usleep(retry_sleep_us_);
          ++retry_times;
          need_retry = true;
          ret = OB_SUCCESS;
        }
      }
    }
  }

  print_access_storage_log("get_pkeys_from_dir", uri, start_ts, 0);
  return ret;
}

int ObStorageUtil::delete_tmp_files(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  const int64_t start_ts = ObTimeUtility::current_time();
  ObIStorageUtil* util = NULL;
  int64_t retry_times = 0;
  bool need_retry = true;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_LIST_FILE) OB_SUCCESS;
#endif

  if (OB_FAIL(ret)) {
  } else if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "delete tmp files get invalid argument", K(ret), K(uri));
  } else if (OB_FAIL(get_util(uri, util))) {
    STORAGE_LOG(WARN, "failed to get util", K(ret), K(uri));
  } else {
    while (OB_SUCC(ret) && need_retry) {
      need_retry = false;
      if (OB_FAIL(util->delete_tmp_files(uri, storage_info))) {
        const int64_t cost_ts = ObTimeUtility::current_time() - start_ts;
        STORAGE_LOG(
            WARN, "failed to delete tmp files", K(ret), K(cost_ts), K(retry_times), K_(max_retry_duraion_us), K(uri));
        if (cost_ts < max_retry_duraion_us_) {
          usleep(retry_sleep_us_);
          ++retry_times;
          need_retry = true;
          ret = OB_SUCCESS;
        }
      }
    }
  }

  print_access_storage_log("delete_tmp_files", uri, start_ts, 0);
  return ret;
}

ObStorageReader::ObStorageReader() : file_length_(-1), reader_(NULL), file_reader_(), start_ts_(0)
{
  uri_[0] = '\0';
}

ObStorageReader::~ObStorageReader()
{
  if (NULL != reader_) {
    STORAGE_LOG(ERROR, "reader not closed", K(uri_));
  }
}

int ObStorageReader::open(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_READER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (NULL != reader_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  }
#ifdef _WITH_OSS
  else if (OB_STORAGE_OSS == type) {
    reader_ = &oss_reader_;
  }
#endif
  else if (OB_STORAGE_FILE == type) {
    reader_ = &file_reader_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(reader_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open reader", K(ret), K(uri));
    } else {
      file_length_ = reader_->get_length();
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed to close read file", K(ret), K(tmp_ret), K(uri));
    }
  }

  return ret;
}

int ObStorageReader::pread(char* buf, const int64_t buf_size, int64_t offset, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_READER_PREAD) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(reader_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_ISNULL(buf) || offset < 0 || offset > file_length_) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", K(ret), KP(buf), K(offset), K(file_length_));
  } else if (OB_FAIL(reader_->pread(buf, buf_size, offset, read_size))) {
    STORAGE_LOG(WARN, "failed to read file", K(ret));
  }
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
  reader_ = NULL;
  file_length_ = -1;
  start_ts_ = 0;
  uri_[0] = '\0';
  return ret;
}

ObStorageWriter::ObStorageWriter() : writer_(NULL), file_writer_(), start_ts_(0)
{
  uri_[0] = '\0';
}

ObStorageWriter::~ObStorageWriter()
{
  if (NULL != writer_) {
    STORAGE_LOG(ERROR, "writer not close");
  }
}

int ObStorageWriter::open(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_WRITE_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (NULL != writer_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  }
#ifdef _WITH_OSS
  else if (OB_STORAGE_OSS == type) {
    writer_ = &oss_writer_;
  }
#endif
  else if (OB_STORAGE_FILE == type) {
    writer_ = &file_writer_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(writer_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri), K(storage_info));
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close write file", K(ret), K(tmp_ret), K(uri));
    }
  }
  return ret;
}

int ObStorageWriter::write(const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_WRITE_WRITE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(writer_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(writer_->write(buf, size))) {
    STORAGE_LOG(WARN, "failed to write", K(ret));
  }
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
  writer_ = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  return ret;
}

ObStorageAppender::ObStorageAppender() : appender_(NULL), file_appender_(), start_ts_(0), is_opened_(false)
{
  uri_[0] = '\0';
}

ObStorageAppender::ObStorageAppender(StorageOpenMode mode)
    : appender_(NULL), file_appender_(mode), start_ts_(0), is_opened_(false)
{
  uri_[0] = '\0';
}

ObStorageAppender::~ObStorageAppender()
{
  if (is_opened_ && NULL != appender_) {
    STORAGE_LOG(ERROR, "appender not close");
  }
}

int ObStorageAppender::open(
    const common::ObString& uri, const common::ObString& storage_info, const AppenderParam& param)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_APPENDER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (NULL != appender_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  }
#ifdef _WITH_OSS
  else if (OB_STORAGE_OSS == type) {
    appender_ = &oss_appender_;
  }
#endif
  else if (OB_STORAGE_FILE == type) {
    appender_ = &file_appender_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(appender_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri), K(storage_info));
    } else {
      is_opened_ = true;
    }
  }

  if (OB_FAIL(ret)) {
    if (OB_SUCCESS != (tmp_ret = close())) {
      STORAGE_LOG(WARN, "failed close write file", K(ret), K(tmp_ret), K(uri));
    }
  }

  UNUSED(param);

  return ret;
}

int ObStorageAppender::open_deprecated(const common::ObString& uri, const common::ObString& storage_info)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  start_ts_ = ObTimeUtility::current_time();

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_APPENDER_OPEN) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (NULL != appender_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "cannot open twice", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%.*s", uri.length(), uri.ptr()))) {
    STORAGE_LOG(WARN, "failed to fill uri", K(ret), K(uri));
  } else if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get type", K(ret), K(uri));
  }
#ifdef _WITH_OSS
  else if (OB_STORAGE_OSS == type) {
    appender_ = &oss_appender_;
  }
#endif
  else if (OB_STORAGE_FILE == type) {
    appender_ = &file_appender_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(appender_->open(uri, storage_info))) {
      STORAGE_LOG(WARN, "failed to open writer", K(ret), K(uri), K(storage_info));
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

int ObStorageAppender::write(const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;

#ifdef ERRSIM
  ret = E(EventTable::EN_BACKUP_IO_APPENDER_WRITE) OB_SUCCESS;
#endif
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(appender_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(appender_->write(buf, size))) {
    STORAGE_LOG(WARN, "failed to write", K(ret));
  }
  return ret;
}

int64_t ObStorageAppender::get_length()
{
  int64_t ret_int = -1;

  if (OB_ISNULL(appender_)) {
    STORAGE_LOG(WARN, "appender not opened");
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

  if (OB_ISNULL(appender_) || !is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not opened", K(ret));
  } else if (OB_FAIL(appender_->close())) {
    STORAGE_LOG(WARN, "failed to close writer", K(ret));
  }
  appender_ = NULL;
  start_ts_ = 0;
  uri_[0] = '\0';
  is_opened_ = false;
  return ret;
}

ObStorageMetaWrapper::ObStorageMetaWrapper() : file_meta_()
{}

ObStorageMetaWrapper::~ObStorageMetaWrapper()
{}

int ObStorageMetaWrapper::get(const common::ObString& uri, const common::ObString& storage_info, char* buf,
    const int64_t buf_size, int64_t& read_size)
{
  int ret = OB_SUCCESS;
  ObIStorageMetaWrapper* meta = NULL;

  if (OB_FAIL(get_meta(uri, meta))) {
    STORAGE_LOG(WARN, "failed to get meta", K(ret), K(uri));
  } else if (OB_FAIL(meta->get(uri, storage_info, buf, buf_size, read_size))) {
    STORAGE_LOG(WARN, "failed to meta get", K(ret), K(uri));
  }
  return ret;
}

int ObStorageMetaWrapper::set(
    const common::ObString& uri, const common::ObString& storage_info, const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObIStorageMetaWrapper* meta = NULL;

  if (OB_FAIL(get_meta(uri, meta))) {
    STORAGE_LOG(WARN, "failed to get meta", K(ret), K(uri));
  } else if (OB_FAIL(meta->set(uri, storage_info, buf, size))) {
    STORAGE_LOG(WARN, "failed to meta set", K(ret), K(uri));
  }
  return ret;
}

int ObStorageMetaWrapper::get_meta(const common::ObString& uri, ObIStorageMetaWrapper*& meta)
{
  int ret = OB_SUCCESS;
  ObStorageType type = OB_STORAGE_MAX_TYPE;
  meta = NULL;

  if (OB_FAIL(get_storage_type_from_path(uri, type))) {
    STORAGE_LOG(WARN, "failed to get storage type", K(ret));
  }
#ifdef _WITH_OSS
  else if (OB_STORAGE_OSS == type) {
    meta = &oss_meta_;
  }
#endif
  else if (OB_STORAGE_FILE == type) {
    meta = &file_meta_;
  } else {
    ret = OB_ERR_SYS;
    STORAGE_LOG(ERROR, "unkown storage type", K(ret), K(uri));
  }

  return ret;
}

}  // namespace common
}  // namespace oceanbase

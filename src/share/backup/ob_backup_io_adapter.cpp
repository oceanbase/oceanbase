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

#include "ob_backup_io_adapter.h"
#include "share/ob_device_manager.h"
#include "lib/restore/ob_object_device.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/stat/ob_diagnose_info.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_errno.h"
 
namespace oceanbase
{
namespace common
{
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];

int ObBackupIoAdapter::open_with_access_type(ObIODevice*& device_handle, ObIOFd &fd, 
              const share::ObBackupStorageInfo *storage_info, const common::ObString &uri,
              ObStorageAccessType access_type)
{
  int ret = OB_SUCCESS;
  ObIODOpt iod_opt_array[2];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;
  iod_opts.opt_cnt_ = 1;
  
  if (access_type >= OB_STORAGE_ACCESS_MAX_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid access type!", K(access_type));
  } else {
    iod_opts.opts_[0].set("AccessType", OB_STORAGE_ACCESS_TYPES_STR[access_type]);
    if (access_type == OB_STORAGE_ACCESS_RANDOMWRITER) 
    {
      iod_opts.opts_[1].set("OpenMode", "CREATE_OPEN_NOLOCK");
      iod_opts.opt_cnt_++;
    }

    if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
      OB_LOG(WARN, "fail to get and init device!", K(ret), KP(storage_info), K(device_handle));
    } else if (OB_FAIL(device_handle->open(uri.ptr(), -1, 0, fd, &iod_opts))) {
      OB_LOG(WARN, "fail to open with access type!", K(ret), K(uri), K(access_type));
    }
  }
  return ret;
}

void release_device(ObIODevice*& dev_handle)
{
  if (OB_ISNULL(dev_handle)) {
    OB_LOG_RET(WARN, OB_INVALID_ARGUMENT, "device handle is null, invalid parameter!");
  } else {
    ObDeviceManager::get_instance().release_device(dev_handle);
    dev_handle = NULL;
  }
}

int ObBackupIoAdapter::close_device_and_fd(ObIODevice*& device_handle, ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (NULL == device_handle) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "device handle is empty");
  } else {
    if (OB_FAIL(device_handle->close(fd))) {
      OB_LOG(WARN, "fail to close fd!", K(ret), K(fd));
    }

    release_device(device_handle);
  }

  return ret;
}

int ObBackupIoAdapter::get_and_init_device(ObIODevice*& dev_handle, 
                                           const share::ObBackupStorageInfo *storage_info, 
                                           const common::ObString &storage_type_prefix)
{
  int ret = OB_SUCCESS;
  char storage_info_str[OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ObIODOpts opts;
  ObIODOpt opt; //only one para
  opts.opts_ = &(opt);
  opts.opt_cnt_ = 1;
  opt.key_ = "storage_info";
  common::ObObjectStorageInfo storage_info_base;
  if (NULL == storage_info) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "storage info is null", K(ret)); 
  } else if (OB_FAIL(storage_info_base.assign(*storage_info))) {
    OB_LOG(WARN, "fail to assign storage info base!", K(ret), KP(storage_info));
  } else if (OB_FAIL(storage_info_base.get_storage_info_str(storage_info_str, sizeof(storage_info_str)))) {
    // no need encrypt
    OB_LOG(WARN, "fail to get storage info str!", K(ret), KP(storage_info));
  } else if (FALSE_IT(opt.value_.value_str = storage_info_str)) {
  } else if (OB_FAIL(ObDeviceManager::get_instance().get_device(storage_info_str, storage_type_prefix, dev_handle))) {
    OB_LOG(WARN, "fail to get device!", K(ret), KP(storage_info), K(storage_type_prefix));
  } else if (OB_FAIL(dev_handle->start(opts))) {
    OB_LOG(WARN, "fail to start device!", K(ret), KP(storage_info), K(storage_type_prefix));
  } 
  return ret;
}

int ObBackupIoAdapter::is_exist(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, bool &exist)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->exist(uri.ptr(), exist))) {
    OB_LOG(WARN, "fail to check exist!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::adaptively_is_exist(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, bool &exist)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->adaptive_exist(uri.ptr(), exist))) {
    OB_LOG(WARN, "fail to check exist!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::get_file_length(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  ObIODFileStat statbuf;
  file_length = -1;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->stat(uri.ptr(), statbuf))) {
    OB_LOG(WARN, "fail to get file length!", K(ret), K(uri), KP(storage_info));
  } else {
    file_length = statbuf.size_;
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::adaptively_get_file_length(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  ObIODFileStat statbuf;
  file_length = -1;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->adaptive_stat(uri.ptr(), statbuf))) {
    OB_LOG(WARN, "fail to get file length!", K(ret), K(uri), KP(storage_info));
  } else {
    file_length = statbuf.size_;
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::del_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->unlink(uri.ptr()))) {
    OB_LOG(WARN, "fail to del file!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::adaptively_del_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->adaptive_unlink(uri.ptr()))) {
    OB_LOG(WARN, "fail to del file!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::del_unmerged_parts(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->del_unmerged_parts(uri.ptr()))) {
    OB_LOG(WARN, "fail to del file!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::mkdir(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->mkdir(uri.ptr(), 0))) {
    OB_LOG(WARN, "fail to mkdir!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

/*this func should not be in the interface level*/
int ObBackupIoAdapter::mk_parent_dir(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  char path[OB_MAX_URI_LENGTH];
  ObIODevice *device_handle = NULL;

  if (uri.empty()) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid args", K(ret), K(uri));
  } else if (OB_FAIL(databuff_printf(path, sizeof(path), "%.*s", uri.length(), uri.ptr()))) {
    OB_LOG(WARN, "failed to fill path", K(ret), K(path));
  } else if (path[strlen(path) - 1] == '/') {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "cannot mk parent dir for dir", K(ret), K(path));
  } else {
    bool found = false;
    for (int64_t i = strlen(path) - 1; i >= 0; --i) {
      if (path[i] == '/') {
        path[i] = '\0';
        found = true;
        OB_LOG(INFO, "found parent dir", K(i), K(path));
        break;
      }
    }

    if (!found) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "no dir found in uri", K(ret), K(uri));
    } else if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
      OB_LOG(WARN, "fail to get device!", K(ret));
    } else if (OB_FAIL(device_handle->mkdir(path, 0))) {
      OB_LOG(WARN, "fail to make parent dir", K(ret), K(path), K(uri));
    }
    release_device(device_handle);
  }
  return ret;
}

int ObBackupIoAdapter::write_single_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
                                         const char *buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;
  ObIOFd fd; 
  ObIODevice *device_handle = NULL;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t file_length = -1;
  int64_t write_size = -1;

  #ifdef ERRSIM
  ret = OB_E(EventTable::EN_BACKUP_IO_BEFORE_WRITE_SINGLE_FILE) OB_SUCCESS;
  #endif

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(open_with_access_type(device_handle, fd, storage_info, 
                      uri, OB_STORAGE_ACCESS_OVERWRITER))) {
    OB_LOG(WARN, "fail to get device and open file !", K(uri), K(ret));
  } else if (OB_FAIL(device_handle->write(fd, buf, size, write_size))) {
    STORAGE_LOG(WARN, "fail to write file", K(ret), K(uri));
  }
  
  if (OB_SUCCESS != (ret_tmp = close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? ret_tmp : ret;
    STORAGE_LOG(WARN, "failed to close device and fd", K(ret), K(ret_tmp));
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

int ObBackupIoAdapter::read_single_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, char *buf, 
                                        const int64_t buf_size, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t file_length = -1;

  if (OB_FAIL(open_with_access_type(device_handle, fd, storage_info, 
                      uri, OB_STORAGE_ACCESS_READER))) {
    OB_LOG(WARN, "fail to get device and open file !", K(uri), K(ret));
  } else if (OB_FAIL(device_handle->pread(fd, 0, buf_size, buf, read_size))) {
    OB_LOG(WARN, "failed to do read single file", K(ret), K(uri));
  } else if (OB_FAIL(get_file_length(uri, storage_info, file_length))) {
    OB_LOG(WARN, "failed to get file size", K(ret), K(uri));
  } else if (file_length != read_size) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "not whole file read, maybe buf not enough",
          K(ret), K(read_size), K(file_length), K(uri));
  }

  if (OB_SUCCESS != (ret_tmp = close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? ret_tmp : ret;
    STORAGE_LOG(WARN, "failed to close device and fd", K(ret), K(ret_tmp));
  }

  return ret;
}

int ObBackupIoAdapter::adaptively_read_single_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, char *buf,
                                        const int64_t buf_size, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;
  const int64_t start_ts = ObTimeUtility::current_time();
  int64_t file_length = -1;

  if (OB_FAIL(open_with_access_type(device_handle, fd, storage_info,
                      uri, OB_STORAGE_ACCESS_ADAPTIVE_READER))) {
    OB_LOG(WARN, "fail to get device and open file !", K(uri), K(ret));
  } else if (OB_FAIL(device_handle->pread(fd, 0, buf_size, buf, read_size))) {
    OB_LOG(WARN, "failed to do read single file", K(ret), K(uri));
  } else if (OB_FAIL(adaptively_get_file_length(uri, storage_info, file_length))) {
    OB_LOG(WARN, "failed to get file size", K(ret), K(uri));
  } else if (file_length != read_size) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "not whole file read, maybe buf not enough",
          K(ret), K(read_size), K(file_length), K(uri));
  }

  if (OB_SUCCESS != (ret_tmp = close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? ret_tmp : ret;
    STORAGE_LOG(WARN, "failed to close device and fd", K(ret), K(ret_tmp));
  }

  return ret;
}

int ObBackupIoAdapter::read_single_text_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
                                             char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = -1;
  if (OB_FAIL(ObBackupIoAdapter::read_single_file(uri, storage_info, buf, buf_size, read_size))) {
    OB_LOG(WARN, "failed to read_single_object", K(ret), K(uri));
  } else if (read_size < 0 || read_size >= buf_size) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "buf not enough", K(ret), K(read_size), K(buf_size));
  } else {
    buf[read_size] = '\0';
  }
  return ret;
}

int ObBackupIoAdapter::adaptively_read_single_text_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
                                             char *buf, const int64_t buf_size)
{
  int ret = OB_SUCCESS;
  int64_t read_size = -1;
  if (OB_FAIL(ObBackupIoAdapter::adaptively_read_single_file(uri, storage_info, buf, buf_size, read_size))) {
    OB_LOG(WARN, "failed to read_single_object", K(ret), K(uri));
  } else if (read_size < 0 || read_size >= buf_size) {
    ret = OB_BUF_NOT_ENOUGH;
    OB_LOG(WARN, "buf not enough", K(ret), K(read_size), K(buf_size));
  } else {
    buf[read_size] = '\0';
  }
  return ret;
}

int ObBackupIoAdapter::list_files(const common::ObString &dir_path, const share::ObBackupStorageInfo *storage_info,
        common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, dir_path))) {
    OB_LOG(WARN, "fail to get device!", K(ret));
  } else if (OB_FAIL(device_handle->scan_dir(dir_path.ptr(), op))) {
    OB_LOG(WARN, "fail to scan dir!", K(ret), K(dir_path), KP(storage_info));
  } 
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::adaptively_list_files(const common::ObString &dir_path, const share::ObBackupStorageInfo *storage_info,
        common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, dir_path))) {
    OB_LOG(WARN, "fail to get device!", K(ret));
  } else if (OB_FAIL(device_handle->adaptive_scan_dir(dir_path.ptr(), op))) {
    OB_LOG(WARN, "fail to scan dir!", K(ret), K(dir_path), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::list_directories(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
                        common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  op.set_dir_flag();
  if (OB_FAIL(list_files(uri, storage_info, op))) {
    OB_LOG(WARN, "fail to list directories!", K(ret), K(uri), KP(storage_info));
  }
  return ret;
}

int ObBackupIoAdapter::is_tagging(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info, bool &is_tagging)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->is_tagging(uri.ptr(), is_tagging))) {
    OB_LOG(WARN, "fail to check tagging!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::read_part_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
      char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;

  if (OB_FAIL(open_with_access_type(device_handle, fd, storage_info, 
                      uri, OB_STORAGE_ACCESS_READER))) {
    OB_LOG(WARN, "fail to get device and open file !", K(uri), K(ret), KP(storage_info));
  } else if (OB_FAIL(device_handle->pread(fd, offset, buf_size, buf, read_size))) {
    OB_LOG(WARN, "fail to read file !", K(uri), K(ret), KP(storage_info));
  }

  if (OB_SUCCESS != (ret_tmp = close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? ret_tmp : ret;
    STORAGE_LOG(WARN, "failed to close device and fd", K(ret), K(ret_tmp), KP(storage_info), K(uri));
  }
  return ret;
}

int ObBackupIoAdapter::adaptively_read_part_file(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info,
      char *buf, const int64_t buf_size, const int64_t offset, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  int ret_tmp = OB_SUCCESS;
  ObIOFd fd;
  ObIODevice*device_handle = NULL;

  if (OB_FAIL(open_with_access_type(device_handle, fd, storage_info,
                      uri, OB_STORAGE_ACCESS_ADAPTIVE_READER))) {
    OB_LOG(WARN, "fail to get device and open file !", K(uri), K(ret), KP(storage_info));
  } else if (OB_FAIL(device_handle->pread(fd, offset, buf_size, buf, read_size))) {
    OB_LOG(WARN, "fail to read file !", K(uri), K(ret), KP(storage_info));
  }

  if (OB_SUCCESS != (ret_tmp = close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? ret_tmp : ret;
    STORAGE_LOG(WARN, "failed to close device and fd", K(ret), K(ret_tmp), KP(storage_info), K(uri));
  }
  return ret;
}

int ObBackupIoAdapter::get_file_size(ObIODevice *device_handle, const ObIOFd &fd, int64_t &file_length)
{
  int ret = OB_SUCCESS;
  int flag = -1;
  void* ctx = NULL;

  file_length = -1;
  ObFdSimulator::get_fd_flag(fd, flag);
  ObObjectDevice* obj_device_handle = (ObObjectDevice*)device_handle;
  if (OB_STORAGE_ACCESS_READER != flag
             && OB_STORAGE_ACCESS_ADAPTIVE_READER != flag
             && OB_STORAGE_ACCESS_APPENDER != flag ) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "Invaild access type, object device only support reader and appender get file size!", K(flag));
  } else if (OB_FAIL(obj_device_handle->get_fd_mng().fd_to_ctx(fd, ctx))) {
    OB_LOG(WARN, "fail to get ctx accroding fd!", K(ret));
  } else {
    if (OB_STORAGE_ACCESS_READER == flag) {
      ObStorageReader *reader = static_cast<ObStorageReader*>(ctx);
      file_length = reader->get_length();
    } else if (OB_STORAGE_ACCESS_ADAPTIVE_READER == flag) {
      ObStorageAdaptiveReader *adaptive_reader = static_cast<ObStorageAdaptiveReader*>(ctx);
      file_length = adaptive_reader->get_length();
    } else {
      ObStorageAppender *appender = static_cast<ObStorageAppender*>(ctx);
      file_length = appender->get_length();
    }
  }
  return ret;
}

int ObBackupIoAdapter::del_dir(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_FAIL(device_handle->rmdir(uri.ptr()))) {
    OB_LOG(WARN, "fail to remove dir!", K(ret), K(uri), KP(storage_info));
  }
  release_device(device_handle);
  return ret;  
}

class ObDelTmpFileOp : public ObBaseDirEntryOperator
{
public: 
  ObDelTmpFileOp(int64_t now_ts, char* dir_path, ObIODevice *device_handle) :
      now_ts_(now_ts), dir_path_(dir_path), device_handle_(device_handle)
      {}
  ~ObDelTmpFileOp() {}
  int func(const dirent *entry) override;
private:
  int64_t now_ts_;
  char* dir_path_;
  ObIODevice *device_handle_;
};

int get_tmp_file_format_timestamp(const char *file_name, 
            bool &is_tmp_file, int64_t &timestamp)
{
  int ret = OB_SUCCESS;
  timestamp = 0;
  const char *tmp_file_format = ".tmp.";
  is_tmp_file = true;

  if (OB_ISNULL(file_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "check need delete tmp file get invalid argument",
        K(ret), KP(file_name));
  } else {
    const int64_t file_name_length = strlen(file_name);
    const int64_t tmp_file_format_length = strlen(tmp_file_format);
    int64_t timestamp_position = -1;
    bool found = true;

    for (int64_t i = file_name_length - 1; i >= 0; --i) {
      found = true;
      for (int64_t j = 0; j < tmp_file_format_length && found; ++j) {
        if (i - j < 0) {
          found = false;
        } else if (file_name[i - j] != tmp_file_format[tmp_file_format_length - j -1]) {
          found = false;
        }
      }

      if (found) {
        timestamp_position = i + 1;
        break;
      }
    }

    if (!found) {
      is_tmp_file = false;
    } else if (timestamp_position >= file_name_length) {
      //found
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "tmp file formate is unexpected", K(ret), K(file_name));
    } else if (OB_FAIL(ob_atoll(file_name + timestamp_position, timestamp))) {
      OB_LOG(WARN, "failed to get tmp file timestamp", K(ret), K(file_name));
    }
  }
  return ret;
}

int ObDelTmpFileOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  int64_t tmp_file_timestamp = 0;
  bool is_tmp_file = false;
  int64_t MAX_OBSOLETE_INTERVAL = 60 * 60L * 1000 * 1000; //1h

  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "failed to check lease", K(ret));
  } else if (DT_REG != entry->d_type) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "dt type is not a regular file!", K(entry->d_type));
  } else {
    if (OB_FAIL(get_tmp_file_format_timestamp(
        entry->d_name, is_tmp_file, tmp_file_timestamp))) {
      OB_LOG(WARN, "failed to get tmp file format timestamp", K(ret), K(entry->d_name));
    } else if (!is_tmp_file) {
      //do nothing
    } else if (now_ts_ - tmp_file_timestamp < MAX_OBSOLETE_INTERVAL) {
      if (REACH_TIME_INTERVAL(100 * 1000)/*100ms*/) {
        OB_LOG(INFO, "tmp file can not delete",
            K(now_ts_), K(tmp_file_timestamp), K(MAX_OBSOLETE_INTERVAL));
      }
    } else {
      char tmp_file_path[OB_MAX_URI_LENGTH] = "";
      if (OB_FAIL(databuff_printf(
          tmp_file_path, OB_MAX_URI_LENGTH, "%s/%s", dir_path_, entry->d_name))) {
        OB_LOG(WARN, "failed to fill path", K(ret));
      } else if (OB_FAIL(device_handle_->unlink(tmp_file_path))) {
        ret = OB_IO_ERROR;
        char errno_buf[OB_MAX_ERROR_MSG_LEN];
        OB_LOG(WARN, "failed to del file",
            K(ret), K(tmp_file_path), K(errno), "errno", strerror_r(errno, errno_buf, sizeof(errno_buf)));
      }
    }
  }
  return ret;  
}

/*for object device, there are device type handle in front uri, before handle, we need to remove it*/
int get_real_file_path(const common::ObString &uri, char *buf, const int64_t buf_size, int device_type)
{
  int ret = OB_SUCCESS;
  int64_t offset = 0;
  const char* prefix = NULL;

  if (OB_STORAGE_OSS == device_type) {
    prefix = OB_OSS_PREFIX;
  } else if (OB_STORAGE_COS == device_type) {
    prefix = OB_COS_PREFIX;
  } else if (OB_STORAGE_S3 == device_type) {
    prefix = OB_S3_PREFIX;
  } else if (OB_STORAGE_FILE == device_type) {
    prefix = OB_FILE_PREFIX;
  } else {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid device type!", K(device_type), K(ret), K(uri));
  }

  if (NULL != prefix) {
    offset = strlen(prefix);
    if (uri.empty()) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid args", K(ret), K(uri));
    } else if (!uri.prefix_match(prefix)) {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid uri", K(ret), K(uri));
    } else if (OB_FAIL(databuff_printf(
                      buf, buf_size, "%.*s",
                      static_cast<int>(uri.length() - offset),
                      uri.ptr() + offset))) {
      OB_LOG(WARN, "failed to fill path", K(ret), K(uri));
    } else if (strlen(buf) <= 0 && buf[0] != '/') {
      ret = OB_INVALID_ARGUMENT;
      OB_LOG(WARN, "invalid file path", K(ret), K(uri), K(buf));
    }
  } 
  return ret;
}

/*only nfs delete the tmp file*/
int ObBackupIoAdapter::delete_tmp_files(const common::ObString &uri, const share::ObBackupStorageInfo *storage_info)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to get device!", K(ret), K(uri), KP(storage_info));
  } else if (OB_STORAGE_FILE == device_handle->device_type_) {
    char dir_path[OB_MAX_URI_LENGTH];
    const int64_t now_ts = ObTimeUtil::current_time();
    ObDelTmpFileOp del_tmp_file_op(now_ts, dir_path, device_handle);
    if (OB_FAIL(get_real_file_path(uri, dir_path, sizeof(dir_path), OB_STORAGE_FILE))) {
      OB_LOG(WARN, "fail to get real path from uri(delete_tmp_files)!", K(ret), K(uri), KP(storage_info));
    } else if (OB_FAIL(device_handle->scan_dir(uri.ptr(), del_tmp_file_op))) {
      OB_LOG(WARN, "fail to list file for delete_tmp_files!", K(ret), K(uri), KP(storage_info));
    }
  } 
  release_device(device_handle);
  return ret;
}

class ObCheckDirEmptOp : public ObBaseDirEntryOperator
{
public: 
  ObCheckDirEmptOp() : file_cnt_(0) {}
  ~ObCheckDirEmptOp() {}
  int func(const dirent *entry) override;
  int64_t get_file_cnt() {return file_cnt_;}
private:
  int64_t file_cnt_;
};

int ObCheckDirEmptOp::func(const dirent *entry)
{
  UNUSED(entry);
  file_cnt_++;
  return OB_ERR_EXIST_OBJECT;
}
  // TODO(wenjinyu.wjy) need to be refactored 4.3
int ObBackupIoAdapter::is_empty_directory(const common::ObString &uri, 
                                        const share::ObBackupStorageInfo *storage_info, 
                                        bool &is_empty_directory)
{
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = NULL;
  ObCheckDirEmptOp ept_dir_op;
  is_empty_directory = true;
  if (OB_FAIL(get_and_init_device(device_handle, storage_info, uri))) {
    OB_LOG(WARN, "fail to del dir!", K(ret), K(uri));
  } else if (OB_FAIL(device_handle->scan_dir(uri.ptr(), ept_dir_op))) {
    int64_t file_cnt = ept_dir_op.get_file_cnt();
    if (OB_ERR_EXIST_OBJECT == ret && 1 == file_cnt) {
      is_empty_directory = false;
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "fail to scan dir!", K(ret), K(uri));
    }
  }
  ept_dir_op.set_dir_flag();
  if (OB_FAIL(ret)) {
  } else if (is_empty_directory && OB_FAIL(device_handle->scan_dir(uri.ptr(), ept_dir_op))) {
    int64_t file_cnt = ept_dir_op.get_file_cnt();
    if (OB_ERR_EXIST_OBJECT == ret && 1 == file_cnt) {
      is_empty_directory = false;
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "fail to scan dir!", K(ret), K(uri));
    }
  }
  release_device(device_handle);
  return ret;
}

int ObBackupIoAdapter::set_access_type(ObIODOpts* opts, bool is_appender, int max_opt_num)
{
  int ret = OB_SUCCESS;
  if (opts->opt_cnt_ >= max_opt_num) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to set access type, opt size is small!", K(opts->opt_cnt_), K(max_opt_num));
  } else {
    const char* access_type = is_appender ? OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_APPENDER] : OB_STORAGE_ACCESS_TYPES_STR[OB_STORAGE_ACCESS_READER];
    opts->opts_[opts->opt_cnt_].set("AccessType", access_type); 
    opts->opt_cnt_++;
  }
  return ret;
}

/*The caller need guarantee that opt not null*/
int ObBackupIoAdapter::set_open_mode(ObIODOpts* opts, bool lock_mode, bool new_file, int max_opt_num)
{
  int ret = OB_SUCCESS;
  if (opts->opt_cnt_ >= max_opt_num) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to set access type, opt size is small!", K(opts->opt_cnt_), K(max_opt_num));
  } else {
    const char* open_mode = lock_mode ? "CREATE_OPEN_LOCK" : (new_file ? "EXCLUSIVE_CREATE" : "ONLY_OPEN_UNLOCK");
    opts->opts_[opts->opt_cnt_].set("OpenMode", open_mode);
    opts->opt_cnt_++;
  }
  return ret;
}

int ObBackupIoAdapter::set_append_strategy(ObIODOpts* opts, bool is_data_file, int64_t epoch, int max_opt_num)
{
  int ret = OB_SUCCESS;
  if ((opts->opt_cnt_ + 1) >= max_opt_num) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "fail to set access type, opt size is small!", K(opts->opt_cnt_), K(max_opt_num));
  } else {
    if (is_data_file) {
      opts->opts_[opts->opt_cnt_].set("AppendStrategy", "OB_APPEND_USE_SLICE_PUT");
      opts->opt_cnt_++;
      if (-1 != epoch) {
        opts->opts_[opts->opt_cnt_].set("AppendVersion", epoch);
        opts->opt_cnt_++;
      }
    } else {
      opts->opts_[opts->opt_cnt_].set("AppendStrategy", "OB_APPEND_USE_OVERRITE");
      opts->opt_cnt_++;
    }
  }
  return ret;
}

int ObFileListArrayOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, entry is null");
  } else if (OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid list entry, d_name is null");
  } else if (name_array_.count() >= 1000000) { //temp fix for bug
    ret = OB_SIZE_OVERFLOW;
    OB_LOG(WARN, "two many files in the directory", K(name_array_.count()), KR(ret));
  } else {
    const ObString file_name(entry->d_name);
    ObString tmp_file;
    if (OB_FAIL(ob_write_string(allocator_, file_name, tmp_file, true))) {
      OB_LOG(WARN, "fail to save file name", K(ret), K(file_name));
    } else if (OB_FAIL(name_array_.push_back(tmp_file))) {
      OB_LOG(WARN, "fail to push filename to array", K(ret), K(tmp_file));
    }
  }
  return ret;
}

//*************ObDirPrefixEntryNameFilter*************
int ObDirPrefixEntryNameFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "dir prefix filter not init", K(ret));
  } else if (OB_ISNULL(entry) || OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (STRLEN(entry->d_name) < STRLEN(filter_str_)) {
    // do nothing
  } else if (0 == STRNCMP(entry->d_name, filter_str_, STRLEN(filter_str_))) {
    ObIODirentEntry p_entry(entry->d_name, entry->d_type);
    if (OB_FAIL(d_entrys_.push_back(p_entry))) {
      OB_LOG(WARN, "fail to push back directory entry", K(ret), K(p_entry), K_(filter_str));
    }
  }
  return ret;
}

int ObDirPrefixEntryNameFilter::init(
    const char *filter_str,
    const int32_t filter_str_len)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(filter_str) || 0 == filter_str_len) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(ret));
  } else if (filter_str_len > (sizeof(filter_str_) - 1)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the length of dir prefix too long", K(ret), K(filter_str_len));
  } else if (OB_FAIL(databuff_printf(filter_str_, sizeof(filter_str_), "%.*s", filter_str_len, filter_str))) {
    OB_LOG(WARN, "failed to init filter_str", K(ret), K(filter_str), K(filter_str_len));
  } else {
    is_inited_ = true;
  }
  return ret;
}

}
}

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

#define USING_LOG_PREFIX SHARE
#include <sys/vfs.h>
#include <sys/statvfs.h>
#include "common/storage/ob_io_device.h"
#include "share/ob_device_manager.h"
#include "share/ob_io_device_helper.h"
#include "common/ob_smart_call.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
/**
 * --------------------------------ObGetFileIdRangeFunctor------------------------------------
 */
int ObGetFileIdRangeFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    bool is_number = true;
    const char* entry_name = entry->d_name;
    for (int64_t i = 0; is_number && i < sizeof(entry->d_name); ++i) {
      if ('\0' == entry_name[i]) {
        break;
      } else if (!isdigit(entry_name[i])) {
        is_number = false;
      }
    }
    if (!is_number) {
      // do nothing, skip invalid file like tmp
    } else {
      uint32_t file_id = static_cast<uint32_t>(strtol(entry->d_name, nullptr, 10));
      if (OB_INVALID_FILE_ID == min_file_id_ || file_id < min_file_id_) {
        min_file_id_ = file_id;
      }
      if (OB_INVALID_FILE_ID == max_file_id_ || file_id > max_file_id_) {
        max_file_id_ = file_id;
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObGetFileSizeFunctor------------------------------------
 */
int ObGetFileSizeFunctor::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(entry)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(entry));
  } else {
    char full_path[common::MAX_PATH_SIZE] = { 0 };
    int p_ret = snprintf(full_path, sizeof(full_path), "%s/%s", dir_, entry->d_name);
    if (p_ret < 0 || p_ret >= sizeof(full_path)) {
      ret = OB_BUF_NOT_ENOUGH;
      LOG_WARN("file name too long", K(ret), K_(dir), K(entry->d_name));
    } else {
      ObIODFileStat statbuf;
      if (OB_FAIL(LOCAL_DEVICE_INSTANCE.stat(full_path, statbuf))
          && OB_NO_SUCH_FILE_OR_DIRECTORY != ret) {
        LOG_WARN("fail to stat file", K(full_path), K(statbuf));
      } else if (OB_NO_SUCH_FILE_OR_DIRECTORY == ret) {
        // file could be renamed (tmp file) or recycled concurrently, ignore
        ret = OB_SUCCESS;
      } else if (S_ISREG(statbuf.mode_)) {
        total_size_ += statbuf.size_;
      } else if (S_ISDIR(statbuf.mode_)) {
        ObGetFileSizeFunctor functor(full_path);
        if (OB_FAIL(LOCAL_DEVICE_INSTANCE.scan_dir(full_path, functor))) {
          LOG_WARN("fail to scan dir", K(ret), K(full_path), K(entry->d_name));
        } else {
          total_size_ += functor.get_total_size();
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected file type", K(full_path), K(statbuf));
      }
    }
  }
  return ret;
}

/**
 * --------------------------------ObScanDirOp------------------------------------
 */
int ObScanDirOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObScanDirOp::set_dir(const char *dir)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dir == nullptr || strlen(dir) >= common::MAX_PATH_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dir));
  } else {
    dir_ = dir;
  }
  return ret;
}

/**
 * --------------------------------ObSNIODeviceWrapper------------------------------------
 */
ObSNIODeviceWrapper &ObSNIODeviceWrapper::get_instance()
{
  static ObSNIODeviceWrapper instance;
  return instance;
}

ObSNIODeviceWrapper::ObSNIODeviceWrapper()
  : local_device_(NULL), is_inited_(false)
{
}

ObSNIODeviceWrapper::~ObSNIODeviceWrapper()
{
  destroy();
}

/*io device helper hold one local/ofs device, and never free*/
int ObSNIODeviceWrapper::get_local_device_from_mgr(share::ObLocalDevice *&local_device)
{
  int ret = OB_SUCCESS;

  common::ObIODevice* device = nullptr;
  common::ObString storage_type_prefix(OB_LOCAL_PREFIX);
  const ObStorageIdMod storage_id_mod(0, ObStorageUsedMod::STORAGE_USED_DATA);
  if(OB_FAIL(common::ObDeviceManager::get_local_device(storage_type_prefix, storage_id_mod, device))) {
    LOG_WARN("fail to get local device", K(ret));
  } else if (OB_ISNULL(local_device = static_cast<share::ObLocalDevice*>(device))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get local device", K(ret));
  }

  if (OB_FAIL(ret)) {
    //if fail, release the resource
    if (nullptr != local_device) {
      common::ObDeviceManager::get_instance().release_device((ObIODevice*&)local_device);
      local_device = nullptr;
    }
  }
  return ret;
}

int ObSNIODeviceWrapper::init(
    const char *data_dir,
    const char *sstable_dir,
    const int64_t block_size,
    const int64_t data_disk_percentage,
    const int64_t data_disk_size)
{
  int ret = OB_SUCCESS;
  const int64_t MAX_IOD_OPT_CNT = 5;
  ObIODOpt iod_opt_array[MAX_IOD_OPT_CNT];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("already inited", K(ret));
  } else if (OB_FAIL(get_local_device_from_mgr(local_device_))) {
    LOG_WARN("fail to get the device", K(ret));
  } else if (OB_ISNULL(data_dir) || OB_ISNULL(sstable_dir)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(data_dir), KP(sstable_dir));
  } else if ('/' != data_dir[0] && '.' != data_dir[0]) {
    ret = OB_IO_ERROR;
    LOG_ERROR("unknown storage type, not support", K(ret), K(data_dir));
  } else {
    iod_opt_array[0].set("data_dir", data_dir);
    iod_opt_array[1].set("sstable_dir", sstable_dir);
    iod_opt_array[2].set("block_size", block_size);
    iod_opt_array[3].set("datafile_disk_percentage", data_disk_percentage);
    iod_opt_array[4].set("datafile_size", data_disk_size);
    iod_opts.opt_cnt_ = MAX_IOD_OPT_CNT;
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(local_device_)) {
    if (OB_FAIL(local_device_->init(iod_opts))) {
      LOG_WARN("fail to init io device", K(ret), K(data_dir), K(sstable_dir), K(block_size),
          K(data_disk_percentage), K(data_disk_size));
    } else {
      is_inited_ = true;
      LOG_INFO("finish to init io device", K(ret), K(data_dir), K(sstable_dir), K(block_size),
          K(data_disk_percentage), K(data_disk_size));
    }
  }

  if (IS_NOT_INIT) {
    destroy();
  }

  return ret;
}

void ObSNIODeviceWrapper::destroy()
{
  if (is_inited_) {
    if (NULL != local_device_) {
      local_device_->destroy();
      common::ObDeviceManager::get_instance().release_device((ObIODevice*&)local_device_);
      local_device_ = NULL;
    }

    is_inited_ = false;
    LOG_INFO("io device wrapper destroy");
  }
}

/**
 * --------------------------------ObIODeviceLocalFileOp------------------------------------
 */
int ObIODeviceLocalFileOp::open(
    const char *pathname,
    const int flags,
    const mode_t mode,
    ObIOFd &fd,
    common::ObIODOpts *opts)
{
  UNUSED(opts);
  int ret = OB_SUCCESS;
  int local_fd = 0;

  if (OB_ISNULL(pathname)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid pathname, ", K(ret), KP(pathname));
  } else if ((local_fd = ::open(pathname, flags, mode)) < 0) {
    ret = convert_sys_errno();
    // use DEBUG log level to avoid too many unnecessary logs
    SHARE_LOG(DEBUG, "Fail to open file, ", K(ret), K(pathname), K(flags), K(mode), K(local_fd), KERRMSG);
  } else {
    fd.first_id_ = ObIOFd::NORMAL_FILE_ID;
    fd.second_id_ = local_fd;
  }
  return ret;
}

int ObIODeviceLocalFileOp::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(fd.is_block_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "The block file does not need close, ", K(ret), K(fd));
  } else if (0 != ::close(static_cast<int32_t>(fd.second_id_))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "Fail to close file, ", K(ret), K(fd));
  }
  return ret;
}

int ObIODeviceLocalFileOp::mkdir(const char *pathname, mode_t mode)
{
  int ret = OB_SUCCESS;
  if (NULL == pathname || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (::mkdir(pathname, mode) != 0) {
    if (EEXIST == errno) {
      ret = OB_SUCCESS;
    } else {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "create directory failed.", K(pathname), K(errno), KERRMSG, K(ret));
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::rmdir(const char *pathname)
{
  int ret = OB_SUCCESS;
  ObIODFileStat f_stat;
  if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(ret), KP(pathname));
  } else if (OB_FAIL(stat(pathname, f_stat))) {
    SHARE_LOG(WARN, "stat path fail", K(pathname), K(ret));
  } else if (!S_ISDIR(f_stat.mode_)) {
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    SHARE_LOG(WARN, "file path is not a directory.", K(pathname), K(ret));
  } else if (0 != ::rmdir(pathname)) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "rmdir failed.", K(pathname), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObIODeviceLocalFileOp::unlink(const char *pathname)
{
  int ret = OB_SUCCESS;
  ObIODFileStat f_stat;
  if (OB_ISNULL(pathname) || OB_UNLIKELY(0 == STRLEN(pathname))) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid arguments.", K(pathname), K(ret));
  } else if (OB_FAIL(stat(pathname, f_stat))) {
    SHARE_LOG(WARN, "stat path fail", K(pathname), K(ret));
  } else if (!S_ISREG(f_stat.mode_)) {
    ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
    SHARE_LOG(WARN, "file path is a directory.", K(pathname), K(ret));
  } else if (0 != ::unlink(pathname)){
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "unlink file failed.", K(pathname), K(errno), KERRMSG, K(ret));
  }
  return ret;
}

int ObIODeviceLocalFileOp::rename(const char *oldpath, const char *newpath)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(oldpath) || OB_ISNULL(newpath)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(oldpath), KP(newpath));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::rename(oldpath, newpath))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to rename file, ", K(ret), K(sys_ret), KERRMSG);
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::seal_file(const ObIOFd &fd)
{
  UNUSED(fd);
  return OB_NOT_SUPPORTED;
}

int ObIODeviceLocalFileOp::scan_dir(const char *dir_name, int (*func)(const dirent *entry))
{
  int ret = OB_SUCCESS;
  DIR *open_dir = nullptr;
  struct dirent entry;
  struct dirent *result = nullptr;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      SHARE_LOG(WARN, "fail to open dir", K(ret), K(dir_name), K(errno), KERRMSG);
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      SHARE_LOG(WARN, "dir does not exist", K(ret), K(dir_name), K(errno), KERRMSG);
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "read dir error", K(ret), KERRMSG);
      } else if (NULL != result
          && 0 != STRCMP(entry.d_name, ".")
          && 0 != STRCMP(entry.d_name, "..")) {
        if (OB_FAIL((*func)(&entry))) {
          SHARE_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
        }
      } else if (NULL == result) {
        break;//end file
      }
    }
    //close dir
    if (NULL != open_dir) {
      ::closedir(open_dir);
      open_dir = nullptr;
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::is_tagging(const char *pathname, bool &is_tagging)
{
  UNUSED(pathname);
  UNUSED(is_tagging);
  return OB_NOT_SUPPORTED;
}

int ObIODeviceLocalFileOp::scan_dir(const char *dir_name, common::ObBaseDirEntryOperator &op)
{
  int ret = OB_SUCCESS;
  DIR *open_dir = nullptr;
  struct dirent entry;
  struct dirent *result = nullptr;

  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      SHARE_LOG(WARN, "fail to open dir", K(ret), K(dir_name), K(errno), KERRMSG);
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      SHARE_LOG(WARN, "dir does not exist", K(ret), K(dir_name), K(errno), KERRMSG);
    }
  } else {
    while (OB_SUCC(ret) && NULL != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "read dir error", K(ret), KERRMSG);
      } else if (NULL != result
          && 0 != STRCMP(entry.d_name, ".")
          && 0 != STRCMP(entry.d_name, "..")) {
        if (OB_FAIL(op.func(&entry))) {
          SHARE_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name));
        }
      } else if (NULL == result) {
        break; //end file
      }
    }
    //close dir
    if (NULL != open_dir) {
      ::closedir(open_dir);
      open_dir = nullptr;
    }
  }
  return ret;
}

/*
 * scan_dir_rec is scan directory recursion,
 * you need to provide file_op and dir_op, dir_op is used to operate directory, file_op is used to operate other files, for example reg file, link file.
 */
int ObIODeviceLocalFileOp::scan_dir_rec(const char *dir_name,
                                        ObScanDirOp &file_op,
                                        ObScanDirOp &dir_op)
{
  int ret = OB_SUCCESS;
  DIR *open_dir = nullptr;
  struct dirent entry;
  struct dirent *result = nullptr;
  if (OB_ISNULL(dir_name)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), K(dir_name));
  } else if (OB_ISNULL(open_dir = ::opendir(dir_name))) {
    if (ENOENT != errno) {
      ret = OB_FILE_NOT_OPENED;
      SHARE_LOG(WARN, "fail to open dir", K(ret), K(dir_name));
    } else {
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      SHARE_LOG(WARN, "dir does not exist", K(ret), K(dir_name));
    }
  } else if (OB_FAIL(file_op.set_dir(dir_name))) {
    SHARE_LOG(WARN, "fail to set dir", K(ret), K(dir_name));
  } else if (OB_FAIL(dir_op.set_dir(dir_name))) {
    SHARE_LOG(WARN, "fail to set dir", K(ret), K(dir_name));
  } else {
    char current_file_path[OB_MAX_FILE_NAME_LENGTH] = {'\0'};
    while (OB_SUCC(ret) && nullptr != open_dir) {
      if (0 != ::readdir_r(open_dir, &entry, &result)) {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "read dir error", K(ret), KERRMSG);
      } else if (nullptr != result
          && 0 != STRCMP(entry.d_name, ".")
          && 0 != STRCMP(entry.d_name, "..")) {
        bool is_dir = false;
        MEMSET(current_file_path, '\0', OB_MAX_FILE_NAME_LENGTH);
        int pret = snprintf(current_file_path, OB_MAX_FILE_NAME_LENGTH, "%s/%s", dir_name, entry.d_name);
        if (pret <= 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_BUF_NOT_ENOUGH;
          SHARE_LOG(WARN, "snprintf failed", K(ret), K(current_file_path), K(dir_name), K(entry.d_name));
        } else if (DT_DIR == entry.d_type) {
          is_dir = true;
        } else if (DT_UNKNOWN == entry.d_type) {
          ObIODFileStat statbuf;
          if (OB_FAIL(ObIODeviceLocalFileOp::stat(current_file_path, statbuf))) {
            SHARE_LOG(WARN, "fail to stat file", K(ret), K(current_file_path));
          } else if (S_ISDIR(statbuf.mode_)) {
            is_dir = true;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (false == is_dir) {
          if (OB_FAIL(file_op.func(&entry))) {
            SHARE_LOG(WARN, "fail to operate file entry", K(ret), K(dir_name), K(entry.d_name));
          }
        } else if (true == is_dir) {
          if (OB_FAIL(SMART_CALL(scan_dir_rec(current_file_path, file_op, dir_op)))) {
            SHARE_LOG(WARN, "scan directory failed", K(ret), K(current_file_path));
          } else if (OB_FAIL(file_op.set_dir(dir_name))) {
            SHARE_LOG(WARN, "fail to set dir", K(ret), K(dir_name));
          } else if (OB_FAIL(dir_op.set_dir(dir_name))) {
            SHARE_LOG(WARN, "fail to set dir", K(ret), K(dir_name));
          } else if (OB_FAIL(dir_op.func(&entry))) {
            SHARE_LOG(WARN, "fail to operate dir entry", K(ret), K(dir_name), K(entry.d_name));
          }
        }
      } else if (NULL == result) {
        break; //end file
      }
    }
  }
  //close dir
  if (nullptr != open_dir) {
    ::closedir(open_dir);
    open_dir = nullptr;
  }
  return ret;
}

int ObIODeviceLocalFileOp::fsync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid fd, not normal file, ", K(ret), K(fd));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fsync(static_cast<int32_t>(fd.second_id_)))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to fsync, ", K(ret), K(sys_ret), K(fd), KERRMSG);
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::fdatasync(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid fd, not normal file, ", K(ret), K(fd));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fdatasync(static_cast<int32_t>(fd.second_id_)))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to fdatasync, ", K(ret), K(sys_ret), K(fd), KERRMSG);
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::fallocate(
    const ObIOFd &fd,
    mode_t mode,
    const int64_t offset,
    const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::fallocate(static_cast<int32_t>(fd.second_id_), mode, offset, len))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "fail to fallocate", K(ret), K(sys_ret), K(fd), K(offset), K(len), KERRMSG);
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::lseek(
    const ObIOFd &fd,
    const int64_t offset,
    const int whence,
    int64_t &result_offset)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else if (-1 == (result_offset = ::lseek(static_cast<int32_t>(fd.second_id_), offset, whence))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "fail to lseek", K(ret), K(fd), K(offset), K(errno), KERRMSG);
  }
  return ret;
}

int ObIODeviceLocalFileOp::truncate(const char *pathname, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(pathname));
  } else {
    int sys_ret = 0;
    if (0 != (sys_ret = ::truncate(pathname, len))) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "fail to ftruncate", K(ret), K(sys_ret), K(pathname), K(len), KERRMSG);
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::exist(const char *pathname, bool &is_exist)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(pathname));
  } else {
    if (0 != ::access(pathname, F_OK)) {
      if (errno == ENOENT) {
        ret = OB_SUCCESS;
        is_exist = false;
      } else {
        ret = convert_sys_errno();
        SHARE_LOG(WARN, "Fail to access file, ", K(ret), K(pathname), K(errno), KERRMSG);
      }
    } else {
      is_exist = true;
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::stat(const char *pathname, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(pathname) || STRLEN(pathname) == 0) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid argument", K(ret), KP(pathname));
  } else {
    struct stat buf;
    if (0 != ::stat(pathname, &buf)) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to stat file, ", K(ret), K(pathname), K(errno), KERRMSG);
    } else {
      statbuf.rdev_ = static_cast<uint64_t>(buf.st_rdev);
      statbuf.dev_ = static_cast<uint64_t>(buf.st_dev);
      statbuf.inode_ = static_cast<uint64_t>(buf.st_ino);
      statbuf.mask_ = 0; // local file system stat does not offer mask
      statbuf.mode_ = static_cast<uint32_t>(buf.st_mode);
      statbuf.nlink_ = static_cast<uint32_t>(buf.st_nlink);
      statbuf.uid_ = static_cast<uint64_t>(buf.st_uid);
      statbuf.gid_ = static_cast<uint64_t>(buf.st_gid);
      statbuf.size_ = static_cast<uint64_t>(buf.st_size);
      statbuf.block_cnt_ = static_cast<uint64_t>(buf.st_blocks);
      statbuf.block_size_ = static_cast<uint64_t>(buf.st_blksize);
      statbuf.atime_s_ = static_cast<int64_t>(buf.st_atim.tv_sec);
      statbuf.mtime_s_ = static_cast<int64_t>(buf.st_mtim.tv_sec);
      statbuf.ctime_s_ = static_cast<int64_t>(buf.st_ctim.tv_sec);
      statbuf.btime_s_ = INT64_MAX; // local file system stat does not offer birth time
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::fstat(const ObIOFd &fd, ObIODFileStat &statbuf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "invalid args, not normal file", K(ret), K(fd));
  } else {
    struct stat buf;
    if (0 != ::fstat(static_cast<int32_t>(fd.second_id_), &buf)) {
      ret = convert_sys_errno();
      SHARE_LOG(WARN, "Fail to stat file, ", K(ret), K(fd), K(errno), KERRMSG);
    } else {
      statbuf.rdev_ = static_cast<uint64_t>(buf.st_rdev);
      statbuf.dev_ = static_cast<uint64_t>(buf.st_dev);
      statbuf.inode_ = static_cast<uint64_t>(buf.st_ino);
      statbuf.mask_ = 0; // local file system stat does not offer mask
      statbuf.mode_ = static_cast<uint32_t>(buf.st_mode);
      statbuf.nlink_ = static_cast<uint32_t>(buf.st_nlink);
      statbuf.uid_ = static_cast<uint64_t>(buf.st_uid);
      statbuf.gid_ = static_cast<uint64_t>(buf.st_gid);
      statbuf.size_ = static_cast<uint64_t>(buf.st_size);
      statbuf.block_cnt_ = static_cast<uint64_t>(buf.st_blocks);
      statbuf.block_size_ = static_cast<uint64_t>(buf.st_blksize);
      statbuf.atime_s_ = static_cast<int64_t>(buf.st_atim.tv_sec);
      statbuf.mtime_s_ = static_cast<int64_t>(buf.st_mtim.tv_sec);
      statbuf.ctime_s_ = static_cast<int64_t>(buf.st_ctim.tv_sec);
      statbuf.btime_s_ = INT64_MAX; // local file system stat does not offer birth time
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::read(
  const ObIOFd &fd,
  void *buf,
  const int64_t size,
  int64_t &read_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "fd is not normal file", K(ret), K(fd));
  } else if (-1 == (read_size = ::read(static_cast<int32_t>(fd.second_id_), buf, size))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "fail to read", K(ret), K(fd), K(size), K(errno), KERRMSG);
  }
  return ret;
}

int ObIODeviceLocalFileOp::write(
  const ObIOFd &fd,
  const void *buf,
  const int64_t size,
  int64_t &write_size)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!fd.is_normal_file())) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "fd is not normal file", K(ret), K(fd));
  } else if (-1 == (write_size = ::write(static_cast<int32_t>(fd.second_id_), buf, size))) {
    ret = convert_sys_errno();
    SHARE_LOG(WARN, "fail to read", K(ret), K(fd), K(size), K(errno), KERRMSG);
  }
  return ret;
}

int ObIODeviceLocalFileOp::pread_impl(
    const int64_t fd,
    void *buf,
    const int64_t size,
    const int64_t offset,
    int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  char *buffer = static_cast<char *>(buf);
  int64_t read_sz = size;
  int64_t read_offset = offset;
  ssize_t sz = 0;
  while (OB_SUCC(ret) && read_sz > 0) {
    sz = ::pread(static_cast<int32_t>(fd), buffer, read_sz, read_offset);
    if (sz < 0) {
      if (EINTR == errno) {
        SHARE_LOG(INFO, "pread is interrupted before any data is read, just retry", K(errno), KERRMSG);
      } else {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        SHARE_LOG(WARN, "failed to pread", K(ret), K(fd), K(read_sz), K(read_offset), K(errno), KERRMSG);
      }
    } else if (0 == sz) {
      // actual read size is 0, may reach the end of file
      SHARE_LOG(INFO, "read nothing because the end of file is reached", K(ret), K(read_sz), K(read_offset));
      break;
    } else {
      buffer += sz;
      read_sz -= sz;
      read_offset += sz;
      read_size += sz;
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::pwrite_impl(
    const int64_t fd,
    const void *buf,
    const int64_t size,
    const int64_t offset,
    int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  const char *buffer = static_cast<const char *>(buf);
  int64_t write_sz = size;
  int64_t write_offset = offset;
  ssize_t sz = 0;
  while (OB_SUCC(ret) && write_sz > 0) {
    sz = ::pwrite(static_cast<int32_t>(fd), buffer, write_sz, write_offset);
    if (sz <= 0) {
      // if physical end of medium is reached and there's no space for any byte, EFBIG is set
      // and we think pwrite will never return 0
      if (EINTR == errno) {
        SHARE_LOG(INFO, "pwrite is interrupted before any data is written, just retry", K(errno), KERRMSG);
      } else {
        ret = ObIODeviceLocalFileOp::convert_sys_errno();
        SHARE_LOG(WARN, "failed to pwrite", K(ret), K(fd), KP(buffer), K(write_sz), K(write_offset), K(errno), KERRMSG);
      }
    } else {
      buffer += sz;
      write_sz -= sz;
      write_offset += sz;
      write_size += sz;
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::convert_sys_errno()
{
  return ObIODeviceLocalFileOp::convert_sys_errno(errno);
}

// Notes: error_no is a positive value
int ObIODeviceLocalFileOp::convert_sys_errno(const int error_no)
{
  int ret = OB_IO_ERROR;
  bool use_warn_log = false;
  switch (error_no) {
    case EACCES:
      ret = OB_FILE_OR_DIRECTORY_PERMISSION_DENIED;
      break;
    case ENOENT:
      ret = OB_NO_SUCH_FILE_OR_DIRECTORY;
      break;
    case EEXIST:
    case ENOTEMPTY:
      ret = OB_FILE_OR_DIRECTORY_EXIST;
      break;
    case ETIMEDOUT:
      ret = OB_TIMEOUT;
      break;
    case EAGAIN:
      ret = OB_EAGAIN;
      break;
    case EDQUOT:
    case ENOSPC:
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      break;
    case EMFILE:
      ret = OB_TOO_MANY_OPEN_FILES;
      break;
    default:
      use_warn_log = true;
      break;
  }
  if (use_warn_log) {
    SHARE_LOG(WARN, "convert sys errno", K(ret), K(error_no), KERRNOMSG(error_no));
  } else {
    SHARE_LOG(INFO, "convert sys errno", K(ret), K(error_no), KERRNOMSG(error_no));
  }
  return ret;
}

int ObIODeviceLocalFileOp::get_block_file_size(
    const char *sstable_dir,
    const int64_t reserved_size,
    const int64_t block_size,
    const int64_t suggest_file_size,
    const int64_t disk_percentage,
    int64_t &block_file_size)
{
  int ret = OB_SUCCESS;
  // check space availability if file not exist
  const int64_t old_block_file_size = block_file_size;
  if (OB_ISNULL(sstable_dir)
      || OB_UNLIKELY(reserved_size < 0)
      || OB_UNLIKELY(block_size <= 0)
      || OB_UNLIKELY(suggest_file_size < 0)
      || OB_UNLIKELY(disk_percentage < 0)
      || OB_UNLIKELY(disk_percentage >= 100)
      || OB_UNLIKELY(block_file_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(sstable_dir), K(reserved_size), K(block_size),
        K(suggest_file_size), K(disk_percentage), K(block_file_size));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::compute_block_file_size(sstable_dir, reserved_size, block_size,
                     suggest_file_size, disk_percentage, block_file_size))) {
    LOG_WARN("fail to compute block file size", KR(ret), K(sstable_dir), K(reserved_size),
             K(block_size), K(suggest_file_size), K(disk_percentage), K(block_file_size));
  } else if (OB_FAIL(ObIODeviceLocalFileOp::check_disk_space_available(sstable_dir,
                     block_file_size, reserved_size, old_block_file_size, (0 == block_file_size)))) {
    LOG_WARN("fail to check disk space available", KR(ret), K(sstable_dir),
             K(block_file_size), K(reserved_size), K(old_block_file_size));
  }
  return ret;
}

int ObIODeviceLocalFileOp::compute_block_file_size(
    const char *sstable_dir,
    const int64_t reserved_size,
    const int64_t block_size,
    const int64_t suggest_file_size,
    const int64_t disk_percentage,
    int64_t &block_file_size)
{
  int ret = OB_SUCCESS;
  int64_t total_space = 0;
  struct statvfs svfs;
  if (OB_ISNULL(sstable_dir)
      || OB_UNLIKELY(reserved_size < 0)
      || OB_UNLIKELY(block_size <= 0)
      || OB_UNLIKELY(suggest_file_size < 0)
      || OB_UNLIKELY(disk_percentage < 0)
      || OB_UNLIKELY(disk_percentage >= 100)
      || OB_UNLIKELY(block_file_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(sstable_dir), K(reserved_size), K(block_size),
        K(suggest_file_size), K(disk_percentage), K(block_file_size));
  } else if (OB_UNLIKELY(0 != statvfs(sstable_dir, &svfs))) {
    ret = ObIODeviceLocalFileOp::convert_sys_errno();
    SHARE_LOG(WARN, "Failed to get disk space ", K(ret), K(sstable_dir));
  } else {
    // remove reserved space for root user
    total_space = std::max(0L, (int64_t)((svfs.f_blocks + svfs.f_bavail - svfs.f_bfree) * svfs.f_bsize - reserved_size));
    block_file_size = suggest_file_size > 0 ? suggest_file_size : total_space * disk_percentage / 100;
    if (block_file_size <= block_size) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(ERROR, "data file size is too small, ", K(ret), K(total_space),
          K(reserved_size), K(block_file_size));
    } else {
      block_file_size = lower_align(block_file_size, block_size);
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::check_disk_space_available(
    const char *sstable_dir,
    const int64_t data_disk_size,
    const int64_t reserved_size,
    const int64_t used_disk_size,
    const bool need_report_user_error)
{
  int ret = OB_SUCCESS;
  struct statvfs svfs;
  if (OB_ISNULL(sstable_dir)
      || OB_UNLIKELY(reserved_size < 0)
      || OB_UNLIKELY(data_disk_size < 0)
      || OB_UNLIKELY(used_disk_size < 0)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "Invalid argument, ", K(ret), KP(sstable_dir),
              K(reserved_size), K(data_disk_size), K(used_disk_size));
  } else if (OB_UNLIKELY(0 != statvfs(sstable_dir, &svfs))) {
    ret = ObIODeviceLocalFileOp::convert_sys_errno();
    SHARE_LOG(WARN, "Failed to get disk space ", K(ret), K(sstable_dir));
  } else {
    // check disk space availability for datafile_size, must satisfy datafile_size < used_disk_size + disk_free_space
    const int64_t free_space = std::max(0L, (int64_t)(svfs.f_bavail * svfs.f_bsize - reserved_size));
    if (data_disk_size > used_disk_size + free_space) {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
      if (need_report_user_error) {
        LOG_DBA_ERROR(OB_SERVER_OUTOF_DISK_SPACE, "msg", "data file size is too large", K(ret),
            K(free_space), K(reserved_size), K(used_disk_size), K(data_disk_size));
      } else {
        LOG_DBA_WARN(OB_SERVER_OUTOF_DISK_SPACE, "msg", "data file size is too large", K(ret),
            K(free_space), K(reserved_size), K(used_disk_size), K(data_disk_size));
      }
    }
  }
  return ret;
}

int ObIODeviceLocalFileOp::open_block_file(
    const char *store_dir,
    const char *sstable_dir,
    const int64_t block_size,
    const int64_t file_size,
    const int64_t disk_percentage,
    const int64_t reserved_size,
    bool &is_exist,
    BlockFileAttr &block_file_attr)
{
  int ret = OB_SUCCESS;
  int sys_ret = 0;
  is_exist = false;
  int64_t adjust_file_size = 0; // The original data file size is 0 because of the first initialization.

  if (OB_FAIL(databuff_printf(block_file_attr.store_path_, OB_MAX_FILE_NAME_LENGTH, "%s/%s/%s",
    store_dir, block_file_attr.block_sstable_dir_name_, block_file_attr.block_sstable_file_name_))) {
    SHARE_LOG(WARN, "The block file path is too long, ", K(ret), K(store_dir));
  } else if (OB_FAIL(exist(block_file_attr.store_path_, is_exist))) {
    SHARE_LOG(WARN, "Fail to check if file exist, ", K(ret), "store_path", block_file_attr.store_path_);
  } else if (!is_exist
      && OB_FAIL(get_block_file_size(sstable_dir, reserved_size, block_size,
          file_size, disk_percentage, adjust_file_size))) {
    SHARE_LOG(WARN, "Fail to get block file size", K(ret), K(block_size), K(file_size),
        K(adjust_file_size), K(disk_percentage));
  } else {
    int open_flag = is_exist ? O_DIRECT | O_RDWR | O_LARGEFILE
                          : O_CREAT | O_EXCL | O_DIRECT | O_RDWR | O_LARGEFILE;
    if ((block_file_attr.block_fd_ = ::open(block_file_attr.store_path_, open_flag,
                                            S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH)) < 0) {
      ret = OB_IO_ERROR;
      SHARE_LOG(ERROR, "open file error", K(ret), "store_path", block_file_attr.store_path_, K(errno), KERRMSG);
    } else {
      if (!is_exist) {
        if (0 != (sys_ret = ::fallocate(block_file_attr.block_fd_, 0/*MODE*/, 0/*offset*/, adjust_file_size))) {
          ret = ObIODeviceLocalFileOp::convert_sys_errno();
          SHARE_LOG(ERROR, "Fail to fallocate block file, ", K(ret), K(sys_ret), "store_path",
                    block_file_attr.store_path_, K(adjust_file_size), KERRMSG);
        } else {
          block_file_attr.block_file_size_ = adjust_file_size;
        }
      } else {
        ObIODFileStat f_stat;
        if (OB_FAIL(stat(block_file_attr.store_path_, f_stat))) {
          SHARE_LOG(ERROR, "stat store_path_ fail ", K(ret), "store_path", block_file_attr.store_path_);
        } else {
          block_file_attr.block_file_size_ = lower_align(f_stat.size_, block_size);
        }
      }
    }

    if (OB_SUCC(ret)) {
      const ObMemAttr mem_attr(OB_SERVER_TENANT_ID, block_file_attr.device_name_);
      block_file_attr.total_block_cnt_ = block_file_attr.block_file_size_ / block_file_attr.block_size_;
      if (OB_ISNULL(block_file_attr.free_block_array_ = (int64_t *) ob_malloc(
                    sizeof(int64_t) * block_file_attr.total_block_cnt_, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret), "block_file_size",
            block_file_attr.block_file_size_, "total_block_cnt", block_file_attr.total_block_cnt_);
      } else if (OB_ISNULL(block_file_attr.block_bitmap_ = (bool *) ob_malloc(
                           sizeof(bool) * block_file_attr.total_block_cnt_, mem_attr))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        SHARE_LOG(WARN, "Fail to allocate memory, ", K(ret), "block_file_size",
            block_file_attr.block_file_size_, "total_block_cnt", block_file_attr.total_block_cnt_);
      } else {
        MEMSET(block_file_attr.block_bitmap_, 1, sizeof(bool) * block_file_attr.total_block_cnt_);
        block_file_attr.free_block_cnt_ = 0;
        block_file_attr.free_block_push_pos_ = 0;
        block_file_attr.free_block_pop_pos_ = 0;
      }
    }
  }

  return ret;
}

} // namespace share
} // namespace oceanbase

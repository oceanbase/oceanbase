/**
 * Copyright (c) 2021 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX SHARE

#include "share/object_storage/ob_device_connectivity.h"
#include "share/config/ob_server_config.h"
#include "share/backup/ob_backup_io_adapter.h"

namespace oceanbase
{
namespace share
{

using namespace oceanbase::common;

/************************* ObDeviceConnectivityCheckManager *************************/
int ObDeviceConnectivityCheckManager::check_device_connectivity(const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  ObDeviceCheckFile check_file;
  if (OB_UNLIKELY(!storage_dest.is_valid() || !storage_dest.get_storage_info()->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("storage_dest is invalid", KR(ret), K(storage_dest));
  } else if (OB_FAIL(check_file.check_io_permission(storage_dest))) {
    LOG_WARN("fail to check io permission", KR(ret), K(storage_dest));
  } else {
    LOG_INFO("succ to check device connectivity", K(storage_dest));
  }
  return ret;
}

/****************************** ObDeviceCheckFile ******************************/
const char ObDeviceCheckFile::OB_STR_CONNECTIVITY_CHECK[] = "connectivity_check";
const char ObDeviceCheckFile::OB_SS_SUFFIX[] = ".obss";

int ObDeviceCheckFile::delete_permission_check_file(const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  ObArray<ObIODirentEntry> d_entrys;
  char check_file_prefix[OB_MAX_DEVICE_CHECK_FILE_NAME_LENGTH] = { 0 };
  ObBackupIoAdapter io_adapter;
  uint64_t server_id_in_GCONF = GCONF.observer_id;
  char path[OB_MAX_URI_LENGTH];

  if (OB_UNLIKELY(!storage_dest.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest));
  } else if (OB_FAIL(databuff_printf(check_file_prefix, sizeof(check_file_prefix), "%s_%lu_%s",
      GCONF.zone.str(), server_id_in_GCONF, "permission"))) {
    LOG_WARN("fail to get check file prefix", KR(ret));
  } else if (OB_FAIL(get_check_file_path_(storage_dest, path))) {
    LOG_WARN("failed to get check file path", KR(ret), K(storage_dest));
  } else {
    ObDirPrefixEntryFilter prefix_op(d_entrys);
    if (OB_FAIL(prefix_op.init(check_file_prefix, static_cast<int32_t>(STRLEN(check_file_prefix))))) {
      LOG_WARN("failed to init dir prefix", KR(ret), K(check_file_prefix));
    } else if (OB_FAIL(io_adapter.adaptively_list_files(path, storage_dest.get_storage_info(), prefix_op))) {
      LOG_WARN("failed to list files", KR(ret));
    } else {
      char del_file_path[OB_MAX_BACKUP_DEST_LENGTH];
      ObIODirentEntry tmp_entry;
      for (int64_t i = 0; OB_SUCC(ret) && i < d_entrys.count(); ++i) {
        tmp_entry = d_entrys.at(i);
        if (OB_ISNULL(tmp_entry.name_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("file name is null", KR(ret));
        } else if (OB_FAIL(databuff_printf(del_file_path, sizeof(del_file_path),
            "%s/%s", path, tmp_entry.name_))) {
          LOG_WARN("failed to set delete file path", KR(ret), K(path), K_(tmp_entry.name));
        } else {
          ObString uri(del_file_path);
          if(OB_FAIL(io_adapter.adaptively_del_file(uri, storage_dest.get_storage_info()))) {
            LOG_WARN("failed to delete permission check file", KR(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObDeviceCheckFile::check_io_permission(const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  const int64_t write_size = 64;
  char buf_write[write_size];
  char *buf_read = nullptr;
  int64_t pos = 0;
  int64_t file_len = 0;
  int64_t read_size = 0;
  char path[OB_MAX_BACKUP_DEST_LENGTH];
  bool write_ok = false;
  ObArenaAllocator allocator;
  if (!storage_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest));
  } else if (OB_FAIL(get_permission_check_file_path_(storage_dest, false/*is_appender*/, path))) {
      LOG_WARN("failed to get permission check file path", KR(ret), K(storage_dest));
  } else if (OB_FAIL(databuff_printf(buf_write, write_size, "writer at %ld",
                                     ObTimeUtility::current_time()))) {
    LOG_WARN("fail to set buf_write", KR(ret));
  } else if (OB_FAIL(io_adapter.write_single_file(path, storage_dest.get_storage_info(),
                     buf_write, write_size, ObStorageIdMod::get_default_id_mod()))) {
    if (is_permission_error_(ret)) {
      ret = OB_OBJECT_STORAGE_PERMISSION_DENIED;
    }
    LOG_WARN("fail to write single file", KR(ret), K(path), K(storage_dest));
  } else if (FALSE_IT(write_ok = true)
      || OB_FAIL(io_adapter.adaptively_get_file_length(path, storage_dest.get_storage_info(), file_len))) {
    if (is_permission_error_(ret)) {
      ret = OB_OBJECT_STORAGE_PERMISSION_DENIED;
    }
    LOG_WARN("fail to get file length", KR(ret), K(path), K(storage_dest));
  } else if (OB_ISNULL(buf_read = reinterpret_cast<char*>(allocator.alloc(file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc buf_read", KR(ret), K(file_len));
  } else if (OB_FAIL(io_adapter.adaptively_read_single_file(path, storage_dest.get_storage_info(), buf_read,
                     file_len, read_size, ObStorageIdMod::get_default_id_mod()))) {
    if (is_permission_error_(ret)) {
      ret = OB_OBJECT_STORAGE_PERMISSION_DENIED;
    }
    LOG_WARN("fail to read single file", KR(ret), K(path), K(storage_dest));
  }
  if (write_ok && (OB_SUCCESS != (tmp_ret = io_adapter.adaptively_del_file(path, storage_dest.get_storage_info())))) {
    if (is_permission_error_(tmp_ret)) {
      tmp_ret = OB_OBJECT_STORAGE_PERMISSION_DENIED;
    }
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    LOG_WARN("fail to del file", KR(tmp_ret), KR(ret), K(path), K(storage_dest));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(check_appender_permission_(storage_dest))) {
    if (is_permission_error_(ret)) {
      ret = OB_OBJECT_STORAGE_PERMISSION_DENIED;
    }
    LOG_WARN("fail to check appender permission", KR(ret), K(storage_dest));
  }

  return ret;
}

int ObDeviceCheckFile::get_check_file_path_(const ObBackupDest &storage_dest, char *path)
{
  int ret = OB_SUCCESS;
  const int64_t cluster_id = GCONF.cluster_id;
  const uint64_t server_id = GCONF.observer_id;
  if (OB_UNLIKELY(!storage_dest.is_valid()) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), K(path));
  } else if (OB_FAIL(databuff_printf(path, OB_MAX_BACKUP_DEST_LENGTH, "%s/%s_%ld/%s_%lu/%s",
             storage_dest.get_root_path().ptr(), "cluster", cluster_id, "server", server_id,
             OB_STR_CONNECTIVITY_CHECK))) {
    LOG_WARN("fail to construct check file path", KR(ret), K(storage_dest));
  }
  return ret;
}

int ObDeviceCheckFile::get_permission_check_file_path_(
    const ObBackupDest &storage_dest,
    const bool is_appender,
    char *path)
{
  int ret = OB_SUCCESS;
  int64_t check_time_s = ObTimeUtility::current_time() / 1000 / 1000;
  char buf[OB_STORAGE_MAX_TIME_STR_LEN] = { 0 };
  const char *prefix = is_appender ? "append" : "put";
  uint64_t server_id_in_GCONF = GCONF.observer_id;
  int64_t pos = 0;
  const int64_t cur_us = ObTimeUtility::fast_current_time();
  if (OB_UNLIKELY(!storage_dest.is_valid()) || OB_ISNULL(path)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(storage_dest), K(path));
  } else if (OB_FAIL(get_check_file_path_(storage_dest, path))) {
    LOG_WARN("fail to get check file path", KR(ret), K(storage_dest));
  } else if (OB_FAIL(storage_time_to_strftime_(check_time_s, buf, sizeof(buf), pos, 'T'/* concat */))) {
    LOG_WARN("fail to convert time", KR(ret), K(storage_dest));
  } else if (OB_FAIL(databuff_printf(path + STRLEN(path), OB_MAX_BACKUP_DEST_LENGTH - STRLEN(path),
      "/%s_%lu_%s_%s_%s%s_%ld", GCONF.zone.str(), server_id_in_GCONF, "permission", prefix, buf, OB_SS_SUFFIX, cur_us))) {
    LOG_WARN("fail to set permission file name", KR(ret), K(buf));
  }
  return ret;
}

int ObDeviceCheckFile::check_appender_permission_(const ObBackupDest &storage_dest)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter io_adapter;
  int64_t write_size = 0;
  ObIODevice *device_handle = NULL;
  ObIODOpt iod_opt_array[DEFAULT_OPT_ARG_NUM];
  ObIODOpts iod_opts;
  iod_opts.opts_ = iod_opt_array;
  iod_opts.opt_cnt_ = 0;
  bool is_data_file = true;
  int64_t epoch = -1;
  ObIOFd fd;
  const static int64_t BUF_LENGTH = 64;
  char data[BUF_LENGTH];
  char path[OB_MAX_BACKUP_DEST_LENGTH];
  if (OB_FAIL(get_permission_check_file_path_(storage_dest, true/*is_appender*/, path))) {
    LOG_WARN("fail to get permission check file path", KR(ret), K(storage_dest));
  } else if (OB_FAIL(io_adapter.set_access_type(&iod_opts, true/*is_appender*/, DEFAULT_OPT_ARG_NUM))) {
    LOG_WARN("fail to set access type");
  } else if (OB_FAIL(io_adapter.set_append_strategy(&iod_opts, is_data_file, epoch, DEFAULT_OPT_ARG_NUM))) {
    LOG_WARN("fail to set append strategy");
  } else if (OB_FAIL(io_adapter.get_and_init_device(device_handle, storage_dest.get_storage_info(), path,
                                                    ObStorageIdMod::get_default_id_mod()))) {
    LOG_WARN( "fail to get device", KR(ret));
  }
  // flag=-1 and mode=0 are invalid, because oss/cos unused flag and mode;
  else if (OB_FAIL(device_handle->open(path, -1/* flag */, 0/* mode */, fd, &iod_opts))) {
    LOG_WARN("fail to open file", KR(ret), K(path));
  } else if (OB_FAIL(databuff_printf(data, sizeof(data), "appender writer at %ld",
                                     ObTimeUtility::current_time()))) {
    LOG_WARN("fail to set data", KR(ret), K(path));
  } else if (OB_FAIL(device_handle->pwrite(fd, 0, strlen(data), data, write_size))) {
    LOG_WARN("fail to write file", KR(ret), K(path), K(data));
  } else if (OB_FAIL(io_adapter.adaptively_del_file(path, storage_dest.get_storage_info()))) {
    LOG_WARN("fail to del file", KR(ret));
  }

  if (OB_SUCCESS != (tmp_ret = io_adapter.close_device_and_fd(device_handle, fd))) {
    ret = (OB_SUCCESS == ret) ? tmp_ret : ret;
    LOG_WARN("fail to close file and release device!", KR(tmp_ret));
  }

  return ret;
}

bool ObDeviceCheckFile::is_permission_error_(const int result)
{
  return ((OB_IO_ERROR == result) || (OB_OBJECT_STORAGE_IO_ERROR == result));
}

int ObDeviceCheckFile::storage_time_to_strftime_(
    const int64_t &ts_s, char *buf, const int64_t buf_len, int64_t &pos, const char concat)
{
  int ret = OB_SUCCESS;
  ObSqlString format;
  struct tm lt;
  int64_t strftime_len = 0;
  time_t t = static_cast<time_t>(ts_s);

  (void) localtime_r(&t, &lt);
  if (OB_FAIL(format.assign("%Y%m%d"))) {
    LOG_WARN("failed to build format string", KR(ret), K(concat));
  } else if (OB_FAIL(format.append_fmt("%c", concat))) {
    LOG_WARN("failed to build format string", KR(ret), K(concat));
  } else if (OB_FAIL(format.append("%H%M%S"))) {
    LOG_WARN("failed to build format string", KR(ret), K(concat));
  } else if (0 == (strftime_len = strftime(buf + pos, buf_len - pos, format.ptr(), &lt))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to convert timestamp to string", KR(ret), K(ts_s), KP(buf), K(buf_len),
             K(pos), K(concat), K(format));
  } else {
    pos += strftime_len;
  }

  return ret;
}

/****************************** ObDirPrefixEntryFilter ******************************/
int ObDirPrefixEntryFilter::init(
    const char *filter_str,
    const int32_t filter_str_len)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    OB_LOG(WARN, "init twice", KR(ret));
  } else if (OB_ISNULL(filter_str) || OB_UNLIKELY((0 == filter_str_len))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret), K(filter_str), K(filter_str_len));
  } else if (OB_UNLIKELY(filter_str_len > (sizeof(filter_str_) - 1))) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "the length of dir prefix is too long", KR(ret), K(filter_str_len));
  } else if (OB_FAIL(databuff_printf(filter_str_, sizeof(filter_str_), "%.*s",
                                     filter_str_len, filter_str))) {
    OB_LOG(WARN, "fail to init filter_str", KR(ret), K(filter_str), K(filter_str_len));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObDirPrefixEntryFilter::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    OB_LOG(WARN, "dir prefix filter not init", KR(ret));
  } else if (OB_ISNULL(entry) || OB_ISNULL(entry->d_name)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", KR(ret));
  } else if (STRLEN(entry->d_name) < STRLEN(filter_str_)) {
    // do nothing
  } else if (0 == STRNCMP(entry->d_name, filter_str_, STRLEN(filter_str_))) {
    ObIODirentEntry p_entry(entry->d_name, entry->d_type);
    if (OB_FAIL(d_entrys_.push_back(p_entry))) {
      OB_LOG(WARN, "fail to push back directory entry", KR(ret), K(p_entry), K_(filter_str));
    }
  }
  return ret;
}

}  // namespace share
}  // namespace oceanbase

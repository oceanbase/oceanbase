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

#include "ob_admin_test_io_device_executor.h"
#include "src/logservice/archiveservice/ob_archive_file_utils.h"
#include "src/share/io/ob_io_manager.h"
#include "src/share/ob_device_manager.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase {
namespace tools {
ObAdminTestIODeviceExecutor::ObAdminTestIODeviceExecutor()
  : is_quiet_(false),
    test_id_(1),
    allocator_()
{
  MEMSET(backup_path_, 0, common::OB_MAX_URI_LENGTH);
  MEMSET(storage_info_, 0, common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH);
}

ObAdminTestIODeviceExecutor::~ObAdminTestIODeviceExecutor()
{}

int ObAdminTestIODeviceExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  lib::set_memory_limit(4 * 1024 * 1024 * 1024LL);
  lib::set_tenant_memory_limit(500, 4 * 1024 * 1024 * 1024LL);

  ObTenantBase *tenant_base = new ObTenantBase(OB_SERVER_TENANT_ID);
  ObMallocAllocator *malloc = ObMallocAllocator::get_instance();
  if (OB_ISNULL(malloc->get_tenant_ctx_allocator(OB_SERVER_TENANT_ID, 0))) {
    if (OB_FAIL(malloc->create_and_add_tenant_allocator(OB_SERVER_TENANT_ID))) {
      STORAGE_LOG(WARN, "failed to create_and_add_tenant_allocator", K(ret));
    }
  }

  if (FAILEDx(tenant_base->init())) {
    STORAGE_LOG(WARN, "failed to init tenant base", K(ret));
  } else if (FALSE_IT(ObTenantEnv::set_tenant(tenant_base))) {
  } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
    STORAGE_LOG(WARN, "init device manager failed", KR(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().init())) {
    STORAGE_LOG(WARN, "failed to init io manager", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    STORAGE_LOG(WARN, "failed to start io manager", K(ret));
  }

  if (FAILEDx(parse_cmd_(argc, argv))) {
    STORAGE_LOG_FILTER(ERROR, "failed to parse cmd", K(ret), K(argc), K(argv));
  } else if (is_quiet_) {
    OB_LOGGER.set_log_level("WARN");
  } else {
    OB_LOGGER.set_log_level("INFO");
  }

  if(OB_FAIL(ret)) {
  } else if (OB_FAIL(run_all_tests_())) {
    STORAGE_LOG(ERROR, "failed to pass all tests", K(ret), K_(backup_path));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  int opt = 0;
  int index = -1;
  const char *opt_str = "h:d:s:q:e:f:i:";
  struct option longopts[] = {{"help", 0, NULL, 'h'},
      {"backup_path", 1, NULL, 'd'},
      {"storage_info", 1, NULL, 's'},
      {"quiet", 0, NULL, 'q' },
      {"s3_url_encode_type", 0, NULL, 'e'},
      {"trigger_freq", 0, NULL, 'f'}, // used for internal testing only
      {"sts_credential", 0, NULL, 'i'},
      {NULL, 0, NULL, 0}};
  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, opt_str, longopts, &index))) {
    switch (opt) {
      case 'h': {
        print_usage_();
        exit(1);
      }
      case 'd': {
        time_t timestamp = time(NULL);
        struct tm *timeinfo = localtime(&timestamp);
        char buffer[OB_MAX_TIME_STR_LENGTH];
        strftime(buffer, sizeof(buffer), "%Y-%m-%d-%H:%M:%S", timeinfo);
        if (OB_FAIL(databuff_printf(backup_path_, sizeof(backup_path_), "%s%s", optarg, buffer))) {
          STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
        }
        break;
      }
      case 's': {
         if (OB_FAIL(databuff_printf(storage_info_, sizeof(storage_info_), "%s", optarg))) {
          STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
        }
        break;
      }
      case 'q': {
        is_quiet_ = true;
        break;
      }
      case 'e': {
        if (OB_FAIL(set_s3_url_encode_type(optarg))) {
          STORAGE_LOG_FILTER(ERROR, "failed to set s3 url encode type", KR(ret));
        }
        break;
      }
      case 'f': {
        int tmp_ret = OB_SUCCESS;
        int64_t trigger_freq = 0;
        if (OB_TMP_FAIL(c_str_to_int(optarg, trigger_freq))) {
          OB_LOG(WARN, "fail to parse trigger freq", KR(tmp_ret), K((char *)optarg));
        } else if (OB_UNLIKELY(trigger_freq < 0)) {
          tmp_ret = OB_INVALID_ARGUMENT;
          OB_LOG(WARN, "invalid trigger freq", KR(tmp_ret), K((char *)optarg));
        } else {
          TP_SET_EVENT(EventTable::EN_OBJECT_STORAGE_IO_RETRY,
              OB_OBJECT_STORAGE_IO_ERROR, 0, trigger_freq);
        }
        break;
      }
      case 'i': {
        if (OB_FAIL(set_sts_credential_key(optarg))) {
          STORAGE_LOG(WARN, "failed to set sts credential", KR(ret));
        }
        break;
      }
      default: {
        print_usage_();
        exit(1);
      }
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::run_all_tests_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(test_backup_dest_connectivity_())) {
    STORAGE_LOG(ERROR, "test backup dest connectivity failed", K(ret), K_(backup_path));
  } else if (OB_FAIL(test_archive_log_())) {
    STORAGE_LOG(ERROR, "test archive log failed", K(ret), K_(backup_path));
  } else if (OB_FAIL(test_backup_data_())) {
    STORAGE_LOG(ERROR, "test backup data failed", K(ret), K_(backup_path));
  } else if (OB_FAIL(test_consume_clog_file_())) {
    STORAGE_LOG(ERROR, "test consume clog file failed", K(ret));
  } else if (OB_FAIL(test_clean_backup_file_())) {
    STORAGE_LOG(ERROR, "test clean backup file failed", K(ret), K_(backup_path));
  } else if (OB_FAIL(test_object_storage_interface_())) {
    STORAGE_LOG(ERROR, "test object storage interface failed", K(ret), K_(backup_path));
  } else if (OB_FAIL(test_list_before_complete_multipart_write_())) {
    STORAGE_LOG(ERROR, "test list before complete multipart write failed", K(ret), K_(backup_path));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_backup_dest_connectivity_()
{
  int ret = OB_SUCCESS;
  TestExecGuard guard("Backup dest connectivity", test_id_, ret);
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  bool is_empty_directory = false;

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(util.is_empty_directory(backup_path_, &storage_info, is_empty_directory))) {
    STORAGE_LOG_FILTER(ERROR, "The path is inaccessible, please check the path and the storage info parameter", K(ret), K_(backup_path));
  } else if (!is_empty_directory) {
    ret = OB_INVALID_BACKUP_DEST;
    STORAGE_LOG_FILTER(ERROR, "Path not empty directory", K(ret), K_(backup_path));
  } else {
    const char* check_file_dir_name = "check_file/";
    char check_file_dir[OB_MAX_URI_LENGTH] = { 0 };

    if (OB_FAIL(databuff_printf(check_file_dir, OB_MAX_URI_LENGTH,
                                "%s%s%s", backup_path_, "/", check_file_dir_name))) {
      STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
    } else if (OB_FAIL(util.mkdir(check_file_dir, &storage_info))) {
      STORAGE_LOG_FILTER(ERROR, "failed to make check file dir", K(ret), K(check_file_dir));
    } else if (OB_FAIL(test_normal_check_file_(check_file_dir))) {
      STORAGE_LOG_FILTER(ERROR, "test normal check file failed", K(ret));
    } else if (OB_FAIL(test_appendable_check_file_(check_file_dir))) {
      STORAGE_LOG_FILTER(ERROR, "test appendable check file failed", K(ret));
    } else if (OB_FAIL(test_multipart_upload_check_file_(check_file_dir))) {
      STORAGE_LOG_FILTER(ERROR, "test multipart upload check file failed", K(ret));
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_normal_check_file_(const char* check_file_dir)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  const char* check_file_name = "check_file.obbak";
  const char* check_file_content = "normal check file";
  char check_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char *read_file_buf = nullptr;
  int64_t check_file_len = 0;
  int64_t real_len = strlen(check_file_content);
  int64_t read_size = 0;
  bool is_exist = false;
  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(check_file_path, OB_MAX_URI_LENGTH,
                            "%s%s", check_file_dir, check_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.write_single_file(check_file_path, &storage_info, check_file_content,
                          real_len, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write check file", K(ret), K(check_file_path));
  } else if (OB_FAIL(util.is_exist(check_file_path, &storage_info, is_exist))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if normal check file is exist", K(ret), K(check_file_path));
  } else if (!is_exist) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "normal check file does not exist!", K(ret), K(check_file_path));
  } else if (OB_FAIL(util.get_file_length(check_file_path, &storage_info, check_file_len))) {
    STORAGE_LOG_FILTER(ERROR, "failed to get check file length", K(ret), K(check_file_path));
  } else if (real_len != check_file_len) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "get check file length does not match real length", K(ret), K(check_file_path), K(real_len), K(check_file_len));
  } else if (OB_ISNULL(read_file_buf = reinterpret_cast<char*>(allocator_.alloc(check_file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(check_file_path), K(check_file_len));
  } else if (OB_FAIL(util.read_single_file(check_file_path, &storage_info, read_file_buf,
                     check_file_len, read_size, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to read check file",  K(ret), K(check_file_path), K(check_file_len));
  } else if (read_size != check_file_len) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read check file length does not match real length",  K(ret), K(check_file_path), K(read_size), K(check_file_len));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_appendable_check_file_(const char* check_file_dir)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_APPENDER;
  bool is_exist = false;
  const char* appendable_check_file_name = "appendable_check_file.obbak";
  const char* appendable_check_file_content = "appendable check file";
  char appendable_check_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char *read_file_buf = nullptr;
  int64_t check_file_len = 0;
  int64_t real_len = strlen(appendable_check_file_content);
  int64_t write_size = 0;
  int64_t read_size = 0;
  int64_t offset = 0;

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(appendable_check_file_path,
              OB_MAX_URI_LENGTH, "%s%s", check_file_dir, appendable_check_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, &storage_info,
                appendable_check_file_path, access_type,
                ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret),
                K(appendable_check_file_path), K(access_type));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "device handle is NULL", K(ret), K(device_handle), K_(backup_path));
  } else if (OB_FAIL(device_handle->pwrite(fd, offset, real_len,
                     appendable_check_file_content, write_size))) {
    STORAGE_LOG_FILTER(ERROR, "failed to pwrite appendable check file", K(ret), K(appendable_check_file_path),
                K(appendable_check_file_content), K(real_len));
  } else if (FALSE_IT(offset += real_len)){
  } else if (OB_FAIL(device_handle->pwrite(fd, offset, real_len,
                     appendable_check_file_content, write_size))) {
    STORAGE_LOG_FILTER(ERROR, "failed to repeat pwrite appendable check file", K(ret), K(appendable_check_file_path),
        K(appendable_check_file_content), K(real_len));
  } else if (FALSE_IT(real_len += real_len)) {
  } else if (OB_FAIL(device_handle->seal_file(fd))) {
    STORAGE_LOG_FILTER(ERROR, "failed to seal appendable check file", K(ret), K(appendable_check_file_path));
  } else if (OB_FAIL(device_handle->seal_file(fd))) {
    STORAGE_LOG_FILTER(ERROR, "failed to repeat seal appendable check file", K(ret), K(appendable_check_file_path));
  } else if (OB_FAIL(util.close_device_and_fd(device_handle, fd))) {
    STORAGE_LOG_FILTER(ERROR, "fail to close appendable file and release device!", K(ret), K(appendable_check_file_path));
  } else if (OB_FAIL(util.adaptively_is_exist(appendable_check_file_path, &storage_info, is_exist))) {
    STORAGE_LOG_FILTER(ERROR, "failed to check if normal check file is exist", K(ret), K(appendable_check_file_path));
  } else if (!is_exist) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "appendable check file does not exist!", K(ret), K(appendable_check_file_path));
  } else if (OB_FAIL(util.adaptively_get_file_length(appendable_check_file_path, &storage_info,
                     check_file_len))) {
    STORAGE_LOG_FILTER(ERROR, "fail to get appendable check file length", K(ret), K(appendable_check_file_path));
  } else if (check_file_len != real_len) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "get appendable check file length does not match real length", K(ret), K(appendable_check_file_path),
                K(real_len), K(check_file_len));
  } else if (OB_ISNULL(read_file_buf = reinterpret_cast<char*>(allocator_.alloc(check_file_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(appendable_check_file_path), K(check_file_len));
  } else if (OB_FAIL(util.adaptively_read_single_file(appendable_check_file_path, &storage_info, read_file_buf,
             check_file_len, read_size, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to read appendable check file",  K(ret), K(appendable_check_file_path), K(check_file_len));
  } else if (read_size != check_file_len) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read appendable check file length does not match real length",  K(ret), K(appendable_check_file_path),
                K(read_size), K(check_file_len));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_multipart_upload_check_file_(const char* check_file_dir)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  const char* check_file_name = "multipart_upload_check_file.obbak";
  const char* check_file_content = "multipart upload check file";
  char check_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char *read_file_buf = nullptr;
  int64_t check_file_len = 0;
  int64_t write_size = 0;
  int64_t real_len = strlen(check_file_content);
  int64_t read_size = 0;
  bool is_exist = false;
  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(check_file_path, OB_MAX_URI_LENGTH,
                            "%s%s", check_file_dir, check_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, &storage_info,
                check_file_path, access_type,
                ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret),
                K(check_file_path), K(access_type));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "device handle is NULL", K(ret), K(device_handle), K_(backup_path));
  } else if (OB_FAIL(device_handle->write(fd, check_file_content, real_len, write_size))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write check file", K(ret), K(check_file_content), K(real_len));
  } else if (OB_FAIL(device_handle->complete(fd))) {
    STORAGE_LOG_FILTER(ERROR, "fail to complete multipart upload", K(ret), K(device_handle), K(fd));
  }
  if (OB_SUCCESS != ret) {
    if (OB_TMP_FAIL(device_handle->abort(fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG_FILTER(ERROR, "fail to abort multipart upload", K(ret), K(tmp_ret), K(device_handle), K(fd));
    }
  }
  if (OB_TMP_FAIL(util.close_device_and_fd(device_handle, fd))) {
    ret = COVER_SUCC(tmp_ret);
    STORAGE_LOG_FILTER(ERROR, "fail to close device and fd", K(ret), K(tmp_ret), K(device_handle), K(fd));
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(util.is_exist(check_file_path, &storage_info, is_exist))) {
      STORAGE_LOG_FILTER(ERROR, "failed to check if normal check file is exist", K(ret), K(check_file_path));
    } else if (!is_exist) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "normal check file does not exist!", K(ret), K(check_file_path));
    } else if (OB_FAIL(util.get_file_length(check_file_path, &storage_info, check_file_len))) {
      STORAGE_LOG_FILTER(ERROR, "failed to get check file length", K(ret), K(check_file_path));
    } else if (real_len != check_file_len) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "get check file length does not match real length", K(ret), K(check_file_path), K(real_len), K(check_file_len));
    } else if (OB_ISNULL(read_file_buf = reinterpret_cast<char*>(allocator_.alloc(check_file_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG_FILTER(ERROR, "failed to allocate buf", K(ret), K(check_file_path), K(check_file_len));
    } else if (OB_FAIL(util.read_single_file(check_file_path, &storage_info, read_file_buf,
                       check_file_len, read_size, ObStorageIdMod::get_default_id_mod()))) {
      STORAGE_LOG_FILTER(ERROR, "failed to read check file",  K(ret), K(check_file_path), K(check_file_len));
    } else if (read_size != check_file_len) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "read check file length does not match real length",  K(ret), K(check_file_path), K(read_size), K(check_file_len));
    }
  }
  return ret;
}

class CleanBackupPathOp : public ObBaseDirEntryOperator
{
public:
  CleanBackupPathOp(char *base_path, share::ObBackupStorageInfo *storage_info)
      : base_path_(base_path), storage_info_(storage_info), cleaned_objects_(0)
  {
    uri_[0] = '\0';
  }
  ~CleanBackupPathOp() {}
  int func(const dirent *entry) override;

  char uri_[OB_MAX_URI_LENGTH];
  char *base_path_;
  share::ObBackupStorageInfo *storage_info_;
  int64_t cleaned_objects_;
};

int CleanBackupPathOp::func(const dirent *entry)
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter adapter;

  if (OB_FAIL(databuff_printf(uri_, sizeof(uri_), "%s/%s", base_path_, entry->d_name))) {
    OB_LOG(WARN, "fail to set uri", K(ret), K_(uri), KPC_(storage_info));
  } else if (OB_FAIL(adapter.del_file(uri_, storage_info_))) {
    OB_LOG(WARN, "fail to delete file", K(ret), K_(uri), KPC_(storage_info));
  } else {
    cleaned_objects_++;
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_clean_backup_file_()
{
  int ret = OB_SUCCESS;
  TestExecGuard guard("clean backup file", test_id_, ret);
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  const char* check_file_dir_name = "check_file/";
  const char* check_file_name = "check_file.obbak";
  const char* appendable_check_file_name = "appendable_check_file.obbak";
  char check_file_dir_path[OB_MAX_URI_LENGTH] = { 0 };
  char check_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char appendable_check_file_path[OB_MAX_URI_LENGTH] = { 0 };

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(check_file_dir_path, OB_MAX_URI_LENGTH, "%s%s%s",
             backup_path_, "/", check_file_dir_name))) {
    STORAGE_LOG_FILTER(ERROR, "fail to databuff printf", K(ret));
  } else if (OB_FAIL(databuff_printf(check_file_path, OB_MAX_URI_LENGTH, "%s%s",
                     check_file_dir_path, check_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "fail to databuff printf", K(ret));
  } else if (OB_FAIL(util.del_file(check_file_path, &storage_info))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del normal check file", K(ret), K(check_file_path));
  } else if (OB_FAIL(databuff_printf(appendable_check_file_path, OB_MAX_URI_LENGTH, "%s%s",
                     check_file_dir_path, appendable_check_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "fail to databuff printf", K(ret));
  } else if (OB_FAIL(util.del_file(appendable_check_file_path, &storage_info))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del appendable check file", K(ret), K(appendable_check_file_path));
  } else if (OB_FAIL(util.del_dir(check_file_dir_path, &storage_info))) {
    STORAGE_LOG_FILTER(ERROR, "failed to del check file dir", K(ret), K(check_file_dir_path));
  } else { // remove the whole test dir
    if (ObStorageType::OB_STORAGE_FILE == storage_info.get_type()) {
      char cmd[OB_MAX_URI_LENGTH] = { 0 };
      if (OB_FAIL(databuff_printf(cmd, OB_MAX_URI_LENGTH,
                                  "rm -rf %s/*", backup_path_ + strlen(OB_FILE_PREFIX)))) {
        STORAGE_LOG_FILTER(ERROR, "fail to fill clean cmd", K(ret), K_(backup_path));
      } else if (0 != std::system(cmd)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "fail to delete dir", K(ret), K_(backup_path), K(cmd));
      }
    } else {
      CleanBackupPathOp op(backup_path_, &storage_info);
      if (OB_FAIL(util.list_files(backup_path_, &storage_info, op))) {
        STORAGE_LOG_FILTER(ERROR, "fail to clean", K(ret), K_(backup_path), K(storage_info));
      }
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_backup_data_()
{
  int ret = OB_SUCCESS;
  TestExecGuard guard("backup data", test_id_, ret);
  int tmp_ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_MULTIPART_WRITER;
  const char* meta_dir_name = "data/meta_info/";
  const char* meta_file_name = "ls_meta_infos.obbak";
  const char* meta_file_content = "test backup meta";
  const char* data_file_name = "data/macro_block_data.0.obbak";
  char meta_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char data_file_path[OB_MAX_URI_LENGTH] = { 0 };
  int64_t real_len = strlen(meta_file_content);
  int64_t offset = 0;
  int64_t write_size = 0;
  int64_t read_size = 0;
  int64_t total_size = 0;

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(meta_file_path, OB_MAX_URI_LENGTH, "%s%s%s%s",
              backup_path_, "/", meta_dir_name, meta_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.mk_parent_dir(meta_file_path, &storage_info))) {
    STORAGE_LOG_FILTER(ERROR, "failed to make parent dir for meta file", K(ret), K(meta_file_path));
  } else if (OB_FAIL(util.write_single_file(meta_file_path, &storage_info, meta_file_content,
                          real_len, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write single meta file", K(ret), K(meta_file_path));
  } else if (OB_FAIL(databuff_printf(data_file_path, OB_MAX_URI_LENGTH, "%s%s%s",
             backup_path_, "/", data_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, &storage_info,
             data_file_path, access_type, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret),
                K(data_file_path), K(access_type));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < UPLOAD_TIMES; ++i) {
      char* buf = nullptr;
      const int64_t buf_size = static_cast<int64_t>(ObRandom::rand(MIN_PART_SIZE, MAX_PART_SIZE));
      if(OB_FAIL(generate_random_str_(buf, buf_size))) {
        STORAGE_LOG_FILTER(ERROR, "failed to generate random str", K(ret), K(buf_size));
      } else if (OB_FAIL(device_handle->pwrite(fd, offset, buf_size,
                         buf, write_size))) {
        STORAGE_LOG_FILTER(ERROR, "failed to upload part", K(ret), K(data_file_path), K(buf_size));
      } else if (write_size != buf_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "upload data size does not match buf size", K(ret), K(write_size), K(buf_size));
      } else {
        total_size += buf_size;
        offset = total_size;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(device_handle->complete(fd))) {
        STORAGE_LOG_FILTER(ERROR, "fail to complete multipart upload", K(ret), K(tmp_ret), K(device_handle), K(fd));
      }
    } else {
      if (OB_TMP_FAIL(device_handle->abort(fd))) {
        ret = COVER_SUCC(tmp_ret);
        STORAGE_LOG_FILTER(ERROR, "fail to abort multipart upload", K(ret), K(tmp_ret), K(device_handle), K(fd));
      }
    }
    if (OB_TMP_FAIL(util.close_device_and_fd(device_handle, fd))) {
      ret = COVER_SUCC(tmp_ret);
      STORAGE_LOG_FILTER(ERROR, "fail to close device and fd", K(ret), K(device_handle), K(fd));
    }
    if(OB_SUCC(ret)) {
       if (OB_FAIL(util.get_file_length(data_file_path, &storage_info, read_size))) {
        STORAGE_LOG_FILTER(ERROR, "fail to get multipart upload file length", K(ret), K(data_file_path));
      } else if (read_size != total_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "get multipart upload file length does not match real file length", K(ret), K(read_size), K(total_size));
      }
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::generate_random_str_(char *&buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL((buf = static_cast<char *>(allocator_.alloc(size))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "fail to allocate memory", K(ret), K(size));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < size; ++i) {
    buf[i] = static_cast<char> ('a' + ObRandom::rand(0, 25));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_archive_log_() {
  int ret = OB_SUCCESS;
  TestExecGuard guard("archive log", test_id_, ret);
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  const char* meta_dir_name = "archive/rounds/";
  const char* meta_file_name = "round_d1002r1_start.obarc";
  const char* meta_file_content = "test archive round start";
  const int64_t MAX_OB_ADMIN_TIMEOUT = 600000; // 10min
  char meta_file_path[OB_MAX_URI_LENGTH] = { 0 };
  int64_t real_len = strlen(meta_file_content);

  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_FAIL(OB_IO_MANAGER.get_tenant_io_manager(OB_SERVER_TENANT_ID, tenant_holder))) {
    STORAGE_LOG(WARN, "fail to get tenant io manager", K(ret));
  } else if (OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tenant holder ptr is null", K(ret));
  } else {
    ObTenantIOConfig io_config(tenant_holder.get_ptr()->get_io_config());
    io_config.object_storage_io_timeout_ms_ = MAX_OB_ADMIN_TIMEOUT;
    if (OB_FAIL(tenant_holder.get_ptr()->update_basic_io_config(io_config))) {
      STORAGE_LOG(WARN, "update tenant io config failed", K(ret), K(io_config));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(meta_file_path, OB_MAX_URI_LENGTH, "%s%s%s%s",
              backup_path_, "/", meta_dir_name, meta_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.mk_parent_dir(meta_file_path, &storage_info))) {
    STORAGE_LOG_FILTER(ERROR, "failed to make parent dir for meta file", K(ret), K(meta_file_path));
  } else if (OB_FAIL(util.write_single_file(meta_file_path, &storage_info, meta_file_content,
                          real_len, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write single meta file", K(ret), K(meta_file_path));
  } else if (OB_FAIL(test_full_clog_file_())) {
    STORAGE_LOG_FILTER(ERROR, "test full clog file failed", K(ret));
  } else if (OB_FAIL(test_partial_clog_file_())) {
    STORAGE_LOG_FILTER(ERROR, "test partial clog file failed", K(ret));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_full_clog_file_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  char full_log_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char* buf = nullptr;
  const char* full_log_file_name = "archive/logstream_1/log/1.obarc";
  const int64_t buf_size = CLOG_FILE_SIZE;
  int64_t file_size = 0;

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(full_log_file_path, OB_MAX_URI_LENGTH, "%s%s%s",
             backup_path_, "/", full_log_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(generate_random_str_(buf, buf_size))) {
    STORAGE_LOG_FILTER(ERROR, "failed to generate random str", K(ret), K(buf_size));
  } else if (OB_FAIL(util.mk_parent_dir(full_log_file_path, &storage_info))) {
    STORAGE_LOG_FILTER(ERROR, "failed to make parent dir for full clog file", K(ret), K(full_log_file_path));
  } else if (OB_FAIL(util.write_single_file(full_log_file_path, &storage_info, buf, buf_size,
                          ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to write single full clog file", K(full_log_file_path), K(buf_size));
  } else if (OB_FAIL(util.get_file_length(full_log_file_path, &storage_info,file_size))) {
    STORAGE_LOG_FILTER(ERROR, "failed to get full clog file length", K(full_log_file_path), K(file_size));
  } else if (CLOG_FILE_SIZE != file_size) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "get full clog file length does not match real length", K(file_size), K(buf_size));
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_partial_clog_file_()
{
  int ret = OB_SUCCESS;
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  ObIOFd fd;
  ObIODevice *device_handle = NULL;
  ObStorageAccessType access_type = OB_STORAGE_ACCESS_APPENDER;
  char partial_log_file_path[OB_MAX_URI_LENGTH] = { 0 };
  const char* partial_log_file_name = "archive/logstream_1/log/2.obarc";
  int64_t offset = 0;
  int64_t write_size = 0;
  int64_t read_size = 0;
  int64_t total_size = 0;

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(partial_log_file_path, OB_MAX_URI_LENGTH, "%s%s%s",
             backup_path_, "/", partial_log_file_name))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, &storage_info,
            partial_log_file_path, access_type, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret),
                K(partial_log_file_path), K(access_type));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < APPEND_TIMES; ++i) {
      char* buf = nullptr;
      const int64_t buf_size = static_cast<int64_t>(ObRandom::rand(MIN_APPEND_SIZE, MAX_APPEND_SIZE));
      if(OB_FAIL(generate_random_str_(buf, buf_size))) {
        STORAGE_LOG_FILTER(ERROR, "failed to generate random str", K(ret), K(buf_size));
      } else if (OB_FAIL(device_handle->pwrite(fd, offset, buf_size,
                        buf, write_size))) {
        STORAGE_LOG_FILTER(ERROR, "failed to append clog", K(ret), K(partial_log_file_path), K(offset), K(buf_size));
      } else if (write_size != buf_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "append data size does not match buf size", K(ret), K(write_size), K(buf_size));
      } else {
        offset += buf_size;
        total_size += buf_size;
      }
    }
    if(OB_SUCC(ret)) {
      int64_t remain_size = 0;
      char* remain_buf = nullptr;
      if (OB_FAIL(util.close_device_and_fd(device_handle, fd))) {
        STORAGE_LOG_FILTER(ERROR, "fail to close append writer and release device!", K(ret), K(partial_log_file_path));
      } else if (OB_FAIL(util.adaptively_get_file_length(partial_log_file_path, &storage_info, read_size))) {
        STORAGE_LOG_FILTER(ERROR, "fail to get appendable clog file length", K(ret), K(partial_log_file_path));
      } else if (read_size != total_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "get appendable clog file length does not match real file length", K(ret), K(read_size), K(total_size));
      } else if (OB_FAIL(util.open_with_access_type(device_handle, fd, &storage_info,
                 partial_log_file_path, access_type, ObStorageIdMod::get_default_id_mod()))) {
        STORAGE_LOG_FILTER(ERROR, "failed to open device with access type", K(ret),
                  K(partial_log_file_path), K(access_type));
      } else if (FALSE_IT(remain_size = CLOG_FILE_SIZE - total_size)) {
      } else if (OB_FAIL(generate_random_str_(remain_buf, remain_size))) {
        STORAGE_LOG_FILTER(ERROR, "failed to generate remaining random str for appendable clog file", K(ret), K(remain_size));
      } else if (OB_FAIL(device_handle->pwrite(fd, offset, remain_size, remain_buf, write_size))) {
        STORAGE_LOG_FILTER(ERROR, "failed to append remaining str for appendable clog file", K(ret), K(remain_size));
      } else if (FALSE_IT(read_size = 0) || FALSE_IT(total_size += remain_size)) {
      } else if (OB_FAIL(device_handle->seal_file(fd))) {
        STORAGE_LOG_FILTER(ERROR, "failed to seal for appendable clog file", K(ret), K(partial_log_file_path));
      } else if (OB_FAIL(util.close_device_and_fd(device_handle, fd))) {
        STORAGE_LOG_FILTER(ERROR, "fail to close append writer and release device!", K(ret), K(partial_log_file_path));
      } else if (OB_FAIL(util.adaptively_get_file_length(partial_log_file_path, &storage_info, read_size))) {
        STORAGE_LOG_FILTER(ERROR, "fail to get appendable clog file length", K(ret), K(partial_log_file_path));
      } else if (read_size != total_size) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG_FILTER(ERROR, "get appendable clog file length does not match real file length", K(ret), K(read_size), K(total_size));
      }
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::test_consume_clog_file_()
{
  int ret = OB_SUCCESS;
  TestExecGuard guard("consume clog file", test_id_, ret);
  ObBackupIoAdapter util;
  share::ObBackupStorageInfo storage_info;
  int64_t min_file_id = 0;
  int64_t max_file_id = 0;
  const char* ls_dir = "archive/logstream_1/log/";
  char ls_path[OB_MAX_URI_LENGTH] = { 0 };
  char full_clog_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char partial_clog_file_path[OB_MAX_URI_LENGTH] = { 0 };
  char* buf1 = nullptr;
  char* buf2 = nullptr;
  const int64_t buf_len = 10 * 1024 * 1024;
  const int64_t offset = ObRandom::rand(0, CLOG_FILE_SIZE - buf_len);
  int64_t read_size = 0;

  if (OB_FAIL(storage_info.set(backup_path_, storage_info_))) {
    STORAGE_LOG_FILTER(ERROR, "failed to set storage info", K_(backup_path));
  } else if (OB_FAIL(databuff_printf(ls_path, OB_MAX_URI_LENGTH, "%s%s%s",
             backup_path_, "/", ls_dir))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_FAIL(archive::ObArchiveFileUtils::get_file_range(ls_path,
                     &storage_info, min_file_id, max_file_id))) {
    STORAGE_LOG_FILTER(ERROR, "failed to get clog file range", K(ret), K(ls_path));
  } else if (min_file_id != 1 || max_file_id != 2) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "got wrong clog file list", K(ret), K(min_file_id), K(max_file_id));
  } else if (OB_FAIL(databuff_printf(full_clog_file_path, OB_MAX_URI_LENGTH, "%s%s%s%s", backup_path_,
             "/", ls_dir, "1.obarc"))) {
    STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
  } else if (OB_ISNULL(buf1 = reinterpret_cast<char*>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG_FILTER(ERROR, "failed to allocate memory", K(ret), K(buf_len));
  } else if (OB_FAIL(util.adaptively_read_part_file(full_clog_file_path, &storage_info, buf1,
             buf_len, offset, read_size, ObStorageIdMod::get_default_id_mod()))) {
    STORAGE_LOG_FILTER(ERROR, "failed to read part from full clog file", K(ret), K(full_clog_file_path),
                K(offset), K(buf_len));
  } else if (read_size != buf_len) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG_FILTER(ERROR, "read full clog file length does not match real buf length", K(ret), K(read_size), K(buf_len));
  } else {
    read_size = 0;
    if (OB_FAIL(databuff_printf(partial_clog_file_path, OB_MAX_URI_LENGTH, "%s%s%s%s", backup_path_,
             "/", ls_dir, "2.obarc"))) {
      STORAGE_LOG_FILTER(ERROR, "failed to databuff printf", K(ret));
    } else if (OB_ISNULL(buf2 = reinterpret_cast<char*>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG_FILTER(ERROR, "failed to allocate memory", K(ret), K(buf_len));
    } else if (OB_FAIL(util.adaptively_read_part_file(partial_clog_file_path, &storage_info, buf2,
               buf_len, offset, read_size, ObStorageIdMod::get_default_id_mod()))) {
      STORAGE_LOG_FILTER(ERROR, "failed to read part from full clog file", K(ret), K(full_clog_file_path),
                  K(offset), K(buf_len));
    } else if (read_size != buf_len) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG_FILTER(ERROR, "read partial clog file length does not match real buf length", K(ret), K(read_size), K(buf_len));
    }
  }
  return ret;
}

int ObAdminTestIODeviceExecutor::print_usage_()
{
  int ret = OB_SUCCESS;
  printf("\n");
  printf("Usage: test_io_device command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-h,--help", "display this message.");
  printf("options:\n");
  printf(HELP_FMT, "-d,--backup-file-path", "absolute backup file path with file prefix");
  printf(HELP_FMT, "-s,--storage-info", "oss/cos should provide storage info");
  printf(HELP_FMT, "-i, --sts_credential", "set STS credential");
  printf(HELP_FMT, "-e,--s3_url_encode_type", "set S3 protocol url encode type");
  printf("samples:\n");
  printf("  test nfs device: \n");
  printf("\tob_admin test_io_device -dfile:///home/admin/backup_info \n");
  printf("  test object device: \n");
  printf("\tob_admin test_io_device -d'oss://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222'\n");
  printf("\tob_admin test_io_device -d'cos://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&appid=333'\n");
  printf("\tob_admin test_io_device -d'cos://home/admin/backup_info' "
         "-s'host=xxx.com&role_arn=xxx&appid=333'\n"
         "-i'sts_url=xxx&sts_ak=aaa&sts_sk=bbb'");
  printf("\tob_admin test_io_device -d'cos://home/admin/backup_info' "
         "-s'host=xxx.com&role_arn=xxx&external_id=xxx&appid=333'\n"
         "-i'sts_url=xxx&sts_ak=aaa&sts_sk=bbb'");
  printf("\tob_admin test_io_device -d's3://home/admin/backup_info' "
         "-s'host=xxx.com&access_id=111&access_key=222&region=333'\n"
         "-e'compliantRfc3986Encoding'");
  return ret;
}

}   //tools
}   //oceanbase
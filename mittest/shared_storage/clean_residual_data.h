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

#ifndef OCEANBASE_SENSITIVE_TEST_SHARED_STORAGE_CLEAN_RESIDUAL_DATA_H_
#define OCEANBASE_SENSITIVE_TEST_SHARED_STORAGE_CLEAN_RESIDUAL_DATA_H_

#include <gtest/gtest.h>
#include "share/backup/ob_backup_struct.h"              // ObBackupStorageInfo
#include "share/backup/ob_backup_io_adapter.h"          // ObBackupIoAdapter
#include "share/backup/ob_backup_path.h"                // ObBackupPath
#include "share/object_storage/ob_device_config_mgr.h"  // ObDeviceConfigMgr

namespace oceanbase {
namespace storage {

class ResidualDataCleaner
{
public:
  ResidualDataCleaner() : is_inited_(false), path_(), storage_info_()
  {}
  virtual ~ResidualDataCleaner()
  {}
  // Directly retrieve the device_config from the mock environment
  // in the test and perform initialization.
  int init_in_mock_env();
  int clean_residual_data();

private:
  int init(const char *path, const char *endpoint, const char *access_info, const char *extension);
  bool is_inited_;
  share::ObBackupPath path_;
  share::ObBackupStorageInfo storage_info_;
};

int ResidualDataCleaner::init(const char *path, const char *endpoint, const char *access_info, const char *extension)
{
  int ret = OB_SUCCESS;
  common::ObStorageType type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
  if (OB_ISNULL(path) || OB_ISNULL(endpoint) || OB_ISNULL(access_info) || OB_ISNULL(extension)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid initialization argument", KR(ret), KP(path), KP(endpoint), KP(access_info), KP(extension));
  } else if (OB_FAIL(path_.init(path))) {
    OB_LOG(WARN, "failed to set path when initiating residual data cleaner", KR(ret), KCSTRING(path));
  } else if (OB_FAIL(get_storage_type_from_path(path, type))) {
    OB_LOG(WARN, "failed to get storage type when initiating residual data cleaner", KR(ret), KCSTRING(path));
  } else if (OB_FAIL(storage_info_.set(type, endpoint, access_info, extension, OB_INVALID_DEST_ID))) {
    OB_LOG(WARN, "failed to set storage info when initiating residual data cleaner", KR(ret), KCSTRING(path), KCSTRING(endpoint));
  } else {
    is_inited_ = true;
    OB_LOG(INFO, "success to initiate residual data cleaner", KCSTRING(path), KCSTRING(endpoint));
  }
  return ret;
};

int ResidualDataCleaner::init_in_mock_env()
{
  int ret = OB_SUCCESS;
  share::ObDeviceConfig device_config;
  if (OB_FAIL(share::ObDeviceConfigMgr::get_instance().get_device_config(
          share::ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config))) {
    OB_LOG(WARN, "failed to get device config when initiating residual data cleaner", KR(ret));
  } else if (OB_FAIL(init(
                 device_config.path_, device_config.endpoint_, device_config.access_info_, device_config.extension_))) {
    OB_LOG(WARN, "failed to init residual data cleaner", KR(ret));
  }
  return ret;
};

int ResidualDataCleaner::clean_residual_data()
{
  int ret = OB_SUCCESS;
  common::ObBackupIoAdapter io_adapter;
  if (OB_FAIL(io_adapter.del_dir(path_.get_obstr(), &storage_info_, true/*is_recursive*/))) {
    if (OB_DIR_NOT_EXIST == ret) {
      OB_LOG(INFO, "dir is not exist when cleaning residual data", KR(ret));
      ret = OB_SUCCESS;
    } else {
      OB_LOG(WARN, "failed to del dir when cleaning residual data", KR(ret));
    }
  } else {
    OB_LOG(INFO, "success to clean residual data");
  }
  return ret;
};

class ResidualDataCleanerHelper
{
public:
  ResidualDataCleanerHelper()
  {}
  virtual ~ResidualDataCleanerHelper()
  {}
  static int clean_in_mock_env();
};

int ResidualDataCleanerHelper::clean_in_mock_env()
{
  int ret = OB_SUCCESS;
  const int failed_count = ::testing::UnitTest::GetInstance()->failed_test_count();
  const ::testing::TestCase *test_case = ::testing::UnitTest::GetInstance()->current_test_case();
  // When all test cases are passed, clean residual data
   if (OB_ISNULL(test_case)) {
      ret = OB_ERR_UNEXPECTED;
      OB_LOG(WARN, "test case is null when try to clean residual data", KR(ret));
    } else if (failed_count == 0) {
      int ret = OB_SUCCESS;
      ResidualDataCleaner cleaner;
      if (OB_FAIL(cleaner.init_in_mock_env())) {
        OB_LOG(WARN, "failed to init residual data cleaner", KR(ret));
      } else if (OB_FAIL(cleaner.clean_residual_data())) {
        OB_LOG(WARN, "failed to clean residual data", KR(ret), K(test_case->name()));
      } else {
        OB_LOG(INFO, "success to clean residual data after test", K(test_case->name()));
      }
  }
  return ret;
};


}  // namespace storage
}  // namespace oceanbase

#endif  // OCEANBASE_SENSITIVE_TEST_SHARED_STORAGE_CLEAN_RESIDUAL_DATA_H_
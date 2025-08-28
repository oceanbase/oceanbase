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

#include <vector>
#include <thread>
#define private public
#define protected public
#include "share/rc/ob_tenant_base.h"
#include "logservice/ob_log_external_storage_handler.h"
#include "logservice/palf/log_block_header.h"
#include "share/object_storage/ob_device_config_mgr.h"
#include "share/io/ob_io_manager.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "log/ob_shared_log_utils.h"
#endif
#include "share/ob_device_manager.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#undef protected
#undef private
#include <gtest/gtest.h>

namespace oceanbase
{
static oceanbase::share::ObTenantBase *unittest_tenant_base_;
namespace unittest
{
using namespace share;
using namespace common;
using namespace logservice;
using namespace palf;

class TestLogExternalStorageCommom : public ::testing::Test
{
public:
  TestLogExternalStorageCommom() {}
  ~TestLogExternalStorageCommom() {}
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = share::ObServerMode::SHARED_STORAGE_MODE;
    unittest_tenant_base_ = OB_NEW(oceanbase::share::ObTenantBase, "unittest", 1);
    auto malloc = ObMallocAllocator::get_instance();
    if (NULL == malloc->get_tenant_ctx_allocator(1, 0)) {
      malloc->create_and_add_tenant_allocator(1);
    }
    unittest_tenant_base_->unit_max_cpu_ = 100.0;
    ObTenantEnv::set_tenant(unittest_tenant_base_);
    ObDeviceConfig device_config;
    char base_dir_c[1024];
    ASSERT_NE(nullptr, getcwd(base_dir_c, 1024));
    std::string base_dir = std::string(base_dir_c) + "/" + BASE_DIR;
    std::string mkdir = "mkdir " + base_dir;
    std::string rmdir = "rm -rf " + base_dir;
    system(rmdir.c_str());
    system(mkdir.c_str());
    std::string root_path = "file://" + base_dir;
    const char *used_for = ObStorageUsedType::get_str(ObStorageUsedType::TYPE::USED_TYPE_ALL);
    const char *state = "ADDED";
    MEMCPY(device_config.path_, root_path.c_str(), root_path.size());
    MEMCPY(device_config.used_for_, used_for, strlen(used_for));
    device_config.sub_op_id_ = 1;
    STRCPY(device_config.state_, state);
    device_config.op_id_ = 1;
    device_config.last_check_timestamp_ = 0;
    device_config.storage_id_ = 1;
    device_config.max_iops_ = 0;
    device_config.max_bandwidth_ = 0;
    const int64_t test_memory = 6 * 1024 * 1024;
    ASSERT_EQ(OB_SUCCESS, ObDeviceManager::get_instance().init_devices_env());
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().init(test_memory));
    ASSERT_EQ(OB_SUCCESS, ObIOManager::get_instance().start());
    ObTenantIOManager *io_service = nullptr;
    EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
    EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
    EXPECT_EQ(OB_SUCCESS, io_service->start());
    unittest_tenant_base_->set(io_service);
    ObTenantEnv::set_tenant(unittest_tenant_base_);
    ASSERT_EQ(OB_SUCCESS, GLOBAL_CONFIG_MGR.init(base_dir.c_str()));
    ASSERT_EQ(OB_SUCCESS, GLOBAL_CONFIG_MGR.add_device_config(device_config));
    ASSERT_EQ(OB_SUCCESS, GLOBAL_EXT_HANDLER.init());
    ASSERT_EQ(OB_SUCCESS, GLOBAL_EXT_HANDLER.start(0));
    EXPECT_EQ(100.0, MTL_CPU_COUNT());
    CLOG_LOG(INFO, "SetUpTestCase success", K(MTL_CPU_COUNT()));
  }

  static void TearDownTestCase()
  {
    ObIOManager::get_instance().stop();
    ObIOManager::get_instance().destroy();
  }

  void SetUp() override
  {
    unittest_tenant_base_->unit_max_cpu_ = 100.0;
  }

  void TearDown() override
  {}

  int upload_blocks(const uint64_t tenant_id,
                    const int64_t palf_id,
                    const palf::block_id_t start_block_id,
                    const std::vector<SCN> &scns)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(start_block_id)
        || scns.empty()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(start_block_id));
    } else {
      ret = upload_blocks_impl_(tenant_id, palf_id, start_block_id, scns);
    }
    return ret;
  }

  int upload_blocks(const uint64_t tenant_id,
                    const int64_t palf_id,
                    const palf::block_id_t start_block_id,
                    const int64_t block_count)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(start_block_id)
        || 0 >= block_count) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(start_block_id), K(block_count));
    } else {
      std::vector<SCN> scns(block_count, SCN::min_scn());
      int64_t start_ts = ObTimeUtility::current_time();
      (void)srand(start_ts);
      int64_t hour = 60 * 60;
      for (auto &scn : scns) {
        scn.convert_from_ts(start_ts);
        int64_t interval = random()%hour + 1;
        start_ts += interval;
      }
      ret = upload_blocks_impl_(tenant_id, palf_id, start_block_id, scns);
    }
    return ret;
  }

  int upload_block(const uint64_t tenant_id,
                   const int64_t palf_id,
                   const palf::block_id_t block_id,
                   const SCN &scn)
  {
    int ret = OB_SUCCESS;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || !is_valid_block_id(block_id)
        || !scn.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid arguments", K(tenant_id), K(palf_id), K(block_id), K(scn));
    } else {
      ret = upload_block_impl_(tenant_id, palf_id, block_id, scn);
    }
    return ret;
  }

  int create_tenant(const uint64_t tenant_id)
  {
    int ret = OB_SUCCESS;
    char tenant_cstr[OB_MAX_URI_LENGTH] = { '\0' };
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    if (!is_valid_tenant_id(tenant_id)) {
      ret = OB_INVALID_ARGUMENT;
    } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
      CLOG_LOG(WARN, "get_storage_dest_ failed", K(tenant_id), K(MAX_PALF_ID));
    } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.construct_tenant_str_(
          dest, tenant_id, tenant_cstr, sizeof(tenant_cstr)))) {
      CLOG_LOG(WARN, "construct_tenant_str_ failed", K(tenant_id), K(MAX_PALF_ID));
    } else {
      std::string tenant_str = tenant_cstr + strlen(OB_FILE_PREFIX);
      std::string mkdir_str = "mkdir -p " + tenant_str;
      system(mkdir_str.c_str());
    }
    return ret;
  }
  int create_palf(const uint64_t tenant_id,
                  const int64_t palf_id)
  {
    int ret = OB_SUCCESS;
    char palf_cstr[OB_MAX_URI_LENGTH] = { '\0' };
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    if (!is_valid_tenant_id(tenant_id)
        || !is_valid_palf_id(palf_id)
        || palf_id > MAX_PALF_ID) {
      ret = OB_INVALID_ARGUMENT;
      CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(palf_id), K(MAX_PALF_ID));
    } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
      CLOG_LOG(WARN, "get_storage_dest_ failed", K(tenant_id), K(palf_id), K(MAX_PALF_ID));
    } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.construct_ls_str_(
          dest, tenant_id, ObLSID(palf_id), palf_cstr, sizeof(palf_cstr)))) {
      CLOG_LOG(WARN, "construct_palf_str_ failed", K(tenant_id), K(palf_id), K(MAX_PALF_ID));
    } else {
      std::string palf_str = palf_cstr + strlen(OB_FILE_PREFIX);
      std::string mkdir_str = "mkdir -p " + palf_str;
      system(mkdir_str.c_str());
    }
    return ret;

  }
  int delete_tenant(const uint64_t tenant_id)
  {
    for (int64_t palf_id = 1; palf_id < MAX_PALF_ID; palf_id++) {
      delete_palf(tenant_id, palf_id);
    }
    // 本地盘的delete_tenant操作要求tenant目录下不为空
    return SHARED_LOG_GLOBAL_UTILS.delete_tenant(tenant_id);
  }
  int delete_palf(const uint64_t tenant_id,
                  const int64_t palf_id)
  {
    // 本地盘的delete_palf操作要求palf目录下不为空
    SHARED_LOG_GLOBAL_UTILS.delete_blocks(tenant_id, ObLSID(palf_id), 0, MAX_BLOCK_ID);
    return SHARED_LOG_GLOBAL_UTILS.delete_ls(tenant_id, ObLSID(palf_id));    
  }

private:
  int fsync_(uint64_t tenant_id, int64_t palf_id)
  {
    int ret = OB_SUCCESS;
    char palf_cstr[OB_MAX_URI_LENGTH] = { '\0' };
    ObBackupDest dest;
    uint64_t storage_id = OB_INVALID_ID;
    if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.get_storage_dest_and_id_(dest, storage_id))) {
    } else if (OB_FAIL(SHARED_LOG_GLOBAL_UTILS.construct_ls_str_(
          dest, tenant_id, ObLSID(palf_id), palf_cstr, sizeof(palf_cstr)))) {
    } else {
      std::string palf_str = palf_cstr + strlen(OB_FILE_PREFIX);
      int fd = ::open(palf_str.c_str(), LOG_READ_FLAG);
      ::fsync(fd);
    }
    return ret;
  }
  int upload_blocks_impl_(const uint64_t tenant_id,
                          const int64_t palf_id,
                          const palf::block_id_t start_block_id,
                          const std::vector<SCN> &scns)
  {
    int ret = OB_SUCCESS;
    const int64_t block_count = scns.size();
    for (int64_t index = 0; index < block_count && OB_SUCC(ret); index++) {
      ret = upload_block_impl_(tenant_id, palf_id, start_block_id + index, scns[index]);
    }
    return ret;
  }
protected:
  virtual int upload_block_impl_(const uint64_t tenant_id,
                                 const int64_t palf_id,
                                 const palf::block_id_t block_id,
                                 const SCN &scn)
  {
    int ret = OB_SUCCESS;
    LogBlockHeader log_block_header;
    char buf[MAX_INFO_BLOCK_SIZE] = {'\0'};
    LSN lsn(block_id*PALF_PHY_BLOCK_SIZE);
    int64_t pos = 0;
    log_block_header.update_lsn_and_scn(lsn, scn);
    if (OB_FAIL(log_block_header.serialize(buf, MAX_INFO_BLOCK_SIZE, pos))) {
      CLOG_LOG(ERROR, "serialize failed", K(pos));
    } else if (OB_FAIL(GLOBAL_EXT_HANDLER.upload(tenant_id, palf_id, block_id, buf, MAX_INFO_BLOCK_SIZE))) {
      CLOG_LOG(ERROR, "upload failed", K(tenant_id), K(palf_id), K(block_id));
    } else {
      CLOG_LOG(INFO, "upload_block success", K(tenant_id), K(palf_id), K(block_id), K(log_block_header));
      fsync_(tenant_id, palf_id);
    }
    return ret;
  }
public:
  static const int64_t MAX_PALF_ID;
  static const int64_t MAX_BLOCK_ID;
  static const std::string BASE_DIR;
  static ObDeviceConfigMgr &GLOBAL_CONFIG_MGR;
  static ObLogExternalStorageHandler GLOBAL_EXT_HANDLER;
};

const int64_t TestLogExternalStorageCommom::MAX_PALF_ID = 1010;
const int64_t TestLogExternalStorageCommom::MAX_BLOCK_ID= 1100;
ObDeviceConfigMgr& TestLogExternalStorageCommom::GLOBAL_CONFIG_MGR = ObDeviceConfigMgr::get_instance();
ObLogExternalStorageHandler TestLogExternalStorageCommom::GLOBAL_EXT_HANDLER;
}
}

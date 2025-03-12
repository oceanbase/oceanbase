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

#define USING_LOG_PREFIX COMMON

#include "ob_admin_executor.h"
#include "lib/net/ob_net_util.h"
#include "share/ob_device_manager.h"
#include "share/io/ob_io_manager.h"
#include "storage/blocksstable/ob_decode_resource_pool.h"
#include "storage/meta_store/ob_server_storage_meta_service.h"
#include "storage/ob_file_system_router.h"

namespace oceanbase
{
using namespace common;
namespace tools
{

ObAdminExecutor::ObAdminExecutor()
    : mock_server_tenant_(OB_SERVER_TENANT_ID),
      storage_env_(),
      reload_config_(ObServerConfig::get_instance(), GCTX),
      config_mgr_(ObServerConfig::get_instance(), reload_config_)
{
  // 设置MTL上下文
  IGNORE_RETURN ObTimerService::get_instance().start();
  mock_server_tenant_.set(&ObTimerService::get_instance());
  mock_server_tenant_.set(&blocksstable::ObDecodeResourcePool::get_instance());
  share::ObTenantEnv::set_tenant(&mock_server_tenant_);
  omt::ObTenantConfigMgr::get_instance().add_tenant_config(OB_SYS_TENANT_ID);
  storage_env_.data_dir_ = data_dir_;
  storage_env_.sstable_dir_ = sstable_dir_;
  storage_env_.default_block_size_ = 2 * 1024 * 1024;

  storage_env_.log_spec_.log_dir_ = slog_dir_;
  storage_env_.log_spec_.max_log_file_size_ = ObLogConstants::MAX_LOG_FILE_SIZE;

  storage_env_.slog_file_spec_.retry_write_policy_ = "normal";
  storage_env_.slog_file_spec_.log_create_policy_ = "normal";
  storage_env_.slog_file_spec_.log_write_policy_ = "truncate";

  storage_env_.clog_dir_ = clog_dir_;

  storage_env_.bf_cache_miss_count_threshold_ = 0;
  storage_env_.bf_cache_priority_ = 1;
  storage_env_.index_block_cache_priority_ = 10;
  storage_env_.user_block_cache_priority_ = 1;
  storage_env_.user_row_cache_priority_ = 1;
  storage_env_.fuse_row_cache_priority_ = 1;
  storage_env_.tablet_ls_cache_priority_ = 1;
  storage_env_.storage_meta_cache_priority_ = 10;
  storage_env_.ethernet_speed_ = 10000;
  storage_env_.data_disk_size_ = 1000 * storage_env_.default_block_size_;

  GCONF.datafile_size = 128 * 1024 * 1024;


}

ObAdminExecutor::~ObAdminExecutor()
{
  blocksstable::ObDecodeResourcePool::get_instance().destroy();
  ObIOManager::get_instance().stop();
  ObIOManager::get_instance().destroy();
  OB_STORAGE_OBJECT_MGR.stop();
  OB_STORAGE_OBJECT_MGR.wait();
  OB_STORAGE_OBJECT_MGR.destroy();
  share::ObIODeviceWrapper::get_instance().destroy();
  ObTimerService::get_instance().stop();
  ObTimerService::get_instance().wait();
  ObTimerService::get_instance().destroy();
  LOG_INFO("destruct ObAdminExecutor");
}

int ObAdminExecutor::prepare_io()
{
  int ret = OB_SUCCESS;

  if (STRLEN(data_dir_) == 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_FAIL(databuff_printf(sstable_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/sstable/", data_dir_))) {
    LOG_WARN("failed to databuff printf", K(ret));
  } else if (OB_FAIL(databuff_printf(clog_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/clog/", data_dir_))) {
    LOG_WARN("failed to gen clog dir", K(ret));
  }

  const int64_t async_io_thread_count = 8;
  const int64_t sync_io_thread_count = 2;
  const int64_t max_io_depth = 256;
  ObTenantIOConfig tenant_io_config = ObTenantIOConfig::default_instance();

  if (OB_FAIL(ret)) {
    // do nothing
  } else if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
    LOG_WARN("init device manager failed", KR(ret));
  } else if (OB_FAIL(share::ObIODeviceWrapper::get_instance().init(
      storage_env_.data_dir_,
      storage_env_.sstable_dir_,
      storage_env_.default_block_size_,
      storage_env_.data_disk_percentage_,
      storage_env_.data_disk_size_))) {
    LOG_WARN("fail to init io device, ", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().init())) {
    LOG_WARN("fail to init io manager", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().add_device_channel(&LOCAL_DEVICE_INSTANCE,
      async_io_thread_count, sync_io_thread_count, max_io_depth))) {
    LOG_WARN("add device channel failed", K(ret));
  } else if (OB_FAIL(ObIOManager::get_instance().start())) {
    LOG_WARN("fail to start io manager", K(ret));
  } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.get_instance().init(data_dir_))) {
    LOG_WARN("fail to init file system router", K(ret));
  } else if (OB_FAIL(SERVER_STORAGE_META_SERVICE.init(false/*is_shared_storage*/))) {
    LOG_WARN("fail to init storage meta service", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.init(false/*is_shared_storage*/, storage_env_.default_block_size_))) {
    LOG_WARN("fail to init block manager, ", K(ret));
  } else if (OB_FAIL(OB_STORAGE_OBJECT_MGR.start(0/*reserved_size*/))) {
    LOG_WARN("fail to start block manager, ", K(ret));
  }

  return ret;
}

int ObAdminExecutor::prepare_decoder()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(blocksstable::ObDecodeResourcePool::get_instance().init())) {
    LOG_WARN("fail to init decoder resource pool");
  }
  return ret;
}

int ObAdminExecutor::load_config()
{
  int ret = OB_SUCCESS;
  // set dump path
  const char *dump_path = "etc/observer.config.bin";
  config_mgr_.set_dump_path(dump_path);
  if (OB_FAIL(config_mgr_.load_config())) {
    STORAGE_LOG(WARN, "fail to load config", K(ret));
  } else {
    ObServerConfig &config = config_mgr_.get_config();
    int32_t local_port = static_cast<int32_t>(config.rpc_port);
    if (config.use_ipv6) {
      char ipv6[MAX_IP_ADDR_LENGTH] = { '\0' };
      obsys::ObNetUtil::get_local_addr_ipv6(config.devname, ipv6, sizeof(ipv6));
      ObAddr tmp_addr = GCTX.self_addr();
      tmp_addr.set_ip_addr(ipv6, local_port);
      GCTX.self_addr_seq_.set_addr(tmp_addr);
    } else {
      uint32_t ipv4_net = 0;
      if (OB_FAIL(obsys::ObNetUtil::get_local_addr_ipv4(config.devname, ipv4_net))) {
        LOG_ERROR("get ipv4 address by devname failed", "devname",
            config.devname.get_value(), KR(ret));
      } else {
        int32_t ipv4 = ntohl(ipv4_net);
        ObAddr tmp_addr = GCTX.self_addr();
        tmp_addr.set_ipv4_addr(ipv4, local_port);
        GCTX.self_addr_seq_.set_addr(tmp_addr);
      }
    }
  }

  return ret;
}

int ObAdminExecutor::set_s3_url_encode_type(const char *type_str) const
{
  // When compliantRfc3986Encoding is set to true:
  // - Adhere to RFC 3986 by supporting the encoding of reserved characters
  //   such as '-', '_', '.', '$', '@', etc.
  // - This approach mitigates inconsistencies in server behavior when accessing
  //   COS using the S3 SDK.
  // Otherwise, the reserved characters will not be encoded,
  // following the default behavior of the S3 SDK.
  int ret = OB_SUCCESS;
  if (OB_ISNULL(type_str)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "type str is null", KR(ret), KP(type_str));
  } else if (OB_FAIL(common::ObDeviceManager::get_instance().init_devices_env())) {
    STORAGE_LOG(WARN, "fail to init device env", KR(ret), K(type_str));
  } else if (0 == STRCASECMP("default", type_str)) {
    Aws::Http::SetCompliantRfc3986Encoding(false);
  } else if (0 == STRCASECMP("compliantRfc3986Encoding", type_str)) {
    Aws::Http::SetCompliantRfc3986Encoding(true);
  } else {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "type str is invalid, expect 'dafault'/'compliantRfc3986Encoding'",
        KR(ret), K(type_str));
  }
  return ret;
}
int ObAdminExecutor::set_sts_credential_key(const char *sts_credential)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(sts_credential)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "sts credential is null", KR(ret), KP(sts_credential));
  } else {
    if (OB_FAIL(ObDeviceManager::get_instance().init_devices_env())) {
      STORAGE_LOG(WARN, "fail to init device env", KR(ret));
    } else if (OB_FAIL(ObObjectStorageInfo::register_cluster_version_mgr(
                   &ObClusterVersionBaseMgr::get_instance()))) {
      STORAGE_LOG(WARN, "fail to register cluster version mgr", KR(ret));
    } else {
      omt::ObTenantConfigGuard tenant_config(TENANT_CONF(OB_SYS_TENANT_ID));
      if (OB_UNLIKELY(!tenant_config.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(
            WARN, "tenant config is invalid", KR(ret), K(OB_SYS_TENANT_ID));
      } else {
        tenant_config->sts_credential = sts_credential;
      }
    }
  }
  return ret;
}
}
}

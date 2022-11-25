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

#include <gtest/gtest.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <gmock/gmock.h>

#include "logservice/palf/palf_options.h"
#include "share/ob_alive_server_tracer.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "storage/slog/ob_storage_logger_manager.h"
#include "storage/slog/ob_storage_logger.h"
#include "storage/blocksstable/ob_log_file_spec.h"
#include "lib/file/file_directory_utils.h"
#include "storage/mock_ob_meta_report.h"

namespace oceanbase
{
using namespace common;
namespace storage
{

class MockObTenantStorageAgent
{
public:
  MockObTenantStorageAgent() : req_transport_(NULL, NULL),
                               self_addr_(ObAddr::IPV4, "127.0.0.1", 52965)
  {}
  ~MockObTenantStorageAgent() {
    system("rm -rf ./mock_ob_tenant_storage");
  }
  int init()
  {
    int ret = OB_SUCCESS;

    MEMCPY(dir_, "./mock_ob_tenant_storage", sizeof("./mock_ob_tenant_storage"));
    FileDirectoryUtils::create_full_path("./mock_ob_tenant_storage");
    log_file_spec_.retry_write_policy_ = "normal";
    log_file_spec_.log_create_policy_ = "normal";
    log_file_spec_.log_write_policy_ = "truncate";
    if (OB_FAIL(SLOGGERMGR.init(dir_, MAX_FILE_SIZE, log_file_spec_))) {

    }

    return ret;
  }
  void destroy()
  {
    SLOGGERMGR.destroy();
    system("rm -rf ./mock_ob_tenant_storage");
  }

public:
  static const int64_t MAX_FILE_SIZE = 256 * 1024 * 1024;

private:
  share::ObLocationService location_service_;
  obrpc::ObBatchRpc batch_rpc_;
  share::schema::ObMultiVersionSchemaService schema_service_;
  share::ObAliveServerTracer server_tracer_;
  palf::PalfDiskOptions disk_options_;
  rpc::frame::ObReqTransport req_transport_;
  MockObMetaReport reporter_;
  ObAddr self_addr_;
  char dir_[128];
  blocksstable::ObLogFileSpec log_file_spec_;
};

}
}

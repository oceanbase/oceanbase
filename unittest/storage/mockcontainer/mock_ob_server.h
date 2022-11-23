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

#ifndef OCEANBASE_MOCK_OB_SERVER_H_
#define OCEANBASE_MOCK_OB_SERVER_H_

#include "storage/tx/ob_trans_service.h"
#include "lib/string/ob_sql_string.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/tx/ob_gts_rpc.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "observer/ob_srv_network_frame.h"
#include "share/config/ob_server_config.h"
#include "share/config/ob_config_manager.h"
#include "share/config/ob_reload_config.h"
#include "observer/ob_service.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/omt/ob_worker_processor.h"
#include "observer/ob_srv_xlator.h"

#include "mock_ob_schema_service.h"
#include "../../share/schema/mock_schema_service.h"
#include "share/ob_alive_server_tracer.h"

#define MAX_PATH_SIZE 1024
#define DATADIR "data"
#define APPNAME "test"

namespace oceanbase
{

using namespace common;
using namespace storage;
using namespace observer;
using namespace share;
using namespace share::schema;
using namespace transaction;

namespace unittest
{

class MockRootRpcProxy : public obrpc::ObCommonRpcProxy
{
public:
  MockRootRpcProxy() {}
  virtual ~MockRootRpcProxy() {}

  int get_frozen_status(const obrpc::Int64 &arg,
                        storage::ObFrozenStatus &frozen_status,
                        const obrpc::ObRpcOpts &opts)
  {
    UNUSED(opts);
    int32_t major_version = static_cast<int32_t>(arg);
    if (0 == arg) {
      major_version = 1;
    }
    frozen_status.frozen_version_.version_ = 0;
    frozen_status.frozen_version_.major_ = major_version;
    frozen_status.frozen_timestamp_ = major_version;
    frozen_status.status_ = COMMIT_SUCCEED;
    frozen_status.schema_version_ = major_version;

    return common::OB_SUCCESS;
  }
};

class MockObServer
{
public:
  MockObServer(const ObServerOptions &opts)
      : is_inited_(false),
      schema_service_(nullptr), gctx_(), net_frame_(gctx_),
      ob_service_(gctx_),
      session_mgr_(),
      warm_up_start_time_(0),
      opts_(opts), config_(ObServerConfig::get_instance()),
      reload_config_(&config_), config_mgr_(config_, reload_config_),
      multi_tenant_(), bandwidth_throttle_() {}
  ~MockObServer() { destroy(); }
  int init(const char *schema_file,
           int64_t data_file_size = 5LL << 30, int64_t macro_block_size = 2LL << 20);
  int init_multi_tenant();
  int init_tenant_mgr();
  int start();
  void destroy();
  int stop();
  int wait();
public:
  ObAddr &get_self() { return self_addr_; }
  MockSchemaService *get_schema_service();
  ObSchemaGetterGuard &get_schema_guard() { return restore_schema_.get_schema_guard(); }
protected:
  bool is_inited_;
protected:
  ObPartitionComponentFactory partition_cfy_;
  //MockObSchemaService schema_service_;
  ObRestoreSchema restore_schema_;
  MockSchemaService *schema_service_;
  share::ObRsMgr rs_mgr_;
  ObGlobalContext gctx_;
  ObSrvNetworkFrame net_frame_;
  obrpc::ObBatchRpc batch_rpc_;
  obrpc::ObSrvRpcProxy srv_rpc_proxy_;
  common::ObMySQLProxy sql_proxy_;
  MockRootRpcProxy rs_rpc_proxy_;
  ObService ob_service_;
  ObSQLSessionMgr session_mgr_;
  share::ObAliveServerTracer server_tracer_;
  int64_t warm_up_start_time_;
protected:
  ObServerOptions opts_;
  ObAddr self_addr_;
  ObServerConfig &config_;
  ObReloadConfig reload_config_;
  ObConfigManager config_mgr_;
  omt::ObMultiTenant multi_tenant_;
  transaction::ObGtsResponseRpc gts_response_rpc_;
  common::ObInOutBandwidthThrottle bandwidth_throttle_;
};

} // unittest
} // oceanbase

#endif

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

#include "lib/string/ob_sql_string.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/ob_rs_mgr.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_remote_sql_proxy.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_partition_component_factory.h"
#include "storage/transaction/ob_gts_rpc.h"
#include "storage/transaction/ob_gts_mgr.h"
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
#include "mock_ob_location_cache.h"
#include "../mock_ob_partition_report.h"
#include "../../share/schema/mock_schema_service.h"
#include "share/ob_alive_server_tracer.h"

#define MAX_PATH_SIZE 1024
#define DATADIR "data"
#define APPNAME "test"

namespace oceanbase {

using namespace common;
using namespace storage;
using namespace observer;
using namespace share;
using namespace share::schema;
using namespace transaction;

namespace storage {
class MockPartitionServiceVx : public ObPartitionService {
  virtual int start_trans(const uint64_t tenant_id, const uint64_t cluster_id,
      const transaction::ObStartTransParam& req, const int64_t expired_time, const uint32_t session_id,
      const uint64_t proxy_session_id, transaction::ObTransDesc& trans_desc)
  {
    return get_trans_service()->start_trans(
        tenant_id, cluster_id, req, expired_time, session_id, proxy_session_id, trans_desc);
  }
  virtual int end_trans(
      bool is_rollback, transaction::ObTransDesc& trans_desc, sql::ObIEndTransCallback& cb, const int64_t expired_time)
  {
    return get_trans_service()->end_trans(is_rollback, trans_desc, cb, expired_time);
  }

  virtual int start_stmt(const transaction::ObStmtParam& stmt_param, transaction::ObTransDesc& trans_desc,
      const common::ObPartitionLeaderArray& pla, common::ObPartitionArray& participants)
  {
    return get_trans_service()->start_stmt(stmt_param, trans_desc, pla, participants);
  }

  virtual int end_stmt(bool is_rollback, const ObPartitionArray& participants,
      const transaction::ObPartitionEpochArray& epoch_arr, const ObPartitionArray& discard_participants,
      const common::ObPartitionLeaderArray& pla, transaction::ObTransDesc& trans_desc)
  {
    bool is_incomplete = false;
    return get_trans_service()->end_stmt(
        is_rollback, is_incomplete, participants, epoch_arr, discard_participants, pla, trans_desc);
  }

  /*
  virtual int start_participant(transaction::ObTransDesc &trans_desc,
      const common::ObPartitionArray &participants, transaction::ObPartitionEpochArray &partition_epoch_arr,
      const transaction::ObPartitionSchemaInfoArray &partition_schema_info_arr,
      storage::ObIPartitionArrayGuard &pkey_guard_arr)
  {
    return get_trans_service()->start_participant(trans_desc, participants, partition_epoch_arr,
  partition_schema_info_arr, pkey_guard_arr);
  }
  */

  virtual int end_participant(
      bool is_rollback, transaction::ObTransDesc& trans_desc, const common::ObPartitionArray& participants)
  {
    return get_trans_service()->end_participant(is_rollback, trans_desc, participants);
  }
};
}  // namespace storage

namespace common {

class MockObRsCb : public ObIPartitionReport {
public:
  MockObRsCb()
  {}
  ~MockObRsCb()
  {}
  int init()
  {
    return OB_SUCCESS;
  }

public:
  int submit_pt_update_task(const ObPartitionKey& pkey, const bool)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  int report_merge_finished(const int64_t frozen_version)
  {
    UNUSED(frozen_version);
    return OB_SUCCESS;
  }
  int submit_pt_remove_task(const oceanbase::common::ObPartitionKey& pkey)
  {
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  int report_merge_error(const common::ObPartitionKey&, const int)
  {
    return OB_SUCCESS;
  }
  int report_rebuild_replica(const common::ObPartitionKey&, const common::ObAddr&, const storage::ObRebuildSwitch&)
  {
    return OB_SUCCESS;
  }
};
}  // namespace common

namespace unittest {

class MockPartitionTableOperator : public ObPartitionTableOperator {
public:
  MockPartitionTableOperator(ObIPartPropertyGetter& prop_getter) : ObPartitionTableOperator(prop_getter)
  {}

  int get(const uint64_t, const int64_t, ObPartitionInfo&)
  {
    return OB_SUCCESS;
  }
};

class MockRootRpcProxy : public obrpc::ObCommonRpcProxy {
public:
  MockRootRpcProxy()
  {}
  virtual ~MockRootRpcProxy()
  {}

  int get_frozen_status(const obrpc::Int64& arg, storage::ObFrozenStatus& frozen_status, const obrpc::ObRpcOpts& opts)
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

class MockObServer {
public:
  MockObServer(const ObServerOptions& opts)
      : is_inited_(false),
        location_cache_(&location_cache_def_),
        schema_service_(nullptr),
        gctx_(),
        net_frame_(gctx_),
        ob_service_(gctx_),
        pt_operator_(ob_service_),
        session_mgr_(gctx_.par_ser_),
        warm_up_start_time_(0),
        global_last_merged_version_(0),
        opts_(opts),
        config_(ObServerConfig::get_instance()),
        reload_config_(&config_),
        config_mgr_(config_, reload_config_),
        xlator_(gctx_),
        procor_(xlator_, self_addr_),
        multi_tenant_(procor_),
        bandwidth_throttle_()
  {}
  ~MockObServer()
  {
    destroy();
  }
  int init(const char* schema_file, MockObLocationCache* location_cache = NULL, int64_t data_file_size = 5LL << 30,
      int64_t macro_block_size = 2LL << 20);
  int init_multi_tenant();
  int init_tenant_mgr();
  int start();
  void destroy();
  int stop();
  int wait();

public:
  ObPartitionService* get_partition_service();
  MockObLocationCache* get_location_cache();
  ObAddr& get_self()
  {
    return self_addr_;
  }
  MockSchemaService* get_schema_service();
  ObSchemaGetterGuard& get_schema_guard()
  {
    return restore_schema_.get_schema_guard();
  }

protected:
  bool is_inited_;

protected:
  MockObLocationCache* location_cache_;

protected:
  ObPartitionComponentFactory partition_cfy_;
  // MockObSchemaService schema_service_;
  ObRestoreSchema restore_schema_;
  MockSchemaService* schema_service_;
  MockObLocationCache location_cache_def_;
  share::ObRsMgr rs_mgr_;
  MockObIPartitionReport rs_cb_;
  ObGlobalContext gctx_;
  ObSrvNetworkFrame net_frame_;
  obrpc::ObBatchRpc batch_rpc_;
  MockPartitionServiceVx partition_service_;
  obrpc::ObSrvRpcProxy srv_rpc_proxy_;
  common::ObMySQLProxy sql_proxy_;
  share::ObRemoteSqlProxy remote_sql_proxy_;
  MockRootRpcProxy rs_rpc_proxy_;
  ObService ob_service_;
  MockPartitionTableOperator pt_operator_;
  ObSQLSessionMgr session_mgr_;
  share::ObAliveServerTracer server_tracer_;
  int64_t warm_up_start_time_;
  int64_t global_last_merged_version_;

protected:
  ObServerOptions opts_;
  ObAddr self_addr_;
  ObServerConfig& config_;
  ObReloadConfig reload_config_;
  ObConfigManager config_mgr_;
  ObSrvXlator xlator_;
  omt::ObWorkerProcessor procor_;
  omt::ObMultiTenant multi_tenant_;
  transaction::ObGtsResponseRpc gts_response_rpc_;
  transaction::ObGlobalTimestampService gts_;
  common::ObInOutBandwidthThrottle bandwidth_throttle_;
};

}  // namespace unittest
}  // namespace oceanbase

#endif

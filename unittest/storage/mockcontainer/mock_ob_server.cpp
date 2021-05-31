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

#define USING_LOG_PREFIX STORAGE
#include "observer/ob_server.h"
#define private public
#include "mock_ob_server.h"
#include "mock_ob_location_cache.h"

#include "lib/string/ob_sql_string.h"
#include "lib/io/ob_io_manager.h"
#include "lib/net/tbnetutil.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/partition_table/ob_partition_info.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/transaction/ob_ts_mgr.h"
#include "share/ob_tenant_mgr.h"

namespace oceanbase {
using namespace common;
using namespace lib;
using namespace blocksstable;

namespace unittest {
using namespace common;
using namespace election;
const char* ELECT_ASYNC_LOG_FILE_NAME = "election.log";

int MockObServer::init(
    const char* schema_file, MockObLocationCache* location_cache, int64_t data_file_size, int64_t macro_block_size)
{
  int ret = OB_SUCCESS;
  int tmp_ret = 0;
  ObStorageEnv env;
  char* logdir = NULL;
  char* clogdir = NULL;
  char* ilogdir = NULL;

  if (is_inited_) {
    STORAGE_LOG(WARN, "ob server inited twice");
    ret = OB_INIT_TWICE;
  } else if (NULL == schema_file) {
    STORAGE_LOG(ERROR, "invalid argument", "schema_file", OB_P(schema_file));
    ret = OB_INVALID_ARGUMENT;
  } else {
  }
  // init config
  if (OB_SUCC(ret)) {
    config_.datafile_size = data_file_size;
    if (opts_.rpc_port_) {
      config_.rpc_port = opts_.rpc_port_;
    }
    if (opts_.mysql_port_) {
      config_.mysql_port = opts_.mysql_port_;
    }
    if (opts_.devname_) {
      config_.devname.set_value(opts_.devname_);
    }
    if (opts_.rs_list_) {
      config_.rootservice_list.set_value(opts_.rs_list_);
    }
    if (opts_.optstr_) {
      config_.add_extra_config(opts_.optstr_);
    }

    if (opts_.devname_ && strlen(opts_.devname_) > 0) {
      config_.devname.set_value(opts_.devname_);
    } else {
      const char* devname = get_default_if();
      if (devname && devname[0] != '\0') {
        LOG_INFO("guess interface name", K(devname));
        config_.devname.set_value(devname);
      } else {
        LOG_INFO("can't guess interface name, use default bond0");
      }
    }

    if (OB_SUCCESS != (ret = ASYNC_LOG_INIT(ELECT_ASYNC_LOG_FILE_NAME, OB_LOG_LEVEL_INFO, true))) {
      LOG_ERROR("async log init error.");
      ret = OB_ELECTION_ASYNC_LOG_WARN_INIT;
    }

    int32_t local_ip = ntohl(obsys::CNetUtil::getLocalAddr(config_.devname));
    int32_t local_port = static_cast<int32_t>(config_.rpc_port);

    config_.print();

    // initialize self address
    self_addr_.set_ipv4_addr(local_ip, local_port);
  }
  // init env
  if (OB_SUCC(ret)) {
    if (NULL == (logdir = new char[MAX_PATH_SIZE])) {
      STORAGE_LOG(ERROR, "new log dir error");
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (clogdir = new char[MAX_PATH_SIZE])) {
      STORAGE_LOG(ERROR, "new clog dir error");
      ret = OB_ERR_UNEXPECTED;
    } else if (NULL == (ilogdir = new char[MAX_PATH_SIZE])) {
      STORAGE_LOG(ERROR, "new clog dir error");
      ret = OB_ERR_UNEXPECTED;
    } else if (0 > (tmp_ret = snprintf(logdir, MAX_PATH_SIZE, "%s/slog", opts_.data_dir_ /*, opts_.appname_*/))) {
      STORAGE_LOG(ERROR, "concate log path fail", "ret", tmp_ret);
      ret = OB_ERR_UNEXPECTED;
    } else if (0 > (tmp_ret = snprintf(clogdir, MAX_PATH_SIZE, "%s/clog", opts_.data_dir_ /*, opts_.appname_*/))) {
      STORAGE_LOG(ERROR, "concate log path fail", "ret", tmp_ret);
      ret = OB_ERR_UNEXPECTED;
    } else if (0 > (tmp_ret = snprintf(ilogdir, MAX_PATH_SIZE, "%s/ilog", opts_.data_dir_ /*, opts_.appname_*/))) {
      STORAGE_LOG(ERROR, "concate log path fail", "ret", tmp_ret);
      ret = OB_ERR_UNEXPECTED;
    } else {
      env.data_dir_ = opts_.data_dir_;
      env.default_block_size_ = macro_block_size;
      env.log_spec_.log_dir_ = logdir;
      env.log_spec_.max_log_size_ = 256 << 20;
      env.clog_dir_ = clogdir;
      env.ilog_dir_ = ilogdir;
      env.index_cache_priority_ = 10;
      env.user_block_cache_priority_ = 1;
      env.user_row_cache_priority_ = 1;
      env.fuse_row_cache_priority_ = 1;
      env.bf_cache_priority_ = 1;
      env.clog_cache_priority_ = 1;
      env.index_clog_cache_priority_ = 1;
    }
  }
  // init schema service
  if (OB_SUCC(ret)) {
    ObSchemaGetterGuard* schema_guard = NULL;
    if (OB_FAIL(restore_schema_.init())) {
      STORAGE_LOG(ERROR, "restore_schema init fail", K(ret));
    } else if (OB_FAIL(restore_schema_.parse_from_file(schema_file, schema_guard))) {
      STORAGE_LOG(ERROR, "parse_from_file fail", K(ret));
    } else {
      schema_service_ = restore_schema_.schema_service_;
    }
    // if (OB_SUCCESS != (ret = schema_service_.init(schema_file))) {
    //  STORAGE_LOG(ERROR, "schema service init error", K(ret));
    //} else {
    //  STORAGE_LOG(INFO, "schema service init success");
    //}
  }
  // init global context
  if (OB_SUCC(ret)) {
    // gctx_.root_service_ = &root_service_;
    // gctx_.ob_service_ = &ob_service_;
    gctx_.schema_service_ = schema_service_;
    gctx_.config_ = &config_;
    gctx_.config_mgr_ = &config_mgr_;
    gctx_.pt_operator_ = &pt_operator_;
    // gctx_.srv_rpc_proxy_ = &srv_rpc_proxy_;
    // gctx_.rs_rpc_proxy_ = &rs_rpc_proxy_;
    GCTX.sql_proxy_ = &sql_proxy_;
    // gctx_.timer_ = &timer_;
    gctx_.self_addr_ = self_addr_;
    // gctx_.rs_mgr_ = &rs_mgr_;
    gctx_.par_ser_ = &partition_service_;
    gctx_.omt_ = &multi_tenant_;
    gctx_.session_mgr_ = &session_mgr_;
    // gctx_.sql_engine_ = &sql_engine_;
    gctx_.warm_up_start_time_ = &warm_up_start_time_;
    gctx_.global_last_merged_version_ = &global_last_merged_version_;
  }
  // init net frame
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = net_frame_.init())) {
      STORAGE_LOG(ERROR, "net frame init error", K(ret));
    } else if (OB_FAIL(batch_rpc_.init(
                   net_frame_.get_batch_rpc_req_transport(), net_frame_.get_high_prio_req_transport(), self_addr_))) {
      LOG_WARN("init batch rpc failed", K(ret));
    } else {
      STORAGE_LOG(INFO, "net frame init success");
    }
  }

  // init location cache
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = location_cache_def_.init())) {
      STORAGE_LOG(ERROR, "location cache init error", K(ret));
    } else if (NULL != location_cache) {
      location_cache_ = location_cache;
      STORAGE_LOG(INFO, "use extern location cache");
    } else {
      STORAGE_LOG(INFO, "location cache init success");
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = bandwidth_throttle_.init(1024 * 1024 * 60))) {
      STORAGE_LOG(ERROR, "failed to init bandwidth_throttle_", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    gctx_.rs_server_status_ = RSS_IS_WORKING;
  }

  // init global kv cache
  if (OB_SUCC(ret)) {
    ret = ObKVGlobalCache::get_instance().init();
  }

  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = init_tenant_mgr())) {
      STORAGE_LOG(WARN, "init tenant mgr failed", K(ret));
    } else {
      STORAGE_LOG(INFO, "init tenant mgr success");
    }
  }

  // init io
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObIOManager::get_instance().init())) {
      STORAGE_LOG(WARN, "io manager init failead", K(ret));
    }
  }

  // init partition service
  if (OB_SUCC(ret)) {
    // make log directory for election
    system("rm -rf log");
    system("mkdir log");
    if (OB_SUCCESS != (ret = partition_service_.init(env,
                           self_addr_,
                           &partition_cfy_,
                           schema_service_,
                           location_cache_,
                           &rs_mgr_,
                           &rs_cb_,
                           net_frame_.get_req_transport(),
                           &batch_rpc_,
                           rs_rpc_proxy_,
                           srv_rpc_proxy_,
                           sql_proxy_,
                           remote_sql_proxy_,
                           server_tracer_,
                           bandwidth_throttle_))) {
      STORAGE_LOG(WARN, "failed to init partitin service", K(ret));
    } else {
      STORAGE_LOG(INFO, "partition service init success");
    }
  }
  // init gts response rpc
  if (OB_SUCC(ret)) {
    if (OB_FAIL(gts_response_rpc_.init(net_frame_.get_req_transport(), self_addr_))) {
      LOG_ERROR("gts response rpc init failed", K(ret));
    } else {
      LOG_INFO("gts response rpc init success");
    }
  }

  // init gtx
  if (OB_SUCC(ret)) {
    if (OB_FAIL(gts_.init(partition_service_.get_election_mgr(), &gts_response_rpc_, self_addr_))) {
      LOG_ERROR("gts init failed", K(ret));
    } else {
      LOG_INFO("gts init success");
    }
  }

  // init gts local cache mgr
  if (OB_SUCC(ret)) {
    ObILocationAdapter* location_adapter =
        static_cast<ObTransService*>(partition_service_.get_trans_service())->get_location_adapter();
    if (OB_SUCCESS != (ret = OB_TS_MGR.init(self_addr_, location_adapter, net_frame_.get_req_transport(), &gts_))) {
      STORAGE_LOG(WARN, "init gts local cache mgr failed", K(ret));
    } else {
      STORAGE_LOG(INFO, "init gts local cache mgr success");
    }
  }

  // init multi tenant
  GCTX = gctx_;
  if (OB_SUCC(ret)) {
    if (OB_SUCCESS != (ret = init_multi_tenant())) {
      STORAGE_LOG(WARN, "init multi tenant failed", K(ret));
    } else {
      STORAGE_LOG(INFO, "init multi tenant success");
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
    STORAGE_LOG(INFO, "ob server inited success");
  }

  return ret;
}

int MockObServer::init_multi_tenant()
{
  int ret = OB_SUCCESS;

  if (OB_SUCCESS != (ret = multi_tenant_.init(self_addr_, 6))) {
    STORAGE_LOG(WARN, "init multi_tenant failed", K(ret));
  } else if (OB_SUCCESS != (ret = multi_tenant_.add_tenant(OB_SYS_TENANT_ID, 3, 3))) {
    STORAGE_LOG(WARN, "add sys tenant failed", K(ret));
  } else if (OB_SUCCESS != (ret = multi_tenant_.add_tenant(OB_ELECT_TENANT_ID, 5, 5))) {
    STORAGE_LOG(WARN, "add election tenant failed", K(ret));
  } else if (OB_SUCCESS != (ret = multi_tenant_.add_tenant(OB_SERVER_TENANT_ID, 5, 5))) {
    STORAGE_LOG(WARN, "add election tenant failed", K(ret));
  } else {
    multi_tenant_.start();
  }
  return ret;
}

int MockObServer::init_tenant_mgr()
{
  int ret = OB_SUCCESS;
  static const int64_t SYS_MEM_MIN = 8LL << 30;
  static const int64_t SYS_MEM_MAX = 16LL << 30;
  static const int64_t SERVER_TENANT_MEM_MIN = 8LL << 30;
  static const int64_t SERVER_TENANT_MEM_MAX = 16LL << 30;
  static const int64_t SERVER_MEM_MAX = 16LL << 30;
  ObTenantManager& omti = ObTenantManager::get_instance();
  obrpc::ObCommonRpcProxy common_rpc;
  share::ObRsMgr rs_mgr;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObTenantManager::get_instance().init(
            self_addr_, srv_rpc_proxy_, rs_rpc_proxy_, rs_mgr_, net_frame_.get_req_transport(), &config_))) {
      LOG_ERROR("Fail to init ObTenantManager, ", K(ret));
    } else if (OB_FAIL(omti.add_tenant(common::OB_SYS_TENANT_ID))) {
      LOG_ERROR("Fail to add sys tenant to tenant manager, ", K(ret));
    } else if (OB_SUCCESS != (ret = omti.set_tenant_mem_limit(OB_SYS_TENANT_ID, SYS_MEM_MIN, SYS_MEM_MAX))) {

      LOG_ERROR("Fail to set tenant mem limit", K(ret));
    } else if (OB_FAIL(omti.add_tenant(common::OB_SERVER_TENANT_ID))) {
      LOG_ERROR("Fail to add sys tenant to tenant manager, ", K(ret));
    } else if (OB_SUCCESS !=
               (ret = omti.set_tenant_mem_limit(OB_SERVER_TENANT_ID, SERVER_TENANT_MEM_MIN, SERVER_TENANT_MEM_MAX))) {

      LOG_ERROR("Fail to set tenant mem limit", K(ret));
    } else {
      lib::set_memory_limit(SERVER_MEM_MAX);
    }
  }
  return ret;
}

void MockObServer::destroy()
{
  partition_service_.destroy();
  ObKVGlobalCache::get_instance().destroy();
  STORAGE_LOG(INFO, "MockObServer::destroy().  destroy gloabal_cache\n");
  net_frame_.destroy();
  STORAGE_LOG(INFO, "MockObServer::destroy().  destroy net_frame\n");
  ObTenantManager& omti = ObTenantManager::get_instance();
  omti.destroy();
  multi_tenant_.destroy();
  ObIOManager::get_instance().destroy();
  ASYNC_LOG_DESTROY();
}

int MockObServer::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    STORAGE_LOG(WARN, "ob server not inited");
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (ret = net_frame_.start())) {
    STORAGE_LOG(WARN, "net frame start error", K(ret));
  } else if (OB_SUCCESS != partition_service_.start()) {
    STORAGE_LOG(WARN, "start partition service fail", K(ret));
  } else if (OB_FAIL(OB_TS_MGR.start())) {
    STORAGE_LOG(WARN, "start gts cache mgr error", K(ret));
  } else {
    STORAGE_LOG(INFO, "net frame start success");
  }

  // wait clog replay over
  if (OB_SUCC(ret)) {
    ret = partition_service_.wait_start_finish();
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "ob server start success");
  }

  return ret;
}

int MockObServer::stop()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    STORAGE_LOG(WARN, "ob server not inited");
    ret = OB_NOT_INIT;
  } else if (OB_SUCCESS != (partition_service_.stop())) {
    STORAGE_LOG(WARN, "partition service stop error", K(ret));
  } else if (OB_SUCCESS != (ret = net_frame_.stop())) {
    STORAGE_LOG(WARN, "net frame stop error", K(ret));
  } else {
    OB_TS_MGR.stop();
    STORAGE_LOG(INFO, "net frame stop success");
  }
  multi_tenant_.stop();

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "ob server stop success");
  }

  return ret;
}

int MockObServer::wait()
{
  int ret = OB_SUCCESS;
  partition_service_.wait();
  multi_tenant_.wait();
  net_frame_.wait();
  (void)OB_TS_MGR.wait();
  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "ob server wait success");
  }
  return ret;
}

ObPartitionService* MockObServer::get_partition_service()
{
  ObPartitionService* ps = NULL;

  if (!is_inited_) {
    STORAGE_LOG(WARN, "ob server not inited");
  } else {
    ps = &partition_service_;
  }

  return ps;
}

MockObLocationCache* MockObServer::get_location_cache()
{
  MockObLocationCache* lc = NULL;

  if (!is_inited_) {
    STORAGE_LOG(WARN, "ob server not inited");
  } else {
    lc = location_cache_;
  }

  return lc;
}

MockSchemaService* MockObServer::get_schema_service()
{
  MockSchemaService* ss = NULL;

  if (!is_inited_) {
    STORAGE_LOG(WARN, "ob server not inited");
  } else {
    ss = schema_service_;
  }

  return ss;
}

}  // namespace unittest
}  // namespace oceanbase

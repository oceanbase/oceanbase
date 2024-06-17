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

#define USING_LOG_PREFIX CLIENT
#include "ob_table_service_client.h"
#include "ob_table_impl.h"
#include "ob_table_rpc_impl.h"
#include "ob_hkv_table.h"

#include "ob_table_service_config.h"

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"
#include "lib/task/ob_timer.h"
#include "lib/encrypt/ob_encrypted_helper.h"
#include "share/schema/ob_multi_version_schema_service.h" // ObMultiVersionSchemaService
#include "share/schema/ob_schema_getter_guard.h"          // ObSchemaGetterGuard
#include "share/schema/ob_schema_struct.h"                // TableStatus
#include "share/table/ob_table_rpc_proxy.h"               // ObTableRpcProxy
#include "share/ob_thread_mgr.h"
#include "rpc/obrpc/ob_net_client.h"      // ObNetClient
#include "sql/optimizer/ob_table_location.h"  // ObTableLocation
#include "sql/session/ob_sql_session_info.h"
#include "ob_tablet_location_proxy.h"
#include "observer/ob_signal_handle.h"
#include "share/ob_get_compat_mode.h"
#include "observer/ob_server_struct.h"
#include "share/ob_schema_status_proxy.h"
#include "share/rc/ob_tenant_base.h"
using namespace oceanbase::common;
using namespace oceanbase::table;
using namespace oceanbase::share;

ObTableServiceClientOptions::ObTableServiceClientOptions()
    :default_request_options_(),
     net_io_thread_num_(16)
{}

namespace oceanbase
{
namespace table
{
class ObTableServiceClientEnvFactory;
class ObTableServiceClientEnv final
{
public:
  ObTableServiceClientEnv()
      :inited_(false),
       tg_id_(lib::TGDefIDs::TblCliSqlPool),
       schema_status_proxy_(sys_sql_client_),
       schema_service_(share::schema::ObMultiVersionSchemaService::get_instance()),
       location_getter_(&location_cache_)
  {}
  ~ObTableServiceClientEnv();
  int init(const ObString &host,
           int32_t mysql_port,
           int32_t rpc_port,
           const ObString &sys_user_name,
           const ObString &sys_user_password,
           int64_t net_io_thread_num);
  void destroy();

  bool inited() const { return inited_; }
  share::schema::ObMultiVersionSchemaService &get_schema_service() { return schema_service_; }
  ObITabletLocationGetter &get_location_getter() { return *location_getter_; }
  obrpc::ObTableRpcProxy &get_table_rpc_proxy() { return table_rpc_proxy_; }
  int get_timer_tg_id() const { return tg_id_; }
  const ObAddr &get_one_server() { return addr_; }

  static int init_sql_client(const ObString &host, int32_t port, const ObString &tenant,
                             const ObString &user, const ObString &passwd, const ObString &db,
                             int tg_id,
                             common::sqlclient::ObSingleMySQLServerProvider& server_provider,
                             common::sqlclient::ObMySQLConnectionPool &conn_pool,
                             common::ObMySQLProxy &sql_client);
private:
  friend class ObTableServiceClientEnvFactory;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableServiceClientEnv);
  int init_net_client(int64_t net_io_thread_num, const ObString &host, int32_t port);
  int init_schema_service();
private:
  bool inited_;
  //thread to deal signals
  observer::ObSignalHandle signal_handle_;
  // cluster address
  ObAddr addr_;
  // schema service
  const int tg_id_;
  common::sqlclient::ObSingleMySQLServerProvider sys_single_server_provider_;
  common::sqlclient::ObMySQLConnectionPool sys_conn_pool_;
  common::ObMySQLProxy sys_sql_client_;
  share::ObSchemaStatusProxy schema_status_proxy_;
  ObTableServiceConfig config_;
  share::schema::ObMultiVersionSchemaService &schema_service_;
  // for partition location
  ObTabletLocationProxy location_proxy_;
  ObTabletLocationCache location_cache_;
  ObITabletLocationGetter *location_getter_;
  // for Table API proxy
  obrpc::ObNetClient net_client_;
  obrpc::ObTableRpcProxy table_rpc_proxy_;
};

ObTableServiceClientEnv::~ObTableServiceClientEnv()
{
  destroy();
}

int ObTableServiceClientEnv::init_sql_client(const ObString &host, int32_t port, const ObString &tenant,
                                             const ObString &user, const ObString &passwd, const ObString &db,
                                             int tg_id,
                                             sqlclient::ObSingleMySQLServerProvider& server_provider,
                                             sqlclient::ObMySQLConnectionPool &conn_pool,
                                             ObMySQLProxy &sql_client)
{
  int ret = OB_SUCCESS;
  UNUSED(tenant);
  ObAddr addr;
  ObConnPoolConfigParam conn_pool_config;
  const int64_t sql_conn_timeout_us = 10L*1000*1000;
  const int64_t sql_query_timeout_us = 10L*1000*1000;
  conn_pool_config.reset();
  conn_pool_config.connection_refresh_interval_ = 60L * 1000L * 1000L; // us
  conn_pool_config.sqlclient_wait_timeout_ = sql_conn_timeout_us / 1000000L; // s
  conn_pool_config.connection_pool_warn_time_ = 60L * 1000L * 1000L;  // us
  conn_pool_config.long_query_timeout_ = sql_query_timeout_us;     // us
  conn_pool_config.sqlclient_per_observer_conn_limit_ = 500;        // @todo as param

  if (!addr.set_ip_addr(host, port)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to set addr", K(host), K(port));
  } else if (FALSE_IT(server_provider.init(addr))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to init provider", K(ret));
  } else if (FALSE_IT(conn_pool.update_config(conn_pool_config))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to update config", K(ret));
  } else if (FALSE_IT(conn_pool.set_server_provider(&server_provider))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("failed to init provider", K(ret));
  } else if (OB_FAIL(conn_pool.set_db_param(user, passwd, db))) {
    LOG_WARN("failed to set db param", K(ret));
  } else if (OB_FAIL(conn_pool.start(tg_id))) {
    LOG_WARN("failed to start connection pool", K(ret));
  } else if (OB_FAIL(sql_client.init(&conn_pool))) {
    LOG_ERROR("failed to init sql client", K(ret));
  } else {
    LOG_DEBUG("connection pool init succ", K(addr));
  }
  return ret;
}

int ObTableServiceClientEnv::init_net_client(int64_t net_io_thread_num, const ObString &host, int32_t port)
{
  int ret = OB_SUCCESS;
  rpc::frame::ObNetOptions opt;
  opt.rpc_io_cnt_ = static_cast<int>(net_io_thread_num);
  opt.mysql_io_cnt_ = 0;
  if (!addr_.set_ip_addr(host, port)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("failed to set addr", K(host), K(port));
  } else if (OB_FAIL(net_client_.init(opt))) {
    LOG_ERROR("init net client fail", K(ret), K(net_io_thread_num));
  } else if (OB_FAIL(net_client_.get_proxy(table_rpc_proxy_))) {
    LOG_ERROR("net client get proxy fail", K(ret));
  } else {
    LOG_INFO("init rpc succ", K(net_io_thread_num));
  }
  return ret;
}

int ObTableServiceClientEnv::init_schema_service()
{
  int ret = OB_SUCCESS;
  static const int64_t max_version_count = 2;
  // init the Schema Service
  uint64_t cluster_version = CLUSTER_CURRENT_VERSION;
  if (OB_FAIL(common::ObClusterVersion::get_instance().init(cluster_version))) {
    LOG_WARN("fail to init cluster version", K(ret), K(cluster_version));
  } else if (OB_FAIL(schema_service_.init(&sys_sql_client_, NULL, &config_,
                                          max_version_count, max_version_count))) {
    LOG_WARN("failed to init schema service", K(ret), K(max_version_count));
  } else {
    // refresh Schema
    share::schema::ObSchemaService::g_ignore_column_retrieve_error_ = true;
    share::schema::ObSchemaService::g_liboblog_mode_ = true;
    ObSEArray<uint64_t, 1> tenant_ids;
    // init GCTX
    bool check_bootstrap = false;
    GCTX.schema_status_proxy_ = &schema_status_proxy_;
    GCTX.schema_service_ = &schema_service_;
    if (OB_FAIL(schema_service_.refresh_and_add_schema(tenant_ids, check_bootstrap))) {
      LOG_WARN("fail to refresh tenant schema", K(ret));
    }

    LOG_INFO("init schema service", K(ret), K(max_version_count), K(cluster_version));
  }
  return ret;
}

int ObTableServiceClientEnv::init(const ObString &host,
                                  int32_t mysql_port,
                                  int32_t rpc_port,
                                  const ObString &sys_user_name,
                                  const ObString &sys_user_password,
                                  int64_t net_io_thread_num)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    if (OB_FAIL(signal_handle_.start())) {
      LOG_ERROR("Start signal handle error", K(ret));
    } else if (OB_FAIL(init_net_client(net_io_thread_num, host, rpc_port))) {
      LOG_WARN("failed to init net client", K(ret));
    } else if (OB_FAIL(TG_START(tg_id_))) {
      LOG_WARN("failed to init connection pool timer", K(ret));
    } else if (OB_FAIL(init_sql_client(host, mysql_port, ObString::make_string("sys"),
                                       sys_user_name, sys_user_password,
                                       ObString::make_string("oceanbase"),
                                       tg_id_, sys_single_server_provider_,
                                       sys_conn_pool_, sys_sql_client_))) {
      LOG_WARN("failed to init sys connection pool", K(ret));
    } else if (OB_FAIL(ObCompatModeGetter::instance().init(&sys_sql_client_))) {
      LOG_WARN("fail to init get compat mode server");
    } else if (OB_FAIL(schema_status_proxy_.init())) {
      LOG_WARN("fail to init schema status proxy", K(ret));
    } else if (OB_FAIL(init_schema_service())) {
      LOG_WARN("failed to init schema service", K(ret));
    } else if (OB_FAIL(location_proxy_.init(sys_sql_client_))) {
      LOG_WARN("failed to init location proxy", K(ret));
    } else if (OB_FAIL(location_cache_.init(location_proxy_))) {
      LOG_WARN("failed to init location cache", K(ret));
    } else {
      inited_ = true;
      LOG_INFO("client env inited", K(host), K(mysql_port), K(rpc_port), K(net_io_thread_num));
    }
  }
  return ret;
}

void ObTableServiceClientEnv::destroy()
{
  sys_conn_pool_.stop();
  TG_DESTROY(tg_id_);
  net_client_.destroy();
  signal_handle_.stop();
  signal_handle_.wait();
}

class ObTableServiceClientEnvFactory final
{
public:
  static int open_client_env(const ObString &host,
                             int32_t mysql_port,
                             int32_t rpc_port,
                             const ObString &sys_user_name,
                             const ObString &sys_user_password,
                             int64_t net_io_thread_num,
                             ObTableServiceClientEnv* &client_env);
  static void close_client_env(ObTableServiceClientEnv* client_env);
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableServiceClientEnvFactory);
  ObTableServiceClientEnvFactory() = delete;
  ~ObTableServiceClientEnvFactory() = delete;
};
// @todo support multiple ob clusters
int ObTableServiceClientEnvFactory::open_client_env(
    const ObString &host,
    int32_t mysql_port,
    int32_t rpc_port,
    const ObString &sys_user_name,
    const ObString &sys_user_password,
    int64_t net_io_thread_num,
    ObTableServiceClientEnv* &client_env)
{
  static ObTableServiceClientEnv CLIENT_ENV;
  int ret = OB_SUCCESS;
  if (CLIENT_ENV.inited()) {
    LOG_INFO("client env already inited", K(ret));
    ObAddr host_addr;
    host_addr.set_ip_addr(host, rpc_port);
    if (CLIENT_ENV.addr_ != host_addr) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("multiple cluster not supported yet", K(ret), K(host_addr));
    } else {
      client_env = &CLIENT_ENV;
    }
  } else if (OB_FAIL(CLIENT_ENV.init(host, mysql_port, rpc_port, sys_user_name, sys_user_password, net_io_thread_num))) {
    LOG_WARN("failed to init client env", K(ret));
  } else {
    client_env = &CLIENT_ENV;
  }
  return ret;
}

void ObTableServiceClientEnvFactory::close_client_env(ObTableServiceClientEnv* client_env)
{
  UNUSED(client_env);
}

/// Implementation class of ObTableServiceClient
class ObTableServiceClientImpl
{
public:
  ObTableServiceClientImpl();
  virtual ~ObTableServiceClientImpl();

  int init(const ObString &host,
           int32_t mysql_port,
           int32_t rpc_port,
           const ObString &tenant,
           const ObString &user,
           const ObString &password,
           const ObString &database,
           const ObString &sys_user_password,
           ObTableServiceClient *client);
  void destroy();
  void set_options(const ObTableServiceClientOptions &options);

  int alloc_table(const ObString &table_name, ObTable *&table);
  void free_table(ObTable *table);

  int create_hkv_table(const ObString &table_name, int64_t partition_num, bool if_not_exists);
  int drop_hkv_table(const ObString &table_name, bool if_exists);

  int alloc_hkv_table(const ObString &table_name, ObHKVTable *&table);
  void free_hkv_table(ObHKVTable *table);

  // location service
  int get_tablet_location(const ObString &table_name, const ObRowkey &rowkey,
                          ObTabletLocation &tablet_location, uint64_t &table_id,
                          ObTabletID &tablet_id);
  int get_tablets_locations(const ObString &table_name, const common::ObIArray<ObRowkey> &rowkeys,
                            common::ObIArray<ObTabletLocation> &tablets_locations,
                            uint64_t &table_id,
                            common::ObIArray<ObTabletID> &tablet_ids,
                            common::ObIArray<sql::RowkeyArray> &rowkeys_per_tablet);
  // location service for ObTableQuery
  int get_tablet_location(const ObString &table_name, const ObString &index_name,
                          const common::ObNewRange &index_prefix, ObTabletLocation &tablet_location,
                          uint64_t &table_id, ObTabletID &tablet_id);
  // schema service
  int get_rowkey_columns(const ObString &table_name, common::ObStrings &rowkey_columns);
  int get_table_id(const ObString &table_name, uint64_t &table_id);

  common::ObMySQLProxy &get_user_sql_client() { return user_sql_client_; }
  obrpc::ObTableRpcProxy &get_table_rpc_proxy() { return client_env_->get_table_rpc_proxy(); }
  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_database_id() const { return database_id_; }
  const ObString &get_credential() const { return credential_; }
public:
  // for debug purpose only
  int alloc_table_v1(const ObString &table_name, ObTable *&table);
  int alloc_table_v2(const ObString &table_name, ObTable *&table);
  int alloc_hkv_table_v1(const ObString &table_name, ObHKVTable *&table);
  int alloc_hkv_table_v2(const ObString &table_name, ObHKVTable *&table);
private:
  int check_set_tenant_db(const ObString &tenant_name, const ObString &db_name);
  int verify_user(const ObString &tenant, const ObString &user, const ObString &pass, const ObString &db);
  int init_tenant_env(const uint64_t &tenant_id);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObTableServiceClientImpl);
private:
  bool inited_;
  ObTableServiceClient *client_;
  ObTableServiceClientEnv *client_env_;
  ObTableServiceClientOptions options_;
  ObArenaAllocator allocator_;
  ObString tenant_name_;
  ObString user_name_;
  ObString database_name_;
  uint64_t tenant_id_;
  uint64_t user_id_;
  uint64_t database_id_;
  // for ObTableImpl
  common::sqlclient::ObSingleMySQLServerProvider user_single_server_provider_;
  common::sqlclient::ObMySQLConnectionPool user_conn_pool_;
  common::ObMySQLProxy user_sql_client_;
  // for ObTableRpcImpl
  char credential_buf_[256];
  ObString credential_;
  // for partition location
  sql::ObSQLSessionInfo session_;  // for get_partition_location only
};

ObTableServiceClientImpl::ObTableServiceClientImpl()
    :inited_(false),
     client_(NULL),
     client_env_(NULL),
     tenant_id_(OB_INVALID_ID),
     user_id_(OB_INVALID_ID),
     database_id_(OB_INVALID_ID)
{}

ObTableServiceClientImpl::~ObTableServiceClientImpl()
{
  destroy();
  LOG_INFO("destruct ObTableServiceClientImpl");
}

int ObTableServiceClientImpl::init(const ObString &host,
                                   int32_t mysql_port,
                                   int32_t rpc_port,
                                   const ObString &tenant,
                                   const ObString &user,
                                   const ObString &passwd,
                                   const ObString &db,
                                   const ObString &sys_root_password,
                                   ObTableServiceClient *client)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else {
    static const uint32_t sess_version = 0;
    static const uint32_t sess_id = 1;
    static const uint64_t proxy_sess_id = 1;
    if (NULL == client) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("client is NULL", K(ret));
    } else if (OB_FAIL(ObTableServiceClientEnvFactory::open_client_env(host, mysql_port, rpc_port,
                                                                       ObString::make_string("root"),
                                                                       sys_root_password, options_.net_io_thread_num(), client_env_))) {
      LOG_WARN("failed to open client env", K(ret));
    } else if (OB_FAIL(verify_user(tenant, user, passwd, db))) {
      LOG_WARN("failed to login", K(ret), K(tenant), K(user), K(db));
    } else if (OB_FAIL(ObTableServiceClientEnv::init_sql_client(host, mysql_port,
        tenant, user, passwd, db,
        client_env_->get_timer_tg_id(), user_single_server_provider_,
        user_conn_pool_, user_sql_client_))) {
      LOG_WARN("failed to init user connection pool", K(ret));
    } else if (OB_FAIL(check_set_tenant_db(tenant, db))) {
      LOG_WARN("failed to check tenant and database", K(ret), K(tenant), K(db));
    } else if (OB_FAIL(init_tenant_env(tenant_id_))) {
      LOG_WARN("failed to init tenant env", K(ret), K(tenant_id_));
    } else if (OB_FAIL(session_.test_init(sess_version, sess_id, proxy_sess_id, &allocator_))) {
      LOG_WARN("init session failed", K(ret));
    } else if (OB_FAIL(session_.load_default_sys_variable(false, true))) {
      LOG_WARN("failed to load default sys var", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, tenant, tenant_name_))) {
      LOG_WARN("failed to copy string", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, db, database_name_))) {
      LOG_WARN("failed to copy string", K(ret));
    } else if (OB_FAIL(ob_write_string(allocator_, user, user_name_))) {
      LOG_WARN("failed to copy string", K(ret));
    } else {
      client_ = client;
      inited_ = true;
      LOG_INFO("table service client inited", K(tenant), K(user), K(db));
    }
  }
  return ret;
}

void ObTableServiceClientImpl::set_options(const ObTableServiceClientOptions &options)
{
  options_ = options;
}

int ObTableServiceClientImpl::check_set_tenant_db(const ObString &tenant_name, const ObString &database_name)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(OB_SYS_TENANT_ID, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_id(tenant_name, tenant_id_))) {
    LOG_WARN("failed to get tenant id", K(ret), K(tenant_name));
  } else if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret), K_(tenant_id));
  } else if (OB_FAIL(schema_guard.get_database_id(tenant_id_, database_name, database_id_))) {
    LOG_WARN("failed to get db id", K(ret), K(database_name));
  } else if (OB_INVALID_ID == tenant_id_ || OB_INVALID_ID == database_id_) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant name or database name", K(ret), K(tenant_name), K(database_name),
             K_(tenant_id), K_(database_id));
  } else
  {
    LOG_INFO("check tenant and database", K(tenant_name), K_(tenant_id), K(database_name), K_(database_id));
  }
  return ret;
}

int ObTableServiceClientImpl::verify_user(const ObString &tenant, const ObString &user, const ObString &pass, const ObString &db)
{
  int ret = OB_SUCCESS;
  char scramble_buf[21];  // don't initialize for random string
  char pass_secret_buf[21];
  ObTableLoginRequest login_request;
  login_request.auth_method_ = 1;
  login_request.client_type_ = 1;
  login_request.client_version_ = 1;
  login_request.reserved1_ = 0;
  login_request.client_capabilities_ = 0;
  login_request.max_packet_size_ = 64*1024*1024;
  login_request.reserved2_ = 0;
  login_request.reserved3_ = 0;
  login_request.tenant_name_ = tenant;
  login_request.user_name_ = user;
  login_request.database_name_ = db;
  login_request.ttl_us_ = 0;
  login_request.pass_scramble_.assign_ptr(scramble_buf, 20);
  int64_t pos = 0;
  if (OB_FAIL(ObEncryptedHelper::encrypt_password(pass,
                                                  login_request.pass_scramble_,
                                                  pass_secret_buf,
                                                  sizeof(pass_secret_buf),
                                                  pos))) {
    LOG_ERROR("encrypt password fail", K(ret));
  } else {
    login_request.pass_secret_.assign_ptr(pass_secret_buf, static_cast<int32_t>(pos));
    ObTableLoginResult login_result;
    if (OB_FAIL(client_env_->get_table_rpc_proxy()
                .to(client_env_->get_one_server())
                .as(OB_SYS_TENANT_ID)
                .login(login_request, login_result))) {
      LOG_WARN("login failed", K(ret), K(login_request));
    } else if (256 < login_result.credential_.length()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("credential is too long", K(ret), K(login_result));
    } else {
      MEMCPY(credential_buf_, login_result.credential_.ptr(), login_result.credential_.length());
      credential_.assign_ptr(credential_buf_, login_result.credential_.length());
      tenant_id_ = login_result.tenant_id_;
      user_id_ = login_result.user_id_;
      database_id_ = login_result.database_id_;

      LOG_INFO("login succ", K(login_result));
    }
  }
  return ret;
}

int ObTableServiceClientImpl::init_tenant_env(const uint64_t &tenant_id)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_TENANT_ID == tenant_id) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", K(ret));
  } else {
    ObTenantBase ctx(tenant_id);
    *ObTenantEnv::get_tenant_local() = ctx;
    ob_get_tenant_id() = ctx.id();
  }
  return ret;
}

void ObTableServiceClientImpl::destroy()
{
  user_conn_pool_.stop();
  if (NULL != client_env_) {
    ObTableServiceClientEnvFactory::close_client_env(client_env_);
    client_env_ = NULL;
  }
  inited_ = false;
  LOG_INFO("table service client destroyed", K_(tenant_name), K_(user_name), K_(database_name));
}

int ObTableServiceClientImpl::alloc_table(const ObString &table_name, ObTable *&table)
{
  return alloc_table_v2(table_name, table);
}

int ObTableServiceClientImpl::alloc_table_v1(const ObString &table_name, ObTable *&table)
{
  int ret = OB_SUCCESS;
  if (NULL != table) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    table = new(std::nothrow) ObTableImpl();
    if (OB_UNLIKELY(NULL == table)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObTableImpl", K(ret));
    } else if (OB_FAIL(table->init(*client_, table_name))) {
      LOG_WARN("failed to init table", K(ret), K(table_name));
    } else {
      table->set_default_request_options(options_.default_request_options());
    }
  }
  return ret;
}

int ObTableServiceClientImpl::alloc_table_v2(const ObString &table_name, ObTable *&table)
{
  int ret = OB_SUCCESS;
  if (NULL != table) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    table = new(std::nothrow) ObTableRpcImpl();
    if (OB_UNLIKELY(NULL == table)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObTableRpcImpl", K(ret));
    } else if (OB_FAIL(table->init(*client_, table_name))) {
      LOG_WARN("failed to init table", K(ret), K(table_name));
    } else {
      table->set_default_request_options(options_.default_request_options());
    }
  }
  return ret;
}

void ObTableServiceClientImpl::free_table(ObTable *table)
{
  if (NULL != table) {
    delete table;
    table = NULL;
  }
}

int ObTableServiceClientImpl::get_rowkey_columns(const ObString &table_name, ObStrings &rowkey_columns)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  const bool is_index = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, database_id_, table_name, is_index, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K_(tenant_id), K_(database_id), K(table_name));
  } else if (NULL == table_schema) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table schema is NULL", K_(tenant_id), K_(database_id), K(table_name));
  } else {
    const common::ObRowkeyInfo &rowkey_info = table_schema->get_rowkey_info();
    uint64_t cid = OB_INVALID_ID;
    const share::schema::ObColumnSchemaV2 *column_schema = NULL;
    int64_t N = rowkey_info.get_size();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(rowkey_info.get_column_id(i, cid))) {
        LOG_WARN("failed to column id");
      } else if (NULL == (column_schema = table_schema->get_column_schema(cid))) {
        ret = OB_ENTRY_NOT_EXIST;
        LOG_WARN("column schema not exist", K(ret), K(table_name), K(cid));
      } else if (OB_FAIL(rowkey_columns.add_string(column_schema->get_column_name_str()))) {
        LOG_WARN("failed to add string", K(ret));
      }
    } // end for
  }
  return ret;
}

int ObTableServiceClientImpl::get_table_id(const ObString &table_name, uint64_t &table_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const bool is_index = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(tenant_id_, database_id_, table_name, is_index,
                                               share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K_(tenant_id), K_(database_id), K(table_name));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("get invalid id", K(ret), K(table_name), K(table_id));
  }
  return ret;
}

int ObTableServiceClientImpl::alloc_hkv_table(const ObString &table_name, ObHKVTable *&table)
{
  return alloc_hkv_table_v2(table_name, table);
}

int ObTableServiceClientImpl::alloc_hkv_table_v1(const ObString &table_name, ObHKVTable *&table)
{
  int ret = OB_SUCCESS;
  if (NULL != table) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    table = new(std::nothrow) ObHKVTable();
    ObTable* tbl = NULL;
    if (OB_FAIL(alloc_table_v1(table_name, tbl))) {
      LOG_WARN("failed to alloc table", K(ret), K(table_name));
    } else if (OB_UNLIKELY(NULL == table)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObTableImpl", K(ret));
    } else if (OB_FAIL(table->init(*client_, tbl))) {
      LOG_WARN("failed to init table", K(ret), K(table_name));
    } else {
      tbl->set_default_request_options(options_.default_request_options());
    }
    if (OB_FAIL(ret)) {
      if (NULL != tbl) {
        free_table(tbl);
        tbl = NULL;
      }
      if (NULL != table) {
        free_hkv_table(table);
        table = NULL;
      }
    }
  }
  return ret;
}

int ObTableServiceClientImpl::alloc_hkv_table_v2(const ObString &table_name, ObHKVTable *&table)
{
  int ret = OB_SUCCESS;
  if (NULL != table) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    table = new(std::nothrow) ObHKVTable();
    ObTable* tbl = NULL;
    if (OB_FAIL(alloc_table_v2(table_name, tbl))) {
      LOG_WARN("failed to alloc table", K(ret), K(table_name));
    } else if (OB_UNLIKELY(NULL == table)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to new ObTableImpl", K(ret));
    } else if (OB_FAIL(table->init(*client_, tbl))) {
      LOG_WARN("failed to init table", K(ret), K(table_name));
    } else {
      tbl->set_default_request_options(options_.default_request_options());
    }
    if (OB_FAIL(ret)) {
      if (NULL != tbl) {
        free_table(tbl);
        tbl = NULL;
      }
      if (NULL != table) {
        free_hkv_table(table);
        table = NULL;
      }
    }
  }
  return ret;
}

void ObTableServiceClientImpl::free_hkv_table(ObHKVTable *table)
{
  if (NULL != table) {
    if (NULL != table->tbl_) {
      free_table(table->tbl_);
      table->tbl_ = NULL;
    }
    delete table;
    table = NULL;
  }
}

int ObTableServiceClientImpl::create_hkv_table(const ObString &table_name, int64_t partition_num, bool if_not_exists)
{
  UNUSED(table_name);
  UNUSED(partition_num);
  UNUSED(if_not_exists);
  return OB_NOT_IMPLEMENT;
}

int ObTableServiceClientImpl::drop_hkv_table(const ObString &table_name, bool if_exists)
{
  UNUSED(table_name);
  UNUSED(if_exists);
  return OB_NOT_IMPLEMENT;
}

int ObTableServiceClientImpl::get_tablet_location(const ObString &table_name, const ObRowkey &rowkey,
                                                  ObTabletLocation &tablet_location, uint64_t &table_id,
                                                  ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  sql::ObTableLocation location_calc;
  const ObTableSchema *table_schema;
  const bool is_index = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(tenant_id_, database_id_, table_name, is_index,
                                               share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K_(tenant_id), K_(database_id), K(table_name));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("get invalid id", K(ret), K(table_name), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id_), K(table_id), K(table_schema));
  } else if (!table_schema->is_partitioned_table()) {
    tablet_id = table_schema->get_tablet_id();
  } else {
    ObSEArray<ObObjectID, 1> part_ids;
    ObSEArray<ObTabletID, 1> tablet_ids;
    ObSEArray<sql::RowkeyArray, 3> rowkey_lists;
    ObSEArray<ObRowkey, 1> rowkeys;
    if (OB_FAIL(rowkeys.push_back(rowkey))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(session_, schema_guard, table_id,
                                                                       rowkeys, tablet_ids, part_ids))) {
      LOG_WARN("failed to calc partition id and tablet id", K(ret));
    } else if (1 != part_ids.count() || 1 != tablet_ids.count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid partition ids and tablet ids", K(ret), K(part_ids), K(tablet_ids));
    } else {
      tablet_id = tablet_ids.at(0);
    }
  }

  if (OB_SUCC(ret)) {
    const bool force_renew = false;
    if (OB_FAIL(client_env_->get_location_getter().get_tablet_location(tenant_name_, tenant_id_, database_name_, table_name,
                                                                       table_id, tablet_id, force_renew,
                                                                       tablet_location))) {
      LOG_WARN("failed to get location", K(ret), K(table_name), K(table_id));
    } else {
      LOG_DEBUG("[yzfdebug] get tablet id", K(ret), K(tablet_id), K(tablet_location));
    }
  }

  return ret;
}

int ObTableServiceClientImpl::get_tablets_locations(const ObString &table_name, const common::ObIArray<ObRowkey> &rowkeys,
                                                    common::ObIArray<share::ObTabletLocation> &tablets_locations,
                                                    uint64_t &table_id,
                                                    common::ObIArray<ObTabletID> &tablet_ids,
                                                    common::ObIArray<sql::RowkeyArray> &rowkeys_per_tablet)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  sql::ObTableLocation location_calc;
  const ObTableSchema *table_schema;
  const bool is_index = false;
  const bool force_renew = false;
  hash::ObHashMap<ObTabletID, uint64_t> tablet_idx_map;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_id(tenant_id_, database_id_, table_name, is_index,
                                               share::schema::ObSchemaGetterGuard::ALL_NON_HIDDEN_TYPES, table_id))) {
    LOG_WARN("failed to get table id", K(ret), K_(tenant_id), K_(database_id), K(table_name));
  } else if (OB_INVALID_ID == table_id) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("get invalid id", K(ret), K(table_name), K(table_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id_), K(table_id), K(table_schema));
  } else if (!table_schema->is_partitioned_table()) {
    tablet_ids.push_back(table_schema->get_tablet_id()); // don't need to fill rowkey_per_part
  } else if (OB_FAIL(tablet_idx_map.create(rowkeys.count(), ObModIds::TABLE_PROC))) {
    LOG_WARN("failed to create hash map", K(ret));
  } else {
    uint64_t tablet_idx = 0;
    sql::RowkeyArray rowkey_list;
    ObSEArray<ObRowkey, 1> rowkey;
    ObSEArray<ObObjectID, 1> tmp_part_ids;
    ObSEArray<ObTabletID, 1> tmp_tablet_ids;
    for (uint64_t i = 0; OB_SUCC(ret) && i < rowkeys.count(); ++i) {
      rowkey.reuse();
      tmp_part_ids.reuse();
      tmp_tablet_ids.reuse();
      if (OB_FAIL(rowkey.push_back(rowkeys.at(i)))) {
        LOG_WARN("failed to push back rowkey", K(ret));
      } else if (OB_FAIL(location_calc.calculate_partition_ids_by_rowkey(session_, schema_guard, table_id, rowkey, tmp_tablet_ids, tmp_part_ids))) {
        LOG_WARN("failed to calc partition id and tablet id", K(ret));
      } else if (1 != tmp_part_ids.count() || 1 != tmp_tablet_ids.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid partition ids or tablet ids", K(ret), K(tmp_part_ids), K(tmp_tablet_ids));
      } else if (OB_SUCC(tablet_idx_map.get_refactored(tmp_tablet_ids.at(0), tablet_idx))) {
      } else if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to get part idx", K(ret));
      } else {
        ret = OB_SUCCESS;
        tablet_idx = tablet_ids.count();
        if (OB_FAIL(tablet_idx_map.set_refactored(tmp_tablet_ids.at(0), tablet_idx))) {
          LOG_WARN("failed to set part idx", K(ret));
        } else if (OB_FAIL(tablet_ids.push_back(tmp_tablet_ids.at(0)))) {
          LOG_WARN("failed to push back tablet id", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_UNLIKELY(tablet_idx < rowkeys_per_tablet.count())) {
          if (OB_FAIL(rowkeys_per_tablet.at(tablet_idx).push_back(i))) {
            LOG_WARN("failed to push back part idx", K(ret));
          }
        } else if (OB_LIKELY(rowkeys_per_tablet.count() == tablet_idx)) {
          rowkey_list.reuse();
          if (OB_FAIL(rowkey_list.push_back(i))) {
            LOG_WARN("failed to push back rowkey", K(ret));
          } else if (OB_FAIL(rowkeys_per_tablet.push_back(rowkey_list))) {
            LOG_WARN("failed to push back rowkey list", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet_idx is invalid", K(tablet_idx), K(rowkeys_per_tablet.count()));
        }
      }
    } // end for
  }
  
  if (OB_SUCC(ret)) {
    ObTabletLocation tablet_location;
    const int64_t N = tablet_ids.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i) {
      tablet_location.reset();
      if (OB_FAIL(client_env_->get_location_getter().get_tablet_location(tenant_name_, tenant_id_, database_name_, table_name,
                                                                         table_id, tablet_ids.at(i), force_renew,
                                                                         tablet_location))) {
        LOG_WARN("failed to get location", K(ret), K(table_name), K(table_id));
      } else if (OB_FAIL(tablets_locations.push_back(tablet_location))) {
        LOG_WARN("failed to push back", K(ret));
      } else {
        LOG_DEBUG("[yzfdebug] get tablet id", K(ret), K(i), "tablet_id", tablet_ids.at(i), K(tablet_location));
      }
    } // end for
  }
  return ret;
}

int ObTableServiceClientImpl::get_tablet_location(const ObString &table_name, const ObString &index_name,
                                                  const common::ObNewRange &index_prefix, ObTabletLocation &tablet_location,
                                                  uint64_t &table_id, ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  share::schema::ObSchemaGetterGuard schema_guard;
  const share::schema::ObTableSchema *table_schema = NULL;
  const bool is_index = false;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(client_env_->get_schema_service().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("failed to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, database_id_, table_name, is_index, table_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K_(tenant_id), K_(database_id), K(table_name));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNKNOWN_TABLE;
    LOG_WARN("table schema is NULL", K_(tenant_id), K_(database_id), K(table_name));
  } else {
    // @todo partitioned table for query is not supported
    table_id = table_schema->get_table_id();
    tablet_id = table_schema->get_tablet_id();
    UNUSED(index_name);
    UNUSED(index_prefix);

    tablet_location.reset();
    ObTabletReplicaLocation fake_leader_loc;
    ObLSRestoreStatus role_status;
    if (OB_FAIL(fake_leader_loc.init_without_check(client_env_->get_one_server(), ObRole::LEADER,
                                       0, ObReplicaType::REPLICA_TYPE_FULL, ObReplicaProperty(),
                                       role_status, 1))) {
      LOG_WARN("fail to init tablet replication location", K(ret));
    } else if (OB_FAIL(tablet_location.add_replica_location(fake_leader_loc))) {
      LOG_WARN("failed to push back", K(ret));
    } else {
      tablet_location.set_tenant_id(tenant_id_);
      tablet_location.set_tablet_id(tablet_id);
      tablet_location.set_renew_time(ObTimeUtility::current_time());
    }
  }
  return ret;
}

} // end namespace table
} // end namespace oceanbase

////////////////////////////////////////////////////////////////
ObTableServiceClient *ObTableServiceClient::alloc_client()
{
  ObTableServiceClient *client = NULL;
  ObTableServiceClientImpl *impl = new(std::nothrow) ObTableServiceClientImpl;
  if (NULL != impl) {
    client = new(std::nothrow) ObTableServiceClient(*impl);
    if (NULL == client) {
      delete impl;
      impl = NULL;
    }
  }
  return client;
}

void ObTableServiceClient::free_client(ObTableServiceClient *client)
{
  if (NULL != client) {
    delete &client->impl_;
    delete client;
    client = NULL;
  }
}

ObTableServiceClient::ObTableServiceClient(ObTableServiceClientImpl &impl)
    :impl_(impl)
{}

ObTableServiceClient::~ObTableServiceClient()
{}

int ObTableServiceClient::init(const ObString &host,
                               int32_t mysql_port,
                               int32_t rpc_port,
                               const ObString &tenant,
                               const ObString &user,
                               const ObString &password,
                               const ObString &database,
                               const ObString &sys_root_password)
{
  return impl_.init(host, mysql_port, rpc_port, tenant, user, password, database, sys_root_password, this);
}

void ObTableServiceClient::destroy()
{
  impl_.destroy();
}
void ObTableServiceClient::set_options(const ObTableServiceClientOptions &options)
{
  impl_.set_options(options);
}

int ObTableServiceClient::alloc_table(const ObString &table_name, ObTable *&table)
{
  return impl_.alloc_table(table_name, table);
}

void ObTableServiceClient::free_table(ObTable *table)
{
  return impl_.free_table(table);
}

int ObTableServiceClient::create_hkv_table(const ObString &table_name, int64_t partition_num, bool if_not_exists)
{
  return impl_.create_hkv_table(table_name, partition_num, if_not_exists);
}

int ObTableServiceClient::drop_hkv_table(const ObString &table_name, bool if_exists)
{
  return impl_.drop_hkv_table(table_name, if_exists);
}

int ObTableServiceClient::alloc_hkv_table(const ObString &table_name, ObHKVTable *&table)
{
  return impl_.alloc_hkv_table(table_name, table);
}

void ObTableServiceClient::free_hkv_table(ObHKVTable *table)
{
  impl_.free_hkv_table(table);
}

int ObTableServiceClient::get_tablet_location(const ObString &table_name, ObRowkey &rowkey,
                                              ObTabletLocation &tablet_location,
                                              uint64_t &table_id,
                                              ObTabletID &tablet_id)
{
  return impl_.get_tablet_location(table_name, rowkey, tablet_location, table_id, tablet_id);
}

int ObTableServiceClient::get_tablets_locations(const ObString &table_name, const common::ObIArray<ObRowkey> &rowkeys,
                                                common::ObIArray<ObTabletLocation> &tablets_locations,
                                                uint64_t &table_id,
                                                common::ObIArray<ObTabletID> &tablet_ids,
                                                common::ObIArray<sql::RowkeyArray> &rowkeys_per_tablet)
{
  return impl_.get_tablets_locations(table_name, rowkeys, tablets_locations, table_id, tablet_ids, rowkeys_per_tablet);
}

int ObTableServiceClient::get_tablet_location(const ObString &table_name, const ObString &index_name,
                                              const common::ObNewRange &index_prefix, ObTabletLocation &tablet_location,
                                              uint64_t &table_id, ObTabletID &tablet_id)
{
  return impl_.get_tablet_location(table_name, index_name, index_prefix, tablet_location, table_id, tablet_id);
}

int ObTableServiceClient::get_rowkey_columns(const ObString &table_name, common::ObStrings &rowkey_columns)
{
  return impl_.get_rowkey_columns(table_name, rowkey_columns);
}

int ObTableServiceClient::get_table_id(const ObString &table_name, uint64_t &table_id)
{
  return impl_.get_table_id(table_name, table_id);
}

oceanbase::common::ObMySQLProxy &ObTableServiceClient::get_user_sql_client()
{
  return impl_.get_user_sql_client();
}

oceanbase::obrpc::ObTableRpcProxy &ObTableServiceClient::get_table_rpc_proxy()
{
  return impl_.get_table_rpc_proxy();
}

uint64_t ObTableServiceClient::get_tenant_id() const
{
  return impl_.get_tenant_id();
}

uint64_t ObTableServiceClient::get_database_id() const
{
  return impl_.get_database_id();
}

const ObString &ObTableServiceClient::get_credential() const
{
  return impl_.get_credential();
}

int ObTableServiceClient::alloc_table_v1(const ObString &table_name, ObTable *&table)
{
  return impl_.alloc_table_v1(table_name, table);
}

int ObTableServiceClient::alloc_table_v2(const ObString &table_name, ObTable *&table)
{
  return impl_.alloc_table_v2(table_name, table);
}

int ObTableServiceClient::alloc_hkv_table_v1(const ObString &table_name, ObHKVTable *&table)
{
  return impl_.alloc_hkv_table_v1(table_name, table);
}

int ObTableServiceClient::alloc_hkv_table_v2(const ObString &table_name, ObHKVTable *&table)
{
  return impl_.alloc_hkv_table_v2(table_name, table);
}

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

#include <iostream>
#include <dirent.h>
#include <getopt.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <fstream>
#include <iterator>
#define private public
#define protected public

#include "lib/utility/ob_test_util.h"
#include "sql/parser/ob_parser.h"
#include "sql/resolver/ob_resolver.h"
#include "lib/allocator/page_arena.h"
#include "lib/json/ob_json_print_utils.h"  // for SJ
#include "sql/ob_sql_init.h"
#include "sql/resolver/ob_schema_checker.h"
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_create_index_stmt.h"
#include "sql/resolver/ddl/ob_create_database_stmt.h"
#include "sql/resolver/ddl/ob_use_database_stmt.h"
#include "sql/resolver/ddl/ob_alter_table_stmt.h"
#include "sql/resolver/ddl/ob_create_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_drop_tablegroup_stmt.h"
#include "sql/resolver/ddl/ob_alter_tablegroup_stmt.h"
#include "share/ob_rpc_struct.h"
#include "common/ob_idc.h"
#include "common/ob_zone_type.h"
#include "sql/resolver/dml/ob_select_stmt.h"
#include "sql/resolver/dml/ob_insert_stmt.h"
#include "sql/resolver/dml/ob_update_stmt.h"
#include "sql/resolver/dml/ob_delete_stmt.h"
#include "sql/resolver/tcl/ob_start_trans_stmt.h"
#include "sql/resolver/tcl/ob_end_trans_stmt.h"
#include "sql/resolver/dcl/ob_create_user_stmt.h"
#include "sql/resolver/dcl/ob_revoke_stmt.h"
#include "sql/resolver/dcl/ob_grant_stmt.h"
#include "sql/resolver/dcl/ob_drop_user_stmt.h"
#include "sql/resolver/dcl/ob_rename_user_stmt.h"
#include "sql/resolver/dcl/ob_set_password_stmt.h"
#include "sql/session/ob_sql_session_info.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "rootserver/ob_schema2ddl_sql.h"

#include "db_initializer.h"
#include "ob_schema_test_utils.cpp"
#include "share/ob_alive_server_tracer.h"
#include "share/schema/ob_schema_service_sql_impl.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_location_cache.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "observer/ob_restore_ctx.h"
#include "observer/ob_inner_sql_connection.h"
#include "rootserver/ob_ddl_operator.h"
#include "rootserver/ob_ddl_service.h"
#include "rootserver/ob_zone_manager.h"
#include "rootserver/ob_server_manager.h"
#include "rootserver/ob_unit_manager.h"
#include "rootserver/ob_index_builder.h"
#include "rootserver/ob_root_balancer.h"
#include "rootserver/ob_leader_coordinator.h"
#include "rootserver/restore/ob_restore_info.h"
#include "rootserver/mock_freeze_info_manager.h"
#include "rootserver/ob_snapshot_info_manager.h"
#include "../partition_table/fake_part_property_getter.h"
#include "../mock_ob_rs_mgr.h"
#include "../../rootserver/server_status_builder.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "rpc/mock_ob_common_rpc_proxy.h"

using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

using namespace oceanbase::common;
using namespace oceanbase::observer;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::obrpc;
using oceanbase::rootserver::ObSchema2DDLSql;
using namespace oceanbase::rootserver;
using namespace oceanbase::share::host;

const int32_t FILE_PATH_LEN = 512;
const int32_t MAX_FILE_NUM = 20;
const char* schema_file_path = "./test_ddl.schema";

struct CmdLineParam {
  char file_names[MAX_FILE_NUM][FILE_PATH_LEN];
  int32_t file_count;
  bool test_input_from_cmd;
  bool print_schema_detail_info;
  std::vector<const char*> file_names_vector;
} clp;

const char* SQL_DIR = "sql";
const char* RESULT_DIR = "result";

bool comparisonFunc(const char* c1, const char* c2)
{
  return strcmp(c1, c2) < 0;
}

enum ParserResultFormat { TREE_FORMAT, JSON_FORMAT };

static uint64_t& TEN = FakePartPropertyGetter::TEN();
static uint64_t& TID = FakePartPropertyGetter::TID();
static int64_t& PID = FakePartPropertyGetter::PID();

class ObFakeCB : public ObIStatusChangeCallback {
public:
  ObFakeCB()
  {}
  int wakeup_balancer()
  {
    return OB_SUCCESS;
  }
  int wakeup_daily_merger()
  {
    return OB_SUCCESS;
  }
  int on_start_server(const oceanbase::common::ObAddr& server)
  {
    UNUSED(server);
    return OB_SUCCESS;
  }
  int on_stop_server(const oceanbase::common::ObAddr& server)
  {
    UNUSED(server);
    return OB_SUCCESS;
  }
  int on_server_status_change(const oceanbase::common::ObAddr& server)
  {
    UNUSED(server);
    return OB_SUCCESS;
  }
};
class MockLocalityManager : public ObILocalityManager {
public:
  struct ServerInfo {
    ServerInfo() : server_(), is_local_(false)
    {}
    ObAddr server_;
    bool is_local_;
    TO_STRING_KV(K_(server), K_(is_local));
  };
  MockLocalityManager() : is_readonly_(false), server_info_()
  {}
  virtual ~MockLocalityManager()
  {}
  virtual int is_local_zone_read_only(bool& is_readonly)
  {
    is_readonly = is_readonly_;
    return OB_SUCCESS;
  }
  virtual int is_local_server(const ObAddr& server, bool& is_local)
  {
    int ret = OB_SUCCESS;
    is_local = false;
    for (int64_t i = 0; i < server_info_.count(); i++) {
      if (server == server_info_.at(i).server_) {
        is_local = server_info_.at(i).is_local_;
        break;
      }
    }
    return ret;
  }
  bool is_readonly_;
  ObArray<ServerInfo> server_info_;
};

class ObFakeServerChangeCB : public ObIServerChangeCallback {
public:
  ObFakeServerChangeCB()
  {}
  virtual ~ObFakeServerChangeCB()
  {}
  virtual int on_server_change()
  {
    return OB_SUCCESS;
  }
};

#define CREATE_TABLE_SCHEMA(ret, schema, replica_num, func)                       \
  {                                                                               \
    ASSERT_EQ(OB_SUCCESS, (*func)(schema));                                       \
    share::schema::ObSchemaTestUtils::table_set_tenant(schema, OB_SYS_TENANT_ID); \
    CREATE_USER_TABLE_SCHEMA(ret, schema);                                        \
  }

class TestDDL : public ::testing::Test {
public:
  TestDDL();
  virtual ~TestDDL();
  virtual void SetUp();
  virtual void TearDown();

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestDDL);

protected:
  // function members
  void fill_tenant_schema(const uint64_t tenant_id, const char* tenant_name, ObTenantSchema& tenant_schema);
  int create_tenant(ObMySQLTransaction& trans, const uint64_t tenant_id);
  void do_resolve(const char* query, ObStmt*& stmt, bool is_print, ParserResultFormat format);
  int create_system_table();
  void do_create_table(ObStmt*& stmt);
  void do_alter_table(ObStmt*& stmt);
  void do_create_index(ObStmt*& stmt);
  void do_create_database(ObStmt*& stmt);
  void do_create_user(ObStmt*& stmt);
  void do_create_tablegroup(ObStmt*& stmt);
  void do_drop_tablegroup(ObStmt*& stmt);
  void do_alter_tablegroup(ObStmt*& stmt);
  void do_use_database(ObStmt*& stmt);
  void do_load_sql(const char* query_str, ObStmt*& stmt, bool is_print, ParserResultFormat format);
  void load_schema_from_file(const char* file_path);
  void generate_index_schema(ObCreateIndexStmt& stmt);
  void generate_index_column_schema(ObSchemaGetterGuard& schema_guard, ObCreateIndexStmt& stmt,
      const ObTableSchema& data_scheam, ObTableSchema& index_schema);

  void do_equal_test();
  void input_test_from_cmd();
  uint64_t get_next_table_id(const uint64_t user_tenant_id);
  bool is_show_sql(const ParseNode& node) const;
  bool is_tenant_space_tables(const uint64_t table_id) const;
  void update_sys_tables(ObStmt* stmt);

  // insert sys table partition info in partition table
  int init_partition_table(const uint64_t tenant_id);

  void do_print_all_table_schema(const uint64_t tenant_id, const uint64_t database_id, std::ofstream& of_tmp);

  void do_print_single_table_schema(const ObTableSchema& table_schema, std::ofstream& of_tmp);

  void do_print_single_table_schema(const uint64_t tenant_id, const uint64_t database_id, const ObString& table_name,
      const bool is_index, std::ofstream& of_tmp);

  void do_print_single_database_schema(const uint64_t tenant_id, const ObString& database_name, std::ofstream& of_tmp);

  void do_print_single_index_schema(
      const ObTableSchema& data_table_schema, const ObString& index_name, std::ofstream& of_tmp);
  int get_index_table_schema(ObSchemaGetterGuard& guard, const uint64_t data_table_id, const ObString& index_name,
      const ObTableSchema* index_table_schema) const;
  void do_print_tenant_schema(const uint64_t tenant_id, std::ofstream& of_tenant);
  void do_print_schema(ObStmt* stmt, std::ofstream& of_tmp);

protected:
  // table id
  // uint64_t next_user_table_id_;
  hash::ObHashMap<uint64_t, uint64_t> next_user_table_id_map_;
  // user_id
  uint64_t sys_user_id_;
  uint64_t next_user_id_;
  // database_id
  uint64_t sys_database_id_;
  uint64_t next_user_database_id_;
  // tenant_id
  uint64_t sys_tenant_id_;
  uint64_t next_user_tenant_id_;
  //
  uint64_t next_user_tablegroup_id_;
  //
  ObArenaAllocator allocator_;
  ObRawExprFactory expr_factory_;
  ObStmtFactory stmt_factory_;
  ObSQLSessionInfo sys_session_info_;
  ObSQLSessionInfo user_session_info_;

  DBInitializer db_initer_;
  ObSchemaServiceSQLImpl schema_service_;

  ObMultiVersionSchemaService multi_schema_service_;
  MockObSrvRpcProxy rpc_proxy_;
  MockObCommonRpcProxy rs_rpc_proxy_;
  ObDDLOperator ddl_operator_;
  ObLeaderCoordinator leader_coordinator_;
  ObZoneManager zone_mgr_;
  ObDDLService ddl_service_;
  FakePartPropertyGetter prop_getter_;
  ObPartitionTableOperator pt_;
  ObLocationFetcher fetcher_;
  ObPartitionLocationCache loc_cache_;
  MockObRsMgr rs_mgr_;
  ObServerManager server_mgr_;
  ObUnitManager unit_mgr_;
  ObRootBalancer balancer_;
  ObAliveServerMap alive_server_;
  MockFreezeInfoManager freeze_info_manager_;
  ObTablegroupSchema tablegroup_schema_;
  MockLocalityManager locality_manager_;
  ObSnapshotInfoManager snapshot_manager_;

  static const int64_t MAX_TENANT_NAME = 64;
};

TestDDL::TestDDL()
    :  // next_user_table_id_(OB_MIN_USER_TABLE_ID),
      next_user_table_id_map_(),
      sys_user_id_(OB_SYS_USER_ID),
      next_user_id_(OB_USER_ID),
      sys_database_id_(OB_SYS_DATABASE_ID),
      next_user_database_id_(OB_USER_DATABASE_ID),
      sys_tenant_id_(OB_SYS_TENANT_ID),
      next_user_tenant_id_(OB_USER_TENANT_ID),
      next_user_tablegroup_id_(OB_USER_TABLEGROUP_ID),
      allocator_(ObModIds::TEST),
      expr_factory_(allocator_),
      stmt_factory_(allocator_),
      db_initer_(),
      multi_schema_service_(),
      rpc_proxy_(),
      ddl_operator_(multi_schema_service_, db_initer_.get_sql_proxy()),
      leader_coordinator_(),
      ddl_service_(),
      prop_getter_(),
      pt_(prop_getter_),
      fetcher_(),
      loc_cache_(fetcher_),
      unit_mgr_(server_mgr_, zone_mgr_),
      tablegroup_schema_()
{}

TestDDL::~TestDDL()
{}

void TestDDL::load_schema_from_file(const char* file_path)
{
  if (file_path != NULL && strncmp(file_path, "", 1) != 0) {
    std::ifstream if_schema(file_path);
    ASSERT_TRUE(if_schema.is_open());
    std::string line;
    while (std::getline(if_schema, line)) {
      ObStmt* stmt = NULL;
      if (line.size() <= 0)
        continue;
      if (line.at(0) == '#')
        continue;
      if (line.at(0) == '\r' || line.at(0) == '\n')
        continue;
      do_load_sql(line.c_str(), stmt, clp.print_schema_detail_info, JSON_FORMAT);
    }
  }
}

bool TestDDL::is_tenant_space_tables(const uint64_t table_id) const
{
  for (int64_t i = 0; i < ARRAYSIZEOF(tenant_space_tables); ++i) {
    if (tenant_space_tables[i] == table_id) {
      return true;
    }
  }
  return false;
}

int TestDDL::create_system_table()
{
  int ret = OB_SUCCESS;
  // ObString sys_database_name(OB_SYS_DATABASE_NAME);
  // session_info_.set_database_name(sys_database_name);
  // array,each type is a pointer to function pointer
  typedef int (*schema_init_func)(ObTableSchema & table_schema);
  const schema_init_func* creator_ptr_array[] = {
      core_table_schema_creators, sys_table_schema_creators, virtual_table_schema_creators, NULL};
  ObArray<ObTableSchema> schema_array;
  for (const schema_init_func** creator_ptr_ptr = creator_ptr_array; OB_SUCCESS == ret && NULL != *creator_ptr_ptr;
       ++creator_ptr_ptr) {
    for (const schema_init_func* creator_ptr = *creator_ptr_ptr; OB_SUCCESS == ret && NULL != *creator_ptr;
         ++creator_ptr) {
      ObTableSchema table_schema;
      if (OB_SUCCESS != (ret = (*creator_ptr)(table_schema))) {
        _OB_LOG(WARN, "create table schema fialed, ret %d", ret);
        ret = OB_SCHEMA_ERROR;
      } else {
        table_schema.set_database_id(combine_id(sys_tenant_id_, table_schema.get_database_id()));
        table_schema.set_table_id(combine_id(sys_tenant_id_, table_schema.get_table_id()));
        schema_array.push_back(table_schema);
      }
      _OB_LOG(INFO,
          "do_create_table table_name=[%s], table_id=[%lu], tenant_id=[%lu], database_id=[%lu]",
          table_schema.get_table_name(),
          table_schema.get_table_id(),
          table_schema.get_tenant_id(),
          table_schema.get_database_id());
    }
  }
  // schema_mgr_->add_new_table_schema_array(schema_array);
  // ObString empty_database_name("");
  // session_info_.set_database_name(empty_database_name);
  return ret;
}

int TestDDL::init_partition_table(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  // insert user table partition info
  TEN = tenant_id;
  const int64_t part_num = 1;
  const int64_t sys_table_cnt = sizeof(tenant_space_tables) / sizeof(uint64_t);
  for (int64_t i = 0; i < sys_table_cnt; ++i) {
    TID = combine_id(TEN, tenant_space_tables[i]);
    for (int64_t j = 0; j < part_num; ++j) {
      PID = j;
      prop_getter_.clear().add(A, LEADER).add(B, FOLLOWER).add(C, FOLLOWER);
      for (int64_t k = 0; k < prop_getter_.get_replicas().count(); ++k) {
        pt_.update(prop_getter_.get_replicas().at(k));
      }
    }
  }

  return ret;
}

void TestDDL::fill_tenant_schema(const uint64_t tenant_id, const char* tenant_name, ObTenantSchema& tenant_schema)
{
  tenant_schema.set_tenant_id(tenant_id);
  tenant_schema.set_tenant_name(tenant_name);
  tenant_schema.set_comment("this is a test tenant");
  tenant_schema.add_zone("test");
  tenant_schema.set_primary_zone("test");
}

int TestDDL::create_tenant(ObMySQLTransaction& trans, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  char tenant_name[MAX_TENANT_NAME];
  if (snprintf(tenant_name, MAX_TENANT_NAME, "t%lu", tenant_id) >= MAX_TENANT_NAME) {
    ret = OB_BUF_NOT_ENOUGH;
    _OB_LOG(WARN, "buf not enough[ret=%d]", ret);
  } else {
    ObTenantSchema tenant_schema;
    fill_tenant_schema(tenant_id, tenant_name, tenant_schema);
    if (OB_FAIL(schema_service_.get_tenant_sql_service().insert_tenant(tenant_schema, trans, NULL))) {
      _OB_LOG(WARN, "insert tenant failed[ret=%d]", ret);
    }
  }
  return ret;
}

void TestDDL::SetUp()
{
  ObString sys_tenant(OB_SYS_TENANT_NAME);
  ObString user_tenant("hualong");
  int ret = OB_SUCCESS;
  ret = sys_session_info_.init_tenant(sys_tenant, sys_tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = sys_session_info_.set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = user_session_info_.init_tenant(user_tenant, next_user_tenant_id_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = user_session_info_.set_user(OB_SYS_USER_NAME, OB_SYS_HOST_NAME, OB_SYS_USER_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, ObPreProcessSysVars::init_sys_var());

  ObArenaAllocator* allocator = NULL;
  uint32_t version = 0;
  if (OB_FAIL(sys_session_info_.init(version, 0, 0, allocator))) {
    OB_LOG(ERROR, "init sys session_info error!", K(ret));
  } else if (OB_FAIL(user_session_info_.init(version, 0, 0, allocator))) {
    OB_LOG(ERROR, "init user session info error!", K(ret));
  }
  ASSERT_EQ(OB_SUCCESS, ret);

  ObObj obj, min_val, max_val;
  ObObj type;
  obj.set_varchar("1");  // OB_LOWERCASE_AND_INSENSITIVE
  // obj.set_int(ObIntType, OB_LOWERCASE_AND_INSENSITIVE);
  min_val.set_varchar("0");
  max_val.set_varchar("2");
  type.set_type(ObIntType);
  ret = user_session_info_.load_sys_variable(
      ObString::make_string(OB_SV_LOWER_CASE_TABLE_NAMES), type, obj, min_val, max_val, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  //  obj.set_varchar();
  //  ObString collation = ObString::make_string(ObCharset::collation_name(collation_type));
  obj.set_varchar("45");
  ret = user_session_info_.load_sys_variable(
      ObString::make_string(OB_SV_COLLATION_CONNECTION), type, obj, min_val, max_val, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  type.set_type(ObIntType);
  obj.set_varchar("45");
  // obj.set_varchar(ObString::make_string(ObCharset::charset_name(CHARSET_UTF8MB4)));
  ret = user_session_info_.load_sys_variable(
      ObString::make_string(OB_SV_CHARACTER_SET_DATABASE), type, obj, min_val, max_val, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObCollationType collation_type = ObCharset::get_default_collation(CHARSET_UTF8MB4);
  ObString collation = ObString::make_string(ObCharset::collation_name(collation_type));
  type.set_type(ObIntType);
  // obj.set_varchar(collation);
  obj.set_varchar("45");
  ret = user_session_info_.load_sys_variable(
      ObString::make_string(OB_SV_COLLATION_DATABASE), type, obj, min_val, max_val, 0);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = next_user_table_id_map_.create(16, ObModIds::OB_HASH_BUCKET_ALTER_TABLE_MAP);
  ASSERT_EQ(OB_SUCCESS, ret);
  sys_session_info_.load_default_sys_variable(true, true);
  ret = create_system_table();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = multi_schema_service_.init(&db_initer_.get_sql_proxy(),
      &db_initer_.get_config(),
      OB_MAX_VERSION_COUNT,
      OB_MAX_VERSION_COUNT_FOR_MERGE,
      false /*with timestamp*/);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObAddr addr;
  addr.set_ip_addr("127.0.0.1", 8051);
  ret = balancer_.init(db_initer_.get_config(),
      multi_schema_service_,
      ddl_service_,
      unit_mgr_,
      server_mgr_,
      pt_,
      *((ObLeaderCoordinator*)NULL),
      *((ObZoneManager*)NULL),
      *((ObEmptyServerChecker*)NULL),
      *((ObRebalanceTaskMgr*)NULL),
      *((ObSrvRpcProxy*)NULL),
      *((ObRestoreCtx*)NULL),
      addr);
  ASSERT_EQ(OB_SUCCESS, ret);
  balancer_.start();

  ASSERT_EQ(OB_SUCCESS,
      ddl_service_.init(rpc_proxy_,
          rs_rpc_proxy_,
          db_initer_.get_sql_proxy(),
          multi_schema_service_,
          pt_,
          server_mgr_,
          zone_mgr_,
          unit_mgr_,
          balancer_,
          freeze_info_manager_,
          snapshot_manager_));

  ASSERT_TRUE(NULL != &db_initer_.get_sql_proxy());
  ObSchemaService* schema_service = multi_schema_service_.get_schema_service();
  ASSERT_TRUE(NULL != schema_service);
  ret = db_initer_.fill_sys_stat_table();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_service_.init(&db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(OB_SUCCESS, pt_.init(db_initer_.get_sql_proxy(), NULL));
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = OB_MALLOC_BIG_BLOCK_SIZE;
  ObKVGlobalCache::get_instance().init(bucket_num, max_cache_size, block_size);
  const char* cache_name = "location_cache";
  int64_t priority = 1L;

  ASSERT_EQ(
      OB_SUCCESS, fetcher_.init(db_initer_.get_config(), pt_, rs_mgr_, rs_rpc_proxy_, rpc_proxy_, &locality_manager_));
  ASSERT_EQ(OB_SUCCESS, alive_server_.init());
  ASSERT_EQ(OB_SUCCESS,
      loc_cache_.init(multi_schema_service_,
          db_initer_.get_config(),
          alive_server_,
          cache_name,
          priority,
          true,
          &locality_manager_));

  // insert system tenant, make refresh tenant succeed,
  // ob_schema_test_utils.cpp is ugly, it need schema_service_ of type schema_serivce_sql_impl_ be
  // defined
  ObSchemaService& schema_service_ = *schema_service;
  ObTableSchema table_schema;
  for (int64_t i = 0; OB_SUCC(ret) && NULL != sys_table_schema_creators[i]; ++i) {
    table_schema.reset();
    table_schema.set_expire_info(ObString::make_string("a > b"));
    ASSERT_EQ(OB_SUCCESS, (*sys_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != core_table_schema_creators[i]; ++i) {
    table_schema.reset();
    table_schema.set_expire_info(ObString::make_string("a > b"));
    ASSERT_EQ(OB_SUCCESS, (*core_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != virtual_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*virtual_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != information_schema_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*information_schema_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != mysql_table_schema_creators[i]; ++i) {
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*mysql_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  ObMySQLTransaction trans;
  ret = trans.start(&db_initer_.get_sql_proxy());
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = create_tenant(trans, OB_USER_TENANT_ID);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create user
  ObUserInfo user;
  FILL_USER_INFO(user, OB_USER_TENANT_ID, 0, "user1", "", "user1", false, 0);
  ret = schema_service_.get_user_sql_service().create_user(user, NULL, trans);
  ASSERT_EQ(OB_SUCCESS, ret);

  // grant database priviledge
  ObOriginalDBKey db_key(OB_USER_TENANT_ID, 0, ObString::make_string("test"));
  ret = schema_service_.get_priv_sql_service().grant_database(db_key, OB_PRIV_ALL, NULL, trans);
  ASSERT_EQ(OB_SUCCESS, ret);
  db_key.db_ = ObString::make_string("hualong");
  ret = schema_service_.get_priv_sql_service().grant_database(db_key, OB_PRIV_ALL, NULL, trans);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = trans.end(true);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = multi_schema_service_.refresh_and_add_schema();
  ASSERT_EQ(OB_SUCCESS, ret);
  // create sys_tenant default tablegroup and database
  ObTablegroupSchema sys_tg_schema;
  ObDatabaseSchema sys_db_schema;
  FILL_TABLEGROUP_SCHEMA(sys_tg_schema, OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID, OB_SYS_TABLEGROUP_NAME, "default tg");
  FILL_DATABASE_SCHEMA(sys_db_schema, OB_SYS_TENANT_ID, OB_SYS_DATABASE_ID, OB_SYS_DATABASE_NAME, "default db");
  CREATE_TABLEGROUP_SCHEMA(ret, sys_tg_schema);
  CREATE_DATABASE_SCHEMA(ret, sys_db_schema);
  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());

  ret = zone_mgr_.init(db_initer_.get_sql_proxy(), leader_coordinator_);
  ASSERT_EQ(OB_SUCCESS, ret);
  zone_mgr_.loaded_ = true;
  // init unit_mgr
  ASSERT_EQ(OB_SUCCESS,
      unit_mgr_.init(db_initer_.get_sql_proxy(), db_initer_.get_config(), leader_coordinator_, multi_schema_service_));
  ASSERT_EQ(OB_SUCCESS, unit_mgr_.load());

  // init server manager
  ObFakeCB cb;
  ObFakeServerChangeCB cb2;
  ASSERT_EQ(OB_SUCCESS,
      server_mgr_.init(cb,
          cb2,
          db_initer_.get_sql_proxy(),
          unit_mgr_,
          zone_mgr_,
          leader_coordinator_,
          db_initer_.get_config(),
          A,
          rpc_proxy_));
  const int64_t now = ObTimeUtility::current_time();
  const ObZone zone = "test";
  const ObRegion region = DEFAULT_REGION_NAME;
  const ObIDC idc = "";
  const ObZoneType zone_type = ObZoneType::ZONE_TYPE_READWRITE;
  ret = zone_mgr_.add_zone(zone, region, idc, zone_type);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = zone_mgr_.start_zone(zone);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObServerStatusBuilder server_builder;
  ASSERT_EQ(OB_SUCCESS, server_builder.init(db_initer_.get_config()));
  server_builder.add(ObServerStatus::OB_SERVER_ACTIVE, now, A, zone)
      .add(ObServerStatus::OB_SERVER_ACTIVE, now, B, zone)
      .add(ObServerStatus::OB_SERVER_ACTIVE, now, C, zone);
  ASSERT_EQ(OB_SUCCESS, server_builder.build(server_mgr_));

  // mock create partition rpc call
  ON_CALL(rpc_proxy_, switch_schema(_, _)).WillByDefault(Return(OB_SUCCESS));

  ASSERT_EQ(OB_SUCCESS, db_initer_.create_tenant_space(OB_USER_TENANT_ID));
  // insert tablegroup first
  tablegroup_schema_.set_tenant_id(OB_USER_TENANT_ID);
  tablegroup_schema_.set_tablegroup_id(combine_id(OB_USER_TENANT_ID, OB_USER_TABLEGROUP_ID));
  tablegroup_schema_.set_tablegroup_name("haijingtg");
  tablegroup_schema_.set_comment("haijingtg is a test tablegroup");
  const bool if_not_exist = false;
  ret = ddl_service_.create_tablegroup(if_not_exist, tablegroup_schema_, NULL);
  ASSERT_EQ(OB_SUCCESS, ret);

  // create unit_config
  ObUnitConfig config;
  config.max_cpu_ = 1;
  config.min_cpu_ = 1;
  config.max_iops_ = 128;
  config.min_iops_ = 128;
  config.max_memory_ = 1073741824;
  config.min_memory_ = 1073741824;
  config.max_disk_size_ = 536870912;
  config.max_session_num_ = 64;
  config.name_ = "config";
  config.unit_config_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, unit_mgr_.create_unit_config(config, if_not_exist));

  // create resource pool
  oceanbase::share::ObResourcePool pool;
  pool.name_ = "test_pool";
  pool.unit_config_id_ = 1;
  pool.tenant_id_ = OB_USER_TENANT_ID;
  pool.resource_pool_id_ = 1;
  pool.unit_count_ = 1;
  ASSERT_EQ(OB_SUCCESS, pool.zone_list_.push_back("test"));
  ASSERT_EQ(OB_SUCCESS, unit_mgr_.create_resource_pool(pool, "config", if_not_exist));

  ObArray<ObUnit> units;
  ObUnit unit;
  unit.server_ = A;
  unit.unit_id_ = 1;
  unit.zone_ = "test";
  unit.resource_pool_id_ = 1;
  unit.group_id_ = 1;
  ASSERT_EQ(OB_SUCCESS, units.push_back(unit));
  ASSERT_EQ(OB_SUCCESS, unit_mgr_.create_sys_units(units));
  ASSERT_EQ(OB_SUCCESS, unit_mgr_.load());

  //  schema_mgr_ = multi_schema_service_.get_schema_manager_by_version(0);

  // create schema
  load_schema_from_file(schema_file_path);
  // prevent interference between test schema and the following DDL test cases
  next_user_table_id_map_.set_refactored(sys_tenant_id_, OB_MIN_USER_TABLE_ID + 100, 1 /*replace*/);

  //  next_table_id_ = 200;
}

void TestDDL::TearDown()
{
  // destroy
  ASSERT_EQ(OB_SUCCESS, loc_cache_.destroy());
  ObKVGlobalCache::get_instance().destroy();
}

void load_sql_file(const char* file_name)
{
  if (file_name != NULL) {
    if (strcmp(".", file_name) != 0 && strcmp("..", file_name) != 0) {
      snprintf(clp.file_names[clp.file_count++],
          strlen(file_name) - 3,  // strlen("test")-1
          "%s",
          file_name);
      _OB_LOG(INFO, "add file %s to cmd", clp.file_names[clp.file_count - 1]);
      clp.file_names_vector.push_back(clp.file_names[clp.file_count - 1]);
    }
  }
}

void load_all_sql_files(const char* directory_name)
{
  DIR* dp = NULL;
  if ((dp = opendir(directory_name)) == NULL) {
    _OB_LOG(ERROR, "error open file");
    return;
  }
  struct dirent* dirp = NULL;
  clp.file_count = 0;
  while ((dirp = readdir(dp)) != NULL) {
    load_sql_file(dirp->d_name);
  }
  std::sort(clp.file_names_vector.begin(), clp.file_names_vector.end(), comparisonFunc);
  for (std::vector<const char*>::iterator iter = clp.file_names_vector.begin(); iter != clp.file_names_vector.end();
       ++iter) {
    _OB_LOG(INFO, "sorted %s", *iter);
  }
  closedir(dp);
}

void TestDDL::do_load_sql(const char* query_str, ObStmt*& stmt, bool is_print, enum ParserResultFormat format)
{
  // ObStmt *stmt = NULL;
  _OB_LOG(INFO, "query_str: %s", query_str);

  do_resolve(query_str, stmt, is_print, format);
  ASSERT_FALSE(HasFatalFailure()) << "query_str: " << query_str << std::endl;
  if (stmt != NULL) {
    switch (stmt->get_stmt_type()) {
      case stmt::T_CREATE_TABLE: {
        do_create_table(stmt);
        break;
      }
      case stmt::T_ALTER_TABLE: {
        do_alter_table(stmt);
        break;
      }
      case stmt::T_CREATE_DATABASE: {
        do_create_database(stmt);
        break;
      }
      case stmt::T_CREATE_INDEX: {
        do_create_index(stmt);
        break;
      }
      case stmt::T_USE_DATABASE: {
        do_use_database(stmt);
        break;
      }
      case stmt::T_CREATE_USER: {
        do_create_user(stmt);
        break;
      }
      case stmt::T_CREATE_TABLEGROUP: {
        do_create_tablegroup(stmt);
        break;
      }
      case stmt::T_DROP_TABLEGROUP: {
        do_drop_tablegroup(stmt);
        break;
      }
      case stmt::T_ALTER_TABLEGROUP: {
        do_alter_tablegroup(stmt);
        break;
      }
      // case stmt::T_CREATE_TENANT: {
      //
      //}
      default:
        break;
    }
    //    schema_mgr_ = multi_schema_service_.get_schema_manager_by_version(0);
  }
}

// output tenant schemas to file
void TestDDL::do_print_tenant_schema(const uint64_t tenant_id, std::ofstream& of_tenant)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObDatabaseSchema*> database_schema_array;
  ObSchemaGetterGuard guard;
  ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = guard.get_database_schemas_in_tenant(tenant_id, database_schema_array);
  OB_ASSERT(OB_SUCC(ret));

  ObSArray<const ObTableSchema*> table_schema_array;
  for (int idx_d = 0; idx_d < database_schema_array.count(); ++idx_d) {
    of_tenant << "=============[" << database_schema_array.at(idx_d)->get_database_name()
              << "] database schema start =============\n";

    of_tenant << SJ(*database_schema_array.at(idx_d)) << std::endl;
    table_schema_array.reset();
    ret = guard.get_table_schemas_in_database(
        OB_USER_TENANT_ID, database_schema_array[idx_d]->get_database_id(), table_schema_array);
    for (int idx_t = 0; idx_t < table_schema_array.count(); ++idx_t) {
      of_tenant << "-------------[" << table_schema_array.at(idx_t)->get_table_name()
                << "] table schema start -----------\n";
      of_tenant << SJ(*table_schema_array.at(idx_t)) << std::endl;
      of_tenant << "-------------[" << table_schema_array.at(idx_t)->get_table_name()
                << "] table schema end -----------\n\n";
    }
    of_tenant << "=============[" << database_schema_array.at(idx_d)->get_database_name()
              << "database schema end ------------------\n\n";
  }
  database_schema_array.reset();
}

void TestDDL::do_print_all_table_schema(const uint64_t tenant_id, const uint64_t database_id, std::ofstream& of_tmp)
{
  int ret = OB_SUCCESS;
  ObSArray<const ObTableSchema*> table_schema_array;
  ObSchemaGetterGuard guard;
  ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = guard.get_table_schemas_in_database(tenant_id, database_id, table_schema_array);
  OB_ASSERT(OB_SUCC(ret));
  for (int idx_t = 0; idx_t < table_schema_array.count(); ++idx_t) {
    of_tmp << "-------------[" << table_schema_array.at(idx_t)->get_table_name()
           << "] table schema start -----------\n";
    of_tmp << SJ(*table_schema_array.at(idx_t)) << std::endl;
    of_tmp << "-------------[" << table_schema_array.at(idx_t)->get_table_name()
           << "] table schema end -----------\n\n";
  }
}

void TestDDL::do_print_single_table_schema(const ObTableSchema& table_schema, std::ofstream& of_tmp)
{
  of_tmp << "-------------[" << table_schema.get_table_name() << "] table schema start -----------\n";
  of_tmp << SJ(table_schema) << std::endl;
  of_tmp << "-------------[" << table_schema.get_table_name() << "] table schema end -----------\n\n";
}

void TestDDL::do_print_single_table_schema(const uint64_t tenant_id, const uint64_t database_id,
    const ObString& table_name, const bool is_index, std::ofstream& of_tmp)
{
  ObSchemaGetterGuard guard;
  int ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObTableSchema* table_schema = NULL;
  ret = guard.get_table_schema(tenant_id, database_id, table_name, is_index, table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_TRUE(NULL != table_schema);
  do_print_single_table_schema(*table_schema, of_tmp);
}

void TestDDL::do_print_single_database_schema(
    const uint64_t tenant_id, const ObString& database_name, std::ofstream& of_tmp)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard guard;
  ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);

  const ObDatabaseSchema* database_schema = NULL;
  ret = guard.get_database_schema(tenant_id, database_name, database_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_ASSERT(database_schema != NULL);

  of_tmp << "=============[" << database_schema->get_database_name() << "] database schema start =============\n";
  of_tmp << SJ(*database_schema) << std::endl;
  of_tmp << "=============[" << database_schema->get_database_name() << "] database schema start =============\n\n";
}

int TestDDL::get_index_table_schema(ObSchemaGetterGuard& guard, const uint64_t data_table_id,
    const ObString& index_name, const ObTableSchema* index_table_schema) const
{
  int ret = OB_SUCCESS;
  char buffer[OB_MAX_TABLE_NAME_LENGTH];
  ObDataBuffer data_buffer(buffer, sizeof(buffer));
  const ObTableSchema* data_table_schema = NULL;
  ObString index_table_name;
  if (OB_FAIL(ObTableSchema::build_index_table_name(data_buffer, data_table_id, index_name, index_table_name))) {
    _OB_LOG(WARN, "build_index_table_name failed");
  } else if (OB_FAIL(guard.get_table_schema(data_table_id, data_table_schema))) {
    _OB_LOG(WARN, "fail to get data table schema");
  } else if (NULL == data_table_schema) {
    ret = OB_ENTRY_NOT_EXIST;
    _OB_LOG(WARN, "data table schema not exist");
  } else if (OB_FAIL(guard.get_table_schema(data_table_schema->get_tenant_id(),
                 data_table_schema->get_database_id(),
                 index_table_name,
                 true,
                 index_table_schema))) {
    _OB_LOG(WARN, "fail to get index table schema");
  } else if (NULL == index_table_schema) {
    ret = OB_ENTRY_NOT_EXIST;
    _OB_LOG(WARN, "index table schema not exist");
  }

  return ret;
}

void TestDDL::do_print_single_index_schema(
    const ObTableSchema& data_table_schema, const ObString& index_name, std::ofstream& of_tmp)
{
  ObSchemaGetterGuard guard;
  int ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  const ObTableSchema* index_schema = NULL;
  ret = get_index_table_schema(guard, data_table_schema.get_table_id(), index_name, index_schema);
  OB_LOG(INFO, "print", K(index_name), K(data_table_schema));
  OB_ASSERT(index_schema != NULL);
  do_print_single_table_schema(*index_schema, of_tmp);
}

// void TestAlterTable::do_print_index_table_schema(const uint64_t tenant_id,
//                                                 const uint64_t database_id,
//                                                 const uint64_t datatable_id)
//{
//  int ret = OB_SUCCESS;
//  ObTableSchema *table_schema = schema_mgr_->get_table_schema(datatable_id);
//  OB_ASSERT(table_schema);
//  uint64_t index_tid_array[OB_MAX_INDEX_PER_TABLE];
//  int64_t index_cnt = OB_MAX_INDEX_PER_TABLE;
//  OB_ASSERT(OB_SUCC(ret));
//  ret = table_schema->get_index_tid_array(index_tid_array, index_cnt);
//  //update all index table schema
//  for (int64_t i = 0; OB_SUCC(ret) && i < index_cnt; ++i) {
//    const ObTableSchema *index_table_schema = NULL;
//    index_table_schema = schema_mgr_->get_table_schema(index_tid_array[i]);
//    OB_ASSERT(NULL != index_table_schema);
//    do_print_single_database_schema(tenant_id,
//                                    database_id,
//                                    index_table_schema->get_table_id());
//  }
//}

// void TestDDL::do_print_all_column_schema(const uint64_t user_tenant_id,
//                                                const ObString &user_table_name,
//                                                std::ofstream &of_tmp)
//{
//  int ret = OB_SUCCESS;
//  ObString sys_table_name("__all_column");
//  const ObTableSchema *sys_table_schema = schema_mgr_->get_table_schema(OB_SYS_TENANT_ID,
//                                                                    OB_SYS_DATABASE_ID,
//                                                                    sys_table_name);
//  OB_ASSERT(OB_SUCC(ret));
//  ObTableSchema::const_column_iterator it_begin =  sys_table_schema->column_begin();
//  ObTableSchema::const_column_iterator it_end =  sys_table_schema->column_end();
//  of_tmp << "[" << table_schema->get_table_name() << "] add columns in __all_column *********";
//  for (;OB_SUCCESS == ret && it_begin != it_end; ++it_begin) {
//    uint64_t tenant_id = (*it_begin)->get_tenant_id();
//    uint64_t table_id = (*it_begin)->get_table_id();
//    if (tenant_id == user_tenant_id && table_id == )
//    of_tmp << SJ(*it_begin) << std::endl;
//  }
//}

void TestDDL::do_print_schema(ObStmt* stmt, std::ofstream& of_tmp)
{
  switch (stmt->get_stmt_type()) {
    case stmt::T_CREATE_TABLE: {
      ObCreateTableStmt* create_table_stmt = static_cast<ObCreateTableStmt*>(stmt);
      do_print_single_table_schema(OB_USER_TENANT_ID,
          create_table_stmt->get_create_table_arg().schema_.get_database_id(),
          create_table_stmt->get_create_table_arg().schema_.get_table_name(),
          false,
          of_tmp);
      ObSArray<ObCreateIndexArg>& index_args = create_table_stmt->get_create_table_arg().index_arg_list_;
      for (int i = 0; i < index_args.count(); ++i) {
        do_print_single_index_schema(
            create_table_stmt->get_create_table_arg().schema_, index_args.at(i).index_name_, of_tmp);
      }
      break;
    }
    case stmt::T_ALTER_TABLE: {
      ObAlterTableStmt* alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt);
      const ObAlterTableArg& arg = alter_table_stmt->get_alter_table_arg();

      const AlterTableSchema& alter_table_schema = arg.alter_table_schema_;
      //      const ObSArray<ObIndexArg *> &index_args = arg.index_arg_list_;
      bool is_index = false;
      //      const ObTableSchema *data_table_schema = schema_mgr_->get_table_schema(alter_table_schema.get_tenant_id(),
      //                                                                       alter_table_schema.get_origin_database_name(),
      //                                                                       alter_table_schema.get_origin_table_name(),
      //                                                                       is_index);
      ObSchemaGetterGuard guard;
      int ret = multi_schema_service_.get_schema_guard(guard);
      ASSERT_EQ(OB_SUCCESS, ret);
      const ObTableSchema* data_table_schema = NULL;
      if (arg.alter_table_schema_.alter_option_bitset_.has_member(ObAlterTableArg::TABLE_NAME)) {
        ret = guard.get_table_schema(alter_table_schema.get_tenant_id(),
            alter_table_schema.get_database_name(),
            alter_table_schema.get_table_name(),
            is_index,
            data_table_schema);
      } else {
        ret = guard.get_table_schema(alter_table_schema.get_tenant_id(),
            alter_table_schema.get_origin_database_name(),
            alter_table_schema.get_origin_table_name(),
            is_index,
            data_table_schema);
      }
      ASSERT_EQ(OB_SUCCESS, ret);
      OB_ASSERT(data_table_schema);
      do_print_single_table_schema(*data_table_schema, of_tmp);
      //      for (int i = 0; i < index_args.count(); ++i) {
      //        do_print_single_index_schema(*data_table_schema,
      //                                     index_args.at(i)->index_name_,
      //                                     of_tmp);
      //      }
      break;
    }
    case stmt::T_CREATE_DATABASE: {
      ObCreateDatabaseStmt* create_database_stmt = static_cast<ObCreateDatabaseStmt*>(stmt);
      do_print_single_database_schema(OB_USER_TENANT_ID,
          create_database_stmt->get_create_database_arg().database_schema_.get_database_name_str(),
          of_tmp);
      break;
    }
    case stmt::T_CREATE_INDEX: {
      ObCreateIndexStmt* create_index_stmt = static_cast<ObCreateIndexStmt*>(stmt);
      const ObCreateIndexArg& index_arg = create_index_stmt->get_create_index_arg();
      bool is_index = false;
      ObSchemaGetterGuard guard;
      int ret = multi_schema_service_.get_schema_guard(guard);
      ASSERT_EQ(OB_SUCCESS, ret);
      const ObTableSchema* data_table_schema = NULL;
      ret = guard.get_table_schema(
          index_arg.tenant_id_, index_arg.database_name_, index_arg.table_name_, is_index, data_table_schema);
      ASSERT_EQ(OB_SUCCESS, ret);
      OB_ASSERT(data_table_schema);
      do_print_single_index_schema(*data_table_schema, create_index_stmt->get_create_index_arg().index_name_, of_tmp);
      break;
    }
    case stmt::T_USE_DATABASE: {
      break;
    }
    case stmt::T_CREATE_USER: {
      break;
    }
    default:
      break;
  }
}

bool TestDDL::is_show_sql(const ParseNode& node) const
{
  bool ret = false;
  switch (node.type_) {
    case T_SHOW_TABLES:
    case T_SHOW_DATABASES:
    case T_SHOW_VARIABLES:
    case T_SHOW_COLUMNS:
    case T_SHOW_SCHEMA:
    case T_SHOW_CREATE_TABLE:
    case T_SHOW_CREATE_VIEW:
    case T_SHOW_TABLE_STATUS:
    case T_SHOW_PARAMETERS:
    // case T_SHOW_INDEXES:
    case T_SHOW_PROCESSLIST:
    case T_SHOW_SERVER_STATUS:
    case T_SHOW_WARNINGS:
    case T_SHOW_GRANTS: {
      ret = true;
      break;
    }
    default: {
      ret = false;
    }
  }
  return ret;
}

void TestDDL::do_resolve(const char* query_str, ObStmt*& stmt, bool is_print, enum ParserResultFormat format)
{
  ObSQLMode mode = SMO_DEFAULT;
  ObParser parser(allocator_, mode);
  ObString query = ObString::make_string(query_str);
  ParseResult parse_result;
  int ret = OB_SUCCESS;
  ret = (parser.parse(query, parse_result));
  ASSERT_EQ(OB_SUCCESS, ret);
  if (is_print) {
    if (JSON_FORMAT == format) {
      _OB_LOG(INFO, "%s", (const char*)SJ(ObParserResultPrintWrapper(*parse_result.result_tree_)));
    } else {
      _OB_LOG(INFO, "%s", (const char*)SJ(ObParserResultTreePrintWrapper(*parse_result.result_tree_)));
    }
  }
  ObSchemaChecker schema_checker;
  ObSchemaGetterGuard guard;
  ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = schema_checker.init(guard);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObResolverParams resolver_ctx;
  resolver_ctx.allocator_ = &allocator_;
  resolver_ctx.schema_checker_ = &schema_checker;
  resolver_ctx.session_info_ = &user_session_info_;
  resolver_ctx.expr_factory_ = &expr_factory_;
  resolver_ctx.stmt_factory_ = &stmt_factory_;
  resolver_ctx.query_ctx_ = stmt_factory_.get_query_ctx();
  ObResolver resolver(resolver_ctx);
  OK(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT, *parse_result.result_tree_->children_[0], stmt));
  if (is_print) {
    _OB_LOG(INFO, "%s", (const char*)SJ(*stmt));
  }
  parser.free_result(parse_result);
}

void TestDDL::do_create_database(ObStmt*& stmt)
{
  ObCreateDatabaseStmt* create_database_stmt = dynamic_cast<ObCreateDatabaseStmt*>(stmt);
  OB_ASSERT(NULL != create_database_stmt);
  schema::ObDatabaseSchema database_schema = create_database_stmt->get_create_database_arg().database_schema_;
  database_schema.set_tenant_id(OB_USER_TENANT_ID);
  database_schema.set_database_id(combine_id(OB_USER_TENANT_ID, next_user_database_id_++));
  database_schema.add_zone("test");
  database_schema.set_primary_zone("test");
  // database_schema.set_comment("haijingdb is a test db");
  int ret = OB_SUCCESS;
  bool if_not_exist = false;
  ret = ddl_service_.create_database(if_not_exist, database_schema, NULL);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestDDL::do_use_database(ObStmt*& stmt)
{
  ObUseDatabaseStmt* use_database_stmt = dynamic_cast<ObUseDatabaseStmt*>(stmt);
  OB_ASSERT(NULL != use_database_stmt);
  user_session_info_.set_default_database(use_database_stmt->get_db_name());
}

uint64_t TestDDL::get_next_table_id(const uint64_t user_tenant_id)
{
  uint64_t next_table_id = OB_INVALID_ID;
  if (OB_HASH_NOT_EXIST == next_user_table_id_map_.get_refactored(user_tenant_id, next_table_id)) {
    next_table_id = OB_MIN_USER_TABLE_ID + 1;  // 50001
    OB_ASSERT(OB_SUCCESS == next_user_table_id_map_.set_refactored(user_tenant_id, next_table_id));
    _OB_LOG(INFO, "tenant_id = [%lu] not exist, set next_table_id = [%lu]", user_tenant_id, next_table_id);
  } else {
    ++next_table_id;
    OB_ASSERT(OB_SUCCESS == next_user_table_id_map_.set_refactored(user_tenant_id, next_table_id, 1 /* replace */));
    _OB_LOG(INFO, "tenant_id = [%lu] exist, set new next_table_id = [%lu]", user_tenant_id, next_table_id);
  }
  return next_table_id;
}

void TestDDL::do_create_table(ObStmt*& stmt)
{
  // add the created table schema
  ObCreateTableStmt* create_table_stmt = dynamic_cast<ObCreateTableStmt*>(stmt);
  ObSEArray<ObColDesc, 16> col_ids;
  OB_ASSERT(NULL != create_table_stmt);
  schema::ObTableSchema table_schema;
  ASSERT_EQ(OB_SUCCESS, table_schema.assign(create_table_stmt->get_create_table_arg().schema_));
  table_schema.set_tablegroup_id(tablegroup_schema_.get_tablegroup_id());

  // combine the database_id and tenant_id
  table_schema.set_database_id(combine_id(table_schema.get_tenant_id(), table_schema.get_database_id()));
  // get the next_table_id of this tenant and database
  uint64_t next_table_id = get_next_table_id(table_schema.get_tenant_id());
  table_schema.set_table_id(combine_id(table_schema.get_tenant_id(), next_table_id));
  // table_schema.set_data_table_id( combine_id(next_user_tenant_id_, next_table_id));
  // only one zone
  table_schema.add_zone("test");
  table_schema.set_primary_zone("test");

  // mock create partition rpc call
  ON_CALL(rpc_proxy_, create_partition(_, _, _))
      .WillByDefault(Invoke(&rpc_proxy_, &MockObSrvRpcProxy::create_partition_wrapper));
  const int64_t frozen_version = 1;

  // TEN = next_user_tenant_id_;
  // TID = combine_id(TEN, table_schema.get_);
  TID = table_schema.get_table_id();
  for (int64_t i = 0; i < 3; ++i) {
    PID = i;
    prop_getter_.clear().add(A, LEADER);
    for (int64_t i = 0; i < prop_getter_.get_replicas().count(); ++i) {
      prop_getter_.get_replicas().at(i).unit_id_ = 1;
      ASSERT_EQ(OB_SUCCESS, pt_.update(prop_getter_.get_replicas().at(i)));
    }
  }

  // create unit
  bool if_not_exist = false;
  int ret = OB_SUCCESS;
  ret = ddl_service_.create_user_table(
      if_not_exist, table_schema, NULL, frozen_version, oceanbase::obrpc::OB_CREATE_TABLE_MODE_STRICT);
  ASSERT_EQ(OB_SUCCESS, ret);

  // ASSERT_EQ(OB_SUCCESS, table_schema.get_column_ids(col_ids));
  // for (int64_t i = 0; i < col_ids.count(); ++i) {
  //  const ObColumnSchemaV2 *col = table_schema.get_column_schema(col_ids.at(i).col_id_);
  //  const_cast<ObColumnSchemaV2*>(col)->set_table_id(table_schema.get_table_id());
  //}

  // ObArray<ObTableSchema> schema_array;
  // schema_array.push_back(table_schema);
  //_OB_LOG(INFO, "do_create_table table_name=[%s], table_id=[%lu], tenant_id=[%lu], database_id=[%lu]",
  //        table_schema.get_table_name(),
  //        table_schema.get_table_id(),
  //        table_schema.get_tenant_id(),
  //        table_schema.get_database_id());
  // OK(schema_mgr_->add_new_table_schema_array(schema_array));
  // schema_mgr_->print_info();

  // update the table_schema id in stmt
  // create_table_stmt->get_create_table_arg().schema_.set_table_id(
  //    combine_id(table_schema.get_tenant_id(), next_table_id));
}

void TestDDL::do_create_tablegroup(ObStmt*& stmt)
{
  ObCreateTablegroupStmt* create_tablegroup_stmt = dynamic_cast<ObCreateTablegroupStmt*>(stmt);
  OB_ASSERT(NULL != create_tablegroup_stmt);
  schema::ObTablegroupSchema tablegroup_schema = create_tablegroup_stmt->get_create_tablegroup_arg().tablegroup_schema_;
  // database_schema.set_tenant_id(next_user_database_id_);
  tablegroup_schema.set_tablegroup_id(combine_id(OB_USER_TENANT_ID, next_user_tablegroup_id_));
  int ret = OB_SUCCESS;
  bool if_not_exist = create_tablegroup_stmt->get_create_tablegroup_arg().if_not_exist_;
  ret = ddl_service_.create_tablegroup(if_not_exist, tablegroup_schema, NULL);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestDDL::do_drop_tablegroup(ObStmt*& stmt)
{
  ObDropTablegroupStmt* drop_tablegroup_stmt = dynamic_cast<ObDropTablegroupStmt*>(stmt);
  OB_ASSERT(NULL != drop_tablegroup_stmt);
  // database_schema.set_tenant_id(next_user_database_id_);
  int ret = OB_SUCCESS;
  ret = ddl_service_.drop_tablegroup(drop_tablegroup_stmt->get_drop_tablegroup_arg());
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestDDL::do_alter_tablegroup(ObStmt*& stmt)
{
  ObAlterTablegroupStmt* alter_tablegroup_stmt = dynamic_cast<ObAlterTablegroupStmt*>(stmt);
  OB_ASSERT(NULL != alter_tablegroup_stmt);
  int ret = OB_SUCCESS;

  ret = ddl_service_.alter_tablegroup(alter_tablegroup_stmt->get_alter_tablegroup_arg());
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestDDL::do_create_index(ObStmt*& stmt)
{
  // add the create index schema
  ObCreateIndexStmt* crt_idx_stmt = dynamic_cast<ObCreateIndexStmt*>(stmt);
  OB_ASSERT(NULL != crt_idx_stmt);
  int ret = OB_SUCCESS;
  const int64_t frozen_version = 1;
  ObIndexBuilder index_builder(ddl_service_);
  ret = index_builder.create_index(crt_idx_stmt->get_create_index_arg(), frozen_version);
  OB_ASSERT(OB_SUCC(ret));
  // generate_index_schema(*crt_idx_stmt);
}

void TestDDL::do_alter_table(ObStmt*& stmt)
{
  ObAlterTableStmt* alter_table_stmt = static_cast<ObAlterTableStmt*>(stmt);
  int ret = OB_SUCCESS;
  const int64_t frozen_version = 1;
  ret = ddl_service_.alter_table(alter_table_stmt->get_alter_table_arg(), frozen_version);
  OB_ASSERT(OB_SUCC(ret));
}

void TestDDL::do_create_user(ObStmt*& stmt)
{
  OB_ASSERT(stmt::T_CREATE_USER == stmt->get_stmt_type());
  ObCreateUserStmt* create_user_stmt = static_cast<ObCreateUserStmt*>(stmt);
  ObUserInfo user_info;
  ObArray<ObUserInfo> user_array;
  const ObStrings& users = create_user_stmt->get_users();
  ObString user_name;
  ObString host_name;
  ObString pwd;
  int64_t ret = OB_SUCCESS;
  for (int64_t i = 0; i < users.count(); i += 3) {
    if (OB_SUCCESS != (ret = users.get_string(i, user_name))) {
      _OB_LOG(WARN, "Get string from ObStrings error count=%lu, i=%ld, ret=%ld", users.count(), i, ret);
    } else if (OB_SUCCESS != (ret = users.get_string(i + 1, host_name))) {
      _OB_LOG(WARN, "Get string from ObStrings error count=%lu, i=%ld, ret=%ld", users.count(), i, ret);
    } else if (OB_SUCCESS != (ret = users.get_string(i + 2, pwd))) {
      // schema.set_tablegroup_id(combine_id(OB_SYS_TENANT_ID, OB_SYS_TABLEGROUP_ID));
      _OB_LOG(WARN, "Get string from ObStrings error count=%lu, i=%ld, ret=%ld", users.count(), i, ret);
    } else {
      ObUserInfo user_info;
      user_info.set_user_id(next_user_id_++);
      user_info.set_user_name(user_name);
      user_info.set_host(host_name);
      user_info.set_passwd(pwd);
      user_info.set_tenant_id(create_user_stmt->get_tenant_id());
      if (OB_SUCCESS != (ret = user_array.push_back(user_info))) {
        _OB_LOG(WARN, "Add user to array error");
      }
    }
  }
}

void TestDDL::generate_index_column_schema(ObSchemaGetterGuard& schema_guard, ObCreateIndexStmt& stmt,
    const ObTableSchema& data_schema, ObTableSchema& index_schema)
{
  int64_t index_rowkey_num = 0;
  uint64_t max_column_id = 0;
  const ObTableSchema* table_schema = NULL;
  ASSERT_EQ(OB_SUCCESS, schema_guard.get_table_schema(data_schema.get_table_id(), table_schema));
  ASSERT_FALSE(NULL == table_schema);

  ObCreateIndexArg& index_arg = stmt.get_create_index_arg();
  for (int64_t i = 0; i < index_arg.index_columns_.count(); ++i) {
    ObColumnSchemaV2 index_column;
    const ObColumnSchemaV2* col = table_schema->get_column_schema(index_arg.index_columns_[i].column_name_);
    ASSERT_FALSE(NULL == col);
    index_column = *col;
    ++index_rowkey_num;
    index_column.set_rowkey_position(index_rowkey_num);
    if (col->get_column_id() > max_column_id) {
      max_column_id = col->get_column_id();
    }
    ASSERT_EQ(OB_SUCCESS, index_schema.add_column(index_column));
  }
  // add primary key
  const ObRowkeyInfo& rowkey_info = table_schema->get_rowkey_info();
  for (int64_t i = 0; i < rowkey_info.get_size(); ++i) {
    uint64_t column_id = OB_INVALID_ID;
    ASSERT_EQ(OB_SUCCESS, rowkey_info.get_column_id(i, column_id));
    if (NULL == index_schema.get_column_schema(column_id)) {
      ++index_rowkey_num;
      const ObColumnSchemaV2* col = table_schema->get_column_schema(column_id);
      ASSERT_FALSE(NULL == col);
      ObColumnSchemaV2 index_column;
      index_column = *col;
      index_column.set_rowkey_position(index_rowkey_num);
      if (col->get_column_id() > max_column_id) {
        max_column_id = col->get_column_id();
      }
      ASSERT_EQ(OB_SUCCESS, index_schema.add_column(index_column));
    }
  }
  // add storing column
  for (int64_t i = 0; i < index_arg.store_columns_.count(); ++i) {
    const ObColumnSchemaV2* col = table_schema->get_column_schema(index_arg.store_columns_[i]);
    OB_ASSERT(col);
    if (col->get_column_id() > max_column_id) {
      max_column_id = col->get_column_id();
    }
    ASSERT_EQ(OB_SUCCESS, index_schema.add_column(*col));
  }
  index_schema.set_rowkey_column_num(index_rowkey_num);
  index_schema.set_max_used_column_id(max_column_id);
}

void TestDDL::generate_index_schema(ObCreateIndexStmt& stmt)
{
  ObTableSchema index_schema;
  ObCreateIndexArg& index_arg = stmt.get_create_index_arg();
  const bool is_index = false;
  ObSchemaGetterGuard guard;
  int ret = multi_schema_service_.get_schema_guard(guard);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObTableSchema* data_table_schema = NULL;
  const ObTableSchema* con_data_table_schema = NULL;
  ret = guard.get_table_schema(
      index_arg.tenant_id_, index_arg.database_name_, index_arg.table_name_, is_index, con_data_table_schema);
  ASSERT_EQ(OB_SUCCESS, ret);
  OB_ASSERT(con_data_table_schema);
  data_table_schema = const_cast<ObTableSchema*>(con_data_table_schema);
  generate_index_column_schema(guard, stmt, *data_table_schema, index_schema);
  ASSERT_EQ(OB_SUCCESS, index_schema.set_table_name(index_arg.index_name_));
  index_schema.set_block_size(index_arg.index_option_.block_size_);
  index_schema.set_is_use_bloomfilter(index_arg.index_option_.use_bloom_filter_);
  index_schema.set_progressive_merge_num(index_arg.index_option_.progressive_merge_num_);
  index_schema.set_data_table_id(data_table_schema->get_table_id());
  ASSERT_EQ(OB_SUCCESS, index_schema.set_compress_func_name(index_arg.index_option_.compress_method_));
  ASSERT_EQ(OB_SUCCESS, index_schema.set_comment(index_arg.index_option_.comment_));
  index_schema.set_table_type(USER_INDEX);
  index_schema.set_index_type(index_arg.index_type_);
  index_schema.set_tenant_id(sys_tenant_id_);
  index_schema.set_tablegroup_id(0);
  _OB_LOG(INFO, "origin index_schema database id is %ld", index_schema.get_database_id());
  // combine the database_id and tenant_id
  // index_schema.set_database_id(combine_id(next_user_tenant_id_, index_schema.get_database_id()));
  // get the next table of this tenant and database_id
  uint64_t next_index_tid = get_next_table_id(index_schema.get_tenant_id());
  index_schema.set_table_id(combine_id(index_schema.get_tenant_id(), next_index_tid));

  // database id is same as data_table schema
  index_schema.set_database_id(data_table_schema->get_database_id());
  // OK(schema_mgr_->add_new_table_schema(index_schema));
  if (data_table_schema != NULL) {
    // data_table_schema->add_index_tid(next_index_tid);
    data_table_schema->add_simple_index_info(
        ObAuxTableMetaInfo(index_schema.get_table_id(), USER_TABLE, OB_INVALID_VERSION));
  } else {
    _OB_LOG(ERROR, "no data table found ");
  }
  //_OB_LOG(INFO, "index_schema: %s", to_cstring(index_schema));
}

// TEST_F(TestDDL, liboblog)
//{
//  ObSchemaManagerGuard guard;
//  int ret = multi_schema_service_.get_schema_manager(guard);
//  ASSERT_EQ(OB_SUCCESS, ret);
//  const ObSchemaManager *schema_mgr = guard.get_schema_mgr();
//  ObArray<const ObTableSchema *> table_schemas;
//  ObArray<const ObDatabaseSchema *> database_schemas;
//  ret = schema_mgr->get_table_schemas_in_tenant(OB_SYS_TENANT_ID, table_schemas);
//  ASSERT_EQ(ret, OB_SUCCESS);
//  ret = schema_mgr->get_database_schemas_in_tenant(OB_SYS_TENANT_ID, database_schemas);
//  OB_LOG(INFO, "number of table schema ", "count", table_schemas.count());
//  for (int64_t i = 0; i < table_schemas.count(); ++i) {
//    const ObTableSchema *table_schema = table_schemas.at(i);
//    OB_LOG(INFO, "table", KT(table_schema->get_table_id()), K(table_schema->get_table_name_str()));
//  }
//  OB_LOG(INFO, "number of database schema", "count", database_schemas.count());
//  for (int64_t i = 0; i < database_schemas.count(); ++i) {
//    const ObDatabaseSchema *database_schema = database_schemas.at(i);
//    OB_LOG(INFO, "db", KT(database_schema->get_database_id()), K(database_schema->get_database_name_str()));
//  }
//  //user tenant
//  table_schemas.reset();
//  database_schemas.reset();
//  ret = schema_mgr->get_table_schemas_in_tenant(OB_USER_TENANT_ID, table_schemas);
//  ret = schema_mgr->get_database_schemas_in_tenant(OB_USER_TENANT_ID, database_schemas);
//  OB_LOG(INFO, "number of table schema ", "count", table_schemas.count());
//  for (int64_t i = 0; i < table_schemas.count(); ++i) {
//    const ObTableSchema *table_schema = table_schemas.at(i);
//    OB_LOG(INFO, "table", KT(table_schema->get_table_id()), K(table_schema->get_table_name_str()));
//  }
//  OB_LOG(INFO, "number of database schema", "count", database_schemas.count());
//  for (int64_t i = 0; i < database_schemas.count(); ++i) {
//    const ObDatabaseSchema *database_schema = database_schemas.at(i);
//    OB_LOG(INFO, "db", KT(database_schema->get_database_id()), K(database_schema->get_database_name_str()));
//  }
//}

// TEST_F(TestDDL, basic_test)
//{
//  //for test input sql in command line
//  if (clp.test_input_from_cmd){
//      input_test_from_cmd();
//      exit(0);
//  }
//
//  std::ofstream sys_tenant_of("result/test_sys_schema.result");
//  std::ofstream user_tenant_of("result/test_user_schema.result");
//
//  do_print_tenant_schema(OB_USER_TENANT_ID, user_tenant_of);
//  do_print_tenant_schema(sys_tenant_id_, sys_tenant_of);
//
//
//  const char *postfix[] = {"test","tmp","result"};
//  int64_t sql_postfix_len = strlen(postfix[0]);
//  int64_t tmp_postfix_len = strlen(postfix[1]);
//  int64_t result_postfix_len = strlen(postfix[2]);
//  char file_name[3][FILE_PATH_LEN];
//  //construct the file name ./sql/test_resolver_xxx.test  sql file
//  //construct the file name ./result/test_resolver_xxx.tmp  tmp result file
//  //construct the file name ./result/test_resolver_xxx.test  correct result file (now is empty)
//  for(int32_t i = 0; i < clp.file_count; ++i){
//    int64_t sql_file_len = strlen(clp.file_names_vector[i]);
//    snprintf(file_name[0],
//             strlen(SQL_DIR) + sql_file_len + sql_postfix_len + 4,
//             "./%s/%s%s",
//             SQL_DIR,
//             //clp.file_names[i],
//             clp.file_names_vector[i],
//             postfix[0]);
//    snprintf(file_name[1],
//             strlen(RESULT_DIR) + sql_file_len + tmp_postfix_len + 4,
//             "./%s/%s%s",
//             RESULT_DIR,
//             //clp.file_names[i],
//             clp.file_names_vector[i],
//             postfix[1]);
//    snprintf(file_name[2],
//             strlen(RESULT_DIR) + sql_file_len + result_postfix_len + 4,
//             "./%s/%s%s",
//             RESULT_DIR,
//            // clp.file_names[i],
//             clp.file_names_vector[i],
//             postfix[2]);
//    _OB_LOG(INFO, "%s\t%s\t%s\t%s",clp.file_names_vector[i],file_name[0], file_name[1], file_name[2]);
//
//    std::ifstream if_sql(file_name[0]);
//    if (!if_sql.is_open()){
//      _OB_LOG(ERROR,"file %s not exist!", file_name[0]);
//      continue;
//    }
//    ASSERT_TRUE(if_sql.is_open());
//    ObStmt *stmt = NULL;
//    std::ofstream of_tmp(file_name[1]);
//    ASSERT_TRUE(of_tmp.is_open());
//    if (!of_tmp.is_open()) {
//      _OB_LOG(ERROR,"file %s not exist!", file_name[1]);
//      continue;
//    }
//    std::string line;
//    int64_t case_id = 0;
//    bool is_print = false;
//    while (std::getline(if_sql, line)) {
//      if (line.size() <= 0) continue;
//      if (line.at(0) == '#') continue;
//      if (line.at(0) == '\r' || line.at(0) == '\n' ) continue;
//      stmt = NULL;
//      of_tmp << "***************   Case "<< ++case_id << "   ***************" << std::endl;
//      of_tmp << line << std::endl;
//      _OB_LOG(INFO, "case %ld: query str %s", case_id, line.c_str());
//      do_load_sql(line.c_str(),stmt, is_print, TREE_FORMAT);
//      do_print_schema(stmt,of_tmp);
//      ASSERT_FALSE(HasFatalFailure());
//      //of_tmp << SJ(*stmt) << std::endl;
//      stmt->~ObStmt();
//    }
//    of_tmp.close();
//    if_sql.close();
//    _OB_LOG(INFO, "test %s finished!, total %ld case", clp.file_names_vector[i], case_id);
//// verify result
//
//    ObSqlString cmd;
//    cmd.assign_fmt("diff -u %s %s", file_name[2], file_name[1]);
//    system(cmd.ptr());
//    std::cout<< std::endl << cmd.ptr() << std::endl;
////    ASSERT_EQ(0, system(cmd.ptr())) << cmd.ptr() << std::endl;
////    std::remove(file_name[1]);
//  }
//}

void print_help_msg(const char* exe_name)
{
  const char* msg = "Put you file test_resolver_xxx.test in the sql sub directory.\n\
 Then add the xxx to the command line param like ./test_resolver -c xxx,\n\
 It will resolve the sql in ./sql/test_resolver_xxx.test and print the result to ./result/test_resolver_xxx.tmp\n\
 If you don't config any param, it will resolver all the file in ./sql directory! \
 ./test_resolver -i can help to input sql from the command!";

  fprintf(stderr, msg);
  fprintf(stderr, "\nUsage: %s  [-c clause_type]\n\n", exe_name);
}

void TestDDL::input_test_from_cmd()
{
  std::ifstream if_schema(schema_file_path);
  ASSERT_TRUE(if_schema.is_open());
  std::string line;
  std::string schema_sql;
  while (std::getline(if_schema, line)) {
    schema_sql += "|\t";
    schema_sql += line;
    schema_sql += '\n';
  }
  if_schema.close();
  ObStmt* stmt = NULL;
  bool is_print = true;
  const char* line_separator = "-------------------------------------------------------------";
  while (true) {
    std::cout << line_separator << std::endl;
    std::cout << "|\t SQL in test_ddl.schema" << std::endl;
    std::cout << line_separator << std::endl;
    std::cout << schema_sql;
    std::cout << line_separator << std::endl;
    std::string sql;
    std::cout << "Please Input SQL: \n>";
    if (getline(std::cin, sql)) {
      std::cout << line_separator << std::endl;
      stmt = NULL;
      std::cout << "SQL=>" << sql << std::endl;
      std::cout << line_separator << std::endl;
      do_load_sql(sql.c_str(), stmt, is_print, TREE_FORMAT);
    }
  }
  // test_resolver->TearDown();
}

void parse_cmd_line_param(int argc, char* argv[], CmdLineParam& clp)
{

  if (1 == argc) {
    load_all_sql_files("./sql");
  } else {
    int opt = 0;
    const char* opt_string = "hc:id";
    struct option longopts[] = {{"help", 0, NULL, 'h'},  // help message
        {"clause_type",
            0,
            NULL,
            'c'},  // use in ./test_resolver -c select  // will run the test in sql/test_resolver_select.test
        {"input", 0, NULL, 'i'},  // ./test_resolver -i will help to quick test a sql in command line
        {"detail",
            0,
            NULL,
            'd'},  // ./test_resolver -id will print the detail info in json format in test_resolver.schema
        {0, 0, 0, 0}};

    memset(&clp, 0, sizeof(clp));
    // clp.reset();
    while ((opt = getopt_long(argc, argv, opt_string, longopts, NULL)) != -1) {
      _OB_LOG(DEBUG, "opt=%d,optarg=%s\n", opt, optarg);
      switch (opt) {
        case 'h': {
          print_help_msg("test_ddl");
          exit(0);
        }
        // add test_resolver_xxx.test
        case 'c': {
          char tmp_file_name[256];
          snprintf(tmp_file_name, strlen("test_ddl_") + strlen(optarg) + 7, "test_ddl_%s.test", optarg);
          _OB_LOG(INFO, "%s", tmp_file_name);
          load_sql_file(tmp_file_name);
          break;
        }
        case 'i': {
          clp.test_input_from_cmd = true;
          break;
        }
        case 'd': {
          clp.print_schema_detail_info = true;
          break;
        }
        default: {
          print_help_msg("test_ddl");
          load_all_sql_files("./sql");
          break;
          // exit(1);
        }
      }
    }
  }
}

int main(int argc, char** argv)
{
  clp.test_input_from_cmd = false;
  clp.print_schema_detail_info = false;
  // argc = 1;
  ::testing::InitGoogleTest(&argc, argv);
  parse_cmd_line_param(argc, argv, clp);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ddl.log");
  init_sql_factories();
  return RUN_ALL_TESTS();
}

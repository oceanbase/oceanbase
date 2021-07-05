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

#define USING_LOG_PREFIX SHARE

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#define private public
#define protected public
#include "rpc/frame/ob_net_easy.h"
#include "rpc/frame/ob_req_deliver.h"
#include "rpc/obrpc/ob_rpc_handler.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_session_handler.h"
#include "common/data_buffer.h"
#include "lib/random/ob_random.h"
#include "share/schema/ob_schema_service_rpc_proxy.h"
#include "mock_schema_service.h"
#include "schema_test_utils.h"

namespace oceanbase {
namespace share {
namespace schema {
using namespace oceanbase;
using namespace oceanbase::rpc::frame;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::rpc;
using namespace std;
#define IO_CNT 1

class QHandler : public ObiReqQHandler {
public:
  QHandler(ObMultiVersionSchemaService* schema_service) : mp_(schema_service)
  {
    mp_.init();
    mp_.set_session_handler(shandler_);
  }

  virtual int onThreadCreated(obsys::CThread*)
  {
    return OB_SUCCESS;
  }
  virtual int onThreadDestroy(obsys::CThread*)
  {
    return OB_SUCCESS;
  }

  bool handlePacketQueue(ObRequest* req, void*)
  {
    const ObRpcPacket& pkt = reinterpret_cast<const ObRpcPacket&>(req->get_packet());
    if (!pkt.is_stream()) {
      LOG_INFO("not stream");
      mp_.set_ob_request(*req);
      mp_.run();
    } else {
      if (!shandler_.wakeup_next_thread(*req)) {
        easy_request_wakeup(req->get_request());
      }
    }
    return true;
  }

private:
  ObGetAllSchemaP mp_;
  ObRpcSessionHandler shandler_;
  ObRpcReqContext ctx_;
};

class ObTestDeliver : public rpc::frame::ObReqDeliver {
public:
  ObTestDeliver(ObMultiVersionSchemaService* schema_service) : handler_(schema_service)
  {}
  int init()
  {
    queue_.setThreadParameter(2, &handler_, NULL);
    queue_.start();
    return 0;
  }

  int deliver(rpc::ObRequest& req)
  {
    queue_.push(&req, 10);
    return 0;
  }

  void stop()
  {
    queue_.stop();
    queue_.wait();
  }

protected:
  ObReqQueueThread queue_;
  QHandler handler_;
};

class TestSchemaServiceRpcProxy : public ::testing::Test {
public:
  TestSchemaServiceRpcProxy() : port_(3100), handler_(server_), server_(&schema_service_), transport_(NULL)
  {}

  virtual void SetUp()
  {
    server_.init();

    ObNetOptions opts;
    opts.rpc_io_cnt_ = IO_CNT;
    net_.init(opts);
    port_ = static_cast<int32_t>(rand.get(3000, 5000));
    while (OB_SUCCESS != net_.add_rpc_listen(port_, handler_, transport_)) {
      port_ = static_cast<int32_t>(rand.get(3000, 5000));
    }
    net_.start();

    int ret = OB_SUCCESS;
    schema_service_.init();
    ObTenantSchema tenant_schema;
    ObUserInfo user_schema;
    ObDatabaseSchema db_schema;
    ObTablegroupSchema tg_schema;
    ObTableSchema table_schema;
    ObOutlineInfo outline_schema;
    ObDBPriv db_priv;
    ObTablePriv table_priv;
    ObSysVariableSchema sys_variable;
    GEN_TENANT_SCHEMA(tenant_schema, 1, "tenant", 0);
    tenant_schema.set_locality("");
    tenant_schema.add_zone("zone");
    ret = schema_service_.add_tenant_schema(tenant_schema, tenant_schema.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);
    sys_variable.set_tenant_id(tenant_schema.get_tenant_id());
    sys_variable.set_schema_version(tenant_schema.get_schema_version());
    sys_variable.set_name_case_mode(OB_LOWERCASE_AND_INSENSITIVE);
    ASSERT_EQ(OB_SUCCESS, schema_service_.add_sys_variable_schema(sys_variable, sys_variable.get_schema_version()));

    GEN_USER_SCHEMA(user_schema, 1, combine_id(1, 1), "user", 0);
    ret = schema_service_.add_user_schema(user_schema, user_schema.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);

    GEN_DATABASE_SCHEMA(db_schema, 1, combine_id(1, 1), "db", 0);
    ret = schema_service_.add_database_schema(db_schema, db_schema.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);

    GEN_TABLEGROUP_SCHEMA(tg_schema, 1, combine_id(1, 1), "tg", 0);
    ret = schema_service_.add_tablegroup_schema(tg_schema, tg_schema.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);

    char table_name[20];
    for (int i = 0; i < 10000; ++i) {
      snprintf(table_name, 20, "table_%d", i + 1);
      GEN_TABLE_SCHEMA(table_schema, 1, combine_id(1, 1), combine_id(1, i + 1), table_name, USER_TABLE, 0);
      ret = schema_service_.add_table_schema(table_schema, table_schema.get_schema_version());
      ASSERT_EQ(OB_SUCCESS, ret);
    }

    GEN_OUTLINE_SCHEMA(outline_schema, 1, combine_id(1, 1), combine_id(1, 1), "outine", "sig", 0);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = schema_service_.add_outline_schema(outline_schema, outline_schema.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);

    GEN_DB_PRIV(db_priv, 1, combine_id(1, 1), "db", OB_PRIV_SELECT, 0);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = schema_service_.add_db_priv(db_priv, db_priv.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);

    GEN_TABLE_PRIV(table_priv, 1, combine_id(1, 1), "db", "table", OB_PRIV_SELECT, 0);
    ASSERT_EQ(OB_SUCCESS, ret);
    ret = schema_service_.add_table_priv(table_priv, table_priv.get_schema_version());
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  virtual void TearDown()
  {
    net_.stop();
    net_.wait();
    server_.stop();
  }

  int send(const char* buf, int len)
  {
    ObReqTransport::Request<ObRpcPacket> req;
    ObReqTransport::Result<ObRpcPacket> res;
    ObAddr dst(ObAddr::IPV4, "127.0.0.1", port_);
    int64_t payload = len;
    transport_->create_request(req, dst, payload, 3000000);
    memcpy(req.buf(), buf, len);
    return transport_->send(req, res);
  }

protected:
  int port_;
  rpc::frame::ObNetEasy net_;
  obrpc::ObRpcHandler handler_;
  MockSchemaService schema_service_;
  ObTestDeliver server_;
  rpc::frame::ObReqTransport* transport_;
  ObRandom rand;
};

TEST_F(TestSchemaServiceRpcProxy, TestGetLatestSchemaVersionP)
{
  int ret = OB_SUCCESS;
  MockSchemaService schema_service;
  schema_service.refreshed_schema_version_ = 1;
  ObGetLatestSchemaVersionP processor(&schema_service);
  ret = processor.process();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, processor.result_);
}

TEST_F(TestSchemaServiceRpcProxy, ObGetAllSchemaP)
{
  int ret = OB_SUCCESS;
  ObAddr dst(ObAddr::IPV4, "127.0.0.1", port_);
  ObSchemaServiceRpcProxy proxy;
  proxy.init(transport_, dst);

  ObArenaAllocator allocator;
  const static int64_t MAX_BUF_LEN = 10 * 1000 * 1000;
  const static int64_t MAX_BUF_LEN_PER_STREAM = OB_MALLOC_BIG_BLOCK_SIZE;
  char* buf = static_cast<char*>(allocator.alloc(MAX_BUF_LEN));
  char* data = static_cast<char*>(allocator.alloc(MAX_BUF_LEN_PER_STREAM));
  ASSERT_TRUE(NULL != buf);
  ASSERT_TRUE(NULL != data);

  ObDataBuffer buffer;
  buffer.set_data(data, MAX_BUF_LEN_PER_STREAM);

  ObGetAllSchemaArg arg;
  arg.schema_version_ = -1;
  arg.tenant_name_ = "tenant";
  typedef ObSchemaServiceRpcProxy::SSHandle<OB_GET_ALL_SCHEMA> SchemaHandle;
  SchemaHandle handle;
  ret = proxy.get_all_schema(arg, buffer, handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t pos = 0;
  while (OB_SUCC(ret)) {
    memcpy(buf + pos, buffer.get_data(), buffer.get_position());
    pos += buffer.get_position();
    if (!handle.has_more()) {
      break;
    } else {
      LOG_INFO("stream has more");
      ret = handle.get_more(buffer);
      ASSERT_EQ(OB_SUCCESS, ret);
    }
  }

  if (OB_SUCC(ret)) {
    int64_t len = pos;
    pos = 0;
    ObAllSchema all_schema;
    ret = all_schema.deserialize(buf, len, pos);
    ASSERT_EQ(OB_SUCCESS, ret);
    LOG_INFO("tenant", K(all_schema.tenant_));
  }
  // ASSERT_TRUE(SchemaTestUtils::equal_tenant_schema(tenant_schema, all_schema.tenant_));
}

}  // end namespace schema
}  // end namespace share
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

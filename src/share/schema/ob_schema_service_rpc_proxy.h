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

#ifndef OCEANBASE_COMMON_OB_SCHEMA_SERVICE_RPC_PROXY_
#define OCEANBASE_COMMON_OB_SCHEMA_SERVICE_RPC_PROXY_

#include "share/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_rpc_struct.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_proxy.h"
#include "rpc/obrpc/ob_rpc_processor.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace obrpc
{
class ObSchemaServiceRpcProxy : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObSchemaServiceRpcProxy);
//  RPC_S(PR5 get_latest_schema_version, OB_GET_LATEST_SCHEMA_VERSION, obrpc::Int64);
  RPC_SS(PR5 get_all_schema, OB_GET_ALL_SCHEMA, (ObGetAllSchemaArg), common::ObDataBuffer);
};

/*
class ObGetLatestSchemaVersionP : public ObRpcProcessor<
                        obrpc::ObSchemaServiceRpcProxy::ObRpc<OB_GET_LATEST_SCHEMA_VERSION> >
{
public:
  explicit ObGetLatestSchemaVersionP(share::schema::ObMultiVersionSchemaService *schema_service)
    : schema_service_(schema_service)
  {}
  virtual ~ObGetLatestSchemaVersionP()
  {
  }
protected:
  int process();
private:
  share::schema::ObMultiVersionSchemaService *schema_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLatestSchemaVersionP);
};
*/

struct ObAllSchema
{
  OB_UNIS_VERSION(1);

public:
  ObAllSchema() {}
  DECLARE_TO_STRING;

  share::schema::ObTenantSchema tenant_;
  common::ObSArray<share::schema::ObUserInfo> users_;
  common::ObSArray<share::schema::ObDatabaseSchema> databases_;
  common::ObSArray<share::schema::ObTablegroupSchema> tablegroups_;
  common::ObSArray<share::schema::ObTableSchema> tables_;
  common::ObSArray<share::schema::ObOutlineInfo> outlines_;
  common::ObSArray<share::schema::ObDBPriv> db_privs_;
  common::ObSArray<share::schema::ObTablePriv> table_privs_;
};

class ObGetAllSchemaP : public ObRpcProcessor<
                        obrpc::ObSchemaServiceRpcProxy::ObRpc<OB_GET_ALL_SCHEMA> >
{
public:
  explicit ObGetAllSchemaP(share::schema::ObMultiVersionSchemaService *schema_service);
  virtual ~ObGetAllSchemaP();
protected:
  virtual int before_process();
  virtual int process();
  virtual int after_process();
private:
  share::schema::ObMultiVersionSchemaService *schema_service_;
  char *buf_;
  int64_t buf_len_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetAllSchemaP);
};

} // namespace obrpc
} // namespace oceanbase

#endif //OCEANBASE_COMMON_OB_SCHEMA_SERVICE_RPC_PROXY_

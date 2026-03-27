/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBMYSQL_OB_SQL_SOCK_PROCESSOR_H_
#define OCEANBASE_OBMYSQL_OB_SQL_SOCK_PROCESSOR_H_
#include "rpc/obmysql/ob_mysql_protocol_processor.h"
#include "rpc/obmysql/ob_mysql_compress_protocol_processor.h"
#include "rpc/obmysql/ob_2_0_protocol_processor.h"

namespace oceanbase
{
namespace rpc { class ObRequest; } // end of namespace rpc
namespace rpc { namespace frame { class ObReqTranslator; } }

namespace obmysql
{
class ObSqlNio;
class ObMySQLhandler;
class ObSqlSockSession;
class ObSqlSockProcessor
{
public:
  ObSqlSockProcessor(ObMySQLHandler& handler):
      mysql_processor_(), compress_processor_(), ob_2_0_processor_() {}
  ~ObSqlSockProcessor() {}
  int decode_sql_packet(ObICSMemPool& mem_pool, ObSqlSockSession& sess, void* read_handle, rpc::ObPacket*& pkt);
  int build_sql_req(ObSqlSockSession& sess, rpc::ObPacket* pkt, rpc::ObRequest*& sql_req);
private:
  ObVirtualCSProtocolProcessor *get_protocol_processor(common::ObCSProtocolType type);
private:
  ObMysqlProtocolProcessor mysql_processor_;
  ObMysqlCompressProtocolProcessor compress_processor_;
  Ob20ProtocolProcessor ob_2_0_processor_;
};

}; // end namespace obmysql
}; // end namespace oceanbase

#endif /* OCEANBASE_OBMYSQL_OB_SQL_SOCK_PROCESSOR_H_ */

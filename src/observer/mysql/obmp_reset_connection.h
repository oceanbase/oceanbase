/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_OBSERVER_OBMP_RESET_CONNECTION
#define OCEANBASE_OBSERVER_OBMP_RESET_CONNECTION

#include "lib/string/ob_string.h"
#include "observer/mysql/obmp_base.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "sql/parser/parse_node.h"
namespace oceanbase
{
namespace sql
{
class ObBasicSessionInfo;
}
namespace observer
{
class ObMPResetConnection : public ObMPBase
{
public:
  static const obmysql::ObMySQLCmd COM = obmysql::COM_RESET_CONNECTION;
  explicit ObMPResetConnection(const ObGlobalContext &gctx)
      :ObMPBase(gctx),
      pkt_()
  {
  }

  virtual ~ObMPResetConnection() {}

protected:
  int process();
  virtual int deserialize()
  { return common::OB_SUCCESS; }

private:

private:
  obmysql::ObMySQLRawPacket pkt_;
  DISALLOW_COPY_AND_ASSIGN(ObMPResetConnection);
};// end of class

} // end of namespace observer
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OBMP_RESET_CONNECTION */

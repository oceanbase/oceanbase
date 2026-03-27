/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OMPK_ROW_H_
#define _OMPK_ROW_H_

#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_row.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKRow : public ObMySQLPacket
{
public:
  explicit OMPKRow(const ObMySQLRow &row);
  virtual ~OMPKRow() { }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_ROW; }

  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;
private:
  DISALLOW_COPY_AND_ASSIGN(OMPKRow);
  const ObMySQLRow &row_;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_ROW_H_ */

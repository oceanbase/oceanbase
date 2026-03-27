/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef _OMPK_FIELD_H_
#define _OMPK_FIELD_H_

#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_mysql_field.h"

namespace oceanbase
{
namespace obmysql
{

class OMPKField
    : public ObMySQLPacket
{
public:
  explicit OMPKField(ObMySQLField &field);
  virtual ~OMPKField() { }
  inline ObMySQLPacketType get_mysql_packet_type() { return ObMySQLPacketType::PKT_FIELD; }

  virtual int serialize(char *buffer, int64_t len, int64_t &pos) const;
private:
  DISALLOW_COPY_AND_ASSIGN(OMPKField);

  ObMySQLField &field_;
};

} // end of namespace obmysql
} // end of namespace oceanbase

#endif /* _OMPK_FIELD_H_ */

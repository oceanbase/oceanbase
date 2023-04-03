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

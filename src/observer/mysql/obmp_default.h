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

#ifndef _OBMP_DEFAULT_H
#define _OBMP_DEFAULT_H

#include "observer/mysql/obmp_base.h"

namespace oceanbase
{
namespace observer
{
// this processor always returns NOT_SUPPORTED error to client
class ObMPDefault: public ObMPBase
{
public:
  explicit ObMPDefault(const ObGlobalContext &gctx)
      :ObMPBase(gctx)
  {}
  virtual ~ObMPDefault() {}

  int process()
  {
    int ret = OB_SUCCESS;
    if (OB_ISNULL(req_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "req_ is NULL", K(ret));
    } else {
      const obmysql::ObMySQLRawPacket &pkt = reinterpret_cast<const obmysql::ObMySQLRawPacket&>(req_->get_packet());
      if (OB_FAIL(send_error_packet(common::OB_NOT_SUPPORTED, NULL))) {
        SERVER_LOG(WARN, "failed to send error packet", K(ret));
      } else {
        SERVER_LOG(WARN, "MySQL command not supported", "cmd", pkt.get_cmd());
      }
    }
    return ret;
  }
  int deserialize() { return common::OB_SUCCESS; }
};

} // end namespace observer
} // end namespace oceanbase

#endif /* _OBMP_DEFAULT_H */

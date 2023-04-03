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

#ifndef OCEANBASE_SHARE_OB_I_SERVER_AUTH_H_
#define OCEANBASE_SHARE_OB_I_SERVER_AUTH_H_
#include "lib/net/ob_addr.h"
#include "io/easy_io_struct.h"

namespace oceanbase
{
namespace share
{
class ObIServerAuth
{
public:
  ObIServerAuth() {}
  virtual ~ObIServerAuth() {}
  virtual int is_server_legitimate(const common::ObAddr& addr, bool& is_valid) = 0;
  virtual int check_ssl_invited_nodes(easy_connection_t &c) = 0;
  virtual void set_ssl_invited_nodes(const common::ObString &new_value) = 0;
};
}; // end namespace share
}; // end namespace oceanbase

#endif /* OCEANBASE_SHARE_OB_I_SERVER_AUTH_H_ */


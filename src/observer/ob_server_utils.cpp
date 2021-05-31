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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_server_utils.h"
#include "observer/ob_server_struct.h"

namespace oceanbase {
using namespace common;

namespace observer {

int ObServerUtils::get_server_ip(ObIAllocator* allocator, ObString& ipstr)
{
  int ret = OB_SUCCESS;
  ObAddr addr = GCTX.self_addr_;
  char ip_buf[OB_IP_STR_BUFF] = {'\0'};
  if (OB_ISNULL(allocator)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid alloctor pointer is NULL", K(ret));
  } else if (!addr.ip_to_string(ip_buf, sizeof(ip_buf))) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip to string failed", K(ret));
  } else {
    ObString ipstr_tmp = ObString::make_string(ip_buf);
    if (OB_FAIL(ob_write_string(*allocator, ipstr_tmp, ipstr))) {
      SERVER_LOG(WARN, "ob write string failed", K(ret));
    } else if (ipstr.empty()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "host ip is empty", K(ret));
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase

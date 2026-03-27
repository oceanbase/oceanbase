/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE

#include "ob_root_addr_agent.h"

namespace oceanbase
{
namespace share
{
  
OB_SERIALIZE_MEMBER(ObRootAddr,
    server_,
    role_,
    sql_port_);

}
}


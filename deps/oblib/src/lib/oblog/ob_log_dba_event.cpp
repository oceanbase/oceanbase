/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "lib/oblog/ob_log_dba_event.h"

 namespace oceanbase
{
namespace common
{
DEFINE_DBA_EVENT_LIST_CPP(server_start)
DEFINE_DBA_EVENT_LIST_CPP(bootstrap)
DEFINE_DBA_EVENT_LIST_CPP(replace_sys)
}
}

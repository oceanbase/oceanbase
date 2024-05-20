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

#include "lib/ob_define.h"
__thread uint64_t tl_thread_tenant_id = oceanbase::common::OB_SERVER_TENANT_ID;

extern int register_query_trace(int64_t tenant_id, int64_t session_id)
{
  return 0;
}

extern int register_transaction_trace(int64_t tenant_id, int64_t trans_id)
{
  return 0;
}

extern int register_logstream_trace(int64_t tenant_id, int64_t ls_id)
{
  return 0;
}

extern int register_tablet_trace(int64_t tenant_id, int64_t tablet_id)
{
  return 0;
}

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

#define USING_LOG_PREFIX RPC_FRAME

#include "rpc/frame/ob_req_processor.h"

#include "lib/time/ob_time_utility.h"
#include "rpc/ob_request.h"
#include "lib/oblog/ob_trace_log.h"
#include "lib/rc/context.h"
#include "rpc/obmysql/ob_mysql_request_utils.h"
using namespace oceanbase::common;
using namespace oceanbase::rpc::frame;

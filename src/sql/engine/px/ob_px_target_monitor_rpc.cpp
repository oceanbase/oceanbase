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

#define USING_LOG_PREFIX SQL_ENG
#include "ob_px_target_monitor_rpc.h"

namespace oceanbase
{
namespace sql
{

OB_SERIALIZE_MEMBER(ObPxRpcAddrTarget, addr_, target_);
OB_SERIALIZE_MEMBER(ObPxRpcFetchStatArgs, tenant_id_, follower_version_, addr_target_array_, need_refresh_all_);
OB_SERIALIZE_MEMBER(ObPxRpcFetchStatResponse, status_, tenant_id_, leader_version_, addr_target_array_);

}
}

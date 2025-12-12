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

#include "ob_lock_wait_mgr_msg.h"

namespace oceanbase
{
namespace lockwaitmgr
{
OB_SERIALIZE_MEMBER(ObLockWaitMgrMsg,
                    type_,
                    tenant_id_,
                    hash_,
                    node_id_,
                    sender_addr_);
OB_SERIALIZE_MEMBER((ObLockWaitMgrDstEnqueueMsg, ObLockWaitMgrMsg),
                    lock_seq_,
                    lock_ts_,
                    tx_id_,
                    holder_tx_id_,
                    sess_id_,
                    query_timeout_us_,
                    recv_ts_,
                    ls_id_,
                    tablet_id_,
                    holder_tx_hold_seq_value_,
                    tx_active_ts_,
                    abs_timeout_ts_,
                    query_sql_);
OB_SERIALIZE_MEMBER( (ObLockWaitMgrDstEnqueueRespMsg, ObLockWaitMgrMsg),
                    enqueue_succ_);
OB_SERIALIZE_MEMBER((ObLockWaitMgrCheckNodeStateMsg, ObLockWaitMgrMsg));
OB_SERIALIZE_MEMBER((ObLockWaitMgrCheckNodeStateRespMsg, ObLockWaitMgrMsg),
                     is_exsit_);
OB_SERIALIZE_MEMBER((ObLockWaitMgrLockReleaseMsg, ObLockWaitMgrMsg));
OB_SERIALIZE_MEMBER((ObLockWaitMgrWakeUpRemoteMsg, ObLockWaitMgrMsg));

} // lockwaitmgr

} // oceanbase
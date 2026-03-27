/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
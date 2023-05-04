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
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_rpc_process.h"
#include "sql/engine/px/p2p_datahub/ob_p2p_dh_mgr.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace obrpc;


int ObPxP2pDhMsgP::process()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(arg_.msg_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to process px p2p datahub msg", K(ret), K(arg_));
  } else if (OB_FAIL(PX_P2P_DH.process_msg(*arg_.msg_))) {
    LOG_WARN("failed to process px p2p datahub msg on dh mgr", K(ret));
  }
  arg_.destroy_arg();
  return ret;
}

void ObPxP2pDhMsgCB::on_timeout()
{
  int ret = OB_SUCCESS;
  LOG_WARN("failed to send p2p datahub message,", K(get_error()), K(start_time_),
      K(timeout_ts_), K(addr_), K(trace_id_));
}


int ObPxP2pDhClearMsgP::process()
{
  int ret = OB_SUCCESS;
  ObIArray<int64_t> &array = arg_.p2p_dh_ids_;
  ObP2PDhKey key;
  ObP2PDatahubMsgBase *msg = nullptr;
  for (int i = 0; i < array.count(); ++i) {
    ret = OB_SUCCESS;
    key.p2p_datahub_id_ = array.at(i);
    key.task_id_ = 0;
    key.px_sequence_id_ = arg_.px_seq_id_;
    bool is_erased = false;
    if (OB_FAIL(PX_P2P_DH.erase_msg_if(key, msg, is_erased, true/* need unregister dm */)) || !is_erased) {
      LOG_TRACE("fail to erase msg", K(ret));
    }
  }
  return ret;
}

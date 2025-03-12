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

#define USING_LOG_PREFIX STORAGE

#include "ob_lob_retry.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "storage/lob/ob_lob_location.h"

namespace oceanbase
{
namespace storage
{

int ObLobRetryUtil::check_need_retry(ObLobAccessParam &param, const int error_code, const int retry_cnt, bool &need_retry)
{
  int ret = OB_SUCCESS;
  if (param.from_rpc_) {
    need_retry = false;
    LOG_WARN("can not retry because from rpc", K(ret), KR(ret), K(error_code), KR(error_code), K(retry_cnt), K(need_retry), K(param));
  } else if (! is_remote_ret_can_retry(error_code)) {
    LOG_WARN("can not retry error code", K(ret), KR(ret), K(error_code), KR(error_code), K(retry_cnt), K(need_retry), K(param));
  } else if (ObTimeUtility::current_time() > param.timeout_) {
    need_retry = false;
    ret = OB_TIMEOUT;
    int64_t cur_time = ObTimeUtility::current_time();
    LOG_WARN("[LOB RETRY] query timeout", K(cur_time), K(param.timeout_), K(ret));
  } else if (IS_INTERRUPTED()) {
    need_retry = false;
    LOG_INFO("[LOB RETRY] Retry is interrupted by worker interrupt signal", KR(ret), K(error_code), KR(error_code), K(retry_cnt), K(need_retry));
  } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
    need_retry = false;
    ret = OB_KILLED_BY_THROTTLING;
    LOG_INFO("[LOB RETRY] Retry is interrupted by worker check wait", K(ret), KR(ret), K(error_code), KR(error_code), K(retry_cnt), K(need_retry));
  } else {
    need_retry = true;
    switch (error_code) {
      case  OB_LS_NOT_EXIST: { // check if tenant has been dropped when ls not exist
        int tmp_ret = OB_SUCCESS;
        bool is_dropped = false;
        share::schema::ObMultiVersionSchemaService *schema_service = GCTX.schema_service_;
        if (OB_ISNULL(schema_service)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("schema_service is nullptr", "tmp_ret", tmp_ret);
        } else if (OB_SUCCESS != (tmp_ret = schema_service->check_if_tenant_has_been_dropped(param.tenant_id_, is_dropped))) {
          LOG_WARN("check if tenant has been dropped fail", "tmp_ret", tmp_ret);
        } else if (is_dropped) {
          ret = OB_TENANT_HAS_BEEN_DROPPED;
          need_retry = false;
        } // no need break
      }
      case	OB_REPLICA_NOT_READABLE:
      case  OB_RPC_CONNECT_ERROR:
      case  OB_RPC_SEND_ERROR:
      case  OB_RPC_POST_ERROR:
      case  OB_NOT_MASTER:
      case  OB_NO_READABLE_REPLICA:
      case  OB_TABLET_NOT_EXIST:
      case  OB_LS_OFFLINE: {
        if (!need_retry) {
        } else if (OB_FAIL(ObLobLocationUtil::lob_refresh_location(param, error_code, retry_cnt))) {
          LOG_WARN("fail to do refresh location", K(ret), K(error_code), K(retry_cnt), K(param));
          need_retry = false;
        }
        LOG_INFO("retry again", K(ret), KR(ret), K(error_code), KR(error_code), K(retry_cnt), K(need_retry), K(param));
        break;
      }
      default: {
        need_retry = false;
        LOG_WARN("unknow retry error_code, not retry", K(ret), KR(ret), K(error_code), KR(error_code), K(retry_cnt), K(need_retry));
      }
    }
  }
  return ret;
}

bool ObLobRetryUtil::is_remote_ret_can_retry(int ret)
{
  return (ret == OB_REPLICA_NOT_READABLE) ||
         (ret == OB_RPC_CONNECT_ERROR) ||
         (ret == OB_RPC_SEND_ERROR) ||
         (ret == OB_RPC_POST_ERROR) ||
         (ret == OB_NOT_MASTER) ||
         (ret == OB_NO_READABLE_REPLICA) ||
         (ret == OB_TABLET_NOT_EXIST) ||
         (ret == OB_LS_NOT_EXIST) ||
         (ret == OB_LS_OFFLINE);
}

} // storage
} // oceanbase

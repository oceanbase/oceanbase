/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_TRANSACTION_OB_TX_FREE_ROUTE_MSG_
#define OCEANBASE_TRANSACTION_OB_TX_FREE_ROUTE_MSG_
#include "share/ob_define.h"
#include "ob_trans_define.h"
#include "ob_tx_msg.h"
namespace oceanbase {
namespace transaction {
struct ObTxFreeRouteMsg
{
  ObTxFreeRouteMsg(int type) : type_(type) {}
  const int type_;
  virtual bool is_valid() const = 0;
  DECLARE_PURE_VIRTUAL_TO_STRING;
  PURE_VIRTUAL_NEED_SERIALIZE_AND_DESERIALIZE;
};

struct ObTxFreeRoutePushState
{
  ObTxFreeRoutePushState()
    : type_(TX_FREE_ROUTE_PUSH_STATE),
      tenant_id_(0), tx_id_(), logic_clock_(0), static_offset_(0),
      dynamic_offset_(0), parts_offset_(0), extra_offset_(0), buf_() {}
  ~ObTxFreeRoutePushState() {
    if (OB_NOT_NULL(buf_.ptr())) {
      ob_free(buf_.ptr());
    }
    buf_.reset();
  }
  const int type_;
  uint64_t tenant_id_;
  ObTransID tx_id_;
  int64_t logic_clock_;
  int64_t static_offset_; // just for MACRO
  int64_t dynamic_offset_;
  int64_t parts_offset_;
  int64_t extra_offset_;
  ObString buf_;
  bool is_valid() const {
    return tenant_id_ > 0
      && tx_id_.is_valid()
      && logic_clock_ > 0
      && dynamic_offset_ >= sizeof(*this)
      && parts_offset_ >= dynamic_offset_
      && extra_offset_ >= parts_offset_
      && buf_.length() >= 0
      && OB_NOT_NULL(buf_.ptr());
  }
  TO_STRING_KV(K_(tenant_id), K_(tx_id), K_(logic_clock), K_(dynamic_offset), K_(parts_offset), K_(extra_offset), K(buf_.length()));
  OB_UNIS_VERSION(1);
};

struct ObTxFreeRoutePushStateResp
{
  int ret_;
  OB_UNIS_VERSION(1);
  TO_STRING_KV(K_(ret));
};

struct ObTxFreeRouteCheckAliveMsg : ObTxFreeRouteMsg
{
 ObTxFreeRouteCheckAliveMsg() : ObTxFreeRouteMsg(TX_FREE_ROUTE_CHECK_ALIVE) {}
  int64_t request_id_;
  uint32_t req_sess_id_;
  uint32_t tx_sess_id_;
  ObTransID tx_id_;
  ObAddr sender_;
  ObAddr receiver_;
  bool is_valid() const {
      return request_id_ > 0
        && req_sess_id_ > 0
        && tx_sess_id_ > 0
        && tx_id_.is_valid()
        && sender_.is_valid()
        && receiver_.is_valid();
  }
  TO_STRING_KV(K_(type), K_(request_id), K_(req_sess_id), K_(tx_sess_id), K_(tx_id), K_(sender), K_(receiver));
  OB_UNIS_VERSION(1);
};

struct ObTxFreeRouteCheckAliveRespMsg : ObTxFreeRouteMsg
{
  ObTxFreeRouteCheckAliveRespMsg() : ObTxFreeRouteMsg(TX_FREE_ROUTE_CHECK_ALIVE_RESP) {}
  int64_t request_id_;
  uint32_t req_sess_id_;
  ObTransID tx_id_;
  ObAddr sender_;
  ObAddr receiver_;
  int ret_;
  bool is_valid() const {
    return request_id_ > 0
      && req_sess_id_ > 0
      && tx_id_.is_valid()
      && sender_.is_valid()
      && receiver_.is_valid();
  }
  TO_STRING_KV(K_(type), K_(request_id), K_(req_sess_id), K_(tx_id), K_(ret), K_(sender), K_(receiver));
  OB_UNIS_VERSION(1);
};

}
}
#endif // OCEANBASE_TRANSACTION_OB_TX_FREE_ROUTE_MSG_

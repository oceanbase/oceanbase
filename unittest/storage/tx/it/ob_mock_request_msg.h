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
#ifndef OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_REQUEST_MSG
#define OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_REQUEST_MSG

#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
using namespace transaction;
namespace memtable
{
enum FAKE_REQUEST_TYPE {
  INVALID_REQUEST = 0,
  FAKE_WRITE_REQUEST = 1,
  FAKE_WRITE_REQUEST_RESP = 2
};

struct ObFakeRequestMsg : ObLink {
  ObFakeRequestMsg(int16_t type = FAKE_REQUEST_TYPE::INVALID_REQUEST) : type_(type), send_addr_() {}
  virtual ~ObFakeRequestMsg() {}
  int16_t type_;
  ObAddr send_addr_;
  VIRTUAL_TO_STRING_KV(K_(type), K_(send_addr));
  OB_UNIS_VERSION_V(1);
};

struct ObFakeWriteRequestMsg : ObFakeRequestMsg {
  ObFakeWriteRequestMsg() : ObFakeRequestMsg(FAKE_REQUEST_TYPE::FAKE_WRITE_REQUEST) {}
  ~ObFakeWriteRequestMsg() {}
  ObTxDesc *tx_;
  int64_t key_;
  int64_t value_;
  int64_t expire_ts_;
  bool inc_seq_;
  ObTxParam tx_param_;
  INHERIT_TO_STRING_KV("ObFakeRequestMsg", ObFakeRequestMsg, KPC_(tx), K_(key), K_(value), K_(expire_ts), K_(inc_seq), K_(tx_param));
  OB_UNIS_VERSION_V(1);
};

struct ObFakeWriteRequestRespMsg : ObFakeRequestMsg {
  ObFakeWriteRequestRespMsg() : ObFakeRequestMsg(FAKE_REQUEST_TYPE::FAKE_WRITE_REQUEST_RESP) {}
  ~ObFakeWriteRequestRespMsg() {}
  int64_t tx_id_;
  int64_t key_;
  int64_t value_;
  ObTxExecResult exec_result_;
  int ret_;
   INHERIT_TO_STRING_KV("ObFakeRequestMsg", ObFakeRequestMsg, K_(tx_id), K_(key), K_(value), K_(exec_result), K_(ret));
  OB_UNIS_VERSION_V(1);
};

} // memtable
} // oceanbase

#endif // OCEANBASE_UNITTEST_STORAGE_TX_OB_MOCK_REQUEST_MSG
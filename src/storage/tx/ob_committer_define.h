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

#ifndef OCEANBASE_STORAGE_TX_OB_TX_COMMITTER
#define OCEANBASE_STORAGE_TX_OB_TX_COMMITTER

#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

namespace oceanbase
{
namespace transaction
{
// =================== NB: REMOVE DURING JOINT DEBUGGING ===================
class ObICommitCallback
{
};
// =================== NB: REMOVE DURING JOINT DEBUGGING ===================

enum class ObTwoPhaseCommitLogType : uint8_t
{
  OB_LOG_TX_INIT = 0,
  OB_LOG_TX_COMMIT_INFO,
  OB_LOG_TX_PREPARE,
  OB_LOG_TX_PRE_COMMIT,
  OB_LOG_TX_COMMIT,
  OB_LOG_TX_ABORT,
  OB_LOG_TX_CLEAR,
  OB_LOG_TX_MAX,
};

enum class ObTwoPhaseCommitMsgType : uint8_t
{
  OB_MSG_TX_UNKNOWN = 0,
  OB_MSG_TX_PREPARE_REQ,
  OB_MSG_TX_PREPARE_RESP,
  OB_MSG_TX_PRE_COMMIT_REQ,
  OB_MSG_TX_PRE_COMMIT_RESP,
  OB_MSG_TX_COMMIT_REQ,
  OB_MSG_TX_COMMIT_RESP,
  OB_MSG_TX_ABORT_REQ,
  OB_MSG_TX_ABORT_RESP,
  OB_MSG_TX_CLEAR_REQ,
  OB_MSG_TX_CLEAR_RESP,
  OB_MSG_TX_PREPARE_REDO_REQ,
  OB_MSG_TX_PREPARE_REDO_RESP,
  OB_MSG_TX_MAX,
};

enum class Ob2PCRole : int8_t
{
  UNKNOWN = -1,
  ROOT = 0,
  INTERNAL,
  LEAF,
};

enum class ObTxState : uint8_t
{
  UNKNOWN = 0,
  INIT = 10,
  REDO_COMPLETE = 20,
  PREPARE = 30,
  PRE_COMMIT = 40,
  COMMIT = 50,
  ABORT = 60,
  CLEAR = 70,
  MAX = 100
};

const int64_t OB_C2PC_UPSTREAM_ID = INT64_MAX - 1;
const int64_t OB_C2PC_SENDER_ID = INT64_MAX - 2;

#define TRX_ENUM_CASE_TO_STR(class_name, src) \
  case class_name::src:                       \
    str = #src;                               \
    break;

static const char *to_str_2pc_role(Ob2PCRole role)
{
  const char *str = "INVALID";
  switch (role) {
    TRX_ENUM_CASE_TO_STR(Ob2PCRole, UNKNOWN)
    TRX_ENUM_CASE_TO_STR(Ob2PCRole, ROOT)
    TRX_ENUM_CASE_TO_STR(Ob2PCRole, INTERNAL)
    TRX_ENUM_CASE_TO_STR(Ob2PCRole, LEAF)
  };
  return str;
}

static const char *to_str_tx_state(ObTxState state)
{
  const char *str = "INVALID";
  switch (state) {
    TRX_ENUM_CASE_TO_STR(ObTxState, UNKNOWN)
    TRX_ENUM_CASE_TO_STR(ObTxState, INIT)
    TRX_ENUM_CASE_TO_STR(ObTxState, REDO_COMPLETE)
    TRX_ENUM_CASE_TO_STR(ObTxState, PREPARE)
    TRX_ENUM_CASE_TO_STR(ObTxState, PRE_COMMIT)
    TRX_ENUM_CASE_TO_STR(ObTxState, COMMIT)
    TRX_ENUM_CASE_TO_STR(ObTxState, ABORT)
    TRX_ENUM_CASE_TO_STR(ObTxState, CLEAR)
    TRX_ENUM_CASE_TO_STR(ObTxState, MAX)
  };
  return str;
}

/* // ObITxCommitter provides method to commit the transaction with user provided callbacks. */
/* // The interface need guarantee the atomicity of the transaction. */
/* class ObITxCommitter */
/* { */
/*   public: */
/*   // transaction commit interface, the callback will be invoked under different situation. */
/*   virtual int commit(ObICommitCallback &cb) = 0; */
/* }; */
} // transaction
} // oceanbase

#endif // OCEANBASE_STORAGE_TX_OB_TX_COMMITTER

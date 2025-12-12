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

#include "ob_local_execution_deadlock_callback.h"
#include "lib/list/ob_dlist.h"
#include "lib/utility/ob_print_utils.h"
#include "share/deadlock/ob_deadlock_detector_mgr.h"
#include "storage/memtable/hash_holder/ob_row_holder_info.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"
#include "storage/memtable/hash_holder/ob_row_hash_holder_map.h"

namespace oceanbase
{
using namespace share;
using namespace transaction;
using namespace share::detector;
namespace memtable
{

int DeadLockBlockCallBack::operator()(ObIArray<share::detector::ObDependencyHolder> &resource_array, bool &need_remove) {
  int ret = OB_SUCCESS;
  share::detector::UserBinaryKey user_key;
  RowHolderInfo row_holder_info;
  ObAddr trans_scheduler;
  share::detector::ObDependencyHolder resource;
  #define PRINT_WRAPPER KR(ret), K_(hash), K(row_holder_info), K(trans_scheduler)
  if (OB_FAIL(mapper_.get_hash_holder(hash_, row_holder_info))) {
    DETECT_LOG(WARN, "get hash holder failed", PRINT_WRAPPER);
  } else if (OB_FAIL(user_key.set_user_key(row_holder_info.tx_id_))) {
    DETECT_LOG(WARN, "set user key failed", PRINT_WRAPPER);
  } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_conflict_trans_scheduler(row_holder_info.tx_id_, trans_scheduler))) {
    DETECT_LOG(WARN, "get trans scheduler failed", PRINT_WRAPPER);
  } else if (OB_FAIL(resource.set_args(trans_scheduler, user_key))) {
    DETECT_LOG(WARN, "resource set args failed", PRINT_WRAPPER);
  } else if (OB_FAIL(resource_array.push_back(resource))) {
    DETECT_LOG(WARN, "fail to push resource to array", PRINT_WRAPPER);
  }
  #undef PRINT_WRAPPER
  need_remove = false;
  return ret;
}

LocalDeadLockCollectCallBack::LocalDeadLockCollectCallBack(const transaction::ObTransID &self_trans_id,
                                                           const char *node_key_buffer,
                                                           const SessionIDPair sess_id_pair,
                                                           const int64_t ls_id,
                                                           const uint64_t tablet_id,
                                                           const transaction::ObTxSEQ &blocked_holder_tx_hold_seq)
: self_trans_id_(self_trans_id),
  sess_id_pair_(sess_id_pair),
  ls_id_(ls_id),
  tablet_id_(tablet_id),
  blocked_holder_tx_hold_seq_(blocked_holder_tx_hold_seq) {

  int64_t str_len = strlen(node_key_buffer);// not contain '\0'
  int64_t min_len = str_len > (NODE_KEY_BUFFER_MAX_LENGTH - 1) ? (NODE_KEY_BUFFER_MAX_LENGTH - 1) : str_len;
  memcpy(node_key_buffer_, node_key_buffer, min_len);
  node_key_buffer_[min_len] = '\0';
  trace_id_ = *ObCurTraceId::get_trace_id();
}

int LocalDeadLockCollectCallBack::operator()(const ObDependencyHolder &, ObDetectorUserReportInfo &info) {
  #define PRINT_WRAPPER K_(self_trans_id), K_(node_key_buffer), K_(sess_id_pair),\
                        K_(ls_id), K_(tablet_id), K(info), K(step), K_(blocked_holder_tx_hold_seq)
  int ret = OB_SUCCESS;
  constexpr int64_t trans_id_str_len = 128;
  constexpr int64_t row_key_str_len = NODE_KEY_BUFFER_MAX_LENGTH;
  constexpr int64_t current_sql_str_len = 2_KB;
  char * buffer_trans_id = nullptr;
  char * buffer_row_key = nullptr;
  char * buffer_current_sql = nullptr;
  SessionGuard sess_guard;
  int step = 0;
  if (++step && OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_pair_, sess_guard))) {
  } else if (++step && !sess_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_UNLIKELY(nullptr == (buffer_trans_id = (char*)mtl_malloc(trans_id_str_len, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_UNLIKELY(nullptr == (buffer_row_key = (char*)mtl_malloc(row_key_str_len, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_UNLIKELY(nullptr == (buffer_current_sql = (char*)mtl_malloc(current_sql_str_len, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObCStringHelper helper;
    ObSharedGuard<char> temp_guard;
    (void) databuff_printf(buffer_trans_id, trans_id_str_len, "{session_id:%ld}(associated:%ld):%s",
                            (int64_t)sess_id_pair_.sess_id_,
                            (int64_t)sess_id_pair_.assoc_sess_id_,
                            helper.convert(self_trans_id_));
    char temp_key_buffer[NODE_KEY_BUFFER_MAX_LENGTH] = {0};
    (void) databuff_printf(temp_key_buffer,
                            NODE_KEY_BUFFER_MAX_LENGTH,
                            "{addr:%s}:{ls:%ld}:{tablet:%ld}:{row_key:%s}",
                            helper.convert(GCTX.self_addr()),
                            ls_id_,
                            tablet_id_,
                            node_key_buffer_);
    info.set_blocked_seq(blocked_holder_tx_hold_seq_);
    ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(temp_key_buffer,
                                                                      NODE_KEY_BUFFER_MAX_LENGTH,
                                                                      buffer_row_key,
                                                                      row_key_str_len);
    const ObString &cur_query_str = sess_guard->get_current_query_string();
    int64_t pos = 0;
    databuff_printf(buffer_current_sql, current_sql_str_len, pos, "%s:", helper.convert(trace_id_));
     ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(cur_query_str.ptr(),
                                                                        cur_query_str.length(),
                                                                        buffer_current_sql + pos,
                                                                        current_sql_str_len - pos);

    if (++step && OB_FAIL(temp_guard.assign((char*)"transaction", [](char*){}))) {
    } else if (++step && OB_FAIL(info.set_module_name(temp_guard))) {
    } else if (++step && OB_FAIL(temp_guard.assign(buffer_trans_id, [](char* buffer){ mtl_free(buffer); }))) {
    } else if (FALSE_IT(buffer_trans_id = nullptr)) {
    } else if (++step && OB_FAIL(info.set_visitor(temp_guard))) {
    } else if (++step && OB_FAIL(temp_guard.assign(buffer_row_key, [](char* buffer){ mtl_free(buffer);}))) {
    } else if (FALSE_IT(buffer_row_key = nullptr)) {
    } else if (++step && OB_FAIL(info.set_resource(temp_guard))) {
    } else if (++step && OB_FAIL(temp_guard.assign(buffer_current_sql, [](char* buffer){ mtl_free(buffer);}))) {
    } else if (FALSE_IT(buffer_current_sql = nullptr)) {
    } else if (++step && OB_FAIL(info.set_extra_info("wait_sql", temp_guard))) {
    }
  }
  if (OB_NOT_NULL(buffer_trans_id)) {
    mtl_free(buffer_trans_id);
  }
  if (OB_NOT_NULL(buffer_row_key)) {
    mtl_free(buffer_row_key);
  }
  if (OB_NOT_NULL(buffer_current_sql)) {
    mtl_free(buffer_current_sql);
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "fail to generate collect info", PRINT_WRAPPER);
  } else {
    DETECT_LOG(INFO, "success generate deadlock collected info", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

int LocalExecutionWaitingForRowFillVirtualInfoOperation::operator()(const bool need_fill_conflict_actions_flag,/*need fill conflict actions flag*/
                                                                    char *buffer,/*to_string buffer*/
                                                                    const int64_t buffer_len,/*to_string buffer length*/
                                                                    int64_t &pos,/*to_string current position*/
                                                                    share::detector::DetectorNodeInfoForVirtualTable &virtual_info/*virtual info to fill*/)
{
  #define PRINT_WRAPPER KR(ret), K_(hash), K(row_holder_info), K(line)
  #define CUSTOM_FAIL(stmt) FALSE_IT(line = __LINE__) || OB_FAIL(stmt)
  int ret = OB_SUCCESS;
  int line = 0;
  RowHolderInfo row_holder_info;
  ObAddr conflict_tx_scheduler;
  SessionIDPair sess_id_pair;
  constexpr int64_t buffer_size = 64;
  ObStringHolder sess_id_string;
  ObStringHolder trans_id_string;
  ObStringHolder holding_sql;
  ObStringHolder holding_sql_request_time;
  ObCStringHelper helper;
  char sess_id_buffer[buffer_size] = {0};
  char trans_id_buffer[buffer_size] = {0};
  int64_t pos_begin = 0;
  if (CUSTOM_FAIL(mapper_.get_hash_holder(hash_, row_holder_info))) {
    DETECT_LOG(WARN, "get hash holder failed", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(ObTransDeadlockDetectorAdapter::get_trans_info_on_participant(row_holder_info.tx_id_,
                                                                                       ls_id_,
                                                                                       conflict_tx_scheduler,
                                                                                       sess_id_pair))) {
    DETECT_LOG(WARN, "get trans info failed", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(databuff_printf(sess_id_buffer, buffer_size, "%ld", (int64_t)sess_id_pair.get_valid_sess_id()))) {
    DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(databuff_printf(trans_id_buffer, buffer_size, "%ld", row_holder_info.tx_id_.get_id()))) {
    DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(sess_id_string.assign(ObString(sess_id_buffer)))) {
    DETECT_LOG(WARN, "failed to construct string holder", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(trans_id_string.assign(ObString(trans_id_buffer)))) {
    DETECT_LOG(WARN, "failed to construct string holder", PRINT_WRAPPER);
  } else if (FALSE_IT(pos_begin = pos)) {
  } else if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "[{session_id:%ld}(associated:%ld):{txid:%ld}(scheduler:%s)]",
                                                                  (int64_t)sess_id_pair.sess_id_,
                                                                  (int64_t)sess_id_pair.assoc_sess_id_,
                                                                  row_holder_info.tx_id_.get_id(),
                                                                  helper.convert(conflict_tx_scheduler)))) {
    DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
  } else if (FALSE_IT(virtual_info.dynamic_block_list_.assign(buffer + pos_begin, pos - pos_begin))) {
  } else {
    if (need_fill_conflict_actions_flag) {
      if (CUSTOM_FAIL(MTL(ObDeadLockDetectorMgr*)->get_holding_sql(sess_id_string,
                                                                   trans_id_string,
                                                                   row_holder_info.seq_,
                                                                   holding_sql_request_time,
                                                                   holding_sql))) {
        DETECT_LOG(WARN, "failed to get holding sql", PRINT_WRAPPER);
      }
      if (OB_FAIL(ret)) {
        int64_t pos_begin = pos;
        int tmp_ret = ret;
        ret = OB_SUCCESS;
        if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "failed to get sql:%ld", (int64_t)tmp_ret))) {
          DETECT_LOG(WARN, "failed to print ret to buffer", PRINT_WRAPPER);
        } else if (FALSE_IT(virtual_info.conflict_actions_.assign(buffer + pos_begin, pos - pos_begin))) {
        }
      } else {
        ObCStringHelper helper;
        int64_t pos_begin = pos;
        if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "%s:%s", helper.convert(holding_sql_request_time), helper.convert(holding_sql)))) {
          DETECT_LOG(WARN, "failed to print sql to buffer", PRINT_WRAPPER);
        } else if (FALSE_IT(virtual_info.conflict_actions_.assign(buffer + pos_begin, pos - pos_begin))) {
        }
      }
    } else {
      virtual_info.conflict_actions_ = ObString("not allow to find conflict actions");
    }
  }
  if (OB_FAIL(ret)) {
    int64_t pos_begin = pos;
    databuff_printf(buffer, buffer_len, pos,
                    "failed to execute LocalExecutionWaitingForRowFillVirtualInfoOperation(), ret:%ld, line:%ld",
                    (int64_t)ret, (int64_t)line);
    virtual_info.dynamic_block_list_.assign(buffer + pos_begin, pos - pos_begin);
    DETECT_LOG(WARN, "failed to do LocalExecutionWaitingForRowFillVirtualInfoOperation", PRINT_WRAPPER);
  } else {
    DETECT_LOG(TRACE, "success to do LocalExecutionWaitingForRowFillVirtualInfoOperation", PRINT_WRAPPER);
  }
  return ret;
  #undef CUSTOM_FAIL
  #undef PRINT_WRAPPER
}

int LocalExecutionWaitingForTransFillVirtualInfoOperation::operator()(const bool need_fill_conflict_actions_flag,/*need fill conflict actions flag*/
                                                                      char *buffer,/*to_string buffer*/
                                                                      const int64_t buffer_len,/*to_string buffer length*/
                                                                      int64_t &pos,/*to_string current position*/
                                                                      share::detector::DetectorNodeInfoForVirtualTable &virtual_info/*virtual info to fill*/)
{
  #define PRINT_WRAPPER KR(ret), K(*this), K(sess_id_pair), K(holding_sql), K(holding_sql_request_time), K(line)
  #define CUSTOM_FAIL(stmt) FALSE_IT(line = __LINE__) || OB_FAIL(stmt)
  int ret = OB_SUCCESS;
  int line = 0;
  ObAddr conflict_tx_scheduler;
  SessionIDPair sess_id_pair;
  constexpr int64_t buffer_size = 64;
  ObStringHolder sess_id_string;
  ObStringHolder trans_id_string;
  ObStringHolder holding_sql;
  ObStringHolder holding_sql_request_time;
  int64_t pos_begin = 0;
  ObCStringHelper helper;
  char sess_id_buffer[buffer_size] = {0};
  char trans_id_buffer[buffer_size] = {0};
  if (CUSTOM_FAIL(ObTransDeadlockDetectorAdapter::get_trans_info_on_participant(conflict_tx_id_,
                                                                                ls_id_,
                                                                                conflict_tx_scheduler,
                                                                                sess_id_pair))) {
    DETECT_LOG(WARN, "get trans info failed", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(databuff_printf(sess_id_buffer, buffer_size, "%ld", (int64_t)sess_id_pair.get_valid_sess_id()))) {
    DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(databuff_printf(trans_id_buffer, buffer_size, "%ld", conflict_tx_id_.get_id()))) {
    DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(sess_id_string.assign(ObString(sess_id_buffer)))) {
    DETECT_LOG(WARN, "failed to construct string holder", PRINT_WRAPPER);
  } else if (CUSTOM_FAIL(trans_id_string.assign(ObString(trans_id_buffer)))) {
    DETECT_LOG(WARN, "failed to construct string holder", PRINT_WRAPPER);
  } else if (FALSE_IT(pos_begin = pos)) {
  } else if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "[{session_id:%ld}(associated:%ld):{txid:%ld}(scheduler:%s)]",
                                                                  (int64_t)sess_id_pair.sess_id_,
                                                                  (int64_t)sess_id_pair.assoc_sess_id_,
                                                                  conflict_tx_id_.get_id(),
                                                                  helper.convert(conflict_tx_scheduler)))) {
    DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
  } else if (FALSE_IT(virtual_info.static_block_list_.assign(buffer + pos_begin, pos - pos_begin))) {
  } else {
    if (need_fill_conflict_actions_flag) {
      if (CUSTOM_FAIL(MTL(ObDeadLockDetectorMgr*)->get_holding_sql(sess_id_string,
                                                                   trans_id_string,
                                                                   conflict_tx_seq_,
                                                                   holding_sql_request_time,
                                                                   holding_sql))) {
        DETECT_LOG(WARN, "failed to get holding sql", PRINT_WRAPPER);
      }
      if (OB_FAIL(ret)) {
        pos_begin = pos;
        int tmp_ret = ret;
        ret = OB_SUCCESS;
        if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "failed to get sql:%ld", (int64_t)tmp_ret))) {
          DETECT_LOG(WARN, "failed to print ret to buffer", PRINT_WRAPPER);
        } else if (FALSE_IT(virtual_info.conflict_actions_.assign(buffer + pos_begin, pos - pos_begin))) {
        }
      } else {
        ObCStringHelper helper;
        int64_t pos_begin = pos;
        if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "%s:%s",
                                        helper.convert(holding_sql_request_time),
                                        helper.convert(holding_sql)))) {
          DETECT_LOG(WARN, "failed to print sql to buffer", PRINT_WRAPPER);
        } else if (FALSE_IT(virtual_info.conflict_actions_.assign(buffer + pos_begin, pos - pos_begin))) {
        }
      }
    } else {
      virtual_info.conflict_actions_ = ObString("not allow to find conflict actions");
    }
  }
  if (OB_FAIL(ret)) {
    int64_t pos_begin = pos;
    databuff_printf(buffer, buffer_len, pos,
                    "failed to execute LocalExecutionWaitingForTransFillVirtualInfoOperation(), ret:%ld, line:%ld",
                    (int64_t)ret, (int64_t)line);
    virtual_info.static_block_list_.assign(buffer + pos_begin, pos - pos_begin);
    DETECT_LOG(WARN, "failed to do LocalExecutionWaitingForTransFillVirtualInfoOperation", PRINT_WRAPPER);
  } else {
    DETECT_LOG(TRACE, "success to do LocalExecutionWaitingForTransFillVirtualInfoOperation", PRINT_WRAPPER);
  }
  return ret;
  #undef CUSTOM_FAIL
  #undef PRINT_WRAPPER
}

RemoteExecutionSideNodeDeadLockCollectCallBack::RemoteExecutionSideNodeDeadLockCollectCallBack(const transaction::ObTransID &self_trans_id,
                                                           const char *node_key_buffer,
                                                           const ObString &query_sql,
                                                           const SessionIDPair sess_id_pair,
                                                           const int64_t ls_id,
                                                           const uint64_t tablet_id,
                                                           const transaction::ObTxSEQ &blocked_holder_tx_hold_seq)
: self_trans_id_(self_trans_id),
  sess_id_pair_(sess_id_pair),
  ls_id_(ls_id),
  tablet_id_(tablet_id),
  blocked_holder_tx_hold_seq_(blocked_holder_tx_hold_seq) {

  int64_t key_str_len = strlen(node_key_buffer);// not contain '\0'
  int64_t key_min_len = key_str_len > (NODE_KEY_BUFFER_MAX_LENGTH - 1) ? (NODE_KEY_BUFFER_MAX_LENGTH - 1) : key_str_len;
  memcpy(node_key_buffer_, node_key_buffer, key_min_len);
  node_key_buffer_[key_min_len] = '\0';

  int64_t query_sql_str_len = query_sql.length();// not contain '\0'
  const char* query_sql_buffer = query_sql.ptr();
  int64_t query_sql_min_len = query_sql_str_len > (QUERY_SQL_BUFFER_MAX_LENGTH - 1) ? (QUERY_SQL_BUFFER_MAX_LENGTH - 1) : query_sql_str_len;
  memcpy(query_sql_buffer_, query_sql_buffer, query_sql_min_len);
  query_sql_buffer_[query_sql_min_len] = '\0';
  trace_id_ = *ObCurTraceId::get_trace_id();
}

int RemoteExecutionSideNodeDeadLockCollectCallBack::operator()(const ObDependencyHolder &, ObDetectorUserReportInfo &info) {
  #define PRINT_WRAPPER K_(self_trans_id), K_(node_key_buffer), K_(sess_id_pair),\
                        K_(ls_id), K_(tablet_id), K(info), K(step), K_(blocked_holder_tx_hold_seq), K(ObString(query_sql_buffer_))
  int ret = OB_SUCCESS;
  constexpr int64_t trans_id_str_len = 128;
  constexpr int64_t row_key_str_len = NODE_KEY_BUFFER_MAX_LENGTH;
  constexpr int64_t current_sql_str_len = 2_KB;
  char * buffer_trans_id = nullptr;
  char * buffer_row_key = nullptr;
  char * buffer_current_sql = nullptr;
  int step = 0;
   if (OB_UNLIKELY(nullptr == (buffer_trans_id = (char*)mtl_malloc(trans_id_str_len, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_UNLIKELY(nullptr == (buffer_row_key = (char*)mtl_malloc(row_key_str_len, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_UNLIKELY(nullptr == (buffer_current_sql = (char*)mtl_malloc(current_sql_str_len, "deadlockCB")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    ObCStringHelper helper;
    ObSharedGuard<char> temp_guard;
    (void) databuff_printf(buffer_trans_id, trans_id_str_len, "{session_id:%ld}(associated:%ld):%s",
                            (int64_t)sess_id_pair_.sess_id_,
                            (int64_t)sess_id_pair_.assoc_sess_id_,
                            helper.convert(self_trans_id_));
    char temp_key_buffer[NODE_KEY_BUFFER_MAX_LENGTH] = {0};
    (void) databuff_printf(temp_key_buffer,
                            NODE_KEY_BUFFER_MAX_LENGTH,
                            "{addr:%s}:{ls:%ld}:{tablet:%ld}:{row_key:%s}",
                            helper.convert(GCTX.self_addr()),
                            ls_id_,
                            tablet_id_,
                            node_key_buffer_);
    info.set_blocked_seq(blocked_holder_tx_hold_seq_);
    ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(temp_key_buffer,
                                                                      NODE_KEY_BUFFER_MAX_LENGTH,
                                                                      buffer_row_key,
                                                                      row_key_str_len);
    int64_t pos = 0;
    databuff_printf(buffer_current_sql, current_sql_str_len, pos, "%s:", helper.convert(trace_id_));
    ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(query_sql_buffer_,
                                                                      QUERY_SQL_BUFFER_MAX_LENGTH,
                                                                      buffer_current_sql + pos,
                                                                      current_sql_str_len - pos);

    if (++step && OB_FAIL(temp_guard.assign((char*)"transaction", [](char*){}))) {
    } else if (++step && OB_FAIL(info.set_module_name(temp_guard))) {
    } else if (++step && OB_FAIL(temp_guard.assign(buffer_trans_id, [](char* buffer){ mtl_free(buffer); }))) {
    } else if (FALSE_IT(buffer_trans_id = nullptr)) {
    } else if (++step && OB_FAIL(info.set_visitor(temp_guard))) {
    } else if (++step && OB_FAIL(temp_guard.assign(buffer_row_key, [](char* buffer){ mtl_free(buffer);}))) {
    } else if (FALSE_IT(buffer_row_key = nullptr)) {
    } else if (++step && OB_FAIL(info.set_resource(temp_guard))) {
    } else if (++step && OB_FAIL(temp_guard.assign(buffer_current_sql, [](char* buffer){ mtl_free(buffer);}))) {
    } else if (FALSE_IT(buffer_current_sql = nullptr)) {
    } else if (++step && OB_FAIL(info.set_extra_info("wait_sql", temp_guard))) {
    }
  }
  if (OB_NOT_NULL(buffer_trans_id)) {
    mtl_free(buffer_trans_id);
  }
  if (OB_NOT_NULL(buffer_row_key)) {
    mtl_free(buffer_row_key);
  }
  if (OB_NOT_NULL(buffer_current_sql)) {
    mtl_free(buffer_current_sql);
  }
  if (OB_FAIL(ret)) {
    DETECT_LOG(WARN, "fail to generate collect info", PRINT_WRAPPER);
  } else {
    DETECT_LOG(INFO, "success generate deadlock collected info", PRINT_WRAPPER);
  }
  return ret;
  #undef PRINT_WRAPPER
}

}
}
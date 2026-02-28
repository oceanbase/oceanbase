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

#include "ob_remote_execution_deadlock_callback.h"
#include "storage/tx/ob_trans_deadlock_adapter.h"

namespace oceanbase
{
using namespace share;
using namespace share::detector;
namespace transaction
{

int ObTransOnDetectOperation::operator()(const common::ObIArray<share::detector::ObDetectorInnerReportInfo> &info,
                                         const int64_t self_idx) {
  UNUSED(info);
  UNUSED(self_idx);
  SessionGuard session_guard;
  int ret = OB_SUCCESS;
  int step = 0;

  if (++step && OB_UNLIKELY(sess_id_pair_.get_valid_sess_id() == 0 || !trans_id_.is_valid())) {
    ret = OB_NOT_INIT;
  } else if (++step && OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_pair_, session_guard))) {
  } else if (++step && !session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (++step && ObCompatibilityMode::MYSQL_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_tx(sess_id_pair_);
  } else if (++step && ObCompatibilityMode::ORACLE_MODE == session_guard->get_compatibility_mode()) {
    ret = ObTransDeadlockDetectorAdapter::kill_stmt(sess_id_pair_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(ERROR, "unknown mode", KR(ret), K(step), K(session_guard.get_session()), K(*this));
  }

  if (!OB_SUCC(ret)) {
    DETECT_LOG(WARN, "execute on detect op failed", KR(ret), K(step), K(*this));
  }

  return ret;
}

int RemoteDeadLockCollectCallBack::operator()(const ObDependencyHolder &blocked_hodler,
                                              ObDetectorUserReportInfo &info) {
  #define PRINT_WRAPPER KR(ret), K(blocked_hodler), K(info), K(*this), K(step), K(self_trans_id), K(*this)
  int ret = OB_SUCCESS;
  int step = 0;
  SessionGuard session_guard;
  ObTransID self_trans_id;
  ObRowConflictInfo *p_conflcit_info = nullptr;
  for (int64_t idx = 0; idx < row_conflict_info_array_.count() && OB_SUCC(ret); ++idx) {
    ObRowConflictInfo &conflcit_info = row_conflict_info_array_.at(idx);
    ObDependencyHolder temp_holder;
    UserBinaryKey holder_key;
    if (OB_FAIL(holder_key.set_user_key(conflcit_info.conflict_tx_id_))) {
      DETECT_LOG(WARN, "fail to generate user key", PRINT_WRAPPER);
    } else if (OB_FAIL(temp_holder.set_args(conflcit_info.conflict_tx_scheduler_, holder_key))) {
      DETECT_LOG(WARN, "fail to generate holder", PRINT_WRAPPER);
    } else if (temp_holder == blocked_hodler) {
      p_conflcit_info = &conflcit_info;
      info.set_blocked_seq(p_conflcit_info->conflict_tx_hold_seq_);
      break;
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(ObTransDeadlockDetectorAdapter::get_session_info(sess_id_pair_, session_guard))) {
    DETECT_LOG(WARN, "got session info is NULL", PRINT_WRAPPER);
  } else if (!session_guard.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(session_guard->get_tx_desc())) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "desc on session is not valid", PRINT_WRAPPER);
  } else if (!(session_guard->get_tx_desc()->get_tx_id().is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    DETECT_LOG(WARN, "trans id on desc on session is not valid", PRINT_WRAPPER);
  } else {
    ObSharedGuard<char> temp_guard;
    const ObString &cur_query_str = session_guard->get_current_query_string();
    constexpr int64_t trans_id_str_len = 128;
    constexpr int64_t trace_id_str_len = 128;
    int64_t current_sql_str_len = std::min(static_cast<int64_t>(cur_query_str.length()) + 256/*including translate_apostrophe'\' and TRACEID*/, static_cast<int64_t>(2_KB));
    char * buffer_visitor = nullptr;
    char * buffer_current_sql = nullptr;
    if (OB_UNLIKELY(nullptr == (buffer_visitor = (char*)mtl_malloc(trans_id_str_len, "deadlockCB")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc memory failed", PRINT_WRAPPER);
    } else if (cur_query_str.empty()) {
      DETECT_LOG(WARN, "cur_query_str on session is empty", K(cur_query_str), K(session_guard.get_session()));
    } else if (OB_UNLIKELY(nullptr == (buffer_current_sql = (char*)mtl_malloc(current_sql_str_len, "deadlockCB")))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "alloc memory failed",PRINT_WRAPPER);
    }
    if (OB_SUCC(ret)) {
      ObCStringHelper helper;
      int64_t pos = 0;
      databuff_printf(buffer_current_sql, current_sql_str_len, pos, "%s:", helper.convert(trace_id_));
      ObTransDeadlockDetectorAdapter::copy_str_and_translate_apostrophe(cur_query_str.ptr(),
                                                                        cur_query_str.length(),
                                                                        buffer_current_sql + pos,
                                                                        current_sql_str_len - pos);
      (void) databuff_printf(buffer_visitor, trans_id_str_len, "{session_id:%ld}(associated:%ld):%s",
                            (int64_t)sess_id_pair_.sess_id_,
                            (int64_t)sess_id_pair_.assoc_sess_id_,
                            helper.convert(session_guard->get_tx_desc()->get_tx_id()));
      if (++step && OB_FAIL(temp_guard.assign((char*)"transaction", DoNothingDeleter()))) {
      } else if (++step && OB_FAIL(info.set_module_name(temp_guard))) {
      } else if (++step && OB_FAIL(generate_resource_info_(p_conflcit_info, info))) {
      } else if (++step && OB_FAIL(temp_guard.assign(buffer_visitor, MtlDeleter()))) {
      } else if (FALSE_IT(buffer_visitor = nullptr)) {
      } else if (++step && OB_FAIL(info.set_visitor(temp_guard))) {
      } else if (++step && OB_FAIL(temp_guard.assign(buffer_current_sql, MtlDeleter()))) {
      } else if (FALSE_IT(buffer_current_sql = nullptr)) {
      } else if (++step && OB_FAIL(info.set_extra_info("wait_sql", temp_guard))) {
      }
    }
    if (OB_FAIL(ret)) {
      DETECT_LOG(WARN, "get string failed in deadlock", PRINT_WRAPPER);
    }
    if (OB_NOT_NULL(buffer_visitor)) {
      mtl_free(buffer_visitor);
    }
    if (OB_NOT_NULL(buffer_current_sql)) {
      mtl_free(buffer_current_sql);
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int RemoteDeadLockCollectCallBack::generate_resource_info_(ObRowConflictInfo *conflict_info,
                                                           ObDetectorUserReportInfo &info) {
  int ret = OB_SUCCESS;
  ObSharedGuard<char> temp_guard;
  if (OB_ISNULL(conflict_info)) {
    if (OB_FAIL(temp_guard.assign((char*)"remote row", DoNothingDeleter()))) {
      DETECT_LOG(WARN, "fail to construct guard", KR(ret), K(info));
    } else if (OB_FAIL(info.set_resource(temp_guard))) {
      DETECT_LOG(WARN, "fail to set resource", KR(ret), K(info));
    }
  } else {
    char * buffer = nullptr;
    constexpr int64_t MAX_LENGTH = 1_KB;
    int64_t pos = 0;
    ObCStringHelper helper;
    if (OB_ISNULL(buffer = (char *)mtl_malloc(MAX_LENGTH, "DLResource"))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      DETECT_LOG(WARN, "fail to alloc memory", KR(ret), KPC(conflict_info), K(info));
    } else if (FALSE_IT(databuff_printf(buffer, MAX_LENGTH, pos,
                                        "{addr:%s}:{ls:%ld}:{tablet:%ld}:{row_key:%s}",
                                        helper.convert(conflict_info->conflict_happened_addr_),
                                        conflict_info->conflict_ls_.id(),
                                        conflict_info->conflict_tablet_.id(),
                                        helper.convert(conflict_info->conflict_row_key_str_)))) {
      DETECT_LOG(WARN, "fail to convert str", KR(ret), KPC(conflict_info), K(info));
    } else if (OB_FAIL(temp_guard.assign(buffer, MtlDeleter()))) {
      DETECT_LOG(WARN, "fail to construct guard", KR(ret), KPC(conflict_info), K(info));
    } else if (FALSE_IT(buffer = nullptr)) {
    } else if (OB_FAIL(info.set_resource(temp_guard))) {
      DETECT_LOG(WARN, "fail to set resource", KR(ret), KPC(conflict_info), K(info));
    }
    if (OB_NOT_NULL(buffer)) {
      mtl_free(buffer);
    }
  }
  return ret;
}

int ObTransDeadLockRemoteExecutionFillVirtualInfoOperation::operator()(const bool need_fill_conflict_actions_flag,
                                                                       char *buffer,
                                                                       const int64_t buffer_len,
                                                                       int64_t &pos,
                                                                       share::detector::DetectorNodeInfoForVirtualTable &virtual_info)
{
  #define PRINT_WRAPPER KR(ret), KP(buffer), K(buffer_len), K(pos), K(virtual_info), K(*this)
  #define CUSTOM_FAIL(stmt) FALSE_IT(line = __LINE__) || OB_FAIL(stmt)
  int ret = OB_SUCCESS;
  int line = 0;
  using ConflictSqlInfo = ObTuple<ObStringHolder/*request_time*/, ObStringHolder/*request_sql*/>;
  ObArray<ConflictSqlInfo> conflict_sqls;
  for (int64_t idx = 0; idx < conflict_info_array_.count() && OB_SUCC(ret); ++idx) {
    ObRowConflictInfo &conflict_info = conflict_info_array_[idx];
    ObStringHolder sess_id_string;
    ObStringHolder trans_id_string;
    constexpr int64_t buffer_size = 64;
    char sess_id_buffer[buffer_size] = {0};
    char trans_id_buffer[buffer_size] = {0};
    ConflictSqlInfo *p_current_conflict_sql_info = nullptr;
    if (CUSTOM_FAIL(conflict_sqls.push_back(ConflictSqlInfo()))) {
      DETECT_LOG(WARN, "failed to push back", PRINT_WRAPPER);
    } else if (FALSE_IT(p_current_conflict_sql_info = &conflict_sqls[conflict_sqls.count() - 1])) {
    } else if (CUSTOM_FAIL(databuff_printf(sess_id_buffer, buffer_size, "%ld", (int64_t)conflict_info.conflict_sess_id_pair_.get_valid_sess_id()))) {
      DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
    } else if (CUSTOM_FAIL(databuff_printf(trans_id_buffer, buffer_size, "%ld", conflict_info.conflict_tx_id_.get_id()))) {
      DETECT_LOG(WARN, "failed to_string", PRINT_WRAPPER);
    } else if (CUSTOM_FAIL(sess_id_string.assign(ObString(sess_id_buffer)))) {
      DETECT_LOG(WARN, "failed to construct string holder", PRINT_WRAPPER);
    } else if (CUSTOM_FAIL(trans_id_string.assign(ObString(trans_id_buffer)))) {
      DETECT_LOG(WARN, "failed to construct string holder", PRINT_WRAPPER);
    } else {
      if (need_fill_conflict_actions_flag) {
        if (CUSTOM_FAIL(MTL(ObDeadLockDetectorMgr*)->get_holding_sql(sess_id_string,
                                                                     trans_id_string,
                                                                     conflict_info.conflict_tx_hold_seq_,
                                                                     p_current_conflict_sql_info->element<0>(),
                                                                     p_current_conflict_sql_info->element<1>()))) {
          DETECT_LOG(WARN, "failed to get holding sql", PRINT_WRAPPER);
          int tmp_ret = ret;
          ret = OB_SUCCESS;
          char err_code[buffer_size];
          if (CUSTOM_FAIL(databuff_printf(err_code, buffer_size, "%ld", (int64_t)tmp_ret))) {
            DETECT_LOG(WARN, "failed to print error code", PRINT_WRAPPER);
          } else if (CUSTOM_FAIL(p_current_conflict_sql_info->element<0>().assign("failed to get sql"))) {
            DETECT_LOG(WARN, "failed to generate holder", PRINT_WRAPPER);
          } else if (CUSTOM_FAIL(p_current_conflict_sql_info->element<1>().assign(err_code))) {
            DETECT_LOG(WARN, "failed to generate holder", PRINT_WRAPPER);
          }
        } else {
          DETECT_LOG(TRACE, "success to get holding sql", PRINT_WRAPPER);
        }
      }
    }
  }
  if (OB_SUCC(ret)) {// rewrite static block list
    int64_t begin_pos = pos;
    if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "["))) {
      DETECT_LOG(WARN, "failed to print first char", PRINT_WRAPPER, K(conflict_sqls));
    } else {
      ObCStringHelper helper;
      for (int64_t idx = 0; idx < conflict_info_array_.count() && OB_SUCC(ret); ++idx) {
        if (idx != conflict_info_array_.count() - 1) {
          if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "{session_id:%ld}(associated:%ld):{txid:%ld}(scheduler:%s), ",
                                          (int64_t)conflict_info_array_[idx].conflict_sess_id_pair_.sess_id_,
                                          (int64_t)conflict_info_array_[idx].conflict_sess_id_pair_.assoc_sess_id_,
                                          conflict_info_array_[idx].conflict_tx_id_.get_id(),
                                          helper.convert(conflict_info_array_[idx].conflict_tx_scheduler_)))) {
            DETECT_LOG(WARN, "failed to print sql", PRINT_WRAPPER, K(conflict_sqls));
          }
        } else {
          if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "{session_id:%ld}(associated:%ld):{txid:%ld}(scheduler:%s)]",
                                          (int64_t)conflict_info_array_[idx].conflict_sess_id_pair_.sess_id_,
                                          (int64_t)conflict_info_array_[idx].conflict_sess_id_pair_.assoc_sess_id_,
                                          conflict_info_array_[idx].conflict_tx_id_.get_id(),
                                          helper.convert(conflict_info_array_[idx].conflict_tx_scheduler_)))) {
            DETECT_LOG(WARN, "failed to print sql", PRINT_WRAPPER, K(conflict_sqls));
          }
        }
      }
      if (OB_SUCC(ret)) {
        virtual_info.static_block_list_.assign(buffer + begin_pos, pos - begin_pos);
      }
    }
  }
  if (OB_SUCC(ret)) {// rewrite conflict actions
    if (need_fill_conflict_actions_flag) {
      int64_t begin_pos = pos;
      ObCStringHelper helper;
      for (int64_t idx = 0; idx < conflict_sqls.count() && OB_SUCC(ret); ++idx) {
        if (idx != conflict_sqls.count() - 1) {
          if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "%s:%s\n",
                                          helper.convert(conflict_sqls[idx].element<0>()),
                                          helper.convert(conflict_sqls[idx].element<1>())))) {
            DETECT_LOG(WARN, "failed to print sql", PRINT_WRAPPER, K(conflict_sqls));
          }
        } else {
          if (CUSTOM_FAIL(databuff_printf(buffer, buffer_len, pos, "%s:%s",
                                          helper.convert(conflict_sqls[idx].element<0>()),
                                          helper.convert(conflict_sqls[idx].element<1>())))) {
            DETECT_LOG(WARN, "failed to print sql", PRINT_WRAPPER, K(conflict_sqls));
          }
        }
      }
      if (OB_SUCC(ret)) {
        virtual_info.conflict_actions_ = ObString(pos - begin_pos, buffer + begin_pos);
      }
    } else {
      virtual_info.conflict_actions_ = ObString("not allow to find conflict actions");
    }
  }
  if (OB_FAIL(ret)) {
    int64_t pos_begin = pos;
    databuff_printf(buffer, buffer_len, pos,
                    "failed to execute ObTransDeadLockRemoteExecutionFillVirtualInfoOperation(), ret:%ld, line:%ld",
                    (int64_t)ret, (int64_t)line);
    virtual_info.static_block_list_.assign(buffer + pos_begin, pos - pos_begin);
  }
  return ret;
  #undef CUSTOM_FAIL
  #undef PRINT_WRAPPER
}

}
}
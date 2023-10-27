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

#include "ob_all_virtual_ha_diagnose.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace observer
{
int ObAllVirtualHADiagnose::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    auto func_iter_ls = [&](const storage::ObLS &ls) -> int
    {
      int ret = OB_SUCCESS;
      storage::DiagnoseInfo diagnose_info;
      if (OB_FAIL(ls.diagnose(diagnose_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          SERVER_LOG(WARN, "ls may have been removed, just skip", K(ls));
          ret = OB_SUCCESS;
        } else if (OB_NOT_RUNNING == ret) {
          SERVER_LOG(WARN, "ls may be during rebalancing ", K(ls));
          ret = OB_SUCCESS;
        } else {
          SERVER_LOG(WARN, "ls stat diagnose info failed", K(ret), K(ls));
        }
      } else if (OB_FAIL(insert_stat_(diagnose_info))) {
        SERVER_LOG(WARN, "insert stat failed", K(ret), K(diagnose_info));
      } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
        SERVER_LOG(WARN, "iter diagnose info faild", KR(ret), K(diagnose_info));
      } else {
        SERVER_LOG(INFO, "iter diagnose info succ", K(diagnose_info));
      }
      return ret;
    };
    auto func_iterate_tenant = [&func_iter_ls]() -> int
    {
      int ret = OB_SUCCESS;
      storage::ObLSService *ls_service = MTL(storage::ObLSService*);
      if (NULL == ls_service) {
        SERVER_LOG(INFO, "tenant has no ObLSService", K(MTL_ID()));
      } else if (OB_FAIL(ls_service->iterate_diagnose(func_iter_ls))) {
        SERVER_LOG(WARN, "iter ls failed", K(ret));
      } else {
        SERVER_LOG(INFO, "iter ls succ", K(ret));
      }
      return ret;
    };
    if (NULL == omt_) {
      SERVER_LOG(INFO, "omt is NULL", K(MTL_ID()));
    } else if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
      SERVER_LOG(WARN, "iter tenant failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "get next row failed", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualHADiagnose::insert_stat_(storage::DiagnoseInfo &diagnose_info)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case TENANT_ID:
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      case LS_ID:
        cur_row_.cells_[i].set_int(diagnose_info.ls_id_);
        break;
      case SVR_IP:
        if (false == GCTX.self_addr().ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        }
        break;
      case SVR_PORT:
        cur_row_.cells_[i].set_int(GCTX.self_addr().get_port());
        break;
      case ELECTION_ROLE:
        if (OB_FAIL(role_to_string(diagnose_info.palf_diagnose_info_.election_role_,
                                   election_role_str_, sizeof(election_role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(election_role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case ELECTION_EPOCH:
        cur_row_.cells_[i].set_int(diagnose_info.palf_diagnose_info_.election_epoch_);
        break;
      case PALF_ROLE:
        if (OB_FAIL(role_to_string(diagnose_info.palf_diagnose_info_.palf_role_,
                                   palf_role_str_, sizeof(palf_role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(palf_role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case PALF_STATE:
        cur_row_.cells_[i].set_varchar(ObString::make_string(replica_state_to_string(diagnose_info.palf_diagnose_info_.palf_state_)));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      case PALF_PROPOSAL_ID:
        cur_row_.cells_[i].set_int(diagnose_info.palf_diagnose_info_.palf_proposal_id_);
        break;
      case LOG_HANDLER_ROLE:
        if (OB_FAIL(role_to_string(diagnose_info.log_handler_diagnose_info_.log_handler_role_,
                                   log_handler_role_str_, sizeof(log_handler_role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(log_handler_role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case LOG_HANDLER_PROPOSAL_ID:
        cur_row_.cells_[i].set_int(diagnose_info.log_handler_diagnose_info_.log_handler_proposal_id_);
        break;
      case LOG_HANDLER_TAKEOVER_STATE:
        if (OB_FAIL(takeover_state_to_string(diagnose_info.rc_diagnose_info_.state_,
                                             log_handler_takeover_state_str_,
                                             sizeof(log_handler_takeover_state_str_)))) {
          SERVER_LOG(WARN, "takeover_state_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(log_handler_takeover_state_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case LOG_HANDLER_TAKEOVER_LOG_TYPE:
        if (OB_FAIL(log_base_type_to_string(diagnose_info.rc_diagnose_info_.log_type_,
                                            log_handler_takeover_log_type_str_,
                                            sizeof(log_handler_takeover_log_type_str_)))) {
          SERVER_LOG(WARN, "log_base_type_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(log_handler_takeover_log_type_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case MAX_APPLIED_SCN:
        cur_row_.cells_[i].set_uint64(diagnose_info.apply_diagnose_info_.max_applied_scn_.get_val_for_inner_table_field());
        break;
      case MAX_REPALYED_LSN:
        cur_row_.cells_[i].set_uint64(diagnose_info.replay_diagnose_info_.max_replayed_lsn_.val_);
        break;
      case MAX_REPLAYED_SCN:
        cur_row_.cells_[i].set_uint64(diagnose_info.replay_diagnose_info_.max_replayed_scn_.get_val_for_inner_table_field());
        break;
      case REPLAY_DIAGNOSE_INFO:
        cur_row_.cells_[i].set_varchar((diagnose_info.replay_diagnose_info_.diagnose_str_.string()));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      case GC_STATE:
        if (OB_FAIL(gc_state_to_string(diagnose_info.gc_diagnose_info_.gc_state_,
                                       gc_state_str_,
                                       sizeof(gc_state_str_)))) {
          SERVER_LOG(WARN, "gc_state_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(gc_state_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case GC_START_TS:
        cur_row_.cells_[i].set_int(diagnose_info.gc_diagnose_info_.gc_start_ts_);
        break;
      //TODO: @keqing.llt archive_scn列目前只占位
      case ARCHIVE_SCN:
        cur_row_.cells_[i].set_uint64(0);
        break;
      case CHECKPOINT_SCN:
        cur_row_.cells_[i].set_uint64(diagnose_info.checkpoint_diagnose_info_.checkpoint_.get_val_for_inner_table_field());
        break;
      case MIN_REC_SCN:
        cur_row_.cells_[i].set_uint64(diagnose_info.checkpoint_diagnose_info_.min_rec_scn_.get_val_for_inner_table_field());
        break;
      case MIN_REC_SCN_LOG_TYPE:
        if (OB_FAIL(log_base_type_to_string(diagnose_info.checkpoint_diagnose_info_.log_type_,
                                            min_rec_log_scn_log_type_str_,
                                            sizeof(min_rec_log_scn_log_type_str_)))) {
          SERVER_LOG(WARN, "log_base_type_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(min_rec_log_scn_log_type_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case RESTORE_HANDLER_ROLE:
        if (OB_FAIL(role_to_string(diagnose_info.restore_diagnose_info_.restore_role_,
                                   restore_handler_role_str_, sizeof(restore_handler_role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K(diagnose_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(restore_handler_role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      case RESTORE_HANDLER_PROPOSAL_ID:
        cur_row_.cells_[i].set_int(diagnose_info.restore_diagnose_info_.restore_proposal_id_);
        break;
      case RESTORE_CONTEXT_INFO:
        cur_row_.cells_[i].set_varchar((diagnose_info.restore_diagnose_info_.restore_context_info_.string()));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      case RESTORE_ERR_CONTEXT_INFO:
        cur_row_.cells_[i].set_varchar((diagnose_info.restore_diagnose_info_.restore_err_context_info_.string()));
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      case ENABLE_SYNC:
        cur_row_.cells_[i].set_bool(diagnose_info.palf_diagnose_info_.enable_sync_);
        break;
      case ENABLE_VOTE:
        cur_row_.cells_[i].set_bool(diagnose_info.palf_diagnose_info_.enable_vote_);
        break;
      case ARB_SRV_INFO:
        cur_row_.cells_[i].set_varchar(ObString(""));
#ifdef OB_BUILD_ARBITRATION
        cur_row_.cells_[i].set_varchar(diagnose_info.arb_srv_diagnose_info_.diagnose_str_.string());
#endif
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unkown column");
        break;
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

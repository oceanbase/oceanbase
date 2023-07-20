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

#include "ob_all_virtual_log_stat.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "logservice/ob_log_service.h"
#include "logservice/ob_log_handler.h"
#include "share/ls/ob_ls_info.h" //MemberList, SimpleMember
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "common/ob_member.h" //ObMember

namespace oceanbase
{
namespace observer
{
ObAllVirtualPalfStat::ObAllVirtualPalfStat(omt::ObMultiTenant *omt) : omt_(omt)
{
}

ObAllVirtualPalfStat::~ObAllVirtualPalfStat()
{
  destroy();
}

void ObAllVirtualPalfStat::destroy()
{
}

int ObAllVirtualPalfStat::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
    auto func_iterate_palf = [&](const palf::PalfHandle &palf_handle) -> int {
      int ret = OB_SUCCESS;
      logservice::ObLogStat log_stat;
      int64_t palf_id = -1;
      palf_handle.get_palf_id(palf_id);
      if (OB_FAIL(palf_handle.stat(log_stat.palf_stat_))) {
        SERVER_LOG(WARN, "PalfHandle stat failed", K(ret), K(palf_id));
      } else if (OB_FAIL(insert_log_stat_(log_stat, &cur_row_))){
        SERVER_LOG(WARN, "ObAllVirtualPalfStat insert_log_stat_ failed", K(ret), K(palf_id), K(log_stat));
      } else {
        SERVER_LOG(TRACE, "iterate this log_stream success", K(palf_id), K(log_stat));
        scanner_.add_row(cur_row_);
      }
      return ret;
    };
    auto func_iterate_tenant = [&func_iterate_palf]() -> int
    {
      int ret = OB_SUCCESS;
      logservice::ObLogService *log_service = MTL(logservice::ObLogService*);
      if (NULL == log_service) {
        SERVER_LOG(INFO, "tenant has no ObLogService", K(MTL_ID()));
      } else if (OB_FAIL(log_service->iterate_palf(func_iterate_palf))) {
        SERVER_LOG(WARN, "ObLogService iterate_palf failed", K(ret));
      } else {
        SERVER_LOG(TRACE, "itearte this tenant success", K(MTL_ID()));
      }
      return ret;
    };
    if (OB_FAIL(omt_->operate_each_tenant_for_sys_or_self(func_iterate_tenant))) {
      SERVER_LOG(WARN, "ObMultiTenant operate_each_tenant_for_sys_or_self failed", K(ret));
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
  }
  if (OB_SUCC(ret) && true == start_to_read_) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "failed to get_next_row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObAllVirtualPalfStat::insert_log_stat_(const logservice::ObLogStat &log_stat, common::ObNewRow *row)
{
  int ret = OB_SUCCESS;
  const palf::PalfStat &palf_stat = log_stat.palf_stat_;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {
        cur_row_.cells_[i].set_int(palf_stat.palf_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {
        if (false == palf_stat.self_.ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret), K(palf_stat));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {
        cur_row_.cells_[i].set_int(palf_stat.self_.get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {
        if (OB_FAIL(role_to_string(palf_stat.role_, role_str_, sizeof(role_str_)))) {
          SERVER_LOG(WARN, "role_to_string failed", K(ret), K(palf_stat));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(role_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {
        cur_row_.cells_[i].set_int(palf_stat.log_proposal_id_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {
        if (0 >= palf_stat.config_version_.to_string(config_version_buf_, VARCHAR_128)) {
          SERVER_LOG(WARN, "config_version_ to_string failed", K(ret), K(palf_stat));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(config_version_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {
        if (OB_FAIL(palf::access_mode_to_string(palf_stat.access_mode_, access_mode_str_, sizeof(access_mode_str_)))) {
          SERVER_LOG(WARN, "access_mode_to_string failed", K(ret), K(palf_stat));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(access_mode_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {
        if (OB_FAIL(member_list_to_string_(palf_stat.paxos_member_list_))) {
          SERVER_LOG(WARN, "memberlist to_string failed", K(ret), K(palf_stat));
        } else {
          cur_row_.cells_[i].set_varchar(member_list_buf_.string());
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {
        cur_row_.cells_[i].set_int(palf_stat.paxos_replica_num_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 10: {
        const bool is_in_sync = log_stat.palf_stat_.is_in_sync_;
        cur_row_.cells_[i].set_bool(is_in_sync);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 11: {
        cur_row_.cells_[i].set_uint64(palf_stat.base_lsn_.val_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 12: {
        cur_row_.cells_[i].set_uint64(palf_stat.begin_lsn_.val_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 13: {
        cur_row_.cells_[i].set_uint64(palf_stat.begin_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 14: {
        cur_row_.cells_[i].set_uint64(palf_stat.end_lsn_.val_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 15: {
        cur_row_.cells_[i].set_uint64(palf_stat.end_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 16: {
        cur_row_.cells_[i].set_uint64(palf_stat.max_lsn_.val_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 17: {
        cur_row_.cells_[i].set_uint64(palf_stat.max_scn_.get_val_for_inner_table_field());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 18: {
        const ObAddr arb_server = palf_stat.arbitration_member_.get_server();
        if (arb_server.is_valid()
            && OB_FAIL(arb_server.ip_port_to_string(arbitration_member_buf_, MAX_SINGLE_MEMBER_LENGTH))) {
          SERVER_LOG(WARN, "ip_port_to_string failed", K(ret), K(palf_stat));
        } else {
          if (!arb_server.is_valid()) {
            memset(arbitration_member_buf_, 0, MAX_SINGLE_MEMBER_LENGTH);
          }
          cur_row_.cells_[i].set_varchar(ObString::make_string(arbitration_member_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 19: {
        if (OB_FAIL(learner_list_to_string_(palf_stat.degraded_list_, degraded_list_buf_))) {
          SERVER_LOG(WARN, "learner list to_string failed", K(ret), K(palf_stat));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(degraded_list_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 20: {
        if (OB_FAIL(learner_list_to_string_(palf_stat.learner_list_, learner_list_buf_))) {
          SERVER_LOG(WARN, "learner list to_string failed", K(ret), K(palf_stat));
        } else {
          ObString learner_list_str = ObString::make_string(learner_list_buf_);
          cur_row_.cells_[i].set_lob_value(ObLongTextType, learner_list_str.ptr(), static_cast<int32_t>(learner_list_str.length()));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualPalfStat::member_list_to_string_(
    const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  share::ObLSReplica::MemberList tmp_member_list;
  if (OB_FAIL(share::ObLSReplica::transform_ob_member_list(
      member_list,
      tmp_member_list))) {
    SERVER_LOG(WARN, "fail to transform member_list", KR(ret), K(member_list));
  } else if (OB_FAIL(share::ObLSReplica::member_list2text(
      tmp_member_list,
      member_list_buf_))) {
    SERVER_LOG(WARN, "member_list2text failed", KR(ret),
               K(member_list), K(tmp_member_list), K(member_list_buf_));
  }
  return ret;
}

int ObAllVirtualPalfStat::learner_list_to_string_(
    const common::GlobalLearnerList &learner_list,
    char *output_buf)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[MAX_IP_PORT_LENGTH];
  memset(output_buf, 0, MAX_LEARNER_LIST_LENGTH);
  const int64_t count = learner_list.get_member_number();
  ObMember tmp_learner;
  for (int64_t i = 0; i < count && (OB_SUCCESS == ret); ++i) {
    if (OB_FAIL(learner_list.get_learner(i, tmp_learner))) {
      SERVER_LOG(WARN, "get_learner failed", KR(ret), K(i));
    }
    if (0 != pos) {
      if (pos + 1 < MAX_LEARNER_LIST_LENGTH) {
        output_buf[pos++] = ',';
      } else {
        ret = OB_BUF_NOT_ENOUGH;
        SERVER_LOG(WARN, "buffer not enough", KR(ret), K(pos));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(tmp_learner.get_server().ip_port_to_string(buf, sizeof(buf)))) {
      SERVER_LOG(WARN, "convert server to string failed", KR(ret), K(tmp_learner));
    } else {
      int n = snprintf(output_buf + pos, MAX_LEARNER_LIST_LENGTH - pos, \
          "%s:%ld", buf, tmp_learner.get_timestamp());
      if (n < 0 || n >= MAX_LEARNER_LIST_LENGTH - pos) {
        ret = OB_BUF_NOT_ENOUGH;
        SERVER_LOG(WARN, "snprintf error or buf not enough", KR(ret), K(n), K(pos));
      } else {
        pos += n;
      }
    }
  }
  return ret;
}

}//namespace observer
}//namespace oceanbase

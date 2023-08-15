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

#include "ob_all_virtual_arbitration_member_info.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "logservice/ob_log_service.h"
#include "logservice/palf/palf_handle.h"
#include "share/ls/ob_ls_info.h" //MemberList, SimpleMember
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "common/ob_member.h" //ObMember

namespace oceanbase
{
namespace observer
{
ObAllVirtualArbMemberInfo::ObAllVirtualArbMemberInfo()
  : schema_service_(NULL),
    omt_(NULL),
    is_inited_(false)
{}

ObAllVirtualArbMemberInfo::~ObAllVirtualArbMemberInfo()
{
  destroy();
}

void ObAllVirtualArbMemberInfo::destroy()
{
  is_inited_ = false;
  schema_service_ = NULL;
  omt_ = NULL;
}

int ObAllVirtualArbMemberInfo::init(
    share::schema::ObMultiVersionSchemaService *schema_service,
    omt::ObMultiTenant *omt)
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(schema_service) || OB_ISNULL(omt)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid arguments", K(ret), K(schema_service), K(omt));
  } else {
    schema_service_ = schema_service;
    omt_ = omt;
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualArbMemberInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (false == start_to_read_) {
#ifdef OB_BUILD_ARBITRATION
    auto func_iterate_palf = [&](const palf::PalfHandle &palf_handle) -> int {
      int ret = OB_SUCCESS;
      palf::ArbMemberInfo arb_member_info;
      int64_t palf_id = -1;
      palf_handle.get_palf_id(palf_id);
      if (OB_FAIL(palf_handle.get_remote_arb_member_info(arb_member_info))) {
        if (OB_NOT_MASTER != ret
            && OB_STATE_NOT_MATCH != ret) {
          SERVER_LOG(WARN, "PalfHandle get_remote_arb_member_info failed", K(ret), K(palf_id));
        } else {
          // rewrite ret to OB_SUCCESS.
          ret = OB_SUCCESS;
        }
      } else if (!arb_member_info.is_valid()) {
        // info is invalid, skip
      } else if (OB_FAIL(insert_arb_member_info_(arb_member_info, &cur_row_))){
        SERVER_LOG(WARN, "ObAllVirtualArbMemberInfo insert_arb_member_info_ failed", K(ret), K(palf_id), K(arb_member_info));
      } else {
        SERVER_LOG(TRACE, "iterate this log_stream success", K(palf_id), K(arb_member_info));
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
      ret = OB_ARBITRATION_INFO_QUERY_FAILED;
    } else {
      scanner_it_ = scanner_.begin();
      start_to_read_ = true;
    }
#else
    ret = OB_ITER_END;
#endif
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

#ifdef OB_BUILD_ARBITRATION
int ObAllVirtualArbMemberInfo::insert_arb_member_info_(
    const palf::ArbMemberInfo &arb_member_info,
    common::ObNewRow *row)
{
  int ret = OB_SUCCESS;
  const int64_t count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
      case OB_APP_MIN_COLUMN_ID: {
        cur_row_.cells_[i].set_int(MTL_ID());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 1: {
        cur_row_.cells_[i].set_int(arb_member_info.palf_id_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 2: {
        if (false == arb_member_info.arb_server_.ip_to_string(ip_, common::OB_IP_PORT_STR_BUFF)) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "ip_to_string failed", K(ret), K(arb_member_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(ip_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 3: {
        cur_row_.cells_[i].set_int(arb_member_info.arb_server_.get_port());
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 4: {
        cur_row_.cells_[i].set_int(arb_member_info.log_proposal_id_);
        cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                              ObCharset::get_default_charset()));
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 5: {
        if (0 >= arb_member_info.config_version_.to_string(config_version_buf_, VARCHAR_128)) {
          SERVER_LOG(WARN, "config_version_ to_string failed", K(ret), K(arb_member_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(config_version_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 6: {
        if (OB_FAIL(palf::access_mode_to_string(arb_member_info.access_mode_,
                access_mode_str_, sizeof(access_mode_str_)))) {
          SERVER_LOG(WARN, "access_mode_to_string failed", K(ret), K(arb_member_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(access_mode_str_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 7: {
        if (OB_FAIL(member_list_to_string_(arb_member_info.paxos_member_list_))) {
          SERVER_LOG(WARN, "memberlist to_string failed", K(ret), K(arb_member_info));
        } else {
          cur_row_.cells_[i].set_varchar(member_list_buf_.string());
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 8: {
        cur_row_.cells_[i].set_int(arb_member_info.paxos_replica_num_);
        break;
      }
      case OB_APP_MIN_COLUMN_ID + 9: {
        const ObAddr arb_server = arb_member_info.arbitration_member_.get_server();
        if (arb_server.is_valid()
            && OB_FAIL(arb_server.ip_port_to_string(arbitration_member_buf_, MAX_SINGLE_MEMBER_LENGTH))) {
          SERVER_LOG(WARN, "ip_port_to_string failed", K(ret), K(arb_member_info));
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
      case OB_APP_MIN_COLUMN_ID + 10: {
        if (OB_FAIL(learner_list_to_string_(arb_member_info.degraded_list_))) {
          SERVER_LOG(WARN, "learner list to_string failed", K(ret), K(arb_member_info));
        } else {
          cur_row_.cells_[i].set_varchar(ObString::make_string(degraded_list_buf_));
          cur_row_.cells_[i].set_collation_type(ObCharset::get_default_collation(
                                                ObCharset::get_default_charset()));
        }
        break;
      }
    }
  }
  return ret;
}

int ObAllVirtualArbMemberInfo::member_list_to_string_(
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
        K(member_list), K(tmp_member_list), K_(member_list_buf));
  }
  return ret;
}

int ObAllVirtualArbMemberInfo::learner_list_to_string_(
    const common::GlobalLearnerList &learner_list)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  char buf[MAX_IP_PORT_LENGTH];
  if (learner_list.get_member_number() == 0) {
    memset(degraded_list_buf_, 0, MAX_LEARNER_LIST_LENGTH);
  } else {
    const int64_t count = learner_list.get_member_number();
    ObMember tmp_learner;
    for (int64_t i = 0; i < count && (OB_SUCCESS == ret); ++i) {
      if (OB_FAIL(learner_list.get_learner(i, tmp_learner))) {
        SERVER_LOG(WARN, "get_learner failed", KR(ret), K(i));
      }
      if (0 != pos) {
        if (pos + 1 < MAX_LEARNER_LIST_LENGTH) {
          degraded_list_buf_[pos++] = ',';
        } else {
          ret = OB_BUF_NOT_ENOUGH;
          SERVER_LOG(WARN, "buffer not enough", KR(ret), K(pos));
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(tmp_learner.get_server().ip_port_to_string(buf, sizeof(buf)))) {
        SERVER_LOG(WARN, "convert server to string failed", KR(ret), K(tmp_learner));
      } else {
        int n = snprintf(degraded_list_buf_ + pos, MAX_LEARNER_LIST_LENGTH - pos, \
            "%s:%ld", buf, tmp_learner.get_timestamp());
        if (n < 0 || n >= MAX_LEARNER_LIST_LENGTH - pos) {
          ret = OB_BUF_NOT_ENOUGH;
          SERVER_LOG(WARN, "snprintf error or buf not enough", KR(ret), K(n), K(pos));
        } else {
          pos += n;
        }
      }
    }
  }
  return ret;
}
#endif

}//namespace observer
}//namespace oceanbase

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
    ret = OB_ITER_END;
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


}//namespace observer
}//namespace oceanbase

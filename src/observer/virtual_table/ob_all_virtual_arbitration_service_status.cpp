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

#include "ob_all_virtual_arbitration_service_status.h"
#include "common/row/ob_row.h"
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/string/ob_string.h"
#include "observer/ob_server_struct.h"
#include "rpc/obrpc/ob_net_keepalive.h"
#include "storage/ob_locality_manager.h"

namespace oceanbase
{
namespace observer
{
const char* ARB_STATUS_ACTIVE = "ACTIVE";
const char* ARB_STATUS_INACTIVE = "INACTIVE";

ObAllVirtualArbServiceStatus::ObAllVirtualArbServiceStatus()
{}

ObAllVirtualArbServiceStatus::~ObAllVirtualArbServiceStatus()
{
  destroy();
}

void ObAllVirtualArbServiceStatus::destroy()
{
}

int ObAllVirtualArbServiceStatus::inner_get_next_row(common::ObNewRow *&row)
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

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

#include "mds_retry_control.h"
#include "lib/ob_errno.h"
#include "logservice/ob_garbage_collector.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

bool RetryParam::check_ls_in_gc_state() const {
  int ret = OB_SUCCESS;
  bool ret_bool = false;
  logservice::ObGCHandler *gc_handler = nullptr;
  ObLSService *ls_service = MTL(ObLSService *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_ISNULL(ls_service)) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "ls service is null", K(*this));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    MDS_LOG(WARN, "fail to get ls", K(*this));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    MDS_LOG(WARN, "fail to get ls", K(*this));
  } else if (OB_ISNULL(gc_handler = ls->get_gc_handler())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "gc handler is null", K(*this));
  } else if (gc_handler->is_log_sync_stopped()) {
    ret_bool = true;
  }

  return ret_bool;
}

}
}
}
/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/concurrency_control/ob_data_validation_service.h"

namespace oceanbase
{
namespace concurrency_control
{

bool ObDataValidationService::need_delay_resource_recycle(const ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle handle;
  ObLS *ls = nullptr;
  const bool need_delay_opt = GCONF._delay_resource_recycle_after_correctness_issue;
  bool need_delay_ret = false;

  if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, handle, ObLSGetMod::TXSTORAGE_MOD))) {
    if (OB_LS_NOT_EXIST != ret) {
      TRANS_LOG(DEBUG, "get log stream failed", K(ls_id), K(ret));
    }
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get log stream failed", K(ls_id), K(ret));
  } else {
    need_delay_ret = ls->need_delay_resource_recycle() && need_delay_opt;

    if (!need_delay_opt && ls->need_delay_resource_recycle()) {
      ls->clear_delay_resource_recycle();
    }
  }

  return need_delay_ret;
}

void ObDataValidationService::set_delay_resource_recycle(const ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  ObLSHandle handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLS *ls = nullptr;
  const bool need_delay_opt = GCONF._delay_resource_recycle_after_correctness_issue;

  if (OB_LIKELY(!need_delay_opt)) {
    // do nothing
  } else if (OB_UNLIKELY(!ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), K(ls_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id, handle, ObLSGetMod::TXSTORAGE_MOD))) {
    if (OB_LS_NOT_EXIST != ret) {
      TRANS_LOG(DEBUG, "get log stream failed", K(ls_id), K(ret));
    }
  } else if (OB_ISNULL(ls = handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "get log stream failed", K(ls_id), K(ret));
  } else {
    ls->set_delay_resource_recycle();
  }
}

} // namespace concurrency_control
} // namespace oceanbase

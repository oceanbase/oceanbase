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

#define USING_LOG_PREFIX STORAGE

#include "mock_access_service.h"

#include "lib/oblog/ob_log.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_errno.h"
#include "storage/access/ob_dml_param.h"

using namespace oceanbase::storage;

MockObAccessService::MockObAccessService(ObLSTabletService *tablet_service)
  : tablet_service_(tablet_service)
{
}

int MockObAccessService::insert_rows(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    transaction::ObTxDesc &tx_desc,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!tx_desc.is_valid())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id), K(tx_desc),
             K(dml_param), K(column_ids), K(row_iter));
  } else if (OB_ISNULL(tablet_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is null", K(ret));
  } else if (OB_FAIL(check_write_allowed_(ls_id,
                                          tablet_id,
                                          ObStoreAccessType::MODIFY,
                                          dml_param,
                                          dml_param.timeout_,
                                          tx_desc,
                                          tablet_handle,
                                          *dml_param.store_ctx_guard_))) {
    LOG_WARN("fail to check query allowed", K(ret), K(ls_id), K(tablet_id));
  } else {
    ret = tablet_service_->insert_rows(tablet_handle,
                                       dml_param.store_ctx_guard_->get_store_ctx(),
                                       dml_param,
                                       column_ids,
                                       row_iter,
                                       affected_rows);
  }
  return ret;
}

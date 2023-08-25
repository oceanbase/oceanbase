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

#include "ob_virtual_data_access_service.h"
#include "lib/ash/ob_active_session_guard.h"

using namespace oceanbase::common;
using namespace oceanbase::sql;
using oceanbase::common::ObNewRowIterator;
namespace oceanbase
{
namespace observer
{
int ObVirtualDataAccessService::table_scan(ObVTableScanParam &param, ObNewRowIterator *&result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  ObVirtualTableIterator *vt_iter = NULL;
  if (OB_FAIL(vt_iter_factory_.create_virtual_table_iterator(param, vt_iter))) {
    COMMON_LOG(WARN, "failed to create virtual table iterator", K(ret), K(param));
  } else if (NULL == vt_iter) {
    ret = OB_ERR_UNEXPECTED;
    COMMON_LOG(WARN, "invalid argument", K(vt_iter));
  } else if (OB_FAIL(vt_iter->set_output_column_ids(param.column_ids_))) {
    COMMON_LOG(WARN, "fail to set output column ids", K(ret));
  } else {
    vt_iter->set_scan_param(&param);
    vt_iter->set_allocator(param.scan_allocator_);
    vt_iter->set_reserved_column_cnt(param.reserved_cell_count_);
    if (OB_FAIL(vt_iter->open())) {
      COMMON_LOG(WARN, "fail to init row iterator", K(ret));
    } else {
      result = static_cast<ObNewRowIterator* > (vt_iter);
    }
  }

  // clean up
  if (OB_FAIL(ret) && !OB_ISNULL(vt_iter)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = revert_scan_iter(vt_iter))) {
      COMMON_LOG(WARN, "failed to revert_scan_iter", K(tmp_ret));
    }
    vt_iter = NULL;
  }
  return ret;
}

int ObVirtualDataAccessService::revert_scan_iter(ObNewRowIterator *result)
{
  ACTIVE_SESSION_FLAG_SETTER_GUARD(in_storage_read);
  int ret = OB_SUCCESS;
  if (NULL == result) {
    COMMON_LOG(DEBUG, "reuslt is null", K(ret), K(result));
  } else {
    ObVirtualTableIterator * vt_iter = dynamic_cast<ObVirtualTableIterator *> (result);
    if (NULL == vt_iter) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "dynamic_cast failed, iter is not vt iter", K(ret));
    } else if (OB_FAIL(vt_iter->close())) {
      COMMON_LOG(WARN, "fail to close ObNewRowIterator", K(ret));
    } else {
      ret = vt_iter_factory_.revert_virtual_table_iterator(vt_iter);
    }
  }
  return ret;
}

int ObVirtualDataAccessService::check_iter(common::ObVTableScanParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(vt_iter_factory_.check_can_create_iter(param))) {
    COMMON_LOG(WARN, "failed to check create virtual table iterator", K(ret), K(param));
  }
  return ret;
}

}
}

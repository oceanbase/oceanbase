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

#include "storage/tablet/ob_mds_range_query_iterator.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{
int ObMdsRangeQueryIteratorHelper::get_mds_table(const ObTabletHandle &tablet_handle, mds::MdsTableHandle &mds_table)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTabletPointer *tablet_pointer = nullptr;

  if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "tablet is null", K(ret));
  } else if (OB_ISNULL(tablet_pointer = tablet->get_pointer_handle().get_resource_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "tablet pointer is null", K(ret));
  } else if (OB_FAIL(tablet_pointer->get_mds_table(tablet->get_tablet_id(), mds_table, false/*not_exist_create*/))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      MDS_LOG(WARN, "fail to get mds table from tablet pointer", K(ret), "tablet_id", tablet->get_tablet_id());
    }
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase
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

#include "ob_i_tablet_mds_customized_interface.h"

namespace oceanbase
{
namespace storage
{

int ObITabletMdsCustomizedInterface::get_ddl_data(ObTabletBindingMdsUserData &ddl_data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest_committed(ddl_data))) {
    if (OB_EMPTY_RESULT == ret) {
      ddl_data.set_default_value(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int ObITabletMdsCustomizedInterface::get_autoinc_seq(share::ObTabletAutoincSeq &inc_seq, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(get_latest_committed(inc_seq, &allocator))) {
    if (OB_EMPTY_RESULT == ret) {
      inc_seq.reset(); // use default value
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

}
}
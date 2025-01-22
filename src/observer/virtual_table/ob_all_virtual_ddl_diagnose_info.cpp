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

#include "ob_all_virtual_ddl_diagnose_info.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/ob_ddl_common.h"
#include "share/inner_table/ob_inner_table_schema_constants.h"
#include "share/ob_ddl_sim_point_define.h"
#include "deps/oblib/src/lib/string/ob_string.h"
#include "deps/oblib/src/lib/utility/ob_macro_utils.h"
#include "deps/oblib/src/lib/alloc/alloc_assist.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
using namespace share;

namespace observer
{
int ObAllVirtualDDLDiagnoseInfo::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualDDLDiagnoseInfo has been inited", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualDDLDiagnoseInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualDDLDiagnoseInfo has not been inited", K(ret));
  } else {
    ret = OB_ITER_END;
  }
  return ret;
}
}// observer
}// oceanbase
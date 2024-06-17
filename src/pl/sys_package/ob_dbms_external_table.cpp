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

#define USING_LOG_PREFIX PL
#include "pl/sys_package/ob_dbms_external_table.h"
#include "pl/ob_pl_package_manager.h"
#include "sql/resolver/expr/ob_raw_expr_util.h"
#include "share/external_table/ob_external_table_file_mgr.h"

namespace oceanbase
{
using namespace sql;
using namespace common;

namespace pl
{


int ObDBMSExternalTable::auto_refresh_external_table(ObExecContext &exec_ctx, ParamStore &params, ObObj &result)
{
  int ret = OB_SUCCESS;
  int32_t interval = 0;
  if (params.count() == 0) {
    interval = 0;
  } else {
    const ObObjParam &param0 = params.at(0);
    if (param0.is_null()) {
      //do nothing
    } else if (OB_FAIL(param0.get_int32(interval))) {
      LOG_WARN("failed to get number", K(ret), K(param0));
    }
  }

  if (OB_SUCC(ret)) {
    OZ (ObExternalTableFileManager::get_instance().auto_refresh_external_table(exec_ctx, interval));
  }
  return ret;
}

} // end of pl
} // end oceanbase

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

#include "storage/ob_store_row_filter.h"
#include "storage/ob_i_store.h"
#include "share/ob_errno.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "observer/ob_server_struct.h"
#include "sql/optimizer/ob_table_location.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace storage
{

int ObStoreRowFilter::init(const ObTableLocation *part_filter, ObExecContext *exec_ctx)
{
  int ret = OB_SUCCESS;
  if (NULL == part_filter || NULL == exec_ctx) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(part_filter), KP(exec_ctx));
  } else {
    part_filter_ = part_filter;
    exec_ctx_ = exec_ctx;
  }
  return ret;
}

int ObStoreRowFilter::check(const ObStoreRow &store_row, bool &is_filtered) const
{
  // support for partition split, not used now
  return OB_NOT_SUPPORTED;
}

} // namespace storage
} // namespace oceanbase

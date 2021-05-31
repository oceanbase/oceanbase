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

namespace oceanbase {
using namespace common;
using namespace sql;

namespace storage {

int ObStoreRowFilter::init(
    const ObTableLocation* part_filter, ObExecContext* exec_ctx, ObPartMgr* part_mgr, const ObPartitionKey& pkey)
{
  int ret = OB_SUCCESS;
  if (NULL == part_filter || NULL == exec_ctx || NULL == part_mgr || !pkey.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(part_filter), KP(exec_ctx), KP(part_mgr), K(pkey));
  } else {
    part_filter_ = part_filter;
    exec_ctx_ = exec_ctx;
    part_mgr_ = part_mgr;
    pkey_ = pkey;
  }
  return ret;
}

int ObStoreRowFilter::check(const ObStoreRow& store_row, bool& is_filtered) const
{
  int ret = OB_SUCCESS;
  int64_t part_id = 0;
  if (NULL == part_filter_ || NULL == exec_ctx_ || NULL == part_mgr_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected error", K(ret), KP(part_filter_), KP(exec_ctx_), KP(part_mgr_));
  } else if (OB_FAIL(part_filter_->calculate_partition_id_by_row(*exec_ctx_, part_mgr_, store_row.row_val_, part_id))) {
    STORAGE_LOG(WARN, "calculate partition id by row failed", K(ret), K(store_row));
  } else {
    is_filtered = (pkey_.get_partition_id() != part_id);
    if (is_filtered) {
      if (EXECUTE_COUNT_PER_SEC(16)) {
        STORAGE_LOG(INFO, "store row is filtered", K_(pkey), K(part_id), K(store_row));
      } else {
        STORAGE_LOG(DEBUG, "store row is filtered", K_(pkey), K(part_id), K(store_row));
      }
    }
  }
  return ret;
}

}  // namespace storage
}  // namespace oceanbase

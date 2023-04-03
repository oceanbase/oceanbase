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

#define USING_LOG_PREFIX OBLOG

#include "ob_log_table_id_cache.h"

namespace oceanbase
{
using namespace common;

namespace libobcdc
{
/////////////////////////////////////////////////////////////////////////////
void TableInfo::reset()
{
  table_id_ = OB_INVALID_ID;
}

int TableInfo::init(const uint64_t table_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(OB_INVALID_ID == table_id)) {
    LOG_ERROR("invalid argument", K(table_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    table_id_ = table_id;
  }

  return ret;
}

bool TableInfoEraserByTenant::operator()(
    const TableID &table_id_key,
    TableInfo &tb_info)
{
  // TODO set tenant_id
  uint64_t target_tenant_id = 0;
  const char *cache_type = NULL;
  if (is_global_normal_index_) {
    cache_type = "GLOBAL_NORMAL_INDEX_TBALE";
  } else {
    cache_type = "SERVED_TABLE_ID_CACHE";
  }

  if (tenant_id_ == target_tenant_id) {
    _LOG_INFO("[DDL] [%s] [REMOVE_BY_TENANT] TENANT_ID=%lu TABLE_ID_INFO=(%lu.%lu)",
        cache_type, tenant_id_, table_id_key.table_id_, tb_info.table_id_);
    // reset value
    tb_info.reset();
  }

  return (tenant_id_ == target_tenant_id);
}

}
}

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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_virtual_table_iterator.h"
#include "observer/virtual_table/ob_tenant_virtual_outline.h"
using oceanbase::common::OB_APP_MIN_COLUMN_ID;
namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
}

namespace common
{
class ObNewRow;
}
namespace share
{
namespace schema
{
class ObOutlineInfo;
}
}
namespace observer
{
class ObTenantVirtualConcurrentLimitSql : public ObTenantVirtualOutlineBase
{
  enum TENANT_VIRTUAL_OUTLINE_COLUMN
  {
    TENANT_ID = OB_APP_MIN_COLUMN_ID,
    DATABASE_ID = OB_APP_MIN_COLUMN_ID + 1,
    OUTLINE_ID = OB_APP_MIN_COLUMN_ID + 2,
    DATABASE_NAME = OB_APP_MIN_COLUMN_ID + 3,
    OUTLINE_NAME = OB_APP_MIN_COLUMN_ID + 4,
    OUTLINE_CONTENT = OB_APP_MIN_COLUMN_ID + 5,
    VISIBLE_SIGNATURE = OB_APP_MIN_COLUMN_ID + 6,
    SQL_TEXT = OB_APP_MIN_COLUMN_ID + 7,
    CONCURRENT_NUM = OB_APP_MIN_COLUMN_ID + 8,
    LIMIT_TARGET = OB_APP_MIN_COLUMN_ID + 9,
  };
public:
  ObTenantVirtualConcurrentLimitSql()
      : ObTenantVirtualOutlineBase(),
      param_idx_(common::OB_INVALID_INDEX)
  {}
  ~ObTenantVirtualConcurrentLimitSql() {}
  void reset();
  virtual int inner_open();
  virtual int inner_get_next_row(common::ObNewRow *&row);

private:
  int fill_cells(const share::schema::ObOutlineInfo *outline_info,
                 const share::schema::ObMaxConcurrentParam *param);
  int get_next_concurrent_limit_row(const share::schema::ObOutlineInfo *outline_info,
                                    bool &is_iter_end);
  int is_need_output(const share::schema::ObOutlineInfo *outline_info, bool &is_output);
private:
  int64_t param_idx_;
  DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualConcurrentLimitSql);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_CONCURRENT_LIMIT_SQL_ */

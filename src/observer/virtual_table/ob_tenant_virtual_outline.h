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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_OUTLINE_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_OUTLINE_

#include "share/ob_define.h"
#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "share/schema/ob_schema_struct.h"
#include "share/ob_virtual_table_iterator.h"

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

class ObTenantVirtualOutlineBase: public common::ObVirtualTableIterator
{
protected:
  struct DBInfo
  {
    DBInfo() : db_name_(), is_recycle_(false) {}

    common::ObString db_name_;
    bool is_recycle_;
  };
public:
  ObTenantVirtualOutlineBase():
      tenant_id_(common::OB_INVALID_TENANT_ID),
      outline_info_idx_(common::OB_INVALID_INDEX),
      outline_infos_(),
      database_infos_()
  {}
  ~ObTenantVirtualOutlineBase() {}
  virtual int inner_open();
  void reset();
  inline void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
protected:
  int is_database_recycle(uint64_t database_id, bool &is_recycle);
  int set_database_infos_and_get_value(uint64_t database_id, bool &is_recycle);
protected:
  uint64_t tenant_id_;
  int64_t outline_info_idx_;
  common::ObSEArray<const share::schema::ObOutlineInfo*, 16> outline_infos_;
  common::hash::ObHashMap<uint64_t, DBInfo> database_infos_;
private:
   DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualOutlineBase);
};

class ObTenantVirtualOutline : public ObTenantVirtualOutlineBase
{
  enum TENANT_VIRTUAL_OUTLINE_COLUMN
  {
    TENANT_ID = OB_APP_MIN_COLUMN_ID,
    DATABASE_ID = OB_APP_MIN_COLUMN_ID + 1,
    OUTLINE_ID = OB_APP_MIN_COLUMN_ID + 2,
    DATABASE_NAME = OB_APP_MIN_COLUMN_ID + 3,
    OUTLINE_NAME = OB_APP_MIN_COLUMN_ID + 4,
    VISIBLE_SIGNATURE = OB_APP_MIN_COLUMN_ID + 5,
    SQL_TEXT = OB_APP_MIN_COLUMN_ID + 6,
    OUTLINE_TARGET = OB_APP_MIN_COLUMN_ID + 7,
    OUTLINE_SQL = OB_APP_MIN_COLUMN_ID + 8,
    SQL_ID = OB_APP_MIN_COLUMN_ID + 9,
    OUTLINE_CONTENT = OB_APP_MIN_COLUMN_ID + 10,
    FORMAT_SQL_TEXT = OB_APP_MIN_COLUMN_ID + 11,
    FORMAT_SQL_ID = OB_APP_MIN_COLUMN_ID + 12,
    FORMAT_OUTLINE = OB_APP_MIN_COLUMN_ID + 13,
  };
public:
  ObTenantVirtualOutline() : ObTenantVirtualOutlineBase() {}
  ~ObTenantVirtualOutline() {}
  void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  int fill_cells(const share::schema::ObOutlineInfo *outline_info);
  int is_output_outline(const share::schema::ObOutlineInfo *outline_info, bool &is_output);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantVirtualOutline);
};
}
}
#endif /* OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_TENANT_VIRTUAL_OUTLINE_ */

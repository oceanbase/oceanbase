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

#ifndef OCEANBASE_UNITTEST_SQL_OPTIMIZER_OB_MOCK_PART_MGR_
#define OCEANBASE_UNITTEST_SQL_OPTIMIZER_OB_MOCK_PART_MGR_
#include "share/ob_errno.h"
#include "share/part/ob_part_mgr.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "common/object/ob_object.h"
using namespace oceanbase;
namespace oceanbase
{
namespace share
{
namespace schema
{
class ObSchemaManager;
}
}
}
namespace test
{
class ObMockPartMgr : public oceanbase::common::ObPartMgr
{
public:
  ObMockPartMgr() : schema_guard_(NULL)
  { }

  virtual inline int get_part(uint64_t table_id,
                              share::schema::ObPartitionLevel part_level,
                              int64_t part_id,
                              const common::ObNewRange &range,
                              bool reverse,
                              common::ObIArray<int64_t> &part_ids);

  virtual int update_part_desc_hash(uint64_t table_id,
                                    int64_t part_num,
                                    int64_t part_space)
  {
    UNUSED(table_id);
    UNUSED(part_num);
    UNUSED(part_space);
    return common::OB_NOT_IMPLEMENT;
  }
  static const int64_t COUNT = 200;
  oceanbase::share::schema::ObSchemaGetterGuard *schema_guard_;
};

int ObMockPartMgr::get_part(uint64_t table_id,
                            share::schema::ObPartitionLevel part_level,
                            int64_t part_id,
                            const common::ObNewRange &range,
                            bool reverse,
                            common::ObIArray<int64_t> &part_ids)
{
  int ret = common::OB_SUCCESS;
  int64_t part_num = 0;
  UNUSED(table_id);
  UNUSED(part_level);
  UNUSED(part_id);
  UNUSED(reverse);
  if (OB_ISNULL(schema_guard_)) {
    ret = common::OB_NOT_INIT;
    OB_LOG(WARN, "Schema mgr not inited", K(ret));
  } else {
    const oceanbase::share::schema::ObTableSchema *table_schema = NULL;
    schema_guard_->get_table_schema(table_id, table_schema);
    if (OB_ISNULL(table_schema)) {
      ret = common::OB_SCHEMA_ERROR;
      OB_LOG(WARN, "Can not find table schema", K(table_id), K(ret));
    } else {
      part_num = table_schema->get_part_option().get_part_num();
    }
  }

  if (OB_FAIL(ret)) {
  } else if (range.is_single_rowkey()) {
    const common::ObObj &value = range.get_start_key().get_obj_ptr()[0];
    int64_t result = 0;
    if (common::ObInt32Type == value.get_type()) {
      result = value.get_int32();
    } else if (common::ObIntType == value.get_type()) {
      result = value.get_int();
    } else { }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(part_ids.push_back(result%part_num))) {
        OB_LOG(WARN, "Failed to add id", K(ret));
      }
    }
  } else {
    for (int64_t idx = 0; OB_SUCC(ret) && idx < part_num; ++idx) {
      if (OB_FAIL(part_ids.push_back(idx))) {
        OB_LOG(WARN, "Failed to add id", K(ret));
      }
    }
  }
  return ret;
}

}

#endif

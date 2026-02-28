// owner: muwei.ym
// owner group: storage_ha

/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "simple_server/env/ob_simple_cluster_test_base.h"

namespace oceanbase
{
namespace unittest
{

#define WRITE_SQL_BY_CONN(conn, sql_str)            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str)); \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

#define WRITE_SQL_FMT_BY_CONN(conn, ...)                                \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));                   \
  ASSERT_EQ(OB_SUCCESS, conn->execute_write(OB_SYS_TENANT_ID, sql.ptr(), affected_rows));

class TestVectorIndexBase : public ObSimpleClusterTestBase {
public:
  TestVectorIndexBase()
      : ObSimpleClusterTestBase("test_vector_index", "20G", "20G")
  {
  }
  virtual ~TestVectorIndexBase() = default;
  void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
  }

  int get_current_scn(share::SCN &current_scn);
  void insert_data(const int64_t idx, const int64_t insert_cnt);
  void search_data(const int64_t idx, const int64_t select_cnt);
  int get_snapshot_metadata(ObPluginVectorIndexAdaptor* adaptor, const ObLSID &ls_id, ObVectorIndexMeta &meta);
  int check_segment_meta_row(ObPluginVectorIndexAdaptor *adaptor, const ObLSID& ls_id, ObVectorIndexSegmentMeta &seg_meta);
  int check_vector_index_task_finished();
  int check_vector_index_task_success();
  int check_vector_index_task_count(const int64_t expected_cnt);

private:
  DISALLOW_COPY_AND_ASSIGN(TestVectorIndexBase);
};

}
}

/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/ob_dml_sql_splicer.h"
#include <gtest/gtest.h>
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

namespace oceanbase
{
namespace unittest
{

TEST(ObPTSqlSplicer, batch) {
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer splicer;
  ObSqlString sql;
  ObString result("INSERT INTO tname (gmt_modified, gmt_create, tenant_id, tablet_id, svr_ip, svr_port, ls_id, compaction_scn, row_count, data_checksum, column_checksums, b_column_checksums) VALUES "
                  "(now(6), now(6), 1004, 200001, '127.0.0.1', 1001, 1001, 1234567890, 100, 1234567, 'column_checksums-01', 'b_column_checksums-01'),"
                  "(now(6), now(6), 1004, 200001, '127.0.0.2', 1002, 1001, 1234567890, 100, 1234567, 'column_checksums-02', 'b_column_checksums-02'),"
                  "(now(6), now(6), 1004, 200001, '127.0.0.3', 1003, 1001, 1234567890, 100, 1234567, 'column_checksums-03', 'b_column_checksums-03') "
                  "ON DUPLICATE KEY UPDATE "
                  "gmt_modified=VALUES(gmt_modified),"
                  "gmt_create=VALUES(gmt_create),"
                  "tenant_id=VALUES(tenant_id),"
                  "tablet_id=VALUES(tablet_id),"
                  "svr_ip=VALUES(svr_ip),"
                  "svr_port=VALUES(svr_port),"
                  "ls_id=VALUES(ls_id),"
                  "compaction_scn=VALUES(compaction_scn),"
                  "row_count=VALUES(row_count),"
                  "data_checksum=VALUES(data_checksum),"
                  "column_checksums=VALUES(column_checksums),"
                  "b_column_checksums=VALUES(b_column_checksums)");

  ObAddr ips[3];
  ObString column_checksums[3] = {"column_checksums-01", "column_checksums-02", "column_checksums-03"};
  ObString b_column_checksums[3] = {"b_column_checksums-01", "b_column_checksums-02", "b_column_checksums-03"};
  ASSERT_TRUE(ips[0].set_ip_addr("127.0.0.1", 1001));
  ASSERT_TRUE(ips[1].set_ip_addr("127.0.0.2", 1002));
  ASSERT_TRUE(ips[2].set_ip_addr("127.0.0.3", 1003));
  char ip[OB_MAX_SERVER_ADDR_SIZE] = "";

  for (int64_t i = 0; i < 3; i++) {
    ASSERT_TRUE(ips[i].ip_to_string(ip, sizeof(ip)));
    ASSERT_EQ(OB_SUCCESS, splicer.add_gmt_modified());
    ASSERT_EQ(OB_SUCCESS, splicer.add_gmt_create());
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("tenant_id", 1004));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("tablet_id", 200001));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("svr_ip", ip));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("svr_port", ips[i].get_port()));
    ASSERT_EQ(OB_SUCCESS, splicer.add_pk_column("ls_id", 1001));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("compaction_scn", 1234567890));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("row_count", 100));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("data_checksum", 1234567));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("column_checksums", column_checksums[i]));
    ASSERT_EQ(OB_SUCCESS, splicer.add_column("b_column_checksums", b_column_checksums[i]));
    ASSERT_EQ(OB_SUCCESS, splicer.finish_row());
  }
  ASSERT_EQ(OB_SUCCESS, splicer.splice_batch_insert_update_sql("tname", sql));
  LOG_INFO("finish splice batch insert update sql", K(ret), K(sql), K(result));
  ASSERT_EQ(0, result.compare(sql.string()));
}

} // end unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

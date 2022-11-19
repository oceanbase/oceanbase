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

#define USING_LOG_PREFIX SHARE

#include "share/ob_tenant_tablet_checksum_iterator.h"
#include "lib/stat/ob_session_stat.h"
#include <gtest/gtest.h>
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestTenantTabletChecksumIterator : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

private:
  void build_checksum_item(const uint64_t tablet_id, ObTabletChecksumItem &item);
  int insert_tablet_checksum_items(const uint64_t item_count);
  int insert_tablet_checksum_item(const ObTabletChecksumItem &item, ObISQLClient &sql_client);

protected:
  const static uint64_t DEFAULT_TENANT_ID = OB_SYS_TENANT_ID;
  const static uint64_t DEFAULT_ITEM_COUNT = 10;
  DBInitializer db_initer_;
  ObTenantTabletChecksumIterator iterator_;
//   ObArray<ObTabletChecksumItem> checksum_items_;
};

void TestTenantTabletChecksumIterator::build_checksum_item(const uint64_t tablet_id, ObTabletChecksumItem &item)
{
  item.tenant_id_ = DEFAULT_TENANT_ID;
  item.tablet_id_ = tablet_id;
  item.data_version_ = 8;
  item.snapshot_version_ = 9;
  item.data_checksum_ = 77777;
  item.row_count_ = 100;
  item.replica_type_ = 3;
}

int TestTenantTabletChecksumIterator::insert_tablet_checksum_items(const uint64_t item_count)
{
  int ret = OB_SUCCESS;
  ObTabletChecksumItem tmp_item;
  for (int64_t i = 0; i < item_count && OB_SUCC(ret); ++i) {
    build_checksum_item(i + 1, tmp_item);
    if (OB_FAIL(insert_tablet_checksum_item(tmp_item, db_initer_.get_sql_proxy()))) {
      LOG_WARN("fail to insert checksum item", KR(ret), K(tmp_item));
    }
  }
  return ret;
}

int TestTenantTabletChecksumIterator::insert_tablet_checksum_item(
    const ObTabletChecksumItem &item, 
    ObISQLClient &sql_client)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  // %zone can be empty
  if (!item.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", K(ret), K(item));
  } else {
    if (OB_FAIL(sql.assign_fmt("INSERT INTO %s (tenant_id, tablet_id, data_version, snapshot_version, data_checksum, "
          "row_count, replica_type, gmt_modified, gmt_create)"
          " VALUES('%lu', '%lu', %ld, %ld, %ld, %ld, %d, now(6), now(6))",
          OB_ALL_TABLET_CHECKSUM_TNAME, item.tenant_id_, item.tablet_id_, item.data_version_, item.snapshot_version_,
          item.data_checksum_, item.row_count_, item.replica_type_))) {
      LOG_ERROR("fail to assign sql", KR(ret), K(item));
    }
  }

  int64_t affected_rows = 0;
  if (OB_FAIL(ret)) {
    // nothing to do
  } else if (OB_FAIL(sql_client.write(sql.ptr(), affected_rows))) {
    LOG_ERROR("execute sql failed", KR(ret), K(sql));
  } else if (!(is_single_row(affected_rows) || is_zero_row(affected_rows))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected affected rows", KR(ret), K(affected_rows), K(item));
  } else {
    LOG_INFO("succ to update info item", K(sql), K(item));
  }
  return ret;
}

void TestTenantTabletChecksumIterator::SetUp()
{
  int ret = db_initer_.init();
  ASSERT_EQ(OB_SUCCESS, ret);
  const bool only_core_tables = false;
  ret = db_initer_.create_system_table(only_core_tables);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = insert_tablet_checksum_items(DEFAULT_ITEM_COUNT);
  ASSERT_EQ(OB_SUCCESS, ret);
  common::ObMySQLProxy &proxy = db_initer_.get_sql_proxy();
  ret = iterator_.init(DEFAULT_TENANT_ID, &proxy);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(TestTenantTabletChecksumIterator, next_checksum_item)
{
  int ret = OB_SUCCESS;
  uint64_t count = 5;
  ObTabletChecksumItem tmp_item;
  for (int64_t i = 0; i < count; ++i) {
    ret = iterator_.next(tmp_item);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(i + 1, tmp_item.tablet_id_);
  }
  
  ret = iterator_.next(tmp_item);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(count + 1, tmp_item.tablet_id_);

  for (int64_t i = count + 2; i <= DEFAULT_ITEM_COUNT + 1; ++i) {
    ret = iterator_.next(tmp_item);
    if (i <= DEFAULT_ITEM_COUNT) {
      ASSERT_EQ(OB_SUCCESS, ret);
      ASSERT_EQ(i, tmp_item.tablet_id_);
    } else {
      ASSERT_EQ(OB_ITER_END, ret);
    }
  }
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  oceanbase::common::ObClusterVersion::get_instance().init(CLUSTER_VERSION_2200);
  return RUN_ALL_TESTS();
}
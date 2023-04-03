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
#include "lib/stat/ob_session_stat.h"
#include "share/ob_core_table_proxy.h"
#include "share/ob_dml_sql_splicer.h"

#include <gtest/gtest.h>
#include "schema/db_initializer.h"

namespace oceanbase
{
namespace share
{
using namespace common;
using namespace share::schema;

class TestCoreTableProxy : public ::testing::Test
{
public:
  virtual void SetUp();
  virtual void TearDown() {}

protected:
  int insert_data(const char *table, int64_t cnt);

  void verify_data(ObCoreTableProxy &kv);

protected:
  schema::DBInitializer initer_;
};

void TestCoreTableProxy::SetUp()
{
  int ret = initer_.init();

  ASSERT_EQ(OB_SUCCESS, ret);

  ret = initer_.create_system_table(true);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = insert_data("abc", 5);
  ASSERT_EQ(OB_SUCCESS, ret);
}

int TestCoreTableProxy::insert_data(const char *table, int64_t cnt)
{
  ObCoreTableProxy kv(table, initer_.get_sql_proxy());
  ObArray<ObCoreTableProxy::UpdateCell> cells;
  int ret = kv.load_for_update();
  if (OB_FAIL(ret)) {
    return ret;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < cnt; i++) {
    ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
    ObArray<ObCoreTableProxy::UpdateCell> cells;
    if (OB_FAIL(dml.add_pk_column("k1", i))
        || OB_FAIL(dml.add_pk_column("k2", "aa"))
        || OB_FAIL(dml.add_column("c1", (i + 1) *100))
        || OB_FAIL(dml.add_column("c2", "zj"))
        || OB_FAIL(dml.splice_core_cells(kv, cells))) {
    }
    int64_t affected_rows = 0;
    if (OB_FAIL(ret)) {
      LOG_WARN("splice core kv cells failed", K(ret));
    } else if (OB_FAIL(kv.replace_row(cells, affected_rows))) {
      LOG_WARN("replace row failed", K(ret));
    }
  }
  return ret;
}

void TestCoreTableProxy::verify_data(ObCoreTableProxy &kv)
{
  ObCoreTableProxy kv2("abc", initer_.get_sql_proxy());
  int ret = kv2.load();
  ASSERT_EQ(OB_SUCCESS, ret);

  ASSERT_EQ(kv.row_count(), kv2.row_count());
  for (int64_t i = 0; i < kv.row_count(); ++i) {
    const ObCoreTableProxy::Row &lr = kv.get_all_row().at(i);
    const ObCoreTableProxy::Row *rr = NULL;
    FOREACH_CNT_X(row, kv2.get_all_row(), NULL == rr) {
      if (lr.get_row_id() == row->get_row_id()) {
        rr = &(*row);
      }
    }
    ASSERT_TRUE(NULL != rr);

    ASSERT_EQ(lr.get_row_id(), rr->get_row_id());
    const char *cols[] = {"k1", "k2", "c1", "c2"};
    for (int64_t k = 0; k < ARRAYSIZEOF(cols); ++k) {
      const char *c = cols[k];
      ObCoreTableProxy::Cell *l = NULL;
      ret = lr.get_cell(c, l);
      if (OB_ENTRY_NOT_EXIST == ret) {
        l = NULL;
      }
      ObCoreTableProxy::Cell *r = NULL;
      ret = rr->get_cell(c, r);
      if (OB_ENTRY_NOT_EXIST == ret) {
        r = NULL;
      }
      ASSERT_TRUE(NULL != l);
      ASSERT_TRUE(NULL != r);

      ASSERT_EQ(l->name_, r->name_);
      ASSERT_EQ(l->value_, r->value_);
    }
  }
}

TEST_F(TestCoreTableProxy, load)
{
  ObCoreTableProxy kv("abc", initer_.get_sql_proxy());

  int ret = kv.load();
  ASSERT_EQ(OB_SUCCESS, ret);

  LOG_INFO("all rows", K(kv.get_all_row()));

  ASSERT_EQ(5, kv.row_count());
  for (int64_t i = 0; i < 5; ++i) {
    ret = kv.next();
    ASSERT_EQ(OB_SUCCESS, ret);
    const ObCoreTableProxy::Row *row = NULL;
    ASSERT_EQ(OB_SUCCESS, kv.get_cur_row(row));
    ASSERT_EQ(i, row->get_row_id());
  }
  ret = kv.next();
  ASSERT_EQ(OB_ITER_END, ret);

  kv.seek_to_head();
  ret = kv.next();
  ASSERT_EQ(OB_SUCCESS, ret);

  int64_t int_val = -1;
  ret = kv.get_int("k1", int_val);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, int_val);

  ret = kv.get_int("k_not_exist", int_val);
  ASSERT_EQ(OB_ERR_NULL_VALUE, ret);

  ObString str;
  ret = kv.get_varchar("c2", str);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, str.compare("zj"));
}

TEST_F(TestCoreTableProxy, null_value)
{
  ObCoreTableProxy kv("abc", initer_.get_sql_proxy());
  int ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  ObArray<ObCoreTableProxy::UpdateCell> cells;

  // update null value
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k1", 1));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c2", dml.NULL_VALUE));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));
  int64_t affected_rows = 0;
  ret = kv.update_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObString str;
  ret = kv.get_all_row().at(1).get_varchar("c2", str);
  ASSERT_EQ(OB_ERR_NULL_VALUE, ret);

  // insert null value
  cells.reuse();
  dml.reset();
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k1", 0));
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k2", "lalala"));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c1", dml.NULL_VALUE));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c2", dml.NULL_VALUE));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));
  ret = kv.replace_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = kv.get_all_row().at(5).get_varchar("c1", str);
  ASSERT_EQ(OB_ERR_NULL_VALUE, ret);
  ret = kv.get_all_row().at(5).get_varchar("c2", str);
  ASSERT_EQ(OB_ERR_NULL_VALUE, ret);
}

TEST_F(TestCoreTableProxy, update_or_insert)
{
  ObCoreTableProxy kv("abc", initer_.get_sql_proxy());
  int ret = kv.load();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  ObArray<ObCoreTableProxy::UpdateCell> cells;

  // update one row
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k1", 1));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c2", "hz"));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("non_exist_column", 1024));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));

  int64_t affected_rows = 0;
  ret = kv.update_row(cells, affected_rows);
  ASSERT_NE(OB_SUCCESS, ret);

  ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = kv.update_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);
  ObString str;
  ret = kv.get_all_row().at(1).get_varchar("c2", str);
  ASSERT_EQ(0, str.compare("hz"));

  // update all rows
  dml.reset();
  ASSERT_EQ(OB_SUCCESS, dml.add_column("k2", 88));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));
  ret = kv.update_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, affected_rows);
  FOREACH_CNT(r, kv.get_all_row()) {
    int64_t int_val = 0;
    ret = r->get_int("k2", int_val);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(88, int_val);
  }

  // update empty rows
  dml.reset();
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k1", 0));
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k2", "lalala"));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c1", -1));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c2", "xxx"));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));
  ret = kv.update_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, affected_rows);

  // inset new row
  ret = kv.replace_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(6, kv.row_count());
  ASSERT_EQ(5, kv.get_all_row().at(5).get_row_id());


  // after all the operations, verify db record with memory rows
  verify_data(kv);
}


TEST_F(TestCoreTableProxy, delete_row)
{
  ObCoreTableProxy kv("abc", initer_.get_sql_proxy());
  int ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);

  ObDMLSqlSplicer dml(ObDMLSqlSplicer::NAKED_VALUE_MODE);
  ObArray<ObCoreTableProxy::UpdateCell> cells;

  // delete all
  int64_t affected_rows = 0;
  ret = kv.delete_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, affected_rows);

  ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(0, kv.row_count());

  ret = insert_data("abc", 5);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = kv.load_for_update();
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(5, kv.row_count());

  // delete one row and insert
  dml.reset();
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k1", 1));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = kv.delete_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);

  ASSERT_EQ(4, kv.row_count());
  ASSERT_EQ(0, kv.get_all_row().at(0).get_row_id());
  ASSERT_EQ(2, kv.get_all_row().at(1).get_row_id());
  ASSERT_EQ(3, kv.get_all_row().at(2).get_row_id());
  ASSERT_EQ(4, kv.get_all_row().at(3).get_row_id());

  dml.reset();
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k1", 0));
  ASSERT_EQ(OB_SUCCESS, dml.add_pk_column("k2", "lalala"));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c1", -1));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("c2", "xxx"));
  ASSERT_EQ(OB_SUCCESS, dml.splice_core_cells(kv, cells));
  ret = kv.replace_row(cells, affected_rows);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(1, affected_rows);
  ASSERT_EQ(5, kv.row_count());
  ASSERT_EQ(1, kv.get_all_row().at(4).get_row_id());

  verify_data(kv);
}

} // end namespace share
} // end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

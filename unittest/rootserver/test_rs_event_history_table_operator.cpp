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

#define USING_LOG_PREFIX RS

#include <gtest/gtest.h>
#define private public
#define protected public

#include "lib/stat/ob_session_stat.h"
#include "../share/schema/db_initializer.h"
#include "lib/time/ob_time_utility.h"
#include "share/ob_define.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "rootserver/ob_rs_event_history_table_operator.h"
#include "share/partition_table/fake_part_property_getter.h"

namespace oceanbase
{
using namespace common;
using namespace common::sqlclient;
using namespace share;
using namespace share::host;
using namespace share::schema;
namespace rootserver
{
struct EventTableRowInfo
{
  int64_t create_time_;
  char *module_;
  char *event_;
  char *name1_;
  char *value1_;
  void alloc();
  void make_row(const char *module, const char *event, const char *name, const char *value);
  TO_STRING_KV(K(create_time_));
};

void EventTableRowInfo::alloc()
{
  module_ = static_cast<char *>(malloc(MAX_ROOTSERVICE_EVENT_DESC_LENGTH));
  event_ = static_cast<char *>(malloc(MAX_ROOTSERVICE_EVENT_DESC_LENGTH));
  name1_ = static_cast<char *>(malloc(MAX_ROOTSERVICE_EVENT_NAME_LENGTH));
  value1_ = static_cast<char *>(malloc(MAX_ROOTSERVICE_EVENT_VALUE_LENGTH));
}

void EventTableRowInfo::make_row(const char *module, const char *event, const char *name, const char *value)
{
  memcpy(module_, module, strlen(module) + 1);
  memcpy(event_, event, strlen(event) + 1);
  memcpy(name1_, name, strlen(name) + 1);
  memcpy(value1_, value, strlen(value) + 1);
}

class FakeEventHistoryTableOperator : public ObRsEventHistoryTableOperator
{
public:
  int get(ObIArray<EventTableRowInfo> &event_infos);
  int build_event_info(const ObMySQLResult &res, EventTableRowInfo &row);
  virtual int process_task(const common::ObString &sql, const bool is_delete);
};

int FakeEventHistoryTableOperator::get(ObIArray<EventTableRowInfo> &event_rows)
{
  int ret = OB_SUCCESS;
  event_rows.reset();
  ObSqlString sql;
  if (OB_FAIL(sql.assign_fmt("SELECT time_to_usec(gmt_create) AS create_time, "
          "module, event, name1, value1 FROM %s",
          OB_ALL_ROOTSERVICE_EVENT_HISTORY_TNAME))) {
    LOG_WARN("sql assign_fmt failed", K(ret));
  } else {
    ObMySQLProxy::MySQLResult res;
    ObMySQLResult *result = NULL;
    if (OB_FAIL(proxy_->read(res, sql.ptr()))) {
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute sql failed", K(sql), K(ret));
    } else {
      while (OB_SUCC(ret)) {
        EventTableRowInfo row;
        if (OB_FAIL(result->next())) {
          if (OB_ITER_END != ret) {
            LOG_WARN("result next failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_FAIL(build_event_info(*result, row))) {
          LOG_WARN("build event info failed", K(ret));
        } else if (OB_FAIL(event_rows.push_back(row))) {
          LOG_WARN("push back event row failed", K(ret));
        }
      }
    }
  }
  return ret;
}

int FakeEventHistoryTableOperator::build_event_info(const ObMySQLResult &res,
    EventTableRowInfo &row)
{
  int ret = OB_SUCCESS;
  row.alloc();

  //It is only used to fill out parameters, and it does not work. It is necessary to ensure that there is no '\0' character in the middle of the corresponding string
  int64_t tmp_real_str_len = 0;
  EXTRACT_CREATE_TIME_FIELD_MYSQL(res, "create_time", row.create_time_, int64_t);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "module", row.module_, MAX_ROOTSERVICE_EVENT_DESC_LENGTH,
                             tmp_real_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "event", row.event_, MAX_ROOTSERVICE_EVENT_DESC_LENGTH,
                             tmp_real_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "name1", row.name1_, MAX_ROOTSERVICE_EVENT_NAME_LENGTH,
                             tmp_real_str_len);
  EXTRACT_STRBUF_FIELD_MYSQL(res, "value1", row.value1_, MAX_ROOTSERVICE_EVENT_VALUE_LENGTH,
                             tmp_real_str_len);
  (void) tmp_real_str_len;
  return ret;
}

int FakeEventHistoryTableOperator::process_task(const common::ObString &sql, const bool is_delete)
{
  UNUSED(sql);
  UNUSED(is_delete);
  while (!stopped_) {
    sleep(1);
  }
  return OB_SUCCESS;
}

class TestEventHistoryTableOperator : public ::testing::Test
{
public:
  TestEventHistoryTableOperator() {}
  virtual ~TestEventHistoryTableOperator() {}
  virtual void SetUp();
  virtual void TearDown();
  virtual void assert_row(EventTableRowInfo &src, EventTableRowInfo &dst);
protected:
  DBInitializer db_initer_;
  ObAddr self_;
  FakeEventHistoryTableOperator fake_operator_;
};

void TestEventHistoryTableOperator::assert_row(EventTableRowInfo &src,
    EventTableRowInfo &dst)
{
  ASSERT_STREQ(src.module_, dst.module_);
  ASSERT_STREQ(src.event_, dst.event_);
  ASSERT_STREQ(src.name1_, dst.name1_);
  ASSERT_STREQ(src.value1_, dst.value1_);
}

void TestEventHistoryTableOperator::SetUp()
{
  int ret = OB_SUCCESS;
  self_ = A;
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(false));
  ret = fake_operator_.init(db_initer_.get_sql_proxy(), self_);
  ASSERT_EQ(OB_SUCCESS, ret);
}

void TestEventHistoryTableOperator::TearDown()
{
  fake_operator_.destroy();
  ROOTSERVICE_EVENT_INSTANCE.destroy();
}

TEST_F(TestEventHistoryTableOperator, test_select_data)
{
  int ret = OB_SUCCESS;
  ret = ROOTSERVICE_EVENT_INSTANCE.init(db_initer_.get_sql_proxy(), self_);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ROOTSERVICE_EVENT_INSTANCE.init(db_initer_.get_sql_proxy(), self_);
  ASSERT_EQ(OB_INIT_TWICE, ret);

  //insert one row;
  struct EventTableRowInfo row;
  row.alloc();
  row.make_row("module", "event", "name1", "value1");
  ROOTSERVICE_EVENT_ADD(row.module_, row.event_,
      row.name1_, row.value1_);
  sleep(2);
  ObArray<EventTableRowInfo> rows;
  ASSERT_EQ(OB_SUCCESS, fake_operator_.get(rows));
  assert_row(row, rows.at(0));
}

TEST_F(TestEventHistoryTableOperator, test_delete_data)
{
  ASSERT_EQ(OB_SUCCESS, ROOTSERVICE_EVENT_INSTANCE.init(db_initer_.get_sql_proxy(), self_));
  const int64_t event_count = 5;
  for (int64_t i = 0; i < event_count; ++i) {
    ObSqlString sql;
    int64_t affected_rows = 0;
    ASSERT_EQ(OB_SUCCESS, sql.assign_fmt("INSERT INTO __all_rootservice_event_history (gmt_create, "
        "module, event) values (usec_to_time(%ld), 'fake', 'fake')",
        ObTimeUtility::current_time() - ObEventHistoryTableOperator::RS_EVENT_HISTORY_DELETE_TIME));
    ASSERT_EQ(OB_SUCCESS, db_initer_.sql_proxy_.write(sql.ptr(), affected_rows));
    ASSERT_EQ(1, affected_rows);
    sleep(2);
  }
  sleep(2);
  ASSERT_EQ(OB_SUCCESS, ROOTSERVICE_EVENT_INSTANCE.async_delete());
  sleep(3);
  ObArray<EventTableRowInfo> rows;
  ASSERT_EQ(OB_SUCCESS, fake_operator_.get(rows));
  ASSERT_EQ(0, rows.size());
}

TEST_F(TestEventHistoryTableOperator, push_many_task)
{
  const int64_t event_count = ObEventHistoryTableOperator::TASK_QUEUE_SIZE * 2;
  for (int64_t i = 0; i < event_count; ++i) {
    char module[64];
    int64_t n = snprintf(module, sizeof(module), "%ld", i);
    ASSERT_TRUE(n > 0 && n < static_cast<int64_t>(sizeof(module)));
    if (i <= ObEventHistoryTableOperator::TASK_QUEUE_SIZE) {
      ASSERT_EQ(OB_SUCCESS, fake_operator_.add_event(module, "fake"));
    } else {
      ASSERT_EQ(OB_SIZE_OVERFLOW, fake_operator_.add_event(module, "fake"));
    }
  }
}

}//end namespace rootserver
}//end namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("ERROR");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

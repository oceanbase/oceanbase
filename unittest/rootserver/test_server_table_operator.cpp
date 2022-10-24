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
#include "gtest/gtest.h"
#include "../share/schema/db_initializer.h"
#include "lib/stat/ob_session_stat.h"
#include "rootserver/ob_server_table_operator.h"
#include "lib/time/ob_time_utility.h"
#include "share/config/ob_server_config.h"
#include "share/ob_lease_struct.h"
#include "server_status_builder.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver
{
class TestServerTabbleOperator : public ::testing::Test
{
public:
  TestServerTabbleOperator() {}
  virtual ~TestServerTabbleOperator() {}
  virtual void SetUp();
  virtual void TearDown() {}
protected:
  void check_result(const ObIArray<ObServerStatus> &statuses,
                    const ObIArray<ObServerStatus> &cmp_statuses);
  DBInitializer db_initer_;
  ObServerTableOperator st_operator_;
};

void TestServerTabbleOperator::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());

  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(false));
}

void TestServerTabbleOperator::check_result(const ObIArray<ObServerStatus> &statuses,
                                            const ObIArray<ObServerStatus> &cmp_statuses)
{
  ASSERT_EQ(statuses.count(), cmp_statuses.count());
  for (int64_t i = 0; i < statuses.count(); ++i) {
    ASSERT_EQ(statuses.at(i).zone_, cmp_statuses.at(i).zone_);
    ASSERT_EQ(statuses.at(i).server_, cmp_statuses.at(i).server_);
    ASSERT_EQ(statuses.at(i).sql_port_, cmp_statuses.at(i).sql_port_);
    ASSERT_EQ(statuses.at(i).register_time_, cmp_statuses.at(i).register_time_);
    //ASSERT_EQ(statuses.at(i).last_hb_time_, cmp_statuses.at(i).last_hb_time_);
    ASSERT_EQ(statuses.at(i).admin_status_, cmp_statuses.at(i).admin_status_)
        << "left: " <<  to_cstring(statuses.at(i)) << "right: " << to_cstring(cmp_statuses.at(i));
    ASSERT_EQ(statuses.at(i).hb_status_, cmp_statuses.at(i).hb_status_);
    ASSERT_EQ(statuses.at(i).get_display_status(), cmp_statuses.at(i).get_display_status());
    ASSERT_EQ(statuses.at(i).block_migrate_in_time_, cmp_statuses.at(i).block_migrate_in_time_);
    ASSERT_EQ(statuses.at(i).start_service_time_, cmp_statuses.at(i).start_service_time_);
    ASSERT_EQ(statuses.at(i).with_rootserver_, cmp_statuses.at(i).with_rootserver_);
    ASSERT_EQ(statuses.at(i).with_partition_, cmp_statuses.at(i).with_partition_);
    ASSERT_STREQ(statuses.at(i).build_version_, cmp_statuses.at(i).build_version_);
  }
}

TEST_F(TestServerTabbleOperator, common)
{
  ObServerTableOperator not_init_st_operator;
  ASSERT_EQ(OB_SUCCESS, st_operator_.init(db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_INIT_TWICE, st_operator_.init(db_initer_.get_sql_proxy()));
  ObServerStatusBuilder builder;
  ASSERT_EQ(OB_SUCCESS, builder.init(db_initer_.get_config()));
  const int64_t now = ObTimeUtility::current_time();

  ObAddr invalid_server;
  ObServerStatus invalid_status;
  ObAddr A(ObAddr::IPV4, "127.0.0.1", 4444);
  ObAddr B(ObAddr::IPV4, "127.0.0.1", 5555);
  ObZone zone = "test";
  builder.add(ObServerStatus::OB_SERVER_INACTIVE, now, A, zone)
      .add(ObServerStatus::OB_SERVER_ACTIVE, now, B, zone);
  ObServerManager::ObServerStatusArray &server_statuses = builder.get_server_statuses();
  server_statuses.at(0).block_migrate_in_time_ = 0;
  server_statuses.at(1).block_migrate_in_time_ = now;
  server_statuses.at(0).start_service_time_ = 0;
  server_statuses.at(1).start_service_time_ = now;

  // add two server
  ASSERT_EQ(OB_NOT_INIT, not_init_st_operator.update(server_statuses.at(0)));
  ASSERT_EQ(OB_INVALID_ARGUMENT, st_operator_.update(invalid_status));
  invalid_status.server_ = A; // server valid, but status still not valid
  ASSERT_EQ(OB_INVALID_ARGUMENT, st_operator_.update(invalid_status));
  ASSERT_EQ(OB_SUCCESS, st_operator_.update(server_statuses.at(0)));
  // udpate again shoud success two
  ASSERT_EQ(OB_SUCCESS, st_operator_.update(server_statuses.at(0)));
  ASSERT_EQ(OB_SUCCESS, st_operator_.update(server_statuses.at(1)));

  // get
  ObArray<ObServerStatus> got_server_statuses;
  ASSERT_EQ(OB_NOT_INIT, not_init_st_operator.get(got_server_statuses));
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  for(int64_t i = 0;i < got_server_statuses.count(); i++) {
    LOG_WARN("print get server status", K(got_server_statuses.at(i)));
  }
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(got_server_statuses.at(0).is_migrate_in_blocked());
  ASSERT_TRUE(got_server_statuses.at(1).is_migrate_in_blocked());
  ASSERT_FALSE(got_server_statuses.at(0).in_service());
  ASSERT_TRUE(got_server_statuses.at(1).in_service());
  ASSERT_FALSE(this->HasFatalFailure());

  // reset rootserver and get
  got_server_statuses.reset();
  server_statuses.at(0).with_rootserver_ = true;
  ASSERT_EQ(OB_SUCCESS, st_operator_.update(server_statuses.at(0)));
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(this->HasFatalFailure());
  ASSERT_EQ(OB_SUCCESS, st_operator_.reset_rootserver(B));
  server_statuses.at(0).with_rootserver_ = false;
  got_server_statuses.reset();
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(this->HasFatalFailure());

  // remove one
  common::ObMySQLTransaction trans;
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));
  ASSERT_EQ(OB_NOT_INIT, not_init_st_operator.remove(B, trans));
  ASSERT_EQ(OB_INVALID_ARGUMENT, st_operator_.remove(invalid_server, trans));
  ASSERT_EQ(OB_SUCCESS, st_operator_.remove(B, trans));
  server_statuses.pop_back();
  got_server_statuses.reset();
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(this->HasFatalFailure());

  // update status and get
  ASSERT_EQ(OB_SUCCESS, trans.start(&db_initer_.get_sql_proxy()));   
  ObServerStatus &As = server_statuses.at(0);
  ASSERT_EQ(A, As.server_);
  ASSERT_EQ(OB_NOT_INIT, not_init_st_operator.update_status(
      A, ObServerStatus::OB_SERVER_DELETING, As.last_hb_time_, trans));
  ASSERT_EQ(OB_INVALID_ARGUMENT, st_operator_.update_status(
      invalid_server, ObServerStatus::OB_SERVER_DELETING, As.last_hb_time_, trans));
  As.admin_status_ = ObServerStatus::OB_SERVER_ADMIN_DELETING;
  ASSERT_EQ(OB_SUCCESS, st_operator_.update_status(
      A, ObServerStatus::OB_SERVER_DELETING, As.last_hb_time_, trans));
  got_server_statuses.reset();
  ASSERT_EQ(OB_SUCCESS, trans.end(true));
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(this->HasFatalFailure());

  // set && clear with_partition flag
  ASSERT_EQ(OB_SUCCESS, st_operator_.update_with_partition(As.server_, true));
  As.with_partition_ = true;
  got_server_statuses.reset();
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(this->HasFatalFailure());

  ASSERT_EQ(OB_SUCCESS, st_operator_.update_with_partition(A, false));
  ASSERT_EQ(OB_SUCCESS, st_operator_.update_with_partition(B, false));
  As.with_partition_ = false;
  got_server_statuses.reset();
  ASSERT_EQ(OB_SUCCESS, st_operator_.get(got_server_statuses));
  check_result(server_statuses, got_server_statuses);
  ASSERT_FALSE(this->HasFatalFailure());
}

}//end namespace rootserver
}//end namesapce oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

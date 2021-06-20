/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software
 * according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *
 * http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A
 * PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "lib/stat/ob_diagnose_info.h"
#include "lib/stat/ob_session_stat.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "lib/random/ob_random.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/partition_table/ob_partition_table_operator.h"
#define private public
#include "observer/virtual_table/ob_all_virtual_sys_event.h"
#include "observer/virtual_table/ob_all_virtual_sys_stat.h"
#include "observer/virtual_table/ob_all_virtual_session_stat.h"
#include "observer/virtual_table/ob_all_virtual_session_event.h"
#include "observer/virtual_table/ob_all_virtual_session_wait.h"
#include "observer/virtual_table/ob_all_virtual_session_wait_history.h"
#include "observer/virtual_table/ob_all_latch.h"
#undef private

#include "share/partition_table/fake_part_property_getter.h"
#include "share/schema/db_initializer.h"
#include "share/schema/ob_schema_test_utils.cpp"
#include "rootserver/fake_rs_list_change_cb.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::host;
using namespace share::schema;

namespace observer {

class MockObAllVirtualSysStat : public oceanbase::observer::ObAllVirtualSysStat {
public:
  virtual int get_next_row(common::ObNewRow*& row);
};

class MockObAllLatch : public oceanbase::observer::ObAllLatch {
public:
  virtual int get_next_row(common::ObNewRow*& row);
  virtual int set_ip(common::ObAddr* addr);

private:
  common::ObString ipstr_;
  int32_t port_;
};

int MockObAllVirtualSysStat::get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  ObObj* cells = cur_row_.cells_;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else {
    const int64_t col_count = output_column_ids_.count();

    if (0 == iter_ && 0 == stat_iter_) {
      if (OB_SUCCESS != (ret = set_ip(addr_))) {
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else if (OB_SUCCESS != (ret = get_all_diag_info())) {
        SERVER_LOG(WARN, "Fail to get tenant status", K(ret));
      }
    }

    if (iter_ >= tenant_dis_.count()) {
      ret = OB_ITER_END;
    }

    if (OB_SUCCESS == ret && 0 == stat_iter_) {
      tenant_id_ = tenant_dis_.at(iter_).first;
    }

    if (OB_SUCC(ret)) {
      uint64_t cell_idx = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
        uint64_t col_id = output_column_ids_.at(i);
        switch (col_id) {
          case TENANT_ID: {
            cells[cell_idx].set_int(tenant_id_);
            break;
          }
          case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(port_);
            break;
          }
          case STATISTIC: {
            if (stat_iter_ < ObStatEventIds::STAT_EVENT_ADD_END) {
              cells[cell_idx].set_int(stat_iter_);
            } else {
              cells[cell_idx].set_int(stat_iter_ - 1);
            }
            break;
          }
          case VALUE: {
            if (stat_iter_ < ObStatEventIds::STAT_EVENT_ADD_END) {
              ObStatEventAddStat* stat = tenant_dis_.at(iter_).second->get_add_stat_stats().get(stat_iter_);
              if (NULL == stat) {
                ret = OB_INVALID_ARGUMENT;
                SERVER_LOG(WARN, "The argument is invalid, ", K(stat_iter_), K(ret));
              } else {
                cells[cell_idx].set_int(stat->stat_value_);
              }
            } else {
              ObStatEventSetStat* stat = tenant_dis_.at(iter_).second->get_set_stat_stats().get(
                  stat_iter_ - ObStatEventIds::STAT_EVENT_ADD_END - 1);
              if (NULL == stat) {
                ret = OB_INVALID_ARGUMENT;
                SERVER_LOG(WARN, "The argument is invalid, ", K(stat_iter_), K(ret));
              } else {
                cells[cell_idx].set_int(stat->stat_value_);
              }
            }
            break;
          }
          case STAT_ID: {
            cells[cell_idx].set_int(OB_STAT_EVENTS[stat_iter_].stat_id_);
            break;
          }
          case NAME: {
            cells[cell_idx].set_varchar(OB_STAT_EVENTS[stat_iter_].name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case CLASS: {
            cells[cell_idx].set_int(OB_STAT_EVENTS[stat_iter_].stat_class_);
            break;
          }
          case CAN_VISIBLE: {
            cells[cell_idx].set_bool(OB_STAT_EVENTS[stat_iter_].can_visible_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }
        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
    }

    if (OB_SUCC(ret)) {
      stat_iter_++;
      row = &cur_row_;
      if (ObStatEventIds::STAT_EVENT_ADD_END == stat_iter_) {
        stat_iter_++;
      }
      if (stat_iter_ >= ObStatEventIds::STAT_EVENT_SET_END) {
        stat_iter_ = 0;
        iter_++;
      }
    }
  }
  return ret;
}

int MockObAllLatch::set_ip(common::ObAddr* addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int MockObAllLatch::get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(addr_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "Some variable is null", K_(allocator), K_(addr), K(ret));
  } else {
    if (0 == iter_ && 0 == latch_iter_) {
      ret = get_all_diag_info();
      if (OB_FAIL(ret)) {
        SERVER_LOG(WARN, "Fail to get tenant status", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (iter_ >= tenant_dis_.count()) {
        ret = OB_ITER_END;
      }
    }

    if (OB_SUCC(ret)) {
      ObObj* cells = cur_row_.cells_;
      std::pair<uint64_t, common::ObDiagnoseTenantInfo*> dipair;
      ObLatchStat* latch_stat = NULL;
      ObString ipstr;
      if (OB_ISNULL(cells)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
      } else if (OB_FAIL(tenant_dis_.at(iter_, dipair))) {
        SERVER_LOG(WARN, "Fail to get tenant dis", K_(iter), K(ret));
      } else if (latch_iter_ >= ObLatchIds::LATCH_END) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "The latch iter exceed", K_(latch_iter), K(ret));
      } else if (OB_SUCCESS != (ret = set_ip(addr_))) {
        SERVER_LOG(WARN, "can't get ip", K(ret));
      } else {
        latch_stat = &dipair.second->get_latch_stats().items_[latch_iter_];
      }

      for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < output_column_ids_.count(); ++cell_idx) {
        const uint64_t column_id = output_column_ids_.at(cell_idx);
        switch (column_id) {
          case TENANT_ID: {
            cells[cell_idx].set_int(dipair.first);
            break;
          }
          case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(addr_->get_port());
            break;
          }
          case LATCH_ID: {
            cells[cell_idx].set_int(OB_LATCHES[latch_iter_].latch_id_);
            break;
          }
          case NAME: {
            cells[cell_idx].set_varchar(OB_LATCHES[latch_iter_].latch_name_);
            cells[cell_idx].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
            break;
          }
          case ADDR: {
            cells[cell_idx].set_null();
            break;
          }
          case LEVEL: {
            cells[cell_idx].set_int(0);
            break;
          }
          case HASH: {
            cells[cell_idx].set_int(0);
            break;
          }
          case GETS: {
            cells[cell_idx].set_int(latch_stat->gets_);
            break;
          }
          case MISSES: {
            cells[cell_idx].set_int(latch_stat->misses_);
            break;
          }
          case SLEEPS: {
            cells[cell_idx].set_int(latch_stat->sleeps_);
            break;
          }
          case IMMEDIATE_GETS: {
            cells[cell_idx].set_int(latch_stat->immediate_gets_);
            break;
          }
          case IMMEDIATE_MISSES: {
            cells[cell_idx].set_int(latch_stat->immediate_misses_);
            break;
          }
          case SPIN_GETS: {
            cells[cell_idx].set_int(latch_stat->spin_gets_);
            break;
          }
          case WAIT_TIME: {
            cells[cell_idx].set_int(latch_stat->wait_time_);
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(cell_idx), K_(output_column_ids), K(ret));
            break;
          }
        }
      }

      if (OB_SUCC(ret)) {
        row = &cur_row_;
        if (++latch_iter_ >= ObLatchIds::LATCH_END) {
          latch_iter_ = 0;
          iter_++;
        }
      }
    }
  }
  return ret;
}

class TestDIPerf : public ::testing::Test {
public:
  TestDIPerf();
  virtual ~TestDIPerf()
  {}
  virtual void SetUp();
  virtual void TearDown()
  {}

protected:
  DBInitializer db_initer_;
  ObSchemaServiceSQLImpl schema_service_;
  ObMultiVersionSchemaService multi_schema_service_;
  FakePartPropertyGetter prop_getter_;
  ObPartitionTableOperator pt_;
  ObArenaAllocator allocator1_;
  MockObAllVirtualSysStat sysstat_;
  oceanbase::observer::ObAllVirtualSysEvent sysevent_;
  oceanbase::observer::ObAllVirtualSessionEvent sesevent_;
  oceanbase::observer::ObAllVirtualSessionStat sesstat_;
  oceanbase::observer::ObAllVirtualSessionWait seswait_;
  oceanbase::observer::ObAllVirtualSessionWaitHistory seswaithistory_;
  MockObAllLatch latch_;
};

TestDIPerf::TestDIPerf()
    : db_initer_(),
      schema_service_(),
      multi_schema_service_(),
      prop_getter_(),
      pt_(prop_getter_),
      allocator1_(ObModIds::TEST)
{}

void TestDIPerf::SetUp()
{
  ASSERT_EQ(OB_SUCCESS, db_initer_.init());
  ObTenantManager::get_instance().init();
  ObKVGlobalCache::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, db_initer_.create_system_table(false));
  ASSERT_EQ(OB_SUCCESS, pt_.init(db_initer_.get_sql_proxy(), NULL));

  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.init(&db_initer_.get_sql_proxy(),
          &db_initer_.get_config(),
          OB_MAX_VERSION_COUNT,
          OB_MAX_VERSION_COUNT_FOR_MERGE,
          false /* with timestamp */));

  int ret = OB_SUCCESS;
  ObTenantSchema tenant_schema;
  tenant_schema.set_tenant_id(OB_SYS_TENANT_ID);
  tenant_schema.set_tenant_name(OB_SYS_TENANT_NAME);
  tenant_schema.set_locality("");
  tenant_schema.add_zone("zone");
  CREATE_TENANT(ret, tenant_schema);
  ASSERT_EQ(OB_SUCCESS, schema_service_.init(&db_initer_.get_sql_proxy()));
  for (int64_t i = 0; OB_SUCC(ret) && NULL != core_table_schema_creators[i]; ++i) {
    ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, (*share::core_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::sys_table_schema_creators[i]; ++i) {
    ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, (*share::sys_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::virtual_table_schema_creators[i]; ++i) {
    ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, (*share::virtual_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::information_schema_table_schema_creators[i]; ++i) {
    ObTableSchema table_schema;
    table_schema.reset();
    ASSERT_EQ(OB_SUCCESS, (*share::information_schema_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  for (int64_t i = 0; OB_SUCC(ret) && NULL != share::mysql_table_schema_creators[i]; ++i) {
    ObTableSchema table_schema;
    ASSERT_EQ(OB_SUCCESS, (*share::mysql_table_schema_creators[i])(table_schema));
    ObSchemaTestUtils::table_set_tenant(table_schema, OB_SYS_TENANT_ID);
    CREATE_USER_TABLE_SCHEMA(ret, table_schema);
  }

  ASSERT_EQ(OB_SUCCESS, multi_schema_service_.refresh_and_add_schema());
  sysevent_.set_allocator(&allocator1_);
  seswait_.set_allocator(&allocator1_);
  sysstat_.set_allocator(&allocator1_);
  sesevent_.set_allocator(&allocator1_);
  sesstat_.set_allocator(&allocator1_);
  seswaithistory_.set_allocator(&allocator1_);
  latch_.set_allocator(&allocator1_);
  ObArray<uint64_t> column_ids;
  const ObTableSchema* table = NULL;
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSSTAT_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, sysstat_.set_output_column_ids(column_ids));
  sysstat_.set_reserved_column_cnt(column_ids.count());
  sysstat_.set_table_schema(table);
  sysstat_.open();

  column_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SYSTEM_EVENT_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, sysevent_.set_output_column_ids(column_ids));
  sysevent_.set_reserved_column_cnt(column_ids.count());
  sysevent_.set_table_schema(table);
  sysevent_.open();

  column_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_EVENT_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, sesevent_.set_output_column_ids(column_ids));
  sesevent_.set_reserved_column_cnt(column_ids.count());
  sesevent_.set_table_schema(table);
  sesevent_.open();

  column_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSTAT_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, sesstat_.set_output_column_ids(column_ids));
  sesstat_.set_reserved_column_cnt(column_ids.count());
  sesstat_.set_table_schema(table);
  sesstat_.open();

  column_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, seswait_.set_output_column_ids(column_ids));
  seswait_.set_reserved_column_cnt(column_ids.count());
  seswait_.set_table_schema(table);
  seswait_.open();

  column_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(
          combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_SESSION_WAIT_HISTORY_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, seswaithistory_.set_output_column_ids(column_ids));
  seswaithistory_.set_reserved_column_cnt(column_ids.count());
  seswaithistory_.set_table_schema(table);
  seswaithistory_.open();

  column_ids.reuse();
  ASSERT_EQ(OB_SUCCESS,
      multi_schema_service_.get_table_schema(combine_id(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_LATCH_TID), table));
  ASSERT_TRUE(NULL != table);
  for (int64_t i = 0; i < table->get_column_count(); ++i) {
    const ObColumnSchemaV2* column = const_cast<ObTableSchema*>(table)->get_column_schema_by_idx(i);
    ASSERT_TRUE(NULL != column);
    ASSERT_EQ(OB_SUCCESS, column_ids.push_back(column->get_column_id()));
  }
  ASSERT_EQ(OB_SUCCESS, latch_.set_output_column_ids(column_ids));
  latch_.set_reserved_column_cnt(column_ids.count());
  latch_.set_table_schema(table);
  latch_.open();
}

TEST_F(TestDIPerf, common)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObNewRow* row;
  common::ObAddr addr;
  ObDITenantCache* caches[640];
  void* buf = NULL;
  uint64_t cnt = 0;
  ObDITenantCollect* collect;
  for (uint64_t i = 0; i < 640; i++) {
    if (NULL == (buf = allocator.alloc(sizeof(ObDITenantCache)))) {
    } else {
      caches[i] = new (buf) ObDITenantCache();
      for (uint64_t j = 1; j < 24; j++) {
        caches[i]->get_node(j, collect);
        collect->base_value_.update_stat(::oceanbase::common::ObStatEventIds::RPC_PACKET_IN, 1);
        collect->base_value_.update_stat(::oceanbase::common::ObStatEventIds::ELECTION_LEADER_REVOKE_COUNT, 1);
        collect->base_value_.get_event_stats()
            .get(::oceanbase::common::ObWaitEventIds::DB_FILE_DATA_READ)
            ->total_waits_++;
        collect->base_value_.get_event_stats()
            .get(::oceanbase::common::ObWaitEventIds::DB_FILE_DATA_READ)
            ->total_timeouts_++;
        collect->base_value_.get_event_stats()
            .get(::oceanbase::common::ObWaitEventIds::REMOVE_PARTITION_WAIT)
            ->total_waits_++;
        collect->base_value_.get_event_stats()
            .get(::oceanbase::common::ObWaitEventIds::REMOVE_PARTITION_WAIT)
            ->total_timeouts_++;
      }
    }
  }
  for (uint64_t i = 1; i < 100000; i++) {
    ObSessionStatEstGuard guard(1, i);
    EVENT_ADD(RPC_PACKET_IN, 1);
    for (uint64_t j = 0; j < 10; j++) {
      WAIT_BEGIN(OMT_IDLE, 0, 0, 0, 0);
      usleep(1);
      WAIT_END(OMT_IDLE);
    }
  }
  {
    // sysstat
    cnt = 0;
    sysstat_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == sysstat_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan sysstat time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  {
    // sysevent
    cnt = 0;
    sysevent_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == sysevent_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan sysevent time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  {
    // session_wait_history
    cnt = 0;
    seswaithistory_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == seswaithistory_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan session_wait_history time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  {
    // session_wait
    cnt = 0;
    seswait_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == seswait_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan session_wait time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  {
    // session_event
    cnt = 0;
    sesevent_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == sesevent_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan session_event time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  {
    // session_stat
    cnt = 0;
    sesstat_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == sesstat_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan session_stat time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  {
    // latch
    cnt = 0;
    latch_.set_addr(addr);
    uint64_t begin_time = ::oceanbase::common::ObTimeUtility::current_time();
    while (1) {
      if (OB_ITER_END == latch_.get_next_row(row)) {
        break;
      } else {
        cnt++;
      }
    }
    uint64_t end_time = ::oceanbase::common::ObTimeUtility::current_time();
    printf("scan latch time: %ld us, row cnt: %ld\n", (end_time - begin_time), cnt);
    allocator1_.reset();
  }
  oceanbase::common::ObDITls<ObSessionDIBuffer>::get_instance()->get_tenant_cache().~ObDITenantCache();
}

}  // namespace observer
}  // namespace oceanbase

int main(int argc, char** argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

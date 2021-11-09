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

#define USING_LOG_PREFIX OBLOG_FETCHER

#include <gtest/gtest.h>
#include "share/ob_define.h"
#define private public
#include "liboblog/src/ob_log_all_svr_cache.h"
#include "liboblog/src/ob_log_systable_helper.h"
#include "ob_log_utils.h"
#include "test_ob_log_fetcher_common_utils.h"
#include "lib/atomic/ob_atomic.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;

namespace oceanbase
{
namespace unittest
{
class TestObLogAllSvrCache: public ::testing::Test
{
public :
  virtual void SetUp() {}
  virtual void TearDown() {}
public :
  static const int64_t ALLSVR_CACHE_UPDATE_INTERVAL = 10 * _MSEC_;
};

static const int64_t SERVER_COUNT = 120;
static const int64_t FIRST_QUERY_RECORD_COUNT = 60;
static const int64_t VARY_RECORD_COUNT = 6;

typedef IObLogSysTableHelper::AllServerRecordArray AllServerRecordArray;
typedef IObLogSysTableHelper::AllServerRecord AllServerRecord;
AllServerRecord all_server_records[SERVER_COUNT];
const char *zones[4] = {"z1", "z2", "z3", "z4"};
const char *regions[4] = {"hz", "sh", "sz", "sh"};
const char *zone_types[4] = {"ReadWrite", "ReadWrite", "ReadWrite", "ReadOnly"};

void generate_data()
{
	int ret = OB_SUCCESS;
	ObString ip_str = "127.0.0.1";

	for(int64_t idx = 0; idx < SERVER_COUNT; idx++) {
		AllServerRecord &record = all_server_records[idx];
		int64_t pos = 0;
		if (OB_FAIL(databuff_printf(record.svr_ip_, sizeof(record.svr_ip_), pos,
				"%.*s", ip_str.length(), ip_str.ptr()))) {
			LOG_ERROR("save ip address fail", K(ret), K(pos),
								"buf_size", sizeof(record.svr_ip_), K(ip_str));
		}
		record.svr_port_ = static_cast<int32_t>(idx + 8000);
		int64_t index = idx % 4;
		switch (index) {
			case 0:
				record.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_INACTIVE;
				break;
			case 1:
				record.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_ACTIVE;
				break;
			case 2:
				record.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_DELETING;
				break;
			case 3:
				record.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_ACTIVE;
				break;
			default:
				break;
		}
    if (OB_FAIL(record.zone_.assign(zones[index]))) {
			LOG_ERROR("record zone assign fail", K(ret), K(record));
    }
	}
}

// To test if the cached __all_server system table data is cached correctly, the following dynamic policy is used for the returned data.
// Assume FIRST_QUERY_RECORD_COUNT=60, VARY_RECORD_COUNT=6
// 1. First query returns: records from 0 to 59
// 2. Second query returns: 6 new rows, i.e. 0 to 65 rows, 60 to 65 rows added
// 3. Third query returns: Decrease the first 6 rows, i.e. return 6 to 65 rows, decrease 0 to 5 rows
// ...
// and so on, until the end, the final validation result 60~119
class MockSysTableHelper1 : public IObLogSysTableHelper
{
public:
	MockSysTableHelper1() : query_time_(1),
												 start_index_(0),
												 end_index_(FIRST_QUERY_RECORD_COUNT - 1),
	                       is_invariable_(false) {}
	virtual ~MockSysTableHelper1() {}

public:
  int query_with_multiple_statement(BatchSQLQuery &batch_query)
  {
    UNUSED(batch_query);
    return 0;
  }

  /// Query __all_clog_history_info_v2 based on log_id to get all servers with service log IDs greater than or equal to log_id logs
  virtual int query_clog_history_by_log_id(
    const common::ObPartitionKey &pkey,
    const uint64_t log_id,
    ClogHistoryRecordArray &records)
	{
		UNUSED(pkey);
		UNUSED(log_id);
		UNUSED(records);

		return 0;
	}

  /// Query __all_clog_history_info_v2 for all servers with timestamp greater than or equal to timestamp log based on timestamp
  virtual int query_clog_history_by_tstamp(
    const common::ObPartitionKey &pkey,
    const int64_t timestamp,
    ClogHistoryRecordArray &records)
	{
		UNUSED(pkey);
		UNUSED(timestamp);
		UNUSED(records);

		return 0;
	}

  /// Query __all_meta_table / __all_root_table to get information about the servers that are serving the partition
  virtual int query_meta_info(
    const common::ObPartitionKey &pkey,
    MetaRecordArray &records)
	{
		UNUSED(pkey);
		UNUSED(records);

		return 0;
	}

  // Query __all_meta_table / __all_root_table for leader information
  virtual int query_leader_info(
    const common::ObPartitionKey &pkey,
    bool &has_leader,
    common::ObAddr &leader)
	{
		UNUSED(pkey);
		UNUSED(has_leader);
		UNUSED(leader);

		return 0;
	}

  /// Query __all_server table for all active server information
  virtual int query_all_server_info(AllServerRecordArray &records)
	{
		int ret = OB_SUCCESS;

		// The first query returns records from 0 to FIRST_QUERY_RECORD_COUNT-1
		if (1 == query_time_) {
			start_index_ = 0;
			end_index_ = FIRST_QUERY_RECORD_COUNT - 1;
		} else {
			if (is_invariable_) {                      // Return records no longer change
				// do nothing
			} else if (0 == (query_time_ & 0x01)) {    // ADD record
				if (end_index_ + VARY_RECORD_COUNT >= SERVER_COUNT) {
					ATOMIC_STORE(&is_invariable_, true);
				} else {
				  end_index_ += VARY_RECORD_COUNT;
				}
			} else if (1 == (query_time_ & 0x01)) {    // minus records
				start_index_ += VARY_RECORD_COUNT;
			}
		}

		// make records
		for (int64_t idx = start_index_; OB_SUCC(ret) && idx <= end_index_; idx++) {
		  AllServerRecord &record = all_server_records[idx];
			if (OB_FAIL(records.push_back(record))) {
				LOG_ERROR("records push error", K(ret), K(record));
			}
		}
		LOG_INFO("query all server info", K(query_time_), K(start_index_),
				                              K(end_index_), K(is_invariable_));
		query_time_++;

		return ret;
	}

  virtual int query_all_zone_info(AllZoneRecordArray &records)
  {
		UNUSED(records);

    return 0;
  }

  virtual int query_all_zone_type(AllZoneTypeRecordArray &records)
  {
    int ret = OB_SUCCESS;

    for (int64_t idx = 0; idx < 4; ++idx) {
      AllZoneTypeRecord record;
      record.zone_type_ = str_to_zone_type(zone_types[idx]);
      if (OB_FAIL(record.zone_.assign(zones[idx]))) {
        LOG_ERROR("record assign zone error", K(ret), K(record));
      } else if (OB_FAIL(records.push_back(record))) {
        LOG_ERROR("records push error", K(ret), K(record));
      }
    }
    return ret;
  }

  virtual int query_cluster_info(ClusterInfo &cluster_info)
  {
		UNUSED(cluster_info);

    return 0;
  }

  virtual int query_cluster_min_observer_version(uint64_t &min_observer_version)
  {
		UNUSED(min_observer_version);

    return 0;
  }

  virtual int reset_connection()
  {
    return 0;
  }

  virtual int query_timezone_info_version(const uint64_t tenant_id,
      int64_t &timezone_info_version)
  {
    UNUSED(tenant_id);
    UNUSED(timezone_info_version);
    return 0;
  }
public:
	int64_t query_time_;
	int64_t start_index_;
	int64_t end_index_;
	bool is_invariable_;
};

class MockSysTableHelper2 : public IObLogSysTableHelper
{
public:
	MockSysTableHelper2() : query_time_(1),
												  start_index_(0),
												  end_index_(FIRST_QUERY_RECORD_COUNT - 1) {}
	virtual ~MockSysTableHelper2() {}

public:
  virtual int query_with_multiple_statement(BatchSQLQuery &batch_query)
  {
    UNUSED(batch_query);
    return 0;
  }

  /// Query __all_clog_history_info_v2 based on log_id to get all servers with service log IDs greater than or equal to log_id logs
  virtual int query_clog_history_by_log_id(
    const common::ObPartitionKey &pkey,
    const uint64_t log_id,
    ClogHistoryRecordArray &records)
	{
		UNUSED(pkey);
		UNUSED(log_id);
		UNUSED(records);

		return 0;
	}

  /// Query __all_clog_history_info_v2 for all servers with timestamp greater than or equal to timestamp log based on timestamp
  virtual int query_clog_history_by_tstamp(
    const common::ObPartitionKey &pkey,
    const int64_t timestamp,
    ClogHistoryRecordArray &records)
	{
		UNUSED(pkey);
		UNUSED(timestamp);
		UNUSED(records);

		return 0;
	}

  /// Query __all_meta_table / __all_root_table to get information about the servers that are serving the partition
  virtual int query_meta_info(
    const common::ObPartitionKey &pkey,
    MetaRecordArray &records)
	{
		UNUSED(pkey);
		UNUSED(records);

		return 0;
	}

  // Query __all_meta_table / __all_root_table for leader information
  virtual int query_leader_info(
    const common::ObPartitionKey &pkey,
    bool &has_leader,
    common::ObAddr &leader)
	{
		UNUSED(pkey);
		UNUSED(has_leader);
		UNUSED(leader);

		return 0;
	}

  /// Query the __all_server table to get all active server information
	// First query: return a batch of servers, 1/3 of which are ACTIVE
	// Second query: return the servers returned in the first query, and the ACTIVE server status is changed to INACTIVE
  virtual int query_all_server_info(AllServerRecordArray &records)
	{
		int ret = OB_SUCCESS;

		// build records
		for (int64_t idx = start_index_; OB_SUCC(ret) && idx <= end_index_; idx++) {
		  AllServerRecord &record = all_server_records[idx];
			if (2 == query_time_) {
				// ACTIVE->INACTIVE
				if (1 == idx % 4) {
					record.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_INACTIVE;
				}
			}
			if (OB_FAIL(records.push_back(record))) {
				LOG_ERROR("records push error", K(ret), K(record));
			}
		}
		LOG_INFO("query all server info", K(query_time_), K(start_index_), K(end_index_));
		query_time_++;

		return ret;
	}

  virtual int query_all_zone_info(AllZoneRecordArray &records)
  {
		int ret = OB_SUCCESS;

    for (int64_t idx = 0; idx < 4; ++idx) {
      AllZoneRecord record;
      if (OB_FAIL(record.zone_.assign(zones[idx]))) {
        LOG_ERROR("record assign zone error", K(ret), K(record));
      } else if (OB_FAIL(record.region_.assign(regions[idx]))) {
        LOG_ERROR("record assign error", K(ret), K(record));
      } else if (OB_FAIL(records.push_back(record))) {
        LOG_ERROR("records push error", K(ret), K(record));
      }
    }

    return ret;
  }

  virtual int query_all_zone_type(AllZoneTypeRecordArray &records)
  {
    int ret = OB_SUCCESS;

    for (int64_t idx = 0; idx < 4; ++idx) {
      AllZoneTypeRecord record;
      record.zone_type_ = str_to_zone_type(zone_types[idx]);
      if (OB_FAIL(record.zone_.assign(zones[idx]))) {
        LOG_ERROR("record assign zone error", K(ret), K(record));
      } else if (OB_FAIL(records.push_back(record))) {
        LOG_ERROR("records push error", K(ret), K(record));
      }
    }
    return ret;
  }

  virtual int query_cluster_info(ClusterInfo &cluster_info)
  {
		UNUSED(cluster_info);

    return 0;
  }

  virtual int query_cluster_min_observer_version(uint64_t &min_observer_version)
  {
		UNUSED(min_observer_version);

    return 0;
  }

  virtual int reset_connection()
  {
    return 0;
  }
  virtual int query_timezone_info_version(const uint64_t tenant_id,
      int64_t &timezone_info_version)
  {
    UNUSED(tenant_id);
    UNUSED(timezone_info_version);
    return 0;
  }
public:
	int64_t query_time_;
	int64_t start_index_;
	int64_t end_index_;
};


////////////////////// Test of basic functions //////////////////////////////////////////
TEST_F(TestObLogAllSvrCache, init)
{
	generate_data();

	ObLogAllSvrCache all_svr_cache;
	MockSysTableHelper1 mock_systable_helper;
  MockFetcherErrHandler1 err_handler;

	// set update interval
	all_svr_cache.set_update_interval_(ALLSVR_CACHE_UPDATE_INTERVAL);

	EXPECT_EQ(OB_SUCCESS, all_svr_cache.init(mock_systable_helper, err_handler));
	while (false == ATOMIC_LOAD(&mock_systable_helper.is_invariable_)) {
		// do nothing
	}
	LOG_INFO("exit", K(mock_systable_helper.start_index_), K(mock_systable_helper.end_index_));

	/// verify result
	EXPECT_EQ(FIRST_QUERY_RECORD_COUNT, all_svr_cache.svr_map_.count());
	int64_t end_index = SERVER_COUNT - 1;
	int64_t start_index = end_index - FIRST_QUERY_RECORD_COUNT + 1;

	// Test servers in the __all_server table
	// Servers in the ACTIVE and DELETING states are serviceable
	// Servers in the INACTIVE state are not serviceable
	for (int64_t idx = start_index; idx <= end_index; idx++) {
		ObAddr svr(ObAddr::IPV4, all_server_records[idx].svr_ip_, all_server_records[idx].svr_port_);
		if (0 == idx % 4) {
			// INACTIVE/ENCRYPTION ZONE
			EXPECT_FALSE(all_svr_cache.is_svr_avail(svr));
		} else {
			// ACTIVE/DELETEING
			EXPECT_TRUE(all_svr_cache.is_svr_avail(svr));
		}
	}

	// test server not in __all_server table
  for (int64_t idx = 0; idx < start_index; idx++) {
		ObAddr svr(ObAddr::IPV4, all_server_records[idx].svr_ip_, all_server_records[idx].svr_port_);
		EXPECT_FALSE(all_svr_cache.is_svr_avail(svr));
	}

	all_svr_cache.destroy();
}

// state change from active to inactive
TEST_F(TestObLogAllSvrCache, all_svr_cache2)
{
	ObLogAllSvrCache all_svr_cache;
	MockSysTableHelper2 mock_systable_helper;
  MockFetcherErrHandler1 err_handler;

	// No threads open, manual assignment
	int ret = OB_SUCCESS;
  if (OB_FAIL(all_svr_cache.svr_map_.init(ObModIds::OB_LOG_ALL_SERVER_CACHE))) {
    LOG_ERROR("init svr map fail", K(ret));
  }
  if (OB_FAIL(all_svr_cache.zone_map_.init(ObModIds::OB_LOG_ALL_SERVER_CACHE))) {
    LOG_ERROR("init svr map fail", K(ret));
  }

	all_svr_cache.cur_version_ = 0;
	all_svr_cache.cur_zone_version_ = 0;
	all_svr_cache.err_handler_ = &err_handler;
  all_svr_cache.systable_helper_ = &mock_systable_helper;

  // update __all_zone
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.update_zone_cache_());

	// manual update and clearance
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.update_server_cache_());
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.purge_stale_records_());

	/// verify result
	EXPECT_EQ(FIRST_QUERY_RECORD_COUNT, all_svr_cache.svr_map_.count());
	int64_t start_index = 0;
	int64_t end_index = FIRST_QUERY_RECORD_COUNT - 1;

	for (int64_t idx = start_index; idx <= end_index; idx++) {
		ObAddr svr(ObAddr::IPV4, all_server_records[idx].svr_ip_, all_server_records[idx].svr_port_);
		if (1 == idx % 4) {
			EXPECT_TRUE(all_svr_cache.is_svr_avail(svr));
		}
	}

	// Second manual update and clearance
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.update_server_cache_());
	EXPECT_EQ(OB_SUCCESS, all_svr_cache.purge_stale_records_());

	// Verify that it is ACTIVE-INACTIVE
	for (int64_t idx = start_index; idx <= end_index; idx++) {
		ObAddr svr(ObAddr::IPV4, all_server_records[idx].svr_ip_, all_server_records[idx].svr_port_);
		if (1 == idx % 4) {
			EXPECT_FALSE(all_svr_cache.is_svr_avail(svr));
		}
	}

	all_svr_cache.destroy();
}

}//end of unittest
}//end of oceanbase

int main(int argc, char **argv)
{
  // ObLogger::get_logger().set_mod_log_levels("ALL.*:DEBUG, TLOG.*:DEBUG");
  // testing::InitGoogleTest(&argc,argv);
  // testing::FLAGS_gtest_filter = "DO_NOT_RUN";
  int ret = 1;
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_ob_log_all_svr_cache.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  testing::InitGoogleTest(&argc, argv);
  ret = RUN_ALL_TESTS();
  return ret;
}

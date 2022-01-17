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

#include <gtest/gtest.h>
#include <vector>
#include <algorithm>

//#include "lib/oblog/ob_log_module.h"
#include "share/ob_define.h"
#include "storage/ob_storage_log_type.h"
#include "storage/transaction/ob_trans_log.h"

#include "liboblog/src/ob_log_instance.h"
#include "ob_log_stream_worker.h"
#define private public
#include "ob_log_rpc.h"
#include "ob_log_utils.h"
#include "ob_log_systable_helper.h"

//#include "ob_log_part_fetch_ctx.h"
//#include "ob_log_fetcher_stream.h"

using namespace oceanbase;
using namespace common;
using namespace liboblog;
using namespace transaction;
using namespace storage;
//using namespace clog;
//using namespace fetcher;

namespace oceanbase
{
namespace unittest
{

/*
 * Utils.
 */
typedef std::vector<ObAddr> Svrs;
typedef std::vector<ObPartitionKey> PKeys;
typedef std::vector<uint64_t> LogIds;
typedef std::vector<int64_t> Tstamps;

class MockFetcherErrHandler1 : public IObLogErrHandler
{
public:
  virtual ~MockFetcherErrHandler1() { }
public:
  virtual void handle_error(const int err_no, const char *fmt, ...)
  {
    UNUSED(err_no);
    va_list ap;
    va_start(ap, fmt);
    //__E__(fmt, ap);
		//LOG_ERROR("test", fmt, ap);
    va_end(ap);
    abort();
  }
};

/*
 * SvrFinder
 *
 */
static const int64_t ALL_SERVER_COUNT   = 100;

static const int64_t QUERY_CLOG_HISTORY_VALID_COUNT   = 10;
static const int64_t QUERY_CLOG_HISTORY_INVALID_COUNT = 5;
static const int64_t QUERY_META_INFO_ADD_COUNT        = 6;

static const int64_t SVR_FINDER_REQ_NUM    = 10 * 1000;
static const int64_t LEADER_FINDER_REQ_NUM = 10 * 1000;

// Construct a request server to initiate asynchronous requests
// request server: query clog/query meta
// Request leader:
class MockSysTableHelperBase: public IObLogSysTableHelper
{
public:
	MockSysTableHelperBase() {}
	virtual ~MockSysTableHelperBase() {}

public:
  /// Query __all_clog_history_info_v2 based on log_id to get all servers with service log IDs greater than or equal to log_id logs
	/// Returns two types of logs: one for servers in the _all_server table, and one for servers not in the _all_server table
  virtual int query_clog_history_by_log_id(
    const common::ObPartitionKey &pkey,
    const uint64_t log_id,
    ClogHistoryRecordArray &records)
	{
    // Generate random results.
    int ret = OB_SUCCESS;

		UNUSED(pkey);
    records.reset();
    ClogHistoryRecord rec;

    int64_t valid_seed = static_cast<int64_t>(pkey.table_id_);
    int64_t invalid_seed = ALL_SERVER_COUNT;
		int64_t cnt = QUERY_CLOG_HISTORY_VALID_COUNT + QUERY_CLOG_HISTORY_INVALID_COUNT;

		for (int64_t idx = 0; idx < cnt; idx++) {
      rec.reset();
      rec.start_log_id_ = log_id;
      rec.end_log_id_ = log_id + 10000;
			if (idx < QUERY_CLOG_HISTORY_VALID_COUNT) {
				// Insert QUERY_CLOG_HISTORY_VALID_COUNT a valid record
      	snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1,
						     "127.0.0.%ld", valid_seed % ALL_SERVER_COUNT);
				valid_seed++;
			} else {
				// Insert QUERY_CLOG_HISTORY_INVALID_COUNT an invalid record
      	snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", invalid_seed);
				invalid_seed++;
			}
      rec.svr_port_ = 8888;

      records.push_back(rec);
		}

		return ret;
	}

  /// Query __all_clog_history_info_v2 for all servers with timestamp greater than or equal to timestamp log based on timestamp
  virtual int query_clog_history_by_tstamp(
    const common::ObPartitionKey &pkey,
    const int64_t timestamp,
    ClogHistoryRecordArray &records)
	{
    // Generate random results.
    int ret = OB_SUCCESS;

		UNUSED(timestamp);

    records.reset();
    ClogHistoryRecord rec;

    int64_t valid_seed = static_cast<int64_t>(pkey.table_id_);
    int64_t invalid_seed = ALL_SERVER_COUNT;
		int64_t cnt = QUERY_CLOG_HISTORY_VALID_COUNT + QUERY_CLOG_HISTORY_INVALID_COUNT;

		for (int64_t idx = 0; idx < cnt; idx++) {
      rec.reset();
      rec.start_log_id_ = 0;
      rec.end_log_id_ = 65536;
			if (idx < QUERY_CLOG_HISTORY_VALID_COUNT) {
				// Insert QUERY_CLOG_HISTORY_VALID_COUNT a valid record
      	snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1,
						     "127.0.0.%ld", valid_seed % ALL_SERVER_COUNT);
				valid_seed++;
			} else {
				// Insert QUERY_CLOG_HISTORY_INVALID_COUNT an invalid record
      	snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", invalid_seed);
				invalid_seed++;
			}
      rec.svr_port_ = 8888;

      records.push_back(rec);
		}

    return ret;
	}

  /// Query __all_meta_table / __all_root_table to get information about the servers that are serving the partition
	// Add records: return a batch of servers to add to query_clog_history, add only those servers for which clog history does not exist
  virtual int query_meta_info(
    const common::ObPartitionKey &pkey,
    MetaRecordArray &records)
	{
    // Generate random results.
    int ret = OB_SUCCESS;

		UNUSED(pkey);
    records.reset();
		MetaRecord rec;

    int64_t seed = static_cast<int64_t>(pkey.table_id_);
		int64_t cnt = QUERY_CLOG_HISTORY_VALID_COUNT + QUERY_META_INFO_ADD_COUNT;

		for (int64_t idx = 0; idx < cnt; idx++) {
      rec.reset();
			if (idx < QUERY_CLOG_HISTORY_VALID_COUNT) {
				// Returns the same server as query_clog_history
      	snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1,
						     "127.0.0.%ld", seed % ALL_SERVER_COUNT);
			} else {
				// Return QUERY_META_INFO_ADD_COUNT additional records
      	snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1,
						     "127.0.0.%ld", seed % ALL_SERVER_COUNT);
			}
      rec.svr_port_ = 8888;
      rec.replica_type_ = REPLICA_TYPE_FULL;
			seed++;

      records.push_back(rec);
		}

    return ret;
	}

  // Query __all_meta_table / __all_root_table for leader information
  virtual int query_leader_info(
    const common::ObPartitionKey &pkey,
    bool &has_leader,
    common::ObAddr &leader)
	{
		int ret = OB_SUCCESS;

		UNUSED(pkey);
    has_leader = true;
    leader.set_ip_addr("127.0.0.1", 8888);

    return ret;
	}

  /// Query __all_server table for all active server information
  virtual int query_all_server_info(AllServerRecordArray &records)
	{
	  int ret = OB_SUCCESS;

		UNUSED(records);

		return ret;
	}

  virtual int query_all_zone_info(AllZoneRecordArray &records)
  {
		UNUSED(records);

    return 0;
  }

  virtual int query_cluster_info(ClusterInfo &cluster_info)
  {
		UNUSED(cluster_info);

    return 0;
  }
};

class MockSysTableHelperDerive1 : public MockSysTableHelperBase
{
public:
	MockSysTableHelperDerive1() {}
	virtual ~MockSysTableHelperDerive1() {}

public:
  /// Query the __all_server table to get all active server information
  /// The _all_server table has 100 servers in the range 127.0.0.1:8888 ~ 127.0.0.99:8888
  virtual int query_all_server_info(AllServerRecordArray &records)
	{
	  int ret = OB_SUCCESS;

		int64_t seed = 0;
		AllServerRecord rec;
		for(int64_t idx = 0; idx < ALL_SERVER_COUNT; idx++) {
      rec.reset();
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", seed);
      rec.svr_port_ = 8888;
			rec.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_ACTIVE;
      records.push_back(rec);
			seed++;
		}

		return ret;
	}
};

class MockSysTableHelperDerive2 : public MockSysTableHelperBase
{
public:
	MockSysTableHelperDerive2() {}
	virtual ~MockSysTableHelperDerive2() {}

public:
  /// Query the __all_server table to get all active server information
  /// The _all_server table has 100 servers in the range of 127.0.0.1:8888 ~ 127.0.0.20:8888
	// 1. 50 of them are ACTIVE
	// 2. 50 of them are INACTIVE
  virtual int query_all_server_info(AllServerRecordArray &records)
	{
	  int ret = OB_SUCCESS;

		int64_t seed = 0;
		AllServerRecord rec;
		for(int64_t idx = 0; idx < ALL_SERVER_COUNT; idx++) {
      rec.reset();
      snprintf(rec.svr_ip_, common::MAX_IP_ADDR_LENGTH + 1, "127.0.0.%ld", seed);
      rec.svr_port_ = 8888;
	 		if (0 == (idx & 0x01)) {
				rec.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_ACTIVE;
			} else {
				rec.status_ = share::ObServerStatus::DisplayStatus::OB_SERVER_INACTIVE;
			}

      records.push_back(rec);
			seed++;
		}

		return ret;
	}
};

class MockObLogRpcBase : public IObLogRpc
{
public:
	MockObLogRpcBase() {}
  virtual ~MockObLogRpcBase() { }

  // Request start log id based on timestamp
  virtual int req_start_log_id_by_tstamp(const common::ObAddr &svr,
      const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(res);
		UNUSED(timeout);

		return ret;
	}

  // Request Leader Heartbeat
  virtual int req_leader_heartbeat(const common::ObAddr &svr,
      const obrpc::ObLogLeaderHeartbeatReq &req,
      obrpc::ObLogLeaderHeartbeatResp &res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(res);
		UNUSED(timeout);

		return ret;
	}

  // Open a new stream
  // Synchronous RPC
  virtual int open_stream(const common::ObAddr &svr,
      const obrpc::ObLogOpenStreamReq &req,
      obrpc::ObLogOpenStreamResp &resp,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(resp);
		UNUSED(timeout);

		return ret;
	}

  // Stream based, get logs
  // Asynchronous RPC
  virtual int async_stream_fetch_log(const common::ObAddr &svr,
      const obrpc::ObLogStreamFetchLogReq &req,
      obrpc::ObLogExternalProxy::AsyncCB<obrpc::OB_LOG_STREAM_FETCH_LOG> &cb,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(req);
		UNUSED(cb);
		UNUSED(timeout);

		return ret;
	}
};

class MockObLogStartLogIdRpc : public MockObLogRpcBase
{
  typedef const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint::Param Param;
  typedef const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint::ParamArray ParamArray;
public:
	MockObLogStartLogIdRpc() :
      spec_err_(false),
      svr_err_(OB_SUCCESS),
      part_err_(OB_SUCCESS)
  {}
  virtual ~MockObLogStartLogIdRpc() { }

  void set_err(const int svr_err, const int part_err)
  {
    svr_err_ = svr_err;
    part_err_ = part_err;
    spec_err_ = true;
  }

  // Request start log id based on timestamp
	// 1. rpc always assumes success
	// 2. 10% chance of server internal error
	// 3. 30% probability that partition returns success (30%) when server succeeds,
	// 30% probability that start_log_id returns pkey-table_id with breakpoint information
  // 4. Support for external error codes
  virtual int req_start_log_id_by_tstamp(const common::ObAddr &svr,
      const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(timeout);

		res.reset();
    // Seed.
    int64_t seed = (get_timestamp());
		int64_t rand = (seed) % 100;
    bool svr_internal_err = (rand < 10);

    // Preferred use of the specified error code
    if (spec_err_) {
      res.set_err(svr_err_);
    } else if (svr_internal_err) {
		  res.set_err(OB_ERR_UNEXPECTED);
    }

    if (OB_SUCCESS == res.get_err()) {
      ParamArray &param_array = req.get_params();
    	for (int64_t idx = 0, cnt = param_array.count(); idx < cnt; ++idx) {
        Param &param = param_array[idx];
      	obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint::Result result;
      	result.reset();
      	result.start_log_id_ = param.pkey_.table_id_;

        if (spec_err_) {
          result.err_ = part_err_;
        } else {
          // 30% success, 30% break.
          rand = (idx + seed) % 100;
          bool succeed = (rand < 30);
          bool breakrpc = (30 <= rand) && (rand < 60);
          result.err_ = (succeed) ? OB_SUCCESS : ((breakrpc) ? OB_EXT_HANDLE_UNFINISH : OB_NEED_RETRY);
        }

      	// Break info is actually not returned.
      	EXPECT_EQ(OB_SUCCESS, res.append_result(result));
    	}
		}

		return ret;
	}

private:
  bool spec_err_;
  int svr_err_;
  int part_err_;
};

class MockObLogRpcDerived2 : public MockObLogRpcBase
{
  typedef obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint Req;
  typedef Req::Param Param;
  typedef Req::ParamArray ParamArray;
public:
	MockObLogRpcDerived2() : request_(NULL),
	                         start_pos_(0),
                           end_pos_(0),
	                         query_time_(0) {}

  virtual ~MockObLogRpcDerived2() {}

	int init(int64_t req_cnt)
	{
		int ret = OB_SUCCESS;

		if (OB_UNLIKELY(req_cnt <= 0)) {
			//LOG_ERROR("invalid_argument");
			ret = OB_INVALID_ARGUMENT;
		} else {
			request_ = new Req;
			request_->reset();
			start_pos_ = 0;
			end_pos_ = req_cnt - 1;
			query_time_ = 1;
		}

		return ret;
	}

	void destroy()
	{
		delete request_;
		start_pos_ = 0;
		end_pos_ = 0;
		query_time_ = 1;
	}

  // Request start log id based on timestamp
	// 1. rpc always assumes success, and no server internal error
	// 2. Each time the second half returns succ and the first half returns break info
  virtual int req_start_log_id_by_tstamp(const common::ObAddr &svr,
      const obrpc::ObLogReqStartLogIdByTsRequestWithBreakpoint& req,
      obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint& res,
      const int64_t timeout)
  {
	  int ret = OB_SUCCESS;
		UNUSED(svr);
		UNUSED(timeout);

		res.reset();
		int64_t mid_index = (end_pos_ - start_pos_ + 1) / 2;
		const ParamArray &param_array = req.get_params();

		if (1 == query_time_) {
            // No validation is required for the first query
            // Save the request parameters
			for (int64_t idx = 0, cnt = param_array.count(); idx < cnt; ++idx) {
				const Param &param = param_array[idx];
      	Param add_param;
      	add_param.reset(param.pkey_, param.start_tstamp_, param.break_info_);

      	if (OB_FAIL(request_->append_param(add_param))) {
          //LOG_ERROR("append param fail", K(ret), K(idx), K(add_param));
				}
			}
		} else {
			// Verify that it is the original request
			is_original_req(&req, start_pos_, end_pos_);
		}

		for (int64_t idx = 0, cnt = param_array.count(); idx < cnt; ++idx) {
			const Param &param = param_array[idx];
			obrpc::ObLogReqStartLogIdByTsResponseWithBreakpoint::Result result;

			if (idx < mid_index) {
				// First half returns break info
				result.reset();
				result.err_ = OB_EXT_HANDLE_UNFINISH;;
			  reset_break_info(result.break_info_, static_cast<uint32_t>(idx), idx + 100);
				result.start_log_id_ = OB_INVALID_ID;

				// dynamically update the break info of the corresponding parameter of the saved requeset, for subsequent verification
        Param &all_param = const_cast<Param &>(request_->params_[idx]);
			  reset_break_info(all_param.break_info_, static_cast<uint32_t>(idx), idx + 100);
			} else {
				// The second half returns success
				result.reset();
				result.err_ = OB_SUCCESS;
				result.start_log_id_ = param.pkey_.table_id_;
			}
		  EXPECT_EQ(OB_SUCCESS, res.append_result(result));
		}
		if (end_pos_ != 0) {
			end_pos_ = mid_index - 1;
		}
		query_time_++;

		return ret;
	}
private:
  void is_original_req(const Req *cur_req, int64_t start_pos, int64_t end_pos)
	{
		ParamArray all_param_array = request_->get_params();
		ParamArray cur_param_array = cur_req->get_params();

		for (int64_t idx = start_pos; idx <= end_pos; idx++) {
			Param all_param = all_param_array[idx];
			Param cur_param = cur_param_array[idx];
			// verify pkey, start_tstamp
			EXPECT_EQ(all_param.pkey_, cur_param.pkey_);
			EXPECT_EQ(all_param.start_tstamp_, cur_param.start_tstamp_);
			// verify BreakInfo
      const obrpc::BreakInfo all_breakinfo = all_param.break_info_;
      const obrpc::BreakInfo cur_breakinfo = cur_param.break_info_;
			EXPECT_EQ(all_breakinfo.break_file_id_, cur_breakinfo.break_file_id_);
			EXPECT_EQ(all_breakinfo.min_greater_log_id_, cur_breakinfo.min_greater_log_id_);
		}
	}

	void reset_break_info(obrpc::BreakInfo &break_info,
			                  uint32_t break_file_id,
												uint64_t min_greater_log_id)
	{
		break_info.break_file_id_ = break_file_id;
		break_info.min_greater_log_id_ = min_greater_log_id;
	}
private:
  Req *request_;
	int64_t start_pos_;
	int64_t end_pos_;
	int64_t query_time_;
};


}
}

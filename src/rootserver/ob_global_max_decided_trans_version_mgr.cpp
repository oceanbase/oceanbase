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

#include "ob_global_max_decided_trans_version_mgr.h"
#include "common/ob_clock_generator.h"
#include "observer/ob_server_struct.h"
#include "share/ob_cluster_version.h"
#include "share/ob_multi_cluster_util.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"

namespace oceanbase {
using namespace common;
using namespace share;
using namespace share::schema;
namespace rootserver {
ObGlobalMaxDecidedTransVersionMgr::ObGlobalMaxDecidedTransVersionMgr()
    : is_inited_(false),
      rpc_proxy_(NULL),
      pt_operator_(NULL),
      schema_service_(NULL),
      self_addr_(),
      global_max_decided_trans_version_(OB_INVALID_TIMESTAMP),
      start_ts_(OB_INVALID_TIMESTAMP),
      contributor_pkey_()
{}

ObGlobalMaxDecidedTransVersionMgr::~ObGlobalMaxDecidedTransVersionMgr()
{
  if (is_inited_) {
    stop();
    wait();
  }
}

class ObGlobalMaxDecidedTransVersionMgr::InsertPartitionFunctor {
public:
  explicit InsertPartitionFunctor(const common::ObPartitionKey& partition_key)
      : partition_key_(partition_key), ret_value_(common::OB_SUCCESS)
  {}
  ~InsertPartitionFunctor()
  {}

public:
  bool operator()(const common::ObAddr& leader, common::ObPartitionArray& partition_array)
  {
    if (OB_SUCCESS != (ret_value_ = partition_array.push_back(partition_key_))) {
      LOG_WARN("partition_array push_back failed", K(ret_value_), K(partition_key_), K(leader));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }

private:
  common::ObPartitionKey partition_key_;
  int ret_value_;

private:
  DISALLOW_COPY_AND_ASSIGN(InsertPartitionFunctor);
};

class ObGlobalMaxDecidedTransVersionMgr::QueryPartitionFunctor {
public:
  QueryPartitionFunctor(obrpc::ObSrvRpcProxy* rpc_proxy, const int64_t version)
      : saved_value_array_(),
        ret_value_(common::OB_SUCCESS),
        rpc_proxy_(rpc_proxy),
        last_max_decided_trans_version_(version)
  {}
  ~QueryPartitionFunctor()
  {}

public:
  bool operator()(const common::ObAddr& leader, common::ObPartitionArray& partition_array)
  {
    if (OB_SUCCESS != (ret_value_ = handle_partition_array_(leader, partition_array))) {
      LOG_WARN("handle_partition_array_ failed",
          K(ret_value_),
          K(leader),
          K(partition_array),
          K_(last_max_decided_trans_version));
    }
    return common::OB_SUCCESS == ret_value_;
  }
  int get_ret_value() const
  {
    return ret_value_;
  }
  int get_min(common::ObPartitionKey& pkey, int64_t& value) const;

private:
  class PartitionValue {
  public:
    PartitionValue(const common::ObPartitionKey& pkey, const int64_t value) : pkey_(pkey), value_(value)
    {}
    PartitionValue() : pkey_(), value_(0)
    {}
    ~PartitionValue()
    {}
    TO_STRING_KV(K_(pkey), K_(value));

  public:
    common::ObPartitionKey pkey_;
    int64_t value_;
  };

private:
  int handle_partition_array_(const common::ObAddr& leader, const common::ObPartitionArray& partition_array);
  common::ObSEArray<PartitionValue, 16> saved_value_array_;
  int ret_value_;
  obrpc::ObSrvRpcProxy* rpc_proxy_;
  int64_t last_max_decided_trans_version_;

private:
  DISALLOW_COPY_AND_ASSIGN(QueryPartitionFunctor);
};

int ObGlobalMaxDecidedTransVersionMgr::init(obrpc::ObSrvRpcProxy* rpc_proxy,
    share::ObPartitionTableOperator* pt_operator, share::schema::ObMultiVersionSchemaService* schema_service,
    const common::ObAddr& self_addr)
{
  int ret = OB_SUCCESS;
  static const int64_t global_max_decided_trans_version_thread_cnt = 1;

  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_ISNULL(rpc_proxy) || OB_ISNULL(pt_operator) || OB_ISNULL(schema_service) || !self_addr.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(rpc_proxy), KP(pt_operator), KP(schema_service), K(self_addr));
  } else if (OB_FAIL(create(global_max_decided_trans_version_thread_cnt, "GMDTVerMgr"))) {
    LOG_WARN("create global_max_decided_trans_version thread failed",
        K(ret),
        K(global_max_decided_trans_version_thread_cnt));
  } else {
    rpc_proxy_ = rpc_proxy;
    pt_operator_ = pt_operator;
    schema_service_ = schema_service;
    self_addr_ = self_addr;
    global_max_decided_trans_version_ = 0;
    start_ts_ = 0;
    is_inited_ = true;
  }
  LOG_INFO("ObGlobalMaxDecidedTransVersionMgr is inited", K(ret));
  return ret;
}

void ObGlobalMaxDecidedTransVersionMgr::run3()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("not init", K(ret));
  } else {
    LOG_INFO("ObGlobalMaxDecidedTransVersionMgr start");

    while (!stop_) {
      if (!cluster_version_before_2000_() && GCONF.enable_one_phase_commit) {
        const int64_t begin_ts = ObClockGenerator::getClock();
        ServerPartitionMap server_partition_map;
        if (OB_FAIL(server_partition_map.init(ObModIds::OB_HASH_BUCKET_SERVER_PARTITION_MAP))) {
          LOG_WARN("server_partition_map init failed", K(ret));
        } else if (OB_FAIL(construct_server_partition_map_(server_partition_map))) {
          LOG_WARN("construct_server_partition_map_ failed", K(ret));
        } else if (OB_FAIL(handle_each_partition_(server_partition_map))) {
          LOG_WARN("handle_each_partition_ failed", K(ret));
        }
        const int64_t cost_ts = ObClockGenerator::getClock() - begin_ts;
        LOG_INFO("ObGlobalMaxDecidedTransVersionMgr run once",
            K(ret),
            K(cost_ts),
            K(global_max_decided_trans_version_),
            K_(contributor_pkey));

        // Obtaining global_max_decided_trans_version_ for the first time after RS starts may fail,
        // In this scenario, if global_max_decided_trans_version_ is used to determine the delay, it will cause a false
        // alarm. Therefore, in this scenario, we use start_ts to determine whether to report delay ERROR
        const int64_t tmp_ts = (global_max_decided_trans_version_ != 0) ? global_max_decided_trans_version_ : start_ts_;
        const int64_t delay_err_threshold =
            (GCTX.is_primary_cluster() ? DELAY_ERROR_THRESHOLD : STANDBY_DELAY_ERROR_THRESHOLD);
        if (ObClockGenerator::getClock() - tmp_ts >= delay_err_threshold && GCONF.enable_one_phase_commit) {
          // one-phase commit open and lag behind too much, alarm
          LOG_ERROR("global_max_decided_trans_version_ delay too long, please attention!",
              K(delay_err_threshold),
              K(global_max_decided_trans_version_),
              K_(contributor_pkey));
        }
      }

      {
        ObThreadCondGuard guard(get_cond());
        if (!stop_) {
          if (OB_SUCC(ret)) {
            get_cond().wait(SUCCESS_INTERVAL_MS);
          } else {
            get_cond().wait(FAIL_INTERVAL_MS);
          }
        }
      }
    }

    LOG_INFO("ObGlobalMaxDecidedTransVersionMgr stop");
  }
}

int ObGlobalMaxDecidedTransVersionMgr::get_global_max_decided_trans_version(int64_t& trans_version) const
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObGlobalMaxDecidedTransVersionMgr is not inited", K(ret));
  } else {
    trans_version = global_max_decided_trans_version_;
  }
  return ret;
}

int ObGlobalMaxDecidedTransVersionMgr::construct_server_partition_map_(ServerPartitionMap& server_partition_map) const
{
  int ret = OB_SUCCESS;

  ObPartitionInfo partition_info;
  ObPartitionTableIterator pt_iter;
  ObAddr leader;
  ObPartitionKey partition_key;

  const bool ignore_row_checksum = false;
  if (OB_FAIL(pt_iter.init(*pt_operator_, *schema_service_, ignore_row_checksum))) {
    LOG_WARN("partition table iterator init failed", K(ret));
  } else {
    while (OB_SUCC(ret) && OB_SUCC(pt_iter.next(partition_info))) {
      bool is_restore = false;
      if (OB_FAIL(get_leader_addr_and_restore_state_(partition_info, leader, is_restore, partition_key))) {
        LOG_WARN("get_leader_server_ failed", K(ret), K(partition_key));
      } else if (is_restore) {
        // just skip restore partition
      } else if (OB_FAIL(construct_server_partition_map_(server_partition_map, leader, partition_key))) {
        LOG_WARN("construct_server_partition_map_ failed", K(ret), K(leader), K(partition_key));
      }
    }
    if (OB_ITER_END != ret) {
      LOG_WARN("fail to get next partition_info", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObGlobalMaxDecidedTransVersionMgr::get_leader_addr_and_restore_state_(const share::ObPartitionInfo& partition_info,
    common::ObAddr& leader, bool& is_restore, common::ObPartitionKey& partition_key) const
{
  int ret = OB_SUCCESS;
  is_restore = false;

  if (!partition_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(partition_info));
  } else {
    bool found = false;
    for (int64_t index = 0; !found && index < partition_info.replica_count(); index++) {
      const ObPartitionReplica& replica = partition_info.get_replicas_v2().at(index);
      if (replica.is_leader_by_election()) {
        leader = replica.server_;
        partition_key = common::ObPartitionKey(replica.table_id_, replica.partition_id_, replica.partition_cnt_);
        found = true;
      } else if (replica.is_restore_leader()) {
        is_restore = true;
        found = true;
        partition_key = common::ObPartitionKey(replica.table_id_, replica.partition_id_, replica.partition_cnt_);
      }
    }

    if (!found) {
      ret = OB_ENTRY_NOT_EXIST;
      LOG_WARN("partition leader not exist", K(ret), K(partition_info));
    }
  }

  return ret;
}

int ObGlobalMaxDecidedTransVersionMgr::construct_server_partition_map_(ServerPartitionMap& server_partition_map,
    const common::ObAddr& leader, const common::ObPartitionKey& partition_key) const
{
  int ret = OB_SUCCESS;

  common::ObPartitionArray tmp_partition_array;
  if (!leader.is_valid() || !partition_key.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(partition_key), K(leader));
  } else if (OB_FAIL(server_partition_map.get(leader, tmp_partition_array)) && OB_ENTRY_NOT_EXIST != ret) {
    LOG_WARN("server_partition_map get failed", K(ret), K(partition_key), K(leader));
  } else {
    if (OB_ENTRY_NOT_EXIST == ret) {
      if (OB_FAIL(server_partition_map.insert(leader, tmp_partition_array))) {
        LOG_WARN("server_partition_map insert failed", K(ret), K(partition_key), K(leader));
      }
    }

    if (OB_SUCCESS == ret) {
      InsertPartitionFunctor functor(partition_key);
      if (OB_SUCCESS != server_partition_map.operate(leader, functor)) {
        ret = functor.get_ret_value();
        LOG_WARN("insert partition functor operate failed", K(ret), K(partition_key), K(leader));
      }
    }
  }

  return ret;
}

int ObGlobalMaxDecidedTransVersionMgr::handle_each_partition_(ServerPartitionMap& server_partition_map)
{
  int ret = OB_SUCCESS;
  int64_t min_value = 0;
  ObPartitionKey pkey;
  QueryPartitionFunctor functor(rpc_proxy_, global_max_decided_trans_version_);
  if (0 == server_partition_map.count()) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != server_partition_map.for_each(functor)) {
    ret = functor.get_ret_value();
    LOG_WARN("for_each partition failed", K(ret));
  } else {
    (void)functor.get_min(pkey, min_value);
    if (min_value < global_max_decided_trans_version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("global_max_decided_trans_version_ change smaller, unexpected",
          K(ret),
          K(global_max_decided_trans_version_),
          K_(contributor_pkey),
          K(min_value),
          K(pkey));
    } else {
      global_max_decided_trans_version_ = min_value;
      contributor_pkey_ = pkey;
    }
  }

  return ret;
}

int ObGlobalMaxDecidedTransVersionMgr::QueryPartitionFunctor::handle_partition_array_(
    const common::ObAddr& leader, const common::ObPartitionArray& partition_array)
{
  int ret = OB_SUCCESS;

  const int64_t TIMEOUT = 1000 * 1000;  // 1s
  const int64_t MAX_PARTITION_CNT = 10000;
  obrpc::ObQueryMaxDecidedTransVersionRequest request;
  obrpc::ObQueryMaxDecidedTransVersionResponse result;

  for (int64_t index = 0; OB_SUCC(ret) && index < partition_array.count(); index++) {
    if (OB_FAIL(request.partition_array_.push_back(partition_array[index]))) {
      LOG_WARN("request partition_array push_back failed", K(ret), K(leader));
    }

    if (OB_SUCC(ret) && ((index + 1) % MAX_PARTITION_CNT == 0 || index == partition_array.count() - 1)) {
      request.last_max_decided_trans_version_ = last_max_decided_trans_version_;
      if (OB_FAIL(rpc_proxy_->to(leader).timeout(TIMEOUT).query_max_decided_trans_version(request, result)) ||
          (OB_SUCCESS != (ret = result.ret_value_))) {
        LOG_WARN("rpc query_max_decided_trans_version failed", K(ret), K(leader));
      } else if (OB_FAIL(saved_value_array_.push_back(PartitionValue(result.pkey_, result.trans_version_)))) {
        LOG_WARN("saved_value_array_ push_back failed", K(ret));
      } else {
        request.reset();
        result.reset();
      }
    }
  }

  return ret;
}

int ObGlobalMaxDecidedTransVersionMgr::QueryPartitionFunctor::get_min(ObPartitionKey& pkey, int64_t& value) const
{
  int64_t tmp_value = INT64_MAX;
  ObPartitionKey tmp_pkey;
  for (int64_t index = 0; index < saved_value_array_.count(); index++) {
    if (saved_value_array_.at(index).value_ < tmp_value) {
      tmp_value = saved_value_array_.at(index).value_;
      tmp_pkey = saved_value_array_.at(index).pkey_;
    }
  }
  pkey = tmp_pkey;
  value = tmp_value;
  return OB_SUCCESS;
}

int ObGlobalMaxDecidedTransVersionMgr::start()
{
  int ret = OB_SUCCESS;
  start_ts_ = ObClockGenerator::getClock();
  ret = ObRsReentrantThread::start();
  return ret;
}

void ObGlobalMaxDecidedTransVersionMgr::stop()
{
  ObRsReentrantThread::stop();
}

void ObGlobalMaxDecidedTransVersionMgr::wait()
{
  ObRsReentrantThread::wait();
  // This thread may be started repeatedly (RS switch), so it is necessary to reset the state after a start and stop,
  // and reset global_max_decided_trans_version_ to 0
  global_max_decided_trans_version_ = 0;
  contributor_pkey_.reset();
  start_ts_ = 0;
}

bool ObGlobalMaxDecidedTransVersionMgr::cluster_version_before_2000_() const
{
  return GET_MIN_CLUSTER_VERSION() < CLUSTER_VERSION_2000;
}
}  // namespace rootserver
}  // namespace oceanbase

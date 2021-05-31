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

#include "ob_empty_server_checker.h"

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

#include "share/ob_srv_rpc_proxy.h"
#include "share/ob_multi_cluster_util.h"

#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/config/ob_server_config.h"

#include "observer/ob_server_struct.h"

#include "ob_server_manager.h"

namespace oceanbase {
namespace rootserver {
using namespace common;
using namespace obrpc;
using namespace share;

ObEmptyServerCheckRound::ObEmptyServerCheckRound(volatile bool& stop)
    : inited_(false),
      stop_(stop),
      version_(0),
      rpc_proxy_(NULL),
      server_mgr_(NULL),
      pt_operator_(NULL),
      schema_service_(NULL)
{}

ObEmptyServerCheckRound::~ObEmptyServerCheckRound()
{}

int ObEmptyServerCheckRound::init(ObServerManager& server_mgr, ObSrvRpcProxy& rpc_proxy,
    ObPartitionTableOperator& pt_operator, schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  const int64_t server_bucket_num = 1000;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(alive_servers_.create(hash::cal_next_prime(server_bucket_num), ObModIds::OB_RS_SERVER_MANAGER))) {
    LOG_WARN("create alive server hash map failed", K(ret), K(server_bucket_num));
  } else if (OB_FAIL(empty_servers_.create(hash::cal_next_prime(server_bucket_num), ObModIds::OB_RS_SERVER_MANAGER))) {
    LOG_WARN("create empty server hash map failed", K(ret), K(server_bucket_num));
  } else {
    server_mgr_ = &server_mgr;
    rpc_proxy_ = &rpc_proxy;
    pt_operator_ = &pt_operator;
    schema_service_ = &schema_service;
    inited_ = true;
  }
  return ret;
}

int ObEmptyServerCheckRound::check(ObThreadCond& cond)
{
  int ret = OB_SUCCESS;
  ObArray<ObServerStatus> all_servers;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    alive_servers_.clear();
    empty_servers_.clear();
    version_ = std::max(std::abs(version_) + 1, ObTimeUtility::current_time());
    LOG_INFO("start check empty server", K_(version));
  }
  // <1> get snapshot of all server status.
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(server_mgr_->get_server_statuses("", all_servers))) {
    LOG_WARN("get all sever status failed", K(ret));
  } else {
    const int64_t now = ObTimeUtility::current_time();
    FOREACH_X(s, all_servers, OB_SUCC(ret))
    {
      if (s->need_check_empty(now)) {
        if (OB_FAIL(empty_servers_.set_refactored(s->server_, s->last_hb_time_))) {
          LOG_WARN("add server to empty servers failed", K(ret));
        }
      } else if (s->is_alive()) {
        const int64_t finish_times = 0;
        if (OB_FAIL(alive_servers_.set_refactored(s->server_, finish_times))) {
          LOG_WARN("add server to alive server failed", K(ret));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (0 == empty_servers_.size()) {
        LOG_INFO("no server with partition and lease expired OB_MAX_ADD_MEMBER_TIMEOUT ago, "
                 "no need check empty",
            LITERAL_K(OB_MAX_ADD_MEMBER_TIMEOUT));
      }
    }

    if (OB_SUCC(ret) && empty_servers_.size() > 0) {
      // <2> send %sync_partition_table rpc to alive servers.

      // release %cond to make pt_sync_finish can be called before all rpc send.
      cond.unlock();
      FOREACH_X(s, all_servers, !stop_ && OB_SUCC(ret))
      {
        if (s->is_alive()) {
          if (OB_FAIL(rpc_proxy_->to(s->server_).timeout(GCONF.rpc_timeout).sync_partition_table(version_))) {
            LOG_WARN("request server sync partition table failed", K(ret), "server", s->server_, K_(version));
          }
        }
      }
      if (OB_SUCC(ret) && stop_) {
        ret = OB_CANCELED;
      }
      cond.lock();
      // <3> wait all alive servers sync partition table finish
      if (OB_SUCC(ret)) {
        if (OB_FAIL(wait_pt_sync_finish(cond))) {
          LOG_WARN("wait partition table sync finish failed", K(ret));
        }
      }
      if (OB_SUCC(ret)) {
        // reset version to avoid concurrent access of %alive_servers_
        version_ = -version_;
        cond.unlock();
        // <4> check whether has member on empty servers, and set with partition flag
        if (OB_FAIL(update_with_partition_flag())) {
          LOG_WARN("update with partition flag failed", K(ret));
        }
        cond.lock();
      }
    }
  }
  if (version_ > 0) {
    version_ = -version_;
  }

  return ret;
}

int ObEmptyServerCheckRound::pt_sync_finish(const common::ObAddr& server, const int64_t version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() || version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(server));
  } else if (version != version_) {
    LOG_INFO("version mismatch, ignore partition table sync finish request",
        K(server),
        K(version),
        "checker_version",
        version_);
  } else {
    int64_t finish_times = 0;
    if (OB_FAIL(alive_servers_.get_refactored(server, finish_times))) {
      LOG_WARN("get server from hash map failed", K(ret), K(server));
    } else {
      finish_times += 1;
      const int overwrite = 1;
      if (OB_FAIL(alive_servers_.set_refactored(server, finish_times, overwrite))) {
        LOG_WARN("over write server pt sync finish times failed", K(ret), K(server));
      }
    }
  }

  return ret;
}

int ObEmptyServerCheckRound::wait_pt_sync_finish(ObThreadCond& cond)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const int64_t begin_time_us = ObTimeUtility::current_time();
    while (OB_SUCC(ret)) {
      bool all_finish = true;
      if (stop_) {
        ret = OB_CANCELED;
      } else {
        FOREACH_X(s, alive_servers_, OB_SUCC(ret))
        {
          if (s->second < ObIPartitionTable::PARTITION_TABLE_REPORT_LEVELS) {
            all_finish = false;
            LOG_DEBUG("partition table not sync finish", "server", s->first);
            // if server offline, we end this check round
            bool alive = false;
            if (OB_FAIL(server_mgr_->check_server_alive(s->first, alive))) {
              LOG_WARN("check server alive failed", K(ret), "server", s->first);
            } else if (!alive) {
              ret = OB_SERVER_NOT_ALIVE;
              LOG_WARN("server lease expired, end this round", K(ret), "server", s->first);
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        if (all_finish) {
          break;
        }
        int64_t wait_time_ms = (begin_time_us + PT_SYNC_TIMEOUT - ObTimeUtility::current_time()) / 1000L;
        if (wait_time_ms <= 0) {
          ret = OB_TIMEOUT;
          LOG_WARN("wait partition table sync finish failed", K(ret), K(begin_time_us), LITERAL_K(PT_SYNC_TIMEOUT));
          FOREACH(s, alive_servers_)
          {
            if (s->second < ObIPartitionTable::PARTITION_TABLE_REPORT_LEVELS) {
              LOG_WARN("partition table still not sync finish",
                  K(ret),
                  "server",
                  s->first,
                  "finish_cnt",
                  s->second,
                  "need_count",
                  static_cast<int64_t>(ObIPartitionTable::PARTITION_TABLE_REPORT_LEVELS));
            }
          }
        } else {
          // check server alive per second.
          const int64_t max_wait_time_ms = 1000;  // 1 second
          wait_time_ms = std::min(wait_time_ms, max_wait_time_ms);
          if (OB_SUCCESS != cond.wait(static_cast<int32_t>(wait_time_ms))) {
            LOG_DEBUG("wait timeout", K(wait_time_ms));
          }
        }
      }
    }
  }
  return ret;
}

int ObEmptyServerCheckRound::update_with_partition_flag()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stop_) {
    ret = OB_CANCELED;
  } else {
    ObPartitionTableIterator iter;
    bool ignore_row_checksum = true;
    if (OB_FAIL(iter.init(*pt_operator_, *schema_service_, ignore_row_checksum))) {
      LOG_WARN("partition table iterator init failed", K(ret));
    } else {
      ObPartitionInfo partition;
      partition.reuse();
      while (!stop_ && OB_SUCC(ret) && empty_servers_.size() > 0) {
        if (OB_FAIL(iter.next(partition))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("iterate partition table failed", K(ret));
          }
          break;
        }
        ObIArray<ObPartitionReplica>& replica_array = partition.get_replicas_v2();
        const ObPartitionReplica* replica = NULL;
        // filter leader member_list
        if (OB_FAIL(partition.find_leader_by_election(replica))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_LEADER_NOT_EXIST;
          }
          LOG_WARN("find leader failed", K(ret), K(partition));
        } else if (OB_ISNULL(replica)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("NULL replica pointer", K(ret));
        } else {
          // check whether leader on alive servers
          int64_t finish_times = 0;
          if (OB_FAIL(alive_servers_.get_refactored(replica->server_, finish_times))) {
            LOG_WARN("partition leader not on partition table synced servers", K(ret), "replica", *replica);
          }
          // check whether has member on empty servers
          FOREACH_CNT_X(m, replica->member_list_, OB_SUCC(ret))
          {
            int64_t last_hb_time = 0;
            if (OB_FAIL(empty_servers_.get_refactored(m->server_, last_hb_time))) {
              if (OB_HASH_NOT_EXIST != ret) {
                LOG_WARN("get from hash map failed", K(ret), "server", m->server_);
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              LOG_INFO("partition has member on sever", K(partition), "server", m->server_);
              if (OB_FAIL(empty_servers_.erase_refactored(m->server_))) {
                LOG_WARN("erase item from hash map failed", K(ret), "server", m->server_);
              }
            }
          }  // end FORECAH member_list
        }
        // filter server of replicas
        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < replica_array.count() && OB_SUCC(ret); ++i) {
            int64_t last_hb_time = 0;
            if (OB_FAIL(empty_servers_.get_refactored(replica_array.at(i).server_, last_hb_time))) {
              if (OB_HASH_NOT_EXIST != ret) {
                LOG_WARN("get from hash map failed", K(ret), "server", replica_array.at(i).server_);
              } else {
                ret = OB_SUCCESS;
              }
            } else {
              LOG_INFO("this sever has partition", K(partition), "server", replica_array.at(i).server_);
              if (OB_FAIL(empty_servers_.erase_refactored(replica_array.at(i).server_))) {
                LOG_WARN("erase item from hash map failed", K(ret), "server", replica_array.at(i).server_);
              }
            }
          }  // end for
        }
      }  // end while
    }
  }
  if (OB_SUCC(ret)) {
    FOREACH_X(it, empty_servers_, !stop_ && OB_SUCC(ret))
    {
      LOG_INFO("try to clear server with partition flag", "server", it->first, "last_hb_time", it->second);
      if (OB_FAIL(server_mgr_->clear_with_partiton(it->first, it->second))) {
        LOG_WARN("clear server with partition flag failed", K(ret), "server", it->first, "last_hb_time", it->second);
      }
    }
  }
  if (OB_SUCC(ret) && stop_) {
    ret = OB_CANCELED;
  }
  return ret;
}

ObEmptyServerChecker::ObEmptyServerChecker() : inited_(false), need_check_(false), check_round_(stop_), cond_()
{}

ObEmptyServerChecker::~ObEmptyServerChecker()
{}

int ObEmptyServerChecker::init(ObServerManager& server_mgr, ObSrvRpcProxy& rpc_proxy,
    ObPartitionTableOperator& pt_operator, schema::ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;
  const int64_t empty_server_checker_thread_cnt = 1;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(cond_.init(ObWaitEventIds::EMPTY_SERVER_CHECK_COND_WAIT))) {
    LOG_WARN("fail to init thread cond, ", K(ret));
  } else if (OB_FAIL(check_round_.init(server_mgr, rpc_proxy, pt_operator, schema_service))) {
    LOG_WARN("check round init failed", K(ret));
  } else if (OB_FAIL(create(empty_server_checker_thread_cnt, "EmptSvrCheck"))) {
    LOG_WARN("create empty server checker thread failed", K(ret), K(empty_server_checker_thread_cnt));
  } else {
    inited_ = true;
  }
  return ret;
}

void ObEmptyServerChecker::run3()
{
  LOG_INFO("empty server checker start");
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (!stop_) {
      ret = OB_SUCCESS;
      ObThreadCondGuard guard(cond_);
      if (need_check_) {
        need_check_ = false;
        if (OB_FAIL(check_round_.check(cond_))) {
          LOG_WARN("empty server check round failed", K(ret));
        }
      }
      if (OB_SUCC(ret) && !stop_ && !need_check_) {
        const int wait_time_ms = 1000;
        if (OB_SUCCESS != cond_.wait(wait_time_ms)) {
          LOG_DEBUG("wait timeout", K(wait_time_ms));
        }
      }
    }
  }
  LOG_INFO("empty server checker stop");
}

void ObEmptyServerChecker::wakeup()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    cond_.broadcast();
  }
}

void ObEmptyServerChecker::stop()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObRsReentrantThread::stop();
    ObThreadCondGuard guard(cond_);
    cond_.broadcast();
  }
}

int ObEmptyServerChecker::notify_check()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObThreadCondGuard guard(cond_);
    need_check_ = true;
    cond_.broadcast();
  }
  return ret;
}

int ObEmptyServerChecker::pt_sync_finish(const common::ObAddr& server, const int64_t version)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!server.is_valid() && version <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(server), K(version));
  } else {
    ObThreadCondGuard guard(cond_);
    if (OB_FAIL(check_round_.pt_sync_finish(server, version))) {
      LOG_WARN("partition table sync finish failed", K(ret), K(server), K(version));
    } else {
      cond_.broadcast();
    }
  }
  return ret;
}

}  // end namespace rootserver
}  // end namespace oceanbase

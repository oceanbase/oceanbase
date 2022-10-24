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

#ifndef MOCK_OB_PARTITION_COMPONENT_FACTORY_H_
#define MOCK_OB_PARTITION_COMPONENT_FACTORY_H_

namespace oceanbase
{
namespace storage
{

class MockObIPartitionComponentFactory : public ObIPartitionComponentFactory
{
public:

  MOCK_METHOD1(get_ls,
               ObLS * (const uint64_t tenant_id));
  MOCK_METHOD1(get_partition,
               ObIPartitionGroup * (const uint64_t tenant_id));
  MOCK_METHOD0(get_ssstore,
               ObSSStore * ());
  MOCK_METHOD0(get_trans_service,
               transaction::ObTransService * ());
  MOCK_METHOD0(get_clog_mgr,
               clog::ObICLogMgr * ());
  MOCK_METHOD0(get_partition_service,
               ObPartitionService * ());
  MOCK_METHOD0(get_election_mgr,
               election::ObElectionMgr * ());
  MOCK_METHOD1(get_log_service,
               clog::ObIPartitionLogService * (const uint64_t tenant_id));
  MOCK_METHOD0(get_replay_engine_wrapper,
               clog::ObLogReplayEngineWrapper * ());

  MOCK_METHOD1(free,
               void(ObIPartitionGroup *partition));
  MOCK_METHOD1(free,
               void(ObReplayStatus *status));
  MOCK_METHOD1(free,
               void(ObSSStore *store));
  MOCK_METHOD1(free,
               void(transaction::ObTransService *txs));
  MOCK_METHOD1(free,
               void(clog::ObICLogMgr *clog_mgr));
  MOCK_METHOD1(free,
               void(ObPartitionService *ptt_service));
  MOCK_METHOD1(free,
               void(replayengine::ObILogReplayEngine *rp_eg));
  MOCK_METHOD1(free,
               void(oceanbase::election::ObIElectionMgr *election_mgr));
  MOCK_METHOD1(free,
               void(clog::ObIPartitionLogService *log_service));
  MOCK_METHOD1(free,
               void(clog::ObLogReplayEngineWrapper *rp_eg));
};

}  // namespace storage
}  // namespace oceanbase

#endif

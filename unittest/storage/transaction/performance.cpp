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

#include <gmock/gmock.h>
#include "../../clog/mock_ob_partition_log_service.h"
#include "../mockcontainer/mock_ob_partition_service.h"
#include "../mockcontainer/mock_ob_end_trans_callback.h"
#include "../mockcontainer/mock_ob_partition.h"
#include "../mockcontainer/mock_ob_trans_service.h"
#include "../mock_ob_partition_component_factory.h"
#define private public
#define protected public
#include "storage/transaction/ob_trans_service.h"
#include "storage/transaction/ob_trans_define.h"
#include "clog/ob_partition_log_service.h"
#include "lib/stat/ob_session_stat.h"
#include "share/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "common/ob_partition_key.h"
#include "storage/transaction/ob_trans_rpc.h"
#include "lib/objectpool/ob_resource_pool.h"
#include "lib/container/ob_array_iterator.h"
#include "sql/resolver/ob_stmt_type.h"
#include "sql/ob_sql_define.h"

namespace oceanbase {

using namespace common;
using namespace transaction;
using namespace storage;
using namespace share;
using namespace obrpc;
using namespace memtable;
using namespace sql;
using namespace clog;

namespace unittest {

LeaderActiveArg leader_active_arg;

// Mock transaction module ObRpcProxy associated with ObTransRpc.
// (1) Packaage sent does not go through the network communication.
// (2) OB_SUCCESS is returned for the function post_trans_msg.
class MockObTransRpc : public ObITransRpc, common::ObSimpleThreadPool {
public:
  MockObTransRpc() : trans_service_(NULL)
  {}
  virtual ~MockObTransRpc()
  {
    ObSimpleThreadPool::destroy();
  }
  int init(ObTransService* trans_service, const ObAddr& self)
  {
    int ret = OB_SUCCESS;
    UNUSED(self);

    if (OB_SUCCESS != (ret = ObSimpleThreadPool::init(get_cpu_num() / 2, 100000))) {
      TRANS_LOG(WARN, "ObSimpleThreadPool init error.", K(ret));
    } else {
      trans_service_ = trans_service;
    }

    return ret;
  }

  void handle(void* task)
  {
    int ret = OB_SUCCESS;

    TransRpcTask* rpc_task = static_cast<TransRpcTask*>(task);
    ObTransRpcResult result;

    if (NULL == trans_service_) {
      TRANS_LOG(WARN, "tranaction service is null");
    } else if (NULL == rpc_task) {
      TRANS_LOG(WARN, "tranaction rpc task is null");
    } else if (OB_SUCCESS != (ret = trans_service_->handle_trans_msg(rpc_task->get_msg(), result))) {
      TRANS_LOG(WARN, "handle transaction message error", K(ret));
    } else {
      TRANS_LOG(INFO, "transaction message handle success", K(rpc_task->get_msg()));
    }

    if (NULL != rpc_task) {
      op_reclaim_free(rpc_task);
      rpc_task = NULL;
    }
  }

  virtual int init(obrpc::ObTransRpcProxy* rpc_proxy, ObTransService* trans_service, const common::ObAddr& self)
  {
    UNUSED(rpc_proxy);
    UNUSED(trans_service);
    UNUSED(self);
    start();
    return OB_SUCCESS;
  }
  virtual int start()
  {
    return OB_SUCCESS;
  }
  virtual void stop()
  {}
  virtual void wait()
  {}
  virtual void destroy()
  {
    trans_service_ = NULL;
  }

  virtual int post_trans_msg(const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
  {
    UNUSED(server);
    UNUSED(msg_type);
    int ret = OB_SUCCESS;

    TransRpcTask* rpc_task = NULL;
    if (NULL == (rpc_task = op_reclaim_alloc(TransRpcTask))) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      if (OB_FAIL(rpc_task->init(msg, ObTransRetryTaskType::ERROR_MSG_TASK))) {
        TRANS_LOG(WARN, "rpc task init error", K(ret), "rpc_task", *rpc_task);
      } else if (OB_SUCCESS != (ret = push(rpc_task))) {
        TRANS_LOG(WARN, "push task error", K(ret));
        op_reclaim_free(rpc_task);
        rpc_task = NULL;
      } else {
        TRANS_LOG(DEBUG, "transaction message push success", K(msg));
      }
    }

    return ret;
  }
  virtual int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTransMsg& msg, const int64_t msg_type)
  {
    UNUSED(tenant_id);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = post_trans_msg(server, msg, msg_type))) {
      TRANS_LOG(WARN, "post transaction message error", K(ret));
    }

    return ret;
  }
  virtual int post_trans_msg(
      const uint64_t tenant_id, const common::ObAddr& server, const ObTrxMsgBase& msg, const int64_t msg_type)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    return OB_SUCCESS;
  }
  virtual int post_batch_msg(const uint64_t tenant_id, const ObAddr& server, const obrpc::ObIFill& msg,
      const int64_t msg_type, const ObPartitionKey& pkey)
  {
    UNUSED(tenant_id);
    UNUSED(server);
    UNUSED(msg);
    UNUSED(msg_type);
    UNUSED(pkey);
    return OB_SUCCESS;
  }
  virtual int post_trans_resp_msg(const uint64_t tenant_id, const ObAddr& server, const ObTransMsg& msg)
  {
    UNUSED(tenant_id);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = post_trans_msg(server, msg, msg.get_msg_type()))) {
      TRANS_LOG(WARN, "post transaction message error", K(ret));
    }

    return ret;
  }

private:
  ObTransService* trans_service_;
};

class MockObLocationAdapter : public ObILocationAdapter {
public:
  MockObLocationAdapter()
  {}
  ~MockObLocationAdapter()
  {}
  int init(ObIPartitionLocationCache* location_cache, share::schema::ObMultiVersionSchemaService* schema_service)
  {
    UNUSED(location_cache);
    UNUSED(schema_service);
    return OB_SUCCESS;
  }
  void destroy()
  {}
  int nonblock_get_strong_leader(const ObPartitionKey& partition, ObAddr& server)
  {
    UNUSED(partition);
    server = self_;
    return OB_SUCCESS;
  }
  int get_strong_leader(const ObPartitionKey& partition, ObAddr& server)
  {
    UNUSED(partition);
    server = self_;
    return OB_SUCCESS;
  }
  int nonblock_renew(const ObPartitionKey& partition, const int64_t expire_renew_time)
  {
    UNUSED(partition);
    UNUSED(expire_renew_time);
    return OB_SUCCESS;
  }
  int nonblock_get(const uint64_t table_id, const int64_t partition_id, share::ObPartitionLocation& location)
  {
    UNUSED(table_id);
    UNUSED(partition_id);
    UNUSED(location);
    return OB_SUCCESS;
  }
  void set_self(const ObAddr& self)
  {
    self_ = self;
  }

private:
  ObAddr self_;
};

struct MySubmitLogTask {
  MySubmitLogTask() : cb(NULL)
  {}
  ~MySubmitLogTask()
  {}
  ObITransSubmitLogCb* cb;
  ObPartitionKey partition;
};

class MockObClogAdapter : public ObIClogAdapter, public ObSimpleThreadPool {
public:
  MockObClogAdapter()
  {}
  ~MockObClogAdapter()
  {
    destroy();
  }
  int init(ObPartitionService* partition_service, const ObAddr& self)
  {
    int ret = OB_SUCCESS;

    UNUSED(partition_service);
    UNUSED(self);

    if (OB_SUCCESS != (ret = ObSimpleThreadPool::init(8, 100000))) {
      TRANS_LOG(WARN, "ObSimpleThreadPool init error", K(ret));
    }

    return ret;
  }

  virtual int init(storage::ObPartitionService* partition_service)
  {
    UNUSED(partition_service);
    return OB_SUCCESS;
  }

  virtual int get_log_id_timestamp(const ObPartitionKey& partition, const int64_t prepare_version,
      storage::ObIPartitionGroup* pg, ObLogMeta& log_meta)
  {
    UNUSED(partition);
    UNUSED(prepare_version);
    UNUSED(pg);
    UNUSED(log_meta);
    return OB_SUCCESS;
  }

  virtual int submit_log_id_alloc_task(const int64_t log_type, ObTransCtx* ctx)
  {
    UNUSED(log_type);
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  virtual int submit_rollback_trans_task(ObTransCtx* ctx)
  {
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  virtual int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, const uint64_t log_id, const int64_t ts, ObITransSubmitLogCb* cb)
  {
    UNUSED(version);
    UNUSED(log_id);
    UNUSED(ts);
    int ret = OB_SUCCESS;

    if (OB_FAIL(submit_log(partition, buff, size, cb))) {
      TRANS_LOG(WARN, "MockObClogAdapter submit log error", K(ret));
    }

    return ret;
  }

  virtual int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg, uint64_t& cur_log_id,
      int64_t& cur_log_timestamp)
  {
    UNUSED(version);
    UNUSED(cur_log_id);
    UNUSED(cur_log_timestamp);
    UNUSED(pg);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = submit_log(partition, buff, size, cb))) {
      TRANS_LOG(WARN, "MockObClogAdapter submit log error", K(ret));
    }

    return ret;
  }

  virtual int submit_log(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, const int64_t base_ts, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      uint64_t& cur_log_id, int64_t& cur_log_timestamp)
  {
    UNUSED(version);
    UNUSED(base_ts);
    UNUSED(pg);
    UNUSED(cur_log_id);
    UNUSED(cur_log_timestamp);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = submit_log(partition, buff, size, cb))) {
      TRANS_LOG(WARN, "MockObClogAdapter submit log error", K(ret));
    }

    return ret;
  }
  virtual int submit_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
      const char* buff, const int64_t size, ObITransSubmitLogCb* cb, storage::ObIPartitionGroup* pg,
      const int64_t base_ts, ObTransLogBufferAggreContainer& container)
  {
    UNUSED(partition);
    UNUSED(version);
    UNUSED(buff);
    UNUSED(size);
    UNUSED(cb);
    UNUSED(pg);
    UNUSED(base_ts);
    UNUSED(container);
    return OB_SUCCESS;
  }
  virtual int flush_aggre_log(const common::ObPartitionKey& partition, const common::ObVersion& version,
      storage::ObIPartitionGroup* pg, ObTransLogBufferAggreContainer& container)
  {
    UNUSED(partition);
    UNUSED(version);
    UNUSED(pg);
    UNUSED(container);
    return OB_SUCCESS;
  }

  int batch_submit_log(const transaction::ObTransID& trans_id, const common::ObPartitionArray& partition_array,
      const clog::ObLogInfoArray& log_info_array, const clog::ObISubmitLogCbArray& cb_array)
  {
    UNUSED(trans_id);
    UNUSED(partition_array);
    UNUSED(log_info_array);
    UNUSED(cb_array);
    return common::OB_SUCCESS;
  }

  int submit_log_task(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, const bool with_need_update_version, const int64_t local_trans_version,
      const bool with_base_ts, const int64_t base_ts, ObITransSubmitLogCb* cb)
  {
    UNUSED(version);
    UNUSED(with_need_update_version);
    UNUSED(local_trans_version);
    UNUSED(with_base_ts);
    UNUSED(base_ts);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = submit_log(partition, buff, size, cb))) {
      TRANS_LOG(WARN, "MockObClogAdapter submit log error", K(ret));
    }

    return ret;
  }

  int submit_log_task(const common::ObPartitionKey& partition, const common::ObVersion& version, const char* buff,
      const int64_t size, ObITransSubmitLogCb* cb)
  {
    UNUSED(version);
    int ret = OB_SUCCESS;

    if (OB_SUCCESS != (ret = submit_log(partition, buff, size, cb))) {
      TRANS_LOG(WARN, "MockObClogAdapter submit log error", K(ret));
    }

    return ret;
  }
  int start()
  {
    return OB_SUCCESS;
  }
  void stop()
  {}
  void wait()
  {}

  void destroy()
  {
    ObSimpleThreadPool::destroy();
  }

  int get_status(const common::ObPartitionKey& partition, const bool check_election, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows)
  {
    UNUSED(partition);
    UNUSED(check_election);
    UNUSED(changing_leader_windows);
    UNUSED(leader_epoch);
    clog_status = OB_SUCCESS;
    return OB_SUCCESS;
  }

  int get_status(storage::ObIPartitionGroup* partition, const bool check_election, int& clog_status,
      int64_t& leader_epoch, common::ObTsWindows& changing_leader_windows)
  {
    UNUSED(partition);
    UNUSED(check_election);
    UNUSED(changing_leader_windows);
    UNUSED(leader_epoch);
    clog_status = OB_SUCCESS;
    return OB_SUCCESS;
  }

  int get_status_unsafe(const common::ObPartitionKey& partition, int& clog_status, int64_t& leader_epoch,
      common::ObTsWindows& changing_leader_windows)
  {
    UNUSED(partition);
    UNUSED(changing_leader_windows);
    UNUSED(leader_epoch);
    clog_status = OB_SUCCESS;
    return OB_SUCCESS;
  }

  int submit_log(const ObPartitionKey& partition, const char* buff, const int64_t size, ObITransSubmitLogCb* cb)
  {
    UNUSED(buff);
    UNUSED(size);
    int ret = OB_SUCCESS;
    MySubmitLogTask* task = NULL;

    if (NULL == (task = op_reclaim_alloc(MySubmitLogTask))) {
      TRANS_LOG(WARN, "new MySubmitLogTask error");
      ret = OB_ERR_UNEXPECTED;
    } else {
      task->cb = cb;
      task->partition = partition;
      if (OB_SUCCESS != (ret = push(task))) {
        TRANS_LOG(WARN, "push task error", K(ret));
      } else {
        TRANS_LOG(DEBUG, "push task success");
      }
    }

    return ret;
  }

  virtual int backfill_nop_log(
      const common::ObPartitionKey& partition, storage::ObIPartitionGroup* pg, const ObLogMeta& log_meta)
  {
    UNUSED(partition);
    UNUSED(pg);
    UNUSED(log_meta);
    return OB_SUCCESS;
  }
  virtual int submit_backfill_nop_log_task(const common::ObPartitionKey& partition, const ObLogMeta& log_meta)
  {
    UNUSED(partition);
    UNUSED(log_meta);
    return OB_SUCCESS;
  }

  virtual int submit_big_trans_callback_task(const common::ObPartitionKey& partition, const int64_t log_type,
      const uint64_t log_id, const int64_t log_timestamp, ObTransCtx* ctx)
  {
    UNUSED(partition);
    UNUSED(log_type);
    UNUSED(log_id);
    UNUSED(log_timestamp);
    UNUSED(ctx);
    return OB_SUCCESS;
  }

  virtual int get_last_submit_timestamp(const common::ObPartitionKey& partition, int64_t& timestamp)
  {
    UNUSED(partition);
    UNUSED(timestamp);
    return OB_SUCCESS;
  }

  void handle(void* task)
  {
    static int64_t log_id;
    MySubmitLogTask* log_task = static_cast<MySubmitLogTask*>(task);
    clog::ObLogType log_type = clog::OB_LOG_SUBMIT;
    log_task->cb->on_success(log_task->partition, log_type, ++log_id, ObClockGenerator::getClock(), false, false);
    TRANS_LOG(DEBUG, "handle log task sucess");
    op_reclaim_free(log_task);
  }
  bool can_start_trans()
  {
    return true;
  }
};

class MyMockObPartitionLogService : public MockPartitionLogService {
public:
  int submit_log(const char* buff, const int64_t size, const storage::ObStorageLogType log_type,
      const common::ObVersion& version, ObITransSubmitLogCb* cb)
  {
    UNUSED(buff);
    UNUSED(cb);
    UNUSED(log_type);
    UNUSED(version);
    int ret = OB_SUCCESS;
    if (1 == size) {
      TRANS_LOG(WARN, "invalid argument", K(size));
      ret = OB_INVALID_ARGUMENT;
    } else {
      TRANS_LOG(INFO, "submit log success.");
    }
    return ret;
  }
};

class MyMockObPartitionComponentFactory : public MockObIPartitionComponentFactory {
public:
  virtual void free(ObIPartitionGroup* partition)
  {
    UNUSED(partition);
    // delete partition;
  }
};

class MyMockObPartition : public MockObIPartitionGroup {
public:
  MyMockObPartition() : partition_log_service_(NULL), pg_file_(NULL), pkey_(nullptr)
  {}
  virtual ~MyMockObPartition()
  {}
  void set_pkey(const ObPartitionKey* pkey)
  {
    pkey_ = const_cast<ObPartitionKey*>(pkey);
  }
  void set_log_service(ObIPartitionLogService* log_service)
  {
    partition_log_service_ = log_service;
  }
  virtual blocksstable::ObStorageFile* get_storage_file()
  {
    return pg_file_;
  }
  virtual const blocksstable::ObStorageFile* get_storage_file() const
  {
    return pg_file_;
  }
  virtual blocksstable::ObStorageFileHandle& get_storage_file_handle()
  {
    return file_handle_;
  }
  virtual int remove_election_from_mgr()
  {
    return OB_SUCCESS;
  }

  // get partition log service
  ObIPartitionLogService* get_log_service()
  {
    return partition_log_service_;
  }
  const ObPartitionKey& get_partition_key() const
  {
    return *pkey_;
  }

private:
  ObIPartitionLogService* partition_log_service_;
  blocksstable::ObStorageFile* pg_file_;
  blocksstable::ObStorageFileHandle file_handle_;
  ObPartitionKey* pkey_;
};

class MyMockObPartitionService : public MockObIPartitionService {
public:
  MyMockObPartitionService() : partition_(NULL)
  {
    partition_ = new MyMockObPartition();
    cp_fty_ = new MyMockObPartitionComponentFactory();
  }
  virtual ~MyMockObPartitionService()
  {
    if (NULL != partition_) {
      delete partition_;
      delete cp_fty_;
      partition_ = NULL;
      cp_fty_ = NULL;
    }
  }
  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroup*& partition) const
  {
    MyMockObPartitionLogService* log_service = NULL;
    int ret = OB_SUCCESS;
    if (!pkey.is_valid()) {
      partition_->set_pkey(&pkey);
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
      TRANS_LOG(WARN, "invalid argument, pkey is invalid.", K(pkey));
      ret = OB_INVALID_ARGUMENT;
    } else if (1 == pkey.table_id_) {
      TRANS_LOG(INFO, "get partition success.", K(pkey.table_id_));
      partition_->set_pkey(&pkey);
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
    } else if (2 == pkey.table_id_) {
      TRANS_LOG(WARN, "invalid argument, get partition error.", K(pkey.table_id_));
      partition_->set_pkey(&pkey);
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
      // ret = OB_ERR_UNEXPECTED;
    } else if (3 == pkey.table_id_) {
      TRANS_LOG(INFO, "get partition success.", K(pkey.table_id_));
      partition_->set_pkey(&pkey);
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
    } else if (4 == pkey.table_id_) {
      TRANS_LOG(INFO, "get partition success.", K(pkey.table_id_));
      partition_->set_pkey(&pkey);
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
    } else {
      partition_->set_pkey(&pkey);
      partition = partition_;
      partition_->set_log_service(new MyMockObPartitionLogService());
      TRANS_LOG(INFO, "test info", K(pkey));
      // do nothing.
    }
    if (NULL != log_service) {
      delete log_service;
      log_service = NULL;
    }

    return ret;
  }
  int get_partition(const common::ObPartitionKey& pkey, ObIPartitionGroupGuard& guard) const
  {
    int ret = common::OB_SUCCESS;
    ObIPartitionGroup* partition = NULL;
    if (OB_FAIL(get_partition(pkey, partition))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else {
      guard.set_partition_group(this->get_pg_mgr(), *partition);
    }
    return ret;
  }
  virtual int get_curr_member_list(const common::ObPartitionKey& pkey, common::ObMemberList& member_list) const
  {
    UNUSED(pkey);
    UNUSED(member_list);
    return OB_SUCCESS;
  }

private:
  MyMockObPartition* partition_;
};

// define local_ip
static const char* LOCAL_IP = "127.0.0.1";
static const int32_t PORT = 8080;
static const ObAddr::VER IP_TYPE = ObAddr::IPV4;

class TestPerformance : public ::testing::Test {
public:
  TestPerformance() : self_(IP_TYPE, LOCAL_IP, PORT), sp_trans_(false), access_mod_(ObTransAccessMode::UNKNOWN)
  {}
  virtual ~TestPerformance()
  {
    destroy();
  }
  virtual void SetUp()
  {
    init();
  }
  virtual void TearDown()
  {}
  int init();
  void destroy();

public:
  int smoke();
  int get_random_partition(ObPartitionLeaderArray& array);
  void set_sp_trans(const bool sp_trans)
  {
    sp_trans_ = sp_trans;
  }
  void set_access_mod(const int32_t access_mod)
  {
    access_mod_ = access_mod;
  }

private:
  int do_trans();

private:
  static const uint64_t TENANT_ID = 1234;
  static const int32_t PARTITION_COUNT_PER_TABLE = 5;
  static const int32_t MAX_TABLE = 10;
  static const int32_t TABLE_NUMBER_START = 20000;
  int generate_participant_arr_guard_(const ObPartitionArray& participants, ObIPartitionArrayGuard& pkey_guard_arr);

private:
  ObAddr self_;
  MockObTransRpc rpc_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  MockObLocationAdapter location_adapter_;
  MockObClogAdapter clog_adapter_;
  ObTransService trans_service_;
  MyMockObPartitionService partition_service_;
  ObPartitionArray partition_array_;
  ObAddrArray addr_array_;
  bool sp_trans_;
  int32_t access_mod_;
  ObLtsSource lts_source_;
  MockObTsMgr* ts_mgr_;
};

int TestPerformance::init()
{
  int ret = OB_SUCCESS;
  ts_mgr_ = new MockObTsMgr(lts_source_);
  // ObKVGlobalCache::get_instance().init();
  oceanbase::common::ObClusterVersion::get_instance().refresh_cluster_version("1.3.0");
  schema_service_ = new share::schema::ObMultiVersionSchemaService();
  rpc_.init(&trans_service_, self_);
  location_adapter_.init(NULL, schema_service_);
  location_adapter_.set_self(self_);
  clog_adapter_.init(&partition_service_, self_);
  trans_service_.init(self_, &rpc_, &location_adapter_, &clog_adapter_, &partition_service_, schema_service_, ts_mgr_);

  trans_service_.start();

  for (int32_t i = 0; i < MAX_TABLE; i++) {
    for (int32_t j = 0; j < PARTITION_COUNT_PER_TABLE; j++) {
      ObPartitionKey partition(combine_id(TENANT_ID, i + TABLE_NUMBER_START), j, PARTITION_COUNT_PER_TABLE);
      partition_array_.push_back(partition);
      addr_array_.push_back(self_);
      trans_service_.add_partition(partition);
      trans_service_.leader_takeover(partition, leader_active_arg);
      trans_service_.leader_active(partition, leader_active_arg);
    }
  }

  sp_trans_ = true;

  return ret;
}

void TestPerformance::destroy()
{
  delete schema_service_;
  delete ts_mgr_;
}

int TestPerformance::generate_participant_arr_guard_(
    const ObPartitionArray& participants, ObIPartitionArrayGuard& pkey_guard_arr)
{
  int ret = OB_SUCCESS;
  pkey_guard_arr.set_pg_mgr(partition_service_.get_pg_mgr());

  for (int64_t i = 0; OB_SUCC(ret) && i < participants.count(); ++i) {
    const ObPartitionKey& pkey = participants.at(i);
    ObIPartitionGroupGuard pkey_guard;
    if (OB_FAIL(partition_service_.get_partition(pkey, pkey_guard))) {
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (NULL == pkey_guard.get_partition_group()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "get partition failed", K(pkey), K(ret));
    } else if (OB_FAIL(pkey_guard_arr.push_back(pkey_guard.get_partition_group()))) {
      STORAGE_LOG(WARN, "pkey guard push back error", K(ret), K(i), K(participants));
    } else {
      STORAGE_LOG(INFO, "pkey guard : ", K(pkey_guard.get_partition_group()->get_partition_key()), K(pkey));
      // do nothing
    }
  }

  return ret;
}

int TestPerformance::do_trans()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = TENANT_ID;
  const uint64_t thread_id = 100;
  const int64_t cur_ts = ObClockGenerator::getClock();
  const int64_t trans_expired_time = cur_ts + 10000000;
  const int64_t stmt_expired_time = cur_ts + 1000000;
  ObStartTransParam trans_param;
  ObTransDesc trans_desc;
  ObPartitionLeaderArray pla;
  ObPartitionArray out;
  ObStmtDesc& stmt_desc = trans_desc.get_cur_stmt_desc();
  MockObEndTransCallback cb_;
  // non-sp trans, each participant executes tree sql requests
  static const int64_t STMT_PER_TRANS = 3;

  if (access_mod_ == ObTransAccessMode::READ_ONLY) {
    trans_param.set_access_mode(ObTransAccessMode::READ_ONLY);
    stmt_desc.stmt_type_ = stmt::T_SELECT;
    stmt_desc.is_sfu_ = false;
  } else {
    trans_param.set_access_mode(ObTransAccessMode::READ_WRITE);
    stmt_desc.stmt_type_ = stmt::T_UPDATE;
    stmt_desc.is_sfu_ = false;
  }
  trans_param.set_type(ObTransType::TRANS_USER);
  trans_param.set_isolation(ObTransIsolation::READ_COMMITED);
  trans_param.set_autocommit(true);
  trans_param.set_cluster_version(GET_MIN_CLUSTER_VERSION());

  ObStmtParam stmt_param;

  stmt_desc.phy_plan_type_ = OB_PHY_PLAN_LOCAL;
  stmt_desc.consistency_level_ = ObTransConsistencyLevel::STRONG;

  if (OB_SUCCESS != (ret = get_random_partition(pla))) {
    TRANS_LOG(WARN, "get random partition error", K(ret));
  } else if (OB_SUCCESS != (ret = trans_service_.start_trans(
                                tenant_id, thread_id, trans_param, trans_expired_time, 0, 0, trans_desc))) {
    TRANS_LOG(WARN, "start trans error", K(ret));
  } else {
    const int64_t stmt_count = sp_trans_ ? 1 : STMT_PER_TRANS;
    for (int64_t i = 0; OB_SUCC(ret) && i < stmt_count; ++i) {
      if (OB_FAIL(stmt_param.init(tenant_id, stmt_expired_time, false))) {
        TRANS_LOG(WARN, "ObStmtParam init error", K(ret), K(stmt_desc));
      } else if (OB_SUCCESS != (ret = trans_service_.start_stmt(stmt_param, trans_desc, pla, out))) {
        TRANS_LOG(WARN, "start stmt error", K(ret));
      } else {
        ObPartitionEpochArray partition_epoch_arr;
        ObIPartitionArrayGuard pkey_guard_arr;
        generate_participant_arr_guard_(pla.get_partitions(), pkey_guard_arr);
        ObPartitionArray participants;
        if (OB_SUCCESS != (ret = trans_service_.start_participant(
                               trans_desc, pla.get_partitions(), partition_epoch_arr, pkey_guard_arr))) {
          TRANS_LOG(WARN, "start participant error", K(ret));
        } else if (OB_SUCCESS !=
                   (ret = trans_service_.end_participant(false /*is_rollback*/, trans_desc, pla.get_partitions()))) {
          TRANS_LOG(WARN, "end participant error", K(ret));
        } else {
          // do nothing
        }
        const bool is_rollback = (OB_SUCCESS == ret) ? false : true;
        // overwrite retcode
        ObPartitionEpochArray epoch_arr;
        bool incomplete = false;
        if (OB_SUCCESS !=
            (ret = trans_service_.end_stmt(
                 is_rollback, incomplete, pla.get_partitions(), epoch_arr, participants, pla, trans_desc))) {
          TRANS_LOG(WARN, "end stmt error", K(ret));
        }
      }
    }
    const bool is_rollback = (OB_SUCCESS == ret) ? false : true;
    if (OB_SUCCESS != (ret = trans_service_.end_trans(is_rollback, trans_desc, cb_, stmt_expired_time))) {
      TRANS_LOG(WARN, "end trans error", K(ret));
    } else {
      ret = cb_.wait();
    }
  }

  return ret;
}

int TestPerformance::smoke()
{
  int ret = OB_SUCCESS;
  const int64_t MAX_LOOP = 1000;

  for (int64_t loop = 0; OB_SUCC(ret) && loop < MAX_LOOP; ++loop) {
    ret = do_trans();
  }

  return ret;
}

int TestPerformance::get_random_partition(ObPartitionLeaderArray& array)
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  if (sp_trans_) {
    count = 1;
  } else {
    count = (rand() % 15) + 2;
  }
  array.reset();
  // Any two of participants of a sentence are not allowed to be the same.
  for (int64_t i = 0; OB_SUCC(ret) && i < count; i++) {
    // const int64_t idx = ObRandom::rand(0, partition_array_.count() - 1);
    const int64_t idx = i;
    ret = array.push(partition_array_[idx], addr_array_[idx]);
  }

  return ret;
}

static void* thr_fn(void* arg)
{
  TestPerformance* p = reinterpret_cast<TestPerformance*>(arg);
  p->smoke();
  p->rpc_.destroy();
  return (void*)0;
}
//////////////////////test///////////////////////////
// test read only transactions without concurrency
TEST_F(TestPerformance, sp_readonly_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  set_sp_trans(true);
  set_access_mod(ObTransAccessMode::READ_ONLY);
  const int64_t start = ObClockGenerator::getClock();
  EXPECT_EQ(OB_SUCCESS, smoke());
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "sp_readonly_performance statistic", "total_used", end - start);
}

// test ready only transactions wiht concurrency
TEST_F(TestPerformance, sp_readonly_concurrent_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t THREAD_NUM = get_cpu_num() * 2;
  pthread_t tids[THREAD_NUM];

  set_sp_trans(true);
  set_access_mod(ObTransAccessMode::READ_ONLY);

  const int64_t start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&tids[i], NULL, thr_fn, this));
  }

  // collect resources of threads
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    pthread_join(tids[i], NULL);
  }
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "sp_readonly_concurrent_performance statistic", "total_used", end - start);
}

// test read-only transactions without concurrency
TEST_F(TestPerformance, mp_readonly_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  set_sp_trans(false);
  set_access_mod(ObTransAccessMode::READ_ONLY);
  const int64_t start = ObClockGenerator::getClock();
  EXPECT_EQ(OB_SUCCESS, smoke());
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "mp_readonly_performance statistic", "total_used", end - start);
}

// test read-only transactions with concurrency
TEST_F(TestPerformance, mp_readonly_concurrent_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t THREAD_NUM = get_cpu_num() * 2;
  pthread_t tids[THREAD_NUM];

  set_sp_trans(false);
  set_access_mod(ObTransAccessMode::READ_ONLY);

  const int64_t start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&tids[i], NULL, thr_fn, this));
  }

  // collect resources of threads
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    pthread_join(tids[i], NULL);
  }
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "mp_readonly_concurrent_performance statistic", "total_used", end - start);
}

// test read-write transactions without concurrency
TEST_F(TestPerformance, sp_read_write_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  set_sp_trans(true);
  set_access_mod(ObTransAccessMode::READ_WRITE);
  const int64_t start = ObClockGenerator::getClock();
  EXPECT_EQ(OB_SUCCESS, smoke());
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "sp_read_write_performance statistic", "total_used", end - start);
}

// test read-write transactions with concurrency
TEST_F(TestPerformance, sp_read_write_concurrent_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t THREAD_NUM = get_cpu_num() * 2;
  pthread_t tids[THREAD_NUM];

  set_sp_trans(true);
  set_access_mod(ObTransAccessMode::READ_WRITE);

  const int64_t start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&tids[i], NULL, thr_fn, this));
  }

  // collect resources of threads
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    pthread_join(tids[i], NULL);
  }
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "sp_read_write_concurrent_performance statistic", "total_used", end - start);
}

// test read-write transactions without concurrency
TEST_F(TestPerformance, mp_read_write_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());
  set_sp_trans(false);
  set_access_mod(ObTransAccessMode::READ_WRITE);
  const int64_t start = ObClockGenerator::getClock();
  EXPECT_EQ(OB_SUCCESS, smoke());
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "mp_read_write_performance statistic", "total_used", end - start);
}

// test read-write transactions with concurrency
TEST_F(TestPerformance, mp_read_write_concurrent_performance)
{
  TRANS_LOG(INFO, "called", "func", test_info_->name());

  const int64_t THREAD_NUM = get_cpu_num() * 2;
  pthread_t tids[THREAD_NUM];

  set_sp_trans(false);
  set_access_mod(ObTransAccessMode::READ_WRITE);

  const int64_t start = ObClockGenerator::getClock();
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    EXPECT_EQ(0, pthread_create(&tids[i], NULL, thr_fn, this));
  }

  // collect resources of threads
  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    pthread_join(tids[i], NULL);
  }
  const int64_t end = ObClockGenerator::getClock();
  TRANS_LOG(INFO, "mp_read_write_concurrent_performance statistic", "total_used", end - start);
}

}  // namespace unittest
}  // namespace oceanbase

using namespace oceanbase;
using namespace oceanbase::common;

int main(int argc, char** argv)
{
  int ret = 1;
  ObLogger& logger = ObLogger::get_logger();
  logger.set_file_name("test_performance.log", true);
  logger.set_log_level(OB_LOG_LEVEL_INFO);
  if (OB_SUCCESS != ObClockGenerator::init()) {
    TRANS_LOG(WARN, "clock generator init error!");
  } else {
    testing::InitGoogleTest(&argc, argv);
    ret = RUN_ALL_TESTS();
  }
  return ret;
}

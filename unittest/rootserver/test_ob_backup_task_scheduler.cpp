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
#include <gmock/gmock.h>
#define private public
#define protected public

#include "rootserver/backup/ob_backup_task_scheduler.h"
#include "rootserver/backup/ob_backup_service.h"
#include "share/ob_srv_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include "../share/partition_table/fake_part_property_getter.h"
#include "rpc/mock_ob_srv_rpc_proxy.h"
#include "lib/container/ob_array_iterator.h"
#include "rootserver/ob_server_manager.h"


#define CID combine_id

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace share::schema;
using namespace obrpc;
using namespace host;
using namespace testing;
using ::testing::_;
using ::testing::Invoke;
using ::testing::Return;

namespace rootserver
{

class fakeBackupJob : public ObIBackupJobScheduler
{
public:
  fakeBackupJob() :ObIBackupJobScheduler(BackupJobType::BACKUP_DATA_JOB) {}
  virtual ~fakeBackupJob() {}
  virtual int process() override { return OB_SUCCESS; }
  virtual int force_cancel(const uint64_t &tenant_id) override { UNUSED(tenant_id); return OB_SUCCESS; };
  virtual int handle_execute_over(const ObBackupScheduleTask *task, bool &can_remove, 
                                  const ObAddr &black_server, const int execute_ret) override 
  {
    UNUSED(task); 
    UNUSED(black_server); 
    UNUSED(execute_ret); 
    can_remove = true; 
    return OB_SUCCESS;
  }
  virtual int get_need_reload_task(common::ObIAllocator &allocator, common::ObIArray<ObBackupScheduleTask *> &tasks) { return OB_SUCCESS; }
};

fakeBackupJob job;
share::ObBackupTaskStatus::Status doing = share::ObBackupTaskStatus::Status::DOING;
share::ObBackupTaskStatus::Status init = share::ObBackupTaskStatus::Status::INIT;
share::ObBackupTaskStatus::Status pending = share::ObBackupTaskStatus::Status::PENDING;
share::ObBackupTaskStatus::Status finish = share::ObBackupTaskStatus::Status::FINISH;
class fakeTask : public ObBackupScheduleTask 
{
public:
  fakeTask(){}
  virtual ~fakeTask() {}
  void build(uint64_t tenant_id, uint64_t job_id, uint64_t task_id, 
       share::ObBackupTaskStatus::Status status = share::ObBackupTaskStatus::Status::INIT)
  {
    ObBackupScheduleTaskKey key;
    key.init(tenant_id, job_id,  task_id, 1, BackupJobType::BACKUP_DATA_JOB);
    task_key_.init(key);
    status_.status_ = status;
    if (status_.is_doing()) {
      dst_ = A;
    }
    set_generate_time(ObTimeUtility::current_time());
  }

public:
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override 
  { 
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == input_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(input_ptr));
    } else {
      fakeTask *my_task = new (input_ptr) fakeTask();
      if (OB_UNLIKELY(nullptr == my_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("taks is nullptr", K(ret));
      } else if (OB_FAIL(my_task->ObBackupScheduleTask::deep_copy(*this))) {
        LOG_WARN("fail to deep copy base task", K(ret));
      }
      if (OB_SUCC(ret)) {
        out_task = my_task;
      }
    }
    return ret;
  }
  virtual int64_t get_deep_copy_size() const override { return sizeof(fakeTask); }
  virtual bool can_execute_on_any_server()const override { return false; }
  virtual int do_update_dst_and_doing_status_(common::ObMySQLProxy &sql_proxy, common::ObAddr &dst, share::ObTaskId &trace_id) override 
  { 
    UNUSED(dst); 
    UNUSED(sql_proxy); 
    UNUSED(trace_id);
    return OB_SUCCESS; 
  }

  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override { UNUSED(rpc_proxy); return OB_SUCCESS; }
  virtual int cancel(obrpc::ObSrvRpcProxy &rpc_proxy) const override { UNUSED(rpc_proxy); return OB_SUCCESS; }
}; 

class fakeTaskV2 : public fakeTask 
{
public:
  virtual bool can_execute_on_any_server()const override { return true; }
  virtual int clone(void *input_ptr, ObBackupScheduleTask *&out_task) const override 
  { 
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == input_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid argument", K(ret), KP(input_ptr));
    } else {
      fakeTaskV2 *my_task = new (input_ptr) fakeTaskV2();
      if (OB_UNLIKELY(nullptr == my_task)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("taks is nullptr", K(ret));
      } else if (OB_FAIL(my_task->ObBackupScheduleTask::deep_copy(*this))) {
        LOG_WARN("fail to deep copy base task", K(ret));
      }
      if (OB_SUCC(ret)) {
        out_task = my_task;
      }
    }
    return ret;
  }
  virtual int64_t get_deep_copy_size() const override { return sizeof(fakeTaskV2); }
  virtual int execute(obrpc::ObSrvRpcProxy &rpc_proxy) const override 
  { 
    UNUSED(rpc_proxy); 
    return OB_REACH_SERVER_DATA_COPY_IN_CONCURRENCY_LIMIT; 
  }
};

void make_servers(
     ObIArray<share::ObBackupRegion> &backup_region,
     ObIArray<share::ObBackupZone> &backup_zone,
     ObIArray<share::ObBackupServer> &optional_server)
{
    ObBackupRegion A_,B_,C_;
    A_.set("REGION1",1); backup_region.push_back(A_);
    B_.set("REGION2",1); backup_region.push_back(B_);
    C_.set("REGION3",2); backup_region.push_back(C_);

    ObBackupZone a,b,c;
    a.set("ZONE1",1); backup_zone.push_back(a);
    b.set("ZONE2",1); backup_zone.push_back(b);
    c.set("ZONE3",2); backup_zone.push_back(c);

    ObBackupServer x,y,z;
    x.set(A,0); optional_server.push_back(x);
    y.set(B,0); optional_server.push_back(y);
    z.set(C,1); optional_server.push_back(z);
}

class FakeObServerManager : public ObServerManager
{
public:
  virtual int get_server_zone (const ObAddr &server, ObZone &zone) const {
    if (server == A) {
      zone = "ZONE1";
    } else if (server == B){
      zone = "ZONE2";
    } else {
      zone = "ZONE3";
    }
    return OB_SUCCESS;
  }

  int is_server_exist (const ObAddr &server, bool &exist) const {
    UNUSED(server);
    exist = true;
    return OB_SUCCESS;
  }

  int get_server_status (const ObAddr &server, ObServerStatus &status) const {
      UNUSED(server);
      status.admin_status_ = ObServerStatus::ServerAdminStatus::OB_SERVER_ADMIN_NORMAL;
      status.hb_status_ = ObServerStatus::HeartBeatStatus::OB_HEARTBEAT_ALIVE;
      status.start_service_time_ = 1;
      return OB_SUCCESS;
  }

  int get_alive_server_count (const ObZone &zone, int64_t& cnt) const 
  {
    int ret = OB_SUCCESS;
    UNUSED(zone);
    cnt = 1;
    return ret;
  }
  int get_alive_servers (const ObZone &zone, ObIServerArray& array) const 
  {
    int ret = OB_SUCCESS;
    if (zone.str() == "ZONE1") {
      array.push_back(A);
    } else if (zone.str() == "ZONE2") {
      array.push_back(B);
    } else {
      array.push_back(C);
    }
    return ret;
  } 
  int check_server_alive(const ObAddr &server, bool &is_alive) const {
    UNUSED(server);
    is_alive = true;
    return OB_SUCCESS;
  }
  int check_in_service(const common::ObAddr &addr, bool &service_started) const {
    UNUSED(addr);
    service_started = true;
    return OB_SUCCESS;
  }
};

class FakeObZoneManager : public ObZoneManager
{
public:
  virtual int get_zone(common::ObIArray<common::ObZone> &zone_list) const {
    zone_list.push_back("ZONE1"); zone_list.push_back("ZONE2"); zone_list.push_back("ZONE3");
    return OB_SUCCESS;
  }
  virtual int get_zone(const common::ObRegion &region, common::ObIArray<common::ObZone> &zone_list) const {
    if (region.str() == "REGION1") {
      zone_list.push_back("ZONE1");
    } else if (region.str() == "REGION2") {
      zone_list.push_back("ZONE2");
    } else {
      zone_list.push_back("ZONE3");
    }
    return OB_SUCCESS;
  }
  virtual int get_region(const common::ObZone &zone, common::ObRegion &region) const {
    if (zone.str() == "ZONE1") {
      region = "REGION1";
    } else if (zone.str() == "ZONE2") {
      region = "REGION2";
    } else {
      region = "REGION3";
    }
    return OB_SUCCESS;
  }
};

class MockBackupMgr : public ObBackupService
{
public:
virtual int get_job(const BackupJobType &type, ObIBackupJobScheduler *&new_job)
{
  UNUSED(type);
  new_job = &job;
  return OB_SUCCESS;
}
};

class MockLeaseService : public ObBackupLeaseService
{
public:
  virtual int check_lease() override { return OB_SUCCESS; }
};

class FakeSqlProxy : public ObMySQLProxy
{
};

class TestBackupTaskScheduler : public testing::Test
{
public:
  virtual void SetUp() {
    make_servers(backup_region, backup_zone, optional_server);
    ASSERT_EQ(OB_SUCCESS, scheduler_.init(&server_mgr, &zone_mgr_, &rpc_proxy, &backup_mgr_, sql_proxy_, lease_service_));
  }
  virtual void TearDown() {}
protected:
  ObArray<share::ObBackupRegion> backup_region;
  ObArray<share::ObBackupZone> backup_zone;
  ObArray<share::ObBackupServer> optional_server;
  ObBackupTaskScheduler scheduler_;
  FakeObServerManager server_mgr;
  FakeObZoneManager zone_mgr_;
  MockObSrvRpcProxy rpc_proxy;
  MockBackupMgr backup_mgr_;
  MockLeaseService lease_service_;
  FakeSqlProxy sql_proxy_;
};

TEST_F(TestBackupTaskScheduler, addTask) 
{
  fakeTask t1,t2,t3;
  t1.build(1001,1,1);
  t2.build(1001,1,1);
  t3.build(1002,1,1,doing);
  scheduler_.queue_.max_size_ = 8;

  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(t1));
  ASSERT_EQ(OB_ENTRY_EXIST, scheduler_.add_task(t2));
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(t3));
  ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(1, scheduler_.queue_.get_in_schedule_task_cnt_());
  ASSERT_EQ(2, scheduler_.queue_.get_task_cnt_());
  for (int64_t i = 2; i < scheduler_.queue_.max_size_; ++i) {
    fakeTask t;
    if (i < 5) {
      t.build(1001,i+1024,1);
      ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(t));
      continue;
    }
    t.build(1002,i+1024,1);
    ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(t));
  }
  fakeTask t4; t4.build(1002,8+1024,1);
  ASSERT_EQ(OB_SIZE_OVERFLOW, scheduler_.add_task(t4));
  scheduler_.queue_.dump_statistics();
}

TEST_F(TestBackupTaskScheduler, popTaskStrategy) 
{
  //1. no task in scheduler
  ObBackupScheduleTask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
  ASSERT_EQ(nullptr, task);
  //2. pop task can't execute on any server
  fakeTask t1,t2;
  t1.build(1001,1,1); t1.set_optional_servers(optional_server);
  t2.build(1001,2,1); t2.set_optional_servers(optional_server);
  {// task cant exec on any server
    ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(t1));
    ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(t2));
    ObTenantBackupScheduleTaskStat tenant_stat;
    scheduler_.tenant_stat_map_.get(1001,tenant_stat);
    ASSERT_EQ(2, tenant_stat.task_cnt_);
    ASSERT_EQ(2, scheduler_.queue_.get_wait_task_cnt_());
    ASSERT_EQ(0, scheduler_.queue_.get_in_schedule_task_cnt_());
    ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
    ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
    ASSERT_EQ(1, scheduler_.queue_.get_in_schedule_task_cnt_());
    ASSERT_TRUE(task->get_task_key() == t1.get_task_key());
    ASSERT_TRUE(task->in_schedule());
    ObAddr dst_1 = task->get_dst();
    ASSERT_TRUE(dst_1 == A || dst_1 == B);
    ObServerBackupScheduleTaskStat server_stat;
    ObBackupServerStatKey key_1;
    key_1.init(dst_1, BackupJobType::BACKUP_DATA_JOB);
    scheduler_.server_stat_map_.get(key_1,server_stat);
    ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);
    ASSERT_EQ(0, server_stat.data_in_limit_ts_);
    task = nullptr;
    ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
    ASSERT_EQ(0, scheduler_.queue_.get_wait_task_cnt_());
    ASSERT_EQ(2, scheduler_.queue_.get_in_schedule_task_cnt_());
    ObAddr dst_2 = task->get_dst();
    ASSERT_TRUE(dst_2 == A || dst_2 == B);
    ASSERT_TRUE(dst_2 != dst_1);
    ObBackupServerStatKey key_2;
    key_2.init(dst_2, BackupJobType::BACKUP_DATA_JOB);
    scheduler_.server_stat_map_.get(key_2,server_stat);
    ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);
    ASSERT_EQ(0, server_stat.data_in_limit_ts_);
  }
  scheduler_.reuse();
  //3. pop task can execute on any server
  fakeTaskV2 ft1,ft2,ft3;
  ft1.build(1001,1,1); ft2.build(1001,2,1); ft3.build(1001,3,1); 
  ASSERT_TRUE(ft1.can_execute_on_any_server());
  ObBackupScheduleTask *test_task = &ft1;
  ASSERT_TRUE(test_task->can_execute_on_any_server());
  ASSERT_EQ(0, scheduler_.queue_.get_wait_task_cnt_());
  {
    ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft1));
    ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft2));
    ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft3));
    ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
    ASSERT_EQ(2, scheduler_.queue_.get_wait_task_cnt_());
    ASSERT_EQ(1, scheduler_.queue_.get_in_schedule_task_cnt_());
    ObAddr dst_1 = task->get_dst();
    ObServerBackupScheduleTaskStat server_stat;
    ObBackupServerStatKey key_1;
    key_1.init(dst_1, BackupJobType::BACKUP_DATA_JOB);
    scheduler_.server_stat_map_.get(key_1, server_stat);
    ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);
    ASSERT_EQ(0, server_stat.data_in_limit_ts_);
    task = nullptr;
    ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
    ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
    ASSERT_EQ(2, scheduler_.queue_.get_in_schedule_task_cnt_());
    ObAddr dst_2 = task->get_dst();
    ASSERT_TRUE(dst_2 != dst_1);
    ObBackupServerStatKey key_2;
    key_2.init(dst_2, BackupJobType::BACKUP_DATA_JOB);
    scheduler_.server_stat_map_.get(key_2, server_stat);
    ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);
    ASSERT_EQ(0, server_stat.data_in_limit_ts_);
    task = nullptr;
    ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
    ASSERT_EQ(0, scheduler_.queue_.get_wait_task_cnt_());
    ASSERT_EQ(3, scheduler_.queue_.get_in_schedule_task_cnt_());
    ObAddr dst_3 = task->get_dst();
    ObBackupServerStatKey key_3;
    key_3.init(dst_2, BackupJobType::BACKUP_DATA_JOB);
    scheduler_.server_stat_map_.get(key_3, server_stat);
    ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);
    ASSERT_EQ(0, server_stat.data_in_limit_ts_);
    scheduler_.reuse();
    ASSERT_EQ(0, scheduler_.queue_.get_task_cnt_());
  } 
}

TEST_F(TestBackupTaskScheduler, removeTask)
{
  fakeTaskV2 ft1,ft2,ft3;
  ObServerBackupScheduleTaskStat server_stat;
  ObTenantBackupScheduleTaskStat tenant_stat;
  ft1.build(1001,1,1); ft2.build(1001,2,1); ft3.build(1001,1,2); 
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft1));
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft2));
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft3));
  ObBackupScheduleTask *task = nullptr;
  ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
  ASSERT_TRUE(task->in_schedule());
  ft1.set_dst(task->get_dst());
  ft1.set_schedule_time(task->get_schedule_time());
  ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
  ft2.set_dst(task->get_dst());
  ft2.set_schedule_time(task->get_schedule_time());
  ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(2, scheduler_.queue_.get_in_schedule_task_cnt_());

  scheduler_.tenant_stat_map_.get(1001, tenant_stat);
  ASSERT_EQ(3, tenant_stat.task_cnt_);
  ObBackupServerStatKey key_1,key_2;
  key_1.init(ft1.get_dst(), BackupJobType::BACKUP_DATA_JOB);
  key_2.init(ft2.get_dst(), BackupJobType::BACKUP_DATA_JOB);
  scheduler_.server_stat_map_.get(key_1, server_stat);
  ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);
  scheduler_.server_stat_map_.get(key_2, server_stat);
  ASSERT_EQ(1, server_stat.in_schedule_task_cnt_);

  // execute_over
  ASSERT_EQ(OB_SUCCESS, scheduler_.execute_over(ft1, OB_SUCCESS));
  scheduler_.tenant_stat_map_.get(1001, tenant_stat);
  ASSERT_EQ(2, tenant_stat.task_cnt_);
  scheduler_.server_stat_map_.get(key_1, server_stat);
  ASSERT_EQ(0, server_stat.in_schedule_task_cnt_);
  ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(1, scheduler_.queue_.get_in_schedule_task_cnt_());

  ASSERT_NE(OB_SUCCESS, scheduler_.execute_over(ft3, OB_SUCCESS));
  ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(1, scheduler_.queue_.get_in_schedule_task_cnt_());
  scheduler_.reuse();

  //cancel task
  fakeTaskV2 ft4,ft5,ft6;
  ft4.build(1001,1,1); ft5.build(1001,1,2); ft6.build(1001,2,1); 
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft4));
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft5));
  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft6));
  ASSERT_EQ(OB_SUCCESS, scheduler_.pop_task_(task));
  ASSERT_EQ(2, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(1, scheduler_.queue_.get_in_schedule_task_cnt_());
  ASSERT_EQ(OB_SUCCESS, scheduler_.cancel_tasks(BackupJobType::BACKUP_DATA_JOB, 1, 1001));
  ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(0, scheduler_.queue_.get_in_schedule_task_cnt_());
}

class mockBackuoJob : public fakeBackupJob {
public:
  MOCK_METHOD3(handle_execute_over,int(const ObBackupScheduleTask *, bool &, const ObAddr &));
};

TEST_F(TestBackupTaskScheduler, work)
{
  EXPECT_CALL(rpc_proxy, check_backup_task_exist(_,_,_)).
    WillRepeatedly(DoAll(SetArgReferee<1>(true), Return(OB_SUCCESS)));
  ObArray<ObBackupServer> servers;
  ObBackupServer x,y;
  x.set(A,0);servers.push_back(x);
  y.set(B,1);servers.push_back(y);
  ObServerBackupScheduleTaskStat server_stat;
  ObTenantBackupScheduleTaskStat tenant_stat;
  ASSERT_TRUE(scheduler_.idling_.stop_);
  ASSERT_TRUE(scheduler_.stop_);
  scheduler_.start();
  ASSERT_FALSE(scheduler_.idling_.stop_);
  ASSERT_FALSE(scheduler_.stop_);
  fakeBackupJob job;
  fakeTask ft1,ft2,ft3;
  ft1.build(1001,1,1); ft2.build(1001,2,1); ft3.build(1001,1,2); 
  ft1.set_optional_servers(servers); //(A B)
  ft2.set_optional_servers(servers); //(A B)
  ft3.set_optional_servers(servers); //(A B)
  scheduler_.add_task(ft1);
  usleep(1000000);
  scheduler_.queue_.dump_statistics();
  scheduler_.add_task(ft2);
  usleep(1000000);
  scheduler_.queue_.dump_statistics();
  scheduler_.tenant_stat_map_.get(1001, tenant_stat);
  ASSERT_EQ(2, tenant_stat.task_cnt_);

  ASSERT_EQ(OB_ENTRY_EXIST, scheduler_.add_task(ft1));// insert exist task

  ASSERT_EQ(OB_SUCCESS, scheduler_.add_task(ft3));// ft3 can't pop, because both of A and B is busy
  usleep(1000000);
  scheduler_.tenant_stat_map_.get(1001, tenant_stat);
  ASSERT_EQ(3, tenant_stat.task_cnt_);
  ASSERT_EQ(1, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(2, scheduler_.queue_.get_in_schedule_task_cnt_());
  ft1.set_dst(A);
  ASSERT_EQ(OB_SUCCESS, scheduler_.execute_over(ft1, OB_SUCCESS)); // ft1 executes over, ft3 can be execute
  usleep(1000000);
  scheduler_.tenant_stat_map_.get(1001, tenant_stat);
  ASSERT_EQ(2, tenant_stat.task_cnt_);
  ASSERT_EQ(0, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(2, scheduler_.queue_.get_in_schedule_task_cnt_());
  scheduler_.stop();
  ASSERT_TRUE(scheduler_.idling_.stop_);
  ASSERT_TRUE(scheduler_.stop_);
}

TEST_F(TestBackupTaskScheduler, inactive_server) 
{
  fakeBackupJob job1;
  EXPECT_CALL(rpc_proxy, check_backup_task_exist(_,_,_)).
  WillRepeatedly(DoAll(SetArgReferee<1>(false), Return(OB_SUCCESS)));
  ObArray<ObBackupServer> servers;
  ObBackupServer x,y;
  x.set(A,0);servers.push_back(x);
  y.set(B,1);servers.push_back(y);
  ObServerBackupScheduleTaskStat server_stat;
  ObTenantBackupScheduleTaskStat tenant_stat;
  scheduler_.start();
  fakeTask ft1,ft2;
  ft1.build(1001,1,1); ft2.build(1001,2,1);
  ft1.set_optional_servers(servers); //(A B)
  ft2.set_optional_servers(servers); //(A B)
  scheduler_.add_task(ft1);
  scheduler_.add_task(ft2);
  usleep(11 * 60 * 1000 * 1000);
  ASSERT_EQ(0, scheduler_.queue_.get_wait_task_cnt_());
  ASSERT_EQ(0, scheduler_.queue_.get_in_schedule_task_cnt_());
  scheduler_.stop();
}

/*
class TestBackupMgr : public testing::Test
{
public:
  virtual void SetUp() {
    mgr_.init(&scheduler);
  }
  virtual void TearDown() {}
protected:
  ObBackupService mgr_;
  ObBackupTaskScheduler scheduler;
};

TEST_F(TestBackupMgr, work)
{
  ASSERT_TRUE(mgr_.idling_.stop_);
  ASSERT_TRUE(mgr_.stop_);
  mgr_.start();
  ASSERT_FALSE(mgr_.idling_.stop_);
  ASSERT_FALSE(mgr_.stop_);
  fakeBackupJob job1,job2;
  job1.job_type_ = BackupJobType::BACKUP_DATA_JOB;
  job2.job_type_ = BackupJobType::VALIDATE_JOB;
  ASSERT_EQ(OB_SUCCESS, mgr_.register_job(&job1));
  ASSERT_EQ(OB_SUCCESS, mgr_.register_job(&job2));
  ASSERT_EQ(BackupJobType::BACKUP_DATA_JOB, mgr_.jobs_.at(0)->get_job_type());
  ASSERT_EQ(BackupJobType::VALIDATE_JOB, mgr_.jobs_.at(1)->get_job_type());
  mgr_.stop();
  ASSERT_TRUE(mgr_.idling_.stop_);
  ASSERT_TRUE(mgr_.stop_);
}
*/

}
}

int main(int argc, char *argv[])
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}
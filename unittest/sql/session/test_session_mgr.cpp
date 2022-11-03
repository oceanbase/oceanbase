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
#include <stdlib.h>
#include <algorithm>
#include <pthread.h>
#include <sys/time.h>
#include "lib/stat/ob_session_stat.h"
#include "sql/session/ob_sql_session_mgr.h"
#include "lib/random/ob_random.h"
#include "rpc/obmysql/obsm_struct.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::observer;
class DISABLED_TestSessionMgr : public ::testing::Test
{
public:
  DISABLED_TestSessionMgr():
    mgr_() {

  }

  virtual void SetUp()
  {
    mgr_.init();
  }

  virtual void TearDown()
  {
  }
private:
  class SessionOperator
  {
  public:
    uint64_t sum;
    SessionOperator() : sum(0) {
    }
    bool operator()(ObSQLSessionMgr::Key key, ObSQLSessionInfo* sess_info) {
      UNUSED(key);
      UNUSED(sess_info);
      sum++;
      return true;
    }
  };

protected:
  ObSQLSessionMgr mgr_;
  SessionOperator sess_operator_;
  ObSMConnection conn_;
};

TEST_F(DISABLED_TestSessionMgr, test_exception)
{
  ObSQLSessionInfo *sess_info = NULL;
  ObSQLSessionInfo *dup_sess = NULL;
  conn_.sessid_ = 1;
  EXPECT_EQ(OB_SUCCESS, mgr_.create_session(&conn_, sess_info));
  EXPECT_EQ(OB_SESSION_ENTRY_EXIST, mgr_.create_session(&conn_, dup_sess));
  EXPECT_EQ(NULL, dup_sess);
  mgr_.revert_session(sess_info);
}

TEST_F(DISABLED_TestSessionMgr, test_create)
{
  ObSQLSessionInfo *sess_info = NULL;
  int64_t sess_cnt = 0;
  const uint32_t SESSION_COUNT = 100;
  for (uint32_t i = 1; i < SESSION_COUNT; ++i) {
    conn_.sessid_ = i;
    conn_.proxy_sessid_ = i * 10;
    EXPECT_EQ(OB_SUCCESS, mgr_.create_session(&conn_, sess_info));
    EXPECT_TRUE(sess_info);
    EXPECT_EQ(i, sess_info->get_sessid());
    EXPECT_EQ(i * 10, sess_info->get_proxy_sessid());
    EXPECT_EQ(SESSION_INIT, sess_info->get_session_state());
    mgr_.revert_session(sess_info);
  }

  for (uint32_t i = 1; i < SESSION_COUNT; ++i) {
    EXPECT_EQ(OB_SUCCESS, mgr_.get_session(i, sess_info));
    EXPECT_TRUE(sess_info);
    EXPECT_EQ(i, sess_info->get_sessid());
    EXPECT_EQ(i * 10, sess_info->get_proxy_sessid());
    EXPECT_EQ(SESSION_INIT, sess_info->get_session_state());
    mgr_.revert_session(sess_info);
    mgr_.get_session_count(sess_cnt);
    EXPECT_EQ(SESSION_COUNT - i, sess_cnt);
    ObFreeSessionCtx ctx;
    ctx.tenant_id_ = OB_SYS_TENANT_ID;
    ctx.sessid_ = i;
    ctx.has_inc_active_num_ = false;
    EXPECT_EQ(OB_SUCCESS, mgr_.free_session(ctx));
  }
}

TEST_F(DISABLED_TestSessionMgr, test_performace)
{
  ObSQLSessionInfo *sess_info = NULL;
  const uint32_t SESSION_COUNT = 500;
  struct timeval  get_beg, get_end, stat_beg, stat_end;
  for (uint32_t i = 1; i < SESSION_COUNT; ++i) {
    conn_.sessid_ = i;
    mgr_.create_session(&conn_, sess_info);
    mgr_.revert_session(sess_info);
  }
  gettimeofday(&get_beg, NULL);
  for (uint32_t i = 1; i < SESSION_COUNT; ++i) {
    mgr_.get_session(i, sess_info);
  }
  gettimeofday(&get_end, NULL);
  gettimeofday(&stat_beg, NULL);
  for (uint32_t i = 1; i < SESSION_COUNT; ++i) {
    ObSessionStatEstGuard guard(1, static_cast<uint32_t>(i));
    EVENT_DEC(ACTIVE_SESSIONS);
  }
  gettimeofday(&stat_end, NULL);
  const uint64_t WEIGHT = 1000000;
  OB_LOG(INFO, "performance average",
         "get_session", (WEIGHT * (get_end.tv_sec - get_beg.tv_sec) + get_end.tv_usec - get_beg.tv_usec) / SESSION_COUNT,
         "stat", (WEIGHT * (stat_end.tv_sec - stat_beg.tv_sec) + stat_end.tv_usec - stat_beg.tv_usec) / SESSION_COUNT);
}

TEST_F(DISABLED_TestSessionMgr, test_version)
{
  ObSQLSessionInfo *sess_info = NULL;
  const uint32_t SESS_ID = 1;
  for (uint32_t i = 0; i < 100; ++i) {
    conn_.sessid_ = SESS_ID;
    conn_.proxy_sessid_ = SESS_ID * 10;
    EXPECT_EQ(OB_SUCCESS, mgr_.create_session(&conn_, sess_info));
    EXPECT_TRUE(sess_info);
    EXPECT_EQ(SESSION_INIT, sess_info->get_session_state());
    EXPECT_EQ(SESS_ID, sess_info->get_sessid());
    EXPECT_EQ(SESS_ID * 10, sess_info->get_proxy_sessid());
    sess_info->set_shadow(true);
    mgr_.revert_session(sess_info);
  }

  for (uint32_t i = 0; i < 100; ++i) {
    EXPECT_EQ(OB_SUCCESS, mgr_.get_session(SESS_ID, sess_info));
    EXPECT_EQ(SESS_ID, sess_info->get_sessid());
    EXPECT_EQ(SESS_ID * 10, sess_info->get_proxy_sessid());
    EXPECT_EQ(true, sess_info->is_shadow());
    mgr_.revert_session(sess_info);
  }
}

TEST_F(DISABLED_TestSessionMgr, test_for_each)
{
  ObSQLSessionInfo *sess_info = NULL;
  for (uint32_t i = 0; i < 100; ++i) {
    conn_.sessid_ = i;
    EXPECT_EQ(OB_SUCCESS, mgr_.create_session(&conn_, sess_info));
    EXPECT_TRUE(sess_info);
    mgr_.revert_session(sess_info);
  }
  EXPECT_EQ(OB_SUCCESS, mgr_.for_each_session(sess_operator_));
  EXPECT_EQ(100, sess_operator_.sum);
}


int64_t create_num = 0;
class ObStressThread_create : public share::ObThreadPool
{
public:
  void run1()
  {
    ObSQLSessionInfo* sess_info = NULL;
    ObSMConnection conn;
    int err = OB_SUCCESS;
    uint32_t N = 1000;
    for (uint32_t i = 0; i < N; i++) {
      uint32_t id = static_cast<uint32_t>(ObRandom::rand(0, N));
      err = mgr_->get_session(id, sess_info);
      if (OB_ENTRY_NOT_EXIST == err) {
        conn.sessid_ = id;
        conn.proxy_sessid_ = id + 1;
        err = mgr_->create_session(&conn, sess_info);
        EXPECT_EQ(true, OB_SUCCESS == err || OB_SESSION_ENTRY_EXIST == err);
        if (OB_SUCCESS == err) {
          ATOMIC_AAF(&create_num, 1);
          EXPECT_EQ(id, sess_info->get_sessid());
          EXPECT_EQ(id + 1, sess_info->get_proxy_sessid());
          // ObSQLSessionInfo::LockGuard lock_guard(sess_info->get_thread_data_lock());
          // sess_info->set_query_start_time(id);
          mgr_->revert_session(sess_info);
        }
      } else {
        EXPECT_EQ(OB_SUCCESS, err);
        // ObSQLSessionInfo::LockGuard lock_guard(sess_info->get_thread_data_lock());
        // EXPECT_EQ(true, 0 == static_cast<uint32_t>(sess_info->get_query_start_time()) ||
        //           id == static_cast<uint32_t>(sess_info->get_query_start_time()));
        mgr_->revert_session(sess_info);
      }
    }
  }
  ObSQLSessionMgr * mgr_;
  ObSMConnection *conn_;
};

TEST_F(DISABLED_TestSessionMgr, create_concurrent)
{
  ObStressThread_create threads;
  threads.mgr_ = &mgr_;
  threads.conn_ = &conn_;
  threads.set_thread_count(4);
  threads.start();
  threads.wait();
  int64_t sess_cnt;
  mgr_.get_session_count(sess_cnt);
  OB_LOG(INFO, "concurrent create result", K(create_num), K(sess_cnt));
  EXPECT_EQ(create_num, sess_cnt);
}

struct ThreadArgs
{
  ObSQLSessionMgr *mgr_;
  ObSMConnection *conn_;
  uint32_t sess_cnt_;
};

void *thread_func_create_session(void *args)
{
  ThreadArgs *thd_args = static_cast<ThreadArgs*>(args);
  int ret = OB_SUCCESS;
  ObSMConnection conn;
  for (uint32_t i = 0; i < thd_args->sess_cnt_; ++i) {
    ObSQLSessionInfo *sess_info = NULL;
    uint32_t id = static_cast<uint32_t>(ObRandom::rand(0, thd_args->sess_cnt_));
    conn.sessid_ = id;
    ret = thd_args->mgr_->create_session(&conn, sess_info);
    if (OB_SUCC(ret)) {
      EXPECT_EQ(SESSION_INIT, sess_info->get_session_state());
      thd_args->mgr_->revert_session(sess_info);
    } else {
      EXPECT_EQ(OB_SESSION_ENTRY_EXIST, ret);
    }
  }
  return NULL;
}

void *thread_func_get_session(void *args)
{
  ThreadArgs *thd_args = static_cast<ThreadArgs*>(args);
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < thd_args->sess_cnt_; ++i) {
    uint32_t id = static_cast<uint32_t>(ObRandom::rand(0, thd_args->sess_cnt_));
    ObSQLSessionInfo *sess_info = NULL;
    ret = thd_args->mgr_->get_session(id, sess_info);
    EXPECT_EQ(true, OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
    if (OB_SUCC(ret)) {
      thd_args->mgr_->revert_session(sess_info);
    }
  }
  return NULL;
}

void *thread_func_shadow_session(void *args)
{
  ThreadArgs *thd_args = static_cast<ThreadArgs*>(args);
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < thd_args->sess_cnt_; ++i) {
    uint32_t id = static_cast<uint32_t>(ObRandom::rand(0, thd_args->sess_cnt_));
    ObSQLSessionInfo *sess_info = NULL;
    ret = thd_args->mgr_->get_session(id, sess_info);
    EXPECT_EQ(true, OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
    if (OB_SUCC(ret)) {
      sess_info->set_shadow(true);
      thd_args->mgr_->revert_session(sess_info);
    }
  }
  return NULL;
}

void *thread_func_free_session(void *args)
{
  ThreadArgs *thd_args = static_cast<ThreadArgs*>(args);
  int ret = OB_SUCCESS;
  for (uint32_t i = 0; i < thd_args->sess_cnt_; ++i) {
    uint32_t id = static_cast<uint32_t>(ObRandom::rand(0, thd_args->sess_cnt_));
    ObFreeSessionCtx ctx;
    ctx.tenant_id_ = OB_SYS_TENANT_ID;
    ctx.sessid_ = id;
    ctx.has_inc_active_num_ = false;
    ret = thd_args->mgr_->free_session(ctx);
    EXPECT_EQ(true, OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);
  }
  return NULL;
}

TEST_F(DISABLED_TestSessionMgr, concurrent)
{
  const int64_t THREAD_NUM = 64;
  pthread_t thread_create_arr[THREAD_NUM];
  pthread_t thread_get_arr[THREAD_NUM];
  pthread_t thread_free_arr[THREAD_NUM];
  pthread_t thread_shadow_arr[THREAD_NUM];
  pthread_attr_t *attr_null = NULL;

  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    ThreadArgs thread_args;
    thread_args.mgr_ = &mgr_;
    thread_args.conn_ = &conn_;
    thread_args.sess_cnt_ = 1000;
    OB_ASSERT(0 == pthread_create(&thread_create_arr[i], attr_null, thread_func_create_session, &thread_args));
    OB_ASSERT(0 == pthread_create(&thread_shadow_arr[i], attr_null, thread_func_shadow_session, &thread_args));
    OB_ASSERT(0 == pthread_create(&thread_get_arr[i], attr_null, thread_func_get_session, &thread_args));
    OB_ASSERT(0 == pthread_create(&thread_free_arr[i], attr_null, thread_func_free_session, &thread_args));
  }

  for (int64_t i = 0; i < THREAD_NUM; ++i) {
    OB_ASSERT(0 == pthread_join(thread_create_arr[i], NULL));
    OB_ASSERT(0 == pthread_join(thread_get_arr[i], NULL));
    OB_ASSERT(0 == pthread_join(thread_free_arr[i], NULL));
  }
}

int main(int argc, char *argv[])
{
  srand((unsigned)time(NULL));
  //  OB_LOGGER.set_file_name("test_session_mgr.log");
  system("rm -rf test_session_mgr.log");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_session_mgr.log", true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

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

#define USING_LOG_PREFIX COMMON

#include <gtest/gtest.h>
#include <gmock/gmock.h>
#include "share/ob_debug_sync.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "rpc/mock_ob_common_rpc_proxy.h"
#include "share/config/ob_server_config.h"
#include <pthread.h>

namespace oceanbase
{
namespace common
{
using namespace obrpc;
using testing::_;
using testing::Return;

TEST(common, ObDebugSyncAction)
{
  ObDebugSyncAction a;
  ASSERT_FALSE(a.is_valid());

  a.sync_point_ = NOW;
  a.execute_ = 1;
  ASSERT_FALSE(a.is_valid());
  a.signal_ = "a";
  ASSERT_TRUE(a.is_valid());
  a.wait_ = "b";
  ASSERT_TRUE(a.is_valid());
  a.execute_ = 0;
  ASSERT_FALSE(a.is_valid());
  a.execute_ = 1;
  ASSERT_TRUE(a.is_valid());
  a.signal_ = "";
  ASSERT_TRUE(a.is_valid());

  a.timeout_ = 1024;
  a.no_clear_ = true;
  a.signal_ = "signal";

  int64_t len = a.get_serialize_size();
  char buf[len];
  LOG_INFO("action serialize size", K(len));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, a.serialize(buf, len, pos));
  ASSERT_EQ(pos, len);
  pos = 0;

  ObDebugSyncAction b;
  ASSERT_EQ(OB_SUCCESS, b.deserialize(buf, len, pos));

  ASSERT_EQ(a.sync_point_, b.sync_point_);
  ASSERT_EQ(a.timeout_, b.timeout_);
  ASSERT_EQ(a.signal_, b.signal_);
  ASSERT_EQ(a.wait_, b.wait_);
  ASSERT_EQ(a.no_clear_, b.no_clear_);
}

TEST(common, ObDSActionArray)
{
  ObDSActionArray aa;
  ASSERT_TRUE(aa.is_empty());
  ASSERT_FALSE(aa.is_active(NOW));

  ObDebugSyncAction a;
  a.sync_point_ = NOW;
  a.execute_ = 2;
  a.wait_ = "abc";
  a.timeout_ = 1024000;

  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  ASSERT_TRUE(aa.is_active(NOW));

  ObDebugSyncAction b;
  aa.copy_action(NOW, b);
  ASSERT_EQ(a.get_serialize_size(), b.get_serialize_size());

  // fetch to empty
  ASSERT_EQ(OB_SUCCESS, aa.fetch_action(NOW, b));
  ASSERT_FALSE(aa.is_empty());
  ASSERT_EQ(OB_SUCCESS, aa.fetch_action(NOW, b));
  ASSERT_TRUE(aa.is_empty());
  ASSERT_EQ(OB_ENTRY_NOT_EXIST, aa.fetch_action(NOW, b));

  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  ASSERT_EQ(OB_SUCCESS, aa.fetch_action(NOW, b));
  // over write
  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  ASSERT_EQ(OB_SUCCESS, aa.fetch_action(NOW, b));
  ASSERT_EQ(OB_SUCCESS, aa.fetch_action(NOW, b));
  ASSERT_TRUE(aa.is_empty());

  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  ASSERT_FALSE(aa.is_empty());

  aa.clear(NOW);
  ASSERT_TRUE(aa.is_empty());

  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  aa.clear_all();
  ASSERT_TRUE(aa.is_empty());

  ObDSActionArray ba;
  ASSERT_EQ(OB_SUCCESS, ba.add_action(a));

  const static int64_t BUF_SIZE = 1024;
  char buf[BUF_SIZE];
  int64_t len = aa.get_serialize_size();
  LOG_INFO("empty debug sync array actions overhead", K(len));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, aa.serialize(buf, len, pos));
  ASSERT_EQ(pos, len);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ba.deserialize(buf, len, pos));
  ASSERT_TRUE(ba.is_empty());

  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  a.sync_point_ = MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT;

  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  len = aa.get_serialize_size();

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, aa.serialize(buf, len, pos));
  ASSERT_EQ(pos, len);

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ba.deserialize(buf, len, pos));
  ASSERT_EQ(pos, len);

  ASSERT_TRUE(ba.is_active(NOW));
  ASSERT_TRUE(ba.is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  // const action array will always be empty
  const bool is_const = true;
  ObDSActionArray ca(is_const);
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ca.deserialize(buf, len, pos));
  ASSERT_EQ(pos, len);

  ASSERT_FALSE(ca.is_active(NOW));
  ASSERT_FALSE(ca.is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  ASSERT_TRUE(ca.is_empty());
}

TEST(common, ObDSSessionActions)
{
  ObMalloc allocator;
  ObDSSessionActions sa;

  ObDebugSyncAction a;
  a.sync_point_ = NOW;
  a.signal_ = "abc";
  a.execute_ = 1024;

  ASSERT_NE(OB_SUCCESS, sa.add_action(a));
  ASSERT_EQ(OB_SUCCESS, sa.init(1024, allocator));

  ASSERT_TRUE(sa.is_inited());

  ASSERT_EQ(OB_SUCCESS, sa.add_action(a));
  ObDSActionArray aa;
  sa.to_thread_local(aa);
  ASSERT_FALSE(aa.is_empty());
  ASSERT_TRUE(aa.is_active(NOW));

  aa.clear_all();
  sa.get_thread_local_result(aa);
  sa.to_thread_local(aa);
  ASSERT_TRUE(aa.is_empty());

  ASSERT_EQ(OB_SUCCESS, sa.add_action(a));
  a.sync_point_ = MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT;
  ASSERT_EQ(OB_SUCCESS, aa.add_action(a));
  sa.to_thread_local(aa);
  ASSERT_TRUE(aa.is_active(NOW));
  ASSERT_FALSE(aa.is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  ASSERT_EQ(OB_SUCCESS, sa.add_action(a));
  sa.clear(NOW);
  sa.to_thread_local(aa);
  ASSERT_FALSE(aa.is_active(NOW));
  ASSERT_TRUE(aa.is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  sa.clear_all();
  sa.to_thread_local(aa);
  ASSERT_TRUE(aa.is_empty());
}

class Timer
{
public:
  Timer() { begin_ = ::oceanbase::common::ObTimeUtility::current_time(); }
  int64_t used() const { return ::oceanbase::common::ObTimeUtility::current_time() - begin_; }
public:
  int64_t begin_;
};

static ObDSEventControl global_event_control; // avoid large stack object
#define WAIT_TIME 200000
#define WAIT_TIME_SAFE (WAIT_TIME * 9 / 10)

#define TO_STRING_(x) #x
#define TO_STRING(x) "" TO_STRING_(x)

void *run_wait(void *arg)
{
  ObDSEventControl *ec = static_cast<ObDSEventControl *>(arg);
  ec->wait("multi-thread-event", WAIT_TIME, true);
  return NULL;
}

TEST(common, ObDSEventControl)
{
  ObDSEventControl &ec = global_event_control;
  ASSERT_NE(OB_SUCCESS, ec.signal(""));

  ObSqlString event;
  for (int64_t i = 0; i < ec.MAX_EVENT_CNT; ++i) {
    ASSERT_EQ(OB_SUCCESS, event.assign_fmt("%ld", i));
    ASSERT_EQ(OB_SUCCESS, ec.signal(event.ptr())) << "event: " << event.ptr() << std::endl;
  }
  ASSERT_NE(OB_SUCCESS, ec.signal("e"));
  ec.clear_event();
  ASSERT_EQ(OB_SUCCESS, ec.signal("e"));
  const bool DO_CLEAR = true;
  const bool NO_CLEAR = false;

  {
    Timer t;
    ASSERT_EQ(OB_SUCCESS, ec.wait("e", WAIT_TIME, NO_CLEAR));
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }

  ASSERT_EQ(OB_SUCCESS, ec.signal("e"));
  {
    Timer t;
    ASSERT_EQ(OB_SUCCESS, ec.wait("e", WAIT_TIME, DO_CLEAR));
    ASSERT_EQ(OB_SUCCESS, ec.wait("e", WAIT_TIME, DO_CLEAR));
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }

  {
    Timer t;
    ASSERT_EQ(OB_SUCCESS, ec.wait("e", WAIT_TIME, DO_CLEAR));
    ASSERT_GT(t.used(), WAIT_TIME_SAFE);
  }

  {
    Timer t;
    ObArray<pthread_t> tids;
    const int64_t MAX_THREAD_CNT = 10;
    int64_t n = 0;
    for (; n < MAX_THREAD_CNT / 2; ++n) {
      ASSERT_EQ(OB_SUCCESS, ec.signal("multi-thread-event"));
    }
    for (int64_t i = 0; i < MAX_THREAD_CNT; ++i) {
      pthread_t tid;
      ASSERT_EQ(0, pthread_create(&tid, NULL, run_wait, &ec));
      ASSERT_EQ(OB_SUCCESS, tids.push_back(tid));
    }
    for (; n < MAX_THREAD_CNT; ++n) {
      ASSERT_EQ(OB_SUCCESS, ec.signal("multi-thread-event"));
    }
    for (int64_t i = 0; i < MAX_THREAD_CNT; ++i) {
      pthread_join(tids.at(i), NULL);
    }
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }

  {
    Timer t;
    pthread_t tid;
    ASSERT_EQ(0, pthread_create(&tid, NULL, run_wait, &ec));
    ec.stop();
    pthread_join(tid, NULL);
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }
}

TEST(debug_sync, ObDebugSync)
{
  MockObCommonRpcProxy rpc;
  GDS.set_rpc_proxy(&rpc);
  ObMalloc allocator;
  ObDSSessionActions sa;
  ASSERT_EQ(OB_SUCCESS, sa.init(1024, allocator));

  GCONF.debug_sync_timeout.set_value("0");
  ASSERT_FALSE(GCONF.is_debug_sync_enabled());
  ASSERT_NE(&GDS.rpc_spread_actions(), GDS.thread_local_actions());

  // test debug sync parser
  const bool L = false; // local
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  reset   ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  clear    ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("now  signal x", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  SIGNAL x-y    execute 1024 ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  NOW  signal x-y    execute 1024 ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  wait_for x-y ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("now  wait_for x-y  no_clear_event", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  wait_for x-y  no_clear_event execute 2 ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  wait_for x-y  timeout 1024 no_clear_event execute 2 ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  wait_for x-y  no_clear_event timeout 1024 execute 2 ", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  now  signal xxx wait_for x-y  timeout 0 no_clear_event execute 2 ", L, sa));

  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("  reset a", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("  signal b ", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("  not_exist_sync_point signal b ", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("now signal not-support-too-long-event-name-bigger-than-32-byte ", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("now signal xyz execute 0", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("now signal xyc unknow_parameters", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("now clear unknow_parameters", L, sa));
  ASSERT_NE(OB_SUCCESS, GDS.add_debug_sync("now wait_for xyc unknow_parameters", L, sa));

  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("  reset ", L, sa));

  GCONF.debug_sync_timeout.set_value("100000000");
  ASSERT_TRUE(GCONF.is_debug_sync_enabled());
  ASSERT_EQ(&GDS.rpc_spread_actions(), GDS.thread_local_actions());

  // test set to thread local and collect result
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", L, sa));
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT signal xyz", L, sa));
  ASSERT_TRUE(GDS.rpc_spread_actions().is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT clear", L, sa));
  ASSERT_FALSE(GDS.rpc_spread_actions().is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT signal xyz", L, sa));
  GDS.rpc_spread_actions().clear_all();
  GDS.set_thread_local_actions(sa);
  ASSERT_TRUE(GDS.rpc_spread_actions().is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  GDS.rpc_spread_actions().clear_all();
  GDS.collect_result_actions(sa);
  GDS.set_thread_local_actions(sa);
  ASSERT_FALSE(GDS.rpc_spread_actions().is_active(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT));

  // test execute
  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", L, sa));
  {
    Timer t;
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("now wait_for xyz timeout " TO_STRING(WAIT_TIME), L, sa));
    ASSERT_GT(t.used(), WAIT_TIME_SAFE);
  }
  ASSERT_TRUE(GDS.rpc_spread_actions().is_empty());

  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT signal xyz execute 2", L, sa));
  DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);
  DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);
  DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);

  ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT signal def wait_for xyz TIMEOUT " TO_STRING(WAIT_TIME) " execute 3", L, sa));

  {
    // get xyz
    Timer t;
    DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);
    DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }

  {
    // timeout
    Timer t;
    DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);
    ASSERT_GT(t.used(), WAIT_TIME_SAFE);
  }

  {
    // not active
    Timer t;
    DEBUG_SYNC(MAJOR_FREEZE_BEFORE_SYS_COORDINATE_COMMIT);
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }

  {
    // get def
    Timer t;
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("NOW wait_for def TIMEout " TO_STRING(WAIT_TIME), L, sa));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("NOW wait_for def TIMEout " TO_STRING(WAIT_TIME), L, sa));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("NOW wait_for def TIMEout " TO_STRING(WAIT_TIME), L, sa));
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }

  {
    // timeout
    Timer t;
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("NOW wait_for def TIMEout " TO_STRING(WAIT_TIME), L, sa));
    ASSERT_GT(t.used(), WAIT_TIME_SAFE);
  }

  // global actions
  const bool G = true; // global
  {
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", L, sa));
    EXPECT_CALL(rpc, broadcast_ds_action(_, _)).Times(3).WillRepeatedly(Return(OB_SUCCESS));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", G, sa));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("now clear", G, sa));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("now signal x", G, sa));
  }

  {
    // mock rpc will not wait
    Timer t;
    EXPECT_CALL(rpc, broadcast_ds_action(_, _)).Times(2).WillRepeatedly(Return(OB_SUCCESS));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("reset", G, sa));
    ASSERT_EQ(OB_SUCCESS, GDS.add_debug_sync("NOW wait_for def TIMEout " TO_STRING(WAIT_TIME), G, sa));
    ASSERT_LT(t.used(), WAIT_TIME_SAFE);
  }
}

TEST(debug_sync, debug_sync_action_overflow)
{
  ObDSActionArray dsa;
  ObDebugSyncAction action;
  action.sync_point_ = NOW;
  action.signal_ = "a";
  action.wait_ = "b";
  action.execute_ = 1;
  ASSERT_TRUE(action.is_valid());
  for (int i = 0; i < ObDSActionArray::MAX_DEBUG_SYNC_CACHED_POINT; i++) {
    action.sync_point_ = (oceanbase::common::ObDebugSyncPoint)(i + 1);
    ASSERT_EQ(OB_SUCCESS, dsa.add_action(action));
    ASSERT_TRUE(dsa.is_active((oceanbase::common::ObDebugSyncPoint)(i + 1 )));
  }
  action.sync_point_ = (oceanbase::common::ObDebugSyncPoint)(ObDSActionArray::MAX_DEBUG_SYNC_CACHED_POINT + 1);
  ASSERT_EQ(OB_SIZE_OVERFLOW, dsa.add_action(action));
  ASSERT_FALSE(dsa.is_active((oceanbase::common::ObDebugSyncPoint)(ObDSActionArray::MAX_DEBUG_SYNC_CACHED_POINT + 1)));
}

}
}

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

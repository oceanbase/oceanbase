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
#include "ob_log_trans_ctx_mgr.h"                   // ObLogTransCtxMgr
#include "common/ob_clock_generator.h" // ObClockGenerator

using namespace oceanbase::common;
using namespace oceanbase::libobcdc;
using namespace oceanbase::transaction;

class ObLogTransCtxMgrTest : public ::testing::Test
{
public:
  static const int64_t SLEEP_TIME = 10000;
  static const int64_t THREAD_NUM = 10;
  static const int64_t RUN_TIME_SEC = 60;
  static const int64_t CACHED_CTX_COUNT = 10000;
  static const int64_t TEST_CTX_COUNT = CACHED_CTX_COUNT + 1024;

public:
  ObLogTransCtxMgrTest();
  virtual ~ObLogTransCtxMgrTest();
  virtual void SetUp();
  virtual void TearDown();

  static void *thread_func(void *args);

public:
  void run();
  void test_imediately_remove();
  void test_dely_remove();

public:
  int32_t port_;
  ObTransID *trans_ids_;
  pthread_t threads_[THREAD_NUM];
  ObLogTransCtxMgr mgr_;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObLogTransCtxMgrTest);
};
ObLogTransCtxMgrTest::ObLogTransCtxMgrTest() : port_(0), trans_ids_(NULL), mgr_()
{
}

ObLogTransCtxMgrTest::~ObLogTransCtxMgrTest()
{
}

void ObLogTransCtxMgrTest::SetUp()
{
}

void ObLogTransCtxMgrTest::TearDown()
{
}

TEST_F(ObLogTransCtxMgrTest, DISABLED_single_thread_immediately_remove)
{
  ObLogTransCtxMgr trans_ctx_mgr;

  EXPECT_NE(OB_SUCCESS, trans_ctx_mgr.init(0));
  EXPECT_NE(OB_SUCCESS, trans_ctx_mgr.init(-1));
  EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.init(CACHED_CTX_COUNT));

  EXPECT_EQ(0, trans_ctx_mgr.get_valid_trans_ctx_count());
  EXPECT_EQ(0, trans_ctx_mgr.get_alloc_trans_ctx_count());
  EXPECT_EQ(0, trans_ctx_mgr.get_free_trans_ctx_count());

  // Up to two transaction context objects are allocated at the same time when used by a single thread following the "allocate-return-release" process.
  // One of them is not deleted from the cache. The logic is verified below.
  int64_t free_count = 0;
  int64_t alloc_count = 0;
  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    ObAddr svr(ObAddr::IPV4, "127.0.0.1", 1 + (int32_t)index);
    ObTransID trans_id;

    // At the beginning, the effective number is 0
    EXPECT_EQ(0, trans_ctx_mgr.get_valid_trans_ctx_count());

    free_count = index <= 1 ? 0 : 1;
    alloc_count = index <= 2 ? index : 2;
    EXPECT_EQ(free_count, trans_ctx_mgr.get_free_trans_ctx_count());
    EXPECT_EQ(alloc_count, trans_ctx_mgr.get_alloc_trans_ctx_count());

    // get with a not-exist trans_id
    TransCtx *ctx1 = NULL;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, trans_ctx_mgr.get_trans_ctx(trans_id, ctx1));

    // create when get a not-exist trans_id
    bool enable_create = true;
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.get_trans_ctx(trans_id, ctx1, enable_create));
    EXPECT_TRUE(NULL != ctx1);

    // get trans_ctx that create just now
    TransCtx *ctx1_get = NULL;
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.get_trans_ctx(trans_id, ctx1_get));
    EXPECT_TRUE(ctx1 == ctx1_get);

    // Valid quantity is 1
    EXPECT_EQ(1, trans_ctx_mgr.get_valid_trans_ctx_count());

    // Idle transaction context object used, idle becomes 0, allocated to a maximum of 2
    free_count = 0;
    alloc_count = index <= 0 ? 1 : 2;
    EXPECT_EQ(free_count, trans_ctx_mgr.get_free_trans_ctx_count());
    EXPECT_EQ(alloc_count, trans_ctx_mgr.get_alloc_trans_ctx_count());

    // revert the trans_ctx
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.revert_trans_ctx(ctx1));

    // A revert before a remove does not affect the number of objects
    EXPECT_EQ(1, trans_ctx_mgr.get_valid_trans_ctx_count());
    free_count = 0;
    alloc_count = index <= 0 ? 1 : 2;
    EXPECT_EQ(free_count, trans_ctx_mgr.get_free_trans_ctx_count());
    EXPECT_EQ(alloc_count, trans_ctx_mgr.get_alloc_trans_ctx_count());

    // remove
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.remove_trans_ctx(trans_id));
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, trans_ctx_mgr.remove_trans_ctx(trans_id));

    // After deletion, the effective number becomes 0
    EXPECT_EQ(0, trans_ctx_mgr.get_valid_trans_ctx_count());

    // After deletion, the object just deleted is not released immediately, but the last deleted object is released
    // So after the second time, the number of free objects becomes 1, but the number of allocated objects remains the same
    free_count = index <= 0 ? 0 : 1;
    alloc_count = index <= 0 ? 1 : 2;
    EXPECT_EQ(free_count, trans_ctx_mgr.get_free_trans_ctx_count());
    EXPECT_EQ(alloc_count, trans_ctx_mgr.get_alloc_trans_ctx_count());

    // revert the last one
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.revert_trans_ctx(ctx1_get));

    // When all returned, the valid number remains 0
    EXPECT_EQ(0, trans_ctx_mgr.get_valid_trans_ctx_count());

    // Even after swapping them all back, the object just deleted is not immediately released until the next time it is deleted
    free_count = index <= 0 ? 0 : 1;
    alloc_count = index <= 0 ? 1 : 2;
    EXPECT_EQ(free_count, trans_ctx_mgr.get_free_trans_ctx_count());
    EXPECT_EQ(alloc_count, trans_ctx_mgr.get_alloc_trans_ctx_count());
  }
}

TEST_F(ObLogTransCtxMgrTest, DISABLED_single_thread_delay_remove)
{
  ObLogTransCtxMgr trans_ctx_mgr;
  ObTransID *tids = (ObTransID *)ob_malloc(sizeof(ObTransID) * TEST_CTX_COUNT);
  TransCtx *tctxs_[TEST_CTX_COUNT];

  (void)memset(tctxs_, 0, sizeof(tctxs_));
  ASSERT_TRUE(NULL != tids);
  EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.init(CACHED_CTX_COUNT));

  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    new(tids + index) ObTransID();
    ObTransID &trans_id = tids[index];

    // get with a not-exist trans_id
    TransCtx *ctx1 = NULL;
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, trans_ctx_mgr.get_trans_ctx(trans_id, ctx1));

    // create when get a not-exist trans_id
    bool enable_create = true;
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.get_trans_ctx(trans_id, ctx1, enable_create));
    EXPECT_TRUE(NULL != ctx1);

    // revert the trans_ctx
    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.revert_trans_ctx(ctx1));
  }

  EXPECT_EQ(TEST_CTX_COUNT + 0, trans_ctx_mgr.get_valid_trans_ctx_count());
  EXPECT_EQ(TEST_CTX_COUNT + 0, trans_ctx_mgr.get_alloc_trans_ctx_count());
  EXPECT_EQ(0, trans_ctx_mgr.get_free_trans_ctx_count());

  int64_t REMOVE_INTERVAL_COUNT = 10;
  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    ObTransID &trans_id = tids[index];

    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.get_trans_ctx(trans_id, tctxs_[index]));
    EXPECT_TRUE(NULL != tctxs_[index]);

    EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.remove_trans_ctx(trans_id));
    EXPECT_EQ(TEST_CTX_COUNT - index - 1, trans_ctx_mgr.get_valid_trans_ctx_count());

    // revert REMOVE_INTERVAL_COUNT the object you got before, so that it is actually deleted on the next remove
    if (index >= REMOVE_INTERVAL_COUNT) {
      int64_t revert_index = index - REMOVE_INTERVAL_COUNT;
      // After revert, the next remove will delete
      EXPECT_EQ(OB_SUCCESS, trans_ctx_mgr.revert_trans_ctx(tctxs_[revert_index]));
      tctxs_[revert_index] = NULL;

      static int64_t alloc_count = TEST_CTX_COUNT;
      static int64_t free_count = 0;

      // The alloc_count is only decremented when a second non-cached object is deleted
      if (revert_index > CACHED_CTX_COUNT) {
        alloc_count--;
      } else if (revert_index > 0) {
        // The free_count is incremented when the cached transaction context object is deleted
        free_count++;
      }

      EXPECT_EQ(alloc_count, trans_ctx_mgr.get_alloc_trans_ctx_count());
      EXPECT_EQ(free_count, trans_ctx_mgr.get_free_trans_ctx_count());
    }
  }

  EXPECT_EQ(CACHED_CTX_COUNT + REMOVE_INTERVAL_COUNT + 1, trans_ctx_mgr.get_alloc_trans_ctx_count());
  EXPECT_EQ(CACHED_CTX_COUNT + 0, trans_ctx_mgr.get_free_trans_ctx_count());

  ob_free((void *)tids);
  tids = NULL;
}

TEST_F(ObLogTransCtxMgrTest, multiple_thread)
{
  EXPECT_EQ(OB_SUCCESS, mgr_.init(CACHED_CTX_COUNT));

  OB_ASSERT(NULL == trans_ids_);
  trans_ids_ = (ObTransID *)ob_malloc(sizeof(ObTransID) * TEST_CTX_COUNT);
  ASSERT_TRUE(NULL != trans_ids_);

  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    new(trans_ids_ + index) ObTransID();
  }

  for (int64_t index = 0; index < THREAD_NUM; index++) {
    ASSERT_EQ(0, pthread_create(threads_ + index, NULL, thread_func, this));
  }

  for (int64_t index = 0; index < THREAD_NUM; index++) {
    if (0 != threads_[index]) {
      pthread_join(threads_[index], NULL);
      threads_[index] = 0;
    }
  }

  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    trans_ids_[index].~ObTransID();
  }

  ob_free(trans_ids_);
  trans_ids_ = NULL;
}

void *ObLogTransCtxMgrTest::thread_func(void *args)
{
  if (NULL != args) {
    ((ObLogTransCtxMgrTest *)args)->run();
  }

  return NULL;
}

void ObLogTransCtxMgrTest::run()
{
  int64_t end_time = ObTimeUtility::current_time() + RUN_TIME_SEC * 1000000;

  while (true) {
    test_imediately_remove();
    test_dely_remove();
    int64_t left_time = end_time - ObTimeUtility::current_time();
    if (left_time <= 0) break;
  }
}

void ObLogTransCtxMgrTest::test_imediately_remove()
{
  ObAddr svr(ObAddr::IPV4, "127.0.0.1", ATOMIC_AAF(&port_, 1));
  ObTransID trans_id;  // Although the svr is the same, the internal inc will be self-increasing

  // get with a not-exist trans_id
  TransCtx *ctx1 = NULL;
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, mgr_.get_trans_ctx(trans_id, ctx1));

  // create when get a not-exist trans_id
  bool enable_create = true;
  EXPECT_EQ(OB_SUCCESS, mgr_.get_trans_ctx(trans_id, ctx1, enable_create));
  EXPECT_TRUE(NULL != ctx1);

  // get trans_ctx that create just now
  TransCtx *ctx1_get = NULL;
  EXPECT_EQ(OB_SUCCESS, mgr_.get_trans_ctx(trans_id, ctx1_get));
  EXPECT_TRUE(ctx1 == ctx1_get);

  // revert the trans_ctx
  EXPECT_EQ(OB_SUCCESS, mgr_.revert_trans_ctx(ctx1));

  usleep((useconds_t)random() % SLEEP_TIME);

  // remove
  EXPECT_EQ(OB_SUCCESS, mgr_.remove_trans_ctx(trans_id));
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, mgr_.remove_trans_ctx(trans_id));

  // Return to the last acquired
  EXPECT_EQ(OB_SUCCESS, mgr_.revert_trans_ctx(ctx1_get));
}

void ObLogTransCtxMgrTest::test_dely_remove()
{
  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    ObTransID &trans_id = trans_ids_[random() % TEST_CTX_COUNT];
    TransCtx *ctx = NULL;
    bool enable_create = true;

    EXPECT_EQ(OB_SUCCESS, mgr_.get_trans_ctx(trans_id, ctx, enable_create));
    EXPECT_TRUE(NULL != ctx);

    usleep((useconds_t)random() % SLEEP_TIME);

    EXPECT_EQ(OB_SUCCESS, mgr_.revert_trans_ctx(ctx));
  }

  for (int64_t index = 0; index < TEST_CTX_COUNT; index++) {
    ObTransID &trans_id = trans_ids_[random() % TEST_CTX_COUNT];
    TransCtx *ctx = NULL;

    int ret = mgr_.get_trans_ctx(trans_id, ctx);

    if (OB_SUCC(ret)) {
      EXPECT_TRUE(NULL != ctx);

      ret = mgr_.remove_trans_ctx(trans_id);
      EXPECT_TRUE(OB_SUCCESS == ret || OB_ENTRY_NOT_EXIST == ret);

      usleep((useconds_t)random() % SLEEP_TIME);

      EXPECT_EQ(OB_SUCCESS, mgr_.revert_trans_ctx(ctx));
    }
  }
}

int main(int argc, char **argv)
{
  // Used for initialization of ObTransID
  ObClockGenerator::init();

  srandom((unsigned)ObTimeUtility::current_time());

  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

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

#define USING_LOG_PREFIX SERVER

#include <gtest/gtest.h>
#include <thread>
#include "share/ob_define.h"
#include "observer/mysql/ob_ra_queue.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace test {

struct DummyAuditRecod {
  uint64_t val;
};

TEST(TestObRaQueue, DummyTest) {
  EXPECT_EQ(1 + 1, 2);
}

TEST(TestObRaQueue, InitTest) {
  ObRaQueue queue;
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, 100);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST(TestObRaQueue, BasicTest) {
  ObRaQueue queue;
  const uint64_t queue_size = 1024;
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, queue_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(malloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }
  int64_t seq = 0;
  int64_t global_id = 0;
  for (auto *audit : audits) {
    ret = queue.push(audit, seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(global_id++, seq);
  }

  // check get_pop_idx and get_push_idx
  EXPECT_EQ(0, queue.get_pop_idx());
  EXPECT_EQ(queue_size, queue.get_push_idx());

  // check size valid
  EXPECT_EQ(queue.get_size(), queue_size);

  ObRaQueue::Ref ref; 
  // check get function
  for (uint64_t req_id = 0; req_id < queue_size; ++req_id) {
    auto *audit = queue.get(req_id, &ref);
    EXPECT_NE(audit, nullptr);
    EXPECT_EQ(static_cast<DummyAuditRecod*>(audit)->val, req_id);
    EXPECT_EQ(ref.idx_, req_id);
    queue.revert(&ref);
  }

  // check pop function
  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = queue.pop();
    EXPECT_NE(audit, nullptr);
    free(audit);
  }

  EXPECT_EQ(0, queue.get_size());
}

TEST(TestObRaQueue, PushTest) {
  ObRaQueue queue;
  const uint64_t queue_size = 1024;
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, queue_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size * 2 ; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(malloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }
  int64_t seq = 0;
  int64_t global_id = 0;
  for (uint64_t i = 0;  i < queue_size; ++i) {
    ret = queue.push(audits[i], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(global_id++, seq);
  }

  // check get_pop_idx and get_push_idx
  EXPECT_EQ(0, queue.get_pop_idx());
  EXPECT_EQ(queue_size, queue.get_push_idx());
  // check size valid
  EXPECT_EQ(queue.get_size(), queue_size);

  // push again
  for (uint64_t i = 0; i < queue_size; ++i) {
    ret = queue.push(audits[i + queue_size], seq);
    EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);
  }
  EXPECT_EQ(queue.get_size(), queue_size);

  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = queue.pop();
    EXPECT_NE(audit, nullptr);
    free(audit);
  }
  EXPECT_EQ(queue.get_size(), 0);

  // now we can push again 
  for (uint64_t i = 0; i < queue_size; ++i) {
    ret = queue.push(audits[i + queue_size], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  EXPECT_EQ(queue_size, queue.get_pop_idx());
  EXPECT_EQ(2 * queue_size, queue.get_push_idx());
  EXPECT_EQ(queue.get_size(), queue_size);
  
  // check get function
  ObRaQueue::Ref ref; 
  for (uint64_t req_id = queue_size; req_id < 2 * queue_size; ++req_id) {
    auto *audit = queue.get(req_id, &ref);
    EXPECT_NE(audit, nullptr);
    EXPECT_EQ(static_cast<DummyAuditRecod*>(audit)->val, req_id);
    EXPECT_EQ(ref.idx_, global_id++);
    queue.revert(&ref);
  }

  // check pop function
  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = queue.pop();
    EXPECT_NE(audit, nullptr);
    free(audit);
  }
  EXPECT_EQ(0, queue.get_size());
}

TEST(TestObRaQueue, ConcurrentPushTest) {
  ObRaQueue queue;
  const uint64_t queue_size = 1024 * 1024 * 80;
  const uint64_t thread_num = 5;
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, queue_size);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size ; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(malloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }

  auto PushHelpr = [&queue, &audits](uint64_t thread_itr) {
    int ret = OB_SUCCESS;
    int64_t seq = 0;
    for (uint64_t i = 0; i < audits.size(); ++i) {
      if (i % thread_num == thread_itr) {
        ret = queue.push(audits[i], seq);
        EXPECT_EQ(ret, OB_SUCCESS);
      }
    }
  };

  std::vector<std::thread> thread_pool;
  for (uint64_t thread_itr = 0; thread_itr < thread_num; ++thread_itr) {
    thread_pool.emplace_back(PushHelpr, thread_itr);
  }
  for (auto &t : thread_pool) {
    t.join();
  }

  // check push success
  EXPECT_EQ(0, queue.get_pop_idx());
  EXPECT_EQ(queue_size, queue.get_push_idx());
  EXPECT_EQ(queue.get_size(), queue_size);

  // get
  ObRaQueue::Ref ref; 
  for (uint64_t req_id = 0; req_id < queue_size; ++req_id) {
    auto *audit = queue.get(req_id, &ref);
    EXPECT_NE(audit, nullptr);
    queue.revert(&ref);
  }

  // check pop function
  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = queue.pop();
    EXPECT_NE(audit, nullptr);
    free(audit);
  }
  EXPECT_EQ(0, queue.get_size());
}


} // namespace test
} // namespace oceanbase

int main(int argc, char *argv[])
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

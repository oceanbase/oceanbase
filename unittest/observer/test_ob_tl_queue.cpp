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
#include "observer/mysql/ob_tl_queue.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace test {

struct DummyAuditRecod {
  uint64_t val;
};

static const int64_t TOTAL_SIZE = 1024l * 1024l * 1024l * 8l;
static const int64_t MY_PAGE_SIZE = 64 * 1024;

TEST(TestObTLQueue, DummyTest) {
  EXPECT_EQ(1 + 1, 2);
}

TEST(TestObTLQueue, InitTest) {
  ObTLQueue queue;
  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));
  const uint64_t capacity = 100;
  const uint64_t subcapacity = 100;
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
}

TEST(TestObTLQueue, SizeTest1) {
  ObTLQueue queue;
  const uint64_t capacity = 1;
  const uint64_t subcapacity = 10;
  const uint64_t queue_size = capacity * subcapacity;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));

  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(0, queue.get_size());
  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }

  uint64_t idx = 0;
  int64_t seq = 0;
  ret = queue.push_with_retry(audits[idx++], seq);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(1, queue.get_size());
  ret = queue.pop_batch();
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, queue.get_size());

  while (idx < queue_size)  {
    ret = queue.push_with_retry(audits[idx++], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  EXPECT_EQ(queue_size - 1, queue.get_size());
  ret = queue.pop_batch();
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, queue.get_size());
}

TEST(TestObTLQueue, SizeTest2) {
  ObTLQueue queue;
  const uint64_t capacity = 2;
  const uint64_t subcapacity = 10;
  const uint64_t queue_size = capacity * subcapacity;
  const uint64_t item_size = queue_size + 5;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));

  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(0, queue.get_size());
  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < item_size; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }

  uint64_t idx = 0;
  int64_t seq = 0;
  for (uint64_t i = 0; i < queue_size; ++i) {
    ret = queue.push_with_retry(audits[i], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(i, seq);
  }
  EXPECT_EQ(queue_size, queue.get_size());

  // pop one batch
  ret = queue.pop_batch();
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(queue_size - subcapacity, queue.get_size());

  for (uint64_t i = queue_size; i < item_size; ++i) {
    ret = queue.push_with_retry(audits[i], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(i, seq);
  }
  EXPECT_EQ(item_size - subcapacity, queue.get_size());

  // pop and free
  while (OB_SUCC(queue.pop_batch())) {
    ;
  }
  EXPECT_EQ(0, queue.get_size());
}
TEST(TestObTLQueue, SizeTest3) {
  ObTLQueue queue;
  const uint64_t capacity = 2;
  const uint64_t subcapacity = 10;
  const uint64_t queue_size = capacity * subcapacity;
  const uint64_t item_size = queue_size - 1;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(0, queue.get_size());
  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < item_size; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }

  uint64_t idx = 0;
  int64_t seq = 0;
  ret = queue.pop_batch();
  EXPECT_EQ(0, queue.get_size());
  EXPECT_EQ(OB_ENTRY_NOT_EXIST, ret);

  while (idx < item_size)  {
    ret = queue.push_with_retry(audits[idx++], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
  }

  EXPECT_EQ(item_size, queue.get_size());
  ret = queue.pop_batch();
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(subcapacity - 1, queue.get_size());

  ret = queue.pop_batch();
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(0, queue.get_size());
}

TEST(TestObTLQueue, BasicTest) {
  ObTLQueue queue;
  const uint64_t capacity = 6;
  const uint64_t subcapacity = 10;
  const uint64_t queue_size = capacity * subcapacity;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);
  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }
  int64_t seq = 0;
  int64_t global_id = 0;
  for (auto *audit : audits) {
    ret = queue.push_with_retry(audit, seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(global_id++, seq);
  }

  // check get_pop_idx and get_push_idx
  EXPECT_EQ(0, queue.get_head_idx());
  EXPECT_EQ(queue_size, queue.get_tail_idx());

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
  for (uint64_t i = 0; i < capacity; ++i) {
    ret = queue.pop_batch();
    EXPECT_EQ(ret, OB_SUCCESS);
  }

  EXPECT_EQ(0, queue.get_size());
}

TEST(TestObTLQueue, PushTest1) {
  ObTLQueue queue;
  const uint64_t capacity = 10;
  const uint64_t subcapacity = 100;
  const uint64_t queue_size = capacity * subcapacity;
  const uint64_t item_size = queue_size + 1;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < item_size ; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }
  int64_t seq = 0;
  int64_t global_id = 0;
  for (uint64_t i = 0;  i < queue_size; ++i) {
    ret = queue.push_with_retry(audits[i], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(global_id++, seq);
  }

  // check get_pop_idx and get_push_idx
  EXPECT_EQ(0, queue.get_head_idx());
  EXPECT_EQ(queue_size, queue.get_tail_idx());
  EXPECT_EQ(queue_size, queue.get_size());

  // push again
  for (uint64_t i = queue_size; i < item_size; ++i) {
    ret = queue.push_with_retry(audits[i], seq);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  }
  EXPECT_EQ(queue.get_size(), queue_size);

  ret = queue.pop_batch();
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(1 * subcapacity, queue.get_head_idx());
  EXPECT_EQ(queue_size, queue.get_tail_idx());
  EXPECT_EQ(queue.get_size(), queue_size - subcapacity);

  // now we can push again 
  for (uint64_t i = queue_size; i < item_size; ++i) {
    ret = queue.push_with_retry(audits[i], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  EXPECT_EQ(1 * subcapacity, queue.get_head_idx());
  EXPECT_EQ(queue_size + 1, queue.get_tail_idx());
  EXPECT_EQ(queue.get_size(), item_size - subcapacity);
  
  // pop and free
  while (OB_SUCC(queue.pop_batch())) {
    ;
  }
  EXPECT_EQ(0, queue.get_size());
}
TEST(TestObTLQueue, PushTest2) {
  ObTLQueue queue;
  const uint64_t capacity = 100;
  const uint64_t subcapacity = 1000;
  const uint64_t queue_size = capacity * subcapacity;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size * 2 ; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }
  int64_t seq = 0;
  int64_t global_id = 0;
  for (uint64_t i = 0;  i < queue_size; ++i) {
    ret = queue.push_with_retry(audits[i], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_EQ(global_id++, seq);
  }

  // check get_pop_idx and get_push_idx
  EXPECT_EQ(0, queue.get_head_idx());
  EXPECT_EQ(queue_size, queue.get_tail_idx());
  EXPECT_EQ(queue.get_size(), queue_size);

  // push again
  for (uint64_t i = 0; i < queue_size; ++i) {
    ret = queue.push_with_retry(audits[i + queue_size], seq);
    EXPECT_EQ(OB_SIZE_OVERFLOW, ret);
  }
  EXPECT_EQ(queue.get_size(), queue_size);

  while (OB_SUCC(queue.pop_batch())) ;
  EXPECT_EQ(queue.get_size(), 0);

  // now we can push again 
  for (uint64_t i = 0; i < queue_size; ++i) {
    ret = queue.push_with_retry(audits[i + queue_size], seq);
    EXPECT_EQ(OB_SUCCESS, ret);
  }
  EXPECT_EQ(queue_size, queue.get_head_idx());
  EXPECT_EQ(2 * queue_size, queue.get_tail_idx());
  EXPECT_EQ(queue.get_size(), queue_size);
  
  while (OB_SUCC(queue.pop_batch())) ;
  EXPECT_EQ(0, queue.get_size());
}

TEST(TestObTLQueue, ConcurrentPushTest) {
  ObTLQueue queue;
  const uint64_t capacity = 1024; 
  const uint64_t subcapacity = 50000; // 5w
  const uint64_t queue_size = capacity * subcapacity;  // 5000w
  const uint64_t thread_num = 5;

  ObConcurrentFIFOAllocator allocator;
  ASSERT_EQ(OB_SUCCESS, allocator.init(TOTAL_SIZE, TOTAL_SIZE, MY_PAGE_SIZE));
  int ret = queue.init(ObModIds::OB_MYSQL_REQUEST_RECORD, capacity, subcapacity, &allocator);
  EXPECT_EQ(OB_SUCCESS, ret);

  std::vector<DummyAuditRecod*> audits;
  for (uint64_t i = 0; i < queue_size ; ++i) {
    auto *audit = static_cast<DummyAuditRecod*>(allocator.alloc(sizeof(DummyAuditRecod)));
    audit->val = i;
    audits.push_back(audit);
  }

  auto PushHelpr = [&queue, &audits](uint64_t thread_itr) {
    int ret = OB_SUCCESS;
    int64_t seq = 0;
    for (uint64_t i = 0; i < audits.size(); ++i) {
      if (i % thread_num == thread_itr) {
        ret = queue.push_with_retry(audits[i], seq);
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
  EXPECT_EQ(queue.get_size(), queue_size);

  // get
  ObRaQueue::Ref ref; 
  for (uint64_t req_id = 0; req_id < queue_size; ++req_id) {
    auto *audit = queue.get(req_id, &ref);
    EXPECT_NE(audit, nullptr);
    queue.revert(&ref);
  }

  // check pop function
  while (OB_SUCC(queue.pop_batch())) {
    ;
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

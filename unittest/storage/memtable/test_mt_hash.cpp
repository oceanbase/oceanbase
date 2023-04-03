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
#include "lib/allocator/ob_malloc.h"
#include "storage/memtable/ob_memtable_key.h"
#include "storage/memtable/ob_mt_hash.h"

namespace oceanbase
{
namespace unittest
{
using namespace oceanbase::common;
using namespace oceanbase::memtable;

class ObTestMemtableKey : public ObMemtableKey
{
public:
  ObTestMemtableKey(const uint64_t test_hash, const int64_t table_id)
  {
    hash_val_ = test_hash;
    table_id_ = table_id;
  }
};

class ObTestAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size)
  {
    return malloc(size);
  }
};

// global instance
ObTestAllocator allocator;
ObMtHash mt_hash(allocator);

// ----------- insert thread ------------
#define PERF_KV_COUNT (1000)
#define PERF_INSERT_ROW_ROW_ROW_ROW_ROW_ROW_THREAD_NUM 64

int64_t total_count = 0;
int64_t total_time = 0;

void* do_insert(void* data)
{
  int param = *(int*)(data);
  TRANS_LOG(INFO, "thread running", "tid", get_itid(), K(param));

  int64_t count = 0;
  const int64_t R = 10L * PERF_KV_COUNT;
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  char *mtk_buf = NULL;
  ObTestMemtableKey *mtk = NULL;
  while (count++ < PERF_KV_COUNT && OB_SUCCESS == ret) {
    int64_t table_id = param * R + count;
    uint64_t hash = murmurhash(&table_id, sizeof(table_id), 0);
    //ObTestMemtableKey mtk(hash, table_id);
    mtk_buf = (char *)(allocator.alloc(sizeof(ObTestMemtableKey)));
    mtk = new (mtk_buf) ObTestMemtableKey(hash, table_id);
    ObMemtableKeyWrapper mtk_wrapper(mtk);
    ObMvccRow *row = reinterpret_cast<ObMvccRow*>(table_id);
    ret = mt_hash.insert(&mtk_wrapper, row);
  }
  int64_t end_ts = ObTimeUtility::current_time();
  ATOMIC_FAA(&total_count, count);
  ATOMIC_FAA(&total_time, (end_ts - start_ts));
  if (OB_SUCCESS != ret) {
    fprintf(stdout, "ret=%d, tid=%ld, elapse=%ld, count=%ld\n", ret, get_itid(), (end_ts - start_ts), count);
  }
  TRANS_LOG(INFO, "thread running", "tid", get_itid(), K(param));
  return NULL;
}

// ------------ get thread -----------------
#define PERF_GET_THREAD_NUM 64
void* do_get(void* data)
{
  int param = *(int*)(data);
  TRANS_LOG(INFO, "thread running", "tid", get_itid(), K(param));
  int64_t count = 0;
  const int64_t R = 10L * PERF_KV_COUNT;
  int ret = OB_SUCCESS;
  int64_t start_ts = ObTimeUtility::current_time();
  ObMvccRow *ret_row = NULL;
  char *mtk_buf = NULL;
  ObTestMemtableKey *mtk = NULL;
  while (count++ < PERF_KV_COUNT && OB_SUCCESS == ret) {
    int64_t table_id = param * R + count;
    uint64_t hash = murmurhash(&table_id, sizeof(table_id), 0);
    mtk_buf = (char *)(allocator.alloc(sizeof(ObTestMemtableKey)));
    mtk = new (mtk_buf) ObTestMemtableKey(hash, table_id);
    ObMemtableKeyWrapper mtk_wrapper(mtk);
    ret = mt_hash.get(&mtk_wrapper, ret_row);
  }
  int64_t end_ts = ObTimeUtility::current_time();
  ATOMIC_FAA(&total_count, count);
  ATOMIC_FAA(&total_time, (end_ts - start_ts));
  if (OB_SUCCESS != ret) {
    fprintf(stdout, "ret=%d, tid=%ld, elapse=%ld, count=%ld\n", ret, get_itid(), (end_ts - start_ts), count);
  }
  TRANS_LOG(INFO, "thread running", "tid", get_itid(), K(param));
  return NULL;
}

TEST(TestMtHash, perf_test)
{
  {
    TRANS_LOG(INFO, "insert perf test start");
    fprintf(stdout, "insert perf test start\n");
    pthread_t threads[PERF_INSERT_ROW_ROW_ROW_ROW_ROW_ROW_THREAD_NUM];
    int thread_param[PERF_INSERT_ROW_ROW_ROW_ROW_ROW_ROW_THREAD_NUM];
    int err = 0;
    for (int i = 0; i < PERF_INSERT_ROW_ROW_ROW_ROW_ROW_ROW_THREAD_NUM; i++) {
      thread_param[i] = i + 1;
      err = pthread_create(threads + i, NULL, do_insert, &(thread_param[i]));
      ASSERT_EQ(0, err);
    }
    for (int i = 0; i < PERF_INSERT_ROW_ROW_ROW_ROW_ROW_ROW_THREAD_NUM; i++) {
      pthread_join(threads[i], NULL);
    }
    int64_t ops = (int64_t)(total_count / (total_time * 1.0L / 1000 / 1000));
    fprintf(stdout, "insert perf: total_count=%ld, total_time=%ld, OPST= %ld\n", total_count, total_time, ops);
    ATOMIC_STORE(&total_time, 0);
    ATOMIC_STORE(&total_count, 0);
    TRANS_LOG(INFO, "insert perf test finish");
    fprintf(stdout, "insert perf test finish\n");
  }


  {
    sleep(5);
    TRANS_LOG(INFO, "get perf test start");
    fprintf(stdout, "get perf test start\n");
    pthread_t threads[PERF_GET_THREAD_NUM];
    int thread_param[PERF_GET_THREAD_NUM];
    int err = 0;
    for (int i = 0; i < PERF_INSERT_ROW_ROW_ROW_ROW_ROW_ROW_THREAD_NUM; i++) {
      thread_param[i] = i + 1;
      err = pthread_create(threads + i, NULL, do_get, &(thread_param[i]));
      ASSERT_EQ(0, err);
    }
    for (int i = 0; i < PERF_GET_THREAD_NUM; i++) {
      pthread_join(threads[i], NULL);
    }
    int64_t ops = (int64_t)(total_count / (total_time * 1.0L / 1000 / 1000));
    fprintf(stdout, "get    perf: NO WARMUP total_count=%ld, total_time=%ld, OPST= %ld\n", total_count, total_time, ops);
    fprintf(stdout, "start test warm get");
    ATOMIC_STORE(&total_time, 0);
    ATOMIC_STORE(&total_count, 0);

    sleep(5);
    for (int i = 0; i < PERF_GET_THREAD_NUM; i++) {
      thread_param[i] = i + 1;
      err = pthread_create(threads + i, NULL, do_get, &(thread_param[i]));
      ASSERT_EQ(0, err);
    }
    for (int i = 0; i < PERF_GET_THREAD_NUM; i++) {
      pthread_join(threads[i], NULL);
    }
    ops = (int64_t)(total_count / (total_time * 1.0L / 1000 / 1000));
    fprintf(stdout, "get    perf: WARMUP total_count=%ld, total_time=%ld, OPST= %ld\n", total_count, total_time, ops);
  }
}

// ------------ multi thread mix test ----------------
#define MIX_TEST_KV_COUNT (100)
#define MIX_TEST_THREAD_NUM 64
void dump_mt_hash(const char* fname)
{
  fprintf(stdout, "begin dump mt_hash into %s\n", fname);
  FILE *fd = NULL;
  if (NULL == (fd = fopen(fname, "w"))) {
    TRANS_LOG(ERROR, "open file fail", K(fname));
  } else {
    mt_hash.dump_hash(fd, true, false, false);
  }
}

void* thread_routine(void* data)
{
  int param = *(int*)(data);
  TRANS_LOG(INFO, "thread running", "tid", get_itid(), K(param));

  int64_t count = 0;
  int ins_ret = OB_SUCCESS;
  int get_ret = OB_SUCCESS;
  const int64_t R = MIX_TEST_KV_COUNT * 100;
  char *mtk_buf = NULL;
  ObTestMemtableKey *mtk = NULL;
  while (count < MIX_TEST_KV_COUNT) {
    int64_t table_id = param * R + ObRandom::rand(0, R);
    uint64_t hash = murmurhash(&table_id, sizeof(table_id), 0);
    mtk_buf = (char *)(allocator.alloc(sizeof(ObTestMemtableKey)));
    mtk = new (mtk_buf) ObTestMemtableKey(hash, table_id);
    ObMemtableKeyWrapper mtk_wrapper(mtk);
    ObMvccRow *row = reinterpret_cast<ObMvccRow*>(table_id);
    ins_ret = mt_hash.insert(&mtk_wrapper, row);
    ObMvccRow *ret_row = NULL;
    get_ret = mt_hash.get(&mtk_wrapper, ret_row);
    OB_ASSERT(OB_SUCCESS == get_ret);
    if (OB_SUCCESS == ins_ret) {
      OB_ASSERT(ret_row == row);
    } else {
    }
    if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
      fprintf(stdout, "param=%d, count=%ld\n", param, count);
    }
    count++;
  }
  fprintf(stdout, "test insert success, thread_param=%d, count=%ld\n", param, --count);
  return NULL;
}

TEST(TestMtHash, multi_thread_test)
{
  TRANS_LOG(INFO, "multi_thread_test start");
  pthread_t threads[MIX_TEST_THREAD_NUM];
  int thread_param[MIX_TEST_THREAD_NUM];
  int err = 0;
  for (int i = 0; i < MIX_TEST_THREAD_NUM; i++) {
    thread_param[i] = i + 1;
    err = pthread_create(threads + i, NULL, thread_routine, &(thread_param[i]));
    ASSERT_EQ(0, err);
  }
  for (int i = 0; i < MIX_TEST_THREAD_NUM; i++) {
    pthread_join(threads[i], NULL);
  }

  dump_mt_hash("dump_mt_hash.txt");

  fprintf(stdout, "test insert success, main thread\n");
  TRANS_LOG(INFO, "multi_thread_test finish");
}

} // namespace unittest  end
} // namespace oceanbase end

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("mt_hash.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  OB_LOGGER.set_log_level("TRACE");
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

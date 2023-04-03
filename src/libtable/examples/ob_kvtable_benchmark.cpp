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

#include "libobtable.h"
#include "lib/thread/thread_pool.h"
using namespace oceanbase::table;
using namespace oceanbase::common;
/*
 * create a table as following before run this example
 * create table t3 (K varbinary(1024), Q varchar(256), T bigint, V varbinary(1024), primary key(K, Q, T));
 *
 */
void usage(const char* progname)
{
  fprintf(stdout, "Usage: %s <observer_host> <port> <tenant> <user> <password> <database> <table> <rpc_port> {prepare|run} <client_type> <thread_num> <rows> <net_io_thread_num> <value_len> <batch_size> <duration> <root_sys_password>\n", progname);
  fprintf(stdout, "Example: ./kvtable_bench '100.88.11.96' 50803 sys root '' test t2 50802 run 2 200 1000000 16 1024 100 120 rootsyspass\n");
}

#define CHECK_RET(ret) \
  if (OB_SUCCESS != (ret)) {                    \
    fprintf(stdout, "error: %d at %d\n", ret, __LINE__);       \
    exit(-1);                                   \
  }
// const
static const char* const Q_VAL = "qualifier";
static const int64_t T_VAL = 1;
int64_t V_LEN = 1024;
static const int64_t PREPARE_STEP = 10000;

// global variables
int64_t ROWS = 1000;
int32_t REPORT_INTERVAL_SEC = 5;
int64_t DURATION_SEC = 120;
int64_t CLIENT_TYPE = 2;
const char* TABLE_NAME = "";
struct MyCounter
{
  int64_t GET_COUNT;
  int64_t PUT_COUNT;
  int64_t FAIL_COUNT;
  int64_t ELAPSED_US;
  MyCounter()
      :GET_COUNT(0),
       PUT_COUNT(0),
       FAIL_COUNT(0),
       ELAPSED_US(0)
  {}
};
MyCounter *COUNTERS = NULL;
int32_t THREAD_NUM = 200;
bool OPT_DEBUG = false;
enum class ActionType
{
  PREPARE = 0,
  RUN_GET = 1,
  RUN_PUT = 2
};
ActionType ACTION_TYPE = ActionType::PREPARE;
static const int64_t MAX_BATCH_SIZE = 1000;
int32_t BATCH_SIZE = 1;
typedef char KeyBuf[128];

// return [0, n-1]
int32_t rand_int(unsigned *seedp, int32_t n)
{
  int32_t v = rand_r(seedp);
  int32_t r = static_cast<int32_t>(static_cast<double>(v) / static_cast<double>(RAND_MAX) * n);
  if (r < 0) {
    r = 0;
  } else if (r > n) {
    r = n;
  }
  return r;
}

void init_generate_k(char *buf, int64_t buf_len)
{
  snprintf(buf, buf_len, "Pneumonoultramicroscopicsilicovolcanoconiosis-%07ld-Pneumonoultramicroscopicsilicovolcanoconiosis-", 0L);
  //fprintf(stderr, "key pattern=<%s>\n", buf);
}

void generate_k(char *buf, int64_t buf_len, int64_t j)
{
  UNUSED(buf_len);
  char tmp[10];
  snprintf(tmp, 10, "%07ld", j);
  memcpy(buf+46, tmp, 7);
}

void generate_v(unsigned *seedp, char *buf, int64_t buf_len)
{
  for (int64_t i = 0; i < buf_len-1; ++i) {
    buf[i] = static_cast<char>('A' + rand_int(seedp, 25));
  }
  buf[buf_len-1] = '\0';
}

class WorkerThread: public ::oceanbase::lib::ThreadPool
{
public:
  WorkerThread(ObTableServiceClient &service_client, int32_t thread_num)
      : service_client_(service_client)
  {
    set_thread_count(thread_num);
  }
  virtual ~WorkerThread() {}
  virtual void run1() override;
private:
  static void prepare(ObHKVTable* kv_table, unsigned *seedp, int64_t step, int64_t idx);
  void get(long thread_idx, ObHKVTable* kv_table, unsigned *seedp);
  void put(long thread_idx, ObHKVTable* kv_table, unsigned *seedp);
  void multi_get(long thread_idx, ObHKVTable* kv_table, unsigned *seedp);
  void multi_put(long thread_idx, ObHKVTable* kv_table, unsigned *seedp);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(WorkerThread);
private:
  // data members
  ObTableServiceClient &service_client_;
};

void WorkerThread::prepare(ObHKVTable* kv_table, unsigned *seedp, int64_t step, int64_t idx)
{
  int ret = OB_SUCCESS;
  const int64_t start_id = step * idx;
  const int64_t end_id = start_id + step;
  int64_t succ_count = 0;
  int64_t fail_count = 0;
  fprintf(stdout, "[thread %ld] prepare rows from %ld to %ld\n", idx, start_id, end_id);
  static const int64_t PREPARE_BATCH_SIZE = 4;
  ObHKVTable::Key key;
  ObHKVTable::Value value;

  ObHKVTable::Keys keys;
  ObHKVTable::Values values;
  KeyBuf *k_buff = new (std::nothrow) KeyBuf[MAX_BATCH_SIZE];
  if (NULL == k_buff) {
    CHECK_RET(1);
  }
  for (int64_t i = 0; i < BATCH_SIZE; ++i) {
    init_generate_k(k_buff[i], 128);
  }
  char v_buff[MAX_BATCH_SIZE][V_LEN+1];
  key.column_qualifier_ = ObString::make_string(Q_VAL);
  key.version_ = T_VAL;

  int64_t begin_ts = ObTimeUtility::current_time();
  int64_t prev_ts = 0;
  int64_t k_part = start_id;
  int64_t prev_report_idx = 0;
  int64_t put_count = 0;
  int64_t prev_put_count = 0;
  while (k_part < end_id) {
    keys.reset();
    values.reset();
    for (int64_t i = 0; i < PREPARE_BATCH_SIZE && k_part < end_id; ++i, k_part++) {
      generate_k(k_buff[i], 128, k_part);
      key.rowkey_.assign_ptr(k_buff[i], 100);

      generate_v(seedp, v_buff[i], V_LEN+1);
      value.set_varchar(ObString::make_string(v_buff[i]));
      value.set_collation_type(CS_TYPE_BINARY);
      ret = keys.push_back(key);
      CHECK_RET(ret);
      ret = values.push_back(value);
      CHECK_RET(ret);
    }
    ret = kv_table->multi_put(keys, values);
    if (OB_SUCCESS != ret) {
      fprintf(stdout, "failed to multi_put row: %d\n", ret);
      fail_count+=keys.count();
    } else {
      succ_count+=keys.count();
      put_count++;
      //fprintf(stdout, "put row succ: n=%ld\n", succ_count);
    }
    if (k_part-start_id > prev_report_idx+1000) {
      int64_t now = ObTimeUtility::current_time();
      int64_t total_elapsed_us = now - begin_ts;
      double put_rt = -0.1;
      if (prev_ts > 0) {
        double elapsed_us = static_cast<double>(now - prev_ts);
        if (0 != put_count - prev_put_count) {
          put_rt = (elapsed_us)/static_cast<double>(put_count-prev_put_count);
        } else {
          fprintf(stderr, "devide by zero, put_count=%ld, prev_put_count=%ld", put_count, prev_put_count);
        }

      }
      fprintf(stdout, "[% 5lds] thread: %ld, inserted %ld rows, errors: %ld, rt: %f\n",
              total_elapsed_us/1000000, idx, succ_count, fail_count, put_rt);
      prev_report_idx = k_part-start_id;
      prev_ts = now;
      prev_put_count = put_count;
    }
  }
  delete [] k_buff;
}

void WorkerThread::get(long thread_idx, ObHKVTable* kv_table, unsigned *seedp)
{
  int ret = OB_SUCCESS;
  ObHKVTable::Key key;
  ObHKVTable::Value value;
  char k_buff[128];
  init_generate_k(k_buff, 128);
  key.rowkey_.assign_ptr(k_buff, 100);
  key.column_qualifier_ = ObString::make_string(Q_VAL);
  key.version_ = T_VAL;
  int64_t begin_ts = ObTimeUtility::current_time();
  while(!has_set_stop()) {
    int64_t j = rand_int(seedp, static_cast<int32_t>(ROWS-1));
    generate_k(k_buff, 128, j);
    ret = kv_table->get(key, value);
    if (OB_SUCCESS != ret) {
      //fprintf(stdout, "failed to get row: %d\n", ret);
      COUNTERS[thread_idx].FAIL_COUNT++;
    } else {
      //fprintf(stdout, "[%ld]get row succ. %s\n", GET_COUNT, S(value));
    }
    COUNTERS[thread_idx].GET_COUNT++;
    if (COUNTERS[thread_idx].GET_COUNT % 100 == 0) {
      int64_t now = ObTimeUtility::current_time();
      COUNTERS[thread_idx].ELAPSED_US = now - begin_ts;
    }
  }
}

void WorkerThread::put(long thread_idx, ObHKVTable* kv_table, unsigned *seedp)
{
  int ret = OB_SUCCESS;
  ObHKVTable::Key key;
  ObHKVTable::Value value;
  char k_buff[128];
  init_generate_k(k_buff, 128);
  key.rowkey_.assign_ptr(k_buff, 100);
  char v_buff[V_LEN+1];
  key.column_qualifier_ = ObString::make_string(Q_VAL);
  key.version_ = T_VAL;
  int64_t begin_ts = ObTimeUtility::current_time();
  while(!has_set_stop()) {
    int64_t j = rand_int(seedp, static_cast<int32_t>(ROWS-1));
    generate_k(k_buff, 128, j);
    generate_v(seedp, v_buff, V_LEN+1);
    value.set_varchar(ObString::make_string(v_buff));
    ret = kv_table->put(key, value);
    if (OB_SUCCESS != ret) {
      fprintf(stdout, "failed to put row: %d\n", ret);
      COUNTERS[thread_idx].FAIL_COUNT++;
    } else {
      //fprintf(stdout, "[%ld]get row succ. %s\n", GET_COUNT, S(value));
    }
    COUNTERS[thread_idx].PUT_COUNT++;
    if (COUNTERS[thread_idx].PUT_COUNT % 100 == 0) {
      int64_t now = ObTimeUtility::current_time();
      COUNTERS[thread_idx].ELAPSED_US = now - begin_ts;
    }
  }
}

void WorkerThread::multi_get(long thread_idx, ObHKVTable* kv_table, unsigned *seedp)
{
  int ret = OB_SUCCESS;
  ObHKVTable::Key key;
  ObHKVTable::Keys keys(ObModIds::TEST, OB_MALLOC_NORMAL_BLOCK_SIZE);
  ObHKVTable::Values values(ObModIds::TEST, OB_MALLOC_NORMAL_BLOCK_SIZE);
  KeyBuf *k_buff = new (std::nothrow) KeyBuf[MAX_BATCH_SIZE];
  if (NULL == k_buff) {
    CHECK_RET(1);
  }
  for (int64_t i = 0; i < BATCH_SIZE; ++i) {
    init_generate_k(k_buff[i], 128);
  }
  key.column_qualifier_ = ObString::make_string(Q_VAL);
  key.version_ = T_VAL;
  int64_t begin_ts = ObTimeUtility::current_time();
  while(!has_set_stop()) {
    keys.reset();
    for (int64_t i = 0; i < BATCH_SIZE; ++i) {
      key.rowkey_.assign_ptr(k_buff[i], 100);
      int64_t j = rand_int(seedp, static_cast<int32_t>(ROWS-1));
      generate_k(k_buff[i], 128, j);
      ret = keys.push_back(key);
      CHECK_RET(ret);
    }
    ret = kv_table->multi_get(keys, values);
    if (OB_SUCCESS != ret) {
      fprintf(stdout, "failed to multi-get row: %d\n", ret);
      COUNTERS[thread_idx].FAIL_COUNT++;
    } else {
      //fprintf(stdout, "[%ld]get row succ. %s\n", GET_COUNT, S(value));
    }
    COUNTERS[thread_idx].GET_COUNT ++;
    if (COUNTERS[thread_idx].GET_COUNT % 100 == 0) {
      int64_t now = ObTimeUtility::current_time();
      COUNTERS[thread_idx].ELAPSED_US = now - begin_ts;
    }
  }
  delete [] k_buff;
}

void WorkerThread::multi_put(long thread_idx, ObHKVTable* kv_table, unsigned *seedp)
{
  int ret = OB_SUCCESS;
  ObHKVTable::Key key;
  ObHKVTable::Value value;
  ObHKVTable::Keys keys;
  ObHKVTable::Values values;
  KeyBuf *k_buff = new (std::nothrow) KeyBuf[MAX_BATCH_SIZE];
  if (NULL == k_buff) {
    CHECK_RET(1);
  }
  for (int64_t i = 0; i < BATCH_SIZE; ++i) {
    init_generate_k(k_buff[i], 128);
  }
  char v_buff[MAX_BATCH_SIZE][V_LEN+1];
  key.column_qualifier_ = ObString::make_string(Q_VAL);
  key.version_ = T_VAL;
  int64_t begin_ts = ObTimeUtility::current_time();
  while(!has_set_stop()) {
    keys.reset();
    values.reset();
    for (int64_t i = 0; i < BATCH_SIZE; ++i) {
      int64_t j = rand_int(seedp, static_cast<int32_t>(ROWS-1));
      generate_k(k_buff[i], 128, j);
      key.rowkey_.assign_ptr(k_buff[i], 100);
      generate_v(seedp, v_buff[i], V_LEN+1);
      value.set_varchar(ObString::make_string(v_buff[i]));
      value.set_collation_type(CS_TYPE_BINARY);
      ret = keys.push_back(key);
      CHECK_RET(ret);
      ret = values.push_back(value);
      CHECK_RET(ret);
    }
    ret = kv_table->multi_put(keys, values);
    if (OB_SUCCESS != ret) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        fprintf(stdout, "failed to multi-put row: %d\n", ret);
      }
      COUNTERS[thread_idx].FAIL_COUNT++;
    } else {
      //fprintf(stdout, "[%ld]get row succ. %s\n", GET_COUNT, S(value));
      COUNTERS[thread_idx].PUT_COUNT++;
    }
    if (COUNTERS[thread_idx].PUT_COUNT % 100 == 0) {
      int64_t now = ObTimeUtility::current_time();
      COUNTERS[thread_idx].ELAPSED_US = now - begin_ts;
    }
  }
  delete [] k_buff;
}

void WorkerThread::run1()
{
  long thread_idx = (long)get_thread_idx();
  int ret = OB_SUCCESS;
  // init a table using the client
  ObHKVTable* kv_table = NULL;
  if (1 == CLIENT_TYPE) {
    ret = service_client_.alloc_hkv_table_v1(ObString::make_string(TABLE_NAME), kv_table);
  } else {
    ret = service_client_.alloc_hkv_table_v2(ObString::make_string(TABLE_NAME), kv_table);
  }
  CHECK_RET(ret);
  if (OPT_DEBUG) {
    fprintf(stdout, "Worker thread started [%ld]\n", thread_idx);
  }
  unsigned seed = static_cast<unsigned>(time(NULL));
  switch(ACTION_TYPE) {
    case ActionType::PREPARE:
      prepare(kv_table, &seed, PREPARE_STEP, thread_idx);
      break;
    case ActionType::RUN_GET:
      if (1 == BATCH_SIZE) {
        get(thread_idx, kv_table, &seed);
      } else {
        multi_get(thread_idx, kv_table, &seed);
      }
      break;
    case ActionType::RUN_PUT:
      if (1 == BATCH_SIZE) {
        put(thread_idx, kv_table, &seed);
      } else {
        multi_put(thread_idx, kv_table, &seed);
      }
      break;
  }
  // cleanup
  service_client_.free_hkv_table(kv_table);
  if (OPT_DEBUG) {
    fprintf(stdout, "Worker thread stopped.\n");
  }
}

int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (argc != 18) {
    usage(argv[0]);
    return -1;
  }
  // parse the arguments
  const char* host = argv[1];
  int32_t port = atoi(argv[2]);
  const char* tenant = argv[3];
  const char* user = argv[4];
  const char* passwd = argv[5];
  const char* db = argv[6];
  TABLE_NAME = argv[7];
  int32_t rpc_port = atoi(argv[8]);
  const char* action = argv[9];
  CLIENT_TYPE = atoi(argv[10]);
  THREAD_NUM = atoi(argv[11]);
  ROWS = atoi(argv[12]);
  int64_t net_io_thread_num = atoi(argv[13]);
  V_LEN = atoi(argv[14]);
  BATCH_SIZE = atoi(argv[15]);
  if (BATCH_SIZE <= 0 || BATCH_SIZE > MAX_BATCH_SIZE) {
    fprintf(stdout, "invalid batch_size = %d max_batch_size=%ld\n", BATCH_SIZE, MAX_BATCH_SIZE);
    return -2;
  }
  DURATION_SEC = atoi(argv[16]);
  const char *root_sys_pass = argv[17];

  fprintf(stdout, "host=%s \nmysql_port=%d \nrpc_port=%d \nclient_type=%ld \nthread_num=%d \nrows=%ld \nio_thread_num=%ld \nduration=%lds\nvalue_len=%ld\nbatch_size=%d\n",
          host, port, rpc_port, CLIENT_TYPE, THREAD_NUM, ROWS, net_io_thread_num, DURATION_SEC, V_LEN, BATCH_SIZE);

  // 1. init the library
  ret = ObTableServiceLibrary::init();
  // 2. init the client
  ObTableServiceClient* p_service_client = ObTableServiceClient::alloc_client();
  ObTableServiceClient &service_client = *p_service_client;
  ObTableServiceClientOptions options;
  options.set_net_io_thread_num(net_io_thread_num);
  service_client.set_options(options);
  ret = service_client.init(ObString::make_string(host), port, rpc_port,
                            ObString::make_string(tenant), ObString::make_string(user),
                            ObString::make_string(passwd), ObString::make_string(db),
                            ObString::make_string(root_sys_pass));
  CHECK_RET(ret);
  ////////////////
  int32_t thread_num = THREAD_NUM;
  if (0 == strcmp(action, "prepare")) {
    ACTION_TYPE = ActionType::PREPARE;
    thread_num = static_cast<int32_t>((ROWS / PREPARE_STEP) + (0 == ROWS%PREPARE_STEP ? 0 : 1));
  } else if (0 == strcmp(action, "run_get")) {
    ACTION_TYPE = ActionType::RUN_GET;
  } else if (0 == strcmp(action, "run_put")) {
    ACTION_TYPE = ActionType::RUN_PUT;
  } else {
    fprintf(stdout, "ERROR: no action\n");
    CHECK_RET(1);
  }
  if (thread_num <= 0 || thread_num > 10000) {
    fprintf(stderr, "Invalid argument, thread_num=%d\n", thread_num);
  } else {
    COUNTERS = new (std::nothrow) MyCounter[thread_num];
  }
  if (NULL == COUNTERS)
  {
    CHECK_RET(2);
  }
  {
    fprintf(stdout, "Initializing %d worker threads...\n", thread_num);
    // execute the benchmark
    WorkerThread workers(service_client, thread_num);
    int64_t begin_ts = ObTimeUtility::current_time();
    int64_t end_ts = begin_ts + (1000000L * DURATION_SEC);
    int64_t last_report_ts = begin_ts;
    if (OB_FAIL(workers.start())) {
      fprintf(stdout, "Failed to start threads!\n");
    } else {
      fprintf(stdout, "%d Threads started!\n", THREAD_NUM);
    }

    int64_t last_get_count = 0;
    int64_t last_fail_count = 0;
    int64_t last_put_count = 0;
    while(1 && ACTION_TYPE != ActionType::PREPARE) {
      sleep(REPORT_INTERVAL_SEC);
      int64_t now = ObTimeUtility::current_time();
      int64_t curr_get_count = 0;
      int64_t curr_fail_count = 0;
      int64_t curr_put_count = 0;
      int64_t total_time = 0;
      for (int64_t i = 0; i < thread_num; ++i) {
        curr_get_count += COUNTERS[i].GET_COUNT;
        curr_fail_count += COUNTERS[i].FAIL_COUNT;
        curr_put_count += COUNTERS[i].PUT_COUNT;
        total_time += COUNTERS[i].ELAPSED_US;
      }
      int64_t total_elapsed_us = now - begin_ts;
      double elapsed_sec = static_cast<double>(now - last_report_ts)/1000000.0;
      double reads_per_sec = static_cast<double>(curr_get_count-last_get_count) / elapsed_sec;
      double writes_per_sec = static_cast<double>(curr_put_count-last_put_count) / elapsed_sec;
      double errors_per_sec = static_cast<double>(curr_fail_count-last_fail_count) / elapsed_sec;
      double rt = 0;
      if (0 == curr_get_count + curr_put_count) {
        fprintf(stderr, "devide by 0, curr_get_count=%ld, curr_put_count=%ld\n", curr_get_count, curr_put_count);
      } else {
        rt = static_cast<double>(total_time / (curr_get_count+curr_put_count)) / 1000.0;
      }

      // [ 108s] threads: 500, tps: 0.00, reads: 152384.80, writes: 0.00, response time: 7.50ms (95%), errors: 0.00, reconnects:  0.00
      if (OPT_DEBUG) {
        fprintf(stdout, "t=%ld last_get_count=%ld curr_get_count=%ld last_put_count=%ld curr_put_count=%ld elapsed_sec=%f\n",
                total_elapsed_us, last_get_count, curr_get_count, last_put_count, curr_put_count, elapsed_sec);
      }
      fprintf(stdout, "[% 5lds] threads: %d, tps: 0.00, reads: %.2f, writes: %.2f, response time: %.2fms (avg), errors: %.2f\n",
              total_elapsed_us/1000000, THREAD_NUM, reads_per_sec, writes_per_sec, rt, errors_per_sec);
      fflush(stdout);
      last_report_ts = now;
      last_get_count = curr_get_count;
      last_fail_count = curr_fail_count;
      last_put_count = curr_put_count;

      if (now > end_ts) {
        break;
      }
    }  // end while
    workers.stop();
    workers.wait();
  }
  ////////////////
  // report
  /*
  2018-01-30 22:14:16 sysbench test finished
      OLTP test statistics:
      queries performed:
      read:                            6074502
      write:                           1735572
      other:                           867786
      total:                           8677860
      transactions:                        433893 (3614.31 per sec.)
      read/write requests:                 7810074 (65057.57 per sec.)
      other operations:                    867786 (7228.62 per sec.)
      ignored errors:                      0      (0.00 per sec.)
      reconnects:                          0      (0.00 per sec.)
  General statistics:
    total time:                          120.0487s
    total number of events:              433893
    total time taken by event execution: 24005.2973s
    response time:
         min:                                 25.05ms
         avg:                                 55.33ms
         max:                                170.90ms
         approx.  95 percentile:              69.14ms
  */
  if (ACTION_TYPE != ActionType::PREPARE) {
    time_t now = time(NULL);
    int64_t curr_get_count = 0;
    int64_t curr_fail_count = 0;
    int64_t curr_put_count = 0;
    int64_t total_time = 0;
    for (int64_t i = 0; i < thread_num; ++i) {
      curr_get_count += COUNTERS[i].GET_COUNT;
      curr_fail_count += COUNTERS[i].FAIL_COUNT;
      curr_put_count += COUNTERS[i].PUT_COUNT;
      total_time += COUNTERS[i].ELAPSED_US;
    }

    if (0 == DURATION_SEC) {
      DURATION_SEC = 1;
      fprintf(stderr, "error: DURATION_SEC is 0, reset to 1\n");
    }
    double readwrite_per_sec = static_cast<double>(curr_get_count+curr_put_count) / static_cast<double>(DURATION_SEC);
    double errors_per_sec = static_cast<double>(curr_fail_count) / static_cast<double>(DURATION_SEC);
    double rt = static_cast<double>(total_time / (curr_get_count+curr_put_count)) / 1000.0;
    fprintf(stdout, "%s kvtable-bench test finished\n", ctime(&now));
    fprintf(stdout, "    queries performed:\n");
    fprintf(stdout, "    read:\t\t%ld\n", curr_get_count);
    fprintf(stdout, "    write:\t\t%ld\n", curr_put_count);
    fprintf(stdout, "    other:\t\t0\n");
    fprintf(stdout, "    total:\t\t%ld\n", curr_get_count+curr_put_count);
    fprintf(stdout, "    transactions:\t\t%ld\t(%.2f per sec)\n", curr_get_count+curr_put_count, readwrite_per_sec);
    fprintf(stdout, "    read/write requests:\t\t%ld\t(%.2f per sec)\n", curr_get_count+curr_put_count, readwrite_per_sec);
    fprintf(stdout, "    other operations:\t\t0\t(0.00 per sec)\n");
    fprintf(stdout, "    ignored errors:\t\t%ld\t(%.2f per sec)\n", curr_fail_count, errors_per_sec);
    fprintf(stdout, "    reconnects:\t\t0\t(0.00 per sec)\n");

    fprintf(stdout, "\nGeneral statistics:\n");
    fprintf(stdout, "    total time: %.2f\n", static_cast<double>(DURATION_SEC));
    fprintf(stdout, "    total time taken by event execution: %.2fs\n", static_cast<double>(total_time)/1000000.0);
    fprintf(stdout, "    response time:\n");
    fprintf(stdout, "        avg:\t\t%.2fms\n", rt);
    fprintf(stdout, "\n");
  }

  delete [] COUNTERS;
  COUNTERS = NULL;
  ////////////////
  // 11. destroy the client
  service_client.destroy();
  ObTableServiceClient::free_client(p_service_client);
  // 12. destroy the library
  ObTableServiceLibrary::destroy();
  return 0;
}

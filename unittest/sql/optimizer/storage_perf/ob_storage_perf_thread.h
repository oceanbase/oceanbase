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

#ifndef OB_TOOLS_STORAGE_PERF_THREAD_H_
#define OB_TOOLS_STORAGE_PERF_THREAD_H_

#include "ob_storage_perf_config.h"
#include "ob_storage_perf_write.h"
#include "ob_storage_perf_read.h"
#include <pthread.h>

namespace oceanbase
{
namespace storageperf
{

using namespace oceanbase::blocksstable;
using namespace oceanbase::share::schema;

enum {MAX_THREAD_COUNT = 128};

class MultiThreadInit
{
public:
  MultiThreadInit();
  int init(MockSchemaService *schema_service,
           ObRestoreSchema *restore_schema,
           const ObStoragePerfConfig& config);
  void set_config(ObStoragePerfConfig &config) {
    config_ = config;
  }
  int get_first_error() {
    int ret = OB_SUCCESS;
    for (int64_t i = 0; OB_SUCC(ret) && i < MAX_THREAD_COUNT; ++i) {
      if (OB_FAIL(ret_[i])) {
      }
    }
    return ret;
  }
protected:
  ObStoragePerfConfig config_;
  ObStorageCacheSuite cache_suite_;
  bool is_inited_;
  ObRestoreSchema *restore_schema_;
  MockSchemaService *schema_service_;
  ObStoragePerfData data_;
  storage::ObPartitionStorage *storage_;
  int ret_[MAX_THREAD_COUNT];
};

class MultiThreadRead : public MultiThreadInit
{
public:
  MultiThreadRead(int thread_no);
  int init(MockSchemaService *schema_service,
           ObRestoreSchema *restore_schema,
           const ObStoragePerfConfig& config);

  int add_read_col(int64_t col) {
    return read_cols_.push_back(col);
  }

  int assign_read_cols(const ObIArray<int64_t> &cols) {
    return read_cols_.assign(cols);
  }

  void set_update_schema(const char *uschema) {
    data_.set_update_schema(uschema);
  }

protected:
  ObArray<int64_t> read_cols_;
  int read_thread_no_;
  pthread_barrier_t barrier_;
};

//MultiThread write
class MultiThreadWrite: public share::ObThreadPool, public MultiThreadInit
{
public:
  MultiThreadWrite(int thread_no);
  virtual ~MultiThreadWrite(){}
  void run1();
  inline int64_t get_total_macro_num() { return total_used_macro_num_; }
private:
  int64_t total_used_macro_num_;
  obsys::ThreadMutex mutex_;
};

//multi thread single get read
class MultiThreadSingleGet: public share::ObThreadPool, public MultiThreadRead
{
public:
  MultiThreadSingleGet(int thread_no);
  virtual ~MultiThreadSingleGet(){}
  void run1();
};

//multi thread multi get read
class MultiThreadMultiGet: public share::ObThreadPool, public MultiThreadRead
{
public:
  MultiThreadMultiGet(int thread_no);
  virtual ~MultiThreadMultiGet(){}
  void run1();
};


//multi thread scan read
class MultiThreadScan: public share::ObThreadPool, public MultiThreadRead
{
public:
  MultiThreadScan(int thread_no);
  virtual ~MultiThreadScan(){}
  void run1();
};

}//oceanbase
}//storageperf

#endif//OB_TOOLS_STORAGE_PERF_THREAD_H_

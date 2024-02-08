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

#include "ob_storage_perf_thread.h"

namespace oceanbase
{
namespace storageperf
{
//MultiThreadInit
MultiThreadInit::MultiThreadInit()
  : is_inited_(false)
{
  for (int64_t i = 0; i < MAX_THREAD_COUNT; ++i) {
    ret_[i] = OB_SUCCESS;
  }
}

int MultiThreadInit::init(MockSchemaService *schema_service, ObRestoreSchema *restore_schema, const ObStoragePerfConfig& config)
{
  int ret = OB_SUCCESS;
  char name[OB_MAX_FILE_NAME_LENGTH];
  memset(name, 0, sizeof(name));

  if(is_inited_){
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "inited twice");
  } else if (NULL == schema_service) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "schema_service is null", K(ret));
  } else {

    config_ = config;

    if(OB_SUCCESS != (ret = cache_suite_.init(10, 1, 1, 1, 100))){
      STORAGE_LOG(WARN, "fail to init cache suite", K(ret));
    } else {
      schema_service_ = schema_service;
      restore_schema_ = restore_schema;
      is_inited_ = true;
    }
  }
  return ret;
}

MultiThreadRead::MultiThreadRead(int thread_no)
  : read_thread_no_(thread_no)
{
}

int MultiThreadRead::init(MockSchemaService *schema_service, ObRestoreSchema *restore_schema, const ObStoragePerfConfig& config)
{
  int ret = OB_SUCCESS;
  MultiThreadInit::init(schema_service, restore_schema, config);
  if (OB_FAIL(data_.init(&config_, schema_service_, &cache_suite_, restore_schema_))) {
    STORAGE_LOG(WARN, "init data failed, ", K(ret));
  } else if (0 != pthread_barrier_init(&barrier_, NULL, read_thread_no_)) {
    STORAGE_LOG(WARN, "init barrier failed");
  }
  return ret;
}


//MultiThreadWrite
MultiThreadWrite::MultiThreadWrite(int thread_no)
  : share::ObThreadPool(thread_no)
  , total_used_macro_num_(0)
{

}

void MultiThreadWrite::run(obsys::CThread *thread, void *arg)
{

  int ret = OB_SUCCESS;
  int64_t used_macro_num = 0;
  ObStoragePerfWrite write;

  if(OB_FAIL(write.init(&config_, (long)arg, restore_schema_, schema_service_))){
    STORAGE_LOG(WARN, "fail to init", K(ret));
  } else if(OB_FAIL(write.create_sstable(cache_suite_, used_macro_num))){
    STORAGE_LOG(WARN, "fail to create sstable", K(ret));
  } else if(OB_FAIL(write.close_sstable())){
    STORAGE_LOG(WARN, "fail to close sstable", K(ret));
  }

  {
    obsys::ThreadGuard guard(&mutex_);
    total_used_macro_num_ += used_macro_num;
  }

  ret_[(long)(arg)] = ret;
}
//MultiThreadSingleGet
MultiThreadSingleGet::MultiThreadSingleGet(int thread_no)
  : share::ObThreadPool(thread_no),
    MultiThreadRead(thread_no)
{

}

void MultiThreadSingleGet::run(obsys::CThread *thread, void *arg)
{

  int ret = OB_SUCCESS;
  ObStoragePerfRead read;
  read.assign_read_cols(read_cols_);
 // const int partition_num = config_.get_total_partition_num();
  if(OB_FAIL(read.init(&config_, (int64_t)arg, &cache_suite_, restore_schema_,
          schema_service_, data_.get_partition_storage(), &barrier_))){
    STORAGE_LOG(WARN, "fail to init read", K(ret));
  } else if(OB_FAIL(read.single_get_speed())){
    STORAGE_LOG(WARN, "fail to test single row speed", K(ret));
  }
  ret_[(long)(arg)] = ret;
}
//MultiThreadMultiGet
MultiThreadMultiGet::MultiThreadMultiGet(int thread_no)
  : share::ObThreadPool(thread_no),
    MultiThreadRead(thread_no)
{

}

void MultiThreadMultiGet::run(obsys::CThread *thread, void *arg)
{

  int ret = OB_SUCCESS;
  ObStoragePerfRead read;
 // const int partition_num = config_.get_total_partition_num();
  read.assign_read_cols(this->read_cols_);
  if(OB_FAIL(read.init(&config_, (int64_t)arg, &cache_suite_, restore_schema_,
          schema_service_, data_.get_partition_storage(), &barrier_))){
    STORAGE_LOG(WARN, "fail to init read", K(ret));
  } else if(OB_FAIL(read.multi_get_speed())){
    STORAGE_LOG(WARN, "fail to get sstable", K(ret));
  }
  ret_[(long)(arg)] = ret;
}
//MultiThreadScan
MultiThreadScan::MultiThreadScan(int thread_no)
  : share::ObThreadPool(thread_no),
    MultiThreadRead(thread_no)
{

}

void MultiThreadScan::run(obsys::CThread *thread, void *arg)
{

  int ret = OB_SUCCESS;
  //const int partition_num = config_.get_total_partition_num();

  ObStoragePerfRead read;
  read.assign_read_cols(this->read_cols_);
  if(OB_FAIL(read.init(&config_, (int64_t)arg, &cache_suite_, restore_schema_,
          schema_service_, data_.get_partition_storage(), &barrier_))){
    STORAGE_LOG(WARN, "fail to init read", K(ret));
  } else if(OB_FAIL(read.scan_speed())){
    STORAGE_LOG(WARN, "fail to get sstable", K(ret));
  }
  ret_[(long)(arg)] = ret;
}


}//oceanbase
}//storageperf

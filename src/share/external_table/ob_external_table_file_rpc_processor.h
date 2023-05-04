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
#ifndef OB_EXTERNAL_TABLE_FILE_RPC_PROCESSOR_H_
#define OB_EXTERNAL_TABLE_FILE_RPC_PROCESSOR_H_
#include "rpc/obrpc/ob_rpc_processor.h"
#include "share/external_table/ob_external_table_file_rpc_proxy.h"
#include "share/external_table/ob_external_table_file_task.h"
#include "deps/oblib/src/lib/lock/ob_thread_cond.h"
#include "deps/oblib/src/lib/atomic/ob_atomic.h"
#include "deps/oblib/src/lib/list/ob_obj_store.h"
namespace observer
{
struct ObGlobalContext;
}
namespace oceanbase
{
namespace share
{

template<class T>
class ObAsyncRpcTaskWaitContext
{
public:
  ObAsyncRpcTaskWaitContext()
      : cond_(), finished_cnt_(0), task_cnt_(0), async_cb_list_() {
  }
  ~ObAsyncRpcTaskWaitContext() = default;
  int init() { return cond_.init(ObWaitEventIds::ASYNC_EXTERNAL_TABLE_LOCK_WAIT); }
  void inc_concurrency_limit_with_signal()
  {
    common::ObThreadCondGuard guard(cond_);
    if (__sync_add_and_fetch(&finished_cnt_, 1) >= task_cnt_) {
      cond_.signal();
    }
  }

  int32_t get_current_concurrency() const
  {
    return ATOMIC_LOAD(&finished_cnt_);
  };

  void inc_concurrency_limit()
  {
    ATOMIC_INC(&finished_cnt_);
  }

  int dec_concurrency_limit()
  {
    int ret = OB_SUCCESS;
    int32_t cur = get_current_concurrency();
    int32_t next = cur - 1;
    if (OB_UNLIKELY(0 == cur)) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      while (ATOMIC_CAS(&finished_cnt_, cur, next) != cur) {
        cur = get_current_concurrency();
        next = cur - 1;
        if (OB_UNLIKELY(0 == cur)) {
          ret = OB_SIZE_OVERFLOW;
          break;
        }
      }
    }
    return ret;
  }

  void set_task_count(int32_t task_count) {
    task_cnt_ = task_count;
  }
  typedef common::ObSEArray<T *, 4> AsyncCbList;

  AsyncCbList &get_cb_list() { return async_cb_list_; }

  int wait_executing_tasks() {
    int ret = OB_SUCCESS;
    common::ObThreadCondGuard guard(cond_);
    while (OB_SUCC(ret) && get_current_concurrency() < task_cnt_) {
      ret = cond_.wait();
    }
    return ret;
  }

  TO_STRING_KV(K_(finished_cnt), K_(task_cnt));
private:
  common::ObThreadCond cond_;
  int32_t finished_cnt_;
  int32_t task_cnt_;

  AsyncCbList async_cb_list_;
};

class ObRpcAsyncFlushExternalTableKVCacheCallBack
      : public obrpc::ObExtenralTableRpcProxy::AsyncCB<obrpc::OB_FLUSH_EXTERNAL_TABLE_FILE_CACHE>
{
public:
  ObRpcAsyncFlushExternalTableKVCacheCallBack(
      ObAsyncRpcTaskWaitContext<ObRpcAsyncFlushExternalTableKVCacheCallBack> *context)
      : context_(context)
  {
  }
  ~ObRpcAsyncFlushExternalTableKVCacheCallBack() = default;
  void on_timeout() override;
  void on_invalid() override;
  void set_args(const Request &arg) { UNUSED(arg); }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const;
  virtual int process();
  const ObFlushExternalTableFileCacheRes &get_task_resp() const { return result_; }
  ObAsyncRpcTaskWaitContext<ObRpcAsyncFlushExternalTableKVCacheCallBack> *get_async_cb_context()
  { return context_; }

  TO_STRING_KV(K_(context));
private:
  ObAsyncRpcTaskWaitContext<ObRpcAsyncFlushExternalTableKVCacheCallBack> *context_;
};

class ObFlushExternalTableKVCacheP : public
    obrpc::ObRpcProcessor<obrpc::ObExtenralTableRpcProxy::ObRpc<obrpc::OB_FLUSH_EXTERNAL_TABLE_FILE_CACHE> >
{
public:
  ObFlushExternalTableKVCacheP() {}
  ~ObFlushExternalTableKVCacheP() {}
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObFlushExternalTableKVCacheP);
};

class ObAsyncLoadExternalTableFileListP : public
    obrpc::ObRpcProcessor<obrpc::ObExtenralTableRpcProxy::ObRpc<obrpc::OB_LOAD_EXTERNAL_FILE_LIST> >
{
public:
  ObAsyncLoadExternalTableFileListP() {}
  ~ObAsyncLoadExternalTableFileListP() {}
  int process();
private:
  DISALLOW_COPY_AND_ASSIGN(ObAsyncLoadExternalTableFileListP);
};

class ObRpcAsyncLoadExternalTableFileCallBack
      : public obrpc::ObExtenralTableRpcProxy::AsyncCB<obrpc::OB_LOAD_EXTERNAL_FILE_LIST>
{
public:
  ObRpcAsyncLoadExternalTableFileCallBack(
      ObAsyncRpcTaskWaitContext<ObRpcAsyncLoadExternalTableFileCallBack> *context)
      : context_(context)
  {
  }
  ~ObRpcAsyncLoadExternalTableFileCallBack() = default;
  void on_timeout() override;
  void on_invalid() override;
  void set_args(const Request &arg) { UNUSED(arg); }
  oceanbase::rpc::frame::ObReqTransport::AsyncCB *clone(
      const oceanbase::rpc::frame::SPAlloc &alloc) const;
  virtual int process();
  const ObLoadExternalFileListRes &get_task_resp() const { return result_; }
  ObAsyncRpcTaskWaitContext<ObRpcAsyncLoadExternalTableFileCallBack> *get_async_cb_context() { return context_; }

  TO_STRING_KV(K_(context));
private:
  ObAsyncRpcTaskWaitContext<ObRpcAsyncLoadExternalTableFileCallBack> *context_;
};
}  // namespace share
}  // namespace oceanbase
#endif /* OB_EXTERNAL_TABLE_FILE_RPC_PROCESSOR_H_ */

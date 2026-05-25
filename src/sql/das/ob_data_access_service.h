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

#ifndef OBDEV_SRC_SQL_DAS_OB_DATA_ACCESS_SERVICE_H_
#define OBDEV_SRC_SQL_DAS_OB_DATA_ACCESS_SERVICE_H_
#include "share/ob_define.h"
#include "sql/das/ob_das_rpc_proxy.h"
#include "sql/das/ob_das_id_cache.h"
#include "sql/das/ob_das_task_result.h"
#include "sql/das/ob_das_ref.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/allocator/ob_slice_alloc.h"
namespace oceanbase
{
namespace sql
{

template <class T>
class ObSliceHashAllocer
{
public:
  ObSliceHashAllocer() : slice_alloc_(NULL) {}
  ~ObSliceHashAllocer()
  {
    if (NULL != slice_alloc_) {
      slice_alloc_->purge_extra_cached_block(0);
      slice_alloc_->~ObSliceAlloc();
      slice_alloc_ = NULL;
    }
  }
  void set_attr(const ObMemAttr &attr)
  {
    if (NULL == slice_alloc_) {
      const int blk_size = choose_blk_size(static_cast<int>(sizeof(T)));
      slice_alloc_ = new (buf_) common::ObSliceAlloc(
          static_cast<int>(sizeof(T)), attr, blk_size);
      slice_alloc_->set_nway(NWAY);
    }
  }
  template <class ... TYPES>
  T *alloc(TYPES&... args)
  {
    T *ret = NULL;
    if (OB_NOT_NULL(slice_alloc_)) {
      void *ptr = slice_alloc_->alloc();
      if (OB_NOT_NULL(ptr)) {
        ret = new (ptr) T(args...);
      }
    }
    return ret;
  }
  void free(T *data)
  {
    if (OB_NOT_NULL(data) && OB_NOT_NULL(slice_alloc_)) {
      data->~T();
      slice_alloc_->free(data);
    }
  }
private:
  static const int NWAY = 8;
  static int choose_blk_size(int obj_size)
  {
    static const int MIN_SLICE_CNT = 32;
    static const int64_t HIGH_BLK_SIZE = (1LL << 18) - 128; // 256KB
    int blk_size = OB_MALLOC_NORMAL_BLOCK_SIZE;
    if (obj_size <= 0) {
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_NORMAL_BLOCK_SIZE / obj_size)) {
    } else if (MIN_SLICE_CNT <= (OB_MALLOC_MIDDLE_BLOCK_SIZE / obj_size)) {
      blk_size = OB_MALLOC_MIDDLE_BLOCK_SIZE;
    } else if (MIN_SLICE_CNT <= (static_cast<int>(HIGH_BLK_SIZE) / obj_size)) {
      blk_size = static_cast<int>(HIGH_BLK_SIZE);
    } else {
      blk_size = OB_MALLOC_BIG_BLOCK_SIZE;
    }
    return blk_size;
  }
  char buf_[sizeof(common::ObSliceAlloc)] __attribute__((aligned(64)));
  common::ObSliceAlloc *slice_alloc_;
  DISALLOW_COPY_AND_ASSIGN(ObSliceHashAllocer);
};

class ObDASRef;
class ObDASTaskArg;
class ObDASTaskResp;
class ObDASExtraData;
class ObDataAccessService
{
public:
  ObDataAccessService();
  ~ObDataAccessService();
  static int mtl_init(ObDataAccessService *&das);
  static void mtl_destroy(ObDataAccessService *&das);
  int init(rpc::frame::ObReqTransport *transport,
           const common::ObAddr &self_addr);
  //开启DAS Task分区相关的事务控制，并执行task对应的op
  int execute_das_task(ObDASRef &das_ref,
      ObDasAggregatedTask &task_ops, bool async = true);
  //关闭DAS Task的执行流程，并释放task持有的资源，并结束相关的事务控制
  int end_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op);
  int get_das_task_id(int64_t &das_id, const share::ObLSID target_ls_id);
  int rescan_das_task(ObDASRef &das_ref, ObDASScanOp &scan_op);
  obrpc::ObDASRpcProxy &get_rpc_proxy() { return das_rpc_proxy_; }
  ObDASTaskResultMgr &get_task_res_mgr() { return task_result_mgr_; }
  static ObDataAccessService &get_instance();
  const common::ObAddr &get_ctrl_addr() const { return ctrl_addr_; };
  void set_max_concurrency(int32_t cpu_count);
  int32_t get_das_concurrency_limit() const { return das_concurrency_limit_; };
  int retry_das_task(ObDASRef &das_ref, ObIDASTaskOp &task_op);
  int setup_extra_result(ObDASRef &das_ref,
                        const ObDASTaskResp &task_resp,
                        const ObIDASTaskOp *task_op,
                        ObDASExtraData *&extra_result);
  int process_task_resp(ObDASRef &das_ref, const ObDASTaskResp &task_resp, const common::ObSEArray<ObIDASTaskOp*, 2> &task_ops);
  int parallel_execute_das_task(common::ObIArray<ObIDASTaskOp *> &task_list);
  int parallel_submit_das_task(ObDASRef &das_ref, ObDasAggregatedTask &agg_task);
  int push_parallel_task(ObDASRef &das_ref, ObDasAggregatedTask &agg_task, int32_t group_id);
  int collect_das_task_info(ObIArray<ObIDASTaskOp*> &task_list, ObDASRemoteInfo &remote_info);
  bool is_lookup_batch_task(const ObDasAggregatedTask &aggregated_tasks,
                            const ObDASTaskArg &task_arg) const;
  int register_das_batch_request(const int64_t request_id,
                                 ObRpcDasAsyncAccessCallBack *async_cb);
  int fetch_and_erase_das_batch_request(const int64_t request_id,
                                        ObRpcDasAsyncAccessCallBack *&async_cb);
  int peek_das_batch_request(const int64_t request_id,
                             ObRpcDasAsyncAccessCallBack *&async_cb);
  int erase_das_batch_request(const int64_t request_id);
  int64_t gen_lookup_batch_request_id() { return ATOMIC_AAF(&next_lookup_batch_request_id_, 1); }
private:
  int execute_dist_das_task(ObDASRef &das_ref,
      ObDasAggregatedTask &task_ops, bool async = true);
  int try_post_lookup_batch_task(ObDASRef &das_ref,
                                 ObDasAggregatedTask &aggregated_tasks,
                                 ObDASTaskArg &task_arg,
                                 int32_t group_id,
                                 bool &use_batch_rpc);
  int clear_task_exec_env(ObDASRef &das_ref, ObIDASTaskOp &task_op);
  int refresh_task_location_info(ObDASRef &das_ref, ObIDASTaskOp &task_op);
  int do_local_das_task(ObIArray<ObIDASTaskOp*> &task_list);
  int do_async_remote_das_task(ObDASRef &das_ref,
                               ObDasAggregatedTask &aggregated_tasks,
                               ObDASTaskArg &task_arg,
                               int32_t group_id);
  int do_sync_remote_das_task(ObDASRef &das_ref, ObDasAggregatedTask &aggregated_tasks, ObDASTaskArg &task_arg);
  void calc_das_task_parallelism(const ObDASRef &das_ref, const ObDasAggregatedTask &task_ops, int &target_parallelism);
  int collect_das_task_attach_info(ObDASRemoteInfo &remote_info,
                                   ObDASBaseRtDef *attach_rtdef);
private:
  obrpc::ObDASRpcProxy das_rpc_proxy_;
  common::ObAddr ctrl_addr_;
  ObDASIDCache id_cache_;
  ObDASTaskResultMgr task_result_mgr_;
  typedef common::hash::HashMapTypes<int64_t, ObRpcDasAsyncAccessCallBack *>::AllocType DasBatchNodeType;
  typedef common::hash::ObHashMap<
      int64_t,
      ObRpcDasAsyncAccessCallBack *,
      common::hash::SpinReadWriteDefendMode,
      common::hash::hash_func<int64_t>,
      common::hash::equal_to<int64_t>,
      ObSliceHashAllocer<DasBatchNodeType>
  > DasBatchReqMap;
  DasBatchReqMap *das_batch_req_maps_;
  int64_t das_batch_req_map_shard_count_;
  int64_t next_lookup_batch_request_id_;
  int32_t das_concurrency_limit_;
};
}  // namespace sql
}  // namespace oceanbase

#endif /* OBDEV_SRC_SQL_DAS_OB_DATA_ACCESS_SERVICE_H_ */

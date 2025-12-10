/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_TABLE_MODELS_OB_I_MODEL_H_
#define OCEANBASE_OBSERVER_TABLE_MODELS_OB_I_MODEL_H_

#include "share/table/ob_table_rpc_struct.h"
#include "share/table/ob_table.h"
#include "observer/table/common/ob_table_common_struct.h"
#include "observer/table/common/ob_table_query_session.h"
#include "observer/table/part_calc/ob_table_part_calc.h"

namespace oceanbase
{
namespace table
{
class ObTablePartCalculator;
class ObIModel
{
public:
  using TabletOp = std::pair<uint64_t, ObSEArray<const ObTableSingleOp*, 32>>;
  using TabletOps = ObSEArray<const TabletOp*, 32>;
  using TabletIdOpsMap = std::unordered_map<uint64_t, TabletOp>;
  using LsIdTabletOpsMap = std::unordered_map<int64_t, TabletOps>;
public:
  ObIModel()
    : allocator_("ObIModel", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      query_session_(nullptr),
      lease_timeout_period_(0)
  {
    new_reqs_.set_attr(ObMemAttr(MTL_ID(), "ModNewReqs"));
    new_results_.set_attr(ObMemAttr(MTL_ID(), "ModNewRes"));
    is_alloc_from_pool_ = true;
    is_alloc_req_res_ = true;
  }
  virtual ~ObIModel() {}
  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableLoginRequest &req,
                      ObTableLoginResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }

  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableOperationRequest &req,
                      ObTableOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableBatchOperationRequest &req,
                      ObTableBatchOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableQueryRequest &req,
                      ObTableQueryResult &res);
  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableQueryAndMutateRequest &req,
                      ObTableQueryAndMutateResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int prepare(ObTableExecCtx &ctx,
                      const ObTableLSOpRequest &req,
                      ObTableLSOpResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }

  // For async query, cannot use ctx from processor directly
  virtual int prepare(ObTableExecCtx &arg_ctx,
                      const ObTableQueryAsyncRequest &req,
                      ObTableQueryAsyncResult &res,
                      ObTableExecCtx *&ctx);
  virtual int prepare(ObTableExecCtx &ctx,
                      const ObRedisRpcRequest &req,
                      ObRedisResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }

  virtual int work(ObTableExecCtx &ctx,
                   const ObTableLoginRequest &req,
                   ObTableLoginResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableLSOpRequest &req,
                   ObTableLSOpResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const common::ObIArray<ObTableLSOpRequest*> &reqs,
                   common::ObIArray<ObTableLSOpResult*> &results)
  {
    UNUSEDx(ctx, reqs, results);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableOperationRequest &req,
                   ObTableOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableBatchOperationRequest &req,
                   ObTableBatchOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableQueryRequest &req,
                   ObTableQueryResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableQueryAndMutateRequest &req,
                   ObTableQueryAndMutateResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int work(ObTableExecCtx &ctx,
                   const ObTableQueryAsyncRequest &req,
                   ObTableQueryAsyncResult &res,
                   ObTableExecCtx &arg_ctx);
  virtual int work(ObTableExecCtx &ctx,
                   const ObRedisRpcRequest &req,
                   ObRedisResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableLoginRequest &req,
                         ObTableLoginResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableLSOpRequest &req,
                         ObTableLSOpResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableOperationRequest &req,
                         ObTableOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableBatchOperationRequest &req,
                         ObTableBatchOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableQueryRequest &req,
                         ObTableQueryResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableQueryAndMutateRequest &req,
                         ObTableQueryAndMutateResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObTableQueryAsyncRequest &req,
                         ObTableQueryAsyncResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int after_work(ObTableExecCtx &ctx,
                         const ObRedisRpcRequest &req,
                         ObRedisResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableLoginRequest &req,
                              ObTableLoginResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableLSOpRequest &req,
                              ObTableLSOpResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableOperationRequest &req,
                              ObTableOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableBatchOperationRequest &req,
                              ObTableBatchOperationResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableQueryRequest &req,
                              ObTableQueryResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableQueryAndMutateRequest &req,
                              ObTableQueryAndMutateResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObTableQueryAsyncRequest &req,
                              ObTableQueryAsyncResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
  virtual int before_response(ObTableExecCtx &ctx,
                              const ObRedisRpcRequest &req,
                              ObRedisResult &res)
  {
    UNUSEDx(ctx, req, res);
    return OB_SUCCESS;
  }
public:
  OB_INLINE const common::ObIArray<ObTableLSOpRequest*> &get_new_requests() const { return new_reqs_; }
  OB_INLINE common::ObIArray<ObTableLSOpResult*> &get_new_results() { return new_results_; }
  OB_INLINE bool is_alloc_from_pool() { return is_alloc_from_pool_; }
  OB_INLINE uint64_t get_lease_timeout_period() { return lease_timeout_period_; }
  OB_INLINE table::ObTableNewQueryAsyncSession *get_query_session() { return query_session_; }
protected:
  int alloc_and_init_request_result(ObTableExecCtx &ctx,
                                    const ObTableLSOpRequest &src_req,
                                    ObTableLSOpResult &res);
  int alloc_and_init_request_result_for_mix_batch(ObTableExecCtx &ctx,
                                                  const ObTableLSOpRequest &src_req,
                                                  ObTableLSOpResult &res);
  int init_request_result_for_mix_batch(ObTableExecCtx &ctx,
                                        const ObTableLSOpRequest &src_req,
                                         ObTableLSOpResult &src_res,
                                        ObIArray<ObTableLSOpRequest*> &new_reqs,
                                        ObIArray<ObTableLSOpResult*> &new_results);
  int calc_single_op_tablet_id(ObTableExecCtx &ctx,
                               ObTablePartCalculator &calculator,
                               ObTableSingleOp &single_op,
                               ObTabletID &tablet_id);
  int prepare_allocate_result(const ObTableLSOpRequest &req,
                              ObTableLSOpResult &res);
  int init_request(const ObTableLSOpRequest &src_req,
                   const LsIdTabletOpsMap &ls_map,
                   common::ObIArray<ObTableLSOpRequest*> &new_reqs);
  int init_tablet_id_ops_map(ObTableExecCtx &ctx,
                             const ObTableLSOpRequest &req,
                             TabletIdOpsMap &map);
  int init_ls_id_tablet_op_map(const TabletIdOpsMap &tablet_map,
                               LsIdTabletOpsMap &ls_map);
  int prepare_allocate_and_init_result(ObTableExecCtx &ctx,
                                       const ObTableLSOpRequest &req,
                                       ObTableLSOpResult &res);
  void free_requests_and_results(ObTableExecCtx &ctx);
  int pre_init_results(ObTableExecCtx &ctx,
                       const ObTableLSOpRequest &src_req,
                       ObTableLSOpResult &src_res,
                       common::ObIArray<ObTableLSOpResult*> &results);
  int init_result(ObTableExecCtx &ctx,
                  const ObTableLSOpRequest &src_req,
                  ObTableLSOpResult &src_res);
  int check_result(ObTableExecCtx &ctx,
                   const ObTableLSOpRequest &src_req,
                   ObTableLSOpResult &src_res);
  int alloc_requests_and_results(common::ObIAllocator &allocator,
                                 const int64_t count,
                                 common::ObIArray<ObTableLSOpRequest*> &reqs,
                                 common::ObIArray<ObTableLSOpResult*> &results);
  int alloc_requests_and_results_for_mix_batch(ObTableExecCtx &ctx,
                                               const int64_t count,
                                               common::ObIArray<ObTableLSOpRequest*> &reqs,
                                               common::ObIArray<ObTableLSOpResult*> &results);
  int check_same_ls(const common::ObIArray<common::ObTabletID> &tablet_ids,
                    bool &is_same,
                    share::ObLSID &ls_id);
  int get_ls_id(const common::ObTabletID &tablet_id, share::ObLSID &ls_id);
  ObTablePartClipType get_clip_type(ObTableExecCtx &ctx, bool hot_only);
protected:
  virtual int get_query_session(uint64_t sessid, const ObQueryOperationType query_type, ObTableNewQueryAsyncSession *&query_session) { return OB_NOT_IMPLEMENT; }
protected:
  common::ObSEArray<ObTableLSOpRequest*, 8> new_reqs_;
  common::ObSEArray<ObTableLSOpResult*, 8> new_results_;
  bool is_alloc_from_pool_;
  bool is_alloc_req_res_;
private:
  common::ObArenaAllocator allocator_;
  table::ObTableNewQueryAsyncSession *query_session_;
  uint64_t lease_timeout_period_;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObIModel);
};

class ObModelGuard
{
public:
  ObModelGuard()
      : alloc_(nullptr),
        model_(nullptr)
  {}
  ~ObModelGuard()
  {
    if (OB_NOT_NULL(alloc_) && OB_NOT_NULL(model_)) {
      model_->~ObIModel();
      alloc_->free(model_);
    }
  }
public:
  OB_INLINE ObIModel *get_model() { return model_; }
  OB_INLINE void set_model(ObIModel *model) { model_ = model; }
  OB_INLINE void set_allocator(common::ObIAllocator *alloc) { alloc_ = alloc; }
private:
  common::ObIAllocator *alloc_;
  ObIModel *model_;
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObModelGuard);
};

} // end of namespace table
} // end of namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_MODELS_OB_I_MODEL_H_ */

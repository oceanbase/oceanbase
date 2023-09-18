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

#ifndef OBDEV_SRC_SQL_DAS_OB_DAS_TASK_H_
#define OBDEV_SRC_SQL_DAS_OB_DAS_TASK_H_
#include "share/ob_define.h"
#include "share/ob_encryption_struct.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_clog_encrypt_info.h"
#include "rpc/obrpc/ob_rpc_result_code.h"
#include "sql/das/ob_das_define.h"
#include "storage/access/ob_dml_param.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "lib/list/ob_obj_store.h"
#include "rpc/obrpc/ob_rpc_processor.h"
namespace oceanbase
{
namespace common
{
class ObNewRowIterator;
}  // namespace common
namespace sql
{
class ObDASTaskArg;
class ObIDASTaskResult;
class ObDASExtraData;
class ObExprFrameInfo;
class ObDASScanOp;
class ObDASTaskFactory;
class ObDasAggregatedTasks;

typedef ObDLinkNode<ObIDASTaskOp*> DasTaskNode;
typedef ObDList<DasTaskNode> DasTaskLinkedList;

struct ObDASRemoteInfo
{
  OB_UNIS_VERSION(1);
public:
  ObDASRemoteInfo()
    : exec_ctx_(nullptr),
      frame_info_(nullptr),
      trans_desc_(nullptr),
      snapshot_(),
      ctdefs_(),
      rtdefs_(),
      flags_(0),
      user_id_(0),
      session_id_(0),
      plan_id_(0)
  {
    sql_id_[0] = '\0';
  }
  OB_INLINE static ObDASRemoteInfo *&get_remote_info()
  {
    RLOCAL_INLINE(ObDASRemoteInfo*, g_remote_info);
    return g_remote_info;
  }
  TO_STRING_EMPTY();
  ObExecContext *exec_ctx_;
  const ObExprFrameInfo *frame_info_;
  transaction::ObTxDesc *trans_desc_; //trans desc，事务是全局信息，由RPC框架管理，这里不维护其内存
  transaction::ObTxReadSnapshot snapshot_; // Mvcc snapshot
  common::ObSEArray<const ObDASBaseCtDef*, 2> ctdefs_;
  common::ObSEArray<ObDASBaseRtDef*, 2> rtdefs_;
  union {
    uint64_t flags_;
    struct {
      uint64_t has_expr_                        : 1;
      // expr should be calculated under the following case:
      // 1. filter pushdown
      // 2. generated column
      uint64_t need_calc_expr_                  : 1;
      uint64_t need_calc_udf_                   : 1;
      uint64_t need_tx_                         : 1;
      uint64_t reserved_                        : 60;
    };
  };
  char sql_id_[common::OB_MAX_SQL_ID_LENGTH + 1];
  uint64_t user_id_;
  uint64_t session_id_;
  uint64_t plan_id_;
};

class ObIDASTaskOp
{
  friend class ObDataAccessService;
  template<obrpc::ObRpcPacketCode pcode>
  friend class ObDASBaseAccessP;
  friend class ObDASRef;
  friend class ObRpcDasAsyncAccessCallBack;
  friend class ObDataAccessService;
  OB_UNIS_VERSION_V(1);
public:
  ObIDASTaskOp(common::ObIAllocator &op_alloc)
    : tenant_id_(common::OB_INVALID_ID),
      task_id_(common::OB_INVALID_ID),
      op_type_(DAS_OP_INVALID),
      errcode_(OB_SUCCESS),
      task_flag_(0),
      trans_desc_(nullptr),
      snapshot_(nullptr),
      tablet_loc_(nullptr),
      op_alloc_(op_alloc),
      related_ctdefs_(op_alloc),
      related_rtdefs_(op_alloc),
      related_tablet_ids_(op_alloc),
      task_status_(ObDasTaskStatus::UNSTART),
      das_task_node_(),
      agg_tasks_(nullptr),
      cur_agg_list_(nullptr),
      op_result_(nullptr)
  {
    das_task_node_.get_data() = this;
  }
  virtual ~ObIDASTaskOp() { }

  virtual int open_op() = 0; //执行具体的DAS Task Op逻辑，由实例化的TaskOp自定义自己的执行逻辑
  virtual int release_op() = 0; //close DAS Task Op,释放对应的资源
  void set_tablet_id(const common::ObTabletID &tablet_id) { tablet_id_ = tablet_id; }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  void set_task_id(const int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() const { return task_id_; }
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
  const share::ObLSID &get_ls_id() const { return ls_id_; }
  void set_tablet_loc(const ObDASTabletLoc *tablet_loc) { tablet_loc_ = tablet_loc; }
  const ObDASTabletLoc *get_tablet_loc() const { return tablet_loc_; }
  inline int64_t get_ref_table_id() const { return tablet_loc_->loc_meta_->ref_table_id_; }
  virtual int decode_task_result(ObIDASTaskResult *task_result) = 0;
  //远程执行填充第一个RPC结果，并返回是否还有剩余的RPC结果
  virtual int fill_task_result(ObIDASTaskResult &task_result,
                               bool &has_more, int64_t &memory_limit)
  {
    UNUSED(task_result);
    UNUSED(has_more);
    UNUSED(memory_limit);
    return OB_NOT_IMPLEMENT;
  }
  virtual int fill_extra_result()
  {
    return common::OB_NOT_IMPLEMENT;
  }
  virtual int init_task_info(uint32_t row_extend_size) = 0;
  virtual int swizzling_remote_task(ObDASRemoteInfo *remote_info) = 0;
  virtual const ObDASBaseCtDef *get_ctdef() const { return nullptr; }
  virtual ObDASBaseRtDef *get_rtdef() { return nullptr; }
  virtual void reset_access_datums_ptr() { }
  DASCtDefFixedArray &get_related_ctdefs() { return related_ctdefs_; }
  DASRtDefFixedArray &get_related_rtdefs() { return related_rtdefs_; }
  ObTabletIDFixedArray &get_related_tablet_ids() { return related_tablet_ids_; }
  virtual int dump_data() const { return common::OB_SUCCESS; }
  const DasTaskNode &get_node() const { return das_task_node_; }
  DasTaskNode &get_node() { return das_task_node_; }
  int get_errcode() const { return errcode_; }
  void set_errcode(int errcode) { errcode_ = errcode; }
  VIRTUAL_TO_STRING_KV(K_(tenant_id),
                       K_(task_id),
                       K_(op_type),
                       K_(errcode),
                       K_(can_part_retry),
                       K_(task_started),
                       K_(in_part_retry),
                       K_(in_stmt_retry),
                       K_(need_switch_param),
                       KPC_(trans_desc),
                       KPC_(snapshot),
                       K_(tablet_id),
                       K_(ls_id),
                       KPC_(tablet_loc),
                       K_(related_ctdefs),
                       K_(related_rtdefs),
                       K_(task_status),
                       K_(related_tablet_ids),
                       K_(das_task_node));
public:
  void set_tenant_id(uint64_t tenant_id) { tenant_id_ = tenant_id; }
  uint64_t get_tenant_id() const { return tenant_id_; }
  void set_type(ObDASOpType op_type) { op_type_ = op_type; }
  ObDASOpType get_type() const { return op_type_; }
  void set_trans_desc(transaction::ObTxDesc *trans_desc) { trans_desc_ = trans_desc; }
  transaction::ObTxDesc *get_trans_desc() { return trans_desc_; }
  void set_snapshot(transaction::ObTxReadSnapshot *snapshot) { snapshot_ = snapshot; }
  transaction::ObTxReadSnapshot *get_snapshot() { return snapshot_; }
  bool is_local_task() const { return task_started_; }
  void set_can_part_retry(const bool flag) { can_part_retry_ = flag; }
  bool can_part_retry() const { return can_part_retry_; }
  bool is_in_retry() const { return in_part_retry_ || in_stmt_retry_; }
  void set_task_status(ObDasTaskStatus status);
  ObDasTaskStatus get_task_status() const { return task_status_; };
  const ObDasAggregatedTasks *get_agg_tasks() const { return agg_tasks_; };
  ObDasAggregatedTasks *get_agg_tasks() { return agg_tasks_; };
  void set_agg_tasks(ObDasAggregatedTasks *agg_tasks)
  {
    OB_ASSERT(agg_tasks != nullptr);
    OB_ASSERT(agg_tasks_ == nullptr);
    agg_tasks_ = agg_tasks;
  };
  // NOT THREAD SAFE. We only advance state on das controller.
  int state_advance();
  void set_cur_agg_list(DasTaskLinkedList *list) { cur_agg_list_ = list; };
  DasTaskLinkedList *get_cur_agg_list() { return cur_agg_list_; };

  ObIDASTaskResult *get_op_result() const { return op_result_; }
  void set_op_result(ObIDASTaskResult *op_result) { op_result_ = op_result; }

  bool get_inner_rescan()          { return inner_rescan_; }
  void set_inner_rescan(bool flag) { inner_rescan_ = flag; }

protected:
  int start_das_task();
  int end_das_task();
protected:
  uint64_t tenant_id_;
  int64_t task_id_;
  ObDASOpType op_type_; //DAS提供的operation type
  int errcode_; //don't need serialize it
  //事务相关信息
  union
  {
    uint32_t task_flag_;
    struct
    {
      /*the first 16 bits are static flags*/
      uint16_t can_part_retry_   : 1;
      uint16_t flag_reserved_    : 15;
      /*the last 16 bits are status masks*/
      uint16_t task_started_     : 1;
      uint16_t in_part_retry_    : 1;
      uint16_t in_stmt_retry_    : 1;
      uint16_t need_switch_param_ : 1; //need to switch param in gi table rescan, this parameter has been deprecated
      uint16_t inner_rescan_ : 1; //disable das retry for inner_rescan
      uint16_t status_reserved_  : 11;
    };
  };
  transaction::ObTxDesc *trans_desc_; //trans desc，事务是全局信息，由RPC框架管理，这里不维护其内存
  transaction::ObTxReadSnapshot *snapshot_; // Mvcc snapshot
  common::ObTabletID tablet_id_;
  share::ObLSID ls_id_;
  const ObDASTabletLoc *tablet_loc_; //does not need serialize it
  common::ObIAllocator &op_alloc_;
  //in DML DAS Task,related_ctdefs_ means related local index ctdefs
  //in Scan DAS Task, related_ctdefs_ have only one element, means the lookup ctdef
  DASCtDefFixedArray related_ctdefs_;
  DASRtDefFixedArray related_rtdefs_;
  ObTabletIDFixedArray related_tablet_ids_;
  ObDasTaskStatus task_status_;  // do not serialize
  DasTaskNode das_task_node_;  // tasks's linked list node, do not serialize
  ObDasAggregatedTasks *agg_tasks_;  // task's agg task, do not serialize
  DasTaskLinkedList *cur_agg_list_;  // task's agg_list, do not serialize
  ObIDASTaskResult *op_result_;
};
typedef common::ObObjStore<ObIDASTaskOp*, common::ObIAllocator&> DasTaskList;
typedef DasTaskList::Iterator DASTaskIter;

class ObIDASTaskResult
{
  OB_UNIS_VERSION_V(1);
public:
  ObIDASTaskResult() : task_id_(0) { }
  virtual ~ObIDASTaskResult() { }
  virtual int init(const ObIDASTaskOp &task_op, common::ObIAllocator &alloc) = 0;
  virtual int reuse() = 0;
  virtual int link_extra_result(ObDASExtraData &extra_result)
  {
    UNUSED(extra_result);
    return common::OB_NOT_IMPLEMENT;
  }
  void set_task_id(int64_t task_id) { task_id_ = task_id; }
  int64_t get_task_id() { return task_id_; }
  VIRTUAL_TO_STRING_KV(K_(task_id));
protected:
  int64_t task_id_; //DAS Task的id编号, 在DAS层每个server上的id是递增并且唯一的
};

class DASOpResultIter
{
public:
  struct WildDatumPtrInfo
  {
    WildDatumPtrInfo(ObEvalCtx &eval_ctx)
      : exprs_(nullptr),
        eval_ctx_(eval_ctx),
        max_output_rows_(0),
        lookup_iter_(nullptr)
    { }
    const ObExprPtrIArray *exprs_;
    ObEvalCtx &eval_ctx_;
    int64_t max_output_rows_;
    //global index scan and its lookup maybe share some expr,
    //so remote lookup task change its datum ptr,
    //and also lead index scan touch the wild datum ptr
    //so need to associate the result iterator of scan and lookup
    //resetting the index scan result datum ptr will also reset the lookup result datum ptr
    DASOpResultIter *lookup_iter_;
  };
public:
  DASOpResultIter()
    : task_iter_(),
      wild_datum_info_(nullptr)
  { }
  DASOpResultIter(const DASTaskIter &task_iter, WildDatumPtrInfo &wild_datum_info)
    : task_iter_(task_iter),
      wild_datum_info_(&wild_datum_info)
  {
  }
  int get_next_row();
  int get_next_rows(int64_t &count, int64_t capacity);
  int next_result();
  const ObDASTabletLoc *get_tablet_loc() const { return (*task_iter_)->get_tablet_loc(); }
  bool is_end() const { return task_iter_.is_end(); }
private:
  int reset_wild_datums_ptr();
private:
  DASTaskIter task_iter_;
  WildDatumPtrInfo *wild_datum_info_;
};

class ObDASTaskArg
{
  OB_UNIS_VERSION(1);
public:
  ObDASTaskArg();
  ~ObDASTaskArg() { }

  int add_task_op(ObIDASTaskOp *task_op);
  ObIDASTaskOp *get_task_op();
  const common::ObSEArray<ObIDASTaskOp*, 2> &get_task_ops() const { return task_ops_; };
  common::ObSEArray<ObIDASTaskOp*, 2> &get_task_ops() { return task_ops_; };
  void set_remote_info(ObDASRemoteInfo *remote_info) { remote_info_ = remote_info; }
  ObDASRemoteInfo *get_remote_info() { return remote_info_; }
  common::ObAddr &get_runner_svr() { return runner_svr_; }
  common::ObAddr &get_ctrl_svr() { return ctrl_svr_; }
  void set_ctrl_svr(const common::ObAddr &ctrl_svr) { ctrl_svr_ = ctrl_svr; }
  bool is_local_task() const { return ctrl_svr_ == runner_svr_; }
  void set_timeout_ts(int64_t ts) { timeout_ts_ = ts; }
  int64_t get_timeout_ts() const { return timeout_ts_; }
  TO_STRING_KV(K_(timeout_ts),
               K_(ctrl_svr),
               K_(runner_svr),
               K_(task_ops),
               KPC_(remote_info));
private:
  int64_t timeout_ts_;
  common::ObAddr ctrl_svr_; //DAS Task的控制端地址
  common::ObAddr runner_svr_; //DAS Task执行端地址
  common::ObSEArray<ObIDASTaskOp*, 2> task_ops_; //对应operation的参数信息,这是一个接口类，具体的定义由DML Service提供
  ObDASRemoteInfo *remote_info_;
};

class ObDASTaskResp
{
  OB_UNIS_VERSION(1);
public:
  ObDASTaskResp();
  int add_op_result(ObIDASTaskResult *op_result);
  const common::ObSEArray<ObIDASTaskResult*, 2> &get_op_results() const { return op_results_; };
  common::ObSEArray<ObIDASTaskResult*, 2> &get_op_results() { return op_results_; };
  void set_err_code(int err_code) { rcode_.rcode_ = err_code; }
  int get_err_code() const { return rcode_.rcode_; }
  const obrpc::ObRpcResultCode &get_rcode() const { return rcode_; }
  void store_err_msg(const common::ObString &msg);
  const char *get_err_msg() const { return rcode_.msg_; }
  int store_warning_msg(const common::ObWarningBuffer &wb);
  void set_has_more(bool has_more) { has_more_ = has_more; }
  bool has_more() const { return has_more_; }
  void set_ctrl_svr(const common::ObAddr &ctrl_svr) { ctrl_svr_ = ctrl_svr; }
  void set_runner_svr(const common::ObAddr &runner_svr) { runner_svr_ = runner_svr; }
  common::ObAddr get_runner_svr() const { return runner_svr_; }
  transaction::ObTxExecResult &get_trans_result() { return trans_result_; }
  const transaction::ObTxExecResult &get_trans_result() const { return trans_result_; }
  void set_das_factory(ObDASTaskFactory *das_factory) { das_factory_ = das_factory; };
  TO_STRING_KV(K_(has_more),
               K_(ctrl_svr),
               K_(runner_svr),
               K_(op_results),
               K_(rcode),
               K_(trans_result));
private:
  bool has_more_; //还有其它的回包消息，需要通过DTL channel进行接收
  common::ObAddr ctrl_svr_; //DAS Task的控制端地址
  common::ObAddr runner_svr_; //DAS Task执行端地址
  common::ObSEArray<ObIDASTaskResult*, 2> op_results_;  // 对应operation的结果信息，这是一个接口类，具体的定义由DML Service解析
  obrpc::ObRpcResultCode rcode_; //返回的错误信息
  transaction::ObTxExecResult trans_result_;
  ObDASTaskFactory *das_factory_;  // no need to serialize
};

template <typename T>
struct DASCtEncoder
{
  static int encode(char *buf, const int64_t buf_len, int64_t &pos, const T *val)
  {
    int ret = common::OB_SUCCESS;
    int64_t idx = 0;
    const ObDASBaseCtDef *ctdef = val;
    ObDASRemoteInfo *remote_info = ObDASRemoteInfo::get_remote_info();
    if (OB_ISNULL(val) || OB_ISNULL(remote_info)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "val is nullptr", K(ret), K(val), K(remote_info));
    } else if (!common::has_exist_in_array(remote_info->ctdefs_, ctdef, &idx)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "val not found in ctdefs", K(ret), K(val), KPC(val));
    } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, static_cast<int32_t>(idx)))) {
      SQL_DAS_LOG(WARN, "encode idx failed", K(ret), K(idx));
    }
    return ret;
  }

  static int decode(const char *buf, const int64_t data_len, int64_t &pos, const T *&val)
  {
    int ret = common::OB_SUCCESS;
    int32_t idx = 0;
    ObDASRemoteInfo *remote_info = ObDASRemoteInfo::get_remote_info();
    if (OB_ISNULL(remote_info)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "remote_info is nullptr", K(ret), K(remote_info));
    } else if (OB_FAIL(common::serialization::decode_i32(buf, data_len, pos, &idx))) {
      SQL_DAS_LOG(WARN, "decode idx failed", K(ret), K(idx));
    } else if (OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= remote_info->ctdefs_.count())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "idx is invalid", K(ret), K(idx), K(remote_info->ctdefs_.count()));
    } else {
      val = static_cast<const T *>(remote_info->ctdefs_.at(idx));
    }
    return ret;
  }

  static int64_t encoded_length(const T *val)
  {
    UNUSED(val);
    int32_t idx = 0;
    return common::serialization::encoded_length_i32(idx);
  }
};

template <typename T>
struct DASRtEncoder
{
  static int encode(char *buf, const int64_t buf_len, int64_t &pos, const T *val)
  {
    int ret = common::OB_SUCCESS;
    int64_t idx = 0;
    ObDASBaseRtDef *rtdef = const_cast<T*>(val);
    ObDASRemoteInfo *remote_info = ObDASRemoteInfo::get_remote_info();
    if (OB_ISNULL(val) || OB_ISNULL(remote_info)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "val is nullptr", K(ret), K(val), K(remote_info));
    } else if (!common::has_exist_in_array(remote_info->rtdefs_, rtdef, &idx)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "val not found in rtdefs", K(ret), K(val), KPC(val));
    } else if (OB_FAIL(common::serialization::encode_i32(buf, buf_len, pos, static_cast<int32_t>(idx)))) {
      SQL_DAS_LOG(WARN, "encode idx failed", K(ret), K(idx));
    }
    return ret;
  }

  static int decode(const char *buf, const int64_t data_len, int64_t &pos, T *&val)
  {
    int ret = common::OB_SUCCESS;
    int32_t idx = 0;
    ObDASRemoteInfo *remote_info = ObDASRemoteInfo::get_remote_info();
    if (OB_ISNULL(remote_info)) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "remote_info is nullptr", K(ret), K(remote_info));
    } else if (OB_FAIL(common::serialization::decode_i32(buf, data_len, pos, &idx))) {
      SQL_DAS_LOG(WARN, "decode idx failed", K(ret), K(idx));
    } else if (OB_UNLIKELY(idx < 0) || OB_UNLIKELY(idx >= remote_info->rtdefs_.count())) {
      ret = common::OB_ERR_UNEXPECTED;
      SQL_DAS_LOG(WARN, "idx is invalid", K(ret), K(idx), K(remote_info->rtdefs_.count()));
    } else {
      val = static_cast<T *>(remote_info->rtdefs_.at(idx));
    }
    return ret;
  }

  static int64_t encoded_length(const T *val)
  {
    UNUSED(val);
    int32_t idx = 0;
    return common::serialization::encoded_length_i32(idx);
  }
};

class ObDASDataFetchReq
{
  OB_UNIS_VERSION(1);
public:
  ObDASDataFetchReq() : tenant_id_(0), task_id_(0) {}
  ~ObDASDataFetchReq() {}
  int init(const uint64_t tenant_id, const int64_t task_id);
public:
  uint64_t get_tenant_id() { return tenant_id_; }
  int64_t get_task_id() { return task_id_; }
  TO_STRING_KV(K_(tenant_id), K_(task_id));
private:
  uint64_t tenant_id_;
  int64_t task_id_;
};

class ObDASDataFetchRes
{
  OB_UNIS_VERSION(1);
public:
  ObDASDataFetchRes();
  ~ObDASDataFetchRes() { datum_store_.reset(); };
  int init(const uint64_t tenant_id, const int64_t task_id);
public:
  ObChunkDatumStore &get_datum_store() { return datum_store_; }
  void set_has_more(const bool has_more) { has_more_ = has_more; }
  bool has_more() { return has_more_; }
  TO_STRING_KV(K_(tenant_id), K_(task_id), K_(has_more), K_(datum_store));
private:
  ObChunkDatumStore datum_store_;
  uint64_t tenant_id_;
  int64_t task_id_;
  bool has_more_;
};

class ObDASDataEraseReq
{
  OB_UNIS_VERSION(1);
public:
  ObDASDataEraseReq() : tenant_id_(0), task_id_(0) {}
  ~ObDASDataEraseReq() {}
  int init(const uint64_t tenant_id, const int64_t task_id);
public:
  uint64_t get_tenant_id() { return tenant_id_; }
  int64_t get_task_id() { return task_id_; }
  TO_STRING_KV(K_(tenant_id), K_(task_id));
private:
  uint64_t tenant_id_;
  int64_t task_id_;
};
}  // namespace sql
}  // namespace oceanbase
#endif /* OBDEV_SRC_SQL_DAS_OB_DAS_TASK_H_ */

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

#define USING_LOG_PREFIX SQL_ENG
#include "sql/ob_sql.h"
#include "sql/engine/ob_exec_context.h"
#include "lib/allocator/ob_mod_define.h"
#include "common/ob_smart_call.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/executor/ob_task_executor_ctx.h"
#include "sql/monitor/ob_phy_plan_monitor_info.h"
#include "lib/profile/ob_perf_event.h"
#include "share/interrupt/ob_global_interrupt_call.h"
#include "ob_operator.h"
#include "observer/ob_server.h"
#ifdef OB_BUILD_SPM
#include "sql/spm/ob_spm_controller.h"
#endif

namespace oceanbase
{
using namespace oceanbase::common;
namespace sql
{

int ObOpKitStore::init(ObIAllocator &alloc, const int64_t size)
{
  int ret = OB_SUCCESS;
  if (size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(size));
  } else if (NULL == (kits_ = static_cast<ObOperatorKit *>(
              alloc.alloc(size * sizeof(kits_[0]))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret));
  } else {
    memset(kits_, 0, size * sizeof(kits_[0]));
    size_ = size;
  }
  LOG_DEBUG("trace init kit store", K(ret), K(size));
  return ret;
}

void ObOpKitStore::destroy()
{
  if (NULL != kits_) {
    for (int64_t i = 0; i < size_; i++) {
      ObOperatorKit &kit = kits_[i];
      if (NULL != kit.op_) {
        kit.op_->destroy();
      }
      if (NULL != kit.input_) {
        kit.input_->~ObOpInput();
      }
    }
  }
}

ObExecContext::ObExecContext(ObIAllocator &allocator)
  : allocator_(allocator),
    phy_op_size_(0),
    phy_op_ctx_store_(NULL),
    phy_op_input_store_(NULL),
    phy_plan_ctx_(NULL),
    expr_op_size_(0),
    expr_op_ctx_store_(NULL),
    task_executor_ctx_(*this),
    my_session_(NULL),
    sql_proxy_(NULL),
    stmt_factory_(NULL),
    expr_factory_(NULL),
    outline_params_wrapper_(NULL),
    execution_id_(OB_INVALID_ID),
    has_non_trivial_expr_op_ctx_(false),
    sql_ctx_(NULL),
    pl_stack_ctx_(nullptr),
    need_disconnect_(true),
    pl_ctx_(NULL),
    package_guard_(NULL),
    row_id_list_(nullptr),
    row_id_list_array_(),
    total_row_count_(0),
    is_evolution_(false),
    reusable_interm_result_(false),
    is_async_end_trans_(false),
    gi_task_map_(nullptr),
    udf_ctx_mgr_(nullptr),
    output_row_(NULL),
    field_columns_(NULL),
    is_direct_local_plan_(false),
    sqc_handler_(nullptr),
    px_task_id_(-1),
    px_sqc_id_(-1),
    bloom_filter_ctx_array_(),
    frames_(NULL),
    frame_cnt_(0),
    op_kit_store_(),
    convert_allocator_(nullptr),
    pwj_map_(nullptr),
    calc_type_(CALC_NORMAL),
    fixed_id_(OB_INVALID_ID),
    check_status_times_(0),
    vt_ift_(nullptr),
    px_batch_id_(0),
    admission_version_(UINT64_MAX),
    admission_addr_map_(),
    use_temp_expr_ctx_cache_(false),
    temp_expr_ctx_map_(),
    dml_event_(ObDmlEventType::DE_INVALID),
    update_columns_(nullptr),
    expect_range_count_(0),
    das_ctx_(allocator),
    parent_ctx_(nullptr),
    nested_level_(0),
    is_ps_prepare_stage_(false),
    register_op_id_(OB_INVALID_ID),
    tmp_alloc_used_(false),
    table_direct_insert_ctx_(),
    errcode_(OB_SUCCESS),
    dblink_snapshot_map_(),
    cur_row_num_(-1)
{
}

ObExecContext::~ObExecContext()
{
  row_id_list_array_.reset();
  destroy_eval_allocator();
  reset_op_ctx();
  //对于后台线程, 需要调用析构
  if (NULL != phy_plan_ctx_ && !THIS_WORKER.has_req_flag()) {
    phy_plan_ctx_->~ObPhysicalPlanCtx();
  }
  phy_plan_ctx_ = NULL;
  // destory gi task info map
  if (OB_NOT_NULL(gi_task_map_)) {
    gi_task_map_->destroy();
    gi_task_map_ = NULL;
  }
  if (OB_NOT_NULL(udf_ctx_mgr_)) {
    udf_ctx_mgr_->~ObUdfCtxMgr();
    udf_ctx_mgr_ = NULL;
  }
  if (OB_NOT_NULL(pl_ctx_)) {
    pl_ctx_->~ObPLCtx();
    pl_ctx_ = NULL;
  }
  if (OB_NOT_NULL(package_guard_)) {
    package_guard_->~ObPLPackageGuard();
    package_guard_ = NULL;
  }
  if (OB_NOT_NULL(pwj_map_)) {
    pwj_map_->destroy();
    pwj_map_ = NULL;
  }
  if (OB_NOT_NULL(vt_ift_)) {
    vt_ift_->~ObIVirtualTableIteratorFactory();
    vt_ift_ = nullptr;
  }
  clean_resolve_ctx();
  sqc_handler_ = nullptr;
  if (OB_LIKELY(NULL != convert_allocator_)) {
    DESTROY_CONTEXT(convert_allocator_);
    convert_allocator_ = NULL;
  }
  admission_addr_map_.destroy();
  if (!temp_expr_ctx_map_.created()) {
  // do nothing
  } else {
    for (hash::ObHashMap<int64_t, int64_t>::iterator it = temp_expr_ctx_map_.begin();
        it != temp_expr_ctx_map_.end();
        ++it) {
      (reinterpret_cast<ObTempExprCtx *>(it->second))->~ObTempExprCtx();
    }
    temp_expr_ctx_map_.destroy();
  }
  update_columns_ = nullptr;
  errcode_ = OB_SUCCESS;
}

void ObExecContext::clean_resolve_ctx()
{
  if (OB_NOT_NULL(expr_factory_)) {
    expr_factory_->~ObRawExprFactory();
    expr_factory_ = nullptr;
  }
  if (OB_NOT_NULL(stmt_factory_)) {
    stmt_factory_->~ObStmtFactory();
    stmt_factory_ = nullptr;
  }
  sql_ctx_ = nullptr;
  pl_stack_ctx_ = nullptr;
}

uint64_t ObExecContext::get_ser_version() const
{
  return SER_VERSION_1;
}

void ObExecContext::reset_op_ctx()
{
  reset_expr_op();
  op_kit_store_.destroy();
}

void ObExecContext::reset_op_env()
{
  reset_op_ctx();
  op_kit_store_.reset();
  phy_op_size_ = 0;
  expr_op_size_ = 0;
  output_row_ = NULL;
  field_columns_ = NULL;
  if (OB_NOT_NULL(gi_task_map_)) {
    if (gi_task_map_->created()) {
      gi_task_map_->clear();
    }
  }
  if (OB_NOT_NULL(udf_ctx_mgr_)) {
    udf_ctx_mgr_->reset();
  }
}

int ObExecContext::get_fk_root_ctx(ObExecContext* &fk_root_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(this->get_parent_ctx())) {
    fk_root_ctx = this;
  } else if (!this->get_my_session()->is_foreign_key_cascade()) {
    fk_root_ctx = this;
  } else if (OB_FAIL(SMART_CALL(get_parent_ctx()->get_fk_root_ctx(fk_root_ctx)))) {
    LOG_WARN("failed to get fk root ctx", K(ret));
  }
  return ret;
}

bool ObExecContext::is_fk_root_ctx()
{
  bool ret = false;
  if (OB_ISNULL(this->get_parent_ctx())) {
    ret = true;
  } else if (!this->get_my_session()->is_foreign_key_cascade()) {
    ret = true;
  }
  return ret;
}

int ObExecContext::init_phy_op(const uint64_t phy_op_size)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(phy_op_size <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to initialize", K(phy_op_size));
  } else if (OB_UNLIKELY(phy_op_size_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init exec ctx twice", K_(phy_op_size));
  } else if (NULL == my_session_) {
    ret = OB_NOT_INIT;
    LOG_WARN("session info not set", K(ret));
  } else {
    phy_op_size_ = phy_op_size;
    if (OB_FAIL(op_kit_store_.init(allocator_, phy_op_size))) {
      LOG_WARN("init operator kit store failed", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(gi_task_map_)) {
      // Do nothing.
    } else if (gi_task_map_->created()) {
      // Do nothing. If this map has been created, it means this plan is trying to reopen.
    } else if (OB_FAIL(gi_task_map_->create(PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM, /* assume no more than 8 table scan in a plan */
        ObModIds::OB_SQL_PX))) {
      LOG_WARN("create gi task map failed", K(ret));
    }
  }
  return ret;
}

int ObExecContext::init_expr_op(uint64_t expr_op_size, ObIAllocator *allocator)
{
  int ret = OB_SUCCESS;
  ObIAllocator &real_alloc = allocator != NULL ? *allocator : allocator_;
  if (OB_UNLIKELY(expr_op_size_ > 0)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init exec ctx twice", K(ret), K_(expr_op_size));
  } else if (expr_op_size > 0) {
    int64_t ctx_store_size = static_cast<int64_t>(expr_op_size * sizeof(ObExprOperatorCtx *));
    if (OB_ISNULL(expr_op_ctx_store_ = static_cast<ObExprOperatorCtx **>(real_alloc.alloc(ctx_store_size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_ERROR("fail to alloc expr_op_ctx_store_ memory", K(ret), K(ctx_store_size));
    } else {
      expr_op_size_ = expr_op_size;
      MEMSET(expr_op_ctx_store_, 0, ctx_store_size);
    }
  }
  return ret;
}

void ObExecContext::reset_expr_op()
{
  if (expr_op_ctx_store_ != NULL) {
    ObExprOperatorCtx **it = expr_op_ctx_store_;
    ObExprOperatorCtx **it_end = &expr_op_ctx_store_[expr_op_size_];
    for (; it != it_end; ++it) {
      if (NULL != (*it)) {
        (*it)->~ObExprOperatorCtx();
      }
    }
    has_non_trivial_expr_op_ctx_ = false;
    expr_op_ctx_store_ = NULL;
    expr_op_size_ = 0;
  }
}

void ObExecContext::destroy_eval_allocator()
{
  eval_res_allocator_.reset();
  eval_tmp_allocator_.reset();
  tmp_alloc_used_ = false;
}

int ObExecContext::get_temp_expr_eval_ctx(const ObTempExpr &temp_expr,
                                          ObTempExprCtx *&temp_expr_ctx)
{
  int ret = OB_SUCCESS;
  if (use_temp_expr_ctx_cache_) {
    if (!temp_expr_ctx_map_.created()) {
      OZ(temp_expr_ctx_map_.create(8, ObMemAttr(OB_SERVER_TENANT_ID, "TempExprCtx")));
    }
    if (OB_SUCC(ret)) {
      int64_t ctx_ptr = 0;
      if (OB_FAIL(temp_expr_ctx_map_.get_refactored(reinterpret_cast<int64_t>(&temp_expr),
                                                    ctx_ptr))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
          OZ(build_temp_expr_ctx(temp_expr, temp_expr_ctx));
          CK(OB_NOT_NULL(temp_expr_ctx));
          OZ(temp_expr_ctx_map_.set_refactored(reinterpret_cast<int64_t>(&temp_expr),
                                               reinterpret_cast<int64_t>(temp_expr_ctx)));
        } else {
          LOG_WARN("fail to get temp expr ctx", K(temp_expr), K(ret));
        }
      } else {
        temp_expr_ctx = reinterpret_cast<ObTempExprCtx *>(ctx_ptr);
      }
    }
  } else {
    OZ(build_temp_expr_ctx(temp_expr, temp_expr_ctx));
  }

  return ret;
}

int ObExecContext::build_temp_expr_ctx(const ObTempExpr &temp_expr, ObTempExprCtx *&temp_expr_ctx)
{
  int ret = OB_SUCCESS;
  uint64_t frame_cnt = 0;
  char **frames = NULL;
  char *mem = static_cast<char*>(get_allocator().alloc(sizeof(ObTempExprCtx)));
  ObArray<char *> tmp_param_frame_ptrs;
  CK(OB_NOT_NULL(mem));
  OX(temp_expr_ctx = new(mem)ObTempExprCtx(*this));
  OZ(temp_expr.alloc_frame(get_allocator(), tmp_param_frame_ptrs, frame_cnt, frames));
  OX(temp_expr_ctx->frames_ = frames);
  OX(temp_expr_ctx->frame_cnt_ = frame_cnt);
  // init expr_op_size_ and expr_op_ctx_store_
  if (OB_SUCC(ret)) {
    if (temp_expr.need_ctx_cnt_ > 0) {
      int64_t ctx_store_size = static_cast<int64_t>(
                               temp_expr.need_ctx_cnt_ * sizeof(ObExprOperatorCtx *));
      if (OB_ISNULL(temp_expr_ctx->expr_op_ctx_store_
                    = static_cast<ObExprOperatorCtx **>(allocator_.alloc(ctx_store_size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_ERROR("fail to alloc expr_op_ctx_store_ memory", K(ret), K(ctx_store_size));
      } else {
        temp_expr_ctx->expr_op_size_ = temp_expr.need_ctx_cnt_;
        MEMSET(temp_expr_ctx->expr_op_ctx_store_, 0, ctx_store_size);
      }
    }
  }

  return ret;
}

int ObExecContext::set_phy_op_ctx_ptr(uint64_t index, void *phy_op)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(index >= phy_op_size_ || NULL == phy_op)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(phy_op_size), K(phy_op), K(index));
  } else {
    phy_op_ctx_store_[index] = phy_op;
  }
  return ret;
}

void *ObExecContext::get_phy_op_ctx_ptr(uint64_t index) const
{
  void *ret = NULL;

  if (NULL != phy_op_ctx_store_ && index < phy_op_size_) {
    ret = phy_op_ctx_store_[index];
  }
  return ret;
}

ObIAllocator &ObExecContext::get_sche_allocator()
{
  return sche_allocator_;
}

ObIAllocator &ObExecContext::get_allocator()
{
  return allocator_;
}

int ObExecContext::create_expr_op_ctx(uint64_t op_id, int64_t op_ctx_size, void *&op_ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(op_id >= expr_op_size_ || op_ctx_size <= 0 || OB_ISNULL(expr_op_ctx_store_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(op_id), K(op_ctx_size), K(expr_op_ctx_store_));
  } else if (OB_UNLIKELY(NULL != get_expr_op_ctx(op_id))) {
    ret = OB_INIT_TWICE;
    LOG_WARN("expr operator context has been created", K(op_id));
  } else if (OB_ISNULL(op_ctx = allocator_.alloc(op_ctx_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("allocate memory failed", K(ret), K(op_id), K(op_ctx_size));
  } else {
    expr_op_ctx_store_[op_id] = static_cast<ObExprOperatorCtx *>(op_ctx);
    has_non_trivial_expr_op_ctx_ = true;
  }
  return ret;
}

void *ObExecContext::get_expr_op_ctx(uint64_t op_id)
{
  return (OB_LIKELY(op_id < expr_op_size_) && !OB_ISNULL(expr_op_ctx_store_)) ? expr_op_ctx_store_[op_id] : NULL;
}

int ObExecContext::create_physical_plan_ctx()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *local_plan_ctx = NULL;
  if (OB_UNLIKELY(phy_plan_ctx_ != NULL)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("phy_plan_ctx_ is not null");
  } else if (OB_UNLIKELY(NULL == (local_plan_ctx = static_cast<ObPhysicalPlanCtx *>(
      allocator_.alloc(sizeof(ObPhysicalPlanCtx)))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_ERROR("no more memory to create physical_plan_ctx");
  } else {
    phy_plan_ctx_ = new (local_plan_ctx) ObPhysicalPlanCtx(allocator_);
    phy_plan_ctx_->set_exec_ctx(this);
  }
  return ret;
}

ObStmtFactory *ObExecContext::get_stmt_factory()
{
  if (OB_ISNULL(stmt_factory_)) {
    if (OB_ISNULL(stmt_factory_ = OB_NEWx(ObStmtFactory, (&allocator_), allocator_))) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to create log plan factory", K(stmt_factory_));
    }
  } else {
    // do nothing
  }
  return stmt_factory_;
}

ObRawExprFactory *ObExecContext::get_expr_factory()
{
  if (OB_ISNULL(expr_factory_)) {
    if (OB_ISNULL(expr_factory_ = OB_NEWx(ObRawExprFactory, (&allocator_), allocator_))) {
      LOG_WARN_RET(OB_ALLOCATE_MEMORY_FAILED, "fail to create log plan factory", K(expr_factory_));
    }
  } else {
    // do nothing
  }
  return expr_factory_;
}

int ObExecContext::check_status()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan ctx is null");
  } else if (phy_plan_ctx_->is_exec_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(ret));
  } else if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null");
  } else if (my_session_->is_terminate(ret)){
    LOG_WARN("execution was terminated", K(ret));
  } else if (IS_INTERRUPTED()) {
    ObInterruptCode &ic = GET_INTERRUPT_CODE();
    ret = ic.code_;
    LOG_WARN("px execution was interrupted", K(ic), K(ret));
  } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
    ret = OB_KILLED_BY_THROTTLING;
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = check_extra_status())) {
    LOG_WARN("check extra status failed", K(tmp_ret));
    if (OB_SUCC(ret)) {
      ret = tmp_ret;
    }
  }
  return ret;
}

int ObExecContext::fast_check_status(const int64_t n)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((check_status_times_++ & n) == n)) {
    ret = check_status();
  }
  return ret;
}

int ObExecContext::check_status_ignore_interrupt()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan_ctx_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("physical plan ctx is null", K(ret));
  } else if (phy_plan_ctx_->is_timeout()) {
    ret = OB_TIMEOUT;
    LOG_WARN("query is timeout", K(ret));
  } else if (OB_ISNULL(my_session_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("session info is null", K(ret));
  } else if (my_session_->is_terminate(ret)){
    LOG_WARN("execution was terminated", K(ret));
  } else if (lib::Worker::WS_OUT_OF_THROTTLE == THIS_WORKER.check_wait()) {
    ret = OB_KILLED_BY_THROTTLING;
  }
  int tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = check_extra_status())) {
    LOG_WARN("check extra status failed", K(tmp_ret));
  } else if (OB_SUCC(ret)) {
    ret = tmp_ret;
  }

  return ret;
}

int ObExecContext::fast_check_status_ignore_interrupt(const int64_t n)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY((check_status_times_++ & n) == n)) {
    ret = check_status_ignore_interrupt();
  }
  return ret;
}

int ObExecContext::init_pl_ctx()
{
  int ret = OB_SUCCESS;
  pl::ObPLCtx *pl_ctx = NULL;
  if (OB_ISNULL(pl_ctx =
    static_cast<pl::ObPLCtx*>(get_allocator().alloc(sizeof(pl::ObPLCtx))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocator memory", K(ret), K(sizeof(pl::ObPLCtx)));
  } else {
    new(pl_ctx)pl::ObPLCtx();
    set_pl_ctx(pl_ctx);
  }
  return ret;
}

uint64_t ObExecContext::get_min_cluster_version() const
{
  return task_executor_ctx_.get_min_cluster_version();
}

const common::ObAddr& ObExecContext::get_addr() const
{
  return MYADDR;
}

int ObExecContext::get_gi_task_map(GIPrepareTaskMap *&gi_task_map)
{
  int ret = OB_SUCCESS;
  gi_task_map = nullptr;
  if (nullptr == gi_task_map_) {
    void *buf = allocator_.alloc(sizeof(GIPrepareTaskMap));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memories", K(ret));
    } else if (FALSE_IT(gi_task_map_ = new(buf) GIPrepareTaskMap())) {
    } else if (OB_FAIL(gi_task_map_->create(PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM, /* assume no more than 8 table scan in a plan */
                                            ObModIds::OB_SQL_PX))) {
      LOG_WARN("Failed to create gi task map", K(ret));
    } else {
      gi_task_map = gi_task_map_;
    }
  } else {
    gi_task_map = gi_task_map_;
  }
  return ret;
}

int ObExecContext::get_convert_charset_allocator(ObArenaAllocator *&allocator)
{
  int ret = OB_SUCCESS;
  allocator = NULL;
  if (OB_ISNULL(convert_allocator_)) {
    if (OB_ISNULL(my_session_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("session is null", K(ret));
    } else {
      lib::ContextParam param;
      param.set_properties(lib::USE_TL_PAGE_OPTIONAL)
           .set_mem_attr(my_session_->get_effective_tenant_id(),
                         common::ObModIds::OB_SQL_EXPR_CALC,
                         common::ObCtxIds::DEFAULT_CTX_ID);
      if (OB_FAIL(CURRENT_CONTEXT->CREATE_CONTEXT(convert_allocator_, param))) {
        SQL_ENG_LOG(WARN, "create entity failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    allocator = &convert_allocator_->get_arena_allocator();
  }

  return ret;
}

void ObExecContext::try_reset_convert_charset_allocator()
{
  if (OB_NOT_NULL(convert_allocator_)) {
    convert_allocator_->reset_remain_one_page();
  }
}

int ObExecContext::get_udf_ctx_mgr(ObUdfCtxMgr *&udf_ctx_mgr)
{
  int ret = OB_SUCCESS;
  udf_ctx_mgr = nullptr;
  if (OB_ISNULL(udf_ctx_mgr_)) {
    void *buf = allocator_.alloc(sizeof(ObUdfCtxMgr));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memories", K(ret));
    } else {
      udf_ctx_mgr_ = new (buf) ObUdfCtxMgr();
      udf_ctx_mgr = udf_ctx_mgr_;
    }
  } else {
    udf_ctx_mgr = udf_ctx_mgr_;
  }
  return ret;
}

int ObExecContext::add_temp_table_interm_result_ids(uint64_t temp_table_id,
                                                    const common::ObAddr &sqc_addr,
                                                    const ObIArray<uint64_t> &ids)
{
  int ret = OB_SUCCESS;
  bool is_existed = false;
  ObIArray<ObSqlTempTableCtx>& temp_ctx = get_temp_table_ctx();
  for (int64_t i = 0; OB_SUCC(ret) && !is_existed && i < temp_ctx.count(); i++) {
    ObSqlTempTableCtx &ctx = temp_ctx.at(i);
    if (temp_table_id == ctx.temp_table_id_) {
      ObTempTableResultInfo info;
      info.addr_ = sqc_addr;
      if (OB_FAIL(info.interm_result_ids_.assign(ids))) {
        LOG_WARN("failed to assign to interm result ids.", K(ret));
      } else if (OB_FAIL(ctx.interm_result_infos_.push_back(info))) {
        LOG_WARN("failed to push back result info", K(ret));
      } else {
        is_existed = true;
      }
    }
  }
  if (OB_SUCC(ret) && !is_existed) {
    ObSqlTempTableCtx ctx;
    ctx.is_local_interm_result_ = false;
    ctx.temp_table_id_ = temp_table_id;
    ObTempTableResultInfo info;
    info.addr_ = sqc_addr;
    if (OB_FAIL(info.interm_result_ids_.assign(ids))) {
      LOG_WARN("failed to assign to interm result ids.", K(ret));
    } else if (OB_FAIL(ctx.interm_result_infos_.push_back(info))) {
      LOG_WARN("failed to push back result info", K(ret));
    } else if (OB_FAIL(temp_ctx.push_back(ctx))) {
      LOG_WARN("failed to push back temp table context", K(ret));
    }
  }
  return ret;
}

ObVirtualTableCtx ObExecContext::get_virtual_table_ctx()
{
  int ret = OB_SUCCESS;
  ObVirtualTableCtx vt_ctx;
  if (OB_ISNULL(vt_ift_)) {
    int64_t len = sizeof(observer::ObVirtualTableIteratorFactory);
    void *buf = allocator_.alloc(len);
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate ObVirtualTableIteratorFactory failed", K(ret), K(len));
    } else {
      vt_ift_ = new(buf) observer::ObVirtualTableIteratorFactory(*GCTX.vt_iter_creator_);
    }
  }
  vt_ctx.vt_iter_factory_ = vt_ift_;
  vt_ctx.session_ = my_session_;
  vt_ctx.schema_guard_ = sql_ctx_->schema_guard_;
  return vt_ctx;
}

int ObExecContext::init_physical_plan_ctx(const ObPhysicalPlan &plan)
{
  int ret = OB_SUCCESS;
  int64_t foreign_key_checks = 0;
  if (OB_ISNULL(phy_plan_ctx_) || OB_ISNULL(my_session_) || OB_ISNULL(sql_ctx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K_(phy_plan_ctx), K_(my_session), K(ret));
  } else if (OB_FAIL(my_session_->get_foreign_key_checks(foreign_key_checks))) {
    LOG_WARN("failed to get foreign_key_checks", K(ret));
  } else {
    int64_t start_time = my_session_->get_query_start_time();
    int64_t plan_timeout = 0;
    const ObPhyPlanHint &phy_plan_hint = plan.get_phy_plan_hint();
    ObConsistencyLevel consistency = INVALID_CONSISTENCY;
    my_session_->set_cur_phy_plan(const_cast<ObPhysicalPlan*>(&plan));
    part_ranges_.set_tenant_id(my_session_->get_effective_tenant_id());
    part_ranges_.set_label("PxTabletRangArr");
    if (OB_UNLIKELY(phy_plan_hint.query_timeout_ > 0)) {
      plan_timeout = phy_plan_hint.query_timeout_;
    } else {
      if (OB_FAIL(my_session_->get_query_timeout(plan_timeout))) {
        LOG_WARN("fail to get query timeout", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (!plan.is_remote_plan()) {
        if (OB_FAIL(phy_plan_ctx_->reserve_param_space(plan.get_param_count()))) {
          LOG_WARN("reserve param space failed", K(ret), K(plan.get_param_count()));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (stmt::T_SELECT == plan.get_stmt_type()) { // select才有weak
        if (sql_ctx_->is_protocol_weak_read_) {
          consistency = WEAK;
        } else if (OB_UNLIKELY(phy_plan_hint.read_consistency_ != INVALID_CONSISTENCY)) {
          consistency = phy_plan_hint.read_consistency_;
        } else {
          consistency = my_session_->get_consistency_level();
        }
      } else {
        consistency = STRONG;
      }
      phy_plan_ctx_->set_consistency_level(consistency);
      phy_plan_ctx_->set_timeout_timestamp(start_time + plan_timeout);
      reference_my_plan(&plan);
      phy_plan_ctx_->set_ignore_stmt(plan.is_ignore());
      phy_plan_ctx_->set_foreign_key_checks(0 != foreign_key_checks);
      phy_plan_ctx_->set_table_row_count_list_capacity(plan.get_access_table_num());
      THIS_WORKER.set_timeout_ts(phy_plan_ctx_->get_timeout_timestamp());
#ifdef OB_BUILD_SPM
      if (sql_ctx_ != NULL && sql_ctx_->spm_ctx_.need_spm_timeout_) {
        phy_plan_ctx_->set_spm_timeout_timestamp(
            ObSpmController::calc_spm_timeout_us(start_time + plan_timeout,
                                                 sql_ctx_->spm_ctx_.baseline_exec_time_));
      }
#endif
    }
  }
  if (OB_SUCC(ret)) {
    const auto &param_store = phy_plan_ctx_->get_param_store();
    int64_t first_array_index = plan.get_first_array_index();
    const ObSqlArrayObj *array_param = NULL;
    if (OB_LIKELY(OB_INVALID_INDEX == first_array_index)) {
      //this query has no array binding, do nothing
    } else if (OB_UNLIKELY(first_array_index >= param_store.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first array index is invalid", K(ret), K(first_array_index), K(param_store.count()));
    } else if (OB_UNLIKELY(!param_store.at(first_array_index).is_ext_sql_array())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("first array param is invalid", K(ret), K(param_store.at(first_array_index)));
    } else if (OB_ISNULL(array_param = reinterpret_cast<const ObSqlArrayObj*>(
        param_store.at(first_array_index).get_ext()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("array param is null", K(ret), K(param_store.at(first_array_index)));
    } else {
      phy_plan_ctx_->set_bind_array_count(array_param->count_);
    }
  }
  return ret;
}

int ObExecContext::set_partition_ranges(const Ob2DArray<ObPxTabletRange> &part_ranges,
                                        char *buf, int64_t size)
{
  int ret = OB_SUCCESS;
  part_ranges_.reset();
  if (OB_UNLIKELY(part_ranges.count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("part ranges is empty", K(ret), K(part_ranges.count()));
  } else {
    int64_t pos = 0;
    ObPxTabletRange tmp_range;
    for (int64_t i = 0; OB_SUCC(ret) && i < part_ranges.count(); ++i) {
      const ObPxTabletRange &cur_range = part_ranges.at(i);
      if (0 == size && OB_FAIL(tmp_range.deep_copy_from<true>(cur_range, get_allocator(), buf, size, pos))) {
        LOG_WARN("deep copy partition range failed", K(ret), K(cur_range));
      } else if (0 != size && OB_FAIL(tmp_range.deep_copy_from<false>(cur_range, get_allocator(), buf, size, pos))) {
        LOG_WARN("deep copy partition range failed", K(ret), K(cur_range));
      } else if (OB_FAIL(part_ranges_.push_back(tmp_range))) {
        LOG_WARN("push back partition range failed", K(ret), K(tmp_range));
      }
    }
  }
  return ret;
}

int ObExecContext::reset_one_row_id_list(const common::ObIArray<int64_t> *row_id_list)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(row_id_list));
  if (OB_SUCC(ret)) {
    row_id_list_array_.reset();
    total_row_count_ = 0;
    OZ(row_id_list_array_.push_back(row_id_list));
    total_row_count_ += row_id_list->count();
  }
  return ret;
}

int ObExecContext::add_row_id_list(const common::ObIArray<int64_t> *row_id_list)
{
  int ret = OB_SUCCESS;
  CK(OB_NOT_NULL(row_id_list));
  if (OB_SUCC(ret)) {
    OZ(row_id_list_array_.push_back(row_id_list));
    total_row_count_ += row_id_list->count();
  }
  return ret;
}

int ObExecContext::get_pwj_map(PWJTabletIdMap *&pwj_map)
{
  int ret = OB_SUCCESS;
  pwj_map = nullptr;
  if (nullptr == pwj_map_) {
    void *buf = allocator_.alloc(sizeof(PWJTabletIdMap));
    if (nullptr == buf) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("Failed to allocate memories", K(ret));
    } else if (FALSE_IT(pwj_map_ = new(buf) PWJTabletIdMap())) {
    } else if (OB_FAIL(pwj_map_->create(PARTITION_WISE_JOIN_TSC_HASH_BUCKET_NUM, /* assume no more than 8 table scan in a plan */
                                        ObModIds::OB_SQL_PX))) {
      LOG_WARN("Failed to create gi task map", K(ret));
    } else {
      pwj_map = pwj_map_;
    }
  } else {
    pwj_map = pwj_map_;
  }
  return ret;
}

int ObExecContext::fill_px_batch_info(ObBatchRescanParams &params,
    int64_t batch_id, sql::ObExpr::ObExprIArray &array)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(phy_plan_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("phy plan ctx is null", K(ret));
  } else if (batch_id >= params.get_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch param is unexpected", K(ret));
  } else {
    common::ObIArray<common::ObObjParam> &one_params =
        params.get_one_batch_params(batch_id);
    ObEvalCtx eval_ctx(*this);
    for (int i = 0; OB_SUCC(ret) && i < one_params.count(); ++i) {
      if (i > params.param_idxs_.count()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch param is unexpected", K(ret));
      } else {
        phy_plan_ctx_->get_param_store_for_update().at(params.get_param_idx(i)) = one_params.at(i);
        if (params.param_expr_idxs_.count() == one_params.count()) {
          sql::ObExpr *expr = NULL;
          int64_t idx = params.param_expr_idxs_.at(i);
          if (OB_FAIL(ret)) {
          } else if (OB_UNLIKELY(idx > array.count())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("expr index out of expr array range", K(ret), K(array), K(idx), K(array.count()));
          } else {
            expr = &array.at(idx - 1);
            expr->get_eval_info(eval_ctx).clear_evaluated_flag();
            ObDynamicParamSetter::clear_parent_evaluated_flag(eval_ctx, *expr);
            ObDatum &param_datum = expr->locate_datum_for_write(eval_ctx);
            if (OB_FAIL(param_datum.from_obj(one_params.at(i), expr->obj_datum_map_))) {
              LOG_WARN("fail to cast datum", K(ret));
            } else if (is_lob_storage(one_params.at(i).get_type()) &&
                       OB_FAIL(ob_adjust_lob_datum(one_params.at(i), expr->obj_meta_,
                                                   expr->obj_datum_map_, get_allocator(), param_datum))) {
              LOG_WARN("adjust lob datum failed", K(ret), K(i),
                       K(one_params.at(i).get_meta()), K(expr->obj_meta_));
            } else {
              expr->get_eval_info(eval_ctx).evaluated_ = true;
            }
          }
        }
      }
    }
    px_batch_id_ = batch_id;
  }
  return ret;
}

int ObExecContext::check_extra_status()
{
  int ret = OB_SUCCESS;
  if (!extra_status_check_.is_empty()) {
    int tmp_ret = OB_SUCCESS;
    DLIST_FOREACH_X(it, extra_status_check_, true) {
      if (OB_SUCCESS != (tmp_ret = it->check())) {
        SQL_ENG_LOG(WARN, "extra check failed", K(tmp_ret), "check_name", it->name(),
                    "query", my_session_->get_current_query_string(),
                    "key", my_session_->get_sessid(),
                    "proxy_sessid", my_session_->get_proxy_sessid());
        ret = OB_SUCC(ret) ? tmp_ret : ret;
      }
    }
  }
  return ret;
}

pl::ObPLPackageGuard* ObExecContext::get_package_guard()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(package_guard_)) {
    if (OB_ISNULL(get_my_session())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("execute context `s session info is null!", K(ret), K(get_my_session()));
    } else if (OB_ISNULL(package_guard_ =
        reinterpret_cast<pl::ObPLPackageGuard*>
          (get_allocator().alloc(sizeof(pl::ObPLPackageGuard))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory for exec context`s package guard!", K(ret));
    } else {
      package_guard_ =
        new(package_guard_)pl::ObPLPackageGuard(get_my_session()->get_effective_tenant_id());
      if (OB_ISNULL(package_guard_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to construct exec context`s package guard!", K(ret), K(package_guard_));
      } else if (OB_FAIL(package_guard_->init())) {
        LOG_WARN("failed to initialize exec context`s package guard!", K(ret));
      }
    }
  }
  return package_guard_;
}

DEFINE_SERIALIZE(ObExecContext)
{
  int ret = OB_SUCCESS;
  uint64_t ser_version = get_ser_version();

  if (!is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("exec context is invalid", K_(phy_op_size), K_(phy_op_ctx_store),
             K_(phy_op_input_store), K_(phy_plan_ctx), K_(my_session), K(ret));
  } else if (OB_FAIL(my_session_->add_changed_package_info(*const_cast<ObExecContext *>(this)))) {
    LOG_WARN("add changed package info failed", K(ret));
  } else {
    my_session_->reset_all_package_changed_info();
    phy_plan_ctx_->set_expr_op_size(expr_op_size_);
    if (ser_version == SER_VERSION_1) {
      OB_UNIS_ENCODE(my_session_->get_login_tenant_id());
    }
    OB_UNIS_ENCODE(phy_op_size_);
    OB_UNIS_ENCODE(*phy_plan_ctx_);
    OB_UNIS_ENCODE(*my_session_);

    OB_UNIS_ENCODE(task_executor_ctx_);
    OB_UNIS_ENCODE(das_ctx_);
    OB_UNIS_ENCODE(*sql_ctx_);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObExecContext)
{
  int ret = OB_ERR_UNEXPECTED;
  UNUSED(buf);
  UNUSED(data_len);
  UNUSED(pos);
  LOG_WARN("not supported", K(ret));
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObExecContext)
{
  int64_t len = 0;
  uint64_t ser_version = get_ser_version();

  if (is_valid() && OB_SUCCESS == my_session_->add_changed_package_info(*const_cast<ObExecContext *>(this))) {
    my_session_->reset_all_package_changed_info();
    phy_plan_ctx_->set_expr_op_size(expr_op_size_);
    if (ser_version == SER_VERSION_1) {
      OB_UNIS_ADD_LEN(my_session_->get_login_tenant_id());
    }
    OB_UNIS_ADD_LEN(phy_op_size_);
    OB_UNIS_ADD_LEN(*phy_plan_ctx_);
    OB_UNIS_ADD_LEN(*my_session_);
    OB_UNIS_ADD_LEN(task_executor_ctx_);
    OB_UNIS_ADD_LEN(das_ctx_);
    OB_UNIS_ADD_LEN(*sql_ctx_);
  }
  return len;
}
}  // namespace sql
}  // namespace oceanbase

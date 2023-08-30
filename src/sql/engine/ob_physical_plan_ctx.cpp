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
#include "sql/engine/ob_physical_plan_ctx.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_autoincrement_service.h"
#include "sql/ob_sql_context.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/executor/ob_executor_rpc_impl.h"
#include "sql/engine/expr/ob_expr_operator.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/px/ob_dfo.h"
#include "lib/utility/ob_print_utils.h"
namespace oceanbase
{
using namespace common;
using namespace share;
using namespace transaction;
namespace sql
{
DEF_TO_STRING(ObRemoteSqlInfo)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(K_(use_ps),
       K_(is_batched_stmt),
       K_(is_original_ps_mode),
       K_(ps_param_cnt),
       K_(remote_sql));
  J_COMMA();
  J_NAME("ps_params");
  J_COLON();
  if (OB_ISNULL(ps_params_) || ps_param_cnt_ <= 0) {
    J_NULL();
  } else {
    J_ARRAY_START();
    for (int64_t i = 0; pos < buf_len && i < ps_param_cnt_; ++i) {
      BUF_PRINTO(ps_params_->at(i));
      if (i != ps_param_cnt_ - 1) {
        J_COMMA();
      }
    }
    J_ARRAY_END();
  }
  J_OBJ_END();
  return pos;
}

ObPhysicalPlanCtx::ObPhysicalPlanCtx(common::ObIAllocator &allocator)
    : allocator_(allocator),
      tenant_id_(OB_INVALID_ID),
      tsc_snapshot_timestamp_(0),
      ts_timeout_us_(0),
      consistency_level_(INVALID_CONSISTENCY),
      param_store_( (ObWrapperAllocator(&allocator_)) ),
      datum_param_store_(ObWrapperAllocator(&allocator_)),
      original_param_cnt_(0),
      param_frame_capacity_(0),
      sql_mode_(SMO_DEFAULT),
      autoinc_params_(allocator),
      last_insert_id_session_(0),
      expr_op_size_(0),
      is_ignore_stmt_(false),
      bind_array_count_(0),
      bind_array_idx_(0),
      tenant_schema_version_(OB_INVALID_VERSION),
      orig_question_mark_cnt_(0),
      tenant_srs_version_(OB_INVALID_VERSION),
      array_param_groups_(),
      affected_rows_(0),
      is_affect_found_row_(false),
      found_rows_(0),
      phy_plan_(NULL),
      curr_col_index_(-1),
      autoinc_cache_handle_(NULL),
      last_insert_id_to_client_(0),
      last_insert_id_cur_stmt_(0),
      autoinc_id_tmp_(0),
      autoinc_col_value_(0),
      last_insert_id_with_expr_(false),
      last_insert_id_changed_(false),
      row_matched_count_(0),
      row_duplicated_count_(0),
      row_deleted_count_(0),
      warning_count_(0),
      is_error_ignored_(false),
      is_select_into_(false),
      is_result_accurate_(true),
      foreign_key_checks_(true),
      unsed_worker_count_since_222rel_(0),
      exec_ctx_(NULL),
      table_row_count_list_(allocator),
      batched_stmt_param_idxs_(allocator),
      implicit_cursor_infos_(allocator),
      cur_stmt_id_(-1),
      is_or_expand_transformed_(false),
      is_show_seed_(false),
      is_multi_dml_(false),
      field_array_(nullptr),
      is_ps_protocol_(false),
      plan_start_time_(0),
      is_ps_rewrite_sql_(false),
      spm_ts_timeout_us_(0)
{
}

ObPhysicalPlanCtx::~ObPhysicalPlanCtx()
{
}

void ObPhysicalPlanCtx::restore_param_store(const int64_t original_param_cnt)
{
  for (int64_t i = param_store_.count(); i > original_param_cnt; --i) {
    param_store_.pop_back();
  }
}

int ObPhysicalPlanCtx::reserve_param_space(int64_t param_count)
{
  int ret = OB_SUCCESS;
  ObObjParam null_obj;
  if (OB_FAIL(param_store_.reserve(param_count))) {
    LOG_WARN("reserve param store failed", K(ret));
  }
  int64_t N = param_count - param_store_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
    if (OB_SUCCESS != (ret = param_store_.push_back(null_obj))) {
      LOG_WARN("failed to add param", K(ret), K(i), K(N));
    }
  }
  return ret;
}

// 1. 生成datum_param_store_
// 2. 生成param frame
int ObPhysicalPlanCtx::init_datum_param_store()
{
  int ret = OB_SUCCESS;
  datum_param_store_.reuse();
  param_frame_ptrs_.reuse();
  param_frame_capacity_ = 0;
  if (OB_FAIL(datum_param_store_.prepare_allocate(param_store_.count()))) {
    LOG_WARN("fail to prepare allocate", K(ret), K(param_store_.count()));
  }
  // 通过param_store, 生成datum_param_store
  for (int64_t i = 0; OB_SUCC(ret) && i < param_store_.count(); i++) {
    ObDatumObjParam &datum_param = datum_param_store_.at(i);
    if (OB_FAIL(datum_param.alloc_datum_reserved_buff(param_store_.at(i).meta_, allocator_))) {
      LOG_WARN("alloc datum reserved buffer failed", K(ret));
    } else if (OB_FAIL(datum_param.from_objparam(param_store_.at(i), &allocator_))) {
      LOG_WARN("fail to convert obj param", K(ret), K(param_store_.at(i)));
    }
  }
  // 分配param frame内存, 并设置param datum
  if (OB_SUCC(ret)) {
    const int64_t old_size = 0;
    if (OB_FAIL(extend_param_frame(old_size))) {
      LOG_WARN("failed to extend param frame", K(ret));
    }
  }
  LOG_DEBUG("inited datum param store", K(datum_param_store_), K(param_store_));

  return ret;
}

int ObPhysicalPlanCtx::sync_last_value_local()
{
  int ret = OB_SUCCESS;
  ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
  ObIArray<AutoincParam> &autoinc_params = get_autoinc_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
    AutoincParam &autoinc_param = autoinc_params.at(i);
    if (OB_FAIL(auto_service.sync_insert_value_local(autoinc_param))) {
      LOG_WARN("failed to sync last insert value locally", K(ret));
    }
  }
	return ret;
}

int ObPhysicalPlanCtx::sync_last_value_global()
{
  int ret = OB_SUCCESS;
  ObAutoincrementService &auto_service = ObAutoincrementService::get_instance();
  ObIArray<AutoincParam> &autoinc_params = get_autoinc_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
    AutoincParam &autoinc_param = autoinc_params.at(i);
    if (OB_FAIL(auto_service.sync_insert_value_global(autoinc_param))) {
      LOG_WARN("failed to sync last insert value globally", K(ret));
    }
  }
  return ret;
}

void ObPhysicalPlanCtx::set_cur_time(const int64_t &session_val, const ObSQLSessionInfo &session)
{
  //如果系统变量设置了timestamp, 则该请求内部是用设置的timestamp
  //如果没有设置, 则使用当前请求执行时的时间
  int64_t ts = session.get_local_timestamp();
  if (0 != ts) {
    cur_time_.set_timestamp(ts);
  } else {
    cur_time_.set_timestamp(session_val);
  }
}

int ObPhysicalPlanCtx::set_row_matched_count(int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(row_count));
  } else {
    row_matched_count_ = row_count;
  }
  return ret;
}

int ObPhysicalPlanCtx::set_row_duplicated_count(int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(row_count));
  } else {
    row_duplicated_count_ = row_count;
  }
  return ret;
}

int ObPhysicalPlanCtx::set_row_deleted_count(int64_t row_count)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(row_count < 0)) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "invalid argument", K(row_count));
  } else {
    row_deleted_count_ = row_count;
  }
  return ret;
}

bool ObPhysicalPlanCtx::is_plain_select_stmt() const
{
  return phy_plan_ != NULL && phy_plan_->get_stmt_type() == stmt::T_SELECT && false == phy_plan_->has_for_update();
}

void ObPhysicalPlanCtx::set_phy_plan(const ObPhysicalPlan *phy_plan)
{
  phy_plan_ = phy_plan;
}

int ObPhysicalPlanCtx::assign_batch_stmt_param_idxs(const BatchParamIdxArray &param_idxs)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(batched_stmt_param_idxs_.init(param_idxs.count()))) {
    LOG_WARN("init batched stmt param idxs failed", K(ret), K(param_idxs));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < param_idxs.count(); ++i) {
    PartParamIdxArray part_param_idxs;
    part_param_idxs.part_id_ = param_idxs.at(i).part_id_;
    part_param_idxs.part_param_idxs_.set_allocator(&allocator_);
    if (OB_FAIL(part_param_idxs.part_param_idxs_.init(param_idxs.at(i).part_param_idxs_.count()))) {
      LOG_WARN("init part param idxs failed", K(ret), K(param_idxs.at(i)));
    } else if (OB_FAIL(append(part_param_idxs.part_param_idxs_, param_idxs.at(i).part_param_idxs_))) {
      LOG_WARN("append part param idxs failed", K(ret), K(param_idxs.at(i)));
    } else if (OB_FAIL(batched_stmt_param_idxs_.push_back(part_param_idxs))) {
      LOG_WARN("store batched stmt param idxs failed", K(ret), K(part_param_idxs));
    }
  }
  return ret;
}

//设置batch update stmt的partition id和param index的映射关系
//batch update stmt中的参数被构造成nested table对象存储在param store上
//partition_ids:array index是batch update stmt中参数的下标，值是param对应的partition id
int ObPhysicalPlanCtx::set_batched_stmt_partition_ids(ObIArray<int64_t> &partition_ids)
{
  int ret = OB_SUCCESS;
  if (!partition_ids.empty()) {
    ObFixedArray<int64_t, ObIAllocator> part_ids(allocator_);
    if (OB_FAIL(part_ids.init(partition_ids.count()))) {
      LOG_WARN("init partition ids failed", K(ret), K(partition_ids.count()));
    } else if (OB_FAIL(batched_stmt_param_idxs_.init((partition_ids.count())))) {
      LOG_WARN("init batched stmt param idxs failed", K(ret));
    } else if (OB_FAIL(implicit_cursor_infos_.prepare_allocate(partition_ids.count()))) {
      LOG_WARN("init implicit cursor infos failed", K(ret), K(partition_ids.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < partition_ids.count(); ++i) {
      int64_t part_id = partition_ids.at(i);
      int64_t array_idx = OB_INVALID_INDEX;
      if (has_exist_in_array(part_ids, part_id, &array_idx)) {
        if (OB_FAIL(batched_stmt_param_idxs_.at(array_idx).part_param_idxs_.push_back(i))) {
          LOG_WARN("store param index to partition ids failed",
                   K(ret), K(array_idx), K(part_ids), K(i));
        }
      } else {
        int64_t last_idx = batched_stmt_param_idxs_.count();
        PartParamIdxArray param_idxs;
        param_idxs.part_id_ = part_id;
        param_idxs.part_param_idxs_.set_allocator(&allocator_);
        if (OB_FAIL(batched_stmt_param_idxs_.push_back(param_idxs))) {
          LOG_WARN("store param idxs failed", K(ret), K(param_idxs));
        } else if (OB_FAIL(batched_stmt_param_idxs_.at(last_idx).part_param_idxs_.init(
            partition_ids.count()))) {
          LOG_WARN("init param_idxs failed", K(ret), K(last_idx), K(partition_ids));
        } else if (OB_FAIL(batched_stmt_param_idxs_.at(last_idx).part_param_idxs_.push_back(i))) {
          LOG_WARN("store param index to param idxs failed", K(ret), K(i));
        } else if (OB_FAIL(part_ids.push_back(part_id))) {
          LOG_WARN("store partition id failed", K(ret), K(part_id));
        }
      }
    }
    LOG_DEBUG("set batched stmt partition ids end", K(ret),
              K(partition_ids), K_(batched_stmt_param_idxs));
  }
  return ret;
}

void ObPhysicalPlanCtx::reset_cursor_info()
{
  affected_rows_ = 0;
  found_rows_ = 0;
  row_matched_count_ = 0;
  row_duplicated_count_ = 0;
  row_deleted_count_ = 0;
}

int ObPhysicalPlanCtx::merge_implicit_cursors(const ObIArray<ObImplicitCursorInfo> &implicit_cursor)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(implicit_cursor, idx) {
    OZ(merge_implicit_cursor_info(implicit_cursor.at(idx)));
  }
  return ret;
}

int ObPhysicalPlanCtx::merge_implicit_cursor_info(const ObImplicitCursorInfo &implicit_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(implicit_cursor.stmt_id_ < 0)
      || OB_UNLIKELY(implicit_cursor.stmt_id_ >= implicit_cursor_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("implicit cursor is invalid", K(ret),
             K(implicit_cursor), K(implicit_cursor_infos_.count()));
  } else if (OB_FAIL(implicit_cursor_infos_.at(implicit_cursor.stmt_id_).
                     merge_cursor(implicit_cursor))) {
    LOG_WARN("merge implicit cursor info failed", K(ret),
             K(implicit_cursor), K(implicit_cursor_infos_));
  }
  LOG_DEBUG("merge implicit cursor info", K(ret), K(implicit_cursor), K(lbt()));
  return ret;
}

const ObIArray<int64_t> *ObPhysicalPlanCtx::get_part_param_idxs(int64_t part_id) const
{
  const ObIArray<int64_t> *part_param_idxs = nullptr;
  for (int64_t i = 0; nullptr == part_param_idxs && i < batched_stmt_param_idxs_.count(); ++i) {
    if (batched_stmt_param_idxs_.at(i).part_id_ == part_id) {
      part_param_idxs = &(batched_stmt_param_idxs_.at(i).part_param_idxs_);
    }
  }
  return part_param_idxs;
}

int ObPhysicalPlanCtx::switch_implicit_cursor()
{
  int ret = OB_SUCCESS;
  if (cur_stmt_id_ < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur stmt id is invalid", K(ret), K_(cur_stmt_id));
  } else if (cur_stmt_id_ >= implicit_cursor_infos_.count()) {
    ret = OB_ITER_END;
    LOG_DEBUG("switch implicit cursor iter end", K(ret),
              K(cur_stmt_id_), K(implicit_cursor_infos_));
  } else if (implicit_cursor_infos_.at(cur_stmt_id_).stmt_id_ != cur_stmt_id_
      && implicit_cursor_infos_.at(cur_stmt_id_).stmt_id_ >= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid implicit cursor", K(ret), K(cur_stmt_id_), K(implicit_cursor_infos_));
  } else {
    const ObImplicitCursorInfo &cursor_info = implicit_cursor_infos_.at(cur_stmt_id_);
    set_affected_rows(cursor_info.affected_rows_);
    set_found_rows(cursor_info.found_rows_);
    set_row_matched_count(cursor_info.matched_rows_);
    set_row_duplicated_count(cursor_info.duplicated_rows_);
    set_row_deleted_count(cursor_info.deleted_rows_);
    ++cur_stmt_id_;
  }
  return ret;
}

int ObPhysicalPlanCtx::extend_datum_param_store(DatumParamStore &ext_datum_store)
{
  int ret = OB_SUCCESS;
  if (ext_datum_store.count() <= 0) {
    // do nothing
  } else {
    const int64_t old_size = datum_param_store_.count();
    for (int i = 0; OB_SUCC(ret) && i < ext_datum_store.count(); i++) {
      if (OB_FAIL(datum_param_store_.push_back(ext_datum_store.at(i)))) {
        LOG_WARN("failed to push back element", K(ret));
      }
    } // for end

    LOG_DEBUG("try to extend param frame",
              K(ext_datum_store), K(datum_param_store_), K(param_store_));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(extend_param_frame(old_size))) {
      LOG_WARN("failed to extend param frame", K(ret));
    } else {
      // transform ext datums to obj params and push back to param_store_
      for (int i = 0; OB_SUCC(ret) && i < ext_datum_store.count(); i++) {
        ObObjParam tmp_obj_param;
        if (OB_FAIL(ext_datum_store.at(i).to_objparam(tmp_obj_param, &allocator_))) {
          LOG_WARN("failed to transform expr datum to obj param", K(ret));
        } else if (OB_FAIL(param_store_.push_back(tmp_obj_param))) {
          LOG_WARN("failed to push back element", K(ret));
        }
      } // for end
    }
    LOG_DEBUG("after extended param frame", K(param_store_));
  }
  return ret;
}

void ObPhysicalPlanCtx::reset_datum_frame(char *frame, int64_t expr_cnt)
{
  const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  for (int64_t j = 0; j < expr_cnt; ++j) {
    ObDatum *datum = reinterpret_cast<ObDatum *>(frame + j * datum_eval_info_size);
    datum->set_null();
  }
}

int ObPhysicalPlanCtx::reserve_param_frame(const int64_t input_capacity)
{
  int ret = OB_SUCCESS;
  if (input_capacity > param_frame_capacity_) {
    const int64_t cnt_per_frame = ObExprFrameInfo::EXPR_CNT_PER_FRAME;
    const int64_t item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    auto calc_frame_cnt = [&](int64_t cap) { return (cap + cnt_per_frame - 1) / cnt_per_frame; };
    // reserve original param frames first
    if (param_frame_capacity_ < original_param_cnt_) {
      if (param_frame_capacity_ > 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("original_param_cnt_ changes after init_datum_param_store",
                 K(ret), K(param_frame_capacity_), K(input_capacity));
      } else {
        int64_t total = original_param_cnt_;
        while (total > 0 && OB_SUCC(ret)) {
          int64_t cnt = std::min(total, cnt_per_frame);
          char *frame = static_cast<char *>(allocator_.alloc(item_size * cnt));
          OV(NULL != frame, OB_ALLOCATE_MEMORY_FAILED);
          OX(MEMSET(frame, 0, item_size * cnt));
          OX(reset_datum_frame(frame, cnt));
          OZ(param_frame_ptrs_.push_back(frame));
          total -= cnt;
        }
        OX(param_frame_capacity_ = original_param_cnt_);
      }
    }

    // reserve frames beyond original param frames, align the last frame with pow of 2.
    int64_t beyond = input_capacity - original_param_cnt_;
    if (OB_SUCC(ret) && beyond > 0) {

      int64_t frame_cnt = calc_frame_cnt(beyond);
      int64_t last = beyond - (frame_cnt - 1) * cnt_per_frame;

      // align last frame capacity to pow of 2.
      last = std::min(cnt_per_frame, next_pow2(last));
      beyond = (frame_cnt - 1) * cnt_per_frame + last;

      const int64_t original_frame_cnt = original_param_cnt_ > 0
          ? calc_frame_cnt(original_param_cnt_)
          : 0;

      const int64_t old_capacity = param_frame_capacity_ - original_param_cnt_;
      const int64_t old_frame_cnt = calc_frame_cnt(old_capacity);
      int64_t frame_idx = old_frame_cnt;
      if (old_capacity % cnt_per_frame != 0) {
        frame_idx--;
      }

      for (; OB_SUCC(ret) && frame_idx < frame_cnt; frame_idx++) {
        const int64_t cnt = frame_idx + 1 == frame_cnt ? last : cnt_per_frame;
        char *frame = static_cast<char *>(allocator_.alloc(item_size * cnt));
        OV(NULL != frame, OB_ALLOCATE_MEMORY_FAILED);
        if (OB_SUCC(ret)) {
          const int64_t array_idx = original_frame_cnt + frame_idx;
          MEMSET(frame, 0, item_size * cnt);
          if (array_idx + 1 == param_frame_ptrs_.count()) {
            // copy last frame data
            int64_t old_last = (param_frame_capacity_ - original_param_cnt_) % cnt_per_frame;
            MEMMOVE(frame, param_frame_ptrs_.at(array_idx), item_size * old_last);
            param_frame_ptrs_.at(array_idx) = frame;
          } else {
            OX(reset_datum_frame(frame, cnt));
            OZ(param_frame_ptrs_.push_back(frame));
          }
        }
      }
      if (OB_SUCC(ret)) {
        param_frame_capacity_ = original_param_cnt_ + beyond;
      }
    }
  }

  return ret;
}

int ObPhysicalPlanCtx::extend_param_frame(const int64_t old_size)
{
  int ret = OB_SUCCESS;
  if (old_size < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(old_size));
  } else if (OB_FAIL(reserve_param_frame(datum_param_store_.count()))) {
    LOG_WARN("reserve param frame failed", K(ret));
  } else {
    for (int64_t i = old_size; i < datum_param_store_.count(); i++) {
      ObDatum *datum = nullptr;
      ObEvalInfo *eval_info = nullptr;
      get_param_frame_info(i, datum, eval_info);
      *datum = datum_param_store_.at(i).datum_;
      eval_info->evaluated_ = false;
    }
  }
  return ret;
}

OB_INLINE void ObPhysicalPlanCtx::get_param_frame_info(int64_t param_idx,
                                                       ObDatum *&datum,
                                                       ObEvalInfo *&eval_info)
{
  const int64_t cnt_per_frame = ObExprFrameInfo::EXPR_CNT_PER_FRAME;
  const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
  int64_t datum_idx = param_idx < original_param_cnt_ ? param_idx : param_idx - original_param_cnt_;
  int64_t idx = datum_idx / cnt_per_frame;
  int64_t off = (datum_idx % cnt_per_frame) * datum_eval_info_size;
  if (original_param_cnt_ > 0 && param_idx >= original_param_cnt_) {
    idx += (original_param_cnt_ + cnt_per_frame - 1) / cnt_per_frame;
  }
  datum = reinterpret_cast<ObDatum*>(param_frame_ptrs_.at(idx) + off);
  eval_info = reinterpret_cast<ObEvalInfo *>(param_frame_ptrs_.at(idx) + off + sizeof(ObDatum));
}

int ObPhysicalPlanCtx::replace_batch_param_datum(const int64_t cur_group_id,
                                                 const int64_t start_param,
                                                 const int64_t param_cnt)
{
  int ret = OB_SUCCESS;
  const int64_t datum_cnt = datum_param_store_.count();
  if (OB_UNLIKELY(start_param < 0 || start_param + param_cnt > datum_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected params", K(ret), K(start_param), K(param_cnt), K(datum_cnt));
  } else {
    for (int64_t i = start_param; OB_SUCC(ret) && i < start_param + param_cnt; i++) {
      if (datum_param_store_.at(i).meta_.is_ext_sql_array()) {
        //need to expand the real param to param frame
        ObDatum *datum = nullptr;
        ObEvalInfo *eval_info = nullptr;
        get_param_frame_info(i, datum, eval_info);
        const ObSqlDatumArray *datum_array = datum_param_store_.at(i).get_sql_datum_array();;
        if (OB_UNLIKELY(cur_group_id < 0) || OB_UNLIKELY(cur_group_id >= datum_array->count_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid group id", K(ret), K(cur_group_id), K(datum_array->count_));
        } else {
          //assign datum ptr to the real param datum
          *datum = datum_array->data_[cur_group_id];
          eval_info->evaluated_ = true;
          LOG_DEBUG("replace batch param datum", K(cur_group_id), KPC(datum), K(datum));
        }
      }
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObArrayParamGroup)
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(row_count_);
  OB_UNIS_ENCODE(column_count_);
  OB_UNIS_ENCODE(start_param_idx_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObArrayParamGroup)
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN(row_count_);
  OB_UNIS_ADD_LEN(column_count_);
  OB_UNIS_ADD_LEN(start_param_idx_);
  return len;
}

OB_DEF_DESERIALIZE(ObArrayParamGroup)
{
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE(row_count_);
  OB_UNIS_DECODE(column_count_);
  OB_UNIS_DECODE(start_param_idx_);
  return ret;
}

OB_DEF_SERIALIZE(ObPhysicalPlanCtx)
{
  int ret = OB_SUCCESS;
  ParamStore empty_param_store;
  const ParamStore *param_store = NULL;
  const ObRowIdListArray *row_id_list_array = NULL;
  int64_t cursor_count = implicit_cursor_infos_.count();
  // used for function sys_view_bigint_param(idx), @note unused anymore
  ObSEArray<common::ObObj, 1> sys_view_bigint_params_;
  char message_[1] = {'\0'}; //error msg buffer, unused anymore
  if (exec_ctx_ != NULL) {
    row_id_list_array = &exec_ctx_->get_row_id_list_array();
  }
  if ((row_id_list_array != NULL && !row_id_list_array->empty() && phy_plan_ != NULL) ||
       is_multi_dml_) {
    param_store = &empty_param_store;
  } else {
    param_store = &param_store_;
  }
  //按老的序列方式进行
  OB_UNIS_ENCODE(tenant_id_);
  OB_UNIS_ENCODE(tsc_snapshot_timestamp_);
  OB_UNIS_ENCODE(cur_time_);
  OB_UNIS_ENCODE(merging_frozen_time_);
  OB_UNIS_ENCODE(ts_timeout_us_);
  OB_UNIS_ENCODE(consistency_level_);
  OB_UNIS_ENCODE(*param_store);
  OB_UNIS_ENCODE(sys_view_bigint_params_);
  OB_UNIS_ENCODE(sql_mode_);
  OB_UNIS_ENCODE(autoinc_params_);
  OB_UNIS_ENCODE(tablet_autoinc_param_);
  OB_UNIS_ENCODE(last_insert_id_session_);
  OB_UNIS_ENCODE(message_);
  OB_UNIS_ENCODE(expr_op_size_);
  OB_UNIS_ENCODE(is_ignore_stmt_);
  if (OB_SUCC(ret) && row_id_list_array != NULL &&
      !row_id_list_array->empty() &&
      phy_plan_ != NULL && !(param_store_.empty())) {
    //按需序列化param store
    //先序列化param store的槽位
    //需要序列化param store个数
    //序列化param index
    //序列化param value
    OB_UNIS_ENCODE(param_store_.count());
    //序列化完才知道被序列化的参数值有多少，所以先跳掉count的位置
    int64_t seri_param_cnt_pos = pos;
    int32_t real_param_cnt = 0;
    pos += serialization::encoded_length_i32(real_param_cnt);
    const ObIArray<int64_t> *param_idxs = NULL;
    if (phy_plan_->get_row_param_map().count() <= 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("row param map is empty");
    } else {
      param_idxs = &(phy_plan_->get_row_param_map().at(0));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_idxs->count(); ++i) {
      ++real_param_cnt;
      OB_UNIS_ENCODE(param_idxs->at(i));
      OB_UNIS_ENCODE(param_store_.at(param_idxs->at(i)));
    }
    for (int64_t k = 0; OB_SUCC(ret) && k < row_id_list_array->count(); ++k) {
      auto row_id_list = row_id_list_array->at(k);
      CK(OB_NOT_NULL(row_id_list));
      for (int64_t i = 0; OB_SUCC(ret) && i < row_id_list->count(); ++i) {
        //row_param_map从1开始，因为index=0的位置用来存放公共param了
        int64_t row_idx = row_id_list->at(i) + 1;
        if (OB_UNLIKELY(row_idx >= phy_plan_->get_row_param_map().count()) || OB_UNLIKELY(row_idx < 0)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row index is invalid", K(phy_plan_->get_row_param_map().count()), K(row_idx));
        } else {
          param_idxs = &(phy_plan_->get_row_param_map().at(row_idx));
        }
        for (int64_t j = 0; OB_SUCC(ret) && j < param_idxs->count(); ++j) {
          ++real_param_cnt;
          OB_UNIS_ENCODE(param_idxs->at(j));
          OB_UNIS_ENCODE(param_store_.at(param_idxs->at(j)));
        }
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialization::encode_i32(buf, buf_len, seri_param_cnt_pos, real_param_cnt))) {
        LOG_WARN("encode int32_t failed", K(buf_len), K(seri_param_cnt_pos), K(real_param_cnt), K(ret));
      }
    }
  } else {
    /**
     * long long ago, param_store_.count() may be serialized or not, depend on if it is 0,
     * if > 0, param_store_.count() will be serialized,
     * if = 0, not.
     * but param_cnt will be always deserialized in deserialize operation, so it will
     * cause problem if we add other member in serialize and deserialize operation after
     * param_store_.count(), like foreign_key_checks_. the serialized data of new member
     * will be deserialized as param_cnt.
     * why everything goes right before, especially when param_store_.count() is 0 ?
     * the last member will consume all serialized data before param_cnt, so we get
     * 0 when try to deserialize param_cnt, and then finish.
     */
    int64_t param_cnt = 0;
    OB_UNIS_ENCODE(param_cnt);
  }
  OB_UNIS_ENCODE(foreign_key_checks_);
  OB_UNIS_ENCODE(unsed_worker_count_since_222rel_);
  OB_UNIS_ENCODE(tenant_schema_version_);
  OB_UNIS_ENCODE(cursor_count);
  OB_UNIS_ENCODE(plan_start_time_);
  OB_UNIS_ENCODE(last_trace_id_);
  OB_UNIS_ENCODE(tenant_srs_version_);
  OB_UNIS_ENCODE(original_param_cnt_);
  OB_UNIS_ENCODE(array_param_groups_.count());
  if (OB_SUCC(ret) && array_param_groups_.count() > 0) {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < array_param_groups_.count(); i++) {
      OB_UNIS_ENCODE(array_param_groups_.at(i));
    }
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPhysicalPlanCtx)
{
  int64_t len = 0;
  ParamStore empty_param_store;
  const ParamStore *param_store = NULL;
  const ObRowIdListArray *row_id_list_array = NULL;
  int64_t cursor_count = implicit_cursor_infos_.count();
  // used for function sys_view_bigint_param(idx), @note unused anymore
  ObSEArray<common::ObObj, 1> sys_view_bigint_params_;
  char message_[1] = {'\0'}; //error msg buffer, unused anymore
  if (exec_ctx_ != NULL) {
    row_id_list_array = &exec_ctx_->get_row_id_list_array();
  }
  if ((row_id_list_array != NULL && !row_id_list_array->empty() && phy_plan_ != NULL) ||
       is_multi_dml_) {
    param_store = &empty_param_store;
  } else {
    param_store = &param_store_;
  }
  //按老的序列方式进行
  OB_UNIS_ADD_LEN(tenant_id_);
  OB_UNIS_ADD_LEN(tsc_snapshot_timestamp_);
  OB_UNIS_ADD_LEN(cur_time_);
  OB_UNIS_ADD_LEN(merging_frozen_time_);
  OB_UNIS_ADD_LEN(ts_timeout_us_);
  OB_UNIS_ADD_LEN(consistency_level_);
  OB_UNIS_ADD_LEN(*param_store);
  OB_UNIS_ADD_LEN(sys_view_bigint_params_);
  OB_UNIS_ADD_LEN(sql_mode_);
  OB_UNIS_ADD_LEN(autoinc_params_);
  OB_UNIS_ADD_LEN(tablet_autoinc_param_);
  OB_UNIS_ADD_LEN(last_insert_id_session_);
  OB_UNIS_ADD_LEN(message_);
  OB_UNIS_ADD_LEN(expr_op_size_);
  OB_UNIS_ADD_LEN(is_ignore_stmt_);
  if (row_id_list_array != NULL && !row_id_list_array->empty() &&
      phy_plan_ != NULL && !(param_store_.empty())) {
    //按需序列化param store
    //需要序列化param store个数
    //序列化param index
    //序列化param value
    OB_UNIS_ADD_LEN(param_store_.count());
    //序列化完才知道被序列化的参数值有多少，所以先跳掉count的位置
    int32_t real_param_cnt = 0;
    len += serialization::encoded_length_i32(real_param_cnt);
    if (phy_plan_->get_row_param_map().count() > 0) {
      const ObIArray<int64_t> &param_idxs = phy_plan_->get_row_param_map().at(0);
      for (int64_t i = 0; i < param_idxs.count(); ++i) {
        OB_UNIS_ADD_LEN(param_idxs.at(i));
        OB_UNIS_ADD_LEN(param_store_.at(param_idxs.at(i)));
      }
    }
    for (int64_t k = 0; k < row_id_list_array->count(); ++k) {
      auto row_id_list = row_id_list_array->at(k);
      if (OB_NOT_NULL(row_id_list)) {
        for (int64_t i = 0; i < row_id_list->count(); ++i) {
          //row_param_map从1开始，因为index=0的位置用来存放公共param了
          int64_t row_idx = row_id_list->at(i) + 1;
          if (row_idx < phy_plan_->get_row_param_map().count() && row_idx >= 0) {
            const ObIArray<int64_t> &param_idxs = phy_plan_->get_row_param_map().at(row_idx);
            for (int64_t j = 0; j < param_idxs.count(); ++j) {
              OB_UNIS_ADD_LEN(param_idxs.at(j));
              OB_UNIS_ADD_LEN(param_store_.at(param_idxs.at(j)));
            }
          }
        }
      }
    }
  } else {
    // see comment in OB_DEF_SERIALIZE(ObPhysicalPlanCtx).
    int64_t param_cnt = 0;
    OB_UNIS_ADD_LEN(param_cnt);
  }
  OB_UNIS_ADD_LEN(foreign_key_checks_);
  OB_UNIS_ADD_LEN(unsed_worker_count_since_222rel_);
  OB_UNIS_ADD_LEN(tenant_schema_version_);
  OB_UNIS_ADD_LEN(cursor_count);
  OB_UNIS_ADD_LEN(plan_start_time_);
  OB_UNIS_ADD_LEN(last_trace_id_);
  OB_UNIS_ADD_LEN(tenant_srs_version_);
  OB_UNIS_ADD_LEN(original_param_cnt_);
  OB_UNIS_ADD_LEN(array_param_groups_.count());
  if (array_param_groups_.count() > 0) {
    for (int64_t i = 0 ; i < array_param_groups_.count(); i++) {
      OB_UNIS_ADD_LEN(array_param_groups_.at(i));
    }
  }
  return len;
}

OB_DEF_DESERIALIZE(ObPhysicalPlanCtx)
{
  int ret = OB_SUCCESS;
  int64_t param_cnt = 0;
  int32_t real_param_cnt = 0;
  int64_t param_idx = OB_INVALID_INDEX;
  ObObjParam param_obj;
  int64_t cursor_count = 0;
  // used for function sys_view_bigint_param(idx), @note unused anymore
  ObSEArray<common::ObObj, 1> sys_view_bigint_params_;
  char message_[1] = {'\0'}; //error msg buffer, unused anymore
  //按老的序列方式进行
  OB_UNIS_DECODE(tenant_id_);
  OB_UNIS_DECODE(tsc_snapshot_timestamp_);
  OB_UNIS_DECODE(cur_time_);
  OB_UNIS_DECODE(merging_frozen_time_);
  OB_UNIS_DECODE(ts_timeout_us_);
  OB_UNIS_DECODE(consistency_level_);
  OB_UNIS_DECODE(param_store_);
  OB_UNIS_DECODE(sys_view_bigint_params_);
  OB_UNIS_DECODE(sql_mode_);
  OB_UNIS_DECODE(autoinc_params_);
  OB_UNIS_DECODE(tablet_autoinc_param_);
  OB_UNIS_DECODE(last_insert_id_session_);
  OB_UNIS_DECODE(message_);
  OB_UNIS_DECODE(expr_op_size_);
  OB_UNIS_DECODE(is_ignore_stmt_);
  OB_UNIS_DECODE(param_cnt);
  if (OB_SUCC(ret) && param_cnt > 0) {
    if (OB_FAIL(param_store_.prepare_allocate(param_cnt))) {
      LOG_WARN("prepare_allocate param store failed", K(ret), K(param_cnt));
    } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &real_param_cnt))) {
      LOG_WARN("decode int32_t failed", K(data_len), K(pos), K(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < real_param_cnt; ++i) {
      ObObj tmp;
      OB_UNIS_DECODE(param_idx);
      OB_UNIS_DECODE(param_obj);
      if (OB_UNLIKELY(param_idx < 0) || OB_UNLIKELY(param_idx >= param_cnt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid param idx", K(param_idx), K(param_cnt));
      } else if (OB_FAIL(deep_copy_obj(allocator_, param_obj, tmp))) {
        LOG_WARN("deep copy object failed", K(ret), K(param_obj));
      } else {
        param_store_.at(param_idx) = tmp;
        param_store_.at(param_idx).set_param_meta();
      }
    }
  }
  if (OB_SUCC(ret) && param_cnt <= 0) {
    //param count <= 0不为按需序列化分区相关的param store，直接序列化所有的param
    //所以需要对param store的所有元素都执行一次深拷贝
  	for (int64_t i = 0; OB_SUCC(ret) && i < param_store_.count(); ++i) {
  	  const ObObjParam &objpara = param_store_.at(i);
  	  ObObj tmp;
      if (OB_FAIL(deep_copy_obj(allocator_, objpara, tmp))) {
      	LOG_WARN("deep copy obj failed", K(ret));
      } else {
        param_store_.at(i) = tmp;
        param_store_.at(i).set_param_meta();
      }
  	}
  }
  OB_UNIS_DECODE(foreign_key_checks_);
  OB_UNIS_DECODE(unsed_worker_count_since_222rel_);
  OB_UNIS_DECODE(tenant_schema_version_);
  OB_UNIS_DECODE(cursor_count);
  if (OB_SUCC(ret) && cursor_count > 0) {
    if (OB_FAIL(implicit_cursor_infos_.prepare_allocate(cursor_count))) {
      LOG_WARN("init implicit cursor infos failed", K(ret));
    }
  }
  OB_UNIS_DECODE(plan_start_time_);
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(plan_start_time_);
    (void)ObSQLUtils::adjust_time_by_ntp_offset(ts_timeout_us_);
  }
  OB_UNIS_DECODE(last_trace_id_);
  OB_UNIS_DECODE(tenant_srs_version_);
  OB_UNIS_DECODE(original_param_cnt_);
  int64_t array_group_count = 0;
  OB_UNIS_DECODE(array_group_count);
  if (OB_SUCC(ret) && array_group_count > 0) {
    for (int64_t i = 0 ; OB_SUCC(ret) && i < array_group_count; i++) {
      ObArrayParamGroup array_p_group;
      OB_UNIS_DECODE(array_p_group);
      if (OB_FAIL(array_param_groups_.push_back(array_p_group))) {
        LOG_WARN("failed to push back");
      }
    }
  }
  if (OB_SUCC(ret) && array_group_count > 0 &&
      datum_param_store_.count() == 0 &&
      datum_param_store_.count() != param_store_.count()) {
    if (OB_FAIL(datum_param_store_.prepare_allocate(param_store_.count()))) {
      LOG_WARN("fail to prepare allocate", K(ret), K(param_store_.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < param_store_.count(); i++) {
      ObDatumObjParam &datum_param = datum_param_store_.at(i);
      if (OB_FAIL(datum_param.alloc_datum_reserved_buff(param_store_.at(i).meta_, allocator_))) {
        LOG_WARN("alloc datum reserved buffer failed", K(ret));
      } else if (OB_FAIL(datum_param.from_objparam(param_store_.at(i), &allocator_))) {
        LOG_WARN("fail to convert obj param", K(ret), K(param_store_.at(i)));
      }
    }
  }
  return ret;
}

void ObPhysicalPlanCtx::add_px_dml_row_info(const ObPxDmlRowInfo &dml_row_info)
{
  add_row_matched_count(dml_row_info.row_match_count_);
  add_row_duplicated_count(dml_row_info.row_duplicated_count_);
  add_row_deleted_count(dml_row_info.row_deleted_count_);
}

int ObPhysicalPlanCtx::get_field(const int64_t idx, ObField &field)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(field_array_) || idx >= field_array_->count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("field array is not init", K(ret), K(field_array_), K(idx));
  } else {
    field = field_array_->at(idx);
  }
  return ret;
}

} //sql
} //oceanbase

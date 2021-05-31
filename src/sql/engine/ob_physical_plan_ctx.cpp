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
namespace oceanbase {
using namespace common;
using namespace share;
using namespace transaction;
namespace sql {
ObPhysicalPlanCtx::ObPhysicalPlanCtx(common::ObIAllocator& allocator)
    : allocator_(allocator),
      tenant_id_(OB_INVALID_ID),
      tsc_snapshot_timestamp_(0),
      cur_time_(),
      merging_frozen_time_(),
      ts_timeout_us_(0),
      consistency_level_(INVALID_CONSISTENCY),
      param_store_((ObWrapperAllocator(&allocator_))),
      datum_param_store_(ObWrapperAllocator(&allocator_)),
      param_frame_capacity_(0),
      sys_view_bigint_params_(),
      sql_mode_(SMO_DEFAULT),
      autoinc_params_(allocator),
      last_insert_id_session_(0),
      expr_op_size_(0),
      is_ignore_stmt_(false),
      bind_array_count_(0),
      bind_array_idx_(0),
      tenant_schema_version_(OB_INVALID_VERSION),
      start_trans_param_(),
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
      is_error_ignored_(false),
      is_select_into_(false),
      is_result_accurate_(true),
      foreign_key_checks_(true),
      unsed_worker_count_since_222rel_(0),
      exec_ctx_(NULL),
      table_scan_stat_(),
      table_row_count_list_(allocator),
      batched_stmt_param_idxs_(allocator),
      implicit_cursor_infos_(allocator),
      cur_stmt_id_(-1),
      is_or_expand_transformed_(false),
      is_show_seed_(false),
      is_multi_dml_(false),
      is_new_engine_(false),
      remote_sql_info_(),
      is_large_query_(false)
{
  message_[0] = '\0';
}

ObPhysicalPlanCtx::~ObPhysicalPlanCtx()
{}

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

// 1. generate datum_param_store_
// 2. generate param frame
int ObPhysicalPlanCtx::init_datum_param_store()
{
  int ret = OB_SUCCESS;
  datum_param_store_.reuse();
  param_frame_ptrs_.reuse();
  param_frame_capacity_ = 0;
  if (OB_FAIL(datum_param_store_.prepare_allocate(param_store_.count()))) {
    LOG_WARN("fail to prepare allocate", K(ret), K(param_store_.count()));
  }
  // by param_store, generate datum_param_store
  for (int64_t i = 0; OB_SUCC(ret) && i < param_store_.count(); i++) {
    ObDatumObjParam& datum_param = datum_param_store_.at(i);
    ObObjDatumMapType obj_datum_map = ObDatum::get_obj_datum_map_type(param_store_.at(i).meta_.get_type());
    if (OBJ_DATUM_NULL == obj_datum_map) {
      // do nothing
    } else {
      uint32_t def_res_len = ObDatum::get_reserved_size(obj_datum_map);
      if (NULL == (datum_param.datum_.ptr_ = static_cast<char*>(allocator_.alloc(def_res_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("fail to alloc memory", K(def_res_len), K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(datum_param.from_objparam(param_store_.at(i)))) {
        LOG_WARN("fail to convert obj param", K(ret), K(param_store_.at(i)));
      }
    }
  }
  // alocate param frame memory and set param datum
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
  ObAutoincrementService& auto_service = ObAutoincrementService::get_instance();
  ObIArray<AutoincParam>& autoinc_params = get_autoinc_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
    AutoincParam& autoinc_param = autoinc_params.at(i);
    if (OB_FAIL(auto_service.sync_insert_value_local(autoinc_param))) {
      LOG_WARN("failed to sync last insert value locally", K(ret));
    }
  }
  return ret;
}

int ObPhysicalPlanCtx::sync_last_value_global()
{
  int ret = OB_SUCCESS;
  ObAutoincrementService& auto_service = ObAutoincrementService::get_instance();
  ObIArray<AutoincParam>& autoinc_params = get_autoinc_params();
  for (int64_t i = 0; OB_SUCC(ret) && i < autoinc_params.count(); ++i) {
    AutoincParam& autoinc_param = autoinc_params.at(i);
    if (OB_FAIL(auto_service.sync_insert_value_global(autoinc_param))) {
      LOG_WARN("failed to sync last insert value globally", K(ret));
    }
  }
  return ret;
}

void ObPhysicalPlanCtx::set_cur_time(const int64_t& session_val, const ObSQLSessionInfo& session)
{
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

void ObPhysicalPlanCtx::set_phy_plan(const ObPhysicalPlan* phy_plan)
{
  phy_plan_ = phy_plan;
  if (nullptr != phy_plan) {
    is_new_engine_ = phy_plan->is_new_engine();
  }
}

int ObPhysicalPlanCtx::assign_batch_stmt_param_idxs(const BatchParamIdxArray& param_idxs)
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

int ObPhysicalPlanCtx::set_batched_stmt_partition_ids(ObIArray<int64_t>& partition_ids)
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
          LOG_WARN("store param index to partition ids failed", K(ret), K(array_idx), K(part_ids), K(i));
        }
      } else {
        int64_t last_idx = batched_stmt_param_idxs_.count();
        PartParamIdxArray param_idxs;
        param_idxs.part_id_ = part_id;
        param_idxs.part_param_idxs_.set_allocator(&allocator_);
        if (OB_FAIL(batched_stmt_param_idxs_.push_back(param_idxs))) {
          LOG_WARN("store param idxs failed", K(ret), K(param_idxs));
        } else if (OB_FAIL(batched_stmt_param_idxs_.at(last_idx).part_param_idxs_.init(partition_ids.count()))) {
          LOG_WARN("init param_idxs failed", K(ret), K(last_idx), K(partition_ids));
        } else if (OB_FAIL(batched_stmt_param_idxs_.at(last_idx).part_param_idxs_.push_back(i))) {
          LOG_WARN("store param index to param idxs failed", K(ret), K(i));
        } else if (OB_FAIL(part_ids.push_back(part_id))) {
          LOG_WARN("store partition id failed", K(ret), K(part_id));
        }
      }
    }
    LOG_DEBUG("set batched stmt partition ids end", K(ret), K(partition_ids), K_(batched_stmt_param_idxs));
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

int ObPhysicalPlanCtx::merge_implicit_cursors(const ObIArray<ObImplicitCursorInfo>& implicit_cursor)
{
  int ret = OB_SUCCESS;
  ARRAY_FOREACH(implicit_cursor, idx)
  {
    OZ(merge_implicit_cursor_info(implicit_cursor.at(idx)));
  }
  return ret;
}

int ObPhysicalPlanCtx::merge_implicit_cursor_info(const ObImplicitCursorInfo& implicit_cursor)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(implicit_cursor.stmt_id_ < 0) ||
      OB_UNLIKELY(implicit_cursor.stmt_id_ >= implicit_cursor_infos_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("implicit cursor is invalid", K(implicit_cursor), K(implicit_cursor_infos_.count()));
  } else if (OB_FAIL(implicit_cursor_infos_.at(implicit_cursor.stmt_id_).merge_cursor(implicit_cursor))) {
    LOG_WARN("merge implicit cursor info failed", K(ret), K(implicit_cursor), K(implicit_cursor_infos_));
  }
  LOG_DEBUG("merge implicit cursor info", K(ret), K(implicit_cursor), K(lbt()));
  return ret;
}

const ObIArray<int64_t>* ObPhysicalPlanCtx::get_part_param_idxs(int64_t part_id) const
{
  const ObIArray<int64_t>* part_param_idxs = nullptr;
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
    LOG_DEBUG("switch implicit cursor iter end", K(ret), K(cur_stmt_id_), K(implicit_cursor_infos_));
  } else if (implicit_cursor_infos_.at(cur_stmt_id_).stmt_id_ != cur_stmt_id_ &&
             implicit_cursor_infos_.at(cur_stmt_id_).stmt_id_ >= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid implicit cursor", K(ret), K(cur_stmt_id_), K(implicit_cursor_infos_));
  } else {
    const ObImplicitCursorInfo& cursor_info = implicit_cursor_infos_.at(cur_stmt_id_);
    set_affected_rows(cursor_info.affected_rows_);
    set_found_rows(cursor_info.found_rows_);
    set_row_matched_count(cursor_info.matched_rows_);
    set_row_duplicated_count(cursor_info.duplicated_rows_);
    set_row_deleted_count(cursor_info.deleted_rows_);
    ++cur_stmt_id_;
  }
  return ret;
}

int ObPhysicalPlanCtx::extend_datum_param_store(DatumParamStore& ext_datum_store)
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
    }  // for end

    LOG_DEBUG("try to extend param frame", K(ext_datum_store), K(datum_param_store_), K(param_store_));
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(extend_param_frame(old_size))) {
      LOG_WARN("failed to extend param frame", K(ret));
    } else {
      // transform ext datums to obj params and push back to param_store_
      for (int i = 0; OB_SUCC(ret) && i < ext_datum_store.count(); i++) {
        ObObjParam tmp_obj_param;
        if (OB_FAIL(ext_datum_store.at(i).to_objparam(tmp_obj_param))) {
          LOG_WARN("failed to transform expr datum to obj param", K(ret));
        } else if (OB_FAIL(param_store_.push_back(tmp_obj_param))) {
          LOG_WARN("failed to push back element", K(ret));
        }
      }  // for end
    }
    LOG_DEBUG("after extended param frame", K(param_store_));
  }

  return ret;
}

int ObPhysicalPlanCtx::reserve_param_frame(const int64_t capacity)
{
  int ret = OB_SUCCESS;
  if (capacity > param_frame_capacity_) {
    const int64_t cnt_per_frame = ObExprFrameInfo::EXPR_CNT_PER_FRAME;
    const int64_t item_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    int64_t frame_cnt = (capacity + cnt_per_frame - 1) / cnt_per_frame;
    int64_t last = capacity - (frame_cnt - 1) * cnt_per_frame;
    // align last frame capacity to pow of 2.
    last = std::min(cnt_per_frame, next_pow2(last));
    int64_t new_capacity = (frame_cnt - 1) * cnt_per_frame + last;

    const int64_t old_frame_cnt = (param_frame_capacity_ + cnt_per_frame - 1) / cnt_per_frame;
    int64_t frame_idx = old_frame_cnt;
    if (param_frame_capacity_ % cnt_per_frame != 0) {
      frame_idx--;
    }

    for (; OB_SUCC(ret) && frame_idx < frame_cnt; frame_idx++) {
      const int64_t cnt = frame_idx + 1 == frame_cnt ? last : cnt_per_frame;
      char* frame = static_cast<char*>(allocator_.alloc(item_size * cnt));
      if (OB_ISNULL(frame)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        MEMSET(frame, 0, item_size * cnt);
        if (frame_idx + 1 == param_frame_ptrs_.count()) {
          // copy last frame data
          int64_t old_last_cnt = param_frame_capacity_ % cnt_per_frame;
          MEMMOVE(frame, param_frame_ptrs_.at(frame_idx), item_size * old_last_cnt);
          param_frame_ptrs_.at(frame_idx) = frame;
        } else {
          if (OB_FAIL(param_frame_ptrs_.push_back(frame))) {
            LOG_WARN("array push back failed", K(ret));
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      param_frame_capacity_ = new_capacity;
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
    const int64_t datum_eval_info_size = sizeof(ObDatum) + sizeof(ObEvalInfo);
    for (int64_t i = old_size; i < datum_param_store_.count(); i++) {
      const int64_t idx = i / ObExprFrameInfo::EXPR_CNT_PER_FRAME;
      const int64_t off = (i % ObExprFrameInfo::EXPR_CNT_PER_FRAME) * datum_eval_info_size;
      ObDatum* datum = reinterpret_cast<ObDatum*>(param_frame_ptrs_.at(idx) + off);
      *datum = datum_param_store_.at(i).datum_;
      ObEvalInfo* eval_info = reinterpret_cast<ObEvalInfo*>(param_frame_ptrs_.at(idx) + off + sizeof(ObDatum));
      eval_info->evaluated_ = true;
    }
  }
  return ret;
}

OB_DEF_SERIALIZE(ObPhysicalPlanCtx)
{
  int ret = OB_SUCCESS;
  ParamStore empty_param_store;
  const ParamStore* param_store = NULL;
  const ObRowIdListArray* row_id_list_array = NULL;
  int64_t cursor_count = implicit_cursor_infos_.count();
  if (exec_ctx_ != NULL) {
    row_id_list_array = &exec_ctx_->get_row_id_list_array();
  }
  if ((row_id_list_array != NULL && !row_id_list_array->empty() && phy_plan_ != NULL) || is_multi_dml_) {
    param_store = &empty_param_store;
  } else {
    param_store = &param_store_;
  }
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
  OB_UNIS_ENCODE(last_insert_id_session_);
  OB_UNIS_ENCODE(message_);
  OB_UNIS_ENCODE(expr_op_size_);
  OB_UNIS_ENCODE(is_ignore_stmt_);
  if (OB_SUCC(ret) && row_id_list_array != NULL && !row_id_list_array->empty() && phy_plan_ != NULL &&
      !(param_store_.empty())) {
    OB_UNIS_ENCODE(param_store_.count());
    int64_t seri_param_cnt_pos = pos;
    int32_t real_param_cnt = 0;
    pos += serialization::encoded_length_i32(real_param_cnt);
    const ObIArray<int64_t>* param_idxs = NULL;
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
  OB_UNIS_ENCODE(is_new_engine_);
  OB_UNIS_ENCODE(is_large_query_);
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPhysicalPlanCtx)
{
  int64_t len = 0;
  ParamStore empty_param_store;
  const ParamStore* param_store = NULL;
  const ObRowIdListArray* row_id_list_array = NULL;
  int64_t cursor_count = implicit_cursor_infos_.count();
  if (exec_ctx_ != NULL) {
    row_id_list_array = &exec_ctx_->get_row_id_list_array();
  }
  if ((row_id_list_array != NULL && !row_id_list_array->empty() && phy_plan_ != NULL) || is_multi_dml_) {
    param_store = &empty_param_store;
  } else {
    param_store = &param_store_;
  }
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
  OB_UNIS_ADD_LEN(last_insert_id_session_);
  OB_UNIS_ADD_LEN(message_);
  OB_UNIS_ADD_LEN(expr_op_size_);
  OB_UNIS_ADD_LEN(is_ignore_stmt_);
  if (row_id_list_array != NULL && !row_id_list_array->empty() && phy_plan_ != NULL && !(param_store_.empty())) {
    OB_UNIS_ADD_LEN(param_store_.count());
    int32_t real_param_cnt = 0;
    len += serialization::encoded_length_i32(real_param_cnt);
    if (phy_plan_->get_row_param_map().count() > 0) {
      const ObIArray<int64_t>& param_idxs = phy_plan_->get_row_param_map().at(0);
      for (int64_t i = 0; i < param_idxs.count(); ++i) {
        OB_UNIS_ADD_LEN(param_idxs.at(i));
        OB_UNIS_ADD_LEN(param_store_.at(param_idxs.at(i)));
      }
    }
    for (int64_t k = 0; k < row_id_list_array->count(); ++k) {
      auto row_id_list = row_id_list_array->at(k);
      if (OB_NOT_NULL(row_id_list)) {
        for (int64_t i = 0; i < row_id_list->count(); ++i) {
          int64_t row_idx = row_id_list->at(i) + 1;
          if (row_idx < phy_plan_->get_row_param_map().count() && row_idx >= 0) {
            const ObIArray<int64_t>& param_idxs = phy_plan_->get_row_param_map().at(row_idx);
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
  OB_UNIS_ADD_LEN(is_new_engine_);
  OB_UNIS_ADD_LEN(is_large_query_);
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
    for (int64_t i = 0; OB_SUCC(ret) && i < param_store_.count(); ++i) {
      const ObObjParam& objpara = param_store_.at(i);
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
  OB_UNIS_DECODE(is_new_engine_);
  OB_UNIS_DECODE(is_large_query_);
  return ret;
}

void ObPhysicalPlanCtx::add_px_dml_row_info(const ObPxDmlRowInfo& dml_row_info)
{
  add_row_matched_count(dml_row_info.row_match_count_);
  add_row_duplicated_count(dml_row_info.row_duplicated_count_);
  add_row_deleted_count(dml_row_info.row_deleted_count_);
}

}  // namespace sql
}  // namespace oceanbase

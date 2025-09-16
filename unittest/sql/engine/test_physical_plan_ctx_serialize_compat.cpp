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
#include <gtest/gtest.h>
#define private public
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/engine/expr/ob_expr_sql_udt_utils.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/engine/ob_exec_context.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer
class ObPhysicalPlanCtxMaster
{
  OB_UNIS_VERSION(3);
public:
  ObPhysicalPlanCtxMaster(common::ObIAllocator &allocator)
    : allocator_(allocator),
      tenant_id_(1001),
      tsc_snapshot_timestamp_(1234567890),
      ts_timeout_us_(1000000),
      consistency_level_(STRONG),
      param_store_((ObWrapperAllocator(&allocator_))),
      datum_param_store_(ObWrapperAllocator(&allocator_)),
      original_param_cnt_(10),
      param_frame_capacity_(10),
      sql_mode_(20),
      autoinc_params_(allocator),
      tablet_autoinc_param_(),
      last_insert_id_session_(100),
      expr_op_size_(1024),
      is_ignore_stmt_(false),
      bind_array_count_(10),
      bind_array_idx_(0),
      tenant_schema_version_(1000),
      orig_question_mark_cnt_(5),
      tenant_srs_version_(1000),
      array_param_groups_(),
      affected_rows_(100),
      is_affect_found_row_(false),
      found_rows_(100),
      phy_plan_(nullptr),
      curr_col_index_(0),
      autoinc_cache_handle_(NULL),
      last_insert_id_to_client_(100),
      last_insert_id_cur_stmt_(100),
      autoinc_id_tmp_(100),
      autoinc_col_value_(100),
      last_insert_id_with_expr_(false),
      last_insert_id_changed_(false),
      row_matched_count_(100),
      row_duplicated_count_(0),
      row_deleted_count_(0),
      warning_count_(10),
      is_error_ignored_(true),
      is_select_into_(false),
      is_result_accurate_(true),
      foreign_key_checks_(false),
      unsed_worker_count_since_222rel_(0),
      exec_ctx_(nullptr),
      table_scan_stat_(),
      table_row_count_list_(allocator),
      batched_stmt_param_idxs_(allocator),
      implicit_cursor_infos_(allocator, 10),
      cur_stmt_id_(-1),
      is_or_expand_transformed_(false),
      is_show_seed_(false),
      is_multi_dml_(false),
      remote_sql_info_(),
      field_array_(nullptr),
      is_ps_protocol_(false),
      plan_start_time_(1234567890),
      is_ps_rewrite_sql_(false),
      spm_ts_timeout_us_(1000000),
      subschema_ctx_(allocator),
      all_local_session_vars_(allocator, 10),
      mview_ids_(allocator, 10),
      last_refresh_scns_(allocator, 10),
      total_memstore_read_row_count_(100),
      total_ssstore_read_row_count_(100),
      tx_id_(100),
      tm_sessid_(100),
      hint_xa_trans_stop_check_lock_(false),
      main_xa_trans_branch_(false),
      dblink_ids_(),
      is_direct_insert_plan_(false),
      check_pdml_affected_rows_(false)
  { }
  common::ObIAllocator &allocator_;
  int64_t tenant_id_;
  int64_t tsc_snapshot_timestamp_;
  common::ObObj cur_time_;
  common::ObObj merging_frozen_time_;
  int64_t ts_timeout_us_;
  common::ObConsistencyLevel consistency_level_;
  ParamStore param_store_;
  DatumParamStore datum_param_store_;
  common::ObSEArray<char *, 1, common::ModulePageAllocator, true> param_frame_ptrs_;
  int64_t original_param_cnt_;
  int64_t param_frame_capacity_;
  ObSQLMode sql_mode_;
  common::ObFixedArray<share::AutoincParam, common::ObIAllocator> autoinc_params_;
  share::ObTabletAutoincParam tablet_autoinc_param_;
  uint64_t last_insert_id_session_;
  int64_t expr_op_size_;
  bool is_ignore_stmt_;
  int64_t bind_array_count_;
  int64_t bind_array_idx_;
  int64_t tenant_schema_version_;
  int64_t orig_question_mark_cnt_;
  common::ObCurTraceId::TraceId last_trace_id_;
  int64_t tenant_srs_version_;
  ObSEArray<ObArrayParamGroup, 2> array_param_groups_;
  transaction::ObTxParam trans_param_;
  int64_t affected_rows_;
  bool is_affect_found_row_;
  int64_t found_rows_;
  const ObPhysicalPlan *phy_plan_;
  int64_t curr_col_index_;
  share::CacheHandle *autoinc_cache_handle_;
  uint64_t last_insert_id_to_client_;
  uint64_t last_insert_id_cur_stmt_;
  uint64_t autoinc_id_tmp_;
  uint64_t autoinc_col_value_;
  bool last_insert_id_with_expr_;
  bool last_insert_id_changed_;
  int64_t row_matched_count_;
  int64_t row_duplicated_count_;
  int64_t row_deleted_count_;
  int64_t warning_count_;
  bool is_error_ignored_;
  bool is_select_into_;
  bool is_result_accurate_;
  bool foreign_key_checks_;
  int64_t unsed_worker_count_since_222rel_;
  const ObExecContext *exec_ctx_;
  ObTableScanStat table_scan_stat_;
  common::ObFixedArray<ObTableRowCount, common::ObIAllocator> table_row_count_list_;
  BatchParamIdxArray batched_stmt_param_idxs_;
  ImplicitCursorInfoArray implicit_cursor_infos_;
  int64_t cur_stmt_id_;
  bool is_or_expand_transformed_;
  bool is_show_seed_;
  bool is_multi_dml_;
  ObRemoteSqlInfo remote_sql_info_;
  const common::ObIArray<ObField> *field_array_;
  bool is_ps_protocol_;
  int64_t plan_start_time_;
  bool is_ps_rewrite_sql_;
  int64_t spm_ts_timeout_us_;
  ObSubSchemaCtx subschema_ctx_;
  bool enable_rich_format_{true};
  common::ObFixedArray<ObSolidifiedVarsContext, common::ObIAllocator> all_local_session_vars_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> mview_ids_;
  common::ObFixedArray<uint64_t, common::ObIAllocator> last_refresh_scns_;
  int64_t total_memstore_read_row_count_;
  int64_t total_ssstore_read_row_count_;
  int64_t tx_id_;
  uint32_t tm_sessid_;
  bool hint_xa_trans_stop_check_lock_;
  bool main_xa_trans_branch_;
  ObSEArray<uint64_t, 8> dblink_ids_;
  bool is_direct_insert_plan_;
  bool check_pdml_affected_rows_;
};

OB_DEF_SERIALIZE(ObPhysicalPlanCtxMaster)
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
  OB_UNIS_ENCODE(enable_rich_format_);
  OB_UNIS_ENCODE(all_local_session_vars_.count());
  for (int64_t i = 0; OB_SUCC(ret) && i < all_local_session_vars_.count(); ++i) {
    if (OB_ISNULL(all_local_session_vars_.at(i).get_local_vars())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret));
    } else {
      OB_UNIS_ENCODE(*all_local_session_vars_.at(i).get_local_vars());
    }
  }
  OB_UNIS_ENCODE(mview_ids_);
  OB_UNIS_ENCODE(last_refresh_scns_);
  OB_UNIS_ENCODE(is_direct_insert_plan_);
  OB_UNIS_ENCODE(check_pdml_affected_rows_);
  return ret;
}

int ObPhysicalPlanCtxMaster::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);
  LOG_INFO("version", K(version), K(len));
  if (OB_SUCC(ret)) {
    if (len < 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can't decode object with negative length", K(len));
    } else if (data_len < len + pos) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("buf length not enough", K(len), K(pos), K(data_len));
    }
  }
  if (OB_SUCC(ret)) {
    const_cast<int64_t&>(data_len) = len;
    int64_t pos_orig = pos;
    buf = buf + pos_orig;
    pos = 0;

    int64_t param_cnt = 0;
    int32_t real_param_cnt = 0;
    int64_t param_idx = OB_INVALID_INDEX;
    ObObjParam param_obj;
    int64_t cursor_count = 0;
    int64_t local_var_array_cnt = 0;
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
        OB_UNIS_DECODE(param_idx);
        OB_UNIS_DECODE(param_obj);
        ObObjParam tmp = param_obj;
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
        ObObjParam tmp = objpara;
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

    if (version == 2) {
      enable_rich_format_ = false;
    } else {
      OB_UNIS_DECODE(enable_rich_format_);
    }

    OB_UNIS_DECODE(local_var_array_cnt);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(all_local_session_vars_.reserve(local_var_array_cnt))) {
        LOG_WARN("reserve local session vars failed", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < local_var_array_cnt; ++i) {
      ObLocalSessionVar *local_vars = OB_NEWx(ObLocalSessionVar, &allocator_);
      if (NULL == local_vars) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("alloc local var failed", K(ret));
      } else if (OB_FAIL(all_local_session_vars_.push_back(ObSolidifiedVarsContext(local_vars, &allocator_)))) {
        LOG_WARN("push back local session var array failed", K(ret));
      } else {
        local_vars->set_allocator(&allocator_);
        OB_UNIS_DECODE(*local_vars);
      }
    }
    // following is not deserialize, please add deserialize ahead.
    if (OB_SUCC(ret) && array_group_count > 0 &&
        datum_param_store_.count() == 0 &&
        datum_param_store_.count() != param_store_.count()) {
      // mock
      // if (OB_FAIL(init_param_store_after_deserialize())) {
      //   LOG_WARN("failed to deserialize param store", K(ret));
      // }
    }
    OB_UNIS_DECODE(mview_ids_);
    OB_UNIS_DECODE(last_refresh_scns_);
    OB_UNIS_DECODE(is_direct_insert_plan_);
    OB_UNIS_DECODE(check_pdml_affected_rows_);

    pos = pos_orig + len;
  }
  return ret;
}

class TestPhysicalPlanCtxSerializeCompat : public ::testing::Test
{
public:
  TestPhysicalPlanCtxSerializeCompat() {}
  virtual ~TestPhysicalPlanCtxSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void fill_physical_plan_ctx(ObPhysicalPlanCtx &ctx)
{
  ctx.tenant_id_ = 1001;
  ctx.tsc_snapshot_timestamp_ = 1234567890;
  ctx.ts_timeout_us_ = 1000000;
  ctx.consistency_level_ = STRONG;
  ctx.sql_mode_ = SMO_DEFAULT;
  ctx.last_insert_id_session_ = 100;
  ctx.expr_op_size_ = 1024;
  ctx.is_ignore_stmt_ = false;
  ctx.bind_array_count_ = 10;
  ctx.bind_array_idx_ = 0;
  ctx.tenant_schema_version_ = 1000;
  ctx.orig_question_mark_cnt_ = 5;
  ctx.tenant_srs_version_ = 1000;
  ctx.affected_rows_ = 100;
  ctx.is_affect_found_row_ = false;
  ctx.found_rows_ = 100;
  ctx.curr_col_index_ = 0;
  ctx.last_insert_id_to_client_ = 100;
  ctx.last_insert_id_cur_stmt_ = 100;
  ctx.autoinc_id_tmp_ = 100;
  ctx.autoinc_col_value_ = 100;
  ctx.last_insert_id_with_expr_ = false;
  ctx.last_insert_id_changed_ = false;
  ctx.row_matched_count_ = 100;
  ctx.row_duplicated_count_ = 0;
  ctx.row_deleted_count_ = 0;
  ctx.warning_count_ = 0;
  ctx.is_error_ignored_ = false;
  ctx.is_select_into_ = false;
  ctx.is_result_accurate_ = true;
  ctx.foreign_key_checks_ = true;
  ctx.unsed_worker_count_since_222rel_ = 0;
  ctx.cur_stmt_id_ = 1;
  ctx.is_or_expand_transformed_ = false;
  ctx.is_direct_insert_plan_ = false;
  ctx.check_pdml_affected_rows_ = true;
}

void verify_basic_fields_equal(const ObPhysicalPlanCtxMaster &ctx1, const ObPhysicalPlanCtx &ctx2)
{
  ASSERT_EQ(ctx1.tenant_id_, ctx2.tenant_id_);
  ASSERT_EQ(ctx1.tsc_snapshot_timestamp_, ctx2.tsc_snapshot_timestamp_);
  ASSERT_EQ(ctx1.cur_time_, ctx2.cur_time_);
  ASSERT_EQ(ctx1.merging_frozen_time_, ctx2.merging_frozen_time_);
  ASSERT_EQ(ctx1.ts_timeout_us_, ctx2.ts_timeout_us_);
  ASSERT_EQ(ctx1.consistency_level_, ctx2.consistency_level_);
  ASSERT_EQ(ctx1.sql_mode_, ctx2.sql_mode_);
  ASSERT_EQ(ctx1.last_insert_id_session_, ctx2.last_insert_id_session_);
  ASSERT_EQ(ctx1.expr_op_size_, ctx2.expr_op_size_);
  ASSERT_EQ(ctx1.is_ignore_stmt_, ctx2.is_ignore_stmt_);
  ASSERT_EQ(ctx1.tenant_schema_version_, ctx2.tenant_schema_version_);
  ASSERT_EQ(ctx1.tenant_srs_version_, ctx2.tenant_srs_version_);
  ASSERT_EQ(ctx1.unsed_worker_count_since_222rel_, ctx2.unsed_worker_count_since_222rel_);
  ASSERT_EQ(ctx1.is_direct_insert_plan_, ctx2.is_direct_insert_plan_);
  ASSERT_EQ(ctx1.check_pdml_affected_rows_, ctx2.check_pdml_affected_rows_);
}

TEST_F(TestPhysicalPlanCtxSerializeCompat, test_42x_to_master)
{
  ObArenaAllocator allocator;
  ObPhysicalPlanCtxMaster ctx_master(allocator);
  ObPhysicalPlanCtx ctx_42x(allocator);
  fill_physical_plan_ctx(ctx_42x);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctx_42x.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctx_master.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(ctx_master, ctx_42x);
}

TEST_F(TestPhysicalPlanCtxSerializeCompat, test_master_to_42x)
{
  ObArenaAllocator allocator;
  ObPhysicalPlanCtxMaster ctx_master(allocator);
  ObPhysicalPlanCtx ctx_42x(allocator);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctx_master.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, ctx_42x.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(ctx_master, ctx_42x);
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObClusterVersion::get_instance().cluster_version_ = CLUSTER_CURRENT_VERSION;
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
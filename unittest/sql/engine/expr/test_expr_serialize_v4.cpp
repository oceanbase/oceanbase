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

// Unit tests for the compact (v4) ObExpr serialization and the receiver-side
// frame layout replay (replay_expr_frame_layout).

#define USING_LOG_PREFIX SQL_ENG
#include <gtest/gtest.h>
#include "sql/engine/expr/ob_expr.h"
#include "sql/engine/expr/ob_expr_ser_compact.h"
#include "sql/engine/ob_ser_compact_bindings.h"
#include "sql/engine/expr/ob_expr_frame_info.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/ob_operator.h"
#include "sql/engine/ob_physical_plan.h"
#include "sql/engine/ob_physical_plan_ctx.h"
#include "sql/code_generator/ob_static_engine_expr_cg.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/engine/px/ob_px_util.h"
#include "sql/das/ob_das_task.h"
#include "sql/das/ob_data_access_service.h"
#include "sql/das/ob_das_scan_ctdef_compact.h"
#include "sql/das/ob_das_deserialize_cache.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_cluster_version.h"
#include "share/rc/ob_tenant_base.h"
#include "lib/allocator/page_arena.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/rc/context.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

enum TestV4WireConstant
{
  TEST_V4_HAS_ARGS = 1 << 3,
  TEST_V4_HAS_PARENTS = 1 << 4,
  TEST_V4_HAS_INNER_FUNCS = 1 << 5,
  TEST_V4_HAS_ATTRS = 1 << 10
};

class TestExprSerializeV4 : public ::testing::Test
{
public:
  TestExprSerializeV4()
  {}
  virtual ~TestExprSerializeV4()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}
};

class ExprSerializeArrayGuard
{
public:
  explicit ExprSerializeArrayGuard(ObExpr::ObExprIArray *array) : saved_(ObExpr::get_serialize_array())
  {
    ObExpr::get_serialize_array() = array;
  }
  void set(ObExpr::ObExprIArray *array)
  {
    ObExpr::get_serialize_array() = array;
  }
  ~ExprSerializeArrayGuard()
  {
    ObExpr::get_serialize_array() = saved_;
  }

private:
  ObExpr::ObExprIArray *saved_;
};

class ClusterVersionGuard
{
public:
  explicit ClusterVersionGuard(const uint64_t cluster_version) :
    saved_(ObClusterVersion::get_instance().get_cluster_version())
  {
    ObClusterVersion::get_instance().update_cluster_version(cluster_version);
  }
  ~ClusterVersionGuard()
  {
    ObClusterVersion::get_instance().update_cluster_version(saved_);
  }

private:
  uint64_t saved_;
};

class TenantCompactOptGuard
{
public:
  TenantCompactOptGuard(const uint64_t tenant_id, const bool enable) : config_(TENANT_CONF(tenant_id)), saved_(0)
  {
    if (config_.is_valid()) {
      saved_ = config_->_sql_rpc_opt_flags;
      config_->_sql_rpc_opt_flags =
        enable ? saved_ | ObSqlRpcOptFlags::COMPACT_EXPR_SERIALIZE : saved_ & ~ObSqlRpcOptFlags::COMPACT_EXPR_SERIALIZE;
    }
  }
  ~TenantCompactOptGuard()
  {
    if (config_.is_valid()) { config_->_sql_rpc_opt_flags = saved_; }
  }
  bool is_valid()
  {
    return config_.is_valid();
  }

private:
  omt::ObTenantConfigGuard config_;
  int64_t saved_;
};

static int decode_test_record(ObDatumMeta *meta, ObDatumMetaCompact *compact, const char *buf, const int64_t data_len,
                              int64_t &pos)
{
  int ret = OB_SUCCESS;
  *compact = ObDatumMetaCompact(*meta);
  DEF_UNIS_VERSION_DECODE_REC(record, *meta, *compact);
  OB_UNIS_DECODE(record);
  if (OB_FAIL(ret)) {
  } else if (!record.decoded()) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (record.version() != ObDatumMetaCompact::unis_declared_version()) {
    ret = OB_NOT_SUPPORTED;
  } else {
    ObDatumMetaReplayCtx replay_ctx;
    ret = compact->replay_to(*meta, replay_ctx);
  }
  return ret;
}

static int decode_test_record(ObExpr *expr, ObExprCompactHeader *header, const char *buf, const int64_t data_len,
                              int64_t &pos)
{
  int ret = OB_SUCCESS;
  *header = ObExprCompactHeader(*expr);
  DEF_UNIS_VERSION_DECODE_REC(record, *expr, *header);
  OB_UNIS_DECODE(record);
  if (OB_FAIL(ret)) {
  } else if (!record.decoded()) {
    ret = OB_DESERIALIZE_ERROR;
  }
  return ret;
}

static int decode_test_compact_expr(ObExpr &expr, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObExprCompactHeader header(expr);
  DEF_UNIS_VERSION_DECODE_REC(record, expr, header);
  OB_UNIS_DECODE(record);
  if (OB_FAIL(ret)) {
  } else if (!record.decoded()) {
    ret = OB_DESERIALIZE_ERROR;
  } else if (record.version() != ObExprCompactHeader::unis_declared_version()) {
    ret = OB_NOT_SUPPORTED;
  }
  return ret;
}

static int decode_v4_test_prefix(const char *buf, const int64_t data_len, int64_t &pos, int64_t &body_end,
                                 int64_t &bitmap)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t body_len = 0;
  int32_t type = T_INVALID;
  int64_t flag = 0;
  int32_t frame_idx = 0;
  ObDatumMeta datum_meta;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &version))) {
  } else if (OB_UNLIKELY(serialization::SER_COMPACT_OPT_VERSION != version)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &body_len))) {
  } else if (OB_UNLIKELY(body_len < 0 || body_len > data_len - pos)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    body_end = pos + body_len;
    if (OB_FAIL(serialization::decode_vi32(buf, body_end, pos, &type))) {
      LOG_WARN("decode test compact expr type failed", K(ret));
    }
    ObDatumMetaCompact compact_meta;
    serialization::CompactExprPtrGuard v4_guard(true);
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(decode_test_record(&datum_meta, &compact_meta, buf, body_end, pos))) {
      LOG_WARN("decode test compact expr datum meta failed", K(ret));
    } else if (OB_FAIL(serialization::decode_vi64(buf, body_end, pos, &flag))) {
      LOG_WARN("decode test compact expr flag failed", K(ret));
    } else if (OB_FAIL(serialization::decode_vi32(buf, body_end, pos, &frame_idx))) {
      LOG_WARN("decode test compact expr frame index failed", K(ret));
    } else if (OB_FAIL(serialization::decode_vi64(buf, body_end, pos, &bitmap))) {
      LOG_WARN("decode test compact expr bitmap failed", K(ret));
    }
  }
  return ret;
}

static int decode_outer_expr_count(const char *buf, const int64_t data_len, int64_t &pos, ObPhysicalPlanCtx &plan_ctx,
                                   int32_t &expr_cnt)
{
  int ret = OB_SUCCESS;
  int64_t need_ctx_cnt = 0;
  ObExprExtraSerializeInfo expr_info;
  expr_info.current_time_ = &plan_ctx.get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx.get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx.get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx.get_last_refresh_scns();
  if (OB_FAIL(serialization::decode(buf, data_len, pos, need_ctx_cnt))) {
  } else if (OB_FAIL(expr_info.deserialize(buf, data_len, pos))) {
  } else if (OB_FAIL(serialization::decode_i32(buf, data_len, pos, &expr_cnt))) {
  }
  return ret;
}

static int decode_das_expr_count(const char *buf, const int64_t data_len, int64_t &pos, ObPhysicalPlanCtx &plan_ctx,
                                 int32_t &expr_cnt)
{
  int ret = OB_SUCCESS;
  int64_t need_ctx_cnt = 0;
  ObExprExtraSerializeInfo expr_info;
  expr_info.current_time_ = &plan_ctx.get_cur_time();
  expr_info.last_trace_id_ = &plan_ctx.get_last_trace_id();
  expr_info.mview_ids_ = &plan_ctx.get_mview_ids();
  expr_info.last_refresh_scns_ = &plan_ctx.get_last_refresh_scns();
  if (OB_FAIL(serialization::decode(buf, data_len, pos, need_ctx_cnt))) {
  } else if (OB_FAIL(expr_info.deserialize(buf, data_len, pos))) {
  } else if (OB_UNLIKELY(pos + serialization::OB_SERIALIZE_SIZE_NEED_BYTES > data_len)) {
    ret = OB_DESERIALIZE_ERROR;
  } else {
    pos += serialization::OB_SERIALIZE_SIZE_NEED_BYTES;
    ret = serialization::decode_i32(buf, data_len, pos, &expr_cnt);
  }
  return ret;
}

// build one ObExpr with CG-like content; fixed flag selects a fixed-length type
static void build_expr(ObExpr &e, ObExprOperatorType type, ObObjType obj_type, int16_t scale, int16_t precision,
                       int32_t max_length, uint32_t res_buf_len, uint32_t frame_idx, bool batch_result)
{
  e.type_ = type;
  e.datum_meta_.type_ = obj_type;
  e.datum_meta_.cs_type_ = CS_TYPE_BINARY;
  e.datum_meta_.scale_ = static_cast<int8_t>(scale);
  e.datum_meta_.precision_ = precision;
  // CG-like obj_meta_ for plain types
  e.obj_meta_.set_type(obj_type);
  e.obj_meta_.set_collation_level(CS_LEVEL_NONE);
  e.obj_meta_.set_collation_type(CS_TYPE_BINARY);
  e.obj_meta_.set_scale(ob_is_bit_tc(obj_type) ? static_cast<ObScale>(precision) : static_cast<ObScale>(scale));
  e.max_length_ = max_length;
  e.obj_datum_map_ = ObDatum::get_obj_datum_map_type(obj_type);
  e.vec_value_tc_ = common::get_vec_value_tc(obj_type, static_cast<int8_t>(scale), precision);
  e.is_fixed_length_data_ = common::is_fixed_length_vec(e.vec_value_tc_);
  e.batch_result_ = batch_result;
  // mirror CG: fixed length types always reserve the type's default size
  if (e.is_fixed_length_data_ && T_QUESTIONMARK != type) {
    res_buf_len = ObDatum::get_reserved_size(e.obj_datum_map_, precision);
  }
  e.res_buf_len_ = res_buf_len;
  e.frame_idx_ = frame_idx;
}

// compare all fields that must survive a v4 round trip
static void check_v4_roundtrip(const ObExpr &src, const ObExpr &dst)
{
  ASSERT_EQ(src.type_, dst.type_);
  ASSERT_EQ(src.datum_meta_.type_, dst.datum_meta_.type_);
  ASSERT_EQ(src.datum_meta_.cs_type_, dst.datum_meta_.cs_type_);
  ASSERT_EQ(src.datum_meta_.scale_, dst.datum_meta_.scale_);
  ASSERT_EQ(src.datum_meta_.precision_, dst.datum_meta_.precision_);
  ASSERT_EQ(src.flag_, dst.flag_);
  ASSERT_EQ(src.frame_idx_, dst.frame_idx_);
  // derived members
  ASSERT_EQ(src.obj_datum_map_, dst.obj_datum_map_);
  ASSERT_EQ(src.vec_value_tc_, dst.vec_value_tc_);
  ASSERT_EQ(src.is_fixed_length_data_, dst.is_fixed_length_data_);
  // Whether metadata was serialized or derived is a codec detail; both paths
  // must reproduce the exact CG-visible values.
  ASSERT_EQ(src.obj_meta_.get_type(), dst.obj_meta_.get_type());
  ASSERT_EQ(src.obj_meta_.get_collation_level(), dst.obj_meta_.get_collation_level());
  ASSERT_EQ(src.obj_meta_.get_collation_type(), dst.obj_meta_.get_collation_type());
  ASSERT_EQ(src.obj_meta_.get_scale(), dst.obj_meta_.get_scale());
  ASSERT_EQ(src.max_length_, dst.max_length_);
  ASSERT_EQ(src.res_buf_len_, dst.res_buf_len_);
  ASSERT_EQ(src.arg_cnt_, dst.arg_cnt_);
  ASSERT_EQ(src.parent_cnt_, dst.parent_cnt_);
  ASSERT_EQ(src.extra_, dst.extra_);
  ASSERT_EQ(src.expr_ctx_id_, dst.expr_ctx_id_);
  ASSERT_EQ(src.local_session_var_id_, dst.local_session_var_id_);
  ASSERT_EQ(src.eval_func_, dst.eval_func_);
  ASSERT_EQ(src.eval_batch_func_, dst.eval_batch_func_);
  ASSERT_EQ(src.eval_vector_func_, dst.eval_vector_func_);
}

TEST_F(TestExprSerializeV4, legacy_record_roundtrip_and_v4_rejection)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ClusterVersionGuard version_guard(CLUSTER_CURRENT_VERSION);
    ObArray<ObExpr> src_exprs;
    ObArray<ObExpr> dst_exprs;
    ASSERT_EQ(OB_SUCCESS, src_exprs.prepare_allocate(1));
    ASSERT_EQ(OB_SUCCESS, dst_exprs.prepare_allocate(1));
    build_expr(src_exprs.at(0), T_REF_COLUMN, ObVarcharType, 0, 0, 64, 32, 2, true);
    src_exprs.at(0).datum_off_ = 17;
    src_exprs.at(0).eval_info_off_ = 29;
    src_exprs.at(0).vector_header_off_ = 41;
    ExprSerializeArrayGuard array_guard(&src_exprs);

    char legacy_buf[4096];
    int64_t legacy_pos = 0;
    ASSERT_EQ(OB_SUCCESS, src_exprs.at(0).serialize(legacy_buf, sizeof(legacy_buf), legacy_pos));
    ASSERT_EQ(legacy_pos, src_exprs.at(0).get_serialize_size());
    static const unsigned char LEGACY_EXPR_GOLDEN[] = {
      0x03, 0xe1, 0x80, 0x80, 0x80, 0x00, 0x59, 0x01, 0x84, 0x80, 0x80, 0x80, 0x00, 0x16, 0x3f, 0x00, 0x00, 0x16,
      0x01, 0x3f, 0x00, 0x40, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x11,
      0x00, 0x20, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x00, 0x1d, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x00, 0x29, 0xff, 0xff, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0xff,
      0x0f, 0xff, 0xff, 0xff, 0xff, 0x0f, 0xff, 0xff, 0xff, 0xff, 0x0f, 0x0d, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x00};
    ASSERT_EQ(static_cast<int64_t>(sizeof(LEGACY_EXPR_GOLDEN)), legacy_pos);
    ASSERT_EQ(0, MEMCMP(LEGACY_EXPR_GOLDEN, legacy_buf, legacy_pos));
    int64_t version_pos = 0;
    int64_t version = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(legacy_buf, legacy_pos, version_pos, &version));
    ASSERT_NE(serialization::SER_COMPACT_OPT_VERSION, version);

    array_guard.set(&dst_exprs);
    int64_t legacy_dpos = 0;
    ASSERT_EQ(OB_SUCCESS, dst_exprs.at(0).deserialize(legacy_buf, legacy_pos, legacy_dpos));
    ASSERT_EQ(legacy_pos, legacy_dpos);
    ASSERT_EQ(src_exprs.at(0).datum_off_, dst_exprs.at(0).datum_off_);
    ASSERT_EQ(src_exprs.at(0).eval_info_off_, dst_exprs.at(0).eval_info_off_);
    ASSERT_EQ(src_exprs.at(0).vector_header_off_, dst_exprs.at(0).vector_header_off_);

    array_guard.set(&src_exprs);
    char v4_buf[4096];
    int64_t v4_pos = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(src_exprs.at(0), v4_buf, sizeof(v4_buf), v4_pos, false));
    array_guard.set(&dst_exprs);
    int64_t reject_pos = 0;
    ASSERT_EQ(OB_NOT_SUPPORTED, dst_exprs.at(0).deserialize(v4_buf, v4_pos, reject_pos));
    ASSERT_LE(reject_pos, v4_pos);
  }
}

TEST_F(TestExprSerializeV4, v4_tls_dispatch_guards)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> src_exprs;
    ObArray<ObExpr> dst_exprs;
    ASSERT_EQ(OB_SUCCESS, src_exprs.prepare_allocate(1));
    ASSERT_EQ(OB_SUCCESS, dst_exprs.prepare_allocate(1));
    build_expr(src_exprs.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
    ExprSerializeArrayGuard array_guard(&src_exprs);

    char legacy_direct[512];
    char legacy_dispatch[512];
    int64_t legacy_direct_pos = 0;
    int64_t legacy_dispatch_pos = 0;
    ASSERT_EQ(OB_SUCCESS, src_exprs.at(0).serialize(legacy_direct, sizeof(legacy_direct), legacy_direct_pos));
    const bool saved_encoding = serialization::using_v4_encoding();
    const bool saved_decoding = serialization::use_compact_expr_ptr();
    {
      serialization::SerCompactEncodingGuard legacy_guard(false);
      ASSERT_FALSE(serialization::using_v4_encoding());
      ASSERT_EQ(OB_SUCCESS,
                encode_v4(&src_exprs.at(0), legacy_dispatch, sizeof(legacy_dispatch), legacy_dispatch_pos, false));
      ASSERT_EQ(legacy_direct_pos, get_serialize_size_v4(&src_exprs.at(0), false));
      {
        serialization::SerCompactEncodingGuard compact_guard(true);
        ASSERT_TRUE(serialization::using_v4_encoding());
      }
      ASSERT_FALSE(serialization::using_v4_encoding());
    }
    ASSERT_EQ(saved_encoding, serialization::using_v4_encoding());
    ASSERT_EQ(legacy_direct_pos, legacy_dispatch_pos);
    ASSERT_EQ(0, MEMCMP(legacy_direct, legacy_dispatch, legacy_direct_pos));

    char compact_direct[512];
    char compact_dispatch[512];
    int64_t compact_direct_pos = 0;
    int64_t compact_dispatch_pos = 0;
    ASSERT_EQ(OB_SUCCESS,
              encode_expr_v4(src_exprs.at(0), compact_direct, sizeof(compact_direct), compact_direct_pos, false));
    {
      serialization::SerCompactEncodingGuard compact_guard(true);
      ASSERT_TRUE(serialization::using_v4_encoding());
      ASSERT_TRUE(serialization::use_compact_expr_ptr());
      ASSERT_EQ(OB_SUCCESS,
                encode_v4(&src_exprs.at(0), compact_dispatch, sizeof(compact_dispatch), compact_dispatch_pos, false));
      ASSERT_EQ(compact_direct_pos, get_serialize_size_v4(&src_exprs.at(0), false));
    }
    ASSERT_EQ(saved_encoding, serialization::using_v4_encoding());
    ASSERT_EQ(compact_direct_pos, compact_dispatch_pos);
    ASSERT_EQ(0, MEMCMP(compact_direct, compact_dispatch, compact_direct_pos));

    array_guard.set(&dst_exprs);
    ObExprCompactHeader decoded_header;
    int64_t decode_pos = 0;
    {
      serialization::CompactExprPtrGuard compact_guard(true);
      ASSERT_TRUE(serialization::use_compact_expr_ptr());
      ASSERT_EQ(saved_encoding, serialization::using_v4_encoding());
      ASSERT_EQ(OB_SUCCESS,
                decode_test_record(&dst_exprs.at(0), &decoded_header, compact_dispatch,
                                               compact_dispatch_pos, decode_pos));
    }
    ASSERT_EQ(saved_decoding, serialization::use_compact_expr_ptr());
    ASSERT_EQ(compact_dispatch_pos, decode_pos);
    check_v4_roundtrip(src_exprs.at(0), dst_exprs.at(0));
  }
}

struct DecodeMacroProbe
{
  int calls_ = 0;
  int result_ = OB_SUCCESS;
  int deserialize(const char *, const int64_t, int64_t &pos)
  {
    ++calls_;
    ++pos;
    return result_;
  }
  int deserialize_parsed(const int &, int64_t &pos)
  {
    return deserialize(nullptr, 0, pos);
  }
  template <int64_t VERSION>
  int deserialize_expected(const char *buf, const int64_t data_len, int64_t &pos)
  {
    return deserialize(buf, data_len, pos);
  }
};

TEST_F(TestExprSerializeV4, decode_macros_respect_record_context_and_preserve_errors)
{
  const char buf[1] = {};
  const int64_t data_len = sizeof(buf);
  const int view = 0;
  for (const int macro : {0, 1, 2}) {
    for (const int64_t start : {0, 1, 2}) {
      for (const int initial_ret : {OB_SUCCESS, OB_ERR_UNEXPECTED}) {
        for (const int decode_ret : {OB_SUCCESS, OB_DESERIALIZE_ERROR}) {
          DecodeMacroProbe probe;
          probe.result_ = decode_ret;
          int ret = initial_ret;
          int64_t pos = start;
          if (macro == 0) {
            OB_UNIS_DECODE_PARSED(probe, view);
          } else if (macro == 1) {
            OB_UNIS_DECODE_REQUIRED(probe);
          } else {
            OB_UNIS_DECODE_EXPECTED(probe, 4);
          }
          // Parsed records own a validated view; only raw-input macros check the outer cursor.
          const bool called = initial_ret == OB_SUCCESS && (macro == 0 || start < data_len);
          ASSERT_EQ(called ? 1 : 0, probe.calls_);
          ASSERT_EQ(start + (called ? 1 : 0), pos);
          ASSERT_EQ(called ? decode_ret : initial_ret, ret);
        }
      }
    }
  }
}

TEST_F(TestExprSerializeV4, parsed_empty_record_validates_version)
{
  char buf[32];
  int64_t data_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, sizeof(buf), data_len, 5));
  ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf, sizeof(buf), data_len, 0));
  serialization::UnisRecordView view;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, view.decode(buf, data_len, pos));
  ASSERT_EQ(data_len, pos);
  ObExpr expr;
  ObExprCompactHeader header(expr);
  DEF_UNIS_VERSION_DECODE_REC(record, expr, header);
  int ret = OB_SUCCESS;
  OB_UNIS_DECODE_PARSED(record, view);
  ASSERT_EQ(OB_NOT_SUPPORTED, ret);
  ASSERT_FALSE(record.decoded());
  ASSERT_EQ(data_len, pos);
}

TEST_F(TestExprSerializeV4, compact_pointer_state_combines_encoding_and_decoding)
{
  ObArray<ObExpr> exprs;
  ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(128));
  ExprSerializeArrayGuard array_guard(&exprs);
  serialization::SerCompactEncodingGuard outer_encoding(false);
  serialization::CompactExprPtrGuard outer_decoding(false);
  for (const bool encoding : {false, true}) {
    for (const bool decoding : {false, true}) {
      serialization::SerCompactEncodingGuard encoding_guard(encoding);
      serialization::CompactExprPtrGuard decoding_guard(decoding);
      const bool compact = encoding || decoding;
      ASSERT_EQ(compact, serialization::use_compact_expr_ptr());
      for (ObExpr *expr : {static_cast<ObExpr *>(nullptr), &exprs.at(0), &exprs.at(127)}) {
        char bytes[8] = {};
        int64_t size = 0;
        ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), size, expr));
        ASSERT_EQ(size, serialization::encoded_length(expr));
        ASSERT_EQ(compact ? (expr == &exprs.at(127) ? 2 : 1) : 4, size);
        ObExpr *decoded = nullptr;
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, serialization::decode(bytes, size, pos, decoded));
        ASSERT_EQ(size, pos);
        ASSERT_EQ(expr, decoded);
      }
      decoding_guard.set(!decoding);
      ASSERT_EQ(encoding || !decoding, serialization::use_compact_expr_ptr());
    }
  }
  // A guard entered while encoding must not save the combined true value as TLS.
  {
    serialization::SerCompactEncodingGuard encoding_guard(true);
    serialization::CompactExprPtrGuard decoding_guard(false);
    ASSERT_TRUE(serialization::use_compact_expr_ptr());
    ASSERT_FALSE(serialization::compact_expr_ptr_decode_state());
  }
  ASSERT_FALSE(serialization::use_compact_expr_ptr());
}

TEST_F(TestExprSerializeV4, expr_segment_single_pass_decode_and_family_validation)
{
  ObArray<ObExpr> exprs;
  ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(2));
  for (int64_t i = 0; i < exprs.count(); ++i) {
    build_expr(exprs.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
    exprs.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
  }
  ExprSerializeArrayGuard array_guard(&exprs);
  char legacy[4096];
  char compact[4096];
  int64_t legacy_len = 0;
  int64_t compact_len = 0;
  {
    serialization::SerCompactEncodingGuard guard(false);
    ASSERT_EQ(OB_SUCCESS, encode_v4(&exprs.at(0), legacy, sizeof(legacy), legacy_len, false));
  }
  {
    serialization::SerCompactEncodingGuard guard(true);
    ASSERT_EQ(OB_SUCCESS, encode_v4(&exprs.at(0), compact, sizeof(compact), compact_len, false));
  }

  auto decode_segment = [](const char *records, const int64_t data_len, const int64_t records_pos, const int32_t count,
                           const int64_t limit, bool &compact) -> int {
    int ret = OB_SUCCESS;
    compact = false;
    if (records_pos < 0 || limit < records_pos || limit > data_len) { return OB_INVALID_ARGUMENT; }
    ObArenaAllocator allocator;
    std::vector<char> blob(limit - records_pos + 32);
    int64_t write_pos = 0;
    ret = serialization::encode_i32(blob.data(), blob.size(), write_pos, count);
    if (OB_SUCCESS == ret) {
      MEMCPY(blob.data() + write_pos, records + records_pos, limit - records_pos);
      write_pos += limit - records_pos;
      ret = serialization::encode_vi64(blob.data(), blob.size(), write_pos, 0);
    }
    ObDASRemoteInfo remote;
    remote.has_expr_ = true;
    ObArray<ObExpr> decoded;
    SerCompactState state = SerCompactState::UNKNOWN;
    int64_t read_pos = 0;
    if (OB_SUCCESS == ret) {
      ret = remote.decode_cacheable_ctdef_expr(blob.data(), write_pos, read_pos, allocator, decoded, state);
    }
    if (OB_SUCCESS == ret) {
      EXPECT_EQ(write_pos, read_pos);
      EXPECT_EQ(count, decoded.count());
      compact = state == SerCompactState::V4;
    } else {
      EXPECT_EQ(0, decoded.count());
      EXPECT_EQ(0, remote.ctdefs_.count());
    }
    return ret;
  };
  bool compact_v4 = false;
  ASSERT_EQ(OB_SUCCESS, decode_segment(legacy, legacy_len, 0, 1, legacy_len, compact_v4));
  ASSERT_FALSE(compact_v4);
  ASSERT_EQ(OB_SUCCESS, decode_segment(compact, compact_len, 0, 1, compact_len, compact_v4));
  ASSERT_TRUE(compact_v4);
  char compact_family[8192];
  MEMCPY(compact_family, compact, compact_len);
  MEMCPY(compact_family + compact_len, compact, compact_len);
  ASSERT_EQ(OB_SUCCESS, decode_segment(compact_family, 2 * compact_len, 0, 2, 2 * compact_len, compact_v4));
  ASSERT_TRUE(compact_v4);
  ASSERT_EQ(OB_SUCCESS, decode_segment(legacy, 0, 0, 0, 0, compact_v4));
  ASSERT_FALSE(compact_v4);

  char mixed[8192];
  MEMCPY(mixed, compact, compact_len);
  MEMCPY(mixed + compact_len, legacy, legacy_len);
  const int64_t mixed_len = compact_len + legacy_len;
  ASSERT_EQ(OB_NOT_SUPPORTED, decode_segment(mixed, mixed_len, 0, 2, mixed_len, compact_v4));
  MEMCPY(mixed, legacy, legacy_len);
  MEMCPY(mixed + legacy_len, compact, compact_len);
  ASSERT_EQ(OB_NOT_SUPPORTED, decode_segment(mixed, mixed_len, 0, 2, mixed_len, compact_v4));

  ASSERT_EQ(OB_DESERIALIZE_ERROR, decode_segment(legacy, legacy_len, 0, -1, legacy_len, compact_v4));
  ASSERT_EQ(OB_DESERIALIZE_ERROR, decode_segment(legacy, 0, 0, 2, 0, compact_v4));
  const int32_t above_old_response_budget = static_cast<int32_t>(das::OB_DAS_MAX_PACKET_SIZE / sizeof(ObExpr) + 1);
  ASSERT_EQ(OB_DESERIALIZE_ERROR,
            decode_segment(legacy, legacy_len, 0, above_old_response_budget, legacy_len, compact_v4));
  std::vector<char> large_valid_segment(static_cast<int64_t>(above_old_response_budget) * compact_len);
  for (int64_t i = 0; i < above_old_response_budget; ++i) {
    MEMCPY(large_valid_segment.data() + i * compact_len, compact, compact_len);
  }
  ASSERT_EQ(OB_SUCCESS, decode_segment(large_valid_segment.data(), large_valid_segment.size(), 0,
                                       above_old_response_budget, large_valid_segment.size(), compact_v4));
  ASSERT_TRUE(compact_v4);
  ASSERT_EQ(OB_DESERIALIZE_ERROR, decode_segment(legacy, legacy_len, 0, 2, legacy_len, compact_v4));
  ASSERT_NE(OB_SUCCESS, decode_segment(compact, compact_len - 1, 0, 1, compact_len - 1, compact_v4));

  const int64_t unsupported_versions[] = {-1, 0, 5};
  for (int64_t i = 0; i < ARRAYSIZEOF(unsupported_versions); ++i) {
    char unsupported[32];
    int64_t unsupported_len = 0;
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_vi64(unsupported, sizeof(unsupported), unsupported_len, unsupported_versions[i]));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(unsupported, sizeof(unsupported), unsupported_len, 0));
    ASSERT_EQ(OB_NOT_SUPPORTED, decode_segment(unsupported, unsupported_len, 0, 1, unsupported_len, compact_v4));
  }

  ObExpr decoded;
  ObExprCompactHeader header;
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decode_test_record(&decoded, &header, compact, compact_len, decode_pos));
  ASSERT_EQ(compact_len, decode_pos);
}

TEST_F(TestExprSerializeV4, expr_serialize_array_guard_constructor_scope)
{
  ObArenaAllocator allocator;
  ObArray<ObExpr> sentinel;
  ASSERT_EQ(OB_SUCCESS, sentinel.prepare_allocate(1));
  ExprSerializeArrayGuard outer_guard(&sentinel);
  ObExprFrameInfo frame_info(allocator);
  ASSERT_EQ(OB_SUCCESS, frame_info.rt_exprs_.prepare_allocate(2));

  {
    ObExprSerializeArrayGuard no_expr_guard(false, nullptr);
    ASSERT_EQ(nullptr, ObExpr::get_serialize_array());
  }
  ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
  {
    ObExprSerializeArrayGuard frame_guard(true, &frame_info);
    ASSERT_EQ(&frame_info.rt_exprs_, ObExpr::get_serialize_array());
  }
  ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
  {
    ObExprSerializeArrayGuard array_guard(&frame_info.rt_exprs_);
    ASSERT_EQ(&frame_info.rt_exprs_, ObExpr::get_serialize_array());
  }
  ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
}

TEST_F(TestExprSerializeV4, expr_frame_member_selection_checks_bounds)
{
  ObArenaAllocator allocator;
  ObExprFrameInfo frame_info(allocator);
  ASSERT_EQ(OB_SUCCESS, frame_info.rt_exprs_.prepare_allocate(1));
  ASSERT_EQ(OB_SUCCESS, frame_info.ser_expr_marks_.init(2));
  ASSERT_EQ(OB_SUCCESS, frame_info.ser_expr_marks_.prepare_allocate(2));
  frame_info.rt_exprs_.at(0).type_ = T_REF_COLUMN;
  frame_info.ser_expr_marks_.at(0) = true;
  frame_info.ser_expr_marks_.at(1) = true;
  ASSERT_TRUE(expr_frame_member_selected(frame_info, 0));
  ASSERT_FALSE(expr_frame_member_selected(frame_info, 1));
  ASSERT_FALSE(expr_frame_member_selected(frame_info, -1));
}

TEST_F(TestExprSerializeV4, compact_plan_flag_lifecycle_and_legacy_input)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObPhysicalPlan plan;
    ASSERT_FALSE(plan.is_serialize_compact_opt_enabled());
    const int64_t reserved_flag = 1LL << 7;
    plan.set_serialize_compact_opt_flags(reserved_flag);
    ASSERT_FALSE(plan.is_serialize_compact_opt_enabled());
    const int64_t flags = reserved_flag | ObPhysicalPlan::SERIALIZE_COMPACT_OPT;
    plan.set_serialize_compact_opt_flags(flags);
    {
      ClusterVersionGuard global_flip_guard(CLUSTER_VERSION_4_4_2_2);
      ASSERT_TRUE(plan.is_serialize_compact_opt_enabled());
    }
    ObPhysicalPlan copied;
    ASSERT_EQ(OB_SUCCESS, copied.copy_common_info(plan));
    ASSERT_EQ(flags, copied.get_serialize_compact_opt_flags());

    std::vector<char> buf(plan.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, plan.serialize(buf.data(), buf.size(), pos));
    ASSERT_EQ(buf.size(), pos);
    ObPhysicalPlan decoded;
    int64_t decoded_pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buf.data(), pos, decoded_pos));
    ASSERT_EQ(flags, decoded.get_serialize_compact_opt_flags());
    ASSERT_EQ(pos, decoded_pos);

    int64_t header_pos = 0;
    int64_t version = 0;
    int64_t body_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf.data(), pos, header_pos, &version));
    const int64_t length_pos = header_pos;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf.data(), pos, header_pos, &body_len));
    const int64_t flags_size = serialization::encoded_length(flags);
    int64_t patch_pos = length_pos;
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_fixed_bytes_i64(buf.data(), buf.size(), patch_pos, body_len - flags_size));
    ObPhysicalPlan legacy_input;
    decoded_pos = 0;
    ASSERT_EQ(OB_SUCCESS, legacy_input.deserialize(buf.data(), pos - flags_size, decoded_pos));
    ASSERT_FALSE(legacy_input.is_serialize_compact_opt_enabled());
    ASSERT_EQ(pos - flags_size, decoded_pos);
    copied.reset();
    ASSERT_EQ(0, copied.get_serialize_compact_opt_flags());
  }
}

TEST_F(TestExprSerializeV4, cache_key_includes_owned_layout_metadata)
{
  ObArenaAllocator allocator;
  char blob[] = "same expression and ctdef records";
  char layout[] = "layout one";
  ObDasDeserializeCacheKey key;
  key.type_ = ObDasDeserializeCacheKey::CacheType::CTDEF_EXPR;
  key.key_.assign_ptr(blob, sizeof(blob));
  key.layout_key_.assign_ptr(layout, sizeof(layout));
  ObDasDeserializeCacheKey copied;
  ASSERT_EQ(OB_SUCCESS, copied.deep_copy(allocator, key));
  ASSERT_TRUE(key.is_equal(copied));
  ASSERT_EQ(key.hash(), copied.hash());
  ASSERT_NE(key.key_.ptr(), copied.key_.ptr());
  ASSERT_NE(key.layout_key_.ptr(), copied.layout_key_.ptr());
  layout[0] = 'L';
  ASSERT_FALSE(key.is_equal(copied));
  ASSERT_NE(key.hash(), copied.hash());
  layout[0] = 'l';
  ASSERT_TRUE(key.is_equal(copied));
  blob[0] = 'S';
  ASSERT_FALSE(key.is_equal(copied));
  ASSERT_EQ('s', copied.key_.ptr()[0]);
}

TEST_F(TestExprSerializeV4, compact_frame_tail_rejects_truncation_and_oversize)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestCompactTail", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArenaAllocator allocator;
    ObExecContext src_ctx(allocator);
    ObPhysicalPlanCtx src_plan_ctx(allocator);
    ObPhysicalPlan plan;
    src_plan_ctx.set_phy_plan(&plan);
    src_ctx.set_physical_plan_ctx(&src_plan_ctx);
    ObExprFrameInfo info(allocator);
    ObFrameInfo frame;
    frame.frame_idx_ = 0;
    frame.expr_cnt_ = 1;
    frame.frame_size_ = sizeof(ObDatum) + sizeof(ObEvalInfo);
    frame.use_rich_format_ = false;
    ASSERT_EQ(OB_SUCCESS, info.const_frame_.init(1));
    ASSERT_EQ(OB_SUCCESS, info.const_frame_.push_back(frame));
    char *frame_buf = static_cast<char *>(allocator.alloc(frame.frame_size_));
    ASSERT_NE(nullptr, frame_buf);
    MEMSET(frame_buf, 0, frame.frame_size_);
    char *frames[] = {frame_buf};
    src_ctx.set_frames(frames);
    src_ctx.set_frame_cnt(1);
    const int64_t size = expr_frame_tail_size(src_ctx, info, true);
    std::vector<char> buf(size + 1, 0);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_frame_tail(buf.data(), size, pos, src_ctx, info, true));
    ASSERT_EQ(size, pos);
    ObExprFrameInfo received_info(allocator);
    ObDASExprFrameLayout layout;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, layout.deserialize_meta(buf.data(), size, pos, received_info));
    ASSERT_EQ(1, layout.frame_cnt_);
    const int64_t payload_pos = pos;
    ObExecContext dst_ctx(allocator);
    ObPhysicalPlanCtx dst_plan_ctx(allocator);
    dst_ctx.set_physical_plan_ctx(&dst_plan_ctx);
    ASSERT_EQ(OB_SUCCESS, decode_expr_frame_data(buf.data(), size, pos, dst_ctx, received_info, layout, true));
    ASSERT_EQ(size, pos);
    pos = payload_pos;
    received_info.const_frame_ptrs_.reuse();
    EXPECT_EQ(OB_DESERIALIZE_ERROR,
              decode_expr_frame_data(buf.data(), size - 1, pos, dst_ctx, received_info, layout, true));
    pos = payload_pos;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(buf.data(), buf.size(), pos, frame.frame_size_ + 1));
    pos = payload_pos;
    received_info.const_frame_ptrs_.reuse();
    EXPECT_EQ(OB_DESERIALIZE_ERROR,
              decode_expr_frame_data(buf.data(), buf.size(), pos, dst_ctx, received_info, layout, true));
  }
}

TEST_F(TestExprSerializeV4, packed_frame_slots_restore_values_and_local_vectors)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "PackedFrame", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    for (const bool rich : {false, true}) {
      ObArenaAllocator allocator;
      ObExecContext src_ctx(allocator);
      ObPhysicalPlanCtx src_plan_ctx(allocator);
      ObPhysicalPlan plan;
      src_plan_ctx.set_phy_plan(&plan);
      src_ctx.set_physical_plan_ctx(&src_plan_ctx);
      ObExprFrameInfo info(allocator);
      ASSERT_EQ(OB_SUCCESS, info.const_frame_.init(1));
      ASSERT_EQ(OB_SUCCESS, info.param_frame_.init(1));
      ASSERT_EQ(OB_SUCCESS, info.dynamic_frame_.init(1));
      ASSERT_EQ(OB_SUCCESS, info.datum_frame_.init(1));
      const int64_t base = sizeof(ObDatum) + sizeof(ObEvalInfo);
      const int64_t stride = base + (rich ? sizeof(VectorHeader) : 0);
      const char binary[] = {'a', '\0', 'b'};
      int64_t integer = 123456789;
      char *frames[4];
      ObIArray<ObFrameInfo> *arrays[] = {&info.const_frame_, &info.param_frame_, &info.dynamic_frame_,
                                       &info.datum_frame_};
      for (int64_t f = 0; f < 4; ++f) {
        ObFrameInfo frame;
        frame.frame_idx_ = f;
        frame.expr_cnt_ = 4;
        frame.frame_size_ = 4 * stride;
        frame.use_rich_format_ = rich;
        ASSERT_EQ(OB_SUCCESS, arrays[f]->push_back(frame));
        frames[f] = static_cast<char *>(allocator.alloc(frame.frame_size_));
        ASSERT_NE(nullptr, frames[f]);
        MEMSET(frames[f], 0, frame.frame_size_);
        for (int64_t j = 0; j < 4; ++j) {
          ObDatum *datum = new (frames[f] + j * stride) ObDatum();
          if (j == 0) { datum->set_null(); }
          if (j == 1) { datum->set_string("", 0); }
          if (j == 2) { datum->set_string(binary, sizeof(binary)); }
          if (j == 3) { datum->set_string(reinterpret_cast<const char *>(&integer), sizeof(integer)); }
          ObEvalInfo *eval = reinterpret_cast<ObEvalInfo *>(frames[f] + j * stride + sizeof(ObDatum));
          eval->set_evaluated(true);
          eval->set_projected(j % 2 == 0);
          eval->cnt_ = 1;
          if (rich) {
            // Opaque sender-only bytes must never survive packed decoding.
            MEMSET(frames[f] + j * stride + base, 0x5a, sizeof(VectorHeader));
          }
        }
      }
      src_ctx.set_frames(frames);
      src_ctx.set_frame_cnt(4);
      plan.set_serialize_compact_opt_flags(ObSqlRpcOptFlags::COMPACT_EXPR_SERIALIZE);
      ObDASExprFrameLayout source_layout;
      ASSERT_EQ(OB_SUCCESS, source_layout.init(src_ctx, info.partial_frame_));
      int64_t old_size = source_layout.meta_size(info);
      // Compare against the old slot encoding with the same compact frame metadata.
      for (int64_t f = 0; f < 4; ++f) {
        old_size += ObPxTreeSerializer::get_serialize_frame_info_size<false>(
          *arrays[f], frames, 4, f == 3);
      }
      const int64_t size = expr_frame_tail_size(src_ctx, info, true);
      const int64_t frame_saving = rich ? 4 * sizeof(VectorHeader)
        + serialization::encoded_length(4 * stride) - serialization::encoded_length(4 * base) : 0;
      ASSERT_EQ(3 * frame_saving, old_size - size);
      std::vector<char> bytes(size);
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, encode_expr_frame_tail(bytes.data(), size, pos, src_ctx, info, true));
      ASSERT_EQ(size, pos);
      ObExecContext dst_ctx(allocator);
      ObPhysicalPlanCtx dst_plan_ctx(allocator);
      ObPhysicalPlan receiving_plan;
      receiving_plan.set_serialize_compact_opt_flags(0);
      dst_plan_ctx.set_phy_plan(&receiving_plan);
      dst_ctx.set_physical_plan_ctx(&dst_plan_ctx);
      ObExprFrameInfo decoded(allocator);
      ObDASExprFrameLayout layout;
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, layout.deserialize_meta(bytes.data(), size, pos, decoded));
      int64_t probe_pos = pos;
      int64_t slot_bytes = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode(bytes.data(), size, probe_pos, slot_bytes));
      ASSERT_EQ(4 * base, slot_bytes);
      // Decode follows the wire format even with compact disabled on the receiving plan.
      ASSERT_EQ(OB_SUCCESS, decode_expr_frame_data(bytes.data(), size, pos, dst_ctx, decoded, layout, true));
      ASSERT_EQ(size, pos);
      ObEvalCtx eval_ctx(dst_ctx);
      for (int64_t f = 0; f < 3; ++f) {
        for (int64_t j = 0; j < 4; ++j) {
          const ObDatum &before = *reinterpret_cast<ObDatum *>(frames[f] + j * stride);
          ObDatum &after = *reinterpret_cast<ObDatum *>(dst_ctx.get_frames()[f] + j * stride);
          ASSERT_EQ(before.is_null(), after.is_null());
          ASSERT_EQ(before.len_, after.len_);
          ASSERT_EQ(0, MEMCMP(frames[f] + j * stride + sizeof(ObDatum),
                             dst_ctx.get_frames()[f] + j * stride + sizeof(ObDatum), sizeof(ObEvalInfo)));
          if (!before.is_null() && before.len_ > 0) {
            ASSERT_NE(before.ptr_, after.ptr_);
            ASSERT_EQ(0, MEMCMP(before.ptr_, after.ptr_, before.len_));
          }
          if (rich) {
            VectorHeader &header = *reinterpret_cast<VectorHeader *>(dst_ctx.get_frames()[f] + j * stride + base);
            ASSERT_EQ(VEC_INVALID, header.get_format());
            for (char c : header.vector_buf_) { ASSERT_EQ(0, c); }
          }
        }
        if (rich) {
          ObExpr expr;
          build_expr(expr, T_INT, ObIntType, 0, 20, 8, 8, f, false);
          expr.datum_off_ = 3 * stride;
          expr.eval_info_off_ = expr.datum_off_ + sizeof(ObDatum);
          expr.vector_header_off_ = expr.datum_off_ + base;
          ASSERT_EQ(OB_SUCCESS, expr.init_vector(eval_ctx, VEC_UNIFORM_CONST, 1));
          ASSERT_EQ(VEC_UNIFORM_CONST, expr.get_format(eval_ctx));
          ASSERT_EQ(integer, expr.get_vector(eval_ctx)->get_int(0));
          // Mutation of the receiver datum must be visible through the rebuilt vector.
          expr.locate_expr_datum(eval_ctx).set_int(integer + 1);
          ASSERT_EQ(integer + 1, expr.get_vector(eval_ctx)->get_int(0));
          ASSERT_EQ(integer, reinterpret_cast<ObDatum *>(frames[f] + 3 * stride)->get_int());
        }
      }
      // The ordinary datum frame still carries no execution-time slot contents.
      for (int64_t i = 0; i < 4 * stride; ++i) { ASSERT_EQ(0, dst_ctx.get_frames()[3][i]); }
      if (rich) {
        const int64_t legacy_size = ObPxTreeSerializer::get_serialize_frame_info_size<false>(
          info.const_frame_, frames, 4);
        std::vector<char> legacy_bytes(legacy_size);
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_frame_info<false>(
          legacy_bytes.data(), legacy_size, pos, info.const_frame_, frames, 4));
        ASSERT_EQ(legacy_size, pos);
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, serialization::decode(legacy_bytes.data(), legacy_size, pos, slot_bytes));
        ASSERT_EQ(4 * stride, slot_bytes);
        char *legacy_frame_ptrs[1] = {};
        char **legacy_frames = legacy_frame_ptrs;
        int64_t legacy_frame_cnt = 1;
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_frame_info<false>(
          legacy_bytes.data(), legacy_size, pos, allocator, info.const_frame_, nullptr,
          legacy_frames, legacy_frame_cnt));
        ASSERT_EQ(legacy_size, pos);
        // Legacy decoding still copies the original rich slots, including VectorHeader.
        for (int64_t j = 0; j < 4; ++j) {
          ASSERT_EQ(0, MEMCMP(frames[0] + j * stride + base,
                             legacy_frames[0] + j * stride + base, sizeof(VectorHeader)));
        }
      }
    }
  }
}

TEST_F(TestExprSerializeV4, packed_frame_slots_reject_malformed_lengths)
{
  ObArenaAllocator allocator;
  const int64_t base = sizeof(ObDatum) + sizeof(ObEvalInfo);
  const int64_t stride = base + sizeof(VectorHeader);
  ObSEArray<ObFrameInfo, 1> frames_info;
  ObFrameInfo frame;
  frame.frame_idx_ = 0;
  frame.expr_cnt_ = 1;
  frame.frame_size_ = stride;
  frame.use_rich_format_ = true;
  ASSERT_EQ(OB_SUCCESS, frames_info.push_back(frame));
  char bytes[256] = {};
  char *frame_ptrs[1] = {};
  char **frames = frame_ptrs;
  int64_t frame_cnt = 1;
  for (const int64_t length : {INT64_MIN, -base - 1, -base}) {
    int64_t written = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, length));
    // Include a complete payload so rejection cannot be attributed to truncation.
    written += base;
    ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, int64_t(0)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, int64_t(0)));
    for (const bool compact : {false, true}) {
      int64_t pos = 0;
      EXPECT_EQ(OB_DESERIALIZE_ERROR, ObPxTreeSerializer::deserialize_frame_info<false>(
        bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, false, compact));
    }
  }
  for (const int64_t length : {base - 1, base + 1}) {
    MEMSET(bytes, 0, sizeof(bytes));
    int64_t written = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, length));
    written += length;
    ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, int64_t(0)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, int64_t(0)));
    int64_t pos = 0;
    EXPECT_EQ(OB_DESERIALIZE_ERROR, ObPxTreeSerializer::deserialize_frame_info<false>(
      bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, false, true));
  }
  MEMSET(bytes, 0, sizeof(bytes));
  int64_t written = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, base));
  int64_t pos = 0;
  // A valid compact length still requires the complete slot payload.
  EXPECT_EQ(OB_DESERIALIZE_ERROR, ObPxTreeSerializer::deserialize_frame_info<false>(
    bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, false, true));
  written += base;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, int64_t(0)));
  ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes, sizeof(bytes), written, int64_t(0)));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_frame_info<false>(
    bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, false, true));
  ASSERT_EQ(written, pos);
  pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, ObPxTreeSerializer::deserialize_frame_info<false>(
    bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, true, true));
  // Non-rich slots retain their original positive-length representation in either format.
  frames_info.at(0).use_rich_format_ = false;
  for (const bool compact : {false, true}) {
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_frame_info<false>(
      bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, false, compact));
    ASSERT_EQ(written, pos);
  }
  frames_info.at(0).use_rich_format_ = true;
  frames_info.at(0).frame_size_ = stride - 1;
  pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, ObPxTreeSerializer::deserialize_frame_info<false>(
    bytes, written, pos, allocator, frames_info, nullptr, frames, frame_cnt, false, true));
}

TEST_F(TestExprSerializeV4, compact_frame_metadata_replaces_previous_layout)
{
  ObArenaAllocator allocator;
  ObExprFrameInfo source(allocator);
  ObExprFrameInfo decoded(allocator);
  ObDASExprFrameLayout layout;
  layout.frame_cnt_ = 1;
  ObFrameInfo frame;
  frame.frame_idx_ = 0;
  frame.expr_cnt_ = 1;
  frame.frame_size_ = sizeof(ObDatum) + sizeof(ObEvalInfo);
  frame.use_rich_format_ = false;
  ASSERT_EQ(OB_SUCCESS, source.const_frame_.init(1));
  ASSERT_EQ(OB_SUCCESS, source.const_frame_.push_back(frame));
  char buf[256];
  int64_t size = 0;
  ASSERT_EQ(OB_SUCCESS, layout.serialize_meta(buf, sizeof(buf), size, source));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, layout.deserialize_meta(buf, size, pos, decoded));
  ASSERT_EQ(size, pos);
  ASSERT_EQ(1, decoded.const_frame_.count());

  source.const_frame_.reuse();
  layout.frame_cnt_ = 0;
  size = 0;
  ASSERT_EQ(OB_SUCCESS, layout.serialize_meta(buf, sizeof(buf), size, source));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, layout.deserialize_meta(buf, size, pos, decoded));
  ASSERT_EQ(size, pos);
  EXPECT_EQ(0, decoded.const_frame_.count());
  EXPECT_EQ(0, layout.frame_cnt_);
}

TEST_F(TestExprSerializeV4, legacy_nested_record_uses_compact_request_reference_context)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestCompactRef", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(128));
    ExprSerializeArrayGuard array_guard(&exprs);
    serialization::SerCompactEncodingGuard outer_encoding(false);
    serialization::CompactExprPtrGuard outer_decoding(false);
    ObDASPushDownTopN src;
    src.sort_key_ = &exprs.at(127);
    src.limit_expr_ = &exprs.at(0);
    src.offset_expr_ = nullptr;
    const int64_t legacy_size = src.get_serialize_size();
    std::vector<char> legacy(legacy_size);
    int64_t legacy_pos = 0;
    ASSERT_EQ(OB_SUCCESS, src.serialize(legacy.data(), legacy.size(), legacy_pos));
    std::vector<char> compact(legacy_size);
    int64_t compact_pos = 0;
    {
      serialization::SerCompactEncodingGuard request_guard(true);
      ASSERT_EQ(legacy_size - 8, src.get_serialize_size());
      ASSERT_EQ(OB_SUCCESS, src.serialize(compact.data(), compact.size(), compact_pos));
      ASSERT_EQ(src.get_serialize_size(), compact_pos);
      ASSERT_TRUE(serialization::use_compact_expr_ptr());
    }
    ASSERT_FALSE(serialization::using_v4_encoding());
    ASSERT_EQ(legacy[0], compact[0]);
    ObDASPushDownTopN decoded;
    int64_t pos = 0;
    {
      serialization::CompactExprPtrGuard request_guard(true);
      ASSERT_EQ(OB_SUCCESS, decoded.deserialize(compact.data(), compact_pos, pos));
      ASSERT_FALSE(serialization::using_v4_encoding());
    }
    ASSERT_EQ(compact_pos, pos);
    ASSERT_EQ(src.sort_key_, decoded.sort_key_);
    ASSERT_EQ(src.limit_expr_, decoded.limit_expr_);
    ASSERT_EQ(nullptr, decoded.offset_expr_);
    ASSERT_FALSE(serialization::use_compact_expr_ptr());
    ObDASPushDownTopN wrong_context;
    pos = 0;
    ASSERT_NE(OB_SUCCESS, wrong_context.deserialize(compact.data(), compact_pos, pos));
    ObDASPushDownTopN legacy_decoded;
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, legacy_decoded.deserialize(legacy.data(), legacy_pos, pos));
    ASSERT_EQ(legacy_pos, pos);
    ASSERT_EQ(src.sort_key_, legacy_decoded.sort_key_);
  }
}

TEST_F(TestExprSerializeV4, datum_meta_v4_standard_envelope_and_fallback)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    const ObDatumMeta cases[] = {ObDatumMeta(ObInt32Type, CS_TYPE_BINARY, 0, 10),
                                 ObDatumMeta(ObDoubleType, CS_TYPE_BINARY, -1, 20),
                                 ObDatumMeta(ObDateType, CS_TYPE_BINARY, 0, 0),
                                 ObDatumMeta(ObNumberType, CS_TYPE_BINARY, 4, 18),
                                 ObDatumMeta(ObDecimalIntType, CS_TYPE_BINARY, 4, 38),
                                 ObDatumMeta(ObDateTimeType, CS_TYPE_BINARY, 6, 0),
                                 ObDatumMeta(ObVarcharType, CS_TYPE_UTF8MB4_GENERAL_CI, 0, 64),
                                 ObDatumMeta(ObBitType, CS_TYPE_BINARY, 0, 16),
                                 ObDatumMeta(ObEnumType, CS_TYPE_UTF8MB4_GENERAL_CI, 0, 0),
                                 ObDatumMeta(ObJsonType, CS_TYPE_UTF8MB4_BIN, -1, -1),
                                 ObDatumMeta(ObLongTextType, CS_TYPE_UTF8MB4_BIN, 0, -1),
                                 ObDatumMeta(ObExtendType, CS_TYPE_INVALID, 0, -1),
                                 ObDatumMeta(ObNullType, CS_TYPE_INVALID, -1, -1),
                                 ObDatumMeta(ObRoaringBitmapType, CS_TYPE_BINARY, -1, -1)};
    for (int64_t i = 0; i < ARRAYSIZEOF(cases); ++i) {
      const ObDatumMeta &src = cases[i];
      char legacy_direct[128];
      char legacy_dispatch[128];
      int64_t legacy_direct_pos = 0;
      int64_t legacy_dispatch_pos = 0;
      ASSERT_EQ(OB_SUCCESS, src.serialize(legacy_direct, sizeof(legacy_direct), legacy_direct_pos));
      {
        serialization::SerCompactEncodingGuard legacy_guard(false);
        ASSERT_EQ(OB_SUCCESS, encode_v4(&src, legacy_dispatch, sizeof(legacy_dispatch), legacy_dispatch_pos));
        ASSERT_EQ(legacy_direct_pos, get_serialize_size_v4(&src));
      }
      ASSERT_EQ(legacy_direct_pos, legacy_dispatch_pos) << i;
      ASSERT_EQ(0, MEMCMP(legacy_direct, legacy_dispatch, legacy_direct_pos)) << i;

      // Compact requests require compact DatumMeta records, including inside Expr.
      ObDatumMeta upgrade_dst;
      ObDatumMetaCompact upgrade_record;
      int64_t upgrade_pos = 0;
      {
        serialization::CompactExprPtrGuard compact_guard(true);
        ASSERT_EQ(OB_NOT_SUPPORTED,
                  decode_test_record(&upgrade_dst, &upgrade_record, legacy_direct, legacy_direct_pos, upgrade_pos))
          << i;
      }
      ASSERT_EQ(legacy_direct_pos, upgrade_pos) << i;
      ASSERT_EQ(src.type_, upgrade_dst.type_) << i;

      char compact[128];
      int64_t compact_pos = 0;
      int64_t compact_size = 0;
      {
        serialization::SerCompactEncodingGuard compact_guard(true);
        compact_size = get_serialize_size_v4(&src);
        ASSERT_GT(compact_size, 0) << i;
        ASSERT_EQ(OB_SUCCESS, encode_v4(&src, compact, compact_size, compact_pos)) << i;
        ASSERT_EQ(compact_size, compact_pos) << i;
        char short_buf[128];
        int64_t short_pos = 0;
        ASSERT_NE(OB_SUCCESS, encode_v4(&src, short_buf, compact_size - 1, short_pos)) << i;
        ASSERT_LE(short_pos, compact_size - 1) << i;
      }
      int64_t version_pos = 0;
      int64_t version = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(compact, compact_pos, version_pos, &version));
      ASSERT_EQ(serialization::SER_COMPACT_OPT_VERSION, version) << i;

      ObDatumMeta dst;
      ObDatumMetaCompact compact_record;
      int64_t decode_pos = 0;
      {
        serialization::CompactExprPtrGuard compact_guard(true);
        ASSERT_EQ(OB_SUCCESS, decode_test_record(&dst, &compact_record, compact, compact_pos, decode_pos)) << i;
      }
      ASSERT_EQ(compact_pos, decode_pos) << i;
      ASSERT_EQ(src.type_, dst.type_) << i;
      ASSERT_EQ(src.cs_type_, dst.cs_type_) << i;
      ASSERT_EQ(src.scale_, dst.scale_) << i;
      ASSERT_EQ(src.precision_, dst.precision_) << i;
      ObDatumMetaReplayCtx replay_ctx;
      ASSERT_EQ(OB_SUCCESS, compact_record.replay_to(dst, replay_ctx)) << i;

      ObDatumMeta truncated_dst;
      ObDatumMetaCompact truncated_record;
      int64_t truncated_pos = 0;
      {
        serialization::CompactExprPtrGuard compact_guard(true);
        ASSERT_NE(OB_SUCCESS,
                  decode_test_record(&truncated_dst, &truncated_record, compact, compact_pos - 1, truncated_pos))
          << i;
      }
      ASSERT_LE(truncated_pos, compact_pos - 1) << i;

      char bad_version[128];
      MEMCPY(bad_version, compact, compact_pos);
      bad_version[0] = 3;
      ObDatumMeta bad_version_dst;
      ObDatumMetaCompact bad_version_record;
      int64_t bad_version_pos = 0;
      {
        serialization::CompactExprPtrGuard compact_guard(true);
        ASSERT_EQ(OB_NOT_SUPPORTED,
                  decode_test_record(&bad_version_dst, &bad_version_record, bad_version, compact_pos, bad_version_pos))
          << i;
      }
      ASSERT_LE(bad_version_pos, compact_pos) << i;
    }
  }
}

TEST_F(TestExprSerializeV4, generic_expr_frame_info_stays_legacy)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ClusterVersionGuard version_guard(CLUSTER_CURRENT_VERSION);
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "ExprFrameV4"));
    ObExprFrameInfo src(allocator);
    ObExprFrameInfo dst(allocator);
    ASSERT_EQ(OB_SUCCESS, src.rt_exprs_.prepare_allocate(1));
    build_expr(src.rt_exprs_.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
    src.rt_exprs_.at(0).datum_off_ = 31;
    src.rt_exprs_.at(0).eval_info_off_ = 47;

    char buf[4096];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, src.serialize(buf, sizeof(buf), pos));
    ASSERT_EQ(pos, src.get_serialize_size());
    int64_t dpos = 0;
    ASSERT_EQ(OB_SUCCESS, dst.deserialize(buf, pos, dpos));
    ASSERT_EQ(pos, dpos);
    ASSERT_EQ(1, dst.rt_exprs_.count());
    ASSERT_EQ(src.rt_exprs_.at(0).datum_off_, dst.rt_exprs_.at(0).datum_off_);
    ASSERT_EQ(src.rt_exprs_.at(0).eval_info_off_, dst.rt_exprs_.at(0).eval_info_off_);
  }
}

TEST_F(TestExprSerializeV4, v4_full_record_guards)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> src_exprs;
    ObArray<ObExpr> dst_exprs;
    ASSERT_EQ(OB_SUCCESS, src_exprs.prepare_allocate(1));
    ASSERT_EQ(OB_SUCCESS, dst_exprs.prepare_allocate(1));
    build_expr(src_exprs.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
    ExprSerializeArrayGuard array_guard(&src_exprs);

    char valid[4096];
    int64_t valid_len = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(src_exprs.at(0), valid, sizeof(valid), valid_len, false));
    static const unsigned char STANDARD_UNIS_V4_DATUM_META_GOLDEN[] = {0x04, 0x92, 0x80, 0x80, 0x80, 0x00, 0x59, 0x04,
                                                                       0x82, 0x80, 0x80, 0x80, 0x00, 0x04, 0x0a, 0x42,
                                                                       0x00, 0x07, 0x04, 0x01, 0x3f, 0x00, 0x04, 0x08};
    ASSERT_EQ(static_cast<int64_t>(sizeof(STANDARD_UNIS_V4_DATUM_META_GOLDEN)), valid_len);
    ASSERT_EQ(0, MEMCMP(STANDARD_UNIS_V4_DATUM_META_GOLDEN, valid, valid_len));
    int64_t envelope_pos = 0;
    int64_t version = 0;
    int64_t body_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(valid, valid_len, envelope_pos, &version));
    ASSERT_EQ(serialization::SER_COMPACT_OPT_VERSION, version);
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(valid, valid_len, envelope_pos, &body_len));
    const int64_t body_begin = envelope_pos;
    ASSERT_EQ(valid_len, body_begin + body_len);

    array_guard.set(&dst_exprs);
    int64_t dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(dst_exprs.at(0), valid, valid_len, dpos));
    ASSERT_EQ(valid_len, dpos);

    char legacy_meta_expr[256];
    int64_t legacy_meta_pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(legacy_meta_expr, sizeof(legacy_meta_expr), legacy_meta_pos,
                                                     ObExprCompactHeader::unis_declared_version()));
    const int64_t length_pos = legacy_meta_pos;
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_fixed_bytes_i64(legacy_meta_expr, sizeof(legacy_meta_expr), legacy_meta_pos, 0));
    const int64_t meta_body_begin = legacy_meta_pos;
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_vi32(legacy_meta_expr, sizeof(legacy_meta_expr), legacy_meta_pos, T_REF_COLUMN));
    ASSERT_EQ(OB_SUCCESS,
              src_exprs.at(0).datum_meta_.serialize(legacy_meta_expr, sizeof(legacy_meta_expr), legacy_meta_pos));
    int64_t length_offset = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(legacy_meta_expr + length_pos,
                                                                serialization::OB_SERIALIZE_SIZE_NEED_BYTES,
                                                                length_offset, legacy_meta_pos - meta_body_begin));
    ObExpr legacy_meta_dst;
    dpos = 0;
    ASSERT_EQ(OB_NOT_SUPPORTED, decode_test_compact_expr(legacy_meta_dst, legacy_meta_expr, legacy_meta_pos, dpos));
    ASSERT_EQ(legacy_meta_pos, dpos);

    char with_next[4097];
    MEMCPY(with_next, valid, valid_len);
    with_next[valid_len] = static_cast<char>(0x5a);
    ObExpr next_dst;
    dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(next_dst, with_next, valid_len + 1, dpos));
    ASSERT_EQ(valid_len, dpos);
    ASSERT_EQ(static_cast<char>(0x5a), with_next[dpos]);

    char malformed[4096];
    int64_t malformed_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(malformed, sizeof(malformed), malformed_len,
                                                     ObExprCompactHeader::unis_declared_version() + 1));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(malformed, sizeof(malformed), malformed_len, body_len));
    MEMCPY(malformed + malformed_len, valid + body_begin, valid_len - body_begin);
    malformed_len += valid_len - body_begin;
    ObExpr malformed_dst;
    dpos = 0;
    ASSERT_EQ(OB_NOT_SUPPORTED, decode_test_compact_expr(malformed_dst, malformed, malformed_len, dpos));
    ASSERT_LE(dpos, malformed_len);

    dpos = 0;
    ASSERT_NE(OB_SUCCESS, decode_test_compact_expr(malformed_dst, valid, 0, dpos));
    ASSERT_LE(dpos, 0);

    malformed_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(malformed, sizeof(malformed), malformed_len,
                                                     serialization::SER_COMPACT_OPT_VERSION));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(malformed, sizeof(malformed), malformed_len, -1));
    dpos = 0;
    ASSERT_EQ(OB_DESERIALIZE_ERROR, decode_test_compact_expr(malformed_dst, malformed, malformed_len, dpos));
    ASSERT_LE(dpos, malformed_len);

    malformed_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(malformed, sizeof(malformed), malformed_len,
                                                     serialization::SER_COMPACT_OPT_VERSION));
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_fixed_bytes_i64(malformed, sizeof(malformed), malformed_len, body_len + 1));
    MEMCPY(malformed + malformed_len, valid + body_begin, body_len);
    malformed_len += body_len;
    dpos = 0;
    ASSERT_EQ(OB_DESERIALIZE_ERROR, decode_test_compact_expr(malformed_dst, malformed, malformed_len, dpos));
    ASSERT_LE(dpos, malformed_len);

    malformed_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(malformed, sizeof(malformed), malformed_len,
                                                     serialization::SER_COMPACT_OPT_VERSION));
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_fixed_bytes_i64(malformed, sizeof(malformed), malformed_len, body_len - 1));
    MEMCPY(malformed + malformed_len, valid + body_begin, body_len - 1);
    malformed_len += body_len - 1;
    ObExpr missing_tail_dst;
    dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(missing_tail_dst, malformed, malformed_len, dpos));
    ASSERT_EQ(malformed_len, dpos);
    ASSERT_EQ(src_exprs.at(0).type_, missing_tail_dst.type_);
    ASSERT_EQ(src_exprs.at(0).max_length_, missing_tail_dst.max_length_);
    ASSERT_EQ(0, missing_tail_dst.res_buf_len_);

    malformed_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(malformed, sizeof(malformed), malformed_len,
                                                     serialization::SER_COMPACT_OPT_VERSION));
    ASSERT_EQ(OB_SUCCESS,
              serialization::encode_fixed_bytes_i64(malformed, sizeof(malformed), malformed_len, body_len + 1));
    MEMCPY(malformed + malformed_len, valid + body_begin, body_len);
    malformed_len += body_len;
    malformed[malformed_len++] = 0;
    dpos = 0;
    ASSERT_EQ(OB_DESERIALIZE_ERROR, decode_test_compact_expr(malformed_dst, malformed, malformed_len, dpos));
    ASSERT_EQ(malformed_len, dpos);
  }
}

TEST_F(TestExprSerializeV4, v4_pointer_arrays_publish_only_after_success)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> src_exprs;
    ObArray<ObExpr> dst_exprs;
    ASSERT_EQ(OB_SUCCESS, src_exprs.prepare_allocate(4));
    ASSERT_EQ(OB_SUCCESS, dst_exprs.prepare_allocate(4));
    for (int64_t i = 0; i < src_exprs.count(); ++i) {
      build_expr(src_exprs.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
      src_exprs.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
      src_exprs.at(i).max_length_ = static_cast<int32_t>(src_exprs.at(i).res_buf_len_);
    }
    ObExpr &src = src_exprs.at(0);
    src.arg_cnt_ = 2;
    src.args_ = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *) * src.arg_cnt_));
    src.args_[0] = &src_exprs.at(1);
    src.args_[1] = nullptr;
    src.parent_cnt_ = 1;
    src.parents_ = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *)));
    src.parents_[0] = &src_exprs.at(2);
    src.attrs_cnt_ = 1;
    src.attrs_ = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *)));
    src.attrs_[0] = &src_exprs.at(3);
    src.inner_func_cnt_ = 1;
    src.inner_functions_ = static_cast<void **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(void *)));
    src.inner_functions_[0] = reinterpret_cast<void *>(expr_default_eval_vector_func);
    ASSERT_NE(nullptr, src.args_);
    ASSERT_NE(nullptr, src.parents_);
    ASSERT_NE(nullptr, src.attrs_);
    ASSERT_NE(nullptr, src.inner_functions_);

    ExprSerializeArrayGuard array_guard(&src_exprs);
    char buf[4096];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(src, buf, sizeof(buf), pos, false));
    array_guard.set(&dst_exprs);
    int64_t dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(dst_exprs.at(0), buf, pos, dpos));
    ASSERT_EQ(pos, dpos);
    ASSERT_EQ(2u, dst_exprs.at(0).arg_cnt_);
    ASSERT_EQ(&dst_exprs.at(1), dst_exprs.at(0).args_[0]);
    ASSERT_EQ(nullptr, dst_exprs.at(0).args_[1]);
    ASSERT_EQ(1u, dst_exprs.at(0).parent_cnt_);
    ASSERT_EQ(&dst_exprs.at(2), dst_exprs.at(0).parents_[0]);
    ASSERT_EQ(1u, dst_exprs.at(0).attrs_cnt_);
    ASSERT_EQ(&dst_exprs.at(3), dst_exprs.at(0).attrs_[0]);
    ASSERT_EQ(1u, dst_exprs.at(0).inner_func_cnt_);
    ASSERT_EQ(src.inner_functions_[0], dst_exprs.at(0).inner_functions_[0]);

    const uint16_t ref_bits[] = {TEST_V4_HAS_ARGS, TEST_V4_HAS_PARENTS, TEST_V4_HAS_ATTRS};
    for (int64_t kind = 0; kind < ARRAYSIZEOF(ref_bits); ++kind) {
      ObArray<ObExpr> one_kind_src;
      ObArray<ObExpr> one_kind_dst;
      ASSERT_EQ(OB_SUCCESS, one_kind_src.prepare_allocate(2));
      ASSERT_EQ(OB_SUCCESS, one_kind_dst.prepare_allocate(2));
      build_expr(one_kind_src.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
      one_kind_src.at(0).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
      one_kind_src.at(0).max_length_ = static_cast<int32_t>(one_kind_src.at(0).res_buf_len_);
      ObExpr **refs = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *)));
      ASSERT_NE(nullptr, refs);
      refs[0] = &one_kind_src.at(1);
      if (TEST_V4_HAS_ARGS == ref_bits[kind]) {
        one_kind_src.at(0).args_ = refs;
        one_kind_src.at(0).arg_cnt_ = 1;
      } else if (TEST_V4_HAS_PARENTS == ref_bits[kind]) {
        one_kind_src.at(0).parents_ = refs;
        one_kind_src.at(0).parent_cnt_ = 1;
      } else {
        one_kind_src.at(0).attrs_ = refs;
        one_kind_src.at(0).attrs_cnt_ = 1;
      }
      array_guard.set(&one_kind_src);
      char corrupt[1024];
      int64_t corrupt_len = 0;
      ASSERT_EQ(OB_SUCCESS, encode_expr_v4(one_kind_src.at(0), corrupt, sizeof(corrupt), corrupt_len, false));
      int64_t item_pos = 0;
      int64_t body_end = 0;
      int64_t bitmap = 0;
      ASSERT_EQ(OB_SUCCESS, decode_v4_test_prefix(corrupt, corrupt_len, item_pos, body_end, bitmap));
      ASSERT_EQ(ref_bits[kind], bitmap);
      int32_t count = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::decode_vi32(corrupt, body_end, item_pos, &count));
      ASSERT_EQ(1, count);
      char bad_ref[8];
      int64_t bad_ref_len = 0;
      ASSERT_EQ(OB_SUCCESS, serialization::encode_vi32(bad_ref, sizeof(bad_ref), bad_ref_len, 127));
      ASSERT_EQ(1, bad_ref_len);
      corrupt[item_pos] = bad_ref[0];

      ObExpr *sentinel_refs[1] = {&one_kind_dst.at(0)};
      ObExpr &failed = one_kind_dst.at(0);
      if (TEST_V4_HAS_ARGS == ref_bits[kind]) {
        failed.args_ = sentinel_refs;
        failed.arg_cnt_ = 1;
      } else if (TEST_V4_HAS_PARENTS == ref_bits[kind]) {
        failed.parents_ = sentinel_refs;
        failed.parent_cnt_ = 1;
      } else {
        failed.attrs_ = sentinel_refs;
        failed.attrs_cnt_ = 1;
      }
      array_guard.set(&one_kind_dst);
      int64_t failed_pos = 0;
      ASSERT_NE(OB_SUCCESS, decode_test_compact_expr(failed, corrupt, corrupt_len, failed_pos));
      if (TEST_V4_HAS_ARGS == ref_bits[kind]) {
        ASSERT_EQ(sentinel_refs, failed.args_);
        ASSERT_EQ(1u, failed.arg_cnt_);
      } else if (TEST_V4_HAS_PARENTS == ref_bits[kind]) {
        ASSERT_EQ(sentinel_refs, failed.parents_);
        ASSERT_EQ(1u, failed.parent_cnt_);
      } else {
        ASSERT_EQ(sentinel_refs, failed.attrs_);
        ASSERT_EQ(1u, failed.attrs_cnt_);
      }
    }

    ObArray<ObExpr> inner_src;
    ObArray<ObExpr> inner_dst;
    ASSERT_EQ(OB_SUCCESS, inner_src.prepare_allocate(1));
    ASSERT_EQ(OB_SUCCESS, inner_dst.prepare_allocate(1));
    build_expr(inner_src.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
    inner_src.at(0).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
    inner_src.at(0).max_length_ = static_cast<int32_t>(inner_src.at(0).res_buf_len_);
    inner_src.at(0).inner_func_cnt_ = 1;
    inner_src.at(0).inner_functions_ =
      static_cast<void **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(void *)));
    inner_src.at(0).inner_functions_[0] = reinterpret_cast<void *>(expr_default_eval_vector_func);
    array_guard.set(&inner_src);
    char inner_valid[1024];
    int64_t inner_valid_len = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(inner_src.at(0), inner_valid, sizeof(inner_valid), inner_valid_len, false));
    int64_t inner_item_pos = 0;
    int64_t inner_body_end = 0;
    int64_t inner_bitmap = 0;
    ASSERT_EQ(OB_SUCCESS,
              decode_v4_test_prefix(inner_valid, inner_valid_len, inner_item_pos, inner_body_end, inner_bitmap));
    ASSERT_EQ(TEST_V4_HAS_INNER_FUNCS, inner_bitmap);
    int32_t inner_count = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi32(inner_valid, inner_body_end, inner_item_pos, &inner_count));
    ASSERT_EQ(1, inner_count);

    int64_t original_envelope_pos = 0;
    int64_t original_version = 0;
    int64_t original_body_len = 0;
    ASSERT_EQ(OB_SUCCESS,
              serialization::decode_vi64(inner_valid, inner_valid_len, original_envelope_pos, &original_version));
    ASSERT_EQ(OB_SUCCESS,
              serialization::decode_vi64(inner_valid, inner_valid_len, original_envelope_pos, &original_body_len));
    char bad_func[16];
    int64_t bad_func_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(bad_func, sizeof(bad_func), bad_func_len, INT64_MAX));
    const int64_t prefix_len = inner_item_pos - original_envelope_pos;
    char inner_corrupt[1024];
    int64_t inner_corrupt_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode_vi64(inner_corrupt, sizeof(inner_corrupt), inner_corrupt_len,
                                                     serialization::SER_COMPACT_OPT_VERSION));
    ASSERT_EQ(OB_SUCCESS, serialization::encode_fixed_bytes_i64(inner_corrupt, sizeof(inner_corrupt), inner_corrupt_len,
                                                                prefix_len + bad_func_len));
    MEMCPY(inner_corrupt + inner_corrupt_len, inner_valid + original_envelope_pos, prefix_len);
    inner_corrupt_len += prefix_len;
    MEMCPY(inner_corrupt + inner_corrupt_len, bad_func, bad_func_len);
    inner_corrupt_len += bad_func_len;

    void *sentinel_funcs[1] = {reinterpret_cast<void *>(expr_default_eval_batch_func)};
    inner_dst.at(0).inner_functions_ = sentinel_funcs;
    inner_dst.at(0).inner_func_cnt_ = 1;
    array_guard.set(&inner_dst);
    int64_t inner_failed_pos = 0;
    ASSERT_NE(OB_SUCCESS,
              decode_test_compact_expr(inner_dst.at(0), inner_corrupt, inner_corrupt_len, inner_failed_pos));
    ASSERT_EQ(sentinel_funcs, inner_dst.at(0).inner_functions_);
    ASSERT_EQ(1u, inner_dst.at(0).inner_func_cnt_);
  }
}

TEST_F(TestExprSerializeV4, compact_row_hole_keeps_frozen_request_decision)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestRowHole", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    const uint64_t tenant_id = 1001;
    share::ObTenantBase tenant_base(tenant_id);
    share::ObTenantSwitchGuard tenant_guard(&tenant_base);
    const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id);
    ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
    TenantCompactOptGuard enabled(tenant_id, true);
    ASSERT_TRUE(enabled.is_valid());

    ObArenaAllocator allocator;
    ObExecContext exec_ctx(allocator);
    ObPhysicalPlanCtx plan_ctx(allocator);
    ObPhysicalPlan plan;
    plan.set_min_cluster_version(DAS_REMOTE_INFO_V4_MIN_VERSION);
    plan.set_serialize_compact_opt_flags(ObPhysicalPlan::SERIALIZE_COMPACT_OPT);
    plan_ctx.set_phy_plan(&plan);
    exec_ctx.set_physical_plan_ctx(&plan_ctx);
    ObExprFrameInfo frame_info(allocator);
    ASSERT_EQ(OB_SUCCESS, frame_info.rt_exprs_.prepare_allocate(1));

    // CG leaves ROW datum metadata at its default, including no collation.
    ObExpr row;
    row.type_ = T_OP_ROW;
    char buf[512];
    int64_t size = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(row, buf, sizeof(buf), size, true));
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(frame_info.rt_exprs_.at(0), buf, size, pos));
    ASSERT_EQ(size, pos);
    ASSERT_EQ(T_INVALID, frame_info.rt_exprs_.at(0).type_);
    ASSERT_EQ(CS_TYPE_INVALID, frame_info.rt_exprs_.at(0).datum_meta_.cs_type_);
    ASSERT_NE(nullptr, frame_info.rt_exprs_.at(0).basic_funcs_);

    ObDataAccessService service;
    ObArray<ObIDASTaskOp *> tasks;
    ObDASRemoteInfo remote_info;
    remote_info.exec_ctx_ = &exec_ctx;
    remote_info.frame_info_ = &frame_info;
    remote_info.has_expr_ = true;
    {
      TenantCompactOptGuard disabled(tenant_id, false);
      ASSERT_TRUE(disabled.is_valid());
      ASSERT_EQ(OB_SUCCESS, service.collect_das_task_info(tasks, remote_info));
      EXPECT_EQ(SerCompactState::V4, remote_info.ser_compact_state_);
    }
    plan.set_serialize_compact_opt_flags(0);
    ASSERT_EQ(OB_SUCCESS, service.collect_das_task_info(tasks, remote_info));
    EXPECT_EQ(SerCompactState::LEGACY, remote_info.ser_compact_state_);
    // Layout validity is established by the receiving PX plan state.
    // Collection only validates the frame pointer and selection bounds.
    remote_info.frame_info_ = &frame_info;
    ASSERT_EQ(OB_SUCCESS, frame_info.ser_expr_marks_.init(2));
    ASSERT_EQ(OB_SUCCESS, frame_info.ser_expr_marks_.prepare_allocate(2));
    EXPECT_EQ(OB_ERR_UNEXPECTED, service.collect_das_task_info(tasks, remote_info));
    remote_info.frame_info_ = nullptr;
    EXPECT_EQ(OB_ERR_UNEXPECTED, service.collect_das_task_info(tasks, remote_info));
  }
}

TEST_F(TestExprSerializeV4, cg_partial_trims_compact_frames_and_roundtrips)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "CgPartialV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    for (const int64_t batch_size : {0, 8}) {
      for (const bool rich : {false, true}) {
        for (const int selected_mask : {0, 1, 2, 3}) {
          ObArenaAllocator allocator;
          ObPhysicalPlan plan;
          plan.set_batch_size(batch_size);
          plan.set_use_rich_format(rich);
          plan.set_min_cluster_version(DAS_REMOTE_INFO_V4_MIN_VERSION);
          ObExprFrameInfo &global = plan.get_expr_frame_info();
          ASSERT_EQ(OB_SUCCESS, global.rt_exprs_.prepare_allocate(4));
          for (int64_t i = 0; i < 4; ++i) {
            build_expr(global.rt_exprs_.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, i < 2 ? 0 : 1,
                       i >= 2 && batch_size > 0);
          }
          const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + (rich ? sizeof(VectorHeader) : 0);
          const int64_t reserved = global.rt_exprs_.at(0).res_buf_len_;
          ObFrameInfo frame;
          frame.frame_idx_ = 0;
          frame.expr_cnt_ = 2;
          frame.frame_size_ = 2 * (item + reserved);
          frame.use_rich_format_ = rich;
          ASSERT_EQ(OB_SUCCESS, global.dynamic_frame_.init(1));
          ASSERT_EQ(OB_SUCCESS, global.dynamic_frame_.push_back(frame));
          frame.frame_idx_ = 1;
          if (batch_size > 0) {
            const int64_t bitmap = ObBitVector::memory_size(batch_size);
            frame.frame_size_ = 2
                                * (sizeof(ObDatum) * batch_size + bitmap + sizeof(ObEvalInfo) + bitmap
                                   + reserved * batch_size + (rich ? sizeof(VectorHeader) + bitmap : 0));
          }
          ASSERT_EQ(OB_SUCCESS, global.datum_frame_.init(1));
          ASSERT_EQ(OB_SUCCESS, global.datum_frame_.push_back(frame));
          ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(global.rt_exprs_, global.const_frame_, global.param_frame_,
                                                         global.dynamic_frame_, global.datum_frame_, batch_size, 0));
          ObColumnRefRawExpr raw[4];
          ObSEArray<ObRawExpr *, 4> selected;
          for (int64_t i = 0; i < 4; ++i) {
            if (selected_mask & (1 << (i % 2))) {
              raw[i].set_rt_expr(&global.rt_exprs_.at(i));
              ASSERT_EQ(OB_SUCCESS, selected.push_back(&raw[i]));
            }
          }
          ObExecContext src_ctx(allocator);
          ObPhysicalPlanCtx src_plan_ctx(allocator);
          src_plan_ctx.set_phy_plan(&plan);
          src_plan_ctx.set_original_param_cnt(0);
          src_ctx.set_physical_plan_ctx(&src_plan_ctx);
          char *frames[2];
          for (int64_t i = 0; i < 2; ++i) {
            const int64_t size =
              i == 0 ? global.dynamic_frame_.at(0).frame_size_ : global.datum_frame_.at(0).frame_size_;
            frames[i] = static_cast<char *>(allocator.alloc(size));
            ASSERT_NE(nullptr, frames[i]);
            MEMSET(frames[i], 0, size);
          }
          src_ctx.set_frames(frames);
          src_ctx.set_frame_cnt(2);
          for (const int64_t flags : {int64_t(0), int64_t(ObSqlRpcOptFlags::COMPACT_EXPR_SERIALIZE)}) {
            const bool compact = flags != 0;
            plan.set_serialize_compact_opt_flags(flags);
            ObExprFrameInfo partial(allocator, global.rt_exprs_);
            ASSERT_EQ(OB_SUCCESS, ObStaticEngineExprCG::generate_partial_expr_frame(plan, partial, selected, rich));
            if (selected_mask < 2) {
              ASSERT_LT(partial.dynamic_frame_.at(0).expr_cnt_, global.dynamic_frame_.at(0).expr_cnt_);
              ASSERT_LT(partial.dynamic_frame_.at(0).frame_size_, global.dynamic_frame_.at(0).frame_size_);
              ASSERT_LT(partial.datum_frame_.at(0).frame_size_, global.datum_frame_.at(0).frame_size_);
            } else {
              ASSERT_EQ(global.dynamic_frame_.at(0).expr_cnt_, partial.dynamic_frame_.at(0).expr_cnt_);
              ASSERT_EQ(global.dynamic_frame_.at(0).frame_size_, partial.dynamic_frame_.at(0).frame_size_);
              ASSERT_EQ(global.datum_frame_.at(0).frame_size_, partial.datum_frame_.at(0).frame_size_);
            }
            if (!compact) { continue; }
            serialization::SerCompactEncodingGuard encoding_guard(true);
            ExprSerializeArrayGuard array_guard(&global.rt_exprs_);
            const int64_t size = ObPxTreeSerializer::get_serialize_expr_frame_info_size<true>(src_ctx, partial);
            std::vector<char> bytes(size);
            int64_t pos = 0;
            ASSERT_EQ(OB_SUCCESS,
                      ObPxTreeSerializer::serialize_expr_frame_info<true>(bytes.data(), size, pos, src_ctx, partial));
            ASSERT_EQ(size, pos);
            ObExecContext dst_ctx(allocator);
            ObPhysicalPlanCtx dst_plan_ctx(allocator);
            ObPhysicalPlan receiving_plan;
            receiving_plan.set_batch_size(batch_size);
            receiving_plan.set_use_rich_format(rich);
            receiving_plan.set_min_cluster_version(DAS_REMOTE_INFO_V4_MIN_VERSION);
            receiving_plan.set_serialize_compact_opt_flags(flags);
            dst_plan_ctx.set_phy_plan(&receiving_plan);
            dst_ctx.set_physical_plan_ctx(&dst_plan_ctx);
            ObExprFrameInfo decoded(allocator);
            SerCompactState state = SerCompactState::UNKNOWN;
            pos = 0;
            ASSERT_EQ(OB_SUCCESS,
                      ObPxTreeSerializer::deserialize_expr_frame_info<true>(
                        bytes.data(), size, pos, dst_ctx, decoded, SerCompactState::V4, &state, &receiving_plan));
            ASSERT_EQ(size, pos);
            ASSERT_EQ(4, decoded.rt_exprs_.count());
            for (int64_t i = 0; i < 4; ++i) {
              ASSERT_EQ(expr_frame_member_selected(partial, i) ? T_REF_COLUMN : T_INVALID,
                        decoded.rt_exprs_.at(i).type_);
              ASSERT_EQ(global.rt_exprs_.at(i).datum_off_, decoded.rt_exprs_.at(i).datum_off_);
              ASSERT_EQ(global.rt_exprs_.at(i).res_buf_off_, decoded.rt_exprs_.at(i).res_buf_off_);
              ASSERT_EQ(global.rt_exprs_.at(i).eval_info_off_, decoded.rt_exprs_.at(i).eval_info_off_);
              if (i >= 2 && batch_size > 0) {
                ASSERT_EQ(global.rt_exprs_.at(i).eval_flags_off_, decoded.rt_exprs_.at(i).eval_flags_off_);
                ASSERT_EQ(global.rt_exprs_.at(i).pvt_skip_off_, decoded.rt_exprs_.at(i).pvt_skip_off_);
              }
            }
            ASSERT_EQ(partial.dynamic_frame_.at(0).frame_size_, decoded.dynamic_frame_.at(0).frame_size_);
            ASSERT_EQ(partial.datum_frame_.at(0).frame_size_, decoded.datum_frame_.at(0).frame_size_);
            ASSERT_TRUE(decoded.partial_frame_);
            ASSERT_FALSE(decoded.is_mark_serialize());
            ObExprFrameInfo copied(allocator);
            ASSERT_EQ(OB_SUCCESS, copied.assign(decoded, allocator));
            ASSERT_TRUE(copied.partial_frame_);
            ASSERT_FALSE(copied.is_mark_serialize());
            for (const ObExprFrameInfo *forward_source : {&decoded, &copied}) {
              const int64_t forward_size =
                ObPxTreeSerializer::get_serialize_expr_frame_info_size<true>(dst_ctx, *forward_source);
              std::vector<char> forwarded_bytes(forward_size);
              int64_t forward_pos = 0;
              ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_expr_frame_info<true>(
                                      forwarded_bytes.data(), forward_size, forward_pos, dst_ctx, *forward_source));
              ASSERT_EQ(forward_size, forward_pos);
              ObExecContext next_ctx(allocator);
              ObPhysicalPlanCtx next_plan_ctx(allocator);
              ObPhysicalPlan next_plan;
              next_plan_ctx.set_phy_plan(&next_plan);
              next_ctx.set_physical_plan_ctx(&next_plan_ctx);
              ObExprFrameInfo forwarded(allocator);
              forward_pos = 0;
              ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_expr_frame_info<true>(
                                      forwarded_bytes.data(), forward_size, forward_pos, next_ctx, forwarded,
                                      SerCompactState::V4, &state, &next_plan));
              ASSERT_EQ(forward_size, forward_pos);
              ASSERT_TRUE(forwarded.partial_frame_);
              ASSERT_EQ(decoded.dynamic_frame_.at(0).frame_size_, forwarded.dynamic_frame_.at(0).frame_size_);
              ASSERT_EQ(decoded.datum_frame_.at(0).frame_size_, forwarded.datum_frame_.at(0).frame_size_);
              ASSERT_EQ(decoded.rt_exprs_.count(), forwarded.rt_exprs_.count());
              for (int64_t i = 0; i < decoded.rt_exprs_.count(); ++i) {
                const ObExpr &before = decoded.rt_exprs_.at(i);
                const ObExpr &after = forwarded.rt_exprs_.at(i);
                ASSERT_EQ(before.type_, after.type_);
                ASSERT_EQ(before.datum_off_, after.datum_off_);
                ASSERT_EQ(before.res_buf_off_, after.res_buf_off_);
                ASSERT_EQ(before.eval_info_off_, after.eval_info_off_);
                ASSERT_EQ(before.eval_flags_off_, after.eval_flags_off_);
                ASSERT_EQ(before.pvt_skip_off_, after.pvt_skip_off_);
              }
            }
            for (ObFrameInfo *frame : {&decoded.dynamic_frame_.at(0), &decoded.datum_frame_.at(0)}) {
              const ObFrameInfo saved = *frame;
              for (const int delta : {-1, 1}) {
                frame->frame_size_ = saved.frame_size_ + delta;
                ASSERT_EQ(OB_ERR_UNEXPECTED,
                          replay_expr_frame_layout(decoded.rt_exprs_, decoded.const_frame_, decoded.param_frame_,
                                                   decoded.dynamic_frame_, decoded.datum_frame_, batch_size, 0, true));
                *frame = saved;
                frame->expr_cnt_ = saved.expr_cnt_ + delta;
                ASSERT_EQ(OB_ERR_UNEXPECTED,
                          replay_expr_frame_layout(decoded.rt_exprs_, decoded.const_frame_, decoded.param_frame_,
                                                   decoded.dynamic_frame_, decoded.datum_frame_, batch_size, 0, true));
                *frame = saved;
              }
            }
          }
        }
      }
    }
  }
}

TEST_F(TestExprSerializeV4, px_partial_preserves_complete_layout_with_v4_holes)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    const uint64_t tenant_id = 1001;
    share::ObTenantBase tenant_base(tenant_id);
    share::ObTenantSwitchGuard tenant_guard(&tenant_base);
    const int add_ret = omt::ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id);
    ASSERT_TRUE(OB_SUCCESS == add_ret || OB_ENTRY_EXIST == add_ret);
    TenantCompactOptGuard compact_opt_guard(tenant_id, true);
    ASSERT_TRUE(compact_opt_guard.is_valid());

    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "PxPartialV4"));
    ObExecContext exec_ctx(allocator);
    ObPhysicalPlanCtx plan_ctx(allocator);
    ObPhysicalPlan phy_plan;
    phy_plan.set_min_cluster_version(DAS_REMOTE_INFO_V4_MIN_VERSION);
    phy_plan.set_serialize_compact_opt_flags(ObPhysicalPlan::SERIALIZE_COMPACT_OPT);
    plan_ctx.set_phy_plan(&phy_plan);
    exec_ctx.set_physical_plan_ctx(&plan_ctx);

    const int64_t expr_cnt = 6;
    const int64_t mark_cnt = 3;
    ObArray<ObExpr> frame_exprs;
    ASSERT_EQ(OB_SUCCESS, frame_exprs.prepare_allocate(expr_cnt));
    ObExprFrameInfo frame_info(allocator, frame_exprs);
    ASSERT_EQ(OB_SUCCESS, frame_info.ser_expr_marks_.init(mark_cnt));
    ASSERT_EQ(OB_SUCCESS, frame_info.ser_expr_marks_.prepare_allocate(mark_cnt));
    for (int64_t i = 0; i < expr_cnt; ++i) {
      build_expr(frame_info.rt_exprs_.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
      frame_info.rt_exprs_.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
    }
    frame_info.ser_expr_marks_.at(0) = true;
    frame_info.ser_expr_marks_.at(1) = false;
    frame_info.ser_expr_marks_.at(2) = true;

    ObArray<ObExpr> sentinel;
    ASSERT_EQ(OB_SUCCESS, sentinel.prepare_allocate(1));
    ExprSerializeArrayGuard array_guard(&sentinel);
    for (const bool plan_opt : {false, true}) {
      phy_plan.set_serialize_compact_opt_flags(plan_opt ? ObPhysicalPlan::SERIALIZE_COMPACT_OPT : 0);
      for (const bool compact : {false, true}) {
        SCOPED_TRACE(::testing::Message() << "plan_opt=" << plan_opt << " compact=" << compact);
        serialization::SerCompactEncodingGuard encoding_guard(compact);
        const int64_t size = ObPxTreeSerializer::get_serialize_expr_frame_info_size<true>(exec_ctx, frame_info);
        std::vector<char> wire(size);
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_expr_frame_info<true>(
                                  wire.data(), wire.size(), pos, exec_ctx, frame_info));
        ASSERT_EQ(size, pos);
        int64_t record_pos = 0;
        int32_t wire_expr_cnt = 0;
        ASSERT_EQ(OB_SUCCESS, decode_outer_expr_count(wire.data(), wire.size(), record_pos, plan_ctx, wire_expr_cnt));
        ASSERT_EQ(compact ? expr_cnt : mark_cnt, wire_expr_cnt);
        ObArray<ObExpr> decoded;
        ASSERT_EQ(OB_SUCCESS, decoded.prepare_allocate(wire_expr_cnt));
        array_guard.set(&decoded);
        for (int64_t i = 0; i < wire_expr_cnt; ++i) {
          ObExprCompactHeader header(decoded.at(i));
          DEF_UNIS_VERSION_DECODE_REC(record, decoded.at(i), header);
          ASSERT_EQ(OB_SUCCESS, record.deserialize(wire.data(), wire.size(), record_pos));
          ASSERT_TRUE(record.decoded());
          const bool selected = i < mark_cnt && frame_info.ser_expr_marks_.at(i);
          const int64_t expected_version = compact ? ObExprCompactHeader::unis_declared_version() :
            (selected ? ObExpr::unis_declared_version() : ObEmptyExpr::unis_declared_version());
          ASSERT_EQ(expected_version, record.version());
          ASSERT_EQ(selected ? T_REF_COLUMN : T_INVALID, decoded.at(i).type_);
          if (compact && !selected) {
            ASSERT_NE(0, header.bitmap_ & V4_IS_HOLE);
            ASSERT_EQ(frame_exprs.at(i).res_buf_len_, decoded.at(i).res_buf_len_);
          } else if (!selected) {
            ASSERT_EQ(nullptr, decoded.at(i).basic_funcs_);
          }
        }
        array_guard.set(&sentinel);
      }
    }
  }
}

TEST_F(TestExprSerializeV4, px_expr_frame_rejects_version_mismatch_and_mixed_holes)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "FrameVersion", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArenaAllocator allocator;
    ObExecContext src_ctx(allocator);
    ObPhysicalPlanCtx src_plan_ctx(allocator);
    src_ctx.set_physical_plan_ctx(&src_plan_ctx);
    ObExprFrameInfo info(allocator);
    ASSERT_EQ(OB_SUCCESS, info.rt_exprs_.prepare_allocate(2));
    for (int64_t i = 0; i < 2; ++i) {
      build_expr(info.rt_exprs_.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
      info.rt_exprs_.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
    }
    struct VersionCase
    {
      bool first_v4_;
      bool second_v4_;
      SerCompactState expected_;
    };
    const VersionCase cases[] = {
      {false, true, SerCompactState::UNKNOWN},
      {true, false, SerCompactState::UNKNOWN},
      {false, true, SerCompactState::LEGACY},
      {true, false, SerCompactState::LEGACY},
      {false, true, SerCompactState::V4},
      {true, false, SerCompactState::V4},
      {true, true, SerCompactState::LEGACY},
      {false, false, SerCompactState::V4},
    };
    for (int64_t c = 0; c < ARRAYSIZEOF(cases); ++c) {
      SCOPED_TRACE(::testing::Message() << "case=" << c);
      ExprSerializeArrayGuard array_guard(&info.rt_exprs_);
      serialization::SerCompactEncodingGuard legacy_guard(false);
      char bytes[4096];
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, encode_das_expr_frame_head(bytes, sizeof(bytes), pos, src_ctx, info));
      ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(bytes, sizeof(bytes), pos, 2));
      for (int64_t i = 0; i < 2; ++i) {
        if (i == 0 ? cases[c].first_v4_ : cases[c].second_v4_) {
          ASSERT_EQ(OB_SUCCESS, encode_expr_v4(info.rt_exprs_.at(i), bytes, sizeof(bytes), pos, true /*as_hole*/));
        } else {
          ASSERT_EQ(OB_SUCCESS, info.rt_exprs_.at(i).serialize(bytes, sizeof(bytes), pos));
        }
      }
      // The record mismatch must be rejected before attempting a frame tail.
      ObExecContext dst_ctx(allocator);
      ObPhysicalPlanCtx dst_plan_ctx(allocator);
      dst_ctx.set_physical_plan_ctx(&dst_plan_ctx);
      ObExprFrameInfo decoded(allocator);
      SerCompactState state = SerCompactState::UNKNOWN;
      int64_t read_pos = 0;
      ASSERT_EQ(OB_NOT_SUPPORTED, ObPxTreeSerializer::deserialize_expr_frame_info<true>(
                                   bytes, pos, read_pos, dst_ctx, decoded, cases[c].expected_, &state));
      ASSERT_EQ(0, decoded.rt_exprs_.count());
      ASSERT_EQ(SerCompactState::UNKNOWN, state);
    }
  }
}

TEST_F(TestExprSerializeV4, dynamic_param_skips_px_layout_hole_parent)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObExpr source;
    build_expr(source, T_OP_EQ, ObInt32Type, 0, 10, 4, 4, 1, false);
    char wire[256];
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(source, wire, sizeof(wire), pos, true));
    ObExpr hole;
    int64_t dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(hole, wire, pos, dpos));
    ASSERT_EQ(T_INVALID, hole.type_);
    ASSERT_EQ(1u, hole.frame_idx_);
    ASSERT_EQ(0u, hole.eval_info_off_);

    // PX holes retain their frame index but have no executable offsets. A
    // parent invalidation must not clear the first datum in that frame.
    alignas(ObEvalInfo) char frame[3 * sizeof(ObEvalInfo)] = {};
    const uint64_t datum = 1;
    MEMCPY(frame, &datum, sizeof(datum));
    char *frames[] = {nullptr, frame};
    ObExecContext ctx(CURRENT_CONTEXT->get_arena_allocator());
    ctx.set_frames(frames);
    ctx.set_frame_cnt(ARRAYSIZEOF(frames));
    ObEvalCtx eval_ctx(ctx);
    ObExpr parent;
    parent.type_ = T_OP_EQ;
    parent.frame_idx_ = 1;
    parent.eval_info_off_ = sizeof(ObEvalInfo);
    ObExpr exec_param;
    exec_param.type_ = T_QUESTIONMARK;
    exec_param.frame_idx_ = 1;
    exec_param.eval_info_off_ = 2 * sizeof(ObEvalInfo);
    ObExpr *parents[] = {&hole, &parent};
    exec_param.parents_ = parents;
    exec_param.parent_cnt_ = ARRAYSIZEOF(parents);
    parent.get_eval_info(eval_ctx).set_evaluated(true);
    exec_param.get_eval_info(eval_ctx).set_evaluated(true);

    ObDynamicParamSetter::clear_parent_evaluated_flag(eval_ctx, exec_param);

    ASSERT_EQ(0, MEMCMP(frame, &datum, sizeof(datum)));
    ASSERT_FALSE(parent.get_eval_info(eval_ctx).is_evaluated(eval_ctx));
    ASSERT_FALSE(exec_param.get_eval_info(eval_ctx).is_evaluated(eval_ctx));
  }
}

TEST_F(TestExprSerializeV4, px_and_das_plan_compact_decision)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "ExprGateV4"));
    ObExecContext exec_ctx(allocator);
    ObPhysicalPlanCtx plan_ctx(allocator);
    ObPhysicalPlan phy_plan;
    phy_plan.set_batch_size(4);
    plan_ctx.set_phy_plan(&phy_plan);
    plan_ctx.set_original_param_cnt(2);
    exec_ctx.set_physical_plan_ctx(&plan_ctx);

    ObExprFrameInfo dense(allocator);
    ASSERT_EQ(OB_SUCCESS, dense.rt_exprs_.prepare_allocate(8));
    for (int64_t i = 0; i < dense.rt_exprs_.count(); ++i) {
      build_expr(dense.rt_exprs_.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
      dense.rt_exprs_.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
    }

    ObArray<ObExpr> sentinel;
    ASSERT_EQ(OB_SUCCESS, sentinel.prepare_allocate(1));
    ExprSerializeArrayGuard array_guard(&sentinel);
    auto serialize_px = [&](const ObExprFrameInfo &frame, const uint64_t cluster_version, ObPhysicalPlan *plan,
                            const bool logical_compact_enabled, char *buf, const int64_t buf_len,
                            int64_t &expected_size, int64_t &written, int32_t &marker) -> int {
      ClusterVersionGuard version_guard(cluster_version);
      if (nullptr != plan) {
        plan->set_serialize_compact_opt_flags(logical_compact_enabled
            && cluster_version >= DAS_REMOTE_INFO_V4_MIN_VERSION ? ObPhysicalPlan::SERIALIZE_COMPACT_OPT : 0);
      }
      serialization::SerCompactEncodingGuard encoding_guard(
          nullptr != plan && plan->is_serialize_compact_opt_enabled());
      plan_ctx.set_phy_plan(plan);
      ObExprSerializeArrayGuard frame_array_guard(true, &frame);
      expected_size = ObPxTreeSerializer::get_serialize_expr_frame_info_size<true>(exec_ctx, frame);
      written = 0;
      int tmp_ret = ObPxTreeSerializer::serialize_expr_frame_info<true>(buf, buf_len, written, exec_ctx, frame);
      if (OB_SUCCESS == tmp_ret) {
        int64_t parse_pos = 0;
        tmp_ret = decode_outer_expr_count(buf, written, parse_pos, plan_ctx, marker);
      }
      return tmp_ret;
    };

    char px_legacy[64 * 1024];
    char px_current[64 * 1024];
    int64_t px_legacy_size = 0;
    int64_t px_legacy_written = 0;
    int32_t px_legacy_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_px(dense, CLUSTER_VERSION_4_4_2_3, &phy_plan, true, px_legacy, sizeof(px_legacy),
                                       px_legacy_size, px_legacy_written, px_legacy_marker));
    ASSERT_EQ(px_legacy_size, px_legacy_written);
    ASSERT_EQ(dense.rt_exprs_.count(), px_legacy_marker);
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    char px_master_off[64 * 1024];
    int64_t px_master_off_size = 0;
    int64_t px_master_off_written = 0;
    int32_t px_master_off_marker = 0;
    ASSERT_EQ(OB_SUCCESS,
              serialize_px(dense, DAS_REMOTE_INFO_V4_MIN_VERSION, &phy_plan, false, px_master_off, sizeof(px_master_off),
                           px_master_off_size, px_master_off_written, px_master_off_marker));
    ASSERT_EQ(px_master_off_size, px_master_off_written);
    ASSERT_EQ(dense.rt_exprs_.count(), px_master_off_marker);
    ASSERT_EQ(px_legacy_written, px_master_off_written);
    ASSERT_EQ(0, MEMCMP(px_legacy, px_master_off, px_legacy_written));

    int64_t px_current_size = 0;
    int64_t px_current_written = 0;
    int32_t px_current_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_px(dense, DAS_REMOTE_INFO_V4_MIN_VERSION, &phy_plan, true, px_current, sizeof(px_current),
                                       px_current_size, px_current_written, px_current_marker));
    ASSERT_EQ(px_current_size, px_current_written);
    ASSERT_EQ(dense.rt_exprs_.count(), px_current_marker);
    ASSERT_LT(px_current_written, px_legacy_written);
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    int64_t px_no_plan_size = 0;
    int64_t px_no_plan_written = 0;
    int32_t px_no_plan_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_px(dense, DAS_REMOTE_INFO_V4_MIN_VERSION, nullptr, true, px_current, sizeof(px_current),
                                       px_no_plan_size, px_no_plan_written, px_no_plan_marker));
    ASSERT_EQ(dense.rt_exprs_.count(), px_no_plan_marker);
    ASSERT_EQ(px_legacy_written, px_no_plan_written);
    ASSERT_EQ(0, MEMCMP(px_legacy, px_current, px_legacy_written));
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    int64_t px_error_size = 0;
    int64_t px_error_written = 0;
    int32_t px_error_marker = 0;
    ASSERT_NE(OB_SUCCESS, serialize_px(dense, DAS_REMOTE_INFO_V4_MIN_VERSION, &phy_plan, true, px_current, px_current_size - 1,
                                       px_error_size, px_error_written, px_error_marker));
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    ObDASRemoteInfo remote_info;
    remote_info.exec_ctx_ = &exec_ctx;
    remote_info.frame_info_ = &dense;
    remote_info.has_expr_ = true;
    remote_info.use_deser_cache_format_ = true;
    remote_info.ser_compact_state_ = SerCompactState::V4;
    auto serialize_das = [&](const ObExprFrameInfo &frame, const uint64_t cluster_version, ObPhysicalPlan *plan,
                             char *buf, const int64_t buf_len, int64_t &expected_size, int64_t &written,
                             int32_t &marker) -> int {
      ClusterVersionGuard version_guard(cluster_version);
      plan_ctx.set_phy_plan(plan);
      remote_info.frame_info_ = &frame;
      if (nullptr != plan) {
        plan->set_serialize_compact_opt_flags(cluster_version >= DAS_REMOTE_INFO_V4_MIN_VERSION
            ? ObPhysicalPlan::SERIALIZE_COMPACT_OPT : 0);
      }
      const bool use_v4 = OB_NOT_NULL(plan) && plan->is_serialize_compact_opt_enabled();
      serialization::SerCompactEncodingGuard v4_guard(use_v4);
      expected_size = remote_info.get_cacheable_ctdef_expr_serialize_size();
      written = 0;
      int tmp_ret = remote_info.serialize_cacheable_ctdef_expr(buf, buf_len, written);
      if (OB_SUCCESS == tmp_ret) {
        int64_t parse_pos = 0;
        tmp_ret = decode_das_expr_count(buf, written, parse_pos, plan_ctx, marker);
      }
      return tmp_ret;
    };

    char das_legacy[64 * 1024];
    char das_current[64 * 1024];
    int64_t das_legacy_size = 0;
    int64_t das_legacy_written = 0;
    int32_t das_legacy_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_das(dense, CLUSTER_VERSION_4_4_2_3, &phy_plan, das_legacy, sizeof(das_legacy),
                                        das_legacy_size, das_legacy_written, das_legacy_marker));
    ASSERT_EQ(das_legacy_size, das_legacy_written);
    ASSERT_EQ(dense.rt_exprs_.count(), das_legacy_marker);

    int64_t das_current_size = 0;
    int64_t das_current_written = 0;
    int32_t das_current_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_das(dense, DAS_REMOTE_INFO_V4_MIN_VERSION, &phy_plan, das_current, sizeof(das_current),
                                        das_current_size, das_current_written, das_current_marker));
    ASSERT_EQ(das_current_size, das_current_written);
    ASSERT_EQ(dense.rt_exprs_.count(), das_current_marker);
    ASSERT_LT(das_current_written, das_legacy_written);
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    ObArray<ObExpr> sparse_exprs;
    ASSERT_EQ(OB_SUCCESS, sparse_exprs.prepare_allocate(256));
    ObExprFrameInfo sparse(allocator, sparse_exprs);
    ASSERT_EQ(OB_SUCCESS, sparse.ser_expr_marks_.init(sparse_exprs.count()));
    ASSERT_EQ(OB_SUCCESS, sparse.ser_expr_marks_.prepare_allocate(sparse_exprs.count()));
    for (int64_t i = 0; i < sparse_exprs.count(); ++i) {
      build_expr(sparse_exprs.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
      sparse_exprs.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
      sparse.ser_expr_marks_.at(i) = (0 == i);
    }

    int64_t sparse_legacy_size = 0;
    int64_t sparse_legacy_written = 0;
    int32_t sparse_legacy_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_px(sparse, CLUSTER_VERSION_4_4_2_3, &phy_plan, true, px_legacy, sizeof(px_legacy),
                                       sparse_legacy_size, sparse_legacy_written, sparse_legacy_marker));
    int64_t sparse_current_size = 0;
    int64_t sparse_current_written = 0;
    int32_t sparse_current_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_px(sparse, DAS_REMOTE_INFO_V4_MIN_VERSION, &phy_plan, true, px_current, sizeof(px_current),
                                       sparse_current_size, sparse_current_written, sparse_current_marker));
    ASSERT_EQ(sparse_exprs.count(), sparse_current_marker);
    ASSERT_EQ(sparse_current_size, sparse_current_written);
    ASSERT_NE(0, MEMCMP(px_legacy, px_current, std::min(sparse_legacy_written, sparse_current_written)));

    int64_t sparse_das_legacy_size = 0;
    int64_t sparse_das_legacy_written = 0;
    int32_t sparse_das_legacy_marker = 0;
    ASSERT_EQ(OB_SUCCESS, serialize_das(sparse, CLUSTER_VERSION_4_4_2_3, &phy_plan, das_legacy, sizeof(das_legacy),
                                        sparse_das_legacy_size, sparse_das_legacy_written, sparse_das_legacy_marker));
    int64_t sparse_das_current_size = 0;
    int64_t sparse_das_current_written = 0;
    int32_t sparse_das_current_marker = 0;
    ASSERT_EQ(OB_SUCCESS,
              serialize_das(sparse, DAS_REMOTE_INFO_V4_MIN_VERSION, &phy_plan, das_current, sizeof(das_current),
                            sparse_das_current_size, sparse_das_current_written, sparse_das_current_marker));
    ASSERT_EQ(sparse_das_current_size, sparse_das_current_written);
    ASSERT_EQ(sparse_exprs.count(), sparse_das_current_marker);
    ASSERT_NE(0, MEMCMP(das_legacy, das_current, std::min(sparse_das_legacy_written, sparse_das_current_written)));
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
  }
}

TEST_F(TestExprSerializeV4, unified_expr_frame_formats_and_array_lifetime)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "UnifiedFrame", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> sentinel;
    ASSERT_EQ(OB_SUCCESS, sentinel.prepare_allocate(1));
    ExprSerializeArrayGuard outer_array_guard(&sentinel);
    for (int shape = 0; shape < 4; ++shape) {
      ObArenaAllocator allocator;
      ObExecContext src_ctx(allocator);
      ObPhysicalPlanCtx src_plan_ctx(allocator);
      ObPhysicalPlan plan;
      plan.set_batch_size(0);
      plan.set_serialize_compact_opt_flags(ObPhysicalPlan::SERIALIZE_COMPACT_OPT);
      src_plan_ctx.set_phy_plan(&plan);
      src_plan_ctx.set_original_param_cnt(0);
      src_ctx.set_physical_plan_ctx(&src_plan_ctx);
      ObExprFrameInfo info(allocator);
      const int64_t count = shape == 3 ? 0 : 3;
      ASSERT_EQ(OB_SUCCESS, info.rt_exprs_.prepare_allocate(count));
      for (int64_t i = 0; i < count; ++i) {
        build_expr(info.rt_exprs_.at(i), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
        info.rt_exprs_.at(i).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
      }
      if (shape == 1 || shape == 2) {
        ASSERT_EQ(OB_SUCCESS, info.ser_expr_marks_.init(2));
        ASSERT_EQ(OB_SUCCESS, info.ser_expr_marks_.prepare_allocate(2));
        info.ser_expr_marks_.at(0) = shape == 1;
        info.ser_expr_marks_.at(1) = false;
      }
      char *frames[1] = {nullptr};
      if (count > 0) {
        ObFrameInfo frame;
        frame.expr_cnt_ = count;
        frame.frame_idx_ = 0;
        frame.frame_size_ = count * (sizeof(ObDatum) + sizeof(ObEvalInfo));
        for (int64_t i = 0; i < count; ++i) {
          const ObExpr &expr = info.rt_exprs_.at(i);
          frame.frame_size_ += expr.res_buf_len_;
          if (ObDynReserveBuf::supported(expr.datum_meta_.type_) && expr.res_buf_len_ > 0) {
            frame.frame_size_ += sizeof(ObDynReserveBuf);
          }
        }
        frame.use_rich_format_ = false;
        ASSERT_EQ(OB_SUCCESS, info.datum_frame_.init(1));
        ASSERT_EQ(OB_SUCCESS, info.datum_frame_.push_back(frame));
        frames[0] = static_cast<char *>(allocator.alloc(frame.frame_size_));
        ASSERT_NE(nullptr, frames[0]);
        MEMSET(frames[0], 0, frame.frame_size_);
        src_ctx.set_frames(frames);
        src_ctx.set_frame_cnt(1);
        ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(info.rt_exprs_, info.const_frame_, info.param_frame_,
                                                       info.dynamic_frame_, info.datum_frame_, 0, 0));
      }
      auto roundtrip = [&](auto with_plan) {
        constexpr bool PLAN_PART = decltype(with_plan)::value;
        std::vector<char> legacy_frame;
        for (const bool compact : {false, true}) {
          SCOPED_TRACE(::testing::Message() << "shape=" << shape << " plan=" << PLAN_PART << " compact=" << compact);
          ObExprSerializeArrayGuard array_guard(&sentinel);
          plan.set_serialize_compact_opt_flags(compact ? ObPhysicalPlan::SERIALIZE_COMPACT_OPT : 0);
          serialization::SerCompactEncodingGuard encoding_guard(plan.is_serialize_compact_opt_enabled());
          const int64_t size = ObPxTreeSerializer::get_serialize_expr_frame_info_size<PLAN_PART>(src_ctx, info);
          ASSERT_EQ(PLAN_PART ? &info.rt_exprs_ : &sentinel, ObExpr::get_serialize_array());
          std::vector<char> bytes(size + sizeof(int32_t));
          int64_t pos = 0;
          ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_expr_frame_info<PLAN_PART>(bytes.data(), bytes.size(),
                                                                                         pos, src_ctx, info));
          ASSERT_EQ(size, pos);
          ASSERT_EQ(PLAN_PART ? &info.rt_exprs_ : &sentinel, ObExpr::get_serialize_array());
          if (!PLAN_PART) {
            if (!compact) {
              legacy_frame.assign(bytes.begin(), bytes.begin() + size);
            } else {
              ASSERT_EQ(legacy_frame.size(), size);
              ASSERT_EQ(0, MEMCMP(legacy_frame.data(), bytes.data(), size));
            }
          } else if (compact) {
            std::vector<char> previous(size);
            int64_t previous_pos = 0;
            ASSERT_EQ(OB_SUCCESS, encode_das_expr_frame_head(previous.data(), size, previous_pos, src_ctx, info));
            ASSERT_EQ(OB_SUCCESS, encode_das_exprs(previous.data(), size, previous_pos, info));
            ASSERT_EQ(OB_SUCCESS,
                      encode_expr_frame_tail(previous.data(), size, previous_pos, src_ctx, info, count > 0));
            ASSERT_EQ(size, previous_pos);
            ASSERT_EQ(0, MEMCMP(previous.data(), bytes.data(), size));
          }
          if (PLAN_PART && count > 0) {
            ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes.data(), bytes.size(), pos, &info.rt_exprs_.at(0)));
          }
          const int64_t total = pos;
          ObExecContext dst_ctx(allocator);
          ObPhysicalPlanCtx dst_plan_ctx(allocator);
          ObPhysicalPlan receiving_plan;
          receiving_plan.set_batch_size(0);
          receiving_plan.set_serialize_compact_opt_flags(ObPhysicalPlan::SERIALIZE_COMPACT_OPT);
          dst_plan_ctx.set_phy_plan(&receiving_plan);
          dst_ctx.set_physical_plan_ctx(&dst_plan_ctx);
          ObExprFrameInfo decoded(allocator);
          ObExprFrameInfo &destination = PLAN_PART ? decoded : info;
          SerCompactState state = SerCompactState::UNKNOWN;
          pos = 0;
          ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_expr_frame_info<PLAN_PART>(
                                  bytes.data(), total, pos, dst_ctx, destination,
                                  SerCompactState::UNKNOWN, &state, &receiving_plan));
          ASSERT_EQ(size, pos);
          ASSERT_EQ(PLAN_PART ? &decoded.rt_exprs_ : &sentinel, ObExpr::get_serialize_array());
          ASSERT_EQ(count > 0 ? 1 : 0, dst_ctx.get_frame_cnt());
          const int64_t decoded_count = PLAN_PART ? serialized_expr_count(info, compact) : count;
          ASSERT_EQ(decoded_count, destination.rt_exprs_.count());
          ASSERT_EQ(!PLAN_PART || count == 0 || compact, receiving_plan.is_serialize_compact_opt_enabled());
          if (PLAN_PART && count > 0) {
            ASSERT_EQ(compact ? SerCompactState::V4 : SerCompactState::LEGACY, state);
            for (int64_t i = 0; i < decoded_count; ++i) {
              const bool selected = expr_frame_member_selected(info, i);
              ASSERT_EQ(selected ? T_REF_COLUMN : T_INVALID, decoded.rt_exprs_.at(i).type_);
              if (compact || selected) {
                ASSERT_EQ(info.rt_exprs_.at(i).datum_off_, decoded.rt_exprs_.at(i).datum_off_);
              }
            }
            serialization::CompactExprPtrGuard ptr_guard(compact);
            ObExpr *reference = nullptr;
            ASSERT_EQ(OB_SUCCESS, serialization::decode(bytes.data(), total, pos, reference));
            ASSERT_EQ(&decoded.rt_exprs_.at(0), reference);
            ASSERT_EQ(total, pos);
            ObDataAccessService service;
            ObArray<ObIDASTaskOp *> tasks;
            ObDASRemoteInfo remote_info;
            remote_info.exec_ctx_ = &dst_ctx;
            remote_info.frame_info_ = &decoded;
            remote_info.has_expr_ = true;
            ASSERT_EQ(OB_SUCCESS, service.collect_das_task_info(tasks, remote_info));
            ASSERT_EQ(state, remote_info.ser_compact_state_);
            serialization::SerCompactEncodingGuard forward_guard(remote_info.ser_compact_state_ == SerCompactState::V4);
            const int64_t forward_size = ObPxTreeSerializer::get_serialize_expr_frame_info_size<true>(dst_ctx, decoded);
            std::vector<char> forwarded(forward_size);
            int64_t forward_pos = 0;
            ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::serialize_expr_frame_info<true>(
                forwarded.data(), forward_size, forward_pos, dst_ctx, decoded));
            ASSERT_EQ(forward_size, forward_pos);
            ObExecContext next_ctx(allocator);
            ObPhysicalPlanCtx next_plan_ctx(allocator);
            next_ctx.set_physical_plan_ctx(&next_plan_ctx);
            ObExprFrameInfo next_info(allocator);
            SerCompactState next_state = SerCompactState::UNKNOWN;
            forward_pos = 0;
            ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_expr_frame_info<true>(
                forwarded.data(), forward_size, forward_pos, next_ctx, next_info, state, &next_state));
            ASSERT_EQ(state, next_state);
            ASSERT_EQ(decoded_count, next_info.rt_exprs_.count());
            ASSERT_EQ(forward_size, forward_pos);
          } else {
            ASSERT_EQ(SerCompactState::UNKNOWN, state);
          }
          if (PLAN_PART && compact) {
            ObExecContext auto_ctx(allocator);
            ObPhysicalPlanCtx auto_plan_ctx(allocator);
            auto_ctx.set_physical_plan_ctx(&auto_plan_ctx);
            ObExprFrameInfo auto_info(allocator);
            ObExprSerializeArrayGuard auto_array_guard(&sentinel);
            pos = 0;
            ASSERT_EQ(OB_SUCCESS, ObPxTreeSerializer::deserialize_expr_frame_info<true>(
                                    bytes.data(), size, pos, auto_ctx, auto_info, SerCompactState::UNKNOWN));
            ASSERT_EQ(size, pos);
          }
          ObExecContext bad_ctx(allocator);
          ObPhysicalPlanCtx bad_plan_ctx(allocator);
          bad_ctx.set_physical_plan_ctx(&bad_plan_ctx);
          ObExprFrameInfo bad_info(allocator);
          ObExprSerializeArrayGuard bad_array_guard(&sentinel);
          pos = 0;
          ASSERT_NE(OB_SUCCESS, ObPxTreeSerializer::deserialize_expr_frame_info<PLAN_PART>(
                                  bytes.data(), size - 1, pos, bad_ctx, PLAN_PART ? bad_info : info,
                                  compact ? SerCompactState::V4 : SerCompactState::LEGACY));
          ASSERT_EQ(0, bad_info.rt_exprs_.count());
          ASSERT_EQ(count, info.rt_exprs_.count());
        }
      };
      roundtrip(std::true_type{});
      ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
      roundtrip(std::false_type{});
      ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
    }
  }
}

TEST_F(TestExprSerializeV4, das_cacheable_ctdef_expr_array_scope)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ClusterVersionGuard version_guard(CLUSTER_VERSION_4_4_2_3);
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "DASCtDefExpr"));
    ObExecContext exec_ctx(allocator);
    ObPhysicalPlanCtx plan_ctx(allocator);
    ObPhysicalPlan phy_plan;
    phy_plan.set_batch_size(1);
    plan_ctx.set_phy_plan(&phy_plan);
    exec_ctx.set_physical_plan_ctx(&plan_ctx);

    ObExprFrameInfo frame_info(allocator);
    ASSERT_EQ(OB_SUCCESS, frame_info.rt_exprs_.prepare_allocate(1));
    build_expr(frame_info.rt_exprs_.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
    frame_info.rt_exprs_.at(0).obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
    ObFrameInfo datum_frame;
    datum_frame.expr_cnt_ = 1;
    datum_frame.frame_idx_ = 0;
    // One non-rich fixed-length vector slot: datum header + eval info/flags +
    // dynamic-buffer header + 4-byte reserved datum data.
    datum_frame.frame_size_ = 33;
    datum_frame.use_rich_format_ = false;
    ASSERT_EQ(OB_SUCCESS, frame_info.datum_frame_.init(1));
    ASSERT_EQ(OB_SUCCESS, frame_info.datum_frame_.push_back(datum_frame));
    char **frames = static_cast<char **>(allocator.alloc(sizeof(char *)));
    ASSERT_NE(nullptr, frames);
    frames[0] = static_cast<char *>(allocator.alloc(datum_frame.frame_size_));
    ASSERT_NE(nullptr, frames[0]);
    MEMSET(frames[0], 0, datum_frame.frame_size_);
    exec_ctx.set_frames(frames);
    exec_ctx.set_frame_cnt(1);

    ObDASScanCtDef scan_ctdef(allocator);
    ASSERT_EQ(OB_SUCCESS, scan_ctdef.pd_expr_spec_.access_exprs_.init(1));
    ASSERT_EQ(OB_SUCCESS, scan_ctdef.pd_expr_spec_.access_exprs_.push_back(&frame_info.rt_exprs_.at(0)));

    ObDASRemoteInfo src;
    src.exec_ctx_ = &exec_ctx;
    src.frame_info_ = &frame_info;
    src.has_expr_ = true;
    src.use_deser_cache_format_ = true;
    src.ser_compact_state_ = SerCompactState::V4;
    ASSERT_EQ(OB_SUCCESS, src.ctdefs_.push_back(&scan_ctdef));

    ObArray<ObExpr> sentinel;
    ASSERT_EQ(OB_SUCCESS, sentinel.prepare_allocate(1));
    ExprSerializeArrayGuard array_guard(&sentinel);

    serialization::SerCompactEncodingGuard v4_guard(true);
    const int64_t size = src.get_cacheable_ctdef_expr_serialize_size();
    ASSERT_GT(size, 0);
    char *buf = static_cast<char *>(allocator.alloc(size));
    ASSERT_NE(nullptr, buf);
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, src.serialize_cacheable_ctdef_expr(buf, size, pos));
    ASSERT_EQ(size, pos);
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    int64_t decode_pos = 0;
    int64_t need_ctx_cnt = 0;
    ObExprExtraSerializeInfo expr_info;
    expr_info.current_time_ = &plan_ctx.get_cur_time();
    expr_info.last_trace_id_ = &plan_ctx.get_last_trace_id();
    expr_info.mview_ids_ = &plan_ctx.get_mview_ids();
    expr_info.last_refresh_scns_ = &plan_ctx.get_last_refresh_scns();
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, decode_pos, need_ctx_cnt));
    ASSERT_EQ(OB_SUCCESS, expr_info.deserialize(buf, pos, decode_pos));
    int64_t ctdef_expr_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, pos, decode_pos, &ctdef_expr_len));
    const int64_t ctdef_expr_begin = decode_pos;

    ObDASRemoteInfo dst;
    dst.exec_ctx_ = &exec_ctx;
    dst.has_expr_ = true;
    ObArray<ObExpr> dst_exprs;
    ObIArray<const ObDASBaseCtDef *> &dst_ctdefs = dst.ctdefs_;
    SerCompactState state = SerCompactState::UNKNOWN;
    ASSERT_EQ(OB_SUCCESS, dst.decode_cacheable_ctdef_expr(buf, ctdef_expr_begin + ctdef_expr_len, decode_pos, allocator,
                                                          dst_exprs, state));
    ASSERT_EQ(ctdef_expr_begin + ctdef_expr_len, decode_pos);
    ASSERT_EQ(1, dst_exprs.count());
    ASSERT_EQ(1, dst_ctdefs.count());
    ASSERT_EQ(SerCompactState::V4, state);
    const ObDASScanCtDef *dst_scan_ctdef = static_cast<const ObDASScanCtDef *>(dst_ctdefs.at(0));
    ASSERT_EQ(1, dst_scan_ctdef->pd_expr_spec_.access_exprs_.count());
    ASSERT_EQ(&dst_exprs.at(0), dst_scan_ctdef->pd_expr_spec_.access_exprs_.at(0));
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    // A V4 mark segment keeps the complete member sequence and represents
    // unselected members as compact holes.
    ObExprFrameInfo mark_frame_info(allocator, frame_info.rt_exprs_);
    ASSERT_EQ(OB_SUCCESS, mark_frame_info.ser_expr_marks_.init(1));
    ASSERT_EQ(OB_SUCCESS, mark_frame_info.ser_expr_marks_.push_back(true));
    ASSERT_EQ(OB_SUCCESS, mark_frame_info.datum_frame_.init(1));
    ASSERT_EQ(OB_SUCCESS, mark_frame_info.datum_frame_.push_back(datum_frame));
    src.frame_info_ = &mark_frame_info;
    const int64_t mark_size = src.get_cacheable_ctdef_expr_serialize_size();
    char *mark_buf = static_cast<char *>(allocator.alloc(mark_size));
    ASSERT_NE(nullptr, mark_buf);
    int64_t mark_pos = 0;
    ASSERT_EQ(OB_SUCCESS, src.serialize_cacheable_ctdef_expr(mark_buf, mark_size, mark_pos));
    ASSERT_EQ(mark_size, mark_pos);

    int64_t mark_probe_pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode(mark_buf, mark_pos, mark_probe_pos, need_ctx_cnt));
    ASSERT_EQ(OB_SUCCESS, expr_info.deserialize(mark_buf, mark_pos, mark_probe_pos));
    int64_t mark_blob_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(mark_buf, mark_pos, mark_probe_pos, &mark_blob_len));
    const int64_t mark_blob_begin = mark_probe_pos;
    int32_t mark_expr_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_i32(mark_buf, mark_pos, mark_probe_pos, &mark_expr_cnt));
    ASSERT_EQ(1, mark_expr_cnt);
    int64_t mark_expr_version = 0;
    int64_t mark_expr_body_len = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(mark_buf, mark_pos, mark_probe_pos, &mark_expr_version));
    ASSERT_EQ(serialization::SER_COMPACT_OPT_VERSION, mark_expr_version);
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(mark_buf, mark_pos, mark_probe_pos, &mark_expr_body_len));
    ASSERT_GE(mark_expr_body_len, 0);
    ASSERT_LE(mark_expr_body_len, mark_pos - mark_probe_pos);

    ObDASRemoteInfo mark_dst;
    mark_dst.exec_ctx_ = &exec_ctx;
    mark_dst.has_expr_ = true;
    ObArray<ObExpr> mark_dst_exprs;
    ObIArray<const ObDASBaseCtDef *> &mark_dst_ctdefs = mark_dst.ctdefs_;
    SerCompactState mark_state = SerCompactState::UNKNOWN;
    int64_t mark_decode_pos = mark_blob_begin;
    ASSERT_EQ(OB_SUCCESS, mark_dst.decode_cacheable_ctdef_expr(mark_buf, mark_blob_begin + mark_blob_len,
                                                               mark_decode_pos, allocator, mark_dst_exprs, mark_state));
    ASSERT_EQ(mark_blob_begin + mark_blob_len, mark_decode_pos);
    ASSERT_EQ(1, mark_dst_exprs.count());
    ASSERT_EQ(1, mark_dst_ctdefs.count());
    ASSERT_EQ(SerCompactState::V4, mark_state);
    const ObDASScanCtDef *mark_dst_scan_ctdef = static_cast<const ObDASScanCtDef *>(mark_dst_ctdefs.at(0));
    ASSERT_EQ(&mark_dst_exprs.at(0), mark_dst_scan_ctdef->pd_expr_spec_.access_exprs_.at(0));
    ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());

    src.frame_info_ = &frame_info;

    const int32_t invalid_counts[] = {-1, INT32_MIN, 2};
    for (int64_t i = 0; i < ARRAYSIZEOF(invalid_counts); ++i) {
      char *malformed = static_cast<char *>(allocator.alloc(pos));
      ASSERT_NE(nullptr, malformed);
      MEMCPY(malformed, buf, pos);
      int64_t count_pos = ctdef_expr_begin;
      ASSERT_EQ(OB_SUCCESS, serialization::encode_i32(malformed, pos, count_pos, invalid_counts[i]));
      ObDASRemoteInfo bad_dst;
      bad_dst.exec_ctx_ = &exec_ctx;
      bad_dst.has_expr_ = true;
      ObArray<ObExpr> bad_exprs;
      ObIArray<const ObDASBaseCtDef *> &bad_ctdefs = bad_dst.ctdefs_;
      SerCompactState bad_state = SerCompactState::UNKNOWN;
      int64_t bad_pos = ctdef_expr_begin;
      ASSERT_NE(OB_SUCCESS, bad_dst.decode_cacheable_ctdef_expr(malformed, ctdef_expr_begin + ctdef_expr_len, bad_pos,
                                                                allocator, bad_exprs, bad_state));
      ASSERT_EQ(0, bad_exprs.count());
      ASSERT_EQ(0, bad_ctdefs.count());
      ASSERT_LE(bad_pos, ctdef_expr_begin + ctdef_expr_len);
      ASSERT_EQ(&sentinel, ObExpr::get_serialize_array());
    }

    ObDASScanCtDef no_expr_scan_ctdef(allocator);
    no_expr_scan_ctdef.ref_table_id_ = 600001;
    ObDASRemoteInfo no_expr;
    no_expr.exec_ctx_ = &exec_ctx;
    no_expr.has_expr_ = false;
    no_expr.use_deser_cache_format_ = true;
    no_expr.ser_compact_state_ = SerCompactState::V4;
    ASSERT_EQ(OB_SUCCESS, no_expr.ctdefs_.push_back(&no_expr_scan_ctdef));
    ASSERT_GT(no_expr.get_cacheable_ctdef_expr_serialize_size(), 0);
  }
}

TEST_F(TestExprSerializeV4, das_inline_scan_ctdef_uses_request_guard)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArenaAllocator allocator(ObMemAttr(OB_SERVER_TENANT_ID, "DASInlineV4"));
    ObExecContext exec_ctx(allocator);
    ObPhysicalPlanCtx plan_ctx(allocator);
    ObPhysicalPlan phy_plan;
    plan_ctx.set_phy_plan(&phy_plan);
    exec_ctx.set_physical_plan_ctx(&plan_ctx);
    ObDASScanCtDef scan_ctdef(allocator);
    scan_ctdef.ref_table_id_ = 600101;

    ObDASRemoteInfo remote_info;
    remote_info.exec_ctx_ = &exec_ctx;
    remote_info.use_deser_cache_format_ = false;
    ASSERT_EQ(OB_SUCCESS, remote_info.ctdefs_.push_back(&scan_ctdef));

    auto encode_and_probe = [&](const bool use_v4, int64_t &record_version) {
      char buf[64 * 1024];
      int64_t pos = 0;
      remote_info.ser_compact_state_ = use_v4 ? SerCompactState::V4 : SerCompactState::LEGACY;
      serialization::SerCompactEncodingGuard guard(!use_v4);
      const int64_t expected_size = remote_info.get_serialize_size();
      ASSERT_EQ(!use_v4, serialization::using_v4_encoding());
      ASSERT_EQ(OB_SUCCESS, remote_info.serialize(buf, sizeof(buf), pos));
      ASSERT_EQ(expected_size, pos);
      ASSERT_EQ(!use_v4, serialization::using_v4_encoding());
      int64_t probe_pos = 0;
      int64_t version = 0;
      int64_t body_size = 0;
      int64_t flags = 0;
      transaction::ObTxReadSnapshot snapshot;
      int64_t ctdef_cnt = 0;
      ObDASOpType op_type = DAS_OP_INVALID;
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, probe_pos, version));
      ASSERT_EQ(1, version);
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, probe_pos, body_size));
      ASSERT_EQ(pos - probe_pos, body_size);
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, probe_pos, flags));
      ASSERT_EQ(remote_info.flags_, flags);
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, probe_pos, snapshot));
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, probe_pos, ctdef_cnt));
      ASSERT_EQ(1, ctdef_cnt);
      ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, probe_pos, op_type));
      ASSERT_EQ(DAS_OP_TABLE_SCAN, op_type);
      ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, pos, probe_pos, &record_version));
      int64_t short_pos = 0;
      ASSERT_NE(OB_SUCCESS, remote_info.serialize(buf, 0, short_pos));
      ASSERT_EQ(0, short_pos);
      ASSERT_EQ(!use_v4, serialization::using_v4_encoding());
    };

    int64_t legacy_version = 0;
    int64_t compact_version = 0;
    encode_and_probe(false, legacy_version);
    encode_and_probe(true, compact_version);
    ASSERT_EQ(1, legacy_version);
    ASSERT_EQ(serialization::SER_COMPACT_OPT_VERSION, compact_version);
  }
}

TEST_F(TestExprSerializeV4, test_roundtrip)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    const int64_t N = 6;
    ObArray<ObExpr> src_exprs;
    ASSERT_EQ(OB_SUCCESS, src_exprs.prepare_allocate(N));
    // e0: const int (fixed, batch)
    build_expr(src_exprs.at(0), T_INT, ObInt32Type, 0, 10, 4, 4, 0, true);
    // e1: ref column varchar (varlen), parent = e2
    build_expr(src_exprs.at(1), T_REF_COLUMN, ObVarcharType, 0, 0, 128, 32, 1, true);
    // e2: cast(varchar) with args=[e1], eval funcs
    build_expr(src_exprs.at(2), T_FUN_SYS_CAST, ObVarcharType, 0, 0, 64, 64, 1, true);
    // e3: questionmark int, extra_ = 0 (param idx 0 must be carried)
    build_expr(src_exprs.at(3), T_QUESTIONMARK, ObInt32Type, 0, 10, 4, 0, 2, false);
    // e4: func expr with extra_/ctx_id/local_session_var_id
    build_expr(src_exprs.at(4), T_FUN_SYS_PART_KEY, ObInt32Type, 0, 10, 4, 4, 1, false);
    // e5: decimal int (fixed, precision dependent res size)
    build_expr(src_exprs.at(5), T_REF_COLUMN, ObDecimalIntType, 2, 10, 8, 8, 1, true);

    // link args/parents
    auto &e1 = src_exprs.at(1);
    auto &e2 = src_exprs.at(2);
    auto &e3 = src_exprs.at(3);
    auto &e4 = src_exprs.at(4);
    e1.parent_cnt_ = 1;
    e1.parents_ = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *)));
    e1.parents_[0] = &e2;
    e2.arg_cnt_ = 1;
    e2.args_ = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *)));
    e2.args_[0] = &e1;
    e2.eval_func_ = reinterpret_cast<ObExpr::EvalFunc>(expr_default_eval_vector_func);
    e2.eval_batch_func_ = expr_default_eval_batch_func;
    e2.eval_vector_func_ = expr_default_eval_vector_func;
    e4.extra_ = 12345;
    e4.expr_ctx_id_ = 7;
    e4.local_session_var_id_ = 42;

    ExprSerializeArrayGuard array_guard(&src_exprs);

    // serialize with v4
    char buf[64 * 1024];
    int64_t pos = 0;
    int64_t v3_total = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ASSERT_EQ(OB_SUCCESS, encode_expr_v4(src_exprs.at(i), buf, sizeof(buf), pos, false));
      v3_total += src_exprs.at(i).get_serialize_size();
    }
    const int64_t v4_total = pos;
    int64_t v4_size_sum = 0;
    for (int64_t i = 0; i < N; ++i) { v4_size_sum += expr_v4_size(src_exprs.at(i), false); }
    ASSERT_EQ(v4_total, v4_size_sum); // expr_v4_size must be exact
    SQL_LOG(INFO, "v4 vs v3 sizes", K(v4_total), K(v3_total));
    ASSERT_EQ(181, v4_total);      // standard UNIS V4 + compact DatumMeta golden
    ASSERT_LT(v4_total, v3_total); // compact format must shrink

    // hole serialization: e1 as hole
    ObExpr &hole_src = src_exprs.at(1);
    int64_t hole_pos = 0;
    char hole_buf[256];
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(hole_src, hole_buf, sizeof(hole_buf), hole_pos, true));
    ASSERT_EQ(hole_pos, expr_v4_size(hole_src, true));

    // deserialize
    ObArray<ObExpr> dst_exprs;
    ASSERT_EQ(OB_SUCCESS, dst_exprs.prepare_allocate(N));
    array_guard.set(&dst_exprs);
    int64_t dpos = 0;
    for (int64_t i = 0; OB_SUCC(ret) && i < N; ++i) {
      ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(dst_exprs.at(i), buf, pos, dpos));
    }
    ASSERT_EQ(dpos, pos);
    for (int64_t i = 0; i < N; ++i) { check_v4_roundtrip(src_exprs.at(i), dst_exprs.at(i)); }
    // args/parents pointers are re-swizzled into the dst array
    ASSERT_EQ(dst_exprs.at(1).parents_[0], &dst_exprs.at(2));
    ASSERT_EQ(dst_exprs.at(2).args_[0], &dst_exprs.at(1));

    // hole round trip: only sizing fields valid; flag_ is masked down to the
    // two sizing bits (batch_result_/is_fixed_length_data_) so a hole stays inert
    ObExpr hole_dst;
    int64_t hole_dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(hole_dst, hole_buf, hole_pos, hole_dpos));
    ASSERT_EQ(hole_dpos, hole_pos);
    ASSERT_EQ(T_INVALID, hole_dst.type_);
    ASSERT_EQ(hole_src.datum_meta_.type_, hole_dst.datum_meta_.type_);
    const uint64_t expect_flag = (hole_src.flag_ & 0x1) | (hole_src.flag_ & (1ull << 6));
    ASSERT_EQ(expect_flag, hole_dst.flag_);
    ASSERT_EQ(hole_src.frame_idx_, hole_dst.frame_idx_);
    ASSERT_EQ(hole_src.res_buf_len_, hole_dst.res_buf_len_);
    ASSERT_EQ(NULL, hole_dst.eval_func_);
    ASSERT_EQ(0u, hole_dst.arg_cnt_);

    // a hole of a const expr must not be treated as const on the receiver:
    // otherwise it would be vector-initialized with an invalid vec_value_tc_.
    ObExpr const_hole;
    const_hole.type_ = T_INT;
    const_hole.datum_meta_.type_ = ObInt32Type;
    const_hole.datum_meta_.cs_type_ = CS_TYPE_BINARY;
    const_hole.datum_meta_.scale_ = 0;
    const_hole.datum_meta_.precision_ = 10;
    const_hole.is_static_const_ = true;
    const_hole.batch_result_ = true;
    const_hole.is_fixed_length_data_ = true;
    const_hole.frame_idx_ = 0;
    const_hole.res_buf_len_ = 8;
    int64_t ch_pos = 0;
    char ch_buf[256];
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(const_hole, ch_buf, sizeof(ch_buf), ch_pos, true));
    ObExpr const_hole_dst;
    int64_t ch_dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(const_hole_dst, ch_buf, ch_pos, ch_dpos));
    ASSERT_TRUE(const_hole_dst.is_batch_result());
    ASSERT_TRUE(const_hole_dst.is_fixed_length_data_);
    ASSERT_FALSE(const_hole_dst.is_const_expr()); // masked: must stay inert
    ASSERT_EQ(MAX_VEC_TC, const_hole_dst.vec_value_tc_);
  }
}

TEST_F(TestExprSerializeV4, data_type_and_expression_shape_matrix)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    struct TypeCase
    {
      ObObjType type_;
      int16_t scale_;
      int16_t precision_;
    };
    const TypeCase type_cases[] = {{ObNullType, 0, 0},
                                   {ObInt32Type, 0, 10},
                                   {ObUInt32Type, 0, 10},
                                   {ObFloatType, SCALE_UNKNOWN_YET, 10},
                                   {ObDoubleType, SCALE_UNKNOWN_YET, 20},
                                   {ObDoubleType, 2, 20},
                                   {ObNumberType, 2, 20},
                                   {ObDateTimeType, 0, 0},
                                   {ObDateType, 0, 0},
                                   {ObTimeType, 0, 0},
                                   {ObYearType, 0, 0},
                                   {ObExtendType, 0, 0},
                                   {ObUnknownType, 0, 0},
                                   {ObVarcharType, 0, 0},
                                   {ObBitType, 0, 64},
                                   {ObEnumType, 0, 0},
                                   {ObEnumInnerType, 0, 0},
                                   {ObTimestampTZType, 6, 0},
                                   {ObTimestampLTZType, 6, 0},
                                   {ObRawType, 0, 0},
                                   {ObIntervalYMType, 0, 0},
                                   {ObIntervalDSType, 6, 0},
                                   {ObURowIDType, 0, 0},
                                   {ObLobType, 0, 0},
                                   {ObJsonType, 0, 0},
                                   {ObGeometryType, 0, 0},
                                   {ObUserDefinedSQLType, 0, 0},
                                   {ObDecimalIntType, 2, 9},
                                   {ObDecimalIntType, 2, 18},
                                   {ObDecimalIntType, 2, 38},
                                   {ObDecimalIntType, 2, 76},
                                   {ObDecimalIntType, 2, 154},
                                   {ObCollectionSQLType, 0, 0},
                                   {ObRoaringBitmapType, 0, 0},
                                   {ObMySQLDateType, 0, 0},
                                   {ObMySQLDateTimeType, 6, 0}};
    const ObExprOperatorType expr_shapes[] = {T_INT,    T_REF_COLUMN, T_QUESTIONMARK,     T_FUN_SYS_CAST,
                                              T_OP_ADD, T_OP_EQ,      T_FUN_SYS_PART_KEY, T_OP_ROW};
    ObArray<ObExpr> src_exprs;
    ObArray<ObExpr> dst_exprs;
    ASSERT_EQ(OB_SUCCESS, src_exprs.prepare_allocate(ARRAYSIZEOF(type_cases)));
    ASSERT_EQ(OB_SUCCESS, dst_exprs.prepare_allocate(ARRAYSIZEOF(type_cases)));
    bool seen[MAX_VEC_TC] = {};
    for (int64_t i = 0; i < ARRAYSIZEOF(type_cases); ++i) {
      const TypeCase &type_case = type_cases[i];
      const VecValueTypeClass tc = get_vec_value_tc(type_case.type_, type_case.scale_, type_case.precision_);
      ASSERT_GE(tc, VEC_TC_NULL);
      ASSERT_LT(tc, MAX_VEC_TC);
      seen[tc] = true;
      const ObObjDatumMapType datum_map = ObDatum::get_obj_datum_map_type(type_case.type_);
      const bool fixed = is_fixed_length_vec(tc);
      const uint32_t reserved_size = fixed ? ObDatum::get_reserved_size(datum_map, type_case.precision_) : 32;
      const int32_t max_length = fixed ? static_cast<int32_t>(reserved_size) : 128;
      build_expr(src_exprs.at(i), expr_shapes[i % ARRAYSIZEOF(expr_shapes)], type_case.type_, type_case.scale_,
                 type_case.precision_, max_length, reserved_size, static_cast<uint32_t>(i % 4), 0 == (i % 2));
      ObExpr &expr = src_exprs.at(i);
      if (VEC_TC_STRING == tc || VEC_TC_ENUM_SET == tc || VEC_TC_ENUM_SET_INNER == tc) {
        expr.datum_meta_.cs_type_ = CS_TYPE_UTF8MB4_GENERAL_CI;
        expr.obj_meta_.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        expr.obj_meta_.set_collation_level(T_INT == expr.type_ ? CS_LEVEL_COERCIBLE : CS_LEVEL_IMPLICIT);
      } else if (fixed && ObNullType != type_case.type_ && ObUnknownType != type_case.type_ && VEC_TC_ENUM_SET != tc
                 && VEC_TC_ENUM_SET_INNER != tc) {
        expr.obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
      } else {
        expr.obj_meta_.set_collation_level(CS_LEVEL_COERCIBLE);
      }
      if (T_QUESTIONMARK == expr.type_) { expr.extra_ = (0 == (i % 2)) ? 0 : i; }
    }
    for (int64_t tc = 0; tc < MAX_VEC_TC; ++tc) { ASSERT_TRUE(seen[tc]) << "missing VecValueTypeClass=" << tc; }

    ExprSerializeArrayGuard array_guard(&src_exprs);
    char buf[128 * 1024];
    int64_t pos = 0;
    for (int64_t i = 0; i < src_exprs.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, encode_expr_v4(src_exprs.at(i), buf, sizeof(buf), pos, false)) << "case=" << i;
    }
    array_guard.set(&dst_exprs);
    int64_t dpos = 0;
    for (int64_t i = 0; i < dst_exprs.count(); ++i) {
      ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(dst_exprs.at(i), buf, pos, dpos)) << "case=" << i;
      check_v4_roundtrip(src_exprs.at(i), dst_exprs.at(i));
    }
    ASSERT_EQ(pos, dpos);
  }
}

// independently compute the expected vectorized datum frame layout
// (mirrors arrange_datums_data section order) and compare with the replayer.
TEST_F(TestExprSerializeV4, test_replay_vector_frame)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    const int64_t batch_size = 4;
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(3));
    // e0: int fixed batch; e1: varchar varlen batch (res_buf_len 32); e2: bigint fixed single
    build_expr(exprs.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, true);
    build_expr(exprs.at(1), T_REF_COLUMN, ObVarcharType, 0, 0, 128, 32, 0, true);
    build_expr(exprs.at(2), T_REF_COLUMN, ObIntType, 0, 20, 8, 8, 0, false);

    // expected layout, computed independently
    auto cnt = [&](ObExpr &e) { return e.is_batch_result() ? batch_size : 1; };
    auto skip = [&](ObExpr &e) { return e.is_batch_result() ? ObBitVector::memory_size(batch_size) : 1; };
    auto bm = [&](ObExpr &e) { return ObBitVector::memory_size(cnt(e)); };
    const int64_t datum_sz = sizeof(ObDatum);
    const int64_t vh_sz = sizeof(VectorHeader);
    int64_t cur = 0;
    int64_t exp_datum_off[3], exp_pvt_skip[3];
    for (int i = 0; i < 3; ++i) {
      exp_datum_off[i] = cur;
      cur += skip(exprs.at(i)) + datum_sz * cnt(exprs.at(i));
      exp_pvt_skip[i] = exp_datum_off[i] + datum_sz * cnt(exprs.at(i));
    }
    int64_t exp_len_arr_1 = cur; // e1 varlen
    cur += sizeof(uint32_t) * (cnt(exprs.at(1)) + 1);
    int64_t exp_ptrs_1 = cur;
    cur += sizeof(char *) * cnt(exprs.at(1));
    int64_t exp_vh[3];
    for (int i = 0; i < 3; ++i) {
      exp_vh[i] = cur;
      cur += vh_sz;
    }
    int64_t exp_nulls[3];
    for (int i = 0; i < 3; ++i) {
      exp_nulls[i] = cur;
      cur += bm(exprs.at(i));
    }
    int64_t exp_cont_1 = cur;
    cur += sizeof(ObDynReserveBuf); // e1 varlen
    int64_t exp_eval_info[3];
    for (int i = 0; i < 3; ++i) {
      exp_eval_info[i] = cur;
      cur += sizeof(ObEvalInfo);
    }
    int64_t exp_eval_flags[3];
    for (int i = 0; i < 3; ++i) {
      exp_eval_flags[i] = cur;
      cur += bm(exprs.at(i));
    }
    int64_t exp_dyn[3];
    for (int i = 0; i < 3; ++i) {
      exp_dyn[i] = cur;
      cur += ObDynReserveBuf::supported(exprs.at(i).datum_meta_.type_) ? sizeof(ObDynReserveBuf) * cnt(exprs.at(i)) : 0;
    }
    int64_t exp_res_off[3];
    int64_t data = 0;
    for (int i = 0; i < 3; ++i) {
      exp_res_off[i] = cur + data;
      data += exprs.at(i).res_buf_len_ * cnt(exprs.at(i));
    }
    const int64_t frame_size = cur + data;

    ObFrameInfo datum_frame;
    datum_frame.expr_cnt_ = 3;
    datum_frame.frame_idx_ = 0;
    datum_frame.frame_size_ = frame_size;
    datum_frame.use_rich_format_ = true;
    ObSEArray<ObFrameInfo, 4> empty_frames, datum_frames;
    ASSERT_EQ(OB_SUCCESS, datum_frames.push_back(datum_frame));

    // scramble the offsets to prove replay sets them
    for (int i = 0; i < 3; ++i) {
      exprs.at(i).datum_off_ = 0xdead;
      exprs.at(i).eval_info_off_ = 0xdead;
    }
    ASSERT_EQ(OB_SUCCESS,
              replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames, datum_frames, batch_size, 0));
    for (int i = 0; i < 3; ++i) {
      ASSERT_EQ(exp_datum_off[i], exprs.at(i).datum_off_) << "expr " << i;
      ASSERT_EQ(exp_pvt_skip[i], exprs.at(i).pvt_skip_off_) << "expr " << i;
      ASSERT_EQ(exp_vh[i], exprs.at(i).vector_header_off_) << "expr " << i;
      ASSERT_EQ(exp_nulls[i], exprs.at(i).null_bitmap_off_) << "expr " << i;
      ASSERT_EQ(exp_eval_info[i], exprs.at(i).eval_info_off_) << "expr " << i;
      ASSERT_EQ(exp_eval_flags[i], exprs.at(i).eval_flags_off_) << "expr " << i;
      ASSERT_EQ(exp_dyn[i], exprs.at(i).dyn_buf_header_offset_) << "expr " << i;
      ASSERT_EQ(exp_res_off[i], exprs.at(i).res_buf_off_) << "expr " << i;
    }
    ASSERT_EQ(exp_len_arr_1, exprs.at(1).len_arr_off_);
    ASSERT_EQ(exp_ptrs_1, exprs.at(1).offset_off_);
    ASSERT_EQ(exp_cont_1, exprs.at(1).cont_buf_off_);
    // fixed length exprs: len_ union carries res_buf_len_
    ASSERT_EQ(exprs.at(0).res_buf_len_, exprs.at(0).len_);
    ASSERT_EQ(exprs.at(2).res_buf_len_, exprs.at(2).len_);

    // frame size mismatch must be detected
    datum_frames.at(0).frame_size_ = frame_size - 1;
    ASSERT_EQ(OB_ERR_UNEXPECTED,
              replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames, datum_frames, batch_size, 0));
    for (const bool all_holes : {false, true}) {
      exprs.at(1).type_ = T_INVALID;
      if (all_holes) {
        exprs.at(0).type_ = T_INVALID;
        exprs.at(2).type_ = T_INVALID;
      }
      datum_frames.at(0).frame_size_ = frame_size;
      ASSERT_EQ(OB_SUCCESS,
                replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames, datum_frames, batch_size, 0));
      for (const int delta : {-1, 1}) {
        datum_frames.at(0).frame_size_ = frame_size + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames,
                                                              datum_frames, batch_size, 0));
        datum_frames.at(0).frame_size_ = frame_size;
        datum_frames.at(0).expr_cnt_ = 3 + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames,
                                                              datum_frames, batch_size, 0));
        datum_frames.at(0).expr_cnt_ = 3;
      }
    }
    for (const int selected_mask : {0, 1, 3, 5, 7}) {
      int64_t last_selected = -1;
      for (int64_t i = 0; i < 3; ++i) {
        exprs.at(i).type_ = selected_mask & (1 << i) ? T_REF_COLUMN : T_INVALID;
        if (T_INVALID != exprs.at(i).type_) { last_selected = i; }
      }
      datum_frames.at(0) = datum_frame;
      const ObExpr *farthest = last_selected < 0 ? nullptr : &exprs.at(last_selected);
      ObStaticEngineExprCG::trim_frame(datum_frames.at(0), farthest, batch_size, true, false, false);
      const int64_t expected_size =
        last_selected < 0 ?
          sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader) :
          exp_res_off[last_selected] + exprs.at(last_selected).res_buf_len_ * cnt(exprs.at(last_selected));
      ASSERT_EQ(expected_size, datum_frames.at(0).frame_size_);
      ASSERT_EQ(3, datum_frames.at(0).expr_cnt_);
      ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames, datum_frames,
                                                     batch_size, 0, true));
      for (int64_t i = 0; i < 3; ++i) {
        ASSERT_EQ(exp_datum_off[i], exprs.at(i).datum_off_);
        ASSERT_EQ(exp_res_off[i], exprs.at(i).res_buf_off_);
        ASSERT_EQ(exp_eval_info[i], exprs.at(i).eval_info_off_);
        ASSERT_EQ(exp_vh[i], exprs.at(i).vector_header_off_);
      }
      for (const int delta : {-1, 1}) {
        datum_frames.at(0).frame_size_ = expected_size + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty_frames, empty_frames, empty_frames,
                                                              datum_frames, batch_size, 0, true));
      }
    }
  }
}

// param frame replay: questionmarks are located by extra_ (param index)
TEST_F(TestExprSerializeV4, test_replay_param_frame)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(2));
    build_expr(exprs.at(0), T_QUESTIONMARK, ObInt32Type, 0, 10, 4, 0, 0, false);
    build_expr(exprs.at(1), T_QUESTIONMARK, ObVarcharType, 0, 0, 128, 0, 0, false);
    exprs.at(0).extra_ = 0;
    exprs.at(1).extra_ = 1;

    const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
    ObFrameInfo param_frame;
    param_frame.expr_cnt_ = 2;
    param_frame.frame_idx_ = 0; // no const frames before it: global index is 0
    param_frame.frame_size_ = 2 * item;
    param_frame.use_rich_format_ = true;
    ObSEArray<ObFrameInfo, 4> empty_frames, param_frames;
    ASSERT_EQ(OB_SUCCESS, param_frames.push_back(param_frame));

    ASSERT_EQ(OB_SUCCESS,
              replay_expr_frame_layout(exprs, empty_frames, param_frames, empty_frames, empty_frames, 0, 2));
    ASSERT_EQ(0u, exprs.at(0).datum_off_);
    ASSERT_EQ((uint32_t)item, exprs.at(1).datum_off_);
    ASSERT_EQ(sizeof(ObDatum), exprs.at(0).eval_info_off_);
    ASSERT_EQ(sizeof(ObDatum) + sizeof(ObEvalInfo), exprs.at(0).vector_header_off_);
    ASSERT_EQ(0u, exprs.at(0).res_buf_len_);
    ASSERT_EQ(0u, exprs.at(1).res_buf_len_);
    // fixed type in rich param frame: len_ is the reserved size (not 0)
    ASSERT_EQ(ObDatum::get_reserved_size(exprs.at(0).obj_datum_map_, exprs.at(0).datum_meta_.precision_),
              exprs.at(0).len_);
  }
}

TEST_F(TestExprSerializeV4, replay_ctx_frame_bounds_and_completion)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> exprs;
    ObArray<ObExprCompactHeader> headers;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(2));
    ASSERT_EQ(OB_SUCCESS, headers.prepare_allocate(2));
    build_expr(exprs.at(0), T_QUESTIONMARK, ObInt32Type, 0, 10, 4, 0, 0, false);
    build_expr(exprs.at(1), T_QUESTIONMARK, ObVarcharType, 0, 0, 128, 0, 0, false);
    exprs.at(0).extra_ = 0;
    exprs.at(1).extra_ = 1;
    headers.at(0) = ObExprCompactHeader(exprs.at(0));
    headers.at(1) = ObExprCompactHeader(exprs.at(1));

    const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + sizeof(VectorHeader);
    ObFrameInfo param_frame;
    param_frame.expr_cnt_ = 2;
    param_frame.frame_idx_ = 0;
    param_frame.frame_size_ = 2 * item;
    param_frame.use_rich_format_ = true;
    ObSEArray<ObFrameInfo, 4> empty_frames, param_frames;
    ASSERT_EQ(OB_SUCCESS, param_frames.push_back(param_frame));

    ObExprReplayCtx replay_ctx(exprs, empty_frames, param_frames, empty_frames, empty_frames, 0, 2);
    ASSERT_EQ(OB_SUCCESS, replay_ctx.prepare(headers));
    ASSERT_NE(OB_SUCCESS, replay_ctx.check());
    ASSERT_EQ(OB_SUCCESS, headers.at(0).replay_to(exprs.at(0), replay_ctx));
    ASSERT_NE(OB_SUCCESS, replay_ctx.check());
    ASSERT_EQ(OB_SUCCESS, headers.at(1).replay_to(exprs.at(1), replay_ctx));
    ASSERT_EQ(OB_SUCCESS, replay_ctx.check());
    ASSERT_NE(OB_SUCCESS, replay_ctx.prepare(headers));

    ObArray<ObExprCompactHeader> short_headers;
    ASSERT_EQ(OB_SUCCESS, short_headers.prepare_allocate(1));
    short_headers.at(0) = ObExprCompactHeader(exprs.at(0));
    ObExprReplayCtx bad_count_ctx(exprs, empty_frames, param_frames, empty_frames, empty_frames, 0, 2);
    ASSERT_NE(OB_SUCCESS, bad_count_ctx.prepare(short_headers));

    ObSEArray<ObFrameInfo, 4> duplicate_frames;
    ASSERT_EQ(OB_SUCCESS, duplicate_frames.push_back(param_frame));
    ObExprReplayCtx duplicate_frame_ctx(exprs, duplicate_frames, param_frames, empty_frames, empty_frames, 0, 2);
    ASSERT_NE(OB_SUCCESS, duplicate_frame_ctx.prepare(headers));

    exprs.at(1).frame_idx_ = 1;
    ObExprReplayCtx missing_frame_ctx(exprs, empty_frames, param_frames, empty_frames, empty_frames, 0, 2);
    ASSERT_NE(OB_SUCCESS, missing_frame_ctx.prepare(headers));
  }
}

// non-vector (item) datum frame replay with a hole in the middle
TEST_F(TestExprSerializeV4, test_replay_item_frame_with_hole)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(3));
    build_expr(exprs.at(0), T_INT, ObInt32Type, 0, 10, 4, 4, 0, false);
    build_expr(exprs.at(1), T_INT, ObVarcharType, 0, 0, 16, 16, 0, false);
    build_expr(exprs.at(2), T_INT, ObInt32Type, 0, 10, 4, 4, 0, false);
    exprs.at(1).type_ = T_INVALID; // hole skeleton

    // expected layout derived from runtime primitives (not literals)
    const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo); // no rich format
    auto consume = [&](ObExpr &e) {
      return (int64_t)e.res_buf_len_
             + (ObDynReserveBuf::supported(e.datum_meta_.type_) && e.res_buf_len_ > 0 ?
                  (int64_t)sizeof(ObDynReserveBuf) :
                  0);
    };
    int64_t data_off = 3 * item;
    int64_t exp_res_off[3];
    int64_t exp_dyn_hdr[3];
    for (int i = 0; i < 3; ++i) {
      data_off += consume(exprs.at(i));
      exp_res_off[i] = data_off - exprs.at(i).res_buf_len_;
      exp_dyn_hdr[i] = exp_res_off[i] - (int64_t)sizeof(ObDynReserveBuf);
    }
    const int64_t frame_size = data_off;

    ObFrameInfo const_frame;
    const_frame.expr_cnt_ = 3;
    const_frame.frame_idx_ = 0;
    const_frame.frame_size_ = frame_size;
    const_frame.use_rich_format_ = false;
    ObSEArray<ObFrameInfo, 4> const_frames, empty_frames;
    ASSERT_EQ(OB_SUCCESS, const_frames.push_back(const_frame));

    ASSERT_EQ(OB_SUCCESS,
              replay_expr_frame_layout(exprs, const_frames, empty_frames, empty_frames, empty_frames, 0, 0));
    for (int i = 0; i < 3; ++i) {
      ASSERT_EQ((uint32_t)(i * item), exprs.at(i).datum_off_) << "expr " << i;
      ASSERT_EQ((uint32_t)(i * item + sizeof(ObDatum)), exprs.at(i).eval_info_off_) << "expr " << i;
      ASSERT_EQ((uint32_t)exp_res_off[i], exprs.at(i).res_buf_off_) << "expr " << i;
    }
    // dyn reserve header sits right before the reserve buffer (varchar hole only)
    ASSERT_EQ((uint32_t)exp_dyn_hdr[1], exprs.at(1).dyn_buf_header_offset_);
    for (const bool all_holes : {false, true}) {
      if (all_holes) {
        exprs.at(0).type_ = T_INVALID;
        exprs.at(2).type_ = T_INVALID;
      }
      ASSERT_EQ(OB_SUCCESS,
                replay_expr_frame_layout(exprs, const_frames, empty_frames, empty_frames, empty_frames, 0, 0));
      for (const int delta : {-1, 1}) {
        const_frames.at(0).frame_size_ = frame_size + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED,
                  replay_expr_frame_layout(exprs, const_frames, empty_frames, empty_frames, empty_frames, 0, 0));
        const_frames.at(0).frame_size_ = frame_size;
        const_frames.at(0).expr_cnt_ = 3 + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED,
                  replay_expr_frame_layout(exprs, const_frames, empty_frames, empty_frames, empty_frames, 0, 0));
        const_frames.at(0).expr_cnt_ = 3;
      }
    }
  }
}

TEST_F(TestExprSerializeV4, replay_empty_frames_validate_metadata)
{
  ObArray<ObExpr> exprs;
  ObSEArray<ObFrameInfo, 4> frames, empty;
  ObFrameInfo frame;
  frame.frame_idx_ = 0;
  frame.expr_cnt_ = 0;
  frame.frame_size_ = 0;
  frame.use_rich_format_ = false;
  ASSERT_EQ(OB_SUCCESS, frames.push_back(frame));
  for (const int batch_size : {0, 4}) {
    ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, empty, empty, frames, batch_size, 0));
    frames.at(0).expr_cnt_ = 1;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, empty, empty, frames, batch_size, 0));
    frames.at(0).expr_cnt_ = 0;
    frames.at(0).frame_size_ = 1;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, empty, empty, frames, batch_size, 0));
    frames.at(0).frame_size_ = 0;
  }
  frames.at(0).expr_cnt_ = 3;
  frames.at(0).frame_size_ = 3 * (sizeof(ObDatum) + sizeof(ObEvalInfo));
  ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 3));
  ++frames.at(0).frame_size_;
  ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 3));
}

TEST_F(TestExprSerializeV4, param_frame_slots_allow_sparse_and_repeated_references)
{
  for (const bool rich : {false, true}) {
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(4));
    const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + (rich ? sizeof(VectorHeader) : 0);
    ObFrameInfo frame;
    frame.frame_idx_ = 0;
    frame.expr_cnt_ = 4;
    frame.frame_size_ = 4 * item;
    frame.use_rich_format_ = rich;
    ObSEArray<ObFrameInfo, 4> frames, empty;
    ASSERT_EQ(OB_SUCCESS, frames.push_back(frame));
    for (const int holes : {0, 2, 4}) {
      for (int64_t i = 0; i < exprs.count(); ++i) {
        build_expr(exprs.at(i), i < holes ? T_INVALID : T_QUESTIONMARK, ObInt32Type, 0, 10, 4, 0, 0, false);
        exprs.at(i).extra_ = 2;
      }
      ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 4));
      for (int64_t i = holes; i < exprs.count(); ++i) { ASSERT_EQ(2 * item, exprs.at(i).datum_off_); }
      for (const int delta : {-1, 1}) {
        frames.at(0).frame_size_ = 4 * item + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 4));
      }
      frames.at(0).frame_size_ = 4 * item;
    }
    // More references than slots are valid when the references share a slot.
    frames.at(0).expr_cnt_ = 3;
    frames.at(0).frame_size_ = 3 * item;
    for (int64_t i = 0; i < exprs.count(); ++i) { exprs.at(i).type_ = T_QUESTIONMARK; }
    ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 3));
    exprs.at(0).extra_ = 3;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 4));
    exprs.at(0).extra_ = UINT64_MAX;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 4));
    exprs.at(0).extra_ = 2;
    exprs.at(0).type_ = T_INT;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 4));
    exprs.at(0).type_ = T_QUESTIONMARK;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, -1));
    frames.at(0).expr_cnt_ = common::MAX_FRAME_SIZE / item + 1;
    frames.at(0).frame_size_ = frames.at(0).expr_cnt_ * item;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 4));
  }
}

TEST_F(TestExprSerializeV4, param_frame_original_and_appended_slot_boundaries)
{
  for (const bool rich : {false, true}) {
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(4));
    const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + (rich ? sizeof(VectorHeader) : 0);
    ObFrameInfo frame;
    frame.frame_idx_ = 0;
    frame.expr_cnt_ = 0;
    frame.frame_size_ = 0;
    frame.use_rich_format_ = rich;
    ObSEArray<ObFrameInfo, 4> const_frames, param_frames, empty;
    ASSERT_EQ(OB_SUCCESS, const_frames.push_back(frame));
    frame.frame_idx_ = 1;
    frame.expr_cnt_ = 3;
    frame.frame_size_ = 3 * item;
    ASSERT_EQ(OB_SUCCESS, param_frames.push_back(frame));
    frame.frame_idx_ = 2;
    frame.expr_cnt_ = 2;
    frame.frame_size_ = 2 * item;
    ASSERT_EQ(OB_SUCCESS, param_frames.push_back(frame));
    for (int64_t i = 0; i < exprs.count(); ++i) {
      build_expr(exprs.at(i), T_QUESTIONMARK, ObInt32Type, 0, 10, 4, 0, i < 2 ? 1 : 2, false);
      exprs.at(i).extra_ = i < 2 ? i * 2 : i + 1;
    }
    ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, const_frames, param_frames, empty, empty, 0, 3));
    ASSERT_EQ(0u, exprs.at(2).datum_off_);
    ASSERT_EQ(item, exprs.at(3).datum_off_);
    // Modulo alone would map this parameter to a valid slot in the wrong frame.
    exprs.at(2).frame_idx_ = 1;
    ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, const_frames, param_frames, empty, empty, 0, 3));
    exprs.at(2).frame_idx_ = 2;
    const int64_t slots = common::MAX_FRAME_SIZE / item;
    param_frames.at(0).expr_cnt_ = slots;
    param_frames.at(0).frame_size_ = slots * item;
    exprs.at(2).extra_ = slots;
    exprs.at(3).extra_ = slots + 1;
    ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, const_frames, param_frames, empty, empty, 0, slots + 2));
    for (int64_t i = 0; i < exprs.count(); ++i) {
      exprs.at(i).frame_idx_ = 1;
      exprs.at(i).extra_ = i;
    }
    ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, const_frames, param_frames, empty, empty, 0, 0));
  }
}

TEST_F(TestExprSerializeV4, partial_param_trim_checks_exact_slots)
{
  for (const bool rich : {false, true}) {
    for (const int selected_mask : {0, 1, 2, 3}) {
      ObArray<ObExpr> exprs;
      ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(4));
      const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + (rich ? sizeof(VectorHeader) : 0);
      for (int64_t i = 0; i < exprs.count(); ++i) {
        const bool selected = selected_mask & (1 << (i / 2));
        build_expr(exprs.at(i), selected ? T_QUESTIONMARK : T_INVALID, ObInt32Type, 0, 10, 4, 0, 0, false);
        exprs.at(i).extra_ = i < 2 ? 1 : 3; // repeated references and unused slots
        exprs.at(i).datum_off_ = exprs.at(i).extra_ * item;
      }
      ObFrameInfo frame;
      frame.frame_idx_ = 0;
      frame.expr_cnt_ = 5;
      frame.frame_size_ = 5 * item;
      frame.use_rich_format_ = rich;
      const ObExpr *farthest = selected_mask & 2 ? &exprs.at(3) : selected_mask & 1 ? &exprs.at(1) : nullptr;
      ObStaticEngineExprCG::trim_frame(frame, farthest, 1, rich, true, true);
      const int64_t expected_slots = selected_mask & 2 ? 4 : selected_mask & 1 ? 2 : 1;
      ASSERT_EQ(expected_slots, frame.expr_cnt_);
      ASSERT_EQ(expected_slots * item, frame.frame_size_);
      ObSEArray<ObFrameInfo, 4> frames, empty;
      ASSERT_EQ(OB_SUCCESS, frames.push_back(frame));
      ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 5, true));
      for (const int delta : {-1, 1}) {
        frames.at(0).frame_size_ = frame.frame_size_ + delta;
        ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 5, true));
        // Even self-consistent metadata must match the selected-slot trim.
        frames.at(0).expr_cnt_ = expected_slots + delta;
        frames.at(0).frame_size_ = (expected_slots + delta) * item;
        ASSERT_EQ(OB_ERR_UNEXPECTED, replay_expr_frame_layout(exprs, empty, frames, empty, empty, 0, 5, true));
        frames.at(0) = frame;
      }
    }
  }
}

TEST_F(TestExprSerializeV4, partial_item_trim_preserves_varlen_hole_offsets)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TrimHole", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    for (const bool rich : {false, true}) {
      for (const bool dynamic : {false, true}) {
        for (const int selected_mask : {0, 1, 3, 5, 7}) {
          ObArray<ObExpr> exprs;
          ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(3));
          build_expr(exprs.at(0), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
          build_expr(exprs.at(1), T_REF_COLUMN, ObVarcharType, 0, 0, 128, 32, 0, false);
          build_expr(exprs.at(2), T_REF_COLUMN, ObInt32Type, 0, 10, 4, 4, 0, false);
          const int64_t item = sizeof(ObDatum) + sizeof(ObEvalInfo) + (rich ? sizeof(VectorHeader) : 0);
          int64_t end = 3 * item;
          int64_t offsets[3];
          int64_t ends[3];
          int64_t farthest_idx = -1;
          for (int64_t i = 0; i < 3; ++i) {
            if (ObDynReserveBuf::supported(exprs.at(i).datum_meta_.type_)) { end += sizeof(ObDynReserveBuf); }
            offsets[i] = end;
            end += exprs.at(i).res_buf_len_;
            ends[i] = end;
            if (selected_mask & (1 << i)) { farthest_idx = i; }
          }
          ObFrameInfo frame;
          frame.frame_idx_ = 0;
          frame.expr_cnt_ = 3;
          frame.frame_size_ = end;
          frame.use_rich_format_ = rich;
          ObSEArray<ObFrameInfo, 4> frames, empty;
          ASSERT_EQ(OB_SUCCESS, frames.push_back(frame));
          ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, empty, dynamic ? frames : empty,
                                                         dynamic ? empty : frames, 0, 0));
          const ObExpr *farthest = farthest_idx < 0 ? nullptr : &exprs.at(farthest_idx);
          ObStaticEngineExprCG::trim_frame(frames.at(0), farthest, 1, rich, false, dynamic);
          ASSERT_EQ(farthest_idx < 0 ? item : ends[farthest_idx], frames.at(0).frame_size_);
          ASSERT_EQ(dynamic ? std::max<int64_t>(1, farthest_idx + 1) : 3, frames.at(0).expr_cnt_);
          for (int64_t i = 0; i < 3; ++i) {
            if (!(selected_mask & (1 << i))) { exprs.at(i).type_ = T_INVALID; }
            exprs.at(i).datum_off_ = 0xdead;
            exprs.at(i).res_buf_off_ = 0xdead;
          }
          ASSERT_EQ(OB_SUCCESS, replay_expr_frame_layout(exprs, empty, empty, dynamic ? frames : empty,
                                                         dynamic ? empty : frames, 0, 0, true));
          for (int64_t i = 0; i < 3; ++i) {
            ASSERT_EQ(i * item, exprs.at(i).datum_off_);
            ASSERT_EQ(offsets[i], exprs.at(i).res_buf_off_);
          }
        }
      }
    }
  }
}

TEST_F(TestExprSerializeV4, compact_layout_header_roundtrip)
{
  ObArenaAllocator allocator;
  ObExprFrameInfo info(allocator);
  ObExprFrameInfo partial(allocator, info.rt_exprs_);
  ObDASExprFrameLayout layout;
  ObExprFrameInfo decoded(allocator);
  char buf[128];
  for (const int64_t batch_size : {0, 8}) {
    layout.cg_batch_size_ = batch_size;
    for (const bool is_partial : {false, true, false}) {
      const ObExprFrameInfo &source = is_partial ? partial : info;
      layout.partial_frame_ = source.partial_frame_;
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, layout.serialize_meta(buf, sizeof(buf), pos, source));
      ASSERT_EQ(layout.meta_size(source), pos);
      int64_t read_pos = 0;
      int64_t first_field = -1;
      ASSERT_EQ(OB_SUCCESS, serialization::decode_vi64(buf, pos, read_pos, &first_field));
      ASSERT_EQ(batch_size, first_field);
      ObDASExprFrameLayout received;
      read_pos = 0;
      ASSERT_EQ(OB_SUCCESS, received.deserialize_meta(buf, pos, read_pos, decoded));
      ASSERT_EQ(pos, read_pos);
      ASSERT_EQ(batch_size, received.cg_batch_size_);
      ASSERT_EQ(is_partial, received.partial_frame_);
      ASSERT_EQ(is_partial, decoded.partial_frame_);
      ASSERT_FALSE(decoded.is_mark_serialize());
    }
  }
}

TEST_F(TestExprSerializeV4, all_vec_tc_init_is_supported)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    char hdr_buf[sizeof(VectorHeader)];
    VectorHeader &hdr = *reinterpret_cast<VectorHeader *>(hdr_buf);
    ObDatum datum;
    ObEvalInfo eval_info;
    for (int tc = 0; tc < MAX_VEC_TC; ++tc) {
      ASSERT_EQ(OB_SUCCESS, hdr.init_uniform_const_vector(static_cast<VecValueTypeClass>(tc), &datum, &eval_info))
        << "vec tc=" << tc;
    }
  }
}

TEST_F(TestExprSerializeV4, legacy_and_v4_expr_ref_codecs)
{
  int ret = OB_SUCCESS;
  lib::ContextParam param;
  param.set_mem_attr(OB_SERVER_TENANT_ID, "TestV4", ObCtxIds::WORK_AREA);
  CREATE_WITH_TEMP_CONTEXT(param)
  {
    const int64_t N = 130; // indexes 1..130: cross the 1-byte varint boundary (127)
    ObArray<ObExpr> exprs;
    ASSERT_EQ(OB_SUCCESS, exprs.prepare_allocate(N));
    ExprSerializeArrayGuard array_guard(&exprs);

    char buf[64];
    // The generic/legacy codec must stay fixed-i32 for rolling compatibility.
    int64_t pos = 0;
    ASSERT_EQ(4, serialization::encoded_length(static_cast<ObExpr *>(nullptr)));
    ASSERT_EQ(4, serialization::encoded_length(&exprs.at(0)));
    ASSERT_EQ(4, serialization::encoded_length(&exprs.at(126)));
    ASSERT_EQ(4, serialization::encoded_length(&exprs.at(127)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(buf, sizeof(buf), pos, static_cast<ObExpr *>(nullptr)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(buf, sizeof(buf), pos, &exprs.at(0)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(buf, sizeof(buf), pos, &exprs.at(126)));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(buf, sizeof(buf), pos, &exprs.at(127)));
    ASSERT_EQ(16, pos);
    int64_t dpos = 0;
    ObExpr *out = nullptr;
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, dpos, out));
    ASSERT_EQ(nullptr, out);
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, dpos, out));
    ASSERT_EQ(&exprs.at(0), out);
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, dpos, out));
    ASSERT_EQ(&exprs.at(126), out);
    ASSERT_EQ(OB_SUCCESS, serialization::decode(buf, pos, dpos, out));
    ASSERT_EQ(&exprs.at(127), out);
    ASSERT_EQ(pos, dpos);

    // The compact-v4 private codec keeps vi32 boundaries.
    ObExpr &src = exprs.at(N - 1);
    build_expr(src, T_FUN_SYS_CAST, ObInt32Type, 0, 10, 4, 4, 0, false);
    src.obj_meta_.set_collation_level(CS_LEVEL_NUMERIC);
    src.max_length_ = static_cast<int32_t>(src.res_buf_len_);
    src.arg_cnt_ = 4;
    src.args_ = static_cast<ObExpr **>(CURRENT_CONTEXT->get_arena_allocator().alloc(sizeof(ObExpr *) * src.arg_cnt_));
    ASSERT_NE(nullptr, src.args_);
    src.args_[0] = nullptr;
    src.args_[1] = &exprs.at(0);
    src.args_[2] = &exprs.at(126);
    src.args_[3] = &exprs.at(127);

    char v4_buf[1024];
    int64_t v4_pos = 0;
    ASSERT_EQ(OB_SUCCESS, encode_expr_v4(src, v4_buf, sizeof(v4_buf), v4_pos, false));
    ASSERT_EQ(v4_pos, expr_v4_size(src, false));
    int64_t parse_pos = 0;
    int64_t body_end = 0;
    int64_t bitmap = 0;
    ASSERT_EQ(OB_SUCCESS, decode_v4_test_prefix(v4_buf, v4_pos, parse_pos, body_end, bitmap));
    ASSERT_NE(0, bitmap & TEST_V4_HAS_ARGS);
    int32_t arg_cnt = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::decode_vi32(v4_buf, body_end, parse_pos, &arg_cnt));
    ASSERT_EQ(4, arg_cnt);
    const int64_t expected_ref_bytes[] = {1, 1, 1, 2};
    const int32_t expected_ref_values[] = {0, 1, 127, 128};
    for (int64_t i = 0; i < arg_cnt; ++i) {
      const int64_t ref_begin = parse_pos;
      int32_t ref = -1;
      ASSERT_EQ(OB_SUCCESS, serialization::decode_vi32(v4_buf, body_end, parse_pos, &ref));
      ASSERT_EQ(expected_ref_values[i], ref);
      ASSERT_EQ(expected_ref_bytes[i], parse_pos - ref_begin);
    }
    ASSERT_EQ(body_end, parse_pos);

    ObArray<ObExpr> decoded;
    ASSERT_EQ(OB_SUCCESS, decoded.prepare_allocate(N));
    array_guard.set(&decoded);
    int64_t v4_dpos = 0;
    ASSERT_EQ(OB_SUCCESS, decode_test_compact_expr(decoded.at(N - 1), v4_buf, v4_pos, v4_dpos));
    ASSERT_EQ(v4_pos, v4_dpos);
    ASSERT_EQ(nullptr, decoded.at(N - 1).args_[0]);
    ASSERT_EQ(&decoded.at(0), decoded.at(N - 1).args_[1]);
    ASSERT_EQ(&decoded.at(126), decoded.at(N - 1).args_[2]);
    ASSERT_EQ(&decoded.at(127), decoded.at(N - 1).args_[3]);
  }
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

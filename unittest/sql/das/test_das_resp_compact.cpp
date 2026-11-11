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

#define USING_LOG_PREFIX SQL
#include <gtest/gtest.h>
#include <vector>
#include <cstdlib>
#include <fstream>
#include <iterator>
#include "sql/das/ob_das_task.h"
#include "sql/das/ob_das_scan_op.h"
#include "sql/engine/basic/ob_chunk_datum_store_compact_header.h"
#include "lib/utility/ob_unis_versioned.h"
#include "sql/plan_cache/ob_plan_cache.h"
#include "share/rc/ob_tenant_base.h"
#include "observer/ob_req_time_service.h"
#include "lib/utility/ob_defer.h"

namespace oceanbase
{
using namespace common;
namespace sql
{

struct ScanResultTestContext
{
  ScanResultTestContext() : allocator_("ScanResultTest"), ctdef_(allocator_), op_(allocator_)
  {
    expr_.datum_meta_.type_ = ObVarcharType;
    ctdef_.pd_expr_spec_.max_batch_size_ = 1;
    op_.set_scan_ctdef(&ctdef_);
    op_.set_scan_rtdef(&rtdef_);
  }
  int set_columns(const int64_t count, const bool rich = false)
  {
    int ret = OB_SUCCESS;
    ctdef_.result_output_.reset();
    ret = ctdef_.result_output_.init(count);
    rtdef_.scan_flag_.enable_rich_format_ = rich;
    for (int64_t i = 0; OB_SUCC(ret) && i < count; ++i) { ret = ctdef_.result_output_.push_back(&expr_); }
    return ret;
  }
  ObArenaAllocator allocator_;
  ObExpr expr_;
  ObDASScanCtDef ctdef_;
  ObDASScanRtDef rtdef_;
  ObDASScanOp op_;
};

class FixedLensTestAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t size) override
  {
    ++alloc_count_;
    last_buffer_ = allocator_.alloc(size);
    return last_buffer_;
  }
  void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    allocator_.set_attr(attr);
    return alloc(size);
  }
  void free(void *ptr) override
  {
    allocator_.free(ptr);
  }
  int64_t alloc_count_ = 0;
  void *last_buffer_ = nullptr;

private:
  ObArenaAllocator allocator_;
};

TEST(TestDASRespCompact, scan_result_reuses_fixed_length_buffer)
{
  ScanResultTestContext ctx;
  FixedLensTestAllocator allocator;
  ObDASScanResult result;
  ASSERT_EQ(OB_SUCCESS, ctx.set_columns(3));
  ASSERT_EQ(OB_SUCCESS, result.init(ctx.op_, allocator));
  ASSERT_EQ(1, allocator.alloc_count_);
  void *first = allocator.last_buffer_;
  for (const int64_t count : {3, 1, 0, 2, 3}) {
    ASSERT_EQ(OB_SUCCESS, result.reuse());
    ASSERT_EQ(OB_SUCCESS, ctx.set_columns(count));
    ASSERT_EQ(OB_SUCCESS, result.init(ctx.op_, allocator));
    ASSERT_EQ(1, allocator.alloc_count_);
    ASSERT_EQ(first, allocator.last_buffer_);
  }
  ctx.expr_.datum_meta_.type_ = ObIntType;
  ctx.expr_.is_fixed_length_data_ = true;
  ctx.expr_.len_ = sizeof(int64_t);
  ASSERT_EQ(OB_SUCCESS, result.reuse());
  ASSERT_EQ(OB_SUCCESS, result.init(ctx.op_, allocator));
  ASSERT_EQ(1, allocator.alloc_count_);
  for (int64_t i = 0; i < 3; ++i) { ASSERT_EQ(ctx.expr_.get_fixed_length(), static_cast<uint32_t *>(first)[i]); }
  ASSERT_EQ(OB_SUCCESS, result.reuse());
  ASSERT_EQ(OB_SUCCESS, ctx.set_columns(4));
  ASSERT_EQ(OB_SUCCESS, result.init(ctx.op_, allocator));
  ASSERT_EQ(2, allocator.alloc_count_);
  ASSERT_NE(first, allocator.last_buffer_);
  result.reset();
  ASSERT_EQ(OB_SUCCESS, result.init(ctx.op_, allocator));
  ASSERT_EQ(3, allocator.alloc_count_);
}

TEST(TestDASRespCompact, scan_result_checks_nonempty_row_store_column_count)
{
  for (const bool rich : {false, true}) {
    for (const bool populated : {false, true}) {
      for (const int64_t received_columns : {1, 2}) {
        SCOPED_TRACE(::testing::Message()
                     << "rich=" << rich << " populated=" << populated << " received_columns=" << received_columns);
        ScanResultTestContext src_ctx;
        ScanResultTestContext dst_ctx;
        ASSERT_EQ(OB_SUCCESS, src_ctx.set_columns(1, rich));
        ASSERT_EQ(OB_SUCCESS, dst_ctx.set_columns(received_columns, rich));
        ObDASScanResult src;
        ObDASScanResult dst;
        ASSERT_EQ(OB_SUCCESS, src.init(src_ctx.op_, src_ctx.allocator_));
        // Exercise shrinking a reused buffer without retaining its old logical column count.
        ASSERT_EQ(OB_SUCCESS, dst_ctx.set_columns(3, rich));
        ASSERT_EQ(OB_SUCCESS, dst.init(dst_ctx.op_, dst_ctx.allocator_));
        ASSERT_EQ(OB_SUCCESS, dst.reuse());
        ASSERT_EQ(OB_SUCCESS, dst_ctx.set_columns(received_columns, rich));
        ASSERT_EQ(OB_SUCCESS, dst.init(dst_ctx.op_, dst_ctx.allocator_));
        if (populated) {
          ObDatum value;
          value.set_string("payload", 7);
          ASSERT_EQ(OB_SUCCESS, src.get_datum_store().add_row(&value, 1, 0, nullptr));
          ASSERT_EQ(OB_SUCCESS, src.get_datum_store().finish_add_row(false));
        }
        serialization::SerCompactEncodingGuard guard(true);
        std::vector<char> bytes(src.get_serialize_size());
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, src.serialize(bytes.data(), bytes.size(), pos));
        ASSERT_EQ(bytes.size(), pos);
        pos = 0;
        const int ret = dst.deserialize(bytes.data(), bytes.size(), pos);
        if (populated && !rich && received_columns != 1) {
          ASSERT_EQ(OB_DESERIALIZE_ERROR, ret);
          ASSERT_EQ(0, dst.get_datum_store().get_row_cnt());
        } else {
          ASSERT_EQ(OB_SUCCESS, ret);
          ASSERT_EQ(bytes.size(), pos);
          ASSERT_EQ(populated ? 1 : 0, dst.get_datum_store().get_row_cnt());
        }
      }
    }
  }
}

TEST(TestDASRespCompact, reused_scan_result_decodes_empty_retry_response)
{
  ScanResultTestContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_columns(2));
  ObDASScanResult dst;
  ASSERT_EQ(OB_SUCCESS, dst.init(ctx.op_, ctx.allocator_));
  ObDASTaskResp decoded;
  decoded.set_compact_das_response(true);
  ASSERT_EQ(OB_SUCCESS, decoded.add_op_result(&dst));

  // Keep the same receiver across a populated result, an empty error result,
  // and a successful retry. Each RPC gets a fresh sender-side result.
  for (int64_t attempt = 0; attempt < 3; ++attempt) {
    SCOPED_TRACE(::testing::Message() << "attempt=" << attempt);
    if (attempt > 0) { ASSERT_EQ(OB_SUCCESS, dst.reuse()); }
    ObDASScanResult src;
    ASSERT_EQ(OB_SUCCESS, src.init(ctx.op_, ctx.allocator_));
    if (attempt != 1) {
      ObDatum values[2];
      values[0].set_string("first", 5);
      values[1].set_string("second", 6);
      ASSERT_EQ(OB_SUCCESS, src.get_datum_store().add_row(values, 2, 0, nullptr));
      ASSERT_EQ(OB_SUCCESS, src.get_datum_store().finish_add_row(false));
    }
    ObDASTaskResp response;
    response.set_compact_das_response(true);
    response.set_err_code(attempt == 1 ? OB_NOT_MASTER : OB_SUCCESS);
    ASSERT_EQ(OB_SUCCESS, response.add_op_result(&src));
    std::vector<char> bytes(response.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, response.serialize(bytes.data(), bytes.size(), pos));
    ASSERT_EQ(bytes.size(), pos);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, decoded.deserialize(bytes.data(), bytes.size(), pos));
    ASSERT_EQ(bytes.size(), pos);
    ASSERT_EQ(response.get_err_code(), decoded.get_err_code());
    ASSERT_EQ(attempt == 1 ? 0 : 1, dst.get_datum_store().get_row_cnt());
    ASSERT_EQ(attempt == 1 ? -1 : 2, dst.get_datum_store().get_col_cnt());
    if (attempt != 1) {
      ObChunkDatumStore::Iterator iter;
      const ObChunkDatumStore::StoredRow *row = nullptr;
      ASSERT_EQ(OB_SUCCESS, dst.get_datum_store().begin(iter));
      ASSERT_EQ(OB_SUCCESS, iter.get_next_row(row));
      ASSERT_NE(nullptr, row);
      ASSERT_EQ(2, row->cnt_);
      ASSERT_EQ(ObString::make_string("first"), row->cells()[0].get_string());
      ASSERT_EQ(ObString::make_string("second"), row->cells()[1].get_string());
      ASSERT_EQ(OB_ITER_END, iter.get_next_row(row));
    }
  }
}

TEST(TestDASRespCompact, scan_result_reuse_clears_populated_store_column_count)
{
  ScanResultTestContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_columns(2));
  ObDASScanResult result;
  ASSERT_EQ(OB_SUCCESS, result.init(ctx.op_, ctx.allocator_));
  ObDatum values[2];
  values[0].set_string("first", 5);
  values[1].set_string("second", 6);
  ASSERT_EQ(OB_SUCCESS, result.get_datum_store().add_row(values, 2, 0, nullptr));
  ASSERT_EQ(2, result.get_datum_store().get_col_cnt());
  ASSERT_EQ(OB_SUCCESS, result.reuse());
  ASSERT_EQ(0, result.get_datum_store().get_row_cnt());
  ASSERT_EQ(-1, result.get_datum_store().get_col_cnt());
  ASSERT_EQ(OB_SUCCESS, result.get_datum_store().add_row(values, 1, 0, nullptr));
  ASSERT_EQ(1, result.get_datum_store().get_col_cnt());
}

TEST(TestDASRespCompact, compact_response_round_trip)
{
  ObAddr ctrl;
  ObAddr runner;
  ASSERT_TRUE(ctrl.set_ip_addr("127.0.0.1", 2881));
  ASSERT_TRUE(runner.set_ip_addr("127.0.0.1", 2882));
  ObDASTaskResp legacy_resp;
  legacy_resp.set_ctrl_svr(ctrl);
  legacy_resp.set_runner_svr(runner);
  legacy_resp.set_has_more(true);
  const int64_t legacy_size = serialization::encoded_length(legacy_resp);
  std::vector<char> legacy_wire(legacy_size);
  int64_t legacy_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(legacy_wire.data(), legacy_wire.size(), legacy_pos, legacy_resp));

  ObDASTaskResp compact_resp;
  compact_resp.set_ctrl_svr(ctrl);
  compact_resp.set_runner_svr(runner);
  compact_resp.set_has_more(true);
  compact_resp.set_compact_das_response(true);
  const int64_t compact_size = serialization::encoded_length(compact_resp);
  ASSERT_EQ(compact_size, legacy_size);
  std::vector<char> compact_wire(compact_size);
  int64_t compact_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(compact_wire.data(), compact_wire.size(), compact_pos, compact_resp));
  ASSERT_EQ(compact_size, compact_pos);
  ASSERT_EQ(legacy_wire, compact_wire);
  int64_t short_pos = 0;
  ASSERT_NE(OB_SUCCESS, serialization::encode(compact_wire.data(), compact_size - 1, short_pos, compact_resp));
  ASSERT_FALSE(serialization::using_v4_encoding());

  ObDASTaskResp decoded_resp;
  decoded_resp.set_compact_das_response(true);
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(compact_wire.data(), compact_wire.size(), decode_pos, decoded_resp));
  ASSERT_EQ(compact_size, decode_pos);
  ASSERT_TRUE(decoded_resp.has_more());
  ASSERT_EQ(ctrl, decoded_resp.get_ctrl_svr());
  ASSERT_EQ(runner, decoded_resp.get_runner_svr());
  ASSERT_EQ(OB_SUCCESS, decoded_resp.get_err_code());
  ASSERT_FALSE(decoded_resp.get_trans_result().is_incomplete());
  ASSERT_TRUE(decoded_resp.get_trans_result().get_touched_ls().empty());
  ASSERT_FALSE(serialization::use_compact_expr_ptr());
}

TEST(TestDASRespCompact, error_warning_and_transaction_fields_are_preserved)
{
  ObDASTaskResp src;
  ObAddr ctrl;
  ObAddr runner;
  ASSERT_TRUE(ctrl.set_ip_addr("127.0.0.1", 2881));
  ASSERT_TRUE(runner.set_ip_addr("127.0.0.1", 2882));
  src.set_ctrl_svr(ctrl);
  src.set_runner_svr(runner);
  src.set_err_code(OB_ERR_PRIMARY_KEY_DUPLICATE);
  src.store_err_msg(ObString::make_string("duplicate key"));
  ObWarningBuffer::WarningItem warning;
  warning.set("response warning");
  warning.set_code(1234);
  ASSERT_EQ(OB_SUCCESS, src.get_rcode().warnings_.push_back(warning));
  src.get_trans_result().set_incomplete();
  ASSERT_EQ(OB_SUCCESS, src.get_trans_result().add_touched_ls(share::ObLSID(123)));

  std::vector<char> legacy;
  for (const bool compact : {false, true}) {
    src.set_compact_das_response(compact);
    std::vector<char> bytes(src.get_serialize_size());
    int64_t pos = 0;
    ASSERT_EQ(OB_SUCCESS, src.serialize(bytes.data(), bytes.size(), pos));
    ASSERT_EQ(bytes.size(), pos);
    if (!compact) {
      legacy = bytes;
    } else {
      ASSERT_EQ(legacy, bytes);
    }
    ObDASTaskResp dst;
    dst.set_compact_das_response(compact);
    pos = 0;
    ASSERT_EQ(OB_SUCCESS, dst.deserialize(bytes.data(), bytes.size(), pos));
    ASSERT_EQ(bytes.size(), pos);
    ASSERT_EQ(ctrl, dst.get_ctrl_svr());
    ASSERT_EQ(runner, dst.get_runner_svr());
    ASSERT_EQ(OB_ERR_PRIMARY_KEY_DUPLICATE, dst.get_err_code());
    ASSERT_STREQ("duplicate key", dst.get_err_msg());
    ASSERT_EQ(1, dst.get_rcode().warnings_.count());
    ASSERT_EQ(1234, dst.get_rcode().warnings_.at(0).get_code());
    ASSERT_STREQ("response warning", dst.get_rcode().warnings_.at(0).get());
    ASSERT_TRUE(dst.get_trans_result().is_incomplete());
    ASSERT_EQ(1, dst.get_trans_result().get_touched_ls().count());
    ASSERT_EQ(share::ObLSID(123), dst.get_trans_result().get_touched_ls().at(0));
  }
}

TEST(TestDASRespCompact, cache_guard_swap_releases_only_the_local_reference)
{
  observer::ObReqTimeGuard request_guard;
  ObLibCacheRegister::register_cache_objs();
  share::ObTenantBase tenant(OB_SERVER_TENANT_ID);
  auto *saved_tenant = share::ObTenantEnv::get_tenant();
  ObPlanCache cache;
  tenant.set(&cache);
  share::ObTenantEnv::set_tenant(&tenant);
  DEFER(tenant.set<ObPlanCache *>(nullptr); share::ObTenantEnv::set_tenant(saved_tenant));
  auto &manager = cache.get_cache_obj_mgr();
  ASSERT_EQ(OB_SUCCESS, manager.init(16, OB_SERVER_TENANT_ID));
  lib::MemoryContext parent = ROOT_CONTEXT;
  {
    ObCacheObjGuard first(DAS_DESER_HANDLE);
    ObCacheObjGuard second(DAS_DESER_HANDLE);
    ObCacheObjGuard destination(DAS_DESER_HANDLE);
    ASSERT_EQ(OB_SUCCESS, manager.alloc(first, NS_DAS_DESER, OB_SERVER_TENANT_ID, parent));
    ASSERT_EQ(OB_SUCCESS, manager.alloc(second, NS_DAS_DESER, OB_SERVER_TENANT_ID, parent));
    auto *first_obj = first.get_cache_obj();
    auto *second_obj = second.get_cache_obj();
    ASSERT_NE(nullptr, first_obj);
    ASSERT_NE(nullptr, second_obj);
    ASSERT_EQ(OB_SUCCESS, manager.add_cache_obj(first_obj));
    ASSERT_EQ(OB_SUCCESS, manager.add_cache_obj(second_obj));
    first_obj->set_added_lc(true);
    second_obj->set_added_lc(true);
    const int64_t first_refs = first_obj->get_ref_count();
    const int64_t second_refs = second_obj->get_ref_count();
    {
      ObCacheObjGuard local(DAS_DESER_HANDLE);
      ASSERT_EQ(OB_SUCCESS, cache.ref_cache_obj(first_obj->get_object_id(), local));
      destination.swap(local);
      ASSERT_EQ(nullptr, local.get_cache_obj());
    }
    ASSERT_EQ(first_obj, destination.get_cache_obj());
    ASSERT_EQ(first_refs + 1, first_obj->get_ref_count());
    {
      ObCacheObjGuard local(DAS_DESER_HANDLE);
      ASSERT_EQ(OB_SUCCESS, cache.ref_cache_obj(second_obj->get_object_id(), local));
      destination.swap(local);
      ASSERT_EQ(first_obj, local.get_cache_obj());
      ASSERT_EQ(first_refs + 1, first_obj->get_ref_count());
    }
    ASSERT_EQ(first_refs, first_obj->get_ref_count());
    ASSERT_EQ(second_refs + 1, second_obj->get_ref_count());
    {
      ObCacheObjGuard local(DAS_DESER_HANDLE);
      ASSERT_EQ(OB_SUCCESS, cache.ref_cache_obj(second_obj->get_object_id(), local));
      destination.swap(local);
      ASSERT_EQ(second_obj, local.get_cache_obj());
    }
    ASSERT_EQ(second_obj, destination.get_cache_obj());
    ASSERT_EQ(second_refs + 1, second_obj->get_ref_count());
    ASSERT_EQ(OB_SUCCESS, manager.get_cache_obj_map().erase_refactored(first_obj->get_object_id()));
    ASSERT_EQ(OB_SUCCESS, manager.get_cache_obj_map().erase_refactored(second_obj->get_object_id()));
  }
  ASSERT_EQ(0, manager.get_alloc_cache_obj_map().size());
}

TEST(TestDASRespCompact, compact_data_fetch_round_trip_without_format_bit)
{
  ObDASDataFetchRes src;
  ASSERT_EQ(OB_SUCCESS, src.init(OB_SERVER_TENANT_ID, 42));
  ASSERT_EQ(OB_SUCCESS, src.get_datum_store().init(INT64_MAX, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                                   "DASFetchTest", false));
  src.set_has_more(true);
  src.set_enable_rich_format(false);
  src.set_compact_das_response(true);

  const int64_t size = serialization::encoded_length(src);
  std::vector<char> wire(size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::encode(wire.data(), wire.size(), pos, src));
  ASSERT_EQ(size, pos);
  ASSERT_FALSE(serialization::using_v4_encoding());

  ObDASDataFetchRes dst;
  ASSERT_EQ(ObDASDataFetchRes::unis_declared_version(), static_cast<uint8_t>(wire[0]));
  serialization::UnisRecordView outer;
  serialization::UnisRecordView store;
  int64_t store_pos = 0;
  ASSERT_EQ(OB_SUCCESS, outer.decode(wire.data(), wire.size(), store_pos));
  ASSERT_EQ(OB_SUCCESS, store.decode(wire.data(), outer.body_end(), store_pos));
  ASSERT_EQ(ObChunkDatumStoreCompactHeader::unis_declared_version(), store.version());
  int64_t decode_pos = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(wire.data(), wire.size(), decode_pos, dst));
  ASSERT_EQ(size, decode_pos);
  ASSERT_EQ(42, dst.get_task_id());
  ASSERT_TRUE(dst.has_more());
  ASSERT_FALSE(serialization::use_compact_expr_ptr());
}

TEST(TestDASRespCompact, rich_fetch_init_prepares_store_record)
{
  ObDASDataFetchRes src;
  ASSERT_EQ(OB_SUCCESS, src.init(OB_SERVER_TENANT_ID, 42));
  src.set_enable_rich_format(true);
  src.set_compact_das_response(true);
  std::vector<char> bytes(src.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, src.serialize(bytes.data(), bytes.size(), pos));
  ASSERT_EQ(bytes.size(), pos);
  ObDASDataFetchRes dst;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, dst.deserialize(bytes.data(), bytes.size(), pos));
  ASSERT_EQ(bytes.size(), pos);
  ASSERT_EQ(42, dst.get_task_id());
  ASSERT_EQ(0, dst.get_datum_store().get_row_cnt());
}

static int locate_response_store_record(const std::vector<char> &bytes, serialization::UnisRecordView &store,
                                        int64_t &store_start, const bool has_base = true)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  serialization::UnisRecordView outer;
  serialization::UnisRecordView base;
  if (OB_FAIL(outer.decode(bytes.data(), bytes.size(), pos))) {
  } else if (has_base && OB_FAIL(base.decode(bytes.data(), outer.body_end(), pos))) {
  } else {
    if (has_base) { pos = base.body_end(); }
    store_start = pos;
    ret = store.decode(bytes.data(), outer.body_end(), pos);
  }
  return ret;
}

// The same test executable can exchange fixtures while linked to the saved
// baseline library and the candidate library. No copied legacy decoder is used.
TEST(TestDASRespCompact, legacy_scan_result_cross_library)
{
  serialization::SerCompactEncodingGuard legacy_guard(false);
  const char *write_dir = std::getenv("OB_SCAN_WIRE_WRITE_DIR");
  const char *read_dir = std::getenv("OB_SCAN_WIRE_READ_DIR");
  for (const bool rich : {false, true}) {
    for (const bool populated : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "rich=" << rich << " populated=" << populated);
      ObDASScanResult src;
      ASSERT_EQ(OB_SUCCESS, src.get_datum_store().init(INT64_MAX, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                                       "LegacyScan", false));
      if (populated) {
        ObDatum value;
        value.set_string("legacy payload", 14);
        ASSERT_EQ(OB_SUCCESS, src.get_datum_store().add_row(&value, 1, 0, nullptr));
        ASSERT_EQ(OB_SUCCESS, src.get_datum_store().finish_add_row(false));
        ObExpr column;
        column.datum_meta_.type_ = ObVarcharType;
        ObArray<ObExpr *> columns;
        ASSERT_EQ(OB_SUCCESS, columns.push_back(&column));
        ASSERT_EQ(OB_SUCCESS, src.get_vec_row_store().init(columns, 1, ObMemAttr(OB_SERVER_TENANT_ID, "LegacyScan"),
                                                            INT64_MAX, false, 0, NONE_COMPRESSOR, false));
        const RowMeta &meta = src.get_vec_row_store().get_row_meta();
        std::vector<char> row_bytes(meta.get_row_fixed_size(), 0);
        ObCompactRow *row = reinterpret_cast<ObCompactRow *>(row_bytes.data());
        row->init(meta);
        row->set_null(meta, 0);
        row->set_row_size(row_bytes.size());
        ObCompactRow *stored = nullptr;
        ASSERT_EQ(OB_SUCCESS, src.get_vec_row_store().add_row(row, stored));
      }
      src.add_io_read_bytes(101);
      src.add_ssstore_read_bytes(202);
      src.add_base_read_row_cnt(303);
      src.add_delta_read_row_cnt(404);
      std::vector<char> bytes(src.get_serialize_size());
      int64_t pos = 0;
      ASSERT_EQ(OB_SUCCESS, src.serialize(bytes.data(), bytes.size(), pos));
      ASSERT_EQ(bytes.size(), pos);
      serialization::UnisRecordView store;
      int64_t store_start = 0;
      ASSERT_EQ(OB_SUCCESS, locate_response_store_record(bytes, store, store_start));
      pos = store.body_end();
      ASSERT_EQ(OB_SUCCESS, serialization::encode(bytes.data(), bytes.size(), pos, rich));
      const std::string name = std::string("/scan_") + (rich ? "rich_" : "row_")
                             + (populated ? "data.bin" : "empty.bin");
      if (write_dir != nullptr) {
        std::ofstream output(std::string(write_dir) + name, std::ios::binary);
        ASSERT_TRUE(output.good());
        output.write(bytes.data(), bytes.size());
        ASSERT_TRUE(output.good());
      }
      if (read_dir != nullptr) {
        std::ifstream input(std::string(read_dir) + name, std::ios::binary);
        ASSERT_TRUE(input.good());
        std::vector<char> baseline((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
        ASSERT_EQ(bytes, baseline);
        bytes.swap(baseline);
      }
      ObDASScanResult dst;
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, dst.deserialize(bytes.data(), bytes.size(), pos));
      ASSERT_EQ(bytes.size(), pos);
      ASSERT_EQ(101, dst.get_io_read_bytes());
      ASSERT_EQ(202, dst.get_ssstore_read_bytes());
      ASSERT_EQ(303, dst.get_base_read_row_cnt());
      ASSERT_EQ(404, dst.get_delta_read_row_cnt());
      ASSERT_EQ(populated ? 1 : 0, dst.get_datum_store().get_row_cnt());
      ASSERT_EQ(populated ? 1 : 0, dst.get_vec_row_store().get_row_cnt());
      if (populated) {
        ObChunkDatumStore::Iterator datum_iter;
        const ObChunkDatumStore::StoredRow *datum_row = nullptr;
        ASSERT_EQ(OB_SUCCESS, dst.get_datum_store().begin(datum_iter));
        ASSERT_EQ(OB_SUCCESS, datum_iter.get_next_row(datum_row));
        ASSERT_EQ(ObString::make_string("legacy payload"), datum_row->cells()[0].get_string());
        ASSERT_EQ(OB_ITER_END, datum_iter.get_next_row(datum_row));
        ObTempRowStore::Iterator vec_iter;
        const ObCompactRow *vec_row = nullptr;
        int64_t read_rows = 0;
        ASSERT_EQ(OB_SUCCESS, dst.get_vec_row_store().begin(vec_iter));
        ASSERT_EQ(OB_SUCCESS, vec_iter.get_next_batch(1, read_rows, &vec_row));
        ASSERT_EQ(1, read_rows);
        ASSERT_TRUE(vec_row->is_null(0));
      }
      // append_block() reconstructs a right-sized block on decode, so its
      // allocation header need not match the producer's header on re-encode.
      // Cross-library producer bytes are compared above; check the decoded
      // data, trailing fields, format bit and size independently here.
      std::vector<char> repeated(dst.get_serialize_size());
      pos = 0;
      ASSERT_EQ(OB_SUCCESS, dst.serialize(repeated.data(), repeated.size(), pos));
      ASSERT_EQ(repeated.size(), pos);
      ASSERT_EQ(OB_SUCCESS, locate_response_store_record(repeated, store, store_start));
      pos = store.body_end();
      bool decoded_rich = false;
      ASSERT_EQ(OB_SUCCESS, serialization::decode(repeated.data(), repeated.size(), pos, decoded_rich));
      ASSERT_EQ(rich, decoded_rich);
    }
  }
}

TEST(TestDASRespCompact, response_record_versions_are_independent_of_pointer_context)
{
  ScanResultTestContext scan_ctx;
  ASSERT_EQ(OB_SUCCESS, scan_ctx.set_columns(1));
  ObAddr server;
  ASSERT_TRUE(server.set_ip_addr("127.0.0.1", 2881));
  for (const bool rich : {false, true}) {
    for (const bool compact : {false, true}) {
      SCOPED_TRACE(::testing::Message() << "rich=" << rich << " compact=" << compact);
      ObDASScanResult src_scan;
      ASSERT_EQ(OB_SUCCESS, src_scan.init(scan_ctx.op_, scan_ctx.allocator_));
      if (rich) {
        serialization::SerCompactEncodingGuard legacy_guard(false);
        std::vector<char> legacy(src_scan.get_serialize_size());
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, src_scan.serialize(legacy.data(), legacy.size(), pos));
        ASSERT_EQ(legacy.size(), pos);
        serialization::UnisRecordView store;
        int64_t store_start = 0;
        ASSERT_EQ(OB_SUCCESS, locate_response_store_record(legacy, store, store_start));
        pos = store.body_end();
        ASSERT_EQ(OB_SUCCESS, serialization::encode(legacy.data(), legacy.size(), pos, true));
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, src_scan.deserialize(legacy.data(), legacy.size(), pos));
        ASSERT_EQ(legacy.size(), pos);
      }
      if (!rich) {
        const int64_t number = 42;
        ObDatum value;
        value.set_string(reinterpret_cast<const char *>(&number), sizeof(number));
        ASSERT_EQ(OB_SUCCESS, src_scan.get_datum_store().add_row(&value, 1, 0, nullptr));
        ASSERT_EQ(OB_SUCCESS, src_scan.get_datum_store().finish_add_row(false));
      }
      src_scan.add_io_read_bytes(101);
      src_scan.add_ssstore_read_bytes(202);
      src_scan.add_base_read_row_cnt(303);
      src_scan.add_delta_read_row_cnt(404);
      ObDASDataFetchRes src_fetch;
      ASSERT_EQ(OB_SUCCESS, src_fetch.init(OB_SERVER_TENANT_ID, 42));
      ASSERT_EQ(OB_SUCCESS, src_fetch.get_datum_store().init(INT64_MAX, OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID,
                                                             "DASFetchTest", false));
      src_fetch.set_compact_das_response(compact);
      src_fetch.set_enable_rich_format(rich);
      src_fetch.set_has_more(true);
      src_fetch.io_read_bytes_ = 505;
      if (!rich) {
        const int64_t number = 7;
        ObDatum value;
        value.set_string(reinterpret_cast<const char *>(&number), sizeof(number));
        ASSERT_EQ(OB_SUCCESS, src_fetch.get_datum_store().add_row(&value, 1, 0, nullptr));
        ASSERT_EQ(OB_SUCCESS, src_fetch.get_datum_store().finish_add_row(false));
      }
      std::vector<char> scan_bytes;
      std::vector<char> fetch_bytes;
      {
        serialization::SerCompactEncodingGuard encoding_guard(compact);
        scan_bytes.resize(src_scan.get_serialize_size());
        fetch_bytes.resize(src_fetch.get_serialize_size());
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, src_scan.serialize(scan_bytes.data(), scan_bytes.size(), pos));
        ASSERT_EQ(scan_bytes.size(), pos);
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, src_fetch.serialize(fetch_bytes.data(), fetch_bytes.size(), pos));
        ASSERT_EQ(fetch_bytes.size(), pos);
      }
      ASSERT_EQ(ObDASScanResult::unis_declared_version(), static_cast<uint8_t>(scan_bytes[0]));
      serialization::UnisRecordView store;
      int64_t store_start = 0;
      ASSERT_EQ(OB_SUCCESS, locate_response_store_record(scan_bytes, store, store_start));
      ASSERT_EQ(compact ? ObChunkDatumStoreCompactHeader::unis_declared_version() :
                          ObChunkDatumStore::unis_declared_version(),
                store.version());
      bool decoded_rich = false;
      int64_t rich_pos = store.body_end();
      ASSERT_EQ(OB_SUCCESS, serialization::decode(scan_bytes.data(), scan_bytes.size(), rich_pos, decoded_rich));
      ASSERT_EQ(rich, decoded_rich) << "store_start=" << store_start << " store_end=" << store.body_end()
                                    << " total=" << scan_bytes.size()
                                    << " flag_byte=" << static_cast<int>(scan_bytes[store.body_end()]);
      ASSERT_EQ(ObDASDataFetchRes::unis_declared_version(), static_cast<uint8_t>(fetch_bytes[0]));
      serialization::UnisRecordView fetch_store;
      int64_t fetch_store_start = 0;
      ASSERT_EQ(OB_SUCCESS, locate_response_store_record(fetch_bytes, fetch_store, fetch_store_start, false));
      ASSERT_EQ(compact ? ObChunkDatumStoreCompactHeader::unis_declared_version() :
                          ObChunkDatumStore::unis_declared_version(),
                fetch_store.version());
      ObDASTaskResp src_resp;
      src_resp.set_ctrl_svr(server);
      src_resp.set_runner_svr(server);
      src_resp.set_compact_das_response(compact);
      ASSERT_EQ(OB_SUCCESS, src_resp.add_op_result(&src_scan));
      std::vector<char> resp_bytes(src_resp.get_serialize_size());
      int64_t resp_pos = 0;
      ASSERT_EQ(OB_SUCCESS, src_resp.serialize(resp_bytes.data(), resp_bytes.size(), resp_pos));
      ASSERT_EQ(resp_bytes.size(), resp_pos);
      for (const bool compact_ptr : {false, true}) {
        serialization::CompactExprPtrGuard ptr_guard(compact_ptr);
        ObDASScanResult dst_scan;
        ASSERT_EQ(OB_SUCCESS, dst_scan.init(scan_ctx.op_, scan_ctx.allocator_));
        ObDASDataFetchRes dst_fetch;
        dst_fetch.set_compact_das_response(!compact);
        int64_t pos = 0;
        ASSERT_EQ(OB_SUCCESS, dst_scan.deserialize(scan_bytes.data(), scan_bytes.size(), pos));
        ASSERT_EQ(scan_bytes.size(), pos);
        ASSERT_EQ(101, dst_scan.get_io_read_bytes());
        ASSERT_EQ(202, dst_scan.get_ssstore_read_bytes());
        ASSERT_EQ(303, dst_scan.get_base_read_row_cnt());
        ASSERT_EQ(404, dst_scan.get_delta_read_row_cnt());
        ASSERT_EQ(rich ? 0 : 1, dst_scan.get_datum_store().get_row_cnt());
        {
          serialization::SerCompactEncodingGuard encoding_guard(compact);
          std::vector<char> repeated(dst_scan.get_serialize_size());
          int64_t repeated_pos = 0;
          ASSERT_EQ(OB_SUCCESS, dst_scan.serialize(repeated.data(), repeated.size(), repeated_pos));
          ASSERT_EQ(repeated.size(), repeated_pos);
          ASSERT_EQ(scan_bytes, repeated);
        }
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, dst_fetch.deserialize(fetch_bytes.data(), fetch_bytes.size(), pos));
        ASSERT_EQ(fetch_bytes.size(), pos);
        ASSERT_EQ(42, dst_fetch.get_task_id());
        ASSERT_TRUE(dst_fetch.has_more());
        ASSERT_EQ(505, dst_fetch.io_read_bytes_);
        ASSERT_EQ(rich ? 0 : 1, dst_fetch.get_datum_store().get_row_cnt());
        ASSERT_EQ(!compact, dst_fetch.use_compact_das_response());
        {
          serialization::SerCompactEncodingGuard encoding_guard(!compact);
          dst_fetch.set_compact_das_response(compact);
          std::vector<char> repeated(dst_fetch.get_serialize_size());
          int64_t repeated_pos = 0;
          ASSERT_EQ(OB_SUCCESS, dst_fetch.serialize(repeated.data(), repeated.size(), repeated_pos));
          ASSERT_EQ(repeated.size(), repeated_pos);
          ASSERT_EQ(fetch_bytes, repeated);
          ASSERT_EQ(!compact, serialization::using_v4_encoding());
        }
        ObDASScanResult nested_scan;
        ASSERT_EQ(OB_SUCCESS, nested_scan.init(scan_ctx.op_, scan_ctx.allocator_));
        ObDASTaskResp dst_resp;
        dst_resp.set_compact_das_response(!compact);
        ASSERT_EQ(OB_SUCCESS, dst_resp.add_op_result(&nested_scan));
        pos = 0;
        ASSERT_EQ(OB_SUCCESS, dst_resp.deserialize(resp_bytes.data(), resp_bytes.size(), pos));
        ASSERT_EQ(resp_bytes.size(), pos);
        ASSERT_EQ(101, nested_scan.get_io_read_bytes());
        ASSERT_EQ(303, nested_scan.get_base_read_row_cnt());
        ASSERT_EQ(compact_ptr, serialization::use_compact_expr_ptr());
        ASSERT_FALSE(serialization::using_v4_encoding());
      }
      scan_bytes[store_start] = static_cast<char>(ObChunkDatumStoreCompactHeader::unis_declared_version() + 1);
      fetch_bytes[fetch_store_start] = static_cast<char>(ObChunkDatumStoreCompactHeader::unis_declared_version() + 1);
      ObDASScanResult bad_scan;
      ObDASDataFetchRes bad_fetch;
      int64_t pos = 0;
      ASSERT_EQ(OB_NOT_SUPPORTED, bad_scan.deserialize(scan_bytes.data(), scan_bytes.size(), pos));
      pos = 0;
      ASSERT_EQ(OB_NOT_SUPPORTED, bad_fetch.deserialize(fetch_bytes.data(), fetch_bytes.size(), pos));
    }
  }
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

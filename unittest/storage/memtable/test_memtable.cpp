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

#include "storage/memtable/ob_memtable.h"
#include "storage/memtable/ob_memtable_iterator.h"
#include "storage/memtable/ob_memtable_mutator.h"

#include "common/cell/ob_cell_reader.h"
#include "lib/allocator/page_arena.h"
#include "lib/container/ob_se_array.h"
#include "storage/ob_i_store.h"
#include "share/ob_srv_rpc_proxy.h"

#include "utils_rowkey_builder.h"
#include "utils_mod_allocator.h"
//#include "utils_mock_ctx.h"
#include "utils_mock_row.h"

#include "../mockcontainer/mock_ob_iterator.h"
#include <gtest/gtest.h>

namespace oceanbase {
namespace unittest {
using namespace oceanbase::common;
using namespace oceanbase::memtable;

ObMemtableCtxFactory f;

static uint64_t MT_TABLE_ID = combine_id(1, 3001);

void test_redo_log(ObIMemtableCtx& mem_ctx, const uint64_t index_id, const ObStoreRowkey& rowkey,
    const storage::ObRowDml dml_type, ObMemtable& replayed_mt, const int64_t trans_version)
{
  const int64_t REDO_BUFFER_SIZE = 2L * 1024L * 1024L;
  char* redo_log_buffer = new char[REDO_BUFFER_SIZE];
  int64_t sel_pos = 0;
  int ret = mem_ctx.fill_redo_log(redo_log_buffer, REDO_BUFFER_SIZE, sel_pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator allocator(0);
  ObMemtableMutatorIterator mmi;

  int64_t des_pos = 0;
  ret = mmi.deserialize(redo_log_buffer, sel_pos, des_pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t res_index_id = 0;
  ObStoreRowkey res_rowkey;
  int64_t schema_version = 0;
  ObRowData new_row;
  storage::ObRowDml res_dml_type = T_DML_UNKNOWN;
  uint32_t res_modify_count = 0;

  ret = mmi.get_next_row(res_index_id, res_rowkey, schema_version, new_row, res_dml_type, res_modify_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(index_id, res_index_id);
  EXPECT_EQ(rowkey, res_rowkey);
  EXPECT_EQ(dml_type, res_dml_type);
  EXPECT_EQ(trans_version - 1, res_modify_count);

  ret = mmi.get_next_row(res_index_id, res_rowkey, schema_version, new_row, res_dml_type, res_modify_count);
  EXPECT_EQ(OB_ITER_END, ret);

  storage::ObStoreCtx ctx;
  ctx.mem_ctx_ = f.alloc();
  ret = replayed_mt.replay(ctx, redo_log_buffer, sel_pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  ctx.mem_ctx_->trans_replay_end(true, trans_version);
  f.free(ctx.mem_ctx_);
  free(redo_log_buffer);
}

void test_redo_log_row_pending(ObIMemtableCtx& mem_ctx, const uint64_t index_id, const ObStoreRowkey& rowkey,
    const storage::ObRowDml dml_type, ObMemtable& replayed_mt, const int64_t trans_version)
{
  const int64_t REDO_BUFFER_SIZE = 2L * 1024L * 1024L;
  char* redo_log_buffer = new char[REDO_BUFFER_SIZE];
  int64_t sel_pos = 0;
  int ret = mem_ctx.fill_redo_log(redo_log_buffer, REDO_BUFFER_SIZE, sel_pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  ObArenaAllocator allocator(0);
  ObMemtableMutatorIterator mmi;

  int64_t des_pos = 0;
  ret = mmi.deserialize(redo_log_buffer, sel_pos, des_pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  uint64_t res_index_id = 0;
  ObStoreRowkey res_rowkey;
  int64_t schema_version = 0;
  ObRowData new_row;
  storage::ObRowDml res_dml_type = T_DML_UNKNOWN;
  uint32_t res_modify_count = 0;

  ret = mmi.get_next_row(res_index_id, res_rowkey, schema_version, new_row, res_dml_type, res_modify_count);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(index_id, res_index_id);
  EXPECT_EQ(rowkey, res_rowkey);
  EXPECT_EQ(dml_type, res_dml_type);
  EXPECT_EQ(trans_version - 1, res_modify_count);

  ret = mmi.get_next_row(res_index_id, res_rowkey, schema_version, new_row, res_dml_type, res_modify_count);
  EXPECT_EQ(OB_ITER_END, ret);

  storage::ObStoreCtx ctx;
  ctx.mem_ctx_ = f.alloc();
  ret = replayed_mt.replay(ctx, redo_log_buffer, sel_pos);
  EXPECT_EQ(OB_SUCCESS, ret);
  // Replay to commit.
  ret = ctx.mem_ctx_->replay_to_commit();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = ctx.mem_ctx_->replay_to_commit();
  EXPECT_EQ(OB_SUCCESS, ret);
  // Commit it.
  ctx.mem_ctx_->trans_end(true, trans_version);
  f.free(ctx.mem_ctx_);
  free(redo_log_buffer);
}

void test_mt_query(RK& rk, const ObIArray<share::schema::ObColDesc>& columns, ObMemtable& mt)
{
  const ObStoreRow* got_row = NULL;
  int ret = OB_SUCCESS;

  ObQueryFlag flag;
  ObStoreCtx ctx;
  ctx.mem_ctx_ = f.alloc();
  ctx.mem_ctx_->trans_begin();
  ctx.mem_ctx_->sub_trans_begin(1000, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.get(ctx, flag, 1001, rk.get_rowkey(), columns, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_DEL_ROW, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*got_row));
  mt.revert_row(got_row);

  ObStoreRange range;
  range.start_key_ = rk.get_rowkey();
  range.end_key_ = rk.get_rowkey();
  range.border_flag_.set_inclusive_start();
  range.border_flag_.set_inclusive_end();
  storage::ObStoreRowIterator* iter = NULL;
  ret = mt.scan(ctx, flag, 1001, range, columns, iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* scan_row = NULL;
  ret = iter->get_next_row(scan_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_DEL_ROW, scan_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), scan_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(scan_row->row_val_.cells_, scan_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*scan_row));
  ret = iter->get_next_row(scan_row);
  EXPECT_EQ(OB_ITER_END, ret);
  mt.revert_iter(iter);

  ObSEArray<ObStoreRowkey, 64> rowkeys;
  rowkeys.push_back(rk.get_rowkey());
  ret = mt.multi_get(ctx, flag, 1001, rowkeys, columns, iter);
  EXPECT_EQ(OB_SUCCESS, ret);
  const ObStoreRow* mget_row = NULL;
  ret = iter->get_next_row(mget_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_DEL_ROW, mget_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), mget_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(mget_row->row_val_.cells_, mget_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*mget_row));
  ret = iter->get_next_row(mget_row);
  EXPECT_EQ(OB_ITER_END, ret);
  mt.revert_iter(iter);
  ctx.mem_ctx_->trans_end(true, 1000);
  f.free(ctx.mem_ctx_);
}

int init_tenant_mgr()
{
  ObTenantManager& tm = ObTenantManager::get_instance();
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  ObReqTransport req_transport(NULL, NULL);
  obrpc::ObSrvRpcProxy rpc_proxy;
  int ret = tm.init(self, rpc_proxy, &req_transport, &ObServerConfig::get_instance());
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = tm.add_tenant(OB_SYS_TENANT_ID);
  EXPECT_EQ(OB_SUCCESS, ret);
  const int64_t ulmt = 16LL << 30;
  const int64_t llmt = 8LL << 30;
  ret = tm.set_tenant_mem_limit(OB_SYS_TENANT_ID, ulmt, llmt);
  EXPECT_EQ(OB_SUCCESS, ret);
  return OB_SUCCESS;
}

TEST(TestObMemtable, smoke_test)
{
  static const ObPartitionKey PKEY(MT_TABLE_ID, 1, 1);

  ObModAllocator allocator;
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;

  ret = mt.init(PKEY);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(PKEY);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);

  wctx.mem_ctx_ = f.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, 1001, rk.get_rowkey().get_obj_cnt(), columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  test_redo_log(*wctx.mem_ctx_, 1001, rk.get_rowkey(), T_DML_INSERT, replayed_mt, 1);
  wctx.mem_ctx_->trans_end(true, 1);
  f.free(wctx.mem_ctx_);

  row.set_dml(T_DML_DELETE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, 1001, rk.get_rowkey().get_obj_cnt(), columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  test_redo_log(*wctx.mem_ctx_, 1001, rk.get_rowkey(), T_DML_DELETE, replayed_mt, 2);
  wctx.mem_ctx_->trans_end(true, 2);
  f.free(wctx.mem_ctx_);

  ObQueryFlag flag;
  const ObStoreRow* got_row = NULL;
  rctx.mem_ctx_ = f.alloc();
  rctx.mem_ctx_->trans_begin();
  rctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.get(rctx, flag, 1001, rk.get_rowkey(), columns, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_ROW_EXIST, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*got_row));
  mt.revert_row(got_row);
  rctx.mem_ctx_->trans_end(true, 1);
  f.free(rctx.mem_ctx_);

  rctx.mem_ctx_ = f.alloc();
  rctx.mem_ctx_->trans_begin();
  rctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.get(rctx, flag, 1001, rk.get_rowkey(), columns, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_ROW_DOES_NOT_EXIST, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*got_row));
  mt.revert_row(got_row);
  rctx.mem_ctx_->trans_end(true, 0);
  f.free(rctx.mem_ctx_);

  test_mt_query(rk, columns, mt);
  test_mt_query(rk, columns, replayed_mt);

  replayed_mt.destroy();
  mt.destroy();
}

TEST(TestObMemtable, rowpendingtest)
{
  static const ObPartitionKey PKEY(MT_TABLE_ID, 1, 1);

  ObModAllocator allocator;
  ObMemtable mt;
  ObMemtable replayed_mt;
  int ret = OB_SUCCESS;

  ret = mt.init(PKEY);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = replayed_mt.init(PKEY);
  EXPECT_EQ(OB_SUCCESS, ret);

  storage::ObStoreCtx wctx;
  storage::ObStoreCtx rctx;

  CD cd(100,
      ObVarcharType,
      CS_TYPE_UTF8MB4_GENERAL_CI,
      101,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      102,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      103,
      ObNumberType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMtRowIterator mri;
  RK rk(V("hello", 5), V(NULL, 0), I(1024), N("3.14"));
  ObStoreRow row;
  row.row_val_.cells_ = const_cast<ObObj*>(rk.get_rowkey().get_obj_ptr());
  row.row_val_.count_ = rk.get_rowkey().get_obj_cnt();
  row.set_dml(T_DML_INSERT);
  mri.add_row(row);

  wctx.mem_ctx_ = f.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, 1001, rk.get_rowkey().get_obj_cnt(), columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  test_redo_log_row_pending(*wctx.mem_ctx_, 1001, rk.get_rowkey(), T_DML_INSERT, replayed_mt, 1);
  // Commit to replay.
  ret = wctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wctx.mem_ctx_->replay_to_commit();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  // Replay end.
  wctx.mem_ctx_->trans_replay_end(true, 1);
  f.free(wctx.mem_ctx_);

  row.set_dml(T_DML_UPDATE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, 1001, rk.get_rowkey().get_obj_cnt(), columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  test_redo_log_row_pending(*wctx.mem_ctx_, 1001, rk.get_rowkey(), T_DML_UPDATE, replayed_mt, 2);
  // Commit to replay.
  ret = wctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wctx.mem_ctx_->replay_to_commit();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  // Replay end.
  wctx.mem_ctx_->trans_replay_end(true, 2);
  f.free(wctx.mem_ctx_);

  row.set_dml(T_DML_DELETE);
  mri.reset();
  mri.add_row(row);
  wctx.mem_ctx_ = f.alloc();
  wctx.mem_ctx_->trans_begin();
  wctx.mem_ctx_->sub_trans_begin(2, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.set(wctx, 1001, rk.get_rowkey().get_obj_cnt(), columns, mri);
  EXPECT_EQ(OB_SUCCESS, ret);
  test_redo_log_row_pending(*wctx.mem_ctx_, 1001, rk.get_rowkey(), T_DML_DELETE, replayed_mt, 3);
  // Commit to replay.
  ret = wctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wctx.mem_ctx_->replay_to_commit();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = wctx.mem_ctx_->commit_to_replay();
  EXPECT_EQ(OB_SUCCESS, ret);
  // Replay end.
  wctx.mem_ctx_->trans_replay_end(true, 3);
  f.free(wctx.mem_ctx_);

  ObQueryFlag flag;
  const ObStoreRow* got_row = NULL;
  rctx.mem_ctx_ = f.alloc();
  rctx.mem_ctx_->trans_begin();
  rctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.get(rctx, flag, 1001, rk.get_rowkey(), columns, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_ROW_EXIST, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*got_row));
  mt.revert_row(got_row);
  rctx.mem_ctx_->trans_end(true, 1);
  f.free(rctx.mem_ctx_);

  rctx.mem_ctx_ = f.alloc();
  rctx.mem_ctx_->trans_begin();
  rctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  ret = mt.get(rctx, flag, 1001, rk.get_rowkey(), columns, got_row);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(+ObActionFlag::OP_ROW_DOES_NOT_EXIST, got_row->flag_);
  EXPECT_EQ(rk.get_rowkey().get_obj_cnt(), got_row->row_val_.count_);
  EXPECT_EQ(rk.get_rowkey(), ObStoreRowkey(got_row->row_val_.cells_, got_row->row_val_.count_));
  fprintf(stdout, "%s\n", to_cstring(*got_row));
  mt.revert_row(got_row);
  rctx.mem_ctx_->trans_end(true, 0);
  f.free(rctx.mem_ctx_);

  test_mt_query(rk, columns, mt);
  test_mt_query(rk, columns, replayed_mt);

  replayed_mt.destroy();
  mt.destroy();
}

void print_storerow(const ObStoreRow* got_row)
{
  static const int64_t BUF_LEN = 16 * 1024;
  char buf[BUF_LEN];
  got_row->to_string(buf, BUF_LEN);
  fprintf(stdout, "Print StoreRow: %s\n", buf);
}

void print_compare_obj(ObObj obj1, ObObj obj2)
{
  static const int64_t PRINT_BUF = 1024;
  char print_buf[PRINT_BUF];
  int64_t pos = 0;
  obj1.print_sql_literal(print_buf, PRINT_BUF, pos);
  sprintf(print_buf + pos, " == ");
  pos += 4;
  obj2.print_sql_literal(print_buf, PRINT_BUF, pos);
  fprintf(stdout, "        COMPARE(%s)COMPARE       \n", print_buf);
  TRANS_LOG(DEBUG, "obj2 = ", K(obj2.get_type()));
}

bool is_row_finsih(const ObObj* value)
{
  return (value->get_type() == ObExtendType) && (value->get_ext() == ObActionFlag::OP_END_FLAG);
}

void test_aborted_sub_trans_redo_node(const char* input_str, int64_t rowkey_len, int64_t total,
    int64_t abort_idx)  // TODO change abort_idx to abort_idx_array
{
  // prepare test input data: ObStoreRowIterator, ObSEArray<ColDesc> columns.
  int64_t ret = OB_SUCCESS;

  CD cd(1,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      2,
      ObIntType,
      CS_TYPE_UTF8MB4_BIN,
      3,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN,
      4,
      ObVarcharType,
      CS_TYPE_UTF8MB4_BIN);
  const ObIArray<share::schema::ObColDesc>& columns = cd.get_columns();

  ObMockIterator insert_iter;
  ASSERT_EQ(OB_SUCCESS, insert_iter.from(input_str));
  ObMockIterator read_iter;
  ASSERT_EQ(OB_SUCCESS, read_iter.from(input_str));
  // input data OK.

  TRANS_LOG(DEBUG, "input data prepare ok");
  static const ObPartitionKey PKEY(MT_TABLE_ID, 1, 1);
  uint64_t index_id = 1001;
  ObModAllocator allocator;
  ObQueryFlag flag;
  ObMemtable mt;
  ObStoreRow* store_row = NULL;
  ret = mt.init(PKEY);
  ASSERT_EQ(ret, OB_SUCCESS);
  ObStoreCtx insert_ctx;
  insert_ctx.mem_ctx_ = f.alloc();

  // Transaction 1: insert 5 rows.
  insert_ctx.mem_ctx_->trans_begin();

  for (int64_t i = 1; i <= total; i++) {
    TRANS_LOG(DEBUG, "insert row", K(i));
    insert_ctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    insert_iter.get_next_row(store_row);
    store_row->set_dml(T_DML_INSERT);
    ret = mt.set(insert_ctx, index_id, rowkey_len, columns, *store_row);
    EXPECT_TRUE(OB_SUCC(ret));
    if (i == abort_idx) {
      TRANS_LOG(DEBUG, "abort row", K(abort_idx));
      insert_ctx.mem_ctx_->sub_trans_abort();
    }
  }

  // get redo log buffer from MemCtx. Then parse the buffer and check.
  ObMemtableMutatorIterator mmi;
  ObArenaAllocator allocator2(0);
  const int64_t REDO_BUFFER_SIZE = 2 * 1024 * 1024;
  char* redo_log_buffer = new char[REDO_BUFFER_SIZE];
  int64_t data_len = 0;
  int64_t pos = 0;
  ret = insert_ctx.mem_ctx_->fill_redo_log(redo_log_buffer, REDO_BUFFER_SIZE, data_len);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = mmi.deserialize(redo_log_buffer, data_len, pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  insert_ctx.mem_ctx_->trans_end(true, 1);

  // check redo log buffer.
  uint64_t res_index_id = 0;
  ObStoreRowkey res_rowkey;
  int64_t schema_version = 0;
  ObRowData new_row;
  storage::ObRowDml res_dml_type = T_DML_UNKNOWN;
  uint32_t res_modify_count = 0;

  ObMockIterator check_iter;
  ASSERT_EQ(OB_SUCCESS, check_iter.from(input_str));
  ObStoreRow* check_row = NULL;

  for (int64_t i = 1; i <= total; i++) {  // for each row
    ret = check_iter.get_next_row(check_row);
    EXPECT_EQ(ret, OB_SUCCESS);
    if (i != abort_idx) {
      ret = mmi.get_next_row(res_index_id, res_rowkey, schema_version, new_row, res_dml_type, res_modify_count);
      EXPECT_EQ(OB_SUCCESS, ret);
      EXPECT_EQ(index_id, res_index_id);
      EXPECT_EQ(res_rowkey, ObStoreRowkey(check_row->row_val_.cells_, rowkey_len));
      EXPECT_EQ(res_dml_type, T_DML_INSERT);
      // ObCompactCellIterator cci;
      // ret = cci.init(data, SPARSE);
      ObCellReader cell_reader;
      ret = cell_reader.init(new_row.data_, new_row.size_, SPARSE);
      EXPECT_EQ(OB_SUCCESS, ret);

      // check trans_node
      uint64_t column_id = OB_INVALID_ID;
      const ObObj* value = NULL;
      ObObj* value_array = check_row->row_val_.cells_;
      int64_t value_count = check_row->row_val_.count_;
      int64_t value_array_index = 0;
      int64_t iter_ret = OB_SUCCESS;

      while (OB_SUCCESS == (iter_ret = cell_reader.next_cell()) && value_array_index < value_count) {
        ret = cell_reader.get_cell(column_id, value);
        EXPECT_FALSE(is_row_finsih(value));
        EXPECT_EQ(OB_SUCCESS, ret);
        print_compare_obj(value_array[value_array_index], *value);
        EXPECT_TRUE(value_array[value_array_index] == *value);
        value_array_index++;
      }
      cell_reader.get_cell(value);
      EXPECT_TRUE(is_row_finsih(value));
      TRANS_LOG(DEBUG, "***********************************");
      TRANS_LOG(DEBUG, " ");
    }
  }

  f.free(insert_ctx.mem_ctx_);

  // Transaction 2 : read 5 rows
  ObStoreCtx read_ctx;
  read_ctx.mem_ctx_ = f.alloc();
  read_ctx.mem_ctx_->trans_begin();

  // read 1
  const oceanbase::storage::ObStoreRow* got_row = NULL;

  for (int64_t i = 1; i <= total; i++) {
    TRANS_LOG(DEBUG, "read r1");
    read_ctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    read_iter.get_next_row(store_row);
    ret = mt.get(read_ctx, flag, index_id, ObStoreRowkey(store_row->row_val_.cells_, rowkey_len), columns, got_row);
    print_storerow(got_row);
    if (i == abort_idx) {
      EXPECT_EQ(+ObActionFlag::OP_ROW_DOES_NOT_EXIST, got_row->flag_);
    } else {
      EXPECT_EQ(+ObActionFlag::OP_ROW_EXIST, got_row->flag_);
    }
    mt.revert_row(got_row);
  }

  read_ctx.mem_ctx_->trans_end(true, 1);
  f.free(read_ctx.mem_ctx_);

  mt.destroy();
  free(redo_log_buffer);
}

TEST(TestMemtable, aborted_sub_trans_redo_node_different_row)
{
  const char* input_str = "bigint  bigint   var           var       \n"
                          "1    101   songjiang     jishiyu   \n"
                          "2    102   lujunyi       yuqilin   \n"
                          "3    103   wuyong        zhiduoxing\n"
                          "4    104   gongsunsheng  ruyunlong \n"
                          "5    105   guansheng     dadao     \n";
  /*
     const char *input_str =
     "int  int   int           int   \n"
     "1    401   501           601   \n"
     "2    402   502           602   \n"
     "3    403   503           603   \n"
     "4    404   504           604   \n"
     "5    405   505           605   \n";
     int64_t total = 5;
     int64_t rowkey_len = 1;
     for(int64_t abort_idx = 1; abort_idx <= total; abort_idx++) {
     test_aborted_sub_trans_redo_node(input_str, rowkey_len, total, abort_idx);
     fprintf(stdout, "\n");
     }
   */
  int64_t abort_idx = 4;
  int64_t total = 5;
  int64_t rowkey_len = 1;
  test_aborted_sub_trans_redo_node(input_str, rowkey_len, total, abort_idx);
  fprintf(stdout, "\n");
}

void test_aborted_sub_trans_redo_node_same_row(
    const char* input_str_update_same_row, int64_t rowkey_len, int64_t total, int64_t abort_idx)
{
  int64_t ret = OB_SUCCESS;
  ObSEArray<share::schema::ObColDesc, 64> columns;
  share::schema::ObColDesc col_desc;
  col_desc.col_id_ = 1;
  col_desc.col_type_.set_type(ObIntType);
  col_desc.col_type_.set_collation_type(CS_TYPE_UTF8MB4_BIN);
  columns.push_back(col_desc);

  col_desc.col_id_ = 2;
  columns.push_back(col_desc);

  col_desc.col_id_ = 3;
  col_desc.col_type_.set_type(ObVarcharType);
  columns.push_back(col_desc);

  col_desc.col_id_ = 4;
  columns.push_back(col_desc);

  ObMockIterator update_iter;
  ASSERT_EQ(OB_SUCCESS, update_iter.from(input_str_update_same_row));
  ObMockIterator read_iter;
  ASSERT_EQ(OB_SUCCESS, read_iter.from(input_str_update_same_row));
  // --- input data OK.

  // Transaction 1: update same row 5 times.
  TRANS_LOG(DEBUG, "start update");
  static const ObPartitionKey PKEY(MT_TABLE_ID, 1, 1);
  uint64_t index_id = 1001;
  ObModAllocator allocator;
  ObQueryFlag flag;
  ObMemtable mt;
  ObStoreRow* store_row = NULL;

  ret = mt.init(PKEY);
  ASSERT_EQ(ret, OB_SUCCESS);
  ObStoreCtx update_ctx;
  update_ctx.mem_ctx_ = f.alloc();

  // Transaction 1: insert 5 rows.
  update_ctx.mem_ctx_->trans_begin();

  for (int64_t i = 1; i <= total; i++) {
    TRANS_LOG(DEBUG, "update row", K(i));
    update_ctx.mem_ctx_->sub_trans_begin(0, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
    update_iter.get_next_row(store_row);
    if (i == 1) {
      store_row->set_dml(T_DML_INSERT);
    } else {
      store_row->set_dml(T_DML_UPDATE);
    }
    ret = mt.set(update_ctx, index_id, rowkey_len, columns, *store_row);
    EXPECT_TRUE(OB_SUCC(ret));
    if (i == abort_idx) {
      TRANS_LOG(DEBUG, "abort row", K(abort_idx));
      update_ctx.mem_ctx_->sub_trans_abort();
    }
  }

  // get redo log buffer from MemCtx. Then parse the buffer and check.
  ObMemtableMutatorIterator mmi;
  ObArenaAllocator allocator2(0);
  const int64_t REDO_BUFFER_SIZE = 2 * 1024 * 1024;
  char* redo_log_buffer = new char[REDO_BUFFER_SIZE];
  int64_t data_len = 0;
  int64_t pos = 0;
  ret = update_ctx.mem_ctx_->fill_redo_log(redo_log_buffer, REDO_BUFFER_SIZE, data_len);
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = mmi.deserialize(redo_log_buffer, data_len, pos);
  EXPECT_EQ(OB_SUCCESS, ret);

  update_ctx.mem_ctx_->trans_end(true, 1);
  // check redo log buffer.
  uint64_t res_index_id = 0;
  ObStoreRowkey res_rowkey;
  int64_t schema_version = 0;
  ObRowData new_row;
  storage::ObRowDml res_dml_type = T_DML_UNKNOWN;
  uint32_t res_modify_count = 0;

  ObMockIterator check_iter;
  ASSERT_EQ(OB_SUCCESS, check_iter.from(input_str_update_same_row));
  ObStoreRow* check_row = NULL;

  for (int64_t i = 1; i <= total; i++) {  // for each sub_trans
    ret = check_iter.get_next_row(check_row);
    EXPECT_EQ(ret, OB_SUCCESS);
    if (i != abort_idx) {
      ret = mmi.get_next_row(res_index_id, res_rowkey, schema_version, new_row, res_dml_type, res_modify_count);
      EXPECT_EQ(OB_SUCCESS, ret);
      EXPECT_EQ(index_id, res_index_id);
      EXPECT_EQ(res_rowkey, ObStoreRowkey(check_row->row_val_.cells_, rowkey_len));
      if (i == 1) {
        EXPECT_EQ(res_dml_type, T_DML_INSERT);
      } else {
        EXPECT_EQ(res_dml_type, T_DML_UPDATE);
      }

      ObCellReader cell_reader;
      ret = cell_reader.init(new_row.data_, new_row.size_, SPARSE);
      EXPECT_EQ(OB_SUCCESS, ret);

      // check trans_node
      uint64_t column_id = OB_INVALID_ID;
      const ObObj* value = NULL;
      ObObj* value_array = check_row->row_val_.cells_;
      int64_t value_count = check_row->row_val_.count_;
      int64_t value_array_index = 0;
      int64_t iter_ret = OB_SUCCESS;

      while (OB_SUCCESS == (iter_ret = cell_reader.next_cell()) && value_array_index < value_count) {
        ret = cell_reader.get_cell(column_id, value);
        EXPECT_EQ(OB_SUCCESS, ret);
        EXPECT_FALSE(is_row_finsih(value));
        print_compare_obj(value_array[value_array_index], *value);
        EXPECT_TRUE(value_array[value_array_index] == *value);
        value_array_index++;
      }
      cell_reader.get_cell(value);
      EXPECT_TRUE(is_row_finsih(value));
      TRANS_LOG(DEBUG, "***********************************");
      TRANS_LOG(DEBUG, " ");
    }
  }

  f.free(update_ctx.mem_ctx_);

  // read 1
  const oceanbase::storage::ObStoreRow* got_row = NULL;
  ObStoreCtx read_ctx;
  read_ctx.mem_ctx_ = f.alloc();
  read_ctx.mem_ctx_->trans_begin();
  TRANS_LOG(DEBUG, "read ");
  read_ctx.mem_ctx_->sub_trans_begin(1, 1000000 + ::oceanbase::common::ObTimeUtility::current_time());
  read_iter.get_next_row(store_row);
  ret = mt.get(read_ctx, flag, index_id, ObStoreRowkey(store_row->row_val_.cells_, rowkey_len), columns, got_row);
  TRANS_LOG(DEBUG, "print got_row start ***************");
  print_storerow(got_row);
  TRANS_LOG(DEBUG, "print got_row end   ***************");
  mt.revert_row(got_row);

  read_ctx.mem_ctx_->trans_end(true, 1);
  f.free(read_ctx.mem_ctx_);

  mt.destroy();
  free(redo_log_buffer);
}

TEST(TestMemtable, aborted_sub_trans_redo_node_same_row)
{
  const char* input_str_update_same_row = "bigint  bigint var           var       \n"
                                          "1    1   songjiang     jishiyu   \n"
                                          "1    2   lujunyi       yuqilin   \n"
                                          "1    3   wuyong        zhiduoxing\n"
                                          "1    4   gongsunsheng  ruyunlong \n"
                                          "1    5   guansheng     dadao     \n";

  int64_t total = 5;
  int64_t rowkey_len = 1;
  int64_t abort_idx = 5;
  fprintf(stdout, "start test same row\n");
  test_aborted_sub_trans_redo_node_same_row(input_str_update_same_row, rowkey_len, total, abort_idx);
  fprintf(stdout, "end   test same row\n");
}

}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char** argv)
{
  // oceanbase::common::ObLogger::get_logger().set_file_name("test_memtable.log", true);
  oceanbase::common::ObLogger::get_logger().set_log_level("DEBUG");
  OB_LOGGER.set_file_name("test_memtable.log", true);
  OB_LOGGER.set_log_level("DEBUG");
  oceanbase::unittest::init_tenant_mgr();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

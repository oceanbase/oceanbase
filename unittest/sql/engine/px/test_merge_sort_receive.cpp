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

#include <gtest/gtest.h>
#define private public
#define protected public
#include "lib/utility/ob_test_util.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/utility/ob_macro_utils.h"
#include "sql/engine/px/exchange/ob_px_merge_sort_receive.h"
#include "sql/dtl/ob_dtl_channel_loop.h"
#include "sql/dtl/ob_dtl_rpc_channel.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/table/ob_fake_table.h"
#include "sql/engine/basic/ob_ra_row_store.h"

using namespace std;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::sql::dtl;

class ObMergeSortReceiveTest : public ::testing::Test
{
public:
  void test_sort(int64_t n_channel, bool local_order, int64_t row_count);
  int mock_channel_loop(ObPxMergeSortReceive::ObPxMergeSortReceiveCtx *recv_ctx,
                        int64_t n_channel,
                        int64_t row_count,
                        bool local_order);
  int init_merge_sort_input(
    ObExecContext &ctx,
    ObPxMergeSortReceive::ObPxMergeSortReceiveCtx *recv_ctx,
    ObPxMergeSortReceive &merge_sort_receive,
    int64_t n_channel);
private:
  int init_open(ObExecContext &ctx,
                ObPxMergeSortReceive &merge_sort_receive,
                int64_t n_channel,
                int64_t row_count,
                bool local_order);
};

class ObMockPxNewRow : public ObPxNewRow
{
public:
  ObMockPxNewRow(int64_t count, bool local_order, int64_t n_channel) :
    count_(count),
    local_order_(local_order),
    cur_(0),
    n_channel_(n_channel) {}
  ~ObMockPxNewRow() = default;
  virtual int get_row(ObNewRow &row);

private:
  int64_t count_;
  bool local_order_;
  int64_t cur_;
  int64_t n_channel_;
};

class ObMockChannelLoop : public ObDtlChannelLoop
{
public:
  ObMockChannelLoop(int64_t n_channel) :
    n_channel_(n_channel), nth_process_channel_(0) {}
  ~ObMockChannelLoop() = default;

  virtual int process_one(int64_t &nth_channel, int64_t timeout);
  virtual int process_one_if(PredFunc pred, int64_t timeout, int64_t &nth_channel);
private:
  int64_t n_channel_;
  int64_t nth_process_channel_;
};

int ObMockPxNewRow::get_row(ObNewRow &row)
{
  int ret = OB_SUCCESS;
  int64_t n_group = local_order_ ? 2 : 1;
  int64_t pos = (cur_ + n_channel_) / n_channel_;
  if (pos  > count_ * n_group) {
    ret = OB_ITER_END;
    cout << "cur_" << cur_ << " ,pos:" << pos << " ,count:" << count_ * n_group << endl;
  }
  int64_t column_count = 1;
  if (OB_SUCC(ret)) {
    row.cells_ = static_cast<ObObj *>(malloc(column_count * sizeof(ObObj)));
    row.count_ = column_count;
    // fill data
    int64_t val = cur_;
    if (local_order_) {
      if (cur_ >= n_channel_ * count_) {
        val = cur_ - n_channel_ * count_;
      }
    }
    // cout << "fill val:" << val << " ,cur_:" << cur_ << endl;
    ObObj *obj = COL(val);
    row.assign(obj, column_count);
  }
  cur_++;
  return ret;
}

int ObMockChannelLoop::process_one(int64_t &nth_channel, int64_t timeout)
{
  int ret = OB_SUCCESS;
  UNUSED(timeout);
  nth_channel = nth_process_channel_ % n_channel_;
//  cout << "channel:" << nth_process_channel_ << endl;
  nth_process_channel_ = (nth_process_channel_ + 1) % n_channel_;
  return ret;
}

int ObMockChannelLoop::process_one_if(PredFunc pred, int64_t timeout, int64_t &nth_channel)
{
  int ret = OB_SUCCESS;
  UNUSED(pred);
  UNUSED(timeout);
  nth_channel = nth_process_channel_ % n_channel_;
//  cout << "channel:" << nth_process_channel_ << endl;
  nth_process_channel_ = (nth_process_channel_ + 1) % n_channel_;
  return ret;
}

int ObMergeSortReceiveTest::init_open(ObExecContext &ctx,
  ObPxMergeSortReceive &merge_sort_receive,
  int64_t n_channel,
  int64_t row_count,
  bool local_order)
{
  int ret = OB_SUCCESS;
  ObPxMergeSortReceive::ObPxMergeSortReceiveCtx *recv_ctx = nullptr;
  ObSQLSessionInfo my_session;
  my_session.test_init(0,0,0,NULL);
  ctx.set_my_session(&my_session);
  merge_sort_receive.set_id(0);
  if (OB_FAIL(ctx.init_phy_op(1))) {
    ret = OB_ERR_UNEXPECTED;
    cout << "fail to init phy op 4" << endl;
  } else if (OB_FAIL(ctx.create_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    cout << "fail to create physical plan ctx for context" << endl;
  } else if (OB_FAIL(merge_sort_receive.init_op_ctx(ctx))) {
    ret = OB_ERR_UNEXPECTED;
    cout << "fail to inner open" << endl;
  } else if (OB_ISNULL(recv_ctx = GET_PHY_OPERATOR_CTX(ObPxMergeSortReceive::ObPxMergeSortReceiveCtx, ctx, merge_sort_receive.get_id()))) {
    ret = OB_ERR_UNEXPECTED;
    cout << "fail to create physica operator ctx" << endl;
  } else if (OB_FAIL(mock_channel_loop(recv_ctx, n_channel, row_count, local_order))) {
    cout << "fail to mock channel loop" << endl;
  } else if (!merge_sort_receive.local_order_ && OB_FAIL(recv_ctx->row_heap_.init(n_channel, merge_sort_receive.sort_columns_))) {
    cout << "fail to init row heap" << endl;
  } else if (OB_FAIL(init_merge_sort_input(ctx, recv_ctx, merge_sort_receive, n_channel))) {
    // mock the number of channels is 2
    cout << "fail to init merge sort input" << endl;
  } else {
    recv_ctx->channel_linked_ = true;
    cout << "success to init" << endl;
  }
  return ret;
}

int ObMergeSortReceiveTest::init_merge_sort_input(
  ObExecContext &ctx,
  ObPxMergeSortReceive::ObPxMergeSortReceiveCtx *recv_ctx,
  ObPxMergeSortReceive &merge_sort_receive,
  int64_t n_channel)
{
  int ret = OB_SUCCESS;
  UNUSED(ctx);
  if (OB_ISNULL(recv_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    cout << "the phy operator ctx is null" << endl;
  } else {
    // init process function
    if (merge_sort_receive.local_order_) {
      if (0 >= n_channel) {
        cout << "channels are not init" << endl;
      } else {
        for(int64_t idx = 0; OB_SUCC(ret) && idx < n_channel; ++idx) {
          ObRARowStore *row_store = OB_NEW(ObRARowStore, ObModIds::OB_SQL_PX);
          int64_t mem_limit = 0;
          if (OB_FAIL(row_store->init(mem_limit))) {
            cout << "row store init fail" << endl;
          } else if (nullptr == row_store) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            cout << "create ra row store fail" << endl;
          } else if (OB_FAIL(recv_ctx->row_stores_.push_back(row_store))) {
            cout << "push back ra row store fail" << endl;
          }
        }
      }
    } else {
      if (0 >= n_channel) {
        cout << "channels are not init" << endl;
      } else {
        cout << "start alloc msi" << endl;
        for(int64_t idx = 0; OB_SUCC(ret) && idx < n_channel; ++idx) {
          ObPxMergeSortReceive::MergeSortInput *msi = OB_NEW(ObPxMergeSortReceive::GlobalOrderInput, ObModIds::OB_SQL_PX, OB_SERVER_TENANT_ID);
          cout << "alloc succ msi" << endl;
          if (nullptr == msi) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            cout << "create merge sort input fail" << endl;
          } else if (OB_FAIL(recv_ctx->merge_inputs_.push_back(msi))) {
            cout << "push back merge sort input fail" << endl;
          } else {
            cout << "succuss to alloc merge sort input" << endl;
          }
        }
      }
    }
  }
  cout << "succuss to init merge sort input" << endl;
  return ret;
}

int ObMergeSortReceiveTest::mock_channel_loop(
  ObPxMergeSortReceive::ObPxMergeSortReceiveCtx *recv_ctx,
  int64_t n_channel,
  int64_t row_count,
  bool local_order)
{
  int ret = OB_SUCCESS;
  ObMockChannelLoop *mock_channel_loop = new ObMockChannelLoop(n_channel);
  if (OB_ISNULL(mock_channel_loop)) {
    cout << "fail to alloc mock channel loop" << endl;
  } else {
    recv_ctx->ptr_row_msg_loop_ = mock_channel_loop;
    ObMockPxNewRow *mock_px_row = new ObMockPxNewRow(row_count, local_order, n_channel);
    if (OB_ISNULL(mock_px_row)) {
      ret = OB_ERR_UNEXPECTED;
      cout << "fail to mock px row" << endl;
    } else {
      recv_ctx->ptr_px_row_ = mock_px_row;
    }
  }
  ObAddr self;
  self.set_ip_addr("127.0.0.1", 8086);
  ObDtlRpcChannel *tmp_channel = new ObDtlRpcChannel(1, 1, self);
  for (int idx = 0; idx < n_channel; ++idx) {
    if (OB_FAIL(recv_ctx->task_channels_.push_back(tmp_channel))) {
      cout << "fail to push back channel" << endl;
    }
  }
  cout << "mock channel success" << endl;
  return ret;
}

// n_channel channel个数
// local_order是否局部有序
// 每组有序多少行，如果是local order，则每个channel有两组，否则是一组
void ObMergeSortReceiveTest::test_sort(int64_t n_channel, bool local_order, int64_t row_count)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  ObPxMergeSortReceive merge_sort_receive(alloc, local_order);
  ObExecContext ctx;
  merge_sort_receive.init_sort_columns(1);
  merge_sort_receive.add_sort_column(0, CS_TYPE_UTF8MB4_BIN, true, ObMaxType, default_asc_direction());
  const ObNewRow *out_row = NULL;
  if (OB_FAIL(init_open(ctx, merge_sort_receive, n_channel, row_count, local_order))) {
    ret = OB_ERR_UNEXPECTED;
    cout << "fail to init open" << endl;
  } else {
    const ObObj *cell = NULL;
    int64_t val;
    int64_t n_group = local_order ? 2 : 1;
    int64_t total_row_count = n_channel * row_count * n_group;
    int64_t last_value = -1;
    bool need_cmp = true;
    for (int64_t cnt = 0; OB_SUCC(ret) && cnt < total_row_count; ++cnt) {
      out_row = NULL;
      if (OB_FAIL(merge_sort_receive.inner_get_next_row(ctx, out_row))) {
        ret = OB_ERR_UNEXPECTED;
        cout << "fail to get next row: " << cnt << " ,total_row_count:" << total_row_count << endl;
      } else {
//        cout << "times:" << cnt << endl;
        cell = &out_row->cells_[0];
        cell->get_int(val);
        // cout << val << endl;
        if (true == need_cmp) {
          ASSERT_TRUE(val > last_value);
          if (val <= last_value) {
            cout << "compare error, data is no order:" << cnt << ", value: " << val << " ,last_value: " << last_value << endl;
          }
          if (local_order) {
            need_cmp = !need_cmp;
          }
        } else {
          if (local_order) {
            need_cmp = !need_cmp;
            ASSERT_EQ(last_value, val);
            if (last_value != val) {
              cout << "compare error, there are two same data in local order:" << cnt << ", value: " << val << " ,last_value: " << last_value << endl;
            }
          }
        }
        last_value = val;
      }
    }
    for (int64_t cnt = 0; OB_SUCC(ret) && cnt < n_channel; ++cnt) {
      ASSERT_EQ(OB_ITER_END, merge_sort_receive.inner_get_next_row(ctx, out_row));
    }
  }

  ASSERT_EQ(OB_SUCCESS, ret);
  return;
}

TEST_F(ObMergeSortReceiveTest, merge_sort_receive_1)
{
  test_sort(1, false, 10);
  test_sort(1, true, 10);

  test_sort(2, false, 10);
  test_sort(2, true, 10);
  test_sort(2, false, 0);
  test_sort(2, true, 0);

  test_sort(3, false, 10);
  test_sort(3, true, 10);
  test_sort(3, false, 0);
  test_sort(3, true, 0);

  test_sort(2, false, 30);
  test_sort(2, true, 30);

  test_sort(2, false, 300);
  test_sort(2, true, 300);

  test_sort(19, false, 300);
  test_sort(19, true, 300);
}

int main(int argc, char **argv)
{
  system("rm -f test_merge_sort.log*");
  OB_LOGGER.set_file_name("test_merge_sort.log", true, true);
  //OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  return RUN_ALL_TESTS();
}

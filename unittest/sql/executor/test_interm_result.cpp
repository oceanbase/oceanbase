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

#include "common/row/ob_row_store.h"
#include "lib/utility/ob_test_util.h"
#include "common/row/ob_row.h"
#include "sql/executor/ob_interm_result.h"
#include "sql/executor/ob_interm_result_pool.h"
#include "sql/executor/ob_interm_result_manager.h"
#include "sql/executor/ob_task_event.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/allocator/ob_mod_define.h"
#include "lib/utility/ob_tracepoint.h"
#include <gtest/gtest.h>

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

class ObIntermResultTest : public ::testing::Test
{
public:
  static const int64_t SCANNER_NUM = 16;
  static const int64_t COL_NUM = 16;
  static const int64_t ROW_NUM = 100;
  static const int64_t BIG_ROW_NUM = 1024*1024*3;

  ObIntermResultTest();
  virtual ~ObIntermResultTest();
  virtual void SetUp();
  virtual void TearDown();

  void exception_test(int expected_ret, int64_t row_num);
private:
  // disallow copy
  ObIntermResultTest(const ObIntermResultTest &other);
  ObIntermResultTest& operator=(const ObIntermResultTest &other);
};

ObIntermResultTest::ObIntermResultTest()
{
}

ObIntermResultTest::~ObIntermResultTest()
{
}

void ObIntermResultTest::SetUp()
{
}

void ObIntermResultTest::TearDown()
{
}

void ObIntermResultTest::exception_test(int expected_ret, int64_t row_num)
{
  int ret = OB_SUCCESS;
  UNUSED(expected_ret);
  ObIntermResult tmp_ir;
  ObIntermResultPool* ir_pool = NULL;
  ObIntermResultItemPool* ir_item_pool = NULL;
  ObIntermResultManager* ir_manager = NULL;
  oceanbase::common::ObNewRow row;
  oceanbase::common::ObNewRow tmp_row;
  ObObj objs[COL_NUM];
  ObObj tmp_objs[COL_NUM];

  ir_pool = ObIntermResultPool::get_instance();
  (void) ir_pool;
  ir_item_pool = ObIntermResultItemPool::get_instance();
  (void) ir_item_pool;
  ir_manager = ObIntermResultManager::get_instance();

  row.count_ = COL_NUM;
  row.cells_ = objs;

  tmp_row.count_ = COL_NUM;
  tmp_row.cells_ = tmp_objs;

  ObIntermResult* ir = NULL;
  ObIntermResultIterator iter;
  int64_t expire_time = ::oceanbase::common::ObTimeUtility::current_time();

  // add row
  for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
      row.cells_[j].set_int(i*COL_NUM+j);
    } // end for
    ret = tmp_ir.add_row(OB_SYS_TENANT_ID, row);
  } // end for
  if (OB_SUCC(ret)) {
    ret = tmp_ir.complete_add_rows(OB_SYS_TENANT_ID);
  }

  // add row
  if (OB_FAIL(ret)) {
    //empty
  } else if (OB_FAIL(ir_manager->alloc_result(ir))) {}

  for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
      row.cells_[j].set_int(i*COL_NUM+j);
    } // end for
    ret = ir->add_row(OB_SYS_TENANT_ID, row);
  } // end for
  if (OB_SUCC(ret)) {
    ret = ir->complete_add_rows(OB_SYS_TENANT_ID);
  }

  if (OB_FAIL(ret)) {
    //empty
  } else if (OB_FAIL(ir_manager->free_result(ir))) {
    //empty
  } else if (OB_FAIL(ir_manager->alloc_result(ir))) {
    //empty
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_num; ++i) {
    for (int64_t j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
      row.cells_[j].set_int(i*COL_NUM+j);
    } // end for
    OK(ir->add_row(OB_SYS_TENANT_ID, row));
  } // end for
  if (OB_SUCC(ret)) {
    ret = ir->complete_add_rows(OB_SYS_TENANT_ID);
  }

  if (OB_FAIL(ret)) {
    //empty
  } else {
    ObScanner scanner;
    ObScanner::Iterator scanner_iter;
    ObIIntermResultItem *iir_item = NULL;
    ObIntermResultItem *ir_item = NULL;
    static int64_t id = 1;
    ObAddr server;
    server.set_ip_addr("127.0.0.1", 8888);
    ObSliceID slice_id;
    slice_id.set_server(server);
    slice_id.set_execution_id(id);
    slice_id.set_job_id(id);
    slice_id.set_task_id(id);
    slice_id.set_slice_id(id);
    id++;
    ObIntermResultInfo ir_info;
    ir_info.init(slice_id);
    if (OB_FAIL(ir_manager->add_result(ir_info, ir, expire_time))) {
      //empty
    } else if (OB_FAIL(ir_manager->get_result(ir_info, iter))) {
      //empty
    }

    bool has_got_first_scanner = false;
    int64_t cur_row_num = 0;
    while (OB_SUCC(ret)) {
      bool should_get_next_item = false;
      if (!has_got_first_scanner) {
        should_get_next_item = true;
      } else {
        if (OB_FAIL(scanner_iter.get_next_row(tmp_row))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            should_get_next_item = true;
          }
        } else {
          for (int64_t j = 0; OB_SUCC(ret) && j < COL_NUM; ++j) {
            ASSERT_EQ(tmp_row.cells_[j].get_int(), cur_row_num*COL_NUM+j);
          }
          cur_row_num++;
        }
      }

      if (OB_SUCC(ret) && should_get_next_item) {
        if (OB_FAIL(iter.get_next_interm_result_item(iir_item))) {
        } else {
          ASSERT_TRUE(NULL != iir_item);
          ir_item = static_cast<ObIntermResultItem *>(iir_item);
          scanner.reset();
          if (!scanner.is_inited() && OB_FAIL(scanner.init())) {
          } else if (OB_FAIL(ir_item->to_scanner(scanner))) {
          } else {
            scanner_iter = scanner.begin();
            has_got_first_scanner = true;
          }
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      ASSERT_EQ(cur_row_num, row_num);
    }

    if (OB_FAIL(ret)) {
      //empty
    } else {
      ASSERT_EQ(OB_ITER_END, iter.get_next_interm_result_item(iir_item));
      ASSERT_EQ(OB_ITER_END, iter.get_next_interm_result_item(iir_item));
      if (OB_FAIL(ir_manager->delete_result(iter))) {
        //empty
      }
    }
  }

  //OB_DELETE(ObIntermResultPool, ObModIds::OB_SQL_EXECUTOR, ir_pool);
  //OB_DELETE(ObScannerPool, ObModIds::OB_SQL_EXECUTOR, ir_item_pool);
  //OB_DELETE(ObIntermResultManager, ObModIds::OB_SQL_EXECUTOR, ir_manager);

  //断点依赖路径正确，加上ccache以后，路径变成绝对路径了
  //ASSERT_EQ(expected_ret, ret);
}

#define EXCEPTION_TEST(test_name, func, key, err, expect_ret, row_num) \
          TEST_F(ObIntermResultTest, test_name) \
          {\
            TP_SET_ERROR("executor/ob_interm_result.cpp", func, key, err); \
            exception_test(expect_ret, row_num); \
            TP_SET_ERROR("executor/ob_interm_result.cpp", func, key, NULL); \
          }\


TEST_F(ObIntermResultTest, basic_test)
{
  exception_test(OB_SUCCESS, BIG_ROW_NUM);
}

EXCEPTION_TEST(et1, "ObIntermResult", "t1", 1, OB_SUCCESS, ROW_NUM);
EXCEPTION_TEST(et2, "reset", "t1", 1, OB_SUCCESS, ROW_NUM);
EXCEPTION_TEST(et3, "add_row", "t1", 1, OB_ERR_UNEXPECTED, ROW_NUM);
EXCEPTION_TEST(et4, "add_row", "t2", 1, OB_ERR_UNEXPECTED, ROW_NUM);
EXCEPTION_TEST(et5, "add_row", "t3", 1, OB_ERR_UNEXPECTED, ROW_NUM);
EXCEPTION_TEST(et6, "add_row", "t4", OB_ERROR, OB_ERROR, ROW_NUM);
EXCEPTION_TEST(et7, "add_row", "t5", OB_ERROR, OB_ERROR, ROW_NUM);
EXCEPTION_TEST(et8, "add_row", "t6", OB_ERROR, OB_ERROR, ROW_NUM);
EXCEPTION_TEST(et9, "set_interm_result", "t1", 1, OB_INVALID_ARGUMENT, ROW_NUM);
EXCEPTION_TEST(et10, "set_interm_result", "t2", 1, OB_INVALID_ARGUMENT, ROW_NUM);
EXCEPTION_TEST(et11, "get_interm_result_info", "t1", 1, OB_NOT_INIT, ROW_NUM);
EXCEPTION_TEST(et12, "get_next_row", "t1", 1, OB_NOT_INIT, ROW_NUM);
EXCEPTION_TEST(et13, "get_next_row", "t2", 1, OB_ERR_UNEXPECTED, ROW_NUM);
EXCEPTION_TEST(et14, "try_inc_cnt", "t1", 1, OB_STATE_NOT_MATCH, ROW_NUM);
EXCEPTION_TEST(et15, "try_dec_cnt", "t1", 1, OB_STATE_NOT_MATCH, ROW_NUM);
EXCEPTION_TEST(et16, "try_begin_recycle", "t1", 1, OB_STATE_NOT_MATCH, ROW_NUM);
EXCEPTION_TEST(et17, "try_end_recycle", "t1", 1, OB_SUCCESS, ROW_NUM);
EXCEPTION_TEST(et18, "try_end_recycle", "t2", 1, OB_SUCCESS, ROW_NUM);

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ObIntermResultItemPool::build_instance();
  ObIntermResultPool::build_instance();
  ObIntermResultManager::build_instance();
  ::testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}

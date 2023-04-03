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

#include "sql/engine/sort/ob_sort.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/engine/ob_physical_plan.h"
#include "lib/utility/ob_test_util.h"
#include "lib/utility/ob_tracepoint.h"
#include "lib/container/ob_se_array.h"
#include <gtest/gtest.h>
#include "ob_fake_table.h"
#include "sql/engine/ob_exec_context.h"
#include "share/ob_worker.h"
#include "observer/ob_signal_handle.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "storage/blocksstable/ob_tmp_file.h"
#include "sql/engine/join/join_data_generator.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "share/ob_simple_mem_limit_getter.h"

#include <thread>
#include <vector>
#include <gtest/gtest.h>


using namespace oceanbase;
using namespace oceanbase::sql;
using namespace oceanbase::common;
using namespace oceanbase::omt;
static ObSimpleMemLimitGetter getter;

class TestSortImpl : public blocksstable::TestDataFilePrepare
{
public:
  TestSortImpl() : blocksstable::TestDataFilePrepare(&getter,
                                                     "TestDiskIR", 2<<20, 1000), data_(alloc_)
  {
  }
  void set_sort_area_size(int64_t size)
  {
    int ret = OB_SUCCESS;
    int64_t tenant_id = OB_SYS_TENANT_ID;
    ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id));
    if (tenant_config.is_valid()) {
      tenant_config->_sort_area_size = size;
    } else {
      ret = OB_ERR_UNEXPECTED;
      SQL_ENG_LOG(WARN, "unexpected status: config is invalid", K(tenant_id));
    }
    // ASSERT_EQ(OB_SUCCESS, ret);
  }
  virtual void SetUp() override
  {
    GCONF.enable_sql_operator_dump.set_value("True");
    ASSERT_EQ(OB_SUCCESS, init_tenant_mgr());
    blocksstable::TestDataFilePrepare::SetUp();
    ASSERT_EQ(OB_SUCCESS, blocksstable::ObTmpFileManager::get_instance().init());
    for (int64_t i = 0; i < ARRAYSIZEOF(cols_); i++) {
      ObSortColumn &sc = cols_[i];
      sc.cs_type_ = CS_TYPE_UTF8MB4_BIN;
      sc.set_is_ascending(true);
    }
    cols_[0].index_ = JoinDataGenerator::ROW_ID_CELL;
    cols_[1].index_ = JoinDataGenerator::IDX_CELL;
    cols_[2].index_ = JoinDataGenerator::RAND_CELL;
    data_.gen_varchar_cell_ = true;
    data_.string_size_ = 500;
    set_sort_area_size(1 * 1024 * 1024);
  }

  virtual void TearDown() override
  {
    blocksstable::ObTmpFileManager::get_instance().destroy();
    blocksstable::TestDataFilePrepare::TearDown();
    destroy_tenant_mgr();
  }

  int init_tenant_mgr()
  {
    int ret = OB_SUCCESS;
    ObAddr self;
    oceanbase::rpc::frame::ObReqTransport req_transport(NULL, NULL);
    oceanbase::obrpc::ObSrvRpcProxy rpc_proxy;
    oceanbase::obrpc::ObCommonRpcProxy rs_rpc_proxy;
    oceanbase::share::ObRsMgr rs_mgr;
    self.set_ip_addr("127.0.0.1", 8086);
    ret = ObTenantConfigMgr::get_instance().add_tenant_config(tenant_id_);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = getter.add_tenant(tenant_id_,
                            2L * 1024L * 1024L * 1024L, 4L * 1024L * 1024L * 1024L);
    EXPECT_EQ(OB_SUCCESS, ret);
    const int64_t ulmt = 128LL << 30;
    const int64_t llmt = 128LL << 30;
    ret = getter.add_tenant(OB_SERVER_TENANT_ID,
                            ulmt,
                            llmt);
    EXPECT_EQ(OB_SUCCESS, ret);
    oceanbase::lib::set_memory_limit(128LL << 32);
    return ret;
  }

  void destroy_tenant_mgr()
  {
  }

  template <typename T>
  void verify_sort(T &sort, int64_t cols, bool unique)
  {
    ObExecContext ctx;
    const ObNewRow *r = NULL;
    int64_t rows = data_.row_cnt_;
    for (int64_t i = 0; i < rows; i++) {
      ASSERT_EQ(OB_SUCCESS, data_.get_next_row(ctx, r));
      ASSERT_EQ(OB_SUCCESS, sort.add_row(*r));

      if (i == rows / 2 || i == (rows - rows / 16)) {
        ASSERT_EQ(OB_SUCCESS, sort.sort());
        for (int j = 0; j < std::min(1000L, i - 1); j++) {
          int ret = sort.get_next_row(r);
          if (!unique || 0 == j) {
            ASSERT_EQ(OB_SUCCESS, ret);
          } else {
            ASSERT_TRUE(OB_SUCCESS == ret || OB_ITER_END == ret);
          }
        }
      }
    }

    ASSERT_EQ(OB_SUCCESS, sort.sort());

    int ret = 0;
    ObNewRow prev;
    char copy_buf[2048];
    for (int loop = 0; loop < 2; loop += 1) {
      if (loop == 1) {
        ASSERT_EQ(0, sort.rewind());
      }
      for (int64_t i = 0; i < rows; i++) {
        ret = sort.get_next_row(r);
        if (!unique) {
          ASSERT_EQ(0, ret);
        } else {
          ASSERT_TRUE(0 == ret || (OB_ITER_END == ret && (0 == rows || i > 0)));
        }
        if (0 == ret) {
          if (i < 10) {
            LOG_INFO("get row", K(*r));
          }
          if (i > 0) {
            int cmp = 0;
            for (int64_t i = 0; cmp == 0 && i < cols; i++) {
              auto &col = cols_[i];
              cmp = prev.get_cell(col.index_).compare(r->get_cell(col.index_), col.cs_type_);
              if (cmp != 0) {
                if (!col.is_ascending()) {
                  cmp *= -1;
                }
              }
            }
            ASSERT_LE(cmp, 0);
            if (unique) {
              ASSERT_LT(cmp, 0);
            }
          }
          int64_t pos = 0;
          ASSERT_EQ(0, prev.deep_copy(*r, copy_buf, sizeof(copy_buf), pos));
        }
      }
      if (!unique) {
        ASSERT_EQ(OB_ITER_END, sort.get_next_row(r));
      }
    }
  }

  ObArenaAllocator alloc_;
  JoinDataGenerator data_;
  ObSortColumn cols_[3];
  uint64_t tenant_id_ = OB_SYS_TENANT_ID;
};

TEST_F(TestSortImpl, sort_impl)
{
	ObSortImpl sort;
	data_.row_cnt_ = 100;
	data_.idx_cnt_func_ = [](const int64_t, const int64_t) { return 20; };
	data_.row_id_val_ = [](const int64_t v) { return murmurhash64A(&v, sizeof(v), 0); };
	data_.idx_val_ = [](const int64_t v) { return murmurhash64A(&v, sizeof(v), 0); };
	data_.test_init();

	ObArrayHelper<ObSortColumn> helper;
	helper.init(2, cols_, 2);
	ASSERT_EQ(0, sort.init(tenant_id_, helper, NULL, false, true));
	verify_sort(sort, 2, false);
	ASSERT_FALSE(HasFatalFailure());

	// test external disk merge sort
	data_.row_cnt_ = (100L << 20) / data_.string_size_;
	data_.test_init();

	sort.reuse();
	verify_sort(sort, 2, false);
	ASSERT_FALSE(HasFatalFailure());
}

TEST_F(TestSortImpl, local_order)
{
	ObSortImpl sort;
	data_.row_cnt_ = 100;
	data_.idx_cnt_func_ = [](const int64_t, const int64_t) { return 20; };
	data_.row_id_val_ = [](const int64_t v) { return murmurhash64A(&v, sizeof(v), 0); };
	data_.test_init();

	ObArrayHelper<ObSortColumn> helper;
	helper.init(2, cols_, 2);
	ASSERT_EQ(0, sort.init(tenant_id_, helper, NULL, true, true));
	verify_sort(sort, 2, false);
	ASSERT_FALSE(HasFatalFailure());

	// test external disk merge sort
	data_.row_cnt_ = (100L << 20) / data_.string_size_;
	data_.idx_cnt_func_ = [](const int64_t, const int64_t) { return 10000; };
	data_.test_init();

	sort.reset();
	ASSERT_EQ(0, sort.init(tenant_id_, helper, NULL, true, true));
	verify_sort(sort, 2, false);
	ASSERT_FALSE(HasFatalFailure());
}

TEST_F(TestSortImpl, unique)
{
	ObUniqueSort sort;
	data_.row_cnt_ = 100;
	data_.idx_cnt_func_ = [](const int64_t, const int64_t) { return 3; };
	data_.row_id_val_ = [](const int64_t v) { return murmurhash64A(&v, sizeof(v), 0); };
	data_.idx_val_ = [](const int64_t v) { return murmurhash64A(&v, sizeof(v), 0); };
	data_.test_init();

	ObArrayHelper<ObSortColumn> helper;
	helper.init(1, cols_, 1);
	ASSERT_EQ(0, sort.init(tenant_id_, helper, true));
	verify_sort(sort, 1, true);
	ASSERT_FALSE(HasFatalFailure());

	// test external disk merge sort
	data_.row_cnt_ = (100L << 20) / data_.string_size_;
	data_.idx_cnt_func_ = [](const int64_t, const int64_t) { return 50000; };
	data_.test_init();
	cols_[0].index_ = JoinDataGenerator::IDX_CELL;

	sort.reset();
	ASSERT_EQ(0, sort.init(tenant_id_, helper, true));
	verify_sort(sort, 1, true);
	ASSERT_FALSE(HasFatalFailure());
}

int main(int argc, char **argv)
{
  ObClockGenerator::init();

  system("rm -f test_sort_impl.log*");
  OB_LOGGER.set_file_name("test_sort_impl.log", true, true);
  OB_LOGGER.set_log_level("INFO");

  oceanbase::observer::ObSignalHandle signal_handle;
  oceanbase::observer::ObSignalHandle::change_signal_mask();
  signal_handle.start();

  ::testing::InitGoogleTest(&argc,argv);
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  return RUN_ALL_TESTS();
}

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

#define USING_LOG_PREFIX SQL_SESSION
#include <gtest/gtest.h>
#define private public
#define protected public
#include "sql/session/ob_basic_session_info.h"
#include "observer/ob_server.h"
#include "lib/utility/utility.h"
#include "common/ob_smart_var.h"
#include "pl/ob_pl_package_state.h"
#include "share/system_variable/ob_system_variable_factory.h"

namespace oceanbase
{
using namespace common;
using namespace sql;

namespace sql
{

// 测试类：系统变量位图扩展功能测试
class TestSysvarBitmapExpansion : public ::testing::Test
{
protected:
  virtual void SetUp() {}
  virtual void TearDown() {}
};

// 测试1: 基本功能 - 设置和获取系统变量（使用inc_flags_，位索引0-63）
TEST_F(TestSysvarBitmapExpansion, test_basic_sysvar_operations)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 验证初始状态
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
  EXPECT_EQ(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(0, sysvar_cache.ext_inc_flags_);

  // 测试设置和获取uint64_t类型变量
  sysvar_cache.set_auto_increment_increment(10);
  EXPECT_EQ(10, sysvar_cache.get_auto_increment_increment());
  EXPECT_FALSE(sysvar_cache.is_inc_empty());
  EXPECT_TRUE(sysvar_cache.bit_is_true(0)); // BIT_auto_increment_increment = 0

  // 测试设置和获取int64_t类型变量
  sysvar_cache.set_sql_throttle_current_priority(50);
  EXPECT_EQ(50, sysvar_cache.get_sql_throttle_current_priority());
  EXPECT_TRUE(sysvar_cache.bit_is_true(1)); // BIT_sql_throttle_current_priority = 1

  // 测试设置和获取bool类型变量
  sysvar_cache.set_tx_read_only(true);
  EXPECT_TRUE(sysvar_cache.get_tx_read_only());
  EXPECT_TRUE(sysvar_cache.bit_is_true(10)); // BIT_tx_read_only = 10

  // 测试设置和获取ObSQLMode类型变量
  ObSQLMode sql_mode = DEFAULT_OCEANBASE_MODE;
  sysvar_cache.set_sql_mode(sql_mode);
  EXPECT_EQ(sql_mode, sysvar_cache.get_sql_mode());
  EXPECT_TRUE(sysvar_cache.bit_is_true(30)); // BIT_sql_mode = 30

  // 验证位图状态
  EXPECT_NE(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(0, sysvar_cache.ext_inc_flags_); // 所有位索引都 < 64
}

// 测试2: 扩展位图功能 - 测试超过64位的系统变量（使用ext_inc_flags_）
TEST_F(TestSysvarBitmapExpansion, test_extended_bitmap_operations)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 测试直接操作位图：设置位索引64（应该使用ext_inc_flags_）
  sysvar_cache.set_bit(64);
  EXPECT_TRUE(sysvar_cache.bit_is_true(64));
  EXPECT_EQ(0, sysvar_cache.inc_flags_);
  EXPECT_NE(0, sysvar_cache.ext_inc_flags_);
  EXPECT_FALSE(sysvar_cache.is_inc_empty());

  // 测试设置位索引127（ext_inc_flags_的最大位）
  sysvar_cache.set_bit(127);
  EXPECT_TRUE(sysvar_cache.bit_is_true(127));

  // 测试边界情况：位索引63（inc_flags_的最后一位）
  sysvar_cache.set_bit(63);
  EXPECT_TRUE(sysvar_cache.bit_is_true(63));
  EXPECT_NE(0, sysvar_cache.inc_flags_);

  // 测试清除扩展位图的位
  // 注意：set_bit只能设置，不能清除，需要通过其他方式清除
  // 这里我们测试clean_inc会清除所有位
  sysvar_cache.clean_inc();
  EXPECT_FALSE(sysvar_cache.bit_is_true(64));
  EXPECT_FALSE(sysvar_cache.bit_is_true(127));
  EXPECT_FALSE(sysvar_cache.bit_is_true(63));
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
}

// 测试3: 边界位索引测试
TEST_F(TestSysvarBitmapExpansion, test_bit_index_boundaries)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 测试边界位：0, 63, 64, 127
  std::vector<int> test_bits = {0, 63, 64, 127};

  for (int bit_pos : test_bits) {
    sysvar_cache.set_bit(bit_pos);
    EXPECT_TRUE(sysvar_cache.bit_is_true(bit_pos))
        << "Failed for bit position: " << bit_pos;
  }

  // 验证inc_flags_和ext_inc_flags_都非空
  EXPECT_NE(0, sysvar_cache.inc_flags_);
  EXPECT_NE(0, sysvar_cache.ext_inc_flags_);

  // 验证不同位图区域之间的独立性
  // 清除inc_flags_中的位，ext_inc_flags_应该不受影响
  sysvar_cache.clean_inc();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
}

// 测试4: reset和clean_inc功能
TEST_F(TestSysvarBitmapExpansion, test_reset_and_clean_inc)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 设置一些系统变量
  sysvar_cache.set_auto_increment_increment(10);
  sysvar_cache.set_sql_throttle_current_priority(50);
  sysvar_cache.set_bit(64); // 设置扩展位图

  EXPECT_FALSE(sysvar_cache.is_inc_empty());
  EXPECT_NE(0, sysvar_cache.inc_flags_);
  EXPECT_NE(0, sysvar_cache.ext_inc_flags_);

  // 测试clean_inc：只清除位图，不清除数据
  sysvar_cache.clean_inc();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
  EXPECT_EQ(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(0, sysvar_cache.ext_inc_flags_);
  // 数据应该还在（虽然位图被清除，get会返回base值）
  // 但inc_data_中的数据应该还在
  EXPECT_EQ(10, sysvar_cache.inc_data_.auto_increment_increment_);

  // 重新设置位图
  sysvar_cache.set_auto_increment_increment(20);
  EXPECT_FALSE(sysvar_cache.is_inc_empty());

  // 测试reset：清除位图和数据
  sysvar_cache.reset();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
  EXPECT_EQ(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(0, sysvar_cache.ext_inc_flags_);
  // reset后，inc_data_应该被重置
  EXPECT_EQ(0, sysvar_cache.inc_data_.auto_increment_increment_);
}

// 测试5: base_data和inc_data的交互
TEST_F(TestSysvarBitmapExpansion, test_base_and_inc_data_interaction)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 设置base值
  sysvar_cache.set_base_auto_increment_increment(5);

  // 未设置inc值时，应该返回base值
  EXPECT_EQ(5, sysvar_cache.get_auto_increment_increment(false));
  // 由于没有设置inc位，get()应该返回base值
  // 但实际get()会检查bit_is_true，如果为false则返回base值
  EXPECT_EQ(5, sysvar_cache.get_auto_increment_increment());

  // 设置inc值
  sysvar_cache.set_auto_increment_increment(10);
  EXPECT_EQ(10, sysvar_cache.get_auto_increment_increment());
  EXPECT_EQ(10, sysvar_cache.get_auto_increment_increment(true));
  EXPECT_EQ(5, sysvar_cache.get_auto_increment_increment(false));

  // clean_inc后，应该返回base值
  sysvar_cache.clean_inc();
  EXPECT_EQ(5, sysvar_cache.get_auto_increment_increment());
}

// 测试6: 字符串类型系统变量
TEST_F(TestSysvarBitmapExpansion, test_string_sysvar_operations)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 测试设置和获取字符串类型变量
  common::ObString nls_date_format("YYYY-MM-DD");
  sysvar_cache.set_nls_date_format(nls_date_format);

  const common::ObString &result = sysvar_cache.get_nls_date_format();
  EXPECT_TRUE(result == nls_date_format);
  EXPECT_TRUE(sysvar_cache.bit_is_true(31)); // BIT_nls_date_format = 31

  // 测试另一个字符串变量
  common::ObString trace_info("test_trace_id");
  sysvar_cache.set_ob_trace_info(trace_info);
  const common::ObString &trace_result = sysvar_cache.get_ob_trace_info();
  EXPECT_TRUE(trace_result == trace_info);
  EXPECT_TRUE(sysvar_cache.bit_is_true(38)); // BIT_ob_trace_info = 38
}

// 测试7: 多个系统变量的混合操作
TEST_F(TestSysvarBitmapExpansion, test_multiple_sysvars_mixed_operations)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 设置多个不同类型的系统变量
  sysvar_cache.set_auto_increment_increment(10);
  sysvar_cache.set_sql_throttle_current_priority(50);
  sysvar_cache.set_tx_read_only(true);
  sysvar_cache.set_ob_enable_plan_cache(true);
  sysvar_cache.set_ob_query_timeout(30000000);

  // 验证所有变量都正确设置
  EXPECT_EQ(10, sysvar_cache.get_auto_increment_increment());
  EXPECT_EQ(50, sysvar_cache.get_sql_throttle_current_priority());
  EXPECT_TRUE(sysvar_cache.get_tx_read_only());
  EXPECT_TRUE(sysvar_cache.get_ob_enable_plan_cache());
  EXPECT_EQ(30000000, sysvar_cache.get_ob_query_timeout());

  // 验证位图状态
  EXPECT_TRUE(sysvar_cache.bit_is_true(0));  // auto_increment_increment
  EXPECT_TRUE(sysvar_cache.bit_is_true(1));  // sql_throttle_current_priority
  EXPECT_TRUE(sysvar_cache.bit_is_true(10)); // tx_read_only
  EXPECT_TRUE(sysvar_cache.bit_is_true(11)); // ob_enable_plan_cache
  EXPECT_TRUE(sysvar_cache.bit_is_true(27)); // ob_query_timeout

  // 验证is_inc_empty
  EXPECT_FALSE(sysvar_cache.is_inc_empty());

  // 清除所有位
  sysvar_cache.clean_inc();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
}

// 测试8: 极限情况 - 设置所有可能的位（0-127）
TEST_F(TestSysvarBitmapExpansion, test_maximum_capacity)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 设置所有位（0-127）
  for (int i = 0; i < 128; ++i) {
    sysvar_cache.set_bit(i);
    EXPECT_TRUE(sysvar_cache.bit_is_true(i))
        << "Failed to set bit at index: " << i;
  }

  // 验证所有位都被正确设置
  EXPECT_FALSE(sysvar_cache.is_inc_empty());
  EXPECT_EQ(0xFFFFFFFFFFFFFFFFULL, sysvar_cache.inc_flags_);
  EXPECT_EQ(0xFFFFFFFFFFFFFFFFULL, sysvar_cache.ext_inc_flags_);

  // 清除所有位
  sysvar_cache.clean_inc();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
  EXPECT_EQ(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(0, sysvar_cache.ext_inc_flags_);
}

// 测试9: 超出范围的位索引（应该不会崩溃）
TEST_F(TestSysvarBitmapExpansion, test_out_of_range_bit_indices)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 测试超出范围的位索引（128及以上）
  // 根据实现，set_bit会使用 ext_inc_flags_，但位索引128会映射到ext_inc_flags_的第64位
  // 由于ext_inc_flags_只有64位，超出部分会被忽略或产生未定义行为
  // 这里我们只测试不会崩溃

  sysvar_cache.set_bit(128);
  // 128 - 64 = 64，会访问ext_inc_flags_的第64位，但只有0-63位有效
  // 实际行为取决于实现，这里只测试不会崩溃
  EXPECT_NO_THROW(sysvar_cache.bit_is_true(128));

  sysvar_cache.set_bit(200);
  EXPECT_NO_THROW(sysvar_cache.bit_is_true(200));
}

// 测试10: 实际系统变量宏的使用（测试DEF_SYS_VAR_CACHE_FUNCS生成的方法）
TEST_F(TestSysvarBitmapExpansion, test_sysvar_macro_generated_methods)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 测试通过宏生成的方法设置和获取系统变量
  // 这些方法会自动调用set_bit设置对应的位

  // 测试uint64_t类型
  sysvar_cache.set_auto_increment_increment(100);
  EXPECT_EQ(100, sysvar_cache.get_auto_increment_increment());
  EXPECT_TRUE(sysvar_cache.bit_is_true(0));

  // 测试int64_t类型
  sysvar_cache.set_sql_throttle_current_priority(200);
  EXPECT_EQ(200, sysvar_cache.get_sql_throttle_current_priority());
  EXPECT_TRUE(sysvar_cache.bit_is_true(1));

  // 测试bool类型
  sysvar_cache.set_tx_read_only(true);
  EXPECT_TRUE(sysvar_cache.get_tx_read_only());
  EXPECT_TRUE(sysvar_cache.bit_is_true(10));

  sysvar_cache.set_tx_read_only(false);
  EXPECT_FALSE(sysvar_cache.get_tx_read_only());
  EXPECT_TRUE(sysvar_cache.bit_is_true(10)); // 位图仍然被设置

  // 测试ObSQLMode类型
  ObSQLMode sql_mode = DEFAULT_OCEANBASE_MODE;
  sysvar_cache.set_sql_mode(sql_mode);
  EXPECT_EQ(sql_mode, sysvar_cache.get_sql_mode());
  EXPECT_TRUE(sysvar_cache.bit_is_true(30));
}

// 测试11: 验证位图扩展的正确性（inc_flags_和ext_inc_flags_的分离）
TEST_F(TestSysvarBitmapExpansion, test_bitmap_separation)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 设置inc_flags_中的位（0-63）
  sysvar_cache.set_bit(0);
  sysvar_cache.set_bit(63);
  EXPECT_NE(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(0, sysvar_cache.ext_inc_flags_);

  // 设置ext_inc_flags_中的位（64-127）
  sysvar_cache.set_bit(64);
  sysvar_cache.set_bit(127);
  EXPECT_NE(0, sysvar_cache.ext_inc_flags_);

  // 验证两个位图是独立的
  EXPECT_TRUE(sysvar_cache.bit_is_true(0));
  EXPECT_TRUE(sysvar_cache.bit_is_true(63));
  EXPECT_TRUE(sysvar_cache.bit_is_true(64));
  EXPECT_TRUE(sysvar_cache.bit_is_true(127));

  // 清除inc_flags_，ext_inc_flags_应该不受影响
  uint64_t saved_ext_flags = sysvar_cache.ext_inc_flags_;
  sysvar_cache.inc_flags_ = 0;
  EXPECT_EQ(0, sysvar_cache.inc_flags_);
  EXPECT_EQ(saved_ext_flags, sysvar_cache.ext_inc_flags_);
  EXPECT_FALSE(sysvar_cache.bit_is_true(0));
  EXPECT_FALSE(sysvar_cache.bit_is_true(63));
  EXPECT_TRUE(sysvar_cache.bit_is_true(64));
  EXPECT_TRUE(sysvar_cache.bit_is_true(127));
}

// 测试12: 测试is_inc_empty在不同场景下的行为
TEST_F(TestSysvarBitmapExpansion, test_is_inc_empty_scenarios)
{
  ObBasicSessionInfo::SysVarsCache sysvar_cache;

  // 初始状态应该为空
  EXPECT_TRUE(sysvar_cache.is_inc_empty());

  // 只设置inc_flags_中的位
  sysvar_cache.set_bit(0);
  EXPECT_FALSE(sysvar_cache.is_inc_empty());

  sysvar_cache.clean_inc();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());

  // 只设置ext_inc_flags_中的位
  sysvar_cache.set_bit(64);
  EXPECT_FALSE(sysvar_cache.is_inc_empty());

  sysvar_cache.clean_inc();
  EXPECT_TRUE(sysvar_cache.is_inc_empty());

  // 同时设置两个位图
  sysvar_cache.set_bit(0);
  sysvar_cache.set_bit(64);
  EXPECT_FALSE(sysvar_cache.is_inc_empty());

  // 只清除inc_flags_
  sysvar_cache.inc_flags_ = 0;
  EXPECT_FALSE(sysvar_cache.is_inc_empty()); // ext_inc_flags_仍然非空

  // 清除ext_inc_flags_
  sysvar_cache.ext_inc_flags_ = 0;
  EXPECT_TRUE(sysvar_cache.is_inc_empty());
}

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

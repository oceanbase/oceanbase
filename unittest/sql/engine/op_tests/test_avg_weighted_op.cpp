/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 *
 * avgWeighted 表达式单测
 *
 * 公式: Σ(value × weight) / Σ(weight)
 *   - value 或 weight 为 NULL 的行跳过
 *   - 所有有效行权重之和为 0 时返回 NULL
 *   - 输入为空或全部行均为 NULL 时返回 NULL
 *
 * 依赖: sql_func_extension_mode='ClickHouse' 才能解析 avgWeighted 语法。
 * 每个测试通过 WITH_TENANT_CONFS({"sql_func_extension_mode","ClickHouse"}) 在
 * prepare()/resolve_sql() 前注入租户配置覆盖。
 *
 * 期望值均选取能整除的整数，方便 verify_ordered 的字符串比较。
 *
 * 测试分组:
 *   Group 1: 标量聚合 (scalar_agg_test)
 *   Group 2: 分组聚合 (merge_groupby_test / hash_groupby_test)
 *   Group 3: 窗口函数 (window_function_test)
 *   Group 4: 窗口函数流式模式 (enable_streaming)
 */

#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase
{
namespace sql
{

class AvgWeightedOpTest : public OpTestKit {};

// ============================================================================
// Group 1: 标量聚合
// ============================================================================

// 基础用例: {10,2},{20,3} → (10*2+20*3)/(2+3) = 80/5 = 16
TEST_F(AvgWeightedOpTest, ScalarBasicDouble)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 单行: 结果等于 value 本身 (42*7/7 = 42)
TEST_F(AvgWeightedOpTest, ScalarSingleRow)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{42.0, 7.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"42"}}));
  }
}

// 等权重退化为算术平均: (10+20+30)/3 = 20
TEST_F(AvgWeightedOpTest, ScalarEqualWeightsIsArithmeticAvg)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 1.0}, {20.0, 1.0}, {30.0, 1.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"20"}}));
  }
}

// value 为 NULL 的行跳过: {6,1},{NULL,2},{12,2} → (6+24)/3 = 10
TEST_F(AvgWeightedOpTest, ScalarNullValueSkipped)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({
            {6.0, 1.0},
            {TestValue::null(), 2.0},
            {12.0, 2.0}
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"10"}}));
  }
}

// weight 为 NULL 的行跳过: {6,1},{12,NULL},{18,2} → (6+36)/3 = 14
TEST_F(AvgWeightedOpTest, ScalarNullWeightSkipped)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({
            {6.0, 1.0},
            {12.0, TestValue::null()},
            {18.0, 2.0}
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"14"}}));
  }
}

// 所有 value 均为 NULL → 结果为 NULL
TEST_F(AvgWeightedOpTest, ScalarAllNullValues)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({
            {TestValue::null(), 1.0},
            {TestValue::null(), 2.0}
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"NULL"}}));
  }
}

// 空表 → NULL
TEST_F(AvgWeightedOpTest, ScalarEmptyTable)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"NULL"}}));
  }
}

// 权重之和为 0 → NULL (除零)
TEST_F(AvgWeightedOpTest, ScalarZeroWeightSumIsNull)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 0.0}, {20.0, 0.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"NULL"}}));
  }
}

// 整数类型: {10,2},{20,3} → 16
TEST_F(AvgWeightedOpTest, ScalarIntegerTypes)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v int, w int")
        .select("avgWeighted(v, w)")
        .with_data({{10, 2}, {20, 3}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 负权重: {10,2},{20,-1} → (20-20)/(2-1) = 0/1 = 0
TEST_F(AvgWeightedOpTest, ScalarNegativeWeightsAllowed)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, -1.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"0"}}));
  }
}

// 表达式作为参数: avgWeighted(v*2, w+1)
// {10,1},{20,2}: (10*2*(1+1) + 20*2*(2+1)) / ((1+1)+(2+1)) = (40+120)/5 = 32
TEST_F(AvgWeightedOpTest, ScalarExpressionArgs)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v * 2, w + 1)")
        .with_data({{10.0, 1.0}, {20.0, 2.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"32"}}));
  }
}

// ============================================================================
// Group 1 新增: 数据类型扩展 (标量聚合, 均含 dual_format_check)
// ============================================================================

// bigint 类型: {10,2},{20,3} → 16  (返回类型固定为 double)
TEST_F(AvgWeightedOpTest, ScalarBigintBasic)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v bigint, w bigint")
        .select("avgWeighted(v, w)")
        .with_data({{10, 2}, {20, 3}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}
#if 0
// decimal(12,2) 类型: 展开后转 double 计算, 结果仍为 double
// {10.00,2.00},{20.00,3.00} → 16
TEST_F(AvgWeightedOpTest, ScalarDecimalBasic)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v decimal(12,2), w decimal(12,2)")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}
#endif
// float 类型: {10.0,2.0},{20.0,3.0} → 16
TEST_F(AvgWeightedOpTest, ScalarFloatBasic)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v float, w float")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 混合类型: double value + int weight → 验证隐式类型转换
// {10.0,2},{20.0,3} → 16
TEST_F(AvgWeightedOpTest, ScalarMixedDoubleValueIntWeight)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w int")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2}, {20.0, 3}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 正交: bigint × NULL value 跳过
// {6,1},{NULL,2},{12,2} → (6*1+12*2)/(1+2) = 30/3 = 10
TEST_F(AvgWeightedOpTest, ScalarBigintNullValueSkipped)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v bigint, w bigint")
        .select("avgWeighted(v, w)")
        .with_data({
            {6, 1},
            {TestValue::null(), 2},
            {12, 2}
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"10"}}));
  }
}

// 正交: decimal × 权重和为 0 → NULL (除零保护)
TEST_F(AvgWeightedOpTest, ScalarDecimalZeroWeightSum)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v decimal(12,2), w decimal(12,2)")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 0.0}, {20.0, 0.0}})
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"NULL"}}));
  }
}

// ============================================================================
// Group 1 新增: 向量化模式正交 (double 数据 × 3 种执行模式)
// ============================================================================
//
//  执行模式说明:
//    Vec2.0  → with_rich_format(true)                  (ObScalarAggregateVecOp)
//    Vec1.0  → with_rich_format(false)                 (ObScalarAggregateOp, uniform format)
//    非向量化 → WITH_TENANT_CONFS({"_rowsets_enabled","False"})  (行格式, row-by-row)
//
//  enable_dual_format_check() 已同时覆盖 Vec1.0 和 Vec2.0；
//  此处追加 3 个显式单模式用例作为正交补充。

// Vec2.0 显式: {10,2},{20,3} → 16
TEST_F(AvgWeightedOpTest, ScalarVec2Explicit)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}})
        .with_rich_format(true)
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// Vec1.0 显式: rich_format=false, batch=256 (uniform format)
TEST_F(AvgWeightedOpTest, ScalarVec1Explicit)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}})
        .with_rich_format(false)
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 非向量化: _rowsets_enabled=False 关闭 rowsets，行格式逐行执行
TEST_F(AvgWeightedOpTest, ScalarNonVecExplicit)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"},
                    {"_rowsets_enabled", "False"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}})
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 正交: bigint × Vec1.0
TEST_F(AvgWeightedOpTest, ScalarBigintVec1)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v bigint, w bigint")
        .select("avgWeighted(v, w)")
        .with_data({{10, 2}, {20, 3}})
        .with_rich_format(false)
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 正交: bigint × 非向量化
TEST_F(AvgWeightedOpTest, ScalarBigintNonVec)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"},
                    {"_rowsets_enabled", "False"}) {
    auto result = scalar_agg_test()
        .table("t", "v bigint, w bigint")
        .select("avgWeighted(v, w)")
        .with_data({{10, 2}, {20, 3}})
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// 正交: NULL 跳过 × 非向量化
// {6,1},{NULL_v,2},{12,2} → (6+24)/3 = 10
TEST_F(AvgWeightedOpTest, ScalarNullSkipNonVec)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"},
                    {"_rowsets_enabled", "False"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({
            {6.0, 1.0},
            {TestValue::null(), 2.0},
            {12.0, 2.0}
        })
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"10"}}));
  }
}

// batch_size=3 跨 batch 聚合: 4 行数据分 (3+1) 两批
// {10,2},{20,3},{10,2},{20,3} → (20+60+20+60)/(2+3+2+3) = 160/10 = 16
TEST_F(AvgWeightedOpTest, ScalarSmallBatch)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = scalar_agg_test()
        .table("t", "v double, w double")
        .select("avgWeighted(v, w)")
        .with_data({{10.0, 2.0}, {20.0, 3.0}, {10.0, 2.0}, {20.0, 3.0}})
        .with_batch_size(3)
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(1, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{"16"}}));
  }
}

// ============================================================================
// Group 2: 分组聚合
// ============================================================================

// MergeGroupBy: 预排序输入
// p=1: {10,2},{20,3} → 16; p=2: {6,1},{12,2} → 10
TEST_F(AvgWeightedOpTest, MergeGroupByBasic)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = merge_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_sorted_data({
            {1, 10.0, 2.0}, {1, 20.0, 3.0},
            {2,  6.0, 1.0}, {2, 12.0, 2.0}
        }, "p ASC")
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, 16}, {2, 10}}));
  }
}

// HashGroupBy: 无序输入
// 分组结果与 MergeGroupBy 相同，但允许无序输出
TEST_F(AvgWeightedOpTest, HashGroupByBasic)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = hash_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_data({
            {2,  6.0, 1.0},
            {1, 10.0, 2.0},
            {2, 12.0, 2.0},
            {1, 20.0, 3.0}
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, 16}, {2, 10}}));
  }
}

// HashGroupBy: 组内存在 NULL，NULL 行跳过后仍能得到正确分组结果
// p=1: {6,1},{NULL_v,2},{12,2} → (6+24)/3 = 10
// p=2: {10,1},{20,NULL_w},{30,2} → (10+60)/3 = 70/3... 换数据
// p=2: {4,1},{8,2} → (4+16)/3 = 20/3... 不整除，换
// p=2: {12,1},{12,2} → (12+24)/3 = 36/3 = 12
TEST_F(AvgWeightedOpTest, HashGroupByWithNulls)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = hash_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_data({
            {1,  6.0, 1.0},
            {1,  TestValue::null(), 2.0},   // value NULL, 跳过
            {1, 12.0, 2.0},                  // p=1: (6+24)/3 = 10
            {2, 12.0, 1.0},
            {2, 12.0, TestValue::null()},    // weight NULL, 跳过
            {2, 12.0, 2.0}                   // p=2: (12+24)/3 = 36/3 = 12
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, 10}, {2, 12}}));
  }
}

// ============================================================================
// Group 2 新增: 分组聚合 - 数据类型扩展
// ============================================================================

// MergeGroupBy: bigint 类型
// p=1: {10,2},{20,3} → 16; p=2: {6,1},{12,2} → 10
TEST_F(AvgWeightedOpTest, MergeGroupByBigint)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = merge_groupby_test()
        .table("t", "p int, v bigint, w bigint")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_sorted_data({
            {1, 10, 2}, {1, 20, 3},
            {2,  6, 1}, {2, 12, 2}
        }, "p ASC")
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, 16}, {2, 10}}));
  }
}

#if 0
// HashGroupBy: decimal(12,2) 类型
// p=1: {10.00,2.00},{20.00,3.00} → 16; p=2: {6.00,1.00},{12.00,2.00} → 10
TEST_F(AvgWeightedOpTest, HashGroupByDecimal)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = hash_groupby_test()
        .table("t", "p int, v decimal(12,2), w decimal(12,2)")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_data({
            {2,  6.0, 1.0},
            {1, 10.0, 2.0},
            {2, 12.0, 2.0},
            {1, 20.0, 3.0}
        })
        .enable_dual_format_check()
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, 16}, {2, 10}}));
  }
}
#endif
// ============================================================================
// Group 2 新增: 分组聚合 - 向量化模式正交
// ============================================================================

// MergeGroupBy Vec1.0 (rich_format=false, uniform format)
TEST_F(AvgWeightedOpTest, MergeGroupByVec1)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = merge_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_sorted_data({
            {1, 10.0, 2.0}, {1, 20.0, 3.0},
            {2,  6.0, 1.0}, {2, 12.0, 2.0}
        }, "p ASC")
        .with_rich_format(false)
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, 16}, {2, 10}}));
  }
}

// MergeGroupBy 非向量化 (_rowsets_enabled=False)
TEST_F(AvgWeightedOpTest, MergeGroupByNonVec)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"},
                    {"_rowsets_enabled", "False"}) {
    auto result = merge_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_sorted_data({
            {1, 10.0, 2.0}, {1, 20.0, 3.0},
            {2,  6.0, 1.0}, {2, 12.0, 2.0}
        }, "p ASC")
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_ordered({{1, 16}, {2, 10}}));
  }
}

// HashGroupBy Vec1.0 (rich_format=false, uniform format)
TEST_F(AvgWeightedOpTest, HashGroupByVec1)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = hash_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_data({
            {2,  6.0, 1.0},
            {1, 10.0, 2.0},
            {2, 12.0, 2.0},
            {1, 20.0, 3.0}
        })
        .with_rich_format(false)
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, 16}, {2, 10}}));
  }
}

// HashGroupBy 非向量化 (_rowsets_enabled=False)
TEST_F(AvgWeightedOpTest, HashGroupByNonVec)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"},
                    {"_rowsets_enabled", "False"}) {
    auto result = hash_groupby_test()
        .table("t", "p int, v double, w double")
        .select("p, avgWeighted(v, w)")
        .group_by("p")
        .with_data({
            {2,  6.0, 1.0},
            {1, 10.0, 2.0},
            {2, 12.0, 2.0},
            {1, 20.0, 3.0}
        })
        .run(engine_);
    EXPECT_EQ(2, result.row_count());
    EXPECT_TRUE(result.verify_unordered({{1, 16}, {2, 10}}));
  }
}

// ============================================================================
// Group 3: 窗口函数
// ============================================================================

#if 0 // 单测框架窗口函数展开未支持, 先屏蔽掉
// OVER (PARTITION BY p): 每行看到整个分区结果
// p=1: {10,2},{20,3} → 16; p=2: {6,1},{12,2} → 10
TEST_F(AvgWeightedOpTest, WindowFullPartitionNoOrder)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, v double, w double")
        .select("p, v, avgWeighted(v, w) over (partition by p)")
        .with_sorted_data({
            {1, 10.0, 2.0},
            {1, 20.0, 3.0},
            {2,  6.0, 1.0},
            {2, 12.0, 2.0}
        }, "p ASC")
        .run(engine_);
    EXPECT_EQ(4, result.row_count());
    // 同分区所有行看到相同的加权平均值
    EXPECT_TRUE(result.verify_ordered({
        {1, 10.0, 16},
        {1, 20.0, 16},
        {2,  6.0, 10},
        {2, 12.0, 10}
    }));
  }
}

// OVER (PARTITION BY p ORDER BY o ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW):
// 累积加权平均
// 数据: {6,1},{12,2},{20,3}
//   row1: 6*1/1 = 6
//   row2: (6*1+12*2)/(1+2) = 30/3 = 10
//   row3: (6*1+12*2+20*3)/(1+2+3) = 90/6 = 15
TEST_F(AvgWeightedOpTest, WindowCumulativeFrame)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {1, 1,  6.0, 1.0},
            {1, 2, 12.0, 2.0},
            {1, 3, 20.0, 3.0},
            {2, 1,  4.0, 1.0},
            {2, 2,  8.0, 1.0}
        }, "p ASC, o ASC")
        .run(engine_);
    EXPECT_EQ(5, result.row_count());
    // p=1: 累积加权平均; p=2: 等权重退化为算术平均
    EXPECT_TRUE(result.verify_ordered({
        {1, 1,  6},   // 6/1 = 6
        {1, 2, 10},   // 30/3 = 10
        {1, 3, 15},   // 90/6 = 15
        {2, 1,  4},   // 4/1 = 4
        {2, 2,  6}    // 12/2 = 6
    }));
  }
}

// 窗口帧内 value 为 NULL 的行跳过:
// {6,1},{NULL_v,2},{12,2}
//   row1: 6
//   row2: 帧=[row1,row2], row2 v=NULL 跳过 → 6*1/1 = 6
//   row3: (6*1+12*2)/(1+2) = 10
TEST_F(AvgWeightedOpTest, WindowNullValueSkippedInFrame)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {1, 1,  6.0,              1.0},
            {1, 2,  TestValue::null(), 2.0},
            {1, 3, 12.0,              2.0}
        }, "p ASC, o ASC")
        .run(engine_);
    EXPECT_EQ(3, result.row_count());
    EXPECT_TRUE(result.verify_ordered({
        {1, 1,  6},
        {1, 2,  6},
        {1, 3, 10}
    }));
  }
}

// 多个分区 + 多行窗口函数混用
// avgWeighted 与 sum(v) 共用同一分区/帧
TEST_F(AvgWeightedOpTest, WindowMixedWithSum)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, "
                "avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), "
                "sum(v) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {1, 1,  6.0, 1.0},
            {1, 2, 12.0, 2.0},
            {1, 3, 18.0, 3.0}
        }, "p ASC, o ASC")
        .run(engine_);
    EXPECT_EQ(3, result.row_count());
    // avgWeighted: 6, 10, (6+24+54)/6=84/6=14
    // sum(v):      6, 18, 36
    EXPECT_TRUE(result.verify_ordered({
        {1, 1,  6,  6},
        {1, 2, 10, 18},
        {1, 3, 14, 36}
    }));
  }
}

// 两个不同粒度的分区窗口
// avgW_by_p:  PARTITION BY p     ORDER BY o
// avgW_by_p2: PARTITION BY p%2   ORDER BY p, o
//
// 数据: p=2(o=1,v=10,w=2),(o=2,v=20,w=3); p=1(o=1,v=6,w=1),(o=2,v=12,w=2); p=3(o=1,v=10,w=1)
// 排序: p%2 ASC, p ASC, o ASC
//
// avgW_by_p 各行:
//   p=2,o=1: 10*2/2=10;   p=2,o=2: (20+60)/5=16
//   p=1,o=1: 6;            p=1,o=2: 30/3=10
//   p=3,o=1: 10
//
// avgW_by_p2 各行 (累积):
//   p%2=0: p=2,o=1 → 10;  p=2,o=2 → 16
//   p%2=1: p=1,o=1 → 6;   p=1,o=2 → 10;
//          p=3,o=1 → (6*1+12*2+10*1)/(1+2+1) = 40/4 = 10
TEST_F(AvgWeightedOpTest, WindowTwoDifferentPartitions)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p%2, p, o, "
                "avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), "
                "avgWeighted(v, w) over (partition by p%2 order by p, o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {2, 1, 10.0, 2.0},
            {2, 2, 20.0, 3.0},
            {1, 1,  6.0, 1.0},
            {1, 2, 12.0, 2.0},
            {3, 1, 10.0, 1.0}   // v=10,w=1 使 p%2=1 末行结果整除: 40/4=10
        }, "p%2 ASC, p ASC, o ASC")
        .run(engine_);
    EXPECT_EQ(5, result.row_count());
    // 输出列: p%2, p, o, avgW_by_p, avgW_by_p2
    EXPECT_TRUE(result.verify_ordered({
        {0, 2, 1, 10, 10},
        {0, 2, 2, 16, 16},
        {1, 1, 1,  6,  6},
        {1, 1, 2, 10, 10},
        {1, 3, 1, 10, 10}
    }));
  }
}

// ============================================================================
// Group 4: 窗口函数流式模式
// ============================================================================

// batch_size=2, 分区边界恰好跨 batch
// p=1: {6,1},{12,2} → 6,10; p=2: {4,1},{8,1} → 4,6
TEST_F(AvgWeightedOpTest, StreamingBatchCrossPartition)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {1, 1,  6.0, 1.0},
            {1, 2, 12.0, 2.0},
            {2, 1,  4.0, 1.0},
            {2, 2,  8.0, 1.0}
        }, "p ASC, o ASC")
        .enable_streaming()
        .with_batch_size(2)
        .run(engine_);
    EXPECT_EQ(4, result.row_count());
    EXPECT_TRUE(result.verify_ordered({
        {1, 1,  6},
        {1, 2, 10},
        {2, 1,  4},
        {2, 2,  6}
    }));
  }
}

// batch_size=1 极端场景: 每次只处理一行
TEST_F(AvgWeightedOpTest, StreamingBatchSizeOne)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {1, 1,  6.0, 1.0},
            {1, 2, 12.0, 2.0},
            {1, 3, 20.0, 3.0}
        }, "p ASC, o ASC")
        .enable_streaming()
        .with_batch_size(1)
        .run(engine_);
    EXPECT_EQ(3, result.row_count());
    EXPECT_TRUE(result.verify_ordered({
        {1, 1,  6},
        {1, 2, 10},
        {1, 3, 15}
    }));
  }
}

// 流式模式 + rescan: 多次 rescan 结果一致
TEST_F(AvgWeightedOpTest, StreamingRescanConsistency)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_sorted_data({
            {1, 1,  6.0, 1.0},
            {1, 2, 12.0, 2.0},
            {2, 1,  4.0, 1.0},
            {2, 2,  8.0, 1.0}
        }, "p ASC, o ASC")
        .enable_streaming()
        .with_batch_size(3)
        .with_rescan_times(3)
        .run(engine_);
    EXPECT_EQ(4, result.row_count());
    // 每次 rescan 产生相同结果
    EXPECT_TRUE(result.verify_ordered({
        {1, 1,  6},
        {1, 2, 10},
        {2, 1,  4},
        {2, 2,  6}
    }));
  }
}

// 大数据量 Generator 模式: 仅验证行数不崩溃
// 50 个分区 × 20 行/分区 = 1000 行
TEST_F(AvgWeightedOpTest, WindowLargeDataGenerator)
{
  WITH_TENANT_CONFS({"sql_func_extension_mode", "ClickHouse"}) {
    auto result = window_function_test()
        .table("t", "p int, o int, v double, w double")
        .select("p, o, avgWeighted(v, w) over (partition by p order by o "
                "ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)")
        .with_data_generator(1000,
            [](int64_t i) -> TestValue { return (int)(i / 20); },      // p: 0..49
            [](int64_t i) -> TestValue { return (int)(i % 20); },      // o: 0..19
            [](int64_t i) -> TestValue { return (double)(i % 7 + 1); }, // v: 1..7 cyclic
            [](int64_t i) -> TestValue { return (double)(i % 3 + 1); }) // w: 1..3 cyclic
        .enable_streaming()
        .with_batch_size(32)
        .run(engine_);
    EXPECT_EQ(1000, result.row_count());
  }
}
#endif

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_avg_weighted_op.log*");
  OB_LOGGER.set_file_name("test_avg_weighted_op.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  common::ObPLogWriterCfg log_cfg;
  OB_LOGGER.init(log_cfg, false);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}

# OpTest Framework 使用指南

OceanBase SQL 算子单元测试框架，位于 `unittest/sql/engine/op_tests/`。定位介于轻量表达式测试和完整优化器测试之间，通过 SQL 驱动的方式测试向量化算子。

## 目录

1. [快速上手](#1-快速上手)
2. [表达式测试](#2-表达式测试)
3. [算子单测](#3-算子单测)
4. [数据注入方式](#4-数据注入方式)
5. [Generator 与大数据测试](#5-generator-与大数据测试)
6. [结果验证](#6-结果验证)
7. [Rescan 测试](#7-rescan-测试)
8. [CSV 文件数据源](#8-csv-文件数据源)
9. [Dump 与大数据场景](#9-dump-与大数据场景)
10. [如何新增算子 SpecBuilder](#10-如何新增算子-specbuilder)
11. [格式模式（1.0/2.0）](#11-格式模式1020)
12. [租户配置与测试宏](#12-租户配置与测试宏)
13. [附录：支持的数据类型](#13-附录支持的数据类型)
14. [附录：Fluent API 完整列表](#14-附录fluent-api完整列表)

---

## 1. 快速上手

### 编译与运行

```bash
cd build_release
ob-make test_material_op           # 编译单个目标
./unittest/sql/engine/op_tests/test_material_op   # 运行
```

### 编译目标一览（`CMakeLists.txt`）

当前 `op_tests/` 下已注册的 gtest 可执行文件（`sql_unittest(...)`）：

| 目标 | 说明 |
|------|------|
| `test_material_op` | Material |
| `test_op_test_engine` | `OpTestEngine` / 配置 / 基础设施 |
| `test_limit_op` | Limit |
| `test_scalar_agg_op` | Scalar Aggregate |
| `test_window_function_op` | Window Function |
| `test_merge_groupby_op` | Merge Group By |
| `test_hash_groupby_op` | Hash Group By |
| `test_expand_op` | Expand（ROLLUP/CUBE 等） |
| `test_merge_join_op` | Merge Join |
| `test_type_coverage` | 类型覆盖 |
| `test_hash_join_op` | Hash Join |
| `test_nested_loop_join_op` | Nested Loop Join |

`test_data/` 会在 CMake 配置时复制到构建目录；`test_window_function_datahub` 目前在 `CMakeLists.txt` 中注释掉（`ob_op_test_datahub.h` 与部分头文件中的 `#define private public` 宏污染冲突，需单独编译单元隔离后再启用）。

### 最小示例

```cpp
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

class MyTest : public OpTestKit {};

TEST_F(MyTest, BasicTest)
{
  auto result = material_test()
      .table("t", "a int, b int")    // 注册表
      .select("a, b")                // SELECT 表达式
      .with_data({{1, 2}, {3, 4}})   // 注入测试数据
      .run(engine_);                 // 执行

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1, 2}, {3, 4}}));
}
```

### 核心流程

```
table() → select() → with_data() → run(engine_) → verify()
```

`run()` 内部执行流程：
```
register_table → resolve_sql → generate_exprs → create_mock_data_source → execute → collect_results
```

### 框架组件

| 文件 | 职责 |
|------|------|
| `ob_op_test_kit.h` | 测试 Fixture + 工厂方法 |
| `ob_op_test_base.h` | CRTP 基类 `OpSpecBuilder<Derived>` + fluent API + `run()` |
| `ob_op_test_engine.h` | 继承 `TestSqlUtils`，提供表达式 CG + 执行 |
| `ob_op_test_data_source.h` | `MockDataSourceOp` 模拟 TableScan |
| `ob_op_test_result.h` | 结果收集 + `verify_ordered()` / `verify_column()` |
| `ob_op_test_types.h` | `TestValue`, `TestRow`, `ColumnGenerator`, `gen::` 生成器 |
| `ob_op_test_file_data.h` | CSV 文件读取器 `CsvDataReader` |
| `ob_op_test_material.h` | Material 算子 SpecBuilder |
| `ob_op_test_limit.h` | Limit 算子 SpecBuilder |
| `ob_op_test_scalar_aggregate.h` | Scalar Aggregate 算子 SpecBuilder |
| `ob_op_test_window_function.h` | Window Function SpecBuilder |
| `ob_op_test_merge_groupby.h` | Merge Group By SpecBuilder |
| `ob_op_test_hash_groupby.h` | Hash Group By SpecBuilder |
| `ob_op_test_expand.h` | Expand SpecBuilder |
| `ob_op_test_merge_join.h` | Merge Join SpecBuilder |
| `ob_op_test_hash_join.h` | Hash Join SpecBuilder |
| `ob_op_test_nested_loop_join.h` | Nested Loop Join SpecBuilder |
| `ob_op_test_join_base.h` | 双表 Join 基类 `JoinSpecBuilder<Derived>`（左右子节点、`.on()` / `.join_type()` 等） |
| `ob_op_test_datahub.h` | Datahub 相关（默认勿与全量 op 头混编，见 CMake 注释） |

`OpTestKit` 工厂方法见 `ob_op_test_kit.h`：`material_test()`、`expr_unit_test()`、`limit_test()`、`scalar_agg_test()`、`window_function_test()`、`merge_groupby_test()`、`hash_groupby_test()`、`expand_test()`、`merge_join_test()`、`hash_join_test()`、`nested_loop_join_test()`。

---

## 2. 表达式测试

使用 `expr_unit_test()` 简化接口，Material bypass 模式直接传递数据。

### 基本用法

```cpp
// 算术表达式
auto result = expr_unit_test()
    .columns("a int, b int")        // 定义输入列（无需 table()）
    .with_expr("a + b")             // 待测表达式（无需 select()）
    .with_data({{1, 2}, {3, 4}})
    .run(engine_);

EXPECT_TRUE(result.verify_ordered({{"3"}, {"7"}}));
```

### 多种表达式

```cpp
// 字符串函数
auto r1 = expr_unit_test()
    .columns("s varchar(32)")
    .with_expr("UPPER(s)")
    .with_data({{"hello"}, {"world"}})
    .run(engine_);
EXPECT_TRUE(r1.verify_ordered({{"HELLO"}, {"WORLD"}}));

// CASE WHEN
auto r2 = expr_unit_test()
    .columns("a int")
    .with_expr("CASE WHEN a > 2 THEN 'big' ELSE 'small' END")
    .with_data({{1}, {3}, {5}})
    .run(engine_);
EXPECT_TRUE(r2.verify_ordered({{"small"}, {"big"}, {"big"}}));

// NULL 处理
auto r3 = expr_unit_test()
    .columns("a int, b int")
    .with_expr("a + b")
    .with_data({{1, 2}, {TestValue::null(), 3}})
    .run(engine_);
EXPECT_TRUE(r3.verify_ordered({{"3"}, {"NULL"}}));
```

### 多列输出表达式

```cpp
auto result = expr_unit_test()
    .columns("a int, b int")
    .with_expr("a + b, a * b, a - b")
    .with_data({{3, 5}})
    .run(engine_);

EXPECT_TRUE(result.verify_ordered({{"8", "15", "-2"}}));
```

---

## 3. 算子单测

### Material 算子

```cpp
auto result = material_test()
    .table("t", "a int, b varchar(16)")
    .select("a, b")
    .with_data({{1, "hello"}, {2, "world"}})
    .run(engine_);

EXPECT_EQ(2, result.row_count());
EXPECT_TRUE(result.verify_ordered({{1, "hello"}, {2, "world"}}));
```

**Bypass 模式**（直通不物化）：

```cpp
auto result = material_test()
    .table("t", "a int")
    .select("a")
    .with_data({{1}, {2}})
    .with_bypass(true)
    .run(engine_);
```

### Limit 算子

```cpp
// LIMIT
auto r1 = limit_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_data({{1, 10}, {2, 20}, {3, 30}, {4, 40}})
    .limit(2)
    .run(engine_);
EXPECT_EQ(2, r1.row_count());

// LIMIT + OFFSET
auto r2 = limit_test()
    .table("t", "a int")
    .select("a")
    .with_data({{1}, {2}, {3}, {4}, {5}})
    .limit(2)
    .offset(1)
    .run(engine_);
EXPECT_EQ(2, r2.row_count());

// 排序数据 + LIMIT
auto r3 = limit_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_sorted_data({{3, 30}, {2, 20}, {1, 10}}, "a ASC")
    .limit(2)
    .run(engine_);
```

### Scalar Aggregate 算子

```cpp
// 基本聚合
auto r1 = scalar_agg_test()
    .table("t", "a int, b int")
    .select("COUNT(*), SUM(a), AVG(b)")
    .with_data({{1, 10}, {2, 20}, {3, 30}})
    .run(engine_);
EXPECT_TRUE(r1.verify_ordered({{"3", "6", "20"}}));

// 空表行为：COUNT 返回 0，SUM 返回 NULL
auto r2 = scalar_agg_test()
    .table("t", "a int")
    .select("COUNT(*), SUM(a)")
    .with_data({})
    .run(engine_);
EXPECT_TRUE(r2.verify_ordered({{"0", "NULL"}}));

// COUNT DISTINCT
auto r3 = scalar_agg_test()
    .table("t", "a int")
    .select("COUNT(DISTINCT a)")
    .with_data({{1}, {2}, {2}, {3}, {3}, {3}})
    .enable_hash_base_distinct(true)
    .run(engine_);
EXPECT_TRUE(r3.verify_ordered({{"3"}}));
```

### Window Function 算子

数据需按 **PARTITION BY + ORDER BY** 涉及表达式预先排好序（与真实执行计划一致）。工厂：`window_function_test()`。

```cpp
auto result = window_function_test()
    .table("t", "p int, o int, win1 int, win2 int, win3 int")
    .select("p%2, p, o, win3,"
            "rank() over (partition by p%2, p order by o),"
            "rank() over (partition by p%2 order by p, o)")
    .with_sorted_data({/* 按 p%2, p, o 排序的行 */}, "p%2 ASC, p ASC, o ASC")
    .run(engine_);
```

### MergeGroupBy 算子

输入需按 **GROUP BY** 键有序；常用 `with_sorted_data(..., "a ASC")`。工厂：`merge_groupby_test()`，也可直接使用 `MergeGroupByTestSpec()`。

```cpp
auto result = merge_groupby_test()
    .table("t", "a int, b int")
    .select("a, SUM(b)")
    .group_by("a")
    .with_sorted_data({{1, 10}, {1, 20}, {2, 30}, {2, 40}}, "a ASC")
    .enable_dual_format_check()
    .run(engine_);
EXPECT_TRUE(result.verify_ordered({{1, 30}, {2, 70}}));
```

### HashGroupBy 算子

对输入顺序无要求；无序输出可用 `verify_unordered`。工厂：`hash_groupby_test()` 或 `HashGroupByTestSpec()`。

```cpp
auto result = hash_groupby_test()
    .table("t", "a int")
    .select("a, COUNT(*)")
    .group_by("a")
    .with_data({{2}, {1}, {2}, {1}, {3}})
    .enable_dual_format_check()
    .run(engine_);
EXPECT_TRUE(result.verify_unordered({{1, 2}, {2, 2}, {3, 1}}));
```

### Expand 算子

用于 **ROLLUP / GROUPING SETS** 等展开；API 含 `group_by_rollup(...)`、`group_by_grouping_sets(...)` 等（见 `ob_op_test_expand.h`）。工厂：`expand_test()`。

```cpp
auto result = expand_test()
    .table("t", "a int")
    .select("a")
    .group_by_rollup("a")
    .with_data({{1}, {2}, {3}})
    .run(engine_);
EXPECT_TRUE(result.verify_unordered({
    {1, 0}, {2, 0}, {3, 0},
    {NULL_VAL, 1}, {NULL_VAL, 1}, {NULL_VAL, 1}
}));
```

### Merge Join 算子

左右输入需按 **Join 等值键** 有序；使用 `with_left_sorted_data` / `with_right_sorted_data`。工厂：`merge_join_test()`。

```cpp
auto result = merge_join_test()
    .left_table("t1", "a int, b int")
    .right_table("t2", "c int, d int")
    .select("t1.a, t1.b, t2.c, t2.d")
    .with_left_sorted_data({{1, 10}, {2, 20}}, "a ASC")
    .with_right_sorted_data({{1, 100}, {2, 200}}, "c ASC")
    .join_type(INNER_JOIN)
    .on("t1.a = t2.c")
    .run(engine_);
```

### Hash Join 算子

不要求预排序；`with_left_data` / `with_right_data` 注入无序行集。Hash Join 支持 **LEFT/RIGHT/FULL OUTER**、**SEMI/ANTI** 等（见 `test_hash_join_op.cpp`）。工厂：`hash_join_test()`。可选：`enable_naaj()`、`enable_sna()`（见 `ob_op_test_hash_join.h`）。

```cpp
auto result = hash_join_test()
    .left_table("t1", "a int, b int")
    .right_table("t2", "c int, d int")
    .select("t1.a, t1.b, t2.c, t2.d")
    .with_left_data({{1, 10}, {2, 20}, {3, 30}})
    .with_right_data({{1, 100}, {3, 300}, {5, 500}})
    .join_type(INNER_JOIN)
    .on("t1.a = t2.c")
    .run(engine_);
EXPECT_TRUE(result.verify_unordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
```

### Nested Loop Join 算子

左表驱动、右表可 **rescan**；支持 **Cross Join**（不写 `.on()`）、**非等值** `.on("t1.a > t2.c")` 等。**不支持 RIGHT_OUTER_JOIN / FULL_OUTER_JOIN**（由优化器保证不落到 NLJ）。工厂：`nested_loop_join_test()`。

```cpp
auto result = nested_loop_join_test()
    .left_table("t1", "a int, b int")
    .right_table("t2", "c int, d int")
    .select("t1.a, t1.b, t2.c, t2.d")
    .with_left_data({{1, 10}, {2, 20}, {3, 30}})
    .with_right_data({{1, 100}, {3, 300}, {5, 500}})
    .join_type(INNER_JOIN)
    .on("t1.a = t2.c")
    .run(engine_);
EXPECT_TRUE(result.verify_ordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
```

双表 Join 的公共 fluent API 定义在 **`JoinSpecBuilder<Derived>`**（`ob_op_test_join_base.h`）：`.left_table` / `.right_table`、`.select`、`.join_type`、`.on`、以及各侧 `with_*_data` / `with_*_sorted_data` / Generator / 文件数据等（与单表 `OpSpecBuilder` 的数据注入能力对齐）。

---

## 4. 数据注入方式

框架支持三种数据注入模式，优先级：**Generator > File > Array**

### 4.1 Array 模式（默认）

```cpp
// 直接传入 TestRow 数据
.with_data({{1, "hello"}, {2, "world"}})

// TestValue 支持的类型：int64_t, double, string, null
.with_data({
    {42, 3.14, "text", TestValue::null()},
    {-1, 0.0, "", TestValue(100)}
})
```

### 4.2 Sorted Data（预排序数据）

```cpp
// 数据会在注入前按 order_desc 排序
.with_sorted_data({{3, 30}, {1, 10}, {2, 20}}, "a ASC")
// 等价于注入 {{1, 10}, {2, 20}, {3, 30}}
```

### 4.3 File Data（CSV 文件）

```cpp
.with_file_data("t", "/path/to/data.csv")
```

### 4.4 Generator 模式

```cpp
// 自定义 generator
.with_data_generator(10000, gen::sequential(1), gen::random_int(1, 100))

// 自动 generator（根据 schema 随机生成）
.with_data_generator(10000)
```

---

## 5. Generator 与大数据测试

### 内置 Generator

所有内置 generator 在 `gen::` 命名空间下：

```cpp
// 顺序整数：start, start+step, start+2*step, ...
gen::sequential(start, step)          // 默认 start=0, step=1

// 常量值
gen::constant(TestValue(42))

// 随机整数 [min, max]，固定 seed 保证 rescan 一致
gen::random_int(min_val, max_val, seed)    // 默认 seed=42

// 随机浮点数 [min, max)
gen::random_double(min_val, max_val, seed)

// 随机字符串（小写字母+数字）
gen::random_string(length, seed)           // 默认 length=10

// 随机 decimal（以字符串表示）
gen::random_decimal(precision, scale, seed)  // 默认 precision=10, scale=2

// 循环取值
gen::cycle({TestValue(1), TestValue(2), TestValue(3)})

// NULL 修饰器：每 n 行插入一个 NULL
gen::nullable(inner_gen, null_every_n)     // 默认 null_every_n=10
```

### 自定义 Lambda Generator

任何 `std::function<TestValue(int64_t row_idx)>` 都可以作为 generator：

```cpp
.with_data_generator(100,
    [](int64_t i) -> TestValue { return i * 100; },
    [](int64_t i) -> TestValue { return i % 2 == 0 ? "even" : "odd"; })
```

### 组合使用

```cpp
// nullable 包装：每 3 行一个 NULL
.with_data_generator(6,
    gen::cycle({TestValue(1), TestValue(2), TestValue(3)}),
    gen::nullable(gen::sequential(10, 10), 3))
// 输出: {1, NULL}, {2, 20}, {3, 30}, {1, NULL}, {2, 50}, {3, 60}
```

### 自动 Generator（基于 Schema）

不传 generator 参数时，框架根据列类型自动选择：

| 列类型 | 自动 Generator |
|--------|---------------|
| `int`, `bigint` 等整数 | `gen::sequential(0)` |
| `double`, `float` | `gen::random_double(0.0, 1000.0)` |
| `varchar(N)`, `char(N)` | `gen::random_string(min(N, 16))` |
| `number`, `decimal` | `gen::random_decimal(10, 2)` |

```cpp
// 自动生成 10000 行随机数据
auto result = material_test()
    .table("t", "a int, b double, c varchar(8)")
    .select("a, b, c")
    .with_data_generator(10000)
    .run(engine_);
EXPECT_EQ(10000, result.row_count());
```

### Rescan 安全机制

所有随机 generator 使用 `std::mt19937_64` + 固定 seed。当 `row_idx == 0` 时自动 reseed，保证 rescan 产生相同数据序列：

```cpp
auto result = material_test()
    .table("t", "a int")
    .select("a")
    .with_data_generator(100, gen::random_int(1, 1000))
    .with_rescan_times(3)  // rescan 3 次，每次数据一致
    .run(engine_);
```

---

## 6. 结果验证

### verify_ordered — 有序验证

```cpp
// 用 TestRow 列表验证
EXPECT_TRUE(result.verify_ordered({{1, 2}, {3, 4}}));

// 字符串格式
EXPECT_TRUE(result.verify_ordered({{"1", "hello"}, {"2", "world"}}));
```

**Generator 重载**（适用于大数据）：

```cpp
// 用 generator 生成期望值
EXPECT_TRUE(result.verify_ordered(10000,
    gen::sequential(1),
    gen::sequential(10, 10)));

// Lambda generator 验证计算表达式结果
EXPECT_TRUE(result.verify_ordered(100,
    [](int64_t i) -> TestValue { return std::to_string(1 + i + 10 + i * 10); }));
```

### verify_unordered — 无序集合相等

适用于 **Hash Join**、**Hash Group By**、**Expand** 等输出顺序不固定或只关心 multiset 相等的场景：

```cpp
EXPECT_TRUE(result.verify_unordered({{1, 10, 1, 100}, {3, 30, 3, 300}}));
```

### verify_column — 单列验证

```cpp
// 验证第 0 列
EXPECT_TRUE(result.verify_column(0, {TestValue(1), TestValue(2), TestValue(3)}));

// Generator 重载
EXPECT_TRUE(result.verify_column(0, 100, gen::sequential(1)));
EXPECT_TRUE(result.verify_column(1, 100, gen::sequential(100, 5)));
```

### 其他验证方法

```cpp
// 行数检查
EXPECT_EQ(100, result.row_count());

// 结果相等
EXPECT_TRUE(result1.equals(result2));

// Checksum 对比
EXPECT_EQ(result1.get_checksum(), result2.get_checksum());

// 打印结果（调试用）
result.print();
```

### Decimal 比较容差

`verify_ordered` 内部使用 `normalize_decimal_str` 做逻辑比较：`"4.000"` 与 `"4"` 视为相等。

---

## 7. Rescan 测试

Rescan 模拟 NLJ 等场景下算子的重复扫描行为。

### 基本 Rescan

```cpp
auto result = material_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_data({{1, 10}, {2, 20}})
    .with_rescan_times(3)
    .run(engine_);

EXPECT_EQ(2, result.row_count());  // 最后一次 scan 的结果
```

### Rescan 内存泄漏检测

```cpp
auto result = material_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_data({{1, 10}, {2, 20}, {3, 30}})
    .with_rescan_times(3)
    .with_rescan_memory_check(true)            // 启用内存检测
    .with_rescan_memory_tolerance(4096)         // 容差 4KB（可选）
    .run(engine_);

EXPECT_TRUE(result.is_rescan_memory_consistent());  // 无内存泄漏
EXPECT_EQ(3, result.get_rescan_count());
EXPECT_GT(result.get_memory_after_first_scan(), 0);
```

### Generator + Rescan

Generator 在 rescan 时自动 reseed，产生相同数据：

```cpp
auto result = material_test()
    .table("t", "a int")
    .select("a")
    .with_data_generator(1000, gen::random_int(1, 100))
    .with_rescan_times(5)
    .with_rescan_memory_check(true)
    .run(engine_);

EXPECT_TRUE(result.is_rescan_memory_consistent());
```

---

## 8. CSV 文件数据源

### with_file_data（流式读取）

```cpp
auto result = material_test()
    .table("t", "a int, b double, c varchar(32)")
    .select("a, b, c")
    .with_file_data("t", "/path/to/data.csv")
    .run(engine_);
```

CSV 文件格式：

```csv
# type: int, double, varchar(32)
1,3.14,hello
2,2.71,world
,1.0,null_value
```

- `# type:` 行（可选）声明各列类型
- 空字段表示 NULL
- 支持流式读取，不需要全部加载到内存

### CsvDataReader（独立使用）

```cpp
CsvDataReader reader;
reader.open("/path/to/data.csv");
TestRow row;
while (reader.read_row(row) == OB_SUCCESS) {
    // process row
}
reader.close();
```

---

## 9. Dump 与大数据场景

### 启用 Dump（溢出到磁盘）

```cpp
auto result = material_test()
    .table("t", "a int, b varchar(32)")
    .select("a, b")
    .with_data_generator(100000, gen::sequential(0), gen::random_string(16))
    .with_sql_operator_dump(true)       // 启用 dump
    .with_hash_area_size(64 * 1024)     // 64KB 内存限制（强制触发 dump）
    .run(engine_);
```

### Checksum 验证模式

大数据场景下，全量存储结果行会占用过多内存。使用 CHECKSUM 模式：

```cpp
auto result = material_test()
    .table("t", "a int")
    .select("a")
    .with_data_generator(1000000)
    .with_dump_verify_mode(OpTestEngine::DumpVerifyMode::CHECKSUM)
    .run(engine_);

// 用 checksum 验证
EXPECT_EQ(expected_checksum, result.get_checksum());
EXPECT_EQ(1000000, result.get_checksum_row_count());
```

### 常用配置

| 方法 | 说明 |
|------|------|
| `.with_batch_size(N)` | 设置向量化 batch 大小（默认 256） |
| `.with_vector_format(VEC_CONTINUOUS)` | 变长列使用 continuous format |
| `.with_sql_operator_dump(true)` | 启用磁盘溢出 |
| `.with_hash_area_size(N)` | 设置内存阈值（字节） |
| `.with_dump_verify_mode(mode)` | 结果验证模式：FULL_ROWS / CHECKSUM / NONE |

---

## 10. 如何新增算子 SpecBuilder

以 Limit 算子为例，说明扩展步骤。

### Step 1: 创建头文件

创建 `ob_op_test_limit.h`：

```cpp
#ifndef OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_
#define OCEANBASE_UNITTEST_SQL_ENGINE_OP_TEST_OB_OP_TEST_LIMIT_H_

#include "unittest/sql/engine/op_tests/ob_op_test_base.h"
#include "sql/engine/basic/ob_limit_vec_op.h"    // 引入算子头文件

namespace oceanbase {
namespace sql {

class LimitTestSpec : public OpSpecBuilder<LimitTestSpec>    // CRTP 继承
{
public:
  LimitTestSpec() : is_top_limit_(true) {}
  ~LimitTestSpec() = default;

  // ===== 算子特有配置 =====
  LimitTestSpec& top_limit(bool is_top = true)
  {
    is_top_limit_ = is_top;
    return *this;
  }

  // ===== 必须实现的两个方法 =====

  /**
   * 创建算子 Spec（对应优化器生成的 ObOpSpec 子类）
   * @param alloc         全局 allocator
   * @param child_spec    子算子（MockDataSourceSpec）的 Spec
   * @param output_exprs  SELECT 输出表达式
   * @param limit_expr    LIMIT 表达式（如果有）
   * @param offset_expr   OFFSET 表达式（如果有）
   * @param use_rich_format 是否使用 rich format
   */
  ObOpSpec *create_spec(common::ObIAllocator &alloc,
                        MockDataSourceSpec *child_spec,
                        const ExprFixedArray &output_exprs,
                        ObExpr *limit_expr,
                        ObExpr *offset_expr,
                        bool use_rich_format)
  {
    int ret = OB_SUCCESS;
    void *mem = alloc.alloc(sizeof(ObLimitVecSpec));
    if (OB_ISNULL(mem)) { return nullptr; }

    ObLimitVecSpec *spec = new (mem) ObLimitVecSpec(alloc, PHY_VEC_LIMIT);

    // 设置算子特有字段
    spec->limit_expr_ = limit_expr;
    spec->offset_expr_ = offset_expr;
    spec->is_top_limit_ = is_top_limit_;

    // 继承通用字段
    spec->plan_ = child_spec->plan_;
    spec->max_batch_size_ = child_spec->max_batch_size_;
    spec->use_rich_format_ = use_rich_format;
    spec->output_ = output_exprs;

    // 设置子算子
    void *child_mem = alloc.alloc(sizeof(ObOpSpec *));
    ObOpSpec **children = reinterpret_cast<ObOpSpec **>(child_mem);
    children[0] = child_spec;
    spec->set_children_pointer(children, 1);

    return spec;
  }

  /**
   * 创建算子 Op（运行时对象）
   * @param ctx        执行上下文
   * @param spec       上面创建的 Spec
   * @param child_op   子算子（MockDataSourceOp）
   */
  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op)
  {
    void *mem = ctx.get_allocator().alloc(sizeof(ObLimitVecOp));
    if (OB_ISNULL(mem)) { return nullptr; }

    ObLimitVecOp *op = new (mem) ObLimitVecOp(ctx, spec, nullptr);

    void *children_mem = ctx.get_allocator().alloc(sizeof(ObOperator *));
    ObOperator **children = reinterpret_cast<ObOperator **>(children_mem);
    children[0] = child_op;
    op->set_children_pointer(children, 1);

    return op;
  }

private:
  bool is_top_limit_;
};

}  // namespace sql
}  // namespace oceanbase
#endif
```

### Step 2: 注册工厂方法

在 `ob_op_test_kit.h` 中添加：

```cpp
#include "unittest/sql/engine/op_tests/ob_op_test_limit.h"

class OpTestKit : public ::testing::Test {
protected:
  // 已有工厂方法
  MaterialTestSpec material_test() { return MaterialTestSpec(); }
  ExprTestSpec expr_unit_test() { return ExprTestSpec(); }

  // 新增
  LimitTestSpec limit_test() { return LimitTestSpec(); }

protected:
  OpTestEngine engine_;
};
```

### Step 3: 编写测试

创建 `test_limit_op.cpp`：

```cpp
#include "unittest/sql/engine/op_tests/ob_op_test_kit.h"

namespace oceanbase {
namespace sql {

class LimitOpTest : public OpTestKit {};

TEST_F(LimitOpTest, BasicLimit)
{
  auto result = limit_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .limit(3)
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{1}, {2}, {3}}));
}

TEST_F(LimitOpTest, LimitWithOffset)
{
  auto result = limit_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .limit(2)
      .offset(2)
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{3}, {4}}));
}

}  // namespace sql
}  // namespace oceanbase

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

### Step 4: 添加 CMake Target

在 `unittest/sql/engine/op_tests/CMakeLists.txt` 中增加 `sql_unittest(目标名 源文件 ... ob_op_test_engine.cpp ob_op_test_data_source.cpp)`，与现有目标保持一致。

### 双表 Join 类算子

Merge Join / Hash Join / Nested Loop Join 等继承 **`JoinSpecBuilder<Derived>`**（见 `ob_op_test_join_base.h`），实现 `create_spec` / `create_op` 时接收 **左右两个** `MockDataSourceSpec*` / `ObOperator*`，并由基类负责 `resolve_sql`、列拆分、`run()` 流水线。新增 Join 时：新建 `ob_op_test_<join>.h` → 在 `ob_op_test_kit.h` 中 `#include` 并添加工厂方法 → CMake 注册测试二进制。

### 关键注意事项

1. **CRTP 模板**: 单表算子 `class MySpec : public OpSpecBuilder<MySpec>`；双表 Join `class MySpec : public JoinSpecBuilder<MySpec>`
2. **create_spec 参数**: `output_exprs` 对于 Material 等缓存算子应使用列引用（column_exprs），对于 Limit 等传递算子应使用 SELECT 表达式
3. **子算子设置**: 使用 `set_children_pointer` 而非 `set_child`
4. **内存分配**: 使用 placement new + `alloc.alloc()`，不使用 STL new

---

## 11. 格式模式（1.0/2.0）

框架支持 OceanBase 1.0（datum-based）和 2.0（vec-based）两种执行模式。

### with_rich_format — 单次运行模式

```cpp
// 使用 2.0 vec 模式（默认）
auto result = material_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_data({{1, 2}, {3, 4}})
    .with_rich_format(true)   // 使用 2.0 vec（默认）
    .run(engine_);

// 使用 1.0 datum 模式
auto result = material_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_data({{1, 2}, {3, 4}})
    .with_rich_format(false)  // 使用 1.0 datum
    .run(engine_);
```

### enable_dual_format_check — 双格式对比模式

自动运行 2.0 和 1.0 两种模式，并对比结果一致性：

```cpp
auto result = material_test()
    .table("t", "a int, b int")
    .select("a, b")
    .with_data({{1, 2}, {3, 4}})
    .enable_dual_format_check()  // 启用双格式对比
    .run(engine_);

// 如果 2.0 和 1.0 结果不一致，测试会失败
// 返回的是 2.0 模式的结果
EXPECT_EQ(2, result.row_count());
```

**适用场景：**
- 新算子开发：确保 1.0 和 2.0 实现一致
- 回归测试：同时覆盖两种执行路径
- Window Function 等复杂算子的正确性验证

**工作原理：**
1. 第一次执行：`engine.enable_rich_format(true)` → 运行 2.0 vec 模式
2. 第二次执行：`engine.enable_rich_format(false)` → 运行 1.0 datum 模式
3. 对比两次结果的行数和每行数据
4. 不一致时抛出 `EXPECT_EQ` 失败
5. 返回 2.0 模式的结果

---

## 12. 租户配置与测试宏

涵盖 **`WITH_TENANT_CONFS`**（租户配置覆盖）与 **`DISABLE_OPTESTS_RET_CHECK`**（关闭返回码检查），定义见 `ob_op_test_base.h`。

### WITH_TENANT_CONFS

单测里有时需要临时改写租户配置项（与 `ObTenantConfig` 中已有项对应），框架通过 `TestDefaultParameterConf` 保存覆盖，在 `OpTestEngine::init()` 等路径里应用。`ob_op_test_base.h` 提供了 **`WITH_TENANT_CONFS`** 宏：在紧随其后的 `{ ... }` 作用域内生效，退出作用域时由 **`TestParameterGuard`** 自动恢复进入前的覆盖状态（等价于手写 `TestParameterGuard guard; guard.set(...);` 链式调用）。

### 头文件

- 继承 **`OpTestKit`** 的用例：已通过 `ob_op_test_kit.h` 间接包含 `ob_op_test_base.h`，一般无需再写 include。
- 仅使用 **`OpTestEngine`** 等自建 fixture 的用例：请 `#include "unittest/sql/engine/op_tests/ob_op_test_base.h"`。

### 写法说明

宏基于 **C++17 `if (init; cond)`** 实现（本仓库默认 `-std=gnu++17`）。参数为 **逗号分隔** 的若干项，每一项是 `std::pair<const char *, const char *>` 的字面量：

- 推荐：`{"_配置项名", "字符串值"}`（与 `TestParameterGuard::set` 的字符串入参一致）。
- 或使用辅助宏：**`OB_TENANT_CONF(K, V)`**，展开为 `::std::pair<const char *, const char *>((K), (V))`，便于对齐多行。

**注意：** C++ 源码里不能写成 JSON 风格的 `"key": "value"`（冒号夹在字符串之间不是合法表达式），请使用上表两种形式之一。

### 示例

```cpp
// 在一段作用域内临时覆盖多个租户配置
WITH_TENANT_CONFS(
    {"_hash_area_size", "2M"},
    {"_enable_hash_join_processor", "7"}
) {
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}})
      .run(engine_);
  EXPECT_EQ(2, result.row_count());
}

// 等价于（仅说明语义，实际优先用宏）
// TestParameterGuard guard;
// guard.set("_hash_area_size", "2M");
// guard.set("_enable_hash_join_processor", "7");
// ... 同上 ...
```

使用 **`OB_TENANT_CONF`** 的等价写法：

```cpp
WITH_TENANT_CONFS(
    OB_TENANT_CONF("_hash_area_size", "2M"),
    OB_TENANT_CONF("_enable_hash_join_processor", "7")
) {
  // ...
}
```

### DISABLE_OPTESTS_RET_CHECK

部分路径在失败返回码时会触发框架内的 `EXPECT`；若你有意校验 `OB_INVALID_ARGUMENT` 等错误码，可使用 **`DISABLE_OPTESTS_RET_CHECK { ... }`**（`ob_op_test_base.h`，基于 C++17 `if (DisableRetCheckGuard g; true)`）在该复合语句内关闭返回码检查。可与 **`WITH_TENANT_CONFS`** 嵌套；**建议宏后始终写 `{ ... }`**，避免与后续裸 `if/else` 产生悬挂 `else` 歧义。

### 行为提示

- 配置名、取值须合法，否则 `TestDefaultParameterConf::set` 可能失败（与直接调用 `guard.set` 相同）；**非法名/非法值不会写入 overrides**。
- 若要让 **`engine_.init()`** 构造租户配置时即带上覆盖，应保证 **`init()` 执行时覆盖已存在于 `TestDefaultParameterConf`**（例如在 `WITH_TENANT_CONFS` 块内调用 `init()`，或先设覆盖再 `destroy()`/`init()` 重拉配置，视用例而定）。

---

## 13. 附录：支持的数据类型

| 类型 | TestValue 构造 | VectorFormat |
|------|---------------|-------------|
| `int`, `bigint`, `int32` | `TestValue(42)` | VEC_FIXED |
| `double`, `float` | `TestValue(3.14)` | VEC_FIXED |
| `varchar(N)`, `char(N)`, `text` | `TestValue("hello")` | VEC_DISCRETE |
| `number`, `decimal` | `TestValue("123.45")` | VEC_DISCRETE |
| `NULL` | `TestValue::null()` | — |

---

## 14. 附录：Fluent API 完整列表

| 方法 | 说明 |
|------|------|
| `.table(name, col_defs)` | 注册表和列定义 |
| `.select(expr)` | 设置 SELECT 表达式 |
| `.where(condition)` | 设置 WHERE 条件 |
| `.order_by(exprs)` | 设置 ORDER BY |
| `.group_by(exprs)` | 设置 GROUP BY |
| `.group_by_rollup(cols)` | Expand：`WITH ROLLUP`（仅 Expand Spec） |
| `.group_by_grouping_sets(desc)` | Expand：`GROUPING SETS(...)`（仅 Expand Spec） |
| `.enable_hash_base_distinct(bool)` | Scalar Aggregate：COUNT DISTINCT 走 hash 路径 |
| `.limit(n)` | 设置 LIMIT |
| `.offset(n)` | 设置 OFFSET |
| `.with_data(rows)` | 注入测试数据（Array 模式） |
| `.with_sorted_data(rows, order)` | 注入预排序数据 |
| `.with_file_data(table, path)` | 从 CSV 文件读取数据 |
| `.with_data_generator(n, gens...)` | Generator 模式（自定义 generator） |
| `.with_data_generator(n)` | Generator 模式（自动随机） |
| `.with_batch_size(n)` | 设置 batch 大小 |
| `.with_vector_format(fmt)` | 设置变长列 VectorFormat |
| `.with_rescan_times(n)` | 设置 rescan 次数 |
| `.with_rescan_memory_check(enable)` | 启用 rescan 内存检测 |
| `.with_rescan_memory_tolerance(n)` | rescan 内存容差（字节） |
| `.with_sql_operator_dump(enable)` | 启用磁盘溢出 |
| `.with_hash_area_size(n)` | 设置内存阈值 |
| `.with_dump_verify_mode(mode)` | 结果验证模式 |
| `.with_rich_format(enable)` | 设置格式模式（true=2.0 vec, false=1.0 datum） |
| `.enable_dual_format_check(enable)` | 启用双格式对比模式（同时测试 2.0 和 1.0） |
| `.run(engine)` | 执行并返回 `OpTestResult` |

**Join 双表 Spec**（`merge_join_test` / `hash_join_test` / `nested_loop_join_test`）在 `JoinSpecBuilder` 上另有 `.left_table` / `.right_table`、`.with_left_data` / `.with_right_data`、`.with_left_sorted_data` / `.with_right_sorted_data`、`.join_type`、`.on` 等，完整列表以 `ob_op_test_join_base.h` 为准。**Hash Join** 另有 `.enable_naaj` / `.enable_sna`（`ob_op_test_hash_join.h`）。

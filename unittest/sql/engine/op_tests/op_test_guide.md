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

---

## 1. 快速上手

### 编译与运行

```bash
cd build_release
ob-make test_material_op           # 编译
./unittest/sql/engine/op_tests/test_material_op   # 运行
```

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

在 `CMakeLists.txt` 中添加编译目标即可。

### 关键注意事项

1. **CRTP 模板**: 必须 `class MySpec : public OpSpecBuilder<MySpec>` 才能链式调用返回正确类型
2. **create_spec 参数**: `output_exprs` 对于 Material 等缓存算子应使用列引用（column_exprs），对于 Limit 等传递算子应使用 SELECT 表达式
3. **子算子设置**: 使用 `set_children_pointer` 而非 `set_child`
4. **内存分配**: 使用 placement new + `alloc.alloc()`，不使用 STL new

---

## 附录：支持的数据类型

| 类型 | TestValue 构造 | VectorFormat |
|------|---------------|-------------|
| `int`, `bigint`, `int32` | `TestValue(42)` | VEC_FIXED |
| `double`, `float` | `TestValue(3.14)` | VEC_FIXED |
| `varchar(N)`, `char(N)`, `text` | `TestValue("hello")` | VEC_DISCRETE |
| `number`, `decimal` | `TestValue("123.45")` | VEC_DISCRETE |
| `NULL` | `TestValue::null()` | — |

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

## 附录：Fluent API 完整列表

| 方法 | 说明 |
|------|------|
| `.table(name, col_defs)` | 注册表和列定义 |
| `.select(expr)` | 设置 SELECT 表达式 |
| `.where(condition)` | 设置 WHERE 条件 |
| `.order_by(exprs)` | 设置 ORDER BY |
| `.group_by(exprs)` | 设置 GROUP BY |
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

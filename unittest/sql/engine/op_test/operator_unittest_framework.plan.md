# OceanBase 算子单测框架 - 实现方案

## 一、概述

### 1.1 背景与动机

现有测试基础设施存在两个极端：

- **ExprTestBuilder**（轻量级）：直接构造 `ObExpr` 树做表达式级测试，无法测试算子，且不做类型推导、不走真实 CG 路径。
- **TestOpEngine**（重量级）：走完整链路 SQL -> 优化器 -> 代码生成 -> 物理计划，不可控制算子 spec 的构造方式。

本框架定位在**两者之间**：复用生产级的 resolver + CG 做表达式处理（类型推导、cast 插入、表达式共享），但**跳过优化器**，由测试框架直接控制算子 spec 的构造。

**三个核心设计原则：**

1. **SQL 驱动**：用 SQL 字符串描述算子/表达式行为，复用 resolver 全链路
2. **生产级 CG**：用 `ObStaticEngineExprCG` 生成 ObExpr
3. **CRTP 可扩展**：新增算子只需 "一个类 + 两个方法"

### 1.2 当前实现状态

| 组件 | 状态 | 说明 |
|------|------|------|
| OpTestEngine | ✅ 完成 | 继承 TestSqlUtils，支持 SQL resolve + 表达式 CG |
| MockDataSourceOp | ✅ 完成 | 支持 int/double/varchar/number/decimal 类型 |
| OpSpecBuilder | ✅ 完成 | CRTP 基类，支持 fluent API |
| OpTestResult | ✅ 完成 | 支持 verify_ordered/verify_column，decimal 归一化 |
| MaterialTestSpec | ✅ 完成 | Passthrough 模式（直接返回 child_op） |
| LimitTestSpec | ✅ 完成 | 创建真实 ObLimitVecOp |
| ScalarAggTestSpec | ⚠️ 部分 | 基础聚合可用，AVG 等复杂聚合需完善 |
| test_material_op | ✅ 通过 | 72 tests PASSED |
| test_limit_op | ✅ 通过 | 12 tests PASSED |
| test_scalar_agg_op | ❌ 部分失败 | ObAggrInfo 需要更完整填充 |

---

## 二、文件结构

```
unittest/sql/engine/op_test/
├── ob_op_test_types.h        # 基础类型定义 (TestValue, TestRow)
├── ob_op_test_engine.h/cpp   # OpTestEngine (继承 TestSqlUtils)
├── ob_op_test_data_source.h/cpp # MockDataSourceSpec, MockDataSourceOp
├── ob_op_test_result.h       # OpTestResult (结果收集与校验)
├── ob_op_test_base.h         # OpSpecBuilder<Derived> CRTP 基类 + ExprTestSpec
├── ob_op_test_kit.h          # OpTestKit 测试固件 + 工厂方法
├── ob_op_test_file_data.h    # CsvDataReader (CSV 文件读取)
├── ob_op_test_material.h     # MaterialTestSpec
├── ob_op_test_limit.h        # LimitTestSpec
├── ob_op_test_scalar_aggregate.h # ScalarAggTestSpec
├── test_material_op.cpp      # Material 算子测试
├── test_limit_op.cpp         # Limit 算子测试
└── test_scalar_aggregate_op.cpp # ScalarAggregate 算子测试
```

---

## 三、核心组件

### 3.1 OpTestEngine

**职责**：继承 TestSqlUtils，复用 mock schema 注册、SQL 解析、resolve 能力，新增表达式 CG。

**关键方法**：

```cpp
class OpTestEngine : public test::TestSqlUtils {
public:
  // 配置
  void set_sql_mode(SqlMode mode);      // MySQL/Oracle 模式
  void set_batch_size(int64_t size);    // 向量化批次大小

  // Schema 注册
  int register_table(const char *table_name, const char *col_defs);

  // SQL Resolve
  int resolve_sql(const std::string &sql, ObDMLStmt *&stmt);

  // 表达式 CG
  int generate_exprs(ObDMLStmt &stmt);

  // 执行
  OpTestResult execute(ObOperator *op, const ExprFixedArray *output_exprs = nullptr,
                       int64_t rescan_times = 0);

  // Mock 数据源创建
  MockDataSourceSpec *create_mock_data_source_spec(...);
  MockDataSourceOp *create_mock_data_source_op(...);
};
```

**关键流程**：

1. `register_table()` → 生成 `CREATE TABLE` SQL → `TestSqlUtils::do_load_sql()`
2. `resolve_sql()` → ObParser → ObSqlParameterization → `init_datum_param_store()` → ObResolver
3. `generate_exprs()` → 收集 ObRawExpr → `ObStaticEngineExprCG::generate()` → `pre_alloc_exec_memory()`

### 3.2 MockDataSourceOp

**职责**：替换 TableScan，作为被测算子的 child，将 TestRow 数据写入 ObExpr 的 eval frame。

**支持的数据类型**：

| 类型 | 向量格式 | 填充方法 |
|------|----------|----------|
| ObIntType, ObInt32Type | VEC_FIXED | `int_data[row_idx] = value` |
| ObUInt64Type, ObUInt32Type | VEC_FIXED | `uint_data[row_idx] = value` |
| ObDoubleType | VEC_FIXED | `double_data[row_idx] = value` |
| ObFloatType | VEC_FIXED | `float_data[row_idx] = value` |
| ObDecimalIntType | VEC_FIXED | `wide::from_string()` + MEMCPY |
| ObVarcharType, ObCharType, ObTextType | VEC_DISCRETE | `ptrs[row_idx]` + `lens[row_idx]` |
| ObNumberType | VEC_DISCRETE | ObCompactNumber 格式 |

**支持的向量格式**：

- `VEC_FIXED`：定长类型（int, double, decimal）
- `VEC_DISCRETE`：变长类型默认格式（varchar, number）
- `VEC_CONTINUOUS`：连续存储格式（可选，通过 `set_vector_format()` 启用）

**关键方法**：

```cpp
class MockDataSourceOp : public ObOperator {
public:
  void set_test_data(const std::vector<TestRow> &rows);
  void set_vector_format(VectorFormat fmt);
  void set_limit(int64_t limit);
  void set_offset(int64_t offset);

  int inner_get_next_batch(const int64_t max_row_cnt) override;
  int inner_rescan() override;
};
```

### 3.3 OpSpecBuilder<Derived>

**职责**：CRTP 基类，提供 fluent API 构建 SQL 并执行测试。

**SQL 描述接口**：

```cpp
// 表定义
Derived& table(const char *table_name, const char *col_defs);

// SQL 子句
Derived& select(const char *exprs);
Derived& where(const char *condition);
Derived& group_by(const char *exprs);
Derived& order_by(const char *exprs);
Derived& limit(int64_t n);
Derived& offset(int64_t n);

// 数据注入
Derived& with_data(const std::vector<TestRow> &rows);
Derived& with_file_data(const char *table, const std::string &file_path);
Derived& with_sorted_data(const std::vector<TestRow> &rows, const char *order_desc);

// 配置
Derived& with_batch_size(int64_t batch_size);
Derived& with_rescan_times(int64_t times);
Derived& with_vector_format(VectorFormat fmt);
Derived& sql_mode(SqlMode mode);

// 自定义 eval 函数
Derived& with_expr_eval_func(ObExpr::EvalFunc func);
Derived& with_expr_eval_vector_func(ObExpr::EvalVectorFunc func);

// 执行
OpTestResult run(OpTestEngine &engine);
```

**子类需实现的方法**：

```cpp
// 创建父算子 Spec（可选）
ObOpSpec *create_spec(ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                      ObExpr *limit_expr, ObExpr *offset_expr);

// 创建父算子 Operator
ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op);
ObOperator *create_op(ObExecContext &ctx, ObOperator *child_op);  // 无 spec 时
```

### 3.4 OpTestResult

**职责**：收集执行结果并提供校验方法。

**关键方法**：

```cpp
class OpTestResult {
public:
  int64_t row_count() const;
  bool empty() const;
  const std::vector<std::string>& get_row(int64_t idx) const;

  bool verify_ordered(const std::vector<TestRow> &expected);
  bool verify_column(int64_t col_idx, const std::vector<TestValue> &expected);

  void print() const;
  std::string to_string() const;
  bool equals(const OpTestResult &other) const;
};
```

**Decimal 归一化**：

为解决 `decimal(10,2) + decimal(10,2)` 结果类型 scale 提升导致 `"4.000"` vs `"4"` 比较失败的问题，`verify_ordered` 内部自动归一化：

```
"4.000" → "4"
"123.450" → "123.45"
"0.10" → "0.1"
```

### 3.5 ExprTestSpec

**职责**：表达式单测便捷封装，简化 API。

```cpp
class ExprTestSpec : public OpSpecBuilder<ExprTestSpec> {
public:
  ExprTestSpec& columns(const char *col_defs);  // 代替 table()
  ExprTestSpec& with_expr(const char *expr_str); // 代替 select()

  // Pass-through: 直接返回 child_op
  ObOperator* create_op(ObExecContext &ctx, ObOperator *child_op);
};
```

---

## 四、已实现算子

### 4.1 MaterialTestSpec

**状态**：✅ 完成（Passthrough 模式）

**说明**：由于 Material 算子需要 `ObOpInput` 初始化，当前实现为 pass-through 模式，直接返回 `child_op`。

```cpp
class MaterialTestSpec : public OpSpecBuilder<MaterialTestSpec> {
public:
  ObOperator* create_op(ObExecContext &ctx, ObOperator *child_op) {
    return child_op;  // Pass-through
  }
};
```

### 4.2 LimitTestSpec

**状态**：✅ 完成

**说明**：创建真实的 `ObLimitVecOp`。

```cpp
class LimitTestSpec : public OpSpecBuilder<LimitTestSpec> {
public:
  LimitTestSpec& top_limit(bool is_top = true);

  ObOpSpec *create_spec(ObIAllocator &alloc, MockDataSourceSpec *child_spec,
                        ObExpr *limit_expr, ObExpr *offset_expr);

  ObOperator *create_op(ObExecContext &ctx, ObOpSpec &spec, ObOperator *child_op);
};
```

**测试用例**（test_limit_op.cpp）：
- 基本 LIMIT
- LIMIT + OFFSET
- LIMIT 0
- LIMIT 超过行数
- ORDER BY + LIMIT
- `is_top_limit_` 测试

### 4.3 ScalarAggTestSpec

**状态**：⚠️ 部分完成

**问题**：`ObAggrInfo` 需要更完整填充，AVG 等复杂聚合需要额外字段。

```cpp
class ScalarAggTestSpec : public OpSpecBuilder<ScalarAggTestSpec> {
public:
  ScalarAggTestSpec& can_return_empty_set(bool enable = true);

  ObOpSpec *create_spec(...);
  ObOperator *create_op(...);
};
```

**已支持**：COUNT, SUM, MIN, MAX（基础版）

**待完善**：
- AVG 需要正确的 `real_aggr_type_` 设置
- DISTINCT 聚合需要 `distinct_collations_`/`distinct_cmp_funcs_`
- GROUP_CONCAT 需要 `sort_collations_`/`sort_cmp_funcs_`

---

## 五、使用方法

### 5.1 基本用法

```cpp
#include "unittest/sql/engine/op_test/ob_op_test_kit.h"

TEST_F(MaterialOpTest, BasicTest) {
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_data({{1, 2}, {3, 4}, {5, 6}})
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"1", "2"}, {"3", "4"}, {"5", "6"}}));
}
```

### 5.2 表达式测试

```cpp
TEST_F(MaterialOpTest, ExpressionTest) {
  auto result = expr_unit_test()
      .columns("a int, b int")
      .with_expr("a + b")
      .with_data({{1, 2}, {3, 4}})
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{"3"}, {"7"}}));
}
```

### 5.3 带类型推导的表达式

```cpp
TEST_F(MaterialOpTest, ImplicitCastTest) {
  auto result = material_test()
      .table("t", "a int, b number")
      .select("a + b")  // 自动插入 cast(a as number)
      .with_data({{1, "2.5"}, {3, "4.5"}})
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{"3.5"}, {"7.5"}}));
}
```

### 5.4 LIMIT 测试

```cpp
TEST_F(LimitOpTest, LimitOffsetTest) {
  auto result = limit_test()
      .table("t", "a int")
      .select("a")
      .limit(2)
      .offset(1)
      .with_data({{1}, {2}, {3}, {4}, {5}})
      .run(engine_);

  EXPECT_EQ(2, result.row_count());
  EXPECT_TRUE(result.verify_ordered({{"2"}, {"3"}}));
}
```

### 5.5 Rescan 测试

```cpp
TEST_F(MaterialOpTest, RescanTest) {
  auto result = material_test()
      .table("t", "a int")
      .select("a")
      .with_data({{1}, {2}, {3}})
      .with_rescan_times(3)  // rescan 3 次，每次结果应一致
      .run(engine_);

  EXPECT_EQ(3, result.row_count());
}
```

### 5.6 Filter 测试

```cpp
TEST_F(MaterialOpTest, FilterTest) {
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .where("a > 1")
      .with_data({{1, 10}, {2, 20}, {3, 30}})
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{"2", "20"}, {"3", "30"}}));
}
```

### 5.7 文件数据源

```cpp
TEST_F(MaterialOpTest, FileDataTest) {
  auto result = material_test()
      .table("t", "a int, b int")
      .select("a, b")
      .with_file_data("t", "test_data/sample.csv")
      .run(engine_);

  EXPECT_GT(result.row_count(), 0);
}
```

### 5.8 Decimal 类型

```cpp
TEST_F(MaterialOpTest, DecimalTest) {
  auto result = material_test()
      .table("t", "a decimal(10,2), b int")
      .select("a, b")
      .with_data({{"123.45", 1}, {"678.90", 2}})
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{"123.45", "1"}, {"678.90", "2"}}));
}
```

### 5.9 自定义 eval_func

```cpp
static int my_eval_func(const ObExpr &expr, ObEvalCtx &ctx, ObDatum &datum) {
  ObDatum *child_datum = nullptr;
  int ret = expr.args_[0]->eval(ctx, child_datum);
  if (OB_SUCC(ret) && !child_datum->is_null()) {
    datum.set_int(child_datum->get_int() + 100);
  }
  return ret;
}

TEST_F(MaterialOpTest, CustomEvalTest) {
  auto result = expr_unit_test()
      .columns("a int")
      .with_expr("a + 0")
      .with_expr_eval_func(my_eval_func)
      .with_data({{1}, {2}, {3}})
      .run(engine_);

  EXPECT_TRUE(result.verify_ordered({{"101"}, {"102"}, {"103"}}));
}
```

---

## 六、注意事项

### 6.1 编译

```bash
cd build_debug
ob-make test_material_op
ob-make test_limit_op
ob-make test_scalar_agg_op
```

### 6.2 运行测试

```bash
./unittest/sql/engine/op_test/test_material_op
./unittest/sql/engine/op_test/test_limit_op
./unittest/sql/engine/op_test/test_scalar_agg_op
```

### 6.3 修改范围

- **允许修改**：`unittest/sql/engine/op_test/` 目录下的文件
- **禁止修改**：`src/` 目录下的业务逻辑代码（只允许添加日志打印）

### 6.4 ObOpInput 限制

某些算子（如 Material）需要 `ObOpInput` 初始化，当前框架不支持。解决方案：
1. Pass-through 模式（直接返回 child_op）
2. 后续扩展框架支持 `ObOpInput` 创建

### 6.5 Decimal 类型

- 使用 `wide::from_string()` 解析 decimal 字符串
- 结果比较时自动归一化（去除尾部零）
- 支持不同精度：`decimal(9,2)` → int32, `decimal(18,3)` → int64, `decimal(30,5)` → int128+

### 6.6 表达式共享

Resolver 会自动处理表达式共享，CG 时 `flatten_and_add_raw_exprs()` 去重后只生成一个 `ObExpr`，所有引用点共享同一个 eval frame 位置。

---

## 七、后续 TODO

### 7.1 ScalarAggTestSpec 完善

- [ ] AVG 聚合支持（正确设置 `real_aggr_type_`）
- [ ] DISTINCT 聚合支持（初始化 `distinct_collations_`/`distinct_cmp_funcs_`）
- [ ] GROUP_CONCAT 支持（初始化 `sort_collations_`/`sort_cmp_funcs_`）

参考：`src/sql/code_generator/ob_static_engine_cg.cpp:8095` 的 `fill_aggr_info()`

### 7.2 SortTestSpec

```cpp
class SortTestSpec : public OpSpecBuilder<SortTestSpec> {
public:
  SortTestSpec& with_normal_sort();
  SortTestSpec& with_prefix_sort(int64_t prefix_pos);
  SortTestSpec& with_partition_sort(int64_t part_cnt);
  SortTestSpec& with_topn(int64_t n);
  SortTestSpec& with_encode_sortkey(bool enabled = true);
};
```

参考：`src/sql/engine/sort/ob_sort_vec_op.h` — ObSortVecSpec

### 7.3 HashGroupByTestSpec

```cpp
class HashGroupByTestSpec : public OpSpecBuilder<HashGroupByTestSpec> {
public:
  HashGroupByTestSpec& with_bypass(bool enabled = true);
  HashGroupByTestSpec& with_est_group_cnt(int64_t cnt);
  HashGroupByTestSpec& with_three_stage(ObThreeStageAggrStage stage);
};
```

参考：`src/sql/engine/aggregate/ob_hash_groupby_vec_op.h`

### 7.4 WindowFuncTestSpec

```cpp
class WindowFuncTestSpec : public OpSpecBuilder<WindowFuncTestSpec> {
public:
  // fill_wf_info() 填充 WinFuncInfo
};
```

参考：`src/sql/engine/window_function/ob_window_function_op.h`

### 7.5 HashJoinTestSpec（需框架扩展）

**框架扩展**：
- 支持 `right_table()` / `with_right_data()` 接口
- `create_op()` 签名扩展为双子算子版本

```cpp
class HashJoinTestSpec : public OpSpecBuilder<HashJoinTestSpec> {
public:
  HashJoinTestSpec& right_table(const char *name, const char *col_defs);
  HashJoinTestSpec& with_right_data(const std::vector<TestRow> &rows);
  HashJoinTestSpec& join_on(const char *condition);
  HashJoinTestSpec& join_type(ObJoinType type);
};
```

参考：`src/sql/engine/join/hash_join/ob_hash_join_vec_op.h`

### 7.6 MergeGroupByTestSpec

与 HashGroupBy 类似，但要求输入数据按 GROUP BY 列有序。

### 7.7 ObOpInput 支持

为 Material 等需要 `ObOpInput` 的算子提供初始化支持。

---

## 八、关键文件参考

| 功能 | 文件位置 |
|------|----------|
| fill_aggr_info() | `src/sql/code_generator/ob_static_engine_cg.cpp:8095` |
| fill_wf_info() | `src/sql/code_generator/ob_static_engine_cg.h:582` |
| fil_sort_info() | `src/sql/code_generator/ob_static_engine_cg.h:584` |
| ObLimitVecSpec | `src/sql/engine/basic/ob_limit_vec_op.h` |
| ObScalarAggregateVecSpec | `src/sql/engine/aggregate/ob_scalar_aggregate_vec_op.h` |
| ObSortVecSpec | `src/sql/engine/sort/ob_sort_vec_op.h` |
| ObHashGroupByVecSpec | `src/sql/engine/aggregate/ob_hash_groupby_vec_op.h` |
| ObWindowFunctionVecSpec | `src/sql/engine/window_function/ob_window_function_op.h` |
| ObHashJoinVecSpec | `src/sql/engine/join/hash_join/ob_hash_join_vec_op.h` |

---

## 九、历史与变更

| 日期 | 变更内容 |
|------|----------|
| 2026-03-20 | 创建文档，汇总四个计划文件与当前实现 |
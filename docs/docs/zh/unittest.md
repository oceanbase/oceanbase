# 编写以及运行单测

## 如何编译及运行所有单元测试

[OceanBase](https://github.com/oceanbase/oceanbase) 有两个单元测试目录。

- `unittest` ：这是主要的单元测试用例，它测试 `src` 目录中的代码。

- `deps/oblib/unittest`： oblib 的测试用例。

首先，你需要编译 `unittest`。你需要进入构建目录中的 `unittest` 目录并显式编译。当你构建 oceanbase 项目时，默认不会构建 unittest。例如：

```bash
bash build.sh --init --make # init and build a debug mode project
cd build_debug/unittest  # 或者 cd build_debug/deps/oblib/unittest
make -j4 # build unittest
```

接着可以运行 `run_tests.sh` 脚本来运行所有测试用例。

## 编译并运行单个测试

可以编译并运行单个测试用例。你可以进入 `build_debug` 目录，执行 `make case-name` 来编译特定的测试用例并运行生成的二进制文件。例如：

```bash
cd build_debug
# **注意**: 不要进入unittest目录
make -j4 test_chunk_row_store
find . -name "test_chunk_row_store"
# 可以看到 ./unittest/sql/engine/basic/test_chunk_row_store
./unittest/sql/engine/basic/test_chunk_row_store
```

## 编写单元测试

作为一个 C++ 项目，[OceanBase](https://github.com/oceanbase/oceanbase)使用[google test](https://github.com/google/googletest)作为单元测试框架。

OceanBase 使用 `test_xxx.cpp` 作为单元测试文件名。你可以创建一个 `test_xxx.cpp` 文件，并将文件名添加到特定的 `CMakeLists.txt` 文件中。

在 `test_xxx.cpp` 文件中，你需要添加头文件 `#include <gtest/gtest.h>` 和下面的 main 函数。

```cpp
int main(int argc, char **argv)
{
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
```

接着可以添加一些函数来测试不同的场景。下面是 `test_ra_row_store_projector.cpp` 的一个例子。

```cpp
///
/// TEST 是 google test 的一个宏。
/// 用来添加一个新的测试函数。
///
/// RARowStore 是一个测试套件的名字，alloc_project_fail 是测试的名字。
///
TEST(RARowStore, alloc_project_fail)
{
  ObEmptyAlloc alloc;
  ObRARowStore rs(&alloc, true);

  ///
  /// ASSERT_XXX 是一些测试宏，可以帮助我们判断结果是否符合预期，如果失败会终止测试。
  ///
  /// 还有一些其它的测试宏，以 `EXPECT_` 开头，如果失败不会终止测试。
  ///
  ASSERT_EQ(OB_SUCCESS, rs.init(100 << 20));
  const int64_t OBJ_CNT = 3;
  ObObj objs[OBJ_CNT];
  ObNewRow r;
  r.cells_ = objs;
  r.count_ = OBJ_CNT;
  int64_t val = 0;
  for (int64_t i = 0; i < OBJ_CNT; i++) {
    objs[i].set_int(val);
    val++;
  }

  int32_t projector[] = {0, 2};
  r.projector_ = projector;
  r.projector_size_ = ARRAYSIZEOF(projector);

  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, rs.add_row(r));
}
```

可以查看文档[google test document](https://google.github.io/googletest/)获取更多关于 `TEST`, `ASSERT` 和 `EXPECT` 的细节。

## GitHub CI的单测

在合并拉取请求之前，CI将测试您的拉取请求。`Farm`将测试`mysql test`和`unittest`。您可以查看下面的`Details`链接的详细信息。

![github ci](images/unittest-github-ci.png)

![github ci farm 详情](images/unittest-ci-details.png)

![Farm unittest](images/unittest-unittest.png)

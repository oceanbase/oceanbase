/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

namespace oceanbase
{
namespace sql
{
extern void __init_vec_cast_func0();
extern void __init_vec_cast_func1();
extern void __init_vec_cast_func2();
extern void __init_vec_cast_func3();
extern void __init_vec_cast_func4();
extern void __init_vec_cast_func5();
extern void __init_vec_cast_func6();

int __init_all_vec_cast_funcs()
{
  __init_vec_cast_func0();
  __init_vec_cast_func1();
  __init_vec_cast_func2();
  __init_vec_cast_func3();
  __init_vec_cast_func4();
  __init_vec_cast_func5();
  __init_vec_cast_func6();
  return 0;
}
} // end sq
} // end oceanbase
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
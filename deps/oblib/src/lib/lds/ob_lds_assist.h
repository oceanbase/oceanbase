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

#ifndef OBLIB_LDS_ASSIST_H
#define OBLIB_LDS_ASSIST_H

#include "lib/utility/ob_macro_utils.h"

/*
  辅助linker script的相关接口
  基本思路是利用linker script的section排序功能(by name)，
  定义两个section name特殊的begin和end节点，
  按字典序排序后这两个节点之间的对象即为目标节点

  假设section为mydata, 链接后地址布局如下:
  var_name         section_name(link前)   section_name(link后)
  ------------------------------------------------------------
  mydata_begin             mydata               mydata
  a                        mydata1              mydata
  b                        mydata2              mydata
  c                        mydata3              mydata
  mydata_end               mydataz              mydata

  另外值得注意的是，这里begin/end没有采用只在link时定义的常用做法，
  而是直接编译期定义, 这是为了解决动态链接问题, 还是以上面的a, b, c为例,
  动态链接下，多个extern同名全局变量以主程序中的为准，link时主程序看不到so中的a,b,c, 从而造成begin与end地址相同
*/

#define LDS_ATTRIBUTE_(section_name) __attribute__((section(#section_name)))
#define LDS_ATTRIBUTE(section_name) LDS_ATTRIBUTE_(section_name)

/*
  在section内声明、定义一个全局变量
  eg.
     static LDS_VAR(mydata, int, i);
     extern LDS_VAR(mydata, int, i);
*/
#ifndef OB_USE_ASAN
#define LDS_VAR(section_name, type, name) \
  type name LDS_ATTRIBUTE(CONCAT(section_name, __COUNTER__))
#else
#define LDS_VAR(section_name, type, name) \
  type name
#endif

#define LDS_VAR_BEGIN_END(section_name, type)                   \
  static type section_name##_begin LDS_ATTRIBUTE(section_name); \
  static type section_name##_end LDS_ATTRIBUTE(section_name##z)

template<typename T, typename Func>
inline void do_ld_iter(T *s, T *e, Func &func)
{
  T *p = s;
  while (p < e) {
    /*
      @param 1: index
      @param 2: addr
    */
    func(p - s, p);
    p++;
  }
}

/*
  遍历section内所有全局变量, 在定义LDS_VAR_BEGIN_END的namespace下调用
*/
#define LDS_ITER(section_name, type, func) \
  do_ld_iter((type*)&section_name##_begin + 1, (type*)&section_name##_end, func)

#endif /* OBLIB_LDS_ASSIST_H */
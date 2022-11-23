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

#ifndef OCEANBASE_DATUM_OB_DATUM_UTIL_H_
#define OCEANBASE_DATUM_OB_DATUM_UTIL_H_

#include "common/object/ob_obj_type.h"

namespace oceanbase
{
namespace common
{

// const int => int map, used in compile time
template <int DEFAULT, int K, int V, int... args>
struct ObConstIntMapping
{
  // liner search key in args, not efficient, can only be used in compile time.
  static constexpr int liner_search(const int key)
  {
    return key == K ? V : ObConstIntMapping<DEFAULT, args...>::liner_search(key);
  }
};

template<int DEFAULT, int K, int V>
struct ObConstIntMapping<DEFAULT, K, V>
{
  static constexpr int liner_search(const int key) { return key == K ? V : DEFAULT; }
};

#define SELECT_ALL(...) __VA_ARGS__
#define SELECT_TYPE_TC(arg) SELECT_ALL arg
typedef ObConstIntMapping<ObMaxTC,
        LST_DO(SELECT_TYPE_TC, (,), OBJ_TYPE_TC_PAIRS)> ObObjTypeTCMap;
#undef SELECT_TYPE_TC
#undef SELECT_ALL

template <ObObjType TYPE>
struct ObObjTypeTraits
{
  constexpr static ObObjTypeClass tc_
      = static_cast<ObObjTypeClass>(ObObjTypeTCMap::liner_search(TYPE));
};

static_assert(ObObjTypeTraits<ObMaxType>::tc_ == ObMaxTC
              && ObObjTypeTraits<static_cast<ObObjType>(ObMaxType - 1)>::tc_ != ObMaxTC,
              "Wrong obj type to type class mapping");



// Two dimension array initializer:
//
//   for (int X = 0; X < N; X++) {
//     for (int Y = 0; Y < M; Y++) {
//       INITER<X, Y>::init_array();
//     }
//   }
template <int N, int M, template <int, int> class INITER, int X = 0, int Y = 0>
struct Ob2DArrayConstIniter
{
  constexpr static int NEXT_X = X + 1;
  constexpr static int NEXT_Y = Y + 1;
  static bool init()
  {
    if (X < N && Y < M) {
      INITER<X, Y>::init_array();
    }
    if (X < N - 1) {
      return Ob2DArrayConstIniter<N, M, INITER, NEXT_X, Y>::init();
    } else if (X == N - 1) {
      return Ob2DArrayConstIniter<N, M, INITER, 0, NEXT_Y>::init();
    } else {
      return true;
    }
  }
};

template <int N, int M, template <int, int> class INITER, int X>
struct Ob2DArrayConstIniter<N, M, INITER, X, M>
{
  static bool init() { return true; }
};

template <int N, int M, template <int, int> class INITER, int Y>
struct Ob2DArrayConstIniter<N, M, INITER, N, Y>
{
  static bool init() { return true; }
};

// array initializer:
//
//   for (int IDX = 0; IDX < N; IDX++) {
//     INITER<IDX>::init_array()
//   }
template <int N, template <int> class INITER, int IDX = 0>
struct ObArrayConstIniter
{
  constexpr static int NEXT = IDX + 1;
  static bool init()
  {
    if (IDX < N) {
      INITER<IDX>::init_array();
      return ObArrayConstIniter<N, INITER, NEXT>::init();
    } else {
      return true;
    }
  }
};

template <int N, template <int> class INITER>
struct ObArrayConstIniter<N, INITER, N>
{
  static bool init() { return true; }
};

} // end namespace common
} // end namespace oceanbase

#endif // OCEANBASE_DATUM_OB_DATUM_UTIL_H_

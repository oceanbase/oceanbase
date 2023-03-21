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
#ifndef OCEANBASE_STORAGE_OB_AVX512_SORT_H_
#define OCEANBASE_STORAGE_OB_AVX512_SORT_H_

#include <immintrin.h>
#include <avx512fintrin.h>
#include <climits>
#include <cfloat>
#include <cstring>
#include <ctype.h>
#include <stdint.h>

#include "share/ob_define.h"
#include "ob_avx512_64bit_sort.h"
namespace oceanbase {
namespace storage {


int64_t lg(int64_t n);
void do_sort(uint64_t* keys, uint64_t* values, const int64_t left, 
               const int64_t right, int64_t depth_limit);


template <typename T>
class SortWithAvx512 {
public:

    int sort(uint64_t* keys, T** values, const int64_t size)
    {    
      //uint64_t *indexes;
      //T** tmp_values;
      int ret = OB_SUCCESS;
      //ObArenaAllocator allocator_;
      if (keys == NULL || values == NULL || size < 0) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        ret = sort_loop(keys, values,(uint64_t*)values,0, 0, size);
      }
      return ret;
    }

    static bool isSupportedByAvx512Sort(int obj_type) {
      if (obj_type == common::ObTinyIntType || 
          obj_type == common::ObSmallIntType ||
          obj_type == common::ObInt32Type ||
          obj_type == common::ObIntType ||
          obj_type == common::ObUTinyIntType ||
          obj_type == common::ObUSmallIntType ||
          obj_type == common::ObUInt32Type ||
          obj_type == common::ObUInt64Type ||
          obj_type == common::ObFloatType ||
          obj_type == common::ObDoubleType ||
          obj_type == common::ObUFloatType ||
          obj_type == common::ObUDoubleType) {      
        return true;
      }
      return false;
    }    
private:
    /*
    * Due to the sorting algorithm implemented by the vectorization method can 
    * only sort numerical arrays.We use key-value to solve this problem. The key 
    * value array is used for comparison and sorting, the value array holds pointers 
    * to sorted elements. When the elements of the key exchanged, the elements 
    * of value are also exchanged.
    *
    * */
    int sort_loop(uint64_t* keys, T** values,uint64_t* indexes, int64_t round, 
                    int64_t left, int64_t right)
    {
        int ret = OB_SUCCESS;
        for (int64_t i = left; OB_SUCC(ret) && i < right; i++) {
            // Use the get_sort_key method provided by T to get the key needed for sorting
            ret = values[i]->get_sort_key(&keys[i]);
        }
        if (OB_SUCC(ret)) {
            // sort the keys array
            qsort_(keys+left,indexes+left,right-left);
        }

        /*
        * Process the same key value
        *
        * example:
        * If we use the sorting algorithm to sort the type T:
        *    struct T {
        *      uint64_t a,
        *      uint64_t b
        *      };
        * T's comparison function: 
        *    compare(T& t1, T& t2) {return t1.a == t2.a ? t1.b < t2.b : t1.a < t2.a;}
        *
        * Since the sorting algorithm implemented by vectorization can only sort 
        * numeric types, we first use T.a as the key to sort according to the 
        * comparison function. After a round of sorting with T.a, the same value 
        * of T.a will be sorted together, E.g: 
        *    1 1 1 2 2 3 3 3 3 3
        *
        * For the same T as T.a, we need to use T.b as the key for the next round of sorting.
        *
        * */
        int64_t idx = left;
        while (OB_SUCC(ret)) {
            while (idx < right - 1 && keys[idx] != keys[idx + 1]) {
            idx++;
            }
            int64_t next_left = idx;
            while (idx < right - 1 && keys[idx] == keys[idx + 1]) {
            idx++;
            }
            idx++;
            while (next_left < idx && next_left < right && 
                    values[next_left]->get_sort_key_end()) {
            next_left++;
            }
            if (next_left >= right - 1) {
            break;
            }
            if (idx - next_left > 1) {
            
            ret = sort_loop(keys, values,indexes, round + 1, next_left, idx);
            }
        }
        return ret;
    }
};

}  // end namespace storage
}  // end namespace oceanbase

#endif
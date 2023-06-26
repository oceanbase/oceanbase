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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_COMPILE_MAPPER_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_COMPILE_MAPPER_H
#define NEED_MDS_REGISTER_DEFINE
#include "mds_register.h"
#undef NEED_MDS_REGISTER_DEFINE
#include "map_type_index_in_tuple.h"
#include "deps/oblib/src/common/meta_programming/ob_type_traits.h"
#include "deps/oblib/src/common/meta_programming/ob_meta_compare.h"
#include "deps/oblib/src/common/meta_programming/ob_meta_copy.h"

// 这个文件负责生成两个编译期的映射关系，一个是从Helper类型和BufferCtx类型到ID的映射，以及反向映射
// 另一个是从Data类型到多版本标志的映射，不需要反向映射
namespace oceanbase
{
namespace storage
{
namespace mds
{

template <typename T>
class __TypeMapper {};

template <int ID>
class __IDMapper {};

// 调用以下宏生成编译期的 TYPE <-> ID 映射关系
#define REGISTER_TYPE_ID(HELPER, CTX, ID) \
template <>\
class __TypeMapper<HELPER> {\
public:\
  static const std::uint64_t id = ID;\
};\
template <>\
class __IDMapper<ID> {\
public:\
  typedef HELPER helper_type;\
  typedef CTX ctx_type;\
};

#define NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
#define _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(helper_type, buffer_ctx_type, ID, TEST) \
REGISTER_TYPE_ID(helper_type, buffer_ctx_type, ID)
#include "mds_register.h"
#undef _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_
#undef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION

// 通过以下宏在编译期获取这些信息
template <typename Tuple, int IDX>
struct TupleIdxType {
  typedef typename std::decay<decltype(std::declval<Tuple>().template element<IDX>())>::type type;
};
template <typename Tuple, typename Type>
struct TupleTypeIdx {
  template <int IDX>
  static constexpr int64_t get_type_idx() {
    return std::is_same<Type, typename TupleIdxType<Tuple, IDX>::type>::value ?
                              IDX :
                              get_type_idx<IDX + 1>();
  }
  template <>
  static constexpr int64_t get_type_idx<Tuple::get_element_size()>() {
    return Tuple::get_element_size();
  }
  static constexpr int64_t value = get_type_idx<0>();
};
#define GET_HELPER_TYPE_BY_ID(ID) __IDMapper<ID>::helper_type
#define GET_HELPER_ID_BY_TYPE(T) __TypeMapper<T>::id
#define GET_CTX_TYPE_BY_ID(ID) __IDMapper<ID>::ctx_type
#define GET_CTX_TYPE_BY_TUPLE_IDX(IDX) \
typename std::decay<decltype(std::declval<BufferCtxTupleHelper>().element<IDX>())>::type

#define GET_MULTIVERSION_FLAG(K, V) get_multi_version_flag<K, V>()

#undef REGISTER_TYPE_ID
#undef REGISTER_TYPE_MULTI_VERSION_FLAG

}
}
}

#endif
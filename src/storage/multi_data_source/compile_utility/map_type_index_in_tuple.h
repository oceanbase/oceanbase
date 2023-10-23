/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MAP_TYPE_INDEX_IN_TUPLE_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MAP_TYPE_INDEX_IN_TUPLE_H

#define NEED_MDS_REGISTER_DEFINE
#include "mds_register.h"
#undef NEED_MDS_REGISTER_DEFINE
#include "lib/container/ob_tuple.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

template <typename K, typename V, bool MULTI_VERSION>
struct TypeHelper {};

template <typename FIRST, typename ...Args>
struct DropFirstElemtTuple { typedef common::ObTuple<Args...> type; };

#define GENERATE_TEST_MDS_TABLE
#define GENERATE_NORMAL_MDS_TABLE
#define GENERATE_LS_INNER_MDS_TABLE
#define _GENERATE_MDS_UNIT_(KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION) \
,TypeHelper<KEY_TYPE, VALUE_TYPE, NEED_MULTI_VERSION>, \
TypeHelper<KEY_TYPE, VALUE_TYPE, !NEED_MULTI_VERSION>
typedef DropFirstElemtTuple<char
#include "mds_register.h"
>::type MdsTableTypeHelper;
#undef _GENERATE_MDS_UNIT_
#undef GENERATE_LS_INNER_MDS_TABLE
#undef GENERATE_NORMAL_MDS_TABLE
#undef GENERATE_TEST_MDS_TABLE

#define NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION
#define _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_(HELPER_CLASS, BUFFER_CTX_TYPE, ID, ENUM_NAME) \
,BUFFER_CTX_TYPE
typedef DropFirstElemtTuple<char
#include "mds_register.h"
>::type BufferCtxTupleHelper;
#undef _GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION_
#undef NEED_GENERATE_MDS_FRAME_CODE_FOR_TRANSACTION

template <typename K, typename V>
inline constexpr bool get_multi_version_flag() {
  return (MdsTableTypeHelper::get_element_index<TypeHelper<K, V, true>>() & 1) == 0;
}

}
}
}
#endif
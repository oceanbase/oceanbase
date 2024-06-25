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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_DUMP_OBJ_PRINTER_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_DUMP_OBJ_PRINTER_H
#include "mds_dump_node.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

struct MdsDumpObjComparer
{
  template <typename MdsTableType>
  struct InnerMdsDumpObjComparer
  {
    template <int IDX>
    int help_compare_key(const MdsDumpKey &lhs,
                         const MdsDumpKey &rhs,
                         int &result)
    {
      #define PRINT_WRAPPER KR(ret), K(lhs.mds_table_id_), K(lhs.mds_unit_id_), K(result)
      int ret = OB_SUCCESS;
      MDS_TG(1_ms);
      if (IDX == lhs.mds_unit_id_) {
        using UnitType = typename std::decay<
                         decltype(std::declval<MdsTableType>().template element<IDX>())>::type;
        using KeyType = typename UnitType::key_type;
        KeyType lhs_key;
        KeyType rhs_key;
        int64_t pos = 0;
        if (MDS_FAIL(lhs_key.mds_deserialize(lhs.key_.ptr(), lhs.key_.length(), pos))) {
          MDS_LOG_NONE(ERROR, "fail to deseialize lhs key");
        } else if (FALSE_IT(pos = 0)) {
        } else if (MDS_FAIL(rhs_key.mds_deserialize(rhs.key_.ptr(), rhs.key_.length(), pos))) {
          MDS_LOG_NONE(ERROR, "fail to deseialize rhs key");
        } else if (OB_FAIL(compare_binary_key(lhs_key, rhs_key, result))) {
          MDS_LOG_NONE(ERROR, "fail to compare binary key");
        } else {
          MDS_LOG_NONE(DEBUG, "success to compare non dummy key");
        }
      } else if (MDS_FAIL(help_compare_key<IDX + 1>(lhs, rhs, result))) {
      }
      return ret;
      #undef PRINT_WRAPPER
    }
    template <>
    int help_compare_key<MdsTableType::get_element_size()>(const MdsDumpKey &lhs,
                                                        const MdsDumpKey &rhs,
                                                        int &result)
    {
      int ret = OB_ERR_UNEXPECTED;
      MDS_LOG(ERROR, "no this mds_unit_id", K(lhs.mds_unit_id_), KR(ret));
      return ret;
    }
  };
  template <int IDX>
  int help_compare(const MdsDumpKey &lhs, const MdsDumpKey &rhs, int &result)
  {
    MDS_TG(1_ms);
    int ret = OB_SUCCESS;
    if (IDX == lhs.mds_table_id_) {
      using MdsTableType = typename std::decay<
                        decltype(std::declval<MdsTableTypeTuple>().element<IDX>())>::type;
      InnerMdsDumpObjComparer<MdsTableType> inner_helper;
      inner_helper.template help_compare_key<0>(lhs, rhs, result);
    } else if (MDS_FAIL(help_compare<IDX + 1>(lhs, rhs, result))) {
    }
    return ret;
  }
  template <>
  int help_compare<MdsTableTypeTuple::get_element_size()>(const MdsDumpKey &lhs,
                                                          const MdsDumpKey &rhs,
                                                          int &result)
  {
    int ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "no this mds_table_id", K(lhs.mds_table_id_), KR(ret));
    return ret;
  }
};

struct MdsDumpObjPrinter
{
  template <typename MdsTableType>
  struct InnerMdsDumpObjPrinter
  {
    template <int IDX>
    void help_print_key(const uint8_t mds_unit_id,
                        const ObString &data_buf,
                        char *buf,
                        const int64_t buf_len,
                        int64_t &pos)
    {
      int ret = OB_SUCCESS;
      MDS_TG(1_ms);
      if (IDX == mds_unit_id) {
        using UnitType = typename std::decay<
                         decltype(std::declval<MdsTableType>().template element<IDX>())>::type;
        using KeyType = typename UnitType::key_type;
        char stack_buffer[sizeof(KeyType)];
        KeyType *user_key = (KeyType *)stack_buffer;
        new (user_key) KeyType();
        int64_t des_pos = 0;
        if (MDS_FAIL(user_key->mds_deserialize(data_buf.ptr(), data_buf.length(), des_pos))) {
          databuff_printf(buf, buf_len, pos, "user_key:ERROR:%d", ret);
        } else {
          databuff_printf(buf, buf_len, pos, "user_key:%s", to_cstring(*user_key));
        }
        user_key->~KeyType();
      } else {
        help_print_key<IDX + 1>(mds_unit_id, data_buf, buf, buf_len, pos);
      }
    }
    template <>
    void help_print_key<MdsTableType::get_element_size()>(const uint8_t mds_unit_id,
                                                       const ObString &data_buf,
                                                       char *buf,
                                                       const int64_t buf_len,
                                                       int64_t &pos)
    {
      databuff_printf(buf, buf_len, pos,
                      "user_key:ERROR:unit_id not in tuple(%ld)", (int64_t)mds_unit_id);
    }
    template <int IDX>
    void help_print_data(const uint8_t mds_unit_id,
                         const ObString &data_buf,
                         char *buf,
                         const int64_t buf_len,
                         int64_t &pos)
    {
      int ret = OB_SUCCESS;
      MDS_TG(1_ms);
      if (IDX == mds_unit_id) {
        using UnitType = typename std::decay<
                         decltype(std::declval<MdsTableType>().template element<IDX>())>::type;
        using ValueType = typename UnitType::value_type;
        char stack_buffer[sizeof(ValueType)];
        ValueType *user_data = (ValueType *)stack_buffer;
        new (user_data) ValueType();
        int64_t des_pos = 0;
        meta::MetaSerializer<ValueType> serializer(DefaultAllocator::get_instance(), *user_data);
        if (MDS_FAIL(serializer.deserialize(data_buf.ptr(), data_buf.length(), des_pos))) {
          databuff_printf(buf, buf_len, pos, "user_data:ERROR:%d", ret);
        } else {
          databuff_printf(buf, buf_len, pos, "user_data:%s", to_cstring(*user_data));
        }
        user_data->~ValueType();
      } else {
        help_print_data<IDX + 1>(mds_unit_id, data_buf, buf, buf_len, pos);
      }
    }
    template <>
    void help_print_data<MdsTableType::get_element_size()>(const uint8_t mds_unit_id,
                                                        const ObString &data_buf,
                                                        char *buf,
                                                        const int64_t buf_len,
                                                        int64_t &pos)
    {
      databuff_printf(buf, buf_len, pos,
                      "user_data:ERROR:mds_unit_id not in tuple(%ld)", (int64_t)mds_unit_id);
    }
  };
  template <int IDX>
  void help_print(const bool need_print_key,
                  const uint8_t mds_table_id,
                  const uint8_t mds_unit_id,
                  const ObString &data_buf,
                  char *buf,
                  const int64_t buf_len,
                  int64_t &pos)
  {
    MDS_TG(1_ms);
    int ret = OB_SUCCESS;
    if (IDX == mds_table_id) {
      using MdsTableType = typename std::decay<
                        decltype(std::declval<MdsTableTypeTuple>().element<IDX>())>::type;
      InnerMdsDumpObjPrinter<MdsTableType> inner_helper;
      if (need_print_key) {
        inner_helper.template help_print_key<0>(mds_unit_id, data_buf, buf, buf_len, pos);
      } else {
        inner_helper.template help_print_data<0>(mds_unit_id, data_buf, buf, buf_len, pos);
      }
    } else {
      help_print<IDX + 1>(need_print_key, mds_table_id, mds_unit_id, data_buf, buf, buf_len, pos);
    }
  }
  template <>
  void help_print<MdsTableTypeTuple::get_element_size()>(const bool need_print_key,
                                                         const uint8_t mds_table_id,
                                                         const uint8_t mds_unit_id,
                                                         const ObString &data_buf,
                                                         char *buf,
                                                         const int64_t buf_len,
                                                         int64_t &pos)
  {
    databuff_printf(buf, buf_len, pos,
                    "user_data:ERROR:table_id not in tuple(%ld)", (int64_t)mds_table_id);
  }
};

}
}
}
#endif
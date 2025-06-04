//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#ifndef OB_SHARE_TRUNCATE_INFO_TRUNCATE_EXPR_UTIL_H_
#define OB_SHARE_TRUNCATE_INFO_TRUNCATE_EXPR_UTIL_H_
#include "share/schema/ob_schema_struct.h"
#include "src/share/ob_rpc_struct.h"
namespace oceanbase
{
namespace share
{

struct ObTruncateInfoUtil final
{
  static const uint64_t TRUNCATE_INFO_CMP_DATA_VERSION = DATA_VERSION_4_3_5_2;
  static bool could_write_truncate_info_part_type(
    const obrpc::ObAlterTableArg::AlterPartitionType &alter_type,
    const schema::ObPartitionFuncType input_part_type,
    const schema::ObPartitionFuncType input_subpart_type)
  {
    bool bret = false;
    if (obrpc::ObAlterTableArg::DROP_PARTITION == alter_type ||
        obrpc::ObAlterTableArg::TRUNCATE_PARTITION == alter_type) {
      bret = could_write_truncate_info_part_type(input_part_type);
    } else if (obrpc::ObAlterTableArg::DROP_SUB_PARTITION == alter_type ||
        obrpc::ObAlterTableArg::TRUNCATE_SUB_PARTITION == alter_type) {
      bret = could_write_truncate_info_part_type(input_part_type)
             && could_write_truncate_info_part_type(input_subpart_type);
    }
    return bret;
  }
private:
  static bool could_write_truncate_info_part_type(
    const schema::ObPartitionFuncType input_part_type)
  {
    return (schema::PARTITION_FUNC_TYPE_RANGE == input_part_type
      || schema::PARTITION_FUNC_TYPE_RANGE_COLUMNS == input_part_type
      || schema::PARTITION_FUNC_TYPE_LIST == input_part_type
      || schema::PARTITION_FUNC_TYPE_LIST_COLUMNS == input_part_type);
  }
};

} // namespace share
} // namespace oceanbase

#endif // OB_SHARE_TRUNCATE_INFO_TRUNCATE_EXPR_UTIL_H_

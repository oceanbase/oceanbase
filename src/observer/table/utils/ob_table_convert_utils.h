/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_CONVERT_UTILS_H
#define _OB_TABLE_CONVERT_UTILS_H 1

namespace oceanbase
{

// forward declaration
namespace common
{
class ObIAllocator;
class ObObj;
} // end namespace common

namespace table
{
class ObTableColumnInfo;
class ObTableConvertUtils
{
public:
  /**
   * @param [in] column_info The info of the column, including column name and type information.
   *                         Used to verify or determine the target type.
   * @param [in,out] obj The object to be converted. On input, it may be a varchar or jsontype.
   *                     On output, it will be set to jsontype with json_bin data.
   */
  static int convert_to_json_bin(common::ObIAllocator &allocator,
                                 const ObTableColumnInfo &column_info,
                                 common::ObObj &obj);
  /**
   * @param [in,out] obj The object to be converted. On input, it must be a jsontype.
   *                     On output, it will be set to a varchar type containing the json_text.
   */
  static int convert_to_json_text(common::ObIAllocator &allocator, common::ObObj &obj);
};

} // end namespace table
} // end namespace oceanbase
#endif /* _OB_TABLE_CONVERT_UTILS_H */

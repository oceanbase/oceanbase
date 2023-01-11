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

#ifndef _OB_TABLE_UTILS_H
#define _OB_TABLE_UTILS_H
#include<common/rowkey/ob_rowkey.h>

namespace oceanbase
{
namespace table
{
class ObTableUtils
{
public:
  // change all rowkeys' collation(char/varchar/binary/varbinary) into CS_TYPE_UTF8MB4_GENERAL_CI
  static int set_rowkey_collation(ObRowkey &rowkey);
private:
  ObTableUtils() = delete;
  ~ObTableUtils() = delete;
};

} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_UTILS_H */
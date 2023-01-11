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

#define USING_LOG_PREFIX SERVER
#include "ob_table_utils.h"

using namespace oceanbase::common;
using namespace oceanbase::table;

int ObTableUtils::set_rowkey_collation(ObRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(rowkey.get_obj_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rowkey obj ptr is null", K(rowkey), K(ret));
  }
  for (int i = 0; i < rowkey.get_obj_cnt(); ++i) {
    ObObj &obj = rowkey.get_obj_ptr()[i];
    if (obj.is_varbinary_or_binary() || obj.is_varchar_or_char()) {
      obj.set_collation_type(ObCollationType::CS_TYPE_UTF8MB4_GENERAL_CI);
    }
  }
  return ret;
}
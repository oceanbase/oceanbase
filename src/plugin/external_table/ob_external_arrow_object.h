/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#pragma once

#include "plugin/external_table/ob_external_arrow_status.h"

namespace oceanbase {

namespace common {
class ObObj;
}

namespace plugin {
namespace external {

/**
 * Map from ObObj to Arrow Type
 */
class ObArrowObject
{
public:

  static int get_array(const common::ObObj &obj, shared_ptr<Array> &array_value);
  /// @note It doesn't return error code if obj type is not supported
  static int get_scalar_value(const common::ObObj &obj, shared_ptr<Scalar> &scalar_value);
};

} // namespace external
} // namespace plugin
} // namespace oceanbase

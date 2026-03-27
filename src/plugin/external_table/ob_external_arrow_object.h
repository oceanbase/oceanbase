/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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

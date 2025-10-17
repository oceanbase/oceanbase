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

#define USING_LOG_PREFIX SHARE

#include "plugin/external_table/ob_external_arrow_status.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_print_utils.h"

using namespace oceanbase::common;

namespace oceanbase {
namespace plugin {
namespace external {

ObArrowStatus &ObArrowStatus::operator=(Status &&status)
{
  this->status_ = std::move(status);
  return_code_ = status_to_return_code(status_);
  return *this;
}

int ObArrowStatus::status_to_return_code(const Status &status)
{
  StatusCode code = status.code();
  switch (code) {
    case StatusCode::OK:                           return OB_SUCCESS;
    case StatusCode::OutOfMemory:                  return OB_ALLOCATE_MEMORY_FAILED;
    case StatusCode::KeyError :                    return OB_ERROR;
    case StatusCode::TypeError:                    return OB_ERROR;
    case StatusCode::Invalid:                      return OB_ERROR;
    case StatusCode::IOError:                      return OB_IO_ERROR;
    case StatusCode::CapacityError:                return OB_ERROR;
    case StatusCode::IndexError:                   return OB_ERROR_OUT_OF_RANGE;
    case StatusCode::Cancelled:                    return OB_CANCELED;
    case StatusCode::UnknownError:                 return OB_ERROR;
    case StatusCode::NotImplemented:               return OB_NOT_IMPLEMENT;
    case StatusCode::SerializationError:           return OB_SERIALIZE_ERROR;
    case StatusCode::RError:                       return OB_ERROR;
    case StatusCode::CodeGenError:                 return OB_ERROR;
    case StatusCode::ExpressionValidationError:    return OB_ERROR;
    case StatusCode::ExecutionError:               return OB_ERROR;
    case StatusCode::AlreadyExists:                return OB_ENTRY_EXIST;
    default:                                       return OB_ERROR;
  }
}

int64_t ObArrowStatus::to_string(char buf[], int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len, pos, "ret=%d, status:%s", return_code_, status_.ToString().c_str());
  return pos;
}

} // namespace external
} // namespace plugin
} // namespace oceanbase

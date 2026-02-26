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

#include <ios>
#include <apache-arrow/arrow/api.h>

#include "lib/utility/ob_macro_utils.h"

namespace oceanbase {
namespace plugin {
namespace external {

/// import `arrow` namespace in `oceanbase::plugin::external`.
/// **Namespaces will not be polluted**
/// Other namespace can't visit symbols in `arrow` namespace
/// even if they include this header and import `external` namespace.
/// **Other things**
/// Some symbols are conflict in the OceanBase, mysql headers and arrow,
/// such as uint16, uint64. So, I can't use `using namespace arrow` in
/// some cpp files when the OceanBase is built utility (compile many cpp
/// files as a bundle).
using namespace arrow;
using namespace std;

#define OBARROW_SUCC(expr) OB_LIKELY((obstatus = (expr)).ok())
#define OBARROW_FAIL(expr) OB_UNLIKELY(!((obstatus = (expr)).ok()))

#define OB_TRY_BEGIN     try { do{} while (0)
#define OB_TRY_END       }                                                                                       \
  catch (const bad_alloc &ex) {                                                                                  \
    ret = OB_ALLOCATE_MEMORY_FAILED;                                                                             \
    LOG_WARN("failed to allocate memory", K(ret), KCSTRING(ex.what()));                                          \
  } catch (const ios_base::failure &ex) {                                                                        \
    ret = OB_IO_ERROR;                                                                                           \
    LOG_WARN("io error", KCSTRING(ex.what()));                                                                   \
  } catch (const system_error &ex) {                                                                             \
    ret = OB_ERROR;                                                                                              \
    LOG_WARN("system error", K(ex.code().value()), KCSTRING(ex.code().message().c_str()), KCSTRING(ex.what()));  \
  } catch (const exception &ex) {                                                                                \
    ret = OB_ERROR;                                                                                              \
    LOG_WARN("catch unknown cpp exception", KCSTRING(ex.what()));                                                \
  } catch (...) {                                                                                                \
    ret = OB_ERROR;                                                                                              \
    LOG_WARN("catch unknown exception");                                                                         \
  }

class ObArrowStatus final
{
public:
  explicit ObArrowStatus(int &ret) : return_code_(ret)
  {}

  ObArrowStatus &operator = (Status &&status);

  bool ok() const { return status_.ok(); }

  int64_t to_string(char buf[], int64_t buf_len) const;

private:
  static int status_to_return_code(const Status &status);

private:
  int &return_code_;
  Status status_;
};

} // namespace external
} // namespace plugin
} // namespace oceanbase

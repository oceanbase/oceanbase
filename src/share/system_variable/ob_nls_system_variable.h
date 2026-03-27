/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SQL_SESSION_OB_NLS_SYSTEM_VARIABLE_
#define OCEANBASE_SQL_SESSION_OB_NLS_SYSTEM_VARIABLE_

#include <utility>
#include "common/object/ob_object.h"

namespace oceanbase
{
namespace share
{
class IsoCurrencyUtils {
public:
  typedef std::pair<const char*, const char*> CountryCurrencyPair;
  IsoCurrencyUtils() {}
  static int get_currency_by_country_name(const common::ObString &country_name,
                                          common::ObString &currency_name);
  static bool is_country_valid(const common::ObString &country_name);
  static const CountryCurrencyPair COUNTRY_CURRENCY_LIST_[];
};

}
}
#endif // OCEANBASE_SQL_SESSION_OB_NLS_SYSTEM_VARIABLE_
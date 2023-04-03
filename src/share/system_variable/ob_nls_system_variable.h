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
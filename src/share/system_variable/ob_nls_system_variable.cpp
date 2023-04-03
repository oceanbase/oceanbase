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

#define USING_LOG_PREFIX SHARE
#include "ob_nls_system_variable.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace share
{
const IsoCurrencyUtils::CountryCurrencyPair IsoCurrencyUtils::COUNTRY_CURRENCY_LIST_[] =
{
  {"AFGHANISTAN", "AFN"},
  {"ALBANIA", "ALL"},
  {"ALBANIA", "DZD"},
  {"ARGENTINA", "ARS"},
  {"ARMENIA", "AMD"},
  {"AUSTRALIA", "AUD"},
  {"AUSTRIA", "EUR"},
  {"AZERBAIJAN", "AZN"},
  {"BAHAMAS", "BSD"},
  {"BAHRAIN", "BHD"},
  {"BANGLADESH", "BDT"},
  {"BELARUS", "BYN"},
  {"BELGIUM", "EUR"},
  {"BELIZE", "BZD"},
  {"BERMUDA", "BMD"},
  {"BOLIVIA", "BOB"},
  {"BRAZIL", "BRL"},
  {"BULGARIA", "BGN"},
  {"CAMBODIA", "KHR"},
  {"CAMEROON", "XAF"},
  {"CANADA", "CAD"},
  {"CHILE", "CLP"},
  {"CHINA", "CNY"},
  {"COLOMBIA", "COP"},
  {"COSTA RICA", "CRC"},
  {"CROATIA", "HRK"},
  {"CYPRUS", "EUR"},
  {"CZECH REPUBLIC", "CZK"},
  {"DJIBOUTI", "DJF"},
  {"ECUADOR", "USD"},
  {"EGYPT", "EGP"},
  {"EL SALVADOR", "SVC"},
  {"ESTONIA", "EUR"},
  {"ETHIOPIA", "ETB"},
  {"FINLAND", "EUR"},
  {"FRANCE", "EUR"},
  {"GABON", "XAF"},
  {"GERMANY", "EUR"},
  {"GREECE", "EUR"},
  {"GUATEMALA", "GTQ"},
  {"HONDURAS", "HNL"},
  {"HONG KONG", "HKD"},
  {"HUNGARY", "HUF"},
  {"ICELAND", "ISK"},
  {"INDIA", "INR"},
  {"INDONESIA", "IDR"},
  {"IRAN", "IRR"},
  {"IRAQ", "IQD"},
  {"IRELAND", "EUR"},
  {"ISRAEL", "ILS"},
  {"ITALY", "EUR"},
  {"JAPAN", "JPY"},
  {"JORDAN", "JOD"},
  {"KAZAKHSTAN", "KZT"},
  {"KENYA", "KES"},
  {"KOREA", "KRW"},
  {"KUWAIT", "KWD"},
  {"LATVIA", "EUR"},
  {"LEBANON", "LBP"},
  {"LIBYA", "LYD"},
  {"LITHUANIA", "EUR"},
  {"LUXEMBOURG", "EUR"},
  {"MALAYSIA", "MYR"},
  {"MALDIVES", "MVR"},
  {"MALTA", "EUR"},
  {"MAURITANIA", "MRU"},
  {"MEXICO", "MXN"},
  {"MONTENEGRO", "EUR"},
  {"MOROCCO", "MAD"},
  {"NEPAL", "NPR"},
  {"NEW ZEALAND", "NZD"},
  {"NICARAGUA", "NIO"},
  {"NIGERIA", "NGN"},
  {"NORWAY", "NOK"},
  {"OMAN", "OMR"},
  {"PAKISTAN", "PKR"},
  {"PANAMA", "PAB"},
  {"PARAGUAY", "PYG"},
  {"PERU", "PEN"},
  {"PHILIPPINES", "PHP"},
  {"POLAND", "PLN"},
  {"PORTUGAL", "EUR"},
  {"QATAR", "QAR"},
  {"ROMANIA", "RON"},
  {"SENEGAL", "XOF"},
  {"SERBIA", "RSD"},
  {"SINGAPORE", "SGD"},
  {"SLOVAKIA", "EUR"},
  {"SLOVENIA", "EUR"},
  {"SOMALIA", "SOS"},
  {"SPAIN", "EUR"},
  {"SRI LANKA", "LKR"},
  {"SUDAN", "SDG"},
  {"SWEDEN", "SEK"},
  {"SWITZERLAND", "CHE"},
  {"TAIWAN", "TWD"},
  {"THAILAND", "THB"},
  {"TUNISIA", "TND"},
  {"TURKEY", "TRY"},
  {"UGANDA", "UGX"},
  {"UKRAINE", "UAH"},
  {"UNITED ARAB EMIRATES", "AED"},
  {"UNITED KINGDOM", "GBP"},
  {"AMERICA", "USD"},
  {"URUGUAY", "UYU"},
  {"UZBEKISTAN", "UZS"},
  {"VENEZUELA", "VEF"},
  {"YEMEN", "YER"},
  {"ZAMBIA", "ZMW"}
};

int IsoCurrencyUtils::get_currency_by_country_name(const common::ObString &country_name,
                                                   common::ObString &currency_name)
{
  int ret = OB_SUCCESS;
  currency_name.reset();
  if (country_name.empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("country name is empty", K(ret));
  } else {
    int32_t count = sizeof(COUNTRY_CURRENCY_LIST_) / sizeof(COUNTRY_CURRENCY_LIST_[0]);
    for (int32_t i = 0; i < count; ++i) {
      const CountryCurrencyPair &country_currency_pair = COUNTRY_CURRENCY_LIST_[i];
      if (country_name.length() == static_cast<int32_t>(strlen(country_currency_pair.first))
          && (0 == country_name.case_compare(country_currency_pair.first))) {
        currency_name.assign_ptr(country_currency_pair.second,
                                 static_cast<int32_t>(strlen(country_currency_pair.second)));
        break;
      }
    }
    if (currency_name.empty()) {
      ret = OB_ERR_UNEXPECTED;
    }
  }
  return ret;
}

bool IsoCurrencyUtils::is_country_valid(const common::ObString &country_name)
{
  bool bool_ret = false;
  if (!country_name.empty()) {
    int32_t count = sizeof(COUNTRY_CURRENCY_LIST_) / sizeof(COUNTRY_CURRENCY_LIST_[0]);
    for (int32_t i = 0; i < count; ++i) {
      const CountryCurrencyPair &country_currency_pair = COUNTRY_CURRENCY_LIST_[i];
      if (country_name.length() == static_cast<int32_t>(strlen(country_currency_pair.first))
          && (0 == country_name.case_compare(country_currency_pair.first))) {
        bool_ret = true;
        break;
      }
    }
  }
  return bool_ret;
}
}
}
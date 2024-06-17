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
#include "lib/locale/ob_locale_type.h"
namespace oceanbase {
namespace common {
// Note that the index of the ob_locale_ja_JP is exactly 2 and should be placed in the 2nd position in the ob_locales.
OB_LOCALE *ob_locales[] = {&ob_locale_en_US, NULL, &ob_locale_ja_JP, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, &ob_locale_ko_KR,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            &ob_locale_zh_CN, &ob_locale_zh_TW, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL,
                            NULL, NULL, NULL, NULL};

OB_LOCALE *ob_locale_by_name(const ObString &locale_name) {
  OB_LOCALE ** locale = NULL;
  OB_LOCALE *result_locale = NULL;
  bool has_found = false;
  OB_LOCALE ** locale_limit = ob_locales + LOCALE_COUNT;
  for (locale = ob_locales ; !has_found && locale < locale_limit  ; locale++) {
    if ( *locale != NULL && 0 == locale_name.case_compare((*locale)->name_) ) {
      result_locale = *locale;
      has_found = true;
    }
  }
  if (NULL == result_locale) {
    result_locale = &ob_locale_en_US;
  }
  return result_locale;
}

bool is_valid_ob_locale(const ObString &in_locale_name, ObString &valid_locale_name) {
  OB_LOCALE ** locale = NULL;
  OB_LOCALE *result_locale = NULL;
  bool ret = true;
  bool has_found = false;
  for (locale = ob_locales; !has_found && locale < ob_locales + LOCALE_COUNT; locale++) {
    if (*locale != NULL && 0 == in_locale_name.case_compare((*locale)->name_)) {
      result_locale = *locale;
      valid_locale_name = ObString(result_locale->name_);
      has_found = true;
    }
  }
  if (NULL == result_locale) {
    ret = false;
  }
  return ret;
}

} // common
} // oceanbase
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

#ifndef OCEANBASE_LOCALE_TYPE_H_
#define OCEANBASE_LOCALE_TYPE_H_

#define MONTH_LENGTH 13
#define DAY_LENGTH 8
#define LOCALE_COUNT 111
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"

namespace oceanbase {
namespace common {

typedef struct OB_LOCALE_TYPE {             /* Different types saved here */
  unsigned int count_{0};           /* How many types */
  const char *name_{NULL}; /* Name of typelib */
  const char **type_names_{NULL};
  unsigned int *type_lengths_{NULL};
  OB_LOCALE_TYPE(unsigned int count,
              const char *name,
              const char **type_names,
              unsigned int *type_lengths):count_(count),
                                          name_(name),
                                          type_names_(type_names),
                                          type_lengths_(type_lengths){}
} OB_LOCALE_TYPE;

class OB_LOCALE {
 public:
  unsigned int number_;
  const char *name_;
  const char *description_;
  const bool is_ascii_;
  OB_LOCALE_TYPE *month_names_;
  OB_LOCALE_TYPE *ab_month_names_;
  OB_LOCALE_TYPE *day_names_;
  OB_LOCALE_TYPE *ab_day_names_;
  unsigned int  max_month_name_length_;
  unsigned int  max_day_name_length_;
  unsigned int  decimal_point_;
  unsigned int  thousand_sep_;
  const char *grouping_;
  OB_LOCALE(unsigned int number_par, const char *name_par, const char *descr_par,
            bool is_ascii_par, OB_LOCALE_TYPE *month_names_par,
            OB_LOCALE_TYPE *ab_month_names_par, OB_LOCALE_TYPE *day_names_par,
            OB_LOCALE_TYPE *ab_day_names_par, unsigned int max_month_name_length_par,
            unsigned int max_day_name_length_par, unsigned int decimal_point_par,
            unsigned int thousand_sep_par, const char *grouping_par)
      : number_(number_par),
        name_(name_par),
        description_(descr_par),
        is_ascii_(is_ascii_par),
        month_names_(month_names_par),
        ab_month_names_(ab_month_names_par),
        day_names_(day_names_par),
        ab_day_names_(ab_day_names_par),
        max_month_name_length_(max_month_name_length_par),
        max_day_name_length_(max_day_name_length_par),
        decimal_point_(decimal_point_par),
        thousand_sep_(thousand_sep_par),
        grouping_(grouping_par){}
};

OB_LOCALE *ob_locale_by_name(const ObString &cs_name);
bool is_valid_ob_locale(const ObString &in_locale_name, ObString &valid_locale_name);

} // common
} // oceanbase

#endif /* OCEANBASE_LOCALE_TYPE_ */

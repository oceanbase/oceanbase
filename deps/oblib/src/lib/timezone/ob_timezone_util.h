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

#ifndef OCEANBASE_LIB_FILE_TIMEZONE_UTIL
#define OCEANBASE_LIB_FILE_TIMEZONE_UTIL
#include "lib/string/ob_string.h"
#include <stdio.h>


namespace oceanbase
{
namespace common
{

typedef int64_t my_time_t;

struct tzhead {
 	unsigned char tzh_magic[4];		/* TZ_MAGIC */
	unsigned char	tzh_reserved[16];	/* reserved for future use */
	unsigned char	tzh_ttisgmtcnt[4];	/* coded number of trans. time flags */
	unsigned char	tzh_ttisstdcnt[4];	/* coded number of trans. time flags */
	unsigned char	tzh_leapcnt[4];		/* coded number of leap seconds */
	unsigned char	tzh_timecnt[4];		/* coded number of transition times */
	unsigned char	tzh_typecnt[4];		/* coded number of local time types */
	unsigned char	tzh_charcnt[4];		/* coded number of abbr. chars */
};

typedef struct ls_info
{
  my_time_t ls_trans;
  int64_t      ls_corr;
} LS_INFO;

typedef struct trantypeinfo
{
  int64_t tt_gmtoff;
  uint64_t tt_isdst;
  uint64_t tt_abbrind;
} TRAN_TYPE_INFO;

typedef struct revtinfo
{
  int64_t rt_offset;
  uint64_t rt_type;
} REVT_INFO;

typedef struct st_time_zone_info
{
  uint64_t leapcnt;
  uint64_t timecnt;
  uint64_t typecnt;
  uint64_t charcnt;
  uint64_t revcnt;
  my_time_t *ats;
  unsigned char	*types;
  TRAN_TYPE_INFO *ttis;
  char *chars;
  LS_INFO *lsis;
  my_time_t *revts;
  REVT_INFO *revtis;
  TRAN_TYPE_INFO *fallback_tti;

} TIME_ZONE_INFO;

class ObTimezoneUtils
{
public:
  ObTimezoneUtils();
  virtual ~ObTimezoneUtils();

  bool is_inited();
  int init(const ObString& timezone_file_name);
  void destroy();
  const TIME_ZONE_INFO *get_tz_ptr(){return &_tz_info;}
  virtual int parse_timezone_file(const ObString& timezone_file_name);
  virtual int print_tz_to_sql(const char* tz_name);

private:
  ObTimezoneUtils(const ObTimezoneUtils &other);
  ObTimezoneUtils &operator = (const ObTimezoneUtils& other);

  static int make_ftype(register char * to, register int flag);
  static int prepare_tz_info(TIME_ZONE_INFO &sp);
  FILE *_fd;
  TIME_ZONE_INFO _tz_info;
  bool _is_inited_;
};

} // end of common
}// end of namespace

#endif //OCEANBASE_LIB_FILE_TIMEZONE_UTIL

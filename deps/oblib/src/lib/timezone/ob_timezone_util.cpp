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

#include <fcntl.h>
#include <stdint.h>
#include "lib/allocator/ob_malloc.h"
#include "lib/timezone/ob_timezone_util.h"
#include "lib/oblog/ob_log.h"

#define O_BINARY  0 /* Flag to my_open for binary files */
#define FILE_BINARY O_BINARY

#define TZ_MAX_TIMES  370
#define TZ_MAX_TYPES  20  /* Maximum number of local time types */
#define TZ_MAX_LEAPS  50  /* Maximum number of leap second corrections */
#define TZ_MAX_CHARS  50
#define MY_ALIGN(A,L) (((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A) MY_ALIGN((A),sizeof(double))
#define TZ_MAX_TIMES  370
#define MY_TZNAME_MAX 255
#define TZ_MAX_REV_RANGES (TZ_MAX_TIMES + TZ_MAX_LEAPS + 2)
#define MY_MAX(a, b)  ((a) > (b) ? (a) : (b))

//
#define uint unsigned int
#define uchar unsigned char
#define uint32 unsigned int
#define int32 int
/*
  Macro for reading 32-bit integer from network byte order (big-endian)
  from a unaligned memory location.
*/
#define int4net(A)        (int32) (((uint32) ((uchar) (A)[3]))        | \
                                  (((uint32) ((uchar) (A)[2])) << 8)  | \
                                  (((uint32) ((uchar) (A)[1])) << 16) | \
                                  (((uint32) ((uchar) (A)[0])) << 24))

namespace oceanbase
{
namespace common
{

ObTimezoneUtils::ObTimezoneUtils():
_fd(NULL), _is_inited_(false)
{
  memset(&_tz_info, 0 ,sizeof(_tz_info));
}
ObTimezoneUtils::~ObTimezoneUtils()
{
  if (_is_inited_) {
    destroy();
  }
}

int ObTimezoneUtils::make_ftype(register char * to, register int flag)
{
  int ret = OB_SUCCESS;
  /* check some possible invalid combinations */
  OB_ASSERT(NULL != to);
  OB_ASSERT((flag & (O_TRUNC | O_APPEND)) != (O_TRUNC | O_APPEND));
  OB_ASSERT((flag & (O_WRONLY | O_RDWR)) != (O_WRONLY | O_RDWR));
  if ((flag & (O_RDONLY|O_WRONLY)) == O_WRONLY)
    *to++= (flag & O_APPEND) ? 'a' : 'w';
  else if (0 != (flag & O_RDWR))
  {
    /* Add '+' after theese */
    if (0 != (flag & (O_TRUNC | O_CREAT)))
      *to++= 'w';
    else if (0 != (flag & O_APPEND))
      *to++= 'a';
    else
      *to++= 'r';
    *to++= '+';
  }
  else
    *to++= 'r';

  if (0 != (flag & FILE_BINARY))
    *to++ = 'b';
  *to = '\0';

  return ret;
} /* make_ftype */

bool ObTimezoneUtils::is_inited()
{
  return _is_inited_;
}

int ObTimezoneUtils::init(const ObString& timezone_file_name)
{
  int ret = OB_SUCCESS;
  if(_is_inited_){

  }else{
    memset(&_tz_info, 0, sizeof(_tz_info));
    if(OB_SUCCESS == parse_timezone_file(timezone_file_name)){
    } else {
      destroy();
    }
  }
  return ret;
}

void ObTimezoneUtils::destroy()
{
  if(NULL != _tz_info.ats){
    ob_free(_tz_info.ats);
    _tz_info.ats = NULL;
    _tz_info.types = NULL;
    _tz_info.ttis = NULL;
    _tz_info.chars = NULL;
    _tz_info.lsis = NULL;
  }
  if(NULL != _tz_info.revts){
    ob_free(_tz_info.revts);
    _tz_info.revts = NULL;
  }
  if(NULL != _tz_info.revtis){
    ob_free(_tz_info.revtis);
    _tz_info.revtis = NULL;
  }
  /*
  if(NULL != _tz_info.fallback_tti){
    ob_free(_tz_info.fallback_tti);
    _tz_info.fallback_tti = NULL;
  }
  */
}

// This function is bug prone. Fix it if you want to use it.
int ObTimezoneUtils::parse_timezone_file(const ObString& timezone_file_name)
{
  int ret = OB_SUCCESS;
  if(_is_inited_){
    destroy();
  }
  unsigned char *p = NULL;
  char type[15];
  TIME_ZONE_INFO &sp = _tz_info;
  make_ftype(type, O_RDONLY|O_BINARY);
  _fd = fopen(timezone_file_name.ptr(), type);
  if(0 == _fd){
    ret = OB_IO_ERROR;
    OB_LOG(ERROR, "cannot open the time zone file");
  } else {
    union{
      struct tzhead tzhead;
      unsigned char buf[sizeof(struct tzhead)+ sizeof(my_time_t) * TZ_MAX_TIMES +
        TZ_MAX_TIMES + sizeof(TRAN_TYPE_INFO) * TZ_MAX_TYPES +
        MY_MAX(TZ_MAX_CHARS + 1, (2 * (MY_TZNAME_MAX + 1))) +
        sizeof(LS_INFO) * TZ_MAX_LEAPS];
    }u;

    uint64_t ttisstdcnt;
    uint64_t ttisgmtcnt;
    char *tzinfo_buf = NULL;
    int64_t Count = sizeof(u.buf);
    //_OB_LOG(INFO, "the size of current timezone buffer is %ld", Count);
    int64_t readbytes = 0;
    if((readbytes = fread(u.buf, sizeof(char), Count, _fd)) != Count){
      //_OB_LOG(INFO, "the time info file size is %ld, but the Count size is %ld", readbytes, Count);
    }
    if(0 != fclose(_fd)){
       ret = OB_IO_ERROR;
       OB_LOG(ERROR, "cannot close the open time zone file");
    } else {
      if(readbytes < static_cast<int64_t>(sizeof(struct tzhead))){
        ret = OB_IO_ERROR;
        OB_LOG(ERROR, "time zone file format is not correct");
      } else {
        ttisstdcnt = int4net(u.tzhead.tzh_ttisgmtcnt);
        ttisgmtcnt = int4net(u.tzhead.tzh_ttisstdcnt);
        sp.leapcnt= int4net(u.tzhead.tzh_leapcnt);
        //_OB_LOG(DEBUG, "the leap second cnt is %lu", sp.leapcnt);
        sp.timecnt= int4net(u.tzhead.tzh_timecnt);
        sp.typecnt= int4net(u.tzhead.tzh_typecnt);
        sp.charcnt= int4net(u.tzhead.tzh_charcnt);
        p = u.tzhead.tzh_charcnt + sizeof(u.tzhead.tzh_charcnt);
        if (sp.leapcnt > TZ_MAX_LEAPS ||
        sp.typecnt == 0 || sp.typecnt > TZ_MAX_TYPES ||
        sp.timecnt > TZ_MAX_TIMES ||
        sp.charcnt > TZ_MAX_CHARS ||
        (ttisstdcnt != sp.typecnt && ttisstdcnt != 0) ||
        (ttisgmtcnt != sp.typecnt && ttisgmtcnt != 0)){
          ret = OB_ERROR;
          OB_LOG(ERROR, "time zone file infomation error, please check");
        } else {
          if ((uint)(readbytes - (p - u.buf)) <
            sp.timecnt * 4 +                       /* ats */
            sp.timecnt +                           /* types */
            sp.typecnt * (4 + 2) +                 /* ttinfos */
            sp.charcnt +                           /* chars */
            sp.leapcnt * (4 + 4) +                 /* lsinfos */
            ttisstdcnt +                            /* ttisstds */
            ttisgmtcnt) {
            ret = OB_ERROR;
            OB_LOG(ERROR, "time zone file infomation error, please check");
          } else {
            int64_t bufSize = ALIGN_SIZE(sp.timecnt * sizeof(my_time_t))
                + ALIGN_SIZE(sp.timecnt) +
                  ALIGN_SIZE(sp.typecnt * sizeof(TRAN_TYPE_INFO)) +
                  ALIGN_SIZE(sp.charcnt) +
                  sp.leapcnt * sizeof(LS_INFO);
            tzinfo_buf = (char *)ob_malloc(bufSize, "TimeZoneUtils");
            if(NULL == tzinfo_buf){
              ret = OB_ALLOCATE_MEMORY_FAILED;
            } else {
              sp.ats= (my_time_t *)tzinfo_buf;
              tzinfo_buf += ALIGN_SIZE(sp.timecnt * sizeof(my_time_t));
              sp.types= (unsigned char *)tzinfo_buf;
              tzinfo_buf += ALIGN_SIZE(sp.timecnt);
              sp.ttis= (TRAN_TYPE_INFO *)tzinfo_buf;
              tzinfo_buf += ALIGN_SIZE(sp.typecnt * sizeof(TRAN_TYPE_INFO));
              sp.chars = tzinfo_buf;
              tzinfo_buf += ALIGN_SIZE(sp.charcnt);
              sp.lsis = NULL; // we do not process the leap seconds now
              //sp.lsis = (LS_INFO *)tzinfo_buf;
              for (uint i = 0; i < sp.timecnt; i++, p+= 4)
                sp.ats[i] = int4net(p);
              for (uint i = 0; i < sp.timecnt; i++){
                sp.types[i] = (uchar) *p++;
                if (sp.types[i] >= sp.typecnt){
                  OB_LOG(ERROR, "error time zone info types typecnt value not correct");
                  ret = OB_ERROR;
                  break;
                }
              }
              if(OB_SUCC(ret)){
                for (uint i = 0; i < sp.typecnt; i++){
                  TRAN_TYPE_INFO * ttisp = NULL;
                  ttisp = &sp.ttis[i];
                  ttisp->tt_gmtoff = int4net(p);
                  p += 4;
                  ttisp->tt_isdst= (unsigned char) *p++;
                  if (ttisp->tt_isdst != 0 && ttisp->tt_isdst != 1){
                    ret = OB_ERROR;
                    OB_LOG(ERROR, "zone info error tt_isdst value not correct");
                    break;
                  }
                  ttisp->tt_abbrind= (unsigned char) *p++;
                  if(ttisp->tt_abbrind > sp.charcnt){
                    ret = OB_ERROR;
                    OB_LOG(ERROR, "zone info error tt_abbrind value not correct");
                    break;
                  }
                }
              }
              if(OB_SUCC(ret)){
                uint i;
                for (i = 0; i < sp.charcnt; i++)
                  sp.chars[i]= *p++;
                // FIXME: fufeng, this ending char is overflow
                sp.chars[i] = '\0';
                for (i = 0; i < sp.leapcnt; i++){
                  LS_INFO *lsisp;
                  // FIXME: fufeng, sp.lsis is NULL
                  lsisp= &sp.lsis[i];
                  lsisp->ls_trans = int4net(p);
                  p += 4;
                  lsisp->ls_corr = int4net(p);
                  p += 4;
                }
                if(OB_SUCCESS != prepare_tz_info(sp)){
                  ret = OB_ERROR;
                  OB_LOG(ERROR, "prepare_tz_info failed");
                }
              }
            }
          }
        }
      }
    }
  }

  if(OB_SUCC(ret)) {
    _is_inited_ = true;
  }

  return ret;
}



template <class T>
inline static const T& min(const T& a, const T& b)
{
    return a < b ? a : b;
}

int ObTimezoneUtils::prepare_tz_info(TIME_ZONE_INFO &tz_info)
{
  int ret = OB_SUCCESS;
  my_time_t cur_time = INT32_MIN;
  my_time_t cur_l, end_time, end_l;
  my_time_t cur_max_seen_l= INT32_MIN;
  int64_t cur_offset, cur_corr, cur_off_and_corr;
  uint64_t next_trans_idx, next_leap_idx;
  uint64_t i;

  my_time_t revts[TZ_MAX_REV_RANGES];
  REVT_INFO revtis[TZ_MAX_REV_RANGES];
  for (i = 0; i < tz_info.typecnt && tz_info.ttis[i].tt_isdst; i++)
    /* no-op */ ;
  if (i == tz_info.typecnt)
      i = 0;
  tz_info.fallback_tti= &(tz_info.ttis[i]);

  tz_info.revcnt = 0;
  if (tz_info.timecnt == 0 || cur_time < tz_info.ats[0]) {
    next_trans_idx = 0;
  } else {
    i = tz_info.types[0];
    next_trans_idx = 1;
  }

  cur_offset = tz_info.ttis[i].tt_gmtoff;

  /* let us find leap correction... unprobable, but... */
  for (next_leap_idx = 0; next_leap_idx < tz_info.leapcnt &&
         cur_time >= tz_info.lsis[next_leap_idx].ls_trans;
         ++next_leap_idx)
    continue;
  if (next_leap_idx > 0)
    cur_corr= tz_info.lsis[next_leap_idx - 1].ls_corr;
  else
    cur_corr= 0;

  // Iterate
  while (tz_info.revcnt < TZ_MAX_REV_RANGES - 1)
  {
    cur_off_and_corr = cur_offset - cur_corr;
    if (cur_off_and_corr < 0 &&
        cur_time < INT32_MIN - cur_off_and_corr)
      cur_time= INT32_MIN - cur_off_and_corr;

    cur_l= cur_time + cur_off_and_corr;

    end_time= min((next_trans_idx < tz_info.timecnt) ? tz_info.ats[next_trans_idx] - 1:
                                                INT32_MAX, static_cast<int64_t>(INT32_MAX));

    if (cur_off_and_corr > 0 &&
        end_time > INT32_MAX - cur_off_and_corr) {
      end_time = INT32_MAX;
      end_time -= cur_off_and_corr;
    }

    end_l = end_time;
    end_l += cur_off_and_corr;

    if (cur_max_seen_l < end_l)
    {
      if (INT32_MIN == cur_max_seen_l)
      {
        revtis[tz_info.revcnt].rt_type = 0;
        revts[tz_info.revcnt] = cur_l;
        revtis[tz_info.revcnt].rt_offset = cur_off_and_corr;
        cur_max_seen_l= end_l;
        tz_info.revcnt++;
      }
      else
      {
        if (cur_l - 1 > cur_max_seen_l)
        {
          revtis[tz_info.revcnt].rt_offset= revtis[tz_info.revcnt-1].rt_offset;
          revtis[tz_info.revcnt].rt_type= 1;
          revts[tz_info.revcnt]= cur_max_seen_l + 1;
          tz_info.revcnt++;
          if (tz_info.revcnt - TZ_MAX_LEAPS == TZ_MAX_TIMES + 1)
            break;
          cur_max_seen_l = cur_l;
          cur_max_seen_l--;
        }

        revtis[tz_info.revcnt].rt_offset= cur_off_and_corr;
        revtis[tz_info.revcnt].rt_type= 0;
        revts[tz_info.revcnt]= cur_max_seen_l + 1;
        cur_max_seen_l= end_l;
        tz_info.revcnt++;
      }
    }

    if (((cur_off_and_corr > 0) &&
    (end_time + cur_off_and_corr >= INT32_MAX)) ||
    end_time == INT32_MAX)
      break;

    cur_time= end_time + 1;
    if (tz_info.ats[0] <= cur_time && 0 != tz_info.timecnt)
      if (tz_info.ats[next_trans_idx] == cur_time && tz_info.timecnt > next_trans_idx)
      {
        cur_offset= tz_info.ttis[tz_info.types[next_trans_idx]].tt_gmtoff;
        ++next_trans_idx;
      }

    if (tz_info.lsis[next_leap_idx].ls_trans == cur_time && tz_info.leapcnt > next_leap_idx)
    {
      cur_corr = tz_info.lsis[next_leap_idx].ls_corr;
      ++next_leap_idx;
    }
  }

  if (tz_info.revcnt == TZ_MAX_REV_RANGES - 1){
    OB_LOG(ERROR, "tz_info.revcnt error not enough space");
    ret = OB_ERROR;
  }
  /* set maximum end_l as finisher */
  revts[tz_info.revcnt] = end_l;
  if (OB_FAIL(ret)) {
  } else if(!(tz_info.revts = (my_time_t*)ob_malloc(sizeof(my_time_t) * (tz_info.revcnt + 1), "TimeZoneUtils"))
      || !(tz_info.revtis = (REVT_INFO*)ob_malloc(sizeof(REVT_INFO) * tz_info.revcnt, "TimeZoneUtils"))
    ){
    OB_LOG(ERROR, "ob_malloc for tz_info.revts tz_info.revtis failed");
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    MEMCPY(tz_info.revts, revts, sizeof(my_time_t) * (tz_info.revcnt + 1));
    MEMCPY(tz_info.revtis, revtis, sizeof(REVT_INFO) * tz_info.revcnt);
  }
  return ret;
}

/**
 use this function to build internal timezone table like mysql
 **/
int ObTimezoneUtils::print_tz_to_sql(const char* tz_name)
{
  int ret = OB_SUCCESS;
  TIME_ZONE_INFO &sp = _tz_info;
  uint64_t i = 0;
    /* Here we assume that all time zones have same leap correction tables */
  _OB_LOG(INFO, "INSERT INTO time_zone (Use_leap_seconds) VALUES ('%s');", sp.leapcnt ? "Y" : "N");
  _OB_LOG(INFO, "SET @time_zone_id= LAST_INSERT_ID();");
  _OB_LOG(INFO, "INSERT INTO time_zone_name (Name, Time_zone_id) VALUES ('%s', @time_zone_id);", tz_name);

  if (0 != sp.timecnt)
  {
    _OB_LOG(INFO, "INSERT INTO time_zone_transition (Time_zone_id, Transition_time, Transition_type_id) VALUES");
    for (i= 0; i < sp.timecnt; i++)
      _OB_LOG(INFO, "%s(@time_zone_id, %ld, %u)", (i == 0 ? " " : ","), sp.ats[i], (uint)sp.types[i]);
    _OB_LOG(INFO, ";");
  }

  _OB_LOG(INFO, "INSERT INTO time_zone_transition_type (Time_zone_id, Transition_type_id, Offset, Is_DST, Abbreviation) VALUES");

  for (i= 0; i < sp.typecnt; i++)
    _OB_LOG(INFO, "%s(@time_zone_id, %lu, %ld, %lu, '%s')",
            (i == 0 ? " " : ","), i,
            sp.ttis[i].tt_gmtoff, sp.ttis[i].tt_isdst,
            sp.chars + sp.ttis[i].tt_abbrind);
  _OB_LOG(INFO, ";");

  return ret;
}



} // namespace common


} // oceanbase

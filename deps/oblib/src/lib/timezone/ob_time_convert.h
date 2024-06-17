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

#ifndef OCEANBASE_LIB_TIMEZONE_OB_TIME_CONVERT_
#define OCEANBASE_LIB_TIMEZONE_OB_TIME_CONVERT_

//#include "lib/timezone/ob_timezone_util.h"
//#include "lib/timezone/ob_timezone_info.h"
#include "lib/string/ob_string.h"
#include "lib/ob_date_unit_type.h"
#include "common/object/ob_obj_type.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "common/ob_accuracy.h"
//#include "lib/container/ob_bit_set.h"

namespace oceanbase
{
namespace common
{
class ObTimeZoneInfoPos;
class ObTimeZoneInfo;
class ObOTimestampData;
struct ObTimeConstStr;
class ObDataTypeCastParams;
struct ObIntervalYMValue;
struct ObIntervalDSValue;
struct ObOracleTimeLimiter;
class ObObj;
class ObDFMElem;
class ObDFMFlag;
template <int64_t N>
class ObFixedBitSet;

#define DT_TYPE_DATE        (1UL << 0)
#define DT_TYPE_TIME        (1UL << 1)
#define DT_TYPE_NONE        (1UL << 2)  // like MYSQL_TIMESTAMP_NONE, set when DT_TYPE_DATETIME is set
                                        // and the string is not in a good format, such as has delimiters
                                        // but has no space in delimiters.
#define DT_MODE_DST_GAP     (1UL << 3)
#define DT_MODE_NEG         (1UL << 4)
#define DT_WEEK_SUN_BEGIN   (1UL << 5)  // sunday is the first day of week, otherwise monday.
#define DT_WEEK_ZERO_BEGIN  (1UL << 6)  // week num will begin with 0, otherwise 1.
#define DT_WEEK_GE_4_BEGIN  (1UL << 7)  // week which has 4 or more days is week 1, otherwise has
                                        // the first sunday of monday.

#define DT_TYPE_ORACLE      (1UL << 8)  //oracle timestamp to nanosecond (nano, tz, ltz)
#define DT_TYPE_STORE_UTC   (1UL << 9)  //store utc  (tz, ltz)
#define DT_TYPE_TIMEZONE    (1UL << 10) //oracle timestamp with time zone (tz)

typedef uint64_t ObDTMode;

#define DT_TYPE_DATETIME          (DT_TYPE_DATE | DT_TYPE_TIME)
#define DT_TYPE_ORACLE_TIMESTAMP  (DT_TYPE_DATETIME | DT_TYPE_ORACLE)
#define DT_TYPE_ORACLE_TTZ        (DT_TYPE_DATETIME | DT_TYPE_ORACLE | DT_TYPE_TIMEZONE)
#define DT_TYPE_CNT               (3)

#define HAS_TYPE_DATE(mode)     (DT_TYPE_DATE & (mode))
#define HAS_TYPE_TIME(mode)     (DT_TYPE_TIME & (mode))
#define IS_TYPE_DATE(mode)      (DT_TYPE_DATE == (mode))
#define IS_TYPE_TIME(mode)      (DT_TYPE_TIME == (mode))
#define IS_TYPE_DATETIME(mode)  (DT_TYPE_DATETIME == (mode))
#define IS_NEG_TIME(mode)       (DT_MODE_NEG & (mode))
#define IS_SUN_BEGIN(mode)      ((DT_WEEK_SUN_BEGIN & (mode)) ? 1 : 0)
#define IS_ZERO_BEGIN(mode)     ((DT_WEEK_ZERO_BEGIN & (mode)) ? 1 : 0)
#define IS_GE_4_BEGIN(mode)     ((DT_WEEK_GE_4_BEGIN & (mode)) ? 1 : 0)
#define HAS_TYPE_ORACLE(mode)   ((DT_TYPE_ORACLE & (mode)) ? 1 : 0)
#define HAS_TYPE_TIMEZONE(mode) ((DT_TYPE_TIMEZONE & (mode)) ? 1 : 0)
#define HAS_TYPE_STORE_UTC(mode) ((DT_TYPE_STORE_UTC & (mode)) ? 1 : 0)

#define DATE_PART_CNT   3
#define TIME_PART_CNT   4
#define OTHER_PART_CNT  4
#define DATETIME_PART_CNT     (DATE_PART_CNT + TIME_PART_CNT)
#define ORACLE_DATE_PART_CNT  (DATE_PART_CNT + TIME_PART_CNT - 1)
#define TOTAL_PART_CNT        (DATETIME_PART_CNT + OTHER_PART_CNT)
#define DT_YEAR   0
#define DT_MON    1
#define DT_MDAY   2

#define DT_HOUR   3
#define DT_MIN    4
#define DT_SEC    5
#define DT_USEC   6

#define DT_DATE   7
#define DT_YDAY   8
#define DT_WDAY   9
#define DT_OFFSET_MIN     10

#define DT_MON_NAME       11  // monthname doesn't contains real data by using month directly
                              // put it after DT_OFFSET_MIN will be fine

extern const int64_t DT_PART_BASE[DATETIME_PART_CNT];
extern const int64_t DT_PART_MIN[DATETIME_PART_CNT];
extern const int64_t DT_PART_MAX[DATETIME_PART_CNT];

extern const int64_t TZ_PART_BASE[DATETIME_PART_CNT];
extern const int64_t TZ_PART_MIN[DATETIME_PART_CNT];
extern const int64_t TZ_PART_MAX[DATETIME_PART_CNT];
extern const int     TZ_PART_ERR[DATETIME_PART_CNT];

#define MONS_PER_YEAR   DT_PART_BASE[DT_MON]
#define HOURS_PER_DAY   DT_PART_BASE[DT_HOUR]
#define MINS_PER_HOUR   DT_PART_BASE[DT_MIN]
#define SECS_PER_MIN    DT_PART_BASE[DT_SEC]
#define USECS_PER_SEC   DT_PART_BASE[DT_USEC]
#define NSECS_PER_SEC   1000000000LL
#define NSECS_PER_USEC   1000LL
#define MSECS_PER_SEC   1000LL
#define USECS_PER_MSEC  1000LL
#define MONS_PER_QUAR   3
#define DAYS_PER_WEEK   7
#define DAYS_PER_NYEAR  365
#define DAYS_PER_LYEAR  366
#define YEARS_PER_CENTURY 100
#define MONTHS_PER_YEAR 12
//in order to optimized perf
//the following literals are defined by const not macro since
//they will be used many times.
extern const int64_t SECS_PER_HOUR;
extern const int64_t SECS_PER_DAY;
extern const int64_t USECS_PER_DAY;
extern const int64_t NSECS_PER_DAY;
extern const int64_t USECS_PER_MIN;
// days from 0000-00-00 to 1970-01-01
#define DAYS_FROM_ZERO_TO_BASE 719528
#define MAX_DAYS_OF_DATE 3652424
#define MIN_DAYS_OF_DATE 366
#define TIMESTAMP_MIN_LENGTH 19
#define DATETIME_MIN_LENGTH 19
#define DATETIME_MAX_LENGTH 26
#define TIME_MIN_LENGTH 10
#define DATE_MIN_LENGTH 10
#define DAYNAME_MAX_LENGTH 9 //Wednesday is longest
#define MONTHNAME_MAX_LENGTH 9 //September is longest
//max timestamp最大值为253402272000 12位
#define TIMESTAMP_VALUE_LENGTH 12
#define SEC_TO_USEC(secs)   ((secs) * USECS_PER_SEC)
#define USEC_TO_SEC(usec)   ((usec) / USECS_PER_SEC)
#define NSEC_TO_SEC(nsec)   ((nsec) / NSECS_PER_SEC)
#define SEC_TO_MIN(secs)    ((secs) / SECS_PER_MIN)
#define MIN_TO_USEC(min)    ((min) * SECS_PER_MIN * USECS_PER_SEC)
#define TIMESTAMP_MAX_VAL   253402272000
#define DATETIME_MAX_VAL    253402300799999999
#define DATE_MAX_VAL        2932896
#define DATETIME_MIN_VAL    -62167132800000000
#define MYSQL_TIMESTAMP_MAX_VAL 253402214399999999
#define MYSQL_TIMESTAMP_MIN_VAL -62167046400000000
#define ORACLE_DATETIME_MIN_VAL -62135596800000000 //start from '0001-1-1 00:00:00'
#define TIME_MAX_HOUR 838
#define TIME_MAX_VAL (3020399 * 1000000LL)    // 838:59:59.

struct ObIntervalLimit {
  static const ObOracleTimeLimiter YEAR;
  static const ObOracleTimeLimiter MONTH;
  static const ObOracleTimeLimiter DAY;
  static const ObOracleTimeLimiter HOUR;
  static const ObOracleTimeLimiter MINUTE;
  static const ObOracleTimeLimiter SECOND;
  static const ObOracleTimeLimiter FRACTIONAL_SECOND;
};

struct ObDateSqlMode {
  union {
    uint64_t date_sql_mode_;
    struct {
      uint64_t allow_invalid_dates_:1;
      uint64_t no_zero_date_:1;
      uint64_t no_zero_in_date_:1;
      // For dayofmonth, year, month, day allow incomplete dates such as '2001-11-00', and not
      // affected by sqlmode NO_ZERO_IN_DATE, you can learn more from the below link by searching
      // the key words "SELECT DAYOFMONTH('2001-11-00'), MONTH('2005-00-00');"
      // https://dev.mysql.com/doc/refman/8.0/en/date-and-time-functions.html
      uint64_t allow_incomplete_dates_:1;
      uint64_t reserved_:28;
    };
  };
  ObDateSqlMode() : date_sql_mode_(0) {};
  ObDateSqlMode(const int64_t date_sql_mode) {
    allow_invalid_dates_ = date_sql_mode & (1ULL << 0);
    no_zero_date_ = date_sql_mode & (1ULL << 1);
    // no_zero_in_date_ = date_sql_mode & (1ULL << 2);
  };
  void init(const ObSQLMode sql_mode) {
    allow_invalid_dates_ = (bool)(SMO_ALLOW_INVALID_DATES & sql_mode);
    no_zero_date_ = (bool)(SMO_NO_ZERO_DATE & sql_mode);
    no_zero_in_date_ = (bool)(SMO_NO_ZERO_IN_DATE & sql_mode);
  }
  TO_STRING_KV(K_(allow_invalid_dates), K_(no_zero_date), K_(allow_incomplete_dates), K_(no_zero_in_date));
};

class ObIntervalParts
{
public:
  ObIntervalParts() {
    reset();
  }
  ~ObIntervalParts() {}
  inline void reset() {
    is_negtive_ = false;
    memset(parts_, 0, sizeof(parts_));
  }
  enum PartName{ INVALID_PART = -1,
                 YEAR_PART = 0,
                 MONTH_PART,
                 DAY_PART,
                 HOUR_PART,
                 MINUTE_PART,
                 SECOND_PART,
                 FRECTIONAL_SECOND_PART,
                 PART_CNT };
  inline int32_t get_part_value(PartName part_name)
  {
    return (part_name < PART_CNT && part_name > INVALID_PART) ?
          (parts_[part_name] * (is_negtive_ ? -1 : 1)) : INT32_MAX;
  }
  bool is_negtive_;
  int32_t parts_[PART_CNT];
};

class ObTime
{
public:
  ObTime()
      : mode_(0),
      time_zone_id_(common::OB_INVALID_INDEX),
      transition_type_id_(common::OB_INVALID_INDEX),
      is_tz_name_valid_(false)
  {
    MEMSET(parts_, 0, sizeof(parts_));
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
    MEMSET(tzd_abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
  }
  explicit ObTime(ObDTMode mode)
      : mode_(mode),
      time_zone_id_(common::OB_INVALID_INDEX),
      transition_type_id_(common::OB_INVALID_INDEX),
      is_tz_name_valid_(false)
  {
    MEMSET(parts_, 0, sizeof(parts_));
    MEMSET(tz_name_, 0, common::OB_MAX_TZ_NAME_LEN);
    MEMSET(tzd_abbr_, 0, common::OB_MAX_TZ_ABBR_LEN);
  }
  ~ObTime() {}
  ObString get_tz_name_str() const
  {
    return ObString(strlen(tz_name_), tz_name_);
  }
  ObString get_tzd_abbr_str() const
  {
    return ObString(strlen(tzd_abbr_), tzd_abbr_);
  }
  int set_tz_name(const ObString &tz_name);
  int set_tzd_abbr(const ObString &tz_abbr);
  DECLARE_TO_STRING;
  ObDTMode  mode_;
  int32_t   parts_[TOTAL_PART_CNT];
  // year:    [1000, 9999].
  // month:   [1, 12].
  // day:     [1, 31].
  // hour:    [0, 23] or [0, 838] if it is a time.
  // minute:  [0, 59].
  // second:  [0, 59].
  // usecond: [0, 1000000], 1000000 can only valid after str_to_ob_time, for round.
    // nanosecond: [0, 1000000000], when HAS_TYPE_ORACLE(mode_)
  // date: date value, day count since 1970-1-1.
  // year day: [1, 366].
  // week day: [1, 7], 1 means monday, 7 means sunday.
  // offset minute:  [-12*60, 14*60].

  char tz_name_[common::OB_MAX_TZ_NAME_LEN];
  char tzd_abbr_[common::OB_MAX_TZ_ABBR_LEN];//the abbr of time zone region with Daylight Saving Time
  int32_t time_zone_id_;
  int32_t transition_type_id_;
  bool is_tz_name_valid_;
};

typedef ObTime ObInterval;

struct ObTimeConstStr {
  ObTimeConstStr() = delete;
  ObTimeConstStr(const char *str) : ptr_(str), len_(static_cast<int32_t>(strlen(str))) {}
  ObTimeConstStr(const char *str, int32_t len) : ptr_(str), len_(len) {}
  inline ObString to_obstring() const
  {
    return ObString(len_, ptr_);
  }
  const char *ptr_;
  int32_t len_;
  TO_STRING_KV("value", ObString(len_, ptr_), K_(len));
};

struct ObTimeConvertCtx
{
  ObTimeConvertCtx(const ObTimeZoneInfo *tz_info, const bool is_timestamp, const bool &need_truncate = false)
     :tz_info_(tz_info),
      oracle_nls_format_(),
      is_timestamp_(is_timestamp),
      need_truncate_(need_truncate) {}
  ObTimeConvertCtx(const ObTimeZoneInfo *tz_info, const ObString &oracle_nls_format, const bool is_timestamp)
     :tz_info_(tz_info),
      oracle_nls_format_(oracle_nls_format),
      is_timestamp_(is_timestamp) {}
  const ObTimeZoneInfo *tz_info_;
  ObString oracle_nls_format_;
  bool is_timestamp_; //means mysql timestamp?
  bool need_truncate_;
};
class ObTimeConverter
{
public:
  // ZERO_DATETIME is the minimal value that satisfied: 0 == value % USECS_PER_DAY.
  static const int64_t ZERO_DATETIME = static_cast<int64_t>(-9223372022400000000);

  // ZERO_DATE is ZERO_DATETIME / USECS_PER_DAY
  static const int32_t ZERO_DATE = static_cast<int32_t>(-106751991);
  static const int64_t ZERO_TIME = 0;
  static const uint8_t ZERO_YEAR = 0;
  static const ObString DEFAULT_NLS_DATE_FORMAT;
  static const ObString DEFAULT_NLS_TIMESTAMP_FORMAT;
  static const ObString DEFAULT_NLS_TIMESTAMP_TZ_FORMAT;
  static const ObString COMPAT_OLD_NLS_DATE_FORMAT;
  static const ObString COMPAT_OLD_NLS_TIMESTAMP_FORMAT;
  static const ObString COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT;
private:
  struct ObYearWeekWdayElems {
    enum ElemSetState
    {
      NOT_SET = 0,
      UPPER_SET = 1,
      LOWER_SET = 2
    };

    ObYearWeekWdayElems() :
      year_set_state_(NOT_SET), year_value_(INT32_MAX),
      week_set_state_(NOT_SET), week_u_set_(true), week_value_(INT32_MAX),
      weekday_set_(false), weekday_value_(INT32_MAX)
    {}
    bool is_year_set() const { return NOT_SET != year_set_state_; }
    bool is_week_u_set() const { return NOT_SET != week_set_state_ && week_u_set_; }
    bool is_week_v_set() const { return NOT_SET != week_set_state_ && !week_u_set_; }
    bool is_upper_week_set() const {return UPPER_SET == week_set_state_; }
    bool is_weekday_set() const { return weekday_set_; }

    VIRTUAL_TO_STRING_KV(K(year_set_state_), K(year_value_), K(week_set_state_), K(week_u_set_),
        K(week_value_), K(weekday_set_), K(weekday_value_));
    ElemSetState year_set_state_;
    int32_t year_value_;

    ElemSetState week_set_state_;
    bool week_u_set_;   // week is set -> true: %u is set. false: %v is set.
    int32_t week_value_;

    bool weekday_set_;
    int32_t weekday_value_;
  };
public:
  // int / double / string -> datetime(timestamp) / interval / date / time / year.
  static int int_to_datetime(int64_t int_part, int64_t dec_part, const ObTimeConvertCtx &cvrt_ctx,
                             int64_t &value, const ObDateSqlMode date_sql_mode);
  static int int_to_date(int64_t int64, int32_t &value, const ObDateSqlMode date_sql_mode);
  static int int_to_time(int64_t int64, int64_t &value);
  static int int_to_year(int64_t int64, uint8_t &value);
  static int literal_date_validate_oracle(const ObString &str, const ObTimeConvertCtx &cvrt_ctx, ObDateTime &value);
  static int literal_timestamp_validate_oracle(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                                               ObObjType &obj_type, ObOTimestampData &value);
  static int literal_interval_ym_validate_oracle(const ObString &str, ObIntervalYMValue &value, ObScale &scale,
                                                 ObDateUnitType part_begin, ObDateUnitType part_end);
  static int literal_interval_ds_validate_oracle(const ObString &str, ObIntervalDSValue &value, ObScale &scale,
                                                 ObDateUnitType part_begin, ObDateUnitType part_end);
  static int str_to_datetime(const ObString &str, const ObTimeConvertCtx &cvrt_ctx, int64_t &value,
                             int16_t *scale = NULL, const ObDateSqlMode date_sql_mode = 0);
  static int str_to_date_oracle(const ObString &str, const ObTimeConvertCtx &cvrt_ctx, ObDateTime &value);
  static int str_to_datetime_format(const ObString &str, const ObString &fmt,
                                    const ObTimeConvertCtx &cvrt_ctx, int64_t &value,
                                    int16_t *scale, const ObDateSqlMode date_sql_mode);
  static int str_to_otimestamp(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                               const ObObjType target_type, ObOTimestampData &value,
                               ObScale &scale);
  /*
   * sys_tz_info: timezone info of sysvariable SYS_VAR_SYSTEM_TIME_ZONE
   */
  static int datetime_to_scn_value(const int64_t datetime_value,
                                   const ObTimeZoneInfo *sys_tz_info,
                                   uint64_t &scn_value);
  /*
   * sys_tz_info: timezone info of sysvariable SYS_VAR_SYSTEM_TIME_ZONE
   */
  static int otimestamp_to_scn_value(const ObOTimestampData &otimestamp_value,
                                     const ObTimeZoneInfo *sys_tz_info,
                                     uint64_t &scn_value);
  /*
   * sys_tz_info: timezone info of sysvariable SYS_VAR_SYSTEM_TIME_ZONE
   */
  static int str_to_scn_value(const ObString &str,
                              const ObTimeZoneInfo *sys_tz_info,
                              const ObTimeZoneInfo *session_tz_info,
                              const ObString &nlf_format,
                              const bool is_oracle_mode,
                              uint64_t &scn_value);
  //convert a scn to timestamp str with ns
  //invoker need to gurantee scn_val is valid
  static int scn_to_str(const uint64_t scn_val,
                        const ObTimeZoneInfo *sys_tz_info,
                        char *buf, int64_t buf_len, int64_t &pos);
  //ob_time store local time
  static int str_to_tz_offset(const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time);
  //ob_time store local time
  static int calc_tz_offset_by_tz_name(const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time);
  static int ob_time_to_utc(const ObObjType obj_type, const ObTimeConvertCtx &cvrt_ctx, ObTime &ob_time);

  static int str_is_date_format(const ObString &str, bool &date_flag);
  static int str_to_date(const ObString &str, int32_t &value, const ObDateSqlMode date_sql_mode = 0);
  static int str_to_time(const ObString &str, int64_t &value, int16_t *scale = NULL, const bool &need_truncate = false);
  static int str_to_year(const ObString &str, uint8_t &value);
  static int str_to_interval(const ObString &str, ObDateUnitType unit_type, int64_t &value);
  // int / double / string <- datetime(timestamp) / date / time / year.
  static int datetime_to_int(int64_t value, const ObTimeZoneInfo *tz_info, int64_t &int64);
  static int datetime_to_double(int64_t value, const ObTimeZoneInfo *tz_info, double &dbl);
  static int datetime_to_str(int64_t value, const ObTimeZoneInfo *tz_info, const ObString &nls_format,
                             int16_t scale, char *buf, int64_t buf_len, int64_t &pos, bool with_delim = true);
  static int otimestamp_to_str(const ObOTimestampData &value, const ObDataTypeCastParams &dtc_params,
                               const int16_t scale, const ObObjType type, char *buf,
                               int64_t buf_len, int64_t &pos);
  static int date_to_int(int32_t value, int64_t &int64);
  static int date_to_str(int32_t value, char *buf, int64_t buf_len, int64_t &pos);
  static int time_to_int(int64_t value, int64_t &int64);
  static int time_to_double(int64_t value, double &dbl);
  static int time_to_str(int64_t value, int16_t scale,
                         char *buf, int64_t buf_len, int64_t &pos, bool with_delim = true);
  static int time_to_datetime(int64_t t_value, int64_t cur_dt_value,
                              const ObTimeZoneInfo *tz_info, int64_t &dt_value, const ObObjType expect_type);
  static int year_to_int(uint8_t value, int64_t &int64);
  static int year_to_str(uint8_t value, char *buf, int64_t buf_len, int64_t &pos);
  // inner cast between datetime, timestamp, date, time, year.
  static int datetime_to_timestamp(int64_t dt_value, const ObTimeZoneInfo *tz_info, int64_t &ts_value);
  static int timestamp_to_datetime(int64_t ts_value, const ObTimeZoneInfo *tz_info, int64_t &dt_value);
  static inline void datetime_to_odate(int64_t dt_value, int64_t &odate_value) { odate_value = dt_value; }
  static int odate_to_otimestamp(int64_t in_value_us, const ObTimeZoneInfo *tz_info, const ObObjType out_type,
                                 ObOTimestampData &out_value);
  static int otimestamp_to_odate(const ObObjType in_type, const ObOTimestampData &in_value,
                                       const ObTimeZoneInfo *tz_info, int64_t &out_value_us);
  static int otimestamp_to_otimestamp(const ObObjType in_type, const ObOTimestampData &in_value,
                                      const ObTimeZoneInfo *tz_info, const ObObjType out_type,
                                      ObOTimestampData &out_value);
  static int extract_offset_from_otimestamp(const ObOTimestampData &in_value, const ObTimeZoneInfo *tz_info,
                                            int32_t &offset_min, ObTime &ob_time);
  static int datetime_to_date(int64_t dt_value, const ObTimeZoneInfo *tz_info, int32_t &d_value);
  static int datetime_to_time(int64_t dt_value, const ObTimeZoneInfo *tz_info, int64_t &t_value);
  static int datetime_to_year(int64_t dt_value, const ObTimeZoneInfo *tz_info, uint8_t &y_value);
  static int date_to_datetime(int32_t d_value, const ObTimeConvertCtx &cvrt_ctx, int64_t &dt_value);
  static int date_to_year(int32_t d_value, uint8_t &y_value);
  // string -> offset. value: seconds, not useconds.
  static int str_to_offset(const ObString &str, int32_t &value, int &ret_more,
                           const bool is_oracle_mode, const bool need_check_valid = false);
  // year / month / day / quarter / week / hour / minite / second / microsecond.
  static int int_to_week(int64_t uint64, int64_t mode, int32_t &value);
  // date add / sub / diff.
  static int date_adjust(const int64_t base_value, const ObString &interval_str,
                         ObDateUnitType unit_type, int64_t &value, bool is_add,
                         const ObDateSqlMode date_sql_mode);
  static int date_adjust(const ObString &base_str, const ObString &interval_str,
                         ObDateUnitType unit_type, int64_t &value, bool is_add);
  static bool is_valid_datetime(const int64_t usec);
  static bool is_valid_otimestamp(const int64_t time_us, const int32_t tail_nsec);
  static void calc_oracle_temporal_minus(const ObOTimestampData &v1, const ObOTimestampData &v2, ObIntervalDSValue &result);
  static int date_add_nmonth(const int64_t ori_date_value, const int64_t nmonth,
                             int64_t &result_date_value, bool auto_adjust_mday = false);
  static int date_add_nsecond(const int64_t ori_date_value, const int64_t nsecond,
                              const int32_t fractional_second, int64_t &result_date_value);
  static int otimestamp_add_nmonth(const ObObjType type, const ObOTimestampData ori_value, const ObTimeZoneInfo *tz_info,
                                   const int64_t nmonth, ObOTimestampData &result_value);
  static int otimestamp_add_nsecond(const ObOTimestampData ori_value, const int64_t nsecond,
                                    const int32_t fractional_second,
                                    ObOTimestampData &result_value);
  static int calc_last_date_of_the_month(const int64_t ori_date_value, int64_t &result_date_value,
                                         const ObObjType dest_type,
                                         const ObDateSqlMode date_sql_mode);
  static int calc_next_date_of_the_wday(const int64_t ori_date_value, const ObString &wday_name, const int64_t week_count, int64_t &result_date_value);
  static int calc_days_and_months_between_dates(const int64_t date_value1, const int64_t date_value2, int64_t &months_diff, int64_t &rest_utc_diff);

public:
  // int / string -> ObTime / ObInterval <- datetime(timestamp) / date / time / year.
  static int int_to_ob_time_with_date(int64_t int64, ObTime &ob_time,
                                      const ObDateSqlMode date_sql_mode);
  static int int_to_ob_time_without_date(int64_t time_second, ObTime &ob_time, int64_t nano_second = 0);
  static int str_to_ob_time_with_date(const ObString &str, ObTime &ob_time, int16_t *scale,
                                      const ObDateSqlMode date_sql_mode,
                                      const bool &need_truncate = false);
  static int str_to_ob_time_without_date(const ObString &str, ObTime &ob_time, int16_t *scale = NULL, const bool &need_truncate = false);
  static int str_to_ob_time_format(const ObString &str, const ObString &fmt, ObTime &ob_time,
                                   int16_t *scale, const ObDateSqlMode date_sql_mode);
  static int str_to_ob_time_oracle_dfm(const ObString &str, const ObTimeConvertCtx &cvrt_ctx,
                                       const ObObjType target_type, ObTime &ob_time, ObScale &scale);
  static int str_to_ob_time_by_dfm_elems(const ObString &str,
                                         const ObIArray<ObDFMElem> &format_elems,
                                         const ObFixedBitSet<OB_DEFAULT_BITSET_SIZE_FOR_DFM> &elem_flags,
                                         const ObTimeConvertCtx &cvrt_ctx,
                                         const ObObjType target_type, ObTime &ob_time, ObScale &scale);

  static int str_to_ob_time_oracle_strict(const ObString &str, const ObTimeConvertCtx &cvrt_ctx, const bool is_timestamp_literal, ObTime &ob_time, ObScale &scale);
  static int calc_date_with_year_week_wday(const ObYearWeekWdayElems &elements, ObTime &ot);
  static int handle_year_week_wday(const ObYearWeekWdayElems &elements, ObTime &ot);
  static int str_to_ob_interval(const ObString &str, ObDateUnitType unit_type, ObInterval &ob_interval);
  static int usec_to_ob_time(int64_t usecs, ObTime &ob_time);
  static int datetime_to_ob_time(int64_t value, const ObTimeZoneInfo *tz_info, ObTime &ob_time);
  static int otimestamp_to_ob_time(const ObObjType type, const ObOTimestampData &ot_data,
                                   const ObTimeZoneInfo *tz_info, ObTime &ob_time,
                                   const bool store_utc_time = true);
  static int date_to_ob_time(int32_t value, ObTime &ob_time);
  static int time_to_ob_time(int64_t value, ObTime &ob_time);
  // int / string <- ObTime -> datetime(timestamp) / date / time.
  static int64_t ob_time_to_int(const ObTime &ob_time, ObDTMode mode);
  static int64_t ob_time_to_int_extract(const ObTime &ob_time, ObDateUnitType unit_type);
  static int ob_time_to_str(const ObTime &ob_time, ObDTMode mode, int16_t scale,
                            char *buf, int64_t buf_len, int64_t &pos, const bool with_delim);
  static bool valid_oracle_year(const ObTime &ob_time);
  static int ob_time_to_str_oracle_dfm(const ObTime &ob_time, ObScale scale, const ObString &format,
                                       char *buf, int64_t buf_len, int64_t &pos);
  static int ob_time_to_str_by_dfm_elems(const ObTime &ob_time, ObScale scale,
                                         const ObIArray<ObDFMElem> &format_elems,
                                         const ObString &format,
                                         char *buf, int64_t buf_len, int64_t &pos);
  static int deduce_max_len_from_oracle_dfm(const ObString &format,
                                            int64_t &max_char_len);
  static int ob_time_to_str_format(const ObTime &ob_time, const ObString &format,
                                   char *buf, int64_t buf_len, int64_t &pos, bool &res_null,
                                   const ObString &locale_name);
  static int ob_time_to_datetime(ObTime &ob_time, const ObTimeConvertCtx &cvrt_ctx, int64_t &value);
  static int ob_time_to_otimestamp(ObTime &ob_time, ObOTimestampData &value);
  static int32_t ob_time_to_date(ObTime &ob_time);
  static int64_t ob_time_to_time(const ObTime &ob_time);
  static int ob_interval_to_interval(const ObInterval &ob_interval, int64_t &value);
  // year / month / day / quarter / week / hour / minite / second / microsecond.
  static int32_t ob_time_to_week(const ObTime &ob_time, ObDTMode mode);
  static int32_t ob_time_to_week(const ObTime &ob_time, ObDTMode mode, int32_t &delta);
  static void get_first_day_of_isoyear(ObTime &ob_time);
  static int get_round_day_of_isoyear(ObTime &ob_time);
  //mysql binary value encoder/decoder
  static int decode_otimestamp(const ObObjType obj_type, const char *data,
                               const int64_t total_len, const ObTimeConvertCtx &cvrt_ctx,
                               ObOTimestampData &otimestamp_val, int8_t &scale);
  static int encode_otimestamp(const ObObjType obj_type, char *buf,
                               const int64_t len, int64_t &pos,
                               const ObTimeZoneInfo *tz_info,
                               const ObOTimestampData &ot_data,
                               const int8_t &scale);
  static int validate_oracle_date(const ObTime &ob_time);

  //oracle interval functions
  static int interval_ym_to_str(const ObIntervalYMValue &value, const ObScale scale,
                                char *buf, int64_t buf_len, int64_t &pos, bool fmt_with_interval);
  static int interval_ds_to_str(const ObIntervalDSValue &value, const ObScale scale,
                                char *buf, int64_t buf_len, int64_t &pos, bool fmt_with_interval);
  static int str_to_interval_ym(const ObString &str, ObIntervalYMValue &value, ObScale &scale,
                                ObDateUnitType part_begin = DATE_UNIT_YEAR, ObDateUnitType part_end = DATE_UNIT_MONTH);
  static int str_to_interval_ds(const ObString &str, ObIntervalDSValue &value, ObScale &scale,
                                ObDateUnitType part_begin = DATE_UNIT_DAY, ObDateUnitType part_end = DATE_UNIT_SECOND);
  static int iso_str_to_interval_ym(const ObString &str, ObIntervalYMValue &value);
  static int iso_str_to_interval_ds(const ObString &str, ObIntervalDSValue &value);
  static int iso_interval_str_parse(const ObString &str, ObIntervalParts &interval_parts, ObIntervalParts::PartName begin_part);

  static void interval_ym_to_display_part(const ObIntervalYMValue &value, ObIntervalParts &display_parts);
  static void interval_ds_to_display_part(const ObIntervalDSValue &value, ObIntervalParts &display_parts);

  static int decode_interval_ym(const char *data, const int64_t len, ObIntervalYMValue &value, ObScale &scale);
  static int encode_interval_ym(char *buf, const int64_t len, int64_t &pos,
                                const ObIntervalYMValue &value, const ObScale scale);
  static int decode_interval_ds(const char *data, const int64_t len, ObIntervalDSValue &value, ObScale &scale);
  static int encode_interval_ds(char *buf, const int64_t len, int64_t &pos,
                                const ObIntervalDSValue &value, const ObScale scale);
  static int data_fmt_nd(char *buffer, int64_t buf_len, int64_t &pos, const int64_t n, int64_t target, bool has_fm_flag = false);
  static int data_fmt_d(char *buffer, int64_t buf_len, int64_t &pos, int64_t target);
  static int data_fmt_s(char *buffer, int64_t buf_len, int64_t &pos, const char *ptr);
  // check each part of ObTime and carry if necessary.
  static void adjust_ob_time(ObTime &ot);

public:
  // other functions.
  static int set_ob_time_part_directly(ObTime &ob_time, int64_t &conflict_bitset, const int64_t part_offset, const int32_t part_value);
  static int set_ob_time_part_may_conflict(ObTime &ob_time, int64_t &conflict_bitset, const int64_t part_offset, const int32_t part_value);
  static int32_t calc_max_name_length(const ObTimeConstStr names[], const int64_t size);
  static int time_overflow_trunc(int64_t &value, const ObScale &time_scale = 0);
  static void round_datetime(int16_t scale, int64_t &value);
  static ObOTimestampData round_otimestamp(const int16_t scale, const ObOTimestampData &in_ot_data);
  static int round_interval_ds(const ObScale scale, ObIntervalDSValue &value);
  static void trunc_datetime(int16_t scale, int64_t &value);
  static bool ob_is_date_datetime_all_parts_zero(const int64_t &value)
  {
    return (ZERO_DATE == value) || (ZERO_DATETIME == value);
  }
  static int get_oracle_err_when_datetime_out_of_range(int64_t part_idx);
  static int get_oracle_err_when_datetime_parts_conflict(int64_t part_idx);
  static int validate_time(ObTime &ob_time);
  static int check_dfm_deterministic(const ObString format,
                                    const ObCollationType format_coll_type,
                                    const bool need_tz, bool &complete);
  static int32_t get_days_of_month(int32_t year, int32_t month);
  struct ObTimeDigits {
    ObTimeDigits()
      : ptr_(NULL), len_(0), value_(0)
    {}
    VIRTUAL_TO_STRING_KV("ptr_", common::ObLenString(ptr_, len_), K(len_), K(value_));
    const char *ptr_;
    int32_t len_;
    int32_t value_;
  };
  struct ObTimeDelims {
    ObTimeDelims()
      : ptr_(NULL), len_(0)
    {}
    VIRTUAL_TO_STRING_KV("ptr_", common::ObLenString(ptr_, len_), K(len_));
    const char *ptr_;
    int32_t len_;
  };
  enum ObHourFlag
  {
    HOUR_UNUSE,
    HOUR_AM,
    HOUR_PM
  };
  /*enum ObYearWeekWdayType
  {
    UPPER_U,
    LOWER_U,
    UPPER_X,
    LOWER_X,
    UPPER_V,
    LOWER_V,
    UPPER_W,
    LOWER_W,
    YEAR_WEEK_WDAY_COUNT
  };
  struct ObAllYearWeekWdaySpec {
    const static int32_t U_MAX = 53;
    const static int32_t U_MIN = 53;
    ObAllYearWeekWdaySpec() {}
    ObYearWeekWdaySpec spec_[YEAR_WEEK_WDAY_COUNT];
  }
  */
public:
  static int validate_datetime(ObTime &ob_time, const ObDateSqlMode date_sql_mode);

private:
  // date add / sub / diff.
  static int merge_date_interval(int64_t base_value, const ObString &interval_str,
                                 ObDateUnitType unit_type, int64_t &value, bool is_add);
  static int merge_date_interval(/*const*/ ObTime &base_time, const ObString &interval_str,
                                 ObDateUnitType unit_type, int64_t &value, bool is_add,
                                 const ObDateSqlMode date_sql_mode);
  // other utility functions.
  static int validate_year(int64_t year);
  static int validate_oracle_timestamp(const ObTime &ob_time);
  static int validate_basic_part_of_ob_time_oracle(const ObTime &ob_time);
  static int validate_tz_part_of_ob_time_oracle(const ObTime &ob_time);
  static int check_leading_precision(const ObTimeDigits &digits);
  static int get_datetime_digits(const char *&str, const char *end, int32_t max_len, ObTimeDigits &digits);
  static int get_datetime_delims(const char *&str, const char *end, ObTimeDelims &delims);
  static int get_datetime_digits_delims(const char *&str, const char *end,
                                        int32_t max_len, ObTimeDigits &digits, ObTimeDelims &delims);
  static int str_to_digit_with_date(const ObString &str, ObTimeDigits *digits, ObTime &obtime, const bool &need_truncate = false);
  static int get_time_zone(const ObTimeDelims *delims, ObTime &ob_time, const char *end_ptr);
  static void skip_delims(const char *&str, const char *end);
  static bool is_year4(int64_t first_token_len);
  static bool is_single_colon(const ObTimeDelims &delims);
  static bool is_space_end_with_single_colon(const ObTimeDelims &delims);
  static bool is_single_dot(const ObTimeDelims &delims);
  static bool is_all_spaces(const ObTimeDelims &delims);
  static bool has_any_space(const ObTimeDelims &delims);
  static bool is_negative(const char *&str, const char *end);
  static int normalize_usecond(ObTimeDigits &digits, const int64_t max_precision,
                                     const bool use_strict_check = false, const bool &need_truncate = false);
  static int normalize_usecond_trunc(ObTimeDigits &digits, bool need_trunc);
  static int apply_date_space_rule(const ObTimeDelims *delims);
  static void apply_date_year2_rule(ObTimeDigits &year);
  static void apply_date_year2_rule(int32_t &year);
  static void apply_date_year2_rule(int64_t &year);
  static int apply_usecond_delim_rule(ObTimeDelims &second, ObTimeDigits &usecond, const int64_t max_precision, const bool use_strict_check, const bool &truncate = false);
  static int apply_datetime_for_time_rule(ObTime &ob_time, const ObTimeDigits *digits, const ObTimeDelims *delims);
//  static int find_time_range(int64_t t, const int64_t *range_boundaries, uint64_t higher_bound, uint64_t& result);
//  static int find_transition_type(int64_t t, const ObTimeZoneInfo *sp, TRAN_TYPE_INFO *& result);
  static int add_timezone_offset(const ObTimeZoneInfo *tz_info, int64_t &value);
  static int sub_timezone_offset(const ObTimeZoneInfo *tz_info, bool is_timestamp, const ObString &tz_abbr_str,
                                 int64_t &value, const bool is_oracle_mode = false);
  static int sub_timezone_offset(const ObTimeZoneInfo &tz_info, const ObString &tz_abbr_str,
                                 int64_t &value_us, int32_t &offset_min, int32_t &tz_id, int32_t &tran_type_id);
  static int get_str_array_idx(const ObString &str, const ObTimeConstStr *array, int32_t count, int32_t &idx);
  static int get_day_and_month_from_year_day(const int32_t yday, const int32_t year, int32_t &month, int32_t &day);
  static int set_ob_time_year_may_conflict(ObTime &ob_time, int32_t &julian_year_value,
                                          int32_t check_year, int32_t set_year, bool overwrite);
private:
  ObTimeConverter();
  virtual ~ObTimeConverter();
  DISALLOW_COPY_AND_ASSIGN(ObTimeConverter);
};

enum ObNLSFormatEnum {
  NLS_DATE = 0,
  NLS_TIMESTAMP,
  NLS_TIMESTAMP_TZ,
  NLS_MAX, // does not support expansion due to error use in ob_rpc_struct.h
};

/**
 * @brief The ObDataTypeCastParams struct
 * 传递用于SQL解析的session环境变量，
 * 特别是用于oracle模式
 * 包括时区/时间格式/字符集等信息
 */

class ObDataTypeCastParams
{
public:
  ObDataTypeCastParams() :
    tz_info_(NULL),
    session_nls_formats_{},
    force_use_standard_format_(false),
    nls_collation_(CS_TYPE_INVALID),
    nls_collation_nation_(CS_TYPE_INVALID),
    connection_collation_(CS_TYPE_UTF8MB4_BIN)
  {
  }
  ObDataTypeCastParams(const ObTimeZoneInfo *tz_info,
                       const ObString *nls_formats,
                       const ObCollationType nls_collation,
                       const ObCollationType nls_collation_nation,
                       const ObCollationType connection_collation,
                       const bool force_use_standard_format = false)
    : tz_info_(tz_info),
      session_nls_formats_{},
      force_use_standard_format_(force_use_standard_format),
      nls_collation_(nls_collation),
      nls_collation_nation_(nls_collation_nation),
      connection_collation_(connection_collation)
  {
    for (int64_t i = 0; NULL != nls_formats && i < ObNLSFormatEnum::NLS_MAX; ++i) {
      session_nls_formats_[i] =  nls_formats[i];
    }
  }
  ObDataTypeCastParams(const ObTimeZoneInfo *tz_info,
                       const ObString nls_date_format,
                       const ObString nls_timestamp_format,
                       const ObString nls_timestmap_tz_format,
                       const ObCollationType nls_collation,
                       const ObCollationType nls_collation_nation,
                       const ObCollationType connection_collation,
                       const bool force_use_standard_format = false)
    : tz_info_(tz_info),
      session_nls_formats_{},
      force_use_standard_format_(force_use_standard_format),
      nls_collation_(nls_collation),
      nls_collation_nation_(nls_collation_nation),
      connection_collation_(connection_collation)
  {
    session_nls_formats_[0] = nls_date_format;
    session_nls_formats_[1] = nls_timestamp_format;
    session_nls_formats_[2] = nls_timestmap_tz_format;
  }
  ObDataTypeCastParams(const ObTimeZoneInfo *tz_info)
    : tz_info_(tz_info),
      session_nls_formats_{},
      force_use_standard_format_(true),
      nls_collation_(CS_TYPE_INVALID),
      nls_collation_nation_(CS_TYPE_INVALID),
      connection_collation_(CS_TYPE_UTF8MB4_BIN)
  {
  }
  ObString get_nls_format(const ObObjType input_type) const
  {
    ObString format_str;
    switch (input_type) {
      case ObDateTimeType:
        format_str = (force_use_standard_format_
                      ? ObTimeConverter::COMPAT_OLD_NLS_DATE_FORMAT
                      : (session_nls_formats_[ObNLSFormatEnum::NLS_DATE].empty()
                          ? ObTimeConverter::DEFAULT_NLS_DATE_FORMAT
                          : session_nls_formats_[ObNLSFormatEnum::NLS_DATE]));
        break;
      case ObTimestampNanoType:
      case ObTimestampLTZType:
        format_str = (force_use_standard_format_
                      ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_FORMAT
                      : (session_nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP].empty()
                          ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_FORMAT
                          : session_nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP]));
        break;
      case ObTimestampTZType:
        format_str = (force_use_standard_format_
                      ? ObTimeConverter::COMPAT_OLD_NLS_TIMESTAMP_TZ_FORMAT
                      : (session_nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ].empty()
                          ? ObTimeConverter::DEFAULT_NLS_TIMESTAMP_TZ_FORMAT
                          : session_nls_formats_[ObNLSFormatEnum::NLS_TIMESTAMP_TZ]));
        break;
      default:
        break;
    }
    return format_str;
  }

  const ObTimeZoneInfo *tz_info_;
  ObString session_nls_formats_[ObNLSFormatEnum::NLS_MAX];
  //only user related str depend nls_format. others do not care it, such as ob_print_sql...
  bool force_use_standard_format_;
  ObCollationType nls_collation_;
  ObCollationType nls_collation_nation_;
  ObCollationType connection_collation_; //as client cs for now
};

}// end of common
}// end of oceanbase

#endif

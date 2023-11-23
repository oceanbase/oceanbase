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
#include  "ob_locale_type.h"

namespace oceanbase {
namespace common {

//LOCALE BEGIN en_US: English - United States
static const char *ob_locale_month_names_en_US[MONTH_LENGTH] = {
    "January", "February",  "March",   "April",    "May",      "June", "July",
    "August",  "September", "October", "November", "December", NULL};
static const char *ob_locale_ab_month_names_en_US[MONTH_LENGTH] = {
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul",
    "Aug", "Sep", "Oct", "Nov", "Dec", NULL};
static const char *ob_locale_day_names_en_US[DAY_LENGTH] = {
    "Monday", "Tuesday",  "Wednesday", "Thursday",
    "Friday", "Saturday", "Sunday", NULL};
static const char *ob_locale_ab_day_names_en_US[DAY_LENGTH] = {
    "Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun", NULL};
static OB_LOCALE_TYPE ob_locale_type_month_names_en_US( MONTH_LENGTH - 1, "", ob_locale_month_names_en_US, NULL);
static OB_LOCALE_TYPE ob_locale_type_ab_month_names_en_US(MONTH_LENGTH - 1, "", ob_locale_ab_month_names_en_US, NULL);
static OB_LOCALE_TYPE ob_locale_type_day_names_en_US(DAY_LENGTH - 1, "", ob_locale_day_names_en_US, NULL);
static OB_LOCALE_TYPE ob_locale_type_ab_day_names_en_US(DAY_LENGTH - 1, "", ob_locale_ab_day_names_en_US, NULL);

OB_LOCALE ob_locale_en_US(0, "en_US", "English - United States", true,
                          &ob_locale_type_month_names_en_US,
                          &ob_locale_type_ab_month_names_en_US,
                          &ob_locale_type_day_names_en_US,
                          &ob_locale_type_ab_day_names_en_US, 9, 9,
                          '.',        /* decimal point en_US */
                          ',',        /* thousands_sep en_US */
                          "\x03\x03"/* grouping      en_US */);

/***** LOCALE BEGIN ja_JP: Japanese - Japan *****/
static const char *ob_locale_month_names_ja_JP[MONTH_LENGTH] = {
    "1月", "2月", "3月",  "4月",  "5月",  "6月", "7月",
    "8月", "9月", "10月", "11月", "12月", NULL};
static const char *ob_locale_ab_month_names_ja_JP[MONTH_LENGTH] = {
    " 1月", " 2月", " 3月", " 4月", " 5月", " 6月", " 7月",
    " 8月", " 9月", "10月", "11月", "12月", NULL};
static const char *ob_locale_day_names_ja_JP[DAY_LENGTH] = {
	"月曜日", "火曜日", "水曜日","木曜日",
	"金曜日", "土曜日", "日曜日", NULL};
static const char *ob_locale_ab_day_names_ja_JP[DAY_LENGTH] = {
    "月", "火", "水", "木", "金", "土", "日", NULL};
static OB_LOCALE_TYPE ob_locale_type_month_names_ja_JP = { MONTH_LENGTH - 1, "", ob_locale_month_names_ja_JP, NULL };
static OB_LOCALE_TYPE ob_locale_type_ab_month_names_ja_JP = { MONTH_LENGTH - 1, "", ob_locale_ab_month_names_ja_JP, NULL};
static OB_LOCALE_TYPE ob_locale_type_day_names_ja_JP = { DAY_LENGTH - 1, "", ob_locale_day_names_ja_JP, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_day_names_ja_JP = { DAY_LENGTH - 1, "", ob_locale_ab_day_names_ja_JP, NULL};

OB_LOCALE ob_locale_ja_JP(2, "ja_JP", "Japanese - Japan", false,
                          &ob_locale_type_month_names_ja_JP,
                          &ob_locale_type_ab_month_names_ja_JP,
                          &ob_locale_type_day_names_ja_JP,
                          &ob_locale_type_ab_day_names_ja_JP, 3, 3,
                          '.',    /* decimal point ja_JP */
                          ',',    /* thousands_sep ja_JP */
                          "\x03"/* grouping      ja_JP */);

/***** LOCALE BEGIN ko_KR: Korean - Korea *****/
static const char *ob_locale_month_names_ko_KR[MONTH_LENGTH] = {
    "일월", "이월", "삼월", "사월",   "오월",   "유월", "칠월",
    "팔월", "구월", "시월", "십일월", "십이월", NULL};
static const char *ob_locale_ab_month_names_ko_KR[MONTH_LENGTH] = {
    " 1월", " 2월", " 3월", " 4월", " 5월", " 6월", " 7월",
    " 8월", " 9월", "10월", "11월", "12월", NULL};
static const char *ob_locale_day_names_ko_KR[DAY_LENGTH] = {
	"월요일", "화요일", "수요일", "목요일",
	"금요일", "토요일", "일요일", NULL};
static const char *ob_locale_ab_day_names_ko_KR[DAY_LENGTH] = {
	"월", "화", "수", "목", "금", "토", "일", NULL};
static OB_LOCALE_TYPE ob_locale_type_month_names_ko_KR = { MONTH_LENGTH - 1, "", ob_locale_month_names_ko_KR, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_month_names_ko_KR = { MONTH_LENGTH - 1, "", ob_locale_ab_month_names_ko_KR, NULL};
static OB_LOCALE_TYPE ob_locale_type_day_names_ko_KR = { DAY_LENGTH - 1, "", ob_locale_day_names_ko_KR, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_day_names_ko_KR = { DAY_LENGTH - 1, "", ob_locale_ab_day_names_ko_KR, NULL};
OB_LOCALE ob_locale_ko_KR(31, "ko_KR", "Korean - Korea", false,
                          &ob_locale_type_month_names_ko_KR,
                          &ob_locale_type_ab_month_names_ko_KR,
                          &ob_locale_type_day_names_ko_KR,
                          &ob_locale_type_ab_day_names_ko_KR, 3, 3,
                          '.',        /* decimal point ko_KR */
                          ',',        /* thousands_sep ko_KR */
                          "\x03\x03" /* grouping      ko_KR */);

/***** LOCALE BEGIN zh_CN: Chinese - Peoples Republic of China *****/
static const char *ob_locale_month_names_zh_CN[MONTH_LENGTH] = {
    "一月", "二月", "三月", "四月",   "五月",   "六月", "七月",
    "八月", "九月", "十月", "十一月", "十二月", NULL};
static const char *ob_locale_ab_month_names_zh_CN[MONTH_LENGTH] = {
    " 1月", " 2月", " 3月", " 4月", " 5月", " 6月", " 7月",
    " 8月", " 9月", "10月", "11月", "12月", NULL};
static const char *ob_locale_day_names_zh_CN[DAY_LENGTH] = {
	"星期一", "星期二", "星期三", "星期四",
	"星期五", "星期六", "星期日", NULL};
static const char *ob_locale_ab_day_names_zh_CN[DAY_LENGTH] = {
	"一", "二", "三", "四", "五", "六", "日", NULL};
static OB_LOCALE_TYPE ob_locale_type_month_names_zh_CN = { MONTH_LENGTH - 1, "", ob_locale_month_names_zh_CN, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_month_names_zh_CN = { MONTH_LENGTH - 1, "", ob_locale_ab_month_names_zh_CN, NULL};
static OB_LOCALE_TYPE ob_locale_type_day_names_zh_CN = { DAY_LENGTH - 1, "", ob_locale_day_names_zh_CN, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_day_names_zh_CN = { DAY_LENGTH - 1, "", ob_locale_ab_day_names_zh_CN, NULL};
OB_LOCALE ob_locale_zh_CN(56, "zh_CN", "Chinese - Peoples Republic of China", false,
						  &ob_locale_type_month_names_zh_CN,
                          &ob_locale_type_ab_month_names_zh_CN,
                          &ob_locale_type_day_names_zh_CN,
                          &ob_locale_type_ab_day_names_zh_CN, 3, 3,
                          '.',    /* decimal point zh_CN */
                          ',',    /* thousands_sep zh_CN */
                          "\x03" /* grouping      zh_CN */);

/***** LOCALE BEGIN zh_TW: Chinese - Taiwan *****/
static const char *ob_locale_month_names_zh_TW[MONTH_LENGTH] = {
    "一月", "二月", "三月", "四月",   "五月",   "六月", "七月",
    "八月", "九月", "十月", "十一月", "十二月", NULL};
static const char *ob_locale_ab_month_names_zh_TW[MONTH_LENGTH] = {
    " 1月", " 2月", " 3月", " 4月", " 5月", " 6月", " 7月",
    " 8月", " 9月", "10月", "11月", "12月", NULL};
static const char *ob_locale_day_names_zh_TW[DAY_LENGTH] = {
    "週一", "週二", "週三", "週四",
	"週五", "週六", "週日", NULL};
static const char *ob_locale_ab_day_names_zh_TW[DAY_LENGTH] = {
	"一", "二", "三", "四","五", "六", "日", NULL};
static OB_LOCALE_TYPE ob_locale_type_month_names_zh_TW = { MONTH_LENGTH - 1, "", ob_locale_month_names_zh_TW, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_month_names_zh_TW = { MONTH_LENGTH - 1, "", ob_locale_ab_month_names_zh_TW, NULL};
static OB_LOCALE_TYPE ob_locale_type_day_names_zh_TW = { DAY_LENGTH - 1, "", ob_locale_day_names_zh_TW, NULL};
static OB_LOCALE_TYPE ob_locale_type_ab_day_names_zh_TW = { DAY_LENGTH - 1, "", ob_locale_ab_day_names_zh_TW, NULL};
OB_LOCALE ob_locale_zh_TW(57, "zh_TW", "Chinese - Taiwan", false,
                          &ob_locale_type_month_names_zh_TW,
                          &ob_locale_type_ab_month_names_zh_TW,
                          &ob_locale_type_day_names_zh_TW,
                          &ob_locale_type_ab_day_names_zh_TW, 3, 2,
                          '.',    /* decimal point zh_TW */
                          ',',    /* thousands_sep zh_TW */
                          "\x03" /* grouping      zh_TW */);

} // common
} // oceanbase

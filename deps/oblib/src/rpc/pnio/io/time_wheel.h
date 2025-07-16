/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define TIME_WHEEL_SLOT_NUM (1<<15)
#define TIME_WHEEL_SLOT_INTERVAL 8192
typedef struct time_wheel_t time_wheel_t;
typedef void (timer_cb_t)(time_wheel_t* tw, dlink_t* l);
typedef struct time_wheel_t {
  timer_cb_t* cb;
  int64_t finished_us;
  dlink_t slot[TIME_WHEEL_SLOT_NUM];
} time_wheel_t;

typedef struct time_dlink_t {
  dlink_t dlink;
  int64_t expire_us;
} time_dlink_t;

extern void tw_init(time_wheel_t* tw, timer_cb_t* cb);
extern int tw_regist(time_wheel_t* tw, dlink_t* l);
extern int tw_regist_timeout(time_wheel_t* tw, time_dlink_t* l, int64_t abs_timeout_us);
extern void tw_check(time_wheel_t* tw);
extern int timerfd_init_tw(eloop_t* ep, timerfd_t* s);
extern void time_dlink_init(time_dlink_t* l);

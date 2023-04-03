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

#include "lib/oblog/ob_log.h"
#include "share/ash/ob_active_sess_hist_list.h"

using namespace oceanbase::common;
using namespace oceanbase::share;

ObActiveSessHistList &ObActiveSessHistList::get_instance()
{
  static ObActiveSessHistList the_one;
  return the_one;
}

int ObActiveSessHistList::init()
{
  int ret = OB_SUCCESS;
  // 30MB at most
  int64_t max_mem_for_ash = 30 * 1024 * 1024;
  list_.set_label("ash_list");
  if (OB_FAIL(list_.prepare_allocate(max_mem_for_ash / sizeof(ActiveSessionStat)))) {
    LOG_WARN("fail init ASH circular buffer", K(ret));
  } else {
    LOG_INFO("init ASH circular buffer OK", "size", list_.size(), K(ret));
  }
  return ret;
}

int ObActiveSessHistList::extend_list(int64_t new_size)
{
  int ret = OB_SUCCESS;
  // can only extend list, can't shrink
  if (OB_FAIL(list_.prepare_allocate(new_size))) {
    LOG_WARN("fail extend ASH circular buffer size", "old_size", list_.size(), K(new_size), K(ret));
  } else {
    LOG_INFO("extend ASH circular buffer size OK", "size", list_.size(), K(ret));
  }
  return ret;
}


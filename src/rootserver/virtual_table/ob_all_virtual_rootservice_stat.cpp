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

#define USING_LOG_PREFIX RS
#include "ob_all_virtual_rootservice_stat.h"
#include "lib/stat/ob_di_cache.h"
#include "lib/statistic_event/ob_stat_event.h"
#include "rootserver/ob_root_service.h"
using namespace oceanbase;
using namespace oceanbase::rootserver;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObAllVirtualRootserviceStat::ObAllVirtualRootserviceStat()
    : ObSimpleVirtualTableIterator(OB_SYS_TENANT_ID, OB_ALL_VIRTUAL_ROOTSERVICE_STAT_TID),
      rootservice_(NULL),
      stat_iter_(0)
{}

int ObAllVirtualRootserviceStat::init(ObRootService& rootservice)
{
  rootservice_ = &rootservice;
  return ObSimpleVirtualTableIterator::init(&rootservice.get_schema_service());
}

int ObAllVirtualRootserviceStat::init_all_data()
{
  int ret = OB_SUCCESS;
  int64_t high_wait_count = 0;
  int64_t high_sched_count = 0;
  int64_t low_wait_count = 0;
  int64_t low_sched_count = 0;
  if (NULL == rootservice_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(ObDIGlobalTenantCache::get_instance().get_the_diag_info(OB_SYS_TENANT_ID, sys_tenant_di_))) {
    LOG_WARN("failed to get the diagnose info", K(ret));
  } else if (OB_FAIL(rootservice_->get_rebalance_task_mgr().get_all_tasks_count(
                 high_wait_count, high_sched_count, low_wait_count, low_sched_count))) {
    LOG_WARN("failed to get task count", K(ret));
  } else {
    EVENT_SET(RS_TASK_QUEUE_SIZE_HIGH_WAIT, high_wait_count);
    EVENT_SET(RS_TASK_QUEUE_SIZE_HIGH_SCHED, high_sched_count);
    EVENT_SET(RS_TASK_QUEUE_SIZE_LOW_WAIT, low_wait_count);
    EVENT_SET(RS_TASK_QUEUE_SIZE_LOW_SCHED, low_sched_count);

    stat_iter_ = 0;
  }
  return ret;
}

// create table __all_virtual_rootservice_stat (`statistic#` bigint, value bigint, stat_id bigint, name varchar(64),
// `class` bigint, can_visible boolean, primary key(`statistic#`));
int ObAllVirtualRootserviceStat::get_next_full_row(
    const share::schema::ObTableSchema* table, common::ObIArray<Column>& columns)
{
  int ret = OB_SUCCESS;
  if (stat_iter_ < 0 || stat_iter_ >= ObStatEventIds::STAT_EVENT_SET_END) {
    ret = OB_ITER_END;
  } else {
    bool found = false;
    int32_t& stat_i = stat_iter_;
    for (; stat_i < ObStatEventIds::STAT_EVENT_SET_END && OB_SUCC(ret); ++stat_i) {
      if (OB_STAT_EVENTS[stat_i].stat_class_ == ObStatClassIds::RS) {
        ADD_COLUMN(set_int, table, "statistic#", stat_i, columns);
        ADD_COLUMN(set_int, table, "stat_id", OB_STAT_EVENTS[stat_i].stat_id_, columns);
        ADD_COLUMN(set_varchar, table, "name", OB_STAT_EVENTS[stat_i].name_, columns);
        ADD_COLUMN(set_int, table, "class", OB_STAT_EVENTS[stat_i].stat_class_, columns);
        ADD_COLUMN(set_bool, table, "can_visible", OB_STAT_EVENTS[stat_i].can_visible_, columns);
        int64_t stat_value = 0;
        if (stat_i < ObStatEventIds::STAT_EVENT_ADD_END) {
          ObStatEventAddStat* stat = sys_tenant_di_.get_add_stat_stats().get(stat_i);
          if (NULL == stat) {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "The argument is invalid, ", K(stat_i), K(ret));
          } else {
            stat_value = stat->stat_value_;
          }
        } else {
          ObStatEventSetStat* stat =
              sys_tenant_di_.get_set_stat_stats().get(stat_i - ObStatEventIds::STAT_EVENT_ADD_END - 1);
          if (NULL == stat) {
            ret = OB_INVALID_ARGUMENT;
            SERVER_LOG(WARN, "The argument is invalid, ", K(stat_i), K(ret));
          } else {
            stat_value = stat->stat_value_;
          }
        }
        ADD_COLUMN(set_int, table, "value", stat_value, columns);
        found = true;
        stat_iter_++;
        break;
      }
    }  // end for
    if (OB_SUCC(ret) && !found) {
      ret = OB_ITER_END;
    }
  }
  return ret;
}

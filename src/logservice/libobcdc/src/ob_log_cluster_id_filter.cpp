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
 *
 * Cluster ID Filter
 */

#define USING_LOG_PREFIX OBLOG_PARSER

#include "ob_log_cluster_id_filter.h"       // ObLogClusterIDFilter

#include "lib/string/ob_string.h"           // ObString

#include "ob_log_utils.h"                   // get_timestamp, get_record_type, split_int64

using namespace oceanbase::common;
namespace oceanbase
{
namespace libobcdc
{

const char IObLogClusterIDFilter::DEFAULT_CLUSTER_ID_BLACK_LIST_DELIMITER = '|';

ObLogClusterIDFilter::ObLogClusterIDFilter() :
    inited_(false),
    cluster_id_ignored_part_trans_count_(0),
    last_cluster_id_ignored_part_trans_count_(0),
    last_stat_time_(0),
    cluster_id_black_list_()
{}

ObLogClusterIDFilter::~ObLogClusterIDFilter()
{
  destroy();
}


int ObLogClusterIDFilter::init(const char *cluster_id_black_list,
    const int64_t cluster_id_black_value_min,
    const int64_t cluster_id_black_value_max)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("init twice", K(inited_));
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(OB_ISNULL(cluster_id_black_list)
      || OB_UNLIKELY(cluster_id_black_value_min > cluster_id_black_value_max))) {
    LOG_ERROR("invalid argument", K(cluster_id_black_list), K(cluster_id_black_value_min),
        K(cluster_id_black_value_max));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(init_cluster_id_black_list_(cluster_id_black_list,
      cluster_id_black_value_min, cluster_id_black_value_max))) {
    LOG_ERROR("init cluster id black list fail", KR(ret), K(cluster_id_black_list),
        K(cluster_id_black_value_min), K(cluster_id_black_value_max));
  } else {
    inited_ = true;
  }

  return ret;
}

int ObLogClusterIDFilter::init_cluster_id_black_list_(const char *cluster_id_black_list,
    const int64_t cluster_id_black_value_min,
    const int64_t cluster_id_black_value_max)
{
  int ret = OB_SUCCESS;
  ObString str(cluster_id_black_list);
  const char delimiter = DEFAULT_CLUSTER_ID_BLACK_LIST_DELIMITER;

  if (OB_ISNULL(cluster_id_black_list)
      || OB_UNLIKELY(cluster_id_black_value_min > cluster_id_black_value_max)) {
    LOG_ERROR("invalid argument", K(cluster_id_black_list), K(cluster_id_black_value_min),
        K(cluster_id_black_value_max));
    ret = OB_INVALID_ARGUMENT;
  }
  // split into int64 data
  else if (OB_FAIL(split_int64(str, delimiter, cluster_id_black_list_))) {
    LOG_ERROR("fail to parse cluster_id_black_list",
        KR(ret), K(str), K(delimiter), K(cluster_id_black_list_));
  } else {
    _LOG_INFO("[STAT] [CLUSTER_ID_BLACK_LIST] count=%ld, black_list='%s', min=%ld, max=%ld",
        cluster_id_black_list_.count(), cluster_id_black_list,
        cluster_id_black_value_min, cluster_id_black_value_max);

    // Check the validity of each element and whether it is within a reasonable range
    for (int64_t idx = 0; OB_SUCCESS == ret && idx < cluster_id_black_list_.count(); idx++) {
      int64_t cluster_id = cluster_id_black_list_.at(idx);

      _LOG_INFO("[STAT] [CLUSTER_ID_BLACK_LIST] idx=%ld, cluster_id=%ld", idx, cluster_id);

      if (OB_UNLIKELY(cluster_id < cluster_id_black_value_min)
          || OB_UNLIKELY(cluster_id > cluster_id_black_value_max)) {
        LOG_ERROR("invalid cluster id in black list, which is out of range",
            K(cluster_id), K(cluster_id_black_value_min),
            K(cluster_id_black_value_max),
            K(cluster_id_black_list));
        ret = OB_INVALID_CONFIG;
      }
    }
  }
  return ret;
}

void ObLogClusterIDFilter::destroy()
{
  inited_ = false;
  cluster_id_ignored_part_trans_count_ = 0;
  last_cluster_id_ignored_part_trans_count_ = 0;
  last_stat_time_ = 0;
  cluster_id_black_list_.destroy();
}

void ObLogClusterIDFilter::stat_ignored_tps()
{
  int64_t cur_time = get_timestamp();
  int64_t cur_count = ATOMIC_LOAD(&cluster_id_ignored_part_trans_count_);
  int64_t last_count = ATOMIC_LOAD(&last_cluster_id_ignored_part_trans_count_);
  int64_t delta_time = (cur_time - last_stat_time_) / 1000000;

  if (last_stat_time_ > 0 && delta_time > 0) {
    double tps = static_cast<double>(cur_count - last_count) / static_cast<double>(delta_time);

    _LOG_INFO("[TPS_STAT] CLUSTER_ID_IGNORED_PART_TPS=%.3lf", tps);
  }

  last_cluster_id_ignored_part_trans_count_ = cur_count;
  last_stat_time_ = cur_time;
}

int ObLogClusterIDFilter::check_is_served(const uint64_t cluster_id, bool &is_served,
    const bool stat_tps)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not inited", K(inited_));
    ret = OB_NOT_INIT;
  } else {
    is_served = true;
    for (int64_t idx = 0; OB_SUCCESS == ret && is_served && idx < cluster_id_black_list_.count(); idx++) {
      // not serve if in blacklist
      if (cluster_id == cluster_id_black_list_.at(idx)) {
        is_served = false;
      }
    }

    if (! is_served && stat_tps) {
      (void)ATOMIC_FAA(&cluster_id_ignored_part_trans_count_, 1);
    }
  }

  return ret;
}

}
}

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

#ifndef OCEANBASE_LIBOBCDC_OB_LOG_CLUSTER_ID_FILTER_H_
#define OCEANBASE_LIBOBCDC_OB_LOG_CLUSTER_ID_FILTER_H_

#include "lib/utility/ob_macro_utils.h"             // CACHE_ALIGNED
#include "lib/container/ob_se_array.h"              // ObSEArray

namespace oceanbase
{
namespace libobcdc
{

class IObLogClusterIDFilter
{
public:
  static const char DEFAULT_CLUSTER_ID_BLACK_LIST_DELIMITER;

public:
  virtual ~IObLogClusterIDFilter() {}

public:
  virtual int check_is_served(const uint64_t cluster_id, bool &is_served,
      const bool stat_tps = true) = 0;
  virtual void stat_ignored_tps() = 0;
};

class ObLogClusterIDFilter : public IObLogClusterIDFilter
{
  static const int64_t DEFAULT_CLUSTER_ID_BLACK_LIST_SIZE = 8;
public:
  ObLogClusterIDFilter();
  virtual ~ObLogClusterIDFilter();

public:
  virtual int check_is_served(const uint64_t cluster_id, bool &is_served,
      const bool stat_tps = true);
  virtual void stat_ignored_tps();

public:
  int init(const char *cluster_id_black_list,
      const int64_t cluster_id_black_value_min,
      const int64_t cluster_id_black_value_max);
  void destroy();

private:
  int init_cluster_id_black_list_(const char *cluster_id_black_list,
      const int64_t cluster_id_black_value_min,
      const int64_t cluster_id_black_value_max);

private:
  bool      inited_;
  // TPS statistics based on cluster_id filtering
  // The TPS statistics here refers to the number of partition transactions
  int64_t   cluster_id_ignored_part_trans_count_ CACHE_ALIGNED;
  int64_t   last_cluster_id_ignored_part_trans_count_ CACHE_ALIGNED;
  int64_t   last_stat_time_ CACHE_ALIGNED;

  // blacklist of cluster id
  common::ObSEArray<int64_t, DEFAULT_CLUSTER_ID_BLACK_LIST_SIZE> cluster_id_black_list_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLogClusterIDFilter);
};

}
}
#endif

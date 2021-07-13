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

#ifndef OCEABASE_OBSERVER_OB_CLUSTER_VERSION_H_
#define OCEABASE_OBSERVER_OB_CLUSTER_VERSION_H_

#include <stdint.h>

namespace oceanbase {
namespace common {
class ObServerConfig;
class ObString;

class ObClusterVersion {
public:
  ObClusterVersion();
  ~ObClusterVersion()
  {
    destroy();
  }
  int init(const common::ObServerConfig* config);
  int init(const uint64_t cluster_version);
  void destroy();
  int64_t to_string(char* buf, const int64_t buf_len) const;
  int refresh_cluster_version(const char* verstr);
  int reload_config();
  uint64_t get_cluster_version();
  void update_cluster_version(const uint64_t cluster_version);

public:
  static ObClusterVersion& get_instance();
  static int is_valid(const char* verstr);
  static int get_version(const char* verstr, uint64_t& version);
  static int get_version(const common::ObString& verstr, uint64_t& version);
  static int64_t print_vsn(char* buf, const int64_t buf_len, uint64_t version);
  static int64_t print_version_str(char* buf, const int64_t buf_len, uint64_t version);
  static const int64_t MAX_VERSION_ITEM = 16;

private:
  bool is_inited_;
  const common::ObServerConfig* config_;
  uint64_t cluster_version_;
};

uint64_t cal_version(const uint64_t major, const uint64_t minor, const uint64_t patch);

#define DEF_MAJOR_VERSION 1
#define DEF_MINOR_VERSION 4
#define DEF_PATCH_VERSION 40

#define CLUSTER_VERSION_140 (oceanbase::common::cal_version(1, 4, 0))
#define CLUSTER_VERSION_141 (oceanbase::common::cal_version(1, 4, 1))
#define CLUSTER_VERSION_142 (oceanbase::common::cal_version(1, 4, 2))
#define CLUSTER_VERSION_143 (oceanbase::common::cal_version(1, 4, 3))
#define CLUSTER_VERSION_1431 (oceanbase::common::cal_version(1, 4, 31))
#define CLUSTER_VERSION_1432 (oceanbase::common::cal_version(1, 4, 32))
#define CLUSTER_VERSION_144 (oceanbase::common::cal_version(1, 4, 4))
#define CLUSTER_VERSION_1440 (oceanbase::common::cal_version(1, 4, 40))
#define CLUSTER_VERSION_1450 (oceanbase::common::cal_version(1, 4, 50))
#define CLUSTER_VERSION_1460 (oceanbase::common::cal_version(1, 4, 60))
#define CLUSTER_VERSION_1461 (oceanbase::common::cal_version(1, 4, 61))
#define CLUSTER_VERSION_1470 (oceanbase::common::cal_version(1, 4, 70))
#define CLUSTER_VERSION_1471 (oceanbase::common::cal_version(1, 4, 71))
#define CLUSTER_VERSION_1472 (oceanbase::common::cal_version(1, 4, 72))
#define CLUSTER_VERSION_1500 (oceanbase::common::cal_version(1, 5, 0))
#define CLUSTER_VERSION_2000 (oceanbase::common::cal_version(2, 0, 0))
#define CLUSTER_VERSION_2100 (oceanbase::common::cal_version(2, 1, 0))
#define CLUSTER_VERSION_2110 (oceanbase::common::cal_version(2, 1, 1))
#define CLUSTER_VERSION_2200 (oceanbase::common::cal_version(2, 2, 0))
#define CLUSTER_VERSION_2210 (oceanbase::common::cal_version(2, 2, 1))
#define CLUSTER_VERSION_2220 (oceanbase::common::cal_version(2, 2, 20))
#define CLUSTER_VERSION_2230 (oceanbase::common::cal_version(2, 2, 30))
#define CLUSTER_VERSION_2240 (oceanbase::common::cal_version(2, 2, 40))
#define CLUSTER_VERSION_2250 (oceanbase::common::cal_version(2, 2, 50))
#define CLUSTER_VERSION_2260 (oceanbase::common::cal_version(2, 2, 60))
#define CLUSTER_VERSION_2270 (oceanbase::common::cal_version(2, 2, 70))
#define CLUSTER_VERSION_2271 (oceanbase::common::cal_version(2, 2, 71))
#define CLUSTER_VERSION_2272 (oceanbase::common::cal_version(2, 2, 72))
#define CLUSTER_VERSION_2273 (oceanbase::common::cal_version(2, 2, 73))
#define CLUSTER_VERSION_2274 (oceanbase::common::cal_version(2, 2, 74))
#define CLUSTER_VERSION_2275 (oceanbase::common::cal_version(2, 2, 75))
#define CLUSTER_VERSION_2276 (oceanbase::common::cal_version(2, 2, 76))
#define CLUSTER_VERSION_3000 (oceanbase::common::cal_version(3, 0, 0))
#define CLUSTER_VERSION_3100 (oceanbase::common::cal_version(3, 1, 0))
// FIXME If you update the above version, please update me, CLUSTER_CURRENT_VERSION & ObUpgradeChecker!!!!!!
//!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
#define CLUSTER_CURRENT_VERSION CLUSTER_VERSION_3100
#define GET_MIN_CLUSTER_VERSION() (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version())
#define GET_UNIS_CLUSTER_VERSION() (::oceanbase::lib::get_unis_compat_version() ?: GET_MIN_CLUSTER_VERSION())

#define IS_CLUSTER_VERSION_BEFORE_1472 \
  (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() < CLUSTER_VERSION_1472)
#define IS_CLUSTER_VERSION_BEFORE_2200 \
  (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() < CLUSTER_VERSION_2200)
#define IS_CLUSTER_VERSION_BEFORE_2240 \
  (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() < CLUSTER_VERSION_2240)
#define IS_CLUSTER_VERSION_BEFORE_3000 \
  (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() < CLUSTER_VERSION_3000)
#define IS_CLUSTER_VERSION_BEFORE_3100 \
  (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() < CLUSTER_VERSION_3100)

#define IS_CLUSTER_VERSION_AFTER_2274 \
  (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() > CLUSTER_VERSION_2274)

#define OB_VSN_MAJOR(version) (static_cast<const uint16_t>((version >> 32) & 0xffff))
#define OB_VSN_MINOR(version) (static_cast<const uint16_t>((version >> 16) & 0xffff))
#define OB_VSN_PATCH(version) (static_cast<const uint16_t>(version & 0xffff))
}  // end of namespace common
}  // end of namespace oceanbase

#endif /* OCEABASE_OBSERVER_OB_CLUSTER_VERSION_H_*/

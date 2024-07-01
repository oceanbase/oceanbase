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
#include "lib/atomic/ob_atomic.h"
#include "common/ob_tenant_data_version_mgr.h"

namespace oceanbase
{
namespace omt
{
class ObTenantConfigMgr;
}
namespace common
{
class ObServerConfig;
class ObString;

//TODO: Will change ObClusterVersion to ObVersion later
class ObClusterVersion
{
public:
  ObClusterVersion();
  ~ObClusterVersion() { destroy(); }
  void destroy();
  int64_t to_string(char *buf, const int64_t buf_len) const;

  int init(const common::ObServerConfig *config,
           const omt::ObTenantConfigMgr *tenant_config_mgr);

  /* cluster version related */
  int init(const uint64_t cluster_version);
  int refresh_cluster_version(const char *verstr);
  int reload_config();
  uint64_t get_cluster_version() { return ATOMIC_LOAD(&cluster_version_); }
  void update_cluster_version(const uint64_t cluster_version);
  /*------------------------*/

  /* data version related */
  int get_tenant_data_version(const uint64_t tenant_id, uint64_t &data_version);
  // ATTENTION!!! this interface only work for unittest
  void update_data_version(const uint64_t data_version);
  /*------------------------*/
public:
  static ObClusterVersion &get_instance();
  static int is_valid(const char *verstr);
  static int get_version(const char *verstr, uint64_t &version);
  static int get_version(const common::ObString &verstr, uint64_t &version);
  static int64_t print_vsn(char *buf, const int64_t buf_len, uint64_t version);
  static int64_t print_version_str(char *buf, const int64_t buf_len, uint64_t version);
  static bool check_version_valid_(const uint64_t version);
public:
  static const int64_t MAX_VERSION_ITEM = 16;
  static const int64_t MAJOR_POS       = 0;
  static const int64_t MINOR_POS       = 1;
  static const int64_t MAJOR_PATCH_POS = 2;
  static const int64_t MINOR_PATCH_POS = 3;
private:
  bool is_inited_;
  const common::ObServerConfig *config_;
  const omt::ObTenantConfigMgr *tenant_config_mgr_;
  uint64_t cluster_version_;
  // ATTENTION!!! this member is only valid for unittest
  uint64_t data_version_;
};

// the version definition is moved to deps/oblib/src/common/ob_version_def.h

#define GET_MIN_CLUSTER_VERSION() (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version())

#define IS_CLUSTER_VERSION_BEFORE_4_1_0_0 (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() < CLUSTER_VERSION_4_1_0_0)
#define IS_CLUSTER_VERSION_AFTER_4_3_1_0 (oceanbase::common::ObClusterVersion::get_instance().get_cluster_version() >= CLUSTER_VERSION_4_3_1_0)

// should check returned ret
#define GET_MIN_DATA_VERSION(tenant_id, data_version) (oceanbase::common::ObClusterVersion::get_instance().get_tenant_data_version((tenant_id), (data_version)))
// !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

} // end of namespace common
} // end of namespace oceanbase

#endif /* OCEABASE_OBSERVER_OB_CLUSTER_VERSION_H_*/

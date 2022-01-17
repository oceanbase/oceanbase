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

#include "lib/hash/ob_hashmap.h"
#include "lib/ob_define.h"
#include "common/sql_mode/ob_sql_mode.h"
#include "share/ob_worker.h"

#ifndef __OB_SHARE_GET_COMPAT_MODE_H__
#define __OB_SHARE_GET_COMPAT_MODE_H__

namespace oceanbase {
namespace share {

class ObCompatModeGetter {
public:
  static ObCompatModeGetter& instance();
  // Provide global function interface
  static int get_tenant_mode(const uint64_t tenant_id, ObWorker::CompatMode& mode);
  static int check_is_oracle_mode_with_tenant_id(const uint64_t tenant_id, bool& is_oracle_mode);
  // init hash map
  int init(common::ObMySQLProxy* proxy);
  // free hash map
  void destroy();
  // According to the tenant id, get the compatibility mode of the tenant system variable,
  // the internal sql will be sent for the first time, and it will be directly read from the cache later.
  int get_tenant_compat_mode(const uint64_t tenant_id, ObWorker::CompatMode& mode);
  // only for unittest used
  int set_tenant_compat_mode(const uint64_t tenant_id, ObWorker::CompatMode& mode);
  int reset_compat_getter_map();

private:
  typedef common::hash::ObHashMap<uint64_t, ObWorker::CompatMode, common::hash::SpinReadWriteDefendMode> MAP;
  static const int64_t bucket_num = common::OB_DEFAULT_TENANT_COUNT;

private:
  MAP id_mode_map_;
  common::ObMySQLProxy* sql_proxy_;
  bool is_inited_;

private:
  ObCompatModeGetter();
  ~ObCompatModeGetter();
  DISALLOW_COPY_AND_ASSIGN(ObCompatModeGetter);
};

}  // namespace share
}  // namespace oceanbase

#endif

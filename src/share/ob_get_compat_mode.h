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
#include "common/ob_tablet_id.h"
#include "lib/worker.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

#ifndef __OB_SHARE_GET_COMPAT_MODE_H__
#define __OB_SHARE_GET_COMPAT_MODE_H__

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{

class ObCompatModeGetter
{
public:
  static ObCompatModeGetter &instance();
  //对外提供全局函数接口
  static int get_tenant_mode(const uint64_t tenant_id, lib::Worker::CompatMode& mode);
  static int get_table_compat_mode(const uint64_t tenant_id, const int64_t table_id, lib::Worker::CompatMode& mode);
  static int get_tablet_compat_mode(const uint64_t tenant_id, const common::ObTabletID &tablet_id, lib::Worker::CompatMode& mode);
  static int check_is_oracle_mode_with_tenant_id(const uint64_t tenant_id, bool &is_oracle_mode);
  static int check_is_oracle_mode_with_table_id(
             const uint64_t tenant_id,
             const int64_t table_id,
             bool &is_oracle_mode);
  //初始化哈希表
  int init(common::ObMySQLProxy *proxy);
  // Init for OBCDC
  //
  // Avoid relying on SQL when CDC consumes archive logs offline
  int init_for_obcdc();
  //释放哈希表内存
  void destroy();
  //根据租户id,拿到租户系统变量的兼容性模式,第一次拿会发内部sql,以后直接从缓存中读取
  int get_tenant_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode& mode);
  // only for unittest used
  int set_tenant_compat_mode(const uint64_t tenant_id, lib::Worker::CompatMode &mode);
  int reset_compat_getter_map();

private:
  typedef common::hash::ObHashMap<uint64_t, lib::Worker::CompatMode, common::hash::SpinReadWriteDefendMode> MAP;
  static const int64_t bucket_num = common::OB_DEFAULT_TENANT_COUNT;

private:
  MAP id_mode_map_;
  common::ObMySQLProxy *sql_proxy_;
  bool is_inited_;
  bool is_obcdc_direct_fetching_archive_log_mode_;

private:
  ObCompatModeGetter();
  ~ObCompatModeGetter();
  DISALLOW_COPY_AND_ASSIGN(ObCompatModeGetter);
};

}   //end share
}   //end oceanbase

#endif

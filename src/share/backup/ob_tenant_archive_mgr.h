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

#ifndef OCEANBASE_SHARE_OB_TENANT_ARCHIVE_MGR_H_
#define OCEANBASE_SHARE_OB_TENANT_ARCHIVE_MGR_H_

#include "share/backup/ob_archive_struct.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase
{
namespace share
{

class ObTenantArchiveMgr final
{
public:
  ObTenantArchiveMgr() {}

  // round op
  static int get_tenant_current_round(const int64_t tenant_id, const int64_t incarnation, ObTenantArchiveRoundAttr &round_attr);

  static int get_dest_round_by_dest_no(const uint64_t tenant_id, const int64_t dest_no, ObTenantArchiveRoundAttr &round_attr);

  static int is_archive_running(
      common::ObISQLClient &proxy, 
      const uint64_t tenant_id, 
      const int64_t dest_no, 
      bool &is_running);

  // piece op
  static int decide_piece_id(
      const SCN &piece_start_scn,
      const int64_t start_piece_id, 
      const int64_t piece_switch_interval, 
      const SCN &scn,
      int64_t &piece_id);
  static int decide_piece_start_scn(
      const SCN & piece_start_scn,
      const int64_t start_piece_id, 
      const int64_t piece_switch_interval, 
      const int64_t piece_id, 
      SCN &start_scn);
  static int decide_piece_end_scn(
      const SCN &piece_start_scn,
      const int64_t start_piece_id, 
      const int64_t piece_switch_interval, 
      const int64_t piece_id, 
      SCN &end_scn);
  // 'ts' must be with unit us.
  static int timestamp_to_day(const int64_t ts, int64_t &day);
};

}
}

#endif
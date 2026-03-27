/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_LOCATION_UTIL_H_
#define OCEANBASE_STORAGE_OB_LOB_LOCATION_UTIL_H_

#include "storage/lob/ob_lob_access_param.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace storage
{

class ObLobLocationUtil
{
public:
  static int get_ls_leader(
      ObLobAccessParam& param,
      const uint64_t tenant_id,
      const share::ObLSID &ls_id,
      common::ObAddr &leader);
  static int is_remote(ObLobAccessParam& param, bool& is_remote, common::ObAddr& dst_addr);
  static int lob_check_tablet_not_exist(ObLobAccessParam &param, uint64_t table_id);

  static int lob_refresh_location(ObLobAccessParam &param, int last_err, int retry_cnt);
  static int get_ls_leader(ObLobAccessParam& param);

};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_LOCATION_UTIL_H_

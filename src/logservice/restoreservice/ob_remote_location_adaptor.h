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

#ifndef OCEANBASE_LOGSERVICE_OB_REMOTE_LOCATION_ADAPTOR_H_
#define OCEANBASE_LOGSERVICE_OB_REMOTE_LOCATION_ADAPTOR_H_

#include <cstdint>
#include "lib/utility/ob_macro_utils.h"
namespace oceanbase
{
namespace common
{
class ObAddr;
}
namespace share
{
class ObLSID;
struct ObLogRestoreSourceItem;
class ObBackupDest;
}
namespace storage
{
class ObLS;
class ObLSService;
}
namespace logservice
{
class ObLogRestoreHandler;
class ObRemoteLocationAdaptor
{
public:
  ObRemoteLocationAdaptor();
  ~ObRemoteLocationAdaptor();
public:
  int init(const uint64_t tenant_id, storage::ObLSService *ls_svr);
  void destroy();
  int update_upstream(share::ObLogRestoreSourceItem &source, bool &source_exist);

private:
  bool is_tenant_primary_();
  int do_update_(const bool is_add_source, const share::ObLogRestoreSourceItem &item);
  int get_source_(share::ObLogRestoreSourceItem &item, bool &exist);
  int check_replica_status_(storage::ObLS &ls, bool &need_update);
  int clean_source_(ObLogRestoreHandler &restore_handler);
  int add_source_(const share::ObLogRestoreSourceItem &item, ObLogRestoreHandler &restore_handler);
  int add_service_source_(const share::ObLogRestoreSourceItem &item, ObLogRestoreHandler &restore_handler);
  int add_location_source_(const share::ObLogRestoreSourceItem &item, ObLogRestoreHandler &restore_handler);
  int add_rawpath_source_(const share::ObLogRestoreSourceItem &item, ObLogRestoreHandler &restore_handler);

private:
  static const int64_t LOCATION_REFRESH_INTERVAL = 5 * 1000 * 1000L;
private:
  bool inited_;
  uint64_t tenant_id_;
  int64_t last_refresh_ts_;
  storage::ObLSService *ls_svr_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRemoteLocationAdaptor);
};
} // namespace logservice
} // namespace oceanbase

#endif /* OCEANBASE_LOGSERVICE_OB_REMOTE_LOCATION_ADAPTOR_H_ */

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

#include "ob_remote_fetch_log.h"
#include "lib/net/ob_addr.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/restoreservice/ob_log_restore_define.h"
#include "ob_log_restore_archive_driver.h"    // ObLogRestoreArchiveDriver
#include "ob_log_restore_net_driver.h"        // ObLogRestoreNetDriver
#include "share/restore/ob_log_restore_source.h"

namespace oceanbase
{
namespace logservice
{
using namespace oceanbase::common;
using namespace oceanbase::share;
ObRemoteFetchLogImpl::ObRemoteFetchLogImpl() :
  inited_(false),
  tenant_id_(OB_INVALID_TENANT_ID),
  archive_driver_(NULL),
  net_driver_(NULL)
{}

ObRemoteFetchLogImpl::~ObRemoteFetchLogImpl()
{
  destroy();
}

int ObRemoteFetchLogImpl::init(const uint64_t tenant_id,
    ObLogRestoreArchiveDriver *archive_driver,
    ObLogRestoreNetDriver *net_driver)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    CLOG_LOG(WARN, "ObRemoteFetchLogImpl init twice", K(inited_));
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_ISNULL(archive_driver)
      || OB_ISNULL(net_driver)) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(tenant_id), K(archive_driver), K(net_driver));
  } else {
    tenant_id_ = tenant_id;
    archive_driver_ = archive_driver;
    net_driver_ = net_driver;
    inited_ = true;
  }
  return ret;
}

void ObRemoteFetchLogImpl::destroy()
{
  inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  archive_driver_ = NULL;
  net_driver_ = NULL;
}

int ObRemoteFetchLogImpl::do_schedule(const share::ObLogRestoreSourceItem &source)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    CLOG_LOG(WARN, "ObRemoteFetchLogImpl not init", K(inited_));
  } else if (is_service_log_source_type(source.type_)) {
    RestoreServiceAttr service_attr;
    ObSqlString value;
    if (OB_FAIL(value.assign(source.value_))) {
      CLOG_LOG(WARN, "string assign failed", K(source));
    } else if (OB_FAIL(service_attr.parse_service_attr_from_str(value))) {
      CLOG_LOG(WARN, "parse_service_attr failed", K(source));
    } else {
      ret = net_driver_->do_schedule(service_attr);
      net_driver_->set_global_recovery_scn(source.until_scn_);
    }
  } else if (is_location_log_source_type(source.type_) || is_raw_path_log_source_type(source.type_)) {
    ret = archive_driver_->do_schedule();
    archive_driver_->set_global_recovery_scn(source.until_scn_);
  } else {
    ret = OB_NOT_SUPPORTED;
    CLOG_LOG(WARN, "unsupported log source type", K(ret), K_(source.type));
  }
  net_driver_->scan_ls(source.type_);
  return ret;
}

void ObRemoteFetchLogImpl::clean_resource()
{
  net_driver_->clean_resource();
}

void ObRemoteFetchLogImpl::update_restore_upper_limit()
{
  (void)net_driver_->set_restore_log_upper_limit();
}

void ObRemoteFetchLogImpl::set_compressor_type(const common::ObCompressorType &compressor_type)
{
  (void)net_driver_->set_compressor_type(compressor_type);
}
} // namespace logservice
} // namespace oceanbase

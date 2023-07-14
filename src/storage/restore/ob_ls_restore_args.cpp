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

#define USING_LOG_PREFIX STORAGE
#include "ob_ls_restore_args.h"

using namespace oceanbase;
using namespace storage;

ObTenantRestoreCtx::ObTenantRestoreCtx()
  : job_id_(0),
    restore_type_(),
    restore_scn_(),
    consistent_scn_(),
    tenant_id_(0),
    backup_cluster_version_(0),
    backup_data_version_(0),
    backup_set_list_(),
    backup_piece_list_()
{
}

ObTenantRestoreCtx::~ObTenantRestoreCtx()
{
}

bool ObTenantRestoreCtx::is_valid() const
{
  return job_id_ > 0 && restore_type_.is_valid() && !backup_set_list_.empty();
}

int ObTenantRestoreCtx::assign(const ObTenantRestoreCtx &args)
{
  int ret = OB_SUCCESS;
  if (!args.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(args));
  } else if (OB_FAIL(backup_set_list_.assign(args.get_backup_set_list()))) {
    LOG_WARN("fail to assign backup set list", K(ret));
  } else if (OB_FAIL(backup_piece_list_.assign(args.get_backup_piece_list()))) {
    LOG_WARN("fail to assign backup piece list", K(ret));
  } else {
    job_id_ = args.get_job_id();
    restore_type_ = args.get_restore_type();
    restore_scn_ = args.get_restore_scn();
    consistent_scn_ = args.get_consistent_scn();
    tenant_id_ = args.get_tenant_id();
    backup_cluster_version_ = args.get_backup_cluster_version();
    backup_data_version_ = args.get_backup_data_version();
  }
  return ret;
}
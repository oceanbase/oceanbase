/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX STORAGE

#include "ob_i_direct_load_mgr.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::sql;

ObBaseTabletDirectLoadMgr::ObBaseTabletDirectLoadMgr()
 : ls_id_(), tablet_id_(), table_key_(), tenant_data_version_(), direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID), ref_cnt_(0)
{
}

ObBaseTabletDirectLoadMgr::~ObBaseTabletDirectLoadMgr()
{
  ls_id_.reset();
  tablet_id_.reset();
  table_key_.reset();
  tenant_data_version_ = 0;
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  ATOMIC_STORE(&ref_cnt_, 0);
}
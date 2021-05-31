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

#include "observer/virtual_table/ob_all_virtual_timestamp_service.h"
#include "lib/time/ob_time_utility.h"
#include "observer/ob_server.h"
#include "observer/ob_server_utils.h"
#include "storage/transaction/ob_ts_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::transaction;

namespace oceanbase {
namespace observer {
ObAllVirtualTimestampService::ObAllVirtualTimestampService(ObTransService* trans_service)
    : init_(false),
      trans_service_(trans_service),
      tenant_ids_index_(0),
      stc_(0),
      expire_time_(0),
      cur_tenant_id_(0),
      ts_value_(OB_INVALID_TIMESTAMP),
      ts_type_(TS_SOURCE_UNKNOWN),
      schema_service_(NULL),
      all_tenants_()
{}

ObAllVirtualTimestampService::~ObAllVirtualTimestampService()
{
  destroy();
}

void ObAllVirtualTimestampService::reset()
{
  init_ = false;
  tenant_ids_index_ = 0;
  stc_ = MonotonicTs(0);
  expire_time_ = 0;
  cur_tenant_id_ = 0;
  ts_value_ = OB_INVALID_TIMESTAMP;
  ts_type_ = TS_SOURCE_UNKNOWN;
  schema_service_ = NULL;
  all_tenants_.reset();
  ObVirtualTableScannerIterator::reset();
}

void ObAllVirtualTimestampService::destroy()
{
  trans_service_ = NULL;
  reset();
}

int ObAllVirtualTimestampService::init(ObMultiVersionSchemaService& schema_service)
{
  int ret = OB_SUCCESS;

  if (init_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "init twice", K(ret));
  } else {
    schema_service_ = &schema_service;
    init_ = true;
  }

  return ret;
}

int ObAllVirtualTimestampService::prepare_start_to_read_()
{
  int ret = OB_SUCCESS;
  const int64_t execute_timeout = 10 * 1000 * 1000;  // 10s
  if (OB_FAIL(fill_tenant_ids_())) {
    SERVER_LOG(WARN, "fail to fill tenant ids", K(ret));
  } else {
    int64_t request_ts = ObTimeUtility::current_time();
    stc_ = MonotonicTs::current_time();
    expire_time_ = request_ts + execute_timeout;
    start_to_read_ = true;
  }
  return ret;
}

int ObAllVirtualTimestampService::get_next_tenant_ts_info_()
{
  int ret = OB_SUCCESS;
  const int64_t WAIT_GTS_US = 500;
  if (tenant_ids_index_ >= all_tenants_.count()) {
    ret = OB_ITER_END;
  } else {
    cur_tenant_id_ = all_tenants_.at(tenant_ids_index_);
    ObITsMgr* ts_mgr = trans_service_->get_ts_mgr();
    do {
      if (ObTimeUtility::current_time() > expire_time_) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "get gts and type timeout, unexpected error", K(ret), K(cur_tenant_id_));
      } else if (OB_FAIL(ts_mgr->get_gts_and_type(cur_tenant_id_, stc_, ts_value_, ts_type_))) {
        if (OB_EAGAIN != ret) {
          SERVER_LOG(WARN, "fail to get gts and type", K(ret), K(cur_tenant_id_));
        } else {
          usleep(WAIT_GTS_US);
        }
      } else {
        // do nothing
      }
    } while (OB_EAGAIN == ret);
    if (OB_SUCC(ret)) {
      if (0 >= ts_value_) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid gts, unexpected error", K(ret), K_(cur_tenant_id), K_(ts_type), K_(ts_value));
      } else if (!is_valid_ts_source(ts_type_)) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "invalid ts source, unexpected error", K(ret), K_(cur_tenant_id), K_(ts_type), K_(ts_value));
      } else {
        tenant_ids_index_++;
        SERVER_LOG(DEBUG, "get tenant gts and type", K(cur_tenant_id_), K_(ts_value), K_(ts_type));
      }
    }
  }
  return ret;
}

int ObAllVirtualTimestampService::fill_tenant_ids_()
{
  int ret = OB_SUCCESS;

  ObSchemaGetterGuard schema_guard;
  if (OB_UNLIKELY(!init_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "schema_service is null", K(ret));
  } else if (OB_FAIL(schema_service_->get_schema_guard(schema_guard))) {
    SERVER_LOG(WARN, "fail to get schema guard", K(ret));
  } else if (OB_FAIL(schema_guard.get_tenant_ids(all_tenants_))) {
    SERVER_LOG(WARN, "fail to get tenant ids", K(ret));
  } else {
    SERVER_LOG(DEBUG, "succeed to get tenant ids", K(all_tenants_));
  }

  return ret;
}

int ObAllVirtualTimestampService::inner_get_next_row(ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  if (!start_to_read_ && OB_FAIL(prepare_start_to_read_())) {
    SERVER_LOG(WARN, "prepare start to read error", K(ret), K(start_to_read_));
  } else if (OB_FAIL(get_next_tenant_ts_info_())) {
    if (OB_ITER_END != ret) {
      SERVER_LOG(WARN, "ObAllVirtualTimestampService iter error", K(ret));
    } else {
      SERVER_LOG(DEBUG, "ObAllVirtualTimestampService iter end success");
    }
  } else {
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: {  // tenant_id
          cur_row_.cells_[i].set_int(cur_tenant_id_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {  // ts_type
          cur_row_.cells_[i].set_int(ts_type_);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {  // ts_value
          cur_row_.cells_[i].set_int(ts_value_);
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid coloum_id", K(ret), K(col_id));
          break;
        }
      }  // switch
    }    // for

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase

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

#include "observer/virtual_table/ob_all_virtual_server_storage.h"
#include "observer/ob_server.h"

namespace oceanbase
{
namespace observer
{
ObAllVirtualServerStorage::ObAllVirtualServerStorage()
  : ObVirtualTableScannerIterator(),
    server_storage_info_array_(),
    storage_pos_(0)
{
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
}

ObAllVirtualServerStorage::~ObAllVirtualServerStorage() { reset(); }

void ObAllVirtualServerStorage::reset()
{
  server_storage_info_array_.reset();
  storage_pos_ = 0;
  MEMSET(ip_buf_, 0, sizeof(ip_buf_));
  ObVirtualTableScannerIterator::reset();
}

int ObAllVirtualServerStorage::inner_open()
{
  int ret = OB_SUCCESS;
  ObAddr addr = GCTX.self_addr();
#ifdef OB_BUILD_SHARED_STORAGE
  SMART_VAR(ObArray<ObDeviceConfig>, device_configs) {
    if (GCTX.is_shared_storage_mode() &&
        OB_FAIL(ObDeviceConfigMgr::get_instance().get_all_device_configs(device_configs))) {
      SERVER_LOG(WARN, "fail to get all device configs", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < device_configs.count(); ++i) {
        SMART_VAR(ObServerStorageInfo, item) {
          ObDeviceConfig &tmp_device_config = device_configs.at(i);
          item.addr_ = addr;
          STRCPY(item.path_, tmp_device_config.path_);
          STRCPY(item.endpoint_, tmp_device_config.endpoint_);
          STRCPY(item.used_for_, tmp_device_config.used_for_);
          STRCPY(item.zone_, GCONF.zone.str());
          item.storage_id_ = tmp_device_config.storage_id_;
          item.max_iops_ = tmp_device_config.max_iops_;
          item.max_bandwidth_ = tmp_device_config.max_bandwidth_;
          item.create_time_ = tmp_device_config.create_timestamp_;
          item.op_id_ = tmp_device_config.op_id_;
          item.sub_op_id_ = tmp_device_config.sub_op_id_;
          STRCPY(item.authorization_, tmp_device_config.access_info_);
          STRCPY(item.encrypt_info_, tmp_device_config.encrypt_info_);
          STRCPY(item.state_, tmp_device_config.state_);
          STRCPY(item.state_info_, tmp_device_config.state_info_);
          item.last_check_timestamp_ = tmp_device_config.last_check_timestamp_;
          STRCPY(item.extension_, tmp_device_config.extension_);
          if (OB_FAIL(server_storage_info_array_.push_back(item))) {
            SERVER_LOG(WARN, "push back storage item failed", KR(ret), K(item));
          }
        }
      }
    }
  }
#endif
  return ret;
}

int ObAllVirtualServerStorage::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  row = nullptr;
  ObObj *cells = cur_row_.cells_;
  if (OB_UNLIKELY(nullptr == cells)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "not init", K(ret), KP(cur_row_.cells_));
  } else if (storage_pos_ >= server_storage_info_array_.count()) {
    row = nullptr;
    ret = OB_ITER_END;
  } else {
    ObServerStorageInfo &item = server_storage_info_array_.at(storage_pos_);
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      const uint64_t column_id = output_column_ids_.at(i);
      switch (column_id) {
        case SVR_IP: {
          if (item.addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
          break;
        }
        case SVR_PORT: {
          cells[i].set_int(item.addr_.get_port());
          break;
        }
        case PATH: {
          cells[i].set_varchar(item.path_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case ENDPOINT: {
          cells[i].set_varchar(item.endpoint_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case USED_FOR: {
          cells[i].set_varchar(item.used_for_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case ZONE: {
          cells[i].set_varchar(item.zone_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STORAGE_ID: {
          cells[i].set_int(static_cast<int64_t>(item.storage_id_));
          break;
        }
        case MAX_IOPS: {
          cells[i].set_int(static_cast<int64_t>(item.max_iops_));
          break;
        }
        case MAX_BANDWIDTH: {
          cells[i].set_int(static_cast<int64_t>(item.max_bandwidth_));
          break;
        }
        case CREATE_TIME: {
          if (is_valid_timestamp_(item.create_time_)) {
            cells[i].set_timestamp(item.create_time_);
          } else {
            // if invalid timestamp, display NULL
            cells[i].reset();
          }
          break;
        }
        case OP_ID: {
          cells[i].set_int(static_cast<int64_t>(item.op_id_));
          break;
        }
        case SUB_OP_ID: {
          cells[i].set_int(static_cast<int64_t>(item.sub_op_id_));
          break;
        }
        case AUTHORIZATION: {
          cells[i].set_varchar(item.authorization_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case ENCRYPT_INFO: {
          cells[i].set_varchar(item.encrypt_info_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STATE: {
          cells[i].set_varchar(item.state_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case STATE_INFO: {
          cells[i].set_varchar(item.state_info_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case LAST_CHECK_TIMESTAMP: {
          if (is_valid_timestamp_(item.last_check_timestamp_)) {
            cells[i].set_timestamp(item.last_check_timestamp_);
          } else {
            // if invalid timestamp, display NULL
            cells[i].reset();
          }
          break;
        }
        case EXTENSION: {
          cells[i].set_varchar(item.extension_);
          cells[i].set_collation_type(
            ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "invalid column id", KR(ret), K(column_id));
          break;
        }
      } // end switch
    } // end for-loop
    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
    ++storage_pos_;
  }
  return ret;
}

bool ObAllVirtualServerStorage::is_valid_timestamp_(const int64_t timestamp) const
{
  bool ret_bool = true;
  if (INT64_MAX == timestamp || 0 > timestamp) {
    ret_bool = false;
  }
  return ret_bool;
}

} // namespace observer
} // namespace oceanbase

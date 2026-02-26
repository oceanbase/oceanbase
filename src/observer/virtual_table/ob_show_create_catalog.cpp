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

#define USING_LOG_PREFIX SERVER
#include "observer/virtual_table/ob_show_create_catalog.h"

#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_printer.h"
#include "sql/session/ob_sql_session_info.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;
using namespace oceanbase::share;
namespace oceanbase
{
namespace observer
{

ObShowCreateCatalog::ObShowCreateCatalog() : ObVirtualTableScannerIterator() {}

ObShowCreateCatalog::~ObShowCreateCatalog() {}

void ObShowCreateCatalog::reset() { ObVirtualTableScannerIterator::reset(); }

int ObShowCreateCatalog::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(allocator_) || OB_ISNULL(schema_guard_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("data member is NULL", K(ret), K(allocator_), K(schema_guard_));
  } else {
    if (!start_to_read_) {
      const ObCatalogSchema *catalog_schema = NULL;
      uint64_t show_catalog_id = OB_INVALID_ID;
      if (OB_FAIL(calc_show_catalog_id(show_catalog_id))) {
        LOG_WARN("failed to calc show catalog id", K(ret));
      } else if (OB_UNLIKELY(OB_INVALID_ID == show_catalog_id)) {
        ret = OB_NOT_SUPPORTED;
        LOG_USER_ERROR(OB_NOT_SUPPORTED, "select a table which is used for show clause");
      } else if (OB_FAIL(schema_guard_->get_catalog_schema_by_id(effective_tenant_id_,
                 show_catalog_id, catalog_schema))) {
        LOG_WARN("failed to get catalog_schema", K(ret), K_(effective_tenant_id), K(show_catalog_id));
      } else if (OB_ISNULL(catalog_schema)) {
        ret = OB_CATALOG_NOT_EXIST;
        LOG_WARN("catalog not exist", K(ret));
      } else {
        if (OB_FAIL(fill_row_cells(show_catalog_id, catalog_schema->get_catalog_name_str()))) {
          LOG_WARN("failed to fill row cells", K(ret),
                   K(show_catalog_id), K(catalog_schema->get_catalog_name_str()));
        } else if (OB_FAIL(scanner_.add_row(cur_row_))) {
          LOG_WARN("failed to add row", K(ret), K(cur_row_));
        } else {
          scanner_it_ = scanner_.begin();
          start_to_read_ = true;
        }
      }
    }
    if (OB_LIKELY(OB_SUCCESS == ret && start_to_read_)) {
      if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
        if (OB_UNLIKELY(OB_ITER_END != ret)) {
          LOG_WARN("failed to get next row", K(ret));
        }
      } else {
        row = &cur_row_;
      }
    }
  }
  return ret;
}

int ObShowCreateCatalog::calc_show_catalog_id(uint64_t &show_catalog_id)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0;
       OB_SUCCESS == ret && OB_INVALID_ID == show_catalog_id && i < key_ranges_.count(); ++i) {
    ObRowkey start_key = key_ranges_.at(i).start_key_;
    ObRowkey end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();
    if (start_key.get_obj_cnt() > 0 && start_key.get_obj_cnt() == end_key.get_obj_cnt()) {
      if (OB_UNLIKELY(NULL == start_key_obj_ptr || NULL == end_key_obj_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("key obj ptr is NULL", K(ret), K(start_key_obj_ptr), K(end_key_obj_ptr));
      } else if (start_key_obj_ptr[0] == end_key_obj_ptr[0]
                 && ObIntType == start_key_obj_ptr[0].get_type()) {
        show_catalog_id = start_key_obj_ptr[0].get_int();
      } else {/*do nothing*/}
    }
  }
  return ret;
}

int ObShowCreateCatalog::fill_row_cells(uint64_t show_catalog_id, const ObString &catalog_name)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(cur_row_.cells_)
      || OB_ISNULL(schema_guard_)
      || OB_ISNULL(allocator_)
      || OB_ISNULL(session_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("class isn't inited", K(cur_row_.cells_), K(schema_guard_), K(allocator_), K(session_));
  } else if (OB_UNLIKELY(cur_row_.count_ < output_column_ids_.count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cur row cell count is less than output coumn", K(ret),
             K(cur_row_.count_), K(output_column_ids_.count()));
  } else {
    uint64_t cell_idx = 0;
    char *catalog_def_buf = NULL;
    int64_t catalog_def_buf_size = OB_MAX_VARCHAR_LENGTH;
    if (OB_UNLIKELY(NULL == (catalog_def_buf = static_cast<char *>(allocator_->alloc(catalog_def_buf_size))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory");
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); ++i) {
      uint64_t col_id = output_column_ids_.at(i);
      switch (col_id) {
        case OB_APP_MIN_COLUMN_ID: {
          // catalog_id
          cur_row_.cells_[cell_idx].set_int(show_catalog_id);
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 1: {
          // catalog_name
          cur_row_.cells_[cell_idx].set_varchar(catalog_name);
          cur_row_.cells_[cell_idx].set_collation_type(
              ObCharset::get_default_collation(ObCharset::get_default_charset()));
          break;
        }
        case OB_APP_MIN_COLUMN_ID + 2: {
          // create_catalog
          int64_t pos = 0;
          if (OB_FAIL(print_catalog_definition(effective_tenant_id_,
                                               show_catalog_id,
                                               catalog_def_buf,
                                               catalog_def_buf_size,
                                               pos))) {
            LOG_WARN("Generate catalog definition failed",
                     K(ret), K(effective_tenant_id_), K(show_catalog_id));
          } else {
            cur_row_.cells_[cell_idx].set_lob_value(ObLongTextType,
                                                    catalog_def_buf,
                                                    static_cast<int32_t>(pos));
            cur_row_.cells_[cell_idx].set_collation_type(
                ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid column id", K(ret), K(cell_idx),
                     K(i), K(output_column_ids_), K(col_id));
          break;
        }
      }
      if (OB_SUCC(ret)) {
        cell_idx++;
      }
    }
  }
  return ret;
}

int ObShowCreateCatalog::print_catalog_definition(const uint64_t tenant_id,
                                                  const uint64_t catalog_id,
                                                  char *buf,
                                                  const int64_t &buf_len,
                                                  int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const ObCatalogSchema *catalog_schema = NULL;
  bool is_oracle_mode = lib::is_oracle_mode();
  ObArenaAllocator allocator(ObModIds::OB_SCHEMA);
  ObSchemaPrinter schema_printer(*schema_guard_);
  if (OB_FAIL(schema_guard_->get_catalog_schema_by_id(tenant_id, catalog_id, catalog_schema))) {
    LOG_WARN("failed to get table schema", K(ret), K(tenant_id), K(catalog_id));
  } else if (OB_ISNULL(catalog_schema)) {
    ret = OB_CATALOG_NOT_EXIST;
    LOG_WARN("catalog not exists", K(ret), K(catalog_id));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     is_oracle_mode ? "CREATE EXTERNAL CATALOG "
                                                    : "CREATE EXTERNAL CATALOG IF NOT EXISTS "))) {
    LOG_WARN("failed to print create catalog prefix",
             K(ret),
             K(catalog_schema->get_catalog_name()));
  } else if (OB_FAIL(schema_printer.print_identifier(buf,
                                                     buf_len,
                                                     pos,
                                                     catalog_schema->get_catalog_name(),
                                                     is_oracle_mode))) {
    LOG_WARN("failed to print create catalog prefix",
             K(ret),
             K(catalog_schema->get_catalog_name()));
  } else if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\nPROPERTIES = (\n"))) {
    LOG_WARN("failed to print create catalog prefix",
             K(ret),
             K(catalog_schema->get_catalog_name()));
  }
  if (OB_FAIL(ret)) {
  } else if (catalog_id == OB_INTERNAL_CATALOG_ID) {
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "  TYPE = 'INTERNAL'\n) "))) {
      LOG_WARN("failed to print TYPE", K(ret));
    }
  } else {
    const ObString &properties_string = catalog_schema->get_catalog_properties_str();
    ObCatalogProperties::CatalogType catalog_type = ObCatalogProperties::CatalogType::INVALID_TYPE;
    if (OB_FAIL(ObCatalogProperties::parse_catalog_type(properties_string, catalog_type))) {
      LOG_WARN("failed to parse catalog type", K(ret));
    } else if (catalog_type == ObCatalogProperties::CatalogType::INVALID_TYPE) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid catalog type", K(ret), K(catalog_type));
    } else if (OB_FAIL(databuff_printf(
                   buf,
                   buf_len,
                   pos,
                   "  TYPE = '%s',",
                   ObCatalogProperties::CATALOG_TYPE_STR[static_cast<size_t>(catalog_type)]))) {
      LOG_WARN("failed to print TYPE", K(ret));
    } else {
      switch (catalog_type) {
        case ObCatalogProperties::CatalogType::ODPS_TYPE: {
          ObODPSCatalogProperties properties;
          if (OB_FAIL(properties.load_from_string(properties_string, allocator))) {
            LOG_WARN("failed to load from string", K(ret));
          } else if (OB_FAIL(print_odps_catalog_definition(properties, buf, buf_len, pos))) {
            LOG_WARN("failed to print odps catalog definition", K(ret));
          }
          break;
        }
        case ObCatalogProperties::CatalogType::FILESYSTEM_TYPE: {
          ObFilesystemCatalogProperties properties;
          if (OB_FAIL(properties.load_from_string(properties_string, allocator))) {
            LOG_WARN("failed to load from string", K(ret));
          } else if (OB_FAIL(print_filesystem_catalog_definition(properties, buf, buf_len, pos))) {
            LOG_WARN("failed to print odps catalog definition", K(ret));
          }
          break;
        }
        case ObCatalogProperties::CatalogType::HMS_TYPE: {
          ObHMSCatalogProperties properties;
          if (OB_FAIL(properties.load_from_string(properties_string, allocator))) {
            LOG_WARN("failed to load from string", K(ret));
          } else if (OB_FAIL(print_hive_catalog_definition(properties, buf, buf_len, pos))) {
            LOG_WARN("failed to print hms catalog definition", K(ret));
          }
          break;
        }
        default: {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid catalog type", K(ret), K(catalog_type));
        }
      }
    }
  }
  LOG_DEBUG("print catalog schema", K(ret), K(*catalog_schema));
  return ret;
}

int ObShowCreateCatalog::print_odps_catalog_definition(const ObODPSCatalogProperties &odps,
                                                       char *buf,
                                                       const int64_t &buf_len,
                                                       int64_t &pos) const
{
  int ret = OB_SUCCESS;
  ObString secret_str("********");
  int64_t option_names_idx = 0;
  if (OB_FAIL(databuff_printf(buf,
                              buf_len,
                              pos,
                              "\n  %s = '%.*s',",
                              ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                  ObODPSCatalogProperties::ObOdpsCatalogOptions::ACCESSTYPE)],
                              odps.access_type_.length(),
                              odps.access_type_.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "\n  %s = '%.*s',",
                                     ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                         ObODPSCatalogProperties::ObOdpsCatalogOptions::ACCESSID)],
                                     secret_str.length(),
                                     secret_str.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "\n  %s = '%.*s',",
                                     ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                         ObODPSCatalogProperties::ObOdpsCatalogOptions::ACCESSKEY)],
                                     secret_str.length(),
                                     secret_str.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "\n  %s = '%.*s',",
                                     ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                         ObODPSCatalogProperties::ObOdpsCatalogOptions::STSTOKEN)],
                                     secret_str.length(),
                                     secret_str.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(databuff_printf(buf,
                                     buf_len,
                                     pos,
                                     "\n  %s = '%.*s',",
                                     ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                         ObODPSCatalogProperties::ObOdpsCatalogOptions::ENDPOINT)],
                                     odps.endpoint_.length(),
                                     odps.endpoint_.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(databuff_printf(
                 buf,
                 buf_len,
                 pos,
                 "\n  %s = '%.*s',",
                 ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                     ObODPSCatalogProperties::ObOdpsCatalogOptions::TUNNEL_ENDPOINT)],
                 odps.tunnel_endpoint_.length(),
                 odps.tunnel_endpoint_.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(
                 databuff_printf(buf,
                                 buf_len,
                                 pos,
                                 "\n  %s = '%.*s',",
                                 ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                     ObODPSCatalogProperties::ObOdpsCatalogOptions::PROJECT_NAME)],
                                 odps.project_.length(),
                                 odps.project_.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(
                 databuff_printf(buf,
                                 buf_len,
                                 pos,
                                 "\n  %s = '%.*s',",
                                 ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                     ObODPSCatalogProperties::ObOdpsCatalogOptions::QUOTA_NAME)],
                                 odps.quota_.length(),
                                 odps.quota_.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (OB_FAIL(databuff_printf(
                 buf,
                 buf_len,
                 pos,
                 "\n  %s = '%.*s',",
                 ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                     ObODPSCatalogProperties::ObOdpsCatalogOptions::COMPRESSION_CODE)],
                 odps.compression_code_.length(),
                 odps.compression_code_.ptr()))) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  } else if (databuff_printf(buf,
                             buf_len,
                             pos,
                             "\n  %s = '%.*s',",
                             ObODPSCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                                 ObODPSCatalogProperties::ObOdpsCatalogOptions::REGION)],
                             odps.region_.length(),
                             odps.region_.ptr())) {
    LOG_WARN("failed to print ODPS_INFO", K(ret));
  }
  if (OB_SUCC(ret)) {
    --pos;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      LOG_WARN("failed to print )", K(ret));
    }
  }
  return ret;
}

int ObShowCreateCatalog::print_filesystem_catalog_definition(
    const share::ObFilesystemCatalogProperties &properties,
    char *buf,
    const int64_t &buf_len,
    int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(
            buf,
            buf_len,
            pos,
            "\n  %s = '%.*s',",
            ObFilesystemCatalogProperties::OPTION_NAMES[static_cast<size_t>(
                ObFilesystemCatalogProperties::ObFilesystemCatalogOptions::WAREHOUSE)],
            properties.warehouse_.length(),
            properties.warehouse_.ptr()))) {
      LOG_WARN("failed to print filesystem catalog info", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    --pos;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      LOG_WARN("failed to print )", K(ret));
    }
  }
  return ret;
}

int ObShowCreateCatalog::print_hive_catalog_definition(
    const ObHMSCatalogProperties &hms, char *buf, const int64_t &buf_len,
    int64_t &pos) const {
  int ret = OB_SUCCESS;
  if (OB_SUCC(ret)) {
    if (OB_FAIL(databuff_printf(buf,
                                buf_len,
                                pos,
                                "\n  %s = '%.*s',",
                                ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::URI],
                                hms.uri_.length(),
                                hms.uri_.ptr()))) {
      LOG_WARN("failed to print HMS_INFO", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (hms.principal_.length() > 0) {
      if (OB_FAIL(databuff_printf(
              buf,
              buf_len,
              pos,
              "\n  %s = '%.*s',",
              ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::PRINCIPAL],
              hms.principal_.length(),
              hms.principal_.ptr()))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hms.keytab_.length() > 0) {
      if (OB_FAIL(
              databuff_printf(buf,
                              buf_len,
                              pos,
                              "\n  %s = '%.*s',",
                              ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::KEYTAB],
                              hms.keytab_.length(),
                              hms.keytab_.ptr()))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hms.krb5conf_.length() > 0) {
      if (OB_FAIL(databuff_printf(
              buf,
              buf_len,
              pos,
              "\n  %s = '%.*s',",
              ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::KRB5CONF],
              hms.krb5conf_.length(),
              hms.krb5conf_.ptr()))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hms.max_client_pool_size_ > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = %ld,",
                                  ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::MAX_CLIENT_POOL_SIZE],
                                  hms.max_client_pool_size_))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hms.socket_timeout_ > 0) {
      if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n  %s = %ld,",
                                  ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::SOCKET_TIMEOUT],
                                  hms.socket_timeout_))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hms.is_set_cache_refresh_interval_sec()) {
      if (OB_FAIL(databuff_printf(buf,
                                  buf_len,
                                  pos,
                                  "\n  %s = %ld,",
                                  ObHMSCatalogProperties::OPTION_NAMES
                                      [ObHMSCatalogProperties::CACHE_REFRESH_INTERVAL_SEC],
                                  hms.get_cache_refresh_interval_sec()))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (hms.hms_catalog_name_.length() > 0) {
      if (OB_FAIL(databuff_printf(
              buf,
              buf_len,
              pos,
              "\n  %s = '%.*s',",
              ObHMSCatalogProperties::OPTION_NAMES[ObHMSCatalogProperties::HMS_CATALOG_NAME],
              hms.hms_catalog_name_.length(),
              hms.hms_catalog_name_.ptr()))) {
        LOG_WARN("failed to print HMS_INFO", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    --pos;
    if (OB_FAIL(databuff_printf(buf, buf_len, pos, "\n) "))) {
      LOG_WARN("failed to print )", K(ret));
    }
  }
  return ret;
}

} // namespace observer
} // namespace oceanbase

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

#include "lib/allocator/ob_allocator.h"
#include "observer/virtual_table/ob_all_virtual_plugin_info.h"
#include "observer/ob_server.h"
#include "plugin/sys/ob_plugin_handle.h"
#include "plugin/sys/ob_plugin_dl_handle.h"
#include "plugin/sys/ob_plugin_entry_handle.h"
#include "plugin/sys/ob_plugin_utils.h"
#include "plugin/sys/ob_plugin_mgr.h"
#include "plugin/adaptor/ob_plugin_adaptor.h"

using namespace oceanbase::plugin;
using namespace oceanbase::common;

namespace oceanbase {
namespace observer {

enum class ObPluginInfoColumn : int64_t {
  SVR_IP = common::OB_APP_MIN_COLUMN_ID,
  SVR_PORT,
  NAME,
  STATUS,
  TYPE,
  LIBRARY,
  LIBRARY_VERSION,
  LIBRARY_REVISION,
  INTERFACE_VERSION,
  AUTHOR,
  LICENSE,
  DESCRIPTION
};

ObAllVirtualPluginInfo::ObAllVirtualPluginInfo() {}

ObAllVirtualPluginInfo::~ObAllVirtualPluginInfo() { reset(); }

void ObAllVirtualPluginInfo::reset() {
  addr_.reset();
  plugin_entries_.reset();
  iter_index_ = -1;
}

int ObAllVirtualPluginInfo::inner_open()
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "Plugin");

  if (FALSE_IT(plugin_entries_.set_attr(mem_attr))) {
  } else if (OB_FAIL(GCTX.plugin_mgr_->list_all_plugin_entries(plugin_entries_))) {
    SERVER_LOG(WARN, "failed to init tenant plugin array");
  } else {
    iter_index_ = 0;
  }

  return ret;
}

int ObAllVirtualPluginInfo::inner_close() {
  int ret = OB_SUCCESS;
  plugin_entries_.reset();
  iter_index_ = -1;
  return ret;
}

int ObAllVirtualPluginInfo::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObPluginEntryHandle *entry_handle = nullptr;
  if (iter_index_ >= 0 && iter_index_ < plugin_entries_.count()) {
    if (OB_FAIL(plugin_entries_.at(iter_index_, entry_handle))) {
      LOG_WARN("failed to get plugin entry handle from plugin entries",
               K(iter_index_), K(ret));
    } else if (OB_ISNULL(entry_handle)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "iterate plugin handle success but got null",
                 K(ret), K(iter_index_), K(plugin_entries_.count()));
    }
    iter_index_++;
  } else {
    ret = OB_ITER_END;
  }

  if (OB_FAIL(ret)) {
  } else {
    const int64_t version_length = 30;
    const ObPluginEntry &plugin_entry = entry_handle->entry();
    ObPluginHandle *plugin_handle = plugin_entry.plugin_handle;
    ObObj *cells = cur_row_.cells_;
    const int64_t col_count = output_column_ids_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
      const ObPluginInfoColumn col_id = static_cast<ObPluginInfoColumn>(output_column_ids_.at(i));
      switch (col_id) {
        case ObPluginInfoColumn::SVR_IP: {
          if (addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
            cells[i].set_varchar(ip_buf_);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "fail to execute ip_to_string", K(ret));
          }
        } break;

        case ObPluginInfoColumn::SVR_PORT: {
          cells[i].set_int(addr_.get_port());
        } break;

        case ObPluginInfoColumn::NAME: {
          cells[i].set_varchar(plugin_entry.name);
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } break;

        case ObPluginInfoColumn::STATUS: {
          if (OB_NOT_NULL(entry_handle)) {
            cells[i].set_varchar(ob_plugin_state_to_string(entry_handle->state()));
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cells[i].set_null();
          }
        } break;

        case ObPluginInfoColumn::TYPE: {
          cells[i].set_varchar(ob_plugin_type_to_string(plugin_entry.interface_type));
          cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        } break;

        case ObPluginInfoColumn::LIBRARY: {
          ObPluginDlHandle *dl_handle = nullptr;
          if (OB_ISNULL(plugin_handle) ||
              OB_ISNULL(dl_handle = plugin_handle->dl_handle())) {
            cells[i].set_null();
          } else {
            ObString dl_name = dl_handle->name();
            cells[i].set_varchar(dl_name);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } break;

        case ObPluginInfoColumn::LIBRARY_VERSION: {
          char *version_buffer = nullptr;
          if (OB_ISNULL(plugin_handle) || OB_ISNULL(plugin_handle->plugin()) || OB_ISNULL(allocator_) ||
              OB_ISNULL(version_buffer = static_cast<char *>(allocator_->alloc(version_length)))) {
            cells[i].set_null();
          } else {
            ObPluginVersionAdaptor plugin_version(plugin_handle->plugin()->version);
            plugin_version.to_string(version_buffer, version_length);

            cells[i].set_varchar(version_buffer);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } break;

        case ObPluginInfoColumn::LIBRARY_REVISION: {
          if (OB_ISNULL(plugin_handle) || OB_ISNULL(plugin_handle->build_revision())) {
            cells[i].set_null();
          } else {
            cells[i].set_varchar(plugin_handle->build_revision());
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } break;

        case ObPluginInfoColumn::INTERFACE_VERSION: {
          void *descriptor = nullptr;
          char *version_buffer = nullptr;
          uint64_t version = 0;
          if (OB_ISNULL(allocator_) ||
              OB_ISNULL(version_buffer = static_cast<char *>(allocator_->alloc(version_length)))) {
            cells[i].set_null();
          } else {
            ObPluginVersionAdaptor plugin_version(plugin_entry.interface_version);
            plugin_version.to_string(version_buffer, version_length);

            cells[i].set_varchar(version_buffer);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        } break;

        case ObPluginInfoColumn::AUTHOR: {
          if (OB_NOT_NULL(plugin_handle) &&
              OB_NOT_NULL(plugin_handle->plugin()) &&
              OB_NOT_NULL(plugin_handle->plugin()->author)) {
            cells[i].set_varchar(plugin_handle->plugin()->author);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cells[i].set_null();
          }
        } break;

        case ObPluginInfoColumn::LICENSE: {
          if (OB_NOT_NULL(plugin_handle) &&
              OB_NOT_NULL(plugin_handle->plugin()) &&
              OB_NOT_NULL(plugin_handle->plugin()->license)) {
            cells[i].set_varchar(plugin_handle->plugin()->license);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cells[i].set_null();
          }
        } break;

        case ObPluginInfoColumn::DESCRIPTION: {
          if (OB_NOT_NULL(plugin_entry.description)) {
            cells[i].set_varchar(plugin_entry.description);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          } else {
            cells[i].set_null();
          }
        } break;

        default: {
          SERVER_LOG(WARN, "unknown cell id", K(i));
          cells[i].set_null();
        }
      }
    }
  }

  if (OB_SUCC(ret)) {
    row = &cur_row_;
  }

  return ret;
}

} // namespace observer
} // namespace oceanbase

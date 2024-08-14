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

#include "ob_mem_leak_checker_info.h"
#include "lib/allocator/ob_mem_leak_checker.h"
#include "common/object/ob_object.h"
#include "share/config/ob_server_config.h"

using namespace oceanbase::common;
using oceanbase::common::ObMemLeakChecker;

namespace oceanbase
{
namespace observer
{
ObMemLeakCheckerInfo::ObMemLeakCheckerInfo()
  : ObVirtualTableIterator(),
    opened_(false),
    addr_(NULL),
    tenant_id_(-1)
{
  leak_checker_ = &get_mem_leak_checker();
  label_ = leak_checker_->get_str();
}

ObMemLeakCheckerInfo::~ObMemLeakCheckerInfo()
{
  reset();
}

void ObMemLeakCheckerInfo::reset()
{
  opened_ = false;
  leak_checker_ = NULL;
  addr_ = NULL;
  tenant_id_ = -1;
  label_ = nullptr;
}

int ObMemLeakCheckerInfo::sanity_check()
{
  int ret = OB_SUCCESS;
  if (NULL == leak_checker_ || NULL == addr_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "invalid argument", K_(leak_checker), K_(addr));
  }
  return ret;
}
int ObMemLeakCheckerInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(sanity_check())) {
    // error
  } else if (!opened_) {
    int ret = info_map_.create(10000);
    if (OB_FAIL(ret)) {
      SERVER_LOG(WARN, "failed to create hashmap", K(ret));
    } else if (OB_FAIL(leak_checker_->load_leak_info_map(info_map_))) {
      SERVER_LOG(WARN, "failed to collection leak info", K(ret));
    } else {
      opened_ = true;
      it_ = info_map_->begin();
    }
  }

  if (OB_SUCC(ret)) {
    if (it_ != info_map_->end()) {
      if (OB_FAIL(fill_row(row))) {
        SERVER_LOG(WARN, "failed to fill row", K(ret));
      }
      it_++;
    } else {
      ret = OB_ITER_END;
    }
  }

  return ret;
}

int ObMemLeakCheckerInfo::fill_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = NULL;
  if (OB_FAIL(sanity_check())) {
    // error
  } else if (OB_ISNULL(cells = cur_row_.cells_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "cur row cell is NULL", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < output_column_ids_.count(); i++) {
      uint64_t col_id = output_column_ids_.at(i);
      switch(col_id) {
      case 16: {
        //svr_ip
        char ipbuf[common::OB_IP_STR_BUFF];
        if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
          SERVER_LOG(WARN, "ip to string failed");
          ret = OB_ERR_UNEXPECTED;
        } else {
          ObString ipstr = ObString::make_string(ipbuf);
          if (OB_FAIL(ob_write_string(*allocator_, ipstr, ipstr))) {
            SERVER_LOG(WARN, "write string failed", K(ret));
          } else {
            cells[i].set_varchar(ipstr);
            cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
          }
        }
      } break;
      case 17: {
        // svr_port
        const int32_t port = addr_->get_port();
        cells[i].set_int(port);
      } break;
      case 18: {
        //mod_name
        cells[i].set_varchar(label_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } break;
      case 19: {
        //mod_type
        cells[i].set_varchar(ObString::make_string("user"));
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } break;
      case 20: {
        // alloc_count
        cells[i].set_int(it_->second.first);
      } break;
      case 21: {
        // alloc_size
        cells[i].set_int(it_->second.second);
      } break;
      case 22: {
        // back_trace
        cells[i].set_varchar(it_->first.bt_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } break;

      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN,
                   "invalid column id",
                   K(ret),
                   K(i),
                   K_(label));
        break;
      }
      }
    }

    if (OB_SUCC(ret)) {
      row = &cur_row_;
    }
  }

  return ret;
}
}
}

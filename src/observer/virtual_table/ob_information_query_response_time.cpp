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

#include "ob_information_query_response_time.h"
#include "lib/container/ob_array_serialization.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_utils.h"
#include "observer/ob_server_struct.h"
#include "observer/ob_server.h"
#include "observer/omt/ob_tenant_timezone_mgr.h"

using namespace oceanbase::common;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace observer {
ObInfoSchemaQueryResponseTimeTable::ObInfoSchemaQueryResponseTimeTable()
 : ObVirtualTableScannerIterator(), 
 addr_(NULL),
 ipstr_(),
 port_(0),
 tenant_id_(OB_INVALID_ID),
 collector_iter_(),
 utility_iter_(0)
{
}

ObInfoSchemaQueryResponseTimeTable::~ObInfoSchemaQueryResponseTimeTable()
{
  reset();
}

void ObInfoSchemaQueryResponseTimeTable::reset()
{
  ObVirtualTableScannerIterator::reset();
  addr_ = NULL;
  port_ = 0;
  ipstr_.reset();
  tenant_id_ = OB_INVALID_ID;
  collector_iter_ = common::hash::ObHashMap<uint64_t, ObRSTTimeCollector*>::iterator();
  utility_iter_ = 0;
}

int ObInfoSchemaQueryResponseTimeTable::set_ip(common::ObAddr* addr)
{
  int ret = OB_SUCCESS;
  char ipbuf[common::OB_IP_STR_BUFF];
  if (NULL == addr) {
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(ipbuf, sizeof(ipbuf))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(ipbuf);
    if (OB_FAIL(ob_write_string(*allocator_, ipstr_, ipstr_))) {
      SERVER_LOG(WARN, "failed to write string", K(ret));
    }
    port_ = addr_->get_port();
  }
  return ret;
}

int ObInfoSchemaQueryResponseTimeTable::inner_open()
{
  int ret = OB_SUCCESS;
  auto& collector = ObRSTCollector::get_instance();
  if(0 == collector.collector_map_.size()){
    SERVER_LOG(WARN, "query response time size is 0");
  } else {
    collector_iter_ = collector.collector_map_.begin();
    if (OB_FAIL(ret = set_ip(addr_))) {
      SERVER_LOG(WARN, "can't get ip", K(ret));
    }
  }
  return ret;
}

int ObInfoSchemaQueryResponseTimeTable::inner_get_next_row(common::ObNewRow*& row)
{
  int ret = OB_SUCCESS;

  ObObj* cells = cur_row_.cells_;
  if (OB_UNLIKELY(NULL == allocator_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "allocator is NULL", K(ret));
  } else if (OB_UNLIKELY(NULL == schema_guard_)) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "schema manager is NULL", K(ret));
  } else if(0 == ObRSTCollector::get_instance().collector_map_.size()){
      ret = OB_ITER_END;
      SERVER_LOG(WARN, "query response time size is 0");
  } else if (collector_iter_ == ObRSTCollector::get_instance().collector_map_.end()){
      ret = OB_ITER_END;
  } else {
    const int64_t col_count = output_column_ids_.count();
    if (OB_SUCC(ret)){
      uint64_t cell_idx = 0;
      for (int64_t j = 0; OB_SUCC(ret) && j < col_count; ++j) {
        uint64_t col_id = output_column_ids_.at(j);
        switch (col_id){
          case TENANT_ID:{
            tenant_id_ = collector_iter_->first;
            cells[cell_idx].set_int(tenant_id_);
            break;
          }
          case SVR_IP: {
            cells[cell_idx].set_varchar(ipstr_);
            break;
          }
          case SVR_PORT: {
            cells[cell_idx].set_int(port_);
            break;
          }
          case QUERY_RESPPONSE_TIME:{
            cells[cell_idx].set_int(collector_iter_->second->bound(utility_iter_));
            break;
          }
          case COUNT: {
            cells[cell_idx].set_int(collector_iter_->second->count(utility_iter_));
            break;
          }
          case TOTAL: {
            cells[cell_idx].set_int(collector_iter_->second->total(utility_iter_));
            break;
          }
          default: {
            ret = OB_ERR_UNEXPECTED;
            SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(output_column_ids_), K(col_id));
            break;
          }
        }

        if (OB_SUCC(ret)) {
          cell_idx++;
        }
      }
    }


    if (OB_SUCC(ret)) {
      row = &cur_row_;
      utility_iter_++;
      if(utility_iter_ == collector_iter_->second->bound_count()){
        collector_iter_++;
        utility_iter_ = 0;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase

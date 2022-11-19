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

#include "ob_virtual_obrpc_send_stat.h"
#include "rpc/obrpc/ob_rpc_packet.h"
#include "rpc/obrpc/ob_rpc_stat.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_utils.h"
#include "lib/alloc/memory_dump.h"
#include "observer/ob_server.h"

using namespace oceanbase::rpc;
using namespace oceanbase::obrpc;
using namespace oceanbase::common;
using namespace oceanbase::observer;

ObVirtualObRpcSendStat::ObVirtualObRpcSendStat()
    : pcode_idx_(0), tenant_idx_(0), tenant_cnt_(0), tenant_ids_(nullptr, ObModIds::OMT), has_start_(false)
{}

ObVirtualObRpcSendStat::~ObVirtualObRpcSendStat()
{
  reset();
}

void ObVirtualObRpcSendStat::reset()
{
  has_start_ = false;
}

int ObVirtualObRpcSendStat::inner_get_next_row(ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObObj *cells = cur_row_.cells_;
  if (!has_start_) {
    // sys tenant show all tenant infos
    if (is_sys_tenant(effective_tenant_id_)) {
      omt::ObMultiTenant *omt = GCTX.omt_;
      if (OB_ISNULL(omt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("omt is null", K(ret));
      } else {
        omt->get_tenant_ids(tenant_ids_);
        tenant_cnt_ = tenant_ids_.size();
      }
    } else {
    // user tenant show self tenant infos
      tenant_ids_.clear();
      tenant_ids_.push_back(effective_tenant_id_);
      tenant_cnt_ = 1;
    }
    has_start_ = true;
  }
  if (pcode_idx_ < ObRpcPacketSet::THE_PCODE_COUNT) {
    if (OB_LIKELY(NULL != cells)) {
      ObRpcPacketSet &set = ObRpcPacketSet::instance();
      RpcStatItem item;
      if (OB_SUCC(RPC_STAT_GET(pcode_idx_, tenant_ids_.at(tenant_idx_), item))) {
        const int64_t col_count = output_column_ids_.count();
        const ObRpcPacketCode pcode = set.pcode_of_idx(pcode_idx_);
        const char *pcode_name = set.name_of_idx(pcode_idx_);

        for (int64_t i = 0; OB_SUCC(ret) && i < col_count; i++) {
          const uint64_t col_id = output_column_ids_.at(i);
          switch (col_id) {
            case TENANT_ID:
              cells[i].set_int(tenant_ids_.at(tenant_idx_));
              break;
            case SVR_IP: {
              ObString ipstr;
              if (OB_FAIL(ObServerUtils::get_server_ip(allocator_, ipstr))) {
                LOG_WARN("get server ip failed", K(ret));
              } else {
                cells[i].set_varchar(ipstr);
                cells[i].set_collation_type(
                    ObCharset::get_default_collation(ObCharset::get_default_charset()));
              }
              break;
            }
            case SVR_PORT:
              cells[i].set_int(GCONF.self_addr_.get_port());
              break;
            case DEST_IP: {
              cells[i].set_varchar(ObString::make_string("0.0.0.0"));
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case DEST_PORT: {
              cells[i].set_int(0);
              break;
            }
            case INDEX:
              cells[i].set_int(pcode_idx_);
              break;
            case ZONE: {
              cells[i].set_varchar(GCONF.zone);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case PCODE:
              cells[i].set_int(pcode);
              break;
            case PCODE_NAME: {
              cells[i].set_varchar(pcode_name);
              cells[i].set_collation_type(
                  ObCharset::get_default_collation(ObCharset::get_default_charset()));
              break;
            }
            case COUNT:
              cells[i].set_int(item.count_);
              break;
            case TOTAL_TIME:
              cells[i].set_int(item.time_);
              break;
            case TOTAL_SIZE:
              cells[i].set_int(item.size_);
              break;
            case MAX_TIME:
              cells[i].set_int(item.max_rt_);
              break;
            case MIN_TIME:
              cells[i].set_int(item.min_rt_);
              break;
            case MAX_SIZE:
              cells[i].set_int(item.max_sz_);
              break;
            case MIN_SIZE:
              cells[i].set_int(item.min_sz_);
              break;
            case FAILURE:
              cells[i].set_int(item.failures_);
              break;
            case TIMEOUT:
              cells[i].set_int(item.timeouts_);
              break;
            case SYNC:
              cells[i].set_int(item.sync_);
              break;
            case ASYNC:
              cells[i].set_int(item.async_);
              break;
            case LAST_TIMESTAMP:
              cells[i].set_timestamp(item.last_ts_);
              break;
            case ISIZE:
              cells[i].set_int(item.isize_);
              break;
            case ICOUNT:
              cells[i].set_int(item.icount_);
              break;
            case NET_TIME:
              cells[i].set_int(item.net_time_);
              break;
            case WAIT_TIME:
              cells[i].set_int(item.wait_time_);
              break;
            case QUEUE_TIME:
              cells[i].set_int(item.queue_time_);
              break;
            case PROCESS_TIME:
              cells[i].set_int(item.process_time_);
              break;
            case ILAST_TIMESTAMP:
              cells[i].set_timestamp(item.ilast_ts_);
              break;
            case DCOUNT:
              cells[i].set_int(item.dcount_);
              break;
            default:
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected column id", K(col_id), K(i), K(ret));
              break;
          }
        }
        row = &cur_row_;
        pcode_idx_++;
        if (pcode_idx_ >= ObRpcPacketSet::THE_PCODE_COUNT && tenant_idx_ < tenant_cnt_ - 1) {
          pcode_idx_ = 0;
          tenant_idx_++;
        }
      } else if (OB_ENTRY_NOT_EXIST == ret) {
        if (tenant_idx_ < tenant_cnt_ - 1) {
          pcode_idx_ = 0;
          tenant_idx_++;
          ret = OB_SUCCESS;
        } else {
          ret = OB_ITER_END;
        }
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("current row is NULL", K(ret));
    }
  } else {
    ret = OB_ITER_END;
  }

  return ret;
}

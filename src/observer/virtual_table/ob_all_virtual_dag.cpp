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
#include "ob_all_virtual_dag.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
using namespace common;
using namespace share;
namespace observer
{

/*
 * ObDagInfoIterator Func
 * */

template <typename T>
int ObDagInfoIterator<T>::open(const int64_t tenant_id)
{
  int ret = common::OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "The ObDagInfoIterator has been opened", K(ret));
  } else if (typeid(T) != typeid(share::ObDagInfo) && typeid(T) != typeid(share::ObDagSchedulerInfo)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "invalid typeid", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_SYS_TENANT_ID == tenant_id) { // sys tenant can get all tenants' info
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) { // non-sys tenant
    STORAGE_LOG(WARN, "failed to push back", K(ret), K(tenant_id));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    uint64_t tenant_id = all_tenants[i];
    if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      MTL_SWITCH(tenant_id) {
        if (typeid(T) == typeid(share::ObDagInfo)) {
          if (OB_FAIL(MTL(ObTenantDagScheduler *)->get_all_dag_info(allocator_, all_tenants_dag_infos_))) {
            STORAGE_LOG(WARN, "failed to get all dag info", K(ret));
          }
        } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->get_all_dag_scheduler_info(allocator_, all_tenants_dag_infos_))) {
          STORAGE_LOG(WARN, "failed to get all dag info", K(ret));
        }
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  } // end of for
  if (OB_SUCC(ret)) {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

template <typename T>
int ObDagInfoIterator<T>::get_next_info(T &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (cur_idx_ >= all_tenants_dag_infos_.count()) {
    ret = OB_ITER_END;
  } else {
    info = *(static_cast<T*>(all_tenants_dag_infos_[cur_idx_++]));
  }
  return ret;
}

template <typename T>
void ObDagInfoIterator<T>::reset()
{
  all_tenants_dag_infos_.reset();
  cur_idx_ = 0;
  allocator_.reset();
  is_opened_ = false;
}

ObAllVirtualDag::ObAllVirtualDag()
    : dag_info_(),
      dag_info_iter_(),
      is_inited_(false)
{
}
ObAllVirtualDag::~ObAllVirtualDag()
{
  reset();
}

int ObAllVirtualDag::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualDag has been inited, ", K(ret));
  } else if (OB_FAIL(dag_info_iter_.open(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to open merge info iter, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}


int ObAllVirtualDag::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualDag has been inited, ", K(ret));
  } else if (OB_FAIL(dag_info_iter_.get_next_info(dag_info_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next merge info, ", K(ret));
    }
  } else if (OB_FAIL(fill_cells(dag_info_))) {
    STORAGE_LOG(WARN, "Fail to fill cells, ", K(ret), K(dag_info_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualDag::fill_cells(share::ObDagInfo &dag_info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t n = 0;
  if (!dag_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid dag info, ", K(ret), K(dag_info));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      //svr_ip
      if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case SVR_PORT:
      //svr_port
      cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
      break;
    case TENANT_ID:
      cells[i].set_int(dag_info.tenant_id_);
      break;
    case DAG_TYPE:
      //dag type
      if (dag_info.dag_type_ >= ObDagType::DAG_TYPE_MINI_MERGE && dag_info.dag_type_ < ObDagType::DAG_TYPE_MAX) {
        cells[i].set_varchar(share::ObIDag::get_dag_type_str(dag_info.dag_type_));
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } else if (dag_info.dag_net_type_ >= ObDagNetType::DAG_NET_TYPE_MIGRATION
          && dag_info.dag_net_type_ < ObDagNetType::DAG_NET_TYPE_MAX) {
        cells[i].set_varchar(share::ObIDagNet::get_dag_net_type_str(dag_info.dag_net_type_));
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      } else {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected dag info, ", K(ret), K(dag_info));
      }
      break;
    case DAG_KEY:
      //dag key
      cells[i].set_varchar(dag_info.dag_key_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case DAG_NET_KEY:
      //dag_net key
      cells[i].set_varchar(dag_info.dag_net_key_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case DAG_ID:
      //dag_id
      n = dag_info.dag_id_.to_string(dag_id_buf_, sizeof(dag_id_buf_));
      if (n < 0 || n >= sizeof(dag_id_buf_)) {
        ret = OB_BUF_NOT_ENOUGH;
        SERVER_LOG(WARN, "buffer not enough", K(ret));
      } else {
        cells[i].set_varchar(dag_id_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case DAG_STATUS:
      // dag_status
      cells[i].set_varchar(share::ObIDag::get_dag_status_str(dag_info.dag_status_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case RUNNING_TASK_CNT:
      //running task count
      cells[i].set_int(dag_info.running_task_cnt_);
      break;
    case ADD_TIME:
      //add time
      cells[i].set_timestamp(dag_info.add_time_);
      break;
    case START_TIME:
      //start time
      cells[i].set_timestamp(dag_info.start_time_);
      break;
    case INDEGREE:
      //indegree
      cells[i].set_int(dag_info.indegree_);
      break;
    case COMMENT: {
      //merge_type
      cells[i].set_varchar(dag_info.comment_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    }
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
    }
  }

  return ret;
}

void ObAllVirtualDag::reset()
{
  ObVirtualTableScannerIterator::reset();
  dag_info_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(dag_id_buf_, 0, sizeof(dag_id_buf_));
}

ObAllVirtualDagScheduler::ObAllVirtualDagScheduler()
    : dag_scheduler_info_(),
      dag_scheduler_info_iter_(),
      is_inited_(false)
{
}
ObAllVirtualDagScheduler::~ObAllVirtualDagScheduler()
{
  reset();
}

int ObAllVirtualDagScheduler::init()
{
  int ret = OB_SUCCESS;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualDagScheduler has been inited, ", K(ret));
  } else if (OB_FAIL(dag_scheduler_info_iter_.open(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to open merge info iter, ", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}


int ObAllVirtualDagScheduler::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualDagScheduler has been inited, ", K(ret));
  } else if (OB_FAIL(dag_scheduler_info_iter_.get_next_info(dag_scheduler_info_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next merge info, ", K(ret));
    }
  } else if (OB_FAIL(fill_cells(dag_scheduler_info_))) {
    STORAGE_LOG(WARN, "Fail to fill cells, ", K(ret), K(dag_scheduler_info_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualDagScheduler::fill_cells(share::ObDagSchedulerInfo &dag_scheduler_info)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t n = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      //svr_ip
      if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case SVR_PORT:
      //svr_port
      cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
      break;
    case TENANT_ID:
      cells[i].set_int(dag_scheduler_info.tenant_id_);
      break;
    case VALUE_TYPE:
      cells[i].set_varchar(share::ObDagSchedulerInfo::ObValueTypeStr[dag_scheduler_info.value_type_]);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case KEY:
      cells[i].set_varchar(dag_scheduler_info.key_);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case VALUE:
      cells[i].set_int(dag_scheduler_info.value_);
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id, ", K(ret), K(col_id));
    }
  }

  return ret;
}

void ObAllVirtualDagScheduler::reset()
{
  ObVirtualTableScannerIterator::reset();
  dag_scheduler_info_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(dag_id_buf_, 0, sizeof(dag_id_buf_));
}

}//storage
}//oceanbase

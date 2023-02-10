// Copyright 2010-2016 Alibaba Inc. All Rights Reserved.
// Author:
//   zhenling.zzg
// this file defines implementation of __all_virtual_plan_real_info

#include "sql/monitor/ob_plan_real_info_manager.h"
#include "sql/monitor/ob_sql_plan_manager.h"
#include "ob_all_virtual_plan_real_info.h"
#include "observer/omt/ob_multi_tenant.h"
#include "observer/ob_server_struct.h"
#include "lib/utility/utility.h"

#include <algorithm> // std::sort

using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::omt;
using namespace oceanbase::share;
namespace oceanbase {
namespace observer {

ObAllVirtualPlanRealInfo::ObAllVirtualPlanRealInfo() :
    ObVirtualTableScannerIterator(),
    plan_real_info_mgr_(nullptr),
    start_id_(INT64_MAX),
    end_id_(INT64_MIN),
    cur_id_(0),
    ref_(),
    addr_(NULL),
    ipstr_(),
    port_(0),
    is_first_get_(true),
    tenant_id_array_(),
    tenant_id_array_idx_(-1),
    with_tenant_ctx_(nullptr)
{
}

ObAllVirtualPlanRealInfo::~ObAllVirtualPlanRealInfo() {
  reset();
}

void ObAllVirtualPlanRealInfo::reset()
{
  if (with_tenant_ctx_ != nullptr && allocator_ != nullptr) {
    if (plan_real_info_mgr_ != nullptr && ref_.idx_ != -1) {
      plan_real_info_mgr_->revert(&ref_);
    }
    with_tenant_ctx_->~ObTenantSpaceFetcher();
    allocator_->free(with_tenant_ctx_);
    with_tenant_ctx_ = nullptr;
  }
  ObVirtualTableScannerIterator::reset();
  is_first_get_ = true;
  cur_id_ = 0;
  tenant_id_array_.reset();
  tenant_id_array_idx_ = -1;
  start_id_ = INT64_MAX;
  end_id_ = INT64_MIN;
  plan_real_info_mgr_ = nullptr;
  addr_ = nullptr;
  port_ = 0;
  ipstr_.reset();
}

int ObAllVirtualPlanRealInfo::inner_open()
{
  int ret = OB_SUCCESS;

  // sys tenant show all tenant plan real info
  if (is_sys_tenant(effective_tenant_id_)) {
    if (OB_FAIL(extract_tenant_ids())) {
      SERVER_LOG(WARN, "failed to extract tenant ids", KR(ret), K(effective_tenant_id_));
    }
  } else {
    // user tenant show self tenant plan real info
    if (OB_FAIL(tenant_id_array_.push_back(effective_tenant_id_))) {
      SERVER_LOG(WARN, "failed to push back tenant", KR(ret), K(effective_tenant_id_));
    }
  }

  SERVER_LOG(DEBUG, "tenant ids", K(effective_tenant_id_), K(tenant_id_array_));

  if (OB_SUCC(ret)) {
    if (NULL == allocator_) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "Invalid Allocator", K(ret));
    } else if (OB_FAIL(set_ip(addr_))) {
      SERVER_LOG(WARN, "failed to set server ip addr", K(ret));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObAllVirtualPlanRealInfo::set_ip(common::ObAddr *addr)
{
  int ret = OB_SUCCESS;
  MEMSET(server_ip_, 0, sizeof(server_ip_));
  if (NULL == addr){
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!addr_->ip_to_string(server_ip_, sizeof(server_ip_))) {
    SERVER_LOG(ERROR, "ip to string failed");
    ret = OB_ERR_UNEXPECTED;
  } else {
    ipstr_ = ObString::make_string(server_ip_);
    port_ = addr_->get_port();
  }
  return ret;
}

int ObAllVirtualPlanRealInfo::check_ip_and_port(bool &is_valid)
{
  int ret = OB_SUCCESS;
  is_valid = true;
  // is_serving_tenant被改成 (svr_ip, svr_port) in (ip1, port1), (ip2, port2), ...
  // 抽出来的query range为[(ip1, port1), (ip1, port1)], [(ip2, port2), (ip2, port2)], ...
  // 需要遍历所有query range，判断本机的ip & port是否落在某一个query range中
  if (key_ranges_.count() >= 1) {
    is_valid = false;
    for (int64_t i = 0; OB_SUCC(ret) && !is_valid && i < key_ranges_.count(); i++) {
      ObNewRange &range = key_ranges_.at(i);
      if (range.get_start_key().get_obj_cnt() != ROWKEY_COUNT ||
          range.get_end_key().get_obj_cnt() != ROWKEY_COUNT) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected rowkey columns",
                   "size of start key", range.get_start_key().get_obj_cnt(),
                   "size of end key", range.get_end_key().get_obj_cnt(),
                   K(ret));
      } else {
        ObObj ip_obj;
        ObObj ip_low = (range.get_start_key().get_obj_ptr()[KEY_IP_IDX]);
        ObObj ip_high = (range.get_end_key().get_obj_ptr()[KEY_IP_IDX]);
        ip_obj.set_varchar(ipstr_);
        ip_obj.set_collation_type(ip_low.get_collation_type());
        if (ip_obj.compare(ip_low) >= 0 && ip_obj.compare(ip_high) <= 0) {
          ObObj port_obj;
          port_obj.set_int32(port_);
          ObObj port_low = (range.get_start_key().get_obj_ptr()[KEY_PORT_IDX]);
          ObObj port_high = (range.get_end_key().get_obj_ptr()[KEY_PORT_IDX]);
          if (port_obj.compare(port_low) >= 0 && port_obj.compare(port_high) <= 0) {
            is_valid = true;
          }
        }
      }
    }
  }
  SERVER_LOG(DEBUG, "check ip and port", K(key_ranges_), K(is_valid), K(ipstr_), K(port_));
  return ret;
}

int ObAllVirtualPlanRealInfo::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  ObPlanRealInfoMgr *prev_mgr = plan_real_info_mgr_;
  if (NULL == allocator_) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "invalid argument", KP(allocator_), K(ret));
  } else if (is_first_get_) {
    bool is_valid = true;
    // init inner iterator varaibales
    tenant_id_array_idx_ = is_reverse_scan() ? tenant_id_array_.count() : -1;
    if (OB_FAIL(check_ip_and_port(is_valid))) {
      SERVER_LOG(WARN, "check ip and port failed", K(ret));
    } else if (!is_valid) {
      ret = OB_ITER_END;;
    }
  }

  if (OB_SUCC(ret) &&
      (nullptr == plan_real_info_mgr_ ||
        cur_id_ < start_id_ ||
        cur_id_ >= end_id_)) {
    //read new tenant plan
    plan_real_info_mgr_ = nullptr;
    while (nullptr == plan_real_info_mgr_ && OB_SUCC(ret)) {
      if (is_reverse_scan())  {
        tenant_id_array_idx_ -= 1;
      } else {
        tenant_id_array_idx_ += 1;
      }
      if (tenant_id_array_idx_ >= tenant_id_array_.count() ||
          tenant_id_array_idx_ < 0) {
        ret = OB_ITER_END;
        break;
      } else {
        //release last tenant ctx memory
        if (with_tenant_ctx_ != nullptr) {
          // before freeing tenant ctx, we must release ref_ if possible
          if (nullptr != prev_mgr && ref_.idx_ != -1) {
            prev_mgr->revert(&ref_);
          }
          with_tenant_ctx_->~ObTenantSpaceFetcher();
          allocator_->free(with_tenant_ctx_);
          with_tenant_ctx_ = nullptr;
        }
        //swith new tenant
        void *buff = nullptr;
        uint64_t t_id = tenant_id_array_.at(tenant_id_array_idx_);
        if (nullptr == (buff = allocator_->alloc(sizeof(ObTenantSpaceFetcher)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(WARN, "failed to allocate memory", K(ret));
        } else {
          with_tenant_ctx_ = new(buff) ObTenantSpaceFetcher(t_id);
          ObSqlPlanMgr *sql_plan_mgr = NULL;
          if (OB_FAIL(with_tenant_ctx_->get_ret())) {
            // 如果指定tenant id查询, 且当前机器没有该租户资源时, 获取
            // tenant space会报OB_TENANT_NOT_IN_SERVER, 此时需要忽略该报
            // 错, 返回该租户的plan real info记录为空
            if (OB_TENANT_NOT_IN_SERVER == ret) {
              ret = OB_SUCCESS;
              continue;
            }
          } else if (OB_ISNULL(sql_plan_mgr=with_tenant_ctx_->entity().get_tenant()->get<ObSqlPlanMgr*>())) {
            plan_real_info_mgr_ = nullptr;
            SERVER_LOG(DEBUG, "unexpect null sql plan manager", K(ret));
          } else {
            plan_real_info_mgr_ = sql_plan_mgr->get_plan_real_info_mgr();
          }
        }

        if (nullptr == plan_real_info_mgr_) {
          SERVER_LOG(DEBUG, "plan real info manager doest not exist", K(t_id));
          continue;
        } else if (OB_SUCC(ret)) {
          start_id_ = INT64_MIN;
          end_id_ = INT64_MAX;
          int64_t start_idx = plan_real_info_mgr_->get_start_idx();
          int64_t end_idx = plan_real_info_mgr_->get_end_idx();
          start_id_ = MAX(start_id_, start_idx);
          end_id_ = MIN(end_id_, end_idx);
          if (start_id_ >= end_id_) {
            SERVER_LOG(DEBUG, "plan_real_info_mgr_ iter end", K(start_id_), K(end_id_), K(t_id));
            prev_mgr = plan_real_info_mgr_;
            plan_real_info_mgr_ = nullptr;
          } else if (is_reverse_scan()) {
            cur_id_ = end_id_ - 1;
          } else {
            cur_id_ = start_id_;
          }
          SERVER_LOG(DEBUG, "start to get rows from __all_virtual_plan_real_info",
                      K(start_id_), K(end_id_), K(cur_id_), K(t_id),
                      K(start_idx), K(end_idx));
        }
      }
    }
  }

  //no more plan, release last tenant's ctx
  if (OB_ITER_END == ret) {
    if (with_tenant_ctx_ != nullptr) {
      if (prev_mgr != nullptr && ref_.idx_ != -1) {
        prev_mgr->revert(&ref_);
      }
      with_tenant_ctx_->~ObTenantSpaceFetcher();
      allocator_->free(with_tenant_ctx_);
      with_tenant_ctx_ = nullptr;
    }
  }

  //fetch next record
  void *rec = NULL;
  if (OB_SUCC(ret)) {
    if (ref_.idx_ != -1) {
      plan_real_info_mgr_->revert(&ref_);
    }
    do {
      ref_.reset();
      ret = plan_real_info_mgr_->get(cur_id_, rec, &ref_);
      if (OB_ENTRY_NOT_EXIST == ret) {
        if (is_reverse_scan()) {
          cur_id_ -= 1;
        } else {
          cur_id_ += 1;
        }
      }
    } while (OB_ENTRY_NOT_EXIST == ret &&
             cur_id_ < end_id_ &&
             cur_id_ >= start_id_);
    if (OB_SUCC(ret) && NULL == rec) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "unexpected null rec", K(cur_id_), K(tenant_id_array_idx_),
                  K(tenant_id_array_), K(ret));
    }
  }

  //fill new row with record
  if (OB_SUCC(ret)) {
    ObPlanRealInfoRecord *record = static_cast<ObPlanRealInfoRecord*> (rec);
    if (OB_FAIL(fill_cells(*record))) {
      SERVER_LOG(WARN, "failed to fill cells", K(ret));
    } else {
      //finish fetch one row
      row = &cur_row_;
      SERVER_LOG(DEBUG, "get next row succ", K(cur_id_));
    }
  }

  // move to next slot
  if (OB_SUCC(ret)) {
    if (!is_reverse_scan()) {
      // forwards
      cur_id_++;
    } else {
      // backwards
      cur_id_--;
    }
    is_first_get_ = false;
  }
  return ret;
}

int ObAllVirtualPlanRealInfo::extract_tenant_ids()
{
  int ret = OB_SUCCESS;
  tenant_id_array_.reset();
  tenant_id_array_idx_ = -1;
  ObRowkey start_key, end_key;
  int64_t N = key_ranges_.count();
  bool is_full_scan = false;
  bool is_always_false = false;

  for (int64_t i = 0; OB_SUCC(ret) && !is_full_scan && !is_always_false && i < N; i++) {
    start_key.reset();
    end_key.reset();
    start_key = key_ranges_.at(i).start_key_;
    end_key = key_ranges_.at(i).end_key_;
    const ObObj *start_key_obj_ptr = start_key.get_obj_ptr();
    const ObObj *end_key_obj_ptr = end_key.get_obj_ptr();

    if (start_key.get_obj_cnt() != end_key.get_obj_cnt() ||
        start_key.get_obj_cnt() != ROWKEY_COUNT) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid range", K(ret));
    } else if (OB_ISNULL(start_key_obj_ptr) || OB_ISNULL(end_key_obj_ptr)) {
      ret = OB_INVALID_ARGUMENT;
      SERVER_LOG(WARN, "invalid arguments", K(ret));
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX].is_min_value() &&
               end_key_obj_ptr[KEY_TENANT_ID_IDX].is_max_value()) {
      is_full_scan = true;
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX].is_max_value() &&
               end_key_obj_ptr[KEY_TENANT_ID_IDX].is_min_value()) {
      is_always_false = true;
    } else if (!(start_key_obj_ptr[KEY_TENANT_ID_IDX].is_min_value() &&
                 end_key_obj_ptr[KEY_TENANT_ID_IDX].is_max_value())
               && start_key_obj_ptr[KEY_TENANT_ID_IDX] != end_key_obj_ptr[KEY_TENANT_ID_IDX]) {
      ret = OB_NOT_IMPLEMENT;
      SERVER_LOG(WARN, "tenant id only supports exact value", K(ret));
    } else if (start_key_obj_ptr[KEY_TENANT_ID_IDX] == end_key_obj_ptr[KEY_TENANT_ID_IDX]) {
      if (ObIntType != start_key_obj_ptr[KEY_TENANT_ID_IDX].get_type() ||
          (start_key_obj_ptr[KEY_TENANT_ID_IDX].get_type()
            != end_key_obj_ptr[KEY_TENANT_ID_IDX].get_type())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "expect tenant id type to be int",
                    K(start_key_obj_ptr[KEY_TENANT_ID_IDX].get_type()),
                    K(end_key_obj_ptr[KEY_TENANT_ID_IDX].get_type()));
      } else {
        int64_t tenant_id = start_key_obj_ptr[KEY_TENANT_ID_IDX].get_int();
        if (tenant_id < 0) {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected tenant id", K(ret));
        } else if (OB_FAIL(add_var_to_array_no_dup(tenant_id_array_, static_cast<uint64_t>(tenant_id)))) {
          SERVER_LOG(WARN, "failed to add tenant_id to array no duplicate", K(ret));
        } else {
          // do nothing
        }
      }
    }
  } // for end
  if (!is_full_scan) {
    // do nothing
  } else if (is_always_false) {
    tenant_id_array_.reset();
  } else if (OB_ISNULL(GCTX.omt_)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "unexpected null of omt", K(ret));
  } else {
    // get all tenant ids
    TenantIdList id_list(16, NULL, ObNewModIds::OB_COMMON_ARRAY);
    GCTX.omt_->get_tenant_ids(id_list);
    tenant_id_array_.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < id_list.size(); i++) {
      if (OB_FAIL(tenant_id_array_.push_back(id_list.at(i)))) {
        SERVER_LOG(WARN, "failed to push back tenant id", K(ret), K(i));
      }
    }
    if (OB_SUCC(ret)) {
      std::sort(tenant_id_array_.begin(), tenant_id_array_.end());
      SERVER_LOG(DEBUG, "get tenant ids from req mgr map", K(tenant_id_array_));
    }
  }
  if (OB_FAIL(ret)) {
    tenant_id_array_.reset();
  }
  return ret;
}

int ObAllVirtualPlanRealInfo::fill_cells(ObPlanRealInfoRecord &record)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;

  if (OB_ISNULL(cells)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid argument", K(cells));
  }
  for (int64_t cell_idx = 0; OB_SUCC(ret) && cell_idx < col_count; cell_idx++) {
    uint64_t col_id = output_column_ids_.at(cell_idx);
    switch(col_id) {
    case TENANT_ID: {
      cells[cell_idx].set_int(tenant_id_array_.at(tenant_id_array_idx_));
      break;
    }
    case SVR_IP: {
      //ipstr_ and port_ were set in set_ip func call
      cells[cell_idx].set_varchar(ipstr_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                            ObCharset::get_default_charset()));
      break;
    }
    case SVR_PORT: {
      cells[cell_idx].set_int(port_);
      break;
    }
    case PLAN_ITEM_ID: {
      cells[cell_idx].set_int(cur_id_);
      break;
    }
    case SQL_ID: {
      cells[cell_idx].set_varchar(record.data_.sql_id_, record.data_.sql_id_len_);
      cells[cell_idx].set_collation_type(ObCharset::get_default_collation(
                                         ObCharset::get_default_charset()));
      break;
    }
    case PLAN_ID: {
      cells[cell_idx].set_int(record.data_.plan_id_);
      break;
    }
    case PLAN_HASH: {
      cells[cell_idx].set_uint64(record.data_.plan_hash_);
      break;
    }
    case ID: {
      cells[cell_idx].set_int(record.data_.id_);
      break;
    }
    case REAL_COST: {
      cells[cell_idx].set_int(record.data_.real_cost_);
      break;
    }
    case REAL_CARDINALITY: {
      cells[cell_idx].set_int(record.data_.real_cardinality_);
      break;
    }
    case CPU_COST: {
      cells[cell_idx].set_int(record.data_.cpu_cost_);
      break;
    }
    case IO_COST: {
      cells[cell_idx].set_int(record.data_.io_cost_);
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(cell_idx), K(col_id));
      break;
    }
    }
  }
  return ret;
}

} //namespace observer
} //namespace oceanbase

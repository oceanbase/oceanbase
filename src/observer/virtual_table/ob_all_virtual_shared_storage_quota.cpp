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

#include "ob_all_virtual_shared_storage_quota.h"
#include "src/observer/net/ob_shared_storage_net_throt_rpc_struct.h"
namespace oceanbase
{
using namespace lib;
namespace observer
{
ObVirtualSharedStorageQuota::ObVirtualSharedStorageQuota() : is_inited_(false)
{}

ObVirtualSharedStorageQuota::~ObVirtualSharedStorageQuota()
{
  reset();
}

int ObVirtualSharedStorageQuota::inner_open()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_)) == false)) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(WARN, "ip_to_string() fail", K(ret));
  }
  return ret;
}

void ObVirtualSharedStorageQuota::reset()
{
  is_inited_ = false;
}

int ObVirtualSharedStorageQuota::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    if (OB_FAIL(add_one_storage_batch_row())) {
      SERVER_LOG(WARN, "add line failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      scanner_it_ = scanner_.begin();
      is_inited_ = true;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_it_.get_next_row(cur_row_))) {
      if (OB_ITER_END != ret) {
        SERVER_LOG(WARN, "fail to get next row", K(ret));
      }
    } else {
      row = &cur_row_;
    }
  }
  return ret;
}

int ObVirtualSharedStorageQuota::add_one_storage_batch_row()
{
  int ret = OB_SUCCESS;
  obrpc::ObSharedDeviceResourceArray usages;
  obrpc::ObSharedDeviceResourceArray limits;
  struct GetLimit
  {
    GetLimit(obrpc::ObSharedDeviceResourceArray &limits) : limits_(limits)
    {}
    int operator()(
        oceanbase::common::hash::HashMapPair<ObTrafficControl::ObStorageKey, ObTrafficControl::ObSharedDeviceControl>
            &entry)
    {
      int ret = OB_SUCCESS;
      int idx_begin = limits_.array_.count();
      for (int i = 0; i < obrpc::ResourceType::ResourceTypeCnt; ++i) {
        limits_.array_.push_back(obrpc::ObSharedDeviceResource());
      }
      if (OB_UNLIKELY(idx_begin < 0)
          || OB_UNLIKELY(idx_begin + obrpc::ResourceType::ResourceTypeCnt > limits_.array_.count())) {
      } else {
        for (int i = 0; i < obrpc::ResourceType::ResourceTypeCnt; ++i) {
          limits_.array_.at(idx_begin + i).key_ = entry.first;
          limits_.array_.at(idx_begin + i).type_ = static_cast<obrpc::ResourceType>(i);
          ObAtomIOClock *clock = entry.second.get_clock(static_cast<obrpc::ResourceType>(i));
          if (OB_ISNULL(clock)) {
            limits_.array_.at(idx_begin + i).value_ = 0;
            SERVER_LOG(WARN, "clock is null", K(ret));
          } else {
            limits_.array_.at(idx_begin + i).value_ = clock->iops_;
          }
        }
      }
      return ret;
    }
    obrpc::ObSharedDeviceResourceArray &limits_;
  };
  struct GetLimitV2
  {
    GetLimitV2(obrpc::ObSharedDeviceResourceArray &limits) : limits_(limits)
    {}
    int operator()(oceanbase::common::hash::HashMapPair<ObTrafficControl::ObStorageKey,
        ObTrafficControl::ObSharedDeviceControlV2 *> &entry)
    {
      int ret = OB_SUCCESS;
      int idx_begin = limits_.array_.count();
      for (int i = 0; i < obrpc::ResourceType::ResourceTypeCnt; ++i) {
        limits_.array_.push_back(obrpc::ObSharedDeviceResource());
      }
      if (OB_UNLIKELY(idx_begin < 0)
          || OB_UNLIKELY(idx_begin + obrpc::ResourceType::ResourceTypeCnt > limits_.array_.count())) {
      } else if (OB_UNLIKELY(OB_ISNULL(entry.second))) {
      } else {
        for (int i = 0; i < obrpc::ResourceType::ResourceTypeCnt; ++i) {
          limits_.array_.at(idx_begin + i).key_ = entry.first;
          limits_.array_.at(idx_begin + i).type_ = static_cast<obrpc::ResourceType>(i);
          limits_.array_.at(idx_begin + i).value_ = entry.second->get_limit(static_cast<obrpc::ResourceType>(i));
        }
      }
      return ret;
    }
    obrpc::ObSharedDeviceResourceArray &limits_;
  };

  struct GetUsage
  {
    GetUsage(obrpc::ObSharedDeviceResource &resource) : resource_(resource)
    {}
    int operator()(hash::HashMapPair<ObTrafficControl::ObIORecordKey, ObTrafficControl::ObSharedDeviceIORecord> &entry)
    {
      int ret = OB_SUCCESS;
      if (resource_.key_ == entry.first.id_) {

        const int64_t bw_in =   entry.second.ibw_.calc();
        const int64_t bw_out =  entry.second.obw_.calc();
        const int64_t req_in =  entry.second.ips_.calc();
        const int64_t req_out = entry.second.ops_.calc();
        const int64_t tagps =   entry.second.tagps_.calc();

        switch (resource_.type_) {
          case obrpc::ResourceType::ops:
            resource_.value_ += req_out;
            break;
          case obrpc::ResourceType::ips:
            resource_.value_ += req_in;
            break;
          case obrpc::ResourceType::iops:
            resource_.value_ += req_out + req_in;
            break;
          case obrpc::ResourceType::obw:
            resource_.value_ += bw_out;
            break;
          case obrpc::ResourceType::ibw:
            resource_.value_ += bw_in;
            break;
          case obrpc::ResourceType::iobw:
            resource_.value_ += bw_out + bw_in;
            break;
          case obrpc::ResourceType::tag:
            resource_.value_ += tagps;
            break;
          default:
            SERVER_LOG(WARN, "unexpected resource type", K(resource_.type_));
        }
      }
      return ret;
    }
    obrpc::ObSharedDeviceResource &resource_;
  };
  limits.array_.reserve(OB_IO_MANAGER.get_tc().get_storage_count() * obrpc::ResourceType::ResourceTypeCnt);
  // get limits
  if (GCONF._enable_tree_based_io_scheduler == false) {
    GetLimit limits_fn(limits);
    if (OB_FAIL(OB_IO_MANAGER.get_tc().foreach_limit(limits_fn))) {
      SERVER_LOG(WARN, "predict failed", K(ret));
    }
  } else {
    GetLimitV2 limits_fn_v2(limits);
    if (OB_FAIL(OB_IO_MANAGER.get_tc().foreach_limit_v2(limits_fn_v2))) {
      SERVER_LOG(WARN, "predict failed", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(usages.array_.assign(limits.array_))) {
    SERVER_LOG(WARN, "assign failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < usages.array_.count(); ++i) {
      usages.array_.at(i).value_ = 0;
      GetUsage usages_fn(usages.array_.at(i));
      if (OB_FAIL(OB_IO_MANAGER.get_tc().foreach_record(usages_fn))) {
        SERVER_LOG(WARN, "predict failed", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (limits.array_.count() != usages.array_.count()) {
  } else {
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < limits.array_.count(); ++i) {
      if (usages.array_.at(i).key_ != usages.array_.at(i).key_) {
        SERVER_LOG(WARN, "unexpected key", K(usages.array_.at(i).key_), K(limits.array_.at(i).key_));
      } else if (OB_TMP_FAIL(add_row(usages.array_.at(i), limits.array_.at(i)))) {
        SERVER_LOG(WARN, "fail to add row", K(tmp_ret), K(cur_row_));
      }
    }
  }
  return ret;
}

int ObVirtualSharedStorageQuota::add_row(
    const obrpc::ObSharedDeviceResource &usage, const obrpc::ObSharedDeviceResource &limit)
{
  int ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    const uint64_t col_id = output_column_ids_.at(i);
    ObObj *cells = cur_row_.cells_;
    switch (col_id) {
      case SVR_IP: {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case SVR_PORT: {
        cells[i].set_int(GCONF.self_addr_.get_port());
        break;
      }
      case MODULE: {
        const ObStorageInfoType &storage_type = limit.key_.get_category();
        if (storage_type == ObStorageInfoType::ALL_ZONE_STORAGE) {
          cells[i].set_varchar("CLOG/DATA");
        } else {
          cells[i].set_varchar("BACKUP/ARCHIVE/RESTORE");
        }
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case CLASS_ID: {
        cells[i].set_int(limit.key_.get_tenant_id());
        break;
      }
      case STORAGE_ID: {
        cells[i].set_int(limit.key_.get_storage_id());
        break;
      }
      case TYPE: {
        const obrpc::ResourceType &type = limit.type_;
        if (type == obrpc::ResourceType::ops) {
          cells[i].set_varchar("ops");
        } else if (type == obrpc::ResourceType::ips) {
          cells[i].set_varchar("ips");
        } else if (type == obrpc::ResourceType::iops) {
          cells[i].set_varchar("iops");
        } else if (type == obrpc::ResourceType::obw) {
          cells[i].set_varchar("obw");
        } else if (type == obrpc::ResourceType::ibw) {
          cells[i].set_varchar("ibw");
        } else if (type == obrpc::ResourceType::iobw) {
          cells[i].set_varchar("iobw");
        } else if (type == obrpc::ResourceType::tag) {
          cells[i].set_varchar("tag");
        } else {
          ret = OB_ERR_UNEXPECTED;
          SERVER_LOG(WARN, "unexpected resource type", K(type), K(ret));
        }
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
        break;
      }
      case REQUIRE: {
        cells[i].set_int(usage.value_);
        break;
      }
      case ASSIGN: {
        if (limit.value_ <= 0 || limit.value_ >= INT64_MAX) {
          cells[i].set_int(INT64_MAX);
        } else {
          cells[i].set_int(limit.value_);
        }
        break;
      }
      default: {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(WARN, "unexpected column id", K(col_id), K(i), K(ret));
        break;
      }
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(scanner_.add_row(cur_row_))) {
      SERVER_LOG(WARN, "fail to add row", K(ret), K(cur_row_));
      if (OB_SIZE_OVERFLOW == ret) {
        ret = OB_SUCCESS;
      }
    }
  }
  return ret;
}

}  // namespace observer
}  // namespace oceanbase
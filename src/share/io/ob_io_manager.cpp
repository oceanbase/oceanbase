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

#define USING_LOG_PREFIX COMMON

#include "ob_io_manager.h"
#include "share/errsim_module/ob_errsim_module_interface_imp.h"
#include "observer/ob_server.h"
#include "src/share/io/io_schedule/ob_io_schedule_v2.h"
#include "lib/restore/ob_object_device.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "share/io/ob_ss_io_request.h"
#endif

using namespace oceanbase::lib;
using namespace oceanbase::common;

OB_SERIALIZE_MEMBER(ObTrafficControl::ObStorageKey, storage_id_, category_, tenant_id_);
namespace oceanbase
{
namespace common
{
// for local device
int64_t get_norm_iops(const int64_t size, const double iops, const ObIOMode mode)
{
  int ret = OB_SUCCESS;
  int64_t norm_iops = 0;
  double bw = 0;
  double iops_scale = 0;
  bool is_io_ability_valid = false;
  if (iops < std::numeric_limits<double>::epsilon()) {
  } else if (FALSE_IT(bw = size * iops)) {
  } else if (mode == ObIOMode::MAX_MODE) {
    norm_iops = bw / STANDARD_IOPS_SIZE;
  } else if (FALSE_IT(ObIOCalibration::get_instance().get_iops_scale(mode, size, iops_scale, is_io_ability_valid))) {
  } else if (iops_scale < std::numeric_limits<double>::epsilon()) {
    norm_iops = bw / STANDARD_IOPS_SIZE;
    LOG_WARN("calc iops scale failed", K(ret), K(bw), K(iops), K(mode));
  } else {
    norm_iops = static_cast<int64_t>(iops / iops_scale);
  }
  return norm_iops;
}

// for local device
int64_t get_norm_bw(const int64_t size, const ObIOMode mode)
{
  int ret = OB_SUCCESS;
  int64_t norm_bw = size;
  double iops_scale = 0;
  bool is_io_ability_valid = false;
  if (mode == ObIOMode::MAX_MODE) {
  } else if (FALSE_IT(ObIOCalibration::get_instance().get_iops_scale(mode, size, iops_scale, is_io_ability_valid))) {
  } else if (iops_scale < std::numeric_limits<double>::epsilon()) {
    LOG_WARN("calc iops scale failed", K(ret), K(mode));
  } else {
    norm_bw = static_cast<int64_t>((double)STANDARD_IOPS_SIZE / iops_scale);
  }
  return max(norm_bw, 1);
}
}  // namespace common
}  // namespace oceanbase
int64_t ObTrafficControl::IORecord::calc()
{
  int64_t now = ObTimeUtility::fast_current_time();
  int64_t last_ts = ATOMIC_LOAD(&last_ts_);
  if (0 != last_ts
      && now - last_ts > 1 * 1000 * 1000
      && ATOMIC_BCAS(&last_ts_, last_ts, 0)) {
    int64_t size = 0;
    IGNORE_RETURN ATOMIC_FAA(&total_size_, size = ATOMIC_SET(&size_, 0));
    ATOMIC_STORE(&last_record_, size * 1000 * 1000 / (now - last_ts));
    ATOMIC_STORE(&last_ts_, now);
  }
  return ATOMIC_LOAD(&last_record_);
}

int ObTrafficControl::ObSharedDeviceIORecord::calc_usage(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (req.fd_.device_handle_->is_object_device() != true) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("io request is not object device", K(req), K(ret));
  } else {
    if (req.get_mode() == ObIOMode::READ) {
      ibw_.inc(req.get_align_size());
      ips_.inc(1);
    } else if (req.get_mode() == ObIOMode::WRITE) {
      obw_.inc(req.get_align_size());
      ops_.inc(1);
    } else /* if (req.get_mode() == ObIOMode::READ) */ {
      tagps_.inc(1);
    }
  }
  return ret;
}

ObTrafficControl::ObSharedDeviceControl::ObSharedDeviceControl()
{
  ibw_clock_.iops_ = INT64_MAX;
  obw_clock_.iops_ = INT64_MAX;
  iobw_clock_.iops_ = INT64_MAX;
  ips_clock_.iops_ = INT64_MAX;
  ops_clock_.iops_ = INT64_MAX;
  iops_clock_.iops_ = INT64_MAX;
  tagps_clock_.iops_ = INT64_MAX;
}

int ObTrafficControl::ObSharedDeviceControl::calc_clock(const int64_t current_ts, ObIORequest &req, int64_t &deadline_ts)
{
  int ret = OB_SUCCESS;
  if (req.fd_.device_handle_->is_object_device() != true) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("io request is not object device", K(req), K(ret));
  } else {
    int64_t io_size = req.get_align_size();
    iobw_clock_.compare_and_update(current_ts, 1.0 / io_size, deadline_ts);
    iops_clock_.compare_and_update(current_ts, 1.0, deadline_ts);
    if (req.get_mode() == ObIOMode::READ) {
      ibw_clock_.compare_and_update(current_ts, 1.0 / io_size, deadline_ts);
      ips_clock_.compare_and_update(current_ts, 1.0, deadline_ts);
    } else if (req.get_mode() == ObIOMode::WRITE) {
      obw_clock_.compare_and_update(current_ts, 1.0 / io_size, deadline_ts);
      ops_clock_.compare_and_update(current_ts, 1.0, deadline_ts);
    } else /* if (req.get_mode() == ObIOMode::READ) */ {
      tagps_clock_.compare_and_update(current_ts, 1.0, deadline_ts);
    }
  }
  return ret;
}

// when use this interface input array default size shoulde be ResourceTypeCnt
void ObTrafficControl::ObSharedDeviceIORecord::reset_total_size(ResourceUsage usages[])
{
  usages[obrpc::ResourceType::ibw].type_   = obrpc::ResourceType::ibw;
  usages[obrpc::ResourceType::ibw].total_  = ibw_.clear();
  usages[obrpc::ResourceType::obw].type_   = obrpc::ResourceType::obw;
  usages[obrpc::ResourceType::obw].total_  = obw_.clear();
  usages[obrpc::ResourceType::ips].type_   = obrpc::ResourceType::ips;
  usages[obrpc::ResourceType::ips].total_  = ips_.clear();
  usages[obrpc::ResourceType::ops].type_   = obrpc::ResourceType::ops;
  usages[obrpc::ResourceType::ops].total_  = ops_.clear();
}

void ObTrafficControl::ObSharedDeviceControl::set_limit(const obrpc::ObSharedDeviceResource &limit)
{
  ObAtomIOClock* clock = get_clock(limit.type_);
  if (OB_ISNULL(clock)) {
    LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid type", K(limit));
  } else {
    ATOMIC_STORE(&clock->iops_, limit.value_);
  }
}

ObTrafficControl::ObSharedDeviceControlV2::ObSharedDeviceControlV2()
{
  init();
}

ObTrafficControl::ObSharedDeviceControlV2::~ObSharedDeviceControlV2()
{
  destroy();
}

int ObTrafficControl::ObSharedDeviceControlV2::init()
{
  int ret = OB_SUCCESS;
  memset(limit_ids_, -1, sizeof(limit_ids_));
  limits_[obrpc::ResourceType::ops] = INT64_MAX / 2;
  limits_[obrpc::ResourceType::ips] = INT64_MAX / 2;
  limits_[obrpc::ResourceType::iops] = INT64_MAX;
  limits_[obrpc::ResourceType::obw] = INT64_MAX / (16 * (1<<11));
  limits_[obrpc::ResourceType::ibw] = INT64_MAX / (16 * (1<<11));
  limits_[obrpc::ResourceType::iobw] = INT64_MAX / (16 * (1<<10));
  limits_[obrpc::ResourceType::tag] = INT64_MAX;
  storage_key_  = ObStorageKey();
  return ret;
}

void ObTrafficControl::ObSharedDeviceControlV2::destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(group_list_.clear())) {
    LOG_WARN("clear map failed", K(ret));
  } else {
    for (int i = 0; i < static_cast<int>(obrpc::ResourceType::ResourceTypeCnt); i++) {
      if (limit_ids_[i] < 0) {
        LOG_WARN("invalid limit id failed", K(ret), K(limit_ids_[i]), K(i));
      } else {
        tclimit_destroy(limit_ids_[i]);
      }
    }
  }
}
int ObTrafficControl::ObSharedDeviceControlV2::set_storage_key(const ObTrafficControl::ObStorageKey &key)
{
  return storage_key_.assign(key);
}
int ObTrafficControl::ObSharedDeviceControlV2::add_shared_device_limits()
{
  int ret = OB_SUCCESS;
  limit_ids_[static_cast<int>(ResourceType::ips)] = tclimit_create(TCLIMIT_COUNT, get_resource_type_str(ResourceType::ips));
  limit_ids_[static_cast<int>(ResourceType::ibw)] = tclimit_create(TCLIMIT_BYTES, get_resource_type_str(ResourceType::ibw));
  limit_ids_[static_cast<int>(ResourceType::ops)] = tclimit_create(TCLIMIT_COUNT, get_resource_type_str(ResourceType::ops));
  limit_ids_[static_cast<int>(ResourceType::obw)] = tclimit_create(TCLIMIT_BYTES, get_resource_type_str(ResourceType::obw));
  LOG_INFO("add shared device limit success",
      "storage_key",
      storage_key_,
      "ips_limit_id",
      limit_ids_[static_cast<int>(ResourceType::ips)],
      "ibw_limit_id",
      limit_ids_[static_cast<int>(ResourceType::ibw)],
      "ops_limit_id",
      limit_ids_[static_cast<int>(ResourceType::ops)],
      "obw_limit_id",
      limit_ids_[static_cast<int>(ResourceType::obw)],
      K(ret));
  return ret;
}

int ObTrafficControl::ObSharedDeviceControlV2::fill_qsched_req_storage_key(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  req.qsched_req_.storage_key_ = this->storage_key_.hash();
  return ret;
}

int ObTrafficControl::ObSharedDeviceControlV2::add_group(const ObIOSSGrpKey &grp_key, const int qid) {
  return transform_ret(group_list_.add_group(grp_key, qid, limit_ids_, ResourceType::ResourceTypeCnt));
}


int ObTrafficControl::ObSharedDeviceControlV2::ObSDGroupList::add_group(const ObIOSSGrpKey &grp_key, const int qid, int* limit_ids, int l_size)
{
  int ret = OB_SUCCESS;
  // add group limits of shared device
  if (l_size > ResourceType::ResourceTypeCnt) {
    LOG_ERROR("l_size is bigger than ResourceTypeCnt", K(l_size), K(ResourceType::ResourceTypeCnt), K(grp_key), K(ret));
  } else if (OB_UNLIKELY(OB_HASH_NOT_EXIST != is_group_key_exist(grp_key))) {
    LOG_WARN("repeat add the group limit ", K(ret), K(grp_key));
  } else if (OB_FAIL(grp_list_.push_back(grp_key))) {
    LOG_WARN("grp list push back failed", K(ret), K(grp_key));
  } else if (ObIOMode::READ == grp_key.get_mode()) {
    if (OB_FAIL(qdisc_add_limit(qid, limit_ids[static_cast<int>(ResourceType::ips)]))) {
      LOG_ERROR("qdisc add limit fail" , K(ret), K(grp_key), K(qid), K(ResourceType::ips), K(limit_ids[static_cast<int>(ResourceType::ips)]));
    } else if (OB_FAIL(qdisc_add_limit(qid, limit_ids[static_cast<int>(ResourceType::ibw)]))) {
      LOG_ERROR("qdisc add limit fail" , K(ret), K(grp_key), K(qid), K(ResourceType::ibw), K(limit_ids[static_cast<int>(ResourceType::ibw)]));
    }
  } else if (ObIOMode::WRITE == grp_key.get_mode()) {
    if (OB_FAIL(qdisc_add_limit(qid, limit_ids[static_cast<int>(ResourceType::ops)]))) {
      LOG_ERROR("qdisc add limit fail" , K(ret), K(grp_key), K(qid), K(ResourceType::ops), K(limit_ids[static_cast<int>(ResourceType::ops)]));
    } else if (OB_FAIL(qdisc_add_limit(qid, limit_ids[static_cast<int>(ResourceType::obw)]))) {
      LOG_ERROR("qdisc add limit fail" , K(ret),K(grp_key), K(qid), K(ResourceType::obw), K(limit_ids[static_cast<int>(ResourceType::obw)]));
    }
  }
  LOG_INFO("add group limit of shared device success", K(grp_key), K(qid), K(ret));
  return ret;
}

int ObTrafficControl::ObSharedDeviceControlV2::is_group_key_exist(const ObIOSSGrpKey &grp_key)
{
  return group_list_.is_group_key_exist(grp_key);
}

int64_t ObTrafficControl::ObSharedDeviceControlV2::get_limit(const obrpc::ResourceType type) const
{
  return limits_[static_cast<int>(type)];
}

int ObTrafficControl::ObSharedDeviceControlV2::update_limit(const obrpc::ObSharedDeviceResource &limit)
{
  int ret = OB_SUCCESS;
  limits_[static_cast<int>(limit.type_)] = limit.value_;
  if (0 != tclimit_set_limit(limit_ids_[static_cast<int>(limit.type_)], limits_[static_cast<int>(limit.type_)])) {
    LOG_WARN("update limit failed", K(ret), K(limit), K(limit_ids_[static_cast<int>(limit.type_)]));
  }
  return ret;
}
ObTrafficControl::ObTrafficControl()
{
  int ret = OB_SUCCESS;
  set_device_bandwidth(observer::ObServer::DEFAULT_ETHERNET_SPEED);
  if (OB_FAIL(shared_device_map_.create(7, "IO_TC_MAP"))) {
    LOG_WARN("create io share device map failed", K(ret));
  } else if (OB_FAIL(shared_device_map_v2_.create(7, "IO_TC_MAP_V2"))) {
    LOG_WARN("create io share device map v2 failed", K(ret));
  }
  if (OB_FAIL(io_record_map_.create(1, "IO_TC_MAP"))) {
    LOG_WARN("create io share device map failed", K(ret));
  }
}

int ObTrafficControl::calc_clock(const int64_t current_ts, ObIORequest &req, int64_t &deadline_ts)
{
  int ret = OB_SUCCESS;
  uint64_t storage_id = ((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod().storage_id_;
  uint8_t mod_id = (uint8_t)((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod().storage_used_mod_;
  ObStorageInfoType table = __storage_table_mapper[mod_id];
  ObStorageKey key(storage_id, req.tenant_id_, table);
  if (req.fd_.device_handle_->is_object_device() != true) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("io request is not object device", K(req), K(ret));
  } else if (((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod().is_valid() != true) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("io request have wrong storage mod", K(req), K(ret));
  } else {
    ObSharedDeviceControl *tc;
    if (OB_NOT_NULL(tc = shared_device_map_.get(key))) {
      // do nothing
    } else if (OB_FAIL(shared_device_map_.set_refactored(key, ObSharedDeviceControl())) && OB_HASH_EXIST != ret) {
      LOG_WARN("set map failed", K(ret));
    } else if (OB_ISNULL(tc = shared_device_map_.get(key))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index from map failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      tc->calc_clock(current_ts, req, deadline_ts);
    }
    if (req.get_mode() == ObIOMode::READ) {
      ibw_clock_.compare_and_update(current_ts, 1.0 / req.get_align_size(), deadline_ts);
    } else {
      obw_clock_.compare_and_update(current_ts, 1.0 / req.get_align_size(), deadline_ts);
    }
  }
  return ret;
}


int ObTrafficControl::calc_usage(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  const ObStorageIdMod &id = ((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod();
  ObIORecordKey key(ObStorageKey(id.storage_id_, req.tenant_id_, id.get_category()), req.tenant_id_);
  ObSharedDeviceIORecord *record = nullptr;
  if (req.fd_.device_handle_->is_object_device() != true) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io request is not object device", K(req), K(ret));
  } else {
    int64_t io_size = req.get_align_size();
    if (OB_NOT_NULL(record = io_record_map_.get(key))) {
      // do nothing
    } else if (OB_FAIL(io_record_map_.set_refactored(key, ObSharedDeviceIORecord())) && OB_HASH_EXIST != ret) {
      LOG_WARN("set map failed", K(ret));
    } else if (OB_ISNULL(record = io_record_map_.get(key))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get index from map failed", K(ret));
    }
    if (OB_SUCC(ret)) {
      record->calc_usage(req);
    }
    // shared_storage_ibw_ and shared_storage_storage_obw_ will accumulate regardless of whether req is succ or not.
    if (req.io_result_ == nullptr) {
      LOG_ERROR("io_result_ is null", K(ret));
    } else if (req.get_mode() == ObIOMode::READ) {
      if (req.io_result_->time_log_.return_ts_ > 0 && req.io_result_->ret_code_.io_ret_ == 0) {
        shared_storage_ibw_.inc(io_size);
      } else {
        shared_storage_ibw_.inc(io_size);
        failed_shared_storage_ibw_.inc(io_size);
      }
    } else {
      if (req.io_result_->time_log_.return_ts_ > 0 && req.io_result_->ret_code_.io_ret_ == 0) {
        shared_storage_obw_.inc(io_size);
      } else {
        shared_storage_obw_.inc(io_size);
        failed_shared_storage_obw_.inc(io_size);
      }
    }
  }
  return ret;
}
int ObTrafficControl::transform_ret(int ret)
{
  switch (ret) {
    case 0:
      ret = OB_SUCCESS;
      break;
    case ENOENT:
      ret = OB_EAGAIN;
      break;
    case -ENOENT:
      ret = OB_EAGAIN;
      break;
    case ENOMEM:
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    case -ENOMEM:
      ret = OB_ALLOCATE_MEMORY_FAILED;
      break;
    default:
      LOG_WARN("unknow ret", K(ret));
      ret = OB_ERR_UNEXPECTED;
      break;
  }
  return ret;
}

void ObTrafficControl::print_server_status()
{
  inner_calc_();
  int64_t net_bw_in =  net_ibw_.calc();
  int64_t net_bw_out = net_obw_.calc();
  int64_t shared_storage_bw_in  = shared_storage_ibw_.calc();
  int64_t shared_storage_bw_out = shared_storage_obw_.calc();
  int64_t failed_shared_storage_bw_in  = failed_shared_storage_ibw_.calc();
  int64_t failed_shared_storage_bw_out = failed_shared_storage_obw_.calc();
  if (net_bw_in || net_bw_out || shared_storage_bw_in || shared_storage_bw_out) {
    _LOG_INFO("[IO STATUS SERVER] net_in=%ldkB/s, net_out=%ldkB/s, bucket_in=%ldkB/s, bucket_out=%ldkB/s, failed_bucket_in=%ldkB/s, failed_bucket_out=%ldkB/s, limit=%ldkB/s",
              net_bw_in / 1024,
              net_bw_out / 1024,
              shared_storage_bw_in / 1024,
              shared_storage_bw_out / 1024,
              failed_shared_storage_bw_in / 1024,
              failed_shared_storage_bw_out / 1024,
              device_bandwidth_ / 1024);
  }
}

void ObTrafficControl::print_bucket_status_V1()
{
  struct PrinterFn
  {
    struct CalFn
    {
      CalFn(const ObStorageKey &key, int64_t &bw_in, int64_t &bw_out, int64_t &req_in, int64_t &req_out, int64_t &tag)
        : key_(key), bw_in_(bw_in), bw_out_(bw_out), req_in_(req_in), req_out_(req_out), tag_(tag) {}
      int operator () (oceanbase::common::hash::HashMapPair<ObIORecordKey, ObSharedDeviceIORecord> &entry) {
        if (key_ == entry.first.id_) {
          bw_in_ +=   entry.second.ibw_.calc();
          bw_out_ +=  entry.second.obw_.calc();
          req_in_ +=  entry.second.ips_.calc();
          req_out_ += entry.second.ops_.calc();
          tag_ +=     entry.second.tagps_.calc();
        }
        return OB_SUCCESS;
      }
      const ObStorageKey &key_;
      int64_t &bw_in_;
      int64_t &bw_out_;
      int64_t &req_in_;
      int64_t &req_out_;
      int64_t &tag_;
    };
    PrinterFn(const hash::ObHashMap<ObIORecordKey, ObSharedDeviceIORecord> &map) : map_(map) {}
    int operator () (oceanbase::common::hash::HashMapPair<ObStorageKey, ObSharedDeviceControl> &entry) {
      int64_t bw_in =   0;
      int64_t bw_out =  0;
      int64_t req_in =  0;
      int64_t req_out = 0;
      int64_t tag =     0;
      CalFn fn(entry.first, bw_in, bw_out, req_in, req_out, tag);
      map_.foreach_refactored(fn);
      if (bw_in || bw_out || req_in || req_out || tag) {
        _LOG_INFO("[IO STATUS BUCKET] storage={%u, %ld, %ld}, in=[%ld / %ld]kB/s, out=[%ld / %ld]kB/s, ips=[%ld / %ld], ops=[%ld / %ld], tag=[%ld / %ld]",
                  entry.first.get_category(),
                  entry.first.get_tenant_id(),
                  entry.first.get_storage_id(),
                  bw_in / 1024,
                  entry.second.ibw_clock_.iops_ == 0 ? INT64_MAX : entry.second.ibw_clock_.iops_ / 1024,
                  bw_out / 1024,
                  entry.second.obw_clock_.iops_ == 0 ? INT64_MAX : entry.second.obw_clock_.iops_ / 1024,
                  req_in,
                  entry.second.ips_clock_.iops_ == 0 ? INT64_MAX : entry.second.ips_clock_.iops_,
                  req_out,
                  entry.second.ops_clock_.iops_ == 0 ? INT64_MAX : entry.second.ops_clock_.iops_,
                  tag,
                  entry.second.tagps_clock_.iops_ / 1024);
      }
      return OB_SUCCESS;
    }
    const hash::ObHashMap<ObIORecordKey, ObSharedDeviceIORecord> &map_;
  };
  PrinterFn fn(io_record_map_);
  shared_device_map_.foreach_refactored(fn);
}

void ObTrafficControl::print_bucket_status_V2()
{
  struct PrinterFn
  {
    struct CalFn
    {
      CalFn(const ObStorageKey &key, int64_t &bw_in, int64_t &bw_out, int64_t &req_in, int64_t &req_out, int64_t &tag)
        : key_(key), bw_in_(bw_in), bw_out_(bw_out), req_in_(req_in), req_out_(req_out), tag_(tag) {}
      int operator () (oceanbase::common::hash::HashMapPair<ObIORecordKey, ObSharedDeviceIORecord> &entry) {
        if (key_ == entry.first.id_) {
          bw_in_ +=   entry.second.ibw_.calc();
          bw_out_ +=  entry.second.obw_.calc();
          req_in_ +=  entry.second.ips_.calc();
          req_out_ += entry.second.ops_.calc();
          tag_ +=     entry.second.tagps_.calc();
        }
        return OB_SUCCESS;
      }
      const ObStorageKey &key_;
      int64_t &bw_in_;
      int64_t &bw_out_;
      int64_t &req_in_;
      int64_t &req_out_;
      int64_t &tag_;
    };
    PrinterFn(const hash::ObHashMap<ObIORecordKey, ObSharedDeviceIORecord> &map) : map_(map) {}
    int operator () (oceanbase::common::hash::HashMapPair<ObStorageKey, ObSharedDeviceControlV2*> &entry) {
      int64_t bw_in =   0;
      int64_t bw_out =  0;
      int64_t req_in =  0;
      int64_t req_out = 0;
      int64_t tag =     0;
      CalFn fn(entry.first, bw_in, bw_out, req_in, req_out, tag);
      map_.foreach_refactored(fn);
      if (OB_UNLIKELY(OB_ISNULL(entry.second))) {
      } else if (bw_in || bw_out || req_in || req_out || tag) {
        _LOG_INFO("[IO STATUS BUCKET] storage={%u, %ld, %ld}, in=[%ld / %ld]kB/s, out=[%ld / %ld]kB/s, ips=[%ld / %ld], ops=[%ld / %ld]",
                  entry.first.get_category(),
                  entry.first.get_tenant_id(),
                  entry.first.get_storage_id(),
                  bw_in / 1024,
                  entry.second->limits_[static_cast<int>(ResourceType::ibw)] / 1024,
                  bw_out / 1024,
                  entry.second->limits_[static_cast<int>(ResourceType::obw)] / 1024,
                  req_in,
                  entry.second->limits_[static_cast<int>(ResourceType::ips)],
                  req_out,
                  entry.second->limits_[static_cast<int>(ResourceType::ops)]);
      }
      return OB_SUCCESS;
    }
    const hash::ObHashMap<ObIORecordKey, ObSharedDeviceIORecord> &map_;
  };
  PrinterFn fn(io_record_map_);
  shared_device_map_v2_.foreach_refactored(fn);
}

int ObTrafficControl::set_limit(const obrpc::ObSharedDeviceResourceArray &limit)
{
  int ret = OB_SUCCESS;
  inner_calc_();
  DRWLock::RDLockGuard guard(rw_lock_);
  for (int i = 0; i < limit.array_.count(); ++i) {
    ObSharedDeviceControl *tc = nullptr;
    if (OB_ISNULL(tc = shared_device_map_.get(limit.array_.at(i).key_))) {
      ret = OB_HASH_NOT_EXIST;
      LOG_WARN("get index from map failed", K(ret));
    } else {
      tc->set_limit(limit.array_.at(i));
    }
  }
  return ret;
}

int ObTrafficControl::set_limit_v2(const obrpc::ObSharedDeviceResourceArray &limit)
{
  int ret = OB_SUCCESS;
  inner_calc_();
  DRWLock::RDLockGuard guard(rw_lock_);
  for (int i = 0; i < limit.array_.count(); ++i) {
    ObSharedDeviceControlV2 *tc = nullptr;
    if (ResourceType::tag == limit.array_.at(i).type_) {
      // Tag is currently unavailable
    } else if (ResourceType::iops == limit.array_.at(i).type_ || ResourceType::iobw == limit.array_.at(i).type_) {
    } else if (OB_UNLIKELY(static_cast<int>(ResourceType::ResourceTypeCnt) <= static_cast<int>(limit.array_.at(i).type_))) {
      LOG_ERROR("unexpected resource type", K(ret), K(limit));
    } else if (OB_FAIL(shared_device_map_v2_.get_refactored(limit.array_.at(i).key_, tc))) {
      LOG_WARN_RET(OB_HASH_NOT_EXIST, "get index from map failed", K(limit.array_.at(i).key_));
    } else if (OB_UNLIKELY(OB_ISNULL(tc))) {
      LOG_WARN_RET(OB_HASH_NOT_EXIST, "tc is not exist", K(limit.array_.at(i).key_));
    } else if (OB_SUCCESS != (tc->update_limit(limit.array_.at(i)))) {
      LOG_WARN("update shared device limit failed", K(ret), K(i), K(limit.array_.at(i)));
    }
  }
  return ret;
}

void ObTrafficControl::inner_calc_()
{
  if (REACH_TIME_INTERVAL(1 * 1000 * 1000)) {
    int ret = OB_SUCCESS;
    int64_t read_bytes = 0;
    int64_t write_bytes = 0;
    reset_pnio_statistics(&read_bytes, &write_bytes);
    net_ibw_.inc(read_bytes);
    net_obw_.inc(write_bytes);
    if (GCONF._enable_tree_based_io_scheduler == false) {
      ATOMIC_STORE(&ibw_clock_.iops_, std::max(0L, device_bandwidth_ - read_bytes));
      ATOMIC_STORE(&obw_clock_.iops_, std::max(0L, device_bandwidth_ - write_bytes));
    } else if (0 != (ret = qdisc_set_limit(OB_IO_MANAGER_V2.get_sub_root_qid((int)ObIOMode::READ), std::max(0L, device_bandwidth_ - read_bytes)))) {
      LOG_WARN("set net_in limit failed", K(ret));
    } else if (0 != (ret = qdisc_set_limit(OB_IO_MANAGER_V2.get_sub_root_qid((int)ObIOMode::WRITE), std::max(0L, device_bandwidth_ - write_bytes)))) {
      LOG_WARN("set net_out limit failed", K(ret));
    }
  }
}

int ObTrafficControl::register_bucket(ObIORequest &req, const int qid) {
  int ret = OB_SUCCESS;
  if (req.fd_.device_handle_->is_object_device()) {
    uint64_t storage_id = ((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod().storage_id_;
    uint8_t mod_id = (uint8_t)((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod().storage_used_mod_;
    ObStorageInfoType storage_type = __storage_table_mapper[mod_id];
    ObTrafficControl::ObStorageKey key(storage_id, req.tenant_id_, storage_type);
    ObIOSSGrpKey grp_key(req.tenant_id_, req.get_group_key());
    ObSharedDeviceControlV2 *tc = nullptr;
    // global register bucket
    if (OB_SUCCESS == shared_device_map_v2_.get_refactored(key, tc)) {
    } else {
      DRWLock::WRLockGuard guard(rw_lock_);
      int tmp_ret = OB_SUCCESS;
      if (OB_UNLIKELY(OB_SUCCESS == shared_device_map_v2_.get_refactored(key, tc))) {
      } else if (OB_ISNULL(tc = OB_NEW(ObSharedDeviceControlV2, SET_IGNORE_MEM_VERSION("SDCtrlV2")))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else if (OB_FAIL(tc->set_storage_key(key))) {
      } else if (OB_FAIL(tc->add_shared_device_limits())) {
        LOG_WARN("add shared device limits failed", K(ret), K(req), K(grp_key), K(qid));
      } else if (OB_FAIL(shared_device_map_v2_.set_refactored(key, tc))) {
        LOG_WARN("set map failed", K(ret));
      }
      if (OB_FAIL(ret)) {
        LOG_WARN("register bucket failed", K(ret), K(key), K(tc));
        ob_delete(tc);
      }
    }

    // register bucket for group
    if (REACH_TIME_INTERVAL(100 * 1000)) {
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(tc)) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("tc is not exist", K(ret), K(tc), K(qid), K(req));
      } else if (OB_SUCCESS == tc->is_group_key_exist(grp_key)) {
      } else {
        DRWLock::WRLockGuard guard(rw_lock_);
        if (OB_SUCCESS == tc->is_group_key_exist(grp_key)) {
        } else if (OB_FAIL(tc->add_group(grp_key, qid))) {
          LOG_WARN("add shared device limits failed", K(ret), K(grp_key), K(qid));
        }
      }
    }

    if (OB_NOT_NULL(tc)) {
      (void)tc->fill_qsched_req_storage_key(req);
    }
  }
  return ret;
}

int64_t ObTrafficControl::ObSharedDeviceControlV2::to_string(char* buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int i = 0; i < static_cast<int>(obrpc::ResourceType::ResourceTypeCnt); i++) {
    if (-1 != limit_ids_[i]) {
      J_KV("%s: id: %ld, limit: %ld, ", get_resource_type_str(static_cast<ResourceType>(i)), K(limit_ids_[i]), K(limits_[i]));
    }
  }
  J_OBJ_END();
  return pos;
}

ObTrafficControl::ObSharedDeviceControlV2::ObSDGroupList::ObSDGroupList()
{
}
ObTrafficControl::ObSharedDeviceControlV2::ObSDGroupList::~ObSDGroupList()
{
}
int ObTrafficControl::ObSharedDeviceControlV2::ObSDGroupList::clear()
{
  grp_list_.reuse();
  return OB_SUCCESS;
}

int ObTrafficControl::ObSharedDeviceControlV2::ObSDGroupList::assign(const ObSDGroupList &other)
{
  grp_list_.assign(other.grp_list_);
  return OB_SUCCESS;
}

int ObTrafficControl::ObSharedDeviceControlV2::ObSDGroupList::is_group_key_exist(const ObIOSSGrpKey &grp_key) {
  int ret = OB_SUCCESS;
  bool is_found = false;
  for (int i = 0; !is_found && i < grp_list_.count(); i++) {
    if (grp_list_.at(i) == grp_key) {
      is_found = true;
    }
  }
  if (is_found == false) {
    ret = OB_HASH_NOT_EXIST;
  }
  return ret;
}

int ObTrafficControl::gc_tenant_infos()
{
  int ret = OB_SUCCESS;
  if (REACH_TIME_INTERVAL(1 * 60 * 1000L * 1000L)) {  // 60s
    DRWLock::WRLockGuard guard(rw_lock_);
    struct GCTenantSharedDeviceInfos
    {
      GCTenantSharedDeviceInfos(
          const ObVector<uint64_t> &tenant_ids, ObSEArray<ObTrafficControl::ObStorageKey, 7> &gc_tenant_infos)
          : tenant_ids_(tenant_ids), gc_tenant_infos_(gc_tenant_infos)
      {}
      int operator()(hash::HashMapPair<ObTrafficControl::ObStorageKey, ObTrafficControl::ObSharedDeviceControl> &pair)
      {
        bool is_find = false;
        for (int i = 0; !is_find && i < tenant_ids_.size(); ++i) {
          if (0 == pair.first.get_tenant_id() || tenant_ids_.at(i) == pair.first.get_tenant_id()) {
            is_find = true;
          }
        }
        if (false == is_find) {
          gc_tenant_infos_.push_back(pair.first);
        }
        return OB_SUCCESS;
      }
      const ObVector<uint64_t> &tenant_ids_;
      ObSEArray<ObTrafficControl::ObStorageKey, 7> &gc_tenant_infos_;
    };
    struct GCTenantSharedDeviceInfosV2
    {
      GCTenantSharedDeviceInfosV2(
          const ObVector<uint64_t> &tenant_ids, ObSEArray<ObTrafficControl::ObStorageKey, 7> &gc_tenant_infos)
          : tenant_ids_(tenant_ids), gc_tenant_infos_(gc_tenant_infos)
      {}
      int operator()(hash::HashMapPair<ObTrafficControl::ObStorageKey, ObTrafficControl::ObSharedDeviceControlV2 *> &pair)
      {
        bool is_find = false;
        for (int i = 0; !is_find && i < tenant_ids_.size(); ++i) {
          if (0 == pair.first.get_tenant_id() || tenant_ids_.at(i) == pair.first.get_tenant_id()) {
            is_find = true;
          }
        }
        if (false == is_find) {
          gc_tenant_infos_.push_back(pair.first);
        }
        return OB_SUCCESS;
      }
      const ObVector<uint64_t> &tenant_ids_;
      ObSEArray<ObTrafficControl::ObStorageKey, 7> &gc_tenant_infos_;
    };
    struct GCTenantRecordInfos
    {
      GCTenantRecordInfos(
          const ObVector<uint64_t> &tenant_ids, ObSEArray<ObTrafficControl::ObIORecordKey, 7> &gc_tenant_infos)
          : tenant_ids_(tenant_ids), gc_tenant_infos_(gc_tenant_infos)
      {}
      int operator()(hash::HashMapPair<ObTrafficControl::ObIORecordKey, ObTrafficControl::ObSharedDeviceIORecord> &pair)
      {
        bool is_find = false;
        for (int i = 0; !is_find && i < tenant_ids_.size(); ++i) {
          if (0 == pair.first.id_.get_tenant_id() || tenant_ids_.at(i) == pair.first.id_.get_tenant_id()) {
            is_find = true;
          }
        }
        if (false == is_find) {
          gc_tenant_infos_.push_back(pair.first);
        }
        return OB_SUCCESS;
      }
      const ObVector<uint64_t> &tenant_ids_;
      ObSEArray<ObTrafficControl::ObIORecordKey, 7> &gc_tenant_infos_;
    };
    ObVector<uint64_t> tenant_ids;
    ObSEArray<ObTrafficControl::ObIORecordKey, 7> gc_tenant_record_infos;
    ObSEArray<ObTrafficControl::ObStorageKey, 7> gc_tenant_shared_device_infos;
    ObSEArray<ObTrafficControl::ObStorageKey, 7> gc_tenant_shared_device_infos_v2;
    GCTenantRecordInfos fn(tenant_ids, gc_tenant_record_infos);
    GCTenantSharedDeviceInfos fn2(tenant_ids, gc_tenant_shared_device_infos);
    GCTenantSharedDeviceInfosV2 fn3(tenant_ids, gc_tenant_shared_device_infos_v2);
    if(OB_ISNULL(GCTX.omt_)) {
    } else if (FALSE_IT(GCTX.omt_->get_tenant_ids(tenant_ids))) {
    } else if (OB_FAIL(io_record_map_.foreach_refactored(fn))) {
      LOG_WARN("SSNT:failed to get gc tenant record infos", K(ret));
    } else if (OB_FAIL(shared_device_map_.foreach_refactored(fn2))) {
      LOG_WARN("SSNT:failed to get gc tenant shared device infos", K(ret));
    } else if (OB_FAIL(shared_device_map_v2_.foreach_refactored(fn3))) {
      LOG_WARN("SSNT:failed to get gc tenant shared device infos", K(ret));
    } else {
      for (int i = 0; i < gc_tenant_record_infos.count(); ++i) {
        if (OB_SUCCESS != io_record_map_.erase_refactored(gc_tenant_record_infos.at(i))) {
          LOG_WARN("SSNT:failed to erase gc tenant record infos", K(ret), K(gc_tenant_record_infos.at(i)));
        } else {
          LOG_INFO("SSNT:erase gc tenant record infos", K(ret), K(gc_tenant_record_infos.at(i)));
        }
      }
      for (int i = 0; i < gc_tenant_shared_device_infos.count(); ++i) {
        if (OB_SUCCESS != shared_device_map_.erase_refactored(gc_tenant_shared_device_infos.at(i))) {
          LOG_WARN(
              "SSNT:failed to erase gc tenant shared device infos", K(ret), K(gc_tenant_shared_device_infos.at(i)));
        } else {
          LOG_INFO("SSNT:erase gc tenant shared device infos", K(ret), K(gc_tenant_shared_device_infos.at(i)));
        }
      }
      for (int i = 0; i < gc_tenant_shared_device_infos_v2.count(); ++i) {
        int tmp_ret = OB_SUCCESS;
        ObTrafficControl::ObSharedDeviceControlV2 *val_ptr = nullptr;
        if (OB_TMP_FAIL(shared_device_map_v2_.erase_refactored(gc_tenant_shared_device_infos_v2.at(i), &val_ptr))) {
          LOG_WARN("SSNT:failed to erase gc tenant shared device infos", K(tmp_ret), K(gc_tenant_shared_device_infos_v2.at(i)), K(val_ptr));
        } else if (OB_ISNULL(val_ptr)) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("SSNT:failed to erase gc tenant shared device infos", K(tmp_ret), K(gc_tenant_shared_device_infos_v2.at(i)), K(val_ptr));
        } else if (FALSE_IT(val_ptr->destroy())) {
          LOG_WARN("SSNT:failed to destroy shared device control", K(tmp_ret), K(gc_tenant_shared_device_infos_v2.at(i)), K(val_ptr));
        } else if (FALSE_IT(ob_delete(val_ptr))) {
        } else {
          LOG_INFO("SSNT:erase gc tenant shared device infos succ", K(ret), K(tmp_ret), K(gc_tenant_shared_device_infos_v2.at(i)), K(val_ptr));
        }
      }
    }
  }
  return ret;
}

ObIOManager::ObIOManager()
  : is_inited_(false),
    is_working_(false),
    mutex_(ObLatchIds::GLOBAL_IO_CONFIG_LOCK),
    io_config_(),
    allocator_(),
    fault_detector_(io_config_),
    io_scheduler_(io_config_, allocator_)
{
}

ObIOManager::~ObIOManager()
{
  destroy();
}

ObIOManager &ObIOManager::get_instance()
{
  static ObIOManager instance;
  return instance;
}

int ObIOManager::init(const int64_t memory_limit,
                      const int32_t queue_depth,
                      const int32_t schedule_thread_count)
{
  int ret = OB_SUCCESS;
  int64_t schedule_queue_count = 0 != schedule_thread_count ? schedule_thread_count : (lib::is_mini_mode() ? 2 : 8);
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_FAIL(OB_IO_MANAGER_V2.init())) {
    LOG_WARN("qsched global init fail");
  } else if (OB_UNLIKELY(memory_limit <= 0|| schedule_queue_count <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(memory_limit), K(schedule_queue_count));
  } else if (OB_FAIL(allocator_.init(OB_MALLOC_MIDDLE_BLOCK_SIZE, "IO_MGR", OB_SERVER_TENANT_ID, memory_limit))) {
    LOG_WARN("init io allocator failed", K(ret));
  } else if (OB_FAIL(channel_map_.create(7, "IO_CHANNEL_MAP"))) {
    LOG_WARN("create channel map failed", K(ret));
  } else if (OB_FAIL(io_scheduler_.init(schedule_queue_count))) {
    LOG_WARN("init io scheduler failed", K(ret));
  } else if (OB_FAIL(fault_detector_.init())) {
    LOG_WARN("init io fault detector failed", K(ret));
  } else if (OB_ISNULL(server_io_manager_ = OB_NEW(ObTenantIOManager, "IO_MGR"))) {
  } else if (OB_FAIL(server_io_manager_->init(OB_SERVER_TENANT_ID, ObTenantIOConfig::default_instance(), &io_scheduler_))) {
    LOG_WARN("init server tenant io mgr failed", K(ret));
  } else {
    ObMemAttr attr(OB_SERVER_TENANT_ID, "IO_MGR");
    SET_USE_500(attr);
    allocator_.set_attr(attr);
    io_config_.set_default_value();
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

struct DestroyChannelMapFn
{
public:
  DestroyChannelMapFn(ObIAllocator &allocator) : allocator_(allocator) {}
  int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObDeviceChannel *> &entry) {
    if (nullptr != entry.second) {
      entry.second->~ObDeviceChannel();
      allocator_.free(entry.second);
    }
    return OB_SUCCESS;
  }
private:
  ObIAllocator &allocator_;
};

struct ReloadIOConfigFn
{
public:
  ReloadIOConfigFn(const ObIOConfig &conf) : conf_(conf) {}
  int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObDeviceChannel *> &entry)
  {
    int ret = OB_SUCCESS;
    ObDeviceChannel *ch = entry.second;
    if (nullptr != ch) {
      if (OB_FAIL(ch->reload_config(conf_))) {
        LOG_WARN("reload device channel config failed", K(ret), KPC(ch));
      }
    }
    return ret;
  }
private:
  const ObIOConfig &conf_;
};

void ObIOManager::destroy()
{
  stop();
  fault_detector_.destroy();
  io_scheduler_.destroy();
  OB_IO_MANAGER_V2.wait();
  OB_IO_MANAGER_V2.destroy();
  DestroyChannelMapFn destry_channel_map_fn(allocator_);
  channel_map_.foreach_refactored(destry_channel_map_fn);
  channel_map_.destroy();
  OB_DELETE(ObTenantIOManager, "IO_MGR", server_io_manager_);
  server_io_manager_ = nullptr;
  allocator_.destroy();
  is_inited_ = false;
  LOG_INFO("io manager is destroyed");
}

int ObIOManager::start()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("IO manager not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(server_io_manager_->start())) {
    LOG_WARN("init server tenant io mgr start failed", K(ret));
  } else if (OB_FAIL(io_scheduler_.start())) {
    LOG_WARN("start io scheduler failed", K(ret));
  } else if (OB_FAIL(OB_IO_MANAGER_V2.start())) {
    LOG_WARN("start io scheduler V2 failed", K(ret));
  } else if (OB_FAIL(fault_detector_.start())) {
    LOG_WARN("start io fault detector failed", K(ret));
  } else {
    is_working_ = true;
  }
  return ret;
}

void ObIOManager::stop()
{
  is_working_ = false;
  if (OB_NOT_NULL(server_io_manager_)) {
    server_io_manager_->stop();
  }
  io_scheduler_.stop();
  OB_IO_MANAGER_V2.stop();
}

void ObIOManager::wait()
{
  io_scheduler_.wait();
}

bool ObIOManager::is_stopped() const
{
  return !is_working_;
}

int ObIOManager::read(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aio_read(info, handle))) {
    LOG_WARN("aio read failed", K(ret), K(info));
  } else if (OB_FAIL(handle.wait())) {
    LOG_WARN("io handle wait failed", K(ret), K(info), K(info.timeout_us_));
    // io callback should be freed by caller
    handle.clear_io_callback();
  }
  return ret;
}

int ObIOManager::write(const ObIOInfo &info)
{
  int ret = OB_SUCCESS;
  ObIOHandle handle;
  if (OB_FAIL(aio_write(info, handle))) {
    LOG_WARN("aio write failed", K(ret), K(info));
  } else if (OB_FAIL(handle.wait())) {
    LOG_WARN("io handle wait failed", K(ret), K(info), K(info.timeout_us_));
    // io callback should be freed by caller
    handle.clear_io_callback();
  }
  return ret;
}

int ObIOManager::aio_read(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(lbt()));
  } else if (OB_FAIL(tenant_aio(info, handle))) {
    LOG_WARN("inner aio failed", K(ret), K(info));
  }
  return ret;
}

int ObIOManager::aio_write(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(lbt()));
  } else if (OB_FAIL(tenant_aio(info, handle))) {
    LOG_WARN("inner aio failed", K(ret), K(info));
  }
  return ret;
}

int ObIOManager::pread(ObIOInfo &info, int64_t &read_size)
{
  int ret = OB_SUCCESS;
  read_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid() || nullptr == info.buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    info.flag_.set_read();
    info.flag_.set_sync();
    info.timeout_us_ = MAX_IO_WAIT_TIME_MS * 1000;
    ObIOHandle handle;
    if (OB_FAIL(tenant_aio(info, handle))) {
      LOG_WARN("do inner aio failed", K(ret), K(info));
    } else {
      while (OB_SUCC(ret) || OB_TIMEOUT == ret || OB_IO_TIMEOUT == ret) { // wait to die
        if (OB_FAIL(handle.wait(MAX_IO_WAIT_TIME_MS))) {
          if (OB_DATA_OUT_OF_RANGE != ret) {
            LOG_WARN("sync read failed", K(ret), K(info));
          }
        } else {
          break;
        }
      }
    }
    if (OB_SUCC(ret) || OB_DATA_OUT_OF_RANGE == ret) {
      read_size = handle.get_data_size();
      MEMCPY(const_cast<char *>(info.buf_), handle.get_buffer(), read_size);
    }
  }
  return ret;
}

int ObIOManager::pwrite(ObIOInfo &info, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  write_size = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid() || nullptr == info.buf_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info));
  } else {
    info.flag_.set_write();
    info.flag_.set_sync();
    info.timeout_us_ = MAX_IO_WAIT_TIME_MS * 1000;
    ObIOHandle handle;
    if (OB_FAIL(tenant_aio(info, handle))) {
      LOG_WARN("do inner aio failed", K(ret), K(info));
    } else {
      while (OB_SUCC(ret) || OB_TIMEOUT == ret || OB_IO_TIMEOUT == ret) { // wait to die
        if (OB_FAIL(handle.wait(MAX_IO_WAIT_TIME_MS))) {
          if (OB_DATA_OUT_OF_RANGE != ret) {
            LOG_WARN("sync write failed", K(ret), K(info));
          }
        } else {
          break;
        }
      }
    }
    if (OB_SUCC(ret) || OB_DATA_OUT_OF_RANGE == ret) {
      write_size = handle.get_data_size();
    }
  }
  return ret;
}

int ObIOManager::detect_read(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("io manager not inited", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("io manager not working", K(ret), K(is_working_));
  } else if (OB_UNLIKELY(!info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(info), K(lbt()));
  } else if (OB_FAIL(get_tenant_io_manager(info.tenant_id_, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(info.tenant_id_));
  } else if (OB_FAIL(tenant_holder.get_ptr()->detect_aio(info, handle))) {
    LOG_WARN("tenant io manager do aio failed", K(ret), K(info), KPC(tenant_holder.get_ptr()));
  } else if (OB_FAIL(handle.wait())) {
    LOG_WARN("io handle wait failed", K(ret), K(info));
  }
  return ret;
}

int ObIOManager::tenant_aio(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
#ifdef ERRSIM
  const ObErrsimModuleType type = THIS_WORKER.get_module_type();
  if (is_errsim_module(info.tenant_id_, type.type_)) {
    ret = OB_IO_ERROR;
    LOG_ERROR("[ERRSIM MODULE] errsim IO error", K(ret), "tenant_id", info.tenant_id_);
    return ret;
  }
#endif

  if (OB_FAIL(get_tenant_io_manager(info.tenant_id_, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(info.tenant_id_));
  } else if (OB_FAIL(tenant_holder.get_ptr()->inner_aio(info, handle))) {
    LOG_WARN("tenant io manager do aio failed", K(ret), K(info), KPC(tenant_holder.get_ptr()));
  }
  return ret;
}

int ObIOManager::adjust_tenant_clock()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else {
    ObTenantIOManager *tenant_io_mgr = nullptr;
    ObArray<ObTenantIOClock *> io_clocks;
    ObVector<uint64_t> tenant_ids;
    if (OB_NOT_NULL(GCTX.omt_)) {
      GCTX.omt_->get_tenant_ids(tenant_ids);
    }
    for (int64_t i = 0; i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (OB_FAIL(get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
      } else if (FALSE_IT(tenant_io_mgr = tenant_holder.get_ptr())) {
      } else if (OB_FAIL(io_clocks.push_back(tenant_io_mgr->get_io_clock()))) {
        LOG_WARN("push back io clock failed", K(ret), K(tenant_ids.size()));
      }
    }
    if (!io_clocks.empty()) {
      if (OB_FAIL(ObTenantIOClock::sync_clocks(io_clocks))) {
        LOG_WARN("sync io clocks failed", K(ret), K(io_clocks));
      }
    }
  }
  return ret;
}

int ObIOManager::set_io_config(const ObIOConfig &conf)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("The ObIOManager has not been inited, ", K(ret));
  } else if (OB_UNLIKELY(!conf.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("Invalid argument, ", K(conf), K(ret));
  } else {
    ObMutexGuard guard(mutex_);
    ReloadIOConfigFn fn(conf);
    if (OB_FAIL(channel_map_.foreach_refactored(fn))) {
      LOG_WARN("reload io config failed", K(ret));
    } else {
      io_config_ = conf;
    }
  }
  LOG_INFO("set io config for io manager, ", K(ret), K(conf));
  return ret;
}

const ObIOConfig &ObIOManager::get_io_config() const
{
  return io_config_;
}

ObIOFaultDetector &ObIOManager::get_device_health_detector()
{
  return fault_detector_;
}

int ObIOManager::get_device_health_status(ObDeviceHealthStatus &dhs, int64_t &device_abnormal_time)
{
  return fault_detector_.get_device_health_status(dhs, device_abnormal_time);
}

int ObIOManager::reset_device_health()
{
  int ret = OB_SUCCESS;
  fault_detector_.reset_device_health();
  return ret;
}

int ObIOManager::add_device_channel(ObIODevice *device_handle,
                                    const int64_t async_channel_thread_count,
                                    const int64_t sync_channel_thread_count,
                                    const int64_t max_io_depth)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  void *buf = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  /* object device async channel count allow set 0 */
  } else if (OB_ISNULL(device_handle) || async_channel_thread_count < 0 || sync_channel_thread_count < 0 || max_io_depth <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle), K(async_channel_thread_count), K(sync_channel_thread_count), K(max_io_depth));
  } else if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObDeviceChannel)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc device channel failed", K(ret));
  } else if (FALSE_IT(device_channel = new (buf) ObDeviceChannel)) {
  } else if (OB_FAIL(device_channel->init(device_handle,
                                          async_channel_thread_count,
                                          sync_channel_thread_count,
                                          max_io_depth,
                                          allocator_))) {
    LOG_WARN("init device_channel failed", K(ret), K(async_channel_thread_count), K(sync_channel_thread_count));
  } else if (OB_FAIL(channel_map_.set_refactored(reinterpret_cast<int64_t>(device_handle), device_channel))) {
    LOG_WARN("set channel map failed", K(ret), KP(device_handle));
  } else {
    LOG_INFO("add io device channel succ", KP(device_handle));
    device_channel = nullptr;
  }
  if (OB_UNLIKELY(nullptr != device_channel)) {
    device_channel->~ObDeviceChannel();
    allocator_.free(device_channel);
  }
  return ret;
}

int ObIOManager::remove_device_channel(ObIODevice *device_handle)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle));
  } else if (OB_FAIL(channel_map_.erase_refactored(reinterpret_cast<int64_t>(device_handle), &device_channel))) {
    LOG_WARN("remove from channel map failed", K(ret), KP(device_handle));
  } else if (nullptr != device_channel) {
    device_channel->~ObDeviceChannel();
    allocator_.free(device_channel);
  }
  return ret;
}

int ObIOManager::get_device_channel(const ObIORequest &req, ObDeviceChannel *&device_channel)
{
  // for now, different device_handle use same channel
  int ret = OB_SUCCESS;
  ObIODevice *device_handle = req.fd_.is_backup_block_file() ? &LOCAL_DEVICE_INSTANCE : req.fd_.device_handle_;
  device_channel = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(device_handle)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(device_handle));
  } else if (OB_FAIL(channel_map_.get_refactored(reinterpret_cast<int64_t>(device_handle), device_channel))) {
    LOG_WARN("get device channel failed", K(ret), KP(device_handle));
  }
  return ret;
}

int ObIOManager::refresh_tenant_io_config(const uint64_t tenant_id, const ObTenantIOConfig &tenant_io_config)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) ||
                         tenant_io_config.memory_limit_ <= 0 ||
                         tenant_io_config.callback_thread_count_ < 0 ||
                         !tenant_io_config.unit_config_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(tenant_io_config));
  } else if (OB_FAIL(get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->update_basic_io_config(tenant_io_config))) {
    LOG_WARN("update tenant io config failed", K(ret), K(tenant_id), K(tenant_io_config));
  }
  return ret;
}

// for unittest
int ObIOManager::modify_group_io_config(const uint64_t tenant_id,
                                        const uint64_t index,
                                        const int64_t min_percent,
                                        const int64_t max_percent,
                                        const int64_t weight_percent)
{
  int ret = OB_SUCCESS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("get tenant io manager failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(tenant_holder.get_ptr()->modify_group_io_config(index, min_percent, max_percent, weight_percent,
                                                                    false, false))) {
    LOG_WARN("update tenant io config failed", K(ret), K(tenant_id), K(min_percent), K(max_percent), K(weight_percent));
  } else if (OB_FAIL(tenant_holder.get_ptr()->refresh_group_io_config())) {
    LOG_WARN("fail to refresh group config", K(ret));
  }
  return ret;
}

int ObIOManager::get_tenant_io_manager(const uint64_t tenant_id, ObRefHolder<ObTenantIOManager> &tenant_holder) const
{
  int ret = OB_SUCCESS;
  if (OB_SERVER_TENANT_ID == tenant_id) {
    tenant_holder.hold(server_io_manager_);
  } else if (MTL_ID() == tenant_id) {
    ObTenantIOManager *tenant_io_mgr = MTL(ObTenantIOManager*);
    tenant_holder.hold(tenant_io_mgr);
  } else if (!is_virtual_tenant_id(tenant_id)) {
    MAKE_TENANT_SWITCH_SCOPE_GUARD(guard);
    if (OB_SUCC(guard.switch_to(tenant_id, false))) {
      ObTenantIOManager *tenant_io_mgr = MTL(ObTenantIOManager*);
      tenant_holder.hold(tenant_io_mgr);
    }
  }
  if (OB_SUCC(ret) && OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_HASH_NOT_EXIST; // for compatibility
  }
  return ret;
}

void ObIOManager::print_sender_status()
{
  char buf[256];
  char buf_arr[4][256];
  char io_status[2048] = { 0 };
  int64_t pos = 0;
  int64_t total = 0;
  int64_t total_arr[4] = {0, 0, 0, 0};
  int64_t buf_arr_pos[4] = {0, 0, 0, 0};
  for (int64_t i = 0; i < io_scheduler_.get_senders_count(); ++i) {
    ObIOSender *sender = io_scheduler_.get_sender(i);
    if (OB_NOT_NULL(sender)) {
      int64_t cnt = sender->get_queue_count();
      common::databuff_printf(buf, sizeof(buf), pos, "%ld ", cnt);
      common::databuff_printf(buf_arr[0], sizeof(buf), buf_arr_pos[0], "%ld ", sender->sender_req_local_r_count_);
      common::databuff_printf(buf_arr[1], sizeof(buf), buf_arr_pos[1], "%ld ", sender->sender_req_local_w_count_);
      common::databuff_printf(buf_arr[2], sizeof(buf), buf_arr_pos[2], "%ld ", sender->sender_req_remote_r_count_);
      common::databuff_printf(buf_arr[3], sizeof(buf), buf_arr_pos[3], "%ld ", sender->sender_req_remote_w_count_);
      total += cnt;
      total_arr[0] += sender->sender_req_local_r_count_;
      total_arr[1] += sender->sender_req_local_w_count_;
      total_arr[2] += sender->sender_req_remote_r_count_;
      total_arr[3] += sender->sender_req_remote_w_count_;
    }
  }
  if (0 != pos) {
    snprintf(io_status,
        sizeof(io_status),
        "req_in_sender, total=%ld, total_local_read=%ld, total_local_write=%ld, total_remote_read=%ld, "
        "total_remote_write=%ld, cnt=%s, local_read=%s, local_write=%s, remote_read=%s, remote_write=%s",
        total,
        total_arr[0],
        total_arr[1],
        total_arr[2],
        total_arr[3],
        buf,
        buf_arr[0],
        buf_arr[1],
        buf_arr[2],
        buf_arr[3]);
    LOG_INFO("[IO STATUS SENDER]", KCSTRING(io_status));
  }
}

void ObIOManager::print_tenant_status()
{
  int ret = OB_SUCCESS;
  ObVector<uint64_t> tenant_ids;
  if (OB_NOT_NULL(GCTX.omt_)) {
    GCTX.omt_->get_tenant_ids(tenant_ids);
    for (int64_t i = 0; OB_SUCC(ret) && i < tenant_ids.size(); ++i) {
      const uint64_t cur_tenant_id = tenant_ids.at(i);
      ObRefHolder<ObTenantIOManager> tenant_holder;
      if (is_virtual_tenant_id(cur_tenant_id)) {
        // do nothing
      } else if (OB_FAIL(get_tenant_io_manager(cur_tenant_id, tenant_holder))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("get tenant io manager failed", K(ret), K(cur_tenant_id));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        tenant_holder.get_ptr()->print_io_status();
      }
    }
  }
  if (OB_NOT_NULL(server_io_manager_)) {
    server_io_manager_->print_io_status();
  }
}

void ObIOManager::print_channel_status()
{
  struct PrintFn
  {
    int operator () (oceanbase::common::hash::HashMapPair<int64_t, ObDeviceChannel*> &entry) {
      if (OB_NOT_NULL(entry.second)) {
        entry.second->print_status();
      }
      return OB_SUCCESS;
    }
  };
  PrintFn fn;
  channel_map_.foreach_refactored(fn);
}

void ObIOManager::print_status()
{
  print_sender_status();
  print_tenant_status();
  print_channel_status();
  tc_.print_server_status();
  if (GCONF._enable_tree_based_io_scheduler == false) {
    tc_.print_bucket_status_V1();
  } else {
    tc_.print_bucket_status_V2();
  }
}

int64_t ObIOManager::get_object_storage_io_timeout_ms(const uint64_t tenant_id) const
{
  int ret = OB_SUCCESS;
  int64_t timeout_ms = DEFAULT_OBJECT_STORAGE_IO_TIMEOUT_MS;
  ObRefHolder<ObTenantIOManager> tenant_holder;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tenant id", KR(ret), K(tenant_id));
  } else if (OB_FAIL(get_tenant_io_manager(tenant_id, tenant_holder))) {
    LOG_WARN("fail to get tenant io manager", KR(ret), K(tenant_id));
  } else if (OB_ISNULL(tenant_holder.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant holder ptr is null", KR(ret));
  } else {
    timeout_ms = tenant_holder.get_ptr()->get_object_storage_io_timeout_ms();
  }
  return timeout_ms;
}

/******************             TenantIOManager              **********************/

int ObTenantIOManager::mtl_new(ObTenantIOManager *&io_service)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  io_service = nullptr;
  if (is_virtual_tenant_id(MTL_ID())) {
    // do nothing
  } else if (OB_ISNULL(buf = ob_malloc(sizeof(ObTenantIOManager), ObMemAttr(MTL_ID(), "IO_MGR")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    FLOG_WARN("failed to alloc tenant io mgr", K(ret));
  } else {
    io_service = new (buf) ObTenantIOManager();
  }
  return ret;
}

int ObTenantIOManager::mtl_init(ObTenantIOManager *&io_service)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  if (OB_ISNULL(io_service)) {
    if (is_virtual_tenant_id(tenant_id)) {
      // do nothing
    } else {
      ret = OB_INVALID_ARGUMENT;
    }
  } else if (OB_FAIL(io_service->init(tenant_id,
                                      ObTenantIOConfig::default_instance(),
                                      &OB_IO_MANAGER.io_scheduler_))) {
    FLOG_WARN("mtl iit tenant io manager failed", K(tenant_id));
  } else {
    FLOG_INFO("mtl init tenant io manager success", K(tenant_id), KPC(io_service));
  }
  return ret;
}

void ObTenantIOManager::mtl_destroy(ObTenantIOManager *&io_service)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(io_service)) {
    io_service->~ObTenantIOManager();
    ob_free(io_service);
    io_service = nullptr;
    FLOG_INFO("mtl destroy tenant io manager success", K(MTL_ID()));
  }
}

ObTenantIOManager::ObTenantIOManager()
  : is_inited_(false),
    is_working_(false),
    ref_cnt_(0),
    io_memory_limit_(0),
    request_count_(0),
    result_count_(0),
    tenant_id_(0),
    io_config_(),
    io_clock_(),
    io_allocator_(),
    io_scheduler_(nullptr),
    callback_mgr_(),
    io_config_lock_(ObLatchIds::TENANT_IO_CONFIG_LOCK),
    group_id_index_map_(),
    io_request_pool_(),
    io_result_pool_()
{

}

ObTenantIOManager::~ObTenantIOManager()
{
  destroy();
}

int ObTenantIOManager::init(const uint64_t tenant_id,
         const ObTenantIOConfig &io_config,
         ObIOScheduler *io_scheduler)
{
  int ret = OB_SUCCESS;
  const uint8_t IO_MODE_CNT = static_cast<uint8_t>(ObIOMode::MAX_MODE) + 1;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id)
        || !io_config.is_valid()
        || nullptr == io_scheduler)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id), K(io_config), KP(io_scheduler));
  } else if (OB_FAIL(init_memory_pool(tenant_id, io_config.memory_limit_))) {
    LOG_WARN("init tenant io memory pool failed", K(ret), K(io_config), K(io_memory_limit_), K(request_count_), K(request_count_));
  } else if (OB_FAIL(io_tracer_.init(tenant_id))) {
    LOG_WARN("init io tracer failed", K(ret));
  } else if (OB_FAIL(io_func_infos_.init(tenant_id))) {
    LOG_WARN("init io func infos failed", K(ret), K(tenant_id));
  } else if (OB_FAIL(io_usage_.init(tenant_id, io_config.group_configs_.count() / IO_MODE_CNT))) {
    LOG_WARN("init io usage failed", K(ret), K(io_usage_), K(io_config.group_configs_.count()));
  } else if (OB_FAIL(io_sys_usage_.init(tenant_id, SYS_MODULE_CNT))) { // local and remote
    LOG_WARN("init io usage failed", K(ret), K(io_sys_usage_), K(SYS_MODULE_CNT), K(SYS_MODULE_CNT * 2));
  } else if (OB_FAIL(io_mem_stats_.init(SYS_MODULE_CNT , io_config.group_configs_.count() / IO_MODE_CNT))) {
    LOG_WARN("init io usage failed", K(ret), K(io_mem_stats_), K(SYS_MODULE_CNT), K(io_config.group_configs_.count()));
  } else if (OB_FAIL(io_clock_.init(tenant_id , io_config, &io_usage_))) {
    LOG_WARN("init io clock failed", K(ret), K(io_config));
  } else if (OB_FAIL(io_scheduler->init_group_queues(tenant_id, io_config.group_configs_.count(), &io_allocator_))) {
    LOG_WARN("init io map failed", K(ret), K(tenant_id), K(io_allocator_));
  } else if (OB_FAIL(init_group_index_map(tenant_id, io_config))) {
    LOG_WARN("init group map failed", K(ret));
  } else if (OB_FAIL(io_config_.deep_copy(io_config))) {
    LOG_WARN("copy io config failed", K(ret), K(io_config_));
  } else if (OB_FAIL(qsched_.init(tenant_id, io_config))) {
    LOG_WARN("init qsched failed", K(ret), K(io_config));
  } else {
    tenant_id_ = tenant_id;
    io_scheduler_ = io_scheduler;
    inc_ref();
    is_inited_ = true;
  }
  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }
  return ret;
}

void ObTenantIOManager::destroy()
{
  ATOMIC_STORE(&is_working_, false);

  const int64_t start_ts = ObTimeUtility::current_time();
  if (is_inited_) {
    while (1 != get_ref_cnt()) {
      if (REACH_TIME_INTERVAL(1000L * 1000L)) { //1s
        LOG_INFO("wait tenant io manager quit", K(MTL_ID()), K(start_ts), K(get_ref_cnt()));
      }
      ob_usleep((useconds_t)10L * 1000L); //10ms
    }
    dec_ref();
    qsched_.destroy();
  }

  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(io_scheduler_) && OB_FAIL(io_scheduler_->remove_phyqueues(MTL_ID()))) {
    LOG_WARN("remove phy_queues from map failed", K(ret), K(MTL_ID()));
  }

  io_clock_.destroy();
  callback_mgr_.destroy();
  io_tracer_.destroy();
  io_scheduler_ = nullptr;
  io_memory_limit_ = 0;
  request_count_ = 0;
  result_count_ = 0;
  io_request_pool_.destroy();
  io_result_pool_.destroy();
  group_id_index_map_.destroy();
  io_allocator_.destroy();
  LOG_INFO("destroy tenant io manager success", K(tenant_id_));
  tenant_id_ = 0;
  is_inited_ = false;
}

int ObTenantIOManager::start()
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_QUEUE_DEPTH = 100000;
  int64_t callback_thread_count = io_config_.get_callback_thread_count();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (is_working()) {
    // do nothing
  } else if (OB_FAIL(callback_mgr_.init(tenant_id_, callback_thread_count, DEFAULT_QUEUE_DEPTH, &io_allocator_))) {
    LOG_WARN("init callback manager failed", K(ret), K(tenant_id_), K(callback_thread_count));
  } else {
    is_working_ = true;
    int tmp_ret = OB_IO_MANAGER.adjust_tenant_clock();
    if (OB_UNLIKELY(OB_SUCCESS != tmp_ret)) {
      LOG_WARN("adjust tenant clock failed", K(tmp_ret));
    }
  }
  return ret;
}

void ObTenantIOManager::stop()
{
  ATOMIC_STORE(&is_working_, false);
  callback_mgr_.destroy();
}

bool ObTenantIOManager::is_working() const
{
  return ATOMIC_LOAD(&is_working_);
}

int ObTenantIOManager::calc_io_memory(const uint64_t tenant_id, const int64_t memory)
{
  int ret = OB_SUCCESS;
  int64_t memory_benchmark = memory / (1L * 1024L * 1024L * 1024L); //base ob 1G
  //1w req占用1.52M
  //1w result占用2.44M
  if (lib::is_mini_mode() && OB_SERVER_TENANT_ID == tenant_id) {
    request_count_ = 5000;
    result_count_ = 5000;
    io_memory_limit_ = 256L * 1024L * 1024L;
  } else if (memory_benchmark <= 1) {
    //1G租户上限共256MB，预分配5w个request(7.6MB)和result(12.2MB)
    request_count_ = 50000;
    result_count_ = 50000;
    io_memory_limit_ = 256L * 1024L * 1024L;
  } else if (memory_benchmark <= 4) {
    //4G租户上限共1G，预分配10w个request(15.2MB)和result(24.4MB)
    request_count_ = 100000;
    result_count_ = 100000;
    io_memory_limit_ = 1024 * 1024L * 1024L;
  } else if (memory_benchmark <= 8) {
    //8G租户上限共2G，预分配20w个request和result
    request_count_ = 200000;
    result_count_ = 200000;
    io_memory_limit_ = 2048L * 1024L * 1024L;
  } else {
    //unlimited，预分配30w个request和result
    request_count_ = 300000;
    result_count_ = 300000;
    io_memory_limit_ = memory;
  }
  LOG_INFO("calc tenant io memory success", K(memory), K(io_memory_limit_), K(request_count_), K(request_count_));
  return ret;
}

int ObTenantIOManager::init_memory_pool(const uint64_t tenant_id, const int64_t memory)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id <= 0 || memory <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io argument", K(ret), K(tenant_id), K(memory));
  } else if (OB_FAIL(calc_io_memory(tenant_id, memory))) {
    LOG_WARN("calc tenant io memory failed", K(ret), K(memory), K(io_memory_limit_), K(request_count_), K(request_count_));
  } else if (OB_FAIL(io_allocator_.init(tenant_id, io_memory_limit_))) {
    LOG_WARN("init io allocator failed", K(ret), K(tenant_id), K(io_memory_limit_));
  } else if (OB_FAIL(io_request_pool_.init(request_count_, io_allocator_))) {
    LOG_WARN("failed to init request memory pool", K(ret), K(request_count_));
  } else if (OB_FAIL(io_result_pool_.init(result_count_, io_allocator_))) {
    LOG_WARN("failed to init result memory pool", K(ret), K(result_count_));
  } else {
    LOG_INFO("init tenant io memory pool success", K(tenant_id), K(memory), K(io_memory_limit_), K(request_count_), K(request_count_));
  }
  return ret;
}

int ObTenantIOManager::update_memory_pool(const int64_t memory)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(memory <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid io argument", K(ret), K(memory));
  } else if (OB_FAIL(calc_io_memory(tenant_id_, memory))) {
    LOG_WARN("calc tenant io memory failed", K(ret), K(memory), K(io_memory_limit_), K(request_count_), K(request_count_));
  } else if (OB_FAIL(io_allocator_.update_memory_limit(io_memory_limit_))) {
    LOG_WARN("update io memory limit failed", K(ret), K(io_memory_limit_));
  } else {
    LOG_INFO("update tenant io memory pool success", K(memory), K(io_memory_limit_), K(request_count_), K(request_count_));
  }
  //todo qilu :update three pool
  return ret;
}

int ObTenantIOManager::alloc_and_init_result(const ObIOInfo &info, ObIOResult *&io_result)
{
  int ret = OB_SUCCESS;
  io_result = nullptr;
  if (OB_FAIL(io_result_pool_.alloc(io_result))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("failed to alloc io result from fixed size pool", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_FAIL(alloc_io_result(io_result))) {
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          LOG_WARN("alloc io result failed, retry until timeout", K(ret));
          //blocking foreground thread
          ret = OB_SUCCESS;
          if (OB_FAIL(try_alloc_result_until_timeout(ObTimeUtility::current_time() + info.timeout_us_, io_result))) {
            LOG_WARN("retry alloc io result failed", K(ret));
          }
        } else {
          LOG_WARN("alloc io result failed", K(ret), KP(io_result));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(io_result)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("io result is null", K(ret));
        } else if (OB_FAIL(io_result->basic_init())) {
          LOG_WARN("basic init io result failed", K(ret));
        }
      }
    }
  } else if (OB_ISNULL(io_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", K(ret));
  } else {
    io_result->tenant_io_mgr_.hold(this);
  }

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(io_result->io_callback_ = info.callback_)) {
  } else if (OB_FAIL(io_result->init(info))) {
    LOG_WARN("init io result failed", K(ret), KPC(io_result));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(io_result)) {
    if (io_result_pool_.contain(io_result)) {
      io_result->reset();
      io_result_pool_.recycle(io_result);
    } else {
      // destroy will be called when free
      io_allocator_.free(io_result);
    }
  }
  return ret;
}

//prepare request and result
int ObTenantIOManager::alloc_req_and_result(const ObIOInfo &info, ObIOHandle &handle, ObIORequest *&io_request, RequestHolder &req_holder)
{
  int ret = OB_SUCCESS;
  ObIOResult *io_result = nullptr;
  if (OB_FAIL(alloc_and_init_result(info, io_result))) {
    LOG_WARN("fail to alloc and init io result", K(ret));
  } else if (OB_ISNULL(io_result)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io result is null", K(ret));
  } else if (OB_FAIL(handle.set_result(*io_result))) {
    LOG_WARN("fail to set result to handle", K(ret), KPC(io_result));
  } else if (OB_FAIL(io_request_pool_.alloc(io_request))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("failed to alloc io io request from fixed size pool", K(ret));
    } else {
      ret = OB_SUCCESS;
      if (OB_FAIL(alloc_io_request(io_request))) {
        if (OB_ALLOCATE_MEMORY_FAILED == ret) {
          LOG_WARN("alloc io request failed, retry until timeout", K(ret));
          //blocking foreground thread
          ret = OB_SUCCESS;
          if (OB_FAIL(try_alloc_req_until_timeout(ObTimeUtility::current_time() + info.timeout_us_, io_request))) {
            LOG_WARN("retry alloc io request failed", K(ret));
          }
        } else {
          LOG_WARN("alloc io request failed", K(ret), KP(io_request));
        }
      }
      if (OB_SUCC(ret)) {
        if (OB_ISNULL(io_request)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("io request is null", K(ret));
        } else if (OB_FAIL(io_request->basic_init())) {
          LOG_WARN("basic init io request failed", K(ret));
        }
      }
    }
  } else {
    io_request->tenant_io_mgr_.hold(this);
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(io_request->init(info, io_result))) {
    LOG_WARN("init io request failed", K(ret), KP(io_request));
  }

  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(io_request)) {
      //free io_request manually
      io_request->free();
    }
  } else {
    req_holder.hold(io_request);
  }
  return ret;
}

int ObTenantIOManager::inner_aio(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  ObIORequest *req = nullptr;
  RequestHolder req_holder;
  logservice::coordinator::ObFailureDetector *detector = MTL(logservice::coordinator::ObFailureDetector *);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if ((SLOG_IO != info.flag_.get_sys_module_id() &&
              CLOG_READ_IO != info.flag_.get_sys_module_id() && CLOG_WRITE_IO != info.flag_.get_sys_module_id()) &&
              NULL != detector && detector->is_data_disk_has_fatal_error()) {
    ret = OB_DISK_HUNG;
    // for temporary positioning issue, get lbt of log replay
    LOG_DBA_ERROR(OB_DISK_HUNG, "msg", "disk has fatal error");
  } else if (OB_FAIL(alloc_req_and_result(info, handle, req, req_holder))) {
    LOG_WARN("pre set io args failed", K(ret), K(info));
  } else if (GCONF._enable_tree_based_io_scheduler) {
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(qsched_.schedule_request(*req))) {
      LOG_WARN("schedule request failed", K(ret), KPC(req));
    }
  } else if (OB_FAIL(io_scheduler_->schedule_request(*req))) {
    LOG_WARN("schedule request failed", K(ret), KPC(req));
  }
  if (OB_FAIL(ret)) {
    // io callback should be freed by caller
    handle.clear_io_callback();
    handle.reset();
  }
  return ret;
}

int ObTenantIOManager::detect_aio(const ObIOInfo &info, ObIOHandle &handle)
{
  int ret = OB_SUCCESS;
  handle.reset();
  ObIORequest *req = nullptr;
  RequestHolder req_holder;
  ObDeviceChannel *device_channel = nullptr;
  ObTimeGuard time_guard("detect_aio_request", 100000); //100ms

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(info.callback_ != nullptr || info.user_data_buf_ != nullptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("callback and user_data_bug should be nullptr", K(ret), K(info.callback_));
  } else if (OB_FAIL(alloc_req_and_result(info, handle, req, req_holder))) {
    LOG_WARN("pre set io args failed", K(ret), K(info));
  } else if (OB_FAIL(req->prepare())) {
    LOG_WARN("prepare io request failed", K(ret), K(req));
  } else if (FALSE_IT(time_guard.click("prepare_detect_req"))) {
  } else if (OB_FAIL(OB_IO_MANAGER.get_device_channel(*req, device_channel))) {
    LOG_WARN("get device channel failed", K(ret), K(req));
  } else {
    if (OB_ISNULL(req->io_result_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io result is null", K(ret));
    } else {
      ObThreadCondGuard guard(req->io_result_->cond_);
      if (OB_FAIL(guard.get_ret())) {
        LOG_ERROR("fail to guard master condition", K(ret));
      } else if (req->is_canceled()) {
        ret = OB_CANCELED;
      } else if (OB_FAIL(device_channel->submit(*req))) {
        if (OB_EAGAIN != ret) {
          LOG_WARN("submit io request failed", K(ret), K(*req), KPC(device_channel));
        }
      } else {
        time_guard.click("device_submit_detect");
      }
    }
  }
  if (time_guard.get_diff() > 100000) {// 100ms
    //print req
    LOG_INFO("submit_detect_request cost too much time", K(ret), K(time_guard), K(req));
  }
  if (OB_FAIL(ret)) {
    handle.reset();
  }
  return ret;
}

int ObTenantIOManager::enqueue_callback(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_FAIL(callback_mgr_.enqueue_callback(req))) {
    LOG_WARN("push io request into callback queue failed", K(ret), K(req));
  }
  return ret;
}
int ObTenantIOManager::retry_io(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (GCONF._enable_tree_based_io_scheduler) {
    if (OB_FAIL(qsched_.schedule_request(req))) {
      LOG_WARN("retry io request failed", K(ret), K(req));
    }
  } else if (OB_FAIL(io_scheduler_->retry_request(req))) {
    LOG_WARN("retry io request into sender failed", K(ret), K(req));
  }
  return ret;
}

int ObTenantIOManager::update_basic_io_config(const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  bool need_adjust_callback = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else {
    // update basic io config
    if (io_config_.unit_config_.weight_ != io_config.unit_config_.weight_
        || io_config_.unit_config_.max_iops_ != io_config.unit_config_.max_iops_
        || io_config_.unit_config_.min_iops_ != io_config.unit_config_.min_iops_
        || io_config_.unit_config_.max_net_bandwidth_ != io_config.unit_config_.max_net_bandwidth_
        || io_config_.unit_config_.net_bandwidth_weight_ != io_config.unit_config_.net_bandwidth_weight_) {
      LOG_INFO("update io unit config", K(tenant_id_), K(io_config.unit_config_), K(io_config_.unit_config_));
      io_config_.unit_config_ = io_config.unit_config_;
      if (OB_FAIL(io_clock_.update_io_clocks(io_config_))) {
        LOG_WARN("update io clock unit config failed", K(ret), K(tenant_id_), K(io_config_), K(io_config), K(io_clock_));
      } else if (OB_FAIL(qsched_.update_config(io_config_))) {
        LOG_WARN("refresh tenant io config failed", K(ret), K(io_config_));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (io_config_.enable_io_tracer_ != io_config.enable_io_tracer_) {
      LOG_INFO("update io tracer", K(tenant_id_), K(io_config.enable_io_tracer_), K(io_config_.enable_io_tracer_));
      ATOMIC_SET(&io_config_.enable_io_tracer_, io_config.enable_io_tracer_);
      if (!io_config.enable_io_tracer_) {
        io_tracer_.reuse();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (io_config_.memory_limit_ != io_config.memory_limit_) {
      LOG_INFO("update io memory limit", K(tenant_id_), K(io_config.memory_limit_), K(io_config_.memory_limit_));
      if (OB_FAIL(update_memory_pool(io_config.memory_limit_))) {
        LOG_WARN("fail to update tenant io manager memory pool", K(ret), K(io_memory_limit_), K(io_config.memory_limit_));
      } else {
        io_config_.memory_limit_ = io_config.memory_limit_;
        need_adjust_callback = true;
      }
    }
    if (OB_FAIL(ret)) {
    } else if (io_config_.callback_thread_count_ != io_config.callback_thread_count_) {
      LOG_INFO("update io callback thread count", K(tenant_id_), K(io_config.callback_thread_count_), K(io_config_.callback_thread_count_));
      io_config_.callback_thread_count_ = io_config.callback_thread_count_;
      need_adjust_callback = true;
    }
    if (OB_SUCC(ret) && need_adjust_callback) {
      int64_t callback_thread_count = io_config_.get_callback_thread_count();
      MTL_SWITCH(tenant_id_) {
        if (OB_FAIL(callback_mgr_.update_thread_count(callback_thread_count))) {
          LOG_WARN("callback manager adjust thread failed", K(ret), K(io_config));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (io_config_.object_storage_io_timeout_ms_ != io_config.object_storage_io_timeout_ms_) {
      LOG_INFO("update object storage io timeout ms", K_(tenant_id), "ori_object_storage_io_timeout_ms",
               io_config_.object_storage_io_timeout_ms_, "new_object_storage_io_timeout_ms",
               io_config.object_storage_io_timeout_ms_);
      io_config_.object_storage_io_timeout_ms_ = io_config.object_storage_io_timeout_ms_;
    }
  }
  return ret;
}

int ObTenantIOManager::try_alloc_req_until_timeout(const int64_t timeout_ts, ObIORequest *&req)
{
  int ret = OB_SUCCESS;
  int64_t retry_alloc_count = 0;
  while (OB_SUCC(ret)) {
    ++retry_alloc_count;
    const int64_t current_ts = ObTimeUtility::current_time();
    if (current_ts > timeout_ts) {
      ret = OB_TIMEOUT;
      LOG_WARN("current time is larger than the timeout timestamp", K(ret), K(current_ts), K(timeout_ts), K(retry_alloc_count));
    } else if (OB_FAIL(alloc_io_request(req))) {
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        const int64_t remain_time = timeout_ts - current_ts;
        const int64_t sleep_time = MIN(remain_time, 1000L);
        if (TC_REACH_TIME_INTERVAL(1000L * 1000L)) {
          LOG_INFO("execute failed, retry later", K(ret), K(remain_time), K(sleep_time), K(retry_alloc_count));
        }
        ob_usleep((useconds_t)sleep_time);
        ret = OB_SUCCESS;
      }
    } else {
      LOG_INFO("retry alloc io_request success", K(retry_alloc_count));
      break;
    }
  }
  return ret;
}

int ObTenantIOManager::try_alloc_result_until_timeout(const int64_t timeout_ts, ObIOResult *&result)
{
  int ret = OB_SUCCESS;
  int64_t retry_alloc_count = 0;
  while (OB_SUCC(ret)) {
    ++retry_alloc_count;
    const int64_t current_ts = ObTimeUtility::current_time();
    if (current_ts > timeout_ts) {
      ret = OB_TIMEOUT;
      LOG_WARN("current time is larger than the timeout timestamp", K(ret), K(current_ts), K(timeout_ts), K(retry_alloc_count));
    } else if (OB_FAIL(alloc_io_result(result))) {
      if (OB_ALLOCATE_MEMORY_FAILED == ret) {
        const int64_t remain_time = timeout_ts - current_ts;
        const int64_t sleep_time = MIN(remain_time, 1000L);
        if (TC_REACH_TIME_INTERVAL(1000L * 1000L)) {
          LOG_INFO("execute failed, retry later", K(ret), K(remain_time), K(sleep_time), K(retry_alloc_count));
        }
        ob_usleep((useconds_t)sleep_time);
        ret = OB_SUCCESS;
      }
    } else {
      LOG_INFO("retry alloc io_result success", K(retry_alloc_count));
      break;
    }
  }
  return ret;
}

int ObTenantIOManager::alloc_io_request(ObIORequest *&req)
{
  int ret = OB_SUCCESS;
  req = nullptr;
  void *buf = nullptr;
#ifdef OB_BUILD_SHARED_STORAGE
  if (OB_ISNULL(buf = io_allocator_.alloc(sizeof(ObSSIORequest)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObSSIORequest)));
  } else {
    req = new (buf) ObSSIORequest;
    req->tenant_io_mgr_.hold(this);
  }
#else
  if (OB_ISNULL(buf = io_allocator_.alloc(sizeof(ObIORequest)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObIORequest)));
  } else {
    req = new (buf) ObIORequest;
    req->tenant_io_mgr_.hold(this);
  }
#endif
  return ret;
}

int ObTenantIOManager::alloc_io_result(ObIOResult *&result)
{
  int ret = OB_SUCCESS;
  result = nullptr;
  void *buf = nullptr;
  if (OB_ISNULL(buf = io_allocator_.alloc(sizeof(ObIOResult)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("allocate memory failed", K(ret), K(sizeof(ObIORequest)));
  } else {
    result = new (buf) ObIOResult;
    result->tenant_io_mgr_.hold(this);
  }
  return ret;
}

int ObTenantIOManager::init_group_index_map(const int64_t tenant_id,
                                            const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(tenant_id, "GROUP_INDEX_MAP");
  if (OB_FAIL(group_id_index_map_.create(7, attr, attr))) {
    LOG_WARN("create group index map failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < io_config.group_configs_.count(); ++i) {
      const ObTenantIOConfig::GroupConfig &config = io_config.group_configs_.at(i);
      ObIOGroupKey key(config.group_id_, config.mode_);
      if (OB_FAIL(group_id_index_map_.set_refactored(key, i, 1 /*overwrite*/))) {
        LOG_WARN("init group_index_map failed", K(ret), K(i));
      }
    }
  }
  return ret;
}

int ObTenantIOManager::get_group_index(const ObIOGroupKey &key, uint64_t &index)
{
  // IOMode in key is correct, no need to consider object device.
  int ret = OB_SUCCESS;
  if (!is_resource_manager_group(key.group_id_)) {
    index = (uint64_t)(key.mode_);
  } else if (OB_FAIL(group_id_index_map_.get_refactored(key, index))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get index from map failed", K(ret), K(key.group_id_), K(index));
    }
  }
  return ret;
}

int ObTenantIOManager::get_group_config(const ObIOGroupKey &key, ObTenantIOConfig::GroupConfig &group_config) const
{
  int ret = OB_SUCCESS;
  uint64_t index = INT64_MAX;
  if (OB_UNLIKELY(!is_resource_manager_group(key.group_id_))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid group id", K(ret), K(key));
  } else if (OB_FAIL(group_id_index_map_.get_refactored(key, index))) {
    if(OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("get index from map failed", K(ret), K(key), K(index));
    }
  } else if (OB_UNLIKELY(index < 0 || index >= io_config_.group_configs_.count())) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(index));
  } else {
    group_config = io_config_.group_configs_.at(index);
  }
  return ret;
}

int ObTenantIOManager::modify_group_io_config(const uint64_t index,
                                              const int64_t min_percent,
                                              const int64_t max_percent,
                                              const int64_t weight_percent,
                                              const bool deleted,
                                              const bool cleared)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (index < 0 || (index >= io_config_.group_configs_.count()) ||
             min_percent < 0 || min_percent > 100 ||
             max_percent < 0 || max_percent > 100 ||
             max_percent < min_percent ||
             weight_percent < 0 || weight_percent > 100) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(index), K(min_percent), K(max_percent), K(weight_percent));
  } else {
    io_config_.group_configs_.at(index).min_percent_ = min_percent;
    io_config_.group_configs_.at(index).max_percent_ = max_percent;
    io_config_.group_configs_.at(index).weight_percent_ = weight_percent;
    io_config_.group_configs_.at(index).cleared_ = cleared;
    io_config_.group_configs_.at(index).deleted_ = deleted;
    io_config_.group_config_change_ = true;
  }
  return ret;
}

int ObTenantIOManager::modify_io_config(const uint64_t group_id,
                                        const char *group_name,
                                        const int64_t min_percent,
                                        const int64_t max_percent,
                                        const int64_t weight_percent,
                                        const int64_t max_net_bandwidth_percent,
                                        const int64_t net_bandwidth_weight_percent)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_valid_resource_group(group_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group id", K(ret), K(tenant_id_), K(group_id));
  } else if (min_percent < 0 || min_percent > 100 ||
             max_percent < 0 || max_percent > 100 ||
             max_percent < min_percent ||
             weight_percent < 0 || weight_percent > 100 ||
             max_net_bandwidth_percent < 0 || max_net_bandwidth_percent > 100 ||
             net_bandwidth_weight_percent < 0 || net_bandwidth_weight_percent > 100) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("invalid group config", K(ret), K(tenant_id_), K(min_percent), K(max_percent), K(weight_percent),
                                    K(max_net_bandwidth_percent), K(net_bandwidth_weight_percent));
  } else {
    for (uint8_t i = (uint8_t)ObIOMode::READ; i <= (uint8_t)ObIOMode::MAX_MODE && OB_SUCC(ret); ++i) {
      uint64_t index = INT64_MAX;
      ObIOMode mode = (ObIOMode)i;
      ObIOGroupKey key(group_id, mode);
      int64_t min = 0;
      int64_t max = 0;
      int64_t weight = 0;
      if (ObIOMode::MAX_MODE == mode) {
        min = min_percent;
        max = max_percent;
        weight = weight_percent;
      } else {
        min = 0;
        max = max_net_bandwidth_percent;
        weight = net_bandwidth_weight_percent;
      }
      DRWLock::WRLockGuard guard(io_config_lock_);
      if (OB_FAIL(get_group_index(key, index))) {
        if (OB_HASH_NOT_EXIST == ret) {
          //1. add new group
          int64_t group_num = io_config_.group_configs_.count();
          if (OB_FAIL(io_config_.add_single_group_config(tenant_id_, key, group_name, min, max, weight))) {
            LOG_WARN("init single group failed", K(group_id));
          } else if (OB_FAIL(group_id_index_map_.set_refactored(key, group_num, 1))) {// overwrite
            LOG_WARN("set group_id and index into map failed", K(ret), K(group_id), K(group_num));
          } else {
            io_config_.group_config_change_ = true;
            LOG_INFO("add group config success", K(group_id), K(io_config_), K(group_num));
          }
        } else {
          LOG_WARN("get group index failed", K(ret), K(tenant_id_), K(group_id));
        }
      } else {
        //2. modify exits groups
        if (index < 0 || (index >= io_config_.group_configs_.count())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("invalid index", K(ret), K(index), K(io_config_.group_configs_.count()));
        } else if (io_config_.group_configs_.at(index).cleared_) {
          io_config_.group_configs_.at(index).cleared_ = false;
        } else if (io_config_.group_configs_.at(index).min_percent_ == min &&
                   io_config_.group_configs_.at(index).max_percent_ == max &&
                   io_config_.group_configs_.at(index).weight_percent_ == weight) {
          //config did not change, do nothing
        } else if (OB_FAIL(modify_group_io_config(index, min, max, weight))) {
          LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(min), K(max), K(weight));
        }
      }
    }
  }
  return ret;
}

int ObTenantIOManager::reset_all_group_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else {
    DRWLock::WRLockGuard guard(io_config_lock_);
    for (int64_t i = 0; i < io_config_.group_configs_.count(); ++i) {
      if (io_config_.group_configs_.at(i).deleted_) {
        //do nothing
      } else if (OB_FAIL(modify_group_io_config(i,
                                                0, 100, 0, /*min_iops, max_iops, weight_iops*/
                                                false, true/*cleared*/))) {
        LOG_WARN("modify group io config failed", K(ret), K(i));
      }
    }
    if(OB_SUCC(ret)) {
      LOG_INFO ("stop all group io control success when delete plan", K(tenant_id_), K(io_config_));
    }
  }
  return ret;
}

int ObTenantIOManager::reset_consumer_group_config(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_resource_manager_group(group_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("cannot reset other group io config", K(ret), K(group_id));
  } else {
    for (uint8_t i = (uint8_t)ObIOMode::READ; i <= (uint8_t)ObIOMode::MAX_MODE; ++i) {
      // 对应group资源清零
      uint64_t index = INT_MAX64;
      DRWLock::WRLockGuard guard(io_config_lock_);
      ObIOMode mode = (ObIOMode)i;
      ObIOGroupKey key(group_id, mode);
      if (OB_FAIL(get_group_index(key, index))) {
        if (OB_HASH_NOT_EXIST == ret) {
          //directive not flush yet, do nothing
          ret = OB_SUCCESS;
          LOG_INFO("directive not flush yet", K(group_id));
        } else {
          LOG_WARN("get index from map failed", K(ret), K(group_id), K(index));
        }
      } else if (OB_UNLIKELY(index < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index, maybe try to reset OTHER_GROUPS or deleted_groups", K(ret), K(index), K(group_id));
      } else if (OB_FAIL(modify_group_io_config(index, 0, 100, 0, false, true/*cleared*/))) {
        LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(index));
      } else {
        LOG_INFO ("stop group io control success when delete directive", K(tenant_id_), K(group_id), K(index), K(io_config_));
      }
    }
  }
  return ret;
}

int ObTenantIOManager::delete_consumer_group_config(const int64_t group_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_UNLIKELY(!is_resource_manager_group(group_id))) {
    ret = OB_INVALID_CONFIG;
    LOG_WARN("cannot delete other group io config", K(ret), K(group_id));
  } else {
    // 1.map释放对应的group
    // 2.config设为unusable，资源清零
    // 3.phyqueue停止接受新请求，但是不会析构
    // 4.clock设置为stop，但是不会析构
    // 5.io_usage暂不处理
    for (uint8_t i = (uint8_t)ObIOMode::READ; i <= (uint8_t)ObIOMode::MAX_MODE; ++i) {
      uint64_t index = INT64_MAX;
      DRWLock::WRLockGuard guard(io_config_lock_);
      ObIOMode mode = (ObIOMode)i;
      ObIOGroupKey key(group_id, mode);
      if (OB_FAIL(get_group_index(key, index))) {
        if (OB_HASH_NOT_EXIST == ret) {
          //GROUP 没有在map里，可能是没有指定资源，io未对其进行隔离或还未下刷
          ret = OB_SUCCESS;
          LOG_INFO("io control not active for this group", K(group_id));
        } else {
          LOG_WARN("get index from map failed", K(ret), K(group_id), K(index));
        }
      } else if (OB_UNLIKELY(index < 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid index, maybe try to delete OTHER_GROUPS or deleted_groups", K(ret), K(index), K(group_id));
      } else {
        if (OB_FAIL(group_id_index_map_.erase_refactored(key))) {
          LOG_WARN("stop phy queues failed", K(ret), K(tenant_id_), K(index));
        } else if (OB_FAIL(modify_group_io_config(index, 0, 100, 0, true/*deleted*/, false))) {
          LOG_WARN("modify group io config failed", K(ret), K(tenant_id_), K(index));
        }
      }
      if (OB_SUCC(ret) && index != 0 && index != INT64_MAX) {
        if (OB_FAIL(io_scheduler_->stop_phy_queues(tenant_id_, index))) {
          LOG_WARN("stop phy queues failed", K(ret), K(tenant_id_), K(index));
        } else {
          io_clock_.stop_clock(index);
          LOG_INFO ("stop group io control success when delete group", K(tenant_id_), K(group_id), K(index), K(io_config_));
        }
      }
    }
  }
  return ret;
}

int ObTenantIOManager::refresh_group_io_config()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_working())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("tenant not working", K(ret), K(tenant_id_));
  } else if (OB_LIKELY(!io_config_.group_config_change_)) {
    // group config not change, do nothing
  } else if (OB_FAIL(io_usage_.refresh_group_num(io_config_.group_configs_.count() / 3))) {
    LOG_WARN("refresh io usage array failed", K(ret), K(io_config_.group_configs_.count()));
  } else if (OB_FAIL(io_mem_stats_.get_mem_stat().refresh_group_num(io_config_.group_configs_.count() / 3))) {
    LOG_WARN("refresh mem array failed", K(ret), K(io_config_.group_configs_.count()));
  } else if (OB_FAIL(io_scheduler_->update_group_queues(tenant_id_, io_config_.group_configs_.count()))) {
    LOG_WARN("refresh phyqueue num failed", K(ret), K(io_config_.group_configs_.count()));
  } else if (OB_FAIL(io_clock_.update_io_clocks(io_config_))) {
    LOG_WARN("refresh io clock failed", K(ret), K(io_config_));
  } else if (OB_FAIL(qsched_.update_config(io_config_))) {
    LOG_WARN("refresh io config failed", K(ret), K(io_config_));
  } else {
    LOG_INFO("refresh group io config success", K(tenant_id_), K(io_config_));
    io_config_.group_config_change_ = false;
  }

  return ret;
}

const ObTenantIOConfig &ObTenantIOManager::get_io_config()
{
  return io_config_;
}

int ObTenantIOManager::trace_request_if_need(const ObIORequest *req, const char* msg, ObIOTracer::TraceType trace_type)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_LIKELY(!ATOMIC_LOAD(&io_config_.enable_io_tracer_))) {
    // do nothing
  } else if (OB_FAIL(io_tracer_.trace_request(req, msg, trace_type))) {
    LOG_WARN("trace io request failed", K(ret), KP(req), KCSTRING(msg), K(trace_type));
  }
  return ret;
}

int64_t ObTenantIOManager::get_group_num()
{
  DRWLock::RDLockGuard guard(io_config_lock_);
  const uint64_t MODE_CNT = static_cast<uint64_t>(ObIOMode::MAX_MODE) + 1;
  int64_t group_num = io_config_.group_configs_.count() / MODE_CNT;
  return group_num;
}


int ObTenantIOManager::print_io_status()
{
  int ret = OB_SUCCESS;
  if (is_working() && is_inited_) {
    char io_status[1024] = { 0 };
    bool need_print_io_config = false;
    io_usage_.calculate_io_usage();
    io_sys_usage_.calculate_io_usage();
    const ObSEArray<ObIOUsageInfo, GROUP_START_NUM> &info = io_usage_.get_io_usage();
    const ObSEArray<ObIOUsageInfo, GROUP_START_NUM> &sys_info = io_sys_usage_.get_io_usage();
    ObSEArray<ObIOFailedReqUsageInfo, GROUP_START_NUM> &failed_req_info = io_usage_.get_failed_req_usage();
    ObSEArray<ObIOFailedReqUsageInfo, GROUP_START_NUM> &sys_failed_req_info = io_sys_usage_.get_failed_req_usage();
    const ObIOMemStat &sys_mem_stat = io_mem_stats_.get_sys_mem_stat();
    const ObIOMemStat &mem_stat = io_mem_stats_.get_mem_stat();
    const int64_t MODE_COUNT = static_cast<int64_t>(ObIOMode::MAX_MODE) + 1;
    const int64_t GROUP_MODE_CNT = static_cast<int64_t>(ObIOGroupMode::MODECNT);
    int64_t ips = 0;
    int64_t ops = 0;
    int64_t ibw = 0;
    int64_t obw = 0;
    double failed_ips = 0;
    double failed_ops = 0;
    int64_t failed_ibw = 0;
    int64_t failed_obw = 0;
    uint64_t group_config_index = 0;
    ObIOMode mode = ObIOMode::MAX_MODE;
    ObIOGroupMode group_mode = ObIOGroupMode::MODECNT;
    int tmp_ret = OB_SUCCESS;
    for (int64_t i = 0; i < info.count(); ++i) {
      if (OB_TMP_FAIL(transform_usage_index_to_group_config_index(i, group_config_index))) {
        continue;
      } else if (group_config_index >= io_config_.group_configs_.count() || group_config_index >= io_clock_.get_group_clocks_count() || info.count() != failed_req_info.count() || info.count() != mem_stat.group_mem_infos_.count()) {
        continue;
      }
      mode = static_cast<ObIOMode>(group_config_index % MODE_COUNT);
      group_mode = static_cast<ObIOGroupMode>(i % GROUP_MODE_CNT);
      ObTenantIOConfig::GroupConfig &group_config = io_config_.group_configs_.at(group_config_index);
      if (group_config.deleted_) {
        continue;
      }
      const char *group_name = i < 4 ? "OTHER_GROUPS" : group_config.group_name_;
      const char *mode_str = get_io_mode_string(group_mode);
      int64_t group_bw = 0;
      double failed_avg_size = 0;
      double failed_req_iops = 0;
      int64_t failed_req_bw = 0;
      int64_t failed_avg_prepare_delay = 0;
      int64_t failed_avg_schedule_delay = 0;
      int64_t failed_avg_submit_delay = 0;
      int64_t failed_avg_device_delay = 0;
      int64_t failed_avg_total_delay = 0;
      double iops_scale = 1.0;
      double failed_iops_scale = 1.0;
      bool is_io_ability_valid = false;  // unused
      int64_t limit = 0;
      int64_t norm_iops = 0;
      if (group_mode == ObIOGroupMode::LOCALREAD) {
        norm_iops = get_norm_iops(info.at(i).avg_byte_, info.at(i).avg_iops_, ObIOMode::READ);
      } else if (group_mode == ObIOGroupMode::LOCALWRITE) {
        norm_iops = get_norm_iops(info.at(i).avg_byte_, info.at(i).avg_iops_, ObIOMode::WRITE);
      } else {
        norm_iops = info.at(i).avg_byte_ * info.at(i).avg_iops_ / STANDARD_IOPS_SIZE;
      }
      if (OB_TMP_FAIL(io_clock_.get_group_limit(group_config_index, limit))) {
        LOG_WARN("get group limit failed", K(ret), K(group_config_index));
      } else if (OB_TMP_FAIL(failed_req_info.at(i).calc(failed_avg_size,
              failed_req_iops,
              failed_req_bw,
              failed_avg_prepare_delay,
              failed_avg_schedule_delay,
              failed_avg_submit_delay,
              failed_avg_device_delay,
              failed_avg_total_delay))) {
      } else if ((info.at(i).avg_byte_ + failed_avg_size) < std::numeric_limits<double>::epsilon()) {
      } else {
        group_bw = static_cast<int64_t>(info.at(i).avg_byte_ * info.at(i).avg_iops_);
        ObIOCalibration::get_instance().get_iops_scale(mode, failed_avg_size, failed_iops_scale, is_io_ability_valid);
        ObIOCalibration::get_instance().get_iops_scale(mode, info.at(i).avg_byte_, iops_scale, is_io_ability_valid);
        switch (group_mode) {
          case ObIOGroupMode::LOCALREAD: {
            if (iops_scale > std::numeric_limits<double>::epsilon()) {
              ips += info.at(i).avg_iops_ / iops_scale;
            }
            if (failed_iops_scale > std::numeric_limits<double>::epsilon()) {
              failed_ips += failed_req_iops / failed_iops_scale;
            }
            break;
          }
          case ObIOGroupMode::LOCALWRITE: {
            if (iops_scale > std::numeric_limits<double>::epsilon()) {
              ops += info.at(i).avg_iops_ / iops_scale;
            }
            if (failed_iops_scale > std::numeric_limits<double>::epsilon()) {
              failed_ops += failed_req_iops / failed_iops_scale;
            }
            break;
          }
          case ObIOGroupMode::REMOTEREAD: {
            failed_ibw += failed_req_bw;
            ibw += static_cast<int64_t>(info.at(i).avg_byte_ * info.at(i).avg_iops_);
            break;
          }
          case ObIOGroupMode::REMOTEWRITE: {
            failed_obw += failed_req_bw;
            obw += static_cast<int64_t>(info.at(i).avg_byte_ * info.at(i).avg_iops_);
            break;
          }
          default:
            break;
        }
        snprintf(io_status, sizeof(io_status),"group_id:%ld, group_name:%s, mode:%s, cur_req:%ld, hold_mem:%ld "
            "[FAILED]:fail_size:%ld, fail_iops:%ld, fail_bw:%ld, [delay/us]:prepare:%ld, schedule:%ld, submit:%ld, rt:%ld, total:%ld, "
            "[SUCC]:size:%ld, iops:%ld, norm_iops:%ld, bw:%ld, limit:%ld, [delay/us]:prepare:%ld, schedule:%ld, submit:%ld, rt:%ld, total:%ld",
            group_config.group_id_,
            group_name,
            mode_str,
            mem_stat.group_mem_infos_.at(i).total_cnt_,
            mem_stat.group_mem_infos_.at(i).total_size_,
            static_cast<int64_t>(failed_avg_size),
            static_cast<int64_t>(failed_req_iops + 0.5),
            static_cast<int64_t>(failed_req_bw),
            failed_avg_prepare_delay,
            failed_avg_schedule_delay,
            failed_avg_submit_delay,
            failed_avg_device_delay,
            failed_avg_total_delay,
            static_cast<int64_t>(info.at(i).avg_byte_),
            static_cast<int64_t>(info.at(i).avg_iops_ + 0.5),
            norm_iops,
            static_cast<int64_t>(group_bw),
            static_cast<int64_t>(limit),
            info.at(i).avg_prepare_delay_us_,
            info.at(i).avg_schedule_delay_us_,
            info.at(i).avg_submit_delay_us_,
            info.at(i).avg_device_delay_us_,
            info.at(i).avg_total_delay_us_
            );
        LOG_INFO("[IO STATUS GROUP]", K_(tenant_id), KCSTRING(io_status));
        need_print_io_config = true;
      }
    }
    // MOCK SYS GROUPS
    for (int64_t i = 0; i < sys_info.count(); ++i) {
      if (OB_TMP_FAIL(transform_usage_index_to_group_config_index(i, group_config_index))) {
        continue;
      } else if (sys_info.count() != sys_failed_req_info.count()) {
        continue;
      }
      mode = static_cast<ObIOMode>(group_config_index % MODE_COUNT);
      group_mode = static_cast<ObIOGroupMode>(i % GROUP_MODE_CNT);
      ObIOModule module = static_cast<ObIOModule>(SYS_MODULE_START_ID + i / GROUP_MODE_CNT);
      const char *mode_str = get_io_mode_string(group_mode);
      int64_t group_bw = 0;
      double failed_avg_size = 0;
      double failed_req_iops = 0;
      int64_t failed_req_bw = 0;
      double iops_scale = 1.0;
      bool is_io_ability_valid = false;  // unused
      double failed_iops_scale = 1.0;
      int64_t failed_avg_prepare_delay = 0;
      int64_t failed_avg_schedule_delay = 0;
      int64_t failed_avg_submit_delay = 0;
      int64_t failed_avg_device_delay = 0;
      int64_t failed_avg_total_delay = 0;
      int64_t norm_iops = 0;
      int64_t norm_failed_iops = 0;
      if (OB_TMP_FAIL(sys_failed_req_info.at(i).calc(failed_avg_size,
              failed_req_iops,
              failed_req_bw,
              failed_avg_prepare_delay,
              failed_avg_schedule_delay,
              failed_avg_submit_delay,
              failed_avg_device_delay,
              failed_avg_total_delay))) {
      } else if ((sys_info.at(i).avg_byte_ + failed_avg_size) < std::numeric_limits<double>::epsilon()) {
      } else {
        switch (group_mode) {
          case ObIOGroupMode::LOCALREAD: {
            norm_iops = get_norm_iops(sys_info.at(i).avg_byte_, sys_info.at(i).avg_iops_, ObIOMode::READ);
            norm_failed_iops = get_norm_iops(failed_avg_size, failed_req_iops, ObIOMode::READ);
            ips += norm_iops;
            failed_ips += norm_failed_iops;
            break;
          }
          case ObIOGroupMode::LOCALWRITE: {
            norm_iops = get_norm_iops(sys_info.at(i).avg_byte_, sys_info.at(i).avg_iops_, ObIOMode::WRITE);
            norm_failed_iops = get_norm_iops(failed_avg_size, failed_req_iops, ObIOMode::WRITE);
            ops += norm_iops;
            failed_ops += norm_failed_iops;
            break;
          }
          case ObIOGroupMode::REMOTEREAD: {
            ibw += static_cast<int64_t>(sys_info.at(i).avg_byte_ * sys_info.at(i).avg_iops_);
            failed_ibw += failed_req_bw;
            break;
          }
          case ObIOGroupMode::REMOTEWRITE: {
            obw += static_cast<int64_t>(sys_info.at(i).avg_byte_ * sys_info.at(i).avg_iops_);
            failed_obw += failed_req_bw;
            break;
          }
          default:
            break;
        }
        group_bw = static_cast<int64_t>(sys_info.at(i).avg_byte_ * sys_info.at(i).avg_iops_);
        snprintf(io_status, sizeof(io_status),
                "sys_group_name:%s, mode:%s, cur_req:%ld, hold_mem:%ld "
                "[FAILED]: fail_size:%ld, fail_iops:%ld, fail_bw:%ld, [delay/us]:prepare:%ld, schedule:%ld, submit:%ld, rt:%ld, total:%ld, "
                "[SUCC]: size:%ld, iops:%ld, norm_iops:%ld, bw:%ld, [delay/us]:prepare:%ld, schedule:%ld, submit:%ld, rt:%ld, total:%ld",
                 get_io_sys_group_name(module),
                 mode_str,
                 sys_mem_stat.group_mem_infos_.at(i).total_cnt_,
                 sys_mem_stat.group_mem_infos_.at(i).total_size_,
                 static_cast<int64_t>(failed_avg_size),
                 static_cast<int64_t>(failed_req_iops + 0.5),
                 static_cast<int64_t>(failed_req_bw),
                 failed_avg_prepare_delay,
                 failed_avg_schedule_delay,
                 failed_avg_submit_delay,
                 failed_avg_device_delay,
                 failed_avg_total_delay,
                 static_cast<int64_t>(sys_info.at(i).avg_byte_),
                 static_cast<int64_t>(sys_info.at(i).avg_iops_ + 0.5),
                 norm_iops,
                 static_cast<int64_t>(group_bw),
                 sys_info.at(i).avg_prepare_delay_us_,
                 sys_info.at(i).avg_schedule_delay_us_,
                 sys_info.at(i).avg_submit_delay_us_,
                 sys_info.at(i).avg_device_delay_us_,
                 sys_info.at(i).avg_total_delay_us_
                 );
        LOG_INFO("[IO STATUS GROUP SYS]", K_(tenant_id), KCSTRING(io_status));
        need_print_io_config = true;
      }
    }
    if (need_print_io_config) {
      ObArray<int64_t> queue_count_array;
      if (OB_FAIL(callback_mgr_.get_queue_count(queue_count_array))) {
        LOG_WARN("get callback queue count failed", K(ret));
      }
      int64_t iops = ips + ops;
      double failed_iops = failed_ips + failed_ops;
      LOG_INFO("[IO STATUS TENANT]", K_(tenant_id), K_(ref_cnt), K_(io_config),
          "hold_mem", io_allocator_.get_allocated_size(),
          "free_req_cnt", io_request_pool_.get_free_cnt(),
          "free_result_cnt", io_result_pool_.get_free_cnt(),
          "callback_queues", queue_count_array,
          "[FAILED]: "
          "fail_ips", lround(failed_ips),
          "fail_ops", lround(failed_ops),
          "fail_iops", lround(failed_iops),
          "fail_ibw", failed_ibw,
          "fail_obw", failed_obw,
          "[SUCC]: "
          "ips", ips,
          "ops", ops,
          "iops", iops,
          "ibw", ibw,
          "obw", obw,
          "iops_limit", io_clock_.get_unit_limit(ObIOMode::MAX_MODE),
          "ibw_limit", io_clock_.get_unit_limit(ObIOMode::READ),
          "obw_limit", io_clock_.get_unit_limit(ObIOMode::WRITE));
    }
    if (ATOMIC_LOAD(&io_config_.enable_io_tracer_)) {
      io_tracer_.print_status();
    }

    // print io function status
    print_io_function_status();

    // print callback status
    {
      const ObArray<ObIORunner *> &runners = callback_mgr_.get_runners();
      char io_callback_status[512] = { 0 };
      int64_t pos = 0;
      for (int64_t i = 0; OB_SUCC(ret) && i < runners.count(); i++) {
        if (OB_FAIL(databuff_printf(io_callback_status, sizeof(io_callback_status), pos,
                                    "runner %ld: thread_id=%ld, queue_count=%ld, ",
                                    i, runners[i]->get_tid(), runners[i]->get_queue_count()))) {
          LOG_WARN("fail to construct callback status", KR(ret),
              K_(tenant_id), K(i), K(pos), K(runners), K(io_callback_status));
        }
      }
      LOG_INFO("[IO STATUS CALLBACK]", K_(tenant_id), K(runners), KCSTRING(io_callback_status));
    }
  }
  return ret;
}

int ObTenantIOManager::print_io_function_status()
{
  int ret = OB_SUCCESS;
  if (!is_working() || !is_inited_) {
    LOG_WARN("is not working or not inited", K(is_working()), K(is_inited_));
  } else {
    char io_status[1024] = { 0 };
    int FUNC_NUM = static_cast<uint8_t>(share::ObFunctionType::MAX_FUNCTION_NUM);
    int GROUP_MODE_NUM = static_cast<uint8_t>(ObIOGroupMode::MODECNT);
    ObIOFuncUsageArr &func_usages = io_func_infos_.func_usages_;
    for (int i = 0; OB_SUCC(ret) && i < FUNC_NUM; ++i) {
      for (int j = 0; OB_SUCC(ret) && j < GROUP_MODE_NUM; ++j) {
        double avg_size = 0;
        double avg_iops = 0;
        int64_t avg_bw = 0;
        int64_t avg_prepare_delay = 0;
        int64_t avg_schedule_delay = 0;
        int64_t avg_submit_delay = 0;
        int64_t avg_device_delay = 0;
        int64_t avg_total_delay = 0;
        const char *mode_str = get_io_mode_string(static_cast<ObIOGroupMode>(j));
        if (i >= func_usages.count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("func usages out of range", K(i), K(func_usages.count()));
        } else if (j >= func_usages.at(i).count()) {
          ret = OB_INVALID_ARGUMENT;
          LOG_ERROR("func usages by mode out of range", K(i), K(j), K(func_usages.at(i).count()));
        } else if (OB_FAIL(func_usages.at(i).at(j).calc(
                       avg_size,
                       avg_iops,
                       avg_bw,
                       avg_prepare_delay,
                       avg_schedule_delay,
                       avg_submit_delay,
                       avg_device_delay,
                       avg_total_delay))) {
          LOG_WARN("fail to calc func usage", K(ret), K(i), K(j));
        } else if (avg_size < std::numeric_limits<double>::epsilon()) {
        } else {
          const char *func_name = to_cstring(get_io_function_name(static_cast<share::ObFunctionType>(i)));
          snprintf(io_status, sizeof(io_status),
                    "function_name:%s, mode:%s, avg_size:%ld, avg_iops:%ld, avg_bw:%ld, [delay/us]: prepare:%ld, schedule:%ld, submit:%ld, device:%ld, total:%ld",
                    func_name,
                    mode_str,
                    static_cast<int64_t>(avg_size + 0.5),
                    static_cast<int64_t>(avg_iops + 0.99),
                    avg_bw,
                    avg_prepare_delay,
                    avg_schedule_delay,
                    avg_submit_delay,
                    avg_device_delay,
                    avg_total_delay);
          LOG_INFO("[IO STATUS FUNCTION]", K_(tenant_id), KCSTRING(io_status));
        }
      }
    }
  }
  return ret;
}

void ObTenantIOManager::inc_ref()
{
  ATOMIC_INC(&ref_cnt_);
}

void ObTenantIOManager::dec_ref()
{
  int ret = OB_SUCCESS;
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (tmp_ref < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("bug: ref_cnt < 0", K(ret), K(tmp_ref));
    abort();
  }
}

int ObTenantIOManager::get_throttled_time(uint64_t group_id, int64_t &throttled_time)
{
  int ret = OB_SUCCESS;
  int64_t current_throttled_time_us = -1;
  if (OB_FAIL(GCTX.cgroup_ctrl_->get_throttled_time(tenant_id_, current_throttled_time_us, group_id))) {
    LOG_WARN("get throttled time failed", K(ret), K(tenant_id_), K(group_id));
  } else if (current_throttled_time_us > 0) {
    uint64_t idx = 0;
    const uint64_t GROUP_MODE_CNT = static_cast<uint64_t>(ObIOGroupMode::MODECNT);
    ObIOGroupKey group_key(group_id, ObIOMode::READ);
    if (OB_FAIL(get_group_index(group_key, idx))) {
      LOG_WARN("get group index failed", K(ret), K(group_id));
    } else {
      idx = idx / GROUP_MODE_CNT;
      throttled_time = current_throttled_time_us - io_usage_.get_group_throttled_time_us().at(idx);
      io_usage_.get_group_throttled_time_us().at(idx) = current_throttled_time_us;
    }
  }
  return ret;
}

const ObIOFuncUsages& ObTenantIOManager::get_io_func_infos()
{
  return io_func_infos_;
}

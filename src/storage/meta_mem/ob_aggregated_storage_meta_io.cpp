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

#include "storage/blocksstable/ob_sstable.h"
#include "ob_aggregated_storage_meta_io.h"

 namespace oceanbase
{
namespace storage
{

ObAggregatedStorageMetaIOInfo::ObAggregatedStorageMetaIOInfo()
  : aggr_params_(),
    first_meta_offset_(UINT64_MAX),
    total_meta_size_(UINT64_MAX),
    macro_id_(),
    disk_type_(ObMetaDiskAddr::DiskType::NONE)
{
}

ObAggregatedStorageMetaIOInfo::ObAggregatedStorageMetaIOInfo(const ObAggregatedStorageMetaIOInfo &aggr_io_info)
  : aggr_params_(),
    first_meta_offset_(aggr_io_info.first_meta_offset_),
    total_meta_size_(aggr_io_info.total_meta_size_),
    macro_id_(aggr_io_info.macro_id_),
    disk_type_(aggr_io_info.disk_type_)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(aggr_params_.assign(aggr_io_info.aggr_params_))) {
    reset();
    LOG_WARN("fail to assign aggr_params", K(ret), K(aggr_io_info));
  }
}

int ObAggregatedStorageMetaIOInfo::init(const common::ObIArray<AggregatedInfo> &aggr_infos)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(aggr_infos.count() == 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(aggr_infos));
  } else if (OB_FAIL(aggr_params_.reserve(aggr_infos.count()))) {
    LOG_WARN("fail to reserve aggr_params", K(ret));
  } else {
    for(int64_t i = 0; OB_SUCC(ret) && i < aggr_infos.count(); ++i) {
      if (OB_UNLIKELY(nullptr == aggr_infos.at(i).element<0>()) ||
          OB_UNLIKELY(nullptr == aggr_infos.at(i).element<1>()) ||
          OB_UNLIKELY(!aggr_infos.at(i).element<1>()->cache_handle_.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, the aggr_info is not valid", K(ret), K(i), K(aggr_infos));
      } else if(OB_FAIL(aggr_params_.push_back(ObAggregatedStorageMetaInfo()))) {
        LOG_WARN("fail to push aggregated storage meta info", K(ret));
      } else {
        blocksstable::ObSSTable *table = static_cast<blocksstable::ObSSTable *>(aggr_infos.at(i).element<0>());
        ObStorageMetaHandle *meta_handle = aggr_infos.at(i).element<1>();

        aggr_params_.at(i).meta_type_ = table->is_co_sstable() ? ObStorageMetaValue::MetaType::CO_SSTABLE
                                                              : ObStorageMetaValue::MetaType::SSTABLE;
        aggr_params_.at(i).meta_key_ = ObStorageMetaKey(MTL_ID(), table->get_addr());
        aggr_params_.at(i).cache_handle_ = meta_handle->cache_handle_;

        meta_handle->phy_addr_ = table->get_addr();
      }
    }

    if (OB_SUCC(ret)) {
      // find the first and last meta addr and size
      const ObMetaDiskAddr &meta_addr = aggr_params_.at(0).meta_key_.get_meta_addr();
      uint64_t first_meta_offset = meta_addr.offset();
      uint64_t last_meta_offset = meta_addr.offset();
      uint64_t last_meta_size = meta_addr.size();
      macro_id_ = meta_addr.block_id();
      disk_type_ = meta_addr.type();
      for (int64_t i = 1; OB_SUCC(ret) && i < aggr_params_.count(); ++i) {
        const ObMetaDiskAddr &cur_meta_addr = aggr_params_.at(i).meta_key_.get_meta_addr();
        // check macro id and disk type
        if (OB_UNLIKELY(cur_meta_addr.block_id() != macro_id_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, the block id of keys is not the same", K(ret), K(i), K(aggr_params_));
          break;
        } else if (OB_UNLIKELY(cur_meta_addr.type() != disk_type_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, the disk type of keys is not the same", K(ret), K(i), K(aggr_params_));
          break;
        }

        if (cur_meta_addr.offset() < first_meta_offset) {
          first_meta_offset = cur_meta_addr.offset();
        }

        if (cur_meta_addr.offset() > last_meta_offset) {
          last_meta_offset = cur_meta_addr.offset();
          last_meta_size = cur_meta_addr.size();
        }
      }

      total_meta_size_ = last_meta_offset - first_meta_offset + last_meta_size;
      first_meta_offset_ = first_meta_offset;
    }
    if (OB_FAIL(ret)) {
      reset();
    }
  }
  return ret;
}

bool ObAggregatedStorageMetaIOInfo::is_valid() const
{
  int ret = OB_SUCCESS;
  bool is_valid = aggr_params_.count() > 0 && UINT64_MAX != first_meta_offset_ && UINT64_MAX != total_meta_size_
        && macro_id_.is_valid() && ObMetaDiskAddr::DiskType::NONE != disk_type_;
  for (int64_t i = 0; is_valid && i < aggr_params_.count(); ++i) {
    is_valid = aggr_params_.at(i).meta_key_.is_valid() && aggr_params_.at(i).cache_handle_.is_valid();
  }
  if (!is_valid) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid storage meta io info", K(ret), K(first_meta_offset_), K(total_meta_size_), K(aggr_params_));
  }
  return is_valid;
}

void ObAggregatedStorageMetaIOInfo::reset()
{
  aggr_params_.reset();
  first_meta_offset_ = UINT64_MAX;
  total_meta_size_ = UINT64_MAX;
  macro_id_.reset();
  disk_type_ = ObMetaDiskAddr::DiskType::NONE;
}

ObAggregatedStorageMetaIOInfo::~ObAggregatedStorageMetaIOInfo()
{
  reset();
}

ObAggregatedStorageMetaIOCallback::ObAggregatedStorageMetaIOCallback(
  common::ObIAllocator *io_allocator,
  const ObAggregatedStorageMetaIOInfo &aggr_io_info,
  const ObMetaDiskAddr &meta_addr)
  : ObSharedObjectIOCallback(io_allocator, meta_addr, common::ObIOCallbackType::STORAGE_META_CALLBACK),
    aggr_io_info_(aggr_io_info)
{
  static_assert(sizeof(*this) <= CALLBACK_BUF_SIZE, "IOCallback buf size not enough");
}

ObAggregatedStorageMetaIOCallback::~ObAggregatedStorageMetaIOCallback()
{
}

int ObAggregatedStorageMetaIOCallback::do_process(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  ObDIActionGuard action_guard("ObAggregatedStorageMetaIOCallback");
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid storage meta cache callback", K(ret), K_(aggr_io_info));
  } else if (OB_UNLIKELY(buf_len <= 0 || buf == nullptr)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data buffer size", K(ret), K(buf_len), KP(buf));
  } else if (OB_FAIL(ObStorageMetaValue::process_aggregated_storage_meta(aggr_io_info_, buf, buf_len))) {
    LOG_WARN("fail to process io buf", K(ret), K_(aggr_io_info));
  }
  return ret;
}

bool ObAggregatedStorageMetaIOCallback::is_valid() const
{
  return ObSharedObjectIOCallback::is_valid() && aggr_io_info_.is_valid();
}


}
}
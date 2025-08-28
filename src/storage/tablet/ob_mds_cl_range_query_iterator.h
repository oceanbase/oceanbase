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

#ifndef OCEANBASE_STORAGE_OB_MDS_CL_RANGE_QUERY_ITERATOR
#define OCEANBASE_STORAGE_OB_MDS_CL_RANGE_QUERY_ITERATOR

#include "storage/tablet/ob_mds_range_query_iterator.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTabletHandle;
class ObTabletPointer;
class ObTablet;

namespace mds
{
class MdsDumpKV;
}

template <typename K, typename T>
class ObMdsRangeQueryIterator
{
public:
  ObMdsRangeQueryIterator();
  ~ObMdsRangeQueryIterator();
  ObMdsRangeQueryIterator(const ObMdsRangeQueryIterator&) = delete;
  ObMdsRangeQueryIterator &operator=(const ObMdsRangeQueryIterator&) = delete;
public:
  int init(
      ObTableScanParam &scan_param,
      const ObTabletHandle &tablet_handle,
      const ObTabletHandle &src_tablet_handle);
  int get_next_mds_kv(
      common::ObIAllocator &allocator, 
      mds::MdsDumpKV *&kv);
  void free_mds_kv(
      common::ObIAllocator &allocator,
      mds::MdsDumpKV *&kv);
private:
  int advance_iter(
    ObSingleTabletMdsRangeQueryIterator<K, T> &iter, 
    bool &is_finished_, 
    mds::MdsDumpKV *&kv);
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  bool cur_finished_;
  bool src_finished_;
  mds::MdsDumpKV *cur_kv_;
  mds::MdsDumpKV *src_kv_;
  ObStoreCtx cur_ctx;
  ObStoreCtx src_ctx;
  ObSingleTabletMdsRangeQueryIterator<K, T> cur_tablet_iter_;
  ObSingleTabletMdsRangeQueryIterator<K, T> src_tablet_iter_;
};

template <typename K, typename T>
ObMdsRangeQueryIterator<K, T>::ObMdsRangeQueryIterator()
  : is_inited_(false),
    allocator_(lib::ObMemAttr(MTL_ID(), "MdsRangeQuery")),
    cur_finished_(false),
    src_finished_(false),
    cur_kv_(nullptr),
    src_kv_(nullptr),
    cur_ctx(),
    src_ctx(),
    cur_tablet_iter_(),
    src_tablet_iter_()
{
}

template <typename K, typename T>
ObMdsRangeQueryIterator<K, T>::~ObMdsRangeQueryIterator()
{
  is_inited_ = false;
  cur_finished_ = false;
  src_finished_ = false;
  cur_kv_ = nullptr;
  src_kv_ = nullptr;
  allocator_.reset();
  // make sure that cur_iter and src_iter should be deconstruct before cur_ctx and src_ctx;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::init(
    ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle,
    const ObTabletHandle &src_tablet_handle)
{
  int ret = common::OB_SUCCESS;
  cur_finished_ = false;
  src_finished_ = false;
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    MDS_LOG(WARN, "this iter has been", K(ret), KPC(tablet_handle.get_obj()));
  } else if (OB_UNLIKELY(!scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid argument", KR(ret), K(scan_param));
  } else if (OB_FAIL(cur_tablet_iter_.init(scan_param, tablet_handle, cur_ctx))) {
    MDS_LOG(WARN, "failed to init cur_tablet_iter", K(ret), KPC(tablet_handle.get_obj()));
  } else if (OB_ISNULL(src_tablet_handle.get_obj())) {
    src_finished_ = true;
  } else if (OB_NOT_NULL(src_tablet_handle.get_obj())) {
    src_finished_ = false;
    
    ObTableScanParam *src_scan_param = nullptr;
    char *buf = nullptr;
    ObLSID ls_id;
    ObTabletID tablet_id;
    if (OB_ISNULL(buf = static_cast<char*>(scan_param.allocator_->alloc(sizeof(ObTableScanParam))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      MDS_LOG(WARN, "allocate mem failed", K(ret), KP(buf));
    } else if (FALSE_IT(src_scan_param = new(buf) ObTableScanParam())) {
    } else if (OB_FAIL(ObMdsRangeQueryIteratorHelper::get_tablet_ls_id_and_tablet_id(src_tablet_handle, ls_id, tablet_id))) {
      MDS_LOG(WARN, "failed to get ls_id and tablet_id", K(ret), KPC(src_tablet_handle.get_obj()));
    } else if (OB_FAIL((ObMdsScanParamHelper::build_customized_scan_param<K, T>(*scan_param.allocator_,
                                                                               ls_id,
                                                                               tablet_id,
                                                                               scan_param.read_version_range_,
                                                                               *scan_param.mds_collector_,
                                                                               *src_scan_param)))) {
      MDS_LOG(WARN, "failed to build src tablet scan param");
    } else if (OB_FAIL(src_tablet_iter_.init(*src_scan_param, src_tablet_handle, src_ctx))) {
      MDS_LOG(WARN, "init src_tablet_iter failed ", K(ret), KPC(src_scan_param), K(src_tablet_handle), KPC(src_tablet_handle.get_obj()), K(src_ctx));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::advance_iter(
    ObSingleTabletMdsRangeQueryIterator<K, T> &iter, 
    bool &is_finished_, 
    mds::MdsDumpKV *&kv)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(kv)) {
    // only one pending_kv for each iter(cur & src) in memory
    allocator_.free(kv);
  } 
  kv = nullptr;
  if (OB_FAIL(iter.get_next_mds_kv(allocator_, kv))) {
    if (OB_ITER_END == ret) {
      is_finished_ = true;
      kv = nullptr;
      ret = OB_SUCCESS;
    } else {
      MDS_LOG(WARN, "fail to get next mds kv from iter", K(ret));
    }
  }
  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::get_next_mds_kv(
    common::ObIAllocator &allocator,
    mds::MdsDumpKV *&kv)
{
  int ret = common::OB_SUCCESS;
  int compare_result = 0;
  kv = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (!cur_finished_ && OB_ISNULL(cur_kv_) && OB_FAIL(advance_iter(cur_tablet_iter_, cur_finished_, cur_kv_))) { // first_time iter cur_tablet
     MDS_LOG(WARN, "fail to get next mds kv from cur_kv", K(ret));
  } else if (!src_finished_ && OB_ISNULL(src_kv_) && OB_FAIL(advance_iter(src_tablet_iter_, src_finished_, src_kv_))) { // first_time iter src_tablet
     MDS_LOG(WARN, "fail to get next mds kv from cur_kv", K(ret));
  } else if (cur_finished_ && src_finished_) {
    // both iter end
    ret = common::OB_ITER_END;
    MDS_LOG(DEBUG, "both iter end", K(ret));
  } else if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, kv))) {
    MDS_LOG(WARN, "fail to alloc and new", K(ret));
  } else if (cur_finished_) {
    if (OB_ISNULL(src_kv_)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "src_kv_ shoulde not be nullptr", K(ret), KPC(cur_kv_), KPC(src_kv_), K(cur_finished_), K(src_finished_));
    } else {
      if (OB_FAIL(kv->assign(*src_kv_, allocator))) {
        MDS_LOG(WARN, "fail to copy", K(ret), K(cur_kv_));
      } else if (OB_FAIL(advance_iter(src_tablet_iter_, src_finished_, src_kv_))) {
        MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
      }
    }
  } else if (src_finished_) {
    if (OB_ISNULL(cur_kv_)) {
      ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "cur_kv_ shoulde not be nullptr", K(ret), KPC(cur_kv_), KPC(src_kv_), K(cur_finished_), K(src_finished_));
    } else {
      if (OB_FAIL(kv->assign(*cur_kv_, allocator))) {
        MDS_LOG(WARN, "fail to copy", K(ret), K(cur_kv_));
      } else if (OB_FAIL(advance_iter(cur_tablet_iter_, cur_finished_, cur_kv_))) {
        MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
      }
    }
  } else if (OB_UNLIKELY(OB_ISNULL(src_kv_) || OB_ISNULL(cur_kv_))) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "unexpected error, both mds table key and mds sstable key should be valid",
        K(ret), KPC(src_kv_), KPC(cur_kv_));
  } else if (OB_FAIL(mds::compare_mds_serialized_buffer(
      src_kv_->k_.key_.ptr(), src_kv_->k_.key_.size(), 
      cur_kv_->k_.key_.ptr(), cur_kv_->k_.key_.size(), 
      compare_result))) {
    MDS_LOG(WARN, "fail to compare binary key", K(ret));
  } else if (compare_result < 0) {
    if (OB_FAIL(kv->assign(*src_kv_, allocator))) {
      MDS_LOG(WARN, "fail to copy", K(ret), K(src_kv_));
    } else if (OB_FAIL(advance_iter(src_tablet_iter_, src_finished_, src_kv_))) {
      MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
    }
  } else if (compare_result == 0) {
    if (OB_FAIL(kv->assign(*cur_kv_, allocator))) {
      MDS_LOG(WARN, "fail to copy", K(ret), K(cur_kv_));
    } else if (OB_FAIL(advance_iter(cur_tablet_iter_, cur_finished_, cur_kv_))) {
      MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
    } else if (OB_FAIL(advance_iter(src_tablet_iter_, src_finished_, src_kv_))) {
      MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
    } else {
      int tmp_ret = OB_ERR_UNEXPECTED;
      MDS_LOG(WARN, "dest_tablet and src_tablet in a transfer progress should not have the same Key", K(tmp_ret), KPC(kv), KPC(cur_kv_), KPC(src_kv_), K(lbt()));
    }
  } else {
    if (OB_FAIL(kv->assign(*cur_kv_, allocator))) {
      MDS_LOG(WARN, "fail to copy", K(ret), K(cur_kv_));
    } else if (OB_FAIL(advance_iter(cur_tablet_iter_, cur_finished_, cur_kv_))) {
      MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    MDS_LOG(DEBUG, "succeed to get next mds kv", K(ret), KPC(kv));
  }

  return ret;
}

template <typename K, typename T>
void ObMdsRangeQueryIterator<K, T>::free_mds_kv(
    common::ObIAllocator &allocator,
    mds::MdsDumpKV *&kv)
{
  if (kv != nullptr) {
    kv->mds::MdsDumpKV::~MdsDumpKV();
    allocator.free(kv);
    kv = nullptr;
  }
}

} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_MDS_CL_RANGE_QUERY_ITERATOR

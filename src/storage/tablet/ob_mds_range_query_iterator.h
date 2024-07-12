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

#ifndef OCEANBASE_STORAGE_OB_MDS_RANGE_QUERY_ITERATOR
#define OCEANBASE_STORAGE_OB_MDS_RANGE_QUERY_ITERATOR

#include <stdint.h>
#include "lib/allocator/page_arena.h"
#include "lib/oblog/ob_log.h"
#include "lib/function/ob_function.h"
#include "storage/multi_data_source/mds_node.h"
#include "storage/multi_data_source/mds_table_iterator.h"
#include "storage/multi_data_source/mds_table_handle.h"
#include "storage/multi_data_source/runtime_utility/common_define.h"
#include "storage/tablet/ob_mds_row_iterator.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"

namespace oceanbase
{
namespace common
{
class ObIAllocator;
}

namespace storage
{
class ObTabletHandle;

namespace mds
{
class MdsDumpKV;
}

class ObMdsRangeQueryIteratorHelper
{
public:
  static int get_mds_table(const ObTabletHandle &tablet_handle, mds::MdsTableHandle &mds_table);
};

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
      ObStoreCtx &store_ctx,
      const common::ObFunction<int(mds::UserMdsNode<K, T>&, bool &)> &filter_func);

  int get_next_mds_kv(common::ObIAllocator &allocator, mds::MdsDumpKV *&kv);
  static void free_mds_kv(common::ObIAllocator &allocator, mds::MdsDumpKV *&kv);
private:
  int init_mds_table_iter();
  int advance_mds_table_iter();
  int advance_mds_sstable_iter();
  int output_from_mds_table(common::ObIAllocator &allocator, mds::MdsDumpKV *&kv);
  int output_from_mds_sstable(common::ObIAllocator &allocator, mds::MdsDumpKV *&kv);
  static int convert_user_node_to_dump_kv(
      common::ObIAllocator &allocator,
      const K &user_key,
      const mds::UserMdsNode<K, T> &user_node,
      mds::MdsDumpKV &kv);
  static int get_key_from_dump_kv(const mds::MdsDumpKV &kv, K &k);
private:
  bool is_inited_;
  common::ObArenaAllocator allocator_;
  share::SCN read_snapshot_;
  mds::ObMdsUnitRowNodeScanIterator<K, T> mds_table_iter_;
  ObMdsRowIterator mds_sstable_iter_;
  bool mds_table_end_;
  bool mds_sstable_end_;
  K mds_table_key_;
  K mds_sstable_key_;
  mds::UserMdsNode<K, T> *mds_table_val_;
  mds::MdsDumpKV mds_sstable_val_;
};

template <typename K, typename T>
ObMdsRangeQueryIterator<K, T>::ObMdsRangeQueryIterator()
  : is_inited_(false),
    allocator_(lib::ObMemAttr(MTL_ID(), "range_query")),
    read_snapshot_(),
    mds_table_iter_(),
    mds_sstable_iter_(),
    mds_table_end_(false),
    mds_sstable_end_(false),
    mds_table_key_(),
    mds_sstable_key_(),
    mds_table_val_(nullptr),
    mds_sstable_val_()
{
}

template <typename K, typename T>
ObMdsRangeQueryIterator<K, T>::~ObMdsRangeQueryIterator()
{
  is_inited_ = false;
  read_snapshot_.reset();
  mds_sstable_iter_.reset();
  mds_table_end_ = false;
  mds_sstable_end_ = false;
  mds_table_key_.reset();
  mds_sstable_key_.reset();
  mds_table_val_ = nullptr;
  mds_sstable_val_.reset();
  allocator_.reset();
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::init(
    ObTableScanParam &scan_param,
    const ObTabletHandle &tablet_handle,
    ObStoreCtx &store_ctx,
    const common::ObFunction<int(mds::UserMdsNode<K, T>&, bool &)> &filter_func)
{
  int ret = common::OB_SUCCESS;
  mds::MdsTableHandle mds_table;

  if (OB_UNLIKELY(is_inited_)) {
    ret = common::OB_INIT_TWICE;
    MDS_LOG(WARN, "init twice", K(ret), K_(is_inited));
  } else if (OB_FAIL(ObMdsRangeQueryIteratorHelper::get_mds_table(tablet_handle, mds_table))) {
    if (common::OB_ENTRY_NOT_EXIST == ret) {
      mds_table_end_ = true; // no mds table, directly mds table end
      ret = common::OB_SUCCESS;
      MDS_LOG(DEBUG, "no mds table", K(ret), K_(mds_table_end));
    } else {
      MDS_LOG(WARN, "failed to get mds table", K(ret));
    }
  } else if (OB_FAIL(mds_table_iter_.init(mds_table, filter_func))) {
    MDS_LOG(WARN, "failed to init mds table iter", K(ret));
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(mds_sstable_iter_.init(scan_param, tablet_handle, store_ctx))) {
    MDS_LOG(WARN, "fail to init mds sstable iter", K(ret), K(scan_param));
  } else {
    // TODO(@bowen.gbw): currently we pass MAX_SCN as read snapshot,
    // in the future, we should consider the semantic of read snapshot when iteratively reading medium info.
    read_snapshot_.convert_for_tx(scan_param.frozen_version_);
    is_inited_ = true;

    MDS_LOG(DEBUG, "succeed to init mds range query iterator", K(ret));
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::advance_mds_table_iter()
{
  int ret = common::OB_SUCCESS;
  if (mds_table_end_) {
  } else if (OB_FAIL(mds_table_iter_.get_next(mds_table_key_, mds_table_val_))) {
    if (common::OB_ITER_END == ret) {
      mds_table_end_ = true;
      ret = common::OB_SUCCESS;
      MDS_LOG(DEBUG, "mds table iter end", K(ret));
    } else {
      MDS_LOG(WARN, "fail to get next from mds table iter", K(ret));
    }
  } else {
    MDS_LOG(DEBUG, "succeed to get next from mds table iter", K(ret), K_(mds_table_key), KPC_(mds_table_val));
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::advance_mds_sstable_iter()
{
  int ret = common::OB_SUCCESS;
  if (mds_sstable_end_) {
  } else {
    mds_sstable_val_.reset();
    if (OB_FAIL(mds_sstable_iter_.get_next_mds_kv(allocator_, mds_sstable_val_))) {
      if (common::OB_ITER_END == ret) {
        mds_sstable_end_ = true;
        ret = common::OB_SUCCESS;
        MDS_LOG(DEBUG, "mds sstable iter end", K(ret));
      } else {
        MDS_LOG(WARN, "fail to get next from mds sstable iter", K(ret));
      }
    } else if (OB_FAIL(get_key_from_dump_kv(mds_sstable_val_, mds_sstable_key_))) {
      MDS_LOG(WARN, "fail to get key", K(ret), K_(mds_sstable_val));
    } else {
      MDS_LOG(DEBUG, "succeed to get next from mds sstable iter", K(ret), K_(mds_sstable_key), K_(mds_sstable_val));
    }
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::output_from_mds_table(common::ObIAllocator &allocator, mds::MdsDumpKV *&kv)
{
  int ret = common::OB_SUCCESS;

  if (mds_table_val_->trans_version_ > read_snapshot_) {
    mds_table_end_ = true;
    ret = OB_ITER_END;
    MDS_LOG(DEBUG, "mds table iter end because trans version is beyond read snapshot", K(ret), K_(read_snapshot), KPC_(mds_table_val));
  } else {
    if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, kv))) {
      MDS_LOG(WARN, "fail to alloc and new", K(ret));
    } else {
      if (OB_FAIL(convert_user_node_to_dump_kv(allocator, mds_table_key_, *mds_table_val_, *kv))) {
        MDS_LOG(WARN, "fail to convert to mds dump kv", K(ret), K_(mds_table_key), KPC_(mds_table_val));
      }

      if (OB_FAIL(ret)) {
        ObTabletObjLoadHelper::free(allocator, kv);
      }
    }
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::output_from_mds_sstable(common::ObIAllocator &allocator, mds::MdsDumpKV *&kv)
{
  int ret = common::OB_SUCCESS;

  if (OB_FAIL(ObTabletObjLoadHelper::alloc_and_new(allocator, kv))) {
    MDS_LOG(WARN, "fail to alloc and new", K(ret));
  } else {
    if (OB_FAIL(kv->assign(mds_sstable_val_, allocator))) {
      MDS_LOG(WARN, "fail to copy", K(ret), K_(mds_sstable_val));
    }

    if (OB_FAIL(ret)) {
      ObTabletObjLoadHelper::free(allocator, kv);
    }
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::convert_user_node_to_dump_kv(
    common::ObIAllocator &allocator,
    const K &user_key,
    const mds::UserMdsNode<K, T> &user_node,
    mds::MdsDumpKV &kv)
{
  int ret = common::OB_SUCCESS;
  constexpr uint8_t mds_table_id = mds::MdsTableTypeTuple::get_element_index<mds::NormalMdsTable>();
  constexpr uint8_t mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<K, T>>::value;
  mds::MdsDumpKey &key = kv.k_;
  mds::MdsDumpNode &node = kv.v_;

  if (OB_FAIL(key.init(mds_table_id, mds_unit_id, user_key, allocator))) {
    MDS_LOG(WARN, "fail to init mds dump key", K(ret), K(mds_table_id), K(mds_unit_id), K(user_key));
  } else if (OB_FAIL(node.init(mds_table_id, mds_unit_id, user_node, allocator))) {
    MDS_LOG(WARN, "fail to init mds dump node", K(ret), K(mds_table_id), K(mds_unit_id), K(user_node));
  }

  return ret;
}

template <typename K, typename T>
int ObMdsRangeQueryIterator<K, T>::get_key_from_dump_kv(const mds::MdsDumpKV &kv, K &k)
{
  int ret = common::OB_SUCCESS;
  const common::ObString &key_str = kv.k_.key_;
  int64_t pos = 0;
  if (OB_FAIL(k.mds_deserialize(key_str.ptr(), key_str.length(), pos))) {
    MDS_LOG(WARN, "fail to deserialize", K(ret));
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

  if (OB_UNLIKELY(!is_inited_)) {
    ret = common::OB_NOT_INIT;
    MDS_LOG(WARN, "not inited", K(ret), K_(is_inited));
  }

  if (OB_FAIL(ret)) {
  } else if (!mds_table_key_.is_valid()) {
    if (OB_FAIL(advance_mds_table_iter())) {
      MDS_LOG(WARN, "fail to advance mds table iter", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!mds_sstable_key_.is_valid()) {
    if (OB_FAIL(advance_mds_sstable_iter())) {
      MDS_LOG(WARN, "fail to advance mds table iter", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (mds_table_end_ && mds_sstable_end_) {
    // both iter end
    ret = common::OB_ITER_END;
    MDS_LOG(DEBUG, "both iter end", K(ret));
  } else if (mds_table_end_) {
    // use mds sstable
    if (OB_FAIL(output_from_mds_sstable(allocator, kv))) {
      MDS_LOG(WARN, "fail to output from mds sstable", K(ret));
    } else if (OB_FAIL(advance_mds_sstable_iter())) {
      MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
    }
  } else if (mds_sstable_end_) {
    // use mds table
    if (OB_FAIL(output_from_mds_table(allocator, kv))) {
      MDS_LOG(WARN, "fail to output from mds table", K(ret));
    } else if (OB_FAIL(advance_mds_table_iter())) {
      MDS_LOG(WARN, "fail to advance mds table iter", K(ret));
    }
  } else if (OB_UNLIKELY(!mds_table_key_.is_valid() || !mds_sstable_key_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(WARN, "unexpected error, both mds table key and mds sstable key should be valid",
        K(ret), K_(mds_table_key), K_(mds_sstable_key));
  } else if (OB_FAIL(mds::compare_binary_key(mds_table_key_, mds_sstable_key_, compare_result))) {
    MDS_LOG(WARN, "fail to compare binary key", K(ret));
  } else if (compare_result < 0) {
    // use mds table
    if (OB_FAIL(output_from_mds_table(allocator, kv))) {
      MDS_LOG(WARN, "fail to output from mds table", K(ret));
    } else if (OB_FAIL(advance_mds_table_iter())) {
      MDS_LOG(WARN, "fail to advance mds table iter", K(ret));
    }
  } else if (compare_result == 0) {
    // use mds table, both iter advance
    if (OB_FAIL(output_from_mds_table(allocator, kv))) {
      MDS_LOG(WARN, "fail to output from mds table", K(ret));
    } else if (OB_FAIL(advance_mds_table_iter())) {
      MDS_LOG(WARN, "fail to advance mds table iter", K(ret));
    } else if (OB_FAIL(advance_mds_sstable_iter())) {
      MDS_LOG(WARN, "fail to advance mds sstable iter", K(ret));
    }
  } else {
    // use mds sstable
    if (OB_FAIL(output_from_mds_sstable(allocator, kv))) {
      MDS_LOG(WARN, "fail to output from mds sstable", K(ret));
    } else if (OB_FAIL(advance_mds_sstable_iter())) {
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

#endif // OCEANBASE_STORAGE_OB_MDS_RANGE_QUERY_ITERATOR

// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/container/ob_vector.h"
#include "storage/direct_load/ob_direct_load_external_fragment.h"
#include "storage/direct_load/ob_direct_load_external_interface.h"

namespace oceanbase
{
namespace storage
{

template <typename T>
class ObDirectLoadExternalFragmentBuilder
{
  typedef ObDirectLoadExternalIterator<T> ExternalIterator;
  typedef ObDirectLoadExternalWriter<T> ExternalWriter;
public:
  ObDirectLoadExternalFragmentBuilder();
  ~ObDirectLoadExternalFragmentBuilder();
  void reuse();
  void reset();
  int init(ObDirectLoadTmpFileManager *file_mgr, ExternalWriter *external_writer);
  int build_fragment(const common::ObVector<T *> &item_list);
  int build_fragment(ExternalIterator &iter);
  const ObDirectLoadExternalFragmentArray &get_fragments() const { return fragments_; }
protected:
  ObDirectLoadTmpFileManager *file_mgr_;
  int64_t dir_id_;
  ExternalWriter *external_writer_;
  ObDirectLoadExternalFragmentArray fragments_;
  bool is_inited_;
};

template <typename T>
ObDirectLoadExternalFragmentBuilder<T>::ObDirectLoadExternalFragmentBuilder()
  : file_mgr_(nullptr), dir_id_(-1), external_writer_(nullptr), is_inited_(false)
{
}

template <typename T>
ObDirectLoadExternalFragmentBuilder<T>::~ObDirectLoadExternalFragmentBuilder()
{
  reset();
}

template <typename T>
void ObDirectLoadExternalFragmentBuilder<T>::reuse()
{
  fragments_.reset();
}

template <typename T>
void ObDirectLoadExternalFragmentBuilder<T>::reset()
{
  file_mgr_ = nullptr;
  dir_id_ = -1;
  external_writer_ = nullptr;
  fragments_.reset();
  is_inited_ = false;
}

template <typename T>
int ObDirectLoadExternalFragmentBuilder<T>::init(ObDirectLoadTmpFileManager *file_mgr,
                                                 ExternalWriter *external_writer)
{
  int ret = common::OB_SUCCESS;
  if (IS_INIT) {
    ret = common::OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObDirectLoadExternalSortRound init twice", KR(ret), KP(this));
  } else if (OB_UNLIKELY(nullptr == file_mgr || nullptr == external_writer)) {
    ret = common::OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid args", KR(ret), KP(file_mgr), KP(external_writer));
  } else {
    if (OB_FAIL(file_mgr->alloc_dir(dir_id_))) {
      STORAGE_LOG(WARN, "fail to alloc dir", KR(ret));
    } else {
      file_mgr_ = file_mgr;
      external_writer_ = external_writer;
      is_inited_ = true;
    }
  }
  return ret;
}

template <typename T>
int ObDirectLoadExternalFragmentBuilder<T>::build_fragment(const common::ObVector<T *> &item_list)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadExternalFragmentBuilder not init", KR(ret));
  } else {
    ObDirectLoadExternalFragment fragment;
    if (OB_FAIL(file_mgr_->alloc_file(dir_id_, fragment.file_handle_))) {
      STORAGE_LOG(WARN, "fail to alloc file", KR(ret));
    } else if (OB_FAIL(external_writer_->open(fragment.file_handle_))) {
      STORAGE_LOG(WARN, "fail to open tmp file", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < item_list.size(); ++i) {
      if (OB_FAIL(external_writer_->write_item(*item_list[i]))) {
        STORAGE_LOG(WARN, "fail to write item", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(external_writer_->close())) {
        STORAGE_LOG(WARN, "fail to close external writer", KR(ret));
      }
    }
    if (OB_SUCC(ret)) {
      fragment.file_size_ = external_writer_->get_file_size();
      fragment.row_count_ = item_list.size();
      fragment.max_data_block_size_ = external_writer_->get_max_block_size();
      if (OB_FAIL(fragments_.push_back(fragment))) {
        STORAGE_LOG(WARN, "fail to push back fragment", KR(ret));
      }
    }
  }
  return ret;
}

template <typename T>
int ObDirectLoadExternalFragmentBuilder<T>::build_fragment(ExternalIterator &iter)
{
  int ret = common::OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = common::OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObDirectLoadExternalFragmentBuilder not init", KR(ret));
  } else {
    ObDirectLoadExternalFragment fragment;
    const T *item = nullptr;
    if (OB_FAIL(file_mgr_->alloc_file(dir_id_, fragment.file_handle_))) {
      STORAGE_LOG(WARN, "fail to alloc file", KR(ret));
    } else if (OB_FAIL(external_writer_->open(fragment.file_handle_))) {
      STORAGE_LOG(WARN, "fail to open tmp file", KR(ret));
    }
    while (OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next_item(item))) {
        if (OB_UNLIKELY(common::OB_ITER_END != ret)) {
          STORAGE_LOG(WARN, "fail to get next item", KR(ret));
        } else {
          ret = common::OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(external_writer_->write_item(*item))) {
        STORAGE_LOG(WARN, "fail to write item", KR(ret));
      } else {
        ++fragment.row_count_;
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_FAIL(external_writer_->close())) {
        STORAGE_LOG(WARN, "fail to close external writer", KR(ret));
      } else if (OB_FAIL(fragments_.push_back(fragment))) {
        STORAGE_LOG(WARN, "fail to push back fragment", KR(ret));
      }
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

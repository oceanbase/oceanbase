/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_SERIALIZE_H_
#define OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_SERIALIZE_H_
#include <iostream>
#include "lib/function/ob_function.h"
#include "lib/allocator/page_arena.h"
#include "common/row/ob_row_iterator.h"
#include "share/ob_lob_access_utils.h"
#include "share/vector_index/ob_vector_index_common_define.h"
#include "share/vector_index/ob_plugin_vector_index_util.h"
#include "ob_vector_index_util.h"

namespace oceanbase
{
namespace share
{
class ObVectorIndexSegment;
class ObVectorIndexSegmentHandle;
class ObVectorIndexMeta;
class ObVectorIndexSegmentMeta;

class ObStreamBuf : public std::streambuf
{
public:
  explicit ObStreamBuf(char *data, const int64_t capacity)
    : std::streambuf(),
      capacity_(capacity),
      data_(data),
      last_error_code_(OB_SUCCESS)
  {}
  bool is_valid() const { return nullptr != data_; }
  bool is_success() const { return OB_SUCCESS == last_error_code_; }

  TO_STRING_KV(K_(data), K_(capacity));
protected:
  int64_t capacity_;
  char *data_;
  int last_error_code_;
};

class ObOStreamBuf : public ObStreamBuf
{
public:
  struct CbParam
  {
    virtual ~CbParam() = default;
  };
  using Callback = ObFunction<int(const char *, const int64_t, CbParam &)>;
  explicit ObOStreamBuf(char *data, const int64_t capacity, CbParam &cb_param, Callback &cb)
    : ObStreamBuf(data, capacity),
      cb_param_(cb_param),
      cb_(cb)
  {
    setp(data_, data_ + capacity_ - 1);
  }

  void check_finish();
  int get_error_code() const { return last_error_code_; }
  TO_STRING_KV(K(this));

protected:
  virtual std::streamsize xsputn(const char* s, std::streamsize count) override;
  virtual int_type overflow(int_type c) override;

private:
  int do_callback();

private:
  CbParam &cb_param_;
  Callback cb_;
};

class ObIStreamBuf : public ObStreamBuf
{
public:
  struct CbParam
  {
    virtual ~CbParam() = default;
  };
  using Callback = ObFunction<int(char *&, const int64_t, int64_t &, CbParam &)>;
  explicit ObIStreamBuf(char *data, const int64_t capacity, CbParam &cb_param, Callback &cb)
    : ObStreamBuf(data, capacity),
      cb_param_(cb_param),
      cb_(cb)
  {
    setg(data_, data_, data_);
  }
  int init();
  int get_error_code() const { return last_error_code_; }
  TO_STRING_KV(K_(data));

protected:
  virtual std::streamsize xsgetn(char* s, std::streamsize n) override;
  virtual int_type underflow() override;

  virtual pos_type seekoff(off_type off, std::ios_base::seekdir dir,
                           std::ios_base::openmode mode = std::ios_base::in | std::ios_base::out) override;

  virtual pos_type seekpos(pos_type pos, std::ios_base::openmode mode = std::ios_base::in | std::ios_base::out) override;

private:
  int do_callback();

private:
  CbParam &cb_param_;
  Callback cb_;
};

class ObHNSWDeserializeCallback {
public:
  struct CbParam : public ObIStreamBuf::CbParam {
    CbParam(ObNewRowIterator *iter, ObIAllocator *allocator)
      : iter_(iter), allocator_(allocator), str_iter_(nullptr),
        is_vec_tablet_rebuild_(false), is_need_unvisible_row_(false),
        seg_meta_(nullptr), index_type_(VIAT_MAX), start_key_(), end_key_()
    {}
    CbParam()
      : iter_(nullptr),
        allocator_(nullptr),
        str_iter_(nullptr),
        is_vec_tablet_rebuild_(false),
        is_need_unvisible_row_(false),
        seg_meta_(nullptr),
        index_type_(VIAT_MAX),
        start_key_(), end_key_()
    {}
    virtual ~CbParam() {
      if (str_iter_ != nullptr) {
        str_iter_->~ObTextStringIter();
        if (allocator_ != nullptr) {
          allocator_->free(str_iter_);
        }
        str_iter_ = nullptr;
      }
    }
    bool is_valid() const
    {
      return nullptr != iter_
             && nullptr != allocator_;
    }
    ObNewRowIterator *iter_;
    ObIAllocator *allocator_;
    ObTextStringIter *str_iter_;
    bool is_vec_tablet_rebuild_;
    bool is_need_unvisible_row_;
    ObVectorIndexSegmentMeta *seg_meta_;
    ObVectorIndexAlgorithmType index_type_;
    char start_key_buf_[OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH] = {0};
    char end_key_buf_[OB_VECTOR_INDEX_SNAPSHOT_KEY_LENGTH] = {0};
    ObString start_key_;
    ObString end_key_;
  };
public:
  ObHNSWDeserializeCallback(void *adp) : index_type_(VIAT_MAX), adp_(adp)
  {}

  int operator()(char *&data, const int64_t data_size, int64_t &read_size, share::ObIStreamBuf::CbParam &cb_param);
public:
  ObVectorIndexAlgorithmType index_type_;
  void *adp_;
};

class ObHNSWSerializeCallback {
public:
  struct CbParam : public ObOStreamBuf::CbParam {
    CbParam()
      : vctx_(nullptr), allocator_(nullptr), tmp_allocator_(nullptr), tx_desc_(nullptr), snapshot_(nullptr),
        timeout_(0), lob_inrow_threshold_(0), tablet_id_(), snapshot_version_(), need_serde_meta_(false),
        is_vec_tablet_rebuild_(false)
    {}
    virtual ~CbParam() {}
    bool is_valid() const
    {
      return nullptr != vctx_
             && nullptr != allocator_
             && nullptr != tx_desc_
             && nullptr != snapshot_
             && tablet_id_.is_valid()
             && snapshot_version_ > 0;
    }

    TO_STRING_KV(KP_(vctx), KP_(allocator), KP_(tmp_allocator), KP_(tx_desc),
        KP_(snapshot), K_(timeout), K_(lob_inrow_threshold), K_(tablet_id),
        K_(snapshot_version), K_(need_serde_meta), K_(is_vec_tablet_rebuild));

    void *vctx_; // ObVecIdxSnapshotDataWriteCtx
    ObIAllocator *allocator_;
    ObIAllocator *tmp_allocator_;
    void *tx_desc_; // transaction::ObTxDesc
    void *snapshot_; // transaction::ObTxReadSnapshot
    int64_t timeout_;
    int64_t lob_inrow_threshold_;
    ObTabletID tablet_id_;
    int64_t snapshot_version_;
    bool need_serde_meta_;
    bool is_vec_tablet_rebuild_;
  };
public:
  ObHNSWSerializeCallback()
  {}
  int operator()(const char *data, const int64_t data_size, share::ObOStreamBuf::CbParam &cb_param);
private:
};

class ObVectorIndexSerializer
{
public:
  explicit ObVectorIndexSerializer(ObIAllocator &allocator)
    : allocator_(allocator)
  {}

  int serialize_meta(ObVectorIndexSegment *segment, ObOStreamBuf::CbParam &cb_param, ObOStreamBuf::Callback &cb);
  int serialize(ObVectorIndexSegment *index, ObOStreamBuf::CbParam &cb_param, ObOStreamBuf::Callback &cb, uint64_t tenant_id, const int64_t capacity = DEFAULT_OUTBUF_CAPACITY);
  int deserialize(ObVectorIndexSegmentHandle &segment_handle, ObIStreamBuf::CbParam &cb_param, ObIStreamBuf::Callback &cb, uint64_t tenant_id);
  int deserialize(ObVectorIndexMeta& meta, ObIStreamBuf::CbParam &cb_param, ObHNSWDeserializeCallback &callback, uint64_t tenant_id);
private:
  static const int64_t DEFAULT_OUTBUF_CAPACITY = 2LL * 1024LL * 1024LL; // 2MB

private:
  bool is_inited_;
  ObIAllocator &allocator_;
};

};
};
#endif // OCEANBASE_SHARE_PLUGIN_VECTOR_INDEX_SERIALIZE_H_
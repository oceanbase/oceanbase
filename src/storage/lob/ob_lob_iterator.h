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

#ifndef OCEANBASE_STORAGE_OB_LOB_ITERATOR_H_
#define OCEANBASE_STORAGE_OB_LOB_ITERATOR_H_

#include "lib/string/ob_string.h"
#include "lib/charset/ob_charset.h"
#include "storage/lob/ob_lob_access_param.h"
#include "storage/lob/ob_lob_meta.h"
#include "storage/lob/ob_lob_meta_manager.h"

namespace oceanbase
{
namespace storage
{

class ObLobQueryIter
{
public:
  ObLobQueryIter() : cs_type_(CS_TYPE_BINARY), is_reverse_(false), is_end_(false), is_inited_(false) {}

  virtual ~ObLobQueryIter() {}

  virtual int get_next_row(ObString& data) = 0;
  virtual void reset() = 0;
  bool is_end() const { return is_end_; }

protected:
  int fill_buffer(ObString& buffer, const ObString &data, ObString &remain_data);

protected:
  ObCollationType cs_type_;
  bool is_reverse_;
  bool is_end_;
  bool is_inited_;
};


class ObLobInRowQueryIter : public ObLobQueryIter
{
public:
  ObLobInRowQueryIter() : ObLobQueryIter(), orgin_data_(), remain_data_(0) {}

  virtual ~ObLobInRowQueryIter() {}

  int open(ObString &data, uint32_t byte_offset, uint32_t byte_len, ObCollationType cs, bool is_reverse = false); // inrow open
  virtual int get_next_row(ObString& data);
  virtual void reset();

  TO_STRING_KV(
    K_(cs_type),
    K_(is_reverse),
    K_(is_end),
    K_(is_inited),
    "orgin_data_length", orgin_data_.length(),
    "remain_data_length", remain_data_.length(),
    K_(orgin_data),
    K_(remain_data)
  );

private:
  ObString orgin_data_;
  ObString remain_data_;

  DISALLOW_COPY_AND_ASSIGN(ObLobInRowQueryIter);
};

class ObLobOutRowQueryIter : public ObLobQueryIter
{
public:
  ObLobOutRowQueryIter() : ObLobQueryIter(), meta_iter_(), param_(), last_data_() {}
  virtual ~ObLobOutRowQueryIter() {}

  int open(ObLobAccessParam &param, ObLobMetaManager *lob_meta_mngr);
  virtual int get_next_row(ObString& data);
  virtual void reset();

private:
  ObLobMetaScanIter meta_iter_;
  ObLobAccessParam param_;
  ObString last_data_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobOutRowQueryIter);
};


class ObLobPartialUpdateRowIter
{
public:
  ObLobPartialUpdateRowIter():
    param_(nullptr), seq_id_tmp_(0), chunk_iter_(0)
  {}

  ~ObLobPartialUpdateRowIter();

  int open(ObLobAccessParam &param, ObLobLocatorV2 &delta_lob, ObLobDiffHeader *diff_header);

  int get_next_row(int64_t &offset, ObLobMetaInfo *&old_info, ObLobMetaInfo *&new_info);

  int64_t get_chunk_size() const  { return partial_data_.chunk_size_; }
  int64_t get_modified_chunk_cnt() const { return partial_data_.get_modified_chunk_cnt(); }

private:
  ObLobMetaInfo old_meta_info_;
  ObLobMetaInfo new_meta_info_;

  // updated lob 
  ObLobAccessParam *param_;
  int32_t seq_id_tmp_;

  ObLobLocatorV2 delta_lob_;
  ObLobPartialData partial_data_;
  int chunk_iter_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObLobPartialUpdateRowIter);
};

}  // storage
}  // oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_ITERATOR_H_
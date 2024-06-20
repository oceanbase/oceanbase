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

#include "storage/tx_table/ob_tx_table_define.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_table/ob_tx_data_table.h"

namespace oceanbase
{
namespace storage
{

void *TxDataDefaultAllocator::alloc(const int64_t size)
{
  common::ObMemAttr attr;
  attr.tenant_id_ = MTL_ID();
  attr.label_ = "TX_DATA_ITER";
  if (size <= 0) {
    abort();
  }
  return ob_malloc(size, attr);
}

int ObTxCtxTableCommonHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, MAGIC_VERSION_))) {
    TRANS_LOG(WARN, "encode version fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, DATA_LEN_))) {
    TRANS_LOG(WARN, "encode data_len fail", K(DATA_LEN_), K(buf_len), K(pos), K(ret));
  }
  return ret;
}

int ObTxCtxTableCommonHeader::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t des_version = 0;
  int64_t des_data_len = 0;

  if (OB_UNLIKELY(nullptr == buf || buf_len <= 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &des_version))) {
    TRANS_LOG(WARN, "decode version fail", K(des_version), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &des_data_len))) {
    TRANS_LOG(WARN, "decode data_len fail", K(des_data_len), K(buf_len), K(pos), K(ret));
  } else if (des_version != MAGIC_VERSION_) {
    ret = OB_NOT_SUPPORTED;
    TRANS_LOG(WARN, "object des_version mismatch", K(ret), K(des_version));
  } else if (des_data_len < 0) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "can't decode object with negative length", K(des_data_len));
  } else if (buf_len < des_data_len + pos) {
    ret = OB_DESERIALIZE_ERROR;
    TRANS_LOG(WARN, "buf length not enough", K(des_data_len), K(pos), K(buf_len));
  }

  return ret;
}

int64_t ObTxCtxTableCommonHeader::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(MAGIC_VERSION_);
  len += serialization::encoded_length_vi64(DATA_LEN_);
  return len;
}

int ObTxCtxTableInfo::serialize(char *buf,
                                const int64_t buf_len,
                                int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t data_len = get_serialize_size_();
  ObTxCtxTableCommonHeader header(MAGIC_VERSION, data_len);
  if (OB_ISNULL(tx_data_guard_.tx_data())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "invalid tx data guard", KR(ret), KPC(this));
  } else if (OB_FAIL(header.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "encode header fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialize_(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize fail", K(ret));
  }
  return ret;
}

int ObTxCtxTableInfo::serialize_(char *buf,
                                 const int64_t buf_len,
                                 int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_id_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize tx_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(ls_id_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize ls_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cluster_id_))) {
    TRANS_LOG(WARN, "encode cluster id failed", K(cluster_id_), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(tx_data_guard_.tx_data()->serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize state_info fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(exec_info_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize exec_info fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(table_lock_info_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize exec_info fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, (int64_t)cluster_version_))) {
    TRANS_LOG(WARN, "encode cluster_version fail", K(buf_len), K(pos), K(ret));
  }

  return ret;
}

int ObTxCtxTableInfo::deserialize(const char *buf,
                                  const int64_t buf_len,
                                  int64_t &pos,
                                  ObTxDataTable &tx_data_table)
{
  int ret = OB_SUCCESS;
  ObTxCtxTableCommonHeader header(MAGIC_VERSION, 0);

  if (OB_FAIL(tx_data_table.alloc_tx_data(tx_data_guard_, false/* enable_throttle */))) {
      STORAGE_LOG(WARN, "alloc tx data failed", KR(ret));
  } else if (OB_FAIL(header.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize header fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(deserialize_(buf, buf_len, pos, tx_data_table))) {
    TRANS_LOG(WARN, "deserialize_ fail", "buf_len", buf_len, K(pos), K(ret));
  }
  return ret;
}

int ObTxCtxTableInfo::deserialize_(const char *buf,
                                   const int64_t buf_len,
                                   int64_t &pos,
                                   ObTxDataTable &tx_data_table)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_id_.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize tx_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(ls_id_.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize ls_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &cluster_id_))) {
    TRANS_LOG(WARN, "encode cluster_id fail", K(cluster_id_), K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(tx_data_guard_.tx_data()->deserialize(buf, buf_len, pos, *tx_data_table.get_tx_data_allocator()))) {
    TRANS_LOG(WARN, "deserialize state_info fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(exec_info_.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize exec_info fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(table_lock_info_.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize exec_info fail.", KR(ret), K(pos), K(buf_len));
  }
  // _NOTE_
  // before 4.2.1.1, the serialize size of table_lock_info_
  // is not accurate(which larger than real serialize size),
  // this caused the size of ObTxCtxTableInfo is also inaccurate
  //
  // when deserialize use `compatible_version_` to decide whether
  // guess extra members by examine remain buf size
  if (OB_SUCC(ret) && compatible_version_ >= ObTxCtxTableMeta::VERSION_1 && buf_len > pos) {
    // has remains, continue to deserialize new members
    if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, (int64_t*)&cluster_version_))) {
      TRANS_LOG(WARN, "dencode cluster_version fail", K(buf_len), K(pos), K(ret));
    } else if (cluster_version_ && cluster_version_ < CLUSTER_VERSION_4_0_0_0) {
      // sanity-check: the decoded version is valid, since v_4_x will not replay
      // redo of less than or equals to v_3_x, the cluster version must be v_4_x
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "cluster version malformed", K(cluster_version_), KPC(this));
    }
  }

  return ret;
}

int64_t ObTxCtxTableInfo::get_serialize_size(void) const
{
  int64_t data_len = get_serialize_size_();
  ObTxCtxTableCommonHeader header(MAGIC_VERSION, data_len);

  int64_t len = 0;
  len += header.get_serialize_size();
  len += data_len;

  return len;
}

int64_t ObTxCtxTableInfo::get_serialize_size_(void) const
{
  int64_t len = 0;
  len += tx_id_.get_serialize_size();
  len += ls_id_.get_serialize_size();
  len += serialization::encoded_length_vi64(cluster_id_);
  len += serialization::encoded_length_vi64((int64_t)cluster_version_);
  len += (OB_NOT_NULL(tx_data_guard_.tx_data()) ? tx_data_guard_.tx_data()->get_serialize_size() : 0);
  len += exec_info_.get_serialize_size();
  len += table_lock_info_.get_serialize_size();
  return len;
}

bool ObTxCtxTableInfo::is_valid() const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tx_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "tx id not valid", K(ret), K(tx_id_));
  } else if (OB_UNLIKELY(!ls_id_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "ls id not valid", K(ret), K(ls_id_));
    // TODO: gengli  this is invalid if it a uncommited tx
    // } else if (OB_UNLIKELY(!state_info_.is_valid())) {
    //   ret = OB_ERR_UNEXPECTED;
    //   TRANS_LOG(ERROR, "state info not valid", K(ret), K(state_info_));
  } else {
    // do nothing
  }
  return OB_SUCC(ret);
}

int ObTxCtxTableMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t data_len = get_serialize_size_();
  ObTxCtxTableCommonHeader header(MAGIC_VERSION, data_len);
  if (OB_FAIL(header.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "encode header fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialize_(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize fail", K(ret));
  }
  return ret;
}

int ObTxCtxTableMeta::serialize_(char* buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_id_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize tx_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(ls_id_.serialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "serialize ls_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, tx_ctx_serialize_size_))) {
    TRANS_LOG(WARN, "encode serizlize_size_ fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, row_num_))) {
    TRANS_LOG(WARN, "encode row_num fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, row_idx_))) {
    TRANS_LOG(WARN, "encode row_idx fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(serialization::encode_vi32(buf, buf_len, pos, version_))) {
    TRANS_LOG(WARN, "encode version fail", K(buf_len), K(pos), K(ret));
  } else {
    TRANS_LOG(DEBUG, "ObTxCtxTableMeta encode succ", K(buf_len), K(pos));
  }
  return ret;
}

int ObTxCtxTableMeta::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObTxCtxTableCommonHeader header(MAGIC_VERSION, 0);

  if (OB_FAIL(header.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize header fail", K(buf_len), K(pos), K(ret));
  } else if (OB_FAIL(deserialize_(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize_ fail", "buf_len", buf_len, K(pos), K(ret));
  }
  return ret;
}

int ObTxCtxTableMeta::deserialize_(const char* buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(tx_id_.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize tx_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(ls_id_.deserialize(buf, buf_len, pos))) {
    TRANS_LOG(WARN, "deserialize ls_id fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::decode_vi64(buf, buf_len, pos, &tx_ctx_serialize_size_))) {
    TRANS_LOG(WARN, "deserialize serizlize_size fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::decode_vi32(buf, buf_len, pos, &row_num_))) {
    TRANS_LOG(WARN, "deserialize row_num fail.", KR(ret), K(pos), K(buf_len));
  } else if (OB_FAIL(serialization::decode_vi32(buf, buf_len, pos, &row_idx_))) {
    TRANS_LOG(WARN, "deserialize row_idx fail.", KR(ret), K(pos), K(buf_len));
  } else if (pos < buf_len) { // decode the version field
    if (OB_FAIL(serialization::decode_vi32(buf, buf_len, pos, &version_))) {
      TRANS_LOG(WARN, "deserialize version fail.", KR(ret), K(pos), K(buf_len));
    }
  } else {
    version_ = VERSION_0; // VERSION_0 without version field serialized
  }
  return ret;
}

int64_t ObTxCtxTableMeta::get_serialize_size() const
{
  int64_t data_len = get_serialize_size_();
  ObTxCtxTableCommonHeader header(MAGIC_VERSION, data_len);

  int64_t len = 0;
  len += header.get_serialize_size();
  len += data_len;

  return len;
}

int64_t ObTxCtxTableMeta::get_serialize_size_() const
{
  int64_t len = 0;
  len += tx_id_.get_serialize_size();
  len += ls_id_.get_serialize_size();
  len += serialization::encoded_length_vi64(tx_ctx_serialize_size_);
  len += serialization::encoded_length_vi32(row_num_);
  len += serialization::encoded_length_vi32(row_idx_);
  len += serialization::encoded_length_vi32(version_);
  return len;
}

DEF_TO_STRING(ObCommitVersionsArray::Node)
{
  int64_t pos = 0;
  J_KV(K_(start_scn),
       K_(commit_version));
  return pos;
}

DEF_TO_STRING(ObCommitVersionsArray)
{
  int64_t pos = 0;
  J_KV(KP(this), K(array_.count()));
  return pos;
}

int ObCommitVersionsArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  const int64_t len = get_serialize_size_();

  if (OB_UNLIKELY(OB_ISNULL(buf) || buf_len <= 0 || pos > buf_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "serialize ObCommitVersionsArray failed.", KR(ret), KP(buf), K(buf_len),
                K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, UNIS_VERSION))) {
    STORAGE_LOG(WARN, "encode UNIS_VERSION failed.", KR(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, len))) {
    STORAGE_LOG(WARN, "encode length of commit versions array failed.", KR(ret), KP(buf),
                K(buf_len), K(pos));
  } else if (OB_FAIL(serialize_(buf, buf_len, pos))) {
    STORAGE_LOG(WARN, "serialize_ ObCommitVersionsArray failed.", KR(ret), KP(buf), K(buf_len),
                K(pos));
  }

  return ret;
}

int ObCommitVersionsArray::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;
  array_.reuse();

  if (OB_UNLIKELY(nullptr == buf || data_len <= 0 || pos > data_len)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments.", KP(buf), K(data_len), K(ret));
  } else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &version))) {
    STORAGE_LOG(WARN, "decode version fail", K(version), K(data_len), K(pos), K(ret));
  } else if (version != UNIS_VERSION) {
    ret = OB_VERSION_NOT_MATCH;
    STORAGE_LOG(WARN, "object version mismatch", K(ret), K(version));
  }  else if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &len))) {
    STORAGE_LOG(WARN, "decode data len fail", K(len), K(data_len), K(pos), K(ret));
  } else if (OB_UNLIKELY(len < 0)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "can't decode object with negative length", KR(ret), K(len));
  } else if (OB_UNLIKELY(data_len < len + pos)) {
    ret = OB_DESERIALIZE_ERROR;
    STORAGE_LOG(WARN, "buf length not correct", KR(ret), K(len), K(pos), K(data_len));
  } else {
    int64_t original_pos = pos;
    pos = 0;
    array_.reuse();
    if (OB_FAIL(deserialize_(buf + original_pos, len, pos))) {
      STORAGE_LOG(WARN, "deserialize_ ObCommitVersionsArray fail", K(len), K(pos), K(ret));
    }
    pos += original_pos;
  }

  return ret;
}

int64_t ObCommitVersionsArray::get_serialize_size() const
{
  int64_t data_len = get_serialize_size_();
  int64_t len = 0;
  len += serialization::encoded_length_vi64(UNIS_VERSION);
  len += serialization::encoded_length_vi64(data_len);
  len += data_len;
  return len;
}

int ObCommitVersionsArray::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < array_.count(); i++) {
    LST_DO_CODE(OB_UNIS_ENCODE, array_.at(i).start_scn_, array_.at(i).commit_version_);
  }
  return ret;
}

int ObCommitVersionsArray::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  ObCommitVersionsArray::Node node;
  while (OB_SUCC(ret) && pos < data_len) {
    LST_DO_CODE(OB_UNIS_DECODE, node.start_scn_, node.commit_version_);
    array_.push_back(node);
  }

  return ret;
}

int64_t ObCommitVersionsArray::get_serialize_size_() const
{
  int64_t len = 0;
  for (int i = 0; i < array_.count(); i++) {
    LST_DO_CODE(OB_UNIS_ADD_LEN, array_.at(i).start_scn_, array_.at(i).commit_version_);
  }
  return len;
}

bool ObCommitVersionsArray::is_valid()
{
  bool bool_ret = true;
  for (int i = 0; i < array_.count() - 1; i++) {
    if (!array_.at(i).start_scn_.is_valid() ||
        !array_.at(i).commit_version_.is_valid() ||
        array_.at(i).start_scn_ > array_.at(i + 1).start_scn_ ||
        array_.at(i).start_scn_ > array_.at(i).commit_version_) {
      bool_ret = false;
      STORAGE_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "this commit version array is invalid", K(array_.at(i)),
                  K(array_.at(i + 1)));
    }
  }
  return bool_ret;
}

bool ObITxDataCheckFunctor::is_decided() const
{
  return tx_data_check_data_.is_rollback_ ||
    ObTxData::ABORT == tx_data_check_data_.state_;
}

void ObITxDataCheckFunctor::resolve_tx_data_check_data_(const int32_t state,
                                                        const share::SCN commit_version,
                                                        const share::SCN end_scn,
                                                        const bool is_rollback)
{
  tx_data_check_data_.state_ = state;
  tx_data_check_data_.commit_version_ = commit_version;
  tx_data_check_data_.end_scn_ = end_scn;
  tx_data_check_data_.is_rollback_ = is_rollback;
}

bool ObITxDataCheckFunctor::may_exist_undecided_state_in_tx_data_table() const
{
  return may_exist_undecided_state_in_tx_data_table_;
}

void ObITxDataCheckFunctor::set_may_exist_undecided_state_in_tx_data_table()
{
  may_exist_undecided_state_in_tx_data_table_ = true;
}

} // end namespace transaction
} // end namespace oceanbase


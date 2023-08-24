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

#include "storage/memtable/ob_memtable_mutator.h"

#include "lib/atomic/atomic128.h"
#include "lib/utility/serialization.h"
#include "lib/checksum/ob_crc64.h"
#include "lib/utility/ob_tracepoint.h"

#include "storage/memtable/ob_memtable_context.h"     // ObTransRowFlag
#include "storage/tx/ob_clog_encrypter.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
using namespace common;
using namespace share;
using namespace serialization;
using namespace storage;
using namespace blocksstable;
namespace memtable
{
ObMemtableMutatorMeta::ObMemtableMutatorMeta():
    magic_(MMB_MAGIC),
    meta_crc_(0),
    meta_size_(sizeof(*this)),
    version_(0),
    flags_(ObTransRowFlag::NORMAL_ROW),
    data_crc_(0),
    data_size_(0),
    row_count_(0),
    unused_(0)
{
}

ObMemtableMutatorMeta::~ObMemtableMutatorMeta()
{
}

bool ObMemtableMutatorMeta::check_magic()
{
  return MMB_MAGIC == magic_;
}

int ObMemtableMutatorMeta::fill_header(const char *buf, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    data_size_ = (uint32_t)data_len;
    data_crc_ = (uint32_t)ob_crc64(buf, data_len);
    meta_crc_ = calc_meta_crc((const char *)this);
  }
  return ret;
}

int ObMemtableMutatorMeta::check_data_integrity(const char *buf, const int64_t data_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || data_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (data_crc_ != (uint32_t)ob_crc64(buf, data_len)) {
    ret = OB_INVALID_LOG;
    TRANS_LOG(WARN, "check_data_integrity fail", K(ret));
  }
  return ret;
}

uint32_t ObMemtableMutatorMeta::calc_meta_crc(const char *buf)
{
  int64_t skip_size = ((char *)&meta_size_ - (char *)this);
  return (uint32_t)ob_crc64(buf + skip_size, meta_size_ - skip_size);
}

int ObMemtableMutatorMeta::inc_row_count()
{
  int ret = OB_SUCCESS;
  ++row_count_;
  return ret;
}

int64_t ObMemtableMutatorMeta::get_row_count() const
{
  return row_count_;
}

int64_t ObMemtableMutatorMeta::get_serialize_size() const
{
  return sizeof(*this);
}

int ObMemtableMutatorMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int32_t meta_size = static_cast<int32_t>(get_serialize_size());
  if (OB_ISNULL(buf) || pos + meta_size > buf_len) {
    TRANS_LOG(WARN, "invalid param", KP(buf), K(meta_size), K(buf_len));
    ret = OB_INVALID_ARGUMENT;
  } else {
    MEMCPY(buf + pos, this, meta_size);
    pos += meta_size;
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "serialize fail",
              "ret", ret,
              "buf", OB_P(buf),
              "buf_len", buf_len,
              "meta_size", meta_size);
  }
  return ret;
}

int ObMemtableMutatorMeta::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  const int32_t min_meta_size = MIN_META_SIZE;
  if (OB_ISNULL(buf) || data_len < 0 || pos + min_meta_size > data_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid param", K(ret), KP(buf), K(data_len), K(pos));
  } else {
    MEMCPY(this, buf + pos, min_meta_size);
    if (!check_magic()) {
      ret = OB_INVALID_LOG;
      TRANS_LOG(WARN, "invalid log: check_magic fail", K(*this));
    } else if (calc_meta_crc(buf + pos) != meta_crc_) {
      ret = OB_INVALID_LOG;
      TRANS_LOG(WARN, "invalid log: check_meta_crc fail", K(*this));
    } else if (pos + meta_size_ > data_len) {
      ret = OB_BUF_NOT_ENOUGH;
      TRANS_LOG(WARN, "buf not enough", K(pos), K(meta_size_), K(data_len));
    } else {
      MEMCPY(this, buf + pos, min(sizeof(*this), static_cast<uint64_t>(meta_size_)));
      pos += meta_size_;
    }
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "deserialize fail",
              "ret", ret,
              "buf", OB_P(buf),
              "data_len", data_len,
              "pos", pos,
              "meta_size", meta_size_);
  }
  return ret;
}

int64_t ObMemtableMutatorMeta::to_string(char *buffer, const int64_t length) const
{
  int64_t pos = 0;
  common::databuff_printf(buffer, length, pos,
                          "%p data_crc=%x meta_size=%d data_size=%d row_count=%d",
                          this, data_crc_, meta_size_, data_size_, row_count_);
  return pos;
}

bool ObMemtableMutatorMeta::is_row_start() const
{
  return ObTransRowFlag::is_row_start(flags_);
}

//only meta_crc is newly generated here, other information will keep unchanged
void ObMemtableMutatorMeta::generate_new_header()
{
  meta_crc_ = calc_meta_crc((const char *)this);
}

int ObMemtableMutatorMeta::set_flags(const uint8_t row_flag)
{
  int ret = OB_SUCCESS;

  if (!ObTransRowFlag::is_valid_row_flag(row_flag)) {
    TRANS_LOG(WARN, "invalid argument", K(row_flag));
    ret = OB_INVALID_ARGUMENT;
  } else {
    flags_ = row_flag;
  }

  return ret;
}

void ObMemtableMutatorMeta::add_encrypt_flag()
{
  ObTransRowFlag::add_encrypt_flag(flags_);
}

void ObMemtableMutatorMeta::remove_encrypt_flag()
{
  ObTransRowFlag::remove_encrypt_flag(flags_);
}

ObEncryptRowBuf::ObEncryptRowBuf() : ptr_(nullptr)
{}

ObEncryptRowBuf::~ObEncryptRowBuf()
{
  if (OB_NOT_NULL(ptr_)) {
    ob_free(ptr_);
    ptr_ = nullptr;
  }
}

int ObEncryptRowBuf::alloc(const int64_t size)
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ptr_)) {
    ob_free(ptr_);
    ptr_ = nullptr;
  }
  if (nullptr == (ptr_ = reinterpret_cast<char *>(ob_malloc(size,
                         ObModIds::OB_TRANS_CLOG_ENCRYPT_INFO)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "alloc decrypt buf failed", K(ret), K(size));
  }
  return ret;
}

ObMutator::ObMutator():
    rowkey_(),
    row_size_(0),
    table_id_(OB_INVALID_ID),
    table_version_(0),
    seq_no_()
{
  rowkey_.assign((ObObj*)obj_array_, OB_MAX_ROWKEY_COLUMN_NUMBER);
}

ObMutator::ObMutator(
    const uint64_t table_id,
    const common::ObStoreRowkey &rowkey,
    const int64_t table_version,
    const transaction::ObTxSEQ seq_no):
    rowkey_(rowkey),
    row_size_(0),
    table_id_(table_id),
    table_version_(table_version),
    seq_no_(seq_no)
{}

void ObMutator::reset()
{
  row_size_ = 0;
  table_id_ = OB_INVALID_ID;
  rowkey_.reset();
  rowkey_.get_rowkey().assign((ObObj*)obj_array_, OB_MAX_ROWKEY_COLUMN_NUMBER);
  table_version_ = 0;
  seq_no_.reset();
}

const char *get_mutator_type_str(MutatorType mutator_type)
{
  const char *type_str = nullptr;
  switch (mutator_type) {
  case MutatorType::MUTATOR_ROW: {
    type_str = "MUTATOR_ROW";
    break;
  }
  case MutatorType::MUTATOR_TABLE_LOCK: {
    type_str = "MUTATOR_TABLE_LOCK";
    break;
  }
  default: {
    type_str = "UNKNOWN_MUTATOR_TYPE";
    break;
  }
  }

  return type_str;
}

int ObMutatorRowHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(encode_i32(buf, buf_len, new_pos, MAGIC_NUM))) {
    TRANS_LOG(WARN, "serialize magic number failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(encode_i8(buf, buf_len, new_pos, (int8_t)mutator_type_))) {
    TRANS_LOG(WARN, "serialize mutator type failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(tablet_id_.serialize(buf, buf_len, new_pos))) {
    TRANS_LOG(WARN, "serialize tablet_id_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    pos = new_pos;
  }
 
  return ret;
}

int ObMutatorRowHeader::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int32_t magic_num = 0;
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(decode_i32(buf, buf_len, new_pos, &magic_num))) {
    TRANS_LOG(WARN, "deserialize magic num fail", K(ret), K(buf_len), K(new_pos));
  } else if (magic_num != MAGIC_NUM) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "magic num not match, maybe error.", K(ret), K(magic_num));
  } else if (OB_FAIL(decode_i8(buf, buf_len, new_pos, (int8_t *)&mutator_type_))) {
    TRANS_LOG(WARN, "deserialize mutator type fail", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(tablet_id_.deserialize(buf, buf_len, new_pos))) {
    TRANS_LOG(WARN, "serialize tablet_id_ failed", K(ret), KP(buf), K(buf_len), K(pos));
  } else {
    pos = new_pos;
  }
  return ret;
}

ObMemtableMutatorRow::ObMemtableMutatorRow():
    dml_flag_(ObDmlFlag::DF_NOT_EXIST),
    update_seq_(0),
    acc_checksum_(0),
    version_(0),
    flag_(0),
    column_cnt_(0)
{
}

ObMemtableMutatorRow::ObMemtableMutatorRow(const uint64_t table_id,
                                           const ObStoreRowkey &rowkey,
                                           const int64_t table_version,
                                           const ObRowData &new_row,
                                           const ObRowData &old_row,
                                           const blocksstable::ObDmlFlag dml_flag,
                                           const uint32_t modify_count,
                                           const uint32_t acc_checksum,
                                           const int64_t version,
                                           const int32_t flag,
                                           const transaction::ObTxSEQ seq_no,
                                           const int64_t column_cnt):
    ObMutator(table_id, rowkey, table_version, seq_no),
    dml_flag_(dml_flag),
    update_seq_((uint32_t)modify_count),
    new_row_(new_row),
    old_row_(old_row),
    acc_checksum_(acc_checksum),
    version_(version),
    flag_(flag),
    column_cnt_(column_cnt)
{}

ObMemtableMutatorRow::~ObMemtableMutatorRow()
{}

void ObMemtableMutatorRow::reset()
{
  ObMutator::reset();
  dml_flag_ = ObDmlFlag::DF_NOT_EXIST;
  update_seq_ = 0;
  new_row_.reset();
  old_row_.reset();
  acc_checksum_ = 0;
  version_ = 0;
  flag_ = 0;
}

int ObMemtableMutatorRow::copy(uint64_t &table_id,
                               ObStoreRowkey &rowkey,
                               int64_t &table_version,
                               ObRowData &new_row,
                               ObRowData &old_row,
                               blocksstable::ObDmlFlag &dml_flag,
                               uint32_t &modify_count,
                               uint32_t &acc_checksum,
                               int64_t &version,
                               int32_t &flag,
                               transaction::ObTxSEQ &seq_no,
                               int64_t &column_cnt) const
{
  int ret = OB_SUCCESS;
  table_id = table_id_;
  rowkey = rowkey_;
  table_version = table_version_;
  new_row = new_row_;
  old_row = old_row_;
  dml_flag = dml_flag_;
  modify_count = update_seq_;
  acc_checksum = acc_checksum_;
  version = version_;
  flag = flag_;
  seq_no = seq_no_;
  column_cnt = column_cnt_;
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObMemtableMutatorRow::handle_encrypt_row_(char *buf, const int64_t buf_len,
                                              const int64_t start_pos, int64_t &end_pos,
                                              const share::ObEncryptMeta &meta)
{
  int ret = OB_SUCCESS;
  int64_t encrypted_len = 0;
  const int64_t expected_encrypted_len = ObEncryptionUtil::encrypted_length(
                         static_cast<ObCipherOpMode>(meta.encrypt_algorithm_), end_pos - start_pos);
  char *encrypted_buf = nullptr;
  int64_t encrypted_buf_size = 0;
  ObEncryptRowBuf row_buf;
  #ifdef ERRSIM
  ret = OB_E(EventTable::EN_ENCRYPT_ALLOCATE_ROW_BUF_FAILED) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "ERRSIM, alloc encrypt buf failed", K(ret));
    return ret;
  }
  #endif
  if (OB_ISNULL(buf) || buf_len < 0 || start_pos < 0 || start_pos > end_pos) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(buf_len), K(start_pos), K(end_pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (expected_encrypted_len >= ObEncryptRowBuf::TMP_ENCRYPT_BUF_LEN) {
    if (OB_FAIL(row_buf.alloc(expected_encrypted_len))) {
      TRANS_LOG(WARN, "alloc encrypt buf failed", K(ret), K(table_id_), K(expected_encrypted_len));
    } else {
      encrypted_buf = row_buf.ptr_;
      encrypted_buf_size = expected_encrypted_len;
    }
  } else {
    encrypted_buf = row_buf.buf_;
    encrypted_buf_size = ObEncryptRowBuf::TMP_ENCRYPT_BUF_LEN;
  }
  if (OB_SUCC(ret)) {
    if (NULL == encrypted_buf) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "encrypted_buf init fail", "ret", ret);
    } else if (OB_FAIL(ObEncryptionUtil::encrypt_data(meta,
                                                      buf + start_pos,
                                                      end_pos - start_pos,
                                                      encrypted_buf,
                                                      encrypted_buf_size,
                                                      encrypted_len))) {
      TRANS_LOG(WARN, "encyrpt row data failed", K(ret), K(table_id_), KP(buf), K(buf_len), K(start_pos), K(end_pos));
    } else if (start_pos + encrypted_len > buf_len) {
      TRANS_LOG(WARN, "buffer not enough for encrypted row", K(table_id_), KP(buf), K(buf_len), K(start_pos), K(end_pos));
      ret = OB_BUF_NOT_ENOUGH;
    } else {
      MEMCPY(buf + start_pos, encrypted_buf, encrypted_len);
      end_pos = start_pos + encrypted_len;
    }
  }
  if (EXECUTE_COUNT_PER_SEC(1)) {
    TRANS_LOG(INFO, "encrypt data for table", K(table_id_), K(ret));
  }
  return ret;
}
#endif

int ObMemtableMutatorRow::serialize(char *buf, int64_t &buf_len, int64_t &pos,
                                    const transaction::ObTxEncryptMeta *encrypt_meta,
                                    transaction::ObCLogEncryptInfo &encrypt_info,
                                    const bool is_big_row)
{
  int ret = OB_SUCCESS;
  transaction::ObTxEncryptMeta *old_meta = NULL;
  bool need_encrypt = false;
  bool use_old = false;
  UNUSED(is_big_row);

  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
#ifdef OB_BUILD_TDE_SECURITY
  } else if (encrypt_meta != NULL && encrypt_meta->is_valid()) {
    need_encrypt = true;
    table_id_ = encrypt_meta->table_id_;
    if (OB_FAIL(encrypt_info.store_and_get_old(table_id_, *encrypt_meta, &old_meta))) {
      if (ret == OB_ENTRY_EXIST) {
        //if hash exist, we give priority to the old_meta
        //in order to avoid inconsistent encrypt_meta for the same table_id
        use_old = true;
        ret = OB_SUCCESS;
      } else {
        TRANS_LOG(WARN, "store clog encrypt info failed", K(ret));
      }
    } else {
      int64_t meta_size = encoded_length_vi64(table_id_) + encrypt_meta->meta_.get_serialize_size();
      if (buf_len - meta_size < OB_MAX_COUNT_NEED_BYTES) {
        //need delete this encrypt meta
        if (OB_FAIL(encrypt_info.remove(table_id_))) {
          TRANS_LOG(WARN, "remove clog encrypt info failed", K(ret));
        }
        ret = OB_SIZE_OVERFLOW;
      } else {
        buf_len -= meta_size;
      }
    }
#endif
  }

  if (OB_SUCC(ret)) {
    int64_t data_pos;
    int64_t new_pos = pos + encoded_length_i32(0);
    if (OB_FAIL(encode_vi64(buf, buf_len, new_pos, table_id_))) {
      TRANS_LOG(WARN, "serialize table id failed", K(ret), KP(buf), K(buf_len), K(pos));
    } else if (FALSE_IT(data_pos = new_pos)) {
    } else if (OB_FAIL(rowkey_.serialize(buf, buf_len, new_pos))
        || OB_FAIL(encode_vi64(buf, buf_len, new_pos, table_version_))
        || OB_FAIL(encode_i8(buf, buf_len, new_pos, dml_flag_))
        || OB_FAIL(encode_vi32(buf, buf_len, new_pos, update_seq_))
        || OB_FAIL(new_row_.serialize(buf, buf_len, new_pos))
        || OB_FAIL(old_row_.serialize(buf, buf_len, new_pos))
        || OB_FAIL(encode_vi32(buf, buf_len, new_pos, acc_checksum_))
        || OB_FAIL(encode_vi64(buf, buf_len, new_pos, version_))
        || OB_FAIL(encode_vi32(buf, buf_len, new_pos, flag_))
        || OB_FAIL(seq_no_.serialize(buf, buf_len, new_pos))) {
        if (OB_BUF_NOT_ENOUGH != ret || buf_len > common::OB_MAX_LOG_ALLOWED_SIZE) {
          TRANS_LOG(INFO, "serialize row fail", K(ret), KP(buf), K(buf_len), K(pos));
        }
#ifdef OB_BUILD_TDE_SECURITY
    } else if (need_encrypt) {
      if (use_old) {
        ret = handle_encrypt_row_(buf, buf_len, data_pos, new_pos, old_meta->meta_);
      } else {
        ret = handle_encrypt_row_(buf, buf_len, data_pos, new_pos, encrypt_meta->meta_);
      }
#endif
    }
    if (FAILEDx(encode_vi64(buf, buf_len, new_pos, column_cnt_))) {
      TRANS_LOG(WARN, "failed to serialize column cnt", K(column_cnt_));
    } else if (FALSE_IT(row_size_ = (uint32_t )(new_pos - pos))) {
    } else if (OB_FAIL(encode_i32(buf, buf_len, pos, row_size_))) {
      TRANS_LOG(WARN, "serialize row fail", K(ret), K(buf_len), K(pos), K(table_id_));
    } else {
      pos = new_pos;
    }
  }
  return ret;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObMemtableMutatorRow::handle_decrypt_row_(const char *buf, const int64_t start_pos,
    const int64_t end_pos, ObEncryptRowBuf &row_buf, const char* &out_buf, int64_t &out_len,
    const share::ObEncryptMeta &meta)
{
  int ret = OB_SUCCESS;
  const int64_t expected_decrypted_len = end_pos - start_pos;
  int64_t buf_size = 0;
  char *tmp_buf = nullptr;
  #ifdef ERRSIM
  ret = OB_E(EventTable::EN_DECRYPT_ALLOCATE_ROW_BUF_FAILED) OB_SUCCESS;
  if (OB_FAIL(ret)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    TRANS_LOG(WARN, "ERRSIM, alloc decrypt buf failed", K(ret));
    return ret;
  }
  #endif
  if (OB_ISNULL(buf) || start_pos < 0 || start_pos > end_pos) {
    TRANS_LOG(WARN, "handle_decrypt_row_, invalid argument", KP(buf), K(start_pos), K(end_pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (expected_decrypted_len >= ObEncryptRowBuf::TMP_ENCRYPT_BUF_LEN) {
    if (OB_FAIL(row_buf.alloc(expected_decrypted_len))) {
      TRANS_LOG(WARN, "alloc decrypt buf failed", K(ret), K(table_id_), K(expected_decrypted_len));
    } else {
      tmp_buf = row_buf.ptr_;
      buf_size = expected_decrypted_len;
    }
  } else {
    tmp_buf = row_buf.buf_;
    buf_size = ObEncryptRowBuf::TMP_ENCRYPT_BUF_LEN;
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(ObEncryptionUtil::decrypt_data(meta,
                  buf + start_pos, end_pos - start_pos, tmp_buf, buf_size, out_len))) {
      TRANS_LOG(WARN, "decrypt row data failed", K(ret), K(table_id_));
    } else {
      out_buf = tmp_buf;
    }
  }
  if (EXECUTE_COUNT_PER_SEC(1)) {
    TRANS_LOG(INFO, "decrypt data for table", K(table_id_), K(ret));
  }
  return ret;
}
#endif

int ObMemtableMutatorRow::deserialize(const char *buf, const int64_t buf_len, int64_t &pos,
                                      ObEncryptRowBuf &row_buf,
                                      const transaction::ObCLogEncryptInfo &encrypt_info,
                                      const bool need_extract_encrypt_meta,
                                      ObEncryptMeta &final_encrypt_meta,
                                      ObCLogEncryptStatMap &encrypt_stat_map,
                                      const bool is_big_row)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  uint32_t encrypted_len = 0;
  const char *decrypted_buf = nullptr;
  int64_t decrypted_len = 0;
  UNUSED(is_big_row);
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(decode_i32(buf, buf_len, new_pos, (int32_t *)&encrypted_len))) {
    TRANS_LOG(WARN, "deserialize encrypted length fail", K(ret), K(buf_len), K(new_pos));
  } else if (pos + encrypted_len > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "size overflow", K(ret), KP(buf), K(buf_len), K(pos), K(encrypted_len));
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, (int64_t *)&table_id_))) {
    TRANS_LOG(WARN, "deserialize table id failed", K(ret), K(buf_len), K(new_pos));
  } else {
    int64_t data_pos = new_pos;
#ifdef OB_BUILD_TDE_SECURITY
    //TODO: table_id_ is just used as encrypt_index, we may rename it in the future
    bool need_decrypt = table_id_ > 0 ? true : false;
    if (need_decrypt) {
      transaction::ObTxEncryptMeta *encrypt_meta = NULL;
      if (OB_FAIL(encrypt_info.get_encrypt_info(table_id_, encrypt_meta))) {
        TRANS_LOG(ERROR, "failed to get encrypt info", K(ret), K(table_id_));
      } else {
        ret = handle_decrypt_row_(buf, data_pos, pos + encrypted_len,
                                  row_buf, decrypted_buf, decrypted_len, encrypt_meta->meta_);
        if (OB_SUCC(ret)) {
          row_size_ = decrypted_len + (data_pos - pos);
          new_pos = 0;
          if (need_extract_encrypt_meta) {
            encrypt_stat_map.set_map(CLOG_CONTAIN_ENCRYPTED_ROW);
            ObCipherOpMode final_mode = static_cast<ObCipherOpMode>(final_encrypt_meta.encrypt_algorithm_);
            ObCipherOpMode cur_mode = static_cast<ObCipherOpMode>(encrypt_meta->meta_.encrypt_algorithm_);
            if (ObBlockCipher::compare_aes_mod_safety(final_mode, cur_mode)) {
              if (OB_FAIL(final_encrypt_meta.assign(encrypt_meta->meta_))) {
                TRANS_LOG(WARN, "failed to assign encrypt_meta", K(ret), K(table_id_));
              }
            }
          }
        }
      }
    } else {
      //no encryption
      decrypted_buf = buf + data_pos;
      decrypted_len = encrypted_len - (data_pos - pos);
      row_size_ = encrypted_len;
      new_pos = 0;
      if (need_extract_encrypt_meta) {
        encrypt_stat_map.set_map(CLOG_CONTAIN_NON_ENCRYPTED_ROW);
      }
    }
#else
    decrypted_buf = buf + data_pos;
    decrypted_len = encrypted_len - (data_pos - pos);
    row_size_ = encrypted_len;
    new_pos = 0;
#endif
    if (OB_SUCC(ret)) {
      if (NULL == decrypted_buf) {
        ret = OB_ERR_UNEXPECTED;
        TRANS_LOG(WARN, "decrypted_buf init fail", "ret", ret);
      } else if (OB_FAIL(rowkey_.deserialize(decrypted_buf, decrypted_len, new_pos))
                || OB_FAIL(decode_vi64(decrypted_buf, decrypted_len, new_pos, &table_version_))
                || OB_FAIL(decode_i8(decrypted_buf, decrypted_len, new_pos, (int8_t *)&dml_flag_))
                || OB_FAIL(decode_vi32(decrypted_buf, decrypted_len, new_pos, (int32_t *)&update_seq_))
                || OB_FAIL(new_row_.deserialize(decrypted_buf, decrypted_len, new_pos))
                || OB_FAIL(old_row_.deserialize(decrypted_buf, decrypted_len, new_pos))) {
        TRANS_LOG(WARN, "deserialize row fail", K(ret), K(table_id_));
      } else {
        acc_checksum_ = 0;
        version_ = 0;
        if (new_pos < decrypted_len) {
          if (OB_FAIL(decode_vi32(decrypted_buf, decrypted_len, new_pos, (int32_t *)&acc_checksum_))) {
            TRANS_LOG(WARN, "deserialize acc checksum fail", K(ret), K(table_id_), K(decrypted_len), K(new_pos));
          } else if (OB_FAIL(decode_vi64(decrypted_buf, decrypted_len, new_pos, (int64_t *)&version_))) {
            TRANS_LOG(WARN, "deserialize version fail", K(ret), K(table_id_), K(decrypted_len), K(new_pos));
          } else {
            // do nothing
          }
        }
        if (OB_SUCC(ret) && (new_pos < decrypted_len)) {
          if (OB_FAIL(decode_vi32(decrypted_buf, decrypted_len, new_pos, (int32_t *)&flag_))) {
            TRANS_LOG(WARN, "deserialize flag fail", K(ret), K(table_id_), K(decrypted_len), K(new_pos));
          }
        }
        if (OB_SUCC(ret) && (new_pos < decrypted_len)) {
          if (OB_FAIL(seq_no_.deserialize(decrypted_buf, decrypted_len, new_pos))) {
            TRANS_LOG(WARN, "deserialize seq no fail", K(ret), K(table_id_), K(decrypted_len), K(new_pos));
          }
        }
        if (OB_SUCC(ret) && (new_pos < decrypted_len)) {
          if (OB_FAIL(decode_vi64(decrypted_buf, decrypted_len, new_pos, (int64_t *)&column_cnt_))) {
            TRANS_LOG(WARN, "deserialize column cnt fail", K(ret), K(table_id_), K(decrypted_len), K(new_pos));
          }
        }
        if (OB_SUCC(ret)) {
          pos += encrypted_len;
        }
      }
    }
  }
  return ret;
}

ObMutatorTableLock::ObMutatorTableLock():
    lock_id_(),
    owner_id_(0),
    mode_(NO_LOCK),
    lock_type_(ObTableLockOpType::UNKNOWN_TYPE),
    create_timestamp_(0),
    create_schema_version_(-1)
{
}

ObMutatorTableLock::ObMutatorTableLock(
    const uint64_t table_id,
    const common::ObStoreRowkey &rowkey,
    const int64_t table_version,
    const ObLockID &lock_id,
    const ObTableLockOwnerID owner_id,
    const ObTableLockMode lock_mode,
    const ObTableLockOpType lock_op_type,
    const transaction::ObTxSEQ seq_no,
    const int64_t create_timestamp,
    const int64_t create_schema_version) :
    ObMutator(table_id, rowkey, table_version, seq_no),
    lock_id_(lock_id),
    owner_id_(owner_id),
    mode_(lock_mode),
    lock_type_(lock_op_type),
    create_timestamp_(create_timestamp),
    create_schema_version_(create_schema_version)
{}

ObMutatorTableLock::~ObMutatorTableLock()
{
  reset();
}

void ObMutatorTableLock::reset()
{
  ObMutator::reset();
  lock_id_.reset();
  owner_id_ = 0;
  mode_ = NO_LOCK;
  lock_type_ = ObTableLockOpType::UNKNOWN_TYPE;
  create_timestamp_ = 0;
  create_schema_version_ = -1;
}

bool ObMutatorTableLock::is_valid() const
{
  return (lock_id_.is_valid() &&
          is_lock_mode_valid(mode_) &&
          is_op_type_valid(lock_type_));
}

int ObMutatorTableLock::copy(ObLockID &lock_id,
                             ObTableLockOwnerID &owner_id,
                             ObTableLockMode &lock_mode,
                             ObTableLockOpType &lock_op_type,
                             transaction::ObTxSEQ &seq_no,
                             int64_t &create_timestamp,
                             int64_t &create_schema_version) const
{
  int ret = OB_SUCCESS;
  lock_id = lock_id_;
  owner_id = owner_id_;
  lock_mode = mode_;
  lock_op_type = lock_type_;
  seq_no = seq_no_;
  create_timestamp = create_timestamp_;
  create_schema_version = create_schema_version_;
  return ret;
}

int ObMutatorTableLock::serialize(
    char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos + encoded_length_i32(0);
  TRANS_LOG(DEBUG, "ObMutatorTableLock::serialize");
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(lock_id_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(owner_id_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(encode_i8(buf, buf_len, new_pos, mode_)) ||
             OB_FAIL(encode_i8(buf, buf_len, new_pos, lock_type_)) ||
             OB_FAIL(seq_no_.serialize(buf, buf_len, new_pos)) ||
             OB_FAIL(encode_vi64(buf, buf_len, new_pos, create_timestamp_)) ||
             OB_FAIL(encode_vi64(buf, buf_len, new_pos, create_schema_version_))) {
    if (OB_BUF_NOT_ENOUGH != ret
        || buf_len > common::OB_MAX_LOG_ALLOWED_SIZE) {
      TRANS_LOG(INFO, "serialize row fail", K(ret), KP(buf),
                K(buf_len), K(pos));
    }
  }

  if (OB_SUCC(ret)) {
    row_size_ = (uint32_t)(new_pos - pos);
    if (OB_FAIL(encode_i32(buf, buf_len, pos, row_size_))) {
      TRANS_LOG(WARN, "serialize row fail", K(ret), K(buf_len), K(pos));
    } else {
      pos = new_pos;
    }
  }
  return ret;
}

int ObMutatorTableLock::deserialize(
    const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(decode_i32(buf, buf_len, new_pos,
                     (int32_t *)&row_size_))) {
    TRANS_LOG(WARN, "deserialize encrypted length fail", K(ret),
              K(buf_len), K(new_pos));
  } else if (pos + row_size_ > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "size overflow", K(ret), KP(buf), K(buf_len),
              K(pos), K_(row_size));
  } else if (OB_FAIL(lock_id_.deserialize(buf, buf_len, new_pos))) {
    TRANS_LOG(WARN, "deserialize lock_id fail", K(ret), K(pos), K(new_pos), K(row_size_), K(buf_len));
  } else if (OB_FAIL(owner_id_.deserialize(buf, buf_len, new_pos))) {
    TRANS_LOG(WARN, "deserialize owner_id fail", K(ret), K(pos), K(new_pos), K(row_size_), K(buf_len));
  } else if (OB_FAIL(decode_i8(buf, buf_len, new_pos, reinterpret_cast<int8_t*>(&mode_)))) {
    TRANS_LOG(WARN, "deserialize lock mode fail", K(ret), K(pos), K(new_pos), K(row_size_), K(buf_len));
  } else if (OB_FAIL(decode_i8(buf, buf_len, new_pos, reinterpret_cast<int8_t*>(&lock_type_)))) {
    TRANS_LOG(WARN, "deserialize lock op type fail", K(ret), K(pos), K(new_pos), K(row_size_), K(buf_len));
  } else if (OB_FAIL(seq_no_.deserialize(buf, buf_len, new_pos))) {
    TRANS_LOG(WARN, "deserialize seq no fail", K(ret));
  } else {
    // do nothing
  }
  if (OB_SUCC(ret) && (new_pos < buf_len)) {
    if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, &create_timestamp_))) {
      TRANS_LOG(WARN, "deserialize create timestamp fail", K(ret));
    }
  }
  if (OB_SUCC(ret) && (new_pos < buf_len)) {
    if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, &create_schema_version_))) {
      TRANS_LOG(WARN, "deserialize create schema fail", K(ret));
    }
  }
  if (OB_SUCC(ret)) {
    pos += row_size_;
  }
  return ret;
}

int ObLsmtMutatorRow::set(const uint64_t table_id,
                          const int64_t table_version,
                          const common::ObTabletID primary_row_id,
                          const blocksstable::ObDmlFlag dml_flag,
                          const obrpc::ObBatchCreateTabletArg * create_arg,
                          const obrpc::ObBatchRemoveTabletArg * remove_arg)
{
  table_id_ = table_id;
  table_version_ = table_version;
  dml_flag_ = dml_flag;
  primary_row_id_ = primary_row_id;
  int ret = OB_SUCCESS;
  if (ObDmlFlag::DF_INSERT == dml_flag){
    if (OB_ISNULL(create_arg)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "get trans node create_arg fail for ObDmlFlag::DF_INSERT", KR(ret), K(dml_flag));
    } else {
      ret = create_arg_.assign(*create_arg);
    }
  } else if (ObDmlFlag::DF_DELETE == dml_flag){
    if (OB_ISNULL(remove_arg)) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "get trans node create_arg fail for ObDmlFlag::DF_DELETE", KR(ret), K(dml_flag));
    } else {
      ret = remove_arg_.assign(*remove_arg);
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "unexpected dml type", KR(ret), K(dml_flag));
  }

  return ret;
}

int ObLsmtMutatorRow::copy( uint64_t &table_id,
                            int64_t &table_version,
                            common::ObTabletID &primary_row_id,
                            blocksstable::ObDmlFlag &dml_flag,
                            obrpc::ObBatchCreateTabletArg &create_arg,
                            obrpc::ObBatchRemoveTabletArg &remove_arg) const
{
  int ret = OB_SUCCESS;
  table_id = table_id_;
  table_version = table_version_;
  dml_flag = dml_flag_;
  primary_row_id = primary_row_id_;
  if (OB_FAIL(create_arg.assign(create_arg_))) {
    TRANS_LOG(WARN, "create_arg.assign failed", KR(ret), K(create_arg), K(dml_flag_));
  } else if (OB_FAIL(remove_arg.assign(remove_arg_))) {
    TRANS_LOG(WARN, "remove_arg.assign failed", KR(ret), K(remove_arg), K(dml_flag_));
  }
  return ret;
}

void ObLsmtMutatorRow::reset()
{
  ObMutator::reset();
  table_id_ = 0;
  dml_flag_ = ObDmlFlag::DF_NOT_EXIST;
  primary_row_id_.reset();
  table_version_ = OB_INVALID_VERSION;
  create_arg_.reset();
  remove_arg_.reset();
}

int ObLsmtMutatorRow::serialize(char *buf, const int64_t buf_len, int64_t &pos,
                                const bool is_big_row)
{
  UNUSED(is_big_row);
  int ret = OB_SUCCESS;
  int64_t new_pos = pos + encoded_length_i32(0);
  int64_t data_pos = new_pos + encoded_length_vi64(table_id_);
  TRANS_LOG(INFO, "ObLsmtMutatorRow::serialize");
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "Invalid param", KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(encode_vi64(buf, buf_len, new_pos, table_id_))) {
    TRANS_LOG(WARN, "serialize table id failed", K(ret), KP(buf),
              K(buf_len), K(pos));
  } else if (  OB_FAIL(encode_vi64(buf, buf_len, new_pos, table_version_))
            || OB_FAIL(encode_vi64(buf, buf_len, new_pos, primary_row_id_.id()))
            || OB_FAIL(encode_i8(buf, buf_len, new_pos, dml_flag_))
            // We will serialize both create_arg_ and remove_arg_ without check dml_type_
            || OB_FAIL(create_arg_.serialize(buf, buf_len, new_pos))
            || OB_FAIL(remove_arg_.serialize(buf, buf_len, new_pos))
            ){
    if (OB_BUF_NOT_ENOUGH != ret || buf_len > common::OB_MAX_LOG_ALLOWED_SIZE) {
      TRANS_LOG(INFO, "serialize row fail", K(ret), KP(buf),
                K(buf_len), K(pos));
    }
  } else {
    TRANS_LOG(DEBUG, "serialize ok", K(dml_flag_), K(pos));
  }

  if (OB_SUCC(ret)) {
    row_size_ = (uint32_t)(new_pos - pos);
    if (OB_FAIL(encode_i32(buf, buf_len, pos, row_size_))) {
      TRANS_LOG(WARN, "ObLsmtMutatorRow::serialize row fail", K(ret), K(buf_len),
                K(pos), K(table_id_));
    } else {
      pos = new_pos;
    }
  }
  return ret;
}

int ObLsmtMutatorRow::deserialize(const char *buf, const int64_t buf_len, int64_t &pos,
                                  const bool is_big_row)
{
  UNUSED(is_big_row);
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t primary_row_id;
  if (OB_ISNULL(buf) || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(decode_i32(buf, buf_len, new_pos,
                     (int32_t *)&row_size_))) {
    TRANS_LOG(WARN, "deserialize encrypted length fail", K(ret),
              K(buf_len), K(new_pos));
  } else if (pos + row_size_ > buf_len) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "size overflow", K(ret), KP(buf), K(buf_len),
              K(pos), K_(row_size));
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, (int64_t *)&table_id_))) {
    TRANS_LOG(WARN, "deserialize table_id_ failed", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, (int64_t *)&table_version_))) {
    TRANS_LOG(WARN, "deserialize table_version_ failed", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(decode_vi64(buf, buf_len, new_pos, (int64_t *)&primary_row_id))) {
    TRANS_LOG(WARN, "deserialize primary_row_id_ failed", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FALSE_IT(primary_row_id_ = primary_row_id)) {
    TRANS_LOG(WARN, "deserialize dml_type_ failed", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(decode_i8(buf, buf_len, new_pos, (int8_t *)&dml_flag_))) {
    TRANS_LOG(WARN, "deserialize dml_type_ failed", K(ret), K(buf_len), K(new_pos));
  } else if (OB_FAIL(create_arg_.deserialize(buf, buf_len, new_pos))){
    TRANS_LOG(WARN, "deserialize create_arg_ fail", KR(ret), K(table_id_), K(pos),
                                                    K(new_pos), K(row_size_), K(buf_len));
  } else if (OB_FAIL(remove_arg_.deserialize(buf, buf_len, new_pos))){
    TRANS_LOG(WARN, "deserialize remove_arg_ fail", KR(ret), K(table_id_), K(pos),
                                                    K(new_pos), K(row_size_), K(buf_len));
  } else {
    TRANS_LOG(DEBUG, "ObLsmtMutatorRow::deserialize ok", K(dml_flag_), K(pos));
  }

  if(OB_SUCC(ret)) {
    pos += row_size_;
  }
  return ret;
}

////////////////////////////////////////////////////////////////////////////////////////////////////
ObMutatorWriter::ObMutatorWriter()
{}

ObMutatorWriter::~ObMutatorWriter()
{}

int ObMutatorWriter::set_buffer(char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  const int64_t meta_size = meta_.get_serialize_size();
  if (OB_ISNULL(buf) || buf_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid argument", K(ret), KP(buf), K(buf_len));
  } else if (buf_len < meta_size) {
    ret = OB_BUF_NOT_ENOUGH;
    TRANS_LOG(WARN, "buf not enough", K(buf_len), K(meta_size));
  } else if (!buf_.set_data(buf, buf_len)) {
    TRANS_LOG(WARN, "set_data fail", KP(buf), K(buf_len));
  } else {
    buf_.get_position() = meta_size;
    row_capacity_ = buf_len;
  }
  return ret;
}

int ObMutatorWriter::append_row_kv(
    const int64_t table_version,
    const RedoDataNode &redo,
    const transaction::ObTxEncryptMeta *encrypt_meta,
    transaction::ObCLogEncryptInfo &encrypt_info,
    const bool is_big_row)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  ObStoreRowkey rowkey;
  const ObMemtableKey *mtk = &redo.key_;
  uint64_t cluster_version = 0;
  bool is_with_head = true;
  if (OB_ISNULL(redo.callback_)) {
    is_with_head = false;
  } else if (OB_FAIL(redo.callback_->get_cluster_version(cluster_version))) {
    TRANS_LOG(WARN, "get cluster version faild.", K(ret));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(mtk)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(ret), K(mtk));
  } else if (OB_FAIL(mtk->decode(rowkey))) {
    TRANS_LOG(WARN, "mtk decode fail", "ret", ret);
  } else if (OB_INVALID_ID == table_id || table_version < 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (!redo.tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tablet_id is unexpected", K(ret), K(redo.tablet_id_));
  } else {
    ObMutatorRowHeader row_header;
    row_header.mutator_type_ = MutatorType::MUTATOR_ROW;
    row_header.tablet_id_ = redo.tablet_id_;
    //TODO: table_id is just used as encrypt_index,
    //      if table_id is no longer used, we may rename it in the future
    ObMemtableMutatorRow row(table_id,
                             rowkey,
                             table_version,
                             redo.new_row_,
                             redo.old_row_,
                             redo.dml_flag_,
                             redo.modify_count_,
                             redo.acc_checksum_,
                             redo.version_,
                             redo.flag_,
                             redo.seq_no_,
                             redo.column_cnt_);
    int64_t tmp_pos = buf_.get_position();
    int64_t row_capacity = row_capacity_;

    if (OB_ISNULL(buf_.get_data())) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "not init", K(ret));
    } else if (is_with_head &&
        OB_FAIL(row_header.serialize(buf_.get_data(), row_capacity, tmp_pos))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        //do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (OB_FAIL(row.serialize(buf_.get_data(), row_capacity, tmp_pos,
                                     encrypt_meta, encrypt_info, is_big_row))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        //do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (OB_FAIL(meta_.inc_row_count())) {
      TRANS_LOG(WARN, "meta inc_row_count failed", K(ret));
    } else {
      buf_.get_position() = tmp_pos;
      row_capacity_ = row_capacity;
    }
  }
  if (OB_SUCCESS != ret && OB_BUF_NOT_ENOUGH != ret) {
    TRANS_LOG(WARN, "append_kv fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int ObMutatorWriter::append_row(
    ObMemtableMutatorRow &row,
    transaction::ObCLogEncryptInfo &encrypt_info,
    const bool is_big_row,
    const bool is_with_head)
{
  int ret = OB_SUCCESS;
  ObMutatorRowHeader row_header;
  row_header.mutator_type_ = MutatorType::MUTATOR_ROW;
  const transaction::ObTxEncryptMeta *encrypt_meta = NULL;
  int64_t buf_len;
  //TODO replace pkey with tablet_id for clog_encrypt_info 
  //row_header.pkey_ = pkey;
  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else {
    int64_t tmp_pos = buf_.get_position();
    if (is_with_head &&
        OB_FAIL(row_header.serialize(buf_.get_data(), buf_.get_capacity(), tmp_pos))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        //do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (FALSE_IT(buf_len = buf_.get_capacity())) {
    } else if (OB_FAIL(row.serialize(buf_.get_data(), buf_len,
                       tmp_pos, encrypt_meta, encrypt_info, is_big_row))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        //do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (OB_FAIL(meta_.inc_row_count())) {
      TRANS_LOG(WARN, "meta inc_row_count failed", K(ret));
    } else {
      buf_.get_position() = tmp_pos;
    }
  }

  return ret;
}

int ObMutatorWriter::append_row_buf(const char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "mutator writer not init", KR(ret));
  } else if (buf_.get_remain() < buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
    TRANS_LOG(WARN, "mutator writer buf not enough", KR(ret));
  } else {
    MEMCPY(buf_.get_data() + buf_.get_position(), buf, buf_len);
    if (OB_FAIL(meta_.inc_row_count())) {
      TRANS_LOG(WARN, "meta inc_row_count failed", K(ret));
    } else {
      buf_.get_position() = buf_.get_position() + buf_len;
    }
  }

  return ret;
}

int ObMutatorWriter::append_table_lock_kv(
    const int64_t table_version,
    const TableLockRedoDataNode &redo)
{
  int ret = OB_SUCCESS;
  uint64_t table_id = 0;
  ObStoreRowkey rowkey;
  const ObMemtableKey *mtk = &redo.key_;
  if (OB_ISNULL(mtk)) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(ret), K(mtk));
  } else if (OB_FAIL(mtk->decode(rowkey))) {
    TRANS_LOG(WARN, "mtk decode fail", "ret", ret);
  } else if (OB_INVALID_ID == table_id || table_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    TRANS_LOG(WARN, "invalid_argument", K(ret), K(table_id), K(table_version));
  } else if (!redo.tablet_id_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "tablet_id is unexpected", K(ret), K(redo.tablet_id_));
  } else {
    ObMutatorRowHeader row_header;
    row_header.mutator_type_ = MutatorType::MUTATOR_TABLE_LOCK;
    row_header.tablet_id_ = redo.tablet_id_;
    ObMutatorTableLock table_lock(table_id,
                                  rowkey,
                                  table_version,
                                  redo.lock_id_,
                                  redo.owner_id_,
                                  redo.lock_mode_,
                                  redo.lock_op_type_,
                                  redo.seq_no_,
                                  redo.create_timestamp_,
                                  redo.create_schema_version_);
    int64_t tmp_pos = buf_.get_position();
    int64_t row_capacity = row_capacity_;
    if (OB_ISNULL(buf_.get_data())) {
      ret = OB_NOT_INIT;
      TRANS_LOG(WARN, "not init", K(ret));
    } else if (OB_FAIL(row_header.serialize(buf_.get_data(),
                                            row_capacity,
                                            tmp_pos))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        //do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (!table_lock.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "mutator tablelock is invalid", K(ret), K(table_lock), KP(redo.callback_));
    } else if (OB_FAIL(table_lock.serialize(buf_.get_data(),
                                            row_capacity,
                                            tmp_pos))) {
      if (ret == OB_ALLOCATE_MEMORY_FAILED) {
        //do nothing
      } else {
        ret = OB_BUF_NOT_ENOUGH;
      }
    } else if (OB_FAIL(meta_.inc_row_count())) {
    } else {
      buf_.get_position() = tmp_pos;
    }
  }
  if (OB_SUCCESS != ret && OB_BUF_NOT_ENOUGH != ret) {
    TRANS_LOG(WARN, "append_kv fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int ObMutatorWriter::serialize(const uint8_t row_flag, int64_t &res_len,
                               transaction::ObCLogEncryptInfo &encrypt_info)
{
  int ret = OB_SUCCESS;
  const int64_t meta_size = meta_.get_serialize_size();
  int64_t meta_pos = 0;
  int64_t end_pos = buf_.get_position();
  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret));
  } else if (0 >= meta_.get_row_count()) {
    TRANS_LOG(DEBUG, "no row exist");
    ret = OB_ENTRY_NOT_EXIST;
  } else if (!ObTransRowFlag::is_valid_row_flag(row_flag)) {
    TRANS_LOG(WARN, "invalid argument", K(row_flag));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(meta_.set_flags(row_flag))) {
    TRANS_LOG(WARN, "set flags error", K(ret), K(row_flag));
  } else if (OB_FAIL(meta_.fill_header(buf_.get_data() + meta_size,
                                       buf_.get_position() - meta_size))) {
  } else if (OB_FAIL(meta_.serialize(buf_.get_data(), meta_size, meta_pos))) {
#ifdef OB_BUILD_TDE_SECURITY
  } else if (((row_flag & ObTransRowFlag::ENCRYPT) > 0) &&
               OB_FAIL(encrypt_info.serialize(buf_.get_data(), buf_.get_capacity(), end_pos))) {
    TRANS_LOG(WARN, "serialize clog encrypt info failed", K(ret), K(buf_.get_capacity()), K(end_pos));
#endif
  } else {
    buf_.get_position() = end_pos;
    res_len = buf_.get_position();
  }
  if (OB_FAIL(ret) && OB_ENTRY_NOT_EXIST != ret) {
    TRANS_LOG(WARN, "serialize fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int64_t ObMutatorWriter::get_serialize_size() const
{
  //@FIXME shanyan.g
  const int64_t SIZE = 2 * OB_MAX_ROW_LENGTH_IN_MEMTABLE;
  return SIZE;
}

#ifdef OB_BUILD_TDE_SECURITY
int ObMutatorWriter::encrypt_big_row_data(
    const char *in_buf, const int64_t in_buf_len, int64_t &in_buf_pos,
    char *out_buf, const int64_t out_buf_len, int64_t &out_buf_pos,
    const int64_t table_id, const transaction::ObCLogEncryptInfo &encrypt_info,
    bool &need_encrypt)
{
  int ret = OB_SUCCESS;
  transaction::ObTxEncryptMeta *encrypt_meta = NULL;
  need_encrypt = false;
  if (!encrypt_info.is_valid() || OB_ISNULL(in_buf) || in_buf_len < 0 || in_buf_pos > in_buf_len
      || OB_ISNULL(out_buf) || out_buf_len < 0 || out_buf_pos > out_buf_len) {
    TRANS_LOG(WARN, "invalid argument", KP(in_buf), K(in_buf_len), K(in_buf_pos), K(table_id));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(encrypt_info.get_encrypt_info(table_id, encrypt_meta))) {
    TRANS_LOG(ERROR, "failed to get encrypt info", K(ret), K(table_id));
  } else if (need_encrypt) {
    if (OB_FAIL(serialization::encode_vi64(out_buf, out_buf_len, out_buf_pos, table_id))) {
      TRANS_LOG(WARN, "failed to encode table id", K(ret));
    } else {
      int64_t encrypted_len = 0;
      ObCipherOpMode opmode = static_cast<ObCipherOpMode>(encrypt_meta->meta_.encrypt_algorithm_);
      int64_t data_len = ObEncryptionUtil::decrypted_length(opmode, out_buf_len - out_buf_pos);
      data_len = min(data_len, in_buf_len - in_buf_pos);
      if (OB_UNLIKELY(data_len < 0)) {
        TRANS_LOG(WARN, "buf to short to hold encrypted data",
                  K(out_buf_len), K(out_buf_pos), K(in_buf_len), K(in_buf_pos));
        ret = OB_BUF_NOT_ENOUGH;
      } else if (OB_FAIL(ObEncryptionUtil::encrypt_data(encrypt_meta->meta_,
                  in_buf + in_buf_pos, data_len,
                  out_buf + out_buf_pos, out_buf_len - out_buf_pos,
                  encrypted_len))) {
        TRANS_LOG(WARN, "failed to encrypt big row data", K(ret));
      } else {
        in_buf_pos += data_len;
        out_buf_pos += encrypted_len;
      }
    }
    if (EXECUTE_COUNT_PER_SEC(1)) {
      TRANS_LOG(INFO, "encrypt big row data", K(table_id), K(ret));
    }
  } else {
    int64_t data_len = min(in_buf_len - in_buf_pos, out_buf_len - out_buf_pos);
    MEMCPY(out_buf + out_buf_pos, in_buf + in_buf_pos, data_len);
    in_buf_pos += data_len;
    out_buf_pos += data_len;
  }
  return ret;
}
#endif

////////////////////////////////////////////////////////////////////////////////////////////////////
ObMemtableMutatorIterator::ObMemtableMutatorIterator()
{
  // big_row_ = false;
  reset();
}

ObMemtableMutatorIterator::~ObMemtableMutatorIterator()
{
  reset();
}

// If leader switch happened before the last log entry of lob row is successfully written,
// there may be memory leak on follower's mutator buf. reset function need to provide some
// basic bottom-line operations.
void ObMemtableMutatorIterator::reset()
{
  // meta_.reset();
  buf_.reset();
  row_header_.reset();
  row_.reset();
  table_lock_.reset();
}

int ObMemtableMutatorIterator::deserialize(const char *buf, const int64_t data_len, int64_t &pos,
    transaction::ObCLogEncryptInfo &encrypt_info)
{
  int ret = OB_SUCCESS;
  int64_t data_pos = pos;
  int64_t end_pos = pos;

  if (OB_ISNULL(buf) || (data_len - pos) <= 0 || data_len < 0) {
    TRANS_LOG(WARN, "invalid argument", KP(buf), K(data_len), K(pos));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_FAIL(meta_.deserialize(buf, data_len, data_pos))) {
    TRANS_LOG(WARN, "decode meta fail", K(ret), KP(buf), K(data_len), K(data_pos));
    ret = (OB_SUCCESS == ret) ? OB_INVALID_DATA : ret;
  } else if (!buf_.set_data(const_cast<char *>(buf + pos), meta_.get_total_size())) {
    TRANS_LOG(WARN, "set_data fail", KP(buf), K(pos), K(meta_.get_total_size()));
  } else if (FALSE_IT(end_pos += meta_.get_total_size())) {
#ifdef OB_BUILD_TDE_SECURITY
  } else if (((meta_.get_flags() & ObTransRowFlag::ENCRYPT) > 0) &&
             (OB_FAIL(encrypt_info.deserialize(buf, data_len, end_pos)))) {
    TRANS_LOG(WARN, "decode clog encrypt info fail", K(ret), KP(buf), K(data_len), K(end_pos));
#endif
  } else {
    pos = end_pos;
    buf_.get_limit() = meta_.get_total_size();
    buf_.get_position() = meta_.get_meta_size();
  }
  if (OB_FAIL(ret)) {
    TRANS_LOG(WARN, "deserialize fail", K(ret), K(buf_), K(meta_));
  }
  return ret;
}

int ObMemtableMutatorIterator::iterate_next_row(ObEncryptRowBuf &decrypt_buf,
    const transaction::ObCLogEncryptInfo &encrypt_info)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf_.get_data())) {
    ret = OB_NOT_INIT;
    TRANS_LOG(WARN, "not init", K(ret), K(buf_));
  } else if (buf_.get_remain_data_len() <= 0) {
    ret = OB_ITER_END;
  } else if (OB_FAIL(
                 row_header_.deserialize(buf_.get_data(), buf_.get_limit(), buf_.get_position()))) {
    TRANS_LOG(WARN, "deserialize mutator row head fail", K(ret), K(buf_), K(meta_));
  } else {
    switch (row_header_.mutator_type_) {
    case MutatorType::MUTATOR_ROW: {
      row_.reset();
      const bool unused_need_extract_encrypt_meta = false;
      ObCLogEncryptStatMap unused_encrypt_stat_map;
      ObEncryptMeta encrypt_meta;
      if (OB_FAIL(row_.deserialize(
              buf_.get_data(), buf_.get_limit(), buf_.get_position(), decrypt_buf,
              encrypt_info, unused_need_extract_encrypt_meta, encrypt_meta,
              unused_encrypt_stat_map, ObTransRowFlag::is_big_row(meta_.get_flags())))) {
        TRANS_LOG(WARN, "deserialize mutator row fail", K(ret));
      }
      break;
    }
    case MutatorType::MUTATOR_TABLE_LOCK: {
      table_lock_.reset();
      if (OB_FAIL(
              table_lock_.deserialize(buf_.get_data(), buf_.get_limit(), buf_.get_position()))) {
        TRANS_LOG(WARN, "deserialize table lock fail", K(ret));
      }
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "Unknown mutator_type", K(ret),K(row_header_.mutator_type_),K(buf_),K(meta_));
      break;
    }
    }
  }

  if (OB_SUCCESS != ret && OB_ITER_END != ret) {
    TRANS_LOG(WARN, "iterate_next_row failed", K(ret), K(meta_), K(row_header_));
  }
  return ret;
}

const ObMutatorRowHeader &ObMemtableMutatorIterator::get_row_head() { return row_header_; }

const ObMemtableMutatorRow &ObMemtableMutatorIterator::get_mutator_row() { return row_; }

const ObMutatorTableLock &ObMemtableMutatorIterator::get_table_lock_row() { return table_lock_; }


}//namespace memtable
}//namespace oceanbase

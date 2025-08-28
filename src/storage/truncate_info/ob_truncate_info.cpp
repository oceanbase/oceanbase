//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#define USING_LOG_PREFIX STORAGE
#include "storage/truncate_info/ob_truncate_info.h"
#include "storage/tablet/ob_tablet_obj_load_helper.h"
#include "storage/multi_data_source/adapter_define/mds_dump_node.h"
#include "share/truncate_info/ob_truncate_info_util.h"
#include "storage/multi_data_source/mds_key_serialize_util.h"
#include "share/compaction/ob_compaction_info_param.h"
namespace oceanbase
{
using namespace share;
using namespace share::schema;
using namespace mds;
namespace storage
{
OB_SERIALIZE_MEMBER_SIMPLE(
    ObTruncateInfoKey,
    tx_id_,
    inc_seq_);

int ObTruncateInfoKey::mds_serialize(char *buf, const int64_t buf_len,
                                     int64_t &pos) const {
  int ret = OB_SUCCESS;
  if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    buf[pos++] = MAGIC_NUMBER;
    if (OB_FAIL(ObMdsSerializeUtil::mds_key_serialize(tx_id_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize tx_id", KR(ret), K_(tx_id));
    } else if (OB_FAIL(ObMdsSerializeUtil::mds_key_serialize(
                   inc_seq_, buf, buf_len, pos))) {
      LOG_WARN("failed to serialize inc_seq", KR(ret), K_(inc_seq));
    }
  }
  return ret;
}
int ObTruncateInfoKey::mds_deserialize(const char *buf, const int64_t buf_len,
                                       int64_t &pos) {
  int ret = OB_SUCCESS;
  int64_t tmp = 0;
  uint8_t magic_number = 0;
  if (pos >= buf_len) {
    ret = OB_BUF_NOT_ENOUGH;
  } else {
    magic_number = buf[pos++];
    if (magic_number != MAGIC_NUMBER) {
      ob_abort(); // compat case, just abort for fast fail
    } else if (OB_FAIL(ObMdsSerializeUtil::mds_key_deserialize(buf, buf_len, pos, tmp))) {
      LOG_WARN("failed to serialize tx_id", KR(ret));
    } else if (FALSE_IT(tx_id_ = tmp)) {
    } else if (OB_FAIL(ObMdsSerializeUtil::mds_key_deserialize(buf, buf_len, pos, tmp))) {
      LOG_WARN("failed to serialize inc_seq", KR(ret));
    } else {
      inc_seq_ = tmp;
    }
  }
  return ret;
}

int64_t ObTruncateInfoKey::mds_get_serialize_size() const
{
  return sizeof(MAGIC_NUMBER)
    + ObMdsSerializeUtil::mds_key_get_serialize_size(tx_id_)
    + ObMdsSerializeUtil::mds_key_get_serialize_size(inc_seq_);
}

void ObTruncateInfoKey::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_KV(K_(tx_id), K_(inc_seq));
  }
}

/*
* ObStorageListRowValues
* */
ObStorageListRowValues::~ObStorageListRowValues()
{
  if (cnt_ > 0 || nullptr != values_) {
    LOG_ERROR_RET(OB_ERR_SYS, "exist unfree row", KP_(values), K_(cnt));
  }
}

void ObStorageListRowValues::destroy(ObIAllocator &allocator)
{
  if (OB_NOT_NULL(values_)) {
    for (int64_t idx = 0; idx < cnt_; ++idx) {
      values_[idx].reset();
    }
    allocator.free(values_);
    values_ = nullptr;
    cnt_ = 0;
  }
}

int ObStorageListRowValues::init(
    ObIAllocator &allocator,
    const common::ObIArray<common::ObNewRow> &row_values)
{
  int ret = OB_SUCCESS;
  void *alloc_buf = nullptr;
  int64_t alloc_buf_size = 0;
  if (OB_UNLIKELY(nullptr != values_ || cnt_ > 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("exist unfree ptr, should not init", KR(ret), KP_(values), K_(cnt));
  } else if (0 == (cnt_ = row_values.count())) {
  } else {
    alloc_buf_size += cnt_ * sizeof(ObNewRow);
    for (int64_t i = 0; i < cnt_; i++) {
      alloc_buf_size += row_values.at(i).get_deep_copy_size();
    } // for
    if (OB_ISNULL(alloc_buf = allocator.alloc(alloc_buf_size))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc memory", KR(ret), K(cnt_));
    } else {
      values_ = new (alloc_buf) ObNewRow[cnt_];
      int64_t pos = cnt_ * sizeof(ObNewRow);
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
        if (OB_FAIL(values_[i].deep_copy(row_values.at(i), (char *)alloc_buf, alloc_buf_size, pos))) {
          LOG_WARN("Fail to deep copy row", K(ret), K(i), K(row_values.at(i)));
        }
      } // for
    }
    if (OB_FAIL(ret)) {
      destroy(allocator);
    }
  }
  return ret;
}

int ObStorageListRowValues::assign(ObIAllocator &allocator, const ObStorageListRowValues &other)
{
  int ret = OB_SUCCESS;
  int64_t deep_copy_size = 0;
  void *alloc_buf = nullptr;
  if (OB_UNLIKELY(nullptr != values_ || cnt_ > 0)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("exist unfree ptr, should not init", KR(ret), KP_(values), K_(cnt));
  } else if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (0 == (cnt_ = other.count())) {
  } else if (FALSE_IT(deep_copy_size = other.get_deep_copy_size())) {
  } else if (OB_ISNULL(alloc_buf = allocator.alloc(deep_copy_size))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc memory", KR(ret), K(cnt_));
  } else {
    values_ = new (alloc_buf) ObNewRow[cnt_];
    int64_t pos = cnt_ * sizeof(ObNewRow);
    const ObNewRow *other_values = other.get_values();
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      if (OB_FAIL(values_[i].deep_copy(other_values[i], (char *)alloc_buf, deep_copy_size, pos))) {
        LOG_WARN("Fail to deep copy row", K(ret), K(i), K(other_values[i]));
      }
    } // for
    if (OB_FAIL(ret)) {
      destroy(allocator);
    }
  }
  return ret;
}

int ObStorageListRowValues::compare(const ObStorageListRowValues &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = (this == &other);
  if (equal) {
  } else if (OB_UNLIKELY(!is_valid() || !other.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to compare", KR(ret), KPC(this), K(other));
  } else {
    equal = (cnt_ == other.cnt_);
    for (int64_t i = 0; OB_SUCC(ret) && equal && i < cnt_; i++) {
      int cmp = 0;
      equal = (values_[i].count_ == other.values_[i].count_);
      for (int64_t j = 0; OB_SUCC(ret) && equal && j < values_[i].count_; ++j) {
        if (OB_FAIL(values_[i].get_cell(j).compare(other.values_[i].get_cell(j), cmp))) {
          LOG_WARN("failed to compare object", KR(ret), K(i), K(j), K(values_[i]), K(other.values_[i]));
        } else {
          equal = (0 == cmp);
        }
      }
    } // for
  }
  return ret;
}

int ObStorageListRowValues::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to serialize", KR(ret), KPC(this));
  } else if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, cnt_))) {
    LOG_WARN("fail to encode count", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i ++) {
    if (OB_FAIL(values_[i].serialize(buf, buf_len, pos))) {
      LOG_WARN("fail to encode row", K(ret));
    }
  }
  return ret;
}

int64_t ObStorageListRowValues::get_serialize_size() const
{
  int64_t len = 0;
  len += serialization::encoded_length_vi64(cnt_);
  for (int64_t i = 0; i < cnt_; i ++) {
    len += values_[i].get_serialize_size();
  }
  return len;
}

int ObStorageListRowValues::deserialize(ObIAllocator &allocator, const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator tmp_allocator("ListRowVal");
  ObObj *obj_array = NULL;
  void *tmp_buf = NULL;
  const int obj_capacity = 1024;
  int64_t tmp_cnt = 0;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &tmp_cnt))) {
    LOG_WARN("fail to decode vi64", K(ret));
  } else if (0 == tmp_cnt) {
    cnt_ = 0;
  } else if (OB_ISNULL(tmp_buf = tmp_allocator.alloc(sizeof(ObObj) * obj_capacity))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buf", KR(ret));
  } else {
    ObSEArray<ObNewRow, 4> deseralize_row_array;
    obj_array = new (tmp_buf) ObObj[obj_capacity];
    ObNewRow tmp_row;
    tmp_row.assign(obj_array, obj_capacity);
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_cnt; i++) {
      ObNewRow dest_row;
      tmp_row.count_ = obj_capacity;
      if (OB_FAIL(tmp_row.deserialize(buf, data_len, pos))) {
        LOG_WARN("fail to deserialize row", K(ret));
      } else if (OB_FAIL(ob_write_row(tmp_allocator, tmp_row, dest_row))) {
        LOG_WARN("fail to copy row", K(ret), K(tmp_row));
      } else if (OB_FAIL(deseralize_row_array.push_back(dest_row))) {
        LOG_WARN("Fail to deep copy row", K(ret), K(i), K(dest_row));
      }
    } // for
    if (FAILEDx(init(allocator, deseralize_row_array))) {
      LOG_WARN("failed to init from deserialize row array", KR(ret), K(deseralize_row_array));
    }
  }
  if (OB_FAIL(ret)) {
    destroy(allocator);
  }
  return ret;
}

int64_t ObStorageListRowValues::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  J_OBJ_START();
  if (0 == cnt_) {
    J_KV(K_(cnt));
  } else {
    J_KV(K_(cnt), "values", ObArrayWrap<ObNewRow>(values_, cnt_));
  }
  J_OBJ_END();
  return pos;
}

int64_t ObStorageListRowValues::get_deep_copy_size() const
{
  int64_t size = 0;
  if (is_valid()) {
    size += cnt_ * sizeof(ObNewRow);
    for (int64_t idx = 0; idx < cnt_; ++idx) {
      size += values_[idx].get_deep_copy_size();
    }
  }
  return size;
}

int ObStorageListRowValues::deep_copy(
    char *buf,
    const int64_t buf_len,
    int64_t &pos,
    ObStorageListRowValues &dest) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to deep copy", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(pos + get_deep_copy_size() > buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deep copy buf is invalid", KR(ret), K(cnt_), K(get_deep_copy_size()), K(pos), K(buf_len));
  } else {
    dest.values_ = new (buf + pos) ObNewRow[cnt_];
    dest.cnt_ = cnt_;
    pos += cnt_ * sizeof(ObNewRow);
    for (int64_t i = 0; OB_SUCC(ret) && i < cnt_; i++) {
      if (OB_FAIL(dest.values_[i].deep_copy(values_[i], buf, buf_len, pos))) {
        LOG_WARN("Fail to deep copy row", K(ret), K(i), K(values_[i]));
      }
    } // for
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(!dest.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deep copy dest is invalid", KR(ret), KPC(this));
    }
  }
  return ret;
}

/*
* ObPartKeyIdxArray
* */
ObPartKeyIdxArray::~ObPartKeyIdxArray()
{
  if (OB_NOT_NULL(col_idxs_)) {
    LOG_ERROR_RET(OB_ERR_SYS, "exist unfree row", K_(col_idxs), K_(cnt));
  }
}

void ObPartKeyIdxArray::destroy(common::ObIAllocator &allocator)
{
  if (OB_NOT_NULL(col_idxs_)) {
    allocator.free(col_idxs_);
    col_idxs_ = nullptr;
  }
  cnt_ = 0;
}

bool ObPartKeyIdxArray::is_valid() const
{
  return cnt_ > 0 && nullptr != col_idxs_;
}

int ObPartKeyIdxArray::init(
    common::ObIAllocator &allocator,
    const ObIArray<int64_t> &part_key_idxs)
{
  return inner_init(allocator, part_key_idxs.count(), part_key_idxs.get_data());
}

int ObPartKeyIdxArray::assign(common::ObIAllocator &allocator, const ObPartKeyIdxArray &other)
{
  return inner_init(allocator, other.count(), other.col_idxs_);
}

int ObPartKeyIdxArray::inner_init(
     common::ObIAllocator &allocator,
     const int64_t count,
     const int64_t *data)
{
  int ret = OB_SUCCESS;
  void *buf = nullptr;
  if (OB_UNLIKELY(count <= 0 || nullptr == data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(count), KP(data));
  } else if (OB_UNLIKELY(cnt_ > 0 || nullptr != col_idxs_)) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data", KR(ret), K(cnt_), KP(col_idxs_));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(int64_t) * count))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc part key array", KR(ret), K(count));
  } else {
    MEMCPY(buf, data, sizeof(int64_t) * count);
    col_idxs_ = static_cast<int64_t *>(buf);
    cnt_ = count;
  }
  return ret;
}

int ObPartKeyIdxArray::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid to serialize", KR(ret));
  } else {
    OB_UNIS_ENCODE_ARRAY(col_idxs_, cnt_);
  }
  return ret;
}

int ObPartKeyIdxArray::deserialize(
      common::ObIAllocator &allocator,
      const char *buf,
      const int64_t data_len,
      int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t tmp_count = 0;
  int64_t *tmp_buf = nullptr;
  LST_DO_CODE(OB_UNIS_DECODE, tmp_count);
  if (OB_FAIL(ret)) {
    LOG_WARN("failed to deserialize count", KR(ret), K(data_len), K(pos));
  } else if (OB_UNLIKELY(tmp_count <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid count", KR(ret), K(tmp_count));
  } else if (OB_ISNULL(tmp_buf = static_cast<int64_t *>(allocator.alloc(sizeof(int64_t) * tmp_count)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc part key array", KR(ret), K(tmp_count));
  } else {
    OB_UNIS_DECODE_ARRAY(tmp_buf, tmp_count);
    if (OB_SUCC(ret)) {
      col_idxs_ = tmp_buf;
      cnt_ = tmp_count;
    } else {
      allocator.free(tmp_buf);
    }
  }
  return ret;
}

int64_t ObPartKeyIdxArray::get_serialize_size() const
{
  int64_t len = 0;
  OB_UNIS_ADD_LEN_ARRAY(col_idxs_, cnt_);
  return len;
}

int64_t ObPartKeyIdxArray::get_deep_copy_size() const
{
  return sizeof(int64_t) * cnt_;
}

int ObPartKeyIdxArray::deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObPartKeyIdxArray &dest) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to deep copy", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(pos + deep_copy_size > buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deep copy buf is invalid", KR(ret), K(deep_copy_size), K(pos), K(buf_len));
  } else {
    dest.col_idxs_ = (int64_t *)(buf + pos);
    dest.cnt_ = cnt_;
    MEMCPY(dest.col_idxs_, col_idxs_, deep_copy_size);
    pos += deep_copy_size;
  }
  return ret;
}

int ObPartKeyIdxArray::compare(const ObPartKeyIdxArray &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = false;
  if (OB_UNLIKELY(!is_valid() || !other.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to compare", KR(ret), KPC(this), K(other));
  } else if (cnt_ == other.cnt_) {
    equal = true;
    for (int64_t idx = 0; idx < cnt_ && equal; ++idx) {
      equal = (col_idxs_[idx] == other.col_idxs_[idx]);
    } // for
  }
  return ret;
}

/*
* ObTruncatePartition
* */
const static char * ObPartTypeStr[] = {
    "RANGE",
    "RANGE_COLUMNS",
    "LIST",
    "LIST_COLUMNS",
    "INVALID_PART_TYPE"
};
const static char * ObPartOpStr[] = {
    "INCLUDING",
    "EXCEPT",
    "ALL",
    "PART_OP_MAX"
};
const char *ObTruncatePartition::part_type_to_str(const TruncatePartType &type)
{
  STATIC_ASSERT(static_cast<int64_t>(PART_TYPE_MAX + 1) == ARRAYSIZEOF(ObPartTypeStr), "part type str len is mismatch");
  const char *str = "";
  if (is_valid_part_type(type)) {
    str = ObPartTypeStr[type];
  } else {
    str = "INVALID_PART_TYPE";
  }
  return str;
}

const char *ObTruncatePartition::part_op_to_str(const TruncatePartOp &op)
{
  STATIC_ASSERT(static_cast<int64_t>(PART_OP_MAX + 1) == ARRAYSIZEOF(ObPartOpStr), "part op str len is mismatch");
  const char *str = "";
  if (op >= INCLUDE && op < PART_OP_MAX) {
    str = ObPartOpStr[op];
  } else {
    str = "INVALID_PART_OP";
  }
  return str;
}

bool ObTruncatePartition::is_valid() const
{
  bool bret = false;
  int64_t part_key_cnt = 0;
  if (OB_UNLIKELY(part_op_ < INCLUDE && part_op_ >= PART_OP_MAX)) {
    LOG_WARN_RET(OB_INVALID_DATA, "invalid part_op", K_(part_op));
  } else if (OB_UNLIKELY(!part_key_idxs_.is_valid())) {
    LOG_WARN_RET(OB_INVALID_DATA, "invalid part_key_idxs", K_(part_key_idxs));
  } else if (FALSE_IT(part_key_cnt = part_key_idxs_.count())) {
  } else if (is_range_part(part_type_)) {
    if (low_bound_val_.is_valid() && high_bound_val_.is_valid()) {
      bret = (low_bound_val_.is_min_row() || part_key_cnt == low_bound_val_.length())
        && (high_bound_val_.is_max_row() || part_key_cnt == high_bound_val_.length());
    }
    if (!bret) {
      LOG_WARN_RET(OB_INVALID_DATA, "invalid range bound", K_(part_type), K_(low_bound_val), K_(high_bound_val));
    }
  } else if (is_list_part(part_type_)) {
    if (list_row_values_.is_valid()) {
      bret = true;
      const ObNewRow *values = list_row_values_.get_values();
      for (int64_t idx = 0; idx < list_row_values_.count() && bret; ++idx) {
        bret = part_key_cnt == values[idx].get_count();
      } // for
    } else {
      bret = false;
    }
    if (!bret) {
      LOG_WARN_RET(OB_INVALID_DATA, "invalid list values", K_(part_type), K_(list_row_values));
    }
  } else {
    LOG_WARN_RET(OB_INVALID_DATA, "invalid part_type", K_(part_type));
  }
  return bret;
}

int ObTruncatePartition::init_range_part(
    ObIAllocator &allocator,
    const TruncatePartType part_type,
    const ObRowkey &low_bound_val,
    const ObRowkey &high_bound_val)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_range_part(part_type))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part type", KR(ret), K(part_type));
  } else if (OB_FAIL(low_bound_val.deep_copy(low_bound_val_/*dst*/, allocator))) {
    LOG_WARN("failed to deep copy low bound val", KR(ret), K(low_bound_val));
  } else if (OB_FAIL(high_bound_val.deep_copy(high_bound_val_/*dst*/, allocator))) {
    LOG_WARN("failed to deep copy high bound val", KR(ret), K(high_bound_val));
  } else {
    part_type_ = part_type;
    part_op_ = INCLUDE;
  }
  if (OB_FAIL(ret)) {
    destroy(allocator);
  }
  return ret;
}

int ObTruncatePartition::init_list_part(
    ObIAllocator &allocator,
    const TruncatePartType part_type,
    const TruncatePartOp part_op,
    const ObListRowValues &list_row_values)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_list_part(part_type) || !is_valid_part_op(part_op))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid part type or op", KR(ret), K(part_type), K(part_op));
  } else if (OB_FAIL(list_row_values_.init(allocator, list_row_values.get_values()))) {
    LOG_WARN("failed to init list row values", KR(ret), K(list_row_values));
  } else {
    part_type_ = part_type;
    part_op_ = part_op;
  }
  if (OB_FAIL(ret)) {
    destroy(allocator);
  }
  return ret;
}

int ObTruncatePartition::assign(ObIAllocator &allocator, const ObTruncatePartition &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (OB_FAIL(part_key_idxs_.assign(allocator, other.part_key_idxs_))) {
    LOG_WARN("failed to assign part key idxs", KR(ret), K(other.part_key_idxs_));
  } else if (is_range_part(other.part_type_)) {
    if (OB_FAIL(other.low_bound_val_.deep_copy(low_bound_val_/*dst*/, allocator))) {
      LOG_WARN("failed to assign low bound val", KR(ret), K(other));
    } else if (OB_FAIL(other.high_bound_val_.deep_copy(high_bound_val_/*dst*/, allocator))) {
      LOG_WARN("failed to assign hight bound val", KR(ret), K(other));
    }
  } else if (is_list_part(other.part_type_)) {
    if (OB_FAIL(list_row_values_.assign(allocator, other.list_row_values_))) {
      LOG_WARN("fail to assign row value", K(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part type", KR(ret), K(other));
  }
  if (OB_SUCC(ret)) {
    part_type_ = other.part_type_;
    part_op_ = other.part_op_;
  } else {
    destroy(allocator);
  }
  return ret;
}

int ObTruncatePartition::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected truncate part to serialize", KR(ret), KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ENCODE,
                part_type_, part_op_, part_key_idxs_);
    if (OB_FAIL(ret)) {
    } else if (is_range_part(part_type_)) {
      LST_DO_CODE(OB_UNIS_ENCODE, low_bound_val_, high_bound_val_);
    } else if (is_list_part(part_type_)) {
      LST_DO_CODE(OB_UNIS_ENCODE, list_row_values_);
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected part type", KR(ret), KPC(this));
    }
  }
  return ret;
}

int64_t ObTruncatePartition::get_serialize_size() const
{
  int64_t len = 0;
  if (OB_UNLIKELY(!is_valid())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Unexpected truncate info to serialize", KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN, part_type_, part_op_, part_key_idxs_);
    if (is_range_part(part_type_)) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, low_bound_val_, high_bound_val_);
    } else if (is_list_part(part_type_)) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, list_row_values_);
    }
  }
  return len;
}

int ObTruncatePartition::deserialize(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, part_type_, part_op_);
  if (FAILEDx(part_key_idxs_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize part_key_idxs", KR(ret));
  } else if (is_range_part(part_type_)) {
    if (OB_FAIL(low_bound_val_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize low bould val", KR(ret));
    } else if (OB_FAIL(high_bound_val_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize high bould val", KR(ret));
    }
  } else if (is_list_part(part_type_)) {
    if (OB_FAIL(list_row_values_.deserialize(allocator, buf, data_len, pos))) {
      LOG_WARN("failed to deserialize list row val", KR(ret));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part type", KR(ret), K(part_type_));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("deserialized truncate part is invalid", KR(ret), KPC(this));
  }
  if (OB_FAIL(ret)) {
    destroy(allocator);
  }
  return ret;
}

int64_t ObTruncatePartition::get_deep_copy_size() const
{
  int64_t size = part_key_idxs_.get_deep_copy_size();
  if (is_range_part(part_type_)) {
    size += (low_bound_val_.get_deep_copy_size() + high_bound_val_.get_deep_copy_size());
  } else if (is_list_part(part_type_)) {
    size += list_row_values_.get_deep_copy_size();
  } else {
    LOG_WARN_RET(OB_ERR_UNEXPECTED, "unexpected part type", KR(ret), K(part_type_));
  }
  return size;
}

int ObTruncatePartition::deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObTruncatePartition &dest) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to deep copy", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(pos + deep_copy_size > buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deep copy buf is invalid", KR(ret), K(deep_copy_size), K(pos), K(buf_len));
  } else if (OB_FAIL(part_key_idxs_.deep_copy(buf, buf_len, pos, dest.part_key_idxs_))) {
    LOG_WARN("failed to deep copy part_key_idxs", KR(ret), K_(part_key_idxs), K(deep_copy_size), K(pos), K(buf_len));
  } else if (is_range_part(part_type_)) {
    if (OB_FAIL(dest.low_bound_val_.deep_copy(low_bound_val_, buf, buf_len, pos))) {
      LOG_WARN("failed to deep copy low_bound_val", KR(ret), K_(low_bound_val), K(deep_copy_size), K(pos), K(buf_len));
    } else if (OB_FAIL(dest.high_bound_val_.deep_copy(high_bound_val_, buf, buf_len, pos))) {
      LOG_WARN("failed to deep copy high_bound_val", KR(ret), K_(high_bound_val), K(deep_copy_size), K(pos), K(buf_len));
    }
  } else if (is_list_part(part_type_)) {
    if (OB_FAIL(list_row_values_.deep_copy(buf, buf_len, pos, dest.list_row_values_))) {
      LOG_WARN("failed to deep copy list_row_values", KR(ret), K_(list_row_values), K(deep_copy_size), K(pos), K(buf_len));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part type", KR(ret), K(part_type_));
  }
  if (OB_SUCC(ret)) {
    dest.part_type_ = part_type_;
    dest.part_op_ = part_op_;
  } else {
    dest.reset();
  }
  return ret;
}

int ObTruncatePartition::shallow_copy(ObTruncatePartition &dest)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to deep copy", KR(ret), KPC(this));
  } else if (is_range_part(part_type_)) {
    dest.low_bound_val_.assign(low_bound_val_.get_obj_ptr(), low_bound_val_.get_obj_cnt());
    dest.high_bound_val_.assign(high_bound_val_.get_obj_ptr(), high_bound_val_.get_obj_cnt());
  } else if (is_list_part(part_type_)) {
    list_row_values_.shallow_copy(dest.list_row_values_);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected part type", KR(ret), K(part_type_));
  }
  if (OB_SUCC(ret)) {
    dest.part_type_ = part_type_;
    dest.part_op_ = part_op_;
    part_key_idxs_.shallow_copy(dest.part_key_idxs_);
  } else {
    dest.reset();
  }
  return ret;
}

int ObTruncatePartition::compare(const ObTruncatePartition &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  equal = false;
  if (this == &other) {
    equal = true;
  } else if ((part_type_ == other.part_type_)
      && (part_op_ == other.part_op_)
      && OB_SUCC(part_key_idxs_.compare(other.part_key_idxs_, equal) && equal)) {
    if (is_range_part(part_type_)) {
      if (OB_SUCC(low_bound_val_.equal(other.low_bound_val_, equal)) && equal) {
        ret = high_bound_val_.equal(other.high_bound_val_, equal);
      }
    } else if (is_list_part(part_type_)) {
      ret = list_row_values_.compare(other.list_row_values_, equal);
    } else {
      ret = OB_INVALID_DATA;
      LOG_WARN("invalid part type", KR(ret), K_(part_type));
    }
  } else {
    equal = false;
  }
  return ret;
}

int64_t ObTruncatePartition::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  gene_info(buf, buf_len, pos);
  return pos;
}

void ObTruncatePartition::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV("part_type", part_type_to_str(part_type_), "part_op", part_op_to_str(part_op_), K_(part_key_idxs));
    J_COMMA();
    if (is_range_part(part_type_)) {
      J_KV(K_(low_bound_val), K_(high_bound_val));
    } else if (is_list_part(part_type_) && list_row_values_.is_valid()) {
      J_KV("list_row_values_cnt", list_row_values_.count());
      J_COMMA();
      const ObNewRow *values = list_row_values_.get_values();
      for (int64_t idx = 0; idx < list_row_values_.count(); ++idx) {
        BUF_PRINTF("[%ld]:", idx);
        const ObNewRow &row = values[idx];
        (void)databuff_print_obj_array(buf, buf_len, pos, row.cells_, row.count_);
        if (idx + 1 != list_row_values_.count()) {
          BUF_PRINTF(";");
        }
      }
    }
    J_OBJ_END();
  }
}

/*
* ObTruncateInfo
* */
ObTruncateInfo::ObTruncateInfo()
  : version_(TRUNCATE_INFO_VERSION_LATEST),
    is_sub_part_(false),
    reserved_(0),
    key_(),
    commit_version_(0),
    schema_version_(0),
    truncate_part_(),
    truncate_subpart_(),
    allocator_(nullptr)
{}

void ObTruncateInfo::destroy()
{
  version_ = TRUNCATE_INFO_VERSION_LATEST;
  is_sub_part_ = false;
  reserved_ = 0;
  key_.reset();
  commit_version_ = 0;
  schema_version_ = 0;
  if (OB_NOT_NULL(allocator_)) {
    truncate_part_.destroy(*allocator_);
    truncate_subpart_.destroy(*allocator_);
    allocator_ = nullptr;
  } else {
    truncate_part_.reset();
    truncate_subpart_.reset();
  }
}

bool ObTruncateInfo::is_valid() const
{
  // commit_version_ is filled after trans commit, should not check
  return key_.is_valid()
    && schema_version_ > 0
    && truncate_part_.is_valid()
    && (!is_sub_part_ || truncate_subpart_.is_valid());
}

int ObTruncateInfo::assign(ObIAllocator &allocator, const ObTruncateInfo &other)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(other));
  } else if (FALSE_IT(allocator_ = &allocator)) {
  } else if (OB_FAIL(truncate_part_.assign(allocator, other.truncate_part_))) {
    LOG_WARN("failed to assign truncate part", KR(ret), K(other));
  } else if (other.is_sub_part_ && OB_FAIL(truncate_subpart_.assign(allocator, other.truncate_subpart_))) {
    LOG_WARN("failed to assign truncate subpart", KR(ret), K(other));
  } else {
    info_ = other.info_;
    key_ = other.key_;
    commit_version_ = other.commit_version_;
    schema_version_ = other.schema_version_;
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int ObTruncateInfo::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              info_,
              key_,
              commit_version_,
              schema_version_,
              truncate_part_);
  if (OB_SUCC(ret) && is_sub_part_) {
    LST_DO_CODE(OB_UNIS_ENCODE, truncate_subpart_);
  }
  return ret;
}

int64_t ObTruncateInfo::get_serialize_size() const
{
  int64_t len = 0;
  if (OB_UNLIKELY(!is_valid())) {
    LOG_ERROR_RET(OB_ERR_UNEXPECTED, "Unexpected truncate info to serialize", KPC(this));
  } else {
    LST_DO_CODE(OB_UNIS_ADD_LEN,
              info_,
              key_,
              commit_version_,
              schema_version_,
              truncate_part_);
    if (is_sub_part_) {
      LST_DO_CODE(OB_UNIS_ADD_LEN, truncate_subpart_);
    }
  }
  return len;
}

int ObTruncateInfo::deserialize(
    ObIAllocator &allocator,
    const char *buf,
    const int64_t data_len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  allocator_ = &allocator;
  LST_DO_CODE(OB_UNIS_DECODE,
              info_,
              key_,
              commit_version_,
              schema_version_);
  if (FAILEDx(truncate_part_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize truncate part", KR(ret), K(data_len), K(pos));
  } else if (is_sub_part_ && OB_FAIL(truncate_subpart_.deserialize(allocator, buf, data_len, pos))) {
    LOG_WARN("failed to deserialize truncate subpart", KR(ret), K(data_len), K(pos));
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("deserialized truncate_info is invalid", KR(ret), KPC(this));
  }
  if (OB_FAIL(ret)) {
    destroy();
  }
  return ret;
}

int64_t ObTruncateInfo::get_deep_copy_size() const
{
  return truncate_part_.get_deep_copy_size() + (is_sub_part_ ? truncate_subpart_.get_deep_copy_size() : 0);
}

int ObTruncateInfo::deep_copy(
      char *buf,
      const int64_t buf_len,
      int64_t &pos,
      ObTruncateInfo &dest) const
{
  int ret = OB_SUCCESS;
  const int64_t deep_copy_size = get_deep_copy_size();
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to deep copy", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(pos + deep_copy_size > buf_len)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("deep copy buf is invalid", KR(ret), K(deep_copy_size), K(pos), K(buf_len));
  } else if (OB_FAIL(truncate_part_.deep_copy(buf, buf_len, pos, dest.truncate_part_))) {
    LOG_WARN("failed to deep copy truncate_part", KR(ret), K_(truncate_part));
  } else if (is_sub_part_ && OB_FAIL(truncate_subpart_.deep_copy(buf, buf_len, pos, dest.truncate_subpart_))) {
    LOG_WARN("failed to deep copy truncate_subpart", KR(ret), K_(is_sub_part), K_(truncate_subpart));
  } else {
    dest.info_ = info_;
    dest.key_ = key_;
    dest.commit_version_ = commit_version_;
    dest.schema_version_ = schema_version_;
  }
  return ret;
}

int ObTruncateInfo::shallow_copy(ObTruncateInfo &dest)
{  
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data to shallow copy", KR(ret), KPC(this));
  } else if (OB_FAIL(truncate_part_.shallow_copy(dest.truncate_part_))) {
    LOG_WARN("failed to shallow copy truncate_part", KR(ret), K_(truncate_part));
  } else if (is_sub_part_ && OB_FAIL(truncate_subpart_.shallow_copy(dest.truncate_subpart_))) {
    LOG_WARN("failed to shallow copy truncate_subpart", KR(ret), K_(is_sub_part), K_(truncate_subpart));
  } else {
    dest.info_ = info_;
    dest.key_ = key_;
    dest.commit_version_ = commit_version_;
    dest.schema_version_ = schema_version_;
    // shallow copy should not assign allocator_
  }
  return ret;
}

void ObTruncateInfo::gene_info(char* buf, const int64_t buf_len, int64_t &pos) const
{
  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    J_OBJ_START();
    J_KV(K_(key), K_(commit_version), K_(schema_version), K_(truncate_part));
    if (is_sub_part_) {
      J_COMMA();
      J_KV(K_(truncate_subpart));
    }
    J_OBJ_END();
  }
}

void ObTruncateInfo::on_commit(const share::SCN &commit_version, const share::SCN &commit_scn)
{ // fill commit version after mds trans commit
  commit_version_ = commit_version.get_val_for_tx();
}

int ObTruncateInfo::compare(const ObTruncateInfo &other, bool &equal) const
{
  equal = false;
  int ret = OB_SUCCESS;
  if (info_ == other.info_
    && key_ == other.key_
    && commit_version_ == other.commit_version_
    && schema_version_ == other.schema_version_) {
    ret = compare_truncate_part_info(other, equal);
  }
  return ret;
}

int ObTruncateInfo::compare_truncate_part_info(const ObTruncateInfo &other, bool &equal) const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(truncate_part_.compare(other.truncate_part_, equal))) {
    LOG_WARN("failed to compare truncate part", KR(ret), KPC(this), K(other));
  } else if (!equal) {
  } else if (is_sub_part_ != other.is_sub_part_) {
    equal = false;
  } else if (is_sub_part_ && OB_FAIL(truncate_subpart_.compare(other.truncate_subpart_, equal))) {
    LOG_WARN("failed to compare truncate subpart", KR(ret), KPC(this), K(other));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase

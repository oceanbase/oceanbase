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

#include "storage/ob_super_block_struct.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"

namespace oceanbase
{
namespace storage
{

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::share;

const MacroBlockId ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK(0, MacroBlockId::EMPTY_ENTRY_BLOCK_INDEX, 0);

// ========================== ObServerSuperBlock ==============================

ObServerSuperBlockHeader::ObServerSuperBlockHeader()
{
  reset();
}

bool ObServerSuperBlockHeader::is_valid() const
{
  return version_ == SERVER_SUPER_BLOCK_VERSION
      && magic_ == SERVER_SUPER_BLOCK_MAGIC && body_size_ > 0;
}

void ObServerSuperBlockHeader::reset()
{
  version_ = 0;
  magic_ = 0;
  body_size_ = 0;
  body_crc_ = 0;
}

DEFINE_SERIALIZE(ObServerSuperBlockHeader)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || buf_len - pos < sizeof(ObServerSuperBlockHeader)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("serialize superblock failed.", K(ret), KP(buf), K(buf_len), K(pos), "header_size",
      sizeof(ObServerSuperBlockHeader));
  } else {
    MEMCPY(buf + pos, this, sizeof(ObServerSuperBlockHeader));
    pos += sizeof(ObServerSuperBlockHeader);
  }
  return ret;
}

DEFINE_DESERIALIZE(ObServerSuperBlockHeader)
{
  int ret = OB_SUCCESS;
  if (NULL == buf || data_len - pos < sizeof(ObServerSuperBlockHeader)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments.", K(ret), KP(buf), K(data_len), K(pos), "header_size",
      sizeof(ObServerSuperBlockHeader));
  } else {
    MEMCPY(this, buf + pos, sizeof(ObServerSuperBlockHeader));
    pos += sizeof(ObServerSuperBlockHeader);
  }

  if (OB_SUCC(ret)) {
    if (OB_UNLIKELY(SERVER_SUPER_BLOCK_VERSION != version_ || SERVER_SUPER_BLOCK_MAGIC != magic_)) {
      ret = OB_ERR_SYS;
      LOG_ERROR("version or magic not match", K(ret), K(*this));
    }
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObServerSuperBlockHeader)
{
  return sizeof(ObServerSuperBlockHeader);
}

ServerSuperBlockBody::ServerSuperBlockBody()
{
  reset();
}

bool ServerSuperBlockBody::is_valid() const
{
  return create_timestamp_ > 0 && modify_timestamp_ >= create_timestamp_ &&
    macro_block_size_ > 0 && total_macro_block_count_ > 0 &&
    total_file_size_ >= macro_block_size_ && replay_start_point_.is_valid() &&
    tenant_meta_entry_.is_valid();
}

void ServerSuperBlockBody::reset()
{
  create_timestamp_ = 0;
  modify_timestamp_ = 0;
  macro_block_size_ = 0;
  total_macro_block_count_ = 0;
  total_file_size_ = 0;
  replay_start_point_.reset();
  tenant_meta_entry_.reset();
}

OB_SERIALIZE_MEMBER(ServerSuperBlockBody, create_timestamp_,
  modify_timestamp_, macro_block_size_, total_macro_block_count_, total_file_size_,
  replay_start_point_, tenant_meta_entry_);

ObServerSuperBlock::ObServerSuperBlock() : header_(), body_() {}

bool ObServerSuperBlock::is_valid() const
{
  return header_.is_valid() && body_.is_valid();
}

int64_t ObServerSuperBlock::get_serialize_size() const
{
  return header_.get_serialize_size() + body_.get_serialize_size();
}

int ObServerSuperBlock::serialize(char *buf, const int64_t buf_size, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_ISNULL(buf) || buf_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (!is_valid()) {
    ret = OB_ERR_SYS;
    LOG_WARN("cannot write invalid super block", K(ret), K(*this));
  } else {
    MEMSET(buf + pos, 0, buf_size - pos);
    if (OB_FAIL(header_.serialize(buf, buf_size, new_pos))) {
      LOG_WARN("failed to encode super block header", K(ret), K(buf_size), K(new_pos), K(*this));
    } else if (OB_FAIL(body_.serialize(buf, buf_size, new_pos))) {
      LOG_WARN("failed to encode super block content", K(ret), K(buf_size), K(new_pos), K(*this));
    } else {
      pos = new_pos;
      LOG_INFO("succeed to serialize super block buf", K(buf_size), K(pos), K(*this));
    }
  }
  return ret;
}

int ObServerSuperBlock::deserialize(const char *buf, const int64_t buf_size, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int32_t calc_crc = 0;
  if (OB_ISNULL(buf) || buf_size <= 0 || pos < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), KP(buf), K(buf_size));
  } else if (is_valid()) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("cannot read super block twice", K(ret), K(*this));
  } else if (OB_FAIL(header_.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode header", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(header_.body_crc_ !=
      (calc_crc = static_cast<int32_t>(ob_crc64(buf + pos, header_.body_size_))))) {
    ret = OB_PHYSIC_CHECKSUM_ERROR;
    LOG_DBA_ERROR(OB_PHYSIC_CHECKSUM_ERROR, "msg", "failed to check crc", K(ret), KP(buf), K(buf_size), K(pos), K_(header), K(calc_crc));
  } else if (OB_FAIL(body_.deserialize(buf, buf_size, pos))) {
    LOG_WARN("failed to decode body", K(ret), KP(buf), K(buf_size), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("invalid data, ", K(ret), K(*this));
  } else {
    LOG_INFO("load server superblock success.", K(buf_size), K(pos), K(*this));
  }
  return ret;
}

void ObServerSuperBlock::reset()
{
  header_.reset();
  body_.reset();
}

int ObServerSuperBlock::construct_header()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!body_.is_valid())) {
    ret = OB_INVALID_DATA;
    LOG_WARN("super block body invalid", K(ret), K_(body));
  } else {
    // calculate crc of content serialized buffer
    int64_t pos = 0;
    int64_t body_buf_len = body_.get_serialize_size();
    char *body_buf = static_cast<char *>(ob_malloc(body_buf_len, "SuperBlock"));
    if (OB_ISNULL(body_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to allocate memory for body", K(ret));
    } else if (OB_FAIL(body_.serialize(body_buf, body_buf_len, pos))) {
      LOG_WARN("fail to serialize super block body", K(ret));
    } else {
      header_.version_ = ObServerSuperBlockHeader::SERVER_SUPER_BLOCK_VERSION;
      header_.magic_ = SERVER_SUPER_BLOCK_MAGIC;
      header_.body_size_ = body_buf_len;
      header_.body_crc_ = static_cast<int32_t>(ob_crc64(body_buf, body_buf_len));
      ob_free(body_buf);
    }
  }
  return ret;
}

int ObServerSuperBlock::format_startup_super_block(
  const int64_t macro_block_size, const int64_t data_file_size)
{
  int ret = OB_SUCCESS;

  if (macro_block_size <= 0 || data_file_size <= 0 || data_file_size < macro_block_size) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(macro_block_size), K(data_file_size));
  } else {
    reset();
    body_.create_timestamp_ = ObTimeUtility::current_time();
    body_.modify_timestamp_ = body_.create_timestamp_;
    body_.macro_block_size_ = macro_block_size;
    body_.total_macro_block_count_ = data_file_size / macro_block_size;
    body_.total_file_size_ = lower_align(data_file_size, macro_block_size);
    body_.tenant_meta_entry_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;

    body_.replay_start_point_.file_id_ = 1;
    body_.replay_start_point_.log_id_ = 1; // Due to the design of slog, the log_id_'s initial value must be 1
    body_.replay_start_point_.offset_ = 0;

    if (OB_FAIL(construct_header())) {
      LOG_WARN("fail to construct super block header", K(ret), K_(body));
    } else {
      LOG_INFO("success to format super block", K(*this));
    }
  }
  return ret;
}

// ========================== ObTenantSnapshotMetaMeta ==============================
int ObTenantSnapshotMeta::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, ls_meta_entry_, snapshot_id_);
  return ret;
}

int ObTenantSnapshotMeta::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, ls_meta_entry_, snapshot_id_);
  return ret;
}

int64_t ObTenantSnapshotMeta::get_serialize_size() const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, ls_meta_entry_, snapshot_id_);
  return len;
}

bool ObTenantSnapshotMeta::is_valid() const
{
  return ls_meta_entry_.is_valid()
         && snapshot_id_.is_valid();
}

void ObTenantSnapshotMeta::reset()
{
  ls_meta_entry_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  snapshot_id_.reset();
}

// ========================== ObTenantSuperBlock ==============================

ObTenantSuperBlock::ObTenantSuperBlock()
{
  reset();
}

ObTenantSuperBlock::ObTenantSuperBlock(const uint64_t tenant_id, const bool is_hidden)
  : tenant_id_(tenant_id), is_hidden_(is_hidden), version_(TENANT_SUPER_BLOCK_VERSION), snapshot_cnt_(0)
{
  replay_start_point_.file_id_ = 1;
  replay_start_point_.log_id_ = 1; // // Due to the design of slog, the log_id_'s initial value must be 1
  replay_start_point_.offset_ = 0;
  tablet_meta_entry_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  ls_meta_entry_ = ObServerSuperBlock::EMPTY_LIST_ENTRY_BLOCK;
  for (int64_t i = 0; i < MAX_SNAPSHOT_NUM; i++) {
    tenant_snapshots_[i].reset();
  }
}

ObTenantSuperBlock::ObTenantSuperBlock(const ObTenantSuperBlock &other)
{
  *this = other;
}

ObTenantSuperBlock &ObTenantSuperBlock::operator=(const ObTenantSuperBlock &other)
{
  if (this != &other) {
    reset();
    tenant_id_ = other.tenant_id_;
    replay_start_point_ = other.replay_start_point_;
    ls_meta_entry_ = other.ls_meta_entry_;
    tablet_meta_entry_ = other.tablet_meta_entry_;
    is_hidden_ = other.is_hidden_;
    version_ = other.version_;
    snapshot_cnt_ = other.snapshot_cnt_;
    for (int64_t i = 0; i < snapshot_cnt_; i++) {
      tenant_snapshots_[i] = other.tenant_snapshots_[i];
    }
  }
  return *this;
}

void ObTenantSuperBlock::reset()
{
  tenant_id_ = OB_INVALID_TENANT_ID;
  replay_start_point_.reset();
  ls_meta_entry_.reset();
  tablet_meta_entry_.reset();
  is_hidden_= false;
  version_ = TENANT_SUPER_BLOCK_VERSION;
  for (int64_t i = 0; i < MAX_SNAPSHOT_NUM; i++) {
    tenant_snapshots_[i].reset();
  }
  snapshot_cnt_ = 0;
}

void ObTenantSuperBlock::copy_snapshots_from(const ObTenantSuperBlock &other)
{
  snapshot_cnt_ = other.snapshot_cnt_;
  for (int64_t i = 0; i < snapshot_cnt_; i++) {
    tenant_snapshots_[i] = other.tenant_snapshots_[i];
  }
}

bool ObTenantSuperBlock::is_valid() const
{
  bool is_valid = OB_INVALID_TENANT_ID != tenant_id_
                  && replay_start_point_.is_valid()
                  && ls_meta_entry_.is_valid()
                  && tablet_meta_entry_.is_valid()
                  && version_ > MIN_SUPER_BLOCK_VERSION
                  && (is_old_version() || IS_EMPTY_BLOCK_LIST(tablet_meta_entry_))
                  && snapshot_cnt_ >= 0;
  return is_valid;
}

int ObTenantSuperBlock::get_snapshot(const ObTenantSnapshotID &snapshot_id, ObTenantSnapshotMeta &snapshot) const
{
  int ret = OB_SUCCESS;
  bool found = false;

  for (int64_t i = 0; OB_SUCC(ret) && !found && i < snapshot_cnt_; i++) {
    if (snapshot_id == tenant_snapshots_[i].snapshot_id_) {
      if (OB_UNLIKELY(!tenant_snapshots_[i].is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("snapshot is invalid", K(ret), K(tenant_snapshots_[i]));
      } else {
        snapshot = tenant_snapshots_[i];
        found = true;
      }
    }
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!found)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("snapshot doesn't exist", K(ret), K(snapshot_id));
  }
  return ret;
}

int ObTenantSuperBlock::add_snapshot(const ObTenantSnapshotMeta &snapshot)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_new_snapshot(snapshot.snapshot_id_))) {
    LOG_WARN("fail to check new snapshot", K(ret), K(snapshot));
  } else {
    tenant_snapshots_[snapshot_cnt_] = snapshot;
    snapshot_cnt_++;
  }
  return ret;
}

int ObTenantSuperBlock::check_new_snapshot(const ObTenantSnapshotID &snapshot_id) const
{
  int ret = OB_SUCCESS;
  if (snapshot_cnt_ >= MAX_SNAPSHOT_NUM) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("num of snapshots has reached the limit", K(ret), K(snapshot_cnt_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < snapshot_cnt_; i++) {
      if (snapshot_id == tenant_snapshots_[i].snapshot_id_) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("repeated snapshot id", K(ret), K(snapshot_id));
      }
    }
  }
  return ret;
}

int ObTenantSuperBlock::delete_snapshot(const ObTenantSnapshotID &snapshot_id)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  for (int64_t i = 0; -1 == index && i < snapshot_cnt_; i++) {
    if (tenant_snapshots_[i].snapshot_id_ == snapshot_id) {
      index = i;
    }
  }
  if (OB_UNLIKELY(index < 0)) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("target snapshot desn't exist", K(ret), K(snapshot_id), K(index));
  } else {
    for (int64_t i = index; i < snapshot_cnt_ - 1; i++) {
      tenant_snapshots_[i] = tenant_snapshots_[i + 1];
    }
    snapshot_cnt_--;
  }
  return ret;
}

int ObTenantSuperBlock::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(UNIS_VERSION);
  if (OB_SUCC(ret)) {
    int64_t size_nbytes = NS_::OB_SERIALIZE_SIZE_NEED_BYTES;
    int64_t pos_bak = (pos += size_nbytes);
    if (OB_SUCC(ret)) {
      if (OB_FAIL(serialize_(buf, buf_len, pos))) {
        LOG_WARN("fail to serialize", K(ret), KP(buf), K(buf_len), K(pos));
      }
    }
    int64_t serial_size = pos - pos_bak;
    int64_t tmp_pos = 0;
    int64_t expect_size = get_serialize_size_();
    assert(expect_size >= serial_size);
    if (OB_SUCC(ret)) {
      ret = common::serialization::encode_fixed_bytes_i64(buf + pos_bak - size_nbytes, size_nbytes, tmp_pos, serial_size);
    }
  }
  return ret;
}

int ObTenantSuperBlock::serialize_(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
      tenant_id_,
      replay_start_point_,
      ls_meta_entry_,
      tablet_meta_entry_,
      is_hidden_,
      tenant_snapshots_,
      snapshot_cnt_);
  return ret;
}

int ObTenantSuperBlock::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t len = 0;
  OB_UNIS_DECODE(version_);
  OB_UNIS_DECODE(len);
  if (OB_SUCC(ret)) {
    if (UNIS_VERSION < version_) {
      ret = ::oceanbase::common::OB_NOT_SUPPORTED;
      LOG_WARN("ObTenantSuperBlock object version mismatch", K(ret), K_(version));
    } else if (len < 0) {
      ret = ::oceanbase::common::OB_ERR_UNEXPECTED;
      LOG_WARN("can't decode object with negative length", K(ret), K(len));
    } else if (data_len < len + pos) {
      ret = ::oceanbase::common::OB_DESERIALIZE_ERROR;
      LOG_WARN("buf length not enough", K(ret), K(len), K(pos), K(data_len));
    } else {
      int64_t pos_orig = pos;
      pos = 0;
      if (OB_FAIL(deserialize_(buf + pos_orig, len, pos))) {
        LOG_WARN("fail to deserialize", K(ret), K(len), K(pos));
      }
      pos = pos_orig + len;
    }
  }
  return ret;
}

int ObTenantSuperBlock::deserialize_(const char *buf, const int64_t data_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
      tenant_id_,
      replay_start_point_,
      ls_meta_entry_,
      tablet_meta_entry_,
      is_hidden_,
      tenant_snapshots_,
      snapshot_cnt_);
  return ret;
}

int64_t ObTenantSuperBlock::get_serialize_size(void) const
{
  int64_t len = get_serialize_size_();
  SERIALIZE_SIZE_HEADER(UNIS_VERSION);
  return len;
}

int64_t ObTenantSuperBlock::get_serialize_size_(void) const
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
      tenant_id_,
      replay_start_point_,
      ls_meta_entry_,
      tablet_meta_entry_,
      is_hidden_,
      tenant_snapshots_,
      snapshot_cnt_);
  return len;
}

}  // end namespace storage
}  // end namespace oceanbase

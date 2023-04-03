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

#ifndef OCEANBASE_SIMPLE_OB_STORAGE_REDO_MODULE_H_
#define OCEANBASE_SIMPLE_OB_STORAGE_REDO_MODULE_H_

#include "storage/slog/ob_storage_log_replayer.h"
#include "lib/list/ob_list.h"
#include "lib/file/file_directory_utils.h"
#include "lib/file/ob_file.h"
#include "lib/random/ob_random.h"
#include "lib/allocator/ob_mod_define.h"
#include "storage/slog/ob_storage_log_struct.h"

namespace oceanbase
{
namespace storage
{

class ComplexObSlog : public ObIBaseStorageLogEntry
{
public:
  virtual bool is_valid() const override;
  ComplexObSlog();
  virtual ~ComplexObSlog() = default;
  bool operator ==(const ComplexObSlog &slog);
  bool operator !=(const ComplexObSlog &slog);
  TO_STRING_EMPTY();

public:
  NEED_SERIALIZE_AND_DESERIALIZE;

public:
  int64_t blocks_[1024];
  int64_t block_cnt_;
};

ComplexObSlog::ComplexObSlog()
  : block_cnt_(0)
{
}

bool ComplexObSlog::is_valid() const
{
  return true;
}

bool ComplexObSlog::operator ==(const ComplexObSlog &slog)
{
  bool ret = true;

  if (block_cnt_ != slog.block_cnt_) {
    ret = false;
  }
  for(int i = 0; ret && i < block_cnt_; i++) {
    if (blocks_[i] != slog.blocks_[i]) {
      ret = false;
    }
  }

  return ret;
}

bool ComplexObSlog::operator !=(const ComplexObSlog &slog)
{
  return !(*this == slog);
}

DEFINE_SERIALIZE(ComplexObSlog)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, block_cnt_))) {
    STORAGE_REDO_LOG(WARN, "Fail to encode block_cnt_", K_(block_cnt));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < block_cnt_; i++) {
      if (OB_FAIL(serialization::encode_vi64(buf, buf_len, pos, blocks_[i]))) {
        STORAGE_REDO_LOG(WARN, "Fail to encode block", K(blocks_[i]));
      }
    }
  }

  return ret;
}

DEFINE_DESERIALIZE(ComplexObSlog)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &block_cnt_))) {
    STORAGE_REDO_LOG(WARN, "Fail to decode block_cnt_", K(ret));
  } else {
    for (int i = 0; OB_SUCC(ret) && i < block_cnt_; i++) {
      if (OB_FAIL(serialization::decode_vi64(buf, data_len, pos, &blocks_[i]))) {
        STORAGE_REDO_LOG(WARN, "Fail to decode block", K(ret));
      }
    }
  }

  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ComplexObSlog)
{
  int64_t len = 0;
  len += serialization::encoded_length(block_cnt_);
  for (int i = 0; i < block_cnt_; i++) {
    len += serialization::encoded_length(blocks_[i]);
  }
  //len += ObRandom::rand(0, 100);

  return len;
}

class SimpleObStorageModule : public ObIRedoModule
{
public:
  SimpleObStorageModule();
  virtual ~SimpleObStorageModule() = default;
  int init(ObStorageLogReplayer *replayer, int64_t num);
  virtual int replay(const ObRedoModuleReplayParam &param) override;
  virtual int parse(
      const int32_t cmd,
      const char *buf,
      const int64_t len,
      FILE *stream) override;
  bool operator ==(SimpleObStorageModule &redo_module);
  void reset();

public:
  ComplexObSlog slogs_[128];
  int64_t slog_cnt_;
  int64_t index_;
};

SimpleObStorageModule::SimpleObStorageModule()
  : slog_cnt_(0), index_(0)
{
}

void SimpleObStorageModule::reset()
{
  index_ = 0;
}

int SimpleObStorageModule::parse(
    const int32_t cmd,
    const char *buf,
    const int64_t len,
    FILE *stream)
{
  UNUSEDx(cmd, buf, len, stream);
  return OB_NOT_SUPPORTED;
}

int SimpleObStorageModule::replay(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;

  const int64_t cmd = param.cmd_;
  const char *buf = param.buf_;
  const int64_t buf_len = param.disk_addr_.size_;
  enum ObRedoLogMainType main_type;
  enum ObRedoLogSubType sub_type;
  int64_t pos = 0;
  ObIRedoModule::parse_cmd(cmd, main_type, sub_type);

  if (ObRedoLogMainType::OB_REDO_LOG_TENANT_STORAGE != main_type) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_REDO_LOG(WARN, "The main type is wrong.", K(main_type));
  } else if (ObRedoLogSubType::OB_REDO_LOG_CREATE_LS == sub_type) {
    if (OB_FAIL(slogs_[index_].deserialize(buf, buf_len, pos))) {
      STORAGE_REDO_LOG(WARN, "Fail to recover slog.", K(sub_type), K(buf), K(buf_len));
    }
  } else if (ObRedoLogSubType::OB_REDO_LOG_DELETE_LS == sub_type) {
    // do nothing
  }

  if (OB_SUCC(ret)) {
    index_++;
    slog_cnt_ = index_;
  }

  return ret;
}

bool SimpleObStorageModule::operator ==(SimpleObStorageModule &redo_module)
{
  bool ret = true;

  if (slog_cnt_ != redo_module.slog_cnt_) {
    ret = false;
  }
  for (int i = 0; ret && i < slog_cnt_; i++) {
    if (slogs_[i] != redo_module.slogs_[i]) {
      ret = false;
    }
  }

  return ret;
}

}
}

#endif

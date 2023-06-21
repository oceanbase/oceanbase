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
#include "src/storage/meta_mem/ob_tablet_map_key.h"
#include "src/storage/ob_super_block_struct.h"
#include "src/storage/slog/ob_storage_log_reader.h"
#include "src/storage/slog/ob_storage_logger.h"
#include "src/storage/ls/ob_ls_tablet_service.h"
#include "src/storage/tx_storage/ob_ls_handle.h"
#include "src/storage/tx_storage/ob_ls_service.h"
#include "src/storage/tablet/ob_tablet.h"

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
  int inner_replay_empty_shell_tablet(const ObRedoModuleReplayParam &param);
  int read_from_disk(
      const ObMetaDiskAddr &addr,
      common::ObArenaAllocator &allocator,
      char *&buf,
      int64_t &buf_len);
  int read_from_disk_addr(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, char *&r_buf, int64_t &r_len);
  int read_from_share_blk(const ObMetaDiskAddr &addr, common::ObArenaAllocator &allocator, char *&buf, int64_t &buf_len);
  int read_from_slog(const ObMetaDiskAddr &phy_addr, char *buf, const int64_t buf_len, int64_t &pos);
  int get_tablet_svr(const share::ObLSID &ls_id, ObLSTabletService *&ls_tablet_svr, ObLSHandle &ls_handle);
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
  } else if (ObRedoLogSubType::OB_REDO_LOG_EMPTY_SHELL_TABLET == sub_type) {
    if (OB_FAIL(inner_replay_empty_shell_tablet(param))) {
      STORAGE_REDO_LOG(WARN, "Fail to inner replay empty shell tablet", K(sub_type), K(param));
    }
  }

  if (OB_SUCC(ret)) {
    index_++;
    slog_cnt_ = index_;
  }

  return ret;
}

int SimpleObStorageModule::get_tablet_svr(
    const ObLSID &ls_id,
    ObLSTabletService *&ls_tablet_svr,
    ObLSHandle &ls_handle)
{
  int ret = OB_SUCCESS;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    STORAGE_LOG(WARN, "fail to get ls handle", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "ls is null", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "tablet service is null", K(ret), K(ls_id));
  }
  return ret;
}

int SimpleObStorageModule::inner_replay_empty_shell_tablet(const ObRedoModuleReplayParam &param)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObArenaAllocator allocator;
  ObLSTabletService *ls_tablet_svr = nullptr;
  ObLSHandle ls_handle;
  char *buf = nullptr;
  int64_t buf_len = 0;
  ObEmptyShellTabletLog slog;
  ObTabletTransferInfo tablet_transfer_info;
  if (OB_FAIL(slog.deserialize_id(param.buf_, param.disk_addr_.size(), pos))) {
    STORAGE_LOG(WARN, "failed to serialize tablet_id_", K(ret), K(param.disk_addr_.size()), K(pos));
  } else if (OB_FAIL(read_from_disk(param.disk_addr_, allocator, buf, buf_len))) {
    STORAGE_LOG(WARN, "read from disk failed", K(ret), K(param.disk_addr_), K(buf_len));
  } else if (OB_FAIL(get_tablet_svr(slog.ls_id_, ls_tablet_svr, ls_handle))) {
    STORAGE_LOG(WARN, "get tablet svr failed", K(ret), K(slog.ls_id_));
  } else if (OB_FAIL(ls_tablet_svr->replay_create_tablet(param.disk_addr_, buf, buf_len, slog.tablet_id_, tablet_transfer_info))) {
    STORAGE_LOG(WARN, "replay empty shell tablet failed", K(ret), K(param.disk_addr_), K(slog.tablet_id_));
  }

  return ret;
}

int SimpleObStorageModule::read_from_disk(
    const ObMetaDiskAddr &addr,
    common::ObArenaAllocator &allocator,
    char *&buf,
    int64_t &buf_len)
{
  int ret = OB_SUCCESS;
  char *read_buf = nullptr;
  const int64_t read_buf_len = addr.size();
  if (ObMetaDiskAddr::DiskType::FILE == addr.type()) {
    ObEmptyShellTabletLog slog;
    int64_t pos = 0;
    if (OB_ISNULL(buf)) {
      if (OB_ISNULL(buf = static_cast<char*>(allocator.alloc(addr.size())))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        buf_len = addr.size();
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(read_from_slog(addr, buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "fail to read from slog", K(ret), K(addr), KP(buf), K(buf_len), K(pos));
      } else if (OB_FAIL(slog.deserialize_id(buf, buf_len, pos))) {
        STORAGE_LOG(WARN, "fail to deserialize id", K(ret), K(addr), KP(buf), K(buf_len), K(pos));
      } else {
        buf += pos;
        buf_len -= pos;
      }
    }
  } else if (OB_ISNULL(read_buf = static_cast<char*>(allocator.alloc(read_buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate buffer", K(ret), K(read_buf_len), KP(read_buf));
  } else if (OB_FAIL(read_from_disk_addr(addr, read_buf, read_buf_len, buf, buf_len))) {
    STORAGE_LOG(WARN, "fail to read tablet from addr", K(ret), K(addr), KP(read_buf), K(read_buf_len));
  }
  return ret;
}

int SimpleObStorageModule::read_from_disk_addr(const ObMetaDiskAddr &addr,
    char *buf, const int64_t buf_len, char *&r_buf, int64_t &r_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!addr.is_valid() || buf_len < addr.size()) || OB_ISNULL(buf)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(addr), KP(buf), K(buf_len));
  } else {
    switch (addr.type()) {
      case ObMetaDiskAddr::DiskType::FILE: {
        int64_t pos = 0;
        if (OB_FAIL(read_from_slog(addr, buf, buf_len, pos))) {
          STORAGE_LOG(WARN, "fail to read from slog", K(ret), K(addr), KP(buf), K(buf_len));
        } else {
          r_buf = buf + pos;
          r_len = addr.size() - pos;
        }
        break;
      }
      default: {
        ret = OB_NOT_SUPPORTED;
        STORAGE_LOG(WARN, "unknown meta disk address type", K(ret), K(addr), KP(buf), K(buf_len));
        break;
      }
    }
  }
  return ret;
}

int SimpleObStorageModule::read_from_slog(const ObMetaDiskAddr &addr,
    char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  ObStorageLogger *logger = MTL(ObStorageLogger*);

  if (OB_UNLIKELY(!addr.is_valid()
               || !addr.is_file()
               || buf_len < addr.size())
               || OB_ISNULL(buf)
               || OB_ISNULL(logger)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(addr), KP(buf), K(buf_len), KP(logger));
  } else {
    // The reason for retrying, here, is that the current SLOG didn't handle the read and write
    // concurrency for the latest item, and an -4103 error will be returned. At present, the
    // optimized changes for SLOG are relatively large, and we will bypass it in the short term.
    int64_t retry_count = 2;
    do {
      int64_t tmp_pos = pos;
      if (OB_FAIL(ObStorageLogReader::read_log(logger->get_dir(), addr, buf_len, buf, tmp_pos, MTL_ID()))) {
        STORAGE_LOG(WARN, "fail to read slog", K(ret), "logger directory", logger->get_dir(), K(addr),
            K(buf_len), KP(buf));
        if (retry_count > 1) {
          sleep(1); // sleep 1s
        }
      } else {
        pos = tmp_pos;
      }
    } while (OB_FAIL(ret) && --retry_count > 0);
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

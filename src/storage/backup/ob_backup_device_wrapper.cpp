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

#include "ob_backup_device_wrapper.h"
#include "storage/backup/ob_backup_factory.h"
#include "storage/restore/ob_tenant_restore_info_mgr.h"

using namespace oceanbase::share;
using namespace oceanbase::common;

namespace oceanbase
{
namespace common
{
extern const char *OB_STORAGE_ACCESS_TYPES_STR[];
}
namespace backup
{

const char *ObBackupWrapperIODevice::FIRST_ID_STR = "FirstId";
const char *ObBackupWrapperIODevice::SECOND_ID_STR = "SecondId";
const char *ObBackupWrapperIODevice::THIRD_ID_STR = "ThirdId";

ObBackupWrapperIODevice::ObBackupWrapperIODevice()
  : is_inited_(false),
    is_opened_(false),
    mutex_(0),
    used_block_cnt_(0),
    total_block_cnt_(0),
    block_size_(0),
    block_list_(),
    max_alloced_block_idx_(-1),
    backup_set_id_(),
    ls_id_(),
    turn_id_(),
    retry_id_(),
    file_id_(),
    block_type_(),
    io_fd_(),
    simulated_fd_id_(),
    simulated_slot_version_(),
    allocator_()
{
  allocator_.set_attr(lib::ObMemAttr(MTL_ID(), ObModIds::BACKUP));
}

ObBackupWrapperIODevice::~ObBackupWrapperIODevice()
{
  destroy();
  if (OB_NOT_NULL(block_list_)) {
    ob_free(block_list_);
    block_list_ = NULL;
  }
}

int ObBackupWrapperIODevice::setup_io_storage_info(const share::ObBackupDest &backup_dest,
    char *buf, const int64_t len, common::ObIODOpts *opts)
{
  int ret = OB_SUCCESS;
  common::ObObjectStorageInfo storage_info_base;
  if (OB_FAIL(storage_info_base.assign(*backup_dest.get_storage_info()))) {
    LOG_WARN("failed to assign storage info base!", K(ret), K(backup_dest));
  } else if (OB_FAIL(storage_info_base.get_storage_info_str(buf, len))) {
    LOG_WARN("failed to get storage info str!", K(ret), K(backup_dest));
  } else if (opts->opt_cnt_ < 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opts cnt too small", K(ret), "opt_cnt", opts->opt_cnt_);
  } else {
    opts->opts_[0].set("storage_info", buf);
  }
  return ret;
}

int ObBackupWrapperIODevice::setup_io_opts_for_backup_device(
    const int64_t backup_set_id, const share::ObLSID &ls_id, const share::ObBackupDataType &backup_data_type,
    const int64_t turn_id, const int64_t retry_id, const int64_t file_id, const ObBackupDeviceMacroBlockId::BlockType &block_type,
    const ObStorageAccessType &access_type, ObIODOpts *opts)
{
  int ret = OB_SUCCESS;
  const int64_t max_opt_num = BACKUP_WRAPPER_DEVICE_OPT_NUM;
  if (OB_ISNULL(opts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("opts should not be null", K(ret));
  } else if (opts->opt_cnt_ != max_opt_num) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("opts cnt too large", K(ret), K(opts->opt_cnt_));
  } else if (backup_set_id <= 0 || !ls_id.is_valid() || !backup_data_type.is_valid()
      || turn_id <= 0 || retry_id < 0 || file_id < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(backup_set_id), K(ls_id), K(backup_data_type),
        K(turn_id), K(retry_id), K(file_id));
  } else {
    ObBackupDeviceMacroBlockId tmp_macro_id;
    if (OB_FAIL(tmp_macro_id.set(backup_set_id, ls_id.id(), static_cast<int64_t>(backup_data_type.type_),
        turn_id, retry_id, file_id, 0/*offset*/, 0/*length*/, block_type))) {
      LOG_WARN("failed to set tmp macro id", K(ret));
    } else {
      opts->opts_[1].set(FIRST_ID_STR, tmp_macro_id.first_id_);
      opts->opts_[2].set(SECOND_ID_STR, tmp_macro_id.second_id_);
      opts->opts_[3].set(THIRD_ID_STR, tmp_macro_id.third_id_);
      opts->opts_[4].set("AccessType", OB_STORAGE_ACCESS_TYPES_STR[access_type]);
      if (access_type == OB_STORAGE_ACCESS_APPENDER)
      {
        opts->opts_[5].set("OpenMode", "CREATE_OPEN_NOLOCK");
      }
    }
  }
  return ret;
}

void ObBackupWrapperIODevice::destroy()
{
  ObObjectDevice::destroy();
}

int ObBackupWrapperIODevice::open(
    const char *pathname, const int flags, const mode_t mode,
    ObIOFd &fd, ObIODOpts *opts)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("backup wrapper io device", K(ret));
  } else if (OB_FAIL(parse_storage_device_type_(pathname))) {
    LOG_WARN("failed to parse storage device type", K(ret), K(pathname));
  } else if (OB_FAIL(parse_io_device_opts_(opts))) {
    LOG_WARN("failed to parse io device opts", K(ret));
  } else if (OB_FAIL(pre_alloc_block_array_())) {
    LOG_WARN("failed to pre alloc block array", K(ret));
  } else if (OB_FAIL(ObObjectDevice::start(*opts))) {
    LOG_WARN("failed to start opts", K(ret));
  } else if (OB_FAIL(ObObjectDevice::open(pathname, flags, mode, fd, opts))) {
    LOG_WARN("failed to open object device", K(ret), K(pathname));
  } else {
    io_fd_ = fd;
    simulated_fd_id_ = fd.fd_id_;
    simulated_slot_version_ = fd.slot_version_;
    is_opened_ = true;
    is_inited_ = true;
  }
  return ret;
}

int ObBackupWrapperIODevice::alloc_block(const ObIODOpts *opts, ObIOFd &block_id)
{
  int ret = OB_SUCCESS;
  ObMutexGuard guard(mutex_);
  int64_t idx = 0;
  bool need_realloc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup wrapper io device do not init", K(ret));
  } else if (OB_FAIL(check_need_realloc_(need_realloc))) {
    LOG_WARN("failed to check need realloc", K(ret));
  } else if (need_realloc && OB_FAIL(realloc_block_array_())) {
    LOG_WARN("failed to realloc block array", K(ret));
  } else if (OB_FAIL(get_min_unused_block_(idx))) {
    LOG_WARN("failed to get min unused block", K(ret));
  } else if (OB_FAIL(convert_block_id_to_addr_(idx, block_id))) {
    LOG_WARN("failed to convert block id to addr", K(ret), K(idx));
  } else {
    LOG_INFO("success alloc block id", K(block_id));
  }
  return ret;
}

int ObBackupWrapperIODevice::pread(const ObIOFd &fd, const int64_t offset, const int64_t size,
    void *buf, int64_t &read_size, ObIODPreadChecker *checker)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("ObBackupWrapperIODevice::pread", 10_ms);
  backup::ObBackupDeviceMacroBlockId macro_id;
  macro_id.first_id_ = fd.first_id_;
  macro_id.second_id_ = fd.second_id_;
  macro_id.third_id_ = fd.third_id_;
  const bool is_index_block = macro_id.is_index_block();
  int64_t io_offset = 0;
  if (is_index_block) {
    io_offset = offset + macro_id.get_offset();
  } else {
    io_offset = macro_id.get_offset() + sizeof(ObBackupCommonHeader) + offset;
  }

  if (OB_FAIL(ObObjectDevice::pread(fd,
                                    io_offset,
                                    size,
                                    buf,
                                    read_size,
                                    checker))) {
    LOG_WARN("failed to pread", K(ret), K(fd), K(offset), K(io_offset), K(size));
  } else {
    LOG_INFO("backup wrapper io device pread", K(fd), K(offset), K(io_offset), K(size), K(read_size));
  }
  return ret;
}

int ObBackupWrapperIODevice::pwrite(const ObIOFd &fd, const int64_t offset,
    const int64_t size, const void *buf, int64_t &write_size)
{
  int ret = OB_SUCCESS;
  ObTimeGuard timeguard("ObBackupWrapperIODevice::pwrite", 10_ms);
  backup::ObBackupDeviceMacroBlockId macro_id;
  macro_id.first_id_ = fd.first_id_;
  macro_id.second_id_ = fd.second_id_;
  macro_id.third_id_ = fd.third_id_;
  const bool is_index_block = macro_id.is_index_block();
  int64_t io_offset = 0;
  if (is_index_block) {
    io_offset = offset + macro_id.get_offset();
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("only index block should pwrite", K(ret), K(macro_id), K(offset), K(size));
  }

  if (FAILEDx(ObObjectDevice::pwrite(fd,
                                     io_offset,
                                     size,
                                     buf,
                                     write_size))) {
    LOG_WARN("failed to pwrite", K(ret), K(macro_id), K(offset), K(io_offset), K(size));
  } else {
    LOG_INFO("backup wrapper io device pwrite", K(fd), K(macro_id), K(offset), K(io_offset), K(size), K(write_size));
  }
  return ret;
}

int ObBackupWrapperIODevice::close(const ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    // do nothing
  } else if (OB_FAIL(ObObjectDevice::close(fd))) {
    LOG_WARN("failed to close fd", K(ret), K(fd));
  } else {
    is_opened_ = false;
    LOG_DEBUG("success close io fd", K(ret), K_(io_fd));
  }
  return ret;
}

void ObBackupWrapperIODevice::dec_ref()
{
  int ret = OB_SUCCESS;
  int64_t tmp_ref = ATOMIC_SAF(&ref_cnt_, 1);
  if (OB_UNLIKELY(tmp_ref < 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ref_cnt < 0", K(ret), K(tmp_ref), KCSTRING(lbt()));
  } else if (0 == tmp_ref) {
    ObLSBackupFactory::free(this);
  }
}

int ObBackupWrapperIODevice::alloc_mem_block(const int64_t size, char *&buf)
{
  int ret = OB_SUCCESS;
  if (size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(size));
  } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(size)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(size));
  } else {
    LOG_INFO("alloc mem block", K(size), KP(buf));
  }
  return ret;
}

int ObBackupWrapperIODevice::parse_storage_device_type_(
    const common::ObString &storage_type_prefix)
{
  int ret = OB_SUCCESS;
  ObStorageType device_type = OB_STORAGE_MAX_TYPE;

  if (storage_type_prefix.prefix_match(OB_LOCAL_PREFIX)) {
    device_type = OB_STORAGE_LOCAL;
  } else if (storage_type_prefix.prefix_match(OB_FILE_PREFIX)) {
    device_type = OB_STORAGE_FILE;
  } else if (storage_type_prefix.prefix_match(OB_OSS_PREFIX)) {
    device_type = OB_STORAGE_OSS;
  } else if (storage_type_prefix.prefix_match(OB_COS_PREFIX)) {
    device_type = OB_STORAGE_COS;
  } else if (storage_type_prefix.prefix_match(OB_S3_PREFIX)) {
    device_type = OB_STORAGE_S3;
  } else {
    ret = OB_INVALID_BACKUP_DEST;
    LOG_WARN("invaild device name info!", K(storage_type_prefix));
  }

  if (OB_SUCCESS != ret) {
  } else {
    this->device_type_ = device_type;
  }

  return ret;
}

int ObBackupWrapperIODevice::pre_alloc_block_array_()
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), ObModIds::BACKUP);
  const int64_t total_block_cnt = DEFAULT_BLOCK_COUNT;
  if (OB_ISNULL(block_list_ = static_cast<int64_t *>(
      ob_malloc(sizeof(int64_t) * total_block_cnt, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(total_block_cnt));
  } else {
    total_block_cnt_ = total_block_cnt;
  }
  return ret;
}

int ObBackupWrapperIODevice::check_need_realloc_(bool &need_realloc)
{
  int ret = OB_SUCCESS;
  need_realloc = used_block_cnt_ == total_block_cnt_;
  return ret;
}

int ObBackupWrapperIODevice::realloc_block_array_()
{
  int ret = OB_SUCCESS;
  const ObMemAttr mem_attr(MTL_ID(), ObModIds::BACKUP);
  const int64_t new_total_block_cnt = total_block_cnt_ * 2;
  int64_t *new_block_list = NULL;
  if (OB_ISNULL(new_block_list
      = static_cast<int64_t *>(ob_malloc(sizeof(int64_t) * new_total_block_cnt, mem_attr)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to allocate memory", K(ret), K(new_total_block_cnt));
  } else {
    MEMCPY(new_block_list, block_list_, total_block_cnt_ * sizeof(int64_t));
    ob_free(block_list_);
    block_list_ = new_block_list;
    total_block_cnt_ = new_total_block_cnt;
  }
  if (OB_FAIL(ret)) {
    if (OB_NOT_NULL(new_block_list)) {
      ob_free(new_block_list);
      new_block_list = NULL;
    }
  }
  return ret;
}

int ObBackupWrapperIODevice::get_min_unused_block_(int64_t &idx)
{
  int ret = OB_SUCCESS;
  idx = ++max_alloced_block_idx_;
  return ret;
}

int ObBackupWrapperIODevice::convert_block_id_to_addr_(const int64_t idx, ObIOFd &block_id)
{
  int ret = OB_SUCCESS;
  ObBackupDeviceMacroBlockId tmp_block_id;
  const ObBackupDeviceMacroBlockId::BlockType block_type = block_type_;
  const int64_t dir_id = static_cast<int64_t>(backup_data_type_.type_);
  int64_t offset = -1;
  int64_t length = -1;
  if (OB_FAIL(get_offset_and_length_(idx, offset, length))) {
    LOG_WARN("failed to get offset and length", K(ret), K(idx));
  } else if (OB_FAIL(tmp_block_id.set(backup_set_id_,
      ls_id_, dir_id, turn_id_, retry_id_, file_id_, offset, length, block_type))) {
    LOG_WARN("failed to set block id", K(ret));
  } else {
    block_id.first_id_ = tmp_block_id.first_id();
    block_id.second_id_ = tmp_block_id.second_id();
    block_id.third_id_ = tmp_block_id.third_id();
    block_id.device_handle_ = this;
  }
  return ret;
}

int ObBackupWrapperIODevice::get_offset_and_length_(const int64_t idx, int64_t &offset, int64_t &length)
{
  int ret = OB_SUCCESS;
  if (idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), K(idx));
  } else {
    offset = idx * DEFAULT_ALLOC_BLOCK_SIZE;
    length = DEFAULT_ALLOC_BLOCK_SIZE;
  }
  return ret;
}

int ObBackupWrapperIODevice::parse_io_device_opts_(common::ObIODOpts *opts)
{
  int ret = OB_SUCCESS;
  ObOptValue opt_value;
  int64_t dir_id = 0;
  ObBackupDeviceMacroBlockId tmp_id;
  if (OB_ISNULL(opts)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arg", K(ret), KP(opts));
  } else if (OB_FAIL(get_opt_value_(opts, FIRST_ID_STR, tmp_id.first_id_))) {
    LOG_WARN("failed to get opt value", K(ret));
  } else if (OB_FAIL(get_opt_value_(opts, SECOND_ID_STR, tmp_id.second_id_))) {
    LOG_WARN("failed to get opt value", K(ret));
  } else if (OB_FAIL(get_opt_value_(opts, THIRD_ID_STR, tmp_id.third_id_))) {
    LOG_WARN("failed to get opt value", K(ret));
  } else {
    backup_set_id_ = tmp_id.backup_set_id_;
    ls_id_ = tmp_id.ls_id_;
    backup_data_type_.type_ = static_cast<ObBackupDataType::BackupDataType>(tmp_id.data_type_);
    turn_id_ = tmp_id.turn_id_;
    retry_id_ = tmp_id.retry_id_;
    file_id_ = tmp_id.file_id_;
    block_type_ = static_cast<ObBackupDeviceMacroBlockId::BlockType>(tmp_id.block_type_);
  }
  return ret;
}

int ObBackupWrapperIODevice::get_opt_value_(ObIODOpts *opts, const char* key, int64_t& value)
{
  int ret = OB_SUCCESS;
  bool exist = false;
  if (OB_ISNULL(opts) || OB_ISNULL(key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), KP(opts), KP(key));
  } else {
    for (int i = 0; i < opts->opt_cnt_; i++) {
      if (0 == STRCMP(opts->opts_[i].key_, key)) {
        value = opts->opts_[i].value_.value_int64;
        exist = true;
        break;
      }
    }
  }
  if (OB_SUCC(ret) && !exist) {
    ret = OB_ENTRY_NOT_EXIST;
    LOG_WARN("key not exist", K(ret), K(key));
  }
  return ret;
}

int ObBackupDeviceHelper::alloc_backup_device(const uint64_t tenant_id, ObBackupWrapperIODevice *&device)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(device = ObLSBackupFactory::get_backup_wrapper_io_device(tenant_id))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc backup device", K(ret), K(tenant_id));
  } else {
    device->inc_ref();
  }

  return ret;
}

void ObBackupDeviceHelper::release_backup_device(ObBackupWrapperIODevice *&device)
{
  if (OB_NOT_NULL(device)) {
    device->dec_ref();
    device = NULL;
  }
}

int ObBackupDeviceHelper::get_device_and_fd(
    const uint64_t tenant_id,
    const int64_t first_id,
    const int64_t second_id,
    const int64_t third_id,
    ObStorageIdMod &mod,
    ObBackupWrapperIODevice *&device,
    ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  ObBackupWrapperIODevice *device_hd = NULL;
  ObBackupDeviceMacroBlockId backup_macro_id;
  backup_macro_id.first_id_ = first_id;
  backup_macro_id.second_id_ = second_id;
  backup_macro_id.third_id_ = third_id;
  ObBackupPath backup_path;
  ObIODOpts io_d_opts;
  ObIODOpt opts[BACKUP_WRAPPER_DEVICE_OPT_NUM];
  io_d_opts.opts_ = opts;
  io_d_opts.opt_cnt_ = BACKUP_WRAPPER_DEVICE_OPT_NUM;
  const int flag = -1;
  const mode_t mode = 0;
  const bool is_index_block = backup_macro_id.is_index_block();
  char buf[common::OB_MAX_BACKUP_STORAGE_INFO_LENGTH] = { 0 };
  ObBackupDest backup_dest;
  if (OB_FAIL(alloc_backup_device(tenant_id, device_hd))) {
    LOG_WARN("failed to alloc backup device", K(ret), K(tenant_id));
  } else if (is_index_block && OB_FAIL(get_companion_index_file_path_(tenant_id, backup_macro_id, backup_path))) {
    LOG_WARN("failed to get companion index file path", K(ret), K(backup_macro_id));
  } else if (!is_index_block && OB_FAIL(get_backup_data_file_path_(tenant_id, backup_macro_id, backup_path))) {
    LOG_WARN("failed to get data file path", K(ret), K(backup_macro_id));
  } else if (OB_FAIL(get_backup_dest_(tenant_id, backup_macro_id.get_backupset_id(), backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id), K(backup_macro_id));
  } else if (OB_FAIL(get_restore_dest_id_(tenant_id, mod))) {
    LOG_WARN("failed to get restore dest id", K(tenant_id));
  } else if (OB_FAIL(ObBackupWrapperIODevice::setup_io_storage_info(backup_dest, buf, sizeof(buf), &io_d_opts))) {
    LOG_WARN("failed to setup io storage info", K(ret), K(backup_dest));
  } else if (OB_FAIL(setup_io_device_opts_(tenant_id, backup_macro_id, &io_d_opts))) {
    LOG_WARN("failed to setup io device opts", K(ret), K(tenant_id), K(backup_macro_id), K(mod));
  } else if (OB_FAIL(device_hd->open(backup_path.get_ptr(),
                                     flag,
                                     mode,
                                     fd,
                                     &io_d_opts))) {
    LOG_WARN("failed to open device", K(ret), K(backup_path));
  } else if (FALSE_IT(device_hd->set_storage_id_mod(mod))) {
  } else {
    fd.first_id_ = first_id;
    fd.second_id_ = second_id;
    fd.third_id_ = third_id;
    device = device_hd;
    device_hd = NULL;
  }

  if (OB_NOT_NULL(device_hd)) {
    release_backup_device(device_hd);
  }

  return ret;
}

int ObBackupDeviceHelper::close_device_and_fd(ObBackupWrapperIODevice *&device_handle, ObIOFd &fd)
{
  int ret = OB_SUCCESS;
  if (NULL == device_handle) {
    ret = OB_INVALID_ARGUMENT;
    OB_LOG(WARN, "device handle is empty");
  } else {
    // device_handle's memory will be released when fd ctx ref cnt become 0.
    if (OB_FAIL(device_handle->close(fd))) {
      OB_LOG(WARN, "fail to close fd!", K(ret), K(fd));
    }
    release_backup_device(device_handle);
  }

  return ret;
}

int ObBackupDeviceHelper::get_companion_index_file_path_(const uint64_t tenant_id,
    const ObBackupDeviceMacroBlockId &backup_macro_id, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  ObBackupDest backup_dest;
  share::ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = backup_macro_id.get_backupset_id();
  const share::ObLSID &ls_id = ObLSID(backup_macro_id.get_ls_id());
  ObBackupDataType backup_data_type;
  backup_data_type.type_ = static_cast<ObBackupDataType::BackupDataType>(backup_macro_id.get_data_type());
  const int64_t turn_id = backup_macro_id.get_turn_id();
  const int64_t retry_id = backup_macro_id.get_retry_id();
  const int64_t file_id = backup_macro_id.get_file_id();
  ObBackupIntermediateTreeType tree_type;
  if (OB_FAIL(backup_macro_id.get_intermediate_tree_type(tree_type))) {
    LOG_WARN("failed to get intermediate tree type", K(ret), K(backup_macro_id));
  } else if (OB_FAIL(get_backup_dest_(tenant_id, backup_set_desc.backup_set_id_, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(backup_set_desc));
  } else if (OB_FAIL(get_backup_type_(tenant_id, backup_set_desc.backup_set_id_, backup_set_desc.backup_type_))) {
    LOG_WARN("failed to get backup set type", K(ret), K(backup_set_desc));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_intermediate_layer_index_backup_path(
      backup_dest, ls_id, backup_data_type, turn_id, retry_id, file_id, tree_type, backup_path))) {
    LOG_WARN("failed to get intermediate layer index backup path", K(ret), K(backup_dest), K(backup_set_desc), K(tree_type));
  } else {
    LOG_DEBUG("get intermediate layer index backup path", K(backup_dest), K(backup_path));
  }
  return ret;
}

int ObBackupDeviceHelper::get_backup_data_file_path_(const uint64_t tenant_id,
    const ObBackupDeviceMacroBlockId &backup_macro_id, ObBackupPath &backup_path)
{
  int ret = OB_SUCCESS;
  backup_path.reset();
  ObBackupDest backup_dest;
  share::ObBackupSetDesc backup_set_desc;
  backup_set_desc.backup_set_id_ = backup_macro_id.get_backupset_id();
  const share::ObLSID &ls_id = ObLSID(backup_macro_id.get_ls_id());
  ObBackupDataType backup_data_type;
  backup_data_type.type_ = static_cast<ObBackupDataType::BackupDataType>(backup_macro_id.get_data_type());
  const int64_t turn_id = backup_macro_id.get_turn_id();
  const int64_t retry_id = backup_macro_id.get_retry_id();
  const int64_t file_id = backup_macro_id.get_file_id();
  if (OB_FAIL(get_backup_dest_(tenant_id, backup_set_desc.backup_set_id_, backup_dest))) {
    LOG_WARN("failed to get backup dest", K(ret), K(backup_set_desc));
  } else if (OB_FAIL(get_backup_type_(tenant_id, backup_set_desc.backup_set_id_, backup_set_desc.backup_type_))) {
    LOG_WARN("failed to get backup set type", K(ret), K(backup_set_desc));
  } else if (OB_FAIL(ObBackupPathUtilV_4_3_2::get_macro_block_backup_path(
      backup_dest, ls_id, backup_data_type, turn_id, retry_id, file_id, backup_path))) {
    LOG_WARN("failed to get backup data file backup path", K(ret), K(backup_dest), K(backup_set_desc));
  } else {
    LOG_INFO("get backup data file backup path", K(backup_dest), K(backup_path));
  }
  return ret;
}

int ObBackupDeviceHelper::setup_io_device_opts_(const uint64_t tenant_id,
    const ObBackupDeviceMacroBlockId &backup_macro_id, common::ObIODOpts *io_d_opts)
{
  int ret = OB_SUCCESS;
  const ObStorageAccessType access_type = OB_STORAGE_ACCESS_READER;
  const int64_t backup_set_id = backup_macro_id.get_backupset_id();
  const share::ObLSID &ls_id = ObLSID(backup_macro_id.get_ls_id());
  ObBackupDataType backup_data_type;
  backup_data_type.type_ = static_cast<ObBackupDataType::BackupDataType>(backup_macro_id.get_data_type());
  const int64_t turn_id = backup_macro_id.get_turn_id();
  const int64_t retry_id = backup_macro_id.get_retry_id();
  const int64_t file_id = backup_macro_id.get_file_id();
  const ObBackupDeviceMacroBlockId::BlockType block_type = static_cast<ObBackupDeviceMacroBlockId::BlockType>(backup_macro_id.block_type_);
  if (OB_FAIL(ObBackupWrapperIODevice::setup_io_opts_for_backup_device(backup_set_id,
      ls_id, backup_data_type, turn_id, retry_id, file_id, block_type, access_type, io_d_opts))) {
    LOG_WARN("failed to setup io opts for backup device", K(ret), K(backup_macro_id));
  }
  return ret;
}

int ObBackupDeviceHelper::get_backup_dest_(const uint64_t tenant_id, const int64_t backup_set_id, share::ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObTenantRestoreInfoMgr *mgr = MTL(ObTenantRestoreInfoMgr *);
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr should not be null", K(ret));
    } else if (OB_FAIL(mgr->get_backup_dest(backup_set_id, backup_dest))) {
      LOG_WARN("failed to get backup dest", K(ret), K(backup_set_id));
    } else {
      LOG_INFO("get backup dest", K(backup_set_id), K(backup_dest));
    }
  }
  return ret;
}

int ObBackupDeviceHelper::get_backup_type_(const uint64_t tenant_id, const int64_t backup_set_id, ObBackupType &backup_type)
{
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    ObTenantRestoreInfoMgr *mgr = MTL(ObTenantRestoreInfoMgr *);
    if (OB_ISNULL(mgr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("mgr should not be null", K(ret));
    } else if (OB_FAIL(mgr->get_backup_type(backup_set_id, backup_type))) {
      LOG_WARN("failed to get backup type", K(ret), K(backup_set_id));
    } else {
      LOG_INFO("get backup type", K(backup_set_id), K(backup_type));
    }
  }
  return ret;
}

int ObBackupDeviceHelper::get_restore_dest_id_(const uint64_t tenant_id, ObStorageIdMod &mod)
{
  int ret = OB_SUCCESS;
  if (mod.storage_used_mod_ != ObStorageUsedMod::STORAGE_USED_RESTORE) {
    // do nothing
  } else {
    MTL_SWITCH(tenant_id) {
      int64_t dest_id = 0;
      ObTenantRestoreInfoMgr *mgr = MTL(ObTenantRestoreInfoMgr *);
      if (OB_ISNULL(mgr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("mgr should not be null", K(ret));
      } else if (OB_FAIL(mgr->get_restore_dest_id(dest_id))) {
        LOG_WARN("failed to get backup type", K(ret), K(tenant_id));
      } else {
        mod.storage_id_ = dest_id;
        LOG_INFO("get backup dest id", K(mod));
      }
    }
  }
  return ret;
}

}
}

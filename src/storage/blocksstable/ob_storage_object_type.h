/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_
#define OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_

#ifdef OB_BUILD_SHARED_STORAGE
#include "storage/shared_storage/ob_ss_reader_writer.h"
#endif
#include "common/storage/ob_device_common.h"

namespace oceanbase
{
namespace blocksstable
{
class MacroBlockId;
class ObStorageObjectOpt;

#define STI(object_type) (ObStorageObjectTypeInstance::get_instance(object_type))

// Generated enum class
enum class ObStorageObjectType : uint8_t
{
    PRIVATE_DATA_MACRO = 0,
    PRIVATE_META_MACRO = 1,
    SHARED_MINI_DATA_MACRO = 2,
    SHARED_MINI_META_MACRO = 3,
    SHARED_MINOR_DATA_MACRO = 4,
    SHARED_MINOR_META_MACRO = 5,
    SHARED_MAJOR_DATA_MACRO = 6,
    SHARED_MAJOR_META_MACRO = 7,
    TMP_FILE = 8,
    SERVER_META = 9,
    PRIVATE_TABLET_META = 10,
    PRIVATE_SLOG_FILE = 11,
    PRIVATE_CKPT_FILE = 12,
    MAJOR_PREWARM_DATA = 13,
    MAJOR_PREWARM_DATA_INDEX = 14,
    MAJOR_PREWARM_META = 15,
    MAJOR_PREWARM_META_INDEX = 16,
    TENANT_DISK_SPACE_META = 17,
    IS_SHARED_TENANT_DELETED = 18,
    SHARED_MICRO_DATA_MACRO = 19,
    SHARED_MICRO_META_MACRO = 20,
    UNSEALED_REMOTE_SEG_FILE = 21,
    SHARED_MDS_MINI_DATA_MACRO = 22,
    SHARED_MDS_MINI_META_MACRO = 23,
    SHARED_MDS_MINOR_DATA_MACRO = 24,
    SHARED_MDS_MINOR_META_MACRO = 25,
    SHARED_TABLET_META = 41,
    SHARED_TABLET_SUB_META = 76,
    TENANT_ROOT_KEY = 77,
    EXTERNAL_TABLE_FILE = 78,
    MACRO_CACHE_CKPT_DATA = 79,
    MACRO_CACHE_CKPT_META = 80,
    MAX
};
static constexpr uint8_t SS_OBJECT_MAX_TYPE_VAL = static_cast<uint8_t>(ObStorageObjectType::MAX);
const char *get_storage_objet_type_str(const ObStorageObjectType type);

class ObStorageObjectTypeBase
{
public:
  ObStorageObjectTypeBase() : type_(ObStorageObjectType::MAX) {}
  ObStorageObjectTypeBase(ObStorageObjectType type) : type_(type) {}
  virtual ~ObStorageObjectTypeBase() {}

  ObStorageObjectType get_type() const { return type_; }
  const char *get_type_str() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
  //the ObjectType is macro type, true or false
  bool is_macro() const { return is_macro_data() || is_macro_meta(); }
  int get_open_flag_for_write() const;
  int get_open_flag_for_read() const;
  int aio_read(const ObStorageObjectReadInfo &read_info, ObStorageObjectHandle &object_handle) const;
  int aio_write(const ObStorageObjectWriteInfo &write_info, ObStorageObjectHandle &object_handle) const;
  // the ObjectType is macro data type, true or false
  virtual bool is_macro_data() const { return false; }
  // the ObjectType is tenant data type, true or false
  virtual bool is_tenant_data() const { return false; }
  // the ObjectType is macro meta type, true or false
  virtual bool is_macro_meta() const { return false; }
  //the ObjectType is tablet meta type, true or false
  virtual bool is_tablet_meta() const { return false; }
  //the ObjectType is tenant meta type, true or false
  virtual bool is_tenant_meta() const { return false; }
  //the ObjectType is private type, true or false
  virtual bool is_private() const { return false; }
  //the ObjectType is shared type, true or false
  virtual bool is_shared() const { return false; }
  //the ObjectType only store in local cache, true or false
  virtual bool is_pin_local() const { return false; }
  //whether need fsync when write
  virtual bool need_fsync() const { return true; }
  //the ObjectType whether use reserved disk space
  virtual bool use_reserved_disk_space() const { return false; }
  //whether can append write
  virtual bool can_append_write() const { return false; }
  //whether use fd cache when reading local cache file of this type, true or false
  virtual bool is_support_fd_cache() const { return false; }
  //whether path include inner tablet
  virtual bool is_path_include_inner_tablet() const { return false; }
  //the ObjectType only store in remote object storage, true or false
  virtual bool is_direct_read() const { return false; }
  //whether this type of object write through object storage, true or false
  virtual bool is_direct_write() const { return false; }
  //whether this type of object exists overwrite with 'different content', true or false
  virtual bool is_overwrite() const { return false; }
  // whether this type of object can read out of bounds
  virtual bool is_read_out_of_bounds() const { return true; }
  //the ObjectType is major type, true or false
  virtual bool is_major() const { return false; }
  //the ObjectType is mds type, true or false
  virtual bool is_mds() const { return false; }
  //the ObjectType is tmp type, true or false
  virtual bool is_tmp_file() const { return false; }
  //whether this type of object support sn mode, true or false
  virtual bool is_support_sn() const { return false; }
  //the ObjectType which 500 tenant can write
  virtual bool server_tenant_can_have() const { return false; }
  // check macro block id valid
  virtual bool is_valid(const MacroBlockId &file_id) const { return false; }
  virtual bool has_effective_tablet_id() const { return false; }
#ifdef OB_BUILD_SHARED_STORAGE
  // path format reverse, macro id to local path
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const { return OB_NOT_SUPPORTED; }
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const { return OB_NOT_SUPPORTED; }
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const { return OB_NOT_SUPPORTED; }
  //whethe the objecttype has effective tablet id, true or false
  void get_ss_macro_block_type(const MacroBlockId &macro_id, storage::ObSSMacroBlockType &ss_macro_block_type) const;
  int get_macro_cache_type(const uint64_t effective_tablet_id, const bool use_effective_tablet_id,
                           storage::ObSSMacroCacheType &macro_cache_type) const;
#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const { return OB_SUCCESS; }
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const { return OB_SUCCESS; }
  void set_ss_object_first_id_(const uint64_t incarnation_id, const uint64_t column_group_id, MacroBlockId &object_id) const;

protected:
  ObStorageObjectType type_;
};

class ObStorageObjectTypeInstance
{
public:
  static const ObStorageObjectTypeBase &get_instance(ObStorageObjectType type);
};


/**
 * ---------------------------------------ObPrivateDataMacroType----------------------------------------
 */
class ObPrivateDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObPrivateDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::PRIVATE_DATA_MACRO) {}
  virtual ~ObPrivateDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_private() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_support_sn() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObPrivateMetaMacroType----------------------------------------
 */
class ObPrivateMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObPrivateMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::PRIVATE_META_MACRO) {}
  virtual ~ObPrivateMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_private() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_support_sn() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMiniDataMacroType----------------------------------------
 */
class ObSharedMiniDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMiniDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MINI_DATA_MACRO) {}
  virtual ~ObSharedMiniDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMiniMetaMacroType----------------------------------------
 */
class ObSharedMiniMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMiniMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MINI_META_MACRO) {}
  virtual ~ObSharedMiniMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMinorDataMacroType----------------------------------------
 */
class ObSharedMinorDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMinorDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MINOR_DATA_MACRO) {}
  virtual ~ObSharedMinorDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMinorMetaMacroType----------------------------------------
 */
class ObSharedMinorMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMinorMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MINOR_META_MACRO) {}
  virtual ~ObSharedMinorMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMajorDataMacroType----------------------------------------
 */
class ObSharedMajorDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMajorDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO) {}
  virtual ~ObSharedMajorDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_major() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMajorMetaMacroType----------------------------------------
 */
class ObSharedMajorMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMajorMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MAJOR_META_MACRO) {}
  virtual ~ObSharedMajorMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_major() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObTmpFileType----------------------------------------
 */
class ObTmpFileType : public ObStorageObjectTypeBase
{
public:
  ObTmpFileType() : ObStorageObjectTypeBase(ObStorageObjectType::TMP_FILE) {}
  virtual ~ObTmpFileType() {}
  virtual bool is_tenant_data() const{ return true; }
  virtual bool is_private() const { return true; }
  virtual bool need_fsync() const { return false; }
  virtual bool can_append_write() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_tmp_file() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObServerMetaType----------------------------------------
 */
class ObServerMetaType : public ObStorageObjectTypeBase
{
public:
  ObServerMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::SERVER_META) {}
  virtual ~ObServerMetaType() {}
  virtual bool is_private() const { return true; }
  virtual bool is_pin_local() const { return true; }
  virtual bool is_overwrite() const { return true; }
  virtual bool server_tenant_can_have() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObPrivateTabletMetaType----------------------------------------
 */
class ObPrivateTabletMetaType : public ObStorageObjectTypeBase
{
public:
  ObPrivateTabletMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::PRIVATE_TABLET_META) {}
  virtual ~ObPrivateTabletMetaType() {}
  virtual bool is_tablet_meta() const { return true; }
  virtual bool is_private() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObPrivateSlogFileType----------------------------------------
 */
class ObPrivateSlogFileType : public ObStorageObjectTypeBase
{
public:
  ObPrivateSlogFileType() : ObStorageObjectTypeBase(ObStorageObjectType::PRIVATE_SLOG_FILE) {}
  virtual ~ObPrivateSlogFileType() {}
  virtual bool is_tenant_meta() const { return true; }
  virtual bool is_private() const { return true; }
  virtual bool use_reserved_disk_space() const { return true; }
  virtual bool can_append_write() const { return true; }
  virtual bool is_overwrite() const { return true; }
  virtual bool server_tenant_can_have() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;

#endif
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObPrivateCkptFileType----------------------------------------
 */
class ObPrivateCkptFileType : public ObStorageObjectTypeBase
{
public:
  ObPrivateCkptFileType() : ObStorageObjectTypeBase(ObStorageObjectType::PRIVATE_CKPT_FILE) {}
  virtual ~ObPrivateCkptFileType() {}
  virtual bool is_tenant_meta() const { return true; }
  virtual bool is_private() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_sn() const { return true; }
  virtual bool server_tenant_can_have() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObMajorPrewarmDataType----------------------------------------
 */
class ObMajorPrewarmDataType : public ObStorageObjectTypeBase
{
public:
  ObMajorPrewarmDataType() : ObStorageObjectTypeBase(ObStorageObjectType::MAJOR_PREWARM_DATA) {}
  virtual ~ObMajorPrewarmDataType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_major() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObMajorPrewarmDataIndexType----------------------------------------
 */
class ObMajorPrewarmDataIndexType : public ObStorageObjectTypeBase
{
public:
  ObMajorPrewarmDataIndexType() : ObStorageObjectTypeBase(ObStorageObjectType::MAJOR_PREWARM_DATA_INDEX) {}
  virtual ~ObMajorPrewarmDataIndexType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_major() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObMajorPrewarmMetaType----------------------------------------
 */
class ObMajorPrewarmMetaType : public ObStorageObjectTypeBase
{
public:
  ObMajorPrewarmMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::MAJOR_PREWARM_META) {}
  virtual ~ObMajorPrewarmMetaType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_major() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObMajorPrewarmMetaIndexType----------------------------------------
 */
class ObMajorPrewarmMetaIndexType : public ObStorageObjectTypeBase
{
public:
  ObMajorPrewarmMetaIndexType() : ObStorageObjectTypeBase(ObStorageObjectType::MAJOR_PREWARM_META_INDEX) {}
  virtual ~ObMajorPrewarmMetaIndexType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_major() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObTenantDiskSpaceMetaType----------------------------------------
 */
class ObTenantDiskSpaceMetaType : public ObStorageObjectTypeBase
{
public:
  ObTenantDiskSpaceMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::TENANT_DISK_SPACE_META) {}
  virtual ~ObTenantDiskSpaceMetaType() {}
  virtual bool is_private() const { return true; }
  virtual bool is_pin_local() const { return true; }
  virtual bool is_overwrite() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int create_parent_dir(const MacroBlockId &file_id, const uint64_t tenant_id,
                                const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObIsSharedTenantDeletedType----------------------------------------
 */
class ObIsSharedTenantDeletedType : public ObStorageObjectTypeBase
{
public:
  ObIsSharedTenantDeletedType() : ObStorageObjectTypeBase(ObStorageObjectType::IS_SHARED_TENANT_DELETED) {}
  virtual ~ObIsSharedTenantDeletedType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool server_tenant_can_have() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMicroDataMacroType----------------------------------------
 */
class ObSharedMicroDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMicroDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MICRO_DATA_MACRO) {}
  virtual ~ObSharedMicroDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
};

/**
 * ---------------------------------------ObSharedMicroMetaMacroType----------------------------------------
 */
class ObSharedMicroMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMicroMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MICRO_META_MACRO) {}
  virtual ~ObSharedMicroMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
};

/**
 * ---------------------------------------ObUnsealedRemoteSegFileType----------------------------------------
 */
class ObUnsealedRemoteSegFileType : public ObStorageObjectTypeBase
{
public:
  ObUnsealedRemoteSegFileType() : ObStorageObjectTypeBase(ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE) {}
  virtual ~ObUnsealedRemoteSegFileType() {}
  virtual bool is_private() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
};

/**
 * ---------------------------------------ObSharedMdsMiniDataMacroType----------------------------------------
 */
class ObSharedMdsMiniDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMdsMiniDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MDS_MINI_DATA_MACRO) {}
  virtual ~ObSharedMdsMiniDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_mds() const { return true; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMdsMiniMetaMacroType----------------------------------------
 */
class ObSharedMdsMiniMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMdsMiniMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MDS_MINI_META_MACRO) {}
  virtual ~ObSharedMdsMiniMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_mds() const { return true; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMdsMinorDataMacroType----------------------------------------
 */
class ObSharedMdsMinorDataMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMdsMinorDataMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MDS_MINOR_DATA_MACRO) {}
  virtual ~ObSharedMdsMinorDataMacroType() {}
  virtual bool is_macro_data() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_mds() const { return true; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedMdsMinorMetaMacroType----------------------------------------
 */
class ObSharedMdsMinorMetaMacroType : public ObStorageObjectTypeBase
{
public:
  ObSharedMdsMinorMetaMacroType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_MDS_MINOR_META_MACRO) {}
  virtual ~ObSharedMdsMinorMetaMacroType() {}
  virtual bool is_macro_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_support_fd_cache() const { return true; }
  virtual bool is_read_out_of_bounds() const { return false; }
  virtual bool is_mds() const { return true; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedTabletMetaType----------------------------------------
 */
class ObSharedTabletMetaType : public ObStorageObjectTypeBase
{
public:
  ObSharedTabletMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_TABLET_META) {}
  virtual ~ObSharedTabletMetaType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObSharedTabletSubMetaType----------------------------------------
 */
class ObSharedTabletSubMetaType : public ObStorageObjectTypeBase
{
public:
  ObSharedTabletSubMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::SHARED_TABLET_SUB_META) {}
  virtual ~ObSharedTabletSubMetaType() {}
  virtual bool is_tablet_meta() const { return true; }
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_overwrite() const { return true; }
  virtual bool is_path_include_inner_tablet() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;
  virtual bool has_effective_tablet_id() const { return true; }

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;
  virtual int get_effective_tablet_id(const MacroBlockId &macro_id, uint64_t &effective_tablet_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObTenantRootKeyType----------------------------------------
 */
class ObTenantRootKeyType : public ObStorageObjectTypeBase
{
public:
  ObTenantRootKeyType() : ObStorageObjectTypeBase(ObStorageObjectType::TENANT_ROOT_KEY) {}
  virtual ~ObTenantRootKeyType() {}
  virtual bool is_shared() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_overwrite() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObExternalTableFileType----------------------------------------
 */
class ObExternalTableFileType : public ObStorageObjectTypeBase
{
public:
  ObExternalTableFileType() : ObStorageObjectTypeBase(ObStorageObjectType::EXTERNAL_TABLE_FILE) {}
  virtual ~ObExternalTableFileType() {}
  virtual bool is_tenant_data() const{ return true; }
  virtual bool is_private() const { return true; }
  virtual bool is_support_sn() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_local_path_format(char *path, const int64_t length, int64_t &pos,
                                   const MacroBlockId &file_id, const uint64_t tenant_id,
                                   const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;
  virtual int local_path_to_macro_id(const char *path, MacroBlockId &macro_id) const;
  virtual int get_parent_dir(char *path, const int64_t length, int64_t &pos,
                             const MacroBlockId &file_id, const uint64_t tenant_id,
                             const uint64_t tenant_epoch_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObMacroCacheCkptDataType----------------------------------------
 */
class ObMacroCacheCkptDataType : public ObStorageObjectTypeBase
{
public:
  ObMacroCacheCkptDataType() : ObStorageObjectTypeBase(ObStorageObjectType::MACRO_CACHE_CKPT_DATA) {}
  virtual ~ObMacroCacheCkptDataType() {}
  virtual bool is_private() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

/**
 * ---------------------------------------ObMacroCacheCkptMetaType----------------------------------------
 */
class ObMacroCacheCkptMetaType : public ObStorageObjectTypeBase
{
public:
  ObMacroCacheCkptMetaType() : ObStorageObjectTypeBase(ObStorageObjectType::MACRO_CACHE_CKPT_META) {}
  virtual ~ObMacroCacheCkptMetaType() {}
  virtual bool is_private() const { return true; }
  virtual bool is_direct_read() const { return true; }
  virtual bool is_direct_write() const { return true; }
  virtual bool is_valid(const MacroBlockId &file_id) const;

#ifdef OB_BUILD_SHARED_STORAGE
  virtual int to_remote_path_format(char *path, const int64_t length, int64_t &pos,
                                    const MacroBlockId &file_id, const char *object_storage_root_dir,
                                    const uint64_t cluster_id, const uint64_t tenant_id,
                                    const uint64_t tenant_epoch_id, const uint64_t server_id, const int64_t ls_epoch_id) const;

#endif
  virtual int opt_to_string(char *buf, const int64_t buf_len, int64_t &pos, const ObStorageObjectOpt &opt) const;
  virtual int get_object_id(const ObStorageObjectOpt &opt, MacroBlockId &object_id) const;
};

} // end namespace blocksstable
} // end namespace oceanbase

#endif // OCEANBASE_BLOCKSSTABLE_OB_STORAGE_OBJECT_TYPE_H_

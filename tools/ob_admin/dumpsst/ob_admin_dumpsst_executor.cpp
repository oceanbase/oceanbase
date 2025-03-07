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

#include "ob_admin_dumpsst_executor.h"
#include "share/ob_io_device_helper.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "share/ob_tenant_mem_limit_getter.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"
#include "storage/tablet/ob_tablet.h"
#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/meta_store/ob_shared_storage_obj_meta.h"
#include "close_modules/shared_storage/storage/shared_storage/ob_public_block_gc_service.h"
#include "close_modules/shared_storage/storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::rootserver;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase
{
namespace tools
{

ObAdminDumpsstExecutor::ObAdminDumpsstExecutor()
    :ObAdminExecutor(),
     is_quiet_(false),
     cmd_(DUMP_MAX),
     hex_print_(false),
     dump_macro_context_(),
     key_hex_str_(NULL),
     master_key_id_(0),
     io_allocator_("Admin_IOUB")
{
}

ObAdminDumpsstExecutor::~ObAdminDumpsstExecutor()
{
}

int ObAdminDumpsstExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  if (OB_SUCC(parse_cmd(argc, argv))) {
    OB_LOGGER.set_log_level( is_quiet_ ? "ERROR" : "INFO");
    lib::set_memory_limit(96 * 1024 * 1024 * 1024LL);
    lib::set_tenant_memory_limit(500, 96 * 1024 * 1024 * 1024LL);

    if (OB_FAIL(ObKVGlobalCache::get_instance().init(
        &ObTenantMemLimitGetter::get_instance(), 1024L, 512 * 1024 * 1024, 64 * 1024))) {
      STORAGE_LOG(ERROR, "Fail to init kv cache, ", K(ret));
    } else if (OB_FAIL(OB_STORE_CACHE.init(
        storage_env_.index_block_cache_priority_,
        storage_env_.user_block_cache_priority_,
        storage_env_.user_row_cache_priority_,
        storage_env_.fuse_row_cache_priority_,
        storage_env_.bf_cache_priority_,
        storage_env_.bf_cache_miss_count_threshold_,
        storage_env_.storage_meta_cache_priority_))) {
      STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env_.data_dir_));
    } else if (OB_FAIL(load_config())) {
      STORAGE_LOG(WARN, "fail to load config", K(ret));
    } else if (0 == STRLEN(data_dir_)) {
      STRCPY(data_dir_, config_mgr_.get_config().data_dir.str());
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_io())) {
      STORAGE_LOG(WARN, "fail to prepare_io", K(ret));
    } else if (OB_FAIL(prepare_decoder())) {
      STORAGE_LOG(WARN, "fail to prepare_decoder", K(ret));
    } else {
      STORAGE_LOG(INFO, "cmd is", K(cmd_));
      switch (cmd_) {
        case DUMP_SUPER_BLOCK:
          print_super_block();
          break;
        case DUMP_MACRO_DATA:
          dump_macro_block(dump_macro_context_);
          break;
        case PRINT_MACRO_BLOCK:
          print_macro_block();
          break;
        case DUMP_TABLET_META:
          dump_tablet_meta(dump_macro_context_);
          break;
        case DUMP_TABLE_STORE:
          dump_table_store(dump_macro_context_);
          break;
        case DUMP_STORAGE_SCHEMA:
          dump_storage_schema(dump_macro_context_);
          break;
#ifdef OB_BUILD_SHARED_STORAGE
        case DUMP_PREWARM_INDEX:
          dump_prewarm_index(dump_macro_context_);
          break;
        case DUMP_PREWARM_DATA:
          dump_prewarm_data(dump_macro_context_);
          break;
        case DUMP_IS_DELETED_OBJ:
          dump_is_deleted_obj(dump_macro_context_);
          break;
        case DUMP_META_LIST:
        case DUMP_GC_INFO:
          dump_meta_list(dump_macro_context_);
          break;
#endif
        default:
          print_usage();
          exit(1);
      }
    }
  }

  return ret;
}

int ObAdminDumpsstExecutor::parse_macro_id(const char *optarg, ObDumpMacroBlockContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(optarg)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret));
  } else {
    const int64_t len = STRLEN(optarg);
    int64_t delimiter_pos = -1;
    char id_str[20] = "\0";
    for (int64_t i = 0; delimiter_pos < 0 && i < len; ++i) {
      if (optarg[i] == '-') {
        delimiter_pos = i;
      }
    }
    if (delimiter_pos > 0) {
      STRNCPY(id_str, optarg, delimiter_pos);
      id_str[delimiter_pos] = '\0';
      context.first_id_ = strtoll(id_str, NULL, 10);
      STRNCPY(id_str, optarg + delimiter_pos + 1, len - delimiter_pos - 1);
      context.second_id_ = strtoll(id_str, NULL, 10);
    } else {
      context.first_id_ = 0;
      context.second_id_ = strtoll(optarg, NULL, 10);
    }
  }
  return ret;
}

int ObAdminDumpsstExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  int opt = 0;
  const char* opt_string = "d:hf:a:i:n:qxk:m:t:o:s:p:";

  struct option longopts[] = {
    // commands
    { "dump", 1, NULL, 'd' },
    { "help", 0, NULL, 'h' },
    // options
    { "data_dir", 1, NULL, 'f' },
    { "macro-id", 1, NULL, 'a' },
    { "micro-id", 1, NULL, 'i' },
    { "macro-size", 1, NULL, 'n' },
    { "quiet", 0, NULL, 'q' },
    { "hex-print", 0, NULL, 'x' },
    { "master_key", 1, NULL, 'k'},
    { "master_key_id", 1, NULL, 'm'},
    { "tablet_id", 1, NULL, 't'},
    { "object_file", 1, NULL, 'o'},
    { "scn", 1, NULL, 's'},
    { "offset", 1, NULL, 'p'},
    // long options
    { "prewarm_index", 1, NULL, 1000},
    { 0, 0, 0, 0}, // end of array, don't change
  };

  int index = -1;
  while ((opt = getopt_long(argc, argv, opt_string, longopts, &index)) != -1) {
    switch (opt) {
    case 'h': {
      print_usage();
      exit(1);
    }
    case 'd': {
      if (0 == strcmp(optarg, "sb") || 0 == strcmp(optarg, "super_block")) {
        cmd_ = DUMP_SUPER_BLOCK;
      } else if (0 == strcmp(optarg, "mb") || 0 == strcmp(optarg, "macro_block")) {
        cmd_ = DUMP_MACRO_DATA;
      } else if (0 == strcmp(optarg, "pm") || 0 == strcmp(optarg, "print_macro")) {
        cmd_ = PRINT_MACRO_BLOCK;
      } else if (0 == strcmp(optarg, "tablet_meta")) {
        cmd_ = DUMP_TABLET_META;
      } else if (0 == strcmp(optarg, "table_store")) {
        cmd_ = DUMP_TABLE_STORE;
      } else if (0 == strcmp(optarg, "storage_schema")) {
        cmd_ = DUMP_STORAGE_SCHEMA;
#ifdef OB_BUILD_SHARED_STORAGE
      } else if (0 == strcmp(optarg, "prewarm_index")) {
        cmd_ = DUMP_PREWARM_INDEX;
      } else if (0 == strcmp(optarg, "prewarm_data")) {
        cmd_ = DUMP_PREWARM_DATA;
      } else if (0 == strcmp(optarg, "meta_list")) {
        cmd_ = DUMP_META_LIST;
      } else if (0 == strcmp(optarg, "is_deleted_obj")) {
        cmd_ = DUMP_IS_DELETED_OBJ;
      } else if (0 == strcmp(optarg, "gc_info")) {
        cmd_ = DUMP_GC_INFO;
#endif
      } else {
        print_usage();
        exit(1);
      }
      break;
    }
    case 'f': {
      int pret = 0;
      STRCPY(data_dir_, optarg);
      break;
    }
    case 'a': {
      if (OB_FAIL(parse_macro_id(optarg, dump_macro_context_))) {
        STORAGE_LOG(ERROR, "fail to parse macro id", K(ret));
      }
      break;
    }
    case 'i': {
      dump_macro_context_.micro_id_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'n': {
      storage_env_.default_block_size_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'q': {
      is_quiet_ = true;
      break;
    }
    case 'x': {
      hex_print_ = true;
      break;
    }
    case 'm': {
      master_key_id_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'k': {
      key_hex_str_ = optarg;
      break;
    }
    case 't': {
      dump_macro_context_.tablet_id_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'p': {
      dump_macro_context_.offset_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 'o': {
      STRCPY(dump_macro_context_.object_file_path_, optarg);
      break;
    }
    case 's': {
      dump_macro_context_.scn_ = strtoll(optarg, NULL, 10);
      break;
    }
    case 1000: {
      STRCPY(dump_macro_context_.prewarm_index_, optarg);
      break;
    }
    default: {
      print_usage();
      exit(1);
    }
    }
  }
  return ret;
}

void ObAdminDumpsstExecutor::print_super_block()
{
  ObCStringHelper helper;
  fprintf(stdout, "SuperBlock: %s\n", helper.convert(OB_STORAGE_OBJECT_MGR.get_server_super_block()));
}

void ObAdminDumpsstExecutor::print_macro_block()
{
  int ret = OB_NOT_IMPLEMENT;
  STORAGE_LOG(ERROR, "not supported command", K(ret));
}

void ObAdminDumpsstExecutor::dump_macro_block(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObMacroBlockCommonHeader common_header;
  int64_t pos = 0;

  io_allocator_.reuse();
  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_.set_block_index(macro_block_context.second_id_);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.offset_ = 0;
  read_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  read_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;

  STORAGE_LOG(INFO, "begin dump macro block", K(macro_block_context));
  if (OB_UNLIKELY(!macro_block_context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid macro block id", K(macro_block_context), K(ret));
#ifdef OB_BUILD_TDE_SECURITY
  } else if (OB_FAIL(init_master_key_getter())) {
    STORAGE_LOG(ERROR, "failed to init master key getter", K(ret));
    STORAGE_LOG(ERROR, "failed to init macro reader", K(ret));
#endif
  } else if (OB_ISNULL(read_info.buf_ = reinterpret_cast<char*>(io_allocator_.alloc(read_info.size_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(read_info.size_));
  } else {
    char *macro_buf = nullptr;
    int64_t buf_size = 0;
    if (STRLEN(macro_block_context.object_file_path_) > 0) { // macro block object
      ObIOFd fd;
      const int64_t offset = 0;
      const int64_t size = OB_DEFAULT_MACRO_BLOCK_SIZE;
      macro_buf = read_info.buf_;
      if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
        STORAGE_LOG(ERROR, "open file failed", K(macro_block_context));
      } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, buf_size))) {
        STORAGE_LOG(ERROR, "read block failed", K(macro_block_context));
      }
      if (fd.is_valid()) {
        (void) LOCAL_DEVICE_INSTANCE.close(fd);
      }
    } else {
      if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle))) {
        STORAGE_LOG(ERROR, "Fail to read macro block, ", K(ret), K(read_info));
      } else {
        macro_buf = read_info.buf_;
        buf_size = macro_handle.get_data_size();
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(common_header.deserialize(macro_buf, buf_size, pos))) {
      STORAGE_LOG(ERROR, "deserialize common header fail", K(ret), K(pos));
    } else if (OB_FAIL(common_header.check_integrity())) {
      STORAGE_LOG(ERROR, "invalid common header", K(ret), K(common_header));
    } else if (ObMacroBlockCommonHeader::SharedSSTableData == common_header.get_type()) {
      if (OB_FAIL(ObAdminCommonUtils::dump_shared_macro_block(dump_macro_context_, macro_buf, buf_size))) {
        STORAGE_LOG(ERROR, "dump shared block fail", K(ret));
      }
    } else {
      if (OB_FAIL(ObAdminCommonUtils::dump_single_macro_block(dump_macro_context_, macro_buf, buf_size))) {
        STORAGE_LOG(ERROR, "dump single block fail", K(ret));
      }
    }
  }
  macro_handle.reset();
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_macro_block, ret=%s\n", ob_error_name(ret));
  }
}

void ObAdminDumpsstExecutor::dump_tablet_meta(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  if (STRLEN(macro_block_context.object_file_path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "object path is null", K(ret), KP(macro_block_context.object_file_path_));
  } else {
    int64_t pos = 0;
    io_allocator_.reuse();
    char *macro_buf = nullptr;
    int64_t read_size = 0;
    ObIOFd fd;
    const int64_t offset = 0;
    const int64_t size = OB_DEFAULT_MACRO_BLOCK_SIZE;
    ObTablet tablet;
    ObArenaAllocator arena_allocator;
    MacroBlockId tablet_meta_obj_id;
    tablet_meta_obj_id.set_version_v2();
    tablet_meta_obj_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    tablet_meta_obj_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_TABLET_META);
    tablet_meta_obj_id.set_incarnation_id(0);
    tablet_meta_obj_id.set_column_group_id(0);
    tablet_meta_obj_id.set_second_id(88888888); // mock tablet id
    tablet_meta_obj_id.set_third_id(88888888); // mock major snapshot version

    ObMetaDiskAddr disk_addr;

    if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(size));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
      STORAGE_LOG(ERROR, "open file failed", K(macro_block_context));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, read_size))) {
      STORAGE_LOG(ERROR, "read block failed", K(macro_block_context));
    } else if (FALSE_IT(disk_addr.set_block_addr(tablet_meta_obj_id, 0/*offset*/, size, ObMetaDiskAddr::DiskType::RAW_BLOCK))) {
    } else if (FALSE_IT(tablet.set_tablet_addr(disk_addr))) {
    } else if (OB_FAIL(tablet.deserialize_for_replay(arena_allocator, macro_buf, size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize tablet", K(ret), KP(macro_buf), K(size));
    } else {
      ObCStringHelper helper;
      fprintf(stdout, "TabletMeta: %s\n", helper.convert(tablet));
    }

    if (fd.is_valid()) {
      (void) LOCAL_DEVICE_INSTANCE.close(fd);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_tablet_meta, ret=%s\n", ob_error_name(ret));
  }
}

void ObAdminDumpsstExecutor::dump_table_store(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  if (STRLEN(macro_block_context.object_file_path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "object path is null", K(ret), KP(macro_block_context.object_file_path_));
  } else {
    int64_t pos = 0;
    io_allocator_.reuse();
    char *macro_buf = nullptr;
    int64_t read_size = 0;
    ObIOFd fd;
    const int64_t offset = 0;
    const int64_t size = OB_DEFAULT_MACRO_BLOCK_SIZE;

    ObArenaAllocator arena_allocator;
    ObSharedObjectHeader header;
    ObTablet tablet;
    ObTabletTableStore table_store;

    if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(size));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
      STORAGE_LOG(ERROR, "open file failed", K(macro_block_context));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, read_size))) {
      STORAGE_LOG(ERROR, "read block failed", K(macro_block_context));
    } else if (OB_FAIL(header.deserialize(macro_buf, OB_DEFAULT_MACRO_BLOCK_SIZE, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize header", K(ret));
    } else if (OB_FAIL(table_store.deserialize(arena_allocator, tablet, macro_buf, read_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize tablet table store", K(ret));
    } else {
      ObCStringHelper helper;
      fprintf(stdout, "TableStore: %s\n", helper.convert(table_store));
    }

    if (fd.is_valid()) {
      (void) LOCAL_DEVICE_INSTANCE.close(fd);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_table_store, ret=%s\n", ob_error_name(ret));
  }
}

void ObAdminDumpsstExecutor::dump_storage_schema(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  if (STRLEN(macro_block_context.object_file_path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "object path is null", K(ret), KP(macro_block_context.object_file_path_));
  } else {
    int64_t pos = 0;
    io_allocator_.reuse();
    char *macro_buf = nullptr;
    int64_t read_size = 0;
    ObIOFd fd;
    const int64_t offset = macro_block_context.offset_;
    const int64_t size = OB_DEFAULT_MACRO_BLOCK_SIZE;

    ObArenaAllocator arena_allocator;
    ObSharedObjectHeader header;
    ObStorageSchema storage_schema;

    if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(size));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
      STORAGE_LOG(ERROR, "open file failed", K(macro_block_context));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, read_size))) {
      STORAGE_LOG(ERROR, "read block failed", K(macro_block_context));
    } else if (OB_FAIL(header.deserialize(macro_buf, OB_DEFAULT_MACRO_BLOCK_SIZE, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize header", K(ret), K(read_size), K(pos), K(macro_block_context));
    } else if (OB_FAIL(storage_schema.deserialize(arena_allocator, macro_buf, read_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize storage schema", K(ret), K(read_size), K(pos), K(macro_block_context));
    } else {
      ObCStringHelper helper;
      fprintf(stdout, "StorageSchema: %s\n", helper.convert(storage_schema));
    }

    if (fd.is_valid()) {
      (void) LOCAL_DEVICE_INSTANCE.close(fd);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_storage_schema, ret=%s\n", ob_error_name(ret));
  }
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObAdminDumpsstExecutor::do_dump_prewarm_index(
    const char *path,
    storage::ObHotTabletInfoIndex &index)
{
  int ret = OB_SUCCESS;
  if (STRLEN(path) == 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "file path is null", KR(ret), K(path));
  } else {
    io_allocator_.reuse();
    int64_t pos = 0;
    int64_t read_size = 0;
    char *macro_buf = nullptr;
    ObIOFd fd;
    const int64_t offset = 0;
    const int64_t size = DEFAULT_MACRO_BLOCK_SIZE;
    if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(ERROR, "failed to alloc macro read info buffer", KR(ret), K(size));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(path, O_RDONLY, 0, fd))) {
      STORAGE_LOG(ERROR, "fail to open file", KR(ret), K(path));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, read_size))) {
      STORAGE_LOG(ERROR, "fail to read file", KR(ret), K(fd), K(offset), K(size), KP(macro_buf));
    } else if (OB_FAIL(index.deserialize(macro_buf, read_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize prewarm index", KR(ret), K(read_size), K(pos), KP(macro_buf));
    }

    if (fd.is_valid()) {
      (void) LOCAL_DEVICE_INSTANCE.close(fd);
    }
  }
  return ret;
}

void ObAdminDumpsstExecutor::dump_prewarm_index(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  ObHotTabletInfoIndex index;
  if (STRLEN(macro_block_context.object_file_path_) == 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "object path is null", K(ret), K_(macro_block_context.object_file_path));
  } else if (OB_FAIL(do_dump_prewarm_index(macro_block_context.object_file_path_, index))) {
    STORAGE_LOG(ERROR, "fail to parse prewarm_index", KR(ret), K_(macro_block_context.object_file_path));
  } else {
    ObCStringHelper helper;
    fprintf(stdout, "ObHotTabletInfoIndex: %s\n", helper.convert(index));
  }

  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_prewarm_index, ret=%s\n", ob_error_name(ret));
  }
}

void ObAdminDumpsstExecutor::dump_prewarm_data(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  ObIOFd fd;
  ObHotTabletInfoIndex index;
  if (STRLEN(macro_block_context.object_file_path_) == 0 || STRLEN(macro_block_context.prewarm_index_) == 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "file path is null", KR(ret), K_(macro_block_context.object_file_path), K_(macro_block_context.prewarm_index));
  } else if (OB_FAIL(do_dump_prewarm_index(macro_block_context.prewarm_index_, index))) {
    STORAGE_LOG(ERROR, "fail to parse prewarm_index", KR(ret), K_(macro_block_context.object_file_path));
  } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
    STORAGE_LOG(ERROR, "fail to open file", KR(ret), K_(macro_block_context.object_file_path));
  } else {
    ObCStringHelper helper;
    fprintf(stdout, "ObHotTabletInfoIndex: %s\n\n", helper.convert(index));
    const int64_t cnt = index.sizes_.count();
    int64_t cur_offset = 0;
    for (int64_t i = 0; (i < cnt) && OB_SUCC(ret); ++i) {
      io_allocator_.reuse();
      char *buf = nullptr;
      const int64_t size = index.sizes_.at(i);
      int64_t read_size = 0;
      int64_t pos = 0;
      ObHotTabletInfo hot_tablet_info;
      if (OB_ISNULL(buf = static_cast<char *>(io_allocator_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(ERROR, "fail to alloc memory", KR(ret), K(size));
      } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, cur_offset, size, buf, read_size))) {
        STORAGE_LOG(ERROR, "fail to read data", KR(ret), K(fd), K(cur_offset), K(size), KP(buf));
      } else if (OB_UNLIKELY(size != read_size)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "read size is wrong", KR(ret), K(size), K(read_size));
      } else if (OB_FAIL(hot_tablet_info.deserialize(buf, size, pos))) {
        STORAGE_LOG(ERROR, "fail to serialize", KR(ret), K(size), K(pos));
      } else {
        ObCStringHelper helper;
        fprintf(stdout, "i=%ld\n ObHotTabletInfo=%s\n", i, helper.convert(hot_tablet_info));
      }
      cur_offset += size;
    }
  }
  if (fd.is_valid()) {
    (void)LOCAL_DEVICE_INSTANCE.close(fd);
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_prewarm_data, ret=%s\n", ob_error_name(ret));
  }
}

void ObAdminDumpsstExecutor::dump_is_deleted_obj(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  if (STRLEN(macro_block_context.object_file_path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "object path is null", K(ret), KP(macro_block_context.object_file_path_));
  } else {
    io_allocator_.reuse();
    int64_t pos = 0;
    int64_t read_size = 0;
    char *macro_buf = nullptr;
    ObIOFd fd;
    const int64_t offset = 0;
    const int64_t size = DEFAULT_MACRO_BLOCK_SIZE;
    ObIsDeletedObj is_deleted_obj;

    if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(size));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
      STORAGE_LOG(ERROR, "open file failed", K(macro_block_context));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, read_size))) {
      STORAGE_LOG(ERROR, "read block failed", K(macro_block_context));
    } else if (OB_FAIL(is_deleted_obj.deserialize(macro_buf, read_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize is_deleted_obj", K(ret), K(read_size), K(pos), K(macro_block_context));
    } else {
      ObCStringHelper helper;
      fprintf(stdout, "ObIsDeletedObj: %s\n", helper.convert(is_deleted_obj));
    }

    if (fd.is_valid()) {
      (void) LOCAL_DEVICE_INSTANCE.close(fd);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_is_deleted_obj, ret=%s\n", ob_error_name(ret));
  }
}

void ObAdminDumpsstExecutor::dump_meta_list(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  if (STRLEN(macro_block_context.object_file_path_) <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "object path is null", K(ret), KP(macro_block_context.object_file_path_));
  } else {
    io_allocator_.reuse();
    int64_t pos = 0;
    int64_t read_size = 0;
    char *macro_buf = nullptr;
    ObIOFd fd;
    const int64_t offset = 0;
    const int64_t size = DEFAULT_MACRO_BLOCK_SIZE;
    ObGCTabletMetaInfoList meta_info;

    if (OB_ISNULL(macro_buf = reinterpret_cast<char*>(io_allocator_.alloc(size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "failed to alloc macro read info buffer", K(ret), K(size));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.open(macro_block_context.object_file_path_, O_RDONLY, 0, fd))) {
      STORAGE_LOG(ERROR, "open file failed", K(macro_block_context));
    } else if (OB_FAIL(LOCAL_DEVICE_INSTANCE.pread(fd, offset, size, macro_buf, read_size))) {
      STORAGE_LOG(ERROR, "read block failed", K(macro_block_context));
    } else if (OB_FAIL(meta_info.deserialize(macro_buf, read_size, pos))) {
      STORAGE_LOG(ERROR, "fail to deserialize GCTabletMetaInfoList", K(ret), K(read_size), K(pos), K(macro_block_context));
    } else {
      ObCStringHelper helper;
      fprintf(stdout, "ObGCTabletMetaInfoList: %s\n", helper.convert(meta_info));
    }

    if (fd.is_valid()) {
      (void) LOCAL_DEVICE_INSTANCE.close(fd);
    }
  }
  if (OB_FAIL(ret)) {
    fprintf(stderr, "fail to dump_meta_list, ret=%s\n", ob_error_name(ret));
  }
}
#endif

#ifdef OB_BUILD_TDE_SECURITY
int ObAdminDumpsstExecutor::init_master_key_getter()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(share::ObMasterKeyGetter::instance().init(NULL, nullptr))) {
    STORAGE_LOG(WARN, "fail to init master key", K(ret));
  }
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  }
  // if (OB_SUCC(ret) && OB_NOT_NULL(key_hex_str_)) {
  //   int64_t len = STRLEN(key_hex_str_);
  //   char master_key[share::OB_MAX_MASTER_KEY_LENGTH + 1];
  //   if (OB_FAIL(common::hex_to_cstr(key_hex_str_, len,
  //     master_key, share::OB_MAX_MASTER_KEY_LENGTH + 1))) {
  //     STORAGE_LOG(WARN, "fail to hex to cstr", K(ret));
  //   } else if (OB_FAIL(share::ObMasterKeyGetter::instance().set_master_key(
  //       master_key_id_, master_key, STRLEN(master_key)))) {
  //     STORAGE_LOG(WARN, "fail to set master key", K(ret));
  //   }
  // }
  return ret;
}
#endif

void ObAdminDumpsstExecutor::print_usage()
{
  printf("\n");
  printf("Usage: dumpsst command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-d,--dump", "dump, args: [super_block|print_macro|macro_block|macro_meta]");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-f,--data_dir", "data dir");
  printf(HELP_FMT, "-a,--macro-id", "macro block id");
  printf(HELP_FMT, "-i,--micro-id", "micro block id, -1 means all micro blocks");
  printf(HELP_FMT, "-n,--macro-size", "macro block size, in bytes");
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-x,--hex-print", "print obj value in hex mode");
  printf(HELP_FMT, "-k,--master_key", "master key, hex str");
  printf(HELP_FMT, "-m,--master_key_id", "master key id");
  printf(HELP_FMT, "-t,--tablet_id", "tablet id");
  printf(HELP_FMT, "-s,--scn", "macro block logical version");
  printf(HELP_FMT, "-o,--object_file", "object file path");
  printf(HELP_FMT, "-p,--offset", "data offset in object file");

  printf("SN mode commands:\n");
  printf("  dump all rows in data macro block: \n");
  printf("\tob_admin dumpsst -d macro_block -f block_file_path -a macro_id\n");
  printf("  dump specified block in the small sstable macro block: \n");
  printf("\tob_admin dumpsst -d macro_block -f block_file_path -a macro_id -t tablet_id -s logical_version\n\n");

  printf("SS mode commands:\n");
  printf("  dump data macro block: \n");
  printf("\tob_admin dumpsst -d macro_block -o object_file_path\n");
  printf("  dump tablet meta: \n");
  printf("\tob_admin dumpsst -d tablet_meta -o object_file_path\n");
  printf("  dump table store: \n");
  printf("\tob_admin dumpsst -d table_store -o object_file_path\n");
  printf("  dump storage schema: \n");
  printf("\tob_admin dumpsst -d storage_schema -o object_file_path -p offset\n");
  printf("  dump is_deleted_obj: \n");
  printf("\tob_admin dumpsst -d is_deleted_obj -o object_file_path\n");
  printf("  dump meta_list: \n");
  printf("\tob_admin dumpsst -d meta_list -o object_file_path\n");
  printf("  dump gc_info: \n");
  printf("\tob_admin dumpsst -d gc_info -o object_file_path\n");
  printf("  dump prewarm_index: \n");
  printf("\tob_admin dumpsst -d prewarm_index -o object_file_path\n");
  printf("  dump prewarm_data: \n");
  printf("\tob_admin dumpsst -d prewarm_data -o object_file_path --prewarm_index index_file\n");
}
} //namespace tools
} //namespace oceanbase

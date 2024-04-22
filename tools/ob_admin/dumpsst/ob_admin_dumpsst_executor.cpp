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

#include "common/log/ob_log_constants.h"
#include "ob_admin_dumpsst_executor.h"
#include "observer/ob_server_struct.h"
#include "share/ob_io_device_helper.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#include "share/ob_tenant_mem_limit_getter.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/ob_file_system_router.h"
#include "storage/blocksstable/ob_storage_cache_suite.h"

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
    } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.init(data_dir_,
        config_mgr_.get_config().cluster.str(),
        config_mgr_.get_config().cluster_id.get_value(),
        config_mgr_.get_config().zone.str(),
        GCTX.self_addr()))) {
      STORAGE_LOG(WARN, "fail to init file system router", K(ret));
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
  const char* opt_string = "d:hf:a:i:n:qxk:m:t:s:";

  struct option longopts[] = {
    // commands
    { "dump", 1, NULL, 'd' },
    { "help", 0, NULL, 'h' },
    // options
    { "file", 1, NULL, 'f' },
    { "macro-id", 1, NULL, 'a' },
    { "micro-id", 1, NULL, 'i' },
    { "macro-size", 1, NULL, 'n' },
    { "quiet", 0, NULL, 'q' },
    { "hex-print", 0, NULL, 'x' },
    { "master_key", 1, NULL, 'k'},
    { "master_key_id", 1, NULL, 'm'},
    { "tablet_id", 1, NULL, 't'},
    { "scn", 1, NULL, 's'}
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
    case 's': {
      dump_macro_context_.scn_ = strtoll(optarg, NULL, 10);
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
  fprintf(stdout, "SuperBlock: %s\n", to_cstring(OB_SERVER_BLOCK_MGR.get_server_super_block()));
}

void ObAdminDumpsstExecutor::print_macro_block()
{
  int ret = OB_NOT_IMPLEMENT;
  STORAGE_LOG(ERROR, "not supported command", K(ret));
}

int ObAdminDumpsstExecutor::dump_single_macro_block(const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  ObSSTableDataBlockReader macro_reader;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(macro_reader.init(buf, size, hex_print_))) {
    STORAGE_LOG(ERROR, "failed to init macro reader", K(ret), KP(buf), K(size));
  } else if (OB_FAIL(macro_reader.dump(dump_macro_context_.tablet_id_, dump_macro_context_.scn_))) {
    STORAGE_LOG(ERROR, "failed dump macro block", K(ret), KP(buf), K(size), K(dump_macro_context_));
  }

  return ret;
}

int ObAdminDumpsstExecutor::dump_shared_macro_block(const char* buf, const int64_t size)
{
  int ret = OB_SUCCESS;
  const int64_t aligned_size = 4096;
  int64_t current_page_offset = aligned_size;
  if (OB_ISNULL(buf) || size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid argument", K(ret), KP(buf), K(size));
  } else {
    while (OB_SUCC(ret) && current_page_offset < size) {
      ObMacroBlockCommonHeader common_header;
      int64_t pos = 0;
      const char* cur_buf = buf + current_page_offset;
      const int64_t cur_size = size - current_page_offset;
      if (OB_FAIL(common_header.deserialize(cur_buf, cur_size, pos))) {
        if (OB_DESERIALIZE_ERROR != ret) {
          STORAGE_LOG(ERROR, "deserialize common header fail", K(ret), K(pos));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_FAIL(common_header.check_integrity())) {
        STORAGE_LOG(ERROR, "invalid common header", K(ret), K(common_header));
      } else if OB_FAIL(dump_single_macro_block(cur_buf,
          common_header.get_header_size() + common_header.get_payload_size())) {
        STORAGE_LOG(ERROR, "dump single block fail", K(ret), K(common_header));
      } else {
        current_page_offset = upper_align(
            current_page_offset + common_header.get_header_size() + common_header.get_payload_size(),
            aligned_size);
      }
    }
  }
  STORAGE_LOG(INFO, "dump shared block finish", K(ret), K(current_page_offset));
  return ret;
}

int ObAdminDumpsstExecutor::dump_macro_block(const ObDumpMacroBlockContext &macro_block_context)
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
    if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle))) {
      STORAGE_LOG(ERROR, "Fail to read macro block, ", K(ret), K(read_info));
    } else if (OB_FAIL(common_header.deserialize(read_info.buf_, macro_handle.get_data_size(), pos))) {
      STORAGE_LOG(ERROR, "deserialize common header fail", K(ret), K(pos));
    } else if (OB_FAIL(common_header.check_integrity())) {
      STORAGE_LOG(ERROR, "invalid common header", K(ret), K(common_header));
    } else if (ObMacroBlockCommonHeader::SharedSSTableData == common_header.get_type()) {
      if (OB_FAIL(dump_shared_macro_block(read_info.buf_, macro_handle.get_data_size()))) {
        STORAGE_LOG(ERROR, "dump shared block fail", K(ret));
      }
    } else {
      if (OB_FAIL(dump_single_macro_block(read_info.buf_, macro_handle.get_data_size()))) {
        STORAGE_LOG(ERROR, "dump single block fail", K(ret));
      }
    }
  }

  macro_handle.reset();
  STORAGE_LOG(INFO, "finish dump macro block", K(common_header), K(macro_block_context));
  return ret;
}

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
  printf(HELP_FMT, "-f,--data-file-name", "data file path");
  printf(HELP_FMT, "-a,--macro-id", "macro block id");
  printf(HELP_FMT, "-i,--micro-id", "micro block id, -1 means all micro blocks");
  printf(HELP_FMT, "-n,--macro-size", "macro block size, in bytes");
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-x,--hex-print", "print obj value in hex mode");
  printf(HELP_FMT, "-k,--master_key", "master key, hex str");
  printf(HELP_FMT, "-m,--master_key_id", "master key id");
  printf(HELP_FMT, "-t,--tablet_id", "tablet id");
  printf(HELP_FMT, "-s,--logical_version", "macro block logical version");
  printf("samples:\n");
  printf("  dump all rows in macro: \n");
  printf("\tob_admin -d macro_block -f block_file_path -a macro_id\n");
  printf("  dump specified block in the shared block: \n");
  printf("\tob_admin -d macro_block -f block_file_path -a macro_id -t tablet_id -s logical_version\n");
}
} //namespace tools
} //namespace oceanbase

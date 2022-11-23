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
     in_csv_(false),
     cmd_(DUMP_MAX),
     skip_log_replay_(false),
     hex_print_(false),
     dump_macro_context_(),
     key_hex_str_(NULL),
     master_key_id_(0)
{
}

ObAdminDumpsstExecutor::~ObAdminDumpsstExecutor()
{
}

int ObAdminDumpsstExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  if (OB_SUCC(parse_cmd(argc, argv))) {
    OB_LOGGER.set_log_level( is_quiet_ ? "ERROR" : "INFO");
    lib::set_memory_limit(96 * 1024 * 1024 * 1024LL);
    lib::set_tenant_memory_limit(500, 96 * 1024 * 1024 * 1024LL);

    if (skip_log_replay_ && DUMP_MACRO_DATA != cmd_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(ERROR, "Only dump macro block can skip slog replay, ", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().init(
        &ObTenantMemLimitGetter::get_instance(), 1024L, 512 * 1024 * 1024, 64 * 1024))) {
      STORAGE_LOG(ERROR, "Fail to init kv cache, ", K(ret));
    } else if (OB_FAIL(OB_STORE_CACHE.init(
        storage_env_.index_block_cache_priority_,
        storage_env_.user_block_cache_priority_,
        storage_env_.user_row_cache_priority_,
        storage_env_.fuse_row_cache_priority_,
        storage_env_.bf_cache_priority_,
        storage_env_.bf_cache_miss_count_threshold_))) {
      STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env_.data_dir_));
    } else if (OB_FAIL(load_config())) {
      STORAGE_LOG(WARN, "fail to load config", K(ret));
    } else if (0 == STRLEN(data_dir_)) {
      STRCPY(data_dir_, config_mgr_.get_config().data_dir.str());
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(prepare_io())) {
      STORAGE_LOG(WARN, "fail to prepare_io", K(ret));
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
        case DUMP_SSTABLE:
          dump_sstable();
          break;
        case PRINT_MACRO_BLOCK:
          print_macro_block();
          break;
        case DUMP_SSTABLE_META:
          dump_sstable_meta();
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
  const char* opt_string = "d:hf:a:i:n:qt:sxk:m:";

  struct option longopts[] = {
    // commands
    { "dump", 1, NULL, 'd' },
    { "help", 0, NULL, 'h' },
    // options
    { "file", 1, NULL, 'f' },
    { "macro-id", 1, NULL, 'a' },
    { "micro-id", 1, NULL, 'i' },
    { "macro-size", 1, NULL, 'n' },
    { "csv", 0, NULL, 0 },
    { "table_key", 1, NULL, 't'},
    { "quiet", 0, NULL, 'q' },
    { "skip_replay", 0, NULL, 's' },
    { "hex-print", 0, NULL, 'x' },
    { "master_key", 1, NULL, 'k'},
    { "master_key_id", 1, NULL, 'm'}
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
      } else if (0 == strcmp(optarg, "sst") || 0 == strcmp(optarg, "sstable")) {
        cmd_ = DUMP_SSTABLE;
      } else if (0 == strcmp(optarg, "sstm") || 0 == strcmp(optarg, "sstable_meta")) {
        cmd_ = DUMP_SSTABLE_META;
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
    case 's': {
      skip_log_replay_ = true;
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
    case 0: {
      std::string name = longopts[index].name;
      if ("csv" == name) {
        in_csv_ = true;
      }
      break;
    }
    case 't': {
      // if (OB_FAIL(parse_table_key(optarg, table_key_))) {
      //   printf("failed to parse table key\n");
      //   print_usage();
      //   exit(1);
      // }
      // break;
      print_usage();
      exit(1);
    }
    case 'k': {
      key_hex_str_ = optarg;
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

int ObAdminDumpsstExecutor::dump_macro_block(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  ObMacroBlockHandle macro_handle;
  ObSSTableDataBlockReader macro_reader;

  ObMacroBlockReadInfo read_info;
  read_info.macro_block_id_.set_block_index(macro_block_context.second_id_);
  read_info.io_desc_.set_category(ObIOCategory::SYS_IO);
  read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_READ);
  read_info.offset_ = 0;
  read_info.size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  

  STORAGE_LOG(INFO, "begin dump macro block", K(macro_block_context));
  if (OB_UNLIKELY(!macro_block_context.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid macro block id", K(macro_block_context), K(ret));
  } else if (OB_FAIL(ObBlockManager::read_block(read_info, macro_handle))) {
    STORAGE_LOG(ERROR, "Fail to read macro block, ", K(ret), K(read_info));
  } else if (OB_FAIL(macro_reader.init(macro_handle.get_buffer(), macro_handle.get_data_size(), hex_print_))) {
    STORAGE_LOG(ERROR, "failed to init macro reader", K(read_info), K(ret));
  } else if (OB_FAIL(macro_reader.dump())) {
    STORAGE_LOG(ERROR, "failed dump macro block", K(ret));
  }

  macro_handle.reset();
  STORAGE_LOG(INFO, "finish dump macro block", K(macro_block_context));
  return ret;
}


void ObAdminDumpsstExecutor::dump_sstable()
{
  int ret = OB_NOT_IMPLEMENT;
  STORAGE_LOG(ERROR, "not supported command", K(ret));
  // adapt later if needed
  /*
  ObSSTable *sstable = NULL;

  if (OB_FAIL(replay_slog_to_get_sstable(sstable))) {
    STORAGE_LOG(ERROR, "failed to acquire table", K_(table_key), K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "sstable is null", K(ret));
  } else {
    const ObIArray<blocksstable::MacroBlockId> &macro_array = sstable->get_macro_block_ids();
    ObDumpMacroBlockContext context;
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_array.count(); ++i) {
      context.second_id_ = macro_array.at(i).block_index();
      if (OB_FAIL(dump_macro_block(context))) {
        STORAGE_LOG(ERROR, "failed to dump macro block", K(context), K(ret));
      }
    }
    if (OB_SUCC(ret) && sstable->has_lob_macro_blocks()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < sstable->get_lob_macro_block_ids().count(); ++i) {
        context.second_id_ = sstable->get_lob_macro_block_ids().at(i).block_index();
        if (OB_FAIL(dump_macro_block(context))) {
          STORAGE_LOG(ERROR, "Failed to dump lob macro block", K(context), K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    printf("failed to dump sstable, ret = %d\n", ret);
  }
  */
}

void ObAdminDumpsstExecutor::dump_sstable_meta()
{
  int ret = OB_NOT_IMPLEMENT;
  STORAGE_LOG(ERROR, "not supported command", K(ret));
  /*
  int ret = OB_SUCCESS;
  ObTableHandle handle;
  ObSSTable *sstable = NULL;
  if (OB_FAIL(ObPartitionService::get_instance().acquire_sstable(table_key_, handle))) {
    STORAGE_LOG(ERROR, "fail to acquire table", K(ret), K(table_key_));
  } else if (OB_FAIL(handle.get_sstable(sstable))) {
    STORAGE_LOG(ERROR, "fail to get sstable", K(ret));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, sstable must not be NULL", K(ret));
  } else {
    char buf[1024];
    const ObSSTableMeta &meta = sstable->get_meta();
    snprintf(buf, 1024, "table_id=%ld, tablet_id=%ld", table_key_.table_id_, table_key_.tablet_id_.id());
    PrintHelper::print_dump_title(buf);
    PrintHelper::print_dump_line("index_id", meta.index_id_);
    PrintHelper::print_dump_line("row_count", meta.row_count_);
    PrintHelper::print_dump_line("occupy_size", meta.occupy_size_);
    PrintHelper::print_dump_line("data_checksum", meta.data_checksum_);
    PrintHelper::print_dump_line("row_checksum", meta.row_checksum_);
    PrintHelper::print_dump_line("macro_block_count", meta.macro_block_count_);
    PrintHelper::print_dump_line("use_old_macro_block_count", meta.use_old_macro_block_count_);
    PrintHelper::print_dump_line("column_count", meta.column_cnt_);
    PrintHelper::print_dump_line("schema_version", meta.schema_version_);
    PrintHelper::print_dump_line("progressive_merge_start_version", meta.progressive_merge_start_version_);
    PrintHelper::print_dump_line("progressive_merge_end_version", meta.progressive_merge_end_version_);
    PrintHelper::print_dump_line("checksum_method", meta.checksum_method_);
    meta.column_metas_.to_string(buf, 1024);
    PrintHelper::print_dump_line("column_metas", buf);
    meta.new_column_metas_.to_string(buf, 1024);
    PrintHelper::print_dump_line("new_column_metas", buf);
  }
  */
}

void ObAdminDumpsstExecutor::print_usage()
{
  printf("\n");
  printf("Usage: dumpsst command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-d,--dump", "dump, args: [super_block|print_macro|macro_block|macro_meta|sstable|sstable_meta]");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-f,--data-file-name", "data file path or the ofs address");
  printf(HELP_FMT, "-a,--macro-id", "macro block id");
  printf(HELP_FMT, "-i,--micro-id", "micro block id, -1 means all micro blocks");
  printf(HELP_FMT, "-n,--macro-size", "macro block size, in bytes");
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-t,--table-key", "table key: table_type,table_id:partition_id,index_id,base_version:multi_version_start:snapshot_version,start_log_ts:end_log_ts:max_log_ts,major_version");
  printf(HELP_FMT, "-s,--skip_replay", "skip slog replay, only work for macro_block mode");
  printf(HELP_FMT, "-x,--hex-print", "print obj value in hex mode");
  printf(HELP_FMT, "-k,--master_key", "master key, hex str");
  printf(HELP_FMT, "-m,--master_key_id", "master key id");
  printf("samples:\n");
  printf("  dump all rows in macro: \n");
  printf("\tob_admin -d macro_block -f block_file_path -a macro_id\n");
}
} //namespace tools
} //namespace oceanbase

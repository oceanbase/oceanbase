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
#include "ob_admin_dumpsst_utils.h"
#include "storage/blocksstable/ob_store_file.h"
#include "storage/ob_partition_service.h"
#include "storage/ob_table_mgr.h"
#include "storage/ob_sstable.h"
#include "storage/ob_tenant_config_mgr.h"
#include "storage/ob_tenant_config_meta_block_reader.h"
#include "storage/blocksstable/ob_micro_block_scanner.h"
#include "storage/blocksstable/ob_micro_block_index_reader.h"
#include "storage/blocksstable/ob_sstable_printer.h"
#include "observer/ob_server_struct.h"
#include "storage/ob_file_system_util.h"
#include "storage/ob_pg_storage.h"
#include "lib/net/ob_net_util.h"

using namespace oceanbase::common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
#define HELP_FMT "\t%-30s%-12s\n"

namespace oceanbase
{
namespace tools
{

ObAdminDumpsstExecutor::ObAdminDumpsstExecutor()
  : storage_env_(),
    is_quiet_(false),
    in_csv_(false),
    cmd_(DUMP_MAX),
    skip_log_replay_(false),
    dump_macro_context_(),
    reload_config_(ObServerConfig::get_instance(), GCTX), config_mgr_(ObServerConfig::get_instance(), reload_config_)
{
  storage_env_.data_dir_ = data_dir_;
  storage_env_.sstable_dir_ = sstable_dir_;
  storage_env_.default_block_size_ = 2 * 1024 * 1024;

  storage_env_.log_spec_.log_dir_ = slog_dir_;
  storage_env_.log_spec_.max_log_size_ = 256 << 20;

  storage_env_.clog_dir_ = clog_dir_;
  storage_env_.ilog_dir_ = ilog_dir_;
  storage_env_.clog_shm_path_ = clog_shm_path_;
  storage_env_.ilog_shm_path_ = ilog_shm_path_;

  storage_env_.bf_cache_miss_count_threshold_ = 0;
  storage_env_.bf_cache_priority_ = 1;
  storage_env_.index_cache_priority_ = 10;
  storage_env_.user_block_cache_priority_ = 1;
  storage_env_.user_row_cache_priority_ = 1;
  storage_env_.fuse_row_cache_priority_ = 1;
  storage_env_.clog_cache_priority_ = 1;
  storage_env_.index_clog_cache_priority_ = 1;
  storage_env_.ethernet_speed_ = 10000;

  GCONF.datafile_size = 128 * 1024 * 1024;
}

ObAdminDumpsstExecutor::~ObAdminDumpsstExecutor()
{
  ObStoreFileSystemWrapper::destroy();
}

int ObAdminDumpsstExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  ObPartitionService partition_service;
  UNUSED(argc);
  UNUSED(argv);

  if (OB_SUCC(parse_cmd(argc, argv))) {
    if (is_quiet_) {
      OB_LOGGER.set_log_level("ERROR");
    } else {
      OB_LOGGER.set_log_level("INFO");
    }

    lib::set_memory_limit(96 * 1024 * 1024 * 1024LL);
    lib::set_tenant_memory_limit(500, 96 * 1024 * 1024 * 1024LL);
    char ip_port_str[MAX_PATH_SIZE] = {};
    if (skip_log_replay_ && DUMP_MACRO_DATA != cmd_) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(ERROR, "Only dump macro block can skip slog replay, ", K(ret));
    } else if (OB_FAIL(ObIOManager::get_instance().init(2 * 1024 * 1024 * 1024LL))) {
      STORAGE_LOG(ERROR, "Fail to init io manager, ", K(ret));
    } else if (OB_FAIL(ObKVGlobalCache::get_instance().init(1024L, 512 * 1024 * 1024, 64 * 1024))) {
      STORAGE_LOG(ERROR, "Fail to init kv cache, ", K(ret));
    } else if (OB_FAIL(OB_STORE_CACHE.init(
        storage_env_.index_cache_priority_,
        storage_env_.user_block_cache_priority_,
        storage_env_.user_row_cache_priority_,
        storage_env_.fuse_row_cache_priority_,
        storage_env_.bf_cache_priority_,
        storage_env_.bf_cache_miss_count_threshold_))) {
      STORAGE_LOG(WARN, "Fail to init OB_STORE_CACHE, ", K(ret), K(storage_env_.data_dir_));
    } else if (OB_FAIL(load_config())) {
      STORAGE_LOG(WARN, "fail to load config", K(ret));
    } else if (OB_FAIL(GCTX.self_addr_.ip_port_to_string(ip_port_str, MAX_PATH_SIZE))) {
      STORAGE_LOG(WARN, "get server ip port fail", K(ret), K(GCTX.self_addr_));
    } else if (OB_FAIL(OB_FILE_SYSTEM_ROUTER.init(data_dir_,
        config_mgr_.get_config().cluster.str(),
        config_mgr_.get_config().cluster_id.get_value(),
        config_mgr_.get_config().zone.str(),
        ip_port_str))) {
      STORAGE_LOG(WARN, "fail to init file system", K(ret));
    } else if (OB_FAIL(ObStoreFileSystemWrapper::init(storage_env_, partition_service))) {
      STORAGE_LOG(ERROR, "Fail to init store file, ", K(ret));
    } else {
      STORAGE_LOG(INFO, "cmd is", K(cmd_));
      switch (cmd_) {
      case DUMP_MACRO_DATA:
      case DUMP_MACRO_META:
      case DUMP_SSTABLE:
      case DUMP_SSTABLE_META:
      case PRINT_MACRO_BLOCK:
        if (OB_FAIL(open_store_file())) {
          STORAGE_LOG(ERROR, "failed to open store file", K(ret));
        }
        break;
      default:
        break;
      }
      if (OB_SUCC(ret)) {
        switch (cmd_) {
          case DUMP_SUPER_BLOCK:
            print_super_block();
            break;
          case DUMP_MACRO_DATA:
            dump_macro_block(dump_macro_context_);
            break;
          case DUMP_MACRO_META:
            print_macro_meta();
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
  }

  return ret;
}

int ObAdminDumpsstExecutor::load_config()
{
  int ret = OB_SUCCESS;
  // set dump path
  const char *dump_path = "etc/observer.config.bin";
  config_mgr_.set_dump_path(dump_path);
  if (OB_FAIL(config_mgr_.load_config())) {
    STORAGE_LOG(WARN, "fail to load config", K(ret));
  } else {
    ObServerConfig &config = config_mgr_.get_config();
    int32_t local_port = static_cast<int32_t>(config.rpc_port);
    int32_t ipv4 = ntohl(obsys::ObNetUtil::get_local_addr_ipv4(config.devname));
    GCTX.self_addr_.set_ipv4_addr(ipv4, local_port);
  }
  return ret;
}

int ObAdminDumpsstExecutor::parse_cmd(int argc, char *argv[])
{
  int ret = OB_SUCCESS;

  int opt = 0;
  const char* opt_string = "d:hf:a:i:n:qt:s:";

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
      } else if (0 == strcmp(optarg, "mm") || 0 == strcmp(optarg, "macro_meta")) {
        cmd_ = DUMP_MACRO_META;
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
      strcpy(data_dir_, optarg);
      pret = snprintf(slog_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/slog", data_dir_);
      if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
        ret = OB_BUF_NOT_ENOUGH;
        STORAGE_LOG(ERROR, "concatenate slog path fail", K(ret));
      }
      if (OB_SUCC(ret)) {
        pret = snprintf(sstable_dir_, OB_MAX_FILE_NAME_LENGTH, "%s/sstable", data_dir_);
        if (pret < 0 || pret >= OB_MAX_FILE_NAME_LENGTH) {
          ret = OB_BUF_NOT_ENOUGH;
          STORAGE_LOG(ERROR, "concatenate slog path fail", K(ret));
        }
      }
      break;
    }
    case 'a': {
      dump_macro_context_.macro_id_ = strtoll(optarg, NULL, 10);
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
    case 0: {
      std::string name = longopts[index].name;
      if ("csv" == name) {
        in_csv_ = true;
      }
      break;
    }
    case 't': {
      if (OB_FAIL(parse_table_key(optarg, table_key_))) {
        printf("failed to parse table key\n");
        print_usage();
        exit(1);
      }
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
  fprintf(stdout, "SuperBlock: %s\n", to_cstring(OB_FILE_SYSTEM.get_server_super_block()));
}

void ObAdminDumpsstExecutor::print_macro_block()
{
  int ret = OB_SUCCESS;
  STORAGE_LOG(ERROR, "not supported command", K(ret));
}

int ObAdminDumpsstExecutor::dump_macro_block(const ObDumpMacroBlockContext &macro_block_context)
{
  int ret = OB_SUCCESS;
  ObStorageFileWithRef file_with_ref;
  ObStorageFileHandle file_handle;
  ObStorageFile *file = nullptr;


  ObMacroBlockHandle macro_handle;
  ObSSTableDataBlockReader macro_reader;

  ObMacroBlockReadInfo read_info;
  ObMacroBlockCtx block_ctx;
  block_ctx.sstable_block_id_.macro_block_id_.set_local_block_id(macro_block_context.macro_id_);
  block_ctx.sstable_block_id_.macro_block_id_in_files_ = 0;
  read_info.macro_block_ctx_ = &block_ctx;
  read_info.io_desc_.category_ = SYS_IO;
  read_info.offset_ = 0;
  read_info.size_ = ObStoreFileSystemWrapper::get_instance().get_macro_block_size();

  STORAGE_LOG(INFO, "begin dump macro block", K(macro_block_context));
  if (OB_FAIL(file_handle.assign(OB_FILE_SYSTEM.get_server_root_handle()))) {
    STORAGE_LOG(WARN, "fail to assign file handle", K(ret));
  } else if (!macro_block_context.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(ERROR, "invalid macro block id", K(macro_block_context), K(ret));
  } else if (OB_ISNULL(file = file_handle.get_storage_file())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(ERROR, "storage file is null", K(ret), K(file_handle));
  } else if (FALSE_IT(macro_handle.set_file(file))) {
  } else if (OB_FAIL(file->read_block(read_info, macro_handle))) {
    STORAGE_LOG(ERROR, "Fail to read macro block, ", K(ret), K(read_info));
  } else if (OB_FAIL(macro_reader.init(macro_handle.get_buffer(), macro_handle.get_data_size()))) {
    STORAGE_LOG(ERROR, "failed to get macro meta", K(block_ctx), K(ret));
    STORAGE_LOG(ERROR, "failed to init macro reader", K(ret));
  } else if (OB_FAIL(macro_reader.dump())) {
    STORAGE_LOG(ERROR, "failed dump macro block", K(ret));
  }

  macro_handle.reset();
  if (nullptr != file) {
    OB_FILE_SYSTEM.free_file(file);
  }
  STORAGE_LOG(INFO, "finish dump macro block", K(macro_block_context));
  return ret;
}

int ObAdminDumpsstExecutor::open_store_file()
{
  int ret = OB_SUCCESS;
  return ret;
}

void ObAdminDumpsstExecutor::print_macro_meta()
{
  // int ret = OB_SUCCESS;
  const ObMacroBlockMeta *meta = NULL;
  MacroBlockId macro_id(dump_macro_context_.macro_id_);
  PrintHelper::print_dump_title("Macro Meta");
  PrintHelper::print_dump_line("macro_block_id", dump_macro_context_.macro_id_);
  PrintHelper::print_dump_line("attr", meta->attr_);
  PrintHelper::print_dump_line("data_version", meta->data_version_);
  PrintHelper::print_dump_line("column_number", meta->column_number_);
  PrintHelper::print_dump_line("rowkey_column_number", meta->rowkey_column_number_);
  PrintHelper::print_dump_line("column_index_scale", meta->column_index_scale_);
  PrintHelper::print_dump_line("row_store_type", meta->row_store_type_);
  PrintHelper::print_dump_line("row_count", meta->row_count_);
  PrintHelper::print_dump_line("occupy_size", meta->occupy_size_);
  PrintHelper::print_dump_line("data_checksum", meta->data_checksum_);
  PrintHelper::print_dump_line("micro_block_count", meta->micro_block_count_);
  PrintHelper::print_dump_line("micro_block_data_offset", meta->micro_block_data_offset_);
  PrintHelper::print_dump_line("micro_block_index_offset", meta->micro_block_index_offset_);
  PrintHelper::print_dump_line("micro_block_endkey_offset", meta->micro_block_endkey_offset_);
  PrintHelper::print_dump_line("compressor", meta->compressor_);
  PrintHelper::print_dump_line("table_id", meta->table_id_);
  PrintHelper::print_dump_line("data_seq", meta->data_seq_);
  PrintHelper::print_dump_line("schema_version", meta->schema_version_);
  PrintHelper::print_dump_line("snapshot_version", meta->snapshot_version_);
  PrintHelper::print_dump_line("schema_rowkey_col_cnt", meta->schema_rowkey_col_cnt_);
  PrintHelper::print_dump_line("row_count_delta", meta->row_count_delta_);
  PrintHelper::print_dump_line("macro_block_deletion_flag", meta->macro_block_deletion_flag_);
  PrintHelper::print_dump_list_start("column_id_array");
  for (int64_t i = 0; i < meta->column_number_; ++i) {
    PrintHelper::print_dump_list_value(meta->column_id_array_[i], i == meta->rowkey_column_number_ - 1);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_dump_list_start("column_type_array");
  for (int64_t i = 0; i < meta->column_number_; ++i) {
    PrintHelper::print_dump_list_value(to_cstring(meta->column_type_array_[i]), i == meta->rowkey_column_number_ - 1);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_dump_list_start("column_checksum");
  for (int64_t i = 0; i < meta->column_number_; ++i) {
    PrintHelper::print_dump_list_value(meta->column_checksum_[i], i== meta->rowkey_column_number_ - 1);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_dump_list_start("end_key");
  for (int64_t i = 0; i < meta->rowkey_column_number_; ++i) {
    PrintHelper::print_cell(meta->endkey_[i], in_csv_);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_dump_list_start("column_order");
  for (int64_t i = 0; i < meta->column_number_; ++i) {
    PrintHelper::print_dump_list_value(meta->column_order_array_[i], i == meta->column_number_ - 1);
  }
  PrintHelper::print_dump_list_end();
  PrintHelper::print_end_line();
}

void ObAdminDumpsstExecutor::dump_sstable()
{
  int ret = OB_SUCCESS;
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
      context.macro_id_ = macro_array.at(i).block_index();
      if (OB_FAIL(dump_macro_block(context))) {
        STORAGE_LOG(ERROR, "failed to dump macro block", K(context), K(ret));
      }
    }
    if (OB_SUCC(ret) && sstable->has_lob_macro_blocks()) {
      for (int64_t i = 0; OB_SUCC(ret) && i < sstable->get_lob_macro_block_ids().count(); ++i) {
        context.macro_id_ = sstable->get_lob_macro_block_ids().at(i).block_index();
        if (OB_FAIL(dump_macro_block(context))) {
          STORAGE_LOG(ERROR, "Failed to dump lob macro block", K(context), K(ret));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    printf("failed to dump sstable, ret = %d\n", ret);
  }
}

void ObAdminDumpsstExecutor::dump_sstable_meta()
{
  int ret = OB_SUCCESS;
  ObSSTable *sstable = NULL;
  if (OB_FAIL(replay_slog_to_get_sstable(sstable))) {
    STORAGE_LOG(ERROR, "fail to acquire table", K(ret), K(table_key_));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "error unexpected, sstable must not be NULL", K(ret));
  } else {
    char buf[1024];
    const ObSSTableMeta &meta = sstable->get_meta();
    snprintf(buf, 1024, "table_id=%ld, partition_id=%ld", table_key_.table_id_, table_key_.pkey_.get_partition_id());
    PrintHelper::print_dump_title(buf);
    PrintHelper::print_dump_line("index_id", meta.index_id_);
    PrintHelper::print_dump_line("row_count", meta.row_count_);
    PrintHelper::print_dump_line("occupy_size", meta.occupy_size_);
    PrintHelper::print_dump_line("data_checksum", meta.data_checksum_);
    PrintHelper::print_dump_line("row_checksum", meta.row_checksum_);
    PrintHelper::print_dump_line("macro_block_count", meta.macro_block_count_);
    PrintHelper::print_dump_line("use_old_macro_block_count", meta.use_old_macro_block_count_);
    PrintHelper::print_dump_line("column_count", meta.column_cnt_);
    PrintHelper::print_dump_line("lob_macro_block_count", meta.lob_macro_block_count_);
    PrintHelper::print_dump_line("lob_use_old_macro_block_count", meta.lob_use_old_macro_block_count_);
    PrintHelper::print_dump_line("schema_version", meta.schema_version_);
    PrintHelper::print_dump_line("progressive_merge_start_version", meta.progressive_merge_start_version_);
    PrintHelper::print_dump_line("progressive_merge_end_version", meta.progressive_merge_end_version_);
    PrintHelper::print_dump_line("checksum_method", meta.checksum_method_);
    meta.column_metas_.to_string(buf, 1024);
    PrintHelper::print_dump_line("column_metas", buf);
    meta.new_column_metas_.to_string(buf, 1024);
    PrintHelper::print_dump_line("new_column_metas", buf);
  }
}

int ObAdminDumpsstExecutor::replay_slog_to_get_sstable(ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  ObBaseFileMgr file_mgr;
  ObPartitionMetaRedoModule pg_mgr;
  ObPartitionComponentFactory cp_fty;

  if (OB_FAIL(file_mgr.init())) {
    STORAGE_LOG(WARN, "fail to init file mgr", K(ret));
  } else if (OB_FAIL(pg_mgr.init(&cp_fty,
                                 &share::schema::ObMultiVersionSchemaService::get_instance(),
                                 &file_mgr))) {
    STORAGE_LOG(WARN, "fail to init pg mgr", K(ret));
  } else {
    ObAdminSlogReplayer replayer(file_mgr, pg_mgr, slog_dir_);
    replayer.init();
    if (OB_FAIL(replayer.replay_slog())) {
      STORAGE_LOG(WARN, "fail to replay slog", K(ret));
    } else if (OB_FAIL(replayer.get_sstable(table_key_, sstable))) {
      STORAGE_LOG(WARN, "fail to get sstable", K(ret));
    }
  }

  return ret;
}


ObAdminSlogReplayer::ObAdminSlogReplayer(
    ObBaseFileMgr &file_mgr,
    ObPartitionMetaRedoModule &pg_mgr,
    char *slog_dir)
    :
        svr_addr_(GCTX.self_addr_),
        svr_root_(),
        file_mgr_(file_mgr),
        pg_mgr_(pg_mgr),
        super_block_(),
        pg_meta_reader_(),
        tenant_file_reader_(),
        slog_dir_(slog_dir)
{
}

ObAdminSlogReplayer::~ObAdminSlogReplayer()
{
  reset();
}

int ObAdminSlogReplayer::init()
{
  int ret = OB_SUCCESS;
  common::ObLogCursor checkpoint;

  blocksstable::ObServerSuperBlock super_block;
  ObStorageFileWithRef file_with_ref;
  ObStorageFileHandle file_handle;
  ObStorageFile *file = nullptr;

  ObTenantMutilAllocatorMgr::get_instance().init();

  if (OB_FAIL(file_handle.assign(OB_FILE_SYSTEM.get_server_root_handle()))) {
    STORAGE_LOG(WARN, "fail to assign file handle", K(ret));
  } else if (OB_FAIL(file_handle.get_storage_file()->read_super_block(super_block_))) {
    STORAGE_LOG(WARN, "failed to read super block", K(ret));
  }
  return ret;
}

int ObAdminSlogReplayer::replay_slog()
{
  int ret = OB_SUCCESS;
  common::ObLogCursor checkpoint;

  if (OB_UNLIKELY(!svr_addr_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid source server", K(ret), K(svr_addr_));
  } else if (OB_FAIL(OB_FILE_SYSTEM.alloc_file(svr_root_.file_))) {
    STORAGE_LOG(WARN, "fail to alloc storage file", K(ret));
  } else if (OB_FAIL(svr_root_.file_->init(svr_addr_,
                                           ObStorageFile::FileType::SERVER_ROOT))) {
    STORAGE_LOG(WARN, "fail to init original server root file", K(ret), K(svr_addr_));
  } else if (OB_FAIL(svr_root_.file_->open(ObFileSystemUtil::WRITE_FLAGS))) {
    STORAGE_LOG(WARN, "fail to open pg file", K(ret));
  } else if (OB_FAIL(svr_root_.file_->read_super_block(super_block_))) {
    STORAGE_LOG(WARN, "failed to read super block", K(ret));
  } else if (OB_FAIL(read_checkpoint_and_replay_log(checkpoint))) {
    STORAGE_LOG(WARN, "fail to read checkpoint and replay log", K(ret));
  } else {
    STORAGE_LOG(INFO, "success to replay log", K(ret));
  }

  return ret;
}

void ObAdminSlogReplayer::reset()
{
  tenant_file_reader_.reset();
  super_block_.reset();
  if (OB_NOT_NULL(svr_root_.file_)) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = OB_FILE_SYSTEM.free_file(svr_root_.file_))) {
      STORAGE_LOG(WARN, "fail to free svr root", K(tmp_ret));
    }
  }
}

int ObAdminSlogReplayer::get_sstable(
    storage::ObITable::TableKey table_key,
    storage::ObSSTable *&sstable)
{
  int ret = OB_SUCCESS;
  sstable = NULL;
  storage::ObIPartitionGroupGuard guard;
  storage::ObPGPartitionGuard pg_guard;
  ObTablesHandle tables_handle;
  common::ObArray<ObSSTable *> sstables;
  if (OB_FAIL(pg_mgr_.get_partition(table_key.pkey_, guard)) || NULL == guard.get_partition_group()) {
    STORAGE_LOG(WARN, "invalid partition", K(ret), K(table_key));
  } else if (OB_FAIL(guard.get_partition_group()->get_pg_storage().get_all_sstables(tables_handle))) {
    STORAGE_LOG(WARN, "failed to get tables", K(ret), K(table_key));
  } else if (OB_FAIL(tables_handle.get_all_sstables(sstables))) {
    STORAGE_LOG(WARN, "failed to get all sstables", K(ret), K(table_key));
  } else {
    for (int i = 0; i < sstables.count(); ++i) {
      if (sstables.at(i)->get_key() == table_key) {
        sstable = sstables.at(i);
        break;
      }
    }
  }
  if (OB_SUCC(ret) && OB_NOT_NULL(sstable)) {
    STORAGE_LOG(INFO, "success to get sstable", K(ret), K(table_key), KPC(sstable));
  } else {
    STORAGE_LOG(WARN, "sstable not found", K(ret), K(table_key), K(sstables));
  }
  return ret;
}

int ObAdminSlogReplayer::replay_server_slog(
    const char *slog_dir,
    const common::ObLogCursor &replay_start_cursor,
    const ObStorageLogCommittedTransGetter &committed_trans_getter)
{
  int ret = OB_SUCCESS;
  ObStorageLogReplayer log_replayer;
  ServerMetaSLogFilter filter_before_parse;
  ObStorageFileHandle svr_root_handle;
  if (OB_ISNULL(slog_dir) || OB_UNLIKELY(!replay_start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(slog_dir), K(replay_start_cursor));
  } else if (FALSE_IT(svr_root_handle.set_storage_file_with_ref(svr_root_))) {
  } else if (OB_FAIL(tenant_file_reader_.read_checkpoint(super_block_.content_.super_block_meta_,
      file_mgr_, svr_root_handle))) {
    STORAGE_LOG(WARN, "fail to read tenant file checkpoint", K(ret));
  } else if (OB_FAIL(log_replayer.init(slog_dir, &filter_before_parse))) {
    STORAGE_LOG(WARN, "fail to init log replayer", K(ret));
  } else if (OB_FAIL(log_replayer.register_redo_module(OB_REDO_LOG_TENANT_FILE, &file_mgr_))) {
    STORAGE_LOG(WARN, "fail to register redo module", K(ret));
  } else if (OB_FAIL(log_replayer.replay(replay_start_cursor, committed_trans_getter))) {
    STORAGE_LOG(WARN, "fail to replay log replayer", K(ret));
  } else if (OB_FAIL(file_mgr_.replay_open_files(svr_addr_))) {
    STORAGE_LOG(WARN, "fail to replay open files", K(ret));
  } else {
    STORAGE_LOG(INFO, "finish replay server slog");
  }
  return ret;
}

int ObAdminSlogReplayer::replay_pg_slog(const char *slog_dir,
    const common::ObLogCursor &replay_start_cursor,
    const blocksstable::ObStorageLogCommittedTransGetter &committed_trans_getter,
    common::ObLogCursor &checkpoint)
{
  int ret = OB_SUCCESS;
  ObStorageLogReplayer log_replayer;
  PGMetaSLogFilter filter_before_parse(file_mgr_);
  if (OB_ISNULL(slog_dir) || OB_UNLIKELY(!replay_start_cursor.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(slog_dir), K(replay_start_cursor));
  } else if (OB_FAIL(pg_meta_reader_.read_checkpoint(file_mgr_, pg_mgr_))) {
    STORAGE_LOG(WARN, "fail to read checkpoint", K(ret));
  } else if (OB_FAIL(log_replayer.init(slog_dir, &filter_before_parse))) {
    STORAGE_LOG(WARN, "fail to init log replayer", K(ret));
  } else if (OB_FAIL(log_replayer.register_redo_module(OB_REDO_LOG_PARTITION, &pg_mgr_))) {
    STORAGE_LOG(WARN, "fail to register redo module", K(ret));
  } else if (OB_FAIL(log_replayer.replay(replay_start_cursor, committed_trans_getter))) {
    STORAGE_LOG(WARN, "fail to replay log replayer", K(ret));
  } else if (OB_FAIL(log_replayer.get_active_cursor(checkpoint))){
    STORAGE_LOG(WARN, "fail to get active cursor", K(ret));
  } else {
    STORAGE_LOG(INFO, "finish replay pg slog", K(checkpoint), K(pg_mgr_.get_pg_mgr().get_total_partition_count()));
  }
  return ret;
}

int ObAdminSlogReplayer::read_checkpoint_and_replay_log(common::ObLogCursor &checkpoint)
{
  int ret = OB_SUCCESS;
  char dir_name[MAX_PATH_SIZE];
  common::ObLogCursor replay_start_cursor;
  ObStorageLogCommittedTransGetter committed_trans_getter;
  ObServerWorkingDir dir(svr_addr_, ObServerWorkingDir::DirStatus::RECOVERING);

  if (OB_FAIL(dir.to_path_string(dir_name, sizeof(dir_name)))) {
    STORAGE_LOG(WARN, "get server ip port fail", K(ret), K(dir));
  } else if (super_block_.content_.replay_start_point_.is_valid()) {
    replay_start_cursor = super_block_.content_.replay_start_point_;
  } else {
    replay_start_cursor.file_id_ = 1;
    replay_start_cursor.log_id_ = 0;
    replay_start_cursor.offset_ = 0;
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "read checkpoint begin", K(slog_dir_), K(svr_addr_), K(replay_start_cursor));
    if (OB_FAIL(committed_trans_getter.init(slog_dir_, replay_start_cursor))) {
      STORAGE_LOG(WARN, "fail to init committed trans getter", K(ret));
    } else if (OB_FAIL(replay_server_slog(slog_dir_, replay_start_cursor, committed_trans_getter))) {
      STORAGE_LOG(WARN, "fail to replay tenant file slog", K(ret));
    } else if (OB_FAIL(replay_pg_slog(slog_dir_, replay_start_cursor, committed_trans_getter, checkpoint))) {
      STORAGE_LOG(WARN, "fail to replay pg slog", K(ret));
    } else if (OB_FAIL(file_mgr_.replay_over())) {
      STORAGE_LOG(WARN, "fail to replay over file mgr", K(ret));
    }
    STORAGE_LOG(INFO, "read checkpoint end", K(ret), K_(slog_dir), K(svr_addr_),
        K(replay_start_cursor), K(checkpoint));
  }
  return ret;
}

int ObAdminSlogReplayer::ServerMetaSLogFilter::filter(
    const ObISLogFilter::Param &param,
    bool &is_filtered) const
{
  int ret = OB_SUCCESS;
  enum ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  is_filtered = false;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);
  is_filtered = (OB_REDO_LOG_TENANT_FILE != main_type);
  return ret;
}

int ObAdminSlogReplayer::PGMetaSLogFilter::filter(
    const ObISLogFilter::Param &param,
    bool &is_filtered) const
{
  int ret = OB_SUCCESS;
  enum ObRedoLogMainType main_type = OB_REDO_LOG_MAX;
  int32_t sub_type = 0;
  is_filtered = false;
  ObIRedoModule::parse_subcmd(param.subcmd_, main_type, sub_type);
  is_filtered = (OB_REDO_LOG_PARTITION != main_type);
  if (!is_filtered) {
    if (OB_VIRTUAL_DATA_FILE_ID == param.attr_.data_file_id_) {
      is_filtered = false;
    } else {
      ObTenantFileKey file_key(param.attr_.tenant_id_, param.attr_.data_file_id_);
      ObTenantFileInfo file_info;
      if (OB_FAIL(file_mgr_.get_tenant_file_info(file_key, file_info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          is_filtered = true;
          ret = OB_SUCCESS;
        }
      } else {
        is_filtered = !file_info.is_normal_status();
      }
    }
  }
  return ret;
}

void ObAdminDumpsstExecutor::print_usage()
{
  printf("\n");
  printf("Usage: dumpsst command [command args] [options]\n");
  printf("commands:\n");
  printf(HELP_FMT, "-d,--dump", "dump, args: [super_block|print_macro|macro_block|macro_meta|sstable|sstable_meta]");
  printf(HELP_FMT, "-h,--help", "display this message.");

  printf("options:\n");
  printf(HELP_FMT, "-f,--data-file-name", "data file path");
  printf(HELP_FMT, "-a,--macro-id", "macro block index");
  printf(HELP_FMT, "-i,--micro-id", "micro block id, -1 means all micro blocks");
  printf(HELP_FMT, "-n,--macro-size", "macro block size, in bytes");
  printf(HELP_FMT, "-q,--quiet", "log level: ERROR");
  printf(HELP_FMT, "-t,--table-key", "table key: table_type,table_id:partition_id,index_id,base_version:multi_version_start:snapshot_version,start_log_ts:end_log_ts:max_log_ts,major_version");
  printf(HELP_FMT, "-s,--skip_replay", "skip slog replay, only work for macro_block mode");
  printf("samples:\n");
  printf("  dump all metas and rows in macro: \n");
  printf("\tob_admin dumpsst -d pm -f block_file_path -a macro_id -i micro_id\n");
  printf("  dump all rows in macro: \n");
  printf("\tob_admin dumpsst -d macro_block -f block_file_path -a macro_id -i micro_id: dump rows in macro\n");
}
} //namespace tools
} //namespace oceanbase

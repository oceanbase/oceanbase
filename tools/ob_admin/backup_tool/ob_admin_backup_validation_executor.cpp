/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan
 * PubL v2. You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY
 * KIND, EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO
 * NON-INFRINGEMENT, MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE. See the
 * Mulan PubL v2 for more details.
 */

#include "ob_admin_backup_validation_executor.h"
#include "ob_admin_backup_validation_task.h"
#include "ob_admin_backup_validation_util.h"
#include "share/scheduler/ob_dag_scheduler.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_master_key_getter.h"
#endif
#define HELP_FMT "\t%-30s%-12s\n"
namespace oceanbase
{
namespace tools
{
ObAdminBackupValidationExecutor::ObAdminBackupValidationExecutor(
    ObAdminBackupValidationType validation_type)
    : is_inited_(false), validation_type_(validation_type), ctx_(nullptr), allocator_("ObAdmBakVal")
{
}
ObAdminBackupValidationExecutor::~ObAdminBackupValidationExecutor()
{
  if (OB_NOT_NULL(ctx_)) {
    ctx_->~ObAdminBackupValidationCtx();
    ctx_ = nullptr;
  }
  STORAGE_LOG(INFO, "Memory hold", K(get_tenant_memory_hold(OB_SERVER_TENANT_ID)));
}

int ObAdminBackupValidationExecutor::execute(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  lib::set_memory_limit(4 * 1024 * 1024 * 1024LL);
  lib::set_tenant_memory_limit(OB_SERVER_TENANT_ID, 4 * 1024 * 1024 * 1024LL);

#ifdef OB_BUILD_TDE_SECURITY
  // The root_key is set to ensure the successful parsing of backup_dest,
  // because there is encryption and decryption of access_key
  share::ObMasterKeyGetter::instance().init(NULL);
  ObMasterKeyGetter::instance().set_root_key(OB_SYS_TENANT_ID, obrpc::RootKeyType::DEFAULT,
                                             ObString());
#endif
  if (OB_FAIL(prepare_backup_validation())) {
    STORAGE_LOG(WARN, "failed to prepare backup validation", K(ret));
  } else if (OB_FAIL(init_())) {
    STORAGE_LOG(WARN, "failed to init", K(ret));
  } else if (OB_FAIL(parse_cmd_(argc, argv))) {
    STORAGE_LOG(WARN, "failed to parse cmd", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::BACKUPPIECE_VALIDATION
             && OB_FAIL(schedule_data_backup_validation_())) {
    STORAGE_LOG(WARN, "failed to scheudle data backup validation", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::BACKUPPIECE_VALIDATION
             && OB_FAIL(wait_data_backup_validation_())) {
    STORAGE_LOG(WARN, "failed to wait data backup validation", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::BACKUPSET_VALIDATION
             && OB_FAIL(schedule_log_archive_validation_())) {
    STORAGE_LOG(WARN, "failed to scheudle log archive validation", K(ret));
  } else if (ctx_->validation_type_ != ObAdminBackupValidationType::BACKUPSET_VALIDATION
             && OB_FAIL(wait_log_archive_validation_())) {
    STORAGE_LOG(WARN, "failed to wait log archive validation", K(ret));
  }

  return ret;
}
int ObAdminBackupValidationExecutor::init_()
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
  share::ObDagWarningHistoryManager *dag_warning_history_manager
      = MTL(share::ObDagWarningHistoryManager *);

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "init twice", K(ret));
  } else if (OB_ISNULL(dag_scheduler) || OB_ISNULL(dag_warning_history_manager)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag_scheduler or dag_warning_history_manager is null", K(ret));
  } else if (OB_FAIL(dag_scheduler->init(MTL_ID()))) {
    STORAGE_LOG(WARN, "failed to init dag scheduler", K(ret));
  } else if (OB_FAIL(dag_warning_history_manager->init(true, MTL_ID(), "ObAdmBakVal"))) {
    STORAGE_LOG(WARN, "failed to init dag warning history manager", K(ret));
  } else if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(ObAdminBackupValidationCtx)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
  } else if (FALSE_IT(ctx_ = new (alc_ptr) ObAdminBackupValidationCtx(allocator_))) {
  } else if (OB_FAIL(ctx_->init())) {
    STORAGE_LOG(WARN, "failed to init ctx", K(ret));
  } else {
    ctx_->validation_type_ = validation_type_;
    ctx_->mb_check_level_ = blocksstable::ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL;
    is_inited_ = true;
    printf("\nOB backup validation tool\n");
    fflush(stdout);
  }
  return ret;
}
int ObAdminBackupValidationExecutor::parse_cmd_(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  void *alc_ptr = nullptr;
  int opt = 0;
  int index = -1;
  int64_t concurrency_ = 1;  // default concurrency
  int64_t memory_limit_ = 4; // default memory limit
  common::ObArenaAllocator tmp_allocator("ObAdmBakVal");

  struct option longopts[] = {{"help", 0, NULL, 0},
                              {"log_archive_dest", 1, NULL, 1},
                              {"data_backup_dest", 1, NULL, 2},
                              {"backuppiece_path", 1, NULL, 3},
                              {"backupset_path", 1, NULL, 4},
                              {"backuppiece_key", 1, NULL, 5},
                              {"backupset_id", 1, NULL, 6},
                              {"check_level", 1, NULL, 8},
                              {"concurrency", 1, NULL, 9},
                              {"io_bandwidth_limit", 1, NULL, 10},
                              {"memory_limit", 1, NULL, 11},
                              {NULL, 0, NULL, 0}};

  char *log_archive_dest_str = nullptr;
  char *data_backup_dest_str = nullptr;
  char *backuppiece_path_str = nullptr;
  char *backupset_path_str = nullptr;
  char *backuppiece_key_str = nullptr;
  char *backupset_id_str = nullptr;
  common::ObArray<common::ObString> str_array;

  while (OB_SUCC(ret) && -1 != (opt = getopt_long(argc, argv, "", longopts, &index))) {
    if (OB_ISNULL(optarg)) {
      print_usage_();
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", K(ret));
    } else {
      switch (opt) {
      case 0: {
        print_usage_();
        exit(0);
      }
      case 1: {
        if (ObAdminBackupValidationType::BACKUPSET_VALIDATION == validation_type_
            || OB_NOT_NULL(backuppiece_path_str) || OB_NOT_NULL(backuppiece_key_str)) {
          print_usage_();
          exit(1);
        } else if (OB_ISNULL(log_archive_dest_str = static_cast<char *>(
                                 tmp_allocator.alloc(common::OB_MAX_URI_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(share::ObBackupDest)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (FALSE_IT(ctx_->log_archive_dest_ = new (alc_ptr) share::ObBackupDest())) {
        } else if (OB_FAIL(databuff_printf(log_archive_dest_str, common::OB_MAX_URI_LENGTH, "%s",
                                           optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        } else if (OB_FAIL(ctx_->log_archive_dest_->set(log_archive_dest_str))) {
          STORAGE_LOG(WARN, "failed to set log archive dest", K(ret));
        }
        break;
      }
      case 2: {
        if (ObAdminBackupValidationType::BACKUPPIECE_VALIDATION == validation_type_
            || OB_NOT_NULL(backupset_path_str) || OB_NOT_NULL(backupset_id_str)) {
          print_usage_();
          exit(1);
        } else if (OB_ISNULL(data_backup_dest_str = static_cast<char *>(
                                 tmp_allocator.alloc(common::OB_MAX_URI_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(share::ObBackupDest)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (FALSE_IT(ctx_->data_backup_dest_ = new (alc_ptr) share::ObBackupDest())) {
        } else if (OB_FAIL(databuff_printf(data_backup_dest_str, common::OB_MAX_URI_LENGTH, "%s",
                                           optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        } else if (OB_FAIL(ctx_->data_backup_dest_->set(data_backup_dest_str))) {
          STORAGE_LOG(WARN, "failed to set data backup dest", K(ret));
        }
        break;
      }
      case 3: {
        if (ObAdminBackupValidationType::BACKUPSET_VALIDATION == validation_type_
            || OB_NOT_NULL(log_archive_dest_str) || OB_NOT_NULL(backuppiece_key_str)) {
          print_usage_();
          exit(1);
        } else if (OB_ISNULL(backuppiece_path_str = static_cast<char *>(
                                 tmp_allocator.alloc(common::OB_MAX_URI_LENGTH * 128)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (OB_FAIL(databuff_printf(backuppiece_path_str, common::OB_MAX_URI_LENGTH * 128,
                                           "%s", optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        } else if (FALSE_IT(str_array.reset())) {
        } else if (OB_FAIL(ObAdminBackupValidationUtil::convert_comma_separated_string_to_array(
                       backuppiece_path_str, str_array))) {
          STORAGE_LOG(WARN, "failed to convert_comma_spearated_string_to_array", K(ret));
        } else {
          FOREACH_X(str, str_array, OB_SUCC(ret))
          {
            share::ObBackupDest *backup_piece_path = nullptr;
            if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(share::ObBackupDest)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
            } else if (FALSE_IT(backup_piece_path = new (alc_ptr) share::ObBackupDest())) {
            } else if (OB_FAIL(backup_piece_path->set(*str))) {
              STORAGE_LOG(WARN, "failed to set backup set path", K(ret));
            } else if (OB_FAIL(ctx_->backup_piece_path_array_.push_back(backup_piece_path))) {
              STORAGE_LOG(WARN, "failed to push back backup_piece_path", K(ret));
            }
          }
        }
        break;
      }
      case 4: {
        if (ObAdminBackupValidationType::BACKUPPIECE_VALIDATION == validation_type_
            || OB_NOT_NULL(data_backup_dest_str) || OB_NOT_NULL(backupset_id_str)) {
          print_usage_();
          exit(1);
        } else if (OB_ISNULL(backupset_path_str = static_cast<char *>(
                                 tmp_allocator.alloc(common::OB_MAX_URI_LENGTH * 128)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (OB_FAIL(databuff_printf(backupset_path_str, common::OB_MAX_URI_LENGTH * 128,
                                           "%s", optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        } else if (FALSE_IT(str_array.reset())) {
        } else if (OB_FAIL(ObAdminBackupValidationUtil::convert_comma_separated_string_to_array(
                       backupset_path_str, str_array))) {
          STORAGE_LOG(WARN, "failed to convert_comma_spearated_string_to_array", K(ret));
        } else {
          FOREACH_X(str, str_array, OB_SUCC(ret))
          {
            share::ObBackupDest *backup_set_path = nullptr;
            if (OB_ISNULL(alc_ptr = allocator_.alloc(sizeof(share::ObBackupDest)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
            } else if (FALSE_IT(backup_set_path = new (alc_ptr) share::ObBackupDest())) {
            } else if (OB_FAIL(backup_set_path->set(*str))) {
              STORAGE_LOG(WARN, "failed to set backup set path", K(ret));
            } else if (OB_FAIL(ctx_->backup_set_path_array_.push_back(backup_set_path))) {
              STORAGE_LOG(WARN, "failed to push back backup_set_path", K(ret));
            }
          }
        }
        break;
      }
      case 5: {
        if (ObAdminBackupValidationType::BACKUPSET_VALIDATION == validation_type_
            || ObAdminBackupValidationType::DATABASE_VALIDATION == validation_type_
            || OB_NOT_NULL(backuppiece_path_str)) {
          print_usage_();
          exit(1);
        } else if (OB_ISNULL(backuppiece_key_str = static_cast<char *>(
                                 tmp_allocator.alloc(common::OB_MAX_URI_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (OB_FAIL(databuff_printf(backuppiece_key_str, common::OB_MAX_URI_LENGTH, "%s",
                                           optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        } else if (FALSE_IT(str_array.reset())) {
        } else if (OB_FAIL(ObAdminBackupValidationUtil::convert_comma_separated_string_to_array(
                       backuppiece_key_str, str_array))) {
          STORAGE_LOG(WARN, "failed to convert_comma_spearated_string_to_array", K(ret));
        } else {
          FOREACH_X(str, str_array, OB_SUCC(ret))
          {
            ObAdminPieceKey backup_piece_key;
            backup_piece_key.reset();
            if (sscanf(str->ptr(), "d%ldr%ldp%ld", &backup_piece_key.dest_id_,
                       &backup_piece_key.round_id_, &backup_piece_key.piece_id_)
                != 3) {
              ret = OB_INVALID_ARGUMENT;
              STORAGE_LOG(WARN, "invalid argument", K(ret));
            } else if (OB_FAIL(ctx_->backup_piece_key_array_.push_back(backup_piece_key))) {
              STORAGE_LOG(WARN, "failed to push back backup_piece_key", K(ret));
            }
          }
        }
        break;
      }
      case 6: {
        if (ObAdminBackupValidationType::BACKUPPIECE_VALIDATION == validation_type_
            || ObAdminBackupValidationType::DATABASE_VALIDATION == validation_type_
            || OB_NOT_NULL(backupset_path_str)) {
          print_usage_();
          exit(1);
        } else if (OB_ISNULL(backupset_id_str = static_cast<char *>(
                                 tmp_allocator.alloc(common::OB_MAX_URI_LENGTH)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
        } else if (OB_FAIL(databuff_printf(backupset_id_str, common::OB_MAX_URI_LENGTH, "%s",
                                           optarg))) {
          STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
        } else if (FALSE_IT(str_array.reset())) {
        } else if (OB_FAIL(ObAdminBackupValidationUtil::convert_comma_separated_string_to_array(
                       backupset_id_str, str_array))) {
          STORAGE_LOG(WARN, "failed to convert_comma_spearated_string_to_array", K(ret));
        } else {
          FOREACH_X(str, str_array, OB_SUCC(ret))
          {
            int64_t backup_set_id = strtoll(str->ptr(), NULL, 10);
            if (backup_set_id <= 0) {
              ret = OB_INVALID_ARGUMENT;
              STORAGE_LOG(WARN, "invalid argument", K(ret));
            } else if (OB_FAIL(ctx_->backup_set_id_array_.push_back(backup_set_id))) {
              STORAGE_LOG(WARN, "failed to push back backup_set_id", K(ret));
            }
          }
        }
        break;
      }
      case 8: {
        if (0 == STRCASECMP(optarg, "none")) {
          ctx_->mb_check_level_ = blocksstable::ObMacroBlockCheckLevel::CHECK_LEVEL_NONE;
        } else if (0 == STRCASECMP(optarg, "physical")) {
          ctx_->mb_check_level_ = blocksstable::ObMacroBlockCheckLevel::CHECK_LEVEL_PHYSICAL;
        } else {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        }

        break;
      }
      case 9: {
        char *endptr = nullptr;
        int64_t tmp_concurrency = 0;
        if (OB_FAIL(ob_strtoll(optarg, endptr, tmp_concurrency))) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        } else if (tmp_concurrency <= 0 || tmp_concurrency > 256 /*max concurrency*/) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        } else {
          concurrency_ = tmp_concurrency;
        }
        break;
      }
      case 10: {
        char *endptr = nullptr;
        int64_t tmp_io_bandwidth_limit = 0;
        if (OB_FAIL(ob_strtoll(optarg, endptr, tmp_io_bandwidth_limit))) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        } else if (tmp_io_bandwidth_limit <= 0) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        } else if (OB_FAIL(ctx_->set_io_bandwidth(tmp_io_bandwidth_limit))) {
          STORAGE_LOG(WARN, "failed to set io bandwidth", K(ret));
        }
        break;
      }
      case 11: {
        char *endptr = nullptr;
        int64_t tmp_memory_limit = 0;
        if (OB_FAIL(ob_strtoll(optarg, endptr, tmp_memory_limit))) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        } else if (tmp_memory_limit <= 0) {
          ret = OB_INVALID_ARGUMENT;
          STORAGE_LOG(WARN, "invalid argument", K(ret));
        } else {
          memory_limit_ = OB_MAX(memory_limit_, tmp_memory_limit);
        }
        break;
      }
      default: {
        STORAGE_LOG(WARN, "unknown option", K(opt));
        print_usage_();
        exit(1);
      }
      }
    }
  }
  if (OB_SUCC(ret)) {
    // reload memory limit
    memory_limit_
        = OB_MAX(memory_limit_ * 1024 * 1024 * 1024LL, concurrency_ * 256L * 1024 * 1024LL);
    lib::set_memory_limit(memory_limit_);
    lib::set_tenant_memory_limit(OB_SERVER_TENANT_ID, memory_limit_);
    STORAGE_LOG(INFO, "succeed to set memory limit", K(memory_limit_));
    printf("Memory limit set to \033[1;32m%.2lf GB\033[0m\n",
           memory_limit_ / 1024.0 / 1024.0 / 1024.0);
    fflush(stdout);
    // reload currency
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      char *ha_high_thread_score = nullptr;
      share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
      if (OB_ISNULL(ha_high_thread_score
                    = static_cast<char *>(tmp_allocator.alloc(common::OB_MAX_URI_LENGTH)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        STORAGE_LOG(WARN, "failed to alloc memory", K(ret));
      } else if (OB_FAIL(databuff_printf(ha_high_thread_score, common::OB_MAX_URI_LENGTH, "%ld",
                                         concurrency_))) {
        STORAGE_LOG(WARN, "failed to databuff printf", K(ret));
      } else if (FALSE_IT(tenant_config->ha_high_thread_score.set_value(ha_high_thread_score))) {
      } else if (OB_ISNULL(dag_scheduler)) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "dag_scheduler is null", K(ret));
      } else if (FALSE_IT(dag_scheduler->reload_config())) {
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "tenant_config is invalid", K(ret));
    }
  }
  return ret;
};
int ObAdminBackupValidationExecutor::print_usage_()
{
  int ret = OB_SUCCESS;
  printf("\n");
  switch (validation_type_) {
  case ObAdminBackupValidationType::DATABASE_VALIDATION:
    printf("Usage: ob_admin validate_database [command args] [options]\n");
    printf("commands:\n");
    printf(HELP_FMT, "--help", "display this message.");
    printf("options:\n");
    printf(HELP_FMT, "--log_archive_dest", "OB log archive dest with storage info");
    printf(HELP_FMT, "--data_backup_dest", "OB data backup dest with storage info");
    printf(HELP_FMT, "--backup_piece_path",
           "OB log archive piece path with storage info, comma separated without "
           "space, shall not be used together with --log_archive_dest");
    printf(HELP_FMT, "--backup_set_path",
           "OB data backup set path with storage info, comma separated without space, shall not be "
           "used together with --data_backup_dest");
    break;
  case ObAdminBackupValidationType::BACKUPPIECE_VALIDATION:
    printf("Usage: ob_admin validate_backuppiece [command args] [options]\n");
    printf("commands:\n");
    printf(HELP_FMT, "--help", "display this message.");
    printf("options:\n");
    printf(HELP_FMT, "--log_archive_dest", "OB log archive dest with storage info");
    printf(HELP_FMT, "--backup_piece_key",
           "OB log archive piece key, comma separated without space, shall used together with "
           "--log_archive_dest");
    printf(HELP_FMT, "--backup_piece_path",
           "OB log archive piece path with storage info, comma separated without space, shall not "
           "be used together with --log_archive_dest");

    break;
  case ObAdminBackupValidationType::BACKUPSET_VALIDATION:
    printf("Usage: ob_admin validate_backupset [command args] [options]\n");
    printf("commands:\n");
    printf(HELP_FMT, "--help", "display this message.");
    printf("options:\n");
    printf(HELP_FMT, "--data_backup_dest", "OB data backup dest with storage info");
    printf(HELP_FMT, "--backup_set_id",
           "OB data backup set id, comma separated without space, shall used together with "
           "--data_backup_dest");
    printf(HELP_FMT, "--backup_set_path",
           "OB data backup set path with storage info, comma separated without space, shall not be "
           "used together with --data_backup_dest");
    break;
  default:
    break;
  }
  printf(HELP_FMT, "--check_level", "macro block check level, [none, physical]");
  printf(HELP_FMT, "--concurrency",
         "default is 1, no more than 256, recommend increasing when using remote storage");
  printf(HELP_FMT, "--io_bandwidth_limit", "default is ulimited, adjust if needed");
  fflush(stdout);
  return ret;
}
int ObAdminBackupValidationExecutor::schedule_data_backup_validation_()
{
  int ret = OB_SUCCESS;
  share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
  ObAdminDataBackupValidationDagInitParam param(ctx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag_scheduler is null", K(ret));
  } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObAdminDataBackupValidationDagNet>(
                 &param))) {
    STORAGE_LOG(WARN, "failed to create and add dag net", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create and add dag net");
  }
  return ret;
}

int ObAdminBackupValidationExecutor::wait_data_backup_validation_()
{
  int ret = OB_SUCCESS;
  share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
  int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag_scheduler is null", K(ret));
  } else {
    while (!dag_scheduler->is_empty()) {
      ctx_->print_data_backup_validation_status();
      ob_usleep(500_ms);
    }
    int64_t end_time = ObTimeUtility::current_time();
    if (ctx_->global_stat_.get_scheduled_tablet_count()
        != ctx_->global_stat_.get_succeed_tablet_count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "scheduled tablet count not equal to succeed tablet count", K(ret));
      ctx_->go_abort("scheduled tablet count not equal to succeed tablet count",
                     "tablet missing or meta info corrupted");
    }
    ctx_->print_data_backup_validation_status();
    if (ctx_->aborted_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "data backup validation failed", K(ret));
      printf("\nData Backup Validation \033[1;31mFailed✘\033[0m");
    } else {
      printf("\nData Backup Validation \033[1;32mPassed✔\033[0m");
    }
    printf("\nTime cost: %ld ms\n", (end_time - start_time) / 1000);
    fflush(stdout);
  }
  return ret;
}
int ObAdminBackupValidationExecutor::schedule_log_archive_validation_()
{
  int ret = OB_SUCCESS;
  share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
  ObAdminLogArchiveValidationDagInitParam param(ctx_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag_scheduler is null", K(ret));
  } else if (OB_FAIL(dag_scheduler->create_and_add_dag_net<ObAdminLogArchiveValidationDagNet>(
                 &param))) {
    STORAGE_LOG(WARN, "failed to create and add dag net", K(ret));
  } else {
    STORAGE_LOG(INFO, "succeed to create and add dag net");
  }
  return ret;
}

int ObAdminBackupValidationExecutor::wait_log_archive_validation_()
{
  int ret = OB_SUCCESS;
  share::ObTenantDagScheduler *dag_scheduler = MTL(share::ObTenantDagScheduler *);
  int64_t start_time = ObTimeUtility::current_time();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not init", K(ret));
  } else if (OB_ISNULL(dag_scheduler)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "dag_scheduler is null", K(ret));
  } else {
    while (!dag_scheduler->is_empty()) {
      ctx_->print_log_archive_validation_status();
      ob_usleep(500_ms);
    }
    int64_t end_time = ObTimeUtility::current_time();
    if (ctx_->global_stat_.get_scheduled_piece_count()
        != ctx_->global_stat_.get_succeed_piece_count()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "scheduled piece count not equal to succeed piece count", K(ret));
      ctx_->go_abort("scheduled piece count not equal to succeed piece count",
                     "piece or meta info corrupted");
    }
    ctx_->print_log_archive_validation_status();
    if (ctx_->aborted_) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "log archive validation failed", K(ret));
      printf("\nLog Archive Validation \033[1;31mFailed✘\033[0m");
    } else {
      printf("\nLog Archive Validation \033[1;32mPassed✔\033[0m");
    }
    printf("\nTime cost: %ld ms\n", (end_time - start_time) / 1000);
    fflush(stdout);
  }
  return ret;
}

}; // namespace tools
}; // namespace oceanbase
ob_set_subtarget(oblogminer_obj_list common
  ob_log_miner_analysis_writer.cpp
  ob_log_miner_analyzer_checkpoint.cpp
  ob_log_miner_analyzer.cpp
  ob_log_miner_args.cpp
  ob_log_miner_batch_record.cpp
  ob_log_miner_batch_record_writer.cpp
  ob_log_miner_br_converter.cpp
  ob_log_miner_br_filter.cpp
  ob_log_miner_br_producer.cpp
  ob_log_miner_br.cpp
  ob_log_miner_data_manager.cpp
  ob_log_miner_file_index.cpp
  ob_log_miner_file_manager.cpp
  ob_log_miner_file_meta.cpp
  ob_log_miner_filter_condition.cpp
  ob_log_miner_flashback_reader.cpp
  ob_log_miner_flashback_writer.cpp
  ob_log_miner_logger.cpp
  ob_log_miner_mode.cpp
  ob_log_miner_progress_range.cpp
  ob_log_miner_record_aggregator.cpp
  ob_log_miner_record_converter.cpp
  ob_log_miner_record_file_format.cpp
  ob_log_miner_record_filter.cpp
  ob_log_miner_record_parser.cpp
  ob_log_miner_record_rewriter.cpp
  ob_log_miner_record.cpp
  ob_log_miner_resource_collector.cpp
  ob_log_miner_timezone_getter.cpp
  ob_log_miner_undo_task.cpp
  ob_log_miner_utils.cpp
  ob_log_miner.cpp
)

add_executable(oblogminer ob_log_miner_main.cpp)
ob_add_new_object_target(oblogminer_objs oblogminer_obj_list)
ob_add_new_object_target(oblogminer_obj_dev oblogminer_obj_list)
# ob_add_new_object_target(oblogminer_main_obj oblogminer_main)
target_include_directories(oblogminer_obj_dev INTERFACE ${CMAKE_CURRENT_SOURCE_DIR})
# add_library(oblogminer_dev SHARED)
target_link_libraries(oblogminer_objs PUBLIC obcdc_static)
target_link_libraries(oblogminer_obj_dev PUBLIC obcdc)
target_link_libraries(oblogminer PRIVATE oblogminer_objs)

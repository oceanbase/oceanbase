set(OB_TESTBENCH_CORE
  ob_testbench_builder.cpp
  ob_testbench_mysql_proxy.cpp
  ob_testbench_transaction_scheduler.cpp
  ob_testbench_transaction_executor_pool.cpp 
  ob_testbench_server_provider.cpp
  ob_testbench_systable_helper.cpp
  ob_testbench_transaction_task.cpp
  ob_testbench_statistics_collector.cpp
)
add_library(ob_testbench_workload ${OB_TESTBENCH_CORE})
target_link_libraries(ob_testbench_workload PUBLIC obcdc)

set(INSTANCE_INSTALL_DIR $ENV{HOME}/.testbench/repository)

set(LINK_MALLOC_HOOK malloc_hook)
add_executable(instance_observer ob_instance_observer.cpp)
target_link_libraries(instance_observer PUBLIC oceanbase ${LINK_MALLOC_HOOK})
add_custom_command(
  TARGET instance_observer 
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:instance_observer> ${INSTANCE_INSTALL_DIR}
)

add_executable(instance_testbench ob_instance_testbench.cpp)
set_target_properties(instance_testbench PROPERTIES 
  COMPILE_FLAGS "-O0"
  LINK_FLAGS "-g"
  BUILD_TYPE Debug  
)
target_link_libraries(instance_testbench PUBLIC ob_testbench_workload)

add_custom_command(
  TARGET instance_testbench 
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:instance_testbench> ${INSTANCE_INSTALL_DIR}
)

execute_process(
  COMMAND bash install_scripts.sh
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/testbench/script
)

set(OB_TESTBENCH_WORKLOAD
  ob_workload_scheduler.cpp
  ob_workload_executor.cpp
)
add_library(ob_testbench_workload ${OB_TESTBENCH_WORKLOAD})
target_link_libraries(ob_testbench_workload PUBLIC oceanbase)

set(INSTANCE_INSTALL_DIR $ENV{HOME}/.testbench/repository)

set(LINK_MALLOC_HOOK malloc_hook)
add_executable(instance_observer ob_instance_observer.cpp)
target_link_libraries(instance_observer PUBLIC oceanbase ${LINK_MALLOC_HOOK})
ADD_CUSTOM_COMMAND(
  TARGET instance_observer 
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:instance_observer> ${INSTANCE_INSTALL_DIR}
)

add_executable(instance_testbench ob_instance_testbench.cpp)
target_link_libraries(instance_testbench PUBLIC ob_testbench_workload)
ADD_CUSTOM_COMMAND(
  TARGET instance_observer 
  POST_BUILD
  COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:instance_testbench> ${INSTANCE_INSTALL_DIR}
)

EXECUTE_PROCESS(
  COMMAND bash install_scripts.sh
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/testbench/script
)

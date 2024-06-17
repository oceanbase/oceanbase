set(MIT_SRCS env/ob_simple_server_helper.cpp)

add_library(mit_env ${MIT_SRCS})

target_include_directories(mit_env PUBLIC
  ${CMAKE_SOURCE_DIR}/unittest ${CMAKE_SOURCE_DIR}/mittest)

target_link_libraries(mit_env PUBLIC
  oceanbase
)

add_subdirectory(logservice)
add_subdirectory(simple_server)
add_subdirectory(mtlenv)
add_subdirectory(multi_replica)
#add_subdirectory(palf_cluster)

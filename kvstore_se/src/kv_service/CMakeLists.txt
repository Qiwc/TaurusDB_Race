cmake_minimum_required(VERSION 2.8)
project(kv_service)

set (CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -DELPP_NO_DEFAULT_LOG_FILE -DELPP_THREAD_SAFE")

set(SRC_BASE ${PROJECT_SOURCE_DIR})
include_directories(
#    ${SRC_BASE}/../../third_party
#    ${SRC_BASE}/../third_party
    ${SRC_BASE}/../include
    )

set(SRC_SERVICE
    ${SRC_BASE}/kv_service.cc
    ${SRC_BASE}/kv_intf.cc kv_hashlog.h)

add_library(${PROJECT_NAME} SHARED ${SRC_SERVICE})
target_link_libraries(${PROJECT_NAME} pthread anl)

add_custom_command(TARGET ${PROJECT_NAME} POST_BUILD COMMAND cp ./*.so ../)

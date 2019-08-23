CMAKE_MINIMUM_REQUIRED(VERSION 2.8)
PROJECT(kv_store)

# SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g ")
SET(SRC_BASE ${PROJECT_SOURCE_DIR})
SET(SRC_CPP
        ${SRC_BASE}/kv_store.cc
        ${SRC_BASE}/kv_intf.cc
        )

# LINK_DIRECTORIES(${LINK_DIRECTORIES}
#    /usr/local/lib/
#    /usr/local/lib64/)

ADD_LIBRARY(${PROJECT_NAME} SHARED ${SRC_CPP})

# ADD_EXECUTABLE(${PROJECT_NAME} ${SRC_CPP} kv_main.cc)
# TARGET_LINK_LIBRARIES(${PROJECT_NAME} pthread)

# ADD_EXECUTABLE(kv_multi ${SRC_CPP} kv_multi.cc)
# TARGET_LINK_LIBRARIES(kv_multi pthread)





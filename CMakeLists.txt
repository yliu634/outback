cmake_minimum_required(VERSION 3.2)


project(xxx)
add_definitions(-std=c++17 -DBOOST_COROUTINES_NO_DEPRECATION_WARNING)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O3 -Wno-unused-result -Wno-unused-function")
# set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -mbmi -mbmi2 -mlzcnt -mpopcnt")

# directories
include_directories("./")
include_directories("deps")

set(LOG_SRC
    "./deps/r2/src/logging.cc"
    "./deps/r2/src/sshed.cc"
    "./deps/ycsb/core_workload.cc"
    "./benchs/terminate.cc")

set(LOG_LIBRARIES
    absl
    gflags
    ibverbs
    pthread
    boost_system
    memcached
	numa
    boost_coroutine
)


set(MY_BOOLEAN_VARIABLE 0) 
if(MY_BOOLEAN_VARIABLE)
file(GLOB SOURCE_FILES "./xcomm/tests/*.cc" ${LOG_SRC})

foreach(SOURCE_FILE ${SOURCE_FILES})
    # Extract the file name without extension (assuming .cc extension)
    get_filename_component(EXECUTABLE_NAME ${SOURCE_FILE} NAME_WE)
    # Create an executable with the extracted name
    add_executable(${EXECUTABLE_NAME} ${SOURCE_FILE})
endforeach()
endif()



########### outback two-sided rdma ###########

file(GLOB client_SORUCES ""  "./benchs/outback/client.cc"  "./deps/ludo/hashutils/*.cc" ${LOG_SRC})
file(GLOB server_SORUCES ""  "./benchs/outback/server.cc"  "./deps/ludo/hashutils/*.cc" ${LOG_SRC})

add_executable(client ${client_SORUCES})
add_executable(server ${server_SORUCES})

set_target_properties(client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/outback/)
set_target_properties(server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/outback/)

target_link_libraries(client ${LOG_LIBRARIES})
target_link_libraries(server ${LOG_LIBRARIES})

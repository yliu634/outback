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



########## race ping pong ##########

file(GLOB race_client_SORUCES ""  "./benchs/race/client.cc"  ${LOG_SRC})
file(GLOB race_server_SORUCES ""  "./benchs/race/server.cc"  ${LOG_SRC})

add_executable(race_client ${race_client_SORUCES})
add_executable(race_server ${race_server_SORUCES})
set_target_properties(race_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/race/)
set_target_properties(race_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/race/)

target_link_libraries(race_client ${LOG_LIBRARIES})
target_link_libraries(race_server ${LOG_LIBRARIES})



########## fasst mica ring ##########

#file(GLOB fasst_client_SORUCES ""  "./benchs/fasst/client.cc" "./mica/util/*.cc" "./mica/alloc/*.cc" ${LOG_SRC})
#file(GLOB fasst_server_SORUCES ""  "./benchs/fasst/server.cc" "./mica/util/*.cc" "./mica/alloc/*.cc" "./mica/util/cityhash/city.cc" ${LOG_SRC})

file(GLOB fasst_client_SORUCES ""  "./benchs/fasst/client.cc" "./mica/*.cc" ${LOG_SRC})
file(GLOB fasst_server_SORUCES ""  "./benchs/fasst/server.cc" "./mica/*.cc" ${LOG_SRC})
add_executable(fasst_client ${fasst_client_SORUCES})
add_executable(fasst_server ${fasst_server_SORUCES})
set_target_properties(fasst_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/fasst/)
set_target_properties(fasst_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/fasst/)
target_link_libraries(fasst_client ${LOG_LIBRARIES})
target_link_libraries(fasst_server ${LOG_LIBRARIES})



########## drtmr two-sided rdma ##########

file(GLOB drtmr_client_SORUCES ""  "./benchs/drtmr/client.cc"  ${LOG_SRC})
file(GLOB drtmr_server_SORUCES ""  "./benchs/drtmr/server.cc"  ${LOG_SRC})

add_executable(drtmr_client ${drtmr_client_SORUCES})
add_executable(drtmr_server ${drtmr_server_SORUCES})
set_target_properties(drtmr_client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/drtmr/)
set_target_properties(drtmr_server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/drtmr/)

target_link_libraries(drtmr_client ${LOG_LIBRARIES})
target_link_libraries(drtmr_server ${LOG_LIBRARIES})



########### outback two-sided rdma ###########

file(GLOB client_SORUCES ""  "./benchs/outback/client.cc"  "./deps/ludo/hashutils/*.cc" ${LOG_SRC})
file(GLOB server_SORUCES ""  "./benchs/outback/server.cc"  "./deps/ludo/hashutils/*.cc" ${LOG_SRC})

add_executable(client ${client_SORUCES})
add_executable(server ${server_SORUCES})

set_target_properties(client PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/outback/)
set_target_properties(server PROPERTIES RUNTIME_OUTPUT_DIRECTORY ./benchs/outback/)

target_link_libraries(client ${LOG_LIBRARIES})
target_link_libraries(server ${LOG_LIBRARIES})

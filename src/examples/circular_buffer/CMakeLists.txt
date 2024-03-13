add_executable(circular_buffer_client circular_buffer_client.cc)

target_link_libraries( circular_buffer_client ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${Boost_TIMER_LIBRARY} ${LIB_PTHREAD} )

add_executable(circular_buffer_server circular_buffer_server.cc)

target_link_libraries( circular_buffer_server ${Boost_FILESYSTEM_LIBRARY} ${Boost_LOG_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY} ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} )





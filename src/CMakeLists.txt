add_library(raft server.cc)

target_link_libraries( raft ${Boost_LOG_LIBRARY} ${Boost_SYSTEM_LIBRARY} ${Boost_THREAD_LIBRARY} ${LIB_PTHREAD} )



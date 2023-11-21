#ifndef __RAFT_BUILDER_COMMUNICATOR_HH__
#define __RAFT_BUILDER_COMMUNICATOR_HH__

#include <string>

#include "slice.hh"

/**
 * Mixin class that adds builder based communicator methods to a base communicator that 
 * provides a send method for all message types.
 */
namespace raft {
  namespace util {
    template<typename _Messages, typename _Builders, typename _BaseCommunicator>
    class builder_communicator : public _BaseCommunicator
    {
    public:
      typedef size_t endpoint;    
      typedef _Builders builders_type;
      typedef typename builders_type::vote_request_builder_type vote_request_builder;
      typedef typename builders_type::vote_response_builder_type vote_response_builder;
      typedef typename builders_type::append_entry_request_builder_type append_entry_request_builder;
      typedef typename builders_type::append_entry_response_builder_type append_entry_response_builder;
      typedef typename builders_type::append_checkpoint_chunk_request_builder_type append_checkpoint_chunk_request_builder;
      typedef typename builders_type::append_checkpoint_chunk_response_builder_type append_checkpoint_chunk_response_builder;
      typedef typename _Messages::checkpoint_header_traits_type checkpoint_header_traits;

      template <typename ... Args>
      builder_communicator(Args&& ... args)
        :
        _BaseCommunicator(std::forward<Args>(args)...)
      {
      }

      void vote_request(endpoint ep, const std::string & address,
			uint64_t request_id,
			uint64_t recipient_id,
			uint64_t term_number,
			uint64_t candidate_id,
			uint64_t last_log_index,
			uint64_t last_log_term)
      {
	auto msg = vote_request_builder().request_id(request_id).recipient_id(recipient_id).term_number(term_number).candidate_id(candidate_id).last_log_index(last_log_index).last_log_term(last_log_term).finish();
	this->send(ep, address, std::move(msg));	
      }

      template<typename EntryProvider>
      void append_entry_request(endpoint ep, const std::string& address,
                                uint64_t request_id,
                                uint64_t recipient_id,
                                uint64_t term_number,
                                uint64_t leader_id,
                                uint64_t previous_log_index,
                                uint64_t previous_log_term,
                                uint64_t leader_commit_index,
                                uint64_t num_entries,
                                EntryProvider entries)
      {
	append_entry_request_builder bld;
	bld.request_id(request_id).recipient_id(recipient_id).term_number(term_number).leader_id(leader_id).previous_log_index(previous_log_index).previous_log_term(previous_log_term).leader_commit_index(leader_commit_index);
	for(uint64_t i=0; i<num_entries; ++i) {
	  bld.entry(entries(i));
	}
	auto msg = bld.finish();
	this->send(ep, address, std::move(msg));	
      }
	
      void append_entry_response(endpoint ep, const std::string& address,
				 uint64_t recipient_id,
				 uint64_t term_number,
				 uint64_t request_term_number,
				 uint64_t request_id,
				 uint64_t begin_index,
				 uint64_t last_index,
				 bool success)
      {
	auto msg = append_entry_response_builder().recipient_id(recipient_id).term_number(term_number).request_term_number(request_term_number).request_id(request_id).begin_index(begin_index).last_index(last_index).success(success).finish();
	this->send(ep, address, std::move(msg));	
      }

      void vote_response(endpoint ep, const std::string& address,
			 uint64_t peer_id,
			 uint64_t term_number,
			 uint64_t request_term_number,
			 uint64_t request_id,
			 bool granted)
      {
	auto msg = vote_response_builder().peer_id(peer_id).term_number(term_number).request_term_number(request_term_number).request_id(request_id).granted(granted).finish();
	this->send(ep, address, std::move(msg));	
      }

      void append_checkpoint_chunk_request(endpoint ep, const std::string& address,
                                           uint64_t request_id,
                                           uint64_t recipient_id,
                                           uint64_t term_number,
                                           uint64_t leader_id,
                                           const typename _Messages::checkpoint_header_type & last_checkpoint_header,
                                           uint64_t checkpoint_begin,
                                           uint64_t checkpoint_end,
                                           bool checkpoint_done,
                                           raft::slice && data)
      {
	append_checkpoint_chunk_request_builder bld;
	bld.request_id(request_id).recipient_id(recipient_id).term_number(term_number).leader_id(leader_id).checkpoint_begin(checkpoint_begin).checkpoint_end(checkpoint_end).checkpoint_done(checkpoint_done).data(std::move(data));
	{
	  auto chb = bld.last_checkpoint_header();
	  chb.last_log_entry_index(checkpoint_header_traits::last_log_entry_index(&last_checkpoint_header));
	  chb.last_log_entry_term(checkpoint_header_traits::last_log_entry_term(&last_checkpoint_header));
          chb.last_log_entry_cluster_time(checkpoint_header_traits::last_log_entry_cluster_time(&last_checkpoint_header));
	  chb.index(checkpoint_header_traits::index(&last_checkpoint_header));
	  chb.configuration(checkpoint_header_traits::configuration(&last_checkpoint_header));
	}
	auto msg = bld.finish();
	this->send(ep, address, std::move(msg));	
      }		       
  
      void append_checkpoint_chunk_response(endpoint ep, const std::string& address,
					    uint64_t recipient_id,
					    uint64_t term_number,
					    uint64_t request_term_number,
					    uint64_t request_id,
					    uint64_t bytes_stored)
      {
	auto msg = append_checkpoint_chunk_response_builder().recipient_id(recipient_id).term_number(term_number).request_term_number(request_term_number).request_id(request_id).bytes_stored(bytes_stored).finish();
	this->send(ep, address, std::move(msg));	
      }
    };
  }
}

#endif

#ifndef __RAFT_FLATBUFFER_MESSAGES_HH__
#define __RAFT_FLATBUFFER_MESSAGES_HH__

#include "raft_generated.h"

namespace raft {
  namespace fbs {
    
    struct append_entry_traits
    {
      typedef const raft_message * const_arg_type;
      typedef const raft_message * pinned_type;
      typedef flatbuffers::Vector<flatbuffers::Offset<log_entry>>::const_iterator iterator_type;
      static const append_entry *  get_append_entry(const_arg_type ae)
      {
	return static_cast<const raft::fbs::append_entry * >(ae->message());
      }      
      static uint64_t recipient_id(const_arg_type ae)
      {
	return get_append_entry(ae)->recipient_id();
      }
      static uint64_t term_number(const_arg_type ae)
      {
	return get_append_entry(ae)->term_number();
      }
      static uint64_t leader_id(const_arg_type ae)
      {
	return get_append_entry(ae)->leader_id();
      }
      // Basic point of Raft is the Log Matching Property which comprises:
      // 1) Index and Term of a log entry uniquely define the content
      // 2) If two logs have entries at the same index with the same term then all preceeding entries
      // also agree
      //
      static uint64_t previous_log_index(const_arg_type ae)
      {
	return get_append_entry(ae)->previous_log_index;
      }
      // The last term sent (only valid if previous_log_index > 0).
      static uint64_t previous_log_term(const_arg_type ae)
      {
	return get_append_entry(ae)->previous_log_term;
      }
      // Last log entry in message that is committed on leader
      static uint64_t leader_commit_index(const_arg_type ae)
      {
	return get_append_entry(ae)->leader_commit_index;
      }
      static std::size_t num_entries(const_arg_type ae)
      {
	return get_append_entry(ae)->entry()->size();
      }
      static iterator_type begin_entries(const_arg_type ae)
      {
	return get_append_entry(ae)->entry()->begin();
      }
      static iterator_type end_entries(const_arg_type ae)
      {
	return get_append_entry(ae)->entry()->end();
      }
      static void release(const_arg_type ae)
      {
	delete [] flatbuffers::GetBufferStartFromRootPointer(ae);
      }
      static pinned_type pin(const_arg_type ae)
      {
	return pinned_type(ae);
      }
    };

    template<typename _Communicator>
    class append_entry_sender
    {
    private:
      _Communicator & comm_;
      typename _Communicator::endpoint ep_;
      std::string address_;
      
	
    public:
      append_entry_sender(_Communicator & comm, typename _Communicator::endpoint ep, const std::string & addr)
	:
	comm_(comm),
	ep_(ep),
	address_(addr)
      {
      }

      template<typename EntryProvider>
      void send(uint64_t recipient_id,
		uint64_t term_number,
		uint64_t leader_id,
		uint64_t previous_log_index,
		uint64_t previous_log_term,
		uint64_t leader_commit_index,
		uint64_t num_entries,
		EntryProvider entries)
      {
	flatbuffers::FlatBufferBuilder fbb;
	std::vector<flatbuffers::Offset<raft::fbs::log_entry>> entries;
	for(uint64_t i=0; i<num_entries; ++i) {
	  msg_.add_entry(entries(i));
	}

	auto e = fbb.CreateVector(entries);  
	
	raft::fbs::append_entryBuilder aeb(fbb);
	aeb.add_recipient_id(recipient_id);
	aeb.add_term_number(term_number);
	aeb.add_leader_id(leader_id);
	aeb.add_previous_log_index(previous_log_index);
	aeb.add_previous_log_term(previous_log_term);
	aeb.add_leader_commit_index(leader_commit_index);
	aeb.add_entry(e);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(fbb, raft::fbs::any_message_append_entry, ae.Union());
	// Finish and get buffer
	fbb.Finish(m);

	fbb.GetBufferPointer();
	comm_.send(ep_, address_, msg_);
      }
    };

    class messages
    {
    public:
      typedef client_request client_request_type;
      typedef client_response client_response_type;
      typedef request_vote request_vote_type;
      typedef vote_response vote_response_type;
      typedef append_checkpoint_chunk append_checkpoint_chunk_type;
      typedef append_checkpoint_chunk_response append_checkpoint_chunk_response_type;
      typedef append_entry append_entry_type;
      typedef append_response append_entry_response_type;
      typedef set_configuration_request set_configuration_request_type;
      typedef set_configuration_response set_configuration_response_type;

      typedef server_description server_description_type;
      typedef simple_configuration_description simple_configuration_description_type;
      typedef configuration_description configuration_description_type;
    };      
  }
}

#endif

#ifndef __RAFT_FLATBUFFER_MESSAGES_HH__
#define __RAFT_FLATBUFFER_MESSAGES_HH__

#include "raft_generated.h"

#include "../slice.hh"

namespace raft {
  namespace fbs {

    // A moveable but not copyable container for a FlatBufferBuilder
    struct flatbuffer_builder_adapter
    {
      // Non-copyable only moveable
      flatbuffer_builder_adapter(const flatbuffer_builder_adapter & s) = delete;
      const flatbuffer_builder_adapter & operator=(const flatbuffer_builder_adapter & s) = delete;
      
      flatbuffer_builder_adapter(flatbuffer_builder_adapter && rhs)
	:
	fbb_(std::move(rhs.fbb_))
      {
      }
      
      const flatbuffer_builder_adapter & operator=(flatbuffer_builder_adapter && rhs)
      {
	fbb_ = std::move(rhs.fbb_);
	return *this;
      }

      std::unique_ptr<flatbuffers::FlatBufferBuilder> fbb_;
      flatbuffer_builder_adapter()
	:
	fbb_(new flatbuffers::FlatBufferBuilder())
      {
      }

      flatbuffers::FlatBufferBuilder & operator* ()
      {
	return *fbb_.get();
      }

      flatbuffers::FlatBufferBuilder * operator->()
      {
	return fbb_.get();
      }

      operator slice ()
      {
	return slice(fbb_->GetBufferPointer(), fbb_->GetSize());
      }
    };
    
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
	return get_append_entry(ae)->previous_log_index();
      }
      // The last term sent (only valid if previous_log_index > 0).
      static uint64_t previous_log_term(const_arg_type ae)
      {
	return get_append_entry(ae)->previous_log_term();
      }
      // Last log entry in message that is committed on leader
      static uint64_t leader_commit_index(const_arg_type ae)
      {
	return get_append_entry(ae)->leader_commit_index();
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
	return ae;
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
	flatbuffer_builder_adapter fbb;
	std::vector<flatbuffers::Offset<raft::fbs::log_entry>> entries_vec;
	for(uint64_t i=0; i<num_entries; ++i) {
	  auto e = entries(i);
	  if (e.is_command()) {
	    auto str = fbb->CreateString(e.data());
	    raft::fbs::log_entryBuilder leb(*fbb);
	    leb.add_type(raft::fbs::log_entry_type_COMMAND);
	    leb.add_term(e.term());
	    leb.add_data(str);
	    entries_vec.push_back(leb.Finish());
	  } else if (e.is_configuration()) {
	    std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers_vec;
	    for(auto i=0; i<e.from_size(); ++i) {
	      servers_vec.push_back(raft::fbs::Createserver_description(*fbb, e.from_id(i), fbb->CreateString(e.from_address(i))));
	    }
	    auto from_desc = raft::fbs::Createsimple_configuration_description(*fbb, fbb->CreateVector(servers_vec));
	    servers_vec.clear();
	    for(auto i=0; i<e.to_size(); ++i) {
	      servers_vec.push_back(raft::fbs::Createserver_description(*fbb, e.to_id(i), fbb->CreateString(e.to_address(i))));
	    }
	    auto to_desc = raft::fbs::Createsimple_configuration_description(*fbb, fbb->CreateVector(servers_vec));
	    auto conf = raft::fbs::Createconfiguration_description(*fbb, from_desc, to_desc);
	    raft::fbs::log_entryBuilder leb(*fbb);
	    leb.add_type(raft::fbs::log_entry_type_CONFIGURATION);
	    leb.add_term(e.term());
	    leb.add_configuration(conf);
	    entries_vec.push_back(leb.Finish());
	  } else {
	    raft::fbs::log_entryBuilder leb(*fbb);
	    leb.add_type(raft::fbs::log_entry_type_NOOP);
	    leb.add_term(e.term());
	    entries_vec.push_back(leb.Finish());
	  }
	}

	auto e = fbb->CreateVector(entries_vec);  
	
	raft::fbs::append_entryBuilder aeb(*fbb);
	aeb.add_recipient_id(recipient_id);
	aeb.add_term_number(term_number);
	aeb.add_leader_id(leader_id);
	aeb.add_previous_log_index(previous_log_index);
	aeb.add_previous_log_term(previous_log_term);
	aeb.add_leader_commit_index(leader_commit_index);
	aeb.add_entry(e);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(*fbb, raft::fbs::any_message_append_entry, ae.Union());
	// Finish and get buffer
	fbb->Finish(m);
	// Send on to communicator
	comm_.send(ep_, address_, std::move(fbb));
      }
    };

    struct request_vote_traits
    {
      typedef const raft_message * const_arg_type;

      static const raft::fbs::request_vote * rv(const_arg_type msg)
      {
	return static_cast<const raft::fbs::request_vote * >(msg->message());
      }

      static uint64_t recipient_id(const_arg_type msg)
      {
	return rv(msg)->recipient_id();
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return rv(msg)->term_number();
      }
      static uint64_t candidate_id(const_arg_type msg)
      {
	return rv(msg)->candidate_id();
      }
      static uint64_t last_log_index(const_arg_type msg)
      {
	return rv(msg)->last_log_index();
      }
      static uint64_t last_log_term(const_arg_type msg)
      {
	return rv(msg)->last_log_term();
      }
    };

    struct vote_response_traits
    {
      typedef const raft_message * const_arg_type;

      static const raft::fbs::vote_response * vr(const_arg_type msg)
      {
	return static_cast<const raft::fbs::vote_response * >(msg->message());
      }

      static uint64_t peer_id(const_arg_type msg)
      {
	return vr(msg)->peer_id();
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return vr(msg)->term_number();
      }
      static uint64_t request_term_number(const_arg_type msg)
      {
	return vr(msg)->request_term_number();
      }
      static bool granted(const_arg_type msg)
      {
	return vr(msg)->granted();
      }
    };

    class append_entry_response_traits
    {
    public:
      typedef const raft_message * const_arg_type;    
    
      static const raft::fbs::append_response * aer(const_arg_type msg)
      {
	return static_cast<const raft::fbs::append_response * >(msg->message());
      }

      static uint64_t recipient_id(const_arg_type msg)
      {
	return aer(msg)->recipient_id();
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return aer(msg)->term_number();
      }
      static uint64_t request_term_number(const_arg_type msg)
      {
	return aer(msg)->request_term_number();
      }
      // Beginning of range of entries appended
      static uint64_t begin_index(const_arg_type msg)
      {
	return aer(msg)->begin_index();
      }
      // One after the last log entry appended
      static uint64_t last_index(const_arg_type msg)
      {
	return aer(msg)->last_index();
      }
      static bool success(const_arg_type msg)
      {
	return aer(msg)->success();
      }
    };

    class append_checkpoint_chunk_traits
    {
    public:
      typedef const raft_message * const_arg_type;    
      typedef const raft_message * pinned_type;
      typedef typename flatbuffers::Vector<flatbuffers::Offset<server_description>>::const_iterator iterator_type;
    
      static const raft::fbs::append_checkpoint_chunk * acc(const_arg_type msg)
      {
	return static_cast<const raft::fbs::append_checkpoint_chunk * >(msg->message());
      }
    
      static uint64_t recipient_id(const_arg_type msg)
      {
	return acc(msg)->recipient_id();
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return acc(msg)->term_number();
      }
      static uint64_t leader_id(const_arg_type msg)
      {
	return acc(msg)->leader_id();
      }
      static uint64_t last_checkpoint_index(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_index();
      }
      static uint64_t last_checkpoint_term(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_term();
      }
      static uint64_t checkpoint_configuration_index(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_configuration()->index();
      }
      static uint64_t checkpoint_configuration_from_size(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_configuration()->description()->from()->servers()->size();
      }
      static uint64_t checkpoint_configuration_from_id(const_arg_type msg, std::size_t i)
      {
	return acc(msg)->last_checkpoint_configuration()->description()->from()->servers()->Get(i)->id();
      }
      static const char * checkpoint_configuration_from_address(const_arg_type msg, std::size_t i)
      {
	return acc(msg)->last_checkpoint_configuration()->description()->from()->servers()->Get(i)->address()->c_str();
      }
      static uint64_t checkpoint_configuration_to_size(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_configuration()->description()->to()->servers()->size();
      }
      static uint64_t checkpoint_configuration_to_id(const_arg_type msg, std::size_t i)
      {
	return acc(msg)->last_checkpoint_configuration()->description()->to()->servers()->Get(i)->id();
      }
      static const char * checkpoint_configuration_to_address(const_arg_type msg, std::size_t i)
      {
	return acc(msg)->last_checkpoint_configuration()->description()->to()->servers()->Get(i)->address()->c_str();
      }
      static uint64_t checkpoint_begin(const_arg_type msg)
      {
	return acc(msg)->checkpoint_begin();
      }
      static uint64_t checkpoint_end(const_arg_type msg)
      {
	return acc(msg)->checkpoint_end();
      }
      static bool checkpoint_done(const_arg_type msg)
      {
	return acc(msg)->checkpoint_done();
      }
      static raft::slice data(const_arg_type msg)
      {
	return acc(msg)->data()->size()>0 ? raft::slice(acc(msg)->data()->Data(), acc(msg)->data()->size()) : raft::slice(nullptr, 0U);
      }
      static void release(const_arg_type msg)
      {
	delete [] flatbuffers::GetBufferStartFromRootPointer(msg);
      }
      static pinned_type pin(const_arg_type msg)
      {
	return msg;
      }
    };

    class append_checkpoint_chunk_response_traits
    {
    public:
      typedef const raft_message * const_arg_type;    

      static const raft::fbs::append_checkpoint_chunk_response * acc(const_arg_type msg)
      {
	return static_cast<const raft::fbs::append_checkpoint_chunk_response * >(msg->message());
      }
    
      static uint64_t recipient_id(const_arg_type msg)
      {
	return acc(msg)->recipient_id();
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return acc(msg)->term_number();
      }
      static uint64_t request_term_number(const_arg_type msg)
      {
	return acc(msg)->request_term_number();
      }
      static uint64_t bytes_stored(const_arg_type msg)
      {
	return acc(msg)->bytes_stored();
      }
    };
  
    class set_configuration_request_traits
    {
    public:
      typedef const raft_message * const_arg_type;    
    
      static const raft::fbs::set_configuration_request * scr(const_arg_type msg)
      {
	return static_cast<const raft::fbs::set_configuration_request * >(msg->message());
      }
    
      static uint64_t old_id(const_arg_type msg)
      {
	return scr(msg)->old_id();
      }
      static std::size_t new_configuration_size(const_arg_type msg)
      {
	return scr(msg)->new_configuration()->servers()->size();
      }
      static uint64_t new_configuration_id(const_arg_type msg, std::size_t i)
      {
	return scr(msg)->new_configuration()->servers()->Get(i)->id();
      }
      static const char * new_configuration_address(const_arg_type msg, std::size_t i)
      {
	return scr(msg)->new_configuration()->servers()->Get(i)->address()->c_str();
      }
    };

    class messages
    {
    public:
      typedef client_request client_request_type;
      typedef request_vote request_vote_type;
      typedef request_vote_traits request_vote_traits_type;
      typedef vote_response vote_response_type;
      typedef vote_response_traits vote_response_traits_type;
      typedef append_checkpoint_chunk append_checkpoint_chunk_type;
      typedef append_checkpoint_chunk_traits append_checkpoint_chunk_traits_type;
      typedef append_checkpoint_chunk_response append_checkpoint_chunk_response_type;
      typedef append_checkpoint_chunk_response_traits append_checkpoint_chunk_response_traits_type;
      typedef append_entry append_entry_type;
      typedef append_entry_traits append_entry_traits_type;
      typedef append_response append_entry_response_type;
      typedef append_entry_response_traits append_entry_response_traits_type;      
      typedef set_configuration_request_traits set_configuration_request_traits_type;

      typedef server_description configuration_description_server_type;
      typedef simple_configuration_description simple_configuration_description_type;
      typedef configuration_description configuration_description_type;
    };      
  }
}

#endif

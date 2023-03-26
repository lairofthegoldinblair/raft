#ifndef __RAFT_FLATBUFFER_MESSAGES_HH__
#define __RAFT_FLATBUFFER_MESSAGES_HH__

#include <boost/iterator/indirect_iterator.hpp>

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

    struct client_request_traits
    {
      typedef const raft_message * const_arg_type;
      typedef const raft_message * pinned_type;
      static const client_request *  get_client_request(const_arg_type ae)
      {
	return static_cast<const raft::fbs::client_request * >(ae->message());
      }      
      static slice get_command_data(const_arg_type cr)
      {
	return slice(reinterpret_cast<const uint8_t *>(get_client_request(cr)->command()->c_str()),
		     get_client_request(cr)->command()->size());
      }
    };
    
    struct append_entry_traits
    {
      typedef const raft_message * const_arg_type;
      typedef const raft_message * pinned_type;
      typedef flatbuffers::Vector<flatbuffers::Offset<log_entry>>::const_iterator pointer_iterator_type;
      typedef boost::indirect_iterator<pointer_iterator_type> iterator_type;
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
	return boost::make_indirect_iterator(get_append_entry(ae)->entry()->begin());
      }
      static iterator_type end_entries(const_arg_type ae)
      {
	return boost::make_indirect_iterator(get_append_entry(ae)->entry()->end());
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

    struct server_description_traits
    {
      typedef const raft::fbs::server_description * const_arg_type;

      static uint64_t id(const_arg_type msg)
      {
	return msg->id();
      }
      static slice address(const_arg_type msg)
      {
	return slice(reinterpret_cast<const uint8_t *>(msg->address()->c_str()), msg->address()->size());
      }
    };

    struct simple_configuration_description_traits
    {
      typedef const raft::fbs::simple_configuration_description * const_arg_type;
      typedef flatbuffers::Vector<flatbuffers::Offset<server_description>>::const_iterator pointer_iterator_type;
      typedef boost::indirect_iterator<pointer_iterator_type> iterator_type;

      static iterator_type begin_servers(const_arg_type msg)
      {
	return boost::make_indirect_iterator(msg->servers()->begin());
      }
      static iterator_type end_servers(const_arg_type msg)
      {
	return boost::make_indirect_iterator(msg->servers()->end());
      }
    };

    struct configuration_description_traits
    {
      typedef const raft::fbs::configuration_description * const_arg_type;
      static const raft::fbs::simple_configuration_description & from(const_arg_type msg)
      {
	return *msg->from();
      }
      static const raft::fbs::simple_configuration_description & to(const_arg_type msg)
      {
	return *msg->to();
      }
    };

  struct log_entry_traits
  {
    typedef raft::fbs::log_entry value_type;
    typedef const value_type * const_arg_type;

    static const raft::fbs::log_entry *  get_log_entry(const_arg_type ae)
    {
      return ae;
    }      
    static uint64_t term(const_arg_type msg)
    {
      return get_log_entry(msg)->term();
    }
    static bool is_command(const_arg_type msg)
    {
      return raft::fbs::log_entry_type_COMMAND == get_log_entry(msg)->type();
    }
    static bool is_configuration(const_arg_type msg)
    {
      return raft::fbs::log_entry_type_CONFIGURATION == get_log_entry(msg)->type();
    }
    static bool is_noop(const_arg_type msg)
    {
      return raft::fbs::log_entry_type_NOOP == get_log_entry(msg)->type();
    }
    static slice data(const_arg_type msg)
    {
      return slice(reinterpret_cast<const uint8_t *>(get_log_entry(msg)->data()->c_str()), get_log_entry(msg)->data()->size());
    }
    static const raft::fbs::configuration_description & configuration(const_arg_type msg)
    {
      return *get_log_entry(msg)->configuration();
    }
    static std::pair<log_entry_traits::const_arg_type, std::function<void()> > create_command(uint64_t term, client_request_traits::const_arg_type req)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_type(log_entry_type_COMMAND);
      // TODO: Would be better to avoid the copy and transfer ownership of the req memory to the log entry
      auto cmd = client_request_traits::get_command_data(req);
      auto data = fbb->CreateString(slice::buffer_cast<const char *>(cmd), slice::buffer_size(cmd));
      leb.add_data(data);
      auto le = leb.Finish();
      fbb->Finish(le);
      
      return std::pair<log_entry_traits::const_arg_type, std::function<void()> >(::flatbuffers::GetRoot<raft::fbs::log_entry>(fbb->GetBufferPointer()),
										 [fbb]() { delete fbb; });
    }
    static std::pair<log_entry_traits::const_arg_type, std::function<void()> > create_noop(uint64_t term)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_type(log_entry_type_NOOP);
      auto le = leb.Finish();
      fbb->Finish(le);
      
      return std::pair<log_entry_traits::const_arg_type, std::function<void()> >(::flatbuffers::GetRoot<raft::fbs::log_entry>(fbb->GetBufferPointer()),
										 [fbb]() { delete fbb; });
    }
    static std::pair<log_entry_traits::const_arg_type, std::function<void()> > create_configuration_from_message(uint64_t term,  const raft::fbs::configuration_description * c)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
      for(auto s = c->from()->servers()->begin(), e = c->from()->servers()->end(); s != e; ++s) {
	servers.push_back(raft::fbs::Createserver_description(*fbb, s->id(), fbb->CreateString(s->address())));
      }
      auto from_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      servers.clear();
      for(auto s = c->to()->servers()->begin(), e = c->to()->servers()->end(); s != e; ++s) {
	servers.push_back(raft::fbs::Createserver_description(*fbb, s->id(), fbb->CreateString(s->address())));
      }
      auto to_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      auto cfg = Createconfiguration_description(*fbb, from_config, to_config);
      
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_type(log_entry_type_CONFIGURATION);
      leb.add_configuration(cfg);
      auto le = leb.Finish();
      fbb->Finish(le);
      
      return std::pair<log_entry_traits::const_arg_type, std::function<void()> >(::flatbuffers::GetRoot<raft::fbs::log_entry>(fbb->GetBufferPointer()),
										 [fbb]() { delete fbb; });
    }
    template<typename _ConfigView>
    static std::pair<log_entry_traits::const_arg_type, std::function<void()> > create_configuration(uint64_t term,  const _ConfigView & config)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
      for(auto i=0; i<config.from_size(); ++i) {
	servers.push_back(raft::fbs::Createserver_description(*fbb, config.from_id(i), fbb->CreateString(config.from_address(i))));
      }
      auto from_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      servers.clear();
      for(auto i=0; i<config.to_size(); ++i) {
	servers.push_back(raft::fbs::Createserver_description(*fbb, config.to_id(i), fbb->CreateString(config.to_address(i))));
      }
      auto to_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      auto cfg = Createconfiguration_description(*fbb, from_config, to_config);
      
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_type(log_entry_type_CONFIGURATION);
      leb.add_configuration(cfg);
      auto le = leb.Finish();
      fbb->Finish(le);
      
      return std::pair<log_entry_traits::const_arg_type, std::function<void()> >(::flatbuffers::GetRoot<raft::fbs::log_entry>(fbb->GetBufferPointer()),
										 [fbb]() { delete fbb; });
    }
    static std::pair<log_entry_traits::const_arg_type, std::function<void()> > create_bootstrap_log_entry(uint64_t id, const char * address)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
	servers.push_back(raft::fbs::Createserver_description(*fbb, id, fbb->CreateString(address)));
      auto from_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      servers.clear();
      auto to_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      auto cfg = Createconfiguration_description(*fbb, from_config, to_config);
      
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(0);
      leb.add_type(log_entry_type_CONFIGURATION);
      leb.add_configuration(cfg);
      auto le = leb.Finish();
      fbb->Finish(le);
      
      return std::pair<log_entry_traits::const_arg_type, std::function<void()> >(::flatbuffers::GetRoot<raft::fbs::log_entry>(fbb->GetBufferPointer()),
										 [fbb]() { delete fbb; });
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
	  const auto & e = entries(i);
	  if (log_entry_traits::is_command(&e)) {
	    auto tmp = log_entry_traits::data(&e);
	    auto str = fbb->CreateString(slice::buffer_cast<const char *>(tmp),
					 slice::buffer_size(tmp));
	    raft::fbs::log_entryBuilder leb(*fbb);
	    leb.add_type(raft::fbs::log_entry_type_COMMAND);
	    leb.add_term(log_entry_traits::term(&e));
	    leb.add_data(str);
	    entries_vec.push_back(leb.Finish());
	  } else if (log_entry_traits::is_configuration(&e)) {
	    std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers_vec;
	    const auto & cfg(log_entry_traits::configuration(&e));
	    const auto & from(configuration_description_traits::from(&cfg));
	    for(auto it = simple_configuration_description_traits::begin_servers(&from), e = simple_configuration_description_traits::end_servers(&from);
		it != e; ++it) {
	      auto slce = server_description_traits::address(&*it);
	      servers_vec.push_back(raft::fbs::Createserver_description(*fbb,
									server_description_traits::id(&*it),
									fbb->CreateString(slice::buffer_cast<const char *>(slce),
											  slice::buffer_size(slce))));
	    }
	    auto from_desc = raft::fbs::Createsimple_configuration_description(*fbb, fbb->CreateVector(servers_vec));
	    servers_vec.clear();
	    const auto & to(configuration_description_traits::to(&cfg));
	    for(auto it = simple_configuration_description_traits::begin_servers(&to), e = simple_configuration_description_traits::end_servers(&to);
		it != e; ++it) {
	      auto slce = server_description_traits::address(&*it);
	      servers_vec.push_back(raft::fbs::Createserver_description(*fbb,
									server_description_traits::id(&*it),
									fbb->CreateString(slice::buffer_cast<const char *>(slce),
											  slice::buffer_size(slce))));
	    }
	    auto to_desc = raft::fbs::Createsimple_configuration_description(*fbb, fbb->CreateVector(servers_vec));
	    auto conf = raft::fbs::Createconfiguration_description(*fbb, from_desc, to_desc);
	    raft::fbs::log_entryBuilder leb(*fbb);
	    leb.add_type(raft::fbs::log_entry_type_CONFIGURATION);
	    leb.add_term(log_entry_traits::term(&e));
	    leb.add_configuration(conf);
	    entries_vec.push_back(leb.Finish());
	  } else {
	    raft::fbs::log_entryBuilder leb(*fbb);
	    leb.add_type(raft::fbs::log_entry_type_NOOP);
	    leb.add_term(log_entry_traits::term(&e));
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

    template<typename _Communicator>
    class request_vote_sender
    {
    private:
      _Communicator & comm_;
      typename _Communicator::endpoint ep_;
      std::string address_;      
	
    public:
      request_vote_sender(_Communicator & comm, typename _Communicator::endpoint ep, const std::string & addr)
	:
	comm_(comm),
	ep_(ep),
	address_(addr)
      {
      }

      void send(uint64_t recipient_id,
		uint64_t term_number,
		uint64_t candidate_id,
		uint64_t last_log_index,
		uint64_t last_log_term)
      {
	flatbuffer_builder_adapter fbb;
	raft::fbs::request_voteBuilder aeb(*fbb);
	aeb.add_recipient_id(recipient_id);
	aeb.add_term_number(term_number);
	aeb.add_candidate_id(candidate_id);
	aeb.add_last_log_index(last_log_index);
	aeb.add_last_log_term(last_log_term);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(*fbb, raft::fbs::any_message_request_vote, ae.Union());
	// Finish and get buffer
	fbb->Finish(m);
	// Send on to communicator
	comm_.send(ep_, address_, std::move(fbb));
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

    template<typename _Communicator>
    class vote_response_sender
    {
    private:
      _Communicator & comm_;
      typename _Communicator::endpoint ep_;
      std::string address_;      
	
    public:
      vote_response_sender(_Communicator & comm, typename _Communicator::endpoint ep, const std::string & addr)
	:
	comm_(comm),
	ep_(ep),
	address_(addr)
      {
      }

      void send(uint64_t peer_id,
		uint64_t term_number,
		uint64_t request_term_number,
		bool granted)
      {
	flatbuffer_builder_adapter fbb;
	raft::fbs::vote_responseBuilder aeb(*fbb);
	aeb.add_peer_id(peer_id);
	aeb.add_term_number(term_number);
	aeb.add_request_term_number(request_term_number);
	aeb.add_granted(granted);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(*fbb, raft::fbs::any_message_vote_response, ae.Union());
	// Finish and get buffer
	fbb->Finish(m);
	// Send on to communicator
	comm_.send(ep_, address_, std::move(fbb));
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

    template<typename _Communicator>
    class append_entry_response_sender
    {
    private:
      _Communicator & comm_;
      typename _Communicator::endpoint ep_;
      std::string address_;      
	
    public:
      append_entry_response_sender(_Communicator & comm, typename _Communicator::endpoint ep, const std::string & addr)
	:
	comm_(comm),
	ep_(ep),
	address_(addr)
      {
      }

      void send(uint64_t recipient_id,
		uint64_t term_number,
		uint64_t request_term_number,
		uint64_t begin_index,
		uint64_t last_index,
		bool success)
      {
	flatbuffer_builder_adapter fbb;
	raft::fbs::append_responseBuilder aeb(*fbb);
	aeb.add_recipient_id(recipient_id);
	aeb.add_term_number(term_number);
	aeb.add_request_term_number(request_term_number);
	aeb.add_begin_index(begin_index);
	aeb.add_last_index(last_index);
	aeb.add_success(success);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(*fbb, raft::fbs::any_message_append_response, ae.Union());
	// Finish and get buffer
	fbb->Finish(m);
	// Send on to communicator
	comm_.send(ep_, address_, std::move(fbb));
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
  
    template<typename _Communicator>
    class append_checkpoint_chunk_response_sender
    {
    private:
      _Communicator & comm_;
      typename _Communicator::endpoint ep_;
      std::string address_;      
	
    public:
      append_checkpoint_chunk_response_sender(_Communicator & comm, typename _Communicator::endpoint ep, const std::string & addr)
	:
	comm_(comm),
	ep_(ep),
	address_(addr)
      {
      }

      void send(uint64_t recipient_id,
		uint64_t term_number,
		uint64_t request_term_number,
		uint64_t bytes_stored)
      {
	flatbuffer_builder_adapter fbb;
	raft::fbs::append_checkpoint_chunk_responseBuilder aeb(*fbb);
	aeb.add_recipient_id(recipient_id);
	aeb.add_term_number(term_number);
	aeb.add_request_term_number(request_term_number);
	aeb.add_bytes_stored(bytes_stored);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(*fbb, raft::fbs::any_message_append_checkpoint_chunk_response, ae.Union());
	// Finish and get buffer
	fbb->Finish(m);
	// Send on to communicator
	comm_.send(ep_, address_, std::move(fbb));
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

    class set_configuration_response_traits
    {
    public:
      typedef const raft_message * const_arg_type;    
    
      static const raft::fbs::set_configuration_response * scr(const_arg_type msg)
      {
	return static_cast<const raft::fbs::set_configuration_response * >(msg->message());
      }
    
      static raft::fbs::client_result result(const_arg_type msg)
      {
	return scr(msg)->result();
      }
      static std::size_t bad_servers_size(const_arg_type msg)
      {
	return scr(msg)->bad_servers()->servers()->size();
      }
      static uint64_t bad_servers_id(const_arg_type msg, std::size_t i)
      {
	return scr(msg)->bad_servers()->servers()->Get(i)->id();
      }
      static const char * bad_servers_address(const_arg_type msg, std::size_t i)
      {
	return scr(msg)->bad_servers()->servers()->Get(i)->address()->c_str();
      }
    };

    class messages
    {
    public:
      typedef log_entry log_entry_type;
      typedef log_entry_traits log_entry_traits_type;
      typedef client_request client_request_type;
      typedef client_request_traits client_request_traits_type;
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

      typedef server_description_traits server_description_traits_type;
      typedef simple_configuration_description_traits simple_configuration_description_traits_type;
      typedef configuration_description_traits configuration_description_traits_type;
      typedef set_configuration_request_traits set_configuration_request_traits_type;
      typedef set_configuration_response_traits set_configuration_response_traits_type;

      typedef server_description configuration_description_server_type;
      typedef simple_configuration_description simple_configuration_description_type;
      typedef configuration_description configuration_description_type;

      typedef raft::fbs::client_result client_result_type;
      static raft::fbs::client_result client_result_success() { return raft::fbs::client_result_SUCCESS; }
      static raft::fbs::client_result client_result_fail() { return raft::fbs::client_result_FAIL; }
      static raft::fbs::client_result client_result_retry() { return raft::fbs::client_result_RETRY; }
      static raft::fbs::client_result client_result_not_leader() { return raft::fbs::client_result_NOT_LEADER; }
    };      
  }
}

#endif

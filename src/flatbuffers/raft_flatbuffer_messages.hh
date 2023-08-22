#ifndef __RAFT_FLATBUFFER_MESSAGES_HH__
#define __RAFT_FLATBUFFER_MESSAGES_HH__

#include <memory>
#include <string_view>

#include <boost/assert.hpp>

#include "raft_generated.h"
#include "slice.hh"
#include "util/call_on_delete.hh"

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

      template <typename _Ty>
      const _Ty * get_root() const
      {
	return ::flatbuffers::GetRoot<_Ty>(fbb_->GetBufferPointer());
      }
      
      operator slice ()
      {
	return slice(fbb_->GetBufferPointer(), fbb_->GetSize());
      }
    };

    struct client_request_traits
    {
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      static const client_request *  get_client_request(const_arg_type ae)
      {
	return  ae.first->message_as_client_request();
      }      
      static slice get_command_data(const_arg_type cr)
      {
	return slice(reinterpret_cast<const uint8_t *>(get_client_request(cr)->command()->c_str()),
		     get_client_request(cr)->command()->size());
      }
    };

    struct client_response_traits
    {
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      static const client_response *  get_client_response(const_arg_type ae)
      {
	return  ae.first->message_as_client_response();
      }
      static client_result result(const_arg_type cr)
      {
        return get_client_response(cr)->result();
      }
      static uint64_t index(const_arg_type cr)
      {
        return get_client_response(cr)->index();
      }
      static uint64_t leader_id(const_arg_type cr)
      {
        return get_client_response(cr)->leader_id();
      }
      static slice response(const_arg_type msg)
      {
	return slice(reinterpret_cast<const uint8_t *>(get_client_response(msg)->response()->c_str()),
		     get_client_response(msg)->response()->size());
      }
    };
    // Returns a reference to the first bytes of data in raft::fbs::entries
    // struct get_log_entries_data
    // {
    //   const uint8_t & operator() (const log_entries * elt) const
    //   {
    // 	return *elt->entry()->Data();
    //   }
    // };

    struct append_entry_traits
    {
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      static const append_entry *  get_append_entry(const_arg_type ae)
      {
	// return ae->message_as_append_entry();
	return ae.first->message_as_append_entry();
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
	return nullptr != get_append_entry(ae)->entries() ? get_append_entry(ae)->entries()->size() : 0;
      }
      static const uint8_t & get_entry(const_arg_type ae, std::size_t i)
      {
	return *get_append_entry(ae)->entries()->Get(i)->entry()->Data();
      }
    };

    struct server_description_traits
    {
      typedef const raft::fbs::server_description * const_arg_type;

      static uint64_t id(const_arg_type msg)
      {
	return msg->id();
      }
      static std::string_view address(const_arg_type msg)
      {
	return std::string_view(msg->address()->c_str(), msg->address()->size());
      }
    };

    struct simple_configuration_description_traits
    {
      typedef const raft::fbs::simple_configuration_description * const_arg_type;

      static std::size_t size(const_arg_type msg)
      {
	return msg->servers()->size();
      }
      static const raft::fbs::server_description & get(const_arg_type msg, std::size_t i)
      {
	return *msg->servers()->Get(i);
      }
    };

    struct configuration_description_traits
    {
      // typedef const raft::fbs::configuration_description * const_arg_type;
      typedef const uint8_t * const_arg_type;

      static const raft::fbs::configuration_description * get_configuration_description(const_arg_type ae)
      {
	return ::flatbuffers::GetSizePrefixedRoot<raft::fbs::configuration_description>(ae);
      }
      
      static const raft::fbs::simple_configuration_description & from(const_arg_type msg)
      {
	return *get_configuration_description(msg)->from();
      }
      static const raft::fbs::simple_configuration_description & to(const_arg_type msg)
      {
	return *get_configuration_description(msg)->to();
      }
    };

    struct configuration_checkpoint_traits
    {
      typedef const raft::fbs::configuration_checkpoint * const_arg_type;
      static uint64_t index(const_arg_type msg)
      {
	return msg->index();
      }
      static const uint8_t & configuration(const_arg_type msg)
      {
	return *msg->configuration()->Data();
      }
    };

    struct checkpoint_header_traits
    {
      typedef const raft::fbs::checkpoint_header * const_arg_type;
      static uint64_t last_log_entry_index(const_arg_type msg)
      {
	return msg->last_log_entry_index();
      }
      static uint64_t last_log_entry_term(const_arg_type msg)
      {
	return msg->last_log_entry_term();
      }
      static uint64_t last_log_entry_cluster_time(const_arg_type msg)
      {
	return msg->last_log_entry_cluster_time();
      }
      static uint64_t index(const_arg_type msg)
      {
	return msg->configuration()->index();
      }
      static const uint8_t & configuration(const_arg_type msg)
      {
	return *msg->configuration()->configuration()->Data();
      }
      static std::pair<const_arg_type, raft::util::call_on_delete> build(uint64_t last_log_entry_index,
									 uint64_t last_log_entry_term,
									 uint64_t last_log_entry_cluster_time,
									 uint64_t configuration_index,
									 const uint8_t * configuration_description);
    };


  struct log_entry_traits
  {
    // Start of the size prefixed flat buffer 
    typedef const uint8_t * const_arg_type;

    static const raft::fbs::log_entry * get_log_entry(const_arg_type ae)
    {
      return ::flatbuffers::GetSizePrefixedRoot<raft::fbs::log_entry>(ae);
    }      
    static uint64_t term(const_arg_type msg)
    {
      return get_log_entry(msg)->term();
    }
    static uint64_t cluster_time(const_arg_type msg)
    {
      return get_log_entry(msg)->cluster_time();
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
    static const uint8_t & configuration(const_arg_type msg)
    {
      return *get_log_entry(msg)->configuration()->Data();
    }
    static std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > create_command(uint64_t term, uint64_t cluster_time, client_request_traits::const_arg_type req)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      // TODO: Would be better to avoid the copy and transfer ownership of the req memory to the log entry
      auto cmd = client_request_traits::get_command_data(req);
      auto data = fbb->CreateString(slice::buffer_cast<const char *>(cmd), slice::buffer_size(cmd));
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_cluster_time(cluster_time);
      leb.add_type(log_entry_type_COMMAND);
      leb.add_data(data);
      auto le = leb.Finish();
      fbb->FinishSizePrefixed(le);

      std::size_t size, offset;
      auto buf = fbb->ReleaseRaw(size, offset);
      delete fbb;
      return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
										 [buf]() { delete [] buf; });
    }
    static std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > create_command(uint64_t term,
                                                                                                   uint64_t cluster_time,
                                                                                                   std::pair<raft::slice, raft::util::call_on_delete> && req)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      // TODO: Would be better to avoid the copy and transfer ownership of the req memory to the log entry
      auto data = fbb->CreateString(slice::buffer_cast<const char *>(req.first), slice::buffer_size(req.first));
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_cluster_time(cluster_time);
      leb.add_type(log_entry_type_COMMAND);
      leb.add_data(data);
      auto le = leb.Finish();
      fbb->FinishSizePrefixed(le);

      std::size_t size, offset;
      auto buf = fbb->ReleaseRaw(size, offset);
      delete fbb;
      return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
										 [buf]() { delete [] buf; });
    }
    static std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > create_noop(uint64_t term, uint64_t cluster_time)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_cluster_time(cluster_time);
      leb.add_type(log_entry_type_NOOP);
      auto le = leb.Finish();
      fbb->FinishSizePrefixed(le);
      
      std::size_t size, offset;
      auto buf = fbb->ReleaseRaw(size, offset);
      delete fbb;
      return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
										 [buf]() { delete [] buf; });
    }
    static std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > create_configuration_from_message(uint64_t term, uint64_t cluster_time, const uint8_t * c)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      // std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
      // for(auto s = c->from()->servers()->begin(), e = c->from()->servers()->end(); s != e; ++s) {
      // 	servers.push_back(raft::fbs::Createserver_description(*fbb, s->id(), fbb->CreateString(s->address())));
      // }
      // auto from_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      // servers.clear();
      // for(auto s = c->to()->servers()->begin(), e = c->to()->servers()->end(); s != e; ++s) {
      // 	servers.push_back(raft::fbs::Createserver_description(*fbb, s->id(), fbb->CreateString(s->address())));
      // }
      // auto to_config = Createsimple_configuration_description(*fbb, fbb->CreateVector(servers));
      // auto cfg = Createconfiguration_description(*fbb, from_config, to_config);
      auto cfg = fbb->CreateVector<uint8_t>(c, ::flatbuffers::GetPrefixedSize(c)+sizeof(::flatbuffers::uoffset_t));
      
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_cluster_time(cluster_time);
      leb.add_type(log_entry_type_CONFIGURATION);
      leb.add_configuration(cfg);
      auto le = leb.Finish();
      fbb->FinishSizePrefixed(le);
      
      std::size_t size, offset;
      auto buf = fbb->ReleaseRaw(size, offset);
      delete fbb;
      return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
										 [buf]() { delete [] buf; });
    }
    template<typename _ConfigView>
    static std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > create_configuration(uint64_t term,  uint64_t cluster_time, const _ConfigView & config)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> cfg;
      {
	flatbuffers::FlatBufferBuilder nested_fbb;
	std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
	for(auto i=0; i<config.from_size(); ++i) {
	  servers.push_back(raft::fbs::Createserver_description(nested_fbb, config.from_id(i), nested_fbb.CreateString(config.from_address(i))));
	}
	auto from_config = Createsimple_configuration_description(nested_fbb, nested_fbb.CreateVector(servers));
	servers.clear();
	for(auto i=0; i<config.to_size(); ++i) {
	  servers.push_back(raft::fbs::Createserver_description(nested_fbb, config.to_id(i), nested_fbb.CreateString(config.to_address(i))));
	}
	auto to_config = Createsimple_configuration_description(nested_fbb, nested_fbb.CreateVector(servers));
	nested_fbb.FinishSizePrefixed(Createconfiguration_description(nested_fbb, from_config, to_config));
	cfg = fbb->CreateVector(nested_fbb.GetBufferPointer(), nested_fbb.GetSize());
      }
      
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(term);
      leb.add_cluster_time(cluster_time);
      leb.add_type(log_entry_type_CONFIGURATION);
      leb.add_configuration(cfg);
      auto le = leb.Finish();
      fbb->FinishSizePrefixed(le);
      
      std::size_t size, offset;
      auto buf = fbb->ReleaseRaw(size, offset);
      delete fbb;
      return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
										 [buf]() { delete [] buf; });
    }
    static std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > create_bootstrap_log_entry(uint64_t id, const char * address)
    {
      auto fbb = new flatbuffers::FlatBufferBuilder();
      ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> cfg;
      {
	flatbuffers::FlatBufferBuilder nested_fbb;
	std::vector<flatbuffers::Offset<raft::fbs::server_description>> servers;
	servers.push_back(raft::fbs::Createserver_description(nested_fbb, id, nested_fbb.CreateString(address)));
	auto from_config = Createsimple_configuration_description(nested_fbb, nested_fbb.CreateVector(servers));
	servers.clear();
	auto to_config = Createsimple_configuration_description(nested_fbb, nested_fbb.CreateVector(servers));
	nested_fbb.FinishSizePrefixed(Createconfiguration_description(nested_fbb, from_config, to_config));
	cfg = fbb->CreateVector(nested_fbb.GetBufferPointer(), nested_fbb.GetSize());
      }
      
      raft::fbs::log_entryBuilder leb(*fbb);
      leb.add_term(0);
      leb.add_cluster_time(0);
      leb.add_type(log_entry_type_CONFIGURATION);
      leb.add_configuration(cfg);
      auto le = leb.Finish();
      fbb->FinishSizePrefixed(le);
      
      std::size_t size, offset;
      auto buf = fbb->ReleaseRaw(size, offset);
      delete fbb;
      return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
										 [buf]() { delete [] buf; });
    }
  };
    

    struct request_vote_traits
    {
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;

      static const raft::fbs::request_vote * rv(const_arg_type msg)
      {
	return msg.first->message_as_request_vote();
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
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;

      static const raft::fbs::vote_response * vr(const_arg_type msg)
      {
	return msg.first->message_as_vote_response();
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
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;

      static const raft::fbs::append_response * aer(const_arg_type msg)
      {
	return msg.first->message_as_append_response();
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
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      // typedef const raft_message * const_arg_type;    
    
      static const raft::fbs::append_checkpoint_chunk * acc(const_arg_type msg)
      {
	return msg.first->message_as_append_checkpoint_chunk();
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
	return acc(msg)->last_checkpoint_header()->last_log_entry_index();
      }
      static uint64_t last_checkpoint_term(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_header()->last_log_entry_term();
      }
      static uint64_t last_checkpoint_cluster_time(const_arg_type msg)
      {
	return acc(msg)->last_checkpoint_header()->last_log_entry_cluster_time();
      }
      static const checkpoint_header & last_checkpoint_header(const_arg_type msg)
      {
	return *acc(msg)->last_checkpoint_header();
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
    };

    class append_checkpoint_chunk_response_traits
    {
    public:
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;

      static const raft::fbs::append_checkpoint_chunk_response * acc(const_arg_type msg)
      {
	return msg.first->message_as_append_checkpoint_chunk_response();
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
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
    
      static const raft::fbs::set_configuration_request * scr(const_arg_type msg)
      {
	return msg.first->message_as_set_configuration_request();
      }
    
      static const raft::fbs::simple_configuration_description & new_configuration(const_arg_type msg)
      {
	return *scr(msg)->new_configuration();
      }
      static uint64_t old_id(const_arg_type msg)
      {
	return scr(msg)->old_id();
      }
    };

    class set_configuration_response_traits
    {
    public:
      typedef std::pair<const raft_message *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
    
      static const raft::fbs::set_configuration_response * scr(const_arg_type msg)
      {
	return msg.first->message_as_set_configuration_response();
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
      static std::string_view bad_servers_address(const_arg_type msg, std::size_t i)
      {
	return std::string_view(scr(msg)->bad_servers()->servers()->Get(i)->address()->c_str(),
				scr(msg)->bad_servers()->servers()->Get(i)->address()->size());
      }
    };

    class open_session_request_traits
    {
    public:
      typedef std::pair<const log_entry_command *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      typedef const log_entry_command * const_view_type;
    
      static const raft::fbs::open_session_request * acc(const_arg_type msg)
      {
	return msg.first->command_as_open_session_request();
      }
    };
    
    class open_session_response_traits
    {
    public:
      typedef std::pair<const log_entry_command *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      typedef const log_entry_command * const_view_type;
    
      static const raft::fbs::open_session_response * acc(const_arg_type msg)
      {
	return msg.first->command_as_open_session_response();
      }
    
      static uint64_t session_id(const_arg_type msg)
      {
	return acc(msg)->session_id();
      }

      static const raft::fbs::open_session_response * acc(const_view_type msg)
      {
	return msg->command_as_open_session_response();
      }
    
      static uint64_t session_id(const_view_type msg)
      {
	return acc(msg)->session_id();
      }
    };
    
    class close_session_request_traits
    {
    public:
      typedef std::pair<const log_entry_command *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      typedef const log_entry_command * const_view_type;
    
      static const raft::fbs::close_session_request * acc(const_arg_type msg)
      {
	return msg.first->command_as_close_session_request();
      }
    
      static uint64_t session_id(const_arg_type msg)
      {
	return acc(msg)->session_id();
      }
    
      static const raft::fbs::close_session_request * acc(const_view_type msg)
      {
	return msg->command_as_close_session_request();
      }
    
      static uint64_t session_id(const_view_type msg)
      {
	return acc(msg)->session_id();
      }
    };
    
    class close_session_response_traits
    {
    public:
      typedef std::pair<const log_entry_command *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      typedef const log_entry_command * const_view_type;
      // typedef const raft_message * const_arg_type;    
    
      static const raft::fbs::close_session_response * acc(const_arg_type msg)
      {
	return msg.first->command_as_close_session_response();
      }
    };
    
    class linearizable_command_traits
    {
    public:
      typedef std::pair<const log_entry_command *, raft::util::call_on_delete> arg_type;
      typedef const arg_type & const_arg_type;
      typedef const log_entry_command * const_view_type;
    
      static const raft::fbs::linearizable_command * acc(const_arg_type msg)
      {
	return msg.first->command_as_linearizable_command();
      }
    
      static uint64_t session_id(const_arg_type msg)
      {
	return acc(msg)->session_id();
      }
    
      static uint64_t first_unacknowledged_sequence_number(const_arg_type msg)
      {
	return acc(msg)->first_unacknowledged_sequence_number();
      }
    
      static uint64_t sequence_number(const_arg_type msg)
      {
	return acc(msg)->sequence_number();
      }

      static slice command(const_arg_type msg)
      {
	return slice(reinterpret_cast<const uint8_t *>(acc(msg)->command()->c_str()),
		     acc(msg)->command()->size());
      }

      static const raft::fbs::linearizable_command * acc(const_view_type msg)
      {
	return msg->command_as_linearizable_command();
      }
    
      static uint64_t session_id(const_view_type msg)
      {
	return acc(msg)->session_id();
      }
    
      static uint64_t first_unacknowledged_sequence_number(const_view_type msg)
      {
	return acc(msg)->first_unacknowledged_sequence_number();
      }
    
      static uint64_t sequence_number(const_view_type msg)
      {
	return acc(msg)->sequence_number();
      }

      static slice command(const_view_type msg)
      {
	return slice(reinterpret_cast<const uint8_t *>(acc(msg)->command()->c_str()),
		     acc(msg)->command()->size());
      }
    };
    
    struct log_entry_command_traits
    {
      // Start of the size prefixed flat buffer 
      typedef const uint8_t * const_arg_type;

      static const raft::fbs::log_entry_command * get_log_entry_command(const_arg_type ae)
      {
        return ::flatbuffers::GetSizePrefixedRoot<raft::fbs::log_entry_command>(ae);
      }      
      static bool is_open_session(const_arg_type msg)
      {
        return any_log_entry_command_open_session_request == get_log_entry_command(msg)->command_type();
      }
      static bool is_close_session(const_arg_type msg)
      {
        return any_log_entry_command_close_session_request == get_log_entry_command(msg)->command_type();
      }
      static bool is_linearizable_command(const_arg_type msg)
      {
        return any_log_entry_command_linearizable_command == get_log_entry_command(msg)->command_type();
      }

      static open_session_request_traits::const_view_type open_session(const_arg_type msg)
      {
        return get_log_entry_command(msg);
      }
      static close_session_request_traits::const_view_type close_session(const_arg_type msg)
      {
        return get_log_entry_command(msg);
      }
      static linearizable_command_traits::const_view_type linearizable_command(const_arg_type msg)
      {
        return get_log_entry_command(msg);
      }
    };
    
    class messages
    {
    public:
      typedef uint8_t log_entry_type;
      typedef log_entry_traits log_entry_traits_type;
      typedef log_entry_command_traits log_entry_command_traits_type;
      typedef client_request client_request_type;
      typedef client_request_traits client_request_traits_type;
      typedef client_response client_response_type;
      typedef client_response_traits client_response_traits_type;
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
      typedef set_configuration_response_traits set_configuration_response_traits_type;

      typedef server_description configuration_description_server_type;
      typedef server_description_traits server_description_traits_type;
      typedef simple_configuration_description simple_configuration_description_type;
      typedef simple_configuration_description_traits simple_configuration_description_traits_type;
      typedef uint8_t configuration_description_type;
      typedef configuration_description_traits configuration_description_traits_type;
      typedef configuration_checkpoint configuration_checkpoint_type;
      typedef configuration_checkpoint_traits configuration_checkpoint_traits_type;
      typedef checkpoint_header checkpoint_header_type;
      typedef checkpoint_header_traits checkpoint_header_traits_type;

      typedef open_session_request open_session_request_type;
      typedef open_session_request_traits open_session_request_traits_type;
      typedef open_session_response open_session_response_type;
      typedef open_session_response_traits open_session_response_traits_type;
      typedef close_session_request close_session_request_type;
      typedef close_session_request_traits close_session_request_traits_type;
      typedef close_session_response close_session_response_type;
      typedef close_session_response_traits close_session_response_traits_type;
      typedef linearizable_command linearizable_command_type;
      typedef linearizable_command_traits linearizable_command_traits_type;

      typedef raft::fbs::client_result client_result_type;
      static raft::fbs::client_result client_result_success() { return raft::fbs::client_result_SUCCESS; }
      static raft::fbs::client_result client_result_fail() { return raft::fbs::client_result_FAIL; }
      static raft::fbs::client_result client_result_retry() { return raft::fbs::client_result_RETRY; }
      static raft::fbs::client_result client_result_not_leader() { return raft::fbs::client_result_NOT_LEADER; }
      static raft::fbs::client_result client_result_session_expired() { return raft::fbs::client_result_SESSION_EXPIRED; }
    };      

    template<typename _Derived, typename _FlatType>
    class raft_message_builder_base
    {
    public:
      typedef _FlatType fbs_type;
      typedef typename _FlatType::Builder fbs_builder_type;
    private:
      std::unique_ptr<flatbuffers::FlatBufferBuilder> fbb_;
    public:
      flatbuffers::FlatBufferBuilder & fbb()
      {
	if (!fbb_) {
	  fbb_ = std::make_unique<flatbuffers::FlatBufferBuilder>();
	}
	return *fbb_;
      }
      std::pair<const raft::fbs::raft_message *, raft::util::call_on_delete> finish()
      {
	static_cast<_Derived *>(this)->preinitialize();
	fbs_builder_type bld(fbb());
	static_cast<_Derived *>(this)->initialize(&bld);
	auto rv = bld.Finish();
	auto m = Createraft_message(fbb(), raft::fbs::any_messageTraits<fbs_type>::enum_value, rv.Union());
	fbb().FinishSizePrefixed(m);
	auto obj = GetSizePrefixedraft_message(fbb().GetBufferPointer());
	BOOST_ASSERT(fbb().GetBufferPointer()+sizeof(::flatbuffers::uoffset_t) == ::flatbuffers::GetBufferStartFromRootPointer(obj));
	auto ret = std::pair<const raft::fbs::raft_message *, raft::util::call_on_delete>(obj, [fbb = fbb_.release()]() { delete fbb; });
	return ret;
      }
    };

    template<typename _Derived, typename _FlatType>
    class nested_builder_base
    {
    public:
      typedef _FlatType fbs_type;
      typedef typename _FlatType::Builder fbs_builder_type;
    private:
      flatbuffers::FlatBufferBuilder & fbb_;
    protected:
      flatbuffers::FlatBufferBuilder & fbb()
      {
	return fbb_;
      }
    public:
      nested_builder_base(flatbuffers::FlatBufferBuilder & fbb)
	:
	fbb_(fbb)
      {
      }
      ::flatbuffers::Offset<fbs_type> finish()
      {
	static_cast<_Derived *>(this)->preinitialize();
	fbs_builder_type bld(fbb_);
	static_cast<_Derived *>(this)->initialize(&bld);
	return bld.Finish();
      }
    };

    class server_description_builder : public nested_builder_base<server_description_builder, raft::fbs::server_description>
    {
    private:
      uint64_t id_ = 0;
      flatbuffers::Offset<flatbuffers::String> address_;
      typedef std::function<void(::flatbuffers::Offset<fbs_type>)> finisher_type;
      std::unique_ptr<finisher_type> finisher_;
    public:
      server_description_builder(flatbuffers::FlatBufferBuilder & fbb)
	:
	nested_builder_base<server_description_builder, raft::fbs::server_description>(fbb)
      {
      }

      template<typename _Function>
      server_description_builder(flatbuffers::FlatBufferBuilder & fbb, _Function && f)
	:
	nested_builder_base<server_description_builder, raft::fbs::server_description>(fbb),
	finisher_(new finisher_type(std::move(f)))
      {
      }

      ~server_description_builder()
      {
	if (finisher_) {
	  (*finisher_)(finish());
	}
      }

      void preinitialize()
      {
      }
      void initialize(fbs_builder_type * bld)
      {
	bld->add_id(id_);
	bld->add_address(address_);
      }
      server_description_builder & id(uint64_t val)
      {
	id_ = val;
	return *this;
      }
      server_description_builder & address(const char * val)
      {
	address_ = fbb().CreateString(val);
	return *this;
      }
    };

    class simple_configuration_description_builder : public nested_builder_base<simple_configuration_description_builder, raft::fbs::simple_configuration_description>
    {
    private:
      std::vector<::flatbuffers::Offset<raft::fbs::server_description>> servers_;
      ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<raft::fbs::server_description>>> servers_offset_;
      typedef std::function<void(::flatbuffers::Offset<fbs_type>)> finisher_type;
      std::unique_ptr<finisher_type> finisher_;
    public:
      simple_configuration_description_builder(flatbuffers::FlatBufferBuilder & fbb)
	:
	nested_builder_base<simple_configuration_description_builder, raft::fbs::simple_configuration_description>(fbb)
      {
      }

      template<typename _Function>
      simple_configuration_description_builder(flatbuffers::FlatBufferBuilder & fbb, _Function && f)
	:
	nested_builder_base<simple_configuration_description_builder, raft::fbs::simple_configuration_description>(fbb),
	finisher_(new finisher_type(std::move(f)))
      {
      }

      ~simple_configuration_description_builder()
      {
	if (finisher_) {
	  (*finisher_)(finish());
	}
      }

      void preinitialize()
      {
	servers_offset_ = fbb().CreateVector(servers_);
      }
      
      void initialize(fbs_builder_type * bld)
      {
	bld->add_servers(servers_offset_);
      }
      simple_configuration_description_builder & server(::flatbuffers::Offset<raft::fbs::server_description> server)
      {
	servers_.push_back(server);
	return *this;
      }
      server_description_builder server()
      {
	return server_description_builder(fbb(), [this](::flatbuffers::Offset<raft::fbs::server_description> s) { this->server(s); });
      }
    };

    class configuration_description_builder
    {
    private:
      std::unique_ptr<flatbuffers::FlatBufferBuilder> fbb_;
      ::flatbuffers::Offset<raft::fbs::simple_configuration_description> from_;
      ::flatbuffers::Offset<raft::fbs::simple_configuration_description> to_;
      typedef std::function<void(const std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete> &)> finisher_type;
      std::unique_ptr<finisher_type> finisher_;

      flatbuffers::FlatBufferBuilder & fbb()
      {
	if (!fbb_) {
	  fbb_ = std::make_unique<flatbuffers::FlatBufferBuilder>();
	}
	return *fbb_;
      }
    public:
      configuration_description_builder()
      {
      }

      template<typename _Function>
      configuration_description_builder(_Function && f)
	:
	finisher_(new finisher_type(std::move(f)))
      {
      }

      ~configuration_description_builder()
      {
	if (finisher_) {
	  (*finisher_)(finish());
	}
      }

      configuration_description_builder & from(::flatbuffers::Offset<raft::fbs::simple_configuration_description> val)
      {
	from_ = val;
	return *this;
      }
      simple_configuration_description_builder from()
      {
	return simple_configuration_description_builder(fbb(), [this](::flatbuffers::Offset<raft::fbs::simple_configuration_description> val) { this->from(val); });
      }
      configuration_description_builder & to(::flatbuffers::Offset<raft::fbs::simple_configuration_description> val)
      {
	to_ = val;
	return *this;
      }
simple_configuration_description_builder to()
      {
	return simple_configuration_description_builder(fbb(), [this](::flatbuffers::Offset<raft::fbs::simple_configuration_description> val) { this->to(val); });
      }
      std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete> finish()
      {
	raft::fbs::configuration_descriptionBuilder cdb(fbb());
	cdb.add_from(from_);
	cdb.add_to(to_);
	fbb().FinishSizePrefixed(cdb.Finish());
      
	std::size_t size, offset;
	auto buf = fbb().ReleaseRaw(size, offset);
	delete fbb_.release();
	return std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
													[buf]() { delete [] buf; });
      }
    };

    class checkpoint_header_builder : public nested_builder_base<checkpoint_header_builder, raft::fbs::checkpoint_header>
    {
    private:
      uint64_t last_log_entry_index_ = 0;
      uint64_t last_log_entry_term_ = 0;
      uint64_t last_log_entry_cluster_time_ = 0;
      uint64_t configuration_log_index_ = 0;
      ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> configuration_;
      ::flatbuffers::Offset<raft::fbs::configuration_checkpoint> checkpoint_;
      typedef std::function<void(::flatbuffers::Offset<fbs_type>)> finisher_type;
      std::unique_ptr<finisher_type> finisher_;
    public:
      checkpoint_header_builder(flatbuffers::FlatBufferBuilder & fbb)
	:
	nested_builder_base<checkpoint_header_builder, raft::fbs::checkpoint_header>(fbb)
      {
      }

      template<typename _Function>
      checkpoint_header_builder(flatbuffers::FlatBufferBuilder & fbb, _Function && f)
	:
	nested_builder_base<checkpoint_header_builder, raft::fbs::checkpoint_header>(fbb),
	finisher_(new finisher_type(std::move(f)))
      {
      }

      ~checkpoint_header_builder()
      {
	if (finisher_) {
	  (*finisher_)(finish());
	}
      }

      void preinitialize()
      {
	raft::fbs::configuration_checkpointBuilder bld(fbb());
	bld.add_index(configuration_log_index_);
	bld.add_configuration(configuration_);
	checkpoint_ = bld.Finish();
      }      
      void initialize(fbs_builder_type * bld)
      {
	bld->add_last_log_entry_index(last_log_entry_index_);
	bld->add_last_log_entry_term(last_log_entry_term_);
	bld->add_last_log_entry_cluster_time(last_log_entry_cluster_time_);
	bld->add_configuration(checkpoint_);
      }
      checkpoint_header_builder & last_log_entry_index(uint64_t val)
      {
	last_log_entry_index_ = val;
	return *this;
      }
      checkpoint_header_builder & last_log_entry_term(uint64_t val)
      {
	last_log_entry_term_ = val;
	return *this;
      }

      checkpoint_header_builder & last_log_entry_cluster_time(uint64_t val)
      {
	last_log_entry_cluster_time_ = val;
	return *this;
      }

      checkpoint_header_builder & index(uint64_t val)
      {
	configuration_log_index_ = val;
	return *this;
      }
      checkpoint_header_builder & configuration(const uint8_t & e)
      {
	// Configuration descriptions are nested flatbuffers so we can just memcpy them into message
	// We use size prefixed buffers so the first uoffset_t is the size of the flatbuffer
	// (does not include the size of the size prefix itself)
	configuration_ = fbb().CreateVector<uint8_t>(&e, ::flatbuffers::GetPrefixedSize(&e)+sizeof(::flatbuffers::uoffset_t));
	return *this;
      }
      checkpoint_header_builder & configuration(const std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete > & val)
      {
	return configuration(*val.first);
      }
      configuration_description_builder configuration()
      {
	return configuration_description_builder([this](const std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete > & val) { this->configuration(val); });
      }
    };

    std::pair<checkpoint_header_traits::const_arg_type, raft::util::call_on_delete> checkpoint_header_traits::build(uint64_t last_log_entry_index,
														    uint64_t last_log_entry_term,
														    uint64_t last_log_entry_cluster_time,
														    uint64_t configuration_index,
														    const uint8_t * configuration_description)
    {
      auto fbb = std::make_unique<::flatbuffers::FlatBufferBuilder>();
      fbb->Finish(checkpoint_header_builder(*fbb).last_log_entry_index(last_log_entry_index).last_log_entry_term(last_log_entry_term).last_log_entry_cluster_time(last_log_entry_cluster_time).index(configuration_index).configuration(*configuration_description).finish());
      auto ptr = ::flatbuffers::GetRoot<raft::fbs::checkpoint_header>(fbb->GetBufferPointer());
      return std::pair<checkpoint_header_traits::const_arg_type, raft::util::call_on_delete>(ptr, [f = std::move(fbb)](){});
    }
    
    class request_vote_builder : public raft_message_builder_base<request_vote_builder, raft::fbs::request_vote>
    {
    private:
      uint64_t recipient_id_ = 0;
      uint64_t term_number_ = 0;
      uint64_t candidate_id_ = 0;
      uint64_t last_log_index_ = 0;
      uint64_t last_log_term_ = 0;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	bld->add_recipient_id(recipient_id_);
	bld->add_term_number(term_number_);
	bld->add_candidate_id(candidate_id_);
	bld->add_last_log_index(last_log_index_);
	bld->add_last_log_term(last_log_term_);
      }
      request_vote_builder & recipient_id(uint64_t val)
      {
	recipient_id_ = val;
	return *this;
      }
      request_vote_builder & term_number(uint64_t val)
      {
	term_number_ = val;
	return *this;
      }
      request_vote_builder & candidate_id(uint64_t val)
      {
	candidate_id_ = val;
	return *this;
      }
      request_vote_builder & last_log_index(uint64_t val)
      {
	last_log_index_ = val;
	return *this;
      }
      request_vote_builder & last_log_term(uint64_t val)
      {
	last_log_term_ = val;
	return *this;
      }
    };

    class vote_response_builder : public raft_message_builder_base<vote_response_builder, raft::fbs::vote_response>
    {
    private:
      uint64_t peer_id_ = 0;
      uint64_t term_number_ = 0;
      uint64_t request_term_number_ = 0;
      bool granted_ = false;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	  bld->add_peer_id(peer_id_);
	  bld->add_term_number(term_number_);
	  bld->add_request_term_number(request_term_number_);
	  bld->add_granted(granted_);
      }	
      vote_response_builder & peer_id(uint64_t val)
      {
	peer_id_ = val;
	return *this;
      }
      vote_response_builder & term_number(uint64_t val)
      {
	term_number_ = val;
	return *this;
      }
      vote_response_builder & request_term_number(uint64_t val)
      {
	request_term_number_ = val;
	return *this;
      }
      vote_response_builder & granted(bool val)
      {
	granted_ = val;
	return *this;
      }
    };

    class client_request_builder : public raft_message_builder_base<client_request_builder, raft::fbs::client_request>
    {
    private:
      ::flatbuffers::Offset<::flatbuffers::String> command_;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	bld->add_command(command_);
      }
      client_request_builder & command(raft::slice && val)
      {
	command_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
				      raft::slice::buffer_size(val));
	return *this;
      }
    };

    class client_response_builder : public raft_message_builder_base<client_response_builder, raft::fbs::client_response>
    {
    private:
      client_result result_= client_result_FAIL;
      uint64_t index_ = 0;
      uint64_t leader_id_ = 0;
      ::flatbuffers::Offset<::flatbuffers::String> response_;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	bld->add_result(result_);
	bld->add_index(index_);
	bld->add_leader_id(leader_id_);
	bld->add_response(response_);
      }
      client_response_builder & result(client_result val)
      {
	result_ = val;
	return *this;
      }
      client_response_builder & index(uint64_t val)
      {
	index_ = val;
	return *this;
      }
      client_response_builder & leader_id(uint64_t val)
      {
	leader_id_ = val;
	return *this;
      }
      client_response_builder & response(raft::slice && val)
      {
	response_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
                                       raft::slice::buffer_size(val));
	return *this;
      }
    };

    class append_entry_builder : public raft_message_builder_base<append_entry_builder, raft::fbs::append_entry>
    {
    private:
      uint64_t recipient_id_ = 0;
      uint64_t term_number_ = 0;
      uint64_t leader_id_ = 0;
      uint64_t previous_log_index_ = 0;
      uint64_t previous_log_term_ = 0;
      uint64_t leader_commit_index_ = 0;
      std::vector<flatbuffers::Offset<raft::fbs::log_entries>> entries_vec_;
      ::flatbuffers::Offset<::flatbuffers::Vector<::flatbuffers::Offset<raft::fbs::log_entries>>> entries_;
    public:
      void preinitialize()
      {
	entries_ = fbb().CreateVector(entries_vec_);  
      }

      void initialize(fbs_builder_type * bld)
      {
	bld->add_recipient_id(recipient_id_);
	bld->add_term_number(term_number_);
	bld->add_leader_id(leader_id_);
	bld->add_previous_log_index(previous_log_index_);
	bld->add_previous_log_term(previous_log_term_);
	bld->add_leader_commit_index(leader_commit_index_);
	bld->add_entries(entries_);
      }

      append_entry_builder & recipient_id(uint64_t val)
      {
	recipient_id_ = val;
	return *this;
      }
      append_entry_builder & term_number(uint64_t val)
      {
	term_number_ = val;
	return *this;
      }
      append_entry_builder & leader_id(uint64_t val)
      {
	leader_id_ = val;
	return *this;
      }
      append_entry_builder & previous_log_index(uint64_t val)
      {
	previous_log_index_ = val;
	return *this;
      }
      append_entry_builder & previous_log_term(uint64_t val)
      {
	previous_log_term_ = val;
	return *this;
      }
      append_entry_builder & leader_commit_index(uint64_t val)
      {
	leader_commit_index_ = val;
	return *this;
      }
      append_entry_builder & entry(const uint8_t & e)
      {
	// Log entries are nested flatbuffers so we can just memcpy them into message
	  // We use size prefixed buffers so the first uoffset_t is the size of the flatbuffer
	  // (does not include the size of the size prefix itself)
	auto v = fbb().CreateVector<uint8_t>(&e, ::flatbuffers::GetPrefixedSize(&e)+sizeof(::flatbuffers::uoffset_t));
	raft::fbs::log_entriesBuilder leb(fbb());
	leb.add_entry(v);
	entries_vec_.push_back(leb.Finish());
	return *this;
      }
      append_entry_builder & entry(const std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete > & val)
      {
	return entry(*val.first);
      }
    };

    class append_response_builder : public raft_message_builder_base<append_response_builder, raft::fbs::append_response>
    {
    private:
      uint64_t recipient_id_ = 0;
      uint64_t term_number_ = 0;
      uint64_t request_term_number_ = 0;
      uint64_t begin_index_ = 0;
      uint64_t last_index_ = 0;
      bool success_ = false;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	  bld->add_recipient_id(recipient_id_);
	  bld->add_term_number(term_number_);
	  bld->add_request_term_number(request_term_number_);
	  bld->add_begin_index(begin_index_);
	  bld->add_last_index(last_index_);
	  bld->add_success(success_);
      }	
      append_response_builder & recipient_id(uint64_t val)
      {
	recipient_id_ = val;
	return *this;
      }
      append_response_builder & term_number(uint64_t val)
      {
	term_number_ = val;
	return *this;
      }
      append_response_builder & request_term_number(uint64_t val)
      {
	request_term_number_ = val;
	return *this;
      }
      append_response_builder & begin_index(uint64_t val)
      {
	begin_index_ = val;
	return *this;
      }
      append_response_builder & last_index(uint64_t val)
      {
	last_index_ = val;
	return *this;
      }
      append_response_builder & success(bool val)
      {
	success_ = val;
	return *this;
      }
    };

    class append_checkpoint_chunk_builder : public raft_message_builder_base<append_checkpoint_chunk_builder, raft::fbs::append_checkpoint_chunk>
    {
    private:
      uint64_t recipient_id_ = 0;
      uint64_t term_number_ = 0;
      uint64_t leader_id_ = 0;
      uint64_t checkpoint_begin_ = 0;
      uint64_t checkpoint_end_ = 0;
      ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> data_;
      ::flatbuffers::Offset<raft::fbs::checkpoint_header> last_checkpoint_header_;
      bool checkpoint_done_ = 0;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	bld->add_recipient_id(recipient_id_);
	bld->add_term_number(term_number_);
	bld->add_leader_id(leader_id_);
	bld->add_checkpoint_begin(checkpoint_begin_);
	bld->add_checkpoint_end(checkpoint_end_);
	bld->add_data(data_);
	bld->add_last_checkpoint_header(last_checkpoint_header_);
	bld->add_checkpoint_done(checkpoint_done_);
      }
      append_checkpoint_chunk_builder & recipient_id(uint64_t val)
      {
	recipient_id_ = val;
	return *this;
      }
      append_checkpoint_chunk_builder & term_number(uint64_t val)
      {
	term_number_ = val;
	return *this;
      }
      append_checkpoint_chunk_builder & leader_id(uint64_t val)
      {
	leader_id_ = val;
	return *this;
      }
      append_checkpoint_chunk_builder & checkpoint_begin(uint64_t val)
      {
	checkpoint_begin_ = val;
	return *this;
      }
      append_checkpoint_chunk_builder & checkpoint_end(uint64_t val)
      {
	checkpoint_end_ = val;
	return *this;
      }
      append_checkpoint_chunk_builder & checkpoint_done(bool val)
      {
	checkpoint_done_ = val;
	return *this;
      }
      append_checkpoint_chunk_builder & data(raft::slice && val)
      {
	data_ = fbb().CreateVector(raft::slice::buffer_cast<const uint8_t *>(val),
				   raft::slice::buffer_size(val));
	return *this;
      }
      append_checkpoint_chunk_builder & last_checkpoint_header(::flatbuffers::Offset<raft::fbs::checkpoint_header> val)
      {
	last_checkpoint_header_ = val;
	return *this;
      }
      checkpoint_header_builder last_checkpoint_header()
      {
	return checkpoint_header_builder(fbb(), [this](::flatbuffers::Offset<raft::fbs::checkpoint_header> val) { this->last_checkpoint_header(val); });
      }
    };

    class append_checkpoint_chunk_response_builder : public raft_message_builder_base<append_checkpoint_chunk_response_builder, raft::fbs::append_checkpoint_chunk_response>
    {
    private:
      uint64_t recipient_id_ = 0;
      uint64_t term_number_ = 0;
      uint64_t request_term_number_ = 0;
      uint64_t bytes_stored_ = 0;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	  bld->add_recipient_id(recipient_id_);
	  bld->add_term_number(term_number_);
	  bld->add_request_term_number(request_term_number_);
	  bld->add_bytes_stored(bytes_stored_);
      }	
      append_checkpoint_chunk_response_builder & recipient_id(uint64_t val)
      {
	recipient_id_ = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & term_number(uint64_t val)
      {
	term_number_ = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & request_term_number(uint64_t val)
      {
	request_term_number_ = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & bytes_stored(uint64_t val)
      {
	bytes_stored_ = val;
	return *this;
      }
    };

    class set_configuration_request_builder : public raft_message_builder_base<set_configuration_request_builder, raft::fbs::set_configuration_request>
    {
    private:
      uint64_t old_id_ = 0;
      ::flatbuffers::Offset<raft::fbs::simple_configuration_description> new_configuration_;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	  bld->add_old_id(old_id_);
	  bld->add_new_configuration(new_configuration_);
      }	
      set_configuration_request_builder & old_id(uint64_t val)
      {
	old_id_ = val;
	return *this;
      }
      set_configuration_request_builder & new_configuration(::flatbuffers::Offset<raft::fbs::simple_configuration_description> val)
      {
	new_configuration_ = val;
	return *this;
      }
      simple_configuration_description_builder new_configuration()
      {
	return simple_configuration_description_builder(fbb(), [this](::flatbuffers::Offset<raft::fbs::simple_configuration_description> val) { this->new_configuration(val); });
      }
    };

    class set_configuration_response_builder : public raft_message_builder_base<set_configuration_response_builder, raft::fbs::set_configuration_response>
    {
    private:
      client_result result_ = client_result_FAIL;
      ::flatbuffers::Offset<raft::fbs::simple_configuration_description> bad_servers_;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
	  bld->add_result(result_);
	  bld->add_bad_servers(bad_servers_);
      }	
      set_configuration_response_builder & result(client_result val)
      {
	result_ = val;
	return *this;
      }
      set_configuration_response_builder & bad_servers(::flatbuffers::Offset<raft::fbs::simple_configuration_description> val)
      {
	bad_servers_ = val;
	return *this;
      }
      simple_configuration_description_builder bad_servers()
      {
	return simple_configuration_description_builder(fbb(), [this](::flatbuffers::Offset<raft::fbs::simple_configuration_description> val) { this->bad_servers(val); });
      }
    };

    class log_entry_builder
    {
    private:
      std::unique_ptr<flatbuffers::FlatBufferBuilder> fbb_;
      uint64_t term_ = 0;
      uint64_t cluster_time_ = 0;
      raft::fbs::log_entry_type type_ = raft::fbs::log_entry_type_NOOP;
      ::flatbuffers::Offset<::flatbuffers::Vector<uint8_t>> configuration_;
      ::flatbuffers::Offset<::flatbuffers::String> data_;
      
      flatbuffers::FlatBufferBuilder & fbb()
      {
	if (!fbb_) {
	  fbb_ = std::make_unique<flatbuffers::FlatBufferBuilder>();
	}
	return *fbb_;
      }
    public:
      log_entry_builder & term(uint64_t val)
      {
	term_ = val;
	return *this;
      }
      
      log_entry_builder & cluster_time(uint64_t val)
      {
	cluster_time_ = val;
	return *this;
      }
      
      log_entry_builder & data(raft::slice && val)
      {
	type_ = raft::fbs::log_entry_type_COMMAND;
	data_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
				   raft::slice::buffer_size(val));
	return *this;
      }

      log_entry_builder & data(const char * val)
      {
	type_ = raft::fbs::log_entry_type_COMMAND;
	data_ = fbb().CreateString(val);
	return *this;
      }

      log_entry_builder & configuration(const uint8_t & e)
      {
	// Configuration descriptions are nested flatbuffers so we can just memcpy them into message
	// We use size prefixed buffers so the first uoffset_t is the size of the flatbuffer
	// (does not include the size of the size prefix itself)
	type_ = raft::fbs::log_entry_type_CONFIGURATION;
	configuration_ = fbb().CreateVector<uint8_t>(&e, ::flatbuffers::GetPrefixedSize(&e)+sizeof(::flatbuffers::uoffset_t));
	return *this;
      }
      log_entry_builder & configuration(const std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete > & val)
      {
	return configuration(*val.first);
      }
      configuration_description_builder configuration()
      {
	return configuration_description_builder([this](const std::pair<configuration_description_traits::const_arg_type, raft::util::call_on_delete > & val) { this->configuration(val); });
      }

      std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete> finish()
      {
	raft::fbs::log_entryBuilder leb(fbb());
	leb.add_term(term_);
	leb.add_cluster_time(cluster_time_);
	leb.add_type(type_);
	leb.add_configuration(configuration_);
	leb.add_data(data_);
	fbb().FinishSizePrefixed(leb.Finish());
      
	std::size_t size, offset;
	auto buf = fbb().ReleaseRaw(size, offset);
	delete fbb_.release();
	return std::pair<log_entry_traits::const_arg_type, raft::util::call_on_delete >(buf + offset,
											[buf]() { delete [] buf; });
      }
    };

    template<typename _Derived, typename _FlatType>
    class log_entry_command_builder_base
    {
    public:
      typedef _FlatType fbs_type;
      typedef typename _FlatType::Builder fbs_builder_type;
    private:
      std::unique_ptr<flatbuffers::FlatBufferBuilder> fbb_;
    public:
      flatbuffers::FlatBufferBuilder & fbb()
      {
	if (!fbb_) {
	  fbb_ = std::make_unique<flatbuffers::FlatBufferBuilder>();
	}
	return *fbb_;
      }
      std::pair<const raft::fbs::log_entry_command *, raft::util::call_on_delete> finish()
      {
	static_cast<_Derived *>(this)->preinitialize();
	fbs_builder_type bld(fbb());
	static_cast<_Derived *>(this)->initialize(&bld);
	auto rv = bld.Finish();
	auto m = Createlog_entry_command(fbb(), raft::fbs::any_log_entry_commandTraits<fbs_type>::enum_value, rv.Union());
	fbb().FinishSizePrefixed(m);
	auto obj = ::flatbuffers::GetSizePrefixedRoot<raft::fbs::log_entry_command>(fbb().GetBufferPointer());
	BOOST_ASSERT(fbb().GetBufferPointer()+sizeof(::flatbuffers::uoffset_t) == ::flatbuffers::GetBufferStartFromRootPointer(obj));
	auto ret = std::pair<const raft::fbs::log_entry_command *, raft::util::call_on_delete>(obj, [fbb = fbb_.release()]() { delete fbb; });
	return ret;
      }
    };

    class open_session_request_builder : public log_entry_command_builder_base<open_session_request_builder, raft::fbs::open_session_request>
    {
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
      }	
    };

    class open_session_response_builder : public log_entry_command_builder_base<open_session_response_builder, raft::fbs::open_session_response>
    {
    private:
      uint64_t session_id_ = 0;

    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
        bld->add_session_id(session_id_);
      }	

      open_session_response_builder & session_id(uint64_t val)
      {
	session_id_ = val;
	return *this;
      }
    };

    class close_session_request_builder : public log_entry_command_builder_base<close_session_request_builder, raft::fbs::close_session_request>
    {
    private:
      uint64_t session_id_ = 0;

    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
        bld->add_session_id(session_id_);
      }	

      close_session_request_builder & session_id(uint64_t val)
      {
	session_id_ = val;
	return *this;
      }
    };

    class close_session_response_builder : public log_entry_command_builder_base<close_session_response_builder, raft::fbs::close_session_response>
    {
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
      }	
    };

    class linearizable_command_builder : public log_entry_command_builder_base<linearizable_command_builder, raft::fbs::linearizable_command>
    {
    private:
      uint64_t session_id_ = 0;
      uint64_t first_unacknowledged_sequence_number_ = 0;
      uint64_t sequence_number_ = 0;
      ::flatbuffers::Offset<::flatbuffers::String> command_;
    public:
      void preinitialize()
      {
      }
      
      void initialize(fbs_builder_type * bld)
      {
        bld->add_session_id(session_id_);
        bld->add_first_unacknowledged_sequence_number(first_unacknowledged_sequence_number_);
        bld->add_sequence_number(sequence_number_);
	bld->add_command(command_);
      }	

      linearizable_command_builder & session_id(uint64_t val)
      {
	session_id_ = val;
	return *this;
      }

      linearizable_command_builder & first_unacknowledged_sequence_number(uint64_t val)
      {
	first_unacknowledged_sequence_number_ = val;
	return *this;
      }

      linearizable_command_builder & sequence_number(uint64_t val)
      {
	sequence_number_ = val;
	return *this;
      }

      linearizable_command_builder & command(raft::slice && val)
      {
	command_ = fbb().CreateString(raft::slice::buffer_cast<const char *>(val),
				      raft::slice::buffer_size(val));
	return *this;
      }
    };

    class builders
    {
    public:
      typedef request_vote_builder request_vote_builder_type; 
      typedef vote_response_builder vote_response_builder_type;
      typedef client_request_builder client_request_builder_type;
      typedef client_response_builder client_response_builder_type;
      typedef append_entry_builder append_entry_builder_type;
      typedef append_response_builder append_response_builder_type;
      typedef append_checkpoint_chunk_builder append_checkpoint_chunk_builder_type;
      typedef append_checkpoint_chunk_response_builder append_checkpoint_chunk_response_builder_type;
      typedef set_configuration_request_builder set_configuration_request_builder_type;
      typedef set_configuration_response_builder set_configuration_response_builder_type;
      typedef log_entry_builder log_entry_builder_type;
      typedef open_session_request_builder open_session_request_builder_type;
      typedef open_session_response_builder open_session_response_builder_type;
      typedef close_session_request_builder close_session_request_builder_type;
      typedef close_session_response_builder close_session_response_builder_type;
      typedef linearizable_command_builder linearizable_command_builder_type;
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
	std::vector<flatbuffers::Offset<raft::fbs::log_entries>> entries_vec;
	for(uint64_t i=0; i<num_entries; ++i) {
	  const auto * e = &entries(i);
	  // Log entries are nested flatbuffers so we can just memcpy them into message
	  // We use size prefixed buffers so the first uoffset_t is the size of the flatbuffer
	  // (does not include the size of the size prefix itself)
	  auto v = fbb->CreateVector<uint8_t>(e, ::flatbuffers::GetPrefixedSize(e)+sizeof(::flatbuffers::uoffset_t));
	  raft::fbs::log_entriesBuilder leb(*fbb);
	  leb.add_entry(v);
	  entries_vec.push_back(leb.Finish());
	}

	auto e = fbb->CreateVector(entries_vec);  
	
	raft::fbs::append_entryBuilder aeb(*fbb);
	aeb.add_recipient_id(recipient_id);
	aeb.add_term_number(term_number);
	aeb.add_leader_id(leader_id);
	aeb.add_previous_log_index(previous_log_index);
	aeb.add_previous_log_term(previous_log_term);
	aeb.add_leader_commit_index(leader_commit_index);
	aeb.add_entries(e);
	auto ae = aeb.Finish();
	// Create the surrounding raft_message
	auto m = raft::fbs::Createraft_message(*fbb, raft::fbs::any_message_append_entry, ae.Union());
	// Finish and get buffer
	fbb->Finish(m);
	// Send on to communicator
	comm_.send(ep_, address_, std::move(fbb));
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
	request_vote_builder bld;
	bld.recipient_id(recipient_id).term_number(term_number).candidate_id(candidate_id).last_log_index(last_log_index).last_log_term(last_log_term);
	// Send on to communicator
	comm_.send(ep_, address_, bld.finish());
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
	vote_response_builder bld;
	bld.peer_id(peer_id).term_number(term_number).request_term_number(request_term_number).granted(granted);
	comm_.send(ep_, address_, bld.finish());	
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

  }
}

#endif

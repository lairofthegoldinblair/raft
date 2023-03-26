#ifndef __RAFTMESSAGES_HH__
#define __RAFTMESSAGES_HH__

#include <chrono>
#include <map>
#include <vector>

#include "checkpoint.hh"
#include "configuration.hh"
#include "log.hh"
#include "slice.hh"

namespace raft {
  namespace native {

    enum client_result { SUCCESS, FAIL, RETRY, NOT_LEADER };
    class client_request
    {
    public:
      std::string command;
      slice get_command_data() const
      {
	return slice::create(command);
      }
    };

    class client_request_traits
    {
    public:
      typedef client_request value_type;
      typedef const value_type& const_arg_type;
    
      slice get_command_data(const_arg_type cr) const
      {
	return slice::create(cr.command);
      }
    };

    class client_response
    {
    public:
      client_result result;
      uint64_t index;
      std::size_t leader_id;
    };

    template<typename simple_configuration_description_type>
    class set_configuration_request
    {
    public:
      uint64_t old_id;
      simple_configuration_description_type new_configuration;
    };

    template<typename simple_configuration_description_type>
    class set_configuration_request_traits
    {
    public:
      typedef set_configuration_request<simple_configuration_description_type> value_type;
      typedef const value_type& const_arg_type;
    
      static uint64_t old_id(const_arg_type msg)
      {
	return msg.old_id;
      }
      static std::size_t new_configuration_size(const_arg_type msg)
      {
	return msg.new_configuration.servers.size();
      }
      static uint64_t new_configuration_id(const_arg_type msg, std::size_t i)
      {
	return msg.new_configuration.servers[i].id;
      }
      static const char * new_configuration_address(const_arg_type msg, std::size_t i)
      {
	return msg.new_configuration.servers[i].address.c_str();
      }
    };

    class set_configuration_response
    {
    public:
      client_result result;
      simple_configuration_description bad_servers;
    };

    class set_configuration_response_traits
    {
    public:
      typedef set_configuration_response value_type;
      typedef const value_type& const_arg_type;
    
      static client_result result(const_arg_type msg)
      {
	return msg.result;
      }
      static std::size_t bad_servers_size(const_arg_type msg)
      {
	return msg.bad_servers.servers.size();
      }
      static uint64_t bad_servers_id(const_arg_type msg, std::size_t i)
      {
	return msg.bad_servers.servers[i].id;
      }
      static const char * bad_servers_address(const_arg_type msg, std::size_t i)
      {
	return msg.bad_servers.servers[i].address.c_str();
      }
    };

    class request_vote
    {
    public:
      uint64_t recipient_id_;
      uint64_t term_number_;
      uint64_t candidate_id_;
      uint64_t last_log_index_;
      uint64_t last_log_term_;

      void set_recipient_id(uint64_t value)
      {
	recipient_id_ = value;
      }
      void set_term_number(uint64_t value)
      {
	term_number_ = value;
      }
      void set_candidate_id(uint64_t value)
      {
	candidate_id_ = value;
      }
      void set_last_log_index(uint64_t value)
      {
	last_log_index_ = value;
      }
      void set_last_log_term(uint64_t value)
      {
	last_log_term_ = value;
      }
    };

    struct request_vote_traits
    {
      typedef request_vote value_type;
      typedef const request_vote & const_arg_type;

      static uint64_t recipient_id(const_arg_type msg)
      {
	return msg.recipient_id_;
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return msg.term_number_;
      }
      static uint64_t candidate_id(const_arg_type msg)
      {
	return msg.candidate_id_;
      }
      static uint64_t last_log_index(const_arg_type msg)
      {
	return msg.last_log_index_;
      }
      static uint64_t last_log_term(const_arg_type msg)
      {
	return msg.last_log_term_;
      }
    };

    class vote_response
    {
    public:
      uint64_t peer_id;
      uint64_t term_number;
      uint64_t request_term_number;
      bool granted;
    };

    struct vote_response_traits
    {
      typedef vote_response value_type;
      typedef const vote_response & const_arg_type;

      static uint64_t peer_id(const_arg_type msg)
      {
	return msg.peer_id;
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return msg.term_number;
      }
      static uint64_t request_term_number(const_arg_type msg)
      {
	return msg.request_term_number;
      }
      static bool granted(const_arg_type msg)
      {
	return msg.granted;
      }
    };

    template<typename _LogEntry>
    class append_entry
    {
    public:
      typedef _LogEntry log_entry_type;
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t leader_id;
      // Basic point of Raft is the Log Matching Property which comprises:
      // 1) Index and Term of a log entry uniquely define the content
      // 2) If two logs have entries at the same index with the same term then all preceeding entries
      // also agree
      //
      uint64_t previous_log_index;
      // The last term sent (only valid if previous_log_index > 0).
      uint64_t previous_log_term;
      // Last log entry in message that is committed on leader
      uint64_t leader_commit_index;
      std::vector<log_entry_type> entry;

      // uint64_t recipient_id() const
      // {
      //   return recipient_id_;
      // }
      // uint64_t term_number() const
      // {
      //   return term_number_;
      // }
      // uint64_t leader_id() const
      // {
      //   return leader_id_;
      // }
      // // Basic point of Raft is the Log Matching Property which comprises:
      // // 1) Index and Term of a log entry uniquely define the content
      // // 2) If two logs have entries at the same index with the same term then all preceeding entries
      // // also agree
      // //
      // uint64_t previous_log_index() const
      // {
      //   return previous_log_index_;
      // }
      // // The last term sent (only valid if previous_log_index > 0).
      // uint64_t previous_log_term() const
      // {
      //   return previous_log_term_;
      // }
      // // Last log entry in message that is committed on leader
      // uint64_t leader_commit_index() const
      // {
      //   return leader_commit_index_;
      // }
      // std::vector<log_entry_type> entry;
    
      void set_recipient_id(uint64_t value)
      {
	recipient_id = value;
      }
      void set_term_number(uint64_t value)
      {
	term_number = value;
      }
      void set_leader_id(uint64_t value)
      {
	leader_id = value;
      }
      void set_previous_log_index(uint64_t value)
      {
	previous_log_index = value;
      }
      void set_previous_log_term(uint64_t value)
      {
	previous_log_term = value;
      }
      void set_leader_commit_index(uint64_t value)
      {
	leader_commit_index = value;
      }
      void add_entry(const log_entry_type & e)
      {
	entry.emplace_back(e);
      }
      void add_entry(log_entry_type && e)
      {
	entry.push_back(e);
      }
    };

    template<typename _LogEntry>
    struct append_entry_traits
    {
      typedef append_entry<_LogEntry> value_type;
      typedef const value_type * const_arg_type;
      typedef const_arg_type pinned_type;
      typedef typename std::vector<_LogEntry>::const_iterator iterator_type;
    
      static uint64_t recipient_id(const_arg_type ae)
      {
	return ae->recipient_id;
      }
      static uint64_t term_number(const_arg_type ae)
      {
	return ae->term_number;
      }
      static uint64_t leader_id(const_arg_type ae)
      {
	return ae->leader_id;
      }
      // Basic point of Raft is the Log Matching Property which comprises:
      // 1) Index and Term of a log entry uniquely define the content
      // 2) If two logs have entries at the same index with the same term then all preceeding entries
      // also agree
      //
      static uint64_t previous_log_index(const_arg_type ae)
      {
	return ae->previous_log_index;
      }
      // The last term sent (only valid if previous_log_index > 0).
      static uint64_t previous_log_term(const_arg_type ae)
      {
	return ae->previous_log_term;
      }
      // Last log entry in message that is committed on leader
      static uint64_t leader_commit_index(const_arg_type ae)
      {
	return ae->leader_commit_index;
      }
      static std::size_t num_entries(const_arg_type ae)
      {
	return ae->entry.size();
      }
      static uint64_t entry_term(const_arg_type ae, std::size_t i)
      {
	return ae->entry[i].term;
      }
      static bool entry_is_configuration(const_arg_type ae, std::size_t i)
      {
	return _LogEntry::CONFIGURATION == ae->entry[i].type;
      }
      static iterator_type begin_entries(const_arg_type ae)
      {
	return ae->entry.begin();
      }
      static iterator_type end_entries(const_arg_type ae)
      {
	return ae->entry.end();
      }
      static void release(const_arg_type ae)
      {
	delete ae;
      }
      static pinned_type pin(const_arg_type ae)
      {
	return ae;
      }
    };

    class append_response
    {
    public:
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t request_term_number;
      // Beginning of range of entries appended
      uint64_t begin_index;
      // One after the last log entry appended
      uint64_t last_index;
      bool success;
    };

    class append_entry_response_traits
    {
    public:
      typedef append_response value_type;
      typedef const value_type & const_arg_type;
    
    
      static uint64_t recipient_id(const_arg_type ae)
      {
	return ae.recipient_id;
      }
      static uint64_t term_number(const_arg_type ae)
      {
	return ae.term_number;
      }
      static uint64_t request_term_number(const_arg_type ae)
      {
	return ae.request_term_number;
      }
      // Beginning of range of entries appended
      static uint64_t begin_index(const_arg_type ae)
      {
	return ae.begin_index;
      }
      // One after the last log entry appended
      static uint64_t last_index(const_arg_type ae)
      {
	return ae.last_index;
      }
      static bool success(const_arg_type ae)
      {
	return ae.success;
      }
    };

    template<typename checkpoint_data_store_type>
    class append_checkpoint_chunk
    {
    public:
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t leader_id;
      // Only needed on a chunk if checkpoint_done==true; a client can know
      // whether the checkpoint is up to date without looking at the data itself
      // (which is is assumed to carry a checkpoint_header that also has the index).
      uint64_t last_checkpoint_index;
      // Ongaro's logcabin does not put the term in the message but assumes that it is
      // serialized as part of the data (the actual checkpoint file).
      // I'm not sure I like that model so I am putting it in the chunk message as well;
      // we'll see how that goes for me :-)  I am only look at this value in the first chunk
      // of a checkpoint.
      uint64_t last_checkpoint_term;
      // Ongaro's logcabin does not put the configuration in the message but assumes that it is
      // serialized as part of the data (the actual checkpoint file).
      // I'm not sure I like that model so I am putting it in the chunk message as well;
      // we'll see how that goes for me :-)  I am only looking at this value in the first chunk
      // of a checkpoint.
      typename checkpoint_data_store_type::configuration_type last_checkpoint_configuration;
      uint64_t checkpoint_begin;
      uint64_t checkpoint_end;
      bool checkpoint_done;
      std::vector<uint8_t> data;
    };

    template<typename checkpoint_data_store_type>
    class append_checkpoint_chunk_traits
    {
    public:
      typedef append_checkpoint_chunk<checkpoint_data_store_type> value_type;
      typedef const value_type & const_arg_type;
      typedef value_type pinned_type;    
    
      static uint64_t recipient_id(const_arg_type ae)
      {
	return ae.recipient_id;
      }
      static uint64_t term_number(const_arg_type ae)
      {
	return ae.term_number;
      }
      static uint64_t leader_id(const_arg_type ae)
      {
	return ae.leader_id;
      }
      static uint64_t last_checkpoint_index(const_arg_type ae)
      {
	return ae.last_checkpoint_index;
      }
      static uint64_t last_checkpoint_term(const_arg_type ae)
      {
	return ae.last_checkpoint_term;
      }
      // Ongaro's logcabin does not put the configuration in the message but assumes that it is
      // serialized as part of the data (the actual checkpoint file).
      // I'm not sure I like that model so I am putting it in the chunk message as well;
      // we'll see how that goes for me :-)  I am only looking at this value in the first chunk
      // of a checkpoint.
      static uint64_t checkpoint_configuration_index(const_arg_type ae)
      {
	return ae.last_checkpoint_configuration.index;
      }
      static uint64_t checkpoint_configuration_from_size(const_arg_type ae)
      {
	return ae.last_checkpoint_configuration.description.from.servers.size();
      }
      static uint64_t checkpoint_configuration_from_id(const_arg_type ae, std::size_t i)
      {
	return ae.last_checkpoint_configuration.description.from.servers[i].id;
      }
      static const char * checkpoint_configuration_from_address(const_arg_type ae, std::size_t i)
      {
	return ae.last_checkpoint_configuration.description.from.servers[i].address.c_str();
      }
      static uint64_t checkpoint_configuration_to_size(const_arg_type ae)
      {
	return ae.last_checkpoint_configuration.description.to.servers.size();
      }
      static uint64_t checkpoint_configuration_to_id(const_arg_type ae, std::size_t i)
      {
	return ae.last_checkpoint_configuration.description.to.servers[i].id;
      }
      static const char * checkpoint_configuration_to_address(const_arg_type ae, std::size_t i)
      {
	return ae.last_checkpoint_configuration.description.to.servers[i].address.c_str();
      }
      static uint64_t checkpoint_begin(const_arg_type ae)
      {
	return ae.checkpoint_begin;
      }
      static uint64_t checkpoint_end(const_arg_type ae)
      {
	return ae.checkpoint_end;
      }
      static bool checkpoint_done(const_arg_type ae)
      {
	return ae.checkpoint_done;
      }
      static slice data(const_arg_type ae)
      {
	return ae.data.size()>0 ? slice(&ae.data[0], ae.data.size()) : slice(nullptr, 0U);
      }
      static void release(const_arg_type ae)
      {
      }
      static pinned_type pin(const_arg_type ae)
      {
	return pinned_type(ae);
      }
    };

    class append_checkpoint_chunk_response
    {
    public:
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t request_term_number;
      uint64_t bytes_stored;
    };

    class append_checkpoint_chunk_response_traits
    {
    public:
      typedef append_checkpoint_chunk_response value_type;
      typedef const value_type & const_arg_type;
      static uint64_t recipient_id(const_arg_type ae)
      {
	return ae.recipient_id;
      }
      static uint64_t term_number(const_arg_type ae)
      {
	return ae.term_number;
      }
      static uint64_t request_term_number(const_arg_type ae)
      {
	return ae.request_term_number;
      }
      static uint64_t bytes_stored(const_arg_type ae)
      {
	return ae.bytes_stored;
      }
    };

    template <typename _Description>
    struct log_entry
    {
    public:
      typedef _Description configuration_description_type;
      enum entry_type { COMMAND, CONFIGURATION, NOOP };
      entry_type type;
      uint64_t term;
      // Populated if type==COMMAND
      std::string data;
      // Populated if type==CONFIGURATION
      configuration_description_type configuration;
    };
  
    template <typename _Description>
    struct log_entry_traits
    {
      typedef log_entry<_Description> value_type;
      typedef const value_type * const_arg_type;

      static uint64_t term(const_arg_type msg)
      {
	return msg->term;
      }
      static bool is_command(const_arg_type msg)
      {
	return value_type::COMMAND == msg->type;
      }
      static bool is_configuration(const_arg_type msg)
      {
	return value_type::CONFIGURATION == msg->type;
      }
      static bool is_noop(const_arg_type msg)
      {
	return value_type::NOOP == msg->type;
      }
      static const _Description & configuration(const_arg_type msg)
      {
	return msg->configuration;
      }
      static std::pair<const log_entry<_Description> *, std::function<void()> > create_command(uint64_t term, const client_request & req)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::COMMAND;
	ret->term = term;
	ret->data = req.command;
	return std::pair<const log_entry<_Description> *, std::function<void()>>(ret, [ret]() { delete ret; });
      }
      static std::pair<const log_entry<_Description> *, std::function<void()> > create_command(uint64_t term, client_request && req)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::COMMAND;
	ret->term = term;
	ret->data = std::move(req.command);
	return std::pair<const log_entry<_Description> *, std::function<void()>>(ret, [ret]() { delete ret; });
      }
      static std::pair<const log_entry<_Description> *, std::function<void()> > create_noop(uint64_t term)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::NOOP;
	ret->term = term;
	return std::pair<const log_entry<_Description> *, std::function<void()>>(ret, [ret]() { delete ret; });
      }
      template<typename Configuration>
      static std::pair<const log_entry<_Description> *, std::function<void()> > create_configuration(uint64_t term, const Configuration & config)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::CONFIGURATION;
	ret->term = term;
	for(auto i=0; i<config.from_size(); ++i) {
	  ret->configuration.from.servers.push_back({ config.from_id(i), config.from_address(i) });
	}
	for(auto i=0; i<config.to_size(); ++i) {
	  ret->configuration.to.servers.push_back({ config.to_id(i), config.to_address(i) });
	}
	return std::pair<const log_entry<_Description> *, std::function<void()>>(ret, [ret]() { delete ret; });
      }
      static std::pair<const log_entry<_Description> *, std::function<void()> > create_bootstrap_log_entry(uint64_t id, const char * address)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::CONFIGURATION;
	ret->term = 0;
	ret->configuration.from.servers.push_back({ id, std::string(address) });
	return std::pair<const log_entry<_Description> *, std::function<void()>>(ret, [ret]() { delete ret; });
      }
    };

    struct server_description_traits
    {
      typedef const server_description * const_arg_type;

      static uint64_t id(const_arg_type msg)
      {
	return msg->id;
      }
      static slice address(const_arg_type msg)
      {
	return slice::create(msg->address);
      }
    };

    struct simple_configuration_description_traits
    {
      typedef const simple_configuration_description * const_arg_type;
      typedef std::vector<server_description>::const_iterator iterator_type;

      static iterator_type begin_servers(const_arg_type msg)
      {
	return msg->servers.begin();
      }
      static iterator_type end_servers(const_arg_type msg)
      {
	return msg->servers.end();
      }
    };

    struct configuration_description_traits
    {
      typedef const configuration_description * const_arg_type;
      static const simple_configuration_description & from(const_arg_type msg)
      {
	return msg->from;
      }
      static const simple_configuration_description & to(const_arg_type msg)
      {
	return msg->to;
      }
    };

    class messages
    {
    public:
      typedef log_entry<configuration_description> log_entry_type;
      typedef log_entry_traits<configuration_description> log_entry_traits_type;
      typedef client_request client_request_type;
      typedef client_request_traits client_request_traits_type;
      typedef client_response client_response_type;
      typedef request_vote_traits request_vote_traits_type;
      typedef request_vote_traits::value_type request_vote_type;
      typedef vote_response_traits::value_type vote_response_type;
      typedef vote_response_traits vote_response_traits_type;
      typedef append_checkpoint_chunk_traits<checkpoint_data_store<configuration_description::checkpoint_type> > append_checkpoint_chunk_traits_type;
      typedef append_checkpoint_chunk_traits_type::const_arg_type append_checkpoint_chunk_arg_type;
      typedef append_checkpoint_chunk_traits_type::value_type append_checkpoint_chunk_type;
      typedef append_checkpoint_chunk_response_traits append_checkpoint_chunk_response_traits_type;
      typedef append_checkpoint_chunk_response append_checkpoint_chunk_response_type;
      typedef append_entry<log_entry_type> append_entry_type;
      typedef append_entry_traits<log_entry_type> append_entry_traits_type;
      typedef append_entry_response_traits append_entry_response_traits_type;
      typedef append_entry_response_traits::value_type append_entry_response_type;
      typedef set_configuration_request<configuration_description::simple_type> set_configuration_request_type;
      typedef set_configuration_request_traits<configuration_description::simple_type> set_configuration_request_traits_type;
      typedef set_configuration_response set_configuration_response_type;
      typedef set_configuration_response_traits set_configuration_response_traits_type;
      typedef configuration_description configuration_description_type;
      typedef configuration_description_type::server_type configuration_description_server_type;
      typedef configuration_description_type::simple_type simple_configuration_description_type;
      typedef server_description_traits server_description_traits_type;
      typedef simple_configuration_description_traits simple_configuration_description_traits_type;
      typedef configuration_description_traits configuration_description_traits_type;

      typedef client_result client_result_type;
      static client_result client_result_success() { return SUCCESS; }
      static client_result client_result_fail() { return FAIL; }
      static client_result client_result_retry() { return RETRY; }
      static client_result client_result_not_leader() { return NOT_LEADER; }
    };

    // TODO: Develop some usable code for a state_machine
    // This consumes the Raft log (and applies commands in the log) and also
    // consumes checkpoint data (though most likely not directly from the protocol
    // rather though a disk file that the protocol writes).
    class state_machine
    {
    };

    // A test client
    // TODO: What is the model for how replicated entries get propagated to a client?
    // For example, when we are a FOLLOWER, entries get committed via append_entries and
    // then should be applied to client.  Presumably we should support both a push model and
    // a pull model.  LogCabin uses a pull model.  Note that we should make a distinction between
    // a client and a state_machine.
    template<typename _Messages>
    class client
    {
    public:
      typedef typename _Messages::simple_configuration_description_type simple_configuration_description_type;
      typedef typename _Messages::client_result_type client_result_type;
      std::deque<client_response> responses;
      std::deque<set_configuration_response> configuration_responses;
      // void on_client_response(const client_response & resp)
      // {
      // 	responses.push_front(resp);
      // }

      void on_client_response(client_result_type result,
			      uint64_t index,
			      std::size_t leader_id)
      {
	client_response resp;
	resp.result = result;
	resp.index = index;
	resp.leader_id = leader_id;      
	responses.push_front(resp);
      }

      static client_result convert(client_result_type result)
      {
	if (_Messages::client_result_success() == result) {
	  return client_result::SUCCESS;
	} else if (_Messages::client_result_fail() == result) {
	  return client_result::FAIL;
	} else if (_Messages::client_result_not_leader() == result) {
	  return client_result::NOT_LEADER;
	} else {
	  return client_result::RETRY;
	}
      }
      void on_configuration_response(client_result_type result)
      {
	set_configuration_response resp;
	resp.result = convert(result);
	configuration_responses.push_front(resp);
      }
    
      void on_configuration_response(client_result_type result, const simple_configuration_description_type & bad_servers)
      {
	set_configuration_response resp;
	resp.result = result;
	resp.bad_servers = bad_servers;
	configuration_responses.push_front(resp);
      }
    
      client()
      {
      }
    };
  }
}
#endif


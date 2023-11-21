#ifndef __RAFTMESSAGES_HH__
#define __RAFTMESSAGES_HH__

#include <chrono>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include <deque>
#include "util/call_on_delete.hh"
#include "slice.hh"

namespace raft {
  namespace native {

    struct server_description
    {
      typedef std::string address_type;
      uint64_t id;
      std::string address;
    };

    struct simple_configuration_description
    {
      typedef server_description server_type;
      std::vector<server_description> servers;
    };

    struct configuration_description
    {
      typedef simple_configuration_description simple_type;
      typedef server_description server_type;
      typedef server_description::address_type address_type;
      simple_type from;
      simple_type to;
    };

    // Data about a configuration that has to be stored in a checkpoint
    struct configuration_checkpoint
    {
      uint64_t index;
      configuration_description description;
    };

    struct configuration_checkpoint_traits
    {
      typedef configuration_checkpoint value_type;
      typedef const value_type * const_arg_type;
    
      static uint64_t index(const_arg_type cr)
      {
	return cr->index;
      }
      static const configuration_description & configuration(const_arg_type cr)
      {
	return cr->description;
      }
      static bool is_valid(const_arg_type cr) 
      {
	return cr->index != std::numeric_limits<uint64_t>::max();
      }
    };

    class checkpoint_header
    {
    public:
      uint64_t last_log_entry_index;
      uint64_t last_log_entry_term;
      uint64_t last_log_entry_cluster_time;
      configuration_checkpoint configuration;
    };

    struct checkpoint_header_traits
    {
      typedef checkpoint_header value_type;
      typedef const value_type * const_arg_type;
    
      static uint64_t index(const_arg_type cr)
      {
	return cr->configuration.index;
      }
      static const configuration_description & configuration(const_arg_type cr)
      {
	return cr->configuration.description;
      }
      static uint64_t last_log_entry_index(const_arg_type cr)
      {
	return cr->last_log_entry_index;
      }
      static uint64_t last_log_entry_term(const_arg_type cr)
      {
	return cr->last_log_entry_term;
      }
      static uint64_t last_log_entry_cluster_time(const_arg_type cr)
      {
	return cr->last_log_entry_cluster_time;
      }
      static std::pair<const_arg_type, raft::util::call_on_delete> build(uint64_t last_log_entry_index,
                                                                         uint64_t last_log_entry_term,
                                                                         uint64_t last_log_entry_cluster_time,
                                                                         uint64_t configuration_index,
                                                                         const configuration_description * configuration_description)
      {
	auto tmp = new checkpoint_header();
	tmp->last_log_entry_index = last_log_entry_index;
	tmp->last_log_entry_term = last_log_entry_term;
	tmp->last_log_entry_cluster_time = last_log_entry_cluster_time;
	tmp->configuration.index = configuration_index;
	tmp->configuration.description = *configuration_description;
	return std::pair<const_arg_type, raft::util::call_on_delete>(tmp, [tmp]() { delete tmp; });
      }
    };

    enum client_result { SUCCESS, FAIL, RETRY, NOT_LEADER, SESSION_EXPIRED };

    class client_response
    {
    public:
      client_result result;
      uint64_t index;
      uint64_t leader_id;
      std::string response;
    };

    class client_response_traits
    {
    public:
      typedef client_response value_type;
      typedef client_response arg_type;
      typedef const value_type& const_arg_type;

      static client_result result(const_arg_type cr)
      {
        return cr.result;
      }
      static uint64_t index(const_arg_type cr)
      {
        return cr.index;
      }
      static uint64_t leader_id(const_arg_type cr)
      {
        return cr.leader_id;
      }
      static slice response(const_arg_type cr)
      {
	return slice::create(cr.response);
      }
    };
    
    class set_configuration_request
    {
    public:
      uint64_t old_id;
      simple_configuration_description new_configuration;
    };

    class set_configuration_request_traits
    {
    public:
      typedef set_configuration_request value_type;
      typedef set_configuration_request arg_type;
      typedef const value_type& const_arg_type;
    
      static uint64_t old_id(const_arg_type msg)
      {
	return msg.old_id;
      }
      static const simple_configuration_description & new_configuration(const_arg_type msg)
      {
	return msg.new_configuration;
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
      typedef set_configuration_request arg_type;
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
      static std::string_view bad_servers_address(const_arg_type msg, std::size_t i)
      {
	return std::string_view(msg.bad_servers.servers[i].address);
      }
    };

    class vote_request
    {
    public:
      uint64_t request_id;
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t candidate_id;
      uint64_t last_log_index;
      uint64_t last_log_term;

      void set_request_id(uint64_t value)
      {
	request_id = value;
      }
      void set_recipient_id(uint64_t value)
      {
	recipient_id = value;
      }
      void set_term_number(uint64_t value)
      {
	term_number = value;
      }
      void set_candidate_id(uint64_t value)
      {
	candidate_id = value;
      }
      void set_last_log_index(uint64_t value)
      {
	last_log_index = value;
      }
      void set_last_log_term(uint64_t value)
      {
	last_log_term = value;
      }
    };

    struct vote_request_traits
    {
      typedef vote_request value_type;
      typedef vote_request arg_type;
      typedef const arg_type & const_arg_type;

      static uint64_t request_id(const_arg_type msg)
      {
	return msg.request_id;
      }
      static uint64_t recipient_id(const_arg_type msg)
      {
	return msg.recipient_id;
      }
      static uint64_t term_number(const_arg_type msg)
      {
	return msg.term_number;
      }
      static uint64_t candidate_id(const_arg_type msg)
      {
	return msg.candidate_id;
      }
      static uint64_t last_log_index(const_arg_type msg)
      {
	return msg.last_log_index;
      }
      static uint64_t last_log_term(const_arg_type msg)
      {
	return msg.last_log_term;
      }
    };

    class vote_response
    {
    public:
      uint64_t peer_id;
      uint64_t term_number;
      uint64_t request_term_number;
      uint64_t request_id;
      bool granted;
    };

    struct vote_response_traits
    {
      typedef vote_response value_type;
      typedef vote_response arg_type;
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
      static uint64_t request_id(const_arg_type msg)
      {
	return msg.request_id;
      }
      static bool granted(const_arg_type msg)
      {
	return msg.granted;
      }
    };

    template<typename _LogEntry>
    class append_entry_request
    {
    public:
      typedef _LogEntry log_entry_type;
      uint64_t request_id;
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
    struct append_entry_request_traits
    {
      // typedef const append_entry_request<_LogEntry> * const_arg_type;
      typedef append_entry_request<_LogEntry> arg_type;
      typedef const append_entry_request<_LogEntry> & const_arg_type;
    
      static uint64_t request_id(const_arg_type ae)
      {
	return ae.request_id;
      }
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
      // Basic point of Raft is the Log Matching Property which comprises:
      // 1) Index and Term of a log entry uniquely define the content
      // 2) If two logs have entries at the same index with the same term then all preceeding entries
      // also agree
      //
      static uint64_t previous_log_index(const_arg_type ae)
      {
	return ae.previous_log_index;
      }
      // The last term sent (only valid if previous_log_index > 0).
      static uint64_t previous_log_term(const_arg_type ae)
      {
	return ae.previous_log_term;
      }
      // Last log entry in message that is committed on leader
      static uint64_t leader_commit_index(const_arg_type ae)
      {
	return ae.leader_commit_index;
      }
      static std::size_t num_entries(const_arg_type ae)
      {
	return ae.entry.size();
      }
      static const _LogEntry & get_entry(const_arg_type ae, std::size_t i)
      {
	return ae.entry[i];
      }
      static uint64_t entry_term(const_arg_type ae, std::size_t i)
      {
	return ae.entry[i].term;
      }
      static bool entry_is_configuration(const_arg_type ae, std::size_t i)
      {
	return _LogEntry::CONFIGURATION == ae.entry[i].type;
      }
    };

    class append_entry_response
    {
    public:
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t request_term_number;
      uint64_t request_id;
      // Beginning of range of entries appended
      uint64_t begin_index;
      // One after the last log entry appended
      uint64_t last_index;
      bool success;
    };

    class append_entry_response_traits
    {
    public:
      typedef append_entry_response value_type;
      typedef append_entry_response arg_type;
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
      static uint64_t request_id(const_arg_type ae)
      {
	return ae.request_id;
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

    class append_checkpoint_chunk_request
    {
    public:
      uint64_t request_id;
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t leader_id;
      // Ongaro's logcabin does not put the configuration in the message but assumes that it is
      // serialized as part of the data (the actual checkpoint file).
      // I'm not sure I like that model so I am putting it in the chunk message as well;
      // we'll see how that goes for me :-)  I am only looking at this value in the first chunk
      // of a checkpoint.
      checkpoint_header last_checkpoint_header;
      uint64_t checkpoint_begin;
      uint64_t checkpoint_end;
      bool checkpoint_done;
      std::vector<uint8_t> data;
    };

    class append_checkpoint_chunk_request_traits
    {
    public:
      typedef append_checkpoint_chunk_request arg_type;
      typedef const append_checkpoint_chunk_request & const_arg_type;
    
      static uint64_t request_id(const_arg_type ae)
      {
	return ae.request_id;
      }
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
	return ae.last_checkpoint_header.last_log_entry_index;
      }
      static uint64_t last_checkpoint_term(const_arg_type ae)
      {
	return ae.last_checkpoint_header.last_log_entry_term;
      }
      static uint64_t last_checkpoint_cluster_time(const_arg_type ae)
      {
	return ae.last_checkpoint_header.last_log_entry_cluster_time;
      }
      // Ongaro's logcabin does not put the configuration in the message but assumes that it is
      // serialized as part of the data (the actual checkpoint file).
      // I'm not sure I like that model so I am putting it in the chunk message as well;
      // we'll see how that goes for me :-)  I am only looking at this value in the first chunk
      // of a checkpoint.
      static const checkpoint_header & last_checkpoint_header(const_arg_type ae)
      {
	return ae.last_checkpoint_header;
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
    };

    class append_checkpoint_chunk_response
    {
    public:
      uint64_t recipient_id;
      uint64_t term_number;
      uint64_t request_term_number;
      uint64_t request_id;
      uint64_t bytes_stored;
    };

    class append_checkpoint_chunk_response_traits
    {
    public:
      typedef append_checkpoint_chunk_response value_type;
      typedef append_checkpoint_chunk_response arg_type;
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
      static uint64_t request_id(const_arg_type ae)
      {
	return ae.request_id;
      }
      static uint64_t bytes_stored(const_arg_type ae)
      {
	return ae.bytes_stored;
      }
    };

    // These classes are necessary for transforming the at-least-once semantics of
    // the raw Raft protocol into linearizable semantics
    class open_session_request
    {
    public:
    };

    class open_session_request_traits
    {
    public:
      typedef open_session_request value_type;
      typedef open_session_request arg_type;
      typedef const value_type & const_arg_type;
      typedef const value_type & const_view_type;
    };
    
    class open_session_response
    {
    public:
      uint64_t session_id;
    };

    class open_session_response_traits
    {
    public:
      typedef open_session_response value_type;
      typedef open_session_response arg_type;
      typedef const value_type & const_arg_type;
      static uint64_t session_id(const_arg_type ae)
      {
	return ae.session_id;
      }
    };
    
    class close_session_request
    {
    public:
      uint64_t session_id;
    };

    class close_session_request_traits
    {
    public:
      typedef close_session_request value_type;
      typedef close_session_request arg_type;
      typedef const value_type & const_arg_type;
      typedef const value_type & const_view_type;
      static uint64_t session_id(const_arg_type ae)
      {
	return ae.session_id;
      }
    };
    
    class close_session_response
    {
    public:
    };

    class close_session_response_traits
    {
    public:
      typedef close_session_response value_type;
      typedef close_session_response arg_type;
      typedef const value_type & const_arg_type;
    };
    
    class linearizable_command_request
    {
    public:
      uint64_t session_id;
      uint64_t first_unacknowledged_sequence_number;
      uint64_t sequence_number;
      std::string command;
    };

    class linearizable_command_request_traits
    {
    public:
      typedef linearizable_command_request value_type;
      typedef linearizable_command_request arg_type;
      typedef const value_type & const_arg_type;
      typedef const value_type & const_view_type;
      static uint64_t session_id(const_arg_type ae)
      {
	return ae.session_id;
      }
      static uint64_t first_unacknowledged_sequence_number(const_arg_type ae)
      {
	return ae.first_unacknowledged_sequence_number;
      }
      static uint64_t sequence_number(const_arg_type ae)
      {
	return ae.sequence_number;
      }
      static slice command(const_arg_type ae)
      {
	return slice::create(ae.command);
      }
    };

    
    struct log_entry_command
    {
      enum command_type { OPEN_SESSION, CLOSE_SESSION, LINEARIZABLE_COMMAND };
      command_type type;
      open_session_request open_session;
      close_session_request close_session;
      linearizable_command_request command;
    };

    struct log_entry_command_traits
    {
      typedef log_entry_command value_type;
      typedef const value_type * const_arg_type;

      static bool is_open_session(const_arg_type msg)
      {
        return log_entry_command::OPEN_SESSION == msg->type;
      }
      static bool is_close_session(const_arg_type msg)
      {
        return log_entry_command::CLOSE_SESSION == msg->type;
      }
      static bool is_linearizable_command(const_arg_type msg)
      {
        return log_entry_command::LINEARIZABLE_COMMAND == msg->type;
      }

      static open_session_request_traits::const_view_type open_session(const_arg_type msg)
      {
        return msg->open_session;
      }
      static close_session_request_traits::const_view_type close_session(const_arg_type msg)
      {
        return msg->close_session;
      }
      static linearizable_command_request_traits::const_view_type linearizable_command(const_arg_type msg)
      {
        return msg->command;
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
      uint64_t cluster_time;
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
      static uint64_t cluster_time(const_arg_type msg)
      {
	return msg->cluster_time;
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
      static slice data(const_arg_type msg)
      {
	return slice::create(msg->data);
      }
      static const _Description & configuration(const_arg_type msg)
      {
	return msg->configuration;
      }
      static std::pair<const log_entry<_Description> *, raft::util::call_on_delete > create_command(uint64_t term,
                                                                                                    uint64_t cluster_time,
                                                                                                    std::pair<raft::slice, raft::util::call_on_delete> && req)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::COMMAND;
	ret->term = term;
        ret->cluster_time = cluster_time;
	ret->data.assign(reinterpret_cast<const char *>(req.first.data()), req.first.size());
	return std::pair<const log_entry<_Description> *, raft::util::call_on_delete>(ret, [ret]() { delete ret; });
      }
      static std::pair<const log_entry<_Description> *, raft::util::call_on_delete > create_noop(uint64_t term, uint64_t cluster_time)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::NOOP;
	ret->term = term;
        ret->cluster_time = cluster_time;
	return std::pair<const log_entry<_Description> *, raft::util::call_on_delete>(ret, [ret]() { delete ret; });
      }
      template<typename Configuration>
      static std::pair<const log_entry<_Description> *, raft::util::call_on_delete > create_configuration(uint64_t term, uint64_t cluster_time, const Configuration & config)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::CONFIGURATION;
	ret->term = term;
        ret->cluster_time = cluster_time;
	for(auto i=0; i<config.from_size(); ++i) {
	  ret->configuration.from.servers.push_back({ config.from_id(i), config.from_address(i) });
	}
	for(auto i=0; i<config.to_size(); ++i) {
	  ret->configuration.to.servers.push_back({ config.to_id(i), config.to_address(i) });
	}
	return std::pair<const log_entry<_Description> *, raft::util::call_on_delete>(ret, [ret]() { delete ret; });
      }
      static std::pair<const log_entry<_Description> *, raft::util::call_on_delete > create_bootstrap_log_entry(uint64_t id, const char * address)
      {
	auto ret = new log_entry<_Description>();
	ret->type = value_type::CONFIGURATION;
	ret->term = 0;
        ret->cluster_time = 0;
	ret->configuration.from.servers.push_back({ id, std::string(address) });
	return std::pair<const log_entry<_Description> *, raft::util::call_on_delete>(ret, [ret]() { delete ret; });
      }
    };

    struct server_description_traits
    {
      typedef const server_description * const_arg_type;

      static uint64_t id(const_arg_type msg)
      {
	return msg->id;
      }
      static std::string_view address(const_arg_type msg)
      {
	return msg->address;
      }
    };

    struct simple_configuration_description_traits
    {
      typedef const simple_configuration_description * const_arg_type;
      static std::size_t size(const_arg_type msg)
      {
	return msg->servers.size();
      }
      static const server_description & get(const_arg_type msg, std::size_t i)
      {
	return msg->servers[i];
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
      typedef log_entry_command log_entry_command_type;
      typedef log_entry_command_traits log_entry_command_traits_type;
      typedef client_response client_response_type;
      typedef client_response_traits client_response_traits_type;
      typedef vote_request_traits vote_request_traits_type;
      typedef vote_request_traits::value_type vote_request_type;
      typedef vote_response_traits::value_type vote_response_type;
      typedef vote_response_traits vote_response_traits_type;
      typedef append_checkpoint_chunk_request_traits append_checkpoint_chunk_request_traits_type;
      typedef append_checkpoint_chunk_request_traits_type::const_arg_type append_checkpoint_chunk_request_arg_type;
      typedef append_checkpoint_chunk_request append_checkpoint_chunk_request_type;
      typedef append_checkpoint_chunk_response_traits append_checkpoint_chunk_response_traits_type;
      typedef append_checkpoint_chunk_response append_checkpoint_chunk_response_type;
      typedef append_entry_request<log_entry_type> append_entry_request_type;
      typedef append_entry_request_traits<log_entry_type> append_entry_request_traits_type;
      typedef append_entry_response_traits append_entry_response_traits_type;
      typedef append_entry_response_traits::value_type append_entry_response_type;
      typedef set_configuration_request set_configuration_request_type;
      typedef set_configuration_request_traits set_configuration_request_traits_type;
      typedef set_configuration_response set_configuration_response_type;
      typedef set_configuration_response_traits set_configuration_response_traits_type;
      typedef configuration_description configuration_description_type;
      typedef configuration_description_type::server_type configuration_description_server_type;
      typedef configuration_description_type::simple_type simple_configuration_description_type;
      typedef server_description_traits server_description_traits_type;
      typedef simple_configuration_description_traits simple_configuration_description_traits_type;
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
      typedef linearizable_command_request linearizable_command_request_type;
      typedef linearizable_command_request_traits linearizable_command_request_traits_type;

      typedef client_result client_result_type;
      static client_result client_result_success() { return SUCCESS; }
      static client_result client_result_fail() { return FAIL; }
      static client_result client_result_retry() { return RETRY; }
      static client_result client_result_not_leader() { return NOT_LEADER; }
      static client_result client_result_session_expired() { return SESSION_EXPIRED; }
    };

    template<typename _Ty>
    class builder_base
    {
    protected:
      std::unique_ptr<_Ty> obj_;
      _Ty * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<_Ty>();
	}
	return obj_.get();
      }
    public:
      builder_base() = default;

      _Ty finish()
      {
	auto obj = std::move(*get_object());
	obj_.reset();
	return obj;
      }
    };

    class server_description_builder : public builder_base<server_description>
    {
    private:
      std::unique_ptr<std::function<void(server_description && )>> finisher_;
      
    public:
      server_description_builder() = default;

      template<typename _Function>
      server_description_builder(_Function && finisher)
	:
	finisher_(new std::function<void(server_description && )>(std::move(finisher)))
      {
      }
      
      ~server_description_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }
      
      server_description_builder & id(uint64_t val)
      {
	get_object()->id = val;
	return *this;
      }
      server_description_builder & address(const char * val)
      {
	get_object()->address = val;
	return *this;
      }
    };

    class simple_configuration_description_builder : public builder_base<simple_configuration_description>
    {
    private:
      std::unique_ptr<std::function<void(simple_configuration_description && )>> finisher_;
      
    public:
      simple_configuration_description_builder() = default;

      template<typename _Function>
      simple_configuration_description_builder(_Function && finisher)
	:
	finisher_(new std::function<void(simple_configuration_description && )>(std::move(finisher)))
      {
      }
      
      ~simple_configuration_description_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }
      
      simple_configuration_description_builder & server(server_description && val)
      {
	get_object()->servers.push_back(std::move(val));
	return *this;
      }

      server_description_builder server()
      {
	return server_description_builder([this](server_description && val) { this->server(std::move(val)); });
      }
    };

    class configuration_description_builder : public builder_base<configuration_description>
    {
    private:
      std::unique_ptr<std::function<void(configuration_description && )>> finisher_;
      
    public:
      configuration_description_builder() = default;

      template<typename _Function>
      configuration_description_builder(_Function && finisher)
	:
	finisher_(new std::function<void(configuration_description && )>(std::move(finisher)))	
      {
      }      

      ~configuration_description_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }
      
      configuration_description_builder & from(simple_configuration_description && val)
      {
	get_object()->from = std::move(val);
	return *this;
      }
      simple_configuration_description_builder from()
      {
	return simple_configuration_description_builder([this](simple_configuration_description && val) { this->from(std::move(val)); });
      }
      configuration_description_builder & to(simple_configuration_description && val)
      {
	get_object()->to = std::move(val);
	return *this;
      }
      simple_configuration_description_builder to()
      {
	return simple_configuration_description_builder([this](simple_configuration_description && val) { this->to(std::move(val)); });
      }
    };

    class checkpoint_header_builder : public builder_base<checkpoint_header>
    {
    private:
      std::unique_ptr<std::function<void(checkpoint_header && )>> finisher_;
      
    public:
      checkpoint_header_builder() = default;

      template<typename _Function>
      checkpoint_header_builder(_Function && finisher)
	:
	finisher_(new std::function<void(checkpoint_header && )>(std::move(finisher)))	
      {
      }
      
      ~checkpoint_header_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }
      
      checkpoint_header_builder & last_log_entry_index(uint64_t val)
      {
	get_object()->last_log_entry_index = val;
	return *this;
      }
      checkpoint_header_builder & last_log_entry_term(uint64_t val)
      {
	get_object()->last_log_entry_term = val;
	return *this;
      }
      checkpoint_header_builder & last_log_entry_cluster_time(uint64_t val)
      {
	get_object()->last_log_entry_cluster_time = val;
	return *this;
      }
      checkpoint_header_builder & index(uint64_t val)
      {
	get_object()->configuration.index = val;
	return *this;
      }
      checkpoint_header_builder & configuration(const configuration_description & val)
      {
	get_object()->configuration.description = val;
	return *this;
      }
      configuration_description_builder configuration()
      {
	return configuration_description_builder([this](configuration_description && val) { this->configuration(val); });
      }
    };

    class vote_request_builder
    {
    private:
      std::unique_ptr<vote_request> obj_;
      vote_request * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<vote_request>();
	  obj_->request_id = 0;
	  obj_->recipient_id = 0;
	  obj_->term_number = 0;
	  obj_->candidate_id = 0;
	  obj_->last_log_index = 0;
	  obj_->last_log_term = 0;
	}
	return obj_.get();
      }
    public:
      vote_request_builder & request_id(uint64_t val)
      {
	get_object()->request_id = val;
	return *this;
      }
      vote_request_builder & recipient_id(uint64_t val)
      {
	get_object()->recipient_id = val;
	return *this;
      }
      vote_request_builder & term_number(uint64_t val)
      {
	get_object()->term_number = val;
	return *this;
      }
      vote_request_builder & candidate_id(uint64_t val)
      {
	get_object()->candidate_id = val;
	return *this;
      }
      vote_request_builder & last_log_index(uint64_t val)
      {
	get_object()->last_log_index = val;
	return *this;
      }
      vote_request_builder & last_log_term(uint64_t val)
      {
	get_object()->last_log_term = val;
	return *this;
      }
      vote_request finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class vote_response_builder
    {
    private:
      std::unique_ptr<vote_response> obj_;
      vote_response * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<vote_response>();
	  obj_->peer_id = 0;
	  obj_->term_number = 0;
	  obj_->request_term_number = 0;
	  obj_->request_id = 0;
	  obj_->granted = false;
	}
	return obj_.get();
      }
    public:
      vote_response_builder & peer_id(uint64_t val)
      {
	get_object()->peer_id = val;
	return *this;
      }
      vote_response_builder & term_number(uint64_t val)
      {
	get_object()->term_number = val;
	return *this;
      }
      vote_response_builder & request_term_number(uint64_t val)
      {
	get_object()->request_term_number = val;
	return *this;
      }
      vote_response_builder & request_id(uint64_t val)
      {
	get_object()->request_id = val;
	return *this;
      }
      vote_response_builder & granted(bool val)
      {
	get_object()->granted = val;
	return *this;
      }
      vote_response finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class client_response_builder : public builder_base<client_response>
    {
    public:
      client_response_builder & result(client_result val)
      {
	get_object()->result = val;
	return *this;
      }
      client_response_builder & index(uint64_t val)
      {
	get_object()->index = val;
	return *this;
      }
      client_response_builder & leader_id(uint64_t val)
      {
	get_object()->leader_id = val;
	return *this;
      }
      client_response_builder & response(raft::slice && val)
      {
	get_object()->response.assign(raft::slice::buffer_cast<const char *>(val),
				     raft::slice::buffer_size(val));
	return *this;
      }
    };

    template<typename _LogEntry>
    class append_entry_request_builder
    {
    private:
      std::unique_ptr<append_entry_request<_LogEntry>> obj_;
      append_entry_request<_LogEntry> * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<append_entry_request<_LogEntry> >();
	  obj_->request_id = 0;
	  obj_->recipient_id = 0;
	  obj_->term_number = 0;
	  obj_->leader_id = 0;
	  obj_->previous_log_index = 0;
	  obj_->previous_log_term = 0;
	  obj_->leader_commit_index = 0;
	}
	return obj_.get();
      }
    public:
      append_entry_request_builder & request_id(uint64_t val)
      {
	get_object()->request_id = val;
	return *this;
      }
      append_entry_request_builder & recipient_id(uint64_t val)
      {
	get_object()->recipient_id = val;
	return *this;
      }
      append_entry_request_builder & term_number(uint64_t val)
      {
	get_object()->term_number = val;
	return *this;
      }
      append_entry_request_builder & leader_id(uint64_t val)
      {
	get_object()->leader_id = val;
	return *this;
      }
      append_entry_request_builder & previous_log_index(uint64_t val)
      {
	get_object()->previous_log_index = val;
	return *this;
      }
      append_entry_request_builder & previous_log_term(uint64_t val)
      {
	get_object()->previous_log_term = val;
	return *this;
      }
      append_entry_request_builder & leader_commit_index(uint64_t val)
      {
	get_object()->leader_commit_index = val;
	return *this;
      }
      append_entry_request_builder & entry(const _LogEntry & e)
      {
	get_object()->entry.push_back(e);
	return *this;
      }
      append_entry_request_builder & entry(const std::pair<const _LogEntry *, raft::util::call_on_delete > & val)
      {
	return entry(*val.first);
      }
      append_entry_request<_LogEntry> finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class append_entry_response_builder
    {
    private:
      std::unique_ptr<append_entry_response> obj_;
      append_entry_response * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<append_entry_response>();
	  obj_->recipient_id = 0;
	  obj_->term_number = 0;
	  obj_->request_term_number = 0;
	  obj_->request_id  = 0;
	  obj_->begin_index = 0;
	  obj_->last_index = 0;
	  obj_->success = false;
	}
	return obj_.get();
      }
    public:
      append_entry_response_builder & recipient_id(uint64_t val)
      {
	get_object()->recipient_id = val;
	return *this;
      }
      append_entry_response_builder & term_number(uint64_t val)
      {
	get_object()->term_number = val;
	return *this;
      }
      append_entry_response_builder & request_term_number(uint64_t val)
      {
	get_object()->request_term_number = val;
	return *this;
      }
      append_entry_response_builder & request_id(uint64_t val)
      {
	get_object()->request_id = val;
	return *this;
      }
      append_entry_response_builder & begin_index(uint64_t val)
      {
	get_object()->begin_index = val;
	return *this;
      }
      append_entry_response_builder & last_index(uint64_t val)
      {
	get_object()->last_index = val;
	return *this;
      }
      append_entry_response_builder & success(bool val)
      {
	get_object()->success = val;
	return *this;
      }
      append_entry_response finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class append_checkpoint_chunk_request_builder : public builder_base<append_checkpoint_chunk_request>
    {
    public:
      append_checkpoint_chunk_request_builder & request_id(uint64_t val)
      {
	get_object()->request_id = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & recipient_id(uint64_t val)
      {
	get_object()->recipient_id = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & term_number(uint64_t val)
      {
	get_object()->term_number = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & leader_id(uint64_t val)
      {
	get_object()->leader_id = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & checkpoint_begin(uint64_t val)
      {
	get_object()->checkpoint_begin = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & checkpoint_end(uint64_t val)
      {
	get_object()->checkpoint_end = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & checkpoint_done(bool val)
      {
	get_object()->checkpoint_done = val;
	return *this;
      }
      append_checkpoint_chunk_request_builder & data(raft::slice && val)
      {
	get_object()->data.assign(raft::slice::buffer_cast<const uint8_t *>(val),
				  raft::slice::buffer_cast<const uint8_t *>(val) + raft::slice::buffer_size(val));
	return *this;
      }
      append_checkpoint_chunk_request_builder & last_checkpoint_header(checkpoint_header && val)
      {
	get_object()->last_checkpoint_header = std::move(val);
	return *this;
      }
      checkpoint_header_builder last_checkpoint_header()
      {
	return checkpoint_header_builder([this](checkpoint_header && val) { this->last_checkpoint_header(std::move(val)); });
      }
    };

    class append_checkpoint_chunk_response_builder
    {
    private:
      std::unique_ptr<append_checkpoint_chunk_response> obj_;
      append_checkpoint_chunk_response * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<append_checkpoint_chunk_response>();
	  obj_->recipient_id = 0;
	  obj_->term_number = 0;
	  obj_->request_term_number = 0;
	  obj_->request_id = 0;
	  obj_->bytes_stored = 0;
	}
	return obj_.get();
      }
    public:
      append_checkpoint_chunk_response_builder & recipient_id(uint64_t val)
      {
	get_object()->recipient_id = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & term_number(uint64_t val)
      {
	get_object()->term_number = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & request_term_number(uint64_t val)
      {
	get_object()->request_term_number = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & request_id(uint64_t val)
      {
	get_object()->request_id = val;
	return *this;
      }
      append_checkpoint_chunk_response_builder & bytes_stored(uint64_t val)
      {
	get_object()->bytes_stored = val;
	return *this;
      }
      append_checkpoint_chunk_response finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class set_configuration_request_builder
    {
    private:
      std::unique_ptr<set_configuration_request> obj_;
      set_configuration_request * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<set_configuration_request>();
	  obj_->old_id = 0;
	}
	return obj_.get();
      }
    public:
      set_configuration_request_builder & old_id(uint64_t val)
      {
	get_object()->old_id = val;
	return *this;
      }
      set_configuration_request_builder & new_configuration(simple_configuration_description && val)
      {
	get_object()->new_configuration = std::move(val);
	return *this;
      }
      simple_configuration_description_builder new_configuration()
      {
	return simple_configuration_description_builder([this](simple_configuration_description && val) { this->new_configuration(std::move(val)); });
      }
      set_configuration_request finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class set_configuration_response_builder
    {
    private:
      std::unique_ptr<set_configuration_response> obj_;
      set_configuration_response * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<set_configuration_response>();
	  obj_->result = FAIL;
	}
	return obj_.get();
      }
    public:
      set_configuration_response_builder & result(client_result val)
      {
	get_object()->result = val;
	return *this;
      }
      set_configuration_response_builder & bad_servers(simple_configuration_description && val)
      {
	get_object()->bad_servers = std::move(val);
	return *this;
      }
      simple_configuration_description_builder bad_servers()
      {
	return simple_configuration_description_builder([this](simple_configuration_description && val) { this->bad_servers(std::move(val)); });
      }
      set_configuration_response finish()
      {
	auto obj = *get_object();
	obj_.reset();
	return obj;
      }
    };

    class log_entry_builder
    {
    private:
      std::unique_ptr<log_entry<configuration_description>> obj_;
      log_entry<configuration_description> * get_object()
      {
	if (nullptr == obj_) {
	  obj_ = std::make_unique<log_entry<configuration_description>>();
	  obj_->term = 0;
	  obj_->cluster_time = 0;
	  obj_->type = log_entry<configuration_description>::NOOP;
	}
	return obj_.get();
      }
    public:
      log_entry_builder & term(uint64_t val)
      {
	get_object()->term = val;
	return *this;
      }

      log_entry_builder & cluster_time(uint64_t val)
      {
	get_object()->cluster_time = val;
	return *this;
      }

      log_entry_builder & data(raft::slice && val)
      {
	get_object()->type = log_entry<configuration_description>::COMMAND;
	get_object()->data.assign(raft::slice::buffer_cast<const char *>(val),
				  raft::slice::buffer_size(val));
	return *this;
      }

      log_entry_builder & data(const char * val)
      {
	get_object()->type = log_entry<configuration_description>::COMMAND;
	get_object()->data.assign(val);
	return *this;
      }

      log_entry_builder & configuration(configuration_description && val)
      {
	get_object()->type = log_entry<configuration_description>::CONFIGURATION;
	get_object()->configuration = std::move(val);
	return *this;
      }
      configuration_description_builder configuration()
      {
	return configuration_description_builder([this](configuration_description && val) { this->configuration(std::move(val)); });
      }
      std::pair<const log_entry<configuration_description> *, raft::util::call_on_delete > finish()
      {
	auto ptr = get_object();
	return std::pair<const log_entry<configuration_description> *, raft::util::call_on_delete >(ptr, [obj = std::move(obj_)](){});
      }
    };

    class open_session_request_builder : public builder_base<open_session_request>
    {
    private:
      std::unique_ptr<std::function<void(open_session_request && )>> finisher_;
      
    public:
      open_session_request_builder() = default;

      template<typename _Function>
      open_session_request_builder(_Function && finisher)
	:
	finisher_(new std::function<void(open_session_request && )>(std::move(finisher)))
      {
      }
      
      ~open_session_request_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }
    };

    class open_session_response_builder : public builder_base<open_session_response>
    {
    private:
      std::unique_ptr<std::function<void(open_session_response && )>> finisher_;
      
    public:
      open_session_response_builder() = default;

      template<typename _Function>
      open_session_response_builder(_Function && finisher)
	:
	finisher_(new std::function<void(open_session_response && )>(std::move(finisher)))
      {
      }
      
      ~open_session_response_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }

      open_session_response_builder & session_id(uint64_t val)
      {
	get_object()->session_id = val;
	return *this;
      }
    };

    class close_session_request_builder : public builder_base<close_session_request>
    {
    private:
      std::unique_ptr<std::function<void(close_session_request && )>> finisher_;
      
    public:
      close_session_request_builder() = default;

      template<typename _Function>
      close_session_request_builder(_Function && finisher)
	:
	finisher_(new std::function<void(close_session_request && )>(std::move(finisher)))
      {
      }
      
      ~close_session_request_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }

      close_session_request_builder & session_id(uint64_t val)
      {
	get_object()->session_id = val;
	return *this;
      }
    };

    class close_session_response_builder : public builder_base<close_session_response>
    {
    private:
      std::unique_ptr<std::function<void(close_session_response && )>> finisher_;
      
    public:
      close_session_response_builder() = default;

      template<typename _Function>
      close_session_response_builder(_Function && finisher)
	:
	finisher_(new std::function<void(close_session_response && )>(std::move(finisher)))
      {
      }
      
      ~close_session_response_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }
    };

    class linearizable_command_request_builder : public builder_base<linearizable_command_request>
    {
    private:
      std::unique_ptr<std::function<void(linearizable_command_request && )>> finisher_;
      
    public:
      linearizable_command_request_builder() = default;

      template<typename _Function>
      linearizable_command_request_builder(_Function && finisher)
	:
	finisher_(new std::function<void(linearizable_command_request && )>(std::move(finisher)))
      {
      }
      
      ~linearizable_command_request_builder()
      {
	if(finisher_) {
	  (*finisher_)(finish());
	}
      }

      linearizable_command_request_builder & session_id(uint64_t val)
      {
	get_object()->session_id = val;
	return *this;
      }
      linearizable_command_request_builder & first_unacknowledged_sequence_number(uint64_t val)
      {
	get_object()->first_unacknowledged_sequence_number = val;
	return *this;
      }
      linearizable_command_request_builder & sequence_number(uint64_t val)
      {
	get_object()->sequence_number = val;
	return *this;
      }
      linearizable_command_request_builder & command(raft::slice && val)
      {
	get_object()->command.assign(raft::slice::buffer_cast<const char *>(val),
				     raft::slice::buffer_size(val));
	return *this;
      }
    };

    class builders
    {
    public:
      typedef vote_request_builder vote_request_builder_type;
      typedef vote_response_builder vote_response_builder_type;
      typedef client_response_builder client_response_builder_type;
      typedef append_entry_request_builder<messages::log_entry_type> append_entry_request_builder_type;
      typedef append_entry_response_builder append_entry_response_builder_type;
      typedef append_checkpoint_chunk_request_builder append_checkpoint_chunk_request_builder_type;
      typedef append_checkpoint_chunk_response_builder append_checkpoint_chunk_response_builder_type;
      typedef set_configuration_request_builder set_configuration_request_builder_type;
      typedef set_configuration_response_builder set_configuration_response_builder_type;
      typedef log_entry_builder log_entry_builder_type;
      typedef open_session_request_builder open_session_request_builder_type;
      typedef open_session_response_builder open_session_response_builder_type;
      typedef close_session_request_builder close_session_request_builder_type;
      typedef close_session_response_builder close_session_response_builder_type;
      typedef linearizable_command_request_builder linearizable_command_request_builder_type;
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

      static client_result convert(client_result_type result)
      {
	if (_Messages::client_result_success() == result) {
	  return client_result::SUCCESS;
	} else if (_Messages::client_result_fail() == result) {
	  return client_result::FAIL;
	} else if (_Messages::client_result_not_leader() == result) {
	  return client_result::NOT_LEADER;
	} else if (_Messages::client_result_session_expired() == result) {
	  return client_result::SESSION_EXPIRED;
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
    
      void on_configuration_response(client_result_type result, const std::vector<std::pair<uint64_t, std::string>> & bad_servers)
      {
	set_configuration_response resp;
	resp.result = convert(result);
	for(const auto & bs : bad_servers) {
	  resp.bad_servers.servers.push_back({bs.first, bs.second});
	}
	configuration_responses.push_front(resp);
      }
    
      client()
      {
      }
    };
  }
}
#endif


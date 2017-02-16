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
  class set_configuration_response
  {
  public:
    client_result result;
    simple_configuration_description_type bad_servers;
  };

  class request_vote
  {
  public:
    uint64_t recipient_id_;
    uint64_t term_number_;
    uint64_t candidate_id_;
    uint64_t last_log_index_;
    uint64_t last_log_term_;

    uint64_t recipient_id() const
    {
      return recipient_id_;
    }
    uint64_t term_number() const
    {
      return term_number_;
    }
    uint64_t candidate_id() const
    {
      return candidate_id_;
    }
    uint64_t last_log_index() const
    {
      return last_log_index_;
    }
    uint64_t last_log_term() const
    {
      return last_log_term_;
    }

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

  // Possible abstractions: A communicator built from : types, serialization, transport
  // vs.
  // A communicator built from : pre-serialized types (e.g. flatbuffers), transport
  //
  // This assumes that we are using type serialization
  template<typename _Communicator>
  class vote_request_sender
  {
  private:
    _Communicator & comm_;
    typename _Communicator::endpoint ep_;
    std::string address_;
    request_vote msg_;
	
  public:
    vote_request_sender(_Communicator & comm, typename _Communicator::endpoint ep, const std::string & addr)
      :
      comm_(comm),
      ep_(ep),
      address_(addr)
    {
    }

    void add_recipient_id(uint64_t i)
    {
      msg_.set_recipient_id(i);
    }

    void add_term_number(uint64_t current_term)
    {
      msg_.set_term_number(current_term);
    }

    void add_candidate_id(uint64_t candidate)
    {
      msg_.set_candidate_id(candidate);
    }

    void add_last_log_index(uint64_t idx)
    {
      msg_.set_last_log_index(idx);
    }

    void add_last_log_term(uint64_t term)
    {
      msg_.set_last_log_term(term);
    }

    void send()
    {
      comm_.send(ep_, address_, msg_);
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
    typedef const value_type & const_arg_type;
    typedef value_type pinned_type;
    typedef typename std::vector<_LogEntry>::const_iterator iterator_type;
    
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
    static iterator_type begin_entries(const_arg_type ae)
    {
      return ae.entry.begin();
    }
    static iterator_type end_entries(const_arg_type ae)
    {
      return ae.entry.end();
    }
    static void release(const_arg_type ae)
    {
    }
    static pinned_type pin(const_arg_type ae)
    {
      return pinned_type(ae);
    }
  };

  // Possible abstractions: A communicator built from : types, serialization, transport
  // vs.
  // A communicator built from : pre-serialized types (e.g. flatbuffers), transport
  //
  // This assumes that we are using type serialization
  //
  // Another nasty issue is that message builders may want to consume data in different
  // orders (e.g flatbuffers is very strict about building data structures bottom up).  I'm
  // trying to avoid baking in such an assumption therefore I am passing all of the data in a
  // somewhat functional manner to avoid control flow issues.  Presumably there are high brow ways of handling
  // this such as expression templates.
  template<typename _Communicator, typename _LogEntry>
  class append_entry_sender
  {
  private:
    _Communicator & comm_;
    typename _Communicator::endpoint ep_;
    std::string address_;
    append_entry<_LogEntry> msg_;
	
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
      msg_.set_recipient_id(recipient_id);
      msg_.set_term_number(term_number);
      msg_.set_leader_id(leader_id);
      msg_.set_previous_log_index(previous_log_index);
      msg_.set_previous_log_term(previous_log_term);
      msg_.set_leader_commit_index(leader_commit_index);
      for(uint64_t i=0; i<num_entries; ++i) {
	msg_.add_entry(entries(i));
      }
      comm_.send(ep_, address_, msg_);
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

  class append_checkpoint_chunk_response
  {
  public:
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t request_term_number;
    uint64_t bytes_stored;
  };

  
  class messages
  {
  public:
    typedef client_request client_request_type;
    typedef client_response client_response_type;
    typedef request_vote request_vote_type;
    typedef vote_response vote_response_type;
    typedef append_checkpoint_chunk<checkpoint_data_store<configuration_description::checkpoint_type> > append_checkpoint_chunk_type;
    typedef append_checkpoint_chunk_response append_checkpoint_chunk_response_type;
    typedef append_entry<log_entry<configuration_description>> append_entry_type;
    typedef append_entry_traits<log_entry<configuration_description>> append_entry_traits_type;
    typedef append_response append_entry_response_type;
    typedef set_configuration_request<configuration_description::simple_type> set_configuration_request_type;
    typedef set_configuration_response<configuration_description::simple_type> set_configuration_response_type;

    typedef configuration_description configuration_description_type;
    typedef configuration_description_type::server_type configuration_description_server_type;
    typedef configuration_description_type::simple_type simple_configuration_description_type;
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
  template<typename simple_configuration_description_type>
  class client
  {
  public:
    typedef set_configuration_response<simple_configuration_description_type> configuration_response;
    std::deque<client_response> responses;
    std::deque<configuration_response> configuration_responses;
    void on_client_response(const client_response & resp)
    {
      responses.push_front(resp);
    }

    void on_configuration_response(const configuration_response & resp)
    {
      configuration_responses.push_front(resp);
    }
    
    client()
    {
    }
  };
}
#endif

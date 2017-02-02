#ifndef __RAFTSERVER_HH__
#define __RAFTSERVER_HH__

#include <chrono>
#include <map>
#include <vector>

#include "boost/logic/tribool.hpp"

#include "checkpoint.hh"
#include "configuration.hh"
#include "log.hh"

namespace raft {

  enum client_result { SUCCESS, FAIL, RETRY, NOT_LEADER };
  class client_request
  {
  public:
    std::string command;
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
    uint64_t recipient_id;
    uint64_t term_number;
    uint64_t candidate_id;
    uint64_t last_log_index;
    uint64_t last_log_term;
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
    // Part 1) of the property arises by having a leader be the only one that proposes entries and guaranteeing
    // that a leader never modifies a log entry with a given (index,term) once created.  Part 2) is guaranteed
    // by ensuring that a peer never appends to its log from a leader unless the content of the last entry is correct; by
    // Part 1) the content of that last entry may be specified by sending the (index,term) from the leader on every append request.
    // One after the last log entry sent.  If no log entries sent yet then 0.
    uint64_t previous_log_index;
    // The last term sent (only valid if previous_log_index > 0).
    uint64_t previous_log_term;
    // Last log entry in message that is committed on leader
    uint64_t leader_commit_index;
    std::vector<log_entry_type> entry;
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

  // TODO: Why do we assume that the checkpoint term is in the checkpoint_header in the
  // data and not in the chunk message?
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
    // we'll see how that goes for me :-)  I am only look at this value in the first chunk
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
    typedef append_response append_entry_response_type;
    typedef set_configuration_request<configuration_description::simple_type> set_configuration_request_type;
    typedef set_configuration_response<configuration_description::simple_type> set_configuration_response_type;

    typedef configuration_description configuration_description_type;
    typedef configuration_description_type::server_type configuration_description_server_type;
    typedef configuration_description_type::simple_type simple_configuration_description_type;
  };

  // Raft only really cares about the fact that a checkpoint maintains the index and term
  // at which the checkpoint is taken.  What client data and how that client data is communicated
  // to peers and disk is more or less irrelevant.  I want to structure things so that these latter
  // aspects are decoupled.
  template<typename checkpoint_data_store_type>
  class peer_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
    typedef typename checkpoint_data_store_type::block_type block_type;
    typedef typename checkpoint_data_store_type::configuration_type configuration_type;
    typedef append_checkpoint_chunk<checkpoint_data_store_type> append_checkpoint_chunk_type;
    
    // One past last byte to written in checkpoint file.
    uint64_t checkpoint_next_byte_;
    // The last log entry that the checkpoint covers.  Need to resume
    // sending log entries to peer from this point.
    uint64_t checkpoint_last_log_entry_index_;
    // The term of the last log entry that the checkpoint covers.  
    uint64_t checkpoint_last_log_entry_term_;
    // Configuration in effect at the time the checkpoint occured
    configuration_type checkpoint_last_configuration_;
    // Checkpoint data we are sending to this peer
    checkpoint_data_ptr data_;
    // Last block sent.  We do not assume that the checkpoint bytes are contiguous in memory
    // so cannot use checkpoint_next_byte_ to know where the next chunk is in data_.
    block_type last_block_sent_;
    // Has the last block been acked?  TODO: Generalize to a window/credit system?
    bool awaiting_ack_;

    peer_checkpoint(uint64_t checkpoint_last_log_entry_index, uint64_t checkpoint_last_log_entry_term,
		    const configuration_type & config, checkpoint_data_ptr data)
      :
      checkpoint_next_byte_(0),
      checkpoint_last_log_entry_index_(checkpoint_last_log_entry_index),
      checkpoint_last_log_entry_term_(checkpoint_last_log_entry_term),
      checkpoint_last_configuration_(config),
      data_(data),
      awaiting_ack_(false)
    {
    }

    bool prepare_checkpoint_chunk(append_checkpoint_chunk_type & msg)
    {
      if (awaiting_ack_) {
	return false;
      }

      if (data_->is_final(last_block_sent_)) {
	return false;
      }

      last_block_sent_ = data_->block_at_offset(checkpoint_next_byte_);

      msg.last_checkpoint_index = checkpoint_last_log_entry_index_;
      msg.last_checkpoint_term = checkpoint_last_log_entry_term_;
      msg.last_checkpoint_configuration = checkpoint_last_configuration_;
      msg.checkpoint_begin = data_->block_begin(last_block_sent_);
      msg.checkpoint_end = data_->block_end(last_block_sent_);
      msg.checkpoint_done = data_->is_final(last_block_sent_);
      msg.data.assign(last_block_sent_.block_data_,
		      last_block_sent_.block_data_+last_block_sent_.block_length_);
      awaiting_ack_ = true;
      return true;
    }
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  template<typename checkpoint_data_store_type, typename configuration_change_type>
  class peer
  {
  public:
    typedef typename checkpoint_data_store_type::configuration_type::address_type address_type;

  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
    // TODO: Templatize; do we actually want multiple addresses?  Does protocol really
    // need to know whether this is one or more addresses?
    address_type address;
    // Leader specific state about peers
    // Index of next log entry to send
    uint64_t next_index_;
    // One past the index of last log entry known to be replicated to peer
    uint64_t match_index_;
    // Vote we got from this peer.  boost::logic::indeterminate means we haven't heard back yet.
    boost::logic::tribool vote_;
    // Used only when LEADER; when does this peer need another heartbeat?
    std::chrono::time_point<std::chrono::steady_clock> requires_heartbeat_;
    // Is the value of next_index_ a guess or has it been confirmed by communication with the peer
    bool is_next_index_reliable_;
    // TODO: Implement the exit protocol
    bool exiting_;
    // Checkpoint state for sending a checkpoint from a server to this peer
    std::shared_ptr<peer_checkpoint<checkpoint_data_store_type> > checkpoint_;
    // State for tracking whether a peer that is newly added to a configuration tracking
    // log appends
    std::shared_ptr<configuration_change_type> configuration_change_;

    void exit()
    {
      exiting_ = true;
    }
  };

  class append_entry_continuation
  {
  public:
    // Leader that sent the append entry request we are responding to.
    uint64_t leader_id;
    // The log index we need flushed in order to respond request
    uint64_t begin_index;
    uint64_t end_index;
    // The term that the client request was part of
    uint64_t term;
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

  // Peer side of a checkpoint transfer
  template<typename checkpoint_data_store_type>
  class in_progress_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::header_type header_type;
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
  public:
    uint64_t end_;
    checkpoint_data_ptr file_;
    
    uint64_t end() const {
      return end_;
    }

    void write(const std::vector<uint8_t> & data)
    {
      // TODO: Support async here
      file_->write(&data[0], data.size());
      end_ += data.size();
    }

    in_progress_checkpoint(checkpoint_data_store_type & store,
			   const header_type & header)
      :
      end_(0),
      file_(store.create(header))
    {
    }
  };

  // Per-server state related to checkpoints
  template <typename checkpoint_data_store_type>
  class server_checkpoint
  {
  public:
    typedef typename checkpoint_data_store_type::checkpoint_data_ptr checkpoint_data_ptr;
  public:
    // One after last log entry checkpointed.
    uint64_t last_checkpoint_index_;
    // Term of last checkpoint
    uint64_t last_checkpoint_term_;
    // continuations depending on checkpoint sync events
    std::vector<std::pair<std::size_t, append_checkpoint_chunk_response> > checkpoint_chunk_response_sync_continuations_;
    // Object to manage receiving a new checkpoint from a leader and writing it to a reliable
    // location
    std::shared_ptr<in_progress_checkpoint<checkpoint_data_store_type> > current_checkpoint_;
    // Checkpoints live here
    checkpoint_data_store_type & store_;

    uint64_t last_checkpoint_index() const {
      return last_checkpoint_index_;
    }
    
    uint64_t last_checkpoint_term() const {
      return last_checkpoint_term_;
    }
    
    void sync(std::size_t leader_id, const append_checkpoint_chunk_response & resp) {
      checkpoint_chunk_response_sync_continuations_.push_back(std::make_pair(leader_id,resp));
    }

    void abandon() {
      if (!!current_checkpoint_) {
	store_.discard(current_checkpoint_->file_);
	current_checkpoint_.reset();
      }
    }

    checkpoint_data_ptr last_checkpoint() {
      return store_.last_checkpoint();
    }

    server_checkpoint(checkpoint_data_store_type & store)
      :
      last_checkpoint_index_(0),
      last_checkpoint_term_(0),
      store_(store)
    {
    }
  };

  template<typename peer_type, typename configuration_description_type>
  class test_configuration
  {
  public:
    typedef typename std::vector<peer_type>::iterator peer_iterator;
  private:
    // The cluster
    std::vector<peer_type> cluster_;
    // My cluster id/index
    std::size_t cluster_idx_;
    // ????
    configuration_description_type default_;
  public:
    test_configuration(std::size_t cluster_idx, const std::vector<peer_type>& peers)
      :
      cluster_(peers),
      cluster_idx_(cluster_idx)
    {
    }

    peer_type & self() {
      return cluster_[cluster_idx_];
    }

    std::size_t num_known_peers() const
    {
      return cluster_.size();
    }

    peer_iterator begin_peers()
    {
      return cluster_.begin();
    }

    peer_iterator end_peers()
    {
      return cluster_.end();
    }

    std::size_t my_cluster_id() const
    {
      return cluster_idx_;
    }

    peer_type & peer_from_id(uint64_t peer_id) {
      return cluster_[peer_id];
    }

    const peer_type & get_peer_from_id(uint64_t peer_id) const {
      return cluster_.at(peer_id);
    }

    bool has_quorum() const {
      // Majority quorum logic
      std::size_t num_votes(0);
      for(auto & p : cluster_) {
	if(p.peer_id == cluster_idx_ || p.vote_) {
	  num_votes += 1;
	}
      }
      return num_votes > (cluster_.size()/2);
    }

    uint64_t get_committed(uint64_t last_synced_index) const {
      // Figure out the minimum over a quorum.
      std::vector<uint64_t> acked;
      // For a leader, syncing to a log is "acking"
      acked.push_back(last_synced_index);
      // For peers we need an append response to get an ack
      for(std::size_t i=0; i<num_known_peers(); ++i) {
	if (i != my_cluster_id()) {
	  acked.push_back(get_peer_from_id(i).match_index_);
	}
      }
      std::sort(acked.begin(), acked.end());
      return acked[(acked.size()-1)/2];
    }

    void reset_staging_servers()
    {
      // Noop
    }

    uint64_t configuration_id() const {
      return 0;
    }

    bool includes_self() const {
      return true;
    }

    bool is_transitional() const {
      return false;
    }

    const configuration_description_type & description() const {
      return default_;
    }
  };

  class test_configuration_change
  {
  public:
    // We start out with an unattainable goal but this will get fixed up in the next interval.
    test_configuration_change(std::chrono::time_point<std::chrono::steady_clock> clock_now)
    {
    }
    
    void on_append_response(std::chrono::time_point<std::chrono::steady_clock> clock_now,
			    uint64_t match_index,
			    uint64_t last_log_index)
    {
    }

    bool is_caught_up() const
    {
      return true;
    }
  };

  template<typename _Peer, typename configuration_description_type>
  class test_configuration_manager
  {
  public:
    typedef typename _Peer::template apply<peer_configuration_change>::type peer_type;
    typedef test_configuration<peer_type, configuration_description_type> configuration_type;
    typedef configuration_description description_type;
    typedef typename configuration_description_type::checkpoint_type checkpoint_type;
  private:
    configuration_type configuration_;
    checkpoint_type default_;
  public:
    test_configuration_manager(std::size_t cluster_idx, const std::vector<peer_type>& peers)
      :
      configuration_(cluster_idx, peers)
    {
    }
    
    const configuration_type & configuration() const
    {
      return configuration_;
    }

    configuration_type & configuration()
    {
      return configuration_;
    }

    bool has_configuration_at(uint64_t log_index) const
    {
      return false;
    }

    void add_logged_description(uint64_t log_index, const description_type & description)
    {
      // Not supported
    }

    void set_checkpoint(const checkpoint_type & description)
    {
      // TODO: Validate that config is same
    }
    
    const checkpoint_type &  get_checkpoint()
    {
      return default_;
    }
    
    void get_checkpoint_state(uint64_t log_index, checkpoint_type & ck) const
    {
    }

    void truncate_prefix(uint64_t )
    {
    }

    void truncate_suffix(uint64_t )
    {
    }    
  };
}
#endif

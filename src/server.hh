#ifndef __RAFTSERVER_HH__
#define __RAFTSERVER_HH__

#include <chrono>
#include <map>
#include <vector>

#include "boost/logic/tribool.hpp"

// For test_communicator: I think I'll templatize and eventually move this code out of here
#include <deque>
#include "boost/variant.hpp"

#include "configuration.hh"
#include "log.hh"

namespace raft {

  enum client_result { SUCCESS, FAIL, RETRY, NOT_LEADER };
  class client_request
  {
  public:
    uint64_t id;
    std::string command;
  };

  class client_response
  {
  public:
    uint64_t id;
    client_result result;
    uint64_t index;
    std::size_t leader_id;
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

  class append_entry
  {
  public:
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
    std::vector<log_entry> entry;
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
    // serialized as part of the data (the actually checkpoint file).
    // I'm not sure I like that model so I am putting it in the chunk message as well;
    // we'll see how that goes for me :-)
    uint64_t last_checkpoint_term;
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

  class test_communicator
  {
  public:
    typedef size_t endpoint;
    template<typename _T>
    void send(endpoint ep, const _T & msg)
    {
      q.push_front(msg);
    }

    typedef boost::variant<request_vote, vote_response, append_entry, append_response,
			   append_checkpoint_chunk, append_checkpoint_chunk_response> any_msg_type;
    std::deque<any_msg_type> q;
  };

  class checkpoint_header
  {
  public:
    uint64_t last_log_entry_index;
    uint64_t last_log_entry_term;
    // TODO: Configuration stuff needs to go here
  };

  class checkpoint_block
  {
  public:
    const uint8_t * block_data_;
    std::size_t block_length_;

    checkpoint_block()
      :
      block_data_(nullptr),
      block_length_(0)
    {
    }

    checkpoint_block(const uint8_t * block_data, std::size_t block_length)
      :
      block_data_(block_data),
      block_length_(block_length)
    {
    }

    bool is_null() const {
      return block_data_ == nullptr;
    }
  };

  // TODO: What abstractions are needed for representation of checkpoints.
  // For example, for a real system this is likely to be on disk (at least somewhere "reliable")
  // but is it a dedicated file, is it just a bunch of blocks scattered throughout a file or something else entirely?
  // Right now I'm representing a checkpoint as a list of blocks with an implementation as an
  // array of data (could be a linked list of stuff as well).
  // TODO: This block stuff is half baked because it isn't consistent with the ack'ing protocol that is expressed
  // in terms of byte offsets; it works but it's goofy.
  class checkpoint_data
  {
  private:
    checkpoint_header header_;
    std::vector<uint8_t> data_;
    // TODO: Configure block size
    std::size_t block_size_;
  public:
    checkpoint_data(const checkpoint_header & header)
      :
      header_(header),
      block_size_(2)
    {
    }

    const checkpoint_header & header() const
    {
      return header_;
    }

    checkpoint_block block_at_offset(uint64_t offset) const {
      if (offset >= data_.size()) {
	return checkpoint_block();	
      }
      
      std::size_t next_block_start = offset;
      std::size_t next_block_end = (std::min)(next_block_start+block_size_, data_.size());
      std::size_t next_block_size = next_block_end - next_block_start;
      return checkpoint_block(&data_[next_block_start], next_block_size);
    }
    
    checkpoint_block next_block(const checkpoint_block & current_block) {
      if (current_block.is_null()) {
	return checkpoint_block(&data_[0], block_size_);
      } else if (!is_final(current_block)) {
	std::size_t next_block_start = (current_block.block_data_ - &data_[0]) + current_block.block_length_;
	std::size_t next_block_end = (std::min)(next_block_start+block_size_, data_.size());
	std::size_t next_block_size = next_block_end - next_block_start;
	return checkpoint_block(&data_[next_block_start], next_block_size);
      } else {
	return checkpoint_block();
      }
    }

    uint64_t block_begin(const checkpoint_block & current_block) const {
      return current_block.block_data_ - &data_[0];
    }

    uint64_t block_end(const checkpoint_block & current_block) const {
      return current_block.block_length_ + block_begin(current_block);
    }

    bool is_final(const checkpoint_block & current_block) {
      return !current_block.is_null() &&
	(current_block.block_data_ + current_block.block_length_) == &data_[data_.size()];
    }


    void write(const uint8_t * data, std::size_t len)
    {
      for(std::size_t i=0; i<len; ++i) {
	data_.push_back(data[i]);
      }
    }
  };


  // Checkpoints live here
  class checkpoint_data_store
  {
  public:
    typedef std::shared_ptr<checkpoint_data> checkpoint_data_ptr;
  private:
    std::shared_ptr<checkpoint_data> last_checkpoint_;
  public:
    std::shared_ptr<checkpoint_data> create(uint64_t last_entry_index, uint64_t last_entry_term) const
    {
      checkpoint_header header;
      header.last_log_entry_index = last_entry_index;
      header.last_log_entry_term = last_entry_term;
      return std::shared_ptr<checkpoint_data>(new checkpoint_data(header));
    }
    void commit(std::shared_ptr<checkpoint_data> f)
    {
      last_checkpoint_ = f;
    }
    std::shared_ptr<checkpoint_data> last_checkpoint() {
      return last_checkpoint_;
    }
  };

  // Raft only really cares about the fact that a checkpoint maintains the index and term
  // at which the checkpoint is taken.  What client data and how that client data is communicated
  // to peers and disk is more or less irrelevant.  I want to structure things so that these latter
  // aspects are decoupled.
  class peer_checkpoint
  {
  public:
    typedef checkpoint_data_store::checkpoint_data_ptr checkpoint_data_ptr;
    
    // One past last byte to written in checkpoint file.
    uint64_t checkpoint_next_byte_;
    // The last log entry that the checkpoint covers.  Need to resume
    // sending log entries to peer from this point.
    uint64_t checkpoint_last_log_entry_index_;
    // The term of the last log entry that the checkpoint covers.  
    uint64_t checkpoint_last_log_entry_term_;
    // Checkpoint data we are sending to this peer
    checkpoint_data_ptr data_;
    // Last block sent.  We do not assume that the checkpoint bytes are contiguous in memory
    // so cannot use checkpoint_next_byte_ to know where the next chunk is in data_.
    checkpoint_block last_block_sent_;
    // Has the last block been acked?  TODO: Generalize to a window/credit system?
    bool awaiting_ack_;

    peer_checkpoint(uint64_t checkpoint_last_log_entry_index, uint64_t checkpoint_last_log_entry_term,
		    checkpoint_data_ptr data)
      :
      checkpoint_next_byte_(0),
      checkpoint_last_log_entry_index_(checkpoint_last_log_entry_index),
      checkpoint_last_log_entry_term_(checkpoint_last_log_entry_term),
      data_(data),
      awaiting_ack_(false)
    {
    }

    bool prepare_checkpoint_chunk(append_checkpoint_chunk & msg);
  };

  // A peer encapsulates what a server knows about other servers in the cluster
  class peer
  {
  public:
    typedef std::string address_type;

  public:
    // peer id = same as index in peer array
    uint64_t peer_id;
    // TODO: Templatize; do we actually want multiple addresses?  Does protocol really
    // need to know whether this is one or more addresses?
    std::string address;
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
    std::shared_ptr<peer_checkpoint> checkpoint_;
    // State for tracking whether a peer that is newly added to a configuration tracking
    // log appends
    std::shared_ptr<peer_configuration_change> configuration_;

    void exit()
    {
      exiting_ = true;
    }
  };

  class client_response_continuation
  {
  public:
    uint64_t client_request_id;
    // The log index we need flushed
    uint64_t index;
    // The term that the client request was part of
    uint64_t term;
    // TODO: Do we want/need this?  Time when the client request should simply timeout.
  };

  class append_entry_continuation
  {
  public:
    // Leader that sent the append entry request we are responding to.
    uint64_t leader_id;
    // The log index we need flushed
    uint64_t begin_index;
    uint64_t end_index;
    // The term that the client request was part of
    uint64_t term;
    // Commit point associated with the append_entry request
    uint64_t commit_index;
  };

  // A test client
  class client
  {
  public:
    std::deque<client_response> responses;
    void on_client_response(const client_response & resp)
    {
      responses.push_front(resp);
    }
  };

  // Peer side of a checkpoint transfer
  class in_progress_checkpoint
  {
  public:
    uint64_t end_;
    std::shared_ptr<checkpoint_data> file_;
    
    uint64_t end() const {
      return end_;
    }

    void write(const std::vector<uint8_t> & data)
    {
      // TODO: Support async here
      file_->write(&data[0], data.size());
      end_ += data.size();
    }

    in_progress_checkpoint(checkpoint_data_store & store,
			   uint64_t last_entry_index,
			   uint64_t last_entry_term)
      :
      end_(0),
      file_(store.create(last_entry_index, last_entry_term))
    {
    }
  };

  // Per-server state related to checkpoints
  class server_checkpoint
  {
  public:
    // One after last log entry checkpointed.
    uint64_t last_checkpoint_index_;
    // Term of last checkpoint
    uint64_t last_checkpoint_term_;
    // continuations depending on checkpoint sync events
    std::vector<std::pair<std::size_t, append_checkpoint_chunk_response> > checkpoint_chunk_response_sync_continuations_;
    // Object to manage receiving a new checkpoint from a leader and writing it to a reliable
    // location
    std::shared_ptr<in_progress_checkpoint> current_checkpoint_;
    // Checkpoints live here
    checkpoint_data_store store_;

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
      current_checkpoint_.reset();
    }

    std::shared_ptr<checkpoint_data> last_checkpoint() {
      return store_.last_checkpoint();
    }

    server_checkpoint();
  };

  // A server encapsulates what a participant in the consensus protocol knows about itself
  class server
  {
  public:
    enum state { LEADER, FOLLOWER, CANDIDATE };
  private:
    static const std::size_t INVALID_PEER_ID = std::numeric_limits<std::size_t>::max();
    
    test_communicator & comm_;

    client & client_;

    // This is the main state as per the Raft description but given our full blown state
    // machine approach there are some other subordinate states to consider mostly related to
    // asynchronous interactions with the log and client.
    state state_;

    // To prevent multiple votes per-term and invalid log truncations (TODO: describe these) we must
    // persist any changes of current_term_ or voted_for_ before proceeding with the protocol.
    // This flag indicates that a change has been made to either of the above two members
    bool log_header_sync_required_;
    
    // Algorithm config parameters
    std::chrono::milliseconds election_timeout_max_;
    std::chrono::milliseconds election_timeout_min_;

    // The cluster 
    std::vector<peer> cluster_;

    // My cluster id/index
    std::size_t cluster_idx_;

    // Leader id if I know it (learned from append_entry and append_checkpoint_chunk messages)
    std::size_t leader_id_;

    // My current term number
    uint64_t current_term_;

    // the peer that got my vote (could be myself)
    peer * voted_for_;

    // Common log state
    in_memory_log log_;

    // We can learn of the commit from a
    // checkpoint or from a majority of peers acknowledging a log entry.
    // last_committed_index_ represents the last point in the log that we KNOW
    // is replicated and therefore safe to apply to a state machine.  It may
    // be an underestimate of last successfully replicated log entry but that fact
    // will later be learned (e.g. when a leader tries to append again and ???).
    // TODO: Does this point to the actual last index committed or one past????
    uint64_t last_committed_index_;
    // TODO: How to determine applied
    uint64_t last_applied_index_;
    uint64_t last_synced_index_;

    // State related to checkpoint processing
    server_checkpoint checkpoint_;
    
    // continuations depending on log sync events
    std::multimap<uint64_t, client_response_continuation> client_response_continuations_;
    std::multimap<uint64_t, append_entry_continuation> append_entry_continuations_;
    // continuations depending on log header sync events
    std::vector<append_entry> append_entry_header_sync_continuations_;
    std::vector<append_checkpoint_chunk> append_checkpoint_chunk_header_sync_continuations_;
    std::vector<std::pair<uint64_t, vote_response> > vote_response_header_sync_continuations_;
    // This flag is essentially another log header sync continuation that indicates vote requests 
    // should be sent out after log header sync (which is the case when we transition to CANDIDATE
    // but not when we transition to FOLLOWER).
    bool send_vote_requests_;


    bool no_log_header_sync_continuations_exist() const {
      return !send_vote_requests_ && 0 == vote_response_header_sync_continuations_.size() &&
	0 == append_entry_header_sync_continuations_.size();
    }
    
    uint64_t last_log_entry_term() const {
      // Something subtle with snapshots/checkpoints occurs here.  
      // After a checkpoint we may have no log entries so the term has to be recorded at the time
      // of the checkpoint.
      return log_.empty() ? checkpoint_.last_checkpoint_term_ : log_.last_entry().term;
    }
    uint64_t last_log_entry_index() const {
      return log_.last_index();
    }        
    uint64_t log_start_index() const {
      return log_.start_index();
    }
    
    // Used by FOLLOWER and CANDIDATE to decide when to initiate a new election.
    std::chrono::time_point<std::chrono::steady_clock> election_timeout_;

    peer & self() {
      return cluster_[cluster_idx_];
    }

    peer & peer_from_id(uint64_t peer_id) {
      return cluster_[peer_id];
    }

    std::chrono::time_point<std::chrono::steady_clock> new_election_timeout() const;
    std::chrono::time_point<std::chrono::steady_clock> new_heartbeat_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const;

    void send_vote_requests();
    void send_vote_response(const vote_response & resp);
    void send_append_entries(std::chrono::time_point<std::chrono::steady_clock> clock_now);
    void send_heartbeats(std::chrono::time_point<std::chrono::steady_clock> clock_now);
    void send_checkpoint_chunk(std::chrono::time_point<std::chrono::steady_clock> clock_now, uint64_t peer_id);

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

    // Based on log sync and/or append_response try to advance the commit point.
    void try_to_commit();

    // Append Entry processing
    void internal_append_entry(const append_entry & req);

    // Append Checkpoint Chunk processing
    void internal_append_checkpoint_chunk(const append_checkpoint_chunk & req);
    void internal_append_checkpoint_chunk_sync(std::size_t leader_id, const append_checkpoint_chunk_response & resp);
    void load_checkpoint();

    // Guards for state transitions.  Most of the cases in which I can't make a transition
    // it is because the transition requires a disk write of the header and such a write is already in
    // progress.  If we have a sufficiently slow disk we are essentially dead!
    bool can_become_follower_at_term(uint64_t term);

    // State Transitions
    void become_follower(uint64_t term);
    void become_candidate();
    void become_leader();
  public:
    server(test_communicator & comm, client & c, std::size_t cluster_idx, const std::vector<peer>& peers);
    ~server();

    // Events
    void on_timer();
    void on_client_request(const client_request & req);
    void on_request_vote(const request_vote & req);
    void on_vote_response(const vote_response & resp);
    void on_append_entry(const append_entry & req);
    void on_append_response(const append_response & req);
    void on_append_checkpoint_chunk(const append_checkpoint_chunk & req);
    void on_append_checkpoint_chunk_response(const append_checkpoint_chunk_response & resp);
    // Append to log flushed to disk
    void on_log_sync(uint64_t index);
    // Update of log header (current_term_, voted_for_) flushed to disk
    void on_log_header_sync();
    // Update of checkpoint file
    void on_checkpoint_sync();

    // Checkpoint stuff
    // TODO: I don't really have a handle on how this should be structured.  Questions:
    // 1. Who initiates periodic checkpoints (doesn't seem like it should be the consensus box; should be client/state machine)
    // 2. How to support mixing of client checkpoint state and consensus checkpoint state (e.g.
    // one could have consensus enforce that client checkpoint state includes consensus state via
    // a subclass)
    // 3. Interface between consensus and existing checkpoint state (which must be read both to restore
    // from a checkpoint and to send checkpoint data from a leader to the peer).

    // This avoids having consensus constrain how the client takes and writes a checkpoint, but eventually
    // have to let consensus know when a checkpoint is complete so that it can be sent to a peer (for example).
    std::shared_ptr<checkpoint_data> begin_checkpoint(uint64_t last_index_in_checkpoint) const;
    bool complete_checkpoint(uint64_t last_index_in_checkpoint, std::shared_ptr<checkpoint_data> i_dont_like_this);

    // Observers for testing
    uint64_t current_term() const {
      return current_term_;
    }
    uint64_t commit_index() const {
      return last_committed_index_;
    }
    state get_state() const {
      return state_;
    }
    const peer & get_peer_from_id(uint64_t peer_id) const {
      return cluster_.at(peer_id);
    }
    bool log_header_sync_required() const {
      return log_header_sync_required_;
    }
    in_memory_log & log() {
      return log_;
    }
    server_checkpoint & checkpoint() {
      return checkpoint_;
    }
  };
}
#endif

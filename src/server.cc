#include <random>

#include <boost/log/core.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/sinks/text_file_backend.hpp>

#include "server.hh"

// General TODO:
// log_entry_type::entry should probably be async since we shouldn't assume the entire log can fit into memory.

namespace raft {

  server::server(server::communicator_type & comm, client_type & c, server::log_type & l,
		 checkpoint_data_store_type & store,
		 configuration_manager_type & config_manager)
    :
    comm_(comm),
    client_(c),
    state_(FOLLOWER),
    election_timeout_max_(300),
    election_timeout_min_(150),
    current_term_(0),
    voted_for_(nullptr),
    log_(l),
    log_header_sync_required_(false),
    last_committed_index_(0),
    last_synced_index_(0),
    checkpoint_(store),
    configuration_(config_manager),
    send_vote_requests_(false)
  {
    election_timeout_ = new_election_timeout();

    // TODO: Read the log if non-empty and apply configuration entries as necessary
    // TODO: This should be async.
    for(auto idx = log_.start_index(); idx < log_.last_index(); ++idx) {
      auto & e(log_.entry(idx));
      if (log_entry_type::CONFIGURATION == e.type) {
	configuration_.add_logged_description(idx, e.configuration);
      }
    }

    // NOTE: We set voted_for_ and current_term_ from the log header so we do not
    // sync!  This is the only place in which this should set them without a sync.
    if (INVALID_PEER_ID != log_.voted_for()) {
      voted_for_ = &configuration().peer_from_id(log_.voted_for());
    }
    current_term_ = log_.current_term();	

    // TODO: Read any checkpoint
    load_checkpoint();
  }

  server::~server()
  {
  }

  std::chrono::time_point<std::chrono::steady_clock> server::new_election_timeout() const
  {
    std::default_random_engine generator;
    std::uniform_int_distribution<std::chrono::milliseconds::rep> dist(election_timeout_min_.count(), 
								       election_timeout_max_.count());
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(generator));
  }

  std::chrono::time_point<std::chrono::steady_clock>
  server::new_heartbeat_timeout(std::chrono::time_point<std::chrono::steady_clock> clock_now) const
  {
    std::default_random_engine generator;
    std::uniform_int_distribution<std::chrono::milliseconds::rep> dist(election_timeout_min_.count()/2, 
								       election_timeout_max_.count()/2);
    return std::chrono::steady_clock::now() + std::chrono::milliseconds(dist(generator));
  }

  void server::send_vote_requests()
  {
    BOOST_ASSERT(!log_header_sync_required_);
    for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
      auto i = peer_it->peer_id;
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" requesting vote from peer " << i;
      peer_type & p(*peer_it);
      p.vote_ = boost::logic::indeterminate;
      if (i != my_cluster_id()) {
	request_vote msg;
	msg.recipient_id = i;
	msg.term_number = current_term_;
	msg.candidate_id = my_cluster_id();
	msg.last_log_index = last_log_entry_index();
	msg.last_log_term = last_log_entry_term();
	// TODO: Templatize and abstract out the communicator address of the peer.
	comm_.send(i, msg);
      }
    }
  }

  void server::send_append_entries(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
      peer_type & p(*peer_it);
      auto i =  p.peer_id;
      if (i != my_cluster_id() && last_log_entry_index() > p.match_index_) {
	// This is next log entry the peer needs
	uint64_t previous_log_index = p.next_index_;
	if (p.next_index_ < log_start_index()) {
	  // We have checkpointed a portion of the log the peer requires
	  // we must send a checkpoint instead and then we can apply log entries.
	  send_checkpoint_chunk(clock_now, i);
	  return;
	}

	// Last time we sent to the peer it was at this term
	uint64_t previous_log_term = 0;
	if (previous_log_index > log_start_index()) {
	  // We've still got the entry so grab the actual term
	  previous_log_term = log_.entry(previous_log_index-1).term;
	} else if (previous_log_index == 0) {
	  // First log entry.  No previous log term.
	  previous_log_term = 0;
	} else if (previous_log_index == checkpoint_.last_checkpoint_index_) {
	  // Boundary case: Last log entry sent was the last entry in the last checkpoint
	  // so we know what term that was
	  previous_log_term = checkpoint_.last_checkpoint_term_;
	} else {
	  // TODO: How do we get here??? Send checkpoint seems to be the right response.
	  send_checkpoint_chunk(clock_now, i);
	  return;
	}

	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " sending append_entry to peer " << i << "; last_log_entry_index()=" << last_log_entry_index() <<
	  " peer.match_index=" << p.match_index_ << "; peer.next_index_=" << p.next_index_;

	append_entry_type msg;
	msg.recipient_id = i;
	msg.term_number = current_term_;
	msg.leader_id = my_cluster_id();
	msg.previous_log_index = previous_log_index;
	msg.previous_log_term = previous_log_term;

	// TODO: For efficiency avoid sending actual data in messages unless p.is_next_index_reliable_ == true
	uint64_t log_entries_sent = 0;
	// Actually put the entries into the message.
	uint64_t idx_end = last_log_entry_index();
	for(uint64_t idx = p.next_index_; idx != idx_end; ++idx) {
	  // TODO: Enforce message size limits
	  msg.entry.push_back(log_.entry(idx));
	  log_entries_sent += 1;
	}
	msg.leader_commit_index = std::min(last_committed_index_, previous_log_index+log_entries_sent);
	
	// TODO: Templatize and abstract out the communicator address of the peer.
	comm_.send(i, msg);
	p.requires_heartbeat_ = new_heartbeat_timeout(clock_now);
      }
    }
  }

  void server::send_heartbeats(std::chrono::time_point<std::chrono::steady_clock> clock_now)
  {
    for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
      peer_type & p(*peer_it);
      if (p.peer_id != my_cluster_id() && p.requires_heartbeat_ <= clock_now) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " sending empty append_entry to peer " << p.peer_id << " as heartbeat";
	append_entry_type msg;
	msg.recipient_id = p.peer_id;
	msg.term_number = current_term_;
	msg.leader_id = my_cluster_id();
	// TODO: What to set in these 3???  Should this logic be the same
	// as send_append_entries???
	msg.previous_log_index = 0;
	msg.previous_log_term = 0;
	msg.leader_commit_index = 0;
	// No entries
	//msg.entry;
	// TODO: Templatize and abstract out the communicator address of the peer.
	comm_.send(p.peer_id, msg);
	p.requires_heartbeat_ = new_heartbeat_timeout(clock_now);
      }
    }
  }

  void server::send_checkpoint_chunk(std::chrono::time_point<std::chrono::steady_clock> clock_now, uint64_t peer_id)
  {
    peer_type & p(peer_from_id(peer_id));

    // Internal error if we call this method without a checkpoint to send
    BOOST_ASSERT(checkpoint_.last_checkpoint());

    // TODO: Should we arrange for abstractions that permit use of sendfile for checkpoint data?  Manual chunking
    // involves extra work and shouldn't really be necessary (though the manual chunking does send heartbeats).
    if (!p.checkpoint_) {
      const auto & config(configuration_.get_checkpoint());
      BOOST_ASSERT(config.is_valid());
      p.checkpoint_.reset(new peer_checkpoint_type(checkpoint_.last_checkpoint_index_, checkpoint_.last_checkpoint_term_,
						   config, checkpoint_.last_checkpoint()));
    }

    // Are we waiting for a response to a previous chunk?
    if (p.checkpoint_->awaiting_ack_) {
      return;
    }

    // Did we already finish the checkpoint?
    if (p.checkpoint_->data_->is_final(p.checkpoint_->last_block_sent_)) {
      return;
    }

    // Move to next chunk and send
    p.checkpoint_->last_block_sent_ = p.checkpoint_->data_->block_at_offset(p.checkpoint_->checkpoint_next_byte_);

    append_checkpoint_chunk_type msg;
    msg.recipient_id = peer_id;
    msg.term_number = current_term_;
    msg.leader_id = my_cluster_id();
    msg.last_checkpoint_index = p.checkpoint_->checkpoint_last_log_entry_index_;
    msg.last_checkpoint_term = p.checkpoint_->checkpoint_last_log_entry_term_;
    msg.last_checkpoint_configuration = p.checkpoint_->checkpoint_last_configuration_;
    msg.checkpoint_begin = p.checkpoint_->data_->block_begin(p.checkpoint_->last_block_sent_);
    msg.checkpoint_end = p.checkpoint_->data_->block_end(p.checkpoint_->last_block_sent_);
    msg.checkpoint_done = p.checkpoint_->data_->is_final(p.checkpoint_->last_block_sent_);
    msg.data.assign(p.checkpoint_->last_block_sent_.block_data_,
		    p.checkpoint_->last_block_sent_.block_data_+p.checkpoint_->last_block_sent_.block_length_);
    comm_.send(peer_id, msg);
    p.checkpoint_->awaiting_ack_ = true;

    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
      " sending append_checkpoint_chunk to peer " << peer_id << "; log_start_index()=" << log_start_index() <<
      " last_log_entry_index()=" << last_log_entry_index() <<
      " peer.match_index=" << p.match_index_ << " peer.next_index_=" << p.next_index_ <<
      " checkpoint.last_log_entry_index=" << p.checkpoint_->checkpoint_last_log_entry_index_ <<
      " checkpoint.last_log_entry_term=" << p.checkpoint_->checkpoint_last_log_entry_term_ <<
      " bytes_sent=" << (msg.checkpoint_end -  msg.checkpoint_begin);

    p.requires_heartbeat_ = new_heartbeat_timeout(clock_now);
  }
  
  bool server::can_become_follower_at_term(uint64_t term)
  {
    if (term > current_term_ && log_header_sync_required_) {
      // I need to advance term but I'm already trying to do that and have an outstanding
      // disk write.  Since I can't cancel the current write I am going to have to wait to
      // issue another.
      return false;
    } else {
      return true;
    }
  }

  void server::become_follower(uint64_t term)
  {
    if (term > current_term_) {
      BOOST_ASSERT(can_become_follower_at_term(term));
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" becoming FOLLOWER at term " << term;
      state_ = FOLLOWER;
      current_term_ = term;
      voted_for_ = nullptr;
      // We've updated current_term_ and voted_for_ ; must sync to disk before responding 
      // Don't call log_.sync_header() here.  We've set the flag and the caller needs to request the sync
      // before returning.
      log_header_sync_required_ = true;
      // ??? Do this now or after the sync completes ???
      election_timeout_ = new_election_timeout();
      leader_id_ = INVALID_PEER_ID;
      log_.update_header(current_term_, INVALID_PEER_ID);

      // Cancel any pending configuration change
      configuration().reset_staging_servers();

      // Cancel any in-progress checkpoint
      checkpoint_.abandon();
    }

    // TODO: If transitioning from leader there may be a log sync to disk
    // that we need to issue?  LogCabin does this but I'm not sure that I see why that it is needed from the protocol perspective.
    // What is required is that the FOLLOWER sync before ack'ing any append entries.
    // Do that now after waiting for any in-progress flush to complete.

    // TODO: If we are transitioning from leader there may be some pending client requests.  What to
    // do about them???  Probably no harm is just letting them linger since the requests are committed.
  }

  void server::become_candidate()
  {
    BOOST_ASSERT(state_ != LEADER);
    if (!configuration().is_valid()) {
      // No configuration yet, we just wait for one to arrive
      election_timeout_ = new_election_timeout();
      return;
    }
    
    if (!log_header_sync_required_) {
      BOOST_ASSERT(no_log_header_sync_continuations_exist());
      BOOST_ASSERT(!send_vote_requests_);
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" becoming CANDIDATE and starting election at term " << (current_term_+1);

      // Cancel any in-progress checkpoint
      checkpoint_.abandon();
      
      current_term_ += 1;
      state_ = CANDIDATE;
      election_timeout_ = new_election_timeout();
      voted_for_ = &self();
      // Log and flush
      log_header_sync_required_ = true;
      log_.update_header(current_term_, voted_for_->peer_id);
      log_.sync_header();
      // Set flag so that vote requests get sent once the log header sync completes (this
      // flag is basically a continuation).  TODO: Do we really need this flag or should this be
      // based on checking that state_ == CANDIDATE after the log header sync is done (in on_log_header_sync)?
      send_vote_requests_ = true;

    } else {
      // TODO: Perhaps make a continuation here and make the candidate transition after the sync?
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" can't become CANDIDATE at term " << (current_term_+1) << " due to outstanding log header sync";
    }  
  }

  void server::become_candidate_on_log_header_sync()
  {
    if (send_vote_requests_) {
      send_vote_requests_ = false;
      // If not a candidate then presumably shouldn't send out vote requests.
      // TODO: On the other hand, if I am a candidate then is it even possible that
      // send_vote_requests_ = false?  We know that setting state_=CANDIDATE implies
      // send_vote_requests_=true, so the question is whether after becoming a CANDIDATE
      // is it possible to trigger multiple log header syncs
      if (state_ == CANDIDATE) {
	send_vote_requests();
	// Special case: single server cluster become_leader without any votes received
	if (has_quorum()) {
	  become_leader();
	}
      }
    }
  }

  void server::become_leader()
  {
    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
      " becoming LEADER";
    BOOST_ASSERT(!log_header_sync_required_);
    state_ = LEADER;
    leader_id_ = my_cluster_id();
    election_timeout_ = new_election_timeout();

    // TODO: Make this relative to the configuration.  I do think this is already correct; send out to all known servers.
    // Send out empty append entries to assert leadership
    std::chrono::time_point<std::chrono::steady_clock> clock_now = std::chrono::steady_clock::now();
    for(auto peer_it = configuration().begin_peers(), peer_end = configuration().end_peers(); peer_it != peer_end; ++peer_it) {
      peer_type & p(*peer_it);
      if (p.peer_id != my_cluster_id()) {
	p.requires_heartbeat_ = clock_now;
	// Guess that peer is up to date with our log; this may be optimistic.  We'll avoid chewing up bandwidth until
	// we hear back from the peer about it's last log entry.
	p.next_index_ = last_log_entry_index();
	p.is_next_index_reliable_ = false;
	p.match_index_ = 0;
	p.checkpoint_.reset();
      } else {
	// TODO: Is this really guaranteed to be true (i.e. the last log entry is synced)?
	// We do know that the log header has been synced, should that also imply a log sync?
	// In LogCabin I believe this is always true because all log writes in a non-leader are synchronous.
	// It is also true that in LogCabin the last_synced_index isn't updated when not leader so in fact it
	// is necessary to update its value when one becomes leader because it must be assumed stale;
	// maybe the sync isn't necessary just being able to know the correct value necessary (and  that
	// happens to be the end of the log)?
	// Perhaps we have to wait for the log to sync before we proceed?
	// My current understanding is that there isn't any need for the leader log to be sync'd
	// last_synced_index_ = last_log_entry_index();
      }
    }

    // TODO: Do we really do this or is this equivalent to sending the NOOP log entry?
    send_heartbeats(clock_now);

    // Append a new log entry in order to get the committed index updated.  TODO: Do we need to flush the log????
    // The empty log entry will also trigger append_entry messages to the peers to let them know we have
    // become leader.  The latter could also be achieved with heartbeats, but would it get the commit index advanced?
    // The rationale for getting commit index advanced has to do with the protocol for doing non-stale reads (that uses
    // RaftConsensus::getLastCommitIndex in logcabin); I need to understand this protocol.
    // std::vector<log_entry_type> v(1);
    // v.back().type = log_entry_type::NOOP;
    // v.back().term = current_term_;
    // log_.append(v);
    

    // TODO: As an optimization cancel any outstanding request_vote since they are not needed
    // we've already got quorum
  }

  void server::on_timer()
  {
    std::chrono::time_point<std::chrono::steady_clock> clock_now = std::chrono::steady_clock::now();

    // TODO: Add checks for slow writes (log flushes and checkpoint writes).  At a miniumum we want to cry
    // for help in such situations.
    switch(state_) {
    case FOLLOWER:
      if (election_timeout_ < clock_now) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " FOLLOWER lost contact with leader.";
	become_candidate();
      }
      break;
    case CANDIDATE:
      if (election_timeout_ < clock_now) {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " CANDIATE timed out election.";
	become_candidate();
      }
      break;
    case LEADER:
      // TODO: Encapsulate this...
      if (configuration().is_staging()) {
	if (!configuration().staging_servers_caught_up()) {
	  set_configuration_response_type resp;
	  if (!configuration().staging_servers_making_progress(resp.bad_servers)) {
	    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " some server in new configuration not making progress so rejecting configuration.";
	    configuration().reset_staging_servers();
	    BOOST_ASSERT(!resp.bad_servers.servers.empty());
	    resp.result = FAIL;
	    client_.on_configuration_response(resp);
	  } else {
	    BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " servers in new configuration not yet caught up.";
	  }
	} else {
	  // Append a transitional configuration entry to the log
	  // and wait for it to commit.
	  std::vector<log_entry_type> v(1);
	  v.back().type = log_entry_type::CONFIGURATION;
	  configuration().get_transitional_configuration(v.back().configuration);
	  v.back().term = current_term_;
	  auto indices = log_.append(v);
	  // This will update the value of configuration()
	  configuration_.add_logged_description(indices.first, v.back().configuration);
	  BOOST_ASSERT(configuration().is_transitional());
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " servers in new configuration caught up. Logging transitional entry at index " << indices.first;
	}
      }
      // Send heartbeats and append_entries if needed
      send_append_entries(clock_now);
      send_heartbeats(clock_now);
      break;
    }
  }

  // Called when the log has been synced to disk
  void server::on_log_sync(uint64_t index)
  {
    last_synced_index_ = index;
    BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
      " log synced to " << last_synced_index_ <<
      " with end of log at " << last_log_entry_index();
    try_to_commit();

    // This is mostly relevant if I am a FOLLOWER (because I can't respond to
    // append_entries until the log entry is sync'd).
    // TODO: What happens if I've changed terms at this point???  I think this
    // will send responses with the updated term and success = false.
    {
      auto it = append_entry_continuations_.begin();
      auto e = append_entry_continuations_.upper_bound(last_synced_index_);
      if (it != e) {
	for(; it != e; ++it) {
	  BOOST_ASSERT(it->second.end_index <= last_synced_index_);
	  append_response resp;
	  resp.recipient_id = my_cluster_id();
	  resp.term_number = current_term_;
	  resp.request_term_number = it->second.term;
	  resp.begin_index = it->second.begin_index;
	  // TODO: Can I respond to more than what was requested?  Possibly useful idea for pipelined append_entry
	  // requests in which we can sync the log once for multiple append_entries.
	  resp.last_index = it->second.end_index;
	  // Leaders won't look at this if it's false
	  resp.success = it->second.term == current_term_;
	  comm_.send(it->second.leader_id, resp);
	}
	append_entry_continuations_.erase(append_entry_continuations_.begin(), e);
      } else {
	BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " log synced to " << last_synced_index_ <<
	  " but no append_entries successfully synced";
      }
    }
  }

  void server::on_log_header_sync()
  {
    BOOST_ASSERT(log_header_sync_required_);
    // TODO: I think we can only be FOLLOWER or CANDIDATE.  A LEADER never
    // updates its log header (or maybe snapshots and config changes will make it do so).
    // No, the log header is just voted_for and current_term and neither of those can change
    // when we are LEADER.
    BOOST_ASSERT(state_ == FOLLOWER || state_ == CANDIDATE);
    log_header_sync_required_ = false;
    for(auto & vr : vote_response_header_sync_continuations_) {
     comm_.send(vr.first, vr.second);      
    }
    vote_response_header_sync_continuations_.clear();
    for(auto & acc : append_checkpoint_chunk_header_sync_continuations_) {
      internal_append_checkpoint_chunk(acc);
    }
    append_checkpoint_chunk_header_sync_continuations_.clear();
    for(auto & ae : append_entry_header_sync_continuations_) {
      internal_append_entry(ae);
    }
    append_entry_header_sync_continuations_.clear();
    
    become_candidate_on_log_header_sync();
  }
  
  void server::on_checkpoint_sync()
  {
    BOOST_ASSERT(checkpoint_.current_checkpoint_);
    BOOST_ASSERT(1U == checkpoint_.checkpoint_chunk_response_sync_continuations_.size());
    internal_append_checkpoint_chunk_sync(checkpoint_.checkpoint_chunk_response_sync_continuations_[0].first,
					  checkpoint_.checkpoint_chunk_response_sync_continuations_[0].second);
  }
  
  void server::on_client_request(const client_request & req)
  {
    switch(state_) {
    case FOLLOWER:
    case CANDIDATE:
      {
	// Not leader don't bother.
	client_response resp;
	resp.id = req.id;
	resp.result = NOT_LEADER;
	resp.index = 0;
	resp.leader_id = leader_id_;
	client_.on_client_response(resp);
	return;
      }
    case LEADER:
      {
	// Create a log entry and try to replicate it
	std::vector<log_entry_type> v(1);
	v.back().type = log_entry_type::COMMAND;
	v.back().data = req.command;
	v.back().term = current_term_;
	// Append to the in memory log then we have to wait for
	// the new log entry to be replicated to a majority (TODO: to disk?)/committed before
	// returning to the client
	auto indices = log_.append(v);
	client_response_continuation cont;
	cont.client_request_id = req.id;
	cont.index = indices.second-1;
	cont.term = current_term_;
	client_response_continuations_.insert(std::make_pair(cont.index, cont));
	// TODO: Do we want to let the log know that we'd like a flush?
	// TODO: What triggers the sending of append_entries to peers?  Currently it is a timer
	// but that limits the minimum latency of replication.  Is that a problem?  I suppose we
	// could just call append_entries or on_timer here.
	return;
      }
    }
  }

  void server::on_set_configuration(const set_configuration_request_type & req)
  {
    if (LEADER != state_) {
      set_configuration_response_type resp;
      resp.result = NOT_LEADER;
      client_.on_configuration_response(resp);
      return;
    }

    if (configuration().configuration_id() != req.old_id) {
      set_configuration_response_type resp;
      resp.result = FAIL;
      client_.on_configuration_response(resp);
      return;
    }

    if (!configuration().is_stable()) {
      set_configuration_response_type resp;
      resp.result = FAIL;
      client_.on_configuration_response(resp);
      return;
    }

    configuration().set_staging_configuration(req.new_configuration);
    BOOST_ASSERT(!configuration().is_stable());
}

  void server::on_request_vote(const request_vote & req)
  {
    // QUESTION: Under what circumstances might we get req.term_number == current_term_ and nullptr == voted_for_?
    // ANSWER: I may have gotten a vote request from a candidate that bumped up my current_term_ but didn't have
    // all of my log entries hence didn't get my vote.
    
    // QUESTION: Is it possible for req.term_number > current_term_ but false==candidate_log_more_complete?
    // ANSWER: Yes.  Here is an example.  I am leader at current_term_, one of my followers gets disconnected 
    // and its log falls behind.  It starts a new election, connectivity returns and I get a request_vote on a new term.
    
    // Term has advanced so become a follower.  Note that we may not vote for this candidate even though
    // we are possibly giving up leadership.
    if (req.term_number > current_term_) {
      if (!can_become_follower_at_term(req.term_number)) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " cannot advance to term " << req.term_number <<
	  " due to outstanding log header sync.  Ignoring request_vote message from candidate " <<
	  req.candidate_id << ".";
	return;
      }
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" becoming follower due to vote request from " << req.candidate_id << " at higher term " << req.term_number;
      become_follower(req.term_number);
    }
    
    // TODO: Is it the case that every entry in a FOLLOWER log is committed?  No, since a FOLLOWER will receive log entries
    // from a LEADER that may not get quorum hence never get committed.  Therefore
    // the log completeness check is strictly stronger than "does the candidate have all my committed entries"?  How do we know that
    // there is a possible leader when this stronger criterion is enforced?  Specifically I am concerned that leadership
    // is flapping and FOLLOWER logs are getting entries in them that never get committed and this makes it hard for anyone to get votes.
    // Maybe a good way of thinking about why this isn't an issue is that we can lexicographically order peers by the pair (last_log_term, last_log_index)
    // which gives a total ordering.  During any election cycle peers maximal with respect to the total ordering are possible leaders.
    bool candidate_log_more_complete_than_mine = req.last_log_term > last_log_entry_term() ||
      (req.last_log_term == last_log_entry_term() && req.last_log_index >= last_log_entry_index());

    // Vote at most once in each term and only vote for a candidate that has all of my committed log entries.
    // The point here is that Raft has the property that a canidate cannot become leader unless it has all committed entries; this
    // means there is no need for a protocol to identify and ship around committed entries after the fact.
    // The fact that the check above implies that the candidate has all of my committed log entries is
    // proven in Ongaro's thesis.
    //
    // Note that in fact it is impossible for req.term_number > current_term_ at this point because
    // we have already updated current_term_ above in that case and become a FOLLOWER.  Logically this way of writing things
    // is a bit clearer to me.
    if (req.term_number >= current_term_ && candidate_log_more_complete_than_mine && nullptr == voted_for_) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" voting for " << req.candidate_id << " at term " << req.term_number;
      become_follower(req.term_number);
      voted_for_ = &peer_from_id(req.candidate_id);
      log_header_sync_required_ = true;
      election_timeout_ = new_election_timeout();
      // Changed voted_for_ and possibly current_term_ ; propagate to log
      log_.update_header(current_term_, req.candidate_id);
      // Must sync log header here because we can't have a crash make us vote twice; the
      // sync call is done below.
    }

    vote_response msg;
    msg.peer_id = my_cluster_id();
    msg.term_number = current_term_;
    msg.request_term_number = req.term_number;
    msg.granted = current_term_ == req.term_number && voted_for_ == &peer_from_id(req.candidate_id);

    if (log_header_sync_required_) {
      log_.sync_header();
      vote_response_header_sync_continuations_.push_back(std::make_pair(req.candidate_id, msg));
    } else {    
      // TODO: Templatize and abstract out the communicator address of the peer.
      comm_.send(req.candidate_id, msg);
    }
  }

  void server::on_vote_response(const vote_response & resp)
  {
    switch(state_) {
    case CANDIDATE:
      // Detect if this response if from a vote_request on a previous
      // term (e.g. we started an election, it timed out we started a new one and then we
      // got this response from the old one).  If so ignore it.
      if (current_term_ == resp.request_term_number) {
	if (current_term_ < resp.term_number) {
	  if (can_become_follower_at_term(resp.term_number)) {
	    become_follower(resp.term_number);
	    if (log_header_sync_required_) {
	      log_.sync_header();
	    }
	  } else {
	    BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	      " cannot advance to term " << resp.term_number <<
	      " due to outstanding log header sync.  Ignoring vote_response message.";
	  }
	} else {
	  // During voting we requested header sync when we updated the term and voted for ourselves.
	  // Vote requests didn't go out until header was synced.
	  BOOST_ASSERT(!log_header_sync_required_);
	  BOOST_ASSERT(current_term_ == resp.term_number);
	  BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " received vote " << resp.granted << " from peer " << resp.peer_id;
	  peer_type & p(peer_from_id(resp.peer_id));
	  // TODO: What happens if we get multiple responses from the same peer
	  // but with different votes?  Is that a protocol error????
	  p.vote_ = resp.granted;
	  if (has_quorum()) {
	    become_leader();
	  }
	}
      }
      break;
    case FOLLOWER:
    case LEADER:
      // Nothing to do.
      break;
    }
  }

  void server::internal_append_entry(const server::append_entry_type & req)
  {
    BOOST_ASSERT(req.term_number == current_term_ && state_ == FOLLOWER);

    // May be the first append_entry for this term; that's when we learn
    // who the leader actually is.
    if (leader_id_ == INVALID_PEER_ID) {
      leader_id_ = req.leader_id;
    } else {
      BOOST_ASSERT(leader_id_ == req.leader_id);
    }

    // TODO: Finish

    // Do we have all log entries up to this one?
    if (req.previous_log_index > last_log_entry_index()) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" received append entry with gap.  req.previous_log_index=" << req.previous_log_index <<
	" last_log_entry_index()=" << last_log_entry_index();
      append_response msg;
      msg.recipient_id = my_cluster_id();
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.last_index = last_log_entry_index();
      msg.success = false;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Do the terms of the last log entry agree?  By the Log Matching Property this will guarantee
    // the logs of the leader that is appending will be identical at the point we begin appending.
    // See the extended Raft paper Figure 7 for examples of how this can come to pass.
    if (req.previous_log_index > log_start_index() &&
	req.previous_log_term != log_.entry(req.previous_log_index-1).term) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" received append entry with mismatch term.  req.previous_log_index=" << req.previous_log_index <<
	" req.previous_log_term=" << req.previous_log_term <<
	" log_.entry(" << (req.previous_log_index-1) << ").term=" << log_.entry(req.previous_log_index-1).term;
      append_response msg;
      msg.recipient_id = my_cluster_id();
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.last_index = req.previous_log_index;
      msg.success = false;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Detect duplicate log entry
    // Here we guard against
    // 1) Getting a duplicate that causes index to go backwards (and would
    // cause truncation of already committed/accepted entries if not guarded).
    // 2) Getting an entry, successfully committing and then getting a duplicate
    // and getting a failure when trying to commit the duplicate (corrupting an already
    // committed entry).

    auto it = req.entry.begin();
    auto entry_end = req.entry.end();
    uint64_t entry_index = req.previous_log_index;
    // Scan through entries in message to find where to start appending from
    for(; it!=entry_end; ++it, ++entry_index) {
      // TODO: should this be entry_index <= log_start_index()?
      if (entry_index < log_start_index()) {
	// This might happen if a leader sends us an entry we've already checkpointed
	continue;
      }
      if (last_log_entry_index() > entry_index) {
	if (log_.entry(entry_index).term == current_term_) {
	  // We've already got this message, keep looking for something new.
	  continue;
	}
	// We've got some uncommitted cruft in our log.  Truncate it and then start appending with
	// this entry.
	BOOST_ASSERT(last_committed_index_ < entry_index);
	// At this point we know that the leader log differs from what we've got so get rid of things
	// from here to the end
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " truncating uncommitted log entries [" << entry_index <<
	  "," << last_log_entry_index() << ") starting at term " << log_.entry(entry_index).term;
	log_.truncate_suffix(entry_index);
	// If we are truncating a transitional configuration then we have learned that an attempt to set
	// configuration has failed.  We need to tell the client this and reset to the prior stable configuration.
	if(configuration().is_transitional() && configuration().configuration_id() >= entry_index) {
	  set_configuration_response_type resp;
	  resp.result = FAIL;
	  client_.on_configuration_response(resp);
	}
	configuration_.truncate_suffix(entry_index);
	// TODO: Something with configurations.  Anything other than truncate the state in the manager?
	break;
      } else {
	// A new entry; start appending from here.
	break;
      }
    }

    // TODO: If nothing remains to append we should just return here.  Should update the committed id even if no new log entries

    if (it != entry_end) {
      BOOST_LOG_TRIVIAL(debug) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" appending log entries [" << entry_index <<
	"," << (entry_index + std::distance(it,entry_end)) << ") starting at term " << it->term;
    }

    // Append to the log
    std::vector<log_entry_type> to_append;
    uint64_t entry_end_index = entry_index;
    for(; it != entry_end; ++it, ++entry_end_index) {
      to_append.push_back(*it);
    }
    std::pair<log_index_type, log_index_type> range = log_.append(to_append);

    // Make sure any configuration entries added are reflected in the configuration manager.
    for(std::size_t i=0; i<to_append.size(); ++i) {
      if (to_append[i].type == log_entry_type::CONFIGURATION) {
	configuration_.add_logged_description(range.first + i, to_append[i].configuration);
      }
    }

    // Update committed id
    // TODO: Do we need to wait for log flush to update this?  I don't see why since the leader has determined
    // this value (possibly without our vote).  On the other hand there might be a problem if we allow a state machine
    // to have entries applied without having them be persistent yet; I think this concern would be valid if the state machine was
    // persisting recoverable state outside of the Raft checkpoint.  Ongaro makes it clear that there is no need
    // to sync this to disk here.
    if (last_committed_index_ < req.leader_commit_index) {
      update_last_committed_index(req.leader_commit_index);
    }

    // Create a continuation for when the log is flushed to disk
    append_entry_continuation cont;
    cont.leader_id = req.leader_id;
    cont.begin_index = entry_index;
    cont.end_index = entry_end_index;
    cont.term= req.term_number;
    // TODO: Not currently using this since I advance last_committed_index_ above.  Figure out if that
    // is the correct thing to do (we should not need this in the continuation according to Ongaro, I just need to understand why).
    // TODO: Better to use lambdas for the continuation?  That way we can include the code right here.
    cont.commit_index = req.leader_commit_index;
    append_entry_continuations_.insert(std::make_pair(cont.end_index, cont));
        
    // Question: When do we flush the log?  Presumably before we respond since the leader takes our
    // response as durable. Answer: Yes.
    // Question: When do we apply the entry to the state machine?  Do we need to wait until the leader informs us this
    // entry is committed?  Answer: Yes.
  }

  void server::on_append_entry(const server::append_entry_type & req)
  {
    if (req.term_number < current_term_) {
      // There is a leader out there that needs to know it's been left behind.
      append_response msg;
      msg.recipient_id = my_cluster_id();
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.last_index = last_log_entry_index();
      msg.success = false;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Should not get an append entry with current_term_ as LEADER
    // that would mean I am sending append_entry messages to myself
    // or multiple leaders on the same term.
    BOOST_ASSERT(req.term_number > current_term_ || state_ != LEADER);

    // I highly doubt a disk write can be reliably cancelled so we have to wait for
    // an outstanding one to complete.  Instead of queuing up just pretend the message is lost.
    if (!can_become_follower_at_term(req.term_number)) {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" cannot advance to term " << req.term_number <<
	" due to outstanding log header sync.  Ignoring append_entry message.";
      return;
    }

    // At this point doesn't really matter what state I'm currently in
    // as I'm going to become a FOLLOWER.
    become_follower(req.term_number);

    // If we were FOLLOWER we didn't reset election timer in become_follower() so do
    // it here as well
    if (log_header_sync_required_) {
      // TODO: We need to put a limit on number of continuations we'll queue up
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" waiting for log header sync.  Queuing append_entry message from recipient " <<
	req.recipient_id << " previous_log_term " << req.previous_log_term <<
	" previous_log_index " << req.previous_log_index;
      log_.sync_header();
      append_entry_header_sync_continuations_.push_back(req);
    } else {
      election_timeout_ = new_election_timeout();
      internal_append_entry(req);
    }
  }

  void server::try_to_commit()
  {
    if (state_ != LEADER) {
      return;
    }

    // Committed index is relative to a configuration...
    uint64_t committed = configuration().get_committed(last_synced_index_);
    if (last_committed_index_ >= committed) {
      // Nothing new to commit
      // TODO: Can committed go backwards (e.g. on a configuration change)?  
      return;
    }

    // If we checkpointed uncommitted log that is VERY BAD
    BOOST_ASSERT(committed > log_start_index());

    if (log_.entry(committed-1).term != current_term_) {
      // TODO: What is going on here?
      return;
    }

    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
      " committed log entries [" << last_committed_index_ <<
      "," << committed << ")";
    update_last_committed_index(committed);

    // TODO: Check use of >= vs. >
    if (state_ == LEADER && last_committed_index_ > configuration().configuration_id()) {
      if (!configuration().includes_self()) {
	// We've committed a new configuration that doesn't include us anymore so give up
	// leadership
	if (can_become_follower_at_term(current_term_+1)) {
	  become_follower(current_term_+1);
	  BOOST_ASSERT(log_header_sync_required_);
	  if (log_header_sync_required_) {
	    log_.sync_header();
	  }
	  // TODO: Anything else to do here?  Do we need a continuation for the log header sync?
	  return;
	} else {
	  // TODO:  What do I do here???  This should never happen because I was leader.
	  BOOST_ASSERT(false);
	}
      }

      if (configuration().is_transitional()) {
	// Log the new configuration and get rid of old servers
	std::vector<log_entry_type> v(1);
	v.back().type = log_entry_type::CONFIGURATION;
	v.back().configuration.from = configuration().description().to;
	v.back().term = current_term_;
	auto indices = log_.append(v);
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " committed transitional configuration at index " << configuration().configuration_id() << 
	  ".  Logging new stable configuration at index " << indices.first;
	// Should move from TRANSITIONAL to STABLE
	configuration_.add_logged_description(indices.first, v.back().configuration);
	BOOST_ASSERT(configuration().is_stable());
      }
    }
    
    // Based on the new commit point we may be able to complete some client requests/configuration change
    // if we are leader or we may be able to respond to the leader for some
    // append entries requests if we are follower
    auto it = client_response_continuations_.begin();
    auto e = client_response_continuations_.upper_bound(last_committed_index_);
    for(; it != e; ++it) {
      BOOST_ASSERT(it->second.index <= last_committed_index_);
      client_response resp;
      resp.id = it->second.client_request_id;
      resp.leader_id = leader_id_;
      resp.index = it->second.index;
      // If I lost leadership I can't guarantee that I committed.
      // TODO: Come up with some interesting test case here (e.g. we
      // get a quorum to ack the entry but I've lost leadership by then;
      // is it really not successful???).  Actually the answer is that the
      // commit of a log entry occurs when a majority of peers have written
      // the log entry to disk.  
      resp.result = it->second.term == current_term_ ? SUCCESS : NOT_LEADER;
      client_.on_client_response(resp);
    }
    client_response_continuations_.erase(client_response_continuations_.begin(), e);
  }

  void server::update_last_committed_index(uint64_t committed)
  {
    last_committed_index_ = committed;
    if (configuration().is_transitional_initiator() && last_committed_index_ > configuration().configuration_id()) {
      // Respond to the client that we've succeeded
      set_configuration_response_type resp;
      resp.result = SUCCESS;
      client_.on_configuration_response(resp);
    }
  }
  
  void server::on_append_response(const append_response & req)
  {
    if (req.request_term_number != current_term_) {
      BOOST_ASSERT(req.request_term_number < current_term_);
      // Discard
      return;
    }

    // I sent out an append_entries at current_term_, term hasn't changed
    // so I must be leader.
    BOOST_ASSERT(state_ == LEADER);

    if (req.term_number > current_term_) {
      if (can_become_follower_at_term(req.term_number)) {
	become_follower(req.term_number);
	// TODO: What do I do here????  Is all the processing below still relevant?
      } else {
	// This may be impossible because if we are waiting for a log header flush
	// for current_term_ then we should never have been leader at current_term_
	// and thus won't be getting a response.
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " cannot become follower at term " << req.term_number <<
	  ". Ignoring append_response message";
      }
      return;
    }
    peer_type & p(peer_from_id(req.recipient_id));
    if (req.success) {
      if (p.match_index_ > req.last_index) {
	// TODO: This is unexpected if we don't pipeline append entries message,
	// otherwise it is OK but we need more complex state around the management of
	// what entries the peer has acknowledged (have we done enough here???)
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " peer " << req.recipient_id << " acknowledged index entries [" << req.begin_index <<
	  "," << req.last_index <<
	  ") but we already had acknowledgment up to index " << p.match_index_;
      } else {
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " peer " << req.recipient_id << " acknowledged index entries [" << req.begin_index <<
	  "," << req.last_index << ")";
	p.match_index_ = req.last_index;
	// Now figure out whether we have a majority of peers ack'ing and can commit something
	try_to_commit();
      }
      // TODO: I suppose if we are pipelining append_entries to a peer then this could cause next_index_
      // to go backwards.  That wouldn't be a correctness issue merely inefficient because we'd send
      // already matched entries.  Nonetheless it is easy to prevent.
      if (p.next_index_ < p.match_index_ || !p.is_next_index_reliable_) {
	p.next_index_ = p.match_index_;
	p.is_next_index_reliable_ = true;
      }

      // A configuration change is underway, we are tracking how quickly the peers are accepting
      // log entries in order to decide whether we should allow the peers into a new config.
      if (p.configuration_change_) {
	std::chrono::time_point<std::chrono::steady_clock> clock_now = std::chrono::steady_clock::now();
	p.configuration_change_->on_append_response(clock_now, p.match_index_, last_log_entry_index());
      }
    } else {
      // Peer failed processing the append_entries request (e.g. our append request would have caused
      // a gap in the peer's log or a term mismatch on the log).  Backup the next_index_ to send to this peer.  
      if (p.next_index_ > 0) {
	p.next_index_ -= 1;
      }
      // Peer tells us about the end of its log.  In the case that we failing because of gaps this can
      // help us close the gap more quickly.  It doesn't necessarily help in case a term mismatch.
      // Things to consider:
      // How does peer.next_index_ get ahead of the peer's log???  It can happen when we become leader
      // when assume that the peer has all of our log when in fact the peer is out of date (in which case
      // is_next_index_reliable_ will be false).  TODO: Can it ever happen with is_next_index_reliable_ is true?
      // TODO: If we are pipelining append_entries then is it possible that we get an old response that causes next_index_ to
      // regress unnecessarily?  Is there anyway to detect and prevent that?
      if (p.next_index_ > req.last_index) {
	if (p.is_next_index_reliable_) {
	  BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	    " peer " << req.recipient_id << " rejected append_entries with peer log at " << req.last_index <<
	    " but leader had a reliable next index of  " << p.next_index_;
	}
	p.next_index_ = req.last_index;
      }
    }
  }

  void server::load_checkpoint()
  {
    // TODO: Actually load the checkpoint image
    checkpoint_data_ptr ckpt = checkpoint_.last_checkpoint();
    if (!ckpt) {
      // TODO: Should be a WARNING or ERROR
      return;
    }
    const checkpoint_header_type & header(ckpt->header());

    if (header.last_log_entry_index < checkpoint_.last_checkpoint_index()) {
      return;
    }

    // Update server_checkpoint 
    checkpoint_.last_checkpoint_index_ = header.last_log_entry_index;
    checkpoint_.last_checkpoint_term_ = header.last_log_entry_term;

    // Anything checkpointed must be committed so we learn of commits this way
    // TODO: What if we get asked to load a checkpoint while we still have
    // configuration().is_transitional_initiator() == true?  Should we wait until
    // after handling the log and configuration before updating the commit index?
    // I suppose it is possible that a configuration that this server initiated gets
    // committed as part of a checkpoint received after losing leadership?
    if(last_committed_index_ < checkpoint_.last_checkpoint_index_) {
      update_last_committed_index(checkpoint_.last_checkpoint_index_);
    }

    // TODO: Now get the log in order.  If my log is consistent with the checkpoint then I
    // may want to keep some of it around.  For example, if I become leader then I may need to
    // bring some peers up to date and it is likely cheaper to send them some log entries
    // rather than a full checkpoint.  If my log is shorter than the range covered by the checkpoint
    // then my log is useless (I can't bring anyone fully up to date and will have to resort to the
    // checkpoint).  If my log is at least as long the checkpoint
    // range but the term at the end of the checkpoint range doesn't match the checkpoint term
    // then I've got the wrong entries in the log and I should trash the entire log as well.
    if (last_log_entry_index() < checkpoint_.last_checkpoint_index_ ||
	(log_start_index() <= checkpoint_.last_checkpoint_index_ &&
	 log_.entry(checkpoint_.last_checkpoint_index_-1).term != checkpoint_.last_checkpoint_term_)) {
      log_.truncate_prefix(checkpoint_.last_checkpoint_index_);
      log_.truncate_suffix(checkpoint_.last_checkpoint_index_);
      configuration_.truncate_prefix(checkpoint_.last_checkpoint_index_);
      configuration_.truncate_suffix(checkpoint_.last_checkpoint_index_);
      
      // TODO: What's up with the log sync'ing logic here????
      
      // logcabin waits for the log to sync when FOLLOWER (but not when
      // LEADER).  logcabin assumes that all log writes are synchronous in a FOLLOWER
      // but I'm not sure I think that needs to be assumed (just that we sync the
      // log before responding to LEADER about append_entries).
    }

    configuration_.set_checkpoint(header.configuration);
  }

  void server::internal_append_checkpoint_chunk(const server::append_checkpoint_chunk_type & req)
  {
    BOOST_ASSERT(req.term_number == current_term_ && state_ == FOLLOWER);

    // May be the first message for this term; that's when we learn
    // who the leader actually is.
    if (leader_id_ == INVALID_PEER_ID) {
      leader_id_ = req.leader_id;
    } else {
      BOOST_ASSERT(leader_id_ == req.leader_id);
    }

    if (!checkpoint_.current_checkpoint_) {
      checkpoint_header_type header;
      header.last_log_entry_index = req.last_checkpoint_index;
      header.last_log_entry_term = req.last_checkpoint_term;
      header.configuration = req.last_checkpoint_configuration;
      checkpoint_.current_checkpoint_.reset(new in_progress_checkpoint<checkpoint_data_store_type>(checkpoint_.store_, header));
    }

    if (req.checkpoint_begin != checkpoint_.current_checkpoint_->end()) {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" received checkpoint chunk at " << req.checkpoint_begin <<
	" expecting at offset " << checkpoint_.current_checkpoint_->end() << ".  Ignoring append_checkpoint_chunk message.";
      return;
    }

    checkpoint_.current_checkpoint_->write(req.data);
    append_checkpoint_chunk_response resp;
    resp.recipient_id = my_cluster_id();
    resp.term_number = resp.request_term_number = current_term_;
    resp.bytes_stored = checkpoint_.current_checkpoint_->end();
    if (req.checkpoint_done) {
      if (req.last_checkpoint_index < checkpoint_.last_checkpoint_index_) {
	BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " received completed checkpoint at index " << req.last_checkpoint_index <<
	  " but already have a checkpoint at " << checkpoint_.last_checkpoint_index_ << ".  Ignoring entire out of date checkpoint.";
	checkpoint_.abandon();
	// TODO: Should we tell the leader that we didn't like its checkpoint?
	return;
      } else {
	checkpoint_.sync(req.leader_id, resp);
      }
    } else {
      comm_.send(req.leader_id, resp);
    }
  }

  void server::internal_append_checkpoint_chunk_sync(std::size_t leader_id, const append_checkpoint_chunk_response & resp)
  {
    if (current_term_ == resp.term_number) {
      // What happens if the current term has changed while we were waiting for sync?  Right now I am suppressing the
      // message but perhaps better to let the old leader know things have moved on.
      // TODO: Interface to load checkpoint state now that it is complete
      // TODO: Do we want an async interface for loading checkpoint state into memory?
      checkpoint_.store_.commit(checkpoint_.current_checkpoint_->file_);
      comm_.send(leader_id, resp);
      load_checkpoint();
    }
    checkpoint_.current_checkpoint_.reset();
  }
  
  void server::on_append_checkpoint_chunk(const server::append_checkpoint_chunk_type & req)
  {
    if (req.term_number < current_term_) {
      // There is a leader out there that needs to know it's been left behind.
      append_checkpoint_chunk_response msg;
      msg.recipient_id = my_cluster_id();
      msg.term_number = current_term_;
      msg.request_term_number = req.term_number;
      msg.bytes_stored = 0;
      comm_.send(req.leader_id, msg);
      return;
    }

    // Should not get an append checkpoint with current_term_ as LEADER
    // that would mean I am sending append_checkpoint messages to myself
    // or multiple leaders on the same term.
    BOOST_ASSERT(req.term_number > current_term_ || state_ != LEADER);

    // I highly doubt a disk write can be reliably cancelled so we have to wait for
    // an outstanding one to complete.  Instead of queuing up just pretend the message is lost.
    if (!can_become_follower_at_term(req.term_number)) {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" cannot advance to term " << req.term_number <<
	" due to outstanding log header sync.  Ignoring append_checkpoint_chunk message.";
      return;
    }

    // At this point doesn't really matter what state I'm currently in
    // as I'm going to become a FOLLOWER.
    become_follower(req.term_number);

    // If we were already FOLLOWER we didn't reset election timer in become_follower() so do
    // it here as well
    if (log_header_sync_required_) {
      // TODO: We need to put a limit on number of continuations we'll queue up
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" waiting for log header sync.  Queuing append_checkpoint_chunk message from recipient " <<
	req.recipient_id << " checkpoint_begin " << req.checkpoint_begin <<
	" checkpoint_end " << req.checkpoint_end;
      log_.sync_header();
      append_checkpoint_chunk_header_sync_continuations_.push_back(req);
    } else {
      election_timeout_ = new_election_timeout();
      internal_append_checkpoint_chunk(req);
    }
  }

  void server::on_append_checkpoint_chunk_response(const append_checkpoint_chunk_response & resp)
  {
    if (resp.request_term_number != current_term_) {
      BOOST_ASSERT(resp.request_term_number < current_term_);
      // Discard
      return;
    }

    // I know I sent append_checkpoint_chunk at current_term_, term hasn't changed
    // so I must be leader
    BOOST_ASSERT(state_ == LEADER);
    BOOST_ASSERT(!log_header_sync_required_);
    
    if (resp.term_number > current_term_) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" received on_append_checkpoint_chunk_response at term " << resp.term_number <<
	". Becoming FOLLOWER and abandoning the append_checkpoint process.";
      BOOST_ASSERT(can_become_follower_at_term(resp.term_number));
      if (can_become_follower_at_term(resp.term_number)) {
	become_follower(resp.term_number);
	BOOST_ASSERT(log_header_sync_required_);
	if (log_header_sync_required_) {
	  log_.sync_header();
	}
	// TODO: What to do here.  We should no longer bother sending the rest of the checkpoint, that much
	// is clear.
	peer_type & p(peer_from_id(resp.recipient_id));
	p.checkpoint_.reset();
      } else {
	// This may be impossible because if we are waiting for a log header flush
	// for current_term_ then we should never have been leader at current_term_
	// and thus won't be getting a response.
	BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	  " cannot become follower at term " << resp.term_number <<
	  ". Ignoring on_append_checkpoint_chunk_response message";
      }
      return;
    }    

    peer_type & p(peer_from_id(resp.recipient_id));

    if(!p.checkpoint_ || !p.checkpoint_->awaiting_ack_) {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" received unexpected append_checkpoint_chunk_message from peer " << resp.recipient_id <<
	". Ignoring message.";
      return;
    }

    // Peer tells us what is needed next
    p.checkpoint_->checkpoint_next_byte_ = resp.bytes_stored;

    // Send next chunk
    p.checkpoint_->awaiting_ack_ = false;
    std::chrono::time_point<std::chrono::steady_clock> clock_now = std::chrono::steady_clock::now();
    send_checkpoint_chunk(clock_now, resp.recipient_id);
  }  

  server::checkpoint_data_ptr server::begin_checkpoint(uint64_t last_index_in_checkpoint) const
  {
    // TODO: What if log_header_sync_required_==true?

    if (last_index_in_checkpoint > last_committed_index_) {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" got request for checkpoint at index " << last_index_in_checkpoint << " but current committed index is " <<
	last_committed_index_;
      return checkpoint_data_ptr();
    }

    checkpoint_header_type header;
    header.last_log_entry_index = last_index_in_checkpoint;
    if (last_index_in_checkpoint > log_start_index() &&
	last_index_in_checkpoint <= last_log_entry_index()) {
      // checkpoint index is still in the log
      const auto & e(log_.entry(last_index_in_checkpoint-1));
      header.last_log_entry_term = e.term;
    } else if (last_index_in_checkpoint == 0) {
      // Requesting an empty checkpoint, silly but OK
      header.last_log_entry_term = 0;      
    } else if (last_index_in_checkpoint == checkpoint_.last_checkpoint_index_) {
      // requesting the checkpoint we've most recently made so we've save the term
      // and don't need to look at log entries
      header.last_log_entry_term = checkpoint_.last_checkpoint_term_;
    } else {
      // we can't perform the requested checkpoint.  for example it is possible that
      // we've gotten a checkpoint from a leader that is newer than anything the client
      // is aware of yet.
      return checkpoint_data_ptr();
    }

    // Configuration state as of the checkpoint goes in the header...
    if (configuration_.has_configuration_at(last_index_in_checkpoint)) {
      configuration_.get_checkpoint_state(last_index_in_checkpoint, header.configuration);
    } else {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" got request for checkpoint at index " << last_index_in_checkpoint << " but does not have configuration at that index.";
    }
    
    return checkpoint_.store_.create(header);
  }

  bool server::complete_checkpoint(uint64_t last_index_in_checkpoint, checkpoint_data_ptr i_dont_like_this)
  {
    // TODO: What if log_header_sync_required_?
    
    if (last_index_in_checkpoint <= checkpoint_.last_checkpoint_index_) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" got request to complete checkpoint at index " << last_index_in_checkpoint << " but already has checkpoint " <<
	" at index " << checkpoint_.last_checkpoint_index_;
      return false;
    }

    // Only remaining case here is that the checkpoint index is in the log
    if (last_index_in_checkpoint <= log_start_index() ||
	last_index_in_checkpoint > last_log_entry_index()) {
      BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" got request to complete checkpoint at index " << last_index_in_checkpoint << " but log interval is [" <<
	log_start_index() << "," << last_log_entry_index() << ")";
      return false;
    }

    checkpoint_.last_checkpoint_index_ = last_index_in_checkpoint;

    // TODO: Didn't we already calculate both the last term and configuration when we started
    // the checkpoint (and created the header)?  Shouldn't we be able to get this info from the
    // header object in the checkpoint_data_ptr?
    const auto & e(log_.entry(last_index_in_checkpoint-1));
    checkpoint_.last_checkpoint_term_ = e.term;

    // Configuration stuff
    if (configuration_.has_configuration_at(last_index_in_checkpoint)) {
      // Tell configuration manager that the config in question is now checkpointed.
      configuration_checkpoint_type last_checkpoint_configuration;
      configuration_.get_checkpoint_state(last_index_in_checkpoint, last_checkpoint_configuration);
      configuration_.set_checkpoint(last_checkpoint_configuration);
    } else {
      BOOST_LOG_TRIVIAL(warning) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
	" got request for checkpoint at index " << last_index_in_checkpoint << " but does not have configuration at that index.";
    }

    // Discard log entries
    // TODO: Understand the log sync'ing implications here
    log_.truncate_prefix(checkpoint_.last_checkpoint_index_);
    configuration_.truncate_prefix(checkpoint_.last_checkpoint_index_);

    checkpoint_.store_.commit(i_dont_like_this);
    
    BOOST_LOG_TRIVIAL(info) << "Server(" << my_cluster_id() << ") at term " << current_term_ <<
      " completed checkpoint at index " << checkpoint_.last_checkpoint_index_ << " and term " <<
      checkpoint_.last_checkpoint_term_ << " current log entries [" << log_start_index() << "," <<
      last_log_entry_index() << ")";

    return true;
  }
}


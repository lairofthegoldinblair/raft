#ifndef __RAFT_STATE_MACHINE_SESSION_HH__
#define __RAFT_STATE_MACHINE_SESSION_HH__

#include <limits>
#include <map>
#include <memory>
#include <tuple>
#include <vector>

#include "boost/endian/arithmetic.hpp"
#include <boost/log/trivial.hpp>

namespace raft {
  namespace state_machine {
    template<typename _Messages>
    class client_session_state
    {
    public:
      typedef _Messages messages_type;
      typedef typename messages_type::client_response_traits_type::arg_type client_response_arg_type;
      typedef typename messages_type::linearizable_command_request_traits_type linearizable_command_request_traits_type;
      typedef typename messages_type::linearizable_command_request_traits_type::const_view_type linearizable_command_request_const_view_type;

    private:

      // For timeout of session, the last cluster time of a command we've received
      uint64_t last_command_received_cluster_time_;

      // For expiring memoized responses.   We can remove any client responses with
      // sequence number less than this value.
      uint64_t first_unacknowledged_sequence_number_;

      // Memoized responses to client requests.  sequence_number => client response.  Used to avoid at most once semantics.
      std::map<uint64_t, std::pair<raft::slice, raft::util::call_on_delete>> memo_;

      // Set first_unacknowledged_sequence_number and clean up unneeded responses.
      void first_unacknowledged_sequence_number (uint64_t val)
      {
        if (val <= first_unacknowledged_sequence_number_) {
          return;
        }
        first_unacknowledged_sequence_number_ = val;
        for(auto it = memo_.begin(); it != memo_.end(); ) {
          if (it->first < first_unacknowledged_sequence_number_) {
            it = memo_.erase(it);
          } else {
            ++it;
          }
        }
      }

    public:
      client_session_state(uint64_t cluster_time)
        :
        last_command_received_cluster_time_(cluster_time),
        first_unacknowledged_sequence_number_(0)
      {
      }
      uint64_t last_command_received_cluster_time() const
      {
        return last_command_received_cluster_time_;
      }
      bool apply(linearizable_command_request_const_view_type cmd, uint64_t cluster_time)
      {
        first_unacknowledged_sequence_number(linearizable_command_request_traits_type::first_unacknowledged_sequence_number(cmd));
        auto sequence_number = linearizable_command_request_traits_type::sequence_number(cmd);
        if (sequence_number < first_unacknowledged_sequence_number_) {
          // This command has been acknowledged a fortiori has already been applied
          return false;
        }
        auto ret = memo_.insert(std::make_pair(sequence_number, std::make_pair(raft::slice(), raft::util::call_on_delete())));
        if (ret.second) {
          // Actually apply to the state machine.   Must do this asynchronously.
          last_command_received_cluster_time_ = cluster_time;
          return true;
        } else {
          // We've already kicked off the request
          return false;
        }
      }
      void update_response(uint64_t sequence_number, std::pair<raft::slice, raft::util::call_on_delete> && response)
      {
        memo_[sequence_number] = std::move(response);
      }
      raft::slice response(uint64_t sequence_number) const
      {
        auto it = memo_.find(sequence_number);
        return it != memo_.end() ? raft::slice(reinterpret_cast<const uint8_t *>(it->second.first.data()), it->second.first.size()) : raft::slice();
      }

      static std::size_t serialize_helper(const std::pair<const uint64_t, std::unique_ptr<client_session_state>> & s);
      static std::size_t serialize_helper(raft::mutable_slice && b, const std::pair<const uint64_t, std::unique_ptr<client_session_state>> & s);
      static std::size_t deserialize_helper(raft::slice && b, std::pair<uint64_t, std::unique_ptr<client_session_state>> & s);
    };

    template<typename _Messages>
    std::size_t client_session_state<_Messages>::serialize_helper(const std::pair<const uint64_t, std::unique_ptr<client_session_state<_Messages>>> & s)
    {
      std::size_t sz = 0;
      sz += sizeof(boost::endian::little_uint64_t);
      sz += sizeof(boost::endian::little_uint64_t);
      sz += sizeof(boost::endian::little_uint64_t);
      sz += sizeof(boost::endian::little_uint64_t);
      for(auto & m : s.second->memo_) {
        sz += sizeof(boost::endian::little_uint64_t);
        sz += sizeof(boost::endian::little_uint64_t);
        sz += m.second.first.size();
      }
      return sz;
    }
    
    template<typename _Messages>
    std::size_t client_session_state<_Messages>::serialize_helper(raft::mutable_slice && b, const std::pair<const uint64_t, std::unique_ptr<client_session_state<_Messages>>> & s)
    {
      std::size_t sz = 0;
      *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = s.first;
      sz += sizeof(boost::endian::little_uint64_t);
      *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = s.second->last_command_received_cluster_time_;
      sz += sizeof(boost::endian::little_uint64_t);
      *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = s.second->first_unacknowledged_sequence_number_;
      sz += sizeof(boost::endian::little_uint64_t);
      *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = s.second->memo_.size();
      sz += sizeof(boost::endian::little_uint64_t);
      for(auto & m : s.second->memo_) {
        *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = m.first;
        sz += sizeof(boost::endian::little_uint64_t);
        *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = m.second.first.size();
        sz += sizeof(boost::endian::little_uint64_t);
        ::memcpy((b+sz).data(), m.second.first.data(), m.second.first.size());
        sz += m.second.first.size();
      }
      return sz;
    }
    
    template<typename _Messages>
    std::size_t client_session_state<_Messages>::deserialize_helper(raft::slice && b, std::pair<uint64_t, std::unique_ptr<client_session_state<_Messages>>> & s)
    {
      std::size_t sz = 0;
      s.first = *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
      sz += sizeof(boost::endian::little_uint64_t);
      s.second.reset(new client_session_state<_Messages>(0));
      s.second->last_command_received_cluster_time_ = *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
      sz += sizeof(boost::endian::little_uint64_t);
      s.second->first_unacknowledged_sequence_number_ =  *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
      sz += sizeof(boost::endian::little_uint64_t);
      std::size_t memo_sz =  *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
      sz += sizeof(boost::endian::little_uint64_t);
      for(std::size_t i=0 ; i<memo_sz; ++i) {
        uint64_t id = *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
        sz += sizeof(boost::endian::little_uint64_t);
        std::size_t buf_sz = *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
        sz += sizeof(boost::endian::little_uint64_t);
        uint8_t * buf = new uint8_t [buf_sz];        
        ::memcpy(buf, (b+sz).data(), buf_sz);
        sz += buf_sz;
        raft::util::call_on_delete deleter = [buf] () { delete [] buf; };
        s.second->memo_[id] = std::make_pair(raft::slice(buf, buf_sz), std::move(deleter));
      }
      return sz;      
    }
    
    template<typename _Messages, typename _Builders, typename _Serialization, typename _Protocol, typename _Communicator, typename _StateMachine>
    class client_session_manager
    {
    public:
      typedef _Messages messages_type;
      typedef _Builders builders_type;
      typedef _Serialization serialization_type;
      typedef _Protocol protocol_type;
      typedef _Communicator communicator_type;
      typedef _StateMachine state_machine_type;
      typedef typename communicator_type::endpoint_type endpoint_type;
      typedef typename messages_type::client_result_type client_result_type;
      typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;
      typedef typename messages_type::log_entry_traits_type log_entry_traits_type;
      typedef typename messages_type::log_entry_command_traits_type log_entry_command_traits_type;
      typedef typename messages_type::linearizable_command_request_traits_type linearizable_command_request_traits_type;
      typedef typename messages_type::linearizable_command_request_traits_type::arg_type linearizable_command_request_arg_type;
      typedef typename messages_type::client_response_traits_type client_response_traits_type;
      typedef typename builders_type::client_response_builder_type client_response_builder;
      typedef typename messages_type::open_session_request_traits_type open_session_request_traits_type;
      typedef typename messages_type::open_session_request_traits_type::arg_type open_session_request_arg_type;
      typedef typename builders_type::open_session_response_builder_type open_session_response_builder;
      typedef typename messages_type::close_session_request_traits_type close_session_request_traits_type;
      typedef typename messages_type::close_session_request_traits_type::arg_type close_session_request_arg_type;
      typedef typename builders_type::close_session_response_builder_type close_session_response_builder;
      typedef client_session_state<messages_type> client_session_type;
    private:
      protocol_type & protocol_;
      communicator_type & comm_;
      state_machine_type & state_machine_;
      // log index/session_id => (endpoint, term)
      std::map<uint64_t, std::pair<endpoint_type, uint64_t>> open_session_responses_;
      // log index => (endpoint, term)
      std::map<uint64_t, std::pair<endpoint_type, uint64_t>> close_session_responses_;
      // log index => (endpoint, term)
      std::map<uint64_t, std::pair<endpoint_type, uint64_t>> client_responses_;
      // session_id => session
      std::map<uint64_t, std::unique_ptr<client_session_type>> sessions_;      
      uint64_t session_timeout_nanos_ = 500000000000UL;
      // completion state of async state machine command application
      class command_application_state
      {
      private:
        uint64_t log_index_=std::numeric_limits<uint64_t>::max();
        uint64_t log_term_=std::numeric_limits<uint64_t>::max();
        uint64_t session_id_=std::numeric_limits<uint64_t>::max();
        uint64_t sequence_number_=std::numeric_limits<uint64_t>::max();
        size_t leader_id_=std::numeric_limits<size_t>::max();
      public:
        bool is_outstanding() const
        {
          return session_id_ < std::numeric_limits<uint64_t>::max();
        }
        uint64_t log_index() const
        {
          return log_index_;
        }
        uint64_t session_id() const
        {
          return session_id_;
        }
        uint64_t sequence_number() const
        {
          return sequence_number_;
        }
        client_response_builder & response(client_response_builder & bld, uint64_t expected_term) const
        {
          return bld.result(log_term_ == expected_term ? messages_type::client_result_success() : messages_type::client_result_not_leader()).index(log_index_).leader_id(leader_id_);
        }
        void begin(uint64_t _log_index, uint64_t _log_term, uint64_t _session_id, size_t _leader_id, uint64_t _sequence_number)
        {
          BOOST_ASSERT(_session_id < std::numeric_limits<uint64_t>::max());
          BOOST_ASSERT(!is_outstanding());
          log_index_ = _log_index;
          log_term_ = _log_term;
          session_id_ = _session_id;
          leader_id_ = _leader_id;
          sequence_number_ = _sequence_number;
        }
        void finish()
        {
          log_index_ = std::numeric_limits<uint64_t>::max();
          log_term_ = std::numeric_limits<uint64_t>::max();
          session_id_ = std::numeric_limits<uint64_t>::max();
          leader_id_ = std::numeric_limits<size_t>::max();
          sequence_number_ = std::numeric_limits<size_t>::max();
        }
      };
      command_application_state command_application_;
      std::vector<uint8_t> checkpoint_buffer_;
      bool checkpoint_in_progress_ = false;

      void expire_sessions(uint64_t cluster_time)
      {
        for(auto it = sessions_.begin(); it != sessions_.end(); ) {
          uint64_t expired = session_timeout_nanos_ < cluster_time ? cluster_time - session_timeout_nanos_ : 0U;
          if (it->second->last_command_received_cluster_time() < expired &&
              (!command_application_.is_outstanding() || command_application_.session_id() != it->first)) {
            BOOST_LOG_TRIVIAL(info) << "[session_manager::expire_sessions] Expiring session with id " << it->first
                                    << " last command received " << std::chrono::duration_cast<std::chrono::seconds>(std::chrono::nanoseconds(cluster_time - it->second->last_command_received_cluster_time())).count()
                                    << " seconds ago.";
            it = sessions_.erase(it);
          } else {
            ++it;
          }
        }
      }

      // TODO: Send response
      void complete_command(bool ret, std::pair<raft::slice, raft::util::call_on_delete> && response)
      {
        auto session_id = command_application_.session_id();
        auto sess_it = sessions_.find(session_id);
        auto log_index = command_application_.log_index();
        if (sessions_.end() != sess_it) {
          sess_it->second->update_response(command_application_.sequence_number(), std::move(response));
          auto it = client_responses_.find(log_index);
          if (client_responses_.end() != it) {
            client_response_builder bld;
            auto resp = command_application_.response(bld, it->second.second).response(sess_it->second->response(command_application_.sequence_number())).finish();
            BOOST_LOG_TRIVIAL(info) << "[session_manager::complete_command] Session(" << session_id << ")"
                                    << " sending client response log entry " << log_index << " of type COMMAND due to completed state machine application: { "
                                    << "\"client_result\" : \"" << client_result_string(client_response_traits_type::result(resp)) << "\""
                                    << ", \"log_index\" : " << client_response_traits_type::index(resp)
                                    << ", \"leader_id\" : " << client_response_traits_type::leader_id(resp)
                                    << " }";
            comm_.send_client_response(std::move(resp), it->second.first);
            client_responses_.erase(it);
          }
        }
        command_application_.finish();
        // It's important to wait until after the command application is finished before
        // responding to the protocol, as the protocol may call back into the session with the
        // next log entry to be applied to the state machine.   If the command application isn't
        // finished, that log entry will bounce off of us and have to be retried.
        BOOST_LOG_TRIVIAL(info) << "[session_manager::complete_command] Completing application of log entry " << log_index << " of type COMMAND due to completed state machine application";
        protocol_.on_command_applied(log_index);
        // TODO: Think about retry logic.  Deterministic failures should be OK and will result in
        // the same state everywhere but non-deterministic failures have to be retried until success or
        // deterministic failure.
        BOOST_ASSERT(ret);
      }

      const char * client_result_string(client_result_type cr)
      {
        if (messages_type::client_result_success() == cr) {
          return "SUCCESS";
        } else if (messages_type::client_result_fail() == cr) {
          return "FAIL";
        } else if (messages_type::client_result_retry() == cr) {
          return "RETRY";
        } else if (messages_type::client_result_not_leader() == cr) {
          return "NOT_LEADER";
        } else if (messages_type::client_result_session_expired() == cr) {
          return "SESSION_EXPIRED";
        } else {
          return "UNKNOWN";
        }
      }
    public:
      void apply(log_entry_const_arg_type le, uint64_t idx, size_t leader_id)
      {
        auto cluster_time = log_entry_traits_type::cluster_time(le);
        // Expire sessions as needed
        expire_sessions(cluster_time);
        // Can't have multiple commands being applied concurrently, use the log as a buffer.
        // Technically we could probably allow sessions to be opened or closed while a state machine command
        // but I don't think it is worth the risk.
        if (command_application_.is_outstanding()) {
          BOOST_LOG_TRIVIAL(debug) << "[session_manager::apply] Log entry at " << idx << " cannot be applied because command is oustanding.";
          return;
        }
        if (checkpoint_in_progress_) {
          BOOST_LOG_TRIVIAL(debug) << "[session_manager::apply] Log entry at " << idx << " cannot be applied because checkpoint is in progress.";
          return;
        }
        auto log_term = log_entry_traits_type::term(le);
        if (log_entry_traits_type::is_command(le)) {
          typename serialization_type::log_entry_command_view_deserialization_type cmd_view(le);
          if (log_entry_command_traits_type::is_linearizable_command(cmd_view.view())) {
              auto lcmd = log_entry_command_traits_type::linearizable_command(cmd_view.view());
              auto session_id = linearizable_command_request_traits_type::session_id(lcmd);
              auto sequence_number = linearizable_command_request_traits_type::sequence_number(lcmd);
              auto sess_it = sessions_.find(session_id);
              if (sessions_.end() != sess_it) {
                // Check if already applied?
                bool should_apply = sess_it->second->apply(lcmd, cluster_time);
                if (should_apply) {
                  BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") initiating state machine application of log entry " << idx << " of type COMMAND";
                  command_application_.begin(idx, log_term, session_id, leader_id, sequence_number);
                  state_machine_.on_command(std::move(cmd_view), [this] (bool ret, std::pair<raft::slice, raft::util::call_on_delete> && response) { this->complete_command(ret, std::move(response)); });
                  return;
                } else {
                  auto it = client_responses_.find(idx);
                  BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") completing application of log entry " << idx << " of type COMMAND due to memoized response";;
                  protocol_.on_command_applied(idx);
                  if (client_responses_.end() != it) {
                    auto to_return = it->second.second == log_term ? messages_type::client_result_success() : messages_type::client_result_not_leader();
                    BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") sending client response log entry " << idx << " of type COMMAND due to memoized response: { "
                                            << "\"client_result\" : \"" << client_result_string(to_return) << "\""
                                            << ", \"log_index\" : " << idx
                                            << ", \"leader_id\" : " << leader_id
                                            << " }";
                    auto resp = client_response_builder().result(to_return).index(idx).leader_id(leader_id).response(sess_it->second->response(sequence_number)).finish();
                    comm_.send_client_response(std::move(resp), it->second.first);
                    client_responses_.erase(it);
                  }
                }
              } else {
                auto it = client_responses_.find(idx);
                BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") completing application of log entry " << idx << " of type COMMAND due to expired session";
                protocol_.on_command_applied(idx);
                if (client_responses_.end() != it) {
                  BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Session(" << session_id << ") sending client response log entry " << idx << " of type COMMAND due to expired session: { "
                                          << "\"client_result\" : \"" << client_result_string(messages_type::client_result_session_expired()) << "\""
                                          << ", \"log_index\" : " << idx
                                          << ", \"leader_id\" : " << leader_id
                                          << " }";
                  auto resp = client_response_builder().result(messages_type::client_result_session_expired()).index(idx).leader_id(leader_id).finish();
                  comm_.send_client_response(std::move(resp), it->second.first);
                  client_responses_.erase(it);
                }
              }

          } else if (log_entry_command_traits_type::is_open_session(cmd_view.view())) {
            // Use the log entry index as the session id and use that to determine where to send
            // the response if we need to.
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Opening session with id " << idx;
            sessions_[idx] = std::make_unique<client_session_type>(cluster_time);
            auto it = open_session_responses_.find(idx);
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type COMMAND opening session with id " << idx;
            protocol_.on_command_applied(idx);
            // TODO: Send not_leader if commit entry at index has different term
            if (open_session_responses_.end() != it) {
              if (it->second.second == log_term) {
                BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Sending open session response for session with id " << idx << " to endpoint " << it->second.first;
                auto resp = open_session_response_builder().session_id(idx).finish();
                comm_.send_open_session_response(std::move(resp), it->second.first);
              }
              open_session_responses_.erase(it);
            }
          } else if (log_entry_command_traits_type::is_close_session(cmd_view.view())) {
            auto close = log_entry_command_traits_type::close_session(cmd_view.view());
            auto session_id = close_session_request_traits_type::session_id(close);
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Closing session with id " << session_id;
            sessions_.erase(session_id);
            auto it = close_session_responses_.find(idx);
            BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type COMMAND closing session " << session_id;
            protocol_.on_command_applied(idx);
            // TODO: Send not_leader if commit entry at index has different term
            if (close_session_responses_.end() != it) {
              if (it->second.second == log_term) {                
                BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Sending close session response for session with id " << session_id << " to endpoint " << it->second.first;
                auto resp = close_session_response_builder().finish();
                comm_.send_close_session_response(std::move(resp), it->second.first);
              }
              close_session_responses_.erase(it);
            }
          }
        } else if (log_entry_traits_type::is_configuration(le)) {
          BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type CONFIGURATION";
          protocol_.on_command_applied(idx);
        } else if (log_entry_traits_type::is_noop(le)) {
          BOOST_LOG_TRIVIAL(info) << "[session_manager::apply] Completing application of log entry " << idx << " of type NOOP";
          protocol_.on_command_applied(idx);
        } else {
          BOOST_ASSERT(false);
        }
      }

      void on_open_session(const endpoint_type & ep,
                           open_session_request_arg_type && req,
                           std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        BOOST_LOG_TRIVIAL(debug) << "[session_manager::on_open_session] Request to open session";
        auto buf = serialization_type::serialize_log_entry_command(std::move(req));
        auto ret = protocol_.on_command(std::move(buf), clock_now);
        if (messages_type::client_result_success() == std::get<0>(ret)) {
          open_session_responses_[std::get<1>(ret)] = std::make_pair(ep, std::get<2>(ret));
        } else {
          auto resp = open_session_response_builder().session_id(0).finish();
          comm_.send_open_session_response(std::move(resp), ep);
        }
      }

      void on_close_session(const endpoint_type & ep,
                            close_session_request_arg_type && req,
                            std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        BOOST_LOG_TRIVIAL(debug) << "[session_manager::on_close_session] Request to close session with id " << close_session_request_traits_type::session_id(req);
        auto buf = serialization_type::serialize_log_entry_command(std::move(req));
        auto ret = protocol_.on_command(std::move(buf), clock_now);
        if (messages_type::client_result_success() == std::get<0>(ret)) {
          close_session_responses_[std::get<1>(ret)] = std::make_pair(ep, std::get<2>(ret));
        } else {
          auto resp = close_session_response_builder().finish();
          comm_.send_close_session_response(std::move(resp), ep);
        }
      }

      void on_linearizable_command(const endpoint_type & ep,
                                   linearizable_command_request_arg_type && req,
                                   std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        BOOST_LOG_TRIVIAL(debug) << "[session_manager::on_linearizable_command] Request to invoke linearizable command on session with id " << linearizable_command_request_traits_type::session_id(req)
                                 << " sequence number " << linearizable_command_request_traits_type::session_id(req);
        auto buf = serialization_type::serialize_log_entry_command(std::move(req));
        auto ret = protocol_.on_command(std::move(buf), clock_now);
        if (messages_type::client_result_success() == std::get<0>(ret)) {
          client_responses_[std::get<1>(ret)] = std::make_pair(ep, std::get<2>(ret));
        } else {
          auto resp = client_response_builder().result(std::get<0>(ret)).index(0).leader_id(protocol_.leader_id()).finish();
          comm_.send_client_response(std::move(resp), ep);
        }
      }

      void on_protocol_state_change(typename protocol_type::state s, uint64_t current_term)
      {
        BOOST_ASSERT(s == protocol_.get_state());
        BOOST_ASSERT(current_term == protocol_.current_term());

        // Cancel all outstanding requests
        if (s != protocol_type::LEADER) {
          for (auto & r : open_session_responses_) {
            BOOST_LOG_TRIVIAL(info) << "[session_manager::on_protocol_state_change] Protocol lost leadership, responding to open_session request"
                                    << " at index " << r.first << " to endpoint " << r.second.first;
            auto resp = open_session_response_builder().session_id(0).finish();
            comm_.send_open_session_response(std::move(resp), r.second.first);
          }
          for (auto & r : close_session_responses_) {
            BOOST_LOG_TRIVIAL(info) << "[session_manager::on_protocol_state_change] Protocol lost leadership, responding to close_session request"
                                    << " at index " << r.first << " to endpoint " << r.second.first;
            auto resp = close_session_response_builder().finish();
            comm_.send_close_session_response(std::move(resp), r.second.first);
          }
          for (auto & r : client_responses_) {
            BOOST_LOG_TRIVIAL(info) << "[session_manager::on_protocol_state_change] Protocol lost leadership, responding to send_command request"
                                    << " at index " << r.first << " to endpoint " << r.second.first;
            auto resp = client_response_builder().result(messages_type::client_result_not_leader()).index(0).leader_id(protocol_.leader_id()).finish();
            comm_.send_client_response(std::move(resp), r.second.first);
          }
        }
      }
      
      // This is a blocking implementation of checkpointing
      void on_checkpoint_request(std::chrono::time_point<std::chrono::steady_clock> clock_now)
      {
        // TODO: We need to wait for any state machine applications to complete.
        // Block processing of any state machine commands
        checkpoint_in_progress_ = true;
        auto ckpt = protocol_.begin_checkpoint(protocol_.applied_index());
        // TODO: Externalize the serialization of session checkpoint state.
        auto sz = serialize_helper(*this);
        // Make it easier to deserialize safely by writing the size
        boost::endian::little_uint64_t little_sz = sz;
        ckpt->write(reinterpret_cast<const uint8_t *>(&little_sz), sizeof(boost::endian::little_uint64_t));
        std::vector<uint8_t> tmp(sz);
        serialize_helper(raft::mutable_slice(&tmp[0], sz), *this);
        ckpt->write(&tmp[0], sz);
        // Synchronous call to state machine to checkpoint.
        state_machine_.checkpoint(ckpt);
        protocol_.complete_checkpoint(ckpt, clock_now);
        checkpoint_in_progress_ = false;
      }

      std::size_t checkpoint_session_buffer_size() const
      {
        return checkpoint_buffer_.size() >= sizeof(boost::endian::little_uint64_t) ?
          *reinterpret_cast<const boost::endian::little_uint64_t *>(&checkpoint_buffer_[0]) + sizeof(boost::endian::little_uint64_t) :
          std::numeric_limits<std::size_t>::max();
      }

      bool checkpoint_buffer_has_all_session_data() const
      {
        return checkpoint_session_buffer_size() <= checkpoint_buffer_.size();
      }

      void restore_checkpoint_block(raft::slice && b, bool is_final)
      {
        if (nullptr == b.data()) {
          sessions_.clear();
          if (1024 > checkpoint_buffer_.capacity()) {
            checkpoint_buffer_.reserve(1024);
          }
          checkpoint_buffer_.resize(0);
          // Clear the state machine
          state_machine_.restore_checkpoint_block(raft::slice(nullptr, 0), false);
        } else {
          bool sessions_restored = checkpoint_buffer_has_all_session_data();
          if (!sessions_restored) {
            auto begin = reinterpret_cast<const uint8_t *>(b.data());
            auto end = begin + b.size();
            checkpoint_buffer_.insert(checkpoint_buffer_.end(), begin, end);
            if (checkpoint_buffer_has_all_session_data()) {
              std::size_t sz = *reinterpret_cast<const boost::endian::little_uint64_t *>(&checkpoint_buffer_[0]);
              deserialize_helper(raft::slice(&checkpoint_buffer_[sizeof(boost::endian::little_uint64_t)], sz), *this);
              if (checkpoint_buffer_.size() > checkpoint_session_buffer_size()) {
                // Send the remainder of to the state machine
                state_machine_.restore_checkpoint_block(raft::slice(&checkpoint_buffer_[checkpoint_session_buffer_size()], checkpoint_buffer_.size() - checkpoint_session_buffer_size()), is_final);
              }
            }
          } else {
            // Session state is restored everything else is bound for the state machine
            // If this is the first data to be sent to the state machine clear it first
            state_machine_.restore_checkpoint_block(std::move(b), is_final);
          }
        }
      }

      client_session_manager(protocol_type & protocol, communicator_type & comm, state_machine_type & state_machine)
        :
        protocol_(protocol),
        comm_(comm),
        state_machine_(state_machine)
      {
      }

      uint64_t session_timeout_nanos() const
      {
        return session_timeout_nanos_;
      }

      std::size_t size() const
      {
        return sessions_.size();
      }

      static std::size_t serialize_helper(const client_session_manager & s)
      {
        std::size_t sz = 0;
        sz += sizeof(boost::endian::little_uint64_t);
        for(const std::pair<const uint64_t, std::unique_ptr<client_session_type>> & s : s.sessions_) {
          sz += client_session_type::serialize_helper(s);
        }
        return sz;
      }
      
      static std::size_t serialize_helper(raft::mutable_slice && b, const client_session_manager & s)
      {
        std::size_t sz = 0;
        if (nullptr != b.data()) {
          *reinterpret_cast<boost::endian::little_uint64_t *>((b+sz).data()) = s.sessions_.size();
        }
        sz += sizeof(boost::endian::little_uint64_t);
        for(const std::pair<const uint64_t, std::unique_ptr<client_session_type>> & s : s.sessions_) {
          sz += client_session_type::serialize_helper((b+sz), s);
        }
        return sz;
      }
      
      static std::size_t deserialize_helper(raft::slice && b, client_session_manager & s)
      {
        std::size_t sz = 0;
        std::size_t num_sessions = *reinterpret_cast<const boost::endian::little_uint64_t *>((b+sz).data());
        sz += sizeof(boost::endian::little_uint64_t);
        for(std::size_t i=0; i<num_sessions; ++i) {
          std::pair<uint64_t, std::unique_ptr<client_session_type>> tmp;
          sz += client_session_type::deserialize_helper((b+sz), tmp);
          s.sessions_.insert(std::move(tmp));
        }
        return sz;
      }
    };
  }
}
  
#endif

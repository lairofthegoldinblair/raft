#ifndef __RAFT_JSON_HH__
#define __RAFT_JSON_HH__

#include <iomanip>
#include <sstream>
#include <string>

namespace raft {
  namespace util {
    template<typename _Messages>
    class json
    {
    public:
      typedef _Messages messages_type;

      // Log types
      typedef typename messages_type::log_entry_type log_entry_type;
      typedef typename messages_type::log_entry_traits_type log_entry_traits_type;
      typedef typename messages_type::log_entry_traits_type::const_arg_type log_entry_const_arg_type;

      // Message argument types and traits for looking at them.    We don't have concrete/value types
      // so we can not call c'tors/d'tors etc.   That is quite intentional.
      typedef typename messages_type::client_result_type client_result_type;
      typedef typename messages_type::vote_request_traits_type vote_request_traits_type;
      typedef typename messages_type::vote_request_traits_type::arg_type vote_request_arg_type;
      typedef typename messages_type::vote_response_traits_type vote_response_traits_type;
      typedef typename messages_type::vote_response_traits_type::arg_type vote_response_arg_type;
      typedef typename messages_type::append_checkpoint_chunk_request_traits_type append_checkpoint_chunk_request_traits_type;
      typedef typename messages_type::append_checkpoint_chunk_request_traits_type::arg_type append_checkpoint_chunk_request_arg_type;
      typedef typename messages_type::append_checkpoint_chunk_response_traits_type append_checkpoint_chunk_response_traits_type;
      typedef typename messages_type::append_checkpoint_chunk_response_traits_type::arg_type append_checkpoint_chunk_response_arg_type;
      typedef typename messages_type::append_entry_request_traits_type append_entry_request_traits_type;
      typedef typename messages_type::append_entry_request_traits_type::arg_type append_entry_request_arg_type;
      typedef typename messages_type::append_entry_response_traits_type append_entry_response_traits_type;
      typedef typename messages_type::append_entry_response_traits_type::arg_type append_entry_response_arg_type;
      typedef typename messages_type::set_configuration_request_traits_type set_configuration_request_traits_type;
      typedef typename messages_type::set_configuration_request_traits_type::arg_type set_configuration_request_arg_type;
      typedef typename messages_type::get_configuration_request_traits_type get_configuration_request_traits_type;
      typedef typename messages_type::get_configuration_request_traits_type::arg_type get_configuration_request_arg_type;


      typedef typename messages_type::checkpoint_header_traits_type checkpoint_header_traits_type;
      typedef typename messages_type::configuration_description_traits_type configuration_description_traits_type;
      typedef typename messages_type::simple_configuration_description_traits_type simple_configuration_description_traits_type;
      typedef typename messages_type::server_description_traits_type server_description_traits_type;

      static std::string simple_configuration_description(const typename messages_type::simple_configuration_description_type & header)
      {
        std::stringstream str;
        str << "[ ";
        for(std::size_t i=0; i<simple_configuration_description_traits_type::size(&header); ++i) {
          if (i > 0) {
            str << ", ";
          }
          str << "{ "
              << "\"id\": " << server_description_traits_type::id(&simple_configuration_description_traits_type::get(&header, i))
              << ", \"address\": \"" << server_description_traits_type::address(&simple_configuration_description_traits_type::get(&header, i)) << "\""
              << " }";
        }
        str << " ]";

        return str.str();
      }
    
      static std::string configuration_description(const typename messages_type::configuration_description_type & header)
      {
        std::stringstream str;
        str << "{ "
            << "\"from\": " << simple_configuration_description(configuration_description_traits_type::from(&header))
            << ", \"to\": " << simple_configuration_description(configuration_description_traits_type::to(&header))
            << " }";

        return str.str();
      }
    
      static std::string checkpoint_header(const typename messages_type::checkpoint_header_type & header)
      {
        std::stringstream str;
        str << "{ "
            << "\"index_end\": " << checkpoint_header_traits_type::log_entry_index_end(&header)
            << ", \"last_term\": " << checkpoint_header_traits_type::last_log_entry_term(&header)
            << ", \"last_cluster_time\": " << checkpoint_header_traits_type::last_log_entry_cluster_time(&header)
            << ", \"configuration\": " << configuration_description(checkpoint_header_traits_type::configuration(&header))
            << " }";

        return str.str();
      }

      static std::string append_checkpoint_chunk_request(const append_checkpoint_chunk_request_arg_type & msg)
      {
        std::stringstream str;
        str << "{ "
            << "\"request_id\": " << append_checkpoint_chunk_request_traits_type::request_id(msg)
            << ", \"recipient_id\": " << append_checkpoint_chunk_request_traits_type::recipient_id(msg)
            << ", \"term_number\": " << append_checkpoint_chunk_request_traits_type::term_number(msg)
            << ", \"leader_id\": " << append_checkpoint_chunk_request_traits_type::leader_id(msg)
            << ", \"checkpoint_index_end\": " << append_checkpoint_chunk_request_traits_type::checkpoint_index_end(msg)
            << ", \"last_checkpoint_term\": " << append_checkpoint_chunk_request_traits_type::last_checkpoint_term(msg)
            << ", \"last_checkpoint_cluster_time\": " << append_checkpoint_chunk_request_traits_type::last_checkpoint_cluster_time(msg)
            << ", \"checkpoint_begin\": " << append_checkpoint_chunk_request_traits_type::checkpoint_begin(msg)
            << ", \"checkpoint_end\": " << append_checkpoint_chunk_request_traits_type::checkpoint_end(msg)
            << ", \"last_checkpoint_header\": " << checkpoint_header(append_checkpoint_chunk_request_traits_type::last_checkpoint_header(msg));
        
        auto s = append_checkpoint_chunk_request_traits_type::data(msg);
        str << std::hex << std::setfill('0') << std::setw(2);
        auto sz = 16;
        const uint8_t * buf = reinterpret_cast<const uint8_t *>(s.data());
        if (s.size() <= 16) {
          str << ", \"data\": ";
          sz = s.size();
        } else {
          str << ", \"data_prefix\": ";
        }
        str << "\"";
        if (sz > 0) {
          str << "0x";
          for(std::size_t i=0; i<sz; ++i) {
            str << (uint32_t) buf[i];
          }
        }
        str << "\"";
        str << " }";

        return str.str();
      }

      static std::string set_configuration_request(const set_configuration_request_arg_type & msg)
      {
        std::stringstream str;
        str << "{ "
            << "\"old_id\": " << set_configuration_request_traits_type::old_id(msg)
            << ", \"new_configuration\": " << simple_configuration_description(set_configuration_request_traits_type::new_configuration(msg))
            << " }";

        return str.str();
      }

      static std::string log_entry(const log_entry_type & entry)
      {
        std::stringstream str;
        str << "{ "
            << "\"term\": " << log_entry_traits_type::term(&entry)
            << ", \"cluster_time\": " << log_entry_traits_type::cluster_time(&entry)
          ;
        if (log_entry_traits_type::is_configuration(&entry)) {
          str << ", \"configuration\": " << configuration_description(log_entry_traits_type::configuration(&entry));
        } else if (log_entry_traits_type::is_command(&entry)) {
          auto s = log_entry_traits_type::data(&entry);
          str << std::hex << std::setfill('0') << std::setw(2);
          auto sz = 16;
          const uint8_t * buf = reinterpret_cast<const uint8_t *>(s.data());
          if (s.size() <= 16) {
            str << ", \"command\": ";
            sz = s.size();
          } else {
            str << ", \"command_prefix\": ";
          }
          str << "\"";
          if (sz > 0) {
            str << "0x";
            for(std::size_t i=0; i<sz; ++i) {
              str << (uint32_t) buf[i];
            }
          }
          str << "\"";
        }
        str << " }";

        return str.str();
      }    
    };
  }
}
#endif

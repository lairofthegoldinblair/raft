#ifndef RAFT_POSIX_FILE_HH
#define RAFT_POSIX_FILE_HH

#include <stdio.h>

namespace raft {
  namespace posix {
    class writable_file
    {
    private:
      FILE * file_;
    public:
      writable_file(const std::string& fname)
      {
	file_ = ::fopen(fname.c_str(), "w");
      }

      ~writable_file()
      {
	::fclose(file_);
      }

      int write(const uint8_t * data, std::size_t len)
      {
	std::size_t written = ::fwrite(data, 1, len, file_);
	if (written != len) {
	  return errno;
	}
	return 0;
      }

      void flush()
      {
	::fflush(file_);
      }
    };
  }
}

#endif

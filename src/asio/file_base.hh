#ifndef __FILE_BASE_HH__
#define __FILE_BASE_HH__

#include <unistd.h>

struct file_base
{
  enum seek_type { seek_set=SEEK_SET, seek_cur=SEEK_CUR, seek_end=SEEK_END };
};

#endif

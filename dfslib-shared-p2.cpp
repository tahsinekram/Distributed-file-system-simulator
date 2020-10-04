#include <string>
#include <iostream>
#include <fstream>
#include <cstddef>
#include <sys/stat.h>

#include "dfslib-shared-p2.h"
#include "proto-src/dfs-service.grpc.pb.h"

// Global log level used throughout the system
// Note: this may be adjusted from the CLI in
// both the client and server executables.
// This shouldn't be changed at the file level.
dfs_log_level_e DFS_LOG_LEVEL = LL_ERROR;


__precompile__(true)
module FoundationDB

import Base: show, open, close, reset, isopen, getkey
export FDBCluster, FDBDatabase, FDBTransaction, FDBError, FDBKeySel, FDBFuture, start_client, stop_client, is_client_running
export FDBNetworkOption, FDBDatabaseOption, FDBTransactionOption, FDBMutationType, FDBStreamingMode
export reset, cancel, commit, get_read_version, set_read_version, get_committed_version
export clearkey, clearkeyrange, getval, setval, watchkey, keysel, getkey, getrange
export atomic, atomic_add, atomic_and, atomic_or, atomic_xor, atomic_max, atomic_min

include("capi/capi.jl")
include("base.jl")

end # module

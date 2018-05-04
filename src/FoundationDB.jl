module FoundationDB

import Base: show, open, close, reset, isopen
export FDBCluster, FDBDatabase, FDBTransaction, FDBError, start_client, stop_client, is_client_running
export reset, cancel, commit, get_read_version, set_read_version, get_committed_version
export setkey, clearkey, clearkeyrange, getval, setval, copyval

include("capi/capi.jl")
include("base.jl")

end # module

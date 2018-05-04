using FoundationDB

@static if VERSION < v"0.7.0-DEV.2005"
    using Base.Test
else
    using Test
end

@testset "start network" begin
    @test !is_client_running()
    @test start_client() === nothing
    @test is_client_running()
end

try
    @testset "basic julia apis" begin
        chk_closed = Vector{Any}()

        open(FDBCluster()) do cluster
            push!(chk_closed, cluster)
            @test cluster.ptr !== C_NULL

            open(FDBDatabase(cluster)) do db
                push!(chk_closed, db)
                @test db.name == "DB"
                @test db.ptr !== C_NULL

                key = UInt8[0,1,2]
                val = UInt8[9, 9, 9]

                open(FDBTransaction(db)) do tran
                    push!(chk_closed, tran)
                    @test tran.ptr !== C_NULL

                    @test getval(tran, key) == nothing
                    @test setval(tran, key, val) == nothing
                    @test getval(tran, key) == val
                    commit(tran)
                end

                open(FDBTransaction(db)) do tran
                    push!(chk_closed, tran)
                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
                    commit(tran)
                end

                open(FDBTransaction(db)) do tran
                    push!(chk_closed, tran)
                    @test getval(tran, key) == nothing
                end
            end
        end

        for item in chk_closed
            @test !isopen(item)
        end
    end

    @testset "parallel updates" begin
        sumval = 0
        function do_updates()
            open(FDBCluster()) do cluster
                open(FDBDatabase(cluster)) do db
                    key = UInt8[0,1,2]
                    valarr = Int[1]
                    val = unsafe_wrap(Array, convert(Ptr{UInt8}, pointer(valarr)), sizeof(Int))
                    for valint in 1:100
                        valarr[1] = valint
                        open(FDBTransaction(db)) do tran
                            @test setval(tran, key, val) == nothing
                            @test commit(tran) == nothing
                        end
                        open(FDBTransaction(db)) do tran
                            @test clearkey(tran, key) == nothing
                            @test commit(tran) == nothing
                        end
                        sleep(rand()/100)
                        open(FDBTransaction(db)) do tran
                            valnow = getval(tran, key)
                            if valnow != nothing
                                sumval += reinterpret(Int, valnow)[1]
                            end
                        end
                    end
                end
            end
        end

        @sync begin
            @async do_updates()
            @async do_updates()
        end

        println("sumval in parallel updates = ", sumval)
        @test 0 <= sumval <= (5050 * 1.5) # series sum of 1:100 = 5050
    end

    @testset "stop network" begin
        @test is_client_running()
        @test stop_client() === nothing
        @test !is_client_running()
        @test_throws Exception start_client()
    end
finally
    stop_client()
end

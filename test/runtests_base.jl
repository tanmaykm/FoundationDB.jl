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

                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
                    @test setval(tran, key, val) == nothing
                    @test getval(tran, key) == val
                end

                open(FDBTransaction(db)) do tran
                    push!(chk_closed, tran)
                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
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

    @testset "auto commit" begin
        open(FDBCluster()) do cluster
            open(FDBDatabase(cluster)) do db
                key = UInt8[0,1,2]
                val = UInt8[9, 9, 9]
                open(FDBTransaction(db)) do tran
                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
                    @test setval(tran, key, val) == nothing
                    @test getval(tran, key) == val
                    @test commit(tran)
                    @test_throws FDBError commit(tran)
                end

                open(FDBTransaction(db)) do tran
                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
                    @test commit(tran)
                end

                open(FDBTransaction(db)) do tran
                    @test getval(tran, key) == nothing
                    @test commit(tran)
                end
            end
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
                        end
                        open(FDBTransaction(db)) do tran
                            @test clearkey(tran, key) == nothing
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

    @testset "large key value" begin
        open(FDBCluster()) do cluster
            open(FDBDatabase(cluster)) do db
                key = ones(UInt8, 10000)
                val = ones(UInt8, 100000)
                open(FDBTransaction(db)) do tran
                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
                    @test setval(tran, key, val) == nothing
                    @test getval(tran, key) == val
                    @test commit(tran)
                    @test reset(tran) == nothing
                    @test commit(tran)              # test that commit is allowed after a reset
                end

                open(FDBTransaction(db)) do tran
                    @test clearkey(tran, key) == nothing
                    @test getval(tran, key) == nothing
                end

                open(FDBTransaction(db)) do tran
                    @test getval(tran, key) == nothing
                    @test reset(tran) == nothing
                end
            end
        end
    end

    @testset "watch" begin
        open(FDBCluster()) do cluster
            open(FDBDatabase(cluster)) do db
                key = UInt8[0,1,2]
                val1 = UInt8[0, 0, 0]
                val2 = UInt8[0, 0, 0]
                open(FDBTransaction(db)) do tran
                    twatch = 0.0
                    watchtask = watchkey(tran, key)
                    timetask = @schedule begin
                        t1 = time()
                        wait(watchtask)
                        twatch = time() - t1
                    end
                    sleep(0.5)
                    @test setval(tran, key, val2) == nothing
                    sleep(0.5)
                    @test clearkey(tran, key) == nothing
                    wait(timetask)
                    @test twatch > 0.4
                end
            end
        end

        open(FDBCluster()) do cluster
            open(FDBDatabase(cluster)) do db
                key = UInt8[0,1,2]
                val1 = UInt8[0, 0, 0]
                val2 = UInt8[0, 0, 0]

                # set an initial value
                open(FDBTransaction(db)) do tran
                    @test setval(tran, key, val1) == nothing
                end

                twatch = time()
                watchtask = nothing
                # start a watch
                open(FDBTransaction(db)) do tran
                    watchtask = watchkey(tran, key)
                end

                timetask = @schedule begin
                    t1 = time()
                    wait(watchtask)
                    twatch = time() - t1
                end

                open(FDBTransaction(db)) do tran
                    sleep(0.5)
                    @test setval(tran, key, val2) == nothing
                    sleep(0.5)
                    @test clearkey(tran, key) == nothing
                end

                wait(timetask)
                @test twatch > 0.4
            end
        end
    end

    @testset "get key" begin
        keys = [UInt8[0,1,x] for x in 1:20]
        val = UInt8[0]

        open(FDBCluster()) do cluster
            open(FDBDatabase(cluster)) do db
                # setup all keys
                open(FDBTransaction(db)) do tran
                    for key in keys
                        @test setval(tran, key, val) == nothing
                    end
                end

                # get
                open(FDBTransaction(db)) do tran
                    emptykey = UInt8[]
                    key = UInt8[0,0,0]
                    @test getkey(tran, keysel(FDBKeySel.last_less_or_equal, key)) == emptykey
                    @test getkey(tran, keysel(FDBKeySel.last_less_than, key)) == emptykey
                    @test getkey(tran, keysel(FDBKeySel.first_greater_than, key)) == UInt8[0,1,1]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_or_equal, key)) == UInt8[0,1,1]

                    key = UInt8[0,1,1]
                    @test getkey(tran, keysel(FDBKeySel.last_less_or_equal, key)) == UInt8[0,1,1]
                    @test getkey(tran, keysel(FDBKeySel.last_less_than, key)) == emptykey
                    @test getkey(tran, keysel(FDBKeySel.first_greater_than, key)) == UInt8[0,1,2]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_or_equal, key)) == UInt8[0,1,1]

                    key = UInt8[0,1,10]
                    @test getkey(tran, keysel(FDBKeySel.last_less_or_equal, key)) == UInt8[0,1,10]
                    @test getkey(tran, keysel(FDBKeySel.last_less_than, key)) == UInt8[0,1,9]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_than, key)) == UInt8[0,1,11]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_or_equal, key)) == UInt8[0,1,10]

                    key = UInt8[0,1,20]
                    @test getkey(tran, keysel(FDBKeySel.last_less_or_equal, key)) == UInt8[0,1,20]
                    @test getkey(tran, keysel(FDBKeySel.last_less_than, key)) == UInt8[0,1,19]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_than, key)) != UInt8[0,1,20]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_or_equal, key)) != UInt8[0,1,19]

                    key = UInt8[0,2,0]
                    @test getkey(tran, keysel(FDBKeySel.last_less_or_equal, key)) == UInt8[0,1,20]
                    @test getkey(tran, keysel(FDBKeySel.last_less_than, key)) == UInt8[0,1,20]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_than, key)) != UInt8[0,2,0]
                    @test getkey(tran, keysel(FDBKeySel.first_greater_or_equal, key)) != UInt8[0,2,0]
                end

                # clear all keys
                open(FDBTransaction(db)) do tran
                    for key in keys
                        @test clearkey(tran, key) == nothing
                    end
                end
            end
        end
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

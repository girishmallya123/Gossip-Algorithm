-module(gossip).
-export([start_main/1, spawn_main_actor/5, spawn_child_node/8]).
-import(math, [pow/2]).

minimum([]) -> io:format("can not find minimum of empty list~n");

minimum([H|T])  ->
        minimum(H, T).

minimum(Min, [H|T]) ->
        case Min < H of
                true -> minimum(Min, T);
                false -> minimum(H, T)
        end;

minimum(Min, []) -> Min.


maximum([])     -> io:format("can not find max from empty list~n");
maximum([H|T])  ->
                maximum(H, T).

maximum(Max, [H|T])     ->
                        case Max > H of
                        true    -> maximum(Max, T);
                        false   -> maximum(H, T)
                        end;
maximum(Max, [])        -> Max.

spawn_child_node(N, RumorCt, Neighbors, Parent, Protocol, S, V, Deltas) ->
    receive
        {ping} -> 
            send_message(Parent, {pong, S, V, self()}),
            spawn_child_node(N, RumorCt, Neighbors, Parent, Protocol, S, V, Deltas);

        {update_parent, P} ->
            spawn_child_node(N, RumorCt, Neighbors, P, Protocol, S, V, Deltas);

        {update_neighbor, NV} ->
            spawn_child_node(N, RumorCt, NV, Parent, Protocol, S , V, Deltas);

        {rumor, ReceivedS, ReceivedV} ->
            NewS = S + ReceivedS,
            NewV = V + ReceivedV,

            if 
                Protocol == push_sum ->
                    Ratio = NewS / NewV,
                    if 
                        length(Deltas) == 3 ->
                            {Left, [_|Right]} = lists:split(0, Deltas),
                            NewDeltas = Left ++ Right ++ [Ratio];
                        true ->
                            NewDeltas = Deltas ++ [Ratio]
                    end,
                    DeltaDifferential = maximum(NewDeltas) - minimum(NewDeltas),
                    TerminationConst = pow(10, -10),
                    if 
                        (length(Deltas) == 3) and (DeltaDifferential =< TerminationConst) ->
                            send_message(Parent, {kill_me, self()});
                        true ->
                            send_message(self(), {spread_rumor, NewS / 2, NewV / 2}),
                            spawn_child_node(N, RumorCt+1, Neighbors, Parent, Protocol, NewS/2, NewV/2, NewDeltas)
                    end;
            true ->
                if 
                    RumorCt == 10 ->
                        send_message(Parent, {kill_me, self()});
                    true ->
                        send_message(self(), {spread_rumor, ReceivedS, ReceivedV}),
                        spawn_child_node(N, RumorCt+1, Neighbors, Parent, Protocol, S, V, Deltas)
                end
        end;

        {spread_rumor, RecS, RecV} ->
            Random_Neighbor = lists:nth(rand:uniform(length(Neighbors)), Neighbors),
            if
                Random_Neighbor == self() ->
                    send_message(self(), {spread_rumor, S, V}),
                    spawn_child_node(N, RumorCt, Neighbors, Parent, Protocol, S, V, Deltas);
                true ->
                    send_message(Random_Neighbor, {rumor, RecS, RecV}),
                    spawn_child_node(N, RumorCt, Neighbors, Parent, Protocol, RecS, RecV, Deltas)
            end
    end.

update_neighbor_nodes(0, _, _, _) ->
    ok;

update_neighbor_nodes(K, Child_actors, N, Topology) ->
    case Topology of
        full_network -> 
            send_message(lists:nth(K, Child_actors), {update_neighbor, Child_actors -- [lists:nth(K, Child_actors)]}),
            update_neighbor_nodes(K-1, Child_actors, N, Topology);
        line ->
            if
                (K == N) or ((K =/= N) and (K == length(Child_actors))) ->
                    send_message(lists:nth(K, Child_actors), {update_neighbor,[lists:nth(K-1, Child_actors)]}),
                    update_neighbor_nodes(K-1, Child_actors,N, Topology);
                K == 1 ->
                    send_message(lists:nth(K, Child_actors), {update_neighbor, [lists:nth(K+1, Child_actors)]}),
                    update_neighbor_nodes(K-1, Child_actors, N, Topology);
                true ->
                    send_message(lists:nth(K, Child_actors), {update_neighbor, [lists:nth(K+1, Child_actors), lists:nth(K-1, Child_actors)]}),
                    update_neighbor_nodes(K-1, Child_actors, N, Topology)
            end;

        twod_grid ->
            if 
                K == N or ((K =/= N) and K == length(Child_actors)) ->
                    Size = length(Child_actors);
                true ->
                    Size = N
            end,
            GridDimension = trunc(pow(pow(ceil(pow(Size, 1/2)),2), 1/2)),
            R = ((K-1) div GridDimension) + 1,
            C = (K-1) rem GridDimension + 1,
            if 
                R > 1 ->
                    UpNeighor = (R-2)*GridDimension + C,
                    NeighborUp = [lists:nth(UpNeighor, Child_actors)];
                true ->
                    NeighborUp = []
            end,
            if
                R < GridDimension ->
                    DownNeighbor = (R)*GridDimension + C,
                    if 
                        DownNeighbor >= length(Child_actors) ->
                            NeighborDown = [];
                    true ->
                        NeighborDown = [lists:nth(DownNeighbor, Child_actors)]
                    end;
                true ->
                    NeighborDown = []
            end,
            if
                C > 1 ->
                    LeftNeighbor = (R-1)*GridDimension + C - 1,
                    NeighborLeft = [lists:nth(LeftNeighbor, Child_actors)];
                true ->
                    NeighborLeft = []
            end,
            if
                C < GridDimension ->
                    RightNeighbor = (R-1)*GridDimension + C + 1,
                    if 
                        RightNeighbor >= length(Child_actors) ->
                            NeighborRight = [];
                        true ->
                            NeighborRight = [lists:nth(RightNeighbor, Child_actors)]
                    end;
                true ->
                    NeighborRight = []
            end,
            Neighbors = NeighborDown ++ NeighborLeft ++ NeighborRight ++ NeighborUp,
            send_message(lists:nth(K, Child_actors), {update_neighbor, Neighbors}),
            update_neighbor_nodes(K-1, Child_actors, N, Topology);
        threed_grid ->
        if 
                K == N or ((K =/= N) and K == length(Child_actors)) ->
                    Size = length(Child_actors);
                true ->
                    Size = N
            end,
            GridDimension = trunc(pow(pow(ceil(pow(Size, 1/2)),2), 1/2)),
            R = ((K-1) div GridDimension) + 1,
            C = (K-1) rem GridDimension + 1,
            if 
                R > 1 ->
                    UpNeighor = (R-2)*GridDimension + C,
                    NeighborUp = [lists:nth(UpNeighor, Child_actors)];
                true ->
                    NeighborUp = []
            end,
            if
                R < GridDimension ->
                    DownNeighbor = (R)*GridDimension + C,
                    if 
                        DownNeighbor >= length(Child_actors) ->
                            NeighborDown = [];
                    true ->
                        NeighborDown = [lists:nth(DownNeighbor, Child_actors)]
                    end;
                true ->
                    NeighborDown = []
            end,
            if
                C > 1 ->
                    LeftNeighbor = (R-1)*GridDimension + C - 1,
                    NeighborLeft = [lists:nth(LeftNeighbor, Child_actors)];
                true ->
                    NeighborLeft = []
            end,
            if
                C < GridDimension ->
                    RightNeighbor = (R-1)*GridDimension + C + 1,
                    if 
                        RightNeighbor >= length(Child_actors) ->
                            NeighborRight = [];
                        true ->
                            NeighborRight = [lists:nth(RightNeighbor, Child_actors)]
                    end;
                true ->
                    NeighborRight = []
            end,
            Neighbors = NeighborDown ++ NeighborLeft ++ NeighborRight ++ NeighborUp,
            NonNeighbors = Child_actors -- Neighbors,
            RandomNonNeighbor = lists:nth(rand:uniform(length(NonNeighbors)), NonNeighbors),
            NewNeighbors = Neighbors ++ [RandomNonNeighbor],
            send_message(lists:nth(K, Child_actors), {update_neighbor, NewNeighbors}),
            update_neighbor_nodes(K-1, Child_actors, N, Topology)
        end.

update_parent_process(0, _, _) ->
    ok;

update_parent_process(K, Child_actors, Pid) ->
    send_message(lists:nth(K, Child_actors), {update_parent, Pid}),
    update_parent_process(K-1, Child_actors, Pid).

send_message(Pid, Msg) ->
    Pid ! Msg.

gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology) ->
    receive
        {pong, S, V, ActorPID} ->
            send_message(ActorPID, {rumor, S, V}),
            gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology);
        {initialize} ->
            send_message(self(), {update_parent}),
            gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology);
        {kill_me, CPid} ->
            io:fwrite("Killing Child Process ~w~n", [CPid]),
            exit(CPid, normal),
            NewChildren = Child_actors -- [CPid],
            if
                length(NewChildren) == 1 ->
                    io:format("Gossip converged in ~f seconds~n", [timer:now_diff(os:timestamp(), StartTime) / 1000]),
                    send_message(self(), {stop}),
                    gossip_handler(NewChildren, K, StartTime, N, Protocol, Topology);
        
                true ->
                    ok
            end,
            send_message(self(), {initialize}),
            gossip_handler(NewChildren, K, StartTime,N,  Protocol, Topology);
        
        {begin_gossip, Children} ->
            if
                length(Children) == 1 ->
                    io:format("Gossip converged in ~f seconds~n", [timer:now_diff(os:timestamp(), StartTime) / 1000]),
                    send_message(self(), {stop}),
                    gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology);
                true ->
                    ok
            end,
            Random_child = lists:nth(rand:uniform(length(Children)), Children),
            send_message(Random_child, {ping}),
            gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology);
        
        {update_parent} ->
            update_parent_process(length(Child_actors),Child_actors, self()),
            send_message(self(), {update_neighbors}),
            gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology);
        
        {update_neighbors} ->
            update_neighbor_nodes(length(Child_actors), Child_actors, N, Topology),
            send_message(self(), {begin_gossip, Child_actors}),
            gossip_handler(Child_actors, K, StartTime, N, Protocol, Topology)
    end.


spawn_main_actor(0, Child_actors, N, Protocol, Topology) -> 
    io:fwrite("Starting gossip with ~p protocol and ~p topology~n", [Protocol, Topology]),
    gossip_handler(Child_actors, N , os:timestamp(), N,  Protocol, Topology);
    
spawn_main_actor(Actor_Index, Child_actors, N, Protocol, Topology) ->
    if 
        Protocol == push_sum ->
            S = Actor_Index,
            V = 1;
        true ->
            S = 0, 
            V = 0
    end,
    Pid = spawn(gossip, spawn_child_node, [Actor_Index, 0, [], 0, Protocol, S, V, []]),
    spawn_main_actor(Actor_Index-1, Child_actors++[Pid],N, Protocol, Topology).

start_main(Args) -> 

    if
        length(Args) < 3 ->
            io:format("Error in command line args ~n");
        true->
            ok
    end,

    N = list_to_integer(atom_to_list(lists:nth(1, Args))),
    Protocol = lists:nth(3, Args),
    Topology = lists:nth(2, Args),
    Pid = spawn(gossip, spawn_main_actor, [N, [] ,N, Protocol, Topology]),
    send_message(Pid, {initialize}).
# Gossip-Algorithm

Compile:
erlc gossip.erl  

Run the algorithm:


erl -noshell -s gossip start_main 512 full_network push_sum -s init stop 


erl -noshell -s gossip start_main 512 full_network gossip -s init stop 

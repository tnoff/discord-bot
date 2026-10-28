'''
The per-iteration work of the commands pod's background loops (#1030).

``workers.result_loops`` keeps the wiring (the broker pop, the idle sleep and the shutdown check);
what happens to one popped result lives here.
'''

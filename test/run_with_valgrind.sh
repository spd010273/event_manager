#!/bin/bash
valgrind --track-origins=yes --read-inline-info=yes --leak-check=full --show-leak-kinds=all -v --trace-children=yes --read-var-info=yes ../event_manager -U postgres -d __em_test__ -p 5432 -h localhost -E 1 -W 1

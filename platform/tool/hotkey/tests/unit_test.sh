#!/bin/bash

rm -rf ht_unit_test
#ln -s ../../../../../build/sdf/platform/tool/hotkey/tests/ht_candidate_win ht_unit_test
#ln -s ../../../../../build/sdf/platform/tool/hotkey/tests/ht_winner_lose ht_unit_test
#ln -s ../../../../../build/sdf/platform/tool/hotkey/tests/ht_winner_lose_win ht_unit_test
#ln -s ../../../../../build/sdf/platform/tool/hotkey/tests/ht_compete_winners ht_unit_test
ln -s ../../../../../build/sdf/platform/tool/hotkey/tests/ht_all_rpts ht_unit_test

gdb --args ht_unit_test



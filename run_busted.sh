#!/usr/bin/env sh
/usr/bin/env resty --errlog-level error -I ./lib -e " require 'busted.runner'({standalone = false, verbose = true, coverage = true, output = 'TAP', ROOT = {'./spec'} })"
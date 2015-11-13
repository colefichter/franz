#!/bin/bash
rebar3 compile
erl -pa _build/default/lib/franz/ebin/ -pa _build/test/lib/franz/test/ -s franz_app bootstrap
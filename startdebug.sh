#!/bin/bash
rebar3 compile
erl -pa _build/default/lib/franz/ebin/ -s franz_app bootstrap
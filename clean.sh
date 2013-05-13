#!/usr/bin/env sh
find . -name "*.pyc" | xargs rm
find . -name "*~*" | xargs rm
find . -name "*#*" | xargs rm
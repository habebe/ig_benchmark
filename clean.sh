#!/usr/bin/env sh
rm -rf build
find . -name "*.pyc" | xargs rm
find . -name "*~*" | xargs rm
find . -name "*#*" | xargs rm
#!/bin/sh

http --verbose POST localhost:8000/users/ @"$1"

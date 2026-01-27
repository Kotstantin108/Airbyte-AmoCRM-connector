#!/usr/bin/env python3
import sys
from airbyte_cdk.entrypoint import launch
from source_amo_custom.source import SourceAmoCustom

if __name__ == "__main__":
    source = SourceAmoCustom()
    launch(source, sys.argv[1:])

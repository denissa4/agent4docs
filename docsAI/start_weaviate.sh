#!/bin/bash

# Check if TalkToDocs is True
if [ "$TalkToDocs" == "True" ]; then
    echo "Starting Weaviate..."
    # Start Weaviate service
    /usr/bin/weaviate --host 0.0.0.0
else
    echo "TalkToDocs is not True, skipping Weaviate start."
fi

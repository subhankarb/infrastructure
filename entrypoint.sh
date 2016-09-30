#!/bin/bash
echo running file for source $SOURCE and date $EVENTDATE
export
echo python3 ./ETL.py -s $SOURCE -d $EVENTDATE
cd /app/ && python3 ./ETL.py -s $SOURCE -d $EVENTDATE

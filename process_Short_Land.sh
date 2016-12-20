#!/bin/sh

# Top level script that calls Python for Short Range 
# processing for hydro-inspector.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

# Source bash environment
. $HOME/.profile

cd /d4/karsten/NWM_INSPECTOR/inspector_processing
python process_Short_Inspector_Land.py

exit 0

#!/bin/sh

# Top level script that calls Python for Medium Range
# processing for hydro-inspector.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

# Source bash environment
. $HOME/.profile

cd /d4/karsten/NWM_INSPECTOR/inspector_processing
python process_Medium_Inspector_Channel_para.py

exit 0

# Python program to pull v1.0 NWM output from NCEP's FTP,
# process the output into compressed post-processed files,
# then push them to hydro-c1-web for display on hydroInspector.

# Logan Karsten 
# National Center for Atmospheric Research
# Research Applications Laboratory

import datetime
import os
import sys

# Append path to include custom libraries for this workflow
sys.path.append('./lib')

# Import custom libraries for this workflow
import compressMod
import inspectorMod

# Establish workflow variables
errTitle = 'Error_Process_Short_Forcing_Inspector'
warningTitle = 'Warning_Process_Short_Forcing_Inspector'
lockFile = '/home/karsten/tmp/Process_Short_Forcing_Inspector.LOCK'
completeDir = '/d4/karsten/NWM_INSPECTOR/Short'
email = 'karsten@ucar.edu'
hoursBack = 12
hoursLag = 2
webDirTmp = '/d2/karsten/INSPECTOR_TMP'
webDirFinal = '/d2/hydroinspector_data/tmp/conus/short_range'
metaLandPath = '/d4/karsten/NWM_INSPECTOR/geospatialMetaData/WRF_Hydro_NWM_v1.1_geospatial_data_template_land_GIS.nc'
metaChanPath = '/d4/karsten/NWM_INSPECTOR/geospatialMetaData/WRF_Hydro_NWM_v1.1_geospatial_data_template_channel_point_netcdf.nc'
metaRtPath = '/d4/karsten/NWM_INSPECTOR/geospatialMetaData/WRF_Hydro_NWM_v1.1_geospatial_data_template_terrain_GIS.nc'

# Get PID from this process
pid = os.getpid()

# Create lock file for this process
inspectorMod.createLock(lockFile,pid,warningTitle,email)

# Establish current date object
dNow = datetime.datetime.now()

# Loop through processing window.
for hourBack in range(hoursBack,hoursLag,-1):
	# Establish datetime objects
	dCycle = dNow - datetime.timedelta(seconds=hourBack*3600)

	hrStrCycle = dCycle.strftime('%H')
	dStr1Cycle = dCycle.strftime('%Y%m%d%H')
	dStr2Cycle = dCycle.strftime('%Y%m%d')

	# Loop through forecast hours
	for hrFcst in range(1,16):
		dInit = dCycle
		dValid = dCycle + datetime.timedelta(seconds=hrFcst*3600)
		fStr = str(hrFcst)
		fStr = fStr.zfill(3)

		# for each product (channel, land, etc),
	   # check for complete file that indicates
	   # file was sucessfully downloaded, processed, and 
	   # posted to hydro-c1-web to ingest into hydroInspector.

		# Forcing
		completePath = completeDir + '/nwm.t' + hrStrCycle + \
	                  'z.short_range.fe.tm00.conus_' + dStr1Cycle + \
							'_f' + fStr + '.COMPLETE'	   
		fileDPath = 'nwm.t' + hrStrCycle + 'z.fe_short_range.f' + fStr + '.conus.nc.gz'
		filePath = 'nwm.t' + hrStrCycle + 'z.fe_short_range.f' + fStr + '.conus.nc'
		fileCompress = 'nwm.' + dStr2Cycle + '_t' + hrStrCycle + '_f' + fStr + '.fe_short_range.' + \
	                  'conus.COMPRESS.nc'
		ftpDir = '/pub/data/nccf/com/nwm/prod/nwm.' + dStr2Cycle + '/fe_short_range'
		if not os.path.isfile(completePath):
			inspectorMod.downloadNWM(ftpDir,completeDir,fileDPath,filePath,errTitle,email,lockFile)
			compressMod.compressNWM(completeDir + '/' + filePath,completeDir + '/' + fileCompress, \
			                        'fe',metaLandPath,errTitle,email,lockFile,initTime=dInit,\
	                              validTime=dValid)
			inspectorMod.copyToWeb(completeDir + '/' + fileCompress,webDirTmp,errTitle,email,lockFile)
			inspectorMod.shuffleFile(fileCompress,webDirFinal,webDirTmp,errTitle,email,lockFile)
			inspectorMod.genFlag(completePath,errTitle,email,lockFile)
			inspectorMod.checkFile(completePath,errTitle,email,lockFile)
			inspectorMod.deleteFile(completeDir + "/" + filePath,errTitle,email,lockFile)
			inspectorMod.deleteFile(completeDir + "/" + fileCompress,errTitle,email,lockFile)

# Delete lock file
inspectorMod.deleteFile(lockFile,errTitle,email,lockFile)

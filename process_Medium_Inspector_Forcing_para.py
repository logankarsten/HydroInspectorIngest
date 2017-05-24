# Program for pulling NWM parallel output (Beta) from NCEP's
# para NOMADS HTTP server. Data is not operational, so we will
# pull whatever is available.

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
errTitle = 'Error_Process_Medium_Forcing_Inspector_Parallel'
warningTitle = 'Warning_Process_Medium_Forcing_Para_Inspector_Parallel'
lockFile = '/home/karsten/tmp/Process_Medium_Forcing_Inspector_Parallel.LOCK'
completeDir = '/d4/karsten/NWM_INSPECTOR/Medium_Para'
email = 'karsten@ucar.edu'
hoursBack = 36
hoursLag = 5
webDirTmp = '/d2/karsten/INSPECTOR_PARA_TMP'
webDirFinal = '/d2/hydroinspector_data/tmp/conus/para/medium_range'

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

	if hrStrCycle == '06' or hrStrCycle == '12' or hrStrCycle == '18' or hrStrCycle == '00':
		# Loop through forecast hours
		for hrFcst in range(3,243,3):
			dInit = dCycle
			dValid = dCycle + datetime.timedelta(seconds=hrFcst*3600)
			fStr = str(hrFcst)
			fStr = fStr.zfill(3)

			# for each product (channel, land, etc),
		   # check for complete file that indicates
		   # file was sucessfully downloaded, processed, and 
		   # posted to hydro-c1-web to ingest into hydroInspector.

			# Channel output
			completePath = completeDir + '/nwm.t' + hrStrCycle + \
	   	               'z.medium_range.fe.tm00.conus_' + dStr1Cycle + \
		                  '_f' + fStr + '.COMPLETE'
			fileDPath = 'nwm.t' + hrStrCycle + 'z.medium_range.forcing.f' + fStr + '.conus.nc'
			fileCompress = 'nwm.' + dStr2Cycle + '_t' + hrStrCycle + '_f' + fStr + '.fe_medium_range.' + \
		                  'conus.COMPRESS.nc'
			httpDir = 'http://para.nomads.ncep.noaa.gov/pub/data/nccf/com/nwm/para/nwm.' + dStr2Cycle + \
                '/forcing_medium_range/'
			if not os.path.isfile(completePath):
				findStatus = inspectorMod.downloadNwmHTTP(httpDir,completeDir,fileDPath,fileCompress,errTitle,email,lockFile)
				if findStatus == 1:
					compressMod.compressV11Forcing(completeDir + '/' + fileCompress,errTitle,email,lockFile)
					inspectorMod.copyToWeb(completeDir + '/' + fileCompress,webDirTmp,errTitle,email,lockFile)
					inspectorMod.shuffleFile(fileCompress,webDirFinal,webDirTmp,errTitle,email,lockFile)
					inspectorMod.genFlag(completePath,errTitle,email,lockFile)
					inspectorMod.checkFile(completePath,errTitle,email,lockFile)
					inspectorMod.deleteFile(completeDir + "/" + fileCompress,errTitle,email,lockFile)

# Delete lock file
inspectorMod.deleteFile(lockFile,errTitle,email,lockFile)

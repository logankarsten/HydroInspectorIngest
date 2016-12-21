# Python program to process SNODAS grids sitting on hydro-c1,
# and transfer them as compressed NetCDF files to hydro-c1-web
# for display on the hydroInspector.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import datetime
import os
import sys
from netCDF4 import Dataset
import numpy as np

# Append path to include custom libraries for this workflow
sys.path.append('/d4/karsten/NWM_INSPECTOR/inspector_processing/lib')

# Import custom libraries for this workflow
import compressMod
import inspectorMod

# Establish workflow variables
snodasInDir = '/d7/karsten/SNODAS_CONUS/grids'
errTitle = 'Error_Process_Snodas_Inspector'
warningTitle = 'Warning_Process_Snodas_Inspector'
lockFile = '/home/karsten/tmp/Process_Snodas_Inspector.LOCK'
completeDir = '/d4/karsten/NWM_INSPECTOR/SNODAS'
email = 'karsten@ucar.edu'
daysBack = 7
webDirTmp = '/d2/karsten/INSPECTOR_TMP'
webDirFinal = '/d2/hydroinspector_data/tmp/conus/SNODAS'
metaLandPath = '/d4/karsten/NWM_INSPECTOR/geospatialMetaData/WRF_Hydro_NWM_v1.1_geospatial_data_template_land_GIS.nc'

# Get PID from this process
pid = os.getpid()

# Create lock file for this process
inspectorMod.createLock(lockFile,pid,warningTitle,email)

# Establish current date object
dNow = datetime.datetime.now()

# Establish EPOCH time object
epoch = datetime.datetime.utcfromtimestamp(0)

# Loop through processing window.
for dayBack in range(daysBack,-1,-1):
	# Establish datetime objects
	dCurrent = dNow - datetime.timedelta(seconds=dayBack*3600*24)
	dMod = datetime.datetime.strptime(dCurrent.strftime('%Y-%m-%d') + ' 06:00:00','%Y-%m-%d %H:%M:%S')

	dt = dMod - epoch
	dtMin = int(dt.seconds/60.0)

	fileIn = snodasInDir + "/SNODAS_REGRIDDED_" + dCurrent.strftime("%Y%m%d") + ".nc"
	completeFlag = completeDir + "/SNODAS_CONUS_" + dCurrent.strftime('%Y%m%d') + '.COMPLETE'
	pathCompress = "SNODAS." + dCurrent.strftime('%Y%m%d') + '.conus.COMPRESS.nc'
	fileCompress = completeDir + "/" + pathCompress

	if not os.path.isfile(completeFlag):
		# Process SNODAS data.

		if not os.path.isfile(fileIn):
			errMsg = "ERROR: Input SNODAS file: " + fileIn + " not found."
			inspectorMod.errOut(errMsg,errTitle,email,lockFile)

		idIn = Dataset(fileIn,'r')
		idMeta = Dataset(metaLandPath,'r')
		idOut = Dataset(fileCompress,'w')

		# Read in SWE/Depth grids
		sweData = idIn.variables['SNEQV'][:,:,:].data
		depthData = idIn.variables['SNOWH'][:,:,:].data

		# Convert missing values to -9999
		indNDV = np.where(sweData < 0.0)
		sweData[indNDV] = -9999.0
		depthData[indNDV] = -9999.0

		numCol = sweData.shape[2]
		numRow = sweData.shape[1]

		# Create output dimensions and metadata
		latDim = idOut.createDimension('x',numCol)
		lonDim = idOut.createDimension('y',numRow)
		timeDim = idOut.createDimension('time')
		refDim = idOut.createDimension('reference_time',1)

		# Create output variables
		sweVar = idOut.createVariable('SNEQV','i4',('time','y','x'),fill_value=-9999,zlib=True,complevel=2)
		sdVar = idOut.createVariable('SNOWH','i4',('time','y','x'),fill_value=-9999,zlib=True,complevel=2)
		xVar = idOut.createVariable('x','f8',('x'),zlib=True,complevel=2)
		yVar = idOut.createVariable('y','f8',('y'),zlib=True,complevel=2)
		timeVar = idOut.createVariable('time','i4',('time'))
		refVar = idOut.createVariable('reference_time','i4',('reference_time'))
		projVar = idOut.createVariable('ProjectionCoordinateSystem','S1')

		# Create global attributes
		idOut.TITLE = "Snow Data Assimilation System (SNODAS) Analysis"
		idOut.model_initialization_time = dCurrent.strftime('%Y-%m-%d') + '_06:00:00'
		idOut.model_valid_time = dCurrent.strftime('%Y-%m-%d') + '_06:00:00'
		idOut.Conventions = "CF-1.6"
		idOut.Source_Software = "/d4/karsten/NWM_INSPECTOR/inspector_processing/process_Snodas_Inspector.py"
		idOut.history = "Created " + dNow.strftime('%a %b %d %H:%M:%S %Y')

		# Create variable output attributes
		xVar.standard_name = "projection_x_coordinate"
		xVar.long_name = "x coordinate of projection"
		xVar._CoordinateAxisType = "GeoX"
		xVar.units = "m"
		xVar.resolution = 1000.

		yVar.standard_name = "projection_y_coordinate"
		yVar.long_name = "y coordinate of projection"
		yVar._CoordinateAxisType = "GeoY"
		yVar.units = "m"
		yVar.resolution = 1000.

		projVar._CoordinateTransformType = "Projection"
		projVar.transform_name = "lambert_conformal_conic"
		projVar.grid_mapping_name = "lambert_conformal_conic"
		projVar._CoordinateAxes = "y x"
		projVar.esri_pe_string = idMeta.variables['ProjectionCoordinateSystem'].esri_pe_string
		projVar.proj4 = "+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs"
		projVar.standard_parallel = [30.,60.]
		projVar.longitude_of_central_meridian = -97.
		projVar.latitude_of_projection_origin = 40.0000076294
		projVar.false_easting = 0.
		projVar.false_northing = 0.
		projVar.earth_radius = 6370000.

		timeVar.long_name = "valid output time"
		timeVar.standard_name = "time"
		timeVar.units = "minutes since 1970-01-01 00:00:00 UTC"

		refVar.long_name = 'model initialization time'
		refVar.standard_name = 'forecast_reference_time'
		refVar.units = 'minutes since 1970-01-01 00:00:00 UTC'

		sweVar.units = "kg m-2"
		sweVar.long_name = "Snow water equivalent"
		sweVar.proj4 = "+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs"
		sweVar.esri_pe_string = idMeta.variables['ProjectionCoordinateSystem'].esri_pe_string
		sweVar.valid_range = [0,100000.0]
		sweVar.missing_value = -9999.

		sdVar.units = "mm"
		sdVar.long_name = "Snow depth"
		sdVar.proj4 = "+proj=lcc +lat_1=30 +lat_2=60 +lat_0=40 +lon_0=-97 +x_0=0 +y_0=0 +a=6370000 +b=6370000 +units=m +no_defs"
		sdVar.esri_pe_string = idMeta.variables['ProjectionCoordinateSystem'].esri_pe_string
		sdVar.valid_range = [0.0,100000.0]
		sdVar.missing_value = -9999.

		# Place data into variables
		xVar[:] = idMeta.variables['x'][:]
		yVar[:] = idMeta.variables['y'][:]
		sweVar[0,:,:] = np.flipud(sweData[0,:,:])
		sdVar[0,:,:] = np.flipud(depthData[0,:,:])
		timeVar[:] = dtMin
		refVar[:] = dtMin 
 
		# Close NetCDF files
		idIn.close()
		idOut.close()
		idMeta.close()

		# Move compressed file to hydro-c1-web
		inspectorMod.copyToWeb(fileCompress,webDirTmp,errTitle,email,lockFile)

		# Final atomic move to final directory for hydroInspector
		#inspectorMod.shuffleFile(pathCompress,webDirFinal,webDirTmp,errTitle,email,lockFile)

		# Create complete flag
		inspectorMod.genFlag(completeFlag,errTitle,email,lockFile)

		# Delete compressed file generated on local disk
		inspectorMod.deleteFile(fileCompress,errTitle,email,lockFile)

# Delete lock file
inspectorMod.deleteFile(lockFile,errTitle,email,lockFile)

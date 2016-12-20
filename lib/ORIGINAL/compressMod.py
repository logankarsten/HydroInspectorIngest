# Compression module, based off the official NWM geospatial appender.
# Code takes in in/out paths, a path to the geospatial metadata 
# NetCDF file and applies compression to selected variables. 

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

import netCDF4
import numpy as np
from inspectorMod import *
import re

outNCType = 'NETCDF4'

# Establish dictionary containing compression attributes needed.
varOffsets = {'FSA':0.0,'FIRA':0.0,'GRDFLX':0.0,'HFX':0.0,'LH':0.0,\
              'UGDRNOFF':0.0,'ACCECAN':0.0,'ACCEDIR':0.0,\
              'ACCETRAN':0.0,'SNOWT_AVG':0.0,'SFCRNOFF':0.0,\
              'TRAD':0.0,'SNLIQ':0.0,'SOIL_T':0.0,'SOIL_M':0.0,\
              'SNOWH':0.0,'SNEQV':0.0,'ISNOW':0.0,'FSNO':0.0,\
              'ACSNOM':0.0,'ACCET':0.0,'CANWAT':0.0,\
              'SOILICE':0.0,'SOILSAT_TOP':0.0,'SOILSAT':0.0,\
              'SNOWT':0.0,'zwattablrt':0.0,'sfcheadsubrt':0.0,\
              'streamflow':0.0,'nudge':0.0,'q_lateral':0.0,\
              'velocity':0.0,'T2D':100.0,'U2D':0.0,'V2D':0.0,\
              'SWDOWN':0.0,'LWDOWN':0.0,'Q2D':0.0,\
              'PSFC':0.0,'RAINRATE':0.0}
                  # Define scale_factor values to be used in compression.
                  #  The lower the value, the more precision retained.
varScaleFactors = {'FSA':0.1,'FIRA':0.1,'GRDFLX':0.1,'HFX':0.1,'LH':0.1,\
                   'UGDRNOFF':0.01,'ACCECAN':0.01,'ACCEDIR':0.01,\
                   'ACCETRAN':0.01,'SNOWT_AVG':0.1,'SFCRNOFF':0.001,\
                   'TRAD':0.1,'SNLIQ':0.1,'SOIL_T':0.1,'SOIL_M':0.01,\
                   'SNOWH':0.001,'SNEQV':0.1,'ISNOW':1.0,'FSNO':0.001,\
                   'ACSNOM':0.1,'ACCET':0.01,'CANWAT':0.01,\
                   'SOILICE':0.01,'SOILSAT_TOP':0.001,'SOILSAT':0.001,\
                   'SNOWT':0.1,'zwattablrt':0.1,'sfcheadsubrt':1.0,\
                   'streamflow':0.1,'nudge':0.1,'q_lateral':0.1,\
                   'velocity':0.01,'T2D':0.01,'U2D':0.001,'V2D':0.001,\
                   'SWDOWN':0.001,'LWDOWN':0.001,'Q2D':0.000001,\
                   'PSFC':0.1,'RAINRATE':1.0}
               # For most variables, the output type will be 4-byte unsigned
               # Integers. RAINRATE for forcing has been kept as floats 
               # as it's highly desireable to keep all precision there.
vardTypeComp = {'FSA':u'i4','FIRA':u'i4','GRDFLX':u'i4','HFX':u'i4',\
                'LH':u'i4','UGDRNOFF':u'i4','ACCECAN':u'i4','SNOWT_AVG':u'i4',\
                'ACCEDIR':u'i4','ACCETRAN':u'i4','TRAD':u'i4','SFCRNOFF':u'i4',\
                'SNLIQ':u'i4','SOIL_T':u'i4','SOIL_M':u'i4','SNOWH':u'i4',\
                'SNEQV':u'i4','ISNOW':u'i4','FSNO':u'i4','ACSNOM':u'i4',\
                'ACCET':u'i4','CANWAT':u'i4','SOILICE':u'i4',\
                'SOILSAT_TOP':u'i4','SOILSAT':u'i4','SNOWT_AVG':u'i4',\
                'zwattablrt':u'i4','sfcheadsubrt':u'i4','streamflow':u'i4',
                'nudge':u'i4','q_lateral':u'i4','velocity':u'i4',\
                'T2D':u'i4','U2D':u'i4','V2D':u'i4','SWDOWN':u'i4',\
                'LWDOWN':u'i4','Q2D':u'i4','PSFC':u'i4','RAINRATE':u'f4'}
             # Note that this is the valid range AFTER conversion to integer
             # values.
validRange = {'FSA':np.array((-1500,1500)),'FIRA':np.array((-1500,1500)),\
              'GRDFLX':np.array((-1500,1500)),'HFX':np.array((-1500,1500)),
              'LH':np.array((-1500,1500)),'UGDRNOFF':np.array((-5,30000)),\
              'ACCECAN':np.array((-5,30000)),'ACCEDIR':np.array((-5,30000)),\
              'ACCETRAN':np.array((-5,30000)),'SNOWT_AVG':np.array((0,400)),\
              'SFCRNOFF':np.array((0,30000)),'TRAD':np.array((0,400)),\
              'SNLIQ':np.array((0,30000)),'SOIL_T':np.array((0,400)),\
              'SOIL_M':np.array((0,1)),'SNOWH':np.array((0,100000)),\
              'SNEQV':np.array((0,100000)),'ISNOW':np.array((-10,10)),\
              'FSNO':np.array((0,1)),'ACSNOM':np.array((0,100000)),\
              'ACCET':np.array((-1000,100000)),'CANWAT':np.array((-5,30000)),\
              'SOILICE':np.array((0,1)),'SOILSAT_TOP':np.array((0,1)),\
              'SOILSAT':np.array((0,1)),'SNOWT':np.array((0,400)),\
              'zwattablrt':np.array((0,10)),'sfcheadsubrt':np.array((0,1000000)),\
              'streamflow':np.array((0,500000)),'nudge':np.array((-500000,500000)),\
              'q_lateral':np.array((0,50000)),'velocity':np.array((0,10000)),\
              'T2D':np.array((0,400)),'U2D':np.array((-100,100)),\
              'V2D':np.array((-100,100)),'SWDOWN':np.array((-5000,5000)),\
              'LWDOWN':np.array((-5000,5000)),'Q2D':np.array((0.0,1)),\
              'PSFC':np.array((0,1000000)),'RAINRATE':np.array((0.0,100.0))}
varsSkip = ['T2D','LWDOWN','SWDOWN','Q2D','U2D','V2D','PSFC',\
            'FSA','GRDFLX','HFX','LH','UGDRNOFF','ACCECAN','ACCEDIR',\
            'ACCETRAN','TRAD','SNLIQ','SOIL_T','SOIL_M','ISNOW',\
            'ACSNOM','CANWAT','SOILICE','FIRA']

def test_metadata_input(in_nc, in_type, test_nc, errTitle, emailAddy, lockFile):
   '''Function for testing consistency between WRF-Hydro output type given and
      the spatial metadata file provided. Dimension names and lengths are used as
      comparison to ensure the correct domain is used by matching dimension sizes.'''

   # Initiate test results
   test_results = False                                                        # Assume an inconsistency
   geogrid = False                                                             # Assume input is not a GEOGRID file
   gis_file = False                                                            # Assume input was not from WRF-Hydro GIS Pre-processor

   # Open Dataset objects on input spatial metadata file and WRF output file
   rootgrp = netCDF4.Dataset(in_nc, 'r')                                       # Open spatial metadata file for reading
   rootgrp2 = netCDF4.Dataset(test_nc, 'r')                                    # Open a WRF-Hydro output file for reading

   # Start comparing dimension sizes
   if in_type == 'land':
      metadata_dims = ['x', 'y']
      wrf_dims = ['west_east', 'south_north']                                 # ['x', 'y']
      metadata_xdim = len(rootgrp.dimensions[metadata_dims[0]])
      metadata_ydim = len(rootgrp.dimensions[metadata_dims[1]])
      wrf_xdim = len(rootgrp2.dimensions[wrf_dims[0]])
      wrf_ydim = len(rootgrp2.dimensions[wrf_dims[1]])
      if sum([metadata_xdim==wrf_xdim, metadata_ydim==wrf_ydim]) == 2:
         test_results = True

      # Check to see if this is a GEOGRID file
      if 'TITLE' in rootgrp.__dict__:
         titleStr = rootgrp.__dict__['TITLE']
         if re.match(re.compile('OUTPUT FROM GEOGRID*'), titleStr):
            geogrid = True

      # Check to see if this was created by the WRF-Hydro GIS pre-processor
      if 'Source_Software' in rootgrp.__dict__:
         titleStr = rootgrp.__dict__['Source_Software']
         if re.match(re.compile('WRF-Hydro GIS*'), titleStr):
            gis_file = True

   elif in_type == 'terrain_rt':
      metadata_dims = ['x', 'y']
      wrf_dims = ['x', 'y']
      metadata_xdim = len(rootgrp.dimensions[metadata_dims[0]])
      metadata_ydim = len(rootgrp.dimensions[metadata_dims[1]])
      wrf_xdim = len(rootgrp2.dimensions[wrf_dims[0]])
      wrf_ydim = len(rootgrp2.dimensions[wrf_dims[1]])
      if sum([metadata_xdim==wrf_xdim, metadata_ydim==wrf_ydim]) == 2:
         test_results = True

      if 'Source_Software' in rootgrp.__dict__:
         titleStr = rootgrp.__dict__['Source_Software']
         if re.match(re.compile('WRF-Hydro GIS*'), titleStr):
            gis_file = True

   elif in_type == 'channel_rt' or in_type == 'reservoir':
      metadata_dims = ['station']                                             # For non LAKEPARM.nc spatial metadata files
      wrf_dims = ['station']

      if 'Source_Software' in rootgrp.__dict__:
         titleStr = rootgrp.__dict__['Source_Software']
         if re.match(re.compile('WRF-Hydro GIS*'), titleStr):
            gis_file = True
            if in_type == 'channel_rt':
               metadata_dims = ['linkDim']                                 # For RouteLink.nc spatial metadata files
            elif in_type == 'reservoir':
               metadata_dims = ['nlakes']                                  # For LAKEPARM.nc spatial metadata files

      metadata_dim = len(rootgrp.dimensions[metadata_dims[0]])
      wrf_dim = len(rootgrp2.dimensions[wrf_dims[0]])
      if metadata_dim==wrf_dim:
         test_results = True

   # Pass test for forcing files    
   elif in_type == 'fe':
      test_results = True
      gis_file = True

   else:
      errMsg = "ERROR: Could not recognize the WRF-Hydro output type given (%s)" %is_type
      errOut(errMsg,errTitle,emailAddy,lockFIle)
      test_results = False

   if test_results != True:
      errMsg = "ERROR: One of the checks between the files resulted in a mismatch"
      errOut(errMsg,errTitle,emailAddy,lockFile)

   rootgrp.close()
   rootgrp2.close()
   return test_results, geogrid, gis_file

def main(in_nc,filetype,out_path,filesList,errTitle,emailAddy,lockFile,isGEOGRID=False, isGIS=False, isCompress=False):
    '''This method will read each input file and write a new netCDF (NETCDF4_CLASSIC)
    file to the output directory. This can save substantial amounts of time because
    writing NETCDF4_CLASSIC variable attributes is very much faster using this method
    than trying to edit or write to a NETCDF3 file.'''

    # Open spatial metadata file for reading
    geo_nc = netCDF4.Dataset(in_nc, 'r')

    # Set some defaults
    mapDims = {}
    fillValue = True                                                           # Fill value for missing values
    excludeVars = []                                                            # Variables to exclude when adding attributes from adVarAtts list
    addVarAtts = {}                                                             # These variable attributes will be added to each data variable in output
    excludeVarAtts = []                                                          # List of variable attributes to remove
    addGlobalAtts = ['Conventions']                                             # Global attributes to move into the new files

    # Handle dimensions by WRF-Hydro output file type
    if filetype == 'land':
        mapDims = {'west_east': u'x', 'south_north': u'y'}                      # Dictionary to map old dimensions to new dimensions
        addVars = [u'x', u'y']                                                  # These variables will be added to the output from the spatial metadata file
        excludeVars = [u'time']                                                 # Variables to exclude when adding attributes from adVarAtts list
        fillValue = -1.0e+33                                                    # Fill value for missing values

    elif filetype == 'terrain_rt':
        # No need to map or add dimensions to this type of file
        excludeVarAtts = [u'coordinates']                                       # Remove this variable attribute as it will be unnecessary
        addVars = [u'x', u'y']                                                  # These variables will be added to the output from the spatial metadata file
        excludeVars = [u'time']                                                 # Variables to exclude when adding attributes from adVarAtts list
        fillValue = -8.9999998e+15

    elif filetype == 'channel_rt':
        # No need to map or add dimensions to this type of file
        addVars = [u'latitude', u'longitude']                                   # These variables will be added to the output from the spatial metadata file
        addVarAtts = {u'coordinates': 'latitude longitude'}                     # These variable attributes will be added to each data variable in output
        excludeVars = [u'time', u'station_id']                                  # Variables to exclude when adding attributes from adVarAtts list
        addGlobalAtts += [u'esri_pe_string', u'proj4', u'featureType']          # Global attributes to move into the new files

    elif filetype == 'reservoir':
        # No need to map or add dimensions to this type of file
        addVars = [u'latitude', u'longitude']                                   # These variables will be added to the output from the spatial metadata file
        addVarAtts = {u'coordinates': 'latitude longitude'}                     # These variable attributes will be added to each data variable in output
        excludeVars = [u'time', 'lake_id']                                      # Variables to exclude when adding attributes from adVarAtts list
        addGlobalAtts += [u'esri_pe_string', u'proj4', u'featureType']          # Global attributes to move into the new files

    elif filetype == 'fe':
        # Raw forcing files use several dimensions       
        mapDims = {'ncl0': u'y', 'ncl1': u'x', 'ncl2': u'y', 'ncl3': u'x',
                   'ncl4': u'y', 'ncl5': u'x', 'ncl6': u'y', 'ncl7': u'x',
                   'ncl8': u'y', 'ncl9': u'x', 'ncl10': u'y', 'ncl11': u'x',
                   'ncl12': u'y', 'ncl13': u'x', 'ncl14': u'y', 'ncl15': u'x',
                   'ncl16': u'y', 'ncl17': u'x', 'ncl18': u'y', 'ncl19': u'x'}
        addVars = [u'x', u'y']
        fillValue = 9.96921E36

    else:
        print 'Could not recognize the WRF-Hydro output type given (%s)' %in_type
        usage()

    # Determine which variable contains the coordinate system/coordinate transform variable(s)
    if filetype in ['land', 'terrain_rt', 'fe']:
        crs_var = [varname for varname,ncvar in geo_nc.variables.iteritems() if u'grid_mapping_name' in ncvar.ncattrs()][0]
        addVarAtts[u'grid_mapping'] = crs_var
        addVarAtts[u'esri_pe_string'] = geo_nc.variables[crs_var].getncattr(u'esri_pe_string')
        # COMMENTED OUT FOR NOW DUE TO GDAL NOT BEING AVAILABLE ON WCOSS
        #sr = osr.SpatialReference()
        #sr.ImportFromESRI([addVarAtts[u'esri_pe_string']])
        #addVarAtts[u'proj4'] = sr.ExportToProj4()
        addVars.append(crs_var)                                                     # Add to the list of variables to append from spatial metadata file

    # Gather global attributes from spatial metadata file
    geoatts = {att:val for att,val in geo_nc.__dict__.iteritems() if att in addGlobalAtts}

    # Iterate over each file in the input directory
    for wrf_file in filesList:

        # Create the output path and filename
        newfile = out_path

        # Open WRF output file for reading

        # WARNING - NetCDF3 is very slow! This script converts all outputs to NETCDF4
        rootgrp1 = netCDF4.Dataset(wrf_file, 'r')                               # Open a netCDF4 write object on the output file
        rootgrp2 = netCDF4.Dataset(newfile, 'w', format=outNCType)              # Open a write object on the output file. , format=rootgrp1.data_model
        rootgrp2.set_fill_on

        # Copy dimensions from WRF-Hydro output file, omitting variables that will be changed
        for dimname, dim in rootgrp1.dimensions.iteritems():
            if dimname in mapDims:
                # Check to see if dimensions already exist in output file                
                if mapDims[dimname] in rootgrp2.dimensions.keys():
                    continue
                else:
                    rootgrp2.createDimension(mapDims[dimname], len(dim))            # Create dimensions with new names
                    continue
            rootgrp2.createDimension(dimname, len(dim))                         # Copy other dimensions from the WRF-Hydro output file

        # For forcing files, copy template variables to output file before others        
        if filetype == 'fe':
            for varname in addVars:
                ncvar = geo_nc.variables[varname]
                var = rootgrp2.createVariable(varname, ncvar.dtype, ncvar.dimensions)
                var.setncatts(ncvar.__dict__)                                       # Copy the variable attributes

        # Copy variables from WRF-Hydro output file, adding variable attributes as necessary       
        for varname, ncvar in rootgrp1.variables.iteritems():
            # Skip variables that are not desired in the output
            if varname in varsSkip:
                continue

            # Replace old variable dimension names with the new dimension names as necessary
            varDims = tuple(mapDims.get(varDim) if varDim in mapDims else varDim for varDim in ncvar.dimensions)
            varAtts = {key:val for key,val in ncvar.__dict__.iteritems() if key not in excludeVarAtts}    # Only keep necessary variable attributes

            if ncvar.dtype == 'int32':

                if isCompress:
                    var = rootgrp2.createVariable(varname, ncvar.dtype, varDims, zlib = True, complevel = 2) # Already integer, simply apply compression
                else:
                    var = rootgrp2.createVariable(varname, ncvar.dtype, varDims)

            else:

                if isCompress:
                    # Note that a default fill value of -9999 is chosen. However, like valid_range, this needs to be convered using the scale_factor
                    # in order for NetCDF libraries to properly maksk out values. 
                    var = rootgrp2.createVariable(varname, vardTypeComp[varname], varDims, fill_value=-9999/varScaleFactors[varname], zlib = True, complevel = 2)
                else:
                    var = rootgrp2.createVariable(varname, ncvar.dtype, varDims, fill_value=fillValue)

            # Over-write existing _FillValues if they exist.
            if '_FillValue' in varAtts:
                del varAtts['_FillValue']
            var.setncatts(varAtts)
            if varname not in excludeVars:
                rootgrp2.variables[varname].setncatts(addVarAtts)               # Copy additional variable attributes from spatial metadata file
            elif varname == u'time':
                rootgrp2.variables[varname].setncattr(u'standard_name', u'time')# Set standard_name for time

        # Find the coordinate system/transform variable or any additional variables from the spatial metadata file and copy
        # Skip this step for forcing files...has already been done above        
        if filetype == 'fe':
            pass
        else:
            for varname in addVars:
                ncvar = geo_nc.variables[varname]
                var = rootgrp2.createVariable(varname, ncvar.dtype, ncvar.dimensions)
                var.setncatts(ncvar.__dict__)                                       # Copy the variable attributes

        # Copy global attributes from both spatial metdata file and original WRF-Hydro output file
        ncatts = rootgrp1.__dict__
        geoatts2 = geoatts
        geoatts2.update(ncatts)
        rootgrp2.setncatts(geoatts2)

        # Add variable values last (makes the script run faster) 
        for varname, ncvar in rootgrp1.variables.iteritems():
            if varname in varsSkip:
                continue

            var = rootgrp2.variables[varname]
            if isCompress:
                if ncvar.dtype == 'int32':
                    var[:] = ncvar[:]
                else:
                    # Apply scale_factor and add_offset, except where floating point output has been specified above. 
                    varTmp = ncvar[:]
                    indNdv = np.where(varTmp == fillValue)
                    indValid = np.where(varTmp != fillValue)
                    varTmp[indNdv] = -9999/varScaleFactors[varname]
                    if vardTypeComp[varname] != 'f4':
                        varTmp[indValid] = (varTmp[indValid] - varOffsets[varname]) / varScaleFactors[varname]
                    if vardTypeComp[varname] != 'f4':
                        varTmp = varTmp.astype(int)
                    var[:] = varTmp
                    if vardTypeComp[varname] != 'f4':
                        var.scale_factor = varScaleFactors[varname]
                        var.add_offset = varOffsets[varname]
                # Set valid_range attributes
                if varname in validRange:
                    var.valid_range = validRange[varname]/varScaleFactors[varname]
            else:
                var[:] = ncvar[:]                                                   # Copy the variable data into the newly created variable
        for varname in addVars:
            ncvar = geo_nc.variables[varname]
            var = rootgrp2.variables[varname]
            # For some reason the grid is upside-down because of the way the y values are sorted
            if isGIS and varname == 'y':
                var[:] = ncvar[::-1]                                          # Reverse order of y variable to flip up-down
            else:
                var[:] = ncvar[:]                                               # Copy the variable data into the newly created variable

        # Delete missing_value attribute as it will conflict with _FillValue for individual variables. 
        if filetype != 'fe':
            del rootgrp2.missing_value

        # Close files
        rootgrp1.close()
        rootgrp2.close()

def compressNWM(fileIn,fileOut,nwmType,metaFile,errTitle,emailAddy,lockFile):
   files_list = []
   files_list.append(fileIn)

   test_results, geogrid, gis_file = test_metadata_input(metaFile, nwmType, files_list[0],\
                                                         errTitle,emailAddy,lockFile)
   main(metaFile,nwmType,fileOut,files_list,errTitle,emailAddy,lockFile,isGEOGRID=geogrid,isGIS=gis_file,isCompress=True)

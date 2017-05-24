# Collection of Python functions used in processing 
# NWM output.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

# Import necessary libraries
import multiprocessing
import os
import sys
import time
import smtplib
from ftplib import FTP
from email.mime.text import MIMEText
import glob
import subprocess
from bs4 import BeautifulSoup
import requests
import urllib

def fetchFTP(ftp,cmd,outDir,fileDownload,errTitle,emailAddy,lockFile):
	fetchStatus = False
	fetchTries = 0
	while not fetchStatus:
		try:
			ftp.retrbinary(cmd,open(outDir + "/" + fileDownload,'wb').write)
			fetchStatus = True
		except:
			fetchTries = fetchTries + 1
			if fetchTries > 10:
				errOutQuiet(lockFile)

def errOut(msgContent,emailTitle,emailRec,lockFile):
   msg = MIMEText(msgContent)
   msg['Subject'] = emailTitle
   msg['From'] = emailRec
   msg['To'] = emailRec
   s = smtplib.SMTP('localhost')
   s.sendmail(emailRec,[emailRec],msg.as_string())
   s.quit()
   # Remove lock file
   os.remove(lockFile)
   sys.exit(1)

def errOutQuiet(lockFile):
	# Remove lock file
	if os.path.isfile(lockFile):
		os.remove(lockFile)
	sys.exit(1)

def warningOut(msgContent,emailTitle,emailRec):
   msg = MIMEText(msgContent)
   msg['Subject'] = emailTitle
   msg['From'] = emailRec
   msg['To'] = emailRec
   s = smtplib.SMTP('localhost')
   s.sendmail(emailRec,[emailRec],msg.as_string())
   s.quit()
   sys.exit(1)

def createLock(lockFile,pid,warningTitle,emailAddy):
   if os.path.isfile(lockFile):
      fileLock = open(lockFile,'r')
      pid = fileLock.readline()
      warningMsg =  "WARNING: Another Pull Program Running. PID: " + pid
      warningOut(warningMsg,warningTitle,emailAddy)
   else:
      fileLock = open(lockFile,'w')
      fileLock.write(str(pid))
      fileLock.close()

def checkFile(fileCheck,errTitle,emailAddy,lockFile):
   # Generic routine to check for existence of file
   if not os.path.isfile(fileCheck):
      errMsg = "ERROR: File: " + fileCheck + " Not Found."
      errOut(errMsg,errTitle,emailAddy,lockFile)

def deleteFile(fileDel,errTitle,emailAddy,lockFile):
   # Generic routine to delete file.
   try:
      os.remove(fileDel)
   except:
      errMsg = "ERROR: Failed to delete: " + fileDel
      errOut(errMsg,errTitle,emailAddy,lockFile)

def cleanOutDir(outDir,errTitle,emailAddy,lockFile):
   # Go through output directory, cleanup any NetCDF files
   # or old gzip files that may be lying around from
   # previous failed attempts. This is to keep things clean.
   
   try:
      pathGz = outDir + "/*.gz"
      pathNc = outDir + "/*.nc"
   
      listGz = glob.glob(pathGz)
      listNc = glob.glob(pathNc)
  
      if len(listGz) > 0:
         for fileDel in listGz:
            os.remove(fileDel)

      if len(listNc) > 0:
         for fileDel in listNc:
            os.remove(fileDel)
   except:
      errMsg = "ERROR: Failure to cleanup output directory."
      errOut(errMsg,errTitle,emailAddy,lockFile)

def downloadNWM(ftpDir,outDir,fileDownload,fileTmp,errTitle,emailAddy,lockFile):
	# Download NWM output from NCEP FTP, then proceed to unzip the files
	# to a specified directory.

   # Create FTP instance
	ftpConnectTries = 0
	ftpConnectStatus = False
	while not ftpConnectStatus:
		try:
			ftp = FTP('ftp.ncep.noaa.gov')
			ftp.login()
			ftpConnectStatus = True
		except:
			ftpConnectStatus = False
			ftpConnectTries = ftpConnectTries + 1
			if ftpConnectTries > 10:
				errOutQuiet(lockFile)

	# Switch directories. If the directory is not found, wait 5 minutes and continue. Around 00 UTC,
	# directories for the new day take a while to get created.
	dirChTries = 0
	dirChStatus = False
	check = 0
	while not dirChStatus:  
		try:
			ftp.cwd(ftpDir)
			dirChStatus = True
			fileList = ftp.nlst()
			for file in fileList:
				if file == fileDownload:
					check = 1
		except:
			dirChTries = dirChTries + 1
			if dirChTries > 10:
				errOutQuiet(lockFile)

	if check != 1:
		errOutQuiet(lockFile)

	# Download gzip file
	downloadTries = 0
	downloadStatus = False
	while not downloadStatus:
		cmd = "RETR " + fileDownload
		p = multiprocessing.Process(target=fetchFTP,args=(ftp,cmd,outDir,fileDownload,errTitle,emailAddy,lockFile,))
		p.start()
		p.join(300)
		if p.is_alive():
			downloadTries = downloadTries + 1
			p.terminate()
			p.join()
			if downloadTries > 10:
				sys.exit(1)
		else:
			# File was sucessfully downloaded
			downloadStatus = True

	# Quit FTP
	ftpDisconnectTries = 0
	ftpDisconnectStatus = False
	while not ftpDisconnectStatus:
		try:
			ftp.quit()
			ftpDisconnectStatus = True
		except:
			ftpDisconnectTries = ftpDisconnectTries + 1
			if ftpDisconnectTries > 10:
				errOutQuiet(lockFile)

   # Unzip file
   #cmd = "gunzip " + outDir + "/" + fileDownload
   #try:
   #   subprocess.call(cmd,shell=True)
   #except:
   #   errMsg = "ERROR: Unable to unzip " + fileDownload 
   #   errOut(errMsg,errTitle,emailAddy,lockFile)

def downloadNwmHTTP(httpDir,outDir,fileDownload,fileOut,errTitle,emailAddy,lockFile):
	# Download NWM files from an HTTP NOMADS server.
	# First list files in the directory.
	ext = 'nc'
	try:
		page = requests.get(httpDir).text
		soup = BeautifulSoup(page,'html.parser')
		dirFiles = [httpDir + '/' + node.get('href') for node in soup.find_all('a') if node.get('href').endswith(ext)]
	except:
		errMsg = "ERROR: Unable to connect to: " + httpDir + " and retrieve listing"
		errOut(errMsg,errTitle,emailAddy,lockFile)

	returnStatus = 0
	# Establish full path to expected file.
	downloadPath = httpDir + '/' + fileDownload
	outPath = outDir + "/" + fileOut
	if len(dirFiles) != 0:
		for fileCheck in dirFiles:
			if fileCheck == downloadPath:
				# Download the file
				try:
					objHandle = urllib.urlretrieve(fileCheck,outPath)
					returnStatus = 1
				except:
					errMsg = "ERROR: Unable to retrieve file: " + fileCheck
					errOut(errMsg,errTitle,emailAddy,lockFile)

	return returnStatus

def renameFile(fileIn,fileOut,errTitle,emailAddy,lockFile):
	# Rename a file.
	try:
		os.rename(fileIn,fileOut)
	except:
		errMsg = "ERROR: Failure to rename file from: " + fileIn + " to: " + fileOut
		errOut(errMsg,errTitle,emailAddy,lockFile)

def copyToWeb(fileCopy,webDir,errTitle,emailAddy,lockFile):
   # Copy file to directory on web server
   cmd = 'scp -q -o LogLevel=QUIET ' + fileCopy + ' hydro-c1-web:' + webDir
   try:
      subprocess.call(cmd,shell=True)
   except:
      errMsg = "ERROR: Failure to Copy: " + fileCopy + " To: " + webDir
      errOut(errMsg,errTitle,emailAddy,lockFile)

def shuffleFile(fileCompress,webDirFinal,webDirTmp,errTitle,emailAddy,lockFile):
   # Perform final move of data from temporary directory
   # on hydro-c1-web to final directory through atomic move.
   cmdP = 'ssh karsten@hydro-c1-web chmod 777 ' + webDirTmp + '/' + \
          fileCompress
   cmd = 'ssh karsten@hydro-c1-web mv ' + webDirTmp + '/' + \
         fileCompress + ' ' + webDirFinal
   try:
      subprocess.call(cmdP,shell=True)
      subprocess.call(cmd,shell=True)
   except:
      errMsg = "ERROR: Failure to move: " + fileCompress + " To: " + webDirFinal
      errOut(errMsg,errTitle,emailAddy,lockFile)

def genFlag(completeFlagPath,errTitle,emailAddy,lockFile):
   # Generate empty flag file 
   cmd = 'touch ' + completeFlagPath
   try:
      subprocess.call(cmd,shell=True)
   except:
      errMsg = "ERROR: Unable to Generate Flag File: " + completeFlagPath
      errOut(errMsg,errTitle,emailAddy,lockFile)

# Collection of Python functions used in processing 
# NWM output.

# Logan Karsten
# National Center for Atmospheric Research
# Research Applications Laboratory

# Import necessary libraries
import multiprocessing
import os
import sys
import smtplib
from ftplib import FTP
from email.mime.text import MIMEText
import glob
import subprocess

def fetchFTP(ftp,cmd,outDir,fileDownload,errTitle,emailAddy,lockFile):
	try:
		ftp.retrbinary(cmd,open(outDir + "/" + fileDownload,'wb').write)
	except:
		errMsg = "ERROR: Error in downloading: " + fileDownload
		errOut(errMsg,errTitle,emailAddy,lockFile)

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
   try:
      ftp = FTP('ftp.ncep.noaa.gov')
      ftp.login()
   except:
      errOut('ERROR: Unable to FTP to ftp.ncep.noaa.gov',errTitle,emailAddy,lockFile)
  
   try:
      ftp.cwd(ftpDir)
      fileList = ftp.nlst()
   except:
      errMsg = "ERROR: Unable to Change FTP to Directory: " + ftpDir
      errOut(errMsg,errTitle,emailAddy,lockFile)

   # Loop through file list and check to make sure data exists on server.
   check = 0
   for file in fileList:
      if file == fileDownload:
         check = 1

   if check != 1:
      errMsg = "ERROR: Expected to Find: " + fileDownload + " File not Found on FTP Server."
      errOut(errMsg,errTitle,emailAddy,lockFile)

   # Download gzip file
   try:
      cmd = "RETR " + fileDownload
      p = multiprocessing.Process(target=fetchFTP,args=(ftp,cmd,outDir,fileDownload,errTitle,emailAddy,lockFile,))
      p.start()
      p.join(300)
      if p.is_alive():
         warningMsg = "ERROR: Download Timeout For: " + fileDownload
         p.terminate()
         p.join()
         warningOut(warningMsg,errTitle,emailAddy)
   except:
      errMsg = "ERROR: Unable to Download: " + fileDownload
      errOut(errMsg,errTitle,emailAddy,lockFile)

   # Quit FTP
   try:
      ftp.quit()
   except:
      errMsg = "ERROR: Unable to successfully exit FTP."
      errOut(errMsg,errTitle,emailAddy,lockFile)

   # Unzip file
   cmd = "gunzip " + outDir + "/" + fileDownload
   try:
      subprocess.call(cmd,shell=True)
   except:
      errMsg = "ERROR: Unable to unzip " + fileDownload 
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

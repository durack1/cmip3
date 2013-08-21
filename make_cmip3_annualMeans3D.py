# -*- coding: utf-8 -*-
"""
Created on Thu May 31 13:16:50 2012

Paul J. Durack 31 May 2012

File created to generate 3D annual mean files
Sourced from Pete G, annual_means.py

PJD 31 May 2012     - Added gc.collect statements
PJD 31 May 2012     - BNU-ESM.piControl.r1i1p1.mo.so.ver-v20120503 using 95% RAM @ nyrs=25yrs, without writing a single file - drop back to 10yrs
PJD 31 May 2012     - Corrected pathin to take var as path argument
PJD 31 May 2012     - Even with nyrs=10 BNU-ESM.piControl.r1i1p1.mo.thetao.ver-v20120503 is using 47% RAM and bombed due to memory
PJD 31 May 2012     - BNU-ESM.piControl.r1i1p1.mo.so.ver-v20120503 is using 90% RAM nyrs=10
                    BNU-ESM.historical.r1i1p1.mo.so.ver-v20120504 ~4% nyrs=10;
                    BNU-ESM.historical.r1i1p1.mo.thetao.ver-v20120504 ~4% nyrs=10
PJD 31 May 2012     - Added code to drop BNU**piControl calculations to consider nyrs=5 all else nyrs=25
PJD 31 May 2012     - Even with nyrs=5, BNU-ESM.piControl.r1i1p1.mo.so.ver-v20120503 ~43%,
                    BNU-ESM.piControl.r1i1p1.mo.thetao.ver-v20120503 ~46%
                    With nyrs=25, BNU-ESM.historical.r1i1p1.mo.so.ver-v20120504 ~8%,
                    BNU-ESM.historical.r1i1p1.mo.thetao.ver-v20120504 ~8%
PJD 31 May 2012     - nyrs=25, BNU-ESM.historical.r1i1p1.mo.so.ver-v20120504 ~8%
                    BNU-ESM.historical.r1i1p1.mo.thetao.ver-v20120504 ~8%
                    CCSM4.piControl.r1i1p1.mo.so.ver-v20120220 ~17%
                    CCSM4.piControl.r1i1p1.mo.thetao.ver-v20120220 ~17%
PJD  1 Jun 2012     - Commented out second del() statement, are deleted variables tripping things over?
PJD  1 Jun 2012     - Issue appears to be with trailing time so 1960-1-1 being included/indexed twice, use 'con' to limit up to bounds?
PJD  1 Jun 2012     - Edited to include missing last year of historical/piControl etc
PJD  4 Jun 2012     - CCSM4 output is shaped 11,lev,lat,lon (doesn't appear an issue for other models: CNRM-CM5)
PJD  4 Jun 2012     - Added setTimeBoundsMonthly(d) with the hope this will fix issues with CCSM4
PJD  5 Jun 2012     - Test run of cdscan on /cmip5/scratch/cmip5/output1/BNU/BNU-ESM/piControl/mon/ocean/Omon/r1i1p1/v20120503/so yields no complaints - remove BNU catch below and rerun - problem still occurs with thetao
PJD  7 Jun 2012     - Added logging with timings, some files were written with 26 elements in a 25yr mean? Which ones?
PJD  7 Jun 2012     - Updated subsequent log write statements to use 'a', rather than 'w'
PJD  7 Jun 2012     - Updated reporting code, log if a file exists and is being skipped
PJD 17 Jun 2012     - Calculated an files were being written at float64 precision - recast back to float32, reducing file sizes by half
                    [durack1@crunchy cmip5]$ date
                    Sun Jun 17 12:15:05 PDT 2012
                    [durack1@crunchy cmip5]$ du -sch /work/durack1/Shared/cmip5/tmp
                    1.6T	/work/durack1/Shared/cmip5/tmp
                    1.6T	total
PJD 17 Jun 2012     - Added 04d formatted years,'an',var and version info to outfilename
PJD 17 Jun 2012     - Reorder logfile variables to align with an2D logfiles
PJD 17 Jun 2012     - Added df -h argument to report more meaningful numbers to logs
PJD 17 Jun 2012     - Added 'ver' to output directory build, required as GISS-E2-R.r1i1p141.ver-v20120105.850.874.nc and GISS-E2-R.r1i1p141.ver-v20120319.850.874.nc exist in same subdir
PJD 17 Jul 2012     - Updated to index ver as 7 (was 6) due to realm being added to xml files
PJD 19 Aug 2012     - Updated nyrs for MIROC4h, was 25 now 5 for this model
PJD 17 Oct 2012     - Upated to run for cmip3
PJD  2 Jun 2013     - Cleaned up code and added oceanonly to known hosts
PJD  2 Jun 2013     - TOCHECK: Off by one end year, many models complete at YEAR-01-01
PJD  8 Jun 2013     - TODO: Fix issue with incorrect time_axis - skip problematic files?:
                    * Outfile: cmip5.giss_model_e_r.sresa1b.run1.an.so.ver-2.2104-2128.nc being processed **
                    2104-1-1 0:0:0.0 2129-1-1 0:0:0.0
                    (1200, 1500)
                    Traceback (most recent call last):
                      File "make_cmip3_annualMeans3D.py", line 190, in <module>
                        d = f(var,time=(cdtime.comptime(lb),cdtime.comptime(ub),'co')) ; # Exclude last year
                        ...
                      File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/tvariable.py", line 320, in setAxis
                        raise CDMSError,"axis length %d does not match corresponding dimension %d"%(len(axis),axislen)
                    cdms2.error.CDMSError: axis length 300 does not match corresponding dimension 311
PJD  8 Jun 2013     - TODO: Rerun with cdms.setAutoBounds off, runs started 120617_1400 all have this set on
PJD  8 Jun 2013     - TODO: Consider reporting and logging memory usage resource.getrusage(resource.RUSAGE_SELF).ru_maxrss should return this

PJD  8 Aug 2013     - PROBLEM:
** Outfile: cmip5.giss_model_e_r.sresa1b.run1.an.so.ver-2.2104-2128.nc being processed **
2104-1-1 0:0:0.0 2129-1-1 0:0:0.0
(1200, 1500)
Traceback (most recent call last):
  File "make_cmip3_annualMeans3D.py", line 201, in <module>
    d = f(var,time=(cdtime.comptime(lb),cdtime.comptime(ub),'co')) ; # Exclude last year
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/cudsinterface.py", line 45, in __call__
    return v(*args, **kwargs)
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/avariable.py", line 155, in __call__
    grid=grid)
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/selectors.py", line 176, in unmodified_select
    fetched = result.subRegion(*specifications)
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/avariable.py", line 759, in subRegion
    return self.subSlice(*slicelist, **d)
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/avariable.py", line 609, in subSlice
    id = self.id)
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/tvariable.py", line 158, in __init__
    self.initDomain(axes, copyaxes=copyaxes)           # Note: clobbers the grid, so set the grid after.
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/tvariable.py", line 292, in initDomain
    self.copyAxis(i, flataxes[i])
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/tvariable.py", line 342, in copyAxis
    self.setAxis (n, mycopy)
  File "/usr/local/uvcdat/latest/lib/python2.7/site-packages/cdms2/tvariable.py", line 320, in setAxis
    raise CDMSError,"axis length %d does not match corresponding dimension %d"%(len(axis),axislen)
cdms2.error.CDMSError: axis length 300 does not match corresponding dimension 311





@author: durack1
"""

import cdms2 as cdms
# Set cdat nc criteria
#cdms.setAutoBounds('on') ; # Set off 120619_0624 as could cause issues with non-gregorian calendars
cdms.setNetcdfShuffleFlag(0)
cdms.setNetcdfDeflateFlag(0)
cdms.setNetcdfDeflateLevelFlag(0)
import os,string,cdtime,cdutil,sys,datetime,argparse,gc
from socket import gethostname
from string import replace

# Set directories
host_name = gethostname()
if host_name in {'crunchy.llnl.gov','oceanonly.llnl.gov'}:
    trim_host = replace(host_name,'.llnl.gov','')
    host_path = '/work/durack1/Shared/cmip3' ; # crunchy 121018
else:
    print '** HOST UNKNOWN, aborting.. **'
    sys.exit()

# Get script arguments
parser = argparse.ArgumentParser()
parser.add_argument('variable',metavar='str',type=str,help='include \'variable\' as a command line argument')
parser.add_argument('experiment',metavar='str',type=str,help='include \'experiment\' as a command line argument')
args = parser.parse_args()
if (args.variable == ''):
   print "** No variables passed, exiting **"
   sys.exit()
else:
   var = args.variable
   exp = args.experiment
   print "".join(['** Processing for variable: ',var,' and experiment: ',exp,' **'])

# Set paths
pathin = '/work/cmip3'
pathout = '/work/durack1/Shared/cmip3'

# Create logfile
time_now = datetime.datetime.now()
time_format = time_now.strftime("%y%m%d_%H%M%S")
logfile = os.path.join(pathout,"".join([time_format,'_make_cmip3_an3D-',exp,'-',var,'-',trim_host,'.log']))
# Log disk info for crunchy:/work
os.chdir('/work')
cmd = 'df -h | grep work'
o = os.popen(cmd).readlines()
oj = "".join(o)
# Open logfile to write
logfile_handle = open(logfile,'w')
logfile_handle.write("".join(['TIME: ',time_format,'\n']))
logfile_handle.write("".join(['HOSTNAME: ',host_name,'\n']))
logfile_handle.write("".join(['WORKDISK: ',oj]))
logfile_handle.close()

# Set inputs
indir = os.path.join(pathin,exp,'ocn/mo',var) # *.xmls
outdir = os.path.join(pathout,exp,'ocn/an',var,'ncs')

# Get input xml files
lst = os.listdir(indir); lst.sort();

# Loop over input
for l in lst:
  print "".join(['** Processing xml: ',l])
  # Check which model is being processed and exclude if a problem - xmls are now updated so if xml exists data passes tests (BNU thetao issue resolved)
  if ( ('MODEL' in l) and ('EXPERIMENT' in exp) and ('VARIABLE' in var) ):
      print "".join(['** Known problem file, skipping annual calculation for ',l])
      continue
  elif ('MIROC4h' in l):
      print "".join(['** MIROC4h file, nyrs set to 5 (not 25) for ',l])
      nyrs = 5 ; # Test if ok
  else:
      nyrs = 25 ; #10 ok, 25 CCSM4.historical.r2i1p1.mo.so.ver-v20120409 fails, mergeTime=duplicate/overlapping time issue
      print "".join([str(nyrs),'yr annual calculation for ',l]); #continue

  # Open file and get times
  f = cdms.open(os.path.join(indir,l))
  t = f[var].getTime()
  c = t.asComponentTime()
  # Extract info from filename
  mod = string.split(l,'.')[1]
  run = string.split(l,'.')[3]
  ver = string.split(l,'.')[6] ; # ver=7 in cmip5
  # Create output directories
  try:
    os.makedirs(os.path.join(outdir,mod,run,ver)) ; # was (outdir,mod)
  except:
    pass
  # Create year indexes
  yrs = []
  for cc in c:
    yr = cc.year
    if yr not in yrs:
        yrs.append(yr) 
  # Set start year
  yrsX = [c[0].year]
  # Set start years
  for i in range(len(yrs)):
     yr = i*nyrs
     if (c[0].year + yr not in yrsX) and (c[0].year + yr in yrs):
       yrsX.append(c[0].year + yr)
  # Add last year to yrsX
  if c[len(c)-1].year not in yrsX:
    yrsX.append(c[len(c)-1].year)
  n = 0
  for i in range(len(yrsX)-1): 
    lb = yrsX[n]
    ub = yrsX[n+1]
    n = n + 1 ; # Increment counter
    # Create output filename - pay attention to last year
    if ub == yrsX[-1]:
	#cmip5.CNRM-CM5.historical.r1i1p1.an.so.ver-v20111021.1850-2005.nc - example
        fout = "".join(['cmip5.',mod,'.',exp,'.',run,'.an.',var,'.',ver,'.',format(lb,"04d"),'-',format(ub,"04d"),'.nc'])
    else:
        fout = "".join(['cmip5.',mod,'.',exp,'.',run,'.an.',var,'.',ver,'.',format(lb,"04d"),'-',format(ub-1,"04d"),'.nc'])
    #continue
    # Check if file already exists and write
    if os.path.isfile(os.path.join(outdir,mod,run,ver,fout)) == False:
      # Report if file is being processed
      print "".join(['** Outfile: ',fout,' being processed **'])
      logfile_handle = open(logfile,'a')
      logfile_handle.write("".join(['** Outfile: ',fout,' being processed **\n']))
      logfile_handle.close()
      # Debugging code
      d = f[var]
      print cdtime.comptime(lb),cdtime.comptime(ub)
      print d.getTime().mapInterval((cdtime.comptime(lb),cdtime.comptime(ub)))
      logfile_handle = open(logfile,'a')
      logfile_handle.write("".join([str(cdtime.comptime(lb)),' ',str(cdtime.comptime(ub)),'\n']))
      logfile_handle.write("".join([str(d.getTime().mapInterval((cdtime.comptime(lb),cdtime.comptime(ub)))),'\n']))
      logfile_handle.close()
      del(d); gc.collect()
      # Read data considering bounds
      if ub == yrsX[-1]:
          d = f(var,time=(cdtime.comptime(lb),cdtime.comptime(ub))) ; # Include last year
      else:
          d = f(var,time=(cdtime.comptime(lb),cdtime.comptime(ub),'co')) ; # Exclude last year
      print "".join(['** Processing annual means for ',str(lb),' to ',str(ub),' **'])
      print d.shape
      print d.getTime()
      t = d.getTime()
      mon = 1
      for ind,val in enumerate(t):
        if ind == 0:
          print [format(ind,'03d'),format(mon,'02d'),t.asComponentTime()[ind]]
          logfile_handle = open(logfile,'a')
          logfile_handle.write("".join(['Start: ',str([format(ind,'03d'),format(mon,'02d'),t.asComponentTime()[ind]]),'\n']))
          logfile_handle.close()
        elif ind == d.shape[0]-1:
          print [format(ind,'03d'),format(mon,'02d'),t.asComponentTime()[ind]]
          logfile_handle = open(logfile,'a')
          logfile_handle.write("".join(['Start: ',str([format(ind,'03d'),format(mon,'02d'),t.asComponentTime()[ind]]),'\n']))
          logfile_handle.close()
        mon = mon + 1
        if mon == 13:
          mon = 1
      cdutil.setTimeBoundsMonthly(d) ; # Correct CCSM4 bounds
      dan = cdutil.YEAR(d)
      dan = dan.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
      print "".join(['Start time: ',str(lb),' End time: ',str(ub),' input shape: ',str(d.shape),' output shape: ',str(dan.shape)])
      logfile_handle = open(logfile,'a')
      logfile_handle.write("".join(['Start time: ',str(lb),' End time: ',str(ub),' input shape: ',str(d.shape),' output shape: ',str(dan.shape),'\n']))
      logfile_handle.close()
      g = cdms.open(os.path.join(outdir,mod,run,ver,fout),'w+')
      # Copy across global attributes 
      att_keys = f.attributes.keys()
      att_dic = {}
      for i in range(len(att_keys)):
          att_dic[i]=att_keys[i],f.attributes[att_keys[i]]
          to_out = att_dic[i]
          setattr(g,to_out[0],to_out[1])
      g.write(dan)
      g.close()
      # Cleanup
      del(d,dan,g,att_keys,att_dic,to_out)
      gc.collect()
    else:
      print "".join([os.path.join(outdir,mod,run,fout),' already exists.. Skipping to next model.. '])
      logfile_handle = open(logfile,'a')
      logfile_handle.write("".join(['** Outfile: ',os.path.join(outdir,mod,run,fout),' already exists.. Skipping to next model.. **\n']))
      logfile_handle.close()
  #for i in
  f.close()
  # Cleanup
  del(f,t,c,mod,run,ver,yrs,yr,yrsX,nyrs)
  gc.collect()
#for l in lst

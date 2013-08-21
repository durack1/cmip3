# -*- coding: utf-8 -*-
"""
Created on Tue Jan 31 13:16:50 2012

Paul J. Durack 30th January 2012

This script builds trends and 50-yr climatologies from annual files

PJD 31 Jan 2012     - Began writing
PJD  1 Feb 2012     - Finalised code to write without looping, quick!
PJD  3 Feb 2012     - Added log code and temporal specifics dependent on experiment
PJD 10 Feb 2012     - Added log code, created additional out dir an_trends
PJD 10 Feb 2012     - Problem with temporal index for /work/durack1/Shared/cmip5/rcp45/atm/an/pr/cmip5.HadCM3.rcp45.r10i1p1.an.pr.ver-v20110905.2006-2035.nc
PJD 16 Feb 2012     - Latest run (120216_061434) produced 349 *an_trend* output files increased from 308 (120210_114950)
PJD  2 Mar 2012     - an_trends nc files 120302_1443: 482
                      [durack1@crunchy cmip5]$ ls */*/an_trends/*/*.nc | wc -l
PJD  2 Mar 2012     - Updated uvcdat path
PJD  2 Mar 2012     - an_trends nc files 120302_2108: 693
PJD  9 Mar 2012     - an_trends nc files 120309_1247: 751
PJD 18 Apr 2012     - Added count and purge code
                      an_trends nc files 120418_1257: 751
PJD 18 Apr 2012     - Added institution global att
PJD 18 Apr 2012     - Added exclusion of variables and experiments (not amip)
PJD 15 Jun 2012     - Added filelist2, which searches for an/*/*.xml files - 3D annual means - filelist1 & 2 then added to filelist
PJD 16 Jun 2012     - Corrected issue with 3D.xml input files, output file must have extension *.nc (not *.xml) to write, otherwise an error is returned
PJD 18 Jun 2012     - Changed var_mean and var_change* fields to float32 (are float64), reorder so var_mean is written then var_change*
PJD 18 Jun 2012     - Added dob global_atts to files from source file, version info should copy, global atts are then rewritten if required
PJD 18 Jun 2012     - Added 'experiment' argument to parallelise runs
PJD 13 Jul 2012     - Added additional variables to exclude
PJD 30 Jul 2012     - Like rcp check, added time check to historical - files which don't include 1950-2000 period fallover
PJD 21 Aug 2012     - Added historicalNat to valid experiment list
PJD 21 Nov 2012     - Added checks to correct units (salinity files CCSM4 & CESM)
PJD 21 Nov 2012     - Added time slicing as memory was an issue for 3D fields
PJD 22 Nov 2012     - 'cob' was returning a 51,depth,y,x array, so changed end bounds from 2000->1999 and 2100->2099
PJD 22 Nov 2012     - Added d load even if not a so/sos variable - corrected to load as well if doesn't satisfy units test
PJD 22 Nov 2012     - TOCHECK: If issues appear with missing masks, set missing as float32: $VAR.setMissing(numpy.float32(1e20))
PJD 28 Nov 2012     - Magnitude of trends is too small - rewrote time_axis for passing to linearregression
PJD 28 Nov 2012     - Tidied up a bunch of explicit calls, and resolves non-loaded module issues (when testing use notation [-2:])
PJD 29 Nov 2012     - Coped from /work/durack1/Shared/cmip5 and updated input
PJD 31 Jul 2013     - Added oceanonly to known hosts
PJD  5 Aug 2013     - Updated cdms info compression etc
PJD  5 Aug 2013     - Replaced all units issues with fixVarUnits function
PJD  5 Aug 2013     - Updated to use fixInterpAxis
PJD  6 Aug 2013     - Replaced log info with writeToLog
PJD  6 Aug 2013     - Added xml -> nc outfile name conversion for 1pct* and sres* experiments
                    - TODO: Add calendar attribute to newd variable before passing to linearregression
                    - TODO: Consider just changing time axis and saving memory usage, rather than duplicating variable
                    - TODO: Add depth and calculated mean eta variables to sigma (terrain-following) files - cmip5.inmcm4; cmip5.MIROC*

@author: durack1
"""
import cdutil,os,datetime,time,glob,re,sys,gc,argparse
import cdms2 as cdm
from durolib import fixInterpAxis,fixVarUnits,globalAttWrite,writeToLog
from genutil.statistics import linearregression
from socket import gethostname
from string import replace
#from numpy.core import shape
#from matplotlib.pyplot import pause

# Set netcdf file criterion - turned on from default 0s
cdm.setCompressionWarnings(0) ; # Suppress warnings
cdm.setNetcdfShuffleFlag(0)
cdm.setNetcdfDeflateFlag(1)
cdm.setNetcdfDeflateLevelFlag(9)
# Hi compression: 1.4Gb file ; # Single salt variable
# No compression: 5.6Gb ; Standard (compression/shuffling): 1.5Gb ; Hi compression w/ shuffling: 1.5Gb
cdm.setAutoBounds(1) ; # Ensure bounds on time and depth axes are generated

start_time = time.time() ; # Set time counter

# Set conditional whether files are created or just numbers are calculated
parser = argparse.ArgumentParser()
parser.add_argument('experiment',metavar='str',type=str,help='including \'experiment\' as a command line argument will select one experiment to process')
args = parser.parse_args()
if (args.experiment in ['all','1pctto2x','1pctto4x','20c3m','picntrl','sresa1b','sresa2','sresb1']):
   experiment = args.experiment ; # 1 = make files
   all_files = False ; # Preset variable before testing
   if 'all' in experiment:
       print "".join(['** Processing files from ALL experiments for *ClimAndSlope.nc file generation **'])
       all_files = True
   else:
       print "".join(['** Processing files from ',experiment ,' for *ClimAndSlope.nc file generation **'])
else:
   print "** No valid experiment specified - no *.nc files will be written **"
   sys.exit()
'''
all_files = False
experiment = 'historical'
'''

# Set host information and directories
host_name = gethostname()
if host_name in {'crunchy.llnl.gov','oceanonly.llnl.gov'}:
    trim_host = replace(host_name,'.llnl.gov','')
    host_path = '/work/durack1/Shared/cmip3/' ; # crunchy 121129
    cdat_path = '/usr/local/uvcdat/latest/bin/'
else:
    print '** HOST UNKNOWN, aborting.. **'
    sys.exit()

# Set logfile attributes
time_now = datetime.datetime.now()
time_format = time_now.strftime("%y%m%d_%H%M%S")
logfile = os.path.join(host_path,"".join([time_format,'_make_cmip3_trendsAndClims-',experiment,'-',trim_host,'.log']))
writeToLog(logfile,"".join(['TIME: ',time_format,'\n']))
writeToLog(logfile,"".join(['HOSTNAME: ',host_name,'\n']))

# Get list of infiles (*.nc) and 3D (*.xml)
# Get list of infiles (xml)
if all_files:
    filelist1 = glob.glob("".join([host_path,'*/*/an/*/*.nc']))
    filelist2 = glob.glob("".join([host_path,'*/*/an/*/*.xml']))
else:
    filelist1 = glob.glob("".join([host_path,experiment,'/*/an/*/*.nc']))
    filelist2 = glob.glob("".join([host_path,experiment,'/*/an/*/*.xml']))

filelist = list(filelist1)
filelist.extend(filelist2) ; filelist.sort()
del(filelist1,filelist2)
gc.collect()
# Trim out variables and experiments of no interest
vars_atm_exclude = ['evspsbl','hfls','hfss','hurs','huss','prw','ps','psl','rlds','rlus','rsds',
                    'rsus','sfcWind','tasmax','tasmin','tauu','tauv','ts','uas','vas']
vars_ocn_exclude = ['soga','zos']
vars_exclude	 = ['sci','rsdscs','ua','rlutcs','prc','rsuscs','ta','wap''cl','hur',
                     'rlds','sbl','rsdt','zg','clt','hus','mc','rsut','rlut','rsutcs',
                     'rsus','va','rldscs','cli','clw']
vars_exclude.extend(vars_atm_exclude) ;
vars_exclude.extend(vars_ocn_exclude) ; vars_exclude.sort()

exps_exclude    = ['amip'] ; exps_exclude.sort()
# Purge entries matching atm_vars_exclude by index
i = 0
filelist2 = []
for file in filelist:
    if all_files:
        if not (file.split('/')[8] in vars_exclude) and not (file.split('/')[5] in exps_exclude):
            filelist2.insert(i,file)
            i = i + 1
    else:
        if not (file.split('/')[8] in vars_exclude) and (file.split('/')[5] in experiment):
            filelist2.insert(i,file)
            i = i + 1

del(filelist,i,file)
filelist = filelist2
del(filelist2)
gc.collect()

# Clean up lists
del(vars_atm_exclude,vars_ocn_exclude,vars_exclude,exps_exclude); gc.collect()

# Report total file count to logfile
if 'logfile' in locals():
    writeToLog(logfile,"".join([host_path,': ',format(len(filelist),"06d"),' nc files found to process\n']))

# Count and purge code
# Deal with existing *.nc files
if all_files:
    ii,o,e = os.popen3("".join(['ls ',host_path,'*/*/an_trends/*/*.nc | wc -l']))
else:
    ii,o,e = os.popen3("".join(['ls ',host_path,experiment,'/*/an_trends/*/*.nc | wc -l']))
nc_count = o.read();
print "".join(['** Purging ',nc_count.strip(),' existing *.nc files **'])
writeToLog(logfile,"".join(['** Purging ',nc_count.strip(),' existing *.nc files **\n']))
if all_files:
    cmd = "".join(['rm -f ',host_path,'*/*/an_trends/*/*.nc'])
else:
    cmd = "".join(['rm -f ',host_path,experiment,'/*/an_trends/*/*.nc'])
# Catch errors with system commands
ii,o,e = os.popen3(cmd) ; # os.popen3 splits results into input, output and error - consider subprocess function in future
print "** *.nc files purged **"
writeToLog(logfile,"** *.nc files purged **\n")
print "** Generating new *.nc files **"
writeToLog(logfile,"** Generating new *.nc files **\n")

filecount = 0
# Loop through files - 1030 is middle of CCSM4.historical testing; 268:300 CCSM4.rcp60 testing
for l in filelist:
    filecount = filecount + 1; filecount_s = '%06d' % filecount
    print "".join(["** Processing: ",l," **"])
    var = l.split('/')[8] ; # Get variable name from filename
    f_in = cdm.open(l) ; # Open file
    d = f_in[var] ; # Create variable object - square brackets indicates cdms "file object" and it's associated axes
    # Determine experiment
    experiment = l.split('/')[-1].split('.')[2]
    time_calc_start = time.time()
    if experiment == 'picntrl':
        # Case of piControl files, need to consider spawning time of subsequent experiment
        logtime_now = datetime.datetime.now()
        logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
        time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start       
        writeToLog(logfile,"".join(['** ',filecount_s,': ',logtime_format,' ',time_since_start_s,'s; piControl file skipped        : ',l,' **\n']))       
        continue
    elif experiment.__contains__('pctto'):
        d = f_in(var)
        # Check units and correct in case of salinity
        if var == 'so' or var == 'sos':
            [d,_] = fixVarUnits(d,var,True)

        # Create ~50-yr linear trend - with period dependent on experiment
        #(slope,intercept),(slope_err,intercept_err) = linearregression(d(),error=1)
        (slope),(slope_err) = linearregression(fixInterpAxis(d),error=1,nointercept=1)
        
        slope = slope.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
        slope_err = slope_err.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
        slope.comment = 'start-end change'
        # Create ~50-yr mean climatology
        clim = cdutil.YEAR.climatology(d())
        del(d) ; gc.collect()
        clim = clim.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
        clim.comment = 'start-end climatological mean'
        outfile = re.sub("[0-9]{4}-[0-9]{4}","start-end_ClimAndSlope",l)
        outfile = re.sub(".xml",".nc",outfile) ; # Correct for 3D an.xml files
    elif experiment == '20c3m':
        # Try for historical as some data doesn't extend 1950-2000        
        try:
            d = f_in(var,time=("1950","1999","cob"))
            # Check units and correct in case of salinity
            if var == 'so' or var == 'sos':
                [d,_] = fixVarUnits(d,var,True)

            #(slope,intercept),(slope_err,intercept_err) = linearregression(d,error=1)
            (slope),(slope_err) = linearregression(fixInterpAxis(d),error=1,nointercept=1)
            #slope,slope_err = statistics.linearregression(d(time=("1950","2000","cob")),error=1,nointercept=1)
            
            slope = slope.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
            slope_err = slope_err.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
            slope.comment = '1950-1999 change'
            clim = cdutil.YEAR.climatology(d)
            del(d) ; gc.collect()
            clim = clim.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
            clim.comment = '1950-1999 climatological mean'
            outfile = re.sub("[0-9]{4}-[0-9]{4}","1950-2000_ClimAndSlope",l)
            outfile = re.sub(".xml",".nc",outfile) ; # Correct for 3D an.xml files
        except:
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            writeToLog(logfile,"".join(['** ',filecount_s,': ',logtime_format,' ',time_since_start_s,'s; PROBLEM file skipped          : ',l,' **\n']))
            continue            
    elif experiment.__contains__('sres'):
        # Try for rcp's as some data doesn't extend 2050-2099
        try:
            d = f_in(var,time=("2050","2099","cob"))
            # Check units and correct in case of salinity
            if var == 'so' or var == 'sos':
                [d,_] = fixVarUnits(d,var,True)

            #(slope,intercept),(slope_err,intercept_err) = linearregression(d,error=1)
            (slope),(slope_err) = linearregression(fixInterpAxis(d),error=1,nointercept=1)

            slope = slope.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
            slope_err = slope_err.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
            slope.comment = '2050-2099 change'
            clim = cdutil.YEAR.climatology(d)
            del(d) ; gc.collect()
            clim = clim.astype('float32') ; # Recast from float64 back to float32 precision - half output file sizes
            clim.comment = '2050-2099 climatological mean'
            outfile = re.sub("[0-9]{4}-[0-9]{4}","2050-2100_ClimAndSlope",l)
            outfile = re.sub(".xml",".nc",outfile) ; # Correct for 3D an.xml files
        except:
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            writeToLog(logfile,"".join(['** ',filecount_s,': ',logtime_format,' ',time_since_start_s,'s; PROBLEM file skipped          : ',l,' **\n']))
            continue
    time_calc_end = time.time() - time_calc_start; time_calc_end_s = '%08.2f' % time_calc_end
    # Rename variables
    slope.id = "".join([var,'_change'])
    slope_err.id = "".join([var,'_change_error'])
    slope_err.comment = 'linear trend error'
    clim.id = "".join([var,'_mean'])
    # Create output file with new path
    outfile = re.sub('/an/','/an_trends/',outfile)
    # Check path exists
    if os.path.exists(os.sep.join(outfile.split('/')[0:-1])) != 1:
        os.makedirs(os.sep.join(outfile.split('/')[0:-1]))
    # Check file exists
    if os.path.exists(outfile):
        print "** File exists.. removing **"
        os.remove(outfile)
    f_out = cdm.open(outfile,'w')
    # Write new outfile global atts
    globalAttWrite(f_out,options=None) ; # Use function to write standard global atts to output file    
    # Copy across global attributes from source file - do this first, then write again so new info overwrites
    for i,key in enumerate(f_in.attributes.keys()):
        setattr(f_out,key,f_in.attributes.get(key))
    del(i,key) ; gc.collect()
    # Write to output file    
    f_out.write(clim) ; # Write clim first as it has all grid stuff
    f_out.write(slope)
    f_out.write(slope_err)
    # Close all files
    f_in.close()
    f_out.close()
    # Log success to file        
    logtime_now = datetime.datetime.now()
    logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
    time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start       
    writeToLog(logfile,"".join(['** ',filecount_s,': ',logtime_format,' ',time_since_start_s,'s; slope&clim: ',time_calc_end_s,'s; created: ',outfile,' **\n']))
    # Cleanup
    del(clim,experiment,filecount_s,logtime_format,logtime_now,outfile)
    del(slope,slope_err,time_calc_end,time_calc_end_s,time_calc_start)
    del(time_since_start,time_since_start_s,var)
    if 'd' in locals():
        del(d)
    # Garbage collection before next file iteration
    gc.collect()
# Log success to file
writeToLog(logfile,"** make_cmip3_trendAndClims.py complete **")
    
"""
# Plotting tips
vcs.init()
v.plot(dannmean)
i = v.createisofill()
levs = vcs.mkscale(32,38,20)
colors = vcs.getcolors(levs)
i.levels=levs
i.fillareacolors=colors
#v.clear(); v.close()
v.plot(dannmean,i)
i.list() ; # list all attributes which can be controlled    
"""
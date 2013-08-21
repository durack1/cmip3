#!uvcdat6
"""
Created on Mon Jan 30 10:26:47 2012

Paul J. Durack 30th January 2012

This script builds annual files

PJD 30 Jan 2012     - Finalised to create annual files from monthly means
PJD 31 Jan 2012     - Considering multithreading code, particularly for 3D file creation (demo code added and commented)
PJD  3 Feb 2012     - Added logging code and corrected format so that first and last year will always be 04d
PJD  3 Feb 2012     - Added code to report skipping 3D files, previously these were unreported
PJD  6 Feb 2012     - Added error reporting code, bombed on file:
                    /work/durack1/Shared/cmip5/rcp45/atm/mo/tas/cmip5.NorESM1-M.rcp45.r1i1p1.mo.tas.ver-v20110901.xml
PJD  6 Feb 2012     - Problem above due to no timeBounds, use cdutil.setTimeBoundsMonthly(var) to resolve issue
PJD  6 Feb 2012     - Converting d=f_in[var] to d=f_in(var) has improved cdutil.YEAR timing by between 10x and 2x,
                      this now explicitly reads the whole array into memory and calcs annual in memory
PJD  6 Feb 2012     - Added missing_value flag correction for d and ann variable
PJD  6 Feb 2012     - Added code to exclude non pr/tas historical data
PJD  6 Feb 2012     - Using latest run 528 files are contained in filelist 120206 1635
PJD  7 Feb 2012     - Have another go at dealing with grids and missing data
PJD  8 Feb 2012     - CDAT Issues too numerous with grids and variable attributes, so wrote code to skip any
                      write issues and skip to next file in loop. Issue with creating:
                      /work/durack1/Shared/cmip5/1pctCO2/ocn/an/sos/cmip5.GFDL-ESM2M.1pctCO2.r1i1p1.an.sos.ver-v1.0001-0200.nc
                      logged to bugzilla: http://uv-cdat.llnl.gov/bugzilla/show_bug.cgi?id=64
PJD 14 Feb 2012     - Charles implemented a type casting fix for bug64, uvcdat/latest includes this fix
PJD 15 Feb 2012     - [durack1@crunchy cmip5]$ ls */*/an/*/*.nc | wc -l = 394 (pre-rerunning on new xmls)
PJD 15 Feb 2012     - Added some gc.collect() calls to tighten memory usage
PJD 15 Feb 2012     - Added code to reapply d.attributes to new ann variable (same with global atts)
PJD 15 Feb 2012     - [durack1@crunchy cmip5]$ ls */*/an/*/*.nc | wc -l = XXX (post-rerunning on new xmls)
PJD 16 Feb 2012     - Considerable file cleanup, due to UVCDAT6.0beta bug fixes resolving many cludgy partially implemented
                      code below
PJD 16 Feb 2012     - Added writing of "completion" logfile statement
PJD 16 Feb 2012     - Latest run (120215_163849) produced 436 *an* output files increased from 394 (120208_162520)
PJD 19 Feb 2012     - Problem using cdutil.YEAR on cmip5.MIROC-ESM.historical.r1i1p1.mo.tas.ver-v20110929.xml - time axis issue?
PJD 19 Feb 2012     - Added try-except around cdutil.YEAR to catch above issue
PJD 19 Feb 2012     - 120219_090047: 571 an*.nc files generated
PJD 28 Feb 2012     - an_nc count 120228_2147: 571  120229_xxxx:
                      [durack1@crunchy cmip5]$ ls */*/an/*/*.nc | wc -l
PJD  2 Mar 2012     - Updated cdat_path as ['/bin/sh: /usr/local/cdat/2012-01-12/bin/cdscan: No such file or directory\n']
PJD  2 Mar 2012     - an nc count 120302_0921: 575
PJD  8 Mar 2012     - an nc count 120308_1851: 784
PJD  9 Mar 2012     - an nc count 120309_1052: 846
PJD 18 Mar 2012     - Added 'zos' to xml creation, so added 'zos' to the vars_exclude list below
PJD 16 Apr 2012     - Updated to log info for nc_good & nc_bad files
PJD 17 Apr 2012     - Updated nc_$ counters to 0, added try statement for file read due to axis length issues with
                    /work/durack1/Shared/cmip5/rcp26/atm/mo/tas/cmip5.CCSM4.rcp26.r1i1p1.mo.tas.ver-v20120203.xml
PJD 18 Apr 2012     - Moved nc_good counter increment, weird it's reporting 3133, when 1453 files exist and filecount is 2091
PJD 30 May 2012     - Added MRRO/MRROS so that annual total, not mean is calculated
PJD  7 Jun 2012     - Added fx fields to var exclusion list to fix code falling over due to no 'time' attribute
PJD  7 Jun 2012     - Converted code to take experiment arg to parallelise across experiments
PJD 16 Jun 2012     - Added xml_path to source new cron xml files - needed to update file.split calls from 8 to 6
PJD 17 Jun 2012     - Added correction to outfile path, so /work/cmip5/ in is converted to /work/durack1/Shared/cmip5/
PJD 17 Jun 2012     - Calculated an files were being written at float64 precision - recast back to float32, reducing file sizes by half
PJD 18 Jun 2012     - TOCHECK: If issues appear with missing masks, set missing as float32: $VAR.setMissing(numpy.float32(1e20))
PJD 10 Jul 2012     - Issues with agessc files, removed agessc + others and 3D files from processing loop
PJD 10 Jul 2012     - Added exclusion list for each realm (atm, land, ocn, seaIce)
PJD 11 Jul 2012     - Added hus,hur,ta,ua,va to atm exclusion list, should do so for all 3D atm/ocn vars
PJD 25 Jul 2012     - Added clisccp to excluded vars
PJD 25 Jul 2012     - There appears no need to reorder realms, outfile is generated from the xml name, so all will be propagated
PJD 16 Aug 2012     - Added check for rsds/ocn 3Dvariable as this is causing the script to bomb:
		      /work/cmip5/historical/ocn/mo/rsds/cmip5.GFDL-ESM2M.historical.r1i1p1.mo.ocn.rsds.ver-1.xml
PJD 19 Aug 2012     - Fixed issue with mo_new files being indexed (piControl) '_new/' string being searched and excluded
PJD 20 Aug 2012     - Added historicalNat to valid experiment list
PJD 16 Oct 2012     - Converted across the cmip3, renamed experiments
PJD 17 Oct 2012     - Updated re.sub to write to cmip3, not cmip5 and Shared
PJD 17 Oct 2012     - Added 'wo' variable to ocn_vars exclusion list - cgcm3.1_t63 was blowing up 96Gb
PJD 19 Oct 2012     - Updated 'mo' rename statement to no affect 'ukmo' -> 'ukan'
PJD 23 Oct 2012     - Corrected an/mo with leading folder '/'
PJD 23 Oct 2012     - Corrected issue with file years not agreeing outfile log created
PJD 23 Oct 2012     - Removed del() statement for variables which don't exist
PJD 24 Oct 2012     - Further removal of del() variables
PJD 25 Oct 2012     - Added additional entry to deal with /mo -> /an and .mo. -> .an. (ukmo files causing grief)
PJD  2 Jun 2013     - Cleaned up code and added oceanonly to known hosts
PJD  2 Jun 2013     - TODO: Consider adding firstyr/lastyr as global attributes to speed up branch_time code
PJD  2 Jun 2013     - TODO: historical taking too long to process, chop further into realms
PJD  2 Jun 2013     - TODO: Start correcting issued files, use /work/cmip5/12xxxx_cmip5_pathologies.txt to document and deal with these
PJD  2 Jun 2013     - TODO: Log counter broken, check logging code and fix
PJD  2 Jun 2013     - TODO: Consider reporting and logging memory usage resource.getrusage(resource.RUSAGE_SELF).ru_maxrss should return this
PJD  2 Jun 2013     - TODO: Consider multiplying WFO/PR by 86400 to get to correct comparable units (cmip3) code commented below
"""

import os,sys,datetime,time,glob,cdutil,cdat_info,pytz,gc,re,argparse
from socket import gethostname
from string import replace
import cdms2 as cdms

# Set nc classic as outputs
cdms.setNetcdfShuffleFlag(0)
cdms.setNetcdfDeflateFlag(0)
cdms.setNetcdfDeflateLevelFlag(0)

start_time = time.time() ; # Set time counter

# Set conditional whether files are created or just numbers are calculated
parser = argparse.ArgumentParser()
parser.add_argument('experiment',metavar='str',type=str,help='including \'experiment\' as a command line argument will select one experiment to process')
args = parser.parse_args()
if (args.experiment in ['all','1pctto2x','1pctto4x','20c3m','picntrl','sresa1b','sresa2','sresb1']):
   experiment = args.experiment ; # 1 = make files
   all_files = False ; # Preset variable before testing
   if 'all' in experiment:
       print "".join(['** Processing *.xml files from ALL experiments for *an.nc file generation **'])
       all_files = True
   else:
       print "".join(['** Processing *.xml files from ',experiment ,' for *an.nc file generation **'])
else:
   print "** No valid experiment specified - no *.nc files will be written **"
   sys.exit()

# Set directories
host_name = gethostname()
if host_name in {'crunchy.llnl.gov','oceanonly.llnl.gov'}:
    trim_host = replace(host_name,'.llnl.gov','')
    host_path 	= '/work/durack1/Shared/cmip3/' ; # crunchy 121016
    xml_path 	= '/work/cmip3/' ; # crunchy 121016
    xml_count	= 6
    cdat_path 	= '/usr/local/uvcdat/latest/bin/'
else:
    print '** HOST UNKNOWN, aborting.. **'
    sys.exit()

# Set logfile attributes
time_now = datetime.datetime.now()
time_format = time_now.strftime("%y%m%d_%H%M%S")
logfile = os.path.join(host_path,"".join([time_format,'_make_cmip3_an-',experiment,'-',trim_host,'.log']))
# Create logfile
if 'logfile' in locals():
    logfile_handle = open(logfile,'w')
    logfile_handle.write("".join(['TIME: ',time_format,'\n']))
    logfile_handle.write("".join(['HOSTNAME: ',host_name,'\n']))
    logfile_handle.close()

# Get list of infiles (xml)
if all_files:
    filelist = glob.glob("".join([xml_path,'*/*/*/*/*.xml'])) ; filelist.sort()
else:
    filelist = glob.glob("".join([xml_path,experiment,'/*/*/*/*.xml'])) ; filelist.sort()

# Trim out variables of no interest
vars_exclude_atm = ['cl','cli','clisccp','clt','clw','hur','hus','mc','prc','prsn','rldscs','rlut','rlutcs',
		    'rsdscs','rsdt','rsuscs','rsut','rsutcs','sbl','sci','ta','ua','va','wap','zg']
vars_exclude_fx = ['areacella','areacello','basin','deptho','orog','sftlf','sftof','volcello']
vars_exclude_land = []
vars_exclude_ocn = ['agessc','cfc11','evs','ficeberg','friver','mlotst','omlmax','rhopoto',
		    'sfriver','so','thetao','uo','vo','wo','vsf','vsfcorr','vsfevap','vsfpr',
		    'vsfriver','wfonocorr']
vars_exclude_seaIce = []
# Generate master list from sublists
vars_exclude = list(vars_exclude_atm)
vars_exclude.extend(vars_exclude_fx)
vars_exclude.extend(vars_exclude_land)
vars_exclude.extend(vars_exclude_ocn)
vars_exclude.extend(vars_exclude_seaIce) ; vars_exclude.sort()
# Purge entries matching atm_vars_exclude by index
i = 0
filelist2 = []
for file in filelist:
    if not ( (file.split('/')[xml_count] in vars_exclude) or ('_new/' in file) ):
        filelist2.insert(i,file)
        i = i + 1

del(filelist,file,vars_exclude,i)
filelist = filelist2
del(filelist2)
gc.collect()

# Report total file count to logfile
if 'logfile' in locals():
    logfile_handle = open(logfile,'a')
    logfile_handle.write("".join([xml_path,': ',format(len(filelist),"06d"),' xml files found to process\n']))
    logfile_handle.close()

filecount = 0
# Loop through files
# Test suite 120207_1334: rcp26[316:321]; rcp26.mo.tos - HadGEM2-ES(316), MPI-ESM-LR(317), NorESM1-M
# Test suite 120208_1623: cmip5.GFDL-ESM2M.1pctCO2.r1i1p1.an.sos.ver-v1.0001-0200.nc [20:21]
# Test suite 120219_0838: cmip5.MIROC-ESM.historical.r1i1p1.mo.tas.ver-v20110929.xml [254] - bombed on cdutil.YEAR

# Count and purge code
# Create counters for nc_good and nc_bad
nc_good = 0; nc_bad1 = 0; nc_bad2 = 0; nc_bad3 = 0; nc_bad4 = 0;
# Deal with existing *.nc files
if all_files:
    exp_path = '*/*/an/*/*.nc'
else:
    exp_path = "".join([experiment,'/*/an/*/*.nc'])
ii,o,e = os.popen3("".join(['ls ',host_path,exp_path,' | wc -l']))
nc_count = o.read();
print "".join(['** Purging ',nc_count.strip(),' existing *.nc files **'])
logfile_handle = open(logfile,'a')
logfile_handle.write("".join(['** Purging ',nc_count.strip(),' existing *.nc files **\n']))
logfile_handle.close()
cmd = "".join(['rm -f ',host_path,exp_path])
# Catch errors with system commands
ii,o,e = os.popen3(cmd) ; # os.popen3 splits results into input, output and error - consider subprocess function in future
print "** *.nc files purged **"
logfile_handle = open(logfile,'a')
logfile_handle.write("** *.nc files purged **\n")
logfile_handle.close()
print "** Generating new *.nc files **"
logfile_handle = open(logfile,'a')
logfile_handle.write("** Generating new *.nc files **\n")
logfile_handle.close()

for l in filelist: # test [1265:1500] 120417 - cmip5.CCSM4.rcp26.r1i1p1.mo.tas.ver-v20120203
    filecount = filecount + 1; filecount_s = '%07d' % filecount
    print "".join(["** Processing: ",l," **"])
    #continue
    var = l.split('/')[xml_count] ; # in path var is 6th indexed
    realm = l.split('/')[xml_count-2] ; # in path realm is 4th indexed
    if var in ['rsds','so','thetao','uo','vo'] and realm in 'ocn':
        print "** NOT CREATING 3D ocean files: so/thetao/uo/vo file found and breaking to next loop entry.. **"
        # Log skip to file
        if 'logfile' in locals():
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            logfile_handle = open(logfile,'a')
            logfile_handle.write("".join(['** ',filecount_s,': ',logtime_format,' ',time_since_start_s,'s; 3D FILE ENCOUNTERED,      skipped: ',l,' **\n']))
            logfile_handle.close()
            # Cleanup
            del(logtime_now,logtime_format,time_since_start,logfile_handle)
        del(var,filecount_s)
        gc.collect()
        continue
    # Open file
    f_in = cdms.open(l)
    # Read variable - square brackets indicates "file object", parentheses indicates variable object
    try:
       d = f_in(var)
    except:
        # Report failure to logfile
        print "** PROBLEM with: " + l + " found and breaking to next loop entry.. **"
        nc_bad1 = nc_bad1 + 1;
        if 'logfile' in locals():
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            logfile_handle = open(logfile,'a')
            err_text = 'PROBLEM 1 (read var error - ann calc failed) creating '
            logfile_handle.write("".join(['** ',format(nc_bad1,"07d"),': ',logtime_format,' ',time_since_start_s,'s; ',err_text,l,' **\n']))
            logfile_handle.close()
        continue
  
    # Explicitly set timeBounds - problem with cmip5.NorESM1-M.rcp45.r1i1p1.mo.tas.ver-v20110901.xml
    cdutil.setTimeBoundsMonthly(d)
    # Get time dimension and convert to component time
    dt = d.getTime()
    dtc = dt.asComponentTime()
    dfirstyr = dtc[0].year
    dlastyr = dtc[-1].year
    # Use cdutil averager functions to generate annual means
    print "** Calculating annual mean **"
    time_anncalc_start = time.time()
    try:
        ann = cdutil.YEAR(d) # ann variable loses d.attribute values, need to recreate from d
        ann = ann.astype('float32') ; # Recast from float64 back to float32 precision
        nc_good = nc_good + 1 ;
    except:
        # Report failure to logfile
        print "** PROBLEM with: " + l + " found and breaking to next loop entry.. **"
        nc_bad2 = nc_bad2 + 1;
        if 'logfile' in locals():
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            logfile_handle = open(logfile,'a')
            err_text = 'PROBLEM 2 (cdutil.YEAR error - ann calc failed) creating '
            logfile_handle.write("".join(['** ',format(nc_bad2,"07d"),': ',logtime_format,' ',time_since_start_s,'s; ',err_text,l,' **\n']))
            logfile_handle.close()
        continue
    time_anncalc_end = time.time() - time_anncalc_start; time_anncalc_end_s = '%08.2f' % time_anncalc_end
    print "** Annual mean calculated **"
    if var in ['pr','wfo','mrro','mrros']:
        print "** PR/WFO/MRRO/MRROS variable corrected for annual total, not mean **"
        ann*12. ; # Correct to annual sum
    
    """ 
    if var in 'wfo':
        ann*-86400 ; # Convert kg m-2 s-1 -> mm/day -> mm/month and into ocean as -ve/blue (wfo)
    if var in 'pr':
        ann*86400 ; # Convert kg m-2 s-1 -> mm/day -> mm/month
    print "** PR/WFO units corrected to mm/month **"
 
    from make_model_plots.m /work/durack1/csiro/Backup/110808/Y_dur041_linux/working/
    elseif strcmp(var,'wfo')
    varmat = (varmat*-86400).*gregorian_days; % Convert kg m-2 s-1 -> mm/day -> mm/month and into ocean as -ve/blue (wfo)
    elseif strcmp(var,'pr')
    varmat = (varmat*86400).*gregorian_days; % Convert kg m-2 s-1 -> mm/day -> mm/month        
    """    

    # Create check values for annual variable
    annt = ann.getTime()
    anntc = annt.asComponentTime()
    annfirstyr = anntc[0].year
    annlastyr = anntc[-1].year
    # Check ann and d yrs agree
    if annfirstyr == dfirstyr and annlastyr == dlastyr:
        # Create outfile name
        outfile = replace(replace(replace(l,'.xml',"".join(['.',format(annfirstyr,"04d"),'-',format(annlastyr,"04d"),'.nc'])),'/mo','/an'),'.mo.','.an.')
        # Correct from /work/cmip5/ to /work/durack1/Shared/cmip5/
        outfile = re.sub('/work/cmip3/','/work/durack1/Shared/cmip3/',outfile)
        # FOR TESTING - write the current working directory
        """
        outfile = re.sub('rcp26/ocn/an/tos/','',outfile)
        print "".join(['outfile: ',outfile])
        """
        # Check that outfile path exists
        dirend = outfile.rfind('/')
        if os.path.exists(outfile[:dirend+1]) != 1:
            # At first run create output directories
            os.makedirs(outfile[:dirend])
        if os.path.exists(outfile):
            print "".join(['** File exists.. removing: ',outfile,' **'])
            os.remove(outfile)
        # Open outfile
        f_out = cdms.open(outfile,'w')
        # Write variable attributes back out to new variable
        for k in d.attributes.keys():
            setattr(ann,k,d.attributes[k])
        # Write out file global atts
        for k in f_in.attributes.keys():
            setattr(f_out,k,f_in.attributes[k])
        history = getattr(f_in,'history')
        # Write new file global atts
        f_out.institution = "Program for Climate Model Diagnosis and Intercomparison (LLNL)"
        f_out.data_contact = "Paul J. Durack; pauldurack@llnl.gov; +1 925 422 5208"
        # Create timestamp, corrected to UTC for history
        local 		= pytz.timezone("America/Los_Angeles")
        time_now 	= datetime.datetime.now();
        local_time_now	= time_now.replace(tzinfo = local)
        utc_time_now 	= local_time_now.astimezone(pytz.utc)
        time_format 	= utc_time_now.strftime("%d-%m-%Y %H:%M:%S %p")
        f_out.history 	= "".join([history,'\n','File processed: ',time_format,' UTC; San Francisco, CA, USA'])
        f_out.host 	= "".join([host_name,'; CDAT version: ',"".join(["%s" % el for el in cdat_info.version()]),'; Python version: ',replace(replace(sys.version,'\n','; '),') ;',');')])
        # Write new variable atts
        ann.comment 	= "Converted to annual from monthly mean data"

        # Write data to file, if successful close infile and outfile
        try:
            f_out.write(ann)
            f_out.close()
            f_in.close() ; # Source file kept open so attributes can be copied across
            nc_good = nc_good + 1 ;
        except:
            print "file write bombed onto next in loop"
            f_out.close()
            f_in.close()
            nc_bad4 = nc_bad4 + 1 ;
            if os.path.exists(outfile):
                print "".join(['** File exists.. removing: ',outfile,' **'])
                os.remove(outfile)
            # Report failure to logfile
            if 'logfile' in locals():
                logtime_now = datetime.datetime.now()
                logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
                time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
                logfile_handle = open(logfile,'a')
                err_text = 'PROBLEM 4 (f_out.write error - nc creation failed) creating '
                logfile_handle.write("".join(['** ',format(nc_bad3,"07d"),': ',logtime_format,' ',time_since_start_s,'s; ',err_text,outfile,' **\n']))
                logfile_handle.close()
            continue

        # Log success to file
        if 'logfile' in locals():
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            logfile_handle = open(logfile,'a')
            logfile_handle.write("".join(['** ',filecount_s,': ',logtime_format,' ',time_since_start_s,'s; cdutil.YEAR(): ',time_anncalc_end_s,'s; created: ',outfile,' **\n']))
            logfile_handle.close()
            # Cleanup
            del(logtime_now,logtime_format,time_since_start,time_since_start_s,logfile_handle)
        del(ann,annfirstyr,annlastyr,dfirstyr,dirend,dlastyr,el,filecount_s,history,k,local_time_now,outfile)
        del(time_anncalc_end,time_anncalc_end_s,time_anncalc_start,time_format,time_now)
        del(utc_time_now,var)
        # Garbage collection before next file iteration
        gc.collect()
    else:
        print "** Error in annual file creation - first/last year of output != first/last year of input **\n"
        f_in.close() ; # Close input file if error
        outfile = replace(replace(l,'.xml',"".join(['.',format(annfirstyr,"04d"),'-',format(annlastyr,"04d"),'.nc'])),'/mo','/an')
        nc_bad3 = nc_bad3 + 1 ;
        # Log failure to file
        if 'logfile' in locals():
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            logfile_handle = open(logfile,'a')
            err_text = 'PROBLEM 3 (first/last year error - nc creation failed) creating '
            logfile_handle.write("".join(['** ',format(nc_bad2,"07d"),': ',logtime_format,' ',time_since_start_s,'s; ',err_text,outfile,' **\n']))            
            logfile_handle.close()
            # Cleanup
            del(logtime_now,logtime_format,time_since_start,time_since_start_s,logfile_handle)
        #del(ann,annfirstyr,annlastyr,dfirstyr,dirend,dlastyr,el,filecount_s,history,k,local_time_now,outfile)
        del(ann,annfirstyr,annlastyr,dfirstyr,dlastyr,filecount_s,outfile)
        #del(time_anncalc_end,time_anncalc_end_s,time_anncalc_start,time_format,time_now)
        del(time_anncalc_end,time_anncalc_end_s,time_anncalc_start)
        #del(utc_time_now,var)
        # Garbage collection before next file iteration
        gc.collect()

# Log success to file
# Create master list of xml_bad
nc_bad = nc_bad1+nc_bad2+nc_bad3+nc_bad4
if 'logfile' in locals():
    logfile_handle = open(logfile,'a')
    logfile_handle.write("** make_cmip5_annualMeans.py complete **\n")
    logfile_handle.write("".join(['** NC file count - Good: ',format(nc_good,"1d"),' **\n']))
    logfile_handle.write("".join(['** NC file count - Bad/skipped: ',format(nc_bad,"1d"),'; bad1 (read var error): ',format(nc_bad1,"1d"),'; bad2 (cdutil.YEAR error): ',format(nc_bad2,"1d"),'; bad3 (first/last year error): ',format(nc_bad3,"1d"),'; bad4 (f_out.write error): ',format(nc_bad4,"1d")]))
    logfile_handle.close()

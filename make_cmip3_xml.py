# -*- coding: utf-8 -*-
"""
Created on Mon Jan 23 09:40:47 2012

Paul J. Durack 5th June 2012

This script builds *.xml files for all "interesting" model variables

PJD  5 Jun 2012     - File copied from /work/durack1/Shared/cmip5/make_cmip5_xml.py
PJD  6 Jun 2012     - Added back in argparser - and makefiles argument
PJD  6 Jun 2012     - Added version '2' flag for data*/ sourced versions as per below
                    /ipcc/data5/sresa1b/ocn/mo/rhopoto/cccma_cgcm3_1_t63/run1
                    /ipcc/sresa1b/ocn/mo/rhopoto/cccma_cgcm3_1_t63/run1
PJD 10 Jun 2012     - Corrected issue with xml_count, now adding ints (rather than strs) - ints has no .strip method, converted to format
PJD 11 Jun 2012     - Decreased log counts to 1000 (was 10000)
PJD 12 Jun 2012     - Tested sizes and mod time for v2 & v1 files, if v1 exists, then don't write v2.. Unless v2 is newer or larger..
PJD 12 Jun 2012     - Corrected, removed fx_vars, land, ocn and seaIce from purge list - output directories are built off these variables
PJD 13 Jun 2012     - Updated to test v1 > v2, so csiro_mk3_5.20c3m duplicate data should now be removed - /work/cmip3/20c3m/ocn/mo/so is a good dir to check
PJD 18 Jun 2012     - Added 'rhopoto','uo','vo','wo' to ocn_vars
PJD 21 Feb 2012     - Added oceanonly to known hosts
PJD 21 Feb 2012     - TODO: Change all os.popen calls to subprocess, and parallelise xml file generation, 4-8 at once?

@author: durack1
"""

import os,sys,shutil,datetime,time,string,glob,gc,subprocess,argparse,re
from socket import gethostname
from collections import deque
from string import replace

# Set time counter and begin code
start_time = time.time() ; # Set time counter

# Set conditional whether files are created or just numbers are calculated
parser = argparse.ArgumentParser()
parser.add_argument('makefiles',metavar='str',type=str,help='including \'makefiles\' as a command line argument will ensure files are written')
args = parser.parse_args()
if (args.makefiles == 'makefiles'):
   make_xml = 1 ; # 1 = make files
   print "** Write mode - new *.xml files will be written **"
elif (args.makefiles == 'report'):
   make_xml = 0 ; # 0 = don't make files
   print "** Report mode - no *.xml files will be written **"

# Set directories
host_name = gethostname()
if host_name in {'crunchy.llnl.gov','oceanonly.llnl.gov'}:
    trim_host = replace(host_name,'.llnl.gov','')
    host_path = '/work/cmip3/' ; # crunchy 120605
    cdat_path = '/usr/local/uvcdat/latest/bin/'
else:
    print '** HOST UNKNOWN, aborting.. **'
    sys.exit()

# Change directory to host
os.chdir(host_path)

# Set logfile attributes
time_now = datetime.datetime.now()
time_format = time_now.strftime("%y%m%d_%H%M%S")
logfile = os.path.join(host_path,"".join([time_format,'_make_cmip3_xml-',trim_host,'.log']))
# Create logfile
logfile_handle = open(logfile,'w')
# Logging the explicit data path that is being searched
os.chdir('/ipcc')
cmd = 'df -h | grep ipcc'
o = os.popen(cmd).readlines()
oj = "".join(o)
logfile_handle.write("".join(['TIME: ',time_format,'\n']))
logfile_handle.write("".join(['HOSTNAME: ',host_name,'\n']))
logfile_handle.write("".join(['SOURCEFILES: ',oj]))
logfile_handle.close()
#sys.exit() ; # Exit once information written to logfile (for testing)
del(trim_host,time_now,time_format,cmd,o,oj)
gc.collect()

# Create data pathnames
path_data       = '/ipcc/'

# Create variable and realm names
experiments = ['1pctto2x','1pctto4x','20c3m','2xco2','amip','picntrl','sresa1b','sresa2','sresb1'] ; experiments.sort()
temporal    = ['mo','fixed'] ; # For months and fixed fields only
atm_vars    = ['pr','tas'] ; atm_vars.sort()
atm_vars2   = ['sci', 'rsdscs', 'hfls', 'hfss', 'ua', 'ps', 'rlutcs', 'vas', 'tasmin', 
               'prw', 'prc', 'sfcWind', 'rsuscs', 'uas', 'ta', 'wap', 'cl', 'hur',
               'tauu', 'rsds', 'rlds', 'sbl', 'huss', 'rsdt', 'psl', 'zg', 'clt', 'evspsbl',
               'tauv', 'prsn', 'hus', 'mc', 'hurs', 'rsut', 'rlut', 'ts', 'rsutcs', 'rsus',
               'rlus', 'va', 'rldscs', 'cli', 'clw', 'tasmax'] ; atm_vars2.sort()
atmOrocn    = ['atm','ocn'] ; atmOrocn.sort()
fx_vars     = ['areacello','basin','deptho','sftof','volcello','areacella','orog','sftlf'] ; fx_vars.sort()
land_vars   = ['mrro','mrros'] ; land_vars.sort()
ocn_vars    = ['so','sos','thetao','tos','wfo','zos','rhopoto','uo','vo','wo'] ; ocn_vars.sort()
seaIce_vars = ['sic','sit'] ; seaIce_vars.sort()
len_vars    = len(atm_vars)+len(atm_vars2)+len(fx_vars)+len(land_vars)+len(ocn_vars)+len(seaIce_vars) ; # Create length counter for reporting


# Mine data directory for paths and files
i1 = 0
list_data_paths = []
for (path, dirs, files) in os.walk(path_data,'false'):
    if files != [] and dirs == []:
        #print "path: " + path ; print files
        # Append to list variable
        list_data_paths += [path]
        if i1 % 5000 == 0:
            print "%06d--" % i1
        i1 = i1 + 1 ; # Increment counter

del(path,dirs,files,path_data)
gc.collect()

# Mine paths for model information and create output filenames
i2 = 0
exclude_paths = ['cam3.3','cmip5','T4031qt','summer']
list_data_outfiles = []; list_data_outfiles_paths = []
for path in list_data_paths:
    path_bits   = path.split('/')
    #print(i2,path_bits)
    if path_bits[2] in exclude_paths:
      continue
    elif 'data' in path_bits[2]:
      model       = path_bits[7] ; #6
      experiment  = path_bits[3] ; #7
      time_ax     = path_bits[5] ; #8
      realisation = path_bits[8] ; #11
      version     = '2' ; #13 - data dirs
      variable    = path_bits[6] ; #12
    else:
      model       = path_bits[6] ; #6
      experiment  = path_bits[2] ; #7
      time_ax     = path_bits[4] ; #8
      realisation = path_bits[7] ; #11
      version     = '1' ; #13 - $experiment dirs
      variable    = path_bits[5] ; #12
    # Test for list entry and trim experiments and variables to manageable list
    if ( (experiment == '20c3m') or (experiment == 'amip') ) and (time_ax in temporal) and ( (variable in ocn_vars) or (variable in atm_vars2) ):
        #                                      cmip3.bccr_bcm2_0.sresa1b.run1.mo.pr.xml
        list_data_outfiles.insert(i2,"".join(['cmip3.',model,'.',experiment,'.',realisation,'.',time_ax,'.',variable,'.ver-',version,'.xml']))
        list_data_outfiles_paths.insert(i2,path)
        i2 = i2 + 1    
    elif (experiment in experiments) and (time_ax in temporal) and ( (variable in ocn_vars) or (variable in atm_vars) or (variable in seaIce_vars) or (variable in land_vars) or (variable in fx_vars) ):
        list_data_outfiles.insert(i2,"".join(['cmip3.',model,'.',experiment,'.',realisation,'.',time_ax,'.',variable,'.ver-',version,'.xml']))
        list_data_outfiles_paths.insert(i2,path)
        i2 = i2 + 1
    #print i2

# Write out diagnostics
logfile_handle = open(logfile,'a')
logtime_now = datetime.datetime.now()
logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
print "\'ipcc\' scan complete.. %d paths total; %d output files to be written (%d vars sampled)" % (i1,len(list_data_outfiles),len_vars)
logfile_handle.write("".join([time_since_start_s,' : \'ipcc\' scan complete.. ',format(i1,"1d"),' paths total; ',format(len(list_data_outfiles),"1d"),' output files to be written (',format(len_vars,"1d"),' vars sampled)\n']))
logfile_handle.close()
del(path,path_bits,model,experiment,time_ax,realisation,version,i1,i2,logtime_now,logtime_format,time_since_start,time_since_start_s)
del(atmOrocn,atm_vars,atm_vars2,exclude_paths,experiments,len_vars,list_data_paths,temporal,variable)
gc.collect()
time.sleep(1) ; # Pause and acknowledge "data" success

# Sort lists by outfiles
outfilesAndPaths = zip(list_data_outfiles,list_data_outfiles_paths)
outfilesAndPaths.sort() ; # sort by str value forgetting case - key=str.lower; requires str object
del(list_data_outfiles,list_data_outfiles_paths)
gc.collect()
list_outfiles, list_outfiles_paths = zip(*outfilesAndPaths)
del(outfilesAndPaths)
gc.collect()

# Use time, size and path info to sort good from bad
# Test files fx: /ipcc/20c3m/land/fixed/sftlf/bccr_bcm2_0/run1 [205] and /ipcc/data2/20c3m/land/fixed/sftlf/bccr_bcm2_0/run1 [206]
# Test files so: cmip3.bccr_bcm2_0.20c3m.run1.mo.so.ver-1.xml [254] and cmip3.bccr_bcm2_0.20c3m.run1.mo.so.ver-2.xml [255]
i1 = 0; one_times = []; two_times = []; one_sizes = []; two_sizes = []; purge_index = []
for index in range(len(list_outfiles)-1):
    # Test names
    one_file = list_outfiles[index]
    two_file = list_outfiles[index+1]
    if re.sub("ver-[0-3].","",one_file) in re.sub("ver-[0-3].","",two_file):
        #print index,' ',one_file,' ',two_file
        # Test times and sizes
        one_path = list_outfiles_paths[index]
        for (path, dirs, files) in os.walk(one_path,'false'):
            for f_index in range(len(files)):
                a = os.path.getmtime(os.path.join(path,files[f_index]))
                one_times.insert(f_index,a)
                b = os.path.getsize(os.path.join(path,files[f_index]))
                one_sizes.insert(f_index,b)
        one_times[0:2] = [min(one_times),max(one_times)]
        one_times[2:] = []
        one_sizes[0:2] = [min(one_sizes),max(one_sizes)]
        one_sizes[2:] = []
        #print 'times1: ',one_times
        #print 'sizes1: ',one_sizes
        del(one_file,two_file,one_path,path,dirs,files,a,b,f_index)
        two_path = list_outfiles_paths[index+1]
        for (path, dirs, files) in os.walk(two_path,'false'):
            for f_index in range(len(files)):
                a = os.path.getmtime(os.path.join(path,files[f_index]))
                two_times.insert(f_index,a)
                b = os.path.getsize(os.path.join(path,files[f_index]))
                two_sizes.insert(f_index,b)                
        two_times[0:2] = [min(two_times),max(two_times)]
        two_times[2:] = []
        two_sizes[0:2] = [min(two_sizes),max(two_sizes)]
        two_sizes[2:] = []        
        #print 'times2: ',two_times
        #print 'sizes2: ',two_sizes
        del(two_path,path,dirs,files,a,b,f_index)
        # From tests make keep call    
        if (one_times == two_times) and (one_sizes == two_sizes):
            # Add v2 entry to purge list            
            purge_index.insert(i1,index+1)
            #print "enter case 1"
            i1 = i1 + 1;
        elif (one_times < two_times) or (one_sizes < two_sizes):
            # If v2 newer, or larger add v1 to purge list
            purge_index.insert(i1,index)
            #print "enter case 2"
            i1 = i1 + 1;
        elif (one_times > two_times) or (one_sizes > two_sizes):
            # If v1 newer, or larger add v2 to purge list
            purge_index.insert(i1,index+1)
            #print "enter case 3"
            i1 = i1 + 1;
        #print i1

del(i1,index,one_file,one_sizes,one_times,two_file,two_sizes,two_times)
gc.collect()

# Truncate duplicated data - from last to first index
list_outfiles = list(list_outfiles)
list_outfiles_paths = list(list_outfiles_paths)
deque((list.pop(list_outfiles,i) for i in sorted(purge_index,reverse=True)),maxlen=0)
deque((list.pop(list_outfiles_paths,i) for i in sorted(purge_index,reverse=True)),maxlen=0)
del(purge_index)
gc.collect()


# Consider breaking code below into subprocess


# Check whether running for file reporting or xml generation:
if make_xml:
    # Create counters for xml_good and xml_bad
    xml_good = 1; xml_bad1 = 1; xml_bad2 = 1; xml_bad3 = 1;
    # Deal with existing *.xml files
    ii,o,e = os.popen3("".join(['ls ',host_path,'*/*/mo/*/*.xml | wc -l']))
    xml_count1 = o.read();
    ii,o,e = os.popen3("".join(['ls ',host_path,'*/*/fx/*/*.xml | wc -l']))
    xml_count2 = o.read();
    xml_count = int(xml_count1)+int(xml_count2);
    print "".join(['** Updating ',format(xml_count,"1d"),' existing *.xml files **'])
    logfile_handle = open(logfile,'a')
    logfile_handle.write("".join(['** Updating ',format(xml_count,"1d"),' existing *.xml files **\n']))
    logfile_handle.close()
    # Catch errors with system commands
    cmd = "".join(['rm -rf ',host_path,'*/*/mo_new'])
    ii,o,e = os.popen3(cmd) ; # os.popen3 splits results into input, output and error - consider subprocess function in future
    print "** Generating new *.xml files **"
    logfile_handle = open(logfile,'a')
    logfile_handle.write("** Generating new *.xml files **\n")
    logfile_handle.close()
    i = 0
    for paths in list_outfiles_paths:
    #for paths in list_data_outfiles_paths.keys()[:5]: ; list_outfiles.index('cmip5.GISS-E2-R.rcp26.r1i1p1.mon.so.ver-v20111011.xml') : 6810
        print "".join(["** processing ",list_outfiles[i]])
        infilenames = glob.glob(os.path.join("".join(list_outfiles_paths[i]),'*.nc'))
        # Construct outfile path from outfilename
        outfile_string  = "".join(list_outfiles[i])
        outfile_bits    = outfile_string.split('.')
        model           = outfile_bits[1]
        experiment      = outfile_bits[2]    
        temporal        = outfile_bits[4]
        if (temporal == 'mo'):
            temporal = 'mo_new' ; # Updated path; existing xmls are in place until successful xml write completion
        elif (temporal == 'fixed'):
            temporal = 'fx_new' ;
        variable = outfile_bits[5]
        if (variable in fx_vars):
	      realm = 'fx'
        elif (variable in land_vars):
            realm = 'land'
        elif (variable in ocn_vars):
            realm = 'ocn'
        elif (variable in seaIce_vars):
            realm = 'seaIce'
        else:
            realm = 'atm'
        #if (variable in
        out_path = os.path.join(experiment,realm,temporal,variable)
        outfilename = os.path.join(host_path,out_path,"".join(list_outfiles[i]))
        # Fix mon -> mo
        #outfilename = string.replace(outfilename,'mon','mo') ; # Replaces mon in filename (not path)
        if os.path.exists(os.path.join(host_path,out_path)) != 1:
            # At first run create output directories
            os.makedirs(os.path.join(host_path,out_path))
        #if os.path.exists
        # Purge existing xmls - must be conditional on first pass
        #if os.path.exists(os.path.join(host_path,out_path)) == 1:
        #    os.remove(os.path.join(host_path,out_path,'*.xml'))
        # if os.path.exists
        if os.path.isfile(outfilename) == 1:
            os.remove(outfilename)
        #if os.path.isfile
        # Generate xml file
        if len(infilenames) != 0:
            # Create a fullpath list of bad files and exclude these, by trimming them out of a filename list
            cmd = "".join([cdat_path,'cdscan -x ',outfilename,' ',os.path.join("".join(list_outfiles_paths[i]),'*.nc')])
            # Catch errors with file generation
            ii,o,e = os.popen3(cmd) ; # os.popen3 splits results into input, output and error - consider subprocess function in future
            st = o.read()
	    # Check cdscan output for warning flags
	    fileerror = False         
            if st.find('Warning:') > -1:
                errstart = st.find('Warning:')
                errend = st.find(':',errstart+8)
                errorcode = st[errstart:errend]
                fileerror = True
            #if st.find
            # Validate file was written
            filenotok = os.path.isfile(outfilename) != True
            # Consider 0-file size check for inputs
            zerofiles = False
            filestocheck = os.listdir(list_outfiles_paths[i])
            for checkfile in filestocheck:
                fileinfo = os.stat(os.path.join(list_outfiles_paths[i],checkfile))
                checksize = fileinfo.st_size
                if checksize == 0:
                    zerofiles = True
            # Begin log
            logfile_handle = open(logfile,'a')
            logtime_now = datetime.datetime.now()
            logtime_format = logtime_now.strftime("%y%m%d_%H%M%S")
            time_since_start = time.time() - start_time ; time_since_start_s = '%09.2f' % time_since_start
            if fileerror and zerofiles:
                # Case cdscan call reports an error - print "".join(ln) ; # Print error
                err_text = ' PROBLEM 1 (cdscan error - zero infile size) creating '
                logfile_handle.write("".join(['** ',format(xml_bad1,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,outfilename,' **\n']))
                logfile_handle.close()
                print "".join(['**',err_text,outfilename,' **'])
                xml_bad1 = xml_bad1 + 1;
                # Purge problem files
                if os.path.isfile(outfilename):
                    os.remove(outfilename)
            elif fileerror:
                # Case cdscan call reports an error - print "".join(ln) ; # Print error
                err_text = "".join([' PROBLEM 2 (cdscan error - \'',errorcode,'\') creating '])
                logfile_handle.write("".join(['** ',format(xml_bad2,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,outfilename,' **\n']))
                logfile_handle.close()
                print "".join(['**',err_text,outfilename,' **'])
                xml_bad2 = xml_bad2 + 1;
                # Purge problem files
                if os.path.isfile(outfilename):
                    os.remove(outfilename)
            elif filenotok:
                # Case cdscan ok, however file wasn't written
                err_text = ' PROBLEM 3 (no outfile) creating '
                logfile_handle.write("".join(['** ',format(xml_bad3,"07d"),' ',logtime_format,' ',time_since_start_s,'s',err_text,outfilename,' **\n']))
                logfile_handle.close()
                print "".join(['**',err_text,outfilename,' **'])
                xml_bad3 = xml_bad3 + 1;
                # Purge problem files
                if os.path.isfile(outfilename):
                    os.remove(outfilename)
            else:
                logfile_handle.write("".join(['** ',format(xml_good,"07d"),' ',logtime_format,' ',time_since_start_s,'s success creating: ',outfilename,' **\n']))
                logfile_handle.close()
                xml_good = xml_good + 1;
            #if len(ln..
        #if len(infilenames..
        i = i + 1
    #for paths in list_..
    # Create master list of xml_bad
    xml_bad = xml_bad1+xml_bad2+xml_bad3
    print "".join(['** Complete for \'ipcc\' sources; Total outfiles: ',format(len(list_outfiles),"01d"),' **'])
    print "".join(['** XML file count - Good: ',format(xml_good-1,"1d"),' **'])
    print "".join(['** XML file count - Bad/skipped: ',format(xml_bad-3,"1d"),'; bad1 (cdscan - zero files): ',format(xml_bad1-1,"1d"),'; bad2 (cdscan - warning specified): ',format(xml_bad2-1,"1d"),'; bad3 (no outfile): ',format(xml_bad3-1,"1d")])
    logfile_handle = open(logfile,'a')
    logfile_handle.write("".join(['** make_cmip3_xml.py complete for \'ipcc\' sources; Total outfiles: ',format(len(list_outfiles),"01d"),' **\n']))
    logfile_handle.write("".join(['** XML file count - Good: ',format(xml_good-1,"1d"),' **\n']))
    logfile_handle.write("".join(['** XML file count - Bad/skipped: ',format(xml_bad-3,"1d"),'; bad1 (cdscan - zero files): ',format(xml_bad1-1,"1d"),'; bad2 (cdscan - warning specified): ',format(xml_bad2-1,"1d"),'; bad3 (no outfile): ',format(xml_bad3-1,"1d"),' **\n']))
    logfile_handle.close()

    # Once run is complete, archive old files and move new files into place
    time_now = datetime.datetime.now()
    time_format = time_now.strftime("%y%m%d_%H%M%S")
    # Ensure /cmip5 directory is cwd
    os.chdir(host_path)
    # Archive old files
    cmd = "".join(['7za a ',host_path,'_archive/',time_format,'_cmip3_xml.7z */*/*/*/*.xml -t7z'])
    fnull = open(os.devnull,'w')
    p = subprocess.call(cmd,stdout=fnull,shell=True)
    fnull.close()
    # Purge old files [durack1@crunchy cmip5]$ rm -rf */*/mo
    cmd = 'rm -rf */*/mo'
    fnull = open(os.devnull,'w')
    p = subprocess.call(cmd,stdout=fnull,shell=True)
    fnull.close()
    cmd = 'rm -rf */*/fx'
    fnull = open(os.devnull,'w')
    p = subprocess.call(cmd,stdout=fnull,shell=True)
    fnull.close()    
    # Move new files into place
    cmd = 'find */*/mo_new -maxdepth 0 -exec sh -c \'mv -f `echo {}` `echo {} | sed s/mo_new/mo/`\' \;'
    fnull = open(os.devnull,'w')
    p = subprocess.call(cmd,stdout=fnull,shell=True)
    fnull.close()
    cmd = 'find */*/fx_new -maxdepth 0 -exec sh -c \'mv -f `echo {}` `echo {} | sed s/fx_new/fx/`\' \;'
    fnull = open(os.devnull,'w')
    p = subprocess.call(cmd,stdout=fnull,shell=True)
    fnull.close()    
    del(time_now,cmd,fnull,p)
    gc.collect()
    #[durack1@crunchy cmip5]$ find */*/mo_new -maxdepth 0 -exec sh -c 'mv -f `echo {}` `echo {} | sed s/mo_new/mo/`' \;
    #[durack1@crunchy cmip5]$ ls -d1 */*/mo_new | sed -e 'p;s/mo_new/mo/' | xargs -n 2 mv
    # Report migration success to prompt and log
    print "".join(['** Archive and migration complete from */*/*_new to */*/*, archive file: ',host_path,'_archive/',time_format,'_cmip3_xml.7z **'])
    logfile_handle = open(logfile,'a')
    logfile_handle.write("".join(['** Archive and migration complete from */*/*_new to */*/*,\n archive file: ',host_path,'_archive/',time_format,'_cmip3_xml.7z **']))
    logfile_handle.close()
    
else:
    print "** make_cmip3_xml.py run in report mode **"
    logfile_handle = open(logfile,'a')
    logfile_handle.write("** make_cmip3_xml.py run in report mode **")
    logfile_handle.close()

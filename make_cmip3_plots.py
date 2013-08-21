# -*- coding: utf-8 -*-
"""
Created on Mon Oct 29 10:31:18 2012

Paul J. Durack 29th October 2012

This script quick-plots cmip3 analysed fields

PJD 29 Oct 2012     - There is a need to scale by year and by unit (1e-3 = x1000)
PJD 29 Oct 2012     - Two examples below (cccma_cgcm3_1.20c3m.run1, giss_model_e_r.20c3m.run5) appear to checkout ok
                    although a quantitative comparison is necessary (regridding, scale, colour map etc)
Colormaps - http://www.scipy.org/Cookbook/Matplotlib/Show_colormaps


# Colormaps - jet is default (blue to red)
* autumn * bone * cool * copper * flag * gray * hot * hsv * jet * pink * prism * spring * summer * winter * spectral
@author: durack1
"""

import pylab *
import cdms2 as cdms
from matplotlib.cm import RdBu_r ; # Red/blue colours
from matplotlib.cm import BrBG ; # Vegetations colours

# Load variable
f = cdms.open('/work/durack1/Shared/cmip3/20c3m/ocn/an_trends/so/cmip3.cccma_cgcm3_1.20c3m.run1.an.ocn.so.ver-1.1950-2000_ClimAndSlope.nc')
so = f['so_change']
shape(so)
sos = so[0, ...]

# Plot
close()
figure(1)
pcolor(so.getLongitude()[:],so.getLatitude()[:],sos*50*1000,vmin=-0.2,vmax=0.2,cmap=RdBu_r)
plt.xlim(0,359)
plt.ylim(-70,70)
colorbar()
f.close()
# Passes by eyeball

f = cdms.open('/work/durack1/Shared/cmip3/20c3m/ocn/an_trends/so/cmip3.giss_model_e_r.20c3m.run5.an.ocn.so.ver-1.1950-2000_ClimAndSlope.nc')
so = f['so_change']
shape(so)
sos = so[0, ...]

# Plot
close()
figure(1)
pcolor(so.getLongitude()[:],so.getLatitude()[:],sos*50*1000,vmin=-0.2,vmax=0.2,cmap=RdBu_r)
plt.xlim(0,359)
plt.ylim(-70,70)
f.close()
# Passes by eyeball
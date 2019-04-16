# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""
import numpy as np
import pandas as pd
import os
import geopandas as gpd
import xarray as xr
from niwa import rd_niwa_vcsn
from pdsql import mssql
from hydrointerp import interp2d
from gistools import vector
import seaborn as sns
from hydrolm import LM
import matplotlib.pyplot as plt

pd.options.display.max_columns = 10

######################################
### Parameters

py_path = os.path.realpath(os.path.dirname(__file__))
base_path = os.path.split(py_path)[0]

input_dir = 'input_files'
output_dir = 'output_files'

rec_streams_shp = 'rec_streams_pareora.shp'
catch_del_shp = 'pareora_catchments.shp'
catch_shp = 'catchment_pareora.shp'

rec_streams_shp_path = os.path.join(base_path, input_dir, rec_streams_shp)
catch_del_shp_path = os.path.join(base_path, input_dir, catch_del_shp)
catch_shp_path = os.path.join(base_path, input_dir, catch_shp)

server = 'edwprod01'
database = 'hydro'
sites_table = 'ExternalSite'
ts_summ_table = 'TSDataNumericDailySumm'
ts_table = 'TSDataNumericDaily'

#rec_sites = ['70105', '70103']
rec_site = '70105'

mtypes = ['precip', 'PET' ]

from_date = '1988-07-01'
to_date = '2015-06-30'

from_date_plot = '1996-06-30'

sites_csv = 'precip_sites_loc.csv'
precip_ts_csv = 'precip_ts_data.csv'
flow_csv = 'pareora_huts_flow.csv'
usage_csv = 'pareora_huts_usage_mon.csv'

#sites = ['403711', '405711', '404810', '405910', '414110', '405610', '403601', '417110', '407810', '400910']
sites = ['403711', '404810', '405910', '414110']

error_mon_plot = 'vcsn_mon_bias.png'
precip_mon_plot = 'station_precip_mon.png'
precip_yr_plot = 'station_precip_yr.png'
reg_plot = 'mon_reg_404810.png'

#####################################
### Process VCSN data

both1 = rd_niwa_vcsn(mtypes, catch_shp_path, buffer_dis=5000)

## Aggregate by month
both2 = both1.groupby(['x', 'y', pd.Grouper(key='time', freq='M')]).sum()
both3 = both2.loc[(slice(None), slice(None), slice(from_date, to_date)), :].copy()

## Aggregate by catchment
both4 = vector.xy_to_gpd(['time', 'rain', 'pe'], 'x', 'y', both3.reset_index(), 4326)
both5 = both4.to_crs(epsg=2193)

catch_del = gpd.read_file(catch_del_shp_path)
catch_del.rename(columns={'SITENUMBER': 'site'}, inplace=True)
catch_del['site'] = catch_del['site'].astype(str)

pts2, poly1 = vector.pts_poly_join(both5, catch_del, 'site')

catch_agg0 = pts2.groupby(['site', 'time'])[['rain', 'pe']].mean()

# Only above Huts
catch_agg1 = catch_agg0.loc[rec_site].copy()
catch_agg1.rename(columns={'rain': 'VCSN Precip', 'pe': 'VCSN PET'}, inplace=True)

# VCSN stats
catch_agg_year = catch_agg1.resample('A-JUN').sum()
vcsn_stats = catch_agg_year.describe()

## VCSN estimates at station locations
#pts0 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'], where_in={'ExtSiteID': sites}, rename_cols=['site', 'x', 'y']).astype(int)
#pts0.to_csv(os.path.join(base_path, input_dir, sites_csv), index=False)

pts0 = pd.read_csv(os.path.join(base_path, input_dir, sites_csv))

pts1 = interp2d.points_to_points(both3.reset_index(), 'time', 'x', 'y', 'rain', pts0, 4326, 2193).reset_index().dropna()
pts1['x'] = pts1['x'].round().astype(int)
pts1['y'] = pts1['y'].round().astype(int)

pts2 = pd.merge(pts0, pts1, on=['x', 'y']).drop(['x', 'y'], axis=1)
pts2['site'] = pts2['site'].astype(str)

####################################
### Process Station data

#ts1 = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_in={'ExtSiteID': sites,'DatasetTypeID': [15]}, from_date=from_date, to_date=to_date).reset_index()
#ts1.rename(columns={'ExtSiteID': 'site', 'DateTime': 'time', 'Value': 'Precip'}, inplace=True)
#ts1.to_csv(os.path.join(base_path, input_dir, precip_ts_csv), index=False)

ts1 = pd.read_csv(os.path.join(base_path, input_dir, precip_ts_csv), parse_dates=['time'], infer_datetime_format=True)

grp1 = ts1.groupby(['site', pd.Grouper(key='time', freq='M')])[['Precip']]
ts2 = grp1.sum()
ts2['days'] = grp1.count()

ts2['daysinmonth'] = ts2.index.get_level_values('time').daysinmonth

ts3 = ts2.drop(['days', 'daysinmonth'], axis=1).copy()
ts3.loc[(ts2['daysinmonth'] - ts2['days']) > 2, 'Precip'] = np.nan

## Regressions to fill missing months
# Monthly
ts4 = ts3.Precip.unstack(0).sort_index()

missing1 = ts4.isnull().any()

ts5 = ts4.copy()
ts5[ts5 == 0] = np.nan

y = ts5.loc[:, missing1]
x = ts5.loc[:, ~missing1]

lm1 = LM(x, y)

ols_mon1 = lm1.predict(x_transform='log', y_transform='log')

for i, s in ols_mon1.summary_df.iterrows():
    ts4.loc[ts4[i].isnull(), i] = np.exp(np.log(ts4.loc[ts4[i].isnull(), s['x sites']]) * float(s['x slopes']) + s['y intercept']).round(1)

ts5 = ts4.stack(0).reorder_levels([1, 0]).sort_index()
ts5.name = 'Precip'

y_lm = ols_mon1.sm_xy['404810']['y_trans']
x_lm = ols_mon1.sm_xy['404810']['x_trans']

xy_lm = pd.concat([x_lm, y_lm], axis=1)

## Plot reg
sns.set_style("whitegrid")
sns.set_context('poster')
fig, ax = plt.subplots(figsize=(15, 10))

sns.regplot(x='405910', y='404810', data=xy_lm, ax=ax)
plt.xlabel('405910 $(log(mm))$')
plt.ylabel('404810 $(log(mm))$')
plt.tight_layout()
plot1 = ax.get_figure()
plot1.savefig(os.path.join(base_path, output_dir, 'plots', reg_plot))


# Aggregate to yearly
ts_year2 = ts4.resample('A-JUN').sum().stack(0).reorder_levels([1, 0]).sort_index()
ts_year2.name = 'Precip'

### Combine with VCSN

pts3 = pd.merge(ts5.to_frame(), pts2, on=['site', 'time'])
pts3.rename(columns={'rain': 'VCSN Precip'}, inplace=True)

pts4 = pts3[pts3['Precip'] > 0].copy()

pts4['abs_normalised_error'] = (pts4['VCSN Precip'] - pts4['Precip']).abs()/pts4['Precip']
pts4['normalised_error'] = (pts4['VCSN Precip'] - pts4['Precip'])/pts4['Precip']
pts4['month'] = pts4.time.dt.strftime('%b')

mane1 = pts4.groupby('site')['abs_normalised_error'].mean()
bias1 = pts4.groupby('site')['normalised_error'].mean()

### Plotting
## Error to VCSN
pts5 = pts4.copy()
pts5.rename(columns={'site': 'Station', 'normalised_error': 'Normalised Error', 'month': 'Month'}, inplace=True)

sns.set_style("whitegrid")
sns.set_context('poster')
fig, ax = plt.subplots(figsize=(15, 10))

sns.boxplot(x='Month', y='Normalised Error', hue='Station', data=pts5, order=['Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'], ax=ax)
ax.set_ylim(-1, 1)
plt.ylabel('Normalised Error of VCSN Precipitation')
plt.tight_layout()
plot1 = ax.get_figure()
plot1.savefig(os.path.join(base_path, output_dir, 'plots', error_mon_plot))

## Monthly station data
ts6 = ts5.reset_index().copy()
ts6['Month'] = ts6.time.dt.strftime('%b')
ts6.rename(columns={'Precip': 'Precipitation (mm)', 'site': 'Station'}, inplace=True)

sns.set_style("whitegrid")
sns.set_context('poster')
fig, ax = plt.subplots(figsize=(15, 10))

sns.boxplot(x='Month', y='Precipitation (mm)', hue='Station', data=ts6, order=['Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec', 'Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun'], ax=ax)
ax.set_ylim(0, 200)
plt.ylabel('Precipitation $(mm)$')
plt.tight_layout()
plot2 = ax.get_figure()
plot2.savefig(os.path.join(base_path, output_dir, 'plots', precip_mon_plot))


## Combine with flow

flow1 = pd.read_csv(os.path.join(base_path, input_dir, flow_csv),  parse_dates=['time'], infer_datetime_format=True)
flow1['site'] = flow1['site'].astype(str)

flow1 = flow1[flow1.site == rec_site]

flow1['flow'] = flow1['flow'] * 60*60*24
flow2 = flow1.groupby(['site', pd.Grouper(key='time', freq='A-JUN')]).sum().reset_index()

## Add Usage

usage1 = pd.read_csv(os.path.join(base_path, input_dir, usage_csv),  parse_dates=['time'], infer_datetime_format=True)
usage1 = usage1[(usage1.time >= from_date) & (usage1.time <= to_date)]

usage2 = usage1.groupby('time')['sd_usage'].sum().reset_index()
usage2['site'] = '70105'

flow2a = pd.merge(flow2, usage2, on=['site', 'time'])
flow2a['flow'] = flow2a['flow'] + flow2a['sd_usage']

## Normalise to area

poly1['area'] = poly1.area

poly2 = poly1.drop('geometry', axis=1).copy()

flow3 = pd.merge(flow2a, poly2, on='site').drop('sd_usage', axis=1)

flow3['flow_mm'] = flow3['flow'] / flow3['area'] * 1000

flow4 = flow3.drop(['flow', 'area'], axis=1).set_index(['site', 'time']).round(1).copy()

rain1 = ts_year2.loc['404810'].copy()
vcsn_pet = catch_agg1['VCSN PET'].resample('A-JUN').sum().round(1)

catch_agg2 = pd.concat([rain1, flow4.loc['70105'], vcsn_pet], axis=1)

## Calc AET

catch_agg2['AET'] = catch_agg2['Precip'] - catch_agg2['flow_mm']

catch_agg2.index = catch_agg2.index.year

catch_agg2.rename(columns={'Precip': '404810 precip', 'flow_mm': '70105 flow'}, inplace=True)

catch_agg3 = catch_agg2.stack(0).reorder_levels([1, 0]).sort_index()
catch_agg3.name = 'Depth (mm)'
catch_agg3.index.names = ['Dataset', 'Water Year End']

plot_df = catch_agg3.loc[(slice(None), slice(2006, None))].reset_index()

## Plot years
sns.set_style("whitegrid")
sns.set_context('poster')
fig, ax = plt.subplots(figsize=(15, 10))

sns.barplot(x='Water Year End', y='Depth (mm)', hue='Dataset', data=plot_df, palette='muted', ax=ax)
plt.tight_layout()
plot2 = ax.get_figure()
plot2.savefig(os.path.join(base_path, output_dir, 'plots', precip_yr_plot))








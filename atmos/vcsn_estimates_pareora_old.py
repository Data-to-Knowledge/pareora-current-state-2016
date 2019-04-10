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
import statsmodels as sm
from hydrolm import LM
#from import_fun import rd_vcn
#from ts_stats_fun import w_resample, flow_stats
#from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot

pd.options.display.max_columns = 10

######################################
### Parameters

py_path = os.path.realpath(os.path.dirname(__file__))
base_path = os.path.split(py_path)[0]

shp_dir = 'input_files'
output_dir = 'output_files'

rec_streams_shp = 'rec_streams_pareora.shp'
catch_del_shp = 'pareora_catchments.shp'
catch_shp = 'catchment_pareora.shp'

rec_streams_shp_path = os.path.join(base_path, shp_dir, rec_streams_shp)
catch_del_shp_path = os.path.join(base_path, shp_dir, catch_del_shp)
catch_shp_path = os.path.join(base_path, shp_dir, catch_shp)

server = 'edwprod01'
database = 'hydro'
sites_table = 'ExternalSite'
ts_summ_table = 'TSDataNumericDailySumm'
ts_table = 'TSDataNumericDaily'

#rec_sites = ['70105', '70103']
rec_site = '70105'

mtypes = ['precip', 'PET' ]

from_date = '1982-07-01'
to_date = '2015-06-30'

flow_csv = 'pareora_huts_flow.csv'
usage_csv = 'pareora_huts_usage_mon.csv'

sites = ['403711', '405711', '404810', '405910', '414110', '405610', '403601', '417110', '407810', '400910']

#####################################
### Create dataframe from many VCN csv files

both1 = rd_niwa_vcsn(mtypes, catch_shp_path, buffer_dis=10000)

## Aggregate by month
both2 = both1.groupby(['x', 'y', pd.Grouper(key='time', freq='M')]).sum()
both3 = both2.loc[(slice(None), slice(None), slice(from_date, to_date)), :].copy()

## Resample in 2D
pe1 = interp2d.points_to_grid(both3.reset_index(), 'time', 'x', 'y', 'pe', 1000, 4326, 2193)
rain1 = interp2d.points_to_grid(both3.reset_index(), 'time', 'x', 'y', 'rain', 1000, 4326, 2193)

pe2 = pe1.to_dataframe().dropna()
rain2 = rain1.to_dataframe().dropna()

## Combine datasets

both4 = pd.concat([rain2, pe2], axis=1).reset_index()

## Aggregate by catchment
both5 = vector.xy_to_gpd(['time', 'rain', 'pe'], 'x', 'y', both4)
#pts0 = both4[both4.time == '1982-07-31'].copy()
#pts0.index.name = 'index'
#pts1 = vector.xy_to_gpd(pts0.index, 'x', 'y', pts0)
catch_del = gpd.read_file(catch_del_shp_path)
catch_del.rename(columns={'SITENUMBER': 'site'}, inplace=True)

pts2, poly1 = vector.pts_poly_join(both5, catch_del, 'site')

catch_agg1 = pts2.groupby(['site', 'time'])[['rain', 'pe']].mean()

## Adjust the vcsn according to the precip gauge 404810
ts1 = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_in={'ExtSiteID': ['404810'],'DatasetTypeID': [15]}, from_date='2005-07-01', to_date=to_date)
ts1 = ts1.droplevel(0)

rain3 = catch_agg1.loc[70103, 'rain']

gauge1 = ts1.resample('A-JUN').sum().Value * 1.07
gauge1.name = '404810'

ols1 = LM(rain3.to_frame(), gauge1.to_frame()).predict()

ols_summ = ols1.summary_df.copy()

rain4 = (rain3 * float(ols_summ['x slopes'][0]) + ols_summ['y intercept'][0]).reset_index()
rain4['site'] = 70105
rain4.set_index(['site', 'time'], inplace=True)

## Combine with flow
#flow1 = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_in={'DatasetTypeID': [5], 'ExtSiteID': rec_sites}, from_date=from_date, to_date=to_date).reset_index()
#flow1.rename(columns={'ExtSiteID': 'site', 'DateTime': 'time', 'Value': 'flow'}, inplace=True)
#flow1.to_csv(os.path.join(base_path, shp_dir, flow_output), index=False)

flow1 = pd.read_csv(os.path.join(base_path, shp_dir, flow_csv),  parse_dates=['time'], infer_datetime_format=True)
flow1['site'] = flow1['site'].astype(str)

flow1 = flow1[flow1.site == rec_site]

flow1['flow'] = flow1['flow'] * 60*60*24
flow2 = flow1.groupby(['site', pd.Grouper(key='time', freq='A-JUN')]).sum().reset_index()

flow2['site'] = pd.to_numeric(flow2['site'])

## Add Usage

usage1 = pd.read_csv(os.path.join(base_path, shp_dir, usage_csv),  parse_dates=['time'], infer_datetime_format=True)
usage1 = usage1[(usage1.time >= from_date) & (usage1.time <= to_date)]

usage2 = usage1.groupby('time')['sd_usage'].sum().reset_index()
usage2['site'] = 70105

flow2a = pd.merge(flow2, usage2, on=['site', 'time'])
flow2a['flow'] = flow2a['flow'] + flow2a['sd_usage']

## Normalise to area

poly1['area'] = poly1.area

poly2 = poly1.drop('geometry', axis=1).copy()

flow3 = pd.merge(flow2a, poly2, on='site').drop('sd_usage', axis=1)

flow3['flow_mm'] = flow3['flow'] / flow3['area'] * 1000

flow4 = flow3.drop(['flow', 'area'], axis=1).set_index(['site', 'time']).copy()

catch_agg2 = pd.concat([rain4, flow4, catch_agg1['pe']], axis=1)

## Calc AET

catch_agg2['AET'] = catch_agg2['rain'] - catch_agg2['flow_mm']



## Testing
t1 = catch_agg2.reset_index()
t2 = t1[t1.site == 70105].drop('site', axis=1).copy()
t2.set_index('time', inplace=True)

t3 = t2.resample('A-JUN').mean()


sns.regplot('AET', 'pe', data=t2)



ts1 = mssql.rd_sql(server, database, ts_table, ['DateTime', 'Value'], where_in={'ExtSiteID': ['404810'],'DatasetTypeID': [15]})
ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
ts1.set_index('DateTime', inplace=True)

day_mean1 = rain2.groupby(level='time').mean()

sum1 = day_mean1.resample('M').sum()['2005-07-01':'2015-06-30']
sum4 = ts1.resample('M').sum()['2005-07-01':'2015-06-30']

sns.regplot(sum1.rain.apply(np.log), sum4.Value.apply(np.log))
sns.regplot(sum1.rain, sum4.Value)

sum1b = day_mean1.resample('A-JUN').sum()['2005-07-01':'2015-06-30']
sum1b.columns = ['vcsn']
sum4b = ts1.resample('A-JUN').sum()['2005-07-01':'2015-06-30']
sum4b.columns = ['404810']

sns.regplot(sum1b['vcsn'], sum4b['404810'])


### Comparisons of VCSN to stations
pts0 = mssql.rd_sql(server, database, sites_table, ['ExtSiteID', 'NZTMX', 'NZTMY'], where_in={'ExtSiteID': sites}, rename_cols=['site', 'x', 'y']).astype(int)

pts1 = interp2d.points_to_points(both3.reset_index(), 'time', 'x', 'y', 'rain', pts0, 4326, 2193).reset_index().dropna()
pts1['x'] = pts1['x'].round().astype(int)
pts1['y'] = pts1['y'].round().astype(int)

pts2 = pd.merge(pts0, pts1, on=['x', 'y']).drop(['x', 'y'], axis=1)
pts2['site'] = pts2['site'].astype(str)

ts1 = mssql.rd_sql_ts(server, database, ts_table, 'ExtSiteID', 'DateTime', 'Value', where_in={'ExtSiteID': sites,'DatasetTypeID': [15]}).reset_index()
ts1.rename(columns={'ExtSiteID': 'site', 'DateTime': 'time', 'Value': 'station'}, inplace=True)

grp1 = ts1.groupby(['site', pd.Grouper(key='time', freq='M')])[['station']]
ts2 = grp1.sum()
ts2['days'] = grp1.count()

ts2['daysinmonth'] = ts2.index.get_level_values('time').daysinmonth

ts3 = ts2[ts2['days'] == ts2['daysinmonth']].copy()

pts3 = pd.merge(ts3, pts2, on=['site', 'time'])
pts3.rename(columns={'rain': 'vcsn'}, inplace=True)

pts4 = pts3[pts3['station'] > 0].copy()

pts4['abs_normalised_error'] = (pts4['vcsn'] - pts4['station']).abs()/pts4['station']
pts4['normalised_error'] = (pts4['vcsn'] - pts4['station'])/pts4['station']
pts4['month'] = pts4.time.dt.month_name()

mane1 = pts4.groupby('site')['abs_normalised_error'].mean()
bias1 = pts4.groupby('site')['normalised_error'].mean()

## Regressions
ts4 = ts2.station.unstack(0).sort_index()
ts4[ts4 == 0] = np.nan

lm1 = LM(ts4.loc[:, ~ts4.columns.isin(['404810'])], ts4[['404810']])

ols1 = lm1.predict()
ols2 = lm1.predict(x_transform='log', y_transform='log')


## Plotting
pts5 = pts4[pts4.site.isin(['403711', '404810', '405910', '414110'])].copy()
pts5.rename(columns={'site': 'Station', 'normalised_error': 'Normalised Error', 'month': 'Month'}, inplace=True)

ax = sns.boxplot(x='Month', y='Normalised Error', hue='Station', data=pts5, order=['July', 'August', 'September', 'October', 'November', 'December', 'January', 'February', 'March', 'April', 'May', 'June'])
ax.set_ylim(-1, 1)




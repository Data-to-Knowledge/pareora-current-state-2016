# -*- coding: utf-8 -*-
"""
Created on Mon Jun 20 15:09:59 2016

@author: MichaelEK
"""
import pandas as pd
from os import path, makedirs
from niwa import rd_niwa_vcsn
from pdsql import mssql
#from import_fun import rd_vcn
#from ts_stats_fun import w_resample, flow_stats
#from hydro_plot_fun import mon_boxplot, dual_mon_boxplot, multi_yr_barplot, reg_plot

######################################
### Parameters

server = 'edwprod01'
database = 'hydro'
ts_summ_table = 'TSDataNumericDailySumm'
ts_table = 'TSDataNumericDaily'

sites = ['403711', '405711', '404810', '405910', '414110', '405610', '403601', '417110', '407810', '400910']

data_dir = 'Y:/VirtualClimate/VCN_precip_ET_2016-06-06'
site_loc_csv = 'pareora_vcn.csv'
site_col = 'Network'

data_type1 = 'PET'
data_type2 = 'precip'
mtypes = ['precip', 'PET' ]

#save_path1 = 'Y:/VirtualClimate/test1_output.csv'

export_path = 'C:/ecan/shared/projects/otop/reports/current_state_2016/figures/climate_v03'
et_mon_png = 'pareora_et_mon.png'
et_yr_png = 'pareora_et_yr.png'
precip_mon_png = 'pareora_precip_mon.png'
precip_yr_png = 'pareora_precip_yr.png'
diff_mon_png = 'pareora_diff_mon.png'
diff_yr_png = 'pareora_diff_yr.png'

## Create directories if needed
if not path.exists(export_path):
    makedirs(export_path)

#####################################
### Create dataframe from many VCN csv files

sites1 = pd.read_csv(site_loc_csv)[site_col].tolist()

both1 = rd_niwa_vcsn(mtypes, sites1)

#et = rd_vcn(data_dir=data_dir, select=site_loc_csv, site_col=site_col,  data_type=data_type1, export=False, export_path=save_path1)
#precip = rd_vcn(data_dir=data_dir, select=site_loc_csv, site_col=site_col, data_type=data_type2, export=False, export_path=save_path1)

## Run stats

day_mean1 = both1.groupby('time')[['rain', 'pe']].mean()
year_sum1 = day_mean1.resample('A-JUN').sum()[:-1]

year_sum1['1988':'2015'].describe()

year_sum1['1992':'2015'].describe()
year_sum1.describe()

### Get station data
ts_summ1 = mssql.rd_sql(server, database, ts_summ_table, where_in={'ExtSiteID': sites,'DatasetTypeID': [15]})
#ts_summ2 = mssql.rd_sql(server, database, ts_summ_table, where_col={'DatasetTypeID': [24]})
#ts_summ2['FromDate'] = pd.to_datetime(ts_summ2['FromDate'])
#ts_summ2['ToDate'] = pd.to_datetime(ts_summ2['ToDate'])
#
#ts_summ2 = ts_summ2[ts_summ2.ToDate > '2016']

ts1 = mssql.rd_sql(server, database, ts_table, ['DateTime', 'Value'], where_in={'ExtSiteID': ['414110'],'DatasetTypeID': [15]})
ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
ts1.set_index('DateTime', inplace=True)
year_sum2 = ts1.resample('A-JUN').sum()

year_sum2.describe()

ts1 = mssql.rd_sql(server, database, ts_table, ['DateTime', 'Value'], where_in={'ExtSiteID': ['403711'],'DatasetTypeID': [15]})
ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
ts1.set_index('DateTime', inplace=True)
year_sum3 = ts1.resample('A-JUN').sum()

year_sum3.describe()

ts1 = mssql.rd_sql(server, database, ts_table, ['DateTime', 'Value'], where_in={'ExtSiteID': ['404810'],'DatasetTypeID': [15]})
ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
ts1.set_index('DateTime', inplace=True)
year_sum4 = ts1.resample('A-JUN').sum()

year_sum4.describe()

ts1 = mssql.rd_sql(server, database, ts_table, ['DateTime', 'Value'], where_in={'ExtSiteID': ['405910'],'DatasetTypeID': [15]})
ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])
ts1.set_index('DateTime', inplace=True)
year_sum5 = ts1.resample('A-JUN').sum()

year_sum5.describe()

####################################
### Plot yearly and monthly values

#mon_boxplot(et_day, dtype='PET', fun='sum', export_path=export_path, export_name=et_mon_png)
#mon_boxplot(precip_day, dtype='precip', fun='sum', export_path=export_path, export_name=precip_mon_png)
#mon_boxplot(diff, dtype='diff', fun='sum', export_path=export_path, export_name=diff_mon_png)
#
#multi_yr_barplot(precip_day, et_day, col='all', dtype='both', single=True, fun='sum', start='1992', end='2015', alf=False, export_path=export_path, export_name=precip_yr_png)

ts1 = mssql.rd_sql(server, database, ts_table, ['ExtSiteID', 'DateTime', 'Value'], where_in={'ExtSiteID': ['405910'],'DatasetTypeID': [15]})
ts1['DateTime'] = pd.to_datetime(ts1['DateTime'])




import os, pickle, datetime
import smtplib, pysftp
import subprocess

import numpy as np
import pandas as pd

from copy import copy

from settings import *
from utils import *

# -----------------------------------------------------------------------------
# Configuration and status check functions
# -----------------------------------------------------------------------------

def load_configuration(*args, **kwargs):

    kwargs = {'ip_address': EC_IP_ADDRESS,
              'ec_login': EC_LOGIN,
              'ec_password': EC_PASSWORD,
              'ec_ssh_key': EC_SSH_KEY,
              'column_names' : EC_DATA_VARIABLES,
              'local_data_directory': EC_DATA_DIRECTORY
              }

    current_directory = os.getcwd()
    os.chdir(kwargs['local_data_directory'])
    if os.path.exists('last_contact.pk'):
        with open('last_contact.pk', 'rb') as file_obj:
            kwargs['last_contact'] = pickle.load(file_obj)
    else:
        # XXX: Placeholder date to download *all* data from station
        kwargs['last_contact'] = datetime.datetime(1, 1, 2017) 
    if os.path.exists('files_downloaded.pk'):
        with open('files_downloaded.pk', 'rb') as file_obj:
            kwargs['files_downloaded'] = pickle.load(file_obj)

    os.chdir(current_directory)

    return kwargs

def check_if_instrument_is_alive(ip_address, 
                                 last_contact=None,
                                 **kwargs):

    current_time = datetime.datetime.now()
    if ping(ip_address): 
        last_contact = current_time

    return {'this_contact': last_contact}

def record_last_time_instrument_was_contacted(this_contact, 
                                              local_data_directory, 
                                              **kwargs):

    current_directory = os.getcwd()
    os.chdir(local_data_directory)
    with open('last_contact.pk', 'wb') as file_obj:
        pickle.dump(this_contact, file_obj)
    os.chdir(current_directory)

# -----------------------------------------------------------------------------
# Data download functions
# -----------------------------------------------------------------------------

def download_data(ip_address, 
                ec_login, 
                ec_password, 
                ec_ssh_key, 
                local_data_directory, 
                **kwargs):
    
    current_dir = os.getcwd()
    last_contacted = kwargs.get('last_contact')
    last_contacted = last_contacted.replace(minute=0)
    today = datetime.datetime.today()
    today = today.replace(minute=0)

    print('-'*79)
    print("Running pipeline on " + today.isoformat())
    print('-'*79)

    os.chdir(local_data_directory)
    
    files_downloaded={'summaries' : []} 
    for key in files_downloaded:
        if not os.path.isdir(key):
            os.mkdir(key)
        os.chdir(key)

        # XXX: Lag by 1 day to avoid downloading summary files being modified...
        last_contacted_lag = last_contacted.replace(minute=0) - datetime.timedelta(days=2)
        yesterday = today - datetime.timedelta(days=1)
        files_in_directory = list_summary_filenames_in_time_window(last_contacted_lag, yesterday)
        print("Downloading data from " + last_contacted_lag.isoformat() 
                + " to " + yesterday.isoformat())

        files_downloaded = os.listdir('.')
        files_to_download = set(files_in_directory) - set(files_downloaded)
        print("Downloading {} files from {}/".format(len(files_to_download), key))

        while len(files_to_download) > 0:
            file_to_download = files_to_download.pop()
            file_failed_to_download = False
            try:
                remote_path = '/home/licor/data/' + key + '/' + file_to_download 
                command = ["scp", "-o IdentityFile=" + ec_ssh_key, 
                        ec_login + "@" + ip_address + ":" + remote_path, "."]
                print("Downloading " + file_to_download + "...")
                output = subprocess.check_output(command)
            except:
                file_failed_to_download = True
                print(file_to_download + " failed to download!")

        os.chdir('..')
    os.chdir(current_dir)

    return {'files_downloaded': files_downloaded}
    
def record_new_data_that_was_transferred(local_data_directory, 
                                        files_downloaded, 
                                        **kwargs):

    current_directory = os.getcwd()
    os.chdir(local_data_directory)
    with open('files_downloaded.pk', 'wb') as file_obj:
        pickle.dump(files_downloaded, file_obj)
    os.chdir(current_directory)

# -----------------------------------------------------------------------------
# Data manipulation and filtering functions
# -----------------------------------------------------------------------------

def load_data(**kwargs):

    local_data_directory = kwargs.get('local_data_directory')
    local_summary_directory = os.path.join(local_data_directory, "summaries")
    
    pdargs = {}
    pdargs['parse_dates'] = [['date', 'time']]

    data_list = [pd.read_table(os.path.join(local_summary_directory, f), 
                header=0, skiprows=[1], **pdargs) for f in 
                os.listdir(local_summary_directory) if 'EP' in f]

    data = pd.concat(data_list)
   
    column_names = list(copy(data.columns))
    if 'date_time' in data.columns:
        data.index = data['date_time']
        column_names.remove('date_time')

    return {'data' : data[column_names]}

def update_master_file(data, **kwargs):

    master_data = pd.read_pickle('master.pk')
    keep_columns = master_data.columns
    data = data[keep_columns]
    data = data[data.index > master_data.index.max()]
    master_data = master_data.append(data)

    with open('master.pk', 'wb') as file_obj:
        pickle.dump(master_data, file_obj)

    return {'data' : master_data}

def convert_data_units(data, **kwargs):

    # Fluxes in g/m2/d
    data['co2_flux'] *= CO2_FLUX_CONVERSION_FACTOR
    data['h2o_flux'] *= H2O_FLUX_CONVERSION_FACTOR

    # Concentration in umol/m3
    data['h2o_molar_density'] *= H2O_MOLAR_DENSITY_CONVERSION_FACTOR
    
    # Temperature in deg. Celcius
    data['air_temperature'] -= KELVIN_CONVERSION_OFFSET 

    data['v_sd'] = np.sqrt(data['v_var'])
    
    return {'data' : data}

def filter_co2_flux(data, min_value=-500, max_value=5e4):

    return data[(data['co2_flux'] > min_value) & (data['co2_flux'] < max_value)]

def filter_H(data, min_value=-500, max_value=5e4):

    return data[(data['H'] > min_value) & (data['H'] < max_value)]

def filter_LE(data, min_value=-500, max_value=5e4):

    return data[(data['LE'] > min_value) & (data['LE'] < max_value)]

def filter_h2o_flux(data, min_value=-500, max_value=5e4):

    return data[(data['h2o_flux'] > min_value) & (data['h2o_flux'] < max_value)]

def filter_frictional_velocity(data, thresh=0.3):

    return data[data['u*'] >= thresh]

def filter_mean_wind_dir(data, sigma):

    mean_dir = data.wind_dir.mean()

    return data[np.abs(data['wind_dir'] - mean_dir) <= sigma]

def filter_qc(data, keep_value=0):

    return data[data['qc_co2_flux'] == keep_value]

def filter_data(data, **kwargs):
    
    data = filter_qc(data)
    data = filter_frictional_velocity(data)
    data = filter_co2_flux(data)
    data = filter_H(data)
    data = filter_LE(data)

    return {'data' : data}

def calculate_centered_moving_average(data, window_size=48):

    idx = np.argsort(data.index)
    data = data.iloc[idx]
    data = standardize_timestamps(data)
    data = fill_missing_data(data)
    ma = data.rolling(window_size, min_periods=2, center=True)
    mean = ma.mean()
    sd = ma.std()

    return {'daily_mean' : mean, 'daily_sd' : sd}


# -----------------------------------------------------------------------------
# Main thread
# -----------------------------------------------------------------------------

if __name__ == "__main__":

    output = run_pipeline(EC_PIPELINE)

    daily_mean = output['daily_mean']
    daily_sd = output['daily_sd']

    # TODO: Set up SQLAlchmey engine or similar...
    engine = ...

    daily_mean.to_sql('<name of EC VALVE table>', engine, if_exists='replace')



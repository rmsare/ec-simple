


# -----------------------------------------------------------------------------
# Pipeline settings 
# -----------------------------------------------------------------------------
EC_DATA_DIRECTORY = '<path to data dir>'
EC_RESULTS_DIRECTORY = '<path to results dir>'
EC_LOGIN = 'licor'
EC_PASSWORD = 'licor'
EC_SSH_KEY = '<path to public key>'
EC_IP_ADDRESS = '166.248.227.207'
EC_DATA_VARIABLES = ['DOY',
                     'daytime',
                     'H',
                     'qc_H',
                     'LE',
                     'qc_LE',
                     'co2_flux',
                     'qc_co2_flux',
                     'h2o_flux',
                     'qc_h2o_flux',
                     'co2_molar_density',
                     'h2o_molar_density',
                     'air_temperature',
                     'air_pressure',
                     #'rand_err_co2_flux',
                     'wind_speed',
                     'wind_dir',
                     'u*',
                     'TKE',
                     'L',
                     '(z-d)/L',
                     'v_var'
                    ]

EC_PIPELINE = ('pipeline.load_configuration',
             'pipeline.check_if_instrument_is_alive',
             'pipeline.download_data',
             'pipeline.record_last_time_instrument_was_contacted',
             'pipeline.load_data',
             'pipeline.update_master_file',
             'pipeline.convert_data_units',
             'pipeline.filter_data',
             'pipeline.calculate_centered_moving_average'
             )

# -----------------------------------------------------------------------------
# Unit conversion constants
# -----------------------------------------------------------------------------
CO2_FLUX_CONVERSION_FACTOR = 3.8 # mmol/m2/s -> g/m2/day
H2O_FLUX_CONVERSION_FACTOR = 1555.2 # mmol/m2/s -> g/m2/day
H2O_MOLAR_DENSITY_CONVERSION_FACTOR = 1000 # mmol/m3 -> umol/m3
KELVIN_CONVERSION_OFFSET = 273.15 # Kelvin -> deg. Celcius


from pyspark.sql.functions import (
    rand, expr, row_number, monotonically_increasing_id, 
    dense_rank, col, lit, to_date, when
)
from pyspark.sql import Window
import pandas as pd
import numpy as np
import datetime
import random
from random import randint, shuffle, random

def get_starting_temps(start, end, frequency, amplitude, noisy, trend, mean, std_dev, year=True, **kwargs) -> pd.DataFrame:
    dates = pd.date_range(start, end)
    num_rows = len(dates)
    if year:
        normal1 = np.random.normal(loc=mean, scale=std_dev, size=num_rows // 2).clip(min=0, max=78)
        normal1 = np.sort(normal1)
        normal2 = np.random.normal(loc=mean, scale=std_dev, size=num_rows // 2 + num_rows % 2).clip(min=0, max=78)
        normal2 = np.sort(normal2)[::-1]
        normal = np.concatenate((normal1, normal2))
    else:
        normal = np.random.normal(loc=mean, scale=std_dev, size=num_rows).clip(min=0, max=78)
    noise = 9.5 * make_timeseries('autoregressive', num_rows, frequency, amplitude, noisy=noisy, trend_factor=trend)
    temps = normal + noise
    pdf = pd.DataFrame({'date': dates, 'starting_temp': temps})
    pdf['date'] = pdf['date'].dt.date
    return pdf

def create_initial_df(spark, num_rows, num_devices, **kwargs):
    factory_ls = ["'A06'", "'D18'", "'J15'", "'C04'", "'T10'"]
    model_ls = ["'SkyJet134'", "'SkyJet234'", "'SkyJet334'", "'EcoJet1000'", "'JetLift7000'",
                "'EcoJet2000'", "'FlyForceX550'", "'TurboFan3200'", "'SkyBolt1'", "'SkyBolt2'",
                "'MightyWing1100'", "'EcoJet3000'", "'AeroGlider4150'", "'SkyBolt250'"]
    return (
        spark.range(num_rows).withColumn('device_id', (rand(seed=0)*num_devices).cast('int') + 1)
        .withColumn('factory_id', expr(f"element_at(array({','.join(factory_ls)}), abs(hash(device_id)%{len(factory_ls)})+1)"))
        .withColumn('model_id', expr(f"element_at(array({','.join(model_ls)}), abs(hash(device_id)%{len(model_ls)})+1)"))
        .drop('id')
    )

def timestamp_sequence_lengths(total, minimum, maximum):
    nums = []
    while total > 0:
        if total < 300:
            n = total
        else:
            n = randint(minimum, maximum)
        nums.append(n)
        total -= n
    shuffle(nums)
    return nums

def get_datetime_list(start_date, end_date, length=None, freq='1 min'):
    if not length:
        length = randint(10, 300)
    time_diff = (end_date - start_date).total_seconds()
    random_second = randint(0, int(time_diff))
    rand_datetime = start_date + datetime.timedelta(seconds=random_second)
    return pd.date_range(start=str(rand_datetime), periods=length, freq=freq)

def create_add_timestamps_func(column_name, frequency, amplitude, minimum, maximum, start, end, **kwargs):
    def add_timestamps(pdf: pd.DataFrame) -> pd.DataFrame:
        num_rows = len(pdf)
        lengths = timestamp_sequence_lengths(num_rows, minimum, maximum)
        timestamp_sequences = [pd.Series(get_datetime_list(start, end, length)) for length in lengths]
        pdf[column_name] = pd.concat(timestamp_sequences, ignore_index=True)
        return pdf
    return add_timestamps

timestamp_schema = '''device_id string, factory_id string, model_id string, timestamp timestamp'''

def sinus_signal(frequency, amplitude, time):
    return np.sin(2*np.pi*frequency * time) * amplitude

def periodic_signal(frequency, amplitude, time):
    freq_val = np.random.normal(loc=frequency, scale=0.5, size=1)
    amplitude_val = np.random.normal(loc=amplitude, scale=1.2, size=1)
    return float(amplitude_val * np.sin(freq_val * time))

def make_timeseries(pattern, num_points, frequency, amplitude, trend_factor=0, noisy=0.3):
    times = np.linspace(0, 10, num_points)
    noise = np.random.normal(loc=0, scale=noisy, size=num_points)
    trend = times * trend_factor

    timeseries = np.zeros(num_points)
    if num_points < 2: 
        return timeseries
    if pattern=='sinusoid':
        for i in range(num_points):
            timeseries[i] = noise[i] + sinus_signal(frequency, amplitude, times[i]) + trend[i]
    elif pattern=='periodic':
        for i in range(num_points):
            timeseries[i] = noise[i] + periodic_signal(frequency, amplitude, times[i]) + trend[i]
    elif pattern=='autoregressive':
        timeseries[0] = random() + noisy
        timeseries[1] = (random()+1) * (noisy + .5)
        for i in range(2, num_points):
            timeseries[i] = noise[i] + (timeseries[i-1] * (1+trend_factor)) - (timeseries[i-2] * (1-trend_factor))
    return timeseries

def create_add_weather_func(num_rows, frequency, amplitude, trend, noisy, **kwargs):
    def add_weather(pdf: pd.DataFrame) -> pd.DataFrame:
        num_rows = len(pdf)
        start_temp = pdf['starting_temp'].iloc[0]
        pdf['pd_timestamp'] = pd.to_datetime(pdf['timestamp'])
        min_time = pdf['pd_timestamp'].min()
        coldest = pdf['pd_timestamp'].dt.normalize() + pd.DateOffset(hours=randint(5, 8))
        hottest = pdf['pd_timestamp'].dt.normalize() + pd.DateOffset(hours=randint(14, 18))
        hottest_time = hottest[0]
        coldest_time = coldest[0]
        timedelta_from_hottest = hottest - pdf['pd_timestamp']
        peak_timestamp_idx = min(num_rows-1, timedelta_from_hottest.idxmin())
        upwards = min_time < hottest_time and min_time > coldest_time
        adjusted_trend_factor = trend
        if not upwards:
            adjusted_trend_factor = -trend
        pdf['temperature'] = start_temp + make_timeseries('periodic', num_rows, frequency, amplitude, adjusted_trend_factor, noisy)
        pdf = pdf.drop('pd_timestamp', axis=1)
        random_lapse_rate = 1.2 + np.random.uniform(.5, 1.5)
        pdf['air_pressure'] = randint(913, 1113) - (pdf['temperature'] - 15) * random_lapse_rate
        return pdf
    return add_weather

weather_schema = '''device_id string, factory_id string, model_id string, timestamp timestamp, date date,
                    trip_id integer, starting_temp double, temperature double, air_pressure double'''

def create_add_lifetime_features_func(num_rows, frequency, amplitude, trend, noisy, **kwawrgs):
    def add_lifetime_features(pdf: pd.DataFrame) -> pd.DataFrame:
        num_points = len(pdf)
        sinusoidal = make_timeseries('sinusoid', num_points, frequency, amplitude, trend, noisy)
        pdf['density'] = abs(sinusoidal - np.mean(sinusoidal)/2) 
        return pdf
    return add_lifetime_features

lifetime_schema = '''device_id string, trip_id int, factory_id string, model_id string, timestamp timestamp,  
                     air_pressure double, temperature double, density float'''

def create_add_trip_features_func(num_rows, frequency, amplitude, trend, noisy, **kwargs):
    def add_trip_features(pdf: pd.DataFrame) -> pd.DataFrame:
        num_points = len(pdf)
        rotation = abs(make_timeseries('autoregressive', num_points, frequency, amplitude)) *100
        init_delay = make_timeseries('sinusoid', num_points, frequency, amplitude, trend, noisy)
        pdf['delay'] =  abs(init_delay * np.sqrt(rotation))
        pdf['rotation_speed'] = rotation
        pdf['airflow_rate'] =  pdf['rotation_speed'].shift(5) / pdf['air_pressure']
        pdf = pdf.fillna(method='bfill') # There shouldn't be nulls, but fill to eliminate
        pdf = pdf.fillna(method='ffill') # the possibility of the notebooks failing
        pdf = pdf.fillna(0)
        return pdf
    return add_trip_features

trip_schema = '''device_id string, trip_id int, factory_id string, model_id string, timestamp timestamp, airflow_rate double,  
            rotation_speed double, air_pressure double, temperature double, delay float, density float'''

def add_defects(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values(['timestamp'])
    pdf['temp_ewma'] = pdf['temperature'].shift(1).ewm(5).mean()
    pdf['temp_difference'] = pdf['temperature'] - pdf['temp_ewma']
    percentile89 = pdf['temperature'].quantile(0.89)
    percentile93 = pdf['temperature'].quantile(0.93)

    conditions = [
      (pdf['temp_difference'] > 1.8) & (pdf['factory_id'] == 'A06') & (pdf['temperature'] > percentile89),
      (pdf['temp_difference'] > 1.8) & (pdf['model_id'] == 'SkyJet234') & (pdf['temperature'] > percentile89),
      (pdf['temp_difference'] > 1.9) & (pdf['model_id'] == 'SkyJet334') & (pdf['temperature'] > percentile93),
      (pdf['delay'] > 39) & (pdf['rotation_speed'] > 590),
      (pdf['density'] > 4.2) & (pdf['air_pressure'] < 780),
    ]
    outcomes = [round(random()+.8), round(random()+.85), round(random()+.85), round(random()+.85), round(random()+.95)]
    pdf['defect'] = np.select(conditions, outcomes, default=0)
    pdf['defect'] = pdf['defect'].shift(20, fill_value=0)
    pdf = pdf.drop(['temp_difference', 'temp_ewma'], axis=1)        
    return pdf

defect_schema = '''device_id string, trip_id int, factory_id string, model_id string, timestamp timestamp, airflow_rate double,  
                    rotation_speed double, air_pressure double, temperature double, delay float, density float, defect float'''

def generate_iot(spark, dgconfig):
    temps = get_starting_temps(**dgconfig['shared'] | dgconfig['temperature']['lifetime'])
    starting_temps = spark.createDataFrame(temps)
    add_timestamps = create_add_timestamps_func(**dgconfig['shared'] | dgconfig['timestamps'])
    add_weather = create_add_weather_func(**dgconfig['shared'] | dgconfig['temperature']['trip'])
    add_lifetime_features = create_add_lifetime_features_func(**dgconfig['shared'] | dgconfig['lifetime'])
    add_trip_features = create_add_trip_features_func(**dgconfig['shared'] | dgconfig['trip'])
    return (
        create_initial_df(spark, **dgconfig['shared'])
        .withColumn('device_id', col('device_id').cast('string'))
        .groupBy('device_id').applyInPandas(add_timestamps, timestamp_schema)
        .withColumn('date', to_date(col('timestamp')))
        .withColumn('trip_id', dense_rank().over(Window.partitionBy('device_id').orderBy('date')))
        .join(starting_temps, 'date', 'left')
        .groupBy('trip_id', 'device_id').applyInPandas(add_weather, weather_schema)
        .drop('starting_temp', 'date')
        .groupBy('device_id').applyInPandas(add_lifetime_features, lifetime_schema)
        .groupBy('device_id', 'trip_id').applyInPandas(add_trip_features, trip_schema)
        .groupBy('device_id').applyInPandas(add_defects, defect_schema)
    )

def delete_entry(w, entry):
    if entry.is_directory:
        w.files.delete_directory(entry.path)
    else:
        w.files.delete(entry.path)



def land_more_data(spark, dbutils, config, dgconfig):
    print('Generating data, this may take a minute...')
    iot_data = generate_iot(spark, dgconfig)
    iot_data.write.format('delta').mode('overwrite').save(config['checkpoints']+'/tmp')
    entire_dataset = spark.read.format('delta').load(config['checkpoints']+'/tmp')
    sensor_data = (
        entire_dataset.drop('defect')
        .withColumn('temperature', when(rand() < 0.1, None).otherwise(col('temperature')))
        .withColumn('air_pressure', when(rand() < 0.05, -col('air_pressure')).otherwise(col('air_pressure')))
        .withColumn('timestamp', expr('timestamp + INTERVAL 3 SECONDS * rand()'))
    )
    num_defects = entire_dataset.count() * .004
    defect_data = (
        entire_dataset.select('defect', 'timestamp', 'device_id')
        .where('defect = 1').orderBy(rand()).limit(int(num_defects/16))
        .union(
            entire_dataset.select('defect', 'timestamp', 'device_id')
            .where('defect = 0').orderBy(rand()).limit(int(15*num_defects/16))
        )
        .withColumn('timestamp', expr('timestamp + INTERVAL 2 HOURS * (2 + rand())'))
        .withColumn('timestamp', when(rand() < 0.05, None).otherwise(col('timestamp')))
    )
    sensor_data.write.mode('append').csv(config['sensor_landing'], header='true')
    defect_data.write.mode('append').csv(config['inspection_landing'], header='true')
    dbutils.fs.rm(config['checkpoints']+'/tmp', recurse=True)

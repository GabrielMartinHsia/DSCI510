#!/usr/bin/env python3

from bs4 import BeautifulSoup
import json
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import re
import requests
import seaborn as sns
from sqlalchemy import create_engine
import sys

# CREATE SQL ENGINES... DATABASES IN THIS FILEPATH
demo_engine = create_engine('sqlite:///ScrapeDemo.db',echo=False)
engine = create_engine('sqlite:///FinalProjectGMH.db',echo=False)

#__________________________________________________________________________________________________________________
def coordinates(location):
    '''
    Uses the opencagedata geocoding API to return latitude and longitude for an input city name.

    Input: list of city name(s) as string(s). Defaults to Anchorage and Auckland (since I've already scraped LA
    and Manila. The idea is to get a few locations around the perimeter of the Pacific)
    '''
    if np.ndim(location) < 1: #if location entered as a string instead of a list containing a string...
        location = [location]

    my_key = '4ffeb0c77c5c4d9a8962be54e9d6c010'
    base_url = 'https://api.opencagedata.com/geocode/v1/json'

    latlon = {}
    for loc in location:
        encoded_loc = requests.utils.quote(loc)
        url = f'{base_url}?q={encoded_loc}&key={my_key}'
        try:
            js = requests.get(url).json()
            major_loc = js.get('results')[0] #likely to return many cities with the name. biggest is top of list.
            lat, lon = major_loc['geometry']['lat'], major_loc['geometry']['lng']
        except:
            print(f"could not get url for '{loc}'")
            lat, lon = None, None

        if lat:
            latlon[loc] = f'{lat:.4f},{lon:.4f}'
        else:
            latlon[loc] = None
            print('invalid location entered')
    return latlon

#_________________________________________________________________________________________________________________
def weather_data(location={'USC':'34.0224,-118.2851'}, start='1950-01-01', end='2022-01-01'):
    '''
    WARNING: RUNNING THIS FUNCTION WITH DEFAULT start & end ARGUMENTS WILL TAKE 6+ HOURS TO COMPLETE.
    IT SCRAPES A darksky.net URL FOR EVERY SINGLE DAY FROM '1950-01-01' TO '2022-01-01' AT THE LOCATION SPECIFIED.
    EACH DAY IS COMPRISED OF 24 OBSERVATIONS (ON THE HOUR) OF 24 DIFFERENT WEATHER PARAMETERS.
    IT CREATES A DICTIONARY WITH 72 * 365 + LEAP DAYS = 26,299 ENTRIES, I.E., 1 DICT ENTRY PER DAY.
    EACH DICT ENTRY IS THE RAW TEXT SCRAPED FROM THE URL FOR THAT DAY/LOCATION, CONTAINING 24 HR x 24 PARAMS = 576
    ELEMENTS. SO THAT'S 576 * 26,299 = 15,148,224 DATA ELEMENTS TOTAL.

    IT THEN WRANGLES THE DATA FROM THIS DICTIONARY INTO A DICTIONARY OF DATA FRAMES, WHICH ARE THEN APPENDED TO
    ONE ANOTHER TO PRODUCE A SINGLE, COMPLETE DATAFRAME (THIS PART TAKES ROUGHLY HALF OF THE 6+ COMPLETION HOURS).
    THIS DATAFRAME IS THEN STORED AS A SQL TABLE WHICH CAN BE QUERRIED AS NEEDED.

    IF YOU DON'T WANT YOUR COMPUTER TIED UP FOR 6+ HOURS, YOU'RE PROBABLY BETTER OFF JUST QUERYING THIS
    DATA FROM THE SQL DATABASE 'FinalProjectGMH.db' PROVIDED IN THE ZIPPED FOLDER. I'LL PROBABLY JUST HAVE THE
    SCRIPT DO SUCH A QUERY AUTOMATICALLY INSTEAD OF CALLING THIS weather_data() FUNCTION. TO SHOW THAT THIS FUNCTION
    ACTUALLY DOES WHAT IT SAYS IT DOES, I'LL HAVE THE SCRIPT CALL IT, BUT FOR AN EXTREMELY TRUNCATED DATE RANGE.
    IT WILL SAVE THE DATA TO AN ENTIRELY DIFFERENT ScrapeDemo.db SQL DATABASE, SO AS NOT TO INTERFERE WITH THE OTHER
    ANALYSIS TASKS THAT PULL FROM THE TRUE (AND MUCH LARGER) FinalProjectGMH.db DATABASE.

    THERE'S CERTAINLY ROOM FOR IMPROVEMENT HERE > I PROBABLY SHOULD HAVE USED SOME KIND OF BATCH PROCESSING FOR THIS
    SINCE IT'S OBVIOUSLY NOT SUPER ROBUST AND IT'D BE NICE TO AT LEAST RETAIN WHAT'S BEEN SCRAPED SO FAR IN THE
    EVENT IT FAILS OUT. FRANKLY, I WAS PRETTY SHOCKED IT ACTUALLY WORKED THE FIRST TIME I RAN IT FOR THE WHOLE
    72-YEAR DATE RANGE (USC CAMPUS LOCATION), AT WHICH POINT I HADN'T EVEN PROVIDED ANY TRY/EXCEPT BLOCKS TO CATCH
    BAD URLs. THE SECOND TIME I TRIED IT (MANILA), I LET IT RUN FOR 12+ HOURS BEFORE FINALLY INTERRUPTING IT. I
    STILL DON'T KNOW WHAT THE ISSUE WAS - SEEMS LIKE PERHAPS SOME KIND OF INFINTE LOOP(?) BUT IT DID RENDER USEFUL
    DATA. THIRD ATTEMPT (DARWIN, AUSTRALIA) FAILED WITH THE DREADED "COOL YOUR JETS, BUDDY - THAT'S QUITE ENOUGH
    SERVER HITS FOR YOU!" MESSAGE FROM Darksky.net :/ ... IT APPEARS MY IP HAS NOT BEEN OUTRIGHT BLOCKED, AS I'M
    STILL ABLE TO SCRAPE TRUNCATED DATE RANGES. I'VE YET TO TRY ANOTHER FULL RUN.

    '''
    date_range = pd.date_range(start=start, end=end, freq='D')
    days = [str(day)[:10] for day in date_range]#[:10] to exclude HH:MM:SS component
    urls = []
    for day in days:
        url = f'https://darksky.net/details/{list(location.values())[0]}/{day}/us12/en'
        urls.append(url)

    raw_days = dict.fromkeys(days) #let's first just collect the raw text from each URL in here...
    for day, url in list(zip(days,urls)):
        try:
            page = requests.get(url)
            soup = BeautifulSoup(page.content, 'html.parser')
            dataTag = soup.findAll('script')[1] #second <script> tag contains all hourly data... the jackpot
        except IndexError:
            print(f'could not make dataTag for {day}')
            dataTag = None

        if dataTag:
            raw_days[day] = dataTag.text
        else:
            print(f"dictionary value for key '{day}' entered as None Type")
            raw_days[day] = None

    #TURN RAW "DICT-LIKE" TEXT INTO ACTUAL DICTIONARY OF ALL DAYS' ACTUAL DATA
    dict_days = dict.fromkeys(days)
    for day, txt in raw_days.items():
        hours = re.findall(r'(\{.*?\})',txt) #list type, containing dict-like elements: {..hour's text data..}
        hourly_data = [] #will append actual hourly dictionaries to this list
        for hour in hours[:24]: #leave out the last two - they're not hours... other stuff we don't want
            data_list = hour.split(',') #split each {'dictionary'} on commas into its key-val pairs
            keys = ['time',
                'summary',
                'icon',
                'precipIntensity',
                'precipProbability',
                'precipType',
                'temperature',
                'apparentTemperature',
                'dewPoint',
                'humidity',
                'pressure',
                'windSpeed',
                'windGust',
                'windBearing',
                'cloudCover',
                'uvIndex',
                'visibility',
                'ozone',
                'azimuth',
                'altitude',
                'dni',
                'ghi',
                'dhi',
                'etr'
               ]

            intstr = r':(-*\d+)'       #regex to get integer values
            fltstr = r':(-*\d+\.\d+)'  #regex to get float values

            hour_dict = dict.fromkeys(keys)
            for data in data_list:
                for key in keys:
                    if key in data:

                        if re.search(fltstr, data):
                            valflt = float(''.join(re.findall(fltstr, data)))
                            hour_dict[key] = valflt

                        elif re.search(intstr, data):
                            valint = int(''.join(re.findall(intstr, data)))
                            hour_dict[key] = valint

                        else:
                            valstr = str(''.join(re.findall(r':"(.*)"', data)))
                            hour_dict[key] = valstr

            hourly_data.append(hour_dict)

        dict_days[day] = hourly_data #store the list of hourly dictionaries in this day's dictionary spot

    #MELT FROM DICT OF HOURS INTO DICT OF "KEYS" I.E., WEATHER PARAMETERS (24-ELM LIST VARIABLE ABOVE)
    melted_days = dict.fromkeys(days)
    for day, hours in dict_days.items():
        melted = dict.fromkeys(keys)
        for k in keys:
            for h in hours[:24]: #the last two are not hourly data
                if melted[k]:#if already any entry for this weather parameter ("key"), append next hour's value
                    melted[k].append(h[k])
                else:#otherwise, this is the first entry for this parameter...
                    melted[k] = [h[k]]
        melted_days[day] = melted

    #GATHER INTO DICTIONARY OF DATA FRAMES
    dframes = dict.fromkeys(days)
    for day, data in melted_days.items():
        try:
            dframes[day] = pd.DataFrame(data)
        except IndexError:
            print(f'cannot create dataframe for {day}')
            dframes[day] = None

    #APPEND DATA FRAMES INTO ONE GIANT ONE ... THIS TAKES 3+ HOURS TO COMPLETE
    df_list = list(dframes.values())
    df_complete = df_list[0]
    for i in range(len(df_list)-1):
        try:
            df_complete = df_complete.append(df_list[i+1])
        except IndexError:
            print(f'could not append dataframe {i+1}')

    #STORE THIS GIANT DF IN A SQL DB TABLE
    df_complete.to_sql(f'weather_{list(location.keys())[0]}', con=demo_engine, if_exists='replace')
    return df_complete


#______________________________________________________________________________________________________________
def solar_data():
    '''
    Scrape and parse json data. collect in dataframe. connect sqlalchemy engine and store
    dataframe as table in database
    '''
    url = 'https://services.swpc.noaa.gov/json/solar-cycle/observed-solar-cycle-indices.json'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    solar_data = json.loads(soup.text)
    df_solar = pd.DataFrame(solar_data)
    df_solar.rename(columns={'time-tag':'time'}, inplace=True)
    df_solar['time'] = pd.to_datetime(df_solar['time'])
    df_solar.to_sql('solar_cycle', con=demo_engine, if_exists='replace')
#     return df_solar

#_______________________________________________________________________________________________________________
def ENSO_data():
    '''
    Scrape ENSO data. save to SQL db.
    '''
    url = 'https://www.cpc.ncep.noaa.gov/data/indices/soi'
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')
    raw = soup.text.splitlines()

    start, end = 1951, 2022
    ENSO = []
    for line in raw[4:(end-start+4)]:
        for element in line.split()[1:]:
            ENSO.append(float(element))

    months = pd.date_range(start='1951-01', end='2022-01', freq='M')

    df_ENSO = pd.DataFrame({'time':months, 'SOI':ENSO})
    df_ENSO['time'] = pd.to_datetime(df_ENSO['time'])
    df_ENSO.to_sql('ENSO', con=demo_engine, if_exists='replace')
#     return df_ENSO

#________________________________________________________________________________________________________________
def CO2emissions_data():
    df_emissions = pd.read_csv('annual-co2-emissions-per-country.csv')
    df_emissions.rename(columns={'Year':'time'}, inplace=True)
    df_emissions['time'] = pd.to_datetime(df_emissions['time'], format='%Y')
    df_emissions = df_emissions.pivot(index='time', columns='Entity', values='Annual CO2 emissions')
    df_emissions.to_sql('CO2_emitted', con=demo_engine, if_exists='replace')
#     return df_emissions

#_________________________________________________________________________________________________________________
def CO2ppm_data():
    df_ppm = pd.read_csv('co2_mm_mlo.csv', skiprows=[x for x in range(51)])
    time = pd.to_datetime(df_ppm.year.apply(str)+'-'+df_ppm.month.apply(str)).to_frame()
    time.columns=['time']
    df_ppm = df_ppm.merge(time, left_index=True, right_index=True)
    df_ppm['time'] = pd.to_datetime(df_ppm['time'])
    df_ppm.set_index('time', inplace=True)
    df_ppm.to_sql('CO2_ppm', con=demo_engine, if_exists='replace')
#     return df_ppm

#Simple Moving (rolling) Average (SMA):_____________________________________________________________________________________
def SMA(df,column,window_nobs,inplace=False,time_unit=None):
    valid_time_units = {"hourly":24*365, #number of hourly observations to make a 1-yr window
                        "daily":365, #... "..."
                        "monthly":12,
                        "yearly":1}

    if df is df_LA or df is df_Manila:
        unit = 24*365 #hourly data
    elif df is df_ENSO or df is df_solar or df is df_CO2ppm:
        unit = 12 #monthly data
    elif df is df_CO2emitted:
        unit = 1 #yearly data
    else:
        if time_unit in valid_time_units:
#             unit = [x for x in time_unit.values()][0]
            unit = valid_time_units[time_unit]
        else:
            print(f'non-standard data frame "{df}", *unit arg required, Nonetype returned.')
            return None

    if inplace==True:
        df[f'SMA_{window_nobs}_{column}'] = df[column].rolling(unit*window_nobs,min_periods=1).mean()[unit*window_nobs:]
        return df
    else:
        return df[column].rolling(unit*window_nobs, min_periods=1).mean()[unit*window_nobs:]

#_________________________________________________________________________________________________________________
def make_datetime(df_list):
    for df in df_list:
        if df is df_LA or df is df_Manila:
            df['time'] = pd.to_datetime(df['time'], unit='s') #unit='s' for 'numpy.float64' types
        else:
            df['time'] = pd.to_datetime(df['time']) #no additional argument for 'str' types

#_________________________________________________________________________________________________________________
def merge_dframes(df_list):
    dframes_merged = df_list[0]
    for i in range(len(df_list)-1):
        dframes_merged = pd.merge(dframes_merged, df_list[i+1], on='time', how='outer')

    return dframes_merged

#_________________________________________________________________________________________________________________
#_________________________________________________________________________________________________________________
################################################ SCRIPT ##########################################################
#_________________________________________________________________________________________________________________


if __name__ == "__main__":

    if len(sys.argv) == 1: #if only arg is the filename of this script ...
        '''if the only arg is the filename of this script, the following will occur:

           1. Truncated weather data for USC will be scraped and saved in the ScrapeDemo.db. All other data will
               be scraped/imported and saved in their own respective tables in ScrapeDemo.db
           2. Data frames will be constructed via queries of the FinalProjectGMH.db included in this folder.
           3. these data frames will be cleaned and merged for analysis.
           4. heat maps for temperature data will be produced.
           5. A set of stacked subplots will be produced.
           6. A heat map of the correlation table will be produced.'''

        #1. scrape & import data to ScrapeDemo.db
        weather_data(start='1985-01-01', end='1985-01-10') #10 days takes this long... imagine 72 years!!
        solar_data()
        ENSO_data()
        CO2emissions_data()
        CO2ppm_data()

        #2. pull data from FinalProjectGMH.db
        df_LA = pd.read_sql(f"SELECT time, temperature FROM 'weather_Los Angeles'", con=engine)
        df_Manila = pd.read_sql(f"SELECT time, temperature FROM weather_Manila", con=engine)
        df_solar = pd.read_sql(f"SELECT time, ssn, smoothed_ssn FROM solar_cycle", con=engine)
        df_ENSO = pd.read_sql(f"SELECT time, SOI FROM ENSO", con=engine)
        df_CO2emitted = pd.read_sql(f"SELECT time, World FROM CO2_emitted", con=engine)
        df_CO2ppm = pd.read_sql(f"SELECT time, interpolated FROM CO2_ppm", con=engine)

        #3. clean, merge, truncate (Solar Data goes back way farther than all the others)
        dframes = [df_LA, df_Manila, df_CO2emitted, df_CO2ppm, df_solar, df_ENSO]
        make_datetime(dframes)
        merged = merge_dframes(dframes)
        merged.rename(columns={'temperature_x':'temp_LA',
                       'temperature_y':'temp_Manila',
                       'World':'CO2_emissions',
                       'interpolated':'CO2_ppm'},
                       inplace=True)
        truncated = merged[merged['time'] >= pd.to_datetime('1950-01-01')].sort_values(by='time')
        truncated.set_index('time', inplace=True)

        #4. heat maps for temperature data
        pvt_LA = df_LA.pivot_table(index=pd.to_datetime(df_LA['time'], unit='s').dt.month.rename('month'),
                   columns=pd.to_datetime(df_LA['time'], unit='s').dt.year.rename('year'),
                   values='temperature',
                   aggfunc=np.mean)
        pvt_Manila = df_Manila.pivot_table(index=pd.to_datetime(df_Manila['time'], unit='s').dt.month.rename('month'),
                   columns=pd.to_datetime(df_Manila['time'], unit='s').dt.year.rename('year'),
                   values='temperature',
                   aggfunc=np.mean)
        plt.figure(figsize=(16,6))
        ax = sns.heatmap(pvt_LA.drop(columns=[2022.0]))
        plt.figure(figsize=(16,6))
        ax = sns.heatmap(pvt_Manila.drop(columns=[1949.0,2022.0]).fillna(method='ffill'))

        #5. stacked subplots
        fig, axes = plt.subplots(6,1, figsize=(16,18), sharex=True)
        axes[0].set_xlim(left=truncated.index[0], right=truncated.index[-1])
        axes[0].plot(truncated.CO2_emissions.dropna(), label='CO2 tons')
        axes[0].legend()
        axes[0].grid()
        axes[1].plot(truncated.CO2_ppm.dropna(), label='CO2 ppm')
        axes[1].legend()
        axes[1].grid()
        axes[2].plot(truncated.SOI.dropna(), label='SOI anomaly')
        axes[2].plot(SMA(truncated, 'SOI', 20, time_unit='daily'), label='smoothed SOI')
        axes[2].legend()
        axes[2].grid()
        axes[3].plot(SMA(truncated, 'temp_LA', 2, time_unit='hourly'), label='LA temp smoothed')
        axes[3].legend()
        axes[3].grid()
        axes[4].plot(SMA(truncated, 'temp_Manila', 2, time_unit='hourly'), label='Manila temp smoothed')
        axes[4].legend()
        axes[4].grid()
        axes[5].plot(truncated.ssn.dropna(), label='ssn')
        axes[5].plot(SMA(truncated, 'ssn', 12, time_unit='daily'), label='ssn smoothed')
        axes[5].legend()
        axes[5].grid()
        plt.show()

        #6. correlation table with heatmap
        SMA(truncated, 'SOI', 20, time_unit='daily', inplace=True)
        SMA(truncated, 'temp_LA', 2, time_unit='hourly', inplace=True)
        SMA(truncated, 'temp_Manila', 2, time_unit='hourly', inplace=True)
        SMA(truncated, 'ssn', 12, time_unit='daily', inplace=True)
        ax = sns.heatmap(truncated.corr())

        print(f'Merged and Truncated Data Frame Head:\n')
        print(truncated.head())

    elif sys.argv[1] == '--scrape_loc': #'--scrape_loc: location' where location is a location name
        '''if two arguments are present, use the second as the inpt to the coordinates() function, and
        scrape a truncated date range (1985-01-01 to 1985-01-10) of weather data at that location...

        A new table will be created in the ScrapeDemo.db containing this scraped data.'''

        print(f'boop... beep... boop... processing location "{sys.argv[2]}" ...')
        weather_data(coordinates(sys.argv[2]), start='1985-01-01', end='1985-01-10')
        print(f'success! check folder for new table: "weather_{sys.argv[2]}" in ScrapeDemo.db')

    elif sys.argv[1] == '--super_huge_long_giant_weather_scrape': #--yeah, that's right... it's long for a reason
        '''again, the second argument is the user's choice of location, but this time the full 72-year date
        range is scraped.

        >>> BE WARNED: THIS WILL LIKELY TAKE AT LEAST 7 HOURS TO COMPLETE !!!!'''

        print('BE WARNED: THIS WILL LIKELY TAKE AT LEAST 7 HOURS TO COMPLETE !!!! ... good luck...')
        weather_data(coordinates(sys.argv[2]))

else:
    print(f'... Importing module {__name__} ...')
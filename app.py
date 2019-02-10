import pandas as pd

import datetime as dt
from datetime import date
from dateutil.relativedelta import relativedelta

from flask import Flask, jsonify, json

# Python SQL toolkit and Object Relational Mapper
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save references to each table
Measurement = Base.classes.measurement
Station = Base.classes.station

session = Session(engine)

# Get the most recent date from the dataset
last_date_in_dataset = engine.execute('SELECT max(date) FROM measurement').fetchall()[0][0]
last_date_in_dataset = dt.datetime.strptime(last_date_in_dataset,'%Y-%m-%d')

first_date_in_dataset = engine.execute('SELECT min(date) FROM measurement').fetchall()[0][0]
first_date_in_dataset = dt.datetime.strptime(first_date_in_dataset,'%Y-%m-%d')

# Subtract a year from this date
start_date = last_date_in_dataset - relativedelta(years=1)

# create a result set that is grouped by date for the last 12 months of data contained in the full dataset
results = session.query(Measurement.date, Measurement.prcp, Measurement.station, Measurement.tobs).all()

# put this result set into a dataframe
results_df = pd.DataFrame(results, columns=['date', 'prcp', 'station', 'tobs'])

# change the date datatype to a date - rename the prcp column
results_df['date'] =  pd.to_datetime(results_df['date'], format='%Y-%m-%d')
results_df = results_df.rename(columns={'prcp': 'precipitation'})

# build a 12 month DF based on the comupted startdate from above
last_year_meas_df = pd.DataFrame(results_df.loc[results_df['date'] >= start_date, :])

# Crate a df with the station data
station_data = session.query(Station.id, Station.elevation, Station.latitude, Station.longitude, Station.station,\
                            Station.name).all()

station_list = pd.DataFrame(station_data, columns=['id', 'elevation', 'latitude', 'longitude', 'station', 'name'])

# get rid of the id column
station_list.drop(['id'], axis=1, inplace=True)
# resort the columns to make them more user friendly
station_list = station_list[ ['station', 'name', 'elevation', 'latitude', 'longitude']]

# change date back to a string
results_df['date'] = results_df['date'].dt.strftime('%Y-%m-%d')


def calc_temps(start_date, end_date):
    """TMIN, TAVG, and TMAX for a list of dates.
    
    Args:
        start_date (string): A date string in the format %Y-%m-%d
        end_date (string): A date string in the format %Y-%m-%d
        
    Returns:
        TMIN, TAVE, and TMAX
    """
    mask = (results_df['date'] >= start_date) & (results_df['date'] <= end_date)
    filtered_df = pd.DataFrame(results_df.loc[mask])

    return [(filtered_df['tobs'].min(), filtered_df['tobs'].max(), filtered_df['tobs'].mean() )]



#################################################
# Flask Setup
#################################################
app = Flask(__name__)

@app.route("/")
def welcome():
    """List all available api routes."""
    return (
        f"The following links can be used to get data dumps<br/>"
        f"<ul>"
            f"<li><a href=""http://127.0.0.1:5000/api/v1.0/precipitation"" target=""_blank"">/api/v1.0/precipitation</a></li>"
            f"&nbsp;&nbsp;&nbsp;&nbsp;Returns a json of all the Hawaii Station precipitation averages<br/>"
            f"<li><a href=""http://127.0.0.1:5000/api/v1.0/stations"" target=""_blank"">/api/v1.0/stations</a></li>"
            f"&nbsp;&nbsp;&nbsp;&nbsp;Returns a json that provides the station id, station name, elevation, lat and long of each stations<br/>"
            f"<li><a href=""http://127.0.0.1:5000/api/v1.0/tobs"" target=""_blank"">/api/v1.0/tobs</a></li>"
            f"&nbsp;&nbsp;&nbsp;&nbsp;Returns a json of all the Hawaii Station observed temps between {start_date.strftime('%Y-%m-%d')} and {last_date_in_dataset.strftime('%Y-%m-%d')}.<br/>"
        f"</ul>"
        f"<br/>"
        f"The following require user to include dates<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;Enter the date in the following format YYYY-MM-DD.  Example: /api/v1.0/2016-01-01<br/>"
        f"&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;Only supports dates beteen {first_date_in_dataset.strftime('%Y-%m-%d')} and {last_date_in_dataset.strftime('%Y-%m-%d')}<br/>"
        f"<ul>"
            f"<li>/api/v1.0/start date</li>"
            f"&nbsp;&nbsp;&nbsp;&nbsp;Returns the min, max and average observed temp between the start date and {last_date_in_dataset.strftime('%Y-%m-%d')}.<br/>"
            f"<li>/api/v1.0/start date/end date</li>"
            f"&nbsp;&nbsp;&nbsp;&nbsp;Returns the min, max and average observed temp between the start date and end date.<br/>"
        f"</ul>"
    )


#   Convert the query results to a Dictionary using `date` as the key and `prcp` as the value.
#   Return the JSON representation of your dictionary.
@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return a list of precipitation totals by date"""
    # totals does not seem to be the correct term for precipitation accumulation across a region.
    # It seems mean is the best way to determine a total precipitation.  However this does assume that each
    # stations accumulation will have equal impact across the entire region
    meas_date_gb = results_df.groupby(['date'])
    date_prcp = meas_date_gb['precipitation'].mean()
    date_prcp_d= dict(date_prcp)

    # I attempted to jus use
    # jsonify(date_prcp_d) and it would not work.  I found putting date_prcp_d into a single dictionary worked around this issue
    message = {
        'precipitation_data': date_prcp_d
    }
    return jsonify(message) 


#   Return a JSON list of stations from the dataset.
@app.route("/api/v1.0/stations")
def stations():
    station_list_t = [tuple(x) for x in station_list.values]
    return jsonify(station_list_t)


#   query for the dates and temperature observations from a year from the last data point.
#   Return a JSON list of Temperature Observations (tobs) for the previous year.
@app.route("/api/v1.0/tobs")
def tobs():
    tobs_list = last_year_meas_df[['date', 'station', 'tobs']].copy()

    # Convert the date to a string for nicer display in json
    tobs_list['date'] =  tobs_list['date'].dt.strftime('%Y-%m-%d')

    tobs_list_t = [tuple(x) for x in tobs_list.values]
    return jsonify(tobs_list_t)

#   * Return a JSON list of the minimum temperature, the average temperature, and the max temperature for a given start or start-end range.
#   * When given the start only, calculate `TMIN`, `TAVG`, and `TMAX` for all dates greater than and equal to the start date.
@app.route("/api/v1.0/<start>")
def summary_only_start(start):
    
    summary_tobs = calc_temps(start,last_date_in_dataset.strftime('%Y-%m-%d'))
    return jsonify(summary_tobs)

#   * When given the start and the end date, calculate the `TMIN`, `TAVG`, and `TMAX` for dates between the start and end date inclusive.
@app.route("/api/v1.0/<start>/<end>")
def summary_start_end(start,end):

    summary_tobs = calc_temps(start,end)
    return jsonify(summary_tobs)



if __name__ == '__main__':
    app.run(debug=True)

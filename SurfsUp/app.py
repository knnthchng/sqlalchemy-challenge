# Import dependencies
import datetime as dt
from flask import Flask, jsonify
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func

############################################
### Database Setup 
############################################

# create engine to hawaii.sqlite
engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# Reflect an existing database into a new model
Base = automap_base()

# Reflect the tables
Base.prepare(autoload_with = engine)

# Save references to each table
Measurements = Base.classes.measurement
Station = Base.classes.station

############################################
### Flask Setup 
############################################

app = Flask(__name__)


############################################
### Flask Routes 
############################################

@app.route("/")
def index():
    """List all available API routes."""

    return (
        "Welcome.<br/>" 
        "Below are the available API routes.<br/>" 
        "Date parameters should be in 'MMDDYYYY' format.<br/>"
        "The dataset ranges from January 1, 2010 to August 23, 2017.<br/>" 
        "/api/v1.0/precipitation<br/>"
        "api/v1.0/stations<br/>" 
        "api/v1.0/tobs<br/>" 
        "/api/v1.0/{start date}<br/>" 
        "/api/v1.0/{start date}/{end date}<br/>"
        )

@app.route("/api/v1.0/precipitation")
def preciptitation():
    """Route for handling precipitation data API"""

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Calculate the date one year from the last date in the dataset.
    date_range = year_prior()

    # Retrieve precipitation data for the last year of the dataset
    prcp_data = session.query(Measurements.date, Measurements.prcp). \
        filter(Measurements.date >= date_range).all()

    session.close()

    # Store records into a list variable
    prcp_list = []
    for date, prcp in prcp_data:
        prcp_list.append(
            {date: prcp}
        )

    return jsonify(prcp_list)

@app.route("/api/v1.0/stations")
def stations():
    """Route for handling weather station data API"""

    # Create session from Python to DB
    session = Session(engine)

    # Retrieve all station data
    all_stations = session. \
        query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
    
    session.close()

    # Store station data into a list variable
    station_list = []
    for station, name, latitude, longitude, elevation in all_stations:
        station_dict = {
            'station': station,
            'name': name,
            'latitude': latitude,
            'longitude': longitude,
            'elevation': elevation
        }
        station_list.append(station_dict)

    return jsonify(station_list)

@app.route("/api/v1.0/tobs")
def tobs():
    """Route for handling temperature data API"""

    # Create session
    session = Session(engine)

    # Query for most active station
    busiest = session. \
        query(Measurements.station, func.count(Measurements.station)). \
        group_by(Measurements.station). \
        order_by(func.count(Measurements.station).desc()).first()[0]
    
    # Calculate the date one year from the last date in the dataset.
    date_range = year_prior()

    # Query temperature data from this station for the previous year
    temp_data = session. \
        query(Measurements.date, Measurements.tobs). \
        filter(Measurements.station == busiest). \
        filter(Measurements.date >= date_range).all()
    
    session.close()

    temp_list = []
    for date, temp in temp_data:
        temp_list.append(
            {date: temp}
        )

    final_dict = {busiest: temp_list}

    return jsonify(final_dict)


@app.route("/api/v1.0/<start>")
def single_date(start):
    """Route for returning min, max, and avg for dates greater or equal to start"""

    # Convert start date into datetime object
    if date_valid(start):
        start_date = dt.datetime.strptime(start, "%m%d%Y").strftime('%Y-%m-%d')

        # Create our session (link) from Python to the DB
        session = Session(engine)

        # Query aggregate temperatures
        agg_temps = session.query(
                Measurements.date,
                func.min(Measurements.tobs),
                func.avg(Measurements.tobs),
                func.max(Measurements.tobs)
            ). \
            filter(Measurements.date >= start_date). \
            group_by(Measurements.date).all()

        session.close()

        # Store returned data into a list of dictionaries
        agg_list = []
        for date, tmin, tavg, tmax in agg_temps:
            agg_dict = {
                'date': date,
                'TMIN': tmin,
                'TAVG': tavg,
                'TMAX': tmax
            }
            agg_list.append(agg_dict)

        return jsonify(agg_list)
    else:
        # Invalid start date provided
        return "Error: Start date entered is not valid or is outside of the dataset.<br/>" \
               f"Start Date: {start}"

@app.route("/api/v1.0/<start>/<end>")
def date_range(start, end):
    """Route for handling min, max, and avg for dates between start and end"""

    # Convert start and end dates into datetime objects
    if date_valid(start):
        if date_valid(end):
            start_date = dt.datetime.strptime(start, "%m%d%Y").strftime('%Y-%m-%d')
            end_date = dt.datetime.strptime(end, "%m%d%Y").strftime('%Y-%m-%d')

            # Create our session (link) from Python to the DB
            session = Session(engine)

            # Query aggregate temperatures
            agg_temps = session.query(
                    Measurements.date,
                    func.min(Measurements.tobs),
                    func.avg(Measurements.tobs),
                    func.max(Measurements.tobs)
                ). \
                filter(Measurements.date >= start_date). \
                filter(Measurements.date <= end_date). \
                group_by(Measurements.date).all()

            session.close()

            # If results are empty, date range is invalid.
            if len(agg_temps) == 0:
                return "Error: Start date is greater than end date.<br/>" \
                       f"Start date: {start_date}<br/>" \
                       f"End date: {end_date}"

            # Store returned data into a list of dictionaries
            agg_list = []
            for date, tmin, tavg, tmax in agg_temps:
                agg_dict = {
                    'date': date,
                    'TMIN': tmin,
                    'TAVG': tavg,
                    'TMAX': tmax
                }
                agg_list.append(agg_dict)

            return jsonify(agg_list)
        else:
            # Invalid end date provided
            return "Error: The entered end date is invalid or beyond the dataset's range.<br/>" \
                   f"End Date: {end}"
    else:
        # Invalid start date provided
        return "Error: The entered start date is invalid or beyond the dataset's range.<br/>" \
               f"Start Date: {start}"


#################################################
### Other Functions
#################################################

def year_prior():
    """Returns the most recent date in the dataset minus 12 months"""

    # Create our session (link) from Python to the DB
    session = Session(engine)

    # Get the most recent date in the dataset
    recent_date = session. \
        query(Measurements.date).order_by(Measurements.date.desc()).first()[0]

    # Calculate the date one year from the last date in data set.
    date_range = (dt.datetime.strptime(recent_date, "%Y-%m-%d") - dt.timedelta(days=365)).strftime('%Y-%m-%d')

    session.close()

    return date_range


def date_valid(date):
    """Verifies if input date is correct and available in dataset"""

    session = Session(engine)

    try:
        # Try to convert date into datetime object and back to string
        dt.datetime.strptime(date, "%m%d%Y").strftime('%Y-%m-%d')

    except ValueError:
        # Date provided is not valid
        session.close()
        return False

    else:
        # Query Measurement table for date provided to validate
        val_date = dt.datetime.strptime(date, "%m%d%Y").strftime('%Y-%m-%d')
        result = session.query(Measurements.date). \
            filter(Measurements.date == val_date).all()

        # Date provided is outside the dataset
        if len(result) == 0:
            session.close()
            return False

        session.close()
        return True


if __name__ == "__main__":
    app.run(debug=True)
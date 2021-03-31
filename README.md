# Exploratory Analysis on Airport using PySpark
This project focuses on running Apache Spark on Google Colab to perform Exploratory Analysis on Flights within US.
## Data

## Installing  and Initializing Spark

## Reading CSV File using Spark

## Insights into Data

**The Number of Records in the dataset are**

`df.count()`

![plot](./query_images/count_rows.JPG)

**The Schema for the dataset is as follows**

`df.printSchema()`

![plot](./query_images/schema.JPG)

**Various Statistical Analysis Factors for the Features can be seen below**

`df.describe().show()`

![plot](./query_images/describe.JPG)

## Spark Transformations and Actions

**Viewing First 5 Rows of Data**

`df.show(5)`

![plot](./query_images/show.JPG)

**Selecting Subset of Features**

`df.select("Origin_airport","Destination_airport","Passengers","Seats").show(15)`

![plot](./query_images/subset.JPG)

**Aggregating the Data**

```python
airportAgg_DF = df.groupBy("Origin_airport").agg(F.sum("Passengers"))
airportAgg_DF.show(10)
```
 
![plot](./query_images/aggregate.JPG)

## Spark SQL

**Find the Airport with Highest Number of Flight Departures**

`originAirports = sqlContext.sql("""select Origin_Airport, sum(Flights) as Flights`
                                    `from df group by Origin_Airport order by sum(Flights) DESC limit 10""")`
`originAirports.show()`
![plot](./query_images/highest_flight_departures.JPG)

**Find the Airport with Highest Number of Passenger Arrivals**

`df.show(5)`
![plot](./query_images/highest_passenger_arrival.JPG)

**Find the Airport with Most Flight Traffic**

`df.show(5)`
![plot](./query_images/airport_most_flights.JPG)

**Find the Airport with Most Passenger Footfall**

`df.show(5)`
![plot](./query_images/airport_most_passengers.JPG)

**Find the Occupancy Rate for Most Popular Routes**

`df.show(5)`
![plot](./query_images/occupancy_rates.JPG)

**Find the Number of Flights for Long Distance Journeys**

`df.show(5)`
![plot](./query_images/FvD1.JPG)

**Find the Average Distances for the Routes with Most Flights**

`df.show(5)`
![plot](./query_images/FvD2.JPG)

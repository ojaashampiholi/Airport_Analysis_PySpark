# Exploratory Analysis on Airport using PySpark
This project focuses on running Apache Spark on Google Colab to perform Exploratory Analysis on Flights within US.
## Data

## Installing  and Initializing Spark

## Reading CSV File using Spark

## Insights into Data

**The Number of Records in the dataset are**

```python
df.count()
```
![plot](./query_images/count_rows.JPG)

**The Schema for the dataset is as follows**

```python
df.printSchema()
```

![plot](./query_images/schema.JPG)

**Various Statistical Analysis Factors for the Features can be seen below**

```python
df.describe().show()
```

![plot](./query_images/describe.JPG)

## Spark Transformations and Actions

**Viewing First 5 Rows of Data**

```python
df.show(5)
```
![plot](./query_images/show.JPG)

**Selecting Subset of Features**

```python
df.select("Origin_airport","Destination_airport","Passengers","Seats").show(15)
```

![plot](./query_images/subset.JPG)

**Aggregating the Data**

```python
airportAgg_DF = df.groupBy("Origin_airport").agg(F.sum("Passengers"))
airportAgg_DF.show(10)
```
 
![plot](./query_images/aggregate.JPG)

## Spark SQL

**Find the Airport with Highest Number of Flight Departures**
```python
originAirports = sqlContext.sql("""select Origin_Airport, sum(Flights) as Flights 
                                    from df group by Origin_Airport order by sum(Flights) DESC limit 10""")
originAirports.show()
```

![plot](./query_images/highest_flight_departures.JPG)

**Find the Airport with Highest Number of Passenger Arrivals**

```python
destinationAirports = sqlContext.sql("""select Destination_airport, sum(Passengers) as Passengers 
                                    from df group by Destination_airport order by sum(Passengers) DESC limit 10""")
destinationAirports.show()
```

![plot](./query_images/highest_passenger_arrival.JPG)

**Find the Airport with Most Flight Traffic**

```python
MostFlightsByAirports = sqlContext.sql("""with destination as (select Destination_airport as Airport, sum(Flights) as Out_Flights 
                                    from df group by Destination_airport),
                                    origin as (select Origin_airport as Airport, sum(Flights) as In_Flights 
                                    from df group by Origin_airport)
                                    select origin.Airport, (destination.Out_Flights+origin.In_Flights) as Total_Flights
                                    from origin, destination 
                                    where origin.Airport = destination.Airport
                                    order by (origin.In_Flights + destination.Out_Flights) DESC
                                    limit 15""")
MostFlightsByAirports.show()
```

![plot](./query_images/airport_most_flights.JPG)

**Find the Airport with Most Passenger Footfall**

```python
MostPassengersByAirports = sqlContext.sql("""with destination as (select Destination_airport as Airport, sum(Passengers*Flights) as Out_Passengers 
                                    from df group by Destination_airport),
                                    origin as (select Origin_airport as Airport, sum(Passengers) as In_Passengers
                                    from df group by Origin_airport)
                                    select origin.Airport, (destination.Out_Passengers+origin.In_Passengers) as Total_Passengers
                                    from origin, destination 
                                    where origin.Airport = destination.Airport
                                    order by (origin.In_Passengers + destination.Out_Passengers) DESC
                                    limit 15""")
MostPassengersByAirports.show()
```

![plot](./query_images/airport_most_passengers.JPG)

**Find the Occupancy Rate for Most Popular Routes**

```python
distanceQuery = sqlContext.sql("""with table1 as 
                                    (select least(Origin_airport, Destination_airport) as Airport1, 
                                    greatest(Destination_airport, Origin_airport) as Airport2, 
                                    sum(Flights) as Flights,
                                    sum(Passengers) as Passengers,
                                    sum(Seats) as Seats
                                    from df
                                    group by least(Origin_airport, Destination_airport), greatest(Destination_airport, Origin_airport)
                                    order by 1,2)
                                    select t.*, (Passengers*100/Seats) as Occupancy_Rate
                                    from table1 t
                                    order by Flights DESC, Seats DESC, Passengers DESC, Occupancy_Rate DESC
                                    limit 15;""")
distanceQuery = distanceQuery.filter((col("Occupancy_Rate").isNotNull()) & (col("Occupancy_Rate")<=100.0))
distanceQuery.show(15)
```

![plot](./query_images/occupancy_rates.JPG)

**Find the Number of Flights for Long Distance Journeys**

```python
distanceQuery = sqlContext.sql("""with table1 as 
                                    (select least(Origin_airport, Destination_airport) as Airport1, 
                                    greatest(Destination_airport, Origin_airport) as Airport2, 
                                    mean(Distance) as Distance,
                                    sum(Flights) as Flights
                                    from df
                                    group by least(Origin_airport, Destination_airport), greatest(Destination_airport, Origin_airport)
                                    order by 1,2)
                                    select t.*
                                    from table1 t
                                    where Flights>0
                                    order by Distance DESC
                                    limit 15;""")
distanceQuery.show(15)
```

![plot](./query_images/FvD1.JPG)

**Find the Average Distances for the Routes with Most Flights**

```python
distanceQuery = sqlContext.sql("""with table1 as 
                                    (select least(Origin_airport, Destination_airport) as Airport1, 
                                    greatest(Destination_airport, Origin_airport) as Airport2, 
                                    mean(Distance) as Distance,
                                    sum(Flights) as Flights
                                    from df
                                    group by least(Origin_airport, Destination_airport), greatest(Destination_airport, Origin_airport)
                                    order by 1,2)
                                    select t.*
                                    from table1 t
                                    where Flights>0
                                    order by Flights DESC
                                    limit 15;""")
distanceQuery.show(15)
```

![plot](./query_images/FvD2.JPG)

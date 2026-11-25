# Bus Route Optimization Using Apache Spark GraphX

## Project Overview

This project focuses on analyzing bus route connectivity and identifying optimized routes using Apache Spark and GraphX.

The system represents bus stops as vertices and connections between consecutive stops as directed edges. It uses graph-processing algorithms to identify highly connected bus stops and calculate shortest-distance paths.

## Technologies Used

- Apache Spark 3.5.9
- Scala 2.13.8
- Apache Spark GraphX
- Spark SQL
- Power BI
- GTFS (General Transit Feed Specification)

## Dataset

The project uses the Cascais Próxima GTFS dataset.

Source: https://mobilitydatabase.org/feeds/gtfs/mdb-1272

The dataset contains bus stops, routes, trips and stop-time information.

## Project Workflow

1. Load GTFS files into Spark.
2. Clean and preprocess the transportation data.
3. Generate numeric IDs for bus stops.
4. Identify consecutive stops within each trip.
5. Generate directed edges between bus stops.
6. Calculate geographical distances using the Haversine formula.
7. Construct a GraphX graph.
8. Calculate degree centrality to identify highly connected stops.
9. Apply Pregel-based iterative relaxation for shortest-path calculation.
10. Visualize results using CLI output and Power BI.

## Algorithms Used

- Haversine Formula: Calculates straight-line geographical distance between bus stops.
- Degree Centrality: Identifies highly connected bus stops.
- Pregel: Supports iterative graph computation.
- Bellman-Ford-style Relaxation: Calculates shortest-distance paths.

## Results

- Total bus stops: 1,083
- Total directed edges: 7,677
- Highest observed degree: 112
- Optimized route distance: Approximately 5.43 km
- Source: Hospital de Cascais
- Destination: Cascais Estação - Largo

## Project Execution

1. Install the required Java and Apache Spark versions.
2. Download and extract the project.
3. Open Windows CMD in the project directory.
4. Start Spark Shell:

```cmd
spark-shell --master "local[2]"
```

5. Enter `:paste` in Spark Shell.
6. Paste and execute the commands from `codes_bus_route.scala` in the documented order.

The code is designed for Spark Shell execution.

## Dashboard

The Power BI dashboard presents:
- Top connected bus stops.
- Total trips across bus routes.
- Optimized route and distance.
- Average connection distance.
- Total stops, routes and connections.

## Limitations

- Distances are calculated using geographical coordinates, not actual road distances.
- Real-time traffic and passenger demand are not included.
- Degree centrality measures network connectivity, not actual passenger congestion.
- The project uses Spark local mode.

## Author

B.Tech Computer Science and Engineering Project


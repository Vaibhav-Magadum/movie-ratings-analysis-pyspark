## Movie Rating Analysis PySpark
A simple PySpark project that analyzes movie ratings to uncover fun and insightful trends like top users, lowest-rated films, rating distributions, genre preferences, and more — all using big data techniques and visualized with Streamlit.


## Features
 - Find the lowest-rated movie of all time
   
 - Top 10 most active users
   
 - Best movies with minimum votes
 
 - Most controversial movies (based on rating variance)
 
 - Average rating by genre

## Tech Stack
 - Python

- PySpark for distributed data processing

 - Streamlit for interactive UI

- Pandas for post-processing & charts

## Quick Start
1. Clone the Repo
   ```
   git clone https://github.com/Vaibhav-Magadum/movie-ratings-analysis-pyspark.git
   cd spark-movie-analysis
   ```

2. Install Requirements
   ```
   pip install -r requirements.txt

   ```
3. Add your data
   
    Place ratings.csv and movies.csv (https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset) into a data/ folder.

4. Run the App
   ```
   streamlit run app.py
   ```

## File structure
```
spark-movie-insights/
│
├── app.py                 # Streamlit app entrypoint
├── src/
│   └── spark_utils.py     # Spark logic for all analysis tasks
├── data/                  # Place your MovieLens CSV files here
├── requirements.txt       # Python dependencies
└── README.md              # You’re reading this!
```

## License
MIT License. Use it, remix it, build on it. Credit is appreciated!

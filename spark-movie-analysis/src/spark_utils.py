from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    desc, col, from_unixtime, to_date, hour, date_format,
    stddev, count, avg, explode, split
)

def get_spark():
    return SparkSession.builder.appName("MovieRatings").getOrCreate()

def load_data(spark, ratings_path, movies_path):
    ratings_df = spark.read.csv(ratings_path, header=True, inferSchema=True)
    movies_df = spark.read.csv(movies_path, header=True, inferSchema=True)
    return ratings_df, movies_df

def task_a_lowest_avg_movie(ratings_df, movies_df):
    ratings_rdd = ratings_df.rdd
    avg_ratings = (ratings_rdd
                   .map(lambda row: (row.movieId, (row.rating, 1)))
                   .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
                   .mapValues(lambda x: x[0] / x[1]))
    lowest = avg_ratings.takeOrdered(1, key=lambda x: x[1])[0]
    title = movies_df.filter(movies_df.movieId == lowest[0]).select("title").first()[0]
    return f"📉 Lowest rated movie: {title} ({lowest[1]:.2f})"

def task_b_top_users(ratings_df):
    df = (ratings_df.groupBy("userId").count()
          .orderBy(desc("count"))
          .limit(10)
          .toPandas())
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

def task_c_rating_distribution(ratings_df):
    ratings_with_date = ratings_df.withColumn("date", to_date(from_unixtime(col("timestamp"))))
    grouped = ratings_with_date.groupBy("date").count().orderBy("date")
    return grouped.toPandas()

def task_d_highest_rated_with_min_votes(ratings_df, movies_df, min_votes=50):
    avg_df = (ratings_df.groupBy("movieId")
              .agg({"rating": "avg", "*": "count"})
              .withColumnRenamed("avg(rating)", "avg_rating")
              .withColumnRenamed("count(1)", "num_ratings"))
    filtered = avg_df.filter(col("num_ratings") >= min_votes)
    top = filtered.orderBy(desc("avg_rating")).limit(10)
    joined = top.join(movies_df, on="movieId").select("title", "avg_rating", "num_ratings")
    df = joined.toPandas()
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

def task_e_active_hours(ratings_df):
    df = (
        ratings_df.withColumn("hour", hour(from_unixtime("timestamp")))
        .withColumn("time", date_format(from_unixtime("timestamp"), "hh a"))
        .groupBy("hour", "time")
        .count()
        .orderBy("hour")
        .select("time", "count")
        .toPandas()
    )
    return df

def task_f_controversial_movies(ratings_df, movies_df, min_ratings=50):
    stats_df = (
        ratings_df.groupBy("movieId")
        .agg(count("*").alias("num_ratings"),
             avg("rating").alias("avg_rating"),
             stddev("rating").alias("stddev_rating"))
        .filter(col("num_ratings") >= min_ratings)
        .orderBy(desc("stddev_rating"))
    )
    joined = stats_df.join(movies_df, on="movieId").select("title", "avg_rating", "stddev_rating", "num_ratings")
    df = joined.limit(10).toPandas()
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

def task_h_avg_rating_by_genre(ratings_df, movies_df):
    movies_split = movies_df.withColumn("genre", explode(split("genres", "\\|")))
    joined = ratings_df.join(movies_split, on="movieId")
    df = (
        joined.groupBy("genre")
        .agg(avg("rating").alias("avg_rating"))
        .orderBy("avg_rating", ascending=False)
        .toPandas()
    )
    df.index = df.index + 1
    df.index.name = "Rank"
    return df

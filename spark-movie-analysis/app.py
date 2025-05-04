import streamlit as st
from src.spark_utils import (
    get_spark, load_data,
    task_a_lowest_avg_movie, task_b_top_users,
    task_c_rating_distribution, task_d_highest_rated_with_min_votes,
 task_f_controversial_movies,
     task_h_avg_rating_by_genre
)

# --- Page Config ---
st.set_page_config(page_title="🎬 Movie Ratings Analysis", layout="wide")
st.title("🎬 Movie Ratings Analysis with PySpark")
st.caption("⚡ Built on PySpark | Designed to keep your brain cool while Spark burns CPU cores.🔥")

# --- Init Spark ---
spark = get_spark()

# --- Load Data with Spinner ---
with st.spinner("⚙️ Booting up Spark... making popcorn 🍿"):
    ratings_df, movies_df = load_data(spark, "data/ratings.csv", "data/movies.csv")

# --- Sidebar: Choose Task ---
st.sidebar.header("🧠 Pick a Query Task")
task = st.sidebar.selectbox(
    "Select analysis type:",
    options=[
        "(a) Lowest Avg Rating",
        "(b) Top Rating Users",
        "(c) Ratings Over Time",
        "(d) Top Rated Movies (min ratings)",
        "(e) Most Controversial Movies",
        "(f) Average Rating by Genre"
    ]
)

# --- Main View ---
st.markdown("### 🔍 Result Panel")

if task == "(a) Lowest Avg Rating":
    st.subheader("📉 Movie with the Lowest Average Rating")
    with st.spinner("🔍 Searching for the cinematic flops..."):
        result = task_a_lowest_avg_movie(ratings_df, movies_df)
        st.success(result)

elif task == "(b) Top Rating Users":
    st.subheader("👑 Users Who Rated the Most Movies")
    with st.spinner("🧮 Counting the click-happy folks..."):
        top_users = task_b_top_users(ratings_df)
        st.dataframe(top_users, use_container_width=True)

elif task == "(c) Ratings Over Time":
    st.subheader("📈 Ratings Trend Over Time")
    with st.spinner("📅 Scrubbing timestamps..."):
        rating_dist = task_c_rating_distribution(ratings_df)
        rating_dist["date"] = rating_dist["date"].astype("datetime64[ns]")
        st.line_chart(rating_dist.set_index("date"))

elif task == "(d) Top Rated Movies (min ratings)":
    st.subheader("🏆 Top Rated Movies (Threshold: 50+ ratings)")
    with st.spinner("✨ Surfacing only the crowd favorites..."):
        top_movies = task_d_highest_rated_with_min_votes(ratings_df, movies_df)
        st.dataframe(top_movies, use_container_width=True)


elif task == "(e) Most Controversial Movies":
    st.subheader("🎭 Movies with the Most Rating Disagreements")
    with st.spinner("🧪 Calculating standard deviation of opinions..."):
        controversial = task_f_controversial_movies(ratings_df, movies_df)
        st.dataframe(controversial, use_container_width=True)


elif task == "(f) Average Rating by Genre":
    st.subheader("🎬 Average Rating by Genre")
    with st.spinner("🍿 Crunching genres and rating averages..."):
        genre_ratings = task_h_avg_rating_by_genre(ratings_df, movies_df)
        genre_ratings["avg_rating"] = genre_ratings["avg_rating"].astype(float)
        st.bar_chart(genre_ratings.set_index("genre"))

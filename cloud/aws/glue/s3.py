def write_parquet(df, path, mode="append"):
    df.write.mode(mode).parquet(path)

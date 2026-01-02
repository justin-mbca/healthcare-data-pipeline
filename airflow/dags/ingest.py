def ingest_csv(file_path):
    """
    Simple CSV ingestion function for Airflow DAGs.
    Args:
        file_path (str): Path to the CSV file.
    Returns:
        list: List of rows (as dicts) from the CSV file.
    """
    import csv
    data = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            data.append(row)
    print(f"Ingested {len(data)} rows from {file_path}")
    return data

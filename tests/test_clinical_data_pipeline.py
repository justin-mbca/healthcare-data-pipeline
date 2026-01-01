import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../airflow/dags')))
import clinical_data_pipeline

def test_file_loaded():
    print("Test file loaded by pytest.")

def test_extract_synthetic_ehr_data(capsys):
    clinical_data_pipeline.extract_synthetic_ehr_data()
    captured = capsys.readouterr()
    assert "Extracting synthetic EHR data..." in captured.out

def test_anonymize_data(capsys):
    clinical_data_pipeline.anonymize_data()
    captured = capsys.readouterr()
    assert "Anonymizing patient data..." in captured.out

def test_perform_data_quality_checks(capsys):
    clinical_data_pipeline.perform_data_quality_checks()
    captured = capsys.readouterr()
    assert "Performing data quality checks..." in captured.out

def test_aggregate_data(capsys):
    clinical_data_pipeline.aggregate_data()
    captured = capsys.readouterr()
    assert "Aggregating processed data..." in captured.out

def test_generate_natural_language_report(capsys):
    clinical_data_pipeline.generate_natural_language_report()
    captured = capsys.readouterr()
    assert "Generating natural language reports..." in captured.out

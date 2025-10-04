#!/usr/bin/env python3
"""
Example usage of DataQualityValidator with PDF generation

This script demonstrates how to use the DataQualityValidator
to generate both text and PDF reports.
"""

from DataQualityValidator import DataQualityValidator


def main():
    # Example file paths (replace with your actual data files)
    SOURCE1_PATH = "source1_data.xlsx"
    SOURCE2_PATH = "source2_data.xlsx"
    
    # Historical data configuration
    HISTORICAL_DATA = [
        {'path': 'report_2023.xlsx', 'year': 2023, 'source': 'A'},
        {'path': 'report_2022.xlsx', 'year': 2022, 'source': 'B'},
        {'path': 'report_2021.xlsx', 'year': 2021, 'source': 'A'},
    ]
    
    # Key columns for matching records between sources (optional)
    KEY_COLUMNS = ['employee_id']  # or None
    
    # Key metrics to validate against historical patterns (optional)
    KEY_METRICS = ['total_employees', 'total_enrollment']  # or None
    
    print("Initializing Data Quality Validator...")
    
    try:
        # Initialize the validator
        validator = DataQualityValidator(
            source1_path=SOURCE1_PATH,
            source2_path=SOURCE2_PATH,
            historical_data_config=HISTORICAL_DATA
        )
        
        print("Running full validation with PDF generation...")
        
        # Run full validation and generate both text and PDF reports
        report = validator.run_full_validation(
            key_columns=KEY_COLUMNS,
            key_metrics=KEY_METRICS,
            report_path='data_confidence_report.txt',
            generate_pdf=True  # This will create both .txt and .pdf reports
        )
        
        print("\\nValidation complete!")
        print("Reports generated:")
        print("  - Text report: data_confidence_report.txt")
        print("  - PDF report: data_confidence_report.pdf")
        
        # Display summary information
        if 'overall_data_confidence' in report['confidence_scores']:
            confidence = report['confidence_scores']['overall_data_confidence']
            print(f"\\nOverall Data Confidence: {confidence:.1f}%")
            
            if confidence >= 90:
                print("✓ VERY HIGH confidence - Data is ready for use")
            elif confidence >= 75:
                print("✓ HIGH confidence - Data is generally reliable")
            elif confidence >= 60:
                print("⚠ MODERATE confidence - Review concerns before use")
            else:
                print("🚩 LOW confidence - Significant review required")
        
    except FileNotFoundError as e:
        print(f"Error: Could not find data file: {e}")
        print("Please ensure your data files exist and update the file "
              "paths in this script.")
    except ImportError as e:
        print(f"Error: Missing required library: {e}")
        print("Please install required dependencies with: "
              "pip install -r requirements.txt")
    except (RuntimeError, ValueError, KeyError) as e:
        print(f"Unexpected error: {e}")


if __name__ == "__main__":
    main()

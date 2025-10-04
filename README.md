# Data Quality Validator

A comprehensive tool for validating data quality and assessing reliability for college workforce and student body data. This tool focuses on data confidence scoring rather than source comparison.

## Features

- **Data Completeness Assessment**: Analyzes missing data patterns and completeness scores
- **Internal Consistency Checks**: Identifies duplicates, outliers, and logical inconsistencies
- **Cross-Source Agreement Analysis**: Compares agreement between multiple data sources
- **Historical Plausibility Assessment**: Validates current data against historical patterns
- **Confidence Scoring**: Provides weighted confidence scores for data reliability
- **Comprehensive Reporting**: Generates both text and PDF reports with detailed findings
- **PDF Report Generation**: Beautiful, professional PDF reports with tables and visualizations

## Installation

### Option 1: Install dependencies manually
```bash
pip install pandas numpy reportlab openpyxl
```

### Option 2: Install from requirements file
```bash
pip install -r requirements.txt
```

## Quick Start

```python
from DataQualityValidator import DataQualityValidator

# Initialize the validator
validator = DataQualityValidator(
    source1_path="source1_data.xlsx",
    source2_path="source2_data.xlsx",
    historical_data_config=[
        {'path': 'report_2023.xlsx', 'year': 2023, 'source': 'A'},
        {'path': 'report_2022.xlsx', 'year': 2022, 'source': 'B'},
    ]
)

# Run full validation and generate reports
report = validator.run_full_validation(
    key_columns=['employee_id'],  # Optional: columns for matching records
    key_metrics=['total_employees', 'total_enrollment'],  # Optional: key metrics to validate
    report_path='data_quality_report.txt',
    generate_pdf=True  # This will create both .txt and .pdf reports
)
```

## Report Outputs

The validator generates two types of reports:

1. **Text Report** (`data_quality_report.txt`): Detailed text-based report with all findings
2. **PDF Report** (`data_quality_report.pdf`): Professional PDF with formatted tables and visual elements

Both reports include:
- Overall confidence scores and levels
- Component breakdowns (completeness, consistency, agreement, plausibility)
- Detailed findings and red flags
- Specific recommendations based on confidence levels
- Historical data coverage summary

## Confidence Levels

- **VERY HIGH (90%+)**: Data is highly reliable across all dimensions
- **HIGH (75-89%)**: Generally reliable with minor concerns
- **MODERATE (60-74%)**: Some reliability concerns requiring investigation
- **LOW (40-59%)**: Significant reliability issues, use with caution
- **VERY LOW (<40%)**: Questionable reliability, thorough review required

## Requirements

- Python 3.7+
- pandas >= 1.3.0
- numpy >= 1.21.0
- reportlab >= 3.6.0 (for PDF generation)
- openpyxl >= 3.0.7 (for Excel file support)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
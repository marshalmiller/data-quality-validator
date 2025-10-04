# Data Quality Validator

A comprehensive tool for validating data quality and assessing reliability for college workforce and student body data. This tool focuses on data confidence scoring rather than source comparison.

## Features

- **Data Completeness Assessment**: Analyzes missing data patterns and completeness scores
- **Internal Consistency Checks**: Identifies duplicates, outliers, and logical inconsistencies
- **Cross-Source Agreement Analysis**: Compares agreement between multiple data sources
- **Historical Plausibility Assessment**: Validates current data against historical patterns
- **Confidence Scoring**: Provides weighted confidence scores for data reliability
- **Enhanced PDF Reports**: Professional, presentation-ready PDF reports with:
  - Executive summary with color-coded confidence levels
  - Interactive confidence gauge visualization
  - Detailed component analysis tables
  - Visual issue summaries with impact levels
  - Professional styling and formatting
  - Action-oriented recommendations
- **Comprehensive Text Reports**: Detailed text-based reports for technical analysis

## Installation

### Option 1: Install dependencies manually
```bash
pip install pandas numpy reportlab matplotlib openpyxl
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

# Run full validation and generate enhanced reports
report = validator.run_full_validation(
    key_columns=['employee_id'],  # Optional: columns for matching records
    key_metrics=['total_employees', 'total_enrollment'],  # Optional: key metrics to validate
    report_path='data_quality_report.txt',
    generate_pdf=True  # Creates beautiful presentation-ready PDF
)
```

## Enhanced PDF Reports

The new PDF reports are designed for presentations and executive summaries:

### Executive Summary Page
- Color-coded confidence score overview
- Executive summary box with status indicators
- Report metadata in professional tables

### Visual Analysis
- **Confidence Gauge**: Interactive matplotlib-generated gauge showing overall score
- **Component Tables**: Detailed breakdown with status indicators (✓ Good, ⚠ Review, 🚩 Critical)
- **Issue Summary**: Color-coded tables showing critical issues vs. concerns

### Professional Styling
- Corporate color scheme with blues and branded elements
- Alternating row colors in tables for readability
- Proper typography and spacing
- Page breaks for logical sections

### Action-Oriented Recommendations
- Priority-based action items
- Color-coded based on confidence level
- Specific next steps for each confidence tier

## Report Outputs

The validator generates two complementary reports:

1. **Enhanced PDF Report** (`data_quality_report.pdf`): 
   - Presentation-ready format
   - Executive summary with confidence gauge
   - Visual tables and color coding
   - Professional styling suitable for stakeholders

2. **Detailed Text Report** (`data_quality_report.txt`): 
   - Complete technical details
   - All findings and analysis
   - Machine-readable format

Both reports include:
- Overall confidence scores and levels
- Component breakdowns (completeness, consistency, agreement, plausibility)
- Detailed findings and red flags
- Specific recommendations based on confidence levels
- Historical data coverage summary

## Confidence Levels

- **VERY HIGH (90%+)**: ✓✓✓ Data is highly reliable across all dimensions
- **HIGH (75-89%)**: ✓✓ Generally reliable with minor concerns
- **MODERATE (60-74%)**: ⚠ Some reliability concerns requiring investigation
- **LOW (40-59%)**: ⚠⚠ Significant reliability issues, use with caution
- **VERY LOW (<40%)**: 🚩 Questionable reliability, thorough review required

## Requirements

- Python 3.7+
- pandas >= 1.3.0
- numpy >= 1.21.0
- reportlab >= 3.6.0 (for PDF generation)
- matplotlib >= 3.5.0 (for charts and visualizations)
- openpyxl >= 3.0.7 (for Excel file support)

## License

This project is licensed under the MIT License - see the LICENSE file for details.
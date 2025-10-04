"""
Data Quality Validator

A comprehensive tool for validating data quality and assessing reliability
for college workforce and student body data.

Requirements:
- pandas
- numpy
- reportlab (for PDF generation)
- matplotlib (for charts and visualizations)

Install dependencies:
pip install pandas numpy reportlab matplotlib
"""

import pandas as pd
import numpy as np
import re
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')

# PDF generation imports
try:
    from reportlab.lib.pagesizes import letter
    from reportlab.platypus import (SimpleDocTemplate, Paragraph, Spacer,
                                    Table, TableStyle, PageBreak, Image)
    from reportlab.lib.styles import (getSampleStyleSheet,
                                      ParagraphStyle)
    from reportlab.lib.units import inch
    from reportlab.lib.colors import HexColor
    from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
    import matplotlib.pyplot as plt
    import matplotlib.patches as patches
    from io import BytesIO
    PDF_AVAILABLE = True
except ImportError:
    PDF_AVAILABLE = False
    print("Warning: reportlab and/or matplotlib not installed. "
          "PDF generation not available.")
    print("Install with: pip install reportlab matplotlib")


class DataQualityValidator:
    """
    Validates data quality and assesses reliability for college workforce
    and student body data. Focuses on data confidence scoring rather than
    source comparison.
    """
    
    def __init__(self, source1_path, source2_path,
                 historical_data_config=None):
        """
        Initialize the validator with two data sources and optional
        historical data.
        
        Args:
            source1_path: Path to first Excel file
            source2_path: Path to second Excel file
            historical_data_config: List of dicts with historical data
                information
                Format: [
                    {'path': 'report_2023.xlsx', 'year': 2023, 'source': 'A'},
                    {'path': 'report_2022.xlsx', 'year': 2022, 'source': 'B'},
                ]
        """
        self.source1_path = source1_path
        self.source2_path = source2_path
        self.historical_data_config = historical_data_config or []
        self.source1 = None
        self.source2 = None
        self.historical_data = []
        self.report = {
            'data_quality_issues': [],
            'reliability_concerns': [],
            'red_flags': [],
            'confidence_scores': {},
            'summary': {}
        }
        
    def load_data(self):
        """Load data from both sources and all historical files."""
        print("Loading data sources...")
        self.source1 = pd.read_excel(self.source1_path)
        self.source2 = pd.read_excel(self.source2_path)
        
        self.historical_data = []
        
        # Convert old format if needed
        if (self.historical_data_config and
                isinstance(self.historical_data_config[0], str)):
            self.historical_data_config = [
                {'path': p} for p in self.historical_data_config
            ]
        
        for config in self.historical_data_config:
            try:
                hist_path = config['path']
                df = pd.read_excel(hist_path)
                
                year = (config.get('year') or
                        self._extract_year_from_path(hist_path))
                source = (config.get('source') or
                          self._extract_source_from_path(hist_path))
                
                self.historical_data.append({
                    'path': hist_path,
                    'year': year,
                    'source': source,
                    'data': df
                })
                
                source_label = f" [Source: {source}]" if source else ""
                year_label = f" ({year})" if year else ""
                print(f"  ✓ Loaded: {Path(hist_path).name}"
                      f"{year_label}{source_label}")
                
            except (FileNotFoundError, PermissionError,
                    ValueError, OSError) as e:
                print(f"  ✗ Warning: Could not load "
                      f"{config.get('path')}: {str(e)}")
        
        self.historical_data.sort(
            key=lambda x: (x['year'] if x['year'] else 0,
                           x['source'] if x['source'] else ''),
            reverse=True
        )
        
        print(f"\nSource 1 loaded: {self.source1.shape[0]} rows, "
              f"{self.source1.shape[1]} columns")
        print(f"Source 2 loaded: {self.source2.shape[0]} rows, "
              f"{self.source2.shape[1]} columns")
        print(f"Historical files loaded: {len(self.historical_data)}")
        
        self._summarize_historical_coverage()
        
    def _extract_year_from_path(self, path):
        """Try to extract year from file path."""
        path_str = str(path)
        match = re.search(r'(20\d{2})', path_str)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_source_from_path(self, path):
        """Try to extract source identifier from file path."""
        path_str = str(path).lower()
        
        patterns = [
            (r'source[_\s]*a', 'A'),
            (r'source[_\s]*b', 'B'),
            (r'source[_\s]*1', 'A'),
            (r'source[_\s]*2', 'B'),
            (r'hr[_\s]', 'HR'),
            (r'payroll', 'Payroll'),
            (r'student', 'Student'),
            (r'sis', 'SIS'),
            (r'ipeds', 'IPEDS'),
        ]
        
        for pattern, source_name in patterns:
            if re.search(pattern, path_str):
                return source_name
        
        return None
    
    def _summarize_historical_coverage(self):
        """Print a summary of historical data coverage."""
        if not self.historical_data:
            return
        
        print("\n📚 Historical Data Coverage:")
        
        years = {}
        for hist in self.historical_data:
            year = hist['year'] or 'Unknown'
            if year not in years:
                years[year] = []
            source_label = (hist['source'] if hist['source']
                            else 'Unknown Source')
            years[year].append(source_label)
        
        for year in sorted(years.keys(), reverse=True):
            if year != 'Unknown':
                sources = ', '.join(years[year])
                print(f"  {year}: {sources}")
        
        if 'Unknown' in years:
            print(f"  Unknown Year: {', '.join(years['Unknown'])}")
    
    def assess_data_completeness(self):
        """Assess data completeness and calculate confidence score."""
        print("\n=== Assessing Data Completeness ===")
        
        completeness_scores = {}
        
        # Source 1 completeness
        source1_missing = self.source1.isnull().sum()
        source1_total_cells = len(self.source1) * len(self.source1.columns)
        source1_missing_cells = source1_missing.sum()
        source1_completeness = (
            (source1_total_cells - source1_missing_cells) /
            source1_total_cells) * 100
        
        print(f"\nSource 1 Completeness: {source1_completeness:.2f}%")
        completeness_scores['source1'] = source1_completeness
        
        if source1_missing.sum() == 0:
            print("✓ No missing data in Source 1")
        else:
            critical_missing = []
            for col in source1_missing[source1_missing > 0].index:
                pct = (source1_missing[col] / len(self.source1)) * 100
                if pct > 50:
                    issue = (f"Source 1 - Critical: '{col}' is {pct:.1f}% "
                             f"missing (may compromise data reliability)")
                    print(f"🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    critical_missing.append(col)
                elif pct > 10:
                    concern = (f"Source 1 - '{col}' has {pct:.1f}% "
                               f"missing values")
                    print(f"⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
        
        # Source 2 completeness
        source2_missing = self.source2.isnull().sum()
        source2_total_cells = len(self.source2) * len(self.source2.columns)
        source2_missing_cells = source2_missing.sum()
        source2_completeness = (
            (source2_total_cells - source2_missing_cells) /
            source2_total_cells) * 100
        
        print(f"\nSource 2 Completeness: {source2_completeness:.2f}%")
        completeness_scores['source2'] = source2_completeness
        
        if source2_missing.sum() == 0:
            print("✓ No missing data in Source 2")
        else:
            critical_missing = []
            for col in source2_missing[source2_missing > 0].index:
                pct = (source2_missing[col] / len(self.source2)) * 100
                if pct > 50:
                    issue = (f"Source 2 - Critical: '{col}' is {pct:.1f}% "
                             f"missing (may compromise data reliability)")
                    print(f"🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    critical_missing.append(col)
                elif pct > 10:
                    concern = (f"Source 2 - '{col}' has {pct:.1f}% "
                               f"missing values")
                    print(f"⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
        
        return completeness_scores
    
    def assess_internal_consistency(self):
        """Check if each source is internally consistent."""
        print("\n=== Assessing Internal Consistency ===")
        
        consistency_scores = {'source1': 0, 'source2': 0}
        max_points = 0
        
        # Check for duplicates
        max_points += 1
        duplicates1 = self.source1.duplicated().sum()
        duplicates2 = self.source2.duplicated().sum()
        
        if duplicates1 > 0:
            pct = (duplicates1 / len(self.source1)) * 100
            if pct > 5:
                issue = (f"Source 1 contains {duplicates1} duplicate rows "
                         f"({pct:.1f}%) - indicates data quality problems")
                print(f"🚩 {issue}")
                self.report['red_flags'].append(issue)
            else:
                concern = (f"Source 1 contains {duplicates1} duplicate "
                           f"rows ({pct:.1f}%)")
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
        else:
            print("✓ No duplicates in Source 1")
            consistency_scores['source1'] += 1
        
        if duplicates2 > 0:
            pct = (duplicates2 / len(self.source2)) * 100
            if pct > 5:
                issue = (f"Source 2 contains {duplicates2} duplicate rows "
                         f"({pct:.1f}%) - indicates data quality problems")
                print(f"🚩 {issue}")
                self.report['red_flags'].append(issue)
            else:
                concern = (f"Source 2 contains {duplicates2} duplicate "
                           f"rows ({pct:.1f}%)")
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
        else:
            print("✓ No duplicates in Source 2")
            consistency_scores['source2'] += 1
        
        # Check for outliers in numeric data
        print("\nChecking for statistical outliers...")
        numeric_cols = self.source1.select_dtypes(include=[np.number]).columns
        
        for col in numeric_cols:
            max_points += 1
            
            # Source 1 outliers
            if col in self.source1.columns:
                q1 = self.source1[col].quantile(0.25)
                q3 = self.source1[col].quantile(0.75)
                iqr = q3 - q1
                outliers1 = ((self.source1[col] < (q1 - 3 * iqr)) |
                             (self.source1[col] > (q3 + 3 * iqr))).sum()
                
                if outliers1 > 0:
                    pct = (outliers1 / len(self.source1)) * 100
                    if pct > 5:
                        concern = (f"Source 1 - '{col}' has {outliers1} "
                                   f"extreme outliers ({pct:.1f}%)")
                        print(f"⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                else:
                    consistency_scores['source1'] += 1
        
        for col in numeric_cols:
            # Source 2 outliers
            if col in self.source2.columns:
                q1 = self.source2[col].quantile(0.25)
                q3 = self.source2[col].quantile(0.75)
                iqr = q3 - q1
                outliers2 = ((self.source2[col] < (q1 - 3 * iqr)) |
                             (self.source2[col] > (q3 + 3 * iqr))).sum()
                
                if outliers2 > 0:
                    pct = (outliers2 / len(self.source2)) * 100
                    if pct > 5:
                        concern = (f"Source 2 - '{col}' has {outliers2} "
                                   f"extreme outliers ({pct:.1f}%)")
                        print(f"⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                else:
                    consistency_scores['source2'] += 1
        
        # Normalize scores
        if max_points > 0:
            consistency_scores['source1'] = (
                (consistency_scores['source1'] / max_points) * 100)
            consistency_scores['source2'] = (
                (consistency_scores['source2'] / max_points) * 100)
        
        return consistency_scores
    
    def assess_source_agreement(self, key_columns=None):
        """Assess how much the two sources agree
        (without declaring one correct)."""
        print("\n=== Assessing Cross-Source Agreement ===")
        
        agreement_score = 0
        
        # Schema agreement
        cols1 = set(self.source1.columns)
        cols2 = set(self.source2.columns)
        common_cols = cols1 & cols2
        
        if cols1 == cols2:
            print("✓ Both sources have identical schema")
            schema_agreement = 100
        else:
            schema_agreement = (len(common_cols) / len(cols1 | cols2)) * 100
            missing_in_source2 = cols1 - cols2
            missing_in_source1 = cols2 - cols1
            
            if missing_in_source2:
                concern = (f"Schema mismatch: {len(missing_in_source2)} "
                           f"columns in Source 1 not in Source 2")
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
            
            if missing_in_source1:
                concern = (f"Schema mismatch: {len(missing_in_source1)} "
                           f"columns in Source 2 not in Source 1")
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
        
        print(f"Schema agreement: {schema_agreement:.1f}%")
        
        # Record count agreement
        count_diff_pct = (abs(len(self.source1) - len(self.source2)) /
                          max(len(self.source1), len(self.source2)) * 100)
        
        if count_diff_pct < 1:
            print(f"✓ Record counts are very close "
                  f"(difference: {count_diff_pct:.2f}%)")
            count_agreement = 100
        elif count_diff_pct < 5:
            print(f"⚠️  Record counts differ by {count_diff_pct:.1f}%")
            count_agreement = 95
            concern = (f"Record count discrepancy: {count_diff_pct:.1f}% "
                       f"difference between sources")
            self.report['reliability_concerns'].append(concern)
        else:
            print(f"🚩 Significant record count difference: "
                  f"{count_diff_pct:.1f}%")
            count_agreement = max(0, 100 - count_diff_pct)
            issue = (f"Major record count discrepancy: Source 1 has "
                     f"{len(self.source1)} records, Source 2 has "
                     f"{len(self.source2)} ({count_diff_pct:.1f}% difference)")
            self.report['red_flags'].append(issue)
        
        # Value agreement (for common records)
        value_agreement = None
        
        if key_columns:
            try:
                common_cols_list = list(common_cols)
                if all(key in common_cols_list for key in key_columns):
                    merged = pd.merge(
                        self.source1,
                        self.source2,
                        on=key_columns,
                        how='inner',
                        suffixes=('_src1', '_src2')
                    )
                    
                    if len(merged) > 0:
                        total_comparisons = 0
                        matching_values = 0
                        disagreement_cols = []
                        
                        for col in common_cols:
                            if col not in key_columns:
                                col1 = f"{col}_src1"
                                col2 = f"{col}_src2"
                                
                                if (col1 in merged.columns and
                                        col2 in merged.columns):
                                    # Count matching values
                                    matches = ((merged[col1] == merged[col2]) |
                                               (merged[col1].isna() &
                                                merged[col2].isna()))
                                    total_comparisons += len(merged)
                                    matching_values += matches.sum()
                                    
                                    match_pct = (
                                        (matches.sum() / len(merged)) * 100)
                                    if match_pct < 90:
                                        disagreement_cols.append(
                                            (col, match_pct))
                        
                        if total_comparisons > 0:
                            value_agreement = (
                                (matching_values / total_comparisons) * 100)
                            print(f"\nValue agreement across common records: "
                                  f"{value_agreement:.1f}%")
                            
                            if value_agreement < 70:
                                issue = (f"Low agreement between sources: "
                                         f"only {value_agreement:.1f}% match")
                                print(f"🚩 {issue}")
                                self.report['red_flags'].append(issue)
                            elif value_agreement < 90:
                                concern = (f"Moderate disagreement between "
                                           f"sources: {value_agreement:.1f}% "
                                           f"agreement")
                                print(f"⚠️  {concern}")
                                self.report['reliability_concerns'].append(
                                    concern)
                            else:
                                print("✓ Sources show high agreement on "
                                      "common records")
                            
                            # Report specific columns with disagreement
                            if disagreement_cols:
                                print("\nColumns with notable disagreement:")
                                for col, match_pct in sorted(
                                        disagreement_cols, key=lambda x: x[1]):
                                    if match_pct < 80:
                                        concern = (f"  '{col}': only "
                                                   f"{match_pct:.1f}% "
                                                   f"agreement")
                                        print(f"  🚩 {concern}")
                                        concerns = (
                                            self.report[
                                                'reliability_concerns'])
                                        concerns.append(
                                            f"Low agreement on '{col}' "
                                            f"({match_pct:.1f}%)")
            
            except (KeyError, ValueError, pd.errors.MergeError) as e:
                print(f"Could not assess value agreement: {str(e)}")
        
        # Calculate overall agreement score
        weights = {'schema': 0.2, 'count': 0.3, 'values': 0.5}
        
        agreement_score = (schema_agreement * weights['schema'] +
                           count_agreement * weights['count'])
        
        if value_agreement is not None:
            agreement_score += value_agreement * weights['values']
        else:
            # Redistribute weight if can't calculate value agreement
            agreement_score = (schema_agreement * 0.4 + count_agreement * 0.6)
        
        return {
            'overall': agreement_score,
            'schema': schema_agreement,
            'count': count_agreement,
            'values': value_agreement
        }
    
    def assess_historical_plausibility(self, key_metric_columns=None):
        """Assess whether current data is plausible given historical
        patterns."""
        if not self.historical_data:
            print("\n⚠️  No historical data provided. Cannot assess "
                  "historical plausibility.")
            return None
        
        print("\n=== Assessing Historical Plausibility ===")
        
        if key_metric_columns is None:
            key_metric_columns = (
                self.source1.select_dtypes(include=[np.number])
                .columns.tolist())
            key_metric_columns = [
                col for col in key_metric_columns
                if any(keyword in col.lower()
                       for keyword in ['total', 'count', 'number',
                                       'enrollment', 'headcount', 'employee',
                                       'student', 'staff', 'fte'])
            ]
        
        if not key_metric_columns:
            key_metric_columns = (
                self.source1.select_dtypes(include=[np.number])
                .columns.tolist()[:5])
        
        plausibility_scores = {'source1': [], 'source2': []}
        
        print(f"\nAnalyzing {len(key_metric_columns)} key metrics against "
              f"historical patterns...")
        
        for col in key_metric_columns:
            # Collect all historical values
            historical_totals = []
            
            for hist in self.historical_data:
                if col in hist['data'].columns:
                    if pd.api.types.is_numeric_dtype(hist['data'][col]):
                        historical_totals.append(hist['data'][col].sum())
            
            if len(historical_totals) >= 3:
                hist_mean = np.mean(historical_totals)
                hist_std = np.std(historical_totals)
                hist_min = min(historical_totals)
                hist_max = max(historical_totals)
                
                # Calculate reasonable range (mean ± 2.5 std dev)
                reasonable_min = hist_mean - 2.5 * hist_std
                reasonable_max = hist_mean + 2.5 * hist_std
                
                print(f"\n📊 {col}:")
                print(f"   Historical range: {hist_min:,.0f} to "
                      f"{hist_max:,.0f}")
                print(f"   Historical mean: {hist_mean:,.0f} "
                      f"(±{hist_std:,.0f})")
                
                # Check Source 1
                if col in self.source1.columns:
                    current_val1 = self.source1[col].sum()
                    z_score1 = (abs(current_val1 - hist_mean) / hist_std
                                if hist_std > 0 else 0)
                    
                    print(f"   Source 1: {current_val1:,.0f} "
                          f"(z-score: {z_score1:.2f})")
                    
                    if (current_val1 < reasonable_min or
                            current_val1 > reasonable_max):
                        issue = (f"Source 1 '{col}' ({current_val1:,.0f}) is "
                                 f"outside reasonable historical range")
                        print(f"   🚩 {issue}")
                        self.report['red_flags'].append(issue)
                        plausibility_scores['source1'].append(
                            max(0, 100 - (z_score1 * 20)))
                    elif z_score1 > 2:
                        concern = (f"Source 1 '{col}' shows unusual deviation "
                                   f"from historical pattern")
                        print(f"   ⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                        plausibility_scores['source1'].append(
                            max(70, 100 - (z_score1 * 10)))
                    else:
                        print("   ✓ Within expected range")
                        plausibility_scores['source1'].append(100)
                
                # Check Source 2
                if col in self.source2.columns:
                    current_val2 = self.source2[col].sum()
                    z_score2 = (abs(current_val2 - hist_mean) / hist_std
                                if hist_std > 0 else 0)
                    
                    print(f"   Source 2: {current_val2:,.0f} "
                          f"(z-score: {z_score2:.2f})")
                    
                    if (current_val2 < reasonable_min or
                            current_val2 > reasonable_max):
                        issue = (f"Source 2 '{col}' ({current_val2:,.0f}) is "
                                 f"outside reasonable historical range")
                        print(f"   🚩 {issue}")
                        self.report['red_flags'].append(issue)
                        plausibility_scores['source2'].append(
                            max(0, 100 - (z_score2 * 20)))
                    elif z_score2 > 2:
                        concern = (f"Source 2 '{col}' shows unusual deviation "
                                   f"from historical pattern")
                        print(f"   ⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                        plausibility_scores['source2'].append(
                            max(70, 100 - (z_score2 * 10)))
                    else:
                        print("   ✓ Within expected range")
                        plausibility_scores['source2'].append(100)
        
        # Check record counts
        print("\n📈 Record Count Plausibility:")
        historical_counts = [len(h['data']) for h in self.historical_data]
        
        if len(historical_counts) >= 3:
            count_mean = np.mean(historical_counts)
            count_std = np.std(historical_counts)
            
            print(f"   Historical average: {count_mean:.0f} records "
                  f"(±{count_std:.0f})")
            print(f"   Source 1: {len(self.source1)} records")
            print(f"   Source 2: {len(self.source2)} records")
            
            if count_std > 0:
                z1 = abs(len(self.source1) - count_mean) / count_std
                z2 = abs(len(self.source2) - count_mean) / count_std
                
                if z1 > 2.5:
                    issue = (f"Source 1 record count ({len(self.source1)}) is "
                             f"highly unusual compared to history")
                    print(f"   🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    plausibility_scores['source1'].append(
                        max(0, 100 - (z1 * 20)))
                elif z1 > 2:
                    concern = (f"Source 1 record count is somewhat unusual "
                               f"(z={z1:.2f})")
                    print(f"   ⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
                    plausibility_scores['source1'].append(
                        max(70, 100 - (z1 * 10)))
                else:
                    print(f"   ✓ Source 1 within expected range (z={z1:.2f})")
                    plausibility_scores['source1'].append(100)
                
                if z2 > 2.5:
                    issue = (f"Source 2 record count ({len(self.source2)}) is "
                             f"highly unusual compared to history")
                    print(f"   🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    plausibility_scores['source2'].append(
                        max(0, 100 - (z2 * 20)))
                elif z2 > 2:
                    concern = (f"Source 2 record count is somewhat unusual "
                               f"(z={z2:.2f})")
                    print(f"   ⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
                    plausibility_scores['source2'].append(
                        max(70, 100 - (z2 * 10)))
                else:
                    print(f"   ✓ Source 2 within expected range (z={z2:.2f})")
                    plausibility_scores['source2'].append(100)
        
        # Calculate average plausibility scores
        avg_plausibility = {}
        if plausibility_scores['source1']:
            avg_plausibility['source1'] = np.mean(
                plausibility_scores['source1'])
        if plausibility_scores['source2']:
            avg_plausibility['source2'] = np.mean(
                plausibility_scores['source2'])
        
        return avg_plausibility
    
    def calculate_confidence_scores(self):
        """Calculate overall confidence scores for each source and the data
        in general."""
        print("\n" + "="*70)
        print("CALCULATING CONFIDENCE SCORES")
        print("="*70)
        
        scores = self.report['confidence_scores']
        
        # Weight factors
        weights = {
            'completeness': 0.25,
            'consistency': 0.25,
            'agreement': 0.25,
            'plausibility': 0.25
        }
        
        # Calculate weighted scores for each source
        for source_key in ['source1', 'source2']:
            source_scores = []
            
            if ('completeness' in scores and
                    source_key in scores['completeness']):
                source_scores.append(
                    scores['completeness'][source_key] *
                    weights['completeness'])
            
            if 'consistency' in scores and source_key in scores['consistency']:
                source_scores.append(
                    scores['consistency'][source_key] *
                    weights['consistency'])
            
            if ('plausibility' in scores and
                    source_key in scores['plausibility']):
                source_scores.append(
                    scores['plausibility'][source_key] *
                    weights['plausibility'])
            
            if source_scores:
                # Agreement score applies to both
                if 'agreement' in scores and 'overall' in scores['agreement']:
                    source_scores.append(
                        scores['agreement']['overall'] *
                        weights['agreement'])
                
                scores[f'{source_key}_overall'] = (
                    sum(source_scores) / len(source_scores) *
                    (len(source_scores) / len(weights)))
        
        # Calculate overall data confidence (not source-specific)
        all_factors = []
        
        if 'completeness' in scores:
            avg_completeness = np.mean([
                scores['completeness'].get('source1', 0),
                scores['completeness'].get('source2', 0)
            ])
            all_factors.append(avg_completeness * weights['completeness'])
        
        if 'consistency' in scores:
            avg_consistency = np.mean([
                scores['consistency'].get('source1', 0),
                scores['consistency'].get('source2', 0)
            ])
            all_factors.append(avg_consistency * weights['consistency'])
        
        if 'agreement' in scores and 'overall' in scores['agreement']:
            all_factors.append(
                scores['agreement']['overall'] * weights['agreement'])
        
        if 'plausibility' in scores:
            avg_plausibility = np.mean([
                scores['plausibility'].get('source1', 0),
                scores['plausibility'].get('source2', 0)
            ])
            all_factors.append(avg_plausibility * weights['plausibility'])
        
        if all_factors:
            scores['overall_data_confidence'] = (
                sum(all_factors) / len(all_factors) *
                (len(all_factors) / len(weights)))
        
        # Interpret scores
        print("\n📊 CONFIDENCE SCORES")
        print("-" * 70)
        
        # Overall data confidence
        if 'overall_data_confidence' in scores:
            overall = scores['overall_data_confidence']
            print(f"\nOverall Data Confidence: {overall:.1f}%")
            
            if overall >= 90:
                level = "VERY HIGH"
                interpretation = ("The data appears highly reliable across "
                                  "all dimensions.")
            elif overall >= 75:
                level = "HIGH"
                interpretation = ("The data appears generally reliable with "
                                  "minor concerns.")
            elif overall >= 60:
                level = "MODERATE"
                interpretation = ("The data has some reliability concerns "
                                  "that should be investigated.")
            elif overall >= 40:
                level = "LOW"
                interpretation = ("The data has significant reliability "
                                  "issues. Use with caution.")
            else:
                level = "VERY LOW"
                interpretation = ("The data reliability is questionable. "
                                  "Thorough review required.")
            
            print(f"Confidence Level: {level}")
            print(f"Interpretation: {interpretation}")
        
        # Individual source scores
        print(f"\nSource 1 Confidence: "
              f"{scores.get('source1_overall', 0):.1f}%")
        print(f"Source 2 Confidence: {scores.get('source2_overall', 0):.1f}%")
        
        # Component breakdown
        print("\n📋 Component Scores:")
        if 'completeness' in scores:
            print("  Completeness:")
            print(f"    - Source 1: "
                  f"{scores['completeness'].get('source1', 0):.1f}%")
            print(f"    - Source 2: "
                  f"{scores['completeness'].get('source2', 0):.1f}%")
        
        if 'consistency' in scores:
            print("  Internal Consistency:")
            print(f"    - Source 1: "
                  f"{scores['consistency'].get('source1', 0):.1f}%")
            print(f"    - Source 2: "
                  f"{scores['consistency'].get('source2', 0):.1f}%")
        
        if 'agreement' in scores and 'overall' in scores['agreement']:
            print(f"  Cross-Source Agreement: "
                  f"{scores['agreement']['overall']:.1f}%")
        
        if 'plausibility' in scores:
            print("  Historical Plausibility:")
            print(f"    - Source 1: "
                  f"{scores['plausibility'].get('source1', 0):.1f}%")
            print(f"    - Source 2: "
                  f"{scores['plausibility'].get('source2', 0):.1f}%")
        
        print("\n" + "="*70)
    
    def generate_report(self, output_path='data_quality_report.txt', 
                       generate_pdf=True):
        """Generate a comprehensive data quality and confidence report."""
        print("\n" + "="*70)
        print("GENERATING DATA QUALITY & CONFIDENCE REPORT")
        print("="*70)
        
        total_concerns = (
            len(self.report['red_flags']) +
            len(self.report['reliability_concerns'])
        )
        
        self.report['summary'] = {
            'total_concerns': total_concerns,
            'red_flags': len(self.report['red_flags']),
            'reliability_concerns': len(self.report['reliability_concerns']),
            'generated_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        
        # Write report to file
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("="*70 + "\n")
            f.write("DATA QUALITY & RELIABILITY ASSESSMENT\n")
            f.write("College Workforce and Student Body Data\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Generated: {self.report['summary']['generated_at']}\n")
            f.write(f"Source 1: {self.source1_path}\n")
            f.write(f"Source 2: {self.source2_path}\n")
            
            if self.historical_data:
                f.write(f"\nHistorical Data Files: "
                        f"{len(self.historical_data)}\n")
                years = {}
                for hist in self.historical_data:
                    year = hist['year'] or 'Unknown'
                    if year not in years:
                        years[year] = []
                    source_label = (hist['source'] if hist['source']
                                   else 'Unknown')
                    years[year].append((Path(hist['path']).name, source_label))
                
                for year in sorted(years.keys(), reverse=True):
                    if year != 'Unknown':
                        f.write(f"  {year}:\n")
                        for filename, source in years[year]:
                            f.write(f"    - {filename} (Source: {source})\n")
            
            f.write("\n")
            f.write("="*70 + "\n")
            f.write("CONFIDENCE SCORES\n")
            f.write("="*70 + "\n\n")
            
            scores = self.report['confidence_scores']
            
            if 'overall_data_confidence' in scores:
                overall = scores['overall_data_confidence']
                f.write(f"Overall Data Confidence: {overall:.1f}%\n\n")
                
                if overall >= 90:
                    level = "VERY HIGH ✓✓✓"
                    interpretation = ("The data appears highly reliable "
                                      "across all dimensions.\nYou can use "
                                      "this data with high confidence for "
                                      "decision-making.")
                elif overall >= 75:
                    level = "HIGH ✓✓"
                    interpretation = ("The data appears generally reliable "
                                      "with minor concerns.\nSuitable for "
                                      "most reporting and analysis purposes.")
                elif overall >= 60:
                    level = "MODERATE ⚠"
                    interpretation = ("The data has some reliability "
                                      "concerns that should be "
                                      "investigated.\nReview specific "
                                      "issues before using for critical "
                                      "decisions.")
                elif overall >= 40:
                    level = "LOW ⚠⚠"
                    interpretation = ("The data has significant "
                                      "reliability issues.\nUse with "
                                      "caution and verify key figures "
                                      "independently.")
                else:
                    level = "VERY LOW 🚩"
                    interpretation = ("The data reliability is "
                                      "questionable.\nThorough review "
                                      "and validation required before "
                                      "use.")
                
                f.write(f"Confidence Level: {level}\n\n")
                f.write(f"{interpretation}\n\n")
            
            f.write("Individual Source Confidence:\n")
            f.write(f"  Source 1: {scores.get('source1_overall', 0):.1f}%\n")
            f.write(f"  Source 2: {scores.get('source2_overall', 0):.1f}%\n\n")
            
            f.write("Component Breakdown:\n")
            if 'completeness' in scores:
                f.write("  Data Completeness:\n")
                f.write(f"    - Source 1: "
                        f"{scores['completeness'].get('source1', 0):.1f}%\n")
                f.write(f"    - Source 2: "
                        f"{scores['completeness'].get('source2', 0):.1f}%\n")
            
            if 'consistency' in scores:
                f.write("  Internal Consistency:\n")
                f.write(f"    - Source 1: "
                        f"{scores['consistency'].get('source1', 0):.1f}%\n")
                f.write(f"    - Source 2: "
                        f"{scores['consistency'].get('source2', 0):.1f}%\n")
            
            if 'agreement' in scores:
                f.write(f"  Cross-Source Agreement: "
                        f"{scores['agreement'].get('overall', 0):.1f}%\n")
            
            if 'plausibility' in scores:
                f.write("  Historical Plausibility:\n")
                f.write(f"    - Source 1: "
                        f"{scores['plausibility'].get('source1', 0):.1f}%\n")
                f.write(f"    - Source 2: "
                        f"{scores['plausibility'].get('source2', 0):.1f}%\n")
            
            f.write("\n")
            f.write("="*70 + "\n")
            f.write("DETAILED FINDINGS\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Total Concerns: {total_concerns}\n")
            f.write(f"  - Critical Issues (Red Flags): "
                    f"{self.report['summary']['red_flags']}\n")
            f.write(f"  - Moderate Concerns: "
                    f"{self.report['summary']['reliability_concerns']}\n\n")
            
            if self.report['red_flags']:
                f.write("🚩 CRITICAL ISSUES (RED FLAGS)\n")
                f.write("-"*70 + "\n")
                f.write("These are serious data quality problems that "
                        "significantly impact reliability:\n\n")
                for issue in self.report['red_flags']:
                    f.write(f"  • {issue}\n")
                f.write("\n")
            
            if self.report['reliability_concerns']:
                f.write("⚠️  RELIABILITY CONCERNS\n")
                f.write("-"*70 + "\n")
                f.write("These issues may affect data reliability and "
                        "should be reviewed:\n\n")
                for concern in self.report['reliability_concerns']:
                    f.write(f"  • {concern}\n")
                f.write("\n")
            
            if (not self.report['red_flags'] and
                    not self.report['reliability_concerns']):
                f.write("✓ NO SIGNIFICANT ISSUES FOUND\n")
                f.write("-"*70 + "\n")
                f.write("The data passed all quality checks without "
                        "major concerns.\n\n")
            
            f.write("="*70 + "\n")
            f.write("RECOMMENDATIONS\n")
            f.write("="*70 + "\n\n")
            
            if 'overall_data_confidence' in scores:
                overall = scores['overall_data_confidence']
                
                if overall >= 75:
                    f.write("✓ DATA IS SUITABLE FOR USE\n\n")
                    f.write("The data shows good reliability across "
                            "multiple dimensions.\n")
                    f.write("You can proceed with using this data for "
                            "reporting and analysis.\n\n")
                    
                    if self.report['reliability_concerns']:
                        f.write("Minor recommendations:\n")
                        f.write("  - Review the concerns listed above "
                                "for awareness\n")
                        f.write("  - Document any known data collection "
                                "changes\n")
                        f.write("  - Consider monitoring these metrics in "
                                "future reports\n")
                
                elif overall >= 60:
                    f.write("⚠️  USE DATA WITH CAUTION\n\n")
                    f.write("The data has moderate reliability concerns.\n\n")
                    f.write("Recommended actions:\n")
                    f.write("  1. Investigate the specific issues "
                            "flagged above\n")
                    f.write("  2. Verify key metrics independently "
                            "before publication\n")
                    f.write("  3. Consider adding data quality notes "
                            "to reports\n")
                    f.write("  4. Establish data validation processes "
                            "for future submissions\n")
                
                else:
                    f.write("🚩 DATA REQUIRES SIGNIFICANT REVIEW\n\n")
                    f.write("The data has substantial reliability issues.\n\n")
                    f.write("Critical actions required:\n")
                    f.write("  1. DO NOT use this data for official "
                            "reporting without validation\n")
                    f.write("  2. Investigate all red flags immediately\n")
                    f.write("  3. Verify data collection and "
                            "extraction processes\n")
                    f.write("  4. Consider re-pulling data from "
                            "source systems\n")
                    f.write("  5. Document all discrepancies and "
                            "their resolutions\n")
            
            f.write("\n")
            f.write("="*70 + "\n")
            f.write("INTERPRETING YOUR SCORES\n")
            f.write("="*70 + "\n\n")
            
            f.write("What the scores mean:\n\n")
            f.write("Completeness (25% of overall score):\n")
            f.write("  - Measures how much data is present vs. missing\n")
            f.write("  - Higher = fewer missing values\n\n")
            
            f.write("Internal Consistency (25% of overall score):\n")
            f.write("  - Measures data quality within each source\n")
            f.write("  - Checks for duplicates, outliers, and "
                    "logical errors\n")
            f.write("  - Higher = cleaner, more reliable data\n\n")
            
            f.write("Cross-Source Agreement (25% of overall score):\n")
            f.write("  - Measures how well the two sources align\n")
            f.write("  - Higher = sources tell a consistent story\n")
            f.write("  - Lower = sources disagree (investigate why)\n\n")
            
            f.write("Historical Plausibility (25% of overall score):\n")
            f.write("  - Measures if current data makes sense "
                    "given past patterns\n")
            f.write("  - Higher = data follows expected trends\n")
            f.write("  - Lower = unusual patterns (may indicate "
                    "errors or real changes)\n\n")
            
            f.write("Note: Low cross-source agreement doesn't mean "
                    "the data is wrong,\n")
            f.write("it means the sources disagree and you need to "
                    "investigate why.\n")
            f.write("Both sources could be correct but measuring "
                    "different things,\n")
            f.write("or one (or both) could have data quality issues.\n\n")
            
            f.write("="*70 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*70 + "\n")
        
        print(f"\n✓ Report saved to: {output_path}")
        print(f"\nSummary: {total_concerns} total concerns identified")
        
        if 'overall_data_confidence' in self.report['confidence_scores']:
            conf_scores = self.report['confidence_scores']
            overall_conf = conf_scores['overall_data_confidence']
            print(f"Overall Data Confidence: {overall_conf:.1f}%")
        
        # Generate PDF version if requested and available
        if generate_pdf and PDF_AVAILABLE:
            pdf_path = output_path.replace('.txt', '.pdf')
            if pdf_path == output_path:  # If not .txt extension
                pdf_path = output_path + '.pdf'
            self.generate_pdf_report(pdf_path)
        elif generate_pdf and not PDF_AVAILABLE:
            print("\n⚠️  PDF generation requested but reportlab "
                  "not available.")
            print("Install reportlab with: pip install reportlab")
        
        return self.report
    
    def generate_pdf_report(self, output_path='data_quality_report.pdf'):
        """Generate a comprehensive, visually appealing PDF data quality
        report."""
        if not PDF_AVAILABLE:
            raise ImportError("reportlab and matplotlib are required for "
                              "PDF generation. Install with: pip install "
                              "reportlab matplotlib")
        
        print(f"Generating enhanced PDF report: {output_path}")
        
        # Create the PDF document with custom styling
        doc = SimpleDocTemplate(
            output_path,
            pagesize=letter,
            rightMargin=0.75*inch,
            leftMargin=0.75*inch,
            topMargin=1*inch,
            bottomMargin=1*inch
        )
        story = []
        
        # Enhanced styles
        styles = getSampleStyleSheet()
        
        # Title style with blue color
        title_style = ParagraphStyle(
            'EnhancedTitle',
            parent=styles['Heading1'],
            fontSize=24,
            spaceAfter=30,
            spaceBefore=20,
            alignment=TA_CENTER,
            textColor=HexColor('#1f4e79'),
            fontName='Helvetica-Bold'
        )
        
        subtitle_style = ParagraphStyle(
            'Subtitle',
            parent=styles['Normal'],
            fontSize=14,
            spaceAfter=30,
            alignment=TA_CENTER,
            textColor=HexColor('#2c5aa0'),
            fontName='Helvetica-Oblique'
        )
        
        heading_style = ParagraphStyle(
            'EnhancedHeading',
            parent=styles['Heading2'],
            fontSize=16,
            spaceAfter=15,
            spaceBefore=25,
            textColor=HexColor('#1f4e79'),
            fontName='Helvetica-Bold',
            borderWidth=1,
            borderColor=HexColor('#1f4e79'),
            borderPadding=5
        )
        
        subheading_style = ParagraphStyle(
            'EnhancedSubHeading',
            parent=styles['Heading3'],
            fontSize=12,
            spaceAfter=10,
            spaceBefore=15,
            textColor=HexColor('#2c5aa0'),
            fontName='Helvetica-Bold'
        )
        
        body_style = ParagraphStyle(
            'EnhancedBody',
            parent=styles['Normal'],
            fontSize=10,
            spaceAfter=6,
            alignment=TA_JUSTIFY,
            fontName='Helvetica'
        )
        
        # Create title page
        story.append(Spacer(1, 1*inch))
        story.append(Paragraph("DATA QUALITY & RELIABILITY ASSESSMENT",
                               title_style))
        story.append(Paragraph("College Workforce and Student Body Data",
                               subtitle_style))
        story.append(Spacer(1, 0.5*inch))
        
        # Executive Summary Box
        scores = self.report['confidence_scores']
        if 'overall_data_confidence' in scores:
            overall = scores['overall_data_confidence']
            
            if overall >= 90:
                summary_color = HexColor('#d5f4e6')
                border_color = HexColor('#28a745')
                status = "EXCELLENT"
            elif overall >= 75:
                summary_color = HexColor('#d1ecf1')
                border_color = HexColor('#17a2b8')
                status = "GOOD"
            elif overall >= 60:
                summary_color = HexColor('#fff3cd')
                border_color = HexColor('#ffc107')
                status = "MODERATE"
            else:
                summary_color = HexColor('#f8d7da')
                border_color = HexColor('#dc3545')
                status = "NEEDS ATTENTION"
            
            # Executive summary table with colored background
            exec_summary = [
                ["EXECUTIVE SUMMARY"],
                [f"Overall Data Confidence: {overall:.1f}%"],
                [f"Data Quality Status: {status}"],
                [f"Generated: {self.report['summary']['generated_at']}"]
            ]
            
            exec_table = Table(exec_summary, colWidths=[5*inch])
            exec_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), border_color),
                ('BACKGROUND', (0, 1), (-1, -1), summary_color),
                ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 12),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 2, border_color),
                ('ROWBACKGROUNDS', (0, 1), (-1, -1), [summary_color]),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
                ('TOPPADDING', (0, 0), (-1, -1), 12),
            ]))
            story.append(exec_table)
        
        story.append(Spacer(1, 0.5*inch))
        
        # Report metadata in an attractive table
        metadata_data = [
            ["Report Details", ""],
            ["Source 1 File", str(self.source1_path)],
            ["Source 2 File", str(self.source2_path)],
        ]
        
        if self.historical_data:
            metadata_data.append(["Historical Files",
                                  f"{len(self.historical_data)} files"])
        
        total_concerns = (len(self.report['red_flags']) +
                          len(self.report['reliability_concerns']))
        metadata_data.append(["Total Concerns", str(total_concerns)])
        
        metadata_table = Table(metadata_data, colWidths=[2*inch, 3.5*inch])
        metadata_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1f4e79')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8f9fa')),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
            ('FONTNAME', (1, 1), (1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
        ]))
        story.append(metadata_table)
        
        # Page break before main content
        story.append(PageBreak())
        
        # Main Content - Confidence Scores with Visual Elements
        story.append(Paragraph("CONFIDENCE ASSESSMENT", heading_style))
        
        if 'overall_data_confidence' in scores:
            # Create confidence gauge visualization using matplotlib
            confidence_chart = self._create_confidence_gauge(overall)
            if confidence_chart:
                story.append(confidence_chart)
                story.append(Spacer(1, 20))
        
        # Enhanced Component Breakdown Table
        story.append(Paragraph("Detailed Component Analysis",
                               subheading_style))
        
        # Header with enhanced styling
        component_data = [
            ["Assessment Component", "Source 1 Score", "Source 2 Score",
             "Combined Score", "Status"]
        ]
        
        # Add component data with status indicators
        if 'completeness' in scores:
            s1_comp = scores['completeness'].get('source1', 0)
            s2_comp = scores['completeness'].get('source2', 0)
            avg_comp = (s1_comp + s2_comp) / 2
            status = ("✓ Good" if avg_comp >= 80 else
                      "⚠ Review" if avg_comp >= 60 else "🚩 Critical")
            component_data.append([
                "Data Completeness", f"{s1_comp:.1f}%", f"{s2_comp:.1f}%",
                f"{avg_comp:.1f}%", status
            ])
        
        if 'consistency' in scores:
            s1_cons = scores['consistency'].get('source1', 0)
            s2_cons = scores['consistency'].get('source2', 0)
            avg_cons = (s1_cons + s2_cons) / 2
            status = ("✓ Good" if avg_cons >= 80 else
                      "⚠ Review" if avg_cons >= 60 else "🚩 Critical")
            component_data.append([
                "Internal Consistency", f"{s1_cons:.1f}%", f"{s2_cons:.1f}%",
                f"{avg_cons:.1f}%", status
            ])
        
        if 'agreement' in scores:
            agreement_score = scores['agreement'].get('overall', 0)
            status = ("✓ Good" if agreement_score >= 80 else
                      "⚠ Review" if agreement_score >= 60 else "🚩 Critical")
            component_data.append([
                "Cross-Source Agreement", "-", "-",
                f"{agreement_score:.1f}%", status
            ])
        
        if 'plausibility' in scores:
            s1_plaus = scores['plausibility'].get('source1', 0)
            s2_plaus = scores['plausibility'].get('source2', 0)
            avg_plaus = ((s1_plaus + s2_plaus) / 2 if s1_plaus and s2_plaus
                         else (s1_plaus or s2_plaus or 0))
            status = ("✓ Good" if avg_plaus >= 80 else
                      "⚠ Review" if avg_plaus >= 60 else "🚩 Critical")
            component_data.append([
                "Historical Plausibility",
                f"{s1_plaus:.1f}%" if s1_plaus else "-",
                f"{s2_plaus:.1f}%" if s2_plaus else "-",
                f"{avg_plaus:.1f}%", status
            ])
        
        component_table = Table(
            component_data,
            colWidths=[2*inch, 0.8*inch, 0.8*inch, 0.8*inch, 1*inch]
        )
        component_table.setStyle(TableStyle([
            # Header styling
            ('BACKGROUND', (0, 0), (-1, 0), HexColor('#1f4e79')),
            ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 10),
            ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
            
            # Data rows
            ('BACKGROUND', (0, 1), (-1, -1), HexColor('#f8f9fa')),
            ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
            ('ALIGN', (0, 1), (-1, -1), 'CENTER'),
            ('ALIGN', (0, 1), (0, -1), 'LEFT'),
            
            # Grid and padding
            ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
            ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
            ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
            ('TOPPADDING', (0, 0), (-1, -1), 8),
            
            # Alternating row colors
            ('ROWBACKGROUNDS', (0, 1), (-1, -1),
             [HexColor('#ffffff'), HexColor('#f8f9fa')]),
        ]))
        story.append(component_table)
        story.append(Spacer(1, 20))
        
        # Issues Summary with Visual Impact
        total_concerns = (len(self.report['red_flags']) +
                          len(self.report['reliability_concerns']))
        
        if total_concerns > 0:
            story.append(Paragraph("IDENTIFIED ISSUES", heading_style))
            
            # Issues summary with color coding
            issues_summary = [
                ["Issue Type", "Count", "Impact Level"],
                ["Critical Issues (Red Flags)",
                 str(len(self.report['red_flags'])), "HIGH"],
                ["Reliability Concerns",
                 str(len(self.report['reliability_concerns'])), "MEDIUM"],
                ["Total Issues", str(total_concerns), "VARIES"]
            ]
            
            issues_table = Table(
                issues_summary,
                colWidths=[2.5*inch, 1*inch, 1.5*inch]
            )
            issues_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#dc3545')),
                ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BACKGROUND', (0, 1), (-1, 1), HexColor('#f8d7da')),
                ('BACKGROUND', (0, 2), (-1, 2), HexColor('#fff3cd')),
                ('BACKGROUND', (0, 3), (-1, 3), HexColor('#d1ecf1')),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 10),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('ALIGN', (0, 1), (0, -1), 'LEFT'),
                ('GRID', (0, 0), (-1, -1), 1, HexColor('#dee2e6')),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
                ('TOPPADDING', (0, 0), (-1, -1), 8),
            ]))
            story.append(issues_table)
            story.append(Spacer(1, 15))
            
            # Detailed issues
            if self.report['red_flags']:
                story.append(Paragraph("🚩 Critical Issues", subheading_style))
                # Limit to first 5 issues
                for i, issue in enumerate(self.report['red_flags'][:5], 1):
                    story.append(Paragraph(f"{i}. {issue}", body_style))
                if len(self.report['red_flags']) > 5:
                    additional = len(self.report['red_flags']) - 5
                    story.append(Paragraph(
                        f"... and {additional} more issues", body_style))
                story.append(Spacer(1, 10))
            
            if self.report['reliability_concerns']:
                story.append(Paragraph("⚠️ Reliability Concerns",
                                      subheading_style))
                # Limit to first 5 concerns for brevity
                rel_concerns = self.report['reliability_concerns'][:5]
                concerns_enum = enumerate(rel_concerns, 1)
                for i, concern in concerns_enum:
                    story.append(Paragraph(f"{i}. {concern}", body_style))
                if len(self.report['reliability_concerns']) > 5:
                    remaining = len(self.report["reliability_concerns"]) - 5
                    more_text = f"... and {remaining} more concerns"
                    story.append(Paragraph(more_text, body_style))
        else:
            # No issues found - positive message
            story.append(Paragraph("VALIDATION RESULTS", heading_style))
            no_issues_data = [
                ["✓ NO SIGNIFICANT ISSUES FOUND"],
                ["All data quality checks passed successfully"],
                ["The data appears reliable and ready for use"]
            ]
            no_issues_table = Table(no_issues_data, colWidths=[5*inch])
            no_issues_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), HexColor('#28a745')),
                ('BACKGROUND', (0, 1), (-1, -1), HexColor('#d5f4e6')),
                ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 12),
                ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 2, HexColor('#28a745')),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
                ('TOPPADDING', (0, 0), (-1, -1), 12),
            ]))
            story.append(no_issues_table)
        
        story.append(PageBreak())
        
        # Recommendations with action items
        story.append(Paragraph("RECOMMENDATIONS & NEXT STEPS", heading_style))
        
        if 'overall_data_confidence' in scores:
            overall = scores['overall_data_confidence']
            
            recommendations = []
            if overall >= 90:
                recommendations = [
                    "✓ Data quality is excellent and ready for production use",
                    "✓ Implement regular monitoring to maintain "
                    "quality standards",
                    "✓ Consider this dataset as a quality benchmark "
                    "for future assessments",
                    "✓ Document current processes to ensure consistency"
                ]
                rec_color = HexColor('#d5f4e6')
                border_color = HexColor('#28a745')
            elif overall >= 75:
                recommendations = [
                    "✓ Data quality is good with minor areas for improvement",
                    "⚠ Address the reliability concerns identified "
                    "in this report",
                    "✓ Implement data validation checks in your pipeline",
                    "✓ Schedule quarterly quality assessments"
                ]
                rec_color = HexColor('#d1ecf1')
                border_color = HexColor('#17a2b8')
            elif overall >= 60:
                recommendations = [
                    "⚠ Investigate and resolve identified quality "
                    "issues before use",
                    "⚠ Implement additional data validation and "
                    "cleaning procedures",
                    "⚠ Consider manual verification of key metrics",
                    "⚠ Establish monthly quality monitoring"
                ]
                rec_color = HexColor('#fff3cd')
                border_color = HexColor('#ffc107')
            else:
                recommendations = [
                    "🚩 DO NOT use this data for critical decisions "
                    "without validation",
                    "🚩 Immediately investigate all red flag issues",
                    "🚩 Review and improve data collection processes",
                    "🚩 Consider re-extracting data from source systems"
                ]
                rec_color = HexColor('#f8d7da')
                border_color = HexColor('#dc3545')
            
            # Create recommendations table
            rec_data = [["PRIORITY ACTIONS"]]
            for rec in recommendations:
                rec_data.append([rec])
            
            rec_table = Table(rec_data, colWidths=[6*inch])
            rec_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), border_color),
                ('BACKGROUND', (0, 1), (-1, -1), rec_color),
                ('TEXTCOLOR', (0, 0), (-1, 0), HexColor('#ffffff')),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 14),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 11),
                ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
                ('ALIGN', (0, 1), (-1, -1), 'LEFT'),
                ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
                ('GRID', (0, 0), (-1, -1), 1, border_color),
                ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
                ('TOPPADDING', (0, 0), (-1, -1), 10),
                ('LEFTPADDING', (0, 1), (-1, -1), 15),
            ]))
            story.append(rec_table)
        
        story.append(Spacer(1, 30))
        
        # Footer with contact/additional info
        footer_style = ParagraphStyle(
            'Footer',
            parent=styles['Normal'],
            fontSize=9,
            textColor=HexColor('#6c757d'),
            alignment=TA_CENTER
        )
        
        story.append(Paragraph("This report was generated by the "
                               "Data Quality Validator", footer_style))
        story.append(Paragraph("For questions or additional analysis, "
                               "please contact your data team",
                               footer_style))
        
        # Build the PDF
        doc.build(story)
        print(f"✓ Enhanced PDF report saved to: {output_path}")
        
        return output_path
    
    def _create_confidence_gauge(self, confidence_score):
        """Create a confidence gauge visualization using matplotlib."""
        try:
            # Create figure
            fig, ax = plt.subplots(figsize=(8, 4))
            fig.patch.set_facecolor('white')
            
            # Define gauge parameters
            # Convert to angle (-90 to 90 degrees)
            theta = confidence_score * 1.8 - 90
            
            # Create gauge background
            background = patches.Wedge((0.5, 0), 0.4, -90, 90,
                                       facecolor='lightgray', alpha=0.3)
            ax.add_patch(background)
            
            # Create colored segments
            segments = [
                (-90, -54, '#dc3545'),  # 0-20%: Red (Very Low)
                (-54, -18, '#fd7e14'),  # 20-40%: Orange (Low)
                (-18, 18, '#ffc107'),   # 40-60%: Yellow (Moderate)
                (18, 54, '#20c997'),    # 60-80%: Teal (Good)
                (54, 90, '#28a745')     # 80-100%: Green (Excellent)
            ]
            
            for start, end, color in segments:
                segment = patches.Wedge((0.5, 0), 0.4, start, end,
                                        facecolor=color, alpha=0.7)
                ax.add_patch(segment)
            
            # Add needle
            needle_x = 0.5 + 0.35 * np.cos(np.radians(theta))
            needle_y = 0.35 * np.sin(np.radians(theta))
            ax.plot([0.5, needle_x], [0, needle_y], 'k-', linewidth=3)
            ax.plot(0.5, 0, 'ko', markersize=8)
            
            # Add labels
            ax.text(0.5, -0.15, f'{confidence_score:.1f}%',
                    ha='center', va='center', fontsize=16, fontweight='bold')
            ax.text(0.5, -0.25, 'Overall Confidence',
                    ha='center', va='center', fontsize=12)
            # Add scale labels
            ax.text(0.1, 0.2, '0%', ha='center', va='center', fontsize=10)
            ax.text(0.5, 0.45, '50%', ha='center', va='center', fontsize=10)
            ax.text(0.9, 0.2, '100%', ha='center', va='center', fontsize=10)
            
            ax.set_xlim(0, 1)
            ax.set_ylim(-0.3, 0.5)
            ax.set_aspect('equal')
            ax.axis('off')
            
            # Save to bytes
            buf = BytesIO()
            plt.savefig(buf, format='png', dpi=150, bbox_inches='tight')
            buf.seek(0)
            plt.close()
            
            # Create reportlab Image
            img = Image(buf, width=6*inch, height=3*inch)
            return img
            
        except (ImportError, IOError, ValueError) as e:
            print(f"Could not create confidence gauge: {e}")
            # Return a simple text representation
            gauge_text = f"Overall Confidence Score: {confidence_score:.1f}%"
            return Paragraph(gauge_text, getSampleStyleSheet()['Normal'])

    def run_full_validation(self, key_columns=None, key_metrics=None,
                            report_path='data_quality_report.txt',
                            generate_pdf=True):
        """Run all validation checks and generate confidence report."""
        self.load_data()
        
        # Run all assessments
        completeness = self.assess_data_completeness()
        self.report['confidence_scores']['completeness'] = completeness
        
        consistency = self.assess_internal_consistency()
        self.report['confidence_scores']['consistency'] = consistency
        
        agreement = self.assess_source_agreement(key_columns=key_columns)
        self.report['confidence_scores']['agreement'] = agreement
        
        plausibility = self.assess_historical_plausibility(
            key_metric_columns=key_metrics)
        if plausibility:
            self.report['confidence_scores']['plausibility'] = plausibility
        
        # Calculate final confidence scores
        self.calculate_confidence_scores()
        
        # Generate report
        return self.generate_report(output_path=report_path,
                                    generate_pdf=generate_pdf)


# Example usage
if __name__ == "__main__":
    SOURCE1_PATH = "source1_data.xlsx"
    SOURCE2_PATH = "source2_data.xlsx"
    
    HISTORICAL_DATA = [
        {'path': 'report_2023.xlsx', 'year': 2023, 'source': 'A'},
        {'path': 'report_2022.xlsx', 'year': 2022, 'source': 'B'},
        {'path': 'report_2021.xlsx', 'year': 2021, 'source': 'A'},
    ]
    
    KEY_COLUMNS = None  # e.g., ['employee_id']
    KEY_METRICS = None  # e.g., ['total_employees', 'total_enrollment']
    
    validator = DataQualityValidator(
        source1_path=SOURCE1_PATH,
        source2_path=SOURCE2_PATH,
        historical_data_config=HISTORICAL_DATA
    )
    
    report = validator.run_full_validation(
        key_columns=KEY_COLUMNS,
        key_metrics=KEY_METRICS,
        report_path='data_confidence_report.txt',
        generate_pdf=True  # This will also create a PDF version
    )

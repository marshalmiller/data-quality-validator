import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import warnings
warnings.filterwarnings('ignore')


class DataQualityValidator:
    """
    Validates data quality and assesses reliability for college workforce and student body data.
    Focuses on data confidence scoring rather than source comparison.
    """
    
    def __init__(self, source1_path, source2_path, historical_data_config=None):
        """
        Initialize the validator with two data sources and optional historical data.
        
        Args:
            source1_path: Path to first Excel file
            source2_path: Path to second Excel file
            historical_data_config: List of dicts with historical data information
                Format: [
                    {'path': 'report_2023.xlsx', 'year': 2023, 'source': 'A'},
                    {'path': 'report_2022.xlsx', 'year': 2022, 'source': 'B'},
                ]
        """
        self.source1_path = source1_path
        self.source2_path = source2_path
        self.historical_data_config = historical_data_config or []
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
        if self.historical_data_config and isinstance(self.historical_data_config[0], str):
            self.historical_data_config = [{'path': p} for p in self.historical_data_config]
        
        for config in self.historical_data_config:
            try:
                hist_path = config['path']
                df = pd.read_excel(hist_path)
                
                year = config.get('year') or self._extract_year_from_path(hist_path)
                source = config.get('source') or self._extract_source_from_path(hist_path)
                
                self.historical_data.append({
                    'path': hist_path,
                    'year': year,
                    'source': source,
                    'data': df
                })
                
                source_label = f" [Source: {source}]" if source else ""
                year_label = f" ({year})" if year else ""
                print(f"  ✓ Loaded: {Path(hist_path).name}{year_label}{source_label}")
                
            except Exception as e:
                print(f"  ✗ Warning: Could not load {config.get('path')}: {str(e)}")
        
        self.historical_data.sort(
            key=lambda x: (x['year'] if x['year'] else 0, x['source'] if x['source'] else ''), 
            reverse=True
        )
        
        print(f"\nSource 1 loaded: {self.source1.shape[0]} rows, {self.source1.shape[1]} columns")
        print(f"Source 2 loaded: {self.source2.shape[0]} rows, {self.source2.shape[1]} columns")
        print(f"Historical files loaded: {len(self.historical_data)}")
        
        self._summarize_historical_coverage()
        
    def _extract_year_from_path(self, path):
        """Try to extract year from file path."""
        import re
        path_str = str(path)
        match = re.search(r'(20\d{2})', path_str)
        if match:
            return int(match.group(1))
        return None
    
    def _extract_source_from_path(self, path):
        """Try to extract source identifier from file path."""
        import re
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
            source_label = hist['source'] if hist['source'] else 'Unknown Source'
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
        source1_completeness = ((source1_total_cells - source1_missing_cells) / source1_total_cells) * 100
        
        print(f"\nSource 1 Completeness: {source1_completeness:.2f}%")
        completeness_scores['source1'] = source1_completeness
        
        if source1_missing.sum() == 0:
            print("✓ No missing data in Source 1")
        else:
            critical_missing = []
            for col in source1_missing[source1_missing > 0].index:
                pct = (source1_missing[col] / len(self.source1)) * 100
                if pct > 50:
                    issue = f"Source 1 - Critical: '{col}' is {pct:.1f}% missing (may compromise data reliability)"
                    print(f"🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    critical_missing.append(col)
                elif pct > 10:
                    concern = f"Source 1 - '{col}' has {pct:.1f}% missing values"
                    print(f"⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
        
        # Source 2 completeness
        source2_missing = self.source2.isnull().sum()
        source2_total_cells = len(self.source2) * len(self.source2.columns)
        source2_missing_cells = source2_missing.sum()
        source2_completeness = ((source2_total_cells - source2_missing_cells) / source2_total_cells) * 100
        
        print(f"\nSource 2 Completeness: {source2_completeness:.2f}%")
        completeness_scores['source2'] = source2_completeness
        
        if source2_missing.sum() == 0:
            print("✓ No missing data in Source 2")
        else:
            critical_missing = []
            for col in source2_missing[source2_missing > 0].index:
                pct = (source2_missing[col] / len(self.source2)) * 100
                if pct > 50:
                    issue = f"Source 2 - Critical: '{col}' is {pct:.1f}% missing (may compromise data reliability)"
                    print(f"🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    critical_missing.append(col)
                elif pct > 10:
                    concern = f"Source 2 - '{col}' has {pct:.1f}% missing values"
                    print(f"⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
        
        return completeness_scores
    
    def assess_internal_consistency(self, key_columns=None):
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
                issue = f"Source 1 contains {duplicates1} duplicate rows ({pct:.1f}%) - indicates data quality problems"
                print(f"🚩 {issue}")
                self.report['red_flags'].append(issue)
            else:
                concern = f"Source 1 contains {duplicates1} duplicate rows ({pct:.1f}%)"
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
        else:
            print("✓ No duplicates in Source 1")
            consistency_scores['source1'] += 1
        
        if duplicates2 > 0:
            pct = (duplicates2 / len(self.source2)) * 100
            if pct > 5:
                issue = f"Source 2 contains {duplicates2} duplicate rows ({pct:.1f}%) - indicates data quality problems"
                print(f"🚩 {issue}")
                self.report['red_flags'].append(issue)
            else:
                concern = f"Source 2 contains {duplicates2} duplicate rows ({pct:.1f}%)"
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
                outliers1 = ((self.source1[col] < (q1 - 3 * iqr)) | (self.source1[col] > (q3 + 3 * iqr))).sum()
                
                if outliers1 > 0:
                    pct = (outliers1 / len(self.source1)) * 100
                    if pct > 5:
                        concern = f"Source 1 - '{col}' has {outliers1} extreme outliers ({pct:.1f}%)"
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
                outliers2 = ((self.source2[col] < (q1 - 3 * iqr)) | (self.source2[col] > (q3 + 3 * iqr))).sum()
                
                if outliers2 > 0:
                    pct = (outliers2 / len(self.source2)) * 100
                    if pct > 5:
                        concern = f"Source 2 - '{col}' has {outliers2} extreme outliers ({pct:.1f}%)"
                        print(f"⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                else:
                    consistency_scores['source2'] += 1
        
        # Normalize scores
        if max_points > 0:
            consistency_scores['source1'] = (consistency_scores['source1'] / max_points) * 100
            consistency_scores['source2'] = (consistency_scores['source2'] / max_points) * 100
        
        return consistency_scores
    
    def assess_source_agreement(self, key_columns=None):
        """Assess how much the two sources agree (without declaring one correct)."""
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
                concern = f"Schema mismatch: {len(missing_in_source2)} columns in Source 1 not in Source 2"
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
            
            if missing_in_source1:
                concern = f"Schema mismatch: {len(missing_in_source1)} columns in Source 2 not in Source 1"
                print(f"⚠️  {concern}")
                self.report['reliability_concerns'].append(concern)
        
        print(f"Schema agreement: {schema_agreement:.1f}%")
        
        # Record count agreement
        count_diff_pct = abs(len(self.source1) - len(self.source2)) / max(len(self.source1), len(self.source2)) * 100
        
        if count_diff_pct < 1:
            print(f"✓ Record counts are very close (difference: {count_diff_pct:.2f}%)")
            count_agreement = 100
        elif count_diff_pct < 5:
            print(f"⚠️  Record counts differ by {count_diff_pct:.1f}%")
            count_agreement = 95
            concern = f"Record count discrepancy: {count_diff_pct:.1f}% difference between sources"
            self.report['reliability_concerns'].append(concern)
        else:
            print(f"🚩 Significant record count difference: {count_diff_pct:.1f}%")
            count_agreement = max(0, 100 - count_diff_pct)
            issue = f"Major record count discrepancy: Source 1 has {len(self.source1)} records, Source 2 has {len(self.source2)} ({count_diff_pct:.1f}% difference)"
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
                                
                                if col1 in merged.columns and col2 in merged.columns:
                                    # Count matching values
                                    matches = (merged[col1] == merged[col2]) | (merged[col1].isna() & merged[col2].isna())
                                    total_comparisons += len(merged)
                                    matching_values += matches.sum()
                                    
                                    match_pct = (matches.sum() / len(merged)) * 100
                                    if match_pct < 90:
                                        disagreement_cols.append((col, match_pct))
                        
                        if total_comparisons > 0:
                            value_agreement = (matching_values / total_comparisons) * 100
                            print(f"\nValue agreement across common records: {value_agreement:.1f}%")
                            
                            if value_agreement < 70:
                                issue = f"Low agreement between sources: only {value_agreement:.1f}% of values match"
                                print(f"🚩 {issue}")
                                self.report['red_flags'].append(issue)
                            elif value_agreement < 90:
                                concern = f"Moderate disagreement between sources: {value_agreement:.1f}% agreement"
                                print(f"⚠️  {concern}")
                                self.report['reliability_concerns'].append(concern)
                            else:
                                print("✓ Sources show high agreement on common records")
                            
                            # Report specific columns with disagreement
                            if disagreement_cols:
                                print("\nColumns with notable disagreement:")
                                for col, match_pct in sorted(disagreement_cols, key=lambda x: x[1]):
                                    if match_pct < 80:
                                        concern = f"  '{col}': only {match_pct:.1f}% agreement"
                                        print(f"  🚩 {concern}")
                                        self.report['reliability_concerns'].append(f"Low agreement on '{col}' ({match_pct:.1f}%)")
            
            except Exception as e:
                print(f"Could not assess value agreement: {str(e)}")
        
        # Calculate overall agreement score
        weights = {'schema': 0.2, 'count': 0.3, 'values': 0.5}
        
        agreement_score = schema_agreement * weights['schema'] + count_agreement * weights['count']
        
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
        """Assess whether current data is plausible given historical patterns."""
        if not self.historical_data:
            print("\n⚠️  No historical data provided. Cannot assess historical plausibility.")
            return None
        
        print("\n=== Assessing Historical Plausibility ===")
        
        if key_metric_columns is None:
            key_metric_columns = self.source1.select_dtypes(include=[np.number]).columns.tolist()
            key_metric_columns = [col for col in key_metric_columns 
                                 if any(keyword in col.lower() 
                                       for keyword in ['total', 'count', 'number', 'enrollment', 
                                                      'headcount', 'employee', 'student', 'staff', 'fte'])]
        
        if not key_metric_columns:
            key_metric_columns = self.source1.select_dtypes(include=[np.number]).columns.tolist()[:5]
        
        plausibility_scores = {'source1': [], 'source2': []}
        
        print(f"\nAnalyzing {len(key_metric_columns)} key metrics against historical patterns...")
        
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
                print(f"   Historical range: {hist_min:,.0f} to {hist_max:,.0f}")
                print(f"   Historical mean: {hist_mean:,.0f} (±{hist_std:,.0f})")
                
                # Check Source 1
                if col in self.source1.columns:
                    current_val1 = self.source1[col].sum()
                    z_score1 = abs(current_val1 - hist_mean) / hist_std if hist_std > 0 else 0
                    
                    print(f"   Source 1: {current_val1:,.0f} (z-score: {z_score1:.2f})")
                    
                    if current_val1 < reasonable_min or current_val1 > reasonable_max:
                        issue = f"Source 1 '{col}' ({current_val1:,.0f}) is outside reasonable historical range"
                        print(f"   🚩 {issue}")
                        self.report['red_flags'].append(issue)
                        plausibility_scores['source1'].append(max(0, 100 - (z_score1 * 20)))
                    elif z_score1 > 2:
                        concern = f"Source 1 '{col}' shows unusual deviation from historical pattern"
                        print(f"   ⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                        plausibility_scores['source1'].append(max(70, 100 - (z_score1 * 10)))
                    else:
                        print(f"   ✓ Within expected range")
                        plausibility_scores['source1'].append(100)
                
                # Check Source 2
                if col in self.source2.columns:
                    current_val2 = self.source2[col].sum()
                    z_score2 = abs(current_val2 - hist_mean) / hist_std if hist_std > 0 else 0
                    
                    print(f"   Source 2: {current_val2:,.0f} (z-score: {z_score2:.2f})")
                    
                    if current_val2 < reasonable_min or current_val2 > reasonable_max:
                        issue = f"Source 2 '{col}' ({current_val2:,.0f}) is outside reasonable historical range"
                        print(f"   🚩 {issue}")
                        self.report['red_flags'].append(issue)
                        plausibility_scores['source2'].append(max(0, 100 - (z_score2 * 20)))
                    elif z_score2 > 2:
                        concern = f"Source 2 '{col}' shows unusual deviation from historical pattern"
                        print(f"   ⚠️  {concern}")
                        self.report['reliability_concerns'].append(concern)
                        plausibility_scores['source2'].append(max(70, 100 - (z_score2 * 10)))
                    else:
                        print(f"   ✓ Within expected range")
                        plausibility_scores['source2'].append(100)
        
        # Check record counts
        print("\n📈 Record Count Plausibility:")
        historical_counts = [len(h['data']) for h in self.historical_data]
        
        if len(historical_counts) >= 3:
            count_mean = np.mean(historical_counts)
            count_std = np.std(historical_counts)
            
            print(f"   Historical average: {count_mean:.0f} records (±{count_std:.0f})")
            print(f"   Source 1: {len(self.source1)} records")
            print(f"   Source 2: {len(self.source2)} records")
            
            if count_std > 0:
                z1 = abs(len(self.source1) - count_mean) / count_std
                z2 = abs(len(self.source2) - count_mean) / count_std
                
                if z1 > 2.5:
                    issue = f"Source 1 record count ({len(self.source1)}) is highly unusual compared to history"
                    print(f"   🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    plausibility_scores['source1'].append(max(0, 100 - (z1 * 20)))
                elif z1 > 2:
                    concern = f"Source 1 record count is somewhat unusual (z={z1:.2f})"
                    print(f"   ⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
                    plausibility_scores['source1'].append(max(70, 100 - (z1 * 10)))
                else:
                    print(f"   ✓ Source 1 within expected range (z={z1:.2f})")
                    plausibility_scores['source1'].append(100)
                
                if z2 > 2.5:
                    issue = f"Source 2 record count ({len(self.source2)}) is highly unusual compared to history"
                    print(f"   🚩 {issue}")
                    self.report['red_flags'].append(issue)
                    plausibility_scores['source2'].append(max(0, 100 - (z2 * 20)))
                elif z2 > 2:
                    concern = f"Source 2 record count is somewhat unusual (z={z2:.2f})"
                    print(f"   ⚠️  {concern}")
                    self.report['reliability_concerns'].append(concern)
                    plausibility_scores['source2'].append(max(70, 100 - (z2 * 10)))
                else:
                    print(f"   ✓ Source 2 within expected range (z={z2:.2f})")
                    plausibility_scores['source2'].append(100)
        
        # Calculate average plausibility scores
        avg_plausibility = {}
        if plausibility_scores['source1']:
            avg_plausibility['source1'] = np.mean(plausibility_scores['source1'])
        if plausibility_scores['source2']:
            avg_plausibility['source2'] = np.mean(plausibility_scores['source2'])
        
        return avg_plausibility
    
    def calculate_confidence_scores(self):
        """Calculate overall confidence scores for each source and the data in general."""
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
            
            if 'completeness' in scores and source_key in scores['completeness']:
                source_scores.append(scores['completeness'][source_key] * weights['completeness'])
            
            if 'consistency' in scores and source_key in scores['consistency']:
                source_scores.append(scores['consistency'][source_key] * weights['consistency'])
            
            if 'plausibility' in scores and source_key in scores['plausibility']:
                source_scores.append(scores['plausibility'][source_key] * weights['plausibility'])
            
            if source_scores:
                # Agreement score applies to both
                if 'agreement' in scores and 'overall' in scores['agreement']:
                    source_scores.append(scores['agreement']['overall'] * weights['agreement'])
                
                scores[f'{source_key}_overall'] = sum(source_scores) / len(source_scores) * (len(source_scores) / len(weights))
        
        # Calculate overall data confidence (not source-specific)
        all_factors = []
        
        if 'completeness' in scores:
            avg_completeness = np.mean([scores['completeness'].get('source1', 0), 
                                       scores['completeness'].get('source2', 0)])
            all_factors.append(avg_completeness * weights['completeness'])
        
        if 'consistency' in scores:
            avg_consistency = np.mean([scores['consistency'].get('source1', 0), 
                                      scores['consistency'].get('source2', 0)])
            all_factors.append(avg_consistency * weights['consistency'])
        
        if 'agreement' in scores and 'overall' in scores['agreement']:
            all_factors.append(scores['agreement']['overall'] * weights['agreement'])
        
        if 'plausibility' in scores:
            avg_plausibility = np.mean([scores['plausibility'].get('source1', 0), 
                                       scores['plausibility'].get('source2', 0)])
            all_factors.append(avg_plausibility * weights['plausibility'])
        
        if all_factors:
            scores['overall_data_confidence'] = sum(all_factors) / len(all_factors) * (len(all_factors) / len(weights))
        
        # Interpret scores
        print("\n📊 CONFIDENCE SCORES")
        print("-" * 70)
        
        # Overall data confidence
        if 'overall_data_confidence' in scores:
            overall = scores['overall_data_confidence']
            print(f"\nOverall Data Confidence: {overall:.1f}%")
            
            if overall >= 90:
                level = "VERY HIGH"
                interpretation = "The data appears highly reliable across all dimensions."
            elif overall >= 75:
                level = "HIGH"
                interpretation = "The data appears generally reliable with minor concerns."
            elif overall >= 60:
                level = "MODERATE"
                interpretation = "The data has some reliability concerns that should be investigated."
            elif overall >= 40:
                level = "LOW"
                interpretation = "The data has significant reliability issues. Use with caution."
            else:
                level = "VERY LOW"
                interpretation = "The data reliability is questionable. Thorough review required."
            
            print(f"Confidence Level: {level}")
            print(f"Interpretation: {interpretation}")
        
        # Individual source scores
        print(f"\nSource 1 Confidence: {scores.get('source1_overall', 0):.1f}%")
        print(f"Source 2 Confidence: {scores.get('source2_overall', 0):.1f}%")
        
        # Component breakdown
        print("\n📋 Component Scores:")
        if 'completeness' in scores:
            print(f"  Completeness:")
            print(f"    - Source 1: {scores['completeness'].get('source1', 0):.1f}%")
            print(f"    - Source 2: {scores['completeness'].get('source2', 0):.1f}%")
        
        if 'consistency' in scores:
            print(f"  Internal Consistency:")
            print(f"    - Source 1: {scores['consistency'].get('source1', 0):.1f}%")
            print(f"    - Source 2: {scores['consistency'].get('source2', 0):.1f}%")
        
        if 'agreement' in scores and 'overall' in scores['agreement']:
            print(f"  Cross-Source Agreement: {scores['agreement']['overall']:.1f}%")
        
        if 'plausibility' in scores:
            print(f"  Historical Plausibility:")
            print(f"    - Source 1: {scores['plausibility'].get('source1', 0):.1f}%")
            print(f"    - Source 2: {scores['plausibility'].get('source2', 0):.1f}%")
        
        print("\n" + "="*70)
    
    def generate_report(self, output_path='data_quality_report.txt'):
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
        with open(output_path, 'w') as f:
            f.write("="*70 + "\n")
            f.write("DATA QUALITY & RELIABILITY ASSESSMENT\n")
            f.write("College Workforce and Student Body Data\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Generated: {self.report['summary']['generated_at']}\n")
            f.write(f"Source 1: {self.source1_path}\n")
            f.write(f"Source 2: {self.source2_path}\n")
            
            if self.historical_data:
                f.write(f"\nHistorical Data Files: {len(self.historical_data)}\n")
                years = {}
                for hist in self.historical_data:
                    year = hist['year'] or 'Unknown'
                    if year not in years:
                        years[year] = []
                    source_label = hist['source'] if hist['source'] else 'Unknown'
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
                    interpretation = "The data appears highly reliable across all dimensions.\nYou can use this data with high confidence for decision-making."
                elif overall >= 75:
                    level = "HIGH ✓✓"
                    interpretation = "The data appears generally reliable with minor concerns.\nSuitable for most reporting and analysis purposes."
                elif overall >= 60:
                    level = "MODERATE ⚠"
                    interpretation = "The data has some reliability concerns that should be investigated.\nReview specific issues before using for critical decisions."
                elif overall >= 40:
                    level = "LOW ⚠⚠"
                    interpretation = "The data has significant reliability issues.\nUse with caution and verify key figures independently."
                else:
                    level = "VERY LOW 🚩"
                    interpretation = "The data reliability is questionable.\nThorough review and validation required before use."
                
                f.write(f"Confidence Level: {level}\n\n")
                f.write(f"{interpretation}\n\n")
            
            f.write("Individual Source Confidence:\n")
            f.write(f"  Source 1: {scores.get('source1_overall', 0):.1f}%\n")
            f.write(f"  Source 2: {scores.get('source2_overall', 0):.1f}%\n\n")
            
            f.write("Component Breakdown:\n")
            if 'completeness' in scores:
                f.write(f"  Data Completeness:\n")
                f.write(f"    - Source 1: {scores['completeness'].get('source1', 0):.1f}%\n")
                f.write(f"    - Source 2: {scores['completeness'].get('source2', 0):.1f}%\n")
            
            if 'consistency' in scores:
                f.write(f"  Internal Consistency:\n")
                f.write(f"    - Source 1: {scores['consistency'].get('source1', 0):.1f}%\n")
                f.write(f"    - Source 2: {scores['consistency'].get('source2', 0):.1f}%\n")
            
            if 'agreement' in scores:
                f.write(f"  Cross-Source Agreement: {scores['agreement'].get('overall', 0):.1f}%\n")
            
            if 'plausibility' in scores:
                f.write(f"  Historical Plausibility:\n")
                f.write(f"    - Source 1: {scores['plausibility'].get('source1', 0):.1f}%\n")
                f.write(f"    - Source 2: {scores['plausibility'].get('source2', 0):.1f}%\n")
            
            f.write("\n")
            f.write("="*70 + "\n")
            f.write("DETAILED FINDINGS\n")
            f.write("="*70 + "\n\n")
            
            f.write(f"Total Concerns: {total_concerns}\n")
            f.write(f"  - Critical Issues (Red Flags): {self.report['summary']['red_flags']}\n")
            f.write(f"  - Moderate Concerns: {self.report['summary']['reliability_concerns']}\n\n")
            
            if self.report['red_flags']:
                f.write("🚩 CRITICAL ISSUES (RED FLAGS)\n")
                f.write("-"*70 + "\n")
                f.write("These are serious data quality problems that significantly impact reliability:\n\n")
                for issue in self.report['red_flags']:
                    f.write(f"  • {issue}\n")
                f.write("\n")
            
            if self.report['reliability_concerns']:
                f.write("⚠️  RELIABILITY CONCERNS\n")
                f.write("-"*70 + "\n")
                f.write("These issues may affect data reliability and should be reviewed:\n\n")
                for concern in self.report['reliability_concerns']:
                    f.write(f"  • {concern}\n")
                f.write("\n")
            
            if not self.report['red_flags'] and not self.report['reliability_concerns']:
                f.write("✓ NO SIGNIFICANT ISSUES FOUND\n")
                f.write("-"*70 + "\n")
                f.write("The data passed all quality checks without major concerns.\n\n")
            
            f.write("="*70 + "\n")
            f.write("RECOMMENDATIONS\n")
            f.write("="*70 + "\n\n")
            
            if 'overall_data_confidence' in scores:
                overall = scores['overall_data_confidence']
                
                if overall >= 75:
                    f.write("✓ DATA IS SUITABLE FOR USE\n\n")
                    f.write("The data shows good reliability across multiple dimensions.\n")
                    f.write("You can proceed with using this data for reporting and analysis.\n\n")
                    
                    if self.report['reliability_concerns']:
                        f.write("Minor recommendations:\n")
                        f.write("  - Review the concerns listed above for awareness\n")
                        f.write("  - Document any known data collection changes\n")
                        f.write("  - Consider monitoring these metrics in future reports\n")
                
                elif overall >= 60:
                    f.write("⚠️  USE DATA WITH CAUTION\n\n")
                    f.write("The data has moderate reliability concerns.\n\n")
                    f.write("Recommended actions:\n")
                    f.write("  1. Investigate the specific issues flagged above\n")
                    f.write("  2. Verify key metrics independently before publication\n")
                    f.write("  3. Consider adding data quality notes to reports\n")
                    f.write("  4. Establish data validation processes for future submissions\n")
                
                else:
                    f.write("🚩 DATA REQUIRES SIGNIFICANT REVIEW\n\n")
                    f.write("The data has substantial reliability issues.\n\n")
                    f.write("Critical actions required:\n")
                    f.write("  1. DO NOT use this data for official reporting without validation\n")
                    f.write("  2. Investigate all red flags immediately\n")
                    f.write("  3. Verify data collection and extraction processes\n")
                    f.write("  4. Consider re-pulling data from source systems\n")
                    f.write("  5. Document all discrepancies and their resolutions\n")
            
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
            f.write("  - Checks for duplicates, outliers, and logical errors\n")
            f.write("  - Higher = cleaner, more reliable data\n\n")
            
            f.write("Cross-Source Agreement (25% of overall score):\n")
            f.write("  - Measures how well the two sources align\n")
            f.write("  - Higher = sources tell a consistent story\n")
            f.write("  - Lower = sources disagree (investigate why)\n\n")
            
            f.write("Historical Plausibility (25% of overall score):\n")
            f.write("  - Measures if current data makes sense given past patterns\n")
            f.write("  - Higher = data follows expected trends\n")
            f.write("  - Lower = unusual patterns (may indicate errors or real changes)\n\n")
            
            f.write("Note: Low cross-source agreement doesn't mean the data is wrong,\n")
            f.write("it means the sources disagree and you need to investigate why.\n")
            f.write("Both sources could be correct but measuring different things,\n")
            f.write("or one (or both) could have data quality issues.\n\n")
            
            f.write("="*70 + "\n")
            f.write("END OF REPORT\n")
            f.write("="*70 + "\n")
        
        print(f"\n✓ Report saved to: {output_path}")
        print(f"\nSummary: {total_concerns} total concerns identified")
        
        if 'overall_data_confidence' in self.report['confidence_scores']:
            print(f"Overall Data Confidence: {self.report['confidence_scores']['overall_data_confidence']:.1f}%")
        
        return self.report
    
    def run_full_validation(self, key_columns=None, key_metrics=None, report_path='data_quality_report.txt'):
        """Run all validation checks and generate confidence report."""
        self.load_data()
        
        # Run all assessments
        completeness = self.assess_data_completeness()
        self.report['confidence_scores']['completeness'] = completeness
        
        consistency = self.assess_internal_consistency(key_columns=key_columns)
        self.report['confidence_scores']['consistency'] = consistency
        
        agreement = self.assess_source_agreement(key_columns=key_columns)
        self.report['confidence_scores']['agreement'] = agreement
        
        plausibility = self.assess_historical_plausibility(key_metric_columns=key_metrics)
        if plausibility:
            self.report['confidence_scores']['plausibility'] = plausibility
        
        # Calculate final confidence scores
        self.calculate_confidence_scores()
        
        # Generate report
        return self.generate_report(output_path=report_path)


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
        report_path='data_confidence_report.txt'
    )

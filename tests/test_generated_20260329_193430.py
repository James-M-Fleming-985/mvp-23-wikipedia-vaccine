import pytest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from layer_mvp_0023 import (
    WikipediaVaccineSearchAPI,
    GrangerCausalityAnalyzer,
    MarketOpportunityDashboard,
    VaccineTrendDataPipeline,
    VaccineTrendAnalysisIntegrator,
    HealthTechAnalysisWorkflow
)


class TestWikipediaVaccineSearchAPI:
    """Unit tests for Wikipedia vaccine search trend data API endpoint."""
    
    def test_api_returns_vaccine_search_trend_data_with_growth_metric(self):
        """Test that API endpoint returns Wikipedia vaccine search trend data with 48.7% growth metric."""
        api = WikipediaVaccineSearchAPI()
        
        result = api.get_vaccine_search_trends()
        
        assert result is not None
        assert 'growth_metric' in result
        assert result['growth_metric'] == 48.7
        assert 'search_trends' in result
        assert isinstance(result['search_trends'], list)
        assert len(result['search_trends']) > 0


class TestGrangerCausalityAnalyzer:
    """Unit tests for Granger causality correlation calculation between vaccine searches and COVID-19 clinical trials."""
    
    def test_calculates_granger_causality_correlation(self):
        """Test that system calculates Granger causality correlation between vaccine searches and COVID-19 clinical trials."""
        analyzer = GrangerCausalityAnalyzer()
        
        vaccine_search_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=24, freq='M'),
            'search_volume': np.random.randint(100, 1000, 24)
        })
        
        clinical_trial_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=24, freq='M'),
            'trial_count': np.random.randint(10, 100, 24)
        })
        
        result = analyzer.calculate_granger_causality(vaccine_search_data, clinical_trial_data)
        
        assert result is not None
        assert 'correlation_coefficient' in result
        assert isinstance(result['correlation_coefficient'], float)
        assert 'p_value' in result
        assert 'causality_direction' in result


class TestMarketOpportunityDashboard:
    """Unit tests for market opportunity dashboard displaying search volume metrics and revenue potential."""
    
    def test_dashboard_displays_search_volume_metrics_and_revenue_potential(self):
        """Test that market opportunity dashboard displays search volume metrics and revenue potential."""
        dashboard = MarketOpportunityDashboard()
        
        search_data = {
            'total_search_volume': 150000,
            'growth_rate': 48.7,
            'trending_keywords': ['covid vaccine', 'mrna vaccine', 'vaccine efficacy']
        }
        
        result = dashboard.generate_market_metrics(search_data)
        
        assert result is not None
        assert 'search_volume_metrics' in result
        assert result['search_volume_metrics']['total_volume'] == 150000
        assert result['search_volume_metrics']['growth_rate'] == 48.7
        assert 'revenue_potential' in result
        assert isinstance(result['revenue_potential'], dict)
        assert 'market_size_estimate' in result['revenue_potential']
        assert result['revenue_potential']['market_size_estimate'] > 0


class TestVaccineTrendDataPipeline:
    """Unit tests for data pipeline processing and storing trend correlation results with 2-month lag analysis."""
    
    def test_processes_and_stores_trend_correlation_with_lag_analysis(self):
        """Test that data pipeline processes and stores trend correlation results with 2-month lag analysis."""
        pipeline = VaccineTrendDataPipeline()
        
        trend_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=24, freq='M'),
            'search_volume': np.random.randint(100, 1000, 24),
            'clinical_trials': np.random.randint(10, 100, 24)
        })
        
        result = pipeline.process_trend_correlation(trend_data, lag_months=2)
        
        assert result is not None
        assert 'correlation_results' in result
        assert 'lag_analysis' in result
        assert result['lag_analysis']['lag_period'] == 2
        assert 'stored_results_id' in result
        assert isinstance(result['stored_results_id'], str)


class TestIntegrationVaccineTrendAnalysis:
    """Testing integration between Wikipedia data fetcher, statistical analysis engine, and market opportunity calculator."""
    
    def test_wikipedia_data_to_granger_analysis_pipeline(self):
        """Test integration from Wikipedia data fetching to Granger causality analysis."""
        api = WikipediaVaccineSearchAPI()
        analyzer = GrangerCausalityAnalyzer()
        integrator = VaccineTrendAnalysisIntegrator()
        
        wikipedia_data = api.get_vaccine_search_trends()
        processed_data = integrator.prepare_data_for_analysis(wikipedia_data)
        
        mock_clinical_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=24, freq='M'),
            'trial_count': np.random.randint(10, 100, 24)
        })
        
        granger_result = analyzer.calculate_granger_causality(processed_data, mock_clinical_data)
        
        assert processed_data is not None
        assert isinstance(processed_data, pd.DataFrame)
        assert granger_result is not None
        assert 'correlation_coefficient' in granger_result
        
    def test_correlation_results_to_market_metrics_integration(self):
        """Test integration from correlation results to market opportunity metrics."""
        analyzer = GrangerCausalityAnalyzer()
        dashboard = MarketOpportunityDashboard()
        integrator = VaccineTrendAnalysisIntegrator()
        
        mock_search_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=24, freq='M'),
            'search_volume': np.random.randint(100, 1000, 24)
        })
        
        mock_clinical_data = pd.DataFrame({
            'date': pd.date_range('2020-01-01', periods=24, freq='M'),
            'trial_count': np.random.randint(10, 100, 24)
        })
        
        correlation_result = analyzer.calculate_granger_causality(mock_search_data, mock_clinical_data)
        market_data = integrator.prepare_market_data(mock_search_data, correlation_result)
        market_metrics = dashboard.generate_market_metrics(market_data)
        
        assert correlation_result is not None
        assert market_data is not None
        assert market_metrics is not None
        assert 'revenue_potential' in market_metrics
        assert 'search_volume_metrics' in market_metrics


class TestE2EHealthTechAnalysis:
    """End-to-end testing of user requesting vaccine trend analysis and receiving complete market opportunity report."""
    
    def test_full_vaccine_trend_to_market_opportunity_workflow(self):
        """Test complete workflow from vaccine trend request to market opportunity report delivery."""
        workflow = HealthTechAnalysisWorkflow()
        
        user_request = {
            'analysis_type': 'vaccine_trend_analysis',
            'time_period': '2020-01-01 to 2022-12-31',
            'include_market_opportunity': True,
            'lag_analysis_months': 2
        }
        
        final_report = workflow.execute_full_analysis(user_request)
        
        assert final_report is not None
        assert 'wikipedia_trend_data' in final_report
        assert final_report['wikipedia_trend_data']['growth_metric'] == 48.7
        
        assert 'granger_causality_analysis' in final_report
        assert 'correlation_coefficient' in final_report['granger_causality_analysis']
        
        assert 'market_opportunity_report' in final_report
        assert 'revenue_potential' in final_report['market_opportunity_report']
        assert 'search_volume_metrics' in final_report['market_opportunity_report']
        
        assert 'data_pipeline_results' in final_report
        assert 'lag_analysis' in final_report['data_pipeline_results']
        assert final_report['data_pipeline_results']['lag_analysis']['lag_period'] == 2
        
        assert 'report_generated_at' in final_report
        assert isinstance(final_report['report_generated_at'], datetime)

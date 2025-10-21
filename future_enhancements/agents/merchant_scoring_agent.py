"""
Merchant Scoring Agent for MerchantIQ system.
Uses XGBoost for ML-based merchant value prediction and scoring.
"""

import asyncio
import os
import pickle
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
import math

# ML imports with fallback handling
try:
    import numpy as np
except ImportError:
    np = None

try:
    import xgboost as xgb
except ImportError:
    xgb = None

from ..utils.logger import get_logger, performance_log, AgentActionContext


@dataclass
class MerchantScore:
    """Merchant scoring result."""
    merchant_id: str
    merchant_name: str
    
    # Core scores
    value_score: float  # Primary value prediction (0-100)
    risk_score: float   # Risk assessment (0-100, lower is better)
    growth_score: float # Growth potential (0-100)
    composite_score: float # Overall composite score (0-100)
    
    # Confidence metrics
    prediction_confidence: float  # Model confidence (0-1)
    data_quality_score: float    # Quality of input data (0-1)
    
    # Feature importance (top contributing features)
    top_features: Dict[str, float]
    
    # Recommendations
    score_tier: str  # 'premium', 'high', 'medium', 'low'
    risk_level: str  # 'low', 'medium', 'high'
    recommendations: List[str]
    
    # Metadata
    model_version: str
    scored_at: datetime


@dataclass
class ModelMetrics:
    """ML model performance metrics."""
    model_version: str
    training_date: datetime
    
    # Performance metrics
    accuracy: float
    precision: float
    recall: float
    f1_score: float
    auc_roc: float
    
    # Data metrics
    training_samples: int
    feature_count: int
    
    # Feature importance
    feature_importance: Dict[str, float]


class MerchantScoringAgent:
    """Agent responsible for ML-based merchant scoring using XGBoost."""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the Merchant Scoring Agent.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.logger = get_logger("agent.merchant_scoring")
        
        # Check for required dependencies
        if xgb is None:
            self.logger.warning("XGBoost not available - using fallback scoring")
        if np is None:
            self.logger.warning("NumPy not available - using fallback methods")
        
        # Agent state
        self.is_running = False
        self.last_scoring_time: Optional[datetime] = None
        self.scored_merchants_count = 0
        
        # ML configuration
        ml_config = self.config.get('ml', {})
        self.model_path = ml_config.get('model_path', 'models/merchant_scoring_model.pkl')
        self.model_version = ml_config.get('version', '1.0.0')
        self.retrain_threshold_days = ml_config.get('retrain_threshold_days', 7)
        self.min_samples_for_training = ml_config.get('min_samples_for_training', 100)
        
        # Scoring parameters
        scoring_config = ml_config.get('scoring', {})
        self.score_weights = scoring_config.get('weights', {
            'value': 0.4,
            'risk': 0.3,
            'growth': 0.3
        })
        self.confidence_threshold = scoring_config.get('confidence_threshold', 0.7)
        
        # XGBoost hyperparameters
        xgb_config = ml_config.get('xgboost', {})
        self.xgb_params = {
            'max_depth': xgb_config.get('max_depth', 6),
            'learning_rate': xgb_config.get('learning_rate', 0.1),
            'n_estimators': xgb_config.get('n_estimators', 100),
            'subsample': xgb_config.get('subsample', 0.8),
            'colsample_bytree': xgb_config.get('colsample_bytree', 0.8),
            'random_state': xgb_config.get('random_state', 42)
        }
        
        # Model and data
        self.model = None
        self.feature_names: List[str] = []
        self.model_metrics: Optional[ModelMetrics] = None
        self.score_cache: Dict[str, MerchantScore] = {}
        
        # Feature engineering settings
        self.feature_config = scoring_config.get('features', {})
    
    @performance_log
    async def start(self) -> None:
        """Start the merchant scoring agent."""
        with AgentActionContext("merchant_scoring", "start"):
            self.is_running = True
            self.logger.info("Merchant Scoring Agent started")
            
            # Try to load existing model
            await self._load_model()
    
    async def stop(self) -> None:
        """Stop the merchant scoring agent."""
        with AgentActionContext("merchant_scoring", "stop"):
            self.is_running = False
            self.logger.info("Merchant Scoring Agent stopped")
    
    @performance_log
    async def score_merchants(
        self, 
        merchant_features: Dict[str, Any],  # MerchantFeatures objects
        attribution_results: Optional[Dict[str, Any]] = None  # AttributionResult objects
    ) -> Dict[str, MerchantScore]:
        """Score merchants using ML model.
        
        Args:
            merchant_features: Dictionary of MerchantFeatures by merchant_id
            attribution_results: Optional attribution results for enhanced scoring
            
        Returns:
            Dictionary mapping merchant_id to MerchantScore
        """
        with AgentActionContext("merchant_scoring", "score_merchants"):
            try:
                if not merchant_features:
                    self.logger.warning("No merchant features provided for scoring")
                    return {}
                
                # Prepare feature matrix
                feature_matrix, merchant_ids = await self._prepare_feature_matrix(
                    merchant_features, attribution_results
                )
                
                if feature_matrix is None or len(feature_matrix) == 0:
                    self.logger.warning("Could not prepare feature matrix")
                    return {}
                
                # Score merchants
                scores = {}
                
                if self.model is not None and xgb is not None and np is not None:
                    # Use ML model for scoring
                    scores = await self._score_with_ml_model(feature_matrix, merchant_ids, merchant_features)
                else:
                    # Use rule-based fallback scoring
                    scores = await self._score_with_rules(merchant_features, attribution_results)
                
                # Update agent state
                self.last_scoring_time = datetime.now()
                self.scored_merchants_count = len(scores)
                self.score_cache.update(scores)
                
                self.logger.info(f"Scored {len(scores)} merchants")
                return scores
                
            except Exception as e:
                self.logger.error(f"Merchant scoring failed: {e}")
                raise
    
    async def _prepare_feature_matrix(
        self, 
        merchant_features: Dict[str, Any],
        attribution_results: Optional[Dict[str, Any]] = None
    ) -> Tuple[Optional[List[List[float]]], List[str]]:
        """Prepare feature matrix for ML model."""
        try:
            if not merchant_features:
                return None, []
            
            merchant_ids = list(merchant_features.keys())
            feature_matrix = []
            
            # Define feature extraction order
            feature_names = [
                'total_volume', 'avg_transaction_value', 'transaction_count',
                'unique_customers', 'repeat_customer_ratio', 'avg_customer_lifetime_value',
                'days_active', 'avg_transactions_per_day', 'peak_day_volume',
                'chargeback_rate', 'fraud_score', 'payment_failure_rate',
                'category_diversity', 'location_spread',
                'growth_rate_30d', 'trend_score',
                'volume_volatility', 'customer_concentration'
            ]
            
            # Add attribution features if available
            if attribution_results:
                attribution_feature_names = [
                    'markov_attribution', 'removal_effect', 'incremental_value',
                    'conversion_contribution', 'avg_position_in_journey'
                ]
                feature_names.extend(attribution_feature_names)
            
            self.feature_names = feature_names
            
            for merchant_id in merchant_ids:
                features = merchant_features[merchant_id]
                row = []
                
                # Extract base features
                for feature_name in feature_names[:18]:  # Base features
                    if hasattr(features, feature_name):
                        value = getattr(features, feature_name)
                        # Handle None/NaN values
                        if value is None or (isinstance(value, float) and math.isnan(value)):
                            value = 0.0
                        row.append(float(value))
                    else:
                        row.append(0.0)
                
                # Extract attribution features if available
                if attribution_results and merchant_id in attribution_results:
                    attribution = attribution_results[merchant_id]
                    for feature_name in feature_names[18:]:  # Attribution features
                        if hasattr(attribution, feature_name):
                            value = getattr(attribution, feature_name)
                            if value is None or (isinstance(value, float) and math.isnan(value)):
                                value = 0.0
                            row.append(float(value))
                        else:
                            row.append(0.0)
                
                feature_matrix.append(row)
            
            return feature_matrix, merchant_ids
            
        except Exception as e:
            self.logger.error(f"Failed to prepare feature matrix: {e}")
            return None, []
    
    async def _score_with_ml_model(
        self, 
        feature_matrix: List[List[float]], 
        merchant_ids: List[str],
        merchant_features: Dict[str, Any]
    ) -> Dict[str, MerchantScore]:
        """Score merchants using the XGBoost model."""
        scores = {}
        
        try:
            # Convert to numpy array if available
            if np is not None:
                X = np.array(feature_matrix)
            else:
                X = feature_matrix
            
            # Make predictions
            predictions = self.model.predict(X)
            
            # Get prediction probabilities if available
            try:
                prediction_proba = self.model.predict_proba(X)
                confidences = [max(proba) for proba in prediction_proba]
            except:
                # For regression models, use prediction variance as confidence proxy
                confidences = [0.8] * len(predictions)  # Default confidence
            
            # Get feature importance
            try:
                feature_importance = dict(zip(
                    self.feature_names,
                    self.model.feature_importances_
                ))
            except:
                feature_importance = {}
            
            # Create MerchantScore objects
            for i, merchant_id in enumerate(merchant_ids):
                features = merchant_features[merchant_id]
                merchant_name = getattr(features, 'merchant_name', 'Unknown')
                
                # Convert prediction to score (0-100)
                raw_score = float(predictions[i])
                value_score = max(0, min(100, raw_score * 100))
                
                # Compute component scores
                risk_score = self._compute_risk_score(features)
                growth_score = self._compute_growth_score(features)
                
                # Composite score
                composite_score = (
                    value_score * self.score_weights.get('value', 0.4) +
                    (100 - risk_score) * self.score_weights.get('risk', 0.3) +
                    growth_score * self.score_weights.get('growth', 0.3)
                )
                
                # Get top contributing features for this merchant
                top_features = self._get_top_features_for_merchant(
                    feature_matrix[i], feature_importance
                )
                
                # Determine tier and recommendations
                score_tier = self._determine_score_tier(composite_score)
                risk_level = self._determine_risk_level(risk_score)
                recommendations = self._generate_recommendations(
                    features, value_score, risk_score, growth_score
                )
                
                score = MerchantScore(
                    merchant_id=merchant_id,
                    merchant_name=merchant_name,
                    value_score=value_score,
                    risk_score=risk_score,
                    growth_score=growth_score,
                    composite_score=composite_score,
                    prediction_confidence=confidences[i] if i < len(confidences) else 0.8,
                    data_quality_score=self._assess_data_quality(features),
                    top_features=top_features,
                    score_tier=score_tier,
                    risk_level=risk_level,
                    recommendations=recommendations,
                    model_version=self.model_version,
                    scored_at=datetime.now()
                )
                
                scores[merchant_id] = score
            
            return scores
            
        except Exception as e:
            self.logger.error(f"ML model scoring failed: {e}")
            # Fallback to rule-based scoring
            return await self._score_with_rules(merchant_features, {})
    
    async def _score_with_rules(
        self, 
        merchant_features: Dict[str, Any],
        attribution_results: Optional[Dict[str, Any]] = None
    ) -> Dict[str, MerchantScore]:
        """Fallback rule-based scoring when ML model is not available."""
        scores = {}
        
        # Calculate percentiles for normalization
        all_volumes = [float(getattr(f, 'total_volume', 0)) for f in merchant_features.values()]
        all_customers = [float(getattr(f, 'unique_customers', 0)) for f in merchant_features.values()]
        all_growth = [float(getattr(f, 'growth_rate_30d', 0)) for f in merchant_features.values()]
        
        # Sort for percentile calculation
        all_volumes.sort()
        all_customers.sort()
        all_growth.sort()
        
        for merchant_id, features in merchant_features.items():
            merchant_name = getattr(features, 'merchant_name', 'Unknown')
            
            # Rule-based value score
            volume = getattr(features, 'total_volume', 0)
            customers = getattr(features, 'unique_customers', 0)
            avg_value = getattr(features, 'avg_transaction_value', 0)
            
            # Percentile-based scoring
            volume_percentile = self._calculate_percentile(volume, all_volumes)
            customer_percentile = self._calculate_percentile(customers, all_customers)
            
            value_score = (volume_percentile * 0.5 + customer_percentile * 0.3 + 
                          min(100, avg_value / 10) * 0.2)
            
            # Risk and growth scores
            risk_score = self._compute_risk_score(features)
            growth_score = self._compute_growth_score(features)
            
            # Composite score
            composite_score = (
                value_score * self.score_weights.get('value', 0.4) +
                (100 - risk_score) * self.score_weights.get('risk', 0.3) +
                growth_score * self.score_weights.get('growth', 0.3)
            )
            
            # Simple feature importance based on contribution to score
            top_features = {
                'total_volume': volume_percentile,
                'unique_customers': customer_percentile,
                'avg_transaction_value': min(100, avg_value / 10),
                'growth_rate_30d': growth_score
            }
            
            score_tier = self._determine_score_tier(composite_score)
            risk_level = self._determine_risk_level(risk_score)
            recommendations = self._generate_recommendations(
                features, value_score, risk_score, growth_score
            )
            
            score = MerchantScore(
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                value_score=value_score,
                risk_score=risk_score,
                growth_score=growth_score,
                composite_score=composite_score,
                prediction_confidence=0.6,  # Lower confidence for rule-based
                data_quality_score=self._assess_data_quality(features),
                top_features=top_features,
                score_tier=score_tier,
                risk_level=risk_level,
                recommendations=recommendations,
                model_version=f"{self.model_version}-rules",
                scored_at=datetime.now()
            )
            
            scores[merchant_id] = score
        
        return scores
    
    def _calculate_percentile(self, value: float, sorted_values: List[float]) -> float:
        """Calculate percentile of value in sorted list."""
        if not sorted_values:
            return 0.0
        
        if value <= sorted_values[0]:
            return 0.0
        if value >= sorted_values[-1]:
            return 100.0
        
        # Find position and interpolate
        for i, v in enumerate(sorted_values):
            if value <= v:
                percentile = (i / len(sorted_values)) * 100
                return percentile
        
        return 100.0
    
    def _compute_risk_score(self, features: Any) -> float:
        """Compute risk score (0-100, higher = riskier)."""
        risk_factors = [
            getattr(features, 'chargeback_rate', 0) * 100,
            getattr(features, 'fraud_score', 0) * 100,
            getattr(features, 'payment_failure_rate', 0) * 100,
            getattr(features, 'volume_volatility', 0) * 50,  # Scale down volatility
            getattr(features, 'customer_concentration', 0) * 50  # Scale down concentration
        ]
        
        # Weighted average of risk factors
        weights = [0.3, 0.3, 0.2, 0.1, 0.1]
        risk_score = sum(factor * weight for factor, weight in zip(risk_factors, weights))
        
        return max(0, min(100, risk_score))
    
    def _compute_growth_score(self, features: Any) -> float:
        """Compute growth potential score (0-100)."""
        growth_rate = getattr(features, 'growth_rate_30d', 0)
        trend_score = getattr(features, 'trend_score', 0)
        days_active = getattr(features, 'days_active', 0)
        
        # Convert growth rate to score (cap at 100% growth = 100 points)
        growth_component = max(0, min(100, (growth_rate + 1) * 50))
        
        # Trend component
        trend_component = max(0, min(100, (trend_score + 1) * 50))
        
        # Activity component (newer merchants get growth bonus)
        if days_active > 0:
            activity_component = max(0, min(100, 100 - (days_active / 365) * 20))
        else:
            activity_component = 50
        
        # Weighted average
        growth_score = (growth_component * 0.5 + trend_component * 0.3 + activity_component * 0.2)
        
        return max(0, min(100, growth_score))
    
    def _get_top_features_for_merchant(
        self, 
        feature_values: List[float], 
        feature_importance: Dict[str, float]
    ) -> Dict[str, float]:
        """Find top contributing features for a specific merchant."""
        if not feature_importance or not feature_values:
            return {}
        
        # Calculate feature contributions (importance * normalized value)
        contributions = {}
        
        for i, feature_name in enumerate(self.feature_names):
            if i < len(feature_values) and feature_name in feature_importance:
                # Normalize feature value (simple min-max to 0-1)
                normalized_value = max(0, min(1, feature_values[i] / 100))
                contribution = feature_importance[feature_name] * normalized_value
                contributions[feature_name] = contribution
        
        # Sort by contribution and return top 5
        sorted_contributions = sorted(contributions.items(), key=lambda x: x[1], reverse=True)
        return dict(sorted_contributions[:5])
    
    def _assess_data_quality(self, features: Any) -> float:
        """Assess quality of feature data (0-1)."""
        quality_score = 1.0
        
        # Check for missing or zero values in key features
        key_features = ['total_volume', 'transaction_count', 'unique_customers']
        
        for feature_name in key_features:
            value = getattr(features, feature_name, 0)
            if value == 0:
                quality_score -= 0.1
        
        # Check data recency
        if hasattr(features, 'computed_at'):
            computed_at = getattr(features, 'computed_at')
            if isinstance(computed_at, datetime):
                days_old = (datetime.now() - computed_at).days
                if days_old > 7:
                    quality_score -= 0.1
        
        return max(0.0, min(1.0, quality_score))
    
    def _determine_score_tier(self, composite_score: float) -> str:
        """Determine merchant tier based on composite score."""
        if composite_score >= 80:
            return 'premium'
        elif composite_score >= 60:
            return 'high'
        elif composite_score >= 40:
            return 'medium'
        else:
            return 'low'
    
    def _determine_risk_level(self, risk_score: float) -> str:
        """Determine risk level based on risk score."""
        if risk_score <= 30:
            return 'low'
        elif risk_score <= 60:
            return 'medium'
        else:
            return 'high'
    
    def _generate_recommendations(
        self, 
        features: Any, 
        value_score: float, 
        risk_score: float, 
        growth_score: float
    ) -> List[str]:
        """Generate recommendations based on scores."""
        recommendations = []
        
        # Value-based recommendations
        if value_score < 40:
            recommendations.append("Focus on increasing transaction volume and customer acquisition")
        elif value_score > 80:
            recommendations.append("Excellent value metrics - consider premium partnership opportunities")
        
        # Risk-based recommendations
        if risk_score > 60:
            recommendations.append("High risk detected - implement enhanced monitoring and fraud prevention")
        elif risk_score < 20:
            recommendations.append("Low risk profile - suitable for automated processing")
        
        # Growth-based recommendations
        if growth_score > 70:
            recommendations.append("Strong growth potential - consider investment in merchant support")
        elif growth_score < 30:
            recommendations.append("Limited growth - focus on retention and optimization strategies")
        
        # Feature-specific recommendations
        chargeback_rate = getattr(features, 'chargeback_rate', 0)
        if chargeback_rate > 0.05:  # 5% chargeback rate
            recommendations.append("High chargeback rate - review dispute management processes")
        
        repeat_ratio = getattr(features, 'repeat_customer_ratio', 0)
        if repeat_ratio < 0.3:  # Less than 30% repeat customers
            recommendations.append("Low customer retention - implement loyalty programs")
        
        if not recommendations:
            recommendations.append("Performance metrics within normal ranges - continue monitoring")
        
        return recommendations
    
    async def _load_model(self) -> None:
        """Load existing ML model if available."""
        try:
            if os.path.exists(self.model_path) and xgb is not None:
                with open(self.model_path, 'rb') as f:
                    model_data = pickle.load(f)
                    self.model = model_data.get('model')
                    self.feature_names = model_data.get('feature_names', [])
                    self.model_metrics = model_data.get('metrics')
                
                self.logger.info(f"Loaded ML model version {self.model_version}")
            else:
                self.logger.info("No existing model found - will use rule-based scoring")
        except Exception as e:
            self.logger.warning(f"Failed to load model: {e}")
            self.model = None
    
    async def train_model(
        self, 
        merchant_features: Dict[str, Any],
        attribution_results: Optional[Dict[str, Any]] = None,
        target_values: Optional[Dict[str, float]] = None
    ) -> bool:
        """Train/retrain the XGBoost model.
        
        Args:
            merchant_features: Training feature data
            attribution_results: Attribution data for enhanced features
            target_values: Target values for supervised learning
            
        Returns:
            True if training successful, False otherwise
        """
        if xgb is None or np is None:
            self.logger.warning("XGBoost or NumPy not available for model training")
            return False
        
        try:
            with AgentActionContext("merchant_scoring", "train_model"):
                # Prepare training data
                feature_matrix, merchant_ids = await self._prepare_feature_matrix(
                    merchant_features, attribution_results
                )
                
                if not feature_matrix or len(feature_matrix) < self.min_samples_for_training:
                    sample_count = len(feature_matrix) if feature_matrix else 0
                    self.logger.warning(f"Insufficient training data: {sample_count} samples")
                    return False
                
                # Prepare target values (if not provided, use composite scoring)
                if target_values is None:
                    target_values = {}
                    temp_scores = await self._score_with_rules(merchant_features, attribution_results)
                    for mid, score in temp_scores.items():
                        target_values[mid] = score.composite_score / 100  # Normalize to 0-1
                
                # Create target array
                targets = [target_values.get(mid, 0.5) for mid in merchant_ids]
                
                # Convert to numpy arrays
                X_train = np.array(feature_matrix)
                y_train = np.array(targets)
                
                # Train XGBoost model
                model = xgb.XGBRegressor(**self.xgb_params)
                model.fit(X_train, y_train)
                
                # Calculate metrics (simplified for demonstration)
                predictions = model.predict(X_train)
                accuracy = 1.0 - np.mean(np.abs(predictions - y_train))  # Simple accuracy metric
                
                # Create model metrics
                metrics = ModelMetrics(
                    model_version=self.model_version,
                    training_date=datetime.now(),
                    accuracy=accuracy,
                    precision=0.8,  # Placeholder
                    recall=0.8,     # Placeholder
                    f1_score=0.8,   # Placeholder
                    auc_roc=0.8,    # Placeholder
                    training_samples=len(feature_matrix),
                    feature_count=len(self.feature_names),
                    feature_importance=dict(zip(self.feature_names, model.feature_importances_))
                )
                
                # Save model
                os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
                model_data = {
                    'model': model,
                    'feature_names': self.feature_names,
                    'metrics': metrics
                }
                
                with open(self.model_path, 'wb') as f:
                    pickle.dump(model_data, f)
                
                # Update agent state
                self.model = model
                self.model_metrics = metrics
                
                self.logger.info(f"Model training completed - Accuracy: {accuracy:.3f}")
                return True
                
        except Exception as e:
            self.logger.error(f"Model training failed: {e}")
            return False
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status."""
        status = {
            "agent_name": "MerchantScoringAgent",
            "is_running": self.is_running,
            "last_scoring_time": self.last_scoring_time.isoformat() if self.last_scoring_time else None,
            "scored_merchants_count": self.scored_merchants_count,
            "score_cache_size": len(self.score_cache),
            "model_available": self.model is not None,
            "dependencies": {
                "xgboost_available": xgb is not None,
                "numpy_available": np is not None
            },
            "configuration": {
                "model_version": self.model_version,
                "confidence_threshold": self.confidence_threshold,
                "score_weights": self.score_weights
            }
        }
        
        if self.model_metrics:
            status["model_metrics"] = {
                "training_date": self.model_metrics.training_date.isoformat(),
                "accuracy": self.model_metrics.accuracy,
                "training_samples": self.model_metrics.training_samples,
                "feature_count": self.model_metrics.feature_count
            }
        
        return status
    
    async def get_merchant_score(self, merchant_id: str) -> Optional[MerchantScore]:
        """Get score for a specific merchant."""
        return self.score_cache.get(merchant_id)
    
    async def get_top_merchants(self, metric: str = "composite_score", limit: int = 10) -> List[Tuple[str, float]]:
        """Get top merchants by scoring metric."""
        if not self.score_cache:
            return []
        
        merchant_values = []
        for merchant_id, score in self.score_cache.items():
            if hasattr(score, metric):
                value = getattr(score, metric)
                merchant_values.append((merchant_id, value))
        
        merchant_values.sort(key=lambda x: x[1], reverse=True)
        return merchant_values[:limit]
    
    async def get_feature_importance(self) -> Dict[str, float]:
        """Get model feature importance."""
        if self.model_metrics and self.model_metrics.feature_importance:
            return self.model_metrics.feature_importance.copy()
        return {}
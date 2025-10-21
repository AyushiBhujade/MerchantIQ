"""
Feature Engineering Agent for MerchantIQ system.
Computes merchant-level features from transaction streams and customer data.
"""

import asyncio
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import defaultdict
import math

from ..utils.logger import get_logger, performance_log, AgentActionContext


@dataclass
class MerchantFeatures:
    """Merchant feature set for ML model."""
    merchant_id: str
    merchant_name: str
    
    # Volume metrics
    total_volume: float
    avg_transaction_value: float
    transaction_count: int
    
    # Customer metrics
    unique_customers: int
    repeat_customer_ratio: float
    avg_customer_lifetime_value: float
    
    # Temporal metrics
    days_active: int
    avg_transactions_per_day: float
    peak_day_volume: float
    
    # Risk metrics
    chargeback_rate: float
    fraud_score: float
    payment_failure_rate: float
    
    # Category and location metrics
    category_diversity: float
    location_spread: int
    
    # Growth metrics
    growth_rate_30d: float
    trend_score: float
    
    # Additional computed features
    volume_volatility: float
    customer_concentration: float
    
    # Timestamp
    computed_at: datetime
    data_window_days: int


class FeatureEngineeringAgent:
    """Agent responsible for computing merchant-level features."""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the Feature Engineering Agent.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.logger = get_logger("agent.feature_engineering")
        
        # Agent state
        self.is_running = False
        self.last_computation_time: Optional[datetime] = None
        self.processed_merchants_count = 0
        
        # Feature computation parameters
        feature_config = self.config.get('features', {})
        self.lookback_days = feature_config.get('lookback_days', 30)
        self.min_transactions = feature_config.get('min_transactions', 5)
        self.growth_window_days = feature_config.get('growth_window_days', 30)
        
        # Caches for incremental computation
        self.merchant_cache: Dict[str, Dict[str, Any]] = {}
        self.feature_cache: Dict[str, MerchantFeatures] = {}
    
    @performance_log
    async def start(self) -> None:
        """Start the feature engineering agent."""
        with AgentActionContext("feature_engineering", "start"):
            self.is_running = True
            self.logger.info("Feature Engineering Agent started")
    
    async def stop(self) -> None:
        """Stop the feature engineering agent."""
        with AgentActionContext("feature_engineering", "stop"):
            self.is_running = False
            self.logger.info("Feature Engineering Agent stopped")
    
    @performance_log
    async def compute_features(self, unified_events: List[Any]) -> Dict[str, MerchantFeatures]:
        """Compute merchant features from unified events.
        
        Args:
            unified_events: List of UnifiedEvent objects from data ingestion
            
        Returns:
            Dictionary mapping merchant_id to MerchantFeatures
        """
        with AgentActionContext("feature_engineering", "compute_features"):
            try:
                # Group events by merchant
                merchant_events = self._group_events_by_merchant(unified_events)
                
                # Compute features for each merchant
                merchant_features = {}
                
                for merchant_id, events in merchant_events.items():
                    if len(events) < self.min_transactions:
                        self.logger.debug(f"Skipping merchant {merchant_id}: insufficient transactions ({len(events)})")
                        continue
                    
                    features = await self._compute_merchant_features(merchant_id, events)
                    if features:
                        merchant_features[merchant_id] = features
                
                # Update agent state
                self.last_computation_time = datetime.now()
                self.processed_merchants_count = len(merchant_features)
                
                # Cache results
                self.feature_cache.update(merchant_features)
                
                self.logger.info(f"Computed features for {len(merchant_features)} merchants")
                return merchant_features
                
            except Exception as e:
                self.logger.error(f"Feature computation failed: {e}")
                raise
    
    def _group_events_by_merchant(self, unified_events: List[Any]) -> Dict[str, List[Any]]:
        """Group unified events by merchant ID."""
        merchant_events = defaultdict(list)
        
        for event in unified_events:
            if hasattr(event, 'merchant_id') and event.merchant_id:
                merchant_events[event.merchant_id].append(event)
        
        return dict(merchant_events)
    
    @performance_log
    async def _compute_merchant_features(self, merchant_id: str, events: List[Any]) -> Optional[MerchantFeatures]:
        """Compute comprehensive features for a single merchant.
        
        Args:
            merchant_id: Merchant identifier
            events: List of unified events for this merchant
            
        Returns:
            MerchantFeatures object or None if computation fails
        """
        try:
            # Sort events by timestamp
            sorted_events = sorted(events, key=lambda x: x.timestamp)
            
            # Basic merchant info
            merchant_name = sorted_events[0].merchant_name if sorted_events else "Unknown"
            
            # Volume metrics
            volume_metrics = self._compute_volume_metrics(sorted_events)
            
            # Customer metrics
            customer_metrics = self._compute_customer_metrics(sorted_events)
            
            # Temporal metrics
            temporal_metrics = self._compute_temporal_metrics(sorted_events)
            
            # Risk metrics
            risk_metrics = self._compute_risk_metrics(sorted_events)
            
            # Category and location metrics
            category_location_metrics = self._compute_category_location_metrics(sorted_events)
            
            # Growth metrics
            growth_metrics = self._compute_growth_metrics(sorted_events)
            
            # Advanced metrics
            advanced_metrics = self._compute_advanced_metrics(sorted_events)
            
            # Create MerchantFeatures object
            features = MerchantFeatures(
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                
                # Volume metrics
                total_volume=volume_metrics['total_volume'],
                avg_transaction_value=volume_metrics['avg_transaction_value'],
                transaction_count=int(volume_metrics['transaction_count']),
                
                # Customer metrics
                unique_customers=int(customer_metrics['unique_customers']),
                repeat_customer_ratio=customer_metrics['repeat_customer_ratio'],
                avg_customer_lifetime_value=customer_metrics['avg_customer_lifetime_value'],
                
                # Temporal metrics
                days_active=int(temporal_metrics['days_active']),
                avg_transactions_per_day=temporal_metrics['avg_transactions_per_day'],
                peak_day_volume=temporal_metrics['peak_day_volume'],
                
                # Risk metrics
                chargeback_rate=risk_metrics['chargeback_rate'],
                fraud_score=risk_metrics['fraud_score'],
                payment_failure_rate=risk_metrics['payment_failure_rate'],
                
                # Category and location metrics
                category_diversity=category_location_metrics['category_diversity'],
                location_spread=int(category_location_metrics['location_spread']),
                
                # Growth metrics
                growth_rate_30d=growth_metrics['growth_rate_30d'],
                trend_score=growth_metrics['trend_score'],
                
                # Advanced metrics
                volume_volatility=advanced_metrics['volume_volatility'],
                customer_concentration=advanced_metrics['customer_concentration'],
                
                computed_at=datetime.now(),
                data_window_days=self.lookback_days
            )
            
            return features
            
        except Exception as e:
            self.logger.error(f"Failed to compute features for merchant {merchant_id}: {e}")
            return None
    
    def _compute_volume_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute volume-related metrics."""
        if not events:
            return {'total_volume': 0.0, 'avg_transaction_value': 0.0, 'transaction_count': 0}
        
        amounts = [event.transaction_amount for event in events]
        total_volume = sum(amounts)
        transaction_count = len(events)
        avg_transaction_value = total_volume / transaction_count if transaction_count > 0 else 0.0
        
        return {
            'total_volume': total_volume,
            'avg_transaction_value': avg_transaction_value,
            'transaction_count': transaction_count
        }
    
    def _compute_customer_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute customer-related metrics."""
        if not events:
            return {
                'unique_customers': 0,
                'repeat_customer_ratio': 0.0,
                'avg_customer_lifetime_value': 0.0
            }
        
        # Customer transaction counts
        customer_transactions = defaultdict(list)
        for event in events:
            customer_transactions[event.customer_id].append(event.transaction_amount)
        
        unique_customers = len(customer_transactions)
        repeat_customers = sum(1 for txns in customer_transactions.values() if len(txns) > 1)
        repeat_customer_ratio = repeat_customers / unique_customers if unique_customers > 0 else 0.0
        
        # Average customer lifetime value
        customer_values = [sum(amounts) for amounts in customer_transactions.values()]
        avg_customer_lifetime_value = sum(customer_values) / len(customer_values) if customer_values else 0.0
        
        return {
            'unique_customers': unique_customers,
            'repeat_customer_ratio': repeat_customer_ratio,
            'avg_customer_lifetime_value': avg_customer_lifetime_value
        }
    
    def _compute_temporal_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute time-based metrics."""
        if not events:
            return {
                'days_active': 0,
                'avg_transactions_per_day': 0.0,
                'peak_day_volume': 0.0
            }
        
        # Get date range
        timestamps = [event.timestamp for event in events]
        min_date = min(timestamps).date()
        max_date = max(timestamps).date()
        days_active = (max_date - min_date).days + 1
        
        # Daily transaction counts and volumes
        daily_stats = defaultdict(lambda: {'count': 0, 'volume': 0.0})
        for event in events:
            date_key = event.timestamp.date()
            daily_stats[date_key]['count'] += 1
            daily_stats[date_key]['volume'] += event.transaction_amount
        
        # Average transactions per day
        avg_transactions_per_day = len(events) / days_active if days_active > 0 else 0.0
        
        # Peak day volume
        peak_day_volume = max(day['volume'] for day in daily_stats.values()) if daily_stats else 0.0
        
        return {
            'days_active': days_active,
            'avg_transactions_per_day': avg_transactions_per_day,
            'peak_day_volume': peak_day_volume
        }
    
    def _compute_risk_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute risk-related metrics."""
        if not events:
            return {
                'chargeback_rate': 0.0,
                'fraud_score': 0.0,
                'payment_failure_rate': 0.0
            }
        
        total_transactions = len(events)
        
        # Chargeback rate (simulated from status)
        chargebacks = sum(1 for event in events 
                         if hasattr(event, 'additional_data') 
                         and event.additional_data 
                         and event.additional_data.get('status') == 'chargeback')
        chargeback_rate = chargebacks / total_transactions if total_transactions > 0 else 0.0
        
        # Fraud score (simulated from additional data)
        fraud_transactions = 0
        for event in events:
            if (hasattr(event, 'additional_data') 
                and event.additional_data 
                and event.additional_data.get('is_fraud', False)):
                fraud_transactions += 1
        fraud_score = fraud_transactions / total_transactions if total_transactions > 0 else 0.0
        
        # Payment failure rate (simulated from status)
        failed_payments = sum(1 for event in events 
                             if hasattr(event, 'additional_data') 
                             and event.additional_data 
                             and event.additional_data.get('status') in ['failed', 'declined'])
        payment_failure_rate = failed_payments / total_transactions if total_transactions > 0 else 0.0
        
        return {
            'chargeback_rate': chargeback_rate,
            'fraud_score': fraud_score,
            'payment_failure_rate': payment_failure_rate
        }
    
    def _compute_category_location_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute category and location diversity metrics."""
        if not events:
            return {'category_diversity': 0.0, 'location_spread': 0}
        
        # Category diversity (Shannon entropy)
        categories = [event.category for event in events if event.category]
        category_counts = defaultdict(int)
        for category in categories:
            category_counts[category] += 1
        
        category_diversity = 0.0
        if category_counts:
            total = sum(category_counts.values())
            for count in category_counts.values():
                if count > 0:
                    p = count / total
                    category_diversity -= p * math.log2(p)
        
        # Location spread (unique locations)
        locations = set()
        for event in events:
            if hasattr(event, 'location') and event.location:
                if isinstance(event.location, dict):
                    location_key = f"{event.location.get('city', '')},{event.location.get('state', '')}"
                    locations.add(location_key)
        
        location_spread = len(locations)
        
        return {
            'category_diversity': category_diversity,
            'location_spread': location_spread
        }
    
    def _compute_growth_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute growth and trend metrics."""
        if len(events) < 2:
            return {'growth_rate_30d': 0.0, 'trend_score': 0.0}
        
        # Sort events by timestamp
        sorted_events = sorted(events, key=lambda x: x.timestamp)
        
        # Split into two periods for growth calculation
        cutoff_date = datetime.now() - timedelta(days=self.growth_window_days // 2)
        
        recent_events = [e for e in sorted_events if e.timestamp >= cutoff_date]
        older_events = [e for e in sorted_events if e.timestamp < cutoff_date]
        
        if not older_events:
            return {'growth_rate_30d': 0.0, 'trend_score': 0.0}
        
        # Calculate growth rate
        recent_volume = sum(e.transaction_amount for e in recent_events)
        older_volume = sum(e.transaction_amount for e in older_events)
        
        growth_rate_30d = 0.0
        if older_volume > 0:
            growth_rate_30d = (recent_volume - older_volume) / older_volume
        
        # Trend score (simple linear trend)
        trend_score = 0.0
        if len(sorted_events) >= 3:
            # Use daily volumes for trend calculation
            daily_volumes = defaultdict(float)
            for event in sorted_events:
                day_key = event.timestamp.date()
                daily_volumes[day_key] += event.transaction_amount
            
            if len(daily_volumes) >= 3:
                dates = sorted(daily_volumes.keys())
                volumes = [daily_volumes[date] for date in dates]
                
                # Simple linear regression slope
                n = len(volumes)
                x_mean = (n - 1) / 2
                y_mean = sum(volumes) / n
                
                numerator = sum((i - x_mean) * (volumes[i] - y_mean) for i in range(n))
                denominator = sum((i - x_mean) ** 2 for i in range(n))
                
                if denominator > 0:
                    trend_score = numerator / denominator
        
        return {
            'growth_rate_30d': growth_rate_30d,
            'trend_score': trend_score
        }
    
    def _compute_advanced_metrics(self, events: List[Any]) -> Dict[str, float]:
        """Compute advanced statistical metrics."""
        if not events:
            return {'volume_volatility': 0.0, 'customer_concentration': 0.0}
        
        # Volume volatility (coefficient of variation)
        amounts = [event.transaction_amount for event in events]
        mean_amount = sum(amounts) / len(amounts)
        
        if mean_amount > 0:
            variance = sum((amount - mean_amount) ** 2 for amount in amounts) / len(amounts)
            std_dev = math.sqrt(variance)
            volume_volatility = std_dev / mean_amount
        else:
            volume_volatility = 0.0
        
        # Customer concentration (Gini coefficient approximation)
        customer_volumes = defaultdict(float)
        for event in events:
            customer_volumes[event.customer_id] += event.transaction_amount
        
        volumes = sorted(customer_volumes.values())
        n = len(volumes)
        
        customer_concentration = 0.0
        if n > 1:
            # Simplified Gini coefficient
            total_volume = sum(volumes)
            if total_volume > 0:
                cumulative_sum = 0
                for i, volume in enumerate(volumes):
                    cumulative_sum += volume
                    customer_concentration += (2 * (i + 1) - n - 1) * volume
                customer_concentration = customer_concentration / (n * total_volume)
        
        return {
            'volume_volatility': volume_volatility,
            'customer_concentration': customer_concentration
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status."""
        return {
            "agent_name": "FeatureEngineeringAgent",
            "is_running": self.is_running,
            "last_computation_time": self.last_computation_time.isoformat() if self.last_computation_time else None,
            "processed_merchants_count": self.processed_merchants_count,
            "cache_size": len(self.feature_cache),
            "configuration": {
                "lookback_days": self.lookback_days,
                "min_transactions": self.min_transactions,
                "growth_window_days": self.growth_window_days
            }
        }
    
    async def get_merchant_features(self, merchant_id: str) -> Optional[MerchantFeatures]:
        """Get cached features for a specific merchant."""
        return self.feature_cache.get(merchant_id)
    
    async def get_top_merchants(self, metric: str = "total_volume", limit: int = 10) -> List[Tuple[str, float]]:
        """Get top merchants by specified metric."""
        if not self.feature_cache:
            return []
        
        # Get metric values for all merchants
        merchant_metrics = []
        for merchant_id, features in self.feature_cache.items():
            if hasattr(features, metric):
                value = getattr(features, metric)
                merchant_metrics.append((merchant_id, value))
        
        # Sort by metric value (descending) and return top N
        merchant_metrics.sort(key=lambda x: x[1], reverse=True)
        return merchant_metrics[:limit]
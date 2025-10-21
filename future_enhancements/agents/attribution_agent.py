"""
Attribution Agent for MerchantIQ system.
Implements Markov Chain attribution modeling for merchant value attribution.
"""

import asyncio
from typing import Dict, List, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
from dataclasses import dataclass
from collections import defaultdict, Counter
import itertools
import math

from ..utils.logger import get_logger, performance_log, AgentActionContext


@dataclass
class CustomerJourney:
    """Represents a customer's journey across merchants."""
    customer_id: str
    touchpoints: List[Dict[str, Any]]  # Each touchpoint has merchant_id, timestamp, amount, etc.
    total_value: float
    journey_length: int
    time_span_days: int
    first_touchpoint: datetime
    last_touchpoint: datetime


@dataclass
class AttributionResult:
    """Attribution result for a merchant."""
    merchant_id: str
    merchant_name: str
    
    # Attribution scores
    first_touch_attribution: float
    last_touch_attribution: float
    linear_attribution: float
    time_decay_attribution: float
    markov_attribution: float
    
    # Contribution metrics
    removal_effect: float  # How much value is lost when this merchant is removed
    incremental_value: float  # Additional value created by this merchant
    conversion_contribution: float  # Contribution to final conversions
    
    # Journey metrics
    avg_position_in_journey: float
    appears_in_journeys: int
    unique_customers_attributed: int
    
    # Computed at
    computed_at: datetime


@dataclass
class MarkovTransition:
    """Markov chain transition between merchants."""
    from_merchant: str
    to_merchant: str
    transition_count: int
    transition_probability: float
    total_value: float
    avg_value_per_transition: float


class AttributionAgent:
    """Agent responsible for merchant value attribution using Markov Chain modeling."""
    
    def __init__(self, config: Optional[Dict] = None):
        """Initialize the Attribution Agent.
        
        Args:
            config: Configuration dictionary
        """
        self.config = config or {}
        self.logger = get_logger("agent.attribution")
        
        # Agent state
        self.is_running = False
        self.last_attribution_time: Optional[datetime] = None
        self.processed_journeys_count = 0
        
        # Attribution parameters
        attribution_config = self.config.get('attribution', {})
        self.journey_window_days = attribution_config.get('journey_window_days', 30)
        self.min_touchpoints = attribution_config.get('min_touchpoints', 2)
        self.time_decay_factor = attribution_config.get('time_decay_factor', 0.5)
        self.conversion_window_hours = attribution_config.get('conversion_window_hours', 24)
        
        # Markov chain parameters
        markov_config = attribution_config.get('markov', {})
        self.removal_iterations = markov_config.get('removal_iterations', 10000)
        self.transition_threshold = markov_config.get('transition_threshold', 10)
        
        # Caches
        self.journey_cache: Dict[str, CustomerJourney] = {}
        self.attribution_results: Dict[str, AttributionResult] = {}
        self.transition_matrix: Dict[Tuple[str, str], MarkovTransition] = {}
    
    @performance_log
    async def start(self) -> None:
        """Start the attribution agent."""
        with AgentActionContext("attribution", "start"):
            self.is_running = True
            self.logger.info("Attribution Agent started")
    
    async def stop(self) -> None:
        """Stop the attribution agent."""
        with AgentActionContext("attribution", "stop"):
            self.is_running = False
            self.logger.info("Attribution Agent stopped")
    
    @performance_log
    async def compute_attribution(self, unified_events: List[Any]) -> Dict[str, AttributionResult]:
        """Compute merchant attribution from unified events.
        
        Args:
            unified_events: List of UnifiedEvent objects
            
        Returns:
            Dictionary mapping merchant_id to AttributionResult
        """
        with AgentActionContext("attribution", "compute_attribution"):
            try:
                # Build customer journeys
                customer_journeys = await self._build_customer_journeys(unified_events)
                
                # Filter journeys by minimum touchpoints
                valid_journeys = {
                    customer_id: journey 
                    for customer_id, journey in customer_journeys.items()
                    if journey.journey_length >= self.min_touchpoints
                }
                
                if not valid_journeys:
                    self.logger.warning("No valid customer journeys found for attribution")
                    return {}
                
                # Build Markov transition matrix
                await self._build_transition_matrix(valid_journeys)
                
                # Compute various attribution models
                attribution_results = {}
                merchants = self._get_unique_merchants(valid_journeys)
                
                for merchant_id in merchants:
                    result = await self._compute_merchant_attribution(merchant_id, valid_journeys)
                    if result:
                        attribution_results[merchant_id] = result
                
                # Update agent state
                self.last_attribution_time = datetime.now()
                self.processed_journeys_count = len(valid_journeys)
                self.attribution_results.update(attribution_results)
                
                self.logger.info(f"Computed attribution for {len(attribution_results)} merchants from {len(valid_journeys)} journeys")
                return attribution_results
                
            except Exception as e:
                self.logger.error(f"Attribution computation failed: {e}")
                raise
    
    async def _build_customer_journeys(self, unified_events: List[Any]) -> Dict[str, CustomerJourney]:
        """Build customer journey objects from unified events."""
        customer_events = defaultdict(list)
        
        # Group events by customer
        for event in unified_events:
            if hasattr(event, 'customer_id') and event.customer_id:
                customer_events[event.customer_id].append(event)
        
        customer_journeys = {}
        
        for customer_id, events in customer_events.items():
            # Sort events by timestamp
            sorted_events = sorted(events, key=lambda x: x.timestamp)
            
            # Filter events within journey window
            cutoff_time = datetime.now() - timedelta(days=self.journey_window_days)
            recent_events = [e for e in sorted_events if e.timestamp >= cutoff_time]
            
            if not recent_events:
                continue
            
            # Build touchpoints
            touchpoints = []
            for event in recent_events:
                touchpoint = {
                    'merchant_id': event.merchant_id,
                    'merchant_name': event.merchant_name,
                    'timestamp': event.timestamp,
                    'amount': event.transaction_amount,
                    'category': event.category,
                    'transaction_id': event.transaction_id
                }
                touchpoints.append(touchpoint)
            
            # Create CustomerJourney
            if touchpoints:
                total_value = sum(tp['amount'] for tp in touchpoints)
                first_touchpoint = min(tp['timestamp'] for tp in touchpoints)
                last_touchpoint = max(tp['timestamp'] for tp in touchpoints)
                time_span = (last_touchpoint - first_touchpoint).days
                
                journey = CustomerJourney(
                    customer_id=customer_id,
                    touchpoints=touchpoints,
                    total_value=total_value,
                    journey_length=len(touchpoints),
                    time_span_days=time_span,
                    first_touchpoint=first_touchpoint,
                    last_touchpoint=last_touchpoint
                )
                
                customer_journeys[customer_id] = journey
        
        return customer_journeys
    
    async def _build_transition_matrix(self, customer_journeys: Dict[str, CustomerJourney]) -> None:
        """Build Markov transition matrix from customer journeys."""
        transitions = defaultdict(lambda: {'count': 0, 'total_value': 0.0})
        
        for journey in customer_journeys.values():
            touchpoints = journey.touchpoints
            
            # Add transitions between consecutive touchpoints
            for i in range(len(touchpoints) - 1):
                current_merchant = touchpoints[i]['merchant_id']
                next_merchant = touchpoints[i + 1]['merchant_id']
                transition_value = touchpoints[i + 1]['amount']
                
                transition_key = (current_merchant, next_merchant)
                transitions[transition_key]['count'] += 1
                transitions[transition_key]['total_value'] += transition_value
            
            # Add start and end transitions
            if touchpoints:
                # Start transition
                first_merchant = touchpoints[0]['merchant_id']
                start_key = ('__start__', first_merchant)
                transitions[start_key]['count'] += 1
                transitions[start_key]['total_value'] += touchpoints[0]['amount']
                
                # End transition
                last_merchant = touchpoints[-1]['merchant_id']
                end_key = (last_merchant, '__end__')
                transitions[end_key]['count'] += 1
                transitions[end_key]['total_value'] += touchpoints[-1]['amount']
        
        # Filter transitions by threshold and compute probabilities
        self.transition_matrix = {}
        
        # Calculate outgoing transition totals for probability normalization
        outgoing_totals = defaultdict(int)
        for (from_merchant, to_merchant), data in transitions.items():
            if data['count'] >= self.transition_threshold:
                outgoing_totals[from_merchant] += int(data['count'])
        
        # Create MarkovTransition objects with probabilities
        for (from_merchant, to_merchant), data in transitions.items():
            if data['count'] >= self.transition_threshold:
                total_outgoing = outgoing_totals[from_merchant]
                probability = data['count'] / total_outgoing if total_outgoing > 0 else 0.0
                avg_value = data['total_value'] / data['count'] if data['count'] > 0 else 0.0
                
                transition = MarkovTransition(
                    from_merchant=from_merchant,
                    to_merchant=to_merchant,
                    transition_count=int(data['count']),
                    transition_probability=probability,
                    total_value=data['total_value'],
                    avg_value_per_transition=avg_value
                )
                
                self.transition_matrix[(from_merchant, to_merchant)] = transition
    
    def _get_unique_merchants(self, customer_journeys: Dict[str, CustomerJourney]) -> Set[str]:
        """Get unique merchants from customer journeys."""
        merchants = set()
        for journey in customer_journeys.values():
            for touchpoint in journey.touchpoints:
                merchants.add(touchpoint['merchant_id'])
        return merchants
    
    @performance_log
    async def _compute_merchant_attribution(self, merchant_id: str, customer_journeys: Dict[str, CustomerJourney]) -> Optional[AttributionResult]:
        """Compute attribution for a specific merchant.
        
        Args:
            merchant_id: Merchant to compute attribution for
            customer_journeys: All customer journeys
            
        Returns:
            AttributionResult or None if computation fails
        """
        try:
            # Find journeys that include this merchant
            relevant_journeys = []
            merchant_name = "Unknown"
            
            for journey in customer_journeys.values():
                merchant_touchpoints = [
                    tp for tp in journey.touchpoints 
                    if tp['merchant_id'] == merchant_id
                ]
                if merchant_touchpoints:
                    relevant_journeys.append(journey)
                    if merchant_name == "Unknown" and merchant_touchpoints:
                        merchant_name = merchant_touchpoints[0]['merchant_name']
            
            if not relevant_journeys:
                return None
            
            # Compute different attribution models
            first_touch = self._compute_first_touch_attribution(merchant_id, relevant_journeys)
            last_touch = self._compute_last_touch_attribution(merchant_id, relevant_journeys)
            linear = self._compute_linear_attribution(merchant_id, relevant_journeys)
            time_decay = self._compute_time_decay_attribution(merchant_id, relevant_journeys)
            markov = await self._compute_markov_attribution(merchant_id, customer_journeys)
            
            # Compute contribution metrics
            removal_effect = await self._compute_removal_effect(merchant_id, customer_journeys)
            incremental_value = self._compute_incremental_value(merchant_id, relevant_journeys)
            conversion_contribution = self._compute_conversion_contribution(merchant_id, relevant_journeys)
            
            # Compute journey metrics
            journey_metrics = self._compute_journey_metrics(merchant_id, relevant_journeys)
            
            return AttributionResult(
                merchant_id=merchant_id,
                merchant_name=merchant_name,
                first_touch_attribution=first_touch,
                last_touch_attribution=last_touch,
                linear_attribution=linear,
                time_decay_attribution=time_decay,
                markov_attribution=markov,
                removal_effect=removal_effect,
                incremental_value=incremental_value,
                conversion_contribution=conversion_contribution,
                avg_position_in_journey=journey_metrics['avg_position'],
                appears_in_journeys=journey_metrics['appears_in_journeys'],
                unique_customers_attributed=journey_metrics['unique_customers'],
                computed_at=datetime.now()
            )
            
        except Exception as e:
            self.logger.error(f"Failed to compute attribution for merchant {merchant_id}: {e}")
            return None
    
    def _compute_first_touch_attribution(self, merchant_id: str, journeys: List[CustomerJourney]) -> float:
        """Compute first-touch attribution value."""
        attributed_value = 0.0
        
        for journey in journeys:
            if journey.touchpoints and journey.touchpoints[0]['merchant_id'] == merchant_id:
                attributed_value += journey.total_value
        
        return attributed_value
    
    def _compute_last_touch_attribution(self, merchant_id: str, journeys: List[CustomerJourney]) -> float:
        """Compute last-touch attribution value."""
        attributed_value = 0.0
        
        for journey in journeys:
            if journey.touchpoints and journey.touchpoints[-1]['merchant_id'] == merchant_id:
                attributed_value += journey.total_value
        
        return attributed_value
    
    def _compute_linear_attribution(self, merchant_id: str, journeys: List[CustomerJourney]) -> float:
        """Compute linear attribution value."""
        attributed_value = 0.0
        
        for journey in journeys:
            merchant_touchpoints = [
                tp for tp in journey.touchpoints 
                if tp['merchant_id'] == merchant_id
            ]
            if merchant_touchpoints:
                # Equal attribution across all touchpoints
                attribution_weight = len(merchant_touchpoints) / journey.journey_length
                attributed_value += journey.total_value * attribution_weight
        
        return attributed_value
    
    def _compute_time_decay_attribution(self, merchant_id: str, journeys: List[CustomerJourney]) -> float:
        """Compute time-decay attribution value."""
        attributed_value = 0.0
        
        for journey in journeys:
            touchpoints = journey.touchpoints
            if not touchpoints:
                continue
            
            # Find touchpoints for this merchant
            merchant_touchpoints = [
                (i, tp) for i, tp in enumerate(touchpoints) 
                if tp['merchant_id'] == merchant_id
            ]
            
            if not merchant_touchpoints:
                continue
            
            # Compute time decay weights
            total_weight = 0.0
            merchant_weight = 0.0
            
            for i, touchpoint in enumerate(touchpoints):
                # Weight decreases exponentially as we go back in time
                days_from_end = (journey.last_touchpoint - touchpoint['timestamp']).days
                weight = math.exp(-self.time_decay_factor * days_from_end)
                total_weight += weight
                
                if touchpoint['merchant_id'] == merchant_id:
                    merchant_weight += weight
            
            if total_weight > 0:
                attribution_weight = merchant_weight / total_weight
                attributed_value += journey.total_value * attribution_weight
        
        return attributed_value
    
    async def _compute_markov_attribution(self, merchant_id: str, customer_journeys: Dict[str, CustomerJourney]) -> float:
        """Compute Markov chain attribution using removal effect."""
        # This is a simplified version of Markov attribution
        # In a full implementation, we would use more sophisticated algorithms
        
        total_conversion_value = sum(journey.total_value for journey in customer_journeys.values())
        
        # Compute conversion probability with all merchants
        conversion_prob_with_all = self._compute_conversion_probability(customer_journeys)
        
        # Compute conversion probability without this merchant
        journeys_without_merchant = self._remove_merchant_from_journeys(merchant_id, customer_journeys)
        conversion_prob_without = self._compute_conversion_probability(journeys_without_merchant)
        
        # Attribution is proportional to the loss in conversion probability
        if conversion_prob_with_all > 0:
            removal_effect = (conversion_prob_with_all - conversion_prob_without) / conversion_prob_with_all
            attributed_value = total_conversion_value * removal_effect
        else:
            attributed_value = 0.0
        
        return attributed_value
    
    def _compute_conversion_probability(self, customer_journeys: Dict[str, CustomerJourney]) -> float:
        """Compute overall conversion probability from journeys."""
        if not customer_journeys:
            return 0.0
        
        # Simple heuristic: probability based on journey completion rates
        completed_journeys = sum(
            1 for journey in customer_journeys.values()
            if journey.journey_length > 1  # More than one touchpoint indicates engagement
        )
        
        return completed_journeys / len(customer_journeys)
    
    def _remove_merchant_from_journeys(self, merchant_id: str, customer_journeys: Dict[str, CustomerJourney]) -> Dict[str, CustomerJourney]:
        """Create modified journeys with specified merchant removed."""
        modified_journeys = {}
        
        for customer_id, journey in customer_journeys.items():
            # Remove touchpoints from this merchant
            filtered_touchpoints = [
                tp for tp in journey.touchpoints 
                if tp['merchant_id'] != merchant_id
            ]
            
            if filtered_touchpoints:
                # Recalculate journey metrics
                total_value = sum(tp['amount'] for tp in filtered_touchpoints)
                first_touchpoint = min(tp['timestamp'] for tp in filtered_touchpoints)
                last_touchpoint = max(tp['timestamp'] for tp in filtered_touchpoints)
                time_span = (last_touchpoint - first_touchpoint).days
                
                modified_journey = CustomerJourney(
                    customer_id=customer_id,
                    touchpoints=filtered_touchpoints,
                    total_value=total_value,
                    journey_length=len(filtered_touchpoints),
                    time_span_days=time_span,
                    first_touchpoint=first_touchpoint,
                    last_touchpoint=last_touchpoint
                )
                
                modified_journeys[customer_id] = modified_journey
        
        return modified_journeys
    
    async def _compute_removal_effect(self, merchant_id: str, customer_journeys: Dict[str, CustomerJourney]) -> float:
        """Compute the removal effect of a merchant."""
        total_value_with_merchant = sum(journey.total_value for journey in customer_journeys.values())
        
        journeys_without_merchant = self._remove_merchant_from_journeys(merchant_id, customer_journeys)
        total_value_without_merchant = sum(journey.total_value for journey in journeys_without_merchant.values())
        
        return total_value_with_merchant - total_value_without_merchant
    
    def _compute_incremental_value(self, merchant_id: str, journeys: List[CustomerJourney]) -> float:
        """Compute incremental value contributed by merchant."""
        incremental_value = 0.0
        
        for journey in journeys:
            merchant_touchpoints = [
                tp for tp in journey.touchpoints 
                if tp['merchant_id'] == merchant_id
            ]
            # Sum direct transaction values from this merchant
            incremental_value += sum(tp['amount'] for tp in merchant_touchpoints)
        
        return incremental_value
    
    def _compute_conversion_contribution(self, merchant_id: str, journeys: List[CustomerJourney]) -> float:
        """Compute contribution to conversions (journey completions)."""
        conversions_with_merchant = 0
        total_conversions = len(journeys)
        
        for journey in journeys:
            has_merchant = any(tp['merchant_id'] == merchant_id for tp in journey.touchpoints)
            if has_merchant and journey.journey_length > 1:  # Multi-touchpoint journey
                conversions_with_merchant += 1
        
        return conversions_with_merchant / total_conversions if total_conversions > 0 else 0.0
    
    def _compute_journey_metrics(self, merchant_id: str, journeys: List[CustomerJourney]) -> Dict[str, Any]:
        """Compute journey-related metrics for a merchant."""
        positions = []
        appears_in_journeys = len(journeys)
        unique_customers = len(set(journey.customer_id for journey in journeys))
        
        for journey in journeys:
            for i, touchpoint in enumerate(journey.touchpoints):
                if touchpoint['merchant_id'] == merchant_id:
                    # Position as ratio (0 = first, 1 = last)
                    position_ratio = i / (journey.journey_length - 1) if journey.journey_length > 1 else 0
                    positions.append(position_ratio)
        
        avg_position = sum(positions) / len(positions) if positions else 0.0
        
        return {
            'avg_position': avg_position,
            'appears_in_journeys': appears_in_journeys,
            'unique_customers': unique_customers
        }
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status."""
        return {
            "agent_name": "AttributionAgent",
            "is_running": self.is_running,
            "last_attribution_time": self.last_attribution_time.isoformat() if self.last_attribution_time else None,
            "processed_journeys_count": self.processed_journeys_count,
            "attribution_results_count": len(self.attribution_results),
            "transition_matrix_size": len(self.transition_matrix),
            "configuration": {
                "journey_window_days": self.journey_window_days,
                "min_touchpoints": self.min_touchpoints,
                "time_decay_factor": self.time_decay_factor,
                "conversion_window_hours": self.conversion_window_hours
            }
        }
    
    async def get_merchant_attribution(self, merchant_id: str) -> Optional[AttributionResult]:
        """Get attribution results for a specific merchant."""
        return self.attribution_results.get(merchant_id)
    
    async def get_top_attributed_merchants(self, model: str = "markov_attribution", limit: int = 10) -> List[Tuple[str, float]]:
        """Get top merchants by attribution model."""
        if not self.attribution_results:
            return []
        
        merchant_values = []
        for merchant_id, result in self.attribution_results.items():
            if hasattr(result, model):
                value = getattr(result, model)
                merchant_values.append((merchant_id, value))
        
        merchant_values.sort(key=lambda x: x[1], reverse=True)
        return merchant_values[:limit]
    
    async def get_transition_matrix(self) -> Dict[Tuple[str, str], MarkovTransition]:
        """Get the current Markov transition matrix."""
        return self.transition_matrix.copy()
    
    async def get_customer_journey(self, customer_id: str) -> Optional[CustomerJourney]:
        """Get journey for a specific customer."""
        return self.journey_cache.get(customer_id)
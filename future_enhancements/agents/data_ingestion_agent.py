"""
Data Ingestion Agent for MerchantIQ system.
Connects to Kafka/MKS topics via Lenses MCP, normalizes JSON records, 
and merges with reference tables to publish unified events.
"""

import asyncio
import os
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass
import pandas as pd

# Note: In actual implementation, we would use the MCP tools directly
# For now, we'll simulate the MCP integration with the available structure

from ..utils.logger import get_logger, performance_log, AgentActionContext
from ..utils.kafka_utils import (
    get_transaction_topics, 
    get_reference_topics, 
    KafkaDataProcessor,
    DataNormalizer
)


@dataclass
class UnifiedEvent:
    """Unified event structure for all transaction types."""
    customer_id: str
    merchant_id: str
    merchant_name: str
    transaction_amount: float
    category: str
    channel: str
    timestamp: datetime
    transaction_id: str
    location: Optional[Dict[str, str]] = None
    additional_data: Optional[Dict[str, Any]] = None


class DataIngestionAgent:
    """Agent responsible for data ingestion from Lenses MCP."""
    
    def __init__(self, environment: str = "financial-data", config: Optional[Dict] = None):
        """Initialize the Data Ingestion Agent.
        
        Args:
            environment: Lenses environment name
            config: Configuration dictionary
        """
        self.environment = environment
        self.config = config or {}
        self.logger = get_logger(f"agent.data_ingestion")
        
        # Initialize processors
        self.data_processor = KafkaDataProcessor()
        self.normalizer = DataNormalizer()
        
        # Agent state
        self.is_running = False
        self.last_ingestion_time: Optional[datetime] = None
        self.ingested_records_count = 0
        
        # Cache for reference data
        self.customer_cache: Dict[str, Dict[str, Any]] = {}
        self.merchant_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_expiry = timedelta(hours=1)
        self.last_cache_update: Optional[datetime] = None
        
        # Topics configuration
        self.transaction_topics = get_transaction_topics()
        self.reference_topics = get_reference_topics()
        
        # Sampling configuration
        sampling_config = self.config.get('sampling', {})
        self.records_limit = sampling_config.get('limit', 1000)
        self.batch_size = sampling_config.get('batch_size', 100)
        self.polling_interval = sampling_config.get('polling_interval', 5)
    
    @performance_log
    async def start(self) -> None:
        """Start the data ingestion agent."""
        with AgentActionContext("data_ingestion", "start"):
            self.is_running = True
            self.logger.info(f"Data Ingestion Agent started for environment: {self.environment}")
            
            # Initial data load
            await self._load_reference_data()
    
    async def stop(self) -> None:
        """Stop the data ingestion agent."""
        with AgentActionContext("data_ingestion", "stop"):
            self.is_running = False
            self.logger.info("Data Ingestion Agent stopped")
    
    @performance_log
    async def ingest_data(self) -> List[UnifiedEvent]:
        """Main ingestion method that fetches and processes data from all topics.
        
        Returns:
            List of unified events from all transaction topics
        """
        with AgentActionContext("data_ingestion", "ingest_data"):
            try:
                # Check if we need to refresh reference data cache
                if self._should_refresh_cache():
                    await self._load_reference_data()
                
                # Fetch transaction data from all topics
                transaction_data = await self._fetch_transaction_data()
                
                # Process and normalize the data
                unified_events = await self._process_transaction_data(transaction_data)
                
                # Update agent state
                self.last_ingestion_time = datetime.now()
                self.ingested_records_count += len(unified_events)
                
                self.logger.info(f"Ingested {len(unified_events)} unified events")
                return unified_events
                
            except Exception as e:
                self.logger.error(f"Data ingestion failed: {e}")
                raise
    
    async def _fetch_transaction_data(self) -> Dict[str, List[Dict[str, Any]]]:
        """Fetch data from all transaction topics.
        
        This method would use the actual Lenses MCP tools in production.
        For now, we simulate the data fetching.
        """
        transaction_data = {}
        
        # In production, this would use the MCP tools:
        # await mcp_lensesmcp_list_topics(environment=self.environment)
        # Then for each topic:
        # await some_mcp_function_to_get_topic_data(environment=self.environment, topic_name=topic)
        
        # For simulation, generate sample data
        for topic in self.transaction_topics:
            messages = await self._simulate_topic_data(topic, self.records_limit // len(self.transaction_topics))
            transaction_data[topic] = messages
            
        return transaction_data
    
    async def _simulate_topic_data(self, topic_name: str, limit: int) -> List[Dict[str, Any]]:
        """Simulate fetching data from a topic (replace with actual MCP calls)."""
        import random
        from datetime import datetime, timedelta
        
        # This simulates what would come from the actual Lenses MCP tools
        if topic_name == "credit-card-transactions":
            return [
                {
                    "transaction_id": f"CC_{i:06d}",
                    "timestamp": (datetime.now() - timedelta(hours=random.randint(0, 48))).isoformat(),
                    "customer_id": f"CUST_{random.randint(1, 500):04d}",
                    "merchant": random.choice(["TechWorld Electronics", "Fashion Hub", "Gourmet Restaurant"]),
                    "category": random.choice(["Electronics", "Fashion", "Food"]),
                    "amount": round(random.uniform(10, 1000), 2),
                    "currency": "USD",
                    "location": {
                        "city": random.choice(["New York", "Los Angeles", "Chicago"]),
                        "state": random.choice(["NY", "CA", "IL"]),
                        "zip": f"{random.randint(10000, 99999)}"
                    },
                    "status": "completed"
                }
                for i in range(min(limit, 50))  # Limit simulation size
            ]
        
        elif topic_name == "paypal-transactions":
            return [
                {
                    "transaction_id": f"PP_{i:06d}",
                    "timestamp": (datetime.now() - timedelta(hours=random.randint(0, 48))).isoformat(),
                    "customer_id": f"CUST_{random.randint(1, 500):04d}",
                    "merchant_id": f"MERCH_{random.randint(1, 50):03d}",
                    "merchant": random.choice(["Online Marketplace", "Digital Services"]),
                    "category": "Online",
                    "amount": round(random.uniform(5, 500), 2),
                    "currency": "USD",
                    "status": "completed",
                    "is_fraud": random.random() < 0.02
                }
                for i in range(min(limit, 30))
            ]
        
        elif topic_name == "auto-loan-payments":
            return [
                {
                    "payment_id": f"AUTO_{i:06d}",
                    "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
                    "customer_id": f"CUST_{random.randint(1, 500):04d}",
                    "amount": round(random.uniform(200, 800), 2),
                    "payment_status": "completed"
                }
                for i in range(min(limit, 20))
            ]
        
        elif topic_name == "home-loan-payments":
            return [
                {
                    "payment_id": f"HOME_{i:06d}",
                    "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
                    "customer_id": f"CUST_{random.randint(1, 500):04d}",
                    "amount": round(random.uniform(1000, 4000), 2),
                    "payment_status": "completed"
                }
                for i in range(min(limit, 15))
            ]
        
        return []
    
    async def _process_transaction_data(self, transaction_data: Dict[str, List[Dict[str, Any]]]) -> List[UnifiedEvent]:
        """Process and normalize transaction data into unified events."""
        unified_events = []
        
        for topic_name, messages in transaction_data.items():
            if not messages:
                continue
                
            # Normalize messages to unified format
            normalized_messages = self.normalizer.normalize_to_transaction_format(topic_name, messages)
            
            # Convert to UnifiedEvent objects
            for msg in normalized_messages:
                if msg and msg.get('transaction_id'):  # Ensure valid message
                    try:
                        # Enrich with reference data
                        enriched_msg = await self._enrich_with_reference_data(msg)
                        
                        # Create UnifiedEvent
                        event = UnifiedEvent(
                            customer_id=enriched_msg.get('customer_id', ''),
                            merchant_id=enriched_msg.get('merchant_id', ''),
                            merchant_name=enriched_msg.get('merchant_name', 'Unknown'),
                            transaction_amount=float(enriched_msg.get('amount', 0)),
                            category=enriched_msg.get('category', 'Unknown'),
                            channel=enriched_msg.get('channel', 'unknown'),
                            timestamp=enriched_msg.get('timestamp', datetime.now()),
                            transaction_id=enriched_msg.get('transaction_id', ''),
                            location=enriched_msg.get('location'),
                            additional_data={
                                'currency': enriched_msg.get('currency'),
                                'status': enriched_msg.get('status'),
                                'source_topic': topic_name
                            }
                        )
                        unified_events.append(event)
                        
                    except Exception as e:
                        self.logger.warning(f"Failed to create unified event from {topic_name}: {e}")
                        continue
        
        return unified_events
    
    async def _enrich_with_reference_data(self, message: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich transaction message with customer and merchant reference data."""
        enriched = message.copy()
        
        # Enrich with customer data
        customer_id = message.get('customer_id')
        if customer_id and customer_id in self.customer_cache:
            customer_data = self.customer_cache[customer_id]
            enriched.update({
                'customer_name': customer_data.get('customer_name'),
                'customer_credit_score': customer_data.get('credit_score'),
                'customer_risk_level': customer_data.get('risk_level')
            })
        
        # Enrich with merchant data
        merchant_id = message.get('merchant_id')
        if merchant_id and merchant_id in self.merchant_cache:
            merchant_data = self.merchant_cache[merchant_id]
            enriched.update({
                'merchant_name': merchant_data.get('merchant_name', enriched.get('merchant_name')),
                'merchant_category': merchant_data.get('category'),
                'merchant_rating': merchant_data.get('rating'),
                'merchant_verified': merchant_data.get('verified')
            })
        
        return enriched
    
    @performance_log
    async def _load_reference_data(self) -> None:
        """Load customer and merchant reference data."""
        with AgentActionContext("data_ingestion", "load_reference_data"):
            try:
                # Load customer reference data
                # In production: await mcp_function_to_get_customers()
                customers_data = await self._simulate_reference_data("ref-customers")
                self.customer_cache = {
                    customer['customer_id']: customer 
                    for customer in customers_data
                }
                
                # Load merchant reference data
                # In production: await mcp_function_to_get_merchants()  
                merchants_data = await self._simulate_reference_data("ref-merchants")
                self.merchant_cache = {
                    merchant['merchant_id']: merchant 
                    for merchant in merchants_data
                }
                
                self.last_cache_update = datetime.now()
                
                self.logger.info(f"Loaded {len(self.customer_cache)} customers and {len(self.merchant_cache)} merchants")
                
            except Exception as e:
                self.logger.error(f"Failed to load reference data: {e}")
                raise
    
    async def _simulate_reference_data(self, topic_name: str) -> List[Dict[str, Any]]:
        """Simulate loading reference data (replace with actual MCP calls)."""
        import random
        
        if topic_name == "ref-customers":
            return [
                {
                    "customer_id": f"CUST_{i:04d}",
                    "customer_name": f"Customer {i}",
                    "email": f"customer{i}@example.com",
                    "credit_score": random.randint(300, 850),
                    "risk_level": random.choice(["low", "medium", "high"])
                }
                for i in range(1, 501)  # 500 customers
            ]
        
        elif topic_name == "ref-merchants":
            merchants = [
                ("TechWorld Electronics", "Electronics"),
                ("Fashion Hub", "Fashion"),
                ("Gourmet Restaurant", "Food"),
                ("Online Marketplace", "Online"),
                ("Digital Services", "Technology"),
                ("BookStore Plus", "Books"),
                ("Fitness Center", "Fitness")
            ]
            
            return [
                {
                    "merchant_id": f"MERCH_{i:03d}",
                    "merchant_name": name,
                    "category": category,
                    "rating": round(random.uniform(3.0, 5.0), 1),
                    "verified": random.random() > 0.1
                }
                for i, (name, category) in enumerate(merchants, 1)
            ]
        
        return []
    
    def _should_refresh_cache(self) -> bool:
        """Check if reference data cache should be refreshed."""
        if self.last_cache_update is None:
            return True
        
        return datetime.now() - self.last_cache_update > self.cache_expiry
    
    def get_status(self) -> Dict[str, Any]:
        """Get current agent status."""
        return {
            "agent_name": "DataIngestionAgent",
            "environment": self.environment,
            "is_running": self.is_running,
            "last_ingestion_time": self.last_ingestion_time.isoformat() if self.last_ingestion_time else None,
            "ingested_records_count": self.ingested_records_count,
            "cache_size": {
                "customers": len(self.customer_cache),
                "merchants": len(self.merchant_cache)
            },
            "last_cache_update": self.last_cache_update.isoformat() if self.last_cache_update else None,
            "configuration": {
                "records_limit": self.records_limit,
                "batch_size": self.batch_size,
                "polling_interval": self.polling_interval
            }
        }
    
    async def get_sample_data(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get sample unified events for testing/preview."""
        events = await self.ingest_data()
        sample_events = events[:limit]
        
        return [
            {
                "customer_id": event.customer_id,
                "merchant_id": event.merchant_id,
                "merchant_name": event.merchant_name,
                "transaction_amount": event.transaction_amount,
                "category": event.category,
                "channel": event.channel,
                "timestamp": event.timestamp.isoformat(),
                "transaction_id": event.transaction_id,
                "location": event.location,
                "additional_data": event.additional_data
            }
            for event in sample_events
        ]
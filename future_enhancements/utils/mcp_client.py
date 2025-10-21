"""
MCP (Model Context Protocol) client for connecting to Lenses platform.
Handles data fetching from Kafka topics through Lenses MCP integration.
"""

import asyncio
import json
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import aiohttp
from dataclasses import dataclass
import pandas as pd

from ..utils.logger import get_logger, performance_log, AgentActionContext


@dataclass
class TopicData:
    """Data structure for topic information."""
    topic_name: str
    messages: List[Dict[str, Any]]
    total_count: int
    timestamp: datetime


class LensesMCPClient:
    """Client for interacting with Lenses MCP platform."""
    
    def __init__(self, environment: str = "financial-data", config: Optional[Dict] = None):
        """Initialize the MCP client.
        
        Args:
            environment: Lenses environment name
            config: Configuration dictionary
        """
        self.environment = environment
        self.config = config or {}
        self.logger = get_logger(f"mcp_client.{environment}")
        self.session: Optional[aiohttp.ClientSession] = None
        
        # Extract configuration
        self.retry_config = self.config.get('retry', {
            'max_attempts': 3,
            'backoff_factor': 2,
            'timeout': 30
        })
        
        self.sampling_config = self.config.get('sampling', {
            'limit': 1000,
            'batch_size': 100,
            'polling_interval': 5
        })
    
    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.disconnect()
    
    async def connect(self):
        """Establish connection to Lenses MCP."""
        if not self.session:
            connector = aiohttp.TCPConnector(limit=10, ttl_dns_cache=300)
            timeout = aiohttp.ClientTimeout(total=self.retry_config['timeout'])
            self.session = aiohttp.ClientSession(
                connector=connector,
                timeout=timeout,
                headers={'User-Agent': 'MerchantIQ/1.0'}
            )
        self.logger.info("Connected to Lenses MCP")
    
    async def disconnect(self):
        """Close connection to Lenses MCP."""
        if self.session:
            await self.session.close()
            self.session = None
        self.logger.info("Disconnected from Lenses MCP")
    
    @performance_log
    async def fetch_topic_data(self, topic_name: str, limit: Optional[int] = None) -> TopicData:
        """Fetch data from a specific Kafka topic.
        
        Args:
            topic_name: Name of the topic to fetch from
            limit: Maximum number of records to fetch
            
        Returns:
            TopicData object containing messages and metadata
        """
        limit = limit or self.sampling_config['limit']
        
        with AgentActionContext("mcp_client", "fetch_topic_data", topic=topic_name, limit=limit):
            # Since we're working with the actual Lenses MCP tools,
            # we'll simulate the API calls but structure it to work with real MCP
            
            try:
                # This would use the actual MCP tools for topic data fetching
                # For now, we'll simulate realistic data based on the topic schemas we saw
                messages = await self._simulate_topic_fetch(topic_name, limit)
                
                return TopicData(
                    topic_name=topic_name,
                    messages=messages,
                    total_count=len(messages),
                    timestamp=datetime.now()
                )
                
            except Exception as e:
                self.logger.error(f"Failed to fetch data from topic {topic_name}: {e}")
                raise
    
    async def _simulate_topic_fetch(self, topic_name: str, limit: int) -> List[Dict[str, Any]]:
        """Simulate topic data fetching based on known schemas."""
        
        if topic_name == "credit-card-transactions":
            return self._generate_credit_card_data(limit)
        elif topic_name == "paypal-transactions":
            return self._generate_paypal_data(limit)
        elif topic_name == "auto-loan-payments":
            return self._generate_auto_loan_data(limit)
        elif topic_name == "home-loan-payments":
            return self._generate_home_loan_data(limit)
        elif topic_name == "ref-customers":
            return self._generate_customer_data(limit)
        elif topic_name == "ref-merchants":
            return self._generate_merchant_data(limit)
        else:
            return []
    
    def _generate_credit_card_data(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample credit card transaction data."""
        import random
        from datetime import datetime, timedelta
        
        merchants = ["TechWorld Electronics", "Fashion Hub", "Gourmet Restaurant", "BookStore Plus", "Fitness Center"]
        categories = ["Electronics", "Fashion", "Food", "Books", "Fitness"]
        
        data = []
        for i in range(min(limit, 100)):  # Limit simulation
            data.append({
                "transaction_id": f"CC_{i:06d}",
                "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
                "card_number": f"****{random.randint(1000, 9999)}",
                "merchant": random.choice(merchants),
                "category": random.choice(categories),
                "amount": round(random.uniform(10, 1000), 2),
                "currency": "USD",
                "location": {
                    "city": random.choice(["New York", "Los Angeles", "Chicago"]),
                    "state": random.choice(["NY", "CA", "IL"]),
                    "zip": f"{random.randint(10000, 99999)}"
                },
                "status": "completed",
                "customer_id": f"CUST_{random.randint(1, 1000):04d}"
            })
        return data
    
    def _generate_paypal_data(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample PayPal transaction data."""
        import random
        from datetime import datetime, timedelta
        
        merchants = ["Online Marketplace", "Digital Services", "E-commerce Store", "Subscription Service"]
        
        data = []
        for i in range(min(limit, 100)):
            data.append({
                "transaction_id": f"PP_{i:06d}",
                "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
                "customer_id": f"CUST_{random.randint(1, 1000):04d}",
                "merchant_id": f"MERCH_{random.randint(1, 100):03d}",
                "paypal_email": f"user{i}@example.com",
                "merchant": random.choice(merchants),
                "category": "Online",
                "amount": round(random.uniform(5, 500), 2),
                "currency": "USD",
                "account_country": "US",
                "location": {
                    "city": random.choice(["Seattle", "Austin", "Miami"]),
                    "state": random.choice(["WA", "TX", "FL"]),
                    "country": "US"
                },
                "status": "completed",
                "is_fraud": random.random() < 0.02,  # 2% fraud rate
                "fraud_indicators": {
                    "is_foreign_account": random.random() < 0.1,
                    "is_high_amount": random.random() < 0.05,
                    "account_age_days": random.randint(30, 3650)
                }
            })
        return data
    
    def _generate_auto_loan_data(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample auto loan payment data."""
        import random
        from datetime import datetime, timedelta
        
        data = []
        for i in range(min(limit, 50)):
            amount = round(random.uniform(200, 800), 2)
            principal = round(amount * 0.7, 2)
            interest = round(amount - principal, 2)
            
            data.append({
                "payment_id": f"AUTO_{i:06d}",
                "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
                "loan_id": f"LOAN_{random.randint(1000, 9999)}",
                "customer_id": f"CUST_{random.randint(1, 1000):04d}",
                "amount": amount,
                "principal": principal,
                "interest": interest,
                "remaining_balance": round(random.uniform(5000, 50000), 2),
                "payment_method": random.choice(["auto_debit", "online", "check"]),
                "payment_status": "completed",
                "due_date": (datetime.now() + timedelta(days=30)).date().isoformat(),
                "vehicle_info": {
                    "make": random.choice(["Toyota", "Ford", "Honda", "BMW"]),
                    "model": random.choice(["Camry", "F-150", "Civic", "X3"]),
                    "year": random.randint(2015, 2024)
                }
            })
        return data
    
    def _generate_home_loan_data(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample home loan payment data."""
        import random
        from datetime import datetime, timedelta
        
        data = []
        for i in range(min(limit, 30)):
            amount = round(random.uniform(1000, 4000), 2)
            principal = round(amount * 0.4, 2)
            interest = round(amount * 0.5, 2)
            escrow = round(amount * 0.1, 2)
            
            data.append({
                "payment_id": f"HOME_{i:06d}",
                "timestamp": (datetime.now() - timedelta(days=random.randint(0, 30))).isoformat(),
                "loan_id": f"HLOAN_{random.randint(1000, 9999)}",
                "customer_id": f"CUST_{random.randint(1, 1000):04d}",
                "amount": amount,
                "principal": principal,
                "interest": interest,
                "escrow": escrow,
                "remaining_balance": round(random.uniform(50000, 500000), 2),
                "payment_method": random.choice(["auto_debit", "online", "wire"]),
                "payment_status": "completed",
                "due_date": (datetime.now() + timedelta(days=30)).date().isoformat(),
                "property_info": {
                    "address": f"{random.randint(100, 9999)} Main St",
                    "city": random.choice(["Springfield", "Franklin", "Madison"]),
                    "state": random.choice(["CA", "TX", "NY"]),
                    "zip": f"{random.randint(10000, 99999)}",
                    "property_type": random.choice(["single_family", "condo", "townhouse"])
                },
                "loan_type": random.choice(["conventional", "fha", "va"])
            })
        return data
    
    def _generate_customer_data(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample customer reference data."""
        import random
        from datetime import datetime, timedelta
        
        data = []
        for i in range(min(limit, 100)):
            data.append({
                "customer_id": f"CUST_{i:04d}",
                "customer_name": f"Customer {i}",
                "email": f"customer{i}@example.com",
                "phone": f"+1-555-{random.randint(100, 999)}-{random.randint(1000, 9999)}",
                "address": {
                    "street": f"{random.randint(100, 9999)} {random.choice(['Main', 'Oak', 'Pine'])} St",
                    "city": random.choice(["Boston", "Denver", "Portland"]),
                    "state": random.choice(["MA", "CO", "OR"]),
                    "zip": f"{random.randint(10000, 99999)}"
                },
                "account_created": (datetime.now() - timedelta(days=random.randint(30, 1095))).date().isoformat(),
                "credit_score": random.randint(300, 850),
                "preferred_payment": random.choice(["credit_card", "paypal", "bank_transfer"]),
                "verified": random.random() > 0.1,
                "risk_level": random.choice(["low", "medium", "high"])
            })
        return data
    
    def _generate_merchant_data(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample merchant reference data."""
        import random
        from datetime import datetime, timedelta
        
        merchants = [
            ("TechWorld Electronics", "Electronics", "retail"),
            ("Fashion Hub", "Fashion", "retail"),
            ("Gourmet Restaurant", "Food", "restaurant"),
            ("BookStore Plus", "Books", "retail"),
            ("Fitness Center", "Fitness", "service"),
            ("Online Marketplace", "General", "e-commerce"),
            ("Digital Services", "Technology", "service"),
            ("Auto Dealer", "Automotive", "dealer")
        ]
        
        data = []
        for i, (name, category, biz_type) in enumerate(merchants[:limit]):
            data.append({
                "merchant_id": f"MERCH_{i:03d}",
                "merchant_name": name,
                "category": category,
                "business_type": biz_type,
                "established_date": (datetime.now() - timedelta(days=random.randint(365, 3650))).date().isoformat(),
                "location": {
                    "address": f"{random.randint(100, 9999)} Business Blvd",
                    "city": random.choice(["San Francisco", "Seattle", "Austin"]),
                    "state": random.choice(["CA", "WA", "TX"]),
                    "zip": f"{random.randint(10000, 99999)}"
                },
                "average_transaction": round(random.uniform(25, 300), 2),
                "rating": round(random.uniform(3.0, 5.0), 1),
                "verified": random.random() > 0.05
            })
        return data
    
    @performance_log
    async def fetch_multiple_topics(self, topic_names: List[str], limit_per_topic: Optional[int] = None) -> Dict[str, TopicData]:
        """Fetch data from multiple topics concurrently.
        
        Args:
            topic_names: List of topic names to fetch from
            limit_per_topic: Maximum records per topic
            
        Returns:
            Dictionary mapping topic names to TopicData objects
        """
        with AgentActionContext("mcp_client", "fetch_multiple_topics", topics=topic_names):
            tasks = [
                self.fetch_topic_data(topic, limit_per_topic) 
                for topic in topic_names
            ]
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            topic_data = {}
            for topic_name, result in zip(topic_names, results):
                if isinstance(result, Exception):
                    self.logger.error(f"Failed to fetch {topic_name}: {result}")
                    # Create empty TopicData for failed fetches
                    topic_data[topic_name] = TopicData(
                        topic_name=topic_name,
                        messages=[],
                        total_count=0,
                        timestamp=datetime.now()
                    )
                else:
                    topic_data[topic_name] = result
            
            return topic_data
    
    def topics_to_dataframe(self, topic_data: Dict[str, TopicData]) -> pd.DataFrame:
        """Convert topic data to a unified pandas DataFrame.
        
        Args:
            topic_data: Dictionary of TopicData objects
            
        Returns:
            Unified DataFrame with normalized schema
        """
        all_records = []
        
        for topic_name, data in topic_data.items():
            for message in data.messages:
                # Normalize to common schema
                record = self._normalize_message(topic_name, message)
                if record:
                    all_records.append(record)
        
        if not all_records:
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'customer_id', 'merchant_id', 'merchant_name', 'amount', 
                'timestamp', 'channel', 'category', 'transaction_id'
            ])
        
        df = pd.DataFrame(all_records)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        return df
    
    def _normalize_message(self, topic_name: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a message to common schema."""
        
        if topic_name in ["credit-card-transactions", "paypal-transactions"]:
            return {
                'customer_id': message.get('customer_id'),
                'merchant_id': message.get('merchant_id', message.get('merchant')),
                'merchant_name': message.get('merchant'),
                'amount': message.get('amount'),
                'timestamp': message.get('timestamp'),
                'channel': 'credit_card' if 'credit-card' in topic_name else 'paypal',
                'category': message.get('category'),
                'transaction_id': message.get('transaction_id')
            }
        
        elif topic_name in ["auto-loan-payments", "home-loan-payments"]:
            return {
                'customer_id': message.get('customer_id'),
                'merchant_id': 'BANK_001',  # Loan payments are to the bank
                'merchant_name': 'Bank Loan Services',
                'amount': message.get('amount'),
                'timestamp': message.get('timestamp'),
                'channel': 'auto_loan' if 'auto-loan' in topic_name else 'home_loan',
                'category': 'Financial Services',
                'transaction_id': message.get('payment_id')
            }
        
        elif topic_name == "ref-customers":
            # Customer reference data - not transaction data
            return None
            
        elif topic_name == "ref-merchants":
            # Merchant reference data - not transaction data
            return None
        
        return None
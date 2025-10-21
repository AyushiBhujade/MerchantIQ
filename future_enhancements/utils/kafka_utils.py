"""
Kafka utilities for the MerchantIQ system.
Provides helper functions for working with Kafka data and schemas.
"""

import json
from typing import Dict, List, Any, Optional, Union
from datetime import datetime, timedelta
import pandas as pd
from dataclasses import dataclass

from ..utils.logger import get_logger


@dataclass
class KafkaTopicInfo:
    """Information about a Kafka topic."""
    name: str
    partitions: int
    replication_factor: int
    message_count: int
    schema: Optional[Dict[str, Any]] = None


class SchemaRegistry:
    """Registry for managing Kafka topic schemas."""
    
    def __init__(self):
        self.logger = get_logger("schema_registry")
        self._schemas = self._initialize_schemas()
    
    def _initialize_schemas(self) -> Dict[str, Dict[str, Any]]:
        """Initialize known schemas for financial data topics."""
        return {
            "credit-card-transactions": {
                "type": "record",
                "name": "credit_card_transaction",
                "fields": [
                    {"name": "transaction_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "card_number", "type": "string"},
                    {"name": "merchant", "type": "string"},
                    {"name": "category", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "currency", "type": "string"},
                    {"name": "location", "type": {
                        "type": "record",
                        "name": "location",
                        "fields": [
                            {"name": "city", "type": "string"},
                            {"name": "state", "type": "string"},
                            {"name": "zip", "type": "string"}
                        ]
                    }},
                    {"name": "status", "type": "string"},
                    {"name": "customer_id", "type": "string"}
                ]
            },
            
            "paypal-transactions": {
                "type": "record",
                "name": "paypal_transaction",
                "fields": [
                    {"name": "transaction_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "customer_id", "type": "string"},
                    {"name": "merchant_id", "type": "string"},
                    {"name": "paypal_email", "type": "string"},
                    {"name": "merchant", "type": "string"},
                    {"name": "category", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "currency", "type": "string"},
                    {"name": "account_country", "type": "string"},
                    {"name": "location", "type": {
                        "type": "record",
                        "name": "location",
                        "fields": [
                            {"name": "city", "type": "string"},
                            {"name": "state", "type": "string"},
                            {"name": "country", "type": "string"}
                        ]
                    }},
                    {"name": "status", "type": "string"},
                    {"name": "is_fraud", "type": "boolean"},
                    {"name": "fraud_indicators", "type": {
                        "type": "record",
                        "name": "fraud_indicators",
                        "fields": [
                            {"name": "is_foreign_account", "type": "boolean"},
                            {"name": "is_high_amount", "type": "boolean"},
                            {"name": "account_age_days", "type": "int"}
                        ]
                    }}
                ]
            },
            
            "auto-loan-payments": {
                "type": "record",
                "name": "auto_loan_payment",
                "fields": [
                    {"name": "payment_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "loan_id", "type": "string"},
                    {"name": "customer_id", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "principal", "type": "double"},
                    {"name": "interest", "type": "double"},
                    {"name": "remaining_balance", "type": "double"},
                    {"name": "payment_method", "type": "string"},
                    {"name": "payment_status", "type": "string"},
                    {"name": "due_date", "type": "string"},
                    {"name": "vehicle_info", "type": {
                        "type": "record",
                        "name": "vehicle_info",
                        "fields": [
                            {"name": "make", "type": "string"},
                            {"name": "model", "type": "string"},
                            {"name": "year", "type": "int"}
                        ]
                    }}
                ]
            },
            
            "home-loan-payments": {
                "type": "record",
                "name": "home_loan_payment",
                "fields": [
                    {"name": "payment_id", "type": "string"},
                    {"name": "timestamp", "type": "string"},
                    {"name": "loan_id", "type": "string"},
                    {"name": "customer_id", "type": "string"},
                    {"name": "amount", "type": "double"},
                    {"name": "principal", "type": "double"},
                    {"name": "interest", "type": "double"},
                    {"name": "escrow", "type": "double"},
                    {"name": "remaining_balance", "type": "double"},
                    {"name": "payment_method", "type": "string"},
                    {"name": "payment_status", "type": "string"},
                    {"name": "due_date", "type": "string"},
                    {"name": "property_info", "type": {
                        "type": "record",
                        "name": "property_info",
                        "fields": [
                            {"name": "address", "type": "string"},
                            {"name": "city", "type": "string"},
                            {"name": "state", "type": "string"},
                            {"name": "zip", "type": "string"},
                            {"name": "property_type", "type": "string"}
                        ]
                    }},
                    {"name": "loan_type", "type": "string"}
                ]
            },
            
            "ref-customers": {
                "type": "record",
                "name": "customer",
                "fields": [
                    {"name": "customer_id", "type": "string"},
                    {"name": "customer_name", "type": "string"},
                    {"name": "email", "type": "string"},
                    {"name": "phone", "type": "string"},
                    {"name": "address", "type": {
                        "type": "record",
                        "name": "address",
                        "fields": [
                            {"name": "street", "type": "string"},
                            {"name": "city", "type": "string"},
                            {"name": "state", "type": "string"},
                            {"name": "zip", "type": "string"}
                        ]
                    }},
                    {"name": "account_created", "type": "string"},
                    {"name": "credit_score", "type": "int"},
                    {"name": "preferred_payment", "type": "string"},
                    {"name": "verified", "type": "boolean"},
                    {"name": "risk_level", "type": "string"}
                ]
            },
            
            "ref-merchants": {
                "type": "record",
                "name": "merchant",
                "fields": [
                    {"name": "merchant_id", "type": "string"},
                    {"name": "merchant_name", "type": "string"},
                    {"name": "category", "type": "string"},
                    {"name": "business_type", "type": "string"},
                    {"name": "established_date", "type": "string"},
                    {"name": "location", "type": {
                        "type": "record",
                        "name": "location",
                        "fields": [
                            {"name": "address", "type": "string"},
                            {"name": "city", "type": "string"},
                            {"name": "state", "type": "string"},
                            {"name": "zip", "type": "string"}
                        ]
                    }},
                    {"name": "average_transaction", "type": "double"},
                    {"name": "rating", "type": "double"},
                    {"name": "verified", "type": "boolean"}
                ]
            }
        }
    
    def get_schema(self, topic_name: str) -> Optional[Dict[str, Any]]:
        """Get schema for a topic."""
        return self._schemas.get(topic_name)
    
    def validate_message(self, topic_name: str, message: Dict[str, Any]) -> bool:
        """Validate a message against its topic schema."""
        schema = self.get_schema(topic_name)
        if not schema:
            return True  # No schema to validate against
        
        try:
            return self._validate_against_schema(message, schema)
        except Exception as e:
            self.logger.warning(f"Schema validation failed for {topic_name}: {e}")
            return False
    
    def _validate_against_schema(self, message: Dict[str, Any], schema: Dict[str, Any]) -> bool:
        """Validate message against Avro-like schema."""
        if schema["type"] != "record":
            return True
        
        fields = schema.get("fields", [])
        for field in fields:
            field_name = field["name"]
            field_type = field["type"]
            
            if field_name not in message:
                self.logger.debug(f"Missing field: {field_name}")
                return False
            
            if not self._validate_field_type(message[field_name], field_type):
                self.logger.debug(f"Invalid type for field {field_name}: {type(message[field_name])}")
                return False
        
        return True
    
    def _validate_field_type(self, value: Any, field_type: Union[str, Dict[str, Any]]) -> bool:
        """Validate field type."""
        if isinstance(field_type, str):
            if field_type == "string":
                return isinstance(value, str)
            elif field_type == "int":
                return isinstance(value, int)
            elif field_type == "double":
                return isinstance(value, (int, float))
            elif field_type == "boolean":
                return isinstance(value, bool)
            else:
                return True  # Unknown type, assume valid
        
        elif isinstance(field_type, dict) and field_type.get("type") == "record":
            return isinstance(value, dict)
        
        return True


class DataNormalizer:
    """Normalizes data from different Kafka topics to a unified schema."""
    
    def __init__(self):
        self.logger = get_logger("data_normalizer")
        self.schema_registry = SchemaRegistry()
    
    def normalize_to_transaction_format(self, topic_name: str, messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Normalize messages to unified transaction format."""
        normalized = []
        
        for message in messages:
            norm_message = self._normalize_single_message(topic_name, message)
            if norm_message:
                normalized.append(norm_message)
        
        return normalized
    
    def _normalize_single_message(self, topic_name: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Normalize a single message based on topic type."""
        
        if topic_name in ["credit-card-transactions", "paypal-transactions"]:
            return {
                'transaction_id': message.get('transaction_id'),
                'customer_id': message.get('customer_id'),
                'merchant_id': self._extract_merchant_id(message, topic_name),
                'merchant_name': message.get('merchant'),
                'amount': float(message.get('amount', 0)),
                'timestamp': self._parse_timestamp(message.get('timestamp')),
                'channel': self._get_channel_from_topic(topic_name),
                'category': message.get('category'),
                'location': self._extract_location(message),
                'currency': message.get('currency', 'USD'),
                'status': message.get('status')
            }
        
        elif topic_name in ["auto-loan-payments", "home-loan-payments"]:
            return {
                'transaction_id': message.get('payment_id'),
                'customer_id': message.get('customer_id'),
                'merchant_id': 'BANK_001',  # Bank as merchant for loan payments
                'merchant_name': 'Bank Financial Services',
                'amount': float(message.get('amount', 0)),
                'timestamp': self._parse_timestamp(message.get('timestamp')),
                'channel': self._get_channel_from_topic(topic_name),
                'category': 'Financial Services',
                'location': None,
                'currency': 'USD',
                'status': message.get('payment_status', 'completed')
            }
        
        return None
    
    def _extract_merchant_id(self, message: Dict[str, Any], topic_name: str) -> str:
        """Extract merchant ID from message."""
        if 'merchant_id' in message:
            return message['merchant_id']
        
        # Generate merchant ID from merchant name if not present
        merchant_name = message.get('merchant', 'Unknown')
        return f"MERCH_{hash(merchant_name) % 10000:04d}"
    
    def _parse_timestamp(self, timestamp_str: str) -> datetime:
        """Parse timestamp string to datetime object."""
        if not timestamp_str:
            return datetime.now()
        
        try:
            # Try different timestamp formats
            formats = [
                '%Y-%m-%dT%H:%M:%S',
                '%Y-%m-%dT%H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d'
            ]
            
            for fmt in formats:
                try:
                    return datetime.strptime(timestamp_str[:len(fmt)], fmt)
                except ValueError:
                    continue
            
            # If no format matches, try pandas
            import pandas as pd
            return pd.to_datetime(timestamp_str)
        
        except Exception as e:
            self.logger.warning(f"Failed to parse timestamp '{timestamp_str}': {e}")
            return datetime.now()
    
    def _get_channel_from_topic(self, topic_name: str) -> str:
        """Get channel name from topic name."""
        channel_mapping = {
            'credit-card-transactions': 'credit_card',
            'paypal-transactions': 'paypal',
            'auto-loan-payments': 'auto_loan',
            'home-loan-payments': 'home_loan'
        }
        return channel_mapping.get(topic_name, 'unknown')
    
    def _extract_location(self, message: Dict[str, Any]) -> Optional[Dict[str, str]]:
        """Extract location information from message."""
        location = message.get('location')
        if isinstance(location, dict):
            return {
                'city': location.get('city'),
                'state': location.get('state'),
                'country': location.get('country', 'US'),
                'zip': location.get('zip')
            }
        return None


class KafkaDataProcessor:
    """Processes and aggregates Kafka data for analytics."""
    
    def __init__(self):
        self.logger = get_logger("kafka_processor")
        self.normalizer = DataNormalizer()
    
    def create_unified_dataframe(self, topic_data: Dict[str, List[Dict[str, Any]]]) -> pd.DataFrame:
        """Create unified DataFrame from multiple topic data."""
        all_transactions = []
        
        for topic_name, messages in topic_data.items():
            if not messages:
                continue
            
            normalized_messages = self.normalizer.normalize_to_transaction_format(topic_name, messages)
            all_transactions.extend(normalized_messages)
        
        if not all_transactions:
            # Return empty DataFrame with expected columns
            return pd.DataFrame(columns=[
                'transaction_id', 'customer_id', 'merchant_id', 'merchant_name',
                'amount', 'timestamp', 'channel', 'category', 'location',
                'currency', 'status'
            ])
        
        df = pd.DataFrame(all_transactions)
        
        # Ensure proper data types
        df['amount'] = pd.to_numeric(df['amount'], errors='coerce')
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        # Sort by timestamp
        df = df.sort_values('timestamp').reset_index(drop=True)
        
        self.logger.info(f"Created unified DataFrame with {len(df)} transactions")
        return df
    
    def enrich_with_reference_data(self, df: pd.DataFrame, 
                                   customers: List[Dict[str, Any]], 
                                   merchants: List[Dict[str, Any]]) -> pd.DataFrame:
        """Enrich transaction data with customer and merchant reference data."""
        
        # Create reference DataFrames
        if customers:
            customers_df = pd.DataFrame(customers)
            df = df.merge(customers_df[['customer_id', 'customer_name', 'credit_score', 'risk_level']], 
                         on='customer_id', how='left')
        
        if merchants:
            merchants_df = pd.DataFrame(merchants)
            df = df.merge(merchants_df[['merchant_id', 'merchant_name', 'category', 'rating', 'verified']], 
                         on='merchant_id', how='left', suffixes=('', '_ref'))
        
        self.logger.info(f"Enriched DataFrame with reference data")
        return df
    
    def calculate_merchant_metrics(self, df: pd.DataFrame, window_days: int = 30) -> pd.DataFrame:
        """Calculate merchant-level metrics from transaction data."""
        
        # Filter to recent data
        cutoff_date = datetime.now() - timedelta(days=window_days)
        recent_df = df[df['timestamp'] >= cutoff_date].copy()
        
        if recent_df.empty:
            return pd.DataFrame(columns=[
                'merchant_id', 'merchant_name', 'total_volume', 'transaction_count',
                'avg_transaction_value', 'unique_customers', 'repeat_customer_ratio'
            ])
        
        # Group by merchant and calculate metrics
        merchant_metrics = []
        
        for merchant_id, group in recent_df.groupby('merchant_id'):
            metrics = {
                'merchant_id': merchant_id,
                'merchant_name': group['merchant_name'].iloc[0] if not group['merchant_name'].isna().all() else 'Unknown',
                'total_volume': group['amount'].sum(),
                'transaction_count': len(group),
                'avg_transaction_value': group['amount'].mean(),
                'unique_customers': group['customer_id'].nunique(),
                'min_transaction': group['amount'].min(),
                'max_transaction': group['amount'].max(),
                'std_transaction': group['amount'].std(),
                'first_transaction': group['timestamp'].min(),
                'last_transaction': group['timestamp'].max()
            }
            
            # Calculate repeat customer ratio
            customer_counts = group['customer_id'].value_counts()
            repeat_customers = (customer_counts > 1).sum()
            metrics['repeat_customer_ratio'] = repeat_customers / metrics['unique_customers'] if metrics['unique_customers'] > 0 else 0
            
            # Calculate transaction frequency (transactions per day)
            date_range = (metrics['last_transaction'] - metrics['first_transaction']).days + 1
            metrics['transaction_frequency'] = metrics['transaction_count'] / max(date_range, 1)
            
            merchant_metrics.append(metrics)
        
        metrics_df = pd.DataFrame(merchant_metrics)
        self.logger.info(f"Calculated metrics for {len(metrics_df)} merchants")
        
        return metrics_df


def get_topic_list() -> List[str]:
    """Get list of supported topic names."""
    return [
        "credit-card-transactions",
        "paypal-transactions", 
        "auto-loan-payments",
        "home-loan-payments",
        "ref-customers",
        "ref-merchants"
    ]


def get_transaction_topics() -> List[str]:
    """Get list of transaction topic names."""
    return [
        "credit-card-transactions",
        "paypal-transactions",
        "auto-loan-payments", 
        "home-loan-payments"
    ]


def get_reference_topics() -> List[str]:
    """Get list of reference data topic names."""
    return [
        "ref-customers",
        "ref-merchants"
    ]